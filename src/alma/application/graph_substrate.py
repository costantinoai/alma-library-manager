"""The ONE semantic-map substrate: corpus-scope layout + incremental placement.

Task 50 M1 (decision 50-G). The 2-D semantic layout is a durable artifact,
not a render-time computation:

* There is ONE computed layout — ``publication_clusters`` rows with
  ``scope = 'corpus'`` (:data:`SUBSTRATE_SCOPE`). A "library map" is a
  filter/highlight over these rows, never a second UMAP fit.
* Full rebuilds (UMAP + clustering + c-TF-IDF labels) run in background
  jobs only — the scheduled graph-layout maintenance tick, the manual
  Rebuild button, the first-run 202 bootstrap. GET endpoints only read.
* Between rebuilds, papers that GAIN a vector are placed incrementally:
  nearest existing cluster centroid in embedding space (when genuinely
  close — :data:`INCREMENTAL_MIN_COSINE`), deterministic jitter around the
  cluster's 2-D centre. Novel papers far from every centroid stay in the
  honest Unclustered group until the next full rebuild.

This module owns the substrate constants, the shared nearest-centroid
assignment used by BOTH the in-build incremental path (`routes/graphs.py::
_build_embedding_paper_map`) and the standalone placement sweep, and
:func:`place_missing_papers` — the tail hook the vector runners and the
maintenance tick call so the substrate tracks vector arrivals without a
full re-layout.
"""

from __future__ import annotations

import hashlib
import logging
import sqlite3
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime

import numpy as np

from alma.core.db_write import write_section
from alma.core.sql_helpers import standalone_paper_sql

logger = logging.getLogger(__name__)

# The one scope the layout is ever computed/persisted for. Library views
# FILTER these rows; they never get their own fit (50-G).
SUBSTRATE_SCOPE = "corpus"

# The one cluster detail level the substrate is built at. This is ALSO the
# default the frontend sends — the two MUST stay equal, or every page visit
# silently bypasses the precomputed path (the 1.5-vs-1.0 mismatch found in the
# 2026-07-25 audit; see tasks/lessons.md "Semantic maps"). 1.5 because 1.0
# merged a coherent single-user corpus into a few mega-clusters.
SUBSTRATE_CLUSTER_RESOLUTION = 1.5

# Outlier group (I-6): papers HDBSCAN judged to be density noise are retained
# as a distinct "Unclustered" group rather than force-merged into a cluster.
OUTLIER_CLUSTER_ID = -1
OUTLIER_LABEL = "Unclustered"

# Provenance of a row's COORDINATES, so an approximation never renders as a
# computed fact. 'layout' = produced by the UMAP fit; 'interpolated' =
# approximated between rebuilds. NULL = placed before this column existed —
# genuinely unknown, and never back-filled as either (the next full rebuild
# stamps it for real).
PLACEMENT_LAYOUT = "layout"
PLACEMENT_INTERPOLATED = "interpolated"

# Absolute cosine floor — a degenerate-input guard ONLY (zero/garbage vectors),
# never the "is this paper close enough" test.
#
# It was that test until 2026-07-27, and it was a no-op: SPECTER2 is a strongly
# anisotropic space, so on the live corpus (9,737 placed papers) two papers
# picked at RANDOM have cosine p1 = 0.717 / median 0.829, and a paper's cosine
# to its nearest cluster centroid has p1 = 0.897. Measured rejection rate at
# 0.10: 0.00%. Every paper attached to whichever centroid was least-far — the
# "stays Unclustered until the next full rebuild" honesty path was unreachable.
# The real test is :data:`ADMISSION_PERCENTILE` below.
INCREMENTAL_MIN_COSINE = 0.10

# THE admission test: a paper joins cluster C only if it is at least as close to
# C's centre as C's own weakest members are — the p5 of the member→centroid
# cosine distribution, estimated from the same bounded sample the centroids come
# from. Self-calibrating (a tight cluster demands more than a diffuse one) and
# corpus-independent, so it cannot rot the way an absolute constant did.
#
# Measured on the live corpus: rejects 12% of held-out papers HDBSCAN *did*
# cluster, and 36% of the papers HDBSCAN judged density noise — a 3× separation
# where the absolute floor had exactly none.
ADMISSION_PERCENTILE = 5.0

# Percentile estimates need a sample. Below this many sampled members a cluster
# admits on the absolute floor alone rather than on a percentile of ~4 points.
MIN_ADMISSION_SAMPLE = 8

# No cluster is ever made unenterable: a handful of near-duplicate members can
# push the p5 to ~1.0, which would reject every genuine newcomer.
MAX_ADMISSION_COSINE = 0.99

# How many already-placed neighbours vote on an incremental paper's position.
# Measured against a layout whose coordinates are a known function of the
# embedding: a paper at a cluster's EDGE lands with 2% shrinkage toward the
# barycentre at k=3, 9% at k=6, 29% at k=12. Six trades a little edge accuracy
# for robustness to a single mis-embedded neighbour; twelve visibly pulls the
# rim of every cluster inward.
INTERPOLATION_NEIGHBOURS = 6

# How many member vectors to sample per cluster when estimating embedding-space
# centroids for placement. Bounds the vector I/O of a placement sweep to
# ~clusters × sample instead of the whole corpus.
_CENTROID_SAMPLE_PER_CLUSTER = 64


class SubstrateUnavailableError(RuntimeError):
    """A substrate-only assembly ran with no corpus layout to read.

    Raised instead of silently fitting a partial layout: the caller builds the
    corpus substrate first (full rebuild path) and retries.
    """


def cluster_jitter(paper_id: str, cluster_id: int, index: int) -> tuple[float, float]:
    """Deterministic tie-break offset for an incrementally placed paper.

    Hash-derived (not random) so repeated placements are stable across runs —
    the layout must never depend on call order or wall clock.

    This is a CLICKABILITY device, not a position: two near-duplicate papers
    interpolate to the same coordinate and would otherwise render as one dot.
    The radius is therefore an order of magnitude below the real within-cluster
    spread (measured p10 = 0.0029, median 0.0104 of the plate), so the nudge is
    invisible against genuine structure.

    Until 2026-07-27 the radius was 0.035–0.055 — 3–5× the MEDIAN distance a
    real UMAP-placed paper sits from its cluster centre. Because placement also
    started from the centroid, incremental papers landed on a visible ring that
    no layout ever produced: on the live corpus the radial histogram decays
    smoothly to 0.030, then holds a 677-row shelf across [0.030, 0.062) with
    only 109 rows beyond it. That shelf was ~9% of clustered rows wearing a
    fabricated position that rendered identically to a computed one.
    """
    digest = hashlib.sha1(f"{paper_id}:{cluster_id}:{index}".encode()).hexdigest()
    angle = (int(digest[:8], 16) / float(16**8)) * (2.0 * np.pi)
    radius = 0.0015 + 0.0005 * (index % 3)
    return float(np.cos(angle) * radius), float(np.sin(angle) * radius)


def cosine(a: np.ndarray, b: np.ndarray) -> float:
    na = float(np.linalg.norm(a))
    nb = float(np.linalg.norm(b))
    if na <= 0 or nb <= 0:
        return 0.0
    return float(np.dot(a, b) / (na * nb))


@dataclass(frozen=True)
class Assignment:
    """Nearest-centroid assignment with its runner-up and margin.

    ``margin = best_cos - second_cos`` is the boundary-uncertainty measure the
    Signal Lab sampler keys on (task 54 §2.1): a small margin means the paper
    sits ambiguously between two regions. ``second_id`` is ``None`` when only
    one centroid exists (margin degenerates to ``best_cos`` so a lone-centroid
    corpus still sorts sensibly). ``best_id`` is :data:`OUTLIER_CLUSTER_ID`
    when the paper is below ``min_cosine`` of every centroid — same honesty
    rule as :func:`assign_to_centroids`.
    """

    best_id: int
    best_cos: float
    second_id: int | None
    second_cos: float
    margin: float


def assign_with_margin(
    vec: np.ndarray,
    centroid_vectors: dict[int, np.ndarray],
    *,
    min_cosine: float = INCREMENTAL_MIN_COSINE,
    admission: dict[int, float] | None = None,
) -> Assignment:
    """Nearest + runner-up centroid for ``vec``, with the assignment margin.

    THE shared assignment rule — :func:`assign_to_centroids` delegates here,
    so the incremental placement path, the standalone sweep, and the Signal
    Lab boundary sampler can never disagree about where a paper belongs.

    ``admission`` is the per-cluster membership test (cluster id → minimum
    cosine, from :func:`load_placement_context`). It is OPT-IN because
    membership is a *placement* concern: the Signal Lab callers ask this
    function "which region is this nearest to, and by how much" over
    super-region centroids, and must keep getting an answer for every paper.
    Placement callers pass it; ranking callers do not.
    """
    if not centroid_vectors:
        return Assignment(OUTLIER_CLUSTER_ID, 0.0, None, 0.0, 0.0)

    # One pass, tracking best + runner-up — the sampler calls this over whole
    # region pools, so avoid the sort-everything approach.
    best_cid, best_cos = OUTLIER_CLUSTER_ID, -2.0
    second_cid: int | None = None
    second_cos = -2.0
    for cid, centroid in centroid_vectors.items():
        c = cosine(vec, centroid)
        if c > best_cos:
            second_cid, second_cos = best_cid, best_cos
            best_cid, best_cos = int(cid), c
        elif c > second_cos:
            second_cid, second_cos = int(cid), c
    if second_cid == OUTLIER_CLUSTER_ID:  # the initial sentinel, not a real runner-up
        second_cid, second_cos = None, 0.0

    # Two gates, in order: the absolute floor (degenerate vectors) and then the
    # cluster's own admission radius (the real membership test).
    floor = max(min_cosine, float((admission or {}).get(best_cid, min_cosine)))
    if best_cos < floor:
        return Assignment(OUTLIER_CLUSTER_ID, best_cos, second_cid, second_cos,
                          best_cos - second_cos if second_cid is not None else best_cos)
    return Assignment(
        best_id=best_cid,
        best_cos=best_cos,
        second_id=second_cid,
        second_cos=second_cos if second_cid is not None else 0.0,
        margin=best_cos - second_cos if second_cid is not None else best_cos,
    )


def assign_to_centroids(
    vec: np.ndarray,
    centroid_vectors: dict[int, np.ndarray],
    *,
    min_cosine: float = INCREMENTAL_MIN_COSINE,
    admission: dict[int, float] | None = None,
) -> int:
    """Nearest-centroid cluster id for ``vec``, or the Unclustered group.

    Thin wrapper over :func:`assign_with_margin` — the one assignment rule —
    kept for the placement paths that only need the winning id.
    """
    return assign_with_margin(
        vec, centroid_vectors, min_cosine=min_cosine, admission=admission
    ).best_id


def substrate_row_count(conn: sqlite3.Connection) -> int:
    """Number of papers currently placed on the substrate."""
    try:
        return int(
            conn.execute(
                "SELECT COUNT(*) FROM publication_clusters WHERE scope = ?",
                (SUBSTRATE_SCOPE,),
            ).fetchone()[0]
            or 0
        )
    except sqlite3.OperationalError:
        return 0


def load_vectors_by_id(
    conn: sqlite3.Connection, paper_ids: list[str], model: str
) -> dict[str, np.ndarray]:
    """Decode active-model vectors for exactly ``paper_ids`` (bounded IN batches)."""
    from alma.core.vector_blob import decode_vector

    out: dict[str, np.ndarray] = {}
    for start in range(0, len(paper_ids), 500):
        batch = paper_ids[start : start + 500]
        rows = conn.execute(
            f"""
            SELECT pe.paper_id, pe.embedding FROM publication_embeddings pe
            WHERE pe.model = ? AND pe.paper_id IN ({','.join('?' for _ in batch)})
            """,
            (model, *batch),
        ).fetchall()
        for row in rows:
            pid = row["paper_id"] if isinstance(row, sqlite3.Row) else row[0]
            blob = row["embedding"] if isinstance(row, sqlite3.Row) else row[1]
            if not blob:
                continue
            try:
                out[str(pid)] = np.asarray(decode_vector(blob), dtype=np.float32)
            except Exception:  # noqa: BLE001 — one bad blob must not sink the sweep
                continue
    return out


# Back-compat alias — the loader went public for the Signal Lab sampler
# (task 54 P2); internal callers migrated, external ones keep working.
_load_vectors_by_id = load_vectors_by_id


@dataclass(frozen=True)
class PlacementContext:
    """Everything an out-of-sample placement needs, from ONE pass of vector I/O.

    ``centroid_vectors`` / ``centroid_coords`` answer *which cluster*;
    ``admission`` answers *is it close enough to be a member*; ``field``
    answers *where exactly*. They are built together because they read the
    same bounded per-cluster sample — splitting them would triple the I/O.
    """

    centroid_vectors: dict[int, np.ndarray]
    centroid_coords: dict[int, tuple[float, float]]
    admission: dict[int, float]
    field: PlacementField | None


@dataclass(frozen=True)
class PlacementField:
    """The already-placed corpus, prepared for similarity-weighted placement.

    Holds L2-normalised member vectors and their 2-D coordinates so a batch of
    newcomers can be located with one matmul each instead of re-normalising the
    corpus per paper.
    """

    ids: tuple[str, ...]
    matrix: np.ndarray  # (N, D), L2-normalised rows
    coords: np.ndarray  # (N, 2)

    def locate(
        self, vec: np.ndarray, *, k: int = INTERPOLATION_NEIGHBOURS
    ) -> tuple[float, float] | None:
        """2-D position for ``vec``: similarity-weighted mean of its k nearest
        ALREADY-PLACED neighbours' coordinates.

        This is an out-of-sample approximation of what the UMAP fit would have
        done — the same thing ``umap.transform`` approximates — and it replaces
        "drop it on the cluster centroid", which threw away every bit of
        within-cluster structure and put a first-author's new preprint at the
        barycentre of a 300-paper region.

        Weights are a softmax over the top-k cosines, scaled by their OWN
        standard deviation. SPECTER2 cosines live in a narrow, corpus-dependent
        band (measured here: random pairs 0.72–0.89), so a fixed temperature
        would be either uniform or one-hot depending on the corpus; the local
        spread is the only scale that self-calibrates.

        Honest bias: a weighted mean of neighbours shrinks toward the interior
        of the neighbour set, so interpolated papers sit slightly closer to
        dense regions than a full re-fit would put them. That is why they are
        stamped :data:`PLACEMENT_INTERPOLATED` rather than passed off as layout
        output — the next full rebuild is what makes them exact.

        Returns ``None`` when the field is empty or the vector is degenerate.
        """
        if self.matrix.size == 0:
            return None
        norm = float(np.linalg.norm(vec))
        if norm <= 0:
            return None
        sims = self.matrix @ (np.asarray(vec, dtype=np.float32) / norm)
        top = min(int(k), sims.shape[0])
        idx = np.argpartition(-sims, top - 1)[:top] if top < sims.shape[0] else np.arange(top)
        s = sims[idx]
        spread = float(np.std(s))
        weights = np.exp((s - s.max()) / max(spread, 1e-3))
        total = float(weights.sum())
        if total <= 0:
            return None
        weights = weights / total
        xy = weights @ self.coords[idx]
        return float(xy[0]), float(xy[1])


def load_placement_context(
    conn: sqlite3.Connection,
    *,
    sample_per_cluster: int = _CENTROID_SAMPLE_PER_CLUSTER,
) -> PlacementContext:
    """Read the substrate once and derive every quantity placement needs.

    Embedding centroids and admission radii are estimated from a bounded
    per-cluster sample of member vectors (I/O stays ~clusters × sample); 2-D
    cluster centres are exact SQL averages. The Unclustered group (-1) is never
    a placement target and never contributes to the field — an approximate
    position must be interpolated from papers whose own position is real.
    """
    from alma.discovery.similarity import get_active_embedding_model

    model = get_active_embedding_model(conn)
    sample_ids: dict[int, list[str]] = defaultdict(list)
    sample_coords: dict[str, tuple[float, float]] = {}
    try:
        rows = conn.execute(
            """
            SELECT paper_id, cluster_id, x, y FROM publication_clusters
            WHERE scope = ? AND cluster_id >= 0
            ORDER BY cluster_id, paper_id
            """,
            (SUBSTRATE_SCOPE,),
        ).fetchall()
    except sqlite3.OperationalError:
        return PlacementContext({}, {}, {}, None)
    for row in rows:
        pid = str(row["paper_id"] if isinstance(row, sqlite3.Row) else row[0])
        cid = int(row["cluster_id"] if isinstance(row, sqlite3.Row) else row[1])
        if len(sample_ids[cid]) < sample_per_cluster:
            sample_ids[cid].append(pid)
            sample_coords[pid] = (
                float((row["x"] if isinstance(row, sqlite3.Row) else row[2]) or 0.5),
                float((row["y"] if isinstance(row, sqlite3.Row) else row[3]) or 0.5),
            )

    all_ids = [pid for ids in sample_ids.values() for pid in ids]
    vectors = load_vectors_by_id(conn, all_ids, model)

    centroid_vectors: dict[int, np.ndarray] = {}
    admission: dict[int, float] = {}
    for cid, ids in sample_ids.items():
        member_vectors = [vectors[pid] for pid in ids if pid in vectors]
        if not member_vectors:
            continue
        stacked = np.stack(member_vectors)
        centroid = np.mean(stacked, axis=0)
        centroid_vectors[cid] = centroid
        # The cluster's own admission radius: how close its WEAKEST members sit
        # to its centre. A newcomer must clear the same bar to claim membership.
        if len(member_vectors) >= MIN_ADMISSION_SAMPLE:
            member_cos = np.array([cosine(v, centroid) for v in member_vectors], dtype=np.float64)
            admission[cid] = min(
                MAX_ADMISSION_COSINE,
                float(np.percentile(member_cos, ADMISSION_PERCENTILE)),
            )

    centroid_coords: dict[int, tuple[float, float]] = {}
    coord_rows = conn.execute(
        """
        SELECT cluster_id, AVG(x), AVG(y) FROM publication_clusters
        WHERE scope = ? AND cluster_id >= 0
        GROUP BY cluster_id
        """,
        (SUBSTRATE_SCOPE,),
    ).fetchall()
    for row in coord_rows:
        cid = int(row[0])
        centroid_coords[cid] = (float(row[1] or 0.5), float(row[2] or 0.5))

    field = None
    field_ids = [pid for pid in all_ids if pid in vectors and pid in sample_coords]
    if field_ids:
        matrix = np.stack([vectors[pid] for pid in field_ids]).astype(np.float32)
        matrix /= np.clip(np.linalg.norm(matrix, axis=1, keepdims=True), 1e-9, None)
        field = PlacementField(
            ids=tuple(field_ids),
            matrix=matrix,
            coords=np.asarray([sample_coords[pid] for pid in field_ids], dtype=np.float32),
        )

    return PlacementContext(centroid_vectors, centroid_coords, admission, field)


def load_cluster_centroids(
    conn: sqlite3.Connection,
    *,
    sample_per_cluster: int = _CENTROID_SAMPLE_PER_CLUSTER,
) -> tuple[dict[int, np.ndarray], dict[int, tuple[float, float]]]:
    """Embedding-space + 2-D centroids of every real substrate cluster.

    Thin projection of :func:`load_placement_context` for callers that only
    need the centroids (super-region assembly).
    """
    ctx = load_placement_context(conn, sample_per_cluster=sample_per_cluster)
    return ctx.centroid_vectors, ctx.centroid_coords


@dataclass(frozen=True)
class Placement:
    """Where an incrementally placed paper goes, and how honestly we know it."""

    cluster_id: int
    x: float
    y: float

    @property
    def is_outlier(self) -> bool:
        return self.cluster_id == OUTLIER_CLUSTER_ID


def place_vectors(
    vectors: dict[str, np.ndarray],
    ctx: PlacementContext,
) -> dict[str, Placement]:
    """THE incremental placement rule: vectors → substrate positions.

    Membership and position are answered SEPARATELY, and that separation is the
    point:

    * **Membership** is the admission gate — does this paper look like a member
      of its nearest cluster? A "no" is an honest Unclustered stamp.
    * **Position** is interpolated from the paper's nearest already-placed
      neighbours REGARDLESS of that verdict.

    Conflating them is what the previous rule did: a rejected paper got
    ``centroid_coords.get(-1, (0.5, 0.5))`` — dead centre of the plate — so
    every paper the gate turned away would have been stacked into a fake blob
    in the middle of the map. A paper can sit in a perfectly well-determined
    spot while honestly belonging to no named region; the map should show it
    there, greyed, not exile it to the origin.

    Both the standalone sweep (:func:`place_missing_papers`) and the in-build
    incremental path in ``routes/graphs.py`` call this, so a paper cannot land
    in two different places depending on which one reached it first.
    """
    placements: dict[str, Placement] = {}
    jitter_idx: dict[int, int] = defaultdict(int)
    for paper_id in sorted(vectors):  # deterministic jitter indices
        vec = vectors[paper_id]
        cid = assign_to_centroids(vec, ctx.centroid_vectors, admission=ctx.admission)

        located = ctx.field.locate(vec) if ctx.field is not None else None
        if located is None:
            # No field (or a degenerate vector): fall back to the cluster centre,
            # and to the plate centre only when there is no cluster either.
            located = ctx.centroid_coords.get(cid, (0.5, 0.5))

        index = jitter_idx[cid]
        jitter_idx[cid] += 1
        jx, jy = cluster_jitter(paper_id, cid, index)
        placements[paper_id] = Placement(
            cluster_id=cid,
            x=min(0.98, max(0.02, located[0] + jx)),
            y=min(0.98, max(0.02, located[1] + jy)),
        )
    return placements


def find_unplaced_papers(
    conn: sqlite3.Connection,
    paper_ids: list[str] | tuple[str, ...] | None = None,
    *,
    limit: int = 1000,
) -> list[str]:
    """Corpus-standalone papers with an active-model vector but no substrate row."""
    from alma.discovery.similarity import get_active_embedding_model

    model = get_active_embedding_model(conn)
    target_clause = ""
    params: list = [model]
    ids = [str(p).strip() for p in (paper_ids or []) if str(p).strip()]
    if ids:
        target_clause = f"AND p.id IN ({','.join('?' for _ in ids)})"
        params.extend(ids)
    params.append(int(limit))
    try:
        rows = conn.execute(
            f"""
            SELECT p.id
            FROM papers p
            JOIN publication_embeddings pe ON pe.paper_id = p.id AND pe.model = ?
            WHERE {standalone_paper_sql('p')}
              {target_clause}
              AND NOT EXISTS (
                  SELECT 1 FROM publication_clusters pc
                  WHERE pc.paper_id = p.id AND pc.scope = '{SUBSTRATE_SCOPE}'
              )
            ORDER BY p.id
            LIMIT ?
            """,
            params,
        ).fetchall()
    except sqlite3.OperationalError:
        return []
    return [str(r[0]) for r in rows]


def place_missing_papers(
    conn: sqlite3.Connection,
    paper_ids: list[str] | tuple[str, ...] | None = None,
    *,
    max_batch: int = 1000,
) -> dict:
    """Place vectored-but-unplaced papers onto the substrate incrementally.

    The cheap, always-safe consistency step between full rebuilds: called by
    the vector runners after they land vectors (targeted) and by the graph
    maintenance tick (catch-up, untargeted). No-ops fast when there is
    nothing to place or no substrate yet (the first full build owns that
    case). Owns its own gated write window — callers never wrap it in a
    transaction.

    Returns ``{"placed": int, "outliers": int, "skipped": str | None}``.
    """
    candidates = find_unplaced_papers(conn, paper_ids, limit=max_batch)
    if not candidates:
        return {"placed": 0, "outliers": 0, "skipped": "no_candidates"}
    if substrate_row_count(conn) == 0:
        # No layout yet — placement has nothing to attach to; the first full
        # rebuild (202 bootstrap / maintenance tick) covers these papers.
        return {"placed": 0, "outliers": 0, "skipped": "no_substrate"}

    ctx = load_placement_context(conn)
    if not ctx.centroid_vectors:
        return {"placed": 0, "outliers": 0, "skipped": "no_centroids"}

    from alma.discovery.similarity import get_active_embedding_model

    model = get_active_embedding_model(conn)
    vectors = load_vectors_by_id(conn, candidates, model)

    now_iso = datetime.now().isoformat()
    placed = 0
    outliers = 0
    rows: list[tuple] = []
    for pid, placement in place_vectors(vectors, ctx).items():
        # Label rides along from the cluster's existing rows; the outlier group
        # keeps its fixed honest label.
        rows.append((pid, placement.cluster_id, placement.x, placement.y, now_iso))
        if placement.is_outlier:
            outliers += 1
        else:
            placed += 1

    if not rows:
        return {"placed": 0, "outliers": 0, "skipped": "no_vectors"}

    labels: dict[int, str] = {OUTLIER_CLUSTER_ID: OUTLIER_LABEL}
    for row in conn.execute(
        """
        SELECT cluster_id, MAX(label) FROM publication_clusters
        WHERE scope = ? AND cluster_id >= 0 GROUP BY cluster_id
        """,
        (SUBSTRATE_SCOPE,),
    ).fetchall():
        labels[int(row[0])] = str(row[1] or "")

    for start in range(0, len(rows), 200):
        batch = rows[start : start + 200]
        with write_section(conn, label="graph_substrate: place papers"):
            for pid, cid, x, y, ts in batch:
                conn.execute(
                    """
                    INSERT INTO publication_clusters
                        (paper_id, scope, cluster_id, label, x, y, updated_at, placement)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(paper_id, scope) DO UPDATE SET
                        cluster_id = excluded.cluster_id,
                        label = excluded.label,
                        x = excluded.x,
                        y = excluded.y,
                        updated_at = excluded.updated_at,
                        placement = excluded.placement
                    """,
                    (
                        pid,
                        SUBSTRATE_SCOPE,
                        cid,
                        labels.get(cid) or f"Cluster {cid + 1}",
                        x,
                        y,
                        ts,
                        PLACEMENT_INTERPOLATED,
                    ),
                )

    logger.info(
        "graph_substrate: placed %d paper(s) by interpolation (%d unclustered)",
        placed + outliers,
        outliers,
    )
    return {"placed": placed, "outliers": outliers, "skipped": None}
