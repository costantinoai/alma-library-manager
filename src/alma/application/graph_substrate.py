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

# A new paper only attaches to an existing centroid when genuinely close
# (cosine ≥ this); otherwise it stays Unclustered until the next full rebuild
# (I-6/I-7) instead of being jittered into whichever centroid is least-far.
INCREMENTAL_MIN_COSINE = 0.10

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
    """Deterministic small offset around a cluster centre for placed papers.

    Hash-derived (not random) so repeated placements are stable across runs —
    the layout must never depend on call order or wall clock.
    """
    digest = hashlib.sha1(f"{paper_id}:{cluster_id}:{index}".encode()).hexdigest()
    angle = (int(digest[:8], 16) / float(16**8)) * (2.0 * np.pi)
    radius = 0.035 + 0.01 * (index % 3)
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
) -> Assignment:
    """Nearest + runner-up centroid for ``vec``, with the assignment margin.

    THE shared assignment rule — :func:`assign_to_centroids` delegates here,
    so the incremental placement path, the standalone sweep, and the Signal
    Lab boundary sampler can never disagree about where a paper belongs.
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

    if best_cos < min_cosine:
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
) -> int:
    """Nearest-centroid cluster id for ``vec``, or the Unclustered group.

    Thin wrapper over :func:`assign_with_margin` — the one assignment rule —
    kept for the placement paths that only need the winning id.
    """
    return assign_with_margin(vec, centroid_vectors, min_cosine=min_cosine).best_id


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


def load_cluster_centroids(
    conn: sqlite3.Connection,
    *,
    sample_per_cluster: int = _CENTROID_SAMPLE_PER_CLUSTER,
) -> tuple[dict[int, np.ndarray], dict[int, tuple[float, float]]]:
    """Embedding-space + 2-D centroids of every real substrate cluster.

    Embedding centroids are estimated from a bounded per-cluster sample of
    member vectors (I/O stays ~clusters × sample); 2-D centres are exact SQL
    averages. The Unclustered group (-1) is never a placement target.
    """
    from alma.discovery.similarity import get_active_embedding_model

    model = get_active_embedding_model(conn)
    sample_ids: dict[int, list[str]] = defaultdict(list)
    try:
        rows = conn.execute(
            """
            SELECT paper_id, cluster_id FROM publication_clusters
            WHERE scope = ? AND cluster_id >= 0
            ORDER BY cluster_id, paper_id
            """,
            (SUBSTRATE_SCOPE,),
        ).fetchall()
    except sqlite3.OperationalError:
        return {}, {}
    for row in rows:
        pid = str(row["paper_id"] if isinstance(row, sqlite3.Row) else row[0])
        cid = int(row["cluster_id"] if isinstance(row, sqlite3.Row) else row[1])
        if len(sample_ids[cid]) < sample_per_cluster:
            sample_ids[cid].append(pid)

    all_ids = [pid for ids in sample_ids.values() for pid in ids]
    vectors = load_vectors_by_id(conn, all_ids, model)

    centroid_vectors: dict[int, np.ndarray] = {}
    for cid, ids in sample_ids.items():
        member_vectors = [vectors[pid] for pid in ids if pid in vectors]
        if member_vectors:
            centroid_vectors[cid] = np.mean(np.stack(member_vectors), axis=0)

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
    return centroid_vectors, centroid_coords


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

    centroid_vectors, centroid_coords = load_cluster_centroids(conn)
    if not centroid_vectors:
        return {"placed": 0, "outliers": 0, "skipped": "no_centroids"}

    from alma.discovery.similarity import get_active_embedding_model

    model = get_active_embedding_model(conn)
    vectors = load_vectors_by_id(conn, candidates, model)

    now_iso = datetime.now().isoformat()
    placed = 0
    outliers = 0
    rows: list[tuple] = []
    jitter_idx: dict[int, int] = defaultdict(int)
    for pid in candidates:
        vec = vectors.get(pid)
        if vec is None:
            continue
        cid = assign_to_centroids(vec, centroid_vectors)
        cx, cy = centroid_coords.get(cid, (0.5, 0.5))
        jx, jy = cluster_jitter(pid, cid, jitter_idx[cid])
        jitter_idx[cid] += 1
        x = min(0.98, max(0.02, cx + jx))
        y = min(0.98, max(0.02, cy + jy))
        # Label rides along from the cluster's existing rows; the outlier group
        # keeps its fixed honest label.
        rows.append((pid, cid, x, y, now_iso))
        if cid == OUTLIER_CLUSTER_ID:
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
                    INSERT INTO publication_clusters (paper_id, scope, cluster_id, label, x, y, updated_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(paper_id, scope) DO UPDATE SET
                        cluster_id = excluded.cluster_id,
                        label = excluded.label,
                        x = excluded.x,
                        y = excluded.y,
                        updated_at = excluded.updated_at
                    """,
                    (pid, SUBSTRATE_SCOPE, cid, labels.get(cid) or f"Cluster {cid + 1}", x, y, ts),
                )

    logger.info(
        "graph_substrate: placed %d paper(s) incrementally (%d unclustered)",
        placed + outliers,
        outliers,
    )
    return {"placed": placed, "outliers": outliers, "skipped": None}
