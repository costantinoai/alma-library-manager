"""The semantic partition: which papers group together, read without coordinates.

This is the core-owned producer that Task 67 §14.3–14.4 (decision D24) carves
out of the map substrate. The retained learning path — Signal Lab's regions,
its sampler and its scoring terms — needs exactly two things from the corpus:

* **membership**: which cluster each embedded paper belongs to, and
* **vectors**: the papers' embeddings, so a cluster has a centroid to compare.

It never needs WHERE a paper sits on a plate. The 2-D layout (UMAP, placement
interpolation, terrain) is the map's business; the map consumes this module,
never the other way round. That direction is what lets the core build, rank
and learn with no layout rows and no map jobs.

Storage (§14.3): two tables, owned here and created from :data:`DDL` by both
the bootstrap schema and migration 40:

* ``semantic_partition_state`` — one row: ``generation`` (advanced by every
  complete publication), ``revision`` (advanced by every incremental
  assignment), model, input fingerprint, algorithm version, provenance.
* ``semantic_partition_members`` — one row per paper: cluster, label,
  ``assignment_kind`` (``fit`` from a clustering run, ``nearest_centroid``
  from incremental assignment, ``outlier``) and a nullable confidence that is
  NEVER invented — a nearest-centroid assignment has none.

Producers: the map's layout build publishes what it clustered through
:func:`publish_partition` (a complete set, one write section, generation + 1);
incremental assignment — the substrate's placement today, the core's
:func:`assign_missing_members` — records rows at the current generation.
Consumers (super-regions, Signal Lab fit / sampler / scoring terms) read
memberships here and nowhere else. No function here selects ``x``/``y`` and
none may — ``tests/test_semantic_partition_coordinate_free.py`` guards it.
"""

from __future__ import annotations

import json
import logging
import sqlite3
from collections import defaultdict
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any

import numpy as np

from alma.ai.graph_versions import LABELLING_VERSION, PARTITION_VERSION
from alma.core.db_write import commit_unless_gated, write_section
from alma.core.sql_helpers import standalone_paper_sql
from alma.core.time import utcnow_iso
from alma.core.vector_blob import decode_vector

logger = logging.getLogger(__name__)

# The one partition scope the corpus is grouped at. The map substrate aliases
# this as its ``SUBSTRATE_SCOPE`` — same rows, same meaning.
PARTITION_SCOPE = "corpus"

# Outlier group: papers the clustering judged to be density noise are kept as
# a distinct "Unclustered" group, never force-merged into a cluster. It is
# never a region and never a centroid.
OUTLIER_CLUSTER_ID = -1
OUTLIER_LABEL = "Unclustered"

# Absolute cosine floor below which a vector is degenerate rather than merely
# far: SPECTER2 is strongly anisotropic (random pairs sit near 0.8), so this
# only catches broken vectors. Real membership is the substrate's per-cluster
# admission radius, passed in as ``admission``.
INCREMENTAL_MIN_COSINE = 0.10

# Per-cluster admission: how close a cluster's WEAKEST sampled members sit to
# its centre is the bar a newcomer must clear to be called a member. The 5th
# percentile keeps one mis-embedded member from widening the gate; the cap
# stops a tiny, tight cluster from rejecting everything.
ADMISSION_PERCENTILE = 5.0
MIN_ADMISSION_SAMPLE = 8
MAX_ADMISSION_COSINE = 0.99

# The one cluster detail level the corpus is partitioned at. This is ALSO the
# default the map sends for its substrate — the two MUST stay equal, or every
# page visit silently bypasses the precomputed path (the 1.5-vs-1.0 mismatch
# found in the 2026-07-25 audit). 1.5 because 1.0 merged a coherent
# single-user corpus into a few mega-clusters.
PARTITION_RESOLUTION = 1.5

# Below this many embedded papers a density clustering says nothing; the
# partition stays honestly unbuilt.
MIN_PARTITION_PAPERS = 30

STATE_TABLE = "semantic_partition_state"
MEMBERS_TABLE = "semantic_partition_members"

# How a membership row was obtained.
ASSIGNMENT_FIT = "fit"  # a clustering run assigned it
ASSIGNMENT_NEAREST = "nearest_centroid"  # incremental, between runs
ASSIGNMENT_OUTLIER = "outlier"  # judged to belong to no cluster

# Where a published generation came from.
PROVENANCE_FIT = "clustering_run"  # the core producer clustered the corpus
PROVENANCE_LEGACY = "legacy_layout_import"  # migration 40 copied the map's layout
PROVENANCE_INCREMENTAL = "incremental_assignment"  # state created by assignment alone

# The one DDL, executed by the bootstrap schema and by migration 40.
DDL: tuple[str, ...] = (
    f"""CREATE TABLE IF NOT EXISTS {STATE_TABLE} (
        id INTEGER PRIMARY KEY CHECK (id = 1),
        generation INTEGER NOT NULL,
        revision INTEGER NOT NULL DEFAULT 0,
        model TEXT NOT NULL,
        input_fingerprint TEXT NOT NULL DEFAULT '',
        algorithm_version TEXT NOT NULL,
        computed_at TEXT NOT NULL,
        provenance TEXT NOT NULL
    )""",
    f"""CREATE TABLE IF NOT EXISTS {MEMBERS_TABLE} (
        paper_id TEXT PRIMARY KEY REFERENCES papers(id) ON DELETE CASCADE,
        generation INTEGER NOT NULL,
        cluster_id INTEGER NOT NULL,
        label TEXT NOT NULL DEFAULT '',
        assignment_kind TEXT NOT NULL,
        membership_confidence REAL
    )""",
    f"CREATE INDEX IF NOT EXISTS idx_{MEMBERS_TABLE}_cluster ON {MEMBERS_TABLE}(cluster_id)",
)


@dataclass(frozen=True)
class Member:
    """One paper's membership: which cluster, called what, known how."""

    cluster_id: int
    label: str = ""
    assignment_kind: str = ASSIGNMENT_FIT
    confidence: float | None = None


@dataclass(frozen=True)
class PartitionState:
    generation: int
    revision: int
    model: str
    input_fingerprint: str
    algorithm_version: str
    computed_at: str
    provenance: str


def read_state(conn: sqlite3.Connection) -> PartitionState | None:
    """The active partition's state, or ``None`` when nothing was ever published."""
    try:
        row = conn.execute(
            f"SELECT generation, revision, model, input_fingerprint, algorithm_version, "
            f"computed_at, provenance FROM {STATE_TABLE} WHERE id = 1"
        ).fetchone()
    except sqlite3.OperationalError:
        return None
    if row is None:
        return None
    return PartitionState(
        int(row[0]), int(row[1]), str(row[2]), str(row[3]), str(row[4]), str(row[5]), str(row[6])
    )


def embedding_set_fingerprint(conn: sqlite3.Connection, model: str) -> str:
    """What a partition was computed FROM: the active-model vector set."""
    row = conn.execute(
        "SELECT COUNT(*), COALESCE(MAX(created_at), '') FROM publication_embeddings WHERE model = ?",
        (model,),
    ).fetchone()
    return f"{model}:{int(row[0])}:{row[1]}"


def partition_fingerprint_sql() -> str:
    """Data half of any view derived from memberships (regions): moves on every
    publication and every incremental assignment, never on a layout."""
    return f"""
    SELECT COALESCE((SELECT generation FROM {STATE_TABLE} WHERE id = 1), 0),
           COALESCE((SELECT revision FROM {STATE_TABLE} WHERE id = 1), 0),
           (SELECT COUNT(*) FROM {MEMBERS_TABLE}),
           (SELECT COALESCE(MAX(cluster_id), -1) FROM {MEMBERS_TABLE})
    """


# How many member vectors to sample per cluster when estimating an
# embedding-space centroid. Bounds vector I/O to ~clusters × sample instead of
# the whole corpus; 64 is well past where the centroid estimate stops moving.
CENTROID_SAMPLE_PER_CLUSTER = 64


def load_vectors_by_id(
    conn: sqlite3.Connection, paper_ids: list[str], model: str
) -> dict[str, np.ndarray]:
    """Decode active-model vectors for exactly ``paper_ids`` (bounded IN batches).

    The canonical vector loader for learning and partition work. One bad blob
    is skipped, never fatal — a single corrupt row must not sink a sweep.
    """
    out: dict[str, np.ndarray] = {}
    for start in range(0, len(paper_ids), 500):
        batch = paper_ids[start : start + 500]
        rows = conn.execute(
            f"""
            SELECT pe.paper_id, pe.embedding FROM publication_embeddings pe
            WHERE pe.model = ? AND pe.paper_id IN ({",".join("?" for _ in batch)})
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


def load_cluster_membership_sample(
    conn: sqlite3.Connection,
    *,
    sample_per_cluster: int = CENTROID_SAMPLE_PER_CLUSTER,
) -> dict[int, list[str]]:
    """Up to ``sample_per_cluster`` member ids per real cluster, deterministic.

    Ordered by ``(cluster_id, paper_id)`` so the same corpus always yields the
    same sample — and therefore the same centroids, which is what lets a
    rebuilt region carry its identity forward. The outlier group (-1) is not a
    cluster and is never sampled. Membership only: no coordinate is read.
    """
    sample: dict[int, list[str]] = defaultdict(list)
    try:
        rows = conn.execute(
            f"""
            SELECT paper_id, cluster_id FROM {MEMBERS_TABLE}
            WHERE cluster_id >= 0
            ORDER BY cluster_id, paper_id
            """
        ).fetchall()
    except sqlite3.OperationalError:  # no partition table yet: honest empty
        return {}
    for row in rows:
        cid = int(row["cluster_id"] if isinstance(row, sqlite3.Row) else row[1])
        if len(sample[cid]) < sample_per_cluster:
            sample[cid].append(str(row["paper_id"] if isinstance(row, sqlite3.Row) else row[0]))
    return dict(sample)


def load_cluster_centroid_vectors(
    conn: sqlite3.Connection,
    *,
    sample_per_cluster: int = CENTROID_SAMPLE_PER_CLUSTER,
) -> dict[int, np.ndarray]:
    """Embedding-space centroid of every real cluster, from a bounded sample.

    A cluster with no decodable member vector has no centroid and is omitted;
    consumers treat absence as "not a region", never as the origin.
    """
    from alma.discovery.similarity import get_active_embedding_model

    sample = load_cluster_membership_sample(conn, sample_per_cluster=sample_per_cluster)
    all_ids = [pid for ids in sample.values() for pid in ids]
    vectors = load_vectors_by_id(conn, all_ids, get_active_embedding_model(conn))

    centroids: dict[int, np.ndarray] = {}
    for cid, ids in sample.items():
        member_vectors = [vectors[pid] for pid in ids if pid in vectors]
        if member_vectors:
            centroids[cid] = np.mean(np.stack(member_vectors), axis=0)
    return centroids


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
    cosine, estimated by the map substrate's placement context). It is OPT-IN
    because membership is a *placement* concern: the Signal Lab callers ask this
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
        return Assignment(
            OUTLIER_CLUSTER_ID,
            best_cos,
            second_cid,
            second_cos,
            best_cos - second_cos if second_cid is not None else best_cos,
        )
    return Assignment(
        best_id=best_cid,
        best_cos=best_cos,
        second_id=second_cid,
        second_cos=second_cos if second_cid is not None else 0.0,
        margin=best_cos - second_cos if second_cid is not None else best_cos,
    )


# ── publication ─────────────────────────────────────────────────────────────


def _write_partition(
    conn: sqlite3.Connection,
    members: Mapping[str, Member],
    *,
    generation: int,
    model: str,
    input_fingerprint: str,
    provenance: str,
    algorithm_version: str,
) -> PartitionState:
    """Replace the whole membership set and the state row. Writes only — the
    caller owns the transaction (a write section, or the migration runner)."""
    conn.execute(f"DELETE FROM {MEMBERS_TABLE}")
    conn.executemany(
        f"INSERT INTO {MEMBERS_TABLE} (paper_id, generation, cluster_id, label, assignment_kind, "
        "membership_confidence) VALUES (?, ?, ?, ?, ?, ?)",
        [
            (pid, generation, int(m.cluster_id), m.label or "", m.assignment_kind, m.confidence)
            for pid, m in members.items()
        ],
    )
    computed_at = utcnow_iso()
    conn.execute(
        f"""INSERT INTO {STATE_TABLE} (id, generation, revision, model, input_fingerprint,
                                      algorithm_version, computed_at, provenance)
            VALUES (1, ?, 0, ?, ?, ?, ?, ?)
            ON CONFLICT(id) DO UPDATE SET generation = excluded.generation, revision = 0,
                model = excluded.model, input_fingerprint = excluded.input_fingerprint,
                algorithm_version = excluded.algorithm_version,
                computed_at = excluded.computed_at, provenance = excluded.provenance""",
        (generation, model, input_fingerprint, algorithm_version, computed_at, provenance),
    )
    return PartitionState(
        generation, 0, model, input_fingerprint, algorithm_version, computed_at, provenance
    )


def publish_partition(
    conn: sqlite3.Connection,
    members: Mapping[str, Member],
    *,
    model: str,
    input_fingerprint: str,
    provenance: str,
    algorithm_version: str = PARTITION_VERSION,
) -> PartitionState:
    """Publish a COMPLETE membership set as the next generation, atomically.

    One outer write section: readers see either the previous generation or
    the new one, never a half-replaced table. Owns its own transaction, so a
    caller must not hold one (the layout build calls this after its own
    batches, not inside them).
    """
    previous = read_state(conn)
    generation = (previous.generation + 1) if previous else 1
    with write_section(conn, label="semantic_partition: publish"):
        state = _write_partition(
            conn,
            members,
            generation=generation,
            model=model,
            input_fingerprint=input_fingerprint,
            provenance=provenance,
            algorithm_version=algorithm_version,
        )
    logger.info(
        "semantic_partition: published generation %d (%d members, %s)",
        generation,
        len(members),
        provenance,
    )
    return state


def record_memberships(conn: sqlite3.Connection, members: Mapping[str, Member]) -> int:
    """Upsert memberships at the CURRENT generation and advance the revision.

    The incremental path between clustering runs. Not a write unit of its
    own: it is reached both standalone (the core catch-up job) and from inside
    the substrate's placement section, so it writes and calls
    ``commit_unless_gated`` like every shared write helper.
    """
    if not members:
        return 0
    state = read_state(conn)
    if state is None:
        # Assignment before any clustering run (a legacy DB whose layout was
        # never imported, or a corpus grown from nothing): open generation 1
        # honestly labelled as assignment-only, so consumers can see it.
        from alma.discovery.similarity import get_active_embedding_model

        model = get_active_embedding_model(conn)
        state = _write_partition(
            conn,
            {},
            generation=1,
            model=model,
            input_fingerprint=embedding_set_fingerprint(conn, model),
            provenance=PROVENANCE_INCREMENTAL,
            algorithm_version=PARTITION_VERSION,
        )
    conn.executemany(
        f"""INSERT INTO {MEMBERS_TABLE} (paper_id, generation, cluster_id, label, assignment_kind,
                                        membership_confidence)
            VALUES (?, ?, ?, ?, ?, ?)
            ON CONFLICT(paper_id) DO UPDATE SET generation = excluded.generation,
                cluster_id = excluded.cluster_id, label = excluded.label,
                assignment_kind = excluded.assignment_kind,
                membership_confidence = excluded.membership_confidence""",
        [
            (
                pid,
                state.generation,
                int(m.cluster_id),
                m.label or "",
                m.assignment_kind,
                m.confidence,
            )
            for pid, m in members.items()
        ],
    )
    conn.execute(f"UPDATE {STATE_TABLE} SET revision = revision + 1 WHERE id = 1")
    commit_unless_gated(conn, label="semantic_partition: record memberships")
    return len(members)


def forget_members(conn: sqlite3.Connection, paper_ids: list[str] | tuple[str, ...]) -> int:
    """Drop memberships whose vector is gone. WRITES ONLY — the caller owns the
    transaction (the Library's embedding invalidation runs this beside its own
    deletes, so a paper never keeps a membership derived from text it no
    longer has). Advances the revision so regions notice.
    """
    ids = [str(p) for p in paper_ids if str(p).strip()]
    if not ids:
        return 0
    removed = 0
    try:
        for start in range(0, len(ids), 400):
            chunk = ids[start : start + 400]
            removed += conn.execute(
                f"DELETE FROM {MEMBERS_TABLE} WHERE paper_id IN ({','.join('?' for _ in chunk)})",
                chunk,
            ).rowcount
        if removed:
            conn.execute(f"UPDATE {STATE_TABLE} SET revision = revision + 1 WHERE id = 1")
    except sqlite3.OperationalError:  # partition tables absent: nothing to forget
        return 0
    return removed


def prune_vectorless_members(conn: sqlite3.Connection) -> int:
    """Self-healing sweep: a membership with no active-model vector is stale.

    Covers what no hook can: vectors removed before the hook existed, and an
    embedding-model switch that deletes every vector of the old model. Found
    live 2026-09-18 — 23 members had lost their vectors while the layout table
    (cleaned by the same invalidation) had none.
    """
    from alma.discovery.similarity import get_active_embedding_model

    try:
        rows = conn.execute(
            f"""
            SELECT m.paper_id FROM {MEMBERS_TABLE} m
            WHERE NOT EXISTS (
                SELECT 1 FROM publication_embeddings pe
                WHERE pe.paper_id = m.paper_id AND pe.model = ?
            )
            """,
            (get_active_embedding_model(conn),),
        ).fetchall()
    except sqlite3.OperationalError:
        return 0
    removed = forget_members(conn, [str(r[0]) for r in rows])
    if removed:
        commit_unless_gated(conn, label="semantic_partition: prune vectorless members")
        logger.info("semantic_partition: pruned %d membership(s) with no vector", removed)
    return removed


def legacy_layout_members(conn: sqlite3.Connection) -> dict[str, Member]:
    """The map's corpus-scope layout rows as memberships (labels and outliers
    kept, coordinates dropped). Empty when there is no layout."""
    try:
        rows = conn.execute(
            """
            SELECT paper_id, cluster_id, COALESCE(label, ''), placement
            FROM publication_clusters WHERE scope = ?
            """,
            (PARTITION_SCOPE,),
        ).fetchall()
    except sqlite3.OperationalError:
        return {}
    members: dict[str, Member] = {}
    for pid, cid, label, placement in rows:
        cid = int(cid)
        if cid < 0:
            kind = ASSIGNMENT_OUTLIER
        elif str(placement or "") == "interpolated":
            kind = ASSIGNMENT_NEAREST
        else:
            kind = ASSIGNMENT_FIT
        members[str(pid)] = Member(cid, str(label), kind)
    return members


def import_legacy_layout(conn: sqlite3.Connection) -> int:
    """Publish the map's existing layout as generation 1 with legacy provenance.

    The migration's seed and the test fixtures' one-liner. A rebuild of the
    partition later carries region identities through the ordinary remap.
    """
    members = legacy_layout_members(conn)
    if not members:
        return 0
    from alma.discovery.similarity import get_active_embedding_model

    model = get_active_embedding_model(conn)
    publish_partition(
        conn,
        members,
        model=model,
        input_fingerprint=embedding_set_fingerprint(conn, model),
        provenance=PROVENANCE_LEGACY,
    )
    return len(members)


def strip_coordinates(payload: dict) -> dict:
    """A regions payload without ``x``/``y`` — what migration 40 copies."""
    out = json.loads(json.dumps(payload))
    for region in out.get("regions", []):
        region.pop("x", None)
        region.pop("y", None)
    return out


# ── incremental assignment ──────────────────────────────────────────────────


@dataclass(frozen=True)
class PartitionContext:
    """Everything nearest-centroid assignment needs, and nothing 2-D."""

    centroid_vectors: dict[int, np.ndarray]
    admission: dict[int, float]
    sample_ids: dict[int, list[str]]
    vectors: dict[str, np.ndarray]


def load_partition_context(
    conn: sqlite3.Connection,
    *,
    sample_per_cluster: int = CENTROID_SAMPLE_PER_CLUSTER,
) -> PartitionContext:
    """Centroids AND admission radii from one bounded member sample.

    The substrate's placement context builds on this (adding coordinates and
    its interpolation field), so placement admits against exactly the
    centroids the partition and its regions were built from.
    """
    from alma.discovery.similarity import get_active_embedding_model

    sample = load_cluster_membership_sample(conn, sample_per_cluster=sample_per_cluster)
    all_ids = [pid for ids in sample.values() for pid in ids]
    vectors = load_vectors_by_id(conn, all_ids, get_active_embedding_model(conn))

    centroids: dict[int, np.ndarray] = {}
    admission: dict[int, float] = {}
    for cid, ids in sample.items():
        member_vectors = [vectors[pid] for pid in ids if pid in vectors]
        if not member_vectors:
            continue
        centroid = np.mean(np.stack(member_vectors), axis=0)
        centroids[cid] = centroid
        if len(member_vectors) >= MIN_ADMISSION_SAMPLE:
            member_cos = np.array([cosine(v, centroid) for v in member_vectors], dtype=np.float64)
            admission[cid] = min(
                MAX_ADMISSION_COSINE, float(np.percentile(member_cos, ADMISSION_PERCENTILE))
            )
    return PartitionContext(centroids, admission, sample, vectors)


def assign_members(
    vectors: Mapping[str, np.ndarray],
    ctx: PartitionContext,
    labels: Mapping[int, str] | None = None,
) -> dict[str, Member]:
    """THE incremental membership rule: nearest admitted centroid or outlier.

    No confidence is recorded — a margin is not a probability, and the
    ``assignment_kind`` says exactly how the row was obtained.
    """
    out: dict[str, Member] = {}
    for pid in sorted(vectors):
        assigned = assign_with_margin(vectors[pid], ctx.centroid_vectors, admission=ctx.admission)
        cid = assigned.best_id
        if cid == OUTLIER_CLUSTER_ID:
            out[pid] = Member(cid, OUTLIER_LABEL, ASSIGNMENT_OUTLIER)
        else:
            out[pid] = Member(cid, (labels or {}).get(cid, ""), ASSIGNMENT_NEAREST)
    return out


def find_unassigned_papers(
    conn: sqlite3.Connection,
    paper_ids: list[str] | tuple[str, ...] | None = None,
    *,
    limit: int = 1000,
) -> list[str]:
    """Standalone papers with an active-model vector and no membership row."""
    from alma.discovery.similarity import get_active_embedding_model

    model = get_active_embedding_model(conn)
    ids = [str(p).strip() for p in (paper_ids or []) if str(p).strip()]
    target = f"AND p.id IN ({','.join('?' for _ in ids)})" if ids else ""
    try:
        rows = conn.execute(
            f"""
            SELECT p.id FROM papers p
            JOIN publication_embeddings pe ON pe.paper_id = p.id AND pe.model = ?
            WHERE {standalone_paper_sql("p")} {target}
              AND NOT EXISTS (SELECT 1 FROM {MEMBERS_TABLE} m WHERE m.paper_id = p.id)
            ORDER BY p.id LIMIT ?
            """,
            (model, *ids, int(limit)),
        ).fetchall()
    except sqlite3.OperationalError:
        return []
    return [str(r[0]) for r in rows]


def cluster_labels(conn: sqlite3.Connection) -> dict[int, str]:
    """Label per cluster from the memberships (one GROUP BY)."""
    try:
        rows = conn.execute(
            f"SELECT cluster_id, COALESCE(MAX(label), '') FROM {MEMBERS_TABLE} "
            "WHERE cluster_id >= 0 GROUP BY cluster_id"
        ).fetchall()
    except sqlite3.OperationalError:
        return {}
    return {int(r[0]): str(r[1] or "") for r in rows}


def assign_missing_members(
    conn: sqlite3.Connection,
    paper_ids: list[str] | tuple[str, ...] | None = None,
    *,
    max_batch: int = 1000,
) -> dict:
    """Give every vectored-but-unassigned paper a membership, without a map.

    The core's catch-up between clustering runs: the same nearest-centroid
    rule the substrate's placement uses, writing memberships only. Owns its
    write (``record_memberships``); callers never wrap it in a transaction.
    """
    candidates = find_unassigned_papers(conn, paper_ids, limit=max_batch)
    if not candidates:
        return {"assigned": 0, "outliers": 0, "skipped": "no_candidates"}
    ctx = load_partition_context(conn)
    if not ctx.centroid_vectors:
        return {"assigned": 0, "outliers": 0, "skipped": "no_partition"}
    from alma.discovery.similarity import get_active_embedding_model

    vectors = load_vectors_by_id(conn, candidates, get_active_embedding_model(conn))
    if not vectors:
        return {"assigned": 0, "outliers": 0, "skipped": "no_vectors"}
    members = assign_members(vectors, ctx, cluster_labels(conn))
    record_memberships(conn, members)
    outliers = sum(1 for m in members.values() if m.cluster_id == OUTLIER_CLUSTER_ID)
    return {"assigned": len(members) - outliers, "outliers": outliers, "skipped": None}


# ── the producer ────────────────────────────────────────────────────────────
#
# Lifted verbatim from the map route (task 67 C2): the map now imports these
# under its old names. The eligibility rule, the c-TF-IDF background and the
# clustering recipe are the core's, so a layout is a projection OF the
# partition, never the other way round.

_CLUSTER_TERM_BACKGROUND_CACHE: dict[
    tuple[str, int, str, int, str], tuple[dict[str, int], int]
] = {}


def connection_cache_identity(conn: sqlite3.Connection) -> str:
    try:
        row = conn.execute("PRAGMA database_list").fetchone()
        path = str(row["file"] if isinstance(row, sqlite3.Row) else row[2] or "")
        return path or f"memory:{id(conn)}"
    except Exception:
        return f"connection:{id(conn)}"


def load_scope_embeddings(
    conn: sqlite3.Connection,
    *,
    scope: str = "library",
) -> dict[str, list[float]]:
    """Load embeddings produced by the active model (the ONE eligibility rule).

    Vectors produced by a previously-configured model are filtered out
    at the SQL layer so every returned vector shares the same
    dimensionality.

    When scope == "library" (default), only embeddings for papers the
    user has saved to the Library are returned. scope == "corpus" returns
    every embedding regardless of paper status.
    """
    from alma.discovery.similarity import get_active_embedding_model

    active_model = get_active_embedding_model(conn)
    try:
        # A subordinate row (dedup twin / part-of component) is never a graph
        # node — it must not be a point, cluster member, centroid input, or edge
        # endpoint. Both scopes join papers and apply the shared standalone gate;
        # the corpus query in particular MUST join (it used to read
        # publication_embeddings directly, so a leftover component vector leaked
        # straight into the map).
        if scope == "library":
            rows = conn.execute(
                f"""
                SELECT pe.paper_id, pe.embedding
                FROM publication_embeddings pe
                JOIN papers p ON p.id = pe.paper_id
                WHERE pe.model = ? AND p.status = 'library'
                  AND {standalone_paper_sql("p")}
                """,
                (active_model,),
            ).fetchall()
        else:
            rows = conn.execute(
                f"""
                SELECT pe.paper_id, pe.embedding
                FROM publication_embeddings pe
                JOIN papers p ON p.id = pe.paper_id
                WHERE pe.model = ? AND {standalone_paper_sql("p")}
                """,
                (active_model,),
            ).fetchall()
    except sqlite3.OperationalError:
        return {}

    # Always decode through the canonical helper — `publication_embeddings`
    # stores float16 since commit 918e5fc, so the old struct-unpack path
    # interpreted bytes as float32 and returned half-dim garbage vectors.
    # `decode_vector` upcasts to runtime float32 and (when given an
    # `expected_dim`) auto-rescues legacy float32 rows by byte length.
    from alma.core.vector_blob import decode_vector

    embeddings: dict[str, list[float]] = {}
    for row in rows:
        if isinstance(row, sqlite3.Row):
            paper_id = row["paper_id"]
            blob = row["embedding"]
        else:
            paper_id = row[0]
            blob = row[1]
        if not blob:
            continue
        try:
            vec = decode_vector(blob)
        except Exception:
            continue
        embeddings[paper_id] = vec.tolist()
    return embeddings


def load_cluster_term_background(
    conn: sqlite3.Connection,
    *,
    max_features: int = 4000,
) -> tuple[dict[str, int], int]:
    """Return corpus-wide paper DF for the same terms the cluster labeller uses.

    The background is always the whole standalone corpus, even for a Library map:
    cluster TF/prevalence stay scoped to the rendered cluster, while IDF answers
    "is this phrase distinctive against everything ALMa knows about?"
    """
    try:
        row = conn.execute(
            f"""
            SELECT COUNT(*) AS n, COALESCE(MAX(COALESCE(p.updated_at, p.created_at, '')), '') AS watermark
            FROM papers p
            WHERE {standalone_paper_sql("p")}
            """
        ).fetchone()
        corpus_n = int(row["n"] if isinstance(row, sqlite3.Row) else row[0] or 0)
        watermark = str(row["watermark"] if isinstance(row, sqlite3.Row) else row[1] or "")
    except sqlite3.OperationalError:
        return {}, 0
    if corpus_n <= 0:
        return {}, 0

    cache_key = (
        connection_cache_identity(conn),
        corpus_n,
        watermark,
        int(max_features),
        LABELLING_VERSION,
    )
    cached = _CLUSTER_TERM_BACKGROUND_CACHE.get(cache_key)
    if cached is not None:
        return cached

    try:
        rows = conn.execute(
            f"""
            SELECT COALESCE(p.title, '') AS title, COALESCE(p.abstract, '') AS abstract
            FROM papers p
            WHERE {standalone_paper_sql("p")}
              AND (
                COALESCE(TRIM(p.title), '') <> ''
                OR COALESCE(TRIM(p.abstract), '') <> ''
              )
            """
        ).fetchall()
    except sqlite3.OperationalError:
        return {}, 0
    docs = [
        f"{row['title'] if isinstance(row, sqlite3.Row) else row[0]}. "
        f"{row['abstract'] if isinstance(row, sqlite3.Row) else row[1]}".strip()
        for row in rows
    ]
    docs = [doc for doc in docs if doc.strip()]
    if not docs:
        return {}, 0

    try:
        from sklearn.feature_extraction.text import CountVectorizer

        from alma.ai.clustering import _build_label_stop_words

        vectorizer = CountVectorizer(
            stop_words=_build_label_stop_words(),
            ngram_range=(1, 2),
            min_df=1,
            max_df=1.0,
            token_pattern=r"(?u)\b[a-zA-Z][a-zA-Z]+\b",
            lowercase=True,
            max_features=max_features,
        )
        counts = vectorizer.fit_transform(docs)
    except ValueError:
        return {}, 0

    binary = counts.copy()
    binary.data = np.ones_like(binary.data)
    df = np.asarray(binary.sum(axis=0)).ravel()
    feature_names = vectorizer.get_feature_names_out()
    result = ({str(term): int(df[idx]) for idx, term in enumerate(feature_names)}, len(docs))
    if len(_CLUSTER_TERM_BACKGROUND_CACHE) > 8:
        _CLUSTER_TERM_BACKGROUND_CACHE.clear()
    _CLUSTER_TERM_BACKGROUND_CACHE[cache_key] = result
    return result


def load_label_texts(conn: sqlite3.Connection, paper_ids: list[str]) -> dict[str, str]:
    """``title. abstract`` per paper, the text the cluster labeller scores."""
    texts = {pid: "" for pid in paper_ids}
    for start in range(0, len(paper_ids), 400):
        chunk = paper_ids[start : start + 400]
        rows = conn.execute(
            f"SELECT id, title, abstract FROM papers WHERE id IN ({','.join('?' for _ in chunk)})",
            chunk,
        ).fetchall()
        for row in rows:
            texts[str(row[0])] = f"{row[1] or ''}. {row[2] or ''}"
    return texts


@dataclass(frozen=True)
class PartitionBuild:
    """One clustering run's outcome: what the map projects and what got published."""

    clustering: Any  # alma.ai.clustering.ClusteringResult
    labels: dict[int, str]
    members: dict[str, Member]
    state: PartitionState | None  # None when the run was not published


def partition_corpus(
    conn: sqlite3.Connection,
    embeddings: Mapping[str, list[float]],
    *,
    precomputed_knn=None,
    resolution: float = PARTITION_RESOLUTION,
    compute_stability: bool = False,
    texts: Mapping[str, str] | None = None,
    publish: bool = True,
) -> PartitionBuild:
    """THE clustering run: UMAP → HDBSCAN on the cosine kNN, c-TF-IDF labels.

    One function for every caller. The core owner (:func:`build_partition`)
    publishes the result as the next generation; the map's layout build calls
    it with the kNN it shares with the 2-D projection and takes its cluster
    assignments from the returned members, so a layout is a projection OF the
    partition and never a second clustering of it. A display variant (custom
    resolution, a Library-scope map, a synchronous GET) passes
    ``publish=False`` and gets the same recipe with nothing written.

    Confidence is recorded only when the method produced a real estimate
    (HDBSCAN membership strength); the k-means fallback's 1.0 is not one.
    """
    from alma.ai.clustering import cluster_publications, label_clusters_tfidf
    from alma.discovery.similarity import get_active_embedding_model

    model = get_active_embedding_model(conn)
    # Captured BEFORE the heavy work: the generation records the inputs it
    # was computed from, not whatever landed while it ran.
    input_fingerprint = embedding_set_fingerprint(conn, model)

    clustering = cluster_publications(
        dict(embeddings),
        compute_stability=compute_stability,
        resolution=resolution,
        precomputed_knn=precomputed_knn,
    )
    background_df, background_n = load_cluster_term_background(conn)
    label_texts = texts if texts is not None else load_label_texts(conn, list(embeddings))
    generated = label_clusters_tfidf(
        clustering.clusters,
        label_texts,
        background_doc_freq=background_df,
        background_doc_count=background_n,
    )
    labels: dict[int, str] = {}
    measured = clustering.method.startswith("hdbscan")
    members: dict[str, Member] = {}
    for cluster, label in zip(clustering.clusters, generated, strict=True):
        cid = int(cluster.cluster_id)
        labels[cid] = str(label or "")
        for pid in cluster.member_keys:
            members[str(pid)] = Member(
                cid,
                labels[cid],
                ASSIGNMENT_FIT,
                clustering.probabilities.get(pid) if measured else None,
            )
    if clustering.outliers:
        labels[OUTLIER_CLUSTER_ID] = OUTLIER_LABEL
        for pid in clustering.outliers:
            members[str(pid)] = Member(OUTLIER_CLUSTER_ID, OUTLIER_LABEL, ASSIGNMENT_OUTLIER)

    state = None
    if publish:
        state = publish_partition(
            conn,
            members,
            model=model,
            input_fingerprint=input_fingerprint,
            provenance=PROVENANCE_FIT,
        )
    return PartitionBuild(clustering, labels, members, state)


def build_partition(conn: sqlite3.Connection) -> PartitionState | None:
    """Cluster the corpus and publish the next generation — no map involved.

    Heavy work runs before any write; the publish is one short section.
    Returns ``None`` when the corpus is too small to partition — an honest
    unbuilt state, not an error.
    """
    from alma.ai import accel

    embeddings = load_scope_embeddings(conn, scope=PARTITION_SCOPE)
    if len(embeddings) < MIN_PARTITION_PAPERS:
        logger.info(
            "semantic_partition: %d embedded paper(s), below %d — partition left unbuilt",
            len(embeddings),
            MIN_PARTITION_PAPERS,
        )
        return None
    return partition_corpus(
        conn, embeddings, precomputed_knn=accel.shared_cosine_knn(embeddings)
    ).state
