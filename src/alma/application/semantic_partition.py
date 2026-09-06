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

Schema note (forward-only, per CLAUDE.md): membership is read from
``publication_clusters`` (``scope='corpus'``) today, because that is the
current schema. When the dedicated ``semantic_partition_members`` table lands
(§14.3), :func:`load_cluster_membership_sample` is the ONE read that moves;
every consumer above it stays unchanged. No function here selects ``x``/``y``
and none may — ``tests/test_semantic_partition_coordinate_free.py`` guards it.
"""

from __future__ import annotations

import sqlite3
from collections import defaultdict
from dataclasses import dataclass

import numpy as np

from alma.core.vector_blob import decode_vector

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
            """
            SELECT paper_id, cluster_id FROM publication_clusters
            WHERE scope = ? AND cluster_id >= 0
            ORDER BY cluster_id, paper_id
            """,
            (PARTITION_SCOPE,),
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
