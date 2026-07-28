"""Super-regions: the substrate's clusters agglomerated to a samplable scale.

Task 54 M0 (design: ``tasks/54_SIGNAL_LAB_LAYER_2026-07-26.md`` §2–3). The
corpus substrate carries a few hundred fine clusters — far too many to
calibrate per-region taste against a few hundred human judgments. This module
aggregates them into ≤ :data:`TARGET_SUPER_REGIONS` **super-regions** and
publishes the result as the ``graph:super_regions`` materialized view:

* **Payload is layout-derived geometry ONLY** — the cluster→region map,
  region centroids, adjacency, labels. Rings (library distance) and
  per-paper margins are deliberately NOT here: they depend on Library
  membership / round context and are computed cheaply at round time, so a
  Library save never invalidates this view (defect D-1 in the task 54
  audit). The fingerprint tracks ``publication_clusters`` alone.
* **Region identity survives re-layouts.** A full re-layout renumbers
  ``publication_clusters.cluster_id``; anything keyed on it silently
  orphans. The build therefore remaps: new region centroids are matched to
  the previous payload's by cosine (greedy, best-first, ≥
  :data:`REMAP_MIN_COSINE`) and carry the old region id forward; unmatched
  old ids are retired, listed in ``remap.retired``. Signal Lab rounds store
  ``region_version`` so the fitter can interpret historical rounds against
  the payload version they were drawn from.
* Consumers read via ``materialized_views.get_stored`` (never build inline);
  freshness is owned by the graph-layout maintenance pass, which calls
  ``materialized_views.get`` at its tail — the same ownership split as the
  other graph views (task 50 M1).

Aggregation is average-link agglomerative clustering on centroid cosine —
≤ a 327×327 problem, milliseconds in scipy — so the build's real cost is the
sampled member-vector I/O ``load_cluster_centroids`` already bounds.
"""

from __future__ import annotations

import base64
import logging
import sqlite3
from typing import Any

import numpy as np

from alma.ai.graph_versions import SUPER_REGION_VERSION, with_version
from alma.application import materialized_views as mv
from alma.application.graph_substrate import (
    SUBSTRATE_SCOPE,
    load_cluster_centroids,
)
from alma.core.vector_blob import decode_vector, encode_vector

logger = logging.getLogger(__name__)

VIEW_KEY = "graph:super_regions"

# Calibration target: enough regions to be spatially meaningful, few enough
# that ~10² judgments give every region a usable posterior (task 53 §update).
TARGET_SUPER_REGIONS = 32

# Mutual-kNN degree for region adjacency. 4 keeps the graph sparse but
# connected at K≈32; adjacency feeds the ring BFS and boundary-pair sampling.
ADJACENCY_KNN = 4

# A new region only inherits an old region's identity when their centroids
# genuinely coincide. Below this, the space moved too much — honest new id.
REMAP_MIN_COSINE = 0.90

# Data half of the fingerprint: the substrate rows this payload is derived
# from. `updated_at` moves on every placement/re-layout write; COUNT catches
# deletes. The version literal makes CODE changes rebuild too (I-4 lesson).
_FINGERPRINT_SQL = with_version(
    """
    SELECT COUNT(*), COALESCE(MAX(updated_at), ''), COALESCE(MAX(cluster_id), -1)
    FROM publication_clusters
    WHERE scope = 'corpus'
    """,
    SUPER_REGION_VERSION,
)


def _b64(vec: np.ndarray) -> str:
    """float16 blob → base64 str (JSON-safe, ~4× smaller than float lists)."""
    return base64.b64encode(encode_vector(vec)).decode("ascii")


def decode_centroid(b64: str) -> np.ndarray:
    """Inverse of the payload's centroid encoding. Shared by every consumer."""
    return decode_vector(base64.b64decode(b64.encode("ascii")))


def _cluster_masses_and_labels(
    conn: sqlite3.Connection,
) -> tuple[dict[int, int], dict[int, str]]:
    """Member count + label per real substrate cluster (one GROUP BY)."""
    masses: dict[int, int] = {}
    labels: dict[int, str] = {}
    rows = conn.execute(
        """
        SELECT cluster_id, COALESCE(MAX(label), '') AS label, COUNT(*) AS mass
        FROM publication_clusters
        WHERE scope = ? AND cluster_id >= 0
        GROUP BY cluster_id
        """,
        (SUBSTRATE_SCOPE,),
    ).fetchall()
    for row in rows:
        cid = int(row["cluster_id"])
        masses[cid] = int(row["mass"] or 0)
        labels[cid] = str(row["label"] or "")
    return masses, labels


def _agglomerate(
    cluster_ids: list[int],
    matrix: np.ndarray,
    target: int,
) -> dict[int, int]:
    """cluster_id → 0-based group index, via average-link cosine linkage.

    Fewer clusters than the target ⇒ identity grouping (each cluster is its
    own region) — no fake merging on a small corpus.
    """
    n = len(cluster_ids)
    if n <= target:
        return {cid: i for i, cid in enumerate(cluster_ids)}

    from scipy.cluster.hierarchy import fcluster, linkage

    link = linkage(matrix, method="average", metric="cosine")
    flat = fcluster(link, t=target, criterion="maxclust")  # 1-based group ids
    # Normalise to dense 0-based indices in first-seen order (stable for tests).
    remap: dict[int, int] = {}
    out: dict[int, int] = {}
    for cid, group in zip(cluster_ids, flat):
        out[cid] = remap.setdefault(int(group), len(remap))
    return out


def _mutual_knn_adjacency(centroids: dict[int, np.ndarray], k: int) -> dict[int, list[int]]:
    """Mutual-kNN edges over region centroids (cosine). Symmetric by design."""
    ids = sorted(centroids.keys())
    if len(ids) <= 1:
        return {rid: [] for rid in ids}
    stack = np.stack([centroids[rid] for rid in ids]).astype(np.float32)
    norms = np.linalg.norm(stack, axis=1, keepdims=True)
    norms[norms == 0] = 1.0
    unit = stack / norms
    sims = unit @ unit.T
    np.fill_diagonal(sims, -2.0)
    kk = min(k, len(ids) - 1)
    nearest: dict[int, set[int]] = {}
    for row_idx, rid in enumerate(ids):
        top = np.argpartition(-sims[row_idx], kk - 1)[:kk]
        nearest[rid] = {ids[j] for j in top}
    adjacency: dict[int, list[int]] = {rid: [] for rid in ids}
    for a in ids:
        for b in nearest[a]:
            if a < b and a in nearest[b]:  # mutual, add once
                adjacency[a].append(b)
                adjacency[b].append(a)
    return {rid: sorted(neighbours) for rid, neighbours in adjacency.items()}


def _carry_identities(
    fresh_centroids: dict[int, np.ndarray],
    previous: dict[str, Any] | None,
) -> tuple[dict[int, int], list[int], int]:
    """Match fresh group indices to previous region ids by centroid cosine.

    Greedy best-cosine-first, one-to-one, threshold :data:`REMAP_MIN_COSINE`.
    Returns (group_index → durable region id, retired old ids, next version).
    """
    if not previous:
        return {g: g for g in fresh_centroids}, [], 1

    old_regions = {
        int(r["id"]): decode_centroid(r["centroid_b64"])
        for r in previous.get("regions", [])
    }
    version = int(previous.get("version") or 0) + 1
    if not old_regions:
        return {g: g for g in fresh_centroids}, [], version

    pairs: list[tuple[float, int, int]] = []
    for g, vec in fresh_centroids.items():
        vn = vec / (np.linalg.norm(vec) or 1.0)
        for old_id, old_vec in old_regions.items():
            on = old_vec / (np.linalg.norm(old_vec) or 1.0)
            pairs.append((float(vn @ on), g, old_id))
    pairs.sort(reverse=True)

    assigned: dict[int, int] = {}
    used_old: set[int] = set()
    for cos, g, old_id in pairs:
        if cos < REMAP_MIN_COSINE:
            break
        if g in assigned or old_id in used_old:
            continue
        assigned[g] = old_id
        used_old.add(old_id)

    next_id = (max(old_regions.keys(), default=-1) + 1) if old_regions else 0
    for g in sorted(fresh_centroids.keys()):
        if g not in assigned:
            assigned[g] = next_id
            next_id += 1
    retired = sorted(set(old_regions.keys()) - used_old)
    return assigned, retired, version


def build_super_regions(conn: sqlite3.Connection) -> dict[str, Any]:
    """The ``graph:super_regions`` build_fn: substrate clusters → regions.

    Empty substrate ⇒ an honest empty payload (``regions: []``) — consumers
    treat it as "lab unavailable", never an error.
    """
    centroid_vectors, centroid_coords = load_cluster_centroids(conn)
    masses, labels = _cluster_masses_and_labels(conn)
    cluster_ids = sorted(centroid_vectors.keys())

    previous = None
    try:
        stored = mv.get_stored(conn, VIEW_KEY)
    except KeyError:  # registration happens at module import; belt only
        stored = None
    if stored is not None:
        previous = stored.get("payload")

    if not cluster_ids:
        _, _, version = _carry_identities({}, previous)
        return {
            "version": version,
            "regions": [],
            "adjacency": {},
            "cluster_to_region": {},
            "remap": {"carried": 0, "new": 0, "retired": []},
        }

    matrix = np.stack([centroid_vectors[cid] for cid in cluster_ids]).astype(np.float32)
    grouping = _agglomerate(cluster_ids, matrix, TARGET_SUPER_REGIONS)

    # Mass-weighted embedding + 2-D centroids per group.
    group_members: dict[int, list[int]] = {}
    for cid, g in grouping.items():
        group_members.setdefault(g, []).append(cid)

    fresh_centroids: dict[int, np.ndarray] = {}
    fresh_coords: dict[int, tuple[float, float]] = {}
    for g, members in group_members.items():
        weights = np.asarray([max(1, masses.get(cid, 1)) for cid in members], dtype=np.float32)
        vecs = np.stack([centroid_vectors[cid] for cid in members])
        fresh_centroids[g] = (vecs * weights[:, None]).sum(axis=0) / weights.sum()
        coords = np.asarray(
            [centroid_coords.get(cid, (0.5, 0.5)) for cid in members], dtype=np.float32
        )
        wx, wy = (coords * weights[:, None]).sum(axis=0) / weights.sum()
        fresh_coords[g] = (float(wx), float(wy))

    id_map, retired, version = _carry_identities(fresh_centroids, previous)

    regions: list[dict[str, Any]] = []
    cluster_to_region: dict[str, int] = {}
    for g, members in sorted(group_members.items()):
        rid = id_map[g]
        # Label: the most massive member cluster's label — honest, cheap, and
        # already human-curated by the substrate's c-TF-IDF pass.
        top_member = max(members, key=lambda cid: masses.get(cid, 0))
        regions.append(
            {
                "id": rid,
                "clusters": sorted(members),
                "label": labels.get(top_member, ""),
                "centroid_b64": _b64(fresh_centroids[g]),
                "x": fresh_coords[g][0],
                "y": fresh_coords[g][1],
                "mass": int(sum(masses.get(cid, 0) for cid in members)),
            }
        )
        for cid in members:
            cluster_to_region[str(cid)] = rid

    adjacency = _mutual_knn_adjacency(
        {id_map[g]: vec for g, vec in fresh_centroids.items()}, ADJACENCY_KNN
    )

    previous_ids = {int(r["id"]) for r in (previous or {}).get("regions", [])}
    carried = sum(1 for rid in id_map.values() if rid in previous_ids)
    return {
        "version": version,
        "regions": regions,
        "adjacency": {str(k): v for k, v in adjacency.items()},
        "cluster_to_region": cluster_to_region,
        "remap": {
            "carried": carried,
            "new": len(regions) - carried,
            "retired": retired,
        },
    }


def compute_rings(conn: sqlite3.Connection, payload: dict[str, Any]) -> dict[int, int]:
    """ring(region) — BFS distance from the Library's regions. Round-time only.

    Ring 0 = regions holding ≥1 ``status='library'`` paper; each adjacency hop
    adds 1; regions unreachable from any library region get
    ``max_ring + 1`` (they still deserve ε-exploration, never exclusion).
    Deliberately computed per call (≤40-node BFS + one GROUP BY): keeping it
    out of the stored payload is what keeps Library saves from invalidating
    the view (D-1).
    """
    cluster_to_region = {
        int(k): int(v) for k, v in (payload.get("cluster_to_region") or {}).items()
    }
    adjacency = {
        int(k): [int(x) for x in v] for k, v in (payload.get("adjacency") or {}).items()
    }
    all_regions = {int(r["id"]) for r in payload.get("regions", [])}
    if not all_regions:
        return {}

    rows = conn.execute(
        """
        SELECT DISTINCT pc.cluster_id
        FROM publication_clusters pc
        JOIN papers p ON p.id = pc.paper_id
        WHERE pc.scope = ? AND pc.cluster_id >= 0 AND p.status = 'library'
        """,
        (SUBSTRATE_SCOPE,),
    ).fetchall()
    seeds = {
        cluster_to_region[int(r[0])]
        for r in rows
        if int(r[0]) in cluster_to_region
    }

    rings: dict[int, int] = {}
    frontier = sorted(seeds)
    depth = 0
    while frontier:
        nxt: list[int] = []
        for rid in frontier:
            if rid in rings:
                continue
            rings[rid] = depth
            nxt.extend(adjacency.get(rid, []))
        frontier = sorted(set(nxt) - set(rings))
        depth += 1

    # Disconnected (or zero-library) regions: one ring beyond the farthest
    # reached, so the γ^ring prior downweights but never zeroes them.
    outer = (max(rings.values()) + 1) if rings else 0
    for rid in all_regions:
        rings.setdefault(rid, outer)
    return rings


# The Activity operation_key this view's rebuild runs under. Named here because
# the layout pass has to EXCLUDE it from `graph_build_in_flight` — it enqueues
# this build itself and would otherwise defer every map view on its own job.
OPERATION_KEY = "materialize.graph.super_regions"

mv.register(
    mv.View(
        key=VIEW_KEY,
        fingerprint_sql=_FINGERPRINT_SQL,
        build_fn=build_super_regions,
        operation_key=OPERATION_KEY,
    )
)
