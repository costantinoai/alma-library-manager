"""Mutual k-NN neighbour graph in the embedding space (Phase 3, I-11).

The paper map's edges used to be intra-cluster CLIQUES: every pair of papers
inside an HDBSCAN cluster got an edge (O(size²) per cluster) and nothing crossed
a cluster boundary. That over-draws dense blobs, hides the real cross-topic
neighbourhoods, and asserts a relationship ("these are linked") that was really
only "these landed in the same cluster".

:func:`mutual_knn_edges` builds the principled alternative — the MUTUAL
k-nearest-neighbour graph in the 768-d SPECTER2 space. An edge ``(a, b)`` exists
iff each paper is among the other's top-k cosine neighbours. Mutuality is what
makes it honest and readable:

* it suppresses hub domination — a broadly-central paper does NOT link to
  everything, only to papers that also rank it highly;
* it is sparse (≤ k·n/2 edges) instead of the clique blow-up;
* every edge means "these two are genuinely each other's nearest work" — a claim
  about the embedding geometry, independent of the clustering.

This is the shared primitive for the semantic edge layer of both the paper map
and (later) the author network.
"""

from __future__ import annotations

import logging

import numpy as np

logger = logging.getLogger(__name__)


def mutual_knn_edges(
    embeddings: dict[str, list[float]],
    *,
    k: int = 8,
    min_similarity: float = 0.45,
) -> list[tuple[str, str, float]]:
    """Mutual k-NN edges over an id→vector map, weighted by cosine similarity.

    Args:
        embeddings: paper_id (or author_id) → embedding vector (raw; L2-norm is
            applied internally so cosine == dot).
        k: neighbours considered per node before the mutuality filter.
        min_similarity: drop neighbours below this cosine similarity, so a node
            in a sparse region simply gets fewer (or no) edges rather than being
            forced to link to weak matches.

    Returns:
        ``[(id_a, id_b, similarity), ...]`` with ``id_a < id_b`` and no
        duplicates. Empty when there are too few points or kNN is unavailable.
    """
    keys = list(embeddings.keys())
    n = len(keys)
    if n < 3:
        return []

    matrix = np.asarray([embeddings[key] for key in keys], dtype=np.float32)
    # L2-normalise so cosine similarity is a dot product (and euclidean kNN ranks
    # identically to cosine kNN) — the geometry SPECTER2 was trained for.
    norms = np.linalg.norm(matrix, axis=1, keepdims=True)
    norms[norms == 0] = 1.0
    normalized = matrix / norms

    k_eff = min(k, n - 1)
    try:
        from sklearn.neighbors import NearestNeighbors

        # +1 because the first neighbour returned is the point itself.
        nn = NearestNeighbors(n_neighbors=k_eff + 1, metric="cosine")
        nn.fit(normalized)
        distances, indices = nn.kneighbors(normalized)
    except Exception as exc:  # pragma: no cover - capability fallback
        logger.warning("mutual_knn_edges: NearestNeighbors failed (n=%d): %s", n, exc)
        return []

    # Per-node neighbour sets (self + sub-threshold matches excluded) and the
    # similarity to each kept neighbour.
    neighbours: list[set[int]] = []
    sim_to: list[dict[int, float]] = []
    for i in range(n):
        nbrs: set[int] = set()
        sims: dict[int, float] = {}
        for dist, j in zip(distances[i], indices[i]):
            j = int(j)
            if j == i:
                continue
            similarity = 1.0 - float(dist)
            if similarity < min_similarity:
                continue
            nbrs.add(j)
            sims[j] = similarity
            if len(nbrs) >= k_eff:
                break
        neighbours.append(nbrs)
        sim_to.append(sims)

    # Mutuality filter: keep (i, j) only when each ranks the other.
    edges: list[tuple[str, str, float]] = []
    seen: set[tuple[int, int]] = set()
    for i in range(n):
        for j in neighbours[i]:
            if i not in neighbours[j]:
                continue
            a, b = (i, j) if i < j else (j, i)
            if (a, b) in seen:
                continue
            seen.add((a, b))
            similarity = max(sim_to[i].get(j, 0.0), sim_to[j].get(i, 0.0))
            edges.append((keys[a], keys[b], round(float(similarity), 3)))
    return edges
