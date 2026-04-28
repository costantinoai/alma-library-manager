"""Publication clustering using HDBSCAN on embeddings."""

import math
from dataclasses import dataclass
from typing import Optional

import numpy as np

# Optional dependency
try:
    import hdbscan as _hdbscan

    _HDBSCAN_AVAILABLE = True
except ImportError:
    _hdbscan = None
    _HDBSCAN_AVAILABLE = False


def _silhouette_optimal_k(
    vectors: np.ndarray,
    *,
    min_k: int = 2,
    max_k: Optional[int] = None,
    sample_size: int = 2000,
) -> int:
    """Pick the number of clusters that maximises the silhouette score.

    Swept over MiniBatchKMeans runs for ``k ∈ [min_k, max_k]`` so the
    caller no longer has to hand-tune a heuristic like
    ``max(2, min(8, √n))``. The silhouette is evaluated on a random
    sample when ``n`` is large so each run stays cheap.

    Returns ``min_k`` when the data is too small or every fit fails.
    """
    from sklearn.cluster import MiniBatchKMeans
    from sklearn.metrics import silhouette_score

    n = int(vectors.shape[0])
    if n < max(4, min_k + 2):
        return max(min_k, 1)

    ceiling = max_k if max_k is not None else int(round(math.sqrt(n) * 2))
    ceiling = min(ceiling, n - 1, 20)
    ceiling = max(ceiling, min_k + 1)

    best_k = min_k
    best_score = -1.0
    for k in range(min_k, ceiling + 1):
        try:
            km = MiniBatchKMeans(
                n_clusters=k,
                random_state=42,
                n_init=5,
                batch_size=min(256, max(32, n * 2)),
            )
            labels = km.fit_predict(vectors)
            if len({int(lbl) for lbl in labels}) < 2:
                continue
            score = float(
                silhouette_score(
                    vectors,
                    labels,
                    sample_size=min(n, sample_size),
                    random_state=42,
                )
            )
        except Exception:
            continue
        if score > best_score:
            best_score = score
            best_k = k
    return best_k


@dataclass
class Cluster:
    """A cluster of publications."""

    cluster_id: int
    member_keys: list[str]  # paper_id UUIDs
    label: str = ""
    centroid: Optional[list[float]] = None


def cluster_publications(
    embeddings: dict[str, list[float]],
    min_cluster_size: Optional[int] = None,
    min_samples: Optional[int] = None,
) -> list[Cluster]:
    """Run HDBSCAN clustering on publication embeddings.

    Falls back to simple k-means via sklearn if hdbscan is not installed.

    Args:
        embeddings: Map of paper_id -> embedding vector.
        min_cluster_size: Minimum cluster size for HDBSCAN.
        min_samples: Minimum samples for HDBSCAN.

    Returns:
        List of Cluster objects with member_keys populated.
    """
    if not embeddings:
        return []

    keys = list(embeddings.keys())
    vectors = np.array([embeddings[k] for k in keys], dtype=np.float32)

    # Adaptive defaults avoid collapsing medium-sized libraries into 1-2 clusters.
    n_items = len(keys)
    if min_cluster_size is None:
        min_cluster_size = max(2, min(18, int(round(math.sqrt(n_items) * 0.9))))
    if min_samples is None:
        min_samples = max(1, min(min_cluster_size - 1, min_cluster_size // 2 or 1))

    def _run_kmeans(
        vecs: np.ndarray,
        n_points: int,
        lower_bound: int = 2,
    ) -> np.ndarray:
        from sklearn.cluster import MiniBatchKMeans

        # Silhouette-driven k. Keeps the [lower_bound, 20] envelope so
        # UI-side cluster chip count stays interpretable.
        n_clusters = _silhouette_optimal_k(vecs, min_k=lower_bound, max_k=20)
        n_clusters = min(n_clusters, max(lower_bound, n_points - 1))
        kmeans = MiniBatchKMeans(
            n_clusters=n_clusters,
            random_state=42,
            n_init=5,
            batch_size=min(256, max(32, n_points * 2)),
        )
        return kmeans.fit_predict(vecs)

    if _HDBSCAN_AVAILABLE:
        clusterer = _hdbscan.HDBSCAN(
            min_cluster_size=min_cluster_size,
            min_samples=min_samples,
            metric="euclidean",
        )
        labels = clusterer.fit_predict(vectors)
        cluster_count = len({int(l) for l in labels if int(l) >= 0})

        # If HDBSCAN is too coarse, refine for a more useful paper map.
        if n_items >= 18 and cluster_count <= 2:
            labels = _run_kmeans(vectors, n_items, lower_bound=3)
    else:
        labels = _run_kmeans(vectors, n_items, lower_bound=2)

    labels = np.array(labels, dtype=np.int32)

    # Re-attach HDBSCAN noise points to nearest centroid when possible so
    # the graph has fewer unlabeled gray nodes.
    if np.any(labels == -1):
        valid_mask = labels >= 0
        if np.any(valid_mask):
            unique_valid = sorted(int(x) for x in np.unique(labels[valid_mask]))
            centroids = {
                lbl: vectors[labels == lbl].mean(axis=0)
                for lbl in unique_valid
            }
            noise_idx = np.where(labels == -1)[0]
            for idx in noise_idx:
                nearest = min(
                    unique_valid,
                    key=lambda lbl: float(np.linalg.norm(vectors[idx] - centroids[lbl])),
                )
                labels[idx] = nearest

    # Normalize labels to dense 0..N-1 for stable colors/ordering.
    unique_labels = sorted(int(x) for x in np.unique(labels) if int(x) >= 0)
    label_map = {old: new for new, old in enumerate(unique_labels)}
    labels = np.array([label_map.get(int(lbl), -1) for lbl in labels], dtype=np.int32)

    # Group by cluster label
    cluster_map: dict[int, list[str]] = {}
    for i, label in enumerate(labels):
        if label == -1:
            continue  # noise in HDBSCAN
        cluster_map.setdefault(int(label), []).append(keys[i])

    clusters = []
    for cid, members in sorted(cluster_map.items()):
        member_vecs = np.array([embeddings[k] for k in members])
        centroid = member_vecs.mean(axis=0).tolist()
        clusters.append(
            Cluster(
                cluster_id=cid,
                member_keys=members,
                centroid=centroid,
            )
        )

    return clusters


def label_clusters_tfidf(
    clusters: list[Cluster],
    texts: dict[str, str],
    top_n: int = 4,
) -> list[str]:
    """Generate cluster labels using TF-IDF keyword extraction.

    For each cluster, extracts top keywords that distinguish it from
    other clusters.

    Args:
        clusters: List of Cluster objects.
        texts: Map of paper_id -> text (title + abstract).
        top_n: Number of top keywords per cluster label.

    Returns:
        List of label strings, one per cluster.
    """
    from sklearn.feature_extraction.text import TfidfVectorizer

    if not clusters:
        return []

    # Build a "document" per cluster (concatenate member texts)
    cluster_docs = []
    for cluster in clusters:
        doc_parts = []
        for key in cluster.member_keys:
            if key in texts:
                doc_parts.append(texts[key])
        cluster_docs.append(" ".join(doc_parts) if doc_parts else "")

    vectorizer = TfidfVectorizer(
        max_features=1000,
        stop_words="english",
        max_df=0.8,
        min_df=1,
    )
    tfidf_matrix = vectorizer.fit_transform(cluster_docs)
    feature_names = vectorizer.get_feature_names_out()

    labels = []
    for i in range(len(clusters)):
        row = tfidf_matrix[i].toarray().flatten()
        top_indices = row.argsort()[-top_n:][::-1]
        keywords = [feature_names[idx] for idx in top_indices if row[idx] > 0]
        labels.append(
            ", ".join(keywords) if keywords else f"Cluster {clusters[i].cluster_id}"
        )

    return labels
