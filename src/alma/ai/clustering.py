"""Publication clustering using the BERTopic recipe over SPECTER2 embeddings.

Two responsibilities:

* :func:`cluster_publications` — group SPECTER2 embedding vectors into
  clusters using the BERTopic pipeline:

      L2-normalise rows  →  UMAP n_components=5 (cosine)
                         →  HDBSCAN(metric=euclidean, leaf)

  L2-normalising before the cosine UMAP makes "euclidean on the reduced
  space" rank-equivalent to cosine in the original 768-d space, which
  is the geometry SPECTER2 was trained for. UMAP-reducing first solves
  the curse of dimensionality — HDBSCAN's density estimate is
  unreliable in 768-d at our scale (50–500 papers), but tractable in
  ~5-d. Falls back to HDBSCAN on the normalised raw vectors when UMAP
  is unavailable or N is too small for a useful reduction, and to
  silhouette-driven MiniBatchKMeans when HDBSCAN collapses or isn't
  installed.
* :func:`label_clusters_tfidf` — assign distinctive phrasal labels via
  class-based TF-IDF (the BERTopic c-TF-IDF formula). Uses (1, 2)-grams
  on top of an English + academic-domain stop-word list, and prefers
  a bigram over its constituent unigrams when both rank highly.

The labels are persisted by the caller in ``graph_cluster_labels``
(see :mod:`alma.ai.cluster_labels`); the cache key is the cluster's
member-set signature, so any membership change re-keys the labels and
forces a fresh refresh.
"""

import logging
import math
from dataclasses import dataclass
from typing import Optional

import numpy as np

logger = logging.getLogger(__name__)

# Optional dependencies
try:
    import hdbscan as _hdbscan

    _HDBSCAN_AVAILABLE = True
except ImportError:
    _hdbscan = None
    _HDBSCAN_AVAILABLE = False

try:
    import umap as _umap

    _UMAP_AVAILABLE = True
except ImportError:
    _umap = None
    _UMAP_AVAILABLE = False


# UMAP-reduce embeddings to this many dimensions before HDBSCAN.
# BERTopic uses 5; the SBERT/SPECTER2 community has converged on this
# value as the right trade-off between preserving structure and making
# density estimation tractable.
_CLUSTER_REDUCED_DIM = 5

# Below this many points, UMAP's neighbourhood estimate is too noisy to
# help — fall back to clustering on normalised raw vectors instead.
_UMAP_MIN_POINTS = 15


def _l2_normalise(vectors: np.ndarray) -> np.ndarray:
    """Row-wise L2-normalise an (n, d) matrix.

    Rationale: SPECTER2 (and most contrastive sentence embedders) are
    trained for cosine similarity; vector magnitude carries no semantic
    signal. After normalisation, ``‖a − b‖² = 2(1 − cos(a, b))``, so
    euclidean distance on the unit-norm rows is rank-equivalent to
    cosine — letting us use HDBSCAN/UMAP/kmeans's fast euclidean code
    paths while still operating in the trained geometry.
    """
    out = vectors.astype(np.float32, copy=True)
    norms = np.linalg.norm(out, axis=1, keepdims=True)
    norms[norms == 0] = 1.0
    return out / norms


def reduce_for_clustering(
    vectors: np.ndarray,
    *,
    n_components: int = _CLUSTER_REDUCED_DIM,
    random_state: int = 42,
) -> np.ndarray:
    """L2-normalise and UMAP-reduce SPECTER2 vectors for clustering.

    Returns the reduced matrix (or the L2-normalised raw matrix when
    UMAP is unavailable or the input is too small). Always returns
    float32 so downstream HDBSCAN/sklearn doesn't pay an upcast.
    """
    normalized = _l2_normalise(vectors)
    n = int(normalized.shape[0])
    if not _UMAP_AVAILABLE or n < _UMAP_MIN_POINTS:
        return normalized
    try:
        reducer = _umap.UMAP(
            n_components=min(n_components, max(2, n - 1)),
            n_neighbors=min(15, max(5, n - 1)),
            min_dist=0.0,  # tight clusters help HDBSCAN's density signal
            metric="cosine",
            random_state=random_state,
        )
        return reducer.fit_transform(normalized).astype(np.float32)
    except Exception as exc:
        logger.warning(
            "UMAP reduction for clustering failed (n=%d); falling back to "
            "normalised raw vectors: %s",
            n,
            exc,
        )
        return normalized


# Generic academic noise that an English stop-word list does not cover.
# Filtered before TF-IDF so labels read as topic phrases rather than
# the boilerplate every research abstract shares ("study", "results",
# "method", ...). Bigrams keep meaningful phrases like "language model"
# even when "model" is filtered as a unigram, because n-gram tokens are
# matched against the stop list as a unit, not term-by-term — sklearn
# only filters whole-token stop matches.
_DOMAIN_STOP_WORDS: frozenset[str] = frozenset(
    [
        # Generic research-talk
        "study", "studies", "paper", "papers", "research", "researcher",
        "researchers", "work", "works", "approach", "approaches", "method",
        "methods", "methodology", "methodologies", "framework", "frameworks",
        "system", "systems", "technique", "techniques", "application",
        "applications", "ie", "eg", "etc",
        # Verbs of presentation
        "present", "presents", "presented", "propose", "proposed", "proposes",
        "introduce", "introduced", "introduces", "describe", "described",
        "describes", "develop", "developed", "develops", "design", "designed",
        "designs", "evaluate", "evaluated", "evaluates", "investigate",
        "investigated", "investigates", "analyze", "analyzed", "analyses",
        "assess", "assessed", "assesses", "demonstrate", "demonstrated",
        "demonstrates", "examine", "examined", "examines", "explore",
        "explored", "explores",
        # Result-talk
        "result", "results", "finding", "findings", "outcome", "outcomes",
        "show", "shows", "shown", "showed", "report", "reports", "reported",
        "conclude", "concluded", "conclusion", "conclusions",
        "observation", "observations", "observe", "observed", "observes",
        "performance",
        # Hedges / fillers
        "novel", "new", "recent", "recently", "important", "significant",
        "based", "using", "used", "use", "via", "however", "moreover",
        "therefore", "additionally", "furthermore", "respectively",
        # Generic nouns that swamp clusters
        "data", "dataset", "datasets", "experiment", "experiments",
        "experimental", "analysis", "evaluation",
        # Citation / paper-structure noise
        "et", "al", "fig", "figure", "figures", "table", "tables",
        "section", "sections", "appendix", "introduction", "discussion",
        "conclusion",
        # Common modifiers that survive sklearn's English list
        "high", "low", "different", "various", "multiple", "single",
        "respective", "general", "specific",
    ]
)


def _build_label_stop_words() -> list[str]:
    """English + domain stop words for cluster labelling."""
    from sklearn.feature_extraction.text import ENGLISH_STOP_WORDS

    return sorted(ENGLISH_STOP_WORDS.union(_DOMAIN_STOP_WORDS))


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
    ceiling = min(ceiling, n - 1, 30)
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
    """Cluster publication embeddings via the BERTopic recipe.

    Pipeline:

        L2-normalise (cosine geometry — what SPECTER2 was trained for)
            →  UMAP to ~5-d (curse-of-dimensionality fix; cosine metric)
            →  HDBSCAN(metric='euclidean', cluster_selection_method='leaf')

    On the normalised input, euclidean distance is rank-equivalent to
    cosine, so HDBSCAN's fast euclidean code path operates in the right
    geometry. UMAP-reducing first turns the unreliable 768-d density
    estimate into a tractable 5-d one — the same reason BERTopic uses
    this shape.

    Falls back gracefully:
    * UMAP unavailable / N < 15 → cluster on normalised raw vectors.
    * HDBSCAN unavailable → silhouette-driven MiniBatchKMeans on the
      reduced space, k ∈ [2, 30].
    * HDBSCAN collapses to ≤ 3 clusters on a non-trivial corpus →
      same kmeans fallback so the paper map is never reduced to a few
      mega-clusters.

    Defaults aim for a *finer* granularity than the previous EOM
    pipeline: ``min_cluster_size = max(3, min(12, ⌈√n × 0.5⌉))`` plus
    leaf method typically produces 2–3× more clusters at the same N,
    which is what the user asked for. The 2-d display layout is
    computed independently by the caller via ``project_embeddings``;
    both pipelines read the same L2-normalised SPECTER2 input with
    cosine UMAP, so visual proximity now agrees with cluster
    boundaries by construction.

    Args:
        embeddings: Map of paper_id -> embedding vector (raw, unscaled
            SPECTER2 — normalisation happens internally).
        min_cluster_size: Minimum cluster size for HDBSCAN.
        min_samples: Minimum samples for HDBSCAN.

    Returns:
        List of Cluster objects with member_keys populated. Centroids
        are computed on the *raw* SPECTER2 vectors so callers (e.g.
        the incremental-layout fast path) can do nearest-centroid
        lookups in the original embedding space.
    """
    if not embeddings:
        return []

    keys = list(embeddings.keys())
    vectors = np.array([embeddings[k] for k in keys], dtype=np.float32)

    n_items = len(keys)
    if min_cluster_size is None:
        # Finer-grained than the previous ⌈√n × 0.9⌉: for n=330 papers
        # this gives ~9 instead of ~16, which the leaf method then
        # explodes into 15-30 clusters depending on density.
        min_cluster_size = max(3, min(12, int(round(math.sqrt(n_items) * 0.5))))
    if min_samples is None:
        # Lower min_samples → less density required → more granular clusters.
        # Floor at 1 so HDBSCAN doesn't refuse very small libraries.
        min_samples = max(1, min(min_cluster_size - 1, max(1, min_cluster_size // 3)))

    cluster_substrate = reduce_for_clustering(vectors)

    def _run_kmeans(
        vecs: np.ndarray,
        n_points: int,
        lower_bound: int = 2,
    ) -> np.ndarray:
        from sklearn.cluster import MiniBatchKMeans

        # Silhouette-driven k inside [lower_bound, 30]. Old ceiling of
        # 20 truncated the silhouette sweep on libraries that the data
        # actually wanted to split further.
        n_clusters = _silhouette_optimal_k(vecs, min_k=lower_bound, max_k=30)
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
            cluster_selection_method="leaf",
        )
        labels = clusterer.fit_predict(cluster_substrate)
        cluster_count = len({int(l) for l in labels if int(l) >= 0})

        # Even with leaf method, HDBSCAN sometimes refuses to split a
        # tightly-packed corpus. Fall back to silhouette-driven kmeans
        # so the paper map is never reduced to a few mega-clusters.
        if n_items >= 18 and cluster_count <= 3:
            labels = _run_kmeans(cluster_substrate, n_items, lower_bound=4)
    else:
        labels = _run_kmeans(cluster_substrate, n_items, lower_bound=2)

    labels = np.array(labels, dtype=np.int32)

    # Re-attach HDBSCAN noise points to the nearest cluster centroid so
    # the graph has fewer unlabeled gray nodes. Done in the SAME reduced
    # space the clustering ran in, so the nearest-centroid choice is
    # consistent with the cluster assignment that produced the centroids.
    if np.any(labels == -1):
        valid_mask = labels >= 0
        if np.any(valid_mask):
            unique_valid = sorted(int(x) for x in np.unique(labels[valid_mask]))
            centroids = {
                lbl: cluster_substrate[labels == lbl].mean(axis=0)
                for lbl in unique_valid
            }
            noise_idx = np.where(labels == -1)[0]
            for idx in noise_idx:
                nearest = min(
                    unique_valid,
                    key=lambda lbl: float(
                        np.linalg.norm(cluster_substrate[idx] - centroids[lbl])
                    ),
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


def _select_label_terms(
    scored: list[tuple[str, float]],
    top_n: int,
) -> list[str]:
    """Pick top-N terms with bigram-prefers-unigram dedup.

    A bigram absorbs its constituent unigrams: if "visual" was picked
    first and "visual cortex" is then candidate, "visual" is dropped
    so the label reads as a phrase rather than a redundant pair. A
    unigram already covered by an earlier bigram is skipped.

    ``scored`` is expected sorted by score descending (and stable-tie-
    broken so the output is deterministic).
    """
    chosen: list[str] = []
    bigram_tokens: set[str] = set()  # tokens carried by any chosen bigram
    chosen_lower: set[str] = set()

    for term, score in scored:
        if score <= 0.0:
            continue
        tokens = term.split()
        is_bigram = len(tokens) == 2
        t_lower = term.lower()

        if t_lower in chosen_lower:
            continue

        if is_bigram:
            # Drop any chosen unigram that this bigram absorbs.
            survivors = [c for c in chosen if c.lower() not in tokens]
            chosen = survivors
            chosen.append(term)
            chosen_lower = {c.lower() for c in chosen}
            bigram_tokens.update(tokens)
        else:
            if t_lower in bigram_tokens:
                continue
            chosen.append(term)
            chosen_lower.add(t_lower)

        if len(chosen) >= top_n:
            break

    return chosen


def label_clusters_tfidf(
    clusters: list[Cluster],
    texts: dict[str, str],
    top_n: int = 4,
) -> list[str]:
    """Generate distinctive cluster labels via class-based TF-IDF (c-TF-IDF).

    For each cluster, scores (1, 2)-gram terms by their frequency in
    that cluster (treated as a single class) weighted by inverse class
    frequency across all clusters, following the BERTopic formula

    ``c-TF-IDF_x_in_class = tf_x_in_class * log(1 + A / f_x)``

    where ``A`` is the average per-class word count and ``f_x`` is the
    total frequency of term ``x`` across all class documents. This
    favours terms that are *characteristic* of a single cluster over
    terms that are merely frequent in it.

    The vocabulary is restricted to alpha tokens (length ≥ 2), with
    sklearn's English stop-words plus a domain stop-list (``study``,
    ``method``, ``result``, …) removed. Bigrams that absorb their
    constituent unigrams are preferred so labels read as phrases
    (``"visual cortex"``) rather than bag-of-keywords
    (``"visual, cortex"``).

    Args:
        clusters: List of Cluster objects.
        texts: Map of paper_id -> text (title + abstract).
        top_n: Maximum number of phrases per cluster label.

    Returns:
        List of label strings, one per cluster, in the same order as
        ``clusters``. An empty cluster, an empty document, or a
        vocabulary that fully drops to stop-words yields a placeholder
        label of the form ``"Cluster <id+1>"``.
    """
    if not clusters:
        return []

    from sklearn.feature_extraction.text import CountVectorizer

    # 1) One "class document" per cluster.
    cluster_docs: list[str] = []
    for cluster in clusters:
        parts = [
            texts[k]
            for k in cluster.member_keys
            if k in texts and texts[k]
        ]
        cluster_docs.append(" ".join(parts) if parts else "")

    if not any(doc.strip() for doc in cluster_docs):
        return [f"Cluster {c.cluster_id + 1}" for c in clusters]

    n_classes = len(cluster_docs)

    # min_df=1 on tiny graphs avoids "vocabulary is empty" — bigrams in a
    # 3-cluster graph will mostly be hapax. max_df=0.95 strips terms that
    # appear in nearly every cluster (boilerplate that survived the
    # stop-list); on tiny graphs we relax it.
    min_df = 2 if n_classes >= 5 else 1
    max_df = 0.95 if n_classes >= 4 else 1.0

    try:
        vectorizer = CountVectorizer(
            stop_words=_build_label_stop_words(),
            ngram_range=(1, 2),
            min_df=min_df,
            max_df=max_df,
            token_pattern=r"(?u)\b[a-zA-Z][a-zA-Z]+\b",
            lowercase=True,
            max_features=4000,
        )
        counts = vectorizer.fit_transform(cluster_docs)
    except ValueError:
        # Empty vocabulary after filtering — fall back to placeholders.
        return [f"Cluster {c.cluster_id + 1}" for c in clusters]

    counts_arr = counts.toarray().astype(np.float64)
    feature_names = vectorizer.get_feature_names_out()

    # 2) c-TF-IDF — BERTopic class-based formula. ``A`` is the average
    # number of tokens per class document; ``f_x`` is the total
    # frequency of term ``x`` across all classes. ``log(1 + A/f_x)``
    # rewards terms used heavily in one class but rare in the corpus.
    class_sizes = counts_arr.sum(axis=1)  # (n_classes,)
    class_sizes_safe = np.maximum(class_sizes, 1.0)
    tf = counts_arr / class_sizes_safe[:, None]  # (n_classes, n_terms)

    A = float(class_sizes.mean()) if float(class_sizes.sum()) > 0 else 1.0
    f_x = counts_arr.sum(axis=0)  # (n_terms,)
    f_x_safe = np.maximum(f_x, 1.0)
    idf = np.log1p(A / f_x_safe)  # (n_terms,)

    cf_idf = tf * idf[None, :]  # (n_classes, n_terms)

    labels: list[str] = []
    for class_idx in range(n_classes):
        row = cf_idf[class_idx]
        # Stable sort by score descending. Ties break by feature index,
        # which is alphabetical (sklearn vocab order) → deterministic.
        sorted_idx = np.argsort(-row, kind="stable")
        # Oversample top_n*4 candidates so the bigram-vs-unigram
        # absorption logic has room to drop redundant picks.
        candidates: list[tuple[str, float]] = []
        for idx in sorted_idx[: top_n * 4]:
            score = float(row[idx])
            if score <= 0.0:
                break
            candidates.append((str(feature_names[idx]), score))

        chosen = _select_label_terms(candidates, top_n)
        labels.append(
            ", ".join(chosen)
            if chosen
            else f"Cluster {clusters[class_idx].cluster_id + 1}"
        )

    return labels
