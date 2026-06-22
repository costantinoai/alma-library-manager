"""Publication clustering using the BERTopic recipe over SPECTER2 embeddings.

Two responsibilities:

* :func:`cluster_publications` — group SPECTER2 embedding vectors into
  clusters using the BERTopic pipeline:

      L2-normalise rows  →  UMAP n_components=5 (cosine)
                         →  HDBSCAN(metric=euclidean, leaf)

  L2-normalising rows makes euclidean distance rank-equivalent to
  cosine IN THE ORIGINAL 768-d space (the geometry SPECTER2 was trained
  for). UMAP then reduces to ~5-d purely as a *clustering substrate*: it
  tames the curse of dimensionality (HDBSCAN's density estimate is
  unreliable in 768-d at our 50–500-paper scale, tractable in ~5-d).
  IMPORTANT (I-8): UMAP is a NONLINEAR projection — it does NOT preserve
  that cosine equivalence "by construction", nor does it preserve
  density. Euclidean on the reduced space is therefore an *approximate*
  clustering geometry, not an exact semantic metric; treat the 2-D map
  as a visualization/clustering aid with distortion, not a faithful
  distance space. Falls back to HDBSCAN on the normalised raw vectors
  when UMAP is unavailable or N is too small for a useful reduction, and
  to silhouette-driven MiniBatchKMeans when HDBSCAN collapses or isn't
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
from dataclasses import dataclass, field
from typing import Any, Optional

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


@dataclass
class ClusteringResult:
    """Outcome of one clustering run, carrying the honesty + diagnostic
    signals the Insights "method" panel surfaces (findings I-4, I-6).

    `clusters` holds only the REAL clusters (dense ids ``0..N-1``). Points
    HDBSCAN judged to be density noise are NOT force-merged into the nearest
    cluster — the old behaviour that silently erased the outlier/uncertainty
    signal (I-6). They are listed in `outliers` and rendered by the caller as
    a distinct "Unclustered" group, never coloured as if they belonged.

    `probabilities` is HDBSCAN's per-point membership strength in ``[0, 1]``
    (``0.0`` for outliers; ``1.0`` everywhere under the k-means fallback,
    which has no soft-membership estimate). `stability` is the mean pairwise
    Adjusted Rand Index across several UMAP seeds (``None`` when not computed
    or undefined) — a value near ``1.0`` means the partition is reproducible,
    a low value means the clusters are an artefact of one random projection.
    """

    clusters: list[Cluster]
    outliers: list[str] = field(default_factory=list)
    probabilities: dict[str, float] = field(default_factory=dict)
    method: str = ""
    params: dict[str, Any] = field(default_factory=dict)
    stability: Optional[float] = None

    @property
    def n_clusters(self) -> int:
        return len(self.clusters)

    @property
    def coverage(self) -> float:
        """Fraction of points assigned to a real cluster (1 − outlier rate)."""
        clustered = sum(len(c.member_keys) for c in self.clusters)
        total = clustered + len(self.outliers)
        return (clustered / total) if total else 0.0


def measure_clustering_stability(
    vectors: np.ndarray,
    *,
    min_cluster_size: int,
    min_samples: int,
    n_seeds: int = 5,
) -> Optional[float]:
    """Mean pairwise Adjusted Rand Index of the partition across UMAP seeds.

    The clustering pipeline's only stochastic stage is the UMAP reduction
    (HDBSCAN is deterministic given a substrate). Re-reducing with different
    ``random_state`` seeds and re-clustering tells us whether the cluster
    structure is *reproducible* (ARI≈1: the data genuinely has these groups)
    or an *artefact* of one lucky projection (ARI low: treat the map as
    suggestive, not authoritative). This is a clustering-validity diagnostic,
    surfaced in the method panel — it never changes the served partition,
    which always uses the fixed ``random_state=42`` run.

    Returns ``None`` when stability is undefined/uncomputable: UMAP missing,
    too few points for a reduction, fewer than two seeds usable, or every run
    collapsed to a single label (ARI needs ≥2 partitions with structure).
    """
    if not _HDBSCAN_AVAILABLE or not _UMAP_AVAILABLE:
        return None
    n = int(vectors.shape[0])
    if n < _UMAP_MIN_POINTS or n_seeds < 2:
        return None

    from sklearn.metrics import adjusted_rand_score

    seeds = [42, 1, 7, 13, 99][:n_seeds]
    label_sets: list[np.ndarray] = []
    for seed in seeds:
        try:
            substrate = reduce_for_clustering(vectors, random_state=seed)
            clusterer = _hdbscan.HDBSCAN(
                min_cluster_size=min_cluster_size,
                min_samples=min_samples,
                metric="euclidean",
                cluster_selection_method="eom",
            )
            labels = np.asarray(clusterer.fit_predict(substrate), dtype=np.int32)
        except Exception:
            continue
        # A run that found no structure (all noise, or one cluster) carries no
        # partition information — skip it rather than letting ARI degenerate.
        if len({int(x) for x in labels if int(x) >= 0}) >= 2:
            label_sets.append(labels)

    if len(label_sets) < 2:
        return None

    scores: list[float] = []
    for i in range(len(label_sets)):
        for j in range(i + 1, len(label_sets)):
            scores.append(float(adjusted_rand_score(label_sets[i], label_sets[j])))
    return round(sum(scores) / len(scores), 4) if scores else None


def cluster_publications(
    embeddings: dict[str, list[float]],
    min_cluster_size: Optional[int] = None,
    min_samples: Optional[int] = None,
    *,
    compute_stability: bool = False,
) -> ClusteringResult:
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
      reduced space, k ∈ [2, 30] (a capability fallback only).

    Cluster COUNT is inferred, not targeted (I-5): HDBSCAN excess-of-mass
    selection with ``min_cluster_size = max(3, min(12, ⌈√n × 0.5⌉))`` returns
    the structure the data actually supports — possibly a single cluster on a
    tight corpus. We do NOT force a minimum count; manufacturing extra clusters
    to look "richer" was the old bug. The 2-d display layout is
    computed independently by the caller via ``project_embeddings``;
    both pipelines read the same L2-normalised SPECTER2 input with
    cosine UMAP, so visual proximity BROADLY agrees with cluster
    boundaries — but they are two SEPARATE nonlinear UMAP projections
    (5-d for clustering, 2-d for display), so the agreement is
    approximate, NOT exact "by construction" (I-8).

    Args:
        embeddings: Map of paper_id -> embedding vector (raw, unscaled
            SPECTER2 — normalisation happens internally).
        min_cluster_size: Minimum cluster size for HDBSCAN.
        min_samples: Minimum samples for HDBSCAN.
        compute_stability: When True, also run the seed-resampling stability
            diagnostic (mean pairwise ARI across UMAP seeds). Off by default
            because it re-fits UMAP several times — enable it on the
            background REBUILD path, not on a synchronous read.

    Returns:
        A :class:`ClusteringResult`. ``clusters`` are the real clusters only
        (dense ids, centroids on the *raw* SPECTER2 vectors so the
        incremental-layout fast path can do nearest-centroid lookups in the
        original embedding space); density-noise points are retained in
        ``outliers`` (I-6), never force-merged. ``probabilities``, ``method``,
        ``params`` and ``stability`` feed the method/uncertainty panel.
    """
    if not embeddings:
        return ClusteringResult(clusters=[])

    keys = list(embeddings.keys())
    vectors = np.array([embeddings[k] for k in keys], dtype=np.float32)

    n_items = len(keys)
    if min_cluster_size is None:
        # Finer-grained than the previous ⌈√n × 0.9⌉: for n=330 papers
        # this gives ~9 instead of ~16, which the leaf method then
        # explodes into 15-30 clusters depending on density.
        min_cluster_size = max(3, min(12, int(round(math.sqrt(n_items) * 0.5))))
    if min_samples is None:
        # min_samples is HDBSCAN's conservativeness knob: higher → more points
        # declared noise. It MUST be ≥ 2 — at min_samples=1 every point is a core
        # point and HDBSCAN can never emit noise, which silently disables the
        # outlier path entirely (I-6) and makes "coverage" meaninglessly 1.0.
        # We sit it at half the cluster size (floored at 2, capped below
        # min_cluster_size) so the density estimate is honest without over-
        # noising a small personal library.
        min_samples = max(2, min(min_cluster_size - 1, min_cluster_size // 2))

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
        # SOTA recipe (I-5): excess-of-mass ('eom') selection infers the NATURAL
        # number of clusters instead of over-splitting (the old 'leaf' method),
        # and we no longer force a minimum cluster count. If the corpus has
        # weak/no structure HDBSCAN may return one cluster (or mostly noise) —
        # that honest answer stands. The previous "≤3 clusters on n≥18 → force ≥4
        # k-means clusters" rescue manufactured structure the data didn't support.
        # prediction_data=True enables approximate_predict for the incremental path.
        clusterer = _hdbscan.HDBSCAN(
            min_cluster_size=min_cluster_size,
            min_samples=min_samples,
            metric="euclidean",
            cluster_selection_method="eom",
            prediction_data=True,
        )
        labels = clusterer.fit_predict(cluster_substrate)
        # Per-point membership strength in [0, 1] (0 for noise). Surfaced as the
        # node's clustering confidence — the uncertainty signal the old
        # force-merge destroyed (I-6). `prediction_data=True` guarantees
        # `probabilities_` is populated (length n, zeros for noise).
        point_probabilities = np.asarray(clusterer.probabilities_, dtype=np.float32)
        method = "hdbscan_eom"
    else:
        # HDBSCAN unavailable: silhouette-driven k-means is the only option (k≥2
        # only because silhouette is undefined for k<2 — a capability floor, NOT a
        # "more clusters is better" target). k-means has no soft-membership or
        # outlier notion, so every point gets probability 1.0 and none are noise.
        labels = _run_kmeans(cluster_substrate, n_items, lower_bound=2)
        point_probabilities = np.ones(n_items, dtype=np.float32)
        method = "kmeans_silhouette"

    labels = np.array(labels, dtype=np.int32)

    # I-6: RETAIN density noise as a real "unclustered" group. The old code
    # re-attached every HDBSCAN -1 point to its nearest centroid, manufacturing
    # membership the density model explicitly rejected and erasing uncertainty.
    # We now keep -1 and let the caller render those points distinctly (grey, no
    # cluster edges): an honest "we don't know where this belongs" beats a
    # confident wrong colour.

    # Normalize the REAL labels to dense 0..N-1 for stable colors/ordering;
    # -1 (noise) is preserved as -1.
    unique_labels = sorted(int(x) for x in np.unique(labels) if int(x) >= 0)
    label_map = {old: new for new, old in enumerate(unique_labels)}
    labels = np.array([label_map.get(int(lbl), -1) for lbl in labels], dtype=np.int32)

    # Group members; collect noise points and the per-paper membership strength.
    cluster_map: dict[int, list[str]] = {}
    outliers: list[str] = []
    probabilities: dict[str, float] = {}
    for i, label in enumerate(labels):
        key = keys[i]
        probabilities[key] = (
            float(point_probabilities[i]) if i < len(point_probabilities) else 0.0
        )
        if int(label) == -1:
            outliers.append(key)  # density noise — honestly unclustered
            continue
        cluster_map.setdefault(int(label), []).append(key)

    clusters: list[Cluster] = []
    for cid, members in sorted(cluster_map.items()):
        member_vecs = np.array([embeddings[k] for k in members])
        centroid = member_vecs.mean(axis=0).tolist()
        clusters.append(
            Cluster(cluster_id=cid, member_keys=members, centroid=centroid)
        )

    # Reproducibility diagnostic (opt-in; re-fits UMAP, so background-only).
    stability: Optional[float] = None
    if compute_stability:
        stability = measure_clustering_stability(
            vectors, min_cluster_size=min_cluster_size, min_samples=min_samples
        )

    return ClusteringResult(
        clusters=clusters,
        outliers=outliers,
        probabilities=probabilities,
        method=method,
        params={
            "min_cluster_size": int(min_cluster_size),
            "min_samples": int(min_samples),
            "selection": "eom" if _HDBSCAN_AVAILABLE else "silhouette",
            "reduced_dim": (
                int(cluster_substrate.shape[1]) if cluster_substrate.ndim == 2 else None
            ),
            "substrate": (
                "umap_cosine"
                if (_UMAP_AVAILABLE and n_items >= _UMAP_MIN_POINTS)
                else "l2_raw"
            ),
            "metric": "euclidean_on_l2norm",
        },
        stability=stability,
    )


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


def score_cluster_terms(
    cluster_member_texts: dict[int, list[str]],
    *,
    ngram_range: tuple[int, int] = (1, 2),
    max_features: int = 4000,
    top_k: int = 40,
) -> dict[int, list[tuple[str, float]]]:
    """Per-cluster ranked terms by PREVALENCE-WEIGHTED c-TF-IDF.

    The plain BERTopic c-TF-IDF concatenates a cluster into ONE document, so a
    term typed many times in a single verbose paper scores as highly as a term
    shared across the whole cluster — surfacing non-co-occurring vocabulary in
    cluster labels and word clouds (confirmed on the live library: big clusters
    were labelled with terms present in only 20–30 % of their papers).

    The fix: multiply each term's class-based TF-IDF (its distinctiveness vs
    other clusters) by its within-cluster PREVALENCE — the fraction of the
    cluster's papers that actually contain it. A term must be both distinctive
    AND recur across the cluster to rank, so labels read as the shared topic
    rather than one paper's idiosyncrasy. Terms confined to a single paper of a
    multi-paper cluster are dropped outright.

    Single source of truth for both :func:`label_clusters_tfidf` and the paper-
    map word clouds. Aggregates per cluster through a SPARSE membership matrix
    so it never densifies the (n_papers × n_terms) matrix — safe on the corpus.

    Returns ``{cluster_id: [(term, score), ...]}`` ranked desc, ≤ ``top_k`` each.
    """
    cids = sorted(cluster_member_texts.keys())
    if not cids:
        return {}

    # Flatten to PAPER-level docs so within-cluster document frequency is
    # measurable; remember which cluster row each paper-doc belongs to.
    paper_docs: list[str] = []
    paper_cluster_row: list[int] = []
    paper_count = {cid: 0 for cid in cids}
    for ci, cid in enumerate(cids):
        for text in cluster_member_texts[cid]:
            if text and text.strip():
                paper_docs.append(text)
                paper_cluster_row.append(ci)
                paper_count[cid] += 1
    if not paper_docs:
        return {cid: [] for cid in cids}

    from scipy.sparse import csr_matrix
    from sklearn.feature_extraction.text import CountVectorizer

    n_classes = len(cids)
    n_papers = len(paper_docs)
    # df thresholds at the PAPER level now: a term must appear in ≥2 papers
    # globally (drops hapax) once the graph is big enough to afford it.
    min_df = 2 if n_papers >= 8 else 1
    max_df = 0.95 if n_papers >= 8 else 1.0
    try:
        vectorizer = CountVectorizer(
            stop_words=_build_label_stop_words(),
            ngram_range=ngram_range,
            min_df=min_df,
            max_df=max_df,
            token_pattern=r"(?u)\b[a-zA-Z][a-zA-Z]+\b",
            lowercase=True,
            max_features=max_features,
        )
        counts = vectorizer.fit_transform(paper_docs)  # (n_papers, n_terms) sparse
    except ValueError:
        return {cid: [] for cid in cids}
    feature_names = vectorizer.get_feature_names_out()

    # Cluster aggregates via a sparse (n_classes × n_papers) membership matrix —
    # M @ counts stays sparse and only the small (n_classes × n_terms) result is
    # densified.
    membership = csr_matrix(
        (np.ones(n_papers), (paper_cluster_row, np.arange(n_papers))),
        shape=(n_classes, n_papers),
    )
    class_counts = np.asarray((membership @ counts).todense(), dtype=np.float64)
    binary = counts.copy()
    binary.data = np.ones_like(binary.data)
    class_doc_freq = np.asarray((membership @ binary).todense(), dtype=np.float64)

    # c-TF-IDF (BERTopic class formula): tf in class × log(1 + A / f_x).
    class_sizes = class_counts.sum(axis=1)
    class_sizes_safe = np.maximum(class_sizes, 1.0)
    tf = class_counts / class_sizes_safe[:, None]
    A = float(class_sizes.mean()) if float(class_sizes.sum()) > 0 else 1.0
    f_x = np.maximum(class_counts.sum(axis=0), 1.0)
    idf = np.log1p(A / f_x)
    cf_idf = tf * idf[None, :]

    # Prevalence: fraction of the cluster's papers containing the term.
    paper_n = np.array([paper_count[cid] for cid in cids], dtype=np.float64)
    prevalence = class_doc_freq / np.maximum(paper_n, 1.0)[:, None]
    score = cf_idf * prevalence

    out: dict[int, list[tuple[str, float]]] = {}
    for ci, cid in enumerate(cids):
        row = score[ci]
        df_row = class_doc_freq[ci]
        # In a real (≥4-paper) cluster a label term must recur in ≥2 papers;
        # tiny clusters keep the single-paper term.
        floor = 2.0 if paper_n[ci] >= 4 else 1.0
        ranked: list[tuple[str, float]] = []
        for idx in np.argsort(-row, kind="stable")[: top_k * 2]:
            value = float(row[idx])
            if value <= 0.0:
                break
            if df_row[idx] < floor:
                continue
            ranked.append((str(feature_names[idx]), round(value, 5)))
            if len(ranked) >= top_k:
                break
        out[cid] = ranked
    return out


def label_clusters_tfidf(
    clusters: list[Cluster],
    texts: dict[str, str],
    top_n: int = 4,
) -> list[str]:
    """Generate distinctive cluster labels via class-based TF-IDF (c-TF-IDF).

    Terms are ranked by :func:`score_cluster_terms` — the BERTopic class-based
    TF-IDF (``tf_x_in_class * log(1 + A / f_x)``) multiplied by the term's
    within-cluster PREVALENCE (fraction of the cluster's papers containing it),
    so a label term must be both *characteristic* of the cluster AND *shared*
    across its papers, not just frequent in a single verbose one.

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

    # Prevalence-weighted c-TF-IDF (shared scorer) → terms that are both
    # distinctive AND recur across the cluster's papers. Then the bigram-absorbs-
    # unigram pass turns the top terms into a phrase label.
    member_texts = {
        cluster.cluster_id: [
            texts[k] for k in cluster.member_keys if k in texts and texts[k]
        ]
        for cluster in clusters
    }
    scored = score_cluster_terms(member_texts, ngram_range=(1, 2), top_k=top_n * 4)

    labels: list[str] = []
    for cluster in clusters:
        chosen = _select_label_terms(scored.get(cluster.cluster_id, []), top_n)
        labels.append(
            ", ".join(chosen) if chosen else f"Cluster {cluster.cluster_id + 1}"
        )
    return labels
