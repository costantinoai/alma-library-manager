"""TF-IDF and semantic similarity computation for publication discovery.

Uses scikit-learn's TfidfVectorizer and cosine_similarity to find
publications that are textually similar to a user's liked papers.
Also provides topic-overlap scoring for multi-signal recommendations.

Dense semantic similarity reads cached vectors from ``publication_embeddings``.
Heavy vector computation is triggered explicitly through Activity-backed AI
jobs, not from read/scoring helpers.
"""

from __future__ import annotations

import logging
import re
import sqlite3
from collections import Counter
from dataclasses import dataclass, field
from typing import Any, Dict, Iterable, List, Optional, Tuple

logger = logging.getLogger(__name__)

_SCHOLARLY_STOPWORDS = {
    "about", "after", "also", "among", "analysis", "approach", "around", "based", "because",
    "between", "brief", "can", "could", "data", "dataset", "datasets", "different", "during",
    "each", "effect", "effects", "find", "findings", "from", "have", "into", "more", "most",
    "paper", "papers", "results", "result", "show", "shows", "study", "their", "there", "these",
    "this", "those", "using", "used", "use", "very", "were", "what", "when", "where", "which",
    "while", "with", "within", "without", "your",
}

_SEMANTIC_CALIBRATION_POINTS: tuple[tuple[float, float], ...] = (
    (0.0, 0.0),
    (0.03, 0.07),
    (0.08, 0.18),
    (0.14, 0.31),
    (0.22, 0.47),
    (0.32, 0.65),
    (0.45, 0.82),
    (0.60, 0.92),
    (0.78, 0.98),
    (1.0, 1.0),
)

_LEXICAL_CALIBRATION_POINTS: tuple[tuple[float, float], ...] = (
    (0.0, 0.0),
    (0.03, 0.04),
    (0.08, 0.11),
    (0.15, 0.24),
    (0.24, 0.42),
    (0.34, 0.60),
    (0.48, 0.79),
    (0.64, 0.92),
    (0.82, 0.98),
    (1.0, 1.0),
)


@dataclass
class LexicalProfile:
    """Precomputed lexical similarity profile for one scoring run."""

    positive_texts: List[str]
    negative_texts: List[str] = field(default_factory=list)
    word_vectorizer: Any = None
    word_positive_matrix: Any = None
    word_negative_matrix: Any = None
    char_vectorizer: Any = None
    char_positive_matrix: Any = None
    char_negative_matrix: Any = None
    positive_terms: Counter = field(default_factory=Counter)
    negative_terms: Counter = field(default_factory=Counter)

try:
    from sklearn.feature_extraction.text import TfidfVectorizer
    from sklearn.metrics.pairwise import cosine_similarity

    _SKLEARN_AVAILABLE = True
except ImportError:
    _SKLEARN_AVAILABLE = False
    logger.warning(
        "scikit-learn is not installed. TF-IDF similarity features are disabled. "
        "Install with: pip install scikit-learn"
    )

try:
    import numpy as np

    _NUMPY_AVAILABLE = True
except ImportError:
    np = None  # type: ignore[assignment]
    _NUMPY_AVAILABLE = False


def is_available() -> bool:
    """Return True if scikit-learn is installed and similarity features work."""
    return _SKLEARN_AVAILABLE


def has_active_embeddings(conn: sqlite3.Connection, *, min_count: int = 1) -> bool:
    """Return True when cached vectors exist for the active model.

    This is intentionally independent of local embedding-provider availability:
    API-sourced Semantic Scholar/SPECTER2 vectors are useful for ranking even
    when local SPECTER2 dependencies are not installed.
    """
    if not _NUMPY_AVAILABLE:
        return False
    try:
        active_model = get_active_embedding_model(conn)
        row = conn.execute(
            "SELECT COUNT(*) AS c FROM publication_embeddings WHERE model = ?",
            (active_model,),
        ).fetchone()
        count = int((row["c"] if row else 0) or 0)
        return count >= max(1, int(min_count or 1))
    except sqlite3.OperationalError:
        return False


def compute_tfidf_vectors(texts: List[str]):
    """Compute TF-IDF vectors from a list of texts.

    Each text is typically a concatenation of a publication's title and abstract.

    Args:
        texts: List of text strings to vectorize.

    Returns:
        Sparse TF-IDF matrix (n_documents x n_features), or None if
        scikit-learn is unavailable or the input is empty.
    """
    if not _SKLEARN_AVAILABLE:
        logger.warning("scikit-learn not available; cannot compute TF-IDF vectors")
        return None

    if not texts:
        return None

    # Filter out empty/whitespace-only texts by replacing with a placeholder
    # so matrix dimensions stay consistent with input indices.
    cleaned = [t.strip() if t and t.strip() else "empty" for t in texts]

    try:
        vectorizer = TfidfVectorizer(
            max_features=5000,
            stop_words="english",
            min_df=1,
            max_df=0.95,
            sublinear_tf=True,
        )
        tfidf_matrix = vectorizer.fit_transform(cleaned)
        return tfidf_matrix
    except ValueError as exc:
        # Raised when all documents are empty or contain only stop words
        logger.debug("TF-IDF vectorization failed: %s", exc)
        return None


def _tokenize_lexical_terms(text: str) -> list[str]:
    return [tok for tok in re.split(r"[^a-z0-9]+", str(text or "").lower()) if len(tok) >= 3]


def _extract_scholarly_terms(*parts: str, limit: int = 10) -> list[str]:
    weighted_terms: Counter = Counter()
    for idx, part in enumerate(parts):
        raw = str(part or "").strip().lower()
        if not raw:
            continue
        tokens = [
            token
            for token in re.split(r"[^a-z0-9]+", raw)
            if len(token) >= 4 and token not in _SCHOLARLY_STOPWORDS
        ]
        multiplier = 2 if idx == 0 else 1
        weighted_terms.update({token: multiplier for token in tokens})
        if len(tokens) >= 2:
            bigrams = []
            for left, right in zip(tokens, tokens[1:]):
                if left == right:
                    continue
                bigram = f"{left} {right}"
                if any(term in _SCHOLARLY_STOPWORDS for term in (left, right)):
                    continue
                bigrams.append(bigram)
            weighted_terms.update({term: multiplier + 1 for term in bigrams})
    return [
        term
        for term, _count in weighted_terms.most_common(max(1, int(limit or 10)))
    ]


def _format_similarity_facets(title: str, values: Iterable[str], *, label: str, limit: int = 8) -> str:
    items = [str(value or "").strip() for value in values if str(value or "").strip()]
    if not items:
        return ""
    unique: list[str] = []
    seen: set[str] = set()
    for item in items:
        lowered = item.lower()
        if lowered in seen:
            continue
        seen.add(lowered)
        unique.append(item)
        if len(unique) >= max(1, int(limit or 8)):
            break
    if not unique:
        return ""
    emphasis = f"{title}. " if title else ""
    return f"{emphasis}{label}: {', '.join(unique)}."


def _interpolate_similarity(raw_score: float, points: tuple[tuple[float, float], ...]) -> float:
    if raw_score <= points[0][0]:
        return points[0][1]
    for (x0, y0), (x1, y1) in zip(points, points[1:]):
        if raw_score <= x1:
            if x1 <= x0:
                return y1
            ratio = (raw_score - x0) / (x1 - x0)
            return y0 + ((y1 - y0) * ratio)
    return points[-1][1]


def _extract_weighted_terms(text: str) -> Counter:
    tokens = _tokenize_lexical_terms(text)
    weighted = Counter(tokens)
    if len(tokens) >= 2:
        weighted.update({" ".join(pair): 2 for pair in zip(tokens, tokens[1:])})
    return weighted


def build_lexical_profile(
    positive_texts: List[str],
    negative_texts: Optional[List[str]] = None,
) -> Optional[LexicalProfile]:
    """Precompute lexical scorers once for a full candidate-scoring pass."""
    positives = [str(text or "").strip() for text in positive_texts if str(text or "").strip()]
    negatives = [str(text or "").strip() for text in (negative_texts or []) if str(text or "").strip()]
    if not positives:
        return None

    profile = LexicalProfile(positive_texts=positives, negative_texts=negatives)
    for text in positives:
        profile.positive_terms.update(_extract_weighted_terms(text))
    for text in negatives:
        profile.negative_terms.update(_extract_weighted_terms(text))

    if not _SKLEARN_AVAILABLE:
        return profile

    corpus = positives + negatives
    try:
        word_vectorizer = TfidfVectorizer(
            max_features=7000,
            stop_words="english",
            min_df=1,
            max_df=0.98,
            sublinear_tf=True,
            ngram_range=(1, 2),
        )
        word_matrix = word_vectorizer.fit_transform(corpus)
        profile.word_vectorizer = word_vectorizer
        profile.word_positive_matrix = word_matrix[: len(positives)]
        profile.word_negative_matrix = word_matrix[len(positives) :] if negatives else None
    except ValueError as exc:
        logger.debug("Word lexical profile failed: %s", exc)

    try:
        char_vectorizer = TfidfVectorizer(
            analyzer="char_wb",
            ngram_range=(3, 5),
            max_features=9000,
            min_df=1,
            sublinear_tf=True,
        )
        char_matrix = char_vectorizer.fit_transform(corpus)
        profile.char_vectorizer = char_vectorizer
        profile.char_positive_matrix = char_matrix[: len(positives)]
        profile.char_negative_matrix = char_matrix[len(positives) :] if negatives else None
    except ValueError as exc:
        logger.debug("Character lexical profile failed: %s", exc)

    return profile


def find_similar_publications(
    liked_texts: List[str],
    candidate_texts: List[str],
    threshold: float = 0.15,
    top_n: int = 20,
) -> List[Tuple[int, float]]:
    """Find candidate publications similar to liked publications using TF-IDF cosine similarity.

    Args:
        liked_texts: Text (title + abstract) for each liked publication.
        candidate_texts: Text (title + abstract) for each candidate publication.
        threshold: Minimum cosine similarity score to include a candidate.
        top_n: Maximum number of similar candidates to return.

    Returns:
        List of (candidate_index, score) tuples sorted by score descending.
        Returns an empty list if scikit-learn is not available, inputs are empty,
        or no candidates meet the threshold.
    """
    if not _SKLEARN_AVAILABLE:
        logger.warning("scikit-learn not available; similarity search disabled")
        return []

    if not liked_texts or not candidate_texts:
        return []

    # Combine all texts so the vectorizer learns a shared vocabulary
    all_texts = liked_texts + candidate_texts
    n_liked = len(liked_texts)

    tfidf_matrix = compute_tfidf_vectors(all_texts)
    if tfidf_matrix is None:
        return []

    liked_vectors = tfidf_matrix[:n_liked]
    candidate_vectors = tfidf_matrix[n_liked:]

    # Compute cosine similarity between each candidate and all liked papers
    sim_matrix = cosine_similarity(candidate_vectors, liked_vectors)

    # For each candidate, take the maximum similarity to any liked paper
    results: List[Tuple[int, float]] = []
    for cand_idx in range(sim_matrix.shape[0]):
        max_score = float(sim_matrix[cand_idx].max())
        if max_score >= threshold:
            results.append((cand_idx, max_score))

    # Sort by score descending and limit to top_n
    results.sort(key=lambda x: x[1], reverse=True)
    return results[:top_n]


def compute_topic_overlap(
    user_topics: Dict[str, float],
    paper_topics: List[Dict[str, float]],
    *,
    conn: Optional[sqlite3.Connection] = None,
    user_topic_embeddings: Optional[Dict[str, Optional["numpy.ndarray"]]] = None,
) -> float:
    """Compute weighted topic overlap between user preferences and a paper.

    When an embedding provider is available (via ``conn``), unmatched topics
    are compared semantically so that e.g. "Machine Learning" and "Deep
    Learning" still contribute a partial score.

    **Hot-path optimization (D-AUDIT-10, 2026-04-24).** When called inside
    `refresh_lens_recommendations` — once per candidate across potentially
    hundreds of candidates — every candidate re-embedded every user-topic
    term on every call. Even with the module-level LRU cache, the
    `_get_topic_embedding` round-trip per `(term, ut)` pair cost ~0.5 ms
    each, and at O(candidates × unmatched_topics × user_topics) it
    dominated the scoring loop wall time on real libraries. Callers in
    the scoring loop must now **pre-compute user-topic embeddings once
    per refresh** and pass them in via `user_topic_embeddings`; the
    inner loop then does a cheap dict lookup instead of another cache
    probe. Callers outside the scoring loop (tests, ad-hoc scoring)
    can still omit the kwarg and pay the legacy path — the module cache
    keeps it bearable at low call volume.

    Args:
        user_topics: Mapping of ``{topic_name: avg_rating_weight}``.  Positive
            weights indicate preferred topics, negative weights indicate topics
            the user dislikes.
        paper_topics: List of dicts with ``term`` (str) and ``score`` (float)
            representing the paper's topics and their relevance scores.
        conn: Optional DB connection for checking embedding provider availability.
        user_topic_embeddings: Optional pre-computed ``{ut: embedding}``
            dict from a single shared caller-side pass. When supplied,
            the semantic fallback skips `_get_topic_embedding(provider,
            ut)` on every iteration.

    Returns:
        A score between -1 and 1.  0.0 is returned when no overlap exists or
        inputs are empty.
    """
    if not user_topics or not paper_topics:
        return 0.0

    score = 0.0
    max_possible = 0.0
    unmatched: list[tuple[str, float]] = []  # (term, relevance)

    for t in paper_topics:
        term = (t.get("term") or "").strip().lower()
        relevance = t.get("score", 0.5) or 0.5
        if term in user_topics:
            weight = user_topics[term]
            score += weight * relevance
            max_possible += abs(weight) * relevance
        elif term:
            unmatched.append((term, relevance))

    # Semantic fallback for unmatched terms.
    #
    # Hot-path vectorisation (2026-04-26 evening): the previous version did a
    # nested Python loop — for every unmatched paper-term, scan every
    # user-topic and call `_cosine_similarity_np`. cProfile of one full lens
    # refresh recorded 4.3 M calls into `_cosine_similarity_np` and 8.6 M
    # calls into `numpy.linalg.norm`, ~30 s of pure overhead per refresh on
    # a real library.
    #
    # Replace with one matmul against a stacked, pre-normalised user-topic
    # matrix per candidate. Build the (n_user, dim) matrix once per refresh
    # by caching it on `user_topic_embeddings` itself (the dict already lives
    # for the duration of the refresh). Each unmatched term then becomes a
    # single dot product into that matrix, and the argmax-with-threshold
    # logic is preserved.
    if unmatched and conn is not None:
        try:
            from alma.ai.providers import get_active_provider
            provider = get_active_provider(conn)
            if provider is not None:
                local_ut_embs: Dict[str, Optional["numpy.ndarray"]]
                if user_topic_embeddings is not None:
                    local_ut_embs = user_topic_embeddings
                else:
                    local_ut_embs = {
                        ut: _get_topic_embedding(provider, ut)
                        for ut in user_topics
                    }
                stacked = _get_or_build_user_topic_matrix(local_ut_embs, user_topics)
                if stacked is not None:
                    ut_matrix, ut_weights = stacked
                    for term, relevance in unmatched:
                        term_emb = _get_topic_embedding(provider, term)
                        if term_emb is None:
                            continue
                        # Cosine vs every user-topic in one matmul.
                        term_norm = float(np.linalg.norm(term_emb))
                        if term_norm <= 0.0:
                            continue
                        sims = (ut_matrix @ term_emb) / term_norm
                        best_idx = int(np.argmax(sims))
                        best_sim = float(sims[best_idx])
                        if best_sim >= 0.6:
                            best_weight = ut_weights[best_idx]
                            semantic_match = (best_sim - 0.6) / 0.4  # 0→1
                            score += best_weight * relevance * semantic_match
                            max_possible += abs(best_weight) * relevance * semantic_match
        except Exception:
            pass

    return score / max_possible if max_possible > 0 else 0.0


def _get_or_build_user_topic_matrix(
    user_topic_embeddings: Dict[str, Optional["numpy.ndarray"]],
    user_topics: Dict[str, float],
) -> Optional[tuple["numpy.ndarray", "numpy.ndarray"]]:
    """Return a pre-normalised stacked matrix of user-topic embeddings.

    Cached on the `user_topic_embeddings` dict itself via a magic key so
    subsequent candidates in the same refresh reuse the same matrix
    instead of rebuilding. Returns `(matrix (n, dim) row-normalised,
    weights (n,))` or `None` if no usable embeddings were found.
    """
    if not _NUMPY_AVAILABLE or user_topic_embeddings is None or user_topics is None:
        return None
    cache_key = "__matrix_cache__"
    cached = user_topic_embeddings.get(cache_key) if isinstance(user_topic_embeddings, dict) else None
    if isinstance(cached, tuple) and len(cached) == 2:
        return cached  # type: ignore[return-value]

    rows: list["numpy.ndarray"] = []
    weights: list[float] = []
    for ut, weight in user_topics.items():
        emb = user_topic_embeddings.get(ut)
        if emb is None:
            continue
        norm = float(np.linalg.norm(emb))
        if norm <= 0.0:
            continue
        rows.append(np.asarray(emb, dtype=np.float32) / norm)
        weights.append(float(weight))
    if not rows:
        out = None
    else:
        out = (np.stack(rows, axis=0), np.asarray(weights, dtype=np.float32))
    try:
        user_topic_embeddings[cache_key] = out  # type: ignore[assignment]
    except Exception:
        pass
    return out


# Module-level cache for topic term embeddings with size limit.
_topic_embedding_cache: Dict[str, Optional["numpy.ndarray"]] = {}
_TOPIC_CACHE_MAX_SIZE = 500


def _get_topic_embedding(provider, term: str) -> Optional["numpy.ndarray"]:
    """Get or compute embedding for a topic term, using module-level cache."""
    if term in _topic_embedding_cache:
        return _topic_embedding_cache[term]
    # Evict oldest entries when cache is full
    if len(_topic_embedding_cache) >= _TOPIC_CACHE_MAX_SIZE:
        # Remove first ~20% of entries (dict preserves insertion order)
        to_remove = list(_topic_embedding_cache.keys())[: _TOPIC_CACHE_MAX_SIZE // 5]
        for k in to_remove:
            del _topic_embedding_cache[k]
    try:
        embeddings = provider.embed([term])
        if embeddings and embeddings[0]:
            emb = np.array(embeddings[0], dtype=np.float32)
            _topic_embedding_cache[term] = emb
            return emb
    except Exception:
        pass
    _topic_embedding_cache[term] = None
    return None


class SpecterEmbedder:
    """Lazy-loading local SPECTER2 embedder using a named adapter."""

    _instance: Optional[SpecterEmbedder] = None
    _model = None
    _tokenizer = None
    _torch = None
    _device: str = "cpu"
    MODEL_NAME = "allenai/specter2_base"
    ADAPTER_NAME = "allenai/specter2"
    ADHOC_QUERY_ADAPTER_NAME = "allenai/specter2_adhoc_query"
    EMBEDDING_DIM = 768

    def __init__(
        self,
        model_name: str = "allenai/specter2_base",
        embedding_dim: int = 768,
        max_length: int = 512,
        adapter_name: str = "allenai/specter2",
        adapter_key: str = "specter2",
    ) -> None:
        self._model_name = model_name
        self._embedding_dim = embedding_dim
        self._max_length = max_length
        self._adapter_name = adapter_name
        self._adapter_key = adapter_key

    @classmethod
    def get_instance(
        cls,
        model_name: str = "allenai/specter2_base",
        embedding_dim: int = 768,
        max_length: int = 512,
        adapter_name: str = "allenai/specter2",
        adapter_key: str = "specter2",
    ) -> SpecterEmbedder:
        """Return the singleton SpecterEmbedder, recreating if config changed."""
        if (
            cls._instance is None
            or cls._instance._model_name != model_name
            or cls._instance._adapter_name != adapter_name
            or cls._instance._adapter_key != adapter_key
            or cls._instance._max_length != max_length
        ):
            if cls._instance is not None:
                logger.info(
                    "SPECTER2 config changed from %s/%s to %s/%s; reloading",
                    cls._instance._model_name,
                    cls._instance._adapter_name,
                    model_name,
                    adapter_name,
                )
                cls._instance._model = None
                cls._instance._tokenizer = None
            cls._instance = cls(
                model_name=model_name,
                embedding_dim=embedding_dim,
                max_length=max_length,
                adapter_name=adapter_name,
                adapter_key=adapter_key,
            )
            cls.MODEL_NAME = model_name
            cls.EMBEDDING_DIM = embedding_dim
        return cls._instance

    def _resolve_device(self) -> str:
        """Prefer NVIDIA CUDA when available; otherwise use CPU."""
        try:
            import torch

            if torch.cuda.is_available():
                return "cuda"
        except Exception:
            pass
        return "cpu"

    def _load_model(self):
        """Load SPECTER2 base and the configured adapter on first use."""
        if self._model is None or self._tokenizer is None:
            try:
                import torch
                from adapters import AutoAdapterModel
                from transformers import AutoTokenizer
            except ImportError as exc:
                raise RuntimeError(
                    "Local SPECTER2 requires `adapters`, `transformers`, `torch`, and `numpy`. "
                    "Install `adapters` in the selected AI environment and retry."
                ) from exc

            if not _NUMPY_AVAILABLE:
                raise RuntimeError("Local SPECTER2 requires numpy")

            device = self._resolve_device()
            self._device = device
            self._torch = torch
            self._tokenizer = AutoTokenizer.from_pretrained(self._model_name)
            self._model = AutoAdapterModel.from_pretrained(self._model_name)
            try:
                self._model.load_adapter(
                    self._adapter_name,
                    source="hf",
                    load_as=self._adapter_key,
                    set_active=True,
                )
            except TypeError:
                self._model.load_adapter(
                    self._adapter_name,
                    load_as=self._adapter_key,
                    set_active=True,
                )
            try:
                self._model.set_active_adapters(self._adapter_key)
            except Exception:
                pass
            self._model.to(device)
            self._model.eval()
        return self._model, self._tokenizer, self._torch

    @property
    def device(self) -> str:
        """Return the currently selected compute device."""
        _ = self._load_model()
        return self._device

    def encode(self, texts: List[str]) -> "numpy.ndarray":
        """Encode texts to SPECTER2 vectors using [CLS] pooling."""
        model, tokenizer, torch = self._load_model()
        cleaned = [str(text or "").strip() or "empty" for text in texts]
        batch = tokenizer(
            cleaned,
            padding=True,
            truncation=True,
            max_length=self._max_length,
            return_tensors="pt",
        )
        batch = {key: value.to(self._device) for key, value in batch.items()}
        with torch.no_grad():
            outputs = model(**batch)
            vectors = outputs.last_hidden_state[:, 0, :].detach().cpu().numpy()
        return np.asarray(vectors, dtype=np.float32)

    def encode_single(self, text: str) -> "numpy.ndarray":
        """Encode a single text string."""
        return self.encode([text])[0]


def _normalize_embedding_vector(vec: Optional["numpy.ndarray"]) -> Optional["numpy.ndarray"]:
    if vec is None:
        return None
    try:
        norm = float(np.linalg.norm(vec))
    except Exception:
        return None
    if norm <= 0.0:
        return None
    return (vec / norm).astype(np.float32)


def prepare_text_specter2(title: str, abstract: str) -> str:
    """Build the canonical SPECTER2 input string.

    Per the SPECTER2 HF README, the model expects
    ``title + tokenizer.sep_token + abstract``. We hard-code ``[SEP]``
    because every SPECTER2 base model is BERT-tokenizer-based and uses
    that exact sep token; the truncation to 512 tokens happens inside
    the encoder. Using ``prepare_text``'s enriched format here was
    causing local vectors to land at ~0.99 cosine to S2-downloaded
    vectors (probe 2026-04-26) instead of the ~1.0 we want when both
    sources share one canonical model key.
    """
    title = (title or "").strip()
    abstract = (abstract or "").strip()
    if not title and not abstract:
        return ""
    return f"{title}[SEP]{abstract}"


def prepare_text(
    title: str,
    abstract: str,
    *,
    keywords: Optional[List[str]] = None,
    topics: Optional[List[str]] = None,
    max_tokens: int = 256,
    query_prefix: str = "",
    is_query: bool = False,
) -> str:
    """Prepare publication text for embedding with model-aware enrichment.

    Strategies applied:
    1. Title repetition — title appears twice for emphasis.
    2. Metadata enrichment — keywords and topics appended when available.
    3. Instruction prefix — provider-specific query hints when configured.
    4. Token-aware truncation — approximate char limit from max_tokens.
    """
    title = (title or "").strip()
    abstract = (abstract or "").strip()

    # ~4 chars per token for English, minus headroom for tokenizer overhead
    prefix = query_prefix if is_query and query_prefix else ""
    char_budget = max_tokens * 4 - 20 - len(prefix)

    parts: list[str] = []
    if title:
        parts.append(f"{title}. {title}.")
    if abstract:
        parts.append(abstract)
    if keywords:
        kw_str = ", ".join(kw for kw in keywords if kw)
        if kw_str:
            parts.append(f"Keywords: {kw_str}.")
    if topics:
        topic_str = ", ".join(t for t in topics if t)
        if topic_str:
            parts.append(f"Topics: {topic_str}.")

    text = " ".join(parts).strip()
    if len(text) > char_budget:
        text = text[:char_budget].rsplit(" ", 1)[0]

    return f"{prefix}{text}" if prefix else text


def prepare_query_text(
    query: str,
    *,
    max_tokens: int = 256,
    query_prefix: str = "",
) -> str:
    """Prepare a search query for embedding with optional instruction prefix."""
    query = (query or "").strip()
    prefix = query_prefix
    char_budget = max_tokens * 4 - 20 - len(prefix)
    if len(query) > char_budget:
        query = query[:char_budget].rsplit(" ", 1)[0]
    return f"{prefix}{query}" if prefix else query


def _split_author_names(authors_raw: object, *, limit: int = 4) -> list[str]:
    authors = [part.strip() for part in str(authors_raw or "").split(",") if part and part.strip()]
    return authors[: max(1, int(limit or 4))]


def _load_publication_topic_terms(
    conn: Optional[sqlite3.Connection],
    paper_id: str,
    *,
    limit: int = 8,
) -> list[str]:
    if conn is None or not paper_id:
        return []
    try:
        rows = conn.execute(
            """
            SELECT COALESCE(t.canonical_name, pt.term, '') AS topic_name, COALESCE(pt.score, 0.0) AS topic_score
            FROM publication_topics pt
            LEFT JOIN topics t ON t.topic_id = pt.topic_id
            WHERE pt.paper_id = ?
            ORDER BY topic_score DESC, topic_name ASC
            LIMIT ?
            """,
            (paper_id, max(1, int(limit or 8))),
        ).fetchall()
    except sqlite3.OperationalError:
        return []
    return [str(row["topic_name"] or "").strip() for row in rows if str(row["topic_name"] or "").strip()]


def build_similarity_text(
    pub: dict,
    *,
    conn: Optional[sqlite3.Connection] = None,
    paper_topics: Optional[List[Dict[str, float]]] = None,
    max_tokens: int = 320,
) -> str:
    """Build a richer scholarly text representation for similarity.

    This is intentionally denser than plain ``title + abstract``. It keeps the
    abstract as the core semantic document, then appends concise metadata that
    often carries domain signal in academic papers: topics, keywords,
    venue/journal, authors, and work type when available.
    """
    title = str(pub.get("title") or "").strip()
    abstract = str(pub.get("abstract") or "").strip()
    paper_id = str(pub.get("id") or pub.get("paper_id") or "").strip()

    topic_terms: list[str] = []
    if paper_topics:
        for item in paper_topics:
            term = str((item or {}).get("term") or "").strip()
            if term:
                topic_terms.append(term)
    if not topic_terms and paper_id:
        topic_terms = _load_publication_topic_terms(conn, paper_id)

    keyword_terms = [str(term).strip() for term in (pub.get("keywords") or []) if str(term).strip()]
    authors = _split_author_names(pub.get("authors"))
    journal = str(pub.get("journal") or pub.get("published_journal") or "").strip()
    work_type = str(pub.get("work_type") or "").strip()
    publication_date = str(pub.get("publication_date") or "").strip()
    terminology_terms = _extract_scholarly_terms(title, abstract, limit=10)

    base = prepare_text(
        title,
        abstract,
        keywords=keyword_terms[:8] or None,
        topics=topic_terms[:8] or None,
        max_tokens=max_tokens,
    )

    extras: list[str] = []
    if topic_terms:
        extras.append(_format_similarity_facets(title, topic_terms, label="Research areas", limit=8))
    if keyword_terms:
        extras.append(_format_similarity_facets("", keyword_terms, label="Keywords", limit=8))
    if terminology_terms:
        extras.append(_format_similarity_facets(title, terminology_terms, label="Terminology", limit=10))
    if authors:
        extras.append(_format_similarity_facets("", authors, label="Authors", limit=4))
    if journal:
        extras.append(f"{title}. Venue: {journal}.")
    if work_type:
        extras.append(f"Study type: {work_type}.")
    if publication_date:
        extras.append(f"Publication date: {publication_date}.")

    full_text = " ".join(part for part in [base, *extras] if part).strip()
    if len(full_text) > max_tokens * 5:
        full_text = full_text[: max_tokens * 5].rsplit(" ", 1)[0]
    return full_text


def calibrate_similarity_score(raw_score: float, *, mode: str = "semantic") -> float:
    """Calibrate scholarly similarity scores into a more usable 0..1 range.

    Academic cosine similarities are often compressed into low-looking values.
    A mild power transform preserves ordering while making mid-strength matches
    visible enough to matter in the ranker and UI.
    """
    try:
        score = float(raw_score)
    except (TypeError, ValueError):
        return 0.0
    score = max(0.0, min(1.0, score))
    points = _SEMANTIC_CALIBRATION_POINTS if mode == "semantic" else _LEXICAL_CALIBRATION_POINTS
    calibrated = _interpolate_similarity(score, points)
    return float(max(0.0, min(1.0, calibrated)))


# ---------------------------------------------------------------------------
# Embedding cache
# ---------------------------------------------------------------------------


def get_active_embedding_model(conn: sqlite3.Connection) -> str:
    """Return the HF id of the currently-configured embedding model.

    The value is read from ``discovery_settings.embedding_model``. The
    default is sourced from ``DISCOVERY_SETTINGS_DEFAULTS`` so this
    helper is the single source of truth for every read that needs to
    filter ``publication_embeddings`` by model.
    """
    from alma.discovery.defaults import DISCOVERY_SETTINGS_DEFAULTS

    try:
        row = conn.execute(
            "SELECT value FROM discovery_settings WHERE key = 'embedding_model'"
        ).fetchone()
    except sqlite3.OperationalError:
        row = None
    if row is not None:
        value = str(row["value"] or "").strip()
        if value:
            return value
    return DISCOVERY_SETTINGS_DEFAULTS["embedding_model"]


def get_cached_embedding(
    paper_id: str,
    conn: sqlite3.Connection,
) -> Optional["numpy.ndarray"]:
    """Return the cached active-model embedding for ``paper_id``.

    This helper never computes missing vectors. Expensive AI vector work
    belongs to explicit Activity-backed jobs.

    Args:
        paper_id: Paper UUID used as the cache key.
        conn: Open SQLite connection with access to the
              ``publication_embeddings`` table.

    Returns:
        1-D numpy array with the active model's dimensionality, or ``None``
        when no cached vector exists.
    """
    if not _NUMPY_AVAILABLE:
        return None

    active_model = get_active_embedding_model(conn)

    row = conn.execute(
        "SELECT embedding FROM publication_embeddings "
        "WHERE paper_id = ? AND model = ?",
        (paper_id, active_model),
    ).fetchone()

    if row is not None:
        from alma.core.vector_blob import decode_vector
        return decode_vector(row["embedding"])

    return None


# ---------------------------------------------------------------------------
# Centroid computation
# ---------------------------------------------------------------------------


def compute_embedding_centroid(
    pubs: List[dict],
    conn: sqlite3.Connection,
) -> Optional["numpy.ndarray"]:
    """Compute the average embedding of a list of publications.

    Each publication dict should contain at least ``id`` (paper_id),
    ``title``, and optionally ``abstract``.

    Args:
        pubs: List of publication dicts.
        conn: Open SQLite connection.

    Returns:
        1-D numpy array representing the centroid, or ``None`` when no
        embeddings could be computed.
    """
    if not _NUMPY_AVAILABLE:
        return None

    embeddings: List["numpy.ndarray"] = []
    for pub in pubs:
        embedding = get_cached_embedding(pub["id"], conn)
        if embedding is not None:
            embeddings.append(embedding)

    if not embeddings:
        return None

    return np.mean(np.stack(embeddings), axis=0)


def load_publication_example_embeddings(
    pubs: List[dict],
    conn: sqlite3.Connection,
    *,
    limit: int = 12,
) -> list["numpy.ndarray"]:
    """Load a small set of normalized publication embeddings for exemplar matching."""
    if not _NUMPY_AVAILABLE:
        return []

    out: list["numpy.ndarray"] = []
    seen: set[str] = set()
    for pub in pubs:
        if len(out) >= max(1, int(limit or 12)):
            break
        paper_id = str(pub.get("id") or "").strip()
        if not paper_id or paper_id in seen:
            continue
        seen.add(paper_id)
        embedding = get_cached_embedding(paper_id, conn)
        normalized = _normalize_embedding_vector(embedding)
        if normalized is not None:
            out.append(normalized)
    return out


# ---------------------------------------------------------------------------
# Semantic similarity scoring
# ---------------------------------------------------------------------------


def compute_semantic_similarity(
    candidate_embedding: Optional["numpy.ndarray"],
    positive_centroid: Optional["numpy.ndarray"],
    negative_centroid: Optional["numpy.ndarray"] = None,
) -> float:
    """Compute semantic similarity score for a candidate paper.

    Uses cosine similarity between a *precomputed* candidate embedding
    and a positive centroid, optionally penalising similarity to a
    negative centroid. The caller is responsible for producing the
    candidate vector with the active provider so all three vectors share
    the same dimensionality — this function will not re-encode text.

    The formula is::

        score = cos_sim(candidate, positive) - 0.5 * cos_sim(candidate, negative)

    The result is clamped to the ``[0, 1]`` range.

    Returns:
        Similarity score in ``[0, 1]``, or ``0.0`` when any required
        input vector is missing.
    """
    if candidate_embedding is None or positive_centroid is None:
        return 0.0

    score = _cosine_similarity_np(candidate_embedding, positive_centroid)

    if negative_centroid is not None:
        neg_sim = _cosine_similarity_np(candidate_embedding, negative_centroid)
        score -= 0.5 * neg_sim

    return float(max(0.0, min(1.0, score)))


def compute_semantic_similarity_details(
    *,
    candidate_embedding: Optional["numpy.ndarray"] = None,
    positive_centroid: Optional["numpy.ndarray"],
    negative_centroid: Optional["numpy.ndarray"] = None,
    positive_examples: Optional[List["numpy.ndarray"]] = None,
    negative_examples: Optional[List["numpy.ndarray"]] = None,
) -> dict[str, float | bool]:
    """Return richer semantic-similarity diagnostics for one candidate.

    The caller must pass a ``candidate_embedding`` produced by the
    active provider; this helper will not encode text on the fly. That
    way every vector in play here (candidate, centroids, exemplars)
    shares the active model's dimensionality.
    """
    if not _NUMPY_AVAILABLE:
        return {
            "raw_score": 0.0,
            "positive_centroid_raw": 0.0,
            "positive_exemplar_raw": 0.0,
            "negative_centroid_raw": 0.0,
            "negative_exemplar_raw": 0.0,
            "candidate_embedding_ready": False,
        }

    normalized_candidate = _normalize_embedding_vector(candidate_embedding)

    if normalized_candidate is None:
        return {
            "raw_score": 0.0,
            "positive_centroid_raw": 0.0,
            "positive_exemplar_raw": 0.0,
            "negative_centroid_raw": 0.0,
            "negative_exemplar_raw": 0.0,
            "candidate_embedding_ready": False,
        }

    pos_centroid = _normalize_embedding_vector(positive_centroid)
    neg_centroid = _normalize_embedding_vector(negative_centroid)
    positive_exemplars = [
        normalized
        for normalized in (_normalize_embedding_vector(vec) for vec in (positive_examples or []))
        if normalized is not None
    ]
    negative_exemplars = [
        normalized
        for normalized in (_normalize_embedding_vector(vec) for vec in (negative_examples or []))
        if normalized is not None
    ]

    centroid_raw = _cosine_similarity_np(normalized_candidate, pos_centroid) if pos_centroid is not None else 0.0
    exemplar_raw = max((_cosine_similarity_np(normalized_candidate, vec) for vec in positive_exemplars), default=0.0)
    neg_centroid_raw = _cosine_similarity_np(normalized_candidate, neg_centroid) if neg_centroid is not None else 0.0
    neg_exemplar_raw = max((_cosine_similarity_np(normalized_candidate, vec) for vec in negative_exemplars), default=0.0)

    positive_signal = max(exemplar_raw, (centroid_raw * 0.35) + (exemplar_raw * 0.65))
    positive_support = min(centroid_raw, exemplar_raw)
    negative_signal = max(neg_exemplar_raw, (neg_centroid_raw * 0.55) + (neg_exemplar_raw * 0.45))
    raw_score = max(
        0.0,
        min(1.0, positive_signal + (positive_support * 0.12) - (0.38 * negative_signal)),
    )

    return {
        "raw_score": float(raw_score),
        "positive_centroid_raw": round(float(centroid_raw), 4),
        "positive_exemplar_raw": round(float(exemplar_raw), 4),
        "positive_support_raw": round(float(positive_support), 4),
        "positive_signal_raw": round(float(positive_signal), 4),
        "negative_centroid_raw": round(float(neg_centroid_raw), 4),
        "negative_exemplar_raw": round(float(neg_exemplar_raw), 4),
        "negative_signal_raw": round(float(negative_signal), 4),
        "candidate_embedding_ready": True,
    }


def compute_lexical_similarity(
    candidate_text: str,
    positive_texts: List[str],
    negative_texts: Optional[List[str]] = None,
    *,
    profile: Optional[LexicalProfile] = None,
) -> float:
    """Compute lexical similarity as a robust fallback when embeddings are unavailable.

    Uses TF-IDF cosine similarity when scikit-learn is available, otherwise falls
    back to a lightweight token-overlap scorer.
    """
    return float(
        compute_lexical_similarity_details(
            candidate_text,
            positive_texts,
            negative_texts=negative_texts,
            profile=profile,
        ).get("raw_score", 0.0)
    )


def compute_lexical_similarity_details(
    candidate_text: str,
    positive_texts: List[str],
    negative_texts: Optional[List[str]] = None,
    *,
    profile: Optional[LexicalProfile] = None,
) -> dict[str, float]:
    """Return richer lexical similarity diagnostics for one candidate."""
    candidate = (candidate_text or "").strip()
    positives = [t.strip() for t in positive_texts if str(t or "").strip()]
    negatives = [t.strip() for t in (negative_texts or []) if str(t or "").strip()]
    if not candidate or not positives:
        return {
            "raw_score": 0.0,
            "word_raw": 0.0,
            "char_raw": 0.0,
            "term_raw": 0.0,
            "negative_penalty": 0.0,
        }

    profile = profile or build_lexical_profile(positives, negatives)
    positives = profile.positive_texts if profile is not None else positives
    negatives = profile.negative_texts if profile is not None else negatives

    word_raw = 0.0
    char_raw = 0.0
    word_neg = 0.0
    char_neg = 0.0

    if profile is not None and profile.word_vectorizer is not None and profile.word_positive_matrix is not None:
        try:
            cand_word = profile.word_vectorizer.transform([candidate])
            if profile.word_positive_matrix.shape[0] > 0:
                word_raw = float(cosine_similarity(cand_word, profile.word_positive_matrix).max())
            if profile.word_negative_matrix is not None and profile.word_negative_matrix.shape[0] > 0:
                word_neg = float(cosine_similarity(cand_word, profile.word_negative_matrix).max())
        except Exception as exc:
            logger.debug("Word lexical similarity failed: %s", exc)

    if profile is not None and profile.char_vectorizer is not None and profile.char_positive_matrix is not None:
        try:
            cand_char = profile.char_vectorizer.transform([candidate])
            if profile.char_positive_matrix.shape[0] > 0:
                char_raw = float(cosine_similarity(cand_char, profile.char_positive_matrix).max())
            if profile.char_negative_matrix is not None and profile.char_negative_matrix.shape[0] > 0:
                char_neg = float(cosine_similarity(cand_char, profile.char_negative_matrix).max())
        except Exception as exc:
            logger.debug("Character lexical similarity failed: %s", exc)

    candidate_terms = _extract_weighted_terms(candidate)
    candidate_term_total = float(sum(candidate_terms.values()) or 1.0)
    term_raw = 0.0
    term_neg = 0.0
    if candidate_terms:
        pos_terms = profile.positive_terms if profile is not None else Counter()
        neg_terms = profile.negative_terms if profile is not None else Counter()
        if pos_terms:
            matched = sum(float(min(weight, pos_terms.get(term, 0))) for term, weight in candidate_terms.items())
            term_raw = matched / candidate_term_total
        if neg_terms:
            neg_matched = sum(float(min(weight, neg_terms.get(term, 0))) for term, weight in candidate_terms.items())
            term_neg = neg_matched / candidate_term_total

    negative_penalty = (word_neg * 0.30) + (char_neg * 0.18) + (term_neg * 0.22)
    score = max(0.0, min(1.0, (word_raw * 0.42) + (char_raw * 0.18) + (term_raw * 0.40) - negative_penalty))
    return {
        "raw_score": float(score),
        "word_raw": round(float(word_raw), 4),
        "char_raw": round(float(char_raw), 4),
        "term_raw": round(float(term_raw), 4),
        "negative_penalty": round(float(negative_penalty), 4),
    }


def batch_compute_lexical_similarity(
    candidate_texts: Dict[str, str],
    profile: Optional[LexicalProfile],
) -> Dict[str, dict]:
    """Batch-compute lexical similarity for all candidates at once.

    Instead of calling compute_lexical_similarity_details per candidate
    (each doing its own transform + cosine_similarity), this transforms
    all candidate texts in one call and computes cosine similarity as a
    single matrix operation — eliminating per-candidate overhead.

    Args:
        candidate_texts: Mapping of candidate key -> text.
        profile: Precomputed LexicalProfile from build_lexical_profile.

    Returns:
        Dict mapping each candidate key to its lexical details dict
        (same shape as compute_lexical_similarity_details output).
    """
    keys = list(candidate_texts.keys())
    texts = [candidate_texts[k] for k in keys]
    n = len(keys)

    # Default result for all candidates
    empty = {"raw_score": 0.0, "word_raw": 0.0, "char_raw": 0.0, "term_raw": 0.0, "negative_penalty": 0.0}
    if not keys or profile is None:
        return {k: dict(empty) for k in keys}

    # --- Batch word TF-IDF cosine similarity ---
    word_raws = [0.0] * n
    word_negs = [0.0] * n
    if profile.word_vectorizer is not None and profile.word_positive_matrix is not None:
        try:
            cand_word_matrix = profile.word_vectorizer.transform(texts)
            if profile.word_positive_matrix.shape[0] > 0:
                # Shape: (n_candidates, n_positive_docs) — take row-wise max
                sim_matrix = cosine_similarity(cand_word_matrix, profile.word_positive_matrix)
                for i in range(n):
                    word_raws[i] = float(sim_matrix[i].max())
            if profile.word_negative_matrix is not None and profile.word_negative_matrix.shape[0] > 0:
                neg_matrix = cosine_similarity(cand_word_matrix, profile.word_negative_matrix)
                for i in range(n):
                    word_negs[i] = float(neg_matrix[i].max())
        except Exception as exc:
            logger.debug("Batch word lexical similarity failed: %s", exc)

    # --- Batch char TF-IDF cosine similarity ---
    char_raws = [0.0] * n
    char_negs = [0.0] * n
    if profile.char_vectorizer is not None and profile.char_positive_matrix is not None:
        try:
            cand_char_matrix = profile.char_vectorizer.transform(texts)
            if profile.char_positive_matrix.shape[0] > 0:
                sim_matrix = cosine_similarity(cand_char_matrix, profile.char_positive_matrix)
                for i in range(n):
                    char_raws[i] = float(sim_matrix[i].max())
            if profile.char_negative_matrix is not None and profile.char_negative_matrix.shape[0] > 0:
                neg_matrix = cosine_similarity(cand_char_matrix, profile.char_negative_matrix)
                for i in range(n):
                    char_negs[i] = float(neg_matrix[i].max())
        except Exception as exc:
            logger.debug("Batch char lexical similarity failed: %s", exc)

    # --- Per-candidate term overlap (lightweight, no sklearn) ---
    pos_terms = profile.positive_terms
    neg_terms = profile.negative_terms
    results: Dict[str, dict] = {}
    for i, key in enumerate(keys):
        candidate_terms = _extract_weighted_terms(texts[i])
        candidate_term_total = float(sum(candidate_terms.values()) or 1.0)
        term_raw = 0.0
        term_neg = 0.0
        if candidate_terms:
            if pos_terms:
                matched = sum(float(min(w, pos_terms.get(t, 0))) for t, w in candidate_terms.items())
                term_raw = matched / candidate_term_total
            if neg_terms:
                neg_matched = sum(float(min(w, neg_terms.get(t, 0))) for t, w in candidate_terms.items())
                term_neg = neg_matched / candidate_term_total

        negative_penalty = (word_negs[i] * 0.30) + (char_negs[i] * 0.18) + (term_neg * 0.22)
        score = max(0.0, min(1.0, (word_raws[i] * 0.42) + (char_raws[i] * 0.18) + (term_raw * 0.40) - negative_penalty))
        results[key] = {
            "raw_score": float(score),
            "word_raw": round(float(word_raws[i]), 4),
            "char_raw": round(float(char_raws[i]), 4),
            "term_raw": round(float(term_raw), 4),
            "negative_penalty": round(float(negative_penalty), 4),
        }

    return results


def _cosine_similarity_np(a: "numpy.ndarray", b: "numpy.ndarray") -> float:
    """Compute cosine similarity between two 1-D numpy vectors.

    Args:
        a: First vector.
        b: Second vector.

    Returns:
        Cosine similarity as a float in ``[-1, 1]``.
    """
    norm_a = np.linalg.norm(a)
    norm_b = np.linalg.norm(b)
    if norm_a == 0.0 or norm_b == 0.0:
        return 0.0
    return float(np.dot(a, b) / (norm_a * norm_b))
