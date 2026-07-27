"""Standalone scoring functions for discovery candidates.

Extracted from DiscoveryEngine for reuse by both the lens system and
the legacy engine.  All functions are stateless — they take a DB connection
and settings dict rather than depending on class state.

9 independent signal families:
  1. source_relevance  — retrieval confidence from the channel that found the paper
  2. topic_score       — overlap between paper topics and user preference profile
  3. text_similarity   — semantic (embedding) or lexical (TF-IDF) text match
  4. author_affinity   — overlap between paper authors and preferred authors
  5. journal_affinity  — overlap between paper journal and preferred journals
  6. recency_boost     — preference for recent publications
  7. citation_quality  — log-scaled citation count
  8. feedback_adj      — adjustment from explicit paper preference history
  9. preference_affinity — Signal Lab swipe/game feedback

``usefulness_boost`` remains diagnostic only.  It is not weighted because it
duplicates recency/citation/similarity and would reward metadata completeness.
"""

from __future__ import annotations

import logging
import math
import re
import sqlite3
from typing import Any

from alma.application.author_signal import build_discovery_author_affinity
from alma.application.signal_projection import (
    ProjectedPaperSignals,
    load_projected_paper_signals,
)
from alma.core import topics
from alma.core.keywords import parse_keywords
from alma.core.scoring_math import (
    clamp as _clamp,
)
from alma.core.scoring_math import (
    consensus_bonus as _shared_consensus_bonus,
)
from alma.core.scoring_math import (
    log_prevalence_weights,
)
from alma.core.sql_helpers import standalone_paper_sql
from alma.core.time import utcnow
from alma.discovery import similarity as sim_module
from alma.discovery.defaults import DISCOVERY_SETTINGS_DEFAULTS
from alma.services.feedback_substrate import get_preference_affinity_signal

logger = logging.getLogger(__name__)


# Multi-source consensus bonus. Math + diminishing-returns shape live in
# `alma.core.scoring_math.consensus_bonus`; we keep the calibration
# constants here so the band ceiling and bonus fraction are visible at
# the call site.
_MAX_DISCOVERY_SCORE = 100.0
_CONSENSUS_BONUS_FRACTION = 0.12

# Sentinel for `score_candidate(topic_provider=...)`: distinguishes "caller
# did not pass a provider → resolve lazily" from "caller explicitly passed
# None → no provider available". Lets the hot loop hoist the subprocess-backed
# `get_active_provider` out of the per-candidate path.
_PROVIDER_UNSET = object()

# Dismissal cluster penalty — paper-side mirror of the author rail's
# `_dismissal_overlap_penalty` (see `alma.application.authors`).
# Pulled out as a separate post-consensus pass (not folded into
# `feedback_adj`) so direct user dismissals can hit harder than the
# bounded ±0.6 projected adjustment allows. Magnitudes here are in
# *score points* (0–100 band), not normalized weights.
#
# Per-axis ceilings reflect the same rank-of-evidence the author rail
# uses: topic > venue > institution > author identity > keyword/tag.
# Caller-supplied projected magnitudes are already in [-1, +1]; we
# multiply by these per-hit ceilings and sum, then cap at 30 points
# so a candidate can never be permanently zero'd by penalty alone —
# the user can still dismiss them explicitly.
_DISMISSAL_TOPIC_PENALTY_PER_HIT = 4.0
_DISMISSAL_VENUE_PENALTY_PER_HIT = 3.0
_DISMISSAL_AUTHOR_PENALTY_PER_HIT = 2.0
_DISMISSAL_AUTHOR_NAME_PENALTY_PER_HIT = 1.5
_DISMISSAL_KEYWORD_PENALTY_PER_HIT = 1.0
_DISMISSAL_PENALTY_CAP = 30.0




def _consensus_bonus(consensus_count: int) -> float:
    """Band-relative bonus for N>1 independent retrieval-source confirmations."""
    return _shared_consensus_bonus(
        consensus_count,
        fraction=_CONSENSUS_BONUS_FRACTION,
        max_score=_MAX_DISCOVERY_SCORE,
    )


# ---------------------------------------------------------------------------
# Author name parsing (shared with engine.py)
# ---------------------------------------------------------------------------

def parse_author_names(authors_str: str) -> list[str]:
    """Parse author strings from multiple formats into normalized display names.

    Supports:
    - ``"First Last, First Last"``
    - ``"Last, First, Last, First"`` (common BibTeX import style)
    - ``"First Last; First Last"``
    - ``"First Last and First Last"``
    """
    s = (authors_str or "").strip()
    if not s:
        return []

    if ";" in s:
        raw = [p.strip() for p in s.split(";") if p.strip()]
    elif re.search(r"\band\b", s, flags=re.IGNORECASE):
        raw = [p.strip() for p in re.split(r"\band\b", s, flags=re.IGNORECASE) if p.strip()]
    else:
        segs = [p.strip() for p in s.split(",") if p.strip()]
        if len(segs) >= 4 and len(segs) % 2 == 0:
            even = segs[0::2]
            even_short_ratio = sum(1 for x in even if len(x.split()) <= 2) / max(len(even), 1)
            if even_short_ratio >= 0.8:
                raw = [f"{segs[i + 1]} {segs[i]}".strip() for i in range(0, len(segs), 2)]
            else:
                raw = segs
        else:
            raw = segs

    return [" ".join(name.split()) for name in raw if name.strip()]


def author_affinity_keys(name: str) -> set[str]:
    """Return robust match keys for author affinity matching."""
    tokens = [t for t in re.split(r"[^a-z0-9]+", (name or "").lower()) if t]
    if not tokens:
        return set()
    first = tokens[0]
    last = tokens[-1]
    keys = {" ".join(tokens)}
    if first and last:
        keys.add(f"{last}|{first[0]}")
    return keys


def _pub_text(pub: dict) -> str:
    """Build a richer similarity document for embedding / lexical scoring."""
    return sim_module.build_similarity_text(pub)


# ---------------------------------------------------------------------------
# Embedding centroid helper
# ---------------------------------------------------------------------------

def compute_centroid_from_ids(
    conn: sqlite3.Connection, paper_ids: list[str]
) -> Any | None:
    """Compute average embedding from cached publication_embeddings rows.

    Only rows produced by the currently-active embedding model are
    averaged; vectors from older models are ignored so the result
    always has the active model's dimensionality.
    """
    if not paper_ids:
        return None
    import numpy as np

    active_model = sim_module.get_active_embedding_model(conn)
    placeholders = ",".join("?" * len(paper_ids))
    rows = conn.execute(
        f"""
        SELECT pe.embedding
        FROM publication_embeddings pe
        JOIN papers p ON p.id = pe.paper_id
        WHERE pe.model = ?
          AND pe.paper_id IN ({placeholders})
          AND {standalone_paper_sql('p')}
        """,
        [active_model, *paper_ids],
    ).fetchall()
    if not rows:
        return None
    from alma.core.vector_blob import decode_vectors_uniform
    matrix, _ = decode_vectors_uniform(r["embedding"] for r in rows)
    if matrix.size == 0:
        return None
    return np.mean(matrix, axis=0)


# ---------------------------------------------------------------------------
# Preference profile
# ---------------------------------------------------------------------------

def load_settings(conn: sqlite3.Connection) -> dict[str, str]:
    """Read discovery settings from DB, merged with defaults."""
    kv: dict[str, str] = dict(DISCOVERY_SETTINGS_DEFAULTS)
    try:
        rows = conn.execute("SELECT key, value FROM discovery_settings").fetchall()
        for r in rows:
            kv[r["key"] if isinstance(r, sqlite3.Row) else r[0]] = (
                r["value"] if isinstance(r, sqlite3.Row) else r[1]
            )
    except sqlite3.OperationalError:
        pass
    return kv


def compute_preference_profile(
    conn: sqlite3.Connection,
    positive_pubs: list[dict],
    negative_pubs: list[dict],
    settings: dict[str, str] | None = None,
    *,
    scope_paper_ids: set[str] | None = None,
) -> dict:
    """Compute a user preference profile from rated publications.

    Aggregates signals from:
    - Rated/liked publications (topics, authors, journals)
    - Collection items (topic overlap with weight 0.5)
    - User-applied tags (treated as high-weight topic signals)
    - Past explicit paper preference

    Returns a dict with topic_weights, author_affinity, journal_affinity,
    feedback positive / negative semantic centroids, and the projected
    paper-feedback graph from `signal_projection`.

    ``scope_paper_ids`` restricts the aggregate signals to a specific set of
    papers (a collection-typed lens passes its collection's paper ids). When
    set, the collection-topic and tag signals only count papers in that set,
    and the followed-author background prior — an explicit Library-expansion
    signal — is skipped, so a collection lens is not contaminated by topics
    and authors from the rest of the Library. ``None`` (the default) keeps the
    Library-wide behaviour used by the ``library_global`` lens.
    """
    if settings is None:
        settings = load_settings(conn)

    topic_weights: dict[str, float] = {}
    journal_affinity: dict[str, float] = {}

    def _accumulate(pubs: list[dict], weight: float) -> None:
        for pub in pubs:
            paper_id = pub.get("id", "")
            if paper_id:
                try:
                    topic_rows = conn.execute(
                        "SELECT pt.term, pt.score, t.canonical_name "
                        "FROM publication_topics pt "
                        "LEFT JOIN topics t ON pt.topic_id = t.topic_id "
                        "WHERE pt.paper_id = ?",
                        (paper_id,),
                    ).fetchall()
                    for tr in topic_rows:
                        term = (tr["canonical_name"] or tr["term"] or "").strip().lower()
                        if term:
                            # 44.1: only the magic 0.5 → the shared default. NO
                            # topic_relevance() here — its 0.1 floor/clamp is a
                            # signal change gated on an A/B (task 10).
                            topic_weights[term] = topic_weights.get(term, 0) + weight * (tr["score"] or topics.DEFAULT_TOPIC_SCORE)
                except sqlite3.OperationalError:
                    logger.warning("publication_topics table not available for preference profile")

            journal = (pub.get("journal") or "").strip().lower()
            if journal:
                journal_affinity[journal] = journal_affinity.get(journal, 0) + weight

    _accumulate(positive_pubs, 1.0)
    _accumulate(negative_pubs, -1.0)

    # -- Collection signals --
    try:
        collection_topic_rows = conn.execute(
            f"""SELECT ci.paper_id, pt.term, pt.score, t.canonical_name
               FROM collection_items ci
               JOIN papers p ON p.id = ci.paper_id
               JOIN publication_topics pt ON pt.paper_id = ci.paper_id
               LEFT JOIN topics t ON pt.topic_id = t.topic_id
               WHERE p.status = 'library'
                 AND {standalone_paper_sql('p')}"""
        ).fetchall()
        for cr in collection_topic_rows:
            c_paper_id = (cr["paper_id"] or "").strip() if isinstance(cr, sqlite3.Row) else ""
            if not c_paper_id:
                continue
            if scope_paper_ids is not None and c_paper_id not in scope_paper_ids:
                # Collection-scoped lens: ignore papers from other collections.
                continue
            term = (cr["canonical_name"] or cr["term"] or "").strip().lower()
            if term:
                # 44.1: shared default only (no floor/clamp — see above).
                topic_weights[term] = topic_weights.get(term, 0) + 0.5 * (
                    cr["score"] or topics.DEFAULT_TOPIC_SCORE
                )
    except sqlite3.OperationalError:
        logger.debug("collection topic signals unavailable")

    # -- Tag signals --
    try:
        tag_rows = conn.execute(
            f"""
            SELECT pt.paper_id, t.name
            FROM publication_tags pt
            JOIN tags t ON pt.tag_id = t.id
            JOIN papers p ON p.id = pt.paper_id
            WHERE {standalone_paper_sql('p')}
            """
        ).fetchall()
        for tr in tag_rows:
            if scope_paper_ids is not None:
                tagged_id = (tr["paper_id"] or "").strip() if isinstance(tr, sqlite3.Row) else ""
                if tagged_id not in scope_paper_ids:
                    # Collection-scoped lens: only tags on in-collection papers.
                    continue
            tag_name = (tr["name"] or "").strip().lower()
            if tag_name:
                topic_weights[tag_name] = topic_weights.get(tag_name, 0) + 2.0
    except sqlite3.OperationalError:
        logger.debug("tags/publication_tags tables not available")

    # -- Followed-author background corpus priors --
    # Followed authors contribute a weak, non-library prior. This expands
    # ranking context without conflating the curated Library with the full
    # monitored corpus. A collection-scoped lens deliberately skips it: the
    # whole point of the collection scope is to keep the taste narrow, and a
    # Library-wide monitored-corpus prior would reintroduce off-collection
    # topics (exactly the contamination the scope is meant to remove).
    try:
        # A collection-scoped lens skips this Library-wide prior entirely.
        if scope_paper_ids is None:
            # Note: the join condition is `lower(...)` only, NOT `lower(trim(...))`.
            # The redundant `trim()` defeats the expression index
            # `idx_publication_authors_openalex_norm` and turned this query into a
            # 12s+ table scan on every Discovery / Find&add request.
            bg_topic_rows = conn.execute(
                f"""
                SELECT COALESCE(t.canonical_name, pt.term, '') AS term, COUNT(DISTINCT pt.paper_id) AS papers
                FROM papers p
                JOIN publication_topics pt ON pt.paper_id = p.id
                JOIN publication_authors pa ON pa.paper_id = p.id
                JOIN authors a ON lower(a.openalex_id) = lower(pa.openalex_id)
                JOIN followed_authors fa ON fa.author_id = a.id
                LEFT JOIN topics t ON t.topic_id = pt.topic_id
                WHERE p.status NOT IN ('library', 'dismissed', 'removed')
                  AND {standalone_paper_sql('p')}
                  AND COALESCE(TRIM(pt.term), '') <> ''
                GROUP BY COALESCE(t.canonical_name, pt.term, '')
                ORDER BY papers DESC, term ASC
                LIMIT 24
                """
            ).fetchall()
            max_bg_topic = max((int(row["papers"] or 0) for row in bg_topic_rows), default=0)
            for row in bg_topic_rows:
                term = str(row["term"] or "").strip().lower()
                papers = int(row["papers"] or 0)
                if term and max_bg_topic > 0:
                    topic_weights[term] = topic_weights.get(term, 0.0) + (0.22 * (papers / max_bg_topic))
    except sqlite3.OperationalError:
        logger.debug("followed-author background topic priors unavailable")

    try:
        bg_venue_rows = conn.execute(
            f"""
            SELECT p.journal, COUNT(DISTINCT p.id) AS papers
            FROM papers p
            JOIN publication_authors pa ON pa.paper_id = p.id
            JOIN authors a ON lower(a.openalex_id) = lower(pa.openalex_id)
            JOIN followed_authors fa ON fa.author_id = a.id
            WHERE p.status NOT IN ('library', 'dismissed', 'removed')
              AND {standalone_paper_sql('p')}
              AND COALESCE(TRIM(p.journal), '') <> ''
            GROUP BY lower(trim(p.journal)), p.journal
            ORDER BY papers DESC, p.journal ASC
            LIMIT 16
            """
        ).fetchall()
        max_bg_venue = max((int(row["papers"] or 0) for row in bg_venue_rows), default=0)
        for row in bg_venue_rows:
            journal = str(row["journal"] or "").strip().lower()
            papers = int(row["papers"] or 0)
            if journal and max_bg_venue > 0:
                journal_affinity[journal] = journal_affinity.get(journal, 0.0) + (0.18 * (papers / max_bg_venue))
    except sqlite3.OperationalError:
        logger.debug("followed-author background venue priors unavailable")

    # -- Normalize accumulated weights to [0, 1] --
    # All three preference dictionaries (topic, author, journal) now go
    # through log-prevalence: log(1 + count) / max_log. The original
    # rationale for keeping authors on linear max-normalization
    # ("identity match, not prevalence") falls apart on heavily skewed
    # libraries — when one author appears on >50% of saved papers, the
    # linear scheme floors every other author at <0.1 and the
    # dominant author crowds out the long tail. Long-tail co-authors
    # still register as "this is someone you've worked with"; they
    # just compete with the dominant author on log-curve terms instead
    # of being drowned by the linear max.
    topic_weights = log_prevalence_weights(topic_weights)
    journal_affinity = log_prevalence_weights(journal_affinity)

    # Author affinity is the canonical author signal (one definition, shared
    # with the Authors page, suggestions, and rankings — see
    # `alma.application.author_signal`). We take its STABLE preference
    # (library + rating + embedding similarity + neighborhood); the volatile
    # interaction component keeps flowing through `_projected_feedback_adjustment`
    # / `_dismissal_cluster_penalty` below, so the two never double-count the
    # same feedback. This replaces the old name-prevalence count and gives the
    # ranker embedding-similarity-aware author affinity for free.
    author_affinity = build_discovery_author_affinity(conn)
    followed_author_ids: set[str] = set()
    followed_author_names: set[str] = set()
    for row in conn.execute(
        """
        SELECT a.openalex_id, a.name
        FROM followed_authors fa
        JOIN authors a ON a.id = fa.author_id
        """
    ).fetchall():
        author_id = str(row["openalex_id"] or "").strip().lower()
        author_name = " ".join(str(row["name"] or "").lower().split())
        if author_id:
            followed_author_ids.add(author_id)
        if author_name:
            followed_author_names.add(author_name)

    # -- Feedback centroids from past recommendations --
    # Structured per-author / per-topic / per-venue / per-keyword / per-tag
    # signal flows through `load_projected_paper_signals` instead — the
    # `_incorporate_feedback` now produces only the positive embedding
    # centroid. Explicit negative preference stays in the structured
    # projection; visibility-only dismissals never enter either path.
    feedback_pos_centroid, feedback_neg_centroid = _incorporate_feedback(conn, settings)
    projected_feedback = load_projected_paper_signals(conn)

    return {
        "topic_weights": topic_weights,
        "author_affinity": author_affinity,
        "followed_author_ids": followed_author_ids,
        "followed_author_names": followed_author_names,
        "journal_affinity": journal_affinity,
        "feedback_positive_centroid": feedback_pos_centroid,
        "feedback_negative_centroid": feedback_neg_centroid,
        "projected_feedback": projected_feedback,
    }


def _normalize_weights(weights: dict[str, float]) -> dict[str, float]:
    """Scale weight dict so the maximum absolute value is 1.0.

    Preserves relative rankings and sign (for negative weights from
    dismissed papers). Returns empty dict unchanged.
    """
    if not weights:
        return weights
    max_abs = max(abs(v) for v in weights.values())
    if max_abs <= 0:
        return weights
    return {k: v / max_abs for k, v in weights.items()}




def _incorporate_feedback(
    conn: sqlite3.Connection, settings: dict[str, str]
) -> tuple[Any, Any]:
    """Build the positive semantic centroid from recommendation feedback.

    The centroid averages cached active-model embeddings of papers the user
    saved or liked. Explicit negative preference flows through the canonical
    structured projection (ratings + feedback events); a visibility-only
    dismiss must never become a global negative centroid. The positive
    centroid feeds `score_candidate`'s `feedback_adj` via cosine similarity.
    Structured signal (authors / topics / venues / keywords / tags) flows
    through `signal_projection` instead — see
    `load_projected_paper_signals`. The legacy
    title-word + comma-split-author fallback that used to live here
    has been removed: it was a coarse last-resort path, the structured
    projection layer covers the same ground far more accurately, and
    keeping a fallback that reads `recommendations.title` produced
    spurious matches on common stopword-like tokens.
    """
    liked_paper_ids: list[str] = []
    try:
        rows = conn.execute(
            f"""SELECT r.paper_id, r.user_action
               FROM recommendations r
               JOIN papers p ON p.id = r.paper_id
               WHERE r.user_action IN ('save', 'like', 'liked')
                 AND {standalone_paper_sql('p')}"""
        ).fetchall()
    except sqlite3.OperationalError:
        logger.warning("recommendations table not available for feedback incorporation")
        return None, None

    for row in rows:
        paper_id = row["paper_id"] or ""
        if not paper_id:
            continue
        liked_paper_ids.append(paper_id)

    pos_centroid = None
    if liked_paper_ids:
        try:
            pos_centroid = compute_centroid_from_ids(conn, liked_paper_ids)
        except Exception as exc:
            logger.warning("Failed to compute positive feedback centroid: %s", exc)
    return pos_centroid, None


# ---------------------------------------------------------------------------
# 10-signal candidate scoring
# ---------------------------------------------------------------------------

def score_candidate(
    candidate: dict,
    preference_profile: dict,
    positive_centroid,
    negative_centroid,
    positive_texts: list[str] | None,
    negative_texts: list[str] | None,
    conn: sqlite3.Connection,
    settings: dict[str, str] | None = None,
    *,
    candidate_text: str | None = None,
    candidate_embedding=None,
    lexical_profile=None,
    positive_example_embeddings=None,
    negative_example_embeddings=None,
    precomputed_lexical_details: dict[str, float] | None = None,
    user_topic_embeddings: dict[str, Any] | None = None,
    preloaded_preference_profile: dict[str, Any] | None = None,
    topic_provider: Any = _PROVIDER_UNSET,
    citation_fabric: dict[str, Any] | None = None,
    lab_ctx: dict[str, Any] | None = None,
) -> tuple[float, dict[str, Any]]:
    """Score a candidate paper using 10 weighted signals (+ bounded bonuses).

    ``topic_provider`` lets a hot-loop caller (the lens-refresh scoring
    loop) pass the embedding provider it already resolved once, so we
    don't re-run ``get_active_provider`` (a subprocess env-probe) per
    candidate. Defaults to the sentinel ``_PROVIDER_UNSET`` → resolve
    lazily, preserving the legacy behaviour for ad-hoc / test callers.

    Returns:
        Tuple of (score_0_to_100, breakdown_dict).
    """
    if settings is None:
        settings = load_settings(conn)

    # Resolve the provider once for this call (or reuse the hoisted one).
    if topic_provider is _PROVIDER_UNSET:
        try:
            from alma.ai.providers import get_active_provider
            _scoring_provider = get_active_provider(conn)
        except Exception:
            _scoring_provider = None
    else:
        _scoring_provider = topic_provider

    current_year = utcnow().year
    recency_window = int(settings.get("limits.recency_window_years", "10"))

    # -- 1. Source relevance --
    source_relevance = float(candidate.get("source_relevance", candidate.get("score", 0.5)))
    # Normalize to [0, 1]
    if source_relevance > 1.0:
        source_relevance = min(1.0, source_relevance / 100.0)

    # -- 2. Topic score --
    paper_topics: list[dict] = candidate.get("topics", [])
    if not paper_topics:
        paper_id = candidate.get("id", "")
        if paper_id:
            try:
                rows = conn.execute(
                    "SELECT term, score FROM publication_topics WHERE paper_id = ?",
                    (paper_id,),
                ).fetchall()
                paper_topics = [{"term": r["term"], "score": r["score"]} for r in rows]
            except sqlite3.OperationalError:
                pass

    if not paper_topics:
        text = _pub_text(candidate)
        words = text.lower().split()
        paper_topics = [
            {"term": w.strip(".,;:!?()[]{}\"'"), "score": 0.3}
            for w in words
            if len(w.strip(".,;:!?()[]{}\"'")) >= 4
        ]

    topic_score = (
        sim_module.compute_topic_overlap(
            preference_profile.get("topic_weights", {}), paper_topics,
            conn=conn,
            user_topic_embeddings=user_topic_embeddings,
            provider=_scoring_provider,
        )
        if paper_topics
        else 0.0
    )
    topic_score = (topic_score + 1.0) / 2.0  # Normalize [-1,1] → [0,1]

    topic_match_mode = "none"
    if paper_topics:
        try:
            topic_match_mode = "semantic" if _scoring_provider is not None else "keyword"
        except Exception as exc:
            # Loud-on-degrade: a failed provider lookup silently dropped
            # the user from semantic to keyword matching. The score
            # downgrade is invisible without this log; users wondering
            # why their semantic discovery quality dropped can find the
            # cause in the backend logs instead of guessing.
            logger.warning(
                "Topic-match provider lookup failed; falling back to keyword-only mode: %s",
                exc,
            )
            topic_match_mode = "keyword"

    # -- 3. Text similarity --
    text_similarity = 0.0
    text_similarity_mode = "none"
    semantic_similarity_raw = 0.0
    lexical_similarity_raw = 0.0
    candidate_text = str(candidate_text or "").strip() or sim_module.build_similarity_text(
        candidate,
        conn=conn,
        paper_topics=paper_topics,
    )
    if candidate_text.strip():
        try:
            semantic_details = sim_module.compute_semantic_similarity_details(
                candidate_embedding=candidate_embedding,
                positive_centroid=positive_centroid,
                negative_centroid=negative_centroid,
                positive_examples=positive_example_embeddings,
                negative_examples=negative_example_embeddings,
            )
            semantic_similarity_raw = float(semantic_details.get("raw_score") or 0.0)
        except Exception as exc:
            # Score 0.0 here means "couldn't compute", not "actually
            # zero similarity" — log at warning so the operator sees
            # when the embedding stack is silently producing neutral
            # scores for every paper.
            logger.warning(
                "Semantic similarity computation failed for candidate (paper_id=%s): %s",
                candidate.get("id") if isinstance(candidate, dict) else None,
                exc,
            )
            semantic_similarity_raw = 0.0
            semantic_details = {
                "positive_centroid_raw": 0.0,
                "positive_exemplar_raw": 0.0,
                "negative_centroid_raw": 0.0,
                "negative_exemplar_raw": 0.0,
                "candidate_embedding_ready": False,
            }
    else:
        semantic_details = {
            "positive_centroid_raw": 0.0,
            "positive_exemplar_raw": 0.0,
            "negative_centroid_raw": 0.0,
            "negative_exemplar_raw": 0.0,
            "candidate_embedding_ready": False,
        }

    semantic_similarity = (
        sim_module.calibrate_similarity_score(semantic_similarity_raw, mode="semantic")
        if semantic_similarity_raw > 0.0
        else 0.0
    )

    if precomputed_lexical_details is not None:
        # Use batch-precomputed results (avoids per-candidate transform overhead)
        lexical_details = precomputed_lexical_details
        lexical_similarity_raw = float(lexical_details.get("raw_score") or 0.0)
    elif candidate_text.strip() and positive_texts:
        try:
            lexical_details = sim_module.compute_lexical_similarity_details(
                candidate_text,
                positive_texts,
                negative_texts=negative_texts,
                profile=lexical_profile,
            )
            lexical_similarity_raw = float(lexical_details.get("raw_score") or 0.0)
        except Exception as exc:
            logger.debug("Lexical similarity failed: %s", exc)
            lexical_similarity_raw = 0.0
            lexical_details = {
                "word_raw": 0.0,
                "char_raw": 0.0,
                "term_raw": 0.0,
                "negative_penalty": 0.0,
            }
    else:
        lexical_details = {
            "word_raw": 0.0,
            "char_raw": 0.0,
            "term_raw": 0.0,
            "negative_penalty": 0.0,
        }
    lexical_similarity = (
        sim_module.calibrate_similarity_score(lexical_similarity_raw, mode="lexical")
        if lexical_similarity_raw > 0.0
        else 0.0
    )

    semantic_blend_weight = 1.0 if semantic_similarity > 0.0 else 0.0
    lexical_blend_weight = 1.0 if lexical_similarity > 0.0 else 0.0
    if semantic_similarity > 0.0 and lexical_similarity > 0.0:
        semantic_blend_weight = 0.68
        lexical_blend_weight = 0.32
        semantic_support = float(semantic_details.get("positive_support_raw") or 0.0)
        lexical_term_support = float(lexical_details.get("term_raw") or 0.0)
        lexical_word_support = float(lexical_details.get("word_raw") or 0.0)
        if semantic_support >= 0.24:
            semantic_blend_weight += 0.08
        elif semantic_support <= 0.10:
            semantic_blend_weight -= 0.06
        if lexical_term_support >= 0.18 or lexical_word_support >= 0.16:
            semantic_blend_weight -= 0.08
        semantic_blend_weight = _clamp(semantic_blend_weight, 0.56, 0.80)
        lexical_blend_weight = 1.0 - semantic_blend_weight
        text_similarity = _clamp(
            (semantic_similarity * semantic_blend_weight) + (lexical_similarity * lexical_blend_weight),
            0.0,
            1.0,
        )
        text_similarity_mode = "hybrid"
    elif semantic_similarity > 0.0:
        text_similarity = semantic_similarity
        text_similarity_mode = "semantic"
    elif lexical_similarity > 0.0:
        text_similarity = lexical_similarity
        text_similarity_mode = "lexical"

    # -- 4. Author affinity --
    author_score = 0.0
    author_affinity_values: list[float] = []
    authors_str = (candidate.get("authors") or "").strip()
    affinity = preference_profile.get("author_affinity", {})
    if authors_str:
        parts = parse_author_names(authors_str)
        for name in parts:
            value = 0.0
            for key in author_affinity_keys(name):
                if key in affinity:
                    value = float(affinity[key])
                    break
            author_affinity_values.append(value)
            author_score += value
        if parts:
            author_score = min(1.0, max(0.0, author_score / max(len(parts), 1)))
    candidate_author_ids = set(_candidate_author_ids(candidate))
    candidate_author_names = {
        " ".join(name.lower().split()) for name in parse_author_names(authors_str)
    }
    followed_author_match = bool(
        candidate_author_ids & set(preference_profile.get("followed_author_ids") or ())
        or candidate_author_names
        & set(preference_profile.get("followed_author_names") or ())
    )

    # -- 5. Journal affinity --
    journal = (candidate.get("journal") or "").strip().lower()
    j_affinity = preference_profile.get("journal_affinity", {})
    journal_score = min(1.0, max(0.0, j_affinity.get(journal, 0))) if journal else 0.0

    # -- 6. Recency boost --
    # Resolve the candidate's year. External-lane candidates often
    # arrive with `year=None` because the merge step drops the int but
    # keeps the ISO `publication_date` — fall back to the date string
    # so corpus rehydration (which fills publication_date but not year
    # on every code path) still drives recency.
    year_value: int | None = None
    raw_year = candidate.get("year")
    if raw_year not in (None, "", 0):
        try:
            year_value = int(raw_year)
        except (TypeError, ValueError):
            year_value = None
    if year_value is None:
        pub_date = str(candidate.get("publication_date") or "").strip()
        if len(pub_date) >= 4 and pub_date[:4].isdigit():
            year_value = int(pub_date[:4])
    if year_value:
        try:
            # Clamp to [0, 1]. A future-dated paper (year_value >
            # current_year, common when OpenAlex back-fills a
            # forthcoming paper with next year's `publication_year`)
            # must not produce recency > 1.0 — that would let a single
            # signal silently overshoot its weight bucket and bias the
            # 10-signal weighted sum.
            recency = 1.0 - ((current_year - year_value) / max(1, recency_window))
            recency = min(1.0, max(0.0, recency))
        except (TypeError, ValueError):
            recency = 0.0
    else:
        recency = 0.0

    # -- 7. Citation quality --
    cited_by_count = candidate.get("cited_by_count", 0) or 0
    try:
        cited_by_count = int(cited_by_count)
    except (TypeError, ValueError):
        cited_by_count = 0
    # T5: influential citation count (when S2 supplies it) carries 2×
    # weight — a 500-citation textbook no longer out-ranks a 30-
    # influential-citation method paper that actually moved the field.
    # `log(1000)` denominator keeps the old calibration anchor so
    # pre-T5 rows without influential counts score identically.
    influential_raw = candidate.get("influential_citation_count", 0) or 0
    try:
        influential_count = int(influential_raw)
    except (TypeError, ValueError):
        influential_count = 0
    effective_citations = max(cited_by_count, 2 * influential_count)
    citation_quality = (
        min(1.0, math.log(effective_citations + 1) / math.log(1000))
        if effective_citations > 0
        else 0.0
    )

    # -- 8. Feedback adjustment --
    # Two complementary inputs:
    #   1. Semantic centroid similarity — cosine of the candidate's
    #      embedding against the average liked paper centroid. Explicit
    #      negative preference comes from the structured projection below.
    #      Captures full-document meaning that structured
    #      tags cannot.
    #   2. Structured projected signal (`signal_projection`) — per
    #      author / topic / venue / keyword / tag / semantic-neighbour /
    #      citation-neighbour signals from `feedback_events`,
    #      `papers.rating`, and `recommendations.user_action`.
    # Both contribute additively, then clamp to [-1, 1].
    feedback_adj = 0.0
    fb_pos_centroid = preference_profile.get("feedback_positive_centroid")
    fb_neg_centroid = preference_profile.get("feedback_negative_centroid")
    projected_axes = _projected_feedback_axes(
        candidate,
        paper_topics,
        authors_str,
        preference_profile.get("projected_feedback"),
    )
    projected_adj = _clamp(
        (0.65 * projected_axes["paper"])
        + (0.55 * projected_axes["semantic_neighbor"])
        + (0.45 * projected_axes["citation_neighbor"])
        + (0.35 * projected_axes["venue"])
        + (0.45 * projected_axes["topic"])
        + (0.30 * projected_axes["keyword"])
        + (0.30 * projected_axes["tag"])
        + (0.40 * projected_axes["author"])
        + (0.30 * projected_axes["author_name"]),
        -0.6,
        0.6,
    )

    if fb_pos_centroid is not None and candidate_embedding is not None:
        try:
            semantic_fb_raw = sim_module.compute_semantic_similarity(
                candidate_embedding, fb_pos_centroid, fb_neg_centroid,
            )
            semantic_fb = sim_module.calibrate_similarity_score(semantic_fb_raw, mode="semantic")
            feedback_adj = (semantic_fb * 2.0) - 1.0
        except Exception as exc:
            logger.debug("Semantic feedback centroid failed: %s", exc)
            feedback_adj = 0.0

    feedback_adj += projected_adj
    feedback_adj = max(-1.0, min(1.0, feedback_adj))
    feedback_adj_norm = (feedback_adj + 1.0) / 2.0  # Shift to [0, 1]

    # -- 9. Preference affinity (Signal Lab) --
    pref_affinity_raw = 0.0
    try:
        # D-AUDIT-10a (2026-04-24): prefer the caller-supplied preload
        # so we don't re-issue the 4-round-trip DB query per candidate
        # inside the scoring loop. Outside the refresh loop (tests,
        # ad-hoc scoring) callers omit `preloaded_preference_profile`
        # and the legacy path runs.
        pref_affinity_raw = get_preference_affinity_signal(
            conn, candidate, preloaded=preloaded_preference_profile,
        )
    except Exception as exc:
        logger.debug("Preference affinity signal failed: %s", exc)
    pref_affinity = (pref_affinity_raw + 1.0) / 2.0  # Shift [-1,1] → [0,1]

    # -- 10. Usefulness boost --
    # Discovery should not only reward resemblance. It should also reward
    # candidates that are timely, credible, and not too redundant with what
    # the user already has.
    novelty = max(
        0.0,
        1.0 - min(1.0, (text_similarity * 0.55) + (author_score * 0.25) + (journal_score * 0.20)),
    )
    metadata_quality = 0.0
    if str(candidate.get("doi") or "").strip():
        metadata_quality += 0.5
    if str(candidate.get("url") or "").strip():
        metadata_quality += 0.3
    if str(candidate.get("abstract") or "").strip():
        metadata_quality += 0.2
    metadata_quality = min(1.0, metadata_quality)
    usefulness_boost = _clamp(
        (novelty * 0.45)
        + (recency * 0.25)
        + (citation_quality * 0.20)
        + (metadata_quality * 0.10),
        0.0,
        1.0,
    )

    # -- Citation-fabric channels (task 47 §7): local-only coupling + co-citation
    # strength vs the loved/saved set, precomputed in the orchestrator. Missing
    # (cold start / no local paper) → 0, so the channel simply doesn't fire. --
    cf = citation_fabric or {}
    coupling_strength = max(0.0, min(1.0, float(cf.get("coupling_strength") or 0.0)))
    cocitation_strength = max(0.0, min(1.0, float(cf.get("cocitation_strength") or 0.0)))

    # -- Weighted combination --
    weights = {
        "source_relevance": float(settings.get("weights.source_relevance", "0.15")),
        "topic_score": float(settings.get("weights.topic_score", "0.20")),
        "text_similarity": float(settings.get("weights.text_similarity", "0.20")),
        "author_affinity": float(settings.get("weights.author_affinity", "0.15")),
        "journal_affinity": float(settings.get("weights.journal_affinity", "0.05")),
        "recency_boost": float(settings.get("weights.recency_boost", "0.10")),
        "citation_quality": float(settings.get("weights.citation_quality", "0.05")),
        "feedback_adj": float(settings.get("weights.feedback_adj", "0.10")),
        "preference_affinity": float(settings.get("weights.preference_affinity", "0.10")),
    }

    # -- Apply recommendation mode adjustments --
    rec_mode = settings.get("recommendation_mode", "balanced").lower()
    if rec_mode == "explore":
        # Explore: boost novelty, reduce familiarity
        weights["recency_boost"] *= 1.5
        weights["citation_quality"] *= 0.5
        weights["author_affinity"] *= 0.5
        weights["journal_affinity"] *= 0.5
    elif rec_mode == "exploit":
        # Exploit: boost familiarity, reduce novelty
        weights["author_affinity"] *= 1.5
        weights["journal_affinity"] *= 1.5
        weights["preference_affinity"] *= 1.5
        weights["recency_boost"] *= 0.5
    # balanced: no adjustment

    weight_sum = sum(max(0.0, float(w)) for w in weights.values())
    if weight_sum <= 0:
        uniform = 1.0 / float(len(weights))
        weights = {k: uniform for k in weights}
    else:
        weights = {k: max(0.0, float(w)) / weight_sum for k, w in weights.items()}

    values = {
        "source_relevance": source_relevance,
        "topic_score": topic_score,
        "text_similarity": text_similarity,
        "author_affinity": author_score,
        "journal_affinity": journal_score,
        "recency_boost": recency,
        "citation_quality": citation_quality,
        "feedback_adj": feedback_adj_norm,
        "preference_affinity": pref_affinity,
    }

    final = sum(values[k] * weights[k] for k in weights)
    weighted_score = max(0.0, min(_MAX_DISCOVERY_SCORE, final * _MAX_DISCOVERY_SCORE))

    # Multi-source consensus bonus. `consensus_buckets` is populated by
    # `_merge_channel_candidates`; ad-hoc callers (tests, single-channel
    # scoring) can omit it without penalty. The pre-bonus weighted score
    # is preserved in the breakdown for provenance.
    consensus_buckets = candidate.get("consensus_buckets") or []
    if not isinstance(consensus_buckets, list):
        consensus_buckets = list(consensus_buckets)
    consensus_count = (
        int(candidate.get("consensus_count") or len(consensus_buckets))
        if consensus_buckets
        else int(candidate.get("consensus_count") or 0)
    )
    consensus_bonus = _consensus_bonus(consensus_count)

    # Citation-fabric bonus (task 47 §7): a bounded ADDITIVE nudge, not a
    # reweighting — so it never dilutes the 10 core signals for the many
    # candidates that share no citation structure (their bonus is 0 and their
    # score is unchanged). Coupling (shared references with the loved/saved set)
    # and co-citation (cited together with a loved/saved paper) each contribute
    # up to their configured ceiling, scaled by the precomputed [0,1] strength.
    coupling_bonus_max = float(settings.get("citation_fabric.coupling_bonus_max", "2.5"))
    cocitation_bonus_max = float(settings.get("citation_fabric.cocitation_bonus_max", "2.5"))
    citation_bonus = (
        coupling_strength * coupling_bonus_max
        + cocitation_strength * cocitation_bonus_max
    )

    # Signal Lab bonus (task 54, D20): same bounded-ADDITIVE pattern as the
    # citation-fabric nudge. `lab_ctx` is loaded once per scoring pass by the
    # caller (None unless the lab weights are promoted off 0.0 AND a fitted
    # model exists), so at the default weights this block contributes exactly
    # 0.0 and adds no breakdown keys — byte-identical to a lab-less build.
    lab_bonus = 0.0
    if lab_ctx is not None:
        from alma.application.signal_lab.scoring_terms import compute_lab_adjustments

        lab_offset_raw, lab_utility_raw = compute_lab_adjustments(
            candidate_embedding, lab_ctx
        )
        lab_bonus = (
            lab_ctx["w_offset"] * lab_offset_raw
            + lab_ctx["w_utility"] * lab_utility_raw
        )
    score_pre_dismissal = min(
        _MAX_DISCOVERY_SCORE, weighted_score + consensus_bonus + citation_bonus + lab_bonus
    )

    # Negative paper-signal cluster penalty — function/breakdown names retain
    # "dismissal" for compatibility. Applied
    # AFTER consensus so multi-source agreement can't entirely rescue
    # a candidate that overlaps with what the user has explicitly
    # disliked or removed. Cap inside `_dismissal_cluster_penalty` keeps total
    # ≤ 30 points so a candidate is never zero'd by penalty alone.
    dismissal_penalty, dismissal_parts = _dismissal_cluster_penalty(
        candidate,
        paper_topics,
        authors_str,
        preference_profile.get("projected_feedback"),
    )
    final_score = max(0.0, score_pre_dismissal - dismissal_penalty)

    breakdown: dict[str, Any] = {}
    for signal in weights:
        # Force Python float — semantic similarities arrive as numpy
        # float32 from the cosine path and would otherwise propagate
        # into json.dumps at staging time, blowing up lens refresh
        # with "Object of type float32 is not JSON serializable".
        v = round(float(values[signal]), 4)
        w = float(weights[signal])
        breakdown[signal] = {
            "value": v,
            "weight": w,
            "weighted": round(v * w, 4),
        }
    breakdown["usefulness_boost"] = {
        "value": round(float(usefulness_boost), 4),
        "weight": 0.0,
        "weighted": 0.0,
        "diagnostic_only": True,
    }
    breakdown["final_score"] = round(float(final_score), 4)
    breakdown["weighted_score_pre_consensus"] = round(float(weighted_score), 4)
    breakdown["consensus_buckets"] = list(consensus_buckets)
    breakdown["consensus_count"] = consensus_count
    breakdown["consensus_bonus"] = round(consensus_bonus, 4)
    breakdown["score_pre_dismissal"] = round(float(score_pre_dismissal), 4)
    breakdown["dismissal_penalty"] = round(float(dismissal_penalty), 4)
    breakdown["dismissal_penalty_parts"] = dismissal_parts
    breakdown["source_type"] = candidate.get("source_type", "")
    breakdown["source_key"] = candidate.get("source_key", "")
    breakdown["text_similarity_mode"] = text_similarity_mode
    breakdown["semantic_similarity_raw"] = round(float(semantic_similarity_raw or 0.0), 4)
    breakdown["lexical_similarity_raw"] = round(float(lexical_similarity_raw or 0.0), 4)
    breakdown["semantic_similarity_centroid_raw"] = float(semantic_details.get("positive_centroid_raw") or 0.0)
    breakdown["semantic_similarity_exemplar_raw"] = float(semantic_details.get("positive_exemplar_raw") or 0.0)
    breakdown["semantic_similarity_support_raw"] = float(semantic_details.get("positive_support_raw") or 0.0)
    breakdown["semantic_similarity_signal_raw"] = float(semantic_details.get("positive_signal_raw") or 0.0)
    breakdown["semantic_similarity_negative_raw"] = round(
        max(
            float(semantic_details.get("negative_centroid_raw") or 0.0),
            float(semantic_details.get("negative_exemplar_raw") or 0.0),
        ),
        4,
    )
    breakdown["semantic_similarity_negative_signal_raw"] = float(semantic_details.get("negative_signal_raw") or 0.0)
    breakdown["lexical_similarity_word_raw"] = float(lexical_details.get("word_raw") or 0.0)
    breakdown["lexical_similarity_char_raw"] = float(lexical_details.get("char_raw") or 0.0)
    breakdown["lexical_similarity_term_raw"] = float(lexical_details.get("term_raw") or 0.0)
    breakdown["lexical_similarity_negative_penalty"] = float(lexical_details.get("negative_penalty") or 0.0)
    breakdown["text_similarity_semantic_weight"] = round(float(semantic_blend_weight), 3)
    breakdown["text_similarity_lexical_weight"] = round(float(lexical_blend_weight), 3)
    breakdown["candidate_embedding_ready"] = bool(semantic_details.get("candidate_embedding_ready"))
    breakdown["topic_match_mode"] = topic_match_mode
    breakdown["projected_feedback_raw"] = round(float(projected_adj or 0.0), 4)
    breakdown["projected_feedback_axes_raw"] = {
        key: round(float(value), 6)
        for key, value in projected_axes.items()
    }
    breakdown["author_affinity_atoms"] = {
        "max": round(max(author_affinity_values, default=0.0), 6),
        "mean": round(
            sum(author_affinity_values) / max(1, len(author_affinity_values)), 6
        ),
        "first": round(author_affinity_values[0], 6)
        if author_affinity_values
        else 0.0,
        "last": round(author_affinity_values[-1], 6)
        if author_affinity_values
        else 0.0,
        "followed": 1.0 if followed_author_match else 0.0,
        "evidence_count": len(author_affinity_values),
    }
    # Citation-fabric provenance (task 47 §7): the two [0,1] strengths, the bonus
    # they earned, and — when a channel fired — the raw count + the single
    # best-matching high-signal paper id, so the UI can render an evidence string
    # ("shares N references with <title>", "cited together with <title> in N
    # papers"). Strengths are always emitted; counts/partners only when non-zero.
    breakdown["coupling_strength"] = round(coupling_strength, 4)
    breakdown["cocitation_strength"] = round(cocitation_strength, 4)
    breakdown["citation_bonus"] = round(float(citation_bonus), 4)
    if lab_ctx is not None:
        breakdown["lab_region_offset_raw"] = round(float(lab_offset_raw), 4)
        breakdown["lab_utility_raw"] = round(float(lab_utility_raw), 4)
        breakdown["lab_bonus"] = round(float(lab_bonus), 4)
    if cf.get("coupling_count"):
        breakdown["coupling_count"] = int(cf.get("coupling_count") or 0)
        if cf.get("coupling_partner_id"):
            breakdown["coupling_partner_id"] = str(cf["coupling_partner_id"])
        if cf.get("coupling_partner_title"):
            breakdown["coupling_partner_title"] = str(cf["coupling_partner_title"])
    if cf.get("cocitation_count"):
        breakdown["cocitation_count"] = int(cf.get("cocitation_count") or 0)
        if cf.get("cocitation_partner_id"):
            breakdown["cocitation_partner_id"] = str(cf["cocitation_partner_id"])
        if cf.get("cocitation_partner_title"):
            breakdown["cocitation_partner_title"] = str(cf["cocitation_partner_title"])

    return final_score, breakdown


def _projected_feedback_axes(
    candidate: dict,
    paper_topics: list[dict],
    authors_str: str,
    projected: Any,
) -> dict[str, float]:
    """Return all nine raw projection axes before model weights or clamping."""

    axis_names = (
        "paper",
        "author",
        "author_name",
        "topic",
        "venue",
        "keyword",
        "tag",
        "semantic_neighbor",
        "citation_neighbor",
    )
    if not isinstance(projected, ProjectedPaperSignals):
        return {
            key: 0.0
            for axis in axis_names
            for key in (axis, f"{axis}_evidence_count")
        }

    axes = {
        key: 0.0
        for axis in axis_names
        for key in (axis, f"{axis}_evidence_count")
    }

    def add(axis: str, value: float) -> None:
        axes[axis] += float(value)
        axes[f"{axis}_evidence_count"] += 1.0

    paper_id = str(candidate.get("paper_id") or candidate.get("id") or "").strip().lower()
    if paper_id:
        add("paper", projected.paper.get(paper_id, 0.0))
        add("semantic_neighbor", projected.semantic_neighbor.get(paper_id, 0.0))
        add("citation_neighbor", projected.citation_neighbor.get(paper_id, 0.0))

    journal = str(candidate.get("journal") or "").strip().lower()
    if journal:
        add("venue", projected.venue.get(journal, 0.0))

    for topic in paper_topics or []:
        term = str(topic.get("term") or topic.get("name") or "").strip().lower()
        if not term:
            continue
        try:
            # 44.1: shared default only (no floor/clamp — A/B-gated).
            topic_strength = float(topic.get("score") or topics.DEFAULT_TOPIC_SCORE)
        except (TypeError, ValueError):
            topic_strength = topics.DEFAULT_TOPIC_SCORE
        add(
            "topic",
            _clamp(topic_strength, 0.1, 1.0)
            * float(projected.topic.get(term, 0.0)),
        )

    for keyword in _candidate_keywords(candidate):
        add("keyword", projected.keyword.get(keyword, 0.0))
        add("tag", projected.tag.get(keyword, 0.0))

    for author_id in _candidate_author_ids(candidate):
        add("author", projected.author.get(author_id, 0.0))

    for author_name in parse_author_names(authors_str):
        add(
            "author_name",
            projected.author_name.get(author_name.strip().lower(), 0.0),
        )

    return axes


def _dismissal_cluster_penalty(
    candidate: dict,
    paper_topics: list[dict],
    authors_str: str,
    projected: Any,
) -> tuple[float, dict[str, float]]:
    """Penalty in *score points* from candidate's overlap with negative projected signals.

    Mirror of `alma.application.authors._dismissal_overlap_penalty`.
    The author rail applies a dedicated post-weighted-sum penalty (up
    to 30% of band) so explicit negative preference can pull harder
    than the bounded `feedback_adj` signal alone. Same idea here.

    Reads the negative side of `ProjectedPaperSignals.{topic, venue,
    author, author_name, keyword, tag}` — these are already
    propagated from dislike / remove / unsave events upstream in
    `signal_projection`. For each candidate axis (topic of the paper,
    journal, paper authors, …) we look up the projected magnitude;
    only the negative-signed contribution adds to the penalty.

    Returns ``(penalty_points, parts)`` where ``parts`` is a per-axis
    breakdown (``"topic"``, ``"venue"``, ``"author"``,
    ``"author_name"``, ``"keyword"``) for provenance.
    """
    parts: dict[str, float] = {}
    if not isinstance(projected, ProjectedPaperSignals):
        return 0.0, parts

    def _neg(value: float) -> float:
        v = float(value or 0.0)
        return -v if v < 0.0 else 0.0

    venue_pen = 0.0
    journal = str(candidate.get("journal") or "").strip().lower()
    if journal:
        venue_pen = _DISMISSAL_VENUE_PENALTY_PER_HIT * _neg(projected.venue.get(journal, 0.0))
    if venue_pen:
        parts["venue"] = round(venue_pen, 3)

    topic_pen = 0.0
    for topic in paper_topics or []:
        term = str(topic.get("term") or topic.get("name") or "").strip().lower()
        if not term:
            continue
        try:
            # 44.1: shared default only (no floor/clamp — A/B-gated).
            topic_strength = float(topic.get("score") or topics.DEFAULT_TOPIC_SCORE)
        except (TypeError, ValueError):
            topic_strength = topics.DEFAULT_TOPIC_SCORE
        topic_pen += (
            _DISMISSAL_TOPIC_PENALTY_PER_HIT
            * _clamp(topic_strength, 0.1, 1.0)
            * _neg(projected.topic.get(term, 0.0))
        )
    if topic_pen:
        parts["topic"] = round(topic_pen, 3)

    keyword_pen = 0.0
    for keyword in _candidate_keywords(candidate):
        keyword_pen += _DISMISSAL_KEYWORD_PENALTY_PER_HIT * _neg(
            projected.keyword.get(keyword, 0.0)
        )
        keyword_pen += _DISMISSAL_KEYWORD_PENALTY_PER_HIT * _neg(
            projected.tag.get(keyword, 0.0)
        )
    if keyword_pen:
        parts["keyword"] = round(keyword_pen, 3)

    author_pen = 0.0
    for author_id in _candidate_author_ids(candidate):
        author_pen += _DISMISSAL_AUTHOR_PENALTY_PER_HIT * _neg(
            projected.author.get(author_id, 0.0)
        )
    if author_pen:
        parts["author"] = round(author_pen, 3)

    author_name_pen = 0.0
    for author_name in parse_author_names(authors_str):
        author_name_pen += _DISMISSAL_AUTHOR_NAME_PENALTY_PER_HIT * _neg(
            projected.author_name.get(author_name.strip().lower(), 0.0)
        )
    if author_name_pen:
        parts["author_name"] = round(author_name_pen, 3)

    total = topic_pen + venue_pen + author_pen + author_name_pen + keyword_pen
    return min(_DISMISSAL_PENALTY_CAP, total), parts


def _candidate_author_ids(candidate: dict) -> list[str]:
    out: list[str] = []
    seen: set[str] = set()
    for authorship in candidate.get("authorships") or []:
        if not isinstance(authorship, dict):
            continue
        author = authorship.get("author")
        raw = (
            authorship.get("openalex_id")
            or authorship.get("author_id")
            or (author.get("id") if isinstance(author, dict) else None)
        )
        normalized = str(raw or "").strip().rstrip("/").split("/")[-1].lower()
        if normalized and normalized not in seen:
            seen.add(normalized)
            out.append(normalized)
    for key in ("author_openalex_ids", "openalex_author_ids", "author_ids"):
        raw = candidate.get(key)
        values: list[Any]
        if isinstance(raw, list):
            values = raw
        elif isinstance(raw, str):
            values = re.split(r"[,;]", raw)
        else:
            values = []
        for value in values:
            normalized = str(value or "").strip().lower()
            if normalized and normalized not in seen:
                seen.add(normalized)
                out.append(normalized)
    return out


def _candidate_keywords(candidate: dict) -> list[str]:
    # Keep the keyword|tags fallback selection here; delegate the actual parsing to
    # the shared single-source parser (alma.core.keywords.parse_keywords). Real
    # discovery candidates carry Python lists of strings, on which parse_keywords is
    # identical to the previous inline split; the only added capability is correctly
    # decoding a JSON-*string* candidate (characterized in tests).
    return parse_keywords(candidate.get("keywords") or candidate.get("tags") or [])
