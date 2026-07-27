"""Discovery candidate scoring loop.

The per-candidate scoring pass lifted out of ``refresh_lens_recommendations``
(D-9) into a standalone, testable function. For each merged candidate it:

  * passes family-balanced retrieval relevance without source-identity feedback,
  * runs the core signal scorer (mutating the candidate in place
    with ``score`` + ``score_breakdown`` + truthful provenance), and
  * accumulates the scoring-profile aggregates the orchestrator logs and uses
    to drive diversity selection.

Pure computation: ``score_candidate`` only *reads* the DB for signal lookups, so
this loop performs no writes. The read-only inputs are bundled in
``ScoringContext`` (instead of ~19 loose params) and the accumulators are
returned as ``ScoringAggregates``; candidate dicts are still mutated in place,
which the caller relies on for the subsequent ranking + persistence.
"""

from __future__ import annotations

import math
import sqlite3
from dataclasses import dataclass
from typing import Any

from alma.discovery.scoring import score_candidate

# The independent signal families scored per candidate, in canonical order.
# ``usefulness_boost`` is retained as a diagnostic atomic decomposition in the
# scorer, but is intentionally not a model input: it deterministically repeats
# recency/citation/similarity and metadata completeness.
# single source of truth; the orchestrator imports this for its post-loop
# averaging so the two can never drift.
SIGNAL_NAMES = (
    "source_relevance",
    "topic_score",
    "text_similarity",
    "author_affinity",
    "journal_affinity",
    "recency_boost",
    "citation_quality",
    "feedback_adj",
    "preference_affinity",
)


@dataclass
class ScoringContext:
    """Read-only inputs for :func:`score_candidates` (built once per refresh)."""

    db: sqlite3.Connection
    profile: dict
    scoring_settings: dict
    positive_centroid: Any
    negative_centroid: Any
    positive_texts: Any
    negative_texts: Any
    positive_example_embeddings: Any
    negative_example_embeddings: Any
    candidate_text_map: dict
    candidate_embedding_map: dict
    citation_fabric_map: dict
    lexical_profile: Any
    precomputed_lexical_map: dict
    user_topic_embeddings: Any
    preloaded_preference_profile: Any
    topic_provider: Any
    lab_ctx: Any = None  # Signal Lab scoring context (task 54); None ⇒ lab off


@dataclass
class ScoringAggregates:
    """Per-refresh scoring-profile accumulators returned by the loop."""

    signal_value_sums: dict
    signal_weighted_sums: dict
    text_mode_counts: dict
    topic_mode_counts: dict
    raw_semantic_scores: list
    raw_semantic_exemplar_scores: list
    raw_semantic_support_scores: list
    raw_lexical_scores: list
    raw_lexical_word_scores: list
    raw_lexical_char_scores: list
    raw_lexical_term_scores: list
    final_scores: list
    embedding_ready_count: int
    compressed_similarity_count: int
    low_similarity_count: int


def score_candidates(merged: dict, ctx: ScoringContext) -> ScoringAggregates:
    """Score every candidate in ``merged`` in place; return the aggregates.

    The loop body is a verbatim lift of the inline scoring pass that used to live
    in ``refresh_lens_recommendations`` — only the surrounding inputs/outputs were
    formalized into ``ScoringContext`` / ``ScoringAggregates``.
    """
    # Unpack the context into the local names the lifted loop body expects.
    db = ctx.db
    profile = ctx.profile
    scoring_settings = ctx.scoring_settings
    positive_centroid = ctx.positive_centroid
    negative_centroid = ctx.negative_centroid
    positive_texts = ctx.positive_texts
    negative_texts = ctx.negative_texts
    positive_example_embeddings = ctx.positive_example_embeddings
    negative_example_embeddings = ctx.negative_example_embeddings
    candidate_text_map = ctx.candidate_text_map
    candidate_embedding_map = ctx.candidate_embedding_map
    citation_fabric_map = ctx.citation_fabric_map
    lexical_profile = ctx.lexical_profile
    precomputed_lexical_map = ctx.precomputed_lexical_map
    user_topic_embeddings = ctx.user_topic_embeddings
    preloaded_preference_profile = ctx.preloaded_preference_profile
    _topic_provider = ctx.topic_provider
    signal_names = SIGNAL_NAMES

    signal_value_sums = {name: 0.0 for name in signal_names}
    signal_weighted_sums = {name: 0.0 for name in signal_names}
    text_mode_counts: dict[str, int] = {}
    topic_mode_counts: dict[str, int] = {}
    raw_semantic_scores: list[float] = []
    raw_semantic_exemplar_scores: list[float] = []
    raw_semantic_support_scores: list[float] = []
    raw_lexical_scores: list[float] = []
    raw_lexical_word_scores: list[float] = []
    raw_lexical_char_scores: list[float] = []
    raw_lexical_term_scores: list[float] = []
    final_scores: list[float] = []
    embedding_ready_count = 0
    compressed_similarity_count = 0
    low_similarity_count = 0
    for key, candidate in merged.items():
        candidate["retrieval_score_breakdown"] = dict(
            candidate.get("score_breakdown") or {}
        )
        # Family-balanced fusion becomes source relevance.  Source/API/lane
        # identities are exposure variables, never reward inputs: learning a
        # multiplier from outcomes produced by the same ranking policy creates
        # a self-reinforcing exposure loop.
        raw_source_relevance = min(1.0, candidate["score"] / 100.0)
        candidate["source_relevance"] = raw_source_relevance
        citation_fabric = dict(citation_fabric_map.get(key) or {})
        # Frontier/reference retrieval knows exact local overlap even before a
        # lead is promoted into `papers`. Preserve that graph signal instead of
        # zeroing it merely because citation_fabric's paper-id join cannot see
        # frontier rows yet.
        coupling_count = int(candidate.get("coupling_count") or 0)
        if coupling_count > 0 and not citation_fabric.get("coupling_count"):
            citation_fabric["coupling_count"] = coupling_count
            citation_fabric["coupling_strength"] = 1.0 - math.exp(
                -coupling_count / 3.0
            )
        cocitation_count = int(candidate.get("cocitation_count") or 0)
        if cocitation_count > 0 and not citation_fabric.get("cocitation_count"):
            citation_fabric["cocitation_count"] = cocitation_count
            citation_fabric["cocitation_strength"] = 1.0 - math.exp(
                -cocitation_count / 3.0
            )
        candidate["citation_fabric_available"] = bool(citation_fabric)
        final_score, breakdown = score_candidate(
            candidate, profile,
            positive_centroid, negative_centroid,
            positive_texts, negative_texts,
            db, scoring_settings,
            candidate_text=candidate_text_map.get(key),
            candidate_embedding=candidate_embedding_map.get(key),
            lexical_profile=lexical_profile,
            positive_example_embeddings=positive_example_embeddings,
            negative_example_embeddings=negative_example_embeddings,
            precomputed_lexical_details=precomputed_lexical_map.get(key),
            user_topic_embeddings=user_topic_embeddings,
            preloaded_preference_profile=preloaded_preference_profile,
            topic_provider=_topic_provider,
            citation_fabric=citation_fabric,
            lab_ctx=ctx.lab_ctx,
        )
        candidate["score"] = final_score
        # Fold retrieval provenance ("why this paper surfaced") into the
        # persisted breakdown so the UI can explain more than the branch
        # label: the actual query string that found it, and the core /
        # explore topic hints that defined the branch.
        matched_query = str(candidate.get("matched_query") or "").strip()
        if matched_query:
            breakdown["matched_query"] = matched_query
        branch_core = [t for t in (candidate.get("branch_core_topics") or []) if t]
        if branch_core:
            breakdown["branch_core_topics"] = branch_core
        branch_explore = [t for t in (candidate.get("branch_explore_topics") or []) if t]
        if branch_explore:
            breakdown["branch_explore_topics"] = branch_explore
        breakdown["ppr_library_raw"] = float(candidate.get("ppr_library") or 0.0)
        breakdown["ppr_loved_raw"] = float(candidate.get("ppr_loved") or 0.0)
        # T4: promote the "truthful provenance" numbers into a clean
        # sub-dict the UI can consume without inspecting the full 60+
        # raw-diagnostic keys. Every number here already exists
        # somewhere in `breakdown` (raw diagnostics) or `candidate`
        # (scoring inputs) — we're just giving the frontend a single
        # canonical place to look.
        specter_cosine = float(breakdown.get("semantic_similarity_raw") or 0.0)
        lexical_similarity_raw = float(breakdown.get("lexical_similarity_raw") or 0.0)
        negative_hit_raw = float(breakdown.get("semantic_similarity_negative_raw") or 0.0)
        candidate_author_text = str(candidate.get("authors") or "").lower()
        profile_authors = [
            str(name or "").lower()
            for name in (profile.get("author_affinity") or {}).keys()
            if name
        ]
        shared_authors: list[str] = []
        if candidate_author_text and profile_authors:
            for name in profile_authors[:50]:
                if len(name) >= 4 and name in candidate_author_text:
                    shared_authors.append(name)
                    if len(shared_authors) >= 5:
                        break
        breakdown["provenance"] = {
            # Normalized 0..1 for the frontend. Legacy rows that
            # persisted 0..100 still coerce cleanly on read.
            "score_pct": round(float(final_score or 0.0) / 100.0, 4),
            "specter_cosine": round(specter_cosine, 4) if specter_cosine else None,
            "lexical_similarity": round(lexical_similarity_raw, 4) if lexical_similarity_raw else None,
            "negative_hit": round(negative_hit_raw, 4) if negative_hit_raw >= 0.35 else None,
            "shared_authors_count": len(shared_authors) if shared_authors else None,
            "shared_authors_sample": shared_authors[0] if shared_authors else None,
        }

        candidate["score_breakdown"] = breakdown
        final_scores.append(float(final_score or 0.0))
        if breakdown.get("candidate_embedding_ready"):
            embedding_ready_count += 1
        text_mode = str(breakdown.get("text_similarity_mode") or "none")
        topic_mode = str(breakdown.get("topic_match_mode") or "none")
        text_mode_counts[text_mode] = int(text_mode_counts.get(text_mode) or 0) + 1
        topic_mode_counts[topic_mode] = int(topic_mode_counts.get(topic_mode) or 0) + 1
        try:
            raw_semantic_scores.append(float(breakdown.get("semantic_similarity_raw") or 0.0))
        except (TypeError, ValueError):
            pass
        try:
            raw_semantic_exemplar_scores.append(float(breakdown.get("semantic_similarity_exemplar_raw") or 0.0))
        except (TypeError, ValueError):
            pass
        try:
            support_value = float(breakdown.get("semantic_similarity_support_raw") or 0.0)
            raw_semantic_support_scores.append(support_value)
        except (TypeError, ValueError):
            pass
        try:
            raw_lexical_scores.append(float(breakdown.get("lexical_similarity_raw") or 0.0))
        except (TypeError, ValueError):
            pass
        for target, key_name in (
            (raw_lexical_word_scores, "lexical_similarity_word_raw"),
            (raw_lexical_char_scores, "lexical_similarity_char_raw"),
            (raw_lexical_term_scores, "lexical_similarity_term_raw"),
        ):
            try:
                target.append(float(breakdown.get(key_name) or 0.0))
            except (TypeError, ValueError):
                pass
        try:
            if float((breakdown.get("text_similarity") or {}).get("value") or 0.0) < 0.24:
                low_similarity_count += 1
        except Exception:
            pass
        try:
            if float(breakdown.get("semantic_similarity_raw") or 0.0) > 0.0 and float(breakdown.get("semantic_similarity_raw") or 0.0) < 0.14:
                compressed_similarity_count += 1
        except Exception:
            pass
        for signal_name in signal_names:
            signal_detail = breakdown.get(signal_name) or {}
            if not isinstance(signal_detail, dict):
                continue
            signal_value_sums[signal_name] += float(signal_detail.get("value") or 0.0)
            signal_weighted_sums[signal_name] += float(signal_detail.get("weighted") or 0.0)

    return ScoringAggregates(
        signal_value_sums=signal_value_sums,
        signal_weighted_sums=signal_weighted_sums,
        text_mode_counts=text_mode_counts,
        topic_mode_counts=topic_mode_counts,
        raw_semantic_scores=raw_semantic_scores,
        raw_semantic_exemplar_scores=raw_semantic_exemplar_scores,
        raw_semantic_support_scores=raw_semantic_support_scores,
        raw_lexical_scores=raw_lexical_scores,
        raw_lexical_word_scores=raw_lexical_word_scores,
        raw_lexical_char_scores=raw_lexical_char_scores,
        raw_lexical_term_scores=raw_lexical_term_scores,
        final_scores=final_scores,
        embedding_ready_count=embedding_ready_count,
        compressed_similarity_count=compressed_similarity_count,
        low_similarity_count=low_similarity_count,
    )
