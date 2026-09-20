"""Canonical defaults for discovery settings."""

from __future__ import annotations

from collections.abc import Mapping

from alma.discovery.semantic_scholar import S2_SPECTER2_MODEL

# THE default signal weights — one owner. The ranker's family specs, the API
# model, the settings defaults and the reset route all read this table.
#
# Fitted, not picked (2026-09-18, `application/discovery/outcome_eval.py`):
# non-negative logistic weights that best separate papers the user later KEPT
# from papers they REJECTED and from the random corpus, under a temporal
# holdout through the real scoring path, selected by 5-fold cross-validation,
# on a Library of 539 papers (280 kept / 50 rejected / 600 corpus). Out of
# sample the previous defaults scored AUC 0.65 vs rejected / 0.58 vs corpus;
# these score 0.91 / 0.80. What the fit found, on two snapshots alike:
#   * embedding similarity carries the ranking (alone: 0.93 / 0.77);
#   * author affinity ANTI-predicts against the corpus — a corpus is mostly the
#     back-catalogue of followed authors, which the Feed already delivers;
#   * topic overlap adds nothing beyond the embedding; recency points the wrong
#     way against rejected papers.
# Zero is a fitted value, not an omission: the sliders bring any family back.
# `source_relevance`, `feedback_adj` and `preference_affinity` cannot be
# measured offline (no retrieval evidence; they read the very events under
# test), so they keep their previous share.
DEFAULT_SIGNAL_WEIGHTS: dict[str, float] = {
    "source_relevance": 0.14,
    "topic_score": 0.0,
    "text_similarity": 0.51,
    "author_affinity": 0.0,
    "journal_affinity": 0.05,
    "recency_boost": 0.0,
    "citation_quality": 0.12,
    "feedback_adj": 0.09,
    "preference_affinity": 0.09,
}
# How the one `text_similarity` slider splits between its two families
# (fitted: semantic 0.444, lexical 0.068).
TEXT_SIMILARITY_SEMANTIC_SHARE = 0.87

DISCOVERY_SETTINGS_DEFAULTS: dict[str, str] = {
    **{f"weights.{key}": str(value) for key, value in DEFAULT_SIGNAL_WEIGHTS.items()},
    # Citation-fabric bonuses (task 47 §7): bounded ADDITIVE nudges (not weights)
    # for candidates that share citation structure with the loved/saved set —
    # coupling (shared references) + co-citation (shared citers). Each scales its
    # ceiling by the precomputed [0,1] strength, so citation-less candidates are
    # unaffected and proven signals are never diluted.
    "citation_fabric.coupling_bonus_max": "2.5",
    "citation_fabric.cocitation_bonus_max": "2.5",
    "strategies.related_works": "true",
    "strategies.topic_search": "true",
    "strategies.followed_authors": "true",
    "strategies.coauthor_network": "true",
    "strategies.citation_chain": "true",
    "strategies.semantic_scholar": "true",
    # S2 list-mode recommendations — calls
    # `POST /recommendations/v1/papers` with the user's top-rated
    # Library papers as positive seeds and their removed/dismissed/
    # disliked papers as negative seeds. Complementary to the
    # free-text-query `semantic_scholar` lane: this one uses S2's
    # learned model directly on paper IDs.
    "strategies.s2_recommend": "true",
    "strategies.branch_explorer": "true",
    "strategies.taste_topics": "true",
    "strategies.taste_authors": "true",
    "strategies.taste_venues": "true",
    "strategies.recent_wins": "true",
    # Scale each lens channel by how often what it surfaced was kept
    # (`application/discovery/channel_yield.py`).
    "strategies.adaptive_channels": "true",
    "limits.max_results": "50",
    # Minimum recommendation score (0-100 scale, matching the emitted
    # `score`). Recommendations scoring below this are dropped at staging time
    # so the feed doesn't pad itself with weak, off-topic matches once the
    # genuinely relevant neighbours run out. 0 = keep everything (legacy
    # behaviour); raise it to tighten the relevance floor.
    "limits.min_score": "0",
    "limits.max_candidates_per_strategy": "20",
    "limits.recency_window_years": "10",
    "limits.feedback_decay_days_full": "90",
    "limits.feedback_decay_days_half": "180",
    # Seconds a single retrieval lane may take before the refresh gives up on
    # it and builds the deck without it. Every lane is a LOCAL read (network
    # collection belongs to scheduled maintenance), so this is a backstop
    # against pathological computation, not a normal budget.
    #
    # It was a hardcoded 8.0 and that made it a binding constraint rather than
    # a backstop: the external lane waits on the shared preference profile,
    # which took 7.8 s, so external was cut on EVERY refresh and the deck
    # silently lost a whole retrieval family (measured 2026-07-27). The profile
    # is now ~3.6 s and the ceiling is a setting, so a slow box can raise it
    # instead of quietly shipping three-quarters of a deck.
    "limits.lane_deadline_seconds": "30",
    "limits.taste_topic_queries": "3",
    "limits.taste_author_queries": "3",
    "limits.taste_venue_queries": "2",
    "limits.recent_win_queries": "2",
    "branches.temperature": "0.28",
    "branches.max_clusters": "6",
    "branches.max_active_for_retrieval": "4",
    "branches.query_core_variants": "2",
    "branches.query_explore_variants": "2",
    "lens.max_seeds": "500",
    "sources.openalex.enabled": "true",
    "sources.semantic_scholar.enabled": "true",
    "sources.crossref.enabled": "true",
    "sources.arxiv.enabled": "true",
    "sources.biorxiv.enabled": "true",
    "sources.europe_pmc.enabled": "true",
    # Semantic Scholar bulk-search filters (T12, 2026-04-25).  All opt-in
    # (empty default = no filter emitted → same request shape as before),
    # so existing lens behavior is preserved for users who don't set
    # them.  Comma-separated values map to S2's `fieldsOfStudy`,
    # `publicationTypes` query params.  `open_access_pdf` is a flag.
    #
    # S2 `fieldsOfStudy` accepts: Computer Science, Medicine, Biology,
    #   Chemistry, Materials Science, Physics, Geology, Psychology,
    #   Art, History, Geography, Sociology, Business, Political Science,
    #   Economics, Philosophy, Mathematics, Engineering,
    #   Environmental Science, Agricultural and Food Sciences, Education,
    #   Law, Linguistics.
    # S2 `publicationTypes` accepts: Review, JournalArticle, CaseReport,
    #   ClinicalTrial, Dataset, Editorial, LettersAndComments,
    #   MetaAnalysis, News, Study, Book, BookSection.
    "sources.semantic_scholar.fields_of_study": "",
    "sources.semantic_scholar.publication_types": "",
    "sources.semantic_scholar.open_access_pdf": "false",
    "monitor_defaults.author_per_refresh": "20",
    "monitor_defaults.search_limit": "15",
    "monitor_defaults.search_temperature": "0.22",
    "monitor_defaults.recency_years": "2",
    "monitor_defaults.include_preprints": "true",
    "monitor_defaults.semantic_scholar_bulk": "true",
    "embedding_model": S2_SPECTER2_MODEL,
    # Auto-refresh is opt-in (default OFF). A periodic job registers only when
    # its `*_enabled` flag is true AND its interval is > 0. The page toggle
    # writes the `*_enabled` flag; Settings writes the interval. The interval
    # default is a sensible 6h so flipping the toggle on "just works" without a
    # second step. Out of the box everything stays OFF because the flags default
    # to "false".
    "schedule.refresh_enabled": "false",
    "schedule.refresh_interval_hours": "6",
    "schedule.feed_refresh_enabled": "false",
    "schedule.feed_refresh_interval_hours": "6",
    "schedule.graph_maintenance_interval_hours": "24",
    "cache.similarity_ttl_hours": "24",
    "recommendation_mode": "balanced",
    # D12 paper-signal composite (blends rating + topic + embedding +
    # author centroid + signal-lab + recency). Used wherever a single
    # "how strong is this paper as a signal right now" number is
    # needed — today: seed selection for network author suggestions.
    # Missing components get their weight redistributed to the
    # present ones, so a paper without a vector still scores.
    "paper_signal_weights.rating": "0.20",
    "paper_signal_weights.topic_alignment": "0.20",
    "paper_signal_weights.embedding_sim": "0.25",
    "paper_signal_weights.author_alignment": "0.15",
    "paper_signal_weights.feedback_events": "0.10",
    "paper_signal_weights.recency": "0.10",
    # D12 candidate-author composite (score of a candidate surfaced by
    # openalex_related / s2_related). Also uses the redistribution
    # rule — a candidate without a centroid still ranks via
    # topic_overlap / seed_cooccurrence / venue_overlap / recency.
    "candidate_author_weights.seed_cooccurrence": "0.25",
    "candidate_author_weights.topic_overlap": "0.25",
    "candidate_author_weights.centroid_sim": "0.20",
    "candidate_author_weights.venue_overlap": "0.15",
    "candidate_author_weights.recency_activity": "0.10",
    "candidate_author_weights.h_index_soft": "0.05",
    # D12 AUTH-SUG-5 bucket weights. Applied to each candidate's raw
    # bucket score in list_author_suggestions. Priority-based dedup
    # still runs first so `cited_by_high_signal > adjacent` label
    # precedence stays intact; weights only reorder the final list.
    #
    # External-network buckets (openalex_related / s2_related) carry
    # the discovery value of the rail — they surface authors the user
    # has NOT already co-authored with or cited. Library_core /
    # adjacent / cited_by_high_signal will always have evidence the
    # network buckets cannot match (raw co-authorship, citation graph
    # presence) so a flat 0.5 weight against library_core's 1.0 was
    # silently starving the discovery side. Equal-footing 0.9 lets a
    # well-scored OpenAlex/S2-related candidate compete for the rail
    # without overtaking a candidate who is literally a library
    # co-author of a 5★ paper. cited_by_high_signal also bumped (it
    # uses ratings now, same trust level as library_core).
    "author_suggestion_weights.library_core": "1.0",
    "author_suggestion_weights.cited_by_high_signal": "0.9",
    "author_suggestion_weights.adjacent": "0.7",
    "author_suggestion_weights.semantic_similar": "0.8",
    "author_suggestion_weights.openalex_related": "0.9",
    "author_suggestion_weights.s2_related": "0.9",
    # TTL for `author_suggestion_cache` rows (network bucket payloads).
    "author_suggestion_cache_ttl_hours": "24",
}


def merge_discovery_defaults(values: Mapping[str, str] | None = None) -> dict[str, str]:
    """Return discovery settings merged with canonical defaults."""
    merged = dict(DISCOVERY_SETTINGS_DEFAULTS)
    if values:
        for key, value in values.items():
            merged[key] = value
    return merged
