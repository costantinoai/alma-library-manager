"""Canonical defaults for discovery settings."""

from __future__ import annotations

from typing import Mapping

from alma.discovery.semantic_scholar import S2_SPECTER2_MODEL


DISCOVERY_SETTINGS_DEFAULTS: dict[str, str] = {
    "weights.source_relevance": "0.15",
    "weights.topic_score": "0.20",
    "weights.text_similarity": "0.20",
    "weights.author_affinity": "0.15",
    "weights.journal_affinity": "0.05",
    "weights.recency_boost": "0.10",
    "weights.citation_quality": "0.05",
    "weights.feedback_adj": "0.10",
    "weights.preference_affinity": "0.10",
    "weights.usefulness_boost": "0.06",
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
    "limits.max_results": "50",
    "limits.max_candidates_per_strategy": "20",
    "limits.recency_window_years": "10",
    "limits.feedback_decay_days_full": "90",
    "limits.feedback_decay_days_half": "180",
    "limits.taste_topic_queries": "3",
    "limits.taste_author_queries": "3",
    "limits.taste_venue_queries": "2",
    "limits.recent_win_queries": "2",
    "branches.temperature": "0.28",
    "branches.max_clusters": "6",
    "branches.max_active_for_retrieval": "4",
    "branches.query_core_variants": "2",
    "branches.query_explore_variants": "2",
    # Absolute minimum per-branch budget for the external retrieval
    # lane. Without a floor, a low-auto_weight branch can be starved
    # to 4-5 recommendations — too few to ever accumulate enough
    # save/dismiss signal to recover. 8 keeps an "underexplored"
    # branch viable while the user evaluates it.
    "branches.min_budget_per_branch": "8",
    "lens.max_seeds": "500",
    "sources.openalex.enabled": "true",
    "sources.semantic_scholar.enabled": "true",
    "sources.crossref.enabled": "true",
    "sources.arxiv.enabled": "true",
    "sources.biorxiv.enabled": "true",
    "sources.openalex.weight": "1.0",
    "sources.semantic_scholar.weight": "0.95",
    "sources.crossref.weight": "0.72",
    "sources.arxiv.weight": "0.66",
    "sources.biorxiv.weight": "0.62",
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
    "schedule.refresh_interval_hours": "0",
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
    "paper_signal_weights.signal_lab": "0.10",
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
