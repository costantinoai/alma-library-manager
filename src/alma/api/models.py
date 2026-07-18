"""Pydantic models for API request/response validation.

v3: UUID-based papers, discovery lenses, feed items, digest alerts.
"""

from typing import Any

from pydantic import BaseModel, Field

from alma.discovery.semantic_scholar import S2_SPECTER2_MODEL

# ============================================================================
# Author Models
# ============================================================================

class AuthorCreate(BaseModel):
    """Request model for creating a new author."""

    scholar_id: str | None = Field(None, description="Google Scholar ID")
    openalex_id: str | None = Field(None, description="OpenAlex author ID")
    orcid: str | None = Field(None, description="ORCID")
    name: str | None = Field(None, description="Optional display name fallback")


class AuthorResponse(BaseModel):
    """Response model for author data."""

    id: str
    name: str
    added_at: str | None = None
    publication_count: int = 0
    affiliation: str | None = None
    email_domain: str | None = None
    citedby: int | None = None
    h_index: int | None = None
    interests: list[str] | None = None
    url_picture: str | None = None
    works_count: int | None = None
    last_fetched_at: str | None = None
    orcid: str | None = None
    openalex_id: str | None = None
    scholar_id: str | None = None
    author_type: str | None = None
    id_resolution_status: str | None = None
    id_resolution_reason: str | None = None
    id_resolution_updated_at: str | None = None
    # Phase D hierarchical-resolver fields (2026-04-24): method = which
    # tier fired (`orcid_direct` / `openalex_provided` / …), confidence
    # ∈ [0, 1]. Lets the UI render a "resolved" checkmark without
    # re-evaluating the status enum.
    id_resolution_method: str | None = None
    id_resolution_confidence: float | None = None
    monitor_health: str | None = None
    monitor_health_reason: str | None = None
    monitor_last_checked_at: str | None = None
    monitor_last_success_at: str | None = None
    monitor_last_status: str | None = None
    monitor_last_error: str | None = None
    monitor_last_result: dict | None = None
    monitor_papers_found: int | None = None
    monitor_items_created: int | None = None
    background_corpus_state: str | None = None
    background_corpus_detail: str | None = None
    background_corpus_last_success_at: str | None = None
    background_corpus_age_days: int | None = None
    background_corpus_publications: int | None = None
    background_corpus_coverage_ratio: float | None = None


class RelatedWork(BaseModel):
    """One row in a Prior/Derivative Works panel (T6).

    Distinct from `PaperResponse` — we intentionally surface a
    trimmed-down projection so the paper detail dialog renders
    fast. `paper_id` is the local UUID when the referenced work is
    already in our corpus; null when it only lives in S2. `source`
    tags where the row came from so the UI can show a "from
    Semantic Scholar" hint on network-origin rows.
    """

    paper_id: str | None = None
    title: str
    authors: str | None = None
    year: int | None = None
    doi: str | None = None
    url: str | None = None
    journal: str | None = None
    abstract: str | None = None
    tldr: str | None = None
    cited_by_count: int = 0
    influential_citation_count: int = 0
    openalex_id: str | None = None
    semantic_scholar_id: str | None = None
    status: str | None = None
    rating: int | None = None
    # Whether this edge (reference or citation) was classified as
    # influential by S2. Meaningful only on citation rows; always
    # False on reference rows for now.
    is_influential: bool = False
    source: str = "local"  # "local" | "s2_remote"


class RelatedWorksResponse(BaseModel):
    """Response envelope for prior/derivative works.

    `direction` = `"prior"` (papers this paper references) or
    `"derivative"` (papers citing this paper).
    """

    direction: str
    source_paper_id: str
    works: list[RelatedWork] = Field(default_factory=list)
    local_count: int = 0
    remote_count: int = 0


class AuthorSuggestionSignal(BaseModel):
    """One piece of evidence backing an author suggestion.

    T7 (2026-04-24) — replaces the single opaque `suggestion_type`
    label with a priority-ordered list of concrete evidence chips.
    `label` is the user-facing string; `kind` is a stable machine
    tag for analytics / future targeting. `count` / `value` /
    `subject` are optional numeric / string facets the UI can use
    for tooltip-rich rendering.
    """

    kind: str
    label: str
    count: int | None = None
    value: float | None = None
    subject: str | None = None


class AuthorMergeMatch(BaseModel):
    """The followed author a suggestion is a likely name-duplicate of."""

    author_id: str
    name: str
    confidence: str  # 'high' | 'medium' | 'low'


class AuthorSuggestionResponse(BaseModel):
    """Suggested collaborator or adjacent author to monitor."""

    key: str
    name: str
    suggestion_type: str
    score: float = 0.0
    openalex_id: str | None = None
    existing_author_id: str | None = None
    known_author_type: str | None = None
    # Set when this suggestion's NAME matches an author you already follow — the UI
    # offers "merge into <them>" instead of "follow as new". Null = a genuinely new
    # name.
    duplicate_of: AuthorMergeMatch | None = None
    shared_paper_count: int = 0
    shared_followed_count: int = 0
    local_paper_count: int = 0
    recent_paper_count: int = 0
    shared_followed_authors: list[str] = Field(default_factory=list)
    shared_topics: list[str] = Field(default_factory=list)
    shared_venues: list[str] = Field(default_factory=list)
    sample_titles: list[str] = Field(default_factory=list)
    # T7: priority-ordered evidence chips built from bucket-specific
    # signals (`shared_paper_count`, `similarity`, etc.). Capped at
    # 4; frontend renders as a row of neutral StatusBadge chips.
    signals: list[AuthorSuggestionSignal] = Field(default_factory=list)
    negative_signal: float = 0.0
    last_removed_at: str | None = None
    # Multi-source consensus: how many independent buckets surfaced
    # this candidate, plus the bucket labels for tooltip rendering.
    consensus_count: int = 1
    consensus_buckets: list[str] = Field(default_factory=list)
    # Signed score adjustment from projected paper feedback (saves /
    # ratings / dismisses propagated through `signal_projection`).
    # Surfaced as a chip on the card when the magnitude clears 1 point.
    paper_signal_adjustment: float = 0.0
    # Per-bucket outcome-calibration multiplier (1.0 = neutral / fresh
    # DB). Provenance only — already folded into `score`.
    bucket_calibration_multiplier: float = 1.0


class AuthorFollowFromPaperRequest(BaseModel):
    """Request model for following one author from a paper card."""

    paper_id: str = Field(..., description="Paper UUID")
    author_name: str = Field(..., min_length=1, description="Author name shown on the paper card")


class AuthorFollowFromPaperResponse(BaseModel):
    """Follow-author result for paper-card actions."""

    author: AuthorResponse
    created: bool = False
    already_followed: bool = False
    matched_via: str | None = None


# ============================================================================
# Paper Models (v3 — replaces Publication models)
# ============================================================================

class PaperResponse(BaseModel):
    """Response model for a paper."""

    id: str = Field(..., description="Paper UUID")
    title: str
    authors: str | None = None
    year: int | None = None
    journal: str | None = None
    abstract: str | None = None
    url: str | None = None
    doi: str | None = None
    publication_date: str | None = None

    # OpenAlex metadata
    openalex_id: str | None = None
    work_type: str | None = None
    language: str | None = None
    is_oa: bool = False
    oa_status: str | None = None
    oa_url: str | None = None
    is_retracted: bool = False
    fwci: float | None = None
    cited_by_count: int = 0
    referenced_works_count: int = 0
    keywords: list[str] | None = None
    # T5: S2 1-2 sentence AI summary. Dense coverage in CS + biomed,
    # sparse elsewhere; `None` means "S2 didn't supply one", distinct
    # from "empty abstract".
    tldr: str | None = None
    # T5: S2's learned "this citation mattered" count. Supplements
    # `cited_by_count` in the `citation_quality` scoring signal.
    influential_citation_count: int = 0

    # Status and library
    status: str = "tracked"
    rating: int = 0
    notes: str | None = None
    added_at: str | None = None
    added_from: str | None = None
    reading_status: str | None = None
    # Row mtime, populated by the `papers.updated_at` trigger default.
    # Surfaced so diagnostic surfaces (Corpus explorer) can show when a row
    # was last touched; without this declaration Pydantic v2's default
    # `extra='ignore'` drops the column silently even when `SELECT p.*`
    # pulls it.
    updated_at: str | None = None

    # Resolution
    openalex_resolution_status: str | None = None
    openalex_resolution_reason: str | None = None

    # Provenance
    source_id: str | None = None

    # Ranking — paper_signal composite score (rating + topic alignment +
    # embedding similarity + author alignment + signal lab + recency).
    # 0..1 scale; populated lazily on Library list when the user sorts
    # by "signal" (or via the maintenance job). 0 means "never scored";
    # distinct from an actual rank of 0.
    global_signal_score: float = 0.0


class PaperActionRequest(BaseModel):
    """Request model for acting on a paper (add/like/love/dismiss)."""

    paper_id: str = Field(..., description="Paper UUID")
    action: str = Field(..., description="Action: add | like | love | dismiss")
    notes: str | None = None
    rating: int | None = Field(None, ge=0, le=5)


class PaperRateRequest(BaseModel):
    """Request model for rating a paper."""

    paper_id: str = Field(..., description="Paper UUID")
    rating: int = Field(..., ge=0, le=5)


class PaperCreateRequest(BaseModel):
    """Request model for manually adding a paper."""

    title: str = Field(..., min_length=1)
    authors: str | None = None
    year: int | None = None
    journal: str | None = None
    abstract: str | None = None
    url: str | None = None
    doi: str | None = None
    status: str = Field("library", description="Initial status: tracked | library")
    added_from: str = Field("manual", description="Source: manual | import | feed | discovery")


# Legacy compatibility aliases
PublicationResponse = PaperResponse


class PublicationSendItem(BaseModel):
    """Item to send via a plugin from a preview."""

    paper_id: str | None = Field(None, description="Paper UUID (preferred)")
    # Legacy fields for backward compatibility
    author_id: str | None = None
    title: str = ""
    authors: str = ""
    year: int | None = None
    abstract: str | None = None
    url: str | None = None
    citations: int | None = 0
    journal: str | None = None


class SendPublicationsRequest(BaseModel):
    """Request to send a previewed list of publications via a plugin."""

    plugin_name: str | None = None
    target: str | None = None
    items: list[PublicationSendItem]


class SavePublicationsRequest(BaseModel):
    """Request to save previewed publications to the database."""

    items: list[PublicationSendItem]


# ============================================================================
# Feed Models (v3 — new)
# ============================================================================

class FeedItemResponse(BaseModel):
    """Response model for a feed item."""

    id: str
    paper_id: str
    author_id: str
    author_name: str | None = None
    matched_author_ids: list[str] = Field(default_factory=list)
    matched_authors: list[str] = Field(default_factory=list)
    matched_monitors: list[dict] = Field(default_factory=list)
    monitor_id: str | None = None
    monitor_type: str | None = None
    monitor_label: str | None = None
    fetched_at: str
    status: str = "new"
    is_new: bool = False
    signal_value: int = 0
    score_breakdown: dict | None = None
    paper: PaperResponse | None = None


class FeedMonitorCreateRequest(BaseModel):
    """Request model for creating a non-author feed monitor."""

    monitor_type: str = Field(..., description="query (keyword monitor) | topic | venue | preprint | branch")
    query: str = Field(..., min_length=1, description="Search string or boolean keyword expression used by the monitor")
    label: str | None = Field(default=None, description="Optional display label")
    config: dict | None = None


class FeedMonitorUpdateRequest(BaseModel):
    """Request model for updating a feed monitor."""

    query: str | None = Field(default=None, min_length=1, description="Updated search string or boolean keyword expression used by the monitor")
    label: str | None = Field(default=None, description="Optional display label")
    enabled: bool | None = Field(default=None, description="Enable or disable this monitor without deleting it")
    config: dict | None = None


class FeedMonitorResponse(BaseModel):
    """Unified feed monitor response model."""

    id: str
    monitor_type: str
    monitor_key: str
    label: str
    enabled: bool = True
    author_id: str | None = None
    author_name: str | None = None
    openalex_id: str | None = None
    scholar_id: str | None = None
    orcid: str | None = None
    config: dict | None = None
    created_at: str | None = None
    updated_at: str | None = None
    last_checked_at: str | None = None
    last_success_at: str | None = None
    last_status: str | None = None
    last_error: str | None = None
    last_result: dict | None = None
    health: str = "ready"
    health_reason: str | None = None


# ============================================================================
# Discovery Models (v3 — lens-based)
# ============================================================================

class LensCreate(BaseModel):
    """Request model for creating a discovery lens."""

    name: str = Field(..., min_length=1)
    context_type: str = Field(..., description="library_global | collection | topic_keyword | tag")
    context_config: dict | None = None
    weights: dict | None = None


class LensUpdate(BaseModel):
    """Request model for updating a discovery lens."""

    name: str | None = None
    context_config: dict | None = None
    weights: dict | None = None
    branch_controls: dict | None = None
    is_active: bool | None = None


class LensResponse(BaseModel):
    """Response model for a discovery lens."""

    id: str
    name: str
    context_type: str
    context_config: dict | None = None
    weights: dict | None = None
    created_at: str
    last_refreshed_at: str | None = None
    is_active: bool = True
    signal_count: int = 0
    recommendation_count: int = 0
    last_suggestion_set_id: str | None = None
    last_ranker_version: str | None = None
    last_retrieval_summary: dict | None = None
    branch_controls: dict | None = None


class BranchSeedSample(BaseModel):
    """Small paper summary for branch visualization previews."""

    paper_id: str | None = None
    title: str
    year: int | None = None
    rating: int = 0


class BranchPreviewItem(BaseModel):
    """One branch node in the lens branch explorer."""

    id: str
    label: str
    seed_count: int
    branch_score: float = 0.0
    core_topics: list[str] = Field(default_factory=list)
    explore_topics: list[str] = Field(default_factory=list)
    direction_hint: str | None = None
    sample_papers: list[BranchSeedSample] = Field(default_factory=list)
    control_state: str | None = None
    is_pinned: bool = False
    is_boosted: bool = False
    is_muted: bool = False
    is_active: bool = True
    recommendation_count: int = 0
    avg_score: float = 0.0
    positive_rate: float = 0.0
    dismiss_rate: float = 0.0
    engagement_rate: float = 0.0
    unseen: int = 0
    unique_sources: int = 0
    auto_weight: float = 1.0
    auto_weight_reason: str | None = None


class LensBranchPreviewResponse(BaseModel):
    """Tree-like preview of branch structure for a lens."""

    lens_id: str
    lens_name: str | None = None
    context_type: str
    seed_count: int = 0
    temperature: float = 0.0
    resolution: float = 1.0
    generated_at: str
    branches: list[BranchPreviewItem] = Field(default_factory=list)


class SuggestionSetResponse(BaseModel):
    """Response model for a suggestion set."""

    id: str
    lens_id: str
    context_type: str
    trigger_source: str
    retrieval_summary: dict | None = None
    ranker_version: str | None = None
    created_at: str


class RecommendationResponse(BaseModel):
    """Response model for a recommendation."""

    id: str
    suggestion_set_id: str | None = None
    lens_id: str | None = None
    paper_id: str
    rank: int | None = None
    score: float = Field(..., description="Recommendation score (0-100)")
    in_library: bool = Field(
        False,
        description="Paper is already a saved Library paper (collection lenses surface these so they can be added to the linked collection).",
    )
    score_breakdown: dict | None = None
    user_action: str | None = None
    action_at: str | None = None
    source_type: str | None = None
    source_api: str | None = None
    source_key: str | None = None
    branch_id: str | None = None
    branch_label: str | None = None
    branch_mode: str | None = None
    created_at: str
    is_new: bool = False
    paper: PaperResponse | None = None


class RecommendationExplainResponse(BaseModel):
    """Detailed explanation of a recommendation's score.

    ``breakdown`` is an opaque ``Dict[str, Any]`` rather than a typed
    Pydantic envelope. Different ranker versions emit different signal
    taxonomies — the legacy 10-signal layout
    (``source_relevance``, ``topic_score``, ``text_similarity``, …,
    ``usefulness_boost``), the v2 retrieval-channel layout
    (``lexical``, ``vector``), and various raw-diagnostic fields — and a
    hardcoded Pydantic model silently dropped every v2 key via
    ``extra='ignore'``, rendering 48 / 201 rows as all-nulls on the live
    DB. The route returns the stored breakdown exactly as scoring wrote
    it, annotated with descriptions for known signal names; the frontend
    iterates keys generically and renders the ones it recognises (see
    ``components/shared/PaperCard.tsx::SIGNAL_META``).
    """

    id: str
    title: str
    score: float
    source_type: str | None = None
    source_key: str | None = None
    breakdown: dict[str, Any] | None = None
    explanation: str | None = None


class DiscoveryWeights(BaseModel):
    """Weights for scoring signals (should sum to ~1.0)."""

    source_relevance: float = Field(0.15, ge=0.0, le=1.0)
    topic_score: float = Field(0.20, ge=0.0, le=1.0)
    text_similarity: float = Field(0.20, ge=0.0, le=1.0)
    author_affinity: float = Field(0.15, ge=0.0, le=1.0)
    journal_affinity: float = Field(0.05, ge=0.0, le=1.0)
    recency_boost: float = Field(0.10, ge=0.0, le=1.0)
    citation_quality: float = Field(0.05, ge=0.0, le=1.0)
    feedback_adj: float = Field(0.10, ge=0.0, le=1.0)
    preference_affinity: float = Field(0.10, ge=0.0, le=1.0)
    usefulness_boost: float = Field(0.06, ge=0.0, le=1.0)


class DiscoveryStrategies(BaseModel):
    """Toggle switches for each retrieval strategy."""

    related_works: bool = True
    topic_search: bool = True
    followed_authors: bool = True
    coauthor_network: bool = True
    citation_chain: bool = True
    semantic_scholar: bool = True
    branch_explorer: bool = True
    taste_topics: bool = True
    taste_authors: bool = True
    taste_venues: bool = True
    recent_wins: bool = True


class DiscoveryLimits(BaseModel):
    """Numeric limits for the discovery engine."""

    max_results: int = Field(50, ge=10, le=200)
    min_score: float = Field(0.0, ge=0.0, le=100.0)
    max_candidates_per_strategy: int = Field(20, ge=5, le=50)
    recency_window_years: int = Field(10, ge=1, le=20)
    feedback_decay_days_full: int = Field(90, ge=1)
    feedback_decay_days_half: int = Field(180, ge=1)


class DiscoverySchedule(BaseModel):
    """Schedule settings for automatic recommendation refresh.

    Auto-refresh is opt-in: the periodic job registers only when
    ``refresh_enabled`` is true AND ``refresh_interval_hours`` > 0. The
    Discovery page toggle drives ``refresh_enabled``; the interval lives in
    Settings.
    """

    refresh_enabled: bool = False
    refresh_interval_hours: int = Field(6, ge=0, le=168)
    graph_maintenance_interval_hours: int = Field(24, ge=0, le=168)


class DiscoveryCache(BaseModel):
    """Cache settings for similarity searches."""

    similarity_ttl_hours: int = Field(24, ge=1, le=168)


class DiscoverySourcePolicy(BaseModel):
    """One external source toggle + weight."""

    enabled: bool = True
    weight: float = Field(1.0, ge=0.0, le=2.5)


class DiscoverySources(BaseModel):
    """Source control plane for external discovery retrieval."""

    openalex: DiscoverySourcePolicy = Field(default_factory=DiscoverySourcePolicy)
    semantic_scholar: DiscoverySourcePolicy = Field(default_factory=lambda: DiscoverySourcePolicy(enabled=True, weight=0.95))
    crossref: DiscoverySourcePolicy = Field(default_factory=lambda: DiscoverySourcePolicy(enabled=True, weight=0.72))
    arxiv: DiscoverySourcePolicy = Field(default_factory=lambda: DiscoverySourcePolicy(enabled=True, weight=0.66))
    biorxiv: DiscoverySourcePolicy = Field(default_factory=lambda: DiscoverySourcePolicy(enabled=True, weight=0.62))


class DiscoveryBranchSettings(BaseModel):
    """Global branch-behavior defaults used by branch-aware retrieval."""

    temperature: float = Field(0.28, ge=0.0, le=1.0)
    max_clusters: int = Field(6, ge=2, le=12)
    max_active_for_retrieval: int = Field(4, ge=1, le=12)
    query_core_variants: int = Field(2, ge=1, le=4)
    query_explore_variants: int = Field(2, ge=1, le=4)


class DiscoveryMonitorDefaults(BaseModel):
    """Default retrieval behavior for Feed monitors."""

    author_per_refresh: int = Field(20, ge=1, le=100)
    search_limit: int = Field(15, ge=1, le=50)
    search_temperature: float = Field(0.22, ge=0.0, le=1.0)
    recency_years: int = Field(2, ge=0, le=10)
    include_preprints: bool = True
    semantic_scholar_bulk: bool = True


class DiscoverySettingsResponse(BaseModel):
    """Full discovery settings response."""

    weights: DiscoveryWeights = Field(default_factory=DiscoveryWeights)
    strategies: DiscoveryStrategies = Field(default_factory=DiscoveryStrategies)
    limits: DiscoveryLimits = Field(default_factory=DiscoveryLimits)
    schedule: DiscoverySchedule = Field(default_factory=DiscoverySchedule)
    cache: DiscoveryCache = Field(default_factory=DiscoveryCache)
    sources: DiscoverySources = Field(default_factory=DiscoverySources)
    branches: DiscoveryBranchSettings = Field(default_factory=DiscoveryBranchSettings)
    monitor_defaults: DiscoveryMonitorDefaults = Field(default_factory=DiscoveryMonitorDefaults)
    embedding_model: str = S2_SPECTER2_MODEL
    recommendation_mode: str = "balanced"


class DiscoverySettingsUpdate(BaseModel):
    """Partial update for discovery settings."""

    weights: DiscoveryWeights | None = None
    strategies: DiscoveryStrategies | None = None
    limits: DiscoveryLimits | None = None
    schedule: DiscoverySchedule | None = None
    cache: DiscoveryCache | None = None
    sources: DiscoverySources | None = None
    branches: DiscoveryBranchSettings | None = None
    monitor_defaults: DiscoveryMonitorDefaults | None = None
    embedding_model: str | None = None
    recommendation_mode: str | None = None


class SimilarityRequest(BaseModel):
    """Request to discover papers similar to given papers."""

    paper_ids: list[str] = Field(..., description="Paper UUIDs to use as seeds")
    limit: int = Field(20, ge=1, le=100)
    force: bool = Field(False, description="Bypass cache")


class SimilarityResultItem(BaseModel):
    """A single result from a similarity search."""

    paper_id: str | None = None
    title: str = ""
    authors: str | None = None
    url: str | None = None
    doi: str | None = None
    score: float = 0.0
    score_breakdown: dict | None = None
    year: int | None = None
    # T2 response: which retrieval lane surfaced this candidate
    # (openalex_related / citation_chain / semantic_scholar_recommend
    # / dense_fallback). Defaults to empty so legacy cached rows that
    # pre-date the contract still deserialize.
    source_type: str = ""
    # Free-form per-lane correlation key (DOI, query, paperId, etc.).
    # Used for React list keys when `paper_id` is unavailable.
    source_key: str = ""


class SimilarityChannelStat(BaseModel):
    """Per-channel retrieval stat for discover-similar responses."""

    name: str
    fetched: int = 0
    skipped_as_existing: int = 0
    error: str | None = None


class SimilarityResponse(BaseModel):
    """Response from discover-similar endpoint."""

    results: list[SimilarityResultItem]
    cached: bool = False
    cache_key: str | None = None
    seed_count: int = 0
    channels: list[SimilarityChannelStat] = Field(default_factory=list)
    dense_fallback_used: bool = False


# ============================================================================
# Library Models
# ============================================================================

class CollectionCreate(BaseModel):
    """Request model for creating a collection."""

    name: str
    description: str | None = None
    color: str = "#3B82F6"


class CollectionResponse(BaseModel):
    """Response model for a collection."""

    id: str
    name: str
    description: str | None = None
    color: str
    created_at: str
    item_count: int = 0
    last_added_at: str | None = None
    avg_citations: float | None = None
    avg_rating: float | None = None
    activity_status: str | None = None


class CollectionAddPaperRequest(BaseModel):
    """Request to add a paper to a collection."""

    paper_id: str


class TagCreate(BaseModel):
    """Request model for creating a tag."""

    name: str
    color: str = "#6B7280"


class TagResponse(BaseModel):
    """Response model for a tag."""

    id: str
    name: str
    color: str


class PaperTagRequest(BaseModel):
    """Request to tag a paper."""

    paper_id: str
    tag_id: str


class FollowAuthorRequest(BaseModel):
    """Request model for following an author.

    ``author_id`` accepts any author reference the canonical resolver
    understands (local ``authors.id`` or an OpenAlex author id — the row is
    created when missing). ``name`` is the display name to stamp on a row
    created this way; without it the row would be named after the raw id
    until the historical backfill hydrates it.
    """

    author_id: str
    notify_new_papers: bool = True
    name: str | None = None


class FollowedAuthorResponse(BaseModel):
    """Response model for a followed author."""

    author_id: str
    followed_at: str
    notify_new_papers: bool
    is_owner: bool = False
    name: str | None = None


# ============================================================================
# Alerts Models (digest-based)
# ============================================================================

class AlertRuleCreate(BaseModel):
    """Request model for creating an alert rule."""

    name: str
    rule_type: str = Field(
        ...,
        description=(
            "Rule type: author, collection, keyword, topic, similarity, "
            "discovery_lens, feed_monitor, branch, library_workflow"
        ),
    )
    rule_config: dict
    channels: list[str]
    enabled: bool = True


class AlertRuleResponse(BaseModel):
    """Response model for an alert rule."""

    id: str
    name: str
    rule_type: str
    rule_config: dict
    channels: list[str]
    enabled: bool
    created_at: str


class AlertCreate(BaseModel):
    """Request model for creating an alert (digest)."""

    name: str
    channels: list[str]
    schedule: str = "manual"
    schedule_config: dict | None = None
    format: str = "grouped"
    enabled: bool = True
    rule_ids: list[str] | None = None


class AlertUpdate(BaseModel):
    """Request model for updating an alert."""

    name: str | None = None
    channels: list[str] | None = None
    schedule: str | None = None
    schedule_config: dict | None = None
    format: str | None = None
    enabled: bool | None = None


class AlertResponse(BaseModel):
    """Response model for an alert (digest)."""

    id: str
    name: str
    channels: list[str]
    schedule: str
    schedule_config: dict | None = None
    format: str
    enabled: bool
    created_at: str
    last_evaluated_at: str | None = None
    # Worst status among the most recent evaluation's history rows
    # (failed > skipped > sent > empty), so a failure is never hidden.
    last_outcome: str | None = None
    # Next slot the hourly sweep can fire this digest; None for manual or
    # disabled digests.
    next_due_at: str | None = None
    rules: list[AlertRuleResponse] | None = None


class AlertRuleAssignment(BaseModel):
    """Request model for assigning rules to an alert."""

    rule_ids: list[str]


class AlertHistoryResponse(BaseModel):
    """Response model for an alert history entry."""

    id: str
    rule_id: str | None = None
    alert_id: str | None = None
    channel: str
    paper_id: str | None = None
    publications: list[str] | None = None
    publication_count: int = 0
    sent_at: str
    status: str
    message_preview: str | None = None
    error_message: str | None = None


class AlertEvaluationResult(BaseModel):
    """Response model for alert evaluation."""

    alert_id: str
    alert_name: str
    digest_id: str | None = None
    digest_name: str | None = None
    matched_rules: int | None = None
    papers_found: int
    papers_new: int
    papers_sent: int
    papers_failed: int | None = None
    channels: list[str]
    channel_results: dict | None = None
    trigger_source: str | None = None
    dry_run: bool
    papers: list[dict] | None = None


class AlertAutomationTemplateRule(BaseModel):
    """One rule payload proposed for an automation template."""

    name: str
    rule_type: str
    rule_config: dict
    channels: list[str] = Field(default_factory=list)
    enabled: bool = True


class AlertAutomationTemplateDigest(BaseModel):
    """One digest payload proposed for an automation template."""

    name: str
    channels: list[str] = Field(default_factory=list)
    schedule: str = "manual"
    schedule_config: dict | None = None
    format: str = "text"
    enabled: bool = True


class AlertAutomationTemplate(BaseModel):
    """Suggested alert automation derived from current product state."""

    key: str
    category: str
    title: str
    description: str
    rationale: str | None = None
    metrics: dict = Field(default_factory=dict)
    rule: AlertAutomationTemplateRule
    alert: AlertAutomationTemplateDigest


class AlertTemplateApplyResponse(BaseModel):
    """Result of materializing a suggested automation (rule + digest)."""

    template_key: str
    template_title: str
    rule: AlertRuleResponse
    alert: AlertResponse


# ============================================================================
# Job Models
# ============================================================================

class JobCreate(BaseModel):
    """Request model for creating a scheduled job."""

    name: str = Field(..., min_length=1, max_length=100)
    description: str | None = Field(None, max_length=500)
    cron_expression: str
    action: str = Field(..., pattern="^(fetch|notify|fetch_and_notify)$")
    plugin_name: str | None = None
    author_ids: list[str] | None = None
    enabled: bool = True


class JobResponse(BaseModel):
    """Response model for job data."""

    id: int
    name: str
    description: str | None = None
    cron_expression: str
    action: str
    plugin_name: str | None = None
    author_ids: list[str] | None = None
    enabled: bool
    next_run: str | None = None
    last_run: str | None = None
    created_at: str


# ============================================================================
# Plugin Models
# ============================================================================

class PluginInfo(BaseModel):
    """Response model for plugin information."""

    name: str
    display_name: str
    version: str
    description: str
    config_schema: dict
    is_configured: bool
    is_healthy: bool | None = None


class PluginConfigUpdate(BaseModel):
    """Request model for updating plugin configuration."""

    config: dict


class PluginTestResult(BaseModel):
    """Response model for plugin connection test."""

    success: bool
    message: str
    timestamp: str


# ============================================================================
# Statistics Models
# ============================================================================

class StatisticsResponse(BaseModel):
    """Response model for overall statistics."""

    total_authors: int
    total_publications: int
    total_citations: int
    active_jobs: int
    configured_plugins: int


# ============================================================================
# Import Models
# ============================================================================

class ImportResultResponse(BaseModel):
    """Response model for an import operation result."""

    total: int = 0
    imported: int = 0
    staged: int = 0
    skipped: int = 0
    failed: int = 0
    errors: list[str] = Field(default_factory=list)
    items: list[dict] = Field(default_factory=list)


class BibtexTextImportRequest(BaseModel):
    """Request model for importing BibTeX from text."""

    content: str
    collection_name: str | None = None


class ZoteroImportRequest(BaseModel):
    """Request model for importing from Zotero."""

    library_id: str
    api_key: str | None = None
    library_type: str = "user"
    collection_key: str | None = None
    collection_name: str | None = None


class ZoteroCollectionsRequest(BaseModel):
    """Request model for listing Zotero collections."""

    library_id: str
    api_key: str | None = None
    library_type: str = "user"


class ZoteroCollectionResponse(BaseModel):
    """Response model for a Zotero collection."""

    key: str
    name: str
    num_items: int = 0
    parent: str | None = None


# ============================================================================
# System Models
# ============================================================================

class HealthResponse(BaseModel):
    status: str
    version: str
    uptime_seconds: float
    database_ok: bool


class VersionResponse(BaseModel):
    api_version: str
    app_version: str
    python_version: str


class ErrorResponse(BaseModel):
    error: str
    message: str
    detail: dict | None = None


# ============================================================================
# Graph Models
# ============================================================================

class GraphData(BaseModel):
    """Response model for graph visualization data."""

    nodes: list[dict]
    edges: list[dict]
    metadata: dict | None = None
