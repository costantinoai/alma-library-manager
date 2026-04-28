"""Pydantic models for API request/response validation.

v3: UUID-based papers, discovery lenses, feed items, digest alerts.
"""

from pydantic import BaseModel, Field, ConfigDict
from typing import Any, Dict, Optional, List
from datetime import datetime

from alma.discovery.semantic_scholar import S2_SPECTER2_MODEL


# ============================================================================
# Author Models
# ============================================================================

class AuthorCreate(BaseModel):
    """Request model for creating a new author."""

    scholar_id: Optional[str] = Field(None, description="Google Scholar ID")
    openalex_id: Optional[str] = Field(None, description="OpenAlex author ID")
    orcid: Optional[str] = Field(None, description="ORCID")
    name: Optional[str] = Field(None, description="Optional display name fallback")


class AuthorResponse(BaseModel):
    """Response model for author data."""

    id: str
    name: str
    added_at: Optional[str] = None
    publication_count: int = 0
    affiliation: Optional[str] = None
    email_domain: Optional[str] = None
    citedby: Optional[int] = None
    h_index: Optional[int] = None
    interests: Optional[List[str]] = None
    url_picture: Optional[str] = None
    works_count: Optional[int] = None
    last_fetched_at: Optional[str] = None
    orcid: Optional[str] = None
    openalex_id: Optional[str] = None
    scholar_id: Optional[str] = None
    author_type: Optional[str] = None
    id_resolution_status: Optional[str] = None
    id_resolution_reason: Optional[str] = None
    id_resolution_updated_at: Optional[str] = None
    # Phase D hierarchical-resolver fields (2026-04-24): method = which
    # tier fired (`orcid_direct` / `openalex_provided` / …), confidence
    # ∈ [0, 1]. Lets the UI render a "resolved" checkmark without
    # re-evaluating the status enum.
    id_resolution_method: Optional[str] = None
    id_resolution_confidence: Optional[float] = None
    monitor_health: Optional[str] = None
    monitor_health_reason: Optional[str] = None
    monitor_last_checked_at: Optional[str] = None
    monitor_last_success_at: Optional[str] = None
    monitor_last_status: Optional[str] = None
    monitor_last_error: Optional[str] = None
    monitor_last_result: Optional[dict] = None
    monitor_papers_found: Optional[int] = None
    monitor_items_created: Optional[int] = None
    background_corpus_state: Optional[str] = None
    background_corpus_detail: Optional[str] = None
    background_corpus_last_success_at: Optional[str] = None
    background_corpus_age_days: Optional[int] = None
    background_corpus_publications: Optional[int] = None
    background_corpus_coverage_ratio: Optional[float] = None


class RelatedWork(BaseModel):
    """One row in a Prior/Derivative Works panel (T6).

    Distinct from `PaperResponse` — we intentionally surface a
    trimmed-down projection so the paper detail dialog renders
    fast. `paper_id` is the local UUID when the referenced work is
    already in our corpus; null when it only lives in S2. `source`
    tags where the row came from so the UI can show a "from
    Semantic Scholar" hint on network-origin rows.
    """

    paper_id: Optional[str] = None
    title: str
    authors: Optional[str] = None
    year: Optional[int] = None
    doi: Optional[str] = None
    url: Optional[str] = None
    journal: Optional[str] = None
    abstract: Optional[str] = None
    tldr: Optional[str] = None
    cited_by_count: int = 0
    influential_citation_count: int = 0
    openalex_id: Optional[str] = None
    semantic_scholar_id: Optional[str] = None
    status: Optional[str] = None
    rating: Optional[int] = None
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
    works: List[RelatedWork] = Field(default_factory=list)
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
    count: Optional[int] = None
    value: Optional[float] = None
    subject: Optional[str] = None


class AuthorSuggestionResponse(BaseModel):
    """Suggested collaborator or adjacent author to monitor."""

    key: str
    name: str
    suggestion_type: str
    score: float = 0.0
    openalex_id: Optional[str] = None
    existing_author_id: Optional[str] = None
    known_author_type: Optional[str] = None
    shared_paper_count: int = 0
    shared_followed_count: int = 0
    local_paper_count: int = 0
    recent_paper_count: int = 0
    shared_followed_authors: List[str] = Field(default_factory=list)
    shared_topics: List[str] = Field(default_factory=list)
    shared_venues: List[str] = Field(default_factory=list)
    sample_titles: List[str] = Field(default_factory=list)
    # T7: priority-ordered evidence chips built from bucket-specific
    # signals (`shared_paper_count`, `similarity`, etc.). Capped at
    # 4; frontend renders as a row of neutral StatusBadge chips.
    signals: List[AuthorSuggestionSignal] = Field(default_factory=list)
    negative_signal: float = 0.0
    last_removed_at: Optional[str] = None


class AuthorFollowFromPaperRequest(BaseModel):
    """Request model for following one author from a paper card."""

    paper_id: str = Field(..., description="Paper UUID")
    author_name: str = Field(..., min_length=1, description="Author name shown on the paper card")


class AuthorFollowFromPaperResponse(BaseModel):
    """Follow-author result for paper-card actions."""

    author: AuthorResponse
    created: bool = False
    already_followed: bool = False
    matched_via: Optional[str] = None


# ============================================================================
# Paper Models (v3 — replaces Publication models)
# ============================================================================

class PaperResponse(BaseModel):
    """Response model for a paper."""

    id: str = Field(..., description="Paper UUID")
    title: str
    authors: Optional[str] = None
    year: Optional[int] = None
    journal: Optional[str] = None
    abstract: Optional[str] = None
    url: Optional[str] = None
    doi: Optional[str] = None
    publication_date: Optional[str] = None

    # OpenAlex metadata
    openalex_id: Optional[str] = None
    work_type: Optional[str] = None
    language: Optional[str] = None
    is_oa: bool = False
    oa_status: Optional[str] = None
    oa_url: Optional[str] = None
    is_retracted: bool = False
    fwci: Optional[float] = None
    cited_by_count: int = 0
    referenced_works_count: int = 0
    keywords: Optional[List[str]] = None
    # T5: S2 1-2 sentence AI summary. Dense coverage in CS + biomed,
    # sparse elsewhere; `None` means "S2 didn't supply one", distinct
    # from "empty abstract".
    tldr: Optional[str] = None
    # T5: S2's learned "this citation mattered" count. Supplements
    # `cited_by_count` in the `citation_quality` scoring signal.
    influential_citation_count: int = 0

    # Status and library
    status: str = "tracked"
    rating: int = 0
    notes: Optional[str] = None
    added_at: Optional[str] = None
    added_from: Optional[str] = None
    reading_status: Optional[str] = None
    # Row mtime, populated by the `papers.updated_at` trigger default.
    # Surfaced so diagnostic surfaces (Corpus explorer) can show when a row
    # was last touched; without this declaration Pydantic v2's default
    # `extra='ignore'` drops the column silently even when `SELECT p.*`
    # pulls it.
    updated_at: Optional[str] = None

    # Resolution
    openalex_resolution_status: Optional[str] = None
    openalex_resolution_reason: Optional[str] = None

    # Provenance
    source_id: Optional[str] = None

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
    notes: Optional[str] = None
    rating: Optional[int] = Field(None, ge=0, le=5)


class PaperRateRequest(BaseModel):
    """Request model for rating a paper."""

    paper_id: str = Field(..., description="Paper UUID")
    rating: int = Field(..., ge=0, le=5)


class PaperCreateRequest(BaseModel):
    """Request model for manually adding a paper."""

    title: str = Field(..., min_length=1)
    authors: Optional[str] = None
    year: Optional[int] = None
    journal: Optional[str] = None
    abstract: Optional[str] = None
    url: Optional[str] = None
    doi: Optional[str] = None
    status: str = Field("library", description="Initial status: tracked | library")
    added_from: str = Field("manual", description="Source: manual | import | feed | discovery")


# Legacy compatibility aliases
PublicationResponse = PaperResponse


class PublicationSendItem(BaseModel):
    """Item to send via a plugin from a preview."""

    paper_id: Optional[str] = Field(None, description="Paper UUID (preferred)")
    # Legacy fields for backward compatibility
    author_id: Optional[str] = None
    title: str = ""
    authors: str = ""
    year: Optional[int] = None
    abstract: Optional[str] = None
    url: Optional[str] = None
    citations: Optional[int] = 0
    journal: Optional[str] = None


class SendPublicationsRequest(BaseModel):
    """Request to send a previewed list of publications via a plugin."""

    plugin_name: Optional[str] = None
    target: Optional[str] = None
    items: List[PublicationSendItem]


class SavePublicationsRequest(BaseModel):
    """Request to save previewed publications to the database."""

    items: List[PublicationSendItem]


# ============================================================================
# Feed Models (v3 — new)
# ============================================================================

class FeedItemResponse(BaseModel):
    """Response model for a feed item."""

    id: str
    paper_id: str
    author_id: str
    author_name: Optional[str] = None
    matched_author_ids: List[str] = Field(default_factory=list)
    matched_authors: List[str] = Field(default_factory=list)
    matched_monitors: List[dict] = Field(default_factory=list)
    monitor_id: Optional[str] = None
    monitor_type: Optional[str] = None
    monitor_label: Optional[str] = None
    fetched_at: str
    status: str = "new"
    is_new: bool = False
    signal_value: int = 0
    score_breakdown: Optional[dict] = None
    paper: Optional[PaperResponse] = None


class FeedMonitorCreateRequest(BaseModel):
    """Request model for creating a non-author feed monitor."""

    monitor_type: str = Field(..., description="query (keyword monitor) | topic | venue | preprint | branch")
    query: str = Field(..., min_length=1, description="Search string or boolean keyword expression used by the monitor")
    label: Optional[str] = Field(default=None, description="Optional display label")
    config: Optional[dict] = None


class FeedMonitorUpdateRequest(BaseModel):
    """Request model for updating a feed monitor."""

    query: Optional[str] = Field(default=None, min_length=1, description="Updated search string or boolean keyword expression used by the monitor")
    label: Optional[str] = Field(default=None, description="Optional display label")
    enabled: Optional[bool] = Field(default=None, description="Enable or disable this monitor without deleting it")
    config: Optional[dict] = None


class FeedMonitorResponse(BaseModel):
    """Unified feed monitor response model."""

    id: str
    monitor_type: str
    monitor_key: str
    label: str
    enabled: bool = True
    author_id: Optional[str] = None
    author_name: Optional[str] = None
    openalex_id: Optional[str] = None
    scholar_id: Optional[str] = None
    orcid: Optional[str] = None
    config: Optional[dict] = None
    created_at: Optional[str] = None
    updated_at: Optional[str] = None
    last_checked_at: Optional[str] = None
    last_success_at: Optional[str] = None
    last_status: Optional[str] = None
    last_error: Optional[str] = None
    last_result: Optional[dict] = None
    health: str = "ready"
    health_reason: Optional[str] = None


# ============================================================================
# Discovery Models (v3 — lens-based)
# ============================================================================

class LensCreate(BaseModel):
    """Request model for creating a discovery lens."""

    name: str = Field(..., min_length=1)
    context_type: str = Field(..., description="library_global | collection | topic_keyword | tag")
    context_config: Optional[dict] = None
    weights: Optional[dict] = None


class LensUpdate(BaseModel):
    """Request model for updating a discovery lens."""

    name: Optional[str] = None
    context_config: Optional[dict] = None
    weights: Optional[dict] = None
    branch_controls: Optional[dict] = None
    is_active: Optional[bool] = None


class LensResponse(BaseModel):
    """Response model for a discovery lens."""

    id: str
    name: str
    context_type: str
    context_config: Optional[dict] = None
    weights: Optional[dict] = None
    created_at: str
    last_refreshed_at: Optional[str] = None
    is_active: bool = True
    signal_count: int = 0
    recommendation_count: int = 0
    last_suggestion_set_id: Optional[str] = None
    last_ranker_version: Optional[str] = None
    last_retrieval_summary: Optional[dict] = None
    branch_controls: Optional[dict] = None


class BranchSeedSample(BaseModel):
    """Small paper summary for branch visualization previews."""

    paper_id: Optional[str] = None
    title: str
    year: Optional[int] = None
    rating: int = 0


class BranchPreviewItem(BaseModel):
    """One branch node in the lens branch explorer."""

    id: str
    label: str
    seed_count: int
    branch_score: float = 0.0
    core_topics: List[str] = Field(default_factory=list)
    explore_topics: List[str] = Field(default_factory=list)
    direction_hint: Optional[str] = None
    sample_papers: List[BranchSeedSample] = Field(default_factory=list)
    control_state: Optional[str] = None
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
    auto_weight_reason: Optional[str] = None


class LensBranchPreviewResponse(BaseModel):
    """Tree-like preview of branch structure for a lens."""

    lens_id: str
    lens_name: Optional[str] = None
    context_type: str
    seed_count: int = 0
    temperature: float = 0.0
    generated_at: str
    branches: List[BranchPreviewItem] = Field(default_factory=list)


class SuggestionSetResponse(BaseModel):
    """Response model for a suggestion set."""

    id: str
    lens_id: str
    context_type: str
    trigger_source: str
    retrieval_summary: Optional[dict] = None
    ranker_version: Optional[str] = None
    created_at: str


class RecommendationResponse(BaseModel):
    """Response model for a recommendation."""

    id: str
    suggestion_set_id: Optional[str] = None
    lens_id: Optional[str] = None
    paper_id: str
    rank: Optional[int] = None
    score: float = Field(..., description="Recommendation score (0-100)")
    score_breakdown: Optional[dict] = None
    user_action: Optional[str] = None
    action_at: Optional[str] = None
    source_type: Optional[str] = None
    source_api: Optional[str] = None
    source_key: Optional[str] = None
    branch_id: Optional[str] = None
    branch_label: Optional[str] = None
    branch_mode: Optional[str] = None
    created_at: str
    paper: Optional[PaperResponse] = None


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
    source_type: Optional[str] = None
    source_key: Optional[str] = None
    breakdown: Optional[Dict[str, Any]] = None
    explanation: Optional[str] = None


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
    max_candidates_per_strategy: int = Field(20, ge=5, le=50)
    recency_window_years: int = Field(10, ge=1, le=20)
    feedback_decay_days_full: int = Field(90, ge=1)
    feedback_decay_days_half: int = Field(180, ge=1)


class DiscoverySchedule(BaseModel):
    """Schedule settings for automatic recommendation refresh."""

    refresh_interval_hours: int = Field(0, ge=0, le=168)
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

    weights: Optional[DiscoveryWeights] = None
    strategies: Optional[DiscoveryStrategies] = None
    limits: Optional[DiscoveryLimits] = None
    schedule: Optional[DiscoverySchedule] = None
    cache: Optional[DiscoveryCache] = None
    sources: Optional[DiscoverySources] = None
    branches: Optional[DiscoveryBranchSettings] = None
    monitor_defaults: Optional[DiscoveryMonitorDefaults] = None
    embedding_model: Optional[str] = None
    recommendation_mode: Optional[str] = None


class SimilarityRequest(BaseModel):
    """Request to discover papers similar to given papers."""

    paper_ids: List[str] = Field(..., description="Paper UUIDs to use as seeds")
    limit: int = Field(20, ge=1, le=100)
    force: bool = Field(False, description="Bypass cache")


class SimilarityResultItem(BaseModel):
    """A single result from a similarity search."""

    paper_id: Optional[str] = None
    title: str = ""
    authors: Optional[str] = None
    url: Optional[str] = None
    doi: Optional[str] = None
    score: float = 0.0
    score_breakdown: Optional[dict] = None
    year: Optional[int] = None
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
    error: Optional[str] = None


class SimilarityResponse(BaseModel):
    """Response from discover-similar endpoint."""

    results: List[SimilarityResultItem]
    cached: bool = False
    cache_key: Optional[str] = None
    seed_count: int = 0
    channels: List[SimilarityChannelStat] = Field(default_factory=list)
    dense_fallback_used: bool = False


# ============================================================================
# Library Models
# ============================================================================

class CollectionCreate(BaseModel):
    """Request model for creating a collection."""

    name: str
    description: Optional[str] = None
    color: str = "#3B82F6"


class CollectionResponse(BaseModel):
    """Response model for a collection."""

    id: str
    name: str
    description: Optional[str] = None
    color: str
    created_at: str
    item_count: int = 0
    last_added_at: Optional[str] = None
    avg_citations: Optional[float] = None
    avg_rating: Optional[float] = None
    activity_status: Optional[str] = None


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
    """Request model for following an author."""

    author_id: str
    notify_new_papers: bool = True


class FollowedAuthorResponse(BaseModel):
    """Response model for a followed author."""

    author_id: str
    followed_at: str
    notify_new_papers: bool
    name: Optional[str] = None


# ============================================================================
# Alerts Models (digest-based)
# ============================================================================

class AlertRuleCreate(BaseModel):
    """Request model for creating an alert rule."""

    name: str
    rule_type: str = Field(..., description="Rule type: author, keyword, topic, similarity, discovery_lens")
    rule_config: dict
    channels: List[str]
    enabled: bool = True


class AlertRuleResponse(BaseModel):
    """Response model for an alert rule."""

    id: str
    name: str
    rule_type: str
    rule_config: dict
    channels: List[str]
    enabled: bool
    created_at: str


class AlertCreate(BaseModel):
    """Request model for creating an alert (digest)."""

    name: str
    channels: List[str]
    schedule: str = "manual"
    schedule_config: Optional[dict] = None
    format: str = "grouped"
    enabled: bool = True
    rule_ids: Optional[List[str]] = None


class AlertUpdate(BaseModel):
    """Request model for updating an alert."""

    name: Optional[str] = None
    channels: Optional[List[str]] = None
    schedule: Optional[str] = None
    schedule_config: Optional[dict] = None
    format: Optional[str] = None
    enabled: Optional[bool] = None


class AlertResponse(BaseModel):
    """Response model for an alert (digest)."""

    id: str
    name: str
    channels: List[str]
    schedule: str
    schedule_config: Optional[dict] = None
    format: str
    enabled: bool
    created_at: str
    last_evaluated_at: Optional[str] = None
    rules: Optional[List[AlertRuleResponse]] = None


class AlertRuleAssignment(BaseModel):
    """Request model for assigning rules to an alert."""

    rule_ids: List[str]


class AlertHistoryResponse(BaseModel):
    """Response model for an alert history entry."""

    id: str
    rule_id: Optional[str] = None
    alert_id: Optional[str] = None
    channel: str
    paper_id: Optional[str] = None
    publications: Optional[List[str]] = None
    publication_count: int = 0
    sent_at: str
    status: str
    message_preview: Optional[str] = None
    error_message: Optional[str] = None


class AlertEvaluationResult(BaseModel):
    """Response model for alert evaluation."""

    alert_id: str
    alert_name: str
    digest_id: Optional[str] = None
    digest_name: Optional[str] = None
    matched_rules: Optional[int] = None
    papers_found: int
    papers_new: int
    papers_sent: int
    papers_failed: Optional[int] = None
    channels: List[str]
    channel_results: Optional[dict] = None
    trigger_source: Optional[str] = None
    dry_run: bool
    papers: Optional[List[dict]] = None


class AlertAutomationTemplateRule(BaseModel):
    """One rule payload proposed for an automation template."""

    name: str
    rule_type: str
    rule_config: dict
    channels: List[str] = Field(default_factory=list)
    enabled: bool = True


class AlertAutomationTemplateDigest(BaseModel):
    """One digest payload proposed for an automation template."""

    name: str
    channels: List[str] = Field(default_factory=list)
    schedule: str = "manual"
    schedule_config: Optional[dict] = None
    format: str = "text"
    enabled: bool = True


class AlertAutomationTemplate(BaseModel):
    """Suggested alert automation derived from current product state."""

    key: str
    category: str
    title: str
    description: str
    rationale: Optional[str] = None
    metrics: dict = Field(default_factory=dict)
    rule: AlertAutomationTemplateRule
    alert: AlertAutomationTemplateDigest


# ============================================================================
# Job Models
# ============================================================================

class JobCreate(BaseModel):
    """Request model for creating a scheduled job."""

    name: str = Field(..., min_length=1, max_length=100)
    description: Optional[str] = Field(None, max_length=500)
    cron_expression: str
    action: str = Field(..., pattern="^(fetch|notify|fetch_and_notify)$")
    plugin_name: Optional[str] = None
    author_ids: Optional[List[str]] = None
    enabled: bool = True


class JobResponse(BaseModel):
    """Response model for job data."""

    id: int
    name: str
    description: Optional[str] = None
    cron_expression: str
    action: str
    plugin_name: Optional[str] = None
    author_ids: Optional[List[str]] = None
    enabled: bool
    next_run: Optional[str] = None
    last_run: Optional[str] = None
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
    is_healthy: Optional[bool] = None


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
    skipped: int = 0
    failed: int = 0
    errors: List[str] = Field(default_factory=list)
    items: List[dict] = Field(default_factory=list)


class BibtexTextImportRequest(BaseModel):
    """Request model for importing BibTeX from text."""

    content: str
    collection_name: Optional[str] = None


class ZoteroImportRequest(BaseModel):
    """Request model for importing from Zotero."""

    library_id: str
    api_key: Optional[str] = None
    library_type: str = "user"
    collection_key: Optional[str] = None
    collection_name: Optional[str] = None


class ZoteroCollectionsRequest(BaseModel):
    """Request model for listing Zotero collections."""

    library_id: str
    api_key: Optional[str] = None
    library_type: str = "user"


class ZoteroCollectionResponse(BaseModel):
    """Response model for a Zotero collection."""

    key: str
    name: str
    num_items: int = 0
    parent: Optional[str] = None


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
    detail: Optional[dict] = None


# ============================================================================
# Graph Models
# ============================================================================

class GraphData(BaseModel):
    """Response model for graph visualization data."""

    nodes: List[dict]
    edges: List[dict]
    metadata: Optional[dict] = None


