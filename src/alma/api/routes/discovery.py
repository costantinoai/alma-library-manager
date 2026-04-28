"""Discovery API endpoints: recommendations, manual search, and settings."""

import hashlib
import json
import logging
import sqlite3
import uuid
from datetime import datetime
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, Depends, HTTPException, Query, status
from pydantic import BaseModel, Field

from alma.api.deps import get_current_user, get_db, open_db_connection
from alma.api.helpers import raise_internal
from alma.api.models import (
    DiscoveryBranchSettings,
    DiscoveryCache,
    DiscoveryLimits,
    DiscoveryMonitorDefaults,
    DiscoverySchedule,
    DiscoverySettingsResponse,
    DiscoverySettingsUpdate,
    DiscoverySourcePolicy,
    DiscoverySources,
    DiscoveryStrategies,
    DiscoveryWeights,
    PaperResponse,
    RecommendationExplainResponse,
    RecommendationResponse,
    SimilarityRequest,
    SimilarityResponse,
    SimilarityResultItem,
)
from alma.application import discovery as discovery_app
from alma.config import get_db_path
from alma.core.http_sources import (
    openalex_usage_delta,
    openalex_usage_snapshot,
    source_diagnostics_scope,
)
from alma.core.operations import OperationOutcome, OperationRunner
from alma.core.redaction import redact_sensitive_text
from alma.discovery.defaults import DISCOVERY_SETTINGS_DEFAULTS

logger = logging.getLogger(__name__)

# Backwards-compatible helper name still referenced by tests and older docs.
_openalex_usage_delta = openalex_usage_delta

router = APIRouter(
    dependencies=[Depends(get_current_user)],
    responses={401: {"description": "Unauthorized"}},
)


def _read_settings(db: sqlite3.Connection) -> DiscoverySettingsResponse:
    """Read all discovery_settings rows and return a structured response."""
    kv: Dict[str, str] = discovery_app.read_settings(db)

    return DiscoverySettingsResponse(
        weights=DiscoveryWeights(
            source_relevance=float(kv.get("weights.source_relevance", "0.15")),
            topic_score=float(kv.get("weights.topic_score", "0.20")),
            text_similarity=float(kv.get("weights.text_similarity", "0.20")),
            author_affinity=float(kv.get("weights.author_affinity", "0.15")),
            journal_affinity=float(kv.get("weights.journal_affinity", "0.05")),
            recency_boost=float(kv.get("weights.recency_boost", "0.10")),
            citation_quality=float(kv.get("weights.citation_quality", "0.05")),
            feedback_adj=float(kv.get("weights.feedback_adj", "0.10")),
            preference_affinity=float(kv.get("weights.preference_affinity", "0.10")),
            usefulness_boost=float(kv.get("weights.usefulness_boost", "0.06")),
        ),
        strategies=DiscoveryStrategies(
            related_works=kv.get("strategies.related_works", "true").lower() == "true",
            topic_search=kv.get("strategies.topic_search", "true").lower() == "true",
            followed_authors=kv.get("strategies.followed_authors", "true").lower() == "true",
            coauthor_network=kv.get("strategies.coauthor_network", "true").lower() == "true",
            citation_chain=kv.get("strategies.citation_chain", "true").lower() == "true",
            semantic_scholar=kv.get("strategies.semantic_scholar", "true").lower() == "true",
            branch_explorer=kv.get("strategies.branch_explorer", "true").lower() == "true",
            taste_topics=kv.get("strategies.taste_topics", "true").lower() == "true",
            taste_authors=kv.get("strategies.taste_authors", "true").lower() == "true",
            taste_venues=kv.get("strategies.taste_venues", "true").lower() == "true",
            recent_wins=kv.get("strategies.recent_wins", "true").lower() == "true",
        ),
        limits=DiscoveryLimits(
            max_results=int(kv.get("limits.max_results", "50")),
            max_candidates_per_strategy=int(kv.get("limits.max_candidates_per_strategy", "20")),
            recency_window_years=int(kv.get("limits.recency_window_years", "10")),
            feedback_decay_days_full=int(kv.get("limits.feedback_decay_days_full", "90")),
            feedback_decay_days_half=int(kv.get("limits.feedback_decay_days_half", "180")),
        ),
        schedule=DiscoverySchedule(
            refresh_interval_hours=int(kv.get("schedule.refresh_interval_hours", "0")),
            graph_maintenance_interval_hours=int(kv.get("schedule.graph_maintenance_interval_hours", "24")),
        ),
        cache=DiscoveryCache(
            similarity_ttl_hours=int(kv.get("cache.similarity_ttl_hours", "24")),
        ),
        sources=DiscoverySources(
            openalex=DiscoverySourcePolicy(
                enabled=kv.get("sources.openalex.enabled", "true").lower() == "true",
                weight=float(kv.get("sources.openalex.weight", "1.0")),
            ),
            semantic_scholar=DiscoverySourcePolicy(
                enabled=kv.get("sources.semantic_scholar.enabled", "true").lower() == "true",
                weight=float(kv.get("sources.semantic_scholar.weight", "0.95")),
            ),
            crossref=DiscoverySourcePolicy(
                enabled=kv.get("sources.crossref.enabled", "true").lower() == "true",
                weight=float(kv.get("sources.crossref.weight", "0.72")),
            ),
            arxiv=DiscoverySourcePolicy(
                enabled=kv.get("sources.arxiv.enabled", "true").lower() == "true",
                weight=float(kv.get("sources.arxiv.weight", "0.66")),
            ),
            biorxiv=DiscoverySourcePolicy(
                enabled=kv.get("sources.biorxiv.enabled", "true").lower() == "true",
                weight=float(kv.get("sources.biorxiv.weight", "0.62")),
            ),
        ),
        branches=DiscoveryBranchSettings(
            temperature=float(kv.get("branches.temperature", "0.28")),
            max_clusters=int(kv.get("branches.max_clusters", "6")),
            max_active_for_retrieval=int(kv.get("branches.max_active_for_retrieval", "4")),
            query_core_variants=int(kv.get("branches.query_core_variants", "2")),
            query_explore_variants=int(kv.get("branches.query_explore_variants", "2")),
        ),
        monitor_defaults=DiscoveryMonitorDefaults(
            author_per_refresh=int(kv.get("monitor_defaults.author_per_refresh", "20")),
            search_limit=int(kv.get("monitor_defaults.search_limit", "15")),
            search_temperature=float(kv.get("monitor_defaults.search_temperature", "0.22")),
            recency_years=int(kv.get("monitor_defaults.recency_years", "2")),
            include_preprints=kv.get("monitor_defaults.include_preprints", "true").lower() == "true",
            semantic_scholar_bulk=kv.get("monitor_defaults.semantic_scholar_bulk", "true").lower() == "true",
        ),
        embedding_model=kv.get("embedding_model", DISCOVERY_SETTINGS_DEFAULTS["embedding_model"]),
        recommendation_mode=(
            kv.get("recommendation_mode", "balanced").strip().lower()
            if kv.get("recommendation_mode", "balanced").strip().lower() in {"explore", "balanced", "exploit"}
            else "balanced"
        ),
    )


def _upsert_setting(db: sqlite3.Connection, key: str, value: str) -> None:
    """Insert or update a single discovery_settings row."""
    discovery_app.upsert_setting(db, key, value)


# Human-readable descriptions for each scoring signal
_SIGNAL_DESCRIPTIONS: Dict[str, str] = {
    "source_relevance": "Position in retrieval results (1st = highest)",
    "topic_score": "Topic overlap with your rated papers",
    "text_similarity": "Semantic similarity to your top-rated papers",
    "author_affinity": "Author overlap with papers you follow",
    "journal_affinity": "Published in a journal you read",
    "recency_boost": "Publication recency (newer = higher)",
    "citation_quality": "Citation count quality indicator",
    "feedback_adj": "Adjusted based on your past feedback",
    "preference_affinity": "Affinity learned from your accumulated feedback profile",
    "usefulness_boost": "Rewards timely, credible, and less redundant papers",
}


class ManualSearchRequest(BaseModel):
    query: str = Field(
        ...,
        min_length=1,
        description="Search string, title, author:..., DOI, OpenAlex URL/ID, or paper link",
    )
    limit: int = Field(default=20, ge=1, le=100)


class ManualAddRequest(BaseModel):
    openalex_id: Optional[str] = None
    doi: Optional[str] = None
    link: Optional[str] = None
    title: Optional[str] = None
    query: Optional[str] = None


class RecommendationLibraryActionRequest(BaseModel):
    rating: Optional[int] = Field(default=None, ge=0, le=5)


def _parse_breakdown(raw: Optional[str]) -> Optional[dict]:
    """Parse a JSON score_breakdown string into a dict, returning None on failure."""
    if not raw:
        return None
    try:
        return json.loads(raw)
    except (json.JSONDecodeError, TypeError):
        return None


def _build_explain_breakdown(raw: Optional[str]) -> Optional[Dict[str, Any]]:
    """Parse the stored JSON score breakdown and return it as-is.

    Different ranker versions emit different signal taxonomies — v1
    emits the 10-signal layout, v2 emits ``{lexical, vector}`` — and
    various raw-diagnostic fields (``final_score``, ``text_similarity_mode``,
    ``semantic_similarity_raw``, …) can appear alongside the signals. The
    previous typed-model filter silently dropped every v2 key, so this
    helper now passes through the stored shape verbatim and only adds a
    ``description`` onto ``{value, weight, weighted}``-shaped signal
    entries whose name we recognise.

    Non-signal fields (scalars like ``final_score`` and mode-selection
    strings) are returned untouched so a generic UI can still display
    them if desired.
    """
    data = _parse_breakdown(raw)
    if not data:
        return None

    out: Dict[str, Any] = {}
    for name, value in data.items():
        if (
            isinstance(value, dict)
            and "value" in value
            and "weight" in value
            and "weighted" in value
        ):
            enriched = dict(value)
            if name in _SIGNAL_DESCRIPTIONS and "description" not in enriched:
                enriched["description"] = _SIGNAL_DESCRIPTIONS[name]
            out[name] = enriched
        else:
            out[name] = value
    return out


# ===================================================================
# Discovery Settings
# ===================================================================

@router.get(
    "/status",
    summary="Discovery refresh status",
)
def get_discovery_status(
    db: sqlite3.Connection = Depends(get_db),
):
    """Return lightweight discovery status, including the last successful refresh.

    ``last_refresh_at`` is the latest ``finished_at`` across the two operation
    keys the system actually writes for a recommendation refresh: per-lens
    user-triggered refreshes (``discovery.lens.refresh:<lens_id>``) and the
    periodic scheduler job (``discovery.refresh_periodic``). Returns ``None``
    only when neither has ever completed.
    """
    try:
        row = db.execute(
            """
            SELECT MAX(finished_at)
            FROM operation_status
            WHERE status = 'completed'
              AND (
                operation_key LIKE 'discovery.lens.refresh:%'
                OR operation_key = 'discovery.refresh_periodic'
              )
            """
        ).fetchone()
        last = row[0] if row and row[0] else None
        return {"last_refresh_at": str(last) if last else None}
    except Exception as exc:
        raise_internal("Failed to read discovery status", exc)


@router.get(
    "/settings",
    response_model=DiscoverySettingsResponse,
    summary="Get discovery settings",
)
def get_discovery_settings(
    db: sqlite3.Connection = Depends(get_db),
):
    """Return the current discovery engine settings."""
    try:
        return _read_settings(db)
    except Exception as e:
        raise_internal("Failed to read discovery settings", e)


@router.put(
    "/settings",
    response_model=DiscoverySettingsResponse,
    summary="Update discovery settings",
)
def update_discovery_settings(
    body: DiscoverySettingsUpdate,
    db: sqlite3.Connection = Depends(get_db),
    user: dict = Depends(get_current_user),
):
    """Partially update discovery settings (only supplied fields are changed)."""
    runner = OperationRunner(db)

    def _handler(_ctx):
        if body.weights is not None:
            w = body.weights
            _upsert_setting(db, "weights.source_relevance", str(w.source_relevance))
            _upsert_setting(db, "weights.topic_score", str(w.topic_score))
            _upsert_setting(db, "weights.text_similarity", str(w.text_similarity))
            _upsert_setting(db, "weights.author_affinity", str(w.author_affinity))
            _upsert_setting(db, "weights.journal_affinity", str(w.journal_affinity))
            _upsert_setting(db, "weights.recency_boost", str(w.recency_boost))
            _upsert_setting(db, "weights.citation_quality", str(w.citation_quality))
            _upsert_setting(db, "weights.feedback_adj", str(w.feedback_adj))
            _upsert_setting(db, "weights.preference_affinity", str(w.preference_affinity))
            _upsert_setting(db, "weights.usefulness_boost", str(w.usefulness_boost))
        if body.strategies is not None:
            s = body.strategies
            _upsert_setting(db, "strategies.related_works", str(s.related_works).lower())
            _upsert_setting(db, "strategies.topic_search", str(s.topic_search).lower())
            _upsert_setting(db, "strategies.followed_authors", str(s.followed_authors).lower())
            _upsert_setting(db, "strategies.coauthor_network", str(s.coauthor_network).lower())
            _upsert_setting(db, "strategies.citation_chain", str(s.citation_chain).lower())
            _upsert_setting(db, "strategies.semantic_scholar", str(s.semantic_scholar).lower())
            _upsert_setting(db, "strategies.branch_explorer", str(s.branch_explorer).lower())
            _upsert_setting(db, "strategies.taste_topics", str(s.taste_topics).lower())
            _upsert_setting(db, "strategies.taste_authors", str(s.taste_authors).lower())
            _upsert_setting(db, "strategies.taste_venues", str(s.taste_venues).lower())
            _upsert_setting(db, "strategies.recent_wins", str(s.recent_wins).lower())
        if body.limits is not None:
            lim = body.limits
            _upsert_setting(db, "limits.max_results", str(lim.max_results))
            _upsert_setting(db, "limits.max_candidates_per_strategy", str(lim.max_candidates_per_strategy))
            _upsert_setting(db, "limits.recency_window_years", str(lim.recency_window_years))
            _upsert_setting(db, "limits.feedback_decay_days_full", str(lim.feedback_decay_days_full))
            _upsert_setting(db, "limits.feedback_decay_days_half", str(lim.feedback_decay_days_half))
        if body.schedule is not None:
            _upsert_setting(db, "schedule.refresh_interval_hours", str(body.schedule.refresh_interval_hours))
            _upsert_setting(db, "schedule.graph_maintenance_interval_hours", str(body.schedule.graph_maintenance_interval_hours))
        if body.cache is not None:
            _upsert_setting(db, "cache.similarity_ttl_hours", str(body.cache.similarity_ttl_hours))
        if body.sources is not None:
            sources = body.sources
            _upsert_setting(db, "sources.openalex.enabled", str(sources.openalex.enabled).lower())
            _upsert_setting(db, "sources.openalex.weight", str(sources.openalex.weight))
            _upsert_setting(db, "sources.semantic_scholar.enabled", str(sources.semantic_scholar.enabled).lower())
            _upsert_setting(db, "sources.semantic_scholar.weight", str(sources.semantic_scholar.weight))
            _upsert_setting(db, "sources.crossref.enabled", str(sources.crossref.enabled).lower())
            _upsert_setting(db, "sources.crossref.weight", str(sources.crossref.weight))
            _upsert_setting(db, "sources.arxiv.enabled", str(sources.arxiv.enabled).lower())
            _upsert_setting(db, "sources.arxiv.weight", str(sources.arxiv.weight))
            _upsert_setting(db, "sources.biorxiv.enabled", str(sources.biorxiv.enabled).lower())
            _upsert_setting(db, "sources.biorxiv.weight", str(sources.biorxiv.weight))
        if body.branches is not None:
            branches = body.branches
            _upsert_setting(db, "branches.temperature", str(branches.temperature))
            _upsert_setting(db, "branches.max_clusters", str(branches.max_clusters))
            _upsert_setting(db, "branches.max_active_for_retrieval", str(branches.max_active_for_retrieval))
            _upsert_setting(db, "branches.query_core_variants", str(branches.query_core_variants))
            _upsert_setting(db, "branches.query_explore_variants", str(branches.query_explore_variants))
        if body.monitor_defaults is not None:
            monitor_defaults = body.monitor_defaults
            _upsert_setting(db, "monitor_defaults.author_per_refresh", str(monitor_defaults.author_per_refresh))
            _upsert_setting(db, "monitor_defaults.search_limit", str(monitor_defaults.search_limit))
            _upsert_setting(db, "monitor_defaults.search_temperature", str(monitor_defaults.search_temperature))
            _upsert_setting(db, "monitor_defaults.recency_years", str(monitor_defaults.recency_years))
            _upsert_setting(db, "monitor_defaults.include_preprints", str(monitor_defaults.include_preprints).lower())
            _upsert_setting(db, "monitor_defaults.semantic_scholar_bulk", str(monitor_defaults.semantic_scholar_bulk).lower())
        if body.embedding_model is not None:
            _upsert_setting(db, "embedding_model", body.embedding_model)
        if body.recommendation_mode is not None:
            mode = str(body.recommendation_mode or "balanced").strip().lower()
            if mode not in {"explore", "balanced", "exploit"}:
                mode = "balanced"
            _upsert_setting(db, "recommendation_mode", mode)
        return OperationOutcome(status="completed", message="Discovery settings updated", result={"updated": True})

    try:
        runner.run(
            operation_key="discovery.settings.update",
            handler=_handler,
            trigger_source="user",
            actor=str(user.get("username") or "api_user"),
        )
        try:
            from alma.api.scheduler import (
                reschedule_citation_graph_maintenance,
                reschedule_discovery_refresh,
            )
            interval_row = db.execute(
                "SELECT value FROM discovery_settings WHERE key = 'schedule.refresh_interval_hours'"
            ).fetchone()
            if interval_row:
                reschedule_discovery_refresh(int(interval_row["value"]))
            graph_interval_row = db.execute(
                "SELECT value FROM discovery_settings WHERE key = 'schedule.graph_maintenance_interval_hours'"
            ).fetchone()
            if graph_interval_row:
                reschedule_citation_graph_maintenance(int(graph_interval_row["value"]))
        except Exception as e:
            logger.debug("Could not reschedule discovery maintenance jobs: %s", e)
        return _read_settings(db)
    except Exception as e:
        raise_internal("Failed to update discovery settings", e)


@router.post(
    "/settings/reset",
    response_model=DiscoverySettingsResponse,
    summary="Reset discovery settings to defaults",
)
def reset_discovery_settings(
    db: sqlite3.Connection = Depends(get_db),
    user: dict = Depends(get_current_user),
):
    """Reset all discovery settings to their default values."""
    runner = OperationRunner(db)

    def _handler(_ctx):
        discovery_app.reset_settings_to_defaults(db)
        return OperationOutcome(status="completed", message="Discovery settings reset to defaults")

    try:
        runner.run(
            operation_key="discovery.settings.reset",
            handler=_handler,
            trigger_source="user",
            actor=str(user.get("username") or "api_user"),
        )
        return _read_settings(db)
    except Exception as e:
        raise_internal("Failed to reset discovery settings", e)


# ===================================================================
# Refresh / Generate
# ===================================================================

@router.post(
    "/refresh",
    summary="Refresh recommendations",
)
def refresh_recommendations(
    background: bool = Query(True, description="Run refresh in background and track in Activity"),
    db: sqlite3.Connection = Depends(get_db),
):
    """Trigger the discovery engine to generate new recommendations.

    Clears existing neutral (unseen/undismissed/unliked) recommendations
    and regenerates fresh ones based on the user's liked publications.

    Returns:
        Dict with the count of new recommendations generated.
    """
    from alma.discovery.engine import refresh_recommendations as refresh_global_recommendations
    from alma.api.scheduler import (
        activity_envelope,
        add_job_log,
        find_active_job,
        schedule_immediate,
        set_job_status,
    )

    db_path = str(get_db_path())
    operation_key = "discovery.refresh_recommendations"
    existing = find_active_job(operation_key)
    if existing:
        return activity_envelope(
            str(existing.get("job_id") or ""),
            status="already_running",
            operation_key=operation_key,
            message="Discovery refresh already running",
        )

    def _run_refresh(job_id: str) -> dict:
        conn = open_db_connection()
        usage_before = openalex_usage_snapshot()
        try:
            liked_count = conn.execute("SELECT COUNT(*) FROM papers WHERE status='library'").fetchone()[0]
            existing_count = conn.execute("SELECT COUNT(*) FROM recommendations").fetchone()[0]
            add_job_log(
                job_id,
                f"Starting discovery refresh (liked={liked_count}, existing_recommendations={existing_count})",
                step="preflight",
            )

            with source_diagnostics_scope() as source_diag:
                new_recs = refresh_global_recommendations(db_path)
                http_source_diagnostics = source_diag.summary()
            new_count = len(new_recs or [])
            after_count = conn.execute("SELECT COUNT(*) FROM recommendations").fetchone()[0]
            usage_after = openalex_usage_snapshot()
            usage_delta = openalex_usage_delta(usage_before, usage_after)
            add_job_log(
                job_id,
                (
                    "OpenAlex usage: "
                    f"calls={usage_delta['openalex_calls']}, "
                    f"saved_by_cache={usage_delta['openalex_calls_saved_by_cache']}, "
                    f"retries={usage_delta['openalex_retries']}, "
                    f"rate_limited={usage_delta['openalex_rate_limited_events']}, "
                    f"credits_used={usage_delta['openalex_credits_used']}, "
                    f"credits_remaining={usage_delta['openalex_credits_remaining']}"
                ),
                step="api_summary",
                data=usage_delta,
            )
            add_job_log(
                job_id,
                "Discovery refresh source diagnostics recorded",
                step="source_diagnostics",
                data=http_source_diagnostics,
            )
            add_job_log(
                job_id,
                f"Discovery refresh finished (new={new_count}, total_recommendations={after_count})",
                step="done",
            )
            return {
                "new_recommendations": new_count,
                "total_recommendations": after_count,
                "openalex_usage": usage_delta,
                "source_diagnostics": http_source_diagnostics,
            }
        finally:
            conn.close()

    if not background:
        job_id = f"discovery_refresh_{uuid.uuid4().hex[:10]}"
        try:
            set_job_status(
                job_id,
                status="running",
                operation_key=operation_key,
                trigger_source="user",
                started_at=datetime.utcnow().isoformat(),
                message="Refreshing discovery recommendations",
            )
            result = _run_refresh(job_id)
            set_job_status(
                job_id,
                status="completed",
                finished_at=datetime.utcnow().isoformat(),
                message="Discovery refresh completed",
                result=result,
            )
            return activity_envelope(job_id, status="completed", operation_key=operation_key, **result)
        except Exception as e:
            set_job_status(
                job_id,
                status="failed",
                finished_at=datetime.utcnow().isoformat(),
                message="Discovery refresh failed",
                error=str(e),
            )
            raise_internal("Discovery refresh failed", e)

    job_id = f"discovery_refresh_{uuid.uuid4().hex[:10]}"
    set_job_status(
        job_id,
        status="queued",
        operation_key=operation_key,
        trigger_source="user",
        started_at=datetime.utcnow().isoformat(),
        message="Refreshing discovery recommendations",
    )

    def _runner() -> dict:
        return _run_refresh(job_id)

    try:
        schedule_immediate(job_id, _runner)
        return activity_envelope(
            job_id,
            status="queued",
            operation_key=operation_key,
            message="Discovery refresh queued",
        )
    except Exception as e:
        set_job_status(
            job_id,
            status="failed",
            finished_at=datetime.utcnow().isoformat(),
            message="Discovery refresh scheduling failed",
            error=str(e),
        )
        raise_internal("Failed to schedule discovery refresh", e)


# ===================================================================
# Discover Similar (with cache)
# ===================================================================

def _similarity_result_items_from_raw(raw_results: list[dict]) -> list[SimilarityResultItem]:
    """Shape raw discovery-engine rows into SimilarityResultItem models.

    The engine emits candidates in the canonical merge shape —
    `{id, source_type, source_key, title, authors, url, doi, score,
    score_breakdown, year, ...}` (see `engine.py::merge_candidate`).
    Historically this helper looked up `recommended_title` / `recommended_*`
    keys that never existed on the merge shape, so every `title` /
    `authors` / `url` / `doi` came back empty. Fixed 2026-04-24 alongside
    T2.
    """
    items: list[SimilarityResultItem] = []
    for r in raw_results:
        breakdown = r.get("score_breakdown")
        if isinstance(breakdown, str):
            try:
                breakdown = json.loads(breakdown)
            except (json.JSONDecodeError, TypeError):
                breakdown = None
        # Prefer the candidate's explicit `paper_id` (dense-fallback path
        # sets this to the real `papers.id` so `<PaperCard>` can open the
        # detail panel). For network-sourced rows, fall back to the
        # transient merge key which at least keeps React list keys stable.
        paper_id = str(r.get("paper_id") or r.get("id") or "").strip() or None
        items.append(SimilarityResultItem(
            paper_id=paper_id,
            source_type=str(r.get("source_type") or ""),
            source_key=str(r.get("source_key") or ""),
            title=str(r.get("title") or ""),
            authors=(r.get("authors") or None),
            url=(r.get("url") or None),
            doi=(r.get("doi") or None),
            score=float(r.get("score") or 0),
            score_breakdown=breakdown,
            year=r.get("year"),
        ))
    return items


def _write_similarity_cache(
    db: sqlite3.Connection,
    cache_key: str,
    sorted_ids: list[str],
    items: list[SimilarityResultItem],
) -> None:
    """Persist similarity results into the cache with settings-driven TTL."""
    try:
        from datetime import datetime as _dt, timedelta
        ttl_row = db.execute(
            "SELECT value FROM discovery_settings WHERE key = 'cache.similarity_ttl_hours'"
        ).fetchone()
        ttl_hours = int(ttl_row["value"]) if ttl_row else 24
        now = _dt.utcnow()
        expires_at = now + timedelta(hours=ttl_hours)
        cache_data = [item.model_dump() for item in items]
        db.execute(
            """INSERT OR REPLACE INTO similarity_cache
               (cache_key, paper_ids, results, created_at, expires_at)
               VALUES (?, ?, ?, ?, ?)""",
            (cache_key, json.dumps(sorted_ids), json.dumps(cache_data),
             now.isoformat(), expires_at.isoformat()),
        )
        db.commit()
    except Exception as exc:
        logger.debug("Cache store failed: %s", redact_sensitive_text(str(exc)))


@router.post(
    "/similar",
    summary="Discover papers similar to selected publications",
)
def discover_similar(
    req: SimilarityRequest,
    db: sqlite3.Connection = Depends(get_db),
):
    """Return cached similar papers inline, or queue a discovery Activity job.

    A fresh cache entry is served synchronously (fast read, no job). A cache
    miss or forced refresh queues the discovery engine on the APS scheduler
    pool and returns an ``activity_envelope``; the frontend polls
    ``GET /activity/{job_id}`` for the ``result.similarity`` payload.
    """
    from alma.api.scheduler import (
        activity_envelope,
        find_active_job,
        schedule_immediate,
        set_job_status,
    )

    if not req.paper_ids:
        raise HTTPException(status_code=400, detail="No paper IDs provided")

    sorted_ids = sorted(req.paper_ids)
    cache_key = hashlib.sha256(json.dumps(sorted_ids).encode()).hexdigest()[:16]

    if not req.force:
        try:
            row = db.execute(
                "SELECT results, expires_at FROM similarity_cache WHERE cache_key = ?",
                (cache_key,),
            ).fetchone()
            if row:
                from datetime import datetime as _dt
                expires_at = _dt.fromisoformat(row["expires_at"])
                if _dt.utcnow() < expires_at:
                    cached_results = json.loads(row["results"])
                    return SimilarityResponse(
                        results=[SimilarityResultItem(**r) for r in cached_results],
                        cached=True,
                        cache_key=cache_key,
                        seed_count=len(req.paper_ids),
                    )
                db.execute("DELETE FROM similarity_cache WHERE cache_key = ?", (cache_key,))
                db.commit()
        except Exception as exc:
            logger.debug("Cache lookup failed: %s", redact_sensitive_text(str(exc)))

    operation_key = f"discovery.similar:{cache_key}"
    existing = find_active_job(operation_key)
    if existing:
        return activity_envelope(
            str(existing.get("job_id") or ""),
            status="already_running",
            operation_key=operation_key,
            message="Similarity search already running for these seeds",
            cache_key=cache_key,
        )

    job_id = f"similar_{uuid.uuid4().hex[:10]}"
    set_job_status(
        job_id,
        status="queued",
        operation_key=operation_key,
        trigger_source="user",
        started_at=datetime.utcnow().isoformat(),
        message=f"Discovering similar papers for {len(req.paper_ids)} seed(s)",
    )

    paper_ids = list(req.paper_ids)
    limit = req.limit

    def _runner():
        try:
            from alma.discovery.engine import (
                discover_similar_with_meta as discover_similar_candidates_with_meta,
            )
            from alma.api.models import SimilarityChannelStat

            db_path = str(get_db_path())
            raw_results, meta = discover_similar_candidates_with_meta(
                db_path, paper_ids, limit=limit
            )
            items = _similarity_result_items_from_raw(raw_results)
            channels = [
                SimilarityChannelStat(**channel)
                for channel in (meta.get("channels") or [])
            ]
            dense_fallback_used = bool(meta.get("dense_fallback_used", False))

            cache_conn = open_db_connection()
            try:
                _write_similarity_cache(cache_conn, cache_key, sorted_ids, items)
            finally:
                cache_conn.close()

            payload = SimilarityResponse(
                results=items,
                cached=False,
                cache_key=cache_key,
                seed_count=len(paper_ids),
                channels=channels,
                dense_fallback_used=dense_fallback_used,
            ).model_dump()
            set_job_status(
                job_id,
                status="completed",
                finished_at=datetime.utcnow().isoformat(),
                processed=len(items),
                total=len(items),
                message=(
                    f"Similarity ready ({len(items)} results"
                    + (", dense-fallback" if dense_fallback_used else "")
                    + ")"
                ),
                result={"similarity": payload},
            )
        except Exception as exc:  # pragma: no cover - runner crash path
            logger.error("Discover similar runner failed: %s", exc)
            set_job_status(
                job_id,
                status="failed",
                finished_at=datetime.utcnow().isoformat(),
                message="Discover similar failed",
                error=str(exc),
            )

    schedule_immediate(job_id, _runner)
    return activity_envelope(
        job_id,
        status="queued",
        operation_key=operation_key,
        message="Queued similarity discovery",
        cache_key=cache_key,
    )


@router.post(
    "/similarity-cache/clear",
    summary="Clear cached similarity-search results",
)
def clear_similarity_cache(
    db: sqlite3.Connection = Depends(get_db),
    user: dict = Depends(get_current_user),
):
    runner = OperationRunner(db)

    def _handler(_ctx):
        try:
            count = int(
                (
                    db.execute("SELECT COUNT(*) AS c FROM similarity_cache").fetchone()["c"]
                )
                or 0
            )
        except sqlite3.OperationalError:
            count = 0
        try:
            db.execute("DELETE FROM similarity_cache")
        except sqlite3.OperationalError:
            count = 0
        return OperationOutcome(
            status="completed",
            message=f"Cleared {count} similarity-cache entries",
            result={"deleted": count},
        )

    try:
        op = runner.run(
            operation_key="discovery.similarity_cache.clear",
            handler=_handler,
            trigger_source="user",
            actor=str(user.get("username") or "api_user"),
            queued=False,
        )
        return {"success": True, "operation": op, "deleted": int((op.get("result") or {}).get("deleted") or 0)}
    except Exception as exc:
        raise_internal("Failed to clear similarity cache", exc)


# ===================================================================
# Manual Discovery Search / Add
# ===================================================================

@router.post(
    "/manual-search",
    summary="Search online sources by query/title/author/link (Activity-backed)",
)
def manual_discovery_search(
    req: ManualSearchRequest,
):
    """Queue a multi-source online search as an Activity job and return its envelope.

    Fans out across OpenAlex, Semantic Scholar, Crossref, arXiv, and bioRxiv
    via the shared ``search_across_sources`` stack and returns deduplicated
    results ranked by personal fit. Runs on the APS scheduler pool so the
    request thread is released before the remote round-trips. Frontend
    callers poll ``GET /activity/{job_id}`` and read the ``result`` payload
    once the status turns ``completed``.
    """
    from alma.api.scheduler import (
        activity_envelope,
        find_active_job,
        schedule_immediate,
        set_job_status,
    )

    query = (req.query or "").strip()
    if not query:
        raise HTTPException(status_code=400, detail="query is required")

    operation_key = f"discovery.manual_search:{hashlib.sha1(query.encode()).hexdigest()[:10]}"
    existing = find_active_job(operation_key)
    if existing:
        return activity_envelope(
            str(existing.get("job_id") or ""),
            status="already_running",
            operation_key=operation_key,
            message="Manual search already running for this query",
        )

    job_id = f"manual_search_{uuid.uuid4().hex[:10]}"
    set_job_status(
        job_id,
        status="queued",
        operation_key=operation_key,
        trigger_source="user",
        started_at=datetime.utcnow().isoformat(),
        message=f"Searching online sources for '{query[:80]}'",
    )

    def _runner():
        try:
            from alma.application.openalex_manual import search_online_sources

            conn = open_db_connection()
            try:
                items = search_online_sources(conn, query, limit=req.limit)
            finally:
                conn.close()
            set_job_status(
                job_id,
                status="completed",
                finished_at=datetime.utcnow().isoformat(),
                processed=len(items),
                total=len(items),
                message=f"Found {len(items)} results",
                result={"query": query, "total": len(items), "items": items},
            )
        except Exception as exc:  # pragma: no cover - runner crash path
            logger.error("Manual discovery search failed: %s", exc)
            set_job_status(
                job_id,
                status="failed",
                finished_at=datetime.utcnow().isoformat(),
                message="Manual discovery search failed",
                error=str(exc),
            )

    schedule_immediate(job_id, _runner)
    return activity_envelope(
        job_id,
        status="queued",
        operation_key=operation_key,
        message=f"Queued online search for '{query[:80]}'",
    )


@router.post(
    "/manual-search/add",
    response_model=PaperResponse,
    summary="Add a manually discovered paper to library",
)
def manual_discovery_add(
    req: ManualAddRequest,
    db: sqlite3.Connection = Depends(get_db),
    user: dict = Depends(get_current_user),
):
    """Resolve one external paper and upsert it into local library."""
    runner = OperationRunner(db)
    captured: dict = {}

    def _handler(_ctx):
        from alma.application.openalex_manual import add_work_to_library

        out = add_work_to_library(
            db,
            openalex_id=req.openalex_id,
            doi=req.doi,
            link=req.link,
            title=req.title,
            query=req.query,
            added_from="discovery_manual",
        )
        captured["paper"] = out
        return OperationOutcome(
            status="completed",
            message="Paper added to library",
            result={"paper_id": out.get("id"), "match_source": out.get("match_source")},
        )

    try:
        runner.run(
            operation_key="discovery.manual_add",
            handler=_handler,
            trigger_source="user",
            actor=str(user.get("username") or "api_user"),
            queued=False,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except HTTPException:
        raise
    except Exception as exc:
        raise_internal("Manual discovery add failed", exc)

    paper = dict(captured.get("paper") or {})
    paper.pop("match_source", None)
    return PaperResponse(**paper)


# ===================================================================
# Recommendations
# ===================================================================

@router.get(
    "/recommendations",
    response_model=List[RecommendationResponse],
    summary="List recommendations",
)
def list_recommendations(
    limit: int = Query(50, ge=1, le=500),
    offset: int = Query(0, ge=0),
    search: Optional[str] = Query(None, description="Filter by title/authors/source key"),
    semantic: bool = Query(False, description="Reserved semantic filter toggle"),
    db: sqlite3.Connection = Depends(get_db),
):
    """Return recommendations that have not been dismissed, with pagination."""
    try:
        rows = discovery_app.list_recommendations(
            db,
            limit=limit,
            offset=offset,
            search=search,
            semantic=semantic,
        )
        return [RecommendationResponse(**r) for r in rows]
    except Exception as e:
        raise_internal("Failed to list recommendations", e)


@router.post(
    "/recommendations/{rec_id}/save",
    summary="Save a recommendation to library",
)
def save_recommendation(
    rec_id: str,
    body: RecommendationLibraryActionRequest | None = None,
    db: sqlite3.Connection = Depends(get_db),
    user: dict = Depends(get_current_user),
):
    """Save a recommendation to the library without forcing a rating."""
    runner = OperationRunner(db)

    def _handler(_ctx):
        out = discovery_app.mark_recommendation_action(
            db,
            rec_id,
            "save",
            rating=(body.rating if body else None),
        )
        if out is None:
            return OperationOutcome(status="noop", message="Recommendation not found", result={"id": rec_id})
        return OperationOutcome(status="completed", message="Recommendation saved", result=out)

    op = runner.run(
        operation_key=f"discovery.recommendation.save:{rec_id}",
        handler=_handler,
        trigger_source="user",
        actor=str(user.get("username") or "api_user"),
    )
    if op["status"] == "noop":
        raise HTTPException(status_code=404, detail="Recommendation not found")
    return op.get("result") or {"id": rec_id, "save": True}


@router.post(
    "/recommendations/{rec_id}/like",
    summary="Like a recommendation",
)
def like_recommendation(
    rec_id: str,
    body: RecommendationLibraryActionRequest | None = None,
    db: sqlite3.Connection = Depends(get_db),
    user: dict = Depends(get_current_user),
):
    """Mark a recommendation as liked."""
    runner = OperationRunner(db)

    def _handler(_ctx):
        out = discovery_app.mark_recommendation_action(
            db,
            rec_id,
            "like",
            rating=(body.rating if body else None),
        )
        if out is None:
            return OperationOutcome(status="noop", message="Recommendation not found", result={"id": rec_id})
        return OperationOutcome(status="completed", message="Recommendation liked", result=out)

    op = runner.run(
        operation_key=f"discovery.recommendation.like:{rec_id}",
        handler=_handler,
        trigger_source="user",
        actor=str(user.get("username") or "api_user"),
    )
    if op["status"] == "noop":
        raise HTTPException(status_code=404, detail="Recommendation not found")
    return op.get("result") or {"id": rec_id, "like": True}


@router.post(
    "/recommendations/{rec_id}/dismiss",
    summary="Dismiss a recommendation",
)
def dismiss_recommendation(
    rec_id: str,
    db: sqlite3.Connection = Depends(get_db),
    user: dict = Depends(get_current_user),
):
    """Mark a recommendation as dismissed."""
    runner = OperationRunner(db)

    def _handler(_ctx):
        out = discovery_app.mark_recommendation_action(db, rec_id, "dismiss")
        if out is None:
            return OperationOutcome(status="noop", message="Recommendation not found", result={"id": rec_id})
        return OperationOutcome(status="completed", message="Recommendation dismissed", result=out)

    op = runner.run(
        operation_key=f"discovery.recommendation.dismiss:{rec_id}",
        handler=_handler,
        trigger_source="user",
        actor=str(user.get("username") or "api_user"),
    )
    if op["status"] == "noop":
        raise HTTPException(status_code=404, detail="Recommendation not found")
    return op.get("result") or {"id": rec_id, "dismiss": True}


@router.post(
    "/recommendations/{rec_id}/dislike",
    summary="Dislike a recommendation (negative signal, no system-wide hide)",
)
def dislike_recommendation(
    rec_id: str,
    db: sqlite3.Connection = Depends(get_db),
    user: dict = Depends(get_current_user),
):
    """Record a negative signal on a recommendation without hiding the
    paper system-wide.

    Distinct from `dismiss`: `dismiss` flips `papers.status` to
    `dismissed` so the paper disappears from every surface; `dislike`
    only updates the recommendation's `user_action` + writes a
    feedback event + adjusts lens signals. The underlying `papers`
    row stays untouched, so the user can still find the paper via
    Find & add or future recommendations — just with less lift.
    """
    runner = OperationRunner(db)

    def _handler(_ctx):
        out = discovery_app.mark_recommendation_action(db, rec_id, "dislike")
        if out is None:
            return OperationOutcome(status="noop", message="Recommendation not found", result={"id": rec_id})
        return OperationOutcome(status="completed", message="Recommendation disliked", result=out)

    op = runner.run(
        operation_key=f"discovery.recommendation.dislike:{rec_id}",
        handler=_handler,
        trigger_source="user",
        actor=str(user.get("username") or "api_user"),
    )
    if op["status"] == "noop":
        raise HTTPException(status_code=404, detail="Recommendation not found")
    return op.get("result") or {"id": rec_id, "dislike": True}


@router.post(
    "/recommendations/{rec_id}/seen",
    summary="Mark recommendation as seen",
)
def mark_seen(
    rec_id: str,
    db: sqlite3.Connection = Depends(get_db),
    user: dict = Depends(get_current_user),
):
    """Mark a recommendation as seen."""
    runner = OperationRunner(db)

    def _handler(_ctx):
        out = discovery_app.mark_recommendation_action(db, rec_id, "seen")
        if out is None:
            return OperationOutcome(status="noop", message="Recommendation not found", result={"id": rec_id})
        return OperationOutcome(status="completed", message="Recommendation marked seen", result=out)

    op = runner.run(
        operation_key=f"discovery.recommendation.seen:{rec_id}",
        handler=_handler,
        trigger_source="user",
        actor=str(user.get("username") or "api_user"),
    )
    if op["status"] == "noop":
        raise HTTPException(status_code=404, detail="Recommendation not found")
    return op.get("result") or {"id": rec_id, "seen": True}


@router.get(
    "/recommendations/{rec_id}/explain",
    response_model=RecommendationExplainResponse,
    summary="Explain a recommendation's score",
)
def explain_recommendation(
    rec_id: str,
    db: sqlite3.Connection = Depends(get_db),
):
    """Return a detailed breakdown of how a recommendation's score was computed.

    Shows each scoring signal's raw value, weight, and weighted contribution,
    along with human-readable descriptions of what each signal measures.
    """
    row = db.execute(
        """
        SELECT
            r.*,
            r.id,
            p.title,
            p.id AS paper_id,
            r.score,
            r.score_breakdown
        FROM recommendations r
        LEFT JOIN papers p ON p.id = r.paper_id
        WHERE r.id = ?
        """,
        (rec_id,),
    ).fetchone()
    if not row:
        raise HTTPException(status_code=404, detail="Recommendation not found")
    row_data = dict(row)

    breakdown_response = _build_explain_breakdown(row_data.get("score_breakdown"))

    return RecommendationExplainResponse(
        id=str(row_data.get("id") or rec_id),
        title=str(row_data.get("title") or row_data.get("paper_id") or row_data.get("id") or rec_id),
        score=float(row_data.get("score") or 0.0),
        source_type=str(row_data.get("source_type") or "lens_retrieval"),
        source_key=str(row_data.get("source_key") or row_data.get("lens_id") or ""),
        breakdown=breakdown_response,
        explanation=None,
    )


@router.delete(
    "/recommendations",
    status_code=status.HTTP_204_NO_CONTENT,
    summary="Clear all recommendations",
)
def clear_recommendations(
    db: sqlite3.Connection = Depends(get_db),
    user: dict = Depends(get_current_user),
):
    """Delete all recommendations."""
    runner = OperationRunner(db)

    def _handler(_ctx):
        deleted = discovery_app.clear_recommendations(db)
        return OperationOutcome(
            status="completed" if deleted > 0 else "noop",
            message=f"Cleared {deleted} recommendations" if deleted > 0 else "No recommendations to clear",
            result={"deleted": deleted},
        )

    runner.run(
        operation_key="discovery.recommendations.clear",
        handler=_handler,
        trigger_source="user",
        actor=str(user.get("username") or "api_user"),
    )


@router.get(
    "/stats",
    summary="Get discovery stats",
)
def discovery_stats(
    db: sqlite3.Connection = Depends(get_db),
):
    """Return aggregate discovery statistics: total, seen, liked, dismissed."""
    try:
        return discovery_app.recommendation_stats(db)
    except Exception as e:
        raise_internal("Failed to load discovery stats", e)

