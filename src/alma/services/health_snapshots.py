"""Durable Health snapshots and their one refresh coordinator.

Health GET routes call only stored readers here. Corpus-wide assessors and
operation planning run in background refreshes: startup warm-up, periodic floor,
successful mutation debounce, explicit user refresh, and job-terminal debounce
all converge on the same coordinator.
"""

from __future__ import annotations

import logging
import sqlite3
import uuid
from datetime import datetime, timedelta, timezone
from typing import Any

from alma.application import materialized_views as mv
from alma.core.time import utcnow
from alma.services import health as health_service

logger = logging.getLogger(__name__)

HEALTH_OPERATIONS_VIEW_KEY = "health:operations"
HEALTH_REFRESH_OPERATION_KEY = "materialize.health.snapshots"
HEALTH_REFRESH_JOB_ID = "health_snapshot_refresh"

_PURE_POST_PREFIXES = (
    "/api/v1/library/import/preflight/",
)
_PURE_POST_PATHS = frozenset(
    {
        "/api/v1/library/import/search",
        "/api/v1/library/import/search/authors",
        "/api/v1/library/import/search/authors/top-works",
        "/api/v1/library/import/search/stream",
        "/api/v1/discovery/manual-search",
        "/api/v1/discovery/similar",
        "/api/v1/health/refresh",
    }
)


def mutation_may_change_health(method: str, path: str) -> bool:
    """Classify HTTP writes that can invalidate a Health snapshot.

    Several pure-read workflows use POST because their query is structured or
    multipart. Keep them out of the refresh coordinator; Activity-backed writes
    are refreshed again at their terminal status.
    """

    verb = str(method or "").upper()
    normalized = str(path or "").rstrip("/") or "/"
    if verb not in {"POST", "PUT", "PATCH", "DELETE"}:
        return False
    if verb == "POST" and (
        normalized in _PURE_POST_PATHS
        or normalized.endswith(("/estimate", "/preview", "/lookup"))
        or any(normalized.startswith(prefix) for prefix in _PURE_POST_PREFIXES)
    ):
        return False
    return True


def _build_operations(conn: sqlite3.Connection) -> dict[str, Any]:
    from alma.services import maintenance

    corpus = stored_payload(conn, health_service.HEALTH_CORPUS_VIEW_KEY)
    return maintenance.list_operations(conn, health_payload=corpus)


mv.register(
    mv.View(
        key=HEALTH_OPERATIONS_VIEW_KEY,
        fingerprint_sql="""
            SELECT
              'health-operations-v1',
              COALESCE((SELECT fingerprint FROM materialized_views
                        WHERE view_key = 'health:corpus'), ''),
              COALESCE((SELECT fingerprint FROM materialized_views
                        WHERE view_key = 'health:authors'), ''),
              (SELECT COUNT(*) FROM operation_status),
              (SELECT COALESCE(MAX(updated_at), '') FROM operation_status),
              (SELECT COUNT(*) FROM discovery_settings
                 WHERE key LIKE 'maintenance.%'
                    OR key IN ('background.idle_wait_minutes',
                               'background.reserved_api_calls'))
        """,
        build_fn=_build_operations,
        operation_key="materialize.health.operations",
    )
)


def stored_envelope(conn: sqlite3.Connection, view_key: str) -> dict[str, Any]:
    """Read one durable Health view without fingerprinting or building."""

    stored = mv.get_stored(conn, view_key, include_rebuilding=False)
    if stored is not None:
        return stored
    return {
        "payload": {},
        "stale": True,
        "rebuilding": refresh_in_flight(),
        "computed_at": None,
        "fingerprint": "",
    }


def stored_payload(conn: sqlite3.Connection, view_key: str) -> dict[str, Any]:
    return dict(stored_envelope(conn, view_key).get("payload") or {})


def stored_operations(conn: sqlite3.Connection) -> dict[str, Any]:
    """Stored operation plan with cheap live budget/policy overlay."""

    from alma.core.http_sources import provider_remaining_credits
    from alma.core.network_policy import network_policy_status

    payload = stored_payload(conn, HEALTH_OPERATIONS_VIEW_KEY)
    from alma.services.maintenance import finalize_operation_plan

    operations = [dict(row) for row in (payload.get("operations") or [])]
    recommended = finalize_operation_plan(operations)
    budget = dict(payload.get("api_budget") or {})
    budget["openalex_credits_remaining"] = provider_remaining_credits("openalex")
    budget["network_policy"] = network_policy_status().to_wire()
    return {
        "generated_at": payload.get("generated_at"),
        "recommended_next": recommended,
        "stages": payload.get("stages") or [],
        "operations": operations,
        "api_budget": budget,
        "stale": not bool(payload),
        "rebuilding": refresh_in_flight(),
    }


def refresh_in_flight() -> bool:
    try:
        from alma.api.scheduler import has_active_job_in_memory

        return has_active_job_in_memory(HEALTH_REFRESH_OPERATION_KEY)
    except Exception:
        return False


def rebuild_all() -> dict[str, Any]:
    """Rebuild corpus → authors → operation plan in dependency order."""

    from alma.api.deps import open_db_connection

    conn = open_db_connection()
    try:
        corpus = mv.rebuild(conn, health_service.HEALTH_CORPUS_VIEW_KEY)
        authors = mv.rebuild(conn, health_service.HEALTH_AUTHORS_VIEW_KEY)
        operations = mv.rebuild(conn, HEALTH_OPERATIONS_VIEW_KEY)
        return {
            "corpus_dimensions": len(corpus.get("dimensions") or []),
            "author_dimensions": len(authors.get("dimensions") or []),
            "operations": len(operations.get("operations") or []),
        }
    finally:
        conn.close()


def request_refresh(*, delay_seconds: int = 2) -> None:
    """Debounce a non-Activity refresh after relevant state changes."""

    try:
        from apscheduler.triggers.date import DateTrigger

        from alma.api.scheduler import get_running_scheduler

        scheduler = get_running_scheduler()
        if scheduler is None:
            return
        scheduler.add_job(
            rebuild_all,
            trigger=DateTrigger(
                run_date=datetime.now(timezone.utc)
                + timedelta(seconds=max(0, int(delay_seconds)))
            ),
            id=HEALTH_REFRESH_JOB_ID,
            name="Refresh Health snapshots",
            replace_existing=True,
            coalesce=True,
            max_instances=1,
        )
    except Exception:
        logger.debug("Health snapshot refresh debounce unavailable", exc_info=True)


def schedule_manual_refresh() -> dict[str, Any]:
    """Queue user-visible refresh and return scheduler Activity envelope."""

    from alma.api.scheduler import (
        activity_envelope,
        find_active_job,
        schedule_immediate,
        set_job_status,
    )

    existing = find_active_job(HEALTH_REFRESH_OPERATION_KEY)
    if existing:
        return activity_envelope(
            str(existing.get("job_id") or ""),
            status="already_running",
            operation_key=HEALTH_REFRESH_OPERATION_KEY,
            message="Health assessment already refreshing",
        )

    job_id = f"health_refresh_{uuid.uuid4().hex[:10]}"
    set_job_status(
        job_id,
        status="queued",
        operation_key=HEALTH_REFRESH_OPERATION_KEY,
        trigger_source="user",
        started_at=utcnow().isoformat(),
        message="Refreshing Health snapshots",
    )

    def _runner() -> None:
        try:
            set_job_status(
                job_id,
                status="running",
                message="Assessing corpus and maintenance plan",
            )
            result = rebuild_all()
            set_job_status(
                job_id,
                status="completed",
                finished_at=utcnow().isoformat(),
                message="Health snapshots refreshed",
                result=result,
            )
        except Exception as exc:
            set_job_status(
                job_id,
                status="failed",
                finished_at=utcnow().isoformat(),
                message="Health refresh failed",
                error=f"{type(exc).__name__}: {exc}",
            )

    schedule_immediate(job_id, _runner)
    return activity_envelope(
        job_id,
        status="queued",
        operation_key=HEALTH_REFRESH_OPERATION_KEY,
        message="Health assessment queued",
    )
