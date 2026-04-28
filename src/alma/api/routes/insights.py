"""Insights & analytics endpoint — aggregated stats for the Insights page."""

import logging
import sqlite3
from datetime import datetime, timedelta
from typing import Any
from fastapi import APIRouter, Depends, HTTPException, status
from pydantic import BaseModel

from alma.api.deps import get_db, get_current_user
from alma.api.helpers import json_loads, raise_internal, safe_div, table_exists
from alma.application.followed_authors import get_followed_author_backfill_status
from alma.discovery.defaults import DISCOVERY_SETTINGS_DEFAULTS

logger = logging.getLogger(__name__)

router = APIRouter(
    tags=["insights"],
    dependencies=[Depends(get_current_user)],
    responses={401: {"description": "Unauthorized"}},
)


def _load_recent_operations(
    db: sqlite3.Connection,
    *,
    operation_key: str,
    limit: int = 10,
) -> list[dict[str, Any]]:
    if not table_exists(db, "operation_status"):
        return []
    rows = db.execute(
        """
        SELECT job_id, operation_key, status, message, started_at, finished_at, updated_at, result_json
        FROM operation_status
        WHERE operation_key = ?
        ORDER BY COALESCE(finished_at, updated_at, started_at) DESC
        LIMIT ?
        """,
        (operation_key, limit),
    ).fetchall()
    out: list[dict[str, Any]] = []
    for row in rows:
        out.append(
            {
                "job_id": row["job_id"],
                "operation_key": row["operation_key"],
                "status": row["status"],
                "message": row["message"],
                "started_at": row["started_at"],
                "finished_at": row["finished_at"],
                "updated_at": row["updated_at"],
                "result": json_loads(row["result_json"]) or {},
            }
        )
    return out


def _aggregate_http_source_diagnostics(
    operation_results: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    merged: dict[str, dict[str, Any]] = {}
    for entry in operation_results:
        source_diagnostics = entry.get("source_diagnostics") or {}
        http_diag = source_diagnostics.get("http") if isinstance(source_diagnostics, dict) else {}
        if not isinstance(http_diag, dict):
            continue
        for source, raw in http_diag.items():
            if not isinstance(raw, dict):
                continue
            target = merged.setdefault(
                str(source),
                {
                    "source": str(source),
                    "operations": 0,
                    "requests": 0,
                    "ok": 0,
                    "http_errors": 0,
                    "transport_errors": 0,
                    "retries": 0,
                    "latency_sum": 0.0,
                    "status_counts": {},
                    "endpoint_counts": {},
                    "last_error": None,
                },
            )
            target["operations"] += 1
            requests = int(raw.get("requests") or 0)
            target["requests"] += requests
            target["ok"] += int(raw.get("ok") or 0)
            target["http_errors"] += int(raw.get("http_errors") or 0)
            target["transport_errors"] += int(raw.get("transport_errors") or 0)
            target["retries"] += int(raw.get("retries") or 0)
            target["latency_sum"] += float(raw.get("avg_latency_ms") or 0.0) * requests
            if raw.get("last_error"):
                target["last_error"] = raw.get("last_error")
            for status_key, count in (raw.get("status_counts") or {}).items():
                counts = target["status_counts"]
                counts[str(status_key)] = int(counts.get(str(status_key)) or 0) + int(count or 0)
            for endpoint in (raw.get("top_endpoints") or []):
                if not isinstance(endpoint, dict):
                    continue
                path = str(endpoint.get("path") or "").strip()
                if not path:
                    continue
                count_value = int(endpoint.get("count") or 0)
                endpoint_counts = target["endpoint_counts"]
                endpoint_counts[path] = int(endpoint_counts.get(path) or 0) + count_value

    out: list[dict[str, Any]] = []
    for source, raw in merged.items():
        requests = int(raw.get("requests") or 0)
        avg_latency_ms = round(float(raw.get("latency_sum") or 0.0) / requests, 2) if requests else 0.0
        top_endpoints = sorted(
            (raw.get("endpoint_counts") or {}).items(),
            key=lambda item: (-int(item[1]), str(item[0])),
        )[:5]
        out.append(
            {
                "source": source,
                "operations": int(raw.get("operations") or 0),
                "requests": requests,
                "ok": int(raw.get("ok") or 0),
                "http_errors": int(raw.get("http_errors") or 0),
                "transport_errors": int(raw.get("transport_errors") or 0),
                "retries": int(raw.get("retries") or 0),
                "avg_latency_ms": avg_latency_ms,
                "status_counts": dict(raw.get("status_counts") or {}),
                "top_endpoints": [
                    {"path": str(path), "count": int(count)}
                    for path, count in top_endpoints
                ],
                "last_error": raw.get("last_error"),
            }
        )
    out.sort(key=lambda item: (-int(item.get("requests") or 0), str(item.get("source") or "")))
    return out


def _aggregate_openalex_usage(operation_results: list[dict[str, Any]]) -> dict[str, Any]:
    summary = {
        "refreshes": 0,
        "request_count": 0,
        "retry_count": 0,
        "rate_limited_events": 0,
        "calls_saved_by_cache": 0,
        "credits_used": 0,
        "credits_remaining": None,
    }
    for entry in operation_results:
        source_diagnostics = entry.get("source_diagnostics") or {}
        openalex = source_diagnostics.get("openalex") if isinstance(source_diagnostics, dict) else None
        if not isinstance(openalex, dict):
            continue
        summary["refreshes"] += 1
        summary["request_count"] += int(openalex.get("request_count") or 0)
        summary["retry_count"] += int(openalex.get("retry_count") or 0)
        summary["rate_limited_events"] += int(openalex.get("rate_limited_events") or 0)
        summary["calls_saved_by_cache"] += int(openalex.get("calls_saved_by_cache") or 0)
        summary["credits_used"] += int(openalex.get("credits_used") or 0)
        credits_remaining = openalex.get("credits_remaining")
        if credits_remaining is not None:
            summary["credits_remaining"] = credits_remaining
    return summary



def _bool_setting(value: Any, default: bool = False) -> bool:
    if value is None:
        return default
    return str(value).strip().lower() in {"1", "true", "yes", "on"}


def _library_workflow_snapshot(db: sqlite3.Connection) -> dict[str, int]:
    _defaults = {
        "total_library": 0, "queued_count": 0, "reading_count": 0,
        "done_count": 0, "excluded_count": 0, "untriaged_count": 0,
        "uncollected_count": 0,
    }
    if not table_exists(db, "papers"):
        return _defaults
    row = db.execute(
        """
        SELECT
            COALESCE(SUM(CASE WHEN status = 'library' THEN 1 ELSE 0 END), 0) AS total_library,
            COALESCE(SUM(CASE WHEN reading_status = 'queued' THEN 1 ELSE 0 END), 0) AS queued_count,
            COALESCE(SUM(CASE WHEN reading_status = 'reading' THEN 1 ELSE 0 END), 0) AS reading_count,
            COALESCE(SUM(CASE WHEN reading_status = 'done' THEN 1 ELSE 0 END), 0) AS done_count,
            COALESCE(SUM(CASE WHEN reading_status = 'excluded' THEN 1 ELSE 0 END), 0) AS excluded_count,
            COALESCE(SUM(
                CASE
                    WHEN status = 'library'
                     AND (reading_status IS NULL OR TRIM(reading_status) = '')
                    THEN 1 ELSE 0
                END
            ), 0) AS untriaged_count
        FROM papers
        """
    ).fetchone()
    uncollected_count = 0
    if table_exists(db, "collection_items"):
        try:
            uncollected_count = int(
                (
                    db.execute(
                        """
                        SELECT COUNT(*) AS c
                        FROM papers p
                        WHERE p.status = 'library'
                          AND NOT EXISTS (
                            SELECT 1 FROM collection_items ci WHERE ci.paper_id = p.id
                          )
                        """
                    ).fetchone()["c"]
                )
                or 0
            )
        except sqlite3.OperationalError:
            uncollected_count = 0

    return {
        "total_library": int((row["total_library"] if row else 0) or 0),
        "queued_count": int((row["queued_count"] if row else 0) or 0),
        "reading_count": int((row["reading_count"] if row else 0) or 0),
        "done_count": int((row["done_count"] if row else 0) or 0),
        "excluded_count": int((row["excluded_count"] if row else 0) or 0),
        "untriaged_count": int((row["untriaged_count"] if row else 0) or 0),
        "uncollected_count": uncollected_count,
    }


def _iso_day(value: str | None) -> str | None:
    raw = str(value or "").strip()
    if len(raw) >= 10:
        return raw[:10]
    return None


def _build_refresh_trend(
    operations: list[dict[str, Any]],
    *,
    primary_key: str,
    secondary_key: str,
) -> list[dict[str, Any]]:
    rows: dict[str, dict[str, Any]] = {}
    for op in operations:
        result = op.get("result") or {}
        day = _iso_day(op.get("finished_at") or op.get("updated_at") or op.get("started_at"))
        if not day or not isinstance(result, dict):
            continue
        target = rows.setdefault(
            day,
            {
                "date": day,
                "runs": 0,
                primary_key: 0,
                secondary_key: 0,
            },
        )
        target["runs"] += 1
        target[primary_key] += int(result.get(primary_key) or 0)
        target[secondary_key] += int(result.get(secondary_key) or 0)
    return [rows[day] for day in sorted(rows.keys())]


def _build_recommendation_action_trend(db: sqlite3.Connection, *, days: int = 30) -> list[dict[str, Any]]:
    if not table_exists(db, "recommendations"):
        return []
    since = (datetime.utcnow() - timedelta(days=days)).isoformat()
    rows = db.execute(
        """
        SELECT
            substr(COALESCE(action_at, created_at), 1, 10) AS day,
            COALESCE(SUM(CASE WHEN user_action = 'like' THEN 1 ELSE 0 END), 0) AS liked,
            COALESCE(SUM(CASE WHEN user_action = 'dismiss' THEN 1 ELSE 0 END), 0) AS dismissed,
            COALESCE(SUM(CASE WHEN user_action = 'save' THEN 1 ELSE 0 END), 0) AS saved,
            COALESCE(SUM(CASE WHEN user_action = 'seen' THEN 1 ELSE 0 END), 0) AS seen
        FROM recommendations
        WHERE COALESCE(action_at, created_at) >= ?
        GROUP BY substr(COALESCE(action_at, created_at), 1, 10)
        ORDER BY day ASC
        """,
        (since,),
    ).fetchall()
    return [
        {
            "date": row["day"],
            "liked": int(row["liked"] or 0),
            "dismissed": int(row["dismissed"] or 0),
            "saved": int(row["saved"] or 0),
            "seen": int(row["seen"] or 0),
        }
        for row in rows
        if row["day"]
    ]


def _compute_alert_usefulness_score(*, total_runs: int, failed_runs: int, empty_runs: int, papers_sent: int, sent_runs: int) -> int:
    reliability = 1.0 - safe_div(failed_runs, max(1, total_runs))
    non_empty_rate = 1.0 - safe_div(empty_runs, max(1, total_runs))
    volume_score = min(safe_div(papers_sent, max(1, sent_runs)) / 4.0, 1.0) if sent_runs else 0.0
    return round(((reliability * 0.45) + (non_empty_rate * 0.30) + (volume_score * 0.25)) * 100)


def _build_alert_history_trend(
    db: sqlite3.Connection,
    *,
    days: int = 30,
    bucket: str = "day",
) -> list[dict[str, Any]]:
    if not table_exists(db, "alert_history"):
        return []
    since = (datetime.utcnow() - timedelta(days=days)).isoformat()
    if bucket == "week":
        rows = db.execute(
            """
            SELECT
                MIN(substr(sent_at, 1, 10)) AS period_start,
                COALESCE(SUM(CASE WHEN status = 'sent' THEN 1 ELSE 0 END), 0) AS sent,
                COALESCE(SUM(CASE WHEN status = 'failed' THEN 1 ELSE 0 END), 0) AS failed,
                COALESCE(SUM(CASE WHEN status = 'empty' THEN 1 ELSE 0 END), 0) AS empty,
                COALESCE(SUM(CASE WHEN status = 'skipped' THEN 1 ELSE 0 END), 0) AS skipped,
                COALESCE(SUM(publication_count), 0) AS publication_count
            FROM alert_history
            WHERE sent_at >= ?
            GROUP BY strftime('%Y-%W', sent_at)
            ORDER BY period_start ASC
            """,
            (since,),
        ).fetchall()
        return [
            {
                "date": row["period_start"],
                "sent": int(row["sent"] or 0),
                "failed": int(row["failed"] or 0),
                "empty": int(row["empty"] or 0),
                "skipped": int(row["skipped"] or 0),
                "total": int(row["sent"] or 0) + int(row["failed"] or 0) + int(row["empty"] or 0) + int(row["skipped"] or 0),
                "publication_count": int(row["publication_count"] or 0),
            }
            for row in rows
            if row["period_start"]
        ]

    rows = db.execute(
        """
        SELECT
            substr(sent_at, 1, 10) AS day,
            COALESCE(SUM(CASE WHEN status = 'sent' THEN 1 ELSE 0 END), 0) AS sent,
            COALESCE(SUM(CASE WHEN status = 'failed' THEN 1 ELSE 0 END), 0) AS failed,
            COALESCE(SUM(CASE WHEN status = 'empty' THEN 1 ELSE 0 END), 0) AS empty,
            COALESCE(SUM(CASE WHEN status = 'skipped' THEN 1 ELSE 0 END), 0) AS skipped,
            COALESCE(SUM(publication_count), 0) AS publication_count
        FROM alert_history
        WHERE sent_at >= ?
        GROUP BY substr(sent_at, 1, 10)
        ORDER BY day ASC
        """,
        (since,),
    ).fetchall()
    return [
        {
            "date": row["day"],
            "sent": int(row["sent"] or 0),
            "failed": int(row["failed"] or 0),
            "empty": int(row["empty"] or 0),
            "skipped": int(row["skipped"] or 0),
            "total": int(row["sent"] or 0) + int(row["failed"] or 0) + int(row["empty"] or 0) + int(row["skipped"] or 0),
            "publication_count": int(row["publication_count"] or 0),
        }
        for row in rows
        if row["day"]
    ]


def _build_branch_trends(
    db: sqlite3.Connection,
    *,
    days: int = 30,
    limit: int = 6,
) -> list[dict[str, Any]]:
    if not table_exists(db, "recommendations"):
        return []
    since = (datetime.utcnow() - timedelta(days=days)).date().isoformat()
    rows = db.execute(
        """
        SELECT
            COALESCE(NULLIF(branch_id, ''), NULL) AS branch_id,
            COALESCE(NULLIF(branch_label, ''), 'Unnamed branch') AS branch_label,
            substr(COALESCE(action_at, created_at), 1, 10) AS day,
            COUNT(*) AS total,
            COALESCE(SUM(CASE WHEN user_action IN ('like', 'save') THEN 1 ELSE 0 END), 0) AS positive,
            COALESCE(SUM(CASE WHEN user_action = 'dismiss' THEN 1 ELSE 0 END), 0) AS dismissed
        FROM recommendations
        WHERE (COALESCE(branch_id, '') <> '' OR COALESCE(branch_label, '') <> '')
          AND substr(COALESCE(action_at, created_at), 1, 10) >= ?
        GROUP BY COALESCE(NULLIF(branch_id, ''), NULL), COALESCE(NULLIF(branch_label, ''), 'Unnamed branch'), substr(COALESCE(action_at, created_at), 1, 10)
        ORDER BY day ASC
        """,
        (since,),
    ).fetchall()
    if not rows:
        return []

    by_branch: dict[str, dict[str, Any]] = {}
    for row in rows:
        branch_key = str(row["branch_id"] or row["branch_label"] or "branch")
        item = by_branch.setdefault(
            branch_key,
            {
                "branch_id": row["branch_id"],
                "branch_label": row["branch_label"] or row["branch_id"] or "Unnamed branch",
                "daily": [],
            },
        )
        item["daily"].append(
            {
                "date": row["day"],
                "total": int(row["total"] or 0),
                "positive": int(row["positive"] or 0),
                "dismissed": int(row["dismissed"] or 0),
                "positive_rate": round(safe_div(float(row["positive"] or 0), max(1.0, float(row["total"] or 0))), 3),
            }
        )

    ranked: list[dict[str, Any]] = []
    for item in by_branch.values():
        daily = sorted(item["daily"], key=lambda point: str(point.get("date") or ""))
        recent = daily[-7:]
        prior = daily[-14:-7]
        recent_total = sum(int(point.get("total") or 0) for point in recent)
        prior_total = sum(int(point.get("total") or 0) for point in prior)
        recent_positive = sum(int(point.get("positive") or 0) for point in recent)
        prior_positive = sum(int(point.get("positive") or 0) for point in prior)
        recent_rate = round(safe_div(float(recent_positive), max(1.0, float(recent_total))), 3)
        prior_rate = round(safe_div(float(prior_positive), max(1.0, float(prior_total))), 3)
        ranked.append(
            {
                **item,
                "daily": daily[-14:],
                "recent_7d_total": recent_total,
                "prior_7d_total": prior_total,
                "recent_7d_positive_rate": recent_rate,
                "prior_7d_positive_rate": prior_rate,
                "delta_positive_rate": round(recent_rate - prior_rate, 3),
            }
        )
    ranked.sort(
        key=lambda item: (
            -int(item.get("recent_7d_total") or 0),
            -abs(float(item.get("delta_positive_rate") or 0.0)),
            str(item.get("branch_label") or "").lower(),
        )
    )
    return ranked[: max(1, int(limit or 6))]


def _build_author_follow_trend(
    db: sqlite3.Connection,
    *,
    days: int = 30,
) -> list[dict[str, Any]]:
    if not table_exists(db, "followed_authors"):
        return []
    since = (datetime.utcnow() - timedelta(days=max(1, days - 1))).date().isoformat()
    try:
        rows = db.execute(
            """
            SELECT substr(followed_at, 1, 10) AS day, COUNT(*) AS follows
            FROM followed_authors
            WHERE COALESCE(substr(followed_at, 1, 10), '') >= ?
            GROUP BY substr(followed_at, 1, 10)
            ORDER BY day ASC
            """,
            (since,),
        ).fetchall()
    except sqlite3.OperationalError:
        return []
    return [
        {"date": row["day"], "follows": int(row["follows"] or 0)}
        for row in rows
        if row["day"]
    ]


def _build_signal_lab_trend(
    db: sqlite3.Connection,
    *,
    days: int = 30,
) -> list[dict[str, Any]]:
    if not table_exists(db, "feedback_events"):
        return []
    since = (datetime.utcnow() - timedelta(days=max(1, days - 1))).isoformat()
    try:
        rows = db.execute(
            """
            SELECT
                substr(created_at, 1, 10) AS day,
                COUNT(*) AS interactions,
                COALESCE(SUM(CASE WHEN event_type = 'feed_action' THEN 1 ELSE 0 END), 0) AS feed_actions,
                COALESCE(SUM(CASE WHEN event_type = 'topic_pref' THEN 1 ELSE 0 END), 0) AS topic_tunes,
                COALESCE(SUM(CASE WHEN event_type = 'rating' THEN 1 ELSE 0 END), 0) AS ratings
            FROM feedback_events
            WHERE COALESCE(created_at, '') >= ?
            GROUP BY substr(created_at, 1, 10)
            ORDER BY day ASC
            """,
            (since,),
        ).fetchall()
    except sqlite3.OperationalError:
        return []
    return [
        {
            "date": row["day"],
            "interactions": int(row["interactions"] or 0),
            "feed_actions": int(row["feed_actions"] or 0),
            "topic_tunes": int(row["topic_tunes"] or 0),
            "ratings": int(row["ratings"] or 0),
        }
        for row in rows
        if row["day"]
    ]


def _build_alert_quality_snapshot(db: sqlite3.Connection, *, days: int = 30) -> dict[str, Any]:
    summary = {
        "total_alerts": 0,
        "enabled_alerts": 0,
        "total_rules": 0,
        "active_alerts_30d": 0,
        "sent_runs_30d": 0,
        "failed_runs_30d": 0,
        "empty_runs_30d": 0,
        "skipped_runs_30d": 0,
        "papers_sent_30d": 0,
        "avg_papers_per_sent": 0.0,
    }
    top_alerts: list[dict[str, Any]] = []

    if table_exists(db, "alerts"):
        row = db.execute(
            """
            SELECT
                COUNT(*) AS total_alerts,
                COALESCE(SUM(CASE WHEN enabled = 1 THEN 1 ELSE 0 END), 0) AS enabled_alerts
            FROM alerts
            """
        ).fetchone()
        summary["total_alerts"] = int((row["total_alerts"] if row else 0) or 0)
        summary["enabled_alerts"] = int((row["enabled_alerts"] if row else 0) or 0)

    if table_exists(db, "alert_rules"):
        row = db.execute("SELECT COUNT(*) AS c FROM alert_rules").fetchone()
        summary["total_rules"] = int((row["c"] if row else 0) or 0)

    if not table_exists(db, "alert_history"):
        return {
            "summary": summary,
            "top_alerts": top_alerts,
            "long_horizon": {"days": 90, "summary": {}, "weekly_trend": []},
        }

    since = (datetime.utcnow() - timedelta(days=days)).isoformat()
    row = db.execute(
        """
        SELECT
            COUNT(DISTINCT COALESCE(alert_id, '')) AS active_alerts,
            COALESCE(SUM(CASE WHEN status = 'sent' THEN 1 ELSE 0 END), 0) AS sent_runs,
            COALESCE(SUM(CASE WHEN status = 'failed' THEN 1 ELSE 0 END), 0) AS failed_runs,
            COALESCE(SUM(CASE WHEN status = 'empty' THEN 1 ELSE 0 END), 0) AS empty_runs,
            COALESCE(SUM(CASE WHEN status = 'skipped' THEN 1 ELSE 0 END), 0) AS skipped_runs,
            COALESCE(SUM(CASE WHEN status = 'sent' THEN publication_count ELSE 0 END), 0) AS papers_sent
        FROM alert_history
        WHERE sent_at >= ?
        """,
        (since,),
    ).fetchone()
    sent_runs = int((row["sent_runs"] if row else 0) or 0)
    papers_sent = int((row["papers_sent"] if row else 0) or 0)
    summary.update(
        {
            "active_alerts_30d": int((row["active_alerts"] if row else 0) or 0),
            "sent_runs_30d": sent_runs,
            "failed_runs_30d": int((row["failed_runs"] if row else 0) or 0),
            "empty_runs_30d": int((row["empty_runs"] if row else 0) or 0),
            "skipped_runs_30d": int((row["skipped_runs"] if row else 0) or 0),
            "papers_sent_30d": papers_sent,
            "avg_papers_per_sent": round(safe_div(papers_sent, max(1, sent_runs)), 2) if sent_runs else 0.0,
        }
    )

    rows = db.execute(
        """
        SELECT
            COALESCE(ah.alert_id, '') AS alert_id,
            COALESCE(a.name, 'Unassigned alert') AS alert_name,
            COUNT(*) AS total_runs,
            COALESCE(SUM(CASE WHEN ah.status = 'sent' THEN 1 ELSE 0 END), 0) AS sent_runs,
            COALESCE(SUM(CASE WHEN ah.status = 'failed' THEN 1 ELSE 0 END), 0) AS failed_runs,
            COALESCE(SUM(CASE WHEN ah.status = 'empty' THEN 1 ELSE 0 END), 0) AS empty_runs,
            COALESCE(SUM(CASE WHEN ah.status = 'skipped' THEN 1 ELSE 0 END), 0) AS skipped_runs,
            COALESCE(SUM(CASE WHEN ah.status = 'sent' THEN ah.publication_count ELSE 0 END), 0) AS papers_sent
        FROM alert_history ah
        LEFT JOIN alerts a ON a.id = ah.alert_id
        WHERE ah.sent_at >= ?
        GROUP BY COALESCE(ah.alert_id, ''), COALESCE(a.name, 'Unassigned alert')
        ORDER BY sent_runs DESC, papers_sent DESC, total_runs DESC
        LIMIT 6
        """,
        (since,),
    ).fetchall()
    for row in rows:
        total_runs = int(row["total_runs"] or 0)
        sent_runs = int(row["sent_runs"] or 0)
        failed_runs = int(row["failed_runs"] or 0)
        empty_runs = int(row["empty_runs"] or 0)
        papers_sent = int(row["papers_sent"] or 0)
        reliability = 1.0 - safe_div(failed_runs, max(1, total_runs))
        non_empty_rate = 1.0 - safe_div(empty_runs, max(1, total_runs))
        volume_score = min(safe_div(papers_sent, max(1, sent_runs)) / 4.0, 1.0) if sent_runs else 0.0
        usefulness_score = round(((reliability * 0.45) + (non_empty_rate * 0.30) + (volume_score * 0.25)) * 100)
        top_alerts.append(
            {
                "alert_id": row["alert_id"] or None,
                "alert_name": row["alert_name"],
                "total_runs": total_runs,
                "sent_runs": sent_runs,
                "failed_runs": failed_runs,
                "empty_runs": empty_runs,
                "skipped_runs": int(row["skipped_runs"] or 0),
                "papers_sent": papers_sent,
                "usefulness_score": usefulness_score,
            }
        )
    long_horizon_days = max(days, 90)
    long_since = (datetime.utcnow() - timedelta(days=long_horizon_days)).isoformat()
    long_row = db.execute(
        """
        SELECT
            COUNT(DISTINCT COALESCE(alert_id, '')) AS active_alerts,
            COALESCE(SUM(CASE WHEN status = 'sent' THEN 1 ELSE 0 END), 0) AS sent_runs,
            COALESCE(SUM(CASE WHEN status = 'failed' THEN 1 ELSE 0 END), 0) AS failed_runs,
            COALESCE(SUM(CASE WHEN status = 'empty' THEN 1 ELSE 0 END), 0) AS empty_runs,
            COALESCE(SUM(CASE WHEN status = 'skipped' THEN 1 ELSE 0 END), 0) AS skipped_runs,
            COALESCE(SUM(CASE WHEN status = 'sent' THEN publication_count ELSE 0 END), 0) AS papers_sent
        FROM alert_history
        WHERE sent_at >= ?
        """,
        (long_since,),
    ).fetchone()
    long_total_runs = (
        int((long_row["sent_runs"] if long_row else 0) or 0)
        + int((long_row["failed_runs"] if long_row else 0) or 0)
        + int((long_row["empty_runs"] if long_row else 0) or 0)
        + int((long_row["skipped_runs"] if long_row else 0) or 0)
    )
    long_sent_runs = int((long_row["sent_runs"] if long_row else 0) or 0)
    long_failed_runs = int((long_row["failed_runs"] if long_row else 0) or 0)
    long_empty_runs = int((long_row["empty_runs"] if long_row else 0) or 0)
    long_papers_sent = int((long_row["papers_sent"] if long_row else 0) or 0)
    long_usefulness = _compute_alert_usefulness_score(
        total_runs=long_total_runs,
        failed_runs=long_failed_runs,
        empty_runs=long_empty_runs,
        papers_sent=long_papers_sent,
        sent_runs=long_sent_runs,
    )
    recent_total_runs = (
        int(summary["sent_runs_30d"])
        + int(summary["failed_runs_30d"])
        + int(summary["empty_runs_30d"])
        + int(summary["skipped_runs_30d"])
    )
    recent_usefulness = _compute_alert_usefulness_score(
        total_runs=recent_total_runs,
        failed_runs=int(summary["failed_runs_30d"]),
        empty_runs=int(summary["empty_runs_30d"]),
        papers_sent=int(summary["papers_sent_30d"]),
        sent_runs=int(summary["sent_runs_30d"]),
    )
    return {
        "summary": summary,
        "top_alerts": top_alerts,
        "long_horizon": {
            "days": long_horizon_days,
            "summary": {
                "active_alerts": int((long_row["active_alerts"] if long_row else 0) or 0),
                "sent_runs": long_sent_runs,
                "failed_runs": long_failed_runs,
                "empty_runs": long_empty_runs,
                "skipped_runs": int((long_row["skipped_runs"] if long_row else 0) or 0),
                "papers_sent": long_papers_sent,
                "usefulness_score": long_usefulness,
                "recent_30d_usefulness_score": recent_usefulness,
                "delta_vs_30d": long_usefulness - recent_usefulness,
            },
            "weekly_trend": _build_alert_history_trend(db, days=long_horizon_days, bucket="week"),
        },
    }


def _build_cold_start_topic_validation(db: sqlite3.Connection, *, limit: int = 12) -> dict[str, Any]:
    if not (table_exists(db, "suggestion_sets") and table_exists(db, "discovery_lenses")):
        return {"total_runs": 0, "validated_runs": 0, "state_counts": {}, "recent": []}
    try:
        rows = db.execute(
            """
            SELECT ss.created_at, ss.retrieval_summary, l.id AS lens_id, l.name AS lens_name
            FROM suggestion_sets ss
            JOIN discovery_lenses l ON l.id = ss.lens_id
            WHERE l.context_type = 'topic_keyword'
            ORDER BY ss.created_at DESC
            LIMIT ?
            """,
            (max(1, int(limit or 12)),),
        ).fetchall()
    except sqlite3.OperationalError:
        return {"total_runs": 0, "validated_runs": 0, "state_counts": {}, "recent": []}

    state_counts: dict[str, int] = {}
    recent: list[dict[str, Any]] = []
    validated_runs = 0
    for row in rows:
        retrieval_summary = json_loads(row["retrieval_summary"])
        cold_start = retrieval_summary.get("cold_start") if isinstance(retrieval_summary, dict) else None
        if not isinstance(cold_start, dict):
            continue
        state = str(cold_start.get("state") or "unknown")
        state_counts[state] = int(state_counts.get(state) or 0) + 1
        if state in {"validated", "partial"}:
            validated_runs += 1
        recent.append(
            {
                "lens_id": row["lens_id"],
                "lens_name": row["lens_name"],
                "created_at": row["created_at"],
                "state": state,
                "seed_count": int(cold_start.get("seed_count") or 0),
                "external_results": int(cold_start.get("external_results") or 0),
                "query": cold_start.get("query"),
            }
        )
    return {
        "total_runs": len(recent),
        "validated_runs": validated_runs,
        "state_counts": state_counts,
        "recent": recent,
    }


class BranchTuningActionRequest(BaseModel):
    branch_id: str
    action: str


def _build_authors_snapshot(db: sqlite3.Connection, monitors: list[dict[str, Any]]) -> dict[str, Any]:
    summary = {
        "total_rows": 0,
        "tracked_authors": 0,
        "provenance_only_authors": 0,
        "ready_tracked": 0,
        "degraded_tracked": 0,
        "disabled_tracked": 0,
        "bridge_gap_count": 0,
        "background_corpus_papers": 0,
        "fresh_backfills": 0,
        "running_backfills": 0,
        "pending_backfills": 0,
        "stale_backfills": 0,
        "thin_backfills": 0,
    }
    degraded: list[dict[str, Any]] = []
    suggestions: list[dict[str, Any]] = []
    corpus_health: list[dict[str, Any]] = []

    if table_exists(db, "authors"):
        row = db.execute("SELECT COUNT(*) AS c FROM authors").fetchone()
        summary["total_rows"] = int((row["c"] if row else 0) or 0)
    if table_exists(db, "followed_authors"):
        row = db.execute("SELECT COUNT(*) AS c FROM followed_authors").fetchone()
        summary["tracked_authors"] = int((row["c"] if row else 0) or 0)
    summary["provenance_only_authors"] = max(0, summary["total_rows"] - summary["tracked_authors"])

    author_monitors = [monitor for monitor in monitors if str(monitor.get("monitor_type") or "") == "author"]
    summary["ready_tracked"] = sum(1 for monitor in author_monitors if monitor.get("health") == "ready")
    summary["degraded_tracked"] = sum(1 for monitor in author_monitors if monitor.get("health") == "degraded")
    summary["disabled_tracked"] = sum(1 for monitor in author_monitors if monitor.get("health") == "disabled")
    summary["bridge_gap_count"] = sum(
        1
        for monitor in author_monitors
        if str(monitor.get("health_reason") or "") == "missing_openalex_id_for_scholar_monitor"
    )
    if table_exists(db, "followed_authors") and table_exists(db, "publication_authors"):
        try:
            row = db.execute(
                """
                SELECT COUNT(DISTINCT p.id) AS c
                FROM papers p
                JOIN publication_authors pa ON pa.paper_id = p.id
                JOIN authors a ON lower(trim(a.openalex_id)) = lower(trim(pa.openalex_id))
                JOIN followed_authors fa ON fa.author_id = a.id
                WHERE p.status <> 'library'
                """
            ).fetchone()
            summary["background_corpus_papers"] = int((row["c"] if row else 0) or 0)
        except sqlite3.OperationalError:
            summary["background_corpus_papers"] = 0
    degraded = [
        {
            "author_id": monitor.get("author_id"),
            "author_name": monitor.get("author_name") or monitor.get("label"),
            "health_reason": monitor.get("health_reason"),
            "last_error": monitor.get("last_error"),
            "last_checked_at": monitor.get("last_checked_at"),
        }
        for monitor in author_monitors
        if monitor.get("health") == "degraded"
    ][:6]

    try:
        from alma.application import authors as authors_app

        raw_suggestions = authors_app.list_author_suggestions(db, limit=6)
        suggestions = [
            {
                "key": item.get("key"),
                "name": item.get("name"),
                "suggestion_type": item.get("suggestion_type"),
                "score": round(float(item.get("score") or 0.0), 3),
                "shared_followed_count": int(item.get("shared_followed_count") or 0),
                "negative_signal": round(float(item.get("negative_signal") or 0.0), 3),
            }
            for item in raw_suggestions
        ]
    except Exception:
        suggestions = []

    if table_exists(db, "followed_authors") and table_exists(db, "authors"):
        try:
            followed_rows = db.execute(
                """
                SELECT a.id, a.name, COALESCE(a.works_count, 0) AS works_count
                FROM authors a
                JOIN followed_authors fa ON fa.author_id = a.id
                ORDER BY a.name
                """
            ).fetchall()
        except sqlite3.OperationalError:
            followed_rows = []
        for row in followed_rows:
            author_id = str(row["id"] or "").strip()
            if not author_id:
                continue
            status = get_followed_author_backfill_status(
                db,
                author_id,
                works_count=int(row["works_count"] or 0),
            )
            state = str(status.get("state") or "unknown")
            if state == "fresh":
                summary["fresh_backfills"] += 1
            elif state == "running":
                summary["running_backfills"] += 1
            elif state == "pending":
                summary["pending_backfills"] += 1
            elif state == "stale":
                summary["stale_backfills"] += 1
            elif state == "thin":
                summary["thin_backfills"] += 1
            if state in {"stale", "thin", "pending", "failed", "unverified", "running"}:
                corpus_health.append(
                    {
                        "author_id": author_id,
                        "author_name": str(row["name"] or "").strip() or author_id,
                        "state": state,
                        "detail": str(status.get("detail") or "").strip() or None,
                        "background_publications": int(status.get("background_publications") or 0),
                        "coverage_ratio": status.get("coverage_ratio"),
                        "last_success_at": status.get("last_success_at"),
                    }
                )

    return {
        "summary": summary,
        "degraded": degraded,
        "suggestions": suggestions,
        "corpus_health": corpus_health[:8],
    }


def _build_signal_lab_snapshot(db: sqlite3.Connection) -> dict[str, Any]:
    summary = {
        "total_interactions": 0,
        "week_interactions": 0,
        "streak_days": 0,
        "topic_coverage": 0,
        "source_diversity_7d": 0,
        "recommendation_engagement_rate": 0.0,
        "xp": 0,
        "level": 1,
    }
    top_topics: list[dict[str, Any]] = []
    top_authors: list[dict[str, Any]] = []
    next_actions: list[str] = []

    try:
        from alma.services.signal_lab import compute_signal_stats, get_signal_results_summary

        stats = compute_signal_stats(db)
        results = get_signal_results_summary(db, days=14)
        behavioral = stats.get("behavioral_metrics") or {}
        outcomes = results.get("recommendation_outcomes") or {}
        summary.update(
            {
                "total_interactions": int(stats.get("total_interactions") or 0),
                "week_interactions": int(stats.get("week_interactions") or 0),
                "streak_days": int(stats.get("streak_days") or 0),
                "topic_coverage": int(stats.get("topic_coverage") or 0),
                "source_diversity_7d": int(behavioral.get("source_diversity_7d") or 0),
                "recommendation_engagement_rate": round(float(outcomes.get("engagement_rate") or 0.0), 3),
                "xp": int(stats.get("xp") or 0),
                "level": int(stats.get("level") or 1),
                "background_corpus_papers": int(stats.get("background_corpus_papers") or 0),
                "background_corpus_authors": int(stats.get("background_corpus_authors") or 0),
            }
        )
        top_topics = list(stats.get("top_topics") or [])[:5]
        top_authors = list(stats.get("top_authors") or [])[:5]
        next_actions = [str(item) for item in (results.get("next_actions") or [])[:4] if str(item).strip()]
    except Exception:
        pass

    return {
        "summary": summary,
        "top_topics": top_topics,
        "top_authors": top_authors,
        "next_actions": next_actions,
    }


def _build_ai_snapshot(
    db: sqlite3.Connection,
    *,
    discovery_settings: dict[str, str],
) -> dict[str, Any]:
    summary = {
        "total_papers": 0,
        "embeddings_ready": False,
        "embedding_provider": "none",
        "embedding_model": str(
            discovery_settings.get("embedding_model")
            or DISCOVERY_SETTINGS_DEFAULTS["embedding_model"]
        ),
        "dominant_embedding_dimension": 0,
        "embedding_dimension_variants": 0,
        "embedding_coverage_pct": 0.0,
        "missing_embeddings": 0,
        "stale_embeddings": 0,
        "up_to_date_embeddings": 0,
        "recent_recommendations_analyzed": 0,
        "hybrid_text_rate": 0.0,
        "semantic_only_rate": 0.0,
        "lexical_only_rate": 0.0,
        "embedding_candidate_ready_rate": 0.0,
        "low_similarity_rate": 0.0,
        "compressed_similarity_rate": 0.0,
        "avg_text_similarity": 0.0,
        "avg_semantic_raw": 0.0,
        "avg_semantic_support_raw": 0.0,
        "avg_lexical_raw": 0.0,
        "avg_lexical_term_raw": 0.0,
    }
    mode_breakdown = {"hybrid": 0, "semantic": 0, "lexical": 0, "none": 0}
    recommendations: list[dict[str, Any]] = []
    capabilities: list[dict[str, Any]] = []

    try:
        from alma.ai.providers import get_active_provider

        provider = get_active_provider(db)
        summary["embeddings_ready"] = provider is not None
        if provider is not None:
            summary["embedding_provider"] = str(getattr(provider, "name", None) or provider.__class__.__name__).lower()
    except Exception:
        summary["embeddings_ready"] = False

    total_papers = 0
    if table_exists(db, "papers"):
        try:
            row = db.execute("SELECT COUNT(*) AS c FROM papers").fetchone()
            total_papers = int((row["c"] if row else 0) or 0)
        except sqlite3.OperationalError:
            total_papers = 0
    summary["total_papers"] = total_papers

    if total_papers > 0 and table_exists(db, "publication_embeddings"):
        try:
            row = db.execute(
                """
                SELECT
                    SUM(CASE WHEN active_pe.paper_id IS NULL THEN 1 ELSE 0 END) AS missing_embeddings,
                    SUM(
                        CASE
                            WHEN active_pe.paper_id IS NULL
                             AND EXISTS (
                                SELECT 1 FROM publication_embeddings other_pe
                                WHERE other_pe.paper_id = p.id AND other_pe.model <> ?
                             )
                            THEN 1 ELSE 0
                        END
                    ) AS stale_embeddings,
                    SUM(CASE WHEN active_pe.paper_id IS NOT NULL THEN 1 ELSE 0 END) AS up_to_date_embeddings
                FROM papers p
                LEFT JOIN publication_embeddings active_pe
                  ON active_pe.paper_id = p.id AND active_pe.model = ?
                """,
                (summary["embedding_model"], summary["embedding_model"]),
            ).fetchone()
            summary["missing_embeddings"] = int((row["missing_embeddings"] if row else 0) or 0)
            summary["stale_embeddings"] = int((row["stale_embeddings"] if row else 0) or 0)
            summary["up_to_date_embeddings"] = int((row["up_to_date_embeddings"] if row else 0) or 0)
            summary["embedding_coverage_pct"] = round(
                (summary["up_to_date_embeddings"] / float(total_papers)) * 100.0,
                1,
            )
            if summary["up_to_date_embeddings"] > 0:
                summary["embeddings_ready"] = True
        except sqlite3.OperationalError:
            pass
        try:
            dim_rows = db.execute(
                """
                SELECT CAST(LENGTH(embedding) / 4 AS INTEGER) AS embedding_dim, COUNT(*) AS count
                FROM publication_embeddings
                WHERE embedding IS NOT NULL AND model = ?
                GROUP BY CAST(LENGTH(embedding) / 4 AS INTEGER)
                ORDER BY count DESC, embedding_dim DESC
                """,
                (summary["embedding_model"],),
            ).fetchall()
            if dim_rows:
                summary["dominant_embedding_dimension"] = int(dim_rows[0]["embedding_dim"] or 0)
                summary["embedding_dimension_variants"] = len(dim_rows)
        except sqlite3.OperationalError:
            pass

    if table_exists(db, "recommendations"):
        try:
            rows = db.execute(
                """
                SELECT score_breakdown
                FROM recommendations
                WHERE score_breakdown IS NOT NULL AND COALESCE(score_breakdown, '') <> ''
                ORDER BY created_at DESC
                LIMIT 500
                """
            ).fetchall()
        except sqlite3.OperationalError:
            rows = []
        text_values: list[float] = []
        semantic_raw_values: list[float] = []
        semantic_support_values: list[float] = []
        lexical_raw_values: list[float] = []
        lexical_term_values: list[float] = []
        candidate_embedding_ready = 0
        low_similarity = 0
        compressed_similarity = 0
        for row in rows:
            breakdown = json_loads(row["score_breakdown"])
            if not isinstance(breakdown, dict):
                continue
            summary["recent_recommendations_analyzed"] += 1
            mode = str(breakdown.get("text_similarity_mode") or "none")
            if mode not in mode_breakdown:
                mode = "none"
            mode_breakdown[mode] += 1
            if bool(breakdown.get("candidate_embedding_ready")):
                candidate_embedding_ready += 1
            try:
                text_value = float(((breakdown.get("text_similarity") or {}).get("value")) or 0.0)
                text_values.append(text_value)
                if text_value < 0.24:
                    low_similarity += 1
            except Exception:
                pass
            try:
                semantic_raw = float(breakdown.get("semantic_similarity_raw") or 0.0)
                semantic_raw_values.append(semantic_raw)
                if semantic_raw > 0.0 and semantic_raw < 0.14:
                    compressed_similarity += 1
            except Exception:
                pass
            try:
                lexical_raw_values.append(float(breakdown.get("lexical_similarity_raw") or 0.0))
            except Exception:
                pass
            try:
                semantic_support_values.append(float(breakdown.get("semantic_similarity_support_raw") or 0.0))
            except Exception:
                pass
            try:
                lexical_term_values.append(float(breakdown.get("lexical_similarity_term_raw") or 0.0))
            except Exception:
                pass
        analyzed = max(1, summary["recent_recommendations_analyzed"])
        summary["hybrid_text_rate"] = round(mode_breakdown["hybrid"] / float(analyzed), 3) if summary["recent_recommendations_analyzed"] else 0.0
        summary["semantic_only_rate"] = round(mode_breakdown["semantic"] / float(analyzed), 3) if summary["recent_recommendations_analyzed"] else 0.0
        summary["lexical_only_rate"] = round(mode_breakdown["lexical"] / float(analyzed), 3) if summary["recent_recommendations_analyzed"] else 0.0
        summary["embedding_candidate_ready_rate"] = round(candidate_embedding_ready / float(analyzed), 3) if summary["recent_recommendations_analyzed"] else 0.0
        summary["low_similarity_rate"] = round(low_similarity / float(analyzed), 3) if summary["recent_recommendations_analyzed"] else 0.0
        summary["compressed_similarity_rate"] = round(compressed_similarity / float(analyzed), 3) if summary["recent_recommendations_analyzed"] else 0.0
        summary["avg_text_similarity"] = round(sum(text_values) / max(1, len(text_values)), 3) if text_values else 0.0
        summary["avg_semantic_raw"] = round(sum(semantic_raw_values) / max(1, len(semantic_raw_values)), 4) if semantic_raw_values else 0.0
        summary["avg_semantic_support_raw"] = round(sum(semantic_support_values) / max(1, len(semantic_support_values)), 4) if semantic_support_values else 0.0
        summary["avg_lexical_raw"] = round(sum(lexical_raw_values) / max(1, len(lexical_raw_values)), 4) if lexical_raw_values else 0.0
        summary["avg_lexical_term_raw"] = round(sum(lexical_term_values) / max(1, len(lexical_term_values)), 4) if lexical_term_values else 0.0

    # LLM-backed capabilities (query planner, recommendation explanations,
    # signal-lab coaching, narrative reports) were removed in 2026-04 (see
    # tasks/01_LLM_PRODUCTION_EXIT.md). The capabilities list is now empty
    # — kept in the response for shape compatibility with the frontend.
    capabilities = []

    if not summary["embeddings_ready"]:
        recommendations.append(
            {
                "id": "enable_embeddings",
                "label": "Enable embeddings",
                "detail": "Vector retrieval and semantic similarity are currently unavailable.",
                "severity": "critical",
            }
        )
    elif summary["embedding_coverage_pct"] < 55:
        recommendations.append(
            {
                "id": "increase_embedding_coverage",
                "label": "Increase embedding coverage",
                "detail": f"Only {summary['embedding_coverage_pct']}% of papers have up-to-date embeddings.",
                "severity": "warning",
            }
        )
    if int(summary["stale_embeddings"] or 0) > 0:
        recommendations.append(
            {
                "id": "refresh_stale_embeddings",
                "label": "Refresh stale embeddings",
                "detail": f"{int(summary['stale_embeddings'])} papers still use an old embedding model.",
                "severity": "warning",
            }
        )
    if summary["recent_recommendations_analyzed"] >= 20 and summary["avg_semantic_raw"] < 0.08 and summary["avg_text_similarity"] < 0.24:
        recommendations.append(
            {
                "id": "tune_similarity_representation",
                "label": "Tune text similarity representation",
                "detail": "Recent recommendation pairs still show compressed semantic and hybrid text similarity, suggesting weak section coverage or sparse embeddings.",
                "severity": "warning",
            }
        )
    if summary["recent_recommendations_analyzed"] >= 20 and float(summary["compressed_similarity_rate"] or 0.0) >= 0.45:
        recommendations.append(
            {
                "id": "recompute_similarity_inputs",
                "label": "Recompute stale vectors and refresh similarity cache",
                "detail": "A large share of recent recommendation pairs still fall into compressed semantic ranges. Recompute stale embeddings and invalidate cached similarity results.",
                "severity": "warning",
            }
        )

    return {
        "summary": summary,
        "mode_breakdown": mode_breakdown,
        "capabilities": capabilities,
        "recommendations": recommendations,
    }


def _build_operational_snapshot(
    db: sqlite3.Connection,
    *,
    monitors: list[dict[str, Any]],
    discovery_settings: dict[str, str],
    alert_snapshot: dict[str, Any],
    authors_snapshot: dict[str, Any],
    ai_snapshot: dict[str, Any],
) -> dict[str, Any]:
    states: list[dict[str, Any]] = []
    plugins: list[dict[str, Any]] = []
    author_summary = authors_snapshot.get("summary") or {}
    degraded_authors = authors_snapshot.get("degraded") or []
    corpus_health = authors_snapshot.get("corpus_health") or []

    degraded_monitor_count = sum(1 for monitor in monitors if monitor.get("health") == "degraded")
    disabled_sources = [
        source
        for source in ("openalex", "semantic_scholar", "crossref", "arxiv", "biorxiv")
        if not _bool_setting(discovery_settings.get(f"sources.{source}.enabled"), True)
    ]

    settings = {}
    try:
        from alma.config import get_all_settings

        settings = get_all_settings() or {}
    except Exception:
        settings = {}

    slack_configured = False
    try:
        from alma.core.secrets import SECRET_SLACK_BOT_TOKEN, get_secret

        slack_configured = bool(str(get_secret(SECRET_SLACK_BOT_TOKEN) or "").strip()) and bool(
            str(settings.get("slack_channel") or "").strip()
        )
    except Exception:
        slack_configured = bool(str(settings.get("slack_channel") or "").strip())

    embeddings_ready = False
    ai_summary = ai_snapshot.get("summary") or {}
    try:
        from alma.ai.providers import get_active_provider

        embeddings_ready = get_active_provider(db) is not None
    except Exception:
        embeddings_ready = False

    unhealthy_plugins = 0
    try:
        from alma.api.deps import get_plugin_registry
        from alma.plugins.config import load_plugin_config

        registry = get_plugin_registry()
        for plugin_meta in registry.get_all_plugins_info():
            name = str(plugin_meta.get("name") or "")
            is_configured = False
            is_healthy = None
            try:
                config = load_plugin_config(name)
                is_configured = bool(config)
                if is_configured:
                    instance = registry.get_instance(name)
                    if instance:
                        health = instance.get_health_status()
                        is_healthy = bool(health.get("healthy")) if health.get("healthy") is not None else None
            except Exception:
                is_configured = False
                is_healthy = None
            if is_configured and is_healthy is False:
                unhealthy_plugins += 1
            plugins.append(
                {
                    "name": name,
                    "display_name": plugin_meta.get("display_name") or name,
                    "is_configured": is_configured,
                    "is_healthy": is_healthy,
                }
            )
    except Exception:
        plugins = []

    recent_failed_operations_24h = 0
    if table_exists(db, "operation_status"):
        cutoff = (datetime.utcnow() - timedelta(hours=24)).isoformat()
        row = db.execute(
            """
            SELECT COUNT(*) AS c
            FROM operation_status
            WHERE status = 'failed'
              AND COALESCE(finished_at, updated_at, started_at) >= ?
            """,
            (cutoff,),
        ).fetchone()
        recent_failed_operations_24h = int((row["c"] if row else 0) or 0)

    if degraded_monitor_count > 0:
        degraded_targets = [
            {
                "id": str(monitor.get("id") or ""),
                "label": str(monitor.get("label") or monitor.get("author_name") or monitor.get("id") or "").strip() or "Monitor",
                "kind": "monitor",
                "action": "refresh_monitor",
                "monitor_id": str(monitor.get("id") or ""),
            }
            for monitor in monitors
            if monitor.get("health") == "degraded"
        ][:3]
        states.append(
            {
                "id": "degraded_monitors",
                "label": "Feed monitors are degraded",
                "severity": "warning",
                "detail": f"{degraded_monitor_count} monitors currently degrade intake coverage.",
                "page": "authors",
                "params": {"followed": "true"},
                "targets": degraded_targets,
            }
        )
    if int(author_summary.get("bridge_gap_count") or 0) > 0:
        author_targets = [
            {
                "id": str(author.get("author_id") or ""),
                "label": str(author.get("author_name") or author.get("author_id") or "").strip() or "Tracked author",
                "kind": "author",
                "action": "repair_author",
                "author_id": str(author.get("author_id") or ""),
            }
            for author in degraded_authors
            if author.get("author_id")
        ][:3]
        states.append(
            {
                "id": "author_bridge_gaps",
                "label": "Some tracked authors are missing OpenAlex bridges",
                "severity": "warning",
                "detail": f"{int(author_summary.get('bridge_gap_count') or 0)} tracked authors still cannot refresh cleanly.",
                "page": "authors",
                "params": {"followed": "true"},
                "targets": author_targets,
            }
        )
    stale_backfill_targets = [
        {
            "id": str(author.get("author_id") or ""),
            "label": str(author.get("author_name") or author.get("author_id") or "").strip() or "Tracked author",
            "kind": "author",
            "action": "backfill_author",
            "author_id": str(author.get("author_id") or ""),
        }
        for author in corpus_health
        if str(author.get("author_id") or "").strip()
        and str(author.get("state") or "") in {"stale", "thin", "pending", "failed", "unverified"}
    ][:3]
    if stale_backfill_targets:
        states.append(
            {
                "id": "followed_author_corpus_stale",
                "label": "Some followed-author historical corpora need maintenance",
                "severity": "warning",
                "detail": f"{len(stale_backfill_targets)} tracked authors have stale, thin, or missing historical backfills.",
                "page": "authors",
                "params": {"followed": "true"},
                "targets": stale_backfill_targets,
            }
        )
    if not embeddings_ready:
        states.append(
            {
                "id": "embeddings_not_ready",
                "label": "Embeddings are not ready",
                "severity": "critical",
                "detail": "Vector retrieval and semantic ranking are currently degraded.",
                "page": "settings",
                "params": {"section": "ai"},
                "targets": [
                    {
                        "id": "compute_all_embeddings",
                        "label": "all embeddings",
                        "kind": "ai",
                        "action": "compute_embeddings",
                    }
                ],
            }
        )
    elif int(ai_summary.get("stale_embeddings") or 0) > 0:
        states.append(
            {
                "id": "stale_embeddings",
                "label": "Some embeddings are stale",
                "severity": "warning",
                "detail": f"{int(ai_summary.get('stale_embeddings') or 0)} papers still use an old embedding model.",
                "page": "settings",
                "params": {"section": "ai"},
                "targets": [
                    {
                        "id": "compute_stale_embeddings",
                        "label": "stale embeddings",
                        "kind": "ai",
                        "action": "compute_stale_embeddings",
                    }
                ],
            }
        )
    if float(ai_summary.get("compressed_similarity_rate") or 0.0) >= 0.45:
        states.append(
            {
                "id": "similarity_compression",
                "label": "Similarity scores look compressed",
                "severity": "warning",
                "detail": "Recent recommendation pairs are still clustering into low semantic ranges. Refresh stale vectors and clear cached similarity results.",
                "page": "settings",
                "params": {"section": "ai"},
                "targets": [
                    {
                        "id": "compute_stale_embeddings",
                        "label": "stale embeddings",
                        "kind": "ai",
                        "action": "compute_stale_embeddings",
                    },
                    {
                        "id": "clear_similarity_cache",
                        "label": "similarity cache",
                        "kind": "ai",
                        "action": "clear_similarity_cache",
                    },
                ],
            }
        )
    alert_summary = alert_snapshot.get("summary") or {}
    alert_top = alert_snapshot.get("top_alerts") or []

    if int(alert_summary.get("enabled_alerts") or 0) > 0 and not slack_configured:
        states.append(
            {
                "id": "slack_unconfigured",
                "label": "Alerts are enabled but Slack is not configured",
                "severity": "critical",
                "detail": "Delivery is degraded because the default Slack channel or token is missing.",
                "page": "settings",
                "params": {"section": "channels"},
            }
        )
    if unhealthy_plugins > 0:
        plugin_targets = [
            {
                "id": plugin["name"],
                "label": plugin["display_name"],
                "kind": "plugin",
                "action": "test_plugin",
                "plugin_name": plugin["name"],
            }
            for plugin in plugins
            if plugin.get("is_configured") and plugin.get("is_healthy") is False
        ][:3]
        states.append(
            {
                "id": "unhealthy_plugins",
                "label": "One or more plugins are unhealthy",
                "severity": "warning",
                "detail": f"{unhealthy_plugins} configured plugins reported unhealthy status.",
                "page": "alerts",
                "params": {"section": "alerts"},
                "targets": plugin_targets,
            }
        )
    failed_alert_targets = [
        {
            "id": str(item.get("alert_id") or item.get("alert_name") or ""),
            "label": str(item.get("alert_name") or "Alert"),
            "kind": "alert",
            "action": "evaluate_alert",
            "alert_id": str(item.get("alert_id") or ""),
        }
        for item in alert_top
        if str(item.get("alert_id") or "").strip() and int(item.get("failed_runs") or 0) > 0
    ][:3]
    if failed_alert_targets:
        states.append(
            {
                "id": "failed_alert_delivery",
                "label": "Some alerts have recent delivery failures",
                "severity": "warning",
                "detail": f"{len(failed_alert_targets)} alerts show recent failed runs and should be re-evaluated or repaired.",
                "page": "alerts",
                "params": {"section": "history"},
                "targets": failed_alert_targets,
            }
        )
    if disabled_sources:
        states.append(
            {
                "id": "sources_disabled",
                "label": "Some Discovery sources are disabled",
                "severity": "info",
                "detail": f"Disabled sources: {', '.join(disabled_sources)}.",
                "page": "settings",
                "params": {"section": "discovery"},
                "targets": [
                    {
                        "id": source,
                        "label": source,
                        "kind": "source",
                        "action": "enable_source",
                        "source": source,
                    }
                    for source in disabled_sources
                ],
            }
        )
    if recent_failed_operations_24h > 0:
        states.append(
            {
                "id": "recent_failed_operations",
                "label": "Recent background operations failed",
                "severity": "warning",
                "detail": f"{recent_failed_operations_24h} operations failed in the last 24 hours.",
                "page": "insights",
                "params": {"tab": "diagnostics"},
            }
        )

    critical_count = sum(1 for state in states if state["severity"] == "critical")
    warning_count = sum(1 for state in states if state["severity"] == "warning")
    healthy_checks = sum(
        1
        for ok in (
            degraded_monitor_count == 0,
            int(author_summary.get("bridge_gap_count") or 0) == 0,
            embeddings_ready,
            int(ai_summary.get("stale_embeddings") or 0) == 0,
            slack_configured or int(alert_summary.get("enabled_alerts") or 0) == 0,
            unhealthy_plugins == 0,
            recent_failed_operations_24h == 0,
        )
        if ok
    )

    return {
        "summary": {
            "issues_total": len(states),
            "critical_count": critical_count,
            "warning_count": warning_count,
            "healthy_checks": healthy_checks,
            "embeddings_ready": embeddings_ready,
            "slack_configured": slack_configured,
            "degraded_monitors": degraded_monitor_count,
            "disabled_sources": len(disabled_sources),
            "unhealthy_plugins": unhealthy_plugins,
            "recent_failed_operations_24h": recent_failed_operations_24h,
        },
        "states": states,
        "plugins": plugins,
        "disabled_sources": disabled_sources,
    }


@router.get(
    "",
    summary="Get insights data",
    description="Returns aggregated statistics for the Insights dashboard.",
)
def get_insights(
    db: sqlite3.Connection = Depends(get_db),
    user: dict = Depends(get_current_user),
):
    """Library-scoped analytics for the Insights page.

    Intent: Insights answers "what does my curated Library look like?"
    Every paper-derived aggregate filters `papers.status = 'library'` so
    tracked / removed / dismissed corpus rows the user never saved can't
    skew the charts (see P1.6 in tasks/STATUS.md, and D5/D7 in
    tasks/08_PRODUCT_DECISIONS.md — curation primitives are Library-only).
    Corpus-wide diagnostics live in the Corpus Explorer (D1); AI-readiness
    panels live in the AI snapshot / Settings — not here.

    Intentionally unscoped:
      - `recommendations`: counts Discovery-engine outcomes (seen / liked /
        dismissed recs). It's a Discovery-layer insight, not curation, so
        it aggregates across all recommendations regardless of paper
        membership.
    """
    try:
        # ── Summary counts (Library-scoped) ──
        row = db.execute(
            "SELECT COUNT(*) AS total, COALESCE(SUM(cited_by_count), 0) AS citations "
            "FROM papers WHERE status = 'library'"
        ).fetchone()
        total_pubs = row["total"]
        total_citations = row["citations"] or 0

        # Distinct materialized `authors` rows with ≥1 Library paper.
        # Same join shape as the `authors` list below (authors ⋈
        # publication_authors ⋈ papers) so `summary.total_authors` equals
        # `len(authors)` — otherwise the header count and the row count
        # drift silently when the junction table references openalex_ids
        # that haven't been backfilled into `authors` yet.
        total_authors = 0
        if table_exists(db, "publication_authors") and table_exists(db, "authors"):
            r = db.execute(
                """
                SELECT COUNT(DISTINCT a.id) AS c
                FROM authors a
                JOIN publication_authors pa
                  ON pa.openalex_id = a.openalex_id
                 AND a.openalex_id IS NOT NULL
                 AND TRIM(a.openalex_id) <> ''
                JOIN papers p ON p.id = pa.paper_id
                WHERE p.status = 'library'
                """
            ).fetchone()
            total_authors = r["c"] or 0

        total_countries = 0
        total_institutions = 0
        if table_exists(db, "publication_institutions"):
            r = db.execute(
                """
                SELECT COUNT(DISTINCT UPPER(TRIM(pi.country_code))) AS c
                FROM publication_institutions pi
                JOIN papers p ON p.id = pi.paper_id
                WHERE p.status = 'library'
                  AND pi.country_code IS NOT NULL
                  AND TRIM(pi.country_code) <> ''
                """
            ).fetchone()
            total_countries = r["c"]
            r = db.execute(
                """
                SELECT COUNT(DISTINCT pi.institution_name) AS c
                FROM publication_institutions pi
                JOIN papers p ON p.id = pi.paper_id
                WHERE p.status = 'library'
                  AND pi.institution_name IS NOT NULL
                  AND TRIM(pi.institution_name) <> ''
                """
            ).fetchone()
            total_institutions = r["c"]

        total_topics = 0
        if table_exists(db, "publication_topics"):
            if table_exists(db, "topics"):
                r = db.execute(
                    """
                    SELECT COUNT(DISTINCT COALESCE(t.canonical_name, pt.term)) AS c
                    FROM publication_topics pt
                    JOIN papers p ON p.id = pt.paper_id
                    LEFT JOIN topics t ON pt.topic_id = t.topic_id
                    WHERE p.status = 'library'
                    """
                ).fetchone()
            else:
                r = db.execute(
                    """
                    SELECT COUNT(DISTINCT pt.term) AS c
                    FROM publication_topics pt
                    JOIN papers p ON p.id = pt.paper_id
                    WHERE p.status = 'library'
                    """
                ).fetchone()
            total_topics = r["c"]

        summary = {
            "total_publications": total_pubs,
            "total_citations": total_citations,
            "total_authors": total_authors,
            "total_countries": total_countries,
            "total_topics": total_topics,
            "total_institutions": total_institutions,
            "avg_citations_per_paper": round(total_citations / total_pubs, 1) if total_pubs else 0.0,
            "avg_papers_per_author": round(total_pubs / total_authors, 1) if total_authors else 0.0,
        }

        # ── Publications by year (Library-scoped) ──
        rows = db.execute(
            "SELECT year, COUNT(*) AS count, "
            "COALESCE(SUM(cited_by_count), 0) AS citations, "
            "ROUND(COALESCE(AVG(cited_by_count), 0), 1) AS avg_citations "
            "FROM papers WHERE status = 'library' AND year IS NOT NULL "
            "GROUP BY year ORDER BY year ASC"
        ).fetchall()
        publications_by_year = [dict(r) for r in rows]

        # ── Countries (Library-scoped) ──
        countries = []
        if table_exists(db, "publication_institutions"):
            rows = db.execute(
                """
                SELECT TRIM(UPPER(pi.country_code)) AS country_code,
                       COUNT(DISTINCT pi.paper_id) AS count
                FROM publication_institutions pi
                JOIN papers p ON p.id = pi.paper_id
                WHERE p.status = 'library'
                  AND pi.country_code IS NOT NULL
                  AND TRIM(pi.country_code) <> ''
                GROUP BY TRIM(UPPER(pi.country_code))
                ORDER BY count DESC LIMIT 20
                """
            ).fetchall()
            countries = [dict(r) for r in rows]

        # ── Top institutions (Library-scoped) ──
        top_institutions = []
        if table_exists(db, "publication_institutions"):
            rows = db.execute(
                """
                SELECT pi.institution_name,
                       pi.country_code,
                       COUNT(DISTINCT pi.paper_id) AS count
                FROM publication_institutions pi
                JOIN papers p ON p.id = pi.paper_id
                WHERE p.status = 'library'
                  AND pi.institution_name IS NOT NULL
                  AND TRIM(pi.institution_name) <> ''
                GROUP BY pi.institution_name
                ORDER BY count DESC LIMIT 15
                """
            ).fetchall()
            top_institutions = [dict(r) for r in rows]

        # ── Top topics (Library-scoped) ──
        top_topics = []
        if table_exists(db, "publication_topics"):
            if table_exists(db, "topics"):
                rows = db.execute(
                    """
                    SELECT COALESCE(t.canonical_name, pt.term) AS term,
                           COUNT(DISTINCT pt.paper_id) AS count,
                           ROUND(COALESCE(AVG(p.cited_by_count), 0), 1) AS avg_citations
                    FROM publication_topics pt
                    JOIN papers p ON p.id = pt.paper_id
                    LEFT JOIN topics t ON pt.topic_id = t.topic_id
                    WHERE p.status = 'library'
                    GROUP BY COALESCE(t.canonical_name, pt.term)
                    ORDER BY count DESC LIMIT 20
                    """
                ).fetchall()
            else:
                rows = db.execute(
                    """
                    SELECT pt.term,
                           COUNT(DISTINCT pt.paper_id) AS count,
                           ROUND(COALESCE(AVG(p.cited_by_count), 0), 1) AS avg_citations
                    FROM publication_topics pt
                    JOIN papers p ON p.id = pt.paper_id
                    WHERE p.status = 'library'
                    GROUP BY pt.term
                    ORDER BY count DESC LIMIT 20
                    """
                ).fetchall()
            top_topics = [dict(r) for r in rows]

        # ── Top journals (Library-scoped) ──
        rows = db.execute(
            "SELECT journal, COUNT(*) AS count, "
            "COALESCE(SUM(cited_by_count), 0) AS citations, "
            "ROUND(COALESCE(AVG(cited_by_count), 0), 1) AS avg_citations "
            "FROM papers "
            "WHERE status = 'library' "
            "  AND journal IS NOT NULL "
            "  AND TRIM(journal) <> '' "
            "GROUP BY journal "
            "ORDER BY count DESC LIMIT 15"
        ).fetchall()
        top_journals = [dict(r) for r in rows]

        # ── Authors (Library-scoped) ──
        # Only authors with ≥1 Library paper. Per-author paper/citation
        # counts are scoped to Library papers so the number advertises an
        # author's footprint inside *this* library, not their full career.
        author_rows = db.execute(
            """
            SELECT
                a.id AS id,
                a.name AS name,
                COALESCE(a.h_index, 0) AS h_index,
                COUNT(DISTINCT pa.paper_id) AS papers,
                COALESCE(SUM(p.cited_by_count), 0) AS citations
            FROM authors a
            JOIN publication_authors pa
              ON pa.openalex_id = a.openalex_id
             AND a.openalex_id IS NOT NULL
             AND TRIM(a.openalex_id) <> ''
            JOIN papers p
              ON p.id = pa.paper_id
             AND p.status = 'library'
            GROUP BY a.id, a.name, a.h_index
            ORDER BY a.name COLLATE NOCASE
            """
        ).fetchall()

        # Top topic per author, counted only over Library papers — so a
        # "top topic" reflects the author's contribution to the library,
        # not topics from their background corpus papers.
        has_topics_table = table_exists(db, "topics")
        author_top_topics: dict[str, str | None] = {}
        if table_exists(db, "publication_topics") and table_exists(db, "publication_authors"):
            if has_topics_table:
                rows = db.execute(
                    """
                    WITH author_paper_topics AS (
                        SELECT
                            pa.openalex_id AS author_openalex_id,
                            COALESCE(t.canonical_name, pt.term) AS term
                        FROM publication_authors pa
                        JOIN papers p ON p.id = pa.paper_id AND p.status = 'library'
                        JOIN publication_topics pt ON pt.paper_id = pa.paper_id
                        LEFT JOIN topics t ON pt.topic_id = t.topic_id
                    ),
                    topic_counts AS (
                        SELECT
                            a.id AS author_id,
                            apt.term,
                            COUNT(*) AS cnt
                        FROM authors a
                        JOIN author_paper_topics apt ON apt.author_openalex_id = a.openalex_id
                            AND a.openalex_id IS NOT NULL AND TRIM(a.openalex_id) != ''
                        GROUP BY a.id, apt.term
                    ),
                    ranked AS (
                        SELECT
                            author_id,
                            term,
                            cnt,
                            ROW_NUMBER() OVER (
                                PARTITION BY author_id
                                ORDER BY cnt DESC, term ASC
                            ) AS rn
                        FROM topic_counts
                    )
                    SELECT author_id, term FROM ranked WHERE rn = 1
                    """
                ).fetchall()
            else:
                rows = db.execute(
                    """
                    WITH author_paper_topics AS (
                        SELECT
                            pa.openalex_id AS author_openalex_id,
                            pt.term
                        FROM publication_authors pa
                        JOIN papers p ON p.id = pa.paper_id AND p.status = 'library'
                        JOIN publication_topics pt ON pt.paper_id = pa.paper_id
                    ),
                    topic_counts AS (
                        SELECT
                            a.id AS author_id,
                            apt.term,
                            COUNT(*) AS cnt
                        FROM authors a
                        JOIN author_paper_topics apt ON apt.author_openalex_id = a.openalex_id
                            AND a.openalex_id IS NOT NULL AND TRIM(a.openalex_id) != ''
                        GROUP BY a.id, apt.term
                    ),
                    ranked AS (
                        SELECT
                            author_id,
                            term,
                            cnt,
                            ROW_NUMBER() OVER (
                                PARTITION BY author_id
                                ORDER BY cnt DESC, term ASC
                            ) AS rn
                        FROM topic_counts
                    )
                    SELECT author_id, term FROM ranked WHERE rn = 1
                    """
                ).fetchall()
            author_top_topics = {row["author_id"]: row["term"] for row in rows}

        authors = []
        for ar in author_rows:
            aid = ar["id"]
            authors.append({
                "id": aid,
                "name": ar["name"],
                "papers": ar["papers"] or 0,
                "citations": ar["citations"] or 0,
                "h_index": ar["h_index"] or 0,
                "top_topic": author_top_topics.get(aid),
            })

        # ── Recommendations (Discovery-layer, intentionally NOT Library-scoped) ──
        # This block reports Discovery engine engagement (seen / liked /
        # dismissed). It's about the recommender, not curation, so it
        # aggregates across all recommendations. Keep here for the single
        # "Insights" screen; if this block grows, consider moving it to a
        # Discovery-specific insight tab.
        rec_data = {
            "total": 0, "seen": 0, "liked": 0, "dismissed": 0,
            "engagement_rate": 0.0,
            "by_lens": [],
        }
        if table_exists(db, "recommendations"):
            r = db.execute(
                "SELECT COUNT(*) AS total, "
                "COALESCE(SUM(CASE WHEN user_action = 'seen' THEN 1 ELSE 0 END), 0) AS seen, "
                "COALESCE(SUM(CASE WHEN user_action = 'like' THEN 1 ELSE 0 END), 0) AS liked, "
                "COALESCE(SUM(CASE WHEN user_action = 'dismiss' THEN 1 ELSE 0 END), 0) AS dismissed "
                "FROM recommendations"
            ).fetchone()
            total_recs = r["total"]
            seen = r["seen"]
            liked = r["liked"]
            dismissed = r["dismissed"]
            engagement = ((liked + dismissed) / total_recs) if total_recs else 0.0

            rows = db.execute(
                "SELECT COALESCE(lens_id, 'unknown') AS lens_id, COUNT(*) AS count, "
                "ROUND(AVG(score), 3) AS avg_score "
                "FROM recommendations GROUP BY lens_id ORDER BY count DESC"
            ).fetchall()
            by_lens = [dict(r) for r in rows]

            rec_data = {
                "total": total_recs,
                "seen": seen,
                "liked": liked,
                "dismissed": dismissed,
                "engagement_rate": round(engagement, 3),
                "by_lens": by_lens,
            }

        # ── Embeddings (Library-scoped coverage) ──
        # "X vectors / Y% coverage" reflects "how much of my Library is
        # semantically indexable". Corpus-wide embedding coverage is an
        # AI-layer diagnostic — it lives in the AI snapshot / Settings,
        # not on the Insights curation dashboard.
        from alma.discovery.similarity import get_active_embedding_model

        emb_model = get_active_embedding_model(db)
        emb_total = 0
        if table_exists(db, "publication_embeddings"):
            r = db.execute(
                """
                SELECT COUNT(*) AS c
                FROM publication_embeddings pe
                JOIN papers p ON p.id = pe.paper_id
                WHERE pe.model = ? AND p.status = 'library'
                """,
                (emb_model,),
            ).fetchone()
            emb_total = r["c"]

        embeddings = {
            "total_vectors": emb_total,
            "model": emb_model,
            "coverage_pct": round(emb_total / total_pubs * 100, 1) if total_pubs else 0.0,
        }

        # ── Library (already Library-scoped) ──
        lib_liked = 0
        lib_avg_rating = 0.0
        r = db.execute(
            "SELECT COUNT(*) AS c, ROUND(COALESCE(AVG(CASE WHEN rating > 0 THEN rating END), 0), 1) AS avg "
            "FROM papers WHERE status = 'library'"
        ).fetchone()
        lib_liked = r["c"]
        lib_avg_rating = r["avg"] or 0.0

        lib_collections = 0
        if table_exists(db, "collections"):
            lib_collections = db.execute("SELECT COUNT(*) AS c FROM collections").fetchone()["c"]

        lib_followed = 0
        if table_exists(db, "followed_authors"):
            lib_followed = db.execute("SELECT COUNT(*) AS c FROM followed_authors").fetchone()["c"]

        library = {
            "total_saved": lib_liked,
            "avg_rating": lib_avg_rating,
            "total_collections": lib_collections,
            "total_followed_authors": lib_followed,
        }

        return {
            "summary": summary,
            "publications_by_year": publications_by_year,
            "countries": countries,
            "top_institutions": top_institutions,
            "top_topics": top_topics,
            "top_journals": top_journals,
            "authors": authors,
            "recommendations": rec_data,
            "embeddings": embeddings,
            "library": library,
        }

    except Exception as e:
        raise_internal("Failed to compute insights", e)


@router.get(
    "/diagnostics",
    summary="Get monitor and discovery diagnostics",
    description="Returns product-facing diagnostics for Feed monitors, Discovery sources, and branch quality.",
)
def get_insights_diagnostics(
    db: sqlite3.Connection = Depends(get_db),
    user: dict = Depends(get_current_user),
):
    try:
        from alma.application import feed_monitors as monitor_app

        monitors = monitor_app.list_feed_monitors(db) if table_exists(db, "feed_monitors") else []
        total_monitors = len(monitors)
        ready_monitors = [m for m in monitors if m.get("health") == "ready"]
        degraded_monitors = [m for m in monitors if m.get("health") == "degraded"]
        disabled_monitors = [m for m in monitors if m.get("health") == "disabled"]

        monitor_rows: list[dict[str, Any]] = []
        for monitor in monitors:
            last_result = monitor.get("last_result") if isinstance(monitor.get("last_result"), dict) else {}
            papers_found = last_result.get("papers_found") if isinstance(last_result, dict) else None
            items_created = last_result.get("items_created") if isinstance(last_result, dict) else None
            yield_rate = None
            if isinstance(papers_found, (int, float)) and int(papers_found) > 0 and isinstance(items_created, (int, float)):
                yield_rate = round(float(items_created) / float(papers_found), 3)
            monitor_rows.append(
                {
                    "id": monitor.get("id"),
                    "label": monitor.get("label"),
                    "monitor_type": monitor.get("monitor_type"),
                    "author_id": monitor.get("author_id"),
                    "author_name": monitor.get("author_name"),
                    "health": monitor.get("health"),
                    "health_reason": monitor.get("health_reason"),
                    "last_checked_at": monitor.get("last_checked_at"),
                    "last_success_at": monitor.get("last_success_at"),
                    "last_status": monitor.get("last_status"),
                    "last_error": monitor.get("last_error"),
                    "papers_found": int(papers_found) if isinstance(papers_found, (int, float)) else 0,
                    "items_created": int(items_created) if isinstance(items_created, (int, float)) else 0,
                    "yield_rate": yield_rate,
                }
            )
        monitor_rows.sort(
            key=lambda item: (
                0 if item["health"] == "degraded" else 1 if item["health"] == "disabled" else 2,
                -(item.get("items_created") or 0),
                str(item.get("label") or "").lower(),
            )
        )

        feed_refresh_ops = _load_recent_operations(db, operation_key="feed.refresh_inbox", limit=45)
        discovery_refresh_ops = _load_recent_operations(db, operation_key="discovery.refresh_recommendations", limit=45)

        recent_feed_refreshes = []
        for op in feed_refresh_ops:
            result = op.get("result") or {}
            recent_feed_refreshes.append(
                {
                    "job_id": op["job_id"],
                    "status": op["status"],
                    "finished_at": op.get("finished_at") or op.get("updated_at"),
                    "items_created": int(result.get("items_created") or 0),
                    "papers_found": int(result.get("papers_found") or 0),
                    "monitors_total": int(result.get("monitors_total") or 0),
                    "monitors_degraded": int(result.get("monitors_degraded") or 0),
                }
            )

        recent_discovery_refreshes = []
        for op in discovery_refresh_ops:
            result = op.get("result") or {}
            recent_discovery_refreshes.append(
                {
                    "job_id": op["job_id"],
                    "status": op["status"],
                    "finished_at": op.get("finished_at") or op.get("updated_at"),
                    "new_recommendations": int(result.get("new_recommendations") or 0),
                    "total_recommendations": int(result.get("total_recommendations") or 0),
                }
            )

        combined_results = []
        for op in [*feed_refresh_ops, *discovery_refresh_ops]:
            result = op.get("result") or {}
            if isinstance(result, dict):
                combined_results.append(result)

        source_diagnostics = _aggregate_http_source_diagnostics(combined_results)
        openalex_usage = _aggregate_openalex_usage(combined_results)

        recommendation_totals = {
            "total": 0,
            "active_unseen": 0,
        }
        source_quality: list[dict[str, Any]] = []
        branch_quality: list[dict[str, Any]] = []
        if table_exists(db, "recommendations"):
            recent_publication_cutoff = (datetime.utcnow() - timedelta(days=365)).date().isoformat()
            total_row = db.execute(
                """
                SELECT
                    COUNT(*) AS total,
                    COALESCE(SUM(CASE WHEN COALESCE(user_action, '') = '' THEN 1 ELSE 0 END), 0) AS active_unseen
                FROM recommendations
                """
            ).fetchone()
            recommendation_totals = {
                "total": int(total_row["total"] or 0),
                "active_unseen": int(total_row["active_unseen"] or 0),
            }

            source_rows = db.execute(
                """
                SELECT
                    COALESCE(NULLIF(source_type, ''), 'unknown') AS source_type,
                    COALESCE(NULLIF(source_api, ''), 'unknown') AS source_api,
                    COUNT(*) AS count,
                    ROUND(COALESCE(AVG(score), 0), 3) AS avg_score,
                    COALESCE(SUM(CASE WHEN user_action = 'like' THEN 1 ELSE 0 END), 0) AS liked,
                    COALESCE(SUM(CASE WHEN user_action = 'dismiss' THEN 1 ELSE 0 END), 0) AS dismissed,
                    COALESCE(SUM(CASE WHEN user_action = 'seen' THEN 1 ELSE 0 END), 0) AS seen
                FROM recommendations
                GROUP BY COALESCE(NULLIF(source_type, ''), 'unknown'), COALESCE(NULLIF(source_api, ''), 'unknown')
                ORDER BY count DESC, avg_score DESC
                """
            ).fetchall()
            for row in source_rows:
                count = int(row["count"] or 0)
                liked = int(row["liked"] or 0)
                dismissed = int(row["dismissed"] or 0)
                source_quality.append(
                    {
                        "source_type": row["source_type"],
                        "source_api": row["source_api"],
                        "count": count,
                        "avg_score": float(row["avg_score"] or 0.0),
                        "liked": liked,
                        "dismissed": dismissed,
                        "seen": int(row["seen"] or 0),
                        "engagement_rate": round((liked + dismissed) / count, 3) if count else 0.0,
                    }
                )

            branch_rows = db.execute(
                """
                SELECT
                    COALESCE(NULLIF(branch_id, ''), NULL) AS branch_id,
                    COALESCE(NULLIF(branch_label, ''), NULL) AS branch_label,
                    COUNT(*) AS count,
                    ROUND(COALESCE(AVG(score), 0), 3) AS avg_score,
                    COALESCE(SUM(CASE WHEN user_action = 'like' THEN 1 ELSE 0 END), 0) AS liked,
                    COALESCE(SUM(CASE WHEN user_action = 'save' THEN 1 ELSE 0 END), 0) AS saved,
                    COALESCE(SUM(CASE WHEN user_action = 'dismiss' THEN 1 ELSE 0 END), 0) AS dismissed,
                    COALESCE(SUM(CASE WHEN COALESCE(user_action, '') = '' THEN 1 ELSE 0 END), 0) AS unseen,
                    COALESCE(SUM(CASE WHEN branch_mode = 'core' THEN 1 ELSE 0 END), 0) AS core_count,
                    COALESCE(SUM(CASE WHEN branch_mode = 'explore' THEN 1 ELSE 0 END), 0) AS explore_count,
                    COALESCE(SUM(
                        CASE
                            WHEN COALESCE(substr(p.publication_date, 1, 10), '') >= ? THEN 1
                            ELSE 0
                        END
                    ), 0) AS recent_count,
                    COUNT(DISTINCT COALESCE(NULLIF(source_type, ''), 'unknown')) AS unique_sources
                FROM recommendations r
                LEFT JOIN papers p ON p.id = r.paper_id
                WHERE COALESCE(branch_id, '') <> '' OR COALESCE(branch_label, '') <> ''
                GROUP BY COALESCE(NULLIF(branch_id, ''), NULL), COALESCE(NULLIF(branch_label, ''), NULL)
                ORDER BY count DESC, avg_score DESC
                """
                ,
                (recent_publication_cutoff,),
            ).fetchall()
            for row in branch_rows:
                count = int(row["count"] or 0)
                liked = int(row["liked"] or 0)
                saved = int(row["saved"] or 0)
                dismissed = int(row["dismissed"] or 0)
                unseen = int(row["unseen"] or 0)
                core_count = int(row["core_count"] or 0)
                explore_count = int(row["explore_count"] or 0)
                recent_count = int(row["recent_count"] or 0)
                unique_sources = int(row["unique_sources"] or 0)
                positive_rate = safe_div(liked + saved, count)
                dismiss_rate = safe_div(dismissed, count)
                recent_share = safe_div(recent_count, count)
                dominant_mode = "core" if core_count >= explore_count else "explore"
                branch_id = row["branch_id"]
                branch_label = row["branch_label"] or row["branch_id"] or "Unnamed branch"
                try:
                    if branch_id:
                        source_mix_rows = db.execute(
                            """
                            SELECT COALESCE(NULLIF(source_type, ''), 'unknown') AS source_type, COUNT(*) AS count
                            FROM recommendations
                            WHERE branch_id = ?
                            GROUP BY COALESCE(NULLIF(source_type, ''), 'unknown')
                            ORDER BY count DESC, source_type ASC
                            LIMIT 4
                            """,
                            (branch_id,),
                        ).fetchall()
                    else:
                        source_mix_rows = db.execute(
                            """
                            SELECT COALESCE(NULLIF(source_type, ''), 'unknown') AS source_type, COUNT(*) AS count
                            FROM recommendations
                            WHERE COALESCE(NULLIF(branch_label, ''), '') = ?
                            GROUP BY COALESCE(NULLIF(source_type, ''), 'unknown')
                            ORDER BY count DESC, source_type ASC
                            LIMIT 4
                            """,
                            (branch_label,),
                        ).fetchall()
                except sqlite3.OperationalError:
                    source_mix_rows = []
                source_mix = [
                    {
                        "source_type": str(source_row["source_type"] or "unknown"),
                        "count": int(source_row["count"] or 0),
                    }
                    for source_row in source_mix_rows
                ]
                if count >= 4 and dismiss_rate >= 0.40:
                    tuning_hint = "Mute or cool this branch. Dismissals are too high."
                    quality_state = "cool"
                elif count >= 4 and positive_rate >= 0.28 and recent_share >= 0.35:
                    tuning_hint = "Boost this branch. It is producing useful, recent recommendations."
                    quality_state = "strong"
                elif count <= 3 and positive_rate >= 0.34:
                    tuning_hint = "Give this branch more budget. Early outcomes are promising but volume is thin."
                    quality_state = "underexplored"
                elif unique_sources <= 1 and positive_rate >= 0.20:
                    tuning_hint = "Diversify source mix. Branch quality is decent but too concentrated."
                    quality_state = "narrow"
                else:
                    tuning_hint = "Monitor this branch. It needs more volume or clearer user feedback."
                    quality_state = "monitor"
                branch_quality.append(
                    {
                        "branch_id": branch_id,
                        "branch_label": branch_label,
                        "count": count,
                        "avg_score": float(row["avg_score"] or 0.0),
                        "liked": liked,
                        "saved": saved,
                        "dismissed": dismissed,
                        "unseen": unseen,
                        "engagement_rate": round((liked + saved + dismissed) / count, 3) if count else 0.0,
                        "positive_rate": round(positive_rate, 3),
                        "dismiss_rate": round(dismiss_rate, 3),
                        "recent_share": round(recent_share, 3),
                        "dominant_mode": dominant_mode,
                        "core_count": core_count,
                        "explore_count": explore_count,
                        "unique_sources": unique_sources,
                        "source_mix": source_mix,
                        "quality_state": quality_state,
                        "tuning_hint": tuning_hint,
                    }
                )

        workflow_snapshot = _library_workflow_snapshot(db)
        from alma.application import discovery as discovery_app

        discovery_settings = discovery_app.read_settings(db)
        authors_snapshot = _build_authors_snapshot(db, monitors)
        alerts_snapshot = _build_alert_quality_snapshot(db, days=30)
        signal_lab_snapshot = _build_signal_lab_snapshot(db)
        ai_snapshot = _build_ai_snapshot(db, discovery_settings=discovery_settings)
        operational_snapshot = _build_operational_snapshot(
            db,
            monitors=monitors,
            discovery_settings=discovery_settings,
            alert_snapshot=alerts_snapshot,
            authors_snapshot=authors_snapshot,
            ai_snapshot=ai_snapshot,
        )
        ready_ratio = safe_div(len(ready_monitors), total_monitors or 1)
        avg_monitor_yield = safe_div(
            sum(float(item.get("yield_rate") or 0.0) for item in monitor_rows if item.get("yield_rate") is not None),
            max(1, sum(1 for item in monitor_rows if item.get("yield_rate") is not None)),
        )
        feed_score = max(0, min(100, round(((ready_ratio * 0.7) + (min(avg_monitor_yield * 1.5, 1.0) * 0.3)) * 100)))

        total_source_count = sum(int(item.get("count") or 0) for item in source_quality)
        total_source_liked = sum(int(item.get("liked") or 0) for item in source_quality)
        total_source_dismissed = sum(int(item.get("dismissed") or 0) for item in source_quality)
        total_source_engaged = total_source_liked + total_source_dismissed
        discovery_score = max(
            0,
            min(
                100,
                round(
                    (
                        (safe_div(total_source_engaged, total_source_count or 1) * 0.45)
                        + (safe_div(total_source_liked, total_source_engaged or 1) * 0.35)
                        + ((1.0 - safe_div(total_source_dismissed, total_source_engaged or 1)) * 0.20)
                    ) * 100
                ),
            ),
        )

        active_branch_rows = [item for item in branch_quality if int(item.get("count") or 0) > 0]
        branch_score = max(
            0,
            min(
                100,
                round(
                    (
                        (
                            safe_div(
                                sum(float(item.get("positive_rate") or 0.0) for item in active_branch_rows),
                                max(1, len(active_branch_rows)),
                            )
                            * 0.50
                        )
                        + (
                            safe_div(
                                sum(float(item.get("recent_share") or 0.0) for item in active_branch_rows),
                                max(1, len(active_branch_rows)),
                            )
                            * 0.20
                        )
                        + (
                            safe_div(
                                sum(min(int(item.get("unique_sources") or 0), 3) / 3.0 for item in active_branch_rows),
                                max(1, len(active_branch_rows)),
                            )
                            * 0.10
                        )
                        + (
                            safe_div(
                                sum((1.0 - float(item.get("dismiss_rate") or 0.0)) for item in active_branch_rows),
                                max(1, len(active_branch_rows)),
                            )
                            * 0.20
                        )
                    )
                    * 100
                ),
            ),
        )

        total_library = max(1, workflow_snapshot["total_library"])
        workflow_score = max(
            0,
            min(
                100,
                round(
                    (
                        ((1.0 - safe_div(workflow_snapshot["untriaged_count"], total_library)) * 0.7)
                        + (safe_div(workflow_snapshot["done_count"] + workflow_snapshot["reading_count"], total_library) * 0.3)
                    ) * 100
                ),
            ),
        )

        scorecards = [
            {
                "id": "feed_monitor_health",
                "label": "Feed Monitor Health",
                "score": feed_score,
                "status": "good" if feed_score >= 75 else "attention" if feed_score >= 50 else "critical",
                "summary": f"{len(ready_monitors)} of {total_monitors} monitors are ready.",
                "detail": f"Average recent yield is {avg_monitor_yield:.2f} and {len(degraded_monitors)} monitors are degraded.",
            },
            {
                "id": "discovery_quality",
                "label": "Discovery Quality",
                "score": discovery_score,
                "status": "good" if discovery_score >= 75 else "attention" if discovery_score >= 50 else "critical",
                "summary": f"{total_source_liked} likes and {total_source_dismissed} dismisses across {total_source_count} recommendations.",
                "detail": f"Engagement is {safe_div(total_source_engaged, total_source_count or 1) * 100:.0f}% across tracked source groups.",
            },
            {
                "id": "branch_signal_quality",
                "label": "Branch Signal Quality",
                "score": branch_score,
                "status": "good" if branch_score >= 75 else "attention" if branch_score >= 50 else "critical",
                "summary": f"{len(active_branch_rows)} branches have tracked recommendation outcomes.",
                "detail": "Branch score reflects positive outcomes, recency share, source diversity, and dismiss pressure.",
            },
            {
                "id": "library_workflow",
                "label": "Library Workflow",
                "score": workflow_score,
                "status": "good" if workflow_score >= 75 else "attention" if workflow_score >= 50 else "critical",
                "summary": f"{workflow_snapshot['untriaged_count']} untriaged and {workflow_snapshot['queued_count']} queued papers.",
                "detail": "Workflow score rewards triaged acquisitions and progress through the reading queue.",
            },
        ]

        recommended_actions: list[dict[str, Any]] = []
        if degraded_monitors:
            recommended_actions.append(
                {
                    "id": "repair_degraded_monitors",
                    "title": "Repair degraded monitors",
                    "detail": f"{len(degraded_monitors)} Feed monitors are degraded and reducing intake coverage.",
                    "page": "authors",
                    "params": {"filter": "", "followed": "true"},
                    "priority": "high",
                }
            )
        noisy_source = next(
            (
                item for item in sorted(
                    source_quality,
                    key=lambda row: (safe_div(float(row.get("dismissed") or 0.0), max(1.0, float(row.get("count") or 0.0))), -float(row.get("count") or 0.0)),
                    reverse=True,
                )
                if int(item.get("count") or 0) >= 4 and safe_div(float(item.get("dismissed") or 0.0), max(1.0, float(item.get("count") or 0.0))) >= 0.35
            ),
            None,
        )
        if noisy_source:
            recommended_actions.append(
                {
                    "id": "tune_noisy_sources",
                    "title": "Tune noisy discovery sources",
                    "detail": f"{noisy_source['source_type']} via {noisy_source['source_api']} is over-producing dismissals.",
                    "page": "settings",
                    "params": {"section": "discovery"},
                    "priority": "medium",
                }
            )
        best_branch = next(
            (
                item for item in sorted(
                    branch_quality,
                    key=lambda row: (float(row.get("engagement_rate") or 0.0), float(row.get("count") or 0.0)),
                    reverse=True,
                )
                if int(item.get("count") or 0) >= 3 and float(item.get("engagement_rate") or 0.0) >= 0.25
            ),
            None,
        )
        if best_branch:
            recommended_actions.append(
                {
                    "id": "operationalize_branch",
                    "title": "Operationalize a strong branch",
                    "detail": f"{best_branch['branch_label']} is already engaging well enough to turn into an alert or branch watch.",
                    "page": "alerts",
                    "params": {"section": "rules"},
                    "priority": "medium",
                }
            )
        weak_branch = next(
            (
                item
                for item in sorted(
                    branch_quality,
                    key=lambda row: (float(row.get("dismiss_rate") or 0.0), -float(row.get("count") or 0.0)),
                    reverse=True,
                )
                if int(item.get("count") or 0) >= 4 and float(item.get("dismiss_rate") or 0.0) >= 0.35
            ),
            None,
        )
        if weak_branch:
            recommended_actions.append(
                {
                    "id": "cool_weak_branch",
                    "title": "Cool a weak branch",
                    "detail": f"{weak_branch['branch_label']} is producing too many dismissals and should be muted, cooled, or rebalanced.",
                    "page": "discovery",
                    "params": {},
                    "priority": "medium",
                }
            )
        underexplored_branch = next(
            (
                item
                for item in sorted(
                    branch_quality,
                    key=lambda row: (float(row.get("positive_rate") or 0.0), -float(row.get("count") or 0.0)),
                    reverse=True,
                )
                if int(item.get("count") or 0) <= 3 and float(item.get("positive_rate") or 0.0) >= 0.34
            ),
            None,
        )
        if underexplored_branch:
            recommended_actions.append(
                {
                    "id": "expand_underexplored_branch",
                    "title": "Expand an underexplored branch",
                    "detail": f"{underexplored_branch['branch_label']} is promising but underfed. Give it more branch budget or exploratory temperature.",
                    "page": "discovery",
                    "params": {},
                    "priority": "low",
                }
            )
        if workflow_snapshot["untriaged_count"] >= 5:
            recommended_actions.append(
                {
                    "id": "triage_library_backlog",
                    "title": "Triage the library backlog",
                    "detail": f"{workflow_snapshot['untriaged_count']} library papers still have no reading status.",
                    "page": "library",
                    "params": {"tab": "all"},
                    "priority": "high",
                }
            )
        if int((authors_snapshot.get("summary") or {}).get("background_corpus_papers") or 0) <= max(3, int((authors_snapshot.get("summary") or {}).get("tracked_authors") or 0)):
            recommended_actions.append(
                {
                    "id": "grow_followed_author_corpus",
                    "title": "Grow followed-author background corpus",
                    "detail": "Tracked authors still have a thin non-library historical corpus. Run more author backfills and review dossier coverage.",
                    "page": "authors",
                    "params": {"followed": "true"},
                    "priority": "high",
                }
            )
        for item in (ai_snapshot.get("recommendations") or [])[:3]:
            recommended_actions.append(
                {
                    "id": str(item.get("id") or "ai_recommendation"),
                    "title": str(item.get("label") or "Tune AI layer"),
                    "detail": str(item.get("detail") or "").strip() or "AI health needs review.",
                    "page": "settings",
                    "params": {"section": "ai"},
                    "priority": "high" if str(item.get("severity") or "") == "critical" else "medium",
                }
            )
        if workflow_snapshot["uncollected_count"] >= 5:
            recommended_actions.append(
                {
                    "id": "organize_library_structure",
                    "title": "Organize uncategorized library papers",
                    "detail": f"{workflow_snapshot['uncollected_count']} library papers are not yet assigned to any collection.",
                    "page": "library",
                    "params": {"tab": "collections"},
                    "priority": "medium",
                }
            )

        try:
            from alma.application import alerts as alerts_app

            automation_opportunities = alerts_app.list_alert_templates(db)[:4]
        except Exception:
            automation_opportunities = []

        feed_refresh_trend = _build_refresh_trend(
            feed_refresh_ops,
            primary_key="items_created",
            secondary_key="papers_found",
        )
        discovery_refresh_trend = _build_refresh_trend(
            discovery_refresh_ops,
            primary_key="new_recommendations",
            secondary_key="total_recommendations",
        )
        recommendation_action_trend = _build_recommendation_action_trend(db, days=30)
        alert_history_trend = _build_alert_history_trend(db, days=30)
        branch_trends = _build_branch_trends(db, days=30)
        author_follow_trend = _build_author_follow_trend(db, days=30)
        signal_lab_trend = _build_signal_lab_trend(db, days=30)
        cold_start_topic_validation = _build_cold_start_topic_validation(db)

        tracked_authors = max(1, int((authors_snapshot.get("summary") or {}).get("tracked_authors") or 0))
        ready_tracked = int((authors_snapshot.get("summary") or {}).get("ready_tracked") or 0)
        bridge_gap_count = int((authors_snapshot.get("summary") or {}).get("bridge_gap_count") or 0)
        stale_backfills = int((authors_snapshot.get("summary") or {}).get("stale_backfills") or 0)
        thin_backfills = int((authors_snapshot.get("summary") or {}).get("thin_backfills") or 0)
        pending_backfills = int((authors_snapshot.get("summary") or {}).get("pending_backfills") or 0)
        authors_score = max(
            0,
            min(
                100,
                round(
                    (
                        (safe_div(ready_tracked, tracked_authors) * 0.55)
                        + ((1.0 - safe_div(bridge_gap_count, tracked_authors)) * 0.20)
                        + ((1.0 - safe_div(stale_backfills + thin_backfills + pending_backfills, tracked_authors)) * 0.25)
                    ) * 100
                ),
            ),
        )
        alert_summary = alerts_snapshot.get("summary") or {}
        sent_runs_30d = int(alert_summary.get("sent_runs_30d") or 0)
        failed_runs_30d = int(alert_summary.get("failed_runs_30d") or 0)
        empty_runs_30d = int(alert_summary.get("empty_runs_30d") or 0)
        alerts_score = max(
            0,
            min(
                100,
                round(
                    (
                        ((1.0 - safe_div(failed_runs_30d, max(1, sent_runs_30d + failed_runs_30d))) * 0.45)
                        + ((1.0 - safe_div(empty_runs_30d, max(1, sent_runs_30d + empty_runs_30d))) * 0.30)
                        + (min(float(alert_summary.get("avg_papers_per_sent") or 0.0) / 4.0, 1.0) * 0.25)
                    ) * 100
                ),
            ),
        )
        low_usefulness_alert = next(
            (
                item
                for item in (alerts_snapshot.get("top_alerts") or [])
                if int(item.get("total_runs") or 0) >= 3 and int(item.get("usefulness_score") or 0) <= 55
            ),
            None,
        )
        signal_summary = signal_lab_snapshot.get("summary") or {}
        signal_score = max(
            0,
            min(
                100,
                round(
                    (
                        (min(int(signal_summary.get("week_interactions") or 0) / 10.0, 1.0) * 0.40)
                        + (min(int(signal_summary.get("source_diversity_7d") or 0) / 4.0, 1.0) * 0.15)
                        + (min(int(signal_summary.get("topic_coverage") or 0) / 8.0, 1.0) * 0.15)
                        + (min(float(signal_summary.get("recommendation_engagement_rate") or 0.0) / 0.45, 1.0) * 0.30)
                    ) * 100
                ),
            ),
        )
        ai_summary = ai_snapshot.get("summary") or {}
        ai_score = max(
            0,
            min(
                100,
                round(
                    (
                        (min(float(ai_summary.get("embedding_coverage_pct") or 0.0) / 100.0, 1.0) * 0.45)
                        + ((1.0 - min(int(ai_summary.get("stale_embeddings") or 0) / max(1, int(ai_summary.get("stale_embeddings") or 0) + int(ai_summary.get("up_to_date_embeddings") or 0)), 1.0)) * 0.15)
                        + (min(float(ai_summary.get("hybrid_text_rate") or 0.0) / 0.6, 1.0) * 0.10)
                        + (min(float(ai_summary.get("avg_text_similarity") or 0.0) / 0.35, 1.0) * 0.10)
                        + ((1.0 - min(float(ai_summary.get("compressed_similarity_rate") or 0.0) / 0.6, 1.0)) * 0.07)
                    ) * 100
                ),
            ),
        )
        operational_summary = operational_snapshot.get("summary") or {}
        operational_score = max(
            0,
            min(
                100,
                round(
                    (
                        ((1.0 - safe_div(int(operational_summary.get("critical_count") or 0), 3)) * 0.45)
                        + ((1.0 - safe_div(int(operational_summary.get("warning_count") or 0), 6)) * 0.30)
                        + (safe_div(int(operational_summary.get("healthy_checks") or 0), 7) * 0.25)
                    ) * 100
                ),
            ),
        )

        return {
            "generated_at": datetime.utcnow().isoformat(),
            "feed": {
                "summary": {
                    "total_monitors": total_monitors,
                    "ready_monitors": len(ready_monitors),
                    "degraded_monitors": len(degraded_monitors),
                    "disabled_monitors": len(disabled_monitors),
                    "author_monitors": sum(1 for m in monitors if m.get("monitor_type") == "author"),
                    "topic_monitors": sum(1 for m in monitors if m.get("monitor_type") == "topic"),
                    "query_monitors": sum(1 for m in monitors if m.get("monitor_type") == "query"),
                },
                "monitors": monitor_rows[:20],
                "recent_refreshes": recent_feed_refreshes,
                "scorecards": [card for card in scorecards if card["id"] == "feed_monitor_health"],
            },
            "discovery": {
                "summary": recommendation_totals,
                "source_quality": source_quality,
                "branch_quality": branch_quality,
                "branch_trends": branch_trends,
                "cold_start_topic_validation": cold_start_topic_validation,
                "source_diagnostics": source_diagnostics,
                "openalex_usage": openalex_usage,
                "recent_refreshes": recent_discovery_refreshes,
                "scorecards": [card for card in scorecards if card["id"] in {"discovery_quality", "branch_signal_quality"}],
            },
            "library": {
                "workflow": workflow_snapshot,
                "scorecards": [card for card in scorecards if card["id"] == "library_workflow"],
            },
            "authors": authors_snapshot,
            "alerts": alerts_snapshot,
            "feedback_learning": signal_lab_snapshot,
            "ai": ai_snapshot,
            "operational": operational_snapshot,
            "trends": {
                "window_days": 30,
                "feed_refresh_daily": feed_refresh_trend,
                "discovery_refresh_daily": discovery_refresh_trend,
                "recommendation_actions_daily": recommendation_action_trend,
                "alert_history_daily": alert_history_trend,
                "alert_history_weekly_90d": ((alerts_snapshot.get("long_horizon") or {}).get("weekly_trend") or []),
                "author_follows_daily": author_follow_trend,
                "feedback_learning_daily": signal_lab_trend,
            },
            "evaluation": {
                "scorecards": scorecards
                + [
                    {
                        "id": "ai_quality",
                        "label": "AI Retrieval Quality",
                        "score": ai_score,
                        "status": "good" if ai_score >= 75 else "attention" if ai_score >= 50 else "critical",
                        "summary": f"{ai_summary.get('embedding_coverage_pct', 0)}% embedding coverage and {int(ai_summary.get('recent_recommendations_analyzed') or 0)} recent recommendations analyzed.",
                        "detail": "AI score reflects embedding coverage, embedding freshness, hybrid-text usage, and similarity quality.",
                    },
                    {
                        "id": "authors_monitoring",
                        "label": "Authors Monitoring",
                        "score": authors_score,
                        "status": "good" if authors_score >= 75 else "attention" if authors_score >= 50 else "critical",
                        "summary": f"{ready_tracked} of {int((authors_snapshot.get('summary') or {}).get('tracked_authors') or 0)} tracked authors are refresh-ready.",
                        "detail": f"{bridge_gap_count} tracked authors still need a stronger identity bridge.",
                    },
                    {
                        "id": "alert_automation_quality",
                        "label": "Alert Automation Quality",
                        "score": alerts_score,
                        "status": "good" if alerts_score >= 75 else "attention" if alerts_score >= 50 else "critical",
                        "summary": f"{sent_runs_30d} sent runs, {failed_runs_30d} failed, {empty_runs_30d} empty in the last 30 days.",
                        "detail": "Alert score balances delivery reliability, non-empty output, and papers delivered per successful run.",
                    },
                    {
                        "id": "feedback_learning",
                        "label": "Feedback Learning",
                        "score": signal_score,
                        "status": "good" if signal_score >= 75 else "attention" if signal_score >= 50 else "critical",
                        "summary": f"{int(signal_summary.get('week_interactions') or 0)} interactions this week with {int(signal_summary.get('source_diversity_7d') or 0)} source groups touched.",
                        "detail": "Learning health reflects recent interaction depth, source diversity, topic coverage, and recommendation engagement.",
                    },
                    {
                        "id": "operational_health",
                        "label": "Operational Health",
                        "score": operational_score,
                        "status": "good" if operational_score >= 75 else "attention" if operational_score >= 50 else "critical",
                        "summary": f"{int(operational_summary.get('issues_total') or 0)} active issues across embeddings, monitors, sources, alerts, and plugins.",
                        "detail": "Operational health focuses on degraded capabilities that directly affect retrieval quality, delivery, and observability.",
                    },
                ],
                "recommended_actions": (
                    recommended_actions
                    + (
                        [
                            {
                                "id": "repair_author_bridges",
                                "title": "Repair author identity bridges",
                                "detail": f"{bridge_gap_count} tracked authors are still missing a clean OpenAlex bridge for reliable refresh.",
                                "page": "authors",
                                "params": {"followed": "true"},
                                "priority": "high",
                            }
                        ]
                        if bridge_gap_count > 0
                        else []
                    )
                    + (
                        [
                            {
                                "id": "tune_low_usefulness_alerts",
                                "title": "Tune low-usefulness alerts",
                                "detail": f"{low_usefulness_alert.get('alert_name', 'This alert')} is generating too many empty or low-yield runs.",
                                "page": "alerts",
                                "params": {"section": "history"},
                                "priority": "medium",
                            }
                        ]
                        if low_usefulness_alert is not None
                        else []
                    )
                    + (
                        [
                            {
                                "id": "grow_feedback_learning_coverage",
                                "title": "Grow feedback-learning coverage",
                                "detail": "Recent interactions are still too shallow for a strong learning loop. Use Discovery, Feed, and Library actions more deliberately.",
                                "page": "discovery",
                                "params": {},
                                "priority": "medium",
                            }
                        ]
                        if int(signal_summary.get("week_interactions") or 0) < 8
                        else []
                    )
                    + (
                        [
                            {
                                "id": "resolve_operational_issues",
                                "title": "Resolve degraded operational states",
                                "detail": f"{int(operational_summary.get('issues_total') or 0)} active issues are reducing product quality or delivery reliability.",
                                "page": "settings",
                                "params": {"section": "operations"},
                                "priority": "high" if int(operational_summary.get("critical_count") or 0) > 0 else "medium",
                            }
                        ]
                        if int(operational_summary.get("issues_total") or 0) > 0
                        else []
                    )
                )[:8],
                "automation_opportunities": automation_opportunities,
            },
        }

    except Exception as e:
        raise_internal("Failed to compute insights diagnostics", e)


@router.post(
    "/discovery/branch-action",
    summary="Apply a branch control action from Insights",
)
def apply_branch_action(
    body: BranchTuningActionRequest,
    db: sqlite3.Connection = Depends(get_db),
    user: dict = Depends(get_current_user),
):
    try:
        from alma.application import discovery as discovery_app

        result = discovery_app.apply_branch_control_action(
            db,
            branch_id=body.branch_id,
            action=body.action,
        )
        db.commit()
        return result
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise_internal("Failed to apply branch action", e)

