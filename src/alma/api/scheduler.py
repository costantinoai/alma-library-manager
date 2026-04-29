"""Background scheduler for periodic alert evaluation and author refresh.

Uses APScheduler's BackgroundScheduler to run jobs in background threads.
The two core periodic jobs are:

1. **evaluate_scheduled_alerts** -- runs every ALERT_CHECK_INTERVAL_HOURS
   (default: 1 hour).  For each enabled alert whose schedule is 'daily' or
   'weekly', the function checks whether enough time has elapsed since
   ``last_evaluated_at`` and, if so, evaluates the alert (matching rules,
   filtering already-alerted papers, sending notifications, recording
   history).

2. **refresh_authors_periodic** -- runs daily at AUTHOR_REFRESH_HOUR
   (default: 03:00 UTC).  Refreshes publication caches for all tracked
   authors.

Environment variables
---------------------
SCHEDULER_ENABLED           -- set to "false" to disable scheduler (default: true)
ALERT_CHECK_INTERVAL_HOURS  -- interval between alert evaluation sweeps (default: 1)
AUTHOR_REFRESH_HOUR         -- UTC hour for the daily author refresh cron (default: 3)
"""

import asyncio
import base64
import json
import logging
import os
import sqlite3
import threading
import uuid
import collections
from datetime import datetime, timedelta, timezone
from typing import Callable, Optional

from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.date import DateTrigger
from apscheduler.triggers.interval import IntervalTrigger
from alma.core.redaction import redact_sensitive_data, redact_sensitive_text

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Module-level state
# ---------------------------------------------------------------------------

_scheduler: Optional[BackgroundScheduler] = None
_job_meta: dict[str, dict] = {}
_job_status: dict[str, dict] = {}
_job_logs: dict[str, collections.deque[dict]] = {}
_job_lock = threading.RLock()
_ACTIVITY_STATUS_LIMIT = 2000
_ACTIVE_STATUSES = {"queued", "scheduled", "running", "cancelling"}


def _activity_conn() -> sqlite3.Connection:
    """Open a connection to the unified DB for durable activity persistence."""
    from alma.config import get_db_path

    conn = sqlite3.connect(str(get_db_path()), check_same_thread=False, timeout=0.25)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA busy_timeout=250")
    return conn


def _json_dumps_safe(value: object) -> str:
    try:
        return json.dumps(value, ensure_ascii=False, default=str)
    except Exception:
        return json.dumps(str(value), ensure_ascii=False)


def _parse_activity_time(value: object) -> Optional[datetime]:
    """Parse an Activity timestamp to naive UTC.

    ALMa stores every `operation_status` / `operation_logs` timestamp as
    naive UTC via `datetime.utcnow().isoformat()`.  This helper accepts
    naive-UTC, tz-aware (including trailing `Z`), and legacy
    `YYYY-MM-DD HH:MM:SS` strings, and always returns naive UTC so
    comparisons against `datetime.utcnow()` are meaningful regardless
    of the dev machine's local timezone.  The pre-2026-04-25 code
    stripped `tzinfo` after converting to local time, producing a
    2-hour offset on timezone-aware inputs and a silent mismatch
    whenever a naive-UTC string was compared to `datetime.now()`.
    """
    if not value:
        return None
    text = str(value).strip()
    if not text:
        return None
    if text.endswith("Z"):
        text = f"{text[:-1]}+00:00"
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        try:
            parsed = datetime.strptime(text, "%Y-%m-%d %H:%M:%S")
        except ValueError:
            return None
    if parsed.tzinfo is not None:
        parsed = parsed.astimezone(timezone.utc).replace(tzinfo=None)
    return parsed


def _is_stale_active_status(status: dict, stale_after_seconds: int = 300) -> bool:
    if str(status.get("status") or "").lower() not in _ACTIVE_STATUSES:
        return False
    stamp = _parse_activity_time(status.get("updated_at")) or _parse_activity_time(status.get("started_at"))
    if stamp is None:
        return True
    # Stored stamps are naive UTC (see `_parse_activity_time`); compare
    # against naive UTC wall-clock so the 300 s stale threshold is
    # meaningful regardless of local timezone.
    return datetime.utcnow() - stamp > timedelta(seconds=max(1, int(stale_after_seconds)))


def _persist_job_status(job_id: str, status: dict) -> None:
    """Persist one job status row into operation_status."""
    known = {
        "job_id",
        "status",
        "message",
        "error",
        "started_at",
        "finished_at",
        "updated_at",
        "processed",
        "total",
        "current_author",
        "operation_key",
        "trigger_source",
        "cancel_requested",
        "result",
    }
    result_json = _json_dumps_safe(status.get("result")) if "result" in status else None
    metadata = {k: v for k, v in status.items() if k not in known}
    metadata_json = _json_dumps_safe(metadata) if metadata else None
    cancel_requested = 1 if status.get("cancel_requested") else 0

    try:
        conn = _activity_conn()
        try:
            conn.execute(
                """
                INSERT INTO operation_status (
                    job_id, status, message, error, started_at, finished_at, updated_at,
                    processed, total, current_author, operation_key, trigger_source,
                    cancel_requested, result_json, metadata_json
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(job_id) DO UPDATE SET
                    status = excluded.status,
                    message = excluded.message,
                    error = excluded.error,
                    started_at = COALESCE(operation_status.started_at, excluded.started_at),
                    finished_at = excluded.finished_at,
                    updated_at = excluded.updated_at,
                    processed = excluded.processed,
                    total = excluded.total,
                    current_author = excluded.current_author,
                    operation_key = COALESCE(excluded.operation_key, operation_status.operation_key),
                    trigger_source = COALESCE(excluded.trigger_source, operation_status.trigger_source),
                    cancel_requested = excluded.cancel_requested,
                    result_json = excluded.result_json,
                    metadata_json = excluded.metadata_json
                """,
                (
                    job_id,
                    str(status.get("status") or ""),
                    status.get("message"),
                    status.get("error"),
                    status.get("started_at"),
                    status.get("finished_at"),
                    status.get("updated_at") or datetime.utcnow().isoformat(),
                    status.get("processed"),
                    status.get("total"),
                    status.get("current_author"),
                    status.get("operation_key"),
                    status.get("trigger_source"),
                    cancel_requested,
                    result_json,
                    metadata_json,
                ),
            )
            conn.commit()
        finally:
            conn.close()
    except Exception as exc:
        logger.debug("Could not persist operation status for %s: %s", job_id, exc)


def _persist_job_log(entry: dict) -> None:
    """Persist one operation log row into operation_logs."""
    try:
        conn = _activity_conn()
        try:
            conn.execute(
                """
                INSERT INTO operation_logs (job_id, timestamp, level, step, message, data_json)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (
                    entry.get("job_id"),
                    entry.get("timestamp") or datetime.utcnow().isoformat(),
                    entry.get("level") or "INFO",
                    entry.get("step"),
                    entry.get("message") or "",
                    _json_dumps_safe(entry.get("data") or {}),
                ),
            )
            conn.commit()
        finally:
            conn.close()
    except Exception as exc:
        logger.debug("Could not persist operation log for %s: %s", entry.get("job_id"), exc)


def _row_to_status(row: sqlite3.Row) -> dict:
    out = {
        "job_id": row["job_id"],
        "status": row["status"],
        "message": row["message"],
        "error": row["error"],
        "started_at": row["started_at"],
        "finished_at": row["finished_at"],
        "updated_at": row["updated_at"],
        "processed": row["processed"],
        "total": row["total"],
        "current_author": row["current_author"],
        "operation_key": row["operation_key"],
        "trigger_source": row["trigger_source"],
        "cancel_requested": bool(row["cancel_requested"]),
    }
    result_json = row["result_json"]
    if result_json:
        try:
            out["result"] = json.loads(result_json)
        except Exception:
            out["result"] = result_json
    meta_json = row["metadata_json"]
    if meta_json:
        try:
            meta = json.loads(meta_json)
            if isinstance(meta, dict):
                out.update(meta)
        except Exception:
            pass
    return redact_sensitive_data(out)


def _status_sort_key(st: dict) -> tuple[str, str]:
    return (str(st.get("updated_at") or ""), str(st.get("job_id") or ""))


def _encode_status_cursor(updated_at: str, job_id: str) -> str:
    raw = f"{updated_at}|{job_id}".encode("utf-8")
    return base64.urlsafe_b64encode(raw).decode("ascii")


def _decode_status_cursor(cursor: Optional[str]) -> Optional[tuple[str, str]]:
    if not cursor:
        return None
    try:
        raw = base64.urlsafe_b64decode(cursor.encode("ascii")).decode("utf-8")
        updated_at, job_id = raw.split("|", 1)
        return (updated_at, job_id)
    except Exception:
        return None


def _encode_log_cursor(log_id: int) -> str:
    raw = str(int(log_id)).encode("utf-8")
    return base64.urlsafe_b64encode(raw).decode("ascii")


def _decode_log_cursor(cursor: Optional[str]) -> Optional[int]:
    if not cursor:
        return None
    try:
        raw = base64.urlsafe_b64decode(cursor.encode("ascii")).decode("utf-8")
        return int(raw.strip())
    except Exception:
        return None


def _load_job_status_from_db(job_id: str) -> Optional[dict]:
    try:
        conn = _activity_conn()
        try:
            row = conn.execute(
                "SELECT * FROM operation_status WHERE job_id = ?",
                (job_id,),
            ).fetchone()
            if not row:
                return None
            return _row_to_status(row)
        finally:
            conn.close()
    except Exception:
        return None


def _load_statuses_from_db(limit: int = _ACTIVITY_STATUS_LIMIT) -> list[dict]:
    try:
        conn = _activity_conn()
        try:
            rows = conn.execute(
                "SELECT * FROM operation_status ORDER BY updated_at DESC LIMIT ?",
                (max(1, limit),),
            ).fetchall()
            return [_row_to_status(r) for r in rows]
        finally:
            conn.close()
    except Exception:
        return []


def _load_job_logs_from_db(job_id: str, limit: int = 100) -> list[dict]:
    try:
        conn = _activity_conn()
        try:
            rows = conn.execute(
                """
                SELECT job_id, timestamp, level, step, message, data_json
                FROM operation_logs
                WHERE job_id = ?
                ORDER BY id DESC
                LIMIT ?
                """,
                (job_id, max(1, min(limit, 500))),
            ).fetchall()
            out: list[dict] = []
            for r in reversed(rows):
                data = {}
                if r["data_json"]:
                    try:
                        parsed = json.loads(r["data_json"])
                        if isinstance(parsed, dict):
                            data = parsed
                    except Exception:
                        pass
                out.append(
                    {
                        "job_id": r["job_id"],
                        "timestamp": r["timestamp"],
                        "level": r["level"],
                        "step": r["step"],
                        "message": redact_sensitive_text(r["message"] or ""),
                        "data": redact_sensitive_data(data),
                    }
                )
            return out
        finally:
            conn.close()
    except Exception:
        return []


def _load_job_logs_page_from_db(
    job_id: str,
    *,
    limit: int = 100,
    before_id: Optional[int] = None,
) -> Optional[tuple[list[dict], Optional[str], bool]]:
    safe_limit = max(1, min(limit, 500))
    try:
        conn = _activity_conn()
        try:
            if before_id:
                rows = conn.execute(
                    """
                    SELECT id, job_id, timestamp, level, step, message, data_json
                    FROM operation_logs
                    WHERE job_id = ? AND id < ?
                    ORDER BY id DESC
                    LIMIT ?
                    """,
                    (job_id, int(before_id), safe_limit + 1),
                ).fetchall()
            else:
                rows = conn.execute(
                    """
                    SELECT id, job_id, timestamp, level, step, message, data_json
                    FROM operation_logs
                    WHERE job_id = ?
                    ORDER BY id DESC
                    LIMIT ?
                    """,
                    (job_id, safe_limit + 1),
                ).fetchall()
            if not rows:
                return ([], None, False)

            has_more = len(rows) > safe_limit
            page_rows = rows[:safe_limit]
            next_cursor = _encode_log_cursor(int(page_rows[-1]["id"])) if has_more else None

            out: list[dict] = []
            for r in reversed(page_rows):
                data = {}
                if r["data_json"]:
                    try:
                        parsed = json.loads(r["data_json"])
                        if isinstance(parsed, dict):
                            data = parsed
                    except Exception:
                        pass
                out.append(
                    {
                        "job_id": r["job_id"],
                        "timestamp": r["timestamp"],
                        "level": r["level"],
                        "step": r["step"],
                        "message": redact_sensitive_text(r["message"] or ""),
                        "data": redact_sensitive_data(data),
                    }
                )
            return (out, next_cursor, has_more)
        finally:
            conn.close()
    except Exception:
        return None


def _dismiss_job_from_db(job_id: str) -> bool:
    try:
        conn = _activity_conn()
        try:
            c1 = conn.execute("DELETE FROM operation_status WHERE job_id = ?", (job_id,))
            c2 = conn.execute("DELETE FROM operation_logs WHERE job_id = ?", (job_id,))
            conn.commit()
            return (c1.rowcount or 0) > 0 or (c2.rowcount or 0) > 0
        finally:
            conn.close()
    except Exception:
        return False

# ---------------------------------------------------------------------------
# Environment configuration helpers
# ---------------------------------------------------------------------------

def _scheduler_enabled() -> bool:
    return os.getenv("SCHEDULER_ENABLED", "true").lower() not in ("false", "0", "no")


def _alert_check_interval_hours() -> int:
    try:
        return max(1, int(os.getenv("ALERT_CHECK_INTERVAL_HOURS", "1")))
    except (ValueError, TypeError):
        return 1


def _author_refresh_hour() -> int:
    try:
        return int(os.getenv("AUTHOR_REFRESH_HOUR", "3")) % 24
    except (ValueError, TypeError):
        return 3


def _discovery_schedule_interval_hours(key: str, default: int = 0) -> int:
    try:
        from alma.api.deps import open_db_connection

        conn = open_db_connection()
        try:
            row = conn.execute(
                "SELECT value FROM discovery_settings WHERE key = ?",
                (key,),
            ).fetchone()
            if not row:
                return default
            return max(0, int(row["value"]))
        finally:
            conn.close()
    except Exception:
        return default


# ---------------------------------------------------------------------------
# Scheduler lifecycle
# ---------------------------------------------------------------------------

def get_scheduler() -> BackgroundScheduler:
    """Return the global scheduler, creating and starting it if needed."""
    global _scheduler
    if _scheduler is None:
        _scheduler = BackgroundScheduler()
        _scheduler.start()
        logger.info("Background scheduler started")
    return _scheduler


def reap_orphan_jobs(stale_after_seconds: int = 300) -> int:
    """Mark jobs abandoned across a process restart as ``cancelled``.

    Worker threads do not survive a backend restart, so any ``operation_status``
    row that still says ``queued / scheduled / running / cancelling`` at
    startup is by definition orphaned — no Python code is advancing it.
    Without this sweep, ghost rows (e.g. a Feed refresh that was mid-cancel
    when the process died) stay "in progress" forever and the UI keeps
    showing a stuck "Cancellation requested" pill.

    To avoid racing a sibling worker that just enqueued a fresh job (this app
    runs uvicorn with multiple workers, each calling ``setup_scheduler`` on
    startup), only rows older than ``stale_after_seconds`` are touched.
    Returns the number of rows reaped.
    """
    try:
        conn = _activity_conn()
        try:
            rows = conn.execute(
                """
                SELECT *
                FROM operation_status
                WHERE status IN ('queued', 'scheduled', 'running', 'cancelling')
                """,
            ).fetchall()
            stale_job_ids = [
                str(row["job_id"])
                for row in rows
                if _is_stale_active_status(_row_to_status(row), stale_after_seconds)
            ]
            if stale_job_ids:
                conn.executemany(
                    """
                    UPDATE operation_status
                    SET status = 'cancelled',
                        finished_at = COALESCE(finished_at, ?),
                        updated_at = ?,
                        message = 'Orphaned across process restart; auto-cancelled',
                        error = COALESCE(error, 'Worker process exited before this job finished'),
                        cancel_requested = 0
                    WHERE job_id = ?
                    """,
                    [
                        (datetime.utcnow().isoformat(), datetime.utcnow().isoformat(), job_id)
                        for job_id in stale_job_ids
                    ],
                )
            reaped = len(stale_job_ids)
            conn.commit()
            if reaped > 0:
                logger.warning(
                    "Reaped %d orphaned activity job(s) from a previous process",
                    reaped,
                )
            return reaped
        finally:
            conn.close()
    except Exception as exc:
        logger.debug("Orphan job sweep failed: %s", exc)
        return 0


def setup_scheduler() -> None:
    """Configure and start the scheduler with the core periodic jobs.

    Safe to call multiple times; jobs are registered with
    ``replace_existing=True``.
    """
    if not _scheduler_enabled():
        logger.info("Scheduler disabled via SCHEDULER_ENABLED env var")
        return

    # Clean up ghost jobs left behind by the previous process before we start
    # registering new work. Idempotent and bounded by ``stale_after_seconds``
    # so a sibling worker's in-flight row is never touched.
    try:
        reap_orphan_jobs()
    except Exception as exc:
        logger.warning("Orphan job reap skipped: %s", exc)

    sched = get_scheduler()

    # -- Alert evaluation sweep (interval) ----------------------------------
    interval_hours = _alert_check_interval_hours()
    sched.add_job(
        evaluate_scheduled_alerts,
        trigger=IntervalTrigger(hours=interval_hours),
        id="evaluate_alerts",
        name="Evaluate scheduled alerts",
        replace_existing=True,
    )
    with _job_lock:
        _job_meta["evaluate_alerts"] = {
            "action": "evaluate_alerts",
            "name": "Evaluate scheduled alerts",
            "description": f"Checks all non-manual alerts every {interval_hours}h",
        }
    logger.info(
        "Registered evaluate_alerts job (interval=%dh)", interval_hours,
    )

    # -- Daily author refresh (cron) ----------------------------------------
    refresh_hour = _author_refresh_hour()
    sched.add_job(
        refresh_authors_periodic,
        trigger=CronTrigger(hour=refresh_hour),
        id="refresh_authors",
        name="Daily author refresh",
        replace_existing=True,
    )
    with _job_lock:
        _job_meta["refresh_authors"] = {
            "action": "refresh_authors",
            "name": "Daily author refresh",
            "description": f"Refreshes all authors daily at {refresh_hour:02d}:00 UTC",
            "cron": f"0 {refresh_hour} * * *",
        }
    logger.info(
        "Registered refresh_authors job (cron hour=%d)", refresh_hour,
    )

    # -- Discovery recommendation refresh (interval) ----------------------
    refresh_hours = _discovery_schedule_interval_hours("schedule.refresh_interval_hours", 0)
    if refresh_hours > 0:
        sched.add_job(
            refresh_recommendations_periodic,
            trigger=IntervalTrigger(hours=refresh_hours),
            id="refresh_recommendations",
            name="Periodic recommendation refresh",
            replace_existing=True,
        )
        with _job_lock:
            _job_meta["refresh_recommendations"] = {
                "action": "refresh_recommendations",
                "name": "Periodic recommendation refresh",
                "description": f"Refreshes discovery recommendations every {refresh_hours}h",
            }
        logger.info(
            "Registered refresh_recommendations job (interval=%dh)", refresh_hours,
        )

    # -- Citation graph maintenance (interval) -----------------------------
    graph_maintenance_hours = _discovery_schedule_interval_hours(
        "schedule.graph_maintenance_interval_hours",
        24,
    )
    if graph_maintenance_hours > 0:
        sched.add_job(
            maintain_citation_graph_periodic,
            trigger=IntervalTrigger(hours=graph_maintenance_hours),
            id="maintain_citation_graph",
            name="Periodic citation graph maintenance",
            replace_existing=True,
        )
        with _job_lock:
            _job_meta["maintain_citation_graph"] = {
                "action": "maintain_citation_graph",
                "name": "Periodic citation graph maintenance",
                "description": f"Backfills missing publication references every {graph_maintenance_hours}h",
            }
        logger.info(
            "Registered maintain_citation_graph job (interval=%dh)", graph_maintenance_hours,
        )

    # -- DB maintenance (daily) -------------------------------------------
    # Reclaims free pages and prunes stale operation_logs. Runs at 04:30
    # UTC — well after the daily author refresh at AUTHOR_REFRESH_HOUR
    # (default 03:00) so the two never compete for the writer lock.
    sched.add_job(
        db_maintenance_periodic,
        trigger=CronTrigger(hour=4, minute=30),
        id="db_maintenance",
        name="Daily DB maintenance",
        replace_existing=True,
    )
    with _job_lock:
        _job_meta["db_maintenance"] = {
            "action": "db_maintenance",
            "name": "Daily DB maintenance",
            "description": "Incremental vacuum + operation_logs retention, daily at 04:30 UTC",
            "cron": "30 4 * * *",
        }
    logger.info("Registered db_maintenance job (cron 04:30 UTC)")


def shutdown_scheduler() -> None:
    """Gracefully shut down the scheduler."""
    global _scheduler
    if _scheduler is not None:
        try:
            _scheduler.shutdown(wait=False)
            logger.info("Background scheduler stopped")
        except Exception as exc:
            logger.warning("Error during scheduler shutdown: %s", exc)
        _scheduler = None


# ===================================================================
# Core scheduled jobs
# ===================================================================

def evaluate_scheduled_alerts() -> None:
    """Sweep all enabled, non-manual digests and evaluate due ones."""
    job_id = "periodic_alert_evaluation"
    operation_key = "alerts.evaluate_scheduled"
    set_job_status(
        job_id,
        status="running",
        trigger_source="scheduler",
        operation_key=operation_key,
        started_at=datetime.utcnow().isoformat(),
        message="Evaluating scheduled alerts",
    )
    logger.info("Starting scheduled alert evaluation sweep")
    try:
        from alma.application import alerts as alerts_app
        from alma.api.deps import open_db_connection

        conn = open_db_connection()

        try:
            alerts = conn.execute(
                "SELECT * FROM alerts WHERE enabled = 1 AND schedule NOT IN ('manual', 'immediate')"
            ).fetchall()
        except sqlite3.OperationalError as exc:
            logger.warning("Cannot query alerts table (may not exist yet): %s", exc)
            conn.close()
            return

        now = datetime.utcnow()
        evaluated_count = 0
        sent_total = 0
        failed_total = 0

        for alert_row in alerts:
            alert = dict(alert_row)
            alert_id = alert["id"]
            schedule = str(alert.get("schedule") or "").strip().lower()
            schedule_config = {}
            try:
                raw_schedule = alert.get("schedule_config")
                if raw_schedule:
                    schedule_config = json.loads(raw_schedule)
                    if not isinstance(schedule_config, dict):
                        schedule_config = {}
            except Exception:
                schedule_config = {}
            last_eval_str = str(alert.get("last_evaluated_at") or "").strip() or None

            try:
                if not _is_due(
                    schedule=schedule,
                    schedule_config=schedule_config,
                    last_evaluated_at=last_eval_str,
                    now=now,
                ):
                    continue

                logger.info("Alert %s (%s) is due -- evaluating", alert["name"], alert_id)
                result = asyncio.run(
                    alerts_app.evaluate_digest(conn, alert_id, trigger_source="scheduler")
                )
                # `open_db_connection` does NOT auto-commit (unlike the
                # request-scoped `get_db` dependency).  `evaluate_digest`
                # writes to `alerted_publications`, `alert_history`, and
                # `alerts.last_evaluated_at`; without an explicit commit
                # the connection close at the end of the sweep rolls them
                # back -- the Slack DM goes out (network side-effect) but
                # the dedup record is lost, so the same paper re-fires on
                # the next sweep.  Same class of fix as
                # `routes/alerts.py:evaluate_alert` in v0.10.0.
                conn.commit()
                if result is None:
                    continue
                sent_total += int(result.get("papers_sent") or 0)
                failed_total += int(result.get("papers_failed") or 0)
                evaluated_count += 1

            except Exception:
                try:
                    conn.rollback()
                except Exception:
                    pass
                logger.exception("Error evaluating alert %s", alert_id)

        conn.close()
        logger.info(
            "Alert evaluation sweep complete: %d/%d alerts evaluated",
            evaluated_count, len(alerts),
        )
        set_job_status(
            job_id,
            status="completed",
            trigger_source="scheduler",
            operation_key=operation_key,
            finished_at=datetime.utcnow().isoformat(),
            message="Scheduled alert evaluation complete",
            result={
                "evaluated": evaluated_count,
                "total": len(alerts),
                "papers_sent": sent_total,
                "papers_failed": failed_total,
            },
        )

    except Exception:
        logger.exception("Fatal error in evaluate_scheduled_alerts")
        set_job_status(
            job_id,
            status="failed",
            trigger_source="scheduler",
            operation_key=operation_key,
            finished_at=datetime.utcnow().isoformat(),
            message="Scheduled alert evaluation failed",
        )


def refresh_authors_periodic() -> None:
    """Refresh publication caches for all tracked authors.

    Reuses the same logic as ``POST /api/v1/fetch/refresh-cache``.
    Catches all exceptions so the scheduler job never crashes.
    """
    job_id = "periodic_author_refresh"
    operation_key = "authors.refresh_periodic"
    set_job_status(
        job_id,
        status="running",
        trigger_source="scheduler",
        operation_key=operation_key,
        started_at=datetime.utcnow().isoformat(),
        message="Refreshing authors (periodic)",
    )
    logger.info("Starting periodic author refresh")
    try:
        from alma.api.deps import open_db_connection
        from alma.api.routes.operations import do_refresh_cache_all

        conn = open_db_connection()
        try:
            result = do_refresh_cache_all(conn)
            logger.info("Periodic author refresh complete: %s", result)

            # Score new feed items by relevance after refresh
            try:
                from alma.application.feed import score_feed_items
                scored = score_feed_items(conn)
                if scored:
                    logger.info("Scored %d feed items after author refresh", scored)
            except Exception as score_exc:
                logger.debug("Feed scoring after refresh failed: %s", score_exc)

            set_job_status(
                job_id,
                status="completed",
                trigger_source="scheduler",
                operation_key=operation_key,
                finished_at=datetime.utcnow().isoformat(),
                message="Periodic author refresh complete",
                result=result,
            )
        finally:
            conn.close()

    except Exception:
        logger.exception("Fatal error in refresh_authors_periodic")
        set_job_status(
            job_id,
            status="failed",
            trigger_source="scheduler",
            operation_key=operation_key,
            finished_at=datetime.utcnow().isoformat(),
            message="Periodic author refresh failed",
        )


def refresh_recommendations_periodic() -> None:
    """Periodically refresh discovery recommendations via the lens system.

    Iterates all active lenses and refreshes recommendations for each.
    If no active lenses exist, auto-creates a default "Library Global" lens
    so the system works out of the box.
    """
    job_id = "periodic_recommendation_refresh"
    operation_key = "discovery.refresh_periodic"
    set_job_status(
        job_id,
        status="running",
        trigger_source="scheduler",
        operation_key=operation_key,
        started_at=datetime.utcnow().isoformat(),
        message="Refreshing discovery recommendations (periodic)",
    )
    logger.info("Starting periodic recommendation refresh via lens system")
    try:
        from alma.application.discovery import (
            list_lenses,
            create_lens,
            refresh_lens_recommendations,
        )
        from alma.api.deps import open_db_connection

        conn = open_db_connection()

        lenses = list_lenses(conn, is_active=True)

        # Auto-create default lens if none exist
        if not lenses:
            logger.info("No active lenses found — creating default 'Library Global' lens")
            try:
                new_lens = create_lens(
                    conn,
                    name="Library Global",
                    context_type="library_global",
                )
                conn.commit()
                lenses = [new_lens]
            except sqlite3.OperationalError as exc:
                logger.warning(
                    "Default lens bootstrap failed (%s); falling back to legacy discovery refresh",
                    exc,
                )
                from alma.discovery.engine import DiscoveryEngine
                from alma.config import get_db_path

                legacy_recs = DiscoveryEngine(get_db_path()).refresh_recommendations()
                conn.close()
                set_job_status(
                    job_id,
                    status="completed",
                    trigger_source="scheduler",
                    operation_key=operation_key,
                    finished_at=datetime.utcnow().isoformat(),
                    message="Periodic recommendation refresh complete (legacy fallback)",
                    result={
                        "total_inserted": len(legacy_recs or []),
                        "lenses_refreshed": 0,
                        "details": [{"mode": "legacy_global_refresh"}],
                    },
                )
                return

        total_inserted = 0
        lens_results = []
        for lens in lenses:
            try:
                result = refresh_lens_recommendations(
                    conn, lens["id"], trigger_source="scheduler"
                )
                conn.commit()
                inserted = (result or {}).get("inserted", 0)
                total_inserted += inserted
                lens_results.append({
                    "lens_id": lens["id"],
                    "lens_name": lens["name"],
                    "inserted": inserted,
                })
                logger.info(
                    "Lens '%s' (%s): %d recommendations inserted",
                    lens["name"], lens["id"], inserted,
                )
            except Exception as exc:
                logger.error("Failed to refresh lens '%s': %s", lens["name"], exc)
                lens_results.append({
                    "lens_id": lens["id"],
                    "lens_name": lens["name"],
                    "error": str(exc),
                })

        conn.close()
        logger.info(
            "Periodic recommendation refresh complete: %d total across %d lenses",
            total_inserted, len(lenses),
        )
        set_job_status(
            job_id,
            status="completed",
            trigger_source="scheduler",
            operation_key=operation_key,
            finished_at=datetime.utcnow().isoformat(),
            message="Periodic recommendation refresh complete",
            result={
                "total_inserted": total_inserted,
                "lenses_refreshed": len(lenses),
                "details": lens_results,
            },
        )
    except Exception:
        logger.exception("Fatal error in refresh_recommendations_periodic")
        set_job_status(
            job_id,
            status="failed",
            trigger_source="scheduler",
            operation_key=operation_key,
            finished_at=datetime.utcnow().isoformat(),
            message="Periodic recommendation refresh failed",
        )


def maintain_citation_graph_periodic() -> None:
    """Periodically backfill local citation/reference edges from OpenAlex."""
    job_id = "periodic_citation_graph_maintenance"
    set_job_status(
        job_id,
        status="running",
        trigger_source="scheduler",
        started_at=datetime.utcnow().isoformat(),
        operation_key="graphs.reference_backfill",
        message="Backfilling publication references (periodic)",
    )
    logger.info("Starting periodic citation graph maintenance")
    try:
        from alma.api.deps import open_db_connection
        from alma.openalex.client import backfill_missing_publication_references

        conn = open_db_connection()
        try:
            result = backfill_missing_publication_references(conn, limit=500)
            conn.commit()
            set_job_status(
                job_id,
                status="completed",
                trigger_source="scheduler",
                operation_key="graphs.reference_backfill",
                finished_at=datetime.utcnow().isoformat(),
                message="Periodic citation graph maintenance complete",
                result=result,
            )
        finally:
            conn.close()
    except Exception:
        logger.exception("Fatal error in maintain_citation_graph_periodic")
        set_job_status(
            job_id,
            status="failed",
            trigger_source="scheduler",
            operation_key="graphs.reference_backfill",
            finished_at=datetime.utcnow().isoformat(),
            message="Periodic citation graph maintenance failed",
        )


def _operation_log_retention_days() -> int:
    """Days of `operation_logs` history to keep before pruning. Default 30."""
    raw = os.getenv("ALMA_OPERATION_LOG_RETENTION_DAYS", "").strip()
    if not raw:
        return 30
    try:
        return max(1, int(raw))
    except ValueError:
        return 30


def db_maintenance_periodic() -> None:
    """Reclaim free pages and prune stale ``operation_logs`` rows.

    Runs daily. Two cheap maintenance steps that share one connection:

    1. ``PRAGMA incremental_vacuum`` — releases pages freed by deletes
       since the last run. Only meaningful when the DB was created (or
       converted) with ``auto_vacuum=INCREMENTAL`` (handled in
       ``init_db_schema``).
    2. ``DELETE FROM operation_logs WHERE timestamp < cutoff`` — caps
       the activity-log table at ``ALMA_OPERATION_LOG_RETENTION_DAYS``
       (default 30). Without this cap, every run accumulates dozens of
       rows and the table grows unbounded.
    """
    job_id = "periodic_db_maintenance"
    set_job_status(
        job_id,
        status="running",
        trigger_source="scheduler",
        started_at=datetime.utcnow().isoformat(),
        operation_key="db.maintenance",
        message="Running DB maintenance",
    )
    summary: dict[str, object] = {}
    try:
        from alma.api.deps import open_db_connection

        conn = open_db_connection()
        try:
            # Operation log retention — committed in its own transaction
            # so the VACUUM below has clean ground.
            retention_days = _operation_log_retention_days()
            cutoff = (
                datetime.utcnow() - timedelta(days=retention_days)
            ).isoformat()
            try:
                cur = conn.execute(
                    "DELETE FROM operation_logs WHERE timestamp < ?",
                    (cutoff,),
                )
                deleted = cur.rowcount
                conn.commit()
                summary["operation_logs_pruned"] = deleted
                summary["operation_logs_retention_days"] = retention_days
            except sqlite3.OperationalError as exc:
                logger.warning("operation_logs prune skipped: %s", exc)

            # Incremental vacuum runs in autocommit and releases all
            # currently-free pages. Cheap when there's little to free.
            try:
                free_before = conn.execute(
                    "PRAGMA freelist_count"
                ).fetchone()[0]
                if free_before:
                    conn.isolation_level = None
                    conn.execute("PRAGMA incremental_vacuum")
                    free_after = conn.execute(
                        "PRAGMA freelist_count"
                    ).fetchone()[0]
                    summary["pages_freed"] = free_before - free_after
                else:
                    summary["pages_freed"] = 0
            except sqlite3.OperationalError as exc:
                logger.warning("incremental_vacuum skipped: %s", exc)
        finally:
            conn.close()
        set_job_status(
            job_id,
            status="completed",
            trigger_source="scheduler",
            operation_key="db.maintenance",
            finished_at=datetime.utcnow().isoformat(),
            message="DB maintenance complete",
            result=summary,
        )
    except Exception:
        logger.exception("Fatal error in db_maintenance_periodic")
        set_job_status(
            job_id,
            status="failed",
            trigger_source="scheduler",
            operation_key="db.maintenance",
            finished_at=datetime.utcnow().isoformat(),
            message="DB maintenance failed",
        )


# ===================================================================
# Internal helpers for alert evaluation
# ===================================================================

def _is_due(
    *,
    schedule: str,
    schedule_config: Optional[dict],
    last_evaluated_at: Optional[str],
    now: datetime,
) -> bool:
    """Return True when the current schedule slot has not been processed."""
    schedule_norm = (schedule or "").strip().lower()
    config = schedule_config if isinstance(schedule_config, dict) else {}

    if schedule_norm == "daily":
        hour, minute = _parse_schedule_time(str(config.get("time") or "09:00"))
        slot = now.replace(hour=hour, minute=minute, second=0, microsecond=0)
        if now < slot:
            return False
    elif schedule_norm == "weekly":
        day_raw = str(config.get("day") or "monday").strip().lower()[:3]
        target_weekday = {
            "mon": 0,
            "tue": 1,
            "wed": 2,
            "thu": 3,
            "fri": 4,
            "sat": 5,
            "sun": 6,
        }.get(day_raw, 0)
        hour, minute = _parse_schedule_time(str(config.get("time") or "09:00"))
        slot_today = now.replace(hour=hour, minute=minute, second=0, microsecond=0)
        days_since = (now.weekday() - target_weekday) % 7
        slot = slot_today - timedelta(days=days_since)
        if now < slot:
            slot -= timedelta(days=7)
    else:
        return False

    if not last_evaluated_at:
        return True
    try:
        last_dt = datetime.fromisoformat(last_evaluated_at)
    except (ValueError, TypeError):
        return True
    return last_dt < slot


def _parse_schedule_time(raw: str) -> tuple[int, int]:
    try:
        hour_s, minute_s = raw.strip().split(":", 1)
        hour = max(0, min(23, int(hour_s)))
        minute = max(0, min(59, int(minute_s)))
        return hour, minute
    except Exception:
        return 9, 0


# ===================================================================
# Public scheduler API
# ===================================================================

def add_cron_job(
    job_id: str,
    cron_expr: str,
    func: Callable,
    args: tuple = (),
    meta: Optional[dict] = None,
) -> str:
    """Add a cron job to the scheduler."""
    sched = get_scheduler()
    trigger = CronTrigger.from_crontab(cron_expr)
    sched.add_job(func, trigger=trigger, id=job_id, replace_existing=True, args=args)
    logger.info("Scheduled job %s with cron '%s'", job_id, cron_expr)
    if meta is None:
        meta = {}
    with _job_lock:
        _job_meta[job_id] = {"cron": cron_expr, **meta}
    return job_id


def list_jobs() -> list[dict]:
    """List all scheduled jobs with metadata."""
    sched = get_scheduler()
    jobs = []
    with _job_lock:
        for j in sched.get_jobs():
            meta = _job_meta.get(j.id, {})
            jobs.append({
                "id": j.id,
                "cron": meta.get("cron"),
                "action": meta.get("action"),
                "name": meta.get("name") or j.name,
                "description": meta.get("description"),
                "next_run": j.next_run_time.isoformat() if j.next_run_time else None,
            })
    return jobs


def remove_job(job_id: str) -> bool:
    """Remove a scheduled job by ID."""
    sched = get_scheduler()
    try:
        sched.remove_job(job_id)
        with _job_lock:
            _job_meta.pop(job_id, None)
        logger.info("Removed job %s", job_id)
        return True
    except Exception as exc:
        logger.warning("Failed to remove job %s: %s", job_id, exc)
        return False


def run_job(job_id: str) -> bool:
    """Trigger a job to run immediately."""
    sched = get_scheduler()
    job = sched.get_job(job_id)
    if not job:
        return False
    try:
        sched.modify_job(job_id, next_run_time=datetime.now())
        logger.info("Triggered job %s to run immediately", job_id)
        return True
    except Exception as exc:
        logger.warning("Failed to run job %s immediately: %s", job_id, exc)
        # Fallback: execute synchronously
        try:
            func = getattr(job, "func", None)
            args = getattr(job, "args", ()) or ()
            kwargs = getattr(job, "kwargs", {}) or {}
            if callable(func):
                func(*args, **kwargs)
                logger.info("Executed job %s synchronously as fallback", job_id)
                return True
        except Exception as exc2:
            logger.error("Fallback run failed for job %s: %s", job_id, exc2)
        return False


def get_job_status(job_id: str) -> Optional[dict]:
    """Get the latest status dict for a job."""
    with _job_lock:
        status = _job_status.get(job_id)
    if status:
        return status
    db_status = _load_job_status_from_db(job_id)
    if db_status:
        with _job_lock:
            _job_status[job_id] = dict(db_status)
        return db_status
    return None


def find_active_job(operation_key: str) -> Optional[dict]:
    """Find an active job by operation key.

    Active statuses are ``queued``, ``scheduled``, and ``running``.
    Returns the most recently updated match.
    """
    if not operation_key:
        return None
    reap_orphan_jobs()
    candidates: list[tuple[str, dict]] = []
    with _job_lock:
        candidates.extend(
            (job_id, st)
            for job_id, st in _job_status.items()
            if st.get("operation_key") == operation_key
            and st.get("status") in _ACTIVE_STATUSES
            and not _is_stale_active_status(st)
        )
    for st in _load_statuses_from_db(limit=_ACTIVITY_STATUS_LIMIT):
        if (
            st.get("operation_key") == operation_key
            and st.get("status") in _ACTIVE_STATUSES
            and not _is_stale_active_status(st)
        ):
            candidates.append((str(st.get("job_id")), st))
    if not candidates:
        return None
    candidates.sort(key=lambda item: str(item[1].get("updated_at") or ""))
    job_id, st = candidates[-1]
    return {"job_id": job_id, **st}


def is_cancellation_requested(job_id: str) -> bool:
    """Return True if cancellation has been requested for a job."""
    st = get_job_status(job_id) or {}
    return bool(st.get("cancel_requested"))


def add_job_log(
    job_id: str,
    message: str,
    *,
    level: str = "INFO",
    step: Optional[str] = None,
    data: Optional[dict] = None,
) -> None:
    """Append a structured log entry for a job."""
    safe_message = redact_sensitive_text(message or "")
    safe_data = redact_sensitive_data(data or {})
    entry = {
        "timestamp": datetime.utcnow().isoformat(),
        "job_id": job_id,
        "level": (level or "INFO").upper(),
        "step": step,
        "message": safe_message,
        "data": safe_data,
    }
    with _job_lock:
        buf = _job_logs.setdefault(job_id, collections.deque(maxlen=500))
        buf.append(entry)
    _persist_job_log(entry)
    line = f"[{job_id}]"
    if step:
        line += f"[{step}]"
    line += f" {safe_message}"
    lvl = entry["level"]
    if lvl in ("ERROR", "CRITICAL"):
        logger.error(line)
    elif lvl == "WARNING":
        logger.warning(line)
    elif lvl == "DEBUG":
        logger.debug(line)
    else:
        logger.info(line)


def get_job_logs(job_id: str, limit: int = 100) -> list[dict]:
    """Return structured logs for a single job."""
    entries, _, _ = get_job_logs_page(job_id, limit=limit, cursor=None)
    return entries


def get_job_logs_page(
    job_id: str,
    *,
    limit: int = 100,
    cursor: Optional[str] = None,
) -> tuple[list[dict], Optional[str], bool]:
    """Return one page of structured logs for a job."""
    safe_limit = max(1, min(limit, 500))
    before_id = _decode_log_cursor(cursor)
    db_page = _load_job_logs_page_from_db(job_id, limit=safe_limit, before_id=before_id)
    if db_page is not None:
        return db_page

    with _job_lock:
        entries = list(_job_logs.get(job_id, collections.deque()))
    if not entries:
        return ([], None, False)

    # In-memory fallback uses an offset cursor against newest-first slices.
    try:
        offset = int(before_id or 0)
    except Exception:
        offset = 0
    reversed_entries = list(reversed(entries))
    page_rev = reversed_entries[offset : offset + safe_limit + 1]
    has_more = len(page_rev) > safe_limit
    page_rev = page_rev[:safe_limit]
    next_cursor = _encode_log_cursor(offset + safe_limit) if has_more else None
    return (list(reversed(page_rev)), next_cursor, has_more)


def set_job_status(job_id: str, **kwargs) -> None:
    """Merge status information for a job."""
    kwargs = redact_sensitive_data(kwargs)
    prev_status = None
    prev_message = None
    with _job_lock:
        status = _job_status.get(job_id, {})
        prev_status = status.get("status")
        prev_message = status.get("message")
        status.update(kwargs)
        status.setdefault("job_id", job_id)
        status["updated_at"] = datetime.utcnow().isoformat()
        _job_status[job_id] = status
        persisted = dict(status)
    _persist_job_status(job_id, persisted)
    status_changed = ("status" in kwargs) and (kwargs.get("status") != prev_status)
    message_changed = ("message" in kwargs) and (kwargs.get("message") != prev_message)
    status_msg = kwargs.get("message")
    status_val = kwargs.get("status")
    if status_val and status_msg and (status_changed or message_changed):
        add_job_log(
            job_id,
            str(status_msg),
            level="ERROR" if status_val == "failed" else "INFO",
            step="status",
            data={"status": status_val} if status_val else None,
        )
    elif status_val and status_changed:
        add_job_log(
            job_id,
            f"Status changed to {status_val}",
            level="ERROR" if status_val == "failed" else "INFO",
            step="status",
        )


def list_all_job_statuses(
    *,
    status: Optional[str] = None,
    trigger_source: Optional[str] = None,
    since: Optional[str] = None,
    limit: int = _ACTIVITY_STATUS_LIMIT,
) -> list[dict]:
    """Return job statuses with optional filtering."""
    merged: dict[str, dict] = {}
    for st in _load_statuses_from_db(limit=max(1, min(limit, _ACTIVITY_STATUS_LIMIT))):
        jid = str(st.get("job_id") or "")
        if jid:
            merged[jid] = st
    with _job_lock:
        for job_id, st_val in _job_status.items():
            existing = merged.get(job_id)
            if not existing:
                merged[job_id] = {"job_id": job_id, **st_val}
                continue
            if str(st_val.get("updated_at") or "") >= str(existing.get("updated_at") or ""):
                merged[job_id] = {"job_id": job_id, **st_val}
    items = list(merged.values())

    if status:
        wanted = status.strip().lower()
        items = [s for s in items if str(s.get("status") or "").lower() == wanted]
    if trigger_source:
        src = trigger_source.strip().lower()
        items = [s for s in items if str(s.get("trigger_source") or "").lower() == src]
    if since:
        since_norm = since.strip()
        items = [s for s in items if str(s.get("updated_at") or "") >= since_norm]

    items.sort(key=lambda s: str(s.get("updated_at") or ""), reverse=True)
    return items[: max(1, min(limit, _ACTIVITY_STATUS_LIMIT))]


def list_all_job_statuses_page(
    *,
    status: Optional[str] = None,
    trigger_source: Optional[str] = None,
    since: Optional[str] = None,
    limit: int = 200,
    cursor: Optional[str] = None,
) -> tuple[list[dict], Optional[str], bool]:
    """Return one page of job statuses with a cursor for older results."""
    safe_limit = max(1, min(limit, _ACTIVITY_STATUS_LIMIT))
    items = list_all_job_statuses(
        status=status,
        trigger_source=trigger_source,
        since=since,
        limit=_ACTIVITY_STATUS_LIMIT,
    )
    cursor_key = _decode_status_cursor(cursor)
    if cursor_key:
        items = [s for s in items if _status_sort_key(s) < cursor_key]

    page = items[: safe_limit + 1]
    has_more = len(page) > safe_limit
    page = page[:safe_limit]
    next_cursor: Optional[str] = None
    if has_more and page:
        last = page[-1]
        next_cursor = _encode_status_cursor(
            str(last.get("updated_at") or ""),
            str(last.get("job_id") or ""),
        )
    return (page, next_cursor, has_more)


def dismiss_job_status(job_id: str) -> bool:
    """Remove a job from Activity state and its detailed logs."""
    removed = False
    with _job_lock:
        if job_id in _job_status:
            _job_status.pop(job_id, None)
            removed = True
        if job_id in _job_logs:
            _job_logs.pop(job_id, None)
            removed = True
    removed = _dismiss_job_from_db(job_id) or removed
    return removed


def activity_envelope(
    job_id: str,
    *,
    status: str,
    operation_key: Optional[str] = None,
    message: Optional[str] = None,
    **extra,
) -> dict:
    """Build a canonical async operation response envelope."""
    payload = {
        "status": status,
        "job_id": job_id,
        "operation_id": job_id,
        "activity_url": f"/api/v1/activity/{job_id}/logs",
    }
    if operation_key:
        payload["operation_key"] = operation_key
    if message:
        payload["message"] = message
    payload.update(extra)
    return payload


def schedule_immediate(job_id: str, func, *args, **kwargs) -> bool:
    """Schedule a function to run immediately in the background."""
    sched = get_scheduler()

    # Ensure every immediate job appears in Activity even if the function
    # itself does not emit status updates.
    existing = get_job_status(job_id) or {}
    if not existing:
        set_job_status(
            job_id,
            status="scheduled",
            message=f"Scheduled: {job_id}",
            started_at=datetime.utcnow().isoformat(),
        )

    def _wrapped():
        st = get_job_status(job_id) or {}
        if st.get("status") == "cancelled" or st.get("cancel_requested"):
            set_job_status(
                job_id,
                status="cancelled",
                finished_at=datetime.utcnow().isoformat(),
                message=st.get("message") or f"Cancelled: {job_id}",
            )
            return
        if st.get("status") in ("scheduled", "queued", "running", None):
            set_job_status(
                job_id,
                status="running",
                started_at=st.get("started_at") or datetime.utcnow().isoformat(),
                message=st.get("message") or f"Running: {job_id}",
            )
        try:
            result = func(*args, **kwargs)
            st_after = get_job_status(job_id) or {}
            if st_after.get("status") not in ("completed", "failed"):
                # Terminal-message contract: prefer a runner-provided
                # `message` in the return dict; otherwise fall back to a
                # bland default.  Pre-2026-04-25 we read whatever
                # `message` was on the latest in-progress
                # `set_job_status` call, which left every Activity row
                # stuck on a stale running line ("Recomputing author
                # centroid", "Fetching SPECTER2 vectors for 4 papers"…)
                # even though the job had moved on.  Runners that want a
                # specific terminal message return
                # `{..., "message": "Refreshed N → M new"}`.
                done_message: Optional[str] = None
                if isinstance(result, dict):
                    candidate = result.get("message")
                    if isinstance(candidate, str) and candidate.strip():
                        done_message = candidate.strip()
                if not done_message:
                    done_message = f"Completed: {job_id}"
                payload = {
                    "status": "completed",
                    "finished_at": datetime.utcnow().isoformat(),
                    "message": done_message,
                }
                if isinstance(result, dict):
                    payload["result"] = result
                set_job_status(job_id, **payload)
        except Exception as exc:
            logger.exception("Immediate job %s failed: %s", job_id, exc)
            set_job_status(
                job_id,
                status="failed",
                finished_at=datetime.utcnow().isoformat(),
                error=str(exc),
                message=f"Failed: {job_id}",
            )
            raise

    if os.getenv("PYTEST_CURRENT_TEST"):
        logger.info("Running immediate job %s inline under pytest", job_id)
        _wrapped()
        return True

    sched.add_job(
        _wrapped,
        trigger=DateTrigger(run_date=datetime.now()),
        id=job_id,
        replace_existing=True,
    )
    logger.info("Scheduled immediate job %s", job_id)
    return True


def schedule_alert(
    alert_id: str,
    schedule: str,
    schedule_config: Optional[dict],
    evaluate_func: Callable,
) -> Optional[str]:
    """Register or update a scheduled job for an alert.

    Args:
        alert_id: Alert ID to use as job ID prefix.
        schedule: Schedule type -- 'daily', 'weekly', 'manual', or 'immediate'.
        schedule_config: Optional configuration dict, e.g.
            {"day": "monday", "time": "09:00"} for weekly, or
            {"time": "09:00"} for daily.
        evaluate_func: Callable to evaluate the alert (called with alert_id).

    Returns:
        The job ID if a scheduled job was created, or None if no job is needed.
    """
    job_id = f"alert_{alert_id}"
    sched = get_scheduler()

    # Remove existing job if any
    try:
        sched.remove_job(job_id)
        with _job_lock:
            _job_meta.pop(job_id, None)
    except Exception:
        pass

    if schedule in ("manual", "immediate"):
        return None

    if schedule == "daily":
        time_str = (schedule_config or {}).get("time", "09:00")
        hour, minute = time_str.split(":")
        cron_expr = f"{minute} {hour} * * *"
    elif schedule == "weekly":
        day = (schedule_config or {}).get("day", "monday")[:3].lower()
        time_str = (schedule_config or {}).get("time", "09:00")
        hour, minute = time_str.split(":")
        day_map = {
            "mon": "MON", "tue": "TUE", "wed": "WED",
            "thu": "THU", "fri": "FRI", "sat": "SAT", "sun": "SUN",
        }
        day_abbr = day_map.get(day, "MON")
        cron_expr = f"{minute} {hour} * * {day_abbr}"
    else:
        return None

    add_cron_job(
        job_id, cron_expr, evaluate_func, args=(alert_id,),
        meta={"action": "alert_evaluate", "name": f"Alert {alert_id}"},
    )
    logger.info("Scheduled alert %s with schedule=%s, cron=%s", alert_id, schedule, cron_expr)
    return job_id


def reschedule_discovery_refresh(interval_hours: int) -> None:
    """Update or remove the periodic recommendation refresh job.

    Args:
        interval_hours: Refresh interval in hours. 0 = disabled.
    """
    sched = get_scheduler()

    # Remove existing job if present
    try:
        sched.remove_job("refresh_recommendations")
    except Exception:
        pass
    with _job_lock:
        _job_meta.pop("refresh_recommendations", None)

    if interval_hours > 0:
        sched.add_job(
            refresh_recommendations_periodic,
            trigger=IntervalTrigger(hours=interval_hours),
            id="refresh_recommendations",
            name="Periodic recommendation refresh",
            replace_existing=True,
        )
        with _job_lock:
            _job_meta["refresh_recommendations"] = {
                "action": "refresh_recommendations",
                "name": "Periodic recommendation refresh",
                "description": f"Refreshes discovery recommendations every {interval_hours}h",
            }
        logger.info(
            "Rescheduled refresh_recommendations job (interval=%dh)", interval_hours,
        )
    else:
        logger.info("Discovery recommendation refresh disabled")


def reschedule_citation_graph_maintenance(interval_hours: int) -> None:
    """Update or remove the periodic citation graph maintenance job."""
    sched = get_scheduler()

    try:
        sched.remove_job("maintain_citation_graph")
    except Exception:
        pass
    with _job_lock:
        _job_meta.pop("maintain_citation_graph", None)

    if interval_hours > 0:
        sched.add_job(
            maintain_citation_graph_periodic,
            trigger=IntervalTrigger(hours=interval_hours),
            id="maintain_citation_graph",
            name="Periodic citation graph maintenance",
            replace_existing=True,
        )
        with _job_lock:
            _job_meta["maintain_citation_graph"] = {
                "action": "maintain_citation_graph",
                "name": "Periodic citation graph maintenance",
                "description": f"Backfills missing publication references every {interval_hours}h",
            }
        logger.info(
            "Rescheduled maintain_citation_graph job (interval=%dh)", interval_hours,
        )
    else:
        logger.info("Citation graph maintenance disabled")


def schedule_embedding_computation(job_id: str) -> None:
    """Run embedding computation via the shared worker."""
    from alma.services.embeddings import run_embedding_computation

    logger.info("Starting embedding computation job %s", job_id)
    run_embedding_computation(
        job_id,
        set_job_status=set_job_status,
        add_job_log=add_job_log,
        is_cancellation_requested=is_cancellation_requested,
    )
