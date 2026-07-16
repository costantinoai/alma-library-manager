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
import collections
import json
import logging
import os
import sqlite3
import threading
import time
from collections.abc import Callable
from datetime import datetime, timedelta, timezone

from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.date import DateTrigger
from apscheduler.triggers.interval import IntervalTrigger

from alma.core.concurrency import enter_job_fanout
from alma.core.db_retry import commit_with_retry
from alma.core.redaction import redact_sensitive_data, redact_sensitive_text

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Module-level state
# ---------------------------------------------------------------------------

_scheduler: BackgroundScheduler | None = None
_job_meta: dict[str, dict] = {}
_job_status: dict[str, dict] = {}
_job_logs: dict[str, collections.deque[dict]] = {}
# Maps job_id -> threading.get_ident() of the worker currently running it.
# Populated by `_register_running_thread` at the top of `_wrapped` and cleared
# in the matching `finally`. Used by `kill_job_thread` to inject JobCancelled
# straight into the thread instead of waiting for a cooperative checkpoint.
_job_threads: dict[str, int] = {}
_job_lock = threading.RLock()
_ACTIVITY_STATUS_LIMIT = 2000
_ACTIVE_STATUSES = {"queued", "scheduled", "running", "cancelling"}
_TERMINAL_STATUSES = {"completed", "failed", "cancelled"}
_ORPHAN_REAP_MESSAGE = "Orphaned across process restart; auto-cancelled"
_ORPHAN_REAP_ERROR = (
    "Backend process restarted or exited while this job was running; "
    "ALMa marked the abandoned worker as cancelled on the next startup. "
    "This is not a job-level exception. Check backend process logs around "
    "the finish time for the restart, reload, crash, or container-stop cause."
)


class JobCancelledError(Exception):
    """Raised at a cooperative Activity checkpoint after user cancellation."""


# Backward-compatible public name used by existing plugins and tests.
JobCancelled = JobCancelledError


def _register_running_thread(job_id: str) -> None:
    """Record the calling thread as the executor of `job_id`."""
    if not job_id:
        return
    with _job_lock:
        _job_threads[job_id] = threading.get_ident()


def _unregister_running_thread(job_id: str) -> None:
    """Forget the executor thread for `job_id` once the run finishes."""
    if not job_id:
        return
    with _job_lock:
        _job_threads.pop(job_id, None)


def kill_job_thread(job_id: str) -> bool:
    """Inject ``JobCancelled`` into the thread running ``job_id``.

    This is the hard-kill backstop for the cooperative cancellation path.
    APScheduler runs jobs in Python threads, so we cannot ``SIGKILL`` them;
    the closest equivalent is ``PyThreadState_SetAsyncExc``, which raises
    the given exception in the target thread at the next Python bytecode
    boundary. Threads stuck in C-level blocking I/O (sockets without a
    timeout, blocking C extensions) will still only react when control
    returns to Python — but for any pure-Python loop or checkpoint-aware
    runner, this turns "cancelling" into an effectively immediate kill.

    Returns True if the exception was scheduled successfully.
    """
    import ctypes

    with _job_lock:
        thread_ident = _job_threads.get(job_id)
    if thread_ident is None:
        return False
    try:
        res = ctypes.pythonapi.PyThreadState_SetAsyncExc(
            ctypes.c_ulong(thread_ident),
            ctypes.py_object(JobCancelled),
        )
    except Exception as exc:
        logger.warning("kill_job_thread: ctypes call failed for %s: %s", job_id, exc)
        return False
    if res == 0:
        # The thread has already exited; nothing to do.
        with _job_lock:
            _job_threads.pop(job_id, None)
        return False
    if res > 1:
        # If more than one thread was hit, the docs say to immediately
        # clear the async exc to avoid leaving threads in an
        # inconsistent state. We treat this as a failure.
        ctypes.pythonapi.PyThreadState_SetAsyncExc(
            ctypes.c_ulong(thread_ident), ctypes.c_void_p(None)
        )
        logger.warning(
            "kill_job_thread: PyThreadState_SetAsyncExc affected %d threads for %s",
            res,
            job_id,
        )
        return False
    logger.info(
        "kill_job_thread: injected JobCancelled into thread %s for job %s",
        thread_ident,
        job_id,
    )
    return True


def _activity_conn() -> sqlite3.Connection:
    """Open a connection to the unified DB for durable activity persistence."""
    from alma.config import get_db_path

    # Activity status/log writes are lightweight but must not vanish under
    # write contention: a 250ms timeout (the old value) dropped job-status
    # rows whenever a foreground transaction held the writer, leaving stuck
    # "in progress" pills AND emitting "database is locked" log noise. 5s is
    # still short enough to stay responsive but rides out a normal burst.
    conn = sqlite3.connect(str(get_db_path()), check_same_thread=False, timeout=5.0)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA busy_timeout=5000")
    # BEGIN IMMEDIATE for every Activity WRITE (task-29 §8.2 "Activity writes
    # bypass the writer gate"). Activity status/log persistence used the default
    # DEFERRED transaction: under WAL a DEFERRED txn starts as a reader and, if
    # another connection already holds the write lock, FAILS the upgrade
    # instantly with "database is locked" instead of waiting — which dropped
    # status/log rows under a write burst (the "only 1 of 8 simultaneous actions
    # logged" symptom). IMMEDIATE acquires the write lock up front, so concurrent
    # Activity + foreground writers serialize via busy_timeout (5 s) rather than
    # racing the upgrade. Read paths through this connection are unaffected:
    # sqlite3 only emits an implicit BEGIN before DML, never before SELECT, so a
    # read-only use never takes the write lock.
    conn.isolation_level = "IMMEDIATE"
    return conn


def _json_dumps_safe(value: object) -> str:
    try:
        return json.dumps(value, ensure_ascii=False, default=str)
    except Exception:
        return json.dumps(str(value), ensure_ascii=False)


def _parse_activity_time(value: object) -> datetime | None:
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
    raw = f"{updated_at}|{job_id}".encode()
    return base64.urlsafe_b64encode(raw).decode("ascii")


def _decode_status_cursor(cursor: str | None) -> tuple[str, str] | None:
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


def _decode_log_cursor(cursor: str | None) -> int | None:
    if not cursor:
        return None
    try:
        raw = base64.urlsafe_b64decode(cursor.encode("ascii")).decode("utf-8")
        return int(raw.strip())
    except Exception:
        return None


def get_job_trigger_source(job_id: str) -> str | None:
    """Return the recorded ``trigger_source`` for ``job_id``, if any.

    Used by chain coordinators to decide whether to auto-queue the
    next stage. A value of ``"user"`` means the job was kicked off by
    a manual Settings click; the per-button contract there is "do
    exactly what the label says," so callers should NOT auto-chain on
    that source. ``"auto:..."`` and ``"scheduler"`` are unattended
    triggers where chaining is the expected continuation.
    """
    if not job_id:
        return None
    with _job_lock:
        cached = _job_status.get(job_id)
        if cached and cached.get("trigger_source"):
            return str(cached.get("trigger_source"))
    row = _load_job_status_from_db(job_id)
    if row and row.get("trigger_source"):
        return str(row.get("trigger_source"))
    return None


def _load_job_status_from_db(job_id: str) -> dict | None:
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
    before_id: int | None = None,
) -> tuple[list[dict], str | None, bool] | None:
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


def _schedule_flag_enabled(key: str, default: bool = False) -> bool:
    """Read a boolean opt-in flag from the discovery_settings KV store.

    Mirrors `_discovery_schedule_interval_hours` for the `*_enabled` toggles
    that gate the opt-in auto-refresh jobs. Values are stored as the strings
    "true"/"false"; anything else falls back to `default`.
    """
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
            return str(row["value"]).strip().lower() in {"1", "true", "yes", "on"}
        finally:
            conn.close()
    except Exception:
        return default


def _register_interval_job(
    sched,
    *,
    job_id: str,
    func,
    name: str,
    description: str,
    enabled: bool,
    interval_hours: int,
) -> bool:
    """Register (or remove) an interval-triggered job from one place.

    The single seam for every opt-in periodic refresh: it removes any existing
    copy first (idempotent — safe to call both at startup and on a live
    settings change), then re-adds the job ONLY when it is opted in (`enabled`)
    AND has a positive interval. Returns True when the job is now scheduled,
    False when it is left disabled.
    """
    try:
        sched.remove_job(job_id)
    except Exception:
        pass
    with _job_lock:
        _job_meta.pop(job_id, None)

    if enabled and interval_hours > 0:
        sched.add_job(
            func,
            trigger=IntervalTrigger(hours=interval_hours),
            id=job_id,
            name=name,
            replace_existing=True,
        )
        with _job_lock:
            _job_meta[job_id] = {
                "action": job_id,
                "name": name,
                "description": description,
            }
        logger.info("Registered %s job (interval=%dh)", job_id, interval_hours)
        return True

    logger.info("%s disabled (auto-refresh opt-in OFF)", job_id)
    return False


# ---------------------------------------------------------------------------
# Scheduler lifecycle
# ---------------------------------------------------------------------------

def active_job_namespaces(
    conn: sqlite3.Connection, *, exclude_operation_key: str | None = None
) -> tuple[set[str], int]:
    """(distinct operation-key namespaces, total count) of currently-active jobs.

    The live input to `job_policy.admit_maintenance` — read-only over
    `operation_status`. The namespace is the first dotted/colon segment of the
    operation key (the same identity the policy catalog is keyed by).
    `exclude_operation_key` drops the caller's OWN op from the count so a running
    background sweep re-checking the gate doesn't count itself as "another op"."""
    try:
        rows = conn.execute(
            "SELECT operation_key FROM operation_status "
            "WHERE status IN ('queued', 'scheduled', 'running', 'cancelling')"
        ).fetchall()
    except sqlite3.OperationalError:
        return set(), 0
    exclude = str(exclude_operation_key or "")
    namespaces: set[str] = set()
    total = 0
    for row in rows:
        key = str((row["operation_key"] if isinstance(row, sqlite3.Row) else row[0]) or "")
        if not key or (exclude and key == exclude):
            continue
        total += 1
        namespaces.add(key.split(".", 1)[0].split(":", 1)[0])
    return namespaces, total


def may_background_run(
    conn: sqlite3.Connection, *, exclude_operation_key: str | None = None
) -> tuple[bool, str]:
    """The single live gate for "may a BACKGROUND health/maintenance op run NOW?"
    (task 37 A).

    Composes the live active-job count (excluding the caller's own op) with the
    in-memory idle clock, then defers to the pure `admit_maintenance` policy.
    Used at BOTH ends of a background op's life: the idle-maintenance healer checks
    it before STARTING a sweep, and a running sweep re-checks it at each
    continuation boundary so it gracefully yields the moment the user does anything
    (pull-based pause — no per-route signalling needed). Returns ``(ok, reason)``.
    """
    from alma.core.job_policy import admit_maintenance
    from alma.core.user_activity import app_is_idle
    from alma.services.background_settings import get_idle_wait_seconds

    _, active_total = active_job_namespaces(
        conn, exclude_operation_key=exclude_operation_key
    )
    return admit_maintenance(
        active_total, app_idle=app_is_idle(get_idle_wait_seconds(conn))
    )


# Graceful-stop reasons a BACKGROUND sweep stamps when it yields mid-run (task 37
# A/C). BOTH are RETRYABLE: the op's unprocessed work stays in the eligibility
# pool and the idle-maintenance healer re-drains it once the app is idle again —
# never a terminal no-match. (Per the lessons rule "upstream rate limits are
# retryable states, not no-match evidence".)
BG_PAUSED_FOR_USER = "paused_for_user"
BG_CREDIT_LIMIT = "credit_limit"


# The one-shot enrichment kick scheduled by POST /onboarding/complete (audit
# 39 finding #4). The user is actively watching the wizard finish, so this
# trigger is USER-FACING (never yields to the idle gate / credit reserve and
# takes the fast adaptive source order) — but it is deliberately NOT the
# literal "user": the chain hooks suppress on == "user" exactly, so the
# onboarding kick still auto-chains metadata → S2 vectors → local fill.
ONBOARDING_KICK_REASON = "onboarding_complete"
ONBOARDING_KICK_TRIGGER = f"auto:{ONBOARDING_KICK_REASON}"


def is_user_facing_trigger(trigger_source: str | None) -> bool:
    """True when a user is actively waiting on this run: a manual Settings
    click ("user") or the onboarding-complete kick. User-facing runs never
    yield to the idle gate, ignore the background credit reserve, and pick
    the fast (paid OpenAlex-first) adaptive source order."""
    value = str(trigger_source or "").strip().lower()
    return value == "user" or value == ONBOARDING_KICK_TRIGGER


def is_background_trigger(trigger_source: str | None) -> bool:
    """A background trigger is anything not user-facing.

    Only background ops yield to the user / honour the credit reserve; a
    user-facing op (see `is_user_facing_trigger`) runs to completion and may
    use the full remaining provider quota.
    """
    return not is_user_facing_trigger(trigger_source)


def background_yield_reason(
    conn: sqlite3.Connection,
    operation_key: str,
    *,
    trigger_source: str | None,
    budget_source: str = "openalex",
) -> tuple[str, str] | None:
    """Should a running BACKGROUND sweep stop NOW, and why? (task 37 A pause + C abort).

    Returns ``None`` to keep going, else ``(reason_code, human_message)``:
    - ``BG_PAUSED_FOR_USER`` — another operation is active or the user is active
      (app not idle): the sweep yields so it never competes with the user.
    - ``BG_CREDIT_LIMIT`` — the provider's live remaining quota is at/below the
      user reserve: the sweep stops before eating into the user's headroom.

    Both leave pending work retryable. A user-initiated run (``trigger_source ==
    'user'``) never yields — checked first, so this is a cheap no-op there. The
    single tripwire a sweep feeds into its ``is_cancelled`` callback (so "start"
    via `may_background_run` and "keep going" use identical rules).
    """
    if not is_background_trigger(trigger_source):
        return None
    ok, reason = may_background_run(conn, exclude_operation_key=operation_key)
    if not ok:
        return (BG_PAUSED_FOR_USER, f"Paused for user activity ({reason}); will resume when idle")
    from alma.core.http_sources import provider_budget_ok
    from alma.services.background_settings import get_reserved_api_calls

    reserve = get_reserved_api_calls(conn)
    if not provider_budget_ok(budget_source, reserve=reserve):
        return (
            BG_CREDIT_LIMIT,
            f"Stopped: {budget_source} quota near its limit "
            f"(reserving {reserve} calls for your manual operations)",
        )
    return None


def make_background_cancel_check(
    conn: sqlite3.Connection,
    job_id: str,
    operation_key: str,
    original: Callable[[str], bool],
    *,
    trigger_source: str | None,
    sink: dict,
    budget_source: str = "openalex",
    min_interval_s: float = 2.0,
) -> Callable[[], bool]:
    """Build the ``is_cancelled`` callable a BACKGROUND sweep feeds its pipeline
    (task 37 A/C) — the ONE shared seam both the title-resolution and
    corpus-rehydrate runners use, so the pause/credit-limit behaviour is DRY.

    Combines the user-cancel probe (``original`` — checked EVERY call, immediate)
    with the background-yield tripwire (`background_yield_reason`), the latter
    THROTTLED to at most once per ``min_interval_s`` because it touches the DB
    (a small `operation_status` read) + the provider budget, and a pipeline probes
    is_cancelled on a tight loop. On a yield it records ``(reason, message)`` into
    ``sink`` and stays sticky-True so the sweep tears down promptly. A
    user-triggered run never yields (`background_yield_reason` no-ops on it), so
    this degrades to a plain cancel probe there.
    """
    state = {"last": 0.0, "yielding": False}

    def _check() -> bool:
        try:
            if original(job_id):
                return True
        except Exception:  # a cancel probe must never crash the sweep
            pass
        if state["yielding"]:
            return True
        now = time.monotonic()
        if now - state["last"] >= float(min_interval_s):
            state["last"] = now
            info = background_yield_reason(
                conn, operation_key, trigger_source=trigger_source, budget_source=budget_source
            )
            if info is not None:
                sink["reason"], sink["message"] = info
                state["yielding"] = True
                return True
        return False

    return _check


def _scheduler_max_workers() -> int:
    """Concurrent background-job cap, env-overridable, clamped to [1, 16].

    SQLite is single-writer: APScheduler's default ThreadPoolExecutor of 10
    let a burst of user actions (each follow chains backfill → corpus
    rehydrate → s2 → embeddings) run ~10 write-heavy jobs at once, which
    monopolised the writer and made foreground writes (dismiss/follow/merge)
    fail with "database is locked". Capping the pool keeps the writer
    available for the user; background work just queues and catches up.
    """
    raw = os.getenv("ALMA_SCHEDULER_WORKERS", "").strip()
    if not raw:
        return 5
    try:
        return max(1, min(16, int(raw)))
    except ValueError:
        return 5


def get_scheduler() -> BackgroundScheduler:
    """Return the global scheduler, creating and starting it if needed."""
    global _scheduler
    if _scheduler is None:
        from apscheduler.executors.pool import ThreadPoolExecutor as APThreadPoolExecutor

        workers = _scheduler_max_workers()
        _scheduler = BackgroundScheduler(
            executors={"default": APThreadPoolExecutor(max_workers=workers)},
            # max_instances=1 + coalesce stop periodic sweeps from piling up
            # multiple concurrent copies of themselves (another writer-lane
            # multiplier); misfire_grace_time keeps a delayed run valid.
            job_defaults={
                "max_instances": 1,
                "coalesce": True,
                "misfire_grace_time": 3600,
            },
        )
        _scheduler.start()
        logger.info("Background scheduler started (max_workers=%d)", workers)
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
            stale_jobs = [
                _row_to_status(row)
                for row in rows
                if _is_stale_active_status(_row_to_status(row), stale_after_seconds)
            ]
            if stale_jobs:
                now = datetime.utcnow().isoformat()
                conn.executemany(
                    """
                    UPDATE operation_status
                    SET status = 'cancelled',
                        finished_at = COALESCE(finished_at, ?),
                        updated_at = ?,
                        message = ?,
                        error = COALESCE(error, ?),
                        cancel_requested = 0
                    WHERE job_id = ?
                    """,
                    [
                        (now, now, _ORPHAN_REAP_MESSAGE, _ORPHAN_REAP_ERROR, str(job["job_id"]))
                        for job in stale_jobs
                    ],
                )
                conn.executemany(
                    """
                    INSERT INTO operation_logs (job_id, timestamp, level, step, message, data_json)
                    VALUES (?, ?, ?, ?, ?, ?)
                    """,
                    [
                        (
                            str(job["job_id"]),
                            now,
                            "ERROR",
                            "orphan_reaped",
                            _ORPHAN_REAP_ERROR,
                            _json_dumps_safe(
                                {
                                    "status": "cancelled",
                                    "reason": "process_restart_or_exit",
                                    "message": _ORPHAN_REAP_MESSAGE,
                                    "previous_status": job.get("status"),
                                    "previous_updated_at": job.get("updated_at"),
                                    "operation_key": job.get("operation_key"),
                                    "trigger_source": job.get("trigger_source"),
                                    "stale_after_seconds": stale_after_seconds,
                                }
                            ),
                        )
                        for job in stale_jobs
                    ],
                )
            reaped = len(stale_jobs)
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

    # -- Discovery recommendation refresh (interval, opt-in) --------------
    # Runs only when the page/Settings toggle (schedule.refresh_enabled) is on
    # AND the interval is > 0. Default OFF.
    refresh_hours = _discovery_schedule_interval_hours("schedule.refresh_interval_hours", 6)
    _register_interval_job(
        sched,
        job_id="refresh_recommendations",
        func=refresh_recommendations_periodic,
        name="Periodic recommendation refresh",
        description=f"Refreshes discovery recommendations every {refresh_hours}h",
        enabled=_schedule_flag_enabled("schedule.refresh_enabled"),
        interval_hours=refresh_hours,
    )

    # -- Feed inbox refresh (interval, opt-in) ----------------------------
    # Symmetric to discovery: gated on schedule.feed_refresh_enabled +
    # schedule.feed_refresh_interval_hours. Default OFF.
    feed_refresh_hours = _discovery_schedule_interval_hours(
        "schedule.feed_refresh_interval_hours", 6
    )
    _register_interval_job(
        sched,
        job_id="refresh_feed_inbox",
        func=refresh_feed_inbox_periodic,
        name="Periodic feed refresh",
        description=f"Refreshes the feed inbox every {feed_refresh_hours}h",
        enabled=_schedule_flag_enabled("schedule.feed_refresh_enabled"),
        interval_hours=feed_refresh_hours,
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

    # -- Idle maintenance healer (interval) -------------------------------
    # Repairs opted-in health dimensions in the background (task 24, Pillar 2).
    # Default OFF per task (toggle on the Health page); ALMA_DISABLE_IDLE_
    # MAINTENANCE is a global hard kill. Low cadence; each tick repairs at most
    # one task with a small batch, bounded by that task's daily cap.
    from alma.services.maintenance import maintenance_repair_periodic

    try:
        idle_interval_hours = max(1, int(os.getenv("ALMA_IDLE_MAINTENANCE_INTERVAL_HOURS", "1")))
    except (TypeError, ValueError):
        idle_interval_hours = 1
    sched.add_job(
        maintenance_repair_periodic,
        trigger=IntervalTrigger(hours=idle_interval_hours),
        id="maintenance_repair",
        name="Idle maintenance healer",
        replace_existing=True,
    )
    with _job_lock:
        _job_meta["maintenance_repair"] = {
            "action": "maintenance_repair",
            "name": "Idle maintenance healer",
            "description": (
                f"Repairs opted-in health tasks every {idle_interval_hours}h, "
                "within per-task daily caps (default OFF)"
            ),
        }
    logger.info("Registered maintenance_repair job (interval=%dh)", idle_interval_hours)

    # -- Hydration ledger drain (interval) --------------------------------
    # Restart-recovery + residual drain for the durable coalescing dispatcher
    # (task-29 Checkpoint D). Re-schedules the coalescing hydration sweeps when
    # the durable ledgers hold pending rows, so work enqueued before a restart
    # resumes under ONE dispatcher. Idempotent (find_active_job), low cadence.
    try:
        drain_interval_minutes = max(1, int(os.getenv("ALMA_HYDRATION_DRAIN_INTERVAL_MINUTES", "15")))
    except (TypeError, ValueError):
        drain_interval_minutes = 15
    sched.add_job(
        drain_pending_hydration_periodic,
        trigger=IntervalTrigger(minutes=drain_interval_minutes),
        id="hydration_drain",
        name="Pending hydration drain",
        replace_existing=True,
    )
    with _job_lock:
        _job_meta["hydration_drain"] = {
            "action": "hydration_drain",
            "name": "Pending hydration drain",
            "description": (
                f"Resumes the coalescing hydration dispatcher every "
                f"{drain_interval_minutes}m when durable ledger rows are pending"
            ),
        }
    logger.info("Registered hydration_drain job (interval=%dm)", drain_interval_minutes)

    # Resume self-rescheduling sweeps that the previous process orphaned
    # mid-run (the reaper above cancelled them but cannot re-arm their
    # continuation). Runs last, with the scheduler already started, so
    # `schedule_immediate` works. Orphan-only + idempotent; never breaks
    # startup. See `services.maintenance.resume_orphaned_sweeps` + tasks/11 A2.
    try:
        from alma.services.maintenance import resume_orphaned_sweeps

        resumed = resume_orphaned_sweeps()
        if resumed:
            logger.warning("Resumed %d orphaned self-rescheduling sweep(s)", resumed)
    except Exception as exc:
        logger.warning("Orphaned-sweep resume skipped: %s", exc)


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
        from alma.api.deps import open_db_connection
        from alma.application import alerts as alerts_app

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
                # evaluate_digest's writes (dedup / history / last_evaluated_at)
                # all run AFTER its search + Slack send, so the lock is never
                # held across network; commit_with_retry flushes them with
                # transient-lock retry on this own (scheduler) connection.
                commit_with_retry(conn, label="evaluate_scheduled_alerts")
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
        from alma.api.deps import open_db_connection
        from alma.application.discovery import (
            create_lens,
            list_lenses,
            refresh_lens_recommendations,
        )

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
                commit_with_retry(conn, label="refresh_recommendations_periodic bootstrap")
                lenses = [new_lens]
            except sqlite3.OperationalError as exc:
                logger.warning(
                    "Default lens bootstrap failed (%s); falling back to legacy discovery refresh",
                    exc,
                )
                from alma.config import get_db_path
                from alma.discovery.engine import DiscoveryEngine

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
                # refresh_lens_recommendations self-gates its lane writes; this
                # flushes any residual on the scheduler's own connection.
                commit_with_retry(conn, label="refresh_recommendations_periodic")
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

        # Maintenance home for the Library signal cache (43.2). The composite
        # depends on library-wide centroid/topic/feedback state that the reads
        # no longer recompute, so converge `papers.global_signal_score` here on
        # the idle scheduler. Best-effort — a failure never fails the refresh.
        try:
            from alma.application.paper_signal import recompute_library_signal_scores

            rescored = recompute_library_signal_scores(conn)
            if rescored:
                logger.info("Recomputed %d Library signal scores (periodic)", rescored)
        except Exception:
            logger.debug("periodic library signal recompute skipped", exc_info=True)

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


def refresh_feed_inbox_periodic() -> None:
    """Periodically refresh the feed inbox from active monitors.

    Mirrors the manual ``POST /feed/refresh`` background runner
    (``routes/feed.py``) but stamps ``trigger_source="scheduler"`` so the run
    is muted from toast spam while staying visible in Activity. Opt-in: only
    registered when feed auto-refresh is enabled (see ``reschedule_feed_refresh``
    / ``setup_scheduler``). Uses its own connection and the gather-then-write
    feed path, so it never holds the writer gate across network I/O.
    """
    job_id = "periodic_feed_refresh"
    operation_key = "feed.refresh_periodic"
    set_job_status(
        job_id,
        status="running",
        trigger_source="scheduler",
        operation_key=operation_key,
        started_at=datetime.utcnow().isoformat(),
        message="Refreshing feed inbox (periodic)",
    )
    logger.info("Starting periodic feed inbox refresh")
    try:
        from alma.api.deps import open_db_connection
        from alma.api.helpers import ActivityJobContext
        from alma.application import feed as feed_app

        conn = open_db_connection()
        try:
            result = feed_app.refresh_feed_inbox(conn, ctx=ActivityJobContext(job_id))
        finally:
            conn.close()

        items_created = int((result or {}).get("items_created") or 0)
        final_status = "noop" if items_created == 0 else "completed"
        final_message = (
            "No new papers found from active monitors"
            if items_created == 0
            else f"Added {items_created} new papers to feed inbox"
        )
        set_job_status(
            job_id,
            status=final_status,
            trigger_source="scheduler",
            operation_key=operation_key,
            finished_at=datetime.utcnow().isoformat(),
            message=final_message,
            result=result,
        )
        logger.info(
            "Periodic feed inbox refresh complete: %d items created", items_created
        )
    except Exception:
        logger.exception("Fatal error in refresh_feed_inbox_periodic")
        set_job_status(
            job_id,
            status="failed",
            trigger_source="scheduler",
            operation_key=operation_key,
            finished_at=datetime.utcnow().isoformat(),
            message="Periodic feed inbox refresh failed",
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
            # backfill_missing_publication_references self-gates its upsert window
            # (write_section); this flushes any residual on this own connection.
            commit_with_retry(conn, label="maintain_citation_graph_periodic")
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


def run_db_housekeeping(conn: sqlite3.Connection) -> dict[str, object]:
    """Prune stale ``operation_logs`` + incremental-vacuum on an OPEN connection.

    The single source of truth for the housekeeping pass, shared by the daily
    ``db_maintenance_periodic`` scheduler job and the on-demand ``housekeeping``
    maintenance task (task-29 Checkpoint C). Caller owns connection lifecycle
    and Activity status; this just does the two cheap steps and reports counts.
    """
    summary: dict[str, object] = {}
    # Operation log retention — committed in its own transaction so the VACUUM
    # below has clean ground.
    retention_days = _operation_log_retention_days()
    cutoff = (datetime.utcnow() - timedelta(days=retention_days)).isoformat()
    try:
        cur = conn.execute("DELETE FROM operation_logs WHERE timestamp < ?", (cutoff,))
        deleted = cur.rowcount
        conn.commit()
        summary["operation_logs_pruned"] = deleted
        summary["operation_logs_retention_days"] = retention_days
    except sqlite3.OperationalError as exc:
        logger.warning("operation_logs prune skipped: %s", exc)

    # Incremental vacuum runs in autocommit and releases all currently-free
    # pages. Cheap when there's little to free.
    try:
        free_before = conn.execute("PRAGMA freelist_count").fetchone()[0]
        if free_before:
            conn.isolation_level = None
            conn.execute("PRAGMA incremental_vacuum")
            free_after = conn.execute("PRAGMA freelist_count").fetchone()[0]
            summary["pages_freed"] = free_before - free_after
        else:
            summary["pages_freed"] = 0
    except sqlite3.OperationalError as exc:
        logger.warning("incremental_vacuum skipped: %s", exc)
    return summary


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
            summary = run_db_housekeeping(conn)
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


def drain_pending_hydration_periodic() -> None:
    """Restart-recovery + residual drain for the durable hydration ledgers
    (task-29 Checkpoint D — durable coalescing dispatcher).

    The paper/author enrichment ledgers are durable SQLite tables, so pending
    work survives a restart — but the insert hooks only SCHEDULE a coalescing
    sweep at insert time. Without this tick, rows enqueued just before a restart
    (or left pending after a bounded sweep) would sit idle until the next insert.
    This periodically re-schedules the COALESCING drains — idempotent via
    `find_active_job`, so it never spawns a second dispatcher when one is live —
    whenever pending rows exist, guaranteeing one dispatcher resumes the durable
    queue. Cheap: two COUNT(*) reads, then at most two idempotent schedule calls.
    """
    from alma.api.deps import open_db_connection

    try:
        conn = open_db_connection()
    except Exception:
        return
    try:
        def _pending(table: str) -> int:
            try:
                return int(
                    conn.execute(
                        f"SELECT COUNT(*) FROM {table} WHERE status = 'pending'"
                    ).fetchone()[0]
                    or 0
                )
            except sqlite3.OperationalError:
                return 0

        # 41.4: retire pending ledger rows for papers that have nothing left to
        # hydrate BEFORE counting, so a fully-satisfied ledger stops scheduling
        # zero-work sweeps (which also hold the "another op is active" slot and
        # starve real background ops).
        try:
            from alma.services.corpus_rehydrate import reconcile_satisfied_pending

            reconcile_satisfied_pending(conn)
        except Exception:
            logger.exception("pending-ledger reconcile failed")

        papers_pending = _pending("paper_enrichment_status")
        authors_pending = _pending("author_enrichment_status")
    finally:
        conn.close()

    if papers_pending:
        try:
            from alma.services.corpus_rehydrate import schedule_pending_hydration_sweep

            schedule_pending_hydration_sweep(reason="restart_drain")
        except Exception:
            logger.exception("restart hydration drain (papers) failed to schedule")
    if authors_pending:
        try:
            from alma.services.author_hydrate import schedule_pending_author_hydration_sweep

            schedule_pending_author_hydration_sweep(reason="restart_drain")
        except Exception:
            logger.exception("restart hydration drain (authors) failed to schedule")

    # Re-arm a deferred post-hydration vector chain (41.2). A background metadata
    # run that yielded on `paused_for_user` / `credit_limit` skips its S2-vector
    # chain hook; the metadata sweep resumes off the durable ledger, but the S2
    # chain has no durable pending rows and would otherwise never run. Fire it
    # here — but only once idle admission passes, which naturally holds only
    # after the metadata sweeps above have drained (an active sweep counts as
    # activity), so S2 runs AFTER metadata, never in competition with it.
    try:
        from alma.core.db_write import run_write_unit
        from alma.services.embedding_chain import (
            clear_post_hydration_chain_pending,
            is_post_hydration_chain_pending,
            schedule_post_hydration_chain,
        )

        conn2 = open_db_connection()
        try:
            if is_post_hydration_chain_pending(conn2):
                ok, _reason = may_background_run(conn2)
                if ok:
                    chain = schedule_post_hydration_chain(
                        conn2, trigger_reason="chain_rearm"
                    )
                    # Duty discharged when we armed the S2 fetch OR there is
                    # nothing left to chain — clear the marker either way.
                    if chain.get("scheduled_jobs") or chain.get("skipped") == "no_candidates":
                        run_write_unit(
                            conn2,
                            lambda: clear_post_hydration_chain_pending(conn2),
                            label="clear post-hydration chain pending",
                        )
        finally:
            conn2.close()
    except Exception:
        logger.exception("post-hydration chain re-arm failed")


# ===================================================================
# Internal helpers for alert evaluation
# ===================================================================

def _is_due(
    *,
    schedule: str,
    schedule_config: dict | None,
    last_evaluated_at: str | None,
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
    meta: dict | None = None,
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


def get_job_status(job_id: str) -> dict | None:
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


def find_active_job(operation_key: str) -> dict | None:
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


def _raise_if_cancel_checkpoint(job_id: str, *, step: str | None = None) -> None:
    """Stop cooperative runners at the next Activity checkpoint.

    Graceful stops (``cancel_mode == "graceful"``) never raise here: the
    runner keeps control and exits on its own at the next
    ``is_cancellation_requested`` loop boundary, so in-flight work is
    finished and committed instead of aborted mid-unit."""
    if not job_id:
        return
    allowed_steps = {"status", "cancel_requested", "cancelled"}
    if step in allowed_steps:
        return
    st = get_job_status(job_id) or {}
    if str(st.get("cancel_mode") or "hard") == "graceful":
        return
    if st.get("cancel_requested") and str(st.get("status") or "").lower() not in _TERMINAL_STATUSES:
        raise JobCancelled(str(st.get("message") or "Operation cancelled"))


def add_job_log(
    job_id: str,
    message: str,
    *,
    level: str = "INFO",
    step: str | None = None,
    data: dict | None = None,
) -> None:
    """Append a structured log entry for a job."""
    _raise_if_cancel_checkpoint(job_id, step=step)
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
    cursor: str | None = None,
) -> tuple[list[dict], str | None, bool]:
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
        if not status:
            db_status = _load_job_status_from_db(job_id)
            if db_status:
                status = dict(db_status)
        existing_status = str(status.get("status") or "").lower()
        incoming_status = str(kwargs.get("status") or "").lower() if "status" in kwargs else ""
        if existing_status in _TERMINAL_STATUSES and incoming_status in _ACTIVE_STATUSES:
            logger.debug(
                "Ignoring active-status regression for terminal job %s (%s -> %s)",
                job_id,
                existing_status,
                incoming_status,
            )
            return
        incoming_cancel_request = bool(kwargs.get("cancel_requested"))
        cancellation_requested = bool(status.get("cancel_requested") or incoming_cancel_request)
        if cancellation_requested and existing_status not in _TERMINAL_STATUSES:
            if (
                status.get("cancel_requested")
                and not incoming_cancel_request
                and incoming_status in {"queued", "scheduled", "running"}
                # Graceful stops never abort the runner from inside a status
                # write — the runner exits at its own loop boundary instead.
                and str(status.get("cancel_mode") or "hard") != "graceful"
            ):
                raise JobCancelled(str(status.get("message") or "Operation cancelled"))
            kwargs["cancel_requested"] = True
            if incoming_status in {"queued", "scheduled", "running"}:
                kwargs["status"] = "cancelling"
                kwargs.setdefault("message", "Cancellation requested; stopping at next checkpoint")
            elif incoming_status == "completed":
                kwargs["status"] = "cancelled"
                kwargs.setdefault("finished_at", datetime.utcnow().isoformat())
                kwargs.setdefault("message", "Operation cancelled")
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
    status: str | None = None,
    trigger_source: str | None = None,
    since: str | None = None,
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
    status: str | None = None,
    trigger_source: str | None = None,
    since: str | None = None,
    limit: int = 200,
    cursor: str | None = None,
) -> tuple[list[dict], str | None, bool]:
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
    next_cursor: str | None = None
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
    operation_key: str | None = None,
    message: str | None = None,
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
        # Register the worker thread *before* anything else so a concurrent
        # `kill_job_thread(job_id)` can inject JobCancelled even if the job
        # is still in the pre-run set_job_status / DB roundtrip below.
        _register_running_thread(job_id)
        # Bound this job's nested fan-out (any ThreadPoolExecutor it spawns) to
        # its policy budget for the duration of the runner. Visible only to
        # `core.concurrency.bounded_thread_pool` calls on this worker thread, so
        # interactive request-path fan-out is untouched. Resolved from the job's
        # own operation_key — fail-open (no clamp) when unclassified.
        operation_key = (get_job_status(job_id) or {}).get("operation_key")
        try:
            with enter_job_fanout(operation_key):
                _run_job_body()
        except JobCancelled:
            set_job_status(
                job_id,
                status="cancelled",
                cancel_requested=True,
                finished_at=datetime.utcnow().isoformat(),
                message="Operation cancelled",
                result={"success": False, "cancelled": True},
            )
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
        finally:
            _unregister_running_thread(job_id)

    def _run_job_body():
        # The actual run, executed inside the fan-out budget context above.
        # Exceptions propagate to _wrapped (JobCancelled / failure handling +
        # thread unregister live there, around the budget context).
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
        result = func(*args, **kwargs)
        st_after = get_job_status(job_id) or {}
        if st_after.get("cancel_requested") or st_after.get("status") in ("cancelling", "cancelled"):
            cancel_result = {
                "success": False,
                "cancelled": True,
                "message": "Operation cancelled",
            }
            if isinstance(result, dict):
                cancel_result.update(result)
                cancel_result["cancelled"] = True
                cancel_result["success"] = False
            set_job_status(
                job_id,
                status="cancelled",
                cancel_requested=True,
                finished_at=datetime.utcnow().isoformat(),
                message="Operation cancelled",
                result=cancel_result,
            )
        elif st_after.get("status") not in ("completed", "failed"):
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
            done_message: str | None = None
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

    if os.getenv("PYTEST_CURRENT_TEST"):
        # Tests normally run immediate jobs inline so a single call can exercise
        # the whole chain. Save/insert-contract tests opt OUT with
        # ALMA_TEST_DEFER_JOBS=1: the job is left queued (exactly as a
        # backgrounded production run would be at this instant) instead of
        # running synchronously in the request thread — which otherwise fans out
        # the hydration chain inline (slow + SQLite write contention). This is
        # test-execution config, not a mock: nothing about the job is faked, it
        # simply isn't run yet, matching prod's async scheduler.
        if os.getenv("ALMA_TEST_DEFER_JOBS") == "1":
            logger.info("Deferring immediate job %s (ALMA_TEST_DEFER_JOBS)", job_id)
            return True
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
    schedule_config: dict | None,
    evaluate_func: Callable,
) -> str | None:
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


def reschedule_discovery_refresh(enabled: bool, interval_hours: int) -> None:
    """Update or remove the periodic recommendation refresh job.

    The job runs only when auto-refresh is opted in (`enabled`) AND the
    interval is > 0. Called live from the discovery-settings PUT handler so a
    toggle/interval change takes effect without a restart.

    Args:
        enabled: Whether discovery auto-refresh is opted in.
        interval_hours: Refresh interval in hours. 0 = disabled.
    """
    _register_interval_job(
        get_scheduler(),
        job_id="refresh_recommendations",
        func=refresh_recommendations_periodic,
        name="Periodic recommendation refresh",
        description=f"Refreshes discovery recommendations every {interval_hours}h",
        enabled=enabled,
        interval_hours=interval_hours,
    )


def reschedule_feed_refresh(enabled: bool, interval_hours: int) -> None:
    """Update or remove the periodic feed-inbox refresh job.

    Symmetric to `reschedule_discovery_refresh`. The job runs only when feed
    auto-refresh is opted in (`enabled`) AND the interval is > 0. Called live
    from the feed-settings PUT handler.

    Args:
        enabled: Whether feed auto-refresh is opted in.
        interval_hours: Refresh interval in hours. 0 = disabled.
    """
    _register_interval_job(
        get_scheduler(),
        job_id="refresh_feed_inbox",
        func=refresh_feed_inbox_periodic,
        name="Periodic feed refresh",
        description=f"Refreshes the feed inbox every {interval_hours}h",
        enabled=enabled,
        interval_hours=interval_hours,
    )


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
