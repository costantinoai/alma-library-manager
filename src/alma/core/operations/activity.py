"""Persistence helpers for operation lifecycle and logs."""

from __future__ import annotations

import json
import logging
import sqlite3
import uuid
from typing import Any

from alma.core.time import utcnow

from .models import OperationContext

logger = logging.getLogger(__name__)


def _json_dumps(value: Any) -> str:
    try:
        return json.dumps(value, ensure_ascii=False, default=str)
    except Exception:
        return json.dumps(str(value), ensure_ascii=False)


def persist_operation_status(
    db: sqlite3.Connection,
    ctx: OperationContext,
    *,
    processed: int | None = None,
    total: int | None = None,
    current_author: str | None = None,
    cancel_requested: bool = False,
    metadata: dict[str, Any] | None = None,
) -> None:
    """Upsert one lifecycle snapshot into operation_status."""
    error_value: str | None = None
    if ctx.error is not None:
        error_value = _json_dumps(ctx.error)

    result_json = _json_dumps(ctx.result) if ctx.result is not None else None
    metadata_json = _json_dumps(metadata) if metadata else None

    db.execute(
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
            ctx.operation_id,
            ctx.status,
            ctx.message,
            error_value,
            ctx.started_at,
            ctx.finished_at,
            utcnow().isoformat(),
            processed,
            total,
            current_author,
            ctx.operation_key,
            ctx.trigger_source,
            1 if cancel_requested else 0,
            result_json,
            metadata_json,
        ),
    )


def record_foreground_action(
    db: sqlite3.Connection,
    *,
    operation_key: str,
    message: str,
    result: dict[str, Any] | None = None,
    job_id: str | None = None,
    status: str = "completed",
) -> str:
    """Record ONE Activity entry for a synchronous foreground action.

    A user action that runs inside the request (no background-job envelope —
    an author "…"-menu action, a Signal Lab purge, attaching a PDF) still has
    to be visible and auditable in Activity. This writes a single finished
    ``operation_status`` row; ``list_all_job_statuses`` merges that table into
    the Activity list, so its ``message`` and structured ``result`` show up.

    Written through the request's OWN gated ``db`` connection inside a short
    ``run_write_unit`` — never the scheduler's second connection, whose plain
    ``BEGIN DEFERRED`` loses the read→write upgrade race under a burst of
    near-simultaneous actions and silently drops the row (lessons:
    "Activity/status rows for foreground actions go through the GATED
    connection").

    Call it AFTER the action's own write unit has returned: it commits as its
    own independent unit, and it is best-effort — a logging failure is logged
    at debug and never reaches the user's action. Returns the job id, so a
    caller can hand it back to the client. ``job_id`` lets a caller that
    already owns one (its application layer logged lines under it) make the
    status row and those lines ONE Activity entry.
    """
    from alma.core.db_write import run_write_unit

    now = utcnow().isoformat()
    namespace = operation_key.split(".", 1)[0].split(":", 1)[0] or "action"
    jid = job_id or f"{namespace}_action_{uuid.uuid4().hex[:10]}"
    ctx = OperationContext(
        operation_key=operation_key,
        trigger_source="user",
        actor="api_user",
        correlation_id=jid,
        operation_id=jid,
        started_at=now,
        finished_at=now,
        status=status,
        message=message,
        result=result,
    )
    try:
        run_write_unit(
            db,
            lambda: persist_operation_status(db, ctx),
            label=f"activity:{operation_key}",
        )
    except Exception:  # noqa: BLE001 — best-effort logging, never fail the action
        logger.debug("foreground activity row skipped (%s)", operation_key, exc_info=True)
    return jid


def last_completed_finished_at(
    db: sqlite3.Connection,
    operation_key: str,
    *,
    prefix: bool = False,
) -> str | None:
    """Return MAX(finished_at) across completed rows for an operation key.

    When ``prefix=True``, match any row whose ``operation_key`` starts with
    the given value (e.g. ``feed.monitor.refresh:`` to cover every monitor).
    """
    if prefix:
        pattern = f"{operation_key}%"
        row = db.execute(
            """
            SELECT MAX(finished_at)
            FROM operation_status
            WHERE status = 'completed' AND operation_key LIKE ?
            """,
            (pattern,),
        ).fetchone()
    else:
        row = db.execute(
            """
            SELECT MAX(finished_at)
            FROM operation_status
            WHERE status = 'completed' AND operation_key = ?
            """,
            (operation_key,),
        ).fetchone()
    if not row:
        return None
    value = row[0]
    return str(value) if value else None


def persist_operation_log(
    db: sqlite3.Connection,
    *,
    operation_id: str,
    level: str = "INFO",
    step: str | None = None,
    message: str,
    data: dict[str, Any] | None = None,
) -> None:
    """Append a single lifecycle log row into operation_logs."""
    db.execute(
        """
        INSERT INTO operation_logs (job_id, timestamp, level, step, message, data_json)
        VALUES (?, ?, ?, ?, ?, ?)
        """,
        (
            operation_id,
            utcnow().isoformat(),
            level,
            step,
            message,
            _json_dumps(data or {}),
        ),
    )
