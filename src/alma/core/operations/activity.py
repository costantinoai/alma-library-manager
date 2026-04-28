"""Persistence helpers for operation lifecycle and logs."""

from __future__ import annotations

import json
import sqlite3
from datetime import datetime
from typing import Any

from .models import OperationContext


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
            datetime.utcnow().isoformat(),
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
            datetime.utcnow().isoformat(),
            level,
            step,
            message,
            _json_dumps(data or {}),
        ),
    )
