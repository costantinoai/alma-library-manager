"""Shared helpers for API route handlers.

Consolidates common utilities previously duplicated across route files.
"""

import json
import logging
import sqlite3
from typing import Any

from fastapi import HTTPException, status

from alma.api.models import PaperResponse
from alma.core.redaction import redact_sensitive_text

logger = logging.getLogger(__name__)


def raise_internal(message: str, exc: Exception) -> None:
    """Log an internal error and raise HTTP 500."""
    logger.error("%s: %s", message, redact_sensitive_text(str(exc)))
    raise HTTPException(
        status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
        detail=message,
    )


def row_to_paper_response(row: sqlite3.Row) -> PaperResponse:
    """Convert a database row to PaperResponse, parsing JSON fields."""
    data = dict(row)
    for field in ("keywords", "sdgs", "counts_by_year"):
        if field in data and data[field] and isinstance(data[field], str):
            try:
                data[field] = json.loads(data[field])
            except (json.JSONDecodeError, TypeError):
                data[field] = None
    return PaperResponse(**data)


def safe_div(numerator: float, denominator: float) -> float:
    """Safe division that returns 0.0 when denominator <= 0."""
    if denominator <= 0:
        return 0.0
    return numerator / denominator


def table_exists(db: sqlite3.Connection, name: str) -> bool:
    """Check whether a table exists in the database."""
    row = db.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?",
        (name,),
    ).fetchone()
    return row is not None


def json_loads(value: Any) -> Any:
    """Safely parse a JSON string, returning the value unchanged on failure."""
    if value is None:
        return None
    if isinstance(value, (dict, list)):
        return value
    if not isinstance(value, str):
        return None
    try:
        return json.loads(value)
    except Exception:
        return None


def normalize_topic_term(term: str) -> str:
    """Normalize a topic term by collapsing whitespace."""
    return " ".join((term or "").strip().split())


def background_mode_requested(background: bool | None) -> bool:
    """Resolve whether a route should run work in the background.

    When callers omit the query parameter, the app follows the scheduler's
    effective availability. This keeps production UI flows non-blocking while
    still letting tests or explicitly scheduler-less runs fall back to a
    synchronous path.
    """
    if background is not None:
        return background
    from alma.api.scheduler import _scheduler_enabled

    return _scheduler_enabled()


class ActivityJobContext:
    """Minimal scheduler-backed progress logger with ``OperationContext`` parity."""

    def __init__(self, job_id: str):
        self.job_id = str(job_id or "").strip()

    def log_step(
        self,
        step: str,
        message: str,
        *,
        data: dict[str, Any] | None = None,
        processed: int | None = None,
        total: int | None = None,
    ) -> None:
        """Persist progress into Activity for background jobs."""
        if not self.job_id:
            return
        from alma.api.scheduler import add_job_log, set_job_status

        payload: dict[str, Any] = {"message": message}
        if processed is not None:
            payload["processed"] = processed
        if total is not None:
            payload["total"] = total
        set_job_status(self.job_id, **payload)
        add_job_log(self.job_id, message, step=step, data=data)
