"""Shared helpers for API route handlers.

Consolidates common utilities previously duplicated across route files.
"""

import json
import logging
import sqlite3
from typing import Any

from fastapi import HTTPException, status

from alma.api.models import PaperResponse
from alma.core.keywords import format_keywords_for_display
from alma.core.redaction import redact_sensitive_text

logger = logging.getLogger(__name__)


def raise_internal(message: str, exc: Exception) -> None:
    """Log an internal error (with full traceback) and raise HTTP 500.

    ``exc_info=exc`` makes the logger emit the complete stack trace, not just
    the exception's ``str()`` — otherwise a 500 leaves nothing in the server
    log to debug from. The client still receives only ``message`` (the
    redacted summary), so we don't leak internals over HTTP.
    """
    logger.error("%s: %s", message, redact_sensitive_text(str(exc)), exc_info=exc)
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
    if "keywords" in data:
        display_keywords = format_keywords_for_display(data.get("keywords"))
        data["keywords"] = display_keywords or None
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


async def stage_pdf_upload(request: Any):
    """Stream a raw ``application/pdf`` request body into the PDF store's staging.

    The one upload intake for every PDF route (attach to a paper, PDF-first
    import). The body is the file itself — not multipart, whose parser spools
    anything over 1 MB into ``/tmp`` (a 128 MB tmpfs in the Docker image).
    Chunks go straight to ``pdfs/.incoming`` under the size cap; no database is
    touched. Answers 413 past the cap and 415 when the bytes are not a PDF.
    Returns the staged file; the caller hands it to a queued job.
    """
    from alma.application.pdfs import store
    from alma.application.pdfs.verify import BodyKind, classify_head

    writer = store.StagingWriter()
    try:
        async for chunk in request.stream():
            writer.write(chunk)
    except store.PdfTooLargeError as exc:
        raise HTTPException(status_code=status.HTTP_413_REQUEST_ENTITY_TOO_LARGE, detail=str(exc)) from exc
    except BaseException:
        writer.abort()
        raise
    staged = writer.finish()
    if staged.bytes == 0 or classify_head(store.read_head(staged.path)) is not BodyKind.PDF:
        store.release_staged(staged)
        raise HTTPException(
            status_code=status.HTTP_415_UNSUPPORTED_MEDIA_TYPE,
            detail="That file is not a PDF",
        )
    return staged


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
        level: str = "INFO",
    ) -> None:
        """Persist progress into Activity for background jobs.

        ``level`` marks a step that went wrong but did not end the job (a
        source that was down, a candidate that was rejected) so it stands out
        in the Activity log without failing the operation.
        """
        if not self.job_id:
            return
        from alma.api.scheduler import add_job_log, set_job_status

        payload: dict[str, Any] = {"message": message}
        if processed is not None:
            payload["processed"] = processed
        if total is not None:
            payload["total"] = total
        set_job_status(self.job_id, **payload)
        add_job_log(self.job_id, message, level=level, step=step, data=data)
