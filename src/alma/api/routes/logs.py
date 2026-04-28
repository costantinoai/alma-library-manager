"""Log viewer API endpoint.

Provides a ring-buffer logging handler that captures the last N log records
from the Python root logger, plus an endpoint to query them.
"""

import collections
import logging
from datetime import datetime
from typing import Optional

from fastapi import APIRouter, Depends, Query

from alma.api.deps import get_current_user
from alma.core.redaction import redact_sensitive_text

logger = logging.getLogger(__name__)

# --------------------------------------------------------------------------
# Ring-buffer logging handler
# --------------------------------------------------------------------------


class RingBufferHandler(logging.Handler):
    """A logging handler that stores log records in a fixed-size deque."""

    def __init__(self, capacity: int = 500):
        super().__init__()
        self.buffer: collections.deque[dict] = collections.deque(maxlen=capacity)

    def emit(self, record: logging.LogRecord) -> None:
        safe_message = redact_sensitive_text(self.format(record))
        self.buffer.append({
            "timestamp": datetime.fromtimestamp(record.created).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "message": safe_message,
        })

    def get_entries(
        self,
        limit: int = 100,
        level: str | None = None,
        logger_name: str | None = None,
        since: Optional[str] = None,
        operation_id: str | None = None,
    ) -> list[dict]:
        """Return the most recent *limit* entries, optionally filtered by level."""
        entries = list(self.buffer)
        if level:
            entries = [e for e in entries if e["level"] == level.upper()]
        if logger_name:
            needle = logger_name.strip().lower()
            entries = [e for e in entries if needle in str(e.get("logger") or "").lower()]
        if since:
            since_norm = since.strip()
            entries = [e for e in entries if str(e.get("timestamp") or "") >= since_norm]
        if operation_id:
            needle = f"[{operation_id.strip()}]"
            entries = [e for e in entries if needle in str(e.get("message") or "")]
        return entries[-limit:]


# Global singleton handler
_ring_handler = RingBufferHandler(capacity=500)
_ring_handler.setFormatter(logging.Formatter("%(message)s"))
_ring_handler.setLevel(logging.DEBUG)


def install_log_handler() -> None:
    """Attach the ring-buffer handler to the root logger (idempotent)."""
    root = logging.getLogger()
    # Ensure informational runtime events are captured in the in-memory feed.
    if root.level > logging.INFO:
        root.setLevel(logging.INFO)
    if _ring_handler not in root.handlers:
        root.addHandler(_ring_handler)


def get_log_entries(
    limit: int = 100,
    level: str | None = None,
    logger_name: str | None = None,
    since: Optional[str] = None,
    operation_id: str | None = None,
) -> list[dict]:
    """Public accessor for the buffered log entries."""
    return _ring_handler.get_entries(limit, level, logger_name, since, operation_id)


# --------------------------------------------------------------------------
# Router
# --------------------------------------------------------------------------

router = APIRouter(
    responses={
        401: {"description": "Unauthorized"},
    },
)


@router.get(
    "",
    summary="View recent log entries",
    description="Return the most recent log entries captured by the in-memory ring buffer.",
)
def get_logs(
    limit: int = Query(100, ge=1, le=500, description="Maximum entries to return"),
    level: str = Query(None, description="Filter by log level (DEBUG, INFO, WARNING, ERROR, CRITICAL)"),
    logger_name: str = Query(None, alias="logger", description="Filter by logger name"),
    since: str = Query(None, description="Only include logs at/after this ISO timestamp"),
    operation_id: str = Query(None, description="Filter by operation id correlation marker"),
    user: dict = Depends(get_current_user),
):
    """Return buffered log entries."""
    return get_log_entries(limit, level, logger_name, since, operation_id)
