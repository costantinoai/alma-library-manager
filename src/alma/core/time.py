"""UTC time helpers with the project's SQLite-compatible semantics."""

from __future__ import annotations

from datetime import datetime, timezone


def utcnow() -> datetime:
    """Return the current UTC time as a timezone-naive ``datetime``.

    ALMa persists UTC timestamps in SQLite ``DATETIME`` columns without an
    offset. Constructing an aware value first avoids the deprecated stdlib
    constructor while preserving that storage contract.
    """
    return datetime.now(timezone.utc).replace(tzinfo=None)


def utcnow_iso() -> str:
    """Return the current naive-UTC time in ISO-8601 form."""
    return utcnow().isoformat()
