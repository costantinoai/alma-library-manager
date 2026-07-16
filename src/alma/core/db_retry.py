"""Retry helpers for transient SQLite write-lock contention.

SQLite is single-writer: only one connection may hold the write lock at a
time.  ALMa runs foreground HTTP writes alongside a pool of background
runner jobs (corpus rehydrate, embeddings, author hydration…), so under a
burst of user actions the foreground write can occasionally lose the race
for the writer even with a generous ``busy_timeout``.  When that happens
SQLite raises ``OperationalError: database is locked`` (or
``database is busy``).

Every connection ALMa opens already sets ``PRAGMA busy_timeout`` (see
``alma.api.deps.open_db_connection``), which makes SQLite *wait* for the
lock rather than fail immediately.  This module is the belt-and-suspenders
layer on top: it wraps the *user-facing* commit so a brief lock that
outlives even the busy_timeout never silently drops a click (a dismissed
suggestion that reappears, a follow that doesn't take).  Background jobs
deliberately do NOT use this — they are idempotent and re-run on the next
sweep, so a dropped background write is self-healing.

Keep the scope narrow: this is for the handful of foreground write
endpoints, not a general-purpose "retry every query" hammer.
"""

from __future__ import annotations

import logging
import sqlite3
import time
from collections.abc import Callable
from typing import TypeVar

logger = logging.getLogger(__name__)

T = TypeVar("T")

# Substrings that mark a *transient* lock error worth retrying. A plain
# "database is locked" / "database is busy" clears once the competing
# writer commits; anything else (corruption, readonly, constraint) is a
# real error we must not paper over.
_TRANSIENT_LOCK_MARKERS = ("database is locked", "database is busy")


def is_transient_lock_error(exc: BaseException) -> bool:
    """True when ``exc`` is a retryable SQLite write-lock contention error."""
    if not isinstance(exc, sqlite3.OperationalError):
        return False
    message = str(exc).lower()
    return any(marker in message for marker in _TRANSIENT_LOCK_MARKERS)


def run_with_lock_retry(
    operation: Callable[[], T],
    *,
    attempts: int = 4,
    base_delay: float = 0.05,
    label: str = "db write",
) -> T:
    """Run ``operation`` retrying only on transient SQLite lock errors.

    ``operation`` should be the full write-then-commit unit so a retry
    re-issues the writes on a clean transaction.  Backoff is exponential
    (``base_delay`` × 2**n) with the final attempt re-raising the original
    error so the caller's normal error handling still runs.

    Args:
        operation: zero-arg callable performing the write + ``commit()``.
        attempts:  total tries (default 4 → up to 3 retries).
        base_delay: seconds before the first retry; doubles each round.
        label:     short description for the retry log line.
    """
    last_exc: sqlite3.OperationalError | None = None
    for attempt in range(1, max(1, attempts) + 1):
        try:
            return operation()
        except sqlite3.OperationalError as exc:
            if not is_transient_lock_error(exc) or attempt >= attempts:
                raise
            last_exc = exc
            delay = base_delay * (2 ** (attempt - 1))
            logger.warning(
                "%s hit SQLite lock (attempt %d/%d) — retrying in %.0fms",
                label,
                attempt,
                attempts,
                delay * 1000,
            )
            time.sleep(delay)
    # Unreachable: the loop either returns or re-raises. Guard for typing.
    assert last_exc is not None
    raise last_exc


def commit_with_retry(
    conn: sqlite3.Connection,
    *,
    attempts: int = 4,
    base_delay: float = 0.05,
    label: str = "commit",
) -> None:
    """Retry ``conn.commit()`` on transient lock errors.

    Use this when the writes are already staged on ``conn`` and only the
    commit can contend for the writer.  When the writes themselves may need
    re-issuing, wrap the whole write+commit block in
    :func:`run_with_lock_retry` instead.
    """
    run_with_lock_retry(
        conn.commit, attempts=attempts, base_delay=base_delay, label=label
    )
