"""One SQLite connection contract: busy timeout, in one place.

Why this module exists — ``busy_timeout`` had drifted to two different values
across the codebase (5 s on the scheduler's Activity connection, 30 s
everywhere else). That inconsistency was not cosmetic: it decided how long a
writer would tolerate a lock before giving up, so the *same* contention
produced a dropped Activity row on one connection and a patient wait on
another.

The contract, post-2026-07-26 audit:

* **Intra-process contention is handled by the writer gate**, not by
  ``busy_timeout`` — see :mod:`alma.core.db_write`. Every writer in the
  process, Activity rows included, serializes through one mutex and then takes
  the SQLite write lock uncontended. A correctly-gated writer should therefore
  never observe SQLITE_BUSY at all.
* ``busy_timeout`` is the **cross-process belt** only (CLI scripts, a second
  uvicorn worker, an external sqlite3 shell). Because it should never fire in
  normal operation, it is set generously and *identically* everywhere: a
  timeout that fires is a bug report, not a tuning knob. A short timeout only
  converts "wait a moment" into "silently lose the row".

Use :func:`apply_busy_timeout` on every connection ALMa opens, and
:data:`SQLITE_CONNECT_TIMEOUT_S` as the ``sqlite3.connect(timeout=...)``
argument so the driver-level and pragma-level budgets agree.
"""

from __future__ import annotations

import sqlite3

# The single value. See the module docstring for why it is uniform and long.
SQLITE_BUSY_TIMEOUT_MS = 30_000

# `sqlite3.connect(timeout=...)` is the same budget expressed in seconds; the
# driver uses it to set busy_timeout itself, so keeping them in sync avoids one
# connection having two different answers to "how long do I wait".
SQLITE_CONNECT_TIMEOUT_S = SQLITE_BUSY_TIMEOUT_MS / 1000.0


def apply_busy_timeout(conn: sqlite3.Connection) -> sqlite3.Connection:
    """Set the canonical ``busy_timeout`` on ``conn`` and return it."""
    conn.execute(f"PRAGMA busy_timeout={SQLITE_BUSY_TIMEOUT_MS}")
    return conn
