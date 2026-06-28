"""User-configurable knobs for background-ops governance (task 37).

The idle-wait and the external-API reserve ship as named constants
(`user_activity.IDLE_THRESHOLD_SECONDS`, `http_sources.RESERVED_USER_CALLS`) but are
overridable from Settings → Data & system. They live in the shared `discovery_settings`
KV store (the same store the feed/discovery settings use); the constants are the
DEFAULTS when a key is unset. ONE reader per knob so the scheduler gate, the per-sweep
yield check, and the Health budget payload all resolve the same value.
"""

from __future__ import annotations

import sqlite3

from alma.core.http_sources import RESERVED_USER_CALLS
from alma.core.user_activity import IDLE_THRESHOLD_SECONDS

# KV keys (namespaced under `maintenance.` like the other scheduler/maintenance knobs).
IDLE_WAIT_MINUTES_KEY = "maintenance.idle_wait_minutes"
RESERVED_API_CALLS_KEY = "maintenance.reserved_api_calls"

# Defaults derived from the shipped constants — single source of truth.
DEFAULT_IDLE_WAIT_MINUTES = int(round(IDLE_THRESHOLD_SECONDS / 60.0))  # 3
DEFAULT_RESERVED_API_CALLS = int(RESERVED_USER_CALLS)  # 200


def _read_raw(conn: sqlite3.Connection, key: str) -> str | None:
    try:
        from alma.application.discovery import lens_crud

        return lens_crud.read_settings(conn).get(key)
    except Exception:
        return None


def get_idle_wait_minutes(conn: sqlite3.Connection) -> int:
    """Configured idle-wait in minutes (0 = run as soon as nothing else is active)."""
    raw = _read_raw(conn, IDLE_WAIT_MINUTES_KEY)
    if raw is None:
        return DEFAULT_IDLE_WAIT_MINUTES
    try:
        return max(0, int(float(raw)))
    except (TypeError, ValueError):
        return DEFAULT_IDLE_WAIT_MINUTES


def get_idle_wait_seconds(conn: sqlite3.Connection) -> float:
    """Idle-wait as seconds for `user_activity.app_is_idle(threshold_seconds=…)`."""
    return float(get_idle_wait_minutes(conn)) * 60.0


def get_reserved_api_calls(conn: sqlite3.Connection) -> int:
    """Configured number of provider calls a BACKGROUND op leaves for the user."""
    raw = _read_raw(conn, RESERVED_API_CALLS_KEY)
    if raw is None:
        return DEFAULT_RESERVED_API_CALLS
    try:
        return max(0, int(float(raw)))
    except (TypeError, ValueError):
        return DEFAULT_RESERVED_API_CALLS
