"""Process-global "last user activity" clock for background-op idle-gating (task 37 A).

Background health/maintenance ops must yield to the user: they may only run once the
app has been idle — no user-initiated request — for `IDLE_THRESHOLD_SECONDS`. We track
the last user-initiated request as an IN-MEMORY monotonic timestamp, updated by the HTTP
middleware. Deliberately NOT a DB write: stamping it on a GET would otherwise violate the
no-write-on-GET rule. In-memory is sufficient — idle-gating is a runtime concern, and
after a restart we start "active" (a grace window) so background work never slams a user
who just opened the app.

What counts as user activity: any request EXCEPT the endpoints the frontend polls on a
timer regardless of user presence. The critical one is `GET /activity` — `useOperationToasts`
polls it app-wide every 12 s; counting it would keep the app permanently "active" and
starve background work entirely.
"""

from __future__ import annotations

import time

# 3 minutes of app-idle before a background sweep may run (user-confirmed, task 37 A).
IDLE_THRESHOLD_SECONDS: float = 180.0

# Paths the frontend polls on a timer independent of user interaction → NOT activity.
# Kept deliberately MINIMAL and conservative: under-ignoring only makes background work
# defer more (safe); the one entry that MUST be here is the app-wide /activity poll, or
# `app_is_idle` could never become true.
_POLL_PATH_PREFIXES: tuple[str, ...] = ("/activity",)

# Start IDLE (last activity "long ago"): a fresh process has seen no user request
# yet, so background work may run until a real request marks the app active. A user
# who just opened the app necessarily generates requests → active; no requests →
# genuinely idle. This also keeps programmatic/test invocations — which make no HTTP
# request — from being mis-read as "active" and needlessly deferring work.
_last_activity_monotonic: float = time.monotonic() - 86_400.0


def is_user_activity_path(path: str) -> bool:
    """True when *path* is a user-initiated request (not a background status poll)."""
    p = str(path or "")
    return not any(p.startswith(prefix) for prefix in _POLL_PATH_PREFIXES)


def touch_user_activity() -> None:
    """Mark "a user just did something" (called by the HTTP middleware).

    A single float write — atomic under CPython's GIL, so no lock is needed.
    """
    global _last_activity_monotonic
    _last_activity_monotonic = time.monotonic()


def seconds_since_user_activity() -> float:
    """Seconds since the last user-initiated request."""
    return max(0.0, time.monotonic() - _last_activity_monotonic)


def reset_for_test() -> None:
    """Reset the idle clock to "long idle" (test-only).

    The clock is a process-global; a test that calls `touch_user_activity()` would
    otherwise leak "active" into a later test whose background sweep then yields
    instead of running. An autouse conftest fixture calls this before each test so
    every test starts from the same idle baseline (matching "a test makes no HTTP
    request, so the app is idle"). Not used in production code.
    """
    global _last_activity_monotonic
    _last_activity_monotonic = time.monotonic() - 86_400.0


def app_is_idle(threshold_seconds: float = IDLE_THRESHOLD_SECONDS) -> bool:
    """True when no user-initiated request has arrived for `threshold_seconds`.

    The gate a background op checks (with `any_operation_active`) before running.
    """
    return seconds_since_user_activity() >= float(threshold_seconds)
