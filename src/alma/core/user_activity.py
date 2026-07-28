"""Process-global "last user activity" clock for background-op idle-gating (task 37 A).

Background health/maintenance ops must yield to the user: they may only run once the
app has been idle — no user-initiated request — for `IDLE_THRESHOLD_SECONDS`. We track
the last user-initiated request as an IN-MEMORY monotonic timestamp, updated by the HTTP
middleware. Deliberately NOT a DB write: stamping it on a GET would otherwise violate the
no-write-on-GET rule. In-memory is sufficient — idle-gating is a runtime concern, and
after a restart we start "active" (a grace window) so background work never slams a user
who just opened the app.

What counts as user activity: any request EXCEPT the endpoints polled on a timer
regardless of user presence. Two of them:

- `GET /activity` — `useOperationToasts` polls it app-wide every 12 s.
- `GET /health` — the container's own `HEALTHCHECK` curls it every 30 s, and nothing
  about that is a user. Counting it pinned the app "active" FOREVER in Docker (30 s
  heartbeat < the 180 s idle threshold), so every background drain started, logged
  "app not idle yet" and was cancelled — corpus enrichment never ran in prod at all
  (found 2026-07-28).

Paths are matched AFTER stripping the API version prefix (`/api/v1/...`), because the
routers mount under it and a bare-path allow-list silently matched nothing. That is
what let the `/activity` entry rot into dead code: `is_user_activity_path` was asked
about `/api/v1/activity`, which does not start with `/activity`, so only the frontend's
`X-Alma-Poll` header was keeping the poll out of the clock. Normalizing here means the
allow-list cannot be disarmed by a mount-prefix change.
"""

from __future__ import annotations

import re
import time

# 3 minutes of app-idle before a background sweep may run (user-confirmed, task 37 A).
IDLE_THRESHOLD_SECONDS: float = 180.0

# Timer-polled routes → NOT activity. Matched EXACTLY (never as a prefix), because
# under both of these sit genuinely user-facing routes: `/activity/{id}/cancel` is a
# button the user pressed, and the Health PAGE lives under `/health/...`.
#
#   /activity — `useOperationToasts` polls it app-wide every 12 s.
#   /health   — the container HEALTHCHECK curls it every 30 s.
#
# Kept deliberately MINIMAL: under-ignoring only makes background work defer more
# (safe), while over-ignoring lets a sweep start while the user is working.
_POLL_PATH_EXACT: frozenset[str] = frozenset({"/activity", "/health"})

# The version prefix every router mounts under (`app.include_router(..., prefix="/api/v1")`).
# Stripped before matching so the allow-list above is written in ROUTE terms, and a
# future `/api/v2` needs no edit here.
_API_PREFIX_RE = re.compile(r"^/api/v\d+(?=/)")


def _strip_api_prefix(path: str) -> str:
    """`/api/v1/activity` → `/activity`; anything else unchanged."""
    return _API_PREFIX_RE.sub("", str(path or ""))

# Start IDLE (last activity "long ago"): a fresh process has seen no user request
# yet, so background work may run until a real request marks the app active. A user
# who just opened the app necessarily generates requests → active; no requests →
# genuinely idle. This also keeps programmatic/test invocations — which make no HTTP
# request — from being mis-read as "active" and needlessly deferring work.
_last_activity_monotonic: float = time.monotonic() - 86_400.0


def is_user_activity_path(path: str) -> bool:
    """True when *path* is a user-initiated request (not a timer/health poll).

    Accepts either form — `/api/v1/activity` or `/activity` — so it is correct
    whether called with a raw request path or an already-normalized route.
    """
    return (_strip_api_prefix(path).rstrip("/") or "/") not in _POLL_PATH_EXACT


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
