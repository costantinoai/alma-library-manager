"""Walkthrough state: the one owner of "has the user seen this guided overlay?".

Covers the first-run wizard (``welcome``), every page tour, and any future
new-feature walkthrough. The browser holds no verdict: state lives in the
``discovery_settings`` key/value table, one row per walkthrough, so it survives
a new browser, a different origin and cleared site data.

Row: ``walkthrough.<id>`` → JSON ``{version, outcome, at, source}``.

- ``version``: the walkthrough revision this outcome applies to.
- ``outcome``: ``done`` (finished), ``skipped`` (dismissed), ``assumed`` (never
  recorded; it predates this store and the install had finished onboarding), or
  ``requested`` (the user asked to see it again).
- ``at``: when the outcome was written.
- ``source``: ``user``, ``upgrade`` (startup migrator) or ``browser_import``.

A walkthrough is **pending** when it has no row, its outcome is ``requested``,
or its recorded version is older than the registry's. Nothing else decides
that, so shipping a revision re-triggers exactly that walkthrough, once:
closing it records the current version.

To ship a new walkthrough add it to ``WALKTHROUGH_VERSIONS``; to re-show one
after changing it, bump its number. That registry is the only place a version
is written.
"""

from __future__ import annotations

import json
import sqlite3
from dataclasses import dataclass

from alma.application.discovery.lens_crud import upsert_setting
from alma.core.time import utcnow

WELCOME_ID = "welcome"

#: Current revision of every walkthrough. Bump a number to re-show that one.
WALKTHROUGH_VERSIONS: dict[str, int] = {
    WELCOME_ID: 1,
    "home": 1,
    "feed": 1,
    "authors": 1,
    "library": 1,
    "discovery": 1,
}

#: The revision every walkthrough had before state moved to the database.
#: Migrated and imported rows record this, not the current number, so a later
#: bump still re-triggers them.
BASELINE_VERSION = 1

KEY_PREFIX = "walkthrough."

#: Closing outcomes a user can produce.
CLOSE_OUTCOMES = ("done", "skipped")

_LEGACY_COMPLETED_KEY = "onboarding.completed"
_LEGACY_COMPLETED_AT_KEY = "onboarding.completed_at"


@dataclass(frozen=True)
class WalkthroughState:
    """One walkthrough's stored record plus its derived pending verdict."""

    id: str
    current_version: int
    pending: bool
    outcome: str | None = None
    recorded_version: int | None = None
    at: str | None = None
    source: str | None = None


def is_known(walkthrough_id: str) -> bool:
    """True when the id is in the registry."""
    return walkthrough_id in WALKTHROUGH_VERSIONS


def _read_record(db: sqlite3.Connection, walkthrough_id: str) -> dict | None:
    """Read one stored record, or None when it has never been written.

    Read errors and malformed rows propagate on purpose. Swallowing one would
    read as "no row" and therefore as "pending": a database hiccup must never
    put an overlay on the screen.
    """
    row = db.execute(
        "SELECT value FROM discovery_settings WHERE key = ?",
        (f"{KEY_PREFIX}{walkthrough_id}",),
    ).fetchone()
    return None if row is None else json.loads(row[0])


def _write_record(
    db: sqlite3.Connection,
    walkthrough_id: str,
    *,
    version: int,
    outcome: str,
    at: str,
    source: str,
) -> None:
    """Upsert one record. The caller owns the write unit."""
    upsert_setting(
        db,
        f"{KEY_PREFIX}{walkthrough_id}",
        json.dumps(
            {"version": version, "outcome": outcome, "at": at, "source": source},
            sort_keys=True,
        ),
    )


def get_walkthrough(db: sqlite3.Connection, walkthrough_id: str) -> WalkthroughState:
    """Derive one walkthrough's state. Pure read."""
    current = WALKTHROUGH_VERSIONS[walkthrough_id]
    record = _read_record(db, walkthrough_id)
    if record is None:
        return WalkthroughState(id=walkthrough_id, current_version=current, pending=True)
    return WalkthroughState(
        id=walkthrough_id,
        current_version=current,
        pending=record["outcome"] == "requested" or record["version"] < current,
        outcome=record["outcome"],
        recorded_version=record["version"],
        at=record["at"],
        source=record["source"],
    )


def record_outcome(db: sqlite3.Connection, walkthrough_id: str, outcome: str) -> None:
    """Record that the user finished or dismissed the current revision.

    Runs inside the caller's write unit.
    """
    if outcome not in CLOSE_OUTCOMES:
        raise ValueError(f"not a closing outcome: {outcome}")
    _write_record(
        db,
        walkthrough_id,
        version=WALKTHROUGH_VERSIONS[walkthrough_id],
        outcome=outcome,
        at=utcnow().isoformat(),
        source="user",
    )


def request_replay(db: sqlite3.Connection, walkthrough_ids: list[str]) -> None:
    """Mark walkthroughs as asked for again. Runs inside the caller's write unit."""
    at = utcnow().isoformat()
    for walkthrough_id in walkthrough_ids:
        _write_record(
            db,
            walkthrough_id,
            version=WALKTHROUGH_VERSIONS[walkthrough_id],
            outcome="requested",
            at=at,
            source="user",
        )


def import_browser_verdicts(
    db: sqlite3.Connection, walkthrough_ids: list[str]
) -> tuple[list[str], list[str]]:
    """Take a browser's old page-tour 'done' flags, one way.

    Only page tours ever had such a flag, so ``welcome`` and ids that left the
    registry are ignored. A browser flag can fill an empty record or sharpen an
    ``assumed`` one; it never overrides what the user did on the server, so an
    old browser cannot cancel a replay the user asked for. Runs inside the
    caller's write unit. Returns ``(imported, ignored)``.
    """
    imported: list[str] = []
    ignored: list[str] = []
    at = utcnow().isoformat()
    for walkthrough_id in dict.fromkeys(walkthrough_ids):
        if not is_known(walkthrough_id) or walkthrough_id == WELCOME_ID:
            ignored.append(walkthrough_id)
            continue
        record = _read_record(db, walkthrough_id)
        if record is not None and record["source"] != "upgrade":
            ignored.append(walkthrough_id)
            continue
        _write_record(
            db,
            walkthrough_id,
            version=BASELINE_VERSION,
            outcome="done",
            at=at,
            source="browser_import",
        )
        imported.append(walkthrough_id)
    return imported, ignored


def migrate_walkthrough_state(conn: sqlite3.Connection) -> int:
    """Startup migrator: replace the old ``onboarding.completed`` flag.

    The old keys are the validator: their presence means this install has not
    been migrated, and deleting them is what makes this run once, with no
    marker row and no schema version. A fresh database never has them.

    A finished install keeps its wizard as ``done``, and each page tour without
    a record becomes ``assumed`` at the baseline version. Page tours were only
    ever recorded in a browser, so the server cannot know which were seen, and
    a user who finished onboarding before this store existed must not be walked
    through every page again. The timestamp is the old completion time.

    Called at startup, never from a GET. Returns the number of rows written.
    """
    completed = conn.execute(
        "SELECT value FROM discovery_settings WHERE key = ?", (_LEGACY_COMPLETED_KEY,)
    ).fetchone()
    completed_at = conn.execute(
        "SELECT value FROM discovery_settings WHERE key = ?", (_LEGACY_COMPLETED_AT_KEY,)
    ).fetchone()
    if completed is None and completed_at is None:
        return 0

    written = 0
    finished = completed is not None and str(completed[0]).strip().lower() in ("1", "true", "yes")
    if finished:
        at = str(completed_at[0]) if completed_at is not None else utcnow().isoformat()
        for walkthrough_id in WALKTHROUGH_VERSIONS:
            if _read_record(conn, walkthrough_id) is not None:
                continue
            _write_record(
                conn,
                walkthrough_id,
                version=BASELINE_VERSION,
                outcome="done" if walkthrough_id == WELCOME_ID else "assumed",
                at=at,
                source="upgrade",
            )
            written += 1

    conn.execute(
        "DELETE FROM discovery_settings WHERE key IN (?, ?)",
        (_LEGACY_COMPLETED_KEY, _LEGACY_COMPLETED_AT_KEY),
    )
    return written
