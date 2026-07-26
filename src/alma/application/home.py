"""Read-only composition for Home's daily research desk.

Home owns presentation, not domain state. Feed and Discovery provide their
activity/review projections, Library owns reading order, Imports/Authors own
their attention predicates, and stored Health materializations provide the
only system-level interruption. This module composes those truths without
stamping a visit or scheduling background work.
"""

from __future__ import annotations

import sqlite3
from datetime import datetime, time, timedelta, timezone
from typing import Any
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from alma.application import alerts as alerts_app
from alma.application import connection_status
from alma.application import feed as feed_app
from alma.application import imports as imports_app
from alma.application import library as library_app
from alma.application import materialized_views as mv
from alma.application.discovery import lens_crud
from alma.core.sql_helpers import standalone_paper_sql
from alma.services import author_attention
from alma.services import health as health_service

RECENT_DAYS = 7
FEED_INBOX_DAYS = 60
# One full tile row on the widest Home grid (4 columns). Fewer is fine —
# Home never pads a row with filler it can't justify.
HIGHLIGHT_LIMIT = 4
# Home can show two complete rows and expands in place to the rest.
READING_PREVIEW_LIMIT = 10
#: Same over-fetch rationale as reading: Home shows a couple of rows and
#: expands in place, so one bounded page avoids a second request.
INBOX_PREVIEW_LIMIT = 10


def _table_exists(db: sqlite3.Connection, table: str) -> bool:
    return (
        db.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?",
            (table,),
        ).fetchone()
        is not None
    )


def _utc_sql(dt: datetime) -> str:
    """UTC-naive ISO text matching the project's SQLite timestamp shape."""
    return dt.astimezone(timezone.utc).replace(tzinfo=None).isoformat()


def _window(
    timezone_name: str,
    *,
    now: datetime | None = None,
) -> tuple[str, str, str, str]:
    """Return SQL day/recent/cutoff starts plus API day-start timestamp."""
    try:
        zone = ZoneInfo(timezone_name)
    except (ZoneInfoNotFoundError, ValueError) as exc:
        raise ValueError(f"Unknown IANA timezone: {timezone_name}") from exc

    current = now or datetime.now(timezone.utc)
    if current.tzinfo is None:
        current = current.replace(tzinfo=timezone.utc)
    local_now = current.astimezone(zone)
    local_date = local_now.date()
    local_midnight = datetime.combine(local_date, time.min, tzinfo=zone)
    recent_midnight = datetime.combine(
        local_date - timedelta(days=RECENT_DAYS),
        time.min,
        tzinfo=zone,
    )
    inbox_midnight = datetime.combine(
        local_date - timedelta(days=FEED_INBOX_DAYS),
        time.min,
        tzinfo=zone,
    )
    return (
        _utc_sql(local_midnight),
        _utc_sql(recent_midnight),
        _utc_sql(inbox_midnight),
        local_midnight.astimezone(timezone.utc).isoformat(),
    )


#: Days in the inflow sparkline, today inclusive.
TREND_DAYS = 7


def _local_days(
    timezone_name: str, *, now: datetime | None = None, days: int = TREND_DAYS
) -> list[tuple[str, str, str]]:
    """`(local_date, utc_start, utc_end)` for the last `days` local days.

    Boundaries are computed per-day in the user's zone rather than by
    subtracting 24h repeatedly, so a DST change shifts one bucket by an hour
    instead of smearing every bucket after it.
    """
    zone = ZoneInfo(timezone_name)
    current = now or datetime.now(timezone.utc)
    if current.tzinfo is None:
        current = current.replace(tzinfo=timezone.utc)
    today = current.astimezone(zone).date()

    windows: list[tuple[str, str, str]] = []
    for offset in range(days - 1, -1, -1):
        day = today - timedelta(days=offset)
        start = datetime.combine(day, time.min, tzinfo=zone)
        end = datetime.combine(day + timedelta(days=1), time.min, tzinfo=zone)
        windows.append((day.isoformat(), _utc_sql(start), _utc_sql(end)))
    return windows


def _trend(
    db: sqlite3.Connection, windows: list[tuple[str, str, str]]
) -> list[dict[str, Any]]:
    """Daily Feed + Discovery inflow across the sparkline window.

    Counts match the headline `today` numbers by construction: Feed keys on a
    paper's FIRST surfacing (so a second monitor can't inflate a day) and
    Discovery counts distinct papers from active lenses. One query per surface
    over the whole window — never one per day.
    """
    if not windows:
        return []

    start = windows[0][1]
    end = windows[-1][2]

    feed_rows = db.execute(
        f"""
        WITH per_paper AS (
            SELECT fi.paper_id, MIN(fi.fetched_at) AS first_seen
            FROM feed_items fi
            GROUP BY fi.paper_id
        )
        SELECT pp.first_seen AS at
        FROM per_paper pp
        JOIN papers p ON p.id = pp.paper_id
        WHERE pp.first_seen >= ? AND pp.first_seen < ?
          AND {standalone_paper_sql('p')}
        """,
        (start, end),
    ).fetchall()

    discovery_rows = db.execute(
        f"""
        SELECT DISTINCT r.paper_id, MIN(r.created_at) AS at
        FROM recommendations r
        JOIN discovery_lenses l ON l.id = r.lens_id AND l.is_active = 1
        JOIN papers p ON p.id = r.paper_id
        WHERE r.created_at >= ? AND r.created_at < ?
          AND {standalone_paper_sql('p')}
        GROUP BY r.paper_id
        """,
        (start, end),
    ).fetchall()

    def bucket(rows: list[Any]) -> dict[str, int]:
        counts: dict[str, int] = {date: 0 for date, _, _ in windows}
        for row in rows:
            stamp = str(row["at"] or "")
            for date, day_start, day_end in windows:
                if day_start <= stamp < day_end:
                    counts[date] += 1
                    break
        return counts

    feed_counts = bucket(feed_rows)
    discovery_counts = bucket(discovery_rows)
    return [
        {
            "date": date,
            "feed": feed_counts[date],
            "discovery": discovery_counts[date],
        }
        for date, _, _ in windows
    ]


def _paper(row: dict[str, Any]) -> dict[str, Any]:
    return {
        "id": str(row.get("id") or ""),
        "title": str(row.get("title") or ""),
        "authors": row.get("authors"),
        "year": row.get("year"),
        "journal": row.get("journal"),
        "abstract": row.get("abstract"),
        "tldr": row.get("tldr"),
        "url": row.get("url"),
        "doi": row.get("doi"),
        "status": row.get("status"),
        # Present only on the reading preview — highlight projections carry
        # their own period instead. Never fabricated.
        "added_at": row.get("added_at"),
        # Present only on Inbox items (see `library._attach_capture_provenance`).
        # Absent everywhere else rather than defaulted, so a missing channel
        # stays visibly missing.
        "capture_channel": row.get("capture_channel"),
        "captured_at": row.get("captured_at"),
    }


def _period(stamp: object, day_start: str) -> str:
    return "today" if str(stamp or "") >= day_start else "last_7_days"


def _feed_reason(candidate: dict[str, Any]) -> dict[str, str]:
    monitors = candidate.get("monitors") or []
    monitor = monitors[0] if monitors else {}
    kind = str(monitor.get("monitor_type") or "monitor")
    label = str(monitor.get("monitor_label") or "").strip()
    if kind == "author":
        text = f"From followed author {label}" if label else "From a followed author"
    elif kind == "venue":
        text = f"From followed journal {label}" if label else "From a followed journal"
    elif label:
        text = f"Matched {label}"
    else:
        text = "Matched one of your Feed monitors"
    return {"kind": kind, "label": text}


def _critical_health_count(db: sqlite3.Connection) -> int:
    """Actionable critical dimensions from stored snapshots only."""
    if not _table_exists(db, "materialized_views"):
        return 0
    count = 0
    for key in (
        health_service.HEALTH_CORPUS_VIEW_KEY,
        health_service.HEALTH_AUTHORS_VIEW_KEY,
    ):
        stored = mv.get_stored(db, key)
        payload = (stored or {}).get("payload") or {}
        for dimension in payload.get("dimensions") or []:
            if (
                dimension.get("severity") == "critical"
                and dimension.get("state") not in {"queued", "not_applicable"}
                and dimension.get("actions")
            ):
                count += 1
    return count


def _attention(db: sqlite3.Connection) -> dict[str, int]:
    imports_pending = imports_app.count_resolution_queue(db)
    monitors_need_resolution = int(
        db.execute(
            """
            SELECT COUNT(*) AS c
            FROM feed_monitors
            WHERE json_extract(COALESCE(config_json, '{}'), '$.needs_resolution')
                  IN (1, 'true')
            """
        ).fetchone()["c"]
        or 0
    )
    # Captures that reached ALMa but resolved to no paper — a link with no DOI,
    # or an upstream failure. They are recorded rather than dropped ("no silent
    # failures"), so this is where they ask for a human. Counted only when the
    # ledger exists, so a pre-migration DB reports 0 instead of raising.
    inbox_unresolved = 0
    if _table_exists(db, "inbox_messages"):
        inbox_unresolved = int(
            db.execute(
                "SELECT COUNT(*) AS c FROM inbox_messages "
                "WHERE outcome IN ('unresolved', 'error')"
            ).fetchone()["c"]
            or 0
        )
    return {
        "imports_pending": imports_pending,
        "monitors_need_resolution": monitors_need_resolution,
        "author_decisions": author_attention.identity_attention_count(db),
        "critical_health": _critical_health_count(db),
        "inbox_unresolved": inbox_unresolved,
    }


def _select_highlights(
    feed: dict[str, Any],
    discovery: dict[str, Any],
    *,
    day_start: str,
) -> list[dict[str, Any]]:
    """Pick one Feed paper, one Discovery paper, then one active source."""
    highlights: list[dict[str, Any]] = []
    used_papers: set[str] = set()

    feed_candidates = list(feed.get("candidates") or [])
    feed_today = [c for c in feed_candidates if str(c.get("first_seen") or "") >= day_start]
    feed_pick = (feed_today or feed_candidates or [None])[0]
    if feed_pick:
        paper_id = str(feed_pick.get("id") or "")
        used_papers.add(paper_id)
        monitor = (feed_pick.get("monitors") or [{}])[0]
        highlights.append(
            {
                "kind": "feed_paper",
                "period": _period(feed_pick.get("first_seen"), day_start),
                "paper": _paper(feed_pick),
                "reason": _feed_reason(feed_pick),
                "monitor_id": monitor.get("monitor_id"),
                "monitor_type": monitor.get("monitor_type"),
            }
        )

    discovery_candidates = list(discovery.get("candidates") or [])
    discovery_today = [
        c
        for c in discovery_candidates
        if str(c.get("created_at") or "") >= day_start
        and str(c.get("id") or "") not in used_papers
    ]
    discovery_recent = [
        c for c in discovery_candidates if str(c.get("id") or "") not in used_papers
    ]
    discovery_pick = (discovery_today or discovery_recent or [None])[0]
    if discovery_pick and len(highlights) < HIGHLIGHT_LIMIT:
        paper_id = str(discovery_pick.get("id") or "")
        used_papers.add(paper_id)
        lens_name = str(discovery_pick.get("lens_name") or "Discovery").strip()
        highlights.append(
            {
                "kind": "discovery_paper",
                "period": _period(discovery_pick.get("created_at"), day_start),
                "paper": _paper(discovery_pick),
                "reason": {
                    "kind": "lens",
                    "label": f"Top match from {lens_name}",
                },
                "lens_id": discovery_pick.get("lens_id"),
                "lens_name": lens_name,
                "recommendation_id": discovery_pick.get("recommendation_id"),
                "score": discovery_pick.get("score"),
            }
        )

    source_updates = list(feed.get("source_updates") or [])
    today_sources = [s for s in source_updates if str(s.get("latest_at") or "") >= day_start]
    for source in [*today_sources, *source_updates]:
        source_id = str(source.get("source_id") or "")
        supporting = next(
            (
                candidate
                for candidate in feed_candidates
                if any(
                    str(monitor.get("monitor_id") or "") == source_id
                    for monitor in candidate.get("monitors") or []
                )
                and str(candidate.get("id") or "") not in used_papers
            ),
            None,
        )
        if supporting is None:
            continue
        source_type = str(source.get("source_type") or "")
        source_label = str(source.get("source_label") or "").strip()
        highlights.append(
            {
                "kind": "source_update",
                "period": _period(source.get("latest_at"), day_start),
                "paper": _paper(supporting),
                "reason": {
                    "kind": source_type,
                    "label": (
                        f"{int(source.get('paper_count') or 0)} new papers from "
                        f"{source_label or 'this source'}"
                    ),
                },
                "source": {
                    "id": source_id,
                    "type": source_type,
                    "label": source_label,
                    "author_id": source.get("author_id"),
                    "paper_count": int(source.get("paper_count") or 0),
                },
            }
        )
        break

    # Top up to a full row with the next-best Discovery matches. One of each
    # kind comes first (the balance rule); the remainder goes to the only
    # bucket that carries an explicit relevance score, so every added tile can
    # still say why it earned its place.
    for candidate in discovery_candidates:
        if len(highlights) >= HIGHLIGHT_LIMIT:
            break
        paper_id = str(candidate.get("id") or "")
        if not paper_id or paper_id in used_papers:
            continue
        used_papers.add(paper_id)
        lens_name = str(candidate.get("lens_name") or "Discovery").strip()
        highlights.append(
            {
                "kind": "discovery_paper",
                "period": _period(candidate.get("created_at"), day_start),
                "paper": _paper(candidate),
                "reason": {"kind": "lens", "label": f"Match from {lens_name}"},
                "lens_id": candidate.get("lens_id"),
                "lens_name": lens_name,
                "recommendation_id": candidate.get("recommendation_id"),
                "score": candidate.get("score"),
            }
        )

    return highlights[:HIGHLIGHT_LIMIT]


def build_daily_brief(
    db: sqlite3.Connection,
    *,
    timezone_name: str,
    now: datetime | None = None,
) -> dict[str, Any]:
    """Build the complete one-request Home payload. Pure read."""
    day_start, recent_start, inbox_cutoff, day_start_api = _window(
        timezone_name,
        now=now,
    )
    feed = feed_app.home_feed_snapshot(
        db,
        day_start=day_start,
        recent_start=recent_start,
        inbox_cutoff=inbox_cutoff,
    )
    discovery = lens_crud.home_discovery_snapshot(
        db,
        day_start=day_start,
        recent_start=recent_start,
    )
    # Over-fetch one bounded page so two measured rows can expand in place
    # without a second request.
    reading = library_app.reading_preview(db, limit=READING_PREVIEW_LIMIT)
    # D13: papers you sent yourself from another device, awaiting triage. Home
    # is the Inbox's home surface; the section hides itself when empty.
    inbox = library_app.inbox_preview(db, limit=INBOX_PREVIEW_LIMIT)
    user_row = db.execute(
        "SELECT value FROM discovery_settings WHERE key = 'user.name'"
    ).fetchone()

    return {
        "generated_at": (now or datetime.now(timezone.utc)).astimezone(
            timezone.utc
        ).isoformat(),
        "day_start": day_start_api,
        "timezone": timezone_name,
        "user_name": str((user_row["value"] if user_row else "") or "").strip()
        or None,
        "activity": {
            "feed": {
                "today": int(feed["today"]),
                "carryover": int(feed["carryover"]),
                "by_monitor_type": feed["by_monitor_type"],
            },
            "discovery": {
                "today": int(discovery["today"]),
                "carryover": int(discovery["carryover"]),
                "lenses_today": int(discovery["lenses_today"]),
            },
            "alerts": {
                "today": alerts_app.count_delivered_since(db, day_start),
            },
            # Oldest day first, today last — the reading order of the strip.
            "trend": _trend(db, _local_days(timezone_name, now=now)),
        },
        # The three outside dependencies that fail silently. Derived from the
        # operation ledger, never probed live on this read path.
        "connections": connection_status.assess_connections(db),
        "highlights": _select_highlights(feed, discovery, day_start=day_start),
        "reading": {
            "total": int(reading["total"]),
            "items": [_paper(item) for item in reading["items"]],
        },
        "inbox": {
            "total": int(inbox["total"]),
            "items": [_paper(item) for item in inbox["items"]],
        },
        "attention": _attention(db),
    }
