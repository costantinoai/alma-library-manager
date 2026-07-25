"""Home endpoints — the landing brief (task 47 Phase 6).

Two routes, and the split between them is the whole design:

* ``GET /home/brief`` answers "what happened, and what needs me" for the window
  since you were last here. It is a PURE READ — it never stamps the visit,
  because a GET that mutates would make a refresh silently destroy the very
  window it just reported (rule §4.2, no writes on GET).
* ``POST /home/seen`` stamps the visit. The page fires it after render, so the
  brief you are looking at always describes the window you actually missed.

Every number delegates to the module that already owns it (feed, discovery,
imports) rather than re-deriving it here, so Home can never disagree with the
page it links to.
"""

import logging
import sqlite3
from datetime import datetime, timedelta, timezone
from typing import Any

from fastapi import APIRouter, Depends, Query
from pydantic import BaseModel

from alma.api.deps import get_current_user, get_db
from alma.api.helpers import raise_internal
from alma.application import imports as imports_app
from alma.application.discovery.lens_crud import read_settings, upsert_setting
from alma.core.db_write import run_write_unit
from alma.core.sql_helpers import standalone_paper_sql

logger = logging.getLogger(__name__)

router = APIRouter(
    tags=["home"],
    dependencies=[Depends(get_current_user)],
    responses={401: {"description": "Unauthorized"}},
)

#: KV key holding the ISO-8601 UTC timestamp of the last Home visit. Lives in
#: the same `discovery_settings` KV every other durable app setting uses.
LAST_SEEN_KEY = "home.last_seen_at"

#: How far back a FIRST-EVER visit looks. Without a stamp there is no honest
#: "since" window, so the page says "here's where things stand" and we report
#: the same horizon the Feed inbox itself is bounded to (D-feed: 60 days).
FIRST_VISIT_WINDOW_DAYS = 60


def _count(db: sqlite3.Connection, sql: str, params: tuple = ()) -> int:
    """Best-effort scalar count. A missing table on a partially-migrated DB
    must never take the landing page down — the number reads 0 instead."""
    try:
        row = db.execute(sql, params).fetchone()
        return int((row[0] if row else 0) or 0)
    except sqlite3.OperationalError:
        return 0


@router.get(
    "/brief",
    summary="Home brief",
    description="Counts and samples for what arrived since the last visit, plus what needs attention.",
)
def get_home_brief(
    since: str | None = Query(
        None,
        description="ISO timestamp to measure from. Defaults to the stored last-visit stamp.",
    ),
    db: sqlite3.Connection = Depends(get_db),
    user: dict = Depends(get_current_user),
):
    try:
        settings = read_settings(db)
        stored_seen = str(settings.get(LAST_SEEN_KEY) or "").strip()
        # Explicit ?since wins (lets a caller ask "what changed since X"), then
        # the stored stamp. Neither → first visit.
        window_start = (since or "").strip() or stored_seen
        first_visit = not window_start
        if first_visit:
            window_start = (
                datetime.now(timezone.utc) - timedelta(days=FIRST_VISIT_WINDOW_DAYS)
            ).isoformat()

        # ── What arrived ────────────────────────────────────────────────────
        new_feed_items = _count(
            db,
            f"""
            SELECT COUNT(*) FROM feed_items fi
            JOIN papers p ON p.id = fi.paper_id
            WHERE fi.fetched_at > ?
              AND COALESCE(fi.status, '') NOT IN ('dismissed')
              AND {standalone_paper_sql('p')}
            """,
            (window_start,),
        )
        alerts_fired = _count(
            db,
            "SELECT COUNT(*) FROM alert_history WHERE sent_at > ?",
            (window_start,),
        )
        new_recommendations = _count(
            db,
            f"""
            SELECT COUNT(*) FROM recommendations r
            JOIN papers p ON p.id = r.paper_id
            WHERE r.created_at > ?
              AND r.user_action IS NULL
              AND {standalone_paper_sql('p')}
            """,
            (window_start,),
        )

        # ── What's waiting (state, not delta — these don't expire) ──────────
        reading_queue = _count(
            db,
            f"""
            SELECT COUNT(*) FROM papers p
            WHERE p.status = 'library'
              AND COALESCE(p.reading_status, '') = 'reading'
              AND {standalone_paper_sql('p')}
            """,
        )
        try:
            imports_pending = imports_app.count_resolution_queue(db)
        except Exception:
            imports_pending = 0

        # Monitors that can't run until you re-link them. The flag lives inside
        # `config_json` (migration #0028 stamps it and disables the row), which
        # is why this reads the JSON rather than a column.
        monitors_need_attention = _count(
            db,
            """
            SELECT COUNT(*) FROM feed_monitors
            WHERE json_extract(COALESCE(config_json, '{}'), '$.needs_resolution') IN (1, 'true')
            """,
        )

        # ── One thing worth looking at ──────────────────────────────────────
        # Top-ranked, un-acted recommendation from the most recent suggestion
        # set. Carried in this payload rather than fetched separately so the
        # landing page costs exactly ONE request.
        insight: dict[str, Any] | None = None
        try:
            row = db.execute(
                f"""
                SELECT r.id AS rec_id, r.paper_id, r.score, r.lens_id, r.score_breakdown,
                       p.title, p.authors, p.year, p.journal, p.url, p.doi,
                       l.name AS lens_name
                FROM recommendations r
                JOIN papers p ON p.id = r.paper_id
                LEFT JOIN discovery_lenses l ON l.id = r.lens_id
                WHERE r.suggestion_set_id = (
                        SELECT id FROM suggestion_sets ORDER BY created_at DESC LIMIT 1
                      )
                  AND r.user_action IS NULL
                  AND COALESCE(p.status, '') NOT IN ('dismissed', 'removed', 'library')
                  AND {standalone_paper_sql('p')}
                ORDER BY r.rank ASC
                LIMIT 1
                """
            ).fetchone()
            if row is not None:
                insight = {
                    # The RECOMMENDATION id — dismissing acts on the rec row,
                    # not the paper (D6: dismiss hides + writes a negative signal).
                    "id": str(row["rec_id"]),
                    "paper_id": str(row["paper_id"]),
                    "title": row["title"],
                    "authors": row["authors"],
                    "year": row["year"],
                    "journal": row["journal"],
                    "url": row["url"],
                    "doi": row["doi"],
                    "score": row["score"],
                    "score_breakdown": row["score_breakdown"],
                    "lens_id": row["lens_id"],
                    "lens_name": row["lens_name"],
                }
        except sqlite3.OperationalError:
            insight = None

        return {
            "since": window_start,
            "first_visit": first_visit,
            "last_seen_at": stored_seen or None,
            "insight": insight,
            "arrived": {
                "feed_items": new_feed_items,
                "alerts_fired": alerts_fired,
                "recommendations": new_recommendations,
            },
            "waiting": {
                "reading": reading_queue,
                "imports_pending": imports_pending,
                "monitors_need_attention": monitors_need_attention,
            },
        }
    except Exception as e:  # pragma: no cover - defensive
        raise_internal("Failed to build the home brief", e)


class HomeSeenResponse(BaseModel):
    last_seen_at: str


@router.post(
    "/seen",
    response_model=HomeSeenResponse,
    summary="Stamp the current Home visit",
    description="Records now() as the last-visit time so the next brief measures from here.",
)
def post_home_seen(
    db: sqlite3.Connection = Depends(get_db),
    user: dict = Depends(get_current_user),
):
    """Stamp the visit. Separate from the brief on purpose: the page reads
    first, renders, and only then marks the window consumed."""
    stamp = datetime.now(timezone.utc).isoformat()
    try:
        # The request's OWN gated connection (BEGIN IMMEDIATE + lock retry) —
        # never a second connection (SQLite write discipline).
        run_write_unit(db, lambda: upsert_setting(db, LAST_SEEN_KEY, stamp))
    except Exception as e:
        raise_internal("Failed to stamp the home visit", e)
    return HomeSeenResponse(last_seen_at=stamp)
