"""Bootstrap endpoint — single API call for initial frontend page load."""

import logging
import sqlite3
from datetime import datetime, timedelta

from fastapi import APIRouter, Depends

from alma.api.deps import get_db, get_current_user

logger = logging.getLogger(__name__)

router = APIRouter(
    tags=["bootstrap"],
    dependencies=[Depends(get_current_user)],
    responses={401: {"description": "Unauthorized"}},
)


@router.get(
    "/bootstrap",
    summary="Bootstrap data for frontend",
    description="Returns summary stats, library counts, and settings needed for the initial page load in a single request.",
)
def get_bootstrap(
    db: sqlite3.Connection = Depends(get_db),
    user: dict = Depends(get_current_user),
):
    # Library counts
    total_papers = db.execute(
        "SELECT COUNT(*) AS c FROM papers WHERE status = 'library'"
    ).fetchone()["c"]

    total_candidates = db.execute(
        "SELECT COUNT(*) AS c FROM papers WHERE status = 'tracked'"
    ).fetchone()["c"]

    total_authors = db.execute(
        "SELECT COUNT(*) AS c FROM authors"
    ).fetchone()["c"]

    followed_authors = 0
    try:
        followed_authors = db.execute(
            "SELECT COUNT(*) AS c FROM followed_authors"
        ).fetchone()["c"]
    except Exception:
        pass

    total_collections = 0
    try:
        total_collections = db.execute(
            "SELECT COUNT(*) AS c FROM collections"
        ).fetchone()["c"]
    except Exception:
        pass

    total_tags = 0
    try:
        total_tags = db.execute(
            "SELECT COUNT(*) AS c FROM tags"
        ).fetchone()["c"]
    except Exception:
        pass

    # Feed unread count — mirror the Feed page's visible window (status='new'
    # AND either publication_date OR fetch date within the last 60 days) so
    # the sidebar badge matches what the user sees when they click into Feed.
    # Must stay in sync with `application/feed.py::list_feed_items`.
    feed_unread = 0
    try:
        feed_cutoff = (datetime.utcnow() - timedelta(days=60)).isoformat()
        feed_unread = db.execute(
            """
            SELECT COUNT(*) AS c
            FROM feed_items fi
            LEFT JOIN papers p ON p.id = fi.paper_id
            WHERE fi.status = 'new'
              AND COALESCE(NULLIF(p.publication_date, ''), fi.fetched_at) >= ?
            """,
            (feed_cutoff,),
        ).fetchone()["c"]
    except Exception:
        pass

    # Active lenses count
    active_lenses = 0
    try:
        active_lenses = db.execute(
            "SELECT COUNT(*) AS c FROM discovery_lenses WHERE is_active = 1"
        ).fetchone()["c"]
    except Exception:
        pass

    # Active alerts count
    active_alerts = 0
    try:
        active_alerts = db.execute(
            "SELECT COUNT(*) AS c FROM alerts WHERE enabled = 1"
        ).fetchone()["c"]
    except Exception:
        pass

    # Pending recommendations
    pending_recs = 0
    try:
        pending_recs = db.execute(
            "SELECT COUNT(*) AS c FROM recommendations WHERE user_action IS NULL"
        ).fetchone()["c"]
    except Exception:
        pass

    return {
        "library": {
            "papers": total_papers,
            "candidates": total_candidates,
            "authors": total_authors,
            "followed_authors": followed_authors,
            "collections": total_collections,
            "tags": total_tags,
        },
        "feed": {
            "unread": feed_unread,
        },
        "discovery": {
            "active_lenses": active_lenses,
            "pending_recommendations": pending_recs,
        },
        "alerts": {
            "active": active_alerts,
        },
    }
