"""Bootstrap endpoint — single API call for initial frontend page load."""

import logging
import sqlite3

from fastapi import APIRouter, Depends

from alma.api.deps import get_current_user, get_db
from alma.application import discovery as discovery_app
from alma.application import feed as feed_app
from alma.version import get_app_version

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

    # Feed badge count — only papers created by the latest completed fetch.
    # "New" is not "untriaged forever"; older untriaged papers remain visible
    # in Feed but stop lighting up the nav after a newer fetch.
    feed_unread = 0
    try:
        feed_unread = feed_app.count_new_feed_items_since_latest_fetch(db)
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

    # Discovery badge: recommendations created by latest successful refresh.
    pending_recs = 0
    new_recs = 0
    try:
        pending_recs = db.execute(
            "SELECT COUNT(*) AS c FROM recommendations WHERE user_action IS NULL"
        ).fetchone()["c"]
        new_recs = discovery_app.count_new_discovery_recommendations(db)
    except Exception:
        pass

    # Onboarding gate. The frontend shows the first-run flow whenever
    # `onboarding.completed` is not set, so it fires on every fresh DB and
    # can be re-triggered from Settings → Restart onboarding. Reuses this
    # single bootstrap fetch — no extra request on boot.
    onboarding_completed = False
    try:
        row = db.execute(
            "SELECT value FROM discovery_settings WHERE key = 'onboarding.completed'"
        ).fetchone()
        onboarding_completed = bool(
            row and str(row["value"]).strip().lower() in ("1", "true", "yes")
        )
    except Exception:
        pass

    has_owner = False
    try:
        has_owner = (
            db.execute(
                "SELECT COUNT(*) AS c FROM followed_authors WHERE is_owner = 1"
            ).fetchone()["c"]
            > 0
        )
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
            "new_recommendations": new_recs,
        },
        "alerts": {
            "active": active_alerts,
        },
        "app": {
            "version": get_app_version(),
        },
        "onboarding": {
            "completed": onboarding_completed,
            "has_owner": has_owner,
        },
    }
