"""Feedback-learning API routes used by the public ALMa surface.

The public product no longer exposes the experimental preference
workbench on `main`, but ALMa still needs two small feedback endpoints:

- `/track` for passive interaction telemetry
- `/reset` for wiping learned feedback state from Settings
"""

from __future__ import annotations

import json
import logging
import sqlite3
import uuid
from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Response, status
from pydantic import BaseModel, Field

from alma.api.deps import get_current_user, get_db
from alma.core.db_write import run_write_unit

logger = logging.getLogger(__name__)

router = APIRouter(
    dependencies=[Depends(get_current_user)],
    responses={401: {"description": "Unauthorized"}},
)


_TRACK_EVENT_TYPES = {
    "external_link_click",
    "abstract_engagement",
    "search_query",
}


class TrackRequest(BaseModel):
    event_type: str = Field(
        ...,
        description="One of: external_link_click, abstract_engagement, search_query",
    )
    paper_id: str | None = Field(
        None, description="Paper ID (null for search queries)"
    )
    context: dict[str, Any] | None = Field(
        None, description="Event-specific context data"
    )


@router.post(
    "/track",
    status_code=status.HTTP_204_NO_CONTENT,
    summary="Track passive interaction events",
)
def track_interaction(
    body: TrackRequest,
    conn: sqlite3.Connection = Depends(get_db),
):
    """Record passive UI interactions without mutating preference profiles."""
    if body.event_type not in _TRACK_EVENT_TYPES:
        raise HTTPException(
            status_code=422,
            detail=f"Invalid event_type '{body.event_type}'. Must be one of {_TRACK_EVENT_TYPES}",
        )

    event_id = uuid.uuid4().hex
    entity_type = "publication" if body.paper_id else "session"
    entity_id = body.paper_id or ""

    # Passive telemetry insert — gated single-statement write. (Not routed
    # through record_feedback: these are non-signal UI events that must NOT
    # touch preference profiles, per this endpoint's contract.)
    run_write_unit(
        conn,
        lambda: conn.execute(
            """INSERT INTO feedback_events
               (id, event_type, entity_type, entity_id, value, context_json)
               VALUES (?, ?, ?, ?, ?, ?)""",
            (
                event_id,
                body.event_type,
                entity_type,
                entity_id,
                None,
                json.dumps(body.context) if body.context else None,
            ),
        ),
        label="feedback_track",
    )
    return Response(status_code=status.HTTP_204_NO_CONTENT)


_FEEDBACK_RESET_TABLES = (
    "feedback_events",
    "lens_signals",
    "missing_author_feedback",
    "author_centroids",
    "author_suggestion_cache",
)


@router.post(
    "/reset",
    summary="Reset learned feedback state",
    description=(
        "Wipes every table that encodes user-derived feedback signal — raw "
        "feedback events, per-lens aggregates, author dismissals, author "
        "centroids, and cached author suggestion buckets — and clears "
        "`recommendations.user_action` so past actions no longer count as "
        "evidence. Saved Library papers, followed authors, lenses, and the "
        "corpus itself are preserved."
    ),
)
def reset_feedback_learning(conn: sqlite3.Connection = Depends(get_db)):
    existing = {
        row[0]
        for row in conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table'"
        ).fetchall()
    }

    def _persist() -> dict[str, int]:
        # One gated unit clears every feedback-encoding table + the
        # recommendation actions, so a reset is all-or-nothing. Per-table
        # OperationalError stays best-effort (a missing/odd table is skipped).
        cleared: dict[str, int] = {}
        for table in _FEEDBACK_RESET_TABLES:
            if table not in existing:
                continue
            try:
                count_row = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()
                count = int(count_row[0] if count_row else 0)
                conn.execute(f"DELETE FROM {table}")  # noqa: S608 — hardcoded allowlist
                cleared[table] = count
            except sqlite3.OperationalError as exc:
                logger.warning("Could not clear feedback-learning table %s: %s", table, exc)

        if "recommendations" in existing:
            try:
                row = conn.execute(
                    "SELECT COUNT(*) FROM recommendations WHERE user_action IS NOT NULL"
                ).fetchone()
                count = int(row[0] if row else 0)
                conn.execute(
                    "UPDATE recommendations SET user_action = NULL, action_at = NULL "
                    "WHERE user_action IS NOT NULL"
                )
                cleared["recommendations.user_action"] = count
            except sqlite3.OperationalError as exc:
                logger.warning("Could not clear recommendations.user_action: %s", exc)
        return cleared

    cleared = run_write_unit(conn, _persist, label="feedback_reset")

    return {
        "success": True,
        "cleared": cleared,
        "total_rows_cleared": sum(cleared.values()),
    }
