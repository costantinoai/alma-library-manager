"""Tag suggestion routes: generate, read, accept, dismiss.

Mounted at ``/api/v1/tags/suggestions`` behind the add-on gate. Merging tags stays
in core; a merge reaches ``tag_suggestions`` through ``on_tag_merged``.
"""

import logging
import sqlite3
import uuid

from fastapi import APIRouter, Depends, HTTPException, status
from pydantic import BaseModel, Field

from alma.api.deps import get_current_user, get_db, open_db_connection
from alma.api.helpers import raise_internal
from alma.api.routes.library import MAX_TAGS_PER_PAPER, _paper_tag_count
from alma.core.db_write import run_write_unit
from alma.core.time import utcnow

logger = logging.getLogger(__name__)

router = APIRouter(
    dependencies=[Depends(get_current_user)],
    responses={401: {"description": "Unauthorized"}},
)


# ---------------------------------------------------------------------------
# Request / Response models
# ---------------------------------------------------------------------------


class TagSuggestionResponse(BaseModel):
    """A single tag suggestion for a publication."""

    paper_id: str = Field(..., description="Paper UUID")
    tag: str = Field(..., description="Suggested tag name")
    tag_id: str | None = Field(None, description="Tag UUID (if tag already exists)")
    confidence: float = Field(..., description="Confidence score (0-1)")
    source: str = Field(..., description="Suggestion source: embedding, tfidf, topic, or rule")
    accepted: bool = Field(False, description="Whether the suggestion has been accepted")
    created_at: str | None = Field(None, description="When the suggestion was created")


class TagSuggestionsResponse(BaseModel):
    """Wrapped response for publication tag suggestions."""

    suggestions: list[TagSuggestionResponse] = Field(
        default_factory=list,
        description="Tag suggestions for the requested publication",
    )
    paper_title: str | None = Field(
        None,
        description="Publication title when available",
    )


class AcceptTagRequest(BaseModel):
    """Request body for accepting a tag suggestion."""

    tag: str = Field(..., description="Tag name to accept")


class BulkSuggestResponse(BaseModel):
    """Response for bulk tag suggestion generation."""

    job_id: str = Field(..., description="Background job identifier")
    operation_id: str | None = Field(None, description="Canonical operation identifier")
    status: str | None = Field(None, description="Operation enqueue status")
    activity_url: str | None = Field(None, description="Activity log URL")
    operation_key: str | None = Field(None, description="Operation dedupe key")
    message: str = Field(..., description="Status message")


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------


@router.get(
    "/{paper_id}",
    response_model=TagSuggestionsResponse,
    summary="Get tag suggestions for a publication",
    description="Returns AI-generated tag suggestions for the specified publication.",
)
def get_tag_suggestions(
    paper_id: str,
    max_tags: int = 5,
    db: sqlite3.Connection = Depends(get_db),
    _user: dict = Depends(get_current_user),
) -> TagSuggestionsResponse:
    """Get cached tag suggestions for a single publication.

    Args:
        paper_id: Paper UUID identifier.
        max_tags: Maximum number of suggestions to return.

    Returns:
        List of tag suggestion objects.
    """
    try:
        title_row = db.execute(
            "SELECT title FROM papers WHERE id = ? LIMIT 1",
            (paper_id,),
        ).fetchone()
        paper_title = title_row["title"] if title_row else None

        cached = db.execute(
            """
            SELECT paper_id, tag, tag_id, confidence, source, accepted, created_at
            FROM tag_suggestions
            WHERE paper_id = ?
            ORDER BY confidence DESC
            LIMIT ?
            """,
            (paper_id, max_tags),
        ).fetchall()

        return TagSuggestionsResponse(
            suggestions=[
                TagSuggestionResponse(
                    paper_id=paper_id,
                    tag=row["tag"],
                    tag_id=row["tag_id"],
                    confidence=row["confidence"],
                    source=row["source"],
                    accepted=bool(row["accepted"]),
                    created_at=row["created_at"],
                )
                for row in cached
            ],
            paper_title=paper_title,
        )

    except Exception as exc:
        raise_internal(
            f"Failed to generate tag suggestions for {paper_id}",
            exc,
        )


@router.post(
    "/generate",
    response_model=BulkSuggestResponse,
    summary="Bulk generate tag suggestions",
    description="Generate tag suggestions for all publications that don't have them yet. Runs as a background job.",
)
def bulk_generate_suggestions(
    db: sqlite3.Connection = Depends(get_db),
    _user: dict = Depends(get_current_user),
) -> BulkSuggestResponse:
    """Trigger bulk tag suggestion generation as a background job.

    Generates suggestions for all publications that don't have any
    suggestions in the tag_suggestions table yet.

    Returns:
        Job ID and status message.
    """
    from alma.api.scheduler import (
        activity_envelope,
        find_active_job,
        schedule_immediate,
        set_job_status,
    )

    operation_key = "tags.bulk_generate_suggestions"
    existing = find_active_job(operation_key)
    if existing:
        env = activity_envelope(
            str(existing.get("job_id") or ""),
            status="already_running",
            operation_key=operation_key,
            message="Bulk tag suggestion job already running",
        )
        return BulkSuggestResponse(
            job_id=env["job_id"],
            operation_id=env.get("operation_id"),
            status=env.get("status"),
            activity_url=env.get("activity_url"),
            operation_key=env.get("operation_key"),
            message=env.get("message") or "Already running",
        )

    job_id = f"bulk_tag_suggest_{uuid.uuid4().hex[:8]}"

    set_job_status(
        job_id,
        status="queued",
        operation_key=operation_key,
        trigger_source="user",
        message="Bulk tag suggestion generation queued",
        started_at=utcnow().isoformat(),
    )

    schedule_immediate(
        job_id,
        _run_bulk_tag_suggestions,
        job_id,
    )

    env = activity_envelope(
        job_id,
        status="queued",
        operation_key=operation_key,
        message="Bulk tag suggestion generation started",
    )
    return BulkSuggestResponse(
        job_id=env["job_id"],
        operation_id=env.get("operation_id"),
        status=env.get("status"),
        activity_url=env.get("activity_url"),
        operation_key=env.get("operation_key"),
        message=env.get("message") or "Bulk tag suggestion generation started",
        )


@router.post(
    "/{paper_id}/accept",
    summary="Accept a tag suggestion",
    description="Accept a tag suggestion, creating a real tag assignment for the publication.",
)
def accept_tag_suggestion(
    paper_id: str,
    body: AcceptTagRequest,
    db: sqlite3.Connection = Depends(get_db),
    _user: dict = Depends(get_current_user),
) -> dict:
    """Accept a tag suggestion.

    Looks up the suggestion, creates a real tag assignment in
    publication_tags, and marks the suggestion as accepted.

    Args:
        paper_id: Paper UUID identifier.
        body: Request body containing the tag name to accept.

    Returns:
        Success status with the created assignment details.
    """
    tag_name = body.tag

    # Find the suggestion
    suggestion = db.execute(
        "SELECT * FROM tag_suggestions WHERE paper_id = ? AND tag = ?",
        (paper_id, tag_name),
    ).fetchone()

    if suggestion is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"No suggestion found for tag '{tag_name}'",
        )

    # Resolve the tag_id (existing) or decide to create one — all reads, so
    # they stay outside the write unit.
    tag_id = suggestion["tag_id"]

    if tag_id:
        # Verify the tag still exists
        tag_row = db.execute("SELECT id FROM tags WHERE id = ?", (tag_id,)).fetchone()
        if tag_row is None:
            tag_id = None

    create_tag = False
    if not tag_id:
        # Try to find existing tag by name
        tag_row = db.execute("SELECT id FROM tags WHERE name = ?", (tag_name,)).fetchone()
        if tag_row:
            tag_id = tag_row["id"]
        else:
            # New tag — generate the id now, create it inside the unit.
            tag_id = uuid.uuid4().hex
            create_tag = True

    # Verify paper exists
    pub_row = db.execute(
        "SELECT id FROM papers WHERE id = ?",
        (paper_id,),
    ).fetchone()

    if pub_row is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Publication not found",
        )

    existing_assignment = db.execute(
        "SELECT 1 FROM publication_tags WHERE paper_id = ? AND tag_id = ?",
        (paper_id, tag_id),
    ).fetchone()
    if existing_assignment is None and _paper_tag_count(db, paper_id) >= MAX_TAGS_PER_PAPER:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail=f"Each paper may have at most {MAX_TAGS_PER_PAPER} tags",
        )

    def _persist() -> None:
        # One atomic unit: create the tag (if new), assign it, and mark the
        # suggestion accepted — so a retry never leaves an orphan tag row.
        if create_tag:
            db.execute(
                "INSERT INTO tags (id, name) VALUES (?, ?)",
                (tag_id, tag_name),
            )
        db.execute(
            "INSERT OR IGNORE INTO publication_tags (paper_id, tag_id) VALUES (?, ?)",
            (paper_id, tag_id),
        )
        db.execute(
            "UPDATE tag_suggestions SET accepted = 1 WHERE paper_id = ? AND tag = ?",
            (paper_id, tag_name),
        )

    run_write_unit(db, _persist, label="tag_accept_suggestion")

    return {
        "success": True,
        "paper_id": paper_id,
        "tag": tag_name,
        "tag_id": tag_id,
    }


@router.delete(
    "/{paper_id}/{tag}",
    status_code=status.HTTP_204_NO_CONTENT,
    summary="Dismiss a tag suggestion",
    description="Remove a tag suggestion for a publication.",
)
def dismiss_tag_suggestion(
    paper_id: str,
    tag: str,
    db: sqlite3.Connection = Depends(get_db),
    _user: dict = Depends(get_current_user),
) -> None:
    """Dismiss (delete) a tag suggestion.

    Args:
        paper_id: Paper UUID identifier.
        tag: Tag name to dismiss.
    """
    def _persist() -> int:
        result = db.execute(
            "DELETE FROM tag_suggestions WHERE paper_id = ? AND tag = ?",
            (paper_id, tag),
        )
        return result.rowcount

    if run_write_unit(db, _persist, label="tag_dismiss_suggestion") == 0:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"No suggestion found for tag '{tag}'",
        )


# ---------------------------------------------------------------------------
# Background task
# ---------------------------------------------------------------------------


def _run_bulk_tag_suggestions(job_id: str) -> None:
    """Background function that generates tag suggestions for all publications.

    Opens its own database connection (since this runs in a scheduler
    thread, not a FastAPI request context).
    """
    from alma.api.scheduler import add_job_log, is_cancellation_requested, set_job_status

    try:
        from alma.config import get_db_path

        get_db_path()
    except Exception as exc:
        set_job_status(
            job_id,
            status="failed",
            message=f"Could not resolve publications DB path: {exc}",
            finished_at=utcnow().isoformat(),
        )
        return

    conn = open_db_connection()

    try:
        set_job_status(
            job_id,
            status="running",
            message="Generating tag suggestions for publications",
        )
        if is_cancellation_requested(job_id):
            set_job_status(
                job_id,
                status="cancelled",
                message="Bulk tag suggestion cancelled before execution",
                finished_at=utcnow().isoformat(),
            )
            return

        from alma.addons.auto_tagger.tagger import bulk_suggest_tags

        def _progress(processed: int, total: int, generated: int, errors: int, paper_id: str) -> None:
            message = f"AI tag suggestions {processed}/{total} (generated={generated}, errors={errors})"
            set_job_status(
                job_id,
                status="running",
                message=message,
                processed=processed,
                total=total,
                generated=generated,
                errors=errors,
            )
            add_job_log(
                job_id,
                message,
                step="tag_suggestion_progress",
                data={"paper_id": paper_id, "processed": processed, "total": total},
            )

        add_job_log(job_id, "Starting AI tag suggestion generation", step="tag_suggestion_start")
        result = bulk_suggest_tags(conn, progress_callback=_progress)

        set_job_status(
            job_id,
            status="completed",
            message=(
                f"Completed: {result['generated']} papers tagged out of "
                f"{result['total']} ({result['errors']} errors)"
            ),
            total=result["total"],
            generated=result["generated"],
            errors=result["errors"],
            finished_at=utcnow().isoformat(),
        )

    except Exception as exc:
        logger.exception("Bulk tag suggestion job %s failed: %s", job_id, exc)
        set_job_status(
            job_id,
            status="failed",
            message=f"Bulk tag suggestion failed: {exc}",
            finished_at=utcnow().isoformat(),
        )
    finally:
        conn.close()
