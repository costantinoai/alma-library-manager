"""Tag suggestion API endpoints.

Provides endpoints for generating, retrieving, accepting, and dismissing
AI-generated tag suggestions for publications.
"""

import logging
import re
import sqlite3
import uuid
from datetime import datetime
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, status
from pydantic import BaseModel, Field

from alma.api.deps import get_db, get_current_user, open_db_connection
from alma.api.helpers import raise_internal

logger = logging.getLogger(__name__)
MAX_TAGS_PER_PAPER = 5

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
    tag_id: Optional[str] = Field(None, description="Tag UUID (if tag already exists)")
    confidence: float = Field(..., description="Confidence score (0-1)")
    source: str = Field(..., description="Suggestion source: embedding, tfidf, topic, or rule")
    accepted: bool = Field(False, description="Whether the suggestion has been accepted")
    created_at: Optional[str] = Field(None, description="When the suggestion was created")


class TagSuggestionsResponse(BaseModel):
    """Wrapped response for publication tag suggestions."""

    suggestions: list[TagSuggestionResponse] = Field(
        default_factory=list,
        description="Tag suggestions for the requested publication",
    )
    paper_title: Optional[str] = Field(
        None,
        description="Publication title when available",
    )


class AcceptTagRequest(BaseModel):
    """Request body for accepting a tag suggestion."""

    tag: str = Field(..., description="Tag name to accept")


class BulkSuggestResponse(BaseModel):
    """Response for bulk tag suggestion generation."""

    job_id: str = Field(..., description="Background job identifier")
    operation_id: Optional[str] = Field(None, description="Canonical operation identifier")
    status: Optional[str] = Field(None, description="Operation enqueue status")
    activity_url: Optional[str] = Field(None, description="Activity log URL")
    operation_key: Optional[str] = Field(None, description="Operation dedupe key")
    message: str = Field(..., description="Status message")


class TagMergeSuggestionResponse(BaseModel):
    """Suggested merge candidate for duplicate/overlapping tags."""

    source_tag_id: str
    source_tag: str
    target_tag_id: str
    target_tag: str
    confidence: float
    reason: str


class TagMergeRequest(BaseModel):
    """Request body for merging tags."""

    source_tag_id: str
    target_tag_id: str


def _normalize_tag_name(value: str) -> str:
    return " ".join(re.sub(r"[^a-z0-9\s]+", " ", (value or "").lower()).split())


def _token_set(value: str) -> set[str]:
    normalized = _normalize_tag_name(value)
    if not normalized:
        return set()
    return {part for part in normalized.split(" ") if part}


def _merge_pair_score(name_a: str, name_b: str) -> tuple[float, str]:
    norm_a = _normalize_tag_name(name_a)
    norm_b = _normalize_tag_name(name_b)
    if not norm_a or not norm_b:
        return (0.0, "")
    if norm_a == norm_b:
        return (0.99, "normalized_exact")

    singular_a = norm_a[:-1] if norm_a.endswith("s") else norm_a
    singular_b = norm_b[:-1] if norm_b.endswith("s") else norm_b
    if singular_a == singular_b:
        return (0.9, "pluralization")

    tokens_a = _token_set(name_a)
    tokens_b = _token_set(name_b)
    if not tokens_a or not tokens_b:
        return (0.0, "")

    inter = len(tokens_a & tokens_b)
    union = len(tokens_a | tokens_b)
    jaccard = float(inter / union) if union else 0.0
    if jaccard >= 0.8:
        return (round(jaccard, 3), "token_overlap")
    return (0.0, "")


def _paper_tag_count(db: sqlite3.Connection, paper_id: str) -> int:
    row = db.execute(
        "SELECT COUNT(DISTINCT tag_id) AS c FROM publication_tags WHERE paper_id = ?",
        (paper_id,),
    ).fetchone()
    return int((row["c"] if row else 0) or 0)


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------


@router.get(
    "/suggestions/{paper_id}",
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
    "/suggestions/generate",
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
    from alma.api.scheduler import set_job_status, schedule_immediate
    from alma.api.scheduler import activity_envelope, find_active_job

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
        started_at=datetime.utcnow().isoformat(),
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


@router.get(
    "/merge-suggestions",
    response_model=list[TagMergeSuggestionResponse],
    summary="List suggested tag merges",
    description="Find likely duplicate/overlapping tags based on normalized names and token overlap.",
)
def list_tag_merge_suggestions(
    limit: int = 25,
    min_confidence: float = 0.8,
    db: sqlite3.Connection = Depends(get_db),
    _user: dict = Depends(get_current_user),
) -> list[TagMergeSuggestionResponse]:
    try:
        rows = db.execute(
            """
            SELECT
                t.id,
                t.name,
                COUNT(pt.paper_id) AS usage_count
            FROM tags t
            LEFT JOIN publication_tags pt ON pt.tag_id = t.id
            GROUP BY t.id, t.name
            ORDER BY t.name
            """
        ).fetchall()
        tags = [dict(r) for r in rows]
        if len(tags) < 2:
            return []

        suggestions: list[TagMergeSuggestionResponse] = []
        seen_pairs: set[tuple[str, str]] = set()

        for i in range(len(tags)):
            for j in range(i + 1, len(tags)):
                a = tags[i]
                b = tags[j]
                score, reason = _merge_pair_score(str(a["name"]), str(b["name"]))
                if score < float(min_confidence):
                    continue

                source = a
                target = b
                usage_a = int(a.get("usage_count") or 0)
                usage_b = int(b.get("usage_count") or 0)
                name_a = str(a.get("name") or "").lower()
                name_b = str(b.get("name") or "").lower()
                prefer_a = usage_a > usage_b or (usage_a == usage_b and name_a < name_b)
                if prefer_a:
                    source = b
                    target = a
                pair_key = (str(source["id"]), str(target["id"]))
                if pair_key in seen_pairs:
                    continue
                seen_pairs.add(pair_key)

                suggestions.append(
                    TagMergeSuggestionResponse(
                        source_tag_id=str(source["id"]),
                        source_tag=str(source["name"]),
                        target_tag_id=str(target["id"]),
                        target_tag=str(target["name"]),
                        confidence=round(float(score), 3),
                        reason=reason,
                    )
                )

        suggestions.sort(key=lambda x: x.confidence, reverse=True)
        return suggestions[: max(1, int(limit))]
    except Exception as exc:
        raise_internal("Failed to compute tag merge suggestions", exc)


@router.post(
    "/merge",
    summary="Merge one tag into another",
    description="Reassign all source tag usages/suggestions to target tag and delete source tag.",
)
def merge_tags(
    body: TagMergeRequest,
    db: sqlite3.Connection = Depends(get_db),
    _user: dict = Depends(get_current_user),
) -> dict:
    source_tag_id = (body.source_tag_id or "").strip()
    target_tag_id = (body.target_tag_id or "").strip()
    if not source_tag_id or not target_tag_id:
        raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY, detail="source_tag_id and target_tag_id are required")
    if source_tag_id == target_tag_id:
        raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY, detail="source_tag_id and target_tag_id must differ")

    source = db.execute("SELECT id, name FROM tags WHERE id = ?", (source_tag_id,)).fetchone()
    target = db.execute("SELECT id, name FROM tags WHERE id = ?", (target_tag_id,)).fetchone()
    if source is None or target is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Source or target tag not found")

    db.execute(
        """
        INSERT OR IGNORE INTO publication_tags (paper_id, tag_id)
        SELECT paper_id, ?
        FROM publication_tags
        WHERE tag_id = ?
        """,
        (target_tag_id, source_tag_id),
    )
    db.execute("DELETE FROM publication_tags WHERE tag_id = ?", (source_tag_id,))

    db.execute(
        """
        INSERT OR IGNORE INTO tag_suggestions
            (paper_id, tag, tag_id, confidence, source, accepted, created_at)
        SELECT
            paper_id,
            ?,
            ?,
            confidence,
            source,
            accepted,
            created_at
        FROM tag_suggestions
        WHERE tag_id = ?
        """,
        (target["name"], target_tag_id, source_tag_id),
    )
    db.execute("DELETE FROM tag_suggestions WHERE tag_id = ?", (source_tag_id,))
    db.execute("DELETE FROM tags WHERE id = ?", (source_tag_id,))
    db.commit()

    return {
        "success": True,
        "source_tag_id": source_tag_id,
        "source_tag": source["name"],
        "target_tag_id": target_tag_id,
        "target_tag": target["name"],
    }


@router.post(
    "/suggestions/{paper_id}/accept",
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

    # Get or create the tag
    tag_id = suggestion["tag_id"]

    if tag_id:
        # Verify the tag still exists
        tag_row = db.execute("SELECT id FROM tags WHERE id = ?", (tag_id,)).fetchone()
        if tag_row is None:
            tag_id = None

    if not tag_id:
        # Try to find existing tag by name
        tag_row = db.execute("SELECT id FROM tags WHERE name = ?", (tag_name,)).fetchone()
        if tag_row:
            tag_id = tag_row["id"]
        else:
            # Create new tag
            tag_id = uuid.uuid4().hex
            db.execute(
                "INSERT INTO tags (id, name) VALUES (?, ?)",
                (tag_id, tag_name),
            )

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

    # Create the tag assignment
    try:
        db.execute(
            "INSERT OR IGNORE INTO publication_tags (paper_id, tag_id) VALUES (?, ?)",
            (paper_id, tag_id),
        )
    except sqlite3.IntegrityError:
        pass  # Tag already assigned

    # Mark suggestion as accepted
    db.execute(
        "UPDATE tag_suggestions SET accepted = 1 WHERE paper_id = ? AND tag = ?",
        (paper_id, tag_name),
    )

    db.commit()

    return {
        "success": True,
        "paper_id": paper_id,
        "tag": tag_name,
        "tag_id": tag_id,
    }


@router.delete(
    "/suggestions/{paper_id}/{tag}",
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
    result = db.execute(
        "DELETE FROM tag_suggestions WHERE paper_id = ? AND tag = ?",
        (paper_id, tag),
    )

    if result.rowcount == 0:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"No suggestion found for tag '{tag}'",
        )

    db.commit()


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

        db_path = str(get_db_path())
    except Exception as exc:
        set_job_status(
            job_id,
            status="failed",
            message=f"Could not resolve publications DB path: {exc}",
            finished_at=datetime.utcnow().isoformat(),
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
                finished_at=datetime.utcnow().isoformat(),
            )
            return

        from alma.ai.auto_tagger import bulk_suggest_tags

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
            finished_at=datetime.utcnow().isoformat(),
        )

    except Exception as exc:
        logger.exception("Bulk tag suggestion job %s failed: %s", job_id, exc)
        set_job_status(
            job_id,
            status="failed",
            message=f"Bulk tag suggestion failed: {exc}",
            finished_at=datetime.utcnow().isoformat(),
        )
    finally:
        conn.close()
