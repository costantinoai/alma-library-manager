"""Topic deduplication and management API routes.

Provides endpoints for:
- Listing canonical topics with alias counts
- Running deterministic deduplication (sync or dry-run)
- Running AI-assisted dedup candidate discovery
- Manually merging two topics
"""

import logging
import sqlite3
import uuid
from datetime import datetime
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Query, status
from pydantic import BaseModel

from alma.api.deps import get_db, get_current_user
from alma.core.redaction import redact_sensitive_text

logger = logging.getLogger(__name__)

router = APIRouter(
    tags=["topics"],
    dependencies=[Depends(get_current_user)],
    responses={401: {"description": "Unauthorized"}},
)


# ---------------------------------------------------------------------------
# Request / response models
# ---------------------------------------------------------------------------


class MergeRequest(BaseModel):
    """Request body for merging two topics."""
    merge_topic_id: str


class TopicResponse(BaseModel):
    """Single canonical topic with metadata."""
    topic_id: str
    canonical_name: str
    normalized_name: str
    source: str
    alias_count: int
    publication_count: int


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------


@router.get(
    "",
    summary="List canonical topics",
    description="Returns all canonical topics with alias and publication counts.",
)
def list_topics(
    db: sqlite3.Connection = Depends(get_db),
    user: dict = Depends(get_current_user),
    limit: int = Query(200, ge=1, le=1000),
    offset: int = Query(0, ge=0),
    search: Optional[str] = Query(None, description="Filter by topic name"),
):
    try:
        from alma.library.topic_deduplication import _ensure_topic_tables
        _ensure_topic_tables(db)
    except Exception:
        pass

    try:
        base_query = """
            SELECT
                t.topic_id,
                t.canonical_name,
                t.normalized_name,
                t.source,
                t.created_at,
                COUNT(DISTINCT ta.alias_id) AS alias_count,
                COUNT(DISTINCT pt.paper_id) AS publication_count
            FROM topics t
            LEFT JOIN topic_aliases ta ON ta.topic_id = t.topic_id
            LEFT JOIN publication_topics pt ON pt.topic_id = t.topic_id
        """
        params = []

        if search:
            base_query += " WHERE t.canonical_name LIKE ? OR t.normalized_name LIKE ?"
            params.extend([f"%{search}%", f"%{search}%"])

        base_query += """
            GROUP BY t.topic_id
            ORDER BY publication_count DESC, t.canonical_name ASC
            LIMIT ? OFFSET ?
        """
        params.extend([limit, offset])

        rows = db.execute(base_query, params).fetchall()

        # Total count for pagination
        count_query = "SELECT COUNT(*) AS c FROM topics"
        count_params = []
        if search:
            count_query += " WHERE canonical_name LIKE ? OR normalized_name LIKE ?"
            count_params = [f"%{search}%", f"%{search}%"]
        total = db.execute(count_query, count_params).fetchone()["c"]

        topics = [dict(r) for r in rows]
        return {
            "topics": topics,
            "total": total,
            "limit": limit,
            "offset": offset,
        }
    except Exception as e:
        logger.error("Failed to list topics: %s", redact_sensitive_text(str(e)))
        raise HTTPException(status_code=500, detail="Failed to list topics")


@router.get(
    "/{topic_id}",
    summary="Get topic details",
    description="Returns a single canonical topic with all its aliases.",
)
def get_topic(
    topic_id: str,
    db: sqlite3.Connection = Depends(get_db),
    user: dict = Depends(get_current_user),
):
    try:
        from alma.library.topic_deduplication import _ensure_topic_tables
        _ensure_topic_tables(db)
    except Exception:
        pass

    row = db.execute(
        "SELECT * FROM topics WHERE topic_id = ?", (topic_id,)
    ).fetchone()
    if not row:
        raise HTTPException(status_code=404, detail="Topic not found")

    aliases = db.execute(
        "SELECT alias_id, raw_term, normalized_term, source, confidence, created_at "
        "FROM topic_aliases WHERE topic_id = ? ORDER BY raw_term",
        (topic_id,),
    ).fetchall()

    pub_count = db.execute(
        "SELECT COUNT(DISTINCT paper_id) AS c "
        "FROM publication_topics WHERE topic_id = ?",
        (topic_id,),
    ).fetchone()["c"]

    return {
        "topic": dict(row),
        "aliases": [dict(a) for a in aliases],
        "publication_count": pub_count,
    }


@router.post(
    "/dedup",
    summary="Run topic deduplication",
    description=(
        "Runs the deterministic deduplication pass: normalizes all "
        "publication_topics terms, groups synonyms, and links to canonical topics."
    ),
)
def run_dedup(
    db: sqlite3.Connection = Depends(get_db),
    user: dict = Depends(get_current_user),
):
    try:
        from alma.library.topic_deduplication import build_canonical_topics
        result = build_canonical_topics(db)
        return {"status": "completed", "result": result}
    except Exception as e:
        logger.error("Topic dedup failed: %s", redact_sensitive_text(str(e)))
        raise HTTPException(status_code=500, detail="Topic deduplication failed")


@router.post(
    "/dedup/dry-run",
    summary="Dry-run topic deduplication",
    description=(
        "Returns proposed merge candidates without applying changes. "
        "Includes both deterministic (token-overlap) and optionally AI-based candidates."
    ),
)
def dedup_dry_run(
    db: sqlite3.Connection = Depends(get_db),
    user: dict = Depends(get_current_user),
    threshold: float = Query(0.85, ge=0.5, le=1.0, description="Minimum similarity threshold"),
    include_ai: bool = Query(False, description="Include AI-based candidates"),
):
    try:
        from alma.library.topic_deduplication import (
            find_similar_topics,
            find_ai_merge_candidates,
            _ensure_topic_tables,
        )
        _ensure_topic_tables(db)

        # First ensure canonical topics exist
        from alma.library.topic_deduplication import build_canonical_topics
        build_result = build_canonical_topics(db)

        # Get deterministic candidates
        deterministic = find_similar_topics(db, threshold=threshold)

        # Optionally get AI candidates
        ai_candidates = []
        if include_ai:
            try:
                from alma.api.scheduler import add_job_log, set_job_status

                job_id = f"topics_ai_dedup_{uuid.uuid4().hex[:8]}"
                set_job_status(
                    job_id,
                    status="running",
                    operation_key="topics.dedup.ai_dry_run",
                    trigger_source="user",
                    message="AI topic dedup dry-run started",
                    started_at=datetime.utcnow().isoformat(),
                )
                add_job_log(job_id, "Computing AI topic merge candidates", step="ai_dedup_start")
                ai_candidates = find_ai_merge_candidates(db, threshold=threshold)
                add_job_log(
                    job_id,
                    f"AI topic merge candidates ready: {len(ai_candidates)}",
                    step="ai_dedup_done",
                    data={"candidates": len(ai_candidates)},
                )
                set_job_status(
                    job_id,
                    status="completed",
                    operation_key="topics.dedup.ai_dry_run",
                    message=f"AI topic dedup dry-run completed ({len(ai_candidates)} candidates)",
                    finished_at=datetime.utcnow().isoformat(),
                )
            except Exception as e:
                logger.warning("AI dedup candidates failed: %s", e)

        return {
            "build_result": build_result,
            "deterministic_candidates": deterministic,
            "ai_candidates": ai_candidates,
            "total_candidates": len(deterministic) + len(ai_candidates),
        }
    except Exception as e:
        logger.error("Topic dedup dry-run failed: %s", redact_sensitive_text(str(e)))
        raise HTTPException(status_code=500, detail="Topic dedup dry-run failed")


@router.post(
    "/{topic_id}/merge",
    summary="Merge two topics",
    description=(
        "Merges the specified topic into the target topic. "
        "All aliases and publication links are transferred."
    ),
)
def merge_topic(
    topic_id: str,
    body: MergeRequest,
    db: sqlite3.Connection = Depends(get_db),
    user: dict = Depends(get_current_user),
):
    try:
        from alma.library.topic_deduplication import merge_topics
        result = merge_topics(db, keep_topic_id=topic_id, merge_topic_id=body.merge_topic_id)
        if "error" in result:
            raise HTTPException(status_code=404, detail=result["error"])
        return {"status": "merged", "result": result}
    except HTTPException:
        raise
    except Exception as e:
        logger.error("Topic merge failed: %s", redact_sensitive_text(str(e)))
        raise HTTPException(status_code=500, detail="Topic merge failed")
