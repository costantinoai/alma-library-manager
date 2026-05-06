"""Paper query API endpoints."""

import hashlib
import logging
import sqlite3
import uuid
import json
from datetime import datetime, timedelta
from typing import List, Optional
from fastapi import APIRouter, Depends, HTTPException, status, Query
from pydantic import BaseModel, Field

from alma.api.deps import get_db, get_current_user
from alma.api.helpers import raise_internal, row_to_paper_response
from alma.api.models import PaperResponse, ErrorResponse
from alma.application import library as library_app
from alma.application import authors as authors_app

logger = logging.getLogger(__name__)

router = APIRouter(
    prefix="/papers",
    tags=["papers"],
    responses={
        401: {"model": ErrorResponse, "description": "Unauthorized"},
        500: {"model": ErrorResponse, "description": "Internal Server Error"},
    }
)


class SemanticPaperSearchRequest(BaseModel):
    """Explicit semantic paper search request."""

    query: str = Field(..., min_length=1, description="Short semantic search query")
    scope: str = Field("library", description="Search scope: library | all")
    limit: int = Field(20, ge=1, le=100, description="Maximum semantic results")


@router.get(
    "",
    response_model=List[PaperResponse],
    summary="Query papers",
    description="Search and filter papers across all authors.",
)
def query_publications(
    scope: Optional[str] = Query(None, description="Paper scope: all | library | background | followed_corpus"),
    status_filter: Optional[str] = Query(None, alias="status", description="Membership status: tracked | library | dismissed | removed"),
    added_from: Optional[str] = Query(None, description="Filter by acquisition/provenance value"),
    openalex_resolution_status: Optional[str] = Query(None, description="Filter by OpenAlex resolution status"),
    has_topics: Optional[bool] = Query(None, description="Filter by presence of publication_topics rows"),
    has_tags: Optional[bool] = Query(None, description="Filter by presence of publication_tags rows"),
    author_id: Optional[str] = Query(None, description="Optional author ID to constrain results to one author corpus"),
    year: Optional[int] = Query(None, description="Filter by specific year"),
    min_year: Optional[int] = Query(None, description="Minimum year (inclusive)"),
    max_year: Optional[int] = Query(None, description="Maximum year (inclusive)"),
    min_citations: Optional[int] = Query(None, description="Minimum citations"),
    search: Optional[str] = Query(None, description="Search in title and abstract"),
    semantic: bool = Query(False, description="Enable semantic search (requires AI provider)"),
    order: Optional[str] = Query(
        None,
        description="Sort order: citations | recent | title | rating | authors | journal | status | added_at",
    ),
    order_dir: Optional[str] = Query(
        None,
        description="Sort direction: asc | desc",
    ),
    limit: int = Query(100, ge=1, le=1000, description="Maximum results"),
    offset: int = Query(0, ge=0, description="Results to skip"),
    db: sqlite3.Connection = Depends(get_db),
    user: dict = Depends(get_current_user),
):
    """Query papers with various filters.

    This endpoint supports multiple filter criteria that can be combined:
    - Filter by year (exact, range)
    - Filter by minimum citations
    - Full-text search in title and abstract
    - Pagination with limit/offset

    Returns:
        List[PaperResponse]: Matching papers ordered by citations

    Example:
        ```bash
        # Get recent highly-cited papers
        curl "http://localhost:8000/api/v1/papers?min_year=2023&min_citations=50&limit=20"

        # Search for specific topics
        curl "http://localhost:8000/api/v1/papers?search=neural+networks"
        ```
    """
    if semantic and search:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Semantic search uses live AI inference and must run through an Activity-backed AI action.",
        )

    try:
        # Build dynamic query. Exclude preprint rows that merged into a
        # published journal twin (see `alma.application.preprint_dedup`)
        # — they keep their UUID for FK integrity but shouldn't appear as
        # duplicate cards in the /papers listing.
        query_parts = [
            "SELECT p.* FROM papers p "
            "WHERE COALESCE(p.canonical_paper_id, '') = ''"
        ]
        params = []

        scope_value = str(scope or "all").strip().lower()
        if scope_value == "library":
            query_parts.append("AND p.status = 'library'")
        elif scope_value in {"background", "non_library"}:
            query_parts.append("AND p.status <> 'library'")
        elif scope_value in {"followed_corpus", "followed_author_corpus"}:
            query_parts.append(
                """
                AND EXISTS (
                    SELECT 1
                    FROM publication_authors pa
                    JOIN authors a ON lower(trim(a.openalex_id)) = lower(trim(pa.openalex_id))
                    JOIN followed_authors fa ON fa.author_id = a.id
                    WHERE pa.paper_id = p.id
                )
                """
            )

        status_value = str(status_filter or "").strip().lower()
        if status_value:
            if status_value not in {
                library_app.TRACKED_STATUS,
                library_app.LIBRARY_STATUS,
                library_app.DISMISSED_STATUS,
                library_app.REMOVED_STATUS,
            }:
                raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Invalid paper status filter")
            query_parts.append("AND p.status = ?")
            params.append(status_value)

        added_from_value = str(added_from or "").strip()
        if added_from_value:
            query_parts.append("AND COALESCE(p.added_from, '') = ?")
            params.append(added_from_value)

        resolution_value = str(openalex_resolution_status or "").strip()
        if resolution_value:
            query_parts.append("AND COALESCE(p.openalex_resolution_status, '') = ?")
            params.append(resolution_value)

        if has_topics is not None:
            query_parts.append(
                f"""AND {'EXISTS' if has_topics else 'NOT EXISTS'} (
                    SELECT 1 FROM publication_topics pt WHERE pt.paper_id = p.id
                )"""
            )

        if has_tags is not None:
            query_parts.append(
                f"""AND {'EXISTS' if has_tags else 'NOT EXISTS'} (
                    SELECT 1 FROM publication_tags tag_rel WHERE tag_rel.paper_id = p.id
                )"""
            )

        if author_id:
            author = authors_app.get_author(db, author_id)
            if author is None:
                raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Author not found")
            author_name = str(author.get("name") or "").strip()
            openalex_id = str(author.get("openalex_id") or "").strip()
            clause, clause_params = authors_app._author_paper_clause(  # type: ignore[attr-defined]
                db,
                author_id=author_id,
                author_name=author_name,
                openalex_id=openalex_id,
            )
            if not clause:
                return []
            query_parts.append(f"AND {clause}")
            params.extend(clause_params)

        if year:
            query_parts.append("AND p.year = ?")
            params.append(year)

        if min_year:
            query_parts.append("AND p.year >= ?")
            params.append(min_year)

        if max_year:
            query_parts.append("AND p.year <= ?")
            params.append(max_year)

        if min_citations is not None:
            query_parts.append("AND p.cited_by_count >= ?")
            params.append(min_citations)

        if search:
            query_parts.append("AND (p.title LIKE ? OR p.abstract LIKE ? OR p.authors LIKE ? OR p.journal LIKE ?)")
            search_pattern = f"%{search}%"
            params.extend([search_pattern, search_pattern, search_pattern, search_pattern])

        # Add ordering and pagination
        ord_clause = "COALESCE(p.cited_by_count, 0) DESC, COALESCE(p.publication_date, '') DESC, COALESCE(p.year, 0) DESC"
        if order:
            o = (order or "").lower().strip()
            requested_dir = (order_dir or "").lower().strip()
            desc_default = {"citations", "recent", "rating", "added_at"}
            dir_sql = "ASC" if requested_dir == "asc" else "DESC"
            if requested_dir not in {"asc", "desc"}:
                dir_sql = "DESC" if o in desc_default else "ASC"
            if o == "recent":
                ord_clause = (
                    "COALESCE(p.publication_date, printf('%04d-01-01', COALESCE(p.year, 0)), "
                    f"COALESCE(p.added_at, p.created_at, '')) {dir_sql}, COALESCE(p.cited_by_count, 0) DESC"
                )
            elif o == "title":
                ord_clause = f"p.title COLLATE NOCASE {dir_sql}"
            elif o == "rating":
                ord_clause = f"COALESCE(p.rating, 0) {dir_sql}, COALESCE(p.added_at, p.created_at, '') DESC"
            elif o == "authors":
                ord_clause = f"COALESCE(p.authors, '') COLLATE NOCASE {dir_sql}, p.title COLLATE NOCASE ASC"
            elif o == "journal":
                ord_clause = f"COALESCE(p.journal, '') COLLATE NOCASE {dir_sql}, p.title COLLATE NOCASE ASC"
            elif o == "status":
                ord_clause = f"p.status COLLATE NOCASE {dir_sql}, p.title COLLATE NOCASE ASC"
            elif o == "added_at":
                ord_clause = f"COALESCE(p.added_at, p.created_at, '') {dir_sql}, p.title COLLATE NOCASE ASC"
            else:
                ord_clause = f"COALESCE(p.cited_by_count, 0) {dir_sql}, COALESCE(p.publication_date, '') DESC, COALESCE(p.year, 0) DESC"
        query_parts.append(f"ORDER BY {ord_clause} LIMIT ? OFFSET ?")
        params.extend([limit, offset])

        # Execute query
        query = " ".join(query_parts)
        cursor = db.execute(query, params)
        papers = cursor.fetchall()

        result = []
        for paper in papers:
            result.append(row_to_paper_response(paper))

        logger.info(f"Retrieved {len(result)} papers (limit={limit}, offset={offset})")
        return result

    except Exception as e:
        raise_internal("Failed to query papers", e)


@router.post(
    "/semantic-search",
    summary="Run explicit SPECTER2 semantic paper search",
    description=(
        "Embed a short query with the SPECTER2 adhoc-query adapter and compare "
        "against cached S2/SPECTER2 paper vectors. Runs through Activity because "
        "query embedding is live AI inference."
    ),
)
def semantic_paper_search(
    body: SemanticPaperSearchRequest,
    db: sqlite3.Connection = Depends(get_db),
    user: dict = Depends(get_current_user),
) -> dict:
    """Queue an explicit SPECTER2 semantic search job."""
    from alma.api.scheduler import activity_envelope, find_active_job, schedule_immediate, set_job_status

    query = " ".join(str(body.query or "").strip().split())
    if not query:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="query is required")
    scope = str(body.scope or "library").strip().lower()
    if scope not in {"library", "all"}:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="scope must be 'library' or 'all'")
    limit = max(1, min(int(body.limit or 20), 100))

    key_hash = hashlib.sha1(f"{scope}|{limit}|{query.lower()}".encode("utf-8")).hexdigest()[:16]
    operation_key = f"papers.semantic_search.{key_hash}"
    existing = find_active_job(operation_key)
    if existing:
        return activity_envelope(
            str(existing.get("job_id") or ""),
            status="already_running",
            operation_key=operation_key,
            message="SPECTER2 semantic search already running",
        )

    job_id = f"papers_semantic_search_{uuid.uuid4().hex[:8]}"
    set_job_status(
        job_id,
        status="queued",
        operation_key=operation_key,
        trigger_source="user",
        message="SPECTER2 semantic search queued; query embedding may use CPU/GPU",
        started_at=datetime.utcnow().isoformat(),
        total=limit,
        processed=0,
    )
    schedule_immediate(job_id, _run_semantic_paper_search, job_id, query, scope, limit)
    return activity_envelope(
        job_id,
        status="queued",
        operation_key=operation_key,
        message="SPECTER2 semantic search queued",
    )


def _run_semantic_paper_search(job_id: str, query: str, scope: str, limit: int) -> None:
    """Background worker for explicit SPECTER2 semantic search."""
    from alma.ai.environment import activate_dependency_environment
    from alma.ai.semantic_search import (
        SPECTER2_ADHOC_QUERY_ADAPTER,
        specter2_semantic_search,
    )
    from alma.api.deps import open_db_connection
    from alma.api.scheduler import add_job_log, set_job_status
    from alma.discovery.semantic_scholar import S2_SPECTER2_MODEL

    conn = open_db_connection()
    try:
        set_job_status(
            job_id,
            status="running",
            processed=0,
            total=limit,
            message="Embedding query with SPECTER2 adhoc-query adapter",
        )
        dep_env = activate_dependency_environment(conn)
        add_job_log(
            job_id,
            "Dependency environment resolved",
            step="environment",
            data={
                "selected_python_executable": dep_env.selected_python_executable,
                "backend_python_executable": dep_env.as_dict().get("backend_python_executable"),
                "python_version_match": dep_env.as_dict().get("python_version_match"),
            },
        )

        vector_count = conn.execute(
            "SELECT COUNT(*) AS c FROM publication_embeddings WHERE model = ?",
            (S2_SPECTER2_MODEL,),
        ).fetchone()["c"]
        if int(vector_count or 0) <= 0:
            set_job_status(
                job_id,
                status="completed",
                processed=0,
                total=0,
                message="No cached S2/SPECTER2 paper vectors are available",
                result={
                    "query": query,
                    "scope": scope,
                    "count": 0,
                    "items": [],
                    "embedding_model": S2_SPECTER2_MODEL,
                    "query_model": SPECTER2_ADHOC_QUERY_ADAPTER,
                },
                finished_at=datetime.utcnow().isoformat(),
            )
            return

        rows = specter2_semantic_search(query, conn, scope=scope, limit=limit)
        items = []
        for row in rows:
            paper = row_to_paper_response(row).model_dump()
            items.append(
                {
                    "paper": paper,
                    "score": round(float(row.get("score") or 0.0), 4),
                    "match_type": row.get("match_type") or "semantic",
                    "embedding_model": row.get("embedding_model") or S2_SPECTER2_MODEL,
                    "query_model": row.get("query_model") or SPECTER2_ADHOC_QUERY_ADAPTER,
                }
            )
        add_job_log(
            job_id,
            "SPECTER2 semantic search complete",
            step="summary",
            data={
                "query": query,
                "scope": scope,
                "results": len(items),
                "searched_vectors": int(vector_count or 0),
                "embedding_model": S2_SPECTER2_MODEL,
                "query_model": SPECTER2_ADHOC_QUERY_ADAPTER,
            },
        )
        set_job_status(
            job_id,
            status="completed",
            processed=len(items),
            total=limit,
            message=f"SPECTER2 semantic search returned {len(items)} result(s)",
            result={
                "query": query,
                "scope": scope,
                "count": len(items),
                "items": items,
                "embedding_model": S2_SPECTER2_MODEL,
                "query_model": SPECTER2_ADHOC_QUERY_ADAPTER,
            },
            finished_at=datetime.utcnow().isoformat(),
        )
    except Exception as exc:
        message = str(exc)
        if "Local SPECTER2 requires" in message:
            message = (
                "SPECTER2 semantic search requires adapters, transformers, torch, and numpy "
                "inside the selected AI environment."
            )
        add_job_log(
            job_id,
            message,
            level="ERROR",
            step="semantic_search_error",
            data={"raw_error": str(exc)},
        )
        set_job_status(
            job_id,
            status="failed",
            error=str(exc),
            message=message,
            finished_at=datetime.utcnow().isoformat(),
        )
    finally:
        conn.close()


@router.get(
    "/enrichment-status",
    summary="Get paper metadata enrichment bookkeeping",
    description=(
        "Pure-read status for corpus metadata rehydration. Reports per-paper "
        "ledger counts and current OpenAlex metadata repair eligibility without "
        "performing any external API calls or writes."
    ),
)
def get_paper_enrichment_status(
    source: str = Query("openalex", description="Enrichment source; currently only openalex"),
    purpose: str = Query("metadata", description="Enrichment purpose; currently only metadata"),
    status_filter: Optional[str] = Query(None, alias="status", description="Optional ledger status filter"),
    paper_id: Optional[str] = Query(None, description="Optional paper id to inspect"),
    limit: int = Query(20, ge=0, le=500, description="Number of per-paper ledger rows to include"),
    offset: int = Query(0, ge=0, description="Per-paper ledger row offset"),
    db: sqlite3.Connection = Depends(get_db),
    user: dict = Depends(get_current_user),
) -> dict:
    source_value = (source or "openalex").strip().lower()
    purpose_value = (purpose or "metadata").strip().lower()
    if source_value != "openalex" or purpose_value != "metadata":
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Only source=openalex&purpose=metadata is currently supported",
        )
    from alma.services.corpus_rehydrate import (
        build_enrichment_status,
        list_enrichment_status_items,
    )

    payload = build_enrichment_status(db)
    payload["items"] = list_enrichment_status_items(
        db,
        status_filter=(status_filter or "").strip() or None,
        paper_id=(paper_id or "").strip() or None,
        limit=limit,
        offset=offset,
    )
    payload["items_limit"] = limit
    payload["items_offset"] = offset
    return payload


@router.post(
    "/rehydrate-metadata",
    summary="Rehydrate missing paper metadata from OpenAlex",
    description=(
        "Queues an Activity-backed corpus repair job. The job batches OpenAlex "
        "work IDs, records one ledger row per paper/source/purpose, skips "
        "already-covered lookup/projection pairs, and writes only improving "
        "paper metadata."
    ),
)
def rehydrate_paper_metadata(
    limit: int = Query(500, ge=1, le=100_000, description="Maximum papers to inspect in this run"),
    force: bool = Query(False, description="Ignore terminal ledger rows and refetch matching lookup/projection pairs"),
    db: sqlite3.Connection = Depends(get_db),
    user: dict = Depends(get_current_user),
) -> dict:
    from alma.api.scheduler import (
        activity_envelope,
        add_job_log,
        find_active_job,
        is_cancellation_requested,
        schedule_immediate,
        set_job_status,
    )
    from alma.services.corpus_rehydrate import run_corpus_metadata_rehydration

    operation_key = "papers.rehydrate_metadata:openalex:metadata"
    existing = find_active_job(operation_key)
    if existing:
        return activity_envelope(
            str(existing.get("job_id") or ""),
            status="already_running",
            operation_key=operation_key,
            message="OpenAlex metadata rehydration already running",
        )

    job_id = f"paper_metadata_rehydrate_{uuid.uuid4().hex[:10]}"
    set_job_status(
        job_id,
        status="queued",
        operation_key=operation_key,
        trigger_source="user",
        started_at=datetime.utcnow().isoformat(),
        processed=0,
        total=limit,
        message=f"OpenAlex metadata rehydration queued for up to {limit} paper(s)",
    )
    add_job_log(
        job_id,
        "OpenAlex metadata rehydration queued",
        step="queued",
        data={"limit": limit, "force": force},
    )

    def _runner() -> dict:
        return run_corpus_metadata_rehydration(
            job_id,
            limit=limit,
            force=force,
            set_job_status=set_job_status,
            add_job_log=add_job_log,
            is_cancellation_requested=is_cancellation_requested,
        )

    schedule_immediate(job_id, _runner)
    return activity_envelope(
        job_id,
        status="queued",
        operation_key=operation_key,
        message="OpenAlex metadata rehydration queued",
        total=limit,
    )


@router.get(
    "/stats",
    summary="Get paper statistics",
    description="Get aggregate statistics about papers in the database.",
)
def get_publication_stats(
    min_year: Optional[int] = Query(None, description="Minimum publication year to include"),
    max_year: Optional[int] = Query(None, description="Maximum publication year to include"),
    top_limit: int = Query(10, ge=1, le=100, description="Top authors/papers limit"),
    db: sqlite3.Connection = Depends(get_db),
    user: dict = Depends(get_current_user),
):
    """Get aggregate paper statistics.

    Args:
        min_year: Filter stats to papers from this year (inclusive)
        max_year: Filter stats to papers up to this year (inclusive)
        top_limit: How many entries to return for top lists

    Returns:
        dict with counts, per-year distribution, top-cited papers, and
        top authors by citations within the provided year window.
    """
    try:
        # Common WHERE clause parts (bound to papers alias `p`)
        where_parts = ["1=1"]
        params: list = []
        if min_year is not None:
            where_parts.append("p.year >= ?")
            params.append(min_year)
        if max_year is not None:
            where_parts.append("p.year <= ?")
            params.append(max_year)
        where = " AND ".join(where_parts)

        # Total papers/citations (single pass)
        totals = db.execute(
            f"""
            SELECT COUNT(*) AS count, COALESCE(SUM(p.cited_by_count), 0) AS total
            FROM papers p
            WHERE {where}
            """,
            params,
        ).fetchone()
        total_pubs = totals["count"] or 0
        total_citations = totals["total"] or 0

        total_authors = db.execute("SELECT COUNT(*) AS c FROM authors").fetchone()["c"] or 0

        # Papers by year (no limit by default; return ascending years for chart readability)
        cursor = db.execute(
            f"""
               SELECT year, COUNT(*) as count
               FROM papers p
               WHERE p.year IS NOT NULL AND {where}
               GROUP BY year
               ORDER BY year ASC
            """,
            params,
        )
        by_year = [dict(row) for row in cursor.fetchall()]

        # Top cited papers (within window)
        cursor = db.execute(
            f"""
               SELECT p.title AS title, COALESCE(p.cited_by_count,0) AS citations, p.year AS year
               FROM papers p
               WHERE {where}
               ORDER BY citations DESC
               LIMIT ?
            """,
            [*params, top_limit],
        )
        top_cited = [dict(row) for row in cursor.fetchall()]

        # Top authors by citations (within window).
        # publication_authors carries (paper_id, openalex_id) as a primary
        # key with display_name always populated, so we aggregate on
        # openalex_id and read the display name straight off the paper row.
        # We intentionally do NOT join to ``authors``: the partial unique
        # index on ``lower(openalex_id)`` can't be used for an equality join
        # on a function, so the planner scans authors on every group (~2s on
        # the real DB). The openalex_id is the stable handle the UI drills
        # into anyway.
        cursor = db.execute(
            f"""
               SELECT
                   pa.openalex_id AS author_id,
                   MAX(pa.display_name) AS name,
                   COALESCE(SUM(p.cited_by_count), 0) AS citations,
                   COUNT(*) AS publications
               FROM publication_authors pa
               JOIN papers p ON p.id = pa.paper_id
               WHERE {where} AND pa.openalex_id IS NOT NULL
                 AND TRIM(pa.openalex_id) <> ''
               GROUP BY pa.openalex_id
               ORDER BY citations DESC
               LIMIT ?
            """,
            [*params, top_limit],
        )
        top_authors = [
            {
                "author_id": row["author_id"],
                "name": row["name"],
                "citations": row["citations"] or 0,
                "publications": row["publications"] or 0,
            }
            for row in cursor.fetchall()
        ]

        # Top journals by publication count (and citations) within window
        # We ignore empty or NULL journal entries for this aggregation.
        cursor = db.execute(
            f"""
               SELECT p.journal AS journal, COUNT(*) AS publications, COALESCE(SUM(p.cited_by_count),0) AS citations
               FROM papers p
               WHERE {where} AND p.journal IS NOT NULL AND TRIM(p.journal) <> ''
               GROUP BY p.journal
               ORDER BY publications DESC, citations DESC
               LIMIT ?
            """,
            [*params, top_limit],
        )
        top_journals = [
            {"journal": row["journal"], "publications": row["publications"], "citations": row["citations"]}
            for row in cursor.fetchall()
        ]

        # Institutions by country (geo stats)
        countries = []
        try:
            # Only if institutions table exists
            chk = db.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='publication_institutions'").fetchone()
            if chk:
                cursor = db.execute(
                    f"""
                       SELECT TRIM(UPPER(pi.country_code)) AS country_code, COUNT(*) AS publications
                       FROM publication_institutions pi
                       JOIN papers p ON pi.paper_id = p.id
                       WHERE {where} AND pi.country_code IS NOT NULL AND TRIM(pi.country_code) <> ''
                       GROUP BY TRIM(UPPER(pi.country_code))
                       ORDER BY publications DESC
                       LIMIT ?
                    """,
                    [*params, top_limit],
                )
                countries = [ {"country_code": r["country_code"], "publications": r["publications"]} for r in cursor.fetchall() ]
        except Exception:
            countries = []

        # Shape response to a simpler schema expected by tests/consumers
        return {
            "total_publications": total_pubs,
            "total_citations": total_citations,
            "total_authors": total_authors,
            "by_year": by_year,
            "top_cited": top_cited,
            "top_authors": top_authors,
            "top_journals": top_journals,
        }

    except Exception as e:
        raise_internal("Failed to retrieve paper statistics", e)


@router.put(
    "/{paper_id}/rate",
    summary="Rate a paper",
    description="Set a star rating (0-5) for a saved Library paper.",
)
def rate_publication(
    paper_id: str,
    rating: int = Query(..., description="Star rating from 0 to 5"),
    pub_db: sqlite3.Connection = Depends(get_db),
    user: dict = Depends(get_current_user),
):
    target = pub_db.execute(
        "SELECT id, title, status FROM papers WHERE id = ?",
        (paper_id,),
    ).fetchone()
    if rating < 0 or rating > 5:
        raise HTTPException(status_code=400, detail="rating must be between 0 and 5")
    if target is None:
        raise HTTPException(status_code=404, detail="Paper not found")
    if str(target["status"] or "") != library_app.LIBRARY_STATUS:
        raise HTTPException(status_code=400, detail="Only saved Library papers can be rated")

    library_app.rate_paper(pub_db, target["id"], int(rating))
    library_app.record_paper_feedback(
        pub_db,
        target["id"],
        action="rate",
        rating=int(rating),
        source_surface="papers",
    )
    pub_db.commit()
    return {
        "success": True,
        "paper_id": target["id"],
        "title": target["title"],
        "rating": int(rating),
    }


@router.get(
    "/{paper_id}/details",
    summary="Get full paper details",
    description="Return a single paper row plus the semantic topics attached to it.",
)
def get_paper_details(
    paper_id: str,
    db: sqlite3.Connection = Depends(get_db),
    user: dict = Depends(get_current_user),
):
    """Full paper details for the PaperDetailPanel popup.

    Returns the standard PaperResponse fields plus a `topics` list of
    ``{term, score, domain, field, subfield, topic_id}`` from
    ``publication_topics`` so the popup can show the semantic labels that
    OpenAlex attached to the work.
    """
    row = db.execute("SELECT * FROM papers WHERE id = ?", (paper_id,)).fetchone()
    if row is None:
        raise HTTPException(status_code=404, detail="Paper not found")
    paper = row_to_paper_response(row).model_dump()

    topic_rows = db.execute(
        """
        SELECT term, score, domain, field, subfield, topic_id
        FROM publication_topics
        WHERE paper_id = ?
        ORDER BY COALESCE(score, 0) DESC, term ASC
        """,
        (paper_id,),
    ).fetchall()
    paper["topics"] = [dict(r) for r in topic_rows]
    return paper


_NETWORK_CACHE_TTL_HOURS = 24


def _network_cache_read(
    db: sqlite3.Connection, paper_id: str, direction: str
) -> Optional[dict]:
    """Return the fresh cached T6b payload, or None when missing/stale."""
    try:
        row = db.execute(
            "SELECT payload_json, expires_at FROM paper_network_cache "
            "WHERE paper_id = ? AND direction = ?",
            (paper_id, direction),
        ).fetchone()
    except sqlite3.OperationalError:
        return None
    if not row:
        return None
    try:
        expires = datetime.fromisoformat(str(row["expires_at"] or ""))
    except ValueError:
        return None
    if expires < datetime.utcnow():
        return None
    try:
        return json.loads(row["payload_json"] or "null")
    except (json.JSONDecodeError, TypeError):
        return None


def _network_cache_write(
    db: sqlite3.Connection, paper_id: str, direction: str, payload: dict
) -> None:
    try:
        now = datetime.utcnow()
        db.execute(
            """
            INSERT OR REPLACE INTO paper_network_cache
                (paper_id, direction, fetched_at, expires_at, payload_json)
            VALUES (?, ?, ?, ?, ?)
            """,
            (
                paper_id,
                direction,
                now.isoformat(),
                (now + timedelta(hours=_NETWORK_CACHE_TTL_HOURS)).isoformat(),
                json.dumps(payload),
            ),
        )
        db.commit()
    except sqlite3.OperationalError as exc:
        logger.debug("paper_network_cache write failed: %s", exc)


def _decode_openalex_abstract(inv_index: dict | None) -> str | None:
    """Reconstruct an abstract from an OpenAlex inverted index.

    OpenAlex returns abstracts as ``{token: [position, ...]}`` rather
    than plain text. This walks the index back into a string. Returns
    ``None`` for missing or malformed input.
    """
    if not isinstance(inv_index, dict) or not inv_index:
        return None
    positions: list[tuple[int, str]] = []
    for token, idx_list in inv_index.items():
        if not isinstance(idx_list, list):
            continue
        for idx in idx_list:
            try:
                positions.append((int(idx), str(token)))
            except (TypeError, ValueError):
                continue
    if not positions:
        return None
    positions.sort(key=lambda kv: kv[0])
    return " ".join(token for _idx, token in positions)


def _openalex_work_to_related_work(
    work: dict,
    *,
    local_index: dict[str, sqlite3.Row],
) -> dict:
    """Shape a raw OpenAlex work dict into the ``RelatedWork`` envelope.

    Looks up ``local_index`` (keyed by bare W-id) to fill in ``paper_id``,
    ``status`` and ``rating`` for in-library matches. Non-matches keep
    ``paper_id=None`` so the frontend's "Pivot" button gracefully
    degrades to a no-op for papers we don't yet hold.
    """
    raw_id = (work.get("id") or "").rstrip("/").split("/")[-1]
    bare_w = raw_id if raw_id.startswith("W") else None
    title = (work.get("display_name") or "").strip() or "Untitled"
    year = work.get("publication_year")
    primary_location = work.get("primary_location") or {}
    src_obj = primary_location.get("source") if isinstance(primary_location, dict) else {}
    journal = (src_obj or {}).get("display_name") if isinstance(src_obj, dict) else None
    url = (
        primary_location.get("landing_page_url")
        if isinstance(primary_location, dict)
        else None
    ) or (work.get("doi") and f"https://doi.org/{(work['doi'] or '').replace('https://doi.org/', '')}") or work.get("id")
    doi_raw = (work.get("doi") or "").replace("https://doi.org/", "").strip() or None

    authorships = work.get("authorships") or []
    authors_str = ", ".join(
        (a.get("author") or {}).get("display_name", "")
        for a in authorships
        if isinstance(a, dict)
    ) or None

    abstract = _decode_openalex_abstract(work.get("abstract_inverted_index"))
    cited_by = int(work.get("cited_by_count") or 0)

    local: sqlite3.Row | None = local_index.get(bare_w) if bare_w else None
    if local is None and doi_raw:
        local = local_index.get(f"doi:{doi_raw.lower()}")
    in_library = local is not None
    return {
        "paper_id": str(local["id"]) if local is not None else None,
        "title": title,
        "authors": authors_str,
        "year": year,
        "doi": doi_raw,
        "url": url,
        "journal": journal,
        "abstract": abstract,
        "tldr": None,
        "cited_by_count": cited_by,
        "influential_citation_count": 0,
        "openalex_id": bare_w,
        "semantic_scholar_id": None,
        "status": (local["status"] if (local is not None and "status" in local.keys()) else None),
        "rating": (local["rating"] if (local is not None and "rating" in local.keys()) else None),
        "is_influential": False,
        "in_library": in_library,
        "source": "openalex",
    }


def _build_local_index(
    db: sqlite3.Connection,
    *,
    openalex_ids: list[str],
    dois: list[str],
) -> dict[str, sqlite3.Row]:
    """Bulk-look-up local papers for an OpenAlex result set.

    Indexes by bare W-id and (lowercased) DOI so
    ``_openalex_work_to_related_work`` can flag in-library hits in O(1).
    """
    out: dict[str, sqlite3.Row] = {}
    if openalex_ids:
        placeholders = ",".join("?" for _ in openalex_ids)
        try:
            rows = db.execute(
                f"SELECT * FROM papers WHERE openalex_id IN ({placeholders}) "
                "AND COALESCE(status, '') != 'removed'",
                openalex_ids,
            ).fetchall()
            for row in rows:
                oa = str(row["openalex_id"] or "").strip()
                if oa:
                    out[oa] = row
        except sqlite3.OperationalError as exc:
            logger.debug("local_index W-id lookup failed: %s", exc)
    if dois:
        normalized = [d.lower() for d in dois if d]
        placeholders = ",".join("?" for _ in normalized)
        try:
            rows = db.execute(
                f"SELECT * FROM papers WHERE LOWER(doi) IN ({placeholders}) "
                "AND COALESCE(status, '') != 'removed'",
                normalized,
            ).fetchall()
            for row in rows:
                d = str(row["doi"] or "").strip().lower()
                if d:
                    out.setdefault(f"doi:{d}", row)
        except sqlite3.OperationalError as exc:
            logger.debug("local_index DOI lookup failed: %s", exc)
    return out


def _anchor_openalex_id(anchor: sqlite3.Row) -> str | None:
    """Return the bare ``Wxxx`` OpenAlex id of an anchor row, or None."""
    raw = str(anchor["openalex_id"] or "").strip() if "openalex_id" in anchor.keys() else ""
    if not raw:
        return None
    bare = raw.rstrip("/").split("/")[-1]
    return bare if bare and bare[0] in ("W", "w") else None


@router.get(
    "/{paper_id}/prior-works",
    summary="Papers referenced by this paper (full OpenAlex graph)",
)
def list_prior_works(
    paper_id: str,
    limit: int = 30,
    db: sqlite3.Connection = Depends(get_db),
    user: dict = Depends(get_current_user),
):
    """Return every paper this paper references, fetched from OpenAlex.

    The OpenAlex graph is the source of truth — local
    ``publication_references`` is no longer queried for this view, so
    the user sees the full bibliography rather than a corpus-only
    subset. Local matches are looked up in bulk and surfaced via
    ``in_library=true`` plus an attached ``paper_id`` so the Pivot
    button still works for in-library hits.

    Cached for 24 h in ``paper_network_cache`` so repeat opens are
    instant; cache key is ``paper_id + 'prior'``.
    """
    bounded = max(1, min(int(limit or 30), 100))
    anchor = db.execute(
        "SELECT id, openalex_id, doi FROM papers WHERE id = ?",
        (paper_id,),
    ).fetchone()
    if anchor is None:
        raise HTTPException(status_code=404, detail="Paper not found")

    seed_oa = _anchor_openalex_id(anchor)
    if not seed_oa:
        # No OpenAlex id on the anchor — there's no remote graph to
        # fetch. Return an empty envelope rather than guessing.
        return {
            "direction": "prior",
            "source_paper_id": paper_id,
            "works": [],
            "remote_count": 0,
            "in_library_count": 0,
        }

    cached = _network_cache_read(db, paper_id, "prior")
    if cached is not None:
        oa_works = cached.get("oa_works") or []
    else:
        from alma.openalex.client import fetch_referenced_works_for_openalex_id

        oa_works = fetch_referenced_works_for_openalex_id(seed_oa, limit=bounded)
        _network_cache_write(db, paper_id, "prior", {"oa_works": oa_works})

    oa_ids = [
        (w.get("id") or "").rstrip("/").split("/")[-1]
        for w in oa_works
        if w.get("id")
    ]
    oa_ids = [oid for oid in oa_ids if oid.startswith("W")]
    dois = [
        (w.get("doi") or "").replace("https://doi.org/", "").strip()
        for w in oa_works
        if w.get("doi")
    ]
    local_index = _build_local_index(db, openalex_ids=oa_ids, dois=dois)
    works = [
        _openalex_work_to_related_work(w, local_index=local_index)
        for w in oa_works
    ]
    in_library_count = sum(1 for w in works if w.get("in_library"))
    return {
        "direction": "prior",
        "source_paper_id": paper_id,
        "works": works[:bounded],
        "remote_count": len(oa_works),
        "in_library_count": in_library_count,
    }


@router.get(
    "/{paper_id}/derivative-works",
    summary="Papers that cite this paper (full OpenAlex graph)",
)
def list_derivative_works(
    paper_id: str,
    limit: int = 30,
    db: sqlite3.Connection = Depends(get_db),
    user: dict = Depends(get_current_user),
):
    """Return every paper that cites this paper, fetched from OpenAlex.

    Uses OpenAlex's ``filter=cites:Wxxx`` query, sorted by
    ``cited_by_count desc`` so high-impact citing papers surface
    first. Local-corpus matches are flagged via ``in_library=true``;
    the user sees the full forward-citation graph rather than only
    locally-known citers.

    Cached for 24 h in ``paper_network_cache``.
    """
    bounded = max(1, min(int(limit or 30), 100))
    anchor = db.execute(
        "SELECT id, openalex_id, doi FROM papers WHERE id = ?",
        (paper_id,),
    ).fetchone()
    if anchor is None:
        raise HTTPException(status_code=404, detail="Paper not found")

    seed_oa = _anchor_openalex_id(anchor)
    if not seed_oa:
        return {
            "direction": "derivative",
            "source_paper_id": paper_id,
            "works": [],
            "remote_count": 0,
            "in_library_count": 0,
        }

    cached = _network_cache_read(db, paper_id, "derivative")
    if cached is not None:
        oa_works = cached.get("oa_works") or []
    else:
        from alma.openalex.client import fetch_citing_works_for_openalex_id

        oa_works = fetch_citing_works_for_openalex_id(seed_oa, limit=bounded)
        _network_cache_write(db, paper_id, "derivative", {"oa_works": oa_works})

    oa_ids = [
        (w.get("id") or "").rstrip("/").split("/")[-1]
        for w in oa_works
        if w.get("id")
    ]
    oa_ids = [oid for oid in oa_ids if oid.startswith("W")]
    dois = [
        (w.get("doi") or "").replace("https://doi.org/", "").strip()
        for w in oa_works
        if w.get("doi")
    ]
    local_index = _build_local_index(db, openalex_ids=oa_ids, dois=dois)
    works = [
        _openalex_work_to_related_work(w, local_index=local_index)
        for w in oa_works
    ]
    in_library_count = sum(1 for w in works if w.get("in_library"))
    return {
        "direction": "derivative",
        "source_paper_id": paper_id,
        "works": works[:bounded],
        "remote_count": len(oa_works),
        "in_library_count": in_library_count,
    }


@router.delete(
    "/{paper_id}",
    status_code=status.HTTP_204_NO_CONTENT,
    summary="Delete a paper",
    description="Remove a specific paper from the database.",
)
def delete_publication(
    paper_id: str,
    db: sqlite3.Connection = Depends(get_db),
    user: dict = Depends(get_current_user),
):
    """Delete a specific paper from the database.

    Args:
        paper_id: UUID of the paper

    Raises:
        HTTPException: If paper is not found

    Example:
        ```bash
        curl -X DELETE "http://localhost:8000/api/v1/papers/550e8400-e29b-41d4-a716-446655440000"
        ```
    """
    try:
        # Check if paper exists
        cursor = db.execute(
            "SELECT id, title FROM papers WHERE id = ?",
            (paper_id,)
        )
        paper = cursor.fetchone()

        if not paper:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Paper not found"
            )

        library_app.soft_remove_from_library(db, paper_id)
        db.commit()

        logger.info(f"Soft-removed paper: {paper['title']} (ID: {paper_id})")

    except HTTPException:
        raise
    except Exception as e:
        raise_internal("Failed to delete paper", e)


# -- Preprint ↔ journal dedup -------------------------------------------------


@router.post(
    "/dedup-preprints",
    summary="Detect + merge preprint↔journal twin papers",
    description=(
        "Scans for pairs where the same work exists as both a preprint "
        "(arXiv / bioRxiv / psyRxiv / chemRxiv / OSF / MDPI) and a "
        "published journal row, collapsing each pair into the journal "
        "version. `scope=library` only collapses pairs where at least one "
        "side is a saved Library paper (fast); `scope=corpus` considers "
        "every pair (can take a while on a large corpus). FK rows migrate "
        "preprint → canonical; Library + Discovery lists filter the "
        "merged preprint out automatically."
    ),
)
def dedup_preprint_twins(
    scope: str = Query("corpus", description="library | corpus"),
    limit: Optional[int] = Query(None, description="Cap the number of pairs to merge"),
    background: bool = Query(True, description="Queue via Activity envelope"),
    db: sqlite3.Connection = Depends(get_db),
    user: dict = Depends(get_current_user),
):
    """Activity-envelope runner for preprint↔journal dedup."""
    from alma.api.deps import _db_path, open_db_connection
    from alma.api.scheduler import (
        activity_envelope,
        add_job_log,
        find_active_job,
        schedule_immediate,
        set_job_status,
    )
    from alma.application.preprint_dedup import run_preprint_dedup

    scope_value = (scope or "corpus").strip().lower()
    if scope_value not in {"library", "corpus"}:
        scope_value = "corpus"

    if not background:
        return run_preprint_dedup(_db_path(), limit=limit, scope=scope_value)

    # One job per (operation, scope) so a Library-scoped run doesn't
    # dedup-block a full-corpus sweep (or vice versa).
    operation_key = f"papers.dedup_preprints:{scope_value}"
    existing = find_active_job(operation_key)
    if existing:
        return activity_envelope(
            str(existing.get("job_id") or ""),
            status="already_running",
            operation_key=operation_key,
            message=f"Preprint dedup already running (scope={scope_value})",
        )

    job_id = f"preprint_dedup_{uuid.uuid4().hex[:10]}"
    set_job_status(
        job_id,
        status="queued",
        operation_key=operation_key,
        trigger_source="user",
        started_at=datetime.utcnow().isoformat(),
        message=f"Scanning for preprint↔journal twins (scope={scope_value})",
    )
    add_job_log(job_id, f"Preprint dedup queued (scope={scope_value})", step="queued")

    def _runner() -> dict:
        class _Ctx:
            def log_step(self, step, *, message=None, processed=None, total=None, **_):
                try:
                    set_job_status(
                        job_id,
                        status="running",
                        message=message,
                        processed=processed,
                        total=total,
                    )
                except Exception:
                    pass

        try:
            summary = run_preprint_dedup(
                _db_path(),
                ctx=_Ctx(),
                limit=limit,
                scope=scope_value,
            )
            add_job_log(
                job_id,
                f"Dedup complete: merged={summary['merged']} skipped={summary['skipped']} errors={summary['errors']}",
                step="done",
                data=summary,
            )
            return summary
        except Exception as exc:
            add_job_log(job_id, f"Dedup failed: {exc}", level="ERROR", step="failed")
            raise

    schedule_immediate(job_id, _runner)
    return activity_envelope(
        job_id,
        status="queued",
        operation_key=operation_key,
        message=f"Preprint dedup queued (scope={scope_value})",
    )
