"""Library API endpoints: likes, collections, tags, followed authors."""

import logging
import sqlite3
import uuid
from datetime import datetime
from typing import List, Optional

from fastapi import APIRouter, Depends, HTTPException, Query, status
from pydantic import BaseModel, Field

from alma.api.deps import get_current_user, get_db
from alma.api.helpers import (
    normalize_topic_term,
    raise_internal,
    row_to_paper_response,
    safe_div,
    table_exists,
)
from alma.api.models import (
    PaperResponse,
    CollectionCreate,
    CollectionResponse,
    TagCreate,
    TagResponse,
    FollowAuthorRequest,
    FollowedAuthorResponse,
)
from alma.application.followed_authors import (
    apply_follow_state,
    ensure_followed_author_contract,
    resolve_canonical_author_id,
    schedule_followed_author_historical_backfill,
)
from alma.application import library as library_app


logger = logging.getLogger(__name__)
MAX_TAGS_PER_PAPER = 5

router = APIRouter(
    dependencies=[Depends(get_current_user)],
    responses={401: {"description": "Unauthorized"}},
)


def _ensure_library_signal_scores(db: sqlite3.Connection) -> None:
    """Populate `papers.global_signal_score` for Library rows that are
    missing it.

    The column is the paper_signal composite used as the Library "Ranking"
    column — a 0..1 blend of rating, topic alignment, embedding
    similarity, author alignment, accumulated feedback interactions,
    and recency
    (weights in `DISCOVERY_SETTINGS_DEFAULTS`). It stays 0 until somebody
    computes it; this helper is the cheapest way to keep the sort
    meaningful without paying compute cost on every `list_saved` call.

    Called only when the caller asks for `order=signal`. Scope: every
    Library row with `global_signal_score = 0` OR missing. One batch
    computation + one UPDATE per row. Errors are swallowed so the
    route still returns rows.
    """
    try:
        pending_rows = db.execute(
            "SELECT id FROM papers "
            "WHERE status = 'library' "
            "AND COALESCE(global_signal_score, 0) = 0"
        ).fetchall()
    except sqlite3.OperationalError as exc:
        logger.debug("signal backfill skipped (schema): %s", exc)
        return
    pending = [str(r["id"]) for r in pending_rows]
    if not pending:
        return
    try:
        from alma.application import paper_signal as _paper_signal

        state = _paper_signal.load_library_state(db)
        scores = _paper_signal.score_papers_batch(db, pending, state)
    except Exception as exc:
        logger.debug("signal backfill compute failed: %s", exc)
        return
    if not scores:
        return
    try:
        db.executemany(
            "UPDATE papers SET global_signal_score = ? WHERE id = ?",
            [(float(scores.get(pid, 0.0)), pid) for pid in pending],
        )
        db.commit()
    except sqlite3.OperationalError as exc:
        logger.debug("signal backfill write failed: %s", exc)


def _paper_tag_count(db: sqlite3.Connection, paper_id: str) -> int:
    if not table_exists(db, "publication_tags"):
        return 0
    row = db.execute(
        "SELECT COUNT(DISTINCT tag_id) AS c FROM publication_tags WHERE paper_id = ?",
        (paper_id,),
    ).fetchone()
    return int((row["c"] if row else 0) or 0)


def _require_library_paper(db: sqlite3.Connection, paper_id: str) -> None:
    """Raise unless the paper is in the saved Library."""
    paper = db.execute(
        "SELECT id, status FROM papers WHERE id = ?",
        (paper_id,),
    ).fetchone()
    if not paper:
        raise HTTPException(status_code=404, detail="Paper not found")
    if str(paper["status"] or "") != library_app.LIBRARY_STATUS:
        raise HTTPException(
            status_code=400,
            detail="Only saved Library papers can use Library organization actions",
        )


def _ensure_topic_alias_table(db: sqlite3.Connection) -> None:
    """Ensure topic tables exist with the new canonical schema."""
    try:
        from alma.library.topic_deduplication import _ensure_topic_tables
        _ensure_topic_tables(db)
    except Exception:
        # Fallback: create minimal tables
        db.execute(
            """CREATE TABLE IF NOT EXISTS topics (
                topic_id TEXT PRIMARY KEY,
                canonical_name TEXT NOT NULL,
                normalized_name TEXT NOT NULL UNIQUE,
                source TEXT DEFAULT 'auto',
                created_at TEXT DEFAULT (datetime('now'))
            )"""
        )
        db.execute(
            """CREATE TABLE IF NOT EXISTS topic_aliases (
                alias_id INTEGER PRIMARY KEY AUTOINCREMENT,
                topic_id TEXT NOT NULL REFERENCES topics(topic_id),
                raw_term TEXT NOT NULL,
                normalized_term TEXT NOT NULL,
                source TEXT DEFAULT 'auto',
                confidence REAL DEFAULT 1.0,
                created_at TEXT DEFAULT (datetime('now')),
                UNIQUE(normalized_term)
            )"""
        )


class TopicSummary(BaseModel):
    canonical: str
    paper_count: int = 0
    aliases: list[str] = Field(default_factory=list)


class TopicCreateRequest(BaseModel):
    name: str = Field(..., min_length=1, description="Canonical topic name")


class TopicAliasCreateRequest(BaseModel):
    alias: str = Field(..., min_length=1)
    canonical: str = Field(..., min_length=1)


class TopicRenameRequest(BaseModel):
    new_name: str = Field(..., min_length=1)


class TopicGroupRequest(BaseModel):
    source: str = Field(..., min_length=1)
    target: str = Field(..., min_length=1)


# ===================================================================
# Library Papers (saved)
# ===================================================================

@router.get(
    "/saved",
    response_model=List[PaperResponse],
    summary="List library papers",
)
def list_saved(
    search: Optional[str] = Query(None, description="Search in title or notes"),
    order: str = Query(
        "date",
        description=(
            "Sort order: date | rating | signal | title | authors | journal | "
            "citations | added_at. 'signal' = the paper_signal composite "
            "ranking (not the user's star rating)."
        ),
    ),
    order_dir: str = Query("desc", description="Sort direction: asc | desc"),
    limit: int = Query(100, ge=1, le=1000),
    offset: int = Query(0, ge=0),
    db: sqlite3.Connection = Depends(get_db),
):
    """Return all library papers with optional search and pagination."""
    try:
        order_value = (order or "date").strip().lower()
        direction_value = (order_dir or "desc").strip().lower()
        direction = "ASC" if direction_value == "asc" else "DESC"
        # When the user asks for signal-ranked order, make sure every
        # Library row has a populated `global_signal_score` — otherwise
        # never-scored papers sink to the bottom regardless of how
        # signal-rich they actually are. Backfill is bounded + cheap
        # (one `score_papers_batch` call over the library set).
        if order_value == "signal":
            _ensure_library_signal_scores(db)
        order_clause = {
            "date": f"COALESCE(publication_date, printf('%04d-01-01', COALESCE(year, 0)), '') {direction}, title COLLATE NOCASE ASC",
            "rating": f"COALESCE(rating, 0) {direction}, COALESCE(added_at, created_at, publication_date, '') DESC",
            "signal": f"COALESCE(global_signal_score, 0) {direction}, COALESCE(rating, 0) DESC, title COLLATE NOCASE ASC",
            "title": f"title COLLATE NOCASE {direction}",
            "authors": f"COALESCE(authors, '') COLLATE NOCASE {direction}, title COLLATE NOCASE ASC",
            "journal": f"COALESCE(journal, '') COLLATE NOCASE {direction}, title COLLATE NOCASE ASC",
            "citations": f"COALESCE(cited_by_count, 0) {direction}, COALESCE(publication_date, printf('%04d-01-01', COALESCE(year, 0)), '') DESC",
            "added_at": f"COALESCE(added_at, created_at, publication_date, '') {direction}, title COLLATE NOCASE ASC",
        }.get(order_value, f"COALESCE(publication_date, printf('%04d-01-01', COALESCE(year, 0)), '') {direction}, title COLLATE NOCASE ASC")

        # `canonical_paper_id IS NULL` excludes preprint rows that were
        # merged into a published journal twin — they stay in `papers`
        # for FK integrity but shouldn't appear as separate Library cards.
        # See `alma.application.preprint_dedup`.
        if search:
            cursor = db.execute(
                f"""SELECT * FROM papers
                    WHERE status = 'library'
                      AND COALESCE(canonical_paper_id, '') = ''
                      AND (
                        title LIKE ?
                        OR COALESCE(notes, '') LIKE ?
                        OR COALESCE(authors, '') LIKE ?
                        OR COALESCE(journal, '') LIKE ?
                      )
                    ORDER BY {order_clause}
                    LIMIT ? OFFSET ?""",
                (f"%{search}%", f"%{search}%", f"%{search}%", f"%{search}%", limit, offset),
            )
        else:
            cursor = db.execute(
                f"""SELECT * FROM papers
                    WHERE status = 'library'
                      AND COALESCE(canonical_paper_id, '') = ''
                    ORDER BY {order_clause}
                    LIMIT ? OFFSET ?""",
                (limit, offset),
            )
        rows = cursor.fetchall()
        return [row_to_paper_response(r) for r in rows]
    except Exception as e:
        raise_internal("Failed to list library papers", e)


class _SavePaperRequest(BaseModel):
    paper_id: Optional[str] = None
    title: Optional[str] = Field(None, min_length=1)
    authors: Optional[str] = None
    year: Optional[int] = None
    journal: Optional[str] = None
    abstract: Optional[str] = None
    url: Optional[str] = None
    doi: Optional[str] = None
    openalex_id: Optional[str] = None
    notes: Optional[str] = None
    rating: Optional[int] = Field(None, ge=0, le=5)
    added_from: Optional[str] = Field(
        None,
        description="Optional provenance override for manual/library-side saves",
    )


@router.post(
    "/saved",
    response_model=PaperResponse,
    status_code=status.HTTP_201_CREATED,
    summary="Add paper to library",
)
def save_publication(
    req: _SavePaperRequest,
    db: sqlite3.Connection = Depends(get_db),
):
    """Add a paper to the library."""
    try:
        paper_id = str(req.paper_id or "").strip() or None
        added_from = str(req.added_from or "").strip() or "library_manual"

        if not paper_id:
            title = str(req.title or "").strip()
            if not title:
                raise HTTPException(status_code=400, detail="paper_id or title is required")
            paper_id = library_app.upsert_paper(
                db,
                title=title,
                authors=req.authors,
                year=req.year,
                journal=req.journal,
                abstract=req.abstract,
                url=req.url,
                doi=req.doi,
                openalex_id=req.openalex_id,
                status=library_app.TRACKED_STATUS,
            )

        cursor_ok = library_app.add_to_library(
            db,
            paper_id,
            rating=req.rating if req.rating is not None else library_app.DEFAULT_LIBRARY_RATING,
            notes=req.notes,
            added_from=added_from,
        )
        if cursor_ok:
            library_app.sync_surface_resolution(
                db,
                paper_id,
                action="save",
                source_surface="library",
            )
        db.commit()
        if not cursor_ok:
            raise HTTPException(status_code=404, detail="Paper not found")
        row = db.execute("SELECT * FROM papers WHERE id = ?", (paper_id,)).fetchone()
        return row_to_paper_response(row)
    except HTTPException:
        raise
    except Exception as e:
        raise_internal("Failed to add paper to library", e)


@router.delete(
    "/saved/{paper_id}",
    status_code=status.HTTP_204_NO_CONTENT,
    summary="Remove paper from library",
)
def unsave_publication(
    paper_id: str,
    db: sqlite3.Connection = Depends(get_db),
):
    """Remove a paper from library."""
    row = db.execute(
        "SELECT id FROM papers WHERE id = ? AND status = 'library'",
        (paper_id,),
    ).fetchone()
    if not row:
        raise HTTPException(status_code=404, detail="Paper not found in library")
    library_app.soft_remove_from_library(db, paper_id)
    db.commit()


@router.put(
    "/saved/{paper_id}",
    response_model=PaperResponse,
    summary="Update notes/rating on a library paper",
)
def update_saved_paper(
    paper_id: str,
    notes: Optional[str] = None,
    rating: Optional[int] = None,
    title: Optional[str] = None,
    authors: Optional[str] = None,
    abstract: Optional[str] = None,
    db: sqlite3.Connection = Depends(get_db),
):
    """Update editable fields on a library paper.

    Accepts any subset of: notes, rating, title, authors, abstract.
    Soft-edit pathway used by the PaperDetailPanel `...` menu so
    the user can repair imports whose title / author list / abstract
    came in malformed (typical for legacy BibTeX rows). Rating still
    writes a feedback event; the metadata fields are silent edits.
    """
    row = db.execute(
        "SELECT * FROM papers WHERE id = ? AND status = 'library'",
        (paper_id,),
    ).fetchone()
    if not row:
        raise HTTPException(status_code=404, detail="Paper not found in library")

    # Build the SET clause dynamically — touch only the columns the
    # caller actually sent so partial edits don't clobber the rest.
    updates: list[str] = []
    params: list[object] = []
    if notes is not None:
        updates.append("notes = ?")
        params.append(notes)
    if rating is not None:
        updates.append("rating = ?")
        params.append(int(rating))
    if title is not None:
        updates.append("title = ?")
        params.append(title.strip())
    if authors is not None:
        updates.append("authors = ?")
        params.append(authors.strip())
    if abstract is not None:
        updates.append("abstract = ?")
        params.append(abstract.strip())
    if not updates:
        # Nothing to change — return the current row so the caller
        # still gets the canonical shape.
        return row_to_paper_response(row)
    updates.append("updated_at = ?")
    params.append(datetime.utcnow().isoformat())
    params.append(paper_id)
    db.execute(
        f"UPDATE papers SET {', '.join(updates)} WHERE id = ?",
        params,
    )
    if rating is not None:
        library_app.record_paper_feedback(
            db,
            paper_id,
            action="rate",
            rating=int(rating),
            source_surface="library",
        )
    db.commit()
    updated = db.execute(
        "SELECT * FROM papers WHERE id = ?",
        (paper_id,),
    ).fetchone()
    return row_to_paper_response(updated)


# ===================================================================
# Bulk operations
# ===================================================================


class _BulkActionRequest(BaseModel):
    paper_ids: List[str] = Field(..., min_length=1, max_length=500)


class _BulkAddToCollectionRequest(BaseModel):
    paper_ids: List[str] = Field(..., min_length=1, max_length=500)
    collection_id: str


class _BulkActionResponse(BaseModel):
    affected: int


@router.post(
    "/bulk/clear-rating",
    response_model=_BulkActionResponse,
    summary="Bulk clear rating on library papers (set rating to 0)",
)
def bulk_clear_rating(
    req: _BulkActionRequest,
    db: sqlite3.Connection = Depends(get_db),
):
    """Set rating to 0 for multiple library papers (paper stays saved)."""
    placeholders = ",".join("?" for _ in req.paper_ids)
    cursor = db.execute(
        f"UPDATE papers SET rating = 0 WHERE id IN ({placeholders}) AND status = 'library'",
        req.paper_ids,
    )
    db.commit()
    return _BulkActionResponse(affected=cursor.rowcount or 0)


@router.post(
    "/bulk/remove",
    response_model=_BulkActionResponse,
    summary="Bulk remove papers from library",
)
def bulk_remove(
    req: _BulkActionRequest,
    db: sqlite3.Connection = Depends(get_db),
):
    """Remove multiple papers from library with soft-remove semantics."""
    placeholders = ",".join("?" for _ in req.paper_ids)
    rows = db.execute(
        f"SELECT id FROM papers WHERE id IN ({placeholders}) AND status = 'library'",
        req.paper_ids,
    ).fetchall()
    for row in rows:
        library_app.soft_remove_from_library(db, str(row["id"]))
    db.commit()
    return _BulkActionResponse(affected=len(rows))


@router.post(
    "/bulk/add-to-collection",
    response_model=_BulkActionResponse,
    summary="Bulk add papers to a collection",
)
def bulk_add_to_collection(
    req: _BulkAddToCollectionRequest,
    db: sqlite3.Connection = Depends(get_db),
):
    """Add multiple saved Library papers to a collection at once."""
    collection = db.execute(
        "SELECT id FROM collections WHERE id = ?",
        (req.collection_id,),
    ).fetchone()
    if not collection:
        raise HTTPException(status_code=404, detail="Collection not found")

    placeholders = ",".join("?" for _ in req.paper_ids)
    library_rows = db.execute(
        f"SELECT id FROM papers WHERE id IN ({placeholders}) AND status = 'library'",
        req.paper_ids,
    ).fetchall()
    now = datetime.utcnow().isoformat()
    added = 0
    for row in library_rows:
        pid = str(row["id"])
        try:
            db.execute(
                "INSERT OR IGNORE INTO collection_items (id, collection_id, paper_id, added_at) VALUES (?, ?, ?, ?)",
                (uuid.uuid4().hex, req.collection_id, pid, now),
            )
            added += int(db.execute("SELECT changes()").fetchone()[0] or 0)
        except sqlite3.IntegrityError:
            pass
    db.commit()
    return _BulkActionResponse(affected=added)


# ===================================================================
# Collections
# ===================================================================

@router.get(
    "/collections",
    response_model=List[CollectionResponse],
    summary="List collections",
)
def list_collections(
    db: sqlite3.Connection = Depends(get_db),
):
    """Return all collections with item counts and health metrics."""
    try:
        cursor = db.execute(
            """SELECT c.*,
                      COALESCE(cnt.n, 0) AS item_count,
                      health.last_added_at,
                      health.avg_citations,
                      health.avg_rating,
                      health.activity_status
               FROM collections c
               LEFT JOIN (
                   SELECT ci.collection_id, COUNT(*) AS n
                   FROM collection_items ci
                   JOIN papers p ON p.id = ci.paper_id
                   WHERE p.status = 'library'
                   GROUP BY ci.collection_id
               ) cnt ON cnt.collection_id = c.id
               LEFT JOIN (
                   SELECT ci.collection_id,
                          MAX(ci.added_at) AS last_added_at,
                          AVG(p.cited_by_count) AS avg_citations,
                          AVG(CASE WHEN p.rating > 0 THEN p.rating ELSE NULL END) AS avg_rating,
                          CASE
                              WHEN MAX(julianday('now') - julianday(ci.added_at)) < 7 THEN 'fresh'
                              WHEN MAX(julianday('now') - julianday(ci.added_at)) < 30 THEN 'active'
                              WHEN MAX(julianday('now') - julianday(ci.added_at)) < 90 THEN 'stale'
                              ELSE 'dormant'
                          END AS activity_status
                   FROM collection_items ci
                   JOIN papers p ON p.id = ci.paper_id AND p.status = 'library'
                   GROUP BY ci.collection_id
               ) health ON health.collection_id = c.id
               ORDER BY c.created_at DESC"""
        )
        rows = cursor.fetchall()
        return [
            CollectionResponse(
                id=r["id"],
                name=r["name"],
                description=r["description"],
                color=r["color"],
                created_at=r["created_at"],
                item_count=r["item_count"],
                last_added_at=r["last_added_at"],
                avg_citations=r["avg_citations"],
                avg_rating=r["avg_rating"],
                activity_status=r["activity_status"],
            )
            for r in rows
        ]
    except Exception as e:
        raise_internal("Failed to list collections", e)


@router.post(
    "/collections",
    response_model=CollectionResponse,
    status_code=status.HTTP_201_CREATED,
    summary="Create a collection",
)
def create_collection(
    req: CollectionCreate,
    db: sqlite3.Connection = Depends(get_db),
):
    """Create a new collection."""
    try:
        cid = uuid.uuid4().hex
        now = datetime.utcnow().isoformat()
        db.execute(
            "INSERT INTO collections (id, name, description, color, created_at) VALUES (?, ?, ?, ?, ?)",
            (cid, req.name, req.description, req.color, now),
        )
        db.commit()
        return CollectionResponse(
            id=cid,
            name=req.name,
            description=req.description,
            color=req.color,
            created_at=now,
            item_count=0,
        )
    except sqlite3.IntegrityError:
        raise HTTPException(status_code=409, detail=f"Collection '{req.name}' already exists")
    except Exception as e:
        raise_internal("Failed to create collection", e)


@router.put(
    "/collections/{collection_id}",
    response_model=CollectionResponse,
    summary="Update a collection",
)
def update_collection(
    collection_id: str,
    req: CollectionCreate,
    db: sqlite3.Connection = Depends(get_db),
):
    """Update name, description, or color of a collection."""
    row = db.execute("SELECT * FROM collections WHERE id = ?", (collection_id,)).fetchone()
    if not row:
        raise HTTPException(status_code=404, detail="Collection not found")
    db.execute(
        "UPDATE collections SET name = ?, description = ?, color = ? WHERE id = ?",
        (req.name, req.description, req.color, collection_id),
    )
    db.commit()
    cnt = db.execute(
        """
        SELECT COUNT(*) AS n
        FROM collection_items ci
        JOIN papers p ON p.id = ci.paper_id
        WHERE ci.collection_id = ?
          AND p.status = 'library'
        """,
        (collection_id,),
    ).fetchone()["n"]
    return CollectionResponse(
        id=collection_id,
        name=req.name,
        description=req.description,
        color=req.color,
        created_at=row["created_at"],
        item_count=cnt,
    )


@router.delete(
    "/collections/{collection_id}",
    status_code=status.HTTP_204_NO_CONTENT,
    summary="Delete a collection",
)
def delete_collection(
    collection_id: str,
    db: sqlite3.Connection = Depends(get_db),
):
    """Delete a collection and cascade-remove its items."""
    cursor = db.execute("DELETE FROM collections WHERE id = ?", (collection_id,))
    # Also remove items (in case FK cascade not enforced by driver)
    db.execute("DELETE FROM collection_items WHERE collection_id = ?", (collection_id,))
    db.commit()
    if cursor.rowcount == 0:
        raise HTTPException(status_code=404, detail="Collection not found")


class _AddItemBody(BaseModel):
    paper_id: str


@router.post(
    "/collections/{collection_id}/items",
    status_code=status.HTTP_201_CREATED,
    summary="Add paper to collection",
)
def add_collection_item(
    collection_id: str,
    body: _AddItemBody,
    db: sqlite3.Connection = Depends(get_db),
):
    """Add a saved Library paper to a collection."""
    coll = db.execute("SELECT id FROM collections WHERE id = ?", (collection_id,)).fetchone()
    if not coll:
        raise HTTPException(status_code=404, detail="Collection not found")
    _require_library_paper(db, body.paper_id)
    now = datetime.utcnow().isoformat()
    try:
        db.execute(
            "INSERT INTO collection_items (collection_id, paper_id, added_at) VALUES (?, ?, ?)",
            (collection_id, body.paper_id, now),
        )
        db.commit()
    except sqlite3.IntegrityError:
        raise HTTPException(status_code=409, detail="Paper already in collection")
    return {"collection_id": collection_id, "paper_id": body.paper_id, "added_at": now}


@router.delete(
    "/collections/{collection_id}/items/{paper_id}",
    status_code=status.HTTP_204_NO_CONTENT,
    summary="Remove paper from collection",
)
def remove_collection_item(
    collection_id: str,
    paper_id: str,
    db: sqlite3.Connection = Depends(get_db),
):
    """Remove a paper from a collection."""
    cursor = db.execute(
        "DELETE FROM collection_items WHERE collection_id = ? AND paper_id = ?",
        (collection_id, paper_id),
    )
    db.commit()
    if cursor.rowcount == 0:
        raise HTTPException(status_code=404, detail="Item not found in collection")


@router.get(
    "/collections/{collection_id}/items",
    response_model=List[PaperResponse],
    summary="List papers in a collection",
)
def list_collection_items(
    collection_id: str,
    db: sqlite3.Connection = Depends(get_db),
):
    """List all papers in a collection."""
    coll = db.execute("SELECT id FROM collections WHERE id = ?", (collection_id,)).fetchone()
    if not coll:
        raise HTTPException(status_code=404, detail="Collection not found")
    cursor = db.execute(
        """SELECT p.*
           FROM collection_items ci
           JOIN papers p ON p.id = ci.paper_id
           WHERE ci.collection_id = ?
             AND p.status = 'library'
           ORDER BY ci.added_at DESC""",
        (collection_id,),
    )
    rows = cursor.fetchall()
    return [row_to_paper_response(r) for r in rows]


# ===================================================================
# Tags
# ===================================================================

@router.get(
    "/tags",
    response_model=List[TagResponse],
    summary="List all tags",
)
def list_tags(
    db: sqlite3.Connection = Depends(get_db),
):
    """Return all tags."""
    rows = db.execute("SELECT * FROM tags ORDER BY name").fetchall()
    return [TagResponse(**dict(r)) for r in rows]


@router.post(
    "/tags",
    response_model=TagResponse,
    status_code=status.HTTP_201_CREATED,
    summary="Create a tag",
)
def create_tag(
    req: TagCreate,
    db: sqlite3.Connection = Depends(get_db),
):
    """Create a new tag."""
    try:
        tid = uuid.uuid4().hex
        db.execute(
            "INSERT INTO tags (id, name, color) VALUES (?, ?, ?)",
            (tid, req.name, req.color),
        )
        db.commit()
        return TagResponse(id=tid, name=req.name, color=req.color)
    except sqlite3.IntegrityError:
        raise HTTPException(status_code=409, detail=f"Tag '{req.name}' already exists")
    except Exception as e:
        raise_internal("Failed to create tag", e)


@router.delete(
    "/tags/{tag_id}",
    status_code=status.HTTP_204_NO_CONTENT,
    summary="Delete a tag",
)
def delete_tag(
    tag_id: str,
    db: sqlite3.Connection = Depends(get_db),
):
    """Delete a tag and cascade-remove its publication assignments."""
    cursor = db.execute("DELETE FROM tags WHERE id = ?", (tag_id,))
    db.execute("DELETE FROM publication_tags WHERE tag_id = ?", (tag_id,))
    db.commit()
    if cursor.rowcount == 0:
        raise HTTPException(status_code=404, detail="Tag not found")


class _AssignTagBody(BaseModel):
    paper_id: str
    tag_id: str


@router.post(
    "/tags/assign",
    status_code=status.HTTP_201_CREATED,
    summary="Assign tag to paper",
)
def assign_tag(
    body: _AssignTagBody,
    db: sqlite3.Connection = Depends(get_db),
):
    """Assign a tag to a saved Library paper."""
    tag = db.execute("SELECT id FROM tags WHERE id = ?", (body.tag_id,)).fetchone()
    if not tag:
        raise HTTPException(status_code=404, detail="Tag not found")
    _require_library_paper(db, body.paper_id)
    existing = db.execute(
        "SELECT 1 FROM publication_tags WHERE paper_id = ? AND tag_id = ?",
        (body.paper_id, body.tag_id),
    ).fetchone()
    if existing:
        raise HTTPException(status_code=409, detail="Tag already assigned to this paper")
    if _paper_tag_count(db, body.paper_id) >= MAX_TAGS_PER_PAPER:
        raise HTTPException(
            status_code=409,
            detail=f"Each paper may have at most {MAX_TAGS_PER_PAPER} tags",
        )
    try:
        db.execute(
            "INSERT INTO publication_tags (paper_id, tag_id) VALUES (?, ?)",
            (body.paper_id, body.tag_id),
        )
        db.commit()
    except sqlite3.IntegrityError:
        raise HTTPException(status_code=409, detail="Tag already assigned to this paper")
    return {"paper_id": body.paper_id, "tag_id": body.tag_id}


@router.delete(
    "/tags/assign/{paper_id}/{tag_id}",
    status_code=status.HTTP_204_NO_CONTENT,
    summary="Remove tag from paper",
)
def remove_tag_assignment(
    paper_id: str,
    tag_id: str,
    db: sqlite3.Connection = Depends(get_db),
):
    """Remove a tag assignment from a paper."""
    cursor = db.execute(
        "DELETE FROM publication_tags WHERE paper_id = ? AND tag_id = ?",
        (paper_id, tag_id),
    )
    db.commit()
    if cursor.rowcount == 0:
        raise HTTPException(status_code=404, detail="Tag assignment not found")


# ===================================================================
# Topics (canonical terms + aliases)
# ===================================================================

def _list_topic_summaries(db: sqlite3.Connection) -> list[TopicSummary]:
    _ensure_topic_alias_table(db)
    if not table_exists(db, "publication_topics"):
        return []

    # Use new canonical topics table when available
    if table_exists(db, "topics"):
        usage_rows = db.execute(
            """
            SELECT COALESCE(t.canonical_name, pt.term) AS canonical,
                   COUNT(DISTINCT pt.paper_id) AS paper_count
            FROM publication_topics pt
            JOIN papers p ON p.id = pt.paper_id
            LEFT JOIN topics t ON pt.topic_id = t.topic_id
            WHERE p.status = 'library'
            GROUP BY canonical
            """
        ).fetchall()
        alias_rows = db.execute(
            """SELECT ta.raw_term, t.canonical_name
               FROM topic_aliases ta
               JOIN topics t ON ta.topic_id = t.topic_id
               ORDER BY t.canonical_name, ta.raw_term"""
        ).fetchall()
    else:
        usage_rows = db.execute(
            """
            SELECT pt.term AS canonical,
                   COUNT(DISTINCT pt.paper_id) AS paper_count
            FROM publication_topics pt
            JOIN papers p ON p.id = pt.paper_id
            WHERE p.status = 'library'
            GROUP BY canonical
            """
        ).fetchall()
        alias_rows = []

    by_canonical: dict[str, dict] = {}
    for row in usage_rows:
        canonical = row["canonical"] if isinstance(row, sqlite3.Row) else row[0]
        count = int(row["paper_count"] if isinstance(row, sqlite3.Row) else row[1])
        if not canonical:
            continue
        by_canonical[canonical] = {
            "canonical": canonical,
            "paper_count": count,
            "aliases": [],
        }

    for row in alias_rows:
        alias = row["raw_term"] if isinstance(row, sqlite3.Row) else row[0]
        canonical = row["canonical_name"] if isinstance(row, sqlite3.Row) else row[1]
        if not canonical:
            continue
        by_canonical.setdefault(
            canonical,
            {"canonical": canonical, "paper_count": 0, "aliases": []},
        )
        if alias and alias.lower() != canonical.lower():
            by_canonical[canonical]["aliases"].append(alias)

    summaries = [
        TopicSummary(
            canonical=v["canonical"],
            paper_count=v["paper_count"],
            aliases=sorted(set(v["aliases"]), key=str.lower),
        )
        for v in by_canonical.values()
    ]
    summaries.sort(key=lambda t: (-t.paper_count, t.canonical.lower()))
    return summaries


@router.get(
    "/topics",
    response_model=List[TopicSummary],
    summary="List canonical topics and aliases",
)
def list_topics(
    db: sqlite3.Connection = Depends(get_db),
):
    return _list_topic_summaries(db)


class TopicHierarchyNode(BaseModel):
    name: str
    paper_count: int


class TopicFieldNode(TopicHierarchyNode):
    subfields: list[TopicHierarchyNode] = Field(default_factory=list)


class TopicDomainNode(TopicHierarchyNode):
    fields: list[TopicFieldNode] = Field(default_factory=list)


class TopicHierarchyResponse(BaseModel):
    domains: list[TopicDomainNode] = Field(default_factory=list)


@router.get(
    "/topics/hierarchy",
    response_model=TopicHierarchyResponse,
    summary="Get OpenAlex topic hierarchy with paper counts",
)
def get_topic_hierarchy(
    db: sqlite3.Connection = Depends(get_db),
):
    """Return OpenAlex topic hierarchy (domain/field/subfield) with paper counts."""
    if not table_exists(db, "publication_topics"):
        return TopicHierarchyResponse(domains=[])

    rows = db.execute(
        """
        SELECT pt.domain, pt.field, pt.subfield, COUNT(DISTINCT pt.paper_id) as paper_count
        FROM publication_topics pt
        JOIN papers p ON p.id = pt.paper_id
        WHERE pt.domain IS NOT NULL AND pt.domain != ''
          AND p.status = 'library'
        GROUP BY pt.domain, pt.field, pt.subfield
        ORDER BY pt.domain, pt.field, pt.subfield
        """
    ).fetchall()

    # Build hierarchical structure
    domains_dict: dict[str, dict] = {}

    for row in rows:
        domain = row["domain"] if isinstance(row, sqlite3.Row) else row[0]
        field = row["field"] if isinstance(row, sqlite3.Row) else row[1]
        subfield = row["subfield"] if isinstance(row, sqlite3.Row) else row[2]
        paper_count = int(row["paper_count"] if isinstance(row, sqlite3.Row) else row[3])

        if not domain:
            continue

        # Initialize domain if needed
        if domain not in domains_dict:
            domains_dict[domain] = {
                "name": domain,
                "paper_count": 0,
                "fields": {},
            }

        # Accumulate domain count
        domains_dict[domain]["paper_count"] += paper_count

        # If we have a field, process field level
        if field:
            if field not in domains_dict[domain]["fields"]:
                domains_dict[domain]["fields"][field] = {
                    "name": field,
                    "paper_count": 0,
                    "subfields": {},
                }

            # Accumulate field count
            domains_dict[domain]["fields"][field]["paper_count"] += paper_count

            # If we have a subfield, add it
            if subfield:
                if subfield not in domains_dict[domain]["fields"][field]["subfields"]:
                    domains_dict[domain]["fields"][field]["subfields"][subfield] = {
                        "name": subfield,
                        "paper_count": paper_count,
                    }

    # Convert to response format
    domains = []
    for domain_data in sorted(domains_dict.values(), key=lambda x: (-x["paper_count"], x["name"])):
        fields = []
        for field_data in sorted(domain_data["fields"].values(), key=lambda x: (-x["paper_count"], x["name"])):
            subfields = [
                TopicHierarchyNode(name=sf["name"], paper_count=sf["paper_count"])
                for sf in sorted(field_data["subfields"].values(), key=lambda x: (-x["paper_count"], x["name"]))
            ]
            fields.append(
                TopicFieldNode(
                    name=field_data["name"],
                    paper_count=field_data["paper_count"],
                    subfields=subfields,
                )
            )

        domains.append(
            TopicDomainNode(
                name=domain_data["name"],
                paper_count=domain_data["paper_count"],
                fields=fields,
            )
        )

    return TopicHierarchyResponse(domains=domains)


@router.post(
    "/topics",
    response_model=TopicSummary,
    status_code=status.HTTP_201_CREATED,
    summary="Create a canonical topic",
)
def create_topic(
    req: TopicCreateRequest,
    db: sqlite3.Connection = Depends(get_db),
):
    canonical = normalize_topic_term(req.name)
    if not canonical:
        raise HTTPException(status_code=422, detail="Topic name cannot be empty")

    _ensure_topic_alias_table(db)

    from alma.library.topic_deduplication import normalize_topic, _topic_id_from_normalized

    normalized = normalize_topic(canonical)
    if not normalized:
        raise HTTPException(status_code=422, detail="Topic name cannot be empty")

    # Check if already exists as alias for another topic
    existing = db.execute(
        "SELECT ta.topic_id, t.canonical_name FROM topic_aliases ta "
        "JOIN topics t ON ta.topic_id = t.topic_id "
        "WHERE ta.normalized_term = ?",
        (normalized,),
    ).fetchone()
    if existing and existing["canonical_name"].lower() != canonical.lower():
        raise HTTPException(
            status_code=409,
            detail=f"'{canonical}' already exists as an alias for '{existing['canonical_name']}'",
        )

    topic_id = _topic_id_from_normalized(normalized)
    now = datetime.utcnow().isoformat()
    db.execute(
        """INSERT OR IGNORE INTO topics (topic_id, canonical_name, normalized_name, source, created_at)
           VALUES (?, ?, ?, 'manual', ?)""",
        (topic_id, canonical, normalized, now),
    )
    db.execute(
        """INSERT OR IGNORE INTO topic_aliases (topic_id, raw_term, normalized_term, source, confidence, created_at)
           VALUES (?, ?, ?, 'manual', 1.0, ?)""",
        (topic_id, canonical, normalized, now),
    )
    db.commit()

    for topic in _list_topic_summaries(db):
        if topic.canonical.lower() == canonical.lower():
            return topic
    return TopicSummary(canonical=canonical, paper_count=0, aliases=[])


@router.post(
    "/topics/aliases",
    response_model=TopicSummary,
    status_code=status.HTTP_201_CREATED,
    summary="Create or update a topic alias",
)
def create_topic_alias(
    req: TopicAliasCreateRequest,
    db: sqlite3.Connection = Depends(get_db),
):
    alias = normalize_topic_term(req.alias)
    canonical = normalize_topic_term(req.canonical)
    if not alias or not canonical:
        raise HTTPException(status_code=422, detail="Alias and canonical must be non-empty")

    _ensure_topic_alias_table(db)

    from alma.library.topic_deduplication import normalize_topic, _topic_id_from_normalized

    canon_normalized = normalize_topic(canonical)
    alias_normalized = normalize_topic(alias)
    topic_id = _topic_id_from_normalized(canon_normalized)
    now = datetime.utcnow().isoformat()

    # Ensure canonical topic exists
    db.execute(
        """INSERT OR IGNORE INTO topics (topic_id, canonical_name, normalized_name, source, created_at)
           VALUES (?, ?, ?, 'manual', ?)""",
        (topic_id, canonical, canon_normalized, now),
    )
    # Insert the alias
    db.execute(
        """INSERT INTO topic_aliases (topic_id, raw_term, normalized_term, source, confidence, created_at)
           VALUES (?, ?, ?, 'manual', 1.0, ?)
           ON CONFLICT(normalized_term) DO UPDATE SET
               topic_id = excluded.topic_id,
               raw_term = excluded.raw_term""",
        (topic_id, alias, alias_normalized, now),
    )
    # Link matching publication_topics
    db.execute(
        "UPDATE publication_topics SET topic_id = ? WHERE topic_id IS NULL AND LOWER(TRIM(term)) = ?",
        (topic_id, alias_normalized),
    )
    db.commit()

    for topic in _list_topic_summaries(db):
        if topic.canonical.lower() == canonical.lower():
            return topic
    return TopicSummary(canonical=canonical, paper_count=0, aliases=[alias] if alias.lower() != canonical.lower() else [])


@router.put(
    "/topics/{topic_name}",
    response_model=TopicSummary,
    summary="Rename a canonical topic",
)
def rename_topic(
    topic_name: str,
    req: TopicRenameRequest,
    db: sqlite3.Connection = Depends(get_db),
):
    old_name = normalize_topic_term(topic_name)
    new_name = normalize_topic_term(req.new_name)
    if not old_name or not new_name:
        raise HTTPException(status_code=422, detail="Topic names cannot be empty")
    if old_name.lower() == new_name.lower():
        for topic in _list_topic_summaries(db):
            if topic.canonical.lower() == old_name.lower():
                return topic
        raise HTTPException(status_code=404, detail="Topic not found")

    _ensure_topic_alias_table(db)

    from alma.library.topic_deduplication import normalize_topic, _topic_id_from_normalized

    old_normalized = normalize_topic(old_name)
    new_normalized = normalize_topic(new_name)

    # Find existing topic by old normalized name
    existing = db.execute(
        "SELECT topic_id FROM topics WHERE normalized_name = ?",
        (old_normalized,),
    ).fetchone()
    if not existing:
        pub_exists = db.execute(
            "SELECT 1 FROM publication_topics WHERE LOWER(TRIM(term)) = LOWER(TRIM(?)) LIMIT 1",
            (old_name,),
        ).fetchone()
        if not pub_exists:
            raise HTTPException(status_code=404, detail="Topic not found")
        # Create a new topic for the rename
        topic_id = _topic_id_from_normalized(new_normalized)
    else:
        topic_id = existing["topic_id"]

    now = datetime.utcnow().isoformat()

    # Update the canonical name
    db.execute(
        "UPDATE topics SET canonical_name = ?, normalized_name = ? WHERE topic_id = ?",
        (new_name, new_normalized, topic_id),
    )

    # Ensure old name is an alias
    db.execute(
        """INSERT OR IGNORE INTO topic_aliases (topic_id, raw_term, normalized_term, source, confidence, created_at)
           VALUES (?, ?, ?, 'manual', 1.0, ?)""",
        (topic_id, old_name, old_normalized, now),
    )
    db.commit()

    for topic in _list_topic_summaries(db):
        if topic.canonical.lower() == new_name.lower():
            return topic
    return TopicSummary(canonical=new_name, paper_count=0, aliases=[old_name])


@router.post(
    "/topics/group",
    response_model=TopicSummary,
    summary="Group one topic under another canonical topic",
)
def group_topic(
    req: TopicGroupRequest,
    db: sqlite3.Connection = Depends(get_db),
):
    source = normalize_topic_term(req.source)
    target = normalize_topic_term(req.target)
    if not source or not target:
        raise HTTPException(status_code=422, detail="Source and target cannot be empty")

    _ensure_topic_alias_table(db)

    from alma.library.topic_deduplication import normalize_topic, _topic_id_from_normalized, merge_topics

    target_normalized = normalize_topic(target)
    source_normalized = normalize_topic(source)
    target_topic_id = _topic_id_from_normalized(target_normalized)
    source_topic_id = _topic_id_from_normalized(source_normalized)
    now = datetime.utcnow().isoformat()

    # Ensure target topic exists
    db.execute(
        """INSERT OR IGNORE INTO topics (topic_id, canonical_name, normalized_name, source, created_at)
           VALUES (?, ?, ?, 'manual', ?)""",
        (target_topic_id, target, target_normalized, now),
    )

    if source_topic_id != target_topic_id:
        # If source topic exists, merge it
        source_exists = db.execute(
            "SELECT 1 FROM topics WHERE topic_id = ?", (source_topic_id,)
        ).fetchone()
        if source_exists:
            merge_topics(db, keep_topic_id=target_topic_id, merge_topic_id=source_topic_id)
        else:
            # Just add the source as an alias
            db.execute(
                """INSERT OR IGNORE INTO topic_aliases (topic_id, raw_term, normalized_term, source, confidence, created_at)
                   VALUES (?, ?, ?, 'manual', 1.0, ?)""",
                (target_topic_id, source, source_normalized, now),
            )
            # Link publication_topics
            db.execute(
                "UPDATE publication_topics SET topic_id = ? WHERE topic_id IS NULL AND LOWER(TRIM(term)) = ?",
                (target_topic_id, source_normalized),
            )
    db.commit()

    for topic in _list_topic_summaries(db):
        if topic.canonical.lower() == target.lower():
            return topic
    return TopicSummary(canonical=target, paper_count=0, aliases=[source] if source.lower() != target.lower() else [])


@router.delete(
    "/topics/{topic_name}",
    status_code=status.HTTP_204_NO_CONTENT,
    summary="Delete a canonical topic mapping",
)
def delete_topic(
    topic_name: str,
    replacement: Optional[str] = Query(
        None,
        description="Optional replacement canonical topic for existing aliases",
    ),
    db: sqlite3.Connection = Depends(get_db),
):
    canonical = normalize_topic_term(topic_name)
    if not canonical:
        raise HTTPException(status_code=422, detail="Topic name cannot be empty")

    _ensure_topic_alias_table(db)

    from alma.library.topic_deduplication import normalize_topic, _topic_id_from_normalized, merge_topics

    canon_normalized = normalize_topic(canonical)
    topic_row = db.execute(
        "SELECT topic_id FROM topics WHERE normalized_name = ?",
        (canon_normalized,),
    ).fetchone()

    pub_has_rows = db.execute(
        "SELECT 1 FROM publication_topics WHERE LOWER(TRIM(term)) = LOWER(TRIM(?)) LIMIT 1",
        (canonical,),
    ).fetchone()
    if not topic_row and not pub_has_rows:
        raise HTTPException(status_code=404, detail="Topic not found")

    if replacement:
        replacement_term = normalize_topic_term(replacement)
        if not replacement_term:
            raise HTTPException(status_code=422, detail="Replacement topic cannot be empty")

        repl_normalized = normalize_topic(replacement_term)
        repl_topic_id = _topic_id_from_normalized(repl_normalized)
        now = datetime.utcnow().isoformat()

        # Ensure replacement topic exists
        db.execute(
            """INSERT OR IGNORE INTO topics (topic_id, canonical_name, normalized_name, source, created_at)
               VALUES (?, ?, ?, 'manual', ?)""",
            (repl_topic_id, replacement_term, repl_normalized, now),
        )

        if topic_row:
            # Merge old topic into replacement
            merge_topics(db, keep_topic_id=repl_topic_id, merge_topic_id=topic_row["topic_id"])
        else:
            # Just update publication_topics
            db.execute(
                "UPDATE publication_topics SET topic_id = ? WHERE LOWER(TRIM(term)) = ?",
                (repl_topic_id, canon_normalized),
            )
    else:
        if topic_row:
            db.execute("DELETE FROM topic_aliases WHERE topic_id = ?", (topic_row["topic_id"],))
            db.execute("UPDATE publication_topics SET topic_id = NULL WHERE topic_id = ?", (topic_row["topic_id"],))
            db.execute("DELETE FROM topics WHERE topic_id = ?", (topic_row["topic_id"],))
        db.execute(
            "DELETE FROM publication_topics WHERE LOWER(TRIM(term)) = LOWER(TRIM(?))",
            (canonical,),
        )
    db.commit()


@router.delete(
    "/topics/aliases/{alias_name}",
    status_code=status.HTTP_204_NO_CONTENT,
    summary="Delete a topic alias",
)
def delete_topic_alias(
    alias_name: str,
    db: sqlite3.Connection = Depends(get_db),
):
    alias = normalize_topic_term(alias_name)
    if not alias:
        raise HTTPException(status_code=422, detail="Alias name cannot be empty")

    _ensure_topic_alias_table(db)

    from alma.library.topic_deduplication import normalize_topic

    alias_normalized = normalize_topic(alias)
    cursor = db.execute(
        "DELETE FROM topic_aliases WHERE normalized_term = ?",
        (alias_normalized,),
    )
    db.commit()
    if cursor.rowcount == 0:
        raise HTTPException(status_code=404, detail="Alias not found")


# ===================================================================
# Followed Authors
# ===================================================================

@router.get(
    "/followed-authors",
    response_model=List[FollowedAuthorResponse],
    summary="List followed authors",
)
def list_followed_authors(
    db: sqlite3.Connection = Depends(get_db),
):
    """Return all followed authors, joining with the authors table for display names."""
    ensure_followed_author_contract(db)
    rows = db.execute(
        """
        SELECT fa.author_id, fa.followed_at, fa.notify_new_papers, a.name
        FROM followed_authors fa
        LEFT JOIN authors a ON a.id = fa.author_id
        ORDER BY fa.followed_at DESC
        """
    ).fetchall()
    return [
        FollowedAuthorResponse(
            author_id=r["author_id"],
            followed_at=r["followed_at"],
            notify_new_papers=bool(r["notify_new_papers"]),
            name=r["name"],
        )
        for r in rows
    ]


@router.post(
    "/followed-authors",
    response_model=FollowedAuthorResponse,
    status_code=status.HTTP_201_CREATED,
    summary="Follow an author",
)
def follow_author(
    req: FollowAuthorRequest,
    db: sqlite3.Connection = Depends(get_db),
):
    """Start following an author."""
    ensure_followed_author_contract(db)
    canonical_author_id = resolve_canonical_author_id(
        db,
        req.author_id,
        create_if_missing=True,
        fallback_name=req.author_id,
    )
    if not canonical_author_id:
        raise HTTPException(status_code=404, detail="Author could not be resolved")
    now = datetime.utcnow().isoformat()
    already_followed = db.execute(
        "SELECT 1 FROM followed_authors WHERE author_id = ? LIMIT 1",
        (canonical_author_id,),
    ).fetchone() is not None
    if already_followed:
        raise HTTPException(status_code=409, detail="Already following this author")

    # One canonical entry point — keeps followed_authors / author_type /
    # feed_monitors synchronized in the same transaction.
    apply_follow_state(db, canonical_author_id, followed=True)
    # Respect the user's notify_new_papers override (default is 1, which
    # apply_follow_state already sets — only override when explicitly false).
    if not req.notify_new_papers:
        db.execute(
            "UPDATE followed_authors SET notify_new_papers = 0 WHERE author_id = ?",
            (canonical_author_id,),
        )
    row = db.execute(
        "SELECT openalex_id FROM authors WHERE id = ?",
        (canonical_author_id,),
    ).fetchone()
    feedback_author_id = str((row["openalex_id"] if row else "") or req.author_id or "").strip()
    if feedback_author_id:
        from alma.application.gap_radar import clear_missing_author_feedback

        clear_missing_author_feedback(db, feedback_author_id)
    db.commit()

    try:
        schedule_followed_author_historical_backfill(canonical_author_id, trigger="library_follow")
    except Exception as exc:
        logger.debug("Could not queue historical backfill for %s: %s", canonical_author_id, exc)
    # Resolve name (same unified DB)
    name = None
    try:
        row = db.execute("SELECT name FROM authors WHERE id = ?", (canonical_author_id,)).fetchone()
        if row:
            name = row["name"]
    except Exception:
        pass
    return FollowedAuthorResponse(
        author_id=canonical_author_id,
        followed_at=now,
        notify_new_papers=req.notify_new_papers,
        name=name,
    )


@router.delete(
    "/followed-authors/{author_id}",
    status_code=status.HTTP_204_NO_CONTENT,
    summary="Unfollow an author",
)
def unfollow_author(
    author_id: str,
    db: sqlite3.Connection = Depends(get_db),
):
    """Stop following an author."""
    ensure_followed_author_contract(db)
    canonical_author_id = resolve_canonical_author_id(db, author_id, create_if_missing=False) or author_id
    existed = db.execute(
        "SELECT 1 FROM followed_authors WHERE author_id = ? LIMIT 1",
        (canonical_author_id,),
    ).fetchone() is not None
    if not existed:
        raise HTTPException(status_code=404, detail="Followed author not found")

    # Single entry point — deletes from followed_authors, demotes
    # authors.author_type, and tears down the mirrored feed_monitors row.
    apply_follow_state(db, canonical_author_id, followed=False)
    db.commit()


# ===================================================================
# Reading Status
# ===================================================================

class ReadingStatusUpdateRequest(BaseModel):
    """Request model for updating paper reading status."""
    reading_status: Optional[str] = Field(
        None,
        description="Reading status: reading | done | excluded | null"
    )


@router.patch(
    "/papers/{paper_id}/reading-status",
    response_model=PaperResponse,
    summary="Update paper reading status",
)
def update_reading_status(
    paper_id: str,
    req: ReadingStatusUpdateRequest,
    db: sqlite3.Connection = Depends(get_db),
):
    """Update the reading status of any tracked paper.

    D2 lifecycle (post-2026-04-26): reading state ∈ {None, reading,
    done, excluded}. Membership of the reading list IS `reading` —
    there's no separate `queued` step. The legacy `queued` value is
    silently coerced to `reading` for back-compat with any client
    still sending it.
    """
    try:
        # Coerce legacy `queued` → `reading` for back-compat.
        incoming = req.reading_status
        if incoming == "queued":
            incoming = "reading"

        valid_statuses = [None, "reading", "done", "excluded"]
        if incoming not in valid_statuses:
            raise HTTPException(
                status_code=400,
                detail=f"Invalid reading status. Must be one of: {valid_statuses}"
            )

        # Update the paper
        cursor = db.execute(
            "UPDATE papers SET reading_status = ? WHERE id = ?",
            (incoming, paper_id),
        )
        db.commit()

        if cursor.rowcount == 0:
            raise HTTPException(status_code=404, detail="Paper not found")

        # Return updated paper
        row = db.execute("SELECT * FROM papers WHERE id = ?", (paper_id,)).fetchone()
        return row_to_paper_response(row)
    except HTTPException:
        raise
    except Exception as e:
        raise_internal("Failed to update reading status", e)


class ReadingQueueResponse(BaseModel):
    """Response model for reading list grouped by status."""
    reading: List[PaperResponse] = Field(default_factory=list)
    done: List[PaperResponse] = Field(default_factory=list)
    excluded: List[PaperResponse] = Field(default_factory=list)


@router.get(
    "/reading-queue",
    response_model=ReadingQueueResponse,
    summary="Get reading list grouped by status",
)
def get_reading_queue(
    db: sqlite3.Connection = Depends(get_db),
):
    """Return the reading list grouped by status.

    D2 lifecycle (post-2026-04-26): reading state ∈ {None, reading,
    done, excluded}. The legacy `queued` bucket has been retired —
    membership of the reading list IS `reading`. Reading state stays
    independent from library membership: marking reading does not
    save, saving does not mark reading.
    """
    try:
        rows = db.execute(
            """SELECT * FROM papers
               WHERE COALESCE(TRIM(reading_status), '') <> ''
               ORDER BY
                   CASE
                       WHEN reading_status = 'reading' THEN 0
                       WHEN reading_status = 'done' THEN 1
                       WHEN reading_status = 'excluded' THEN 2
                       ELSE 3
                   END,
                   COALESCE(publication_date, added_at, created_at, '') DESC"""
        ).fetchall()

        queue = ReadingQueueResponse()
        for row in rows:
            paper = row_to_paper_response(row)
            reading_status = paper.reading_status

            if reading_status == "reading":
                queue.reading.append(paper)
            elif reading_status == "done":
                queue.done.append(paper)
            elif reading_status == "excluded":
                queue.excluded.append(paper)

        return queue
    except Exception as e:
        raise_internal("Failed to get reading list", e)


@router.get(
    "/workflow-summary",
    summary="Get library workflow summary",
)
def get_library_workflow_summary(
    db: sqlite3.Connection = Depends(get_db),
):
    """Return a workflow-oriented summary of the library state."""
    try:
        total_row = db.execute(
            """
            SELECT
                COUNT(*) AS total_library,
                ROUND(COALESCE(AVG(CASE WHEN rating > 0 THEN rating END), 0), 2) AS avg_rating,
                COALESCE(SUM(CASE WHEN reading_status IS NULL OR TRIM(reading_status) = '' THEN 1 ELSE 0 END), 0) AS no_status_count,
                COALESCE(SUM(CASE WHEN COALESCE(added_from, '') LIKE 'feed%' THEN 1 ELSE 0 END), 0) AS from_feed,
                COALESCE(SUM(CASE WHEN COALESCE(added_from, '') LIKE 'discovery%' THEN 1 ELSE 0 END), 0) AS from_discovery,
                COALESCE(SUM(CASE WHEN COALESCE(added_from, '') = 'import' THEN 1 ELSE 0 END), 0) AS from_import,
                COALESCE(SUM(CASE WHEN COALESCE(added_from, '') NOT LIKE 'feed%' AND COALESCE(added_from, '') NOT LIKE 'discovery%' AND COALESCE(added_from, '') != 'import' THEN 1 ELSE 0 END), 0) AS from_manual_or_other
            FROM papers
            WHERE status = 'library'
            """
        ).fetchone()
        reading_row = db.execute(
            """
            SELECT
                COALESCE(SUM(CASE WHEN reading_status = 'reading' THEN 1 ELSE 0 END), 0) AS reading_count,
                COALESCE(SUM(CASE WHEN reading_status = 'done' THEN 1 ELSE 0 END), 0) AS done_count,
                COALESCE(SUM(CASE WHEN reading_status = 'excluded' THEN 1 ELSE 0 END), 0) AS excluded_count,
                COALESCE(SUM(CASE WHEN COALESCE(TRIM(reading_status), '') <> '' THEN 1 ELSE 0 END), 0) AS reading_list_count
            FROM papers
            """
        ).fetchone()

        total_library = int(total_row["total_library"] or 0)
        collections_total = 0
        if table_exists(db, "collections"):
            collections_total = int(
                db.execute("SELECT COUNT(*) AS c FROM collections").fetchone()["c"] or 0
            )

        uncollected_count = 0
        if table_exists(db, "collection_items"):
            uncollected_count = int(
                db.execute(
                    """
                    SELECT COUNT(*) AS c
                    FROM papers p
                    WHERE p.status = 'library'
                      AND NOT EXISTS (
                        SELECT 1 FROM collection_items ci WHERE ci.paper_id = p.id
                      )
                    """
                ).fetchone()["c"]
                or 0
            )

        tagged_count = 0
        if table_exists(db, "publication_tags"):
            tagged_count = int(
                db.execute(
                    """
                    SELECT COUNT(DISTINCT p.id) AS c
                    FROM papers p
                    JOIN publication_tags pt ON pt.paper_id = p.id
                    WHERE p.status = 'library'
                    """
                ).fetchone()["c"]
                or 0
            )

        topic_count = 0
        if table_exists(db, "publication_topics"):
            topic_count = int(
                db.execute(
                    """
                    SELECT COUNT(DISTINCT p.id) AS c
                    FROM papers p
                    JOIN publication_topics pt ON pt.paper_id = p.id
                    WHERE p.status = 'library'
                    """
                ).fetchone()["c"]
                or 0
            )

        source_rows = db.execute(
            """
            SELECT
                CASE
                    WHEN COALESCE(added_from, '') = '' THEN 'manual'
                    WHEN COALESCE(added_from, '') LIKE 'feed%' THEN 'feed'
                    WHEN COALESCE(added_from, '') LIKE 'discovery%' THEN 'discovery'
                    ELSE added_from
                END AS source,
                COUNT(*) AS count
            FROM papers
            WHERE status = 'library'
            GROUP BY CASE
                WHEN COALESCE(added_from, '') = '' THEN 'manual'
                WHEN COALESCE(added_from, '') LIKE 'feed%' THEN 'feed'
                WHEN COALESCE(added_from, '') LIKE 'discovery%' THEN 'discovery'
                ELSE added_from
            END
            ORDER BY count DESC, source ASC
            """
        ).fetchall()

        reading_rows = db.execute(
            """
            SELECT reading_status AS reading_bucket, COUNT(*) AS count
            FROM papers
            WHERE COALESCE(TRIM(reading_status), '') <> ''
            GROUP BY reading_status
            ORDER BY count DESC, reading_bucket ASC
            """
        ).fetchall()

        recent_rows = db.execute(
            """
            SELECT *
            FROM papers
            WHERE status = 'library'
            ORDER BY COALESCE(added_at, created_at) DESC
            LIMIT 6
            """
        ).fetchall()

        next_rows = db.execute(
            """
            SELECT *
            FROM papers
            WHERE reading_status = 'reading'
            ORDER BY
                COALESCE(rating, 0) DESC,
                COALESCE(publication_date, added_at, created_at, '') DESC
            LIMIT 6
            """
        ).fetchall()

        # "Needs Attention" — Library papers with concrete metadata gaps.
        # This list powers the landing-page Needs Attention card and is meant
        # to surface rows the user should clean up:
        #   - no canonical identifier (neither DOI nor OpenAlex ID)
        #   - missing abstract (short or empty)
        #   - missing authors (short or empty)
        #   - OpenAlex enrichment stuck in non-terminal / failed states
        # Papers that are healthy by those measures are never shown here.
        needs_attention_rows = db.execute(
            """
            SELECT *,
                   CASE
                     WHEN (openalex_id IS NULL OR TRIM(openalex_id) = '')
                          AND (doi IS NULL OR TRIM(doi) = '') THEN 1
                     ELSE 0
                   END AS flag_no_identifier,
                   CASE
                     WHEN abstract IS NULL OR LENGTH(TRIM(abstract)) < 40 THEN 1
                     ELSE 0
                   END AS flag_no_abstract,
                   CASE
                     WHEN authors IS NULL OR LENGTH(TRIM(authors)) < 3 THEN 1
                     ELSE 0
                   END AS flag_no_authors,
                   CASE
                     WHEN openalex_resolution_status IN (
                         'pending_enrichment',
                         'not_openalex_resolved',
                         'failed'
                     ) THEN 1
                     ELSE 0
                   END AS flag_enrichment_stuck,
                   (
                     CASE
                       WHEN (openalex_id IS NULL OR TRIM(openalex_id) = '')
                            AND (doi IS NULL OR TRIM(doi) = '') THEN 1
                       ELSE 0
                     END
                     + CASE
                         WHEN abstract IS NULL OR LENGTH(TRIM(abstract)) < 40 THEN 1
                         ELSE 0
                       END
                     + CASE
                         WHEN authors IS NULL OR LENGTH(TRIM(authors)) < 3 THEN 1
                         ELSE 0
                       END
                     + CASE
                         WHEN openalex_resolution_status IN (
                             'pending_enrichment',
                             'not_openalex_resolved',
                             'failed'
                         ) THEN 1
                         ELSE 0
                       END
                   ) AS issue_count
            FROM papers
            WHERE status = 'library'
              AND (
                    (
                      (openalex_id IS NULL OR TRIM(openalex_id) = '')
                      AND (doi IS NULL OR TRIM(doi) = '')
                    )
                 OR (abstract IS NULL OR LENGTH(TRIM(abstract)) < 40)
                 OR (authors IS NULL OR LENGTH(TRIM(authors)) < 3)
                 OR openalex_resolution_status IN (
                        'pending_enrichment',
                        'not_openalex_resolved',
                        'failed'
                    )
              )
            ORDER BY issue_count DESC,
                     COALESCE(rating, 0) DESC,
                     COALESCE(added_at, created_at) DESC
            LIMIT 6
            """
        ).fetchall()

        def _attention_reasons(row) -> list[dict]:
            """Translate the per-row flag columns into actionable reasons.

            Each reason is a small dict with `code` (stable enum the
            frontend can switch on for icons/buttons), `label` (short
            human text shown next to the row), `detail` (concrete
            specifics — the actual failing field value, status string,
            or measured length, so the user knows *what* is wrong, not
            just *that* something is wrong), and `action` (suggested
            fix verb; null when there's no canonical action).
            """
            out: list[dict] = []
            if row["flag_enrichment_stuck"]:
                status_raw = str(row["openalex_resolution_status"] or "").strip() or "unknown"
                # The status enum is verbose ("not_openalex_resolved");
                # surface it verbatim so the user can see exactly which
                # state machine bucket the row got stuck in.
                detail = f"Resolution status: {status_raw}"
                out.append({
                    "code": "enrichment_stuck",
                    "label": "OpenAlex enrichment didn't complete",
                    "detail": detail,
                    "action": "rerun_enrichment",
                })
            if row["flag_no_identifier"]:
                # Always include both fields — saying which one is
                # actually missing makes the message actionable
                # ("paste a DOI" vs "paste an OpenAlex URL").
                missing: list[str] = []
                if not str(row["doi"] or "").strip():
                    missing.append("DOI")
                if not str(row["openalex_id"] or "").strip():
                    missing.append("OpenAlex ID")
                out.append({
                    "code": "no_identifier",
                    "label": "No canonical identifier — paper can't be linked",
                    "detail": "Missing: " + " + ".join(missing) if missing else None,
                    "action": "find_identifier",
                })
            if row["flag_no_abstract"]:
                # Distinguish "totally absent" from "too short to be useful"
                # — the fix is the same (re-enrich) but the user often
                # wants to know whether the row ever had an abstract.
                abstract_text = str(row["abstract"] or "").strip()
                if not abstract_text:
                    detail = "Abstract is empty"
                else:
                    detail = f"Abstract is only {len(abstract_text)} chars (need 40+)"
                out.append({
                    "code": "no_abstract",
                    "label": "Missing abstract",
                    "detail": detail,
                    "action": "rerun_enrichment",
                })
            if row["flag_no_authors"]:
                authors_text = str(row["authors"] or "").strip()
                if not authors_text:
                    detail = "Author list is empty"
                else:
                    # Show the truncated current value so the user can
                    # see what the import gave us — often enough to
                    # decide whether to fix manually or re-enrich.
                    preview = authors_text[:60].replace("\n", " ")
                    detail = f"Currently: \"{preview}\""
                out.append({
                    "code": "no_authors",
                    "label": "Missing author list",
                    "detail": detail,
                    "action": "rerun_enrichment",
                })
            return out
        needs_attention_count = int(
            (
                db.execute(
                    """
                    SELECT COUNT(*) AS c
                    FROM papers
                    WHERE status = 'library'
                      AND (
                            (
                              (openalex_id IS NULL OR TRIM(openalex_id) = '')
                              AND (doi IS NULL OR TRIM(doi) = '')
                            )
                         OR (abstract IS NULL OR LENGTH(TRIM(abstract)) < 40)
                         OR (authors IS NULL OR LENGTH(TRIM(authors)) < 3)
                         OR openalex_resolution_status IN (
                                'pending_enrichment',
                                'not_openalex_resolved',
                                'failed'
                            )
                      )
                    """
                ).fetchone()["c"]
                or 0
            )
        )

        top_collection_rows = []
        if table_exists(db, "collections") and table_exists(db, "collection_items"):
            top_collection_rows = db.execute(
                """
                SELECT c.name, COUNT(p.id) AS count
                FROM collections c
                LEFT JOIN collection_items ci ON ci.collection_id = c.id
                LEFT JOIN papers p ON p.id = ci.paper_id AND p.status = 'library'
                GROUP BY c.id, c.name
                ORDER BY count DESC, c.name ASC
                LIMIT 6
                """
            ).fetchall()

        top_tag_rows = []
        if table_exists(db, "tags") and table_exists(db, "publication_tags"):
            top_tag_rows = db.execute(
                """
                SELECT t.name, COUNT(DISTINCT pt.paper_id) AS count
                FROM tags t
                JOIN publication_tags pt ON pt.tag_id = t.id
                JOIN papers p ON p.id = pt.paper_id
                WHERE p.status = 'library'
                GROUP BY t.id, t.name
                ORDER BY count DESC, t.name ASC
                LIMIT 8
                """
            ).fetchall()

        top_topic_rows = []
        if table_exists(db, "publication_topics"):
            top_topic_rows = db.execute(
                """
                SELECT COALESCE(t.canonical_name, pt.term, '') AS term, COUNT(DISTINCT pt.paper_id) AS count
                FROM publication_topics pt
                JOIN papers p ON p.id = pt.paper_id
                LEFT JOIN topics t ON t.topic_id = pt.topic_id
                WHERE p.status = 'library'
                  AND COALESCE(TRIM(pt.term), '') <> ''
                GROUP BY COALESCE(t.canonical_name, pt.term, '')
                ORDER BY count DESC, term ASC
                LIMIT 8
                """
            ).fetchall()

        health = {
            "collection_coverage_pct": round((1.0 - (uncollected_count / max(1, total_library))) * 100, 1),
            "tag_coverage_pct": round((tagged_count / max(1, total_library)) * 100, 1),
            "topic_coverage_pct": round((topic_count / max(1, total_library)) * 100, 1),
            "rated_pct": round((safe_div(int(db.execute("SELECT COUNT(*) AS c FROM papers WHERE status = 'library' AND rating > 0").fetchone()["c"] or 0), max(1, total_library))) * 100, 1),
            "cleanup_flags": {
                "uncollected": uncollected_count,
                "untagged": max(0, total_library - tagged_count),
                "untopiced": max(0, total_library - topic_count),
            },
        }
        cleanup_guidance = []
        if uncollected_count >= 5:
            cleanup_guidance.append("Group uncategorized papers into collections so Discovery can reuse stronger structure.")
        if max(0, total_library - tagged_count) >= 5:
            cleanup_guidance.append("Apply tags to under-structured papers to improve downstream retrieval and cleanup flows.")
        if max(0, total_library - topic_count) >= 5:
            cleanup_guidance.append("Enrich missing topics so topic-driven discovery and reporting stay reliable.")

        return {
            "summary": {
                "total_library": total_library,
                "avg_rating": float(total_row["avg_rating"] or 0.0),
                "reading_count": int(reading_row["reading_count"] or 0),
                "done_count": int(reading_row["done_count"] or 0),
                "excluded_count": int(reading_row["excluded_count"] or 0),
                "reading_list_count": int(reading_row["reading_list_count"] or 0),
                "collections_total": collections_total,
                "uncollected_count": uncollected_count,
            },
            "acquisition": {
                "from_feed": int(total_row["from_feed"] or 0),
                "from_discovery": int(total_row["from_discovery"] or 0),
                "from_import": int(total_row["from_import"] or 0),
                "from_manual_or_other": int(total_row["from_manual_or_other"] or 0),
            },
            "source_mix": [
                {"source": row["source"], "count": int(row["count"] or 0)}
                for row in source_rows
            ],
            "reading_mix": [
                {"status": row["reading_bucket"], "count": int(row["count"] or 0)}
                for row in reading_rows
            ],
            "recent_additions": [row_to_paper_response(row).model_dump() for row in recent_rows],
            "next_up": [row_to_paper_response(row).model_dump() for row in next_rows],
            "needs_attention": [
                {
                    **row_to_paper_response(row).model_dump(),
                    "attention_reasons": _attention_reasons(row),
                }
                for row in needs_attention_rows
            ],
            "needs_attention_count": needs_attention_count,
            "health": health,
            "cleanup_guidance": cleanup_guidance,
            "structure": {
                "top_collections": [{"name": row["name"], "count": int(row["count"] or 0)} for row in top_collection_rows],
                "top_tags": [{"name": row["name"], "count": int(row["count"] or 0)} for row in top_tag_rows],
                "top_topics": [{"term": row["term"], "count": int(row["count"] or 0)} for row in top_topic_rows],
            },
        }
    except Exception as e:
        raise_internal("Failed to compute library workflow summary", e)
