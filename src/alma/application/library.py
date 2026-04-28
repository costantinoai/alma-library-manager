"""Library use-case layer: paper CRUD, collections, tags.

All SQL queries against the v3 papers/collections/tags tables live here.
Routes are thin transport callers to these functions.
"""

import json
import logging
import sqlite3
import uuid
from datetime import datetime
from typing import Optional

from alma.core.utils import normalize_doi, resolve_existing_paper_id

logger = logging.getLogger(__name__)

TRACKED_STATUS = "tracked"
LIBRARY_STATUS = "library"
DISMISSED_STATUS = "dismissed"
REMOVED_STATUS = "removed"

DEFAULT_LIBRARY_RATING = 3
DISLIKE_RATING = 1

# Identifiers that must never be stored as the empty string. The partial
# UNIQUE index on `papers.openalex_id` (WHERE openalex_id IS NOT NULL)
# treats "" as a value and a second blank insert collides. Same hazard
# for `doi` (case-insensitive lookup) and `semantic_scholar_id`.
_BLANKABLE_IDENTIFIERS = (
    "openalex_id",
    "doi",
    "semantic_scholar_id",
    "semantic_scholar_corpus_id",
)


def _normalize_paper_identifiers(kwargs: dict) -> None:
    """Coerce blank identifier kwargs to None and bare-form the DOI in place."""
    for key in _BLANKABLE_IDENTIFIERS:
        if key not in kwargs:
            continue
        val = kwargs[key]
        if val is None:
            continue
        stripped = str(val).strip()
        kwargs[key] = stripped or None
    if kwargs.get("doi"):
        kwargs["doi"] = normalize_doi(kwargs["doi"])


def _needs_enrichment(db: sqlite3.Connection, paper_id: str) -> bool:
    """True when a paper has not yet been resolved against OpenAlex.

    A paper needs enrichment when its `openalex_resolution_status` is
    unset or still pending AND it has no `publication_topics` rows —
    the two signals together catch both freshly-imported rows (no
    resolution attempt yet) and partially-enriched rows that lost
    topic/institution data.
    """
    try:
        row = db.execute(
            "SELECT openalex_resolution_status FROM papers WHERE id = ?",
            (paper_id,),
        ).fetchone()
    except sqlite3.OperationalError:
        return False
    if row is None:
        return False
    status = str((row["openalex_resolution_status"] if "openalex_resolution_status" in row.keys() else "") or "").strip().lower()
    if status == "openalex_resolved":
        return False
    try:
        topic_row = db.execute(
            "SELECT 1 FROM publication_topics WHERE paper_id = ? LIMIT 1",
            (paper_id,),
        ).fetchone()
    except sqlite3.OperationalError:
        topic_row = None
    return topic_row is None


def _schedule_paper_enrichment(paper_id: str) -> None:
    """Queue an Activity-backed OpenAlex enrichment job for one paper.

    Dedupes via ``find_active_job`` so re-saving a paper doesn't enqueue
    the same job twice. Silently no-ops when the scheduler isn't
    available (unit-test contexts or subprocesses that don't import
    ``alma.api.scheduler``). The job opens its own DB connection inside
    the runner to avoid sharing the caller's transaction with the
    scheduler thread.
    """
    # Under pytest ``schedule_immediate`` runs the job inline, which would
    # make ``add_to_library`` call the real OpenAlex enrichment network
    # path for every save. Tests that care about enrichment must invoke
    # ``enrich_publication`` explicitly (with mocks) — everyone else gets
    # the fast library mutation without side effects.
    import os
    if os.environ.get("PYTEST_CURRENT_TEST"):
        return
    try:
        from alma.api.deps import open_db_connection
        from alma.api.scheduler import (
            find_active_job,
            schedule_immediate,
            set_job_status,
        )
        from alma.library.enrichment import enrich_publication
    except Exception:
        return

    operation_key = f"library.enrich:{paper_id}"
    try:
        if find_active_job(operation_key):
            return
    except Exception:
        return

    job_id = f"library_enrich_{uuid.uuid4().hex[:10]}"
    now = datetime.utcnow().isoformat()
    try:
        set_job_status(
            job_id,
            status="queued",
            operation_key=operation_key,
            trigger_source="library.add",
            started_at=now,
            message=f"Enriching paper {paper_id[:8]} via OpenAlex",
            processed=0,
            total=1,
        )
    except Exception:
        return

    def _runner():
        conn = None
        try:
            conn = open_db_connection()
            summary = enrich_publication(paper_id, conn)
            conn.commit()
            set_job_status(
                job_id,
                status="completed",
                finished_at=datetime.utcnow().isoformat(),
                processed=1,
                total=1,
                message=(
                    "Enrichment complete"
                    if summary.get("enriched")
                    else f"Enrichment skipped: {summary.get('reason', 'unknown')}"
                ),
                result=summary,
            )
        except Exception as exc:
            logger.warning("Library enrichment runner failed for %s: %s", paper_id, exc)
            try:
                set_job_status(
                    job_id,
                    status="failed",
                    finished_at=datetime.utcnow().isoformat(),
                    message="Library enrichment failed",
                    error=str(exc),
                )
            except Exception:
                pass
        finally:
            if conn is not None:
                try:
                    conn.close()
                except Exception:
                    pass

    try:
        schedule_immediate(job_id, _runner)
    except Exception as exc:
        logger.debug("schedule_immediate for library.enrich failed: %s", exc)


def _table_exists(db: sqlite3.Connection, table: str) -> bool:
    row = db.execute(
        "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?",
        (table,),
    ).fetchone()
    return row is not None


# ============================================================================
# Paper CRUD
# ============================================================================

def get_paper(db: sqlite3.Connection, paper_id: str) -> Optional[dict]:
    """Get a paper by ID."""
    row = db.execute("SELECT * FROM papers WHERE id = ?", (paper_id,)).fetchone()
    return dict(row) if row else None


def get_paper_by_openalex_id(db: sqlite3.Connection, openalex_id: str) -> Optional[dict]:
    """Get a paper by OpenAlex ID."""
    row = db.execute(
        "SELECT * FROM papers WHERE openalex_id = ?", (openalex_id,)
    ).fetchone()
    return dict(row) if row else None


def get_paper_by_doi(db: sqlite3.Connection, doi: str) -> Optional[dict]:
    """Get a paper by DOI."""
    row = db.execute("SELECT * FROM papers WHERE doi = ?", (doi,)).fetchone()
    return dict(row) if row else None


def get_paper_by_semantic_scholar_id(db: sqlite3.Connection, semantic_scholar_id: str) -> Optional[dict]:
    """Get a paper by Semantic Scholar paperId."""
    try:
        row = db.execute(
            "SELECT * FROM papers WHERE semantic_scholar_id = ?",
            (semantic_scholar_id,),
        ).fetchone()
    except sqlite3.OperationalError:
        return None
    return dict(row) if row else None


def find_paper_by_title(db: sqlite3.Connection, title: str) -> Optional[dict]:
    """Find a paper by exact title match."""
    row = db.execute("SELECT * FROM papers WHERE title = ?", (title,)).fetchone()
    return dict(row) if row else None


def create_paper(db: sqlite3.Connection, **kwargs) -> str:
    """Insert a new paper. Returns the paper ID.

    Required: title
    Optional: all other paper fields
    """
    _normalize_paper_identifiers(kwargs)
    paper_id = kwargs.pop("id", None) or str(uuid.uuid4())
    kwargs.setdefault("status", TRACKED_STATUS)
    kwargs.setdefault("created_at", datetime.utcnow().isoformat())
    kwargs.setdefault("updated_at", datetime.utcnow().isoformat())

    # Serialize JSON fields
    for json_field in ("keywords", "sdgs", "counts_by_year"):
        val = kwargs.get(json_field)
        if val is not None and not isinstance(val, str):
            kwargs[json_field] = json.dumps(val)

    columns = ["id"] + list(kwargs.keys())
    placeholders = ", ".join(["?"] * len(columns))
    col_str = ", ".join(columns)
    values = [paper_id] + list(kwargs.values())

    db.execute(f"INSERT INTO papers ({col_str}) VALUES ({placeholders})", values)
    return paper_id


def upsert_paper(db: sqlite3.Connection, **kwargs) -> str:
    """Insert or update a paper. Deduplicates via the canonical triple
    (openalex_id → doi → year+normalized_title) plus a semantic_scholar_id
    side-channel and a year-less title fallback.

    Returns the paper ID (existing or new).
    """
    _normalize_paper_identifiers(kwargs)

    # Canonical triple lookup (see lessons → "Canonical paper-dedup helper
    # lives in core.utils"). resolve_existing_paper_id refuses a title-only
    # match when year is missing, so we do a soft title fallback below.
    title = kwargs.get("title")
    year = kwargs.get("year")
    paper_id = resolve_existing_paper_id(
        db,
        openalex_id=kwargs.get("openalex_id"),
        doi=kwargs.get("doi"),
        title=title,
        year=year,
    )
    existing = get_paper(db, paper_id) if paper_id else None

    # S2 paperId is a fourth identifier outside the canonical triple.
    if existing is None and kwargs.get("semantic_scholar_id"):
        existing = get_paper_by_semantic_scholar_id(db, kwargs["semantic_scholar_id"])

    # Title-only fallback for year-less callers (importer paths). Per the
    # canonical-helper lesson this runs *after* the core helper, never instead.
    if existing is None and year is None and title:
        existing = find_paper_by_title(db, title)

    if existing:
        paper_id = existing["id"]
        # Update non-null fields (don't overwrite existing data with None)
        updates = {}
        for key, val in kwargs.items():
            if val is not None and key not in ("id", "created_at"):
                # External upserts must not undo explicit membership decisions.
                if key == "status" and str(existing.get("status") or "").strip().lower() in {
                    LIBRARY_STATUS,
                    DISMISSED_STATUS,
                    REMOVED_STATUS,
                }:
                    continue
                # Serialize JSON fields
                if key in ("keywords", "sdgs", "counts_by_year") and not isinstance(val, str):
                    val = json.dumps(val)
                updates[key] = val

        if updates:
            if _should_invalidate_embedding(existing, updates):
                _invalidate_embedding_artifacts(db, paper_id)
            updates["updated_at"] = datetime.utcnow().isoformat()
            set_clause = ", ".join(f"{k} = ?" for k in updates.keys())
            db.execute(
                f"UPDATE papers SET {set_clause} WHERE id = ?",
                list(updates.values()) + [paper_id],
            )
        return paper_id
    else:
        return create_paper(db, **kwargs)


def update_paper(db: sqlite3.Connection, paper_id: str, **kwargs) -> bool:
    """Update specific fields of a paper. Returns True if found."""
    if not kwargs:
        return False

    existing = get_paper(db, paper_id)
    if existing is None:
        return False

    # Serialize JSON fields
    for json_field in ("keywords", "sdgs", "counts_by_year"):
        val = kwargs.get(json_field)
        if val is not None and not isinstance(val, str):
            kwargs[json_field] = json.dumps(val)

    if _should_invalidate_embedding(existing, kwargs):
        _invalidate_embedding_artifacts(db, paper_id)

    kwargs["updated_at"] = datetime.utcnow().isoformat()
    set_clause = ", ".join(f"{k} = ?" for k in kwargs.keys())
    cursor = db.execute(
        f"UPDATE papers SET {set_clause} WHERE id = ?",
        list(kwargs.values()) + [paper_id],
    )
    return cursor.rowcount > 0


def delete_paper(db: sqlite3.Connection, paper_id: str) -> bool:
    """Soft-remove a paper by ID. Returns True if found."""
    if get_paper(db, paper_id) is None:
        return False
    return soft_remove_from_library(db, paper_id)


def _should_invalidate_embedding(existing: dict, updates: dict) -> bool:
    text_fields = ("title", "abstract")
    for field in text_fields:
        if field not in updates:
            continue
        old_val = str(existing.get(field) or "").strip()
        new_val = str(updates.get(field) or "").strip()
        if old_val != new_val:
            return True
    return False


def _invalidate_embedding_artifacts(db: sqlite3.Connection, paper_id: str) -> None:
    """Clear embedding-derived artifacts when source text changes."""
    try:
        db.execute("DELETE FROM publication_embeddings WHERE paper_id = ?", (paper_id,))
    except sqlite3.OperationalError:
        pass
    try:
        db.execute("DELETE FROM publication_clusters WHERE paper_id = ?", (paper_id,))
    except sqlite3.OperationalError:
        pass


def add_to_library(
    db: sqlite3.Connection,
    paper_id: str,
    rating: int = DEFAULT_LIBRARY_RATING,
    notes: Optional[str] = None,
    added_from: str = "manual",
    default_reading_status: Optional[str] = None,
    override_added_from: bool = False,
) -> bool:
    """Move a paper to library status.

    ``added_from`` is preserved when the row already has a non-empty value, so
    callers don't accidentally clobber earlier provenance (e.g. a feed-tracked
    paper saved via a UI button should stay `feed`). Set ``override_added_from``
    when the current call represents a stronger user signal and should win —
    the canonical case is a BibTeX/Zotero import promoting a row that was
    previously auto-tracked from feed or discovery.
    """
    now = datetime.utcnow().isoformat()
    added_from_clause = (
        "added_from = ?"
        if override_added_from
        else "added_from = CASE WHEN COALESCE(TRIM(added_from), '') = '' THEN ? ELSE added_from END"
    )
    updates = [
        f"status = '{LIBRARY_STATUS}'",
        "rating = CASE WHEN COALESCE(rating, 0) > ? THEN rating ELSE ? END",
        "notes = COALESCE(?, notes)",
        "added_at = COALESCE(added_at, ?)",
        added_from_clause,
        """reading_status = CASE
               WHEN ? IS NOT NULL AND (reading_status IS NULL OR TRIM(reading_status) = '') THEN ?
               ELSE reading_status
           END""",
        "updated_at = ?",
    ]
    params = [rating, rating, notes, now, added_from, default_reading_status, default_reading_status, now]
    cursor = db.execute(
        f"UPDATE papers SET {', '.join(updates)} WHERE id = ?",
        (*params, paper_id),
    )
    if cursor.rowcount > 0 and _needs_enrichment(db, paper_id):
        # Commit the library-state change before scheduling so the
        # enrichment job's independent connection sees the latest row.
        if db.in_transaction:
            db.commit()
        _schedule_paper_enrichment(paper_id)
    return cursor.rowcount > 0


def dismiss_paper(db: sqlite3.Connection, paper_id: str) -> bool:
    """Dismiss a paper (hide from feed/discovery, not in library)."""
    now = datetime.utcnow().isoformat()
    cursor = db.execute(
        "UPDATE papers SET status = ?, rating = ?, updated_at = ? WHERE id = ?",
        (DISMISSED_STATUS, DISLIKE_RATING, now, paper_id),
    )
    if cursor.rowcount > 0:
        # Cascade GC: this paper was the only "live" reason some of
        # its co-authors stayed `status='active'`. The cascade soft-
        # removes any author who has no other live attachment and
        # isn't followed. Audited via operation_logs.
        from alma.application.author_lifecycle import cascade_gc_for_paper

        cascade_gc_for_paper(db, paper_id, reason=f"paper {paper_id} dismissed")
    return cursor.rowcount > 0


def sink_disliked_paper(db: sqlite3.Connection, paper_id: str) -> bool:
    """Store a paper in the dislike sink without removing it from Feed history.

    Feed dislikes are signals only: keep membership untouched and set rating 1.
    """
    now = datetime.utcnow().isoformat()
    cursor = db.execute(
        """
        UPDATE papers
        SET rating = ?,
            updated_at = ?
        WHERE id = ?
        """,
        (DISLIKE_RATING, now, paper_id),
    )
    return cursor.rowcount > 0


def soft_remove_from_library(db: sqlite3.Connection, paper_id: str) -> bool:
    """Remove from Library without deleting provenance."""
    now = datetime.utcnow().isoformat()
    cursor = db.execute(
        """
        UPDATE papers
        SET status = ?,
            rating = ?,
            updated_at = ?
        WHERE id = ?
        """,
        (REMOVED_STATUS, DISLIKE_RATING, now, paper_id),
    )
    if cursor.rowcount:
        record_paper_feedback(
            db,
            paper_id,
            action="remove",
            rating=DISLIKE_RATING,
            source_surface="library",
        )
        # Same cascade logic as `dismiss_paper`: any co-author whose
        # only live attachment was this paper is now orphan and
        # gets soft-removed. Soft, audited, reversible.
        from alma.application.author_lifecycle import cascade_gc_for_paper

        cascade_gc_for_paper(db, paper_id, reason=f"paper {paper_id} removed from library")
    return cursor.rowcount > 0


def rate_paper(db: sqlite3.Connection, paper_id: str, rating: int) -> bool:
    """Set rating on a paper."""
    now = datetime.utcnow().isoformat()
    cursor = db.execute(
        "UPDATE papers SET rating = ?, updated_at = ? WHERE id = ?",
        (rating, now, paper_id),
    )
    return cursor.rowcount > 0


def rating_signal_value(rating: int | None) -> int:
    """Map star rating to ranking signal."""
    value = int(rating or 0)
    if value >= 5:
        return 2
    if value >= 4:
        return 1
    if value in {1, 2}:
        return -1
    return 0


def record_paper_feedback(
    db: sqlite3.Connection,
    paper_id: str,
    *,
    action: str,
    rating: int | None = None,
    source_surface: str,
) -> None:
    """Record a paper feedback event through the shared feedback table."""
    if not _table_exists(db, "feedback_events"):
        return
    event_id = uuid.uuid4().hex
    value = {
        "action": action,
        "rating": rating,
        "signal_value": rating_signal_value(rating),
    }
    context = {
        "surface": source_surface,
        "paper_id": paper_id,
        "acted_at": datetime.utcnow().isoformat(),
    }
    db.execute(
        """INSERT INTO feedback_events
           (id, event_type, entity_type, entity_id, value, context_json)
           VALUES (?, ?, ?, ?, ?, ?)""",
        (
            event_id,
            "paper_action",
            "publication",
            paper_id,
            json.dumps(value),
            json.dumps(context),
        ),
    )


def sync_surface_resolution(
    db: sqlite3.Connection,
    paper_id: str,
    *,
    action: str,
    source_surface: str,
) -> None:
    """Resolve matching feed and discovery rows for one paper after a user action.

    This keeps Feed, Discovery, and Library aligned so the same paper does not
    remain actionable on one surface after being accepted or dismissed on another.
    """
    if not paper_id:
        return

    now = datetime.utcnow().isoformat()
    recommendation_action = {
        "add": "save",
        "save": "save",
        "like": "like",
        "love": "like",
        "dismiss": "dismiss",
        "dislike": "dismiss",
    }.get(action)
    feed_status = {
        "add": "add",
        "save": "add",
        "like": "like",
        "love": "love",
        "dismiss": "dislike",
        "dislike": "dislike",
    }.get(action)

    if feed_status and _table_exists(db, "feed_items"):
        db.execute(
            """
            UPDATE feed_items
            SET status = ?
            WHERE paper_id = ?
              AND COALESCE(status, 'new') = 'new'
            """,
            (feed_status, paper_id),
        )

    if recommendation_action and _table_exists(db, "recommendations"):
        rec_rows = db.execute(
            """
            SELECT id, lens_id
            FROM recommendations
            WHERE paper_id = ?
              AND (user_action IS NULL OR TRIM(user_action) = '')
            """,
            (paper_id,),
        ).fetchall()
        if rec_rows:
            db.execute(
                """
                UPDATE recommendations
                SET user_action = ?, action_at = ?
                WHERE paper_id = ?
                  AND (user_action IS NULL OR TRIM(user_action) = '')
                """,
                (recommendation_action, now, paper_id),
            )

            signal_value = -1 if recommendation_action == "dismiss" else 1
            if action == "love":
                signal_value = 2
            if source_surface != "discovery":
                try:
                    from alma.application.discovery import record_lens_signal

                    seen_lenses: set[str] = set()
                    for row in rec_rows:
                        lens_id = str(row["lens_id"] or "").strip()
                        if not lens_id or lens_id in seen_lenses:
                            continue
                        seen_lenses.add(lens_id)
                        record_lens_signal(
                            db,
                            lens_id=lens_id,
                            paper_id=paper_id,
                            signal_value=signal_value,
                            source=f"{source_surface}_handoff",
                        )
                except Exception:
                    pass


def list_papers(
    db: sqlite3.Connection,
    *,
    status: Optional[str] = None,
    search: Optional[str] = None,
    year: Optional[int] = None,
    min_year: Optional[int] = None,
    max_year: Optional[int] = None,
    min_citations: Optional[int] = None,
    order: str = "citations",
    limit: int = 100,
    offset: int = 0,
) -> list[dict]:
    """Query papers with filters."""
    parts = ["SELECT * FROM papers WHERE 1=1"]
    params: list = []

    if status:
        parts.append("AND status = ?")
        params.append(status)

    if search:
        parts.append("AND (title LIKE ? OR abstract LIKE ?)")
        pattern = f"%{search}%"
        params.extend([pattern, pattern])

    if year:
        parts.append("AND year = ?")
        params.append(year)
    if min_year:
        parts.append("AND year >= ?")
        params.append(min_year)
    if max_year:
        parts.append("AND year <= ?")
        params.append(max_year)
    if min_citations is not None:
        parts.append("AND cited_by_count >= ?")
        params.append(min_citations)

    ord_map = {
        "citations": "cited_by_count DESC, year DESC",
        "recent": "year DESC, cited_by_count DESC",
        "title": "title COLLATE NOCASE ASC",
        "added": "added_at DESC",
        "rating": "rating DESC, added_at DESC",
    }
    parts.append(f"ORDER BY {ord_map.get(order, ord_map['citations'])}")
    parts.append("LIMIT ? OFFSET ?")
    params.extend([limit, offset])

    rows = db.execute(" ".join(parts), params).fetchall()
    return [dict(r) for r in rows]


def get_library_papers(db: sqlite3.Connection, **kwargs) -> list[dict]:
    """Get papers in the library."""
    return list_papers(db, status="library", **kwargs)


def get_favorites(db: sqlite3.Connection, limit: int = 100) -> list[dict]:
    """Get favorite papers (rating >= 4)."""
    rows = db.execute(
        "SELECT * FROM papers WHERE status = 'library' AND rating >= 4 ORDER BY rating DESC, added_at DESC LIMIT ?",
        (limit,),
    ).fetchall()
    return [dict(r) for r in rows]


# ============================================================================
# Collections
# ============================================================================

def create_collection(
    db: sqlite3.Connection, name: str, description: Optional[str] = None, color: str = "#3B82F6"
) -> str:
    """Create a collection. Returns the collection ID."""
    coll_id = str(uuid.uuid4())
    now = datetime.utcnow().isoformat()
    db.execute(
        "INSERT INTO collections (id, name, description, color, created_at) VALUES (?, ?, ?, ?, ?)",
        (coll_id, name, description, color, now),
    )
    return coll_id


def list_collections(db: sqlite3.Connection) -> list[dict]:
    """List all collections with item counts."""
    rows = db.execute(
        """SELECT c.*, COUNT(ci.paper_id) AS item_count
           FROM collections c
           LEFT JOIN collection_items ci ON ci.collection_id = c.id
           GROUP BY c.id
           ORDER BY c.name"""
    ).fetchall()
    return [dict(r) for r in rows]


def add_to_collection(db: sqlite3.Connection, collection_id: str, paper_id: str) -> None:
    """Add a saved Library paper to a collection."""
    paper = get_paper(db, paper_id)
    if paper is None:
        raise ValueError("Paper not found")
    if str(paper.get("status") or "") != LIBRARY_STATUS:
        raise ValueError("Only saved Library papers can be added to collections")
    now = datetime.utcnow().isoformat()
    db.execute(
        "INSERT OR IGNORE INTO collection_items (collection_id, paper_id, added_at) VALUES (?, ?, ?)",
        (collection_id, paper_id, now),
    )


def remove_from_collection(db: sqlite3.Connection, collection_id: str, paper_id: str) -> bool:
    """Remove a paper from a collection."""
    cursor = db.execute(
        "DELETE FROM collection_items WHERE collection_id = ? AND paper_id = ?",
        (collection_id, paper_id),
    )
    return cursor.rowcount > 0


def get_collection_papers(db: sqlite3.Connection, collection_id: str) -> list[dict]:
    """Get saved Library papers in a collection."""
    rows = db.execute(
        """SELECT p.* FROM papers p
           JOIN collection_items ci ON ci.paper_id = p.id
           WHERE ci.collection_id = ?
             AND p.status = 'library'
           ORDER BY ci.added_at DESC""",
        (collection_id,),
    ).fetchall()
    return [dict(r) for r in rows]


def delete_collection(db: sqlite3.Connection, collection_id: str) -> bool:
    """Delete a collection."""
    cursor = db.execute("DELETE FROM collections WHERE id = ?", (collection_id,))
    return cursor.rowcount > 0


# ============================================================================
# Tags
# ============================================================================

def create_tag(db: sqlite3.Connection, name: str, color: str = "#6B7280") -> str:
    """Create a tag. Returns the tag ID."""
    tag_id = str(uuid.uuid4())
    db.execute("INSERT INTO tags (id, name, color) VALUES (?, ?, ?)", (tag_id, name, color))
    return tag_id


def list_tags(db: sqlite3.Connection) -> list[dict]:
    """List all tags."""
    rows = db.execute("SELECT * FROM tags ORDER BY name").fetchall()
    return [dict(r) for r in rows]


def tag_paper(db: sqlite3.Connection, paper_id: str, tag_id: str) -> None:
    """Tag a saved Library paper."""
    paper = get_paper(db, paper_id)
    if paper is None:
        raise ValueError("Paper not found")
    if str(paper.get("status") or "") != LIBRARY_STATUS:
        raise ValueError("Only saved Library papers can be tagged")
    db.execute(
        "INSERT OR IGNORE INTO publication_tags (paper_id, tag_id) VALUES (?, ?)",
        (paper_id, tag_id),
    )


def untag_paper(db: sqlite3.Connection, paper_id: str, tag_id: str) -> bool:
    """Remove a tag from a paper."""
    cursor = db.execute(
        "DELETE FROM publication_tags WHERE paper_id = ? AND tag_id = ?",
        (paper_id, tag_id),
    )
    return cursor.rowcount > 0


def get_paper_tags(db: sqlite3.Connection, paper_id: str) -> list[dict]:
    """Get all tags for a paper."""
    rows = db.execute(
        """SELECT t.* FROM tags t
           JOIN publication_tags pt ON pt.tag_id = t.id
           WHERE pt.paper_id = ?
           ORDER BY t.name""",
        (paper_id,),
    ).fetchall()
    return [dict(r) for r in rows]


def get_papers_by_tag(db: sqlite3.Connection, tag_id: str) -> list[dict]:
    """Get saved Library papers with a specific tag."""
    rows = db.execute(
        """SELECT p.* FROM papers p
           JOIN publication_tags pt ON pt.paper_id = p.id
           WHERE pt.tag_id = ?
             AND p.status = 'library'
           ORDER BY p.added_at DESC""",
        (tag_id,),
    ).fetchall()
    return [dict(r) for r in rows]


def delete_tag(db: sqlite3.Connection, tag_id: str) -> bool:
    """Delete a tag."""
    cursor = db.execute("DELETE FROM tags WHERE id = ?", (tag_id,))
    return cursor.rowcount > 0
