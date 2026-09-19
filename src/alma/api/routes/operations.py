"""Operational API endpoints for bulk fetch/update actions."""

import hashlib
import logging
import sqlite3
import uuid
from datetime import datetime
from pathlib import Path
from types import SimpleNamespace

from fastapi import APIRouter, Depends, HTTPException

from alma.api.deps import (  # internal helpers for path resolution
    _data_dir,
    _db_path,
    get_current_user,
    open_db_connection,
)
from alma.api.models import SavePublicationsRequest
from alma.api.scheduler import (
    activity_envelope,
    add_job_log,
    find_active_job,
    is_cancellation_requested,
    list_jobs,
    remove_job,
    run_job,
    schedule_immediate,
    set_job_status,
)
from alma.config import get_fetch_year
from alma.core.backend import _settings as _fb_settings
from alma.core.backend import fetch_publications_by_id
from alma.core.utils import derive_source_id

logger = logging.getLogger(__name__)

router = APIRouter(
    prefix="/fetch",
    tags=["fetch"],
    dependencies=[Depends(get_current_user)],
    responses={
        401: {"description": "Unauthorized"},
        500: {"description": "Internal Server Error"},
    },
)


def _existing_author_source_ids(db: sqlite3.Connection, author_id: str) -> set[str]:
    """Return the set of source-ids already linked to a given author_id.

    Identity keys in priority order: DOI → title. Used to exclude already-known
    works from the bulk preview list.
    """
    try:
        ex_rows = db.execute(
            """SELECT COALESCE(NULLIF(p.doi, ''), '') AS sid, p.title, p.url
               FROM papers p
               JOIN publication_authors pa ON pa.paper_id = p.id
               WHERE pa.author_id = ?""",
            (author_id,),
        ).fetchall()
    except Exception:
        return set()
    existing: set[str] = set()
    for er in ex_rows:
        sid = (er["sid"] or "").strip()
        if sid:
            existing.add(sid)
        else:
            existing.add((er["title"] or "").strip())
    return existing


def _preview_row_for(author_id: str, p: dict) -> dict:
    """Project a scholar-backend publication dict into the preview JSON shape."""
    citations = p.get("num_citations") if p.get("num_citations") is not None else p.get("citations", 0)
    return {
        "author_id": author_id,
        "title": p.get("title") or "",
        "authors": p.get("authors") or "",
        "year": p.get("year"),
        "abstract": p.get("abstract") or p.get("summary"),
        "url": p.get("pub_url") or p.get("url"),
        "citations": citations,
        "journal": p.get("journal"),
        "doi": p.get("doi"),
    }


@router.post("/preview", summary="Fetch & preview for all authors (Activity-backed)")
def fetch_preview_all(
    user: dict = Depends(get_current_user),
):
    """Queue a background preview fetch across every author.

    The previous implementation iterated authors synchronously in the request
    thread and held an anyio threadpool worker for the full duration of the
    remote round-trips (often many seconds). Now runs on the APS scheduler pool
    and reports progress per author; result JSON contains the combined preview.
    """
    operation_key = "fetch.preview_all"
    existing = find_active_job(operation_key)
    if existing:
        return activity_envelope(
            str(existing.get("job_id") or ""),
            status="already_running",
            operation_key=operation_key,
            message="Preview-all already running",
        )

    job_id = f"preview_all_{uuid.uuid4().hex[:10]}"
    set_job_status(
        job_id,
        status="queued",
        operation_key=operation_key,
        trigger_source="user",
        started_at=datetime.now().isoformat(),
        message="Queued bulk preview fetch",
    )

    def _runner():
        try:
            from_year = get_fetch_year()
            conn = open_db_connection()
            try:
                rows = conn.execute("SELECT id FROM authors").fetchall()
                total = len(rows)
                if total == 0:
                    set_job_status(
                        job_id,
                        status="completed",
                        finished_at=datetime.now().isoformat(),
                        processed=0,
                        total=0,
                        message="No authors to preview",
                        result={"total": 0, "items": []},
                    )
                    return

                result: list[dict] = []
                processed = 0
                for r in rows:
                    if is_cancellation_requested(job_id):
                        set_job_status(
                            job_id,
                            status="cancelled",
                            finished_at=datetime.now().isoformat(),
                            processed=processed,
                            total=total,
                            message="Preview-all cancelled",
                            result={"items": result, "cancelled": True},
                        )
                        add_job_log(
                            job_id,
                            f"Cancellation acknowledged at {processed}/{total}",
                            step="cancelled",
                        )
                        return

                    author_id = r["id"]
                    existing_source_ids = _existing_author_source_ids(conn, author_id)
                    pubs = fetch_publications_by_id(
                        author_id,
                        output_folder=_data_dir(),
                        args=SimpleNamespace(update_cache=False, test_fetching=False),
                        from_year=from_year,
                    ) or []
                    for p in pubs:
                        if derive_source_id(p) in existing_source_ids:
                            continue
                        result.append(_preview_row_for(author_id, p))
                    processed += 1
                    set_job_status(
                        job_id,
                        status="running",
                        processed=processed,
                        total=total,
                        current_author=author_id,
                        message=f"Previewed {processed}/{total} authors",
                    )
            finally:
                conn.close()

            set_job_status(
                job_id,
                status="completed",
                finished_at=datetime.now().isoformat(),
                processed=total,
                total=total,
                message=f"Preview-all completed ({len(result)} new)",
                result={"total": len(result), "items": result},
            )
        except Exception as exc:  # pragma: no cover - runner crash path
            logger.error("Preview-all runner failed: %s", exc)
            set_job_status(
                job_id,
                status="failed",
                finished_at=datetime.now().isoformat(),
                message="Preview-all failed",
                error=str(exc),
            )

    schedule_immediate(job_id, _runner)
    return activity_envelope(
        job_id,
        status="queued",
        operation_key=operation_key,
        message="Queued bulk preview fetch",
    )


@router.post("/preview/save", summary="Save preview publications (bulk) to DB")
def save_preview_publications_bulk(
    req: SavePublicationsRequest,
    user: dict = Depends(get_current_user),
):
    """Persist selected preview publications as a background operation."""
    items = req.items or []
    if not items:
        return {"success": True, "saved": 0, "status": "noop"}

    fingerprint_parts = []
    for it in items[:300]:
        fingerprint_parts.append(
            f"{it.author_id}|{it.title}|{it.year or ''}|{getattr(it, 'doi', None) or ''}"
        )
    fingerprint = hashlib.sha1("|".join(fingerprint_parts).encode("utf-8")).hexdigest()[:12]
    operation_key = f"fetch.preview_save:{fingerprint}:{len(items)}"
    existing = find_active_job(operation_key)
    if existing:
        return activity_envelope(
            str(existing.get("job_id") or ""),
            status="already_running",
            operation_key=operation_key,
            message="Preview save is already running for this payload",
            total=len(items),
        )

    job_id = f"preview_save_{uuid.uuid4().hex[:10]}"
    set_job_status(
        job_id,
        status="queued",
        operation_key=operation_key,
        trigger_source="user",
        started_at=datetime.now().isoformat(),
        processed=0,
        total=len(items),
        message="Queued preview save operation",
    )

    def _runner():
        from collections import defaultdict
        from pathlib import Path as _Path

        from alma.openalex.client import upsert_papers as _upsert

        try:
            groups = defaultdict(list)
            for it in items:
                groups[it.author_id].append(it)

            total_saved = 0
            author_total = len(groups)
            author_processed = 0
            for author_id, lst in groups.items():
                if is_cancellation_requested(job_id):
                    summary = {
                        "success": False,
                        "saved": total_saved,
                        "cancelled": True,
                        "processed_authors": author_processed,
                        "total_authors": author_total,
                    }
                    add_job_log(job_id, "Preview save cancelled by user", step="cancelled", data=summary)
                    set_job_status(
                        job_id,
                        status="cancelled",
                        finished_at=datetime.now().isoformat(),
                        message="Preview save cancelled",
                        processed=author_processed,
                        total=author_total,
                        result=summary,
                    )
                    return

                works = []
                for it in lst:
                    works.append({
                        "title": it.title,
                        "authors": it.authors or "",
                        "abstract": it.abstract or "",
                        "year": it.year,
                        "pub_url": it.url or "",
                        "doi": getattr(it, "doi", None) or "",
                        "num_citations": it.citations or 0,
                        "journal": it.journal or "",
                    })

                # Note: author_id association handled via publication_authors (from authorships)
                total_saved += _upsert(works, db_path=_Path(_db_path()))
                author_processed += 1
                set_job_status(
                    job_id,
                    status="running",
                    processed=author_processed,
                    total=author_total,
                    current_author=author_id,
                    message="Saving preview publications",
                )
                if author_processed % 10 == 0 or author_processed == author_total:
                    add_job_log(
                        job_id,
                        f"Preview save progress {author_processed}/{author_total} (saved={total_saved})",
                        step="progress",
                    )

            summary = {"success": True, "saved": total_saved, "authors": author_total}
            set_job_status(
                job_id,
                status="completed",
                finished_at=datetime.now().isoformat(),
                processed=author_total,
                total=author_total,
                message="Preview save completed",
                result=summary,
            )
        except Exception as e:
            logger.error("Error saving preview publications (bulk): %s", e)
            set_job_status(
                job_id,
                status="failed",
                finished_at=datetime.now().isoformat(),
                message="Preview save failed",
                error=str(e),
            )

    schedule_immediate(job_id, _runner)
    return activity_envelope(
        job_id,
        status="queued",
        operation_key=operation_key,
        message="Queued preview save operation",
        total=len(items),
    )


@router.post("/hard-reset", summary="Hard reset publications database and refetch all authors")
def hard_reset_publications_db(user: dict = Depends(get_current_user)):
    """Schedule a background hard reset and return a job id for progress polling."""
    job_id = f"hard_reset_{uuid.uuid4().hex[:10]}"
    set_job_status(
        job_id,
        status="queued",
        operation_key="fetch.hard_reset_publications",
        trigger_source="user",
        started_at=datetime.now().isoformat(),
        message="Starting hard reset",
    )

    def _runner():
        try:
            from alma.config import get_db_path as _get_db_path
            pub_path = Path(str(_get_db_path()))
            pub_path.parent.mkdir(parents=True, exist_ok=True)

            # Backup existing
            if pub_path.exists():
                ts = datetime.now().strftime("%Y%m%d_%H%M%S")
                backup = pub_path.with_name(f"{pub_path.name}.{ts}.bak")
                pub_path.rename(backup)
                logger.info("Backed up publications DB to %s", str(backup))

            # Fresh DB
            with sqlite3.connect(str(pub_path)) as conn:
                conn.execute(
                    """CREATE TABLE IF NOT EXISTS publications (
                        author_id TEXT,
                        title TEXT,
                        source_id TEXT,
                        year INTEGER,
                        abstract TEXT,
                        url TEXT,
                        doi TEXT,
                        citations INTEGER,
                        journal TEXT,
                        authors TEXT,
                        PRIMARY KEY (author_id, title, source_id)
                    )"""
                )
                conn.commit()

            # Settings window
            from_year = get_fetch_year()
            cfg = _fb_settings()
            backend = (cfg.get("backend") or "openalex").lower()

            # Iterate authors
            adb = open_db_connection()
            try:
                rows = adb.execute("SELECT id, name FROM authors").fetchall()
            finally:
                adb.close()
            total = len(rows)
            processed = 0
            total_pubs = 0
            for r in rows:
                author_id = r["id"]
                author_name = r["name"]
                set_job_status(job_id, status="running", processed=processed, total=total, current_author=author_name)
                pubs = fetch_publications_by_id(
                    author_id,
                    output_folder=str(pub_path.parent),
                    args=SimpleNamespace(update_cache=True, test_fetching=False),
                    from_year=from_year,
                ) or []
                total_pubs += len(pubs)
                processed += 1
                set_job_status(job_id, status="running", processed=processed, total=total, current_author=author_name)

            set_job_status(job_id, status="completed", finished_at=datetime.now().isoformat(), result={
                "success": True,
                "authors": total,
                "publications": total_pubs,
                "from_year": from_year,
                "backend": backend,
            })
        except Exception as e:  # pragma: no cover
            logger.error("Hard reset runner failed: %s", e)
            set_job_status(job_id, status="failed", finished_at=datetime.now().isoformat(), error=str(e))

    schedule_immediate(job_id, _runner)
    return activity_envelope(
        job_id,
        status="queued",
        operation_key="fetch.hard_reset_publications",
        message="Hard reset queued",
    )


@router.get("/jobs", summary="List scheduled jobs")
def list_scheduled_jobs():
    return list_jobs()


@router.delete("/jobs/{job_id}", summary="Delete a scheduled job")
def delete_job(job_id: str):
    if not remove_job(job_id):
        raise HTTPException(status_code=404, detail="Job not found")
    return {"success": True, "job_id": job_id}


@router.post("/jobs/{job_id}/run", summary="Run job immediately")
def run_job_now(job_id: str):
    if not run_job(job_id):
        raise HTTPException(status_code=404, detail="Job not found or execution failed")
    return {"success": True, "job_id": job_id}
