"""Operational API endpoints for bulk fetch/update actions."""

import hashlib
import logging
import sqlite3
import uuid
from datetime import datetime
from pathlib import Path
from types import SimpleNamespace

from fastapi import APIRouter, Depends, HTTPException, Query

from alma.api.deps import (  # internal helpers for path resolution
    _data_dir,
    _db_path,
    get_current_user,
    get_db,
    open_db_connection,
)
from alma.api.helpers import raise_internal
from alma.api.models import JobCreate, JobResponse, SavePublicationsRequest
from alma.api.scheduler import (
    activity_envelope,
    add_cron_job,
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


def do_refresh_cache_all(authors_db: sqlite3.Connection, job_id: str | None = None) -> dict:
    """Core function to refresh cache for all authors."""
    cursor = authors_db.execute("SELECT id, name FROM authors")
    authors = cursor.fetchall()
    if not authors:
        return {"success": True, "authors": 0, "refreshed": 0}

    total_refreshed = 0
    from_year = get_fetch_year()
    processed = 0
    failures: list[dict[str, str]] = []
    for row in authors:
        if job_id and is_cancellation_requested(job_id):
            set_job_status(
                job_id,
                status="cancelled",
                finished_at=datetime.now().isoformat(),
                message="Refresh cancelled by user",
                result={
                    "success": False,
                    "authors": len(authors),
                    "refreshed": total_refreshed,
                    "cancelled": True,
                    "processed": processed,
                },
            )
            add_job_log(
                job_id,
                f"Cancellation acknowledged at {processed}/{len(authors)} authors",
                step="cancelled",
            )
            return {
                "success": False,
                "authors": len(authors),
                "refreshed": total_refreshed,
                "cancelled": True,
                "processed": processed,
            }

        author_id = row["id"]
        author_name = row["name"]
        # One author must not take the whole sweep down. This runs unattended
        # (authors.refresh_periodic at 01:00), where a single bad row used to
        # abort every remaining author with one fatal error. Failures are LOUD
        # (stack trace in the log) and counted into the result, so Activity
        # shows "refreshed N, failed M" instead of a silent partial success.
        try:
            pubs = fetch_publications_by_id(
                author_id,
                output_folder=_data_dir(),
                args=SimpleNamespace(update_cache=True, test_fetching=False),
                from_year=from_year,
            )
            total_refreshed += len(pubs or [])
        except Exception:
            logger.exception("Cache refresh failed for author %s (%s)", author_name, author_id)
            failures.append({"author_id": author_id, "author_name": author_name})
        processed += 1
        if job_id:
            try:
                set_job_status(job_id, status="running", processed=processed, total=len(authors), current_author=author_name)
            except Exception:
                pass

    logger.info(
        "Refreshed cache for %d author(s), %d pubs total, %d failed",
        len(authors),
        total_refreshed,
        len(failures),
    )
    result = {
        # Truthful status: a sweep that skipped authors is NOT a clean success.
        "success": not failures,
        "authors": len(authors),
        "refreshed": total_refreshed,
    }
    if failures:
        result["failed"] = len(failures)
        result["failed_authors"] = [f["author_name"] for f in failures[:20]]
    return result


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


@router.post("/refresh-cache", summary="Refresh cache for all authors (no send)")
def refresh_cache_all(
    background: bool = Query(True, description="Run in background and track in Activity"),
    db: sqlite3.Connection = Depends(get_db),
    user: dict = Depends(get_current_user),
):
    """Refresh cached publications for all authors without sending notifications."""
    operation_key = "fetch.refresh_cache_all"
    existing = find_active_job(operation_key)
    if existing:
        return activity_envelope(
            str(existing.get("job_id") or ""),
            status="already_running",
            operation_key=operation_key,
            message="Bulk refresh is already running",
        )

    if not background:
        try:
            return do_refresh_cache_all(db)
        except Exception as e:
            raise_internal("Failed to refresh cache for all authors", e)

    job_id = f"refresh_all_{uuid.uuid4().hex[:10]}"
    set_job_status(
        job_id,
        status="queued",
        operation_key=operation_key,
        trigger_source="user",
        started_at=datetime.now().isoformat(),
        message="Refreshing cache for all authors",
    )

    def _runner():
        conn = open_db_connection()
        try:
            return do_refresh_cache_all(conn, job_id=job_id)
        finally:
            conn.close()

    schedule_immediate(job_id, _runner)
    return activity_envelope(
        job_id,
        status="queued",
        operation_key=operation_key,
        message="Queued bulk refresh",
    )


@router.post("/jobs", response_model=JobResponse, summary="Schedule a fetch job")
def schedule_job(payload: JobCreate):
    """Schedule a cron job for fetch operations."""
    try:
        # `fetch_and_notify` is gone with the plain-text digest pipeline it
        # scheduled (task 55). Alert delivery is the alerts engine's job now,
        # with its own schedules, rules and per-channel dedup.
        if payload.action == "fetch":
            def _job_refresh():
                conn = open_db_connection()
                try:
                    return do_refresh_cache_all(conn)
                finally:
                    conn.close()
            job_func = _job_refresh
            name = payload.name or "Refresh Cache"
        else:
            raise HTTPException(status_code=400, detail="Invalid action")

        job_suffix = uuid.uuid4().hex[:10]
        job_id = f"job_{job_suffix}"
        add_cron_job(job_id, payload.cron_expression, job_func, meta={
            "action": payload.action,
            "name": name,
            "description": payload.description,
        })
        logger.info("Scheduled %s (%s) with cron %s", name, job_id, payload.cron_expression)
        return JobResponse(
            id=int(job_suffix, 16) & 0x7FFFFFFF,
            name=name,
            description=payload.description,
            cron_expression=payload.cron_expression,
            action=payload.action,
            plugin_name=payload.plugin_name,
            author_ids=payload.author_ids,
            enabled=True,
            next_run=None,
            last_run=None,
            created_at=datetime.now().isoformat(),
        )
    except HTTPException:
        raise
    except Exception as e:
        raise_internal("Failed to schedule job", e)


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


@router.post("/run", summary="Run fetch action asynchronously")
def run_async_action(payload: dict, user: dict = Depends(get_current_user)):
    action = payload.get("action")
    if action != "fetch":
        raise HTTPException(status_code=400, detail="Invalid action")

    operation_key = f"fetch.run_action:{action}"
    existing = find_active_job(operation_key)
    if existing:
        return activity_envelope(
            str(existing.get("job_id") or ""),
            status="already_running",
            operation_key=operation_key,
            message=f"{action} action already running",
        )
    job_id = f"run_{action}_{uuid.uuid4().hex[:10]}"
    set_job_status(
        job_id,
        status="running",
        operation_key=operation_key,
        trigger_source="user",
        started_at=datetime.now().isoformat(),
        message=f"Starting {action}",
    )

    def _runner():
        try:
            conn = open_db_connection()
            try:
                res = do_refresh_cache_all(conn, job_id=job_id)
            finally:
                conn.close()
            set_job_status(job_id, status="completed", finished_at=datetime.now().isoformat(), result=res)
        except Exception as e:  # pragma: no cover
            set_job_status(job_id, status="failed", finished_at=datetime.now().isoformat(), error=str(e))

    schedule_immediate(job_id, _runner)
    return activity_envelope(
        job_id,
        status="queued",
        operation_key=operation_key,
        message=f"{action} action queued",
    )
