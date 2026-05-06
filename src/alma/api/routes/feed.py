"""Feed inbox routes (transport-only wrappers over application use-cases)."""

from __future__ import annotations

from datetime import datetime
import logging
import sqlite3
import uuid

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel

from alma.api.deps import get_current_user, get_db, open_db_connection
from alma.api.helpers import ActivityJobContext, background_mode_requested, raise_internal
from alma.api.models import FeedItemResponse, FeedMonitorCreateRequest, FeedMonitorResponse, FeedMonitorUpdateRequest
from alma.application import feed as feed_app
from alma.application import feed_monitors as monitor_app
from alma.core.operations import OperationOutcome, OperationRunner

logger = logging.getLogger(__name__)

router = APIRouter(
    dependencies=[Depends(get_current_user)],
    responses={401: {"description": "Unauthorized"}},
)


class FeedBulkActionRequest(BaseModel):
    feed_item_ids: list[str]
    action: str


def _run_feed_refresh_sync(*, db: sqlite3.Connection, user: dict) -> dict[str, object]:
    """Run a feed refresh synchronously using the operation lifecycle store."""
    runner = OperationRunner(db)

    def _handler(ctx):
        result = feed_app.refresh_feed_inbox(db, ctx=ctx)
        items_created = int(result.get("items_created") or 0)
        if items_created == 0:
            return OperationOutcome(
                status="noop",
                message="No new papers found from active monitors",
                result=result,
            )
        return OperationOutcome(
            status="completed",
            message=f"Added {items_created} new papers to feed inbox",
            result=result,
        )

    op = runner.run(
        operation_key="feed.refresh_inbox",
        handler=_handler,
        trigger_source="user",
        actor=str(user.get("username") or "api_user"),
        running_message="Feed refresh: fetching new papers from active monitors",
    )
    return {"operation": op, "result": op.get("result")}


def _run_feed_monitor_refresh_sync(*, db: sqlite3.Connection, user: dict, monitor_id: str) -> dict[str, object]:
    """Run a single feed-monitor refresh synchronously using operation tracking."""
    runner = OperationRunner(db)

    def _handler(ctx):
        result = feed_app.refresh_feed_monitor(db, monitor_id, ctx=ctx)
        if result is None:
            return OperationOutcome(
                status="noop",
                message="Feed monitor not found",
                result={"found": False},
            )
        items_created = int(result.get("items_created") or 0)
        return OperationOutcome(
            status="completed" if items_created > 0 or str(result.get("status") or "") == "completed" else "noop",
            message=f"Refreshed feed monitor {monitor_id}",
            result=result,
        )

    op = runner.run(
        operation_key=f"feed.monitor.refresh:{monitor_id}",
        handler=_handler,
        trigger_source="user",
        actor=str(user.get("username") or "api_user"),
        queued=False,
        running_message="Refreshing feed monitor",
    )
    if op["status"] == "noop" and (op.get("result") or {}).get("found") is False:
        raise HTTPException(status_code=404, detail="Feed monitor not found")
    return {"operation": op, "result": op.get("result")}


@router.get(
    "",
    summary="List feed inbox items",
)
def list_feed_items(
    status: str | None = Query(
        default=None,
        description="Optional status filter: all | new | add | like | love | dislike",
    ),
    sort: str = Query(
        default="chronological",
        description="Sort order: chronological (default) or relevance",
    ),
    limit: int = Query(default=50, ge=1, le=200),
    offset: int = Query(default=0, ge=0),
    since_days: int = Query(
        default=60,
        ge=1,
        le=3650,
        description="Restrict to items published (or fetched) within the last N days. Defaults to 60.",
    ),
    db: sqlite3.Connection = Depends(get_db),
):
    """Return feed inbox items from feed_items table."""
    try:
        items, total = feed_app.list_feed_items(
            db,
            status=status,
            sort=sort,
            limit=limit,
            offset=offset,
            since_days=since_days,
        )
        return {"items": [FeedItemResponse(**item).model_dump() for item in items], "total": total}
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:
        raise_internal("Failed to list feed items", exc)


@router.get(
    "/status",
    summary="Feed refresh status",
)
def get_feed_status(
    db: sqlite3.Connection = Depends(get_db),
):
    """Return lightweight feed status, including the last successful refresh.

    ``last_refresh_at`` is the latest ``finished_at`` across completed full
    inbox or per-monitor feed refreshes in ``operation_status``. ``new_count``
    counts only still-new papers fetched during that latest window.
    """
    try:
        _, last = feed_app.latest_feed_fetch_window(db)
        return {
            "last_refresh_at": last,
            "new_count": feed_app.count_new_feed_items_since_latest_fetch(db),
        }
    except Exception as exc:
        raise_internal("Failed to read feed status", exc)


@router.get(
    "/monitors",
    summary="List feed monitors",
)
def list_feed_monitors(
    db: sqlite3.Connection = Depends(get_db),
):
    try:
        items = monitor_app.list_feed_monitors(db)
        return [FeedMonitorResponse(**item).model_dump() for item in items]
    except Exception as exc:
        raise_internal("Failed to list feed monitors", exc)


@router.post(
    "/monitors",
    summary="Create a non-author feed monitor",
)
def create_feed_monitor(
    payload: FeedMonitorCreateRequest,
    db: sqlite3.Connection = Depends(get_db),
):
    try:
        created = monitor_app.create_feed_monitor(
            db,
            monitor_type=payload.monitor_type,
            query=payload.query,
            label=payload.label,
            config=payload.config,
        )
        return FeedMonitorResponse(**created).model_dump()
    except sqlite3.IntegrityError as exc:
        raise HTTPException(status_code=409, detail=str(exc))
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    except Exception as exc:
        raise_internal("Failed to create feed monitor", exc)


@router.delete(
    "/monitors/{monitor_id}",
    summary="Delete a non-author feed monitor",
)
def delete_feed_monitor(
    monitor_id: str,
    db: sqlite3.Connection = Depends(get_db),
):
    try:
        deleted = monitor_app.delete_feed_monitor(db, monitor_id)
        if not deleted:
            raise HTTPException(status_code=404, detail="Feed monitor not found")
        return {"success": True, "monitor_id": monitor_id}
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    except HTTPException:
        raise
    except Exception as exc:
        raise_internal("Failed to delete feed monitor", exc)


@router.put(
    "/monitors/{monitor_id}",
    summary="Update a feed monitor",
)
def update_feed_monitor(
    monitor_id: str,
    payload: FeedMonitorUpdateRequest,
    db: sqlite3.Connection = Depends(get_db),
):
    if payload.query is None and payload.label is None and payload.enabled is None and payload.config is None:
        raise HTTPException(status_code=400, detail="No monitor changes were provided")
    try:
        updated = monitor_app.update_feed_monitor(
            db,
            monitor_id,
            query=payload.query,
            label=payload.label,
            enabled=payload.enabled,
            config=payload.config,
        )
        if updated is None:
            raise HTTPException(status_code=404, detail="Feed monitor not found")
        return FeedMonitorResponse(**updated).model_dump()
    except sqlite3.IntegrityError as exc:
        raise HTTPException(status_code=409, detail=str(exc))
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    except HTTPException:
        raise
    except Exception as exc:
        raise_internal("Failed to update feed monitor", exc)


def _run_feed_action(
    *,
    feed_item_id: str,
    action: str,
    db: sqlite3.Connection,
    user: dict,
) -> dict:
    existing = feed_app.get_feed_item(db, feed_item_id)
    if not existing:
        raise HTTPException(status_code=404, detail="Feed item not found")

    runner = OperationRunner(db)

    def _handler(_ctx):
        result = feed_app.apply_feed_action(db, feed_item_id, action)
        if result is None:
            return OperationOutcome(
                status="noop",
                message=f"Feed item {feed_item_id} no longer exists",
                result={"changed": False},
            )
        return OperationOutcome(
            status="completed",
            message=f"Applied '{action}' to feed item {feed_item_id}",
            result=result,
        )

    operation = runner.run(
        operation_key=f"feed.action:{action}",
        handler=_handler,
        trigger_source="user",
        actor=str(user.get("username") or "api_user"),
        queued=False,
        running_message=f"Applying '{action}' action to feed item",
    )
    item = feed_app.get_feed_item(db, feed_item_id)
    return {
        "item": FeedItemResponse(**item).model_dump() if item else None,
        "operation": operation,
    }


@router.post(
    "/refresh",
    summary="Refresh feed inbox from active monitors",
)
def refresh_feed_inbox(
    background: bool | None = Query(
        default=None,
        description="Run as a background Activity job when the scheduler is enabled",
    ),
    db: sqlite3.Connection = Depends(get_db),
    user: dict = Depends(get_current_user),
):
    """Fetch recent works from active monitors and populate the feed inbox."""
    try:
        if not background_mode_requested(background):
            return _run_feed_refresh_sync(db=db, user=user)

        monitor_app.sync_author_monitors(db)
        active_monitors = [monitor for monitor in monitor_app.list_feed_monitors(db) if monitor.get("enabled", True)]
        if not active_monitors:
            return _run_feed_refresh_sync(db=db, user=user)

        from alma.api.scheduler import activity_envelope, add_job_log, find_active_job, schedule_immediate, set_job_status

        operation_key = "feed.refresh_inbox"
        existing = find_active_job(operation_key)
        if existing:
            return activity_envelope(
                str(existing.get("job_id") or ""),
                status="already_running",
                operation_key=operation_key,
                message="Feed refresh already running",
                total=len(active_monitors),
            )

        job_id = f"feed_refresh_{uuid.uuid4().hex[:10]}"
        queued_message = f"Queued feed refresh for {len(active_monitors)} active monitors"
        set_job_status(
            job_id,
            status="queued",
            operation_key=operation_key,
            trigger_source="user",
            started_at=datetime.utcnow().isoformat(),
            processed=0,
            total=len(active_monitors),
            message=queued_message,
        )
        add_job_log(job_id, queued_message, step="queued", data={"monitors_total": len(active_monitors)})

        def _runner() -> None:
            conn = open_db_connection()
            try:
                result = feed_app.refresh_feed_inbox(conn, ctx=ActivityJobContext(job_id))
                items_created = int(result.get("items_created") or 0)
                final_status = "noop" if items_created == 0 else "completed"
                final_message = (
                    "No new papers found from active monitors"
                    if items_created == 0
                    else f"Added {items_created} new papers to feed inbox"
                )
                set_job_status(
                    job_id,
                    status=final_status,
                    finished_at=datetime.utcnow().isoformat(),
                    processed=len(active_monitors),
                    total=len(active_monitors),
                    message=final_message,
                    result=result,
                    operation_key=operation_key,
                    trigger_source="user",
                )
            except Exception as exc:
                add_job_log(job_id, f"Feed refresh failed: {exc}", level="ERROR", step="failed")
                set_job_status(
                    job_id,
                    status="failed",
                    finished_at=datetime.utcnow().isoformat(),
                    message=f"Feed refresh failed: {exc}",
                    error=str(exc),
                    operation_key=operation_key,
                    trigger_source="user",
                )
            finally:
                conn.close()

        schedule_immediate(job_id, _runner)
        return activity_envelope(
            job_id,
            status="queued",
            operation_key=operation_key,
            message=queued_message,
            total=len(active_monitors),
            processed=0,
        )
    except Exception as exc:
        raise_internal("Failed to refresh feed inbox", exc)


@router.post(
    "/monitors/{monitor_id}/refresh",
    summary="Refresh one feed monitor",
)
def refresh_feed_monitor(
    monitor_id: str,
    background: bool | None = Query(
        default=None,
        description="Run as a background Activity job when the scheduler is enabled",
    ),
    db: sqlite3.Connection = Depends(get_db),
    user: dict = Depends(get_current_user),
):
    try:
        if not background_mode_requested(background):
            return _run_feed_monitor_refresh_sync(db=db, user=user, monitor_id=monitor_id)

        monitor_app.sync_author_monitors(db)
        monitors = monitor_app.list_feed_monitors(db)
        monitor = next((item for item in monitors if str(item.get("id") or "") == str(monitor_id or "")), None)
        if monitor is None:
            raise HTTPException(status_code=404, detail="Feed monitor not found")
        if not monitor.get("enabled", True):
            return _run_feed_monitor_refresh_sync(db=db, user=user, monitor_id=monitor_id)

        from alma.api.scheduler import activity_envelope, add_job_log, find_active_job, schedule_immediate, set_job_status

        operation_key = f"feed.monitor.refresh:{monitor_id}"
        existing = find_active_job(operation_key)
        monitor_label = str(monitor.get("label") or monitor_id)
        if existing:
            return activity_envelope(
                str(existing.get("job_id") or ""),
                status="already_running",
                operation_key=operation_key,
                message=f"Feed monitor refresh already running for {monitor_label}",
                total=1,
            )

        job_id = f"feed_monitor_refresh_{uuid.uuid4().hex[:10]}"
        queued_message = f"Queued feed monitor refresh for {monitor_label}"
        set_job_status(
            job_id,
            status="queued",
            operation_key=operation_key,
            trigger_source="user",
            started_at=datetime.utcnow().isoformat(),
            processed=0,
            total=1,
            message=queued_message,
        )
        add_job_log(job_id, queued_message, step="queued", data={"monitor_id": monitor_id})

        def _runner() -> None:
            conn = open_db_connection()
            try:
                result = feed_app.refresh_feed_monitor(conn, monitor_id, ctx=ActivityJobContext(job_id))
                if result is None:
                    set_job_status(
                        job_id,
                        status="noop",
                        finished_at=datetime.utcnow().isoformat(),
                        processed=1,
                        total=1,
                        message="Feed monitor not found",
                        result={"found": False},
                        operation_key=operation_key,
                        trigger_source="user",
                    )
                    return

                items_created = int(result.get("items_created") or 0)
                final_status = "completed" if items_created > 0 or str(result.get("status") or "") == "completed" else "noop"
                set_job_status(
                    job_id,
                    status=final_status,
                    finished_at=datetime.utcnow().isoformat(),
                    processed=1,
                    total=1,
                    message=f"Refreshed feed monitor {monitor_label}",
                    result=result,
                    operation_key=operation_key,
                    trigger_source="user",
                )
            except Exception as exc:
                add_job_log(job_id, f"Feed monitor refresh failed: {exc}", level="ERROR", step="failed")
                set_job_status(
                    job_id,
                    status="failed",
                    finished_at=datetime.utcnow().isoformat(),
                    message=f"Feed monitor refresh failed: {exc}",
                    error=str(exc),
                    operation_key=operation_key,
                    trigger_source="user",
                )
            finally:
                conn.close()

        schedule_immediate(job_id, _runner)
        return activity_envelope(
            job_id,
            status="queued",
            operation_key=operation_key,
            message=queued_message,
            total=1,
            processed=0,
        )
    except HTTPException:
        raise
    except Exception as exc:
        raise_internal("Failed to refresh feed monitor", exc)


@router.post("/{feed_item_id}/add", summary="Add feed paper to library")
def add_feed_item(
    feed_item_id: str,
    db: sqlite3.Connection = Depends(get_db),
    user: dict = Depends(get_current_user),
):
    try:
        return _run_feed_action(feed_item_id=feed_item_id, action="add", db=db, user=user)
    except HTTPException:
        raise
    except Exception as exc:
        raise_internal("Failed to add feed item to library", exc)


@router.post("/{feed_item_id}/like", summary="Like feed paper and save to library")
def like_feed_item(
    feed_item_id: str,
    db: sqlite3.Connection = Depends(get_db),
    user: dict = Depends(get_current_user),
):
    try:
        return _run_feed_action(feed_item_id=feed_item_id, action="like", db=db, user=user)
    except HTTPException:
        raise
    except Exception as exc:
        raise_internal("Failed to like feed item", exc)


@router.post("/{feed_item_id}/love", summary="Love feed paper and save to library with 5 stars")
def love_feed_item(
    feed_item_id: str,
    db: sqlite3.Connection = Depends(get_db),
    user: dict = Depends(get_current_user),
):
    try:
        return _run_feed_action(feed_item_id=feed_item_id, action="love", db=db, user=user)
    except HTTPException:
        raise
    except Exception as exc:
        raise_internal("Failed to love feed item", exc)


@router.post("/{feed_item_id}/dislike", summary="Dislike feed paper and send a negative signal")
def dislike_feed_item(
    feed_item_id: str,
    db: sqlite3.Connection = Depends(get_db),
    user: dict = Depends(get_current_user),
):
    try:
        return _run_feed_action(feed_item_id=feed_item_id, action="dislike", db=db, user=user)
    except HTTPException:
        raise
    except Exception as exc:
        raise_internal("Failed to dislike feed item", exc)


@router.post("/bulk-action", summary="Apply one action to multiple feed items")
def bulk_feed_action(
    body: FeedBulkActionRequest,
    db: sqlite3.Connection = Depends(get_db),
    user: dict = Depends(get_current_user),
):
    if body.action not in feed_app.VALID_FEED_ACTIONS:
        raise HTTPException(status_code=400, detail=f"Invalid feed action: {body.action}")
    results: list[dict] = []
    for feed_item_id in body.feed_item_ids:
        try:
            result = feed_app.apply_feed_action(
                db,
                str(feed_item_id or "").strip(),
                body.action,
            )
            if result is not None:
                results.append(result)
        except Exception as exc:
            logger.debug("Bulk feed action failed for %s: %s", feed_item_id, exc)
            continue
    return {
        "affected": len(results),
        "action": body.action,
        "results": results,
    }
