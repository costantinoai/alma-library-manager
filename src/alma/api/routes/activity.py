"""Active operations API endpoint.

Exposes the current and recently completed job statuses from the scheduler.
"""

import logging
from datetime import datetime

from fastapi import APIRouter, Depends, HTTPException, Query, Response, status

from alma.api.deps import get_current_user

logger = logging.getLogger(__name__)

router = APIRouter(
    responses={
        401: {"description": "Unauthorized"},
    },
)


@router.get(
    "",
    summary="List active operations",
    description="Return all active and recently completed job statuses from the scheduler.",
)
def get_active_operations(
    response: Response,
    status_filter: str | None = Query(None, alias="status", description="Filter by status"),
    trigger_source: str | None = Query(None, description="Filter by trigger source"),
    since: str | None = Query(None, description="Only include operations updated at/after ISO timestamp"),
    cursor: str | None = Query(None, description="Cursor for pagination"),
    limit: int = Query(200, ge=1, le=2000),
    user: dict = Depends(get_current_user),
):
    """Return every tracked job status (running, completed, failed)."""
    from alma.api.scheduler import list_all_job_statuses_page

    items, next_cursor, has_more = list_all_job_statuses_page(
        status=status_filter,
        trigger_source=trigger_source,
        since=since,
        limit=limit,
        cursor=cursor,
    )
    response.headers["X-Has-More"] = "true" if has_more else "false"
    if next_cursor:
        response.headers["X-Next-Cursor"] = next_cursor
    return items


@router.get(
    "/{job_id}",
    summary="Get one operation status",
    description=(
        "Return the latest status envelope for a specific job id, including any "
        "result JSON once the job has completed. Frontend callers use this to "
        "poll for Activity-backed operations that produce result payloads "
        "(e.g. discovery search, author preview)."
    ),
)
def get_operation_status(
    job_id: str,
    user: dict = Depends(get_current_user),
):
    from alma.api.scheduler import get_job_status

    st = get_job_status(job_id)
    if not st:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Operation '{job_id}' not found",
        )
    return st


@router.get(
    "/{job_id}/logs",
    summary="Get logs for one operation",
    description="Return detailed structured log entries for a specific job id.",
)
def get_operation_logs(
    job_id: str,
    response: Response,
    cursor: str | None = Query(None, description="Cursor for pagination"),
    limit: int = Query(100, ge=1, le=500),
    user: dict = Depends(get_current_user),
):
    from alma.api.scheduler import get_job_logs_page

    entries, next_cursor, has_more = get_job_logs_page(job_id, limit=limit, cursor=cursor)
    response.headers["X-Has-More"] = "true" if has_more else "false"
    if next_cursor:
        response.headers["X-Next-Cursor"] = next_cursor
    return entries


@router.delete(
    "/{job_id}",
    summary="Dismiss an operation from Activity",
    description="Removes an operation entry and its detailed logs from Activity.",
)
def dismiss_operation(
    job_id: str,
    user: dict = Depends(get_current_user),
):
    from alma.api.scheduler import dismiss_job_status

    ok = dismiss_job_status(job_id)
    if not ok:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Operation '{job_id}' not found",
        )
    return {"success": True, "job_id": job_id}


@router.post(
    "/{job_id}/cancel",
    summary="Cancel an operation",
    description=(
        "Request cancellation for a queued/running operation. "
        "Queued jobs are unscheduled immediately; running jobs are marked "
        "as cancel-requested and stop at the next cooperative checkpoint."
    ),
)
def cancel_operation(
    job_id: str,
    user: dict = Depends(get_current_user),
):
    from alma.api.scheduler import add_job_log, get_job_status, get_scheduler, set_job_status

    st = get_job_status(job_id)
    if not st:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Operation '{job_id}' not found",
        )

    current = (st.get("status") or "").lower()
    if current in {"completed", "failed", "cancelled"}:
        return {
            "success": True,
            "job_id": job_id,
            "status": current,
            "cancel_requested": False,
            "message": f"Operation already {current}",
        }

    removed = False
    try:
        sched = get_scheduler()
        job = sched.get_job(job_id)
        if job is not None:
            sched.remove_job(job_id)
            removed = True
    except Exception as exc:
        logger.debug("Cancel unschedule attempt failed for %s: %s", job_id, exc)

    if removed or current in {"queued", "scheduled"}:
        set_job_status(
            job_id,
            status="cancelled",
            cancel_requested=True,
            finished_at=datetime.utcnow().isoformat(),
            message="Operation cancelled",
        )
        add_job_log(job_id, "Cancellation completed before execution", step="cancelled")
        return {
            "success": True,
            "job_id": job_id,
            "status": "cancelled",
            "cancel_requested": True,
            "message": "Operation cancelled",
        }

    # Running: cooperative cancellation only.
    set_job_status(
        job_id,
        status="cancelling",
        cancel_requested=True,
        message="Cancellation requested; stopping at next checkpoint",
    )
    add_job_log(job_id, "Cancellation requested by user", step="cancel_requested")
    return {
        "success": True,
        "job_id": job_id,
        "status": "cancelling",
        "cancel_requested": True,
        "message": "Cancellation requested",
    }
