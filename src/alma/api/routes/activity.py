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
    from alma.api.scheduler import (
        add_job_log,
        get_job_status,
        get_scheduler,
        kill_job_thread,
        set_job_status,
    )

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

    # Running: flag the cooperative cancel checkpoint AND inject JobCancelled
    # straight into the worker thread via PyThreadState_SetAsyncExc. The flag
    # lets runners that are between Python statements bail at their next
    # `is_cancellation_requested(job_id)` check; the async-exc injection
    # interrupts pure-Python loops at the next bytecode boundary so the user
    # doesn't have to wait for a checkpoint to come around.
    set_job_status(
        job_id,
        status="cancelling",
        cancel_requested=True,
        cancel_mode="hard",
        message="Cancellation requested; killing worker thread",
    )
    add_job_log(job_id, "Cancellation requested by user", step="cancel_requested")
    killed = kill_job_thread(job_id)
    if killed:
        add_job_log(
            job_id,
            "Injected JobCancelled into worker thread",
            step="cancel_requested",
        )
    return {
        "success": True,
        "job_id": job_id,
        "status": "cancelling",
        "cancel_requested": True,
        "message": "Cancellation requested" + (" (thread interrupt sent)" if killed else ""),
    }


@router.post(
    "/{job_id}/stop",
    summary="Gracefully stop an operation",
    description=(
        "Request a GRACEFUL stop for a running operation: the worker keeps "
        "control, finishes its in-flight batch, commits the work done so far "
        "and exits at its next cooperative checkpoint. No thread interrupt "
        "is sent (contrast with /cancel, which kills the worker thread). "
        "Queued jobs are unscheduled immediately."
    ),
)
def stop_operation(
    job_id: str,
    user: dict = Depends(get_current_user),
):
    from alma.api.scheduler import (
        add_job_log,
        get_job_status,
        get_scheduler,
        set_job_status,
    )

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

    # A stop request never downgrades a hard cancel already in flight.
    if st.get("cancel_requested"):
        return {
            "success": True,
            "job_id": job_id,
            "status": current,
            "cancel_requested": True,
            "message": "Cancellation already requested",
        }

    # Not started yet → nothing to finish gracefully; cancel outright.
    removed = False
    try:
        sched = get_scheduler()
        job = sched.get_job(job_id)
        if job is not None:
            sched.remove_job(job_id)
            removed = True
    except Exception as exc:
        logger.debug("Stop unschedule attempt failed for %s: %s", job_id, exc)

    if removed or current in {"queued", "scheduled"}:
        set_job_status(
            job_id,
            status="cancelled",
            cancel_requested=True,
            cancel_mode="graceful",
            finished_at=datetime.utcnow().isoformat(),
            message="Operation cancelled",
        )
        add_job_log(job_id, "Stop completed before execution", step="cancelled")
        return {
            "success": True,
            "job_id": job_id,
            "status": "cancelled",
            "cancel_requested": True,
            "message": "Operation cancelled",
        }

    # Running: raise the cooperative flag only. Runners break at their next
    # `is_cancellation_requested(job_id)` loop boundary with the current
    # batch's work already committed; the final partial summary is merged
    # into the Activity row when the runner returns.
    set_job_status(
        job_id,
        status="cancelling",
        cancel_requested=True,
        cancel_mode="graceful",
        message="Graceful stop requested; finishing current batch",
    )
    add_job_log(job_id, "Graceful stop requested by user", step="cancel_requested")
    return {
        "success": True,
        "job_id": job_id,
        "status": "cancelling",
        "cancel_requested": True,
        "message": "Graceful stop requested; work done so far will be saved",
    }
