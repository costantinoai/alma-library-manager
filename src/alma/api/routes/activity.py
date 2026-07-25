"""Active operations API endpoint.

Exposes the current and recently completed job statuses from the scheduler.
"""

import logging
import sqlite3

from fastapi import APIRouter, Depends, HTTPException, Query, Response, status

from alma.api.deps import get_current_user, get_db
from alma.core.time import utcnow

logger = logging.getLogger(__name__)

router = APIRouter(
    responses={
        401: {"description": "Unauthorized"},
    },
)

_OWNERLESS_ERROR = (
    "The worker executing this operation is gone (backend restart, reload, or "
    "crash), so there was nothing left to interrupt. ALMa closed the row when "
    "you asked it to stop. Any work already committed is kept; re-run the "
    "operation to drain the remainder."
)


def _apply_manual_stop_cooldown(db: sqlite3.Connection, st: dict) -> dict | None:
    """Hold automation off the operation the user just stopped by hand.

    Only for BACKGROUND runs (`scheduler`, `auto:*`): those are the ones the app
    starts on its own, so without a cooldown the hourly healer tick or the
    startup orphan-resume simply brings the job back and "stop" means nothing.
    Stopping a run the USER launched aborts that run only — their standing
    automation policy is none of this action's business.

    Returns a wire fragment describing the pause, or None when nothing was
    paused (user-triggered run, or an operation nothing auto-schedules).
    """
    from alma.api.scheduler import is_user_facing_trigger
    from alma.services import maintenance

    if is_user_facing_trigger(st.get("trigger_source")):
        return None
    operation_key = str(st.get("operation_key") or "")
    try:
        paused = maintenance.pause_task_by_operation_key(db, operation_key)
    except Exception as exc:
        # Never fail the stop itself because the cooldown could not be written —
        # stopping is the user's instruction; the cooldown is a courtesy.
        logger.warning("Manual-stop cooldown failed for %s: %s", operation_key, exc)
        return None
    if paused is None:
        return None
    task, until = paused
    return {
        "task_key": task.key,
        "task_label": task.label,
        "paused_by_user_until": until.isoformat(),
    }


def _with_cooldown(payload: dict, cooldown: dict | None) -> dict:
    """Attach the manual-stop cooldown (if any) to a stop-verb response."""
    if cooldown:
        payload = {**payload, "automation_paused": cooldown}
    return payload


def _has_pending_scheduler_job(job_id: str) -> bool:
    """True when APScheduler still holds a not-yet-started job for ``job_id``."""
    from alma.api.scheduler import get_scheduler

    try:
        return get_scheduler().get_job(job_id) is not None
    except Exception as exc:
        logger.debug("Scheduler lookup failed for %s: %s", job_id, exc)
        return False


def _finalize_ownerless(job_id: str, *, verb: str) -> dict:
    """Close an active-looking row that has no worker thread behind it.

    `schedule_immediate` runs jobs in THIS process, so an `operation_status`
    row that says running/cancelling while `has_running_thread` is False is a
    zombie: its thread exited without a terminal write, or the process that
    owned it died. Nothing will ever advance it — cooperative checkpoints
    never come around and `PyThreadState_SetAsyncExc` has no target — so the
    row used to sit at "cancelling" forever. Worse, every stop click re-stamped
    `updated_at`, which reset the orphan reaper's 300 s staleness clock and
    made the row permanently un-reapable. Finalizing here is what makes the
    hard X actually terminal.
    """
    from alma.api.scheduler import add_job_log, set_job_status

    set_job_status(
        job_id,
        status="cancelled",
        cancel_requested=True,
        finished_at=utcnow().isoformat(),
        message="Operation cancelled (worker no longer running)",
        error=_OWNERLESS_ERROR,
        result={"success": False, "cancelled": True, "ownerless": True},
    )
    add_job_log(job_id, f"{verb}: no live worker thread; row closed", step="cancelled")
    logger.warning("Finalized ownerless activity row %s (%s)", job_id, verb.lower())
    return {
        "success": True,
        "job_id": job_id,
        "status": "cancelled",
        "cancel_requested": True,
        "message": "Operation cancelled (its worker was already gone)",
    }


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
    db: sqlite3.Connection = Depends(get_db),
    user: dict = Depends(get_current_user),
):
    from alma.api.scheduler import (
        add_job_log,
        get_job_status,
        get_scheduler,
        has_running_thread,
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

    cooldown = _apply_manual_stop_cooldown(db, st)
    if cooldown:
        add_job_log(
            job_id,
            f"Automation for {cooldown['task_label']} paused until "
            f"{cooldown['paused_by_user_until']} (stopped by user)",
            step="paused_by_user",
            data=cooldown,
        )

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
            finished_at=utcnow().isoformat(),
            message="Operation cancelled",
        )
        add_job_log(job_id, "Cancellation completed before execution", step="cancelled")
        return _with_cooldown(
            {
                "success": True,
                "job_id": job_id,
                "status": "cancelled",
                "cancel_requested": True,
                "message": "Operation cancelled",
            },
            cooldown,
        )

    # The row says active but no thread in this process owns it → nothing will
    # ever advance it (see `_finalize_ownerless`). Finish it here.
    if not has_running_thread(job_id):
        return _with_cooldown(_finalize_ownerless(job_id, verb="Kill"), cooldown)

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
    return _with_cooldown(
        {
            "success": True,
            "job_id": job_id,
            "status": "cancelling",
            "cancel_requested": True,
            "message": "Cancellation requested" + (" (thread interrupt sent)" if killed else ""),
        },
        cooldown,
    )


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
    db: sqlite3.Connection = Depends(get_db),
    user: dict = Depends(get_current_user),
):
    from alma.api.scheduler import (
        add_job_log,
        get_job_status,
        get_scheduler,
        has_running_thread,
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

    cooldown = _apply_manual_stop_cooldown(db, st)
    if cooldown:
        add_job_log(
            job_id,
            f"Automation for {cooldown['task_label']} paused until "
            f"{cooldown['paused_by_user_until']} (stopped by user)",
            step="paused_by_user",
            data=cooldown,
        )

    # Ownerless row (no worker thread AND nothing pending in the scheduler) →
    # no checkpoint will ever come around. Close it instead of parking.
    if not has_running_thread(job_id) and not _has_pending_scheduler_job(job_id):
        return _with_cooldown(_finalize_ownerless(job_id, verb="Stop"), cooldown)

    # A stop request never downgrades a hard cancel already in flight.
    if st.get("cancel_requested"):
        return _with_cooldown(
            {
                "success": True,
                "job_id": job_id,
                "status": current,
                "cancel_requested": True,
                "message": "Cancellation already requested",
            },
            cooldown,
        )

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
            finished_at=utcnow().isoformat(),
            message="Operation cancelled",
        )
        add_job_log(job_id, "Stop completed before execution", step="cancelled")
        return _with_cooldown(
            {
                "success": True,
                "job_id": job_id,
                "status": "cancelled",
                "cancel_requested": True,
                "message": "Operation cancelled",
            },
            cooldown,
        )

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
    return _with_cooldown(
        {
            "success": True,
            "job_id": job_id,
            "status": "cancelling",
            "cancel_requested": True,
            "message": "Graceful stop requested; work done so far will be saved",
        },
        cooldown,
    )
