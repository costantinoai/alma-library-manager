"""Scheduler status and control API endpoints.

Provides endpoints to inspect the state of the background scheduler and
manually trigger scheduled jobs.
"""

import logging
from datetime import datetime

from fastapi import APIRouter, Depends, HTTPException

from alma.api.deps import get_current_user
from alma.api.scheduler import (
    get_scheduler,
    list_jobs,
    run_job,
    _scheduler_enabled,
)

logger = logging.getLogger(__name__)

router = APIRouter(
    dependencies=[Depends(get_current_user)],
    responses={401: {"description": "Unauthorized"}},
)


@router.get(
    "/status",
    summary="Get scheduler status",
    description="Returns whether the scheduler is running and a list of registered jobs with their next run times.",
)
def scheduler_status():
    """Return the current scheduler status and all registered jobs."""
    enabled = _scheduler_enabled()

    # Attempt to read the scheduler state without creating one if disabled
    from alma.api.scheduler import _scheduler
    running = _scheduler is not None and _scheduler.running if _scheduler else False

    jobs = []
    if running:
        try:
            jobs = list_jobs()
        except Exception as exc:
            logger.warning("Failed to list scheduler jobs: %s", exc)

    return {
        "enabled": enabled,
        "running": running,
        "jobs": jobs,
        "checked_at": datetime.utcnow().isoformat(),
    }


@router.post(
    "/trigger/{job_id}",
    summary="Manually trigger a scheduled job",
    description="Trigger a registered scheduler job to run immediately.",
)
def trigger_job(job_id: str):
    """Trigger a scheduler job to run immediately by its ID."""
    from alma.api.scheduler import _scheduler
    if _scheduler is None or not _scheduler.running:
        raise HTTPException(
            status_code=503,
            detail="Scheduler is not running",
        )

    ok = run_job(job_id)
    if not ok:
        raise HTTPException(
            status_code=404,
            detail=f"Job '{job_id}' not found or could not be triggered",
        )

    return {
        "success": True,
        "job_id": job_id,
        "message": f"Job '{job_id}' triggered to run immediately",
        "triggered_at": datetime.utcnow().isoformat(),
    }
