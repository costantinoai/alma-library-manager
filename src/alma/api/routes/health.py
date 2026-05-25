"""Health control-center API (task 24, Pillar 2) — maintenance operations.

Mounted at ``/api/v1/health``. The canonical data-health *dimensions* are
served by ``/api/v1/insights/health`` (Pillar 1); this router exposes the
*maintenance operations* that repair them:

- ``GET  /health/operations`` — every registered maintenance task with its
  config (enabled / daily cap), the canonical pending-work count, and the
  most-recent run (status / duration / trigger).
- ``POST /health/operations/{key}/run`` — run one task now (bounded, idempotent).
- ``POST /health/operations/{key}/config`` — set enabled / daily_cap.

All reads ride the existing materialised-view + operation_status layers; no
new tables. The dedicated Health page (Phase 3) is the primary consumer.
"""

from __future__ import annotations

import sqlite3
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, Field

from alma.api.deps import get_current_user, get_db
from alma.services import maintenance

router = APIRouter()


@router.get(
    "/operations",
    summary="Maintenance operations status",
    description=(
        "Every registered maintenance task with its enabled / daily-cap config, "
        "the canonical count of papers it would address, and its most-recent run."
    ),
)
def get_operations(
    db: sqlite3.Connection = Depends(get_db),
    _user: dict = Depends(get_current_user),
):
    return maintenance.list_operations(db)


@router.post(
    "/operations/{key}/run",
    summary="Run a maintenance task now",
    description=(
        "Schedule one bounded run (trigger_source='user'). Idempotent — returns "
        "the in-flight job if the same operation is already running."
    ),
)
def run_operation(
    key: str,
    db: sqlite3.Connection = Depends(get_db),
    _user: dict = Depends(get_current_user),
):
    task = maintenance.REGISTRY.get(key)
    if task is None:
        raise HTTPException(status_code=404, detail=f"Unknown maintenance task: {key}")
    job_id = maintenance.run_task_now(db, task)
    if not job_id:
        # scheduler not importable (e.g. tests/CLI) — surface a clear noop.
        return {"key": key, "status": "noop", "job_id": None}
    return {"key": key, "status": "queued", "job_id": job_id}


class MaintenanceConfigRequest(BaseModel):
    """Partial update — only provided fields change."""

    enabled: Optional[bool] = Field(default=None, description="Allow the idle healer to run this task")
    daily_cap: Optional[int] = Field(default=None, ge=1, description="Max items the healer processes per UTC day")


@router.post(
    "/operations/{key}/config",
    summary="Configure a maintenance task",
    description="Set the auto-enable flag and/or the daily cap for one task.",
)
def set_operation_config(
    key: str,
    body: MaintenanceConfigRequest,
    db: sqlite3.Connection = Depends(get_db),
    _user: dict = Depends(get_current_user),
):
    task = maintenance.REGISTRY.get(key)
    if task is None:
        raise HTTPException(status_code=404, detail=f"Unknown maintenance task: {key}")
    maintenance.set_task_config(db, task, enabled=body.enabled, daily_cap=body.daily_cap)
    return maintenance.describe_task(db, task, _health_payload(db))


def _health_payload(db: sqlite3.Connection) -> dict:
    from alma.application import materialized_views as mv
    from alma.services import health as health_service

    return (mv.get(db, health_service.HEALTH_CORPUS_VIEW_KEY).get("payload")) or {}
