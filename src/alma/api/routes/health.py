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

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel, Field

from alma.api.deps import get_current_user, get_db
from alma.services import health as health_service
from alma.services import maintenance

router = APIRouter()


@router.get(
    "/dimensions/{key}/items",
    summary="Papers affected by a health dimension",
    description=(
        "Paginated list of the papers a Data Health dimension is flagging, so "
        "the Health page can drill down to which papers and offer fixes."
    ),
)
def get_dimension_items(
    key: str,
    limit: int = Query(20, ge=1, le=100),
    offset: int = Query(0, ge=0),
    db: sqlite3.Connection = Depends(get_db),
    _user: dict = Depends(get_current_user),
):
    if key not in health_service.DIMENSION_ITEM_KEYS:
        raise HTTPException(status_code=404, detail=f"No drilldown for dimension: {key}")
    items = health_service.dimension_items(db, key, limit=limit, offset=offset)
    return {"key": key, "limit": limit, "offset": offset, "items": items}


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


@router.get(
    "/operations/{key}/estimate",
    summary="Recompute a maintenance op's pending count + ETA for chosen params",
    description=(
        "Cheap recompute of just ``candidates_pending`` + ``eta`` for a given "
        "``scope`` / ``dry_run`` — lets the UI refresh the ETA when the user "
        "changes a control without re-listing every operation."
    ),
)
def estimate_operation(
    key: str,
    scope: Optional[str] = Query(None),
    dry_run: Optional[bool] = Query(None),
    batch_size: Optional[int] = Query(None, ge=1),
    db: sqlite3.Connection = Depends(get_db),
    _user: dict = Depends(get_current_user),
):
    task = maintenance.REGISTRY.get(key)
    if task is None:
        raise HTTPException(status_code=404, detail=f"Unknown maintenance task: {key}")
    params: dict = {}
    if scope is not None:
        params["scope"] = scope
    if dry_run is not None:
        params["dry_run"] = dry_run
    return maintenance.estimate_task(
        db, task, _health_payload(db), params=params or None, batch_size=batch_size
    )


class RunMaintenanceRequest(BaseModel):
    """Optional body. ``target_paper_ids`` restricts the run to a specific set (a
    drilldown 'fix selected'). ``params`` carries run-time controls a task declares
    in its ``params_spec`` (e.g. ``{"scope": "library"}`` or ``{"dry_run": true}``).
    Omit both to run the task at its default scope + daily cap."""

    target_paper_ids: Optional[list[str]] = Field(default=None)
    params: Optional[dict] = Field(default=None)


@router.post(
    "/operations/{key}/run",
    summary="Run a maintenance task now",
    description=(
        "Schedule one bounded run (trigger_source='user'). Idempotent — returns "
        "the in-flight job if the same operation is already running. An optional "
        "body restricts the run to specific paper ids."
    ),
)
def run_operation(
    key: str,
    body: Optional[RunMaintenanceRequest] = None,
    db: sqlite3.Connection = Depends(get_db),
    _user: dict = Depends(get_current_user),
):
    task = maintenance.REGISTRY.get(key)
    if task is None:
        raise HTTPException(status_code=404, detail=f"Unknown maintenance task: {key}")
    targets = body.target_paper_ids if body else None
    params = body.params if body else None
    job_id = maintenance.run_task_now(db, task, target_paper_ids=targets, params=params)
    if not job_id:
        # scheduler not importable (e.g. tests/CLI) — surface a clear noop.
        return {"key": key, "status": "noop", "job_id": None}
    return {"key": key, "status": "queued", "job_id": job_id}


class MaintenanceConfigRequest(BaseModel):
    """Partial update — only provided fields change."""

    enabled: Optional[bool] = Field(default=None, description="Allow the idle healer to run this task")
    daily_cap: Optional[int] = Field(default=None, ge=1, description="Max items the healer processes per UTC day")
    batch_size: Optional[int] = Field(default=None, ge=1, description="API items per request (overridable ops only)")


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
    maintenance.set_task_config(
        db, task, enabled=body.enabled, daily_cap=body.daily_cap, batch_size=body.batch_size
    )
    return maintenance.describe_task(db, task, _health_payload(db))


def _health_payload(db: sqlite3.Connection) -> dict:
    from alma.application import materialized_views as mv
    from alma.services import health as health_service

    return (mv.get(db, health_service.HEALTH_CORPUS_VIEW_KEY).get("payload")) or {}
