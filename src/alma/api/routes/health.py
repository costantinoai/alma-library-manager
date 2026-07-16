"""Health control-center API (task 24, Pillar 2) — maintenance operations.

Mounted at ``/api/v1/health``. The canonical data-health *dimensions* are
served by ``/api/v1/insights/health`` (Pillar 1); this router exposes the
*maintenance operations* that repair them:

- ``GET  /health/operations`` — backend-ordered maintenance stages: every task
  with its separated config (auto-enable / auto daily cap / remembered manual
  limit / request batch), the canonical pending-work count, readiness/blocked
  state, and the most-recent run (status / duration / trigger).
- ``POST /health/operations/{key}/run`` — run one task now with an atomic spec
  (the visible Run-now values travel with the click; bounded, idempotent).
- ``POST /health/operations/{key}/config`` — set any of the four separated
  controls; impossible values are rejected with 422, never silently clamped.

All reads ride the existing materialised-view + operation_status layers; no
new tables. The dedicated Health page (Phase 3) is the primary consumer.
"""

from __future__ import annotations

import sqlite3

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel, Field

from alma.api.deps import get_current_user, get_db
from alma.core.db_write import run_write_unit
from alma.services import health as health_service
from alma.services import maintenance
from alma.services.background_settings import (
    DEFAULT_IDLE_WAIT_MINUTES,
    DEFAULT_RESERVED_API_CALLS,
    IDLE_WAIT_MINUTES_KEY,
    RESERVED_API_CALLS_KEY,
    get_idle_wait_minutes,
    get_reserved_api_calls,
)
from alma.services.maintenance_contracts import (
    MaintenanceRunSpec,
    MaintenanceTrigger,
    MaintenanceValidationError,
)

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
    # H-11: report has_more (sentinel-row probe) so the UI never shows "Load more"
    # on an exact final page.
    items, has_more = health_service.dimension_items_page(db, key, limit=limit, offset=offset)
    return {"key": key, "limit": limit, "offset": offset, "items": items, "has_more": has_more}


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


class GovernanceSettings(BaseModel):
    """Background-ops governance knobs (task 37) — Settings → Data & system.

    KV-backed in the shared `discovery_settings` store; the shipped constants are
    the defaults. The gate reads `idle_wait_minutes`; the per-sweep reserve + the
    Health budget read `reserved_api_calls`.
    """

    idle_wait_minutes: int = Field(
        DEFAULT_IDLE_WAIT_MINUTES,
        ge=0,
        le=120,
        description="Minutes the app must be idle (no user request) before a background sweep may run. 0 = run as soon as nothing else is active.",
    )
    reserved_api_calls: int = Field(
        DEFAULT_RESERVED_API_CALLS,
        ge=0,
        le=100_000,
        description="External-API calls (OpenAlex daily quota) a background sweep always leaves for your manual operations.",
    )


@router.get(
    "/governance",
    response_model=GovernanceSettings,
    summary="Background-ops governance knobs (idle-wait + API reserve)",
)
def get_governance_settings(
    db: sqlite3.Connection = Depends(get_db),
    _user: dict = Depends(get_current_user),
):
    return GovernanceSettings(
        idle_wait_minutes=get_idle_wait_minutes(db),
        reserved_api_calls=get_reserved_api_calls(db),
    )


@router.put(
    "/governance",
    response_model=GovernanceSettings,
    summary="Update background-ops governance knobs",
)
def update_governance_settings(
    body: GovernanceSettings,
    db: sqlite3.Connection = Depends(get_db),
    _user: dict = Depends(get_current_user),
):
    """Persist the two knobs (one gated write unit; SQLite write discipline). The
    gate reads them live per check, so no restart/reschedule is needed."""
    from alma.application.discovery import lens_crud

    def _apply() -> None:
        lens_crud.upsert_setting(db, IDLE_WAIT_MINUTES_KEY, str(body.idle_wait_minutes))
        lens_crud.upsert_setting(db, RESERVED_API_CALLS_KEY, str(body.reserved_api_calls))

    run_write_unit(db, _apply, label="background_governance_update")
    return body


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
    scope: str | None = Query(None),
    dry_run: bool | None = Query(None),
    max_items: int | None = Query(None, ge=1),
    request_batch_size: int | None = Query(None, ge=1),
    db: sqlite3.Connection = Depends(get_db),
    _user: dict = Depends(get_current_user),
):
    task = maintenance.REGISTRY.get(key)
    if task is None:
        raise HTTPException(status_code=404, detail=f"Unknown maintenance task: {key}")
    try:
        spec = MaintenanceRunSpec(
            max_items=max_items or maintenance.get_task_manual_limit(db, task),
            scope=scope,
            dry_run=bool(dry_run) if dry_run is not None else bool(task.supports_dry_run),
            request_batch_size=request_batch_size,
        )
        plan = maintenance.plan_task(db, task, spec, health_payload=_health_payload(db))
    except MaintenanceValidationError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc
    # One plan, one ETA: ``plan.to_wire()`` already carries the bounded-run ETA
    # (computed from the same ``selected``/scope/batch the launch will use), so
    # there is no secondary whole-backlog recomputation here.
    payload = plan.to_wire()
    payload["key"] = key
    return payload


class RunMaintenanceRequest(BaseModel):
    """Atomic run controls. The visible values travel with the Run click."""

    target_ids: list[str] = Field(default_factory=list)
    max_items: int | None = Field(default=None, ge=1)
    request_batch_size: int | None = Field(default=None, ge=1)
    scope: str | None = None
    dry_run: bool = False
    force: bool = False
    confirmation_token: str | None = None
    plan_fingerprint: str | None = None


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
    body: RunMaintenanceRequest | None = None,
    db: sqlite3.Connection = Depends(get_db),
    _user: dict = Depends(get_current_user),
):
    task = maintenance.REGISTRY.get(key)
    if task is None:
        raise HTTPException(status_code=404, detail=f"Unknown maintenance task: {key}")
    request = body or RunMaintenanceRequest()
    try:
        spec = MaintenanceRunSpec(
            trigger=MaintenanceTrigger.USER,
            target_ids=request.target_ids,
            max_items=request.max_items or maintenance.get_task_manual_limit(db, task),
            request_batch_size=request.request_batch_size,
            scope=request.scope,
            dry_run=request.dry_run,
            force=request.force,
            confirmation_token=request.confirmation_token,
            plan_fingerprint=request.plan_fingerprint,
        )
        outcome = maintenance.run_task_now(db, task, spec=spec)
    except MaintenanceValidationError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc
    return {
        "key": key,
        "status": outcome.status,
        "job_id": outcome.job_id,
        "message": outcome.message,
        "plan": outcome.plan.to_wire(),
    }


class MaintenanceConfigRequest(BaseModel):
    """Partial update — only provided fields change."""

    auto_enabled: bool | None = Field(default=None, description="Allow safe idle repair")
    auto_daily_cap: int | None = Field(default=None, ge=1)
    remembered_manual_limit: int | None = Field(default=None, ge=1)
    request_batch_size: int | None = Field(default=None, ge=1)


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
    try:
        maintenance.set_task_config(
            db,
            task,
            auto_enabled=body.auto_enabled,
            auto_daily_cap=body.auto_daily_cap,
            remembered_manual_limit=body.remembered_manual_limit,
            request_batch_size=body.request_batch_size,
        )
    except MaintenanceValidationError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc
    return maintenance.describe_task(db, task, _health_payload(db))


def _health_payload(db: sqlite3.Connection) -> dict:
    from alma.application import materialized_views as mv
    from alma.services import health as health_service

    return (mv.get(db, health_service.HEALTH_CORPUS_VIEW_KEY).get("payload")) or {}


@router.get(
    "/threads",
    summary="Dump all backend thread stacks (diagnostic)",
    description=(
        "Read-only diagnostic for write-lock stalls: returns every live "
        "thread's name + current stack so the holder of the SQLite writer "
        "(or the process writer gate) can be identified while reads still "
        "flow. No DB access — usable even when the writer is wedged."
    ),
)
def dump_thread_stacks():
    import sys
    import threading
    import traceback

    frames = sys._current_frames()
    out = []
    for thread in threading.enumerate():
        frame = frames.get(thread.ident)
        out.append(
            {
                "name": thread.name,
                "daemon": thread.daemon,
                "stack": traceback.format_stack(frame) if frame else [],
            }
        )
    return {"threads": out}
