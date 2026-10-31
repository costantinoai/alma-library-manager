"""Add-on list, switch and Remove.

Disable stops an add-on and keeps its data. Remove is a separate, explicit step
that deletes its tables, files, settings and credentials. It runs as an Activity
job on the pattern of the library reset: backup first, then a short write unit,
then file and settings deletes outside any transaction.
"""

from __future__ import annotations

import logging
import sqlite3
import uuid
from typing import Any

from fastapi import APIRouter, Depends, HTTPException, status
from pydantic import BaseModel, ConfigDict

from alma.api.deps import get_current_user, get_db, open_db_connection
from alma.application import materialized_views as mv
from alma.core import addons
from alma.core.db_write import write_section
from alma.core.time import utcnow

logger = logging.getLogger(__name__)

router = APIRouter(
    prefix="/addons",
    tags=["addons"],
    responses={401: {"description": "Unauthorized"}},
)

_OPERATIONAL_VIEW = "insights:diag:operational"
#: Backup, tables, files, settings: the phases Activity counts.
_REMOVE_PHASES = 4


class AddonSwitch(BaseModel):
    model_config = ConfigDict(extra="forbid")

    enabled: bool


class AddonRemove(BaseModel):
    model_config = ConfigDict(extra="forbid")

    confirm: str


def _addon_or_404(addon_id: str) -> addons.AddOn:
    try:
        return addons.get(addon_id)
    except addons.AddonNotFound as exc:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Not found") from exc


def _describe(db: sqlite3.Connection, addon: addons.AddOn) -> dict[str, Any]:
    return {
        "id": addon.id,
        "name": addon.name,
        "description": addon.description,
        "docs_path": addon.docs_path,
        "default_enabled": addon.default_enabled,
        "requires": list(addon.requires),
        "enabled": addons.is_enabled(addon.id),
        "blocked": addons.blocked_reason(addon.id),
        "has_data": bool(addon.has_user_data(db)),
        "disable_warning": addon.disable_warning(db) if addon.disable_warning else None,
    }


@router.get("", summary="List add-ons")
def list_addons(
    db: sqlite3.Connection = Depends(get_db),
    _user: dict = Depends(get_current_user),
) -> list[dict[str, Any]]:
    """Every add-on in this build, on or off."""
    return [_describe(db, addon) for addon in addons.present()]


@router.get("/{addon_id}", summary="One add-on and what Remove would delete")
def get_addon(
    addon_id: str,
    db: sqlite3.Connection = Depends(get_db),
    _user: dict = Depends(get_current_user),
) -> dict[str, Any]:
    addon = _addon_or_404(addon_id)
    return {**_describe(db, addon), "footprint": addons.footprint(db, addon_id)}


@router.put(
    "/{addon_id}/enabled",
    summary="Switch an add-on on or off",
    description=(
        "Takes effect at once: routes answer 404 and jobs leave the scheduler while "
        "off. Data, credentials and settings stay. A job already running finishes "
        "and is listed in `still_finishing`."
    ),
)
def set_addon_enabled(
    addon_id: str,
    payload: AddonSwitch,
    db: sqlite3.Connection = Depends(get_db),
    _user: dict = Depends(get_current_user),
) -> dict[str, Any]:
    addon = _addon_or_404(addon_id)
    try:
        addons.set_enabled(db, addon_id, payload.enabled)
    except addons.AddonConflict as exc:
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=str(exc)) from exc
    # Health's operational view reads add-on checks; it must not wait for its
    # hourly refresh to drop a check that was just switched off.
    try:
        mv.request_refresh(db, _OPERATIONAL_VIEW, force=True)
    except Exception as exc:  # noqa: BLE001
        logger.warning("Could not refresh the operational view: %s", exc)
    still_finishing = [] if payload.enabled else addons.active_operations(db, addon_id)
    return {**_describe(db, addon), "still_finishing": still_finishing}


def _run_remove(job_id: str, addon_id: str) -> dict[str, Any]:
    """Delete everything this add-on owns, backup first.

    A crash between the steps leaves orphan files or settings; running Remove
    again clears them, because each step tolerates work already done.
    """
    from alma.api.routes.library_mgmt import _create_backup
    from alma.api.scheduler import add_job_log, set_job_status

    phase = 0

    def _require_switch_off() -> None:
        # The switch may have moved since the route checked it.
        if addons.switch_is_on(addon_id):
            raise addons.AddonConflict(f"{addon_id} was switched on, Remove stopped")

    _require_switch_off()

    def _progress(step: str, message: str, data: dict[str, Any]) -> None:
        nonlocal phase
        phase += 1
        set_job_status(
            job_id, status="running", processed=phase, total=_REMOVE_PHASES, message=message
        )
        add_job_log(job_id, message, step=step, data=data)

    # A mistake here cannot be undone any other way.
    backup_name = _create_backup()["backup_name"]
    _progress("backup", f"Backup created: {backup_name}", {"backup_name": backup_name})

    conn = open_db_connection()
    try:
        with write_section(conn, label=f"addon remove {addon_id}"):
            _require_switch_off()
            tables = addons.drop_tables(conn, addon_id)
            core_rows = addons.delete_core_rows(conn, addon_id)
    finally:
        conn.close()
    _progress(
        "tables",
        f"Dropped {len(tables)} tables",
        {"tables": tables, "core_rows": core_rows},
    )

    # After the commit: slow file I/O never runs inside a write transaction.
    files = addons.remove_files(addon_id)
    _progress("files", f"Deleted {len(files)} folders", {"folders": files})

    removed = addons.remove_settings(addon_id)
    _progress("settings", "Deleted settings and credentials", removed)

    # A state on Health may name what just went, such as a removed provider.
    probe = open_db_connection()
    try:
        mv.request_refresh_after_write(probe, _OPERATIONAL_VIEW, label="addon remove")
    finally:
        probe.close()

    return {
        "backup_name": backup_name,
        "tables": tables,
        "core_rows": core_rows,
        "folders": files,
        **removed,
    }


@router.post(
    "/{addon_id}/remove",
    summary="Delete an add-on's data for good",
    description=(
        "Needs `confirm` to equal the add-on id, and the add-on switched off first "
        "(409 otherwise, and 409 while one of its jobs is active). Drops its tables, "
        "deletes the rows it declares in core tables, its files, settings and "
        "credentials, after a backup. Papers are never touched. Switching it on again "
        "starts empty."
    ),
)
def remove_addon(
    addon_id: str,
    payload: AddonRemove,
    db: sqlite3.Connection = Depends(get_db),
    user: dict = Depends(get_current_user),
) -> dict[str, Any]:
    from alma.api.scheduler import (
        activity_envelope,
        find_active_job,
        schedule_immediate,
        set_job_status,
    )

    addon = _addon_or_404(addon_id)
    if payload.confirm != addon_id:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Type {addon_id} to confirm",
        )
    if addons.switch_is_on(addon_id):
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail=f"Switch {addon.name} off before removing it",
        )
    active = addons.active_operations(db, addon_id)
    if active:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail=f"{addon.name} still has work running: {', '.join(active)}",
        )

    operation_key = addons.remove_operation_key(addon_id)
    existing = find_active_job(operation_key)
    if existing:
        return activity_envelope(
            str(existing.get("job_id") or ""),
            status="already_running",
            operation_key=operation_key,
            message=f"Removing {addon.name} is already running",
        )

    job_id = f"addon_remove_{addon_id}_{uuid.uuid4().hex[:10]}"
    set_job_status(
        job_id,
        status="queued",
        operation_key=operation_key,
        trigger_source="user",
        actor=str(user.get("username") or "api_user"),
        started_at=utcnow().isoformat(),
        processed=0,
        total=_REMOVE_PHASES,
        message=f"Queued removal of {addon.name}",
    )

    def _runner() -> None:
        try:
            summary = _run_remove(job_id, addon_id)
            set_job_status(
                job_id,
                status="completed",
                finished_at=utcnow().isoformat(),
                processed=_REMOVE_PHASES,
                total=_REMOVE_PHASES,
                message=f"Removed {addon.name}",
                result=summary,
            )
        except Exception as exc:  # pragma: no cover - runner crash path
            logger.exception("Removing add-on %s failed", addon_id)
            set_job_status(
                job_id,
                status="failed",
                finished_at=utcnow().isoformat(),
                message=f"Removing {addon.name} failed",
                error=str(exc),
            )

    schedule_immediate(job_id, _runner)
    return activity_envelope(
        job_id,
        status="queued",
        operation_key=operation_key,
        message=f"Queued removal of {addon.name}",
        total=_REMOVE_PHASES,
    )
