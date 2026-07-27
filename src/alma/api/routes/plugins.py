"""Central external-integration routes.

The manifest registry owns discovery, activation, generated configuration
schema/storage, status, and actions. Alerts and Inbox remain core features;
integrations only implement their declared send/receive seams.
"""

from __future__ import annotations

import asyncio
import logging
import sqlite3
import uuid

from fastapi import APIRouter, Depends, HTTPException, status
from pydantic import BaseModel, ConfigDict, ValidationError

from alma.api.deps import get_current_user, get_db
from alma.api.models import ErrorResponse, PluginConfigResponse, PluginInfo
from alma.core.redaction import redact_sensitive_text
from alma.core.time import utcnow

logger = logging.getLogger(__name__)

router = APIRouter(
    prefix="/plugins",
    tags=["plugins"],
    responses={
        401: {"model": ErrorResponse, "description": "Unauthorized"},
        500: {"model": ErrorResponse, "description": "Internal Server Error"},
    },
)


def _get_registry():
    # Call-time resolution keeps the dependency replaceable in contract tests.
    from alma.api.deps import get_plugin_registry

    return get_plugin_registry()


def _to_plugin_info(described: dict) -> PluginInfo:
    return PluginInfo.model_validate(described)


def _manifest_or_404(plugin_id: str):
    try:
        return _get_registry().get(plugin_id)
    except KeyError as exc:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Plugin '{plugin_id}' not found",
        ) from exc


@router.get(
    "",
    response_model=list[PluginInfo],
    summary="List external integrations",
    description=(
        "Every registered external integration, its send/receive capabilities, "
        "generated configuration schema, activation, and direction status."
    ),
)
def list_plugins(
    db: sqlite3.Connection = Depends(get_db),
    _user: dict = Depends(get_current_user),
) -> list[PluginInfo]:
    try:
        return [_to_plugin_info(entry) for entry in _get_registry().describe_all(db)]
    except Exception as exc:
        logger.error(
            "Error listing integrations: %s",
            redact_sensitive_text(str(exc)),
        )
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to list integrations",
        ) from exc


@router.get(
    "/{plugin_id}",
    response_model=PluginInfo,
    summary="Get integration details",
)
def get_plugin(
    plugin_id: str,
    db: sqlite3.Connection = Depends(get_db),
    _user: dict = Depends(get_current_user),
) -> PluginInfo:
    return _to_plugin_info(_manifest_or_404(plugin_id).describe(db))


class PluginActivation(BaseModel):
    model_config = ConfigDict(extra="forbid")

    enabled: bool


@router.put(
    "/{plugin_id}/enabled",
    response_model=PluginInfo,
    summary="Activate or deactivate an integration",
    description=(
        "Deactivation retains credentials/configuration, stops capture polling "
        "and Alert delivery, and removes the integration's Home status pills."
    ),
)
def set_plugin_enabled(
    plugin_id: str,
    payload: PluginActivation,
    db: sqlite3.Connection = Depends(get_db),
    _user: dict = Depends(get_current_user),
) -> PluginInfo:
    manifest = _manifest_or_404(plugin_id)
    manifest.set_enabled(payload.enabled)
    return _to_plugin_info(manifest.describe(db))


@router.get(
    "/{plugin_id}/config",
    response_model=PluginConfigResponse,
    summary="Read validated integration configuration",
)
def get_plugin_config(
    plugin_id: str,
    db: sqlite3.Connection = Depends(get_db),
    _user: dict = Depends(get_current_user),
) -> PluginConfigResponse:
    manifest = _manifest_or_404(plugin_id)
    return PluginConfigResponse(plugin_id=plugin_id, config=manifest.config(db))


@router.put(
    "/{plugin_id}/config",
    response_model=PluginConfigResponse,
    summary="Validate and replace integration configuration",
)
def update_plugin_config(
    plugin_id: str,
    payload: dict,
    db: sqlite3.Connection = Depends(get_db),
    _user: dict = Depends(get_current_user),
) -> PluginConfigResponse:
    manifest = _manifest_or_404(plugin_id)
    try:
        config = manifest.save_config(db, payload)
    except ValidationError as exc:
        raise HTTPException(status_code=422, detail=exc.errors()) from exc
    return PluginConfigResponse(plugin_id=plugin_id, config=config)


@router.post(
    "/{plugin_id}/test",
    summary="Test an integration",
    description=(
        "Runs the integration manifest's registered connectivity test through "
        "the Activity envelope and the same transport used in production."
    ),
)
def test_plugin_connection(
    plugin_id: str,
    user: dict = Depends(get_current_user),
) -> dict:
    manifest = _manifest_or_404(plugin_id)
    if manifest.connection_tester is None:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Plugin '{plugin_id}' has no connectivity test",
        )

    current_status = manifest.status()
    if not current_status.get("configured"):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=(f"{manifest.display_name} is not configured. Finish it in Settings → Plugins."),
        )
    if manifest.can("send") and not current_status.get("can_send"):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=(
                f"{manifest.display_name} has no outbound target. Finish it in Settings → Plugins."
            ),
        )

    from alma.api.scheduler import (
        activity_envelope,
        find_active_job,
        schedule_immediate,
        set_job_status,
    )

    operation_key = f"integrations.{manifest.id}.test"
    existing = find_active_job(operation_key)
    if existing:
        return activity_envelope(
            existing["job_id"],
            status=str(existing.get("status") or "running"),
            operation_key=operation_key,
            message=f"{manifest.display_name} test already in progress",
            already_running=True,
        )

    job_id = f"integration_{manifest.id}_test_{uuid.uuid4().hex[:10]}"
    set_job_status(
        job_id,
        status="queued",
        operation_key=operation_key,
        trigger_source="user",
        actor=str(user.get("username") or "api_user"),
        started_at=utcnow().isoformat(),
        message=f"Testing {manifest.display_name}",
    )

    def _runner() -> dict:
        try:
            return asyncio.run(manifest.connection_tester())
        except Exception as exc:  # pragma: no cover - external boundary
            return {
                "ok": False,
                "error": str(exc),
                "message": f"{manifest.display_name} test failed: {exc}",
            }

    schedule_immediate(job_id, _runner)
    return activity_envelope(
        job_id,
        status="queued",
        operation_key=operation_key,
        message=f"{manifest.display_name} test queued",
    )
