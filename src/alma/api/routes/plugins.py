"""Delivery-channel API endpoints — what can reach you, and can it reach you now.

Serves :mod:`alma.channels`, the one registry that knows every channel and which
directions it supports. The path stays ``/plugins`` because that is what the
Settings UI and the health card already call; the object underneath is a channel
descriptor, not a plugin instance.

Configuration is NOT written here. Slack and email are configured in Settings,
which owns the secret store; a second config writer on this router is how the
bot token ended up mirrored into ``config/slack.json`` in plaintext. Reads and
connectivity tests only.
"""

import logging
from datetime import datetime

from fastapi import APIRouter, Depends, HTTPException, status

from alma.api.deps import get_current_user
from alma.api.models import ErrorResponse, PluginInfo, PluginTestResult
from alma.core.redaction import redact_sensitive_text
from alma.core.time import utcnow

logger = logging.getLogger(__name__)

router = APIRouter(
    prefix="/plugins",
    tags=["plugins"],
    responses={
        401: {"model": ErrorResponse, "description": "Unauthorized"},
        500: {"model": ErrorResponse, "description": "Internal Server Error"},
    }
)


def _get_registry():
    # Resolve at call time so tests can monkeypatch alma.api.deps.get_plugin_registry
    from alma.api.deps import get_plugin_registry as _gpr
    return _gpr()


def _to_plugin_info(described: dict) -> PluginInfo:
    """Descriptor → API model.

    ``is_healthy`` is deliberately None: health is what the last CONNECTIVITY
    TEST found, and the registry holds no per-process instance to remember it
    (that cache is what let a stale plugin object outlive the token it was built
    from). A channel reports configured-or-not here; whether it works is what
    ``POST /plugins/{name}/test`` is for.
    """
    return PluginInfo(
        name=described["name"],
        display_name=described["display_name"],
        version=described["version"],
        description=described["description"],
        config_schema=described["config_schema"],
        capabilities=described["capabilities"],
        is_configured=described["is_configured"],
        can_send=described["can_send"],
        can_receive=described["can_receive"],
        is_healthy=None,
    )


@router.get(
    "",
    response_model=list[PluginInfo],
    summary="List all delivery channels",
    description=(
        "Every channel ALMa knows about, with the directions it supports "
        "(`send` / `receive`) and whether each direction is configured."
    ),
)
def list_plugins(user: dict = Depends(get_current_user)):
    """List all registered delivery channels.

    Example:
        ```bash
        curl http://localhost:8000/api/v1/plugins
        ```
    """
    try:
        return [_to_plugin_info(entry) for entry in _get_registry().describe_all()]
    except Exception as e:
        logger.error("Error listing channels: %s", redact_sensitive_text(str(e)))
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to list plugins"
        )


@router.get(
    "/{plugin_name}",
    response_model=PluginInfo,
    summary="Get channel details",
    description="Retrieve detailed information about a specific delivery channel.",
)
def get_plugin(plugin_name: str, user: dict = Depends(get_current_user)):
    """Get detailed information about one channel.

    Raises:
        HTTPException: 404 if the channel is not registered.
    """
    try:
        return _to_plugin_info(_get_registry().get(plugin_name).describe())
    except KeyError:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Plugin '{plugin_name}' not found"
        )
    except Exception as e:
        logger.error(
            "Error retrieving channel %s: %s",
            plugin_name,
            redact_sensitive_text(str(e)),
        )
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to retrieve plugin"
        )


@router.post(
    "/{plugin_name}/test",
    summary="Test channel connection",
    description=(
        "Test if the channel can reach its service. Slack and email run "
        "asynchronously through the Activity envelope so the exact code path "
        "that delivers real alerts is what gets exercised."
    ),
)
def test_plugin_connection(
    plugin_name: str,
    user: dict = Depends(get_current_user),
):
    """Test channel connectivity.

    For ``slack`` / ``email``: queue an Activity-enveloped job that sends a real
    test message through the same notifier real alerts use, and return the
    canonical activity envelope so the frontend can poll progress.

    Any other registered channel falls back to its plugin's synchronous
    ``test_connection()``.
    """
    if plugin_name == "slack":
        return _slack_test_envelope(user)

    if plugin_name == "email":
        return _email_test_envelope(user)

    try:
        descriptor = _get_registry().get(plugin_name)
    except KeyError:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Plugin '{plugin_name}' not found",
        )

    plugin = descriptor.outbound_plugin()
    if plugin is None:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Plugin '{plugin_name}' is not configured",
        )

    try:
        success = plugin.test_connection()
    except Exception as e:
        logger.error(
            "Error testing channel %s: %s",
            plugin_name,
            redact_sensitive_text(str(e)),
        )
        success = False

    return PluginTestResult(
        success=success,
        message="Connection test successful" if success else "Connection test failed",
        timestamp=datetime.now().isoformat(),
    )


def _slack_test_envelope(user: dict) -> dict:
    """Queue a Slack connectivity test through the Activity envelope.

    Validates the token + channel synchronously (so the user gets a 4xx
    immediately when nothing is configured) and runs the actual
    ``chat.postMessage`` call on the scheduler thread pool.
    """
    import asyncio
    import uuid

    from alma.api.scheduler import (
        activity_envelope,
        find_active_job,
        schedule_immediate,
        set_job_status,
    )
    from alma.slack.client import get_slack_notifier

    operation_key = "alerts.slack.test"

    # Synchronous validation -> fast 4xx for misconfiguration.
    notifier = get_slack_notifier()
    if not notifier.is_configured:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Slack token not configured. Set it in Settings -> Channels.",
        )
    try:
        notifier.resolve_channel(None)
    except ValueError as exc:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(exc),
        )

    # Coalesce duplicate clicks while a test is already in flight.
    existing = find_active_job(operation_key)
    if existing:
        return activity_envelope(
            existing["job_id"],
            status=str(existing.get("status") or "running"),
            operation_key=operation_key,
            message="Slack test already in progress",
            already_running=True,
        )

    job_id = f"alerts_slack_test_{uuid.uuid4().hex[:10]}"
    actor = str(user.get("username") or "api_user")
    set_job_status(
        job_id,
        status="queued",
        operation_key=operation_key,
        trigger_source="user",
        actor=actor,
        started_at=utcnow().isoformat(),
        message="Sending Slack test message",
    )

    def _runner() -> dict:
        from alma.slack.client import SlackResolveError

        # Open a fresh notifier inside the worker thread so the cache is
        # not shared with the request thread (per `lessons.md:1042`).
        local_notifier = get_slack_notifier()
        target = local_notifier.resolve_channel(None)

        # Resolve eagerly so a bad channel name surfaces with a precise
        # "channel_not_found" message instead of a generic "API ok=false".
        try:
            resolved_id = local_notifier._resolve_target(target)
        except SlackResolveError as exc:
            return {
                "ok": False,
                "target": target,
                "error": str(exc),
                "message": f"Could not resolve Slack target {target!r}: {exc}",
            }

        try:
            ok = asyncio.run(local_notifier.send_test_message())
        except Exception as exc:  # pragma: no cover - network failures
            return {
                "ok": False,
                "target": target,
                "error": str(exc),
                "message": f"Slack test failed: {exc}",
            }
        if ok:
            return {
                "ok": True,
                "target": target,
                "resolved_id": resolved_id,
                "message": f"Slack test message delivered to {target}",
            }
        return {
            "ok": False,
            "target": target,
            "message": (
                "Slack API rejected the test message "
                f"(resolved to {resolved_id}) -- check the bot's chat:write scope."
            ),
        }

    schedule_immediate(job_id, _runner)
    return activity_envelope(
        job_id,
        status="queued",
        operation_key=operation_key,
        message="Slack test queued",
    )


def _email_test_envelope(user: dict) -> dict:
    """Queue an SMTP connectivity test through the Activity envelope.

    Mirrors :func:`_slack_test_envelope`: validate config synchronously (fast
    4xx when nothing is set up), then send a real test email on the scheduler
    pool via the same :func:`alma.mailer.client.get_email_notifier` the digest
    delivery path uses.
    """
    import asyncio
    import uuid

    from alma.api.scheduler import (
        activity_envelope,
        find_active_job,
        schedule_immediate,
        set_job_status,
    )
    from alma.mailer.client import get_email_notifier

    operation_key = "alerts.email.test"

    notifier = get_email_notifier()
    if not notifier.is_configured:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Email not configured. Set SMTP host, from, and recipients in Settings -> Email digests.",
        )

    existing = find_active_job(operation_key)
    if existing:
        return activity_envelope(
            existing["job_id"],
            status=str(existing.get("status") or "running"),
            operation_key=operation_key,
            message="Email test already in progress",
            already_running=True,
        )

    job_id = f"alerts_email_test_{uuid.uuid4().hex[:10]}"
    actor = str(user.get("username") or "api_user")
    set_job_status(
        job_id,
        status="queued",
        operation_key=operation_key,
        trigger_source="user",
        actor=actor,
        started_at=utcnow().isoformat(),
        message="Sending test email",
    )

    def _runner() -> dict:
        # Fresh notifier inside the worker thread (don't share request-thread state).
        local_notifier = get_email_notifier()
        try:
            ok = asyncio.run(local_notifier.send_test_message())
        except Exception as exc:  # pragma: no cover - network failures
            return {"ok": False, "error": str(exc), "message": f"Email test failed: {exc}"}
        if ok:
            recipients = ", ".join(local_notifier.resolve_recipients())
            return {"ok": True, "target": recipients, "message": f"Test email sent to {recipients}"}
        return {"ok": False, "message": "SMTP rejected the test email — check host, port, and credentials."}

    schedule_immediate(job_id, _runner)
    return activity_envelope(
        job_id,
        status="queued",
        operation_key=operation_key,
        message="Email test queued",
    )
