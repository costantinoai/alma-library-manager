"""Inbox capture endpoints — status and a manual sweep.

The Inbox (D13) normally fills itself: a scheduler job polls the configured
delivery channels every few minutes. These endpoints exist so the loop is
**observable and forceable** rather than a black box you have to trust:

* ``GET  /inbox/status`` — is capture configured, what is waiting, what failed.
  Pure read; drives Settings and the Health card.
* ``POST /inbox/sweep``  — check the channels NOW instead of waiting for the
  next tick. Activity-backed, so a manual capture shows up in the operations
  log exactly like the scheduled one.

See `docs/concepts/inbox.md`.
"""

from __future__ import annotations

import logging
import sqlite3
from typing import Literal
from urllib.parse import urlparse

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel

from alma.api.deps import get_current_user, get_db
from alma.core.db_write import run_write_unit
from alma.core.operations import OperationOutcome, OperationRunner
from alma.core.operations.activity import record_foreground_action

logger = logging.getLogger(__name__)

router = APIRouter()


class CaptureMessageAction(BaseModel):
    """One explicit decision on a failed capture record."""

    action: Literal["archive", "retry"]
    replacement_link: str | None = None


@router.get(
    "/messages",
    summary="List capture messages that need attention (pure read)",
    description=(
        "Returns the durable failed-capture ledger for review. By default, "
        "archived records are omitted. Never writes."
    ),
)
def list_capture_messages(
    include_archived: bool = Query(False),
    limit: int = Query(100, ge=1, le=500),
    db: sqlite3.Connection = Depends(get_db),
    _user: dict = Depends(get_current_user),
):
    from alma.application.inbound_capture import list_attention_messages

    return list_attention_messages(
        db,
        include_archived=include_archived,
        limit=limit,
    )


@router.post(
    "/messages/{message_id}/action",
    summary="Archive or retry a failed capture",
    description=(
        "The single mutation route for capture review. Archive keeps the audit "
        "record but removes it from attention. Retry uses a replacement paper "
        "link, resolves outside the SQLite write window, and updates the same "
        "ledger record."
    ),
)
def apply_capture_message_action(
    message_id: str,
    payload: CaptureMessageAction,
    db: sqlite3.Connection = Depends(get_db),
    _user: dict = Depends(get_current_user),
):
    from alma.application.inbound_capture import (
        archive_attention_message,
        message_for_retry,
        persist_attention_retry,
        resolve_message,
    )

    try:
        if payload.action == "archive":
            result = run_write_unit(
                db,
                lambda: archive_attention_message(db, message_id),
                label="inbox_message_archive",
            )
            record_foreground_action(
                db,
                operation_key="inbox.capture.archive",
                message="Archived a capture that could not be identified",
                result=result,
            )
            return {"action": "archive", **result}

        replacement_link = str(payload.replacement_link or "").strip()
        parsed = urlparse(replacement_link)
        if parsed.scheme not in {"http", "https"} or not parsed.netloc:
            raise ValueError("Provide a complete http:// or https:// link to the paper")

        # Pure reads + upstream resolution happen before the write unit. The
        # short gated unit below contains only the paper + ledger writes.
        message = message_for_retry(db, message_id, replacement_link)
        resolved = resolve_message(message)
        capture = run_write_unit(
            db,
            lambda: persist_attention_retry(
                db,
                message_id=message_id,
                message=message,
                replacement_link=replacement_link,
                resolved=resolved,
            ),
            label="inbox_message_retry",
        )
        result = {
            "id": message_id,
            "action": "retry",
            "outcome": capture.outcome,
            "paper_id": capture.paper_id,
            "title": capture.title,
            "error": capture.error,
        }
        record_foreground_action(
            db,
            operation_key="inbox.capture.retry",
            message=(
                f"Retried a capture: {capture.title or capture.outcome}"
            ),
            result=result,
            status="completed" if capture.outcome in {"resolved", "duplicate"} else "failed",
        )
        return result
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except LookupError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@router.get(
    "/status",
    summary="Inbox capture status (pure read)",
    description=(
        "Whether any delivery channel is configured, how many papers are "
        "waiting in the Inbox, and how many captured messages failed to "
        "resolve and need a human. Never writes."
    ),
)
def inbox_status(
    db: sqlite3.Connection = Depends(get_db),
    _user: dict = Depends(get_current_user),
):
    from alma.api.helpers import table_exists
    from alma.application.library import INBOX_STATUS

    # Channel discovery is import-safe and self-gating: an unconfigured
    # channel is simply absent, not an error.
    try:
        from alma.services.inbox_channels import available_channels

        channels = [channel.name for channel in available_channels()]
    except Exception as exc:  # pragma: no cover - defensive
        logger.debug("Inbox channel discovery failed: %s", exc)
        channels = []

    waiting = int(
        db.execute("SELECT COUNT(*) AS c FROM papers WHERE status = ?", (INBOX_STATUS,)).fetchone()[
            "c"
        ]
        or 0
    )

    unresolved = 0
    last_captured_at = None
    if table_exists(db, "inbox_messages"):
        unresolved = int(
            db.execute(
                """SELECT COUNT(*) AS c FROM inbox_messages
                   WHERE outcome IN ('unresolved', 'error')
                     AND archived_at IS NULL"""
            ).fetchone()["c"]
            or 0
        )
        row = db.execute("SELECT MAX(created_at) AS last FROM inbox_messages").fetchone()
        last_captured_at = row["last"] if row else None

    return {
        "configured": bool(channels),
        "channels": channels,
        "waiting": waiting,
        "needs_attention": unresolved,
        "last_captured_at": last_captured_at,
    }


@router.post(
    "/sweep",
    summary="Check capture channels now",
    description=(
        "Polls every configured delivery channel immediately instead of "
        "waiting for the next scheduled tick. Idempotent: messages already "
        "captured are skipped on their `(channel, external_id)` key, so "
        "pressing this twice cannot duplicate a paper."
    ),
)
def sweep_inbox_now(
    db: sqlite3.Connection = Depends(get_db),
    user: dict = Depends(get_current_user),
):
    from alma.services.inbox_channels import available_channels
    from alma.services.inbox_sweep import run_inbox_sweep

    if not available_channels():
        raise HTTPException(
            status_code=400,
            detail=(
                "No capture channel is configured. Set a Slack capture channel "
                "in Settings → Plugins first."
            ),
        )

    runner = OperationRunner(db)

    def _handler(_ctx):
        result = run_inbox_sweep(db)
        captured = int(result.get("captured") or 0)
        errors = result.get("errors") or []
        if errors:
            # A channel we could not READ is a failure. Reporting "nothing new"
            # here would dress a hard error as success — the exact silent
            # failure this endpoint exists to prevent.
            raise SlackChannelError(_explain_channel_error(errors[0]["error"]))
        # A sweep that captured nothing still PROVED the connection — token,
        # scopes, channel and bot membership all worked. Say so: "nothing new"
        # alone reads like a failure the user has to go and diagnose.
        reached = [
            str(entry.get("target") or entry.get("channel"))
            for entry in (result.get("channels") or [])
            if entry.get("reachable")
        ]
        where = ", ".join(f"#{name.lstrip('#')}" for name in reached) or "your channels"
        if captured:
            message = f"Captured {captured} paper(s) from {where} — waiting in your Inbox"
        else:
            message = f"Connected to {where}. No new links to capture."
        return OperationOutcome(
            status="completed" if captured else "noop",
            message=message,
            result=result,
        )

    try:
        op = runner.run(
            operation_key="inbox.capture_sweep_manual",
            handler=_handler,
            trigger_source="user",
            actor=str(user.get("username") or "api_user"),
        )
    except SlackChannelError as exc:
        # 502: the failure is upstream (Slack rejected us), not a bad request.
        raise HTTPException(status_code=502, detail=str(exc)) from exc
    return op.get("result") or {"captured": 0, "channels": []}


class SlackChannelError(RuntimeError):
    """A capture channel could not be read. Carries user-facing guidance."""


#: Slack's error codes are precise but opaque. Each maps to exactly one thing
#: the user has to go and do, so say that instead of echoing the code.
_CHANNEL_ERROR_HELP: dict[str, str] = {
    "missing_scope": (
        "The Slack app is missing a permission. ALMa needs BOTH `channels:read` "
        "and `groups:read` to look a channel up by name, plus `groups:history` "
        "(private) or `channels:history` (public), and `reactions:write`. Add "
        "them under OAuth & Permissions, then click Reinstall to Workspace — "
        "scopes do nothing until the app is reinstalled."
    ),
    "not_in_channel": (
        "The bot is not a member of that channel. Open it in Slack and send "
        "`/invite @ALMa` (use your bot's name). Installing the app does not "
        "join any channel by itself."
    ),
    "channel_not_found": (
        "No channel by that name is visible to the bot. Check the spelling in "
        "Settings (use the bare name, no `#`), and make sure the bot has been "
        "invited to it — a private channel the bot has not joined is invisible "
        "to it. You can also paste the channel ID (starts with `C`) instead."
    ),
    "invalid_auth": (
        "Slack rejected the bot token. Re-copy the Bot User OAuth Token from "
        "OAuth & Permissions into Settings → Plugins."
    ),
    "account_inactive": (
        "The Slack app was uninstalled or disabled. Reinstall it to your "
        "workspace, then re-copy the token."
    ),
    "ratelimited": (
        "Slack is rate-limiting ALMa. Wait a minute and try again; the "
        "scheduled sweep will catch up on its own."
    ),
}


def _explain_channel_error(raw: str) -> str:
    """Turn a Slack error into the one action that fixes it."""
    text = str(raw or "").strip()
    lowered = text.lower()
    for code, help_text in _CHANNEL_ERROR_HELP.items():
        if code in lowered:
            return f"{help_text} (Slack said: {code})"
    return f"Could not read your capture channel. Slack said: {text}"
