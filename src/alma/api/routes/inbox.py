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

from fastapi import APIRouter, Depends, HTTPException

from alma.api.deps import get_current_user, get_db
from alma.core.operations import OperationOutcome, OperationRunner

logger = logging.getLogger(__name__)

router = APIRouter()


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
        db.execute(
            "SELECT COUNT(*) AS c FROM papers WHERE status = ?", (INBOX_STATUS,)
        ).fetchone()["c"]
        or 0
    )

    unresolved = 0
    last_captured_at = None
    if table_exists(db, "inbox_messages"):
        unresolved = int(
            db.execute(
                "SELECT COUNT(*) AS c FROM inbox_messages "
                "WHERE outcome IN ('unresolved', 'error')"
            ).fetchone()["c"]
            or 0
        )
        row = db.execute(
            "SELECT MAX(created_at) AS last FROM inbox_messages"
        ).fetchone()
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
                "in Settings → Channels first."
            ),
        )

    runner = OperationRunner(db)

    def _handler(_ctx):
        result = run_inbox_sweep(db)
        captured = int(result.get("captured") or 0)
        return OperationOutcome(
            status="completed" if captured else "noop",
            message=(
                f"Captured {captured} paper(s) to your Inbox"
                if captured
                else "No new papers in your capture channels"
            ),
            result=result,
        )

    op = runner.run(
        operation_key="inbox.capture_sweep_manual",
        handler=_handler,
        trigger_source="user",
        actor=str(user.get("username") or "api_user"),
    )
    return op.get("result") or {"captured": 0, "channels": []}
