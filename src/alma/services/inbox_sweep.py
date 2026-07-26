"""Poll every configured Inbox channel and capture what it delivers.

The runner that joins `services.inbox_channels` (transport) to
`application.inbound_capture` (pipeline). Scheduled periodically and runnable
on demand from Settings.

**Write discipline.** Each message is resolved — network I/O, potentially slow —
entirely OUTSIDE any transaction, then landed in a short `write_section`. Rule 2
of the SQLite discipline: never hold a write txn across network I/O. One write
window per message rather than one per sweep, so a slow upstream on message 7
never holds the writer gate for messages 1–6, and a crash mid-sweep leaves every
already-captured message durably recorded.

Acknowledgement happens after the commit, for the same reason: talking to Slack
is network I/O and must not sit inside a write window.
"""

from __future__ import annotations

import logging
import sqlite3
from typing import Any

from alma.application.inbound_capture import (
    find_recorded,
    latest_cursor,
    persist_capture,
    record_message,
    resolve_message,
)
from alma.application.inbox_schema import CaptureResult, InboundChannel, InboundMessage
from alma.core.db_write import write_section

logger = logging.getLogger(__name__)


def sweep_channel(
    conn: sqlite3.Connection,
    channel: InboundChannel,
    *,
    limit: int | None = None,
) -> dict[str, Any]:
    """Poll one channel and capture everything new. Returns a count summary.

    Never raises for one bad message: a message that fails is recorded with
    ``outcome='error'`` and the sweep continues, because one unparseable link
    must not block every capture behind it.
    """
    summary: dict[str, Any] = {
        "channel": channel.name,
        "fetched": 0,
        "resolved": 0,
        "duplicate": 0,
        "unresolved": 0,
        "error": 0,
        "skipped_already_seen": 0,
    }

    cursor = latest_cursor(conn, channel=channel.name)
    try:
        messages = channel.fetch(since_cursor=cursor)
    except Exception as exc:
        logger.warning("Inbox fetch failed for %s: %s", channel.name, exc)
        summary["fetch_error"] = str(exc)
        return summary

    if limit is not None:
        messages = messages[: max(0, int(limit))]
    summary["fetched"] = len(messages)

    for message in messages:
        # At-least-once delivery: a message we already processed is a no-op.
        # Checked per message rather than trusting the cursor alone, because a
        # channel may legitimately over-fetch when its API can't express an
        # exact cursor.
        if find_recorded(conn, channel=message.channel, external_id=message.external_id):
            summary["skipped_already_seen"] += 1
            continue

        result = _capture_one(conn, message)
        summary[result.outcome] = summary.get(result.outcome, 0) + 1

        # Post-commit, outside any write window — this is network I/O.
        try:
            channel.acknowledge(message, result)
        except Exception as exc:  # pragma: no cover - best effort by contract
            logger.debug("Inbox ack failed for %s: %s", message.external_id, exc)

    return summary


def _capture_one(conn: sqlite3.Connection, message: InboundMessage) -> CaptureResult:
    """Resolve ONE message, then land it and record it in a single write window.

    The two-phase split is load-bearing, not stylistic:

    * `resolve_message` is the network half and runs with no transaction open,
      per write-discipline rule 2.
    * `persist_capture` + `record_message` share ONE `write_section`, so the
      paper row and its ledger row commit atomically. They must not be split
      across two windows — `write_section` opens with a `conn.rollback()`, so a
      paper written before the section would be silently discarded when the
      section for the ledger row opened. (Caught by
      `test_sweep_captures_through_a_real_channel`, which saw the paper land at
      `tracked` because the promote had been rolled back.)
    """
    resolved = resolve_message(message)

    try:
        with write_section(conn, label=f"inbox_capture:{message.channel}"):
            result = persist_capture(conn, message, resolved)
            record_message(conn, message, result)
    except Exception as exc:
        # Nothing committed, so the next sweep retries this message cleanly.
        # Loud: a silently unrecorded message is how a capture loop starts
        # duplicating papers.
        logger.warning(
            "Could not capture inbox message %s/%s: %s",
            message.channel,
            message.external_id,
            exc,
        )
        return CaptureResult(
            outcome="error", extracted=resolved.extracted, error=str(exc)
        )

    return result


def run_inbox_sweep(
    conn: sqlite3.Connection,
    *,
    limit: int | None = None,
) -> dict[str, Any]:
    """Poll every configured channel. Safe to call when none are set up."""
    from alma.services.inbox_channels import available_channels

    channels = available_channels()
    if not channels:
        logger.info("Inbox sweep: no channels configured")
        return {"channels": [], "captured": 0}

    per_channel = [sweep_channel(conn, channel, limit=limit) for channel in channels]
    return {
        "channels": per_channel,
        "captured": sum(int(entry.get("resolved") or 0) for entry in per_channel),
    }
