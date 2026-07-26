"""Slack as an Inbox delivery channel — send yourself a paper from your phone.

You paste a link into a private `#alma-inbox` channel; ALMa polls it, resolves
the paper, parks it in the Inbox and reacts on your message so you can see it
landed without opening the app.

**Why polling and not the Events API.** Slack's normal "app receives messages"
design HTTP-POSTs to your server, which needs a public HTTPS endpoint. ALMa
binds `127.0.0.1` with no auth by default (see docker-compose.yml), so there is
nothing for Slack to reach. Polling is outbound-only: no port, no tunnel, no
public surface. Socket Mode would also work outbound and would be instant, but
it needs a second app-level token and a supervised websocket — worth doing later
if the poll interval ever feels slow, and it slots in as another
:class:`InboundChannel` without touching the capture pipeline.

**Why a channel and not your self-DM.** A bot token (`xoxb`) can only read
conversations the bot is a member of, and Slack gives it no way into the DM you
have with yourself. Reading a self-DM needs a user token (`xoxp` + `im:history`),
which can read your entire Slack — a much broader credential for a personal
tool. A private channel the bot is invited to costs one habit change and keeps
the token narrow.

Required bot scopes, on top of the `chat:write` alerts already use:
``groups:history`` + ``groups:read`` (private channel) or ``channels:history`` +
``channels:read`` (public), and ``reactions:write`` for the receipt.
"""

from __future__ import annotations

import logging
from typing import Any

from alma.application.inbox_schema import CaptureResult, InboundMessage

logger = logging.getLogger(__name__)

CHANNEL_NAME = "slack"

#: Settings key holding the channel to poll (name or Slack ID). Capture stays
#: OFF until this is set — deliberately separate from `slack_channel`, which is
#: where alerts are POSTED. Reading your alert channel back in would re-ingest
#: ALMa's own notifications.
SETTING_INBOX_CHANNEL = "slack_inbox_channel"

#: Emoji receipt per outcome. The point of the reaction is that the loop closes
#: on the phone: you flick a link and watch it land, without opening ALMa.
REACTIONS: dict[str, str] = {
    "resolved": "inbox_tray",
    "duplicate": "books",
    "unresolved": "question",
    "error": "warning",
}

#: Slack returns at most this many messages per `conversations.history` page.
#: One page per sweep is plenty for a personal capture channel; anything older
#: is picked up on the next sweep because the cursor only advances over
#: messages actually recorded.
FETCH_LIMIT = 100


class SlackInboxChannel:
    """Reads a Slack channel and hands messages to the capture pipeline.

    Implements `application.inbox_schema.InboundChannel`. Knows nothing about
    papers, DOIs or the Inbox — it fetches and it acknowledges.
    """

    name = CHANNEL_NAME

    def __init__(self, notifier=None, channel: str | None = None) -> None:
        self._notifier = notifier
        self._channel = channel
        self._channel_id: str | None = None

    # -- configuration ----------------------------------------------------

    def _get_notifier(self):
        if self._notifier is None:
            from alma.slack.client import get_slack_notifier

            self._notifier = get_slack_notifier()
        return self._notifier

    def _target(self) -> str:
        """The configured channel to poll, or '' when capture is off."""
        if self._channel is not None:
            return self._channel
        try:
            from alma.config import get_setting

            return str(get_setting(SETTING_INBOX_CHANNEL) or "").strip()
        except Exception:
            return ""

    def is_configured(self) -> bool:
        """Both a token AND an explicitly nominated channel are required."""
        try:
            return bool(self._get_notifier().is_configured()) and bool(self._target())
        except Exception:
            return False

    def _resolve_channel_id(self) -> str:
        if self._channel_id is None:
            self._channel_id = self._get_notifier().resolve_channel_id(self._target())
        return self._channel_id

    # -- InboundChannel ---------------------------------------------------

    def fetch(self, *, since_cursor: str | None) -> list[InboundMessage]:
        """Messages newer than ``since_cursor``, oldest first.

        ``since_cursor`` is a Slack `ts`, which doubles as both the message id
        and its timestamp — so it maps straight onto `conversations.history`'s
        ``oldest``. That parameter is INCLUSIVE, so the cursor message itself
        comes back and is filtered out here; re-fetching it would be harmless
        anyway (the pipeline dedupes on `(channel, external_id)`), but there is
        no reason to pay for it.

        Bot messages are skipped, including ALMa's own — otherwise a reply we
        posted would be read back as a capture on the next sweep.
        """
        client = self._get_notifier().get_client()
        channel_id = self._resolve_channel_id()

        params: dict[str, Any] = {"channel": channel_id, "limit": FETCH_LIMIT}
        if since_cursor:
            params["oldest"] = since_cursor

        response = client.conversations_history(**params)
        raw_messages = list(response.get("messages") or [])
        # Slack returns newest-first; capture in the order they were sent.
        raw_messages.reverse()

        messages: list[InboundMessage] = []
        for raw in raw_messages:
            ts = str(raw.get("ts") or "").strip()
            if not ts or ts == since_cursor:
                continue
            # Skip bots (ALMa's own receipts and any other app) and the
            # channel-join / topic-change noise Slack files as messages.
            if raw.get("bot_id") or raw.get("subtype"):
                continue

            messages.append(
                InboundMessage(
                    channel=self.name,
                    external_id=ts,
                    received_at=_ts_to_iso(ts),
                    text=str(raw.get("text") or ""),
                    urls=tuple(_links_from(raw)),
                    metadata={
                        "channel_id": channel_id,
                        "slack_ts": ts,
                        "user": str(raw.get("user") or ""),
                    },
                )
            )
        return messages

    def acknowledge(self, message: InboundMessage, result: CaptureResult) -> None:
        """React on the source message; reply in-thread when it needs attention.

        Best-effort by contract: the paper is already saved and the message
        already recorded, so a failed acknowledgement must never fail the
        capture. Slack also rejects a duplicate reaction (`already_reacted`),
        which is expected on a replay and equally harmless.
        """
        channel_id = str(message.metadata.get("channel_id") or "")
        if not channel_id:
            return

        try:
            client = self._get_notifier().get_client()
        except Exception as exc:
            logger.debug("Slack ack skipped (no client): %s", exc)
            return

        emoji = REACTIONS.get(result.outcome)
        if emoji:
            try:
                client.reactions_add(
                    channel=channel_id, timestamp=message.external_id, name=emoji
                )
            except Exception as exc:
                logger.debug("Slack reaction failed for %s: %s", message.external_id, exc)

        # Only speak up when something needs the user: a silent ✅ is the goal.
        if result.outcome in {"unresolved", "error"} and result.error:
            try:
                client.chat_postMessage(
                    channel=channel_id,
                    thread_ts=message.external_id,
                    text=f"Couldn't add this to your reading inbox — {result.error}",
                )
            except Exception as exc:
                logger.debug("Slack thread reply failed: %s", exc)


def _links_from(raw: dict) -> list[str]:
    """URLs Slack already parsed out of the message.

    Slack sends links as `<https://…|label>` in `text` and, separately, as
    structured blocks. Its own parse beats a regex over the display form, so
    these are handed to the extractor alongside the raw text.
    """
    urls: list[str] = []

    for attachment in raw.get("attachments") or []:
        for key in ("original_url", "title_link", "from_url"):
            value = str((attachment or {}).get(key) or "").strip()
            if value:
                urls.append(value)

    def _walk(element: Any) -> None:
        if isinstance(element, dict):
            if element.get("type") == "link" and element.get("url"):
                urls.append(str(element["url"]).strip())
            for value in element.values():
                _walk(value)
        elif isinstance(element, list):
            for item in element:
                _walk(item)

    _walk(raw.get("blocks") or [])

    # Preserve order, drop repeats.
    return list(dict.fromkeys(url for url in urls if url))


def _ts_to_iso(ts: str) -> str:
    """Slack `ts` ("1753612800.001900") → ISO-8601 UTC.

    Never fabricates: an unparseable ts falls back to now, which is the honest
    "we know we received it, we don't know when it was sent".
    """
    from datetime import datetime, timezone

    try:
        return datetime.fromtimestamp(float(ts), tz=timezone.utc).isoformat()
    except (TypeError, ValueError):
        from alma.core.time import utcnow

        return utcnow().isoformat()
