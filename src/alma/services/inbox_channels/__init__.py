"""Delivery channels that feed the Inbox.

Each module here adapts one transport to the `application.inbox_schema`
contract — fetch messages, acknowledge outcomes — and nothing more. Resolution,
corpus landing and Inbox membership all live in
`application.inbound_capture`, so a new channel is an adapter, never a
pipeline.

Registration is explicit rather than auto-discovered: a channel that is not
listed is not polled, and reading this list tells you exactly what can put a
paper in your Inbox.
"""

from __future__ import annotations

import logging

from alma.application.inbox_schema import InboundChannel

logger = logging.getLogger(__name__)


def available_channels() -> list[InboundChannel]:
    """Every channel that is configured enough to poll.

    Unconfigured channels are skipped silently — "AI is opt-in" generalises
    here: a Slack token that was never set is not an error, the channel is
    simply off. A channel whose import fails is logged and skipped rather than
    taking the whole sweep down with it.
    """
    channels: list[InboundChannel] = []

    try:
        from alma.services.inbox_channels.slack import SlackInboxChannel

        slack = SlackInboxChannel()
        if slack.is_configured():
            channels.append(slack)
    except Exception as exc:  # pragma: no cover - defensive
        logger.warning("Slack inbox channel unavailable: %s", exc)

    return channels
