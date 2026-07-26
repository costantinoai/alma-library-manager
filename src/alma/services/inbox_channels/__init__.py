"""Delivery channels that feed the Inbox.

Each module here adapts one transport to the `application.inbox_schema`
contract — fetch messages, acknowledge outcomes — and nothing more. Resolution,
corpus landing and Inbox membership all live in
`application.inbound_capture`, so a new channel is an adapter, never a
pipeline.

Registration is not here. Every channel ALMa knows about — in EITHER direction —
is declared once in :mod:`alma.channels`, with a capability flag saying which
directions it supports. This module holds the inbound *adapters*; that module
holds the list.
"""

from __future__ import annotations

import logging

from alma.application.inbox_schema import InboundChannel

logger = logging.getLogger(__name__)


def available_channels() -> list[InboundChannel]:
    """Every receive-capable channel that is configured enough to poll.

    Unconfigured channels are skipped silently — "AI is opt-in" generalises
    here: a Slack token that was never set is not an error, the channel is
    simply off. A channel that RAISES while reporting its own configuration is
    logged at WARNING by the registry rather than being read as "off".
    """
    from alma.channels import get_channel_registry

    return get_channel_registry().inbound_channels()
