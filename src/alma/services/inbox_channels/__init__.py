"""Delivery channels that feed the Inbox.

Each module here adapts one transport to the `application.inbox_schema`
contract — fetch messages, acknowledge outcomes — and nothing more. Resolution,
corpus landing and Inbox membership all live in
`application.inbound_capture`, so a new channel is an adapter, never a
pipeline.

Registration is not here. Every optional subsystem is declared once in the
runtime plugin catalogue with capability flags. This module holds inbound
adapters; ``alma.plugins.registry`` owns the list.
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
    from alma.plugins.registry import get_plugin_registry

    return get_plugin_registry().inbound_channels()
