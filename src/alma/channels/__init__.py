"""One registry of delivery channels, with a capability per direction.

A **channel** is a service ALMa exchanges messages with. Each one declares what
it can do — ``send`` (ALMa posts to it), ``receive`` (it feeds the Inbox), or
both — and that declaration is the single greppable answer to "what can reach
me, and what can I reach?".

Two registries used to answer half of it each: ``plugins.registry.PluginRegistry``
knew about senders, ``services.inbox_channels.available_channels()`` about
receivers, and Slack appeared in both without either knowing. The capability
flag lives on the ENTRY here rather than on a fused base class, so the two
protocols stay separate and neither grows a speculative half:

* outbound → :class:`alma.plugins.base.MessagingPlugin` (send a rendered message)
* inbound  → :class:`alma.application.inbox_schema.InboundChannel` (fetch + ack)

Registration is EXPLICIT, deliberately — the same rule the inbound side already
had. A channel that is not in :data:`CHANNELS` cannot post to you and cannot put
a paper in your Inbox, and reading that list is how you know.

**Why email declares ``send`` with no plugin object.** Email delivery is owned by
:class:`alma.mailer.client.EmailNotifier` through the alerts engine; it has never
had a ``MessagingPlugin`` class. The capability is still true, so it is declared
true — a registry that hid email because of an implementation detail would be
exactly the lie the merge was meant to end. ``outbound_plugin()`` returns None
there, and the descriptor says so.
"""

from __future__ import annotations

import logging
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any

from alma.application.inbox_schema import InboundChannel
from alma.plugins.base import MessagingPlugin

logger = logging.getLogger(__name__)

#: What a channel can do. `send` = ALMa posts to it; `receive` = it feeds the Inbox.
Capability = str
SEND: Capability = "send"
RECEIVE: Capability = "receive"


@dataclass(frozen=True)
class ChannelDescriptor:
    """One channel: who it is, what it can do, and how to reach each half."""

    name: str
    display_name: str
    version: str
    description: str
    capabilities: tuple[Capability, ...]

    #: JSON schema for the channel's configuration, for config UIs.
    config_schema: dict[str, Any] = field(default_factory=dict)

    #: Build the configured outbound plugin, or None when this channel has no
    #: `MessagingPlugin` implementation / is not configured to send.
    outbound_factory: Callable[[], MessagingPlugin | None] | None = None

    #: Build the configured inbound adapter, or None when capture is off.
    inbound_factory: Callable[[], InboundChannel | None] | None = None

    #: Per-direction configuration status (`configured`, `can_send`, `can_receive`).
    status_factory: Callable[[], dict[str, Any]] | None = None

    def can(self, capability: Capability) -> bool:
        return capability in self.capabilities

    def status(self) -> dict[str, Any]:
        """Live configuration status. Never raises — status must stay readable."""
        if self.status_factory is None:
            return {"configured": False, "can_send": False, "can_receive": False}
        try:
            return dict(self.status_factory())
        except Exception as exc:  # pragma: no cover - defensive
            logger.warning("Channel %s could not report status: %s", self.name, exc)
            return {"configured": False, "can_send": False, "can_receive": False}

    def outbound_plugin(self) -> MessagingPlugin | None:
        """The configured sender, or None. Never raises."""
        if self.outbound_factory is None:
            return None
        try:
            return self.outbound_factory()
        except Exception as exc:
            logger.warning("Channel %s outbound unavailable: %s", self.name, exc)
            return None

    def inbound_channel(self) -> InboundChannel | None:
        """The configured receiver, or None. Never raises.

        An UNCONFIGURED channel is a quiet None: capture is opt-in, so a Slack
        token that was never set is not an error. A channel that RAISES is a bug
        and is logged at WARNING — the distinction that let a live TypeError
        masquerade as "not configured" for the whole first release of capture.
        """
        if self.inbound_factory is None:
            return None
        try:
            return self.inbound_factory()
        except Exception as exc:
            logger.warning("Channel %s inbound unavailable: %s", self.name, exc)
            return None

    def describe(self) -> dict[str, Any]:
        """Metadata + live status, the shape the API serves."""
        status = self.status()
        return {
            "name": self.name,
            "display_name": self.display_name,
            "version": self.version,
            "description": self.description,
            "capabilities": list(self.capabilities),
            "config_schema": dict(self.config_schema),
            "is_configured": bool(status.get("configured")),
            "can_send": bool(status.get("can_send")),
            "can_receive": bool(status.get("can_receive")),
            "status": status,
        }


# ---------------------------------------------------------------------------
# The channels
# ---------------------------------------------------------------------------


def _slack_outbound() -> MessagingPlugin | None:
    """A Slack plugin bound to the CURRENT credentials, or None when unset."""
    from alma.config import get_slack_channel, get_slack_token
    from alma.plugins.slack import SlackPlugin

    token = get_slack_token()
    if not token:
        return None
    return SlackPlugin(
        {"api_token": token, "default_channel": get_slack_channel() or ""}
    )


def _slack_inbound() -> InboundChannel | None:
    from alma.services.inbox_channels.slack import SlackInboxChannel

    channel = SlackInboxChannel()
    return channel if channel.is_configured() else None


def _slack_status() -> dict[str, Any]:
    from alma.slack.client import slack_status

    return slack_status()


def _email_status() -> dict[str, Any]:
    from alma.mailer.client import get_email_notifier

    configured = bool(get_email_notifier().is_configured)
    return {
        "configured": configured,
        "can_send": configured,
        "can_receive": False,
    }


def _slack_config_schema() -> dict[str, Any]:
    from alma.plugins.slack import SlackPlugin

    # `__new__` without `__init__`: the schema is static metadata and must be
    # readable before any credential exists, which `SlackPlugin(...)` would
    # reject.
    return SlackPlugin.__new__(SlackPlugin).get_config_schema()


CHANNELS: tuple[ChannelDescriptor, ...] = (
    ChannelDescriptor(
        name="slack",
        display_name="Slack",
        version="2.0.0",
        description=(
            "Post alert digests to a channel, and capture papers you send "
            "yourself from your phone."
        ),
        capabilities=(SEND, RECEIVE),
        config_schema=_slack_config_schema(),
        outbound_factory=_slack_outbound,
        inbound_factory=_slack_inbound,
        status_factory=_slack_status,
    ),
    ChannelDescriptor(
        name="email",
        display_name="Email",
        version="1.0.0",
        description="Deliver alert digests over SMTP.",
        capabilities=(SEND,),
        # Delivery is EmailNotifier's, through the alerts engine — no
        # MessagingPlugin class exists, hence no outbound_factory. The
        # capability is declared because it is true.
        status_factory=_email_status,
    ),
)


class ChannelRegistry:
    """Lookup over :data:`CHANNELS`. Stateless — no cached instances.

    Instance caching is what made the old plugin registry subtle: a plugin built
    from a token, cached for the process, and silently stale after the user
    pasted a new one. Factories are cheap, so every accessor builds fresh from
    the current configuration.
    """

    def __init__(self, descriptors: tuple[ChannelDescriptor, ...] = CHANNELS):
        self._descriptors = descriptors

    def names(self) -> list[str]:
        return [descriptor.name for descriptor in self._descriptors]

    def get(self, name: str) -> ChannelDescriptor:
        """Look a channel up by name.

        Raises:
            KeyError: if no channel is registered under that name.
        """
        for descriptor in self._descriptors:
            if descriptor.name == name:
                return descriptor
        raise KeyError(
            f"Channel '{name}' is not registered. "
            f"Available channels: {', '.join(self.names())}"
        )

    def all(self) -> tuple[ChannelDescriptor, ...]:
        return self._descriptors

    def with_capability(self, capability: Capability) -> list[ChannelDescriptor]:
        return [d for d in self._descriptors if d.can(capability)]

    def describe_all(self) -> list[dict[str, Any]]:
        return [descriptor.describe() for descriptor in self._descriptors]

    def outbound_plugin(self, name: str) -> MessagingPlugin | None:
        return self.get(name).outbound_plugin()

    def inbound_channels(self) -> list[InboundChannel]:
        """Every receive-capable channel that is configured enough to poll."""
        channels: list[InboundChannel] = []
        for descriptor in self.with_capability(RECEIVE):
            channel = descriptor.inbound_channel()
            if channel is not None:
                channels.append(channel)
        return channels


_registry: ChannelRegistry | None = None


def get_channel_registry() -> ChannelRegistry:
    """The process-wide channel registry."""
    global _registry
    if _registry is None:
        _registry = ChannelRegistry()
    return _registry


__all__ = [
    "CHANNELS",
    "RECEIVE",
    "SEND",
    "Capability",
    "ChannelDescriptor",
    "ChannelRegistry",
    "get_channel_registry",
]
