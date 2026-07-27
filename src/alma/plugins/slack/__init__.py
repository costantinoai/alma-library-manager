"""Slack integration plugin.

One package owns its generated config schema, storage mapping, status, core
Alerts sender, core Inbox adapter, and connectivity test. Network transport
remains exclusively in :mod:`alma.slack.client`.
"""

from __future__ import annotations

import sqlite3

from pydantic import BaseModel, Field

from alma.plugins.configs import StrictPluginConfig
from alma.plugins.manifest import RECEIVE, SEND, PluginManifest


class SlackConfig(StrictPluginConfig):
    bot_token: str = Field(
        default="",
        title="Bot token",
        description="Slack Bot User OAuth Token. Stored in the secret store.",
        json_schema_extra={"x-alma-secret": True, "x-alma-order": 10},
    )
    alert_channel: str = Field(
        default="",
        title="Send alerts to",
        description="Channel name, Slack id, or user name for outbound digests.",
        json_schema_extra={"x-alma-order": 20},
    )
    inbox_channel: str = Field(
        default="",
        title="Capture papers from",
        description="Channel ALMa polls for paper links sent from your phone.",
        json_schema_extra={"x-alma-order": 30},
    )


def read_config(_db: sqlite3.Connection | None) -> SlackConfig:
    from alma.config import get_setting, get_slack_token
    from alma.core.secrets import mask_secret

    return SlackConfig(
        bot_token=mask_secret(get_slack_token(), prefix=10, suffix=4) or "",
        alert_channel=str(get_setting("slack_channel") or ""),
        inbox_channel=str(get_setting("slack_inbox_channel") or ""),
    )


def write_config(_db: sqlite3.Connection | None, config: BaseModel) -> None:
    from alma.config import update_settings
    from alma.core.secrets import SECRET_SLACK_BOT_TOKEN, delete_secret, set_secret

    parsed = SlackConfig.model_validate(config)
    token = parsed.bot_token.strip()
    if "..." not in token:
        if token:
            set_secret(SECRET_SLACK_BOT_TOKEN, token)
        else:
            delete_secret(SECRET_SLACK_BOT_TOKEN)
    update_settings(
        {
            "slack_channel": parsed.alert_channel.strip() or None,
            "slack_inbox_channel": parsed.inbox_channel.strip() or None,
        }
    )


def status(_db: sqlite3.Connection | None) -> dict:
    from alma.slack.client import slack_status

    return slack_status()


def inbound_channel():
    from alma.services.inbox_channels.slack import SlackInboxChannel

    channel = SlackInboxChannel()
    return channel if channel.is_configured() else None


async def send_alert(papers: list[dict], alert_name: str) -> bool:
    from alma.slack.client import get_slack_notifier

    return await get_slack_notifier().send_paper_alert(
        channel=None,
        papers=papers,
        alert_name=alert_name,
    )


async def test_connection() -> dict:
    from alma.slack.client import SlackResolveError, get_slack_notifier

    notifier = get_slack_notifier()
    target = notifier.resolve_channel(None)
    try:
        resolved_id = notifier._resolve_target(target)
    except SlackResolveError as exc:
        return {
            "ok": False,
            "target": target,
            "error": str(exc),
            "message": f"Could not resolve Slack target {target!r}: {exc}",
        }
    try:
        ok = await notifier.send_test_message()
    except Exception as exc:  # pragma: no cover - network failure boundary
        return {
            "ok": False,
            "target": target,
            "error": str(exc),
            "message": f"Slack test failed: {exc}",
        }
    return {
        "ok": ok,
        "target": target,
        "resolved_id": resolved_id,
        "message": (
            f"Slack test message delivered to {target}"
            if ok
            else "Slack rejected the test message; check the bot's chat:write scope."
        ),
    }


SLACK_PLUGIN = PluginManifest(
    id="slack",
    display_name="Slack",
    version="3.0.0",
    description="Post alert digests and capture papers sent from Slack into Home.",
    kind="integration",
    capabilities=(SEND, RECEIVE),
    config_model=SlackConfig,
    read_config=read_config,
    write_config=write_config,
    status_factory=status,
    docs_path="/user-guide/connecting-slack/",
    inbound_factory=inbound_channel,
    alert_sender=send_alert,
    connection_tester=test_connection,
    action_ids=("test", "capture"),
)

__all__ = ["SLACK_PLUGIN", "SlackConfig"]
