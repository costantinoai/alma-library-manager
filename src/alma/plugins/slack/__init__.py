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
        json_schema_extra={"x-alma-secret": True, "x-alma-order": 10, "x-alma-required": True},
    )
    alert_channel: str = Field(
        default="",
        title="Send alerts to",
        description="Channel name, Slack id, or user name for outbound digests.",
        json_schema_extra={"x-alma-order": 20, "x-alma-adds": "alert digests posted to Slack"},
    )
    inbox_channel: str = Field(
        default="",
        title="Capture papers from",
        description="Channel ALMa polls for paper links sent from your phone.",
        json_schema_extra={"x-alma-order": 30, "x-alma-adds": "papers captured from your phone"},
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
    from alma.config import get_slack_token
    from alma.core.secrets import SECRET_SLACK_BOT_TOKEN, get_secret
    from alma.slack.client import slack_status

    state = slack_status()
    token = get_slack_token()
    return {
        **state,
        "missing": "Add a channel to send alerts to or to capture papers from.",
        "source": (
            ("saved" if get_secret(SECRET_SLACK_BOT_TOKEN) == token else "environment")
            if token
            else None
        ),
    }


def secret_values() -> tuple[str, ...]:
    from alma.config import get_slack_token

    return (get_slack_token() or "",)


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
    """Ask Slack about the token and the channels. Posts nothing.

    It also runs right after a save, so it may never reach anyone. The price
    is that delivery itself stays unproven: `chat:write` is read from the
    token's scopes, and membership of a private channel is not tested.
    """
    from alma.slack.client import SlackResolveError, get_slack_notifier, slack_status

    notifier = get_slack_notifier()
    auth = notifier.auth_status()
    if auth["ok"] is not True:
        return auth

    state = slack_status()
    if state["can_send"] and auth["scopes"] is not None and "chat:write" not in auth["scopes"]:
        return {
            "ok": False,
            "error": "missing_scope",
            "message": "This token has no chat:write scope, so it cannot post alerts.",
            "action": "Add chat:write to the Slack app, reinstall it, and paste the new token.",
        }
    for target in (state["outbound_channel"], state["inbound_channel"]):
        if not target:
            continue
        try:
            notifier.resolve_channel_id(target)
        except SlackResolveError as exc:
            if "slack_unreachable" in str(exc):
                return {"ok": None, "error": str(exc), "message": "Slack could not be reached."}
            return {
                "ok": False,
                "error": str(exc),
                "message": f"Slack does not know {target!r}: {exc}",
                "action": "Check the name, and that the bot has been invited to the channel.",
            }
    return {
        "ok": True,
        "message": f"Token accepted by {auth['team']}, channels found. No message was sent.",
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
    docs_path="/plugins/slack/",
    inbound_factory=inbound_channel,
    alert_sender=send_alert,
    connection_tester=test_connection,
    action_ids=("test", "capture"),
    secret_values=secret_values,
)

__all__ = ["SLACK_PLUGIN", "SlackConfig"]
