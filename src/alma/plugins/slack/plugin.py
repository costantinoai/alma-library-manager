"""Slack as an outbound messaging plugin — a thin face over the one transport.

This class used to be a SECOND Slack client: its own `requests` calls to
``auth.test`` / ``conversations.list`` / ``users.list`` / ``chat.postMessage``,
its own name→ID caches, its own token handling — running in the same process as
:class:`alma.slack.client.SlackNotifier`, which does all of that with
``slack_sdk``. Two transports meant two caches, two error vocabularies, and a
credential that had to be kept in two places to feed both.

Now there is one transport. This class supplies the *plugin* half — identity,
config schema, and the ``MessagingPlugin`` send/test seam that the channel
registry advertises — and delegates every byte on the wire to the notifier.

Delegation is per call, not per instance: :func:`get_slack_notifier` reads the
current configuration each time, so a token changed in Settings takes effect on
the next send instead of on the next process restart.
"""

import logging
from typing import Any

from alma.plugins.base import (
    MessagingPlugin,
    PluginConfigError,
    PluginConnectionError,
)

logger = logging.getLogger(__name__)


class SlackPlugin(MessagingPlugin):
    """Slack messaging plugin.

    Configuration:
        api_token (str): Slack Bot API token (starts with 'xoxb-')
        default_channel (str, optional): Default channel or user name

    Example:
        >>> plugin = SlackPlugin({'api_token': 'xoxb-…', 'default_channel': 'general'})
        >>> plugin.send_message("Hello!", "general")
    """

    # Plugin metadata
    @property
    def name(self) -> str:
        return "slack"

    @property
    def display_name(self) -> str:
        return "Slack"

    @property
    def version(self) -> str:
        return "2.0.0"

    @property
    def description(self) -> str:
        return "Send notifications to Slack channels and direct messages"

    def _validate_config(self) -> None:
        """Validate Slack plugin configuration.

        Raises:
            PluginConfigError: If required configuration is missing or invalid
        """
        if "api_token" not in self.config:
            raise PluginConfigError("Missing required config: 'api_token'")

        token = self.config["api_token"]
        if not isinstance(token, str) or not token.startswith("xoxb-"):
            raise PluginConfigError(
                "api_token must be a valid Slack bot token (starts with 'xoxb-')"
            )

    def get_config_schema(self) -> dict[str, Any]:
        """Return JSON schema for Slack configuration."""
        return {
            "type": "object",
            "required": ["api_token"],
            "properties": {
                "api_token": {
                    "type": "string",
                    "description": "Slack Bot User OAuth Token (starts with 'xoxb-')",
                    "secret": True,
                    "pattern": "^xoxb-",
                },
                "default_channel": {
                    "type": "string",
                    "description": "Default channel or user name for messages",
                    "default": "",
                },
            },
        }

    def _notifier(self):
        """The single Slack transport, built from the CURRENT configuration."""
        from alma.slack.client import get_slack_notifier

        return get_slack_notifier()

    def test_connection(self) -> bool:
        """Verify the workspace accepts our token (``auth.test``)."""
        success = self._notifier().check_auth()
        self.record_test_result(success)
        return success

    def send_message(self, message: str, target: str) -> bool:
        """Send an already-rendered message to a Slack channel or user.

        Args:
            message: The message to send (pre-formatted)
            target: Channel name, ``#name``, Slack ID, or user display name

        Returns:
            True if the message was sent, False otherwise

        Raises:
            PluginConnectionError: If the transport has no usable token
        """
        try:
            return self._notifier().post_text(target, message)
        except RuntimeError as exc:
            # The notifier raises RuntimeError when it holds no token at all.
            # Everything else it reports as False, and so do we.
            raise PluginConnectionError(f"Failed to send message: {exc}") from exc
