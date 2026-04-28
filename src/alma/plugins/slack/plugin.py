"""Slack plugin implementation.

This module contains the SlackPlugin class which implements the MessagingPlugin
interface for sending messages to Slack channels and direct messages.
"""

import logging
import requests
from typing import List, Dict, Any, Optional

from alma.plugins.base import (
    MessagingPlugin,
    Publication,
    Author,
    PluginConfigError,
    PluginConnectionError,
)

logger = logging.getLogger(__name__)


class SlackPlugin(MessagingPlugin):
    """Slack messaging plugin.

    This plugin sends formatted messages to Slack channels or direct messages.
    It supports both channel names and user names as targets.

    Configuration:
        api_token (str): Slack Bot API token (starts with 'xoxb-')
        default_channel (str, optional): Default channel or user name

    Example:
        >>> config = {
        ...     'api_token': 'replace-with-real-slack-token',
        ...     'default_channel': 'general'
        ... }
        >>> plugin = SlackPlugin(config)
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
        return "1.0.0"

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

        logger.debug(f"Slack plugin initialized with token: {token[:10]}...")

    def get_config_schema(self) -> Dict[str, Any]:
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

    def test_connection(self) -> bool:
        """Test Slack API connection.

        Returns:
            True if connection succeeds, False otherwise
        """
        try:
            # Use auth.test endpoint to verify the token
            url = "https://slack.com/api/auth.test"
            headers = {"Authorization": f"Bearer {self.config['api_token']}"}

            response = requests.get(url, headers=headers, timeout=10).json()

            success = response.get("ok", False)
            self.record_test_result(success)

            if success:
                logger.info(f"Slack connection test passed for team: {response.get('team')}")
            else:
                logger.warning(f"Slack connection test failed: {response.get('error')}")

            return success

        except Exception as e:
            logger.error(f"Slack connection test error: {e}")
            self.record_test_result(False)
            return False

    def send_message(self, message: str, target: str) -> bool:
        """Send a message to a Slack channel or user.

        Args:
            message: The message to send (pre-formatted)
            target: Channel name (without #) or user name

        Returns:
            True if message was sent successfully, False otherwise

        Raises:
            PluginConnectionError: If unable to connect to Slack API
        """
        token = self.config["api_token"]

        # Try to resolve as channel first
        channel_id = self._get_channel_id_by_name(target, token)
        if channel_id:
            return self._send_message_to_channel(channel_id, message, token)

        # Try to resolve as user
        user_id = self._get_user_id_by_name(target, token)
        if user_id:
            dm_channel_id = self._open_im_channel(user_id, token)
            if dm_channel_id:
                return self._send_message_to_channel(dm_channel_id, message, token)

        # Final fallback: attempt to send using target as-is (useful for tests/envs)
        try:
            if self._send_message_to_channel(target, message, token):
                return True
        except Exception:
            pass

        # Neither found
        logger.error(f"'{target}' is not a valid channel or user in this Slack workspace")
        return False

    def format_publications(self, publications: List[Publication]) -> str:
        """Format publications for Slack.

        Args:
            publications: List of publications to format

        Returns:
            Slack-formatted message string
        """
        if not publications:
            return "No new publications since my last check."

        # Remove duplicates by title
        seen_titles = set()
        unique_pubs = []
        for pub in publications:
            if pub.title not in seen_titles:
                seen_titles.add(pub.title)
                unique_pubs.append(pub)

        # Format each publication
        formatted = ["List of publications since my last check:\n"]
        for pub in unique_pubs:
            formatted.append(self._format_single_publication(pub))

        return "\n".join(formatted)

    def format_authors(self, authors: List[Author]) -> str:
        """Format authors list for Slack.

        Args:
            authors: List of authors to format

        Returns:
            Slack-formatted author list
        """
        # Sort alphabetically
        sorted_authors = sorted(authors, key=lambda a: a.name.lower())

        # Format with Scholar IDs
        formatted = "\n".join([
            f"\t{author.name},\t\tGoogle Scholar ID: {author.scholar_id}"
            for author in sorted_authors
        ])

        return f"List of monitored authors:\n```{formatted}```"

    def format_test_message(self, message: str = "This is a test message") -> str:
        """Format a test message for Slack.

        Args:
            message: The test message content

        Returns:
            Slack-formatted test message
        """
        width = len(message) + 2
        border = "#" * width
        return f"```\n{border}\n#{message}#\n{border}```"

    # Private helper methods adapted from slack_bot.py

    def _format_single_publication(self, pub: Publication) -> str:
        """Format a single publication for Slack."""
        details = []
        details.append("-" * 50)
        details.append("")

        # Title as clickable link
        title = f"*<{pub.pub_url}|{pub.title}>*"
        details.append(title)

        # Authors (abbreviated if > 4)
        authors = pub.authors.split(",")
        if len(authors) < 5:
            details.append(f"Authors: {pub.authors}")
        else:
            details.append(
                f"Authors: {authors[0]}, [+{len(authors)-2}], {authors[-1]}"
            )

        details.append(f"Journal: {pub.journal}")
        details.append("")
        details.append(f"Abstract: _{pub.abstract}_")
        details.append("")

        return "\n".join(details)

    def _get_channel_id_by_name(self, channel_name: str, token: str) -> Optional[str]:
        """Get channel ID by name."""
        url = "https://slack.com/api/conversations.list"
        headers = {"Authorization": f"Bearer {token}"}
        params = {
            "types": "public_channel,private_channel",
            "limit": 1000,
        }

        while True:
            try:
                response = requests.get(url, headers=headers, params=params, timeout=10).json()

                if not response.get("ok"):
                    logger.warning(f"Failed to list channels: {response.get('error')}")
                    return None

                for channel in response.get("channels", []):
                    if channel.get("name") == channel_name:
                        return channel.get("id")

                # Handle pagination
                cursor = response.get("response_metadata", {}).get("next_cursor")
                if cursor:
                    params["cursor"] = cursor
                else:
                    break

            except Exception as e:
                logger.error(f"Error listing channels: {e}")
                return None

        return None

    def _get_user_id_by_name(self, user_name: str, token: str) -> Optional[str]:
        """Get user ID by name."""
        url = "https://slack.com/api/users.list"
        headers = {"Authorization": f"Bearer {token}"}
        params = {"limit": 1000}

        while True:
            try:
                response = requests.get(url, headers=headers, params=params, timeout=10).json()

                if not response.get("ok"):
                    logger.warning(f"Failed to list users: {response.get('error')}")
                    return None

                for member in response.get("members", []):
                    user_handle = member.get("name", "")
                    real_name = member.get("real_name", "")

                    if user_handle == user_name or real_name == user_name:
                        return member.get("id")

                # Handle pagination
                cursor = response.get("response_metadata", {}).get("next_cursor")
                if cursor:
                    params["cursor"] = cursor
                else:
                    break

            except Exception as e:
                logger.error(f"Error listing users: {e}")
                return None

        return None

    def _open_im_channel(self, user_id: str, token: str) -> Optional[str]:
        """Open or retrieve a DM channel with a user."""
        url = "https://slack.com/api/conversations.open"
        headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json; charset=utf-8",
        }
        data = {"users": user_id}

        try:
            response = requests.post(url, headers=headers, json=data, timeout=10).json()

            if not response.get("ok"):
                logger.warning(f"Error opening DM for user {user_id}: {response}")
                return None

            return response.get("channel", {}).get("id")

        except Exception as e:
            logger.error(f"Error opening DM channel: {e}")
            return None

    def _send_message_to_channel(
        self, channel_id: str, message: str, token: str
    ) -> bool:
        """Send message to a specific channel ID."""
        url = "https://slack.com/api/chat.postMessage"
        headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json; charset=utf-8",
        }
        data = {"channel": channel_id, "text": message}

        try:
            response = requests.post(url, headers=headers, json=data, timeout=10).json()

            if not response.get("ok"):
                logger.warning(
                    f"Sending message to {channel_id} failed. "
                    f"Error: {response.get('error')}"
                )
                return False

            logger.debug(f"Message successfully sent to {channel_id}")
            return True

        except Exception as e:
            logger.error(f"Error sending message: {e}")
            raise PluginConnectionError(f"Failed to send message: {e}")
