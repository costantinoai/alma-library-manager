"""The outbound half of a delivery channel: what it takes to SEND.

A messaging plugin answers one question — *can ALMa deliver a finished message
to this service, and how is it configured?* It is the mirror image of
:class:`alma.application.inbox_schema.InboundChannel`, which answers *what can
this service deliver to ALMa?*. Both are registered together in
:mod:`alma.channels`, per direction, so one service can do one or both.

**Scope line (task 55).** How a paper LOOKS in a channel is transport-scoped;
what a paper IS is application-scoped. Rendering therefore lives with the
transport that knows the medium's markup — Block Kit in
:class:`alma.slack.client.SlackNotifier`, MIME in
:class:`alma.mailer.client.EmailNotifier` — and identity lives in
`application.inbound_capture`. This class carries neither: it is the *delivery*
seam, and it takes text that is already rendered.

Three ``format_publications`` / ``format_authors`` / ``format_test_message``
abstract methods used to sit here, with a README FIXME asking whether they
belonged on the base class. They are gone rather than promoted: they produced
the old plain-text digest that the Block Kit alert pipeline replaced, and after
that pipeline was deleted nothing called them. The honest answer to "should this
be shared?" was "nothing was using either copy".
"""

from abc import ABC, abstractmethod
from datetime import datetime
from typing import Any


class PluginConfigError(Exception):
    """Raised when a plugin is not properly configured."""
    pass


class PluginConnectionError(Exception):
    """Raised when a plugin cannot connect to its service."""
    pass


class MessagingPlugin(ABC):
    """Abstract base class for outbound messaging platforms.

    The plugin lifecycle:
    1. Initialize with configuration
    2. Validate configuration (raises during ``__init__``)
    3. Send an already-rendered message (via ``send_message``)
    4. Check reachability (via ``test_connection`` / ``get_health_status``)
    """

    def __init__(self, config: dict[str, Any]):
        """Initialize the plugin with configuration.

        Args:
            config: Plugin-specific configuration dictionary

        Raises:
            PluginConfigError: If required configuration is missing
        """
        self.config = config
        self._validate_config()
        self._last_test: datetime | None = None
        self._last_test_success: bool = False

    @abstractmethod
    def _validate_config(self) -> None:
        """Validate that required configuration keys are present.

        Raises:
            PluginConfigError: If required configuration is missing
        """
        pass

    @abstractmethod
    def send_message(self, message: str, target: str) -> bool:
        """Send an already-rendered message to the target.

        Args:
            message: Pre-formatted message string
            target: Target identifier (channel, email, webhook URL, etc.)

        Returns:
            True if message was sent successfully, False otherwise

        Raises:
            PluginConnectionError: If unable to connect to the service
        """
        pass

    @abstractmethod
    def test_connection(self) -> bool:
        """Test if the plugin is properly configured and can connect.

        This method should verify:
        - Configuration is valid
        - Can authenticate with the service

        Returns:
            True if connection test succeeds, False otherwise
        """
        pass

    @abstractmethod
    def get_config_schema(self) -> dict[str, Any]:
        """Return JSON schema for plugin configuration.

        This schema is used by the GUI and API to provide configuration forms.

        Returns:
            JSON schema dict describing required and optional config fields
        """
        pass

    @property
    @abstractmethod
    def name(self) -> str:
        """Plugin name (e.g., 'slack', 'email', 'discord').

        Must be lowercase and alphanumeric.
        """
        pass

    @property
    @abstractmethod
    def display_name(self) -> str:
        """Human-readable plugin name (e.g., 'Slack', 'Email', 'Discord')."""
        pass

    @property
    @abstractmethod
    def version(self) -> str:
        """Plugin version string (semver format)."""
        pass

    @property
    @abstractmethod
    def description(self) -> str:
        """Brief description of the plugin's functionality."""
        pass

    # Non-abstract helper methods

    def get_health_status(self) -> dict[str, Any]:
        """Get the current health status of the plugin.

        Returns:
            Dictionary with health status information:
            - healthy: bool
            - last_test: datetime or None
            - last_test_success: bool
            - message: str describing the status
        """
        return {
            "healthy": self._last_test_success if self._last_test else None,
            "last_test": self._last_test.isoformat() if self._last_test else None,
            "last_test_success": self._last_test_success,
            "message": self._get_health_message(),
        }

    def _get_health_message(self) -> str:
        """Generate a human-readable health status message."""
        if not self._last_test:
            return "Plugin has not been tested yet"
        if self._last_test_success:
            time_ago = (datetime.now() - self._last_test).total_seconds()
            if time_ago < 3600:
                return f"Last test passed {int(time_ago / 60)} minutes ago"
            elif time_ago < 86400:
                return f"Last test passed {int(time_ago / 3600)} hours ago"
            else:
                return f"Last test passed {int(time_ago / 86400)} days ago"
        else:
            return "Last test failed"

    def record_test_result(self, success: bool) -> None:
        """Record the result of a connection test.

        Args:
            success: Whether the test succeeded
        """
        self._last_test = datetime.now()
        self._last_test_success = success

    def __repr__(self) -> str:
        """String representation of the plugin."""
        return f"<{self.display_name}Plugin v{self.version}>"
