"""Abstract base class for messaging plugins.

This module defines the MessagingPlugin interface that all messaging platform
plugins must implement. It provides a standardized way to send notifications
across different platforms while maintaining platform-specific formatting.
"""

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import List, Dict, Any, Optional
from datetime import datetime


@dataclass
class Publication:
    """Publication data structure passed to plugins for formatting.

    Attributes:
        title: Publication title
        authors: Comma-separated string of author names
        year: Publication year
        abstract: Publication abstract/summary
        pub_url: URL to the publication
        journal: Journal or venue name
        citations: Number of citations (optional)
    """
    title: str
    authors: str
    year: str
    abstract: str
    pub_url: str
    journal: str
    citations: Optional[int] = None


@dataclass
class Author:
    """Author data structure.

    Attributes:
        name: Author's full name
        scholar_id: Google Scholar ID
    """
    name: str
    scholar_id: str


class PluginConfigError(Exception):
    """Raised when a plugin is not properly configured."""
    pass


class PluginConnectionError(Exception):
    """Raised when a plugin cannot connect to its service."""
    pass


class MessagingPlugin(ABC):
    """Abstract base class for messaging platform plugins.

    All messaging plugins must inherit from this class and implement
    the required abstract methods. This ensures a consistent interface
    across all supported platforms.

    The plugin lifecycle:
    1. Initialize with configuration
    2. Validate configuration (via test_connection)
    3. Format messages (via format_* methods)
    4. Send messages (via send_message)
    5. Check health (via get_health_status)
    """

    def __init__(self, config: Dict[str, Any]):
        """Initialize the plugin with configuration.

        Args:
            config: Plugin-specific configuration dictionary

        Raises:
            PluginConfigError: If required configuration is missing
        """
        self.config = config
        self._validate_config()
        self._last_test: Optional[datetime] = None
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
        """Send a formatted message to the target.

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
    def format_publications(self, publications: List[Publication]) -> str:
        """Format a list of publications for this platform.

        Args:
            publications: List of Publication objects to format

        Returns:
            Platform-specific formatted string
        """
        pass

    @abstractmethod
    def format_authors(self, authors: List[Author]) -> str:
        """Format a list of authors for this platform.

        Args:
            authors: List of Author objects to format

        Returns:
            Platform-specific formatted string
        """
        pass

    @abstractmethod
    def format_test_message(self, message: str = "This is a test message") -> str:
        """Format a test message for this platform.

        Args:
            message: The test message content

        Returns:
            Platform-specific formatted test message
        """
        pass

    @abstractmethod
    def test_connection(self) -> bool:
        """Test if the plugin is properly configured and can connect.

        This method should verify:
        - Configuration is valid
        - Can authenticate with the service
        - Can reach the target endpoint

        Returns:
            True if connection test succeeds, False otherwise
        """
        pass

    @abstractmethod
    def get_config_schema(self) -> Dict[str, Any]:
        """Return JSON schema for plugin configuration.

        This schema is used by the GUI and API to provide configuration forms.

        Returns:
            JSON schema dict describing required and optional config fields

        Example:
            {
                "type": "object",
                "required": ["api_token", "channel"],
                "properties": {
                    "api_token": {
                        "type": "string",
                        "description": "API authentication token",
                        "secret": True
                    },
                    "channel": {
                        "type": "string",
                        "description": "Default channel for messages"
                    }
                }
            }
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

    def get_health_status(self) -> Dict[str, Any]:
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
