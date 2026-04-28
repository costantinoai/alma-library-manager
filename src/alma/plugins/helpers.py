"""Plugin initialization helpers.

Centralizes the repeated load-config / check-configured / register / create-instance
pattern used across API route files.
"""

import logging
from typing import Optional

from alma.plugins.config import load_plugin_config
from alma.plugins.registry import PluginRegistry, get_global_registry

logger = logging.getLogger(__name__)


def get_slack_plugin(registry: Optional[PluginRegistry] = None, required: bool = True):
    """Load, register (if needed), and return a configured Slack plugin instance.

    Args:
        registry: Plugin registry to use. Defaults to the global registry.
        required: If True, raise RuntimeError when the plugin is not configured.

    Returns:
        Tuple of (plugin_instance, config_dict).

    Raises:
        RuntimeError: If required=True and the Slack plugin is not properly configured.
    """
    if registry is None:
        registry = get_global_registry()

    config = load_plugin_config("slack")
    if not config or "api_token" not in config:
        if required:
            raise RuntimeError("Slack plugin not configured")
        return None, config

    # Lazy import to avoid circular imports at module level
    from alma.plugins.slack import SlackPlugin

    if "slack" not in registry.list_plugins():
        registry.register(SlackPlugin)

    plugin = registry.create_instance("slack", config, cache=True)
    return plugin, config
