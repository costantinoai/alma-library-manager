"""ALMa external integration plugins.

Registration, activation, schema, and configuration ownership live in
``alma.plugins.registry``. Alerts and Inbox remain core features; integration
plugins adapt their capability-specific protocols.
"""

from alma.plugins.registry import get_plugin_registry, plugin_enabled

__version__ = "1.0.0"
__all__ = ["get_plugin_registry", "plugin_enabled"]
