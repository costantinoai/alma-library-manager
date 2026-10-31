"""ALMa external integration plugins.

Registration, activation, schema, and configuration ownership live in
``alma.plugins.registry``. Inbox is a core feature; integration plugins adapt
their capability-specific protocols. An add-on may bring its own integrations.
"""

from alma.plugins.registry import get_plugin_registry

__version__ = "1.0.0"
__all__ = ["get_plugin_registry"]
