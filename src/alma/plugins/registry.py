"""Plugin registry for managing messaging platform plugins.

This module provides the PluginRegistry class which manages the registration,
discovery, and instantiation of messaging plugins.
"""

import logging
from typing import Dict, Type, List, Optional, Any

from alma.plugins.base import MessagingPlugin, PluginConfigError

logger = logging.getLogger(__name__)


class PluginRegistry:
    """Central registry for managing messaging plugins.

    The registry handles:
    - Plugin registration (manual or auto-discovery)
    - Plugin instantiation with configuration
    - Plugin lookup by name
    - Plugin listing and metadata access

    Example:
        >>> registry = PluginRegistry()
        >>> registry.register(SlackPlugin)
        >>> plugin = registry.create_instance('slack', {'api_token': '...'})
        >>> plugin.send_message("Hello", "#general")
    """

    def __init__(self):
        """Initialize an empty plugin registry."""
        self._plugin_classes: Dict[str, Type[MessagingPlugin]] = {}
        self._plugin_instances: Dict[str, MessagingPlugin] = {}
        logger.info("Plugin registry initialized")

    def register(self, plugin_class: Type[MessagingPlugin]) -> None:
        """Register a plugin class.

        Args:
            plugin_class: A class that inherits from MessagingPlugin

        Raises:
            TypeError: If plugin_class doesn't inherit from MessagingPlugin
            ValueError: If a plugin with the same name is already registered
        """
        if not issubclass(plugin_class, MessagingPlugin):
            raise TypeError(
                f"{plugin_class.__name__} must inherit from MessagingPlugin"
            )

        # Create a temporary instance to get the plugin name
        # We need to provide a minimal config just to get the name
        try:
            temp_instance = plugin_class.__new__(plugin_class)
            plugin_name = temp_instance.name
        except AttributeError:
            raise TypeError(
                f"{plugin_class.__name__} must implement the 'name' property"
            )

        if plugin_name in self._plugin_classes:
            raise ValueError(
                f"Plugin '{plugin_name}' is already registered"
            )

        self._plugin_classes[plugin_name] = plugin_class
        logger.info(f"Registered plugin: {plugin_name}")

    def unregister(self, plugin_name: str) -> None:
        """Unregister a plugin by name.

        Args:
            plugin_name: Name of the plugin to unregister

        Raises:
            KeyError: If plugin is not registered
        """
        if plugin_name not in self._plugin_classes:
            raise KeyError(f"Plugin '{plugin_name}' is not registered")

        del self._plugin_classes[plugin_name]
        # Also remove any instances
        if plugin_name in self._plugin_instances:
            del self._plugin_instances[plugin_name]

        logger.info(f"Unregistered plugin: {plugin_name}")

    def get_plugin_class(self, plugin_name: str) -> Type[MessagingPlugin]:
        """Get a registered plugin class by name.

        Args:
            plugin_name: Name of the plugin

        Returns:
            The plugin class

        Raises:
            KeyError: If plugin is not registered
        """
        if plugin_name not in self._plugin_classes:
            raise KeyError(
                f"Plugin '{plugin_name}' not found. "
                f"Available plugins: {', '.join(self.list_plugins())}"
            )
        return self._plugin_classes[plugin_name]

    def create_instance(
        self,
        plugin_name: str,
        config: Dict[str, Any],
        cache: bool = True
    ) -> MessagingPlugin:
        """Create an instance of a plugin with the given configuration.

        Args:
            plugin_name: Name of the plugin to instantiate
            config: Configuration dictionary for the plugin
            cache: If True, cache the instance for reuse

        Returns:
            Configured plugin instance

        Raises:
            KeyError: If plugin is not registered
            PluginConfigError: If configuration is invalid
        """
        plugin_class = self.get_plugin_class(plugin_name)

        # Check if we have a cached instance
        if cache and plugin_name in self._plugin_instances:
            logger.debug(f"Returning cached instance of {plugin_name}")
            return self._plugin_instances[plugin_name]

        # Create new instance
        try:
            instance = plugin_class(config)
            logger.info(f"Created instance of {plugin_name}")

            if cache:
                self._plugin_instances[plugin_name] = instance

            return instance
        except Exception as e:
            logger.error(f"Failed to create instance of {plugin_name}: {e}")
            raise

    def get_instance(self, plugin_name: str) -> Optional[MessagingPlugin]:
        """Get a cached plugin instance if it exists.

        Args:
            plugin_name: Name of the plugin

        Returns:
            Cached plugin instance or None if not cached
        """
        return self._plugin_instances.get(plugin_name)

    def list_plugins(self) -> List[str]:
        """Get a list of all registered plugin names.

        Returns:
            List of plugin names
        """
        return list(self._plugin_classes.keys())

    def get_plugin_info(self, plugin_name: str) -> Dict[str, Any]:
        """Get metadata about a registered plugin.

        Args:
            plugin_name: Name of the plugin

        Returns:
            Dictionary with plugin metadata

        Raises:
            KeyError: If plugin is not registered
        """
        plugin_class = self.get_plugin_class(plugin_name)

        # Create temporary instance to get metadata
        temp = plugin_class.__new__(plugin_class)

        return {
            "name": temp.name,
            "display_name": temp.display_name,
            "version": temp.version,
            "description": temp.description,
            "config_schema": temp.get_config_schema(),
        }

    def get_all_plugins_info(self) -> List[Dict[str, Any]]:
        """Get metadata for all registered plugins.

        Returns:
            List of plugin metadata dictionaries
        """
        return [
            self.get_plugin_info(name)
            for name in self.list_plugins()
        ]

    def auto_discover(self) -> None:
        """Automatically discover and register plugins in the plugins directory.

        This method scans the plugins directory for plugin classes and
        registers them automatically. Useful for dynamic plugin loading.

        Note: Currently a placeholder for future implementation.
        """
        # TODO: Implement plugin auto-discovery
        # This would scan plugins/ directory for Python files,
        # import them, and register any MessagingPlugin subclasses
        logger.info("Auto-discovery not yet implemented")

    def clear_cache(self) -> None:
        """Clear all cached plugin instances.

        This forces fresh instances to be created on next access.
        """
        self._plugin_instances.clear()
        logger.info("Cleared plugin instance cache")

    def __repr__(self) -> str:
        """String representation of the registry."""
        plugin_count = len(self._plugin_classes)
        instance_count = len(self._plugin_instances)
        return (
            f"<PluginRegistry: {plugin_count} plugins registered, "
            f"{instance_count} instances cached>"
        )


# Global registry instance
_global_registry: Optional[PluginRegistry] = None


def get_global_registry() -> PluginRegistry:
    """Get or create the global plugin registry.

    Returns:
        The global PluginRegistry instance
    """
    global _global_registry
    if _global_registry is None:
        _global_registry = PluginRegistry()
    return _global_registry


def reset_global_registry() -> None:
    """Reset the global plugin registry.

    Useful for testing or when you need a fresh registry.
    """
    global _global_registry
    _global_registry = None
    logger.info("Global registry reset")
