# Plugin System Documentation

## Overview

The Scholar Publication Bot now supports a plugin-based architecture for messaging platforms. This allows easy integration with multiple notification services through a unified interface.

## Architecture

```
plugins/
├── base.py              # Abstract MessagingPlugin base class
├── registry.py          # Plugin registration and management
├── config.py            # Configuration loading utilities
└── slack/              # Slack plugin implementation
    ├── __init__.py
    └── plugin.py
```

## Quick Start

### Using the Slack Plugin

```python
from alma.plugins.registry import PluginRegistry
from alma.plugins.slack import SlackPlugin
from alma.plugins.config import load_slack_config_from_file

# Load configuration (preferred under ./config)
config = load_slack_config_from_file("./config/slack.config")

# Create registry and register plugin
registry = PluginRegistry()
registry.register(SlackPlugin)

# Create plugin instance
plugin = registry.create_instance("slack", config)

# Test connection
if plugin.test_connection():
    # Send a message
    plugin.send_message("Hello from the plugin system!", "general")
```

### Formatting Publications

```python
from alma.plugins.base import Publication, Author

# Create publication data
pubs = [
    Publication(
        title="Neural Networks for NLP",
        authors="Smith, J., Doe, A.",
        year="2024",
        abstract="A novel approach to...",
        pub_url="https://example.com/paper1",
        journal="Nature",
        citations=42
    )
]

# Format for Slack
message = plugin.format_publications(pubs)
plugin.send_message(message, "publications")
```

### Formatting Authors

```python
authors = [
    Author(name="John Doe", scholar_id="abc123"),
    Author(name="Jane Smith", scholar_id="xyz789")
]

message = plugin.format_authors(authors)
plugin.send_message(message, "general")
```

## Creating a New Plugin

To create a new messaging plugin, inherit from `MessagingPlugin` and implement all abstract methods:

```python
from alma.plugins.base import MessagingPlugin, Publication, Author
from typing import List, Dict, Any

class MyPlugin(MessagingPlugin):
    @property
    def name(self) -> str:
        return "myplugin"

    @property
    def display_name(self) -> str:
        return "My Plugin"

    @property
    def version(self) -> str:
        return "1.0.0"

    @property
    def description(self) -> str:
        return "Description of my plugin"

    def _validate_config(self) -> None:
        # Validate required configuration keys
        if "api_key" not in self.config:
            raise PluginConfigError("Missing api_key")

    def send_message(self, message: str, target: str) -> bool:
        # Implementation here
        pass

    def format_publications(self, publications: List[Publication]) -> str:
        # FIXME: this is independent from the particular plugin, ans it can be the same for all messaging services (except for the webhook, in which we can overwrite if needed). Move to base class?
        # Implementation here
        pass

    def format_authors(self, authors: List[Author]) -> str:
        # FIXME: this is independent from the particular plugin, ans it can be the same for all messaging services (except for the webhook, in which we can overwrite if needed). Move to base class?
        # Implementation here
        pass

    def format_test_message(self, message: str = "Test") -> str:
        # FIXME: this is independent from the particular plugin, ans it can be the same for all messaging services (except for the webhook, in which we can overwrite if needed). Move to base class?
        # Implementation here
        pass

    def test_connection(self) -> bool:
        # FIXME: this is independent from the particular plugin, ans it can be the same for all messaging services (except for the webhook, in which we can overwrite if needed). Move to base class?
        # Implementation here
        pass

    def get_config_schema(self) -> Dict[str, Any]:
        return {
            "type": "object",
            "required": ["api_key"],
            "properties": {
                "api_key": {
                    "type": "string",
                    "description": "API authentication key"
                }
            }
        }
```

## Configuration Management

### Loading from INI Files

```python
from alma.plugins.config import PluginConfigLoader

loader = PluginConfigLoader()
config = loader.load_from_ini("./config/slack.config", "slack")
```

### Loading from JSON

```python
config = loader.load_from_json("./config/plugin_config.json")
```

### Creating Config Templates

```python
from alma.plugins.config import create_plugin_config_template

create_plugin_config_template("slack", "./slack.config.template")
```

## Plugin Registry

The `PluginRegistry` manages all registered plugins:

```python
from alma.plugins.registry import get_global_registry

# Get global registry
registry = get_global_registry()

# Register plugins
registry.register(SlackPlugin)
registry.register(EmailPlugin)

# List available plugins
print(registry.list_plugins())  # ['slack', 'email']

# Get plugin info
info = registry.get_plugin_info('slack')
print(info['display_name'])  # 'Slack'
print(info['version'])       # '1.0.0'

# Create instance
plugin = registry.create_instance('slack', config)
```

## Health Monitoring

Plugins track their health status:

```python
# Test connection
plugin.test_connection()

# Get health status
status = plugin.get_health_status()
print(status['healthy'])          # True/False
print(status['last_test'])        # ISO timestamp
print(status['message'])          # Human-readable status
```

## Error Handling

The plugin system provides specific exceptions:

```python
from plugins.base import PluginConfigError, PluginConnectionError

try:
    plugin = SlackPlugin({'invalid': 'config'})
except PluginConfigError as e:
    print(f"Configuration error: {e}")

try:
    plugin.send_message("Hello", "nowhere")
except PluginConnectionError as e:
    print(f"Connection error: {e}")
```

## Integration Guidance

All messaging should use the plugin system only (no legacy helpers). Configure via env vars or `./config/slack.config` and create instances through the registry.

## Future Plugins

Planned plugins include:
- **Email**: SMTP-based email notifications
- **Discord**: Webhook-based Discord notifications
- **Webhook**: Generic HTTP webhook support
- **MS Teams**: Microsoft Teams connector

## Testing

See `tests/test_plugins.py` for comprehensive plugin tests including:
- Plugin registration
- Configuration validation
- Message formatting
- Connection testing
- Error handling
