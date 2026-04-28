"""Plugin configuration management.

This module provides utilities for loading and managing plugin configurations
from various sources (files, environment variables, etc.).
"""

import json
import configparser
import logging
from pathlib import Path
from typing import Dict, Any, Optional

logger = logging.getLogger(__name__)


class PluginConfigLoader:
    """Load plugin configurations from various sources.

    This class supports loading plugin configurations from:
    - INI/config files (e.g., slack.config)
    - JSON files
    - Python dictionaries

    Example:
        >>> loader = PluginConfigLoader()
        >>> config = loader.load_from_ini('./config/slack.config', 'slack')
        >>> print(config['api_token'])
    """

    @staticmethod
    def load_from_ini(
        file_path: str,
        section: str = "slack",
        required_keys: Optional[list] = None
    ) -> Dict[str, Any]:
        """Load configuration from an INI/config file.

        Args:
            file_path: Path to the INI file
            section: Section name in the INI file
            required_keys: List of required keys to validate

        Returns:
            Dictionary with configuration values

        Raises:
            FileNotFoundError: If file doesn't exist
            KeyError: If required section or keys are missing
        """
        path = Path(file_path)
        if not path.exists():
            raise FileNotFoundError(f"Config file not found: {file_path}")

        config = configparser.ConfigParser()
        config.read(file_path, encoding="utf-8")

        if not config.has_section(section):
            raise KeyError(f"Section '{section}' not found in {file_path}")

        # Convert to dictionary
        config_dict = dict(config[section])

        # Validate required keys
        if required_keys:
            missing = [k for k in required_keys if k not in config_dict]
            if missing:
                raise KeyError(
                    f"Missing required keys in {section}: {', '.join(missing)}"
                )

        logger.info(f"Loaded config from {file_path} [{section}]")
        return config_dict

    @staticmethod
    def load_from_json(
        file_path: str,
        required_keys: Optional[list] = None
    ) -> Dict[str, Any]:
        """Load configuration from a JSON file.

        Args:
            file_path: Path to the JSON file
            required_keys: List of required keys to validate

        Returns:
            Dictionary with configuration values

        Raises:
            FileNotFoundError: If file doesn't exist
            json.JSONDecodeError: If file is not valid JSON
            KeyError: If required keys are missing
        """
        path = Path(file_path)
        if not path.exists():
            raise FileNotFoundError(f"Config file not found: {file_path}")

        with open(file_path, "r", encoding="utf-8") as f:
            config_dict = json.load(f)

        # Validate required keys
        if required_keys:
            missing = [k for k in required_keys if k not in config_dict]
            if missing:
                raise KeyError(
                    f"Missing required keys: {', '.join(missing)}"
                )

        logger.info(f"Loaded config from {file_path}")
        return config_dict

    @staticmethod
    def save_to_ini(
        config: Dict[str, Any],
        file_path: str,
        section: str = "slack"
    ) -> None:
        """Save configuration to an INI/config file.

        Args:
            config: Configuration dictionary
            file_path: Path to save the file
            section: Section name in the INI file
        """
        path = Path(file_path)
        path.parent.mkdir(parents=True, exist_ok=True)

        config_parser = configparser.ConfigParser()
        config_parser[section] = config

        with open(file_path, "w", encoding="utf-8") as f:
            config_parser.write(f)

        logger.info(f"Saved config to {file_path} [{section}]")

    @staticmethod
    def save_to_json(
        config: Dict[str, Any],
        file_path: str,
        indent: int = 2
    ) -> None:
        """Save configuration to a JSON file.

        Args:
            config: Configuration dictionary
            file_path: Path to save the file
            indent: JSON indentation level
        """
        path = Path(file_path)
        path.parent.mkdir(parents=True, exist_ok=True)

        with open(file_path, "w", encoding="utf-8") as f:
            json.dump(config, f, indent=indent)

        logger.info(f"Saved config to {file_path}")


def load_slack_config_from_file(file_path: str = "./config/slack.config") -> Dict[str, Any]:
    """Convenience function to load Slack configuration.

    This helper mirrors legacy loading behavior while returning a plugin-friendly schema.

    Args:
        file_path: Path to slack.config file

    Returns:
        Dictionary with Slack configuration

    Example:
        >>> config = load_slack_config_from_file()
        >>> plugin = SlackPlugin(config)
    """
    loader = PluginConfigLoader()
    config = loader.load_from_ini(
        file_path,
        section="slack",
        required_keys=["api_token", "channel_name"]
    )

    # Ensure proper naming for plugin
    if "channel_name" in config:
        config["default_channel"] = config.pop("channel_name")

    return config


def create_plugin_config_template(plugin_name: str, output_file: str) -> None:
    """Create a configuration template file for a plugin.

    Args:
        plugin_name: Name of the plugin
        output_file: Path to save the template

    Example:
        >>> create_plugin_config_template('slack', './slack.config.template')
    """
    templates = {
        "slack": {
            "api_token": "replace-with-real-slack-token",
            "default_channel": "general",
        },
        "email": {
            "smtp_host": "smtp.gmail.com",
            "smtp_port": "587",
            "smtp_username": "your-email@gmail.com",
            "smtp_password": "your-password",
            "from_address": "bot@example.com",
            "to_addresses": "notifications@example.com",
        },
        "discord": {
            "webhook_url": "https://discord.com/api/webhooks/...",
        },
    }

    if plugin_name not in templates:
        raise ValueError(f"No template available for plugin: {plugin_name}")

    # Save as INI format
    loader = PluginConfigLoader()
    loader.save_to_ini(
        templates[plugin_name],
        output_file,
        section=plugin_name
    )

    logger.info(f"Created config template: {output_file}")


def load_plugin_config(plugin_name: str) -> Optional[Dict[str, Any]]:
    """Load configuration for a specific plugin.

    Attempts to load configuration from multiple sources:
    1. JSON file: ./config/{plugin_name}.json
    2. INI file: ./src/{plugin_name}.config
    3. Returns None if no configuration found

    Args:
        plugin_name: Name of the plugin (e.g., 'slack', 'email')

    Returns:
        Configuration dictionary or None if not configured

    Example:
        >>> config = load_plugin_config('slack')
        >>> if config:
        ...     plugin = SlackPlugin(config)
    """
    loader = PluginConfigLoader()

    # Environment/secret-store override for runtime secrets.
    if plugin_name == "slack":
        from alma.config import get_slack_channel, get_slack_token

        token = get_slack_token()
        default_channel = get_slack_channel()
        if token:
            logger.info("Loaded Slack configuration from runtime credential sources")
            cfg = {"api_token": token}
            if default_channel:
                cfg["default_channel"] = default_channel
            return cfg

    # Try JSON format in ./config
    json_path = f"./config/{plugin_name}.json"
    try:
        return loader.load_from_json(json_path)
    except FileNotFoundError:
        pass

    # Try INI format in ./config first, then legacy ./src
    for ini_path in (f"./config/{plugin_name}.config", f"./src/{plugin_name}.config"):
        try:
            config = loader.load_from_ini(ini_path, section=plugin_name)
            # Normalize channel_name to default_channel for backward compatibility
            if "channel_name" in config:
                config["default_channel"] = config.pop("channel_name")
            return config
        except (FileNotFoundError, KeyError):
            continue

    # No configuration found
    logger.debug(f"No configuration found for plugin: {plugin_name}")
    return None


def save_plugin_config(plugin_name: str, config: Dict[str, Any]) -> None:
    """Save configuration for a specific plugin.

    Saves configuration to JSON format: ./config/{plugin_name}.json

    Args:
        plugin_name: Name of the plugin
        config: Configuration dictionary to save

    Example:
        >>> save_plugin_config('slack', {
        ...     'api_token': 'replace-with-real-slack-token',
        ...     'default_channel': '#general'
        ... })
    """
    loader = PluginConfigLoader()
    json_path = f"./config/{plugin_name}.json"
    loader.save_to_json(config, json_path)
    logger.info(f"Saved configuration for {plugin_name} to {json_path}")


# Backward compatibility alias
    
