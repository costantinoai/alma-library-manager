"""External integration-plugin contracts.

Alerts and Inbox are core ALMa features. An integration manifest describes how
an external service adapts one or both of their narrow protocols, plus identity,
activation, configuration schema/storage, and status.

Configuration models are the schema source of truth. The API serves
``model_json_schema()`` from the exact Pydantic class it uses to validate writes;
there is no handwritten schema copy for the frontend or documentation to drift
from.
"""

from __future__ import annotations

import logging
import sqlite3
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from typing import Any, Literal

from pydantic import BaseModel

from alma.application.inbox_schema import InboundChannel

logger = logging.getLogger(__name__)

PluginKind = Literal["integration"]
Capability = Literal["send", "receive"]

SEND: Capability = "send"
RECEIVE: Capability = "receive"

ConfigReader = Callable[[sqlite3.Connection | None], BaseModel]
ConfigWriter = Callable[[sqlite3.Connection | None, BaseModel], None]
StatusFactory = Callable[[sqlite3.Connection | None], dict[str, Any]]
InboundFactory = Callable[[], InboundChannel | None]
AlertSender = Callable[[list[dict[str, Any]], str], Awaitable[bool]]
ConnectionTester = Callable[[], Awaitable[dict[str, Any]]]


def activation_setting_key(plugin_id: str) -> str:
    """Canonical flat settings key for runtime activation."""
    return f"plugins.{plugin_id}.enabled"


@dataclass(frozen=True)
class PluginManifest:
    """One explicitly registered external integration."""

    id: str
    display_name: str
    version: str
    description: str
    kind: PluginKind
    capabilities: tuple[Capability, ...]
    config_model: type[BaseModel]
    read_config: ConfigReader
    write_config: ConfigWriter
    status_factory: StatusFactory
    docs_path: str
    inbound_factory: InboundFactory | None = None
    alert_sender: AlertSender | None = None
    connection_tester: ConnectionTester | None = None
    action_ids: tuple[str, ...] = field(default_factory=tuple)

    def can(self, capability: Capability) -> bool:
        return capability in self.capabilities

    def is_enabled(self) -> bool:
        """Read the explicit activation flag.

        Fresh settings get every flag from ``DEFAULT_SETTINGS``. Existing
        profiles are upgraded by ``migrate_settings_schema`` before the API
        starts. The default argument is a bootstrap default, not a legacy
        inference from credentials.
        """
        from alma.config import DEFAULT_SETTINGS, get_setting

        key = activation_setting_key(self.id)
        value = get_setting(key, DEFAULT_SETTINGS[key])
        if not isinstance(value, bool):
            raise ValueError(f"{key} must be a boolean")
        return value

    def set_enabled(self, enabled: bool) -> None:
        from alma.config import update_settings

        update_settings({activation_setting_key(self.id): bool(enabled)})

    def status(self, db: sqlite3.Connection | None = None) -> dict[str, Any]:
        """Current non-secret status. Fail visibly in logs, never leak details."""
        try:
            return dict(self.status_factory(db))
        except Exception as exc:  # pragma: no cover - defensive boundary
            logger.warning("Plugin %s could not report status: %s", self.id, exc)
            return {
                "configured": False,
                "can_send": False,
                "can_receive": False,
                "error": "status unavailable",
            }

    def config(self, db: sqlite3.Connection | None = None) -> dict[str, Any]:
        return self.read_config(db).model_dump(mode="json")

    def save_config(self, db: sqlite3.Connection | None, payload: dict[str, Any]) -> dict[str, Any]:
        validated = self.config_model.model_validate(payload)
        self.write_config(db, validated)
        return self.config(db)

    def config_schema(self) -> dict[str, Any]:
        return self.config_model.model_json_schema()

    def inbound_channel(self) -> InboundChannel | None:
        if self.inbound_factory is None:
            return None
        try:
            return self.inbound_factory()
        except Exception as exc:
            logger.warning("Plugin %s inbound adapter unavailable: %s", self.id, exc)
            return None

    def describe(self, db: sqlite3.Connection | None = None) -> dict[str, Any]:
        status = self.status(db)
        return {
            "id": self.id,
            "display_name": self.display_name,
            "version": self.version,
            "description": self.description,
            "kind": self.kind,
            "capabilities": list(self.capabilities),
            "enabled": self.is_enabled(),
            "config_schema": self.config_schema(),
            "configured": bool(status.get("configured")),
            "can_send": bool(status.get("can_send")),
            "can_receive": bool(status.get("can_receive")),
            "status": status,
            "actions": list(self.action_ids),
            "docs_path": self.docs_path,
        }
