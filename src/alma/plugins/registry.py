"""The one explicit catalogue of ALMa external integrations."""

from __future__ import annotations

import sqlite3
from typing import Any

from alma.application.pdf_schema import TIER_ORDER, PdfSource
from alma.plugins.email import EMAIL_PLUGIN
from alma.plugins.manifest import PDF_SOURCE, RECEIVE, SEND, Capability, PluginManifest
from alma.plugins.open_access import OPEN_ACCESS_PLUGIN
from alma.plugins.shadow_libraries import SHADOW_LIBRARIES_PLUGIN
from alma.plugins.slack import SLACK_PLUGIN

PLUGINS: tuple[PluginManifest, ...] = (
    SLACK_PLUGIN,
    EMAIL_PLUGIN,
    OPEN_ACCESS_PLUGIN,
    SHADOW_LIBRARIES_PLUGIN,
)


class PluginRegistry:
    """Stateless lookup over the explicit integration manifests.

    Adapters are built from current credentials on every access. Rotating a
    token therefore takes effect immediately; the registry never caches a
    configured client.
    """

    def __init__(self, manifests: tuple[PluginManifest, ...] = PLUGINS):
        ids = [manifest.id for manifest in manifests]
        if len(ids) != len(set(ids)):
            raise ValueError("Plugin ids must be unique")
        self._manifests = manifests

    def ids(self) -> list[str]:
        return [manifest.id for manifest in self._manifests]

    def get(self, plugin_id: str) -> PluginManifest:
        for manifest in self._manifests:
            if manifest.id == plugin_id:
                return manifest
        raise KeyError(
            f"Plugin '{plugin_id}' is not registered. Available plugins: {', '.join(self.ids())}"
        )

    def all(self) -> tuple[PluginManifest, ...]:
        return self._manifests

    def with_capability(self, capability: Capability) -> list[PluginManifest]:
        return [manifest for manifest in self._manifests if manifest.can(capability)]

    def describe_all(self, db: sqlite3.Connection | None = None) -> list[dict[str, Any]]:
        return [manifest.describe(db) for manifest in self._manifests]

    def set_enabled(self, plugin_id: str, enabled: bool) -> dict[str, Any]:
        manifest = self.get(plugin_id)
        manifest.set_enabled(enabled)
        return manifest.describe()

    def inbound_channels(self):
        channels = []
        for manifest in self.with_capability(RECEIVE):
            if not manifest.is_enabled():
                continue
            channel = manifest.inbound_channel()
            if channel is not None:
                channels.append(channel)
        return channels

    def enabled_delivery_plugins(self) -> list[PluginManifest]:
        return [
            manifest
            for manifest in self.with_capability(SEND)
            if manifest.is_enabled() and manifest.status().get("can_send")
        ]

    def pdf_fetch_enabled(self) -> bool:
        """True when at least one PDF-source plugin is switched on."""
        return any(manifest.is_enabled() for manifest in self.with_capability(PDF_SOURCE))

    def pdf_sources(self) -> list[tuple[PluginManifest, PdfSource]]:
        """Every enabled plugin's PDF sources, in run order.

        Open-tier sources come before any shadow-tier source whatever plugin
        contributes them; within a tier, plugin order then each plugin's own
        source order is kept (a stable sort). Built from current config on
        every call, like the other seams.
        """
        pairs = [
            (manifest, source)
            for manifest in self.with_capability(PDF_SOURCE)
            if manifest.is_enabled()
            for source in manifest.pdf_sources()
        ]
        return sorted(pairs, key=lambda pair: TIER_ORDER.get(pair[1].tier, len(TIER_ORDER)))


_registry: PluginRegistry | None = None


def get_plugin_registry() -> PluginRegistry:
    global _registry
    if _registry is None:
        _registry = PluginRegistry()
    return _registry


def plugin_enabled(plugin_id: str) -> bool:
    """The shared activation seam used by Alerts and Inbox adapters."""
    return get_plugin_registry().get(plugin_id).is_enabled()
