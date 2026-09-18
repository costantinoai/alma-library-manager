"""Shared integration-configuration primitives."""

from __future__ import annotations

import sqlite3
from collections.abc import Callable, Mapping

from pydantic import BaseModel, ConfigDict


class StrictPluginConfig(BaseModel):
    """Every plugin config rejects unknown fields; typos never persist."""

    model_config = ConfigDict(extra="forbid")


def plugin_setting_key(plugin_id: str, field_name: str) -> str:
    """Canonical flat settings key for one plugin config field."""
    return f"plugins.{plugin_id}.{field_name}"


def settings_backed_config(
    plugin_id: str,
    model: type[StrictPluginConfig],
    *,
    secrets: Mapping[str, str] | None = None,
) -> tuple[
    Callable[[sqlite3.Connection | None], StrictPluginConfig],
    Callable[[sqlite3.Connection | None, BaseModel], None],
]:
    """``(read_config, write_config)`` for a plugin whose config lives in settings.

    Each field ``f`` is stored under ``plugins.<plugin_id>.<f>`` (defaults in
    ``DEFAULT_SETTINGS``), except the fields named in ``secrets`` (field →
    secret-store key), which live in the secret store and are read back
    MASKED. Writing a still-masked value (``****…``) leaves the stored secret
    untouched; writing an empty one deletes it. Newer plugins use this instead
    of hand-mapping fields to legacy flat keys.
    """
    secret_fields = dict(secrets or {})

    def read_config(_db: sqlite3.Connection | None) -> StrictPluginConfig:
        from alma.config import DEFAULT_SETTINGS, get_setting
        from alma.core.secrets import get_secret, mask_secret

        values: dict = {}
        for name, field in model.model_fields.items():
            if name in secret_fields:
                values[name] = mask_secret(get_secret(secret_fields[name]), suffix=4) or ""
                continue
            key = plugin_setting_key(plugin_id, name)
            values[name] = get_setting(key, DEFAULT_SETTINGS.get(key, field.default))
        return model.model_validate(values)

    def write_config(_db: sqlite3.Connection | None, config: BaseModel) -> None:
        from alma.config import update_settings
        from alma.core.secrets import delete_secret, set_secret

        parsed = model.model_validate(config)
        updates: dict = {}
        for name in model.model_fields:
            value = getattr(parsed, name)
            if name in secret_fields:
                text = str(value or "").strip()
                if text.startswith("****"):
                    continue  # the masked read-back: unchanged
                if text:
                    set_secret(secret_fields[name], text)
                else:
                    delete_secret(secret_fields[name])
                continue
            updates[plugin_setting_key(plugin_id, name)] = value.strip() if isinstance(value, str) else value
        if updates:
            update_settings(updates)

    return read_config, write_config
