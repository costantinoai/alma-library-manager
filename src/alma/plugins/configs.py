"""Shared integration-configuration primitives."""

from __future__ import annotations

from pydantic import BaseModel, ConfigDict


class StrictPluginConfig(BaseModel):
    """Every plugin config rejects unknown fields; typos never persist."""

    model_config = ConfigDict(extra="forbid")
