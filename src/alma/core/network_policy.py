"""One runtime policy for every outbound network transport.

Persistent switch lives in core settings. ``ALMA_DISABLE_NETWORK`` is ops hard
override: it can turn access off, never force it on over user's stored choice.
Transports call :func:`require_network_access` immediately before real I/O so
cached/local work remains available while outbound traffic is barred.

A second, narrower question lives here too: may the SCHEDULER start network
work nobody asked for (:func:`unattended_network_enabled`)? That one is per
profile, because every profile shares one provider key.
"""

from __future__ import annotations

import os
from dataclasses import dataclass

from alma.config import get_env_profile, get_setting

NETWORK_ACCESS_SETTING = "network_access_enabled"
NETWORK_DISABLE_ENV = "ALMA_DISABLE_NETWORK"
UNATTENDED_NETWORK_ENV = "ALMA_UNATTENDED_NETWORK"
_TRUTHY = {"1", "true", "yes", "on"}


class ExternalAccessError(RuntimeError):
    """Base for admission failures that must stop an external operation."""


class NetworkAccessDisabledError(ExternalAccessError):
    """Raised before outbound call when global switch is off."""


def _env_disables_network() -> bool:
    value = str(os.getenv(NETWORK_DISABLE_ENV, "")).strip().lower()
    return value in _TRUTHY


def network_access_enabled() -> bool:
    """Return effective outbound-network state."""

    if _env_disables_network():
        return False
    return get_setting(NETWORK_ACCESS_SETTING, True) is True


def require_network_access(source: str) -> None:
    """Fail before real I/O when outbound access is disabled."""

    if network_access_enabled():
        return
    label = str(source or "external service").strip() or "external service"
    raise NetworkAccessDisabledError(
        f"External network access is disabled; {label} request was not sent. "
        "Enable it in Settings → Connections to retry."
    )


def unattended_network_enabled() -> bool:
    """May the scheduler START network work nobody asked for, on this profile?

    Every profile shares ONE provider key: the prod container, the dev server,
    worktree copies and test profiles. A copy seeded from prod carries prod's
    authors, monitors and pending ledgers, so each copy running the same
    clock-driven sweeps spends the same shared daily quota again. On 2026-09-19 a
    worktree copy spent 74% of it overnight (task 85). So only the ``prod``
    profile runs them by default. ``ALMA_UNATTENDED_NETWORK`` overrides either
    way: ``1`` to exercise background sweeps on a copy, ``0`` to hold them in prod.

    Work a user asked for, whether a click or a chain one of their actions
    started, is not unattended and never consults this. The admission gate that
    applies it is ``api.scheduler.scheduled_network_refusal``.
    """
    raw = str(os.getenv(UNATTENDED_NETWORK_ENV, "")).strip().lower()
    if raw:
        return raw in _TRUTHY
    return get_env_profile() == "prod"


@dataclass(frozen=True, slots=True)
class NetworkPolicyStatus:
    enabled: bool
    settings_enabled: bool
    forced_off_by_env: bool
    # Whether the scheduler may start network work on its own on this profile,
    # and which profile that is — so Health can say why a dev copy's background
    # sweeps never run instead of leaving them silently idle.
    unattended_enabled: bool
    profile: str

    def to_wire(self) -> dict[str, bool | str]:
        return {
            "enabled": self.enabled,
            "settings_enabled": self.settings_enabled,
            "forced_off_by_env": self.forced_off_by_env,
            "unattended_enabled": self.unattended_enabled,
            "profile": self.profile,
        }


def network_policy_status() -> NetworkPolicyStatus:
    """Return truthful stored/effective state for API surfaces."""

    stored = get_setting(NETWORK_ACCESS_SETTING, True) is True
    forced_off = _env_disables_network()
    return NetworkPolicyStatus(
        enabled=stored and not forced_off,
        settings_enabled=stored,
        forced_off_by_env=forced_off,
        unattended_enabled=unattended_network_enabled(),
        profile=get_env_profile(),
    )
