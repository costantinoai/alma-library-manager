"""One runtime policy for every outbound network transport.

Persistent switch lives in core settings. ``ALMA_DISABLE_NETWORK`` is ops hard
override: it can turn access off, never force it on over user's stored choice.
Transports call :func:`require_network_access` immediately before real I/O so
cached/local work remains available while outbound traffic is barred.
"""

from __future__ import annotations

import os
from dataclasses import dataclass

from alma.config import get_setting

NETWORK_ACCESS_SETTING = "network_access_enabled"
NETWORK_DISABLE_ENV = "ALMA_DISABLE_NETWORK"


class ExternalAccessError(RuntimeError):
    """Base for admission failures that must stop an external operation."""


class NetworkAccessDisabledError(ExternalAccessError):
    """Raised before outbound call when global switch is off."""


def _env_disables_network() -> bool:
    value = str(os.getenv(NETWORK_DISABLE_ENV, "")).strip().lower()
    return value in {"1", "true", "yes", "on"}


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


@dataclass(frozen=True, slots=True)
class NetworkPolicyStatus:
    enabled: bool
    settings_enabled: bool
    forced_off_by_env: bool

    def to_wire(self) -> dict[str, bool]:
        return {
            "enabled": self.enabled,
            "settings_enabled": self.settings_enabled,
            "forced_off_by_env": self.forced_off_by_env,
        }


def network_policy_status() -> NetworkPolicyStatus:
    """Return truthful stored/effective state for API surfaces."""

    stored = get_setting(NETWORK_ACCESS_SETTING, True) is True
    forced_off = _env_disables_network()
    return NetworkPolicyStatus(
        enabled=stored and not forced_off,
        settings_enabled=stored,
        forced_off_by_env=forced_off,
    )
