"""Shared provider-quota forecasting and admission.

Planning, UI disablement, scheduler admission, and transport hard stops consume
this module. OpenAlex exposes a finite daily pool in credit units; other sources
currently expose only rate limits and therefore report an unknown/unbounded
daily pool.
"""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass
from typing import Any

from alma.core.network_policy import ExternalAccessError, network_access_enabled
from alma.openalex.http import SEARCH_COST_CREDITS

OPENALEX = "openalex"
_OPENALEX_SEARCH_TASKS = frozenset({"title_resolution", "corpus_metadata"})


class ProviderQuotaExceededError(ExternalAccessError):
    """Raised before a request/run whose known quota cannot cover its cost."""


@dataclass(frozen=True, slots=True)
class QuotaForecast:
    provider: str
    unit: str
    requests: int
    required: int
    remaining: int | None
    reserve: int
    available: int | None
    sufficient: bool
    known: bool
    network_enabled: bool
    reason: str | None = None

    def to_wire(self) -> dict[str, Any]:
        return {
            "provider": self.provider,
            "unit": self.unit,
            "requests": self.requests,
            "required": self.required,
            "remaining": self.remaining,
            "reserve": self.reserve,
            "available": self.available,
            "sufficient": self.sufficient,
            "known": self.known,
            "network_enabled": self.network_enabled,
            "reason": self.reason,
        }


def _has_source(sources: Iterable[str], source: str) -> bool:
    needle = source.strip().lower()
    return any(needle in str(value or "").strip().lower() for value in sources)


def _openalex_request_count(
    expected_requests: dict[str, int],
    *,
    selected: int,
    sources: Iterable[str],
) -> int:
    direct = sum(
        max(0, int(count or 0))
        for source, count in expected_requests.items()
        if "openalex" in str(source or "").lower()
    )
    if direct:
        return direct
    return max(0, int(selected)) if _has_source(sources, OPENALEX) else 0


def forecast_operation_quota(
    *,
    task_key: str,
    sources: Iterable[str],
    selected: int,
    expected_requests: dict[str, int],
    reserve: int = 0,
) -> QuotaForecast | None:
    """Forecast finite daily quota for one bounded operation.

    Mixed-source operations use the selected-item ceiling for OpenAlex when ETA
    cannot isolate its lane. Title search costs 10 credits per request; other
    list/filter calls cost one. This is a safe maximum, not an average hidden
    behind optimistic provider success assumptions.
    """

    source_tuple = tuple(sources)
    if not source_tuple:
        return None
    enabled = network_access_enabled()
    requests = _openalex_request_count(
        expected_requests,
        selected=selected,
        sources=source_tuple,
    )
    if requests <= 0:
        if enabled:
            return None
        return QuotaForecast(
            provider="network",
            unit="requests",
            requests=0,
            required=0,
            remaining=None,
            reserve=0,
            available=0,
            sufficient=False,
            known=True,
            network_enabled=False,
            reason="External network access is disabled in Settings.",
        )

    credits_each = SEARCH_COST_CREDITS if task_key in _OPENALEX_SEARCH_TASKS else 1
    return forecast_openalex_quota(
        list_requests=requests if credits_each == 1 else 0,
        search_requests=requests if credits_each == SEARCH_COST_CREDITS else 0,
        reserve=reserve,
    )


def forecast_openalex_quota(
    *,
    list_requests: int = 0,
    search_requests: int = 0,
    reserve: int = 0,
) -> QuotaForecast:
    """Forecast a mixed OpenAlex workload in canonical credit units."""

    list_count = max(0, int(list_requests))
    search_count = max(0, int(search_requests))
    requests = list_count + search_count
    required = list_count + search_count * SEARCH_COST_CREDITS
    enabled = network_access_enabled()
    from alma.core.http_sources import provider_remaining_credits

    remaining = provider_remaining_credits(OPENALEX)
    available = None if remaining is None else max(0, remaining - max(0, int(reserve)))
    known = remaining is not None
    sufficient = required <= 0 or (enabled and (available is None or available >= required))
    reason = None
    if required > 0 and not enabled:
        reason = "External network access is disabled in Settings."
    elif known and not sufficient:
        reason = (
            f"Needs {required:,} OpenAlex credits; {available:,} available "
            f"after the {max(0, int(reserve)):,}-credit reserve."
        )
    return QuotaForecast(
        provider=OPENALEX,
        unit="credits",
        requests=requests,
        required=required,
        remaining=remaining,
        reserve=max(0, int(reserve)),
        available=available,
        sufficient=sufficient,
        known=known,
        network_enabled=enabled,
        reason=reason,
    )


def refresh_quota_forecast(
    raw: dict[str, Any] | None,
    *,
    network_required: bool = False,
) -> QuotaForecast | None:
    """Refresh remaining/admission fields without replanning request cost."""

    if not raw:
        if network_required and not network_access_enabled():
            return QuotaForecast(
                provider="network",
                unit="requests",
                requests=0,
                required=0,
                remaining=None,
                reserve=0,
                available=0,
                sufficient=False,
                known=True,
                network_enabled=False,
                reason="External network access is disabled in Settings.",
            )
        return None
    provider = str(raw.get("provider") or "")
    requests = max(0, int(raw.get("requests") or 0))
    required = max(0, int(raw.get("required") or 0))
    reserve = max(0, int(raw.get("reserve") or 0))
    if provider == OPENALEX:
        # Preserve the already-planned mixed cost exactly; only live availability
        # changes between durable Health snapshot rebuilds.
        enabled = network_access_enabled()
        from alma.core.http_sources import provider_remaining_credits

        remaining = provider_remaining_credits(OPENALEX)
        available = None if remaining is None else max(0, remaining - reserve)
        known = remaining is not None
        sufficient = required <= 0 or (
            enabled and (available is None or available >= required)
        )
        reason = None
        if required > 0 and not enabled:
            reason = "External network access is disabled in Settings."
        elif known and not sufficient:
            reason = (
                f"Needs {required:,} OpenAlex credits; {available:,} available "
                f"after the {reserve:,}-credit reserve."
            )
        return QuotaForecast(
            provider=provider,
            unit=str(raw.get("unit") or "credits"),
            requests=requests,
            required=required,
            remaining=remaining,
            reserve=reserve,
            available=available,
            sufficient=sufficient,
            known=known,
            network_enabled=enabled,
            reason=reason,
        )

    enabled = network_access_enabled()
    return QuotaForecast(
        provider=provider or "network",
        unit=str(raw.get("unit") or "requests"),
        requests=requests,
        required=required,
        remaining=None,
        reserve=reserve,
        available=None if enabled else 0,
        sufficient=enabled,
        known=True,
        network_enabled=enabled,
        reason=None if enabled else "External network access is disabled in Settings.",
    )


def provider_can_start(source: str, *, required: int = 1) -> bool:
    """Cheap source-lane admission used by interactive fallback search."""

    if not network_access_enabled():
        return False
    if str(source or "").strip().lower() != OPENALEX:
        return True
    from alma.core.http_sources import provider_remaining_credits

    remaining = provider_remaining_credits(OPENALEX)
    return remaining is None or remaining >= max(0, int(required))


def require_provider_quota(source: str, *, required: int = 1) -> None:
    """Hard stop immediately before a paid provider call."""

    if provider_can_start(source, required=required):
        return
    from alma.core.http_sources import provider_remaining_credits

    remaining = provider_remaining_credits(source)
    raise ProviderQuotaExceededError(
        f"{source} quota cannot cover this request "
        f"({max(0, int(required))} required, {remaining or 0} remaining)."
    )
