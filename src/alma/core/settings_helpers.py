"""Canonical readers for the KV settings stores (discovery_settings, feed
monitor defaults, source policy, …).

These were duplicated as private `_setting_bool` / `_setting_int` /
`_setting_float` helpers across `application/feed.py`, `discovery/source_search.py`
and others. One source of truth here; callers import and alias to their legacy
private names. ``lo``/``hi`` are optional — pass both to clamp (the feed
monitor-defaults behaviour); omit them for an unbounded read (the source-search
weight behaviour).
"""

from __future__ import annotations

from alma.core.scoring_math import clamp

_TRUTHY = {"1", "true", "yes", "on"}


def setting_bool(settings: dict | None, key: str, default: bool) -> bool:
    raw = (settings or {}).get(key)
    if raw is None:
        return default
    return str(raw).strip().lower() in _TRUTHY


def setting_int(
    settings: dict | None, key: str, default: int,
    lo: int | None = None, hi: int | None = None,
) -> int:
    raw = (settings or {}).get(key)
    if raw is None:
        return default
    try:
        value = int(raw)
    except (TypeError, ValueError):
        return default
    if lo is not None and hi is not None:
        return int(clamp(value, lo, hi))
    return value


def setting_float(
    settings: dict | None, key: str, default: float,
    lo: float | None = None, hi: float | None = None,
) -> float:
    raw = (settings or {}).get(key)
    if raw is None:
        return default
    try:
        value = float(raw)
    except (TypeError, ValueError):
        return default
    if lo is not None and hi is not None:
        return clamp(value, lo, hi)
    return value
