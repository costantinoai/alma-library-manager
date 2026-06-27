"""Canonical paper-row update primitives.

Paper metadata arrives from several source adapters. Most source writes
must be fill-only: use a value only when the local field is still empty,
and use max-only for counters where larger source counts are improvements.
Keeping that policy here prevents each source runner from hand-rolling a
slightly different ``UPDATE papers SET ... CASE`` block.
"""

from __future__ import annotations

import sqlite3
from datetime import datetime
from typing import Any, Mapping

from alma.core.utils import clean_display_text

# Display-text columns on ``papers`` that get HTML-strip + NFC + dotless-ı repair
# at write time (mirrors the frontend repair allowlist). Identifier / URL / date
# columns are deliberately excluded — they must round-trip byte-for-byte.
_DISPLAY_TEXT_FIELDS = frozenset({"title", "authors", "journal", "abstract", "tldr"})


def _normalize_write_value(field: str, value: Any) -> Any:
    """Strip strings; additionally clean display-text fields (HTML + NFC + dotless-ı)."""
    if not isinstance(value, str):
        return value
    if field in _DISPLAY_TEXT_FIELDS:
        return clean_display_text(value).strip()
    return value.strip()


def _is_empty(value: Any) -> bool:
    if value is None:
        return True
    if isinstance(value, str):
        return not value.strip()
    return False


def _usable(value: Any) -> bool:
    if value is None:
        return False
    if isinstance(value, str):
        return bool(value.strip())
    return True


def _is_jan_first(value: Any) -> bool:
    """True iff ``value`` is a YYYY-01-01 string (the year-only fallback)."""
    if not isinstance(value, str):
        return False
    return value.strip().endswith("-01-01")


def fill_only_update_paper(
    conn: sqlite3.Connection,
    paper_id: str,
    *,
    fill_fields: Mapping[str, Any] | None = None,
    fill_null_fields: Mapping[str, Any] | None = None,
    max_int_fields: Mapping[str, Any] | None = None,
    always_fields: Mapping[str, Any] | None = None,
    prefer_specific_date_fields: Mapping[str, Any] | None = None,
    touch_updated_at: bool = True,
) -> list[str]:
    """Apply canonical fill-only updates to one ``papers`` row.

    Returns the field names that changed. ``fill_fields`` writes only
    when the existing value is NULL/blank; ``fill_null_fields`` writes
    only when the existing value is NULL; ``max_int_fields`` writes only
    when the new integer is larger than the current value;
    ``always_fields`` is for source-owned fields that should replace the
    local value only when the caller has already decided that is correct;
    ``prefer_specific_date_fields`` upgrades a stored ``YYYY-01-01``
    year-only fallback to a full ``YYYY-MM-DD`` date when the source
    provides one (and otherwise behaves like ``fill_fields``).
    """

    # Catch caller mistakes early: the same field key in two mode-dicts
    # would silently let the later loop overwrite the earlier one's
    # decision. Always-loud rather than ambiguous-quiet.
    field_groups = (
        ("fill_fields", fill_fields),
        ("fill_null_fields", fill_null_fields),
        ("max_int_fields", max_int_fields),
        ("always_fields", always_fields),
        ("prefer_specific_date_fields", prefer_specific_date_fields),
    )
    seen_in: dict[str, str] = {}
    fields: list[str] = []
    for group_name, group in field_groups:
        if not group:
            continue
        for key in group.keys():
            key_str = str(key)
            prior = seen_in.get(key_str)
            if prior is not None and prior != group_name:
                raise ValueError(
                    f"fill_only_update_paper: field {key_str!r} appears in "
                    f"both {prior!r} and {group_name!r} — pick exactly one mode"
                )
            seen_in[key_str] = group_name
            fields.append(key_str)
    fields = list(dict.fromkeys(fields))
    if not fields:
        return []

    select_cols = ", ".join(fields)
    row = conn.execute(
        f"SELECT {select_cols} FROM papers WHERE id = ?",
        (paper_id,),
    ).fetchone()
    if row is None:
        return []

    updates: dict[str, Any] = {}

    for field, value in (fill_fields or {}).items():
        if _usable(value) and _is_empty(row[field]):
            updates[str(field)] = _normalize_write_value(str(field), value)

    for field, value in (fill_null_fields or {}).items():
        if _usable(value) and row[field] is None:
            updates[str(field)] = _normalize_write_value(str(field), value)

    for field, value in (max_int_fields or {}).items():
        try:
            new_value = int(value)
        except (TypeError, ValueError):
            continue
        try:
            old_value = int(row[field] or 0)
        except (TypeError, ValueError):
            old_value = 0
        if new_value > old_value:
            updates[str(field)] = new_value

    for field, value in (always_fields or {}).items():
        if not _usable(value):
            continue
        normalized = _normalize_write_value(str(field), value)
        if row[field] != normalized:
            updates[str(field)] = normalized

    for field, value in (prefer_specific_date_fields or {}).items():
        if not _usable(value):
            continue
        normalized = _normalize_write_value(str(field), value)
        existing = row[field]
        # Upgrade only when the new date is a real day (not Jan 1) and
        # the existing slot is empty or itself a Jan-1 fallback. A new
        # Jan-1 still fills an empty slot (we have nothing better) but
        # never overwrites an already-stored date.
        if not _is_jan_first(normalized):
            if _is_empty(existing) or _is_jan_first(existing):
                if existing != normalized:
                    updates[str(field)] = normalized
        elif _is_empty(existing):
            updates[str(field)] = normalized

    if not updates:
        return []

    changed = list(updates.keys())
    assignments = [f"{field} = ?" for field in changed]
    params = [updates[field] for field in changed]
    if touch_updated_at:
        assignments.append("updated_at = ?")
        params.append(datetime.utcnow().isoformat())
    params.append(paper_id)
    conn.execute(
        f"UPDATE papers SET {', '.join(assignments)} WHERE id = ?",
        params,
    )
    return changed
