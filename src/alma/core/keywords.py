"""Pure helper for OpenAlex keyword parsing (Task 10 / B1 — DRY consolidation).

`papers.keywords` stores OpenAlex keyword *display-name* strings — usually a JSON
list of strings, but legacy/import rows may store a ``,``/``;``-delimited string,
and the raw OpenAlex feed payload carries a list of ``{keyword|display_name}``
objects.

:func:`parse_keywords` is the SINGLE parser for all of those shapes. It coerces any
of them to a lowercase, stripped, order-preserving list (no dedupe, no semantic
filtering — callers that need dedupe/limit keep that in their own thin wrapper). It
consolidates three previously-duplicated parsers
(``signal_projection._parse_keywords`` / ``discovery.scoring._candidate_keywords`` /
``authors._candidate_projection_keywords``) into one greppable source of truth.

History: a ``clean_openalex_keywords`` layer (strip Wikidata "(...)" disambiguations
+ a field-generic stoplist) was prototyped and A/B-tested against liked-vs-dismissed
separation. The A/B showed cleaning is signal-neutral (disambiguation is a *bijective
token rename*, so an atomic producer+consumer flip leaves Cohen's d unchanged) or
mildly harmful (the field stoplist drops terms that are real taste for the user), so
it was dropped rather than shipped unused. See
``tasks/10_TOPIC_KEYWORD_DATA_QUALITY_AND_SCALE.md`` §B.1.
"""

from __future__ import annotations

import json
from typing import Any

__all__ = ["parse_keywords"]


def _coerce(raw: Any) -> Any:
    """JSON-decode / bytes-decode a stored keywords value; pass lists/dicts through.

    Matches ``signal_projection._coerce_value`` so the consolidation is
    behaviour-preserving for the producer/DB side (JSON-string or delimited text).
    """
    if isinstance(raw, (list, dict)):
        return raw
    if raw is None:
        return None
    if isinstance(raw, bytes):
        raw = raw.decode("utf-8", errors="replace")
    if isinstance(raw, str):
        text = raw.strip()
        if not text:
            return None
        try:
            return json.loads(text)
        except json.JSONDecodeError:
            return text
    return raw


def _item_to_text(item: Any) -> str:
    """One keyword item -> its display string.

    OpenAlex keyword *objects* arrive as ``{"keyword"|"display_name": ...}`` (the
    raw feed / monitor-match path); stored rows arrive as plain strings.
    """
    if isinstance(item, dict):
        return str(item.get("keyword") or item.get("display_name") or "").strip()
    return str(item or "").strip()


def parse_keywords(raw: Any) -> list[str]:
    """Parse a stored/raw keywords value into a lowercase, stripped, ordered list.

    No dedupe, no semantic filtering. Empty items are dropped. Accepts a JSON list,
    a ``,``/``;``-delimited string, or a list (or bare instance) of OpenAlex keyword
    dicts.
    """
    value = _coerce(raw)
    if isinstance(value, list):
        items: list[Any] = value
    elif isinstance(value, str):
        items = value.replace(";", ",").split(",")
    elif isinstance(value, dict):
        items = [value]  # a bare OpenAlex keyword object
    else:
        items = []
    out: list[str] = []
    for item in items:
        text = _item_to_text(item).lower()
        if text:
            out.append(text)
    return out
