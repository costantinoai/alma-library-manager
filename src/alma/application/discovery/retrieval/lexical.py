"""Lexical retrieval channel — keyword/title search via OpenAlex.

Split out of the discovery god-module (D-9); pure move.
"""

from __future__ import annotations

import sqlite3
from datetime import datetime

from alma.discovery import openalex_related

from ..seed_profile import _extract_keywords


def _retrieve_lexical_channel(
    db: sqlite3.Connection,
    lens: dict,
    seeds: list[dict],
    *,
    limit: int,
) -> list[dict]:
    config = lens.get("context_config") or {}
    explicit_topics = config.get("topics") if isinstance(config.get("topics"), list) else None
    if lens["context_type"] == "topic_keyword":
        keyword = str(config.get("keyword") or config.get("query") or "").strip()
        explicit_topics = [keyword] if keyword else []
    topics = _extract_keywords(seeds, explicit=explicit_topics, max_keywords=10)
    if not topics:
        return []
    results = openalex_related.search_works_by_topics(
        topics, limit=limit, from_year=datetime.utcnow().year - 3
    )
    # Stamp provenance so downstream `_derive_recommendation_provenance`
    # routes these to the `lexical` bucket instead of the un-tagged
    # `lens_retrieval` catch-all. `source_key` carries the actual query
    # so the per-source-key diversity cap can group same-query results.
    source_key = " OR ".join(topics[:10])
    for item in results:
        if not str(item.get("source_type") or "").strip():
            item["source_type"] = "lexical"
        if not str(item.get("source_api") or "").strip():
            item["source_api"] = "openalex"
        if not str(item.get("source_key") or "").strip():
            item["source_key"] = source_key
    return results
