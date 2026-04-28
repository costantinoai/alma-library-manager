"""Single-responsibility author profile writer.

After a deep refresh we want to persist *every* field OpenAlex returns
about an author — canonical display name, institutional history,
affiliation, citations, works count, h-index, topics, cited-by-year
series, ORCID — plus recompute the SPECTER2 centroid from their works.

Before this helper, different callers wrote overlapping subsets with
different overwrite semantics:
- `_apply_author_resolution_result` used `COALESCE(NULLIF(...))` for
  string fields and `MAX(COALESCE(..., 0), ?)` for numerics.
- The inline block in `_refresh_author_cache_impl` blindly overwrote
  citedby / h_index / works_count with whatever OpenAlex returned
  (including `0` on a transient miss) and didn't touch
  `display_name`, `orcid`, `cited_by_year`, `institutions`.

This module centralises the contract so a deep refresh actually
refreshes everything and a partial / failed OpenAlex response never
zeroes out existing data.
"""

from __future__ import annotations

import json
import logging
import sqlite3
from datetime import datetime
from typing import Any, Optional

logger = logging.getLogger(__name__)


def _json_or_none(value: Any) -> Optional[str]:
    if value is None:
        return None
    try:
        return json.dumps(value, ensure_ascii=False)
    except (TypeError, ValueError):
        return None


def apply_author_profile_update(
    db: sqlite3.Connection,
    author_id: str,
    profile: dict[str, Any] | None,
) -> dict[str, Any]:
    """Persist every field returned by `fetch_author_profile`.

    Semantics:

    * **Strings** (name, affiliation, orcid) — COALESCE-based:
      incoming value only overwrites when the current row is NULL or
      empty. Prevents a transient OpenAlex miss from blanking a name
      we already have.
    * **Numerics** (citedby, h_index, works_count) — MAX-based: only
      upgrades. Monotonic counters from OpenAlex can temporarily dip
      during their rebuild windows; MAX keeps us on the high-water mark.
    * **Topics / interests** — COALESCE, formatted as JSON.
    * **Institutions / cited_by_year** — replaced wholesale when
      OpenAlex returned a non-empty payload; left alone otherwise.
    * **last_refreshed_at** — always stamped.

    Returns a summary dict listing which fields changed so the caller
    can surface them in Activity logs.
    """
    author_key = str(author_id or "").strip()
    if not author_key:
        return {"updated": [], "skipped_reason": "empty_author_id"}
    if not profile:
        db.execute(
            "UPDATE authors SET last_fetched_at = ? WHERE id = ?",
            (datetime.utcnow().isoformat(), author_key),
        )
        return {"updated": ["last_fetched_at"], "skipped_reason": "empty_profile"}

    fields: list[str] = []
    params: list[Any] = []
    changed: list[str] = []

    # Canonical name — OpenAlex's display_name is authoritative when we
    # currently have either the placeholder (id string) or an empty row,
    # but we never clobber a user-curated override. "Canonical" here =
    # upgrade-only.
    display_name = str(profile.get("display_name") or "").strip()
    if display_name:
        fields.append("name = CASE WHEN COALESCE(NULLIF(TRIM(name), ''), '') IN ('', ?) THEN ? ELSE name END")
        params.extend([author_key, display_name])
        changed.append("name")

    affiliation = str(profile.get("affiliation") or "").strip()
    if affiliation:
        fields.append("affiliation = COALESCE(NULLIF(TRIM(affiliation), ''), ?)")
        params.append(affiliation)
        changed.append("affiliation")
    else:
        # Nothing new — preserve existing.
        pass

    orcid = str(profile.get("orcid") or "").strip()
    if orcid:
        from alma.core.utils import normalize_orcid

        normalized = normalize_orcid(orcid)
        if normalized:
            fields.append("orcid = COALESCE(NULLIF(TRIM(orcid), ''), ?)")
            params.append(normalized)
            changed.append("orcid")

    # Monotonic counters — MAX only upgrades. A 0 from a transient OA
    # outage can't zero our h-index / citations.
    def _monotonic(col: str, value: Any) -> None:
        try:
            numeric = int(value or 0)
        except (TypeError, ValueError):
            return
        fields.append(f"{col} = MAX(COALESCE({col}, 0), ?)")
        params.append(numeric)
        changed.append(col)

    if "citedby" in profile or "cited_by_count" in profile:
        _monotonic("citedby", profile.get("citedby", profile.get("cited_by_count", 0)))
    if "h_index" in profile:
        _monotonic("h_index", profile.get("h_index", 0))
    if "works_count" in profile:
        _monotonic("works_count", profile.get("works_count", 0))

    # Topics — overwrite with fresh JSON when non-empty, preserve otherwise.
    interests = profile.get("interests")
    if isinstance(interests, list) and interests:
        serialised = _json_or_none(interests)
        if serialised:
            fields.append("interests = ?")
            params.append(serialised)
            changed.append("interests")

    # Institution history — replace wholesale when fresh list is non-empty.
    institutions = profile.get("institutions")
    if isinstance(institutions, list) and institutions:
        serialised = _json_or_none(institutions)
        if serialised:
            fields.append("institutions = ?")
            params.append(serialised)
            changed.append("institutions")

    # Cited-by-year time series — same pattern.
    cited_by_year = profile.get("cited_by_year")
    if cited_by_year:
        serialised = _json_or_none(cited_by_year)
        if serialised:
            fields.append("cited_by_year = ?")
            params.append(serialised)
            changed.append("cited_by_year")

    # url_picture — thumbnail if OpenAlex returned one.
    url_picture = str(profile.get("url_picture") or "").strip()
    if url_picture:
        fields.append("url_picture = COALESCE(NULLIF(TRIM(url_picture), ''), ?)")
        params.append(url_picture)
        changed.append("url_picture")

    fields.append("last_fetched_at = ?")
    params.append(datetime.utcnow().isoformat())

    if not fields:
        return {"updated": [], "skipped_reason": "no_changes"}

    params.append(author_key)
    sql = f"UPDATE authors SET {', '.join(fields)} WHERE id = ?"
    try:
        db.execute(sql, params)
        if db.in_transaction:
            db.commit()
    except sqlite3.OperationalError as exc:
        logger.warning("apply_author_profile_update failed for %s: %s", author_key, exc)
        return {"updated": [], "error": str(exc)}

    return {"updated": changed}


def refresh_author_centroid_safe(
    db: sqlite3.Connection,
    openalex_id: Optional[str],
) -> bool:
    """Convenience wrapper that swallows import/DB errors.

    Call this at the end of a deep refresh whether or not the refresh
    went through the modern OpenAlex backfill — the centroid should
    always reflect whatever embeddings we currently have for this
    author, and the helper is cheap when the embedding table is small
    or missing.
    """
    oid = str(openalex_id or "").strip()
    if not oid:
        return False
    try:
        from alma.application.author_backfill import refresh_author_centroid

        return bool(refresh_author_centroid(db, oid))
    except Exception as exc:
        logger.debug("centroid refresh failed for %s: %s", oid, exc)
        return False
