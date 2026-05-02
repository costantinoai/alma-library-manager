"""Paper metadata merge helpers.

The corpus rehydration jobs use these helpers instead of ad-hoc UPDATE
statements so API fetchers share the same fill-only and no-op-write
semantics.
"""

from __future__ import annotations

import json
import sqlite3
from datetime import datetime
from typing import Any

from alma.core.utils import normalize_doi
from alma.openalex.client import _normalize_openalex_work_id, upsert_work_sidecars


def _text(value: Any) -> str:
    return str(value or "").strip()


def _canon_json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"), default=str)


def _as_int(value: Any) -> int | None:
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _as_float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _changed_json(current: Any, incoming: Any) -> bool:
    if incoming in (None, [], {}):
        return False
    current_text = _text(current)
    if not current_text:
        return True
    try:
        parsed_current = json.loads(current_text)
    except Exception:
        return True
    return _canon_json(parsed_current) != _canon_json(incoming)


def _sidecar_counts(conn: sqlite3.Connection, paper_id: str) -> dict[str, int]:
    row = conn.execute(
        """
        SELECT
            (SELECT COUNT(*) FROM publication_topics WHERE paper_id = ?) AS topics,
            (SELECT COUNT(*) FROM publication_institutions WHERE paper_id = ?) AS institutions,
            (SELECT COUNT(*) FROM publication_authors WHERE paper_id = ?) AS authorships,
            (SELECT COUNT(*) FROM publication_references WHERE paper_id = ?) AS refs
        """,
        (paper_id, paper_id, paper_id, paper_id),
    ).fetchone()
    if not row:
        return {"topics": 0, "institutions": 0, "authorships": 0, "references": 0}
    return {
        "topics": int(row["topics"] or 0),
        "institutions": int(row["institutions"] or 0),
        "authorships": int(row["authorships"] or 0),
        "references": int(row["refs"] or 0),
    }


def merge_openalex_work_metadata(
    conn: sqlite3.Connection,
    paper_id: str,
    work: dict[str, Any],
    *,
    hydrate_missing_sidecars: bool = True,
) -> dict[str, Any]:
    """Merge one normalized OpenAlex work into an existing paper row.

    Returns a compact summary with changed field names and write counts.
    Paper columns are updated only when a value is missing, has improved
    precision, or OpenAlex is the authoritative source for that scalar.
    Structured sidecars are populated only when currently absent.
    """
    row = conn.execute("SELECT * FROM papers WHERE id = ?", (paper_id,)).fetchone()
    if row is None:
        return {
            "paper_id": paper_id,
            "exists": False,
            "changed": False,
            "fields_filled": [],
            "paper_fields_changed": [],
            "sidecars_filled": [],
            "db_writes": 0,
        }

    updates: dict[str, Any] = {}
    paper_fields_changed: list[str] = []

    def current(field: str) -> Any:
        return row[field]

    def set_if_empty(field: str, value: Any, label: str | None = None) -> None:
        if value is None:
            return
        if isinstance(value, str) and not value.strip():
            return
        if _text(current(field)):
            return
        updates[field] = value
        paper_fields_changed.append(label or field)

    def set_if_diff(field: str, value: Any, label: str | None = None) -> None:
        if value is None:
            return
        if isinstance(value, str) and not value.strip():
            return
        if current(field) == value:
            return
        updates[field] = value
        paper_fields_changed.append(label or field)

    title = _text(work.get("title"))
    authors = _text(work.get("authors"))
    journal = _text(work.get("journal"))
    abstract = _text(work.get("abstract"))
    url = _text(work.get("pub_url"))
    doi = normalize_doi(work.get("doi"))
    openalex_id = _normalize_openalex_work_id(_text(work.get("openalex_id")))
    year = _as_int(work.get("year"))
    publication_date = _text(work.get("publication_date"))
    citations = _as_int(work.get("num_citations"))
    source_id = doi or url or title

    set_if_empty("title", title)
    set_if_empty("authors", authors)
    set_if_empty("journal", journal)
    set_if_empty("abstract", abstract)
    set_if_empty("url", url)
    set_if_empty("doi", doi)
    set_if_empty("openalex_id", openalex_id)
    set_if_empty("source_id", source_id)
    if year is not None and current("year") is None:
        updates["year"] = year
        paper_fields_changed.append("year")
    if publication_date:
        cur_pub = _text(current("publication_date"))
        if not cur_pub or (len(cur_pub) == 4 and publication_date.startswith(f"{cur_pub}-")):
            updates["publication_date"] = publication_date
            paper_fields_changed.append("publication_date")
    if citations is not None and citations > int(current("cited_by_count") or 0):
        updates["cited_by_count"] = citations
        paper_fields_changed.append("cited_by_count")

    set_if_empty("work_type", _text(work.get("type")))
    set_if_empty("language", _text(work.get("language")))

    open_access = work.get("open_access") if isinstance(work.get("open_access"), dict) else {}
    if open_access:
        set_if_diff("is_oa", 1 if open_access.get("is_oa") else 0)
        set_if_diff("oa_status", _text(open_access.get("oa_status")))
        set_if_diff("oa_url", _text(open_access.get("oa_url")))
    set_if_diff("is_retracted", 1 if work.get("is_retracted") else 0)
    fwci = _as_float(work.get("fwci"))
    if fwci is not None:
        set_if_diff("fwci", fwci)

    cited_pct = work.get("cited_by_percentile") if isinstance(work.get("cited_by_percentile"), dict) else {}
    pct_min = _as_float(cited_pct.get("min")) if cited_pct else None
    pct_max = _as_float(cited_pct.get("max")) if cited_pct else None
    if pct_min is not None:
        set_if_diff("cited_by_percentile_min", pct_min)
    if pct_max is not None:
        set_if_diff("cited_by_percentile_max", pct_max)

    ref_count = _as_int(work.get("referenced_works_count"))
    if ref_count is not None:
        set_if_diff("referenced_works_count", ref_count)
    inst_count = _as_int(work.get("institutions_distinct_count"))
    if inst_count is not None:
        set_if_diff("institutions_count", inst_count)
    country_count = _as_int(work.get("countries_distinct_count"))
    if country_count is not None:
        set_if_diff("countries_count", country_count)

    biblio = work.get("biblio") if isinstance(work.get("biblio"), dict) else {}
    if biblio:
        set_if_empty("volume", _text(biblio.get("volume")))
        set_if_empty("issue", _text(biblio.get("issue")))
        set_if_empty("first_page", _text(biblio.get("first_page")))
        set_if_empty("last_page", _text(biblio.get("last_page")))

    for field, value in (
        ("keywords", work.get("keywords")),
        ("sdgs", work.get("sdgs")),
        ("counts_by_year", work.get("counts_by_year")),
    ):
        if _changed_json(current(field), value):
            updates[field] = _canon_json(value)
            paper_fields_changed.append(field)

    db_writes = 0
    if updates:
        now = datetime.utcnow().isoformat()
        updates["fetched_at"] = now
        updates["updated_at"] = now
        assignments = ", ".join(f"{field} = ?" for field in updates)
        conn.execute(
            f"UPDATE papers SET {assignments} WHERE id = ?",
            (*updates.values(), paper_id),
        )
        db_writes += 1

    sidecars_filled: list[str] = []
    if hydrate_missing_sidecars:
        sidecar_payload: dict[str, Any] = {}
        counts = _sidecar_counts(conn, paper_id)
        topics = work.get("topics") if isinstance(work.get("topics"), list) else []
        institutions = work.get("institutions") if isinstance(work.get("institutions"), list) else []
        authorships = work.get("authorships") if isinstance(work.get("authorships"), list) else []
        referenced_works = work.get("referenced_works") if isinstance(work.get("referenced_works"), list) else []
        if topics and counts["topics"] <= 0:
            sidecar_payload["topics"] = topics
        if institutions and counts["institutions"] <= 0:
            sidecar_payload["institutions"] = institutions
        if authorships and counts["authorships"] <= 0:
            sidecar_payload["authorships"] = authorships
        if referenced_works and counts["references"] <= 0:
            sidecar_payload["referenced_works"] = referenced_works
        if sidecar_payload:
            sidecar_summary = upsert_work_sidecars(conn, paper_id, **sidecar_payload)
            for key, count in sidecar_summary.items():
                if int(count or 0) > 0:
                    sidecars_filled.append(f"sidecars.{key}")
                    db_writes += int(count or 0)

    fields_filled = [*paper_fields_changed, *sidecars_filled]
    return {
        "paper_id": paper_id,
        "exists": True,
        "changed": bool(fields_filled),
        "fields_filled": fields_filled,
        "paper_fields_changed": paper_fields_changed,
        "sidecars_filled": sidecars_filled,
        "db_writes": db_writes,
    }

