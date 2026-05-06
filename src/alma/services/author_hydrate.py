"""Author metadata hydration jobs.

Mirrors the corpus paper rehydration ledger for author profile and
affiliation metadata. Fetching is explicit and Activity-backed; read
endpoints only inspect the ledger/evidence tables.
"""

from __future__ import annotations

import hashlib
import json
import logging
import sqlite3
import uuid
from collections import Counter
from datetime import datetime, timedelta
from typing import Any, Callable, Literal

from alma.application.author_affiliation import (
    ensure_author_affiliation_evidence_table,
    recompute_display_affiliation,
    score_affiliation_candidates,
)
from alma.application.author_profile import apply_author_profile_update
from alma.core.utils import normalize_orcid
from alma.discovery.orcid import fetch_record_by_orcid
from alma.openalex.client import (
    _AUTHORS_SELECT_FIELDS,
    _normalize_openalex_author_id,
    batch_get_author_details,
)

logger = logging.getLogger(__name__)

OPENALEX_SOURCE = "openalex"
ORCID_SOURCE = "orcid"
S2_SOURCE = "semantic_scholar"
CROSSREF_SOURCE = "crossref"
PROFILE_PURPOSE = "profile"
AFFILIATION_PURPOSE = "affiliation"
ALIASES_PURPOSE = "aliases"
PENDING_STATUS = "pending"
RETRYABLE_STATUS = "retryable_error"
TERMINAL_NO_MATCH_STATUS = "terminal_no_match"
TERMINAL_STATUSES = {"enriched", "unchanged", TERMINAL_NO_MATCH_STATUS}
UNCHANGED_RETRY_AFTER = timedelta(days=30)
OPENALEX_AUTHOR_FIELDS_KEY = (
    "openalex_authors:"
    + hashlib.sha1(_AUTHORS_SELECT_FIELDS.encode("utf-8")).hexdigest()[:12]
)
ORCID_FIELDS_KEY = "orcid_record_v1"
S2_AUTHOR_FIELDS_KEY = "s2_author_v1"
CROSSREF_AUTHOR_FIELDS_KEY = "crossref_author_orcid_v1"

SOURCE_PURPOSES: dict[str, tuple[str, ...]] = {
    OPENALEX_SOURCE: (PROFILE_PURPOSE, AFFILIATION_PURPOSE, ALIASES_PURPOSE),
    ORCID_SOURCE: (PROFILE_PURPOSE, AFFILIATION_PURPOSE),
    S2_SOURCE: (PROFILE_PURPOSE, ALIASES_PURPOSE),
    CROSSREF_SOURCE: (AFFILIATION_PURPOSE,),
}


def _utcnow() -> datetime:
    return datetime.utcnow()


def _utcnow_iso() -> str:
    return _utcnow().isoformat()


def _json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, default=str)


def _row_value(row: sqlite3.Row, key: str) -> Any:
    try:
        return row[key]
    except Exception:
        return None


def _ensure_author_enrichment_status_table(conn: sqlite3.Connection) -> None:
    conn.execute(
        """CREATE TABLE IF NOT EXISTS author_enrichment_status (
            author_id TEXT NOT NULL REFERENCES authors(id) ON DELETE CASCADE,
            source TEXT NOT NULL,
            purpose TEXT NOT NULL,
            lookup_key TEXT NOT NULL DEFAULT '',
            fields_key TEXT NOT NULL DEFAULT '',
            status TEXT NOT NULL,
            reason TEXT,
            fields_requested_json TEXT,
            fields_filled_json TEXT,
            attempts INTEGER NOT NULL DEFAULT 0,
            last_attempt_at TEXT,
            next_retry_at TEXT,
            updated_at TEXT NOT NULL,
            PRIMARY KEY (author_id, source, purpose)
        )"""
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_author_enrichment_status_lookup "
        "ON author_enrichment_status(source, purpose, lookup_key, fields_key, status)"
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_author_enrichment_status_retry "
        "ON author_enrichment_status(source, purpose, status, next_retry_at)"
    )


def ensure_author_hydration_tables(conn: sqlite3.Connection) -> None:
    _ensure_author_enrichment_status_table(conn)
    ensure_author_affiliation_evidence_table(conn)


def _fields_key(source: str) -> str:
    if source == OPENALEX_SOURCE:
        return OPENALEX_AUTHOR_FIELDS_KEY
    if source == ORCID_SOURCE:
        return ORCID_FIELDS_KEY
    if source == S2_SOURCE:
        return S2_AUTHOR_FIELDS_KEY
    if source == CROSSREF_SOURCE:
        return CROSSREF_AUTHOR_FIELDS_KEY
    return f"{source}_v1"


def _lookup_key(source: str, row: sqlite3.Row) -> str:
    if source == OPENALEX_SOURCE:
        oid = _normalize_openalex_author_id(str(_row_value(row, "openalex_id") or ""))
        return f"openalex:{oid.lower()}" if oid else ""
    if source in {ORCID_SOURCE, CROSSREF_SOURCE}:
        orcid = normalize_orcid(str(_row_value(row, "orcid") or ""))
        return f"orcid:{orcid.lower()}" if orcid else ""
    if source == S2_SOURCE:
        sid = str(_row_value(row, "semantic_scholar_id") or "").strip()
        return f"s2:{sid}" if sid else ""
    return ""


def _source_available(source: str, row: sqlite3.Row) -> bool:
    return bool(_lookup_key(source, row))


def _upsert_enrichment_status(
    conn: sqlite3.Connection,
    *,
    author_id: str,
    source: str,
    purpose: str,
    lookup_key: str,
    fields_key: str,
    status: str,
    reason: str = "",
    fields_requested: list[str] | None = None,
    fields_filled: list[str] | None = None,
) -> None:
    now = _utcnow_iso()
    next_retry_at = None
    if status in {RETRYABLE_STATUS, "unchanged"}:
        next_retry_at = (_utcnow() + UNCHANGED_RETRY_AFTER).isoformat()
    conn.execute(
        """
        INSERT INTO author_enrichment_status (
            author_id, source, purpose, lookup_key, fields_key, status, reason,
            fields_requested_json, fields_filled_json, attempts,
            last_attempt_at, next_retry_at, updated_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 1, ?, ?, ?)
        ON CONFLICT(author_id, source, purpose) DO UPDATE SET
            lookup_key = excluded.lookup_key,
            fields_key = excluded.fields_key,
            status = excluded.status,
            reason = excluded.reason,
            fields_requested_json = excluded.fields_requested_json,
            fields_filled_json = excluded.fields_filled_json,
            attempts = CASE
                WHEN author_enrichment_status.lookup_key = excluded.lookup_key
                 AND author_enrichment_status.fields_key = excluded.fields_key
                THEN author_enrichment_status.attempts + 1
                ELSE 1
            END,
            last_attempt_at = excluded.last_attempt_at,
            next_retry_at = excluded.next_retry_at,
            updated_at = excluded.updated_at
        """,
        (
            author_id,
            source,
            purpose,
            lookup_key,
            fields_key,
            status,
            reason[:1000],
            _json(fields_requested or []),
            _json(fields_filled or []),
            now,
            next_retry_at,
            now,
        ),
    )


def _insert_affiliation_evidence(
    conn: sqlite3.Connection,
    *,
    author_id: str,
    source: str,
    institution_name: str,
    role: str,
    institution_openalex_id: str | None = None,
    institution_ror: str | None = None,
    start_date: str | None = None,
    end_date: str | None = None,
    is_current: bool = False,
    evidence_url: str | None = None,
    confidence: float | None = None,
) -> bool:
    name = str(institution_name or "").strip()
    if not name:
        return False
    start_key = str(start_date or "").strip()
    cur = conn.execute(
        """
        INSERT INTO author_affiliation_evidence (
            author_id, source, institution_openalex_id, institution_ror,
            institution_name, role, start_date, end_date, is_current,
            evidence_url, confidence, observed_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(author_id, source, institution_name, role, start_date)
        DO UPDATE SET
            institution_openalex_id = COALESCE(excluded.institution_openalex_id, author_affiliation_evidence.institution_openalex_id),
            institution_ror = COALESCE(excluded.institution_ror, author_affiliation_evidence.institution_ror),
            end_date = COALESCE(excluded.end_date, author_affiliation_evidence.end_date),
            is_current = MAX(COALESCE(author_affiliation_evidence.is_current, 0), COALESCE(excluded.is_current, 0)),
            evidence_url = COALESCE(excluded.evidence_url, author_affiliation_evidence.evidence_url),
            confidence = COALESCE(excluded.confidence, author_affiliation_evidence.confidence),
            observed_at = excluded.observed_at
        """,
        (
            author_id,
            source,
            institution_openalex_id,
            institution_ror,
            name,
            role,
            start_key,
            end_date,
            1 if is_current else 0,
            evidence_url,
            confidence,
            _utcnow_iso(),
        ),
    )
    return (cur.rowcount or 0) > 0


def _clear_affiliation_evidence(conn: sqlite3.Connection, *, author_id: str, source: str) -> None:
    conn.execute(
        "DELETE FROM author_affiliation_evidence WHERE author_id = ? AND source = ?",
        (author_id, source),
    )


def _author_row(conn: sqlite3.Connection, author_id: str) -> sqlite3.Row | None:
    return conn.execute("SELECT * FROM authors WHERE id = ?", (author_id,)).fetchone()


def _profile_from_openalex_detail(detail: dict[str, Any]) -> dict[str, Any]:
    topics = detail.get("topics") if isinstance(detail.get("topics"), list) else []
    return {
        "display_name": detail.get("display_name"),
        "affiliation": detail.get("institution"),
        "orcid": detail.get("orcid"),
        "citedby": detail.get("cited_by_count"),
        "h_index": detail.get("h_index"),
        "works_count": detail.get("works_count"),
        "interests": [t.get("term") for t in topics if isinstance(t, dict) and t.get("term")],
        "institutions": detail.get("affiliations") if isinstance(detail.get("affiliations"), list) else [],
    }


def _hydrate_openalex(conn: sqlite3.Connection, row: sqlite3.Row, purposes: tuple[str, ...]) -> dict[str, Any]:
    author_id = str(row["id"])
    lookup_key = _lookup_key(OPENALEX_SOURCE, row)
    fields_key = _fields_key(OPENALEX_SOURCE)
    oid = _normalize_openalex_author_id(str(row["openalex_id"] or ""))
    if not oid:
        for purpose in purposes:
            _upsert_enrichment_status(
                conn,
                author_id=author_id,
                source=OPENALEX_SOURCE,
                purpose=purpose,
                lookup_key="",
                fields_key=fields_key,
                status=TERMINAL_NO_MATCH_STATUS,
                reason="missing_openalex_id",
            )
        return {"source": OPENALEX_SOURCE, "status": TERMINAL_NO_MATCH_STATUS, "filled": []}

    detail = batch_get_author_details([oid], batch_size=1, max_workers=1).get(oid)
    if not detail:
        for purpose in purposes:
            _upsert_enrichment_status(
                conn,
                author_id=author_id,
                source=OPENALEX_SOURCE,
                purpose=purpose,
                lookup_key=lookup_key,
                fields_key=fields_key,
                status=TERMINAL_NO_MATCH_STATUS,
                reason="openalex_author_not_found",
            )
        return {"source": OPENALEX_SOURCE, "status": TERMINAL_NO_MATCH_STATUS, "filled": []}

    filled_by_purpose: dict[str, list[str]] = {}
    if PROFILE_PURPOSE in purposes:
        result = apply_author_profile_update(conn, author_id, _profile_from_openalex_detail(detail))
        filled_by_purpose[PROFILE_PURPOSE] = list(result.get("updated") or [])
        _upsert_enrichment_status(
            conn,
            author_id=author_id,
            source=OPENALEX_SOURCE,
            purpose=PROFILE_PURPOSE,
            lookup_key=lookup_key,
            fields_key=fields_key,
            status="enriched" if filled_by_purpose[PROFILE_PURPOSE] else "unchanged",
            fields_requested=["display_name", "orcid", "works_count", "cited_by_count", "summary_stats", "topics"],
            fields_filled=filled_by_purpose[PROFILE_PURPOSE],
        )

    if AFFILIATION_PURPOSE in purposes:
        evidence_count = 0
        _clear_affiliation_evidence(conn, author_id=author_id, source=OPENALEX_SOURCE)
        institution = str(detail.get("institution") or "").strip()
        if institution:
            if _insert_affiliation_evidence(
                conn,
                author_id=author_id,
                source=OPENALEX_SOURCE,
                institution_name=institution,
                role="last_known_institution",
                is_current=True,
                confidence=0.86,
                evidence_url=f"https://openalex.org/{oid}",
            ):
                evidence_count += 1
        for aff in detail.get("affiliations") or []:
            if not isinstance(aff, dict):
                continue
            years = aff.get("years") if isinstance(aff.get("years"), list) else []
            latest = max([int(y) for y in years if str(y).isdigit()], default=None)
            if _insert_affiliation_evidence(
                conn,
                author_id=author_id,
                source=OPENALEX_SOURCE,
                institution_name=str(aff.get("name") or ""),
                role="affiliation",
                start_date=str(latest) if latest else None,
                is_current=bool(latest and latest >= datetime.utcnow().year - 2),
                confidence=0.78,
                evidence_url=f"https://openalex.org/{oid}",
            ):
                evidence_count += 1
        filled = ["affiliation_evidence"] if evidence_count else []
        filled_by_purpose[AFFILIATION_PURPOSE] = filled
        _upsert_enrichment_status(
            conn,
            author_id=author_id,
            source=OPENALEX_SOURCE,
            purpose=AFFILIATION_PURPOSE,
            lookup_key=lookup_key,
            fields_key=fields_key,
            status="enriched" if evidence_count else "unchanged",
            fields_requested=["last_known_institutions", "affiliations"],
            fields_filled=filled,
        )

    if ALIASES_PURPOSE in purposes:
        alias_count = 0
        try:
            from alma.application.author_merge import record_orcid_aliases

            alias_result = record_orcid_aliases(conn, author_id)
            alias_count = int(alias_result.get("aliases_recorded") or alias_result.get("recorded") or 0)
        except Exception as exc:
            logger.debug("OpenAlex ORCID alias hydration skipped for %s: %s", author_id, exc)
        filled = ["orcid_aliases"] if alias_count else []
        filled_by_purpose[ALIASES_PURPOSE] = filled
        _upsert_enrichment_status(
            conn,
            author_id=author_id,
            source=OPENALEX_SOURCE,
            purpose=ALIASES_PURPOSE,
            lookup_key=lookup_key,
            fields_key=fields_key,
            status="enriched" if alias_count else "unchanged",
            fields_requested=["orcid_aliases"],
            fields_filled=filled,
        )

    return {"source": OPENALEX_SOURCE, "status": "ok", "filled": filled_by_purpose}


def _hydrate_s2(conn: sqlite3.Connection, row: sqlite3.Row, purposes: tuple[str, ...]) -> dict[str, Any]:
    from alma.discovery.semantic_scholar import fetch_authors_batch

    author_id = str(row["id"])
    lookup_key = _lookup_key(S2_SOURCE, row)
    fields_key = _fields_key(S2_SOURCE)
    sid = str(row["semantic_scholar_id"] or "").strip()
    if not sid:
        for purpose in purposes:
            _upsert_enrichment_status(
                conn,
                author_id=author_id,
                source=S2_SOURCE,
                purpose=purpose,
                lookup_key="",
                fields_key=fields_key,
                status=TERMINAL_NO_MATCH_STATUS,
                reason="missing_semantic_scholar_id",
            )
        return {"source": S2_SOURCE, "status": TERMINAL_NO_MATCH_STATUS, "filled": []}

    data = fetch_authors_batch([sid], batch_size=1).get(sid)
    if not data:
        for purpose in purposes:
            _upsert_enrichment_status(
                conn,
                author_id=author_id,
                source=S2_SOURCE,
                purpose=purpose,
                lookup_key=lookup_key,
                fields_key=fields_key,
                status=TERMINAL_NO_MATCH_STATUS,
                reason="semantic_scholar_author_not_found",
            )
        return {"source": S2_SOURCE, "status": TERMINAL_NO_MATCH_STATUS, "filled": []}

    filled_by_purpose: dict[str, list[str]] = {}
    if PROFILE_PURPOSE in purposes:
        profile = {
            "display_name": data.get("name"),
            "citedby": data.get("citationCount"),
            "h_index": data.get("hIndex"),
            "works_count": data.get("paperCount"),
        }
        result = apply_author_profile_update(conn, author_id, profile)
        filled = list(result.get("updated") or [])
        filled_by_purpose[PROFILE_PURPOSE] = filled
        _upsert_enrichment_status(
            conn,
            author_id=author_id,
            source=S2_SOURCE,
            purpose=PROFILE_PURPOSE,
            lookup_key=lookup_key,
            fields_key=fields_key,
            status="enriched" if filled else "unchanged",
            fields_requested=["name", "citationCount", "hIndex", "paperCount"],
            fields_filled=filled,
        )
    if ALIASES_PURPOSE in purposes:
        aliases = [str(a).strip() for a in (data.get("aliases") or []) if str(a).strip()]
        _upsert_enrichment_status(
            conn,
            author_id=author_id,
            source=S2_SOURCE,
            purpose=ALIASES_PURPOSE,
            lookup_key=lookup_key,
            fields_key=fields_key,
            status="enriched" if aliases else "unchanged",
            fields_requested=["aliases"],
            fields_filled=["aliases"] if aliases else [],
        )
        filled_by_purpose[ALIASES_PURPOSE] = ["aliases"] if aliases else []
    return {"source": S2_SOURCE, "status": "ok", "filled": filled_by_purpose}


def _hydrate_orcid(conn: sqlite3.Connection, row: sqlite3.Row, purposes: tuple[str, ...]) -> dict[str, Any]:
    author_id = str(row["id"])
    lookup_key = _lookup_key(ORCID_SOURCE, row)
    fields_key = _fields_key(ORCID_SOURCE)
    orcid = normalize_orcid(str(row["orcid"] or ""))
    if not orcid:
        for purpose in purposes:
            _upsert_enrichment_status(
                conn,
                author_id=author_id,
                source=ORCID_SOURCE,
                purpose=purpose,
                lookup_key="",
                fields_key=fields_key,
                status=TERMINAL_NO_MATCH_STATUS,
                reason="missing_orcid",
            )
        return {"source": ORCID_SOURCE, "status": TERMINAL_NO_MATCH_STATUS, "filled": []}

    record = fetch_record_by_orcid(orcid)
    if not record:
        for purpose in purposes:
            _upsert_enrichment_status(
                conn,
                author_id=author_id,
                source=ORCID_SOURCE,
                purpose=purpose,
                lookup_key=lookup_key,
                fields_key=fields_key,
                status=TERMINAL_NO_MATCH_STATUS,
                reason="orcid_record_not_found",
            )
        return {"source": ORCID_SOURCE, "status": TERMINAL_NO_MATCH_STATUS, "filled": []}

    filled_by_purpose: dict[str, list[str]] = {}
    if PROFILE_PURPOSE in purposes:
        filled = ["other_names"] if record.other_names else []
        _upsert_enrichment_status(
            conn,
            author_id=author_id,
            source=ORCID_SOURCE,
            purpose=PROFILE_PURPOSE,
            lookup_key=lookup_key,
            fields_key=fields_key,
            status="enriched" if filled else "unchanged",
            fields_requested=["person.name", "person.other_names", "person.addresses"],
            fields_filled=filled,
        )
        filled_by_purpose[PROFILE_PURPOSE] = filled

    if AFFILIATION_PURPOSE in purposes:
        evidence_count = 0
        _clear_affiliation_evidence(conn, author_id=author_id, source=ORCID_SOURCE)
        for affiliation in record.affiliations:
            if _insert_affiliation_evidence(
                conn,
                author_id=author_id,
                source=ORCID_SOURCE,
                institution_name=affiliation.institution_name,
                institution_ror=affiliation.institution_ror,
                role=affiliation.role,
                start_date=affiliation.start_date,
                end_date=affiliation.end_date,
                is_current=affiliation.is_current,
                evidence_url=affiliation.evidence_url or f"https://orcid.org/{orcid}",
                confidence=1.0 if affiliation.role == "employment" else 0.72,
            ):
                evidence_count += 1
        filled = ["affiliation_evidence"] if evidence_count else []
        _upsert_enrichment_status(
            conn,
            author_id=author_id,
            source=ORCID_SOURCE,
            purpose=AFFILIATION_PURPOSE,
            lookup_key=lookup_key,
            fields_key=fields_key,
            status="enriched" if evidence_count else "unchanged",
            fields_requested=["activities-summary.employments", "activities-summary.educations"],
            fields_filled=filled,
        )
        filled_by_purpose[AFFILIATION_PURPOSE] = filled
    return {"source": ORCID_SOURCE, "status": "ok", "filled": filled_by_purpose}


def _fetch_crossref_affiliations(orcid: str) -> list[str] | None:
    from alma.core.http_sources import get_source_http_client

    normalized = normalize_orcid(orcid or "")
    if not normalized:
        return []
    try:
        resp = get_source_http_client(CROSSREF_SOURCE).get(
            "/works",
            params={
                "filter": f"orcid:{normalized}",
                "rows": 20,
                "select": "DOI,title,author,issued",
                "sort": "published",
                "order": "desc",
            },
            timeout=25,
        )
    except Exception as exc:
        logger.debug("Crossref author-affiliation fetch failed for %s: %s", normalized, exc)
        return None
    if resp.status_code != 200:
        logger.debug("Crossref author-affiliation fetch returned HTTP %d", resp.status_code)
        return None
    try:
        items = (((resp.json() or {}).get("message") or {}).get("items")) or []
    except Exception:
        return None
    out: list[str] = []
    for item in items:
        for author in (item or {}).get("author") or []:
            if not isinstance(author, dict):
                continue
            raw_orcid = normalize_orcid(str(author.get("ORCID") or author.get("orcid") or ""))
            if raw_orcid and raw_orcid != normalized:
                continue
            for aff in author.get("affiliation") or []:
                if not isinstance(aff, dict):
                    continue
                name = str(aff.get("name") or "").strip()
                if name:
                    out.append(name)
    return list(dict.fromkeys(out))


def _hydrate_crossref(conn: sqlite3.Connection, row: sqlite3.Row, purposes: tuple[str, ...]) -> dict[str, Any]:
    author_id = str(row["id"])
    lookup_key = _lookup_key(CROSSREF_SOURCE, row)
    fields_key = _fields_key(CROSSREF_SOURCE)
    orcid = normalize_orcid(str(row["orcid"] or ""))
    if not orcid:
        _upsert_enrichment_status(
            conn,
            author_id=author_id,
            source=CROSSREF_SOURCE,
            purpose=AFFILIATION_PURPOSE,
            lookup_key="",
            fields_key=fields_key,
            status=TERMINAL_NO_MATCH_STATUS,
            reason="missing_orcid",
        )
        return {"source": CROSSREF_SOURCE, "status": TERMINAL_NO_MATCH_STATUS, "filled": []}

    names = _fetch_crossref_affiliations(orcid)
    if names is None:
        _upsert_enrichment_status(
            conn,
            author_id=author_id,
            source=CROSSREF_SOURCE,
            purpose=AFFILIATION_PURPOSE,
            lookup_key=lookup_key,
            fields_key=fields_key,
            status=RETRYABLE_STATUS,
            reason="crossref_fetch_failed",
            fields_requested=["works.author.affiliation"],
            fields_filled=[],
        )
        return {"source": CROSSREF_SOURCE, "status": RETRYABLE_STATUS, "filled": {}}

    evidence_count = 0
    _clear_affiliation_evidence(conn, author_id=author_id, source=CROSSREF_SOURCE)
    for name in names:
        if _insert_affiliation_evidence(
            conn,
            author_id=author_id,
            source=CROSSREF_SOURCE,
            institution_name=name,
            role="recent_authorship",
            is_current=False,
            evidence_url=f"https://orcid.org/{orcid}",
            confidence=0.54,
        ):
            evidence_count += 1
    filled = ["affiliation_evidence"] if evidence_count else []
    _upsert_enrichment_status(
        conn,
        author_id=author_id,
        source=CROSSREF_SOURCE,
        purpose=AFFILIATION_PURPOSE,
        lookup_key=lookup_key,
        fields_key=fields_key,
        status="enriched" if evidence_count else "unchanged",
        fields_requested=["works.author.affiliation"],
        fields_filled=filled,
    )
    return {"source": CROSSREF_SOURCE, "status": "ok", "filled": {AFFILIATION_PURPOSE: filled}}


def hydrate_author_metadata(
    conn: sqlite3.Connection,
    author_id: str,
    *,
    sources: tuple[str, ...] = (OPENALEX_SOURCE, ORCID_SOURCE, S2_SOURCE, CROSSREF_SOURCE),
    purposes: tuple[str, ...] = (PROFILE_PURPOSE, AFFILIATION_PURPOSE, ALIASES_PURPOSE),
) -> dict[str, Any]:
    """Best-effort metadata hydration for one author."""
    ensure_author_hydration_tables(conn)
    author_key = str(author_id or "").strip()
    row = _author_row(conn, author_key)
    if row is None:
        return {"author_id": author_key, "success": False, "message": "author_not_found"}

    results: dict[str, Any] = {}
    evidence_touched = False
    for source in sources:
        allowed_purposes = tuple(p for p in purposes if p in SOURCE_PURPOSES.get(source, ()))
        if not allowed_purposes:
            continue
        try:
            if source == OPENALEX_SOURCE:
                result = _hydrate_openalex(conn, row, allowed_purposes)
            elif source == S2_SOURCE:
                result = _hydrate_s2(conn, row, allowed_purposes)
            elif source == ORCID_SOURCE:
                result = _hydrate_orcid(conn, row, allowed_purposes)
            elif source == CROSSREF_SOURCE:
                result = _hydrate_crossref(conn, row, allowed_purposes)
            else:
                continue
            results[source] = result
            filled = result.get("filled") if isinstance(result, dict) else {}
            if isinstance(filled, dict) and AFFILIATION_PURPOSE in filled and filled[AFFILIATION_PURPOSE]:
                evidence_touched = True
        except Exception as exc:
            logger.warning("Author hydration failed for %s via %s: %s", author_key, source, exc)
            lookup_key = _lookup_key(source, row)
            for purpose in allowed_purposes:
                _upsert_enrichment_status(
                    conn,
                    author_id=author_key,
                    source=source,
                    purpose=purpose,
                    lookup_key=lookup_key,
                    fields_key=_fields_key(source),
                    status=RETRYABLE_STATUS,
                    reason=str(exc),
                )
            results[source] = {"source": source, "status": RETRYABLE_STATUS, "error": str(exc)}

    decision = recompute_display_affiliation(conn, author_key) if evidence_touched else None
    conn.commit()
    return {
        "author_id": author_key,
        "success": True,
        "sources": results,
        "display_affiliation": decision.selected_affiliation if decision else None,
        "affiliation_changed": bool(decision.changed) if decision else False,
        "affiliation_conflict": bool(decision.conflict) if decision else False,
    }


def enqueue_pending_author_hydration(
    conn: sqlite3.Connection,
    author_id: str,
    *,
    priority: Literal["high", "low"],
    reason: str = "author_seen",
) -> bool:
    """Mark an author as needing hydration.

    High-priority callers reset terminal rows and schedule a sweep.
    Low-priority callers only write ledger rows; they ride the next
    manual/follow-triggered sweep.
    """
    if priority not in {"high", "low"}:
        raise ValueError("priority must be 'high' or 'low'")
    ensure_author_hydration_tables(conn)
    row = _author_row(conn, author_id)
    if row is None:
        return False
    now = _utcnow_iso()
    queued = False
    for source, source_purposes in SOURCE_PURPOSES.items():
        if not _source_available(source, row):
            continue
        for purpose in source_purposes:
            existing = conn.execute(
                """
                SELECT status FROM author_enrichment_status
                WHERE author_id = ? AND source = ? AND purpose = ?
                """,
                (author_id, source, purpose),
            ).fetchone()
            existing_status = str(existing["status"] if existing else "" or "")
            if priority == "low" and existing_status in TERMINAL_STATUSES:
                continue
            conn.execute(
                """
                INSERT INTO author_enrichment_status (
                    author_id, source, purpose, lookup_key, fields_key, status,
                    fields_requested_json, fields_filled_json, attempts,
                    last_attempt_at, next_retry_at, updated_at
                )
                VALUES (?, ?, ?, ?, ?, ?, '[]', '[]', 0, NULL, NULL, ?)
                ON CONFLICT(author_id, source, purpose) DO UPDATE SET
                    lookup_key = excluded.lookup_key,
                    fields_key = excluded.fields_key,
                    status = CASE
                        WHEN ? = 'low'
                         AND author_enrichment_status.status IN ('enriched', 'unchanged', 'terminal_no_match')
                        THEN author_enrichment_status.status
                        ELSE excluded.status
                    END,
                    next_retry_at = CASE WHEN ? = 'high' THEN NULL ELSE author_enrichment_status.next_retry_at END,
                    updated_at = excluded.updated_at
                """,
                (
                    author_id,
                    source,
                    purpose,
                    _lookup_key(source, row),
                    _fields_key(source),
                    PENDING_STATUS,
                    now,
                    priority,
                    priority,
                ),
            )
            queued = True
    if queued and priority == "high":
        try:
            schedule_pending_author_hydration_sweep(reason=reason)
        except Exception as exc:
            logger.debug("auto author hydration schedule skipped: %s", exc)
    return queued


def _enqueue_candidates_for_run(
    conn: sqlite3.Connection,
    *,
    limit: int | None,
    force: bool,
) -> int:
    ensure_author_hydration_tables(conn)
    params: list[Any] = []
    limit_clause = ""
    if limit is not None:
        params.append(max(1, int(limit)))
        limit_clause = "LIMIT ?"
    rows = conn.execute(
        f"""
        SELECT *
        FROM authors
        WHERE COALESCE(status, 'active') != 'removed'
          AND (
            COALESCE(NULLIF(TRIM(openalex_id), ''), '') != ''
            OR COALESCE(NULLIF(TRIM(orcid), ''), '') != ''
            OR COALESCE(NULLIF(TRIM(semantic_scholar_id), ''), '') != ''
          )
        ORDER BY
            CASE WHEN author_type = 'followed' THEN 0 ELSE 1 END,
            COALESCE(last_fetched_at, added_at, '') DESC,
            name ASC
        {limit_clause}
        """,
        params,
    ).fetchall()
    count = 0
    for row in rows:
        if enqueue_pending_author_hydration(
            conn,
            str(row["id"]),
            priority="high" if force else "low",
            reason="manual_rehydrate_prepare",
        ):
            count += 1
    conn.commit()
    return count


def _select_source_candidates(
    conn: sqlite3.Connection,
    *,
    source: str,
    limit: int | None,
    force: bool,
) -> list[sqlite3.Row]:
    ensure_author_hydration_tables(conn)
    now = _utcnow_iso()
    params: list[Any] = [source]
    force_clause = "1 = 1" if force else """
    (
        es.status IN ('pending', 'queued')
        OR es.status = ?
        OR (
            a.author_type = 'followed'
            AND es.status = 'unchanged'
            AND (es.next_retry_at IS NULL OR es.next_retry_at <= ?)
        )
        OR (
            a.author_type != 'followed'
            AND es.status = 'pending'
        )
    )
    """
    if not force:
        params.extend([RETRYABLE_STATUS, now])
    limit_clause = ""
    if limit is not None:
        params.append(max(1, int(limit)))
        limit_clause = "LIMIT ?"
    return conn.execute(
        f"""
        SELECT DISTINCT a.*
        FROM author_enrichment_status es
        JOIN authors a ON a.id = es.author_id
        WHERE es.source = ?
          AND COALESCE(a.status, 'active') != 'removed'
          AND {force_clause}
        ORDER BY
            CASE WHEN a.author_type = 'followed' THEN 0 ELSE 1 END,
            COALESCE(es.updated_at, '') ASC,
            a.name ASC
        {limit_clause}
        """,
        params,
    ).fetchall()


def run_author_metadata_rehydration(
    job_id: str | None = None,
    *,
    limit: int | None = 500,
    force: bool = False,
    set_job_status: Callable[..., Any] | None = None,
    add_job_log: Callable[..., Any] | None = None,
    is_cancellation_requested: Callable[[str], bool] | None = None,
) -> dict[str, Any]:
    """Activity runner for author profile/affiliation hydration."""
    from alma.api.deps import open_db_connection

    conn = open_db_connection()
    close_conn = True
    jid = job_id or f"author_metadata_rehydrate_{uuid.uuid4().hex[:10]}"
    summary: Counter[str] = Counter()
    try:
        ensure_author_hydration_tables(conn)
        bounded_limit = None if limit is None else max(1, min(int(limit), 100_000))
        enqueued = _enqueue_candidates_for_run(conn, limit=bounded_limit, force=force)
        if add_job_log:
            add_job_log(jid, "Prepared author hydration ledger", step="prepare", data={"enqueued": enqueued, "limit": bounded_limit})

        phases = [
            ("openalex", OPENALEX_SOURCE),
            ("semantic_scholar", S2_SOURCE),
            ("orcid", ORCID_SOURCE),
            ("crossref", CROSSREF_SOURCE),
        ]
        total_candidates = 0
        source_rows: dict[str, list[sqlite3.Row]] = {}
        for _, source in phases:
            rows = _select_source_candidates(conn, source=source, limit=bounded_limit, force=force)
            source_rows[source] = rows
            total_candidates += len(rows)
        processed = 0
        if set_job_status:
            set_job_status(
                jid,
                status="running",
                processed=0,
                total=total_candidates,
                message=f"Hydrating metadata for {total_candidates} author-source candidate(s)",
            )

        for phase_name, source in phases:
            rows = source_rows[source]
            if add_job_log:
                add_job_log(jid, f"{phase_name} phase selected {len(rows)} author(s)", step=f"{phase_name}_phase_prepare")
            for row in rows:
                if is_cancellation_requested and is_cancellation_requested(jid):
                    if set_job_status:
                        set_job_status(jid, status="cancelling", message="Cancellation requested")
                    try:
                        from alma.api.scheduler import JobCancelled

                        raise JobCancelled()
                    except ImportError:
                        raise RuntimeError("Operation cancelled")
                author_id = str(row["id"])
                result = hydrate_author_metadata(conn, author_id, sources=(source,))
                processed += 1
                summary[f"{source}.processed"] += 1
                if result.get("affiliation_changed"):
                    summary["display.changed"] += 1
                if result.get("affiliation_conflict"):
                    summary["display.conflicts"] += 1
                source_payload = result.get("sources", {}).get(source, {})
                filled = source_payload.get("filled") if isinstance(source_payload, dict) else {}
                if isinstance(filled, dict):
                    for purpose, values in filled.items():
                        if values:
                            summary[f"{source}.{purpose}.enriched"] += 1
                        else:
                            summary[f"{source}.{purpose}.unchanged"] += 1
                if set_job_status:
                    set_job_status(
                        jid,
                        status="running",
                        processed=processed,
                        total=total_candidates,
                        current_author=str(row["name"] or author_id),
                        message=f"Hydrated {processed}/{total_candidates} author-source candidate(s)",
                    )
            if add_job_log:
                add_job_log(jid, f"{phase_name} phase completed", step=f"{phase_name}_phase_done", data=dict(summary))

        msg = (
            "Author metadata rehydration complete: "
            f"OpenAlex={summary.get('openalex.processed', 0)}, "
            f"S2={summary.get('semantic_scholar.processed', 0)}, "
            f"ORCID={summary.get('orcid.processed', 0)}, "
            f"Crossref={summary.get('crossref.processed', 0)}, "
            f"display_changed={summary.get('display.changed', 0)}, "
            f"conflicts={summary.get('display.conflicts', 0)}"
        )
        if add_job_log:
            add_job_log(jid, msg, step="done", data=dict(summary))
        return {"success": True, "message": msg, "summary": dict(summary), "processed": processed, "total": total_candidates}
    finally:
        if close_conn:
            conn.close()


def schedule_pending_author_hydration_sweep(
    *,
    reason: str = "author_follow",
    limit: int = 500,
) -> str | None:
    try:
        from alma.api.scheduler import (
            add_job_log,
            find_active_job,
            is_cancellation_requested,
            schedule_immediate,
            set_job_status,
        )
    except Exception:
        return None

    operation_key = "authors.rehydrate_metadata"
    existing = find_active_job(operation_key)
    if existing:
        return str(existing.get("job_id") or "") or None

    job_id = f"author_metadata_rehydrate_{uuid.uuid4().hex[:10]}"
    bounded_limit = max(1, min(int(limit or 500), 100_000))
    set_job_status(
        job_id,
        status="queued",
        operation_key=operation_key,
        trigger_source=f"auto:{reason}",
        started_at=_utcnow_iso(),
        processed=0,
        total=bounded_limit,
        message=f"Author metadata hydration auto-queued for up to {bounded_limit} author(s)",
    )
    add_job_log(job_id, "Auto-queued author metadata hydration", step="queued", data={"reason": reason, "limit": bounded_limit})

    def _runner() -> dict[str, Any]:
        return run_author_metadata_rehydration(
            job_id,
            limit=bounded_limit,
            force=False,
            set_job_status=set_job_status,
            add_job_log=add_job_log,
            is_cancellation_requested=is_cancellation_requested,
        )

    schedule_immediate(job_id, _runner)
    return job_id


def build_author_enrichment_status(conn: sqlite3.Connection) -> dict[str, Any]:
    try:
        rows = conn.execute(
            """
            SELECT source, purpose, status, COUNT(*) AS count
            FROM author_enrichment_status
            GROUP BY source, purpose, status
            ORDER BY source, purpose, status
            """
        ).fetchall()
    except sqlite3.OperationalError:
        rows = []
    return {
        "summary": [dict(row) for row in rows],
        "total": sum(int(row["count"] or 0) for row in rows),
    }


def list_author_enrichment_status_items(
    conn: sqlite3.Connection,
    *,
    limit: int = 100,
) -> list[dict[str, Any]]:
    try:
        rows = conn.execute(
            """
            SELECT
                es.*,
                a.name AS author_name,
                a.openalex_id,
                a.orcid,
                a.semantic_scholar_id
            FROM author_enrichment_status es
            JOIN authors a ON a.id = es.author_id
            ORDER BY es.updated_at DESC
            LIMIT ?
            """,
            (max(1, min(int(limit or 100), 500)),),
        ).fetchall()
    except sqlite3.OperationalError:
        return []
    return [dict(row) for row in rows]


def list_author_affiliations(conn: sqlite3.Connection, author_id: str) -> dict[str, Any]:
    row = _author_row(conn, author_id)
    if row is None:
        return {"author_id": author_id, "found": False, "items": []}
    items = score_affiliation_candidates(conn, author_id)
    return {
        "author_id": author_id,
        "author_name": str(row["name"] or author_id),
        "display_affiliation": str(row["affiliation"] or "") or None,
        "items": items,
    }
