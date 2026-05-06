"""Corpus metadata rehydration jobs.

Repairs local paper metadata from authoritative external IDs while
keeping per-paper bookkeeping so reruns skip already-covered work.
"""

from __future__ import annotations

import hashlib
import json
import logging
import sqlite3
from collections import Counter
from datetime import datetime, timedelta
from typing import Any, Callable

from alma.application.paper_metadata import merge_openalex_work_metadata
from alma.openalex.client import (
    _WORKS_SELECT_FIELDS,
    _normalize_openalex_work_id,
    _normalize_work,
)
from alma.openalex.http import get_client

logger = logging.getLogger(__name__)

OPENALEX_SOURCE = "openalex"
S2_SOURCE = "semantic_scholar"
CROSSREF_SOURCE = "crossref"
METADATA_PURPOSE = "metadata"
PENDING_STATUS = "pending"
OPENALEX_WORKS_FIELDS = [field.strip() for field in _WORKS_SELECT_FIELDS.split(",") if field.strip()]
OPENALEX_WORKS_FIELDS_KEY = (
    "openalex_works:"
    + hashlib.sha1(_WORKS_SELECT_FIELDS.encode("utf-8")).hexdigest()[:12]
)
TERMINAL_STATUSES = {"enriched", "unchanged", "terminal_no_match"}
RETRYABLE_STATUS = "retryable_error"
# How long to wait before re-fetching an OpenAlex work that returned no
# new fields last time. OpenAlex backfills abstracts months after first
# indexing (especially for ARVO / Journal of Vision proceedings and other
# venues that publish abstracts late), so an `unchanged` outcome must
# expire — otherwise local SPECTER2 stays starved of input text forever.
UNCHANGED_RETRY_AFTER = timedelta(days=30)


def _utcnow() -> datetime:
    return datetime.utcnow()


def _utcnow_iso() -> str:
    return _utcnow().isoformat()


def _json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, default=str)


def _openalex_work_id(raw: str) -> str:
    return _normalize_openalex_work_id(str(raw or "").strip()).strip()


def openalex_lookup_key(raw: str) -> str:
    work_id = _openalex_work_id(raw)
    return f"openalex:{work_id.lower()}" if work_id else ""


def _ensure_enrichment_status_table(conn: sqlite3.Connection) -> None:
    conn.execute(
        """CREATE TABLE IF NOT EXISTS paper_enrichment_status (
            paper_id TEXT NOT NULL REFERENCES papers(id) ON DELETE CASCADE,
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
            PRIMARY KEY (paper_id, source, purpose)
        )"""
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_paper_enrichment_status_lookup "
        "ON paper_enrichment_status(source, purpose, lookup_key, fields_key, status)"
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_paper_enrichment_status_retry "
        "ON paper_enrichment_status(source, purpose, status, next_retry_at)"
    )


def _openalex_lookup_expr() -> str:
    return (
        "'openalex:' || lower(trim(CASE "
        "WHEN lower(trim(p.openalex_id)) LIKE 'https://openalex.org/%' THEN substr(trim(p.openalex_id), 22) "
        "WHEN lower(trim(p.openalex_id)) LIKE 'http://openalex.org/%' THEN substr(trim(p.openalex_id), 21) "
        "WHEN lower(trim(p.openalex_id)) LIKE 'openalex.org/%' THEN substr(trim(p.openalex_id), 14) "
        "ELSE trim(p.openalex_id) END))"
    )


def _missing_metadata_clause() -> str:
    return """
    (
        COALESCE(NULLIF(TRIM(p.doi), ''), '') = ''
        OR COALESCE(NULLIF(TRIM(p.abstract), ''), '') = ''
        OR COALESCE(NULLIF(TRIM(p.url), ''), '') = ''
        OR COALESCE(NULLIF(TRIM(p.publication_date), ''), '') = ''
        OR COALESCE(NULLIF(TRIM(p.authors), ''), '') = ''
        OR COALESCE(NULLIF(TRIM(p.journal), ''), '') = ''
        OR NOT EXISTS (SELECT 1 FROM publication_authors pa WHERE pa.paper_id = p.id)
        OR NOT EXISTS (SELECT 1 FROM publication_topics pt WHERE pt.paper_id = p.id)
        OR NOT EXISTS (SELECT 1 FROM publication_references pr WHERE pr.paper_id = p.id)
    )
    """


def _eligible_status_clause(force: bool) -> str:
    if force:
        return "1 = 1"
    lookup_expr = _openalex_lookup_expr()
    # `unchanged` is treated like `retryable_error` for retry timing:
    # the row only returns to the candidate pool once `next_retry_at`
    # has elapsed. A NULL `next_retry_at` on an `unchanged` row is
    # legacy state from before the cooldown was added — also eligible.
    return f"""
    (
        es.paper_id IS NULL
        OR COALESCE(es.lookup_key, '') != {lookup_expr}
        OR COALESCE(es.fields_key, '') != ?
        OR COALESCE(es.status, '') IN ('pending', 'queued')
        OR (
            es.status IN ('{RETRYABLE_STATUS}', 'unchanged')
            AND (es.next_retry_at IS NULL OR es.next_retry_at <= ?)
        )
        OR COALESCE(es.status, '') NOT IN (
            'enriched', 'unchanged', 'terminal_no_match', '{RETRYABLE_STATUS}', 'pending', 'queued'
        )
    )
    """


def _select_openalex_candidates(
    conn: sqlite3.Connection,
    *,
    limit: int | None,
    force: bool = False,
) -> list[sqlite3.Row]:
    _ensure_enrichment_status_table(conn)
    lookup_expr = _openalex_lookup_expr()
    params: list[Any] = [OPENALEX_SOURCE, METADATA_PURPOSE]
    status_clause = _eligible_status_clause(force)
    if not force:
        params.extend([OPENALEX_WORKS_FIELDS_KEY, _utcnow_iso()])
    limit_clause = ""
    if limit is not None:
        params.append(max(1, int(limit)))
        limit_clause = "LIMIT ?"
    return conn.execute(
        f"""
        SELECT
            p.id,
            p.title,
            p.openalex_id,
            {lookup_expr} AS lookup_key,
            es.status AS enrichment_status,
            es.lookup_key AS previous_lookup_key,
            es.fields_key AS previous_fields_key,
            es.next_retry_at
        FROM papers p
        LEFT JOIN paper_enrichment_status es
          ON es.paper_id = p.id
         AND es.source = ?
         AND es.purpose = ?
        WHERE COALESCE(NULLIF(TRIM(p.openalex_id), ''), '') != ''
          AND COALESCE(p.canonical_paper_id, '') = ''
          AND {_missing_metadata_clause()}
          AND {status_clause}
        ORDER BY
            CASE WHEN es.status = '{RETRYABLE_STATUS}' THEN 0 ELSE 1 END,
            COALESCE(p.fetched_at, p.updated_at, p.created_at, '') DESC,
            p.id ASC
        {limit_clause}
        """,
        params,
    ).fetchall()


def _select_s2_candidates(
    conn: sqlite3.Connection,
    *,
    limit: int | None,
) -> list[sqlite3.Row]:
    """Pick papers eligible for the batched Semantic Scholar phase.

    Eligibility: has DOI or `semantic_scholar_id`, isn't a canonical
    duplicate, and the S2 ledger row (if any) is neither `enriched`
    nor `terminal_no_match`. The phase exists because S2 carries fields
    OpenAlex doesn't — `tldr`, `influentialCitationCount` — that the
    Discovery ranker and PaperCard surfaces actively consume.
    """
    _ensure_enrichment_status_table(conn)
    params: list[Any] = [S2_SOURCE, METADATA_PURPOSE]
    limit_clause = ""
    if limit is not None:
        params.append(max(1, int(limit)))
        limit_clause = "LIMIT ?"
    return conn.execute(
        f"""
        SELECT
            p.id,
            p.title,
            p.doi,
            p.semantic_scholar_id,
            p.abstract,
            es.status AS s2_status
        FROM papers p
        LEFT JOIN paper_enrichment_status es
          ON es.paper_id = p.id
         AND es.source = ?
         AND es.purpose = ?
        WHERE COALESCE(p.canonical_paper_id, '') = ''
          AND (
              COALESCE(NULLIF(TRIM(p.doi), ''), '') != ''
              OR COALESCE(NULLIF(TRIM(p.semantic_scholar_id), ''), '') != ''
          )
          AND (
              es.paper_id IS NULL
              OR COALESCE(es.status, '') NOT IN ('enriched', 'terminal_no_match')
          )
        ORDER BY
            CASE WHEN COALESCE(NULLIF(TRIM(p.tldr), ''), '') = '' THEN 0 ELSE 1 END,
            CASE WHEN COALESCE(NULLIF(TRIM(p.abstract), ''), '') = '' THEN 0 ELSE 1 END,
            COALESCE(p.fetched_at, p.updated_at, p.created_at, '') DESC,
            p.id ASC
        {limit_clause}
        """,
        params,
    ).fetchall()


def _candidate_count(conn: sqlite3.Connection) -> int:
    lookup_expr = _openalex_lookup_expr()
    row = conn.execute(
        f"""
        SELECT COUNT(*) AS c
        FROM papers p
        LEFT JOIN paper_enrichment_status es
          ON es.paper_id = p.id
         AND es.source = ?
         AND es.purpose = ?
        WHERE COALESCE(NULLIF(TRIM(p.openalex_id), ''), '') != ''
          AND COALESCE(p.canonical_paper_id, '') = ''
          AND {_missing_metadata_clause()}
          AND (
              es.paper_id IS NULL
              OR COALESCE(es.lookup_key, '') != {lookup_expr}
              OR COALESCE(es.fields_key, '') != ?
              OR COALESCE(es.status, '') IN ('pending', 'queued')
              OR (
                  es.status IN ('{RETRYABLE_STATUS}', 'unchanged')
                  AND (es.next_retry_at IS NULL OR es.next_retry_at <= ?)
              )
              OR COALESCE(es.status, '') NOT IN (
                  'enriched', 'unchanged', 'terminal_no_match', '{RETRYABLE_STATUS}', 'pending', 'queued'
              )
          )
        """,
        (OPENALEX_SOURCE, METADATA_PURPOSE, OPENALEX_WORKS_FIELDS_KEY, _utcnow_iso()),
    ).fetchone()
    return int(row["c"] or 0) if row else 0


def _upsert_enrichment_status(
    conn: sqlite3.Connection,
    *,
    paper_id: str,
    lookup_key: str,
    status: str,
    reason: str = "",
    fields_filled: list[str] | None = None,
    retry_after: timedelta | None = None,
) -> None:
    now = _utcnow()
    next_retry_at = (now + retry_after).isoformat() if retry_after else None
    conn.execute(
        """
        INSERT INTO paper_enrichment_status (
            paper_id, source, purpose, lookup_key, fields_key, status, reason,
            fields_requested_json, fields_filled_json, attempts,
            last_attempt_at, next_retry_at, updated_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 1, ?, ?, ?)
        ON CONFLICT(paper_id, source, purpose) DO UPDATE SET
            lookup_key = excluded.lookup_key,
            fields_key = excluded.fields_key,
            status = excluded.status,
            reason = excluded.reason,
            fields_requested_json = excluded.fields_requested_json,
            fields_filled_json = excluded.fields_filled_json,
            attempts = CASE
                WHEN paper_enrichment_status.lookup_key = excluded.lookup_key
                 AND paper_enrichment_status.fields_key = excluded.fields_key
                THEN paper_enrichment_status.attempts + 1
                ELSE 1
            END,
            last_attempt_at = excluded.last_attempt_at,
            next_retry_at = excluded.next_retry_at,
            updated_at = excluded.updated_at
        """,
        (
            paper_id,
            OPENALEX_SOURCE,
            METADATA_PURPOSE,
            lookup_key,
            OPENALEX_WORKS_FIELDS_KEY,
            status,
            reason,
            _json(OPENALEX_WORKS_FIELDS),
            _json(fields_filled or []),
            now.isoformat(),
            next_retry_at,
            now.isoformat(),
        ),
    )


def _fetch_openalex_chunk(chunk_ids: list[str]) -> tuple[dict[str, dict], dict[str, str], int]:
    """Fetch one OpenAlex-ID chunk.

    Returns `(works_by_id, retryable_errors_by_id, remote_calls)`.
    A whole-chunk network/API failure marks every requested paper
    retryable; a 200 with missing IDs is treated by the caller as a
    terminal no-match.
    """
    clean_ids = []
    seen: set[str] = set()
    for raw in chunk_ids:
        work_id = _openalex_work_id(raw)
        key = work_id.lower()
        if not work_id or key in seen:
            continue
        seen.add(key)
        clean_ids.append(work_id)
    if not clean_ids:
        return {}, {}, 0

    client = get_client()

    def _request(ids: list[str]) -> tuple[dict[str, dict], dict[str, str], int]:
        pipe_filter = "openalex_id:" + "|".join(f"https://openalex.org/{wid}" for wid in ids)
        try:
            resp = client.get(
                "/works",
                params={
                    "filter": pipe_filter,
                    "per-page": len(ids),
                    "select": _WORKS_SELECT_FIELDS,
                },
                timeout=30,
            )
        except Exception as exc:
            return {}, {wid: str(exc) for wid in ids}, 1

        if resp.status_code == 200:
            out: dict[str, dict] = {}
            for work in (resp.json() or {}).get("results") or []:
                work_id = _openalex_work_id(str(work.get("id") or ""))
                if work_id:
                    out[work_id.lower()] = work
            return out, {}, 1
        if resp.status_code in {400, 414} and len(ids) > 1:
            midpoint = max(1, len(ids) // 2)
            left, left_errors, left_calls = _request(ids[:midpoint])
            right, right_errors, right_calls = _request(ids[midpoint:])
            left.update(right)
            left_errors.update(right_errors)
            return left, left_errors, left_calls + right_calls
        return {}, {wid: f"OpenAlex returned HTTP {resp.status_code}" for wid in ids}, 1

    return _request(clean_ids)


def _run_s2_batched_phase(
    conn: sqlite3.Connection,
    *,
    limit: int | None,
    job_id: str,
    set_job_status: Callable[..., None],
    add_job_log: Callable[..., None],
    is_cancellation_requested: Callable[[str], bool],
    base_processed: int,
    base_total: int,
) -> Counter[str]:
    """Phase 1.5 — batched Semantic Scholar pass.

    Runs after the OpenAlex Phase 1 batched loop. For every paper with
    DOI or `semantic_scholar_id` whose S2 ledger row isn't already
    `enriched`/`terminal_no_match`, calls `fetch_papers_batch` in
    chunks of 100 and applies fill-only metadata via `_apply_s2_paper`
    (which also persists `tldr` + `influential_citation_count`).
    Writes an S2 ledger row per paper so reruns skip covered work.
    """
    from alma.discovery import semantic_scholar
    from alma.services.s2_vectors import _lookup_ids_for_row, _lookup_key_for_row

    summary: Counter[str] = Counter()
    rows = _select_s2_candidates(conn, limit=limit)
    summary["candidates"] = len(rows)
    if not rows:
        return summary

    add_job_log(
        job_id,
        f"Phase 1.5: batched Semantic Scholar fill for {len(rows)} paper(s)",
        step="s2_phase_prepare",
        data={"papers": len(rows)},
    )

    paper_lookup_ids: dict[str, list[str]] = {
        str(row["id"]): _lookup_ids_for_row(row) for row in rows
    }

    chunk_size = 100
    processed = 0
    for start in range(0, len(rows), chunk_size):
        if is_cancellation_requested(job_id):
            break
        chunk_rows = rows[start:start + chunk_size]
        lookup_ids: list[str] = []
        for row in chunk_rows:
            lookup_ids.extend(paper_lookup_ids.get(str(row["id"]), []))
        lookup_ids = list(dict.fromkeys(lookup_ids))
        try:
            fetched = semantic_scholar.fetch_papers_batch(
                lookup_ids,
                batch_size=len(lookup_ids),
                raise_on_error=False,
            )
        except Exception as exc:
            logger.warning("S2 batched fetch failed for chunk %d: %s", start, exc)
            fetched = {}
            summary["chunk_errors"] += 1

        # Index responses by paperId AND DOI for downstream matching.
        fetched_by_s2: dict[str, dict] = {}
        fetched_by_doi: dict[str, dict] = {}
        for paper in fetched.values():
            pid = str(paper.get("paperId") or "").strip()
            if pid:
                fetched_by_s2[pid] = paper
            doi = ""
            try:
                doi = str(((paper.get("externalIds") or {}).get("DOI") or "")).strip().lower()
            except Exception:
                doi = ""
            if doi:
                fetched_by_doi[doi] = paper

        summary["remote_calls"] += 1 if lookup_ids else 0

        for row in chunk_rows:
            paper_id = str(row["id"])
            s2_id = str(row["semantic_scholar_id"] or "").strip()
            doi = str(row["doi"] or "").strip()
            paper = (
                fetched_by_s2.get(s2_id) if s2_id else None
            ) or (
                fetched_by_doi.get(doi.lower()) if doi else None
            )
            lookup_key = _lookup_key_for_row(row)
            if paper is None:
                _write_ledger(
                    conn,
                    paper_id=paper_id,
                    source=S2_SOURCE,
                    lookup_key=lookup_key,
                    status="terminal_no_match",
                    reason="s2_no_match",
                    fields_filled=[],
                    fields_key="s2_paper_v1",
                )
                summary["terminal_no_match"] += 1
                processed += 1
                continue
            try:
                fields_filled = _apply_s2_paper(
                    conn, paper_id=paper_id, row=row, paper=paper
                )
            except Exception as exc:
                logger.warning("S2 apply failed for %s: %s", paper_id, exc)
                _write_ledger(
                    conn,
                    paper_id=paper_id,
                    source=S2_SOURCE,
                    lookup_key=lookup_key,
                    status=RETRYABLE_STATUS,
                    reason=str(exc),
                    fields_filled=[],
                    fields_key="s2_paper_v1",
                    retry_after=timedelta(hours=6),
                )
                summary["retryable_error"] += 1
                processed += 1
                continue
            if fields_filled:
                status_value = "enriched"
                reason = f"filled:{len(fields_filled)}"
                summary["enriched"] += 1
                retry = None
                for field in fields_filled:
                    summary[f"field.{field}"] += 1
            else:
                status_value = "unchanged"
                reason = "no_local_improvements"
                summary["unchanged"] += 1
                retry = UNCHANGED_RETRY_AFTER
            _write_ledger(
                conn,
                paper_id=paper_id,
                source=S2_SOURCE,
                lookup_key=lookup_key,
                status=status_value,
                reason=reason,
                fields_filled=fields_filled,
                fields_key="s2_paper_v1",
                retry_after=retry,
            )
            processed += 1

        conn.commit()
        set_job_status(
            job_id,
            status="running",
            processed=base_processed,
            total=base_total,
            message=(
                f"Phase 1.5 (S2 batched): {processed}/{len(rows)} paper(s), "
                f"enriched={int(summary['enriched'])} unchanged={int(summary['unchanged'])} "
                f"no_match={int(summary['terminal_no_match'])}"
            ),
        )

    add_job_log(
        job_id,
        "Phase 1.5 batched S2 complete",
        step="s2_phase_done",
        data=dict(summary),
    )
    return summary


def build_enrichment_status(conn: sqlite3.Connection) -> dict[str, Any]:
    """Return pure-read corpus enrichment bookkeeping for Settings/API."""
    _ensure_enrichment_status_table(conn)
    status_rows = conn.execute(
        """
        SELECT status, COUNT(*) AS count
        FROM paper_enrichment_status
        WHERE source = ? AND purpose = ?
        GROUP BY status
        ORDER BY status
        """,
        (OPENALEX_SOURCE, METADATA_PURPOSE),
    ).fetchall()
    missing = conn.execute(
        """
        SELECT
            COUNT(*) AS total,
            SUM(CASE WHEN COALESCE(NULLIF(TRIM(openalex_id), ''), '') != '' THEN 1 ELSE 0 END) AS with_openalex_id,
            SUM(CASE WHEN COALESCE(NULLIF(TRIM(openalex_id), ''), '') = '' THEN 1 ELSE 0 END) AS without_openalex_id,
            SUM(CASE WHEN COALESCE(NULLIF(TRIM(openalex_id), ''), '') != ''
                      AND COALESCE(NULLIF(TRIM(doi), ''), '') = '' THEN 1 ELSE 0 END) AS missing_doi,
            SUM(CASE WHEN COALESCE(NULLIF(TRIM(openalex_id), ''), '') != ''
                      AND COALESCE(NULLIF(TRIM(abstract), ''), '') = '' THEN 1 ELSE 0 END) AS missing_abstract,
            SUM(CASE WHEN COALESCE(NULLIF(TRIM(openalex_id), ''), '') != ''
                      AND COALESCE(NULLIF(TRIM(url), ''), '') = '' THEN 1 ELSE 0 END) AS missing_url,
            SUM(CASE WHEN COALESCE(NULLIF(TRIM(openalex_id), ''), '') != ''
                      AND COALESCE(NULLIF(TRIM(publication_date), ''), '') = '' THEN 1 ELSE 0 END) AS missing_publication_date,
            SUM(CASE WHEN COALESCE(NULLIF(TRIM(openalex_id), ''), '') != ''
                      AND NOT EXISTS (SELECT 1 FROM publication_authors pa WHERE pa.paper_id = papers.id)
                     THEN 1 ELSE 0 END) AS missing_authorships,
            SUM(CASE WHEN COALESCE(NULLIF(TRIM(openalex_id), ''), '') != ''
                      AND NOT EXISTS (SELECT 1 FROM publication_topics pt WHERE pt.paper_id = papers.id)
                     THEN 1 ELSE 0 END) AS missing_topics,
            SUM(CASE WHEN COALESCE(NULLIF(TRIM(openalex_id), ''), '') != ''
                      AND NOT EXISTS (SELECT 1 FROM publication_references pr WHERE pr.paper_id = papers.id)
                     THEN 1 ELSE 0 END) AS missing_references
        FROM papers
        WHERE COALESCE(canonical_paper_id, '') = ''
        """
    ).fetchone()
    retry_waiting = conn.execute(
        """
        SELECT COUNT(*) AS c
        FROM paper_enrichment_status
        WHERE source = ?
          AND purpose = ?
          AND status = ?
          AND next_retry_at IS NOT NULL
          AND next_retry_at > ?
        """,
        (OPENALEX_SOURCE, METADATA_PURPOSE, RETRYABLE_STATUS, _utcnow_iso()),
    ).fetchone()
    return {
        "source": OPENALEX_SOURCE,
        "purpose": METADATA_PURPOSE,
        "fields_key": OPENALEX_WORKS_FIELDS_KEY,
        "fields_requested": OPENALEX_WORKS_FIELDS,
        "status_counts": {str(row["status"]): int(row["count"] or 0) for row in status_rows},
        "eligible_now": _candidate_count(conn),
        "retryable_waiting": int(retry_waiting["c"] or 0) if retry_waiting else 0,
        "papers_total": int(missing["total"] or 0) if missing else 0,
        "with_openalex_id": int(missing["with_openalex_id"] or 0) if missing else 0,
        "without_openalex_id": int(missing["without_openalex_id"] or 0) if missing else 0,
        "missing": {
            "doi": int(missing["missing_doi"] or 0) if missing else 0,
            "abstract": int(missing["missing_abstract"] or 0) if missing else 0,
            "url": int(missing["missing_url"] or 0) if missing else 0,
            "publication_date": int(missing["missing_publication_date"] or 0) if missing else 0,
            "authorships": int(missing["missing_authorships"] or 0) if missing else 0,
            "topics": int(missing["missing_topics"] or 0) if missing else 0,
            "references": int(missing["missing_references"] or 0) if missing else 0,
        },
    }


def list_enrichment_status_items(
    conn: sqlite3.Connection,
    *,
    status_filter: str | None = None,
    paper_id: str | None = None,
    limit: int = 50,
    offset: int = 0,
) -> list[dict[str, Any]]:
    """Return per-paper enrichment ledger rows for inspection."""
    _ensure_enrichment_status_table(conn)
    clauses = ["es.source = ?", "es.purpose = ?"]
    params: list[Any] = [OPENALEX_SOURCE, METADATA_PURPOSE]
    if status_filter:
        clauses.append("es.status = ?")
        params.append(status_filter.strip())
    if paper_id:
        clauses.append("es.paper_id = ?")
        params.append(paper_id.strip())
    params.extend([max(0, min(int(limit or 0), 500)), max(0, int(offset or 0))])
    rows = conn.execute(
        f"""
        SELECT
            es.paper_id,
            p.title,
            p.openalex_id,
            p.doi,
            es.source,
            es.purpose,
            es.lookup_key,
            es.fields_key,
            es.status,
            es.reason,
            es.fields_filled_json,
            es.attempts,
            es.last_attempt_at,
            es.next_retry_at,
            es.updated_at
        FROM paper_enrichment_status es
        LEFT JOIN papers p ON p.id = es.paper_id
        WHERE {" AND ".join(clauses)}
        ORDER BY es.updated_at DESC, es.paper_id ASC
        LIMIT ? OFFSET ?
        """,
        params,
    ).fetchall()
    items: list[dict[str, Any]] = []
    for row in rows:
        fields_filled: list[str] = []
        raw_fields = row["fields_filled_json"]
        if raw_fields:
            try:
                parsed = json.loads(raw_fields)
                if isinstance(parsed, list):
                    fields_filled = [str(item) for item in parsed]
            except Exception:
                fields_filled = []
        items.append(
            {
                "paper_id": row["paper_id"],
                "title": row["title"],
                "openalex_id": row["openalex_id"],
                "doi": row["doi"],
                "source": row["source"],
                "purpose": row["purpose"],
                "lookup_key": row["lookup_key"],
                "fields_key": row["fields_key"],
                "status": row["status"],
                "reason": row["reason"],
                "fields_filled": fields_filled,
                "attempts": int(row["attempts"] or 0),
                "last_attempt_at": row["last_attempt_at"],
                "next_retry_at": row["next_retry_at"],
                "updated_at": row["updated_at"],
            }
        )
    return items


def run_corpus_metadata_rehydration(
    job_id: str,
    *,
    limit: int | None = None,
    force: bool = False,
    set_job_status: Callable[..., None],
    add_job_log: Callable[..., None],
    is_cancellation_requested: Callable[[str], bool],
) -> dict[str, Any]:
    """Rehydrate missing paper metadata from batched OpenAlex work-ID fetches."""
    from alma.api.deps import open_db_connection

    conn = open_db_connection()
    if limit is not None:
        limit = max(1, min(int(limit), 100_000))
    batch_size = 50
    retry_after = timedelta(hours=6)
    summary: Counter[str] = Counter()
    field_counts: Counter[str] = Counter()
    try:
        _ensure_enrichment_status_table(conn)
        rows = _select_openalex_candidates(conn, limit=limit, force=force)
        total = len(rows)
        summary["candidates"] = total
        if total == 0:
            result = {
                "source": OPENALEX_SOURCE,
                "purpose": METADATA_PURPOSE,
                "fields_key": OPENALEX_WORKS_FIELDS_KEY,
                "candidates": 0,
                "requested": 0,
                "fetched": 0,
                "enriched": 0,
                "unchanged": 0,
                "terminal_no_match": 0,
                "retryable_error": 0,
                "db_writes": 0,
                "remote_calls": 0,
                "message": "No papers need OpenAlex metadata rehydration",
            }
            set_job_status(
                job_id,
                status="completed",
                processed=0,
                total=0,
                message=result["message"],
                result=result,
                finished_at=_utcnow_iso(),
            )
            return result

        set_job_status(
            job_id,
            status="running",
            processed=0,
            total=total,
            message=f"Rehydrating OpenAlex metadata for {total} paper(s)",
        )
        add_job_log(
            job_id,
            "Prepared OpenAlex metadata rehydration",
            step="prepare",
            data={
                "candidates": total,
                "batch_size": batch_size,
                "force": force,
                "fields_key": OPENALEX_WORKS_FIELDS_KEY,
                "max_remote_calls": (total + batch_size - 1) // batch_size,
            },
        )

        processed = 0
        # Paper IDs whose Phase 1 OpenAlex pass left abstract still missing
        # — collected during the loop so Phase 2 can fall back to S2 and
        # Crossref without another scan of the ledger.
        fallback_paper_ids: list[str] = []
        for start in range(0, total, batch_size):
            if is_cancellation_requested(job_id):
                conn.commit()
                set_job_status(
                    job_id,
                    status="cancelled",
                    processed=processed,
                    total=total,
                    message="Corpus metadata rehydration cancelled",
                    finished_at=_utcnow_iso(),
                )
                return {
                    **dict(summary),
                    "field_counts": dict(field_counts),
                    "cancelled": True,
                    "message": "Corpus metadata rehydration cancelled",
                }

            batch_rows = rows[start:start + batch_size]
            batch_ids = [_openalex_work_id(str(row["openalex_id"] or "")) for row in batch_rows]
            batch_ids = [work_id for work_id in batch_ids if work_id]
            works_by_id, retryable_errors, remote_calls = _fetch_openalex_chunk(batch_ids)
            summary["remote_calls"] += remote_calls
            summary["requested"] += len(batch_ids)
            summary["fetched"] += len(works_by_id)

            batch_summary: Counter[str] = Counter()
            for row in batch_rows:
                paper_id = str(row["id"])
                work_id = _openalex_work_id(str(row["openalex_id"] or ""))
                lookup_key = openalex_lookup_key(work_id)
                key = work_id.lower()
                if not work_id:
                    _upsert_enrichment_status(
                        conn,
                        paper_id=paper_id,
                        lookup_key="",
                        status="terminal_no_match",
                        reason="missing_openalex_id",
                    )
                    summary["terminal_no_match"] += 1
                    batch_summary["terminal_no_match"] += 1
                    processed += 1
                    continue

                if work_id in retryable_errors or key in retryable_errors:
                    reason = retryable_errors.get(work_id) or retryable_errors.get(key) or "openalex_error"
                    _upsert_enrichment_status(
                        conn,
                        paper_id=paper_id,
                        lookup_key=lookup_key,
                        status=RETRYABLE_STATUS,
                        reason=reason,
                        retry_after=retry_after,
                    )
                    summary["retryable_error"] += 1
                    batch_summary["retryable_error"] += 1
                    processed += 1
                    continue

                raw_work = works_by_id.get(key)
                if raw_work is None:
                    _upsert_enrichment_status(
                        conn,
                        paper_id=paper_id,
                        lookup_key=lookup_key,
                        status="terminal_no_match",
                        reason="openalex_id_not_found",
                    )
                    summary["terminal_no_match"] += 1
                    batch_summary["terminal_no_match"] += 1
                    processed += 1
                    continue

                normalized = _normalize_work(raw_work)
                merge_summary = merge_openalex_work_metadata(conn, paper_id, normalized)
                fields_filled = [str(field) for field in merge_summary.get("fields_filled") or []]
                for field in fields_filled:
                    field_counts[field] += 1
                db_writes = int(merge_summary.get("db_writes") or 0)
                summary["db_writes"] += db_writes
                if fields_filled:
                    status_value = "enriched"
                    reason = f"filled:{len(fields_filled)}"
                    summary["enriched"] += 1
                    batch_summary["enriched"] += 1
                else:
                    status_value = "unchanged"
                    reason = "no_local_improvements"
                    summary["unchanged"] += 1
                    batch_summary["unchanged"] += 1
                # Cooldown for `unchanged` so missing-field papers (e.g. ARVO
                # / Journal of Vision works that OpenAlex hasn't indexed
                # abstracts for yet) re-enter the candidate pool later
                # instead of becoming a permanent dead end.
                unchanged_retry = (
                    UNCHANGED_RETRY_AFTER if status_value == "unchanged" else None
                )
                _upsert_enrichment_status(
                    conn,
                    paper_id=paper_id,
                    lookup_key=lookup_key,
                    status=status_value,
                    reason=reason,
                    fields_filled=fields_filled,
                    retry_after=unchanged_retry,
                )
                # Queue Phase 2 fallback when OpenAlex didn't fill the
                # abstract AND we still have a DOI / s2_id to try.
                if not _abstract_present(_read_hydration_row(conn, paper_id)):
                    latest = conn.execute(
                        "SELECT doi, semantic_scholar_id FROM papers WHERE id = ?",
                        (paper_id,),
                    ).fetchone()
                    if latest is not None and (
                        str(latest["doi"] or "").strip()
                        or str(latest["semantic_scholar_id"] or "").strip()
                    ):
                        fallback_paper_ids.append(paper_id)
                processed += 1

            conn.commit()
            set_job_status(
                job_id,
                status="running",
                processed=processed,
                total=total,
                message=f"Rehydrated {processed}/{total} paper(s)",
            )
            add_job_log(
                job_id,
                f"OpenAlex rehydration batch {start // batch_size + 1} complete",
                step="batch",
                data={
                    "processed": processed,
                    "total": total,
                    "remote_calls": remote_calls,
                    "requested": len(batch_ids),
                    "fetched": len(works_by_id),
                    "enriched": int(batch_summary["enriched"]),
                    "unchanged": int(batch_summary["unchanged"]),
                    "terminal_no_match": int(batch_summary["terminal_no_match"]),
                    "retryable_error": int(batch_summary["retryable_error"]),
                },
            )

        # Phase 1.5 — batched Semantic Scholar pass. Runs on every paper
        # with DOI or s2_id whose S2 ledger isn't already
        # `enriched`/`terminal_no_match`. Distinct from the existing
        # `services/s2_vectors.run_s2_vector_backfill` job (which is
        # vector-fetch-flow specific and skips papers that already have
        # an S2 vector); this phase fills `tldr` +
        # `influential_citation_count` + abstract-fallback for every
        # eligible paper, since those S2-only fields drive the Discovery
        # ranker (`citation_quality`) and PaperCard's TLDR display.
        s2_summary: Counter[str] = Counter()
        if not is_cancellation_requested(job_id):
            try:
                s2_summary = _run_s2_batched_phase(
                    conn,
                    limit=limit,
                    job_id=job_id,
                    set_job_status=set_job_status,
                    add_job_log=add_job_log,
                    is_cancellation_requested=is_cancellation_requested,
                    base_processed=processed,
                    base_total=total,
                )
            except Exception as exc:
                logger.warning("Phase 1.5 batched S2 failed: %s", exc)
                add_job_log(
                    job_id,
                    f"Phase 1.5 batched S2 failed: {exc}",
                    level="WARNING",
                    step="s2_phase_error",
                    data={"error": str(exc)},
                )

        # Phase 2 — cross-source abstract fallback. Per-paper (not batched)
        # because Crossref has no batch-by-DOI endpoint. Runs only on rows
        # OpenAlex AND S2 both left without an abstract — typically ARVO /
        # Journal of Vision proceedings whose upstream
        # `abstract_inverted_index` is null AND whose S2 records also
        # lack abstracts.
        fallback_summary: Counter[str] = Counter()
        # Re-filter the Phase-1 fallback list against current paper state:
        # Phase 1.5's batched S2 call may have filled the abstract on some
        # of these rows, so they no longer need Crossref.
        fallback_paper_ids = list(dict.fromkeys(fallback_paper_ids))
        if fallback_paper_ids:
            still_missing: list[str] = []
            for pid in fallback_paper_ids:
                row_now = conn.execute(
                    "SELECT abstract, doi FROM papers WHERE id = ?", (pid,)
                ).fetchone()
                if row_now is None:
                    continue
                if str(row_now["abstract"] or "").strip():
                    continue
                if not str(row_now["doi"] or "").strip():
                    continue  # Crossref needs a DOI.
                still_missing.append(pid)
            fallback_paper_ids = still_missing
        if fallback_paper_ids:
            add_job_log(
                job_id,
                "Phase 2: Crossref abstract fallback for residual misses",
                step="fallback_prepare",
                data={"papers": len(fallback_paper_ids)},
            )
            for idx, paper_id_local in enumerate(fallback_paper_ids, start=1):
                if is_cancellation_requested(job_id):
                    break
                try:
                    # S2 already ran in Phase 1.5 — only Crossref left.
                    hyd = hydrate_paper_metadata(
                        conn,
                        paper_id_local,
                        sources=(CROSSREF_SOURCE,),
                    )
                except Exception as exc:
                    logger.warning(
                        "Phase 2 hydration failed for %s: %s", paper_id_local, exc
                    )
                    continue
                fallback_summary["attempted"] += 1
                if hyd.get("abstract_filled"):
                    fallback_summary["abstract_filled"] += 1
                if hyd.get("sources_filled"):
                    fallback_summary["enriched"] += 1
                    for src in hyd["sources_filled"]:
                        fallback_summary[f"source.{src}"] += 1
                if idx % 50 == 0 or idx == len(fallback_paper_ids):
                    conn.commit()
                    set_job_status(
                        job_id,
                        status="running",
                        processed=processed,
                        total=total,
                        message=(
                            f"Cross-source fallback: {idx}/{len(fallback_paper_ids)} processed, "
                            f"{int(fallback_summary['abstract_filled'])} abstracts filled"
                        ),
                    )
            add_job_log(
                job_id,
                "Phase 2 cross-source fallback complete",
                step="fallback_done",
                data=dict(fallback_summary),
            )

        result = {
            "source": OPENALEX_SOURCE,
            "purpose": METADATA_PURPOSE,
            "fields_key": OPENALEX_WORKS_FIELDS_KEY,
            "candidates": int(summary["candidates"]),
            "requested": int(summary["requested"]),
            "fetched": int(summary["fetched"]),
            "enriched": int(summary["enriched"]),
            "unchanged": int(summary["unchanged"]),
            "terminal_no_match": int(summary["terminal_no_match"]),
            "retryable_error": int(summary["retryable_error"]),
            "db_writes": int(summary["db_writes"]),
            "remote_calls": int(summary["remote_calls"]),
            "field_counts": dict(field_counts),
            "s2_phase": dict(s2_summary),
            "fallback": dict(fallback_summary),
            "force": force,
            "message": (
                "Corpus metadata rehydration complete: "
                f"OA enriched={int(summary['enriched'])} unchanged={int(summary['unchanged'])} "
                f"retry={int(summary['retryable_error'])}; "
                f"S2 enriched={int(s2_summary.get('enriched', 0))} "
                f"unchanged={int(s2_summary.get('unchanged', 0))} "
                f"no_match={int(s2_summary.get('terminal_no_match', 0))}; "
                f"Crossref abstracts_filled={int(fallback_summary.get('abstract_filled', 0))}"
            ),
        }
        set_job_status(
            job_id,
            status="completed",
            processed=processed,
            total=total,
            message=result["message"],
            result=result,
            finished_at=_utcnow_iso(),
        )
        add_job_log(job_id, "OpenAlex metadata rehydration complete", step="done", data=result)
        return result
    except Exception:
        conn.rollback()
        logger.exception("Corpus metadata rehydration failed")
        raise
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Per-paper hydration with cross-source fallback
# ---------------------------------------------------------------------------
#
# `hydrate_paper_metadata` is the canonical entry point for filling missing
# bibliographic fields on ONE local paper. It fans out OpenAlex → Semantic
# Scholar → Crossref by DOI, fill-only on every step, and stops once
# `abstract` is non-empty (the field local SPECTER2 actually requires).
# Used by the rehydration bulk runner AND by the canonical paper-insert
# sites so newly-added papers get hydrated without waiting for a manual
# corpus-maintenance click.

_HYDRATE_FIELDS = (
    "id",
    "title",
    "authors",
    "abstract",
    "url",
    "doi",
    "journal",
    "publication_date",
    "year",
    "openalex_id",
    "semantic_scholar_id",
    "semantic_scholar_corpus_id",
    "cited_by_count",
    "tldr",
)


def _read_hydration_row(conn: sqlite3.Connection, paper_id: str) -> sqlite3.Row | None:
    return conn.execute(
        f"SELECT {', '.join(_HYDRATE_FIELDS)} FROM papers WHERE id = ?",
        (paper_id,),
    ).fetchone()


def _abstract_present(row: sqlite3.Row | None) -> bool:
    if row is None:
        return False
    return bool(str(row["abstract"] or "").strip())


def _apply_s2_paper(conn: sqlite3.Connection, *, paper_id: str, row: sqlite3.Row, paper: dict) -> list[str]:
    """Fill-only paper UPDATE from a Semantic Scholar paper response.

    Wraps `services/s2_vectors._apply_s2_metadata` (which fills the
    canonical bibliographic fields) and ALSO persists `tldr` and
    `influential_citation_count` from the same response — those two
    are S2-only signals that the user asked us to capture
    everywhere we can ("enrich all possible").
    """
    from alma.services.s2_vectors import _apply_s2_metadata as _s2_apply

    before = _read_hydration_row(conn, paper_id)
    _s2_apply(conn, paper_id=paper_id, row=row, paper=paper)
    # tldr + influential_citation_count: S2 returns these in the same
    # batch projection so writing them is free. Mirrors
    # `s2_vectors.run_s2_vector_backfill` lines 507-519.
    tldr_obj = paper.get("tldr") if isinstance(paper, dict) else None
    tldr_text = ""
    if isinstance(tldr_obj, dict):
        tldr_text = str(tldr_obj.get("text") or "").strip()
    try:
        influential_count = int(paper.get("influentialCitationCount") or 0)
    except (TypeError, ValueError):
        influential_count = 0
    if tldr_text or influential_count > 0:
        conn.execute(
            """
            UPDATE papers
            SET tldr = COALESCE(NULLIF(tldr, ''), NULLIF(?, '')),
                influential_citation_count = CASE
                    WHEN ? > COALESCE(influential_citation_count, 0)
                        THEN ?
                    ELSE influential_citation_count
                END
            WHERE id = ?
            """,
            (tldr_text, influential_count, influential_count, paper_id),
        )
    after = _read_hydration_row(conn, paper_id)
    if before is None or after is None:
        return []
    filled: list[str] = []
    for field in (
        "abstract",
        "url",
        "doi",
        "publication_date",
        "year",
        "semantic_scholar_id",
        "semantic_scholar_corpus_id",
        "cited_by_count",
        "tldr",
    ):
        if str(before[field] or "") != str(after[field] or ""):
            filled.append(field)
    # influential_citation_count isn't on _HYDRATE_FIELDS; check it
    # explicitly so it shows up in fields_filled for ledger reporting.
    if influential_count > 0:
        old_ic = conn.execute(
            "SELECT influential_citation_count FROM papers WHERE id = ?",
            (paper_id,),
        ).fetchone()
        if old_ic is not None and int(old_ic[0] or 0) >= influential_count:
            pass
        else:
            filled.append("influential_citation_count")
    return filled


def _apply_crossref_candidate(
    conn: sqlite3.Connection, *, paper_id: str, candidate: dict
) -> list[str]:
    """Fill-only paper UPDATE from a Crossref candidate dict."""
    from alma.core.utils import normalize_doi

    abstract = str(candidate.get("abstract") or "").strip()
    journal = str(candidate.get("journal") or "").strip()
    publication_date = str(candidate.get("publication_date") or "").strip()
    url = str(candidate.get("url") or "").strip()
    doi = normalize_doi(str(candidate.get("doi") or "")) or ""
    try:
        year = int(candidate.get("year")) if candidate.get("year") is not None else None
    except (TypeError, ValueError):
        year = None
    try:
        cited_by_count = int(candidate.get("cited_by_count") or 0)
    except (TypeError, ValueError):
        cited_by_count = 0

    before = _read_hydration_row(conn, paper_id)
    if before is None:
        return []

    conn.execute(
        """
        UPDATE papers
        SET abstract = CASE WHEN COALESCE(abstract, '') = '' AND ? != '' THEN ? ELSE abstract END,
            journal = CASE WHEN COALESCE(journal, '') = '' AND ? != '' THEN ? ELSE journal END,
            publication_date = CASE
                WHEN COALESCE(publication_date, '') = '' AND ? != '' THEN ?
                ELSE publication_date
            END,
            url = CASE WHEN COALESCE(url, '') = '' AND ? != '' THEN ? ELSE url END,
            doi = CASE WHEN COALESCE(doi, '') = '' AND ? != '' THEN ? ELSE doi END,
            year = COALESCE(year, ?),
            cited_by_count = CASE
                WHEN ? > COALESCE(cited_by_count, 0) THEN ?
                ELSE cited_by_count
            END,
            updated_at = ?
        WHERE id = ?
        """,
        (
            abstract, abstract,
            journal, journal,
            publication_date, publication_date,
            url, url,
            doi, doi,
            year,
            cited_by_count, cited_by_count,
            _utcnow_iso(),
            paper_id,
        ),
    )

    after = _read_hydration_row(conn, paper_id)
    if after is None:
        return []
    filled: list[str] = []
    for field in (
        "abstract",
        "journal",
        "publication_date",
        "url",
        "doi",
        "year",
        "cited_by_count",
    ):
        if str(before[field] or "") != str(after[field] or ""):
            filled.append(field)
    return filled


def _ledger_status(conn: sqlite3.Connection, *, paper_id: str, source: str) -> str | None:
    row = conn.execute(
        """
        SELECT status FROM paper_enrichment_status
        WHERE paper_id = ? AND source = ? AND purpose = ?
        """,
        (paper_id, source, METADATA_PURPOSE),
    ).fetchone()
    return str(row["status"]) if row else None


def _write_ledger(
    conn: sqlite3.Connection,
    *,
    paper_id: str,
    source: str,
    lookup_key: str,
    status: str,
    reason: str,
    fields_filled: list[str],
    fields_key: str,
    retry_after: timedelta | None = None,
) -> None:
    """Generic ledger writer mirroring `_upsert_enrichment_status` for any source."""
    now = _utcnow()
    next_retry_at = (now + retry_after).isoformat() if retry_after else None
    conn.execute(
        """
        INSERT INTO paper_enrichment_status (
            paper_id, source, purpose, lookup_key, fields_key, status, reason,
            fields_requested_json, fields_filled_json, attempts,
            last_attempt_at, next_retry_at, updated_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 1, ?, ?, ?)
        ON CONFLICT(paper_id, source, purpose) DO UPDATE SET
            lookup_key = excluded.lookup_key,
            fields_key = excluded.fields_key,
            status = excluded.status,
            reason = excluded.reason,
            fields_requested_json = excluded.fields_requested_json,
            fields_filled_json = excluded.fields_filled_json,
            attempts = CASE
                WHEN paper_enrichment_status.lookup_key = excluded.lookup_key
                 AND paper_enrichment_status.fields_key = excluded.fields_key
                THEN paper_enrichment_status.attempts + 1
                ELSE 1
            END,
            last_attempt_at = excluded.last_attempt_at,
            next_retry_at = excluded.next_retry_at,
            updated_at = excluded.updated_at
        """,
        (
            paper_id, source, METADATA_PURPOSE, lookup_key, fields_key,
            status, reason, _json([fields_key]), _json(fields_filled),
            now.isoformat(), next_retry_at, now.isoformat(),
        ),
    )


def _hydrate_via_openalex(conn: sqlite3.Connection, paper_id: str, row: sqlite3.Row) -> tuple[str, list[str]]:
    """Returns `(status, fields_filled)`."""
    work_id = _openalex_work_id(str(row["openalex_id"] or ""))
    if not work_id:
        return ("skipped", [])
    lookup_key = openalex_lookup_key(work_id)
    works_by_id, retryable, _ = _fetch_openalex_chunk([work_id])
    key = work_id.lower()
    if key in retryable or work_id in retryable:
        reason = retryable.get(work_id) or retryable.get(key) or "openalex_error"
        _upsert_enrichment_status(
            conn, paper_id=paper_id, lookup_key=lookup_key,
            status=RETRYABLE_STATUS, reason=reason,
            retry_after=timedelta(hours=6),
        )
        return (RETRYABLE_STATUS, [])
    raw_work = works_by_id.get(key)
    if raw_work is None:
        _upsert_enrichment_status(
            conn, paper_id=paper_id, lookup_key=lookup_key,
            status="terminal_no_match", reason="openalex_id_not_found",
        )
        return ("terminal_no_match", [])
    merge_summary = merge_openalex_work_metadata(conn, paper_id, _normalize_work(raw_work))
    fields_filled = [str(f) for f in merge_summary.get("fields_filled") or []]
    if fields_filled:
        status_value = "enriched"
        reason = f"filled:{len(fields_filled)}"
    else:
        status_value = "unchanged"
        reason = "no_local_improvements"
    _upsert_enrichment_status(
        conn, paper_id=paper_id, lookup_key=lookup_key,
        status=status_value, reason=reason, fields_filled=fields_filled,
        retry_after=UNCHANGED_RETRY_AFTER if status_value == "unchanged" else None,
    )
    return (status_value, fields_filled)


def _hydrate_via_s2(conn: sqlite3.Connection, paper_id: str, row: sqlite3.Row) -> tuple[str, list[str]]:
    """Try Semantic Scholar by paperId then DOI."""
    from alma.discovery import semantic_scholar
    from alma.services.s2_vectors import _lookup_ids_for_row, _lookup_key_for_row

    lookup_ids = _lookup_ids_for_row(row)
    lookup_key = _lookup_key_for_row(row)
    if not lookup_ids:
        return ("skipped", [])
    try:
        fetched = semantic_scholar.fetch_papers_batch(lookup_ids, batch_size=len(lookup_ids))
    except Exception as exc:
        _write_ledger(
            conn, paper_id=paper_id, source=S2_SOURCE, lookup_key=lookup_key,
            status=RETRYABLE_STATUS, reason=str(exc), fields_filled=[],
            fields_key="s2_paper_v1", retry_after=timedelta(hours=6),
        )
        return (RETRYABLE_STATUS, [])
    paper = None
    s2_id = str(row["semantic_scholar_id"] or "").strip()
    doi = str(row["doi"] or "").strip()
    for candidate in fetched.values():
        cand_paper_id = str(candidate.get("paperId") or "").strip()
        cand_doi = ""
        try:
            cand_doi = str(((candidate.get("externalIds") or {}).get("DOI") or "")).strip()
        except Exception:
            cand_doi = ""
        if s2_id and cand_paper_id == s2_id:
            paper = candidate
            break
        if doi and cand_doi.lower() == doi.lower():
            paper = candidate
            break
    if paper is None:
        _write_ledger(
            conn, paper_id=paper_id, source=S2_SOURCE, lookup_key=lookup_key,
            status="terminal_no_match", reason="s2_no_match", fields_filled=[],
            fields_key="s2_paper_v1",
        )
        return ("terminal_no_match", [])
    fields_filled = _apply_s2_paper(conn, paper_id=paper_id, row=row, paper=paper)
    if fields_filled:
        status_value = "enriched"
        reason = f"filled:{len(fields_filled)}"
        retry = None
    else:
        status_value = "unchanged"
        reason = "no_local_improvements"
        retry = UNCHANGED_RETRY_AFTER
    _write_ledger(
        conn, paper_id=paper_id, source=S2_SOURCE, lookup_key=lookup_key,
        status=status_value, reason=reason, fields_filled=fields_filled,
        fields_key="s2_paper_v1", retry_after=retry,
    )
    return (status_value, fields_filled)


def _hydrate_via_crossref(conn: sqlite3.Connection, paper_id: str, row: sqlite3.Row) -> tuple[str, list[str]]:
    """Try Crossref by DOI."""
    from alma.core.utils import normalize_doi
    from alma.discovery.crossref import fetch_work_by_doi

    doi = normalize_doi(str(row["doi"] or "")) or ""
    if not doi:
        return ("skipped", [])
    lookup_key = f"crossref:{doi.lower()}"
    try:
        candidate = fetch_work_by_doi(doi)
    except Exception as exc:
        _write_ledger(
            conn, paper_id=paper_id, source=CROSSREF_SOURCE, lookup_key=lookup_key,
            status=RETRYABLE_STATUS, reason=str(exc), fields_filled=[],
            fields_key="crossref_v1", retry_after=timedelta(hours=6),
        )
        return (RETRYABLE_STATUS, [])
    if candidate is None:
        _write_ledger(
            conn, paper_id=paper_id, source=CROSSREF_SOURCE, lookup_key=lookup_key,
            status="terminal_no_match", reason="crossref_doi_not_found",
            fields_filled=[], fields_key="crossref_v1",
        )
        return ("terminal_no_match", [])
    fields_filled = _apply_crossref_candidate(conn, paper_id=paper_id, candidate=candidate)
    if fields_filled:
        status_value = "enriched"
        reason = f"filled:{len(fields_filled)}"
        retry = None
    else:
        status_value = "unchanged"
        reason = "no_local_improvements"
        retry = UNCHANGED_RETRY_AFTER
    _write_ledger(
        conn, paper_id=paper_id, source=CROSSREF_SOURCE, lookup_key=lookup_key,
        status=status_value, reason=reason, fields_filled=fields_filled,
        fields_key="crossref_v1", retry_after=retry,
    )
    return (status_value, fields_filled)


def hydrate_paper_metadata(
    conn: sqlite3.Connection,
    paper_id: str,
    *,
    sources: tuple[str, ...] = (OPENALEX_SOURCE, S2_SOURCE, CROSSREF_SOURCE),
) -> dict[str, Any]:
    """Best-effort metadata hydration for ONE local paper.

    Fans out OpenAlex (by openalex_id) → Semantic Scholar (by paperId/DOI)
    → Crossref (by DOI), **fill-only on every step, every source runs**.
    Each source contributes whatever fields it has that the local row
    doesn't — abstract, journal, publication_date, citations, tldr,
    influential_citation_count, OA flags, biblio, keywords, etc. The
    helper does NOT short-circuit on abstract presence, because each
    source carries data the others don't (S2 has tldr + influential
    citation count; Crossref has the authoritative bibliographic
    metadata for some venues; OpenAlex has the topology). Per-source
    `paper_enrichment_status` rows let reruns skip work the ledger has
    already covered.

    Skipping by source happens only when the paper lacks the required
    identifier for that source (no openalex_id → skip OpenAlex; no
    DOI/s2_id → skip S2; no DOI → skip Crossref). Already-`enriched`
    or `terminal_no_match` ledger rows from a previous run also skip
    on this pass — see `_ledger_status` guard inside each step.
    """
    _ensure_enrichment_status_table(conn)
    row = _read_hydration_row(conn, paper_id)
    if row is None:
        return {
            "paper_id": paper_id,
            "exists": False,
            "sources_attempted": [],
            "sources_filled": [],
            "abstract_filled": False,
        }

    summary: dict[str, Any] = {
        "paper_id": paper_id,
        "exists": True,
        "sources_attempted": [],
        "sources_filled": [],
        "fields_by_source": {},
        "abstract_filled": False,
        "abstract_already_present": _abstract_present(row),
    }

    for source in sources:
        # Re-read row each iteration so a downstream source sees the
        # fields a prior source already filled (avoids redundant writes
        # and lets the ledger note "no_local_improvements" correctly).
        row = _read_hydration_row(conn, paper_id) or row
        if source == OPENALEX_SOURCE:
            status, filled = _hydrate_via_openalex(conn, paper_id, row)
        elif source == S2_SOURCE:
            status, filled = _hydrate_via_s2(conn, paper_id, row)
        elif source == CROSSREF_SOURCE:
            status, filled = _hydrate_via_crossref(conn, paper_id, row)
        else:
            continue
        if status == "skipped":
            continue
        summary["sources_attempted"].append(source)
        if filled:
            summary["sources_filled"].append(source)
            summary["fields_by_source"][source] = filled

    final_row = _read_hydration_row(conn, paper_id)
    summary["abstract_filled"] = _abstract_present(final_row) and not summary["abstract_already_present"]
    return summary


def schedule_pending_hydration_sweep(
    *,
    reason: str = "paper_insert",
    limit: int = 500,
) -> str | None:
    """Kick off an Activity-enveloped rehydration job in the background.

    Idempotent: if a job is already active for this operation key,
    returns its job_id without queueing a new one. Otherwise queues
    a fresh `paper_metadata_rehydrate_*` job through the same
    `schedule_immediate` + `set_job_status` + `add_job_log`
    envelope the user-facing Settings → Corpus Maintenance route
    uses, so on-add hydration shows up in the Activity tab and
    cancels cleanly.

    Returns the active or newly-queued job_id; returns None if the
    scheduler isn't importable (e.g., during tests or CLI tools).
    """
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

    operation_key = "papers.rehydrate_metadata:openalex:metadata"
    existing = find_active_job(operation_key)
    if existing:
        return str(existing.get("job_id") or "") or None

    import uuid as _uuid

    job_id = f"paper_metadata_rehydrate_{_uuid.uuid4().hex[:10]}"
    bounded_limit = max(1, min(int(limit or 500), 100_000))
    set_job_status(
        job_id,
        status="queued",
        operation_key=operation_key,
        trigger_source=f"auto:{reason}",
        started_at=_utcnow_iso(),
        processed=0,
        total=bounded_limit,
        message=f"OpenAlex metadata rehydration auto-queued for up to {bounded_limit} paper(s)",
    )
    add_job_log(
        job_id,
        "Auto-queued by paper-insert hook",
        step="queued",
        data={"limit": bounded_limit, "trigger_reason": reason},
    )

    def _runner() -> dict[str, Any]:
        return run_corpus_metadata_rehydration(
            job_id,
            limit=bounded_limit,
            force=False,
            set_job_status=set_job_status,
            add_job_log=add_job_log,
            is_cancellation_requested=is_cancellation_requested,
        )

    schedule_immediate(job_id, _runner)
    return job_id


def enqueue_pending_hydration(
    conn: sqlite3.Connection,
    paper_id: str,
    *,
    auto_schedule: bool = True,
) -> bool:
    """Mark a paper as needing hydration without fetching synchronously.

    Used by the canonical paper-insert sites (Library, Feed, Discovery)
    so a freshly-added row enters the rehydration runner's candidate
    pool on the next sweep instead of waiting for the user to click a
    Settings button. Cheap: one INSERT OR IGNORE per identifier source
    the paper has, no HTTP, no OpenAlex projection check.

    When `auto_schedule=True` (the default) and at least one new pending
    ledger row was written, also schedules a background rehydration job
    through the same Activity envelope as the user-facing trigger —
    async, observable, idempotent against an already-running sweep.
    """
    _ensure_enrichment_status_table(conn)
    row = conn.execute(
        "SELECT id, openalex_id, doi, semantic_scholar_id, abstract FROM papers WHERE id = ?",
        (paper_id,),
    ).fetchone()
    if row is None:
        return False
    if str(row["abstract"] or "").strip():
        return False
    has_oa = bool(str(row["openalex_id"] or "").strip())
    has_doi = bool(str(row["doi"] or "").strip())
    has_s2 = bool(str(row["semantic_scholar_id"] or "").strip())
    if not (has_oa or has_doi or has_s2):
        return False
    queued = False
    now = _utcnow_iso()
    sources_to_queue: list[tuple[str, str]] = []
    if has_oa:
        sources_to_queue.append((OPENALEX_SOURCE, OPENALEX_WORKS_FIELDS_KEY))
    if has_s2 or has_doi:
        sources_to_queue.append((S2_SOURCE, "s2_paper_v1"))
    if has_doi:
        sources_to_queue.append((CROSSREF_SOURCE, "crossref_v1"))
    for source, fields_key in sources_to_queue:
        existing = _ledger_status(conn, paper_id=paper_id, source=source)
        if existing in {"enriched", "terminal_no_match"}:
            continue
        conn.execute(
            """
            INSERT INTO paper_enrichment_status (
                paper_id, source, purpose, lookup_key, fields_key, status,
                fields_requested_json, fields_filled_json, attempts,
                last_attempt_at, next_retry_at, updated_at
            )
            VALUES (?, ?, ?, '', ?, ?, '[]', '[]', 0, NULL, NULL, ?)
            ON CONFLICT(paper_id, source, purpose) DO UPDATE SET
                status = CASE
                    WHEN paper_enrichment_status.status IN ('enriched', 'terminal_no_match')
                        THEN paper_enrichment_status.status
                    ELSE excluded.status
                END,
                updated_at = excluded.updated_at
            """,
            (paper_id, source, METADATA_PURPOSE, fields_key, PENDING_STATUS, now),
        )
        queued = True
    if queued and auto_schedule:
        # Fire-and-forget: idempotent against an already-active job. The
        # scheduler import is lazy so this helper stays callable from
        # CLI / test contexts where `alma.api.scheduler` isn't wired.
        try:
            schedule_pending_hydration_sweep(reason="paper_insert")
        except Exception as exc:
            logger.debug("auto schedule_pending_hydration_sweep skipped: %s", exc)
    return queued
