"""Corpus metadata rehydration jobs.

Repairs local paper metadata from authoritative external IDs while
keeping per-paper bookkeeping so reruns skip already-covered work.
"""

from __future__ import annotations

import hashlib
import html
from html.parser import HTMLParser
import json
import logging
import sqlite3
from collections import Counter
from datetime import datetime, timedelta
from typing import Any, Callable

from alma.application.paper_metadata import merge_openalex_work_metadata
from alma.core.utils import (
    normalize_id_list,
    normalize_doi,
    normalize_title_key,
    utcnow as _utcnow,
    utcnow_iso as _utcnow_iso,
)
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
ABSTRACT_RECOVERY_SOURCE = "abstract_recovery"
# Synthetic source used by `_resolve_identifiers_via_title`. Phase 4 of
# `tasks/13_END_TO_END_HYDRATION_VECTOR_CHAIN.md`. Distinct from
# `openalex` / `semantic_scholar` because the lookup is by title, not
# by an existing identifier — different inputs, different retry policy.
TITLE_RESOLUTION_SOURCE = "title_resolution"
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
_AUTO_PAPER_INSERT_HYDRATION_LIMIT = 25


def _json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, default=str)


def _openalex_work_id(raw: str) -> str:
    return _normalize_openalex_work_id(str(raw or "").strip()).strip()


def openalex_lookup_key(raw: str) -> str:
    work_id = _openalex_work_id(raw)
    return f"openalex:{work_id.lower()}" if work_id else ""


def _s2_lookup_key_for_values(semantic_scholar_id: str, doi: str) -> str:
    s2_id = str(semantic_scholar_id or "").strip().lower()
    doi_value = str(doi or "").strip().lower()
    return f"{s2_id}|{doi_value}"


def _crossref_lookup_key(raw_doi: str) -> str:
    doi = normalize_doi(str(raw_doi or "")) or str(raw_doi or "").strip()
    return f"crossref:{doi.lower()}" if doi else ""


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
    target_paper_ids: list[str] | tuple[str, ...] | None = None,
) -> list[sqlite3.Row]:
    _ensure_enrichment_status_table(conn)
    lookup_expr = _openalex_lookup_expr()
    params: list[Any] = [OPENALEX_SOURCE, METADATA_PURPOSE]
    status_clause = _eligible_status_clause(force)
    target_ids = normalize_id_list(target_paper_ids)
    target_clause = ""
    if target_ids:
        target_clause = f"AND p.id IN ({','.join('?' for _ in target_ids)})"
        params.extend(target_ids)
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
          {target_clause}
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
    target_paper_ids: list[str] | tuple[str, ...] | None = None,
) -> list[sqlite3.Row]:
    """Pick papers eligible for the batched Semantic Scholar phase.

    Eligibility: has DOI or `semantic_scholar_id`, isn't a canonical
    duplicate, and the S2 ledger row is missing, stale for a changed
    lookup, pending, or past its retry clock. The phase exists because
    S2 carries fields OpenAlex doesn't — `tldr`,
    `influentialCitationCount` — that the Discovery ranker and
    PaperCard surfaces actively consume.
    """
    _ensure_enrichment_status_table(conn)
    params: list[Any] = [S2_SOURCE, METADATA_PURPOSE]
    target_ids = normalize_id_list(target_paper_ids)
    target_clause = ""
    if target_ids:
        target_clause = f"AND p.id IN ({','.join('?' for _ in target_ids)})"
        params.extend(target_ids)
    params.append(_utcnow_iso())
    limit_clause = ""
    if limit is not None:
        params.append(max(1, int(limit)))
        limit_clause = "LIMIT ?"
    lookup_expr = (
        "lower(trim(COALESCE(p.semantic_scholar_id, ''))) || '|' || "
        "lower(trim(COALESCE(p.doi, '')))"
    )
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
          {target_clause}
          AND (
              COALESCE(NULLIF(TRIM(p.doi), ''), '') != ''
              OR COALESCE(NULLIF(TRIM(p.semantic_scholar_id), ''), '') != ''
          )
          AND (
              es.paper_id IS NULL
              OR COALESCE(es.lookup_key, '') != {lookup_expr}
              OR COALESCE(es.fields_key, '') != 's2_paper_v1'
              OR COALESCE(es.status, '') IN ('pending', 'queued')
              OR (
                  es.status IN ('{RETRYABLE_STATUS}', 'unchanged')
                  AND (es.next_retry_at IS NULL OR es.next_retry_at <= ?)
              )
              OR COALESCE(es.status, '') NOT IN (
                  'enriched', 'unchanged', 'terminal_no_match',
                  '{RETRYABLE_STATUS}', 'pending', 'queued'
              )
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
    target_paper_ids: list[str] | tuple[str, ...] | None = None,
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
    from alma.services.s2_vectors import (
        _fetch_lookup_ids_resilient,
        _lookup_ids_for_row,
        _lookup_key_for_row,
    )

    summary: Counter[str] = Counter()
    rows = _select_s2_candidates(conn, limit=limit, target_paper_ids=target_paper_ids)
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
        fetched, terminal_lookup_errors, retryable_lookup_errors = _fetch_lookup_ids_resilient(
            lookup_ids,
            job_id=job_id,
            add_job_log=add_job_log,
            batch_label=f"s2_metadata:{start}",
        )

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
                row_lookup_ids = paper_lookup_ids.get(paper_id, [])
                retry_reason = next(
                    (
                        retryable_lookup_errors.get(item)
                        for item in row_lookup_ids
                        if retryable_lookup_errors.get(item)
                    ),
                    "",
                )
                if retry_reason:
                    _write_ledger(
                        conn,
                        paper_id=paper_id,
                        source=S2_SOURCE,
                        lookup_key=lookup_key,
                        status=RETRYABLE_STATUS,
                        reason=retry_reason,
                        fields_filled=[],
                        fields_key="s2_paper_v1",
                        retry_after=timedelta(hours=6),
                    )
                    summary["retryable_error"] += 1
                    processed += 1
                    continue
                terminal_reason = next(
                    (
                        terminal_lookup_errors.get(item)
                        for item in row_lookup_ids
                        if terminal_lookup_errors.get(item)
                    ),
                    "",
                )
                _write_ledger(
                    conn,
                    paper_id=paper_id,
                    source=S2_SOURCE,
                    lookup_key=lookup_key,
                    status="terminal_no_match",
                    reason=terminal_reason or "s2_no_match",
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


def _select_crossref_abstract_candidates(
    conn: sqlite3.Connection,
    *,
    limit: int | None,
    seed_paper_ids: list[str] | None = None,
) -> list[str]:
    """Pick DOI papers that still need Crossref abstract fallback.

    This selector is intentionally independent of the OpenAlex phase.
    Insert hooks may create pending Crossref ledger rows even when the
    OpenAlex row is already ``enriched``; those papers must not be
    stranded just because there are zero OpenAlex candidates.
    """

    _ensure_enrichment_status_table(conn)
    params: list[Any] = [CROSSREF_SOURCE, METADATA_PURPOSE]
    seed_clause = ""
    if seed_paper_ids:
        unique = list(dict.fromkeys(str(pid) for pid in seed_paper_ids if str(pid).strip()))
        if unique:
            placeholders = ",".join("?" for _ in unique)
            seed_clause = f"AND p.id IN ({placeholders})"
            params.extend(unique)
    params.append(_utcnow_iso())
    limit_clause = ""
    if limit is not None:
        params.append(max(1, int(limit)))
        limit_clause = "LIMIT ?"
    rows = conn.execute(
        f"""
        SELECT p.id
        FROM papers p
        LEFT JOIN paper_enrichment_status es
          ON es.paper_id = p.id
         AND es.source = ?
         AND es.purpose = ?
        WHERE COALESCE(p.canonical_paper_id, '') = ''
          AND COALESCE(NULLIF(TRIM(p.abstract), ''), '') = ''
          AND COALESCE(NULLIF(TRIM(p.doi), ''), '') != ''
          {seed_clause}
          AND (
              es.paper_id IS NULL
              OR COALESCE(es.lookup_key, '') != ('crossref:' || lower(trim(p.doi)))
              OR COALESCE(es.fields_key, '') != 'crossref_v1'
              OR COALESCE(es.status, '') IN ('pending', 'queued')
              OR (
                  es.status IN ('{RETRYABLE_STATUS}', 'unchanged')
                  AND (es.next_retry_at IS NULL OR es.next_retry_at <= ?)
              )
              OR COALESCE(es.status, '') NOT IN (
                  'enriched', 'unchanged', 'terminal_no_match', '{RETRYABLE_STATUS}', 'pending', 'queued'
              )
          )
        ORDER BY COALESCE(p.fetched_at, p.updated_at, p.created_at, '') DESC, p.id ASC
        {limit_clause}
        """,
        params,
    ).fetchall()
    return [str(row["id"]) for row in rows]


def _run_crossref_abstract_phase(
    conn: sqlite3.Connection,
    *,
    paper_ids: list[str],
    job_id: str,
    set_job_status: Callable[..., None],
    add_job_log: Callable[..., None],
    is_cancellation_requested: Callable[[str], bool],
    base_processed: int,
    base_total: int,
) -> Counter[str]:
    """Phase 2 — batched Crossref abstract fallback."""

    fallback_summary: Counter[str] = Counter()
    paper_ids = list(dict.fromkeys(pid for pid in paper_ids if pid))
    if not paper_ids:
        return fallback_summary

    from alma.core.utils import normalize_doi as _normalize_doi
    from alma.discovery.crossref import fetch_works_by_dois

    paper_to_doi: dict[str, str] = {}
    for pid in paper_ids:
        row_doi = conn.execute("SELECT doi FROM papers WHERE id = ?", (pid,)).fetchone()
        if row_doi is None:
            continue
        norm = _normalize_doi(str(row_doi["doi"] or ""))
        if norm:
            paper_to_doi[pid] = norm

    unique_dois = list({norm for norm in paper_to_doi.values()})
    add_job_log(
        job_id,
        "Phase 2: Crossref abstract fallback (batched)",
        step="fallback_prepare",
        data={
            "papers": len(paper_ids),
            "unique_dois": len(unique_dois),
            "expected_http_calls": (len(unique_dois) + 49) // 50,
        },
    )
    try:
        candidates_by_doi = fetch_works_by_dois(unique_dois, batch_size=50)
    except Exception as exc:
        logger.warning("Phase 2 batched Crossref fetch failed: %s", exc)
        candidates_by_doi = {}

    applied = 0
    for idx, pid in enumerate(paper_ids, start=1):
        if is_cancellation_requested(job_id):
            break
        doi = paper_to_doi.get(pid)
        if not doi:
            fallback_summary["skipped_no_doi"] += 1
            continue
        cand = candidates_by_doi.get(doi.lower())
        lookup_key = f"crossref:{doi.lower()}"
        if cand is None:
            _write_ledger(
                conn,
                paper_id=pid,
                source=CROSSREF_SOURCE,
                lookup_key=lookup_key,
                status="terminal_no_match",
                reason="crossref_doi_not_found",
                fields_filled=[],
                fields_key="crossref_v1",
            )
            fallback_summary["terminal_no_match"] += 1
            continue
        try:
            fields_filled = _apply_crossref_candidate(
                conn, paper_id=pid, candidate=cand
            )
        except Exception as exc:
            logger.warning("Phase 2 apply failed for %s: %s", pid, exc)
            _write_ledger(
                conn,
                paper_id=pid,
                source=CROSSREF_SOURCE,
                lookup_key=lookup_key,
                status=RETRYABLE_STATUS,
                reason=str(exc),
                fields_filled=[],
                fields_key="crossref_v1",
                retry_after=timedelta(hours=6),
            )
            fallback_summary["retryable_error"] += 1
            continue
        applied += 1
        fallback_summary["attempted"] += 1
        if fields_filled:
            status_value = "enriched"
            reason = f"filled:{len(fields_filled)}"
            retry: timedelta | None = None
            fallback_summary["enriched"] += 1
            fallback_summary["source.crossref"] += 1
        else:
            status_value = "unchanged"
            reason = "no_local_improvements"
            retry = UNCHANGED_RETRY_AFTER
        _write_ledger(
            conn,
            paper_id=pid,
            source=CROSSREF_SOURCE,
            lookup_key=lookup_key,
            status=status_value,
            reason=reason,
            fields_filled=fields_filled,
            fields_key="crossref_v1",
            retry_after=retry,
        )
        if "abstract" in (fields_filled or []):
            fallback_summary["abstract_filled"] += 1
        if idx % 50 == 0 or idx == len(paper_ids):
            conn.commit()
            set_job_status(
                job_id,
                status="running",
                processed=base_processed,
                total=base_total,
                message=(
                    f"Cross-source fallback: {idx}/{len(paper_ids)} applied, "
                    f"{int(fallback_summary['abstract_filled'])} abstracts filled"
                ),
            )
    conn.commit()
    add_job_log(
        job_id,
        "Phase 2 cross-source fallback complete",
        step="fallback_done",
        data={**dict(fallback_summary), "applied": applied},
    )
    return fallback_summary


class _AbstractMetaParser(HTMLParser):
    _META_NAMES = {
        "citation_abstract",
        "dc.description",
        "dcterms.description",
        "description",
        "og:description",
    }

    def __init__(self) -> None:
        super().__init__()
        self.abstracts: list[str] = []

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        if tag.lower() != "meta":
            return
        attr_map = {str(k).lower(): (v or "") for k, v in attrs}
        name = (attr_map.get("name") or attr_map.get("property") or "").strip().lower()
        content = html.unescape(attr_map.get("content") or "").strip()
        if name in self._META_NAMES and _is_usable_recovered_abstract(content):
            self.abstracts.append(_clean_recovered_abstract(content))


def _clean_recovered_abstract(text: str) -> str:
    return " ".join(html.unescape(str(text or "")).split()).strip()


def _is_usable_recovered_abstract(text: str) -> bool:
    cleaned = _clean_recovered_abstract(text)
    if len(cleaned) < 40:
        return False
    lower = cleaned.lower()
    if lower.startswith(("http://", "https://")):
        return False
    return " " in cleaned


def _extract_abstract_from_html(text: str) -> str:
    parser = _AbstractMetaParser()
    try:
        parser.feed(text or "")
    except Exception:
        return ""
    return parser.abstracts[0] if parser.abstracts else ""


def _fetch_html_abstract(url: str) -> str:
    raw_url = str(url or "").strip()
    if not raw_url.lower().startswith(("http://", "https://")):
        return ""
    try:
        from alma.core.http_sources import get_source_http_client

        resp = get_source_http_client("publisher").get(raw_url, timeout=20)
    except Exception as exc:
        logger.debug("abstract recovery HTML fetch failed for %s: %s", raw_url, exc)
        return ""
    if not (200 <= int(resp.status_code) < 400):
        return ""
    content_type = (resp.headers.get("content-type") or "").lower()
    if "pdf" in content_type:
        return ""
    try:
        return _extract_abstract_from_html(resp.text or "")
    except Exception:
        return ""


def _unpaywall_urls_for_doi(doi: str) -> list[str]:
    from alma.config import get_contact_email
    from alma.core.http_sources import get_source_http_client

    if not get_contact_email():
        return []
    try:
        resp = get_source_http_client("unpaywall").get(f"/{doi}", timeout=20)
    except Exception as exc:
        logger.debug("Unpaywall lookup failed for %s: %s", doi, exc)
        return []
    if resp.status_code != 200:
        return []
    try:
        payload = resp.json() or {}
    except Exception:
        return []

    urls: list[str] = []
    locations = []
    best = payload.get("best_oa_location")
    if isinstance(best, dict):
        locations.append(best)
    if isinstance(payload.get("oa_locations"), list):
        locations.extend(loc for loc in payload["oa_locations"] if isinstance(loc, dict))
    for loc in locations:
        for key in ("url_for_landing_page", "url", "url_for_pdf"):
            value = str(loc.get(key) or "").strip()
            if value and value not in urls:
                urls.append(value)
    return urls


def _select_abstract_recovery_candidates(
    conn: sqlite3.Connection,
    *,
    limit: int | None,
    target_paper_ids: list[str] | tuple[str, ...] | None = None,
) -> list[str]:
    _ensure_enrichment_status_table(conn)
    params: list[Any] = [ABSTRACT_RECOVERY_SOURCE, METADATA_PURPOSE]
    target_ids = normalize_id_list(target_paper_ids)
    target_clause = ""
    if target_ids:
        target_clause = f"AND p.id IN ({','.join('?' for _ in target_ids)})"
        params.extend(target_ids)
    params.append(_utcnow_iso())
    limit_clause = ""
    if limit is not None:
        params.append(max(1, int(limit)))
        limit_clause = "LIMIT ?"
    rows = conn.execute(
        f"""
        SELECT p.id
        FROM papers p
        LEFT JOIN paper_enrichment_status es
          ON es.paper_id = p.id
         AND es.source = ?
         AND es.purpose = ?
        WHERE COALESCE(p.canonical_paper_id, '') = ''
          AND COALESCE(NULLIF(TRIM(p.abstract), ''), '') = ''
          {target_clause}
          AND (
              COALESCE(NULLIF(TRIM(p.oa_url), ''), '') != ''
              OR COALESCE(NULLIF(TRIM(p.url), ''), '') != ''
              OR COALESCE(NULLIF(TRIM(p.doi), ''), '') != ''
          )
          AND (
              es.paper_id IS NULL
              OR COALESCE(es.status, '') IN ('pending', 'queued')
              OR (
                  es.status IN ('{RETRYABLE_STATUS}', 'unchanged')
                  AND (es.next_retry_at IS NULL OR es.next_retry_at <= ?)
              )
              OR COALESCE(es.status, '') NOT IN (
                  'enriched', 'unchanged', 'terminal_no_match', '{RETRYABLE_STATUS}', 'pending', 'queued'
              )
          )
        ORDER BY COALESCE(p.fetched_at, p.updated_at, p.created_at, '') DESC, p.id ASC
        {limit_clause}
        """,
        params,
    ).fetchall()
    return [str(row["id"]) for row in rows]


def _run_abstract_recovery_phase(
    conn: sqlite3.Connection,
    *,
    limit: int | None,
    target_paper_ids: list[str] | tuple[str, ...] | None = None,
    job_id: str,
    set_job_status: Callable[..., None],
    add_job_log: Callable[..., None],
    is_cancellation_requested: Callable[[str], bool],
    base_processed: int,
    base_total: int,
) -> Counter[str]:
    summary: Counter[str] = Counter()
    paper_ids = _select_abstract_recovery_candidates(
        conn, limit=limit, target_paper_ids=target_paper_ids
    )
    if not paper_ids:
        return summary
    add_job_log(
        job_id,
        "Phase 3: OA / landing-page abstract recovery",
        step="abstract_recovery_prepare",
        data={"papers": len(paper_ids)},
    )
    for idx, paper_id in enumerate(paper_ids, start=1):
        if is_cancellation_requested(job_id):
            break
        row = _read_hydration_row(conn, paper_id)
        if row is None or _abstract_present(row):
            continue
        status, filled = _hydrate_via_abstract_recovery(conn, paper_id, row)
        summary["attempted"] += 1
        summary[status] += 1
        if "abstract" in filled:
            summary["abstract_filled"] += 1
        if idx % 20 == 0 or idx == len(paper_ids):
            conn.commit()
            set_job_status(
                job_id,
                status="running",
                processed=base_processed,
                total=base_total,
                message=(
                    f"Abstract recovery: {idx}/{len(paper_ids)} checked, "
                    f"{int(summary['abstract_filled'])} abstracts filled"
                ),
            )
    conn.commit()
    add_job_log(
        job_id,
        "Phase 3 abstract recovery complete",
        step="abstract_recovery_done",
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


def _select_title_resolution_candidates(
    conn: sqlite3.Connection,
    *,
    limit: int | None,
    target_paper_ids: list[str] | tuple[str, ...] | None = None,
) -> list[sqlite3.Row]:
    """Pick title-only papers whose identifiers haven't been resolved yet.

    A paper is in the pool iff:
    - it has a non-empty title;
    - it has none of `openalex_id` / `doi` / `semantic_scholar_id`;
    - the `paper_enrichment_status` row for `title_resolution` is not
      already `enriched` or `terminal_no_match` (sticky terminal states
      keep us from hammering the same dead-end title).

    Phase 4 of `tasks/13_END_TO_END_HYDRATION_VECTOR_CHAIN.md`.
    """
    bound = "" if limit is None else "LIMIT ?"
    params: list[Any] = [TITLE_RESOLUTION_SOURCE, METADATA_PURPOSE]
    target_ids = normalize_id_list(target_paper_ids)
    target_clause = ""
    if target_ids:
        target_clause = f"AND p.id IN ({','.join('?' for _ in target_ids)})"
        params.extend(target_ids)
    if limit is not None:
        params.append(int(limit))
    return list(
        conn.execute(
            f"""
            SELECT p.id, p.title, p.year,
                   p.openalex_id, p.doi, p.semantic_scholar_id, p.abstract
            FROM papers p
            LEFT JOIN paper_enrichment_status pes
              ON pes.paper_id = p.id
             AND pes.source = ?
             AND pes.purpose = ?
            WHERE COALESCE(NULLIF(TRIM(p.title), ''), '') != ''
              AND COALESCE(NULLIF(TRIM(p.openalex_id), ''), '') = ''
              AND COALESCE(NULLIF(TRIM(p.doi), ''), '') = ''
              AND COALESCE(NULLIF(TRIM(p.semantic_scholar_id), ''), '') = ''
              {target_clause}
              AND (
                  pes.status IS NULL
                  OR pes.status NOT IN ('enriched', 'terminal_no_match')
              )
            {bound}
            """,
            params,
        ).fetchall()
    )


def _run_title_resolution_phase(
    conn: sqlite3.Connection,
    *,
    job_id: str,
    limit: int | None,
    target_paper_ids: list[str] | tuple[str, ...] | None = None,
    add_job_log: Callable[..., None],
    is_cancellation_requested: Callable[[str], bool],
) -> dict[str, int]:
    """Bulk identifier resolution for title-only papers.

    Runs `_resolve_identifiers_via_title` per candidate. Commits per
    paper so a long phase doesn't hold the writer lock (per
    `lessons.md`: "Bulk background jobs must commit per unit of work";
    "Background jobs must release the writer lock before every remote
    call"). Returns a small summary the parent runner attaches to its
    own job result.
    """
    candidates = _select_title_resolution_candidates(
        conn, limit=limit, target_paper_ids=target_paper_ids
    )
    summary = {"attempted": 0, "resolved": 0}
    if not candidates:
        return summary

    add_job_log(
        job_id,
        "Phase 0: title-resolution for title-only papers",
        step="title_resolution_prepare",
        data={"candidates": len(candidates)},
    )

    for idx, row in enumerate(candidates, start=1):
        if is_cancellation_requested(job_id):
            break
        try:
            status, filled = _resolve_identifiers_via_title(
                conn, str(row["id"]), row
            )
        except Exception as exc:
            logger.warning(
                "title-resolution phase failed for %s: %s", row["id"], exc
            )
            continue
        summary["attempted"] += 1
        if filled:
            summary["resolved"] += 1
        # Commit every paper so the writer lock is released before the
        # next remote call.
        conn.commit()

    add_job_log(
        job_id,
        "Phase 0 title-resolution complete",
        step="title_resolution_done",
        data=dict(summary),
    )
    return summary


def run_corpus_metadata_rehydration(
    job_id: str,
    *,
    limit: int | None = None,
    force: bool = False,
    target_paper_ids: list[str] | tuple[str, ...] | None = None,
    set_job_status: Callable[..., None],
    add_job_log: Callable[..., None],
    is_cancellation_requested: Callable[[str], bool],
) -> dict[str, Any]:
    """Rehydrate missing paper metadata from batched OpenAlex work-ID fetches."""
    from alma.api.deps import open_db_connection

    conn = open_db_connection()
    if limit is not None:
        limit = max(1, min(int(limit), 100_000))
    target_ids = normalize_id_list(target_paper_ids)
    batch_size = 50
    retry_after = timedelta(hours=6)
    summary: Counter[str] = Counter()
    field_counts: Counter[str] = Counter()
    try:
        _ensure_enrichment_status_table(conn)

        # Phase 0: identifier resolution for title-only papers. Runs
        # FIRST because a successful resolution lands an openalex_id /
        # DOI / s2_id that the OpenAlex / S2 / Crossref selectors below
        # then pick up in the same run. Bounded by `limit` so a huge
        # backlog of title-only papers can't starve the existing
        # phases. Phase 4–5 of
        # `tasks/13_END_TO_END_HYDRATION_VECTOR_CHAIN.md`.
        title_resolution_summary = _run_title_resolution_phase(
            conn,
            job_id=job_id,
            limit=limit,
            target_paper_ids=target_ids,
            add_job_log=add_job_log,
            is_cancellation_requested=is_cancellation_requested,
        )
        summary["title_resolution_attempted"] = title_resolution_summary["attempted"]
        summary["title_resolution_resolved"] = title_resolution_summary["resolved"]

        rows = _select_openalex_candidates(
            conn, limit=limit, force=force, target_paper_ids=target_ids
        )
        total = len(rows)
        summary["candidates"] = total
        if total == 0:
            add_job_log(
                job_id,
                "No papers need OpenAlex metadata rehydration; continuing downstream phases",
                step="prepare",
                data={"candidates": 0, "force": force},
            )

        set_job_status(
            job_id,
            status="running",
            processed=0,
            total=total,
            message=(
                f"Rehydrating OpenAlex metadata for {total} paper(s)"
                if total
                else "Running downstream hydration phases"
            ),
        )
        add_job_log(
            job_id,
            "Prepared OpenAlex metadata rehydration",
            step="prepare",
            data={
                "candidates": total,
                "batch_size": batch_size,
                "force": force,
                "target_paper_ids": target_ids,
                "fields_key": OPENALEX_WORKS_FIELDS_KEY,
                "max_remote_calls": (total + batch_size - 1) // batch_size,
            },
        )

        processed = 0
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
                    target_paper_ids=target_ids,
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

        # Phase 2 — Crossref abstract fallback. This selector is not
        # gated on OpenAlex candidates: insert hooks can leave a pending
        # Crossref row when OpenAlex is already enriched, and the sweep
        # must still process it.
        fallback_summary: Counter[str] = Counter()
        if not is_cancellation_requested(job_id):
            crossref_ids = _select_crossref_abstract_candidates(
                conn, limit=limit, seed_paper_ids=target_ids or None
            )
            try:
                fallback_summary = _run_crossref_abstract_phase(
                    conn,
                    paper_ids=crossref_ids,
                    job_id=job_id,
                    set_job_status=set_job_status,
                    add_job_log=add_job_log,
                    is_cancellation_requested=is_cancellation_requested,
                    base_processed=processed,
                    base_total=total,
                )
            except Exception as exc:
                logger.warning("Phase 2 Crossref fallback failed: %s", exc)
                add_job_log(
                    job_id,
                    f"Phase 2 Crossref fallback failed: {exc}",
                    level="WARNING",
                    step="fallback_error",
                    data={"error": str(exc)},
                )

        # Phase 3 — public landing-page/OA abstract metadata recovery.
        abstract_recovery_summary: Counter[str] = Counter()
        if not is_cancellation_requested(job_id):
            try:
                abstract_recovery_summary = _run_abstract_recovery_phase(
                    conn,
                    limit=limit,
                    target_paper_ids=target_ids,
                    job_id=job_id,
                    set_job_status=set_job_status,
                    add_job_log=add_job_log,
                    is_cancellation_requested=is_cancellation_requested,
                    base_processed=processed,
                    base_total=total,
                )
            except Exception as exc:
                logger.warning("Phase 3 abstract recovery failed: %s", exc)
                add_job_log(
                    job_id,
                    f"Phase 3 abstract recovery failed: {exc}",
                    level="WARNING",
                    step="abstract_recovery_error",
                    data={"error": str(exc)},
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
            "abstract_recovery": dict(abstract_recovery_summary),
            "force": force,
            "target_paper_ids": target_ids,
            "message": (
                "Corpus metadata rehydration complete: "
                f"OA enriched={int(summary['enriched'])} unchanged={int(summary['unchanged'])} "
                f"retry={int(summary['retryable_error'])}; "
                f"S2 enriched={int(s2_summary.get('enriched', 0))} "
                f"unchanged={int(s2_summary.get('unchanged', 0))} "
                f"no_match={int(s2_summary.get('terminal_no_match', 0))}; "
                f"Crossref abstracts_filled={int(fallback_summary.get('abstract_filled', 0))}; "
                f"OA/landing abstracts_filled={int(abstract_recovery_summary.get('abstract_filled', 0))}"
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

        # Auto-chain to the S2 vector backfill only when this run was
        # *not* started by a manual Settings click. The per-button
        # contract there is "do exactly what the label says": clicking
        # "Rehydrate corpus" must not silently start an S2 vector
        # fetch (which has its own button and its own quota cost).
        # Auto-chain still fires for the per-insert / scheduler paths
        # where there's no user click to confuse.
        try:
            from alma.api.scheduler import get_job_trigger_source
            from alma.services.embedding_chain import schedule_post_hydration_chain

            trigger_source = get_job_trigger_source(job_id) or ""
            if trigger_source == "user":
                add_job_log(
                    job_id,
                    "Skipped post-hydration chain: user-triggered run",
                    step="chain_post_hydration_skipped",
                    data={"trigger_source": trigger_source},
                )
            else:
                chain = schedule_post_hydration_chain(
                    conn,
                    trigger_reason="post_hydration",
                    limit=limit,
                    target_paper_ids=target_ids,
                )
                if chain.get("scheduled_jobs"):
                    chain_id = str(chain.get("chain_id") or "").strip()
                    if chain_id:
                        # Stamp the starter so /api/v1/activity exposes chain
                        # membership on this row too — Activity UI groups
                        # every job sharing a chain_id under one envelope.
                        from alma.api.scheduler import set_job_status

                        set_job_status(
                            job_id,
                            chain_id=chain_id,
                            chain_step="hydrate",
                        )
                    add_job_log(
                        job_id,
                        "Chained S2 vector backfill auto-scheduled",
                        step="chain_post_hydration",
                        data=chain,
                    )
        except Exception as exc:
            logger.debug("post-hydration chain skipped: %s", exc)

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
    "influential_citation_count",
    "tldr",
    "oa_url",
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


def _apply_s2_paper(
    conn: sqlite3.Connection,
    *,
    paper_id: str,
    row: sqlite3.Row,
    paper: dict,
) -> list[str]:
    """Fill-only paper UPDATE from a Semantic Scholar paper response.

    Wraps `services/s2_vectors._apply_s2_metadata` (which fills the
    canonical bibliographic fields) and ALSO persists `tldr` and
    `influential_citation_count` plus the free SPECTER2 vector from the
    same response — those are S2-only signals that the user asked us to
    capture everywhere we can ("enrich all possible").
    """
    from alma.ai.embedding_sources import EMBEDDING_SOURCE_SEMANTIC_SCHOLAR
    from alma.discovery import semantic_scholar
    from alma.services.s2_vectors import (
        _apply_s2_metadata as _s2_apply,
        _clear_fetch_status,
    )

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
        from alma.core.paper_updates import fill_only_update_paper

        fill_only_update_paper(
            conn,
            paper_id,
            fill_fields={"tldr": tldr_text},
            max_int_fields={"influential_citation_count": influential_count},
        )
    vector_stored = False
    vector = semantic_scholar.extract_specter2_vector(paper)
    if vector:
        try:
            vector_stored = semantic_scholar.upsert_specter2_vector(
                conn,
                paper_id,
                vector,
                source=EMBEDDING_SOURCE_SEMANTIC_SCHOLAR,
                created_at=_utcnow_iso(),
            )
            _clear_fetch_status(
                conn,
                paper_id=paper_id,
                model=semantic_scholar.S2_SPECTER2_MODEL,
            )
        except Exception as exc:
            logger.warning(
                "S2 vector store failed during metadata phase for %s: %s",
                paper_id,
                exc,
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
        "influential_citation_count",
        "tldr",
    ):
        if str(before[field] or "") != str(after[field] or ""):
            filled.append(field)
    if vector_stored:
        filled.append("specter2_vector")
    return filled


def _apply_crossref_candidate(
    conn: sqlite3.Connection, *, paper_id: str, candidate: dict
) -> list[str]:
    """Fill-only paper UPDATE from a Crossref candidate dict."""
    from alma.core.paper_updates import fill_only_update_paper
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

    return fill_only_update_paper(
        conn,
        paper_id,
        fill_fields={
            "abstract": abstract,
            "journal": journal,
            "publication_date": publication_date,
            "url": url,
            "doi": doi,
        },
        fill_null_fields={"year": year},
        max_int_fields={"cited_by_count": cited_by_count},
    )


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


def _has_any_identifier(row: sqlite3.Row) -> bool:
    """True iff the paper carries any identifier the source helpers can use."""
    return bool(
        str(row["openalex_id"] or "").strip()
        or str(row["doi"] or "").strip()
        or str(row["semantic_scholar_id"] or "").strip()
    )


def _resolve_identifiers_via_title(
    conn: sqlite3.Connection, paper_id: str, row: sqlite3.Row
) -> tuple[str, list[str]]:
    """Phase 0 of `hydrate_paper_metadata`: acquire an identifier when
    a title-only paper has none.

    Calls **OpenAlex** `/works?search=...` first because:

    - OpenAlex's polite pool (mailto-tagged, configured) gives us
      ~10 RPS with consistent latency and a 100 K/day soft cap per
      user — orders of magnitude more headroom than S2.
    - Semantic Scholar's `/paper/search` is throttled to 1 RPS even
      with an API key and shares an anonymous pool of 5 000 req /
      5 min. Hitting it first burns a budget we want to spend on the
      title-search rescue inside the vector backfill.

    Falls back to S2 only when OpenAlex returned nothing acceptable.
    The S2 search response carries `embedding.specter_v2` when present,
    but that vector is also recoverable via the resolved DOI / s2_id
    on the next vector-backfill pass — so calling S2 here isn't on
    the critical path.

    Accepts the top result iff title token Jaccard >= 0.92 AND
    |year_delta| <= 1 — same threshold as the title-search rescue
    inside `run_s2_vector_backfill` so a paper has one match contract
    end-to-end.

    On accept, fill-only writes any of `semantic_scholar_id` / `doi` /
    `openalex_id` the candidate offers; the existing OpenAlex / S2 /
    Crossref helpers downstream then run with the resolved id and pull
    full metadata. Records the outcome under `TITLE_RESOLUTION_SOURCE`
    so reruns skip resolved or terminal-no-match papers.

    Returns ``("skipped" | "enriched" | "unchanged" | "terminal_no_match" | "retryable_error", filled)``.
    """
    if _has_any_identifier(row):
        return ("skipped", [])
    title = str(row["title"] or "").strip()
    if not title:
        return ("skipped", [])

    from alma.core.utils import normalize_title_key as _norm_title_key

    # Skip if a recent terminal_no_match already exhausted this title.
    # The `unchanged` TTL applies to "we found a candidate but it gave
    # us no new fields"; for `terminal_no_match` we leave it sticky
    # until the title itself changes (handled below via `lookup_key`).
    lookup_key = f"title:{_norm_title_key(title)}"
    if not lookup_key.endswith(":") and len(lookup_key) > len("title:"):
        existing = conn.execute(
            """
            SELECT status, lookup_key
            FROM paper_enrichment_status
            WHERE paper_id = ? AND source = ? AND purpose = ?
            """,
            (paper_id, TITLE_RESOLUTION_SOURCE, METADATA_PURPOSE),
        ).fetchone()
        if existing and str(existing["lookup_key"]) == lookup_key:
            if str(existing["status"]) in TERMINAL_STATUSES:
                return (str(existing["status"]), [])

    try:
        local_year = int(row["year"]) if row["year"] is not None else None
    except (TypeError, ValueError):
        local_year = None

    from alma.services.title_resolution import (
        TITLE_RESOLUTION_MAX_RESULTS,
        TITLE_RESOLUTION_QUERY_MAX_CHARS,
        _accept_match,
    )

    query_text = title[:TITLE_RESOLUTION_QUERY_MAX_CHARS]

    def _accepts(cand_title: str, cand_year: int | None) -> tuple[bool, float]:
        # Thin closure over the canonical helper so the per-paper
        # local_title / local_year don't have to be threaded through
        # every call site below.
        return _accept_match(title, local_year, cand_title or "", cand_year)

    fields_filled: list[str] = []
    matched_via: str | None = None

    # 1) OpenAlex first. Polite pool (mailto-tagged, see
    #    `core.http_sources._POLICIES["openalex"]`) gives the largest
    #    rate budget and the most consistent latency, and OpenAlex
    #    indexes a much larger long tail than S2 for non-STEM venues.
    try:
        from alma.library.enrichment import _search_work_candidates

        oa_candidates = _search_work_candidates(
            query_text, per_page=TITLE_RESOLUTION_MAX_RESULTS
        )
    except Exception as exc:
        logger.debug(
            "title-resolution: OpenAlex search failed for %s: %s", paper_id, exc
        )
        oa_candidates = []

    best_oa = None
    best_oa_score = 0.0
    for cand in oa_candidates:
        cand_title = str(cand.get("display_name") or "").strip()
        try:
            cand_year = (
                int(cand.get("publication_year"))
                if cand.get("publication_year") is not None
                else None
            )
        except (TypeError, ValueError):
            cand_year = None
        accept, score = _accepts(cand_title, cand_year)
        if accept and score > best_oa_score:
            best_oa = cand
            best_oa_score = score

    if best_oa is not None:
        from alma.core.paper_updates import fill_only_update_paper
        from alma.core.utils import canonical_lookup_doi as _canonical

        oa_id_raw = str(best_oa.get("id") or "").strip()
        oa_id = _normalize_openalex_work_id(oa_id_raw) if oa_id_raw else ""
        new_doi = _canonical(str(best_oa.get("doi") or "")) or ""
        changed = fill_only_update_paper(
            conn,
            paper_id,
            fill_fields={
                **({"openalex_id": oa_id} if oa_id else {}),
                **({"doi": new_doi} if new_doi else {}),
            },
        )
        for field in changed:
            if field in ("openalex_id", "doi"):
                fields_filled.append(field)
        matched_via = f"oa_search:{best_oa_score:.2f}"

    # 2) Semantic Scholar — fallback only when OpenAlex didn't resolve.
    #    Skipping when OpenAlex already handed us identifiers saves a
    #    1-RPS budget call we'd rather spend on the title-search rescue
    #    inside the vector backfill (which has no OpenAlex equivalent).
    s2_rate_limited = False
    if not _has_any_identifier(
        conn.execute(
            "SELECT openalex_id, doi, semantic_scholar_id FROM papers WHERE id = ?",
            (paper_id,),
        ).fetchone()
    ):
        try:
            from alma.discovery import semantic_scholar

            s2_candidates = semantic_scholar.search_papers(
                query_text,
                limit=TITLE_RESOLUTION_MAX_RESULTS,
                raise_on_rate_limit=True,
            )
        except semantic_scholar.SemanticScholarBatchError as exc:
            if getattr(exc, "status_code", None) == 429:
                s2_rate_limited = True
                s2_candidates = []
            else:
                logger.debug(
                    "title-resolution: S2 search error for %s: %s", paper_id, exc
                )
                s2_candidates = []
        except Exception as exc:
            logger.debug(
                "title-resolution: S2 search failed for %s: %s", paper_id, exc
            )
            s2_candidates = []

        best_s2 = None
        best_s2_score = 0.0
        for cand in s2_candidates:
            cand_title = str(cand.get("title") or "").strip()
            try:
                cand_year_raw = cand.get("year")
                cand_year = (
                    int(cand_year_raw) if cand_year_raw is not None else None
                )
            except (TypeError, ValueError):
                cand_year = None
            accept, score = _accepts(cand_title, cand_year)
            if accept and score > best_s2_score:
                best_s2 = cand
                best_s2_score = score

        if best_s2 is not None:
            from alma.core.paper_updates import fill_only_update_paper
            from alma.core.utils import canonical_lookup_doi as _canonical

            new_s2_id = str(best_s2.get("semantic_scholar_id") or "").strip()
            new_doi = _canonical(str(best_s2.get("doi") or "")) or ""
            if new_s2_id or new_doi:
                changed = fill_only_update_paper(
                    conn,
                    paper_id,
                    fill_fields={
                        **({"semantic_scholar_id": new_s2_id} if new_s2_id else {}),
                        **({"doi": new_doi} if new_doi else {}),
                    },
                )
                for field in changed:
                    if field in ("semantic_scholar_id", "doi") and field not in fields_filled:
                        fields_filled.append(field)
                matched_via = (
                    matched_via + "+s2_search"
                    if matched_via
                    else f"s2_search:{best_s2_score:.2f}"
                )

    if fields_filled:
        status_value = "enriched"
        reason = f"title_match:{matched_via}" if matched_via else "title_match"
        retry: timedelta | None = None
    elif matched_via:
        # Found a candidate but neither side gave us a new identifier
        # the local row didn't already have. Keep retryable so a future
        # change in upstream coverage can fill in.
        status_value = "unchanged"
        reason = "title_match_no_new_ids"
        retry = UNCHANGED_RETRY_AFTER
    elif s2_rate_limited:
        status_value = RETRYABLE_STATUS
        reason = "s2_rate_limited"
        retry = timedelta(minutes=10)
    else:
        status_value = "terminal_no_match"
        reason = "title_search_no_candidates_above_threshold"
        retry = None
    _write_ledger(
        conn,
        paper_id=paper_id,
        source=TITLE_RESOLUTION_SOURCE,
        lookup_key=lookup_key,
        status=status_value,
        reason=reason,
        fields_filled=fields_filled,
        fields_key="title_resolution_v1",
        retry_after=retry,
    )
    return (status_value, fields_filled)


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
        fetched = semantic_scholar.fetch_papers_batch(
            lookup_ids,
            batch_size=len(lookup_ids),
            raise_on_error=True,
        )
    except semantic_scholar.SemanticScholarBatchError as exc:
        status_code = getattr(exc, "status_code", None)
        retryable = (
            status_code is None
            or status_code in {401, 403, 408, 425, 429}
            or (status_code is not None and status_code >= 500)
        )
        if not retryable:
            _write_ledger(
                conn, paper_id=paper_id, source=S2_SOURCE, lookup_key=lookup_key,
                status="terminal_no_match", reason=str(exc), fields_filled=[],
                fields_key="s2_paper_v1",
            )
            return ("terminal_no_match", [])
        _write_ledger(
            conn, paper_id=paper_id, source=S2_SOURCE, lookup_key=lookup_key,
            status=RETRYABLE_STATUS, reason=str(exc), fields_filled=[],
            fields_key="s2_paper_v1", retry_after=timedelta(hours=6),
        )
        return (RETRYABLE_STATUS, [])
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


def _hydrate_via_abstract_recovery(
    conn: sqlite3.Connection, paper_id: str, row: sqlite3.Row
) -> tuple[str, list[str]]:
    """Recover an abstract from public OA / publisher metadata.

    Safe default only: fetch known landing pages, parse standard
    metadata tags, and ask Unpaywall for OA locations when the app has
    a contact email. Scholar, Sci-Hub, and PDF parsing remain policy /
    dependency decisions outside this default path.
    """

    from alma.core.paper_updates import fill_only_update_paper
    from alma.core.utils import canonical_lookup_doi

    if _abstract_present(row):
        return ("skipped", [])

    urls: list[str] = []
    for field in ("oa_url", "url"):
        value = str(row[field] or "").strip()
        if value and value not in urls:
            urls.append(value)

    doi = canonical_lookup_doi(str(row["doi"] or "")) or ""
    if doi:
        for value in _unpaywall_urls_for_doi(doi):
            if value and value not in urls:
                urls.append(value)

    lookup_key = "|".join(urls[:3]) or (f"doi:{doi}" if doi else "")
    fields_filled: list[str] = []
    reason = "no_candidate_urls"
    for url in urls:
        if url.lower().endswith(".pdf"):
            continue
        abstract = _fetch_html_abstract(url)
        if not abstract:
            reason = "no_html_meta_abstract"
            continue
        fields_filled = fill_only_update_paper(
            conn,
            paper_id,
            fill_fields={"abstract": abstract},
        )
        reason = "html_meta_abstract"
        break

    if fields_filled:
        status_value = "enriched"
        retry: timedelta | None = None
    else:
        status_value = "unchanged"
        retry = UNCHANGED_RETRY_AFTER

    _write_ledger(
        conn,
        paper_id=paper_id,
        source=ABSTRACT_RECOVERY_SOURCE,
        lookup_key=lookup_key,
        status=status_value,
        reason=reason,
        fields_filled=fields_filled,
        fields_key="abstract_recovery_html_meta_v1",
        retry_after=retry,
    )
    return (status_value, fields_filled)


def hydrate_paper_metadata(
    conn: sqlite3.Connection,
    paper_id: str,
    *,
    sources: tuple[str, ...] = (
        OPENALEX_SOURCE,
        S2_SOURCE,
        CROSSREF_SOURCE,
        ABSTRACT_RECOVERY_SOURCE,
    ),
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
    unless the source-specific lookup key has changed.
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

    import time as _time

    summary: dict[str, Any] = {
        "paper_id": paper_id,
        "exists": True,
        "sources_attempted": [],
        "sources_filled": [],
        "fields_by_source": {},
        "abstract_filled": False,
        "abstract_already_present": _abstract_present(row),
        # Per-source wall-clock seconds — phase 7 of
        # `tasks/13_END_TO_END_HYDRATION_VECTOR_CHAIN.md`. Empty for any
        # source whose helper returned `skipped` (so a downstream
        # profiler can tell "didn't run" from "ran in 0 seconds").
        "wall_seconds_by_source": {},
    }

    # Phase 0: identifier resolution by title. Runs before the source
    # loop so a title-only paper acquires an openalex_id / DOI / s2_id
    # the source helpers can then use. Cheap no-op when the row already
    # carries any identifier.
    t0 = _time.perf_counter()
    pre_status, pre_filled = _resolve_identifiers_via_title(conn, paper_id, row)
    if pre_status not in ("skipped",):
        summary["wall_seconds_by_source"][TITLE_RESOLUTION_SOURCE] = round(
            _time.perf_counter() - t0, 4
        )
        summary["sources_attempted"].append(TITLE_RESOLUTION_SOURCE)
        if pre_filled:
            summary["sources_filled"].append(TITLE_RESOLUTION_SOURCE)
            summary["fields_by_source"][TITLE_RESOLUTION_SOURCE] = pre_filled
            # Re-read the row so the downstream loop sees the freshly
            # resolved identifiers.
            row = _read_hydration_row(conn, paper_id) or row

    for source in sources:
        # Re-read row each iteration so a downstream source sees the
        # fields a prior source already filled (avoids redundant writes
        # and lets the ledger note "no_local_improvements" correctly).
        row = _read_hydration_row(conn, paper_id) or row
        t_src = _time.perf_counter()
        if source == OPENALEX_SOURCE:
            status, filled = _hydrate_via_openalex(conn, paper_id, row)
        elif source == S2_SOURCE:
            status, filled = _hydrate_via_s2(conn, paper_id, row)
        elif source == CROSSREF_SOURCE:
            status, filled = _hydrate_via_crossref(conn, paper_id, row)
        elif source == ABSTRACT_RECOVERY_SOURCE:
            status, filled = _hydrate_via_abstract_recovery(conn, paper_id, row)
        else:
            continue
        if status == "skipped":
            continue
        summary["sources_attempted"].append(source)
        summary["wall_seconds_by_source"][source] = round(
            _time.perf_counter() - t_src, 4
        )
        if filled:
            summary["sources_filled"].append(source)
            summary["fields_by_source"][source] = filled

    final_row = _read_hydration_row(conn, paper_id)
    summary["abstract_filled"] = _abstract_present(final_row) and not summary["abstract_already_present"]
    summary["wall_seconds_total"] = round(
        sum(summary["wall_seconds_by_source"].values()), 4
    )
    return summary


def schedule_pending_hydration_sweep(
    *,
    reason: str = "paper_insert",
    limit: int | None = None,
    target_paper_ids: list[str] | tuple[str, ...] | None = None,
) -> str | None:
    """Kick off an Activity-enveloped rehydration job in the background.

    Idempotent: if a job is already active for this operation key,
    returns its job_id without queueing a new one. Otherwise queues
    a fresh ``paper_metadata_rehydrate_*`` job through the canonical
    Activity envelope (:func:`core.job_envelope.schedule_with_envelope`)
    so on-add hydration shows up in the Activity tab and cancels
    cleanly.

    Returns the active or newly-queued job_id; returns None if the
    scheduler isn't importable (e.g., during tests or CLI tools).
    """
    from alma.core.job_envelope import schedule_with_envelope

    target_ids = normalize_id_list(target_paper_ids)
    if limit is None and reason == "paper_insert":
        target_limit = len(target_ids) or _AUTO_PAPER_INSERT_HYDRATION_LIMIT
        limit = max(1, min(_AUTO_PAPER_INSERT_HYDRATION_LIMIT, target_limit))
    bounded_limit = None if limit is None else max(1, min(int(limit), 100_000))
    if target_ids:
        queued_message = (
            "Paper metadata rehydration auto-queued for "
            f"{len(target_ids)} target paper(s)"
        )
    elif bounded_limit is not None:
        queued_message = (
            f"OpenAlex metadata rehydration auto-queued for up to {bounded_limit} paper(s)"
        )
    else:
        queued_message = "OpenAlex metadata rehydration auto-queued for all eligible papers"

    def _runner_factory(job_id: str) -> Callable[[], dict[str, Any]]:
        from alma.api.scheduler import (
            add_job_log,
            is_cancellation_requested,
            set_job_status,
        )

        def _runner() -> dict[str, Any]:
            return run_corpus_metadata_rehydration(
                job_id,
                limit=bounded_limit,
                force=False,
                target_paper_ids=target_ids,
                set_job_status=set_job_status,
                add_job_log=add_job_log,
                is_cancellation_requested=is_cancellation_requested,
            )

        return _runner

    return schedule_with_envelope(
        operation_key="papers.rehydrate_metadata:openalex:metadata",
        job_id_prefix="paper_metadata_rehydrate",
        trigger_source=f"auto:{reason}",
        queued_message=queued_message,
        runner_factory=_runner_factory,
        log_message="Auto-queued by paper-insert hook",
        log_data={
            "limit": bounded_limit,
            "all_eligible": bounded_limit is None,
            "trigger_reason": reason,
            "target_paper_ids": target_ids,
            "target_count": len(target_ids),
        },
        extra_status_fields={
            "started_at": _utcnow_iso(),
            "processed": 0,
            "total": len(target_ids) or bounded_limit or 0,
        },
    )


def enqueue_pending_hydration(
    conn: sqlite3.Connection,
    paper_id: str,
    *,
    auto_schedule: bool = True,
) -> bool:
    """Mark a paper as needing hydration without fetching synchronously.

    Used by the canonical paper-insert sites (Library, Feed, Discovery)
    so a freshly-added row enters the rehydration/vector chain on the
    next sweep instead of waiting for the user to click a Settings
    button. Cheap: one INSERT OR IGNORE per relevant source the paper
    has, no HTTP, no OpenAlex projection check.

    When `auto_schedule=True` (the default) and at least one new pending
    ledger row was written, also schedules a background rehydration job
    through the same Activity envelope as the user-facing trigger —
    async, observable, idempotent against an already-running sweep.
    """
    _ensure_enrichment_status_table(conn)
    row = conn.execute(
        "SELECT id, openalex_id, doi, semantic_scholar_id, title, abstract FROM papers WHERE id = ?",
        (paper_id,),
    ).fetchone()
    if row is None:
        return False
    has_oa = bool(str(row["openalex_id"] or "").strip())
    has_doi = bool(str(row["doi"] or "").strip())
    has_s2 = bool(str(row["semantic_scholar_id"] or "").strip())
    has_title = bool(str(row["title"] or "").strip())
    needs_abstract = not bool(str(row["abstract"] or "").strip())
    # Phase 5 of `tasks/13_END_TO_END_HYDRATION_VECTOR_CHAIN.md`:
    # title-only papers (no DOI / openalex_id / s2_id but a usable
    # title) also enter the pool — `_resolve_identifiers_via_title`
    # will try to acquire an identifier on the next sweep, and the
    # downstream OpenAlex / S2 / Crossref helpers run with whatever it
    # finds. A paper with neither identifier nor title has nothing for
    # the rehydrator to grip; skip it.
    if not (has_oa or has_doi or has_s2 or has_title):
        return False
    queued = False
    now = _utcnow_iso()
    sources_to_queue: list[tuple[str, str, str]] = []
    if not (has_oa or has_doi or has_s2) and has_title:
        # Title-only paper: queue under TITLE_RESOLUTION_SOURCE so the
        # sweep's per-paper dispatcher knows to try title resolution
        # first. The downstream sources will be queued automatically
        # once resolution lands an identifier.
        sources_to_queue.append((
            TITLE_RESOLUTION_SOURCE,
            "title_resolution_v1",
            f"title:{normalize_title_key(str(row['title'] or ''))}",
        ))
    if has_oa:
        sources_to_queue.append((
            OPENALEX_SOURCE,
            OPENALEX_WORKS_FIELDS_KEY,
            openalex_lookup_key(str(row["openalex_id"] or "")),
        ))
    if has_s2 or has_doi:
        sources_to_queue.append((
            S2_SOURCE,
            "s2_paper_v1",
            _s2_lookup_key_for_values(
                str(row["semantic_scholar_id"] or ""),
                str(row["doi"] or ""),
            ),
        ))
    if has_doi and needs_abstract:
        sources_to_queue.append((
            CROSSREF_SOURCE,
            "crossref_v1",
            _crossref_lookup_key(str(row["doi"] or "")),
        ))
    for source, fields_key, lookup_key in sources_to_queue:
        existing = conn.execute(
            """
            SELECT status, lookup_key, fields_key
            FROM paper_enrichment_status
            WHERE paper_id = ? AND source = ? AND purpose = ?
            """,
            (paper_id, source, METADATA_PURPOSE),
        ).fetchone()
        if (
            existing is not None
            and str(existing["status"] or "") in {"enriched", "terminal_no_match"}
            and str(existing["lookup_key"] or "") == lookup_key
            and str(existing["fields_key"] or "") == fields_key
        ):
            continue
        conn.execute(
            """
            INSERT INTO paper_enrichment_status (
                paper_id, source, purpose, lookup_key, fields_key, status, reason,
                fields_requested_json, fields_filled_json, attempts,
                last_attempt_at, next_retry_at, updated_at
            )
            VALUES (?, ?, ?, ?, ?, ?, NULL, '[]', '[]', 0, NULL, NULL, ?)
            ON CONFLICT(paper_id, source, purpose) DO UPDATE SET
                lookup_key = excluded.lookup_key,
                fields_key = excluded.fields_key,
                status = CASE
                    WHEN paper_enrichment_status.lookup_key = excluded.lookup_key
                     AND paper_enrichment_status.fields_key = excluded.fields_key
                     AND paper_enrichment_status.status IN ('enriched', 'terminal_no_match')
                        THEN paper_enrichment_status.status
                    ELSE excluded.status
                END,
                reason = excluded.reason,
                fields_requested_json = excluded.fields_requested_json,
                fields_filled_json = excluded.fields_filled_json,
                attempts = excluded.attempts,
                last_attempt_at = excluded.last_attempt_at,
                next_retry_at = excluded.next_retry_at,
                updated_at = excluded.updated_at
            """,
            (paper_id, source, METADATA_PURPOSE, lookup_key, fields_key, PENDING_STATUS, now),
        )
        queued = True
    if queued and auto_schedule:
        # Fire-and-forget: idempotent against an already-active job. The
        # scheduler import is lazy so this helper stays callable from
        # CLI / test contexts where `alma.api.scheduler` isn't wired.
        try:
            schedule_pending_hydration_sweep(
                reason="paper_insert",
                target_paper_ids=[paper_id],
            )
        except Exception as exc:
            logger.debug("auto schedule_pending_hydration_sweep skipped: %s", exc)
    return queued
