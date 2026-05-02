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
METADATA_PURPOSE = "metadata"
OPENALEX_WORKS_FIELDS = [field.strip() for field in _WORKS_SELECT_FIELDS.split(",") if field.strip()]
OPENALEX_WORKS_FIELDS_KEY = (
    "openalex_works:"
    + hashlib.sha1(_WORKS_SELECT_FIELDS.encode("utf-8")).hexdigest()[:12]
)
TERMINAL_STATUSES = {"enriched", "unchanged", "terminal_no_match"}
RETRYABLE_STATUS = "retryable_error"


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
    return f"""
    (
        es.paper_id IS NULL
        OR COALESCE(es.lookup_key, '') != {lookup_expr}
        OR COALESCE(es.fields_key, '') != ?
        OR COALESCE(es.status, '') IN ('pending', 'queued')
        OR (
            es.status = '{RETRYABLE_STATUS}'
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
    limit: int,
    force: bool = False,
) -> list[sqlite3.Row]:
    _ensure_enrichment_status_table(conn)
    lookup_expr = _openalex_lookup_expr()
    params: list[Any] = [OPENALEX_SOURCE, METADATA_PURPOSE]
    status_clause = _eligible_status_clause(force)
    if not force:
        params.extend([OPENALEX_WORKS_FIELDS_KEY, _utcnow_iso()])
    params.append(max(1, int(limit or 1)))
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
        LIMIT ?
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
                  es.status = '{RETRYABLE_STATUS}'
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
    limit: int = 500,
    force: bool = False,
    set_job_status: Callable[..., None],
    add_job_log: Callable[..., None],
    is_cancellation_requested: Callable[[str], bool],
) -> dict[str, Any]:
    """Rehydrate missing paper metadata from batched OpenAlex work-ID fetches."""
    from alma.api.deps import open_db_connection

    conn = open_db_connection()
    limit = max(1, min(int(limit or 500), 5000))
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
                _upsert_enrichment_status(
                    conn,
                    paper_id=paper_id,
                    lookup_key=lookup_key,
                    status=status_value,
                    reason=reason,
                    fields_filled=fields_filled,
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
            "force": force,
            "message": (
                "OpenAlex metadata rehydration completed: "
                f"enriched={int(summary['enriched'])}, unchanged={int(summary['unchanged'])}, "
                f"retryable_error={int(summary['retryable_error'])}"
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
