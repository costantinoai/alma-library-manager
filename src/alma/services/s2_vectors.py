"""Semantic Scholar SPECTER2 vector ingestion."""

from __future__ import annotations

import logging
import json
import sqlite3
import time
from datetime import datetime
from typing import Callable

from alma.ai.embedding_sources import EMBEDDING_SOURCE_SEMANTIC_SCHOLAR
from alma.core.utils import normalize_doi
from alma.core.vector_blob import encode_vector
from alma.discovery import semantic_scholar

logger = logging.getLogger(__name__)

FETCH_SOURCE = EMBEDDING_SOURCE_SEMANTIC_SCHOLAR
TERMINAL_FETCH_STATUSES = {"unmatched", "missing_vector", "lookup_error"}


def _doi_from_s2(row: dict) -> str:
    external = row.get("externalIds") or {}
    return normalize_doi(str(external.get("DOI") or "")) or ""


def _lookup_ids_for_row(row: sqlite3.Row) -> list[str]:
    """Return S2 Graph API lookup ids for one local paper."""
    out: list[str] = []
    s2_id = str(row["semantic_scholar_id"] or "").strip()
    doi = normalize_doi(str(row["doi"] or "")) or ""
    if s2_id:
        out.append(s2_id)
    if doi:
        out.append(f"DOI:{doi}")
    return list(dict.fromkeys(out))


def _lookup_key_for_row(row: sqlite3.Row) -> str:
    s2_id = str(row["semantic_scholar_id"] or "").strip().lower()
    doi = str(row["doi"] or "").strip().lower()
    return f"{s2_id}|{doi}"


def _ensure_fetch_status_table(conn: sqlite3.Connection) -> None:
    conn.execute(
        """CREATE TABLE IF NOT EXISTS publication_embedding_fetch_status (
            paper_id TEXT NOT NULL REFERENCES papers(id) ON DELETE CASCADE,
            model TEXT NOT NULL,
            source TEXT NOT NULL,
            status TEXT NOT NULL,
            reason TEXT,
            lookup_key TEXT,
            lookup_ids_json TEXT,
            attempts INTEGER NOT NULL DEFAULT 0,
            updated_at TEXT NOT NULL,
            PRIMARY KEY (paper_id, model, source)
        )"""
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_embedding_fetch_status_model "
        "ON publication_embedding_fetch_status(model, source, status)"
    )


def _upsert_fetch_status(
    conn: sqlite3.Connection,
    *,
    row: sqlite3.Row,
    model: str,
    status: str,
    reason: str,
    lookup_ids: list[str],
) -> None:
    conn.execute(
        """
        INSERT INTO publication_embedding_fetch_status
            (paper_id, model, source, status, reason, lookup_key, lookup_ids_json, attempts, updated_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, 1, ?)
        ON CONFLICT(paper_id, model, source) DO UPDATE SET
            status = excluded.status,
            reason = excluded.reason,
            lookup_key = excluded.lookup_key,
            lookup_ids_json = excluded.lookup_ids_json,
            attempts = publication_embedding_fetch_status.attempts + 1,
            updated_at = excluded.updated_at
        """,
        (
            str(row["id"]),
            model,
            FETCH_SOURCE,
            status,
            reason,
            _lookup_key_for_row(row),
            json.dumps(lookup_ids),
            datetime.utcnow().isoformat(),
        ),
    )


def _clear_fetch_status(conn: sqlite3.Connection, *, paper_id: str, model: str) -> None:
    conn.execute(
        """
        DELETE FROM publication_embedding_fetch_status
        WHERE paper_id = ? AND model = ? AND source = ?
        """,
        (paper_id, model, FETCH_SOURCE),
    )


def _fetch_lookup_ids_resilient(
    lookup_ids: list[str],
    *,
    job_id: str,
    add_job_log: Callable[..., None],
    batch_label: str,
    min_retry_size: int = 1,
) -> tuple[dict[str, dict], dict[str, str], dict[str, str]]:
    """Fetch lookup ids and isolate per-id failures.

    Returns fetched papers, terminal lookup errors, and retryable lookup errors.
    Retryable errors must not be recorded as no-match terminal misses.
    """
    deduped = [item for item in dict.fromkeys(lookup_ids) if item]
    if not deduped:
        return {}, {}, {}

    try:
        fetched = semantic_scholar.fetch_papers_batch(
            deduped,
            batch_size=len(deduped),
            raise_on_error=True,
        )
        return fetched, {}, {}
    except semantic_scholar.SemanticScholarBatchError as exc:
        status_code = getattr(exc, "status_code", None)
        retryable = (
            status_code is None
            or status_code in {401, 403, 408, 425, 429}
            or (status_code is not None and status_code >= 500)
        )
        if retryable:
            add_job_log(
                job_id,
                "S2/SPECTER2 vector fetch deferred by upstream service",
                level="WARNING",
                step="retryable_lookup_error",
                data={
                    "batch": batch_label,
                    "lookup_ids": len(deduped),
                    "status_code": status_code,
                    "error": str(exc),
                },
            )
            if status_code == 429 or (status_code is not None and status_code >= 500):
                time.sleep(2)
            return {}, {}, {lookup_id: str(exc) for lookup_id in deduped}
        if len(deduped) <= min_retry_size:
            add_job_log(
                job_id,
                "S2/SPECTER2 lookup id failed",
                level="WARNING",
                step="lookup_error",
                data={"batch": batch_label, "lookup_id": deduped[0], "error": str(exc)},
            )
            return {}, {deduped[0]: str(exc)}, {}

        midpoint = max(1, len(deduped) // 2)
        left, left_terminal, left_retryable = _fetch_lookup_ids_resilient(
            deduped[:midpoint],
            job_id=job_id,
            add_job_log=add_job_log,
            batch_label=f"{batch_label}.a",
            min_retry_size=min_retry_size,
        )
        right, right_terminal, right_retryable = _fetch_lookup_ids_resilient(
            deduped[midpoint:],
            job_id=job_id,
            add_job_log=add_job_log,
            batch_label=f"{batch_label}.b",
            min_retry_size=min_retry_size,
        )
        left.update(right)
        left_terminal.update(right_terminal)
        left_retryable.update(right_retryable)
        return left, left_terminal, left_retryable


def run_s2_vector_backfill(
    job_id: str,
    *,
    limit: int = 200,
    set_job_status: Callable[..., None],
    add_job_log: Callable[..., None],
    is_cancellation_requested: Callable[[str], bool],
) -> None:
    """Fetch API-sourced S2 SPECTER2 vectors for known DOI/S2-backed papers."""
    from alma.api.deps import open_db_connection

    conn = open_db_connection()
    model = semantic_scholar.S2_SPECTER2_MODEL
    try:
        _ensure_fetch_status_table(conn)
        limit = max(1, min(int(limit or 200), 5000))
        # Skip a paper only if it already has an *S2-sourced* vector for
        # this model. Locally-computed vectors (source='local', and any
        # other non-S2 source) are deliberately treated as upgradeable —
        # remote S2 embeddings take priority, so as soon as S2 grows
        # coverage for a paper we re-fetch and overwrite the local fill.
        rows = conn.execute(
            """
            SELECT p.id, p.doi, p.semantic_scholar_id
            FROM papers p
            LEFT JOIN publication_embedding_fetch_status fs
              ON fs.paper_id = p.id
             AND fs.model = ?
             AND fs.source = ?
             AND fs.lookup_key = lower(trim(COALESCE(p.semantic_scholar_id, ''))) || '|' || lower(trim(COALESCE(p.doi, '')))
            WHERE (
                COALESCE(NULLIF(TRIM(p.semantic_scholar_id), ''), '') != ''
                OR COALESCE(NULLIF(TRIM(p.doi), ''), '') != ''
            )
            AND NOT EXISTS (
                SELECT 1 FROM publication_embeddings pe
                WHERE pe.paper_id = p.id
                  AND pe.model = ?
                  AND pe.source = ?
            )
            AND COALESCE(fs.status, '') NOT IN ('unmatched', 'missing_vector', 'lookup_error')
            LIMIT ?
            """,
            (model, FETCH_SOURCE, model, FETCH_SOURCE, limit),
        ).fetchall()

        total = len(rows)
        if total == 0:
            set_job_status(
                job_id,
                status="completed",
                processed=0,
                total=0,
                message="No papers need S2/SPECTER2 vectors",
                finished_at=datetime.utcnow().isoformat(),
            )
            return

        set_job_status(
            job_id,
            status="running",
            processed=0,
            total=total,
            message=f"Fetching remote S2/SPECTER2 vectors for {total} papers",
        )

        paper_lookup_ids = {str(row["id"]): _lookup_ids_for_row(row) for row in rows}
        lookup_ids = [lookup_id for ids in paper_lookup_ids.values() for lookup_id in ids]

        lookup_ids = list(dict.fromkeys(lookup_ids))
        add_job_log(
            job_id,
            "Prepared S2/SPECTER2 vector lookup",
            step="prepare",
            data={
                "papers": total,
                "lookup_ids": len(lookup_ids),
                "with_semantic_scholar_id": sum(1 for row in rows if str(row["semantic_scholar_id"] or "").strip()),
                "with_doi": sum(1 for row in rows if normalize_doi(str(row["doi"] or ""))),
                "remote_fetch_only": True,
                "local_compute": False,
            },
        )

        processed = 0
        stored = 0
        missing = 0
        unmatched = 0
        errors = 0
        lookup_failures = 0
        chunk_size = 50

        for start in range(0, total, chunk_size):
            if is_cancellation_requested(job_id):
                set_job_status(
                    job_id,
                    status="cancelled",
                    processed=processed,
                    total=total,
                    message="S2/SPECTER2 vector fetch cancelled",
                    finished_at=datetime.utcnow().isoformat(),
                )
                return

            batch_rows = rows[start:start + chunk_size]
            batch_lookup_ids_by_paper = {
                str(row["id"]): paper_lookup_ids.get(str(row["id"]), [])
                for row in batch_rows
            }
            batch_ids = [
                lookup_id
                for row in batch_rows
                for lookup_id in paper_lookup_ids.get(str(row["id"]), [])
            ]
            fetched, terminal_lookup_errors, retryable_lookup_errors = _fetch_lookup_ids_resilient(
                batch_ids,
                job_id=job_id,
                add_job_log=add_job_log,
                batch_label=str(start),
            )
            lookup_errors = len(terminal_lookup_errors) + len(retryable_lookup_errors)
            errors += lookup_errors

            batch_stored_before = stored
            batch_missing_before = missing
            batch_unmatched_before = unmatched
            batch_lookup_failures_before = lookup_failures
            batch_inserted_paper_ids: list[str] = []
            fetched_by_s2 = {
                str(paper.get("paperId") or "").strip(): paper
                for paper in fetched.values()
                if str(paper.get("paperId") or "").strip()
            }
            fetched_by_doi = {
                doi: paper
                for paper in fetched.values()
                if (doi := _doi_from_s2(paper))
            }
            fetched_by_request = {
                str(paper.get("_requested_id") or "").strip(): paper
                for paper in fetched.values()
                if str(paper.get("_requested_id") or "").strip()
            }

            for row in batch_rows:
                paper_id = str(row["id"])
                s2_id = str(row["semantic_scholar_id"] or "").strip()
                doi = normalize_doi(str(row["doi"] or "")) or ""
                paper = (fetched_by_s2.get(s2_id) if s2_id else None) or (
                    fetched_by_doi.get(doi) if doi else None
                ) or (
                    fetched_by_request.get(f"DOI:{doi}") if doi else None
                ) or (
                    fetched_by_request.get(s2_id) if s2_id else None
                )
                processed += 1
                if paper is None:
                    lookup_ids_for_paper = batch_lookup_ids_by_paper.get(paper_id, [])
                    retryable_for_paper = {
                        lookup_id: retryable_lookup_errors[lookup_id]
                        for lookup_id in lookup_ids_for_paper
                        if lookup_id in retryable_lookup_errors
                    }
                    terminal_for_paper = {
                        lookup_id: terminal_lookup_errors[lookup_id]
                        for lookup_id in lookup_ids_for_paper
                        if lookup_id in terminal_lookup_errors
                    }
                    if retryable_for_paper:
                        _upsert_fetch_status(
                            conn,
                            row=row,
                            model=model,
                            status="error",
                            reason=(
                                "Semantic Scholar lookup was deferred by a retryable "
                                f"batch/API error: {next(iter(retryable_for_paper.values()))}"
                            ),
                            lookup_ids=lookup_ids_for_paper,
                        )
                        continue
                    if terminal_for_paper:
                        lookup_failures += 1
                        _upsert_fetch_status(
                            conn,
                            row=row,
                            model=model,
                            status="lookup_error",
                            reason=(
                                "Semantic Scholar rejected the current DOI/S2 lookup id: "
                                f"{next(iter(terminal_for_paper.values()))}"
                            ),
                            lookup_ids=lookup_ids_for_paper,
                        )
                        continue
                    unmatched += 1
                    _upsert_fetch_status(
                        conn,
                        row=row,
                        model=model,
                        status="unmatched",
                        reason="Semantic Scholar returned no paper for current DOI/S2 lookup ids",
                        lookup_ids=batch_lookup_ids_by_paper.get(paper_id, []),
                    )
                    continue

                fetched_s2_id = str(paper.get("paperId") or "").strip()
                corpus_id = str(paper.get("corpusId") or "").strip()
                if fetched_s2_id or corpus_id:
                    conn.execute(
                        """
                        UPDATE papers
                        SET semantic_scholar_id = COALESCE(NULLIF(semantic_scholar_id, ''), ?),
                            semantic_scholar_corpus_id = COALESCE(NULLIF(semantic_scholar_corpus_id, ''), ?)
                        WHERE id = ?
                        """,
                        (fetched_s2_id or None, corpus_id or None, paper_id),
                    )

                # T5: piggy-back the SPECTER2 backfill to populate
                # `papers.tldr` + `papers.influential_citation_count`
                # when S2 supplies them. Both are free on this batch
                # call (the FIELDS projection already requests them),
                # so we'd be wasting data by not writing them.
                tldr_obj = paper.get("tldr")
                tldr_text = ""
                if isinstance(tldr_obj, dict):
                    tldr_text = (tldr_obj.get("text") or "").strip()
                try:
                    influential_count = int(paper.get("influentialCitationCount") or 0)
                except (TypeError, ValueError):
                    influential_count = 0
                if tldr_text or influential_count > 0:
                    conn.execute(
                        """
                        UPDATE papers
                        SET tldr = COALESCE(NULLIF(tldr, ''), ?),
                            influential_citation_count = CASE
                                WHEN COALESCE(influential_citation_count, 0) = 0 THEN ?
                                ELSE influential_citation_count
                            END
                        WHERE id = ?
                        """,
                        (tldr_text or None, influential_count, paper_id),
                    )

                vector = semantic_scholar.extract_specter2_vector(paper)
                if not vector:
                    missing += 1
                    _upsert_fetch_status(
                        conn,
                        row=row,
                        model=model,
                        status="missing_vector",
                        reason="Semantic Scholar returned the paper without embedding.specter_v2",
                        lookup_ids=batch_lookup_ids_by_paper.get(paper_id, []),
                    )
                    continue
                try:
                    # Upsert with priority: a non-S2 vector (typically a
                    # locally-computed SPECTER2 fill) gets overwritten by
                    # the remote one; an existing S2 vector is left
                    # alone (the WHERE on the conflict clause makes the
                    # operation a no-op rather than a redundant rewrite).
                    cursor = conn.execute(
                        """
                        INSERT INTO publication_embeddings
                            (paper_id, embedding, model, source, created_at)
                        VALUES (?, ?, ?, ?, ?)
                        ON CONFLICT(paper_id, model) DO UPDATE SET
                            embedding  = excluded.embedding,
                            source     = excluded.source,
                            created_at = excluded.created_at
                        WHERE publication_embeddings.source != ?
                        """,
                        (
                            paper_id,
                            encode_vector(vector),
                            model,
                            EMBEDDING_SOURCE_SEMANTIC_SCHOLAR,
                            datetime.utcnow().isoformat(),
                            EMBEDDING_SOURCE_SEMANTIC_SCHOLAR,
                        ),
                    )
                    if cursor.rowcount > 0:
                        stored += 1
                        batch_inserted_paper_ids.append(paper_id)
                    _clear_fetch_status(conn, paper_id=paper_id, model=model)
                except Exception as exc:
                    logger.warning("S2 vector store failed for %s: %s", paper_id, exc)
                    errors += 1
                    _upsert_fetch_status(
                        conn,
                        row=row,
                        model=model,
                        status="error",
                        reason=str(exc),
                        lookup_ids=batch_lookup_ids_by_paper.get(paper_id, []),
                    )
            conn.commit()
            # Keep `author_centroids` coherent with the new embeddings.
            if batch_inserted_paper_ids:
                try:
                    from alma.application.author_backfill import (
                        refresh_centroids_for_papers,
                    )

                    refresh_centroids_for_papers(
                        conn, batch_inserted_paper_ids, model=model
                    )
                    conn.commit()
                except Exception:
                    logger.debug(
                        "author centroid refresh skipped after S2 batch",
                        exc_info=True,
                    )
            add_job_log(
                job_id,
                "S2/SPECTER2 vector batch processed",
                step="batch",
                data={
                    "batch_start": start,
                    "paper_count": len(batch_rows),
                    "lookup_ids": len(batch_ids),
                    "fetched": len(fetched),
                    "stored": stored - batch_stored_before,
                    "missing_vectors": missing - batch_missing_before,
                    "unmatched_papers": unmatched - batch_unmatched_before,
                    "terminal_lookup_failures": lookup_failures - batch_lookup_failures_before,
                    "lookup_errors": lookup_errors,
                    "processed_papers": processed,
                    "total_papers": total,
                    "remote_fetch_only": True,
                    "local_compute": False,
                },
            )
            set_job_status(
                job_id,
                status="running",
                processed=min(total, processed),
                total=total,
                errors=errors,
                message=(
                    f"S2 fetch only: stored {stored}, no vector {missing}, "
                    f"unmatched {unmatched}, bad lookup {lookup_failures}, lookup errors {errors}"
                ),
            )

        set_job_status(
            job_id,
            status="completed",
            processed=total,
            total=total,
            errors=errors,
            message=(
                f"S2 fetch complete: stored {stored}, no vector {missing}, "
                f"unmatched {unmatched}, bad lookup {lookup_failures}, lookup errors {errors}"
            ),
            result={
                "processed": total,
                "stored": stored,
                "missing_vectors": missing,
                "unmatched": unmatched,
                "lookup_failures": lookup_failures,
                "errors": errors,
                "model": model,
            },
            finished_at=datetime.utcnow().isoformat(),
        )
        add_job_log(
            job_id,
            "S2/SPECTER2 vector fetch complete",
            step="summary",
            data={
                "stored": stored,
                "missing_vectors": missing,
                "unmatched": unmatched,
                "lookup_failures": lookup_failures,
                "errors": errors,
                "remote_fetch_only": True,
                "local_compute": False,
            },
        )
    except Exception as exc:
        logger.exception("S2 vector fetch failed: %s", exc)
        set_job_status(
            job_id,
            status="failed",
            message=f"S2/SPECTER2 vector fetch failed: {exc}",
            finished_at=datetime.utcnow().isoformat(),
        )
    finally:
        conn.close()
