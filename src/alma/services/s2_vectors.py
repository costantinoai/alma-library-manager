"""Semantic Scholar SPECTER2 vector ingestion.

This module is the *vector* leg of the embedding chain. Its sole
external trigger is ``POST /paper/batch`` against Semantic Scholar to
retrieve SPECTER2 embeddings for papers that already carry a usable
``semantic_scholar_id`` or DOI. Metadata (``abstract``, ``doi``,
``year``, etc.) that arrives in the same batch response is persisted
opportunistically as a fill-only side-effect — free data we already
paid for, never used to overwrite a curated value.

Identity resolution (title-search rescue for papers without a usable
ID) is *not* this job's concern: it lives in
``alma.services.title_resolution`` and runs on its own cadence. A
paper that S2 cannot match here gets stamped ``unmatched`` /
``bad_local_doi`` and is left for the title-resolution sweep to
unblock; once that sweep writes a new identity, the trigger
``papers_clear_fetch_status_on_id_change`` drops the terminal status
and the next vector sweep picks the paper up cleanly.
"""

from __future__ import annotations

import logging
import json
import sqlite3
import time
from datetime import datetime
from typing import Callable

from alma.ai.embedding_sources import EMBEDDING_SOURCE_SEMANTIC_SCHOLAR
from alma.core.paper_updates import fill_only_update_paper
from alma.core.utils import canonical_lookup_doi, normalize_doi, validate_doi_shape
from alma.core.vector_blob import encode_vector
from alma.discovery import semantic_scholar

logger = logging.getLogger(__name__)

FETCH_SOURCE = EMBEDDING_SOURCE_SEMANTIC_SCHOLAR
# `bad_local_doi` is also terminal: it means the local DOI fails the
# registry-shape regex, so re-sending the same string produces the same
# 400. The status clears via `clear_terminal_fetch_status_for_paper`
# when the DOI is rewritten by hydration.
TERMINAL_FETCH_STATUSES = {
    "unmatched",
    "missing_vector",
    "lookup_error",
    "bad_local_doi",
}


def _doi_from_s2(row: dict) -> str:
    """Return the canonical-lookup DOI from an S2 paper's externalIds."""
    external = row.get("externalIds") or {}
    return canonical_lookup_doi(str(external.get("DOI") or "")) or ""


def _lookup_ids_for_values(semantic_scholar_id: str, doi: str) -> list[str]:
    """Return S2 Graph API lookup ids for one identifier pair.

    DOIs are emitted via `canonical_lookup_doi` (lowercased / decoded)
    so the `_requested_id` round-trip on the response side compares
    apples to apples regardless of the original case in `papers.doi`.
    Malformed DOIs (failing `validate_doi_shape`) are intentionally
    dropped here — the caller marks them `bad_local_doi` instead of
    emitting a guaranteed-to-fail HTTP request.
    """
    out: list[str] = []
    s2_id = str(semantic_scholar_id or "").strip()
    doi = canonical_lookup_doi(str(doi or "")) or ""
    if s2_id:
        out.append(s2_id)
    if doi and validate_doi_shape(doi):
        out.append(f"DOI:{doi}")
    return list(dict.fromkeys(out))


def _lookup_ids_for_row(row: sqlite3.Row) -> list[str]:
    """Return S2 Graph API lookup ids for one local paper."""
    return _lookup_ids_for_values(
        str(row["semantic_scholar_id"] or ""),
        str(row["doi"] or ""),
    )


def _lookup_key_for_values(semantic_scholar_id: str, doi: str) -> str:
    """Stable per-paper lookup key for `publication_embedding_fetch_status`.

    Uses raw `lower(trim(...))` to match the SQL JOIN expression in the
    SELECT (which can't call Python helpers). External-API canonicalization
    (URL-decode, fragment strip) happens in `canonical_lookup_doi`,
    which is *not* applied here — the small mismatch only affects DOIs
    that carry trailing fragments, which are rare and self-heal on the
    next sweep when the upsert overwrites the prior key.
    """
    s2_id = str(semantic_scholar_id or "").strip().lower()
    doi_value = str(doi or "").strip().lower()
    return f"{s2_id}|{doi_value}"


def _lookup_key_for_row(row: sqlite3.Row) -> str:
    return _lookup_key_for_values(
        str(row["semantic_scholar_id"] or ""),
        str(row["doi"] or ""),
    )


def _lookup_status_for_s2_paper(row: sqlite3.Row, paper: dict, lookup_ids: list[str]) -> tuple[str, list[str]]:
    """Return the post-fetch lookup key and lookup IDs for status writes."""
    s2_id = str(row["semantic_scholar_id"] or "").strip() or str(paper.get("paperId") or "").strip()
    doi = str(row["doi"] or "").strip() or _doi_from_s2(paper)
    effective_lookup_ids = list(dict.fromkeys(lookup_ids + _lookup_ids_for_values(s2_id, doi)))
    return _lookup_key_for_values(s2_id, doi), effective_lookup_ids


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
    lookup_key: str | None = None,
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
            lookup_key or _lookup_key_for_row(row),
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


def _apply_s2_metadata(conn: sqlite3.Connection, *, paper_id: str, row: sqlite3.Row, paper: dict) -> None:
    """Fill local paper metadata from a successful S2 paper response."""
    fetched_s2_id = str(paper.get("paperId") or "").strip()
    corpus_id = str(paper.get("corpusId") or "").strip()
    doi = _doi_from_s2(paper)
    abstract = str(paper.get("abstract") or "").strip()
    url = str(paper.get("url") or "").strip()
    publication_date = str(paper.get("publicationDate") or "").strip()
    try:
        year = int(paper.get("year")) if paper.get("year") is not None else None
    except (TypeError, ValueError):
        year = None
    try:
        citation_count = int(paper.get("citationCount") or 0)
    except (TypeError, ValueError):
        citation_count = 0

    source_id = doi or url or str(row["title"] or "").strip()
    fill_only_update_paper(
        conn,
        paper_id,
        fill_fields={
            "semantic_scholar_id": fetched_s2_id,
            "semantic_scholar_corpus_id": corpus_id,
            "doi": doi,
            "abstract": abstract,
            "url": url,
            "publication_date": publication_date,
            "source_id": source_id,
        },
        fill_null_fields={"year": year},
        max_int_fields={"cited_by_count": citation_count},
        always_fields={"fetched_at": datetime.utcnow().isoformat()},
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
            SELECT p.id, p.title, p.year, p.doi, p.semantic_scholar_id
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
            AND COALESCE(fs.status, '') NOT IN ('unmatched', 'missing_vector', 'lookup_error', 'bad_local_doi')
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
        # `with_doi` reports DOIs that survived shape validation — i.e.
        # the count we'll actually send to S2. Pre-validation totals are
        # available via `with_doi_raw` for diagnostic comparison.
        validated_doi = sum(
            1 for row in rows if validate_doi_shape(str(row["doi"] or ""))
        )
        raw_doi = sum(
            1 for row in rows if normalize_doi(str(row["doi"] or ""))
        )
        add_job_log(
            job_id,
            "Prepared S2/SPECTER2 vector lookup",
            step="prepare",
            data={
                "papers": total,
                "lookup_ids": len(lookup_ids),
                "with_semantic_scholar_id": sum(1 for row in rows if str(row["semantic_scholar_id"] or "").strip()),
                "with_doi": validated_doi,
                "with_doi_raw": raw_doi,
                "with_bad_local_doi": raw_doi - validated_doi,
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
        bad_local_doi = 0
        # Bumped from 50 → 250 (2026-05-08). The S2 `/paper/batch`
        # endpoint accepts up to 500 IDs per call (see
        # `semantic_scholar.fetch_papers_batch` cap). At 2 lookup IDs
        # per paper (s2_id + DOI), 250 papers fits comfortably under
        # that. Drops a 4 909-paper queue from ~99 batches to ~20 and
        # cuts wall-clock from ~5–8 min to ~2–3 min, which makes the
        # in-process worker far less likely to be killed mid-flight by
        # uvicorn `--reload` or a container restart.
        chunk_size = 250

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

            batch_bad_local_doi_before = bad_local_doi
            for row in batch_rows:
                paper_id = str(row["id"])
                s2_id = str(row["semantic_scholar_id"] or "").strip()
                # Match on the canonical-lookup DOI form so the
                # response-side keys (which were also built from
                # `canonical_lookup_doi`) round-trip cleanly. The local
                # `papers.doi` may be mixed-case or URL-encoded; that
                # cosmetic difference must not block a match.
                doi = canonical_lookup_doi(str(row["doi"] or "")) or ""
                processed += 1

                # `bad_local_doi`: row has no s2_id and a DOI that
                # fails the registry-shape regex. Sending it would
                # produce a guaranteed-to-fail HTTP 400; the right
                # remediation is fixing the import (DOI typo, fragment
                # not stripped, etc.) — not retrying the same string.
                # The status clears via
                # `clear_terminal_fetch_status_for_paper` when the DOI
                # is rewritten by hydration.
                raw_local_doi = str(row["doi"] or "").strip()
                if (
                    not s2_id
                    and raw_local_doi
                    and not validate_doi_shape(raw_local_doi)
                ):
                    bad_local_doi += 1
                    _upsert_fetch_status(
                        conn,
                        row=row,
                        model=model,
                        status="bad_local_doi",
                        reason=(
                            "Local DOI fails registry-shape regex; "
                            "fix the import or rerun hydration to "
                            "rewrite the DOI before retrying."
                        ),
                        lookup_ids=batch_lookup_ids_by_paper.get(paper_id, []),
                    )
                    continue

                paper = (fetched_by_s2.get(s2_id) if s2_id else None) or (
                    fetched_by_doi.get(doi) if doi else None
                ) or (
                    fetched_by_request.get(f"DOI:{doi}") if doi else None
                ) or (
                    fetched_by_request.get(s2_id) if s2_id else None
                )
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
                    # The title-resolution sweep
                    # (`alma.services.title_resolution`) handles the
                    # `unmatched` backlog on its own cadence; this job
                    # leaves the row stamped and moves on.
                    continue

                lookup_ids_for_paper = batch_lookup_ids_by_paper.get(paper_id, [])
                status_lookup_key, status_lookup_ids = _lookup_status_for_s2_paper(
                    row,
                    paper,
                    lookup_ids_for_paper,
                )
                _apply_s2_metadata(conn, paper_id=paper_id, row=row, paper=paper)

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
                    fill_only_update_paper(
                        conn,
                        paper_id,
                        fill_fields={"tldr": tldr_text} if tldr_text else None,
                        max_int_fields={"influential_citation_count": influential_count}
                        if influential_count > 0
                        else None,
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
                        lookup_ids=status_lookup_ids,
                        lookup_key=status_lookup_key,
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
                        lookup_ids=status_lookup_ids,
                        lookup_key=status_lookup_key,
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
                    "bad_local_doi": bad_local_doi - batch_bad_local_doi_before,
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
                    f"unmatched {unmatched}, bad local DOI {bad_local_doi}, "
                    f"bad lookup {lookup_failures}, lookup errors {errors}"
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
                f"unmatched {unmatched}, bad local DOI {bad_local_doi}, "
                f"bad lookup {lookup_failures}, lookup errors {errors}"
            ),
            result={
                "processed": total,
                "stored": stored,
                "missing_vectors": missing,
                "unmatched": unmatched,
                "bad_local_doi": bad_local_doi,
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
                "bad_local_doi": bad_local_doi,
                "lookup_failures": lookup_failures,
                "errors": errors,
                "remote_fetch_only": True,
                "local_compute": False,
            },
        )

        # Auto-chain to the local SPECTER2 fill only when this run
        # was *not* started by a manual Settings click. The per-button
        # contract there is "do exactly what the label says": clicking
        # "Fetch Missing S2 Vectors" must not silently start a heavy
        # local compute job. Auto-chain still fires for the per-insert
        # / scheduler paths where there's no user click to confuse.
        try:
            from alma.api.scheduler import get_job_trigger_source
            from alma.services.embedding_chain import schedule_post_s2_chain

            trigger_source = get_job_trigger_source(job_id) or ""
            if trigger_source == "user":
                add_job_log(
                    job_id,
                    "Skipped post-S2 chain: user-triggered run",
                    step="chain_post_s2_skipped",
                    data={"trigger_source": trigger_source},
                )
            else:
                chain = schedule_post_s2_chain(conn, trigger_reason="post_s2_fetch")
                if chain.get("scheduled_jobs"):
                    chain_id = str(chain.get("chain_id") or "").strip()
                    if chain_id:
                        from alma.api.scheduler import set_job_status

                        set_job_status(
                            job_id,
                            chain_id=chain_id,
                            chain_step="s2_fetch",
                        )
                    add_job_log(
                        job_id,
                        "Chained local SPECTER2 fill auto-scheduled",
                        step="chain_post_s2",
                        data=chain,
                    )
        except Exception as exc:
            logger.debug("post-S2 chain skipped: %s", exc)
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
