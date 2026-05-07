"""Semantic Scholar SPECTER2 vector ingestion."""

from __future__ import annotations

import logging
import json
import re
import sqlite3
import time
from datetime import datetime
from typing import Callable

from alma.ai.embedding_sources import EMBEDDING_SOURCE_SEMANTIC_SCHOLAR
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

    # The S2 vector fetch already requests this metadata. Persist it as a
    # fill-only enrichment so future local compute and source links are not
    # starved by fields we threw away.
    conn.execute(
        """
        UPDATE papers
        SET semantic_scholar_id = COALESCE(NULLIF(semantic_scholar_id, ''), ?),
            semantic_scholar_corpus_id = COALESCE(NULLIF(semantic_scholar_corpus_id, ''), ?),
            doi = CASE WHEN COALESCE(doi, '') = '' AND ? != '' THEN ? ELSE doi END,
            abstract = CASE WHEN COALESCE(abstract, '') = '' AND ? != '' THEN ? ELSE abstract END,
            url = CASE WHEN COALESCE(url, '') = '' AND ? != '' THEN ? ELSE url END,
            publication_date = CASE
                WHEN COALESCE(publication_date, '') = '' AND ? != '' THEN ?
                ELSE publication_date
            END,
            year = COALESCE(year, ?),
            cited_by_count = CASE
                WHEN ? > COALESCE(cited_by_count, 0) THEN ?
                ELSE cited_by_count
            END,
            source_id = COALESCE(NULLIF(source_id, ''), ?, ?, ?),
            fetched_at = ?,
            updated_at = ?
        WHERE id = ?
        """,
        (
            fetched_s2_id or None,
            corpus_id or None,
            doi,
            doi,
            abstract,
            abstract,
            url,
            url,
            publication_date,
            publication_date,
            year,
            citation_count,
            citation_count,
            doi or None,
            url or None,
            str(row["title"] or "").strip() or None,
            datetime.utcnow().isoformat(),
            datetime.utcnow().isoformat(),
            paper_id,
        ),
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


_TITLE_TOKEN_RE = re.compile(r"[a-z0-9]+")
# Threshold for the title-search rescue. Jaccard token-set on lowercased
# alpha-only tokens. Calibrated to be tight enough that a clean
# title-only match almost certainly identifies the same work, while
# leaving room for differing punctuation, articles ("the"/"a"), or
# acronym expansions in the search response. See
# `tasks/13_END_TO_END_HYDRATION_VECTOR_CHAIN.md` open question
# "Title-resolution match threshold".
TITLE_RESCUE_JACCARD_THRESHOLD = 0.92
TITLE_RESCUE_YEAR_DELTA = 1
TITLE_RESCUE_MAX_RESULTS = 3
TITLE_RESCUE_QUERY_MAX_CHARS = 200
# Cap on per-run title searches against S2 (`/paper/search` is the
# tightest endpoint — 1 RPS even with an API key). 50 calls = ~52
# seconds at the floor; bigger backlogs roll over to the next sweep.
# Phase 8 of `tasks/13_END_TO_END_HYDRATION_VECTOR_CHAIN.md`.
TITLE_RESCUE_PER_RUN_BUDGET = 50


def _title_tokens(title: str) -> frozenset[str]:
    """Lowercased alpha-numeric token set for Jaccard comparison."""
    return frozenset(_TITLE_TOKEN_RE.findall((title or "").lower()))


def _jaccard(a: frozenset[str], b: frozenset[str]) -> float:
    """Token-set Jaccard. Empty inputs return 0.0."""
    if not a or not b:
        return 0.0
    union = a | b
    if not union:
        return 0.0
    return len(a & b) / len(union)


def _title_search_rescue_one(
    conn: sqlite3.Connection,
    *,
    row: sqlite3.Row,
    model: str,
    job_id: str,
    add_job_log: Callable[..., None],
    batch_label: str,
) -> dict:
    """Attempt one title-search rescue for an unmatched paper.

    Calls Semantic Scholar `/paper/search` once with the local title.
    Accepts the highest-Jaccard candidate iff:

    - title token-set Jaccard >= ``TITLE_RESCUE_JACCARD_THRESHOLD``
    - year delta vs local year is at most ``TITLE_RESCUE_YEAR_DELTA``
      (pass automatically when either side has no year)

    On accept, fill-only-writes the resolved s2_id / DOI / abstract back
    to the local row (the trigger
    ``papers_clear_fetch_status_on_id_change`` then drops the now-stale
    `unmatched` ledger row), upserts the vector when present, and
    explicitly clears the leftover fetch_status in case neither id
    changed (e.g. local already had the right DOI but S2 only resolved
    via title).

    Returns ``{"rescued": bool, "stored": bool, "jaccard": float, "reason": str}``.
    """
    paper_id = str(row["id"])
    title = str(row["title"] or "").strip()
    if not title:
        return {"rescued": False, "stored": False, "jaccard": 0.0, "reason": "no_title"}

    try:
        local_year = int(row["year"]) if row["year"] is not None else None
    except (TypeError, ValueError):
        local_year = None

    try:
        candidates = semantic_scholar.search_papers(
            title[:TITLE_RESCUE_QUERY_MAX_CHARS],
            limit=TITLE_RESCUE_MAX_RESULTS,
            raise_on_rate_limit=True,
        )
    except semantic_scholar.SemanticScholarBatchError as exc:
        # 429 / transient — defer, don't mark terminal. The `unmatched`
        # row stays so the next sweep retries; a truly persistent
        # mismatch will keep getting `unmatched` only after S2 actually
        # answers with no candidates.
        add_job_log(
            job_id,
            "Title-search rescue deferred by rate limit",
            level="WARNING",
            step="title_rescue_rate_limited",
            data={
                "batch": batch_label,
                "paper_id": paper_id,
                "status_code": getattr(exc, "status_code", None),
            },
        )
        return {
            "rescued": False,
            "stored": False,
            "jaccard": 0.0,
            "reason": "rate_limited",
        }
    except Exception as exc:
        add_job_log(
            job_id,
            "Title-search rescue raised an exception",
            level="WARNING",
            step="title_rescue_error",
            data={"batch": batch_label, "paper_id": paper_id, "error": str(exc)},
        )
        return {"rescued": False, "stored": False, "jaccard": 0.0, "reason": "search_error"}

    if not candidates:
        return {"rescued": False, "stored": False, "jaccard": 0.0, "reason": "no_results"}

    local_tokens = _title_tokens(title)
    best = None
    best_jaccard = 0.0
    for cand in candidates:
        cand_title = str(cand.get("title") or "").strip()
        if not cand_title:
            continue
        cand_tokens = _title_tokens(cand_title)
        jaccard = _jaccard(local_tokens, cand_tokens)
        if jaccard < TITLE_RESCUE_JACCARD_THRESHOLD:
            continue
        cand_year_raw = cand.get("year")
        try:
            cand_year = int(cand_year_raw) if cand_year_raw is not None else None
        except (TypeError, ValueError):
            cand_year = None
        if (
            local_year is not None
            and cand_year is not None
            and abs(local_year - cand_year) > TITLE_RESCUE_YEAR_DELTA
        ):
            continue
        if jaccard > best_jaccard:
            best = cand
            best_jaccard = jaccard

    if best is None:
        return {
            "rescued": False,
            "stored": False,
            "jaccard": 0.0,
            "reason": "no_match_above_threshold",
        }

    new_s2_id = str(best.get("semantic_scholar_id") or "").strip()
    new_doi = canonical_lookup_doi(str(best.get("doi") or "")) or ""
    new_abstract = str(best.get("abstract") or "").strip()

    # Fill-only writes — never overwrite a local value that's already
    # set (don't undo a hand-curated DOI just because S2 returned a
    # different one). The trigger fires only if the column actually
    # changed.
    conn.execute(
        """
        UPDATE papers
        SET semantic_scholar_id = COALESCE(NULLIF(semantic_scholar_id, ''), ?),
            doi = CASE
                WHEN COALESCE(NULLIF(TRIM(doi), ''), '') = '' AND ? != ''
                    THEN ?
                ELSE doi
            END,
            abstract = CASE
                WHEN COALESCE(NULLIF(TRIM(abstract), ''), '') = '' AND ? != ''
                    THEN ?
                ELSE abstract
            END,
            updated_at = ?
        WHERE id = ?
        """,
        (
            new_s2_id or None,
            new_doi,
            new_doi,
            new_abstract,
            new_abstract,
            datetime.utcnow().isoformat(),
            paper_id,
        ),
    )

    stored = False
    vector = best.get("specter2_embedding")
    if isinstance(vector, list) and vector:
        try:
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
            stored = cursor.rowcount > 0
        except Exception as exc:
            logger.warning(
                "Title-search rescue vector store failed for %s: %s", paper_id, exc
            )
            stored = False

    # Belt-and-suspenders: if neither id changed (so the trigger didn't
    # fire) but we did rescue the paper, the leftover `unmatched` row
    # would still trap it. Clear it explicitly.
    _clear_fetch_status(conn, paper_id=paper_id, model=model)
    return {
        "rescued": True,
        "stored": stored,
        "jaccard": round(best_jaccard, 4),
        "reason": "title_match",
    }


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
        title_rescue_calls = 0
        title_rescue_skipped_budget = 0
        title_rescue_rate_limited = 0
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

            batch_bad_local_doi_before = bad_local_doi
            unmatched_in_batch: list[sqlite3.Row] = []
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
                    # Defer the title-search rescue until after the batch
                    # commit so we never compete with the in-progress
                    # writer. The rescue runs at most one search call
                    # per paper.
                    unmatched_in_batch.append(row)
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

            # Title-search rescue pass for residual unmatched papers in
            # this batch. One `/paper/search` call per paper, accepted
            # only above the Jaccard + year-delta threshold. Runs after
            # the batch commit so the writer lock is released between
            # remote calls (lessons.md "Background jobs must release
            # the writer lock before every remote call AND between
            # phases").
            #
            # Three guards keep us polite (Phase 8 of
            # `tasks/13_END_TO_END_HYDRATION_VECTOR_CHAIN.md`):
            # - `TITLE_RESCUE_PER_RUN_BUDGET` caps total search calls
            #   per run so a 5 000-paper backlog can't burn S2's 1 RPS
            #   /paper/search quota in one go.
            # - The first 429 short-circuits the rest of this batch's
            #   rescue: more search calls into a known-overloaded
            #   upstream just dig the hole deeper.
            # - The shared HTTP client's adaptive throttle (engaged on
            #   any 429) already lengthens the per-request interval to
            #   30 s for the next minute; we respect that by stopping
            #   here.
            batch_rescued = 0
            batch_rescued_with_vector = 0
            rate_limited = False
            for unmatched_row in unmatched_in_batch:
                if title_rescue_calls >= TITLE_RESCUE_PER_RUN_BUDGET:
                    title_rescue_skipped_budget += 1
                    continue
                title_rescue_calls += 1
                outcome = _title_search_rescue_one(
                    conn,
                    row=unmatched_row,
                    model=model,
                    job_id=job_id,
                    add_job_log=add_job_log,
                    batch_label=str(start),
                )
                if outcome.get("reason") == "rate_limited":
                    rate_limited = True
                    title_rescue_rate_limited += 1
                    # Don't keep firing into a known-overloaded
                    # endpoint; let the adaptive throttle drain.
                    break
                if not outcome["rescued"]:
                    continue
                batch_rescued += 1
                # Reflect the rescue in our running counters so the
                # batch log + final summary tell a coherent story. The
                # `unmatched` counter still reflects rows that failed
                # both batch + title-search, exactly the
                # ones still terminal after this run.
                unmatched -= 1
                if outcome["stored"]:
                    stored += 1
                    batch_inserted_paper_ids.append(str(unmatched_row["id"]))
                    batch_rescued_with_vector += 1
            if batch_rescued or rate_limited:
                conn.commit()
                add_job_log(
                    job_id,
                    "Title-search rescue pass",
                    step="title_rescue",
                    data={
                        "batch_start": start,
                        "considered": len(unmatched_in_batch),
                        "rescued": batch_rescued,
                        "rescued_with_vector": batch_rescued_with_vector,
                        "rate_limited": rate_limited,
                        "calls_so_far": title_rescue_calls,
                        "budget": TITLE_RESCUE_PER_RUN_BUDGET,
                    },
                )

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

        # Phase 6 of `tasks/13_END_TO_END_HYDRATION_VECTOR_CHAIN.md`:
        # auto-schedule the local SPECTER2 fill if the active provider
        # is local SPECTER2 and there are still papers with title +
        # abstract that lack the active-model vector. Skip silently
        # otherwise (no provider switch, no surprise).
        try:
            from alma.services.embedding_chain import schedule_post_s2_chain

            chain = schedule_post_s2_chain(conn, trigger_reason="post_s2_fetch")
            if chain.get("scheduled_jobs"):
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
