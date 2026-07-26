"""Corpus author backfill — fetch all works + SPECTER2 vectors for a resolved author.

Why this exists: author centroids (the mean SPECTER2 vector of an
author's works, used by `paper_signal.author_alignment` and by the
D12 network bucket candidate scoring) are only as good as the paper
coverage we have for that author. Before this job existed, an author
might have 50 works in OpenAlex but only 3 in our corpus — their
centroid was basically random.

What it does per author:
  1. Fetch the profile → get declared `works_count`. Skip the author
     entirely if `local_paper_count >= works_count` (trust parity).
  2. Page through every OpenAlex work
     (`fetch_works_page_for_author` with cursor pagination).
  3. Upsert each work into `papers` + `publication_authors` using
     the canonical dedup triple (openalex_id → doi →
     (year, normalized_title)).
  4. For any newly-landed (or previously vector-less) paper with a
     DOI or Semantic Scholar ID, batch-fetch the SPECTER2 vector
     via `fetch_papers_batch(fields='embedding.specter_v2')` and
     insert into `publication_embeddings`.
  5. Recompute this author's centroid over ALL their
     `publication_embeddings` (corpus-wide, per lesson "feature
     vectors should be built from the widest available ground truth
     for that entity") and UPSERT into `author_centroids`.

The batch variant walks every author that has a resolved OpenAlex
ID whose centroid is missing or stale (>14 days). It commits between
authors so concurrent reads don't freeze (per the "bulk background
jobs must commit per unit of work" lesson).
"""

from __future__ import annotations

import logging
import sqlite3
from collections import Counter
from collections.abc import Callable
from datetime import datetime, timedelta, timezone
from typing import Any

from alma.ai.embedding_sources import EMBEDDING_SOURCE_SEMANTIC_SCHOLAR
from alma.core.db_write import commit_unless_gated, write_section
from alma.core.sql_helpers import standalone_paper_sql
from alma.core.utils import (
    canonical_lookup_doi,
    normalize_doi,
    resolve_existing_paper_id,
)
from alma.discovery import semantic_scholar
from alma.openalex import client as openalex_client

logger = logging.getLogger(__name__)


_CENTROID_STALE_DAYS = 14
_S2_BATCH_SIZE = 100
_VECTOR_FIELDS = "paperId,externalIds,embedding.specter_v2"


# -- centroid maintenance --------------------------------------------

def refresh_author_centroid(
    conn: sqlite3.Connection,
    openalex_id: str,
    *,
    model: str | None = None,
) -> bool:
    """Recompute and UPSERT one author's centroid from their embeddings.

    Returns True when a row was written (author had ≥1 embedded paper),
    False otherwise. Safe to call during hot paths — fails silently if
    numpy is unavailable or the tables are missing.
    """

    oid = str(openalex_id or "").strip().lower()
    if not oid:
        return False
    try:
        import numpy as np
    except ImportError:
        return False
    if model is None:
        try:
            from alma.discovery.similarity import get_active_embedding_model

            model = get_active_embedding_model(conn)
        except Exception:
            return False
    if not model:
        return False
    try:
        rows = conn.execute(
            f"""
            SELECT pe.embedding AS embedding
            FROM publication_authors pa
            JOIN publication_embeddings pe
              ON pe.paper_id = pa.paper_id AND pe.model = ?
            JOIN papers p ON p.id = pa.paper_id
            WHERE lower(pa.openalex_id) = ?
              AND {standalone_paper_sql('p')}
            """,
            (model, oid),
        ).fetchall()
    except sqlite3.OperationalError:
        return False
    from alma.core.vector_blob import decode_vectors_uniform, encode_vector

    # Uniform decoder so a single rogue legacy-fp32 paper row can't
    # blow up the centroid build with "all input arrays must have the
    # same shape" — see lessons.md "Vector blob storage dtype".
    matrix, _ = decode_vectors_uniform(row["embedding"] for row in rows)
    if matrix.size == 0:
        conn.execute(
            "DELETE FROM author_centroids WHERE author_openalex_id = ? AND model = ?",
            (oid, model),
        )
        return False
    centroid = np.mean(matrix, axis=0)
    conn.execute(
        """
        INSERT INTO author_centroids
            (author_openalex_id, model, centroid_blob, paper_count, updated_at)
        VALUES (?, ?, ?, ?, ?)
        ON CONFLICT(author_openalex_id, model) DO UPDATE SET
            centroid_blob = excluded.centroid_blob,
            paper_count = excluded.paper_count,
            updated_at = excluded.updated_at
        """,
        (
            oid,
            model,
            encode_vector(centroid),
            int(matrix.shape[0]),
            datetime.now(timezone.utc).isoformat(),
        ),
    )
    return True


def refresh_centroids_for_papers(
    conn: sqlite3.Connection,
    paper_ids: list[str],
    *,
    model: str | None = None,
) -> int:
    """Recompute every author's centroid touched by these papers.

    Call this after a batch `publication_embeddings` insert so the
    cached centroids stay in lock-step with the embedding corpus.
    Returns the count of centroids updated.
    """

    if not paper_ids:
        return 0
    placeholders = ",".join("?" * len(paper_ids))
    try:
        rows = conn.execute(
            f"""
            SELECT DISTINCT lower(trim(openalex_id)) AS oid
            FROM publication_authors
            WHERE paper_id IN ({placeholders})
              AND COALESCE(TRIM(openalex_id), '') <> ''
            """,
            paper_ids,
        ).fetchall()
    except sqlite3.OperationalError:
        return 0
    updated = 0
    for row in rows:
        if refresh_author_centroid(conn, row["oid"], model=model):
            updated += 1
    return updated


# -- centroid-only recompute (task-29 Checkpoint C: split from deep refresh) ---
#
# The monolithic `refresh_author_works_and_vectors` paginated works, fetched S2
# vectors, AND recomputed the centroid in one call. Step 9 of the canonical
# maintenance order ("recompute touched author centroids") is a *separate*
# operation: vectors change (S2 fetch / local embed) → only the centroid needs
# refreshing, with no network and no works re-pagination. These two functions
# back the standalone `author_centroids` maintenance task so a centroid refresh
# is bounded, cheap, and local-only.


def _authors_needing_centroid_sql() -> str:
    """Shared SELECT body for the count + the recompute selection so the Health
    card's pending number is exactly what a run would touch. A centroid 'needs
    recompute' when it is missing, when its stored `paper_count` no longer
    matches the author's current embedded-paper count (vectors added/removed),
    or when it predates the newest embedding for that author (a vector was
    refreshed in place). Both callers bind the embedding model twice (the
    `pe.model` filter and the `author_centroids` join)."""
    return f"""
        SELECT lower(trim(a.openalex_id)) AS oid,
               COUNT(DISTINCT pe.paper_id) AS emb_count,
               ac.paper_count             AS centroid_count,
               MAX(pe.created_at)         AS newest_emb,
               ac.updated_at              AS centroid_at,
               ac.author_openalex_id      AS centroid_oid
        FROM authors a
        JOIN publication_authors pa
          ON lower(trim(pa.openalex_id)) = lower(trim(a.openalex_id))
        JOIN publication_embeddings pe
          ON pe.paper_id = pa.paper_id AND pe.model = ?
        JOIN papers p
          ON p.id = pa.paper_id
        LEFT JOIN author_centroids ac
          ON ac.author_openalex_id = lower(trim(a.openalex_id)) AND ac.model = ?
        WHERE COALESCE(TRIM(a.openalex_id), '') <> ''
          AND {standalone_paper_sql('p')}
        GROUP BY oid
        HAVING ac.author_openalex_id IS NULL
            OR ac.paper_count <> emb_count
            OR ac.updated_at < MAX(pe.created_at)
        ORDER BY oid
    """


def _centroid_model(conn: sqlite3.Connection, model: str | None) -> str:
    if model:
        return model
    from alma.discovery.similarity import get_active_embedding_model

    return get_active_embedding_model(conn) or semantic_scholar.S2_SPECTER2_MODEL


def count_authors_needing_centroid(
    conn: sqlite3.Connection, *, model: str | None = None
) -> int:
    """How many authors have an out-of-date centroid (the `author_centroids`
    maintenance task's pending count). Never raises — a schema gap reports 0."""
    resolved = _centroid_model(conn, model)
    try:
        rows = conn.execute(
            f"SELECT COUNT(*) AS n FROM ({_authors_needing_centroid_sql()})",
            (resolved, resolved),
        ).fetchone()
        return int((rows["n"] if rows else 0) or 0)
    except sqlite3.OperationalError:
        return 0


def recompute_author_centroids(
    conn: sqlite3.Connection,
    *,
    limit: int,
    model: str | None = None,
    job_id: str | None = None,
    set_job_status: Callable[..., Any] | None = None,
    add_job_log: Callable[..., Any] | None = None,
    is_cancellation_requested: Callable[[str], bool] | None = None,
) -> dict:
    """Recompute stale/missing author centroids from EXISTING local embeddings.

    Bounded by `limit` (unit = one author). Pure-local: no network, no works
    pagination — just `refresh_author_centroid` over the authors whose centroid
    drifted from their embedding set. Each author's recompute is its own short
    writer-gated section (no I/O inside), so a long run never holds the write
    lock across the loop and cancellation lands cleanly on an author boundary.
    """
    cap = max(1, int(limit))
    resolved = _centroid_model(conn, model)
    # Gather the work list first (read-only), then write per author.
    rows = conn.execute(
        _authors_needing_centroid_sql() + " LIMIT ?",
        (resolved, resolved, cap),
    ).fetchall()
    oids = [str(r["oid"]) for r in rows if r["oid"]]
    total = len(oids)
    summary = {"selected": total, "processed": 0, "updated": 0, "cancelled": False}
    if set_job_status and job_id:
        set_job_status(job_id, status="running", total=total, processed=0)
    for idx, oid in enumerate(oids, start=1):
        if is_cancellation_requested and job_id and is_cancellation_requested(job_id):
            summary["cancelled"] = True
            break
        try:
            with write_section(conn, label="recompute_author_centroids"):
                if refresh_author_centroid(conn, oid, model=resolved):
                    summary["updated"] += 1
        except Exception as exc:  # one bad author never aborts the sweep
            logger.warning("centroid recompute failed for %s: %s", oid, exc)
        summary["processed"] = idx
        if set_job_status and job_id and (idx % 25 == 0 or idx == total):
            set_job_status(job_id, processed=idx, total=total)
    if add_job_log and job_id:
        add_job_log(
            job_id,
            f"Recomputed {summary['updated']} author centroid(s) of {total} stale "
            f"({'cancelled' if summary['cancelled'] else 'complete'})",
            step="centroids_done",
        )
    return summary


def _fetch_missing_s2_vectors_for_author(
    conn: sqlite3.Connection,
    openalex_id: str,
    *,
    log: Callable[..., None] | None = None,
) -> Counter[str]:
    """Fetch missing S2/SPECTER2 vectors for one author's local papers."""

    oid = str(openalex_id or "").strip().lower()
    summary: Counter[str] = Counter()
    if not oid:
        return summary
    model = semantic_scholar.S2_SPECTER2_MODEL
    from alma.services.s2_vectors import (
        TERMINAL_FETCH_STATUSES,
        _clear_fetch_status,
        _doi_from_s2,
        _ensure_fetch_status_table,
        _lookup_ids_for_row,
        _lookup_key_for_row,
        _upsert_fetch_status,
    )

    _ensure_fetch_status_table(conn)
    terminal_statuses = tuple(sorted(TERMINAL_FETCH_STATUSES))
    terminal_clause = ",".join("?" for _ in terminal_statuses)
    pending = conn.execute(
        f"""
        SELECT p.id, p.doi, p.semantic_scholar_id
        FROM publication_authors pa
        JOIN papers p ON p.id = pa.paper_id
        LEFT JOIN publication_embedding_fetch_status fs
         ON fs.paper_id = p.id
         AND fs.model = ?
         AND fs.source = ?
         AND fs.lookup_key = (
             lower(trim(COALESCE(p.semantic_scholar_id, '')))
             || '|'
             || lower(trim(COALESCE(p.doi, '')))
         )
        WHERE lower(pa.openalex_id) = ?
          AND {standalone_paper_sql('p')}
          AND (
               COALESCE(NULLIF(TRIM(p.doi), ''), '') <> ''
            OR COALESCE(NULLIF(TRIM(p.semantic_scholar_id), ''), '') <> ''
          )
          AND NOT EXISTS (
              SELECT 1
              FROM publication_embeddings pe
              WHERE pe.paper_id = p.id
                AND pe.model = ?
                AND pe.source = ?
          )
          AND COALESCE(fs.status, '') NOT IN ({terminal_clause})
        """,
        (
            model,
            EMBEDDING_SOURCE_SEMANTIC_SCHOLAR,
            oid,
            model,
            EMBEDDING_SOURCE_SEMANTIC_SCHOLAR,
            *terminal_statuses,
        ),
    ).fetchall()

    lookups: list[tuple[str, str]] = []
    pending_by_id: dict[str, sqlite3.Row] = {}
    bad_doi_rows: list[sqlite3.Row] = []
    for row in pending:
        paper_id = str(row["id"])
        pending_by_id[paper_id] = row
        lookup_ids = _lookup_ids_for_row(row)
        if lookup_ids:
            # Preserve the previous one-request-per-paper author budget:
            # prefer S2 id, fall back to a validated DOI.
            lookups.append((paper_id, lookup_ids[0]))
            continue
        if str(row["doi"] or "").strip():
            bad_doi_rows.append(row)

    # Write the bad_local_doi statuses through the writer GATE before the first
    # S2 call. Gating matters twice over: BEGIN IMMEDIATE (not a raw DEFERRED
    # commit that loses the lock-upgrade race under concurrency → "database is
    # locked"), and the section closes before any network, so the writer lock is
    # never held across the chunk loop's minutes-long S2 rate-limit / 429 backoff
    # (the original "database is locked" root cause, verified live 2026-06-05).
    if bad_doi_rows:
        with write_section(conn, label="author vectors: bad_local_doi"):
            for row in bad_doi_rows:
                _upsert_fetch_status(
                    conn,
                    row=row,
                    model=model,
                    status="bad_local_doi",
                    reason=(
                        "Local DOI fails registry-shape regex; fix the import "
                        "or rerun hydration to rewrite the DOI before retrying."
                    ),
                    lookup_ids=[],
                    lookup_key=_lookup_key_for_row(row),
                )
                summary["bad_local_doi"] += 1
    if not lookups:
        return summary

    if log is not None:
        log(
            "fetch_vectors",
            f"Fetching SPECTER2 vectors for {len(lookups)} author paper(s)",
            processed=0,
            total=len(lookups),
        )
    vectors_found = 0
    for chunk_start in range(0, len(lookups), _S2_BATCH_SIZE):
        chunk = lookups[chunk_start:chunk_start + _S2_BATCH_SIZE]
        lookup_ids = [lid for _, lid in chunk]
        try:
            batch = semantic_scholar.fetch_papers_batch(
                lookup_ids,
                fields=_VECTOR_FIELDS,
                batch_size=_S2_BATCH_SIZE,
                raise_on_error=True,
            )
        except semantic_scholar.SemanticScholarBatchError as exc:
            logger.warning(
                "author S2 vector fetch deferred for %s (%d ids): %s",
                oid,
                len(lookup_ids),
                exc,
            )
            summary["vector_fetch_errors"] += len(chunk)
            # Gated write of the error statuses (BEGIN IMMEDIATE, committed on
            # exit). The section opens AFTER the failed fetch and closes before
            # the next chunk's S2 call, so the writer lock is never held across a
            # 429 backoff sleep — the repeat-error path used to keep a raw txn
            # open across every retry.
            with write_section(conn, label="author vectors: s2_error"):
                for paper_id, lookup_id in chunk:
                    row = pending_by_id.get(paper_id)
                    if row is None:
                        continue
                    _upsert_fetch_status(
                        conn,
                        row=row,
                        model=model,
                        status="error",
                        reason=str(exc),
                        lookup_ids=[lookup_id],
                        lookup_key=_lookup_key_for_row(row),
                    )
            continue
        by_lookup = {
            str(v.get("_requested_id") or "").strip(): v
            for v in batch.values()
            if v.get("_requested_id")
        }
        by_s2 = {
            str(v.get("paperId") or "").strip(): v
            for v in batch.values()
            if str(v.get("paperId") or "").strip()
        }
        by_doi = {
            doi: v
            for v in batch.values()
            if (doi := _doi_from_s2(v))
        }
        # Gated write of this chunk's vectors/statuses — opened AFTER the S2
        # batch fetch above, so the writer lock (BEGIN IMMEDIATE) is held only
        # for the local writes, never across the network call. The by_* dicts
        # are in-memory projections of the already-fetched batch.
        with write_section(conn, label="author vectors: upsert"):
            for paper_id, lookup_id in chunk:
                local_row = pending_by_id.get(paper_id)
                if local_row is None:
                    continue
                s2_id = str(local_row["semantic_scholar_id"] or "").strip()
                doi = canonical_lookup_doi(str(local_row["doi"] or "")) or ""
                paper = (
                    by_lookup.get(lookup_id)
                    or (by_s2.get(s2_id) if s2_id else None)
                    or (by_doi.get(doi) if doi else None)
                )
                if not paper:
                    summary["vectors_missing"] += 1
                    _upsert_fetch_status(
                        conn,
                        row=local_row,
                        model=model,
                        status="unmatched",
                        reason=(
                            "Semantic Scholar returned no paper for current "
                            "DOI/S2 lookup id"
                        ),
                        lookup_ids=[lookup_id],
                        lookup_key=_lookup_key_for_row(local_row),
                    )
                    continue
                vec = semantic_scholar.extract_specter2_vector(paper)
                if not vec:
                    summary["vectors_missing"] += 1
                    _upsert_fetch_status(
                        conn,
                        row=local_row,
                        model=model,
                        status="missing_vector",
                        reason=(
                            "Semantic Scholar returned the paper without "
                            "embedding.specter_v2"
                        ),
                        lookup_ids=[lookup_id],
                        lookup_key=_lookup_key_for_row(local_row),
                    )
                    continue
                if semantic_scholar.upsert_specter2_vector(
                    conn,
                    paper_id,
                    vec,
                    source=EMBEDDING_SOURCE_SEMANTIC_SCHOLAR,
                    created_at=datetime.now(timezone.utc).isoformat(),
                ):
                    vectors_found += 1
                _clear_fetch_status(conn, paper_id=paper_id, model=model)
        if log is not None:
            log(
                "fetch_vectors",
                f"Vectors: {vectors_found}/{len(lookups)}",
                processed=min(chunk_start + _S2_BATCH_SIZE, len(lookups)),
                total=len(lookups),
            )
    summary["vectors_fetched"] = vectors_found
    return summary


# -- backfill runner (per author) ------------------------------------

def refresh_author_works_and_vectors(
    db_path: str,
    author_openalex_id: str,
    *,
    ctx: Any | None = None,
    full_refetch: bool = False,
    profile_cache: dict | None = None,
) -> dict:
    """Fetch all works + SPECTER2 vectors for one author.

    `ctx` is an optional log-step forwarder (see lesson "Activity
    progress must push to operation_status"): if present, calls
    `ctx.log_step(step, message=..., processed=..., total=...)` at
    every phase boundary so the Activity row advances live.

    `full_refetch=True` bypasses the `local >= declared` shortcut and
    always paginates — useful when OpenAlex reshuffles a prolific
    author's counts and the shortcut would cache stale coverage.

    `profile_cache` is an optional mapping from normalized OpenAlex
    author ID → curated profile dict (same shape as
    `openalex_client.fetch_author_profile`). When the lookup hits, we
    skip Phase 1's per-author profile HTTP call. Bulk callers like
    `_deep_refresh_all_impl` use this to collapse N profile fetches
    into a couple of pipe-filter batches via
    `openalex_client.batch_get_author_profiles`.
    """

    from alma.api.deps import open_db_connection

    summary = {
        "author_openalex_id": author_openalex_id,
        "works_fetched": 0,
        "papers_new": 0,
        "papers_updated": 0,
        "vectors_fetched": 0,
        "vectors_missing": 0,
        "vector_fetch_errors": 0,
        "centroid_updated": False,
        "skipped": False,
        # Pass the OpenAlex profile we already fetched in Phase 1 back to
        # the caller. Lets `_refresh_author_cache_impl` skip a second
        # `fetch_author_profile` round-trip per author on bulk deep refresh.
        "profile": None,
    }

    def _log(step: str, message: str, **progress: Any) -> None:
        if ctx is not None:
            try:
                ctx.log_step(step, message=message, **progress)
            except Exception:
                logger.debug("ctx.log_step failed on %s", step, exc_info=True)

    conn = open_db_connection()
    try:
        oid_norm = openalex_client._normalize_openalex_author_id(author_openalex_id)

        # Phase 1: fetch declared works_count and compare. Pre-batched
        # caches (e.g. `_deep_refresh_all_impl`'s pipe-filter pre-flight)
        # win first — saves one OpenAlex roundtrip per author. Falls
        # back to a per-author fetch on cache miss / cache absent.
        cached_profile = None
        if isinstance(profile_cache, dict) and profile_cache:
            cached_profile = profile_cache.get(oid_norm)
        if cached_profile is not None:
            _log("profile", "Author profile served from pre-fetched cache")
            profile = cached_profile
        else:
            _log("profile", "Fetching author profile")
            try:
                profile = openalex_client.fetch_author_profile(oid_norm)
            except Exception as exc:
                logger.warning("author profile fetch failed for %s: %s", oid_norm, exc)
                profile = None
        # Stash before the early-return shortcut so callers always get
        # the profile we paid for, even when we skip pagination.
        summary["profile"] = profile
        declared = int((profile or {}).get("works_count") or 0)

        existing_rows = conn.execute(
            """
            SELECT COUNT(DISTINCT paper_id) AS n
            FROM publication_authors
            WHERE lower(openalex_id) = ?
            """,
            (oid_norm.lower(),),
        ).fetchone()
        existing_count = int(existing_rows["n"] if existing_rows else 0)

        if (
            not full_refetch
            and declared > 0
            and existing_count >= declared
        ):
            summary["skipped"] = True
            summary["declared_works"] = declared
            summary["existing_local"] = existing_count
            _log(
                "skip",
                f"Already have {existing_count}/{declared} works; skipping",
                processed=declared,
                total=declared,
            )
            vector_summary = _fetch_missing_s2_vectors_for_author(
                conn,
                oid_norm,
                log=_log,
            )
            summary["vectors_fetched"] = int(vector_summary.get("vectors_fetched") or 0)
            summary["vectors_missing"] = int(vector_summary.get("vectors_missing") or 0)
            summary["vector_fetch_errors"] = int(vector_summary.get("vector_fetch_errors") or 0)
            # still refresh centroid — embeddings may have just arrived (gated
            # local write; no raw commit racing the gate).
            with write_section(conn, label="author centroid (skip path)"):
                summary["centroid_updated"] = refresh_author_centroid(
                    conn,
                    oid_norm,
                    model=semantic_scholar.S2_SPECTER2_MODEL,
                )
            return summary

        # Phase 2: paginate through all works.
        cursor: str | None = "*"
        works: list[dict] = []
        total_hint = declared or None
        while cursor:
            page = openalex_client.fetch_works_page_for_author(
                oid_norm, cursor=cursor, per_page=100
            )
            batch = page.get("results") or []
            if not batch:
                break
            works.extend(batch)
            summary["works_fetched"] += len(batch)
            if page.get("total") is not None and total_hint is None:
                total_hint = int(page["total"])
            _log(
                "fetch_works",
                f"Fetched {summary['works_fetched']} works",
                processed=summary["works_fetched"],
                total=total_hint or summary["works_fetched"],
            )
            # Release the writer lock (if anything is pending) before the next
            # paginated HTTP call — lesson: commit before every remote call on
            # bulk jobs. commit_unless_gated flushes when standalone, no-ops if
            # ever called from inside a gated unit.
            commit_unless_gated(conn, label="author_backfill fetch_works flush")
            cursor = page.get("next_cursor")

        # Phase 3: upsert each work + publication_authors row. All works are
        # already gathered (Phase 2), so the writes are local-only; chunked
        # writer-gated IMMEDIATE sections keep each lock window short — a
        # prolific author can carry thousands of works, and one giant
        # transaction would stall foreground writes for its whole duration.
        now_iso = datetime.now(timezone.utc).isoformat()
        new_paper_ids: list[str] = []
        upsert_chunk = 200
        for chunk_start in range(0, len(works), upsert_chunk):
            with write_section(conn, label="author_backfill works upsert"):
                for work in works[chunk_start:chunk_start + upsert_chunk]:
                    paper_id, is_new = _upsert_work(conn, work, now=now_iso)
                    if paper_id is None:
                        continue
                    summary["papers_new" if is_new else "papers_updated"] += 1
                    if is_new:
                        new_paper_ids.append(paper_id)
                    # Ensure publication_authors row for this author exists.
                    _ensure_authorship_row(
                        conn,
                        paper_id=paper_id,
                        openalex_id=oid_norm,
                        display_name=str((profile or {}).get("display_name") or "").strip(),
                        work=work,
                    )

        # Phase 4: identify papers still missing an S2-sourced SPECTER2
        # vector, then batch-fetch via Semantic Scholar. The vector model
        # is always Semantic Scholar's SPECTER2 model, not the app's active
        # provider model.
        vector_summary = _fetch_missing_s2_vectors_for_author(
            conn,
            oid_norm,
            log=_log,
        )
        summary["vectors_fetched"] = int(vector_summary.get("vectors_fetched") or 0)
        summary["vectors_missing"] = int(vector_summary.get("vectors_missing") or 0)
        summary["vector_fetch_errors"] = int(vector_summary.get("vector_fetch_errors") or 0)

        # Phase 4.5: route newly-inserted works through the CENTRAL enrichment
        # ledger so they get abstract recovery → S2 vectors → local fill like
        # every other entry path. Previously author-backfill was the one cold
        # entry path: works S2 had no vector for stayed `missing_vector` forever
        # with no metadata recovery and no local fill (task 47 Phase 9). Batched:
        # one gated ledger write + one coalesced sweep for the whole run. The
        # sweep is idempotent (fixed key) and enqueue only writes pending rows
        # for genuinely-missing fields, so fully-hydrated works cost nothing.
        if new_paper_ids:
            try:
                from alma.services.corpus_rehydrate import (
                    enqueue_pending_hydration,
                    schedule_pending_hydration_sweep,
                )

                with write_section(conn, label="author_backfill enrich enqueue"):
                    for pid in new_paper_ids:
                        enqueue_pending_hydration(conn, pid, auto_schedule=False)
                schedule_pending_hydration_sweep(
                    reason="author_backfill",
                    target_paper_ids=new_paper_ids,
                )
            except Exception as exc:
                _log("enrich_enqueue_skipped", f"Author enrichment enqueue skipped: {exc}")

        # Phase 5: recompute centroid (gated local write — no raw commit racing
        # the writer gate under concurrent deep-refresh workers).
        _log("centroid", "Recomputing author centroid")
        with write_section(conn, label="author centroid"):
            summary["centroid_updated"] = refresh_author_centroid(
                conn,
                oid_norm,
                model=semantic_scholar.S2_SPECTER2_MODEL,
            )
        return summary
    finally:
        conn.close()


# -- bounded seed ----------------------------------------------------

SEED_TARGET_PAPERS = 2
"""How many of an author's own papers the corpus needs before they become a
first-class citizen of the semantic surfaces.

TWO, because that is `alma.ai.projections.MIN_AUTHOR_PUBLICATIONS` — the author map
refuses to place anyone below it (one paper is not a research position, it is a
coincidence). The same threshold also gives `_sample_titles_for_openalex_author`
something to show and gives the author field two papers to average a score over.
"""

_SEED_SCAN_PER_PAGE = 50
"""Candidates pulled per author. Deliberately >> SEED_TARGET_PAPERS: the top
hits are frequently near-duplicates of each other (OpenAlex lists
"identification-categorization" and "identification–categorization" as separate
works), and dedup collapses them on landing, so taking exactly N candidates
would reliably land fewer than N papers."""

_SEED_MAX_PAGES = 4
"""Cursor pages walked in one seed attempt (≤200 works at `_SEED_SCAN_PER_PAGE`).

Bounds the cost of a pathological author while still being deep enough that
reaching the end is the NORMAL outcome — which is what lets a genuinely
un-seedable author be marked terminal instead of retried forever."""


def _author_position_in_work(work: dict, openalex_id: str) -> str:
    """This author's `author_position` on this work ("first"/"middle"/"last")."""
    oid = str(openalex_id or "").strip().lower()
    if not oid:
        return ""
    for authorship in work.get("authorships") or []:
        if str(authorship.get("openalex_id") or "").strip().lower() == oid:
            return str(authorship.get("position") or "").strip().lower()
    return ""


SEED_STATUS_SEEDED = "seeded"
SEED_STATUS_EXHAUSTED = "exhausted"
"""Tried and TERMINAL: upstream simply does not hold `SEED_TARGET_PAPERS` works
for this author, so no repair can ever place them. Mirrors
`s2_vectors.TERMINAL_FETCH_STATUSES` — the health dimension reports these as
`exhausted` instead of counting them as outstanding work forever."""


def _record_seed_attempt(
    conn: sqlite3.Connection,
    openalex_id: str,
    *,
    status: str,
    declared_works: int | None,
    local_papers: int,
    reason: str = "",
) -> None:
    """Stamp the outcome of one seed attempt (own gated write section)."""
    try:
        with write_section(conn, label="author seed status"):
            conn.execute(
                """
                INSERT INTO author_seed_status (
                    author_openalex_id, status, declared_works, local_papers,
                    reason, attempts, updated_at
                ) VALUES (?, ?, ?, ?, ?, 1, ?)
                ON CONFLICT(author_openalex_id) DO UPDATE SET
                    status = excluded.status,
                    declared_works = excluded.declared_works,
                    local_papers = excluded.local_papers,
                    reason = excluded.reason,
                    attempts = author_seed_status.attempts + 1,
                    updated_at = excluded.updated_at
                """,
                (
                    openalex_id.lower(),
                    status,
                    declared_works,
                    local_papers,
                    reason[:500],
                    datetime.now(timezone.utc).isoformat(),
                ),
            )
    except sqlite3.OperationalError as exc:
        # Pre-migration DB: the seed itself still worked, only the bookkeeping
        # is unavailable. Never let a status stamp lose landed papers.
        logger.debug("author_seed_status unavailable for %s: %s", openalex_id, exc)


def _existing_paper_ids_for_author(conn: sqlite3.Connection, openalex_id: str) -> set[str]:
    """The corpus paper ids already linked to this author (any status)."""
    oid = str(openalex_id or "").strip().lower()
    if not oid:
        return set()
    try:
        rows = conn.execute(
            "SELECT DISTINCT paper_id FROM publication_authors WHERE lower(openalex_id) = ?",
            (oid,),
        ).fetchall()
    except sqlite3.OperationalError:
        return set()
    return {str(r["paper_id"] if isinstance(r, sqlite3.Row) else r[0]) for r in rows}


def count_local_papers_for_author(conn: sqlite3.Connection, openalex_id: str) -> int:
    """How many first-class corpus papers we hold for this OpenAlex author."""
    oid = str(openalex_id or "").strip().lower()
    if not oid:
        return 0
    try:
        row = conn.execute(
            f"""
            SELECT COUNT(DISTINCT pa.paper_id) AS n
            FROM publication_authors pa
            JOIN papers p ON p.id = pa.paper_id
            WHERE lower(pa.openalex_id) = ?
              AND {standalone_paper_sql('p')}
            """,
            (oid,),
        ).fetchone()
    except sqlite3.OperationalError:
        return 0
    return int(row["n"] if isinstance(row, sqlite3.Row) else (row[0] if row else 0))


def seed_papers_for_author(
    conn: sqlite3.Connection,
    author_openalex_id: str,
    *,
    target_papers: int = SEED_TARGET_PAPERS,
    log: Callable[..., None] | None = None,
) -> dict:
    """Land an author's most-cited OWN papers so they gain a semantic position.

    The bounded sibling of `refresh_author_works_and_vectors`: that one paginates
    an author's entire output to make their centroid honest; this one fetches the
    few papers needed to make them EXIST on the semantic surfaces at all.

    Why it is needed (measured 2026-07-26): an author suggested from citation or
    co-author expansion typically has 0–1 papers in the corpus — they are
    suggested precisely because you don't have them yet. Below two papers they
    have no map dot (`MIN_AUTHOR_PUBLICATIONS`), no sample titles (those are read from
    the local corpus), and no score (nothing to average). The suggestion card was
    therefore thinnest exactly where the user most needed evidence.

    Selection: their most-cited FIRST-AUTHOR works, topped up with their
    most-cited works in any position if they don't have enough. The top-up is not
    a nicety — a senior PI who publishes last-author would otherwise land nothing
    and stay invisible, which is the failure this function exists to remove.

    Papers land as `status='tracked'` (corpus context, never Library — D2/D4).

    Returns a summary dict; never raises on upstream failure.
    """

    def _log(step: str, message: str, **progress: Any) -> None:
        if log is not None:
            try:
                log(step, message, **progress)
            except Exception:
                logger.debug("seed log failed on %s", step, exc_info=True)

    oid_norm = openalex_client._normalize_openalex_author_id(author_openalex_id)
    summary: dict[str, Any] = {
        "author_openalex_id": oid_norm,
        "existing_local": 0,
        "papers_landed": 0,
        "first_author_used": 0,
        "topped_up": 0,
        "vectors_fetched": 0,
        "skipped": False,
        "reason": "",
    }
    if not oid_norm:
        summary["skipped"] = True
        summary["reason"] = "no_openalex_id"
        return summary

    existing = count_local_papers_for_author(conn, oid_norm)
    summary["existing_local"] = existing
    if existing >= target_papers:
        summary["skipped"] = True
        summary["reason"] = "already_covered"
        return summary

    # Gather over the network FIRST — never hold a write txn across HTTP.
    page = openalex_client.fetch_works_page_for_author(
        oid_norm, per_page=_SEED_SCAN_PER_PAGE, sort="cited_by_count:desc"
    )
    candidates = page.get("results") or []
    next_cursor = page.get("next_cursor")
    # What upstream actually HOLDS for this author. `total` is the OpenAlex
    # result count; the page length is the floor when the meta count is absent.
    declared = page.get("total")
    declared_works = int(declared) if isinstance(declared, int) else len(candidates)
    summary["declared_works"] = declared_works

    if not candidates:
        summary["skipped"] = True
        summary["reason"] = str(page.get("error") or "no_works")
        # Terminality is decided by what OpenAlex HOLDS (`meta.count`), never by
        # the length of this list. `fetch_works_page_for_author` filters client
        # side — work types outside its allowlist (dataset, dissertation,
        # editorial…) and file-looking titles are dropped — so an author with a
        # full catalogue of filtered-out types arrives here with an empty list
        # and no error. Stamping that terminal retired seedable authors
        # permanently (finding B-3, 2026-07-26).
        #
        # An upstream ERROR is likewise retryable; only a genuinely empty
        # catalogue is terminal.
        if not page.get("error") and declared_works < target_papers:
            _record_seed_attempt(
                conn, oid_norm, status=SEED_STATUS_EXHAUSTED,
                declared_works=declared_works, local_papers=existing,
                reason=f"OpenAlex holds only {declared_works} work(s) for this author",
            )
        _log("seed_fetch", f"No usable works on page 1 for {oid_norm}")
        return summary

    # Land papers until the author CLEARS the threshold, counting what actually
    # landed rather than what we tried. Two distinct ways a candidate yields
    # nothing, both observed on the first live run (2026-07-26):
    #   * near-duplicate top hits collapse onto one row via the dedup triple
    #     (OpenAlex lists "identification-categorization" and the en-dash
    #     variant as separate works);
    #   * the top-cited candidate is very often the ONE paper we already hold
    #     for them — `_upsert_work` returns that existing id and the authorship
    #     INSERT OR IGNORE is a no-op, so it is not new coverage.
    # Pre-seeding `seen_paper_ids` with what they already own makes both cases
    # fall out of the same guard. Without it, Shannon and Hoyer each reported
    # "1 paper landed" and stayed stuck at one paper.
    #
    # PAGINATED, because both of those failures consume candidates without
    # producing coverage. A single page meant an author whose top 50 works all
    # collapsed stayed short forever: the retryable status invited another run,
    # and every run re-fetched the identical page-1 and landed nothing again
    # (finding C-5, 2026-07-26). Walking the cursor also makes exhaustion
    # PROVABLE — see `catalogue_walked` below.
    needed = target_papers - existing
    now_iso = datetime.now(timezone.utc).isoformat()
    landed: list[str] = []
    seen_paper_ids: set[str] = _existing_paper_ids_for_author(conn, oid_norm)
    pages_read = 0
    catalogue_walked = False

    while True:
        pages_read += 1
        # Already citation-desc from the API, so preserving order preserves rank.
        first_author = [
            w for w in candidates if _author_position_in_work(w, oid_norm) == "first"
        ]
        others = [w for w in candidates if _author_position_in_work(w, oid_norm) != "first"]
        ordered = first_author + others
        first_author_ids = {id(w) for w in first_author}

        with write_section(conn, label="seed suggestion author papers"):
            for work in ordered:
                if len(landed) >= needed:
                    break
                paper_id, _is_new = _upsert_work(conn, work, now=now_iso)
                if paper_id is None or paper_id in seen_paper_ids:
                    continue
                seen_paper_ids.add(paper_id)
                _ensure_authorship_row(
                    conn,
                    paper_id=paper_id,
                    openalex_id=oid_norm,
                    # No fallback name. `_ensure_authorship_row` reads the
                    # correct display name for THIS author on THIS work out of
                    # the structured authorships. The old fallback took the
                    # comma-joined list's FIRST name, which is this author only
                    # when they are first — precisely not the top-up case this
                    # branch exists to serve (finding N-1, 2026-07-26).
                    display_name="",
                    work=work,
                )
                landed.append(paper_id)
                if id(work) in first_author_ids:
                    summary["first_author_used"] += 1
                else:
                    summary["topped_up"] += 1

        if len(landed) >= needed:
            break
        if not next_cursor:
            # Cursor ran out: we have now SEEN this author's whole catalogue.
            catalogue_walked = True
            break
        if pages_read >= _SEED_MAX_PAGES:
            break
        page = openalex_client.fetch_works_page_for_author(
            oid_norm,
            per_page=_SEED_SCAN_PER_PAGE,
            sort="cited_by_count:desc",
            cursor=next_cursor,
        )
        if page.get("error"):
            break
        candidates = page.get("results") or []
        next_cursor = page.get("next_cursor")
        if not candidates and not next_cursor:
            catalogue_walked = True
            break

    summary["papers_landed"] = len(landed)
    summary["pages_read"] = pages_read
    final_local = existing + len(landed)
    _log(
        "seed_papers",
        f"Landed {len(landed)} paper(s) for {oid_norm} over {pages_read} page(s) "
        f"({summary['first_author_used']} first-author, {summary['topped_up']} topped up)",
    )

    # Record the outcome so the health gap can CONVERGE. An author still short of
    # the threshold after we have seen their whole upstream catalogue can never
    # clear it — OpenAlex simply has fewer than `target_papers` works for them
    # (observed live: two suggested authors with exactly one work in the entire
    # index). Marking that terminal is what stops `authors.unplaceable` counting
    # them forever and every repair reporting "Seeded 0 of N".
    if final_local >= target_papers:
        status = SEED_STATUS_SEEDED
        reason = ""
    elif declared_works < target_papers:
        status = SEED_STATUS_EXHAUSTED
        reason = f"OpenAlex holds only {declared_works} work(s) for this author"
    elif catalogue_walked:
        # We walked the cursor to its end and STILL could not reach the target.
        # Upstream's count says it has enough works; every one of them either
        # dedups onto a paper we already hold or is unusable. Nothing a later
        # run can do differently, so this is terminal — the honest verdict a
        # single-page fetch could never reach (C-5).
        status = SEED_STATUS_EXHAUSTED
        reason = (
            f"walked all {declared_works} upstream work(s); none add new coverage"
        )
    else:
        # Page budget stopped us short of the end of the catalogue. Genuinely
        # retryable: there are unseen works left.
        status = SEED_STATUS_SEEDED
        reason = "target not reached; upstream has more works to try"
    summary["seed_status"] = status
    _record_seed_attempt(
        conn, oid_norm, status=status, declared_works=declared_works,
        local_papers=final_local, reason=reason,
    )

    if not landed:
        summary["reason"] = summary["reason"] or "nothing_landed"
        return summary

    # Vectors are what actually buy the map position — a paper with no embedding
    # leaves the author exactly as unplaceable as before.
    try:
        vectors = _fetch_missing_s2_vectors_for_author(conn, oid_norm, log=None)
        summary["vectors_fetched"] = int(vectors.get("vectors_fetched") or 0)
    except Exception as exc:  # noqa: BLE001 — a vector miss must not lose the papers
        logger.warning("seed: vector fetch failed for %s: %s", oid_norm, exc)

    with write_section(conn, label="seed author centroid"):
        try:
            summary["centroid_updated"] = refresh_author_centroid(
                conn, oid_norm, model=semantic_scholar.S2_SPECTER2_MODEL
            )
        except Exception as exc:  # noqa: BLE001
            logger.warning("seed: centroid refresh failed for %s: %s", oid_norm, exc)

    return summary


# -- batch variant ---------------------------------------------------

def backfill_all_resolved_authors(
    db_path: str,
    *,
    ctx: Any | None = None,
    limit: int | None = None,
    is_cancellation_requested: Callable[[], bool] | None = None,
) -> dict:
    """Run `refresh_author_works_and_vectors` over every resolved author
    whose centroid is missing or older than 14 days.

    Commits between authors so concurrent reads stay responsive.
    """

    from alma.api.deps import open_db_connection

    conn = open_db_connection()
    try:
        model = semantic_scholar.S2_SPECTER2_MODEL
        cutoff_iso = (
            datetime.now(timezone.utc) - timedelta(days=_CENTROID_STALE_DAYS)
        ).isoformat()
        rows = conn.execute(
            """
            SELECT DISTINCT lower(a.openalex_id) AS oid
            FROM authors a
            LEFT JOIN author_centroids ac
              ON ac.author_openalex_id = lower(a.openalex_id)
             AND ac.model = ?
            WHERE COALESCE(TRIM(a.openalex_id), '') <> ''
              AND (ac.author_openalex_id IS NULL OR ac.updated_at < ?)
            ORDER BY a.openalex_id
            """ + (" LIMIT ?" if limit else ""),
            (model, cutoff_iso, limit) if limit else (model, cutoff_iso),
        ).fetchall()
    finally:
        conn.close()

    candidates = [str(r["oid"]) for r in rows if r["oid"]]
    total = len(candidates)

    # Pre-flight: pipe-filter every candidate's profile into ONE cache so each
    # per-author run below skips its own `fetch_author_profile` roundtrip
    # (mirrors `_deep_refresh_all_impl`'s pre-fetch — ceil(N/50) batched calls
    # instead of N per-author calls). Failure is non-fatal: on a cache miss
    # `refresh_author_works_and_vectors` falls back to the per-author fetch.
    profile_cache: dict = {}
    if candidates:
        try:
            profile_cache = openalex_client.batch_get_author_profiles(
                candidates, batch_size=50, max_workers=4
            )
        except Exception as exc:
            logger.warning(
                "author-works profile pre-fetch failed (falling back per-author): %s",
                exc,
            )
            profile_cache = {}
    summary = {
        "total": total,
        "processed": 0,
        "skipped": 0,
        "papers_new": 0,
        "vectors_fetched": 0,
        "centroids_updated": 0,
        "failures": 0,
        "cancelled": False,
    }
    if ctx is not None:
        try:
            ctx.log_step(
                "start", message=f"Backfilling {total} authors", processed=0, total=total
            )
        except Exception:
            pass

    for idx, oid in enumerate(candidates, start=1):
        if is_cancellation_requested and is_cancellation_requested():
            summary["cancelled"] = True
            break
        try:
            per = refresh_author_works_and_vectors(
                db_path, oid, ctx=None, profile_cache=profile_cache
            )
        except Exception as exc:
            logger.warning("author backfill failed for %s: %s", oid, exc)
            summary["failures"] += 1
            continue
        summary["processed"] += 1
        if per.get("skipped"):
            summary["skipped"] += 1
        summary["papers_new"] += int(per.get("papers_new") or 0)
        summary["vectors_fetched"] += int(per.get("vectors_fetched") or 0)
        if per.get("centroid_updated"):
            summary["centroids_updated"] += 1
        if ctx is not None:
            try:
                ctx.log_step(
                    "progress",
                    message=f"Processed {idx}/{total}",
                    processed=idx,
                    total=total,
                )
            except Exception:
                pass
    return summary


# -- helpers ---------------------------------------------------------

def _upsert_work(
    conn: sqlite3.Connection, work: dict, *, now: str
) -> tuple[str | None, bool]:
    """Upsert one OpenAlex work into `papers`. Returns (paper_id, is_new).

    Delegates to `openalex.client._upsert_single_paper` so every
    OpenAlex paper insert in the codebase runs through the same
    collision-safe pipeline: boundary-normalize blank and URL-form
    identifiers, dedup via `resolve_existing_paper_id`, use
    `INSERT OR IGNORE`, and rescue `IntegrityError` on both INSERT and
    UPDATE paths (D-AUDIT-10 Phase B + 2026-04-25 follow-up).  Prior
    to this delegation `author_backfill.py` kept its own UPDATE
    without either the URL-form normalization or the partial-UNIQUE
    twin check, which surfaced as
    `UNIQUE constraint failed: papers.openalex_id` on the 2026-04-24
    single-author deep refresh.  The `is_new` flag is derived from a
    cheap pre-call `resolve_existing_paper_id` lookup.
    """
    from alma.openalex.client import (
        _ensure_schema,
        _normalize_openalex_work_id,
        _upsert_single_paper,
    )

    title = str(work.get("title") or "").strip()
    if not title:
        return None, False

    oa_norm = _normalize_openalex_work_id(str(work.get("openalex_id") or "").strip()) or ""
    doi_norm = normalize_doi(str(work.get("doi") or "").strip()) or ""
    year_raw = work.get("year")
    try:
        year = int(year_raw) if year_raw is not None and str(year_raw).strip() else None
    except (TypeError, ValueError):
        year = None

    pre_existing = resolve_existing_paper_id(
        conn, openalex_id=oa_norm, doi=doi_norm, title=title, year=year
    )
    _ensure_schema(conn)
    paper_id = _upsert_single_paper(conn, work)
    if paper_id is None:
        return None, False
    return str(paper_id), pre_existing is None


def _ensure_authorship_row(
    conn: sqlite3.Connection,
    *,
    paper_id: str,
    openalex_id: str,
    display_name: str,
    work: dict,
) -> None:
    """Insert a `publication_authors` row linking this paper to the author."""

    oid = str(openalex_id or "").strip()
    if not oid:
        return
    # Prefer the structured authorships entry (it carries the correct
    # display_name for THIS author as listed on THIS paper, which may
    # differ from the profile's canonical name for past-name authors).
    display = display_name
    for ap in work.get("authorships") or []:
        if str(ap.get("openalex_id") or "").strip().lower() == oid.lower():
            candidate = str(ap.get("display_name") or "").strip()
            if candidate:
                display = candidate
            break
    conn.execute(
        """
        INSERT OR IGNORE INTO publication_authors
            (paper_id, openalex_id, display_name)
        VALUES (?, ?, ?)
        """,
        (paper_id, oid, display or ""),
    )


__all__ = [
    "refresh_author_works_and_vectors",
    "backfill_all_resolved_authors",
    "refresh_author_centroid",
    "refresh_centroids_for_papers",
    "seed_papers_for_author",
    "count_local_papers_for_author",
    "SEED_TARGET_PAPERS",
]
