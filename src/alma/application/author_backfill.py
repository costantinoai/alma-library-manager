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
from datetime import datetime, timedelta, timezone
from typing import Any, Callable, Optional

from alma.ai.embedding_sources import EMBEDDING_SOURCE_SEMANTIC_SCHOLAR
from alma.core.utils import normalize_doi, resolve_existing_paper_id
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
    model: Optional[str] = None,
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
            """
            SELECT pe.embedding AS embedding
            FROM publication_authors pa
            JOIN publication_embeddings pe
              ON pe.paper_id = pa.paper_id AND pe.model = ?
            WHERE lower(pa.openalex_id) = ?
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
    model: Optional[str] = None,
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


# -- backfill runner (per author) ------------------------------------

def refresh_author_works_and_vectors(
    db_path: str,
    author_openalex_id: str,
    *,
    ctx: Optional[Any] = None,
    full_refetch: bool = False,
    profile_cache: Optional[dict] = None,
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
            # still refresh centroid — embeddings may have just arrived
            summary["centroid_updated"] = refresh_author_centroid(conn, oid_norm)
            conn.commit()
            return summary

        # Phase 2: paginate through all works.
        cursor: Optional[str] = "*"
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
            # Release writer lock between HTTP calls (lesson: commit
            # before every remote call on bulk jobs).
            if conn.in_transaction:
                conn.commit()
            cursor = page.get("next_cursor")

        # Phase 3: upsert each work + publication_authors row.
        now_iso = datetime.now(timezone.utc).isoformat()
        new_paper_ids: list[str] = []
        for work in works:
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
        if conn.in_transaction:
            conn.commit()

        # Phase 4: identify papers still missing an active-model
        # SPECTER2 vector, then batch-fetch via Semantic Scholar.
        try:
            from alma.discovery.similarity import get_active_embedding_model

            model = get_active_embedding_model(conn) or semantic_scholar.S2_SPECTER2_MODEL
        except Exception:
            model = semantic_scholar.S2_SPECTER2_MODEL

        pending = conn.execute(
            """
            SELECT p.id AS paper_id, p.doi AS doi, p.semantic_scholar_id AS s2_id
            FROM publication_authors pa
            JOIN papers p ON p.id = pa.paper_id
            LEFT JOIN publication_embeddings pe
              ON pe.paper_id = p.id AND pe.model = ?
            WHERE lower(pa.openalex_id) = ?
              AND pe.paper_id IS NULL
              AND (
                   COALESCE(NULLIF(TRIM(p.doi), ''), '') <> ''
                OR COALESCE(NULLIF(TRIM(p.semantic_scholar_id), ''), '') <> ''
              )
            """,
            (model, oid_norm.lower()),
        ).fetchall()

        lookups: list[tuple[str, str]] = []  # (paper_id, lookup_id)
        for row in pending:
            paper_id = str(row["paper_id"])
            s2_id = str(row["s2_id"] or "").strip()
            doi = normalize_doi(str(row["doi"] or "")) or str(row["doi"] or "").strip()
            if s2_id:
                lookups.append((paper_id, s2_id))
            elif doi:
                lookups.append((paper_id, f"DOI:{doi}"))

        if lookups:
            _log(
                "fetch_vectors",
                f"Fetching SPECTER2 vectors for {len(lookups)} papers",
                processed=0,
                total=len(lookups),
            )
            vectors_found = 0
            for chunk_start in range(0, len(lookups), _S2_BATCH_SIZE):
                chunk = lookups[chunk_start:chunk_start + _S2_BATCH_SIZE]
                lookup_ids = [lid for _, lid in chunk]
                batch = semantic_scholar.fetch_papers_batch(
                    lookup_ids,
                    fields=_VECTOR_FIELDS,
                    batch_size=_S2_BATCH_SIZE,
                )
                # Match results back to paper_ids via _requested_id stamp.
                by_lookup = {
                    str(v.get("_requested_id") or "").strip(): v
                    for v in batch.values()
                    if v.get("_requested_id")
                }
                for paper_id, lookup_id in chunk:
                    row = by_lookup.get(lookup_id)
                    if not row:
                        summary["vectors_missing"] += 1
                        continue
                    vec = semantic_scholar.extract_specter2_vector(row)
                    if not vec:
                        summary["vectors_missing"] += 1
                        continue
                    _insert_vector(conn, paper_id, model, vec, source=EMBEDDING_SOURCE_SEMANTIC_SCHOLAR)
                    vectors_found += 1
                if conn.in_transaction:
                    conn.commit()
                _log(
                    "fetch_vectors",
                    f"Vectors: {vectors_found}/{len(lookups)}",
                    processed=min(chunk_start + _S2_BATCH_SIZE, len(lookups)),
                    total=len(lookups),
                )
            summary["vectors_fetched"] = vectors_found

        # Phase 5: recompute centroid.
        _log("centroid", "Recomputing author centroid")
        summary["centroid_updated"] = refresh_author_centroid(
            conn, oid_norm, model=model
        )
        conn.commit()
        return summary
    finally:
        conn.close()


# -- batch variant ---------------------------------------------------

def backfill_all_resolved_authors(
    db_path: str,
    *,
    ctx: Optional[Any] = None,
    limit: Optional[int] = None,
    is_cancellation_requested: Optional[Callable[[], bool]] = None,
) -> dict:
    """Run `refresh_author_works_and_vectors` over every resolved author
    whose centroid is missing or older than 14 days.

    Commits between authors so concurrent reads stay responsive.
    """

    from alma.api.deps import open_db_connection

    conn = open_db_connection()
    try:
        try:
            from alma.discovery.similarity import get_active_embedding_model

            model = get_active_embedding_model(conn) or semantic_scholar.S2_SPECTER2_MODEL
        except Exception:
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
            per = refresh_author_works_and_vectors(db_path, oid, ctx=None)
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
) -> tuple[Optional[str], bool]:
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
    paper_id = _upsert_single_paper(conn, work, _ensure_schema(conn))
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


def _insert_vector(
    conn: sqlite3.Connection,
    paper_id: str,
    model: str,
    vector: list[float],
    *,
    source: str,
) -> None:
    """Insert one SPECTER2 vector blob into publication_embeddings."""

    from alma.core.vector_blob import encode_vector

    conn.execute(
        """
        INSERT INTO publication_embeddings
            (paper_id, model, source, embedding, created_at)
        VALUES (?, ?, ?, ?, ?)
        ON CONFLICT(paper_id, model) DO UPDATE SET
            embedding = excluded.embedding,
            source = excluded.source,
            created_at = excluded.created_at
        """,
        (
            paper_id,
            model,
            source,
            encode_vector(vector),
            datetime.now(timezone.utc).isoformat(),
        ),
    )


__all__ = [
    "refresh_author_works_and_vectors",
    "backfill_all_resolved_authors",
    "refresh_author_centroid",
    "refresh_centroids_for_papers",
]
