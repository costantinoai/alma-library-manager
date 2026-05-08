"""Title-search-based identity resolution for Semantic Scholar papers.

Papers without a working ``semantic_scholar_id`` / DOI never receive a
SPECTER2 vector — ``run_s2_vector_backfill`` either skips them outright
(no identity at all) or stamps them ``unmatched`` / ``bad_local_doi``
after S2 returns nothing for the lookup we tried.

This sweep covers the gap. For each paper with a title but no good
identity, we call Semantic Scholar ``/paper/search`` with the local
title and accept the highest-Jaccard candidate above a tight threshold.
On match, we fill the resolved s2_id / DOI / abstract back into the
``papers`` row (fill-only — never overwrite a curated value). The
trigger ``papers_clear_fetch_status_on_id_change`` then drops the
stale ``unmatched`` ledger row, and the next S2 vector sweep picks the
paper up cleanly.

Free-data side effect: ``/paper/search`` already returns SPECTER2
embeddings in the FIELDS projection. When present we capture the
vector here too — same response, no extra HTTP cost — so we don't
have to refetch the same paper from ``/paper/batch`` next sweep.
"""

from __future__ import annotations

import logging
import re
import sqlite3
from datetime import datetime
from typing import Callable

from alma.ai.embedding_sources import EMBEDDING_SOURCE_SEMANTIC_SCHOLAR
from alma.core.utils import canonical_lookup_doi
from alma.core.vector_blob import encode_vector
from alma.discovery import semantic_scholar

logger = logging.getLogger(__name__)

_TITLE_TOKEN_RE = re.compile(r"[a-z0-9]+")

# Match threshold. Jaccard token-set on lowercased alpha-numeric tokens.
# Tight enough that a clean title-only match almost certainly identifies
# the same work, while leaving room for differing punctuation, articles,
# or acronym expansions in the search response.
TITLE_RESOLUTION_JACCARD_THRESHOLD = 0.92
TITLE_RESOLUTION_YEAR_DELTA = 1
TITLE_RESOLUTION_MAX_RESULTS = 3
TITLE_RESOLUTION_QUERY_MAX_CHARS = 200
# Per-run cap on /paper/search calls. S2 throttles /paper/search to
# ~1 RPS even with an API key, so 50 calls = ~52 s at the floor;
# larger backlogs roll over to the next sweep.
TITLE_RESOLUTION_PER_RUN_BUDGET = 50


def _title_tokens(title: str) -> frozenset[str]:
    return frozenset(_TITLE_TOKEN_RE.findall((title or "").lower()))


def _jaccard(a: frozenset[str], b: frozenset[str]) -> float:
    if not a or not b:
        return 0.0
    union = a | b
    if not union:
        return 0.0
    return len(a & b) / len(union)


def _resolve_one_paper(
    conn: sqlite3.Connection,
    *,
    row: sqlite3.Row,
    model: str,
    job_id: str,
    add_job_log: Callable[..., None],
) -> dict:
    """One ``/paper/search`` title-resolution attempt for one paper.

    Returns a dict with keys ``resolved`` (bool), ``vector_stored``
    (bool), ``jaccard`` (float), and ``reason`` (str). Side effects
    on a successful match:

    - fill-only writes of ``semantic_scholar_id`` / ``doi`` / ``abstract``
    - optional fill-only insert of the SPECTER2 vector when the search
      response carries it (free data — no extra HTTP call)
    - explicit clearance of the stale fetch_status row in case the
      trigger ``papers_clear_fetch_status_on_id_change`` didn't fire
      (e.g. local already had the right DOI but no s2_id).
    """
    from alma.core.paper_updates import fill_only_update_paper

    paper_id = str(row["id"])
    title = str(row["title"] or "").strip()
    if not title:
        return {"resolved": False, "vector_stored": False, "jaccard": 0.0, "reason": "no_title"}

    try:
        local_year = int(row["year"]) if row["year"] is not None else None
    except (TypeError, ValueError):
        local_year = None

    try:
        candidates = semantic_scholar.search_papers(
            title[:TITLE_RESOLUTION_QUERY_MAX_CHARS],
            limit=TITLE_RESOLUTION_MAX_RESULTS,
            raise_on_rate_limit=True,
        )
    except semantic_scholar.SemanticScholarBatchError as exc:
        # 429 / transient — defer, don't mark terminal. The eligibility
        # SELECT picks the row up again on the next sweep.
        add_job_log(
            job_id,
            "Title resolution deferred by rate limit",
            level="WARNING",
            step="rate_limited",
            data={
                "paper_id": paper_id,
                "status_code": getattr(exc, "status_code", None),
            },
        )
        return {"resolved": False, "vector_stored": False, "jaccard": 0.0, "reason": "rate_limited"}
    except Exception as exc:
        add_job_log(
            job_id,
            "Title resolution raised an exception",
            level="WARNING",
            step="search_error",
            data={"paper_id": paper_id, "error": str(exc)},
        )
        return {"resolved": False, "vector_stored": False, "jaccard": 0.0, "reason": "search_error"}

    if not candidates:
        return {"resolved": False, "vector_stored": False, "jaccard": 0.0, "reason": "no_results"}

    local_tokens = _title_tokens(title)
    best = None
    best_jaccard = 0.0
    for cand in candidates:
        cand_title = str(cand.get("title") or "").strip()
        if not cand_title:
            continue
        cand_tokens = _title_tokens(cand_title)
        jaccard = _jaccard(local_tokens, cand_tokens)
        if jaccard < TITLE_RESOLUTION_JACCARD_THRESHOLD:
            continue
        cand_year_raw = cand.get("year")
        try:
            cand_year = int(cand_year_raw) if cand_year_raw is not None else None
        except (TypeError, ValueError):
            cand_year = None
        if (
            local_year is not None
            and cand_year is not None
            and abs(local_year - cand_year) > TITLE_RESOLUTION_YEAR_DELTA
        ):
            continue
        if jaccard > best_jaccard:
            best = cand
            best_jaccard = jaccard

    if best is None:
        return {
            "resolved": False,
            "vector_stored": False,
            "jaccard": 0.0,
            "reason": "no_match_above_threshold",
        }

    new_s2_id = str(best.get("semantic_scholar_id") or "").strip()
    new_doi = canonical_lookup_doi(str(best.get("doi") or "")) or ""
    new_abstract = str(best.get("abstract") or "").strip()

    # Fill-only writes: never overwrite a hand-curated DOI just because
    # S2 returned a different one. The id-change trigger fires only if
    # a column actually changed.
    fill_only_update_paper(
        conn,
        paper_id,
        fill_fields={
            "semantic_scholar_id": new_s2_id,
            "doi": new_doi,
            "abstract": new_abstract,
        },
    )

    vector_stored = False
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
            vector_stored = cursor.rowcount > 0
        except Exception as exc:
            logger.warning(
                "Title-resolution vector store failed for %s: %s", paper_id, exc
            )
            vector_stored = False

    # Belt-and-suspenders: if neither id changed (so the trigger didn't
    # fire) the leftover terminal fetch_status row would still trap the
    # paper. Clear it explicitly.
    conn.execute(
        """
        DELETE FROM publication_embedding_fetch_status
        WHERE paper_id = ? AND model = ? AND source = ?
        """,
        (paper_id, model, EMBEDDING_SOURCE_SEMANTIC_SCHOLAR),
    )

    return {
        "resolved": True,
        "vector_stored": vector_stored,
        "jaccard": round(best_jaccard, 4),
        "reason": "title_match",
    }


def run_title_resolution_sweep(
    job_id: str,
    *,
    limit: int = 50,
    set_job_status: Callable[..., None],
    add_job_log: Callable[..., None],
    is_cancellation_requested: Callable[[str], bool],
) -> None:
    """Resolve paper identity via Semantic Scholar ``/paper/search``.

    Eligibility: papers with a non-empty title that either lack a
    usable identity (no ``semantic_scholar_id`` and no DOI) or carry a
    terminal ``unmatched`` / ``bad_local_doi`` fetch_status row for the
    active SPECTER2 model. Capped at
    ``min(limit, TITLE_RESOLUTION_PER_RUN_BUDGET)`` actual
    ``/paper/search`` calls per run; the first 429 short-circuits the
    rest of the run so we don't keep firing into a known-overloaded
    upstream.
    """
    from alma.api.deps import open_db_connection

    conn = open_db_connection()
    model = semantic_scholar.S2_SPECTER2_MODEL
    try:
        budget = max(1, min(int(limit or 50), TITLE_RESOLUTION_PER_RUN_BUDGET))

        rows = conn.execute(
            """
            SELECT p.id, p.title, p.year, p.doi, p.semantic_scholar_id
            FROM papers p
            LEFT JOIN publication_embedding_fetch_status fs
              ON fs.paper_id = p.id
             AND fs.model = ?
             AND fs.source = ?
            WHERE NULLIF(TRIM(p.title), '') IS NOT NULL
              AND NOT EXISTS (
                  SELECT 1 FROM publication_embeddings pe
                  WHERE pe.paper_id = p.id
                    AND pe.model = ?
                    AND pe.source = ?
              )
              AND (
                  COALESCE(fs.status, '') IN ('unmatched', 'bad_local_doi')
                  OR (
                      COALESCE(NULLIF(TRIM(p.semantic_scholar_id), ''), '') = ''
                      AND COALESCE(NULLIF(TRIM(p.doi), ''), '') = ''
                  )
              )
            LIMIT ?
            """,
            (
                model,
                EMBEDDING_SOURCE_SEMANTIC_SCHOLAR,
                model,
                EMBEDDING_SOURCE_SEMANTIC_SCHOLAR,
                budget,
            ),
        ).fetchall()

        total = len(rows)
        if total == 0:
            set_job_status(
                job_id,
                status="completed",
                processed=0,
                total=0,
                message="No papers need title resolution",
                finished_at=datetime.utcnow().isoformat(),
            )
            return

        set_job_status(
            job_id,
            status="running",
            processed=0,
            total=total,
            message=f"Resolving identity for {total} papers via S2 /paper/search",
        )

        add_job_log(
            job_id,
            "Prepared title-resolution sweep",
            step="prepare",
            data={"papers": total, "budget": budget},
        )

        processed = 0
        resolved = 0
        vectors_captured = 0
        errors = 0
        rate_limited = False

        for row in rows:
            if is_cancellation_requested(job_id):
                set_job_status(
                    job_id,
                    status="cancelled",
                    processed=processed,
                    total=total,
                    message="Title resolution cancelled",
                    finished_at=datetime.utcnow().isoformat(),
                )
                return
            if rate_limited:
                # Don't keep firing into a rate-limited /paper/search;
                # let the adaptive throttle drain. Remaining rows roll
                # over to the next sweep automatically.
                break

            outcome = _resolve_one_paper(
                conn,
                row=row,
                model=model,
                job_id=job_id,
                add_job_log=add_job_log,
            )
            processed += 1
            if outcome["reason"] == "rate_limited":
                rate_limited = True
                continue
            if outcome["reason"] == "search_error":
                errors += 1
                continue
            if outcome["resolved"]:
                resolved += 1
                if outcome["vector_stored"]:
                    vectors_captured += 1
                conn.commit()

            set_job_status(
                job_id,
                status="running",
                processed=processed,
                total=total,
                errors=errors,
                message=(
                    f"Title resolution: resolved {resolved}, "
                    f"vectors captured {vectors_captured}, errors {errors}"
                ),
            )

        conn.commit()

        set_job_status(
            job_id,
            status="completed",
            processed=processed,
            total=total,
            errors=errors,
            message=(
                f"Title resolution complete: resolved {resolved}, "
                f"vectors captured {vectors_captured}, errors {errors}, "
                f"rate_limited {rate_limited}"
            ),
            result={
                "processed": processed,
                "resolved": resolved,
                "vectors_captured": vectors_captured,
                "errors": errors,
                "rate_limited": rate_limited,
                "model": model,
            },
            finished_at=datetime.utcnow().isoformat(),
        )
        add_job_log(
            job_id,
            "Title resolution sweep complete",
            step="summary",
            data={
                "processed": processed,
                "resolved": resolved,
                "vectors_captured": vectors_captured,
                "errors": errors,
                "rate_limited": rate_limited,
            },
        )
    except Exception as exc:
        logger.exception("Title resolution sweep failed: %s", exc)
        set_job_status(
            job_id,
            status="failed",
            message=f"Title resolution sweep failed: {exc}",
            finished_at=datetime.utcnow().isoformat(),
        )
    finally:
        conn.close()
