"""Title-search-based identity resolution. OpenAlex first, S2 fallback.

Two-tier strategy (revised 2026-05-08):

1. **OpenAlex** ``/works?search=<title>`` first. The polite pool runs
   at ~10 RPS — 10× S2's ``/paper/search`` cap — and OpenAlex indexes
   a much larger long tail than S2 for non-STEM venues. On a
   high-confidence Jaccard match we fill ``openalex_id`` and ``doi``
   back into ``papers`` (fill-only — never overwrite a curated value).
   The next ``/paper/batch`` sweep then picks up the SPECTER2 vector
   cleanly using the new DOI.
2. **S2 ``/paper/search`` fallback** only when OpenAlex misses or
   returns nothing above the Jaccard + year-delta threshold. Capped
   at ``S2_FALLBACK_PER_RUN_BUDGET`` (50 calls per outer run — the
   S2 1 RPS reality) so an OpenAlex-cold corpus can't blow S2's
   quota in one shot. Free-data side effect when S2 hits: the FIELDS
   projection includes SPECTER2 embeddings, so the vector is
   captured here too — same response, no extra HTTP.

Eligibility (the SELECT): papers with a non-empty title that either
lack a usable identity (no ``semantic_scholar_id`` and no DOI) or
carry a terminal ``unmatched`` / ``bad_local_doi`` fetch_status row
for the active SPECTER2 model.

Self-rescheduling: each invocation processes at most
``TITLE_RESOLUTION_PER_RUN_BUDGET`` (500) papers. When eligible
candidates remain after this run AND we made progress AND we weren't
cancelled AND ``continuation_depth < _MAX_CONTINUATION_DEPTH``, the
runner queues a fresh continuation job with the same ``operation_key``
and the parent's ``trigger_source``. One user click can therefore
drain a backlog of arbitrary size without being killed by a uvicorn
reload — each outer chunk is short (~1 minute), and the next
continuation picks up where the eligibility query left off.

The trigger ``papers_clear_fetch_status_on_id_change`` (defined in
``api/deps.py``) drops the stale ``unmatched`` ledger row when DOI
or ``semantic_scholar_id`` changes; we also explicitly clear the
row at the end of every successful resolve as a belt-and-suspenders
for the openalex_id-only case (the trigger keys on doi + s2_id, not
openalex_id).
"""

from __future__ import annotations

import logging
import re
import sqlite3
from datetime import datetime
from typing import Callable, Optional

from alma.ai.embedding_sources import EMBEDDING_SOURCE_SEMANTIC_SCHOLAR
from alma.core.db_write import write_section
from alma.core.utils import canonical_lookup_doi
from alma.discovery import semantic_scholar
from alma.openalex.client import _normalize_openalex_work_id

logger = logging.getLogger(__name__)

_TITLE_TOKEN_RE = re.compile(r"[a-z0-9]+")

# Match threshold. Jaccard token-set on lowercased alpha-numeric
# tokens. Tight enough that a clean title-only match almost certainly
# identifies the same work, while leaving room for differing
# punctuation, articles, or acronym expansions.
TITLE_RESOLUTION_JACCARD_THRESHOLD = 0.92
TITLE_RESOLUTION_YEAR_DELTA = 1
TITLE_RESOLUTION_MAX_RESULTS = 3
TITLE_RESOLUTION_QUERY_MAX_CHARS = 200
# Per outer-run cap on OpenAlex `/works?search=` calls — the polite
# pool's ~10 RPS makes 500 calls land in ~50 s wall-clock.
TITLE_RESOLUTION_PER_RUN_BUDGET = 500
# Per outer-run cap on S2 `/paper/search` fallback calls — the
# endpoint runs at 1 RPS even with an API key, so 50 calls = ~52 s
# at the floor. An OpenAlex-cold corpus that misses every OpenAlex
# call still can't blow S2's quota in one outer run.
S2_FALLBACK_PER_RUN_BUDGET = 50
# Self-rescheduling depth cap. 50 outer runs × 500 papers = 25 000
# papers per click — generous for any realistic backlog and bounded
# so a stuck loop can't run away.
_MAX_CONTINUATION_DEPTH = 50


def _title_tokens(title: str) -> frozenset[str]:
    return frozenset(_TITLE_TOKEN_RE.findall((title or "").lower()))


def _jaccard(a: frozenset[str], b: frozenset[str]) -> float:
    if not a or not b:
        return 0.0
    union = a | b
    return len(a & b) / len(union) if union else 0.0


def _accept_match(
    local_title: str,
    local_year: Optional[int],
    cand_title: str,
    cand_year: Optional[int],
) -> tuple[bool, float]:
    """Score a candidate by Jaccard + year-delta. Returns (accept, score).

    Public helper — also imported by ``corpus_rehydrate.py`` so the
    rehydration-side title-resolution flow uses the same threshold
    semantics as the standalone sweep.
    """
    if not cand_title:
        return False, 0.0
    score = _jaccard(_title_tokens(local_title), _title_tokens(cand_title))
    if score < TITLE_RESOLUTION_JACCARD_THRESHOLD:
        return False, score
    if (
        local_year is not None
        and cand_year is not None
        and abs(local_year - cand_year) > TITLE_RESOLUTION_YEAR_DELTA
    ):
        return False, score
    return True, score


def _pick_best_candidate(
    candidates: list[dict],
    *,
    title: str,
    local_year: Optional[int],
    title_key: str,
    year_key: str,
) -> tuple[Optional[dict], float]:
    """Return ``(best_candidate, score)`` from a candidate list.

    ``title_key`` and ``year_key`` adapt for the source-specific shapes
    (OpenAlex: ``display_name`` / ``publication_year``; S2: ``title`` /
    ``year``). Returns ``(None, 0.0)`` when nothing clears the
    Jaccard + year-delta thresholds.
    """
    best: Optional[dict] = None
    best_score = 0.0
    for cand in candidates:
        cand_title = str(cand.get(title_key) or "").strip()
        cand_year_raw = cand.get(year_key)
        try:
            cand_year = int(cand_year_raw) if cand_year_raw is not None else None
        except (TypeError, ValueError):
            cand_year = None
        accept, score = _accept_match(title, local_year, cand_title, cand_year)
        if accept and score > best_score:
            best = cand
            best_score = score
    return best, best_score


def _outcome(
    *,
    resolved: bool = False,
    vector_stored: bool = False,
    jaccard: float = 0.0,
    reason: str,
    source: str,
) -> dict:
    """Build a uniformly-shaped outcome dict for a single resolve attempt."""
    return {
        "resolved": resolved,
        "vector_stored": vector_stored,
        "jaccard": jaccard,
        "reason": reason,
        "source": source,
    }


# Eligibility predicate shared by the runner's SELECT and the
# remaining-count helper. Two ? placeholders for the ON clause
# (model, source) and two more for the NOT EXISTS subquery
# (model, source) — callers must bind those four positionally
# whenever they interpolate this fragment.
_ELIGIBILITY_FROM_WHERE = """
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
"""


def _try_openalex_match(
    conn: sqlite3.Connection,
    *,
    row: sqlite3.Row,
    title: str,
    local_year: Optional[int],
) -> dict:
    """Try OpenAlex ``/works?search=`` for one paper.

    Returns ``{"resolved": bool, "vector_stored": bool, "jaccard": float,
    "reason": str, "source": "openalex"}``. On accept, fill-only writes
    the resolved ``openalex_id`` and ``doi`` back to the local row.
    OpenAlex doesn't expose SPECTER2 embeddings, so ``vector_stored``
    is always False on this path — the next vector sweep handles it.
    """
    from alma.core.paper_updates import fill_only_update_paper
    from alma.library.enrichment import _search_work_candidates

    paper_id = str(row["id"])

    try:
        candidates = _search_work_candidates(
            title[:TITLE_RESOLUTION_QUERY_MAX_CHARS],
            per_page=TITLE_RESOLUTION_MAX_RESULTS,
        )
    except Exception as exc:
        logger.debug("OpenAlex title search failed for %s: %s", paper_id, exc)
        return _outcome(reason="openalex_search_error", source="openalex")

    if not candidates:
        return _outcome(reason="openalex_no_results", source="openalex")

    best, best_score = _pick_best_candidate(
        candidates,
        title=title,
        local_year=local_year,
        title_key="display_name",
        year_key="publication_year",
    )
    if best is None:
        return _outcome(
            reason="openalex_no_match_above_threshold", source="openalex"
        )

    oa_id_raw = str(best.get("id") or "").strip()
    oa_id = _normalize_openalex_work_id(oa_id_raw) if oa_id_raw else ""
    new_doi = canonical_lookup_doi(str(best.get("doi") or "")) or ""

    fill_fields: dict[str, str] = {}
    if oa_id:
        fill_fields["openalex_id"] = oa_id
    if new_doi:
        fill_fields["doi"] = new_doi
    if fill_fields:
        # Gated write: the OpenAlex search above already ran (network OUTSIDE the
        # gate). BEGIN IMMEDIATE + writer gate instead of a raw DEFERRED commit
        # that could lose the lock-upgrade race against a concurrent writer.
        with write_section(conn, label="title resolution: openalex fill"):
            fill_only_update_paper(conn, paper_id, fill_fields=fill_fields)

    return _outcome(
        resolved=True,
        jaccard=round(best_score, 4),
        reason="openalex_title_match",
        source="openalex",
    )


def _try_s2_search_match(
    conn: sqlite3.Connection,
    *,
    row: sqlite3.Row,
    title: str,
    local_year: Optional[int],
    model: str,
    job_id: str,
    add_job_log: Callable[..., None],
) -> dict:
    """Try S2 ``/paper/search`` for one paper.

    Returns ``{"resolved": bool, "vector_stored": bool, "jaccard": float,
    "reason": str, "source": "s2"}``. On accept, fill-only writes the
    resolved ``semantic_scholar_id`` / ``doi`` / ``abstract``. Captures
    the SPECTER2 vector when the search response carries it (free
    data, no extra HTTP).
    """
    from alma.core.paper_updates import fill_only_update_paper

    paper_id = str(row["id"])

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
            "S2 fallback deferred by rate limit",
            level="WARNING",
            step="s2_rate_limited",
            data={
                "paper_id": paper_id,
                "status_code": getattr(exc, "status_code", None),
            },
        )
        return _outcome(reason="rate_limited", source="s2")
    except Exception as exc:
        add_job_log(
            job_id,
            "S2 fallback raised an exception",
            level="WARNING",
            step="s2_search_error",
            data={"paper_id": paper_id, "error": str(exc)},
        )
        return _outcome(reason="s2_search_error", source="s2")

    if not candidates:
        return _outcome(reason="s2_no_results", source="s2")

    best, best_score = _pick_best_candidate(
        candidates,
        title=title,
        local_year=local_year,
        title_key="title",
        year_key="year",
    )
    if best is None:
        return _outcome(reason="s2_no_match_above_threshold", source="s2")

    new_s2_id = str(best.get("semantic_scholar_id") or "").strip()
    new_doi = canonical_lookup_doi(str(best.get("doi") or "")) or ""
    new_abstract = str(best.get("abstract") or "").strip()

    # One gated write window for the fill + the SPECTER2 vector. The S2 search
    # above already ran, and the vector rode in on that response (no further
    # network), so the writer lock is held only for these local upserts.
    vector_stored = False
    with write_section(conn, label="title resolution: s2 fill"):
        fill_only_update_paper(
            conn,
            paper_id,
            fill_fields={
                "semantic_scholar_id": new_s2_id,
                "doi": new_doi,
                "abstract": new_abstract,
            },
        )

        vector = best.get("specter2_embedding")
        if isinstance(vector, list) and vector:
            try:
                vector_stored = semantic_scholar.upsert_specter2_vector(
                    conn,
                    paper_id,
                    vector,
                    source=EMBEDDING_SOURCE_SEMANTIC_SCHOLAR,
                    created_at=datetime.utcnow().isoformat(),
                )
            except Exception as exc:
                logger.warning(
                    "S2 fallback vector store failed for %s: %s", paper_id, exc
                )

    return _outcome(
        resolved=True,
        vector_stored=vector_stored,
        jaccard=round(best_score, 4),
        reason="s2_title_match",
        source="s2",
    )


def _clear_terminal_status_row(
    conn: sqlite3.Connection, *, paper_id: str, model: str
) -> None:
    """Belt-and-suspenders: drop any stale terminal status row.

    The trigger ``papers_clear_fetch_status_on_id_change`` fires on
    DOI / s2_id changes, so the openalex_id-only resolution path needs
    explicit cleanup. Cheap (single DELETE keyed on PK).
    """
    conn.execute(
        """
        DELETE FROM publication_embedding_fetch_status
        WHERE paper_id = ? AND model = ? AND source = ?
        """,
        (paper_id, model, EMBEDDING_SOURCE_SEMANTIC_SCHOLAR),
    )


def _count_remaining_eligible(conn: sqlite3.Connection, model: str) -> int:
    """Count papers still eligible for title resolution after this run.

    Used by the self-rescheduling decision at the end of each outer
    run. Mirrors the eligibility predicate in the runner's SELECT
    minus the LIMIT.
    """
    row = conn.execute(
        f"SELECT COUNT(*) AS c {_ELIGIBILITY_FROM_WHERE}",
        (
            model,
            EMBEDDING_SOURCE_SEMANTIC_SCHOLAR,
            model,
            EMBEDDING_SOURCE_SEMANTIC_SCHOLAR,
        ),
    ).fetchone()
    return int(row["c"]) if row else 0


def _active_model(conn: sqlite3.Connection) -> str:
    from alma.discovery.similarity import get_active_embedding_model

    return get_active_embedding_model(conn)


def count_remaining_eligible(conn: sqlite3.Connection, model: Optional[str] = None) -> int:
    """Public count of papers still eligible for title resolution.

    The Health ``identity.unresolved`` dimension count, the maintenance op's
    pending count, and this op's drilldown ALL share ONE definition (the
    ``_ELIGIBILITY_FROM_WHERE`` predicate) so the dimension reconciles with both
    its repair op and its drilldown. Defaults to the active embedding model.
    """
    return _count_remaining_eligible(conn, model or _active_model(conn))


def list_remaining_eligible(
    conn: sqlite3.Connection,
    model: Optional[str] = None,
    *,
    limit: int = 20,
    offset: int = 0,
) -> list[sqlite3.Row]:
    """Paginated papers eligible for title resolution — the drilldown behind the
    Health ``identity.unresolved`` dimension.

    Uses the SAME ``_ELIGIBILITY_FROM_WHERE`` as the count + the runner, so the
    drilldown shows exactly the population the count reports and the op processes
    (H-1 reconciliation). Returns rows shaped for the health drilldown
    (``paper_id``/title/date/authors/status/doi/openalex_id + the fetch status).
    """
    mdl = model or _active_model(conn)
    limit = max(1, min(int(limit), 100))
    offset = max(0, int(offset))
    sql = f"""
        SELECT p.id AS paper_id, p.title, p.publication_date, p.authors, p.status,
               p.doi, p.openalex_id, COALESCE(fs.status, '') AS resolution_status
        {_ELIGIBILITY_FROM_WHERE}
        ORDER BY COALESCE(p.publication_date, '') DESC, p.title
        LIMIT ? OFFSET ?
    """
    return conn.execute(
        sql,
        (mdl, EMBEDDING_SOURCE_SEMANTIC_SCHOLAR, mdl, EMBEDDING_SOURCE_SEMANTIC_SCHOLAR, limit, offset),
    ).fetchall()


def run_title_resolution_sweep(
    job_id: str,
    *,
    limit: int = 500,
    set_job_status: Callable[..., None],
    add_job_log: Callable[..., None],
    is_cancellation_requested: Callable[[str], bool],
    continuation_depth: int = 0,
) -> None:
    """Resolve paper identity via OpenAlex (first) then S2 (fallback).

    Eligibility: papers with a non-empty title that either lack a
    usable identity (no ``semantic_scholar_id`` and no DOI) or carry
    a terminal ``unmatched`` / ``bad_local_doi`` fetch_status row for
    the active SPECTER2 model. Bounded per outer run by
    ``min(limit, TITLE_RESOLUTION_PER_RUN_BUDGET)`` papers
    (OpenAlex calls) and ``S2_FALLBACK_PER_RUN_BUDGET`` (S2 calls).
    Self-reschedules a continuation job when more remain — see module
    docstring for the rationale.
    """
    from alma.api.deps import open_db_connection

    conn = open_db_connection()
    model = semantic_scholar.S2_SPECTER2_MODEL
    try:
        budget = max(1, min(int(limit or 500), TITLE_RESOLUTION_PER_RUN_BUDGET))

        rows = conn.execute(
            f"""
            SELECT p.id, p.title, p.year, p.doi, p.semantic_scholar_id
            {_ELIGIBILITY_FROM_WHERE}
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
            message=(
                f"Resolving identity for {total} papers "
                f"(OpenAlex first, S2 fallback)"
            ),
        )
        add_job_log(
            job_id,
            "Prepared title-resolution sweep",
            step="prepare",
            data={
                "papers": total,
                "openalex_budget": budget,
                "s2_fallback_budget": S2_FALLBACK_PER_RUN_BUDGET,
                "continuation_depth": continuation_depth,
            },
        )

        processed = 0
        resolved_via_openalex = 0
        resolved_via_s2 = 0
        vectors_captured = 0
        s2_fallback_calls = 0
        s2_rate_limited = False

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

            paper_id = str(row["id"])
            title = str(row["title"] or "").strip()
            try:
                local_year = int(row["year"]) if row["year"] is not None else None
            except (TypeError, ValueError):
                local_year = None

            if not title:
                processed += 1
                continue

            outcome = _try_openalex_match(
                conn, row=row, title=title, local_year=local_year,
            )

            # S2 fallback only when OpenAlex didn't resolve. Bounded by
            # S2_FALLBACK_PER_RUN_BUDGET + the rate-limit short-circuit.
            # Skip silently when neither condition allows; the next
            # sweep retries.
            if not outcome["resolved"]:
                if (
                    not s2_rate_limited
                    and s2_fallback_calls < S2_FALLBACK_PER_RUN_BUDGET
                ):
                    s2_fallback_calls += 1
                    s2_outcome = _try_s2_search_match(
                        conn,
                        row=row,
                        title=title,
                        local_year=local_year,
                        model=model,
                        job_id=job_id,
                        add_job_log=add_job_log,
                    )
                    if s2_outcome["reason"] == "rate_limited":
                        s2_rate_limited = True
                    elif s2_outcome["resolved"]:
                        outcome = s2_outcome

            processed += 1

            if outcome["resolved"]:
                if outcome["source"] == "openalex":
                    resolved_via_openalex += 1
                else:
                    resolved_via_s2 += 1
                if outcome["vector_stored"]:
                    vectors_captured += 1
                # Gated cleanup DELETE (no network). The fill/vector writes for
                # this paper were already committed inside the _try_* helper's
                # own write_section; this drops any stale terminal status row.
                with write_section(conn, label="title resolution: clear terminal status"):
                    _clear_terminal_status_row(conn, paper_id=paper_id, model=model)

            # set_job_status writes on the SCHEDULER's own connection — it runs
            # here with NO write_section held (the gate above has closed), so it
            # never blocks on the writer lock this thread is holding.
            set_job_status(
                job_id,
                status="running",
                processed=processed,
                total=total,
                message=(
                    f"Title resolution: openalex={resolved_via_openalex}, "
                    f"s2_fallback={resolved_via_s2}, "
                    f"vectors={vectors_captured}"
                ),
            )

        # No final commit: every resolved paper's writes were committed inside
        # their own write_section as the loop ran (gather-from-network → gated
        # write), so nothing is left pending here.

        # Self-rescheduling decision. Queue a continuation when:
        # - we made progress (avoid infinite loops on a stuck corpus)
        # - more eligible candidates remain (work to do)
        # - depth cap not tripped (runaway guard)
        # - not cancelled (don't re-fire after Cancel)
        resolved = resolved_via_openalex + resolved_via_s2
        remaining = _count_remaining_eligible(conn, model)
        cancelled = is_cancellation_requested(job_id)
        # One total budget across continuations: ``limit`` carries the REMAINING
        # session budget (the first run gets the user's full limit; each
        # continuation is handed limit−processed), so the whole logical run can
        # never resolve more than the original limit. Previously the continuation
        # re-passed the per-run budget, resetting the cap on every chunk.
        remaining_session = max(0, int(limit or 500) - processed)
        will_continue = (
            not cancelled
            and resolved > 0
            and remaining > 0
            and remaining_session > 0
            and continuation_depth < _MAX_CONTINUATION_DEPTH
        )

        result_data: dict = {
            "processed": processed,
            "resolved_via_openalex": resolved_via_openalex,
            "resolved_via_s2_fallback": resolved_via_s2,
            "vectors_captured": vectors_captured,
            "s2_fallback_calls": s2_fallback_calls,
            "s2_rate_limited": s2_rate_limited,
            "remaining_eligible": remaining,
            "remaining_session_budget": remaining_session,
            "continuation_depth": continuation_depth,
            "model": model,
        }

        if will_continue:
            from uuid import uuid4

            from alma.api.scheduler import (
                get_job_status,
                get_job_trigger_source,
                schedule_immediate,
                set_job_status as _set_job_status,
            )
            # Lazy import the route wrapper to avoid the routes ↔
            # services cycle (routes/ai.py imports this module).
            from alma.api.routes.ai import _run_title_resolution_sweep

            parent_source = get_job_trigger_source(job_id) or "auto:continuation"
            parent_status = get_job_status(job_id) or {}
            parent_chain_id = str(parent_status.get("chain_id") or "").strip()
            parent_chain_step = str(parent_status.get("chain_step") or "title_resolution").strip()
            new_job_id = f"title_resolution_{uuid4().hex[:8]}"
            status_kwargs = {
                "status": "queued",
                "operation_key": "ai.title_resolution_sweep",
                "trigger_source": parent_source,
                "message": (
                    f"Title resolution continuation queued "
                    f"({remaining} eligible, depth {continuation_depth + 1})"
                ),
                "started_at": datetime.utcnow().isoformat(),
            }
            if parent_chain_id:
                status_kwargs["chain_id"] = parent_chain_id
                status_kwargs["chain_step"] = parent_chain_step or "title_resolution"
            _set_job_status(new_job_id, **status_kwargs)
            schedule_immediate(
                new_job_id,
                _run_title_resolution_sweep,
                new_job_id,
                remaining_session,
                continuation_depth + 1,
            )
            result_data["continuation_job_id"] = new_job_id
            add_job_log(
                job_id,
                "Title resolution continuation queued",
                step="continuation_queued",
                data={
                    "next_job_id": new_job_id,
                    "remaining": remaining,
                    "depth": continuation_depth + 1,
                    "trigger_source": parent_source,
                    "chain_id": parent_chain_id or None,
                },
            )

        message = (
            f"Title resolution complete: "
            f"openalex={resolved_via_openalex}, "
            f"s2_fallback={resolved_via_s2}, "
            f"vectors={vectors_captured}, "
            f"remaining={remaining}"
        )
        if will_continue:
            message += ", continuation queued"

        set_job_status(
            job_id,
            status="completed",
            processed=processed,
            total=total,
            message=message,
            result=result_data,
            finished_at=datetime.utcnow().isoformat(),
        )
        add_job_log(
            job_id,
            "Title resolution sweep complete",
            step="summary",
            data=result_data,
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
