"""Title-search-based identity resolution. ADAPTIVE source order.

Two sources, order picked per run (2026-07-04, driven by the OpenAlex
usage-based pricing and a real-corpus benchmark: OpenAlex ``?search`` ≈
0.3 s/paper at $1.00/1k; S2 ``/paper/search`` free but ≈ 6 s/paper under
server-side throttling):

- **User-triggered run with a healthy budget → OpenAlex-first.** The user
  is waiting; the run only picks this order when remaining credits cover
  the whole run plus the user reserve. The OpenAlex primary is ungated; a
  miss advances to the free S2 stage.
- **Background / low-budget run → S2-first.** Free; the paid OpenAlex
  fallback is capped at ``OPENALEX_FALLBACK_PER_RUN_BUDGET`` searches per
  outer run. When the cap is hit the paper is stamped retryable (NOT
  terminal — OpenAlex never saw it) and the next run picks it up.

Either way: on a high-confidence Jaccard match we fill identifiers +
metadata back into ``papers`` (fill-only — never overwrite a curated
value). Free-data side effect on S2 hits: the FIELDS projection includes
SPECTER2 embeddings, so the vector is captured in the same response — no
extra HTTP.

Eligibility (the SELECT): papers with a non-empty title that either
lack a usable identity (no ``semantic_scholar_id`` and no DOI) or
carry a terminal ``unmatched`` / ``bad_local_doi`` fetch_status row
for the active SPECTER2 model.

Decoupled multi-source fetch/write (tasks/11 + tasks/38): the per-paper
loop runs through ``core.fetch_pipeline.run_staged_fetch_pipeline`` as TWO
**independent** source stages — an S2 stage (own pool, 1 RPS) whose MISSES
flow into an OpenAlex fallback stage (own pool, budget-gated) — both
feeding a SINGLE writer (this thread) that batches its ``write_section``
flushes. Because the stages run concurrently, item A's OpenAlex fallback
fetch happens while item B's S2 fetch is still in flight, and an S2 429
never stalls the OpenAlex stage or the writer (task 38). Fetch and write
stay independent channels, so the writer gate is never held across a
network call. Wall-clock is now dominated by S2's 1 RPS (~75 papers per
75 s run); the continuation loop drains larger backlogs across runs — the
trade accepted for making the steady-state sweep free.

Every attempt is stamped in the ``paper_enrichment_status`` ledger
(``title_resolution`` source): resolved → identifiers; genuine no-match →
sticky ``terminal_no_match``; rate-limit/error → TTL'd ``retryable_error``.
The eligibility predicate excludes those, so a stale / non-fetchable title
leaves the pool and is never re-fetched.

Self-rescheduling: each invocation processes at most
``TITLE_RESOLUTION_PER_RUN_BUDGET`` (500) papers OR ``_PER_RUN_SECONDS``
of wall-clock, whichever comes first. When eligible candidates remain AND
we made forward progress (processed > 0 — safe because every attempt is
stamped) AND we weren't cancelled AND ``continuation_depth <
_MAX_CONTINUATION_DEPTH``, the runner queues a continuation with the same
``operation_key`` and the parent's ``trigger_source``. A restart that
orphans a run mid-flight is healed by ``maintenance.resume_orphaned_sweeps``
at startup, so a user-initiated backlog drains without a re-click.

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
import threading
from datetime import datetime, timedelta
from typing import Any, Callable, Optional

from alma.ai.embedding_sources import EMBEDDING_SOURCE_SEMANTIC_SCHOLAR
from alma.core.db_write import write_section
from alma.core.fetch_pipeline import (
    FetchError,
    FetchStage,
    PipelineResult,
    make_deadline,
    run_staged_fetch_pipeline,
)
from alma.core.sql_helpers import standalone_paper_sql
from alma.core.utils import canonical_lookup_doi, utcnow_iso as _utcnow_iso
from alma.discovery import semantic_scholar
from alma.openalex.client import _normalize_openalex_work_id

logger = logging.getLogger(__name__)

# The `paper_enrichment_status` ledger source/purpose this sweep writes an
# outcome row under — the SAME values `corpus_rehydrate` uses for its Phase-0
# title resolver, so the two share one ledger contract (a resolved/no-match
# title stamped by either is honored by both).
_TITLE_RESOLUTION_SOURCE = "title_resolution"
_METADATA_PURPOSE = "metadata"
# Wall-clock budget for one outer run. With a concurrent fetch pool an outer
# run finishes well inside this; the deadline is the belt that caps the
# worst case so a uvicorn `--reload` can only ever orphan ≤ this much work
# (the continuation/resume then drains the rest). See `tasks/11`.
_PER_RUN_SECONDS = 75.0
# This sweep's canonical operation key (job-policy namespace `ai`). Used by the
# task-37 background-yield gate to exclude this op's own row from the
# "another operation is active?" check.
_TITLE_SWEEP_OPERATION_KEY = "ai.title_resolution_sweep"
# Fetch-pool width for the OpenAlex fallback stage. Clamped down to the
# running job's `fanout_budget` by `bounded_thread_pool` (default 4 for the
# `ai` maintenance namespace); the per-run `_FallbackGate` caps total paid
# calls regardless of width.
_FETCH_WORKERS = 8
# Worker width for the PRIMARY S2 stage. S2 ``/paper/search`` is 1 RPS even
# with a key (the source client self-serialises), so a couple of workers is
# plenty to keep the queue draining concurrently with the fallback stage —
# no benefit from going wider.
_S2_FETCH_WORKERS = 2
# Writer flush size: how many fetched results the single writer batches into
# one `write_section` (one BEGIN IMMEDIATE → commit) before yielding.
_WRITE_BATCH_SIZE = 50

_TITLE_TOKEN_RE = re.compile(r"[a-z0-9]+")

# Match threshold. Jaccard token-set on lowercased alpha-numeric
# tokens. Tight enough that a clean title-only match almost certainly
# identifies the same work, while leaving room for differing
# punctuation, articles, or acronym expansions.
TITLE_RESOLUTION_JACCARD_THRESHOLD = 0.92
TITLE_RESOLUTION_YEAR_DELTA = 1
TITLE_RESOLUTION_MAX_RESULTS = 3
TITLE_RESOLUTION_QUERY_MAX_CHARS = 200
# Per outer-run cap on papers attempted (S2 primary calls). S2 is free but
# 1 RPS; the `_PER_RUN_SECONDS` deadline is the effective cap per run and
# the continuation loop drains the rest.
TITLE_RESOLUTION_PER_RUN_BUDGET = 500
# Per outer-run cap on PAID OpenAlex `/works?search=` fallback calls
# ($0.001 each — 100 calls = $0.10 = 10% of the free daily budget). Papers
# past the cap are stamped retryable, not terminal, so the next run
# retries them.
OPENALEX_FALLBACK_PER_RUN_BUDGET = 100
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


# Eligibility predicate shared by the runner's SELECT, the remaining-count
# helper, and the drilldown — so the Health `identity.unresolved` dimension,
# the op's pending count, and the drilldown all report the SAME population
# (H-1 reconciliation). Bind its placeholders ONLY via `_eligibility_params`
# so the order stays correct everywhere:
#   1. (model, source)        — fetch-status LEFT JOIN
#   2. (model, source)        — no-vector NOT EXISTS
#   3. (source, purpose, now) — title-resolution dead-end exclusion
# The dead-end exclusion is what stops the sweep from re-fetching stale,
# non-fetchable titles every run: once a title is stamped `enriched` /
# `terminal_no_match` (sticky) or is inside a `retryable_error` / `unchanged`
# retry window, it leaves the pool. Because all three readers share this
# fragment, excluding dead-ends keeps them reconciled.
_ELIGIBILITY_FROM_WHERE = f"""
        FROM papers p
        LEFT JOIN publication_embedding_fetch_status fs
          ON fs.paper_id = p.id
         AND fs.model = ?
         AND fs.source = ?
        WHERE {standalone_paper_sql('p')}
          AND NULLIF(TRIM(p.title), '') IS NOT NULL
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
          AND NOT EXISTS (
              SELECT 1 FROM paper_enrichment_status tre
              WHERE tre.paper_id = p.id
                AND tre.source = ?
                AND tre.purpose = ?
                AND (
                    tre.status IN ('enriched', 'terminal_no_match')
                    OR (
                        tre.status IN ('retryable_error', 'unchanged')
                        AND tre.next_retry_at IS NOT NULL
                        AND tre.next_retry_at > ?
                    )
                )
          )
"""


def _eligibility_params(model: str) -> tuple:
    """Positional binds for `_ELIGIBILITY_FROM_WHERE` (see its comment).

    Centralised so the count, the drilldown, and the runner can never drift
    out of bind-order — the bug class the H-1 reconciliation tests guard.
    """
    return (
        model, EMBEDDING_SOURCE_SEMANTIC_SCHOLAR,   # fetch-status join
        model, EMBEDDING_SOURCE_SEMANTIC_SCHOLAR,   # no-vector NOT EXISTS
        _TITLE_RESOLUTION_SOURCE, _METADATA_PURPOSE, _utcnow_iso(),  # dead-end exclusion
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


def _count_remaining_eligible(
    conn: sqlite3.Connection, model: str, *, queued_only: bool = False
) -> int:
    """Count papers still eligible for title resolution after this run.

    Used by the self-rescheduling decision at the end of each outer
    run. Mirrors the eligibility predicate in the runner's SELECT
    minus the LIMIT. ``queued_only`` (41.3) ANDs the shared
    enqueued-never-attempted predicate for the Health "queued" split.
    """
    extra = ""
    if queued_only:
        from alma.services.corpus_rehydrate import queued_metadata_exists_sql

        extra = f" AND {queued_metadata_exists_sql('p')}"
    row = conn.execute(
        f"SELECT COUNT(*) AS c {_ELIGIBILITY_FROM_WHERE}{extra}",
        _eligibility_params(model),
    ).fetchone()
    return int(row["c"]) if row else 0


def _active_model(conn: sqlite3.Connection) -> str:
    from alma.discovery.similarity import get_active_embedding_model

    return get_active_embedding_model(conn)


def count_remaining_eligible(
    conn: sqlite3.Connection, model: Optional[str] = None, *, queued_only: bool = False
) -> int:
    """Public count of papers still eligible for title resolution.

    The Health ``identity.unresolved`` dimension count, the maintenance op's
    pending count, and this op's drilldown ALL share ONE definition (the
    ``_ELIGIBILITY_FROM_WHERE`` predicate) so the dimension reconciles with both
    its repair op and its drilldown. Defaults to the active embedding model.

    ``queued_only`` restricts to the enqueued-but-never-attempted subset (41.3),
    so Health can show "N queued — runs when idle" for a fresh corpus whose
    pipeline simply hasn't reached these papers yet.
    """
    return _count_remaining_eligible(conn, model or _active_model(conn), queued_only=queued_only)


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
        _eligibility_params(mdl) + (limit, offset),
    ).fetchall()


# ----------------------------------------------------------------------
# Fetch / write split for the producer-consumer pipeline (tasks/11).
#
# The FETCH helpers run on the concurrent pool — network ONLY, no DB. The
# WRITE helpers run on the single writer thread inside one ``write_section``.
# This is what decouples the two channels so the writer gate is never held
# across a network call and a 500-paper run finishes in ~minutes, not ~20.
# ----------------------------------------------------------------------

# Per-paper title-resolution ledger fields key (versioned so a future field
# change re-opens the pool). Shared shape with corpus_rehydrate Phase 0.
_TITLE_LEDGER_FIELDS_KEY = "title_resolution_v1"
_RETRYABLE_STATUS = "retryable_error"


class _FallbackGate:
    """Thread-safe per-run budget + 429 short-circuit for the PAID OpenAlex
    fallback stage, shared across fetch workers.

    OpenAlex ``/works?search=`` costs $0.001/call, so an S2-cold run must not
    let a concurrent pool drain the daily credit budget. ``acquire()`` hands
    out at most ``budget`` paid attempts for the whole outer run and stops
    once ``block()`` fires (first 429)."""

    def __init__(self, budget: int) -> None:
        self._lock = threading.Lock()
        self._remaining = max(0, int(budget))
        self.blocked = False
        self.calls = 0

    def acquire(self) -> bool:
        with self._lock:
            if self.blocked or self._remaining <= 0:
                return False
            self._remaining -= 1
            self.calls += 1
            return True

    def block(self) -> None:
        with self._lock:
            self.blocked = True


def _local_year(row: sqlite3.Row) -> Optional[int]:
    try:
        return int(row["year"]) if row["year"] is not None else None
    except (TypeError, ValueError):
        return None


def _title_ledger_key(title: str) -> str:
    """Ledger ``lookup_key`` for a title (so a changed title re-opens it)."""
    from alma.core.utils import normalize_title_key

    return f"title:{normalize_title_key(title or '')}"


def _fetch_openalex_title_match(
    title: str, local_year: Optional[int]
) -> tuple[Optional[dict], float]:
    """FETCH (worker): OpenAlex ``/works?search`` → best raw work + score.

    Requests the FULL works projection (``_search_work_candidates`` ⇒
    ``_WORKS_SELECT_FIELDS``), so the returned work already carries
    abstract/authorships/topics/references — the writer fills ALL of it in
    one shot, with no Phase-1 re-fetch (B1). NO DB writes here.
    """
    from alma.library.enrichment import _search_work_candidates

    candidates = _search_work_candidates(
        title[:TITLE_RESOLUTION_QUERY_MAX_CHARS],
        per_page=TITLE_RESOLUTION_MAX_RESULTS,
    )
    if not candidates:
        return None, 0.0
    return _pick_best_candidate(
        candidates,
        title=title,
        local_year=local_year,
        title_key="display_name",
        year_key="publication_year",
    )


def _fetch_s2_title_match(
    title: str, local_year: Optional[int]
) -> tuple[Optional[dict], float, bool]:
    """FETCH (worker): S2 ``/paper/search`` → (candidate, score, rate_limited).

    The S2 response carries ``abstract`` + the SPECTER2 vector, which the
    writer persists for free. NO DB writes here.
    """
    try:
        candidates = semantic_scholar.search_papers(
            title[:TITLE_RESOLUTION_QUERY_MAX_CHARS],
            limit=TITLE_RESOLUTION_MAX_RESULTS,
            raise_on_rate_limit=True,
        )
    except semantic_scholar.SemanticScholarBatchError as exc:
        if getattr(exc, "status_code", None) == 429:
            return None, 0.0, True
        return None, 0.0, False
    except Exception as exc:  # transient/search error → treat as no-match this run
        logger.debug("title-resolution S2 search failed: %s", exc)
        return None, 0.0, False
    if not candidates:
        return None, 0.0, False
    best, score = _pick_best_candidate(
        candidates,
        title=title,
        local_year=local_year,
        title_key="title",
        year_key="year",
    )
    return best, score, False


# ----------------------------------------------------------------------
# Two INDEPENDENT source stages (tasks/38). The FREE S2 stage churns the
# backlog at 1 RPS; its MISSES flow into the PAID OpenAlex fallback stage,
# budget-gated per run — concurrently. So an S2 429 never stalls the
# OpenAlex stage or the writer (and vice versa). Both are NETWORK-ONLY
# workers; the single writer applies their results.
# ----------------------------------------------------------------------


def _fetch_s2_stage(row: sqlite3.Row) -> dict:
    """STAGE 0 (worker): FREE S2 ``/paper/search`` for one paper's identity.

    Returns a result dict the writer applies (on a hit) or the miss router
    advances to the OpenAlex fallback stage (on a miss). An S2 429 stamps
    the paper retryable — never terminal. NETWORK ONLY — never touches the
    DB.
    """
    paper_id = str(row["id"])
    title = str(row["title"] or "").strip()
    base: dict = {"paper_id": paper_id, "title": title, "row": row}
    if not title:
        return {**base, "source": "none", "resolved": False, "reason": "empty_title"}

    local_year = _local_year(row)
    best_s2, s2_score, rate_limited = _fetch_s2_title_match(title, local_year)
    if rate_limited:
        return {**base, "source": "s2", "resolved": False, "reason": "rate_limited"}
    if best_s2 is not None:
        return {**base, "source": "s2", "resolved": True,
                "candidate": best_s2, "score": round(s2_score, 4)}
    return {**base, "source": "s2", "resolved": False, "reason": "s2_no_match"}


def _advance_after_s2(row: sqlite3.Row, result: dict) -> Optional[sqlite3.Row]:
    """Miss router for the S2 stage: hand the row to the OpenAlex fallback
    stage ONLY on a genuine S2 miss. A hit, an empty-title terminal, or a
    rate-limited retryable goes straight to the writer. (``FetchError`` is
    routed to the writer by the pipeline itself — a network error is not a
    content miss.)"""
    if result.get("resolved") or result.get("reason") in {"empty_title", "rate_limited"}:
        return None
    return row


def _fetch_openalex_stage(
    row: sqlite3.Row, *, fallback_gate: Optional[_FallbackGate] = None
) -> dict:
    """PAID OpenAlex ``/works?search`` stage — fallback OR primary.

    Fallback mode (``fallback_gate`` set, S2-first order): guarded by the
    per-run ``_FallbackGate`` ($0.001/call) — when the budget is spent or a
    429 has tripped, returns a RETRYABLE budget-exhausted outcome (never
    terminal: OpenAlex hasn't seen the title, so stamping
    ``terminal_no_match`` would wrongly evict it from the pool). A genuine
    both-sources miss is terminal (``title_no_match``).

    Primary mode (``fallback_gate is None``, OpenAlex-first order): ungated —
    the order was only chosen because the daily budget is healthy — and a
    miss (``openalex_no_match``) ADVANCES to the free S2 stage instead of
    terminating. NETWORK ONLY — never touches the DB.
    """
    paper_id = str(row["id"])
    title = str(row["title"] or "").strip()
    base: dict = {"paper_id": paper_id, "title": title, "row": row}
    if not title:
        return {**base, "source": "none", "resolved": False, "reason": "empty_title"}
    local_year = _local_year(row)

    if fallback_gate is not None:
        # Live-credit pre-check BEFORE burning a gate slot or a socket: with
        # the daily pool below one search + the user reserve, every paid call
        # would 429 into its full backoff ladder — 4 workers grinding those
        # ladders wedged a sweep for 40+ minutes (2026-07-04 e2e). Retryable,
        # never terminal: OpenAlex hasn't seen the title.
        from alma.core.http_sources import RESERVED_USER_CALLS
        from alma.openalex.http import SEARCH_COST_CREDITS, get_client

        if get_client().budget_drained(reserve=SEARCH_COST_CREDITS + RESERVED_USER_CALLS):
            return {**base, "source": "openalex", "resolved": False,
                    "reason": "fallback_budget_exhausted"}
        if not fallback_gate.acquire():
            return {**base, "source": "openalex", "resolved": False,
                    "reason": "fallback_budget_exhausted"}

    best_oa, oa_score = _fetch_openalex_title_match(title, local_year)
    if best_oa is not None:
        return {**base, "source": "openalex", "resolved": True,
                "work": best_oa, "score": round(oa_score, 4)}
    miss_reason = "openalex_no_match" if fallback_gate is None else "title_no_match"
    return {**base, "source": "openalex", "resolved": False, "reason": miss_reason}


def _advance_after_openalex(row: sqlite3.Row, result: dict) -> Optional[sqlite3.Row]:
    """Miss router for the PRIMARY OpenAlex stage (OpenAlex-first order):
    hand the row to the free S2 stage ONLY on a genuine OpenAlex miss."""
    if result.get("resolved") or result.get("reason") == "empty_title":
        return None
    return row


def build_title_resolution_stages(
    fallback_gate: _FallbackGate, *, openalex_first: bool = False
) -> list[FetchStage]:
    """The two independent source stages for title resolution (tasks/38).

    Shared by the standalone "Resolve missing identity" sweep AND the
    corpus-rehydrate Phase-0 twin, so the two resolvers stay unified on ONE
    staged-pipeline wiring. Background-ops governance (task 37 pause + credit
    reserve) composes through the runner's ``is_cancelled`` callback
    (``scheduler.make_background_cancel_check``), not per-stage gating — so the
    stages stay source-pure here.

    ``openalex_first`` picks the ADAPTIVE order (benchmarked 2026-07-04 on
    the real corpus: OpenAlex ≈ 0.3 s/paper at $0.001 each; S2 ≈ 6 s/paper,
    free): user-triggered runs with a healthy budget go OpenAlex-first
    (ungated primary — the user is waiting; a miss advances to free S2);
    background runs and low-budget runs go S2-first with the gate-capped
    paid fallback.
    """
    if openalex_first:
        return [
            FetchStage(
                name="openalex_title",
                fetch_one=_fetch_openalex_stage,  # primary mode: no gate
                advance_on=_advance_after_openalex,
                workers=_FETCH_WORKERS,
                thread_name_prefix="alma-title-oa",
            ),
            FetchStage(
                name="s2_title",
                fetch_one=_fetch_s2_stage,
                workers=_S2_FETCH_WORKERS,
                thread_name_prefix="alma-title-s2",
            ),
        ]
    return [
        FetchStage(
            name="s2_title",
            fetch_one=_fetch_s2_stage,
            advance_on=_advance_after_s2,
            workers=_S2_FETCH_WORKERS,
            thread_name_prefix="alma-title-s2",
        ),
        FetchStage(
            name="openalex_title",
            fetch_one=lambda row: _fetch_openalex_stage(row, fallback_gate=fallback_gate),
            workers=_FETCH_WORKERS,
            thread_name_prefix="alma-title-oa",
        ),
    ]


def run_title_resolution_pipeline(
    rows: list,
    *,
    conn: sqlite3.Connection,
    model: str,
    counters: dict[str, int],
    fallback_gate: _FallbackGate,
    is_cancelled: Callable[[], bool],
    on_progress: Optional[Callable[[int, int], None]] = None,
    deadline: Optional[float] = None,
    openalex_first: bool = False,
) -> PipelineResult:
    """Run the staged two-source title-resolution pipeline over ``rows``.

    ONE definition of the resolver wiring (stages + writer), shared by the
    standalone sweep and corpus-rehydrate Phase 0 — so a fix to the
    decoupling lands in both. The writer (``_write_title_results``) runs on
    THIS (caller) thread and owns every DB write. ``openalex_first`` selects
    the adaptive source order (see ``build_title_resolution_stages``).
    """
    return run_staged_fetch_pipeline(
        rows,
        stages=build_title_resolution_stages(fallback_gate, openalex_first=openalex_first),
        write_batch=lambda batch: _write_title_results(
            conn, batch, model=model, counters=counters
        ),
        batch_size=_WRITE_BATCH_SIZE,
        deadline=deadline,
        is_cancelled=is_cancelled,
        on_progress=on_progress,
    )


def _apply_openalex_title_match(
    conn: sqlite3.Connection, paper_id: str, raw_work: dict
) -> tuple[list[str], Optional[str]]:
    """WRITE: fill id+doi AND merge the FULL metadata the search already
    returned (B1 — no Phase-1 re-fetch), then stamp the OpenAlex enrichment
    ledger ``enriched`` so the rehydrate Phase-1 selector skips this paper.

    Returns ``(fields_filled, merged_into)``. ``merged_into`` is non-None
    when the matched work's openalex_id already belongs to a DIFFERENT paper
    row — the title search has discovered that THIS row and that owner are the
    same paper. Rather than leave two canonical rows for the manual, never-auto
    dedup (which is why Feed / Discovery showed duplicates), we SOFT-merge this
    row INTO the owner (D3 — ``canonical_paper_id`` stamp, no hard delete;
    migrates FK / feedback / recommendations, fills the owner's empty scalars)
    and return the owner id so the caller stamps a terminal ``duplicate_merged``
    ledger row on the now-hidden loser. Writing the openalex_id onto this row
    would instead trip the UNIQUE ``idx_papers_openalex_id`` and abort the batch."""
    from alma.application.paper_metadata import merge_openalex_work_metadata
    from alma.core.paper_updates import fill_only_update_paper
    from alma.openalex.client import _normalize_work
    from alma.services.corpus_rehydrate import _upsert_enrichment_status, openalex_lookup_key

    oa_id_raw = str(raw_work.get("id") or "").strip()
    oa_id = _normalize_openalex_work_id(oa_id_raw) if oa_id_raw else ""
    new_doi = canonical_lookup_doi(str(raw_work.get("doi") or "")) or ""

    if oa_id:
        owner = conn.execute(
            "SELECT id FROM papers WHERE openalex_id = ? AND id <> ?",
            (oa_id, paper_id),
        ).fetchone()
        if owner is not None:
            # Same-openalex_id ⇒ same paper. Soft-merge THIS row into the owner
            # (D3 — canonical_paper_id stamp, no hard delete) so Feed / Discovery
            # collapse to one card, instead of leaving two canonical rows for the
            # manual, never-auto library_dedup. The owner keeps the unique
            # openalex_id; the merge migrates this row's FK / feedback /
            # recommendations and fills any richer scalar it carried.
            from alma.application.preprint_dedup import merge_duplicate_paper_rows

            owner_id = str(owner["id"])
            merge_duplicate_paper_rows(
                conn,
                loser_id=paper_id,
                keeper_id=owner_id,
                reason=f"duplicate_identity:{owner_id}",
            )
            return [], owner_id

    fill_fields: dict[str, str] = {}
    if oa_id:
        fill_fields["openalex_id"] = oa_id
    if new_doi:
        fill_fields["doi"] = new_doi
    fields_filled: list[str] = []
    if fill_fields:
        for f in (fill_only_update_paper(conn, paper_id, fill_fields=fill_fields) or []):
            fields_filled.append(str(f))

    # The search response already carries the full works projection — merge it
    # now instead of letting Phase 1 re-fetch the same work by openalex_id.
    merge_summary = merge_openalex_work_metadata(conn, paper_id, _normalize_work(raw_work))
    for f in (merge_summary.get("fields_filled") or []):
        if str(f) not in fields_filled:
            fields_filled.append(str(f))

    if oa_id:
        _upsert_enrichment_status(
            conn,
            paper_id=paper_id,
            lookup_key=openalex_lookup_key(oa_id),
            status="enriched",
            reason="title_resolution_inline_merge",
            fields_filled=fields_filled,
        )
    return fields_filled, None


def _apply_s2_title_match(
    conn: sqlite3.Connection, paper_id: str, candidate: dict
) -> tuple[list[str], bool]:
    """WRITE: merge the FULL metadata the S2 search response already carried
    (same fill semantics as ``s2_vectors._apply_s2_metadata`` + the tldr /
    influential-count extras of ``corpus_rehydrate._apply_s2_paper``), store
    the free SPECTER2 vector, then stamp the S2 enrichment ledger
    ``enriched`` so the rehydrate Phase-1.5 selector doesn't re-fetch the
    same record (B1 twin of ``_apply_openalex_title_match``).
    Returns ``(fields_filled, vector_stored)``."""
    from alma.core.paper_updates import fill_only_update_paper
    from alma.services.corpus_rehydrate import (
        S2_SOURCE,
        _s2_lookup_key_for_values,
        _write_ledger,
    )

    new_s2_id = str(candidate.get("semantic_scholar_id") or "").strip()
    new_doi = canonical_lookup_doi(str(candidate.get("doi") or "")) or ""
    new_abstract = str(candidate.get("abstract") or "").strip()
    new_url = str(candidate.get("url") or "").strip()
    try:
        year = int(candidate.get("year")) if candidate.get("year") is not None else None
    except (TypeError, ValueError):
        year = None
    try:
        cited_by = int(candidate.get("cited_by_count") or 0)
    except (TypeError, ValueError):
        cited_by = 0
    try:
        influential = int(candidate.get("influential_citation_count") or 0)
    except (TypeError, ValueError):
        influential = 0
    fields_filled = [
        str(f)
        for f in (
            fill_only_update_paper(
                conn,
                paper_id,
                fill_fields={
                    "semantic_scholar_id": new_s2_id,
                    "semantic_scholar_corpus_id": str(
                        candidate.get("semantic_scholar_corpus_id") or ""
                    ).strip(),
                    "doi": new_doi,
                    "abstract": new_abstract,
                    "url": new_url,
                    "publication_date": str(candidate.get("publication_date") or "").strip(),
                    "source_id": new_doi or new_url or str(candidate.get("title") or "").strip(),
                    "tldr": str(candidate.get("tldr") or "").strip(),
                },
                fill_null_fields={"year": year},
                max_int_fields={
                    "cited_by_count": cited_by,
                    "influential_citation_count": influential,
                },
                always_fields={"fetched_at": datetime.utcnow().isoformat()},
            )
            or []
        )
    ]
    vector_stored = False
    vector = candidate.get("specter2_embedding")
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
            logger.warning("title-resolution S2 vector store failed for %s: %s", paper_id, exc)

    # Stamp the S2 metadata ledger from the paper's FINAL identifiers so the
    # Phase-1.5 selector's lookup-key comparison matches and skips this paper
    # (fill-only above may have kept a pre-existing doi/s2_id over ours).
    id_row = conn.execute(
        "SELECT semantic_scholar_id, doi FROM papers WHERE id = ?", (paper_id,)
    ).fetchone()
    if id_row is not None:
        _write_ledger(
            conn,
            paper_id=paper_id,
            source=S2_SOURCE,
            lookup_key=_s2_lookup_key_for_values(
                str(id_row["semantic_scholar_id"] or ""), str(id_row["doi"] or "")
            ),
            status="enriched",
            reason="title_resolution_inline_merge",
            fields_filled=fields_filled,
            fields_key="s2_paper_v1",
        )
    return fields_filled, vector_stored


def _write_title_results(
    conn: sqlite3.Connection,
    results: list,
    *,
    model: str,
    counters: dict[str, int],
) -> None:
    """WRITE stage (caller thread): apply a batch of fetch results in ONE
    ``write_section``.

    Stamps a ``title_resolution`` ledger row for EVERY outcome so a
    stale/non-fetchable title leaves the candidate pool and is not re-fetched
    next run (the eligibility predicate excludes these). NO network here.
    """
    from alma.services.corpus_rehydrate import _write_ledger

    def _stamp(paper_id: str, title: str, status: str, reason: str,
               fields: list[str], retry_after) -> None:
        _write_ledger(
            conn,
            paper_id=paper_id,
            source=_TITLE_RESOLUTION_SOURCE,
            lookup_key=_title_ledger_key(title),
            status=status,
            reason=reason,
            fields_filled=fields,
            fields_key=_TITLE_LEDGER_FIELDS_KEY,
            retry_after=retry_after,
        )

    with write_section(conn, label="title resolution batch"):
        for r in results:
            # A fetch that raised: defer it (retryable) — never a terminal miss.
            if isinstance(r, FetchError):
                row = r.item
                if row is not None:
                    _stamp(str(row["id"]), str(row["title"] or ""),
                           _RETRYABLE_STATUS, f"fetch_error:{r.error}", [],
                           timedelta(hours=6))
                counters["errors"] += 1
                continue

            paper_id = str(r["paper_id"])
            title = str(r["title"])
            if r.get("resolved"):
                if r.get("source") == "openalex":
                    fields, merged_into = _apply_openalex_title_match(conn, paper_id, r["work"])
                    if merged_into is not None:
                        # Same-openalex_id duplicate: the row was soft-merged INTO
                        # its keeper (Feed / Discovery now show one card). Sticky
                        # terminal keeps the now-hidden loser out of the pool.
                        _stamp(paper_id, title, "terminal_no_match",
                               f"duplicate_merged:{merged_into}", fields, None)
                        counters["duplicate_merged"] += 1
                        continue
                    counters["resolved_via_openalex"] += 1
                else:
                    fields, vector_stored = _apply_s2_title_match(conn, paper_id, r["candidate"])
                    counters["resolved_via_s2"] += 1
                    if vector_stored:
                        counters["vectors_captured"] += 1
                # Identity now exists → drop any stale terminal vector-fetch row.
                _clear_terminal_status_row(conn, paper_id=paper_id, model=model)
                _stamp(paper_id, title, "enriched",
                       f"title_match:{r.get('source')}:{r.get('score')}", fields, None)
            elif r.get("reason") in ("rate_limited", "fallback_budget_exhausted"):
                # Retryable, never terminal: rate-limited means the source
                # said "not now"; budget-exhausted means OpenAlex never saw
                # the title this run. Both re-enter the pool after the TTL.
                _stamp(paper_id, title, _RETRYABLE_STATUS, str(r.get("reason")), [],
                       timedelta(minutes=10))
                counters["errors"] += 1
            else:
                # Genuine no-match: sticky terminal so we never re-fetch this
                # stale / non-fetchable title (user directive, tasks/11 A0).
                _stamp(paper_id, title, "terminal_no_match",
                       r.get("reason") or "title_no_match", [], None)
                counters["no_match"] += 1


def run_title_resolution_sweep(
    job_id: str,
    *,
    limit: int = 500,
    set_job_status: Callable[..., None],
    add_job_log: Callable[..., None],
    is_cancellation_requested: Callable[[str], bool],
    continuation_depth: int = 0,
) -> None:
    """Resolve paper identity via S2 (first, free) then OpenAlex (fallback).

    Eligibility: papers with a non-empty title that either lack a
    usable identity (no ``semantic_scholar_id`` and no DOI) or carry
    a terminal ``unmatched`` / ``bad_local_doi`` fetch_status row for
    the active SPECTER2 model. Bounded per outer run by
    ``min(limit, TITLE_RESOLUTION_PER_RUN_BUDGET)`` papers (S2 calls)
    and ``OPENALEX_FALLBACK_PER_RUN_BUDGET`` (paid OpenAlex calls).
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
            _eligibility_params(model) + (budget,),
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

        # Adaptive order (benchmarked 2026-07-04: OpenAlex ≈ 0.3 s/paper paid,
        # S2 ≈ 6 s/paper free): a USER-FACING run (manual click or the
        # onboarding-complete kick) with enough remaining credits for the
        # whole run + the user reserve goes OpenAlex-first — the user is
        # waiting. Background / low-budget runs go free-S2-first.
        from alma.core.http_sources import RESERVED_USER_CALLS as _RESERVED
        from alma.api.scheduler import (
            get_job_trigger_source as _get_trigger,
            is_user_facing_trigger as _is_user_facing,
        )
        from alma.openalex.http import SEARCH_COST_CREDITS, get_client as _oa_client

        # One title = one ?search = SEARCH_COST_CREDITS budget units — size the
        # reserve in the same unit the credit headers report.
        openalex_first = (
            _is_user_facing(_get_trigger(job_id))
            and not _oa_client().budget_drained(
                reserve=total * SEARCH_COST_CREDITS + _RESERVED
            )
        )
        order_label = "OpenAlex first, S2 fallback" if openalex_first else "S2 first, OpenAlex fallback"

        set_job_status(
            job_id,
            status="running",
            processed=0,
            total=total,
            message=f"Resolving identity for {total} papers ({order_label})",
        )
        add_job_log(
            job_id,
            "Prepared title-resolution sweep",
            step="prepare",
            data={
                "papers": total,
                "order": "openalex_first" if openalex_first else "s2_first",
                "run_budget": budget,
                "openalex_fallback_budget": OPENALEX_FALLBACK_PER_RUN_BUDGET,
                "continuation_depth": continuation_depth,
            },
        )

        # Decouple the FETCH side (TWO independent source stages — OpenAlex,
        # then S2 fallback for its misses — each at its own rate, running
        # concurrently) from the WRITE channel (this thread, batched). Fetchers
        # are network-only; the single writer below owns every DB write — so the
        # writer gate is held only for short, batched windows and NEVER across a
        # network call, and an OpenAlex 429 can't stall the S2 stage or the
        # writer. A wall-clock deadline caps the run so a uvicorn restart can
        # orphan at most ~_PER_RUN_SECONDS of work; the continuation/resume
        # drains the rest. See `core.fetch_pipeline` + `tasks/38` (+ `tasks/11`).
        counters: dict[str, int] = {
            "resolved_via_openalex": 0,
            "resolved_via_s2": 0,
            "vectors_captured": 0,
            "no_match": 0,
            "duplicate_identity": 0,
            "duplicate_merged": 0,
            "errors": 0,
        }
        fallback_gate = _FallbackGate(OPENALEX_FALLBACK_PER_RUN_BUDGET)

        # Background governance (task 37 A/C): a BACKGROUND sweep yields the
        # moment the user does anything (pause) or the OpenAlex quota nears the
        # user reserve (credit_limit). ONE central tripwire feeds is_cancelled;
        # `yield_sink` captures the reason so we stamp the graceful, RETRYABLE
        # outcome below and skip the continuation (the idle healer re-drains it).
        # A user-triggered run never yields (background_yield_reason no-ops).
        from alma.api.scheduler import (
            BG_CREDIT_LIMIT as _BG_CREDIT_LIMIT,
            get_job_trigger_source,
            make_background_cancel_check,
        )

        trigger_source = get_job_trigger_source(job_id)
        yield_sink: dict[str, str] = {}
        _is_cancelled = make_background_cancel_check(
            conn,
            job_id,
            _TITLE_SWEEP_OPERATION_KEY,
            is_cancellation_requested,
            trigger_source=trigger_source,
            sink=yield_sink,
        )

        def _progress(done: int, total_now: int) -> None:
            # Runs on THIS (writer) thread, between batched write_sections, so
            # the scheduler's own-connection status write never contends with a
            # held writer gate (the lessons rule on foreground/job status writes).
            set_job_status(
                job_id,
                status="running",
                processed=done,
                total=total_now,
                message=(
                    f"Title resolution: s2={counters['resolved_via_s2']}, "
                    f"openalex_fallback={counters['resolved_via_openalex']}, "
                    f"vectors={counters['vectors_captured']}"
                ),
            )

        pipeline_result = run_title_resolution_pipeline(
            rows,
            conn=conn,
            model=model,
            counters=counters,
            fallback_gate=fallback_gate,
            deadline=make_deadline(_PER_RUN_SECONDS),
            is_cancelled=_is_cancelled,
            on_progress=_progress,
            openalex_first=openalex_first,
        )

        processed = pipeline_result.processed
        resolved_via_openalex = counters["resolved_via_openalex"]
        resolved_via_s2 = counters["resolved_via_s2"]
        vectors_captured = counters["vectors_captured"]
        openalex_fallback_calls = fallback_gate.calls
        openalex_rate_limited = fallback_gate.blocked

        # Stopped mid-pipeline (do NOT reschedule a continuation either way):
        if pipeline_result.cancelled:
            if yield_sink:
                # Graceful BACKGROUND yield — pause (user active) or credit_limit
                # (quota near the reserve). RETRYABLE: unprocessed papers stay
                # eligible and the idle healer re-drains them. We stamp 'completed'
                # (this run finished gracefully) and carry `abort_reason` +, for a
                # credit stop, the live remaining credits so the Health card can
                # report "last operation aborted due to credit limit (N left)".
                reason = yield_sink.get("reason", "")
                message = yield_sink.get("message", "Background sweep yielded")
                result_payload: dict = {
                    "success": True,
                    "yielded": True,
                    "abort_reason": reason,
                    "processed": processed,
                    "resolved_via_s2": resolved_via_s2,
                    "resolved_via_openalex_fallback": resolved_via_openalex,
                }
                if reason == _BG_CREDIT_LIMIT:
                    from alma.core.http_sources import provider_remaining_credits

                    result_payload["openalex_credits_remaining"] = provider_remaining_credits(
                        "openalex"
                    )
                set_job_status(
                    job_id,
                    status="completed",
                    processed=processed,
                    total=total,
                    message=message,
                    finished_at=datetime.utcnow().isoformat(),
                    result=result_payload,
                )
                add_job_log(job_id, message, step=reason or "background_yield", data=result_payload)
                return
            set_job_status(
                job_id,
                status="cancelled",
                processed=processed,
                total=total,
                message="Title resolution cancelled",
                finished_at=datetime.utcnow().isoformat(),
            )
            return

        # No final commit: every result's writes were committed inside the
        # pipeline's batched write_sections (gather-from-network → gated write),
        # so nothing is left pending here.

        # Self-rescheduling decision. Queue a continuation when:
        # - we made forward progress (processed > 0). Safe against infinite
        #   loops now that EVERY processed paper is stamped in the
        #   title-resolution ledger (resolved → has ids; no-match → sticky
        #   terminal; retryable → TTL'd), so the eligible pool strictly shrinks
        #   each run. This replaces the old `resolved > 0` guard, which stalled
        #   the whole sweep whenever a (now time-boxed) chunk hit a prefix of
        #   unresolvable titles.
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
            and processed > 0
            and remaining > 0
            and remaining_session > 0
            and continuation_depth < _MAX_CONTINUATION_DEPTH
        )

        # 42.2: a run can "complete" having done nothing because it hit its
        # deadline sitting in provider throttling. Carry deadline_hit / dropped /
        # a no-progress warning so the message and the Health card tell the truth
        # instead of reporting a clean success.
        deadline_hit = bool(pipeline_result.deadline_hit)
        dropped = int(pipeline_result.dropped)
        no_progress_throttled = processed == 0 and (deadline_hit or openalex_rate_limited)
        result_data: dict = {
            "processed": processed,
            "resolved_via_s2": resolved_via_s2,
            "resolved_via_openalex_fallback": resolved_via_openalex,
            "vectors_captured": vectors_captured,
            "duplicate_identity": counters["duplicate_identity"],
            "duplicate_merged": counters["duplicate_merged"],
            "openalex_fallback_calls": openalex_fallback_calls,
            "openalex_rate_limited": openalex_rate_limited,
            "remaining_eligible": remaining,
            "remaining_session_budget": remaining_session,
            "continuation_depth": continuation_depth,
            "deadline_hit": deadline_hit,
            "dropped": dropped,
            "model": model,
        }
        if no_progress_throttled:
            result_data["no_progress"] = True
            result_data["warning"] = (
                "OpenAlex / Semantic Scholar is throttling — this run made no "
                "progress. The papers stay eligible and will retry when the app "
                "is idle; try again later."
            )

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
                "operation_key": _TITLE_SWEEP_OPERATION_KEY,
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

        if no_progress_throttled:
            # 0 processed + throttled/deadline: do NOT report "complete" — say so.
            message = (
                f"Title resolution made no progress (provider throttling); "
                f"remaining={remaining} — will retry."
            )
        else:
            message = (
                f"Title resolution complete: "
                f"openalex={resolved_via_openalex}, "
                f"s2_fallback={resolved_via_s2}, "
                f"vectors={vectors_captured}, "
                f"remaining={remaining}"
            )
            if deadline_hit:
                message += ", deadline hit"
            if dropped:
                message += f", dropped={dropped}"
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
