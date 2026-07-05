"""End-to-end coordinator for the hydration → vector chain.

Three Activity jobs run in sequence on the unattended path (per-paper
insert hooks, periodic scheduler sweeps):

1. `run_corpus_metadata_rehydration` — multi-source metadata fanout
   (OpenAlex + Semantic Scholar + Crossref + title-resolution).
2. `run_s2_vector_backfill` — pull S2-side SPECTER2 vectors.
3. `run_embedding_computation(scope="missing")` — local SPECTER2 fill
   for residual papers with title + abstract.

This module exposes the two tail-hooks each runner calls when it
finishes successfully:

- `schedule_post_hydration_chain` — after metadata hydration.
- `schedule_post_s2_chain` — after S2 vector backfill.

**The chain only fires on unattended runs.** Each parent runner
checks its own ``trigger_source`` via
``alma.api.scheduler.get_job_trigger_source`` before calling its
chain hook. A value of ``"user"`` means the parent run was kicked
off by a manual Settings button; the per-button contract there is
"do exactly what the label says," so chaining is suppressed and a
``chain_*_skipped`` log line is emitted instead. Any other source
(``"auto:..."`` from a prior chain step, ``"scheduler"`` from
periodic sweeps, ``None`` from internal callers) chains normally.
The user has separate Settings buttons for every step (Resolve
Missing Identity / Fetch Missing S2 Vectors / AI Compute Missing) so
manual control is always available without surprise side-effects.

Each schedule call:

- Imports the scheduler lazily so this module remains importable from
  CLI / test contexts where `alma.api.scheduler` isn't wired.
- Skips when zero candidates remain (the next stage has nothing to do).
- Uses `find_active_job` for idempotency — a sweep that already has
  the same operation_key in flight is reused, not duplicated.
- Stamps `chain_id` (a uuid hex shared across the three child jobs)
  into the Activity metadata so the UI can render the sequence as one
  block.
"""
from __future__ import annotations

import logging
import sqlite3
import uuid
from typing import Any, Callable, Optional

from alma.core.job_envelope import target_scoped_operation_key
from alma.core.sql_helpers import standalone_paper_sql
from alma.core.utils import normalize_id_list

logger = logging.getLogger(__name__)

# Operation keys used for `find_active_job` deduplication. These must
# match nothing else in the codebase that targets the same Activity
# slot.
_S2_FETCH_OPERATION_KEY = "embeddings.s2_vector_backfill"
_LOCAL_FILL_OPERATION_KEY = "embeddings.local_specter2_fill"

# Default per-run cap on the auto-scheduled S2 fetch. Mirrors the
# manual S2 fetch endpoint's upper bound (`s2_vectors.run_s2_vector_backfill`
# clamps to 5000).
_AUTO_S2_FETCH_LIMIT = 5000


# Durable "the S2-vector chain is owed" marker (41.2). A background metadata
# rehydration that yields on `paused_for_user` / `credit_limit` skips its
# post-hydration chain hook (the sweep resumes via the durable enrichment
# ledger, but the chain — which has no durable pending rows of its own — would be
# lost). We record this KV flag on that yield; the idle drain re-arms the chain
# once metadata has drained and the app is idle, then clears it. Lives in the
# shared `discovery_settings` KV, like the governance knobs.
_CHAIN_PENDING_KEY = "hydration.post_hydration_chain_pending"
_TRUTHY = {"1", "true", "yes", "on"}


def _queued_clause(queued_only: bool, alias: str = "p") -> str:
    """`AND <queued predicate>` when restricting a candidate count to the
    enqueued-but-never-attempted subset (41.3), else empty. Reuses the ONE shared
    predicate so S2 / local / identity / metadata all agree on "queued"."""
    if not queued_only:
        return ""
    from alma.services.corpus_rehydrate import queued_metadata_exists_sql

    return f"AND {queued_metadata_exists_sql(alias)}"


def _new_chain_id() -> str:
    return uuid.uuid4().hex[:10]


def chain_trigger_reason(parent_trigger_source: Optional[str], default: str) -> str:
    """Pick the ``trigger_reason`` for a chain hop scheduled from a parent run.

    Keeps the onboarding-complete kick USER-FACING end-to-end: when the
    parent was the onboarding kick, every hop re-uses the onboarding reason
    (child trigger stays ``auto:onboarding_complete`` → never yields to the
    idle gate while the user watches the wizard finish). Every other parent
    gets the hop's own default reason (``post_hydration`` / ``post_s2_fetch``).
    """
    from alma.api.scheduler import ONBOARDING_KICK_REASON, ONBOARDING_KICK_TRIGGER

    if str(parent_trigger_source or "").strip().lower() == ONBOARDING_KICK_TRIGGER:
        return ONBOARDING_KICK_REASON
    return default


def mark_post_hydration_chain_pending(conn: sqlite3.Connection) -> None:
    """Remember that a background yield deferred the S2-vector chain (41.2).

    Caller-owns-transaction: wrap in ``write_section`` / ``run_write_unit``.
    """
    from alma.application.discovery import lens_crud

    lens_crud.upsert_setting(conn, _CHAIN_PENDING_KEY, "1")


def clear_post_hydration_chain_pending(conn: sqlite3.Connection) -> None:
    """Clear the deferred-chain marker once the drain has re-armed it (41.2).

    Caller-owns-transaction: wrap in ``write_section`` / ``run_write_unit``.
    """
    from alma.application.discovery import lens_crud

    lens_crud.upsert_setting(conn, _CHAIN_PENDING_KEY, "0")


def is_post_hydration_chain_pending(conn: sqlite3.Connection) -> bool:
    """True when a background yield left the S2-vector chain owed (41.2)."""
    from alma.application.discovery import lens_crud

    raw = lens_crud.read_settings(conn).get(_CHAIN_PENDING_KEY)
    return str(raw or "").strip().lower() in _TRUTHY


def _has_local_specter2_provider(conn: sqlite3.Connection) -> bool:
    """True iff the active provider can compute SPECTER2 locally."""
    try:
        from alma.ai.providers import get_active_provider

        provider = get_active_provider(conn)
        if provider is None:
            return False
        return (
            getattr(provider, "name", "") == "local"
            and getattr(provider, "model_name", "") == "allenai/specter2_base"
        )
    except Exception:
        return False


def _count_s2_fetch_terminal(conn: sqlite3.Connection) -> int:
    """Papers with a DOI/s2_id and no active-model vector that are TERMINAL in the
    S2 fetch ledger — Semantic Scholar was tried and has no vector for them
    (``missing_vector`` / ``unmatched`` / ``lookup_error`` / ``bad_local_doi``).
    The complement of ``_count_s2_fetch_candidates``: these will NOT be re-fetched,
    so the Health page surfaces them as 'no fix via S2 — only local fill helps'."""
    try:
        from alma.discovery.semantic_scholar import S2_SPECTER2_MODEL

        model = S2_SPECTER2_MODEL
        row = conn.execute(
            f"""
            SELECT COUNT(*) AS c
            FROM papers p
            JOIN publication_embedding_fetch_status fs
              ON fs.paper_id = p.id
             AND fs.model = ?
             AND fs.source = 'semantic_scholar'
            WHERE COALESCE(fs.status, '') IN (
                'unmatched', 'missing_vector', 'lookup_error', 'bad_local_doi'
            )
            AND NOT EXISTS (
                SELECT 1 FROM publication_embeddings pe
                WHERE pe.paper_id = p.id
                  AND pe.model = ?
                  AND pe.source = 'semantic_scholar'
            )
            AND {standalone_paper_sql("p")}
            """,
            (model, model),
        ).fetchone()
        return int((row["c"] if row else 0) or 0)
    except sqlite3.OperationalError:
        return 0


def _count_s2_fetch_candidates(
    conn: sqlite3.Connection,
    *,
    target_paper_ids: list[str] | tuple[str, ...] | None = None,
    queued_only: bool = False,
) -> int:
    """Estimate of papers eligible for the next S2 vector backfill run.

    Mirrors the SELECT in `run_s2_vector_backfill` (DOI or s2_id, no
    active-model vector yet, no terminal fetch_status row). Used only
    to skip scheduling when zero — coarse over- / under-counts are
    fine. ``queued_only`` (41.3) ANDs the shared enqueued-never-attempted
    predicate so Health can show the "queued" split for this dimension.
    """
    try:
        from alma.discovery.semantic_scholar import S2_SPECTER2_MODEL

        model = S2_SPECTER2_MODEL
        target_ids = normalize_id_list(target_paper_ids)
        target_clause = ""
        params: list[Any] = [model]
        if target_ids:
            target_clause = f"AND p.id IN ({','.join('?' for _ in target_ids)})"
            params.extend(target_ids)
        params.append(model)
        queued_clause = _queued_clause(queued_only)
        row = conn.execute(
            f"""
            SELECT COUNT(*) AS c
            FROM papers p
            LEFT JOIN publication_embedding_fetch_status fs
              ON fs.paper_id = p.id
             AND fs.model = ?
             AND fs.source = 'semantic_scholar'
            WHERE (
                COALESCE(NULLIF(TRIM(p.semantic_scholar_id), ''), '') != ''
                OR COALESCE(NULLIF(TRIM(p.doi), ''), '') != ''
            )
            AND {standalone_paper_sql("p")}
            {target_clause}
            {queued_clause}
            AND NOT EXISTS (
                SELECT 1 FROM publication_embeddings pe
                WHERE pe.paper_id = p.id
                  AND pe.model = ?
                  AND pe.source = 'semantic_scholar'
            )
            AND COALESCE(fs.status, '') NOT IN (
                'unmatched', 'missing_vector', 'lookup_error', 'bad_local_doi'
            )
            """,
            params,
        ).fetchone()
        return int((row["c"] if row else 0) or 0)
    except sqlite3.OperationalError:
        return 0


def _count_local_specter2_candidates(
    conn: sqlite3.Connection,
    *,
    target_paper_ids: list[str] | tuple[str, ...] | None = None,
    queued_only: bool = False,
) -> int:
    """Papers that local SPECTER2 *can* compute right now.

    ``queued_only`` (41.3) restricts to the enqueued-never-attempted subset for
    the Health "queued" split.
    """
    try:
        from alma.discovery.semantic_scholar import S2_SPECTER2_MODEL

        model = S2_SPECTER2_MODEL
        target_ids = normalize_id_list(target_paper_ids)
        target_clause = ""
        params: list[Any] = [model]
        if target_ids:
            target_clause = f"AND p.id IN ({','.join('?' for _ in target_ids)})"
            params.extend(target_ids)
        queued_clause = _queued_clause(queued_only)
        row = conn.execute(
            f"""
            SELECT COUNT(*) AS c
            FROM papers p
            WHERE NOT EXISTS (
                SELECT 1 FROM publication_embeddings pe
                WHERE pe.paper_id = p.id AND pe.model = ?
            )
            AND {standalone_paper_sql("p")}
            {target_clause}
            {queued_clause}
            AND COALESCE(NULLIF(TRIM(p.title), ''), '') != ''
            AND COALESCE(NULLIF(TRIM(p.abstract), ''), '') != ''
            """,
            params,
        ).fetchone()
        return int((row["c"] if row else 0) or 0)
    except sqlite3.OperationalError:
        return 0


def _schedule_with_envelope(
    *,
    operation_key: str,
    job_id_prefix: str,
    chain_id: str,
    chain_step: str,
    trigger_source: str,
    queued_message: str,
    runner_factory: Callable[[str], Callable[..., Any]],
    log_data: Optional[dict[str, Any]] = None,
) -> Optional[str]:
    """Chain-coordinator wrapper around :func:`core.job_envelope.schedule_with_envelope`.

    Always stamps ``chain_id`` / ``chain_step`` on the Activity row and
    logs ``"Auto-queued by chain coordinator"``; the underlying envelope
    is the same one paper / author hydration sweeps use.
    """
    from alma.core.job_envelope import schedule_with_envelope

    return schedule_with_envelope(
        operation_key=operation_key,
        job_id_prefix=job_id_prefix,
        trigger_source=trigger_source,
        queued_message=queued_message,
        runner_factory=runner_factory,
        chain_id=chain_id,
        chain_step=chain_step,
        log_message="Auto-queued by chain coordinator",
        log_data=log_data,
    )


def schedule_post_hydration_chain(
    conn: sqlite3.Connection,
    *,
    chain_id: Optional[str] = None,
    trigger_reason: str = "post_hydration",
    limit: int | None = None,
    target_paper_ids: list[str] | tuple[str, ...] | None = None,
) -> dict[str, Any]:
    """After metadata hydration: queue the S2 vector backfill if useful.

    Returns ``{"chain_id": str, "scheduled_jobs": list[str]}``.
    """
    chain_id = chain_id or _new_chain_id()
    scheduled: list[str] = []
    target_ids = normalize_id_list(target_paper_ids)
    s2_limit = (
        _AUTO_S2_FETCH_LIMIT
        if limit is None
        else max(1, min(int(limit), _AUTO_S2_FETCH_LIMIT))
    )

    candidates_text = _count_s2_fetch_candidates(
        conn, target_paper_ids=target_ids or None
    )
    if candidates_text <= 0:
        logger.debug("post-hydration chain skipped S2 fetch: zero candidates")
        return {
            "chain_id": chain_id,
            "scheduled_jobs": scheduled,
            "skipped": "no_candidates",
            "target_paper_ids": target_ids,
        }

    def _runner_factory(job_id: str) -> Callable[[], Any]:
        from alma.api.scheduler import (
            add_job_log,
            is_cancellation_requested,
            set_job_status,
        )
        from alma.services.s2_vectors import run_s2_vector_backfill

        def _run() -> Any:
            return run_s2_vector_backfill(
                job_id,
                limit=s2_limit,
                target_paper_ids=target_ids,
                set_job_status=set_job_status,
                add_job_log=add_job_log,
                is_cancellation_requested=is_cancellation_requested,
            )

        return _run

    job_id = _schedule_with_envelope(
        operation_key=target_scoped_operation_key(_S2_FETCH_OPERATION_KEY, target_ids),
        job_id_prefix="auto_s2_fetch",
        chain_id=chain_id,
        chain_step="s2_fetch",
        trigger_source=f"auto:{trigger_reason}",
        queued_message=(
            f"S2 vector fetch auto-queued for up to {min(candidates_text, s2_limit)} "
            f"of {candidates_text} candidate(s)"
        ),
        runner_factory=_runner_factory,
        log_data={
            "limit": s2_limit,
            "target_paper_ids": target_ids,
            "target_count": len(target_ids),
            "candidate_count": candidates_text,
        },
    )
    if job_id:
        scheduled.append(job_id)
    return {
        "chain_id": chain_id,
        "scheduled_jobs": scheduled,
        "limit": s2_limit,
        "target_paper_ids": target_ids,
    }


def schedule_post_s2_chain(
    conn: sqlite3.Connection,
    *,
    chain_id: Optional[str] = None,
    trigger_reason: str = "post_s2_fetch",
    limit: int | None = None,
    target_paper_ids: list[str] | tuple[str, ...] | None = None,
) -> dict[str, Any]:
    """After S2 vector backfill: queue local SPECTER2 fill if useful.

    Skips unless the active provider IS local SPECTER2. The previous
    decision (`CONTRACTS.md` Phase 3) explicitly forbids
    auto-fanout on a model switch — this hook only fires because the
    user already opted in by configuring the local provider AND a paper
    was inserted, so there's clear intent.
    """
    chain_id = chain_id or _new_chain_id()
    scheduled: list[str] = []
    target_ids = normalize_id_list(target_paper_ids)
    local_limit = None if limit is None else max(1, int(limit))

    if not _has_local_specter2_provider(conn):
        return {
            "chain_id": chain_id,
            "scheduled_jobs": scheduled,
            "skipped": "no_local_specter2",
            "target_paper_ids": target_ids,
        }

    candidates = _count_local_specter2_candidates(
        conn, target_paper_ids=target_ids or None
    )
    if candidates == 0:
        return {
            "chain_id": chain_id,
            "scheduled_jobs": scheduled,
            "skipped": "no_candidates",
            "target_paper_ids": target_ids,
        }

    def _runner_factory(job_id: str) -> Callable[[], Any]:
        from alma.api.scheduler import (
            add_job_log,
            is_cancellation_requested,
            set_job_status,
        )
        from alma.services.embeddings import run_embedding_computation

        def _run() -> Any:
            return run_embedding_computation(
                job_id,
                scope="missing",
                limit=local_limit,
                target_paper_ids=target_ids,
                set_job_status=set_job_status,
                add_job_log=add_job_log,
                is_cancellation_requested=is_cancellation_requested,
            )

        return _run

    job_id = _schedule_with_envelope(
        operation_key=target_scoped_operation_key(_LOCAL_FILL_OPERATION_KEY, target_ids),
        job_id_prefix="auto_local_specter2",
        chain_id=chain_id,
        chain_step="local_specter2_fill",
        trigger_source=f"auto:{trigger_reason}",
        queued_message=(
            f"Local SPECTER2 fill auto-queued for "
            f"{min(candidates, local_limit or candidates)} candidate(s)"
        ),
        runner_factory=_runner_factory,
        log_data={
            "limit": local_limit,
            "target_paper_ids": target_ids,
            "target_count": len(target_ids),
            "candidate_count": candidates,
        },
    )
    if job_id:
        scheduled.append(job_id)
    return {
        "chain_id": chain_id,
        "scheduled_jobs": scheduled,
        "limit": local_limit,
        "target_paper_ids": target_ids,
    }
