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


def _new_chain_id() -> str:
    return uuid.uuid4().hex[:10]


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


def _count_s2_fetch_candidates(
    conn: sqlite3.Connection,
    *,
    target_paper_ids: list[str] | tuple[str, ...] | None = None,
) -> int:
    """Estimate of papers eligible for the next S2 vector backfill run.

    Mirrors the SELECT in `run_s2_vector_backfill` (DOI or s2_id, no
    active-model vector yet, no terminal fetch_status row). Used only
    to skip scheduling when zero — coarse over- / under-counts are
    fine.
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
            {target_clause}
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
) -> int:
    """Papers that local SPECTER2 *can* compute right now."""
    try:
        from alma.discovery.semantic_scholar import S2_SPECTER2_MODEL

        model = S2_SPECTER2_MODEL
        target_ids = normalize_id_list(target_paper_ids)
        target_clause = ""
        params: list[Any] = [model]
        if target_ids:
            target_clause = f"AND p.id IN ({','.join('?' for _ in target_ids)})"
            params.extend(target_ids)
        row = conn.execute(
            f"""
            SELECT COUNT(*) AS c
            FROM papers p
            WHERE NOT EXISTS (
                SELECT 1 FROM publication_embeddings pe
                WHERE pe.paper_id = p.id AND pe.model = ?
            )
            {target_clause}
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
        operation_key=_S2_FETCH_OPERATION_KEY,
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
    decision (`EMBEDDINGS_COMPONENT.md` Phase 3) explicitly forbids
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
        operation_key=_LOCAL_FILL_OPERATION_KEY,
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
