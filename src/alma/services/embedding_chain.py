"""End-to-end coordinator for the hydration → vector chain.

Phase 6 of `tasks/13_END_TO_END_HYDRATION_VECTOR_CHAIN.md`.

Three Activity jobs run in sequence after a paper insert:

1. `run_corpus_metadata_rehydration` — multi-source metadata fanout
   (OpenAlex + Semantic Scholar + Crossref + title-resolution).
2. `run_s2_vector_backfill` — pull S2-side SPECTER2 vectors.
3. `run_embedding_computation(scope="missing")` — local SPECTER2 fill
   for residual papers with title + abstract.

This module exposes the two tail-hooks each runner calls when it
finishes successfully:

- `schedule_post_hydration_chain` — after metadata hydration.
- `schedule_post_s2_chain` — after S2 vector backfill.

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


def _count_s2_fetch_candidates(conn: sqlite3.Connection) -> int:
    """Estimate of papers eligible for the next S2 vector backfill run.

    Mirrors the SELECT in `run_s2_vector_backfill` (DOI or s2_id, no
    active-model vector yet, no terminal fetch_status row). Used only
    to skip scheduling when zero — coarse over- / under-counts are
    fine.
    """
    try:
        from alma.discovery.semantic_scholar import S2_SPECTER2_MODEL

        model = S2_SPECTER2_MODEL
        row = conn.execute(
            """
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
            (model, model),
        ).fetchone()
        return int((row["c"] if row else 0) or 0)
    except sqlite3.OperationalError:
        return 0


def _count_local_specter2_candidates(conn: sqlite3.Connection) -> int:
    """Papers that local SPECTER2 *can* compute right now."""
    try:
        from alma.discovery.semantic_scholar import S2_SPECTER2_MODEL

        model = S2_SPECTER2_MODEL
        row = conn.execute(
            """
            SELECT COUNT(*) AS c
            FROM papers p
            WHERE NOT EXISTS (
                SELECT 1 FROM publication_embeddings pe
                WHERE pe.paper_id = p.id AND pe.model = ?
            )
            AND COALESCE(NULLIF(TRIM(p.title), ''), '') != ''
            AND COALESCE(NULLIF(TRIM(p.abstract), ''), '') != ''
            """,
            (model,),
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
) -> Optional[str]:
    """Idempotent Activity-enveloped schedule helper.

    `runner_factory(job_id)` must return a zero-arg callable that runs
    the actual work using `set_job_status` / `add_job_log` /
    `is_cancellation_requested` from `alma.api.scheduler`. Passing the
    job_id explicitly avoids the holder-pattern race where the worker
    runs before the caller can stash the id.

    Imports the scheduler lazily; a missing scheduler returns None so
    callers stay testable without the FastAPI app loaded.
    """
    try:
        from alma.api.scheduler import (
            add_job_log,
            find_active_job,
            schedule_immediate,
            set_job_status,
        )
    except Exception:
        return None

    existing = find_active_job(operation_key)
    if existing:
        return str(existing.get("job_id") or "") or None

    job_id = f"{job_id_prefix}_{uuid.uuid4().hex[:10]}"
    set_job_status(
        job_id,
        status="queued",
        operation_key=operation_key,
        trigger_source=trigger_source,
        message=queued_message,
        chain_id=chain_id,
        chain_step=chain_step,
    )
    add_job_log(
        job_id,
        "Auto-queued by chain coordinator",
        step="queued",
        data={"chain_id": chain_id, "chain_step": chain_step},
    )

    runner = runner_factory(job_id)
    schedule_immediate(job_id, runner)
    return job_id


def schedule_post_hydration_chain(
    conn: sqlite3.Connection,
    *,
    chain_id: Optional[str] = None,
    trigger_reason: str = "post_hydration",
) -> dict[str, Any]:
    """After metadata hydration: queue the S2 vector backfill if useful.

    Returns ``{"chain_id": str, "scheduled_jobs": list[str]}``.
    """
    chain_id = chain_id or _new_chain_id()
    scheduled: list[str] = []

    if _count_s2_fetch_candidates(conn) <= 0:
        logger.debug("post-hydration chain skipped S2 fetch: zero candidates")
        return {
            "chain_id": chain_id,
            "scheduled_jobs": scheduled,
            "skipped": "no_candidates",
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
                limit=_AUTO_S2_FETCH_LIMIT,
                set_job_status=set_job_status,
                add_job_log=add_job_log,
                is_cancellation_requested=is_cancellation_requested,
            )

        return _run

    candidates_text = _count_s2_fetch_candidates(conn)
    job_id = _schedule_with_envelope(
        operation_key=_S2_FETCH_OPERATION_KEY,
        job_id_prefix="auto_s2_fetch",
        chain_id=chain_id,
        chain_step="s2_fetch",
        trigger_source=f"auto:{trigger_reason}",
        queued_message=f"S2 vector fetch auto-queued for {candidates_text} candidate(s)",
        runner_factory=_runner_factory,
    )
    if job_id:
        scheduled.append(job_id)
    return {"chain_id": chain_id, "scheduled_jobs": scheduled}


def schedule_post_s2_chain(
    conn: sqlite3.Connection,
    *,
    chain_id: Optional[str] = None,
    trigger_reason: str = "post_s2_fetch",
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

    if not _has_local_specter2_provider(conn):
        return {
            "chain_id": chain_id,
            "scheduled_jobs": scheduled,
            "skipped": "no_local_specter2",
        }

    candidates = _count_local_specter2_candidates(conn)
    if candidates == 0:
        return {
            "chain_id": chain_id,
            "scheduled_jobs": scheduled,
            "skipped": "no_candidates",
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
        queued_message=f"Local SPECTER2 fill auto-queued for {candidates} candidate(s)",
        runner_factory=_runner_factory,
    )
    if job_id:
        scheduled.append(job_id)
    return {"chain_id": chain_id, "scheduled_jobs": scheduled}
