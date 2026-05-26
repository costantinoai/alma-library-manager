"""Maintenance-task registry (task 24, Pillar 2) — the bridge between the
canonical health dimensions (``alma.services.health``) and the bounded repair
runners.

Each :class:`MaintenanceTask` declares a stable ``key`` (equal to the health
dimension's ``repair_task``), a human label, which health dimensions it
repairs, the canonical "pending work" count to read, the bounded runner to
invoke, a cost class, a default daily cap, and whether the idle healer may run
it unattended (**default OFF** — opt-in only).

The SAME runner backs three callers — the manual Settings buttons, the
Health-page *Run now* action, and the periodic healer (Phase 4) — differing
only in ``trigger_source``. We reuse each runner's existing ``operation_key``
(rather than minting a ``maintenance.*`` key) so operation history stays
unified: "last run" on the Health page reflects every execution, however it
was triggered. ``find_active_job`` (inside ``schedule_with_envelope``) keys off
that same ``operation_key``, so a Run-now can never double-fire alongside a
manual job already in flight.

Pure orchestration + reads — no new tables. Per-task config lives in
``discovery_settings`` (``maintenance.<key>.enabled`` /
``maintenance.<key>.daily_cap``).
"""

from __future__ import annotations

import logging
import os
import sqlite3
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Callable, Optional

from alma.services import health as health_service
from alma.application import materialized_views as mv

logger = logging.getLogger(__name__)

# The idle healer processes at most this many items per task per tick. Small +
# frequent keeps each run gentle on the upstream APIs and the writer lock; the
# per-task daily_cap bounds the total across a UTC day.
HEALER_PER_TICK_LIMIT = 50

# Cost classes — surfaced so the UI / healer can reason about how heavy a task
# is before running it unattended.
COST_CHEAP = "cheap"  # local DB work only
COST_NETWORK = "network"  # remote API calls (OpenAlex / Crossref / S2)
COST_COMPUTE = "compute"  # local CPU/GPU (SPECTER2)


# --------------------------------------------------------------------------
# Runner bindings — bind the scheduler callbacks to each bounded runner at the
# service layer (mirroring the route wrappers in ai.py) so this module never
# imports the route layer. Each takes (job_id, cap) and returns None; ``cap``
# bounds the work so daily caps + run-now stay predictable.
# --------------------------------------------------------------------------


Targets = "list[str] | tuple[str, ...] | None"


def _run_corpus_metadata(job_id: str, cap: int, target_paper_ids=None, params=None) -> None:
    from alma.api.scheduler import add_job_log, is_cancellation_requested, set_job_status
    from alma.services.corpus_rehydrate import run_corpus_metadata_rehydration

    run_corpus_metadata_rehydration(
        job_id,
        limit=cap,
        target_paper_ids=target_paper_ids,
        set_job_status=set_job_status,
        add_job_log=add_job_log,
        is_cancellation_requested=is_cancellation_requested,
    )


def _run_s2_vector(job_id: str, cap: int, target_paper_ids=None, params=None) -> None:
    from alma.api.scheduler import add_job_log, is_cancellation_requested, set_job_status
    from alma.services.s2_vectors import run_s2_vector_backfill

    # Honor the per-op batch override (_schedule_task injected it into params from
    # the configured batch size, the same value the ETA was computed from).
    chunk_size = int((params or {}).get("batch_size") or 250)
    run_s2_vector_backfill(
        job_id,
        limit=cap,
        target_paper_ids=target_paper_ids,
        chunk_size=chunk_size,
        set_job_status=set_job_status,
        add_job_log=add_job_log,
        is_cancellation_requested=is_cancellation_requested,
    )


def _run_embedding(job_id: str, cap: int, target_paper_ids=None, params=None) -> None:
    from alma.api.scheduler import add_job_log, is_cancellation_requested, set_job_status
    from alma.services.embeddings import run_embedding_computation

    # scope="missing_stale" keeps history under the same operation_key as the
    # manual Compute-Embeddings button (ai.compute_embeddings).
    run_embedding_computation(
        job_id,
        scope="missing_stale",
        limit=cap,
        target_paper_ids=target_paper_ids,
        set_job_status=set_job_status,
        add_job_log=add_job_log,
        is_cancellation_requested=is_cancellation_requested,
    )


def _run_title_resolution(job_id: str, cap: int, target_paper_ids=None, params=None) -> None:
    # The title-resolution sweep scans title-only papers globally; it has no
    # target_paper_ids parameter, so per-paper targeting is a no-op (bulk run).
    from alma.api.scheduler import add_job_log, is_cancellation_requested, set_job_status
    from alma.services.title_resolution import run_title_resolution_sweep

    run_title_resolution_sweep(
        job_id,
        limit=cap,
        set_job_status=set_job_status,
        add_job_log=add_job_log,
        is_cancellation_requested=is_cancellation_requested,
    )


# --- Author / dedup jobs folded in from the old Corpus-maintenance card ------
# These reuse the exact service functions the legacy routes called.
# ``schedule_with_envelope`` already opened the queued operation_status row under
# the task's *base* operation_key; the underlying fn (or schedule_immediate's
# auto-finalize) then updates it by job_id WITHOUT re-stamping operation_key
# (COALESCE preserves it), so "last run" + idempotency stay keyed to the base.


def _run_author_metadata(job_id: str, cap: int, target_paper_ids=None, params=None) -> None:
    from alma.api.scheduler import add_job_log, is_cancellation_requested, set_job_status
    from alma.services.author_hydrate import run_author_metadata_rehydration

    run_author_metadata_rehydration(
        job_id,
        limit=cap,
        force=bool((params or {}).get("force", False)),
        set_job_status=set_job_status,
        add_job_log=add_job_log,
        is_cancellation_requested=is_cancellation_requested,
    )


def _run_refresh_authors(job_id: str, cap: int, target_paper_ids=None, params=None):
    # _deep_refresh_all_impl manages its own per-author status under job_id and
    # processes the whole scope (the cap doesn't bound it — the scope does). Lazy
    # route import keeps this module importable without the FastAPI app loaded.
    from alma.api.deps import open_db_connection
    from alma.api.routes.authors import _deep_refresh_all_impl

    scope = str((params or {}).get("scope") or "followed")
    conn = open_db_connection()
    try:
        return _deep_refresh_all_impl(conn, job_id=job_id, scope=scope)
    finally:
        conn.close()


def _run_gc_orphan_authors(job_id: str, cap: int, target_paper_ids=None, params=None):
    from alma.api.deps import open_db_connection
    from alma.application.author_lifecycle import garbage_collect_orphan_authors

    dry_run = bool((params or {}).get("dry_run", False))
    conn = open_db_connection()
    try:
        return garbage_collect_orphan_authors(conn, dry_run=dry_run, job_id=job_id)
    finally:
        conn.close()


def _run_dedup_orcid(job_id: str, cap: int, target_paper_ids=None, params=None):
    from alma.api.deps import open_db_connection
    from alma.application.author_merge import dedup_followed_authors_by_orcid

    conn = open_db_connection()
    try:
        return dedup_followed_authors_by_orcid(conn, job_id=job_id)
    finally:
        conn.close()


def _run_dedup_preprint_twins(job_id: str, cap: int, target_paper_ids=None, params=None):
    from alma.api.deps import _db_path
    from alma.application.preprint_dedup import run_preprint_dedup

    scope = str((params or {}).get("scope") or "library")
    return run_preprint_dedup(_db_path(), limit=cap, scope=scope)


# --- count_fn wrappers (cheap reads; signature (conn, params=None) -> int) ----


def _count_author_metadata(conn: sqlite3.Connection, params=None) -> int:
    from alma.services.author_hydrate import count_metadata_candidates

    return count_metadata_candidates(conn)


def _count_refresh_authors(conn: sqlite3.Connection, params=None) -> int:
    from alma.application.author_lifecycle import select_authors_for_scope

    scope = str((params or {}).get("scope") or "followed")
    return select_authors_for_scope(conn, scope, count_only=True)


def _count_gc_orphans(conn: sqlite3.Connection, params=None) -> int:
    from alma.application.author_lifecycle import count_orphan_authors

    return count_orphan_authors(conn)


def _count_dedup_orcid(conn: sqlite3.Connection, params=None) -> int:
    from alma.application.author_merge import count_dedup_orcid_candidates

    return count_dedup_orcid_candidates(conn)


def _count_preprint_twins(conn: sqlite3.Connection, params=None) -> int:
    from alma.application.preprint_dedup import count_preprint_twins

    scope = str((params or {}).get("scope") or "library")
    return count_preprint_twins(conn, scope)


def _count_title_resolution_eligible(conn: sqlite3.Connection, params=None) -> int:
    """Papers the title-resolution sweep would ACTUALLY process now — its own
    eligibility predicate, which excludes papers already stamped terminal
    (`terminal_no_match` / `unmatched`). The raw `identity.unresolved` dimension
    counts every paper without an OpenAlex id, so reading it as the op's pending
    over-reported work the sweep then skips; this keeps the count honest."""
    from alma.discovery.similarity import get_active_embedding_model
    from alma.services.title_resolution import _count_remaining_eligible

    try:
        return int(_count_remaining_eligible(conn, get_active_embedding_model(conn)) or 0)
    except Exception:
        logger.exception("title-resolution eligible count failed")
        return 0


# --------------------------------------------------------------------------
# Task registry
# --------------------------------------------------------------------------


@dataclass(frozen=True)
class MaintenanceTask:
    """One repairable health concern + the bounded runner that fixes it."""

    key: str  # stable id; equals the health dimension repair_task
    label: str
    description: str
    health_dimensions: tuple[str, ...]  # canonical dim keys it addresses
    candidate_path: str  # pending count from the health payload: "totals.X" | "dim:KEY" | "" if count_fn
    operation_key: str  # the runner's operation_status key (history / idempotency)
    job_id_prefix: str
    cost: str
    runner: Callable[..., None]  # (job_id, cap, target_paper_ids=None, params=None) -> None
    default_enabled: bool = False
    default_daily_cap: int = 200
    # --- optional, for the author / dedup jobs folded in from Corpus maintenance ---
    # Pending count for backlogs that aren't a health-payload path (authors, twins).
    # Signature: (conn, params=None) -> int. Takes precedence over candidate_path.
    count_fn: Optional[Callable[..., int]] = None
    # Declares run-time controls the UI should render, e.g.
    #   {"scope": {"options": [...], "default": "followed"}} or {"dry_run": {"default": False}}.
    params_spec: Optional[dict[str, Any]] = None
    # Which eta.PROFILES entry to use (defaults to ``key``).
    eta_key: Optional[str] = None


REGISTRY: dict[str, MaintenanceTask] = {
    task.key: task
    for task in (
        MaintenanceTask(
            key="corpus_metadata",
            label="Rehydrate corpus metadata",
            description=(
                "Fetch missing abstracts, references, topics, authors, DOIs, and "
                "dates from OpenAlex / Crossref for papers eligible for enrichment."
            ),
            health_dimensions=(
                "papers.missing_abstract",
                "papers.missing_references",
                "papers.missing_topics",
                "papers.missing_authorships",
                "papers.missing_doi",
                "papers.missing_publication_date",
                "papers.missing_url",
            ),
            candidate_path="totals.eligible_now",
            operation_key="papers.rehydrate_metadata:openalex:metadata",
            job_id_prefix="maint_corpus_metadata",
            cost=COST_NETWORK,
            runner=_run_corpus_metadata,
        ),
        MaintenanceTask(
            key="s2_vector",
            label="Fetch missing S2 vectors",
            description=(
                "Fetch precomputed SPECTER2 vectors from Semantic Scholar for "
                "papers that have a DOI / S2 id but no embedding."
            ),
            health_dimensions=("embeddings.s2_vector_missing",),
            candidate_path="dim:embeddings.s2_vector_missing",
            operation_key="ai.backfill_s2_vectors",
            job_id_prefix="maint_s2_vector",
            cost=COST_NETWORK,
            runner=_run_s2_vector,
        ),
        MaintenanceTask(
            key="embedding",
            label="Compute embeddings locally",
            description=(
                "Compute SPECTER2 embeddings on the local provider for papers that "
                "have a title + abstract but no vector (and refresh stale ones)."
            ),
            health_dimensions=("embeddings.local_computable", "embeddings.coverage"),
            candidate_path="dim:embeddings.local_computable",
            operation_key="ai.compute_embeddings",
            job_id_prefix="maint_embedding",
            cost=COST_COMPUTE,
            runner=_run_embedding,
        ),
        MaintenanceTask(
            key="title_resolution",
            label="Resolve missing identity",
            description=(
                "Resolve paper identity via Semantic Scholar title search for papers "
                "with no usable DOI / S2 id, so they can later be enriched + embedded."
            ),
            health_dimensions=("identity.unresolved",),
            candidate_path="dim:identity.unresolved",
            operation_key="ai.title_resolution_sweep",
            job_id_prefix="maint_title_resolution",
            cost=COST_NETWORK,
            runner=_run_title_resolution,
            # Honest pending: the op's own eligibility (excludes terminal_no_match /
            # unmatched), not the raw identity.unresolved gap it would skip past.
            count_fn=_count_title_resolution_eligible,
        ),
        # --- Folded in from the old Settings "Corpus maintenance" card --------
        MaintenanceTask(
            key="author_metadata",
            label="Rehydrate author metadata",
            description=(
                "Fill author profile fields + affiliation evidence from OpenAlex, "
                "ORCID, Semantic Scholar, and Crossref for authors that need it."
            ),
            health_dimensions=("authors.followed_unresolved", "authors.affiliation_conflicts"),
            candidate_path="",
            operation_key="authors.rehydrate_metadata",
            job_id_prefix="maint_author_metadata",
            cost=COST_NETWORK,
            runner=_run_author_metadata,
            count_fn=_count_author_metadata,
        ),
        MaintenanceTask(
            key="refresh_authors",
            label="Refresh authors",
            description=(
                "Full per-author pipeline: identity resolution → OpenAlex profile "
                "update → works + SPECTER2 vectors backfill → centroid recompute. "
                "Scope picks the author pool (followed is fast; corpus is the long tail)."
            ),
            health_dimensions=("authors.followed_unresolved", "authors.no_match", "authors.resolution_error"),
            candidate_path="",
            operation_key="authors.deep_refresh_all",
            job_id_prefix="maint_refresh_authors",
            cost=COST_NETWORK,
            runner=_run_refresh_authors,
            count_fn=_count_refresh_authors,
            params_spec={
                "scope": {
                    "options": ["followed", "followed_plus_library", "library", "needs_metadata", "corpus"],
                    "default": "followed",
                }
            },
        ),
        MaintenanceTask(
            key="gc_orphan_authors",
            label="Garbage-collect orphan authors",
            description=(
                "Soft-remove authors who aren't followed and have no live paper "
                "attachment (mirrors the paper lifecycle, D3). Preview first, then sweep."
            ),
            health_dimensions=(),
            candidate_path="",
            operation_key="authors.garbage_collect_orphans",
            job_id_prefix="maint_gc_orphans",
            cost=COST_CHEAP,
            runner=_run_gc_orphan_authors,
            count_fn=_count_gc_orphans,
            params_spec={"dry_run": {"default": False}},
        ),
        MaintenanceTask(
            key="dedup_orcid",
            label="Dedup authors by ORCID",
            description=(
                "Walk followed authors, find OpenAlex profiles sharing the same ORCID, "
                "and auto-merge (richer-profile-wins) or record an alias so duplicates "
                "stop resurfacing in suggestions."
            ),
            health_dimensions=("authors.merge_conflicts",),
            candidate_path="",
            operation_key="authors.dedup_by_orcid",
            job_id_prefix="maint_dedup_orcid",
            cost=COST_NETWORK,
            runner=_run_dedup_orcid,
            count_fn=_count_dedup_orcid,
        ),
        MaintenanceTask(
            key="dedup_preprint_twins",
            label="Dedup preprint↔journal twins",
            description=(
                "Detect papers that exist as both a preprint (arXiv / bioRxiv / OSF…) "
                "and a published journal version and collapse each pair into the journal "
                "row; FK rows migrate to the canonical paper."
            ),
            health_dimensions=(),
            candidate_path="",
            operation_key="papers.dedup_preprints",
            job_id_prefix="maint_dedup_twins",
            cost=COST_CHEAP,
            runner=_run_dedup_preprint_twins,
            count_fn=_count_preprint_twins,
            params_spec={"scope": {"options": ["library", "corpus"], "default": "library"}},
        ),
    )
}


# --------------------------------------------------------------------------
# Config (discovery_settings) — enabled flag + daily cap per task.
# --------------------------------------------------------------------------


def _setting_key(task_key: str, suffix: str) -> str:
    return f"maintenance.{task_key}.{suffix}"


def _read_raw_setting(conn: sqlite3.Connection, key: str) -> Optional[str]:
    try:
        row = conn.execute(
            "SELECT value FROM discovery_settings WHERE key = ?", (key,)
        ).fetchone()
    except sqlite3.OperationalError:
        return None
    if row is None:
        return None
    return row["value"] if isinstance(row, sqlite3.Row) else row[0]


def get_task_enabled(conn: sqlite3.Connection, task: MaintenanceTask) -> bool:
    raw = _read_raw_setting(conn, _setting_key(task.key, "enabled"))
    if raw is None:
        return task.default_enabled
    return str(raw).strip().lower() in {"1", "true", "yes", "on"}


def get_task_daily_cap(conn: sqlite3.Connection, task: MaintenanceTask) -> int:
    raw = _read_raw_setting(conn, _setting_key(task.key, "daily_cap"))
    if raw is None:
        return task.default_daily_cap
    try:
        return max(1, int(str(raw).strip()))
    except (TypeError, ValueError):
        return task.default_daily_cap


def get_task_batch_size(conn: sqlite3.Connection, task: MaintenanceTask) -> Optional[int]:
    """The per-op API batch size (items per request) the runner + ETA both use.

    Returns ``None`` when the op's batch is fixed (per-item ops, or multi-phase ops
    with no single knob) — see ``eta.batch_bounds``. Otherwise the stored override
    clamped to the endpoint's [default, max], defaulting to the profile default."""
    from alma.services.eta import batch_bounds, effective_batch_size

    bounds = batch_bounds(task.eta_key or task.key)
    if bounds is None:
        return None
    raw = _read_raw_setting(conn, _setting_key(task.key, "batch_size"))
    override: Optional[int] = None
    if raw is not None:
        try:
            override = int(str(raw).strip())
        except (TypeError, ValueError):
            override = None
    return effective_batch_size(task.eta_key or task.key, override)


def set_task_config(
    conn: sqlite3.Connection,
    task: MaintenanceTask,
    *,
    enabled: Optional[bool] = None,
    daily_cap: Optional[int] = None,
    batch_size: Optional[int] = None,
) -> None:
    """Persist enabled / daily_cap / batch_size for a task (only provided fields)."""
    from alma.application.discovery import upsert_setting
    from alma.services.eta import batch_bounds

    if enabled is not None:
        upsert_setting(conn, _setting_key(task.key, "enabled"), "true" if enabled else "false")
    if daily_cap is not None:
        upsert_setting(conn, _setting_key(task.key, "daily_cap"), str(max(1, int(daily_cap))))
    if batch_size is not None and batch_bounds(task.eta_key or task.key) is not None:
        # Stored raw; reads clamp to [default, max] via effective_batch_size.
        upsert_setting(conn, _setting_key(task.key, "batch_size"), str(max(1, int(batch_size))))


# --------------------------------------------------------------------------
# Candidate counts + last-run status (reads only)
# --------------------------------------------------------------------------


def _candidate_count(health_payload: dict[str, Any], candidate_path: str) -> int:
    """Pending-work count for a task, read from the canonical health snapshot."""
    if candidate_path.startswith("totals."):
        totals = health_payload.get("totals") or {}
        return int(totals.get(candidate_path.split(".", 1)[1]) or 0)
    if candidate_path.startswith("dim:"):
        wanted = candidate_path.split(":", 1)[1]
        for dim in health_payload.get("dimensions") or []:
            if dim.get("key") == wanted:
                return int(dim.get("count") or 0)
    return 0


def default_params(task: MaintenanceTask) -> dict[str, Any]:
    """The run-time params a task uses when the user hasn't chosen any — read from
    ``params_spec`` defaults (e.g. the default scope). Drives the default count/ETA."""
    spec = task.params_spec or {}
    out: dict[str, Any] = {}
    if isinstance(spec.get("scope"), dict):
        out["scope"] = spec["scope"].get("default")
    if isinstance(spec.get("dry_run"), dict):
        out["dry_run"] = bool(spec["dry_run"].get("default", False))
    return out


def task_pending_count(
    conn: sqlite3.Connection,
    task: MaintenanceTask,
    health_payload: dict[str, Any],
    *,
    params: Optional[dict[str, Any]] = None,
) -> int:
    """Pending-work count for a task: its ``count_fn`` (author / dedup backlogs)
    when present, else the canonical health-payload path. Never raises — a broken
    counter logs and reports 0 so the operations list still renders."""
    if task.count_fn is not None:
        try:
            return int(task.count_fn(conn, params if params is not None else default_params(task)) or 0)
        except Exception:
            logger.exception("count_fn failed for maintenance task %s", task.key)
            return 0
    return _candidate_count(health_payload, task.candidate_path)


def _last_run(conn: sqlite3.Connection, operation_key: str) -> Optional[dict[str, Any]]:
    """Most-recent operation_status row for ``operation_key`` (any trigger)."""
    try:
        row = conn.execute(
            """
            SELECT job_id, status, message, error, started_at, finished_at,
                   updated_at, trigger_source, processed, total
            FROM operation_status
            WHERE operation_key = ?
            ORDER BY COALESCE(finished_at, updated_at, started_at) DESC
            LIMIT 1
            """,
            (operation_key,),
        ).fetchone()
    except sqlite3.OperationalError:
        return None
    if row is None:
        return None
    started = str(row["started_at"] or "")
    finished = str(row["finished_at"] or "")
    duration_seconds: Optional[float] = None
    if started and finished:
        try:
            d = datetime.fromisoformat(finished) - datetime.fromisoformat(started)
            duration_seconds = round(d.total_seconds(), 1)
        except ValueError:
            duration_seconds = None
    return {
        "job_id": row["job_id"],
        "status": row["status"],
        "message": row["message"],
        "error": row["error"],
        "started_at": started or None,
        "finished_at": finished or None,
        "updated_at": str(row["updated_at"] or "") or None,
        "trigger_source": row["trigger_source"],
        "processed": int(row["processed"] or 0) if row["processed"] is not None else None,
        "total": int(row["total"] or 0) if row["total"] is not None else None,
        "duration_seconds": duration_seconds,
    }


def _last_success_at(conn: sqlite3.Connection, operation_key: str) -> Optional[str]:
    """Finished-at of the most recent *successful* run for ``operation_key``.

    Distinct from ``last_run`` (any status): this is "when did this function
    last complete successfully", so the UI can show staleness even when the
    latest attempt failed or is still running.
    """
    try:
        row = conn.execute(
            """
            SELECT COALESCE(finished_at, updated_at) AS at
            FROM operation_status
            WHERE operation_key = ? AND status = 'completed'
            ORDER BY COALESCE(finished_at, updated_at) DESC
            LIMIT 1
            """,
            (operation_key,),
        ).fetchone()
    except sqlite3.OperationalError:
        return None
    if row is None:
        return None
    return str(row["at"] or "") or None


def describe_task(
    conn: sqlite3.Connection, task: MaintenanceTask, health_payload: dict[str, Any]
) -> dict[str, Any]:
    """Full operations-status record for one task (the GET payload shape)."""
    from alma.services.eta import batch_bounds, detect_auth, estimate_eta

    pending = task_pending_count(conn, task, health_payload)
    openalex_authed, s2_authed = detect_auth()
    batch = get_task_batch_size(conn, task)  # None for fixed-batch ops
    bounds = batch_bounds(task.eta_key or task.key)
    return {
        "key": task.key,
        "label": task.label,
        "description": task.description,
        "cost": task.cost,
        "repairs": list(task.health_dimensions),
        "operation_key": task.operation_key,
        "candidates_pending": pending,
        # Run-time controls the UI should render (scope select / dry-run preview).
        "params_spec": task.params_spec,
        # Per-op API batch size (items/request). Present only for overridable ops;
        # the UI renders a bounded control and both the ETA + the runner honor it.
        "batch_size": batch,
        "batch_size_default": bounds[0] if bounds else None,
        "batch_size_max": bounds[1] if bounds else None,
        # ETA to drain the whole backlog over the network for the DEFAULT params +
        # the configured batch size (None for local / nothing-pending). Recomputed
        # each poll (shrinks as work completes); the /estimate endpoint recomputes
        # it live when the user changes scope or batch size.
        "eta": estimate_eta(
            task.eta_key or task.key,
            pending,
            openalex_authed=openalex_authed,
            s2_authed=s2_authed,
            batch_size=batch,
        ),
        "enabled": get_task_enabled(conn, task),
        "default_enabled": task.default_enabled,
        "daily_cap": get_task_daily_cap(conn, task),
        "last_run": _last_run(conn, task.operation_key),
        "last_success_at": _last_success_at(conn, task.operation_key),
    }


def estimate_task(
    conn: sqlite3.Connection,
    task: MaintenanceTask,
    health_payload: dict[str, Any],
    *,
    params: Optional[dict[str, Any]] = None,
    batch_size: Optional[int] = None,
) -> dict[str, Any]:
    """Recompute just the pending count + ETA for chosen ``params`` / ``batch_size``
    (e.g. a different scope or a dragged batch slider), so the UI can refresh the
    ETA live without re-listing every operation. ``batch_size`` here is the user's
    in-progress choice (not yet persisted); falls back to the stored config."""
    from alma.services.eta import detect_auth, estimate_eta

    effective = {**default_params(task), **(params or {})}
    pending = task_pending_count(conn, task, health_payload, params=effective)
    openalex_authed, s2_authed = detect_auth()
    batch = batch_size if batch_size is not None else get_task_batch_size(conn, task)
    return {
        "key": task.key,
        "params": effective,
        "candidates_pending": pending,
        "eta": estimate_eta(
            task.eta_key or task.key,
            pending,
            openalex_authed=openalex_authed,
            s2_authed=s2_authed,
            batch_size=batch,
        ),
    }


def list_operations(conn: sqlite3.Connection) -> dict[str, Any]:
    """All maintenance tasks with config + candidate counts + last-run status."""
    payload = (mv.get(conn, health_service.HEALTH_CORPUS_VIEW_KEY).get("payload")) or {}
    return {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "operations": [describe_task(conn, task, payload) for task in REGISTRY.values()],
    }


# --------------------------------------------------------------------------
# Run-now (manual trigger) — bounded by the configured daily cap.
# --------------------------------------------------------------------------


def _schedule_task(
    conn: sqlite3.Connection,
    task: MaintenanceTask,
    *,
    limit: int,
    trigger_source: str,
    queued_message: str,
    log_message: str,
    target_paper_ids: Optional[list[str]] = None,
    params: Optional[dict[str, Any]] = None,
) -> Optional[str]:
    """Schedule one bounded run of ``task`` (shared by run-now + the healer).

    Idempotent: ``schedule_with_envelope`` returns the in-flight job_id if a job
    with the same operation_key is already running (manual, healer, or chain).
    ``params`` carries run-time controls (scope / dry_run) to the runner.
    """
    from alma.core.job_envelope import schedule_with_envelope

    runner = task.runner

    # Inject the configured batch size so the runner uses the SAME value the ETA
    # was computed from (overridable ops only; None for fixed-batch ops). Run-now
    # and the healer both flow through here, so both honor the override.
    run_params = dict(params or {})
    batch = get_task_batch_size(conn, task)
    if batch is not None and "batch_size" not in run_params:
        run_params["batch_size"] = batch
    run_params = run_params or None

    def _factory(job_id: str) -> Callable[[], None]:
        return lambda: runner(job_id, limit, target_paper_ids, run_params)

    return schedule_with_envelope(
        operation_key=task.operation_key,
        job_id_prefix=task.job_id_prefix,
        trigger_source=trigger_source,
        queued_message=queued_message,
        runner_factory=_factory,
        log_message=log_message,
    )


def run_task_now(
    conn: sqlite3.Connection,
    task: MaintenanceTask,
    *,
    target_paper_ids: Optional[list[str]] = None,
    params: Optional[dict[str, Any]] = None,
) -> Optional[str]:
    """Schedule one bounded run of ``task`` under trigger_source='user'.

    ``target_paper_ids`` restricts the run to a specific set (a drilldown
    "fix selected") — bounded to exactly that set; otherwise the daily cap.
    ``params`` carries the chosen scope / dry-run (falls back to the task's
    ``params_spec`` defaults so a plain Run-now still works).
    """
    targets = [str(p) for p in (target_paper_ids or []) if str(p).strip()] or None
    limit = len(targets) if targets else get_task_daily_cap(conn, task)
    effective = {**default_params(task), **(params or {})}
    bits = [f"{len(targets)} selected" if targets else f"limit {limit}"]
    if effective:
        bits.append(", ".join(f"{k}={v}" for k, v in effective.items()))
    scope = " · ".join(bits)
    return _schedule_task(
        conn,
        task,
        limit=limit,
        trigger_source="user",
        queued_message=f"{task.label} queued from Health ({scope})",
        log_message=f"{task.label} queued from Health page ({scope})",
        target_paper_ids=targets,
        params=effective or None,
    )


# --------------------------------------------------------------------------
# The idle healer (Phase 4) — periodic, default OFF, daily-capped.
# --------------------------------------------------------------------------

ENV_DISABLE = "ALMA_DISABLE_IDLE_MAINTENANCE"

_SEVERITY_RANK = {"critical": 0, "warning": 1, "info": 2, "ok": 3}


def _utc_midnight_iso() -> str:
    """Today's UTC midnight as a naive ISO string (matches stored timestamps)."""
    return datetime.utcnow().replace(hour=0, minute=0, second=0, microsecond=0).isoformat()


def _healer_used_today(conn: sqlite3.Connection, operation_key: str) -> int:
    """Items the healer has already processed for ``operation_key`` since UTC
    midnight (only scheduler-triggered runs count toward the daily cap)."""
    try:
        row = conn.execute(
            """
            SELECT COALESCE(SUM(COALESCE(processed, 0)), 0) AS used
            FROM operation_status
            WHERE operation_key = ?
              AND trigger_source = 'scheduler'
              AND COALESCE(finished_at, started_at, updated_at) >= ?
            """,
            (operation_key, _utc_midnight_iso()),
        ).fetchone()
        return int((row["used"] if row else 0) or 0)
    except sqlite3.OperationalError:
        return 0


def _worst_severity_rank(payload: dict[str, Any], dim_keys: tuple[str, ...]) -> int:
    """Lowest (worst) severity rank among the task's health dimensions."""
    sev_by_key = {d.get("key"): d.get("severity") for d in (payload.get("dimensions") or [])}
    ranks = [_SEVERITY_RANK.get(sev_by_key.get(k) or "", 9) for k in dim_keys]
    return min(ranks) if ranks else 9


def maintenance_repair_periodic() -> None:
    """One healer tick: repair the single highest-severity enabled task that has
    pending work and remaining daily budget, with a small bounded batch.

    Default OFF — a task only runs if it was opted in on the Health page. The
    ``ALMA_DISABLE_IDLE_MAINTENANCE`` env var is a global hard kill. Reads the
    canonical health snapshot (never recomputes), respects in-flight jobs via
    ``find_active_job``, and schedules under ``trigger_source='scheduler'`` so
    chain-suppression is automatic and the run shows up on the Health page.
    """
    if str(os.getenv(ENV_DISABLE, "")).strip().lower() in {"1", "true", "yes", "on"}:
        logger.info("idle maintenance: disabled via %s", ENV_DISABLE)
        return

    from alma.api.deps import open_db_connection
    from alma.api.scheduler import find_active_job

    conn = open_db_connection()
    try:
        payload = (mv.get(conn, health_service.HEALTH_CORPUS_VIEW_KEY).get("payload")) or {}

        # Build the candidate set: enabled tasks with pending work, remaining
        # daily budget, and no run already in flight.
        candidates: list[tuple[int, int, MaintenanceTask]] = []
        any_active = False
        for task in REGISTRY.values():
            if find_active_job(task.operation_key):
                any_active = True
                continue
            if not get_task_enabled(conn, task):
                continue
            pending = task_pending_count(conn, task, payload)
            if pending <= 0:
                continue
            remaining = get_task_daily_cap(conn, task) - _healer_used_today(conn, task.operation_key)
            if remaining <= 0:
                logger.info("idle maintenance: %s hit its daily cap", task.key)
                continue
            rank = _worst_severity_rank(payload, task.health_dimensions)
            candidates.append((rank, remaining, task))

        # One maintenance task per tick — never run two repairs concurrently
        # (writer-lock courtesy). If another maintenance run is already in
        # flight, defer entirely.
        if any_active:
            logger.info("idle maintenance: a maintenance run is already active; deferring tick")
            return
        if not candidates:
            logger.info("idle maintenance: nothing enabled with pending work")
            return

        # Worst severity first, then the larger remaining budget.
        candidates.sort(key=lambda c: (c[0], -c[1]))
        rank, remaining, task = candidates[0]
        batch = min(remaining, HEALER_PER_TICK_LIMIT)

        job_id = _schedule_task(
            conn,
            task,
            limit=batch,
            trigger_source="scheduler",
            queued_message=f"Idle maintenance: {task.label} (limit {batch})",
            log_message=f"Idle healer queued {task.label} (batch {batch})",
        )
        logger.info(
            "idle maintenance: queued %s (batch=%d, remaining_cap=%d, job=%s)",
            task.key,
            batch,
            remaining,
            job_id,
        )
    except Exception:
        logger.exception("Fatal error in maintenance_repair_periodic")
    finally:
        try:
            conn.close()
        except Exception:
            pass
