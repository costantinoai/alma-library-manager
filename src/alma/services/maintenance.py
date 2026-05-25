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

import sqlite3
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Callable, Optional

from alma.services import health as health_service
from alma.application import materialized_views as mv

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


def _run_corpus_metadata(job_id: str, cap: int) -> None:
    from alma.api.scheduler import add_job_log, is_cancellation_requested, set_job_status
    from alma.services.corpus_rehydrate import run_corpus_metadata_rehydration

    run_corpus_metadata_rehydration(
        job_id,
        limit=cap,
        set_job_status=set_job_status,
        add_job_log=add_job_log,
        is_cancellation_requested=is_cancellation_requested,
    )


def _run_s2_vector(job_id: str, cap: int) -> None:
    from alma.api.scheduler import add_job_log, is_cancellation_requested, set_job_status
    from alma.services.s2_vectors import run_s2_vector_backfill

    run_s2_vector_backfill(
        job_id,
        limit=cap,
        set_job_status=set_job_status,
        add_job_log=add_job_log,
        is_cancellation_requested=is_cancellation_requested,
    )


def _run_embedding(job_id: str, cap: int) -> None:
    from alma.api.scheduler import add_job_log, is_cancellation_requested, set_job_status
    from alma.services.embeddings import run_embedding_computation

    # scope="missing_stale" keeps history under the same operation_key as the
    # manual Compute-Embeddings button (ai.compute_embeddings).
    run_embedding_computation(
        job_id,
        scope="missing_stale",
        limit=cap,
        set_job_status=set_job_status,
        add_job_log=add_job_log,
        is_cancellation_requested=is_cancellation_requested,
    )


def _run_title_resolution(job_id: str, cap: int) -> None:
    from alma.api.scheduler import add_job_log, is_cancellation_requested, set_job_status
    from alma.services.title_resolution import run_title_resolution_sweep

    run_title_resolution_sweep(
        job_id,
        limit=cap,
        set_job_status=set_job_status,
        add_job_log=add_job_log,
        is_cancellation_requested=is_cancellation_requested,
    )


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
    candidate_path: str  # where to read the pending count: "totals.X" | "dim:KEY"
    operation_key: str  # the runner's operation_status key (history / idempotency)
    job_id_prefix: str
    cost: str
    runner: Callable[[str, int], None]
    default_enabled: bool = False
    default_daily_cap: int = 200


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


def set_task_config(
    conn: sqlite3.Connection,
    task: MaintenanceTask,
    *,
    enabled: Optional[bool] = None,
    daily_cap: Optional[int] = None,
) -> None:
    """Persist enabled / daily_cap for a task (only the provided fields)."""
    from alma.application.discovery import upsert_setting

    if enabled is not None:
        upsert_setting(conn, _setting_key(task.key, "enabled"), "true" if enabled else "false")
    if daily_cap is not None:
        upsert_setting(conn, _setting_key(task.key, "daily_cap"), str(max(1, int(daily_cap))))


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


def describe_task(
    conn: sqlite3.Connection, task: MaintenanceTask, health_payload: dict[str, Any]
) -> dict[str, Any]:
    """Full operations-status record for one task (the GET payload shape)."""
    return {
        "key": task.key,
        "label": task.label,
        "description": task.description,
        "cost": task.cost,
        "repairs": list(task.health_dimensions),
        "operation_key": task.operation_key,
        "candidates_pending": _candidate_count(health_payload, task.candidate_path),
        "enabled": get_task_enabled(conn, task),
        "default_enabled": task.default_enabled,
        "daily_cap": get_task_daily_cap(conn, task),
        "last_run": _last_run(conn, task.operation_key),
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


def run_task_now(conn: sqlite3.Connection, task: MaintenanceTask) -> Optional[str]:
    """Schedule one bounded run of ``task`` under trigger_source='user'.

    Idempotent: ``schedule_with_envelope`` returns the in-flight job_id if a job
    with the same operation_key is already running (manual or otherwise).
    """
    from alma.core.job_envelope import schedule_with_envelope

    cap = get_task_daily_cap(conn, task)
    runner = task.runner

    def _factory(job_id: str) -> Callable[[], None]:
        return lambda: runner(job_id, cap)

    return schedule_with_envelope(
        operation_key=task.operation_key,
        job_id_prefix=task.job_id_prefix,
        trigger_source="user",
        queued_message=f"{task.label} queued from Health (limit {cap})",
        runner_factory=_factory,
        log_message=f"{task.label} queued from Health page",
    )
