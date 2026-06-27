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
``discovery_settings`` under four separated, validated keys:
``maintenance.<key>.auto_enabled`` (idle-healer opt-in; never true for a
destructive task), ``maintenance.<key>.auto_daily_cap`` (unattended units per
UTC day), ``maintenance.<key>.remembered_manual_limit`` (the visible Run-now
default), and ``maintenance.<key>.request_batch_size`` (upstream payload size,
overridable ops only). The ambiguous legacy ``enabled`` / ``daily_cap`` /
``batch_size`` keys are migrated once at startup by ``migrate_maintenance_config``
and then deleted; forward code reads only the four current keys.
"""

from __future__ import annotations

import logging
import os
import sqlite3
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Callable, Optional

from alma.services import health as health_service
from alma.application import materialized_views as mv
from alma.services.maintenance_contracts import (
    BatchSpec,
    MaintenanceRunPlan,
    MaintenanceRunSpec,
    MaintenanceStage,
    MaintenanceTask,
    MaintenanceTrigger,
    MaintenanceUnit,
    MaintenanceValidationError,
    PlanDependency,
    ScopeSpec,
    StageBudget,
    TargetKind,
    fingerprint_plan,
)

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

SOURCE_OPENALEX = "openalex"
SOURCE_SEMANTIC_SCHOLAR = "semantic_scholar"
SOURCE_CROSSREF = "crossref"
SOURCE_ORCID = "orcid"
SOURCE_LANDING_PAGE = "landing_page"

# Human labels for the ordered stage groups the Health UI renders (Checkpoint G).
# The backend owns grouping + order + labels; the frontend renders these stages
# verbatim with no hard-coded task-key arrays.
STAGE_LABELS: dict[MaintenanceStage, str] = {
    MaintenanceStage.AUTHOR_IDENTITY: "Author identity & profile",
    MaintenanceStage.AUTHOR_CANONICALIZATION: "Author canonicalization",
    MaintenanceStage.AUTHOR_WORKS: "Author works",
    MaintenanceStage.PAPER_IDENTITY: "Paper identity",
    MaintenanceStage.PAPER_METADATA: "Paper metadata",
    MaintenanceStage.PAPER_CANONICALIZATION: "Canonicalize papers",
    MaintenanceStage.REMOTE_VECTORS: "Vectors — Semantic Scholar",
    MaintenanceStage.LOCAL_EMBEDDINGS: "Vectors — local embeddings",
    MaintenanceStage.DERIVED: "Derived data",
    MaintenanceStage.CLEANUP: "Cleanup",
    MaintenanceStage.HOUSEKEEPING: "Database housekeeping",
}


# --------------------------------------------------------------------------
# Runner bindings — bind the scheduler callbacks to each bounded runner at the
# service layer (mirroring the route wrappers in ai.py) so this module never
# imports the route layer. Each takes (job_id, cap) and returns None; ``cap``
# bounds the work so daily caps + run-now stay predictable.
# --------------------------------------------------------------------------


Targets = "list[str] | tuple[str, ...] | None"


@contextmanager
def _maintenance_conn():
    """One open → run → close pattern for the runners whose application function
    takes a DB connection (the deep-refresh / GC / ORCID-dedup sweeps). The other
    runners hand the connection to the service via callback or a db path; this
    converges the connection-taking ones onto a single, obvious lifecycle so a
    reader never has to reverse-engineer who closes what. Lazy import keeps this
    module importable without the FastAPI app loaded."""
    from alma.api.deps import open_db_connection

    conn = open_db_connection()
    try:
        yield conn
    finally:
        conn.close()


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


def _run_gc_orphan_authors(job_id: str, cap: int, target_paper_ids=None, params=None):
    from alma.application.author_lifecycle import garbage_collect_orphan_authors

    dry_run = bool((params or {}).get("dry_run", False))
    # cap = the run's total budget (was previously ignored — the sweep collected
    # every orphan regardless of the cap).
    with _maintenance_conn() as conn:
        return garbage_collect_orphan_authors(conn, dry_run=dry_run, limit=cap, job_id=job_id)


def _run_dedup_orcid(job_id: str, cap: int, target_paper_ids=None, params=None):
    from alma.application.author_merge import dedup_followed_authors_by_orcid

    # cap = the run's total budget (was previously ignored — the sweep scanned
    # every followed author, one ORCID network call each).
    with _maintenance_conn() as conn:
        return dedup_followed_authors_by_orcid(conn, limit=cap, job_id=job_id)


def _run_dedup_preprint_twins(job_id: str, cap: int, target_paper_ids=None, params=None):
    from alma.api.deps import _db_path
    from alma.application.preprint_dedup import run_preprint_dedup

    scope = str((params or {}).get("scope") or "library")
    return run_preprint_dedup(_db_path(), limit=cap, scope=scope)


# --- Checkpoint C: discrete author / derived / cleanup / housekeeping runners --
# These split the old monolithic "deep refresh" (which hid identity + works +
# vectors + centroid in one call) and register the derived-data rebuilds that
# previously lived only as scattered routes, so the Health page shows the FULL
# ordered maintenance DAG instead of nine cards. Each binds to the canonical
# existing service function — no logic is duplicated here.


class _ProgressCtx:
    """Minimal ``ctx`` adapter forwarding a service's ``log_step`` progress to
    the Activity row, so batch runners written for the route layer report
    processed/total under the maintenance envelope without bespoke wiring."""

    def __init__(self, job_id: str, set_job_status: Callable[..., Any], add_job_log: Callable[..., Any]):
        self._job_id = job_id
        self._set = set_job_status
        self._log = add_job_log

    def log_step(self, step: str, *, message: Optional[str] = None, processed=None, total=None, **_: Any) -> None:
        fields = {k: v for k, v in {"processed": processed, "total": total}.items() if v is not None}
        if fields:
            self._set(self._job_id, **fields)
        if message:
            self._log(self._job_id, message, step=step)


def _run_author_works(job_id: str, cap: int, target_paper_ids=None, params=None):
    """Author works expansion (step 3): paginate + upsert each resolved author's
    OpenAlex works (and the S2 vectors / centroid they imply). Bounded by `cap`
    authors. Distinct from `author_metadata` (identity/profile fill) and
    `author_centroids` (centroid-only recompute)."""
    from alma.api.deps import _db_path
    from alma.api.scheduler import add_job_log, is_cancellation_requested, set_job_status
    from alma.application.author_backfill import backfill_all_resolved_authors

    ctx = _ProgressCtx(job_id, set_job_status, add_job_log)
    return backfill_all_resolved_authors(
        _db_path(),
        ctx=ctx,
        limit=cap,
        is_cancellation_requested=lambda: is_cancellation_requested(job_id),
    )


def _run_author_centroids(job_id: str, cap: int, target_paper_ids=None, params=None):
    """Centroid recompute (step 9): refresh stale/missing author centroids from
    EXISTING local embeddings only — no network, no works re-pagination."""
    from alma.api.scheduler import add_job_log, is_cancellation_requested, set_job_status
    from alma.application.author_backfill import recompute_author_centroids

    with _maintenance_conn() as conn:
        return recompute_author_centroids(
            conn,
            limit=cap,
            job_id=job_id,
            set_job_status=set_job_status,
            add_job_log=add_job_log,
            is_cancellation_requested=is_cancellation_requested,
        )


def _run_reference_graph(job_id: str, cap: int, target_paper_ids=None, params=None):
    """Reference backfill (step 10, derived): fetch missing OpenAlex reference
    edges for up to `cap` papers. Graph/projection caches rebuild themselves
    (fingerprint-driven MV layer) once references land."""
    from alma.api.scheduler import set_job_status
    from alma.openalex.client import backfill_missing_publication_references

    with _maintenance_conn() as conn:
        result = backfill_missing_publication_references(conn, limit=cap)
    set_job_status(job_id, processed=int(result.get("papers_updated") or 0), total=int(result.get("candidates") or 0))
    return result


def _run_cluster_labels(job_id: str, cap: int, target_paper_ids=None, params=None):
    """Cluster-label refresh (step 10, derived): regenerate TF-IDF labels for the
    library paper-map clusters and invalidate the cache so the next render is
    fresh. One pass (unit = operation); `cap` is not a per-item budget here."""
    from alma.api.routes.graphs import _cluster_label_refresh_impl

    scope = str((params or {}).get("scope") or "library")
    graph_type = str((params or {}).get("graph_type") or "paper_map")
    with _maintenance_conn() as conn:
        return _cluster_label_refresh_impl(conn, graph_type=graph_type, scope=scope, job_id=job_id)


def _run_topic_normalize(job_id: str, cap: int, target_paper_ids=None, params=None):
    """Topic normalization (step 10, derived): the deterministic canonical-topic
    pass (NFKD + acronym folding → canonical term + aliases). Safe + idempotent;
    fuzzy/AI merges stay manual."""
    from alma.library.topic_deduplication import build_canonical_topics

    with _maintenance_conn() as conn:
        return build_canonical_topics(conn)


def _run_library_dedup(job_id: str, cap: int, target_paper_ids=None, params=None):
    """Generic library dedup + stable-ID pass (paper canonicalization).
    DESTRUCTIVE: merges duplicate author/paper rows and rewires FKs — manual
    gate only, never auto."""
    from alma.library.deduplication import run_deduplication

    with _maintenance_conn() as conn:
        return run_deduplication(conn, job_id=job_id)


def _run_housekeeping(job_id: str, cap: int, target_paper_ids=None, params=None):
    """DB housekeeping (independent): prune stale Activity logs + incremental
    vacuum. Shares one implementation with the daily scheduler job."""
    from alma.api.scheduler import run_db_housekeeping

    with _maintenance_conn() as conn:
        return run_db_housekeeping(conn)


# --- count_fn wrappers (cheap reads; signature (conn, params=None) -> int) ----


def _count_author_works(conn: sqlite3.Connection, params=None) -> int:
    """Resolved authors whose works/centroid are missing or >14 days stale — the
    pool `backfill_all_resolved_authors` would walk."""
    from datetime import timedelta

    from alma.discovery import semantic_scholar

    cutoff = (datetime.now(timezone.utc) - timedelta(days=14)).isoformat()
    try:
        row = conn.execute(
            """
            SELECT COUNT(*) AS n FROM (
                SELECT lower(a.openalex_id) AS oid
                FROM authors a
                LEFT JOIN author_centroids ac
                  ON ac.author_openalex_id = lower(a.openalex_id) AND ac.model = ?
                WHERE COALESCE(TRIM(a.openalex_id), '') <> ''
                  AND (ac.author_openalex_id IS NULL OR ac.updated_at < ?)
                GROUP BY oid
            )
            """,
            (semantic_scholar.S2_SPECTER2_MODEL, cutoff),
        ).fetchone()
        return int((row["n"] if row else 0) or 0)
    except sqlite3.OperationalError:
        return 0


def _count_author_centroids(conn: sqlite3.Connection, params=None) -> int:
    from alma.application.author_backfill import count_authors_needing_centroid

    return count_authors_needing_centroid(conn)


def _count_reference_graph(conn: sqlite3.Connection, params=None) -> int:
    """Identified papers with no local reference edges yet (the backfill pool)."""
    try:
        row = conn.execute(
            """
            SELECT COUNT(*) AS n
            FROM papers p
            WHERE COALESCE(TRIM(p.openalex_id), '') <> ''
              AND NOT EXISTS (SELECT 1 FROM publication_references r WHERE r.paper_id = p.id)
            """
        ).fetchone()
        return int((row["n"] if row else 0) or 0)
    except sqlite3.OperationalError:
        return 0


def _count_topic_normalize(conn: sqlite3.Connection, params=None) -> int:
    """Topic terms not yet linked to a canonical `topics` row."""
    try:
        row = conn.execute(
            "SELECT COUNT(*) AS n FROM publication_topics WHERE topic_id IS NULL"
        ).fetchone()
        return int((row["n"] if row else 0) or 0)
    except sqlite3.OperationalError:
        return 0


def _count_library_dedup(conn: sqlite3.Connection, params=None) -> int:
    """Cheap duplicate proxy: papers sharing a normalized DOI beyond the first.
    A non-zero value means the destructive full dedup pass has real work."""
    try:
        row = conn.execute(
            """
            SELECT COALESCE(SUM(c - 1), 0) AS n FROM (
                SELECT COUNT(*) AS c FROM papers
                WHERE COALESCE(TRIM(doi), '') <> ''
                GROUP BY lower(trim(doi)) HAVING COUNT(*) > 1
            )
            """
        ).fetchone()
        return int((row["n"] if row else 0) or 0)
    except sqlite3.OperationalError:
        return 0


def _count_housekeeping(conn: sqlite3.Connection, params=None) -> int:
    """Prunable Activity-log rows past the retention window + free DB pages —
    a truthful 'is there housekeeping to do' signal."""
    from datetime import timedelta

    try:
        cutoff = (datetime.utcnow() - timedelta(days=30)).isoformat()
        prunable = int(
            (conn.execute(
                "SELECT COUNT(*) AS n FROM operation_logs WHERE timestamp < ?", (cutoff,)
            ).fetchone() or {"n": 0})["n"] or 0
        )
        free = int((conn.execute("PRAGMA freelist_count").fetchone() or [0])[0] or 0)
        return prunable + free
    except sqlite3.OperationalError:
        return 0


def _count_author_metadata(conn: sqlite3.Connection, params=None) -> int:
    from alma.services.author_hydrate import count_metadata_candidates

    return count_metadata_candidates(conn)


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
            stage=MaintenanceStage.PAPER_METADATA,
            order=50,
            unit=MaintenanceUnit.PAPER,
            target_kind=TargetKind.PAPER,
            supports_targets=True,
            prerequisites=("title_resolution",),
            unlocks=("dedup_preprint_twins", "s2_vector"),
            default_manual_limit=500,
            max_manual_limit=5_000,
            default_auto_daily_cap=500,
            auto_chunk_size=100,
            sources=(
                SOURCE_OPENALEX,
                SOURCE_SEMANTIC_SCHOLAR,
                SOURCE_CROSSREF,
                SOURCE_LANDING_PAGE,
            ),
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
            stage=MaintenanceStage.REMOTE_VECTORS,
            order=70,
            unit=MaintenanceUnit.PAPER,
            target_kind=TargetKind.PAPER,
            supports_targets=True,
            prerequisites=("dedup_preprint_twins",),
            unlocks=("embedding",),
            default_manual_limit=500,
            max_manual_limit=5_000,
            default_auto_daily_cap=500,
            auto_chunk_size=200,
            request_batch=BatchSpec(MaintenanceUnit.LOOKUP_ID, default=250, maximum=500),
            sources=(SOURCE_SEMANTIC_SCHOLAR,),
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
            stage=MaintenanceStage.LOCAL_EMBEDDINGS,
            order=80,
            unit=MaintenanceUnit.PAPER,
            target_kind=TargetKind.PAPER,
            supports_targets=True,
            prerequisites=("s2_vector",),
            unlocks=("author_centroids",),
            default_manual_limit=500,
            max_manual_limit=5_000,
            default_auto_daily_cap=500,
            auto_chunk_size=100,
            local_compute=True,
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
            stage=MaintenanceStage.PAPER_IDENTITY,
            order=40,
            unit=MaintenanceUnit.PAPER,
            target_kind=TargetKind.PAPER,
            supports_targets=False,
            unlocks=("corpus_metadata",),
            default_manual_limit=200,
            max_manual_limit=5_000,
            default_auto_daily_cap=200,
            auto_chunk_size=100,
            sources=(SOURCE_SEMANTIC_SCHOLAR,),
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
            # No health dimension. Re-hydration is a PRODUCER of affiliation
            # evidence, not a RESOLVER of conflicts: an `affiliation_conflicts`
            # flag means current sources genuinely disagree, which a re-fetch only
            # re-confirms — it's cleared by a human PICK on the Authors page
            # (pick_affiliation), not by this op. Mapping the op to that dimension
            # made the card advertise a fix it can't perform AND mismatched its
            # pending count (0 to rehydrate) against the conflict count (≥1). The
            # conflict is surfaced instead as an Observed/needs-review dimension.
            health_dimensions=(),
            candidate_path="",
            operation_key="authors.rehydrate_metadata",
            job_id_prefix="maint_author_metadata",
            cost=COST_NETWORK,
            runner=_run_author_metadata,
            stage=MaintenanceStage.AUTHOR_IDENTITY,
            order=10,
            unit=MaintenanceUnit.AUTHOR_SOURCE_ATTEMPT,
            target_kind=TargetKind.AUTHOR,
            supports_targets=False,
            unlocks=("dedup_orcid",),
            count_fn=_count_author_metadata,
            default_manual_limit=200,
            max_manual_limit=2_000,
            default_auto_daily_cap=200,
            auto_chunk_size=50,
            sources=(
                SOURCE_OPENALEX,
                SOURCE_ORCID,
                SOURCE_SEMANTIC_SCHOLAR,
                SOURCE_CROSSREF,
            ),
        ),
        MaintenanceTask(
            key="author_works",
            label="Expand author works",
            description=(
                "Producer: paginate each resolved author's OpenAlex works and "
                "upsert the papers (plus the S2 vectors + centroid they imply) for "
                "authors whose coverage is missing or >14 days stale. Heavy and "
                "optional — bulk expansion is opt-in; a single Follow expands that "
                "one author on the action path."
            ),
            # No health-dimension claim — on purpose. This op only acts on already
            # RESOLVED authors (non-empty openalex_id) whose centroid is missing or
            # >14 days stale (see _count_author_works). It structurally cannot touch
            # `followed_unresolved` (no openalex_id by definition), `no_match`, or
            # `resolution_error` (identity states healed per-author on the Authors
            # page). Claiming those three was the root of the reported "maintenance
            # due asks to backfill some authors but does not resolve" bug: the Health
            # repair count never dropped because the run worked a disjoint population.
            # It stays visible via its own count_fn as an honest freshness op that
            # repairs no health gap; the orphaned identity dims fall to Diagnostics.
            health_dimensions=(),
            candidate_path="",
            operation_key="authors.backfill_works",
            job_id_prefix="maint_author_works",
            cost=COST_NETWORK,
            runner=_run_author_works,
            stage=MaintenanceStage.AUTHOR_WORKS,
            order=30,
            unit=MaintenanceUnit.AUTHOR,
            target_kind=TargetKind.AUTHOR,
            supports_targets=False,
            prerequisites=("dedup_orcid",),
            unlocks=("title_resolution", "corpus_metadata"),
            optional=True,
            count_fn=_count_author_works,
            default_manual_limit=25,
            max_manual_limit=500,
            default_auto_daily_cap=25,
            auto_chunk_size=10,
            sources=(SOURCE_OPENALEX, SOURCE_SEMANTIC_SCHOLAR),
            local_compute=True,
            # Reuse the per-author multi-source rate profile for a truthful ETA.
            eta_key="refresh_authors",
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
            stage=MaintenanceStage.CLEANUP,
            order=100,
            unit=MaintenanceUnit.AUTHOR,
            target_kind=TargetKind.AUTHOR,
            supports_targets=False,
            prerequisites=("embedding",),
            manual_gate=True,
            count_fn=_count_gc_orphans,
            default_manual_limit=500,
            max_manual_limit=5_000,
            default_auto_daily_cap=500,
            auto_chunk_size=100,
            destructive=True,
            supports_dry_run=True,
        ),
        MaintenanceTask(
            key="dedup_orcid",
            label="Dedup authors by ORCID",
            description=(
                "Walk followed authors, find OpenAlex profiles sharing the same ORCID, "
                "and auto-merge (richer-profile-wins) or record an alias so duplicates "
                "stop resurfacing in suggestions."
            ),
            # No health dimension. This op CREATES merges (it can even leave a
            # `merge_conflicts` behind when a merge keeps a conflicting hard id);
            # it does not RESOLVE them — that's a human decision on the Authors
            # page (resolve_conflict). Mapping it to `merge_conflicts` both
            # mis-claimed the repair AND collapsed the op into "All clear" whenever
            # the conflict count was 0, hiding its real pending dedup work. Its
            # pending count (ORCID-dedup candidates) now stands on its own.
            health_dimensions=(),
            candidate_path="",
            operation_key="authors.dedup_by_orcid",
            job_id_prefix="maint_dedup_orcid",
            cost=COST_NETWORK,
            runner=_run_dedup_orcid,
            stage=MaintenanceStage.AUTHOR_CANONICALIZATION,
            order=20,
            unit=MaintenanceUnit.AUTHOR,
            target_kind=TargetKind.AUTHOR,
            supports_targets=False,
            prerequisites=("author_metadata",),
            unlocks=("author_works",),
            manual_gate=True,
            count_fn=_count_dedup_orcid,
            default_manual_limit=100,
            max_manual_limit=500,
            default_auto_daily_cap=100,
            auto_chunk_size=25,
            sources=(SOURCE_OPENALEX,),
            destructive=True,
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
            stage=MaintenanceStage.PAPER_CANONICALIZATION,
            order=60,
            unit=MaintenanceUnit.PAIR,
            target_kind=TargetKind.PAIR,
            supports_targets=False,
            prerequisites=("corpus_metadata",),
            unlocks=("s2_vector",),
            manual_gate=True,
            count_fn=_count_preprint_twins,
            default_manual_limit=500,
            max_manual_limit=5_000,
            default_auto_daily_cap=500,
            auto_chunk_size=100,
            destructive=True,
            scope=ScopeSpec(options=("library", "corpus"), default="library"),
        ),
        # --- Checkpoint C: generic library dedup (paper canonicalization) ------
        MaintenanceTask(
            key="library_dedup",
            label="Dedup library (papers + authors)",
            description=(
                "Generic stable-ID + duplicate collapse: merge papers/authors that "
                "share a DOI / OpenAlex id / ORCID / (title, year), rewire every FK, "
                "and assign stable UIDs. DESTRUCTIVE — manual gate, never auto."
            ),
            health_dimensions=(),
            candidate_path="",
            operation_key="library.deduplicate",
            job_id_prefix="maint_library_dedup",
            cost=COST_CHEAP,
            runner=_run_library_dedup,
            stage=MaintenanceStage.PAPER_CANONICALIZATION,
            order=62,
            unit=MaintenanceUnit.OPERATION,
            target_kind=TargetKind.NONE,
            supports_targets=False,
            prerequisites=("corpus_metadata",),
            unlocks=("s2_vector",),
            manual_gate=True,
            count_fn=_count_library_dedup,
            default_manual_limit=1,
            max_manual_limit=1,
            default_auto_daily_cap=1,
            max_auto_daily_cap=1,
            destructive=True,
        ),
        # --- Checkpoint C: derived-data rebuilds (centroids → refs → clusters → topics)
        MaintenanceTask(
            key="author_centroids",
            label="Recompute author centroids",
            description=(
                "Refresh the mean-SPECTER2 centroid for authors whose vector set "
                "changed (added/removed embeddings). Local-only, no network — keeps "
                "Discovery author-alignment in sync after vectors land."
            ),
            health_dimensions=(),
            candidate_path="",
            operation_key="authors.recompute_centroids",
            job_id_prefix="maint_author_centroids",
            cost=COST_CHEAP,
            runner=_run_author_centroids,
            stage=MaintenanceStage.DERIVED,
            order=85,
            unit=MaintenanceUnit.AUTHOR,
            target_kind=TargetKind.NONE,
            supports_targets=False,
            prerequisites=("embedding",),
            count_fn=_count_author_centroids,
            default_manual_limit=500,
            max_manual_limit=5_000,
            default_auto_daily_cap=1_000,
            auto_chunk_size=200,
            local_compute=True,
        ),
        MaintenanceTask(
            key="reference_graph",
            label="Backfill references & graph",
            description=(
                "Fetch missing OpenAlex citation/reference edges for identified "
                "papers so the citation graph, projections, and clusters render on "
                "real edges. Graph / MV caches rebuild themselves once edges land."
            ),
            health_dimensions=("papers.missing_references",),
            candidate_path="",
            operation_key="graphs.reference_backfill",
            job_id_prefix="maint_reference_graph",
            cost=COST_NETWORK,
            runner=_run_reference_graph,
            stage=MaintenanceStage.DERIVED,
            order=90,
            unit=MaintenanceUnit.PAPER,
            target_kind=TargetKind.NONE,
            supports_targets=False,
            prerequisites=("corpus_metadata",),
            count_fn=_count_reference_graph,
            default_manual_limit=250,
            max_manual_limit=5_000,
            default_auto_daily_cap=250,
            auto_chunk_size=100,
            sources=(SOURCE_OPENALEX,),
        ),
        MaintenanceTask(
            key="cluster_labels",
            label="Refresh cluster labels",
            description=(
                "Regenerate the deterministic TF-IDF top-term labels for the library "
                "paper-map clusters and invalidate the graph cache so the next render "
                "reflects current membership. One pass."
            ),
            health_dimensions=(),
            candidate_path="",
            operation_key="graphs.cluster_labels:paper_map:library",
            job_id_prefix="maint_cluster_labels",
            cost=COST_COMPUTE,
            runner=_run_cluster_labels,
            stage=MaintenanceStage.DERIVED,
            order=92,
            unit=MaintenanceUnit.OPERATION,
            target_kind=TargetKind.NONE,
            supports_targets=False,
            prerequisites=("embedding",),
            default_manual_limit=1,
            max_manual_limit=1,
            default_auto_daily_cap=1,
            max_auto_daily_cap=1,
            local_compute=True,
        ),
        MaintenanceTask(
            key="topic_normalize",
            label="Normalize topics",
            description=(
                "Deterministic canonical-topic pass: fold NFKD / acronym variants of "
                "each topic term into one canonical `topics` row + aliases and link "
                "publications. Safe + idempotent; fuzzy/AI merges stay manual."
            ),
            health_dimensions=(),
            candidate_path="",
            operation_key="topics.normalize_canonical",
            job_id_prefix="maint_topic_normalize",
            cost=COST_CHEAP,
            runner=_run_topic_normalize,
            stage=MaintenanceStage.DERIVED,
            order=94,
            unit=MaintenanceUnit.OPERATION,
            target_kind=TargetKind.NONE,
            supports_targets=False,
            prerequisites=("corpus_metadata",),
            count_fn=_count_topic_normalize,
            default_manual_limit=1,
            max_manual_limit=1,
            default_auto_daily_cap=1,
            max_auto_daily_cap=1,
        ),
        # --- Checkpoint C: independent DB housekeeping --------------------------
        MaintenanceTask(
            key="housekeeping",
            label="Database housekeeping",
            description=(
                "Prune stale Activity logs past the retention window and "
                "incremental-vacuum freed pages. Independent of the repair DAG; also "
                "runs nightly on a schedule."
            ),
            health_dimensions=(),
            candidate_path="",
            operation_key="db.maintenance",
            job_id_prefix="maint_housekeeping",
            cost=COST_CHEAP,
            runner=_run_housekeeping,
            stage=MaintenanceStage.HOUSEKEEPING,
            order=110,
            unit=MaintenanceUnit.OPERATION,
            target_kind=TargetKind.NONE,
            supports_targets=False,
            count_fn=_count_housekeeping,
            default_manual_limit=1,
            max_manual_limit=1,
            default_auto_daily_cap=1,
            max_auto_daily_cap=1,
        ),
    )
}
REGISTRY = dict(sorted(REGISTRY.items(), key=lambda item: item[1].order))


# --------------------------------------------------------------------------
# Config (discovery_settings) — four distinct, validated concepts per task.
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


def get_task_auto_enabled(conn: sqlite3.Connection, task: MaintenanceTask) -> bool:
    raw = _read_raw_setting(conn, _setting_key(task.key, "auto_enabled"))
    if raw is None:
        return task.default_auto_enabled
    enabled = str(raw).strip().lower() in {"1", "true", "yes", "on"}
    # Safety is structural even if a legacy/bad DB value says otherwise.
    return enabled and not task.destructive


def _read_validated_int(
    conn: sqlite3.Connection,
    key: str,
    *,
    default: int,
    minimum: int,
    maximum: int,
) -> int:
    raw = _read_raw_setting(conn, key)
    if raw is None:
        return default
    try:
        value = int(str(raw).strip())
    except (TypeError, ValueError):
        return default
    # Config migration guarantees current rows are valid. A corrupt/manual edit
    # falls back visibly to the declared default; it is never silently changed.
    return value if minimum <= value <= maximum else default


def get_task_auto_daily_cap(conn: sqlite3.Connection, task: MaintenanceTask) -> int:
    return _read_validated_int(
        conn,
        _setting_key(task.key, "auto_daily_cap"),
        default=task.default_auto_daily_cap,
        minimum=1,
        maximum=task.max_auto_daily_cap,
    )


def get_task_manual_limit(conn: sqlite3.Connection, task: MaintenanceTask) -> int:
    return _read_validated_int(
        conn,
        _setting_key(task.key, "remembered_manual_limit"),
        default=task.default_manual_limit,
        minimum=1,
        maximum=task.max_manual_limit,
    )


def get_task_request_batch_size(
    conn: sqlite3.Connection, task: MaintenanceTask
) -> Optional[int]:
    """The exact upstream lookup-ID payload size used by ETA and runner.

    Invalid persisted values are not clamped. Startup migration corrects legacy
    rows; a later corrupt/manual edit falls back to the declared default.
    """
    if task.request_batch is None:
        return None
    return _read_validated_int(
        conn,
        _setting_key(task.key, "request_batch_size"),
        default=task.request_batch.default,
        minimum=1,
        maximum=task.request_batch.maximum,
    )


def set_task_config(
    conn: sqlite3.Connection,
    task: MaintenanceTask,
    *,
    auto_enabled: Optional[bool] = None,
    auto_daily_cap: Optional[int] = None,
    remembered_manual_limit: Optional[int] = None,
    request_batch_size: Optional[int] = None,
) -> None:
    """Persist only validated values; impossible intent is rejected, never clamped."""
    from alma.application.discovery import upsert_setting
    from alma.core.db_write import run_write_unit

    if auto_enabled and task.destructive:
        raise MaintenanceValidationError(f"{task.key} is destructive and cannot be auto-enabled")
    if auto_daily_cap is not None:
        task.validate_auto_daily_cap(auto_daily_cap)
    if remembered_manual_limit is not None:
        try:
            task.validate_max_items(remembered_manual_limit)
        except MaintenanceValidationError as exc:
            raise MaintenanceValidationError(str(exc).replace("max_items", "manual_limit")) from exc
    if request_batch_size is not None:
        if task.request_batch is None:
            raise MaintenanceValidationError(f"{task.key} has no configurable request batch")
        task.request_batch.validate(request_batch_size)

    def _persist() -> None:
        if auto_enabled is not None:
            upsert_setting(
                conn,
                _setting_key(task.key, "auto_enabled"),
                "true" if auto_enabled else "false",
            )
        if auto_daily_cap is not None:
            upsert_setting(conn, _setting_key(task.key, "auto_daily_cap"), str(auto_daily_cap))
        if remembered_manual_limit is not None:
            upsert_setting(
                conn,
                _setting_key(task.key, "remembered_manual_limit"),
                str(remembered_manual_limit),
            )
        if request_batch_size is not None:
            upsert_setting(
                conn,
                _setting_key(task.key, "request_batch_size"),
                str(request_batch_size),
            )

    run_write_unit(conn, _persist, label=f"maintenance config {task.key}")


def migrate_maintenance_config(conn: sqlite3.Connection) -> list[dict[str, Any]]:
    """Forward-only migration from the ambiguous legacy maintenance keys.

    Called at startup, never from GET. Corrections are returned for startup logs
    and old keys are deleted so forward code has exactly one interpretation.
    """
    from alma.application.discovery import upsert_setting

    corrections: list[dict[str, Any]] = []
    for task in REGISTRY.values():
        legacy = {
            "enabled": _read_raw_setting(conn, _setting_key(task.key, "enabled")),
            "daily_cap": _read_raw_setting(conn, _setting_key(task.key, "daily_cap")),
            "batch_size": _read_raw_setting(conn, _setting_key(task.key, "batch_size")),
        }

        if _read_raw_setting(conn, _setting_key(task.key, "auto_enabled")) is None:
            requested = str(legacy["enabled"] or "").strip().lower() in {"1", "true", "yes", "on"}
            effective = requested and not task.destructive
            upsert_setting(conn, _setting_key(task.key, "auto_enabled"), "true" if effective else "false")
            if requested != effective:
                corrections.append({"task": task.key, "field": "auto_enabled", "from": requested, "to": effective})

        def _legacy_int(raw: Any, default: int, maximum: int) -> int:
            try:
                value = int(str(raw).strip())
            except (TypeError, ValueError):
                value = default
            return value if 1 <= value <= maximum else default

        if _read_raw_setting(conn, _setting_key(task.key, "auto_daily_cap")) is None:
            value = _legacy_int(legacy["daily_cap"], task.default_auto_daily_cap, task.max_auto_daily_cap)
            upsert_setting(conn, _setting_key(task.key, "auto_daily_cap"), str(value))
        if _read_raw_setting(conn, _setting_key(task.key, "remembered_manual_limit")) is None:
            value = _legacy_int(legacy["daily_cap"], task.default_manual_limit, task.max_manual_limit)
            upsert_setting(conn, _setting_key(task.key, "remembered_manual_limit"), str(value))
        if task.request_batch is not None and _read_raw_setting(
            conn, _setting_key(task.key, "request_batch_size")
        ) is None:
            value = _legacy_int(
                legacy["batch_size"], task.request_batch.default, task.request_batch.maximum
            )
            upsert_setting(conn, _setting_key(task.key, "request_batch_size"), str(value))

        conn.execute(
            "DELETE FROM discovery_settings WHERE key IN (?, ?, ?)",
            (
                _setting_key(task.key, "enabled"),
                _setting_key(task.key, "daily_cap"),
                _setting_key(task.key, "batch_size"),
            ),
        )
    return corrections


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
    """Compatibility projection for existing runners during typed migration."""
    out: dict[str, Any] = {}
    if task.scope is not None:
        out["scope"] = task.scope.default
    if task.supports_dry_run:
        out["dry_run"] = True
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


def _validated_spec(task: MaintenanceTask, spec: MaintenanceRunSpec) -> MaintenanceRunSpec:
    if spec.trigger == MaintenanceTrigger.USER:
        task.validate_max_items(spec.max_items)
    else:
        task.validate_auto_daily_cap(spec.max_items)
    if spec.target_ids and not task.supports_targets:
        raise MaintenanceValidationError(f"{task.key} does not support targets")
    if task.target_kind == TargetKind.NONE and spec.target_ids:
        raise MaintenanceValidationError(f"{task.key} does not accept target ids")
    scope = task.scope.validate(spec.scope) if task.scope is not None else None
    if task.scope is None and spec.scope is not None:
        raise MaintenanceValidationError(f"{task.key} has no scope control")
    batch = (
        task.request_batch.validate(spec.request_batch_size)
        if task.request_batch is not None
        else None
    )
    if task.request_batch is None and spec.request_batch_size is not None:
        raise MaintenanceValidationError(f"{task.key} has no configurable request batch")
    if spec.dry_run and not task.supports_dry_run:
        raise MaintenanceValidationError(f"{task.key} does not support dry-run")
    if spec.force and not task.supports_force:
        raise MaintenanceValidationError(f"{task.key} does not support force")
    return spec.model_copy(update={"scope": scope, "request_batch_size": batch})


def plan_task(
    conn: sqlite3.Connection,
    task: MaintenanceTask,
    spec: MaintenanceRunSpec,
    *,
    health_payload: Optional[dict[str, Any]] = None,
) -> MaintenanceRunPlan:
    """Build the one plan consumed by estimate, launch, Activity, and UI."""
    validated = _validated_spec(task, spec)
    payload = health_payload
    if payload is None:
        payload = (mv.get(conn, health_service.HEALTH_CORPUS_VIEW_KEY).get("payload")) or {}
    params = {
        key: value
        for key, value in {
            "scope": validated.scope,
            "dry_run": validated.dry_run if task.supports_dry_run else None,
            "force": validated.force if task.supports_force else None,
        }.items()
        if value is not None
    }
    pending = (
        len(validated.target_ids)
        if validated.target_ids
        else task_pending_count(conn, task, payload, params=params)
    )
    selected = min(max(0, int(pending)), validated.max_items)
    if task.unit == MaintenanceUnit.OPERATION:
        # An operation-unit task (rebuild / housekeeping / dedup pass) is ONE
        # pass, runnable whenever invoked regardless of the informational pending
        # count — so it never falls through to the selected==0 noop gate. The
        # pending count stays as the "is there dirty work" signal on the card.
        selected = 1
    dependencies: list[PlanDependency] = []
    for key in task.prerequisites:
        dependency = REGISTRY[key]
        dep_pending = task_pending_count(conn, dependency, payload)
        dependencies.append(
            PlanDependency(
                key=dependency.key,
                label=dependency.label,
                pending=dep_pending,
                required=not dependency.optional,
            )
        )

    from alma.services.eta import detect_auth, estimate_eta

    openalex_authed, s2_authed = detect_auth()
    eta = estimate_eta(
        task.eta_key or task.key,
        selected,
        openalex_authed=openalex_authed,
        s2_authed=s2_authed,
        batch_size=validated.request_batch_size,
    )
    expected_requests = {str(eta["source"]): int(eta["requests"])} if eta else {}
    fingerprint_payload = {
        "task_key": task.key,
        "spec": validated.model_dump(mode="json", exclude={"confirmation_token", "plan_fingerprint"}),
        "pending": pending,
        "selected": selected,
        "dependencies": [(row.key, row.pending, row.required) for row in dependencies],
    }
    fingerprint = fingerprint_plan(fingerprint_payload)
    confirmation = f"confirm:{task.key}:{fingerprint}" if task.destructive and not validated.dry_run else None
    return MaintenanceRunPlan(
        task_key=task.key,
        spec=validated,
        pending=int(pending),
        selected=int(selected),
        unit=task.unit,
        dependencies=tuple(dependencies),
        expected_requests=expected_requests,
        stage_allocations=(StageBudget(task.stage.value, int(selected), task.unit),),
        fingerprint=fingerprint,
        confirmation_token=confirmation,
        # ETA already reflects the bounded ``selected`` count, so the /estimate
        # endpoint can return it directly instead of recomputing a whole-backlog
        # ETA that ignores max_items.
        eta=eta,
    )


def _task_budget(
    conn: sqlite3.Connection,
    task: MaintenanceTask,
    *,
    health_payload: Optional[dict[str, Any]] = None,
    params: Optional[dict[str, Any]] = None,
    batch_size: Optional[int] = None,
    target_paper_ids: Optional[list[str]] = None,
    limit: Optional[int] = None,
) -> dict[str, Any]:
    """Canonical pending/batch/ETA budget for maintenance UI and launches."""
    from alma.services.eta import detect_auth, estimate_eta

    effective = {**default_params(task), **(params or {})}
    target_ids = [str(pid) for pid in (target_paper_ids or []) if str(pid).strip()]
    if target_ids:
        pending = len(target_ids)
    else:
        payload = health_payload
        if payload is None:
            payload = (mv.get(conn, health_service.HEALTH_CORPUS_VIEW_KEY).get("payload")) or {}
        pending = task_pending_count(conn, task, payload, params=effective)

    if batch_size is not None:
        if task.request_batch is None:
            raise MaintenanceValidationError(f"{task.key} has no configurable request batch")
        batch = task.request_batch.validate(batch_size)
    else:
        batch = get_task_request_batch_size(conn, task)

    pending = max(0, int(pending))
    selected = pending if limit is None else min(pending, max(0, int(limit)))
    openalex_authed, s2_authed = detect_auth()
    return {
        "params": effective,
        "target_paper_ids": target_ids,
        "target_count": len(target_ids),
        "candidates_pending": pending,
        "run_limit": int(limit) if limit is not None else None,
        "selected_items": int(selected),
        "batch_size": batch,
        "eta": estimate_eta(
            task.eta_key or task.key,
            selected,
            openalex_authed=openalex_authed,
            s2_authed=s2_authed,
            batch_size=batch,
        ),
    }


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
    budget = _task_budget(conn, task, health_payload=health_payload)
    dependencies = []
    for key in task.prerequisites:
        dependency = REGISTRY[key]
        dependencies.append(
            {
                "key": dependency.key,
                "label": dependency.label,
                "pending": task_pending_count(conn, dependency, health_payload),
                "required": not dependency.optional,
            }
        )
    params_spec: dict[str, Any] = {}
    if task.scope is not None:
        params_spec["scope"] = {
            "options": list(task.scope.options),
            "default": task.scope.default,
        }
    if task.supports_dry_run:
        params_spec["dry_run"] = {"default": True}
    return {
        "key": task.key,
        "label": task.label,
        "description": task.description,
        "cost": task.cost,
        "sources": list(task.sources),
        "local_compute": bool(task.local_compute),
        "destructive": bool(task.destructive),
        "stage": task.stage.value,
        "order": task.order,
        "unit": task.unit.value,
        "target_kind": task.target_kind.value,
        "supports_targets": task.supports_targets,
        "prerequisites": list(task.prerequisites),
        "dependencies": dependencies,
        "unlocks": list(task.unlocks),
        "optional": task.optional,
        "manual_gate": task.manual_gate,
        "repairs": list(task.health_dimensions),
        "operation_key": task.operation_key,
        "candidates_pending": budget["candidates_pending"],
        "params_spec": params_spec or None,
        "request_batch_size": budget["batch_size"],
        "request_batch_default": task.request_batch.default if task.request_batch else None,
        "request_batch_max": task.request_batch.maximum if task.request_batch else None,
        "request_batch_unit": task.request_batch.unit.value if task.request_batch else None,
        # ETA to drain the whole backlog over the network for the DEFAULT params +
        # the configured batch size (None for local / nothing-pending). Recomputed
        # each poll (shrinks as work completes); the /estimate endpoint recomputes
        # it live when the user changes scope or batch size.
        "eta": budget["eta"],
        "auto_enabled": get_task_auto_enabled(conn, task),
        "default_auto_enabled": task.default_auto_enabled,
        "auto_daily_cap": get_task_auto_daily_cap(conn, task),
        "max_auto_daily_cap": task.max_auto_daily_cap,
        "manual_limit": get_task_manual_limit(conn, task),
        "default_manual_limit": task.default_manual_limit,
        "max_manual_limit": task.max_manual_limit,
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
    budget = _task_budget(
        conn,
        task,
        health_payload=health_payload,
        params=params,
        batch_size=batch_size,
    )
    return {
        "key": task.key,
        "params": budget["params"],
        "candidates_pending": budget["candidates_pending"],
        "request_batch_size": budget["batch_size"],
        "eta": budget["eta"],
    }


def list_operations(conn: sqlite3.Connection) -> dict[str, Any]:
    """Backend-owned order, stage grouping, readiness, and operation state."""
    payload = (mv.get(conn, health_service.HEALTH_CORPUS_VIEW_KEY).get("payload")) or {}
    operations = [describe_task(conn, task, payload) for task in REGISTRY.values()]
    recommended: Optional[dict[str, Any]] = None
    for operation in operations:
        blocked_by = [
            row
            for row in operation["dependencies"]
            if row["required"] and int(row["pending"] or 0) > 0
        ]
        operation["blocked_by"] = blocked_by
        pending = int(operation["candidates_pending"] or 0)
        if pending <= 0:
            operation["readiness"] = "healthy"
        elif blocked_by:
            operation["readiness"] = "blocked"
        elif operation["manual_gate"]:
            operation["readiness"] = "manual_review"
        elif operation["optional"]:
            operation["readiness"] = "optional"
        else:
            operation["readiness"] = "ready"
        operation["recommended"] = False
        # Recommended-next must be SAFE: it drives the one-click "Run recommended
        # sequence", so destructive/manual-gate ops and optional heavy producers
        # are never auto-recommended — the user reaches those deliberately.
        if (
            recommended is None
            and pending > 0
            and not blocked_by
            and not operation["optional"]
            and not operation["manual_gate"]
            and not operation["destructive"]
        ):
            operation["recommended"] = True
            recommended = {
                "key": operation["key"],
                "label": operation["label"],
                "readiness": operation["readiness"],
                "reason": "First actionable operation in dependency order",
            }

    stages: list[dict[str, Any]] = []
    for task in REGISTRY.values():
        if stages and stages[-1]["key"] == task.stage.value:
            stages[-1]["operation_keys"].append(task.key)
        else:
            stages.append(
                {
                    "key": task.stage.value,
                    # Human label so the UI renders grouping/order from backend
                    # data only (Checkpoint G — no frontend task-key arrays).
                    "label": STAGE_LABELS.get(task.stage, task.stage.value.replace("_", " ").title()),
                    "order": task.order,
                    "operation_keys": [task.key],
                }
            )
    return {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "recommended_next": recommended,
        "stages": stages,
        "operations": operations,
    }


# --------------------------------------------------------------------------
# Run-now (manual trigger) — bounded by the configured daily cap.
# --------------------------------------------------------------------------


def _maintenance_preflight_payload(
    conn: sqlite3.Connection,
    task: MaintenanceTask,
    *,
    limit: int,
    trigger_source: str,
    target_paper_ids: Optional[list[str]] = None,
    params: Optional[dict[str, Any]] = None,
    health_payload: Optional[dict[str, Any]] = None,
    plan_fingerprint: Optional[str] = None,
) -> dict[str, Any]:
    """Activity payload describing the bounded work selected for this run."""
    budget = _task_budget(
        conn,
        task,
        health_payload=health_payload,
        params=params,
        target_paper_ids=target_paper_ids,
        limit=limit,
    )
    return {
        "task_key": task.key,
        "operation_key": task.operation_key,
        "label": task.label,
        "cost": task.cost,
        "sources": list(task.sources),
        "local_compute": bool(task.local_compute),
        "destructive": bool(task.destructive),
        "max_manual_limit": task.max_manual_limit,
        "trigger_source": trigger_source,
        # The plan this launch was authorized against — ties the Activity record
        # back to the exact estimate the user saw (None for healer/auto runs).
        "plan_fingerprint": plan_fingerprint,
        "params": budget["params"],
        "target_paper_ids": budget["target_paper_ids"],
        "target_count": budget["target_count"],
        "candidates_pending": budget["candidates_pending"],
        "run_limit": budget["run_limit"],
        "selected_items": budget["selected_items"],
        "batch_size": budget["batch_size"],
        "eta": budget["eta"],
    }


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
    health_payload: Optional[dict[str, Any]] = None,
    plan_fingerprint: Optional[str] = None,
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
    batch = get_task_request_batch_size(conn, task)
    if batch is not None and "batch_size" not in run_params:
        run_params["batch_size"] = batch
    run_params = run_params or None
    preflight = _maintenance_preflight_payload(
        conn,
        task,
        limit=limit,
        trigger_source=trigger_source,
        target_paper_ids=target_paper_ids,
        params=run_params,
        health_payload=health_payload,
        plan_fingerprint=plan_fingerprint,
    )

    def _factory(job_id: str) -> Callable[[], None]:
        return lambda: runner(job_id, limit, target_paper_ids, run_params)

    return schedule_with_envelope(
        operation_key=task.operation_key,
        job_id_prefix=task.job_id_prefix,
        trigger_source=trigger_source,
        queued_message=queued_message,
        runner_factory=_factory,
        log_message=log_message,
        log_data={"preflight": preflight},
    )


@dataclass(frozen=True, slots=True)
class MaintenanceLaunch:
    status: str
    job_id: Optional[str]
    plan: MaintenanceRunPlan


def run_task_now(
    conn: sqlite3.Connection,
    task: MaintenanceTask,
    *,
    spec: Optional[MaintenanceRunSpec] = None,
    target_paper_ids: Optional[list[str]] = None,
    params: Optional[dict[str, Any]] = None,
) -> MaintenanceLaunch:
    """Plan and schedule one atomic, bounded user run.

    ``target_paper_ids``/``params`` remain as a temporary internal compatibility
    bridge for old callers; the API/UI send ``MaintenanceRunSpec`` directly.
    """
    if spec is None:
        legacy = dict(params or {})
        targets = [str(p) for p in (target_paper_ids or []) if str(p).strip()]
        spec = MaintenanceRunSpec(
            trigger=MaintenanceTrigger.USER,
            target_ids=targets,
            max_items=len(targets) if targets else get_task_manual_limit(conn, task),
            request_batch_size=legacy.pop("batch_size", None),
            scope=legacy.pop("scope", None),
            dry_run=bool(legacy.pop("dry_run", False)),
            force=bool(legacy.pop("force", False)),
        )
        if legacy:
            raise MaintenanceValidationError(
                f"unsupported run controls for {task.key}: {', '.join(sorted(legacy))}"
            )

    plan = plan_task(conn, task, spec)
    if spec.plan_fingerprint and spec.plan_fingerprint != plan.fingerprint:
        raise MaintenanceValidationError("maintenance plan changed; refresh the estimate before running")
    if task.destructive and not plan.spec.dry_run:
        if spec.confirmation_token != plan.confirmation_token:
            raise MaintenanceValidationError(
                f"{task.key} requires an explicit confirmation token from the current plan"
            )
    if plan.selected <= 0:
        return MaintenanceLaunch(status="noop", job_id=None, plan=plan)

    from alma.api.scheduler import find_active_job

    existing = find_active_job(task.operation_key)
    if existing:
        return MaintenanceLaunch(
            status="already_running",
            job_id=str(existing.get("job_id") or "") or None,
            plan=plan,
        )

    effective = {
        key: value
        for key, value in {
            "scope": plan.spec.scope,
            "dry_run": plan.spec.dry_run if task.supports_dry_run else None,
            "force": plan.spec.force if task.supports_force else None,
            "batch_size": plan.spec.request_batch_size,
        }.items()
        if value is not None
    }
    targets = plan.spec.target_ids or None
    bits = [f"{len(targets)} selected" if targets else f"limit {plan.spec.max_items}"]
    if effective:
        bits.append(", ".join(f"{k}={v}" for k, v in effective.items()))
    scope = " · ".join(bits)
    job_id = _schedule_task(
        conn,
        task,
        limit=plan.spec.max_items,
        trigger_source="user",
        queued_message=f"{task.label} queued from Health ({scope})",
        log_message=f"{task.label} queued from Health page ({scope})",
        target_paper_ids=targets,
        params=effective or None,
        plan_fingerprint=plan.fingerprint,
    )
    return MaintenanceLaunch(status="queued" if job_id else "noop", job_id=job_id, plan=plan)


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
        for task in REGISTRY.values():
            if task.destructive:
                # Structural belt: legacy/manual DB state can never make an
                # apply/merge/delete operation eligible for automation.
                continue
            if find_active_job(task.operation_key):
                continue
            if not get_task_auto_enabled(conn, task):
                continue
            pending = task_pending_count(conn, task, payload)
            if pending <= 0:
                continue
            remaining = get_task_auto_daily_cap(conn, task) - _healer_used_today(conn, task.operation_key)
            if remaining <= 0:
                logger.info("idle maintenance: %s hit its daily cap", task.key)
                continue
            rank = _worst_severity_rank(payload, task.health_dimensions)
            candidates.append((rank, remaining, task))

        if not candidates:
            logger.info("idle maintenance: nothing enabled with pending work")
            return

        # Global admission (Checkpoint E): the typed job-policy catalog decides
        # whether a maintenance job may START now — (1) the maintenance lane is
        # SERIALIZED across EVERY maintenance namespace (not just the registry
        # tasks: a live embeddings/materialize/graphs job also holds the lane),
        # and (2) capacity is RESERVED for user/product work. One source of truth
        # for "may this background job run", shared with the rest of the system.
        from alma.api.scheduler import active_job_namespaces, scheduler_worker_capacity
        from alma.core.job_policy import admit_maintenance

        active_ns, active_total = active_job_namespaces(conn)
        admitted, reason = admit_maintenance(active_ns, active_total, scheduler_worker_capacity())
        if not admitted:
            logger.info("idle maintenance: deferring tick — %s", reason)
            return

        # Worst severity first, then the larger remaining budget.
        candidates.sort(key=lambda c: (c[0], -c[1]))
        rank, remaining, task = candidates[0]
        batch = min(remaining, task.auto_chunk_size, HEALER_PER_TICK_LIMIT)

        job_id = _schedule_task(
            conn,
            task,
            limit=batch,
            trigger_source="scheduler",
            queued_message=f"Idle maintenance: {task.label} (limit {batch})",
            log_message=f"Idle healer queued {task.label} (batch {batch})",
            health_payload=payload,
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
