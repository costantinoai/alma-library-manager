"""Global background-job policy catalog (task-29 Checkpoint E).

ONE typed classification + admission policy for EVERY background-job producer in
the system — not only the maintenance cards. §2's DRY contract names this the
single owner of "all background-job classification/admission", and §7.1 requires
that *every* immediate/cron producer carry a typed policy so the structural CI
test (`tests/test_job_policy_catalog.py`) can fail on an unclassified scheduling
call.

The catalog is keyed by **operation-key namespace** (the first dotted segment of
an `operation_key`, e.g. ``authors`` in ``authors.rehydrate_metadata``). Every
scheduled job stamps an `operation_key`, so the namespace is the stable, greppable
identity shared by a producer family; jobs in one namespace share a resource +
admission profile (they hit the same tables / sources). The maintenance registry
(`alma.services.maintenance`) is a *specialization* of this catalog — its tasks'
operation keys fall under namespaces classified here as ``MAINTENANCE`` — not a
parallel abstraction.

Forward-only: this module describes the CURRENT producer set. Adding a new
scheduling call in a new namespace (or a new scheduling module) fails the
structural test until a deliberate classification is added here.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import StrEnum


class JobClass(StrEnum):
    """The six admission classes from §7.1."""

    MAINTENANCE = "maintenance"  # ordered corpus/author repair (the Health DAG)
    SCHEDULED_PRODUCT = "scheduled_product"  # cron-driven product refresh
    USER_PRODUCT = "user_product"  # explicit user/product action
    NOTIFICATION = "notification"  # alerts + plugin delivery
    DATA_MANAGEMENT = "data_management"  # backup/restore/reset/dedup (destructive)
    HOUSEKEEPING = "housekeeping"  # log prune / vacuum (DB-exclusive, quiet slot)


class ResourceKind(StrEnum):
    DB_READER = "db_reader"
    DB_WRITER = "db_writer"
    DB_EXCLUSIVE = "db_exclusive"  # needs a quiet slot (vacuum)
    NETWORK = "network"
    LOCAL_MODEL = "local_model"  # SPECTER2 / GPU


@dataclass(frozen=True, slots=True)
class JobPolicy:
    """The admission/coordination contract for one producer namespace."""

    namespace: str
    job_class: JobClass
    # Lower number = higher-priority lane (§7 manual > action > continuation >
    # idle > housekeeping). Used by the dispatcher/healer to order contention.
    priority: int
    resources: frozenset[ResourceKind]
    sources: tuple[str, ...] = ()  # rate-limited external sources it shares
    coalescing: bool = True  # dedups via find_active_job on its operation_key
    durable: bool = False  # pending work survives restart (ledger-backed)
    cancellable: bool = True  # cooperative cancel at chunk boundaries
    activity_visible: bool = True
    max_concurrency: int = 1  # concurrent jobs allowed in this namespace
    may_overlap_maintenance: bool = True  # may run while the maintenance lane drains
    destructive: bool = False  # mutates/removes rows irreversibly
    # Max width of THIS job's *nested* fan-out (a ThreadPoolExecutor it spawns
    # internally) while it runs as a background job. `_scheduler_max_workers`
    # bounds how many jobs run at once; this bounds how wide each one fans out,
    # so N concurrent jobs can't each open a 12-worker pool and storm SQLite /
    # the external APIs. `core.concurrency.bounded_thread_pool` reads it; the
    # interactive request path has no job context and is never clamped. Generous
    # for latency-sensitive network lanes that carry their own deadlines
    # (discovery/feed/lenses), tighter for DB-writing namespaces.
    fanout_budget: int = 4

    def __post_init__(self) -> None:
        # Structural invariants every policy must satisfy (cheap, fail-fast).
        if self.destructive and self.job_class not in {
            JobClass.DATA_MANAGEMENT,
            JobClass.MAINTENANCE,
        }:
            raise ValueError(f"{self.namespace}: destructive jobs must be data_management/maintenance")
        if self.job_class == JobClass.HOUSEKEEPING and ResourceKind.DB_EXCLUSIVE not in self.resources:
            raise ValueError(f"{self.namespace}: housekeeping must declare DB_EXCLUSIVE")
        if self.fanout_budget < 1:
            raise ValueError(f"{self.namespace}: fanout_budget must be >= 1")


_R = ResourceKind


def _p(namespace: str, job_class: JobClass, priority: int, resources: set[ResourceKind], **kw) -> JobPolicy:
    return JobPolicy(namespace=namespace, job_class=job_class, priority=priority, resources=frozenset(resources), **kw)


# --------------------------------------------------------------------------
# The catalog. One entry per operation-key namespace in use across the codebase.
# Priority lanes: 10 user/product action · 20 scheduled product · 30 maintenance
# · 40 notification · 50 data-management · 60 housekeeping. The serialized
# maintenance lane (§7) is every MAINTENANCE-class namespace at max_concurrency 1
# and may_overlap_maintenance False.
# --------------------------------------------------------------------------

JOB_POLICIES: dict[str, JobPolicy] = {p.namespace: p for p in (
    # ---- Maintenance lane (serialized; the Health DAG) ----------------------
    _p("ai", JobClass.MAINTENANCE, 30, {_R.NETWORK, _R.DB_WRITER, _R.LOCAL_MODEL},
       sources=("semantic_scholar", "openalex"), durable=True, max_concurrency=1, may_overlap_maintenance=False),
    _p("embeddings", JobClass.MAINTENANCE, 30, {_R.NETWORK, _R.DB_WRITER, _R.LOCAL_MODEL},
       sources=("semantic_scholar",), durable=True, max_concurrency=1, may_overlap_maintenance=False),
    _p("papers", JobClass.MAINTENANCE, 30, {_R.NETWORK, _R.DB_WRITER},
       sources=("openalex", "semantic_scholar", "crossref"), durable=True, max_concurrency=1, may_overlap_maintenance=False),
    _p("corpus", JobClass.MAINTENANCE, 30, {_R.NETWORK, _R.DB_WRITER},
       sources=("openalex", "crossref"), durable=True, max_concurrency=1, may_overlap_maintenance=False),
    _p("authors", JobClass.MAINTENANCE, 30, {_R.NETWORK, _R.DB_WRITER, _R.LOCAL_MODEL},
       sources=("openalex", "orcid", "semantic_scholar", "crossref"), durable=True, max_concurrency=1, may_overlap_maintenance=False),
    _p("graphs", JobClass.MAINTENANCE, 30, {_R.NETWORK, _R.DB_WRITER},
       sources=("openalex",), max_concurrency=1, may_overlap_maintenance=False),
    _p("materialize", JobClass.MAINTENANCE, 30, {_R.DB_WRITER},
       max_concurrency=1, may_overlap_maintenance=False),
    _p("topics", JobClass.MAINTENANCE, 30, {_R.DB_WRITER},
       max_concurrency=1, may_overlap_maintenance=False),
    _p("maintenance", JobClass.MAINTENANCE, 30, {_R.DB_WRITER},
       max_concurrency=1, may_overlap_maintenance=False),
    # ---- User / product work (must keep capacity; may overlap maintenance) ---
    # Discovery/feed/lenses fan out across many retrieval lanes that already
    # carry per-lane deadlines + 429 abandonment; a generous fanout_budget keeps
    # a user-initiated (backgrounded) refresh fast while still being a declared,
    # greppable ceiling rather than an unbounded pool.
    _p("discovery", JobClass.USER_PRODUCT, 10, {_R.NETWORK, _R.DB_WRITER, _R.LOCAL_MODEL},
       sources=("openalex", "semantic_scholar"), max_concurrency=2, fanout_budget=12),
    _p("feed", JobClass.USER_PRODUCT, 10, {_R.NETWORK, _R.DB_WRITER}, sources=("openalex",),
       max_concurrency=2, fanout_budget=8),
    _p("lenses", JobClass.USER_PRODUCT, 10, {_R.DB_WRITER, _R.LOCAL_MODEL}, max_concurrency=2, fanout_budget=8),
    _p("publications", JobClass.USER_PRODUCT, 10, {_R.NETWORK, _R.DB_WRITER},
       sources=("openalex", "semantic_scholar", "crossref"), max_concurrency=2),
    _p("imports", JobClass.USER_PRODUCT, 10, {_R.NETWORK, _R.DB_WRITER}, sources=("openalex",), max_concurrency=2),
    _p("tags", JobClass.USER_PRODUCT, 10, {_R.DB_WRITER}, max_concurrency=2),
    _p("operations", JobClass.USER_PRODUCT, 10, {_R.NETWORK, _R.DB_WRITER},
       sources=("openalex", "semantic_scholar"), max_concurrency=2),
    _p("fetch", JobClass.USER_PRODUCT, 10, {_R.NETWORK, _R.DB_WRITER}, sources=("openalex",), max_concurrency=2),
    _p("onboarding", JobClass.USER_PRODUCT, 10, {_R.NETWORK, _R.DB_WRITER}, sources=("openalex",), max_concurrency=1),
    # ---- Notification / integration ----------------------------------------
    _p("alerts", JobClass.NOTIFICATION, 40, {_R.DB_WRITER, _R.NETWORK}, max_concurrency=1),
    _p("plugins", JobClass.NOTIFICATION, 40, {_R.NETWORK}, durable=False, max_concurrency=2),
    # ---- Data-management / destructive utilities ---------------------------
    _p("library", JobClass.DATA_MANAGEMENT, 50, {_R.DB_WRITER, _R.DB_EXCLUSIVE},
       coalescing=True, max_concurrency=1, may_overlap_maintenance=False, destructive=True),
    _p("signal", JobClass.DATA_MANAGEMENT, 50, {_R.DB_WRITER}, max_concurrency=1, destructive=True),
    # ---- Housekeeping (DB-exclusive, quiet slot) ---------------------------
    _p("db", JobClass.HOUSEKEEPING, 60, {_R.DB_WRITER, _R.DB_EXCLUSIVE},
       coalescing=True, max_concurrency=1, may_overlap_maintenance=False),
)}


# Allowlist of modules that legitimately contain a raw scheduling call
# (`schedule_immediate` / `schedule_with_envelope`). The structural test asserts
# the scanned set is a SUBSET of this manifest, so a scheduling call appearing in
# a NEW module fails CI until the producer is classified above and the module is
# recorded here. (A superset is tolerated — refactors that REMOVE the last call
# from a module must not break CI; the guard that matters is "no NEW unclassified
# scheduling site".) Paths are relative to `src/alma/`. Note: onboarding / feed /
# discovery schedule via the `schedule_pending_*` wrapper helpers rather than a
# raw call, so they may not contain the literal token — they are kept here as
# known producers for documentation.
SCHEDULING_MODULES: frozenset[str] = frozenset({
    "api/scheduler.py",
    "core/job_envelope.py",
    "api/routes/ai.py",
    "api/routes/alerts.py",
    "api/routes/authors.py",
    "api/routes/discovery.py",
    "api/routes/feed.py",
    "api/routes/graphs.py",
    "api/routes/imports.py",
    "api/routes/lenses.py",
    "api/routes/library_mgmt.py",
    "api/routes/operations.py",
    "api/routes/plugins.py",
    "api/routes/publications.py",
    "api/routes/tags.py",
    "api/routes/onboarding.py",
    "application/followed_authors.py",
    "application/library.py",
    "application/materialized_views.py",
    "application/feed.py",
    "application/discovery/__init__.py",
    "library/importer.py",
    "services/s2_vectors.py",
    "services/title_resolution.py",
    "services/corpus_rehydrate.py",
    "services/author_hydrate.py",
    "services/embedding_chain.py",
    "services/maintenance.py",
})


def policy_for(operation_key: str) -> JobPolicy | None:
    """The policy for an operation key, resolved by its namespace prefix."""
    namespace = str(operation_key or "").split(".", 1)[0].split(":", 1)[0]
    return JOB_POLICIES.get(namespace)


def reserved_user_capacity(total_workers: int) -> int:
    """Workers reserved for USER_PRODUCT work so maintenance can't consume the
    whole pool (§7 "reserve capacity for explicit user/product work"). At least
    one, and never the whole pool."""
    return max(1, min(total_workers - 1, total_workers // 3))


def admit_maintenance(
    active_namespaces: set[str], active_total: int, total_workers: int
) -> tuple[bool, str]:
    """Decide whether a maintenance job may START now, given the live job mix.

    The two §7 admission rules, as one pure (testable) function over the catalog
    + live counts:

    1. **Serialized maintenance lane** — at most one maintenance-class job in
       flight, so the repair DAG never races itself for the writer/sources.
    2. **Reserved user capacity** — starting maintenance must still leave
       `reserved_user_capacity` free worker slots for explicit user/product work,
       so a backlog drain can't starve a user's click.

    Returns ``(admit, reason)``; the reason is logged when deferred.
    """
    if any(ns in MAINTENANCE_NAMESPACES for ns in active_namespaces):
        return (False, "maintenance lane busy (serialized — one maintenance job at a time)")
    reserved = reserved_user_capacity(total_workers)
    free_after_start = int(total_workers) - int(active_total) - 1
    if free_after_start < reserved:
        return (False, f"reserving {reserved} worker slot(s) for user/product work")
    return (True, "admitted")


# Convenience views used by admission logic + tests.
MAINTENANCE_NAMESPACES: frozenset[str] = frozenset(
    ns for ns, p in JOB_POLICIES.items() if p.job_class == JobClass.MAINTENANCE
)
DESTRUCTIVE_NAMESPACES: frozenset[str] = frozenset(
    ns for ns, p in JOB_POLICIES.items() if p.destructive
)
