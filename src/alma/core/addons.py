"""The add-on seam (D27): what a non-core feature declares and how core calls it.

Core is Feed, Discovery, Authors and Library. Everything else ships as a folder
under ``src/alma/addons/<id>/`` that declares one :class:`AddOn` as ``ADDON`` in
its ``__init__.py``. Deleting that folder must leave core working, so:

* **One explicit list.** :data:`ADDON_MODULES` names every add-on by string. This
  is the one place core touches an add-on's name, and there is no discovery.
* **Core never imports ``alma.addons``.** It calls add-ons through the functions
  here. An add-on may import core. A static test enforces the direction.
* **The switch is ``plugins.<id>.enabled`` in ``settings.json``.** It is read on
  every call, so a change takes effect at runtime. Routes stay mounted behind a
  gate answering 404, and jobs are removed from the scheduler.
* **An add-on's tables are the add-on's.** Core bootstrap does not create them;
  :func:`ensure_schema` does, on first enable. Disable keeps them. Only the
  separate Remove deletes them.

The manifest module must stay light: ``alma.api.deps`` reaches this registry
early, so a manifest that imports routes or services at import time would
create a cycle. Heavy pieces go behind the lazy callables below.
"""

from __future__ import annotations

import importlib
import logging
import os
import re
import shutil
import sqlite3
import threading
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, field
from datetime import datetime
from functools import partial
from pathlib import Path
from typing import Any

from alma.config import (
    activation_setting_key,
    delete_settings_keys,
    get_all_settings,
    get_data_dir,
    get_db_path,
    get_setting,
    update_settings,
)

logger = logging.getLogger(__name__)

#: Every official add-on, in the order their routes, jobs and pills appear.
ADDON_MODULES: tuple[str, ...] = ()

_PACKAGE = "alma.addons"
_SQL_NAME = re.compile(r"^[a-z_][a-z0-9_]*$")


@dataclass(frozen=True)
class JobSpec:
    """One periodic job, registered only while its add-on is on."""

    job_id: str
    func: Callable[[], None]
    name: str
    description: str
    interval_minutes: Callable[[], int]


def _no_routers() -> Sequence[tuple[Any, str, Sequence[str]]]:
    return ()


def _no_policies() -> Sequence[Any]:
    return ()


def _no_paths() -> Sequence[Path]:
    return ()


def _no_plugins() -> Sequence[Any]:
    return ()


def _no_user_data(_conn: sqlite3.Connection) -> bool:
    return False


@dataclass(frozen=True)
class AddOn:
    """Everything core may know about one add-on."""

    id: str
    name: str
    description: str
    docs_path: str
    default_enabled: bool = False
    #: Add-on ids this one needs on. The switch refuses in both directions.
    requires: tuple[str, ...] = ()
    #: Consulted once, when the activation key is absent, so an upgrade keeps a
    #: feature in use on. Must tolerate its tables not existing.
    has_user_data: Callable[[sqlite3.Connection], bool] = _no_user_data
    #: ``(router, prefix, tags)`` triples, imported lazily.
    routers: Callable[[], Sequence[tuple[Any, str, Sequence[str]]]] = _no_routers
    jobs: tuple[JobSpec, ...] = ()
    job_policies: Callable[[], Sequence[Any]] = _no_policies
    ddl: tuple[str, ...] = ()
    #: Drop order, children first. Remove deletes these and nothing else.
    tables: tuple[str, ...] = ()
    schema_version: int = 1
    #: ``(version, migrate(conn))`` pairs, run above the stored version.
    migrations: tuple[tuple[int, Callable[[sqlite3.Connection], None]], ...] = ()
    #: ``(table, follows_merge)``: tables holding a ``paper_id``. A preprint
    #: group moves every one of them onto its published root; a duplicate merge
    #: rewires only those with ``follows_merge``. Read for every PRESENT add-on,
    #: on or off, so a disabled add-on's rows still follow their paper.
    paper_tables: tuple[tuple[str, bool], ...] = ()
    #: ``settings.json`` keys this add-on owns, with their defaults. Remove
    #: deletes them. The activation key is not one of them.
    settings_defaults: Mapping[str, Any] = field(default_factory=dict)
    #: Secret-store keys this add-on owns. Remove deletes them.
    secret_keys: tuple[str, ...] = ()
    #: Directories this add-on owns, inside the data directory.
    file_roots: Callable[[], Sequence[Path]] = _no_paths
    plugins: Callable[[], Sequence[Any]] = _no_plugins
    #: ``home_counts(db, day_start)``: figures for Home's activity row, each
    #: ``{label, hint, today, href}``.
    home_counts: Callable[[sqlite3.Connection, str], Sequence[dict[str, Any]]] | None = None
    #: ``home_pills(db, now)``: pills for Home's status line.
    home_pills: Callable[[sqlite3.Connection, datetime], Sequence[dict[str, Any]]] | None = None
    #: ``health_checks(db)``: entries for the Health operational states.
    health_checks: Callable[[sqlite3.Connection], Sequence[dict[str, Any]]] | None = None
    #: ``nav_badge(db)``: the count on this add-on's sidebar entry.
    nav_badge: Callable[[sqlite3.Connection], int] | None = None
    #: Operation-key namespaces that block Remove while active.
    operation_prefixes: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        for name in (self.id, *self.tables, *(table for table, _ in self.paper_tables)):
            if not _SQL_NAME.match(name):
                raise ValueError(f"Add-on {self.id!r}: {name!r} is not a plain identifier")
        if activation_setting_key(self.id) in self.settings_defaults:
            raise ValueError(f"Add-on {self.id!r}: the activation key is not a setting default")


class AddonConflict(RuntimeError):
    """A switch or Remove request that cannot proceed now (HTTP 409)."""


class AddonNotFound(KeyError):
    """No add-on has this id (HTTP 404)."""


_load_lock = threading.RLock()
_loaded: tuple[AddOn, ...] | None = None
#: Add-on id -> why it is held off this process (a newer schema, a failed DDL).
_blocked: dict[str, str] = {}


def _import_addon(module_name: str) -> AddOn | None:
    """Import one add-on, or None when its folder is not in this build.

    Only the add-on's own absence is tolerated. Any other import error inside
    it stays fatal, so a broken add-on cannot pass for a missing one.
    """
    try:
        module = importlib.import_module(module_name)
    except ModuleNotFoundError as exc:
        if exc.name in {module_name, _PACKAGE}:
            logger.info("Add-on %s is not in this build", module_name)
            return None
        raise
    return module.ADDON


def present() -> tuple[AddOn, ...]:
    """Every add-on in this build, enabled or not."""
    global _loaded
    if _loaded is None:
        with _load_lock:
            if _loaded is None:
                found = [addon for name in ADDON_MODULES if (addon := _import_addon(name))]
                ids = [addon.id for addon in found]
                if len(ids) != len(set(ids)):
                    raise ValueError(f"Add-on ids must be unique: {ids}")
                _loaded = tuple(found)
    return _loaded


def reset_for_tests() -> None:
    """Forget the loaded list and blocks, so a test can install its own."""
    global _loaded
    with _load_lock:
        _loaded = None
        _blocked.clear()


def get(addon_id: str) -> AddOn:
    """The add-on with this id. Raises :class:`AddonNotFound`."""
    for addon in present():
        if addon.id == addon_id:
            return addon
    raise AddonNotFound(addon_id)


def switch_is_on(addon_id: str) -> bool:
    """The stored switch alone, whatever a block says.

    Remove reads this one: a blocked add-on answers False to :func:`is_enabled`
    while its switch is still on, and that must not count as switched off.
    """
    key = activation_setting_key(addon_id)
    value = get_setting(key, False)
    if not isinstance(value, bool):
        raise ValueError(f"{key} must be a boolean")
    return value


def is_enabled(addon_id: str) -> bool:
    """Switched on, in this build, and not blocked. Read on every call."""
    if not any(addon.id == addon_id for addon in present()) or addon_id in _blocked:
        return False
    return switch_is_on(addon_id)


def enabled() -> tuple[AddOn, ...]:
    """Every add-on that is on right now."""
    return tuple(addon for addon in present() if is_enabled(addon.id))


def blocked_reason(addon_id: str) -> str | None:
    """Why this add-on is held off, or None."""
    return _blocked.get(addon_id)


# ---------------------------------------------------------------------------
# Schema
# ---------------------------------------------------------------------------


def schema_key(addon_id: str) -> str:
    """The ``discovery_settings`` row holding this add-on's schema version."""
    return f"addon.{addon_id}.schema_version"


def _stored_version(conn: sqlite3.Connection, addon_id: str) -> int:
    row = conn.execute(
        "SELECT value FROM discovery_settings WHERE key = ?", (schema_key(addon_id),)
    ).fetchone()
    return int(row[0]) if row else 0


def _existing_tables(conn: sqlite3.Connection, tables: Sequence[str]) -> set[str]:
    if not tables:
        return set()
    placeholders = ", ".join("?" for _ in tables)
    rows = conn.execute(
        f"SELECT name FROM sqlite_master WHERE type = 'table' AND name IN ({placeholders})",
        tuple(tables),
    ).fetchall()
    return {str(row[0]) for row in rows}


def ensure_schema(conn: sqlite3.Connection, addon_id: str) -> bool:
    """Bring this add-on's tables up to its declared version.

    The outermost writer, so it opens its own write unit: never call it inside
    one. A database written by a newer build of the add-on blocks that add-on
    alone, loudly, and core keeps working. Returns True when the add-on may run.
    """
    from alma.core.db_write import run_write_unit

    addon = get(addon_id)
    stored = _stored_version(conn, addon_id)
    if stored > addon.schema_version:
        reason = (
            f"its tables were written by a newer ALMa (schema {stored}, "
            f"this build reads {addon.schema_version})"
        )
        _blocked[addon_id] = reason
        logger.error("Add-on %s is off: %s", addon_id, reason)
        return False
    if stored == addon.schema_version and len(_existing_tables(conn, addon.tables)) == len(
        addon.tables
    ):
        return True

    def _apply() -> None:
        for version, migrate in sorted(addon.migrations, key=lambda pair: pair[0]):
            if version > stored:
                migrate(conn)
        for statement in addon.ddl:
            conn.execute(statement)
        conn.execute(
            """
            INSERT INTO discovery_settings (key, value, updated_at)
            VALUES (?, ?, CURRENT_TIMESTAMP)
            ON CONFLICT(key) DO UPDATE SET value = excluded.value, updated_at = excluded.updated_at
            """,
            (schema_key(addon_id), str(addon.schema_version)),
        )

    run_write_unit(conn, _apply, label=f"addon schema {addon_id}")
    return True


def startup(conn: sqlite3.Connection) -> None:
    """Resolve first-run switches and bring enabled add-ons' schema current.

    Runs after core bootstrap and before the scheduler. An absent activation key
    gets ``default_enabled or has_user_data(conn)``, once; a key that exists is
    never changed. There is no fresh-versus-upgrade branch: restoring a database
    without its settings file takes the same path. One add-on failing here costs
    that add-on, never core.
    """
    raw = get_all_settings()
    resolved: dict[str, bool] = {}
    for addon in present():
        key = activation_setting_key(addon.id)
        if key not in raw:
            resolved[key] = bool(addon.default_enabled or addon.has_user_data(conn))
    if resolved:
        update_settings(resolved)
        logger.info("Add-on first-run switches: %s", resolved)
    for addon in enabled():
        try:
            ensure_schema(conn, addon.id)
        except Exception as exc:
            _blocked[addon.id] = f"its schema could not be applied: {type(exc).__name__}"
            logger.exception("Add-on %s is off: schema failed", addon.id)


# ---------------------------------------------------------------------------
# Jobs
# ---------------------------------------------------------------------------


def _run_job(addon_id: str, spec: JobSpec) -> None:
    """Run one periodic tick, unless the add-on went off after it was dispatched."""
    if not is_enabled(addon_id):
        logger.info("Skipped %s: add-on %s is off", spec.job_id, addon_id)
        return
    spec.func()


def sync_jobs(addon_id: str, sched: Any = None) -> None:
    """Make the scheduler match this add-on's switch. Safe at any time.

    ``sched`` is the scheduler during startup. A later switch passes nothing and
    this is a no-op when no scheduler runs (``SCHEDULER_ENABLED=false``).
    """
    from alma.api import scheduler

    target = sched if sched is not None else scheduler.get_running_scheduler()
    if target is None:
        return
    on = is_enabled(addon_id)
    for spec in get(addon_id).jobs:
        scheduler.register_interval_job(
            target,
            job_id=spec.job_id,
            func=partial(_run_job, addon_id, spec),
            name=spec.name,
            description=spec.description,
            enabled=on,
            interval_minutes=spec.interval_minutes(),
        )


def job_policy(namespace: str) -> Any | None:
    """The policy an add-on declares for this operation namespace, or None."""
    for addon in present():
        for policy in addon.job_policies():
            if policy.namespace == namespace:
                return policy
    return None


def active_operations(conn: sqlite3.Connection, addon_id: str) -> list[str]:
    """This add-on's operation namespaces with work still active. Read-only."""
    from alma.api.scheduler import active_job_namespaces

    namespaces, _total = active_job_namespaces(conn)
    return sorted(namespaces & set(get(addon_id).operation_prefixes))


# ---------------------------------------------------------------------------
# Switch
# ---------------------------------------------------------------------------


def set_enabled(conn: sqlite3.Connection, addon_id: str, value: bool) -> None:
    """The switch path: dependency check, schema, setting, jobs.

    The Health refresh follows on its own: the request middleware refreshes on
    every successful write. An in-flight job finishes and is not killed.
    """
    addon = get(addon_id)
    if value:
        off = [needed for needed in addon.requires if not is_enabled(needed)]
        if off:
            raise AddonConflict(f"{addon.name} needs {', '.join(off)} switched on first")
        from alma.api.scheduler import find_active_job

        if find_active_job(remove_operation_key(addon_id)):
            raise AddonConflict(f"{addon.name} is being removed")
        # A block lifts only once the schema is known good.
        try:
            ready = ensure_schema(conn, addon_id)
        except Exception as exc:
            _blocked[addon_id] = f"its schema could not be applied: {type(exc).__name__}"
            logger.exception("Add-on %s is off: schema failed", addon_id)
            raise AddonConflict(f"{addon.name} cannot start: {_blocked[addon_id]}") from exc
        if not ready:
            raise AddonConflict(f"{addon.name} cannot start: {_blocked[addon_id]}")
        _blocked.pop(addon_id, None)
    else:
        dependents = [
            other.name
            for other in enabled()
            if addon_id in other.requires and other.id != addon_id
        ]
        if dependents:
            raise AddonConflict(f"{', '.join(dependents)} needs {addon.name} switched on")
    update_settings({activation_setting_key(addon_id): value})
    sync_jobs(addon_id)


# ---------------------------------------------------------------------------
# What an add-on holds, and Remove
# ---------------------------------------------------------------------------


def remove_operation_key(addon_id: str) -> str:
    """The Activity key of this add-on's Remove job."""
    return f"addons.remove:{addon_id}"


def _file_root(path: Path) -> Path:
    """Resolve a file root, refusing anything Remove must never delete.

    A root must sit strictly inside the data directory or the database
    directory, the two places ALMa stores files. It may not be either of them,
    an ancestor of either, or any folder holding the database file, even when
    that folder is itself inside the data directory.
    """
    resolved = Path(os.path.realpath(path))
    db_file = Path(os.path.realpath(get_db_path()))
    bases = {Path(os.path.realpath(get_data_dir())), db_file.parent}
    inside = any(base in resolved.parents for base in bases)
    covers = resolved in db_file.parents or any(
        resolved == base or resolved in base.parents for base in bases
    )
    if not inside or covers:
        raise AddonConflict(f"Refusing a file root Remove must not delete: {resolved}")
    return resolved


def footprint(conn: sqlite3.Connection, addon_id: str) -> dict[str, Any]:
    """What Remove would delete: rows per table, files and bytes. Read-only."""
    addon = get(addon_id)
    existing = _existing_tables(conn, addon.tables)
    tables = [
        {
            "name": table,
            # Table names come from the manifest and pass `_SQL_NAME`.
            "rows": int(conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]),  # noqa: S608
        }
        for table in addon.tables
        if table in existing
    ]
    files = 0
    size = 0
    for root in addon.file_roots():
        for folder, _dirs, names in os.walk(_file_root(root)):
            for name in names:
                files += 1
                size += os.path.getsize(os.path.join(folder, name))
    settings = get_all_settings()
    return {
        "tables": tables,
        "rows": sum(table["rows"] for table in tables),
        "files": files,
        "bytes": size,
        "settings": sorted(key for key in addon.settings_defaults if key in settings),
        "secrets": list(addon.secret_keys),
    }


def drop_tables(conn: sqlite3.Connection, addon_id: str) -> list[str]:
    """Drop this add-on's tables and schema row. The caller owns the write unit."""
    addon = get(addon_id)
    existing = _existing_tables(conn, addon.tables)
    dropped = []
    for table in addon.tables:
        if table in existing:
            conn.execute(f"DROP TABLE {table}")  # noqa: S608 -- manifest, `_SQL_NAME`
            dropped.append(table)
    conn.execute("DELETE FROM discovery_settings WHERE key = ?", (schema_key(addon_id),))
    return dropped


def remove_files(addon_id: str) -> list[str]:
    """Delete this add-on's directories. Never inside a write transaction."""
    removed = []
    for root in get(addon_id).file_roots():
        resolved = _file_root(root)
        if resolved.exists():
            shutil.rmtree(resolved)
            removed.append(str(resolved))
    return removed


def remove_settings(addon_id: str) -> dict[str, list[str]]:
    """Delete this add-on's settings and secrets. The activation key stays."""
    from alma.core.secrets import delete_secret

    addon = get(addon_id)
    delete_settings_keys(list(addon.settings_defaults))
    for secret in addon.secret_keys:
        delete_secret(secret)
    return {"settings": list(addon.settings_defaults), "secrets": list(addon.secret_keys)}


# ---------------------------------------------------------------------------
# Hook collectors: what core surfaces read
# ---------------------------------------------------------------------------


def _collect(
    addon: AddOn, hook: Callable[..., Sequence[dict[str, Any]]] | None, *args: Any
) -> list[dict[str, Any]]:
    """Run one hook. A broken add-on costs its own entry, never a core surface."""
    if hook is None:
        return []
    try:
        return list(hook(*args))
    except Exception:
        logger.exception("Add-on %s hook failed", addon.id)
        return []


def plugins() -> tuple[Any, ...]:
    """Integration manifests of the add-ons that are on."""
    return tuple(manifest for addon in enabled() for manifest in addon.plugins())


def paper_tables(*, follows_merge: bool | None = None) -> tuple[str, ...]:
    """Tables with a ``paper_id`` across every PRESENT add-on, on or off."""
    return tuple(
        table
        for addon in present()
        for table, follows in addon.paper_tables
        if follows_merge is None or follows == follows_merge
    )


def data_tables() -> tuple[str, ...]:
    """Every table of every present add-on, in drop order."""
    return tuple(table for addon in present() for table in addon.tables)


def home_counts(conn: sqlite3.Connection, day_start: str) -> list[dict[str, Any]]:
    """Figures for Home's activity row, each tagged with its add-on id."""
    return [
        {"id": addon.id, **entry}
        for addon in enabled()
        for entry in _collect(addon, addon.home_counts, conn, day_start)
    ]


def home_pills(conn: sqlite3.Connection, now: datetime) -> list[dict[str, Any]]:
    """Home status pills of the add-ons that are on."""
    return [
        pill for addon in enabled() for pill in _collect(addon, addon.home_pills, conn, now)
    ]


def health_checks(conn: sqlite3.Connection) -> list[dict[str, Any]]:
    """Operational states of the add-ons that are on."""
    return [
        check for addon in enabled() for check in _collect(addon, addon.health_checks, conn)
    ]


def bootstrap_state(conn: sqlite3.Connection) -> list[dict[str, Any]]:
    """``{id, enabled, badge}`` per present add-on, for the first page load."""
    state = []
    for addon in present():
        on = is_enabled(addon.id)
        badge = 0
        if on and addon.nav_badge is not None:
            try:
                badge = int(addon.nav_badge(conn))
            except Exception:
                logger.exception("Add-on %s nav badge failed", addon.id)
        state.append({"id": addon.id, "enabled": on, "badge": badge})
    return state
