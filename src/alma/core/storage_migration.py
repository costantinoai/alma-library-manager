"""Startup storage validation, legacy migration, and dev-profile seeding.

This module is intentionally DECOUPLED from ``alma.config`` (which is
forward-only and just resolves the *current* canonical locations). Here
lives the one-time "is the data where the current code expects it, and if
not, what do we do about it?" logic — validators + migrators triggered by
a validation miss, per the project's schema/config principle.

What it does at startup (`validate_and_migrate_storage`):

1. Resolve the current OS-standard locations from ``alma.config``.
2. If the current DB / config already exist → log and proceed (the common
   case, and the ONLY case in Docker, where ``DATA_DIR`` is pinned to
   ``/app/data`` so "current" == the existing volume).
3. Otherwise look in legacy locations (a ``./data`` next to the project
   root or CWD, a project-root ``.env`` / ``settings.json``).
4. If legacy data is found, decide — SEPARATELY for the database and for
   config — whether to **migrate** it to the new location or **start
   fresh**:
   - interactive TTY: prompt the user;
   - non-interactive: read ``ALMA_DB_MIGRATION`` / ``ALMA_CONFIG_MIGRATION``
     (``migrate`` | ``fresh``); if unset, **halt loudly** rather than
     silently guess with the user's data.

Every step logs a clear message so the decision and outcome are visible.
"""
from __future__ import annotations

import logging
import os
import shutil
import sys
from pathlib import Path
from typing import Optional

logger = logging.getLogger(__name__)

# Files that constitute "config" alongside the DB.
_SETTINGS_NAME = "settings.json"
_ENV_NAME = ".env"
_DB_NAME = "scholar.db"
# scholar.db carries WAL/SHM siblings that must travel with it.
_DB_SIDECARS = (f"{_DB_NAME}-wal", f"{_DB_NAME}-shm")


class StorageMigrationHalt(RuntimeError):
    """Raised when a migration decision is required but cannot be made
    (legacy data found, non-interactive, and no policy env var set)."""


# --------------------------------------------------------------------------
# Legacy-location discovery
# --------------------------------------------------------------------------
def _legacy_roots() -> list[Path]:
    """Directories the pre-relocation code wrote into: a ``./data`` next to
    the project root and next to the current working directory."""
    roots: list[Path] = []
    try:
        from alma.config import get_project_root

        roots.append(get_project_root())
    except Exception:  # pragma: no cover - defensive
        pass
    roots.append(Path.cwd())
    # de-dup while preserving order
    seen: set[Path] = set()
    out: list[Path] = []
    for r in roots:
        rp = r.resolve()
        if rp not in seen:
            seen.add(rp)
            out.append(rp)
    return out


def find_legacy_db() -> Optional[Path]:
    """Return the first legacy ``data/scholar.db`` that exists, else None."""
    for root in _legacy_roots():
        candidate = root / "data" / _DB_NAME
        if candidate.exists():
            return candidate
    return None


def find_legacy_config() -> dict[str, Optional[Path]]:
    """Return legacy ``settings.json`` / ``.env`` paths that exist."""
    found: dict[str, Optional[Path]] = {"settings": None, "env": None}
    for root in _legacy_roots():
        if found["settings"] is None and (root / _SETTINGS_NAME).exists():
            found["settings"] = root / _SETTINGS_NAME
        if found["env"] is None and (root / _ENV_NAME).exists():
            found["env"] = root / _ENV_NAME
    return found


# --------------------------------------------------------------------------
# Decision (prompt / env policy / halt)
# --------------------------------------------------------------------------
def _decide(kind: str, legacy_desc: str, new_desc: str, env_var: str,
            interactive: bool) -> str:
    """Return 'migrate' or 'fresh'. Prompt when interactive, else read the
    policy env var, else halt loudly."""
    policy = (os.getenv(env_var) or "").strip().lower()
    if policy in {"migrate", "fresh"}:
        logger.info("[storage] %s: %s policy from %s=%s", kind,
                    "migrating" if policy == "migrate" else "starting fresh",
                    env_var, policy)
        return policy

    if interactive:
        print(f"\n  ── ALMa storage: existing {kind} found ─────────────────────")
        print(f"     Legacy : {legacy_desc}")
        print(f"     New    : {new_desc}")
        print(f"     [m] migrate the existing {kind} to the new location")
        print(f"     [f] start fresh (leave the legacy {kind} untouched)")
        while True:
            choice = input(f"     Migrate {kind}? [m/f]: ").strip().lower()
            if choice in {"m", "migrate"}:
                return "migrate"
            if choice in {"f", "fresh"}:
                return "fresh"
            print("     Please answer 'm' or 'f'.")

    # Non-interactive and no policy → refuse to guess with user data.
    raise StorageMigrationHalt(
        f"Found legacy {kind} at {legacy_desc} but the new location "
        f"({new_desc}) is empty, and ALMa is running non-interactively with "
        f"no decision provided. Set {env_var}=migrate to copy it across, or "
        f"{env_var}=fresh to start fresh. Refusing to silently guess."
    )


# --------------------------------------------------------------------------
# Migration steps
# --------------------------------------------------------------------------
def _copy_file(src: Path, dst: Path) -> None:
    dst.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(src, dst)
    logger.info("[storage] copied %s -> %s", src, dst)


def _migrate_db(legacy_db: Path, new_db: Path) -> None:
    _copy_file(legacy_db, new_db)
    for sidecar in _DB_SIDECARS:
        s = legacy_db.with_name(sidecar)
        if s.exists():
            _copy_file(s, new_db.with_name(sidecar))


# --------------------------------------------------------------------------
# Orchestration
# --------------------------------------------------------------------------
def validate_and_migrate_storage(interactive: Optional[bool] = None) -> None:
    """Validate current storage locations and migrate legacy data on a miss.

    Safe to call once at startup, before the DB is opened. Idempotent: once
    the current locations are populated it just logs and returns.
    """
    from alma import config

    if interactive is None:
        interactive = sys.stdin is not None and sys.stdin.isatty()

    profile = config.get_env_profile()
    config_dir = config.get_config_dir(create=False)
    logger.info("[storage] profile=%s · data_dir=%s · config_dir=%s",
                profile, config.get_data_dir(create=False), config_dir)

    # Legacy ./data migration is a PROD-upgrade concern only. A non-prod
    # profile (e.g. ALMA_ENV=dev) is an isolated namespace populated by
    # seed_dev_profile() — it must NEVER adopt, migrate, or halt on the legacy
    # ./data. Otherwise a dev server started non-interactively would discover
    # the repo's ./data/scholar.db and halt on startup.
    if profile != "prod":
        new_db = config.get_db_path()
        if new_db.exists():
            logger.info("[storage] %s profile — database present at %s", profile, new_db)
        else:
            logger.info("[storage] %s profile — fresh isolated DB at %s "
                        "(populate it with seed_dev_profile)", profile, new_db)
        return

    # Snapshot config-file presence BEFORE touching get_db_path() — reading
    # the `database` setting auto-creates settings.json as a side effect,
    # which would otherwise mask a legacy-config migration.
    new_settings = config.get_settings_path()
    new_env = config.get_env_file_path()
    settings_existed = new_settings.exists()
    env_existed = new_env.exists()

    # An explicit location override (env or, for the DB, the `database`
    # setting) means the operator/test/Docker chose where data lives — there
    # is no implicit "old vs new" to reconcile, so never migrate or prompt.
    db_explicit = bool(
        os.getenv("DB_PATH") or os.getenv("DATA_DIR") or config.get_setting("database")
    )
    config_explicit = bool(os.getenv("ALMA_CONFIG_DIR") or os.getenv("ALMA_SETTINGS_PATH"))

    # --- database ---------------------------------------------------------
    new_db = config.get_db_path()
    if new_db.exists():
        logger.info("[storage] database present at %s", new_db)
    elif db_explicit:
        logger.info("[storage] database location explicitly set (%s) — skipping "
                    "legacy migration (fresh DB will be created here)", new_db)
    else:
        legacy_db = find_legacy_db()
        if legacy_db is None:
            logger.info("[storage] no database yet — fresh start at %s", new_db)
        elif legacy_db.resolve() == new_db.resolve():
            logger.info("[storage] database already at canonical path %s", new_db)
        else:
            decision = _decide("database", str(legacy_db), str(new_db),
                               "ALMA_DB_MIGRATION", interactive)
            if decision == "migrate":
                _migrate_db(legacy_db, new_db)
                logger.info("[storage] database migrated to %s", new_db)
            else:
                logger.warning("[storage] starting with a FRESH database at %s "
                               "(legacy left untouched at %s)", new_db, legacy_db)

    # --- config (settings.json + .env) -----------------------------------
    if settings_existed or env_existed:
        logger.info("[storage] config present (settings=%s, env=%s)",
                    settings_existed, env_existed)
    elif config_explicit:
        logger.info("[storage] config location explicitly set — skipping legacy "
                    "config migration")
    else:
        legacy = find_legacy_config()
        if not legacy["settings"] and not legacy["env"]:
            logger.info("[storage] no legacy config — fresh config at %s", config_dir)
        else:
            legacy_desc = ", ".join(
                str(p) for p in (legacy["settings"], legacy["env"]) if p
            )
            decision = _decide("config", legacy_desc, str(config_dir),
                               "ALMA_CONFIG_MIGRATION", interactive)
            if decision == "migrate":
                if legacy["settings"] and legacy["settings"].resolve() != new_settings.resolve():
                    _copy_file(legacy["settings"], new_settings)
                if legacy["env"] and legacy["env"].resolve() != new_env.resolve():
                    _copy_file(legacy["env"], new_env)
                logger.info("[storage] config migrated to %s", config_dir)
            else:
                logger.warning("[storage] starting with FRESH config at %s "
                               "(legacy left untouched)", config_dir)


# --------------------------------------------------------------------------
# Dev-profile seeding (ALMA_ENV=dev)
# --------------------------------------------------------------------------
def seed_dev_profile(*, force: bool = False) -> bool:
    """Seed the dev profile's data + config from the prod profile, once.

    Called by the dev runner (``ALMA_ENV=dev``). Copies the prod DB +
    settings + ``.env`` into the ``alma-dev`` locations if they're empty,
    so dev works against a realistic *copy* of prod without ever sharing a
    live SQLite writer or mutating prod config. Returns True if it copied
    anything. No-op for the prod profile.
    """
    from alma import config

    if config.get_env_profile() == "prod":
        return False

    seeded = False
    dev_db = config.get_db_path()
    dev_settings = config.get_settings_path()
    dev_env = config.get_env_file_path()

    # Resolve the prod locations by temporarily forcing the prod profile.
    prev = os.environ.get("ALMA_ENV")
    os.environ["ALMA_ENV"] = "prod"
    try:
        prod_db = config.get_db_path()
        prod_settings = config.get_settings_path()
        prod_env = config.get_env_file_path()
    finally:
        if prev is None:
            os.environ.pop("ALMA_ENV", None)
        else:
            os.environ["ALMA_ENV"] = prev

    if (force or not dev_db.exists()) and prod_db.exists() and prod_db.resolve() != dev_db.resolve():
        _migrate_db(prod_db, dev_db)
        logger.info("[storage] dev DB seeded from prod %s", prod_db)
        seeded = True
    if (force or not dev_settings.exists()) and prod_settings.exists():
        _copy_file(prod_settings, dev_settings)
        seeded = True
    if (force or not dev_env.exists()) and prod_env.exists():
        _copy_file(prod_env, dev_env)
        seeded = True

    if not seeded:
        logger.info("[storage] dev profile already populated or no prod data to seed")
    return seeded
