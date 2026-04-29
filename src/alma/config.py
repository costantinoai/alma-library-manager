"""Centralized configuration management for Scholar Bot.

This module provides the single source of truth for all configuration paths
and settings. All other modules MUST import from here rather than defining
their own hardcoded paths.

Configuration priority (highest to lowest):
1. Environment variables (e.g., DB_PATH)
2. settings.json file (with relative paths)
3. Defaults (./data directory)

Key principles:
- All paths in settings.json MUST be relative to the project root
- No hardcoded absolute paths anywhere in the codebase
- This module resolves relative paths to absolute paths at runtime
- When settings are updated via UI, only settings.json is modified
- A single unified database (scholar.db) replaces the old split
  authors.db + publications.db layout.
"""

import os
import json
import logging
from pathlib import Path
from typing import Optional, Dict, Any
from functools import lru_cache

logger = logging.getLogger(__name__)

# Load `.env` eagerly so every `os.getenv(...)` downstream sees the
# values the user saved through the Settings UI. `.env` lives at the
# project root (gitignored); it's the ONLY source of truth for
# external-API secrets. The Settings UI reads the key from env,
# displays it redacted, and on rotation writes back via
# `python-dotenv.set_key` after backing up the previous value under
# `<KEY>_OLD_<timestamp>`.
try:
    from dotenv import load_dotenv as _load_dotenv
    # Walk up looking for `.env`, then load. `override=False` keeps any
    # pre-existing shell env vars — useful for CI where a test suite
    # wants to inject a key without touching `.env`. Failures (file
    # missing, mode 600 owned by a different uid in a Docker bind mount,
    # encoding glitch) are demoted to a warning: env vars passed into
    # the process from the runtime (docker `--env-file`, compose
    # `env_file:`, the user's shell) still apply.
    _env_path = Path(__file__).resolve().parent.parent.parent / ".env"
    if _env_path.exists():
        try:
            _load_dotenv(_env_path, override=False)
        except (PermissionError, OSError, UnicodeDecodeError) as exc:
            logger.warning(
                "Could not load .env at %s (%s) — using environment vars only",
                _env_path, exc,
            )
except ImportError:
    logger.debug("python-dotenv not installed — skipping .env load")

# Project root is discovered from settings.json when present, otherwise
# from other repo-root markers so a missing settings file does not break
# bootstrap.
_PROJECT_ROOT: Optional[Path] = None
_PROJECT_ROOT_MARKERS = ("settings.json", "pyproject.toml", "docker-compose.yml", ".git")

DEFAULT_SETTINGS: Dict[str, Any] = {
    "database": "./data/scholar.db",
    "slack_config_path": "./config/slack.config",
    "api_call_delay": "1.0",
    # OpenAlex is the primary source: open citation graph, polite-pool
    # rate limits, no scraping, no auth wall. The legacy "scholar"
    # backend (Google Scholar via the `scholarly` package) is kept as
    # an option for users who explicitly opt in, but it must never be
    # the first-run default — it ships disabled in the lite image and
    # is the wrong shape for a public testing release.
    "backend": "openalex",
    "openalex_email": None,
    "fetch_full_history": False,
    "from_year": None,
    "slack_channel": None,
    "id_resolution_semantic_scholar_enabled": True,
    "id_resolution_orcid_enabled": True,
    "id_resolution_scholar_scrape_auto_enabled": False,
    "id_resolution_scholar_scrape_manual_enabled": True,
}


def _looks_like_project_root(candidate: Path) -> bool:
    return any((candidate / marker).exists() for marker in _PROJECT_ROOT_MARKERS)


def _write_default_settings_file(settings_path: Path) -> bool:
    """Write defaults to ``settings_path``. Returns False if the file
    isn't writable from this process (typical for a Docker bind-mount
    of a host file owned by a different uid) — the caller falls back
    to in-memory defaults so the app boots regardless.
    """
    try:
        settings_path.parent.mkdir(parents=True, exist_ok=True)
        with open(settings_path, "w", encoding="utf-8") as f:
            json.dump(DEFAULT_SETTINGS, f, indent=2)
            f.write("\n")
        return True
    except (PermissionError, OSError) as exc:
        logger.warning(
            "Could not write default settings to %s (%s) — using "
            "in-memory defaults; Settings UI changes will not persist "
            "until the file is writable",
            settings_path, exc,
        )
        return False


def _find_project_root() -> Path:
    """Find the project root by looking for repo-root markers.

    Searches from current working directory upwards until a known
    project marker is found. Falls back to the module's source-tree root,
    then finally to the current working directory.
    """
    global _PROJECT_ROOT
    if _PROJECT_ROOT is not None:
        return _PROJECT_ROOT

    current = Path.cwd()
    for parent in [current] + list(current.parents):
        if _looks_like_project_root(parent):
            _PROJECT_ROOT = parent
            logger.debug(f"Found project root at: {_PROJECT_ROOT}")
            return _PROJECT_ROOT

    module_candidate = Path(__file__).resolve().parents[2]
    if _looks_like_project_root(module_candidate):
        _PROJECT_ROOT = module_candidate
        logger.debug(f"Fell back to module project root at: {_PROJECT_ROOT}")
        return _PROJECT_ROOT

    _PROJECT_ROOT = current
    logger.warning(f"Project root markers not found, using current directory as root: {_PROJECT_ROOT}")
    return _PROJECT_ROOT


def get_project_root() -> Path:
    """Get the project root directory.

    Returns:
        Path: Absolute path to the project root
    """
    return _find_project_root()


def get_settings_path() -> Path:
    """Get the path to settings.json.

    Priority:
      1. ``ALMA_SETTINGS_PATH`` env var. Useful in containers where the
         project root is read-only (no bind-mount of settings.json) but
         the data volume is writable — point this at e.g.
         ``/app/data/settings.json``.
      2. ``settings.json`` next to the project root.

    Returns:
        Path: Absolute path to settings.json
    """
    env_path = os.getenv("ALMA_SETTINGS_PATH")
    if env_path:
        # Absolute paths are the documented form here (the env var
        # exists precisely to point at writable locations outside the
        # project root, e.g. /app/data/settings.json in containers),
        # so don't route them through _resolve_path — that helper's
        # "paths should be relative" warning is meant for settings
        # values like ``database``, not for this escape hatch.
        candidate = Path(env_path)
        if candidate.is_absolute():
            candidate.parent.mkdir(parents=True, exist_ok=True)
            return candidate
        return _resolve_path(env_path)
    return get_project_root() / "settings.json"


def ensure_settings_file() -> Path:
    """Create a default settings.json when it is missing or blank.

    Returns:
        Path: Absolute path to settings.json
    """
    settings_path = get_settings_path()
    if not settings_path.exists():
        _write_default_settings_file(settings_path)
        logger.info("Created default settings file at %s", settings_path)
        return settings_path

    try:
        raw = settings_path.read_text(encoding="utf-8")
    except Exception as exc:
        logger.warning("Failed reading settings file %s: %s", settings_path, exc)
        return settings_path

    if not raw.strip():
        _write_default_settings_file(settings_path)
        logger.info("Replaced blank settings file at %s with defaults", settings_path)

    return settings_path


@lru_cache(maxsize=1)
def _load_settings() -> Dict[str, Any]:
    """Load settings from settings.json with caching.

    Returns:
        Dict containing settings, or empty dict if file doesn't exist
    """
    settings_path = ensure_settings_file()

    try:
        with open(settings_path, 'r', encoding="utf-8") as f:
            settings = json.load(f)
        logger.debug(f"Loaded settings from {settings_path}")
        return settings
    except Exception as e:
        logger.error(f"Failed to load settings from {settings_path}: {e}")
        return {}


def reload_settings():
    """Clear the settings cache to force reload on next access.

    Call this after modifying settings.json via the API.
    """
    _load_settings.cache_clear()
    logger.debug("Settings cache cleared")


def get_setting(key: str, default: Any = None) -> Any:
    """Get a setting value by key.

    Args:
        key: Setting key to retrieve
        default: Default value if key doesn't exist

    Returns:
        Setting value or default
    """
    settings = _load_settings()
    return settings.get(key, default)


def _resolve_path(relative_path: str, ensure_parent: bool = True) -> Path:
    """Resolve a relative path to an absolute path.

    Args:
        relative_path: Relative path (e.g., './data/authors.db')
        ensure_parent: If True, ensure parent directory exists

    Returns:
        Path: Absolute path
    """
    if not relative_path:
        raise ValueError("Path cannot be empty")

    path = Path(relative_path)

    # If already absolute, return as-is (but log a warning)
    if path.is_absolute():
        logger.warning(f"Path is absolute, should be relative: {relative_path}")
        return path

    # Resolve relative to project root
    absolute_path = get_project_root() / path

    # Ensure parent directory exists
    if ensure_parent:
        absolute_path.parent.mkdir(parents=True, exist_ok=True)

    return absolute_path


def get_data_dir() -> Path:
    """Get the data directory where databases and caches are stored.

    Priority:
    1. DATA_DIR environment variable
    2. Default: ./data

    Returns:
        Path: Absolute path to data directory
    """
    env_dir = os.getenv("DATA_DIR")
    if env_dir:
        return _resolve_path(env_dir)

    return _resolve_path("./data")


def get_db_path() -> Path:
    """Get the path to the unified scholar database.

    Priority:
    1. DB_PATH environment variable
    2. ``database`` setting from settings.json
    3. Default: {data_dir}/scholar.db

    Returns:
        Path: Absolute path to scholar.db
    """
    # Environment variable override
    env_path = os.getenv("DB_PATH")
    if env_path:
        return _resolve_path(env_path)

    # New unified key
    db_setting = get_setting("database")
    if db_setting:
        return _resolve_path(db_setting)

    # Default
    return get_data_dir() / "scholar.db"


def get_slack_config_path() -> Path:
    """Get the path to the Slack configuration file.

    Priority:
    1. SLACK_CONFIG_PATH environment variable
    2. slack_config_path setting from settings.json
    3. Default: ./config/slack.config

    Returns:
        Path: Absolute path to slack.config
    """
    # Environment variable override
    env_path = os.getenv("SLACK_CONFIG_PATH")
    if env_path:
        return _resolve_path(env_path)

    # Settings.json value
    settings_path = get_setting("slack_config_path")
    if settings_path:
        return _resolve_path(settings_path)

    # Default
    return _resolve_path("./config/slack.config")


def update_settings(updates: Dict[str, Any]) -> None:
    """Update settings.json with new values.

    This is the ONLY function that should modify settings.json.
    All paths in updates MUST be relative paths.

    Args:
        updates: Dictionary of settings to update

    Raises:
        ValueError: If any path value is absolute
    """
    # Validate that all path values are relative
    path_keys = ["database", "slack_config_path"]
    for key in path_keys:
        if key in updates:
            value = updates[key]
            if value and Path(value).is_absolute():
                raise ValueError(f"Path for {key} must be relative, got: {value}")

    # Load current settings
    settings_path = ensure_settings_file()
    try:
        with open(settings_path, 'r', encoding="utf-8") as f:
            settings = json.load(f)
    except Exception:
        settings = {}

    # Update with new values
    settings.update(updates)

    # Write back to file
    with open(settings_path, 'w', encoding="utf-8") as f:
        json.dump(settings, f, indent=2)
        f.write("\n")

    # Clear cache to force reload
    reload_settings()
    logger.info(f"Updated settings: {list(updates.keys())}")


def delete_settings_keys(keys: list[str]) -> None:
    """Delete keys from settings.json if they exist."""
    if not keys:
        return
    settings_path = ensure_settings_file()
    try:
        with open(settings_path, "r", encoding="utf-8") as f:
            settings = json.load(f)
    except Exception:
        settings = {}
    removed: list[str] = []
    for key in keys:
        if key in settings:
            settings.pop(key, None)
            removed.append(key)
    if removed:
        with open(settings_path, "w", encoding="utf-8") as f:
            json.dump(settings, f, indent=2)
            f.write("\n")
        reload_settings()
        logger.info("Deleted settings keys: %s", removed)


def get_backend() -> str:
    """Get the configured backend (scholar or openalex).

    Returns:
        str: Backend name, defaults to 'openalex'
    """
    return get_setting("backend", "openalex").lower()


def get_from_year() -> Optional[int]:
    """Get the configured from_year for publication filtering.

    Returns:
        int or None: Year to filter from, or None for full history
    """
    year = get_setting("from_year")
    if year is None:
        return None
    try:
        return int(year)
    except (ValueError, TypeError):
        logger.warning(f"Invalid from_year value: {year}, returning None")
        return None


def get_fetch_full_history() -> bool:
    """Get whether to fetch full publication history.

    Returns:
        bool: True if full history should be fetched
    """
    return bool(get_setting("fetch_full_history", False))


def get_fetch_year() -> Optional[int]:
    """Compute the effective from_year for publication fetching.

    Combines backend, fetch_full_history, and from_year settings into a single
    resolved value. This replaces the ~7 duplicated blocks across the codebase:

        cfg = _fb_settings()
        backend = (cfg.get("backend") or "scholar").lower()
        if backend == "openalex" and cfg.get("fetch_full_history", False):
            from_year = None
        else:
            from_year = cfg.get("from_year") or datetime.now().year

    Returns:
        int: Year to fetch from (inclusive), or None for full history.
    """
    from datetime import datetime

    backend = get_backend()
    if backend == "openalex" and get_fetch_full_history():
        return None
    year = get_from_year()
    return year if year is not None else datetime.now().year


def get_api_call_delay() -> float:
    """Get the API call delay in seconds.

    Returns:
        float: Delay between API calls
    """
    delay = get_setting("api_call_delay", "1.0")
    try:
        return float(delay)
    except (ValueError, TypeError):
        return 1.0


def get_openalex_email() -> Optional[str]:
    """Get the OpenAlex API email for polite pool access.

    Priority:
    1. OPENALEX_EMAIL environment variable
    2. openalex_email setting from settings.json

    Returns:
        str or None: Email address or None
    """
    env_email = os.getenv("OPENALEX_EMAIL")
    if env_email:
        return env_email

    return get_setting("openalex_email")


def get_openalex_api_key() -> Optional[str]:
    """Get the OpenAlex API key.

    Resolution order:
      1. `OPENALEX_API_KEY` env var (populated from `.env` at startup
         via `dotenv.load_dotenv`, or injected by `docker --env-file` /
         compose `env_file:`). Used by tests and any host install with
         a writable `.env`.
      2. `data/secrets.json` via the namespaced secret store. This is
         the canonical persistence path for Docker named-volume
         installs where `/app/.env` lives in the read-only image layer
         and can't be rotated through the Settings UI.

    The Settings UI rotation flow writes to BOTH (best-effort `.env`,
    always secret store), so a rotation made via the UI takes effect
    immediately and survives restart regardless of deployment shape.
    """

    raw = os.getenv("OPENALEX_API_KEY")
    if raw and raw.strip():
        return raw.strip()

    try:
        from alma.core.secrets import SECRET_OPENALEX_API_KEY, get_secret

        stored = get_secret(SECRET_OPENALEX_API_KEY)
        return stored if stored else None
    except Exception:
        return None


def get_contact_email() -> Optional[str]:
    """Get the best contact email for third-party API identification."""
    for env_key in ("ALMA_CONTACT_EMAIL", "CONTACT_EMAIL", "CROSSREF_MAILTO", "OPENALEX_EMAIL"):
        raw = os.getenv(env_key)
        if raw and raw.strip():
            return raw.strip()

    for setting_key in ("contact_email", "crossref_mailto", "openalex_email"):
        raw = get_setting(setting_key)
        if raw and str(raw).strip():
            return str(raw).strip()

    return None


def get_app_user_agent() -> str:
    """Return the default ALMa user agent used for upstream API calls."""
    explicit = os.getenv("ALMA_USER_AGENT") or get_setting("user_agent")
    if explicit and str(explicit).strip():
        return str(explicit).strip()

    email = get_contact_email()
    if email:
        return f"ALMa/3.0 ({email})"
    return "ALMa/3.0"


def get_crossref_mailto() -> Optional[str]:
    """Get the Crossref mailto contact parameter."""
    raw = os.getenv("CROSSREF_MAILTO")
    if raw and raw.strip():
        return raw.strip()
    value = get_setting("crossref_mailto")
    if value and str(value).strip():
        return str(value).strip()
    return get_contact_email()


def get_semantic_scholar_api_key() -> Optional[str]:
    """Get the Semantic Scholar API key, if configured."""
    env_key = os.getenv("SEMANTIC_SCHOLAR_API_KEY")
    if env_key:
        return env_key
    try:
        from alma.core.secrets import get_secret, SECRET_SEMANTIC_SCHOLAR_API_KEY

        return get_secret(SECRET_SEMANTIC_SCHOLAR_API_KEY)
    except Exception:
        return None


def get_slack_token() -> Optional[str]:
    """Get the Slack Bot User OAuth Token.

    Priority:
    1. SLACK_TOKEN environment variable
    2. unified secret store

    Returns:
        str or None: Slack bot token or None if not configured
    """
    token = os.getenv("SLACK_TOKEN") or os.getenv("SLACK_API_TOKEN")
    if token:
        return token
    try:
        from alma.core.secrets import get_secret, SECRET_SLACK_BOT_TOKEN
        return get_secret(SECRET_SLACK_BOT_TOKEN)
    except Exception:
        return None


def get_slack_channel() -> Optional[str]:
    """Get the default Slack channel for notifications.

    Priority:
    1. SLACK_CHANNEL environment variable
    2. slack_channel setting from settings.json

    Returns:
        str or None: Slack channel name or None if not configured
    """
    channel = os.getenv("SLACK_CHANNEL") or os.getenv("SLACK_DEFAULT_CHANNEL")
    if channel:
        return channel

    channel = get_setting("slack_channel")
    if channel:
        return channel

    return None


def get_openai_api_key() -> Optional[str]:
    """Get the OpenAI API key.

    Priority:
    1. OPENAI_API_KEY environment variable
    2. unified secret store
    """
    env_key = os.getenv("OPENAI_API_KEY")
    if env_key:
        return env_key
    try:
        from alma.core.secrets import get_secret, SECRET_OPENAI_API_KEY
        return get_secret(SECRET_OPENAI_API_KEY)
    except Exception:
        return None


# Expose a dict-like interface for backward compatibility
def get_all_settings() -> Dict[str, Any]:
    """Get all settings as a dictionary.

    Returns:
        Dict containing all settings
    """
    return _load_settings().copy()
