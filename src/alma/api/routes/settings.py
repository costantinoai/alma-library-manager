"""Settings API endpoints.

Provides read/write access to local settings without exposing external APIs.
Settings are stored in the repository's `settings.json` file.

IMPORTANT: All settings access goes through alma.config module.
This ensures all path values are relative and properly resolved.

Secret handling (2026-04-24):
- OpenAlex API key lives only in `.env` at the project root. The
  Settings GET reads it from `os.environ` (which `config.py` loads
  via `dotenv.load_dotenv` at startup), masks all but the last 4
  characters, and returns the masked form. PUT rotates the value
  via `dotenv.set_key` and preserves the previous value as
  `OPENALEX_API_KEY_OLD_<utc-timestamp>` so nothing is lost.
- `data/secrets.json` is no longer read or written by app routes.
  It remains a local-only file (gitignored) that users can clean up
  manually.
"""

import logging
import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, status
import sqlite3
from pydantic import BaseModel, Field, field_validator

from alma.config import (
    DEFAULT_SETTINGS,
    get_all_settings,
    get_db_path,
    update_settings as config_update_settings,
    delete_settings_keys as config_delete_settings_keys,
    reload_settings,
)
from alma.api.deps import get_current_user
from alma.openalex.http import get_client as get_openalex_client, reset_client as reset_openalex_client
from alma.slack.client import get_slack_notifier
from alma.plugins.config import save_plugin_config
from alma.core.redaction import redact_sensitive_text
from alma.core.secrets import (
    SECRET_SLACK_BOT_TOKEN,
    delete_secret,
    get_secret,
    mask_secret,
    set_secret,
)

# Where `.env` lives. Kept next to `settings.json` at the project
# root — the same path `alma.config._find_project_root()` resolves.
_DOTENV_PATH = Path(__file__).resolve().parent.parent.parent.parent.parent / ".env"
_OPENALEX_ENV_KEY = "OPENALEX_API_KEY"


def _rotate_openalex_env_key(new_value: str) -> None:
    """Write `new_value` as OPENALEX_API_KEY in `.env`, backing up any
    existing value under `OPENALEX_API_KEY_OLD_<utc-timestamp>`.

    Uses `python-dotenv.set_key` so formatting, quoting, and key
    ordering are handled correctly. Also updates the live process env
    so the change takes effect without a restart.
    """

    try:
        from dotenv import set_key
    except ImportError:  # pragma: no cover — required dependency
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="python-dotenv is required for secret rotation",
        )

    _DOTENV_PATH.touch(exist_ok=True)
    previous = os.environ.get(_OPENALEX_ENV_KEY, "")
    if previous and previous != new_value:
        # Microsecond precision so two rotations in the same wall-clock
        # second don't share an archive key (set_key would overwrite
        # the earlier one and silently lose the intermediate value).
        ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S_%fZ")
        archive_key = f"{_OPENALEX_ENV_KEY}_OLD_{ts}"
        set_key(str(_DOTENV_PATH), archive_key, previous)
        os.environ[archive_key] = previous
    set_key(str(_DOTENV_PATH), _OPENALEX_ENV_KEY, new_value)
    os.environ[_OPENALEX_ENV_KEY] = new_value


def _delete_openalex_env_key() -> None:
    """Remove OPENALEX_API_KEY from `.env` (after archiving any value)."""

    try:
        from dotenv import set_key, unset_key
    except ImportError:
        return

    previous = os.environ.get(_OPENALEX_ENV_KEY, "")
    if previous:
        # Microsecond precision so two rotations in the same wall-clock
        # second don't share an archive key (set_key would overwrite
        # the earlier one and silently lose the intermediate value).
        ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S_%fZ")
        archive_key = f"{_OPENALEX_ENV_KEY}_OLD_{ts}"
        _DOTENV_PATH.touch(exist_ok=True)
        set_key(str(_DOTENV_PATH), archive_key, previous)
        os.environ[archive_key] = previous
    if _DOTENV_PATH.exists():
        try:
            unset_key(str(_DOTENV_PATH), _OPENALEX_ENV_KEY)
        except Exception:
            logger.debug("unset_key failed for %s", _OPENALEX_ENV_KEY, exc_info=True)
    os.environ.pop(_OPENALEX_ENV_KEY, None)

logger = logging.getLogger(__name__)

router = APIRouter(
    prefix="/settings",
    tags=["settings"],
    dependencies=[Depends(get_current_user)],
    responses={401: {"description": "Unauthorized"}},
)


class SettingsModel(BaseModel):
    backend: str = Field("scholar", pattern="^(scholar|openalex)$", description="Publication backend")
    openalex_email: Optional[str] = Field(None, description="Optional contact email sent with OpenAlex requests")
    fetch_full_history: Optional[bool] = Field(False, description="Fetch full author history (OpenAlex)")
    from_year: Optional[int] = Field(None, description="Fetch from this year onward; ignored if fetch_full_history is true")
    api_call_delay: Optional[str] = Field("1.0", description="Legacy UI delay; kept for compatibility")
    # Paths (exposed for convenience; prefer env-vars in production)
    # IMPORTANT: All paths MUST be relative (e.g., ./data/scholar.db)
    database: Optional[str] = Field(None, description="Relative path to the unified scholar database")
    slack_config_path: Optional[str] = Field(None, description="Relative path to Slack plugin config file (INI)")
    # Slack notification settings
    slack_token: Optional[str] = Field(None, description="Slack Bot User OAuth Token")
    slack_channel: Optional[str] = Field(None, description="Default Slack channel for notifications")
    # OpenAlex API key (optional, for premium/institutional access)
    openalex_api_key: Optional[str] = Field(None, description="OpenAlex API key for premium access")
    # Identifier resolution strategy settings
    id_resolution_semantic_scholar_enabled: Optional[bool] = Field(
        True,
        description="Use Semantic Scholar API for Scholar ID resolution",
    )
    id_resolution_orcid_enabled: Optional[bool] = Field(
        True,
        description="Use ORCID public API researcher links for Scholar ID resolution",
    )
    id_resolution_scholar_scrape_auto_enabled: Optional[bool] = Field(
        False,
        description="Allow automatic Google Scholar scraping fallback in pipelines",
    )
    id_resolution_scholar_scrape_manual_enabled: Optional[bool] = Field(
        True,
        description="Allow manual Google Scholar scraping from the Authors UI",
    )

    @field_validator('database', 'slack_config_path')
    @classmethod
    def validate_relative_path(cls, v: Optional[str]) -> Optional[str]:
        """Ensure paths are relative, not absolute."""
        if v is not None and Path(v).is_absolute():
            raise ValueError(f"Path must be relative, got absolute path: {v}")
        return v


def _read_settings() -> dict:
    """Read settings using centralized config module."""
    settings = get_all_settings()
    for key, value in DEFAULT_SETTINGS.items():
        if key not in settings:
            settings[key] = value
    return settings


def _write_settings(data: dict) -> None:
    """Write settings using centralized config module."""
    # Filter out None values to avoid overwriting with empty data
    updates = {k: v for k, v in data.items() if v is not None}
    config_update_settings(updates)
    logger.info("Settings updated via centralized config: %s", list(updates.keys()))


def _is_masked_openalex_key(value: object) -> bool:
    return isinstance(value, str) and value.startswith("****")


def _is_masked_slack_token(value: object) -> bool:
    if not isinstance(value, str):
        return False
    token = value.strip()
    # Current GET response mask format is "<prefix>...<suffix>".
    return "..." in token


def _export_settings_sanitized(raw: dict) -> dict:
    """Return settings safe for export payloads."""
    out = dict(raw or {})
    out["slack_token"] = mask_secret(get_secret(SECRET_SLACK_BOT_TOKEN), prefix=10, suffix=4)
    out["openalex_api_key"] = mask_secret(
        os.environ.get(_OPENALEX_ENV_KEY) or None, suffix=4
    )
    return out


def _sync_slack_plugin_config_from_settings(settings: dict) -> None:
    """Mirror Settings Slack fields into plugin config for consistency."""
    token = str(get_secret(SECRET_SLACK_BOT_TOKEN) or "").strip()
    channel = str(settings.get("slack_channel") or "").strip()
    cfg: dict[str, str] = {}
    if token:
        cfg["api_token"] = token
    if channel:
        cfg["default_channel"] = channel
    save_plugin_config("slack", cfg)


@router.get("", response_model=SettingsModel)
def get_settings():
    """Retrieve core settings.

    Note: slack_token is masked for security -- only the first 10 characters
    are returned so the UI can show whether a token is configured.
    """
    raw = _read_settings()
    slack_token_masked = mask_secret(get_secret(SECRET_SLACK_BOT_TOKEN), prefix=10, suffix=4)
    openalex_api_key_masked = mask_secret(
        os.environ.get(_OPENALEX_ENV_KEY) or None, suffix=4
    )

    return SettingsModel(
        backend=raw.get("backend", "scholar"),
        openalex_email=raw.get("openalex_email"),
        fetch_full_history=bool(raw.get("fetch_full_history", False)),
        from_year=raw.get("from_year"),
        api_call_delay=str(raw.get("api_call_delay", "1.0")),
        database=raw.get("database"),
        slack_config_path=raw.get("slack_config_path"),
        slack_token=slack_token_masked,
        slack_channel=raw.get("slack_channel"),
        openalex_api_key=openalex_api_key_masked,
        id_resolution_semantic_scholar_enabled=bool(
            raw.get("id_resolution_semantic_scholar_enabled", True)
        ),
        id_resolution_orcid_enabled=bool(raw.get("id_resolution_orcid_enabled", True)),
        id_resolution_scholar_scrape_auto_enabled=bool(
            raw.get("id_resolution_scholar_scrape_auto_enabled", False)
        ),
        id_resolution_scholar_scrape_manual_enabled=bool(
            raw.get("id_resolution_scholar_scrape_manual_enabled", True)
        ),
    )


@router.get("/export")
def export_data_snapshot():
    """Export a full JSON snapshot for backup/migration use.

    Includes sanitized settings and core DB tables.
    """
    settings = _export_settings_sanitized(_read_settings())

    data_tables = [
        "authors",
        "papers",
        "collections",
        "collection_items",
        "tags",
        "publication_tags",
        "followed_authors",
        "alerts",
        "alert_rules",
        "alert_rule_assignments",
        "alert_history",
        "recommendations",
        "publication_topics",
        "publication_institutions",
        "operation_status",
        "operation_logs",
    ]

    dump: dict[str, list[dict]] = {}
    try:
        conn = sqlite3.connect(str(get_db_path()), check_same_thread=False)
        conn.row_factory = sqlite3.Row
        try:
            for table in data_tables:
                try:
                    rows = conn.execute(f"SELECT * FROM {table}").fetchall()
                    dump[table] = [dict(r) for r in rows]
                except sqlite3.OperationalError:
                    # Table may be absent in older DBs.
                    dump[table] = []
        finally:
            conn.close()
    except Exception as exc:
        logger.error("Export snapshot failed while reading DB: %s", exc)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to export snapshot",
        )

    return {
        "exported_at": datetime.utcnow().isoformat() + "Z",
        "schema_version": 1,
        "settings": settings,
        "data": dump,
    }


@router.put("", response_model=SettingsModel)
def update_settings(payload: SettingsModel):
    """Update core settings locally.

    Notes:
    - `backend` must be either `scholar` or `openalex`
    - `openalex_email` is optional and sent as a contact parameter for OpenAlex requests
    - All paths MUST be relative (e.g., ./data/scholar.db)
    - No external APIs are called as part of this endpoint
    """
    try:
        data = payload.model_dump()

        # Secrets are persisted in the secret store, not settings.json.
        incoming_openalex_key = data.pop("openalex_api_key", None)
        incoming_slack_token = data.pop("slack_token", None)

        if incoming_openalex_key is not None and not _is_masked_openalex_key(incoming_openalex_key):
            clean_openalex_key = str(incoming_openalex_key).strip()
            if clean_openalex_key:
                _rotate_openalex_env_key(clean_openalex_key)
            else:
                _delete_openalex_env_key()
            # The shared OpenAlex client is reset below (single call
            # for either rotation or delete + any email change).

        if incoming_slack_token is not None and not _is_masked_slack_token(incoming_slack_token):
            clean_slack_token = str(incoming_slack_token).strip()
            if clean_slack_token:
                set_secret(SECRET_SLACK_BOT_TOKEN, clean_slack_token)
            else:
                delete_secret(SECRET_SLACK_BOT_TOKEN)

        # Write settings first (this validates that paths are relative)
        _write_settings(data)
        # Ensure plaintext credential keys are absent from settings.json.
        config_delete_settings_keys(["slack_token", "openalex_api_key"])

        # Validate DB path can be accessed using the resolved absolute path
        from alma.config import get_db_path

        def _ensure_db(resolved_path: Path):
            try:
                resolved_path.parent.mkdir(parents=True, exist_ok=True)
                conn = sqlite3.connect(str(resolved_path))
                try:
                    conn.execute("SELECT 1").fetchone()
                finally:
                    conn.close()
            except Exception as e:
                logger.warning("Invalid database path: %s", redact_sensitive_text(str(e)))
                raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Invalid database path")

        # Reload settings to get updated paths
        reload_settings()

        # Ensure shared OpenAlex client picks up changed credentials/settings.
        if incoming_openalex_key is not None or "openalex_email" in data:
            reset_openalex_client()

        if incoming_slack_token is not None or "slack_channel" in data:
            try:
                _sync_slack_plugin_config_from_settings(_read_settings())
            except Exception as exc:
                logger.warning("Failed to sync Slack plugin config from settings: %s", exc)

        if data.get("database"):
            _ensure_db(get_db_path())

        # Return masked values from canonical GET representation.
        return get_settings()
    except HTTPException:
        # Preserve intended status codes from validation helpers
        raise
    except ValueError as e:
        # Path validation errors from pydantic
        logger.warning("Invalid settings payload: %s", redact_sensitive_text(str(e)))
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Invalid settings payload")
    except Exception as e:
        logger.error(f"Failed to update settings: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to update settings",
        )


@router.get("/test/openalex")
def test_openalex_connectivity():
    """Test connectivity to the OpenAlex API via shared client."""
    try:
        client = get_openalex_client()
        resp = client.get("/authors", params={"search": "test", "per-page": 1}, timeout=10)
        ok = resp.status_code == 200
        return {
            "success": ok,
            "status": resp.status_code,
            "credits": client.credits_summary(),
        }
    except Exception as e:
        logger.error("OpenAlex connectivity test failed: %s", e)
        return {"success": False, "error": "OpenAlex connectivity test failed"}


@router.get("/openalex/usage")
def get_openalex_usage():
    """Return current OpenAlex credit/rate usage from the shared client."""
    try:
        client = get_openalex_client()
        rate = client.get_rate_limit_status()

        def _to_int(v):
            try:
                if v is None:
                    return None
                return int(v)
            except Exception:
                return None

        # OpenAlex /rate-limit response nests data under "rate_limit" key:
        # {"api_key": "...", "rate_limit": {"credits_limit": N, "credits_used": N, ...}}
        # When no API key is configured, rate is None and we fall back to the
        # values the client captured from X-RateLimit-* response headers on
        # every prior API call.
        rl = (rate or {}).get("rate_limit") or rate or {}
        remaining = _to_int(rl.get("credits_remaining"))
        used = _to_int(rl.get("credits_used"))
        limit = _to_int(rl.get("credits_limit"))
        resets_in_seconds = _to_int(rl.get("resets_in_seconds"))
        reset_at = rl.get("resets_at")

        # Fall back to header-captured values so the usage card is truthful
        # when /rate-limit is unavailable (no API key) but the client has made
        # at least one request and recorded X-RateLimit-* headers. Each field
        # falls back independently so partial /rate-limit responses don't
        # shadow good header data.
        credits_remaining = remaining if remaining is not None else client.rate_remaining
        credits_used = used if used is not None else client.credits_used
        credits_limit = limit if limit is not None else client.rate_limit
        if reset_at is None and client.rate_reset:
            reset_at = client.rate_reset

        # Distinguish the three data-availability cases so the frontend can
        # render a truthful label — based on whether we actually have credit
        # data to show, not on request_count (which get_rate_limit_status
        # itself bumps, so cold-start is distinguishable only by looking at
        # the captured credit fields):
        #  - `openalex_rate_limit`: authoritative /rate-limit response parsed
        #  - `local_headers`: values captured from X-RateLimit-* headers on
        #    prior API calls
        #  - `no_calls_yet`: no credit data of any kind available. Never
        #    return a literal "unknown".
        has_credit_data = any(
            v is not None for v in (credits_used, credits_remaining, credits_limit)
        )
        if rate:
            source = "openalex_rate_limit"
        elif has_credit_data:
            source = "local_headers"
        else:
            source = "no_calls_yet"

        summary = (
            f"{credits_used if credits_used is not None else '?'} credits used, "
            f"{credits_remaining if credits_remaining is not None else '?'} remaining "
            f"(limit {credits_limit if credits_limit is not None else '?'}, "
            f"{client.request_count} requests)"
        )

        return {
            "source": source,
            "request_count": client.request_count,
            "retry_count": client.retry_count,
            "rate_limited_events": client.rate_limited_events,
            "calls_saved_by_cache": client.calls_saved_by_cache,
            "credits_used": credits_used,
            "credits_remaining": credits_remaining,
            "credits_limit": credits_limit,
            "resets_in_seconds": resets_in_seconds,
            "reset_at": reset_at,
            "summary": summary,
        }
    except Exception as e:
        logger.error("Failed to read OpenAlex usage: %s", e)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to read OpenAlex usage",
        )


@router.get("/test/scholar")
def test_scholar_connectivity():
    """Best-effort test for scholarly availability (no real scrape)."""
    try:
        import scholarly  # noqa: F401
        # We avoid real requests to Scholar here to be respectful.
        return {"success": True, "message": "scholarly library available"}
    except Exception as e:
        logger.error("Scholar connectivity test failed: %s", e)
        return {"success": False, "error": "Scholar connectivity test failed"}


@router.post("/test/slack")
async def test_slack_connectivity():
    """Send a test message to the configured Slack channel.

    Uses Slack credentials from the unified secret store plus
    ``SLACK_TOKEN``/``SLACK_CHANNEL`` environment overrides.

    Returns a JSON object with ``success`` (bool), ``message`` (str),
    and optionally ``error`` (str) on failure.
    """
    try:
        notifier = get_slack_notifier()

        if not notifier.is_configured:
            return {
                "success": False,
                "error": (
                    "Slack is not configured. Set slack_token in Settings "
                    "or SLACK_TOKEN environment variable."
                ),
            }

        result = await notifier.send_test_message()

        if result:
            return {
                "success": True,
                "message": "Test message sent successfully to Slack",
                "channel": notifier.resolve_channel(),
            }
        else:
            return {
                "success": False,
                "error": "Failed to send test message. Check token and channel.",
            }
    except ValueError as e:
        logger.error("Slack test validation failed: %s", e)
        return {"success": False, "error": "Slack connectivity test failed"}
    except Exception as e:
        logger.error("Slack test failed: %s", e)
        return {"success": False, "error": "Slack connectivity test failed"}
