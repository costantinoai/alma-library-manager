"""Settings API endpoints.

Provides read/write access to local settings without exposing external APIs.
Settings are stored in the repository's `settings.json` file.

IMPORTANT: All settings access goes through alma.config module.
This ensures all path values are relative and properly resolved.

Secret handling (2026-04-29; generalized to S2 2026-05-25):
- Third-party API keys (OpenAlex, Semantic Scholar) share ONE resolution
  chain — env-first (`.env` / Docker `env_file`) → `data/secrets.json`
  secret store — defined in `alma.config`. The `.env` is the single
  shared config source across bare-metal dev, bare-metal prod, and
  Docker; the secret store is the always-writable canonical mirror the
  Settings UI rotates into (needed because Docker's `/app/.env` is a
  read-only image layer, so a `.env`-only rotation raised a
  `PermissionError` and 500'd the entire PUT /settings).
- Rotation goes through the key-agnostic `_rotate_env_secret` /
  `_delete_env_secret` helpers: write secret store (canonical), update
  the in-process env so the change takes effect immediately (every
  resolver is env-first), then best-effort write `.env` (preserving the
  previous value under `<ENV_KEY>_OLD_<utc-timestamp>`). `.env` write
  failures are logged at INFO and do not propagate.
- The Settings GET reads each key via its `config` resolver (env first,
  secret store fallback) and masks all but the last 4 characters.
"""

import logging
import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, status
import requests
import sqlite3
from pydantic import BaseModel, Field, field_validator

from alma.config import (
    DEFAULT_SETTINGS,
    get_all_settings,
    get_db_path,
    get_openalex_api_key,
    get_semantic_scholar_api_key,
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
    SECRET_OPENALEX_API_KEY,
    SECRET_SEMANTIC_SCHOLAR_API_KEY,
    SECRET_SLACK_BOT_TOKEN,
    SECRET_SMTP_PASSWORD,
    delete_secret,
    get_secret,
    mask_secret,
    set_secret,
)

_OPENALEX_ENV_KEY = "OPENALEX_API_KEY"
_SEMANTIC_SCHOLAR_ENV_KEY = "SEMANTIC_SCHOLAR_API_KEY"


def _dotenv_path():
    """Canonical ``.env`` path — the OS-standard config-dir ``.env`` (see
    ``alma.config.get_env_file_path``). Centralised so the Settings-UI key
    rotation writes to the same file ``config`` loads at startup, in Docker
    (``ALMA_CONFIG_DIR=/app`` → ``/app/.env``) and bare-metal alike."""
    from alma.config import get_env_file_path

    return get_env_file_path()


logger = logging.getLogger(__name__)


def _archive_timestamp() -> str:
    """Microsecond-precision UTC timestamp for OPENALEX_API_KEY_OLD_<ts>
    archive keys — two rotations in the same wall-clock second otherwise
    share a key and silently lose the intermediate value.
    """
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S_%fZ")


def _try_write_dotenv_rotation(env_key: str, new_value: str, previous: str) -> None:
    """Best-effort `.env` write for any third-party credential. Mirrors the
    secret-store rotation onto the project-root `.env` so users with a
    writable host `.env` (Path 2 bind-mount or a native install) see the new
    value reflected there.

    Logs and swallows write failures: in Docker named-volume installs
    `/app/.env` lives in the read-only image layer and `set_key` raises
    OSError/PermissionError. The secret-store write has already happened
    by the time this is called, so a failure here is non-fatal — the new
    key is still persisted in `data/secrets.json` and live in the
    process env.
    """
    try:
        from dotenv import set_key
    except ImportError:  # pragma: no cover — required dependency
        return

    dotenv_path = _dotenv_path()
    try:
        dotenv_path.parent.mkdir(parents=True, exist_ok=True)
        dotenv_path.touch(exist_ok=True)
    except (PermissionError, OSError) as exc:
        logger.info(
            "Skipping `.env` rotation for `%s` (`%s` not writable: %s) — "
            "value is still persisted in the secret store",
            env_key, dotenv_path, exc,
        )
        return

    try:
        if previous and previous != new_value:
            archive_key = f"{env_key}_OLD_{_archive_timestamp()}"
            set_key(str(dotenv_path), archive_key, previous)
            os.environ[archive_key] = previous
        set_key(str(dotenv_path), env_key, new_value)
    except (PermissionError, OSError) as exc:
        logger.info("`.env` write skipped for `%s` rotation: %s", env_key, exc)


def _try_delete_dotenv(env_key: str) -> None:
    """Best-effort `.env` removal of any third-party credential. Same
    rationale as `_try_write_dotenv_rotation` — the secret-store delete has
    already happened, so swallow filesystem errors.
    """
    try:
        from dotenv import set_key, unset_key
    except ImportError:
        return

    dotenv_path = _dotenv_path()
    if not dotenv_path.exists():
        return

    previous = os.environ.get(env_key, "")
    try:
        if previous:
            archive_key = f"{env_key}_OLD_{_archive_timestamp()}"
            set_key(str(dotenv_path), archive_key, previous)
            os.environ[archive_key] = previous
        unset_key(str(dotenv_path), env_key)
    except (PermissionError, OSError) as exc:
        logger.info("`.env` delete skipped for `%s` rotation: %s", env_key, exc)


def _rotate_env_secret(env_key: str, secret_key: str, new_value: str) -> None:
    """Persist a new third-party API credential to ALMa's single resolution
    chain (env-first → secret store), so it takes effect everywhere the
    `config` resolvers read it — bare-metal dev, bare-metal prod, and Docker.

    Writes to two coordinated locations:
      1. The secret store (`data/secrets.json`) — the always-writable
         canonical persistence path. Survives container restart and works
         under Docker named volumes where `/app/.env` is in the read-only
         image layer.
      2. The project-root `.env` — best-effort, archives the previous value
         under `<ENV_KEY>_OLD_<timestamp>`. Failures are logged at INFO and
         swallowed (expected under Docker's read-only `.env`).

    Always updates the in-process env so the change takes effect without a
    restart (every resolver is env-first).
    """
    previous = os.environ.get(env_key, "")

    # Canonical persistent write — must succeed, propagates errors.
    set_secret(secret_key, new_value)

    os.environ[env_key] = new_value
    _try_write_dotenv_rotation(env_key, new_value, previous)


def _delete_env_secret(env_key: str, secret_key: str) -> None:
    """Remove a third-party API credential from the secret store, in-process
    env, and (best-effort) `.env`."""
    delete_secret(secret_key)
    _try_delete_dotenv(env_key)
    os.environ.pop(env_key, None)


router = APIRouter(
    prefix="/settings",
    tags=["settings"],
    dependencies=[Depends(get_current_user)],
    responses={401: {"description": "Unauthorized"}},
)


class SettingsModel(BaseModel):
    backend: str = Field("openalex", pattern="^(scholar|openalex)$", description="Publication backend")
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
    # Email / SMTP digest channel (sibling of Slack). Add "email" to an alert's
    # channels to receive digests here. Password lives in the secret store.
    smtp_host: Optional[str] = Field(None, description="SMTP server host for email digests")
    smtp_port: Optional[int] = Field(587, description="SMTP port (587 = STARTTLS, 465 = implicit TLS)")
    smtp_username: Optional[str] = Field(None, description="SMTP auth username")
    smtp_password: Optional[str] = Field(None, description="SMTP auth password (stored in the secret store)")
    smtp_from: Optional[str] = Field(None, description="From address for digest emails")
    smtp_to: Optional[str] = Field(None, description="Recipient list (comma / newline separated)")
    smtp_use_tls: Optional[bool] = Field(True, description="Use STARTTLS (ignored on port 465)")
    # OpenAlex API key — REQUIRED since 2026-02-13 (the email "polite pool"
    # was discontinued; keyless requests get 100 credits/day then HTTP 409).
    openalex_api_key: Optional[str] = Field(None, description="OpenAlex API key (required — get one free at openalex.org/settings/api)")
    # Semantic Scholar API key — strongly recommended; without it S2 uses the
    # shared anonymous worldwide pool and 429s frequently (stalls Discovery).
    semantic_scholar_api_key: Optional[str] = Field(None, description="Semantic Scholar API key (strongly recommended — avoids shared-pool 429s)")
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


def _is_masked_secret_value(value: object) -> bool:
    """True when the incoming value is a redacted echo of the GET mask
    (`****<suffix>`), i.e. the UI re-submitted the masked display rather
    than a new credential. Shared by every `mask_secret(..., suffix=4)`
    field (OpenAlex key, S2 key)."""
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
    # `get_openalex_api_key` / `get_semantic_scholar_api_key` resolve
    # env-then-secret-store, so the masked display reflects whichever
    # location actually holds the value — consistent with how each client
    # itself reads it.
    out["openalex_api_key"] = mask_secret(get_openalex_api_key(), suffix=4)
    out["semantic_scholar_api_key"] = mask_secret(get_semantic_scholar_api_key(), suffix=4)
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
    openalex_api_key_masked = mask_secret(get_openalex_api_key(), suffix=4)
    semantic_scholar_api_key_masked = mask_secret(get_semantic_scholar_api_key(), suffix=4)
    smtp_password_masked = mask_secret(get_secret(SECRET_SMTP_PASSWORD), suffix=4)

    return SettingsModel(
        backend=raw.get("backend", "openalex"),
        openalex_email=raw.get("openalex_email"),
        fetch_full_history=bool(raw.get("fetch_full_history", False)),
        from_year=raw.get("from_year"),
        api_call_delay=str(raw.get("api_call_delay", "1.0")),
        database=raw.get("database"),
        slack_config_path=raw.get("slack_config_path"),
        slack_token=slack_token_masked,
        slack_channel=raw.get("slack_channel"),
        smtp_host=raw.get("smtp_host"),
        smtp_port=int(raw.get("smtp_port", 587) or 587),
        smtp_username=raw.get("smtp_username"),
        smtp_password=smtp_password_masked,
        smtp_from=raw.get("smtp_from"),
        smtp_to=raw.get("smtp_to"),
        smtp_use_tls=bool(raw.get("smtp_use_tls", True)),
        openalex_api_key=openalex_api_key_masked,
        semantic_scholar_api_key=semantic_scholar_api_key_masked,
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
        conn = sqlite3.connect(str(get_db_path()), check_same_thread=False, timeout=30.0)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA busy_timeout=30000")
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
        incoming_s2_key = data.pop("semantic_scholar_api_key", None)
        incoming_slack_token = data.pop("slack_token", None)
        incoming_smtp_password = data.pop("smtp_password", None)

        if incoming_openalex_key is not None and not _is_masked_secret_value(incoming_openalex_key):
            clean_openalex_key = str(incoming_openalex_key).strip()
            if clean_openalex_key:
                _rotate_env_secret(_OPENALEX_ENV_KEY, SECRET_OPENALEX_API_KEY, clean_openalex_key)
            else:
                _delete_env_secret(_OPENALEX_ENV_KEY, SECRET_OPENALEX_API_KEY)
            # The shared OpenAlex client is reset below (single call
            # for either rotation or delete + any email change).

        if incoming_s2_key is not None and not _is_masked_secret_value(incoming_s2_key):
            clean_s2_key = str(incoming_s2_key).strip()
            if clean_s2_key:
                _rotate_env_secret(_SEMANTIC_SCHOLAR_ENV_KEY, SECRET_SEMANTIC_SCHOLAR_API_KEY, clean_s2_key)
            else:
                _delete_env_secret(_SEMANTIC_SCHOLAR_ENV_KEY, SECRET_SEMANTIC_SCHOLAR_API_KEY)
            # No client reset needed: `core.http_sources._semantic_headers`
            # reads `get_semantic_scholar_api_key()` fresh on every request,
            # and the in-process env was just updated.

        if incoming_slack_token is not None and not _is_masked_slack_token(incoming_slack_token):
            clean_slack_token = str(incoming_slack_token).strip()
            if clean_slack_token:
                set_secret(SECRET_SLACK_BOT_TOKEN, clean_slack_token)
            else:
                delete_secret(SECRET_SLACK_BOT_TOKEN)

        if incoming_smtp_password is not None and not _is_masked_secret_value(incoming_smtp_password):
            clean_smtp_password = str(incoming_smtp_password).strip()
            if clean_smtp_password:
                set_secret(SECRET_SMTP_PASSWORD, clean_smtp_password)
            else:
                delete_secret(SECRET_SMTP_PASSWORD)

        # Write settings first (this validates that paths are relative)
        _write_settings(data)
        # Ensure plaintext credential keys are absent from settings.json.
        config_delete_settings_keys(
            ["slack_token", "openalex_api_key", "semantic_scholar_api_key", "smtp_password"]
        )

        # Validate DB path can be accessed using the resolved absolute path
        from alma.config import get_db_path

        def _ensure_db(resolved_path: Path):
            try:
                resolved_path.parent.mkdir(parents=True, exist_ok=True)
                conn = sqlite3.connect(str(resolved_path), timeout=30.0)
                try:
                    conn.execute("PRAGMA busy_timeout=30000")
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


@router.get("/openalex/status")
def get_openalex_status():
    """Report whether the OpenAlex API key is configured and, if so, whether
    a live probe accepts it. Drives the connection pill in Settings →
    Connections → OpenAlex, mirroring the Semantic Scholar status contract.

    States (green dot = ``valid is True``):
      - ``configured=False`` — no key set (env or secret store).
      - ``valid=True``       — probe returned 200, or 429 (key authenticated
                               before the rate limiter applied).
      - ``valid=False``      — OpenAlex rejected the key (401 / 403).
      - ``valid=None``       — probe could not complete / unexpected status.

    The probe hits ``/rate-limit`` directly (bypasses the response cache) so
    a manual re-check always re-probes with the current key.
    """
    try:
        client = get_openalex_client()
        return client.probe_credentials()
    except Exception as e:
        logger.error("Failed to probe OpenAlex credentials: %s", e)
        return {
            "configured": bool(get_openalex_api_key()),
            "valid": None,
            "detail": "Could not probe OpenAlex.",
        }


@router.get("/semantic-scholar/status")
def get_semantic_scholar_status():
    """Report whether the Semantic Scholar key is configured and, if so,
    whether a live authenticated probe accepts it. Drives the connection dot
    in Settings → Connections → Semantic Scholar.

    States (green dot = ``valid is True``):
      - ``configured=False``         — no key set (env or secret store).
      - ``valid=True``               — probe returned 200, OR 429 (the key
                                       was accepted; S2 authenticates before
                                       applying the 1 req/s throttle).
      - ``valid=False``              — S2 rejected the key (401 / 403).
      - ``valid=None``               — probe could not complete (network /
                                       timeout / unexpected status); unknown.

    The probe is a single cheapest-possible `/paper/search?limit=1` call with
    a short timeout and no retries — it is on-demand only (card mount / manual
    refresh), so it doesn't meaningfully spend the 1 req/s budget.
    """
    key = get_semantic_scholar_api_key()
    if not key:
        return {"configured": False, "valid": None, "detail": "No API key set."}

    try:
        resp = requests.get(
            "https://api.semanticscholar.org/graph/v1/paper/search",
            params={"query": "machine learning", "limit": 1, "fields": "title"},
            headers={"x-api-key": key, "Accept": "application/json"},
            timeout=8,
        )
    except requests.RequestException as exc:
        logger.info("Semantic Scholar key probe could not complete: %s", exc)
        return {
            "configured": True,
            "valid": None,
            "detail": f"Could not reach Semantic Scholar ({exc.__class__.__name__}).",
        }

    if resp.status_code == 200:
        return {"configured": True, "valid": True, "detail": "Key accepted."}
    if resp.status_code == 429:
        return {
            "configured": True,
            "valid": True,
            "detail": "Key accepted (rate-limited right now — 1 req/s).",
        }
    if resp.status_code in (401, 403):
        return {
            "configured": True,
            "valid": False,
            "detail": "Semantic Scholar rejected the key (invalid or unauthorized).",
        }
    return {
        "configured": True,
        "valid": None,
        "detail": f"Unexpected Semantic Scholar response ({resp.status_code}).",
    }


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
