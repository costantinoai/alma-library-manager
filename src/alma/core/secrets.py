"""Unified secret storage for runtime credentials.

Secrets are stored outside settings.json to avoid plaintext config leakage.
"""

from __future__ import annotations

import json
import logging
import os
import sqlite3
import threading
from pathlib import Path
from typing import Optional

logger = logging.getLogger(__name__)

SECRET_SLACK_BOT_TOKEN = "slack.bot_token"
SECRET_SEMANTIC_SCHOLAR_API_KEY = "semantic_scholar.api_key"
SECRET_OPENAI_API_KEY = "openai.api_key"
SECRET_ZOTERO_API_KEY = "zotero.api_key"
# `SECRET_OPENALEX_API_KEY` constant removed 2026-04-24 —
# OpenAlex key now lives only in `.env` (see `alma.config.get_openalex_api_key`).
# `SECRET_ANTHROPIC_API_KEY` constant removed 2026-04-27 with the LLM
# exit (see `tasks/01_LLM_PRODUCTION_EXIT.md`).

_SECRET_KEYS = {
    SECRET_SLACK_BOT_TOKEN,
    SECRET_SEMANTIC_SCHOLAR_API_KEY,
    SECRET_OPENAI_API_KEY,
    SECRET_ZOTERO_API_KEY,
}

_LOCK = threading.RLock()


def _find_project_root() -> Path:
    try:
        from alma.config import get_project_root

        return get_project_root()
    except Exception:
        pass

    current = Path.cwd()
    for parent in [current] + list(current.parents):
        if any((parent / marker).exists() for marker in ("settings.json", "pyproject.toml", "docker-compose.yml", ".git")):
            return parent
    return current


def _resolve_store_path() -> Path:
    raw = os.getenv("ALMA_SECRETS_PATH", "").strip()
    if raw:
        p = Path(raw).expanduser()
        if not p.is_absolute():
            p = _find_project_root() / p
        return p
    return _find_project_root() / "data" / "secrets.json"


def _read_store() -> dict[str, str]:
    path = _resolve_store_path()
    if not path.exists():
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
        if isinstance(payload, dict):
            out: dict[str, str] = {}
            for k, v in payload.items():
                if isinstance(k, str) and isinstance(v, str):
                    out[k] = v
            return out
    except Exception as exc:
        logger.warning("Failed reading secret store: %s", exc)
    return {}


def _write_store(data: dict[str, str]) -> None:
    path = _resolve_store_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(data, indent=2, sort_keys=True), encoding="utf-8")
    try:
        os.chmod(tmp, 0o600)
    except Exception:
        pass
    tmp.replace(path)
    try:
        os.chmod(path, 0o600)
    except Exception:
        pass


def get_secret(secret_key: str) -> Optional[str]:
    """Read a namespaced secret."""
    key = (secret_key or "").strip()
    if not key:
        return None
    with _LOCK:
        val = _read_store().get(key)
    if val is None:
        return None
    text = str(val).strip()
    return text or None


def set_secret(secret_key: str, secret_value: str) -> None:
    """Set or update a namespaced secret."""
    key = (secret_key or "").strip()
    val = (secret_value or "").strip()
    if not key:
        raise ValueError("secret_key is required")
    if not val:
        raise ValueError("secret_value is required")
    if key not in _SECRET_KEYS:
        logger.warning("Setting non-standard secret key '%s'", key)
    with _LOCK:
        data = _read_store()
        data[key] = val
        _write_store(data)


def delete_secret(secret_key: str) -> None:
    """Delete a namespaced secret if it exists."""
    key = (secret_key or "").strip()
    if not key:
        return
    with _LOCK:
        data = _read_store()
        if key in data:
            data.pop(key, None)
            _write_store(data)


def mask_secret(value: Optional[str], prefix: int = 0, suffix: int = 4) -> Optional[str]:
    """Return a masked representation for UI responses."""
    if not value:
        return None
    token = value.strip()
    if not token:
        return None
    if prefix > 0 and len(token) > (prefix + suffix):
        return f"{token[:prefix]}...{token[-suffix:]}"
    if len(token) > suffix:
        return f"****{token[-suffix:]}"
    return "****"


def bootstrap_secret_store(conn: sqlite3.Connection) -> None:
    """One-time migration from legacy plaintext secret locations."""
    try:
        from alma.config import get_all_settings, delete_settings_keys
    except Exception:
        return

    settings = get_all_settings()
    moved_settings_keys: list[str] = []

    slack_token = str(settings.get("slack_token") or "").strip()
    if slack_token and not get_secret(SECRET_SLACK_BOT_TOKEN):
        set_secret(SECRET_SLACK_BOT_TOKEN, slack_token)
        moved_settings_keys.append("slack_token")

    # OpenAlex key migration path removed 2026-04-24. If a legacy
    # `openalex_api_key` still sits in settings.json, just strip it —
    # the user reimports through the Settings UI (env-backed) or sets
    # `OPENALEX_API_KEY` in `.env` directly.
    if settings.get("openalex_api_key"):
        moved_settings_keys.append("openalex_api_key")

    if moved_settings_keys:
        delete_settings_keys(moved_settings_keys)

    try:
        row = conn.execute(
            "SELECT value FROM discovery_settings WHERE key = 'ai.openai_api_key'"
        ).fetchone()
        openai_key = (row["value"] if row and isinstance(row, sqlite3.Row) else (row[0] if row else "")) or ""
        openai_key = str(openai_key).strip()
        if openai_key and not get_secret(SECRET_OPENAI_API_KEY):
            set_secret(SECRET_OPENAI_API_KEY, openai_key)
        if openai_key:
            conn.execute(
                "UPDATE discovery_settings SET value = '', updated_at = ? WHERE key = 'ai.openai_api_key'",
                ("1970-01-01T00:00:00",),
            )
            conn.commit()
    except Exception as exc:
        logger.warning("OpenAI key migration to secret store failed: %s", exc)
