"""What the OpenAI add-on owns, and what core may do to it.

It has no tables of its own. Its vectors and author centroids sit in core tables,
so it declares those rows instead, and Remove deletes them and nothing else. This
module stays light, because the manifest imports it.
"""

from __future__ import annotations

import os
import sqlite3

from alma.core.addons import CoreRows
from alma.core.secrets import SECRET_OPENAI_API_KEY, get_secret

PROVIDER_NAME = "openai"
MODEL_NAME = "text-embedding-3-small"

#: The raw id, and every versioned storage key built from it.
_MODEL_MATCH = "model = ? OR model GLOB ?"
_MODEL_PARAMS = (MODEL_NAME, f"{MODEL_NAME}@*")

CORE_ROWS: tuple[CoreRows, ...] = (
    CoreRows(
        label=f"OpenAI vectors ({MODEL_NAME})",
        table="publication_embeddings",
        where=f"{_MODEL_MATCH} OR source = ?",
        params=(*_MODEL_PARAMS, PROVIDER_NAME),
    ),
    CoreRows(
        label="Author centroids built from them",
        table="author_centroids",
        where=_MODEL_MATCH,
        params=_MODEL_PARAMS,
    ),
)


def get_openai_api_key() -> str | None:
    """The key: ``OPENAI_API_KEY`` first, then the secret store."""
    env_key = os.getenv("OPENAI_API_KEY")
    if env_key:
        return env_key
    try:
        return get_secret(SECRET_OPENAI_API_KEY)
    except Exception:
        return None


def _is_selected(conn: sqlite3.Connection) -> bool:
    row = conn.execute("SELECT value FROM discovery_settings WHERE key = 'ai.provider'").fetchone()
    return row is not None and str(row[0] or "").strip().lower() == PROVIDER_NAME


def has_user_data(conn: sqlite3.Connection) -> bool:
    """True when OpenAI is in use, so an upgrade must keep it on.

    Any one of these counts: it is the selected provider, a key is saved or set
    in the environment, or its vectors are stored.
    """
    if get_openai_api_key():
        return True
    try:
        if _is_selected(conn):
            return True
        vectors = CORE_ROWS[0]
        return (
            conn.execute(
                f"SELECT 1 FROM {vectors.table} WHERE {vectors.where} LIMIT 1",  # noqa: S608
                vectors.params,
            ).fetchone()
            is not None
        )
    except sqlite3.OperationalError:
        return False


def disable_warning(conn: sqlite3.Connection) -> str | None:
    """What switching off would cost, while it is the selected provider."""
    if not _is_selected(conn):
        return None
    return (
        "OpenAI is your embedding provider. With it off nothing computes new vectors, and "
        "Health shows no provider. Discovery reads the SPECTER2 vectors from Semantic Scholar "
        "until a provider runs again. Your OpenAI vectors and key stay."
    )


def on_remove(conn: sqlite3.Connection) -> None:
    """Leave no choice behind that points at what is gone. Runs inside Remove's unit."""
    from alma.ai.providers import forget_provider

    forget_provider(conn, PROVIDER_NAME)
    # The legacy home of the key, scrubbed long ago and kept as an empty row.
    conn.execute("DELETE FROM discovery_settings WHERE key = 'ai.openai_api_key'")
