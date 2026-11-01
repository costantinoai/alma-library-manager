"""The embedding provider: local SPECTER2, or none.

The choice is stored in discovery_settings (key: ai.provider), and
``embedding_model`` follows it: that row is the model every reader filters vectors
by. Semantic Scholar's SPECTER2 vectors arrive whatever the choice, so SPECTER2 is
the model to read with no provider running as well.
"""

import logging
import sqlite3
from dataclasses import dataclass

from alma.ai.environment import activate_dependency_environment
from alma.ai.import_state import module_available
from alma.core.time import utcnow

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class LocalModelConfig:
    """Configuration for a local embedding model."""

    key: str
    hf_id: str
    dimension: int
    max_tokens: int
    query_prefix: str
    display_name: str
    description: str
    backend: str = "specter2"


LOCAL_MODELS: dict[str, LocalModelConfig] = {
    "specter2-base": LocalModelConfig(
        key="specter2-base",
        hf_id="allenai/specter2_base",
        dimension=768,
        max_tokens=512,
        query_prefix="",
        display_name="SPECTER2 Base",
        description="Scientific papers · Local fallback",
        backend="specter2",
    ),
}

DEFAULT_LOCAL_MODEL = "specter2-base"

#: Every value `ai.provider` can hold.
PROVIDERS: tuple[str, ...] = ("none", "local")


# ---------------------------------------------------------------------------
# Local SPECTER2
# ---------------------------------------------------------------------------


class LocalEmbeddingProvider:
    """Embedding provider using local SPECTER2 for scientific papers."""

    def __init__(self, model_key: str = DEFAULT_LOCAL_MODEL) -> None:
        self._config = LOCAL_MODELS.get(model_key, LOCAL_MODELS[DEFAULT_LOCAL_MODEL])

    @property
    def name(self) -> str:
        return "local"

    @property
    def dimension(self) -> int:
        return self._config.dimension

    @property
    def model_config(self) -> LocalModelConfig:
        return self._config

    @property
    def model_name(self) -> str:
        return self._config.hf_id

    def is_available(self) -> bool:
        """Check if the configured local embedding stack is installed."""
        return all(
            module_available(module)
            for module in ("adapters", "transformers", "torch", "numpy")
        )

    def embed(self, texts: list[str]) -> list[list[float]]:
        """Embed texts using the configured local model."""
        from alma.discovery.similarity import SpecterEmbedder

        embedder = SpecterEmbedder.get_instance(
            model_name=self._config.hf_id,
            embedding_dim=self._config.dimension,
            max_length=self._config.max_tokens,
        )
        embeddings = embedder.encode(texts)
        return [row.tolist() for row in embeddings]


# ---------------------------------------------------------------------------
# Provider resolution helpers
# ---------------------------------------------------------------------------

def _read_setting(conn: sqlite3.Connection, key: str, default: str = "") -> str:
    """Read a single value from the discovery_settings table."""
    try:
        row = conn.execute(
            "SELECT value FROM discovery_settings WHERE key = ?", (key,)
        ).fetchone()
        if row is not None:
            return row["value"] if isinstance(row, sqlite3.Row) else row[0]
    except sqlite3.OperationalError:
        pass
    return default


def _write_setting(conn: sqlite3.Connection, key: str, value: str) -> None:
    conn.execute(
        "INSERT OR REPLACE INTO discovery_settings (key, value, updated_at) VALUES (?, ?, ?)",
        (key, value, utcnow().isoformat()),
    )


def configured_provider_name(conn: sqlite3.Connection) -> str:
    """The stored choice, whether or not that provider can run."""
    return _read_setting(conn, "ai.provider", "none").strip().lower() or "none"


def expected_embedding_model(conn: sqlite3.Connection) -> str:
    """The model readers filter by: the local model's, or SPECTER2 with no provider."""
    from alma.discovery.defaults import DISCOVERY_SETTINGS_DEFAULTS

    if configured_provider_name(conn) == "local":
        model_key = _read_setting(conn, "ai.local_model", DEFAULT_LOCAL_MODEL)
        return (LOCAL_MODELS.get(model_key) or LOCAL_MODELS[DEFAULT_LOCAL_MODEL]).hf_id
    return DISCOVERY_SETTINGS_DEFAULTS["embedding_model"]


def sync_embedding_model(conn: sqlite3.Connection) -> None:
    """Make ``embedding_model`` follow the provider. The caller owns the unit."""
    _write_setting(conn, "embedding_model", expected_embedding_model(conn))


def get_active_provider(conn: sqlite3.Connection) -> LocalEmbeddingProvider | None:
    """Return the local provider when it is selected and can run, else None."""
    # Keep optional imports aligned with the dependency env selected in Settings.
    activate_dependency_environment(conn)

    if configured_provider_name(conn) != "local":
        return None

    provider = LocalEmbeddingProvider(
        model_key=_read_setting(conn, "ai.local_model", DEFAULT_LOCAL_MODEL)
    )
    if not provider.is_available():
        logger.warning("Embedding provider 'local' is configured but not available")
        return None
    return provider


def list_available_providers(
    conn: sqlite3.Connection | None = None,
) -> list[dict]:
    """Return information about each known embedding provider.

    Each entry contains:
    - name: Provider identifier.
    - dimension: Embedding vector dimension.
    - available: Whether the provider can be used right now.
    - reason: Human-readable explanation when not available.
    """
    if conn is not None:
        activate_dependency_environment(conn)

    providers_info: list[dict] = []

    # Local SPECTER2 stack
    current_model_key = DEFAULT_LOCAL_MODEL
    if conn is not None:
        current_model_key = _read_setting(conn, "ai.local_model", DEFAULT_LOCAL_MODEL)
    local_provider = LocalEmbeddingProvider(model_key=current_model_key)
    local_available = local_provider.is_available()
    if local_available:
        local_reason = None
    else:
        local_reason = "Local SPECTER2 requires adapters, transformers, torch, and numpy"
    # Probe torch for the device the local SPECTER2 encoder will use.
    # Match SpecterEmbedder._resolve_device() exactly: prefer CUDA when
    # torch sees an available GPU, otherwise CPU. Done with a small
    # try/except so missing torch (lite variant) just reports None.
    local_device: str | None = None
    if local_available:
        try:
            import torch  # type: ignore

            local_device = "cuda" if torch.cuda.is_available() else "cpu"
        except Exception:
            local_device = None

    providers_info.append({
        "name": "local",
        "display_name": "Local SPECTER2",
        "model_display_name": local_provider.model_config.display_name,
        "provider_type": "local_embedding",
        "icon": "cpu",
        "description": "Compute missing scientific-paper vectors with local SPECTER2.",
        "canonical_model": local_provider.model_name,
        "dimension": local_provider.dimension,
        "available": local_available,
        "reason": local_reason,
        "device": local_device,
        "local_models": [
            {
                "key": m.key,
                "display_name": m.display_name,
                "description": m.description,
                "dimension": m.dimension,
                "hf_id": m.hf_id,
                "backend": m.backend,
            }
            for m in LOCAL_MODELS.values()
        ],
        "selected_model": current_model_key,
    })

    return providers_info
