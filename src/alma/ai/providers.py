"""Multi-provider embedding system.

Supports two embedding backends:
- Local SPECTER2 (`local`, scientific paper embeddings) -- opt-in
- OpenAI (API, text-embedding-3-small, 1536-dim) -- opt-in

All providers implement the EmbeddingProvider protocol.
Provider selection is stored in discovery_settings (key: ai.provider).
"""

import logging
import sqlite3
import time
from typing import Optional, Protocol, runtime_checkable

from alma.ai.environment import activate_dependency_environment
from alma.ai.import_state import module_available
from alma.config import get_openai_api_key

logger = logging.getLogger(__name__)

from dataclasses import dataclass


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
def _import_openai_module():
    """Import and return the openai module."""
    import openai

    return openai


def _import_requests_module():
    """Import and return the requests module."""
    import requests

    return requests


# ---------------------------------------------------------------------------
# Protocol
# ---------------------------------------------------------------------------


@runtime_checkable
class EmbeddingProvider(Protocol):
    """Protocol that all embedding providers must implement."""

    name: str
    dimension: int

    @property
    def model_name(self) -> str:
        """Canonical model identifier stored in publication_embeddings.model.

        This is the single source of truth that must equal the value
        written to ``discovery_settings.embedding_model`` so read paths
        that filter by the active model can match vectors produced by
        this provider. Examples: ``allenai/specter2_base``,
        ``text-embedding-3-small``, ``nomic-embed-text``.
        """
        ...

    def is_available(self) -> bool:
        """Return True if this provider is ready to produce embeddings."""
        ...

    def embed(self, texts: list[str]) -> list[list[float]]:
        """Embed a batch of texts into dense vectors.

        Args:
            texts: List of text strings to embed.

        Returns:
            List of embedding vectors, one per input text.
        """
        ...


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
# OpenAI (API, text-embedding-3-small)
# ---------------------------------------------------------------------------


class OpenAIProvider:
    """Embedding provider using the OpenAI text-embedding-3-small model.

    The API key is read from the unified secret store
    (with OPENAI_API_KEY environment variable override).
    """

    name: str = "openai"
    dimension: int = 1536
    MODEL_NAME: str = "text-embedding-3-small"

    def __init__(self, api_key: str = "", api_call_delay: float = 0.5) -> None:
        self._api_key = api_key
        self._api_call_delay = api_call_delay

    @property
    def model_name(self) -> str:
        return self.MODEL_NAME

    def _resolve_api_key(self) -> str:
        """Return the API key from provider override or global config."""
        if self._api_key:
            return self._api_key
        return get_openai_api_key() or ""

    def is_available(self) -> bool:
        """Check if the openai package is installed and an API key is set."""
        if not module_available("openai"):
            return False
        return bool(self._resolve_api_key())

    def embed(self, texts: list[str]) -> list[list[float]]:
        """Embed texts via the OpenAI embeddings API.

        Respects the configured API call delay between requests.
        """
        api_key = self._resolve_api_key()
        if not api_key:
            raise RuntimeError("OpenAI API key is not configured")

        if not module_available("openai"):
            raise RuntimeError("openai package is not installed")

        openai_module = _import_openai_module()
        client = openai_module.OpenAI(api_key=api_key)
        results: list[list[float]] = []

        # Process in batches of 100 to stay within API limits
        batch_size = 100
        for i in range(0, len(texts), batch_size):
            batch = texts[i : i + batch_size]

            if i > 0 and self._api_call_delay > 0:
                time.sleep(self._api_call_delay)

            response = client.embeddings.create(
                model=self.MODEL_NAME,
                input=batch,
            )
            for item in response.data:
                results.append(item.embedding)

        return results


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


def get_active_provider(conn: sqlite3.Connection) -> Optional[EmbeddingProvider]:
    """Return the embedding provider selected in discovery_settings.

    Reads ``ai.provider`` from the database and instantiates the matching
    provider. For OpenAI, the API key and call delay are also read from
    settings.

    Returns:
        The active provider instance, or None if ``ai.provider`` is
        ``"none"`` or the selected provider is not available.
    """
    # Keep optional imports aligned with the dependency env selected in Settings.
    activate_dependency_environment(conn)

    provider_name = _read_setting(conn, "ai.provider", "none")

    if provider_name == "none":
        return None

    if provider_name == "local":
        model_key = _read_setting(conn, "ai.local_model", DEFAULT_LOCAL_MODEL)
        provider = LocalEmbeddingProvider(model_key=model_key)
    elif provider_name == "openai":
        delay_str = _read_setting(conn, "api_call_delay", "0.5")
        try:
            delay = float(delay_str)
        except (ValueError, TypeError):
            delay = 0.5
        provider = OpenAIProvider(api_call_delay=delay)
    else:
        logger.warning("Unknown embedding provider: %s", provider_name)
        return None

    if not provider.is_available():
        logger.warning(
            "Embedding provider '%s' is configured but not available",
            provider_name,
        )
        return None

    return provider


def list_available_providers(
    conn: Optional[sqlite3.Connection] = None,
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

    # OpenAI
    openai_provider = OpenAIProvider()
    openai_available = openai_provider.is_available()
    if not module_available("openai"):
        reason = "openai package is not installed"
    elif not openai_provider._resolve_api_key():
        reason = "OpenAI API key is not configured"
    else:
        reason = None
    providers_info.append({
        "name": openai_provider.name,
        "display_name": "OpenAI embeddings",
        "model_display_name": openai_provider.MODEL_NAME,
        "provider_type": "hosted_embedding",
        "icon": "cloud",
        "description": "Use the OpenAI embeddings API for new or missing vectors.",
        "canonical_model": openai_provider.model_name,
        "dimension": openai_provider.dimension,
        "available": openai_available,
        "reason": reason,
    })


    return providers_info
