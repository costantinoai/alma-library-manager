"""Embedding providers.

Local SPECTER2 (`local`) is the one provider core owns. Any other provider comes
from an add-on through ``AddOn.embedding_provider``, and exists here only while
that add-on is on, so a switched-off provider reads as not available everywhere.

All providers implement the EmbeddingProvider protocol. The choice is stored in
discovery_settings (key: ai.provider), and ``embedding_model`` follows it: that
row is the model every reader filters vectors by, so it must always name a model
something can keep filling. Semantic Scholar's SPECTER2 vectors arrive whatever
the provider, which makes SPECTER2 the model to read when no provider is running.
"""

import logging
import sqlite3
from dataclasses import dataclass
from typing import Any, Protocol, runtime_checkable

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

#: The provider names core owns. Any other name belongs to an add-on.
CORE_PROVIDERS: tuple[str, ...] = ("none", "local")


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


class AddonEmbeddingProvider(EmbeddingProvider, Protocol):
    """What an add-on's provider class offers on top of embedding."""

    #: Canonical model id, without the storage version.
    MODEL_NAME: str
    #: Name for status lines, such as Home's embeddings pill.
    DISPLAY_NAME: str
    #: The pip line Settings shows when the environment lacks what it needs.
    INSTALL_LINE: str

    @classmethod
    def from_settings(cls, conn: sqlite3.Connection) -> "AddonEmbeddingProvider":
        """Build the provider from stored settings."""
        ...

    @classmethod
    def describe(cls) -> dict[str, Any]:
        """Its ``/ai/status`` entry. ``credential``, when set, asks Settings for
        a key: ``{label, placeholder}``."""
        ...

    @classmethod
    def save_credential(cls, value: str) -> None:
        """Store this provider's key. An empty value deletes it."""
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


def addon_providers() -> dict[str, type[AddonEmbeddingProvider]]:
    """Provider classes of the add-ons that are on, by name."""
    from alma.core import addons

    classes = [
        addon.embedding_provider()
        for addon in addons.enabled()
        if addon.embedding_provider is not None
    ]
    return {cls.name: cls for cls in classes}


def selectable_providers() -> set[str]:
    """Every provider name that can be chosen right now."""
    return {*CORE_PROVIDERS, *addon_providers()}


def switched_off_provider(conn: sqlite3.Connection) -> str | None:
    """The chosen provider's name when nothing can run it, else None.

    Its add-on is off or gone. The choice is kept, so switching it back on
    resumes where it was, and Health says plainly that no provider runs.
    """
    name = configured_provider_name(conn)
    return None if name in selectable_providers() else name


def hosted_provider_label() -> str | None:
    """The display name of the first add-on provider ready to embed."""
    for provider_class in addon_providers().values():
        if provider_class().is_available():
            return provider_class.DISPLAY_NAME
    return None


def expected_embedding_model(conn: sqlite3.Connection) -> str:
    """The model the chosen provider writes, or SPECTER2 when it cannot run."""
    from alma.discovery.defaults import DISCOVERY_SETTINGS_DEFAULTS

    name = configured_provider_name(conn)
    if name == "local":
        model_key = _read_setting(conn, "ai.local_model", DEFAULT_LOCAL_MODEL)
        return (LOCAL_MODELS.get(model_key) or LOCAL_MODELS[DEFAULT_LOCAL_MODEL]).hf_id
    provider_class = addon_providers().get(name)
    if provider_class is not None and provider_class().is_available():
        return provider_class.MODEL_NAME
    return DISCOVERY_SETTINGS_DEFAULTS["embedding_model"]


def sync_embedding_model(conn: sqlite3.Connection) -> None:
    """Make ``embedding_model`` follow the provider. The caller owns the unit.

    Readers filter by this row, and a model nothing fills leaves Discovery
    looking at a shrinking set while Semantic Scholar's vectors go unread.
    """
    _write_setting(conn, "embedding_model", expected_embedding_model(conn))


def reconcile_embedding_model(conn: sqlite3.Connection) -> None:
    """Correct ``embedding_model`` after an add-on provider came or went.

    Only an add-on provider's choice is touched: with `none` or `local` the row
    stays as set, so a value someone chose is never overwritten at startup.
    Opens its own write unit, and only when there is something to change.
    """
    from alma.core.db_write import run_write_unit

    if configured_provider_name(conn) in CORE_PROVIDERS:
        return
    if _read_setting(conn, "embedding_model", "") == expected_embedding_model(conn):
        return
    run_write_unit(conn, lambda: sync_embedding_model(conn), label="reconcile embedding model")


def forget_provider(conn: sqlite3.Connection, name: str) -> None:
    """Stop choosing a provider that was removed. The caller owns the unit.

    The choice falls back to the install's default: local SPECTER2 where the
    environment can run it (D19), otherwise none.
    """
    from alma.api.deps import default_ai_provider

    if configured_provider_name(conn) == name:
        fallback = default_ai_provider()
        _write_setting(conn, "ai.provider", fallback if fallback in CORE_PROVIDERS else "none")
    sync_embedding_model(conn)


def get_active_provider(conn: sqlite3.Connection) -> EmbeddingProvider | None:
    """Return the embedding provider selected in discovery_settings.

    Reads ``ai.provider`` from the database and builds the matching provider.

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
    else:
        provider_class = addon_providers().get(provider_name)
        if provider_class is None:
            logger.warning("Embedding provider '%s' is unknown or switched off", provider_name)
            return None
        provider = provider_class.from_settings(conn)

    if not provider.is_available():
        logger.warning(
            "Embedding provider '%s' is configured but not available",
            provider_name,
        )
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

    providers_info.extend(provider.describe() for provider in addon_providers().values())

    return providers_info
