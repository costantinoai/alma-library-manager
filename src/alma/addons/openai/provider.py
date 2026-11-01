"""The OpenAI embedding provider.

Vectors from this model live in the core table ``publication_embeddings`` under a
versioned key, apart from SPECTER2's: the two cannot be compared.
"""

from __future__ import annotations

import logging
import sqlite3
import time
from typing import Any

from alma.addons.openai.schema import MODEL_NAME, PROVIDER_NAME, get_openai_api_key
from alma.ai.import_state import module_available
from alma.core.secrets import SECRET_OPENAI_API_KEY, delete_secret, set_secret

logger = logging.getLogger(__name__)


def _import_openai_module():
    """Import and return the openai module."""
    import openai

    return openai


class OpenAIProvider:
    """Embedding provider using the OpenAI text-embedding-3-small model.

    The API key is read from the unified secret store
    (with OPENAI_API_KEY environment variable override).
    """

    name: str = PROVIDER_NAME
    dimension: int = 1536
    MODEL_NAME: str = MODEL_NAME
    DISPLAY_NAME: str = "OpenAI embeddings"
    INSTALL_LINE: str = "pip install openai numpy"

    def __init__(self, api_key: str = "", api_call_delay: float = 0.5) -> None:
        self._api_key = api_key
        self._api_call_delay = api_call_delay

    @classmethod
    def from_settings(cls, conn: sqlite3.Connection) -> "OpenAIProvider":
        """Build the provider with the stored call delay."""
        row = conn.execute(
            "SELECT value FROM discovery_settings WHERE key = 'api_call_delay'"
        ).fetchone()
        try:
            delay = float(row[0]) if row else 0.5
        except (ValueError, TypeError):
            delay = 0.5
        return cls(api_call_delay=delay)

    @classmethod
    def describe(cls) -> dict[str, Any]:
        """This provider's entry in ``/ai/status``."""
        provider = cls()
        if not module_available("openai"):
            reason = "openai package is not installed"
        elif not provider._resolve_api_key():
            reason = "OpenAI API key is not configured"
        else:
            reason = None
        return {
            "name": provider.name,
            "display_name": cls.DISPLAY_NAME,
            "model_display_name": cls.MODEL_NAME,
            "provider_type": "hosted_embedding",
            "icon": "cloud",
            "description": "Use the OpenAI embeddings API for new or missing vectors.",
            "canonical_model": provider.model_name,
            "dimension": provider.dimension,
            "available": provider.is_available(),
            "reason": reason,
            "credential": {"label": "OpenAI API Key", "placeholder": "Paste OpenAI API key"},
        }

    @classmethod
    def save_credential(cls, value: str) -> None:
        """Store the API key. An empty value deletes it."""
        if value:
            set_secret(SECRET_OPENAI_API_KEY, value)
        else:
            delete_secret(SECRET_OPENAI_API_KEY)

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
        from alma.core.network_policy import require_network_access

        require_network_access("openai")
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
        max_attempts = 3
        rate_limit_error = getattr(openai_module, "RateLimitError", None)
        for i in range(0, len(texts), batch_size):
            batch = texts[i : i + batch_size]

            if i > 0 and self._api_call_delay > 0:
                time.sleep(self._api_call_delay)

            for attempt in range(1, max_attempts + 1):
                try:
                    response = client.embeddings.create(
                        model=self.MODEL_NAME,
                        input=batch,
                    )
                    break
                except Exception as exc:
                    retryable = rate_limit_error is not None and isinstance(
                        exc, rate_limit_error
                    )
                    if not retryable:
                        status = getattr(exc, "status_code", None)
                        message = str(exc)
                        retryable = status in (429, 500, 502, 503, 504) or any(
                            code in message
                            for code in ("429", "500", "502", "503", "504")
                        )
                    if not retryable or attempt >= max_attempts:
                        raise
                    wait = 2.0 * (2 ** (attempt - 1))
                    logger.warning(
                        "OpenAI embeddings batch failed (attempt %d/%d): %s; "
                        "retrying in %.0fs",
                        attempt,
                        max_attempts,
                        exc,
                        wait,
                    )
                    time.sleep(wait)
            for item in response.data:
                results.append(item.embedding)

        return results
