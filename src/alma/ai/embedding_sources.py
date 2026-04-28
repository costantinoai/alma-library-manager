"""Canonical embedding-source labels stored in publication_embeddings.source."""

from __future__ import annotations

EMBEDDING_SOURCE_UNKNOWN = "unknown"
EMBEDDING_SOURCE_SEMANTIC_SCHOLAR = "semantic_scholar"
EMBEDDING_SOURCE_LOCAL = "local"
EMBEDDING_SOURCE_OPENAI = "openai"


def source_for_provider_name(provider_name: str) -> str:
    """Return the canonical source label for one embedding provider."""
    normalized = (provider_name or "").strip().lower()
    if normalized == EMBEDDING_SOURCE_LOCAL:
        return EMBEDDING_SOURCE_LOCAL
    if normalized == EMBEDDING_SOURCE_OPENAI:
        return EMBEDDING_SOURCE_OPENAI
    return EMBEDDING_SOURCE_UNKNOWN
