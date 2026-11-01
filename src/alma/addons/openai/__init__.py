"""OpenAI: embeddings from the OpenAI API, as an alternative to local SPECTER2.

This file is the manifest and stays light. Everything heavy sits behind the lazy
callables below, because core reaches this module while it is still importing.
"""

from __future__ import annotations

from typing import Any

from alma.addons.openai.schema import CORE_ROWS, disable_warning, has_user_data, on_remove
from alma.core.addons import AddOn
from alma.core.secrets import SECRET_OPENAI_API_KEY


def _embedding_provider() -> Any:
    from alma.addons.openai.provider import OpenAIProvider

    return OpenAIProvider


ADDON = AddOn(
    id="openai",
    name="OpenAI",
    description=(
        "Compute embeddings with the OpenAI API. Local SPECTER2 and Semantic Scholar "
        "vectors work without it."
    ),
    docs_path="concepts/ai/",
    has_user_data=has_user_data,
    secret_keys=(SECRET_OPENAI_API_KEY,),
    # A job computing OpenAI vectors runs under the shared `ai` namespace.
    operation_prefixes=("ai",),
    embedding_provider=_embedding_provider,
    core_rows=CORE_ROWS,
    on_remove=on_remove,
    disable_warning=disable_warning,
)
