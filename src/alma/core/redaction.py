"""Utility helpers to redact credential-like values from logs and payloads."""

from __future__ import annotations

import re
from typing import Any

_REDACTED = "[REDACTED]"
# Matches dict keys whose NAME indicates a credential value.
# Anchored on underscore boundaries + end-of-key so that public identifiers
# (openalex_id, openai_llm_model, slack_channel) and metric names
# (max_tokens, token_overlap, tokenizer) are NOT redacted, while credential
# names (api_key, access_token, slack_token, smtp_password, webhook_url, etc.)
# are. Substring matches on `openai` / `openalex` / `slack` were intentionally
# dropped — those identify public resources, not secrets.
_SENSITIVE_KEY_RE = re.compile(
    r"""
    (?:^|_)(?:
        api[_-]?key
      | (?:access|refresh|bearer|api|auth|slack|github|gitlab|openai|openalex|anthropic)[_-]?token
      | token
      | password
      | secret
      | authorization
      | webhook(?:_url)?
    )$
    """,
    re.IGNORECASE | re.VERBOSE,
)
_PATTERNS: list[tuple[re.Pattern[str], str]] = [
    (
        re.compile(r"(?i)\b(api[_-]?key|token|access[_-]?token|password|secret)\s*=\s*([^&\s]+)"),
        r"\1=" + _REDACTED,
    ),
    (
        re.compile(r"(?i)\b(api[_-]?key|token|access[_-]?token|password|secret)\s*:\s*([^\s,;]+)"),
        r"\1: " + _REDACTED,
    ),
    (
        re.compile(r"(?i)\bauthorization\s*:\s*bearer\s+[A-Za-z0-9._\-+/=]+"),
        "Authorization: Bearer " + _REDACTED,
    ),
    (
        re.compile(r"(?i)\bbearer\s+[A-Za-z0-9._\-+/=]{8,}"),
        "Bearer " + _REDACTED,
    ),
    (
        re.compile(r"\bxox[baprs]-[A-Za-z0-9\-]+\b"),
        _REDACTED,
    ),
    (
        re.compile(r"\bsk-[A-Za-z0-9]{10,}\b"),
        _REDACTED,
    ),
]


def redact_sensitive_text(text: str) -> str:
    """Redact common credential patterns from a free-form string."""
    if not text:
        return text
    out = str(text)
    for pattern, replacement in _PATTERNS:
        out = pattern.sub(replacement, out)
    return out


def redact_sensitive_data(value: Any, *, _depth: int = 0) -> Any:
    """Recursively redact sensitive-looking keys/values in nested data."""
    if _depth > 8:
        return value
    if isinstance(value, str):
        return redact_sensitive_text(value)
    if isinstance(value, dict):
        out: dict[Any, Any] = {}
        for key, item in value.items():
            key_str = str(key)
            if _SENSITIVE_KEY_RE.search(key_str):
                out[key] = _REDACTED
            else:
                out[key] = redact_sensitive_data(item, _depth=_depth + 1)
        return out
    if isinstance(value, list):
        return [redact_sensitive_data(item, _depth=_depth + 1) for item in value]
    if isinstance(value, tuple):
        return tuple(redact_sensitive_data(item, _depth=_depth + 1) for item in value)
    if isinstance(value, set):
        return {redact_sensitive_data(item, _depth=_depth + 1) for item in value}
    return value
