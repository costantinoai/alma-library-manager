"""Utility helpers to redact credential-like values from logs and payloads."""

from __future__ import annotations

import re
from typing import Any

_REDACTED = "[REDACTED]"

# THE one list of secret NAMES, shared by text and dict-key redaction so the two
# cannot drift. A name is listed because its VALUE is a credential.
#
# ``_key`` is deliberately NOT a blanket suffix: `zotero_key`, `operation_key`,
# `view_key` and `cache_key` are public handles that Activity and the UI must keep
# showing. Only the kinds below are secrets. `member_key` is listed because in
# ALMa it IS one (Anna's Archive). A new handle stays safe by staying off this
# list; a new secret field must end in one of these names.
_SECRET_NAME = r"""(?:
    (?:api|member|access|secret|private)[_-]?key
  | (?:(?:access|refresh|bearer|api|auth|slack|github|gitlab|openai|openalex|anthropic)[_-]?)?token
  | pass(?:word|wd|phrase)
  | secret
  | credentials?
  | cookie
  | webhook(?:[_-]?url)?
)"""

# Dict keys: the name must be the whole key or its last `_`, `-` or `.` part, so
# public names (openalex_id, max_tokens, token_overlap, tokenizer) are untouched.
_SENSITIVE_KEY_RE = re.compile(
    rf"(?:^|[._-])(?:authorization|{_SECRET_NAME})$",
    re.IGNORECASE | re.VERBOSE,
)

# Never start inside a word, but do start after `_`, `-` or `.`: `\b` does not
# fire after `_`, which is how `openalex_api_key=` used to walk through.
_NOT_IN_WORD = r"(?<![A-Za-z0-9])"

_PATTERNS: list[tuple[re.Pattern[str], str]] = [
    # `name=value`, `name: value` and a dict repr's `'name': 'value'`. An
    # unquoted value runs to whitespace or `&`: eating a stray bracket is
    # cosmetic, leaving the tail of a password is not.
    (
        re.compile(
            rf"""{_NOT_IN_WORD}({_SECRET_NAME})(["']?\s*[:=]\s*)(?:"[^"]*"|'[^']*'|[^\s&]+)""",
            re.IGNORECASE | re.VERBOSE,
        ),
        r"\1\2" + _REDACTED,
    ),
    # A bare ``key`` URL query parameter is a credential by convention (Anna's
    # Archive member API, Google APIs). Only redacted inside a query string
    # (``?key=`` / ``&key=``) so prose like "sort key=year" stays readable.
    (
        re.compile(r"(?i)([?&]key=)([^&\s#]+)"),
        r"\1" + _REDACTED,
    ),
    # Any scheme, and a short value the generic Bearer rule below would pass. Unless
    # the value is a closed quote, the rest of the line goes: a shorter fallback takes
    # the scheme of `b'Bearer x` and leaves the credential, with Bearer rule blinded.
    (
        re.compile(r"""(?i)\b(authorization["']?\s*[:=]\s*)(?:"[^"]*"|'[^']*'|[^\n]*)"""),
        r"\1" + _REDACTED,
    ),
    (
        re.compile(r"(?i)\bbearer\s+[A-Za-z0-9._\-+/=]{8,}"),
        "Bearer " + _REDACTED,
    ),
    # Self-describing tokens, whatever name they sit under.
    (
        re.compile(rf"{_NOT_IN_WORD}xox[a-z](?:\.xox[a-z])?-[A-Za-z0-9\-]+"),
        _REDACTED,
    ),
    (
        re.compile(rf"{_NOT_IN_WORD}xapp-[A-Za-z0-9\-]+"),
        _REDACTED,
    ),
    (
        re.compile(rf"{_NOT_IN_WORD}sk-[A-Za-z0-9_\-]{{10,}}"),
        _REDACTED,
    ),
    # A Slack webhook's path IS the secret.
    (
        re.compile(r"(?i)(https://hooks\.slack\.com/)[^\s'\"<>]+"),
        r"\1" + _REDACTED,
    ),
    # `scheme://user:password@host` and `scheme://token@host`.
    (
        re.compile(r"(?i)\b([a-z][a-z0-9+.\-]*://)[^/\s@'\"?#]+@"),
        r"\1" + _REDACTED + "@",
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
