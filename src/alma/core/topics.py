"""Shared helpers for OpenAlex topic confidence and scoring.

OpenAlex ``publication_topics`` rows carry a relevance ``score``. Task 10's
remaining signal-quality work needs any thresholding to be centralized: a
confidence gate scattered as ``score >= 0.2`` across ranking, retrieval, display,
and reports would drift quickly. This module is the single backend source for
"is this topic confident enough to drive signal?".

The helpers here are read-time only. Raw ``publication_topics`` rows stay intact
for auditability, manual topic cleanup, and future re-tuning.
"""

from __future__ import annotations

from typing import Any

__all__ = [
    "DEFAULT_TOPIC_SCORE",
    "TOPIC_SIGNAL_MIN_SCORE",
    "filter_confident_topic_dicts",
    "topic_confidence_sql",
    "topic_relevance",
    "topic_score_value",
    "topic_signal_relevance",
]


DEFAULT_TOPIC_SCORE = 0.5
"""Fallback relevance for legacy rows without a stored OpenAlex score."""

TOPIC_SIGNAL_MIN_SCORE = 0.20
"""Default minimum relevance for topic rows that may affect ranking/retrieval.

This value is deliberately conservative: the live dev DB has a heavy score mass
near 1.0, and a 0.20 gate drops only the weakest tail. It still requires a live
A/B before being wired into recommendation signal paths.
"""


def topic_score_value(raw_score: Any, *, default: float = DEFAULT_TOPIC_SCORE) -> float:
    """Parse a topic score into a bounded numeric relevance.

    ``None``/invalid scores are treated as ``default`` so legacy topic rows do not
    silently disappear. Values are clamped to OpenAlex's expected ``[0, 1]`` range.
    """
    try:
        value = float(default if raw_score is None else raw_score)
    except (TypeError, ValueError):
        value = float(default)
    return max(0.0, min(1.0, value))


def topic_relevance(raw_score: Any, *, default: float = DEFAULT_TOPIC_SCORE) -> float:
    """Score used as a multiplier after a topic has passed any confidence gate."""
    return max(0.1, topic_score_value(raw_score, default=default))


def topic_signal_relevance(
    raw_score: Any,
    *,
    min_score: float = TOPIC_SIGNAL_MIN_SCORE,
    default: float = DEFAULT_TOPIC_SCORE,
) -> float | None:
    """Return the normalized relevance, or ``None`` when below the signal gate."""
    value = topic_score_value(raw_score, default=default)
    if value < min_score:
        return None
    return max(0.1, value)


def topic_confidence_sql(alias: str = "pt", *, min_score: float = TOPIC_SIGNAL_MIN_SCORE) -> str:
    """SQL predicate for topic rows allowed to affect signal/retrieval.

    The predicate embeds the centralized constants so callers can keep SQL readable
    without hand-maintaining parallel parameter lists. ``alias`` must be a trusted
    local SQL alias, never user input.
    """
    alias = (alias or "pt").strip()
    return f"COALESCE({alias}.score, {DEFAULT_TOPIC_SCORE}) >= {float(min_score)}"


def filter_confident_topic_dicts(
    topics: list[dict[str, Any]] | tuple[dict[str, Any], ...] | None,
    *,
    min_score: float = TOPIC_SIGNAL_MIN_SCORE,
) -> list[dict[str, Any]]:
    """Filter topic dictionaries by the shared confidence gate.

    Returned dictionaries are shallow copies with an explicit normalized
    ``score``. Terms are not renamed or canonicalized here; callers own display
    names vs canonical names.
    """
    out: list[dict[str, Any]] = []
    for topic in topics or []:
        if not isinstance(topic, dict):
            continue
        term = str(topic.get("term") or topic.get("name") or "").strip()
        if not term:
            continue
        relevance = topic_signal_relevance(topic.get("score"), min_score=min_score)
        if relevance is None:
            continue
        next_topic = dict(topic)
        next_topic["score"] = relevance
        out.append(next_topic)
    return out
