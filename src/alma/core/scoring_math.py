"""Shared scoring primitives used across paper Discovery and the author rail.

These four helpers were duplicated across `discovery.scoring`,
`application.signal_projection`, `application.authors`,
`application.discovery`, `application.feed`, `application.gap_radar`,
`application.paper_signal`, and `discovery.source_search` — sometimes
multiple times in the same file. Consolidating here means a calibration
change (e.g. tuning the consensus bonus fraction or the half-life
default) takes effect everywhere by construction.
"""

from __future__ import annotations

import math
from typing import Mapping


def clamp(value: float, lo: float, hi: float) -> float:
    """Constrain ``value`` to ``[lo, hi]``."""
    return max(lo, min(hi, value))


def age_decay(age_days: float | None, *, half_life_days: float) -> float:
    """Half-life decay factor in ``(0, 1]``.

    Returns ``1.0`` when ``age_days`` is ``None`` (treat as fresh) or
    ``0.5 ** (age_days / half_life_days)`` otherwise. The same shape
    is used for paper-feedback events, recommendation history, missing-
    author feedback, signal-lab swipes, and the recency component of
    paper_signal scoring.
    """
    if age_days is None:
        return 1.0
    return math.pow(0.5, age_days / half_life_days)


def consensus_bonus(
    n: int, *, fraction: float = 0.12, max_score: float = 100.0
) -> float:
    """Band-relative diminishing-returns bonus for ``N>1`` source confirmations.

    Returns ``fraction × max_score × sqrt(n - 1)`` when ``n > 1``,
    otherwise ``0``. With the default calibration (`fraction=0.12`,
    `max_score=100`) this gives ``+12 / +17 / +21 / +24`` for
    2 / 3 / 4 / 5 sources — diminishing returns so multi-source
    agreement registers as confirmation without overrunning a strong
    single-source signal. Both paper Discovery and the author
    suggestion rail use this with the same defaults.
    """
    if n <= 1:
        return 0.0
    return fraction * max_score * math.sqrt(n - 1)


def log_prevalence_weights(counts: Mapping[str, float]) -> dict[str, float]:
    """Sign-preserving log-prevalence normalization to ``[-1, 1]``.

    For each entry returns ``sign(v) × log(1 + |v|) / log(1 + max|v|)``.
    The top entry is pinned at ``±1.0``; long-tail entries decay
    logarithmically rather than linearly. Empty / all-zero inputs are
    returned as a plain dict copy.

    Mirrors the prevalence pattern the author rail already used —
    sharing the user's #1 topic gets weight 1.0, sharing one that
    appears in 5/50 of the user's papers gets ~0.42 (versus ~0.10
    under linear max-normalization). Long-tail interests stay
    visible in scoring instead of being drowned by the dominant
    cluster.
    """
    if not counts:
        return {}
    max_abs = max(abs(v) for v in counts.values())
    if max_abs <= 0:
        return dict(counts)
    max_log = math.log1p(max_abs)
    if max_log <= 0:
        return dict(counts)
    return {
        key: math.copysign(math.log1p(abs(value)) / max_log, value)
        for key, value in counts.items()
    }
