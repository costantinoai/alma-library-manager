"""signal_valence — the ONE owner of per-paper preference valence.

Maps a paper's stored signals to a single valence in [-1, +1] for the
space-owned heat field (`/graphs/signal-field`) and any future consumer
that needs "how does the user/engine feel about this paper" as ONE
number. Centralised so the weights are named, greppable, and can never
drift between surfaces (user call 2026-07-25: no hardcoded valence
literals in route code).

Hierarchy: the strongest USER signal wins; engine evidence fills the
rest at reduced authority. Papers with no signal at all return None —
no information is NOT a neutral opinion, and must not dilute the field.
"""

from __future__ import annotations

# ── Named weights (the contract) ──────────────────────────────────────────
# User signals — full authority.
VALENCE_REMOVED = -0.8
"""status='removed'/'dismissed' — the D3/D6 hard negative."""

VALENCE_NEGATIVE_ACTION = -0.6
"""The user dismissed/removed a recommendation of this paper (D6)."""

VALENCE_LIBRARY = 0.35
"""Saved to Library — a mild, durable positive."""

RATING_NEUTRAL = 3
"""The neutral star on the fixed 1–5 rating domain."""

RATING_HALF_RANGE = 2.0
"""Stars from neutral to either end (3★→1★ or 3★→5★)."""

# Engine evidence — half the authority of a user signal.
SCORE_NEUTRAL = 50.0
"""The neutral point of the internal 0–100 recommendation score."""

SCORE_HALF_RANGE = 50.0
"""Score distance from neutral to either end."""

ENGINE_AUTHORITY = 0.5
"""How much an engine opinion counts relative to a user signal."""

# Membership states that read as a hard negative (D3: removed stays
# visible in the corpus and reads as a negative signal).
NEGATIVE_STATUSES = ("removed", "dismissed")

# `recommendations.user_action` values that record a user rejection.
NEGATIVE_REC_ACTIONS = ("dismiss", "dismissed", "remove", "removed")


def rating_valence(rating: int) -> float:
    """Star rating → valence: 1★ = -1, 3★ = 0, 5★ = +1."""
    return (rating - RATING_NEUTRAL) / RATING_HALF_RANGE


def score_valence(score: float) -> float:
    """Internal 0–100 score → engine valence (half user authority)."""
    raw = (float(score) - SCORE_NEUTRAL) / SCORE_HALF_RANGE
    return max(-1.0, min(1.0, raw)) * ENGINE_AUTHORITY


def paper_valence(
    *,
    status: str,
    rating: int,
    n_negative_actions: int,
    rec_score: float | None,
) -> float | None:
    """Resolve a paper's signals to one valence, strongest-user-first.

    Returns None when the paper carries no signal at all — the caller
    must EXCLUDE it from the field rather than treat it as neutral.
    """
    if status in NEGATIVE_STATUSES:
        return VALENCE_REMOVED
    if rating > 0:
        return rating_valence(rating)
    if n_negative_actions > 0:
        return VALENCE_NEGATIVE_ACTION
    if status == "library":
        return VALENCE_LIBRARY
    if rec_score is not None:
        return score_valence(rec_score)
    return None
