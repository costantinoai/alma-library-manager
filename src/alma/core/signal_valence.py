"""signal_valence — the ONE owner of per-paper preference valence.

Maps a paper's stored signals to a single valence in [-1, +1] for the
space-owned heat field (`/graphs/signal-field`) and any future consumer
that needs "how does the user/engine feel about this paper" as ONE
number. Centralised so the weights are named, greppable, and can never
drift between surfaces (user call 2026-07-25: no hardcoded valence
literals in route code).

Hierarchy: the strongest USER signal wins; engine evidence fills the
rest at reduced authority. Papers with no signal at all return None so
callers can DISTINGUISH "no information" from an explicit neutral; the
field endpoint maps None to `VALENCE_NO_SIGNAL` because every substrate
point must carry a value (no holes in the terrain).
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
"""The neutral star on the fixed 1–5 rating domain.

Also the value `alma.application.library.DEFAULT_LIBRARY_RATING` stamps on
EVERY save, so a stored 3 is a placeholder, not an opinion — see
`rating_is_expressed`."""


def rating_is_expressed(rating: int) -> bool:
    """Has the user actually rated this paper?

    Only a rating that DEVIATES from neutral counts. Saving a paper writes
    `rating = 3` (`DEFAULT_LIBRARY_RATING`), so `rating > 0` is true for the
    entire library and cannot distinguish "saved" from "rated exactly neutral".
    Reading a stored 3 as an expressed opinion made `paper_valence` return a
    hard 0.0 for every saved paper — the `status == 'library'` branch below was
    unreachable, your whole library read as explicit indifference, and both map
    terrains flattened to neutral yellow (user catch 2026-07-26).
    """
    return rating > 0 and rating != RATING_NEUTRAL

RATING_HALF_RANGE = 2.0
"""Stars from neutral to either end (3★→1★ or 3★→5★)."""

# Engine evidence — half the authority of a user signal.
SCORE_NEUTRAL = 50.0
"""The neutral point of the internal 0–100 recommendation score."""

SCORE_HALF_RANGE = 50.0
"""Score distance from neutral to either end."""

ENGINE_AUTHORITY = 0.5
"""How much an engine opinion counts relative to a user signal."""

VALENCE_NO_SIGNAL = 0.0
"""Papers with no signal at all still occupy the space: they contribute
NEUTRAL mass so the field covers every substrate point (user call
2026-07-25 — no empty holes in the terrain). Neutral, not positive:
unworked territory reads yellow, pulling optimistic green honestly
toward "no opinion yet"."""

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

    Returns None when the paper carries no signal at all; the field
    endpoint substitutes `VALENCE_NO_SIGNAL` so the terrain has no
    holes, while other callers can still tell "no info" apart.
    """
    if status in NEGATIVE_STATUSES:
        return VALENCE_REMOVED
    if rating_is_expressed(rating):
        return rating_valence(rating)
    if n_negative_actions > 0:
        return VALENCE_NEGATIVE_ACTION
    if status == "library":
        return VALENCE_LIBRARY
    if rec_score is not None:
        return score_valence(rec_score)
    return None
