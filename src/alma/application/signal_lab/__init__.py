"""Signal Lab — the reversible minigame layer (task 54, D20).

The layer's contract in one paragraph: games are pure data
(:mod:`.spec.MiniGame`); the layer writes exactly one row per answered round
to ``signal_lab_rounds`` (:mod:`.rounds`); the fitted model is the
``signal_lab:model`` materialized view — a pure function of the rounds,
recomputed wholesale (:mod:`.fit`); ranking reads it as additive
``score_candidate`` terms behind ``weights.lab_*`` (default ``"0.0"``); and
purging the lab (:mod:`.purge`) is DELETE + invalidate, total and immediate.

``available_games()`` is the EXPLICIT registry — this list is what can reach
your signal, the same discipline as
``services.inbox_channels.available_channels()``.
"""

from __future__ import annotations

from alma.application.signal_lab.spec import MiniGame


def available_games() -> list[MiniGame]:
    """Every registered minigame, in display order.

    Explicit by design: a game not listed here cannot draw rounds, cannot be
    answered, and its historical rounds are skipped (with a counted warning)
    at fit time.
    """
    from alma.application.signal_lab.games.triplet_best_worst import (
        TRIPLET_BEST_WORST,
    )

    return [TRIPLET_BEST_WORST]


def get_game(game_id: str) -> MiniGame:
    """Resolve a game id or raise ``KeyError`` — never a silent fallback."""
    for game in available_games():
        if game.id == game_id:
            return game
    raise KeyError(f"unknown signal-lab game: {game_id!r}")
