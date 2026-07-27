"""Signal Lab — first-class reversible calibration (task 54, D20).

The layer's contract in one paragraph: games are pure data
(:mod:`.spec.MiniGame`); the layer writes exactly one row per answered round
to ``signal_lab_rounds`` (:mod:`.rounds`); the fitted model is the
``signal_lab:model`` materialized view — a pure function of the rounds,
recomputed wholesale (:mod:`.fit`); ranking reads it as additive
``score_candidate`` terms behind ``weights.lab_*`` (default ``"0.0"``); and
purging the lab (:mod:`.purge`) is DELETE + invalidate, total and immediate.

``available_games()`` is the EXPLICIT registry — this list is what can reach
your signal, the same discipline as
the external-integration registry.
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
    from alma.application.signal_lab.games.triplet_odd_one_out import (
        TRIPLET_ODD_ONE_OUT,
    )

    return [TRIPLET_BEST_WORST, TRIPLET_ODD_ONE_OUT]


def get_game(game_id: str) -> MiniGame:
    """Resolve a game id or raise ``KeyError`` — never a silent fallback."""
    for game in available_games():
        if game.id == game_id:
            return game
    raise KeyError(f"unknown signal-lab game: {game_id!r}")


def lab_tuning(conn) -> dict:
    """Sampler/fitter tuning from the feature's one validated parser."""
    from alma.application.signal_lab.settings import read

    settings = read(conn)
    return {
        "gamma_start": settings.ring_decay,
        "epsilon": settings.exploration_rate,
        "coverage_target": settings.coverage_target,
        "refit_every_rounds": settings.refit_every_rounds,
        "holdout_percent": settings.holdout_percent,
        "override_min_votes": settings.override_min_votes,
    }
