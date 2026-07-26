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
    """The lab's tunable numbers, resolved settings-over-defaults. ONE parser.

    Every knob lives in ``DISCOVERY_SETTINGS_DEFAULTS`` under ``signal_lab.*``
    (task 54): right defaults, tunable from Settings without a code change.
    """
    from alma.application.discovery.lens_crud import read_settings

    s = read_settings(conn)

    def _f(key: str, fallback: float) -> float:
        try:
            return float(s.get(key, fallback))
        except (TypeError, ValueError):
            return fallback

    return {
        "gamma_start": _f("signal_lab.gamma_start", 0.35),
        "epsilon": _f("signal_lab.epsilon", 0.20),
        "coverage_target": max(1, int(_f("signal_lab.coverage_target", 20))),
        "refit_every_rounds": max(1, int(_f("signal_lab.refit_every_rounds", 5))),
        "holdout_percent": min(50, max(0, int(_f("signal_lab.holdout_percent", 15)))),
        "override_min_votes": max(1, int(_f("signal_lab.override_min_votes", 3))),
    }
