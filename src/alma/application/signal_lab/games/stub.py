"""Simulator alias of the best-worst game (task 54 stage 0).

The simulator drives the SAME interpreter production uses — that is the
point of stage 0 — under a distinct id so simulated rounds can never be
confused with real ones. Not registered in ``available_games()``.
"""

from __future__ import annotations

from dataclasses import replace

from alma.application.signal_lab.games.triplet_best_worst import TRIPLET_BEST_WORST

BEST_WORST_SIM_GAME = replace(
    TRIPLET_BEST_WORST,
    id="best_worst_sim",
    title="Best / worst (simulator stub)",
)
