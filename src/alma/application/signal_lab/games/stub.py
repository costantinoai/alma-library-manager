"""The best-worst interpreter — M0's simulator stub, M1's real game.

Best + worst on three items fully determines the ranking, so one round
yields all three ordered pairs from ≤2 clicks (best-worst scaling,
Louviere; Kiritchenko & Mohammad 2017). NOT registered in
``available_games()`` during M0 — the simulator drives it directly; M1
promotes it to ``triplet_best_worst`` with real copy.

D20: this module is pure — data plus ``interpret``. No I/O.
"""

from __future__ import annotations

from alma.application.signal_lab.spec import (
    Constraint,
    DrawSpec,
    MiniGame,
    Pref,
    RoundRow,
)


def interpret_best_worst(rnd: RoundRow) -> list[Constraint]:
    """best ≻ everything, everything ≻ worst. Skip / malformed ⇒ nothing.

    A partial answer (best only, or best == worst) interprets to zero
    constraints rather than a weak one: the absence of a verdict is not a
    verdict (same principle as D6's ``defer``).
    """
    answer = rnd.answer or {}
    best = str(answer.get("best") or "")
    worst = str(answer.get("worst") or "")
    if not best or not worst or best == worst:
        return []
    if best not in rnd.shown or worst not in rnd.shown:
        return []
    mid = [p for p in rnd.shown if p not in (best, worst)]
    prefs: list[Constraint] = [Pref(best, worst)]
    prefs.extend(Pref(best, m) for m in mid)
    prefs.extend(Pref(m, worst) for m in mid)
    return prefs


BEST_WORST_SIM_GAME = MiniGame(
    id="best_worst_sim",
    title="Best / worst (simulator stub)",
    question="Which would you read first — and which would you skip?",
    options=("best", "worst", "cant_tell"),
    draw=DrawSpec(region_mode="within", k=3),
    interpret=interpret_best_worst,
)
