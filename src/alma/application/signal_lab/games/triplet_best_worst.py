"""triplet_best_worst — the default calibration round (task 54 M1).

Best + worst on three papers fully determines the ranking: one screen,
≤2 clicks, three ordered pairs (best-worst scaling, Louviere; more reliable
per annotation than rating scales — Kiritchenko & Mohammad 2017; K-wise
beats pairwise on sample efficiency — Zhu et al. 2023).

D20: pure data + one pure function. No I/O.
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


TRIPLET_BEST_WORST = MiniGame(
    id="triplet_best_worst",
    title="Calibrate: pick your read",
    question="Which would you read first — and which would you skip?",
    options=("best", "worst", "cant_tell"),
    draw=DrawSpec(region_mode="within", k=3),
    interpret=interpret_best_worst,
)
