"""triplet_odd_one_out — the boundary/metric round (task 54 M2).

Two papers from region r, one from adjacent region s, all low-margin. "Which
one doesn't belong?" is the only question format that identifies the METRIC
rather than the utility (Schultz & Joachims 2003; crowd kernel, Tamuz 2011;
t-STE) — it is what sharpens region boundaries and neighbour suggestions.

Interpretation: the two papers kept together are mutually nearer than the
odd one (two Sim constraints), and each casts a vote for the region it was
kept with. Picking the PLANTED intruder confirms the current boundary (votes
for the status quo); picking a same-region paper as odd is boundary evidence
against it.

D20: pure data + one pure function. No I/O.
"""

from __future__ import annotations

from alma.application.signal_lab.spec import (
    Constraint,
    DrawSpec,
    MiniGame,
    RegionVote,
    RoundRow,
    Sim,
)


def interpret_odd_one_out(rnd: RoundRow) -> list[Constraint]:
    answer = rnd.answer or {}
    odd = str(answer.get("odd") or "")
    if not odd or odd not in rnd.shown or len(rnd.shown) != 3:
        return []
    kept = [p for p in rnd.shown if p != odd]
    out: list[Constraint] = [
        Sim(anchor=kept[0], near=kept[1], far=odd),
        Sim(anchor=kept[1], near=kept[0], far=odd),
    ]
    # Region votes only when the round carries its boundary context.
    if rnd.region_id is not None and rnd.pair_region_id is not None:
        # The kept pair reads as one region; the odd paper as the other side.
        out.extend(RegionVote(paper_id=p, region_id=int(rnd.region_id)) for p in kept)
        out.append(RegionVote(paper_id=odd, region_id=int(rnd.pair_region_id)))
    return out


TRIPLET_ODD_ONE_OUT = MiniGame(
    id="triplet_odd_one_out",
    title="Calibrate: odd one out",
    question="Which paper doesn't belong with the other two?",
    options=("odd", "cant_tell"),
    draw=DrawSpec(region_mode="boundary", k=3),
    interpret=interpret_odd_one_out,
)
