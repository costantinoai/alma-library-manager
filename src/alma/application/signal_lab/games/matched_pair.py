"""matched_pair — two papers, one difference, one clean bit of evidence.

Why this game exists
--------------------

The triplet games ask "which of these would you read first". The papers differ
on every axis at once — topic, venue, authors, method, era, writing — so the
single bit of preference they produce cannot be attributed to any one attribute
without confounding it with the others. That is why they fit only three things:
region, a global utility direction in embedding space, and (from within-region
rounds, where topic is roughly held constant) authors.

A matched pair is drawn so the two papers **agree on region and differ on
exactly one attribute**. The same single click then carries clean evidence
about that attribute, because nothing else varied. It is the difference between
"I preferred this paper" and "at equal topic, I preferred this venue".

Why venue first
---------------

SPECTER2 does not encode the journal, so the learned utility direction cannot
represent venue preference at all. `journal_affinity` is computed from Library
prevalence — it knows which venues you *save from*, never which you would
*choose between* at equal topic. That is genuinely missing information, and a
matched pair is the cheapest instrument that can collect it.

Deliberately NOT topics: SPECTER2 space is largely topical, so the utility
direction already encodes topic preference, and `topic_score` estimates the
same quantity from hundreds of Library decisions. A third estimator of one
quantity is how a model becomes collinear with itself — the trap
`usefulness_boost` fell into before it was demoted to diagnostic.

D20: pure data + one pure function. No I/O.

Note the interpreter returns a plain :class:`Pref`, exactly like the triplet
games. It has paper ids and nothing else, so it *cannot* know the venues — and
should not. Attribution to the contrast axis happens at FIT time, where the
paper→venue map is available, keyed on the game's ``draw.contrast``. That keeps
interpretation retroactively fixable (task 54 §0) and keeps games I/O-free.
"""

from __future__ import annotations

from alma.application.signal_lab.spec import (
    Constraint,
    DrawSpec,
    MiniGame,
    Pref,
    RoundRow,
)


def interpret_matched_pair(rnd: RoundRow) -> list[Constraint]:
    """The picked paper beats the other one. Skip / malformed ⇒ nothing.

    A pair with no pick, or a pick that is not one of the two shown papers, is
    the absence of a verdict rather than a weak one — the same rule the triplet
    games follow, and D6's `defer`.
    """
    answer = rnd.answer or {}
    picked = str(answer.get("picked") or "")
    if not picked or picked not in rnd.shown:
        return []
    others = [p for p in rnd.shown if p != picked]
    if len(others) != 1:
        return []
    return [Pref(picked, others[0])]


MATCHED_PAIR_VENUE = MiniGame(
    id="matched_pair_venue",
    title="Signal Lab: same field, different venue",
    question="Same area of work — which would you rather read?",
    options=("picked", "cant_tell"),
    draw=DrawSpec(region_mode="within", k=2, contrast="venue"),
    interpret=interpret_matched_pair,
)
