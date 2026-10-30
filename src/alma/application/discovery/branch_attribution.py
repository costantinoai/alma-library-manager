"""Assign a recommendation to a branch from its complete retrieval evidence.

An ambiguous or lens-wide hit remains unattributed. Such a card can be shown,
but its verdict must not be used to train a branch.
"""

from __future__ import annotations

from collections import defaultdict


def branch_can_retrieve(branch: dict) -> bool:
    """True for a branch that still searches: active, or auto-muted and probing.

    A probing branch keeps a small budget, because a branch that gets no cards
    gets no verdicts and could never recover. A branch the user muted never
    probes.
    """
    return bool(branch.get("is_active", True) or branch.get("is_probing"))


def _coverage_state(branch: dict) -> str:
    if branch.get("is_probing"):
        return "probing"
    return "active" if branch.get("is_active", True) else "muted"


def _coverage_reason(state: str, row: dict) -> str | None:
    """Why a branch has no card: the first stage that lost it."""
    if row["cards"]:
        return None
    if state == "muted":
        return "muted"
    if not row["retrieved"]:
        return "no_retrieval_hit"
    if not row["attributed"]:
        return "only_shared_hits"
    if not row["eligible"]:
        return "all_ineligible"
    return "no_slot"


def branch_coverage(
    branches: list[dict],
    *,
    pool: list[dict],
    eligible: list[dict],
    slate: list[dict],
) -> list[dict]:
    """How far each branch got: retrieved, attributed, eligible, on the deck.

    A branch with no card is a fact about this refresh, so it is stated with the
    stage that lost it rather than left for the user to notice.
    """
    rows: list[dict] = []
    for branch in branches:
        branch_id = str(branch.get("id") or "")
        state = _coverage_state(branch)
        cards = [card for card in slate if card.get("branch_id") == branch_id]
        row = {
            "branch_id": branch_id,
            "label": branch.get("label"),
            "state": state,
            "seed_count": int(branch.get("seed_count") or 0),
            "retrieved": sum(1 for item in pool if branch_id in (item.get("branch_evidence") or {})),
            "attributed": sum(1 for item in pool if item.get("branch_id") == branch_id),
            "eligible": sum(1 for item in eligible if item.get("branch_id") == branch_id),
            "cards": len(cards),
            "floor_cards": sum(
                1 for card in cards if (card.get("_selection") or {}).get("branch_floor")
            ),
        }
        row["reason"] = _coverage_reason(state, row)
        rows.append(row)
    return rows


def _strongest_lane_mode(candidate: dict, branch_id: str) -> str:
    """Return the core/explore mode of the branch's best-ranked hit that has one."""
    hits = [
        hit for hit in candidate.get("retrieval_hits") or []
        if str(hit.get("branch_id") or "") == branch_id and hit.get("branch_mode")
    ]
    if not hits:
        return "branch"
    return str(min(hits, key=lambda hit: int(hit.get("rank") or 1))["branch_mode"])


def attribute_candidate(candidate: dict, branches: list[dict]) -> None:
    """Choose a dominant active branch without depending on lane merge order.

    The branch context travels as one unit. Lanes stamp `branch_id`, topics and
    `matched_query` together, so whatever replaces the id must settle the rest:
    otherwise a card explains itself with the branch that lost.
    """
    stamped_branch_id = str(candidate.get("branch_id") or "")
    active = {
        str(branch["id"]): branch
        for branch in branches
        if branch_can_retrieve(branch)
    }
    evidence: dict[str, list[float]] = defaultdict(list)
    for hit in candidate.get("retrieval_hits") or []:
        branch_id = str(hit.get("branch_id") or "")
        if branch_id in active:
            evidence[branch_id].append(1.0 / (60.0 + max(1, int(hit.get("rank") or 1))))
    scores = {
        branch_id: sum(sorted(weights, reverse=True)[:4])
        for branch_id, weights in evidence.items()
    }
    candidate["branch_evidence"] = {
        branch_id: round(weight, 8) for branch_id, weight in sorted(scores.items())
    }
    candidate["branch_id"] = None
    candidate["branch_label"] = None
    candidate["branch_mode"] = None
    candidate["branch_core_topics"] = []
    candidate["branch_explore_topics"] = []
    matched_query = candidate.pop("matched_query", None)
    if not scores:
        candidate["branch_attribution"] = "lens_wide"
        return
    ranked = sorted(scores.items(), key=lambda item: (-item[1], item[0]))
    winner, weight = ranked[0]
    # A pair of branches with indistinguishable support is an honest overlap,
    # not a random consequence of which retrieval lane returned first.
    if len(ranked) > 1 and weight < 1.5 * ranked[1][1]:
        candidate["branch_attribution"] = "ambiguous"
        return
    branch = active[winner]
    candidate["branch_id"] = winner
    candidate["branch_label"] = branch.get("label")
    candidate["branch_mode"] = _strongest_lane_mode(candidate, winner)
    candidate["branch_core_topics"] = list(branch.get("core_topics") or [])
    candidate["branch_explore_topics"] = list(branch.get("explore_topics") or [])
    if matched_query and stamped_branch_id == winner:
        candidate["matched_query"] = matched_query
    candidate["branch_attribution"] = "retrieval_evidence"
