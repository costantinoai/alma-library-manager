"""Assign a recommendation to a branch from its complete retrieval evidence.

An ambiguous or lens-wide hit remains unattributed. Such a card can be shown,
but its verdict must not be used to train a branch.
"""

from __future__ import annotations

from collections import defaultdict


def attribute_candidate(candidate: dict, branches: list[dict]) -> None:
    """Choose a dominant active branch without depending on lane merge order."""
    active = {
        str(branch["id"]): branch
        for branch in branches
        if branch.get("is_active", True)
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
    candidate["branch_mode"] = "branch"
    candidate["branch_attribution"] = "retrieval_evidence"
