"""Durable identities for the seed clusters shown as Discovery branches.

Resolution is read-only, so a preview GET cannot mutate state. The completed
refresh persists the resolved identities in its existing gated write unit.
Only a strong, unambiguous one-to-one seed overlap carries an old identity and
its controls forward; a split or merge with uncertain lineage starts cold.
"""

from __future__ import annotations

import json
import sqlite3
from typing import Any

_MIN_SEED_OVERLAP = 0.70
_MIN_BEST_MARGIN = 0.10


def _seed_ids(branch: dict[str, Any]) -> set[str]:
    return {str(value).strip() for value in branch.get("_seed_ids") or [] if str(value).strip()}


def _overlap(left: set[str], right: set[str]) -> float:
    if not left or not right:
        return 0.0
    return len(left & right) / len(left | right)


def resolve_branch_identities(
    db: sqlite3.Connection,
    *,
    lens_id: str,
    branches: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    """Reuse a prior branch ID only for mutual, clearly dominant seed overlap.

    New clusters keep their deterministic provisional ID from the seed builder
    until the refresh writes it. Never use recommendation paper IDs as seeds.
    """
    rows = db.execute(
        "SELECT id, seed_ids, state FROM discovery_branches WHERE lens_id = ?",
        (lens_id,),
    ).fetchall()
    prior = [
        (str(row["id"]), set(json.loads(row["seed_ids"])))
        for row in rows if row["state"] == "active"
    ]
    used_ids = {str(row["id"]) for row in rows}
    current = [_seed_ids(branch) for branch in branches]
    scores = [
        [_overlap(seed_ids, old_seed_ids) for _, old_seed_ids in prior]
        for seed_ids in current
    ]
    resolved: list[dict[str, Any]] = []
    for index, branch in enumerate(branches):
        item = dict(branch)
        ranked = sorted(enumerate(scores[index]), key=lambda pair: (-pair[1], prior[pair[0]][0]))
        best_index, best_score = ranked[0] if ranked else (-1, 0.0)
        runner_up = ranked[1][1] if len(ranked) > 1 else 0.0
        matched = best_score >= _MIN_SEED_OVERLAP and best_score - runner_up >= _MIN_BEST_MARGIN
        if matched:
            competing = sorted(
                (scores[other][best_index] for other in range(len(branches)) if other != index),
                reverse=True,
            )
            if competing and best_score - competing[0] < _MIN_BEST_MARGIN:
                matched = False
        if matched:
            item["id"] = prior[best_index][0]
            item["identity_state"] = "continued"
            item["identity_overlap"] = round(best_score, 3)
        else:
            # A retired identity must never silently regain old controls or
            # taste when its old seed signature happens to reappear later.
            provisional = str(item["id"])
            if provisional in used_ids:
                generation = 2
                while f"{provisional}-g{generation}" in used_ids:
                    generation += 1
                item["id"] = f"{provisional}-g{generation}"
            item["identity_state"] = "new"
            item["identity_overlap"] = None
        used_ids.add(str(item["id"]))
        resolved.append(item)
    return resolved


def persist_branch_identities(
    db: sqlite3.Connection,
    *,
    lens_id: str,
    branches: list[dict[str, Any]],
    now: str,
) -> None:
    """Persist one resolved branch snapshot inside the refresh's write unit."""
    active_ids: list[str] = []
    for branch in branches:
        branch_id = str(branch["id"])
        active_ids.append(branch_id)
        seed_ids = sorted(_seed_ids(branch))
        cursor = db.execute(
            """
            INSERT INTO discovery_branches (
                id, lens_id, seed_ids, label, state, created_at, updated_at, retired_at
            ) VALUES (?, ?, ?, ?, 'active', ?, ?, NULL)
            ON CONFLICT(id) DO UPDATE SET
                seed_ids = excluded.seed_ids,
                label = excluded.label,
                state = 'active',
                updated_at = excluded.updated_at,
                retired_at = NULL
            WHERE discovery_branches.lens_id = excluded.lens_id
            """,
            (branch_id, lens_id, json.dumps(seed_ids), str(branch.get("label") or ""), now, now),
        )
        if cursor.rowcount != 1:
            raise ValueError(f"branch id {branch_id} already belongs to another lens")
    if active_ids:
        placeholders = ",".join("?" for _ in active_ids)
        db.execute(
            "UPDATE discovery_branches SET state = 'retired', retired_at = ?, updated_at = ? "
            f"WHERE lens_id = ? AND state = 'active' AND id NOT IN ({placeholders})",
            (now, now, lens_id, *active_ids),
        )
    else:
        db.execute(
            "UPDATE discovery_branches SET state = 'retired', retired_at = ?, updated_at = ? "
            "WHERE lens_id = ? AND state = 'active'",
            (now, now, lens_id),
        )
