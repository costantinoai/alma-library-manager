"""Durable identities and stored membership of Discovery branches.

`discovery_branches` is the truth: an active row's `seed_ids` is that branch's
membership, and an ordinary refresh keeps it. A rebuild is the only thing that
replaces a partition, and it comes from one of the reasons in `rebuild_reason`.

Resolution is read-only, so a preview GET cannot mutate state. The completed
refresh persists the plan in its existing gated write unit.

At a rebuild, a strong, unambiguous one-to-one seed overlap carries an old
identity forward with its history. Anything else starts cold (D26), and the
retired row names its successors. A pin, boost or mute is an instruction rather
than learned taste, so it follows the majority of the branch's papers; when no
branch holds that majority the plan reports it as dropped.
"""

from __future__ import annotations

import json
import sqlite3
from typing import Any, NamedTuple

from .lens_crud import _normalize_branch_controls, get_lens, update_lens

_MIN_SEED_OVERLAP = 0.70
_MIN_BEST_MARGIN = 0.10

#: Papers joined plus papers left, over the papers at the last build. At this
#: share the structure describes a different set of papers, so a refresh rebuilds.
TURNOVER_REBUILD = 1.0
#: Below that, the panel recommends a rebuild and leaves the choice to the user.
TURNOVER_RECOMMEND = 0.5
#: Share of members that joined and sit outside the fit of the built members.
MISFIT_RECOMMEND = 0.03
#: The fit floor: this quantile of the built members' similarity to their centroid.
FIT_FLOOR_QUANTILE = 0.10

#: A new branch holding this share of an old branch's papers is its successor.
_SUCCESSOR_SHARE = 0.25
#: A control follows the old branch's papers only into a branch with their majority.
_CONTROL_MAJORITY = 0.5

_CONTROL_KEYS = ("muted", "pinned", "boosted")


class StoredBranch(NamedTuple):
    """One active row: its members, and which of them joined since the build."""

    id: str
    seed_ids: frozenset[str]
    joined: dict[str, str]


def _seed_ids(branch: dict[str, Any]) -> set[str]:
    return {str(value).strip() for value in branch.get("_seed_ids") or [] if str(value).strip()}


def _overlap(left: set[str], right: set[str]) -> float:
    if not left or not right:
        return 0.0
    return len(left & right) / len(left | right)


def load_stored_branches(db: sqlite3.Connection, *, lens_id: str) -> list[StoredBranch]:
    """The lens's active branches, in a fixed order."""
    rows = db.execute(
        "SELECT id, seed_ids, joined_seeds FROM discovery_branches "
        "WHERE lens_id = ? AND state = 'active' ORDER BY id",
        (lens_id,),
    ).fetchall()
    return [
        StoredBranch(
            id=str(row["id"]),
            seed_ids=frozenset(str(value) for value in json.loads(row["seed_ids"])),
            joined=dict(json.loads(row["joined_seeds"])),
        )
        for row in rows
    ]


def rebuild_reason(
    structure: dict[str, Any] | None,
    *,
    resolution: float,
    turnover: float,
    requested: bool,
) -> str | None:
    """Why this plan must recluster, or None to keep the stored membership.

    Two reasons are the user's (a requested rebuild, a changed Cluster detail)
    and one is mechanical: a structure the papers have outgrown. Time is not a
    reason, because a rebuild costs identities and an unchanged Library gains
    nothing from one.
    """
    if structure is None:
        return "initial"
    if requested:
        return "user"
    if abs(float(structure["resolution"]) - resolution) > 1e-3:
        return "resolution"
    if turnover >= TURNOVER_REBUILD:
        return "drift"
    return None


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


def _control_of(branch_id: str, controls: dict[str, Any]) -> str | None:
    return next((key for key in _CONTROL_KEYS if branch_id in (controls.get(key) or [])), None)


def plan_branch_lineage(
    stored: list[StoredBranch],
    branches: list[dict[str, Any]],
    *,
    current_seed_ids: set[str],
    controls: dict[str, Any],
) -> dict[str, Any]:
    """Map a rebuilt partition onto the structure it replaces.

    Pure. `branches` are already through `resolve_branch_identities`, so a
    continued branch has its old id. Every other old branch retires here, with
    a reason, its successors and the fate of its control.
    """
    members = {str(branch["id"]): _seed_ids(branch) for branch in branches}
    retired: list[dict[str, Any]] = []
    leading: dict[str, list[str]] = {}
    top_shares: dict[str, float] = {}

    for old in stored:
        if old.id in members:
            continue
        kept = set(old.seed_ids) & current_seed_ids
        shares = sorted(
            (
                (len(kept & seeds) / len(kept), branch_id)
                for branch_id, seeds in members.items()
                if kept & seeds
            ),
            key=lambda pair: (-pair[0], pair[1]),
        )
        successors = [branch_id for share, branch_id in shares if share >= _SUCCESSOR_SHARE]
        if shares and not successors:
            successors = [shares[0][1]]
        if successors:
            leading.setdefault(successors[0], []).append(old.id)
        top_shares[old.id] = shares[0][0] if shares else 0.0
        retired.append({"id": old.id, "successor_ids": successors})

    carried: dict[str, str] = {}
    dropped: list[dict[str, str]] = []
    for item in retired:
        successors = item["successor_ids"]
        if not successors:
            item["reason"] = "emptied"
        elif len(successors) > 1:
            item["reason"] = "split"
        elif len(leading[successors[0]]) > 1:
            item["reason"] = "merged"
        else:
            item["reason"] = "replaced"
        control = _control_of(item["id"], controls)
        if control is None:
            continue
        if top_shares[item["id"]] >= _CONTROL_MAJORITY:
            carried[item["id"]] = successors[0]
        else:
            dropped.append({"id": item["id"], "control": control})

    return {
        "continued": sorted(set(members) & {old.id for old in stored}),
        "retired": retired,
        "controls_carried": carried,
        "controls_dropped": dropped,
    }


def carry_controls(controls: dict[str, Any], carried: dict[str, str]) -> dict[str, Any]:
    """Rename carried branch ids; the normaliser settles two controls on one branch."""
    if not carried:
        return controls
    renamed = {
        **controls,
        **{
            key: [carried.get(branch_id, branch_id) for branch_id in controls.get(key) or []]
            for key in _CONTROL_KEYS
        },
    }
    return _normalize_branch_controls(renamed)


def branch_structure_report(plan: Any) -> dict[str, Any]:
    """What this plan did to the structure, for Activity and the refresh result."""
    structure = plan.structure
    report = {
        key: structure[key]
        for key in ("reason", "seed_count", "joined", "left", "misfit", "turnover")
    }
    report["branches"] = len(plan.branches)
    lineage = structure["lineage"]
    if structure["reason"] is None:
        report["message"] = (
            f"kept {len(plan.branches)} branches, "
            f"{structure['joined']} joined and {structure['left']} left since the last build"
        )
        return report
    report["lineage"] = lineage
    report["message"] = (
        f"branches rebuilt ({structure['reason']}): {len(plan.branches)} branches, "
        f"{len(lineage['continued'])} continued, {len(lineage['retired'])} retired, "
        f"{len(lineage['controls_carried'])} controls carried, "
        f"{len(lineage['controls_dropped'])} dropped"
    )
    return report


def read_retired_branches(
    db: sqlite3.Connection,
    *,
    lens: dict[str, Any],
) -> list[dict[str, Any]]:
    """Branches that ended since the current structure was built, or by that build."""
    structure = lens.get("branch_structure")
    if not structure:
        return []
    rows = db.execute(
        "SELECT id, label, retired_reason, successor_ids, retired_at FROM discovery_branches "
        "WHERE lens_id = ? AND state = 'retired' AND retired_at >= ? "
        "ORDER BY retired_at DESC, id",
        (lens["id"], structure["built_at"]),
    ).fetchall()
    controls = lens.get("branch_controls") or {}
    return [
        {
            "id": str(row["id"]),
            "label": str(row["label"] or ""),
            "reason": str(row["retired_reason"] or "emptied"),
            "successor_ids": json.loads(row["successor_ids"]),
            "retired_at": str(row["retired_at"]),
            "control_state": _control_of(str(row["id"]), controls),
        }
        for row in rows
    ]


def persist_branch_identities(
    db: sqlite3.Connection,
    *,
    lens_id: str,
    branches: list[dict[str, Any]],
    now: str,
    structure: dict[str, Any],
) -> None:
    """Persist one resolved branch plan inside the refresh's write unit.

    `structure` is the plan's own record. Without a rebuild reason only the
    membership moves: joins, departures and what a removal emptied.
    """
    active_ids: list[str] = []
    for branch in branches:
        branch_id = str(branch["id"])
        active_ids.append(branch_id)
        seed_ids = sorted(_seed_ids(branch))
        joined = json.dumps(branch.get("_joined") or {}, sort_keys=True)
        cursor = db.execute(
            """
            INSERT INTO discovery_branches (
                id, lens_id, seed_ids, joined_seeds, label, state,
                created_at, updated_at, retired_at
            ) VALUES (?, ?, ?, ?, ?, 'active', ?, ?, NULL)
            ON CONFLICT(id) DO UPDATE SET
                seed_ids = excluded.seed_ids,
                joined_seeds = excluded.joined_seeds,
                label = excluded.label,
                state = 'active',
                updated_at = excluded.updated_at,
                retired_at = NULL,
                retired_reason = NULL,
                successor_ids = '[]'
            WHERE discovery_branches.lens_id = excluded.lens_id
            """,
            (
                branch_id, lens_id, json.dumps(seed_ids), joined,
                str(branch.get("label") or ""), now, now,
            ),
        )
        if cursor.rowcount != 1:
            raise ValueError(f"branch id {branch_id} already belongs to another lens")

    lineage = structure.get("lineage") or {}
    for item in lineage.get("retired") or []:
        db.execute(
            "UPDATE discovery_branches SET state = 'retired', retired_at = ?, updated_at = ?, "
            "retired_reason = ?, successor_ids = ? WHERE id = ? AND lens_id = ?",
            (now, now, item["reason"], json.dumps(item["successor_ids"]), item["id"], lens_id),
        )
    # What is still active and absent lost its last paper in an ordinary refresh.
    placeholders = ",".join("?" for _ in active_ids)
    excluded = f"AND id NOT IN ({placeholders})" if active_ids else ""
    db.execute(
        "UPDATE discovery_branches SET state = 'retired', retired_at = ?, updated_at = ?, "
        f"retired_reason = 'emptied' WHERE lens_id = ? AND state = 'active' {excluded}",
        (now, now, lens_id, *active_ids),
    )

    if structure.get("reason"):
        # Read the row now: the user may have changed a control during the refresh.
        lens = get_lens(db, lens_id) or {}
        previous = lens.get("branch_structure") or {}
        update_lens(
            db,
            lens_id,
            branch_controls=carry_controls(
                lens.get("branch_controls") or {},
                lineage.get("controls_carried") or {},
            ),
        )
        db.execute(
            "UPDATE discovery_lenses SET branch_structure = ? WHERE id = ?",
            (
                json.dumps(
                    {
                        "revision": int(previous.get("revision") or 0) + 1,
                        "built_at": now,
                        "trigger": structure["reason"],
                        "resolution": structure["resolution"],
                        "seed_count": structure["seed_count"],
                    }
                ),
                lens_id,
            ),
        )
