"""Round lifecycle — the ONLY writer of ``signal_lab_rounds`` (D20).

A round is client state until answered: the GET that shows three papers
writes nothing, and the answer POST lands exactly one row here, inside one
``run_write_unit``. No network in the transaction, no fitting inline — the
refit is a debounced background rebuild of the ``signal_lab:model`` view,
enqueued strictly after the write lock is released
(``run_after_gate_release``, the 2026-07-26 lesson).
"""

from __future__ import annotations

import hashlib
import json
import logging
import sqlite3
from typing import Any

from alma.ai.graph_versions import SIGNAL_LAB_POLICY_VERSION
from alma.application.signal_lab.fit import MODEL_VIEW_KEY
from alma.application.signal_lab.spec import RoundRow
from alma.core.db_write import run_after_gate_release, run_write_unit

logger = logging.getLogger(__name__)

# Holdout share: rounds reserved for evaluation, never trained on. Stamped at
# CREATE time from the round's content hash — deterministic, so refit order
# can never move a round across the train/holdout line (task 54 §6 stage 1).
HOLDOUT_PERCENT = 15

# Refit debounce: enqueue a model rebuild every Nth answered round (plus page
# open + purge). `operation_key` dedup collapses bursts beyond this.
REFIT_EVERY_N_ROUNDS = 5


def _holdout_stamp(game_id: str, shown: list[str], percent: int = HOLDOUT_PERCENT) -> bool:
    digest = hashlib.sha1(f"{game_id}|{'|'.join(shown)}".encode()).hexdigest()
    return int(digest[:8], 16) % 100 < percent


def record_answer(
    db: sqlite3.Connection,
    *,
    game_id: str,
    shown: list[str],
    answer: dict[str, Any] | None,
    skipped: bool,
    region_id: int | None,
    pair_region_id: int | None,
    region_version: int | None,
    ring: int | None,
    reaction_ms: int | None,
) -> int:
    """Persist one answered round. One write unit, one INSERT, nothing else.

    A skip is stored too — it is evidence about judgeability (fatigue and
    unjudgeable-pool diagnostics read it), it just interprets to zero
    constraints at fit time.
    """
    if not shown:
        raise ValueError("a round must show at least one paper")

    from alma.application.signal_lab import lab_tuning

    tuning = lab_tuning(db)

    def _unit() -> int:
        cur = db.execute(
            """
            INSERT INTO signal_lab_rounds
                (game_id, region_id, pair_region_id, region_version, ring,
                 policy_version, shown_json, answer_json, skipped,
                 reaction_ms, holdout)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                game_id,
                region_id,
                pair_region_id,
                region_version,
                ring,
                SIGNAL_LAB_POLICY_VERSION,
                json.dumps(shown),
                json.dumps(answer) if answer is not None else None,
                1 if skipped else 0,
                reaction_ms,
                1 if _holdout_stamp(game_id, shown, tuning["holdout_percent"]) else 0,
            ),
        )
        return int(cur.lastrowid or 0)

    round_id = run_write_unit(db, _unit, label="signal_lab.answer")
    _maybe_enqueue_refit(db, every=tuning["refit_every_rounds"])
    return round_id


def _maybe_enqueue_refit(db: sqlite3.Connection, *, every: int = REFIT_EVERY_N_ROUNDS) -> None:
    """Debounced model refit — deferred past this thread's write lock.

    ``enqueue_rebuild`` persists job state on the scheduler's own connection;
    firing it while this thread still holds the SQLite write lock busy-waits
    the whole timeout and then drops the row, so the enqueue is ALWAYS routed
    through ``run_after_gate_release(..., conn=db)``.
    """
    try:
        n = int(
            db.execute(
                "SELECT COUNT(*) FROM signal_lab_rounds WHERE answer_json IS NOT NULL"
            ).fetchone()[0]
            or 0
        )
    except sqlite3.OperationalError:
        return
    if n == 0 or n % max(1, every) != 0:
        return

    def _enqueue() -> None:
        from alma.application import materialized_views as mv

        mv.enqueue_rebuild(MODEL_VIEW_KEY)

    run_after_gate_release(_enqueue, conn=db, label="signal_lab refit")


def load_rounds(conn: sqlite3.Connection) -> list[RoundRow]:
    """Every answered-or-skipped round, oldest first, parsed for the fitter."""
    try:
        rows = conn.execute(
            """
            SELECT id, game_id, region_id, pair_region_id, region_version,
                   ring, policy_version, shown_json, answer_json, skipped, holdout
            FROM signal_lab_rounds
            ORDER BY id
            """
        ).fetchall()
    except sqlite3.OperationalError:
        return []
    out: list[RoundRow] = []
    for row in rows:
        try:
            shown = list(json.loads(row["shown_json"] or "[]"))
            answer = json.loads(row["answer_json"]) if row["answer_json"] else None
        except (TypeError, ValueError):
            logger.warning("signal_lab: undecodable round %s skipped", row["id"])
            continue
        out.append(
            RoundRow(
                id=int(row["id"]),
                game_id=str(row["game_id"]),
                region_id=row["region_id"],
                pair_region_id=row["pair_region_id"],
                region_version=row["region_version"],
                ring=row["ring"],
                policy_version=int(row["policy_version"]),
                shown=[str(s) for s in shown],
                answer=answer if isinstance(answer, dict) else None,
                skipped=bool(row["skipped"]),
                holdout=bool(row["holdout"]),
            )
        )
    return out
