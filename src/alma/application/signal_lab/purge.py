"""Purge — the one destructive Signal Lab control (D20).

Total and immediate: delete every round AND the derived model row in ONE
write unit, so the lab's effect on ranking vanishes atomically with its
evidence. The invalidate half is not optional — scoring reads the model via
``get_stored``, which never checks the fingerprint (task 50 ownership
split), so a purge that only deleted rounds would keep serving the stale
model until an unrelated rebuild landed (defect D-2, task 54 §1.2).

Purge and activation are deliberately separate. Disabling Signal Lab is a
reversible consumption gate: Home, ranking, and maps ignore retained evidence.
Purge is the explicit irreversible action that deletes it.
"""

from __future__ import annotations

import logging
import sqlite3

from alma.application import materialized_views as mv
from alma.application.signal_lab.fit import MODEL_VIEW_KEY
from alma.core.db_write import run_after_gate_release, run_write_unit

logger = logging.getLogger(__name__)


def purge(db: sqlite3.Connection) -> dict[str, int]:
    """Delete all lab evidence + the derived model. Returns counts for the UI.

    The caller (Settings route) owns the Activity row — recorded through the
    request's OWN gated connection (`persist_operation_status`), never the
    scheduler's. The follow-up rebuild (to an honest empty model) is enqueued
    strictly after the write lock is released.
    """

    def _unit() -> dict[str, int]:
        n = int(db.execute("SELECT COUNT(*) FROM signal_lab_rounds").fetchone()[0] or 0)
        db.execute("DELETE FROM signal_lab_rounds")
        mv.invalidate(db, MODEL_VIEW_KEY)
        return {"rounds_deleted": n}

    result = run_write_unit(db, _unit, label="signal_lab.purge")

    def _enqueue() -> None:
        mv.enqueue_rebuild(MODEL_VIEW_KEY)

    run_after_gate_release(_enqueue, conn=db, label="signal_lab purge refit")
    logger.info("signal_lab purge: %s rounds deleted", result["rounds_deleted"])
    return result
