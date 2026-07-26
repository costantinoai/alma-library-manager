"""Signal Lab routes — ONE family, generic over ``game_id`` (task 54).

M0 ships the layer's control surface only:

* ``GET  /signal-lab/games``  — the explicit roster + availability.
* ``GET  /signal-lab/model``  — the fitted heads + holdout metrics
  (pure ``get_stored`` read — a GET never fits).
* ``POST /signal-lab/purge``  — the one destructive control (D20):
  delete every round + the derived model, log Activity, refit empty.

M1 adds ``GET /{game}/round`` + ``POST /{game}/round/answer``. Never a
route per game.
"""

from __future__ import annotations

import logging
import sqlite3
import uuid

from fastapi import APIRouter, Depends

from alma.api.deps import get_db
from alma.application import materialized_views as mv
from alma.application import signal_lab as lab
from alma.application.signal_lab import purge as lab_purge
from alma.application.signal_lab.fit import MODEL_VIEW_KEY
from alma.core.db_write import run_write_unit
from alma.core.time import utcnow

logger = logging.getLogger(__name__)

router = APIRouter()


@router.get("/games")
def list_games(db: sqlite3.Connection = Depends(get_db)) -> dict:
    """The explicit game roster. What is not listed cannot reach your signal."""
    rounds = 0
    try:
        rounds = int(
            db.execute("SELECT COUNT(*) FROM signal_lab_rounds").fetchone()[0] or 0
        )
    except sqlite3.OperationalError:
        pass
    return {
        "games": [
            {
                "id": g.id,
                "title": g.title,
                "question": g.question,
                "options": list(g.options),
            }
            for g in lab.available_games()
        ],
        "rounds_recorded": rounds,
    }


@router.get("/model")
def get_model(db: sqlite3.Connection = Depends(get_db)) -> dict:
    """The fitted model summary — heads, counts, holdout metrics.

    Pure stored read (task 50 ownership split): never fits inline. ``ready``
    is False until the first background fit lands.
    """
    stored = mv.get_stored(db, MODEL_VIEW_KEY)
    if stored is None:
        return {"ready": False}
    payload = stored["payload"]
    return {
        "ready": True,
        "computed_at": stored.get("computed_at"),
        "counts": payload.get("counts", {}),
        "gamma": payload.get("gamma"),
        "holdout": payload.get("holdout", {}),
        "region_offsets": payload.get("region_offsets", {}),
        "overrides": len(payload.get("region_overrides") or {}),
    }


@router.post("/purge")
def purge_signal_lab(db: sqlite3.Connection = Depends(get_db)) -> dict:
    """Delete every lab round and everything derived from them (D20).

    Library, ratings, and the always-on feedback history are untouched.
    Irreversible by design — the confirm lives in the Settings UI.
    """
    result = lab_purge.purge(db)

    # Activity row through the request's OWN gated connection — never the
    # scheduler's second connection (lessons: "Activity/status rows for
    # foreground actions go through the GATED connection").
    try:
        from alma.core.operations.activity import persist_operation_status
        from alma.core.operations.models import OperationContext

        now = utcnow().isoformat()
        jid = f"signal_lab_purge_{uuid.uuid4().hex[:10]}"
        ctx = OperationContext(
            operation_key="signal_lab.purge",
            trigger_source="user",
            actor="api_user",
            correlation_id=jid,
            operation_id=jid,
            started_at=now,
            finished_at=now,
            status="completed",
            message=(
                f"Signal Lab purged — {result['rounds_deleted']} round(s) deleted"
            ),
            result=result,
        )
        run_write_unit(
            db,
            lambda: persist_operation_status(db, ctx),
            label="signal_lab.purge.activity",
        )
    except Exception:  # noqa: BLE001 — best-effort logging, never fail the purge
        logger.debug("signal lab purge activity log skipped", exc_info=True)

    return {"status": "purged", **result}
