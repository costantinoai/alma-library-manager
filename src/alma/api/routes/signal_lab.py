"""Signal Lab routes — ONE family, generic over ``game_id`` (task 54).

* ``GET  /signal-lab/games``                — the explicit roster.
* ``GET  /signal-lab/{game}/round``         — one round. ZERO writes: the
  round is client state until answered; the response carries a signed
  token binding (game, shown, region context) so the answer POST cannot be
  forged into a different triplet.
* ``POST /signal-lab/{game}/round/answer``  — one INSERT via the layer.
* ``GET  /signal-lab/model``                — fitted heads + holdout
  metrics (pure ``get_stored`` read — a GET never fits).
* ``POST /signal-lab/purge``                — the one destructive control
  (D20): delete every round + the derived model, log Activity, refit empty.

Never a route per game.
"""

from __future__ import annotations

import base64
import hashlib
import hmac
import json
import logging
import secrets
import sqlite3
import uuid

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel

from alma.api.deps import get_db
from alma.application import materialized_views as mv
from alma.application import signal_lab as lab
from alma.application.signal_lab import eval as lab_eval  # noqa: F401 — registers the eval view
from alma.application.signal_lab import policy as lab_policy
from alma.application.signal_lab import purge as lab_purge
from alma.application.signal_lab import rounds as lab_rounds
from alma.application.signal_lab.fit import MODEL_VIEW_KEY
from alma.core.db_write import run_write_unit
from alma.core.time import utcnow

logger = logging.getLogger(__name__)

router = APIRouter()

# Per-process token secret. A round is client state (never persisted on GET),
# so the token only needs to survive the round-trip within one backend
# lifetime; after a restart the client simply fetches a fresh round.
_TOKEN_SECRET = secrets.token_bytes(32)


def _sign_round(claims: dict) -> str:
    body = json.dumps(claims, sort_keys=True, separators=(",", ":")).encode()
    mac = hmac.new(_TOKEN_SECRET, body, hashlib.sha256).digest()[:16]
    return base64.urlsafe_b64encode(body + b"." + mac).decode("ascii")


def _verify_round(token: str) -> dict:
    try:
        raw = base64.urlsafe_b64decode(token.encode("ascii"))
        body, mac = raw.rsplit(b".", 1)
        expected = hmac.new(_TOKEN_SECRET, body, hashlib.sha256).digest()[:16]
        if not hmac.compare_digest(mac, expected):
            raise ValueError("bad signature")
        return json.loads(body)
    except (ValueError, TypeError) as exc:
        raise HTTPException(
            status_code=409,
            detail="Round token invalid or from a previous backend run — fetch a new round.",
        ) from exc


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


@router.get("/{game_id}/round")
def get_round(game_id: str, db: sqlite3.Connection = Depends(get_db)) -> dict:
    """One round for ``game_id``. Zero writes — durable only when answered.

    ``available: False`` (with a reason) when the substrate isn't ready or
    no region has a judgeable pool; never an error. The paper payload is the
    tile-rendering minimum; the token binds everything the answer needs.
    """
    try:
        game = lab.get_game(game_id)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=f"unknown game {game_id!r}") from exc

    spec = lab_policy.build_round(db, k=game.draw.k)
    if spec is None:
        return {
            "available": False,
            "reason": "The corpus map hasn't been built yet, or no region has "
            "enough judgeable papers.",
        }

    placeholders = ",".join("?" for _ in spec["shown"])
    rows = db.execute(
        f"""
        SELECT id, title, authors, year, journal, abstract, tldr
        FROM papers WHERE id IN ({placeholders})
        """,
        spec["shown"],
    ).fetchall()
    by_id = {str(r["id"]): r for r in rows}
    papers = []
    for pid in spec["shown"]:  # preserve the policy's shuffled order
        row = by_id.get(pid)
        if row is None:
            return {"available": False, "reason": "Round papers vanished; retry."}
        papers.append(
            {
                "id": pid,
                "title": row["title"],
                "authors": row["authors"],
                "year": row["year"],
                "journal": row["journal"],
                "summary": (row["tldr"] or row["abstract"] or "")[:400],
            }
        )

    claims = {
        "game": game.id,
        "shown": spec["shown"],
        "region_id": spec["region_id"],
        "pair_region_id": spec["pair_region_id"],
        "region_version": spec["region_version"],
        "ring": spec["ring"],
        "nonce": uuid.uuid4().hex,
    }
    return {
        "available": True,
        "question": game.question,
        "options": list(game.options),
        "papers": papers,
        "token": _sign_round(claims),
    }


class RoundAnswer(BaseModel):
    token: str
    answer: dict | None = None  # None ⇒ "can't tell" skip
    reaction_ms: int | None = None


@router.post("/{game_id}/round/answer")
def answer_round(
    game_id: str, body: RoundAnswer, db: sqlite3.Connection = Depends(get_db)
) -> dict:
    """Persist one answered round — one INSERT through the layer's writer."""
    try:
        lab.get_game(game_id)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=f"unknown game {game_id!r}") from exc

    claims = _verify_round(body.token)
    if claims.get("game") != game_id:
        raise HTTPException(status_code=409, detail="token belongs to a different game")
    shown = [str(s) for s in claims.get("shown") or []]

    answer = body.answer
    skipped = answer is None
    if answer is not None:
        # The answer may only reference shown papers — reject junk early so
        # the fit never has to defend against it.
        for key in ("best", "worst"):
            value = answer.get(key)
            if value is not None and value not in shown:
                raise HTTPException(status_code=422, detail=f"{key!r} not in shown set")

    round_id = lab_rounds.record_answer(
        db,
        game_id=game_id,
        shown=shown,
        answer=answer,
        skipped=skipped,
        region_id=claims.get("region_id"),
        pair_region_id=claims.get("pair_region_id"),
        region_version=claims.get("region_version"),
        ring=claims.get("ring"),
        reaction_ms=body.reaction_ms,
    )
    return {"status": "recorded", "round_id": round_id, "skipped": skipped}


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


@router.get("/eval")
def get_eval(db: sqlite3.Connection = Depends(get_db)) -> dict:
    """Stage-1 promotion evidence: held-out accuracy + hypothetical churn.

    Uses ``mv.get`` (fingerprint-driven): first call builds inline (bounded —
    ≤200 recommendation vectors), later calls serve the cached row and refresh
    in the background when rounds or the live list change.
    """
    from alma.application.signal_lab.eval import EVAL_VIEW_KEY

    return mv.get(db, EVAL_VIEW_KEY)["payload"]


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
