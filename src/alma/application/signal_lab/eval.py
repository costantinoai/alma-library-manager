"""Stage-1 counterfactual eval — the promotion evidence (task 54 §6).

The ``insights:signal_lab_eval`` view answers two questions with the lab's
weights still at 0.0:

* **Model number** — held-out pairwise accuracy per nested head (already
  computed by the fit; re-served here alongside the outcome number).
* **Outcome number** — top-20 churn of the LIVE Discovery list under a
  hypothetical promotion (``HYPOTHETICAL_POINTS`` per head): how many of the
  current top papers would change, and the mean rank displacement. Accuracy
  says the model learned something; churn says whether shipping it would
  visibly matter.

Heads get promoted by raising ``weights.lab_*`` in Settings, one at a time,
only when both numbers argue for it (D20 stage gates).
"""

from __future__ import annotations

import sqlite3
from typing import Any

import numpy as np

from alma.ai.graph_versions import SIGNAL_LAB_FIT_VERSION, with_version
from alma.application import materialized_views as mv

EVAL_VIEW_KEY = "insights:signal_lab_eval"

# The what-if weight (score points) applied per head for the churn probe.
HYPOTHETICAL_POINTS = 2.5

# How many live recommendations the churn probe rescoreS, and the window the
# overlap is measured on.
_PROBE_POOL = 200
_TOP_N = 20

_FINGERPRINT_SQL = with_version(
    """
    SELECT (SELECT COUNT(*) FROM signal_lab_rounds),
           (SELECT COALESCE(MAX(id), 0) FROM signal_lab_rounds),
           (SELECT COUNT(*) FROM recommendations
             WHERE user_action IS NULL OR user_action = 'seen')
    """,
    SIGNAL_LAB_FIT_VERSION,
)


def build_signal_lab_eval(conn: sqlite3.Connection) -> dict[str, Any]:
    from alma.application.graph_substrate import load_vectors_by_id
    from alma.application.signal_lab import scoring_terms
    from alma.application.signal_lab.fit import MODEL_VIEW_KEY
    from alma.discovery.similarity import get_active_embedding_model

    model_stored = mv.get_stored(conn, MODEL_VIEW_KEY)
    if model_stored is None:
        return {"ready": False}
    payload = model_stored["payload"]

    # Hypothetical lab context: the fitted heads at the probe weight.
    lab_ctx = {
        "w_offset": HYPOTHETICAL_POINTS,
        "w_utility": HYPOTHETICAL_POINTS,
        "offsets": {
            int(k): float(v) for k, v in (payload.get("region_offsets") or {}).items()
        },
        "utility": None,
        "centroids": {},
    }
    from alma.application import super_regions as sr
    from alma.application.signal_lab.fit import decode_head_vector

    if payload.get("utility_b64"):
        lab_ctx["utility"] = decode_head_vector(payload["utility_b64"])
    regions_stored = mv.get_stored(conn, sr.VIEW_KEY)
    if regions_stored is not None and lab_ctx["offsets"]:
        for region in regions_stored["payload"].get("regions", []):
            lab_ctx["centroids"][int(region["id"])] = sr.decode_centroid(
                region["centroid_b64"]
            )

    rows = conn.execute(
        """
        SELECT id, paper_id, score FROM recommendations
        WHERE user_action IS NULL OR user_action = 'seen'
        ORDER BY score DESC LIMIT ?
        """,
        (_PROBE_POOL,),
    ).fetchall()
    churn: dict[str, Any] = {"pool": len(rows)}
    if rows:
        model = get_active_embedding_model(conn)
        vectors = load_vectors_by_id(conn, [str(r["paper_id"]) for r in rows], model)
        baseline = [(str(r["id"]), float(r["score"])) for r in rows]
        adjusted = []
        for r in rows:
            bonus = 0.0
            vec = vectors.get(str(r["paper_id"]))
            if vec is not None:
                off, util = scoring_terms.compute_lab_adjustments(vec, lab_ctx)
                bonus = lab_ctx["w_offset"] * off + lab_ctx["w_utility"] * util
            adjusted.append((str(r["id"]), float(r["score"]) + bonus))
        base_top = [rid for rid, _ in baseline[:_TOP_N]]
        adj_sorted = [rid for rid, _ in sorted(adjusted, key=lambda kv: -kv[1])]
        adj_top = adj_sorted[:_TOP_N]
        entered = len(set(adj_top) - set(base_top))
        base_rank = {rid: i for i, (rid, _) in enumerate(baseline)}
        displacement = [abs(base_rank[rid] - i) for i, rid in enumerate(adj_sorted)]
        churn.update(
            {
                "top_n": _TOP_N,
                "hypothetical_points": HYPOTHETICAL_POINTS,
                "entered_top": entered,
                "mean_rank_displacement": round(float(np.mean(displacement)), 2),
            }
        )

    return {
        "ready": True,
        "holdout": payload.get("holdout", {}),
        "counts": payload.get("counts", {}),
        "churn": churn,
    }


mv.register(
    mv.View(
        key=EVAL_VIEW_KEY,
        fingerprint_sql=_FINGERPRINT_SQL,
        build_fn=build_signal_lab_eval,
        operation_key="materialize.signal_lab.eval",
    )
)
