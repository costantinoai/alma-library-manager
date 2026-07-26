"""Lab → ranking bridge: the ONLY reader of the model for scoring (task 54 P11).

`load_lab_scoring_context` runs ONCE per scoring pass (lens refresh / feed
scan) and returns None unless BOTH lab weights are... nonzero? No — unless a
model AND super-regions exist; the per-candidate cost is then 32 dot
products (region assignment) + one dot product (utility), zero queries.
Weights stay "0.0" until stage-1 evidence promotes them (D20) — with both
at zero the loader returns None and score_candidate's output is
byte-identical to a lab-less build.
"""

from __future__ import annotations

import sqlite3
from typing import Any

import numpy as np


def load_lab_scoring_context(
    conn: sqlite3.Connection, settings: dict[str, str]
) -> dict[str, Any] | None:
    """One-shot load of everything scoring needs. None ⇒ lab contributes 0."""
    w_offset = float(settings.get("weights.lab_region_offset", "0.0") or 0.0)
    w_utility = float(settings.get("weights.lab_utility", "0.0") or 0.0)
    if w_offset <= 0 and w_utility <= 0:
        return None
    from alma.application import materialized_views as mv
    from alma.application import super_regions as sr
    from alma.application.signal_lab.fit import MODEL_VIEW_KEY, decode_head_vector

    model_stored = mv.get_stored(conn, MODEL_VIEW_KEY)
    regions_stored = mv.get_stored(conn, sr.VIEW_KEY)
    if model_stored is None:
        return None
    payload = model_stored["payload"]
    offsets = {int(k): float(v) for k, v in (payload.get("region_offsets") or {}).items()}
    utility = (
        decode_head_vector(payload["utility_b64"]) if payload.get("utility_b64") else None
    )
    centroids: dict[int, np.ndarray] = {}
    if regions_stored is not None and offsets:
        for region in regions_stored["payload"].get("regions", []):
            centroids[int(region["id"])] = sr.decode_centroid(region["centroid_b64"])
    if not offsets and utility is None:
        return None
    return {
        "w_offset": w_offset,
        "w_utility": w_utility,
        "offsets": offsets,
        "utility": utility,
        "centroids": centroids,
    }


def compute_lab_adjustments(embedding, lab_ctx: dict[str, Any]) -> tuple[float, float]:
    """(region_offset_raw, utility_raw) for one candidate. Pure, no queries.

    Region via nearest super-centroid on the embedding already in hand —
    the same cosine rule as everywhere else (graph_substrate).
    """
    if embedding is None:
        return 0.0, 0.0
    vec = np.asarray(embedding, dtype=np.float32)
    offset_raw = 0.0
    centroids = lab_ctx.get("centroids") or {}
    if centroids and lab_ctx.get("offsets"):
        from alma.application.graph_substrate import assign_with_margin

        assigned = assign_with_margin(vec, centroids)
        offset_raw = float(lab_ctx["offsets"].get(assigned.best_id, 0.0))
    utility_raw = 0.0
    utility = lab_ctx.get("utility")
    if utility is not None and utility.shape[0] == vec.shape[0]:
        norm = float(np.linalg.norm(vec)) or 1.0
        utility_raw = float(np.clip((utility @ vec) / norm, -1.0, 1.0))
    return offset_raw, utility_raw
