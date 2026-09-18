"""Lab → ranking bridge: the ONLY reader of the model for scoring (task 54 P11).

Two shapes of consumption live here. `load_lab_scoring_context` +
`compute_lab_adjustments` MEASURE the per-candidate heads (region offset +
utility direction) on the signed unit interval; `measure_candidate` writes
them into the breakdown and the ranker (`discovery.ranker.LAB_ADJUSTMENTS`)
is the one place they are WEIGHTED into a score. `fold_lab_offsets` serves the
CATEGORICAL heads, which do not score on their own at all — they are added
into the curated signal that already answers the same question, so there is
never a second definition of "how much do I care about this author / this
venue".

`load_lab_scoring_context` runs ONCE per scoring pass (lens refresh / feed
scan) and returns None unless the Lab is enabled, at least one head weight is
positive, and a fitted model exists; the per-candidate cost is then ≤32 dot
products (region assignment) + one dot product (utility), zero queries. With
the loader returning None, `measure_candidate` writes no Lab keys, the ranker
reads them as *unavailable*, and the score is byte-identical to a lab-less
build.
"""

from __future__ import annotations

import sqlite3
from typing import Any

import numpy as np

CATEGORICAL_HEAD_MAX_AFFINITY = 0.35
"""How far a FULLY-evidenced categorical head may move an affinity, at the
maximum setting.

The setting is expressed in points on the 0–100 score, because that is what
`lab_region_offset` and `lab_utility` are — they are added to the score
directly. The categorical heads are not: they are added into an affinity map
that `score_candidate` then clamps to [0, 1] before weighting.

Using the raw points there is a unit error with teeth. At the default 5.0, an
offset of 0.2 became +1.0 on a [0, 1] map, so the clamp turned every judged
entity into a hard 1.0 or 0.0 — a binary override of the curated signal, in a
feature whose whole contract is "Signal Lab nudges, your Library decides". It
shipped that way for the author head; measured on the dev DB 2026-07-28, a
venue at +0.2 took `nature neuroscience` from 0.9 to −0.1.

0.35 is the ceiling at the maximum setting (10.0). It is deliberately of the
same order as a real curated affinity: strong enough to reorder near-ties and
to be felt, never enough to replace what your Library said.
"""


def fold_lab_offsets(
    conn: sqlite3.Connection,
    affinity: dict[str, float],
    *,
    head: str,
    weight_key: str,
) -> int:
    """ADD a fitted categorical head into a curated affinity map. Returns how
    many keys it moved.

    THE way a Signal Lab categorical head reaches ranking. Every such head —
    authors today, venues today — folds into the ONE curated signal that already
    answers its question (``author_affinity``, ``journal_affinity``) instead of
    scoring as a parallel term beside it. A parallel term would let the same
    evidence be counted twice and drift apart, which is the trap the
    rating-contract guard exists to prevent.

    The four gates, in order, are the same for every head, which is why they are
    spelled once here rather than copied per call site:

    1. one settings read for both gates — this runs inside the preference-profile
       build, on the scoring hot path;
    2. ``signal_lab.enabled`` — disabling the lab keeps every round and the
       fitted model but stops anything reading them;
    3. weight ≤ 0 ⇒ return before touching the materialized view at all;
    4. offsets are win rates in [-1, 1], converted from the setting's SCORE
       points into affinity units (see `CATEGORICAL_HEAD_MAX_AFFINITY`), and
       the result is ADDED, never substituted. Signal Lab nudges; your Library
       decides.
    """
    from alma.application import materialized_views as mv
    from alma.application.discovery.lens_crud import read_settings
    from alma.application.signal_lab.fit import MODEL_VIEW_KEY
    from alma.discovery.defaults import LAB_HEAD_MAX_POINTS, lab_head_points

    # `read_settings` already answers "no settings table" with the defaults;
    # any OTHER failure is a real read error and must surface, not read as
    # "the Lab contributes 0" (an unreadable state impersonating an empty one).
    settings = read_settings(conn)

    points = lab_head_points(settings, weight_key)
    if points <= 0:
        return 0
    # Points on the 0-100 score → affinity units on a [0, 1] map.
    weight = (
        min(1.0, points / LAB_HEAD_MAX_POINTS) * CATEGORICAL_HEAD_MAX_AFFINITY
    )

    stored = mv.get_stored(conn, MODEL_VIEW_KEY)
    offsets = (stored or {}).get("payload", {}).get(head) or {}
    if not offsets:
        return 0

    moved = 0
    for key, offset in offsets.items():
        try:
            delta = float(offset) * weight
        except (TypeError, ValueError):
            continue
        if delta:
            affinity[key] = affinity.get(key, 0.0) + delta
            moved += 1
    return moved


def load_lab_scoring_context(
    conn: sqlite3.Connection, settings: dict[str, str]
) -> dict[str, Any] | None:
    """One-shot load of everything scoring needs. None ⇒ lab contributes 0."""
    from alma.application.discovery.ranker import resolve_lab_points
    from alma.application.signal_lab.utility import utility_confidence
    # Same parser the ranker weights with, so the gate and the score can never
    # disagree about whether a head is on.
    if not any(points > 0 for points in resolve_lab_points(settings).values()):
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
        decode_head_vector(payload["utility_delta_b64"])
        if payload.get("utility_delta_b64")
        else None
    )
    centroids: dict[int, np.ndarray] = {}
    if regions_stored is not None and offsets:
        for region in regions_stored["payload"].get("regions", []):
            centroids[int(region["id"])] = sr.decode_centroid(region["centroid_b64"])
    if not offsets and utility is None:
        return None
    confidence = utility_confidence(payload)
    return {
        "offsets": offsets,
        "utility": utility,
        "utility_confidence": confidence,
        "centroids": centroids,
        # Provenance stamped onto every measured candidate (→ the immutable
        # ranking snapshot's exposure), so a stored score names the model and
        # region payload that produced its Lab inputs.
        "generation": {
            "model_fingerprint": model_stored.get("fingerprint"),
            "model_computed_at": model_stored.get("computed_at"),
            "regions_fingerprint": (
                regions_stored.get("fingerprint") if regions_stored else None
            ),
            "regions_computed_at": (
                regions_stored.get("computed_at") if regions_stored else None
            ),
            "utility_confidence": confidence,
        },
    }


def compute_lab_adjustments(embedding, lab_ctx: dict[str, Any]) -> tuple[float, float]:
    """(region_offset_raw, utility_raw) for one candidate. Pure, no queries.

    Region via nearest super-centroid on the embedding already in hand —
    the same cosine rule as everywhere else (semantic_partition).
    """
    if embedding is None:
        return 0.0, 0.0
    vec = np.asarray(embedding, dtype=np.float32)
    offset_raw = 0.0
    centroids = lab_ctx.get("centroids") or {}
    if centroids and lab_ctx.get("offsets"):
        from alma.application.semantic_partition import assign_with_margin

        assigned = assign_with_margin(vec, centroids)
        offset_raw = float(lab_ctx["offsets"].get(assigned.best_id, 0.0))
    utility_raw = 0.0
    utility = lab_ctx.get("utility")
    if utility is not None and utility.shape[0] == vec.shape[0]:
        norm = float(np.linalg.norm(vec)) * float(np.linalg.norm(utility))
        if norm > 1e-8:
            confidence = float(lab_ctx.get("utility_confidence", 1.0))
            utility_raw = float(np.clip(((utility @ vec) / norm) * confidence, -1.0, 1.0))
    return offset_raw, utility_raw
