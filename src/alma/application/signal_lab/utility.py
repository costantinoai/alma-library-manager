"""Shared evidence scaling and semantic projections for Signal Lab consumers.

Pure math: ranking, Home and map adapters depend on this module, never on each
other. These functions do not read or change geometric layout.
"""

from __future__ import annotations

import numpy as np

# A full-strength directional projection needs roughly twenty best/worst
# rounds (three pairwise preferences each). Before that, Terrain moves
# proportionally rather than painting a confident result from one click.
UTILITY_FULL_STRENGTH_PREFS = 60


def utility_confidence(model_payload: dict) -> float:
    """Evidence multiplier shared by map and ranking consumers."""
    train_prefs = int((model_payload.get("counts") or {}).get("train_prefs") or 0)
    return min(1.0, train_prefs / UTILITY_FULL_STRENGTH_PREFS)


def project_utility_to_regions(
    model_payload: dict,
    regions_payload: dict,
) -> dict[int, float]:
    """Project the game-only utility direction onto semantic super-regions.

    Raw cosine-like projections are mass-centred so the result expresses
    relative movement (some regions up, some down) instead of tinting the whole
    map green. The range is normalised, then confidence-scaled by the number of
    training comparisons; a handful of answers therefore nudges Terrain, while
    a mature deck can use the configured full strength.
    """
    encoded = model_payload.get("utility_delta_b64")
    if not encoded:
        return {}

    from alma.application.signal_lab.fit import decode_head_vector
    from alma.application.super_regions import decode_centroid

    utility = decode_head_vector(str(encoded))
    utility_norm = float(np.linalg.norm(utility))
    if utility_norm <= 1e-8:
        return {}

    raw: dict[int, float] = {}
    masses: dict[int, float] = {}
    for region in regions_payload.get("regions") or []:
        centroid = decode_centroid(str(region["centroid_b64"]))
        if centroid.shape != utility.shape:
            continue
        centroid_norm = float(np.linalg.norm(centroid))
        if centroid_norm <= 1e-8:
            continue
        region_id = int(region["id"])
        raw[region_id] = float(np.dot(utility, centroid) / (utility_norm * centroid_norm))
        masses[region_id] = float(max(1, int(region.get("mass") or 1)))
    if not raw:
        return {}

    total_mass = sum(masses.values())
    centre = sum(raw[rid] * masses[rid] for rid in raw) / total_mass
    centred = {rid: value - centre for rid, value in raw.items()}
    span = max(abs(value) for value in centred.values())
    if span <= 1e-8:
        return {}

    confidence = utility_confidence(model_payload)
    return {
        rid: float(np.clip((value / span) * confidence, -1.0, 1.0))
        for rid, value in centred.items()
    }
