"""Round policy — the ONLY chooser of what a minigame shows (task 54 §3).

Selection = judgeability × informativeness, library-outward:

1. **Region** — sampled ∝ ``γ^ring · mass^0.5 · uncertainty · staleness ·
   judgeability`` (the ring prior), with a fixed ε share of ring-uniform
   exploration. ε is the single most important guardrail in the design:
   without it the sampler only ever asks where it already looks and the
   taste model becomes self-confirming.
2. **Pool** — the region's papers that are actually judgeable: a vector
   AND a title AND an abstract or TLDR. A paper with no abstract is not a
   question, it is a coin flip (cold-start lesson: Rashid/Golbandi —
   entropy alone surfaces items the user cannot judge).
3. **Triplet** — ~:data:`CANDIDATE_TRIPLETS` random draws scored by BALD
   ensemble disagreement (Houlsby 2011) when the fitted ensemble exists,
   else uniform. Presentation order is shuffled — position bias is real
   and free to remove.

Everything here is deterministic given the ``rng`` — the stage-0 simulator
drives these exact functions with seeded generators, so what we measure is
what ships.
"""

from __future__ import annotations

import logging
import sqlite3
from dataclasses import dataclass

import numpy as np

logger = logging.getLogger(__name__)

# Ring-uniform exploration share (task 54 §3).
EPSILON = 0.20

# Candidate triplets scored per round.
CANDIDATE_TRIPLETS = 200

# Sub-linear mass exponent — two giant regions must not eat every round.
MASS_EXPONENT = 0.5

# A region needs at least this many judgeable papers to host a k=3 round
# without immediately repeating cards.
MIN_POOL_FACTOR = 3


@dataclass(frozen=True)
class RegionChoice:
    region_id: int
    ring: int
    explored: bool  # True when the ε branch picked it


def region_weights(
    payload: dict,
    rings: dict[int, int],
    *,
    gamma: float,
    uncertainty: dict[int, float] | None = None,
    staleness: dict[int, float] | None = None,
    judgeability: dict[int, float] | None = None,
) -> dict[int, float]:
    """The ring-prior sampling weight per region. Missing factors default 1.

    Pure; every factor is a plain dict so the simulator can plant scenarios.
    """
    weights: dict[int, float] = {}
    for region in payload.get("regions", []):
        rid = int(region["id"])
        w = (gamma ** rings.get(rid, 0)) * (max(1, int(region.get("mass") or 1)) ** MASS_EXPONENT)
        w *= (uncertainty or {}).get(rid, 1.0)
        w *= (staleness or {}).get(rid, 1.0)
        w *= (judgeability or {}).get(rid, 1.0)
        if w > 0:
            weights[rid] = float(w)
    return weights


def choose_region(
    weights: dict[int, float],
    rings: dict[int, int],
    rng: np.random.Generator,
    *,
    epsilon: float = EPSILON,
) -> RegionChoice | None:
    """Weighted draw, with an ε share of ring-uniform exploration.

    The ε branch first picks a RING uniformly, then a region uniformly
    inside it — so the far corpus is sampled at a rate independent of how
    little the model currently cares about it.
    """
    if not weights:
        return None
    ids = sorted(weights)
    if float(rng.random()) < epsilon:
        ring_values = sorted({rings.get(rid, 0) for rid in ids})
        ring = ring_values[int(rng.integers(len(ring_values)))]
        in_ring = [rid for rid in ids if rings.get(rid, 0) == ring]
        rid = in_ring[int(rng.integers(len(in_ring)))]
        return RegionChoice(region_id=rid, ring=ring, explored=True)
    probs = np.asarray([weights[rid] for rid in ids], dtype=np.float64)
    probs /= probs.sum()
    rid = ids[int(rng.choice(len(ids), p=probs))]
    return RegionChoice(region_id=rid, ring=rings.get(rid, 0), explored=False)


def load_region_pool(
    conn: sqlite3.Connection,
    payload: dict,
    region_id: int,
    *,
    model: str,
    limit: int = 800,
) -> list[str]:
    """Judgeable paper ids of a region: vector + title + abstract-or-TLDR.

    Bounded; vectors for the pool are loaded separately (region-scoped, the
    task 54 §5 efficiency budget) via ``graph_substrate.load_vectors_by_id``.
    """
    clusters = [
        int(cid)
        for cid, rid in (payload.get("cluster_to_region") or {}).items()
        if int(rid) == int(region_id)
    ]
    if not clusters:
        return []
    placeholders = ",".join("?" for _ in clusters)
    try:
        rows = conn.execute(
            f"""
            SELECT pc.paper_id
            FROM publication_clusters pc
            JOIN papers p ON p.id = pc.paper_id
            JOIN publication_embeddings pe
              ON pe.paper_id = pc.paper_id AND pe.model = ?
            WHERE pc.scope = 'corpus'
              AND pc.cluster_id IN ({placeholders})
              AND TRIM(COALESCE(p.title, '')) != ''
              AND (TRIM(COALESCE(p.abstract, '')) != ''
                   OR TRIM(COALESCE(p.tldr, '')) != '')
            LIMIT ?
            """,
            (model, *clusters, limit),
        ).fetchall()
    except sqlite3.OperationalError:
        return []
    return [str(r[0]) for r in rows]


def draw_triplets(
    pool: list[str],
    rng: np.random.Generator,
    *,
    n: int = CANDIDATE_TRIPLETS,
    k: int = 3,
) -> list[tuple[str, ...]]:
    """``n`` distinct random k-subsets of the pool (fewer when the pool is tiny)."""
    if len(pool) < k:
        return []
    seen: set[tuple[str, ...]] = set()
    out: list[tuple[str, ...]] = []
    attempts = 0
    while len(out) < n and attempts < n * 4:
        attempts += 1
        picks = tuple(sorted(rng.choice(len(pool), size=k, replace=False).tolist()))
        key = tuple(pool[i] for i in picks)
        if key not in seen:
            seen.add(key)
            out.append(key)
    return out


def bald_scores(
    triplets: list[tuple[str, ...]],
    vectors: dict[str, np.ndarray],
    ensemble: list[np.ndarray],
) -> np.ndarray:
    """Ensemble disagreement per triplet — vote entropy over each member's
    predicted "best" (Houlsby 2011; what NEXT deploys for triplet collection).

    No ensemble (cold start) ⇒ zeros: the caller falls back to a uniform
    pick, which is the correct cold-start behaviour anyway.
    """
    scores = np.zeros(len(triplets), dtype=np.float64)
    if not ensemble:
        return scores
    for t_idx, trip in enumerate(triplets):
        vecs = [vectors.get(pid) for pid in trip]
        if any(v is None for v in vecs):
            continue
        stack = np.stack(vecs)  # k × d
        votes = np.zeros(len(trip), dtype=np.float64)
        for w in ensemble:
            votes[int(np.argmax(stack @ w))] += 1.0
        p = votes / votes.sum()
        nz = p[p > 0]
        scores[t_idx] = float(-(nz * np.log(nz)).sum())
    return scores


def select_triplet(
    pool: list[str],
    vectors: dict[str, np.ndarray],
    ensemble: list[np.ndarray],
    rng: np.random.Generator,
    *,
    k: int = 3,
) -> list[str] | None:
    """Draw candidates, score by BALD, return the winner in SHUFFLED order."""
    if len(pool) < max(k, MIN_POOL_FACTOR):
        return None
    triplets = draw_triplets(pool, rng, k=k)
    if not triplets:
        return None
    scores = bald_scores(triplets, vectors, ensemble)
    best = int(np.argmax(scores)) if scores.max() > 0 else int(rng.integers(len(triplets)))
    chosen = list(triplets[best])
    rng.shuffle(chosen)  # position bias is free to remove
    return chosen
