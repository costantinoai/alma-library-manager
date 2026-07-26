"""Stage-0 simulator — the numbers BEFORE any user answers a round.

Task 54 §6 stage 0: a synthetic user (logistic choice over a hidden utility
``w*``, with noise and a skip rate) answers rounds selected by competing
policies, on either synthetic geometry or the real corpus's vectors. The
output is judgments-to-accuracy curves per policy, and each measurement
carries a CUT DECISION:

* ``bald`` must clearly beat ``stratified_random`` by N=200 judgments or
  the ensemble layer is deleted from the design (it is the largest single
  chunk of complexity in the policy).
* offset-recovery error vs judgment count sets the §3 coverage threshold
  empirically instead of a guess.

Everything runs through the SHIPPING code paths — ``policy.select_triplet``
/ ``policy.bald_scores`` for selection and ``fit.fit_model`` for learning —
so what the simulator measures is what production does. No DB writes, no
clock, fully seeded.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Any

import numpy as np

from alma.application.signal_lab.fit import decode_head_vector, fit_model
from alma.application.signal_lab.games.stub import BEST_WORST_SIM_GAME
from alma.application.signal_lab.policy import bald_scores, draw_triplets
from alma.application.signal_lab.spec import RoundRow

logger = logging.getLogger(__name__)

# Checkpoints (answered-judgment counts) at which holdout accuracy is read.
CHECKPOINTS = (25, 50, 100, 200)

# Synthetic-user behaviour: softmax temperature on w*·x (lower = noisier
# choices) and the probability a triplet is skipped outright.
CHOICE_TEMPERATURE = 3.0
SKIP_RATE = 0.10


@dataclass
class SimWorld:
    """The fixed ground truth one simulation runs against."""

    vectors: dict[str, np.ndarray]
    paper_regions: dict[str, int]
    w_true: np.ndarray
    region_pools: dict[int, list[str]] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if not self.region_pools:
            pools: dict[int, list[str]] = {}
            for pid, rid in self.paper_regions.items():
                pools.setdefault(rid, []).append(pid)
            self.region_pools = {r: sorted(p) for r, p in pools.items()}


def synthetic_world(
    *,
    n_regions: int = 8,
    papers_per_region: int = 40,
    dim: int = 16,
    seed: int = 0,
) -> SimWorld:
    """A gaussian-mixture corpus with a planted utility direction."""
    rng = np.random.default_rng(seed)
    centers = rng.normal(size=(n_regions, dim)).astype(np.float32)
    centers /= np.linalg.norm(centers, axis=1, keepdims=True)
    vectors: dict[str, np.ndarray] = {}
    paper_regions: dict[str, int] = {}
    for r in range(n_regions):
        for i in range(papers_per_region):
            pid = f"r{r}_p{i}"
            vectors[pid] = (
                centers[r] + rng.normal(scale=0.35, size=dim).astype(np.float32)
            )
            paper_regions[pid] = r
    w_true = rng.normal(size=dim).astype(np.float32)
    w_true /= np.linalg.norm(w_true)
    return SimWorld(vectors=vectors, paper_regions=paper_regions, w_true=w_true)


def world_from_corpus(conn, *, max_papers: int = 2000, seed: int = 0) -> SimWorld | None:
    """The real corpus's geometry with the Library centroid as ``w*``.

    Read-only. Returns None when the substrate/vectors are too thin — the
    caller falls back to :func:`synthetic_world`.
    """
    from alma.application import materialized_views as mv
    from alma.application import super_regions as sr
    from alma.application.graph_substrate import load_vectors_by_id
    from alma.discovery.similarity import get_active_embedding_model

    stored = mv.get_stored(conn, sr.VIEW_KEY)
    if stored is None:
        return None
    payload = stored["payload"]
    cluster_to_region = {
        int(k): int(v) for k, v in (payload.get("cluster_to_region") or {}).items()
    }
    if not cluster_to_region:
        return None

    rows = conn.execute(
        "SELECT paper_id, cluster_id FROM publication_clusters "
        "WHERE scope = 'corpus' AND cluster_id >= 0"
    ).fetchall()
    rng = np.random.default_rng(seed)
    pairs = [(str(r[0]), cluster_to_region.get(int(r[1]))) for r in rows]
    pairs = [(pid, rid) for pid, rid in pairs if rid is not None]
    if len(pairs) > max_papers:
        idx = rng.choice(len(pairs), size=max_papers, replace=False)
        pairs = [pairs[i] for i in idx]

    model = get_active_embedding_model(conn)
    vectors = load_vectors_by_id(conn, [pid for pid, _ in pairs], model)
    paper_regions = {pid: rid for pid, rid in pairs if pid in vectors}
    if len(paper_regions) < 100:
        return None

    lib_rows = conn.execute(
        "SELECT id FROM papers WHERE status = 'library' LIMIT 2000"
    ).fetchall()
    lib_vecs = load_vectors_by_id(conn, [str(r[0]) for r in lib_rows], model)
    if len(lib_vecs) < 5:
        return None
    w_true = np.mean(np.stack(list(lib_vecs.values())), axis=0)
    w_true = (w_true / (np.linalg.norm(w_true) or 1.0)).astype(np.float32)
    return SimWorld(
        vectors={k: v for k, v in vectors.items() if k in paper_regions},
        paper_regions=paper_regions,
        w_true=w_true,
    )


# ---------------------------------------------------------------------------
# Synthetic user + selection policies
# ---------------------------------------------------------------------------


def _synthetic_answer(
    trip: tuple[str, ...],
    world: SimWorld,
    rng: np.random.Generator,
) -> dict[str, Any] | None:
    """Best-worst pick from a softmax over ``w*·x``; None = skip."""
    if float(rng.random()) < SKIP_RATE:
        return None
    utils = np.asarray([float(world.w_true @ world.vectors[p]) for p in trip])
    p_best = np.exp(CHOICE_TEMPERATURE * utils)
    p_best /= p_best.sum()
    best = int(rng.choice(len(trip), p=p_best))
    rest = [i for i in range(len(trip)) if i != best]
    p_worst = np.exp(-CHOICE_TEMPERATURE * utils[rest])
    p_worst /= p_worst.sum()
    worst = rest[int(rng.choice(len(rest), p=p_worst))]
    return {"best": trip[best], "worst": trip[worst]}


def _select(
    policy: str,
    world: SimWorld,
    ensemble: list[np.ndarray],
    rng: np.random.Generator,
) -> tuple[str, ...] | None:
    """One triplet under a named policy — the competitors of stage 0."""
    region_ids = sorted(world.region_pools)
    region = region_ids[int(rng.integers(len(region_ids)))]  # stratified: uniform region
    pool = world.region_pools[region]
    if len(pool) < 3:
        return None
    triplets = draw_triplets(pool, rng, n=60)
    if not triplets:
        return None
    if policy == "stratified_random":
        return triplets[int(rng.integers(len(triplets)))]
    if policy == "bald":
        scores = bald_scores(triplets, world.vectors, ensemble)
        if scores.max() <= 0:
            return triplets[int(rng.integers(len(triplets)))]
        return triplets[int(np.argmax(scores))]
    if policy == "margin":
        # Closest-utility spread under the CURRENT point estimate — a
        # classic uncertainty-sampling baseline (needs an estimate too).
        w = ensemble[0] if ensemble else None
        if w is None:
            return triplets[int(rng.integers(len(triplets)))]
        spreads = []
        for trip in triplets:
            u = [float(w @ world.vectors[p]) for p in trip]
            spreads.append(max(u) - min(u))
        return triplets[int(np.argmin(np.asarray(spreads)))]
    raise ValueError(f"unknown policy {policy!r}")


def _holdout_accuracy(
    w_fit: np.ndarray | None,
    world: SimWorld,
    rng: np.random.Generator,
    *,
    n_pairs: int = 400,
) -> float | None:
    """Ground-truth pairwise accuracy of a fitted w on fresh random pairs."""
    if w_fit is None:
        return None
    ids = sorted(world.vectors)
    hits = judged = 0
    for _ in range(n_pairs):
        a, b = (ids[i] for i in rng.choice(len(ids), size=2, replace=False))
        ta, tb = float(world.w_true @ world.vectors[a]), float(world.w_true @ world.vectors[b])
        if abs(ta - tb) < 1e-6:
            continue
        fa, fb = float(w_fit @ world.vectors[a]), float(w_fit @ world.vectors[b])
        if fa == fb:
            continue
        judged += 1
        hits += 1 if (fa > fb) == (ta > tb) else 0
    return round(hits / judged, 4) if judged else None


def run_policy(
    world: SimWorld,
    policy: str,
    *,
    rounds_budget: int = 220,
    refit_every: int = 5,
    seed: int = 1,
) -> dict[str, Any]:
    """Simulate one policy end-to-end through the SHIPPING fit."""
    rng = np.random.default_rng(seed)
    games = {BEST_WORST_SIM_GAME.id: BEST_WORST_SIM_GAME}
    history: list[RoundRow] = []
    ensemble: list[np.ndarray] = []
    answered = 0
    curve: dict[int, float | None] = {}
    checkpoints = iter(CHECKPOINTS)
    next_cp = next(checkpoints, None)

    for i in range(1, rounds_budget + 1):
        trip = _select(policy, world, ensemble, rng)
        if trip is None:
            continue
        answer = _synthetic_answer(trip, world, rng)
        history.append(
            RoundRow(
                id=i,
                game_id=BEST_WORST_SIM_GAME.id,
                region_id=world.paper_regions.get(trip[0]),
                pair_region_id=None,
                region_version=1,
                ring=0,
                policy_version=1,
                shown=list(trip),
                answer=answer,
                skipped=answer is None,
                holdout=False,  # ground-truth eval below is the real holdout
            )
        )
        if answer is not None:
            answered += 1

        refit_due = answered > 0 and (answered % refit_every == 0 or answered == next_cp)
        if refit_due:
            payload = fit_model(
                history,
                games=games,
                vectors=world.vectors,
                paper_regions=world.paper_regions,
                prior=None,
            )
            ensemble = [decode_head_vector(b) for b in payload["ensemble_b64"]]
        if next_cp is not None and answered >= next_cp:
            payload = fit_model(
                history,
                games=games,
                vectors=world.vectors,
                paper_regions=world.paper_regions,
                prior=None,
            )
            w_fit = (
                decode_head_vector(payload["utility_b64"])
                if payload["utility_b64"]
                else None
            )
            curve[next_cp] = _holdout_accuracy(w_fit, world, np.random.default_rng(99))
            next_cp = next(checkpoints, None)
        if next_cp is None and answered >= max(CHECKPOINTS):
            break

    return {"policy": policy, "answered": answered, "accuracy_at": curve}


def offset_recovery(
    world: SimWorld,
    *,
    judgments_grid: tuple[int, ...] = (10, 20, 40, 80),
    seed: int = 3,
) -> dict[int, float]:
    """Mean |fitted − true| region-offset error vs judgment count.

    Sets the §3 coverage threshold empirically: the grid point where the
    error stabilises is the "enough judgments per region" number.
    """
    rng = np.random.default_rng(seed)
    games = {BEST_WORST_SIM_GAME.id: BEST_WORST_SIM_GAME}
    true_means = {
        r: float(np.mean([world.w_true @ world.vectors[p] for p in pool]))
        for r, pool in world.region_pools.items()
    }
    spread = max(true_means.values()) - min(true_means.values()) or 1.0
    normalised_true = {
        r: 2.0 * (v - min(true_means.values())) / spread - 1.0
        for r, v in true_means.items()
    }

    out: dict[int, float] = {}
    history: list[RoundRow] = []
    i = 0
    for target in judgments_grid:
        while sum(1 for h in history if h.answer) < target * len(world.region_pools):
            i += 1
            all_ids = sorted(world.vectors)
            trip = tuple(
                all_ids[j] for j in rng.choice(len(all_ids), size=3, replace=False)
            )
            answer = _synthetic_answer(trip, world, rng)
            history.append(
                RoundRow(
                    id=i, game_id=BEST_WORST_SIM_GAME.id, region_id=None,
                    pair_region_id=None, region_version=1, ring=0,
                    policy_version=1, shown=list(trip), answer=answer,
                    skipped=answer is None, holdout=False,
                )
            )
        payload = fit_model(
            history, games=games, vectors=world.vectors,
            paper_regions=world.paper_regions, prior=None,
        )
        offsets = {int(k): v for k, v in payload["region_offsets"].items()}
        if not offsets:
            out[target] = 1.0
            continue
        lo, hi = min(offsets.values()), max(offsets.values())
        norm = (hi - lo) or 1.0
        errs = [
            abs((2.0 * (offsets[r] - lo) / norm - 1.0) - normalised_true[r])
            for r in offsets
            if r in normalised_true
        ]
        out[target] = round(float(np.mean(errs)), 4) if errs else 1.0
    return out


def run_stage0(world: SimWorld, *, seeds: tuple[int, ...] = (1, 2, 3)) -> dict[str, Any]:
    """The full stage-0 report: policy curves (seed-averaged) + gates."""
    curves: dict[str, dict[int, list[float]]] = {}
    for policy in ("stratified_random", "bald", "margin"):
        for seed in seeds:
            result = run_policy(world, policy, seed=seed)
            for cp, acc in result["accuracy_at"].items():
                if acc is not None:
                    curves.setdefault(policy, {}).setdefault(cp, []).append(acc)

    summary = {
        policy: {cp: round(float(np.mean(vals)), 4) for cp, vals in sorted(cps.items())}
        for policy, cps in curves.items()
    }
    final_cp = max(CHECKPOINTS)
    bald_final = summary.get("bald", {}).get(final_cp)
    random_final = summary.get("stratified_random", {}).get(final_cp)
    gates = {
        "bald_beats_random_at_200": (
            None
            if bald_final is None or random_final is None
            else bool(bald_final > random_final + 0.02)
        ),
    }
    return {
        "accuracy_curves": summary,
        "offset_recovery_error": offset_recovery(world),
        "gates": gates,
    }
