"""Fit — ``rounds → model``, pure and wholesale. The ``signal_lab:model`` view.

The load-bearing rule (task 54 §0): the fitted model is a pure function of
the round history, recomputed from scratch on every rebuild. There is NO
incremental update path — incremental accumulation is exactly what makes
``preference_profiles`` unpurgeable. Purge deletes the rounds; the next
rebuild of this view is honestly empty, through the same code path as every
other fit.

:func:`fit_model` is deliberately connection-free: the view's ``build_fn``
(:func:`build_signal_lab_model`) gathers inputs (rounds, vectors, paper→
region map, prior) and hands them over. That keeps the fit golden-file
testable and lets the stage-0 simulator drive it with synthetic answers.

Heads, in order of statistical safety (task 53):

* ``region_offsets`` — James–Stein-shrunk per-region preference win-rates.
* ``utility`` — Bradley–Terry logistic on ``w·x``, ridge-shrunk to the
  Library-centroid prior, plus a K-vector bootstrap ensemble (the BALD
  disagreement scorer the round policy reads).
* ``region_overrides`` — odd-one-out boundary votes past a threshold.
* metric head — M2, gated on the stage-0 reachability check. Not fitted here.

M0 note: the prior is the Library vector centroid. M1 wires the true Rocchio
prior (``feedback_positive_centroid − feedback_negative_centroid``) once the
scoring integration lands — same shape, better zero-round behaviour.

Holdout: rounds stamped ``holdout=1`` at creation never train; the payload
reports pairwise accuracy of each nested model on them (prior-only vs
+offsets vs +utility), which is the promotion evidence (task 54 §6).
"""

from __future__ import annotations

import base64
import logging
import sqlite3
from collections import defaultdict
from typing import Any

import numpy as np

from alma.ai.graph_versions import (
    SIGNAL_LAB_FIT_VERSION,
    SIGNAL_LAB_POLICY_VERSION,
    with_version,
)
from alma.application import materialized_views as mv
from alma.application.signal_lab.spec import MiniGame, Pref, RegionVote, RoundRow
from alma.core.vector_blob import decode_vector, encode_vector

logger = logging.getLogger(__name__)

MODEL_VIEW_KEY = "signal_lab:model"

# James–Stein-style shrinkage mass: a region's offset is pulled toward the
# global mean with the weight of this many pseudo-observations.
OFFSET_SHRINKAGE = 8.0

# Bootstrap ensemble size for the BALD disagreement scorer (Houlsby 2011).
ENSEMBLE_K = 8

# Ridge pull of the utility vector toward the prior, and full-batch
# gradient-descent settings (vectorised — a per-sample python loop timed out
# the stage-0 simulator at 500 s; the batch form is ~100× faster and drops
# the sample-order dependence entirely).
UTILITY_RIDGE = 0.10
UTILITY_EPOCHS = 200
UTILITY_LR = 0.5

# A paper needs this many consistent boundary votes before the lab overlays
# its region assignment.
OVERRIDE_MIN_VOTES = 3

# Ring-prior start (task 54 §3). γ annealing (competence gate) lands in M1;
# M0 publishes the constant so the policy has one source for it.
GAMMA_START = 0.35

_FINGERPRINT_SQL = with_version(
    "SELECT COUNT(*), COALESCE(MAX(id), 0) FROM signal_lab_rounds",
    SIGNAL_LAB_FIT_VERSION,
    str(SIGNAL_LAB_POLICY_VERSION),
)


def _b64(vec: np.ndarray) -> str:
    return base64.b64encode(encode_vector(vec)).decode("ascii")


def decode_head_vector(b64: str) -> np.ndarray:
    """Inverse of the payload's vector encoding. Shared by every consumer."""
    return decode_vector(base64.b64decode(b64.encode("ascii")))


def _unit_or_none(vec: np.ndarray | None) -> np.ndarray | None:
    if vec is None:
        return None
    norm = float(np.linalg.norm(vec))
    return None if norm <= 0 else (vec / norm).astype(np.float32)


# ---------------------------------------------------------------------------
# The pure fit
# ---------------------------------------------------------------------------


def fit_model(
    rounds: list[RoundRow],
    *,
    games: dict[str, MiniGame],
    vectors: dict[str, np.ndarray],
    paper_regions: dict[str, int],
    prior: np.ndarray | None,
    gamma_start: float = GAMMA_START,
    override_min_votes: int = OVERRIDE_MIN_VOTES,
    coverage_target: int = 20,
) -> dict[str, Any]:
    """Fit every head from scratch. Pure — no I/O, no clock, no randomness
    beyond seeds derived from round ids (so identical inputs ⇒ identical
    payload, the resume/golden-file property).
    """
    train_prefs: list[Pref] = []
    holdout_prefs: list[Pref] = []
    votes: dict[str, defaultdict[int, int]] = {}
    unknown_game_rounds = 0
    answered = 0
    skipped = 0

    for rnd in rounds:
        if rnd.skipped or rnd.answer is None:
            skipped += 1
            continue
        game = games.get(rnd.game_id)
        if game is None:
            unknown_game_rounds += 1
            continue
        answered += 1
        try:
            constraints = game.interpret(rnd)
        except Exception:  # noqa: BLE001 — one bad round must not sink the fit
            logger.warning("signal_lab fit: interpret failed for round %s", rnd.id)
            continue
        for c in constraints:
            if isinstance(c, Pref):
                (holdout_prefs if rnd.holdout else train_prefs).append(c)
            elif isinstance(c, RegionVote):
                votes.setdefault(c.paper_id, defaultdict(int))[c.region_id] += 1
            # Sim constraints feed the metric head — M2, gated.

    gamma = _anneal_gamma(rounds, gamma_start, coverage_target)
    prior_unit = _unit_or_none(prior)
    offsets = _fit_region_offsets(train_prefs, paper_regions)
    utility, ensemble = _fit_utility(train_prefs, vectors, prior_unit)
    overrides = _fit_overrides(votes, min_votes=override_min_votes)
    holdout = _holdout_metrics(
        holdout_prefs, vectors, paper_regions, prior_unit, offsets, utility
    )

    return {
        "fit_version": SIGNAL_LAB_FIT_VERSION,
        "policy_version": SIGNAL_LAB_POLICY_VERSION,
        "gamma": gamma,
        "counts": {
            "rounds": len(rounds),
            "answered": answered,
            "skipped": skipped,
            "unknown_game_rounds": unknown_game_rounds,
            "train_prefs": len(train_prefs),
            "holdout_prefs": len(holdout_prefs),
        },
        "region_offsets": {str(k): round(v, 4) for k, v in offsets.items()},
        "utility_b64": _b64(utility) if utility is not None else None,
        "ensemble_b64": [_b64(w) for w in ensemble],
        "region_overrides": overrides,
        "holdout": holdout,
    }


def _anneal_gamma(
    rounds: list[RoundRow], gamma_start: float, coverage_target: int
) -> float:
    """Competence-gated ring expansion (task 54 §3), derived purely from rounds.

    γ anneals ×1.25 per fully-covered ring level: ring ≤ k is covered when
    every region the policy has asked about at those rings holds ≥
    ``coverage_target`` answered rounds. Derived from history ⇒ purge resets
    it for free. (Confidence/plateau conditions join in M2 once the eval
    trend is persisted.)
    """
    per: dict[tuple[int, int], int] = {}
    for rnd in rounds:
        if rnd.answer is None or rnd.region_id is None or rnd.ring is None:
            continue
        key = (int(rnd.ring), int(rnd.region_id))
        per[key] = per.get(key, 0) + 1
    if not per:
        return gamma_start
    levels = 0
    ring = 0
    while True:
        at_ring = [n for (r, _), n in per.items() if r == ring]
        if not at_ring or min(at_ring) < coverage_target:
            break
        levels += 1
        ring += 1
    return float(min(1.0, gamma_start * (1.25 ** levels)))


def _fit_region_offsets(
    prefs: list[Pref], paper_regions: dict[str, int]
) -> dict[int, float]:
    """Per-region win-rate offsets, James–Stein-shrunk toward the global mean.

    A preferred paper scores +1 for its region, the rejected one −1; the
    offset is the shrunk mean in [−1, 1]. ~32 parameters, converges in tens
    of rounds — the head that ships first for a reason (task 53).
    """
    sums: defaultdict[int, float] = defaultdict(float)
    counts: defaultdict[int, int] = defaultdict(int)
    for p in prefs:
        for pid, val in ((p.a, 1.0), (p.b, -1.0)):
            region = paper_regions.get(pid)
            if region is not None:
                sums[region] += val
                counts[region] += 1
    if not counts:
        return {}
    total_n = sum(counts.values())
    grand_mean = sum(sums.values()) / total_n
    return {
        r: (sums[r] + OFFSET_SHRINKAGE * grand_mean) / (counts[r] + OFFSET_SHRINKAGE)
        for r in counts
    }


def _sgd_utility(
    prefs: list[Pref],
    vectors: dict[str, np.ndarray],
    prior_unit: np.ndarray | None,
    *,
    seed: int,
    sample_with_replacement: bool,
) -> np.ndarray | None:
    """One Bradley–Terry logistic fit of ``w`` on preference differences.

    Full-batch gradient descent on the n×d difference matrix — vectorised,
    deterministic given the bootstrap sample, and fast enough that the
    stage-0 simulator can refit hundreds of times.
    """
    usable = [
        (vectors[p.a], vectors[p.b])
        for p in prefs
        if p.a in vectors and p.b in vectors
    ]
    if not usable:
        return None
    diffs = np.stack([(xa - xb) for xa, xb in usable]).astype(np.float32)  # n × d
    if sample_with_replacement:
        rng = np.random.default_rng(seed)
        idx = rng.choice(len(diffs), size=len(diffs), replace=True)
        diffs = diffs[idx]
    n, dim = diffs.shape
    w = (
        prior_unit.copy()
        if prior_unit is not None and prior_unit.shape[0] == dim
        else np.zeros(dim, dtype=np.float32)
    )
    anchor = w.copy()
    for _ in range(UTILITY_EPOCHS):
        z = diffs @ w
        residual = 1.0 / (1.0 + np.exp(-z)) - 1.0  # σ(z) − 1, wants each diff > 0
        grad = (diffs.T @ residual) / n + UTILITY_RIDGE * (w - anchor)
        w = w - UTILITY_LR * grad
    return w.astype(np.float32)


def _fit_utility(
    prefs: list[Pref],
    vectors: dict[str, np.ndarray],
    prior_unit: np.ndarray | None,
) -> tuple[np.ndarray | None, list[np.ndarray]]:
    """Point utility + K-member bootstrap ensemble (the BALD scorer)."""
    point = _sgd_utility(prefs, vectors, prior_unit, seed=0, sample_with_replacement=False)
    if point is None:
        return None, []
    ensemble = []
    for k in range(ENSEMBLE_K):
        w = _sgd_utility(prefs, vectors, prior_unit, seed=k + 1, sample_with_replacement=True)
        if w is not None:
            ensemble.append(w)
    return point, ensemble


def _fit_overrides(
    votes: dict[str, dict[int, int]], *, min_votes: int = OVERRIDE_MIN_VOTES
) -> dict[str, dict[str, int]]:
    """Region overrides for papers with enough consistent boundary votes."""
    out: dict[str, dict[str, int]] = {}
    for pid, per_region in votes.items():
        region, n = max(per_region.items(), key=lambda kv: kv[1])
        if n >= min_votes and n > sum(per_region.values()) - n:
            out[pid] = {"region_id": int(region), "votes": int(n)}
    return out


def _pairwise_accuracy(
    prefs: list[Pref],
    score: dict[str, float] | None,
) -> float | None:
    """Share of held-out pairs the scorer orders correctly. None when unscorable."""
    if not prefs or score is None:
        return None
    hits = 0
    judged = 0
    for p in prefs:
        sa, sb = score.get(p.a), score.get(p.b)
        if sa is None or sb is None or sa == sb:
            continue
        judged += 1
        hits += 1 if sa > sb else 0
    return round(hits / judged, 4) if judged else None


def _holdout_metrics(
    holdout_prefs: list[Pref],
    vectors: dict[str, np.ndarray],
    paper_regions: dict[str, int],
    prior_unit: np.ndarray | None,
    offsets: dict[int, float],
    utility: np.ndarray | None,
) -> dict[str, Any]:
    """Nested-model pairwise accuracy on the holdout — the promotion evidence."""
    ids = {p.a for p in holdout_prefs} | {p.b for p in holdout_prefs}

    def _scores(w: np.ndarray | None, use_offsets: bool) -> dict[str, float] | None:
        if w is None and not use_offsets:
            return None
        out: dict[str, float] = {}
        for pid in ids:
            s = 0.0
            scored = False
            if w is not None and pid in vectors:
                s += float(w @ vectors[pid])
                scored = True
            if use_offsets:
                region = paper_regions.get(pid)
                if region is not None and region in offsets:
                    s += offsets[region]
                    scored = True
            if scored:
                out[pid] = s
        return out

    return {
        "pairs": len(holdout_prefs),
        "prior_accuracy": _pairwise_accuracy(holdout_prefs, _scores(prior_unit, False)),
        "offsets_accuracy": _pairwise_accuracy(
            holdout_prefs, _scores(prior_unit, True)
        ),
        "utility_accuracy": _pairwise_accuracy(holdout_prefs, _scores(utility, True)),
    }


# ---------------------------------------------------------------------------
# The view build_fn — gathers inputs, then calls the pure fit
# ---------------------------------------------------------------------------


def build_signal_lab_model(conn: sqlite3.Connection) -> dict[str, Any]:
    """Gather (rounds, vectors, regions, prior) and run :func:`fit_model`."""
    from alma.application import signal_lab as lab
    from alma.application import super_regions as sr
    from alma.application.graph_substrate import load_vectors_by_id
    from alma.application.signal_lab.rounds import load_rounds
    from alma.discovery.similarity import get_active_embedding_model

    rounds = load_rounds(conn)
    games = {g.id: g for g in lab.available_games()}

    shown_ids = sorted({pid for rnd in rounds for pid in rnd.shown})
    model = get_active_embedding_model(conn)
    vectors = load_vectors_by_id(conn, shown_ids, model) if shown_ids else {}

    # paper → durable region id, via the current super-region payload. Rounds
    # from retired regions simply fall out of the offsets head (their Pref
    # constraints still train the utility vector).
    paper_regions: dict[str, int] = {}
    stored = mv.get_stored(conn, sr.VIEW_KEY)
    if stored is not None and shown_ids:
        cluster_to_region = {
            int(k): int(v)
            for k, v in (stored["payload"].get("cluster_to_region") or {}).items()
        }
        placeholders = ",".join("?" for _ in shown_ids)
        try:
            rows = conn.execute(
                f"""
                SELECT paper_id, cluster_id FROM publication_clusters
                WHERE scope = 'corpus' AND paper_id IN ({placeholders})
                """,
                shown_ids,
            ).fetchall()
            for row in rows:
                region = cluster_to_region.get(int(row["cluster_id"]))
                if region is not None:
                    paper_regions[str(row["paper_id"])] = region
        except sqlite3.OperationalError:
            pass

    # M0 prior: the Library vector centroid (M1 swaps in the true Rocchio
    # profile prior — see module docstring).
    prior: np.ndarray | None = None
    try:
        rows = conn.execute(
            """
            SELECT pe.embedding FROM publication_embeddings pe
            JOIN papers p ON p.id = pe.paper_id
            WHERE pe.model = ? AND p.status = 'library'
            LIMIT 2000
            """,
            (model,),
        ).fetchall()
        if rows:
            decoded = [decode_vector(r["embedding"]) for r in rows if r["embedding"]]
            if decoded:
                prior = np.mean(np.stack(decoded), axis=0)
    except sqlite3.OperationalError:
        pass

    from alma.application.signal_lab import lab_tuning

    tuning = lab_tuning(conn)
    return fit_model(
        rounds,
        games=games,
        vectors=vectors,
        paper_regions=paper_regions,
        prior=prior,
        gamma_start=tuning["gamma_start"],
        override_min_votes=tuning["override_min_votes"],
        coverage_target=tuning["coverage_target"],
    )


mv.register(
    mv.View(
        key=MODEL_VIEW_KEY,
        fingerprint_sql=_FINGERPRINT_SQL,
        build_fn=build_signal_lab_model,
        operation_key="materialize.signal_lab.model",
        # Thread path on purpose: small numpy over ≤ thousands of rounds — a
        # worker-process spawn would cost more than the fit.
    )
)
