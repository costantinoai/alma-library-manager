"""Goal-directed active query policy for Signal Lab.

One request builds one whole deck, read-only:

1. load every judgeable paper from every super-region (no arbitrary prefix);
2. allocate region/edge attention from Library-outward importance, posterior
   uncertainty, coverage, answerability, real staleness, and protected
   exploration;
3. generate a bounded structured candidate set from the full eligible pool;
4. score every game outcome with expected information gain (EIG);
5. greedily condition later choices on the deck's accumulated information,
   paper exposure, region exposure, and recent unordered-query cooldown.

Best/worst uses a six-outcome MaxDiff likelihood. Odd-one-out uses a
three-outcome diagonal-metric likelihood. Cold start uses a D-optimal geometry
proxy until a bootstrap posterior exists. Presentation order is shuffled only
after statistical selection, so it never changes query identity.
"""

from __future__ import annotations

import itertools
import math
import sqlite3
from dataclasses import dataclass

import numpy as np

from alma.application.signal_lab.evidence import (
    EdgeEvidence,
    LedgerEvidence,
    edge_key,
    load_ledger_evidence,
)
from alma.application.signal_lab.query import canonical_query_key

# Protected ring-uniform exploration share.
EPSILON = 0.20

# Candidate budget grows with region size but remains latency-bounded.
MIN_CANDIDATES = 256
MAX_CANDIDATES = 768

# One query needs three judgeable papers.
MIN_POOL_FACTOR = 3

# Answered/skipped queries excluded from normal acquisition. Deliberate
# repeated-measure queries need their own future policy and explicit label.
RECENT_QUERY_LIMIT = 500

# One week restores full staleness priority after a recently asked region/edge.
STALENESS_RECOVERY_DAYS = 7.0

# Sub-linear mass keeps a giant region important without letting n^3 dominate.
MASS_EXPONENT = 0.5


@dataclass(frozen=True)
class RegionChoice:
    region_id: int
    ring: int
    explored: bool


@dataclass
class Candidate:
    papers: tuple[str, str, str]
    predictions: np.ndarray | None
    information: np.ndarray | None
    eig: float
    goal_risk: float
    design: float
    focus: float = 0.0


@dataclass
class QueueContext:
    payload: dict
    pools: dict[int, list[str]]
    paper_regions: dict[str, int]
    vectors: dict[str, np.ndarray]
    rings: dict[int, int]
    adjacency: dict[int, list[int]]
    masses: dict[int, int]
    ledger: LedgerEvidence
    base_region_weights: dict[int, float]
    utility_ensemble: list[np.ndarray]
    metric_ensemble: list[np.ndarray]


def region_weights(
    payload: dict,
    rings: dict[int, int],
    *,
    gamma: float,
    uncertainty: dict[int, float] | None = None,
    staleness: dict[int, float] | None = None,
    judgeability: dict[int, float] | None = None,
) -> dict[int, float]:
    """Pure region allocation weights; caller owns evidence derivation."""
    weights: dict[int, float] = {}
    for region in payload.get("regions", []):
        region_id = int(region["id"])
        mass = max(1, int(region.get("mass") or 1))
        weight = (gamma ** rings.get(region_id, 0)) * (mass**MASS_EXPONENT)
        weight *= (uncertainty or {}).get(region_id, 1.0)
        weight *= (staleness or {}).get(region_id, 1.0)
        weight *= (judgeability or {}).get(region_id, 1.0)
        if weight > 0:
            weights[region_id] = float(weight)
    return weights


def choose_region(
    weights: dict[int, float],
    rings: dict[int, int],
    rng: np.random.Generator,
    *,
    epsilon: float = EPSILON,
) -> RegionChoice | None:
    """Weighted draw with ring-uniform epsilon exploration."""
    if not weights:
        return None
    ids = sorted(weights)
    if float(rng.random()) < epsilon:
        ring_values = sorted({rings.get(region_id, 0) for region_id in ids})
        ring = ring_values[int(rng.integers(len(ring_values)))]
        in_ring = [region_id for region_id in ids if rings.get(region_id, 0) == ring]
        region_id = in_ring[int(rng.integers(len(in_ring)))]
        return RegionChoice(region_id=region_id, ring=ring, explored=True)
    probs = np.asarray([weights[region_id] for region_id in ids], dtype=np.float64)
    probs /= probs.sum()
    region_id = ids[int(rng.choice(len(ids), p=probs))]
    return RegionChoice(
        region_id=region_id,
        ring=rings.get(region_id, 0),
        explored=False,
    )


def draw_triplets(
    pool: list[str],
    rng: np.random.Generator,
    *,
    n: int = MIN_CANDIDATES,
    k: int = 3,
) -> list[tuple[str, ...]]:
    """Distinct unordered k-subsets sampled from the complete input pool."""
    if len(pool) < k:
        return []
    possible = math.comb(len(pool), k)
    if possible <= n:
        return list(itertools.combinations(sorted(pool), k))

    seen: set[tuple[str, ...]] = set()
    attempts = 0
    max_attempts = max(n * 12, 100)
    while len(seen) < n and attempts < max_attempts:
        attempts += 1
        indices = np.sort(rng.choice(len(pool), size=k, replace=False))
        seen.add(tuple(sorted(pool[int(index)] for index in indices)))
    return sorted(seen)


def _softmax(logits: np.ndarray) -> np.ndarray:
    shifted = logits - np.max(logits, axis=-1, keepdims=True)
    exp = np.exp(np.clip(shifted, -60.0, 60.0))
    return exp / np.maximum(exp.sum(axis=-1, keepdims=True), 1e-12)


def _maxdiff_predictions(
    triplet: tuple[str, str, str],
    vectors: dict[str, np.ndarray],
    ensemble: list[np.ndarray],
) -> np.ndarray | None:
    """K×6 probability matrix over ordered (best, worst) outcomes."""
    if len(ensemble) < 2 or any(paper_id not in vectors for paper_id in triplet):
        return None
    stack = np.stack([vectors[paper_id] for paper_id in triplet])
    utilities = np.stack([stack @ head for head in ensemble])
    outcomes = [(best, worst) for best in range(3) for worst in range(3) if best != worst]
    logits = np.stack(
        [utilities[:, best] - utilities[:, worst] for best, worst in outcomes],
        axis=1,
    )
    return _softmax(logits)


def _odd_predictions(
    triplet: tuple[str, str, str],
    vectors: dict[str, np.ndarray],
    ensemble: list[np.ndarray],
) -> np.ndarray | None:
    """K×3 probability matrix; outcome index identifies odd paper."""
    if len(ensemble) < 2 or any(paper_id not in vectors for paper_id in triplet):
        return None
    stack = np.stack([vectors[paper_id] for paper_id in triplet])
    logits = np.empty((len(ensemble), 3), dtype=np.float64)
    for head_index, metric in enumerate(ensemble):
        if metric.shape[0] != stack.shape[1]:
            return None
        for odd in range(3):
            kept = [index for index in range(3) if index != odd]
            diff = stack[kept[0]] - stack[kept[1]]
            logits[head_index, odd] = -float(metric @ (diff * diff))
    return _softmax(logits)


def expected_information_gain(predictions: np.ndarray | None) -> float:
    """Mutual information I(response; posterior member) in nats."""
    if predictions is None or predictions.shape[0] < 2:
        return 0.0
    clipped = np.clip(predictions, 1e-12, 1.0)
    mean = np.clip(clipped.mean(axis=0), 1e-12, 1.0)
    predictive_entropy = -float(np.sum(mean * np.log(mean)))
    conditional_entropy = -float(np.mean(np.sum(clipped * np.log(clipped), axis=1)))
    return max(0.0, predictive_entropy - conditional_entropy)


def expected_information_scores(
    triplets: list[tuple[str, ...]],
    vectors: dict[str, np.ndarray],
    ensemble: list[np.ndarray],
) -> np.ndarray:
    """Pure full-outcome MaxDiff EIG scorer used by simulator and runtime."""
    return np.asarray(
        [
            expected_information_gain(
                _maxdiff_predictions(
                    (str(triplet[0]), str(triplet[1]), str(triplet[2])),
                    vectors,
                    ensemble,
                )
            )
            for triplet in triplets
        ],
        dtype=np.float64,
    )


def _ordering_goal_risk(
    triplet: tuple[str, str, str],
    vectors: dict[str, np.ndarray],
    ensemble: list[np.ndarray],
) -> float:
    """GURO-style aleatoric × epistemic ordering risk proxy.

    Bootstrap logit variance approximates posterior confidence width; logistic
    derivative gives comparison ambiguity. High score means ordering error can
    still change in a direction this query observes.
    """
    if len(ensemble) < 2 or any(paper_id not in vectors for paper_id in triplet):
        return 0.0
    stack = np.stack([vectors[paper_id] for paper_id in triplet])
    utilities = np.stack([stack @ head for head in ensemble])
    risks = []
    for left, right in itertools.combinations(range(3), 2):
        logits = utilities[:, left] - utilities[:, right]
        mean_logit = float(np.mean(logits))
        probability = 1.0 / (1.0 + math.exp(-max(-30.0, min(30.0, mean_logit))))
        aleatoric = probability * (1.0 - probability)
        epistemic = float(np.std(logits))
        risks.append(aleatoric * epistemic)
    return float(np.mean(risks)) if risks else 0.0


def _design_score(
    triplet: tuple[str, str, str],
    vectors: dict[str, np.ndarray],
) -> float:
    """Cold-start D-optimal proxy from two independent contrast directions."""
    if any(paper_id not in vectors for paper_id in triplet):
        return 0.0
    stack = np.stack([vectors[paper_id] for paper_id in triplet]).astype(np.float64)
    norms = np.linalg.norm(stack, axis=1, keepdims=True)
    stack = stack / np.maximum(norms, 1e-12)
    contrasts = np.stack([stack[0] - stack[2], stack[1] - stack[2]])
    gram = contrasts @ contrasts.T
    sign, logdet = np.linalg.slogdet(np.eye(2) + gram)
    return float(logdet) if sign > 0 else 0.0


def _information_matrix(predictions: np.ndarray | None) -> np.ndarray | None:
    """Posterior-member separation contributed by one query."""
    if predictions is None or predictions.shape[0] < 2:
        return None
    centered = predictions - predictions.mean(axis=0, keepdims=True)
    return (centered @ centered.T) / max(1, predictions.shape[1])


def _conditional_information_gain(
    accumulated: np.ndarray | None,
    candidate: np.ndarray | None,
) -> float:
    """Small K×K log-det gain; fast BatchBALD-style redundancy control."""
    if candidate is None:
        return 0.0
    size = candidate.shape[0]
    before = np.eye(size, dtype=np.float64) * 1e-6
    if accumulated is not None:
        before += accumulated
    after = before + candidate
    sign_before, log_before = np.linalg.slogdet(before)
    sign_after, log_after = np.linalg.slogdet(after)
    if sign_before <= 0 or sign_after <= 0:
        return 0.0
    return max(0.0, float(log_after - log_before))


def _normalise(values: list[float]) -> np.ndarray:
    array = np.asarray(values, dtype=np.float64)
    if len(array) == 0:
        return array
    lo = float(np.min(array))
    hi = float(np.max(array))
    if hi - lo <= 1e-12:
        return np.ones(len(array), dtype=np.float64) if hi > 0 else np.zeros(len(array))
    return (array - lo) / (hi - lo)


def _candidate_budget(pool_size: int) -> int:
    return min(
        MAX_CANDIDATES,
        max(MIN_CANDIDATES, int(20.0 * math.sqrt(max(1, pool_size)))),
    )


def _make_candidate(
    triplet: tuple[str, str, str],
    *,
    vectors: dict[str, np.ndarray],
    ensemble: list[np.ndarray],
    mode: str,
    focus: float = 0.0,
) -> Candidate:
    predictions = (
        _maxdiff_predictions(triplet, vectors, ensemble)
        if mode == "within"
        else _odd_predictions(triplet, vectors, ensemble)
    )
    return Candidate(
        papers=triplet,
        predictions=predictions,
        information=_information_matrix(predictions),
        eig=expected_information_gain(predictions),
        goal_risk=(_ordering_goal_risk(triplet, vectors, ensemble) if mode == "within" else 0.0),
        design=_design_score(triplet, vectors),
        focus=focus,
    )


def _select_candidate(
    candidates: list[Candidate],
    *,
    accumulated_information: np.ndarray | None,
    paper_exposure: dict[str, int],
) -> Candidate | None:
    if not candidates:
        return None
    eig = _normalise([candidate.eig for candidate in candidates])
    goal = _normalise([candidate.goal_risk for candidate in candidates])
    design = _normalise([candidate.design for candidate in candidates])
    focus = _normalise([candidate.focus for candidate in candidates])
    conditional = _normalise(
        [
            _conditional_information_gain(accumulated_information, candidate.information)
            for candidate in candidates
        ]
    )

    has_posterior = any(candidate.predictions is not None for candidate in candidates)
    scores = np.zeros(len(candidates), dtype=np.float64)
    for index, candidate in enumerate(candidates):
        if has_posterior:
            score = (
                0.50 * eig[index]
                + 0.22 * conditional[index]
                + 0.18 * goal[index]
                + 0.07 * focus[index]
                + 0.03 * design[index]
            )
        else:
            score = 0.70 * design[index] + 0.30 * focus[index]
        exposure = sum(paper_exposure.get(paper_id, 0) for paper_id in candidate.papers)
        scores[index] = score * (0.55**exposure)
    return candidates[int(np.argmax(scores))]


def _load_region_pools(
    conn: sqlite3.Connection,
    payload: dict,
    *,
    model: str,
) -> tuple[dict[int, list[str]], dict[str, int]]:
    """Every judgeable active-model paper, grouped by durable super-region."""
    cluster_to_region = {
        int(cluster_id): int(region_id)
        for cluster_id, region_id in (payload.get("cluster_to_region") or {}).items()
    }
    if not cluster_to_region:
        return {}, {}
    rows = conn.execute(
        """
        SELECT DISTINCT pc.paper_id, pc.cluster_id
        FROM publication_clusters pc
        JOIN papers p ON p.id = pc.paper_id
        JOIN publication_embeddings pe
          ON pe.paper_id = pc.paper_id AND pe.model = ?
        WHERE pc.scope = 'corpus'
          AND TRIM(COALESCE(p.title, '')) != ''
          AND (TRIM(COALESCE(p.abstract, '')) != ''
               OR TRIM(COALESCE(p.tldr, '')) != '')
        ORDER BY pc.paper_id
        """,
        (model,),
    ).fetchall()
    pools: dict[int, list[str]] = {}
    paper_regions: dict[str, int] = {}
    for row in rows:
        region_id = cluster_to_region.get(int(row["cluster_id"]))
        if region_id is None:
            continue
        paper_id = str(row["paper_id"])
        pools.setdefault(region_id, []).append(paper_id)
        paper_regions[paper_id] = region_id
    return pools, paper_regions


def _region_posterior_factors(
    pools: dict[int, list[str]],
    vectors: dict[str, np.ndarray],
    ensemble: list[np.ndarray],
) -> dict[int, float]:
    """Normalised posterior disagreement over representative region members."""
    if len(ensemble) < 2:
        return {region_id: 1.0 for region_id in pools}
    raw: dict[int, float] = {}
    for region_id, pool in pools.items():
        if not pool:
            continue
        stride = max(1, len(pool) // 96)
        sample = [paper_id for paper_id in pool[::stride][:96] if paper_id in vectors]
        if len(sample) < 2:
            raw[region_id] = 0.0
            continue
        stack = np.stack([vectors[paper_id] for paper_id in sample])
        scores = np.stack([stack @ head for head in ensemble]).astype(np.float64)
        scores -= scores.mean(axis=1, keepdims=True)
        scores /= np.maximum(scores.std(axis=1, keepdims=True), 1e-6)
        raw[region_id] = float(np.mean(np.std(scores, axis=0)))
    peak = max(raw.values(), default=0.0)
    if peak <= 0:
        return {region_id: 1.0 for region_id in pools}
    return {region_id: 0.5 + raw.get(region_id, 0.0) / peak for region_id in pools}


def _staleness(age_days: float | None) -> float:
    if age_days is None:
        return 1.5
    return 0.5 + min(1.0, age_days / STALENESS_RECOVERY_DAYS)


def _build_context(conn: sqlite3.Connection) -> QueueContext | None:
    from alma.application import materialized_views as mv
    from alma.application import super_regions as sr
    from alma.application.graph_substrate import load_vectors_by_id
    from alma.application.signal_lab import lab_tuning
    from alma.application.signal_lab.fit import MODEL_VIEW_KEY, decode_head_vector
    from alma.discovery.similarity import get_active_embedding_model

    stored = mv.get_stored(conn, sr.VIEW_KEY)
    if stored is None or not stored["payload"].get("regions"):
        return None
    payload = stored["payload"]
    model = get_active_embedding_model(conn)
    pools, paper_regions = _load_region_pools(conn, payload, model=model)
    all_ids = [paper_id for pool in pools.values() for paper_id in pool]
    vectors = load_vectors_by_id(conn, all_ids, model)
    pools = {
        region_id: [paper_id for paper_id in pool if paper_id in vectors]
        for region_id, pool in pools.items()
    }
    pools = {region_id: pool for region_id, pool in pools.items() if pool}

    utility_ensemble: list[np.ndarray] = []
    metric_ensemble: list[np.ndarray] = []
    gamma = lab_tuning(conn)["gamma_start"]
    model_stored = mv.get_stored(conn, MODEL_VIEW_KEY)
    if model_stored is not None:
        model_payload = model_stored["payload"]
        gamma = float(model_payload.get("gamma") or gamma)
        utility_ensemble = [
            decode_head_vector(encoded) for encoded in model_payload.get("ensemble_b64") or []
        ]
        metric_ensemble = [
            decode_head_vector(encoded)
            for encoded in model_payload.get("metric_ensemble_b64") or []
        ]

    rings = sr.compute_rings(conn, payload)
    adjacency = {
        int(region_id): [int(neighbour) for neighbour in neighbours]
        for region_id, neighbours in (payload.get("adjacency") or {}).items()
    }
    masses = {
        int(region["id"]): max(1, int(region.get("mass") or 1))
        for region in payload.get("regions", [])
    }
    ledger = load_ledger_evidence(
        conn,
        paper_regions=paper_regions,
        recent_limit=RECENT_QUERY_LIMIT,
    )
    tuning = lab_tuning(conn)
    posterior = _region_posterior_factors(pools, vectors, utility_ensemble)
    uncertainty: dict[int, float] = {}
    staleness: dict[int, float] = {}
    judgeability: dict[int, float] = {}
    for region_id in pools:
        history = ledger.regions.get(region_id)
        answered = history.answered if history is not None else 0
        coverage = 1.0 / (1.0 + answered / float(tuning["coverage_target"]))
        uncertainty[region_id] = coverage * posterior.get(region_id, 1.0)
        staleness[region_id] = _staleness(history.age_days if history else None)
        judgeability[region_id] = history.answerability if history else 1.0
    base_weights = region_weights(
        payload,
        rings,
        gamma=gamma,
        uncertainty=uncertainty,
        staleness=staleness,
        judgeability=judgeability,
    )
    base_weights = {
        region_id: weight
        for region_id, weight in base_weights.items()
        if len(pools.get(region_id, [])) >= MIN_POOL_FACTOR
    }
    return QueueContext(
        payload=payload,
        pools=pools,
        paper_regions=paper_regions,
        vectors=vectors,
        rings=rings,
        adjacency=adjacency,
        masses=masses,
        ledger=ledger,
        base_region_weights=base_weights,
        utility_ensemble=utility_ensemble,
        metric_ensemble=metric_ensemble,
    )


def _within_candidates(
    context: QueueContext,
    *,
    game_id: str,
    region_id: int,
    excluded: set[str],
    rng: np.random.Generator,
) -> list[Candidate]:
    pool = context.pools.get(region_id, [])
    triplets = draw_triplets(
        pool,
        rng,
        n=_candidate_budget(len(pool)),
    )
    candidates = []
    for raw in triplets:
        triplet = (str(raw[0]), str(raw[1]), str(raw[2]))
        if canonical_query_key(game_id, triplet) in excluded:
            continue
        candidates.append(
            _make_candidate(
                triplet,
                vectors=context.vectors,
                ensemble=context.utility_ensemble,
                mode="within",
            )
        )
    return candidates


def _boundary_focus_pools(
    context: QueueContext,
    region_id: int,
    pair_region_id: int,
) -> tuple[list[str], list[str], dict[str, float]]:
    """Low-margin cores plus complete pools for protected broad exploration."""
    from alma.application.graph_substrate import assign_with_margin
    from alma.application.super_regions import decode_centroid

    centroids = {
        int(region["id"]): decode_centroid(region["centroid_b64"])
        for region in context.payload.get("regions", [])
        if int(region["id"]) in (region_id, pair_region_id)
    }
    if len(centroids) != 2:
        return [], [], {}
    margins: dict[str, float] = {}
    for paper_id in context.pools.get(region_id, []) + context.pools.get(pair_region_id, []):
        vector = context.vectors.get(paper_id)
        if vector is not None:
            margins[paper_id] = float(assign_with_margin(vector, centroids).margin)

    def _core(region: int) -> list[str]:
        full = [paper_id for paper_id in context.pools.get(region, []) if paper_id in margins]
        core_size = min(len(full), max(24, int(4.0 * math.sqrt(max(1, len(full))))))
        return sorted(full, key=lambda paper_id: (margins[paper_id], paper_id))[:core_size]

    return _core(region_id), _core(pair_region_id), margins


def _boundary_candidates(
    context: QueueContext,
    *,
    game_id: str,
    region_id: int,
    pair_region_id: int,
    excluded: set[str],
    rng: np.random.Generator,
) -> list[Candidate]:
    pool_r = context.pools.get(region_id, [])
    pool_s = context.pools.get(pair_region_id, [])
    if len(pool_r) < 2 or not pool_s:
        return []
    core_r, core_s, margins = _boundary_focus_pools(context, region_id, pair_region_id)
    if len(core_r) < 2 or not core_s:
        return []

    target = min(
        MAX_CANDIDATES,
        max(MIN_CANDIDATES, int(16.0 * math.sqrt(len(pool_r) + len(pool_s)))),
    )
    triplets: set[tuple[str, str, str]] = set()
    attempts = 0
    while len(triplets) < target and attempts < target * 14:
        attempts += 1
        # Most candidates target edge ambiguity; 25% draw from full pools so
        # a static centroid mistake cannot trap acquisition forever.
        focused = float(rng.random()) >= 0.25
        source_r = core_r if focused else pool_r
        source_s = core_s if focused else pool_s
        if len(source_r) < 2 or not source_s:
            continue
        r_indices = rng.choice(len(source_r), size=2, replace=False)
        s_index = int(rng.integers(len(source_s)))
        papers = tuple(
            sorted(
                (
                    source_r[int(r_indices[0])],
                    source_r[int(r_indices[1])],
                    source_s[s_index],
                )
            )
        )
        if canonical_query_key(game_id, papers) not in excluded:
            triplets.add(papers)

    candidates = []
    for triplet in sorted(triplets):
        mean_margin = float(np.mean([margins.get(paper_id, 1.0) for paper_id in triplet]))
        candidates.append(
            _make_candidate(
                triplet,
                vectors=context.vectors,
                ensemble=context.metric_ensemble,
                mode="boundary",
                focus=1.0 / (1e-6 + mean_margin),
            )
        )
    return candidates


def _ordered_boundary_neighbours(
    context: QueueContext,
    *,
    region_id: int,
    selected_edges: dict[tuple[int, int], int],
    coverage_target: int,
    rng: np.random.Generator,
) -> list[int]:
    neighbours = [
        neighbour
        for neighbour in context.adjacency.get(region_id, [])
        if len(context.pools.get(neighbour, [])) >= 1
    ]
    weighted: list[tuple[int, float]] = []
    for neighbour in neighbours:
        key = edge_key(region_id, neighbour)
        history = context.ledger.edges.get(key, EdgeEvidence())
        prior_variance = 1.0 / 12.0
        uncertainty = 0.5 + history.posterior_variance / prior_variance
        coverage = 1.0 / (1.0 + history.answered / float(coverage_target))
        mass = (context.masses.get(region_id, 1) + context.masses.get(neighbour, 1)) ** 0.5
        diversity = 1.0 / (1.0 + selected_edges.get(key, 0))
        weight = (
            mass
            * uncertainty
            * coverage
            * _staleness(history.age_days)
            * history.answerability
            * diversity
        )
        weighted.append((neighbour, max(weight, 1e-12)))

    ordered: list[int] = []
    remaining = weighted[:]
    while remaining:
        probs = np.asarray([weight for _, weight in remaining], dtype=np.float64)
        probs /= probs.sum()
        index = int(rng.choice(len(remaining), p=probs))
        neighbour, _ = remaining.pop(index)
        ordered.append(neighbour)
    return ordered


def build_queue(
    conn: sqlite3.Connection,
    *,
    game_id: str,
    count: int,
    k: int = 3,
    region_mode: str = "within",
    rng: np.random.Generator | None = None,
) -> list[dict]:
    """Build one diverse zero-write deck under current posterior and ledger."""
    from alma.application.signal_lab import lab_tuning

    if k != 3:
        raise ValueError("Signal Lab acquisition currently requires k=3")
    context = _build_context(conn)
    if context is None:
        return []
    tuning = lab_tuning(conn)
    rng = rng or np.random.default_rng()
    excluded = set(context.ledger.recent_queries)
    selected_regions: dict[int, int] = {}
    selected_edges: dict[tuple[int, int], int] = {}
    paper_exposure: dict[str, int] = {}
    accumulated_information: np.ndarray | None = None
    out: list[dict] = []
    exhausted_regions: set[int] = set()

    for _ in range(count):
        chosen_spec: dict | None = None
        for _attempt in range(max(12, len(context.base_region_weights) * 2)):
            effective_weights = {
                region_id: weight / (1.0 + selected_regions.get(region_id, 0))
                for region_id, weight in context.base_region_weights.items()
                if region_id not in exhausted_regions
            }
            choice = choose_region(
                effective_weights,
                context.rings,
                rng,
                epsilon=tuning["epsilon"],
            )
            if choice is None:
                break

            pair_region_id: int | None = None
            if region_mode == "boundary":
                candidates: list[Candidate] = []
                for neighbour in _ordered_boundary_neighbours(
                    context,
                    region_id=choice.region_id,
                    selected_edges=selected_edges,
                    coverage_target=tuning["coverage_target"],
                    rng=rng,
                ):
                    candidates = _boundary_candidates(
                        context,
                        game_id=game_id,
                        region_id=choice.region_id,
                        pair_region_id=neighbour,
                        excluded=excluded,
                        rng=rng,
                    )
                    if candidates:
                        pair_region_id = neighbour
                        break
            else:
                candidates = _within_candidates(
                    context,
                    game_id=game_id,
                    region_id=choice.region_id,
                    excluded=excluded,
                    rng=rng,
                )

            selected = _select_candidate(
                candidates,
                accumulated_information=accumulated_information,
                paper_exposure=paper_exposure,
            )
            if selected is None:
                exhausted_regions.add(choice.region_id)
                continue

            shown = list(selected.papers)
            rng.shuffle(shown)
            chosen_spec = {
                "shown": shown,
                "region_id": choice.region_id,
                "pair_region_id": pair_region_id,
                "region_version": int(context.payload.get("version") or 1),
                "ring": choice.ring,
                "explored": choice.explored,
            }
            excluded.add(canonical_query_key(game_id, selected.papers))
            selected_regions[choice.region_id] = selected_regions.get(choice.region_id, 0) + 1
            for paper_id in selected.papers:
                paper_exposure[paper_id] = paper_exposure.get(paper_id, 0) + 1
            if pair_region_id is not None:
                key = edge_key(choice.region_id, pair_region_id)
                selected_edges[key] = selected_edges.get(key, 0) + 1
            if selected.information is not None:
                accumulated_information = (
                    selected.information
                    if accumulated_information is None
                    else accumulated_information + selected.information
                )
            break

        if chosen_spec is None:
            break
        out.append(chosen_spec)
    return out
