"""Does the ranker's order predict what the user actually did?

Everything else about scoring is correct by construction (inputs fire, the
calibration is derived, replay is exact). This is the measurement: papers the
user later kept or rated up should outrank papers they ignored or rejected.

**Design (Task 67 §14.5).**

* *Temporal holdout.* The taste profile is built only from Library papers
  added BEFORE a cutoff (the median ``added_at``); the papers being judged are
  positives and negatives that are NOT in that profile. A saved paper never
  scores high by being inside its own profile.
* *The real path.* Test papers are measured by ``measure_corpus_papers`` and
  ranked by ``repaired_prior_score`` — the functions a refresh uses.
* *Three bars.* Positives against (a) a seeded random corpus sample — can it
  find taste at all; (b) recommendations shown and left alone; (c) papers
  explicitly rejected — the hard one, since both sides were surfaced.
* *AUC with a bootstrap interval.* Pairwise ranking accuracy is the right
  question for a ranker and is defined at small n; the interval says when a
  number means nothing. A comparison under ``MIN_GROUP`` papers, or whose
  interval contains 0.5, is reported **inconclusive**, never as a result.
* *What earns its weight.* Each family alone (its value as the score), and
  the full score with that family's weight removed.

**Limits, stated so nobody over-reads this.** Offline papers have no
retrieval evidence and no citation-fabric entry, so ``retrieval`` and the
graph atoms of ``citation`` cannot be evaluated here. ``feedback`` and
``preference`` read the all-time signal projection, which includes events on
the very papers under test: they are flagged leak-prone and a *content-only*
score (their weights at zero) is reported beside the full one. The percentile
tables are the stored ones, measured under today's profile — monotone, so they
cannot reorder a family, but not re-derived for the cutoff. Positions shown
to the user were chosen by an earlier ranker (position bias is not corrected).
"""

from __future__ import annotations

import random
import sqlite3
from typing import Any

import numpy as np

from alma.ai.graph_versions import with_version
from alma.application import materialized_views as mv

EVAL_VERSION = "2026.09-2"  # -2: Signal Lab head evidence block
MIN_PROFILE = 20  # profile papers needed before the cutoff
MIN_GROUP = 15  # papers per side before a comparison is reported as a result
RANDOM_SAMPLE = 600
BOOTSTRAP_DRAWS = 1000
LEAK_PRONE = ("feedback", "preference")
OFFLINE_UNAVAILABLE = ("retrieval",)


def auc(positives: list[float], negatives: list[float]) -> float | None:
    """P(a positive outranks a negative); ties count half. ``None`` if a side is empty."""
    if not positives or not negatives:
        return None
    pos = np.asarray(positives, dtype=np.float64)[:, None]
    neg = np.asarray(negatives, dtype=np.float64)[None, :]
    return float(((pos > neg).sum() + 0.5 * (pos == neg).sum()) / (pos.size * neg.size))


def auc_with_interval(
    positives: list[float], negatives: list[float], *, seed: int = 0
) -> dict[str, Any]:
    """AUC, a seeded 95% bootstrap interval, and an honest verdict."""
    value = auc(positives, negatives)
    out: dict[str, Any] = {"n_pos": len(positives), "n_neg": len(negatives), "auc": value}
    if value is None:
        return {**out, "verdict": "no_data"}
    rng = np.random.default_rng(seed)
    pos, neg = np.asarray(positives), np.asarray(negatives)
    draws = [
        auc(list(rng.choice(pos, pos.size)), list(rng.choice(neg, neg.size)))
        for _ in range(BOOTSTRAP_DRAWS)
    ]
    low, high = (float(x) for x in np.percentile(draws, [2.5, 97.5]))
    if min(len(positives), len(negatives)) < MIN_GROUP or low <= 0.5 <= high:
        verdict = "inconclusive"
    else:
        verdict = "predicts" if value > 0.5 else "anti_predicts"
    return {**out, "auc": round(value, 4), "ci95": [round(low, 4), round(high, 4)], "verdict": verdict}


def auc_delta_with_interval(
    base: tuple[list[float], list[float]], alt: tuple[list[float], list[float]], *, seed: int = 0
) -> dict[str, Any]:
    """``AUC(alt) − AUC(base)`` over the SAME papers, with a paired bootstrap.

    Two AUC intervals that overlap say nothing about their difference: both
    rankings are scored on the same papers, so most of their noise is shared.
    Resampling the papers once per draw and scoring both rankings on that draw
    keeps the shared part out of the interval. ``base[i]`` and ``alt[i]`` must
    be the same paper under two rankings. The verdict is ``improves`` /
    ``worsens`` only when the interval excludes 0.
    """
    (base_pos, base_neg), (alt_pos, alt_neg) = base, alt
    if len(base_pos) != len(alt_pos) or len(base_neg) != len(alt_neg):
        raise ValueError("paired comparison needs the same papers under both rankings")
    before, after = auc(base_pos, base_neg), auc(alt_pos, alt_neg)
    out: dict[str, Any] = {"n_pos": len(base_pos), "n_neg": len(base_neg)}
    if before is None or after is None:
        return {**out, "delta": None, "verdict": "no_data"}
    bp, bn, ap, an = (np.asarray(x, dtype=np.float64) for x in (base_pos, base_neg, alt_pos, alt_neg))
    rng = np.random.default_rng(seed)
    draws = []
    for _ in range(BOOTSTRAP_DRAWS):
        i, j = rng.integers(0, bp.size, bp.size), rng.integers(0, bn.size, bn.size)
        draws.append(auc(list(ap[i]), list(an[j])) - auc(list(bp[i]), list(bn[j])))
    low, high = (float(x) for x in np.percentile(draws, [2.5, 97.5]))
    if min(bp.size, bn.size) < MIN_GROUP or low <= 0.0 <= high:
        verdict = "no_measurable_effect"
    else:
        verdict = "improves" if after > before else "worsens"
    return {**out, "delta": round(after - before, 4), "ci95": [round(low, 4), round(high, 4)], "verdict": verdict}


FIT_FOLDS = 5
FIT_L2 = (0.001, 0.01, 0.1, 1.0)


def _family_matrix(rewards: list[dict], keys: list[str], calibration) -> np.ndarray:
    """Family values as the ranker reads them: unavailable → the corpus prior."""
    from alma.application.discovery.ranker import FAMILY_SPECS, _family_reading

    specs = {spec.key: spec for spec in FAMILY_SPECS}
    rows = []
    for reward in rewards:
        row = []
        for key in keys:
            value, available, _ = _family_reading(reward, specs[key], calibration)
            row.append(float(value) if available else float(calibration.prior(key, specs[key].prior_mean)))
        rows.append(row)
    return np.asarray(rows, dtype=np.float64)


def _fit_nonnegative_logistic(x: np.ndarray, y: np.ndarray, sw: np.ndarray, l2: float) -> np.ndarray:
    """Weighted logistic regression with w ≥ 0 (a family can be ignored, never
    inverted) and a free intercept. Coefficients act on raw family values in
    [0, 1] — the same scale the ranker multiplies — so they ARE weights."""
    from scipy.optimize import minimize

    def loss(theta: np.ndarray) -> tuple[float, np.ndarray]:
        w, b = theta[:-1], theta[-1]
        z = x @ w + b
        p = 1.0 / (1.0 + np.exp(-z))
        eps = 1e-12
        value = -np.sum(sw * (y * np.log(p + eps) + (1 - y) * np.log(1 - p + eps))) + l2 * np.sum(w * w)
        grad_z = sw * (p - y)
        return float(value), np.concatenate([x.T @ grad_z + 2 * l2 * w, [grad_z.sum()]])

    start = np.concatenate([np.full(x.shape[1], 0.1), [0.0]])
    bounds = [(0.0, None)] * x.shape[1] + [(None, None)]
    result = minimize(loss, start, jac=True, method="L-BFGS-B", bounds=bounds)
    return result.x[:-1]


def fit_family_weights(
    rewards_by_group: dict[str, list[dict]], current: dict[str, float], calibration
) -> dict[str, Any]:
    """Weights that best separate kept papers from rejected AND from the corpus,
    judged out of sample.

    Only families that can be measured honestly offline are fitted; the
    retrieval family (no evidence offline) and the leak-prone pair keep their
    current share. Both bars count equally: rejected papers and the random
    corpus each carry half of the negative mass, so 600 random papers cannot
    drown 40 real verdicts. Selection is by 5-fold cross-validated AUC (mean
    of the two bars); the L2 strength is chosen the same way.
    """
    held = [k for k in current if k in LEAK_PRONE or k in OFFLINE_UNAVAILABLE]
    keys = [k for k in current if k not in held]
    pos = rewards_by_group.get("positive") or []
    rej = rewards_by_group.get("negative") or []
    rnd = rewards_by_group.get("random_corpus") or []
    if min(len(pos), len(rej), len(rnd)) < MIN_GROUP:
        return {"ready": False, "reason": "too few judged papers to fit weights"}

    x = np.vstack([_family_matrix(g, keys, calibration) for g in (pos, rej, rnd)])
    y = np.concatenate([np.ones(len(pos)), np.zeros(len(rej) + len(rnd))])
    group = np.concatenate([np.zeros(len(pos)), np.ones(len(rej)), np.full(len(rnd), 2)])
    sw = np.where(group == 0, 1.0 / len(pos), np.where(group == 1, 0.5 / len(rej), 0.5 / len(rnd)))

    rng = np.random.default_rng(0)
    fold = np.empty(len(y), dtype=int)
    for g in (0, 1, 2):  # stratified by group
        idx = np.flatnonzero(group == g)
        rng.shuffle(idx)
        fold[idx] = np.arange(len(idx)) % FIT_FOLDS

    def bars(scores: np.ndarray, mask: np.ndarray) -> tuple[float, float]:
        p = list(scores[mask & (group == 0)])
        return tuple(auc(p, list(scores[mask & (group == g)])) for g in (1, 2))  # rejected, random

    def cv_bars(weight_fn) -> tuple[float, float]:
        per_fold = [bars(x @ weight_fn(fold != k), fold == k) for k in range(FIT_FOLDS)]
        return float(np.mean([b[0] for b in per_fold])), float(np.mean([b[1] for b in per_fold]))

    def cv(weight_fn) -> float:
        return float(np.mean(cv_bars(weight_fn)))

    current_vec = np.asarray([current[k] for k in keys])
    semantic_vec = np.asarray([1.0 if k == "semantic" else 0.0 for k in keys])
    fitted_cv = {l2: cv(lambda train, l2=l2: _fit_nonnegative_logistic(x[train], y[train], sw[train], l2)) for l2 in FIT_L2}
    best_l2 = max(fitted_cv, key=fitted_cv.get)
    w = _fit_nonnegative_logistic(x, y, sw, best_l2)
    if w.sum() <= 0:
        return {"ready": False, "reason": "fit found no positive weight"}

    budget = 1.0 - sum(current[k] for k in held)  # what the fitted families share
    fitted = {k: round(float(v / w.sum() * budget), 4) for k, v in zip(keys, w, strict=True)}

    # Candidates scored on the SAME folds, per bar. A sparse optimum from one
    # library is evidence of direction, not a default to ship: the half-way
    # blend toward the current weights is scored beside it.
    def fit_on(train: np.ndarray) -> np.ndarray:
        v = _fit_nonnegative_logistic(x[train], y[train], sw[train], best_l2)
        return v / v.sum() * budget if v.sum() > 0 else v

    def blend_on(train: np.ndarray) -> np.ndarray:
        return 0.5 * fit_on(train) + 0.5 * current_vec / current_vec.sum() * budget

    def no_citation_on(train: np.ndarray) -> np.ndarray:
        v = fit_on(train).copy()
        v[keys.index("semantic")] += v[keys.index("citation")]
        v[keys.index("citation")] = 0.0
        return v

    candidates = {
        "current": cv_bars(lambda _t: current_vec),
        "fitted": cv_bars(fit_on),
        "blend_half": cv_bars(blend_on),
        "fitted_citation_to_semantic": cv_bars(no_citation_on),
        "semantic_only": cv_bars(lambda _t: semantic_vec),
    }
    return {
        "ready": True,
        "fitted_families": keys,
        "held_at_current": {k: round(current[k], 4) for k in held},
        "weights": {**fitted, **{k: round(current[k], 4) for k in held}},
        "l2": best_l2,
        "cv_auc": {
            "fitted": round(fitted_cv[best_l2], 4),
            "current": round(cv(lambda _train: current_vec), 4),
            "semantic_only": round(cv(lambda _train: semantic_vec), 4),
        },
        "cv_by_bar": {
            name: {"vs_rejected": round(a, 4), "vs_random": round(b, 4)} for name, (a, b) in candidates.items()
        },
        "blend_half": {
            k: round(0.5 * fitted[k] + 0.5 * float(current[k] / current_vec.sum() * budget), 4) for k in keys
        },
        "n": {"kept": len(pos), "rejected": len(rej), "random": len(rnd)},
    }


def _holdout_profile(
    conn: sqlite3.Connection,
) -> tuple[str, list[dict], list[dict], set[str]] | None:
    """Cutoff, the taste inputs that existed before it, and every Library paper
    known before it (none of which may be a test item, whatever its rating)."""
    from alma.application.discovery import load_library_preference_inputs

    library_pubs, positive_pubs, negative_pubs = load_library_preference_inputs(conn)
    dated = sorted(str(p.get("added_at") or "") for p in library_pubs if p.get("added_at"))
    if len(dated) < 2 * MIN_PROFILE:
        return None
    cutoff = dated[len(dated) // 2]

    def before(pubs: list[dict]) -> list[dict]:
        return [p for p in pubs if str(p.get("added_at") or "") and str(p["added_at"]) < cutoff]

    known = {str(p["id"]) for p in before(library_pubs)}
    return cutoff, before(positive_pubs), before(negative_pubs), known


def evaluate_ranker_outcomes(conn: sqlite3.Connection) -> dict[str, Any]:
    """Run the evaluation. Read-only; heavy (scores ~1k papers) — job or script."""
    from alma.application.discovery.calibration import load_calibration
    from alma.application.discovery.offline_measure import (
        build_profile_inputs,
        measure_corpus_papers,
    )
    from alma.application.discovery.ranker import (
        FAMILY_SPECS,
        LAB_ADJUSTMENTS,
        _family_reading,
        repaired_prior_score,
        resolve_family_weights,
        resolve_lab_points,
    )
    from alma.application.recommendation_outcomes import (
        build_paper_outcome_map,
        build_recommendation_outcomes,
    )
    from alma.application.signal_lab.scoring_terms import load_lab_scoring_context
    from alma.discovery.defaults import LAB_HEAD_MAX_POINTS

    holdout = _holdout_profile(conn)
    if holdout is None:
        return {"ready": False, "reason": f"fewer than {2 * MIN_PROFILE} dated Library papers"}
    cutoff, positive_pubs, negative_pubs, known_before = holdout
    profile_ids = {str(p["id"]) for p in positive_pubs} | {str(p["id"]) for p in negative_pubs}

    outcomes = build_paper_outcome_map(conn)
    groups: dict[str, list[str]] = {
        "positive": [pid for pid, o in outcomes.items() if o.classification == "positive" and pid not in known_before],
        "negative": [pid for pid, o in outcomes.items() if o.classification == "negative" and pid not in known_before],
    }
    judged = set(outcomes) | known_before
    # "Shown" means an actual item impression (the outcome owner's contract): a
    # recommendation row with no action is mostly the CURRENT deck — papers this
    # very ranker just selected and the user has not seen — which would score
    # high by construction and say nothing about the ranker.
    groups["shown_ignored"] = sorted(
        {
            r.paper_id
            for r in build_recommendation_outcomes(conn)
            if r.classification == "neutral" and r.impression_at
        }
        - judged
    )
    inputs = build_profile_inputs(conn, positive_pubs, negative_pubs, scope_paper_ids=profile_ids)
    corpus = [
        str(r[0])
        for r in conn.execute(
            """SELECT p.id FROM papers p JOIN publication_embeddings pe ON pe.paper_id = p.id
               WHERE p.status NOT IN ('library', 'dismissed', 'removed') AND pe.model = ?
               ORDER BY p.id""",
            (inputs.model,),
        )
        if str(r[0]) not in judged
    ]
    random.Random(f"{EVAL_VERSION}:{len(corpus)}").shuffle(corpus)
    groups["random_corpus"] = corpus[:RANDOM_SAMPLE]

    calibration = load_calibration(conn)
    # The Lab's RAW inputs are measured even on an install whose heads sit at 0
    # points (the context loader returns nothing when every head is off), so
    # "what would the Lab add here?" has an answer before anyone turns it on.
    # The configured points still decide the full score below.
    lab_ctx = load_lab_scoring_context(
        conn, {**inputs.settings, **{spec.weight_setting: str(LAB_HEAD_MAX_POINTS) for spec in LAB_ADJUSTMENTS}}
    )
    weights = resolve_family_weights(inputs.settings)
    lab_points = resolve_lab_points(inputs.settings)
    no_lab = {key: 0.0 for key in lab_points}
    content_weights = {k: (0.0 if k in LEAK_PRONE else v) for k, v in weights.items()}

    # One measurement per paper; every variant below is a re-ranking of it.
    rewards: dict[str, dict] = {}
    all_ids = sorted({pid for ids in groups.values() for pid in ids})
    for pid, _breakdown, reward in measure_corpus_papers(
        conn, all_ids, inputs, calibration=calibration, lab_ctx=lab_ctx
    ):
        rewards[pid] = reward

    def scores(ids: list[str], *, w: dict[str, float], lab: dict[str, float]) -> list[float]:
        return [
            repaired_prior_score(rewards[p], weights=w, lab_points=lab, calibration=calibration)[0]
            for p in ids
            if p in rewards
        ]

    def family_values(ids: list[str], spec) -> list[float]:
        out = []
        for p in ids:
            if p in rewards:
                value, available, _ = _family_reading(rewards[p], spec, calibration)
                if available:
                    out.append(float(value))
        return out

    comparisons: dict[str, Any] = {}
    for against in ("random_corpus", "shown_ignored", "negative"):
        pos_ids, neg_ids = groups["positive"], groups[against]
        full = auc_with_interval(scores(pos_ids, w=weights, lab=lab_points), scores(neg_ids, w=weights, lab=lab_points))
        block: dict[str, Any] = {
            "full_score": full,
            "content_only": auc_with_interval(
                scores(pos_ids, w=content_weights, lab=no_lab), scores(neg_ids, w=content_weights, lab=no_lab)
            ),
            "lab_off": auc_with_interval(scores(pos_ids, w=weights, lab=no_lab), scores(neg_ids, w=weights, lab=no_lab)),
            "families": {},
        }
        for spec in FAMILY_SPECS:
            if spec.key in OFFLINE_UNAVAILABLE:
                continue
            alone = auc_with_interval(family_values(pos_ids, spec), family_values(neg_ids, spec))
            without = {k: (0.0 if k == spec.key else v) for k, v in weights.items()}
            ablated = auc(scores(pos_ids, w=without, lab=lab_points), scores(neg_ids, w=without, lab=lab_points))
            block["families"][spec.key] = {
                "weight": round(weights.get(spec.key, 0.0), 4),
                "alone": alone,
                "loss_when_removed": (
                    round(full["auc"] - ablated, 4) if full.get("auc") is not None and ablated is not None else None
                ),
                "leak_prone": spec.key in LEAK_PRONE,
            }
        comparisons[against] = block

    fit = fit_family_weights(
        {
            name: [(rewards[p]) for p in ids if p in rewards]
            for name, ids in groups.items()
            if name in ("positive", "negative", "random_corpus")
        },
        weights,
        calibration,
    )

    # What-if weightings: the same measurements re-ranked, to size a finding
    # before anyone touches a default. Evidence, not a recommendation engine.
    def what_if(w: dict[str, float]) -> dict[str, Any]:
        return {
            against: auc_with_interval(
                scores(groups["positive"], w=w, lab=lab_points), scores(groups[against], w=w, lab=lab_points)
            )
            for against in ("random_corpus", "negative")
        }

    scenarios = {
        "author_zero": {k: (0.0 if k == "author" else v) for k, v in weights.items()},
        "author_to_semantic": {
            k: (0.0 if k == "author" else v + weights.get("author", 0.0) if k == "semantic" else v)
            for k, v in weights.items()
        },
        "semantic_only": {k: (1.0 if k == "semantic" else 0.0) for k in weights},
        "shipped_defaults": resolve_family_weights(None),
    }

    lab = _lab_evidence(
        conn,
        groups=groups,
        rewards=rewards,
        rank=lambda ids, points: scores(ids, w=weights, lab=points),
        configured=lab_points,
        max_points=LAB_HEAD_MAX_POINTS,
        adjustments=LAB_ADJUSTMENTS,
    )

    return {
        "ready": True,
        "version": EVAL_VERSION,
        "lab": lab,
        "what_if": {name: what_if(w) for name, w in scenarios.items()},
        "fit": fit,
        "cutoff": cutoff,
        "profile": {"positives": len(positive_pubs), "negatives": len(negative_pubs)},
        "groups": {name: len([p for p in ids if p in rewards]) for name, ids in groups.items()},
        "calibrated": bool(getattr(calibration, "calibrated", False)),
        "lab_active": lab_ctx is not None,
        "comparisons": comparisons,
        "limits": {
            "offline_unavailable": list(OFFLINE_UNAVAILABLE) + ["citation graph atoms"],
            "leak_prone_families": list(LEAK_PRONE),
            "position_bias": "not corrected",
        },
    }


def _lab_evidence(
    conn: sqlite3.Connection,
    *,
    groups: dict[str, list[str]],
    rewards: dict[str, dict],
    rank,
    configured: dict[str, float],
    max_points: float,
    adjustments,
) -> dict[str, Any]:
    """Do the Signal Lab heads improve the order — and could they, yet?

    Each additive head is re-ranked alone and together, at its configured
    points and at the ceiling, over the same measurements. Two honesty rules:

    * a test paper that was SHOWN in a Lab round is left out — the head was
      fitted on the user's answer about that very paper;
    * ``reach`` says how many test papers a head moves at all. A head that
      touches a handful of papers cannot change an AUC, and that is a fact
      about how much has been played, not about whether the head works.

    The categorical heads (author, venue) are not re-ranked here: they fold
    into their family's affinity before measurement, so their reach into the
    score is that family's weight — reported by the caller's family table.
    """
    from alma.application.discovery.ranker import _signed_reading
    from alma.application.signal_lab.rounds import load_rounds

    rounds = load_rounds(conn)
    answered = [r for r in rounds if r.answer is not None and not r.skipped]
    shown = {pid for r in rounds for pid in r.shown}
    clean = {name: [p for p in ids if p in rewards and p not in shown] for name, ids in groups.items()}
    heads = {spec.key: spec.atom_key for spec in adjustments}
    off = {key: 0.0 for key in heads}

    judged = sorted({p for ids in clean.values() for p in ids})
    reach = {
        key: round(sum(1 for p in judged if abs(_signed_reading(rewards[p], atom)[0]) > 1e-9) / len(judged), 4)
        if judged
        else 0.0
        for key, atom in heads.items()
    }

    scenarios: dict[str, dict[str, float]] = {"off": off, "as_configured": dict(configured)}
    for key in heads:
        scenarios[f"{key}_only_max"] = {**off, key: max_points}
    scenarios["all_max"] = {key: max_points for key in heads}

    bars: dict[str, Any] = {}
    for against in ("negative", "random_corpus"):
        pos, neg = clean["positive"], clean[against]
        ranked = {name: (rank(pos, pts), rank(neg, pts)) for name, pts in scenarios.items()}
        bars[against] = {name: auc_with_interval(*pair) for name, pair in ranked.items()}
        # The question a reader has: versus no Lab at all, on the same papers.
        bars[against]["vs_off"] = {
            name: auc_delta_with_interval(ranked["off"], ranked[name]) for name in ("as_configured", "all_max")
        }

    return {
        "rounds_answered": len(answered),
        "rounds_by_game": {g: sum(1 for r in answered if r.game_id == g) for g in sorted({r.game_id for r in answered})},
        "test_papers_excluded_as_shown": len({p for ids in groups.values() for p in ids if p in shown}),
        "configured_points": {k: float(v) for k, v in configured.items()},
        "reach": reach,
        "bars": bars,
    }


# ── stored view ────────────────────────────────────────────────────────────
#
# The evaluation scores ~1k papers, so no request computes it. It is a stored
# view: a GET reads the row (`load_outcome_summary`), the periodic scoring tick
# and a weights save ask for a refresh (`request_outcome_eval_refresh`), and the
# shared view worker builds it in the background.

EVAL_VIEW_KEY = "scoring:outcome_eval"

# The answer changes when what is judged changes (Library, verdicts — bucketed
# like the calibration, so one save does not re-score a thousand papers), when
# the weights being judged change, or when the calibration they are read
# through is rebuilt. The calibration row's own fingerprint stands in for the
# embedding set and model.
_FINGERPRINT_SQL = with_version(
    """
    SELECT (SELECT COUNT(*) / 5 FROM papers WHERE status = 'library'),
           (SELECT COUNT(*) / 10 FROM feedback_events),
           (SELECT COALESCE(GROUP_CONCAT(key || '=' || value, ';'), '')
              FROM (SELECT key, value FROM discovery_settings
                     WHERE key LIKE 'weights.%' ORDER BY key)),
           (SELECT COALESCE(MAX(fingerprint), '') FROM materialized_views
             WHERE view_key = 'scoring:calibration')
    """,
    EVAL_VERSION,
)

mv.register(
    mv.View(
        key=EVAL_VIEW_KEY,
        fingerprint_sql=_FINGERPRINT_SQL,
        build_fn=evaluate_ranker_outcomes,
        operation_key="materialize.scoring.outcome_eval",
    )
)

#: Summary bars, hardest first: both sides of "rejected" were surfaced to the
#: user; "random_corpus" only asks whether the ranker finds taste at all.
SUMMARY_BARS = ("negative", "random_corpus")


def request_outcome_eval_refresh(conn: sqlite3.Connection, *, force: bool = False) -> str | None:
    """Ask for a background re-evaluation if its inputs moved (or ``force``).

    Returns the active/new job id, or ``None`` when the stored result is
    current. Never computes here.
    """
    return mv.request_refresh(conn, EVAL_VIEW_KEY, force=force)


def load_outcome_summary(conn: sqlite3.Connection) -> dict[str, Any]:
    """The stored evaluation, reduced to what a surface states. Pure row read.

    ``state`` is ``not_built`` (never evaluated), ``not_ready`` (too little
    history — ``reason`` says what is missing) or ``ready``. ``current`` is
    False when the inputs moved since the stored run.
    """
    envelope = mv.get_stored(conn, EVAL_VIEW_KEY)
    if envelope is None:
        return {"state": "not_built", "rebuilding": mv.is_rebuilding(EVAL_VIEW_KEY)}
    payload = envelope.get("payload") or {}
    base = {
        "computed_at": envelope.get("computed_at"),
        "rebuilding": bool(envelope.get("rebuilding")),
        # False when the weights, the Library or the calibration moved since:
        # the numbers then describe an earlier ranker and the surface says so.
        "current": mv.is_current(conn, EVAL_VIEW_KEY),
    }
    if not payload.get("ready"):
        return {**base, "state": "not_ready", "reason": payload.get("reason")}
    comparisons = payload.get("comparisons") or {}
    return {
        **base,
        "state": "ready",
        "cutoff": payload.get("cutoff"),
        "bars": {
            bar: (comparisons.get(bar) or {}).get("full_score")
            for bar in SUMMARY_BARS
            if (comparisons.get(bar) or {}).get("full_score")
        },
        "lab": _lab_summary(payload.get("lab")),
    }


def _lab_summary(lab: dict[str, Any] | None) -> dict[str, Any] | None:
    """What the Signal Lab card states: played how much, changed the order how."""
    if not lab:
        return None
    return {
        "rounds_answered": lab.get("rounds_answered"),
        "configured_points": lab.get("configured_points"),
        "vs_off": {bar: ((lab.get("bars") or {}).get(bar) or {}).get("vs_off") for bar in SUMMARY_BARS},
    }
