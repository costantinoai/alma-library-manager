"""Auditable multivariate rankers for Discovery.

Production uses a repaired family-level hand prior.  The ridge implementation
is a shadow challenger until enough immutable v3 observations exist.
"""

from __future__ import annotations

import math
from dataclasses import dataclass

from .features import build_feature_snapshot, flatten_reward_features

RANKER_VERSION = "discovery-v3-family-prior"
SHADOW_VERSION = "discovery-v3-prior-centered-ridge-shadow"
SHADOW_MIN_OBSERVATIONS = 80
SHADOW_MIN_PER_CLASS = 20

# Families sum to one.  Each composite is built from atomic snapshot values;
# the old usefulness composite and source/lane calibration are excluded.
_PRIOR_WEIGHTS = {
    "retrieval": 0.13,
    "semantic": 0.20,
    "lexical": 0.08,
    "topic": 0.14,
    "author": 0.10,
    "venue": 0.05,
    "recency": 0.08,
    "citation": 0.08,
    "feedback": 0.08,
    "preference": 0.06,
}


def _clip(value: float, low: float = 0.0, high: float = 1.0) -> float:
    return max(low, min(high, float(value)))


def _value(features: dict[str, float], name: str) -> float:
    return float(features.get(name, 0.0) or 0.0)


def _snapshot_value(
    snapshot: dict,
    name: str,
    *,
    neutral: float = 0.0,
) -> float:
    detail = snapshot.get(name) or {}
    if not isinstance(detail, dict) or not detail.get("availability"):
        return neutral
    return float(detail.get("value") or 0.0)


def prior_family_values(snapshot: dict) -> dict[str, float]:
    """Project wide immutable atoms into ten non-duplicated model families."""

    f = flatten_reward_features(snapshot)

    def measured(name: str, neutral: float = 0.5) -> float:
        return _snapshot_value(snapshot, name, neutral=neutral)

    return {
        "retrieval": max(
            _value(f, "retrieval_rrf_lexical"),
            _value(f, "retrieval_rrf_semantic"),
            _value(f, "retrieval_rrf_citation"),
            _value(f, "retrieval_rrf_taste"),
        )
        * 0.75
        + _clip(_value(f, "retrieval_family_count") / 4.0) * 0.25,
        "semantic": _clip(
            max(
                measured("semantic_similarity_centroid_raw"),
                measured("semantic_similarity_exemplar_raw"),
                measured("semantic_similarity_support_raw"),
            )
            - 0.5 * measured("semantic_similarity_negative_raw", 0.0)
        ),
        "lexical": _clip(
            0.45 * measured("lexical_similarity_word_raw")
            + 0.35 * measured("lexical_similarity_char_raw")
            + 0.20 * measured("lexical_similarity_term_raw")
            - 0.5 * measured("lexical_similarity_negative_penalty", 0.0)
        ),
        "topic": _clip(measured("topic_score")),
        "author": _clip(measured("author_affinity")),
        "venue": _clip(measured("journal_affinity")),
        "recency": _clip(measured("recency_boost")),
        # Coupling and co-citation remain atomic in the snapshot.  The prior
        # uses maxima, not sums, to avoid double-paying correlated graph views.
        "citation": _clip(
            0.50 * measured("citation_quality")
            + 0.10 * _clip(measured("fwci") / 3.0)
            + 0.20
            * max(
                measured("coupling_strength"),
                measured("cocitation_strength"),
            )
            + 0.20
            * max(
                measured("ppr_library_raw"),
                measured("ppr_loved_raw"),
            )
        ),
        "feedback": _clip(measured("feedback_adj")),
        "preference": _clip(measured("preference_affinity")),
    }


def repaired_prior_score(
    snapshot: dict,
    *,
    weights: dict[str, float] | None = None,
) -> tuple[float, dict[str, float]]:
    """Score independent signal families once and return exact contributions."""

    family_values = prior_family_values(snapshot)
    # Retraction is a verified lifecycle fact, not missingness. Keep it as a
    # bounded post-family penalty so no other signal can hide it.
    retraction_penalty = 0.45 * _snapshot_value(
        snapshot, "is_retracted", neutral=0.0
    )
    active_weights = weights or _PRIOR_WEIGHTS
    contributions = {
        family: active_weights[family] * value
        for family, value in family_values.items()
    }
    score = _clip(sum(contributions.values()) - retraction_penalty)
    return round(100.0 * score, 6), {
        family: round(100.0 * contribution, 6)
        for family, contribution in contributions.items()
    } | {"retraction_penalty": round(-100.0 * retraction_penalty, 6)}


def apply_repaired_prior(
    candidates: dict[str, dict],
    *,
    timestamp: str,
    scoring_settings: dict[str, str] | None = None,
    shadow_model: PriorCenteredLogisticRidge | None = None,
    shadow_training_size: int = 0,
) -> None:
    """Attach immutable features and replace legacy composite scores in place."""

    active_weights = _resolve_prior_weights(scoring_settings)
    for candidate in candidates.values():
        reward, exposure = build_feature_snapshot(candidate, timestamp=timestamp)
        score, contributions = repaired_prior_score(
            reward,
            weights=active_weights,
        )
        candidate["reward_features"] = reward
        candidate["exposure_features"] = exposure
        candidate["base_signal_score"] = candidate.get("score")
        candidate["score"] = score
        candidate["prior_score"] = score
        candidate["shadow_score"] = (
            round(
                100.0
                * shadow_model.predict_probability(
                    prior_family_values(reward)
                ),
                6,
            )
            if shadow_model is not None
            else None
        )
        breakdown = candidate.get("score_breakdown") or {}
        breakdown["ranker_version"] = RANKER_VERSION
        breakdown["family_contributions"] = contributions
        breakdown["base_signal_score"] = candidate.get("base_signal_score")
        breakdown["shadow_ranker_version"] = (
            SHADOW_VERSION if shadow_model is not None else None
        )
        breakdown["shadow_training_size"] = int(shadow_training_size)
        breakdown["shadow_score"] = candidate["shadow_score"]
        breakdown["final_score"] = score
        candidate["score_breakdown"] = breakdown


def _resolve_prior_weights(
    settings: dict[str, str] | None,
) -> dict[str, float]:
    """Map user-visible signal controls onto non-duplicated ranker families."""

    if not settings:
        return dict(_PRIOR_WEIGHTS)

    def configured(key: str, default: float) -> float:
        try:
            return max(0.0, float(settings.get(key, default)))
        except (TypeError, ValueError):
            return default

    text = configured("weights.text_similarity", 0.20)
    raw = {
        "retrieval": configured("weights.source_relevance", 0.15),
        "semantic": text * 0.70,
        "lexical": text * 0.30,
        "topic": configured("weights.topic_score", 0.20),
        "author": configured("weights.author_affinity", 0.15),
        "venue": configured("weights.journal_affinity", 0.05),
        "recency": configured("weights.recency_boost", 0.10),
        "citation": configured("weights.citation_quality", 0.05),
        "feedback": configured("weights.feedback_adj", 0.10),
        "preference": configured("weights.preference_affinity", 0.10),
    }
    total = sum(raw.values())
    if total <= 0.0:
        raise ValueError("At least one Discovery ranking weight must be positive")
    return {family: weight / total for family, weight in raw.items()}


@dataclass
class PriorCenteredLogisticRidge:
    """Small-data shadow model with coefficients shrunk to a declared prior."""

    feature_names: list[str]
    coefficients: list[float]
    intercept: float = 0.0

    def predict_probability(self, features: dict[str, float]) -> float:
        z = self.intercept + sum(
            coefficient * float(features.get(name, 0.0) or 0.0)
            for name, coefficient in zip(self.feature_names, self.coefficients)
        )
        return 1.0 / (1.0 + math.exp(-max(-30.0, min(30.0, z))))

    @classmethod
    def fit(
        cls,
        rows: list[dict[str, float]],
        labels: list[int],
        *,
        prior: dict[str, float],
        sample_weights: list[float] | None = None,
        regularization: float = 12.0,
        learning_rate: float = 0.05,
        iterations: int = 500,
    ) -> PriorCenteredLogisticRidge:
        """Fit logistic loss + L2 distance to prior; intended for offline eval."""

        if len(rows) != len(labels):
            raise ValueError("rows and labels must have identical lengths")
        if sample_weights is not None and len(sample_weights) != len(rows):
            raise ValueError(
                "sample_weights must have one value per training row"
            )
        names = sorted(prior)
        beta = [float(prior[name]) for name in names]
        prior_beta = list(beta)
        intercept = 0.0
        n = max(1, len(rows))
        weights = (
            [max(0.0, float(value)) for value in sample_weights]
            if sample_weights is not None
            else [1.0] * len(rows)
        )
        weight_total = max(1e-9, sum(weights))
        for _ in range(max(1, iterations)):
            grad = [0.0] * len(beta)
            intercept_grad = 0.0
            for row, label, sample_weight in zip(rows, labels, weights):
                z = intercept + sum(
                    beta[j] * float(row.get(name, 0.0) or 0.0)
                    for j, name in enumerate(names)
                )
                probability = 1.0 / (1.0 + math.exp(-max(-30.0, min(30.0, z))))
                error = probability - int(label)
                intercept_grad += sample_weight * error
                for j, name in enumerate(names):
                    grad[j] += (
                        sample_weight
                        * error
                        * float(row.get(name, 0.0) or 0.0)
                    )
            for j in range(len(beta)):
                grad[j] = (
                    grad[j] / weight_total
                    + regularization
                    * (beta[j] - prior_beta[j])
                    / n
                )
                beta[j] -= learning_rate * grad[j]
            intercept -= learning_rate * intercept_grad / weight_total
        return cls(names, beta, intercept)


def fit_shadow_ranker(
    observations: list,
    *,
    scoring_settings: dict[str, str] | None = None,
) -> tuple[PriorCenteredLogisticRidge | None, int]:
    """Fit only on sufficient randomized v3 evidence, one row per paper."""

    by_paper: dict[str, object] = {}
    for observation in observations:
        paper_id = str(getattr(observation, "paper_id", "") or "").strip()
        if paper_id and paper_id not in by_paper:
            by_paper[paper_id] = observation
    rows = list(by_paper.values())
    labels = [int(getattr(row, "label")) for row in rows]
    positives = sum(labels)
    negatives = len(labels) - positives
    if (
        len(rows) < SHADOW_MIN_OBSERVATIONS
        or positives < SHADOW_MIN_PER_CLASS
        or negatives < SHADOW_MIN_PER_CLASS
    ):
        return None, len(rows)

    features = [
        prior_family_values(getattr(row, "reward_features"))
        for row in rows
    ]
    propensities = [
        float(getattr(row, "inclusion_probability"))
        * float(getattr(row, "position_probability"))
        for row in rows
    ]
    # Clipped inverse-propensity weights: retain randomized correction without
    # allowing one rare slot to dominate this single-user sample.
    sample_weights = [
        min(10.0, 1.0 / max(0.02, propensity))
        for propensity in propensities
    ]
    prior = _resolve_prior_weights(scoring_settings)
    return (
        PriorCenteredLogisticRidge.fit(
            features,
            labels,
            prior=prior,
            sample_weights=sample_weights,
        ),
        len(rows),
    )
