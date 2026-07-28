"""The one ranker for every paper score in ALMa.

Discovery, Feed and Online Search all rank through :func:`rank_candidate`, so a
paper cannot score differently depending on which page you opened.  Production
uses a repaired family-level hand prior; the ridge implementation is a shadow
challenger until enough immutable v3 observations exist.

The score is built in exactly three moves, and :func:`repaired_prior_score`
emits all three so the UI can show *only* and *all* of what produced it:

1. every family's value is derived from its atoms (:data:`FAMILY_SPECS`),
2. available families are weighted and renormalised to sum 1,
3. bounded adjustments (retraction) are subtracted and the result is clipped.

Invariant, asserted by ``tests/test_score_explanation_closure.py``::

    sum(family points) + sum(adjustment points) + clipped == final_score

``FAMILY_SPECS`` is the single source of truth for what a family is made of.
Both the value and its explanation are derived from it, so the UI can never
describe a formula the scorer is not running.
"""

from __future__ import annotations

import math
from dataclasses import dataclass, field

from .features import build_feature_snapshot

RANKER_VERSION = "discovery-v4-family-prior"
SHADOW_VERSION = "discovery-v3-prior-centered-ridge-shadow"
SHADOW_MIN_OBSERVATIONS = 80
SHADOW_MIN_PER_CLASS = 20

# Bounded post-family penalty. Retraction is a verified lifecycle fact, not
# missingness, so it sits outside the families where no signal can hide it.
_RETRACTION_PENALTY = 0.45


@dataclass(frozen=True)
class Atom:
    """One measured input to a family, and how it enters that family's value.

    ``role`` decides the combinator:

    * ``sum``     — adds ``weight * value``.
    * ``max``     — competes inside ``group``; the group adds
      ``weight * max(member values)`` once.  Used where two views of the same
      evidence would otherwise be double-paid (three similarity views of one
      embedding, two graph views of one citation neighbourhood).
    * ``penalty`` — subtracts ``weight * value``, and never makes a family
      "available" on its own.

    ``scale`` divides the raw value before clipping, for atoms that are not
    already in [0, 1] (``fwci`` runs 0..~3+).
    """

    key: str
    label: str
    weight: float
    role: str = "sum"
    group: str = ""
    scale: float = 1.0


@dataclass(frozen=True)
class FamilySpec:
    """A ranking family: one weighted question about a paper."""

    key: str
    label: str
    description: str
    #: The settings key whose slider drives this family's weight.
    weight_setting: str
    #: That setting's default, used when it is absent from the settings map.
    #: Mirrors ``alma.discovery.defaults.DISCOVERY_SETTINGS_DEFAULTS``.
    weight_default: float
    #: Fraction of that setting's value this family takes (``text_similarity``
    #: drives both semantic and lexical).
    weight_share: float = 1.0
    #: What this family scores on a typical paper, measured over the corpus.
    #: Used to IMPUTE the family when it could not be measured, so an unknown
    #: neither rewards nor punishes the paper — see `repaired_prior_score`.
    prior_mean: float = 0.5
    atoms: tuple[Atom, ...] = field(default_factory=tuple)

    @property
    def default_weight(self) -> float:
        """This family's share of its slider at the shipped defaults."""

        return self.weight_default * self.weight_share


# The ten families, in default-weight order. Every number that reaches a score
# is declared here — there is no second place to look.
FAMILY_SPECS: tuple[FamilySpec, ...] = (
    FamilySpec(
        key="semantic",
        label="Semantic",
        description="Embedding similarity to what you already keep.",
        weight_setting="weights.text_similarity",
        weight_default=0.20,
        prior_mean=0.509,
        weight_share=0.70,
        atoms=(
            Atom("semantic_similarity_centroid_raw", "Library centroid", 1.0, "max", "positive"),
            Atom("semantic_similarity_exemplar_raw", "Closest exemplar", 1.0, "max", "positive"),
            Atom("semantic_similarity_support_raw", "Support set", 1.0, "max", "positive"),
            Atom("semantic_similarity_negative_raw", "Similarity to passed-on papers", 0.5, "penalty"),
        ),
    ),
    FamilySpec(
        key="topic",
        label="Topic",
        description="Overlap with the topics your rated papers cluster on.",
        weight_setting="weights.topic_score",
        weight_default=0.20,
        prior_mean=0.740,
        atoms=(Atom("topic_score", "Topic overlap", 1.0),),
    ),
    FamilySpec(
        key="retrieval",
        label="Retrieval",
        description="How strongly the search channels surfaced it, and how many agreed.",
        weight_setting="weights.source_relevance",
        weight_default=0.15,
        prior_mean=0.642,
        atoms=(
            Atom("retrieval_rrf_semantic", "Vector channel rank", 0.75, "max", "rrf"),
            Atom("retrieval_rrf_lexical", "Lexical channel rank", 0.75, "max", "rrf"),
            Atom("retrieval_rrf_citation", "Citation channel rank", 0.75, "max", "rrf"),
            Atom("retrieval_rrf_taste", "Taste channel rank", 0.75, "max", "rrf"),
            Atom("retrieval_family_count", "Channels that agreed", 0.25, "sum", scale=4.0),
        ),
    ),
    FamilySpec(
        key="author",
        label="Author",
        description="Authors you follow or repeatedly save.",
        weight_setting="weights.author_affinity",
        weight_default=0.15,
        prior_mean=0.618,
        atoms=(Atom("author_affinity", "Author affinity", 1.0),),
    ),
    FamilySpec(
        key="lexical",
        label="Lexical",
        description="Terminology overlap — the words, not the meaning.",
        weight_setting="weights.text_similarity",
        weight_default=0.20,
        prior_mean=0.265,
        weight_share=0.30,
        atoms=(
            Atom("lexical_similarity_word_raw", "Word overlap", 0.45),
            Atom("lexical_similarity_char_raw", "Character n-grams", 0.35),
            Atom("lexical_similarity_term_raw", "Key terms", 0.20),
            Atom("lexical_similarity_negative_penalty", "Overlap with passed-on papers", 0.5, "penalty"),
        ),
    ),
    FamilySpec(
        key="recency",
        label="Recency",
        description="How recently it was published.",
        weight_setting="weights.recency_boost",
        weight_default=0.10,
        prior_mean=0.468,
        atoms=(Atom("recency_boost", "Publication recency", 1.0),),
    ),
    FamilySpec(
        key="citation",
        label="Citation",
        description="Citation weight, and citation-graph proximity to your library.",
        weight_setting="weights.citation_quality",
        weight_default=0.05,
        prior_mean=0.276,
        atoms=(
            Atom("citation_quality", "Citation count", 0.50),
            Atom("fwci", "Field-weighted impact", 0.10, scale=3.0),
            Atom("coupling_strength", "Shared references with your library", 0.20, "max", "graph"),
            Atom("cocitation_strength", "Cited alongside your library", 0.20, "max", "graph"),
            Atom("ppr_library_raw", "Graph proximity to library", 0.20, "max", "ppr"),
            Atom("ppr_loved_raw", "Graph proximity to loved papers", 0.20, "max", "ppr"),
        ),
    ),
    FamilySpec(
        key="feedback",
        label="Feedback",
        description="Your explicit verdicts on similar papers.",
        weight_setting="weights.feedback_adj",
        weight_default=0.10,
        prior_mean=0.957,
        atoms=(Atom("feedback_adj", "Feedback adjustment", 1.0),),
    ),
    FamilySpec(
        key="preference",
        label="Preference",
        description="The taste profile accumulated from Signal Lab and your history.",
        weight_setting="weights.preference_affinity",
        weight_default=0.10,
        prior_mean=0.618,
        atoms=(Atom("preference_affinity", "Preference affinity", 1.0),),
    ),
    FamilySpec(
        key="venue",
        label="Venue",
        description="Journals and conferences you read.",
        weight_setting="weights.journal_affinity",
        weight_default=0.05,
        prior_mean=0.548,
        atoms=(Atom("journal_affinity", "Venue affinity", 1.0),),
    ),
)

_SPEC_BY_KEY = {spec.key: spec for spec in FAMILY_SPECS}

# Explore / exploit reweighting. Ported from the retired composite stage so the
# Settings control keeps reweighting the ranking it claims to reweight. Applied
# to family weights BEFORE renormalisation, so the modes stay zero-sum.
_MODE_MULTIPLIERS: dict[str, dict[str, float]] = {
    "explore": {"recency": 1.5, "citation": 0.5, "author": 0.5, "venue": 0.5},
    "exploit": {"author": 1.5, "venue": 1.5, "preference": 1.5, "recency": 0.5},
    "balanced": {},
}


def _clip(value: float, low: float = 0.0, high: float = 1.0) -> float:
    return max(low, min(high, float(value)))


def _atom_reading(snapshot: dict, atom: Atom) -> tuple[float, bool]:
    """Return ``(clipped value, available)`` for one atom of a family."""

    detail = snapshot.get(atom.key) or {}
    if not isinstance(detail, dict) or not detail.get("availability"):
        return 0.0, False
    return _clip(float(detail.get("value") or 0.0) / atom.scale), True


def _family_reading(snapshot: dict, spec: FamilySpec) -> tuple[float, bool, list[dict]]:
    """Derive one family's value, availability and per-atom explanation.

    A family is *available* when at least one of its non-penalty atoms was
    measured. An unavailable family is dropped from the score entirely (see
    :func:`repaired_prior_score`) rather than being credited a neutral value —
    a paper with no journal must not collect half the venue weight for free.
    """

    atoms: list[dict] = []
    total = 0.0
    available = False
    # `max` atoms compete within their group; the group pays its weight once.
    max_groups: dict[str, tuple[float, float]] = {}

    for atom in spec.atoms:
        value, atom_available = _atom_reading(snapshot, atom)
        if atom_available and atom.role != "penalty":
            available = True
        atoms.append(
            {
                "key": atom.key,
                "label": atom.label,
                "value": round(value, 6),
                "weight": round(atom.weight, 6),
                "role": atom.role,
                "group": atom.group or None,
                "available": atom_available,
            }
        )
        if atom.role == "max":
            best_weight, best_value = max_groups.get(atom.group, (atom.weight, 0.0))
            max_groups[atom.group] = (best_weight, max(best_value, value))
        elif atom.role == "penalty":
            total -= atom.weight * value
        else:
            total += atom.weight * value

    for group_weight, group_value in max_groups.values():
        total += group_weight * group_value

    return _clip(total), available, atoms


def prior_family_values(snapshot: dict) -> dict[str, float]:
    """Project atoms into the ten families. Kept for the shadow ranker's input."""

    return {
        spec.key: _family_reading(snapshot, spec)[0] for spec in FAMILY_SPECS
    }


def repaired_prior_score(
    snapshot: dict,
    *,
    weights: dict[str, float] | None = None,
) -> tuple[float, dict]:
    """Score every family once and return the closed explanation.

    **Weights are FIXED — never rescaled per paper.** A score is a ranking key,
    so its only job is to compare papers against each other, and a denominator
    that changes per paper destroys exactly that. Two papers scoring 69 must
    mean the same thing.

    Renormalising over "available" families (shipped briefly in v0.22.0) broke
    that, and it broke it in a biased direction: the families that go missing
    are the ones papers score BADLY on (measured corpus means: citation 0.28,
    lexical 0.27, semantic 0.51 — against feedback 0.96, topic 0.74). Dropping
    a weak family and handing its weight to the strong ones is a free upgrade,
    so a paper rose by having less evidence. Prod showed it: Feed rows (3
    families missing) averaged 68.1 against Discovery's 62.0 with all ten.

    A family that could not be measured is IMPUTED at its corpus prior mean
    (``FamilySpec.prior_mean``) instead. Unknown then costs nothing and buys
    nothing, and the denominator stays constant, so scores remain comparable
    across papers and across surfaces. Zero-filling would be the opposite
    error — it ranks by hydration completeness, which is the trap that got
    ``usefulness_boost`` deleted.

    Returns ``(score, explanation)``; the explanation's family points,
    adjustment points and clipping term sum exactly to the score.
    """

    configured = weights or _resolve_prior_weights(None)
    readings = {spec.key: _family_reading(snapshot, spec) for spec in FAMILY_SPECS}

    families: list[dict] = []
    points_total = 0.0
    for spec in FAMILY_SPECS:
        measured_value, available, atoms = readings[spec.key]
        # Imputed when unmeasured. `value` is what the score actually used, so
        # the arithmetic on screen always reconciles; `available` tells the UI
        # to label it as an estimate rather than an observation.
        value = measured_value if available else spec.prior_mean
        weight = max(0.0, configured.get(spec.key, 0.0))
        points = 100.0 * weight * value
        points_total += points
        families.append(
            {
                "key": spec.key,
                "label": spec.label,
                "description": spec.description,
                "value": round(value, 6),
                "weight": round(weight, 6),
                "points": round(points, 6),
                "available": available,
                "imputed": not available,
                "prior_mean": round(spec.prior_mean, 6),
                "atoms": atoms,
            }
        )

    retraction_value, retraction_available = _atom_reading(
        snapshot, Atom("is_retracted", "Retracted", 1.0)
    )
    retraction_points = -100.0 * _RETRACTION_PENALTY * retraction_value
    adjustments = [
        {
            "key": "retraction",
            "label": "Retracted",
            "description": "A retracted paper is capped down regardless of every other signal.",
            "points": round(retraction_points, 6),
            "available": retraction_available,
        }
    ]

    raw_total = points_total + retraction_points
    score = _clip(raw_total, 0.0, 100.0)
    explanation = {
        "ranker_version": RANKER_VERSION,
        "final_score": round(score, 6),
        "families": families,
        "adjustments": adjustments,
        # Closes the invariant when the raw total left the 0..100 band.
        "clipped": round(score - raw_total, 6),
    }
    return round(score, 6), explanation


def rank_candidate(
    candidate: dict,
    *,
    timestamp: str,
    scoring_settings: dict[str, str] | None = None,
) -> tuple[float, dict]:
    """Rank ONE candidate. The single entry point for Feed and Online Search.

    Discovery ranks in bulk through :func:`apply_repaired_prior`; both land on
    the same families, weights and explanation, which is what keeps a paper's
    score identical on every surface that shows it.
    """

    ranked = {"_": candidate}
    apply_repaired_prior(
        ranked, timestamp=timestamp, scoring_settings=scoring_settings
    )
    return float(candidate["score"]), candidate["score_breakdown"]


def apply_repaired_prior(
    candidates: dict[str, dict],
    *,
    timestamp: str,
    scoring_settings: dict[str, str] | None = None,
    shadow_model: PriorCenteredLogisticRidge | None = None,
    shadow_training_size: int = 0,
) -> None:
    """Attach immutable features and write the ranking score in place."""

    active_weights = _resolve_prior_weights(scoring_settings)
    for candidate in candidates.values():
        reward, exposure = build_feature_snapshot(candidate, timestamp=timestamp)
        score, explanation = repaired_prior_score(reward, weights=active_weights)
        candidate["reward_features"] = reward
        candidate["exposure_features"] = exposure
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
        breakdown["explanation"] = explanation
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
    """Map the user-visible weight sliders + mode onto family weights.

    Each family declares which setting drives it and what share of it it takes
    (``FamilySpec.weight_setting`` / ``weight_share``), so adding a family is a
    one-line change here-adjacent instead of a second mapping to keep in sync.
    """

    settings = settings or {}

    def configured(spec: FamilySpec) -> float:
        try:
            raw = float(settings.get(spec.weight_setting, spec.weight_default))
        except (TypeError, ValueError):
            raw = spec.weight_default
        return max(0.0, raw) * spec.weight_share

    mode = str(settings.get("recommendation_mode", "balanced") or "balanced").strip().lower()
    multipliers = _MODE_MULTIPLIERS.get(mode, {})

    raw = {
        spec.key: configured(spec) * multipliers.get(spec.key, 1.0)
        for spec in FAMILY_SPECS
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
