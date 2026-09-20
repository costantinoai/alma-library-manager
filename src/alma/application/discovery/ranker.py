"""The one ranker for every paper score in ALMa.

Discovery, Feed and Online Search all rank through :func:`rank_candidate`, so a
paper cannot score differently depending on which page you opened.  Production
uses a repaired family-level hand prior; the ridge implementation is a shadow
challenger until enough immutable v3 observations exist.

The score is built in exactly three moves, and :func:`repaired_prior_score`
emits all three so the UI can show *only* and *all* of what produced it:

1. every family's value is derived from its atoms (:data:`FAMILY_SPECS`),
2. families are weighted with FIXED weights (unmeasured ones imputed at their
   corpus prior mean),
3. bounded adjustments — retraction (:data:`_RETRACTION_PENALTY`) and the
   Signal Lab heads (:data:`LAB_ADJUSTMENTS`) — are added and the result is
   clipped to 0..100.

Invariant, asserted by ``tests/test_score_explanation_closure.py``::

    sum(family points) + sum(adjustment points) + clipped == final_score

``FAMILY_SPECS`` and ``LAB_ADJUSTMENTS`` are the single source of truth for
what reaches a score. Both the value and its explanation are derived from
them, so the UI can never describe a formula the scorer is not running — and
a measured input nobody declared here cannot move a score silently
(``tests/test_signal_lab_ranker_boundary.py``, bug B1: the Lab heads were
measured and dropped for two releases).
"""

from __future__ import annotations

import math
from dataclasses import dataclass, field

from alma.discovery.defaults import (
    DEFAULT_SIGNAL_WEIGHTS,
    LAB_HEAD_DEFAULT_POINTS,
    LAB_HEAD_MAX_POINTS,
    TEXT_SIMILARITY_SEMANTIC_SHARE,
    lab_head_points,
)

from .calibration import UNCALIBRATED, ScoringCalibration
from .features import build_feature_snapshot

# v5: the Signal Lab region + utility heads enter the score as an explained
# adjustment (they were measured and discarded in v4), and the immutable
# snapshot records their signed inputs plus the effective weights.
# v6: how inputs are READ changed. Semantic atoms go through the shared
# similarity calibration instead of raw cosines, `fwci` through a log ratio,
# the citation graph pair corroborates instead of competing, and the atom that
# was `min()` of its own groupmates left the semantic group. Same inputs, same
# weights, different arithmetic — so a v5 score and a v6 score are not
# comparable and stored explanations must not be mixed.
# v7: lexical atoms read through the lexical calibration, the feedback family is
# a bounded weighted mean of its two estimates instead of a clamped sum, and
# every `prior_mean` was re-measured on a random corpus sample through the real
# measurement path (`scripts/measure_ranking_priors.py`). Bumped so a score
# computed under the v6 arithmetic is never read as comparable with these.
RANKER_VERSION = "discovery-v7-calibrated-prior"
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

    ``curve`` says HOW the raw value is mapped into [0, 1], and ``scale`` is
    that curve's parameter:

    * ``linear`` (default) — ``value / scale``, for inputs already on a bounded
      or near-uniform range.
    * ``log_ratio`` — for a RATIO to a reference point, where 1.0 means "average"
      and the tail runs orders of magnitude past it:
      ``0.5 + 0.5 * log10(value) / scale``, so 1.0 maps to the middle and
      ``scale`` counts the decades either side that reach the ends.
    * ``lexical`` — the same idea for a lexical overlap fraction, through the
      lexical table of that one calibration.
    * ``similarity`` — for a cosine, through the ONE shared calibration
      (``core.scoring_math.calibrate_similarity_score``), which maps it to its
      percentile in the corpus. A cosine has no useful absolute scale: measured
      over 11k papers, similarity to one library spans 0.785 to 0.956 between
      the 1st and 99th percentiles. Reading that raw let a family holding the
      second-largest weight move the ranking by 0.23 points out of 100 — the
      user's slider silently overridden by the units of the measurement.

    ``fwci`` is the reason the second exists. It is a ratio to the field average
    and it is heavy-tailed: measured over 6,108 papers, the median is 0.16 and
    the maximum 8,825. Dividing by 3 put 22.5% of papers at the ceiling and
    squeezed the median to 0.05, so the input mostly distinguished "enormous"
    from "everything else". Two decades either side of the field average
    (0.01 -> 0, 1 -> 0.5, 100 -> 1) leaves 1% at the ceiling and a median of
    0.30 — a signal that varies where the papers actually are.
    """

    key: str
    label: str
    weight: float
    role: str = "sum"
    group: str = ""
    scale: float = 1.0
    curve: str = "linear"


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
    #: How each ``max``-role group combines its members: ``"max"`` (default) or
    #: ``"noisy_or"``. Declared per group because the right answer depends on
    #: what the members ARE:
    #:
    #: * ``max`` when they are the same evidence measured several ways. Paying
    #:   each would multiply one fact by however many ways we happened to
    #:   measure it.
    #: * ``noisy_or`` (``1 - Π(1 - v)``) when they are DIFFERENT evidence for
    #:   the same question. Two independent reasons to believe should beat one,
    #:   but sub-additively — corroboration, not double payment. Bounded in
    #:   [0, 1] and monotone, so it still cannot exceed the group's weight.
    combine: dict[str, str] = field(default_factory=dict)

    @property
    def default_weight(self) -> float:
        """This family's share of its slider at the shipped defaults."""

        return self.weight_default * self.weight_share


# The ten families, in default-weight order. Every number that reaches a score
# is declared here — there is no second place to look.
#
# `prior_mean` values were measured 2026-09-06 with
# `scripts/measure_ranking_priors.py`: 800 RANDOM corpus papers scored through
# the real measurement path with the real profile (never a Discovery deck, which
# retrieval already selected for these very quantities). `retrieval` cannot be
# measured that way — a corpus paper has run through no retrieval — and keeps
# its declared value. Re-measure after any change to how a family is read.
FAMILY_SPECS: tuple[FamilySpec, ...] = (
    FamilySpec(
        key="semantic",
        label="Semantic",
        description="Embedding similarity to what you already keep.",
        weight_setting="weights.text_similarity",
        weight_default=DEFAULT_SIGNAL_WEIGHTS["text_similarity"],
        prior_mean=0.452,
        weight_share=TEXT_SIMILARITY_SEMANTIC_SHARE,
        atoms=(
            Atom("semantic_similarity_centroid_raw", "Library centroid", 1.0, "max", "positive", curve="similarity"),
            Atom("semantic_similarity_exemplar_raw", "Closest exemplar", 1.0, "max", "positive", curve="similarity"),
            # `semantic_similarity_support_raw` used to sit in this group as a
            # third competitor. It is defined as `min(centroid, exemplar)`
            # (`similarity.py`), so in a group that pays the MAXIMUM it could
            # never win — not on this corpus, but arithmetically, always. It
            # won 0 of 138 measured papers and dropping it moved no score by
            # any amount. It stays recorded as a diagnostic (the two views
            # agreeing is worth seeing) and out of the arithmetic.
            Atom("semantic_similarity_negative_raw", "Similarity to passed-on papers", 0.5, "penalty", curve="similarity"),
        ),
    ),
    FamilySpec(
        key="topic",
        label="Topic",
        description="Overlap with the topics your rated papers cluster on.",
        weight_setting="weights.topic_score",
        weight_default=DEFAULT_SIGNAL_WEIGHTS["topic_score"],
        prior_mean=0.688,
        atoms=(Atom("topic_score", "Topic overlap", 1.0),),
    ),
    FamilySpec(
        key="retrieval",
        label="Retrieval",
        description="How strongly the search channels surfaced it, and how many agreed.",
        weight_setting="weights.source_relevance",
        weight_default=DEFAULT_SIGNAL_WEIGHTS["source_relevance"],
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
        weight_default=DEFAULT_SIGNAL_WEIGHTS["author_affinity"],
        prior_mean=0.588,
        atoms=(Atom("author_affinity", "Author affinity", 1.0),),
    ),
    FamilySpec(
        key="lexical",
        label="Lexical",
        description="Terminology overlap — the words, not the meaning.",
        weight_setting="weights.text_similarity",
        weight_default=DEFAULT_SIGNAL_WEIGHTS["text_similarity"],
        prior_mean=0.380,
        weight_share=1.0 - TEXT_SIMILARITY_SEMANTIC_SHARE,
        atoms=(
            # Same rule as the semantic atoms: a raw overlap fraction goes through
            # the ONE lexical calibration before anything weights it.
            Atom("lexical_similarity_word_raw", "Word overlap", 0.45, curve="lexical"),
            Atom("lexical_similarity_char_raw", "Character n-grams", 0.35, curve="lexical"),
            Atom("lexical_similarity_term_raw", "Key terms", 0.20, curve="lexical"),
            Atom("lexical_similarity_negative_penalty", "Overlap with passed-on papers", 0.5, "penalty", curve="lexical"),
        ),
    ),
    FamilySpec(
        key="recency",
        label="Recency",
        description="How recently it was published.",
        weight_setting="weights.recency_boost",
        weight_default=DEFAULT_SIGNAL_WEIGHTS["recency_boost"],
        prior_mean=0.325,
        atoms=(Atom("recency_boost", "Publication recency", 1.0),),
    ),
    FamilySpec(
        key="citation",
        label="Citation",
        description="Citation weight, and citation-graph proximity to your library.",
        weight_setting="weights.citation_quality",
        weight_default=DEFAULT_SIGNAL_WEIGHTS["citation_quality"],
        prior_mean=0.199,
        # Shared references and co-citation are DIFFERENT relations — measured
        # correlation 0.59, and each decides the group's value on a large share
        # of papers — so a paper with both deserves more than a paper with one.
        # The two PPR walks are near-duplicates (0.92) over nested populations,
        # so they keep competing.
        combine={"graph": "noisy_or", "ppr": "max"},
        atoms=(
            Atom("citation_quality", "Citation count", 0.50),
            Atom("fwci", "Field-weighted impact", 0.10, scale=2.0, curve="log_ratio"),
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
        weight_default=DEFAULT_SIGNAL_WEIGHTS["feedback_adj"],
        prior_mean=0.657,
        atoms=(Atom("feedback_adj", "Feedback adjustment", 1.0),),
    ),
    FamilySpec(
        key="preference",
        label="Preference",
        description="The taste profile accumulated from Signal Lab and your history.",
        weight_setting="weights.preference_affinity",
        weight_default=DEFAULT_SIGNAL_WEIGHTS["preference_affinity"],
        prior_mean=0.513,
        atoms=(Atom("preference_affinity", "Preference affinity", 1.0),),
    ),
    FamilySpec(
        key="venue",
        label="Venue",
        description="Journals and conferences you read.",
        weight_setting="weights.journal_affinity",
        weight_default=DEFAULT_SIGNAL_WEIGHTS["journal_affinity"],
        prior_mean=0.198,
        atoms=(Atom("journal_affinity", "Venue affinity", 1.0),),
    ),
)

_SPEC_BY_KEY = {spec.key: spec for spec in FAMILY_SPECS}


@dataclass(frozen=True)
class LabAdjustmentSpec:
    """One Signal Lab head that enters the score as an additive adjustment.

    The head's raw value is measured by ``measure_candidate`` (via
    ``signal_lab.scoring_terms.compute_lab_adjustments``) on the signed unit
    interval, evidence damper already applied. The ranker weights it ONCE::

        points = clamp(setting, 0, weight_max) * clip(raw, -1, 1)

    so a head at ``weights.lab_*`` = 5 moves a score by at most ±5 points. The
    categorical heads (author, venue) are NOT here: they fold into the curated
    author / venue affinity before measurement (``fold_lab_offsets``), so the
    same evidence is never paid twice.
    """

    key: str
    label: str
    description: str
    #: The snapshot key carrying the signed raw value.
    atom_key: str
    #: The settings key whose slider is this head's weight, in score points.
    weight_setting: str
    weight_default: float = LAB_HEAD_DEFAULT_POINTS
    weight_max: float = LAB_HEAD_MAX_POINTS


LAB_ADJUSTMENTS: tuple[LabAdjustmentSpec, ...] = (
    LabAdjustmentSpec(
        key="region",
        label="Region preference",
        description="How the Signal Lab rounds you answered rate this paper's region.",
        atom_key="lab_region_offset_raw",
        weight_setting="weights.lab_region_offset",
    ),
    LabAdjustmentSpec(
        key="utility",
        label="Learned direction",
        description=(
            "Alignment with the utility direction fitted from your Signal Lab "
            "answers, scaled by how much evidence backs it."
        ),
        atom_key="lab_utility_raw",
        weight_setting="weights.lab_utility",
    ),
)

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


def _atom_reading(snapshot: dict, atom: Atom, calibration: ScoringCalibration) -> tuple[float, bool]:
    """Return ``(clipped value, available)`` for one atom of a family."""

    detail = snapshot.get(atom.key) or {}
    if not isinstance(detail, dict) or not detail.get("availability"):
        return 0.0, False
    raw = float(detail.get("value") or 0.0)
    if atom.curve in ("similarity", "lexical"):
        # Through THIS install's derived table (corpus percentile); raw when
        # the install has not been measured yet.
        reading = calibration.read(atom.key, raw)
    elif atom.curve == "log_ratio":
        # A ratio of 0 is "no measured impact at all", which the log cannot
        # express; the floor puts it at the bottom of the range rather than at
        # negative infinity.
        reading = 0.5 + 0.5 * math.log10(max(raw, 1e-3)) / atom.scale
    else:
        reading = raw / atom.scale
    return _clip(reading), True


def _signed_reading(snapshot: dict, key: str) -> tuple[float, bool]:
    """Return ``(value clipped to [-1, 1], available)`` for a signed input.

    The Lab heads are directions, not levels: −1 is "your answers say no" as
    strongly as +1 says yes. Family atoms clip to [0, 1]; these must not.
    """

    detail = snapshot.get(key) or {}
    if not isinstance(detail, dict) or not detail.get("availability"):
        return 0.0, False
    return _clip(float(detail.get("value") or 0.0), -1.0, 1.0), True


def _family_reading(
    snapshot: dict, spec: FamilySpec, calibration: ScoringCalibration | None = None
) -> tuple[float, bool, list[dict]]:
    """Derive one family's value, availability and per-atom explanation.

    A family is *available* when at least one of its non-penalty atoms was
    measured. An unavailable family is dropped from the score entirely (see
    :func:`repaired_prior_score`) rather than being credited a neutral value —
    a paper with no journal must not collect half the venue weight for free.
    """

    calibration = calibration or UNCALIBRATED
    atoms: list[dict] = []
    total = 0.0
    available = False
    # `max`-role atoms share a group; the group pays its weight ONCE, combined
    # by the family's declared policy (`FamilySpec.combine`).
    max_groups: dict[str, tuple[float, float]] = {}
    group_best: dict[str, tuple[str, float]] = {}

    for atom in spec.atoms:
        value, atom_available = _atom_reading(snapshot, atom, calibration)
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
                # Filled in below for grouped atoms: did THIS atom's value reach
                # the score? Published rather than left for the reader to infer,
                # because the answer now depends on the group's combine policy
                # and a UI recomputing it would drift from the scorer.
                "counted": atom_available,
            }
        )
        if atom.role == "max":
            mode = spec.combine.get(atom.group, "max")
            best_weight, best_value = max_groups.get(atom.group, (atom.weight, 0.0))
            if mode == "noisy_or":
                combined = 1.0 - (1.0 - _clip(best_value)) * (1.0 - _clip(value))
            else:
                combined = max(best_value, value)
            max_groups[atom.group] = (best_weight, combined)
            if atom_available:
                leader_key, leader_value = group_best.get(atom.group, ("", -1.0))
                if value > leader_value:
                    group_best[atom.group] = (atom.key, value)
        elif atom.role == "penalty":
            total -= atom.weight * value
        else:
            total += atom.weight * value

    # Under `max` only the strongest member is paid; under `noisy_or` every
    # measured member contributes.
    for entry in atoms:
        group = entry["group"]
        if not group or entry["role"] != "max":
            continue
        if not entry["available"]:
            entry["counted"] = False
        elif spec.combine.get(group, "max") == "max":
            entry["counted"] = group_best.get(group, ("", 0.0))[0] == entry["key"]

    for group_weight, group_value in max_groups.values():
        total += group_weight * group_value

    return _clip(total), available, atoms


def prior_family_values(
    snapshot: dict, calibration: ScoringCalibration | None = None
) -> dict[str, float]:
    """Project atoms into the ten families. Kept for the shadow ranker's input."""

    return {
        spec.key: _family_reading(snapshot, spec, calibration)[0] for spec in FAMILY_SPECS
    }


def _lab_adjustment(snapshot: dict, lab_points: dict[str, float]) -> dict:
    """The ONE Signal Lab explanation row: summed points + per-head atoms.

    Each head is weighted exactly once here. ``lab_points`` is already clamped
    to ``[0, weight_max]`` by :func:`resolve_lab_points`; the raw value is
    already damped by its evidence (``scoring_terms``) and is clipped to the
    signed unit interval, so ``|points| <= weight_max`` per head by
    construction. A head whose input was never measured (Lab disabled, no
    model, no embedding) is *unavailable* and contributes exactly zero — the
    same distinction the families draw between "unknown" and "measured 0".
    """

    atoms: list[dict] = []
    total = 0.0
    for spec in LAB_ADJUSTMENTS:
        value, available = _signed_reading(snapshot, spec.atom_key)
        weight = lab_points.get(spec.key, 0.0)
        points = weight * value
        total += points
        atoms.append(
            {
                "key": spec.atom_key,
                "label": spec.label,
                "description": spec.description,
                "value": round(value, 6),
                "weight": round(weight, 6),
                "points": round(points, 6),
                "available": available,
            }
        )
    return {
        "key": "signal_lab",
        "label": "Signal Lab",
        "description": (
            "What the rounds you answered in the Signal Lab say about this "
            "paper, in score points. Each head is bounded by its Settings weight."
        ),
        "points": round(total, 6),
        "available": any(atom["available"] for atom in atoms),
        "atoms": atoms,
    }


def repaired_prior_score(
    snapshot: dict,
    *,
    weights: dict[str, float] | None = None,
    lab_points: dict[str, float] | None = None,
    calibration: ScoringCalibration | None = None,
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

    ``lab_points`` are the Signal Lab head weights in score points (see
    :func:`resolve_lab_points`); ``None`` means the shipped defaults, the same
    convention as ``weights``. Pass an all-zero map to score as if the Lab
    were off — that is how eval builds its baseline.

    Returns ``(score, explanation)``; the explanation's family points,
    adjustment points and clipping term sum exactly to the score.
    """

    configured = weights or resolve_family_weights(None)
    lab_weights = lab_points if lab_points is not None else resolve_lab_points(None)
    calibration = calibration or UNCALIBRATED
    readings = {spec.key: _family_reading(snapshot, spec, calibration) for spec in FAMILY_SPECS}

    families: list[dict] = []
    points_total = 0.0
    for spec in FAMILY_SPECS:
        measured_value, available, atoms = readings[spec.key]
        # Imputed when unmeasured. `value` is what the score actually used, so
        # the arithmetic on screen always reconciles; `available` tells the UI
        # to label it as an estimate rather than an observation.
        prior = calibration.prior(spec.key, spec.prior_mean)
        value = measured_value if available else prior
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
                "prior_mean": round(prior, 6),
                "atoms": atoms,
            }
        )

    retraction_value, retraction_available = _atom_reading(
        snapshot, Atom("is_retracted", "Retracted", 1.0), calibration
    )
    retraction_points = -100.0 * _RETRACTION_PENALTY * retraction_value
    lab_row = _lab_adjustment(snapshot, lab_weights)
    adjustments = [
        {
            "key": "retraction",
            "label": "Retracted",
            "description": "A retracted paper is capped down regardless of every other signal.",
            "points": round(retraction_points, 6),
            "available": retraction_available,
            "atoms": [],
        },
        lab_row,
    ]

    raw_total = points_total + retraction_points + lab_row["points"]
    score = _clip(raw_total, 0.0, 100.0)
    explanation = {
        "ranker_version": RANKER_VERSION,
        "final_score": round(score, 6),
        # What an all-average paper scores under the same weights — the point
        # a reader should compare `final_score` against. Not part of the
        # closure sum; it is a reference, not a contribution.
        "reference_score": round(
            sum(
                100.0 * configured.get(spec.key, 0.0) * calibration.prior(spec.key, spec.prior_mean)
                for spec in FAMILY_SPECS
            ),
            6,
        ),
        # Which derived calibration read the inputs (None ⇒ uncalibrated).
        "calibration": calibration.generation,
        "families": families,
        "adjustments": adjustments,
        # Closes the invariant when the raw total left the 0..100 band.
        "clipped": round(score - raw_total, 6),
    }
    return round(score, 6), explanation


def typical_prior_score(
    scoring_settings: dict[str, str] | None = None,
    calibration: ScoringCalibration | None = None,
) -> float:
    """The score a paper at every family's corpus prior would get, under the
    active weights and mode. THE reference point for reading a score.

    The scale runs 0..100, but a paper that is average on everything does not
    score 50 — it scores whatever the weighted priors sum to (about 58 at the
    shipped defaults). The meter used to colour anything under 70 as a caution,
    so the typical candidate read as a warning. A score is only meaningful
    relative to this number, so the ranker publishes it beside every score.
    """

    weights = resolve_family_weights(scoring_settings)
    calibration = calibration or UNCALIBRATED
    return round(
        sum(
            100.0 * weights[spec.key] * calibration.prior(spec.key, spec.prior_mean)
            for spec in FAMILY_SPECS
        ),
        6,
    )


def rank_candidate(
    candidate: dict,
    *,
    timestamp: str,
    scoring_settings: dict[str, str] | None = None,
    calibration: ScoringCalibration | None = None,
) -> tuple[float, dict]:
    """Rank ONE candidate. The single entry point for Feed and Online Search.

    Discovery ranks in bulk through :func:`apply_repaired_prior`; both land on
    the same families, weights and explanation, which is what keeps a paper's
    score identical on every surface that shows it.
    """

    ranked = {"_": candidate}
    apply_repaired_prior(
        ranked, timestamp=timestamp, scoring_settings=scoring_settings, calibration=calibration
    )
    return float(candidate["score"]), candidate["score_breakdown"]


def apply_repaired_prior(
    candidates: dict[str, dict],
    *,
    timestamp: str,
    scoring_settings: dict[str, str] | None = None,
    shadow_model: PriorCenteredLogisticRidge | None = None,
    shadow_training_size: int = 0,
    calibration: ScoringCalibration | None = None,
) -> None:
    """Attach immutable features and write the ranking score in place."""

    active_weights = resolve_family_weights(scoring_settings)
    lab_points = resolve_lab_points(scoring_settings)
    calibration = calibration or UNCALIBRATED
    calibration_record = calibration.as_dict()
    for candidate in candidates.values():
        reward, exposure = build_feature_snapshot(candidate, timestamp=timestamp)
        score, explanation = repaired_prior_score(
            reward, weights=active_weights, lab_points=lab_points, calibration=calibration
        )
        # What the score was computed WITH, next to what it was computed FROM,
        # so an immutable observation can be replayed under the same settings
        # and any later drift in weights or model generation is visible.
        exposure["ranking_weights"] = {
            "ranker_version": RANKER_VERSION,
            "families": dict(active_weights),
            "lab_points": dict(lab_points),
            # The derived tables and priors this score was read through, so a
            # replay is exact even after the calibration has been rebuilt.
            "calibration": calibration_record,
        }
        candidate["reward_features"] = reward
        candidate["exposure_features"] = exposure
        candidate["score"] = score
        candidate["prior_score"] = score
        candidate["shadow_score"] = (
            round(
                100.0
                * shadow_model.predict_probability(
                    prior_family_values(reward, calibration)
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


def resolve_family_weights(
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


def resolve_lab_points(settings: dict[str, str] | None) -> dict[str, float]:
    """Map the Signal Lab sliders onto head weights, in score points.

    Unlike the family weights these are NOT renormalised: a head's setting IS
    its maximum reach on the 0..100 score, which is what the Settings card
    promises ("up to N points"). Parsing, default fallback and the ceiling
    clamp live in :func:`alma.discovery.defaults.lab_head_points`, shared with
    the categorical folds and the scoring-context gate. Public because eval
    replays stored snapshots under the current settings through it, and the
    gate uses it to decide whether loading a model is worth anything.
    """

    return {
        spec.key: lab_head_points(settings, spec.weight_setting)
        for spec in LAB_ADJUSTMENTS
    }


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
    prior = resolve_family_weights(scoring_settings)
    return (
        PriorCenteredLogisticRidge.fit(
            features,
            labels,
            prior=prior,
            sample_weights=sample_weights,
        ),
        len(rows),
    )
