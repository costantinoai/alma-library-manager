"""signal_tuning — what each scoring signal is worth once the others are held fixed.

Why this exists
---------------

``reports.signal_impact`` reports a MARGINAL statistic per signal: the
difference in that signal's mean between papers you reacted well to and papers
you reacted badly to. That is the confounded view, and on this data it is
actively misleading:

- ``author_affinity`` looked like the strongest positive signal in the set
  (Cohen's d ≈ +0.61 marginal) and is worthless once the others are controlled
  (partial ≈ −0.13). It was reading "this came from a lane that also produces
  good papers", not "you like this author".
- ``is_lexical_fallback`` **flipped sign** — −0.67 marginal, +1.40 partial.

A marginal difference answers "does this signal co-occur with success". A
partial coefficient answers "does this signal ADD anything once the rest of the
vector is known", which is the only version of the question a weight can act
on. Both belong in the UI, side by side, because the gap between them is itself
the finding.

What this module refuses to do
------------------------------

**It does not promote anything.** It is a diagnostic that reports what a fitted
head WOULD say. Promotion stays a deliberate act on held-out evidence.

**It states its own weakness in the payload.** With ~100 engaged rows and 9
signals this fit is over-parameterised by any events-per-variable rule of
thumb, and every number it produces is in-sample. Those facts ship as fields
(``events_per_variable``, ``admissible_parameters``, ``over_parameterised``,
``in_sample``) rather than as a caveat in a docstring nobody renders, because a
coefficient table with no sample-size context is exactly how a reader talks
themselves into re-weighting on noise.

**It never reads an exposure feature.** Lane identity, raw rank, position,
metadata completeness and hydration path describe how a paper REACHED you, not
what it is. Fitting on them produces a model that predicts its own delivery
mechanism. ``is_lexical_fallback`` — the largest sign flip in the set, and an
artifact of a write-then-read race — is the cautionary tale.
"""

from __future__ import annotations

import json
import logging
import math
import sqlite3
from dataclasses import dataclass
from typing import Any

logger = logging.getLogger(__name__)

# The reward signals a stored `score_breakdown` carries. Deliberately spelled
# here rather than discovered from the JSON: a diagnostic that silently grows a
# column when a new key appears would change what "controlled for" means
# between two runs without anyone deciding to.
TUNABLE_SIGNALS = (
    "source_relevance",
    "topic_score",
    "text_similarity",
    "author_affinity",
    "journal_affinity",
    "recency_boost",
    "citation_quality",
    "feedback_adj",
    "preference_affinity",
)

EVENTS_PER_VARIABLE_FLOOR = 10.0
"""Events per variable a logistic fit needs before its coefficients mean much.

The classical rule (Peduzzi et al. 1996) counts events in the RARER class, so
32 negatives over 9 signals is ~3.6 — a third of the floor. This is the number
that decides whether the table is evidence or decoration, so it is named."""

RIDGE_STRENGTH = 1.0
"""L2 penalty. Deliberately strong: at this sample size an unregularised fit
would separate the classes and report enormous coefficients with no meaning."""

MIN_ROWS_TO_FIT = 30
MIN_PER_CLASS = 8
"""Below either of these no fit is attempted at all. Reporting a coefficient
from five positives is worse than reporting nothing, because it looks the same
as a real one."""

ROLLING_WINDOWS = 3
"""How many equal-count time slices the rolling view uses.

Equal COUNT, not equal duration: usage is bursty, and equal-duration windows
would hand one slice 80 rows and another 4, then invite a comparison between
them."""


@dataclass(frozen=True)
class Observation:
    """One impressed recommendation with a resolved outcome."""

    paper_id: str
    suggestion_set_id: str
    at: str
    label: int
    values: dict[str, float]


@dataclass(frozen=True)
class SignalCoefficient:
    signal: str
    partial_beta: float
    prior_weight: float

    def as_dict(self) -> dict[str, Any]:
        return {
            "signal": self.signal,
            "partial_beta": round(self.partial_beta, 4),
            "prior_weight": round(self.prior_weight, 4),
        }


def signal_tuning(conn: sqlite3.Connection) -> dict[str, Any]:
    """Partial coefficients, their drift over time, and shadow-vs-prior scoring.

    Pure read. Returns a payload that always carries its own sample context, so
    a caller cannot render the coefficients without the numbers that say how
    much to trust them.
    """
    observations = _load_observations(conn)
    priors = _configured_weights(conn)

    overall = _fit(observations, priors)
    rolling = _rolling(observations, priors)
    shadow = _shadow_vs_prior(conn)

    n_pos = sum(1 for o in observations if o.label == 1)
    n_neg = len(observations) - n_pos
    rarer = min(n_pos, n_neg)
    epv = (rarer / len(TUNABLE_SIGNALS)) if TUNABLE_SIGNALS else 0.0

    return {
        "report_type": "signal_tuning",
        "n_observations": len(observations),
        "n_positive": n_pos,
        "n_negative": n_neg,
        "n_distinct_papers": len({o.paper_id for o in observations}),
        "n_suggestion_sets": len({o.suggestion_set_id for o in observations}),
        "events_per_variable": round(epv, 2),
        "admissible_parameters": int(rarer / EVENTS_PER_VARIABLE_FLOOR),
        "over_parameterised": epv < EVENTS_PER_VARIABLE_FLOOR,
        # Every coefficient here is fitted on the rows it is evaluated on.
        # There is no held-out estimate and the payload must not imply one.
        "in_sample": True,
        "fitted": overall is not None,
        "coefficients": [c.as_dict() for c in (overall or [])],
        "rolling": rolling,
        "shadow": shadow,
        "note": (
            "Partial coefficients hold the other signals fixed; the marginal "
            "differences in signal_impact do not. Where the two disagree, the "
            "marginal one is reading a confound. In-sample and, at this sample "
            "size, over-parameterised — read the direction, not the magnitude."
        ),
    }


def _load_observations(conn: sqlite3.Connection) -> list[Observation]:
    """Impressed recommendations with a positive/negative outcome and a breakdown.

    Same cohort as ``signal_impact`` on purpose: two diagnostics of the same
    data that disagreed about which rows they described would be unreadable.
    """
    from alma.application.recommendation_outcomes import build_recommendation_outcomes

    out: list[Observation] = []
    for rec in build_recommendation_outcomes(conn):
        if not rec.is_seen:
            continue
        if rec.is_positive:
            label = 1
        elif rec.is_negative:
            label = 0
        else:
            continue
        raw = (rec.score_breakdown or "").strip()
        if not raw:
            continue
        try:
            breakdown = json.loads(raw)
        except (json.JSONDecodeError, TypeError):
            continue
        values: dict[str, float] = {}
        for key in TUNABLE_SIGNALS:
            entry = breakdown.get(key)
            if isinstance(entry, dict) and "value" in entry:
                values[key] = float(entry["value"])
            elif isinstance(entry, (int, float)):
                values[key] = float(entry)
        if len(values) != len(TUNABLE_SIGNALS):
            continue
        out.append(
            Observation(
                paper_id=rec.paper_id,
                suggestion_set_id=str(rec.suggestion_set_id or ""),
                # When the reaction happened, falling back to when the row was
                # created. Ordering by the REACTION is what makes the rolling
                # view a story about your taste rather than about our crawl.
                at=str(rec.action_at or rec.created_at or ""),
                label=label,
                values=values,
            )
        )
    # Time order is what makes the rolling view a TREND rather than an
    # arbitrary partition.
    out.sort(key=lambda o: o.at)
    return out


def _configured_weights(conn: sqlite3.Connection) -> dict[str, float]:
    """The weights actually in force, so the fit can be read against them."""
    from alma.discovery.defaults import DISCOVERY_SETTINGS_DEFAULTS

    stored = {
        str(row["key"]): str(row["value"])
        for row in conn.execute("SELECT key, value FROM discovery_settings").fetchall()
    }
    out: dict[str, float] = {}
    for signal in TUNABLE_SIGNALS:
        key = f"weights.{signal}"
        raw = stored.get(key, DISCOVERY_SETTINGS_DEFAULTS.get(key, "0"))
        try:
            out[signal] = float(raw)
        except (TypeError, ValueError):
            out[signal] = 0.0
    return out


def _fit(
    observations: list[Observation],
    priors: dict[str, float],
) -> list[SignalCoefficient] | None:
    """Standardised ridge-logistic coefficients, or None when unfittable.

    Standardising first is what makes the coefficients comparable to each
    other: without it a signal that happens to live on a wider scale reports a
    smaller β for the same real effect.
    """
    if len(observations) < MIN_ROWS_TO_FIT:
        return None
    labels = [o.label for o in observations]
    if sum(labels) < MIN_PER_CLASS or (len(labels) - sum(labels)) < MIN_PER_CLASS:
        return None

    import numpy as np
    from sklearn.linear_model import LogisticRegression

    matrix = np.asarray(
        [[o.values[s] for s in TUNABLE_SIGNALS] for o in observations],
        dtype=np.float64,
    )
    y = np.asarray(labels, dtype=np.int64)

    centre = matrix.mean(axis=0)
    spread = matrix.std(axis=0)
    # A constant column carries no information; leaving its spread at 0 would
    # divide by zero, and imputing one would invent variance that is not there.
    spread[spread < 1e-9] = 1.0
    standardised = (matrix - centre) / spread

    try:
        model = LogisticRegression(
            penalty="l2",
            C=1.0 / RIDGE_STRENGTH,
            solver="lbfgs",
            max_iter=2000,
        ).fit(standardised, y)
    except Exception as exc:  # noqa: BLE001 — a diagnostic must not sink a report
        logger.warning("Signal tuning fit failed: %s", exc)
        return None

    betas = model.coef_[0]
    return [
        SignalCoefficient(
            signal=signal,
            partial_beta=float(beta),
            prior_weight=float(priors.get(signal, 0.0)),
        )
        for signal, beta in zip(TUNABLE_SIGNALS, betas, strict=True)
    ]


def _rolling(
    observations: list[Observation],
    priors: dict[str, float],
) -> list[dict[str, Any]]:
    """Refit over equal-count time slices, so weight drift is visible.

    A window that cannot support a fit reports ``fitted: false`` with its dates
    rather than being dropped: a gap in the trend is information, and silently
    omitting it would make the remaining windows look like a continuous series.
    """
    if len(observations) < MIN_ROWS_TO_FIT * 2:
        return []
    size = math.ceil(len(observations) / ROLLING_WINDOWS)
    windows: list[dict[str, Any]] = []
    for start in range(0, len(observations), size):
        slice_ = observations[start : start + size]
        if not slice_:
            continue
        fit = _fit(slice_, priors)
        windows.append(
            {
                "from": slice_[0].at[:10],
                "to": slice_[-1].at[:10],
                "n": len(slice_),
                "n_positive": sum(1 for o in slice_ if o.label == 1),
                "fitted": fit is not None,
                "coefficients": [c.as_dict() for c in (fit or [])],
            }
        )
    return windows


def _shadow_vs_prior(conn: sqlite3.Connection) -> dict[str, Any]:
    """How differently the shadow ranker would have ordered the same decks.

    Compared on the ranking rows themselves rather than on outcomes, because
    that comparison is available immediately and for every candidate — you can
    see what promoting the head WOULD do before there is enough feedback to say
    whether it would be better.
    """
    rows = conn.execute(
        """
        SELECT suggestion_set_id, prior_score, shadow_score
        FROM discovery_ranking_candidates
        WHERE shadow_score IS NOT NULL
        ORDER BY suggestion_set_id, prior_score DESC
        """
    ).fetchall()
    if not rows:
        return {
            "available": False,
            "reason": "no candidate has a shadow score yet",
            "n_sets": 0,
            "n_candidates": 0,
        }

    by_set: dict[str, list[tuple[float, float]]] = {}
    for row in rows:
        by_set.setdefault(str(row["suggestion_set_id"]), []).append(
            (float(row["prior_score"]), float(row["shadow_score"]))
        )

    displacements: list[float] = []
    top10_overlap: list[float] = []
    for pairs in by_set.values():
        if len(pairs) < 2:
            continue
        prior_order = sorted(range(len(pairs)), key=lambda i: -pairs[i][0])
        shadow_order = sorted(range(len(pairs)), key=lambda i: -pairs[i][1])
        rank_of = {idx: pos for pos, idx in enumerate(shadow_order)}
        displacements.append(
            sum(abs(rank_of[idx] - pos) for pos, idx in enumerate(prior_order)) / len(pairs)
        )
        k = min(10, len(pairs))
        overlap = len(set(prior_order[:k]) & set(shadow_order[:k])) / k
        top10_overlap.append(overlap)

    if not displacements:
        return {
            "available": False,
            "reason": "no suggestion set has two or more shadow-scored candidates",
            "n_sets": len(by_set),
            "n_candidates": len(rows),
        }

    return {
        "available": True,
        "reason": None,
        "n_sets": len(by_set),
        "n_candidates": len(rows),
        "mean_rank_displacement": round(sum(displacements) / len(displacements), 3),
        "mean_top10_overlap": round(sum(top10_overlap) / len(top10_overlap), 3),
    }
