"""Outcome-based reweighting from observed follow / reject outcomes.

Today this owns the **author-suggestion bucket** calibration: each suggestion
bucket's smoothed follow rate becomes a multiplier in ``MULTIPLIER_BAND``. A
fresh DB returns a neutral 1.0 — no behaviour change until events exist.

The paper side lives elsewhere since 2026-07-27: how a paper reached the user
is exposure and stays out of the score, so the per-source score multiplier
that used to be computed here is gone. Its successor scales *retrieval*
channel weights instead — ``application/discovery/channel_yield.py`` — and
shares this module's band.
"""

from __future__ import annotations

import sqlite3
from dataclasses import dataclass, field
from datetime import datetime, timezone

from alma.core.scoring_math import age_decay, clamp
from alma.core.scoring_math import days_since as _days_since

# Bayesian priors. α=β means the prior peaks at 0.5 (no opinion); a
# higher sum means more "data" is needed to move the smoothed estimate
# away from neutral. 2/2 is a soft prior — a source with one save and
# one dismiss returns 0.5 (still neutral); a source with 10 saves and 0
# dismisses returns ~0.83 (clearly positive but not saturated).
_PRIOR_ALPHA = 2.0
_PRIOR_BETA = 2.0
_DEFAULT_WINDOW_DAYS = 180.0
_DEFAULT_HALF_LIFE_DAYS = 60.0
_DEFAULT_MULTIPLIER_LO = 0.5
_DEFAULT_MULTIPLIER_HI = 1.5
#: The band every outcome-derived multiplier stays in: a bad week cannot kill a
#: source and a hot week cannot make one dominate.
MULTIPLIER_BAND = (_DEFAULT_MULTIPLIER_LO, _DEFAULT_MULTIPLIER_HI)


@dataclass
class OutcomeCalibration:
    """Result of one calibration pass.

    `multipliers` is what scoring needs; the rest is for diagnostics —
    so a developer can read the retrieval summary and see whether a
    quality estimate is grounded in real traffic or still mostly prior.
    """

    multipliers: dict[str, float] = field(default_factory=dict)
    quality: dict[str, float] = field(default_factory=dict)
    positive_counts: dict[str, float] = field(default_factory=dict)
    negative_counts: dict[str, float] = field(default_factory=dict)
    impressions: dict[str, int] = field(default_factory=dict)


def _finalize_calibration(out, *, multiplier_lo: float, multiplier_hi: float):
    """Beta-Bernoulli posterior mean per key (alpha=beta=2 priors -> neutral 0.5
    at zero traffic) mapped linearly to a multiplier in [lo, hi] (quality 0.5 ->
    1.0, no scaling). Shared by the paper-Discovery + author-bucket calibrators."""
    keys = set(out.positive_counts) | set(out.negative_counts) | set(out.impressions)
    for key in keys:
        positives = out.positive_counts.get(key, 0.0)
        negatives = out.negative_counts.get(key, 0.0)
        quality = (positives + _PRIOR_ALPHA) / (
            positives + negatives + _PRIOR_ALPHA + _PRIOR_BETA
        )
        out.quality[key] = quality
        center = (multiplier_lo + multiplier_hi) / 2.0
        spread = (multiplier_hi - multiplier_lo) / 2.0
        multiplier = center + spread * ((quality * 2.0) - 1.0)
        out.multipliers[key] = clamp(multiplier, multiplier_lo, multiplier_hi)
    return out


def compute_author_bucket_calibration(
    db: sqlite3.Connection,
    *,
    window_days: float = _DEFAULT_WINDOW_DAYS,
    half_life_days: float = _DEFAULT_HALF_LIFE_DAYS,
    multiplier_lo: float = _DEFAULT_MULTIPLIER_LO,
    multiplier_hi: float = _DEFAULT_MULTIPLIER_HI,
) -> OutcomeCalibration:
    """Per-bucket quality for the Suggested Authors rail.

    Reads from two author-side log tables that the rail's follow /
    reject routes populate with bucket attribution:

      - `author_suggestion_follow_log`     → positive outcomes
      - `missing_author_feedback`          → negative outcomes (`action='remove'`)

    Same Bayesian smoothing + time decay shape as paper-Discovery
    calibration. Empty when neither table exists or carries bucket-
    attributed rows. The recommendations table is irrelevant here —
    author rail suggestions don't write into it.
    """
    out = OutcomeCalibration()
    now = datetime.now(timezone.utc)

    # Positive outcomes — every follow logged through the rail.
    try:
        rows = db.execute(
            """
            SELECT lower(trim(suggestion_bucket)) AS bucket, created_at
            FROM author_suggestion_follow_log
            WHERE COALESCE(TRIM(suggestion_bucket), '') <> ''
            """
        ).fetchall()
    except sqlite3.OperationalError:
        rows = []
    for row in rows:
        bucket = str(row["bucket"] or "").strip().lower()
        if not bucket:
            continue
        age_days = _days_since(row["created_at"], now)
        if age_days is not None and age_days > window_days:
            continue
        weight = age_decay(age_days, half_life_days=half_life_days)
        out.positive_counts[bucket] = out.positive_counts.get(bucket, 0.0) + weight

    # Negative outcomes — every reject from the rail (signal_value < 0).
    try:
        rows = db.execute(
            """
            SELECT lower(trim(suggestion_bucket)) AS bucket, signal_value, created_at
            FROM missing_author_feedback
            WHERE COALESCE(TRIM(suggestion_bucket), '') <> ''
            """
        ).fetchall()
    except sqlite3.OperationalError:
        rows = []
    for row in rows:
        bucket = str(row["bucket"] or "").strip().lower()
        if not bucket:
            continue
        signal = float(row["signal_value"] or 0.0)
        if signal >= 0:
            continue
        age_days = _days_since(row["created_at"], now)
        if age_days is not None and age_days > window_days:
            continue
        weight = age_decay(age_days, half_life_days=half_life_days)
        out.negative_counts[bucket] = (
            out.negative_counts.get(bucket, 0.0) + abs(signal) * weight
        )

    # Impressions — count of distinct authors per bucket the rail has
    # surfaced over its lifetime. Used as a sanity check ("how grounded
    # is this estimate?"); not part of the smoothing.
    try:
        impression_rows = db.execute(
            """
            SELECT bucket, COUNT(*) AS n FROM (
                SELECT lower(trim(suggestion_bucket)) AS bucket
                FROM author_suggestion_follow_log
                WHERE COALESCE(TRIM(suggestion_bucket), '') <> ''
                UNION ALL
                SELECT lower(trim(suggestion_bucket)) AS bucket
                FROM missing_author_feedback
                WHERE COALESCE(TRIM(suggestion_bucket), '') <> ''
            )
            GROUP BY bucket
            """
        ).fetchall()
    except sqlite3.OperationalError:
        impression_rows = []
    for row in impression_rows:
        bucket = str(row["bucket"] or "").strip().lower()
        if bucket:
            out.impressions[bucket] = int(row["n"] or 0)

    return _finalize_calibration(out, multiplier_lo=multiplier_lo, multiplier_hi=multiplier_hi)
