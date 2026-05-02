"""Outcome-based reweighting of retrieval sources.

Each recommendation row is stamped with `source_type`, `source_api`,
`source_key`, `branch_mode` at retrieval time. Each downstream user
action lands in `feedback_events` keyed by paper_id. Joining the two
gives a count of positive vs negative outcomes per source attribute.
We smooth with Bayesian priors so a fresh DB (zero traffic) returns a
neutral 1.0 multiplier — no behavior change until enough events exist
to move the prior. Old events fall outside the window and stop counting.

The output is a `{source_key: multiplier}` map in `[lo, hi]` (default
`[0.5, 1.5]`) intended to multiply `source_relevance` before the
10-signal scorer runs. The cap stops a single bad week from killing a
source; the floor stops a hot week from making one source dominate.
"""

from __future__ import annotations

import json
import sqlite3
from dataclasses import dataclass, field
from datetime import datetime, timezone

from alma.application.signal_projection import normalize_feedback_event_value
from alma.core.scoring_math import age_decay, clamp


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


_SOURCE_KEY_EXPR = (
    "COALESCE(NULLIF(TRIM(source_api), ''), NULLIF(TRIM(source_type), ''))"
)
# Calibration dimensions supported on the recommendations table. Each
# maps a logical name to the SQL key expression that picks the column
# (or composite COALESCE) used as the calibration key. Add new
# dimensions here, not by writing parallel functions.
_DIMENSION_KEY_EXPR: dict[str, str] = {
    "source_api": _SOURCE_KEY_EXPR,
    "branch_mode": "NULLIF(TRIM(branch_mode), '')",
    "branch_id": "NULLIF(TRIM(branch_id), '')",
}


def compute_outcome_calibration(
    db: sqlite3.Connection,
    *,
    dimension: str = "source_api",
    window_days: float = _DEFAULT_WINDOW_DAYS,
    half_life_days: float = _DEFAULT_HALF_LIFE_DAYS,
    multiplier_lo: float = _DEFAULT_MULTIPLIER_LO,
    multiplier_hi: float = _DEFAULT_MULTIPLIER_HI,
) -> OutcomeCalibration:
    """Compute per-key quality multipliers from observed outcomes.

    `dimension` selects the calibration axis: `source_api` (default,
    paper-Discovery API quality), `branch_mode` (retrieval lane:
    `core` / `explore` / `safe`), or `branch_id` (per-branch outcome
    quality). All three share the same Bayesian smoothing and time-
    decay shape — the dimension just changes which column groups
    the events.

    Returns an empty result when the necessary tables are missing
    (fresh DB, mid-migration) or the dimension is unknown. Callers
    default missing keys to a 1.0 multiplier — "no opinion" rather
    than "downweight".
    """
    out = OutcomeCalibration()
    key_expr = _DIMENSION_KEY_EXPR.get(dimension)
    if key_expr is None:
        return out
    # Both queries reference the recommendations columns by bare name —
    # `feedback_events` doesn't share any of (source_api, source_type,
    # branch_mode, branch_id) so SQLite resolves the bare references
    # unambiguously without needing the `r.` prefix.
    try:
        rows = db.execute(
            f"""
            SELECT
                {key_expr} AS dim_key,
                fe.event_type AS event_type,
                fe.value      AS event_value,
                fe.created_at AS created_at
            FROM recommendations r
            JOIN feedback_events fe
              ON fe.entity_id = r.paper_id
             AND fe.entity_type IN ('publication', 'paper')
            WHERE {key_expr} IS NOT NULL
            """
        ).fetchall()
    except sqlite3.OperationalError:
        return out

    now = datetime.now(timezone.utc)
    impression_rows = []
    try:
        impression_rows = db.execute(
            f"""
            SELECT
                {key_expr} AS dim_key,
                COUNT(*) AS impressions
            FROM recommendations
            WHERE {key_expr} IS NOT NULL
            GROUP BY 1
            """
        ).fetchall()
    except sqlite3.OperationalError:
        impression_rows = []
    for row in impression_rows:
        key = str(row["dim_key"] or "").strip().lower()
        if key:
            out.impressions[key] = int(row["impressions"] or 0)

    for row in rows:
        dim_key = str(row["dim_key"] or "").strip().lower()
        if not dim_key:
            continue
        signal = normalize_feedback_event_value(row["event_type"], row["event_value"])
        if signal == 0.0:
            continue
        age_days = _days_since(row["created_at"], now)
        if age_days is not None and age_days > window_days:
            continue
        weight = age_decay(age_days, half_life_days=half_life_days)
        if signal > 0:
            out.positive_counts[dim_key] = (
                out.positive_counts.get(dim_key, 0.0) + signal * weight
            )
        else:
            out.negative_counts[dim_key] = (
                out.negative_counts.get(dim_key, 0.0) + abs(signal) * weight
            )

    keys = set(out.positive_counts) | set(out.negative_counts) | set(out.impressions)
    for key in keys:
        positives = out.positive_counts.get(key, 0.0)
        negatives = out.negative_counts.get(key, 0.0)
        # Beta-Bernoulli posterior mean. With α=β=2 priors and zero
        # traffic, returns 0.5 — neutral, no opinion.
        quality = (positives + _PRIOR_ALPHA) / (
            positives + negatives + _PRIOR_ALPHA + _PRIOR_BETA
        )
        out.quality[key] = quality
        # Map quality ∈ [0, 1] to a multiplier ∈ [lo, hi] linearly,
        # with quality=0.5 (neutral) → 1.0 (no scaling).
        center = (multiplier_lo + multiplier_hi) / 2.0
        spread = (multiplier_hi - multiplier_lo) / 2.0
        multiplier = center + spread * ((quality * 2.0) - 1.0)
        out.multipliers[key] = clamp(multiplier, multiplier_lo, multiplier_hi)
    return out


def _days_since(raw, now: datetime) -> float | None:
    if not raw:
        return None
    text = str(raw).strip()
    if not text:
        return None
    if text.endswith("Z"):
        text = f"{text[:-1]}+00:00"
    try:
        dt = datetime.fromisoformat(text)
    except ValueError:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return max(0.0, (now - dt.astimezone(timezone.utc)).total_seconds() / 86400.0)


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


def compose_calibration_multipliers(
    *multipliers: float,
    multiplier_lo: float = _DEFAULT_MULTIPLIER_LO,
    multiplier_hi: float = _DEFAULT_MULTIPLIER_HI,
) -> float:
    """Combine N independent calibration multipliers into one band-limited value.

    Each individual multiplier sits in `[lo, hi]` (default `[0.5, 1.5]`).
    Naively multiplying three of them could overshoot to `3.375x` or
    crash to `0.125x`. We compose in log-space so neutral inputs (1.0)
    are identity, then clamp the result back into the same band so a
    candidate hot on three independent axes still maxes at `1.5x` —
    the same ceiling a single-axis hot signal would have. This keeps
    one axis from quietly dominating the overall multiplier when N>1.
    """
    import math

    log_sum = 0.0
    for m in multipliers:
        if m <= 0:
            continue
        log_sum += math.log(m)
    composite = math.exp(log_sum) if multipliers else 1.0
    return clamp(composite, multiplier_lo, multiplier_hi)


def calibration_multiplier_for(
    calibration: OutcomeCalibration | None,
    source_api: str | None,
    source_type: str | None,
) -> float:
    """Return the multiplier for a single candidate.

    Falls through `source_api` → `source_type` → 1.0 (no calibration).
    Callers thread this in once per candidate.
    """
    if calibration is None or not calibration.multipliers:
        return 1.0
    for key in (source_api, source_type):
        if key:
            normalized = str(key).strip().lower()
            if normalized in calibration.multipliers:
                return calibration.multipliers[normalized]
    return 1.0
