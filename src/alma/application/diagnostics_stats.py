"""Shared small-sample statistics for Insights diagnostics & reports.

Why this module exists — findings **I-23 / I-25 / I-26 / I-30 / I-31**: several
Insights surfaces need the SAME three things and previously either hand-rolled
them inconsistently or skipped them entirely, presenting statistical noise as
fact (a 3-paper branch labelled "boost", a single empty alert window scored
"good", a ±0.01 mean delta called "impact"). This is the ONE place that answers:

1. **"Do we even have enough data to say anything?"** — :func:`is_sufficient`
   plus the ``MIN_*`` thresholds give an explicit sufficiency gate, so a card or
   metric returns *insufficient data* instead of a misleading 0 (which the UI
   reads as "critical") or an optimistic default (read as "good"). (I-23/I-26)
2. **A rate with honest uncertainty** — :func:`wilson_interval` /
   :class:`RateEstimate` give the Wilson score interval for a binomial
   proportion. Wilson behaves well at small N (unlike the normal/Wald
   approximation, which produces intervals that escape ``[0, 1]`` and collapse
   to width 0 at p=0 or p=1), so we only call a rate "good"/"poor" when even the
   conservative bound clears the threshold. Used to gate prescriptive
   "boost/mute" branch advice (I-25).
3. **A two-sample comparison with uncertainty** — :func:`compare_means` /
   :class:`MeanComparison` compare a scoring component's mean between
   positively- and negatively-received papers. The honest statistic is the
   difference of means with a confidence interval (Welch, unequal variances)
   plus a standardized effect size (Cohen's d); a delta is only called a real
   association when the interval excludes zero on an adequately-powered cohort
   (I-31), never on an arbitrary ±0.01 cutoff.

Deliberately dependency-free (pure ``math``) so it runs anywhere in the read
path without pulling numpy/scipy into a request.
"""

from __future__ import annotations

import math
from dataclasses import dataclass

# ── Sufficiency thresholds (the single greppable source of truth) ──────────
#
# Below these counts a surface must show "insufficient data" rather than a
# directional claim or prescriptive advice. These are deliberate floors for a
# personal-scale tool, not statistical-power calculations:
#
# * MIN_RATE_SAMPLE — minimum trials before a binomial rate (engagement,
#   positive-rate, reliability) may drive a "good/poor" verdict or a
#   boost/mute recommendation. At ~8 trials the 95% Wilson interval has
#   narrowed enough to separate "mostly positive" from "mostly negative" for
#   the rates we see in practice; at 3-4 it spans almost the whole [0, 1] range.
# * MIN_GROUP_SAMPLE — minimum per-group size for a two-sample (liked vs
#   dismissed) mean comparison to be reported as an association.
MIN_RATE_SAMPLE = 8
MIN_GROUP_SAMPLE = 5

# Standard-normal quantile for a 95% two-sided interval (z_{0.975}). We use the
# normal quantile rather than a per-df Student-t because the surfaces here are
# descriptive uncertainty bands, not formal hypothesis tests, and avoiding a
# t-distribution table keeps the module dependency-free.
_Z_95 = 1.959963984540054


def is_sufficient(total: int, *, minimum: int = MIN_RATE_SAMPLE) -> bool:
    """True when ``total`` observations clear the sufficiency floor."""
    return int(total or 0) >= minimum


def wilson_interval(
    successes: int, total: int, *, z: float = _Z_95
) -> tuple[float, float]:
    """95% Wilson score interval ``(lo, hi)`` for a binomial proportion.

    Unlike the Wald interval ``p ± z·sqrt(p(1-p)/n)`` (which underflows below 0
    / overflows above 1 and degenerates to zero width at the extremes), the
    Wilson interval stays inside ``[0, 1]`` and keeps a sensible width at small
    ``n`` — exactly the regime a personal library lives in. Returns ``(0, 0)``
    for an empty sample so callers treat it as "no information".
    """
    n = int(total or 0)
    if n <= 0:
        return (0.0, 0.0)
    p = successes / n
    z2 = z * z
    denom = 1.0 + z2 / n
    center = (p + z2 / (2 * n)) / denom
    margin = (z * math.sqrt((p * (1 - p) + z2 / (4 * n)) / n)) / denom
    return (max(0.0, center - margin), min(1.0, center + margin))


@dataclass(frozen=True)
class RateEstimate:
    """A binomial rate with its sample size and Wilson uncertainty band.

    Carries enough to render "12/40 (30%, 95% CI 18–45%)" and to gate advice:
    :meth:`confidently_above` / :meth:`confidently_below` require BOTH a
    sufficient sample AND the conservative Wilson bound to clear the threshold,
    so a lucky 2-of-2 can never trigger a "boost" verdict.
    """

    successes: int
    total: int

    @property
    def rate(self) -> float:
        return round(self.successes / self.total, 4) if self.total else 0.0

    @property
    def ci(self) -> tuple[float, float]:
        lo, hi = wilson_interval(self.successes, self.total)
        return (round(lo, 4), round(hi, 4))

    @property
    def sufficient(self) -> bool:
        return is_sufficient(self.total)

    def confidently_above(self, threshold: float) -> bool:
        """Sufficient sample AND even the lower CI bound beats ``threshold``."""
        return self.sufficient and self.ci[0] >= threshold

    def confidently_below(self, threshold: float) -> bool:
        """Sufficient sample AND even the upper CI bound is under ``threshold``."""
        return self.sufficient and self.ci[1] <= threshold


@dataclass(frozen=True)
class MeanComparison:
    """Difference of two sample means (a − b) with a Welch CI and Cohen's d.

    ``direction`` is the honest verdict: ``"higher"`` / ``"lower"`` only when the
    cohort is adequately powered AND the 95% CI for the difference excludes 0;
    otherwise ``"inconclusive"``. This is what replaces the old ±0.01 cutoff in
    signal-impact (I-31).
    """

    n_a: int
    mean_a: float
    n_b: int
    mean_b: float
    diff: float
    ci_low: float
    ci_high: float
    cohens_d: float

    @property
    def sufficient(self) -> bool:
        return self.n_a >= MIN_GROUP_SAMPLE and self.n_b >= MIN_GROUP_SAMPLE

    @property
    def direction(self) -> str:
        if not self.sufficient:
            return "inconclusive"
        if self.ci_low > 0.0:
            return "higher"
        if self.ci_high < 0.0:
            return "lower"
        return "inconclusive"


def _mean_var(values: list[float]) -> tuple[float, float]:
    """Return (mean, sample variance). Variance is 0 for n < 2."""
    n = len(values)
    if n == 0:
        return (0.0, 0.0)
    mean = sum(values) / n
    if n < 2:
        return (mean, 0.0)
    var = sum((v - mean) ** 2 for v in values) / (n - 1)
    return (mean, var)


def compare_means(a: list[float], b: list[float], *, z: float = _Z_95) -> MeanComparison:
    """Welch (unequal-variance) comparison of two samples, a − b.

    Uses the Welch standard error ``sqrt(var_a/n_a + var_b/n_b)`` and a normal
    quantile for the CI (descriptive band, not a formal test — see module
    docstring). Cohen's d uses the pooled SD; it is 0 when the pooled SD is 0
    (e.g. constant or single-value groups) to avoid a divide-by-zero.
    """
    n_a, n_b = len(a), len(b)
    mean_a, var_a = _mean_var(a)
    mean_b, var_b = _mean_var(b)
    diff = mean_a - mean_b

    se = math.sqrt((var_a / n_a if n_a else 0.0) + (var_b / n_b if n_b else 0.0))
    margin = z * se
    ci_low, ci_high = diff - margin, diff + margin

    # Pooled SD for Cohen's d (standardized effect size).
    if n_a >= 2 and n_b >= 2:
        pooled_var = ((n_a - 1) * var_a + (n_b - 1) * var_b) / (n_a + n_b - 2)
        pooled_sd = math.sqrt(pooled_var) if pooled_var > 0 else 0.0
    else:
        pooled_sd = 0.0
    cohens_d = diff / pooled_sd if pooled_sd > 0 else 0.0

    return MeanComparison(
        n_a=n_a,
        mean_a=round(mean_a, 4),
        n_b=n_b,
        mean_b=round(mean_b, 4),
        diff=round(diff, 4),
        ci_low=round(ci_low, 4),
        ci_high=round(ci_high, 4),
        cohens_d=round(cohens_d, 4),
    )


def shannon_evenness(counts: list[int]) -> float:
    """Normalized Shannon entropy (Pielou's evenness) in ``[0, 1]``.

    A genuine diversity metric for I-29 (collection topic diversity) that
    replaces ``len(top_five)`` — which maxed at 5 and ignored the *distribution*
    entirely. 0 = one topic dominates; 1 = topics are perfectly even. Returns 0
    for fewer than two distinct topics (no diversity to speak of).
    """
    positive = [c for c in counts if c > 0]
    k = len(positive)
    if k < 2:
        return 0.0
    total = sum(positive)
    entropy = -sum((c / total) * math.log(c / total) for c in positive)
    # Divide by the maximum possible entropy (log k) to normalize to [0, 1].
    return round(entropy / math.log(k), 4)
