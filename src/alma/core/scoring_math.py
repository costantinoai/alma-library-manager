"""Shared scoring primitives used across paper Discovery and the author rail.

These four helpers were duplicated across `discovery.scoring`,
`application.signal_projection`, `application.authors`,
`application.discovery`, `application.feed`, `application.gap_radar`,
`application.paper_signal`, and `discovery.source_search` — sometimes
multiple times in the same file. Consolidating here means a calibration
change (e.g. tuning the consensus bonus fraction or the half-life
default) takes effect everywhere by construction.
"""

from __future__ import annotations

import math
from collections.abc import Mapping
from datetime import datetime, timezone

from alma.core.utils import normalize_text

# Tokens too common to discriminate results for the query-text match.
# Deliberately tiny — only glue words that appear in almost every academic
# title. Dropping them stops "the role of X in Y" queries from scoring every
# paper containing "the/of/in".
QUERY_STOPWORDS = frozenset(
    {"a", "an", "and", "are", "at", "by", "for", "from", "in", "is", "of", "on", "or", "the", "to", "with"}
)


def clamp(value: float, lo: float, hi: float) -> float:
    """Constrain ``value`` to ``[lo, hi]``."""
    return max(lo, min(hi, value))


# Reciprocal-rank-fusion smoothing constant (Cormack et al., SIGIR 2009). 60 is
# the value from that paper and the de-facto standard: it compresses the gap
# between rank 1 and rank 10 enough that agreement ACROSS retrievers outweighs
# any single retriever's top slot.
#
# Declared here, once. Three modules fuse ranks — the Find & Add search merge,
# the Discovery lexical lane, and the two-level channel merge — and each used
# to carry its own `60` and its own inline `1/(k+rank)`. A constant that means
# "how much do we trust rank position" cannot live in three files.
RRF_K: int = 60


def rrf_weight(rank: int, *, k: int = RRF_K) -> float:
    """Contribution of ONE appearance at 1-based ``rank`` to an RRF score.

    Summed across every list a candidate appears in. Unbounded above (a
    candidate in many lists scores higher), which is the property that makes
    RRF reward consensus.
    """
    return 1.0 / (k + max(1, int(rank)))


def rrf_score_normalized(rank: int, *, k: int = RRF_K) -> float:
    """RRF contribution rescaled so rank 1 == 1.0.

    Used where a fused value has to stay on a comparable 0–1 scale before being
    multiplied by a channel weight — summing raw ``rrf_weight`` there would let
    the absolute pool size leak into the blend.
    """
    return (k + 1.0) / (k + max(1, int(rank)))


def query_tokens(query: str) -> tuple[str, list[str]]:
    """``(normalized_query, discriminating_tokens)`` for `query_match_score`.

    Glue words are dropped unless the query is *nothing but* glue words, in
    which case they are kept so a degenerate query still matches something.
    """
    query_norm = normalize_text(query or "")
    all_tokens = query_norm.split()
    tokens = [t for t in all_tokens if t not in QUERY_STOPWORDS] or all_tokens
    return query_norm, tokens


def query_match_score(query_norm: str, tokens: list[str], candidate: Mapping) -> float:
    """Lexical closeness of one candidate to the search query, in ``[0, 1]``.

    Two ingredients, both over `normalize_text` output:

    - token coverage: fraction of query tokens found in the title/authors
      (full weight) or the abstract (half weight — a token buried in the
      abstract is weaker evidence than one in the title);
    - exact phrase: the whole normalized query appearing inside the title
      (or, weaker, the abstract) — the strongest "this is the paper I typed"
      signal, e.g. pasting a full title.

    Lives here rather than in `discovery.source_search` because the Semantic
    Scholar bulk adapter needs it too: `/paper/search/bulk` has **no relevance
    sort** (only `paperId` / `publicationDate` / `citationCount`), so the
    ordering has to be reconstructed locally from text. Importing it from
    `source_search` would be circular — that module imports the adapter.
    """
    if not tokens:
        return 0.0
    title_norm = normalize_text(str(candidate.get("title") or ""))
    authors_norm = normalize_text(str(candidate.get("authors") or ""))
    abstract_norm = normalize_text(str(candidate.get("abstract") or ""))
    strong_tokens = set(title_norm.split()) | set(authors_norm.split())
    abstract_tokens = set(abstract_norm.split())

    covered = sum(
        1.0 if token in strong_tokens else (0.5 if token in abstract_tokens else 0.0)
        for token in tokens
    )
    coverage = covered / len(tokens)
    phrase = 1.0 if query_norm in title_norm else (0.5 if query_norm in abstract_norm else 0.0)
    return clamp((0.8 * coverage) + (0.2 * phrase), 0.0, 1.0)


def rank_score(index: int, total: int, *, ndigits: int = 4) -> float:
    """Descending position score in ``[0, 1]``: ``1.0`` for the top result,
    decaying linearly to ``0`` at the tail.

    ``round(max(0.0, 1.0 - index / max(total, 1)), ndigits)``. The discovery
    source adapters (arxiv / crossref / openalex_related / semantic_scholar)
    use this to turn a result's rank into a relevance proxy when the source
    returns no numeric score. ``max(total, 1)`` guards the empty case.
    """
    return round(max(0.0, 1.0 - (index / max(total, 1))), ndigits)


def age_decay(age_days: float | None, *, half_life_days: float) -> float:
    """Half-life decay factor in ``(0, 1]``.

    Returns ``1.0`` when ``age_days`` is ``None`` (treat as fresh) or
    ``0.5 ** (age_days / half_life_days)`` otherwise. The same shape
    is used for paper-feedback events, recommendation history, missing-
    author feedback, signal-lab swipes, and the recency component of
    paper_signal scoring.
    """
    if age_days is None:
        return 1.0
    return math.pow(0.5, age_days / half_life_days)


def consensus_bonus(
    n: int, *, fraction: float = 0.12, max_score: float = 100.0
) -> float:
    """Band-relative diminishing-returns bonus for ``N>1`` source confirmations.

    Returns ``fraction × max_score × sqrt(n - 1)`` when ``n > 1``,
    otherwise ``0``. With the default calibration (`fraction=0.12`,
    `max_score=100`) this gives ``+12 / +17 / +21 / +24`` for
    2 / 3 / 4 / 5 sources — diminishing returns so multi-source
    agreement registers as confirmation without overrunning a strong
    single-source signal. Both paper Discovery and the author
    suggestion rail use this with the same defaults.
    """
    if n <= 1:
        return 0.0
    return fraction * max_score * math.sqrt(n - 1)


def log_prevalence_weights(counts: Mapping[str, float]) -> dict[str, float]:
    """Sign-preserving log-prevalence normalization to ``[-1, 1]``.

    For each entry returns ``sign(v) × log(1 + |v|) / log(1 + max|v|)``.
    The top entry is pinned at ``±1.0``; long-tail entries decay
    logarithmically rather than linearly. Empty / all-zero inputs are
    returned as a plain dict copy.

    Mirrors the prevalence pattern the author rail already used —
    sharing the user's #1 topic gets weight 1.0, sharing one that
    appears in 5/50 of the user's papers gets ~0.42 (versus ~0.10
    under linear max-normalization). Long-tail interests stay
    visible in scoring instead of being drowned by the dominant
    cluster.
    """
    if not counts:
        return {}
    max_abs = max(abs(v) for v in counts.values())
    if max_abs <= 0:
        return dict(counts)
    max_log = math.log1p(max_abs)
    if max_log <= 0:
        return dict(counts)
    return {
        key: math.copysign(math.log1p(abs(value)) / max_log, value)
        for key, value in counts.items()
    }


def days_since(raw, now: datetime) -> float | None:
    """Whole-day age of an ISO timestamp ``raw`` relative to ``now`` (made
    UTC-aware), clamped to >= 0; ``None`` when missing/unparseable. Uses
    ``datetime.fromisoformat`` (+ trailing-Z handling). The more permissive
    strptime-loop variant in ``application/paper_signal`` accepts a wider set of
    SQLite timestamp formats and is intentionally kept separate."""
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


def rank_churn(
    baseline: Mapping[str, float],
    adjusted: Mapping[str, float],
    *,
    top_n: int,
) -> dict[str, float | int]:
    """How differently two scorings of the SAME pool order it.

    ``baseline`` / ``adjusted`` map one candidate id to its score under each
    scoring; they must cover the same ids. Returns

    * ``mean_rank_displacement`` — mean absolute change in rank position,
    * ``top_overlap`` — fraction of the baseline top-``top_n`` still in the
      adjusted top-``top_n`` (``k = min(top_n, pool)``),
    * ``entered_top`` — how many candidates joined that top-``k``,
    * ``pool`` / ``top_n`` — the sizes those numbers are relative to.

    One implementation for every "what would promoting this head DO" probe,
    so two callers never report differently-defined displacements under one
    label.
    """

    ids = list(baseline)
    if set(ids) != set(adjusted):
        raise ValueError("rank_churn: baseline and adjusted must score the same pool")
    pool = len(ids)
    k = min(int(top_n), pool)
    if pool < 2:
        return {
            "pool": pool,
            "top_n": k,
            "mean_rank_displacement": 0.0,
            "top_overlap": 1.0 if pool else 0.0,
            "entered_top": 0,
        }
    # Ties break on id so two identical scorings always report zero churn.
    base_order = sorted(ids, key=lambda cid: (-baseline[cid], cid))
    adj_order = sorted(ids, key=lambda cid: (-adjusted[cid], cid))
    base_rank = {cid: pos for pos, cid in enumerate(base_order)}
    displacement = sum(abs(base_rank[cid] - pos) for pos, cid in enumerate(adj_order))
    base_top = set(base_order[:k])
    adj_top = set(adj_order[:k])
    return {
        "pool": pool,
        "top_n": k,
        "mean_rank_displacement": round(displacement / pool, 3),
        "top_overlap": round(len(base_top & adj_top) / k, 3),
        "entered_top": len(adj_top - base_top),
    }


# ── Calibration tables ─────────────────────────────────────────────────────
#
# A raw similarity or overlap has no useful absolute scale, so calibrated inputs
# are read as their percentile in THIS install's corpus. The tables are derived
# per library (`application/discovery/calibration.py`) and stored, never written
# into code: a corpus in another field sits somewhere else entirely. This module
# only owns the arithmetic.


def interpolate_calibration(
    raw_score: float, points: tuple[tuple[float, float], ...]
) -> float:
    """Piecewise-linear interpolation through a monotone (x, y) table."""

    if raw_score <= points[0][0]:
        return points[0][1]
    for (x0, y0), (x1, y1) in zip(points, points[1:]):
        if raw_score <= x1:
            if x1 <= x0:
                return y1
            ratio = (raw_score - x0) / (x1 - x0)
            return y0 + ((y1 - y0) * ratio)
    return points[-1][1]


def calibrate_similarity_score(
    raw_score: float, points: tuple[tuple[float, float], ...] | None
) -> float:
    """Map a raw similarity onto [0, 1] through a derived table.

    ``points`` ``None`` means "this install has not been measured yet": the raw
    value is returned clipped, which is the honest uncalibrated reading rather
    than a curve tuned for some other corpus.
    """

    try:
        score = float(raw_score)
    except (TypeError, ValueError):
        return 0.0
    score = max(0.0, min(1.0, score))
    if not points:
        return score
    return float(max(0.0, min(1.0, interpolate_calibration(score, points))))


def shrink_toward(total: float, n: float, grand_mean: float, strength: float) -> float:
    """A group's mean pulled toward the grand mean by ``strength``
    pseudo-observations. THE shrinkage expression: every caller goes through
    it, so they differ in what they count and how hard they shrink, never in
    the maths."""
    return (total + strength * grand_mean) / (n + strength)


def empirical_bayes_rates(
    counts: Mapping[str, tuple[float, float]],
) -> tuple[float, float | None, dict[str, float]]:
    """Shrink per-group success rates by a strength DERIVED from the data.

    ``counts`` maps group -> ``(successes, trials)``. Returns ``(grand rate,
    strength, {group: shrunk rate})``. The strength is the beta-binomial
    method-of-moments estimate: how much the groups' rates vary BEYOND what
    their sample sizes alone would produce. When they do not (or there are
    fewer than two groups), the strength is ``None`` — infinite — and every
    group gets the grand rate: the data does not say the groups differ, so
    nothing is allowed to act as if they did. No tuned constant anywhere.
    """
    groups = {k: (float(s), float(n)) for k, (s, n) in counts.items() if n > 0}
    trials = sum(n for _, n in groups.values())
    if not groups or trials <= 0:
        return 0.0, None, {}
    grand = sum(s for s, _ in groups.values()) / trials
    noise = grand * (1.0 - grand)
    spread_room = trials - sum(n * n for _, n in groups.values()) / trials
    between = 0.0
    if len(groups) >= 2 and noise > 0 and spread_room > 0:
        observed = sum(n * ((s / n) - grand) ** 2 for s, n in groups.values())
        between = max(0.0, (observed - (len(groups) - 1) * noise) / spread_room)
    if between <= 0.0:
        return grand, None, {k: grand for k in groups}
    strength = max(0.0, noise / between - 1.0)
    return grand, strength, {k: shrink_toward(s, n, grand, strength) for k, (s, n) in groups.items()}
