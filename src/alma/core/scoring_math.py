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
