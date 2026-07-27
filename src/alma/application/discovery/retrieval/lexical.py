"""Lexical retrieval channel — keyword/title search via OpenAlex.

Two corrections over the original single-query implementation (task 62 §3):

1. **Per-topic queries, not one giant OR.** The lane used to issue a single
   ``" OR ".join(topics[:16])`` search. Beyond the parsing defect fixed in
   ``openalex_related.build_openalex_query``, one 16-term disjunction is a
   blurred query: OpenAlex ranks by relevance to the *whole* expression, so a
   paper matching one term weakly outranks a paper matching another term
   strongly, and the lane cannot tell which topic produced which candidate.
   We now run one query per topic and fuse the rankings.

2. **Rank fusion, not score fusion.** Per-query relevance scores are not
   comparable across queries. RRF combines ranks only, which is what makes it
   robust to that (Cormack et al., SIGIR 2009). A candidate returned by three
   different topic queries outranks one returned first by a single query —
   cross-query agreement is the signal.

Every result carries a ``RetrievalHit`` per query it appeared in, so the
downstream two-level fusion sees all the evidence rather than a winner.
"""

from __future__ import annotations

import sqlite3
from concurrent.futures import as_completed

from alma.core.concurrency import bounded_thread_pool
from alma.core.time import utcnow
from alma.discovery import openalex_related
from alma.discovery.source_search import RRF_K

from ..seed_profile import _extract_keywords
from ._common import FAMILY_LEXICAL, attach_hits

# Per-topic queries issued per refresh. Each is one OpenAlex list call; the
# lane runs them concurrently so wall-clock is one call, not N.
_MAX_TOPIC_QUERIES = 8

# Results requested per topic query. Deliberately larger than the lane's final
# `limit` so fusion has depth to work with — RRF only distinguishes candidates
# it can see in more than one list.
_PER_QUERY_LIMIT = 25

# Concurrency for the per-topic fan-out. OpenAlex list calls are <1 s and the
# polite pool is generous; 6 keeps peak in-flight modest.
_QUERY_WORKERS = 6


def _retrieve_lexical_channel(
    db: sqlite3.Connection,
    lens: dict,
    seeds: list[dict],
    *,
    limit: int,
) -> list[dict]:
    """Return lexically-retrieved candidates, fused across per-topic queries."""
    topics = _lexical_query_terms(lens, seeds)
    if not topics:
        return []

    from_year = utcnow().year - 3
    runs = _run_topic_queries(topics, from_year=from_year)
    if not runs:
        return []

    return _fuse_runs(runs, limit=limit)


def _lexical_query_terms(lens: dict, seeds: list[dict]) -> list[str]:
    """Pick the topic terms this lens should search on, best first."""
    config = lens.get("context_config") or {}
    explicit_topics = config.get("topics") if isinstance(config.get("topics"), list) else None
    if lens["context_type"] == "topic_keyword":
        keyword = str(config.get("keyword") or config.get("query") or "").strip()
        explicit_topics = [keyword] if keyword else []

    topics = _extract_keywords(seeds, explicit=explicit_topics, max_keywords=10)

    # Adopted custom-direction terms (task 47 §8) expand the vocabulary so the
    # lane also pulls papers matching a region the user adopted from the map.
    try:
        from ..lens_crud import _resolve_lens_branch_controls

        for direction in _resolve_lens_branch_controls(lens).get("custom_directions") or []:
            for term in direction.get("terms") or []:
                term = str(term).strip()
                if term and term not in topics:
                    topics.append(term)
    except Exception:
        pass

    return topics[:_MAX_TOPIC_QUERIES]


def _run_topic_queries(topics: list[str], *, from_year: int) -> list[tuple[str, list[dict]]]:
    """Issue one OpenAlex search per topic, concurrently.

    Returns ``(query_key, ordered_results)`` pairs. A query that fails or
    returns nothing is dropped rather than poisoning the fusion — a missing
    list simply contributes no ranks.
    """
    runs: list[tuple[str, list[dict]]] = []
    pool = bounded_thread_pool(min(_QUERY_WORKERS, len(topics)), thread_name_prefix="lex-topic")
    try:
        future_map = {
            pool.submit(
                openalex_related.search_works_by_topics,
                [topic],
                limit=_PER_QUERY_LIMIT,
                from_year=from_year,
            ): topic
            for topic in topics
        }
        for future in as_completed(future_map):
            topic = future_map[future]
            try:
                results = future.result() or []
            except Exception:
                continue
            if not results:
                continue
            attach_hits(
                results,
                family=FAMILY_LEXICAL,
                retriever_id="lexical:topic",
                source_api="openalex",
                query_key=topic,
            )
            runs.append((topic, results))
    finally:
        pool.shutdown(wait=False)
    return runs


def _fuse_runs(runs: list[tuple[str, list[dict]]], *, limit: int) -> list[dict]:
    """Reciprocal-rank-fuse the per-topic result lists into one ranking.

    ``score = Σ 1 / (RRF_K + rank)`` over every list the candidate appears in,
    normalized to (0, 1] so the channel's output stays on the same nominal
    scale the merge layer expects. The winning dict keeps the accumulated
    ``retrieval_hits`` from all of its appearances.
    """
    from ._common import _candidate_key

    fused: dict[str, dict] = {}
    rrf: dict[str, float] = {}
    for query_key, results in runs:
        for rank, item in enumerate(results, start=1):
            key = _candidate_key(item)
            rrf[key] = rrf.get(key, 0.0) + 1.0 / (RRF_K + rank)
            existing = fused.get(key)
            if existing is None:
                item["source_key"] = query_key
                fused[key] = item
            else:
                # Same paper from a second query: keep one dict, but carry the
                # extra hit so the evidence count is truthful.
                existing.setdefault("retrieval_hits", []).extend(
                    item.get("retrieval_hits") or []
                )

    if not fused:
        return []

    top = max(rrf.values())
    for key, item in fused.items():
        item["score"] = round(rrf[key] / top, 6) if top > 0 else 0.0

    ranked = sorted(fused.values(), key=lambda c: float(c.get("score") or 0.0), reverse=True)
    return ranked[: max(1, limit)]
