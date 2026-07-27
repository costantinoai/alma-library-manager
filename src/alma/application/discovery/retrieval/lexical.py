"""Local lexical retrieval over corpus plus offline candidate frontier."""

from __future__ import annotations

import sqlite3

from alma.core.scoring_math import query_match_score, query_tokens, rrf_weight
from alma.core.sql_helpers import standalone_paper_sql

from ..frontier import load_live_frontier
from ..seed_profile import _extract_keywords
from ._common import FAMILY_LEXICAL, _candidate_key, attach_hits

_MAX_TOPIC_QUERIES = 8
_PER_QUERY_LIMIT = 25


def _retrieve_lexical_channel(
    db: sqlite3.Connection,
    lens: dict,
    seeds: list[dict],
    *,
    limit: int,
) -> list[dict]:
    """Rank cached candidates per topic, then fuse topic runs with RRF."""

    topics = _lexical_query_terms(lens, seeds)
    if not topics:
        return []
    pool = _load_local_pool(db)
    if not pool:
        return []

    runs: list[tuple[str, list[dict]]] = []
    for topic in topics:
        normalized, tokens = query_tokens(topic)
        ranked = []
        for candidate in pool:
            score = query_match_score(normalized, tokens, candidate)
            if score <= 0.0:
                continue
            ranked.append({**candidate, "score": score})
        ranked.sort(key=lambda item: float(item["score"]), reverse=True)
        ranked = ranked[:_PER_QUERY_LIMIT]
        if not ranked:
            continue
        attach_hits(
            ranked,
            family=FAMILY_LEXICAL,
            retriever_id="lexical:topic",
            source_api="local",
            query_key=topic,
        )
        runs.append((topic, ranked))
    return _fuse_runs(runs, limit=limit)


def _lexical_query_terms(lens: dict, seeds: list[dict]) -> list[str]:
    config = lens.get("context_config") or {}
    explicit_topics = (
        config.get("topics") if isinstance(config.get("topics"), list) else None
    )
    if lens["context_type"] == "topic_keyword":
        keyword = str(config.get("keyword") or config.get("query") or "").strip()
        explicit_topics = [keyword] if keyword else []
    topics = _extract_keywords(seeds, explicit=explicit_topics, max_keywords=10)

    from ..lens_crud import _resolve_lens_branch_controls

    for direction in _resolve_lens_branch_controls(lens).get(
        "custom_directions"
    ) or []:
        for term in direction.get("terms") or []:
            term = str(term).strip()
            if term and term not in topics:
                topics.append(term)
    return topics[:_MAX_TOPIC_QUERIES]


def _load_local_pool(db: sqlite3.Connection) -> list[dict]:
    """Load searchable non-Library corpus rows and live frontier leads."""

    out: dict[str, dict] = {}
    rows = db.execute(
        f"""
        SELECT id, title, authors, abstract, url, doi, openalex_id,
               semantic_scholar_id, year, journal, cited_by_count
        FROM papers
        WHERE status NOT IN ('library', 'dismissed', 'removed')
          AND {standalone_paper_sql('papers')}
        """
    ).fetchall()
    for row in rows:
        candidate = {
            "paper_id": row["id"],
            "title": row["title"] or "",
            "authors": row["authors"] or "",
            "abstract": row["abstract"] or "",
            "url": row["url"] or "",
            "doi": row["doi"] or "",
            "openalex_id": row["openalex_id"] or "",
            "semantic_scholar_id": row["semantic_scholar_id"] or "",
            "year": row["year"],
            "journal": row["journal"] or "",
            "cited_by_count": row["cited_by_count"] or 0,
            "source_type": "lexical_local",
            "source_api": "local",
        }
        out[_candidate_key(candidate)] = candidate

    for candidate in load_live_frontier(db):
        candidate.update(
            {"source_type": "lexical_frontier", "source_api": "local"}
        )
        out.setdefault(_candidate_key(candidate), candidate)
    return list(out.values())


def _fuse_runs(
    runs: list[tuple[str, list[dict]]],
    *,
    limit: int,
) -> list[dict]:
    fused: dict[str, dict] = {}
    rrf: dict[str, float] = {}
    for query_key, results in runs:
        for rank, item in enumerate(results, start=1):
            key = _candidate_key(item)
            rrf[key] = rrf.get(key, 0.0) + rrf_weight(rank)
            existing = fused.get(key)
            if existing is None:
                item["source_key"] = query_key
                fused[key] = item
            else:
                existing.setdefault("retrieval_hits", []).extend(
                    item.get("retrieval_hits") or []
                )
    if not fused:
        return []
    top = max(rrf.values())
    for key, item in fused.items():
        item["score"] = round(rrf[key] / top, 6)
    ranked = sorted(
        fused.values(),
        key=lambda candidate: float(candidate.get("score") or 0.0),
        reverse=True,
    )
    return ranked[: max(1, int(limit))]
