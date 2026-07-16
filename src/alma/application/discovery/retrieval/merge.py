"""Channel merge & diversity selection.

Dedupes per-channel candidate pools into one weighted set, selects a diverse
recommendation slate, and summarizes the source/author/topic/venue mix. Split
out of the discovery god-module (D-9); pure move.
"""

from __future__ import annotations

import math
from collections import Counter
from typing import Any

from ._common import (
    _candidate_author_keys,
    _candidate_key,
    _candidate_source_bucket,
    _candidate_topic_keys,
    _candidate_venue_key,
)


def _select_diverse_recommendation_candidates(
    candidates: list[dict],
    *,
    limit: int,
    staging_limit: int,
) -> tuple[list[dict], dict[str, Any]]:
    """Select a high-relevance but non-monopolistic staging list.

    The scorer still decides candidate quality. This pass only chooses among
    score-qualified candidates so the persisted suggestion set has
    source/channel coverage and does not collapse onto one prolific author,
    venue, or topic. It stages more than the visible limit because later
    lifecycle filters can remove rows after paper upsert resolves candidate
    identity.
    """
    pool = [item for item in candidates if isinstance(item, dict)]
    if not pool:
        return [], {
            "candidate_pool": 0,
            "selected": 0,
            "staging_limit": 0,
            "source_counts_preview": {},
            "branch_counts_preview": {},
        }

    target = max(1, min(len(pool), int(staging_limit or limit or 1)))
    visible_limit = max(1, int(limit or target))
    top_score = max(float(item.get("score") or 0.0) for item in pool)
    relevance_floor = max(24.0, top_score * 0.48)

    source_groups = {
        _candidate_source_bucket(item)
        for item in pool
        if float(item.get("score") or 0.0) >= relevance_floor
    }
    branch_groups = {
        str(item.get("branch_id") or "").strip()
        for item in pool
        if str(item.get("branch_id") or "").strip()
        and float(item.get("score") or 0.0) >= relevance_floor
    }
    branch_target = min(len(branch_groups), max(1, int(math.ceil(visible_limit * 0.20)))) if branch_groups else 0
    source_target = min(len(source_groups), max(2, int(math.ceil(visible_limit * 0.30)))) if len(source_groups) > 1 else len(source_groups)

    source_cap = max(4, int(math.ceil(target * 0.45)))
    author_cap = max(3, int(math.ceil(target * 0.16)))
    venue_cap = max(4, int(math.ceil(target * 0.22)))
    topic_cap = max(5, int(math.ceil(target * 0.30)))

    remaining = list(pool)
    selected: list[dict] = []
    selected_ids: set[int] = set()
    source_counts: Counter[str] = Counter()
    branch_counts: Counter[str] = Counter()
    author_counts: Counter[str] = Counter()
    venue_counts: Counter[str] = Counter()
    topic_counts: Counter[str] = Counter()

    def _would_exceed_caps(item: dict) -> bool:
        source = _candidate_source_bucket(item)
        if len(source_groups) > 1 and source_counts[source] >= source_cap:
            return True
        for author in _candidate_author_keys(item):
            if author_counts[author] >= author_cap:
                return True
        venue = _candidate_venue_key(item)
        if venue and venue_counts[venue] >= venue_cap:
            return True
        for topic in _candidate_topic_keys(item)[:3]:
            if topic_counts[topic] >= topic_cap:
                return True
        return False

    def _adjusted_score(item: dict) -> float:
        score = float(item.get("score") or 0.0)
        source = _candidate_source_bucket(item)
        branch_id = str(item.get("branch_id") or "").strip()
        authors = _candidate_author_keys(item)
        topics = _candidate_topic_keys(item)
        venue = _candidate_venue_key(item)

        adjusted = score
        if branch_id and branch_counts[branch_id] == 0 and sum(branch_counts.values()) < branch_target:
            adjusted += 10.0
        if source_counts[source] == 0 and sum(1 for v in source_counts.values() if v > 0) < source_target:
            adjusted += 6.0
        adjusted -= source_counts[source] * 1.35
        if branch_id:
            adjusted -= branch_counts[branch_id] * 0.8
        if authors:
            adjusted -= max(author_counts[a] for a in authors) * 2.25
        if venue:
            adjusted -= venue_counts[venue] * 1.5
        if topics:
            adjusted -= max(topic_counts[t] for t in topics[:3]) * 1.0
        return adjusted

    while len(selected) < target and remaining:
        best_idx: int | None = None
        best_value = float("-inf")
        for idx, item in enumerate(remaining):
            if id(item) in selected_ids:
                continue
            score = float(item.get("score") or 0.0)
            if score < relevance_floor and len(selected) < visible_limit:
                continue
            if _would_exceed_caps(item):
                continue
            value = _adjusted_score(item)
            if value > best_value:
                best_idx = idx
                best_value = value

        if best_idx is None:
            # Relax caps/floor only for the overflow staging tail. The visible
            # portion should stay quality-gated; the tail exists to survive
            # later lifecycle filters.
            if len(selected) < visible_limit:
                candidates_left = [
                    (idx, item)
                    for idx, item in enumerate(remaining)
                    if id(item) not in selected_ids and float(item.get("score") or 0.0) >= relevance_floor
                ]
            else:
                candidates_left = [
                    (idx, item)
                    for idx, item in enumerate(remaining)
                    if id(item) not in selected_ids
                ]
            if not candidates_left:
                break
            best_idx, _item = max(
                candidates_left,
                key=lambda pair: float(pair[1].get("score") or 0.0),
            )

        item = remaining.pop(best_idx)
        selected.append(item)
        selected_ids.add(id(item))
        source_counts[_candidate_source_bucket(item)] += 1
        branch_id = str(item.get("branch_id") or "").strip()
        if branch_id:
            branch_counts[branch_id] += 1
        for author in _candidate_author_keys(item):
            author_counts[author] += 1
        venue = _candidate_venue_key(item)
        if venue:
            venue_counts[venue] += 1
        for topic in _candidate_topic_keys(item)[:3]:
            topic_counts[topic] += 1

    preview = selected[:visible_limit]
    preview_sources = Counter(_candidate_source_bucket(item) for item in preview)
    preview_branches = Counter(str(item.get("branch_id") or "").strip() for item in preview if str(item.get("branch_id") or "").strip())
    preview_authors: Counter[str] = Counter()
    preview_venues: Counter[str] = Counter()
    preview_topics: Counter[str] = Counter()
    for item in preview:
        preview_authors.update(_candidate_author_keys(item))
        venue = _candidate_venue_key(item)
        if venue:
            preview_venues[venue] += 1
        preview_topics.update(_candidate_topic_keys(item)[:3])

    summary = {
        "candidate_pool": len(pool),
        "selected": len(selected),
        "visible_limit": visible_limit,
        "staging_limit": target,
        "top_score": round(top_score, 3),
        "min_selected_score": round(min((float(item.get("score") or 0.0) for item in selected), default=0.0), 3),
        "relevance_floor": round(relevance_floor, 3),
        "source_counts_preview": dict(preview_sources),
        "branch_counts_preview": dict(preview_branches),
        "max_author_count_preview": max(preview_authors.values(), default=0),
        "max_venue_count_preview": max(preview_venues.values(), default=0),
        "max_topic_count_preview": max(preview_topics.values(), default=0),
        "source_target": source_target,
        "branch_target": branch_target,
    }
    return selected, summary


def _recommendation_mix_summary(rec_rows: list[tuple], *, ranked_by_paper: list[dict]) -> dict[str, Any]:
    by_paper = {
        str(item.get("paper_id") or "").strip(): item
        for item in ranked_by_paper
        if str(item.get("paper_id") or "").strip()
    }
    source_counts: Counter[str] = Counter()
    branch_counts: Counter[str] = Counter()
    source_api_counts: Counter[str] = Counter()
    author_counts: Counter[str] = Counter()
    venue_counts: Counter[str] = Counter()
    topic_counts: Counter[str] = Counter()
    for row in rec_rows:
        paper_id = str(row[3] or "").strip()
        source_counts[str(row[7] or "unknown")] += 1
        if row[8]:
            source_api_counts[str(row[8])] += 1
        if row[10]:
            branch_counts[str(row[10])] += 1
        candidate = by_paper.get(paper_id) or {}
        author_counts.update(_candidate_author_keys(candidate))
        venue = _candidate_venue_key(candidate)
        if venue:
            venue_counts[venue] += 1
        topic_counts.update(_candidate_topic_keys(candidate)[:3])
    return {
        "total": len(rec_rows),
        "source_type_counts": dict(source_counts),
        "source_api_counts": dict(source_api_counts),
        "branch_counts": dict(branch_counts),
        "branch_attributed": sum(branch_counts.values()),
        "max_author_count": max(author_counts.values(), default=0),
        "max_venue_count": max(venue_counts.values(), default=0),
        "max_topic_count": max(topic_counts.values(), default=0),
        "top_authors": dict(author_counts.most_common(5)),
        "top_venues": dict(venue_counts.most_common(5)),
        "top_topics": dict(topic_counts.most_common(5)),
    }


def _merge_channel_candidates(
    *,
    channel_weights: dict[str, float],
    channels: dict[str, list[dict]],
) -> dict[str, dict]:
    provenance_fields = (
        "source_type",
        "source_api",
        "source_key",
        "branch_id",
        "branch_label",
        "branch_mode",
        "taste_strength",
        "negative_pref_penalty",
    )
    metadata_fields = (
        "title",
        "authors",
        "abstract",
        "url",
        "doi",
        "openalex_id",
        "semantic_scholar_id",
        "semantic_scholar_corpus_id",
        "specter2_embedding",
        "specter2_model",
        "year",
        "journal",
        # T5 — S2-only fields. Kept in the back-fill list so a later
        # S2 lane can populate them on a candidate first found by
        # OpenAlex.
        "tldr",
        "influential_citation_count",
    )

    def _blank(value: object) -> bool:
        if value is None:
            return True
        if isinstance(value, str):
            return not value.strip()
        if isinstance(value, (list, tuple, dict, set)):
            return len(value) == 0
        return False

    merged: dict[str, dict] = {}
    bucket_sets: dict[str, set[str]] = {}
    for channel_name, items in channels.items():
        channel_weight = float(channel_weights.get(channel_name, 0.0) or 0.0)
        if channel_weight <= 0:
            continue
        for item in items:
            key = _candidate_key(item)
            score = float(item.get("score", 0.0) or 0.0)
            weighted = score * channel_weight
            if key not in merged:
                merged[key] = {
                    "title": (item.get("title") or "").strip(),
                    "authors": (item.get("authors") or "").strip(),
                    "abstract": (item.get("abstract") or "").strip(),
                    "url": (item.get("url") or "").strip(),
                    "doi": (item.get("doi") or "").strip(),
                    "openalex_id": (item.get("openalex_id") or "").strip(),
                    "semantic_scholar_id": (item.get("semantic_scholar_id") or "").strip(),
                    "semantic_scholar_corpus_id": str(item.get("semantic_scholar_corpus_id") or "").strip(),
                    "specter2_embedding": item.get("specter2_embedding"),
                    "specter2_model": item.get("specter2_model"),
                    "year": item.get("year"),
                    "journal": item.get("journal"),
                    "cited_by_count": int(item.get("cited_by_count") or 0),
                    # T5: thread S2-origin tldr + influential count through
                    # the merge so downstream upsert_paper can persist them.
                    # Non-S2 lanes won't set these; we keep them as default.
                    "tldr": (item.get("tldr") or "").strip(),
                    "influential_citation_count": int(item.get("influential_citation_count") or 0),
                    "score": 0.0,
                    "score_breakdown": {},
                    "_primary_weighted": 0.0,
                    "_branch_match_weighted": -1.0,
                }
                bucket_sets[key] = set()
                for field in provenance_fields:
                    if field in item:
                        merged[key][field] = item.get(field)
            else:
                for field in metadata_fields:
                    if _blank(merged[key].get(field)) and not _blank(item.get(field)):
                        merged[key][field] = item.get(field)
                merged[key]["cited_by_count"] = max(
                    int(merged[key].get("cited_by_count") or 0),
                    int(item.get("cited_by_count") or 0),
                )
            merged[key]["score"] += weighted
            merged[key]["score_breakdown"][channel_name] = {
                "value": score,
                "weight": channel_weight,
                "weighted": weighted,
            }
            item_branch_id = str(item.get("branch_id") or "").strip()
            if item_branch_id and weighted >= float(merged[key].get("_branch_match_weighted", -1.0) or -1.0):
                merged[key]["_branch_match_weighted"] = weighted
                merged[key]["_branch_match"] = {
                    "branch_id": item_branch_id,
                    "branch_label": item.get("branch_label"),
                    "branch_mode": item.get("branch_mode"),
                    "branch_core_topics": item.get("branch_core_topics"),
                    "branch_explore_topics": item.get("branch_explore_topics"),
                    "matched_query": item.get("matched_query"),
                }
            # Consensus bucket: each non-external channel contributes one
            # bucket per channel name; the external channel contributes one
            # bucket per distinct `source_api` (openalex / semantic_scholar /
            # …) so the same paper surfaced by both OpenAlex *and* S2 inside
            # the external lane counts as 2 independent confirmations rather
            # than 1. The post-score consensus bonus reads `consensus_buckets`
            # and rewards multi-source agreement on a band-relative
            # diminishing-returns curve (see scoring._consensus_bonus).
            if channel_name == "external":
                source_api = str(item.get("source_api") or "").strip().lower()
                bucket_sets[key].add(f"external:{source_api or 'unknown'}")
            else:
                bucket_sets[key].add(f"channel:{channel_name}")
            if weighted >= float(merged[key].get("_primary_weighted", 0.0) or 0.0):
                merged[key]["_primary_weighted"] = weighted
                for field in provenance_fields:
                    if field in item:
                        merged[key][field] = item.get(field)
    for key, value in merged.items():
        value["score"] = round(value["score"] * 100.0, 4)
        value.pop("_primary_weighted", None)
        value.pop("_branch_match_weighted", None)
        branch_match = value.pop("_branch_match", None)
        if isinstance(branch_match, dict) and branch_match.get("branch_id"):
            for field in (
                "branch_id",
                "branch_label",
                "branch_mode",
                "branch_core_topics",
                "branch_explore_topics",
                "matched_query",
            ):
                if _blank(value.get(field)) and not _blank(branch_match.get(field)):
                    value[field] = branch_match.get(field)
            value["branch_attribution_source"] = "branch_lane"
        buckets = sorted(bucket_sets.get(key) or set())
        value["consensus_buckets"] = buckets
        value["consensus_count"] = len(buckets)
    return merged
