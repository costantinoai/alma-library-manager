"""Channel merge & diversity selection.

Dedupes per-channel candidate pools into one weighted set, selects a diverse
recommendation slate, and summarizes the source/author/topic/venue mix. Split
out of the discovery god-module (D-9); pure move.
"""

from __future__ import annotations

import json
from collections import Counter
from typing import Any

from alma.core.scoring_math import rrf_score_normalized, rrf_weight

from ._common import (
    _candidate_author_keys,
    _candidate_key,
    _candidate_topic_keys,
    _candidate_venue_key,
)
from .types import RetrievalHit

# Fields survive canonical identity merge.  Keep this explicit: it is the
# boundary between upstream APIs and all downstream feature extraction.
_MERGED_FIELDS = (
    "id",
    "paper_id",
    "title",
    "authors",
    "authorships",
    "institutions",
    "abstract",
    "tldr",
    "url",
    "doi",
    "openalex_id",
    "semantic_scholar_id",
    "semantic_scholar_corpus_id",
    "specter2_embedding",
    "specter2_model",
    "year",
    "publication_date",
    "journal",
    "source",
    "source_type",
    "source_api",
    "source_key",
    "branch_id",
    "branch_label",
    "branch_mode",
    "branch_core_topics",
    "branch_explore_topics",
    "matched_query",
    "topics",
    "keywords",
    "referenced_works",
    "referenced_works_count",
    "related_works",
    "cited_by_api_url",
    "counts_by_year",
    "cited_by_percentile",
    "fwci",
    "open_access",
    "is_retracted",
    "is_paratext",
    "language",
    "type",
    "field_provenance",
    "taste_strength",
    "negative_pref_penalty",
)

_MAX_FIELDS = (
    "cited_by_count",
    "influential_citation_count",
)
_UNION_FIELDS = {
    "authorships",
    "institutions",
    "topics",
    "keywords",
    "referenced_works",
    "related_works",
    "counts_by_year",
}


def _blank(value: object) -> bool:
    if value is None:
        return True
    if isinstance(value, str):
        return not value.strip()
    if isinstance(value, (list, tuple, dict, set)):
        return len(value) == 0
    return False


def _hit_identity(hit: RetrievalHit) -> tuple:
    return (
        hit.family,
        hit.retriever_id,
        hit.source_api,
        hit.rank,
        hit.query_key,
        hit.seed_key,
        hit.relation,
        hit.branch_id,
    )


def _collection_item_key(value: Any) -> str:
    if not isinstance(value, dict):
        return str(value).strip().lower()
    for key in (
        "openalex_id",
        "id",
        "term",
        "name",
        "display_name",
        "year",
    ):
        normalized = str(value.get(key) or "").strip().lower()
        if normalized:
            return f"{key}:{normalized}"
    return json.dumps(value, sort_keys=True, default=str)


def _merge_collection_values(current: object, incoming: object) -> list:
    """Stable union for rich list metadata from independent APIs/lanes."""

    left = list(current) if isinstance(current, (list, tuple)) else []
    right = list(incoming) if isinstance(incoming, (list, tuple)) else []
    index = {_collection_item_key(value): idx for idx, value in enumerate(left)}
    for value in right:
        key = _collection_item_key(value)
        existing_idx = index.get(key)
        if existing_idx is None:
            index[key] = len(left)
            left.append(value)
            continue
        existing = left[existing_idx]
        if isinstance(existing, dict) and isinstance(value, dict):
            merged = dict(existing)
            for field, incoming_value in value.items():
                if _blank(merged.get(field)) and not _blank(incoming_value):
                    merged[field] = incoming_value
                elif field == "score":
                    merged[field] = max(
                        float(merged.get(field) or 0.0),
                        float(incoming_value or 0.0),
                    )
            left[existing_idx] = merged
    return left


def _merge_field_provenance(current: object, incoming: object) -> dict[str, list[str]]:
    """Union provider evidence independently for every candidate field."""

    left = current if isinstance(current, dict) else {}
    right = incoming if isinstance(incoming, dict) else {}
    merged: dict[str, list[str]] = {}
    for field in set(left) | set(right):
        sources: list[str] = []
        for raw in (left.get(field), right.get(field)):
            values = raw if isinstance(raw, list) else [raw]
            for value in values:
                source = str(value or "").strip()
                if source and source not in sources:
                    sources.append(source)
        if sources:
            merged[str(field)] = sources
    return merged


def _candidate_hits(
    item: dict,
) -> list[RetrievalHit]:
    supplied = item.get("retrieval_hits")
    if not isinstance(supplied, list) or not supplied:
        raise ValueError("Every retrieval candidate must carry at least one RetrievalHit")
    if not all(isinstance(hit, RetrievalHit) for hit in supplied):
        raise TypeError("retrieval_hits must contain only RetrievalHit values")
    return supplied


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
    merged: dict[str, dict] = {}
    hits_by_key: dict[str, list[RetrievalHit]] = {}
    seen_hits: dict[str, set[tuple]] = {}
    observed_fields: dict[str, set[str]] = {}
    for items in channels.values():
        for item in items:
            key = _candidate_key(item)
            if key not in merged:
                merged[key] = {"score": 0.0, "score_breakdown": {}}
                hits_by_key[key] = []
                seen_hits[key] = set()
                observed_fields[key] = set()
            target = merged[key]
            for field in _MERGED_FIELDS:
                incoming = item.get(field)
                if field == "field_provenance" and not _blank(incoming):
                    target[field] = _merge_field_provenance(
                        target.get(field),
                        incoming,
                    )
                elif field in _UNION_FIELDS and not _blank(incoming):
                    target[field] = _merge_collection_values(
                        target.get(field), incoming
                    )
                elif _blank(target.get(field)) and not _blank(incoming):
                    target[field] = incoming
                if field in item and incoming is not None:
                    observed_fields[key].add(field)
            for field in _MAX_FIELDS:
                if field in item and item.get(field) is not None:
                    observed_fields[key].add(field)
                target[field] = max(
                    int(target.get(field) or 0),
                    int(item.get(field) or 0),
                )
            for hit in _candidate_hits(item):
                identity = _hit_identity(hit)
                if identity not in seen_hits[key]:
                    seen_hits[key].add(identity)
                    hits_by_key[key].append(hit)

    # Level 1: combine all observations from the same retriever.  Repeated
    # query/seed hits help, but only sub-linearly so a fan-out lane cannot win
    # merely by issuing more queries.
    family_evidence: dict[str, dict[str, float]] = {}
    for key, hits in hits_by_key.items():
        by_family_retriever: dict[str, dict[str, list[float]]] = {}
        for hit in hits:
            by_family_retriever.setdefault(hit.family, {}).setdefault(
                hit.retriever_id, []
            ).append(rrf_weight(hit.rank))
        for family, retrievers in by_family_retriever.items():
            retriever_scores = [
                sum(sorted(values, reverse=True)[:4])
                for values in retrievers.values()
            ]
            # Mean across retrievers prevents a family with more configured
            # APIs from receiving a structural advantage.
            family_evidence.setdefault(family, {})[key] = (
                sum(retriever_scores) / max(1, len(retriever_scores))
            )

    # Level 2: rank candidates inside each family, then fuse family ranks.
    # This makes lexical/vector/graph/external comparable despite incompatible
    # native score scales.
    family_ranks: dict[str, dict[str, int]] = {}
    for family, evidence in family_evidence.items():
        ordered = sorted(evidence, key=lambda key: (-evidence[key], key))
        family_ranks[family] = {key: rank for rank, key in enumerate(ordered, 1)}

    weight_keys = {
        "lexical": "lexical",
        "semantic": "vector",
        "citation": "graph",
        "taste": "external",
    }
    positive_weights = {
        family: max(
            0.0,
            float(channel_weights.get(weight_keys.get(family, family), 0.0) or 0.0),
        )
        for family in family_evidence
    }
    weight_total = sum(positive_weights.values()) or float(len(positive_weights) or 1)
    for key, value in merged.items():
        family_components: dict[str, dict[str, float | int]] = {}
        fused = 0.0
        for family in family_evidence:
            rank = family_ranks[family].get(key)
            if rank is None:
                continue
            normalized_rrf = rrf_score_normalized(rank)
            weight = positive_weights[family] / weight_total
            contribution = weight * normalized_rrf
            fused += contribution
            family_components[family] = {
                "rank": rank,
                "rrf": round(normalized_rrf, 6),
                "weight": round(weight, 6),
                "weighted": round(contribution, 6),
            }
        hits = hits_by_key[key]
        families = sorted({hit.family for hit in hits})
        retrievers = sorted({hit.retriever_id for hit in hits})
        apis = sorted(
            {str(hit.source_api).strip().lower() for hit in hits if hit.source_api}
        )
        value["score"] = round(100.0 * fused, 4)
        value["score_breakdown"] = family_components
        value["retrieval_hits"] = [hit.to_dict() for hit in hits]
        value["retrieval_families"] = families
        value["retriever_ids"] = retrievers
        value["source_apis"] = apis
        value["consensus_buckets"] = [
            *(f"family:{family}" for family in families),
            *(f"api:{api}" for api in apis),
        ]
        value["consensus_count"] = len(families)
        value["cross_family_evidence_count"] = len(families)
        value["retrieval_hit_count"] = len(hits)
        value["candidate_key"] = key
        value["field_availability"] = {
            field: True for field in sorted(observed_fields[key])
        }
    return merged
