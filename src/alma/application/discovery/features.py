"""Leakage-safe feature snapshots for Discovery ranking observations."""

from __future__ import annotations

import math
from datetime import datetime, timezone
from typing import Any

FEATURE_SCHEMA_VERSION = "discovery-features-v3"

_SCALAR_DIAGNOSTICS = (
    "semantic_similarity_centroid_raw",
    "semantic_similarity_exemplar_raw",
    "semantic_similarity_support_raw",
    "semantic_similarity_negative_raw",
    "semantic_similarity_signal_raw",
    "semantic_similarity_negative_signal_raw",
    "lexical_similarity_word_raw",
    "lexical_similarity_char_raw",
    "lexical_similarity_term_raw",
    "lexical_similarity_negative_penalty",
    "coupling_strength",
    "cocitation_strength",
    "coupling_count",
    "cocitation_count",
    "ppr_library_raw",
    "ppr_loved_raw",
)

_SIGNAL_FEATURES = (
    "topic_score",
    "author_affinity",
    "journal_affinity",
    "recency_boost",
    "citation_quality",
    "feedback_adj",
    "preference_affinity",
)

_PROJECTION_AXES = (
    "paper",
    "author",
    "author_name",
    "topic",
    "venue",
    "keyword",
    "tag",
    "semantic_neighbor",
    "citation_neighbor",
)


def _number(value: Any) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def _feature(value: Any, *, available: bool = True, evidence_count: int = 0) -> dict:
    return {
        "value": _number(value),
        "availability": 1 if available else 0,
        "evidence_count": max(0, int(evidence_count or 0)),
    }


def build_feature_snapshot(
    candidate: dict,
    *,
    timestamp: str | None = None,
) -> tuple[dict, dict]:
    """Return ``(reward, exposure)`` snapshots.

    Reward excludes source/API/lane identity, metadata completeness, and
    hydration/timeout state.  Those remain in exposure for diagnostics and
    counterfactual evaluation.
    """

    feature_timestamp = timestamp or datetime.now(timezone.utc).isoformat()
    breakdown = candidate.get("score_breakdown") or {}
    reward: dict[str, dict] = {}
    retrieval_breakdown = candidate.get("retrieval_score_breakdown") or {}
    if not retrieval_breakdown:
        retrieval_breakdown = {
            family: detail
            for family, detail in breakdown.items()
            if family in {"lexical", "vector", "graph", "external"}
            and isinstance(detail, dict)
            and "rrf" in detail
        }
    for family in ("lexical", "semantic", "citation", "taste"):
        detail = retrieval_breakdown.get(family) or {}
        reward[f"retrieval_rrf_{family}"] = _feature(
            detail.get("rrf"),
            available=bool(detail),
            evidence_count=sum(
                1
                for hit in candidate.get("retrieval_hits") or []
                if str(hit.get("family") or "") == family
            ),
        )
    reward["retrieval_family_count"] = _feature(
        candidate.get("cross_family_evidence_count"),
        evidence_count=candidate.get("retrieval_hit_count") or 0,
    )

    for name in _SIGNAL_FEATURES:
        detail = breakdown.get(name) or {}
        availability = True
        if name == "author_affinity":
            availability = bool(str(candidate.get("authors") or "").strip())
        elif name == "journal_affinity":
            availability = bool(str(candidate.get("journal") or "").strip())
        elif name == "recency_boost":
            availability = bool(candidate.get("publication_date") or candidate.get("year"))
        elif name == "citation_quality":
            availability = bool(
                (candidate.get("field_availability") or {}).get("cited_by_count")
                or (candidate.get("field_availability") or {}).get(
                    "influential_citation_count"
                )
            )
        reward[name] = _feature(
            detail.get("value"),
            available=availability and isinstance(detail, dict) and "value" in detail,
        )
    for name in _SCALAR_DIAGNOSTICS:
        availability = name in breakdown
        if name.startswith("semantic_similarity"):
            availability = bool(breakdown.get("candidate_embedding_ready"))
        elif name.startswith("lexical_similarity"):
            availability = bool(str(candidate.get("title") or candidate.get("abstract") or "").strip())
        elif (
            name.startswith("coupling")
            or name.startswith("cocitation")
        ):
            availability = bool(candidate.get("citation_fabric_available"))
        elif name == "ppr_library_raw":
            availability = candidate.get("ppr_library") is not None
        elif name == "ppr_loved_raw":
            availability = candidate.get("ppr_loved") is not None
        reward[name] = _feature(
            breakdown.get(name),
            available=availability,
            evidence_count=int(breakdown.get(name.replace("strength", "count")) or 0)
            if name.endswith("_strength")
            else 0,
        )

    referenced = candidate.get("referenced_works")
    reference_count = (
        len(referenced)
        if isinstance(referenced, list)
        else int(candidate.get("referenced_works_count") or 0)
    )
    reward["fwci"] = _feature(
        candidate.get("fwci"),
        available=candidate.get("fwci") is not None,
    )
    citation_count = int(candidate.get("cited_by_count") or 0)
    influential_count = int(candidate.get("influential_citation_count") or 0)
    reward["citation_count_log"] = _feature(
        math.log1p(max(0, citation_count)),
        available=(candidate.get("field_availability") or {}).get(
            "cited_by_count"
        )
        is not None
        or "cited_by_count" in candidate,
        evidence_count=citation_count,
    )
    reward["influential_citation_count_log"] = _feature(
        math.log1p(max(0, influential_count)),
        available="influential_citation_count" in candidate,
        evidence_count=influential_count,
    )
    cited_percentile = candidate.get("cited_by_percentile") or {}
    reward["cited_by_percentile_min"] = _feature(
        cited_percentile.get("min"),
        available=isinstance(cited_percentile, dict)
        and cited_percentile.get("min") is not None,
    )
    reward["cited_by_percentile_max"] = _feature(
        cited_percentile.get("max"),
        available=isinstance(cited_percentile, dict)
        and cited_percentile.get("max") is not None,
    )
    reward["reference_count"] = _feature(
        reference_count,
        available=referenced is not None
        or candidate.get("referenced_works_count") is not None,
        evidence_count=reference_count,
    )
    reward["is_retracted"] = _feature(
        1.0 if candidate.get("is_retracted") else 0.0,
        available="is_retracted" in candidate,
    )
    year = candidate.get("year")
    try:
        paper_age = max(0, datetime.fromisoformat(feature_timestamp).year - int(year))
        age_available = True
    except (TypeError, ValueError):
        paper_age = 0
        age_available = False
    reward["paper_age_years"] = _feature(
        paper_age,
        available=age_available,
    )
    open_access = candidate.get("open_access")
    reward["is_open_access"] = _feature(
        bool(
            candidate.get("is_open_access")
            or (isinstance(open_access, dict) and open_access.get("is_oa"))
        ),
        available="is_open_access" in candidate or isinstance(open_access, dict),
    )
    publication_type = str(candidate.get("type") or "").strip().lower()
    reward["is_preprint"] = _feature(
        publication_type in {"preprint", "posted-content"}
        or str(candidate.get("source_type") or "") == "preprint_lane",
        available=bool(publication_type)
        or str(candidate.get("source_type") or "") == "preprint_lane",
    )

    projection = breakdown.get("projected_feedback_axes_raw") or {}
    for axis in _PROJECTION_AXES:
        reward[f"projection_{axis}_raw"] = _feature(
            projection.get(axis),
            available=axis in projection,
            evidence_count=(projection.get(f"{axis}_evidence_count") or 0),
        )

    author_atoms = breakdown.get("author_affinity_atoms") or {}
    for atom in ("max", "mean", "first", "last", "followed"):
        reward[f"author_affinity_{atom}"] = _feature(
            author_atoms.get(atom),
            available=bool(author_atoms),
            evidence_count=author_atoms.get("evidence_count") or 0,
        )

    exposure = {
        "feature_schema_version": FEATURE_SCHEMA_VERSION,
        "feature_timestamp": feature_timestamp,
        "source_type": candidate.get("source_type"),
        "source_api": candidate.get("source_api"),
        "source_apis": list(candidate.get("source_apis") or []),
        "branch_id": candidate.get("branch_id"),
        "branch_mode": candidate.get("branch_mode"),
        "retriever_ids": list(candidate.get("retriever_ids") or []),
        "retrieval_hits": list(candidate.get("retrieval_hits") or []),
        "has_abstract": bool(str(candidate.get("abstract") or "").strip()),
        "has_date": bool(candidate.get("publication_date") or candidate.get("year")),
        "has_embedding": (
            candidate.get("_ranking_embedding") is not None
            or candidate.get("specter2_embedding") is not None
        ),
        "embedding_model": (
            candidate.get("_ranking_embedding_model")
            or candidate.get("specter2_model")
        ),
        "embedding_model_compatible": candidate.get("embedding_model_compatible"),
    }
    return reward, exposure


def flatten_reward_features(snapshot: dict) -> dict[str, float]:
    """Flatten snapshot values; unavailable inputs stay prior-neutral at zero."""

    return {
        name: _number(detail.get("value"))
        for name, detail in snapshot.items()
        if isinstance(detail, dict)
    }
