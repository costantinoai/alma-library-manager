"""Taste/branch retrieval over offline frontier candidates."""

from __future__ import annotations

import sqlite3
from collections import Counter
from typing import Any

from ..frontier import load_live_frontier, search_frontier
from ..lens_crud import (
    _apply_branch_controls,
    _resolve_lens_branch_controls,
    read_settings,
)
from ..seed_profile import (
    _build_recent_win_queries,
    _build_seed_branches,
    _candidate_negative_preference_penalty,
    _negative_preference_context,
    _plan_branch_queries_deterministic,
    _resolve_branch_resolution,
    _resolve_branch_temperature,
    _top_preferred_authors,
    _top_profile_terms,
)
from ._common import FAMILY_TASTE, _candidate_key, attach_hits


def _retrieve_external_channel(
    db: sqlite3.Connection,
    lens: dict,
    seeds: list[dict],
    *,
    limit: int,
    preference_profile: dict[str, Any] | None = None,
    positive_pubs: list[dict] | None = None,
) -> tuple[list[dict], dict[str, Any]]:
    """Run lens-specific query/identity retrievers against local frontier."""

    frontier = load_live_frontier(db)
    if not frontier:
        return [], {
            "external_lanes": {},
            "lane_runs": [],
            "network_calls": 0,
            "frontier_size": 0,
        }

    settings = read_settings(db)
    controls = _resolve_lens_branch_controls(lens)
    temperature = _resolve_branch_temperature(
        settings,
        controls.get("temperature"),
    )
    resolution = _resolve_branch_resolution(
        controls.get("resolution"),
        settings,
    )
    profile = preference_profile or {}
    scope_ids = (
        {str(seed["id"]) for seed in seeds if seed.get("id")}
        if str(lens.get("context_type") or "") != "library_global"
        else None
    )

    runs: list[tuple[str, str, list[dict], dict[str, Any]]] = []
    branches = _apply_branch_controls(
        _build_seed_branches(
            db,
            seeds,
            settings=settings,
            max_branches=min(6, int(settings.get("branches.max_clusters", "6"))),
            temperature=temperature,
            resolution=resolution,
            lens_id=str(lens.get("id") or "") or None,
        ),
        controls,
        db=db,
        lens_id=str(lens.get("id") or "") or None,
    )
    # Branch Studio's knobs are user-facing sliders persisted through
    # `PUT /discovery/settings`. Hard-coding their default values here made the
    # sliders inert — the UI claimed control it did not have.
    max_active = _setting_int(settings, "branches.max_active_for_retrieval", 4, 1, 12)
    core_variants = _setting_int(settings, "branches.query_core_variants", 2, 1, 4)
    explore_variants = _setting_int(settings, "branches.query_explore_variants", 2, 1, 4)

    active_branches = [item for item in branches if item.get("is_active")]
    for branch in active_branches[:max_active]:
        plan = _plan_branch_queries_deterministic(
            branch,
            temperature=temperature,
            max_core=core_variants,
            max_explore=explore_variants,
        )
        for mode, queries in (
            ("branch_core", plan.get("core_queries") or []),
            ("branch_explore", plan.get("explore_queries") or []),
        ):
            for query in queries:
                runs.append(
                    (
                        mode,
                        query,
                        search_frontier(frontier, query, limit=max(8, limit // 2)),
                        {
                            "branch_id": str(branch.get("id") or ""),
                            "branch_mode": mode.removeprefix("branch_"),
                            "branch_label": str(branch.get("label") or ""),
                            "branch_core_topics": list(
                                branch.get("core_topics") or []
                            ),
                            "branch_explore_topics": list(
                                branch.get("explore_topics") or []
                            ),
                        },
                    )
                )

    for topic, _weight in _top_profile_terms(
        dict(profile.get("topic_weights") or {}),
        limit=4,
    ):
        runs.append(
            (
                "taste_topic",
                topic,
                search_frontier(frontier, topic, limit=max(8, limit // 2)),
                {},
            )
        )
    for query, _strength in _build_recent_win_queries(
        db,
        list(positive_pubs or []),
        limit=3,
    ):
        runs.append(
            (
                "recent_win",
                query,
                search_frontier(frontier, query, limit=max(8, limit // 2)),
                {},
            )
        )

    preferred_authors = _top_preferred_authors(
        db,
        limit=4,
        scope_paper_ids=scope_ids,
    )
    for author, _weight in preferred_authors:
        author_key = " ".join(author.lower().split())
        matched = [
            {**candidate, "score": 1.0}
            for candidate in frontier
            if author_key
            and author_key
            in " ".join(str(candidate.get("authors") or "").lower().split())
        ][: max(8, limit // 2)]
        runs.append(("taste_author", author, matched, {}))

    for venue, _weight in _top_profile_terms(
        dict(profile.get("journal_affinity") or {}),
        limit=3,
        min_weight=0.14,
    ):
        venue_key = " ".join(venue.lower().split())
        matched = [
            {**candidate, "score": 1.0}
            for candidate in frontier
            if venue_key
            and venue_key
            in " ".join(str(candidate.get("journal") or "").lower().split())
        ][: max(8, limit // 2)]
        runs.append(("taste_venue", venue, matched, {}))

    negative_context = _negative_preference_context(db, profile)
    merged: dict[str, dict] = {}
    lane_runs: list[dict[str, Any]] = []
    for lane_type, query, candidates, metadata in runs:
        if not candidates:
            continue
        attach_hits(
            candidates,
            family=FAMILY_TASTE,
            retriever_id=f"taste:{lane_type}",
            source_api="local",
            query_key=query,
            branch_id=metadata.get("branch_id"),
            branch_mode=metadata.get("branch_mode"),
        )
        lane_runs.append(
            {
                "lane_type": lane_type,
                "query": query,
                "result_count": len(candidates),
                "duration_ms": 0,
                **{
                    key: value
                    for key, value in metadata.items()
                    if key in {"branch_id", "branch_mode", "branch_label"}
                },
            }
        )
        for candidate in candidates:
            candidate.update(metadata)
            candidate["source_type"] = lane_type
            candidate["matched_query"] = query
            penalty = _candidate_negative_preference_penalty(
                candidate,
                negative_context,
            )
            candidate["negative_pref_penalty"] = penalty
            candidate["score"] = max(
                0.0,
                float(candidate.get("score") or 0.0) * (1.0 - penalty),
            )
            key = _candidate_key(candidate)
            existing = merged.get(key)
            if existing is None:
                merged[key] = candidate
                continue
            existing.setdefault("retrieval_hits", []).extend(
                candidate.get("retrieval_hits") or []
            )
            if float(candidate.get("score") or 0.0) > float(
                existing.get("score") or 0.0
            ):
                existing["score"] = candidate["score"]

    ranked = sorted(
        merged.values(),
        key=lambda candidate: float(candidate.get("score") or 0.0),
        reverse=True,
    )[: max(1, int(limit))]
    lane_counts = Counter(
        str(candidate.get("source_type") or "frontier") for candidate in ranked
    )
    return ranked, {
        "external_lanes": dict(lane_counts),
        "lane_runs": lane_runs,
        "network_calls": 0,
        "frontier_size": len(frontier),
    }


def _setting_int(settings: dict, key: str, default: int, lo: int, hi: int) -> int:
    """Read an integer setting, clamped to the range the UI offers."""
    try:
        return max(lo, min(hi, int(settings.get(key, default))))
    except (TypeError, ValueError):
        return default
