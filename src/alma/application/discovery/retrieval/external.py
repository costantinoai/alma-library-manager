"""Taste/branch retrieval over offline frontier candidates."""

from __future__ import annotations

import sqlite3
from collections import Counter, defaultdict, deque
from itertools import chain, zip_longest
from typing import Any

from alma.core.settings_helpers import setting_int

from ..frontier import (
    build_frontier_search_index,
    frontier_rank_key,
    load_live_frontier,
    search_frontier,
)
from ..lens_crud import (
    _resolve_lens_branch_controls,
    read_settings,
)
from ..seed_profile import (
    _build_recent_win_queries,
    _candidate_negative_preference_penalty,
    _negative_preference_context,
    _plan_branch_queries_deterministic,
    _resolve_branch_resolution,
    _resolve_branch_temperature,
    _top_negative_terms,
    _top_preferred_authors,
    _top_profile_terms,
    lens_branch_cap,
    resolve_lens_branches,
)
from ._common import FAMILY_TASTE, _candidate_key, attach_hits
from .vector import _PROBE_SHARE_DIVISOR

#: The queue of hits no branch asked for: taste topics, recent wins, authors, venues.
_LENS_WIDE_QUEUE = ""


def _queue_of_searches(hits_by_search: dict[str, list[tuple[float, dict]]]) -> deque[dict]:
    """One queue's candidates: each kind of search in turn.

    A branch alternates core and explore; the lens-wide queue alternates its
    lanes. Each is ordered on its own. In one list, an explore query's literal
    title match outranks the branch's own topic, so a chess branch filled with
    "neural activity" titles, and a venue match, which says nothing about the
    topic, scores 1.0 and outranks every taste topic.
    """
    ordered = [
        [
            candidate
            for score, candidate in sorted(
                hits, key=lambda pair: frontier_rank_key(pair[0], pair[1])
            )
        ]
        for _search, hits in sorted(hits_by_search.items())
    ]
    return deque(
        item for item in chain.from_iterable(zip_longest(*ordered)) if item is not None
    )


def _share_the_cut(
    candidates: list[dict],
    *,
    limit: int,
    probing_ids: set[str],
) -> list[dict]:
    """Cut the channel to `limit`, round-robin across branches.

    A cut by score alone hands the channel to whichever branch has the most
    literal title matches, however good the others are. Here each branch, and
    the lens-wide searches as one more queue, takes one candidate in turn; a
    queue that runs dry leaves its turns to the rest. A probing branch stops at
    the small share the vector lane also gives it.
    """
    by_queue: dict[str, dict[str, list[tuple[float, dict]]]] = defaultdict(
        lambda: defaultdict(list)
    )
    for candidate in candidates:
        keep = 1.0 - float(candidate.get("negative_pref_penalty") or 0.0)
        for hit in candidate.get("retrieval_hits") or []:
            queue = str(hit.branch_id or _LENS_WIDE_QUEUE)
            by_queue[queue][str(hit.branch_mode or hit.retriever_id)].append(
                (float(hit.raw_score or 0.0) * keep, candidate)
            )
    queues = {name: _queue_of_searches(hits) for name, hits in by_queue.items()}
    probe_cap = max(2, (limit // max(1, len(queues))) // _PROBE_SHARE_DIVISOR)
    taken: Counter[str] = Counter()

    picked: dict[str, dict] = {}
    while len(picked) < limit and queues:
        for name in list(queues):
            queue = queues[name]
            while queue and _candidate_key(queue[0]) in picked:
                queue.popleft()
            if not queue or (name in probing_ids and taken[name] >= probe_cap):
                del queues[name]
                continue
            candidate = queue.popleft()
            picked[_candidate_key(candidate)] = candidate
            taken[name] += 1
            if len(picked) >= limit:
                break
    return list(picked.values())


def _retrieve_external_channel(
    db: sqlite3.Connection,
    lens: dict,
    seeds: list[dict],
    *,
    limit: int,
    preference_profile: dict[str, Any] | None = None,
    positive_pubs: list[dict] | None = None,
    resolved_branches: list[dict] | None = None,
) -> tuple[list[dict], dict[str, Any]]:
    """Run lens-specific query/identity retrievers against local frontier."""

    profile = preference_profile or {}
    scope_ids = (
        {str(seed["id"]) for seed in seeds if seed.get("id")}
        if str(lens.get("context_type") or "") != "library_global"
        else None
    )
    # What the lens has learned exists with or without leads to search, and it
    # is what the page's taste ledger shows after every refresh.
    preferred_topics = _top_profile_terms(dict(profile.get("topic_weights") or {}), limit=4)
    recent_wins = _build_recent_win_queries(db, list(positive_pubs or []), limit=3)
    preferred_authors = _top_preferred_authors(db, limit=4, scope_paper_ids=scope_ids)
    preferred_venues = _top_profile_terms(
        dict(profile.get("journal_affinity") or {}),
        limit=3,
        min_weight=0.14,
    )
    negative_context = _negative_preference_context(db, profile)
    taste_summary = {
        "taste_profile": {
            "topics": [
                {"term": term, "weight": round(weight, 4)} for term, weight in preferred_topics
            ],
            "authors": [
                {"name": name, "weight": round(float(weight), 4)}
                for name, weight in preferred_authors
            ],
            "venues": [
                {"name": name, "weight": round(weight, 4)} for name, weight in preferred_venues
            ],
            "recent_wins": [
                {"query": query, "strength": round(float(strength), 4)}
                for query, strength in recent_wins
            ],
        },
        "negative_profile": {
            "topics": _top_negative_terms(negative_context["topics"], limit=4, field_name="term"),
            "authors": _top_negative_terms(negative_context["authors"], limit=4, field_name="name"),
            "venues": _top_negative_terms(negative_context["journals"], limit=3, field_name="name"),
        },
    }

    frontier = load_live_frontier(db)
    if not frontier:
        return [], {
            "external_lanes": {},
            "lane_runs": [],
            "network_calls": 0,
            "frontier_size": 0,
            **taste_summary,
        }
    frontier_search = build_frontier_search_index(frontier)

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

    runs: list[tuple[str, str, list[dict], dict[str, Any]]] = []
    branches = (
        resolved_branches
        if resolved_branches is not None
        else resolve_lens_branches(
            db,
            lens,
            seeds,
            max_branches=lens_branch_cap(settings),
            temperature=temperature,
            resolution=resolution,
        ).branches
    )
    # Branch Studio's knobs are user-facing sliders persisted through
    # `PUT /discovery/settings`. Hard-coding their default values here made the
    # sliders inert — the UI claimed control it did not have.
    max_active = setting_int(settings, "branches.max_active_for_retrieval", 4, 1, 12)
    core_variants = setting_int(settings, "branches.query_core_variants", 2, 1, 4)
    explore_variants = setting_int(settings, "branches.query_explore_variants", 2, 1, 4)

    active_branches = [item for item in branches if item.get("is_active")]
    # Probing branches sit outside the cap: behind four active ones they would
    # never run, and a branch with no cards can never recover.
    probing_branches = [item for item in branches if item.get("is_probing")]
    for branch in [*active_branches[:max_active], *probing_branches]:
        probing = bool(branch.get("is_probing"))
        plan = _plan_branch_queries_deterministic(
            branch,
            temperature=temperature,
            max_core=core_variants,
            max_explore=1 if probing else explore_variants,
        )
        for mode, queries in (
            ("branch_core", [] if probing else plan.get("core_queries") or []),
            ("branch_explore", plan.get("explore_queries") or []),
        ):
            for query in queries:
                runs.append(
                    (
                        mode,
                        query,
                        search_frontier(frontier_search, query, limit=max(8, limit // 2)),
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

    for topic, _weight in preferred_topics:
        runs.append(
            (
                "taste_topic",
                topic,
                search_frontier(frontier_search, topic, limit=max(8, limit // 2)),
                {},
            )
        )
    for query, _strength in recent_wins:
        runs.append(
            (
                "recent_win",
                query,
                search_frontier(frontier_search, query, limit=max(8, limit // 2)),
                {},
            )
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

    for venue, _weight in preferred_venues:
        venue_key = " ".join(venue.lower().split())
        matched = [
            {**candidate, "score": 1.0}
            for candidate in frontier
            if venue_key
            and venue_key
            in " ".join(str(candidate.get("journal") or "").lower().split())
        ][: max(8, limit // 2)]
        runs.append(("taste_venue", venue, matched, {}))

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

    ranked = _share_the_cut(
        list(merged.values()),
        limit=max(1, int(limit)),
        probing_ids={str(branch.get("id")) for branch in probing_branches},
    )
    lane_counts = Counter(
        str(candidate.get("source_type") or "frontier") for candidate in ranked
    )
    return ranked, {
        "external_lanes": dict(lane_counts),
        "lane_runs": lane_runs,
        "network_calls": 0,
        "frontier_size": len(frontier),
        **taste_summary,
    }
