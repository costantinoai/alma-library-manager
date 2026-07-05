"""External retrieval channel — multi-source live search (S2/Crossref/arXiv/
bioRxiv/OpenAlex) over branch-planned queries.

Split out of the discovery god-module (D-9); pure move. The largest channel:
plans branch queries from the seed profile, fans out across sources, and folds
results back through the branch-control lifecycle.
"""

from __future__ import annotations

import logging
from concurrent.futures import Future

from alma.core.concurrency import bounded_thread_pool
from datetime import datetime
from time import perf_counter
from typing import Any

from alma.core.scoring_math import clamp
from alma.discovery import openalex_related, source_search

from ._common import _candidate_key
from ..lens_crud import (
    _apply_branch_auto_lifecycle,
    _apply_branch_controls,
    _enrich_branches_with_outcomes,
    _load_branch_outcome_map,
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
    _top_negative_terms,
    _top_preferred_authors,
    _top_profile_terms,
)

logger = logging.getLogger(__name__)

_clamp = clamp  # D-3: canonical clamp under the legacy local name


def _retrieve_external_channel(
    db: sqlite3.Connection,
    lens: dict,
    seeds: list[dict],
    *,
    limit: int,
    preference_profile: Optional[dict[str, Any]] = None,
    positive_pubs: Optional[list[dict]] = None,
) -> tuple[list[dict], dict[str, Any]]:
    def _setting_bool(key: str, default: bool) -> bool:
        raw = settings.get(key)
        if raw is None:
            return default
        return str(raw).strip().lower() in {"1", "true", "yes", "on"}

    def _setting_int(key: str, default: int, lo: int, hi: int) -> int:
        raw = settings.get(key)
        if raw is None:
            return default
        try:
            value = int(raw)
        except (TypeError, ValueError):
            return default
        return max(lo, min(hi, value))

    out: list[dict] = []
    settings = read_settings(db)
    branch_enabled = _setting_bool("strategies.branch_explorer", True)
    topic_search_enabled = _setting_bool("strategies.topic_search", True)
    taste_topics_enabled = _setting_bool("strategies.taste_topics", True)
    taste_authors_enabled = _setting_bool("strategies.taste_authors", True)
    taste_venues_enabled = _setting_bool("strategies.taste_venues", True)
    recent_wins_enabled = _setting_bool("strategies.recent_wins", True)
    recommendation_mode = str(settings.get("recommendation_mode", "balanced") or "balanced").strip().lower()
    branch_controls = _resolve_lens_branch_controls(lens)
    # The old discrete pin/boost/mute "smart suggestion" panel was replaced by
    # continuous per-branch auto_weight (see _compute_branch_auto_weight),
    # which is applied directly inside the branch budget allocator below.
    # Manual branch_controls remain the only user-driven overrides.
    effective_branch_controls = branch_controls
    temperature = _resolve_branch_temperature(settings, branch_controls.get("temperature"))
    resolution = _resolve_branch_resolution(branch_controls.get("resolution"), settings)
    current_year = datetime.utcnow().year
    profile = preference_profile or {}
    preferred_topics = _top_profile_terms(
        dict(profile.get("topic_weights") or {}),
        limit=_setting_int("limits.taste_topic_queries", 3, 1, 6),
    )
    preferred_authors = _top_preferred_authors(
        db,
        limit=_setting_int("limits.taste_author_queries", 3, 1, 6),
    )
    preferred_venues = _top_profile_terms(
        dict(profile.get("journal_affinity") or {}),
        limit=_setting_int("limits.taste_venue_queries", 2, 1, 4),
        min_weight=0.14,
    )
    recent_win_queries = _build_recent_win_queries(
        db,
        list(positive_pubs or []),
        limit=_setting_int("limits.recent_win_queries", 2, 1, 4),
    )
    negative_context = _negative_preference_context(db, profile)

    # Cache repeated source searches within one refresh run.  Each cache entry
    # is a `Future` returned by the shared lane executor so that every
    # submission is dispatched in parallel; consumers block with `.result()`
    # at the point of use.  `_lane_timings` records per-call wall-clock and
    # `_lane_diagnostics` captures per-source timing + timeouts so that
    # `lane_runs` can surface a `duration_ms` and `slowest_source` value
    # even though each future was fired concurrently (D-AUDIT-10b / -10c).
    query_cache: dict[tuple[str, str, int, int], "Future[list[dict]]"] = {}
    _lane_timings: dict[tuple, int] = {}
    _lane_diagnostics: dict[tuple, dict[str, Any]] = {}
    lane_runs: list[dict[str, Any]] = []

    def _lane_diag_fields(cache_key: tuple) -> dict[str, Any]:
        """Pull the slowest-source + timeout info from diagnostics for a lane_run."""
        diag = _lane_diagnostics.get(cache_key) or {}
        fields: dict[str, Any] = {}
        slowest = diag.get("slowest_source")
        if isinstance(slowest, dict):
            fields["slowest_source"] = slowest.get("source")
            fields["slowest_source_ms"] = slowest.get("duration_ms")
        per_source = diag.get("per_source_ms")
        if per_source:
            fields["per_source_ms"] = dict(per_source)
        timed_out = diag.get("timed_out_sources") or []
        if timed_out:
            fields["timed_out_sources"] = list(timed_out)
        return fields

    # S-6: 12 lane workers (was 6). A single refresh queues ~26-30 lane futures
    # (branch core/explore + taste + graph + external + S2-recommend +, before
    # S-2, the per-author fetches); 6 workers serialized them into ~5 waves. The
    # rate-limited sources can't be sped up by more workers — S2 is gated to 1
    # rps process-wide by SourceHttpClient._concurrency_slot, arXiv to 1 req/3s —
    # so widening only overlaps the FAST sources (OpenAlex 100 req/s, Crossref).
    # Worst case 12 × 2 OpenAlex sub-calls (search_works_hybrid) ≈ 3 OA req/s
    # averaged over the 8s lane deadline, far under the 100 req/s key ceiling.
    lane_executor = bounded_thread_pool(12, thread_name_prefix="lens-lane")

    def _submit_source_search(
        cache_key: tuple,
        query: str,
        per_query: int,
        from_year: int,
        *,
        mode: str,
    ) -> "Future[list[dict]]":
        """Submit a `search_across_sources` call to the lane executor.

        Stores the Future in `query_cache` keyed by `cache_key` so repeat
        requests in the same refresh piggyback on the same in-flight call.
        Wraps the fn with a timer that records wall-clock duration into
        `_lane_timings` so `lane_runs` can carry `duration_ms` without a
        second timing pass.  Per-source diagnostics (which of the 5
        sources was slowest, which timed out) feed into
        `_lane_diagnostics` so `lane_runs` expose them for profiling.
        """
        existing = query_cache.get(cache_key)
        if existing is not None:
            return existing

        diagnostics: dict[str, Any] = {}

        def _timed_call() -> list[dict]:
            started = perf_counter()
            try:
                return source_search.search_across_sources(
                    query,
                    limit=per_query,
                    from_year=from_year,
                    settings=settings,
                    mode=mode,
                    temperature=temperature,
                    semantic_scholar_mode="bulk",
                    diagnostics=diagnostics,
                )
            finally:
                _lane_timings[cache_key] = int(round((perf_counter() - started) * 1000))
                _lane_diagnostics[cache_key] = diagnostics

        future = lane_executor.submit(_timed_call)
        query_cache[cache_key] = future
        return future

    def _resolve_lane(cache_entry: "Future[list[dict]] | list[dict]") -> list[dict]:
        if isinstance(cache_entry, Future):
            try:
                return cache_entry.result() or []
            except Exception as exc:
                logger.warning("lens lane source search failed: %s", exc)
                return []
        return cache_entry or []

    # Branch exploration: cluster seed papers, use AI to craft search
    # queries from seed titles+abstracts, then search across all sources.
    #
    # Two-pass structure so every lane submission is queued on the shared
    # lane_executor before any `.result()` blocks.  Pass 1 builds one
    # `branch_plan` per branch (with LLM query plans precomputed) and
    # submits every source search.  Pass 2 iterates the plans and consumes
    # results from the cache — the executor has been working on them
    # concurrently in the meantime.
    branch_plans: list[dict[str, Any]] = []
    if branch_enabled and seeds:
        max_branches = _setting_int("branches.max_clusters", 6, 2, 12)
        max_active = _setting_int("branches.max_active_for_retrieval", 4, 1, 12)
        core_variants = _setting_int("branches.query_core_variants", 2, 1, 4)
        explore_variants = _setting_int("branches.query_explore_variants", 2, 1, 4)

        branches = _build_seed_branches(
            db,
            seeds,
            settings=settings,
            max_branches=max_branches,
            temperature=temperature,
            resolution=resolution,
            lens_id=str(lens.get("id") or "") or None,
        )
        branches = _apply_branch_controls(
            branches,
            effective_branch_controls,
            db=db,
            lens_id=str(lens.get("id") or "").strip() or None,
        )
        # Enrich each branch with its outcome history so the budget allocator
        # can read `auto_weight` (continuous multiplier derived from past
        # save/dismiss patterns) alongside the manual pin/boost/mute flags.
        branch_outcome_map = _load_branch_outcome_map(
            db,
            lens_id=str(lens.get("id") or "").strip() or None,
            days=60,
        )
        branches = _enrich_branches_with_outcomes(
            branches,
            branch_outcome_map,
            db=db,
            lens_id=str(lens.get("id") or "").strip() or None,
        )
        # Auto-lifecycle: branches whose auto_weight crossed the rotate
        # or mute threshold get their topics rotated or get auto-muted.
        # Self-correcting on the next refresh — if the rotation pulls
        # saves, auto_weight rises and the branch returns to its core
        # angle automatically.
        branches = _apply_branch_auto_lifecycle(branches)
        active_branches = [branch for branch in branches if branch.get("is_active")]
        if active_branches:
            prioritized = active_branches[:max_active]
            branch_budget_weights = []
            for branch in prioritized:
                # auto_weight is the continuous multiplier from past outcomes
                # (range AUTO_WEIGHT_FLOOR..AUTO_WEIGHT_CEIL, neutral 1.0 when
                # signal is thin). Pin/Boost are hard floors that prevent the
                # auto-weight from starving a branch the user explicitly
                # endorsed; without an override, auto_weight rules.
                weight = float(branch.get("auto_weight") or 1.0)
                if branch.get("is_pinned"):
                    weight = max(weight, 1.65)
                elif branch.get("is_boosted"):
                    weight = max(weight, 1.3)
                branch_budget_weights.append(weight)
            core_ratio = _clamp(0.82 - (0.36 * temperature), 0.42, 0.82)
            total_branch_weight = sum(branch_budget_weights) or 1.0
            # Absolute minimum per-branch budget. A branch starved
            # below this floor never gets enough recommendations to
            # generate calibration signal — making its weakness
            # self-fulfilling. The proportional share still applies on
            # top, but no active branch drops below `min_budget`.
            min_per_branch = max(
                4,
                _setting_int("branches.min_budget_per_branch", 8, 1, 32),
            )
            for branch, branch_weight in zip(prioritized, branch_budget_weights):
                branch_id = str(branch.get("id") or "")
                branch_label = str(branch.get("label") or branch_id)
                core_topics = list(branch.get("core_topics") or [])
                explore_topics = list(branch.get("explore_topics") or [])
                proportional = int(round((limit * branch_weight) / total_branch_weight))
                per_branch = max(min_per_branch, proportional)
                branch_score_bonus = 0.0
                if branch.get("is_pinned"):
                    branch_score_bonus = 0.1
                elif branch.get("is_boosted"):
                    branch_score_bonus = 0.05

                # Branch query planning is deterministic — stitches queries
                # from core / explore topics + seed titles. The LLM-backed
                # planner was removed in 2026-04 (see tasks/01_LLM_PRODUCTION_EXIT.md);
                # the deterministic stitcher is now the only path.
                query_plan = _plan_branch_queries_deterministic(
                    branch,
                    temperature=temperature,
                    max_core=core_variants,
                    max_explore=explore_variants,
                )

                core_queries = [q for q in (query_plan.get("core_queries") or []) if str(q).strip()][:core_variants]
                explore_queries = [q for q in (query_plan.get("explore_queries") or []) if str(q).strip()][:explore_variants]
                if not core_queries and core_topics:
                    core_queries = [" ".join(core_topics[:3])]
                if not explore_queries and explore_topics:
                    explore_queries = [" ".join(explore_topics[:3])]

                core_limit_total = max(1, int(round(per_branch * core_ratio)))
                explore_limit_total = max(0, per_branch - core_limit_total)

                core_per_query = (
                    max(2, core_limit_total // max(1, len(core_queries)))
                    if core_queries and core_limit_total > 0
                    else 0
                )
                explore_per_query = (
                    max(1, explore_limit_total // max(1, len(explore_queries)))
                    if explore_queries and explore_limit_total > 0
                    else 0
                )
                from_year_core = current_year - (2 if temperature <= 0.35 else 4)
                from_year_explore = current_year - (3 if temperature <= 0.35 else 6)

                # Queue all submissions for this branch onto the lane executor.
                # Submissions are non-blocking, so queuing proceeds to the next
                # branch immediately.
                if core_per_query > 0:
                    for query in core_queries:
                        _submit_source_search(
                            ("core", query, core_per_query, from_year_core),
                            query,
                            core_per_query,
                            from_year_core,
                            mode="core",
                        )
                if explore_per_query > 0:
                    for query in explore_queries:
                        _submit_source_search(
                            ("explore", query, explore_per_query, from_year_explore),
                            query,
                            explore_per_query,
                            from_year_explore,
                            mode="explore",
                        )

                branch_plans.append(
                    {
                        "branch_id": branch_id,
                        "branch_label": branch_label,
                        "core_topics": core_topics,
                        "explore_topics": explore_topics,
                        "core_queries": core_queries,
                        "explore_queries": explore_queries,
                        "core_per_query": core_per_query,
                        "explore_per_query": explore_per_query,
                        "from_year_core": from_year_core,
                        "from_year_explore": from_year_explore,
                        "branch_score_bonus": branch_score_bonus,
                    }
                )

    taste_budget_factor = {
        "explore": 0.30,
        "balanced": 0.34 if temperature <= 0.35 else 0.28,
        "exploit": 0.38,
    }.get(recommendation_mode, 0.34 if temperature <= 0.35 else 0.28)
    taste_budget_total = max(6, int(round(limit * taste_budget_factor)))
    topic_hint = preferred_topics[0][0] if preferred_topics else ""
    lane_specs: list[dict[str, Any]] = []
    explicit_topic_keyword = ""

    if topic_search_enabled and lens["context_type"] == "topic_keyword":
        config = lens.get("context_config") or {}
        explicit_topic_keyword = str(config.get("keyword") or config.get("query") or "").strip()
        if explicit_topic_keyword:
            lane_specs.append(
                {
                    "lane_type": "cold_start_topic",
                    "query": explicit_topic_keyword,
                    "source_key": explicit_topic_keyword,
                    "strength": 0.76 if not seeds else 0.58,
                    "budget": max(4, int(round(limit * (0.40 if not seeds else 0.18)))),
                    "from_year": current_year - 5,
                    "mode": "core" if not seeds else "explore",
                }
            )
        elif not seeds:
            lane_specs.append(
                {
                    "lane_type": "cold_start_topic",
                    "query": str(lens.get("name") or "").strip(),
                    "source_key": str(lens.get("name") or "").strip(),
                    "strength": 0.62,
                    "budget": max(4, int(round(limit * 0.34))),
                    "from_year": current_year - 5,
                    "mode": "core",
                }
            )

    if taste_topics_enabled:
        topic_budget = max(2, int(round(taste_budget_total * 0.34)))
        for topic, strength in preferred_topics:
            lane_specs.append(
                {
                    "lane_type": "taste_topic",
                    "query": topic,
                    "source_key": topic,
                    "strength": _clamp(0.58 + (float(strength) * 0.35), 0.45, 1.0),
                    "budget": topic_budget,
                    "from_year": current_year - 4,
                    "mode": "core",
                }
            )

    if taste_authors_enabled:
        author_budget = max(2, int(round(taste_budget_total * 0.28)))
        for author, strength in preferred_authors:
            author_query = author if not topic_hint else f"{author} {topic_hint}"
            lane_specs.append(
                {
                    "lane_type": "taste_author",
                    "query": author_query,
                    "source_key": author,
                    "strength": _clamp(0.54 + (float(strength) * 0.32), 0.42, 1.0),
                    "budget": author_budget,
                    "from_year": current_year - 4,
                    "mode": "core",
                }
            )

    if taste_venues_enabled:
        venue_budget = max(1, int(round(taste_budget_total * 0.16)))
        for venue, strength in preferred_venues:
            venue_query = venue if not topic_hint else f"{venue} {topic_hint}"
            lane_specs.append(
                {
                    "lane_type": "taste_venue",
                    "query": venue_query,
                    "source_key": venue,
                    "strength": _clamp(0.5 + (float(strength) * 0.3), 0.38, 0.94),
                    "budget": venue_budget,
                    "from_year": current_year - 5,
                    "mode": "core",
                }
            )

    if recent_wins_enabled:
        recent_budget = max(2, int(round(taste_budget_total * 0.22)))
        for query, strength in recent_win_queries:
            lane_specs.append(
                {
                    "lane_type": "recent_win",
                    "query": query,
                    "source_key": query,
                    "strength": float(strength),
                    "budget": recent_budget,
                    "from_year": current_year - 3,
                    "mode": "explore" if temperature >= 0.4 else "core",
                }
            )

    # --- Prefetch: submit every remaining source-search + followed-author
    # --- fetch + S2 recommend to the lane executor BEFORE any consumer blocks
    # --- on a future.  This unlocks cross-section parallelism (branch queries,
    # --- taste lanes, followed-author fetches, and S2 recommend all share the
    # --- same pool of workers and run concurrently).
    for lane in lane_specs:
        query = str(lane.get("query") or "").strip()
        if not query:
            continue
        per_query = max(1, int(lane.get("budget") or 1))
        from_year = int(lane.get("from_year") or current_year - 4)
        mode = str(lane.get("mode") or "core")
        cache_key = (str(lane.get("lane_type") or "taste"), query, per_query, from_year)
        _submit_source_search(cache_key, query, per_query, from_year, mode=mode)

    # S2 recommend lane — resolve seeds from DB, then submit the network call
    # as a Future so it overlaps with the other lanes.  The consume block
    # later reads the result and emits the lane_run + `out` entries.
    s2_recommend_enabled = _setting_bool("strategies.s2_recommend", True)
    s2_source_enabled = _setting_bool("sources.semantic_scholar.enabled", True)
    s2_recommend_future: "Future[list[dict]] | None" = None
    s2_recommend_budget = 0
    s2_positive_seed_ids: list[str] = []
    s2_negative_seed_ids: list[str] = []
    s2_holder: dict[str, Any] = {}
    if s2_recommend_enabled and s2_source_enabled:
        from alma.discovery import semantic_scholar as _s2_lane

        pos_rows = db.execute(
            """
            SELECT semantic_scholar_id, semantic_scholar_corpus_id, doi
            FROM papers
            WHERE status = 'library' AND COALESCE(rating, 0) >= 4
              AND (
                COALESCE(NULLIF(TRIM(semantic_scholar_id), ''), '') != ''
                OR COALESCE(NULLIF(TRIM(doi), ''), '') != ''
                OR COALESCE(NULLIF(TRIM(semantic_scholar_corpus_id), ''), '') != ''
              )
            ORDER BY COALESCE(rating, 0) DESC, COALESCE(added_at, '') DESC
            LIMIT 50
            """
        ).fetchall()
        neg_rows = db.execute(
            """
            SELECT semantic_scholar_id, semantic_scholar_corpus_id, doi
            FROM papers
            WHERE (
                status IN ('removed', 'dismissed')
                OR COALESCE(rating, 0) BETWEEN 1 AND 2
              )
              AND (
                COALESCE(NULLIF(TRIM(semantic_scholar_id), ''), '') != ''
                OR COALESCE(NULLIF(TRIM(doi), ''), '') != ''
                OR COALESCE(NULLIF(TRIM(semantic_scholar_corpus_id), ''), '') != ''
              )
            LIMIT 100
            """
        ).fetchall()

        def _row_to_s2_seed_id(row) -> str:
            s2 = str(row["semantic_scholar_id"] or "").strip()
            if s2:
                return s2
            doi = (row["doi"] or "").strip()
            if doi:
                return f"DOI:{doi}"
            corpus = str(row["semantic_scholar_corpus_id"] or "").strip()
            if corpus:
                return f"CorpusID:{corpus}"
            return ""

        s2_positive_seed_ids = [seed for row in pos_rows if (seed := _row_to_s2_seed_id(row))]
        s2_negative_seed_ids = [seed for row in neg_rows if (seed := _row_to_s2_seed_id(row))]
        s2_recommend_budget = max(6, int(round(limit * 0.18)))
        if s2_positive_seed_ids:

            def _s2_recommend_call(
                pos: list[str] = s2_positive_seed_ids,
                neg: list[str] = s2_negative_seed_ids,
                budget: int = s2_recommend_budget,
                holder: dict[str, Any] = s2_holder,
            ) -> list[dict]:
                started = perf_counter()
                try:
                    return _s2_lane.recommend_from_seeds(pos, neg, limit=budget) or []
                except Exception as exc:
                    holder["error"] = str(exc)
                    return []
                finally:
                    holder["ms"] = int(round((perf_counter() - started) * 1000))

            s2_recommend_future = lane_executor.submit(_s2_recommend_call)

    # Followed-author OpenAlex fetches — submit each in parallel, consume
    # once branches and taste lanes have been drained.
    followed_rows = db.execute(
        """
        SELECT a.openalex_id
        FROM followed_authors fa
        JOIN authors a ON a.id = fa.author_id
        WHERE a.openalex_id IS NOT NULL AND TRIM(a.openalex_id) != ''
        LIMIT 10
        """
    ).fetchall()
    year_floor = current_year - 2
    follow_budget_base = {
        "explore": 0.14,
        "balanced": 0.20 + (0.25 * temperature),
        "exploit": 0.28 + (0.18 * temperature),
    }.get(recommendation_mode, 0.20 + (0.25 * temperature))
    follow_budget = max(3, int(round(limit * follow_budget_base)))
    follow_author_limit = min(6, follow_budget)
    # S-2: ONE batched OpenAlex OR-filter call (author.id:A1|A2|...) for all
    # followed authors instead of one request per author. Mirrors the Feed
    # path (batch_fetch_recent_works_for_authors); frees the per-author worker
    # slots on lane_executor for the branch/taste/S2 lanes. A pipe filter is a
    # single OpenAlex list call regardless of author count (≤100 ids/filter),
    # far under the 100 req/s key limit.
    followed_author_ids = [
        (row["openalex_id"] or "").strip()
        for row in followed_rows
        if (row["openalex_id"] or "").strip()
    ]
    followed_author_holder: dict[str, int] = {}
    followed_author_future: "Future[dict[str, list[dict]]] | None" = None
    if followed_author_ids:

        def _timed_batch_author_fetch(
            ids: list[str] = followed_author_ids,
            from_year: int = year_floor,
            per_fetch: int = follow_author_limit,
            holder: dict[str, int] = followed_author_holder,
        ) -> dict[str, list[dict]]:
            started = perf_counter()
            try:
                return openalex_related.batch_fetch_recent_works_for_authors(
                    ids,
                    from_year=from_year,
                    per_author_limit=per_fetch,
                )
            finally:
                holder["ms"] = int(round((perf_counter() - started) * 1000))

        followed_author_future = lane_executor.submit(_timed_batch_author_fetch)

    # --- Consume pass: branch plans first (each resolve blocks only on the
    # --- slowest single future because the rest are already mid-flight).
    for plan in branch_plans:
        branch_id = plan["branch_id"]
        branch_label = plan["branch_label"]
        core_topics = plan["core_topics"]
        explore_topics = plan["explore_topics"]
        core_queries = plan["core_queries"]
        explore_queries = plan["explore_queries"]
        core_per_query = plan["core_per_query"]
        explore_per_query = plan["explore_per_query"]
        from_year_core = plan["from_year_core"]
        from_year_explore = plan["from_year_explore"]
        branch_score_bonus = plan["branch_score_bonus"]

        if core_per_query > 0:
            for query in core_queries:
                cache_key = ("core", query, core_per_query, from_year_core)
                core_results = _resolve_lane(query_cache[cache_key])
                lane_runs.append(
                    {
                        "lane_type": "branch_core",
                        "branch_id": branch_id,
                        "branch_label": branch_label,
                        "query": query,
                        "from_year": from_year_core,
                        "result_count": len(core_results),
                        "duration_ms": int(_lane_timings.get(cache_key, 0)),
                        **_lane_diag_fields(cache_key),
                    }
                )
                for idx, item in enumerate(core_results):
                    rank_factor = _clamp(1.0 - (idx / max(1, core_per_query * 1.6)), 0.2, 1.0)
                    base = float(item.get("score", 0.35) or 0.35)
                    score = _clamp((base * 0.78) + (rank_factor * 0.22), 0.03, 1.0)
                    out.append(
                        {
                            **item,
                            "score": round(_clamp(score + branch_score_bonus, 0.03, 1.0), 4),
                            "source_type": "branch",
                            "source_key": str(item.get("source_key") or branch_label),
                            "branch_id": branch_id,
                            "branch_label": branch_label,
                            "branch_mode": "core",
                            "matched_query": query,
                            "branch_core_topics": list(core_topics),
                            "branch_explore_topics": list(explore_topics),
                        }
                    )

        if explore_per_query > 0:
            for query in explore_queries:
                cache_key = ("explore", query, explore_per_query, from_year_explore)
                explore_results = _resolve_lane(query_cache[cache_key])
                lane_runs.append(
                    {
                        "lane_type": "branch_explore",
                        "branch_id": branch_id,
                        "branch_label": branch_label,
                        "query": query,
                        "from_year": from_year_explore,
                        "result_count": len(explore_results),
                        "duration_ms": int(_lane_timings.get(cache_key, 0)),
                        **_lane_diag_fields(cache_key),
                    }
                )
                for idx, item in enumerate(explore_results):
                    rank_factor = _clamp(1.0 - (idx / max(1, explore_per_query * 1.8)), 0.1, 1.0)
                    base = float(item.get("score", 0.2) or 0.2)
                    score = _clamp(
                        (base * (0.45 + (0.35 * temperature)))
                        + (rank_factor * (0.18 + (0.22 * temperature))),
                        0.02,
                        0.98,
                    )
                    out.append(
                        {
                            **item,
                            "score": round(_clamp(score + branch_score_bonus, 0.02, 0.98), 4),
                            "source_type": "branch",
                            "source_key": str(item.get("source_key") or branch_label),
                            "branch_id": branch_id,
                            "branch_label": branch_label,
                            "branch_mode": "explore",
                            "matched_query": query,
                            "branch_core_topics": list(core_topics),
                            "branch_explore_topics": list(explore_topics),
                        }
                    )

    # --- Consume pass: taste lane_specs.
    for lane in lane_specs:
        query = str(lane.get("query") or "").strip()
        if not query:
            continue
        per_query = max(1, int(lane.get("budget") or 1))
        from_year = int(lane.get("from_year") or current_year - 4)
        cache_key = (str(lane.get("lane_type") or "taste"), query, per_query, from_year)
        lane_results = _resolve_lane(query_cache.get(cache_key))
        source_key = str(lane.get("source_key") or query)
        lane_runs.append(
            {
                "lane_type": str(lane.get("lane_type") or "taste"),
                "query": query,
                "source_key": source_key,
                "from_year": from_year,
                "result_count": len(lane_results),
                "duration_ms": int(_lane_timings.get(cache_key, 0)),
                **_lane_diag_fields(cache_key),
            }
        )
        lane_strength = float(lane.get("strength") or 0.5)
        lane_type = str(lane.get("lane_type") or "taste_topic")
        for idx, item in enumerate(lane_results):
            rank_factor = _clamp(1.0 - (idx / max(1, per_query * 1.8)), 0.12, 1.0)
            base = float(item.get("score", 0.22) or 0.22)
            score = _clamp((base * 0.62) + (rank_factor * 0.18) + (lane_strength * 0.20), 0.02, 1.0)
            out.append(
                {
                    **item,
                    "score": round(score, 4),
                    "source_type": lane_type,
                    "source_key": source_key,
                    "taste_strength": round(lane_strength, 4),
                    "branch_mode": str(item.get("branch_mode") or ""),
                }
            )

    # --- Consume pass: followed-author fetches (one batched OpenAlex call).
    if followed_author_future is not None:
        try:
            # F4: bound the wait (was unbounded → only the ~20-30 s HTTP
            # timeout). TimeoutError is an Exception subclass in 3.11, so the
            # handler below degrades a slow fetch to empty without stalling.
            recs_by_author = (
                followed_author_future.result(timeout=source_search.DEFAULT_LANE_DEADLINE_S) or {}
            )
        except Exception as exc:
            logger.warning("followed-author batch works fetch failed: %s", exc)
            recs_by_author = {}
        # One call served every author, so the timing is shared across the
        # per-author lane_runs entries below.
        batch_ms = int(followed_author_holder.get("ms") or 0)
        for author_key, recs in recs_by_author.items():
            for item in recs:
                base = float(item.get("score", 0.3) or 0.3)
                score = _clamp(base, 0.05, 1.0)
                out.append(
                    {
                        **item,
                        "score": round(score, 4),
                        "source_type": "followed_author",
                        "source_key": author_key,
                        "branch_mode": "followed_author",
                        "source_api": "openalex",
                    }
                )
            lane_runs.append(
                {
                    "lane_type": "followed_author",
                    "query": author_key,
                    "source_key": author_key,
                    "from_year": year_floor,
                    "result_count": len(recs),
                    "duration_ms": batch_ms,
                }
            )
            if len(out) >= (limit + follow_budget):
                break

    # S2 list-mode recommendations lane — consume the future submitted above
    # (runs concurrently with branch + taste + followed-author lanes on the
    # shared lane_executor).
    if s2_recommend_enabled and s2_source_enabled:
        recommended: list[dict] = []
        if s2_recommend_future is not None:
            try:
                # F4: bound the wait — S2 is the rate-limit-prone source.
                recommended = s2_recommend_future.result(timeout=source_search.DEFAULT_LANE_DEADLINE_S) or []
            except Exception as exc:
                s2_holder.setdefault("error", str(exc))
                recommended = []
        for idx, item in enumerate(recommended):
            rank_factor = _clamp(
                1.0 - (idx / max(1, s2_recommend_budget * 1.4)), 0.2, 1.0
            )
            base = float(item.get("score", 0.45) or 0.45)
            score = _clamp((base * 0.68) + (rank_factor * 0.22), 0.04, 1.0)
            out.append(
                {
                    **item,
                    "score": round(score, 4),
                    "source_type": "semantic_scholar_recommend",
                    "source_key": f"pos={len(s2_positive_seed_ids)}/neg={len(s2_negative_seed_ids)}",
                    "source_api": "semantic_scholar",
                    "branch_mode": "s2_recommend",
                }
            )
        lane_runs.append(
            {
                "lane_type": "semantic_scholar_recommend",
                "query": f"pos={len(s2_positive_seed_ids)} neg={len(s2_negative_seed_ids)}",
                "source_key": "s2_recommend",
                "from_year": 0,
                "result_count": len(recommended),
                "duration_ms": int(s2_holder.get("ms") or 0),
                **({"error": s2_holder["error"]} if s2_holder.get("error") else {}),
            }
        )

    if negative_context:
        filtered: list[dict] = []
        for item in out:
            penalty = _candidate_negative_preference_penalty(item, negative_context)
            if penalty >= 0.72:
                continue
            if penalty > 0.0:
                item = dict(item)
                item["score"] = round(float(item.get("score", 0.0) or 0.0) * (1.0 - (penalty * 0.55)), 4)
                item["negative_pref_penalty"] = round(penalty, 4)
            filtered.append(item)
        out = filtered

    # Dedupe by candidate identity and keep highest score.
    merged: dict[str, dict] = {}
    for item in out:
        key = _candidate_key(item)
        if key not in merged or float(item.get("score", 0.0) or 0.0) > float(merged[key].get("score", 0.0) or 0.0):
            merged[key] = item

    ranked = sorted(merged.values(), key=lambda x: float(x.get("score", 0.0) or 0.0), reverse=True)
    summary = {
        "recommendation_mode": recommendation_mode,
        "temperature": round(temperature, 3),
        "taste_profile": {
            "topics": [
                {"term": topic, "weight": round(float(weight), 4)}
                for topic, weight in preferred_topics
            ],
            "authors": [
                {"name": author, "weight": round(float(weight), 4)}
                for author, weight in preferred_authors
            ],
            "venues": [
                {"name": venue, "weight": round(float(weight), 4)}
                for venue, weight in preferred_venues
            ],
            "recent_wins": [
                {"query": query, "strength": round(float(strength), 4)}
                for query, strength in recent_win_queries
            ],
        },
        "negative_profile": {
            "topics": _top_negative_terms(dict(negative_context.get("topics") or {}), limit=4, field_name="term"),
            "authors": _top_negative_terms(dict(negative_context.get("authors") or {}), limit=4, field_name="name"),
            "venues": _top_negative_terms(dict(negative_context.get("journals") or {}), limit=3, field_name="name"),
        },
        "budgets": {
            "taste_budget_total": int(taste_budget_total),
            "followed_author_budget": int(follow_budget),
            "branch_explorer_enabled": bool(branch_enabled),
            "branch_controls": branch_controls,
            "effective_branch_controls": effective_branch_controls,
            "taste_lanes_enabled": {
                "topics": bool(taste_topics_enabled),
                "authors": bool(taste_authors_enabled),
                "venues": bool(taste_venues_enabled),
                "recent_wins": bool(recent_wins_enabled),
            },
        },
        "lane_runs": lane_runs[:24],
        "cold_start_topic": {
            "keyword": explicit_topic_keyword,
            "seed_count": len(seeds),
            "enabled": bool(explicit_topic_keyword) or (lens["context_type"] == "topic_keyword"),
        } if lens["context_type"] == "topic_keyword" else None,
    }
    # All lane futures have been resolved above — release worker threads.
    lane_executor.shutdown(wait=False)
    return ranked[:limit], summary
