"""Unified multi-source search for discovery retrieval."""

from __future__ import annotations

import logging
from concurrent.futures import as_completed, wait
from time import perf_counter
from typing import Any

from alma.core.concurrency import bounded_thread_pool
from alma.core.http_sources import bind_source_diagnostics, get_source_http_client
from alma.core.scoring_math import clamp as _clamp
from alma.core.scoring_math import query_match_score, rrf_weight
from alma.core.scoring_math import query_tokens as build_query_tokens
from alma.core.settings_helpers import (
    setting_bool as _setting_bool,
)
from alma.core.utils import (
    candidate_dedup_key as _candidate_key,
)
from alma.discovery import (
    arxiv,
    biorxiv,
    crossref,
    europe_pmc,
    openalex_related,
    semantic_scholar,
)

logger = logging.getLogger(__name__)

# Default cap on wall-clock per lane.  Tuned from D-AUDIT-10c live probe:
# semantic_scholar bulk search timed out in 21/24 lanes, arxiv in 16/24,
# dragging every lane to ~12 s.  bioRxiv — the slowest source that still
# completes reliably — averages ~2.3 s with a ~7 s tail.  A 8 s deadline
# keeps bioRxiv tails intact while cutting the S2 + arxiv tail every lane
# used to wait for.  `openalex` (<1 s) and `crossref` (~1 s) never hit the
# deadline.
DEFAULT_LANE_DEADLINE_S: float = 8.0

# Lane deadline for the *streaming* Find & Add surface.  Unlike the
# Discovery/Feed lens fan-out (where every lane blocks the refresh and
# the 8 s cap above is the floor on total wall-clock), the stream
# renders each source's cards the moment its lane returns — a slow lane
# only delays its own chip.  15 s lets arXiv (3.1 s courtesy pacing +
# routinely slow export.arxiv.org responses) and a once-throttled S2
# finish instead of showing "timeout" on most searches.
FINDADD_LANE_DEADLINE_S: float = 15.0


SOURCE_KEYS = (
    "openalex",
    "semantic_scholar",
    "crossref",
    "arxiv",
    "biorxiv",
    "europe_pmc",
)
PREPRINT_SOURCE_KEYS = frozenset({"arxiv", "biorxiv"})
DEFAULT_SOURCE_ENABLED: dict[str, bool] = {
    "openalex": True,
    "semantic_scholar": True,
    "crossref": True,
    "arxiv": True,
    "biorxiv": True,
    "europe_pmc": True,
}
def _is_blank(value: object) -> bool:
    if value is None:
        return True
    if isinstance(value, str):
        return not value.strip()
    if isinstance(value, (list, tuple, set, dict)):
        return len(value) == 0
    return False


def _merge_unique_list(existing: object, incoming: object) -> list:
    merged: list = []
    seen: set[str] = set()
    for group in (existing or [], incoming or []):
        if not isinstance(group, list):
            continue
        for item in group:
            key = repr(item)
            if key in seen:
                continue
            seen.add(key)
            merged.append(item)
    return merged


def _merge_candidate_metadata(best: dict, other: dict) -> dict:
    merged = dict(best)
    fill_fields = (
        "openalex_id",
        "semantic_scholar_id",
        "semantic_scholar_corpus_id",
        "specter2_embedding",
        "specter2_model",
        "title",
        "authors",
        "abstract",
        "journal",
        "url",
        "doi",
        "canonical_doi",
        "publication_date",
        "year",
    )
    for field in fill_fields:
        if _is_blank(merged.get(field)) and not _is_blank(other.get(field)):
            merged[field] = other.get(field)

    list_fields = (
        "topics",
        "keywords",
        "institutions",
        "authorships",
        "referenced_works",
    )
    for field in list_fields:
        combined = _merge_unique_list(merged.get(field), other.get(field))
        if combined:
            merged[field] = combined

    merged["cited_by_count"] = max(
        int(merged.get("cited_by_count") or 0),
        int(other.get("cited_by_count") or 0),
    )

    # Accumulate provenance across ALL merges: prefer the already-built
    # `source_apis` list when present, falling back to the scalar
    # `source_api`. (Building from the scalars alone dropped earlier
    # sources once a paper was returned by 3+ engines.)
    def _provenance(item: dict) -> list:
        existing = item.get("source_apis")
        if isinstance(existing, list) and existing:
            return existing
        single = str(item.get("source_api") or "").strip()
        return [single] if single else []

    source_names = _merge_unique_list(_provenance(merged), _provenance(other))
    if source_names:
        merged["source_apis"] = source_names
    return merged




def _split_csv_setting(settings: dict[str, str] | None, key: str) -> list[str]:
    """Parse a comma-separated setting into a trimmed-non-empty list.

    Returns ``[]`` when the setting is missing or all whitespace so the
    caller can pass ``value or None`` to opt out of the filter cleanly.
    """
    if not settings:
        return []
    raw = str(settings.get(key) or "")
    return [part.strip() for part in raw.split(",") if part.strip()]


def resolve_source_policy(
    settings: dict[str, str] | None,
) -> dict[str, bool]:
    """Resolve source enable flags; API identity never supplies rank weight."""

    from alma.core.network_policy import network_access_enabled

    if not network_access_enabled():
        return {source: False for source in SOURCE_KEYS}

    cfg = settings or {}
    enabled: dict[str, bool] = {}
    for source in SOURCE_KEYS:
        enabled[source] = _setting_bool(
            cfg,
            f"sources.{source}.enabled",
            DEFAULT_SOURCE_ENABLED[source],
        )

    return enabled


def _build_source_calls(
    query, *, per_source_limit, from_year, settings, semantic_scholar_mode, enabled, s2_fail_fast=False
):
    """Assemble the ``(source_name, thunk)`` list for the enabled discovery
    sources. Shared by ``search_across_sources`` + ``stream_across_sources`` so
    the call shape and the S2 bulk-filter pass-throughs live in one place.

    ``s2_fail_fast`` (Find & Add stream only): the interactive S2 search call
    gets a 1-retry budget and raises on 429 instead of sitting in the shared
    client's background backoff chain (5 retries, up to 60 s) — the lane
    surfaces a truthful "rate-limited" error instead of a generic timeout."""
    s2_fields_of_study = _split_csv_setting(settings, "sources.semantic_scholar.fields_of_study")
    s2_publication_types = _split_csv_setting(settings, "sources.semantic_scholar.publication_types")
    s2_open_access_pdf = _setting_bool(settings or {}, "sources.semantic_scholar.open_access_pdf", False)
    source_calls = []
    from alma.core.provider_quota import provider_can_start
    from alma.openalex.http import SEARCH_COST_CREDITS

    openalex_available = enabled["openalex"] and provider_can_start(
        "openalex",
        required=SEARCH_COST_CREDITS,
    )
    if enabled["openalex"] and not openalex_available:
        logger.info(
            "Skipping OpenAlex search lane: daily quota cannot cover one search; "
            "continuing fallback sources"
        )
    if openalex_available:
        source_calls.append(
            ("openalex", lambda: openalex_related.search_works_hybrid(query, limit=per_source_limit, from_year=from_year))
        )
    # S-10: if S2 just 429'd (its adaptive cooldown is armed process-wide), skip
    # it for the rest of this refresh pass instead of queuing every remaining
    # lane behind the 30 s floor + waiting out each lane deadline. Strictly
    # reduces calls — never adds any. The cooldown self-clears after 60 s.
    s2_cooling = enabled["semantic_scholar"] and get_source_http_client(
        "semantic_scholar"
    ).is_in_adaptive_cooldown()
    if s2_cooling:
        logger.debug("Skipping Semantic Scholar lane: adaptive 429 cooldown active")
    if enabled["semantic_scholar"] and not s2_cooling:
        source_calls.append(
            (
                "semantic_scholar",
                (
                    lambda: semantic_scholar.search_papers_bulk(
                        query,
                        limit=per_source_limit,
                        from_year=from_year,
                        fields_of_study=s2_fields_of_study or None,
                        publication_types=s2_publication_types or None,
                        open_access_pdf=s2_open_access_pdf,
                    )
                    if semantic_scholar_mode == "bulk"
                    else semantic_scholar.search_papers(
                        query,
                        limit=per_source_limit,
                        raise_on_rate_limit=s2_fail_fast,
                        max_retries=1 if s2_fail_fast else None,
                    )
                ),
            )
        )
    if enabled["crossref"]:
        source_calls.append(
            ("crossref", lambda: crossref.search_works(query, limit=per_source_limit, from_year=from_year))
        )
    if enabled["arxiv"]:
        source_calls.append(
            ("arxiv", lambda: arxiv.search_works(query, limit=per_source_limit, from_year=from_year))
        )
    if enabled["biorxiv"]:
        source_calls.append(
            ("biorxiv", lambda: biorxiv.search_works(query, limit=per_source_limit, from_year=from_year))
        )
    if enabled["europe_pmc"]:
        source_calls.append(
            (
                "europe_pmc",
                lambda: europe_pmc.search_works(
                    query,
                    limit=per_source_limit,
                    from_year=from_year,
                ),
            )
        )
    return source_calls


def _stamp_candidate_provenance(candidate: dict, source_name: str, query: str) -> None:
    """Stamp the provenance fields every merge layer sets on a candidate.

    Shared by the Discovery lens merge (`_merge_candidates_from_sources`)
    and the Find & Add ranking (`rank_by_query_relevance`) so the two
    scoring strategies can't drift on what a candidate carries."""
    candidate["source_api"] = source_name
    candidate["source_key"] = str(candidate.get("source_key") or query)
    candidate["source_type"] = (
        str(candidate.get("source_type") or "").strip()
        or ("preprint_lane" if source_name in PREPRINT_SOURCE_KEYS else "external_search")
    )
    candidate["query"] = query


def _merge_candidates_from_sources(
    items_by_source,
    query,
    *,
    limit,
    mode,
    temperature,
    enabled=None,
):
    """Dedup one query without making API identity a relevance feature."""

    merged: dict[str, dict] = {}
    best_rank_score: dict[str, float] = {}
    for source_name, items in items_by_source:
        if enabled is not None and not enabled.get(source_name):
            continue
        if not items:
            continue

        total = max(len(items), 1)
        for idx, item in enumerate(items):
            candidate = dict(item)
            key = _candidate_key(candidate)
            if key in ("url:", "title:"):
                continue

            rank_factor = _clamp(1.0 - (idx / total), 0.05, 1.0)
            candidate["score"] = round(rank_factor, 4)
            _stamp_candidate_provenance(candidate, source_name, query)

            prev = merged.get(key)
            if prev is None:
                merged[key] = candidate
                best_rank_score[key] = rank_factor
                continue
            if best_rank_score[key] < rank_factor:
                merged[key] = _merge_candidate_metadata(candidate, prev)
                best_rank_score[key] = rank_factor
            else:
                merged[key] = _merge_candidate_metadata(prev, candidate)

    query_norm, tokens = build_query_tokens(query)
    text_weight = _clamp(
        0.78 - (0.30 * float(temperature or 0.0))
        - (0.08 if mode == "explore" else 0.0),
        0.40,
        0.80,
    )
    for key, candidate in merged.items():
        text_match = query_match_score(query_norm, tokens, candidate)
        candidate["score"] = round(
            _clamp(
                (text_weight * text_match)
                + ((1.0 - text_weight) * best_rank_score[key]),
                0.0,
                1.0,
            ),
            4,
        )
    ranked = sorted(merged.values(), key=lambda x: float(x.get("score", 0.0) or 0.0), reverse=True)
    return ranked[: max(1, limit)]


def rank_by_query_relevance(items_by_source, query, *, limit, enabled=None):
    """Search-engine ranking for the Find & Add surface (D-FA ranking contract).

    Dedup + rank candidates from ``(source_name, items)`` pairs by closeness
    to the *query*, not personal fit:

    - **Reciprocal Rank Fusion** over each source's own relevance order
      (every engine sorts by relevance; arXiv/Crossref are asked to
      explicitly). A paper returned by several engines accumulates
      ``1/(RRF_K + rank)`` per appearance, so cross-source consensus rises.
    - **Query-text match** (`core.scoring_math.query_match_score`): exact/near title, author
      and abstract matches dominate — pasting a title puts that paper first.

    Final score = 0.7 · text match + 0.3 · normalized RRF. Deliberately
    profile-free: no source weights, no explore/temperature bias, no
    personal-fit scoring — those belong to the Discovery lens merge
    (`_merge_candidates_from_sources`). Personal fit is computed *after*
    ranking, for the per-result chip only.

    ``enabled`` filters sources when provided (the streamed path passes it).
    """
    query = (query or "").strip()
    query_norm, query_tokens = build_query_tokens(query)

    merged: dict[str, dict] = {}
    rrf_by_key: dict[str, float] = {}
    for source_name, items in items_by_source:
        if enabled is not None and not enabled.get(source_name):
            continue
        for idx, item in enumerate(items or []):
            candidate = dict(item)
            key = _candidate_key(candidate)
            if key in ("url:", "title:"):
                continue
            rrf_by_key[key] = rrf_by_key.get(key, 0.0) + rrf_weight(idx + 1)
            _stamp_candidate_provenance(candidate, source_name, query)
            prev = merged.get(key)
            merged[key] = candidate if prev is None else _merge_candidate_metadata(prev, candidate)

    if not merged:
        return []

    max_rrf = max(rrf_by_key.values())
    for key, candidate in merged.items():
        text_match = query_match_score(query_norm, query_tokens, candidate)
        consensus = (rrf_by_key[key] / max_rrf) if max_rrf > 0 else 0.0
        candidate["score"] = round(_clamp((0.7 * text_match) + (0.3 * consensus), 0.0, 1.0), 4)

    ranked = sorted(merged.values(), key=lambda x: float(x.get("score", 0.0) or 0.0), reverse=True)
    return ranked[: max(1, limit)]


def search_across_sources(
    query: str,
    *,
    limit: int,
    from_year: int | None,
    settings: dict[str, str] | None = None,
    mode: str = "core",
    temperature: float = 0.28,
    semantic_scholar_mode: str = "interactive",
    lane_deadline_s: float | None = None,
    diagnostics: dict[str, Any] | None = None,
) -> list[dict]:
    """Run one query across enabled discovery sources and merge candidates.

    Parameters
    ----------
    lane_deadline_s:
        Wall-clock cap on the fan-out.  Sources that have not returned by
        this deadline are dropped for this lane (their thread may keep
        running but the lane does not wait).  Defaults to
        `DEFAULT_LANE_DEADLINE_S`.  Pass `0` or a negative value to
        disable.
    diagnostics:
        If supplied, the function mutates this dict with per-source
        timing and timeout info:
            ``per_source_ms``: {source_name: wall_clock_ms}
            ``timed_out_sources``: [source_name, ...]
            ``slowest_source``: (source_name, ms) of the slowest returned
                                source (excludes timeouts).
    """
    query = (query or "").strip()
    if not query:
        return []

    enabled = resolve_source_policy(settings)
    if not any(enabled.values()):
        return []

    # Per-source limit is intentionally bounded to avoid request explosions.
    per_source_limit = max(3, min(25, limit))
    source_calls = _build_source_calls(
        query,
        per_source_limit=per_source_limit,
        from_year=from_year,
        settings=settings,
        semantic_scholar_mode=semantic_scholar_mode,
        enabled=enabled,
    )

    per_source_ms: dict[str, int] = {}

    def _timed_source(source_name: str, fn):
        """Wrap a source call so its wall-clock is recorded per source."""
        bound = bind_source_diagnostics(fn)

        def _run() -> list[dict]:
            started = perf_counter()
            try:
                return bound() or []
            finally:
                per_source_ms[source_name] = int(round((perf_counter() - started) * 1000))

        return _run

    deadline = lane_deadline_s if lane_deadline_s is not None else DEFAULT_LANE_DEADLINE_S
    deadline_enabled = deadline and deadline > 0

    results_by_source: list[tuple[str, list[dict]]] = []
    timed_out_sources: list[str] = []
    # NOTE: `ThreadPoolExecutor` as a context manager calls
    # `shutdown(wait=True)` on exit, which would block waiting for the
    # slow sources we are trying to abandon.  Manage the executor
    # explicitly so we can return as soon as the deadline fires and call
    # `shutdown(wait=False)` — background HTTP threads finish on their
    # own (each source library already caps its per-request timeout).
    executor = bounded_thread_pool(
        max(1, min(6, len(source_calls))),
        thread_name_prefix="lens-source",
    )
    try:
        future_map = {
            executor.submit(_timed_source(source_name, search_fn)): source_name
            for source_name, search_fn in source_calls
        }
        done, not_done = wait(
            list(future_map.keys()),
            timeout=deadline if deadline_enabled else None,
        )
        for future in done:
            source_name = future_map[future]
            try:
                items = future.result() or []
            except Exception as exc:
                logger.debug("Source search failed (%s): %s", source_name, exc)
                items = []
            results_by_source.append((source_name, items))
        for future in not_done:
            source_name = future_map[future]
            timed_out_sources.append(source_name)
            logger.debug(
                "source %s did not return before lane deadline (%.1fs); dropped",
                source_name,
                deadline,
            )
            # Also attempt cancellation — this will only succeed for futures
            # that have not yet started, but is cheap and harmless otherwise.
            future.cancel()
    finally:
        # wait=False so we return immediately; the running worker threads
        # keep running until their per-request HTTP timeouts fire, then
        # exit naturally.  Python's process-level cleanup joins them on
        # interpreter shutdown.
        executor.shutdown(wait=False)

    if diagnostics is not None:
        diagnostics["per_source_ms"] = dict(per_source_ms)
        diagnostics["timed_out_sources"] = list(timed_out_sources)
        if per_source_ms:
            slowest = max(per_source_ms.items(), key=lambda kv: kv[1])
            diagnostics["slowest_source"] = {"source": slowest[0], "duration_ms": slowest[1]}

    return _merge_candidates_from_sources(
        results_by_source,
        query,
        limit=limit,
        mode=mode,
        temperature=temperature,
    )


def stream_across_sources(
    query: str,
    *,
    limit: int,
    from_year: int | None,
    settings: dict[str, str] | None = None,
    semantic_scholar_mode: str = "interactive",
    lane_deadline_s: float | None = None,
    s2_fail_fast: bool = False,
):
    """Yield per-source results as each lane completes.

    Generator variant of ``search_across_sources`` for the Find & Add
    streaming surface. Emits dict events of one of these shapes:

        {"type": "source_pending",  "source": <name>}            # at lane start
        {"type": "source_complete", "source": <name>, "items": [...], "ms": <int>}
        {"type": "source_timeout",  "source": <name>, "ms": <int>}
        {"type": "source_error",    "source": <name>, "error": <str>, "ms": <int>}

    The caller is responsible for the candidate-merge / dedup / ranking
    layer; this function only fans out and yields raw items so the UI
    can render skeletons → per-source results → final ranked list as
    they arrive.
    """
    query = (query or "").strip()
    if not query:
        return

    enabled = resolve_source_policy(settings)
    if not any(enabled.values()):
        return

    per_source_limit = max(3, min(25, limit))
    source_calls = _build_source_calls(
        query,
        per_source_limit=per_source_limit,
        from_year=from_year,
        settings=settings,
        semantic_scholar_mode=semantic_scholar_mode,
        enabled=enabled,
        s2_fail_fast=s2_fail_fast,
    )

    deadline = lane_deadline_s if lane_deadline_s is not None else DEFAULT_LANE_DEADLINE_S
    deadline_enabled = deadline and deadline > 0

    # Emit pending events up front so the UI can render skeletons immediately.
    for source_name, _ in source_calls:
        yield {"type": "source_pending", "source": source_name}

    executor = bounded_thread_pool(
        max(1, min(6, len(source_calls))),
        thread_name_prefix="findadd-stream",
    )
    started_at = perf_counter()
    try:
        future_map = {
            executor.submit(bind_source_diagnostics(search_fn)): source_name
            for source_name, search_fn in source_calls
        }
        # `as_completed(timeout=)` gives us results in completion order
        # AND raises TimeoutError when the deadline elapses — we catch
        # that and emit timeout events for the remaining lanes.
        try:
            for future in as_completed(
                list(future_map.keys()),
                timeout=deadline if deadline_enabled else None,
            ):
                source_name = future_map[future]
                ms = int(round((perf_counter() - started_at) * 1000))
                try:
                    items = future.result() or []
                    yield {
                        "type": "source_complete",
                        "source": source_name,
                        "items": items,
                        "ms": ms,
                    }
                except Exception as exc:
                    logger.debug("source %s errored: %s", source_name, exc)
                    yield {
                        "type": "source_error",
                        "source": source_name,
                        "error": str(exc),
                        "ms": ms,
                    }
        except TimeoutError:
            ms = int(round((perf_counter() - started_at) * 1000))
            for future, source_name in future_map.items():
                if not future.done():
                    future.cancel()
                    yield {"type": "source_timeout", "source": source_name, "ms": ms}
    finally:
        executor.shutdown(wait=False)


def merge_streamed_results(
    raw_by_source: dict[str, list[dict]],
    query: str,
    *,
    limit: int,
    settings: dict[str, str] | None = None,
) -> list[dict]:
    """Dedup + rank per-source raw items collected via
    `stream_across_sources` by query relevance (RRF + query-text match).

    This is the Find & Add ranking: closeness to what the user *typed*
    decides the order, search-engine style. The Discovery lens merge and its
    branch exploration policy are intentionally not used here."""
    query = (query or "").strip()
    if not query or not raw_by_source:
        return []

    enabled = resolve_source_policy(settings)
    return rank_by_query_relevance(
        raw_by_source.items(),
        query,
        limit=limit,
        enabled=enabled,
    )
