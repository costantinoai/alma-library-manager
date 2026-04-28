"""Unified multi-source search for discovery retrieval."""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor, as_completed, wait
import logging
from time import perf_counter
from typing import Any, Dict, Optional

from alma.core.http_sources import bind_source_diagnostics
from alma.core.utils import normalize_doi
from alma.discovery import arxiv, biorxiv, crossref, openalex_related, semantic_scholar

logger = logging.getLogger(__name__)

# Default cap on wall-clock per lane.  Tuned from D-AUDIT-10c live probe:
# semantic_scholar bulk search timed out in 21/24 lanes, arxiv in 16/24,
# dragging every lane to ~12 s.  bioRxiv — the slowest source that still
# completes reliably — averages ~2.3 s with a ~7 s tail.  A 8 s deadline
# keeps bioRxiv tails intact while cutting the S2 + arxiv tail every lane
# used to wait for.  `openalex` (<1 s) and `crossref` (~1 s) never hit the
# deadline.
DEFAULT_LANE_DEADLINE_S: float = 8.0

SOURCE_KEYS = ("openalex", "semantic_scholar", "crossref", "arxiv", "biorxiv")
PREPRINT_SOURCE_KEYS = frozenset({"arxiv", "biorxiv"})
DEFAULT_SOURCE_ENABLED: dict[str, bool] = {
    "openalex": True,
    "semantic_scholar": True,
    "crossref": True,
    "arxiv": True,
    "biorxiv": True,
}
DEFAULT_SOURCE_WEIGHTS: dict[str, float] = {
    "openalex": 1.0,
    "semantic_scholar": 0.95,
    "crossref": 0.72,
    "arxiv": 0.66,
    "biorxiv": 0.62,
}


def _clamp(value: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, value))


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
    source_names = _merge_unique_list(
        [str(merged.get("source_api") or "").strip()] if str(merged.get("source_api") or "").strip() else [],
        [str(other.get("source_api") or "").strip()] if str(other.get("source_api") or "").strip() else [],
    )
    if source_names:
        merged["source_apis"] = source_names
    return merged


def _candidate_key(item: dict) -> str:
    canonical_doi = normalize_doi((item.get("canonical_doi") or "").strip())
    if canonical_doi:
        return f"doi:{canonical_doi.lower()}"
    doi = normalize_doi((item.get("doi") or "").strip())
    if doi:
        return f"doi:{doi.lower()}"
    title = (item.get("title") or "").strip().lower()
    if title:
        return f"title:{title}"
    url = (item.get("url") or "").strip().lower()
    return f"url:{url}"


def _setting_bool(settings: dict[str, str], key: str, default: bool) -> bool:
    raw = settings.get(key)
    if raw is None:
        return default
    return str(raw).strip().lower() in {"1", "true", "yes", "on"}


def _setting_float(settings: dict[str, str], key: str, default: float) -> float:
    raw = settings.get(key)
    if raw is None:
        return default
    try:
        return float(raw)
    except (TypeError, ValueError):
        return default


def _split_csv_setting(settings: Optional[dict[str, str]], key: str) -> list[str]:
    """Parse a comma-separated setting into a trimmed-non-empty list.

    Returns ``[]`` when the setting is missing or all whitespace so the
    caller can pass ``value or None`` to opt out of the filter cleanly.
    """
    if not settings:
        return []
    raw = str(settings.get(key) or "")
    return [part.strip() for part in raw.split(",") if part.strip()]


def resolve_source_policy(
    settings: Optional[dict[str, str]],
) -> tuple[dict[str, bool], dict[str, float]]:
    """Resolve source enable flags and normalized source weights."""
    cfg = settings or {}
    enabled: dict[str, bool] = {}
    raw_weights: dict[str, float] = {}
    for source in SOURCE_KEYS:
        enabled[source] = _setting_bool(
            cfg,
            f"sources.{source}.enabled",
            DEFAULT_SOURCE_ENABLED[source],
        )
        raw_weights[source] = _setting_float(
            cfg,
            f"sources.{source}.weight",
            DEFAULT_SOURCE_WEIGHTS[source],
        )

    # Backward compatibility with the existing semantic scholar strategy toggle.
    if "strategies.semantic_scholar" in cfg:
        enabled["semantic_scholar"] = _setting_bool(
            cfg,
            "strategies.semantic_scholar",
            enabled["semantic_scholar"],
        )

    active_total = sum(max(0.0, raw_weights[s]) for s in SOURCE_KEYS if enabled[s])
    if active_total <= 0:
        return enabled, dict(DEFAULT_SOURCE_WEIGHTS)
    normalized = {
        source: (max(0.0, raw_weights[source]) / active_total) if enabled[source] else 0.0
        for source in SOURCE_KEYS
    }
    return enabled, normalized


def search_across_sources(
    query: str,
    *,
    limit: int,
    from_year: Optional[int],
    settings: Optional[dict[str, str]] = None,
    mode: str = "core",
    temperature: float = 0.28,
    semantic_scholar_mode: str = "interactive",
    lane_deadline_s: Optional[float] = None,
    diagnostics: Optional[dict[str, Any]] = None,
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

    enabled, source_weights = resolve_source_policy(settings)
    if not any(enabled.values()):
        return []

    # Per-source limit is intentionally bounded to avoid request explosions.
    per_source_limit = max(3, min(25, limit))
    # Semantic Scholar bulk-search filter pass-throughs (T12, 2026-04-25).
    # All three default to None / False so the call shape is unchanged
    # when the user hasn't configured any filter — keeps existing lens
    # behavior stable.  Parsed here once per lane so each source closure
    # captures the resolved values.
    s2_fields_of_study = _split_csv_setting(settings, "sources.semantic_scholar.fields_of_study")
    s2_publication_types = _split_csv_setting(settings, "sources.semantic_scholar.publication_types")
    s2_open_access_pdf = _setting_bool(settings or {}, "sources.semantic_scholar.open_access_pdf", False)
    merged: Dict[str, dict] = {}
    source_calls = []
    if enabled["openalex"]:
        source_calls.append(
            ("openalex", lambda: openalex_related.search_works_hybrid(query, limit=per_source_limit, from_year=from_year))
        )
    if enabled["semantic_scholar"]:
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
                    else semantic_scholar.search_papers(query, limit=per_source_limit)
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
    executor = ThreadPoolExecutor(
        max_workers=max(1, min(5, len(source_calls))),
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

    for source_name, items in results_by_source:
        if not items:
            continue

        total = max(len(items), 1)
        for idx, item in enumerate(items):
            candidate = dict(item)
            key = _candidate_key(candidate)
            if key.endswith("url:"):
                continue

            base = float(candidate.get("score", 0.0) or 0.0)
            rank_factor = _clamp(1.0 - (idx / total), 0.05, 1.0)
            source_weight = float(source_weights.get(source_name, 0.0) or 0.0)
            if source_weight <= 0.0:
                continue

            # Explore mode intentionally rewards slightly deeper ranks to broaden
            # retrieval surface; core mode stays closer to top-ranked items.
            mode_bias = 0.6 if mode == "explore" else 0.85
            temperature_bias = _clamp(0.75 + (temperature * 0.5), 0.65, 1.15)
            weighted = _clamp(
                ((base * mode_bias) + (rank_factor * (1.0 - mode_bias))) * source_weight * temperature_bias,
                0.01,
                1.0,
            )
            candidate["score"] = round(weighted, 4)
            candidate["source_api"] = source_name
            candidate["source_key"] = str(candidate.get("source_key") or query)
            candidate["source_type"] = (
                str(candidate.get("source_type") or "").strip()
                or ("preprint_lane" if source_name in PREPRINT_SOURCE_KEYS else "external_search")
            )
            candidate["query"] = query

            prev = merged.get(key)
            if prev is None:
                merged[key] = candidate
                continue
            if float(prev.get("score", 0.0) or 0.0) < candidate["score"]:
                merged[key] = _merge_candidate_metadata(candidate, prev)
            else:
                merged[key] = _merge_candidate_metadata(prev, candidate)

    ranked = sorted(merged.values(), key=lambda x: float(x.get("score", 0.0) or 0.0), reverse=True)
    return ranked[: max(1, limit)]


def stream_across_sources(
    query: str,
    *,
    limit: int,
    from_year: Optional[int],
    settings: Optional[dict[str, str]] = None,
    mode: str = "core",
    temperature: float = 0.28,
    semantic_scholar_mode: str = "interactive",
    lane_deadline_s: Optional[float] = None,
):
    """Yield per-source results as each lane completes.

    Generator variant of ``search_across_sources`` for the Find & Add
    streaming surface. Emits dict events of one of these shapes:

        {"type": "source_pending",  "source": <name>}            # at lane start
        {"type": "source_complete", "source": <name>, "items": [...], "ms": <int>}
        {"type": "source_timeout",  "source": <name>, "ms": <int>}
        {"type": "source_error",    "source": <name>, "error": <str>, "ms": <int>}

    The caller is responsible for the candidate-merge / dedup / personal
    scoring layer; this function only fans out and yields raw items so
    the UI can render skeletons → per-source results → final ranked
    list as they arrive.
    """
    query = (query or "").strip()
    if not query:
        return

    enabled, _source_weights = resolve_source_policy(settings)
    if not any(enabled.values()):
        return

    per_source_limit = max(3, min(25, limit))
    s2_fields_of_study = _split_csv_setting(settings, "sources.semantic_scholar.fields_of_study")
    s2_publication_types = _split_csv_setting(settings, "sources.semantic_scholar.publication_types")
    s2_open_access_pdf = _setting_bool(settings or {}, "sources.semantic_scholar.open_access_pdf", False)

    source_calls = []
    if enabled["openalex"]:
        source_calls.append(
            ("openalex", lambda: openalex_related.search_works_hybrid(query, limit=per_source_limit, from_year=from_year))
        )
    if enabled["semantic_scholar"]:
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
                    else semantic_scholar.search_papers(query, limit=per_source_limit)
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

    deadline = lane_deadline_s if lane_deadline_s is not None else DEFAULT_LANE_DEADLINE_S
    deadline_enabled = deadline and deadline > 0

    # Emit pending events up front so the UI can render skeletons immediately.
    for source_name, _ in source_calls:
        yield {"type": "source_pending", "source": source_name}

    executor = ThreadPoolExecutor(
        max_workers=max(1, min(5, len(source_calls))),
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
    settings: Optional[dict[str, str]] = None,
    mode: str = "core",
    temperature: float = 0.28,
) -> list[dict]:
    """Apply the same dedup + scoring that `search_across_sources` does
    to a set of per-source raw items already collected via
    `stream_across_sources`. Returns the ranked, dedup'd list."""
    query = (query or "").strip()
    if not query or not raw_by_source:
        return []

    enabled, source_weights = resolve_source_policy(settings)
    merged: Dict[str, dict] = {}

    for source_name, items in raw_by_source.items():
        if not enabled.get(source_name):
            continue
        if not items:
            continue
        total = max(len(items), 1)
        for idx, item in enumerate(items):
            candidate = dict(item)
            key = _candidate_key(candidate)
            if key.endswith("url:"):
                continue

            base = float(candidate.get("score", 0.0) or 0.0)
            rank_factor = _clamp(1.0 - (idx / total), 0.05, 1.0)
            source_weight = float(source_weights.get(source_name, 0.0) or 0.0)
            if source_weight <= 0.0:
                continue

            mode_bias = 0.6 if mode == "explore" else 0.85
            temperature_bias = _clamp(0.75 + (temperature * 0.5), 0.65, 1.15)
            weighted = _clamp(
                ((base * mode_bias) + (rank_factor * (1.0 - mode_bias))) * source_weight * temperature_bias,
                0.01,
                1.0,
            )
            candidate["score"] = round(weighted, 4)
            candidate["source_api"] = source_name
            candidate["source_key"] = str(candidate.get("source_key") or query)
            candidate["source_type"] = (
                str(candidate.get("source_type") or "").strip()
                or ("preprint_lane" if source_name in PREPRINT_SOURCE_KEYS else "external_search")
            )
            candidate["query"] = query

            prev = merged.get(key)
            if prev is None:
                merged[key] = candidate
                continue
            if float(prev.get("score", 0.0) or 0.0) < candidate["score"]:
                merged[key] = _merge_candidate_metadata(candidate, prev)
            else:
                merged[key] = _merge_candidate_metadata(prev, candidate)

    ranked = sorted(merged.values(), key=lambda x: float(x.get("score", 0.0) or 0.0), reverse=True)
    return ranked[: max(1, limit)]
