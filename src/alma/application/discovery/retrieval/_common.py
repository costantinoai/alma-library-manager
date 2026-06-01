"""Retrieval shared helpers.

Candidate identity keys, source/author/topic/venue bucketing, and the
deadline-bounded future drainer. Split out of the discovery god-module (D-9);
pure move. Depends on nothing else in the discovery package — the merge layer
and the graph channel import from here.
"""

from __future__ import annotations

from concurrent.futures import as_completed

from alma.core.utils import normalize_doi
from alma.discovery.scoring import parse_author_names


# Wall-clock cap on the graph lane's external citation fallbacks (OpenAlex
# related/citing/referenced + S2 related). The graph lane previously had NO
# overall deadline — when Semantic Scholar rate-limited (429 → up to ~2 min
# of retry+cooldown PER call, see core/http_sources S2 policy), the
# `with ThreadPoolExecutor` (shutdown=wait) fallbacks waited out the full
# budget × calls, ballooning a normally-4 s lane to minutes (task 19 F2
# live-reproduced a 7.3 min hang). Mirrors source_search.DEFAULT_LANE_DEADLINE_S.
_GRAPH_FALLBACK_DEADLINE_S: float = 8.0


def _drain_futures_within_deadline(
    executor: ThreadPoolExecutor,
    future_map: dict,
    deadline_s: float,
) -> dict:
    """Return the subset of ``future_map`` that completed within ``deadline_s``.

    Cancels any futures still pending at the deadline and shuts the executor
    down with ``wait=False`` so a worker stuck in a slow/rate-limited HTTP
    call (e.g. S2 429 retry/cooldown) does not block the caller — it exits on
    its own per-request HTTP timeout. Same bounded-fan-out contract as
    ``discovery/source_search.search_across_sources``.
    """
    done_map: dict = {}
    try:
        for fut in as_completed(list(future_map.keys()), timeout=max(0.1, deadline_s)):
            done_map[fut] = future_map[fut]
    except TimeoutError:
        for fut in future_map:
            if not fut.done():
                fut.cancel()
    finally:
        executor.shutdown(wait=False)
    return done_map


def _candidate_source_bucket(candidate: dict) -> str:
    source_type = str(candidate.get("source_type") or "").strip()
    if source_type:
        return source_type
    if str(candidate.get("branch_id") or "").strip():
        return "branch"
    source_api = str(candidate.get("source_api") or "").strip()
    if source_api:
        return f"external:{source_api}"
    return "lens_retrieval"


def _candidate_author_keys(candidate: dict) -> list[str]:
    names = parse_author_names(str(candidate.get("authors") or ""))
    out: list[str] = []
    seen: set[str] = set()
    for name in names:
        key = " ".join(str(name or "").lower().split())
        if len(key) < 3 or key in seen:
            continue
        seen.add(key)
        out.append(key)
        if len(out) >= 8:
            break
    return out


def _candidate_topic_keys(candidate: dict) -> list[str]:
    out: list[str] = []
    seen: set[str] = set()
    raw = candidate.get("topics") or []
    raw_topics = [raw] if isinstance(raw, str) else list(raw)
    for topic in raw_topics:
        term = ""
        if isinstance(topic, dict):
            term = str(topic.get("term") or topic.get("name") or "").strip().lower()
        else:
            term = str(topic or "").strip().lower()
        if term and term not in seen:
            seen.add(term)
            out.append(term)
        if len(out) >= 5:
            break
    raw_core = candidate.get("branch_core_topics") or []
    raw_explore = candidate.get("branch_explore_topics") or []
    core_topics = [raw_core] if isinstance(raw_core, str) else list(raw_core)
    explore_topics = [raw_explore] if isinstance(raw_explore, str) else list(raw_explore)
    for topic in core_topics + explore_topics:
        term = str(topic or "").strip().lower()
        if term and term not in seen:
            seen.add(term)
            out.append(term)
        if len(out) >= 5:
            break
    return out


def _candidate_venue_key(candidate: dict) -> str:
    return " ".join(str(candidate.get("journal") or "").lower().split())


def _candidate_key(item: dict) -> str:
    doi = normalize_doi((item.get("doi") or "").strip())
    if doi:
        return f"doi:{doi.lower()}"
    openalex_id = (item.get("openalex_id") or "").strip().lower()
    if openalex_id:
        return f"openalex:{openalex_id}"
    title = (item.get("title") or "").strip().lower()
    return f"title:{title}"
