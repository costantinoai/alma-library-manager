"""Retrieval shared helpers.

Candidate identity keys, source/author/topic/venue bucketing, and the
deadline-bounded future drainer. Split out of the discovery god-module (D-9);
pure move. Depends on nothing else in the discovery package — the merge layer
and the graph channel import from here.
"""

from __future__ import annotations

from alma.core.time import utcnow
from alma.core.utils import candidate_dedup_key
from alma.discovery.scoring import parse_author_names

from .types import RetrievalHit

# The four evidence families the two-level RRF fuses across. A family groups
# retrievers that fail the same way, so fusing inside a family first stops 16
# topic queries from outvoting one vector run.
FAMILY_LEXICAL = "lexical"
FAMILY_SEMANTIC = "semantic"
FAMILY_CITATION = "citation"
FAMILY_TASTE = "taste"
RETRIEVAL_FAMILIES = (FAMILY_LEXICAL, FAMILY_SEMANTIC, FAMILY_CITATION, FAMILY_TASTE)


def attach_hits(
    items: list[dict],
    *,
    family: str,
    retriever_id: str,
    source_api: str | None = None,
    query_key: str | None = None,
    seed_key: str | None = None,
    relation: str | None = None,
    branch_id: str | None = None,
    branch_mode: str | None = None,
) -> list[dict]:
    """Stamp one ``RetrievalHit`` per item for a single retriever run.

    Every lane calls this on the ordered results of each individual query /
    seed / relation, so ``rank`` is meaningful (1-based within *that* run) and
    a candidate surfaced by several runs accumulates several hits.

    Also backfills the flat ``source_type`` / ``source_api`` provenance fields.
    82 of 119 embedding-less recommendations in the dev DB carried a NULL
    ``source_api`` because only the lexical lane stamped it, which made the
    embedding gap unattributable by lane. Provenance is now set at the one
    place every lane already passes through.

    Args:
        items: Ordered candidate dicts from one retriever run, best first.
        family: One of :data:`RETRIEVAL_FAMILIES`.
        retriever_id: Stable identifier for the algorithm/query surface
            (``"lexical:topic"``, ``"graph:reference"``, ``"taste:author"``).
            Deliberately not an API name — API identity must never become an
            extra relevance vote (task 62 §3).
        source_api: Which upstream answered. When omitted, each item must carry
            its own ``source_api``; API identity is exposure only.
        query_key / seed_key / relation / branch_id / branch_mode: Optional
            provenance describing *what* was asked.

    Returns:
        The same list, mutated in place, for call-site chaining.
    """
    if family not in RETRIEVAL_FAMILIES:
        raise ValueError(f"family must be one of {RETRIEVAL_FAMILIES}, got {family!r}")

    stamped_at = utcnow().isoformat()
    pool_size = len(items)
    for idx, item in enumerate(items):
        if not isinstance(item, dict):
            continue
        hit_source_api = str(source_api or item.get("source_api") or "").strip()
        if not hit_source_api:
            raise ValueError("Every retrieval hit requires source_api provenance")
        hit = RetrievalHit(
            candidate_key=_candidate_key(item),
            family=family,
            retriever_id=retriever_id,
            source_api=hit_source_api,
            rank=idx + 1,
            pool_size=pool_size,
            raw_score=_coerce_score(item.get("score")),
            query_key=query_key,
            seed_key=seed_key,
            relation=relation,
            branch_id=branch_id or (str(item.get("branch_id") or "").strip() or None),
            branch_mode=branch_mode or (str(item.get("branch_mode") or "").strip() or None),
            retrieved_at=stamped_at,
        )
        item.setdefault("retrieval_hits", []).append(hit)
        # Flat provenance: fill only when the lane has not already set a more
        # specific value, so a sublane's own label is never overwritten.
        if not str(item.get("source_type") or "").strip():
            item["source_type"] = retriever_id
        if not str(item.get("source_api") or "").strip():
            item["source_api"] = hit_source_api
        if query_key and not str(item.get("source_key") or "").strip():
            item["source_key"] = query_key
    return items


def _coerce_score(value: object) -> float | None:
    try:
        return float(value)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return None

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
    """Use the one canonical candidate identity route."""

    return candidate_dedup_key(item)
