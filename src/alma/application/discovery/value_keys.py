"""Canonical exact candidate values used by branch-local learning and replay."""

from __future__ import annotations

from alma.core.author_names import parse_author_names
from alma.core.keywords import parse_keywords


def _values(values: list[str], *, limit: int) -> list[str]:
    return list(dict.fromkeys(
        value for raw in values
        if (value := " ".join(str(raw or "").casefold().split()))
    ))[:limit]


def candidate_value_keys(candidate: dict) -> dict[str, list[str]]:
    """Extract paper values, never branch query terms or mutable display labels.

    Author IDs and names remain separate: a shared name cannot silently become
    evidence about a specific identity. Keywords are phrases, not tokens.
    """
    author_ids: list[str] = []
    author_names: list[str] = []
    for authorship in candidate.get("authorships") or []:
        if not isinstance(authorship, dict):
            continue
        author = authorship.get("author") or {}
        if not isinstance(author, dict):
            author = {}
        raw_id = authorship.get("openalex_id") or authorship.get("author_id") or author.get("id")
        if raw_id:
            author_ids.append(str(raw_id).strip().rstrip("/").split("/")[-1])
        raw_name = author.get("display_name") or author.get("name") or authorship.get("name")
        if raw_name:
            author_names.append(str(raw_name))
    if not author_names:
        author_names = parse_author_names(str(candidate.get("authors") or ""))
    raw_topics = candidate.get("topics") or []
    if isinstance(raw_topics, str):
        raw_topics = [raw_topics]
    topic_names = [
        str(topic.get("term") or topic.get("name") or topic.get("display_name") or "")
        if isinstance(topic, dict) else str(topic)
        for topic in raw_topics
    ]
    result = {
        "author_id": _values(author_ids, limit=12),
        "author_name": _values(author_names, limit=12),
        "topic": _values(topic_names, limit=12),
        "venue": _values([str(candidate.get("journal") or "")], limit=1),
        "keyword": _values(parse_keywords(candidate.get("keywords") or []), limit=20),
        "work_type": _values([str(candidate.get("type") or "")], limit=1),
    }
    return result
