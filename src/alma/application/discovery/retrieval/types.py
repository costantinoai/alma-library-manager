"""Typed retrieval evidence shared by every Discovery lane.

Candidates and retrieval evidence are deliberately separate.  A paper can be
returned by several queries, retrievers, and source APIs; collapsing those
observations before fusion destroys both ranking signal and diagnostics.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Any


@dataclass(frozen=True)
class RetrievalHit:
    """One immutable observation that a retriever returned a candidate."""

    candidate_key: str
    family: str
    retriever_id: str
    source_api: str | None
    rank: int
    pool_size: int
    raw_score: float | None = None
    query_key: str | None = None
    seed_key: str | None = None
    relation: str | None = None
    branch_id: str | None = None
    branch_mode: str | None = None
    retrieved_at: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)
