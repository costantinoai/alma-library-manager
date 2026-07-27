"""One read model for Signal Lab ledger evidence.

Sampler and Home summary need identical answers for query identity, coverage,
staleness, skips, and edge history. This module owns those semantics so the
acquisition policy cannot optimize a different metric from the one users see.
"""

from __future__ import annotations

import json
import sqlite3
from dataclasses import dataclass, field

from alma.application.signal_lab.query import canonical_query_key


@dataclass
class RegionEvidence:
    asked: int = 0
    answered: int = 0
    skipped: int = 0
    age_days: float | None = None

    @property
    def answerability(self) -> float:
        """Beta-smoothed probability that user can judge this region."""
        return (self.answered + 2.0) / (self.asked + 3.0)


@dataclass
class EdgeEvidence:
    asked: int = 0
    answered: int = 0
    skipped: int = 0
    conflicts: int = 0
    age_days: float | None = None

    @property
    def answerability(self) -> float:
        return (self.answered + 2.0) / (self.asked + 3.0)

    @property
    def posterior_variance(self) -> float:
        """Beta(1,1) variance for probability current boundary is rejected."""
        alpha = 1.0 + self.conflicts
        beta = 1.0 + max(0, self.answered - self.conflicts)
        total = alpha + beta
        return (alpha * beta) / (total * total * (total + 1.0))


@dataclass
class LedgerEvidence:
    today: int = 0
    total: int = 0
    answered: int = 0
    skipped: int = 0
    unique_queries: int = 0
    duplicate_queries: int = 0
    recent_queries: set[str] = field(default_factory=set)
    regions: dict[int, RegionEvidence] = field(default_factory=dict)
    edges: dict[tuple[int, int], EdgeEvidence] = field(default_factory=dict)


def edge_key(a: int, b: int) -> tuple[int, int]:
    return (a, b) if a < b else (b, a)


def load_ledger_evidence(
    conn: sqlite3.Connection,
    *,
    paper_regions: dict[str, int] | None = None,
    recent_limit: int = 500,
) -> LedgerEvidence:
    """Read ledger once; tolerate pre-feature databases as honest empty state."""
    try:
        rows = conn.execute(
            """
            SELECT id, game_id, region_id, pair_region_id, shown_json,
                   answer_json, skipped, created_at,
                   CASE WHEN date(created_at) = date('now') THEN 1 ELSE 0 END
                       AS is_today,
                   julianday('now') - julianday(created_at) AS age_days
            FROM signal_lab_rounds
            ORDER BY id
            """
        ).fetchall()
    except sqlite3.OperationalError:
        return LedgerEvidence()

    evidence = LedgerEvidence(total=len(rows))
    seen: set[str] = set()
    recent_start = max(0, len(rows) - max(0, recent_limit))
    for index, row in enumerate(rows):
        try:
            shown = [str(pid) for pid in json.loads(row["shown_json"] or "[]")]
            decoded = json.loads(row["answer_json"]) if row["answer_json"] else None
            answer = decoded if isinstance(decoded, dict) else None
        except (TypeError, ValueError):
            shown = []
            answer = None

        game_id = str(row["game_id"])
        query_key = canonical_query_key(game_id, shown)
        if query_key in seen:
            evidence.duplicate_queries += 1
        else:
            seen.add(query_key)
        if index >= recent_start:
            evidence.recent_queries.add(query_key)

        skipped = bool(row["skipped"])
        answered = answer is not None and not skipped
        evidence.today += int(row["is_today"] or 0)
        evidence.skipped += int(skipped)
        evidence.answered += int(answered)

        age_days = max(0.0, float(row["age_days"] or 0.0))
        if row["region_id"] is not None:
            region = evidence.regions.setdefault(int(row["region_id"]), RegionEvidence())
            region.asked += 1
            region.answered += int(answered)
            region.skipped += int(skipped)
            region.age_days = age_days

        if row["region_id"] is None or row["pair_region_id"] is None:
            continue
        key = edge_key(int(row["region_id"]), int(row["pair_region_id"]))
        edge = evidence.edges.setdefault(key, EdgeEvidence())
        edge.asked += 1
        edge.answered += int(answered)
        edge.skipped += int(skipped)
        edge.age_days = age_days
        odd = str((answer or {}).get("odd") or "")
        if (
            answered
            and odd
            and paper_regions is not None
            and paper_regions.get(odd) != int(row["pair_region_id"])
        ):
            edge.conflicts += 1

    evidence.unique_queries = len(seen)
    return evidence
