#!/usr/bin/env python3
"""Aggregate Discovery evidence in an offline SQLite copy without exposing records.

The database is opened immutable/read-only. Output contains counts and numeric
feature summaries only; paper text, entity values, and identifiers never leave
the process. No application imports or migrations run.
"""

from __future__ import annotations

import argparse
import json
import math
import sqlite3
from collections import defaultdict
from pathlib import Path
from typing import Any

FEATURES: dict[str, tuple[str, ...]] = {
    "author": (
        "author_affinity", "author_affinity_max", "author_affinity_mean",
        "author_affinity_first", "author_affinity_last", "author_affinity_followed",
        "projection_author_raw", "projection_author_name_raw",
    ),
    "topic": ("topic_score", "projection_topic_raw"),
    "venue": ("journal_affinity", "projection_venue_raw"),
    "keyword": ("projection_keyword_raw",),
    "tag": ("projection_tag_raw",),
    "semantic": (
        "semantic_similarity_centroid_raw",
        "semantic_similarity_exemplar_raw", "semantic_similarity_support_raw",
        "semantic_similarity_signal_raw", "semantic_similarity_negative_raw",
        "semantic_similarity_negative_signal_raw", "projection_semantic_neighbor_raw",
    ),
    "recency": ("recency_boost", "paper_age_years"),
    "citation": (
        "citation_quality", "citation_count_log", "influential_citation_count_log",
        "reference_count", "coupling_strength", "cocitation_strength",
        "projection_citation_neighbor_raw",
    ),
}

POSITIVE = frozenset({"save", "saved", "like", "liked", "love"})
NEGATIVE = frozenset({"dislike", "disliked", "remove", "removed"})
VISIBILITY = frozenset({"dismiss", "dismissed"})
NEUTRAL = frozenset({"read", "seen"})


def _columns(db: sqlite3.Connection, table: str) -> set[str]:
    if not db.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (table,)
    ).fetchone():
        return set()
    return {str(row[1]) for row in db.execute(f"PRAGMA table_info({table})")}


def _json_object(raw: Any) -> dict[str, Any]:
    try:
        value = json.loads(raw) if isinstance(raw, str) else raw
    except (TypeError, ValueError):
        return {}
    return value if isinstance(value, dict) else {}


def _action(value: Any) -> str:
    action = str(value or "").strip().lower()
    if action in POSITIVE:
        return "positive"
    if action in NEGATIVE:
        return "negative"
    if action in VISIBILITY:
        return "visibility"
    if action in NEUTRAL:
        return "neutral"
    return "none_or_other"


class NumericSummary:
    def __init__(self) -> None:
        self.available = 0
        self.numeric = 0
        self.nonzero = 0
        self.mean = 0.0
        self.m2 = 0.0
        self.distinct: set[float] = set()

    def add(self, field: Any) -> None:
        if not isinstance(field, dict) or not field.get("availability"):
            return
        self.available += 1
        try:
            value = float(field["value"])
        except (KeyError, TypeError, ValueError):
            return
        if not math.isfinite(value):
            return
        self.numeric += 1
        self.nonzero += value != 0.0
        self.distinct.add(value)
        delta = value - self.mean
        self.mean += delta / self.numeric
        self.m2 += delta * (value - self.mean)

    def result(self) -> dict[str, int | float | None]:
        return {
            "available": self.available,
            "numeric": self.numeric,
            "nonzero": self.nonzero,
            "distinct": len(self.distinct),
            "variance": round(self.m2 / self.numeric, 10) if self.numeric else None,
        }


def audit(db: sqlite3.Connection) -> dict[str, Any]:
    tables = {
        name: _columns(db, name)
        for name in (
            "discovery_lenses", "suggestion_sets", "recommendations",
            "discovery_ranking_candidates", "discovery_impressions", "feedback_events",
        )
    }
    table_counts = {
        name: {"present": bool(columns), "rows": db.execute(f"SELECT COUNT(*) FROM {name}").fetchone()[0] if columns else 0}
        for name, columns in tables.items()
    }

    recommendation_rows: dict[str, tuple[str, str, str]] = {}
    branch_ids: set[str] = set()
    branch_verdicts: set[str] = set()
    action_counts: dict[str, int] = defaultdict(int)
    branch_action_counts: dict[str, int] = defaultdict(int)
    if "id" in tables["recommendations"]:
        branch_sql = "branch_id" if "branch_id" in tables["recommendations"] else "NULL"
        set_sql = "suggestion_set_id" if "suggestion_set_id" in tables["recommendations"] else "NULL"
        action_sql = "user_action" if "user_action" in tables["recommendations"] else "NULL"
        for rec_id, branch_raw, set_raw, action_raw in db.execute(
            f"SELECT id, {branch_sql}, {set_sql}, {action_sql} FROM recommendations"
        ):
            branch = str(branch_raw or "").strip()
            group = _action(action_raw)
            recommendation_rows[str(rec_id)] = (branch, str(set_raw or ""), group)
            action_counts[group] += 1
            if branch:
                branch_ids.add(branch)
                branch_action_counts[group] += 1
                if group in {"positive", "negative"}:
                    branch_verdicts.add(branch)

    impressed_recs: set[str] = set()
    impressed_branch_recs: set[str] = set()
    if "recommendation_id" in tables["discovery_impressions"]:
        for (rec_raw,) in db.execute("SELECT recommendation_id FROM discovery_impressions"):
            rec = str(rec_raw or "")
            impressed_recs.add(rec)
            if recommendation_rows.get(rec, ("", "", ""))[0]:
                impressed_branch_recs.add(rec)

    event_links = 0
    valid_event_links = 0
    branch_event_links = 0
    branch_event_verdicts = 0
    if "context_json" in tables["feedback_events"]:
        value_sql = "value" if "value" in tables["feedback_events"] else "NULL"
        type_sql = "event_type" if "event_type" in tables["feedback_events"] else "NULL"
        for raw_context, raw_value, raw_type in db.execute(
            f"SELECT context_json, {value_sql}, {type_sql} FROM feedback_events"
        ):
            rec = str(_json_object(raw_context).get("recommendation_id") or "").strip()
            if not rec:
                continue
            event_links += 1
            target = recommendation_rows.get(rec)
            if target is None:
                continue
            valid_event_links += 1
            if target[0]:
                branch_event_links += 1
                event_action = _json_object(raw_value).get("action") or raw_type
                if _action(event_action) in {"positive", "negative"}:
                    branch_event_verdicts += 1

    summaries = {
        group: {name: NumericSummary() for name in names}
        for group, names in FEATURES.items()
    }
    snapshots_with_valid_json = 0
    if "reward_features" in tables["discovery_ranking_candidates"]:
        for (raw,) in db.execute("SELECT reward_features FROM discovery_ranking_candidates"):
            reward = _json_object(raw)
            snapshots_with_valid_json += bool(reward)
            for group in summaries.values():
                for name, summary in group.items():
                    summary.add(reward.get(name))

    return {
        "schema_version": 1,
        "tables": table_counts,
        "recommendations": {
            "with_branch": sum(1 for branch, _, _ in recommendation_rows.values() if branch),
            "distinct_branches": len(branch_ids),
            "branches_with_explicit_verdict": len(branch_verdicts),
            "actions": dict(sorted(action_counts.items())),
            "branch_actions": dict(sorted(branch_action_counts.items())),
            "with_impression": len(impressed_recs & recommendation_rows.keys()),
            "with_branch_and_impression": len(impressed_branch_recs),
        },
        "feedback_event_links": {
            "with_recommendation_id": event_links,
            "matching_recommendation": valid_event_links,
            "matching_branch": branch_event_links,
            "matching_branch_explicit_verdict": branch_event_verdicts,
        },
        "snapshots": {
            "valid_json_objects": snapshots_with_valid_json,
            "numeric_features": {
                group: {name: summary.result() for name, summary in fields.items()}
                for group, fields in summaries.items()
            },
        },
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db", type=Path, required=True, help="Offline SQLite database copy")
    args = parser.parse_args()
    if not args.db.is_file():
        parser.error("--db must name an existing SQLite file")
    # immutable=1 prevents journal/shm creation; run only against a consistent copy.
    uri = args.db.resolve().as_uri() + "?mode=ro&immutable=1"
    try:
        with sqlite3.connect(uri, uri=True) as db:
            db.execute("PRAGMA query_only=ON")
            result = audit(db)
    except sqlite3.DatabaseError as exc:
        parser.error(f"cannot read SQLite copy: {exc}")
    print(json.dumps(result, indent=2, sort_keys=True, allow_nan=False))


if __name__ == "__main__":
    main()
