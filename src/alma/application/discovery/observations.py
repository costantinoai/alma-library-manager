"""Immutable ranking-candidate and actual-visibility observation writes."""

from __future__ import annotations

import json
import sqlite3
import uuid
from dataclasses import dataclass
from typing import Any

from alma.core.time import utcnow

from .exploration import POLICY_VERSION
from .features import FEATURE_SCHEMA_VERSION
from .ranker import RANKER_VERSION


@dataclass(frozen=True)
class LTRObservation:
    """One impressed candidate with an explicitly linked binary outcome."""

    ranking_candidate_id: str
    recommendation_id: str
    suggestion_set_id: str
    paper_id: str
    reward_features: dict
    label: int
    inclusion_probability: float
    position_probability: float
    position: int
    feature_timestamp: str
    outcome_timestamp: str


def ranking_candidate_rows(
    *,
    suggestion_set_id: str,
    lens_id: str,
    candidates: list[dict],
    created_at: str,
) -> list[tuple]:
    """Serialize the full ranked pool once; caller performs the batch write."""

    rows: list[tuple] = []
    for fused_rank, candidate in enumerate(candidates, start=1):
        selection = candidate.get("_selection") or {}
        exposure = dict(candidate.get("exposure_features") or {})
        exposure["selection"] = {
            "selected": selection.get("final_position") is not None,
            "exploration": bool(selection.get("exploration")),
            "inclusion_probability": selection.get("inclusion_probability"),
            "position_probability": selection.get("position_probability"),
            "final_position": selection.get("final_position"),
            "policy_version": POLICY_VERSION,
        }
        rows.append(
            (
                uuid.uuid4().hex,
                suggestion_set_id,
                lens_id,
                str(candidate.get("candidate_key") or ""),
                candidate.get("paper_id"),
                fused_rank,
                float(candidate.get("prior_score") or candidate.get("score") or 0.0),
                (
                    float(candidate["shadow_score"])
                    if candidate.get("shadow_score") is not None
                    else None
                ),
                json.dumps(candidate.get("reward_features") or {}, separators=(",", ":")),
                json.dumps(exposure, separators=(",", ":")),
                json.dumps(candidate.get("retrieval_hits") or [], separators=(",", ":")),
                FEATURE_SCHEMA_VERSION,
                str(
                    (candidate.get("exposure_features") or {}).get("feature_timestamp")
                    or created_at
                ),
                1 if selection.get("final_position") is not None else 0,
                1 if selection.get("exploration") else 0,
                selection.get("inclusion_probability"),
                selection.get("position_probability"),
                selection.get("final_position"),
                POLICY_VERSION,
                RANKER_VERSION,
                created_at,
            )
        )
    return rows


def insert_ranking_candidates(db: sqlite3.Connection, rows: list[tuple]) -> None:
    if not rows:
        return
    db.executemany(
        """
        INSERT INTO discovery_ranking_candidates (
            id, suggestion_set_id, lens_id, candidate_key, paper_id,
            fused_rank, prior_score, shadow_score, reward_features,
            exposure_features, retrieval_hits, feature_schema_version,
            feature_timestamp, selected, exploration,
            inclusion_probability, position_probability, final_position,
            policy_version, ranker_version, created_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        rows,
    )


def record_impressions(
    db: sqlite3.Connection,
    items: list[dict[str, Any]],
    *,
    seen_at: str | None = None,
) -> dict[str, int]:
    """Record first actual visibility per recommendation/surface idempotently."""

    stamp = seen_at or utcnow().isoformat()
    normalized: dict[tuple[str, str], tuple[int, str]] = {}
    for item in items:
        recommendation_id = str(item.get("recommendation_id") or "").strip()
        surface = str(item.get("surface") or "").strip().lower()
        sort_mode = str(item.get("sort_mode") or "").strip().lower()
        try:
            position = int(item.get("position"))
        except (TypeError, ValueError) as exc:
            raise ValueError("Every impression requires a positive position") from exc
        if (
            not recommendation_id
            or surface not in {"discovery_card", "discovery_compact"}
            or sort_mode not in {"relevance", "recent", "custom"}
            or position < 1
        ):
            raise ValueError(
                "Every impression requires recommendation_id, surface, sort_mode, and positive position"
            )
        normalized[(recommendation_id, surface)] = (position, sort_mode)
    if not normalized:
        return {"received": 0, "inserted": 0}

    rec_ids = sorted({key[0] for key in normalized})
    rows_by_id: dict[str, sqlite3.Row] = {}
    for start in range(0, len(rec_ids), 200):
        chunk = rec_ids[start : start + 200]
        placeholders = ",".join("?" for _ in chunk)
        rows = db.execute(
            f"""
            SELECT r.id, r.suggestion_set_id, r.lens_id, r.paper_id,
                   rc.id AS ranking_candidate_id
            FROM recommendations r
            LEFT JOIN discovery_ranking_candidates rc
              ON rc.suggestion_set_id = r.suggestion_set_id
             AND rc.paper_id = r.paper_id
             AND rc.selected = 1
            WHERE r.id IN ({placeholders})
            """,
            chunk,
        ).fetchall()
        rows_by_id.update({str(row["id"]): row for row in rows})
    missing = sorted(set(rec_ids) - set(rows_by_id))
    if missing:
        raise ValueError(f"Unknown recommendation id(s): {', '.join(missing[:5])}")
    unlinked = sorted(
        rec_id
        for rec_id, row in rows_by_id.items()
        if row["ranking_candidate_id"] is None
    )
    if unlinked:
        raise ValueError(
            "Recommendation(s) lack immutable ranking observations: "
            + ", ".join(unlinked[:5])
        )

    before = db.total_changes
    db.executemany(
        """
        INSERT INTO discovery_impressions (
            id, ranking_candidate_id, recommendation_id, suggestion_set_id,
            lens_id, paper_id, position, surface, sort_mode, seen_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(recommendation_id, surface) DO NOTHING
        """,
        [
            (
                uuid.uuid4().hex,
                rows_by_id[rec_id]["ranking_candidate_id"],
                rec_id,
                rows_by_id[rec_id]["suggestion_set_id"],
                rows_by_id[rec_id]["lens_id"],
                rows_by_id[rec_id]["paper_id"],
                position,
                surface,
                sort_mode,
                stamp,
            )
            for (rec_id, surface), (position, sort_mode) in normalized.items()
        ],
    )
    return {
        "received": len(normalized),
        "inserted": db.total_changes - before,
    }


def load_ltr_observations(
    db: sqlite3.Connection,
    *,
    randomized_only: bool = True,
) -> list[LTRObservation]:
    """Load only impressed v3 rows with recommendation-linked outcomes.

    Historical ranks never receive invented propensities.  Feedback is joined
    through the recommendation id stamped by the canonical Discovery action
    path, preventing a later reaction to the same paper from labeling every
    earlier exposure.
    """

    sql = """
        SELECT
            rc.id AS ranking_candidate_id,
            di.recommendation_id,
            rc.suggestion_set_id,
            rc.paper_id,
            rc.reward_features,
            rc.inclusion_probability,
            rc.position_probability,
            di.position,
            rc.feature_timestamp,
            fe.value,
            fe.created_at AS outcome_timestamp
        FROM discovery_ranking_candidates rc
        JOIN discovery_impressions di
          ON di.ranking_candidate_id = rc.id
        JOIN feedback_events fe
          ON json_extract(fe.context_json, '$.recommendation_id')
             = di.recommendation_id
         AND fe.entity_type IN ('publication', 'paper')
         AND fe.entity_id = rc.paper_id
         AND fe.created_at >= di.seen_at
         AND fe.created_at >= rc.feature_timestamp
        WHERE rc.selected = 1
          AND rc.feature_schema_version = ?
          AND rc.inclusion_probability > 0
          AND rc.position_probability > 0
          AND di.sort_mode = 'relevance'
    """
    params: list[Any] = [FEATURE_SCHEMA_VERSION]
    if randomized_only:
        sql += " AND rc.exploration = 1"
    sql += " ORDER BY fe.created_at ASC, rc.id ASC"

    # One explicit outcome per recommendation. Repeated actions keep the first
    # causal response; later preference edits remain useful to taste projection
    # but do not rewrite the original ranking label.
    seen_recommendations: set[str] = set()
    observations: list[LTRObservation] = []
    for row in db.execute(sql, params).fetchall():
        recommendation_id = str(row["recommendation_id"])
        if recommendation_id in seen_recommendations:
            continue
        try:
            value = json.loads(row["value"] or "{}")
        except (TypeError, ValueError):
            continue
        signal = float(value.get("signal_value") or 0.0)
        action = str(value.get("action") or "").strip().lower()
        if signal > 0 or action in {"save", "like", "love"}:
            label = 1
        elif signal < 0 or action in {"dislike", "remove"}:
            label = 0
        else:
            # read/dismiss/seen are resolution or visibility, not preference.
            continue
        try:
            reward_features = json.loads(row["reward_features"] or "{}")
        except (TypeError, ValueError):
            continue
        seen_recommendations.add(recommendation_id)
        observations.append(
            LTRObservation(
                ranking_candidate_id=str(row["ranking_candidate_id"]),
                recommendation_id=recommendation_id,
                suggestion_set_id=str(row["suggestion_set_id"]),
                paper_id=str(row["paper_id"]),
                reward_features=reward_features,
                label=label,
                inclusion_probability=float(row["inclusion_probability"]),
                position_probability=float(row["position_probability"]),
                position=int(row["position"]),
                feature_timestamp=str(row["feature_timestamp"]),
                outcome_timestamp=str(row["outcome_timestamp"]),
            )
        )
    return observations
