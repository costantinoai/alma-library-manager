"""Discovery persistence & CRUD.

Settings, recommendations, lenses, lens-signals, branch-control lifecycle, and
row/JSON mapping — split out of the discovery god-module (D-9). Pure move:
signatures unchanged and every name is re-exported from
``alma.application.discovery`` for backward compatibility.

This is a *leaf* layer: it depends only on the canonical core/scoring helpers,
the discovery defaults, and ``application.library`` (whose discovery import is
deferred, so this top-level import stays cycle-free). It never imports the
retrieval or seed-profile layers — those depend on it, not the reverse.
"""

from __future__ import annotations

import hashlib
import json
import math
import sqlite3
import uuid
from collections import defaultdict
from datetime import datetime, timedelta
from typing import Any, Optional

from alma.application import library as library_app
from alma.core.components import not_component_sql
from alma.core.db_write import run_write_unit
from alma.core.sql_helpers import canonical_paper_filter
from alma.core.scoring_math import age_decay, clamp
from alma.discovery.defaults import (
    DISCOVERY_SETTINGS_DEFAULTS,
    merge_discovery_defaults,
)

_clamp = clamp  # D-3: canonical clamp under the legacy local name


def _safe_div(numerator: float, denominator: float) -> float:
    """Safe division that returns 0.0 when denominator <= 0."""
    if denominator <= 0:
        return 0.0
    return numerator / denominator


VALID_CONTEXT_TYPES = {"library_global", "collection", "topic_keyword", "tag"}


DEFAULT_CHANNEL_WEIGHTS: dict[str, dict[str, float]] = {
    "library_global": {"lexical": 0.30, "vector": 0.35, "graph": 0.20, "external": 0.15},
    "collection": {"lexical": 0.25, "vector": 0.40, "graph": 0.20, "external": 0.15},
    "topic_keyword": {"lexical": 0.45, "vector": 0.25, "graph": 0.10, "external": 0.20},
    "tag": {"lexical": 0.35, "vector": 0.30, "graph": 0.15, "external": 0.20},
}


VALID_RECOMMENDATION_ACTIONS = {"save", "read", "like", "love", "dismiss", "dislike", "seen"}


_PAPER_DISMISS_SIGNAL_SOFT = -0.38


_PAPER_DISMISS_SIGNAL_HARD = -0.82


_PAPER_DISMISS_DECAY_HALF_LIFE_DAYS = 180.0


_PAPER_DISMISS_HARD_HALF_LIFE_DAYS = 365.0


_PAPER_DISMISS_HARD_THRESHOLD = 3


_PAPER_DISMISS_SUPPRESSION_THRESHOLD = -0.18


DEFAULT_BRANCH_CONTROLS: dict[str, Any] = {
    "temperature": None,
    # Cluster-granularity knob (mirrors the Insights graph "Cluster detail").
    # None → engine default 1.0. >1 finer (more branches); <1 coarser.
    "resolution": None,
    "pinned": [],
    "muted": [],
    "boosted": [],
}


def _table_exists(db: sqlite3.Connection, table: str) -> bool:
    row = db.execute(
        "SELECT name FROM sqlite_master WHERE type = 'table' AND name = ?",
        (table,),
    ).fetchone()
    return row is not None


def read_settings(db: sqlite3.Connection) -> dict[str, str]:
    """Read discovery settings as key/value strings merged with defaults."""
    try:
        rows = db.execute("SELECT key, value FROM discovery_settings").fetchall()
    except sqlite3.OperationalError:
        rows = []
    return merge_discovery_defaults({r["key"]: r["value"] for r in rows})


def upsert_setting(db: sqlite3.Connection, key: str, value: str) -> None:
    """Insert or update one discovery setting row."""
    db.execute(
        """
        INSERT INTO discovery_settings (key, value, updated_at)
        VALUES (?, ?, CURRENT_TIMESTAMP)
        ON CONFLICT(key) DO UPDATE SET value = excluded.value, updated_at = excluded.updated_at
        """,
        (key, value),
    )


def reset_settings_to_defaults(db: sqlite3.Connection) -> None:
    """Reset all settings keys to built-in defaults."""
    for key, value in DISCOVERY_SETTINGS_DEFAULTS.items():
        upsert_setting(db, key, value)


def list_recommendations(
    db: sqlite3.Connection,
    *,
    limit: int = 50,
    offset: int = 0,
    search: Optional[str] = None,
    semantic: bool = False,
) -> list[dict]:
    """List recommendations with optional lexical search, enriched with paper data."""
    query = [
        "SELECT r.*,"
        " p.title AS paper_title,"
        " p.authors AS paper_authors,"
        " p.abstract AS paper_abstract,"
        " p.year AS paper_year,"
        " p.journal AS paper_journal,"
        " p.url AS paper_url,"
        " p.doi AS paper_doi,"
        " p.publication_date AS paper_publication_date,"
        " p.cited_by_count AS paper_cited_by_count,"
        " p.status AS paper_status,"
        " p.rating AS paper_rating,"
        " p.openalex_id AS paper_openalex_id,"
        " p.tldr AS paper_tldr,"
        " p.influential_citation_count AS paper_influential_citation_count",
        "FROM recommendations r",
        "LEFT JOIN papers p ON p.id = r.paper_id",
        "WHERE 1=1",
    ]
    params: list[object] = []
    if search and search.strip():
        pattern = f"%{search.strip()}%"
        query.append("AND (p.title LIKE ? OR p.authors LIKE ? OR r.paper_id LIKE ?)")
        params.extend([pattern, pattern, pattern])
    if semantic:
        # Semantic filtering is not implemented in phase-6 application layer.
        # Keep deterministic lexical behavior.
        pass
    # `canonical_paper_id IS NULL` drops recs whose paper was merged
    # into a published journal twin (preprint_dedup). The canonical
    # version will have its own rec row if the lens retrieval touched it.
    # `not_component_sql` drops figures / SI / datasets / author responses —
    # the same component read-gate the Feed inbox uses, so a part-of-a-paper
    # never surfaces as a standalone recommendation either.
    query.append(
        "AND r.user_action IS NULL AND p.status NOT IN ('library', 'dismissed', 'removed') "
        "AND COALESCE(TRIM(p.reading_status), '') = '' "
        f"AND {canonical_paper_filter('p')} "
        "AND " + not_component_sql("p") + " "
        "ORDER BY r.score DESC, COALESCE(p.publication_date, printf('%04d-01-01', COALESCE(p.year, 0))) DESC, r.created_at DESC LIMIT ? OFFSET ?"
    )
    params.extend([limit, offset])
    rows = db.execute(" ".join(query), params).fetchall()
    # Read-time logical-duplicate safety net (mirrors the Feed inbox). Rows are
    # ranked score DESC, so keep the FIRST occurrence of each (year + normalized
    # title) and drop lower-ranked twins — the v0.19.0 duplicate-identity
    # regression surfaced the same paper under two paper_ids. canonical_paper_filter
    # already hides rows the background collapse merged; this covers the window
    # before it runs. Strong-id conflicts (distinct DOIs / openalex_ids) never fold.
    from alma.core.utils import logical_dup_signature, strong_identifiers_conflict

    deduped: list[sqlite3.Row] = []
    seen: dict[str, dict] = {}
    for r in rows:
        sig = logical_dup_signature(r["paper_title"], r["paper_year"])
        keeper = seen.get(sig) if sig is not None else None
        if keeper is not None and not strong_identifiers_conflict(
            incoming_doi=r["paper_doi"],
            incoming_openalex_id=r["paper_openalex_id"],
            candidate_doi=keeper["doi"],
            candidate_openalex_id=keeper["openalex_id"],
        ):
            continue
        if sig is not None:
            seen[sig] = {"doi": r["paper_doi"], "openalex_id": r["paper_openalex_id"]}
        deduped.append(r)
    return [_normalize_recommendation(dict(r)) for r in deduped]


def mark_recommendation_action(
    db: sqlite3.Connection,
    rec_id: str,
    action: str,
    *,
    rating: Optional[int] = None,
) -> Optional[dict]:
    """Apply user action to recommendation row."""
    row = db.execute("SELECT * FROM recommendations WHERE id = ?", (rec_id,)).fetchone()
    if not row:
        return None

    if action not in VALID_RECOMMENDATION_ACTIONS:
        raise ValueError(f"Unsupported recommendation action: {action}")

    paper_id = str((row["paper_id"] if isinstance(row, sqlite3.Row) else "") or "").strip()
    current_rating_row = db.execute(
        "SELECT rating FROM papers WHERE id = ?",
        (paper_id,),
    ).fetchone() if paper_id else None
    current_rating = int((current_rating_row["rating"] if current_rating_row else 0) or 0)

    effective_rating = current_rating
    feedback_action: str | None = None
    stamp_recommendation = action in {"save", "read", "dismiss", "seen"}
    now = datetime.utcnow().isoformat()

    def _persist() -> None:
        # One atomic discovery-action unit (writer gate + BEGIN IMMEDIATE +
        # retry): paper membership/rating → recommendation stamp →
        # cross-surface reconcile → signal event → per-lens signal, committed
        # together. add_to_library defers enrichment scheduling past the gate;
        # record_paper_feedback (now the shared engine adapter) and
        # record_lens_signal do not commit. The engine keys on paper_id so it
        # won't double the explicit recommendation/lens writes below.
        nonlocal effective_rating, feedback_action
        if action == "save":
            effective_rating = max(current_rating, int(rating or 3), 3)
            if paper_id:
                library_app.add_to_library(
                    db,
                    paper_id,
                    rating=effective_rating,
                    added_from="discovery_save",
                )
            feedback_action = "save"
        elif action == "read":
            if paper_id:
                db.execute(
                    """
                    UPDATE papers
                    SET reading_status = 'reading',
                        updated_at = ?
                    WHERE id = ?
                    """,
                    (now, paper_id),
                )
        elif action in {"like", "love"}:
            effective_rating = int(rating or (5 if action == "love" else 4))
            effective_rating = max(1, min(5, effective_rating))
            if paper_id:
                db.execute(
                    "UPDATE papers SET rating = ?, updated_at = ? WHERE id = ?",
                    (effective_rating, now, paper_id),
                )
            feedback_action = "love" if effective_rating >= 5 else "like"
        elif action == "dismiss":
            effective_rating = 1
            feedback_action = "dismiss"
        elif action == "dislike":
            effective_rating = 1
            if paper_id:
                db.execute(
                    "UPDATE papers SET rating = ?, updated_at = ? WHERE id = ?",
                    (effective_rating, now, paper_id),
                )
            feedback_action = "dislike"

        if stamp_recommendation:
            if paper_id and action in {"read", "dismiss"}:
                db.execute(
                    """
                    UPDATE recommendations
                    SET user_action = ?, action_at = ?
                    WHERE paper_id = ?
                      AND (user_action IS NULL OR TRIM(user_action) = '')
                    """,
                    (action, now, paper_id),
                )
            else:
                db.execute(
                    "UPDATE recommendations SET user_action = ?, action_at = ? WHERE id = ?",
                    (action, now, rec_id),
                )

        if paper_id and action == "save":
            library_app.sync_surface_resolution(
                db,
                paper_id,
                action="save",
                source_surface="discovery",
            )
        if paper_id and feedback_action:
            library_app.record_paper_feedback(
                db,
                paper_id,
                action=feedback_action,
                rating=effective_rating,
                source_surface="discovery",
            )
        lens_id = row["lens_id"] if isinstance(row, sqlite3.Row) else None
        if lens_id and paper_id and feedback_action:
            signal_value = library_app.rating_signal_value(effective_rating)
            record_lens_signal(
                db,
                lens_id=str(lens_id),
                paper_id=paper_id,
                signal_value=signal_value,
                source="recommendation_action",
            )

    run_write_unit(db, _persist, label="discovery_rec_action")
    return {
        "id": rec_id,
        action: True,
        "paper_id": paper_id,
        "user_action": action if stamp_recommendation else None,
        "action_at": now if stamp_recommendation else None,
        "rating": effective_rating,
    }


def _parse_action_datetime(raw: object) -> datetime | None:
    text = str(raw or "").strip()
    if not text:
        return None
    try:
        return datetime.fromisoformat(text.replace("Z", "+00:00")).replace(tzinfo=None)
    except ValueError:
        try:
            return datetime.fromisoformat(text[:10])
        except ValueError:
            return None


def _paper_dismissal_scores(rows: list[sqlite3.Row]) -> dict[str, float]:
    """Return decayed suppression scores for dismissed Discovery papers.

    Mirrors the author-rail dismissal model: one dismissal is a temporary
    negative signal, repeated dismissals stack, and three recent dismissals add
    a stronger long half-life penalty. The score is used only to decide whether
    a paper is still cooling down before being eligible for fresh Discovery
    recommendations.
    """
    now = datetime.utcnow()
    grouped: dict[str, list[datetime | None]] = defaultdict(list)
    for row in rows:
        paper_id = str(row["paper_id"] or "").strip()
        if not paper_id:
            continue
        acted_at = _parse_action_datetime(row["action_at"]) or _parse_action_datetime(row["created_at"])
        grouped[paper_id].append(acted_at)

    scores: dict[str, float] = {}
    for paper_id, timestamps in grouped.items():
        score = 0.0
        valid_timestamps: list[datetime] = []
        for acted_at in timestamps:
            age_days = max(0.0, (now - acted_at).total_seconds() / 86400.0) if acted_at else 0.0
            score += _PAPER_DISMISS_SIGNAL_SOFT * age_decay(
                age_days,
                half_life_days=_PAPER_DISMISS_DECAY_HALF_LIFE_DAYS,
            )
            if acted_at:
                valid_timestamps.append(acted_at)
        if len(timestamps) >= _PAPER_DISMISS_HARD_THRESHOLD:
            latest = max(valid_timestamps) if valid_timestamps else now
            age_days = max(0.0, (now - latest).total_seconds() / 86400.0)
            score += _PAPER_DISMISS_SIGNAL_HARD * age_decay(
                age_days,
                half_life_days=_PAPER_DISMISS_HARD_HALF_LIFE_DAYS,
            )
        scores[paper_id] = score
    return scores


def clear_recommendations(db: sqlite3.Connection) -> int:
    """Delete all recommendations and return deleted count."""
    count_row = db.execute("SELECT COUNT(*) AS c FROM recommendations").fetchone()
    total = int((count_row["c"] if count_row else 0) or 0)
    db.execute("DELETE FROM recommendations")
    return total


def recommendation_stats(db: sqlite3.Connection) -> dict:
    """Aggregate recommendation stats.

    The ``actioned`` count is ``SUM(user_action IS NOT NULL)`` — the number
    of recommendations where the user has taken an explicit action (save,
    like, love, dismiss). It was previously named ``seen``, which wrongly
    implied "viewed / impression count"; those are not recorded here.
    """
    row = db.execute(
        """
        SELECT
            COUNT(*) AS total,
            SUM(CASE WHEN user_action IS NOT NULL THEN 1 ELSE 0 END) AS actioned,
            SUM(CASE WHEN user_action = 'save' THEN 1 ELSE 0 END) AS saved,
            SUM(CASE WHEN user_action = 'like' THEN 1 ELSE 0 END) AS liked,
            SUM(CASE WHEN user_action = 'dismiss' THEN 1 ELSE 0 END) AS dismissed
        FROM recommendations
        """
    ).fetchone()
    return {
        "total": int((row["total"] if row else 0) or 0),
        "actioned": int((row["actioned"] if row else 0) or 0),
        "saved": int((row["saved"] if row else 0) or 0),
        "liked": int((row["liked"] if row else 0) or 0),
        "dismissed": int((row["dismissed"] if row else 0) or 0),
    }


def get_recommendation(db: sqlite3.Connection, rec_id: str) -> Optional[dict]:
    """Get one recommendation row."""
    row = db.execute("SELECT * FROM recommendations WHERE id = ?", (rec_id,)).fetchone()
    return _normalize_recommendation(dict(row)) if row else None


def list_lenses(
    db: sqlite3.Connection,
    *,
    is_active: Optional[bool] = None,
    limit: int = 100,
    offset: int = 0,
) -> list[dict]:
    """List discovery lenses with signal/recommendation counts."""
    where = ["1=1"]
    params: list[object] = []
    if is_active is not None:
        where.append("l.is_active = ?")
        params.append(1 if is_active else 0)

    rows = db.execute(
        f"""
        SELECT
            l.*,
            COALESCE(ls.signal_count, 0) AS signal_count,
            COALESCE(r.recommendation_count, 0) AS recommendation_count,
            (
                SELECT ss.id
                FROM suggestion_sets ss
                WHERE ss.lens_id = l.id
                ORDER BY COALESCE(ss.created_at, '') DESC
                LIMIT 1
            ) AS last_suggestion_set_id,
            (
                SELECT ss.ranker_version
                FROM suggestion_sets ss
                WHERE ss.lens_id = l.id
                ORDER BY COALESCE(ss.created_at, '') DESC
                LIMIT 1
            ) AS last_ranker_version,
            (
                SELECT ss.retrieval_summary
                FROM suggestion_sets ss
                WHERE ss.lens_id = l.id
                ORDER BY COALESCE(ss.created_at, '') DESC
                LIMIT 1
            ) AS last_retrieval_summary
        FROM discovery_lenses l
        LEFT JOIN (
            SELECT lens_id, COUNT(*) AS signal_count
            FROM lens_signals
            GROUP BY lens_id
        ) ls ON ls.lens_id = l.id
        LEFT JOIN (
            SELECT lens_id, COUNT(*) AS recommendation_count
            FROM recommendations
            GROUP BY lens_id
        ) r ON r.lens_id = l.id
        WHERE {" AND ".join(where)}
        ORDER BY l.created_at DESC
        LIMIT ? OFFSET ?
        """,
        [*params, limit, offset],
    ).fetchall()
    return [_map_lens_row(r) for r in rows]


def get_lens(db: sqlite3.Connection, lens_id: str) -> Optional[dict]:
    """Fetch one lens by ID."""
    rows = db.execute(
        """
        SELECT
            l.*,
            COALESCE(ls.signal_count, 0) AS signal_count,
            COALESCE(r.recommendation_count, 0) AS recommendation_count,
            (
                SELECT ss.id
                FROM suggestion_sets ss
                WHERE ss.lens_id = l.id
                ORDER BY COALESCE(ss.created_at, '') DESC
                LIMIT 1
            ) AS last_suggestion_set_id,
            (
                SELECT ss.ranker_version
                FROM suggestion_sets ss
                WHERE ss.lens_id = l.id
                ORDER BY COALESCE(ss.created_at, '') DESC
                LIMIT 1
            ) AS last_ranker_version,
            (
                SELECT ss.retrieval_summary
                FROM suggestion_sets ss
                WHERE ss.lens_id = l.id
                ORDER BY COALESCE(ss.created_at, '') DESC
                LIMIT 1
            ) AS last_retrieval_summary
        FROM discovery_lenses l
        LEFT JOIN (
            SELECT lens_id, COUNT(*) AS signal_count
            FROM lens_signals
            GROUP BY lens_id
        ) ls ON ls.lens_id = l.id
        LEFT JOIN (
            SELECT lens_id, COUNT(*) AS recommendation_count
            FROM recommendations
            GROUP BY lens_id
        ) r ON r.lens_id = l.id
        WHERE l.id = ?
        """,
        (lens_id,),
    ).fetchall()
    if not rows:
        return None
    return _map_lens_row(rows[0])


def create_lens(
    db: sqlite3.Connection,
    *,
    name: str,
    context_type: str,
    context_config: Optional[dict] = None,
    weights: Optional[dict] = None,
    branch_controls: Optional[dict] = None,
) -> dict:
    """Create a new discovery lens."""
    if context_type not in VALID_CONTEXT_TYPES:
        raise ValueError(f"Invalid context_type: {context_type}")

    lens_id = uuid.uuid4().hex
    now = datetime.utcnow().isoformat()
    effective_weights = weights or default_channel_weights(context_type)
    db.execute(
        """
        INSERT INTO discovery_lenses (
            id, name, context_type, context_config, weights, branch_controls, created_at, is_active
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, 1)
        """,
        (
            lens_id,
            name.strip(),
            context_type,
            _json_dump(context_config),
            _json_dump(effective_weights),
            _json_dump(_normalize_branch_controls(branch_controls)),
            now,
        ),
    )
    lens = get_lens(db, lens_id)
    if lens is None:
        raise RuntimeError("Lens creation failed")
    return lens


def update_lens(
    db: sqlite3.Connection,
    lens_id: str,
    *,
    name: Optional[str] = None,
    context_config: Optional[dict] = None,
    weights: Optional[dict] = None,
    branch_controls: Optional[dict] = None,
    is_active: Optional[bool] = None,
) -> Optional[dict]:
    """Partially update a discovery lens."""
    updates: list[str] = []
    params: list[object] = []
    if name is not None:
        updates.append("name = ?")
        params.append(name.strip())
    if context_config is not None:
        updates.append("context_config = ?")
        params.append(_json_dump(context_config))
    if weights is not None:
        updates.append("weights = ?")
        params.append(_json_dump(weights))
    if branch_controls is not None:
        updates.append("branch_controls = ?")
        params.append(_json_dump(_normalize_branch_controls(branch_controls)))
    if is_active is not None:
        updates.append("is_active = ?")
        params.append(1 if is_active else 0)

    if not updates:
        return get_lens(db, lens_id)

    params.append(lens_id)

    cursor = db.execute(
        f"UPDATE discovery_lenses SET {', '.join(updates)} WHERE id = ?",
        params,
    )
    if cursor.rowcount == 0:
        return None
    return get_lens(db, lens_id)


def apply_branch_control_action(
    db: sqlite3.Connection,
    *,
    branch_id: str,
    action: str,
) -> dict[str, Any]:
    """Apply one branch control action across lenses that emitted this branch."""
    branch_id = str(branch_id or "").strip()
    action = str(action or "").strip().lower()
    if not branch_id:
        raise ValueError("branch_id is required")

    if action == "cool":
        action = "mute"
    if action not in {"pin", "boost", "mute", "reset"}:
        raise ValueError(f"Unsupported branch action: {action}")

    lens_rows = db.execute(
        """
        SELECT DISTINCT lens_id
        FROM recommendations
        WHERE branch_id = ?
          AND COALESCE(lens_id, '') <> ''
        """,
        (branch_id,),
    ).fetchall()
    lens_ids = [str(row["lens_id"] or "").strip() for row in lens_rows if str(row["lens_id"] or "").strip()]
    updated_lenses: list[str] = []

    for lens_id in lens_ids:
        lens = get_lens(db, lens_id)
        if lens is None:
            continue
        controls = _normalize_branch_controls(lens.get("branch_controls"))
        pinned = [item for item in controls.get("pinned") or [] if item != branch_id]
        boosted = [item for item in controls.get("boosted") or [] if item != branch_id]
        muted = [item for item in controls.get("muted") or [] if item != branch_id]

        if action == "pin":
            pinned.append(branch_id)
        elif action == "boost":
            boosted.append(branch_id)
        elif action == "mute":
            muted.append(branch_id)

        next_controls = {
            **controls,
            "pinned": pinned,
            "boosted": boosted,
            "muted": muted,
        }
        update_lens(db, lens_id, branch_controls=next_controls)
        updated_lenses.append(lens_id)

    return {
        "branch_id": branch_id,
        "action": action,
        "matched_lenses": len(lens_ids),
        "updated_lenses": updated_lenses,
    }


# Auto-weight tuning constants. The weight is a continuous multiplier on
# branch retrieval budget derived from past per-branch outcomes (saves and
# negative Discovery actions recorded in `recommendations`).
#
# - PRIOR_STRENGTH: pseudo-count of "neutral" actions blended in. Acts as a
#   Bayesian prior so a branch with 2 dismisses doesn't get crushed to 0.3x.
#   Higher = more conservative; weight stays near 1.0 until samples accumulate.
# - WEIGHT_FLOOR / WEIGHT_CEIL: clamps so a single bad streak can't kill a
#   branch, and a single good streak can't dominate the budget pool.
# - HALF_LIFE_DAYS: exponential decay so a 60-day-old action contributes
#   ~20% of a fresh action. Lets a branch recover from a bad past.
_AUTO_WEIGHT_PRIOR_STRENGTH = 6.0


_AUTO_WEIGHT_FLOOR = 0.3


_AUTO_WEIGHT_CEIL = 1.8


_AUTO_WEIGHT_HALF_LIFE_DAYS = 30.0


# Branch auto-lifecycle thresholds. When a branch's auto_weight drops
# meaningfully below neutral, the system intervenes:
#   - At ROTATE_THRESHOLD, the branch's `core_topics` are swapped with
#     its `explore_topics` for the next refresh — same seed set, but
#     probing a different conceptual angle. If saves come in under the
#     new angle, auto_weight rises and the rotation reverses
#     automatically (everything is recomputed each refresh, so the
#     mapping is deterministic and self-correcting).
#   - At MUTE_THRESHOLD, the branch is auto-muted: external lane
#     receives no budget for it. The cluster's seeds still influence
#     ranking (centroid + author / topic affinity carry through), but
#     the system stops asking external APIs for more like it.
# The user can override with an explicit pin/boost; manual mute is
# still respected. These thresholds are advisory, not a kill switch.
_AUTO_WEIGHT_ROTATE_THRESHOLD = 0.65


_AUTO_WEIGHT_MUTE_THRESHOLD = 0.55


def _compute_branch_auto_weight(
    *,
    weighted_positive: float,
    weighted_dismissed: float,
    raw_total: int,
) -> tuple[float, str]:
    """Map per-branch outcome stats to a continuous budget multiplier.

    Returns (weight, reason). Weight is in [_AUTO_WEIGHT_FLOOR, _AUTO_WEIGHT_CEIL];
    1.0 means "neutral, no effect on allocation".

    The math: blend the observed positive share with a 50/50 prior of strength
    PRIOR_STRENGTH, then map deviations from 0.5 linearly into the [floor, ceil]
    range (0.5 -> 1.0, 1.0 -> CEIL, 0.0 -> FLOOR).

    Brand-new branches (no signal yet) get a small visibility lift —
    1.15× — so they're guaranteed enough surface area on their first
    couple of refreshes to actually accumulate save/dismiss feedback.
    Without it, a new branch competes with established ones at neutral
    1.0 and may never accumulate the ~6–10 actions needed before the
    smoothed share moves meaningfully.
    """
    weighted_total = weighted_positive + weighted_dismissed
    if weighted_total <= 0.0 or raw_total <= 0:
        return 1.15, "new branch — surfacing for early feedback"

    smoothed_share = (weighted_positive + 0.5 * _AUTO_WEIGHT_PRIOR_STRENGTH) / (
        weighted_total + _AUTO_WEIGHT_PRIOR_STRENGTH
    )
    if smoothed_share >= 0.5:
        weight = 1.0 + (smoothed_share - 0.5) * 2.0 * (_AUTO_WEIGHT_CEIL - 1.0)
    else:
        weight = 1.0 - (0.5 - smoothed_share) * 2.0 * (1.0 - _AUTO_WEIGHT_FLOOR)
    weight = round(_clamp(weight, _AUTO_WEIGHT_FLOOR, _AUTO_WEIGHT_CEIL), 3)

    raw_share = weighted_positive / weighted_total if weighted_total > 0 else 0.0
    if raw_total < 3:
        reason = f"thin signal ({raw_total} actions) — staying near neutral"
    elif weight >= 1.15:
        reason = f"{raw_share * 100:.0f}% positive across {raw_total} actions — boosting"
    elif weight <= 0.85:
        reason = f"{(1.0 - raw_share) * 100:.0f}% dismiss across {raw_total} actions — pulling back"
    else:
        reason = f"mixed outcomes across {raw_total} actions — neutral"
    return weight, reason


def _decay_factor(action_iso: str, *, today_julian: float, half_life: float) -> float:
    """Return exp(-age_days / half_life). Newer action -> closer to 1.0.

    Anything we can't parse (missing or malformed timestamp) is treated as
    "today" — so we don't lose the action, we just stop decaying it.
    """
    try:
        action_date = datetime.fromisoformat(str(action_iso)[:10])
    except (TypeError, ValueError):
        return 1.0
    age_days = max(0.0, today_julian - action_date.toordinal())
    return math.exp(-age_days / max(1.0, half_life))


def _aggregate_branch_outcomes(
    db: sqlite3.Connection,
    *,
    lens_id: Optional[str] = None,
    days: int = 60,
) -> dict[str, dict[str, Any]]:
    """Walk the recommendations table once, returning per-branch outcome stats.

    Keyed by branch_id when present, else by `label:<lower-label>` so callers
    can look up branches that lost their stable id but kept their label. Each
    value carries raw counts (total/positive/dismissed/unseen/unique_sources)
    plus exponentially-decayed weighted_positive / weighted_dismissed used by
    the auto-weight computation.
    """
    if not _table_exists(db, "recommendations"):
        return {}
    since = (datetime.utcnow() - timedelta(days=max(7, int(days or 60)))).date().isoformat()
    params: list[Any] = [since]
    lens_clause = ""
    if str(lens_id or "").strip():
        lens_clause = "AND COALESCE(lens_id, '') = ?"
        params.append(str(lens_id).strip())
    try:
        rows = db.execute(
            f"""
            SELECT
                COALESCE(NULLIF(branch_id, ''), '') AS branch_id,
                COALESCE(NULLIF(branch_label, ''), 'Unnamed branch') AS branch_label,
                COALESCE(user_action, '') AS user_action,
                COALESCE(action_at, created_at) AS acted_at,
                COALESCE(score, 0.0) AS score,
                COALESCE(NULLIF(source_type, ''), 'unknown') AS source_type
            FROM recommendations
            WHERE (COALESCE(branch_id, '') <> '' OR COALESCE(branch_label, '') <> '')
              AND substr(COALESCE(action_at, created_at), 1, 10) >= ?
              {lens_clause}
            """,
            params,
        ).fetchall()
    except sqlite3.OperationalError:
        return {}

    today_julian = float(datetime.utcnow().toordinal())
    grouped: dict[str, dict[str, Any]] = {}
    for row in rows:
        branch_id = str(row["branch_id"] or "").strip()
        branch_label = str(row["branch_label"] or branch_id or "Unnamed branch").strip() or "Unnamed branch"
        action = str(row["user_action"] or "").strip().lower()
        decay = _decay_factor(row["acted_at"], today_julian=today_julian, half_life=_AUTO_WEIGHT_HALF_LIFE_DAYS)
        bucket = grouped.setdefault(
            branch_id or f"label:{branch_label.lower()}",
            {
                "branch_id": branch_id or None,
                "branch_label": branch_label,
                "total": 0,
                "positive": 0,
                "dismissed": 0,
                "unseen": 0,
                "weighted_positive": 0.0,
                "weighted_dismissed": 0.0,
                "score_sum": 0.0,
                "source_types": set(),
            },
        )
        bucket["total"] += 1
        bucket["score_sum"] += float(row["score"] or 0.0)
        bucket["source_types"].add(str(row["source_type"] or "unknown"))
        if action in ("like", "save"):
            bucket["positive"] += 1
            bucket["weighted_positive"] += decay
        elif action in ("dismiss", "dismissed", "dislike", "disliked"):
            bucket["dismissed"] += 1
            bucket["weighted_dismissed"] += decay
        else:
            bucket["unseen"] += 1
    return grouped


def _load_branch_outcome_map(
    db: sqlite3.Connection,
    *,
    lens_id: Optional[str] = None,
    days: int = 60,
) -> dict[str, dict[str, Any]]:
    """Per-branch outcome dict consumed by the branch tile API + budget allocator.

    Keyed by branch_id when present and also by `label:<lower-label>` so callers
    can fall back when a branch lost its stable id. Each value carries the raw
    outcome counts plus the derived `auto_weight` and `auto_weight_reason`.
    """
    aggregated = _aggregate_branch_outcomes(db, lens_id=lens_id, days=days)
    outcome_map: dict[str, dict[str, Any]] = {}
    for bucket in aggregated.values():
        total = int(bucket["total"])
        if total <= 0:
            continue
        positive = int(bucket["positive"])
        dismissed = int(bucket["dismissed"])
        unseen = int(bucket["unseen"])
        weight, reason = _compute_branch_auto_weight(
            weighted_positive=float(bucket["weighted_positive"]),
            weighted_dismissed=float(bucket["weighted_dismissed"]),
            raw_total=total,
        )
        outcome = {
            "recommendation_count": total,
            "avg_score": round(float(bucket["score_sum"]) / total, 3),
            "positive_rate": round(_safe_div(float(positive), float(total)), 3),
            "dismiss_rate": round(_safe_div(float(dismissed), float(total)), 3),
            "engagement_rate": round(_safe_div(float(positive + dismissed), float(total)), 3),
            "unseen": unseen,
            "unique_sources": len(bucket["source_types"]),
            "auto_weight": weight,
            "auto_weight_reason": reason,
        }
        branch_id = bucket["branch_id"]
        branch_label = str(bucket["branch_label"] or "").strip() or "Unnamed branch"
        if branch_id:
            outcome_map[branch_id] = outcome
        outcome_map[f"label:{branch_label.lower()}"] = outcome
    return outcome_map


def delete_lens(db: sqlite3.Connection, lens_id: str) -> bool:
    """Delete one lens."""
    cursor = db.execute("DELETE FROM discovery_lenses WHERE id = ?", (lens_id,))
    return cursor.rowcount > 0


def list_lens_signals(
    db: sqlite3.Connection,
    lens_id: str,
    *,
    limit: int = 100,
) -> list[dict]:
    """List captured lens-specific user signals."""
    rows = db.execute(
        """
        SELECT
            ls.id,
            ls.lens_id,
            ls.paper_id,
            ls.signal_value,
            ls.source,
            ls.created_at,
            p.title AS paper_title
        FROM lens_signals ls
        LEFT JOIN papers p ON p.id = ls.paper_id
        WHERE ls.lens_id = ?
        ORDER BY ls.created_at DESC
        LIMIT ?
        """,
        (lens_id, limit),
    ).fetchall()
    return [
        {
            "id": r["id"],
            "lens_id": r["lens_id"],
            "paper_id": r["paper_id"],
            "paper_title": r["paper_title"],
            "signal_value": int(r["signal_value"] or 0),
            "source": r["source"] or "user",
            "created_at": r["created_at"],
        }
        for r in rows
    ]


def list_lens_recommendations(
    db: sqlite3.Connection,
    lens_id: str,
    *,
    limit: int = 100,
    offset: int = 0,
) -> list[dict]:
    """List recommendations for one lens, enriched with paper metadata."""
    rows = db.execute(
        """
        SELECT r.*,
               p.title AS paper_title,
               p.authors AS paper_authors,
               p.abstract AS paper_abstract,
               p.year AS paper_year,
               p.journal AS paper_journal,
               p.url AS paper_url,
               p.doi AS paper_doi,
               p.publication_date AS paper_publication_date,
               p.cited_by_count AS paper_cited_by_count,
               p.status AS paper_status,
               p.rating AS paper_rating,
               p.openalex_id AS paper_openalex_id,
               p.tldr AS paper_tldr,
               p.influential_citation_count AS paper_influential_citation_count
        FROM recommendations r
        LEFT JOIN papers p ON p.id = r.paper_id
        WHERE r.lens_id = ?
          AND r.user_action IS NULL
          AND p.status NOT IN ('library', 'dismissed', 'removed')
          AND COALESCE(TRIM(p.reading_status), '') = ''
        ORDER BY r.score DESC, COALESCE(r.rank, 999999) ASC, r.created_at DESC
        LIMIT ? OFFSET ?
        """,
        (lens_id, limit, offset),
    ).fetchall()
    return [_normalize_recommendation(dict(r)) for r in rows]


def default_channel_weights(context_type: str) -> dict[str, float]:
    """Return normalized default retrieval channel weights for one context."""
    weights = DEFAULT_CHANNEL_WEIGHTS.get(context_type)
    if not weights:
        raise ValueError(f"Invalid context_type: {context_type}")
    total = sum(float(v or 0.0) for v in weights.values())
    if total <= 0:
        return {"lexical": 0.25, "vector": 0.25, "graph": 0.25, "external": 0.25}
    return {k: float(v) / total for k, v in weights.items()}


def record_lens_signal(
    db: sqlite3.Connection,
    *,
    lens_id: str,
    paper_id: str,
    signal_value: int,
    source: str = "user",
) -> bool:
    """Insert or update a lens-specific feedback signal."""
    row = db.execute("SELECT id FROM discovery_lenses WHERE id = ?", (lens_id,)).fetchone()
    if not row:
        return False
    db.execute(
        """
        INSERT INTO lens_signals (lens_id, paper_id, signal_value, source)
        VALUES (?, ?, ?, ?)
        ON CONFLICT(lens_id, paper_id, source) DO UPDATE SET
            signal_value = excluded.signal_value,
            created_at = datetime('now')
        """,
        (lens_id, paper_id, int(signal_value), source),
    )
    return True


def _normalize_channel_weights(raw: dict) -> dict[str, float]:
    keys = ("lexical", "vector", "graph", "external")
    parsed = {k: float(raw.get(k, 0.0) or 0.0) for k in keys}
    total = sum(parsed.values())
    if total <= 0:
        return {"lexical": 0.25, "vector": 0.25, "graph": 0.25, "external": 0.25}
    return {k: parsed[k] / total for k in keys}


def _map_lens_row(row: sqlite3.Row) -> dict:
    return {
        "id": row["id"],
        "name": row["name"],
        "context_type": row["context_type"],
        "context_config": _json_load(row["context_config"]),
        "weights": _json_load(row["weights"]),
        "branch_controls": _normalize_branch_controls(_json_load(row["branch_controls"])),
        "created_at": row["created_at"],
        "last_refreshed_at": row["last_refreshed_at"],
        "is_active": bool(row["is_active"]),
        "signal_count": int(row["signal_count"] or 0),
        "recommendation_count": int(row["recommendation_count"] or 0),
        "last_suggestion_set_id": row["last_suggestion_set_id"],
        "last_ranker_version": row["last_ranker_version"],
        "last_retrieval_summary": _json_load(row["last_retrieval_summary"]),
    }


def _json_dump(value: Optional[dict]) -> Optional[str]:
    if value is None:
        return None
    return json.dumps(value)


def _json_load(value: Optional[str]) -> Optional[dict]:
    if not value:
        return None
    try:
        parsed = json.loads(value)
        return parsed if isinstance(parsed, dict) else None
    except Exception:
        return None


def _normalize_branch_controls(raw: Optional[dict]) -> dict[str, Any]:
    value = raw if isinstance(raw, dict) else {}
    out: dict[str, Any] = dict(DEFAULT_BRANCH_CONTROLS)

    temp_raw = value.get("temperature")
    if temp_raw is not None:
        try:
            out["temperature"] = round(_clamp(float(temp_raw), 0.0, 1.0), 3)
        except (TypeError, ValueError):
            out["temperature"] = None

    res_raw = value.get("resolution")
    if res_raw is not None:
        try:
            out["resolution"] = round(_clamp(float(res_raw), 0.5, 3.0), 3)
        except (TypeError, ValueError):
            out["resolution"] = None

    for key in ("pinned", "muted", "boosted"):
        seen: set[str] = set()
        normalized: list[str] = []
        items = value.get(key) if isinstance(value.get(key), list) else []
        for item in items:
            branch_id = str(item or "").strip()
            if not branch_id or branch_id in seen:
                continue
            seen.add(branch_id)
            normalized.append(branch_id)
        out[key] = normalized

    if out["pinned"]:
        out["boosted"] = [bid for bid in out["boosted"] if bid not in set(out["pinned"])]
    if out["muted"]:
        muted = set(out["muted"])
        out["pinned"] = [bid for bid in out["pinned"] if bid not in muted]
        out["boosted"] = [bid for bid in out["boosted"] if bid not in muted]

    return out


def _resolve_lens_branch_controls(lens: Optional[dict]) -> dict[str, Any]:
    return _normalize_branch_controls((lens or {}).get("branch_controls"))


def _enrich_branches_with_outcomes(
    branches: list[dict],
    outcome_map: dict[str, dict[str, Any]],
    *,
    db: Optional[sqlite3.Connection] = None,
    lens_id: Optional[str] = None,
) -> list[dict]:
    """Merge per-branch outcome stats (incl. auto_weight) into each branch dict.

    Three-tier lookup so a long-running user's calibration history is not
    silently orphaned every time K-means reshuffles a single seed:

    1. **Exact branch_id match** — the common path; same cluster identity
       across refreshes.
    2. **Label fallback** — `label:<lower-label>` for cases where the
       seed set didn't drift but the id derivation differs (legacy rows).
    3. **Lineage match** — when `db` and `lens_id` are provided, look up
       past branch_ids in `recommendations` for this lens and find any
       previous branch whose seed set overlaps ≥ 70 % with the current
       cluster's seed set. Inherit that branch's outcome history. This
       is what catches the "user added 5 papers, K-means reshuffled,
       branch got a new id, calibration reset" failure mode.
    """
    enriched: list[dict] = []
    lineage_lookup_cache: Optional[list[dict]] = None
    LINEAGE_OVERLAP_THRESHOLD = 0.70
    for branch in branches:
        branch_id = str(branch.get("id") or "").strip()
        branch_label = str(branch.get("label") or "").strip()
        outcome = (
            outcome_map.get(branch_id)
            or outcome_map.get(f"label:{branch_label.lower()}")
            or {}
        )
        if not outcome and db is not None and lens_id and branch.get("seed_context"):
            current_seed_ids = {
                str(s.get("paper_id") or "").strip()
                for s in (branch.get("seed_context") or [])
                if str(s.get("paper_id") or "").strip()
            }
            if current_seed_ids:
                if lineage_lookup_cache is None:
                    lineage_lookup_cache = _load_branch_seed_history(db, lens_id=lens_id)
                best_overlap = 0.0
                best_outcome: dict[str, Any] = {}
                for prior in lineage_lookup_cache:
                    prior_seed_ids = prior.get("seed_ids") or set()
                    if not prior_seed_ids:
                        continue
                    overlap = len(current_seed_ids & prior_seed_ids) / max(
                        len(current_seed_ids | prior_seed_ids), 1
                    )
                    if overlap >= LINEAGE_OVERLAP_THRESHOLD and overlap > best_overlap:
                        prior_id = prior.get("branch_id") or ""
                        prior_label = prior.get("branch_label") or ""
                        candidate_outcome = (
                            outcome_map.get(prior_id)
                            or outcome_map.get(f"label:{str(prior_label).lower()}")
                            or {}
                        )
                        if candidate_outcome:
                            best_overlap = overlap
                            best_outcome = {
                                **candidate_outcome,
                                "auto_weight_reason": (
                                    str(candidate_outcome.get("auto_weight_reason") or "")
                                    + f" (inherited via {overlap:.0%} seed overlap)"
                                ).strip(),
                            }
                outcome = best_outcome
        enriched.append({**branch, **outcome})
    return enriched


def _apply_branch_auto_lifecycle(branches: list[dict]) -> list[dict]:
    """Apply auto-rotation / auto-mute based on each branch's auto_weight.

    Pure transformation — runs after `_enrich_branches_with_outcomes` so
    every branch has its `auto_weight`, but before retrieval so the
    rotated topics actually shape the external query plan.

    User-set pin / boost / mute take precedence. A pinned or boosted
    branch isn't auto-rotated (the user explicitly said it's good).
    A user-muted branch stays muted. Rotation only fires when the
    branch is otherwise normal AND auto_weight crossed the rotate
    threshold.
    """
    out: list[dict] = []
    for branch in branches:
        if branch.get("is_pinned") or branch.get("is_boosted"):
            out.append(branch)
            continue
        if branch.get("is_muted"):
            out.append(branch)
            continue

        weight = float(branch.get("auto_weight") or 1.0)
        item = dict(branch)
        if weight <= _AUTO_WEIGHT_MUTE_THRESHOLD:
            item["is_active"] = False
            item["is_muted"] = True
            item["control_state"] = "auto_muted"
            item["auto_managed_state"] = "auto_muted"
            reason = item.get("auto_weight_reason") or ""
            item["auto_weight_reason"] = (
                f"{reason} — auto-muted (auto_weight {weight:.2f} ≤ {_AUTO_WEIGHT_MUTE_THRESHOLD})"
                .strip()
            )
        elif weight <= _AUTO_WEIGHT_ROTATE_THRESHOLD:
            core = list(item.get("core_topics") or [])
            explore = list(item.get("explore_topics") or [])
            if core and explore:
                item["core_topics"] = explore
                item["explore_topics"] = core
                item["auto_managed_state"] = "rotated"
                item["rotation_note"] = (
                    "Topics rotated: probing the explore-angle while the "
                    "core angle accumulates dismisses. Will revert when "
                    "auto_weight recovers above "
                    f"{_AUTO_WEIGHT_ROTATE_THRESHOLD}."
                )
                # Refresh the human-readable label from the new core_topics
                # so Branch Studio displays what the branch is actually
                # probing this refresh.
                item["label"] = " / ".join(item["core_topics"][:2]) or item.get("label")
                reason = item.get("auto_weight_reason") or ""
                item["auto_weight_reason"] = (
                    f"{reason} — rotating to explore angle (auto_weight "
                    f"{weight:.2f} ≤ {_AUTO_WEIGHT_ROTATE_THRESHOLD})"
                    .strip()
                )
        out.append(item)
    return out


def _load_branch_seed_history(
    db: sqlite3.Connection,
    *,
    lens_id: str,
    limit: int = 200,
) -> list[dict]:
    """Snapshot of past `(branch_id, seed_ids)` for one lens.

    The recommendations table records each rec's `branch_id` and the
    seed paper_ids it sprang from indirectly through co-occurrence —
    we approximate the past branch's seed set by collecting the
    `paper_id`s of recommendations that carried that branch_id from
    library-status seeds. This is good enough for the lineage match
    in `_enrich_branches_with_outcomes`.
    """
    if not _table_exists(db, "recommendations"):
        return []
    try:
        rows = db.execute(
            """
            SELECT
                COALESCE(NULLIF(branch_id, ''), '') AS branch_id,
                COALESCE(NULLIF(branch_label, ''), '') AS branch_label,
                paper_id
            FROM recommendations
            WHERE COALESCE(lens_id, '') = ?
              AND COALESCE(NULLIF(branch_id, ''), '') <> ''
            ORDER BY created_at DESC
            LIMIT ?
            """,
            (str(lens_id), int(limit)),
        ).fetchall()
    except sqlite3.OperationalError:
        return []
    grouped: dict[str, dict[str, Any]] = {}
    for row in rows:
        bid = str(row["branch_id"] or "").strip()
        if not bid:
            continue
        bucket = grouped.setdefault(bid, {
            "branch_id": bid,
            "branch_label": str(row["branch_label"] or "").strip(),
            "seed_ids": set(),
        })
        pid = str(row["paper_id"] or "").strip()
        if pid:
            bucket["seed_ids"].add(pid)
    return list(grouped.values())


def _branch_control_state(branch_id: str, controls: dict[str, Any]) -> str:
    if branch_id in set(controls.get("muted") or []):
        return "muted"
    if branch_id in set(controls.get("pinned") or []):
        return "pinned"
    if branch_id in set(controls.get("boosted") or []):
        return "boosted"
    return "normal"


def _resolve_branch_control_via_lineage(
    branch: dict,
    pinned: set[str],
    muted: set[str],
    boosted: set[str],
    history: list[dict],
) -> tuple[bool, bool, bool]:
    """Recover pin/mute/boost via seed-set overlap when branch_id drifted.

    Mirrors the calibration-history lineage match in
    `_enrich_branches_with_outcomes`. Returns (pinned, boosted, muted).
    """
    if not history or not (branch.get("seed_context") or []):
        return False, False, False
    current_seed_ids = {
        str(s.get("paper_id") or "").strip()
        for s in (branch.get("seed_context") or [])
        if str(s.get("paper_id") or "").strip()
    }
    if not current_seed_ids:
        return False, False, False
    LINEAGE_OVERLAP_THRESHOLD = 0.70
    out_pinned = out_boosted = out_muted = False
    for prior in history:
        prior_seed_ids = prior.get("seed_ids") or set()
        if not prior_seed_ids:
            continue
        overlap = len(current_seed_ids & prior_seed_ids) / max(
            len(current_seed_ids | prior_seed_ids), 1
        )
        if overlap < LINEAGE_OVERLAP_THRESHOLD:
            continue
        prior_id = prior.get("branch_id") or ""
        if not prior_id:
            continue
        if prior_id in pinned:
            out_pinned = True
        if prior_id in boosted:
            out_boosted = True
        if prior_id in muted:
            out_muted = True
    return out_pinned, out_boosted, out_muted


def _apply_branch_controls(
    branches: list[dict],
    controls: dict[str, Any],
    *,
    db: Optional[sqlite3.Connection] = None,
    lens_id: Optional[str] = None,
) -> list[dict]:
    pinned = set(controls.get("pinned") or [])
    muted = set(controls.get("muted") or [])
    boosted = set(controls.get("boosted") or [])

    history: list[dict] = []
    if db is not None and lens_id and (pinned or muted or boosted):
        history = _load_branch_seed_history(db, lens_id=str(lens_id))

    annotated: list[dict] = []
    for branch in branches:
        branch_id = str(branch.get("id") or "").strip()
        state = _branch_control_state(branch_id, controls)
        is_pinned = branch_id in pinned
        is_boosted = branch_id in boosted
        is_muted = branch_id in muted
        # Lineage fallback: if the branch_id changed because K-means
        # reshuffled, recover the user's pin/mute/boost via seed-set
        # overlap with past branches.
        if not (is_pinned or is_boosted or is_muted) and history:
            inh_pinned, inh_boosted, inh_muted = _resolve_branch_control_via_lineage(
                branch, pinned, muted, boosted, history
            )
            if inh_pinned and not is_muted:
                is_pinned = True
                state = "pinned"
            elif inh_boosted and not is_muted:
                is_boosted = True
                state = "boosted"
            if inh_muted:
                is_muted = True
                state = "muted"
        item = {
            **branch,
            "control_state": state,
            "is_pinned": is_pinned,
            "is_boosted": is_boosted,
            "is_muted": is_muted,
            "is_active": not is_muted,
        }
        annotated.append(item)

    def _sort_key(branch: dict) -> tuple[int, int, float, int]:
        state = str(branch.get("control_state") or "normal")
        state_rank = {"pinned": 0, "boosted": 1, "normal": 2, "muted": 3}.get(state, 2)
        active_rank = 0 if branch.get("is_active") else 1
        return (
            active_rank,
            state_rank,
            -float(branch.get("branch_score") or 0.0),
            -int(branch.get("seed_count") or 0),
        )

    annotated.sort(key=_sort_key)
    return annotated


def _make_branch_id(
    lens_id: Optional[str],
    cluster_seed_ids: list[str],
    core_topics: list[str],
) -> str:
    """Build a stable branch ID from the cluster's *identity*, not its labels.

    D-AUDIT-5 (2026-04-23): the previous implementation hashed
    ``core_topics`` + top-3 ``sample_papers``. Both are downstream derivations
    of the clustering — when the seed set drifted (new library rows, rating
    changes), keyword extraction produced different top-N topics and/or
    strength-sorting shuffled the top-3 samples. Preview and refresh ran the
    same pipeline but saw different seed sets, so the resulting branch IDs
    drifted — the preview UI rendered zero metrics for every branch because
    IDs never matched the stored ``recommendations.branch_id``.

    The fix hashes the cluster's *full sorted seed paper ID set*, scoped by
    ``lens_id``. Two calls that cluster the same papers together under the
    same lens produce the same ID; a cluster that truly changes membership
    (a paper moves between clusters) correctly gets a new ID. Core topics
    are still used for the human-readable slug, but no longer affect
    identity — so labels can drift without breaking the preview ↔ stored
    join.
    """
    seed_ids = sorted({str(sid or "").strip() for sid in cluster_seed_ids if str(sid or "").strip()})
    lens_prefix = (lens_id or "").strip()
    # Fall back to topic-keyed basis only when we have no seed IDs at all
    # (cold-start keyword branches) — rare, but keeps the function total.
    if seed_ids:
        basis = f"{lens_prefix}||{'|'.join(seed_ids)}"
    else:
        core_key = "|".join(
            (topic or "").strip().lower() for topic in core_topics[:3] if (topic or "").strip()
        )
        basis = f"{lens_prefix}||topic:{core_key}" if core_key else f"{lens_prefix}||branch"
    digest = hashlib.sha1(basis.encode("utf-8")).hexdigest()[:10]
    # Human-readable slug still comes from core_topics — labels can drift
    # without affecting identity because `digest` is now decoupled from them.
    core_label_key = "|".join(
        (topic or "").strip().lower() for topic in core_topics[:3] if (topic or "").strip()
    )
    slug_parts = [part for part in core_label_key.replace("/", " ").split("|") if part][:2]
    slug = "-".join(
        "".join(ch if ch.isalnum() else "-" for ch in part).strip("-")
        for part in slug_parts
    ).strip("-")
    slug = slug or "branch"
    return f"branch-{slug[:36]}-{digest}"


def _normalize_recommendation(row: dict) -> dict:
    if "paper_id" not in row:
        raise ValueError("Recommendation row missing required v3 field: paper_id")

    # Build paper sub-object from joined columns (paper_title, paper_authors, etc.)
    paper = None
    if row.get("paper_title"):
        paper = {
            "id": row["paper_id"],
            "title": row.get("paper_title") or "",
            "authors": row.get("paper_authors") or "",
            "abstract": row.get("paper_abstract") or "",
            "year": row.get("paper_year"),
            "journal": row.get("paper_journal") or "",
            "url": row.get("paper_url") or "",
            "doi": row.get("paper_doi") or "",
            "publication_date": row.get("paper_publication_date") or None,
            "cited_by_count": row.get("paper_cited_by_count") or 0,
            "status": row.get("paper_status") or "tracked",
            "rating": row.get("paper_rating") or 0,
            "openalex_id": row.get("paper_openalex_id") or "",
            # T5: surface S2 TLDR + influential citation count on the
            # paper sub-object. Null/0 defaults stay silent on the
            # frontend when the row pre-dates the rollout.
            "tldr": row.get("paper_tldr") or None,
            "influential_citation_count": row.get("paper_influential_citation_count") or 0,
        }

    return {
        "id": row["id"],
        "suggestion_set_id": row.get("suggestion_set_id"),
        "lens_id": row.get("lens_id"),
        "paper_id": row["paper_id"],
        "rank": row.get("rank"),
        "score": row.get("score", 0.0),
        "score_breakdown": _json_load(row.get("score_breakdown")) if isinstance(row.get("score_breakdown"), str) else row.get("score_breakdown"),
        "user_action": row.get("user_action"),
        "action_at": row.get("action_at"),
        "source_type": row.get("source_type"),
        "source_api": row.get("source_api"),
        "source_key": row.get("source_key"),
        "branch_id": row.get("branch_id"),
        "branch_label": row.get("branch_label"),
        "branch_mode": row.get("branch_mode"),
        "created_at": row.get("created_at"),
        "paper": paper,
    }
