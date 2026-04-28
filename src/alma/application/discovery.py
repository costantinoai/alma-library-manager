"""Discovery use-cases (settings, recommendations, lenses, and signals)."""

from __future__ import annotations

import json
import hashlib
import logging
import math
import sqlite3
import struct
import uuid
from collections import defaultdict
from concurrent.futures import Future, ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta
from time import perf_counter
from typing import Any, Callable, Optional

from alma.ai.embedding_sources import EMBEDDING_SOURCE_SEMANTIC_SCHOLAR
from alma.discovery import openalex_related
from alma.discovery.semantic_scholar import S2_SPECTER2_MODEL
from alma.discovery import similarity as sim_module
from alma.discovery import source_search
from alma.discovery.defaults import DISCOVERY_SETTINGS_DEFAULTS, merge_discovery_defaults
from alma.discovery.scoring import (
    compute_preference_profile,
    score_candidate,
    load_settings as load_scoring_settings,
)
from alma.openalex.client import (
    _upsert_referenced_works,
    batch_fetch_referenced_works_for_openalex_ids,
    batch_fetch_works_by_openalex_ids,
)
from alma.core.utils import normalize_doi
from . import library as library_app
from .feed import _commit_if_pending


def _safe_div(numerator: float, denominator: float) -> float:
    """Safe division that returns 0.0 when denominator <= 0."""
    if denominator <= 0:
        return 0.0
    return numerator / denominator


def _planner_clamp(value: float, lo: float, hi: float) -> float:
    """Bound `value` into [lo, hi]. Used by the deterministic branch planner."""
    return max(lo, min(hi, value))


def _planner_sanitize_queries(values: list[Any], max_items: int) -> list[str]:
    """Deduplicate, normalise, length-clip a list of candidate query strings.

    Used by `_plan_branch_queries_deterministic` to scrub the queries it
    stitches together from branch topics + seed titles. Strips internal
    whitespace, drops duplicates (case-insensitive), enforces a 6..180
    char window, and caps the result at `max_items`.
    """
    seen: set[str] = set()
    out: list[str] = []
    for value in values:
        if not isinstance(value, str):
            continue
        q = " ".join(value.replace("\n", " ").split()).strip()
        if len(q) < 6:
            continue
        if len(q) > 180:
            q = q[:180].strip()
        key = q.lower()
        if key in seen:
            continue
        seen.add(key)
        out.append(q)
        if len(out) >= max_items:
            break
    return out


def _plan_branch_queries_deterministic(
    branch: dict,
    *,
    temperature: float,
    max_core: int,
    max_explore: int,
) -> dict[str, Any]:
    """Stitch a small set of branch search queries from topics + seed titles.

    Lifted out of the (now-deleted) LLM-backed `discovery_query_planner`
    so Discovery branch retrieval has a single, deterministic, zero-LLM
    path. Builds:
      - `core_queries`: anchored on the branch's `core_topics`, optionally
        combined with the strongest seed title. Bounded to `max_core`.
      - `explore_queries`: blends `explore_topics` with the lead core
        topic to nudge retrieval slightly outside the cluster centre.
        Bounded to `max_explore`. Temperature picks a soft modifier
        (`benchmarks` for low temp, `applications` for higher).
    """
    max_core = max(1, min(4, int(max_core)))
    max_explore = max(1, min(4, int(max_explore)))
    temperature = _planner_clamp(float(temperature), 0.0, 1.0)

    core_topics = [str(x).strip() for x in (branch.get("core_topics") or []) if str(x).strip()]
    explore_topics = [str(x).strip() for x in (branch.get("explore_topics") or []) if str(x).strip()]
    seed_context = branch.get("seed_context") or []
    seed_titles: list[str] = []
    if isinstance(seed_context, list):
        for item in seed_context[:3]:
            if not isinstance(item, dict):
                continue
            title = str(item.get("title") or "").strip()
            if title:
                seed_titles.append(title)

    core: list[str] = []
    if core_topics:
        core.append(" ".join(core_topics[:3]))
    if seed_titles and core_topics:
        core.append(f"{seed_titles[0]} {core_topics[0]}")
    elif seed_titles:
        core.append(seed_titles[0])
    core = _planner_sanitize_queries(core, max_core)
    if not core and core_topics:
        core = _planner_sanitize_queries([" ".join(core_topics[:2])], max_core)

    explore: list[str] = []
    if explore_topics:
        explore.append(" ".join(explore_topics[:3]))
    if core_topics and explore_topics:
        explore.append(f"{core_topics[0]} {explore_topics[0]} methods")
    if seed_titles and explore_topics:
        explore.append(f"{seed_titles[0]} {explore_topics[0]}")
    if not explore and core_topics:
        soft = "applications" if temperature >= 0.4 else "benchmarks"
        explore.append(f"{core_topics[0]} {soft}")

    return {
        "core_queries": _planner_sanitize_queries(core, max_core),
        "explore_queries": _planner_sanitize_queries(explore, max_explore),
    }

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Scoring cache — library-derived artifacts that are stable between refreshes
# ---------------------------------------------------------------------------

def _library_fingerprint(positive_ids: list[str], negative_ids: list[str]) -> str:
    """Compute a stable hash of the library state used for scoring."""
    payload = "|".join(sorted(positive_ids)) + "||" + "|".join(sorted(negative_ids))
    return hashlib.sha256(payload.encode()).hexdigest()[:16]


def _cache_get(db: sqlite3.Connection, cache_key: str, fingerprint: str) -> Optional[dict]:
    """Load a cached artifact if the fingerprint matches."""
    try:
        row = db.execute(
            "SELECT value_json, value_blob, fingerprint FROM scoring_cache WHERE cache_key = ?",
            (cache_key,),
        ).fetchone()
        if row and str(row["fingerprint"]) == fingerprint:
            return {"json": row["value_json"], "blob": row["value_blob"]}
    except Exception:
        pass
    return None


def _cache_put(db: sqlite3.Connection, cache_key: str, cache_type: str,
               fingerprint: str, *, value_json: Optional[str] = None,
               value_blob: Optional[bytes] = None) -> None:
    """Store a cached artifact."""
    try:
        db.execute(
            """INSERT INTO scoring_cache (cache_key, cache_type, fingerprint, value_json, value_blob, created_at)
               VALUES (?, ?, ?, ?, ?, datetime('now'))
               ON CONFLICT(cache_key) DO UPDATE SET
                   cache_type = excluded.cache_type,
                   fingerprint = excluded.fingerprint,
                   value_json = excluded.value_json,
                   value_blob = excluded.value_blob,
                   created_at = excluded.created_at""",
            (cache_key, cache_type, fingerprint, value_json, value_blob),
        )
    except Exception as exc:
        logger.debug("Cache write failed for %s: %s", cache_key, exc)

try:
    import numpy as np

    _NUMPY_AVAILABLE = True
except Exception:
    np = None  # type: ignore[assignment]
    _NUMPY_AVAILABLE = False

VALID_CONTEXT_TYPES = {"library_global", "collection", "topic_keyword", "tag"}

DEFAULT_CHANNEL_WEIGHTS: dict[str, dict[str, float]] = {
    "library_global": {"lexical": 0.30, "vector": 0.35, "graph": 0.20, "external": 0.15},
    "collection": {"lexical": 0.25, "vector": 0.40, "graph": 0.20, "external": 0.15},
    "topic_keyword": {"lexical": 0.45, "vector": 0.25, "graph": 0.10, "external": 0.20},
    "tag": {"lexical": 0.35, "vector": 0.30, "graph": 0.15, "external": 0.20},
}

VALID_RECOMMENDATION_ACTIONS = {"save", "like", "dismiss", "dislike", "seen"}
RECOMMENDATION_PROVENANCE_COLUMNS: dict[str, str] = {
    "source_type": "TEXT",
    "source_api": "TEXT",
    "source_key": "TEXT",
    "branch_id": "TEXT",
    "branch_label": "TEXT",
    "branch_mode": "TEXT",
}
DEFAULT_BRANCH_CONTROLS: dict[str, Any] = {
    "temperature": None,
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


def _table_columns(db: sqlite3.Connection, table: str) -> set[str]:
    try:
        rows = db.execute(f"PRAGMA table_info({table})").fetchall()
    except sqlite3.OperationalError:
        return set()
    return {str(r[1]) for r in rows}


def _chunked(items: list[str], size: int) -> list[list[str]]:
    return [items[idx:idx + size] for idx in range(0, len(items), size)]


def _ensure_recommendation_provenance_columns(db: sqlite3.Connection) -> None:
    columns = _table_columns(db, "recommendations")
    for column, ddl in RECOMMENDATION_PROVENANCE_COLUMNS.items():
        if column in columns:
            continue
        try:
            db.execute(f"ALTER TABLE recommendations ADD COLUMN {column} {ddl}")
        except Exception:
            continue


def _derive_recommendation_provenance(candidate: dict, lens_id: str) -> dict[str, Any]:
    branch_mode = str(candidate.get("branch_mode") or "").strip() or None
    branch_id = str(candidate.get("branch_id") or "").strip() or None
    branch_label = str(candidate.get("branch_label") or "").strip() or None
    source_api = str(candidate.get("source_api") or "").strip() or None
    source_type = str(candidate.get("source_type") or "").strip() or None
    if not source_type:
        if branch_mode == "followed_author":
            source_type = "followed_author"
        elif branch_id or branch_label:
            source_type = "branch"
        elif source_api:
            source_type = "external_search"
        else:
            source_type = "lens_retrieval"
    source_key = str(candidate.get("source_key") or "").strip() or None
    if not source_key:
        source_key = branch_id or branch_mode or source_type or lens_id
    return {
        "source_type": source_type,
        "source_api": source_api,
        "source_key": source_key,
        "branch_id": branch_id,
        "branch_label": branch_label,
        "branch_mode": branch_mode,
    }


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
    query.append(
        "AND r.user_action IS NULL AND p.status NOT IN ('library', 'dismissed', 'removed') "
        "AND COALESCE(p.canonical_paper_id, '') = '' "
        "ORDER BY r.score DESC, COALESCE(p.publication_date, printf('%04d-01-01', COALESCE(p.year, 0))) DESC, r.created_at DESC LIMIT ? OFFSET ?"
    )
    params.extend([limit, offset])
    rows = db.execute(" ".join(query), params).fetchall()
    return [_normalize_recommendation(dict(r)) for r in rows]


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
    added_from = None
    if action == "save":
        effective_rating = max(current_rating, int(rating or 3), 3)
        added_from = "discovery_save"
        if paper_id:
            library_app.add_to_library(
                db,
                paper_id,
                rating=effective_rating,
                added_from=added_from,
            )
    elif action == "like":
        effective_rating = max(current_rating, int(rating or 4), 4)
        added_from = "discovery_like"
        if paper_id:
            library_app.add_to_library(
                db,
                paper_id,
                rating=effective_rating,
                added_from=added_from,
            )
    elif action == "dismiss":
        effective_rating = 1
        if paper_id:
            library_app.dismiss_paper(db, paper_id)
    elif action == "dislike":
        # D6: dislike on Discovery writes a negative signal but does
        # NOT hide the paper system-wide (`dismiss_paper` flips
        # `papers.status`; `dislike` deliberately leaves it alone).
        # The recommendation row is still stamped `user_action='dislike'`
        # so the active Discovery list drops it on the next poll —
        # soft-negative at the card level, signal-only at the paper
        # level.
        effective_rating = 2

    now = datetime.utcnow().isoformat()
    db.execute(
        "UPDATE recommendations SET user_action = ?, action_at = ? WHERE id = ?",
        (action, now, rec_id),
    )
    if paper_id and action in {"save", "like", "dismiss", "dislike"}:
        library_app.sync_surface_resolution(
            db,
            paper_id,
            action=action,
            source_surface="discovery",
        )
        library_app.record_paper_feedback(
            db,
            paper_id,
            action=action,
            rating=effective_rating,
            source_surface="discovery",
        )
    lens_id = row["lens_id"] if isinstance(row, sqlite3.Row) else None
    if lens_id and paper_id:
        signal_value = library_app.rating_signal_value(effective_rating)
        record_lens_signal(
            db,
            lens_id=str(lens_id),
            paper_id=paper_id,
            signal_value=signal_value,
            source="recommendation_action",
        )
    return {
        "id": rec_id,
        action: True,
        "paper_id": paper_id,
        "action_at": now,
        "rating": effective_rating,
    }


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
# dismisses recorded in `recommendations`).
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
    """
    weighted_total = weighted_positive + weighted_dismissed
    if weighted_total <= 0.0 or raw_total <= 0:
        return 1.0, "no signal yet"

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
        elif action == "dismiss":
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
        ORDER BY COALESCE(r.rank, 999999) ASC, r.score DESC, r.created_at DESC
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


def _upsert_s2_specter2_embedding(
    db: sqlite3.Connection,
    paper_id: str,
    candidate: dict,
) -> bool:
    vector = candidate.get("specter2_embedding")
    if not isinstance(vector, list) or not vector:
        return False
    try:
        values = [float(value) for value in vector]
        blob = struct.pack(f"<{len(values)}f", *values)
    except (TypeError, ValueError, struct.error):
        return False
    cursor = db.execute(
        """
        INSERT OR IGNORE INTO publication_embeddings (paper_id, embedding, model, source, created_at)
        VALUES (?, ?, ?, ?, ?)
        """,
        (
            paper_id,
            blob,
            S2_SPECTER2_MODEL,
            EMBEDDING_SOURCE_SEMANTIC_SCHOLAR,
            datetime.utcnow().isoformat(),
        ),
    )
    return cursor.rowcount > 0


def refresh_lens_recommendations(
    db: sqlite3.Connection,
    lens_id: str,
    *,
    trigger_source: str = "user",
    limit: int = 50,
    ctx=None,
) -> Optional[dict]:
    """Generate per-lens recommendations using 4 retrieval channels."""
    overall_start = perf_counter()
    phase_started = overall_start
    timings_ms: dict[str, int] = {}

    def _log(step: str, message: str, **kwargs):
        if ctx is not None:
            ctx.log_step(step, message, **kwargs)

    lens = get_lens(db, lens_id)
    if lens is None:
        return None

    lens_name = lens.get("name") or lens_id[:12]
    seeds = _load_seed_papers_for_lens(db, lens)
    timings_ms["seed_load"] = int(round((perf_counter() - phase_started) * 1000))
    if not seeds:
        return {
            "lens_id": lens_id,
            "context_type": lens["context_type"],
            "channels": {"lexical": 0, "vector": 0, "graph": 0, "external": 0},
            "weights": _normalize_channel_weights(lens.get("weights") or default_channel_weights(lens["context_type"])),
            "inserted": 0,
            "message": "No seed papers for lens context",
            "timings_ms": {"seed_load": timings_ms["seed_load"], "total": int(round((perf_counter() - overall_start) * 1000))},
        }

    _log("seeds", f"Lens '{lens_name}': loaded {len(seeds)} seed papers", data={"seeds": len(seeds)})

    weights = lens.get("weights") or default_channel_weights(lens["context_type"])
    channel_weights = _normalize_channel_weights(weights)

    scoring_settings = load_scoring_settings(db)
    _library_pubs, positive_pubs, negative_pubs = _load_library_preference_inputs(db)
    profile = compute_preference_profile(db, positive_pubs, negative_pubs, scoring_settings)
    phase_started = perf_counter()

    _log("retrieval", f"Lens '{lens_name}': running 4 retrieval channels (lexical, vector, graph, external)")
    lexical = _retrieve_lexical_channel(db, lens, seeds, limit=limit)
    vector = _retrieve_vector_channel(db, lens, seeds, limit=limit)
    graph, graph_summary = _retrieve_graph_channel(db, lens, seeds, limit=limit)
    external, external_summary = _retrieve_external_channel(
        db,
        lens,
        seeds,
        limit=limit,
        preference_profile=profile,
        positive_pubs=positive_pubs,
    )
    timings_ms["channel_retrieval"] = int(round((perf_counter() - phase_started) * 1000))
    _log(
        "retrieval_channels",
        f"Lens '{lens_name}': retrieval finished with {len(lexical) + len(vector) + len(graph) + len(external)} raw candidates",
        data={
            "channels": {
                "lexical": len(lexical),
                "vector": len(vector),
                "graph": len(graph),
                "external": len(external),
            },
            "graph_cache": graph_summary,
            "external_lanes": external_summary.get("external_lanes") or {},
        },
    )
    if (external_summary.get("lane_runs") or []) or graph_summary.get("fallback_sources"):
        _log(
            "retrieval_detail",
            f"Lens '{lens_name}': retrieval plan used {len(external_summary.get('lane_runs') or [])} external lane runs",
            data={
                "graph_fallback": graph_summary,
                "external_lane_runs": (external_summary.get("lane_runs") or [])[:20],
            },
        )

    _log(
        "merge",
        f"Lens '{lens_name}': merging candidates — lexical={len(lexical)}, vector={len(vector)}, graph={len(graph)}, external={len(external)}",
        data={"lexical": len(lexical), "vector": len(vector), "graph": len(graph), "external": len(external)},
    )

    merged = _merge_channel_candidates(
        channel_weights=channel_weights,
        channels={
            "lexical": lexical,
            "vector": vector,
            "graph": graph,
            "external": external,
        },
    )
    timings_ms["merge"] = int(round((perf_counter() - phase_started) * 1000))
    _log(
        "merge_result",
        f"Lens '{lens_name}': merged into {len(merged)} unique candidates",
        data={
            "unique_candidates": len(merged),
            "channel_weights": channel_weights,
        },
    )

    cached_embeddings_available = sim_module.has_active_embeddings(db)

    _log(
        "scoring",
        f"Lens '{lens_name}': scoring {len(merged)} candidates with 10-signal hybrid ranker",
        data={
            "candidate_count": len(merged),
            "signals": 10,
            "positive_library_examples": len(positive_pubs),
            "negative_library_examples": len(negative_pubs),
            "cached_embeddings_available": cached_embeddings_available,
            "embeddings_available": cached_embeddings_available,
        },
    )

    # --- Apply 10-signal scoring to replace simple channel-weighted scores ---
    # Library fingerprint: used to cache artifacts that only change when library changes
    import numpy as np
    positive_ids = [str(p.get("id") or "") for p in positive_pubs if p.get("id")]
    negative_ids = [str(p.get("id") or "") for p in negative_pubs if p.get("id")]
    active_embedding_model = sim_module.get_active_embedding_model(db)
    lib_fp = f"{active_embedding_model}:{_library_fingerprint(positive_ids, negative_ids)}"

    # Compute/cache embedding centroids for text similarity
    phase_started = perf_counter()
    positive_centroid = None
    negative_centroid = None
    positive_texts = [sim_module.build_similarity_text(p, conn=db) for p in positive_pubs]
    positive_texts = [t for t in positive_texts if t]
    negative_texts = [sim_module.build_similarity_text(p, conn=db) for p in negative_pubs]
    negative_texts = [t for t in negative_texts if t]
    lexical_profile = sim_module.build_lexical_profile(positive_texts, negative_texts) if positive_texts else None
    timings_ms["lexical_profile"] = int(round((perf_counter() - phase_started) * 1000))

    phase_started = perf_counter()
    positive_example_embeddings = []
    negative_example_embeddings = []
    centroid_cache_hit = False
    if cached_embeddings_available and positive_pubs:
        # Try loading cached centroids
        cached_pos = _cache_get(db, "positive_centroid", lib_fp)
        if cached_pos and cached_pos["blob"]:
            try:
                positive_centroid = np.frombuffer(cached_pos["blob"], dtype=np.float32).copy()
                centroid_cache_hit = True
            except Exception:
                positive_centroid = None
        if positive_centroid is None:
            try:
                positive_centroid = sim_module.compute_embedding_centroid(positive_pubs, db)
                if positive_centroid is not None:
                    _cache_put(db, "positive_centroid", "centroid", lib_fp,
                               value_blob=positive_centroid.astype(np.float32).tobytes())
            except Exception as exc:
                logger.warning("Failed to compute positive centroid for lens scoring: %s", exc)

        cached_neg = _cache_get(db, "negative_centroid", lib_fp)
        if cached_neg and cached_neg["blob"]:
            try:
                negative_centroid = np.frombuffer(cached_neg["blob"], dtype=np.float32).copy()
            except Exception:
                negative_centroid = None
        if negative_centroid is None and negative_pubs:
            try:
                negative_centroid = sim_module.compute_embedding_centroid(negative_pubs, db)
                if negative_centroid is not None:
                    _cache_put(db, "negative_centroid", "centroid", lib_fp,
                               value_blob=negative_centroid.astype(np.float32).tobytes())
            except Exception as exc:
                logger.debug("Failed to compute negative centroid: %s", exc)

        # Exemplar embeddings (cached)
        cached_pos_ex = _cache_get(db, "positive_exemplars", lib_fp)
        if cached_pos_ex and cached_pos_ex["blob"]:
            try:
                raw = np.frombuffer(cached_pos_ex["blob"], dtype=np.float32)
                dim = positive_centroid.shape[0] if positive_centroid is not None else 384
                positive_example_embeddings = [row.copy() for row in raw.reshape(-1, dim)]
            except Exception:
                positive_example_embeddings = []
        if not positive_example_embeddings:
            try:
                positive_example_embeddings = sim_module.load_publication_example_embeddings(positive_pubs, db, limit=12)
                if positive_example_embeddings:
                    blob = np.stack(positive_example_embeddings).astype(np.float32).tobytes()
                    _cache_put(db, "positive_exemplars", "exemplars", lib_fp, value_blob=blob)
            except Exception as exc:
                logger.debug("Failed to load positive exemplar embeddings: %s", exc)

        if negative_pubs:
            cached_neg_ex = _cache_get(db, "negative_exemplars", lib_fp)
            if cached_neg_ex and cached_neg_ex["blob"]:
                try:
                    raw = np.frombuffer(cached_neg_ex["blob"], dtype=np.float32)
                    dim = positive_centroid.shape[0] if positive_centroid is not None else 384
                    negative_example_embeddings = [row.copy() for row in raw.reshape(-1, dim)]
                except Exception:
                    negative_example_embeddings = []
            if not negative_example_embeddings:
                try:
                    negative_example_embeddings = sim_module.load_publication_example_embeddings(negative_pubs, db, limit=8)
                    if negative_example_embeddings:
                        blob = np.stack(negative_example_embeddings).astype(np.float32).tobytes()
                        _cache_put(db, "negative_exemplars", "exemplars", lib_fp, value_blob=blob)
                except Exception as exc:
                    logger.debug("Failed to load negative exemplar embeddings: %s", exc)
    timings_ms["centroids"] = int(round((perf_counter() - phase_started) * 1000))

    phase_started = perf_counter()
    candidate_text_map: dict[str, str] = {}
    for key, candidate in merged.items():
        try:
            candidate_text = sim_module.build_similarity_text(
                candidate,
                conn=db,
                paper_topics=candidate.get("topics") or None,
            )
        except Exception:
            candidate_text = ""
        if candidate_text.strip():
            candidate_text_map[key] = candidate_text
    timings_ms["candidate_texts"] = int(round((perf_counter() - phase_started) * 1000))

    phase_started = perf_counter()
    candidate_embedding_map: dict[str, Any] = {}
    reused_embedding_count = 0
    if cached_embeddings_available and candidate_text_map:
        # First: load existing embeddings from DB for candidates that already have paper IDs
        candidate_paper_ids: dict[str, str] = {}  # key -> paper_id
        for key, candidate in merged.items():
            pid = str(candidate.get("paper_id") or candidate.get("id") or "").strip()
            if pid and key in candidate_text_map:
                candidate_paper_ids[key] = pid
        if candidate_paper_ids:
            pid_to_key = {pid: key for key, pid in candidate_paper_ids.items()}
            for chunk in _chunked(list(pid_to_key.keys()), 200):
                placeholders = ", ".join("?" for _ in chunk)
                rows = db.execute(
                    f"SELECT paper_id, embedding FROM publication_embeddings "
                    f"WHERE model = ? AND paper_id IN ({placeholders})",
                    [active_embedding_model, *chunk],
                ).fetchall()
                for row in rows:
                    key = pid_to_key.get(str(row["paper_id"]))
                    if key and row["embedding"]:
                        try:
                            candidate_embedding_map[key] = np.frombuffer(row["embedding"], dtype=np.float32).copy()
                            reused_embedding_count += 1
                        except Exception:
                            pass
    timings_ms["candidate_embedding_batch"] = int(round((perf_counter() - phase_started) * 1000))
    _log(
        "scoring_inputs",
        f"Lens '{lens_name}': prepared scoring inputs ({len(positive_texts)} positive docs, {len(negative_texts)} negative docs)",
        data={
            "positive_texts": len(positive_texts),
            "negative_texts": len(negative_texts),
            "positive_centroid_ready": positive_centroid is not None,
            "negative_centroid_ready": negative_centroid is not None,
            "positive_examples_ready": len(positive_example_embeddings),
            "negative_examples_ready": len(negative_example_embeddings),
            "lexical_profile_ready": lexical_profile is not None,
            "candidate_texts": len(candidate_text_map),
            "candidate_embeddings_ready": len(candidate_embedding_map),
            "candidate_embeddings_reused": reused_embedding_count,
            "candidate_embeddings_computed": 0,
            "centroid_cache_hit": centroid_cache_hit,
            "library_fingerprint": lib_fp,
            "centroid_prep_ms": timings_ms["centroids"],
            "lexical_profile_ms": timings_ms["lexical_profile"],
            "candidate_text_ms": timings_ms["candidate_texts"],
            "candidate_embedding_batch_ms": timings_ms["candidate_embedding_batch"],
            "cached_embeddings_available": cached_embeddings_available,
            "embeddings_available": cached_embeddings_available,
        },
    )

    # Batch-compute lexical similarity for all candidates at once
    # (single matrix transform + cosine instead of per-candidate calls)
    phase_started = perf_counter()
    precomputed_lexical_map: dict[str, dict] = {}
    if lexical_profile is not None and candidate_text_map:
        try:
            precomputed_lexical_map = sim_module.batch_compute_lexical_similarity(
                candidate_text_map, lexical_profile,
            )
        except Exception as exc:
            logger.warning("Batch lexical similarity failed, falling back to per-candidate: %s", exc)
    timings_ms["batch_lexical"] = int(round((perf_counter() - phase_started) * 1000))

    # D-AUDIT-10 (2026-04-24): pre-embed every user-topic term ONCE per
    # refresh. Inside `compute_topic_overlap`, the semantic fallback
    # previously re-embedded every user_topic for every candidate
    # (nested loop: O(candidates × unmatched_paper_topics ×
    # user_topics)), even though the module-level LRU cache absorbed
    # repeated calls. That nested call graph was the prime suspect for
    # the 27-min / 31-rec baseline: with e.g. 500 candidates × 5
    # unmatched topics × 50 user topics = 125 000 `_get_topic_embedding`
    # lookups per refresh, the per-call overhead dominates even with a
    # warm cache. Hoisting the `user_topic_embeddings` dict one level
    # up collapses that to `O(user_topics)` provider calls + a cheap
    # dict lookup inside the hot loop. Returns `None` when no
    # embedding provider is configured — the semantic fallback bails
    # out via its existing `provider is None` guard.
    phase_started = perf_counter()
    user_topic_embeddings: Optional[dict[str, Any]] = None
    user_topic_weights = profile.get("topic_weights") or {}
    if user_topic_weights:
        try:
            from alma.ai.providers import get_active_provider
            _topic_provider = get_active_provider(db)
        except Exception:
            _topic_provider = None
        if _topic_provider is not None:
            user_topic_embeddings = {}
            for ut in user_topic_weights:
                try:
                    user_topic_embeddings[ut] = sim_module._get_topic_embedding(
                        _topic_provider, ut,
                    )
                except Exception:
                    user_topic_embeddings[ut] = None
    timings_ms["user_topic_embeddings"] = int(round((perf_counter() - phase_started) * 1000))

    # D-AUDIT-10 follow-up (2026-04-24): batch-embed every candidate
    # topic term ONCE up front so `_get_topic_embedding` inside the
    # scoring loop hits the module cache every time. Before this, the
    # semantic fallback inside `compute_topic_overlap` called
    # `provider.embed([term])` one term at a time for every unmatched
    # paper topic — at 500 candidates × ~5 unmatched topics each that's
    # ~2500 sequential provider round-trips, which even a local
    # SPECTER2 model takes tens of seconds to satisfy. Doing one big
    # `provider.embed(all_terms)` call warms the cache in ~O(1) network
    # round-trip, after which the per-term lookup is a dict hit.
    phase_started = perf_counter()
    if user_topic_embeddings is not None and _topic_provider is not None:
        candidate_topic_terms: set[str] = set()
        for candidate in merged.values():
            for t in (candidate.get("topics") or []):
                term = (t.get("term") or "").strip().lower()
                if term and term not in sim_module._topic_embedding_cache:
                    candidate_topic_terms.add(term)
        if candidate_topic_terms:
            # Bound the batch size so the embedding provider's own
            # request budget isn't exceeded on huge refreshes. 256 is
            # a safe default for OpenAI / SPECTER2; bump later if we
            # see throughput headroom.
            terms = sorted(candidate_topic_terms)
            for chunk_start in range(0, len(terms), 256):
                chunk = terms[chunk_start:chunk_start + 256]
                try:
                    embeddings = _topic_provider.embed(chunk)
                except Exception:
                    embeddings = []
                if not embeddings:
                    # Provider refused — mark the chunk as "attempted"
                    # so we don't retry inside the hot loop.
                    for term in chunk:
                        sim_module._topic_embedding_cache[term] = None
                    continue
                import numpy as np
                for term, vec in zip(chunk, embeddings):
                    if vec:
                        try:
                            sim_module._topic_embedding_cache[term] = np.array(
                                vec, dtype=np.float32,
                            )
                        except Exception:
                            sim_module._topic_embedding_cache[term] = None
                    else:
                        sim_module._topic_embedding_cache[term] = None
    timings_ms["candidate_topic_embeddings"] = int(round((perf_counter() - phase_started) * 1000))

    # D-AUDIT-10a (2026-04-24): preload preference_profiles + candidate
    # authors once per refresh. `get_preference_affinity_signal` inside
    # `score_candidate` otherwise makes 4 DB round trips per candidate
    # (`SUM(interaction_count)` + topic affinity lookup + per-candidate
    # `publication_authors` + author affinity lookup) — on a 500-candidate
    # refresh that's ~2 000 trips under the SQLite writer lock. Hoisting
    # to one preload + an `IN (?, ?, …)` authors batch collapses the
    # hot-loop cost to cheap dict hits.
    phase_started = perf_counter()
    from alma.services.signal_lab import (
        preload_candidate_authors as _preload_authors,
        preload_preference_profile_maps as _preload_pref,
    )
    preloaded_preference_profile = _preload_pref(db)
    if preloaded_preference_profile is not None:
        candidate_paper_id_list = [
            str(candidate.get("paper_id") or candidate.get("id") or "").strip()
            for candidate in merged.values()
        ]
        preloaded_preference_profile["authors_by_paper"] = _preload_authors(
            db, candidate_paper_id_list,
        )
    timings_ms["preference_profile_preload"] = int(round((perf_counter() - phase_started) * 1000))

    # Score each candidate with full 10-signal system
    phase_started = perf_counter()
    signal_names = (
        "source_relevance",
        "topic_score",
        "text_similarity",
        "author_affinity",
        "journal_affinity",
        "recency_boost",
        "citation_quality",
        "feedback_adj",
        "preference_affinity",
        "usefulness_boost",
    )
    signal_value_sums = {name: 0.0 for name in signal_names}
    signal_weighted_sums = {name: 0.0 for name in signal_names}
    text_mode_counts: dict[str, int] = {}
    topic_mode_counts: dict[str, int] = {}
    raw_semantic_scores: list[float] = []
    raw_semantic_exemplar_scores: list[float] = []
    raw_semantic_support_scores: list[float] = []
    raw_lexical_scores: list[float] = []
    raw_lexical_word_scores: list[float] = []
    raw_lexical_char_scores: list[float] = []
    raw_lexical_term_scores: list[float] = []
    final_scores: list[float] = []
    embedding_ready_count = 0
    compressed_similarity_count = 0
    low_similarity_count = 0
    for key, candidate in merged.items():
        # Channel score becomes source_relevance (normalized to 0-1)
        candidate["source_relevance"] = min(1.0, candidate["score"] / 100.0)
        final_score, breakdown = score_candidate(
            candidate, profile,
            positive_centroid, negative_centroid,
            positive_texts, negative_texts,
            db, scoring_settings,
            candidate_text=candidate_text_map.get(key),
            candidate_embedding=candidate_embedding_map.get(key),
            lexical_profile=lexical_profile,
            positive_example_embeddings=positive_example_embeddings,
            negative_example_embeddings=negative_example_embeddings,
            precomputed_lexical_details=precomputed_lexical_map.get(key),
            user_topic_embeddings=user_topic_embeddings,
            preloaded_preference_profile=preloaded_preference_profile,
        )
        candidate["score"] = final_score
        # Fold retrieval provenance ("why this paper surfaced") into the
        # persisted breakdown so the UI can explain more than the branch
        # label: the actual query string that found it, and the core /
        # explore topic hints that defined the branch.
        matched_query = str(candidate.get("matched_query") or "").strip()
        if matched_query:
            breakdown["matched_query"] = matched_query
        branch_core = [t for t in (candidate.get("branch_core_topics") or []) if t]
        if branch_core:
            breakdown["branch_core_topics"] = branch_core
        branch_explore = [t for t in (candidate.get("branch_explore_topics") or []) if t]
        if branch_explore:
            breakdown["branch_explore_topics"] = branch_explore

        # T4: promote the "truthful provenance" numbers into a clean
        # sub-dict the UI can consume without inspecting the full 60+
        # raw-diagnostic keys. Every number here already exists
        # somewhere in `breakdown` (raw diagnostics) or `candidate`
        # (scoring inputs) — we're just giving the frontend a single
        # canonical place to look.
        specter_cosine = float(breakdown.get("semantic_similarity_raw") or 0.0)
        lexical_similarity_raw = float(breakdown.get("lexical_similarity_raw") or 0.0)
        negative_hit_raw = float(breakdown.get("semantic_similarity_negative_raw") or 0.0)
        candidate_author_text = str(candidate.get("authors") or "").lower()
        profile_authors = [
            str(name or "").lower()
            for name in (profile.get("author_affinity") or {}).keys()
            if name
        ]
        shared_authors: list[str] = []
        if candidate_author_text and profile_authors:
            for name in profile_authors[:50]:
                if len(name) >= 4 and name in candidate_author_text:
                    shared_authors.append(name)
                    if len(shared_authors) >= 5:
                        break
        breakdown["provenance"] = {
            # Normalized 0..1 for the frontend. Legacy rows that
            # persisted 0..100 still coerce cleanly on read.
            "score_pct": round(float(final_score or 0.0) / 100.0, 4),
            "specter_cosine": round(specter_cosine, 4) if specter_cosine else None,
            "lexical_similarity": round(lexical_similarity_raw, 4) if lexical_similarity_raw else None,
            "negative_hit": round(negative_hit_raw, 4) if negative_hit_raw >= 0.35 else None,
            "shared_authors_count": len(shared_authors) if shared_authors else None,
            "shared_authors_sample": shared_authors[0] if shared_authors else None,
        }

        candidate["score_breakdown"] = breakdown
        final_scores.append(float(final_score or 0.0))
        if breakdown.get("candidate_embedding_ready"):
            embedding_ready_count += 1
        text_mode = str(breakdown.get("text_similarity_mode") or "none")
        topic_mode = str(breakdown.get("topic_match_mode") or "none")
        text_mode_counts[text_mode] = int(text_mode_counts.get(text_mode) or 0) + 1
        topic_mode_counts[topic_mode] = int(topic_mode_counts.get(topic_mode) or 0) + 1
        try:
            raw_semantic_scores.append(float(breakdown.get("semantic_similarity_raw") or 0.0))
        except (TypeError, ValueError):
            pass
        try:
            raw_semantic_exemplar_scores.append(float(breakdown.get("semantic_similarity_exemplar_raw") or 0.0))
        except (TypeError, ValueError):
            pass
        try:
            support_value = float(breakdown.get("semantic_similarity_support_raw") or 0.0)
            raw_semantic_support_scores.append(support_value)
        except (TypeError, ValueError):
            pass
        try:
            raw_lexical_scores.append(float(breakdown.get("lexical_similarity_raw") or 0.0))
        except (TypeError, ValueError):
            pass
        for target, key_name in (
            (raw_lexical_word_scores, "lexical_similarity_word_raw"),
            (raw_lexical_char_scores, "lexical_similarity_char_raw"),
            (raw_lexical_term_scores, "lexical_similarity_term_raw"),
        ):
            try:
                target.append(float(breakdown.get(key_name) or 0.0))
            except (TypeError, ValueError):
                pass
        try:
            if float((breakdown.get("text_similarity") or {}).get("value") or 0.0) < 0.24:
                low_similarity_count += 1
        except Exception:
            pass
        try:
            if float(breakdown.get("semantic_similarity_raw") or 0.0) > 0.0 and float(breakdown.get("semantic_similarity_raw") or 0.0) < 0.14:
                compressed_similarity_count += 1
        except Exception:
            pass
        for signal_name in signal_names:
            signal_detail = breakdown.get(signal_name) or {}
            if not isinstance(signal_detail, dict):
                continue
            signal_value_sums[signal_name] += float(signal_detail.get("value") or 0.0)
            signal_weighted_sums[signal_name] += float(signal_detail.get("weighted") or 0.0)
    timings_ms["scoring"] = int(round((perf_counter() - phase_started) * 1000))
    avg_signal_values = {
        name: round(signal_value_sums[name] / max(1, len(merged)), 4)
        for name in signal_names
    }
    avg_signal_weighted = {
        name: round(signal_weighted_sums[name] / max(1, len(merged)), 4)
        for name in signal_names
    }
    top_driver_names = [
        name
        for name, _value in sorted(
            avg_signal_weighted.items(),
            key=lambda item: item[1],
            reverse=True,
        )[:3]
    ]
    _log(
        "scoring_profile",
        f"Lens '{lens_name}': scoring finished in {timings_ms['scoring']}ms; average drivers were {', '.join(top_driver_names) or 'n/a'}",
        data={
            "candidate_count": len(merged),
            "scoring_ms": timings_ms["scoring"],
            "score_range": {
                "min": round(min(final_scores), 3) if final_scores else 0.0,
                "avg": round(sum(final_scores) / max(1, len(final_scores)), 3) if final_scores else 0.0,
                "max": round(max(final_scores), 3) if final_scores else 0.0,
            },
            "avg_signal_values": avg_signal_values,
            "avg_signal_weighted": avg_signal_weighted,
            "text_similarity_modes": text_mode_counts,
            "topic_match_modes": topic_mode_counts,
            "candidate_embeddings_used": embedding_ready_count,
            "raw_similarity": {
                "semantic_avg": round(sum(raw_semantic_scores) / max(1, len(raw_semantic_scores)), 4) if raw_semantic_scores else 0.0,
                "semantic_exemplar_avg": round(sum(raw_semantic_exemplar_scores) / max(1, len(raw_semantic_exemplar_scores)), 4) if raw_semantic_exemplar_scores else 0.0,
                "semantic_support_avg": round(sum(raw_semantic_support_scores) / max(1, len(raw_semantic_support_scores)), 4) if raw_semantic_support_scores else 0.0,
                "lexical_avg": round(sum(raw_lexical_scores) / max(1, len(raw_lexical_scores)), 4) if raw_lexical_scores else 0.0,
                "lexical_word_avg": round(sum(raw_lexical_word_scores) / max(1, len(raw_lexical_word_scores)), 4) if raw_lexical_word_scores else 0.0,
                "lexical_char_avg": round(sum(raw_lexical_char_scores) / max(1, len(raw_lexical_char_scores)), 4) if raw_lexical_char_scores else 0.0,
                "lexical_term_avg": round(sum(raw_lexical_term_scores) / max(1, len(raw_lexical_term_scores)), 4) if raw_lexical_term_scores else 0.0,
                "compressed_rate": round(compressed_similarity_count / max(1, len(merged)), 3) if merged else 0.0,
                "low_text_similarity_rate": round(low_similarity_count / max(1, len(merged)), 3) if merged else 0.0,
            },
        },
    )

    ranked = sorted(merged.values(), key=lambda x: x["score"], reverse=True)[: max(1, limit)]
    _log(
        "scoring_result",
        f"Lens '{lens_name}': ranked top {len(ranked)} candidates after scoring",
        data={
            "ranked": len(ranked),
            "top_candidates": [
                {
                    "title": str(item.get("title") or "")[:120],
                    "score": round(float(item.get("score") or 0.0), 3),
                    "source_type": item.get("source_type"),
                    "branch_label": item.get("branch_label"),
                }
                for item in ranked[:5]
            ],
        },
    )

    suggestion_set_id = uuid.uuid4().hex
    now = datetime.utcnow().isoformat()
    external_lane_counts: dict[str, int] = {}
    for item in external:
        lane = str(item.get("source_type") or item.get("branch_mode") or "external").strip() or "external"
        external_lane_counts[lane] = external_lane_counts.get(lane, 0) + 1
    graph_lane_counts: dict[str, int] = {}
    for item in graph:
        lane = str(item.get("source_type") or "graph").strip() or "graph"
        graph_lane_counts[lane] = graph_lane_counts.get(lane, 0) + 1
    retrieval_summary = {
        "seed_count": len(seeds),
        "recommendation_mode": external_summary.get("recommendation_mode", "balanced"),
        "temperature": external_summary.get("temperature"),
        "channels": {
            "lexical": len(lexical),
            "vector": len(vector),
            "graph": len(graph),
            "external": len(external),
        },
        "graph_lanes": graph_lane_counts,
        "graph_cache": graph_summary,
        "external_lanes": external_lane_counts,
        "weights": channel_weights,
        "taste_profile": external_summary.get("taste_profile") or {},
        "negative_profile": external_summary.get("negative_profile") or {},
        "budgets": external_summary.get("budgets") or {},
        "lane_runs": external_summary.get("lane_runs") or [],
    }
    cold_start_summary = _build_topic_keyword_cold_start_summary(
        lens,
        seed_count=len(seeds),
        lexical_count=len(lexical),
        graph_count=len(graph),
        external_lane_counts=external_lane_counts,
    )
    if cold_start_summary is not None:
        retrieval_summary["cold_start"] = cold_start_summary
        _log(
            "cold_start",
            f"Lens '{lens_name}': topic cold-start state is {cold_start_summary['state']}",
            data=cold_start_summary,
        )

    _ensure_recommendation_provenance_columns(db)
    # NOTE: old recommendations are deleted atomically with the insert below,
    # NOT here — so a crash during scoring doesn't wipe existing recommendations.

    _log("insert", f"Lens '{lens_name}': staging top {len(ranked)} recommendations")

    phase_started = perf_counter()
    staged_candidates: list[tuple[int, dict, str]] = []
    staged_paper_ids: list[str] = []
    for idx, candidate in enumerate(ranked, start=1):
        paper_id = library_app.upsert_paper(
            db,
            title=candidate["title"],
            authors=candidate.get("authors"),
            abstract=candidate.get("abstract"),
            year=candidate.get("year"),
            journal=candidate.get("journal"),
            url=candidate.get("url"),
            doi=candidate.get("doi"),
            openalex_id=candidate.get("openalex_id"),
            semantic_scholar_id=candidate.get("semantic_scholar_id"),
            semantic_scholar_corpus_id=candidate.get("semantic_scholar_corpus_id"),
            cited_by_count=int(candidate.get("cited_by_count") or 0),
            # T5 — persist S2 TLDR + influential citation count so Library +
            # PaperCard + citation_quality scoring can use them without
            # re-fetching. Falsy values are skipped by `upsert_paper` so
            # existing rows don't get their TLDRs clobbered by later
            # non-S2 lanes.
            tldr=(candidate.get("tldr") or None),
            influential_citation_count=(
                int(candidate["influential_citation_count"])
                if candidate.get("influential_citation_count") is not None
                else None
            ),
            status="tracked",
            added_from="discovery",
        )
        _upsert_s2_specter2_embedding(db, paper_id, candidate)
        staged_candidates.append((idx, candidate, paper_id))
        staged_paper_ids.append(paper_id)
    timings_ms["paper_upsert"] = int(round((perf_counter() - phase_started) * 1000))
    # Commit the tracked-paper upserts independently of the rec swap below.
    # Per `lessons.md` → "Background jobs must release the writer lock ...
    # AND between phases" + "commit per unit of work": the paper rows are
    # useful on their own (Corpus, Feed, Library backfill) even if a later
    # phase fails. Keeping them in one txn with the swap means an 11-min
    # refresh that crashes at the swap discards the entire upsert phase.
    _commit_if_pending(db)

    phase_started = perf_counter()
    status_by_paper: dict[str, str] = {}
    actioned_paper_ids: set[str] = set()
    unique_paper_ids = [paper_id for paper_id in dict.fromkeys(staged_paper_ids) if str(paper_id).strip()]
    for chunk in _chunked(unique_paper_ids, 200):
        placeholders = ", ".join("?" for _ in chunk)
        status_rows = db.execute(
            f"SELECT id, status FROM papers WHERE id IN ({placeholders})",
            chunk,
        ).fetchall()
        for row in status_rows:
            status_by_paper[str(row["id"])] = str(row["status"] or "tracked")
        action_rows = db.execute(
            f"""
            SELECT DISTINCT paper_id
            FROM recommendations
            WHERE paper_id IN ({placeholders})
              AND user_action IS NOT NULL
            """,
            chunk,
        ).fetchall()
        actioned_paper_ids.update(str(row["paper_id"]) for row in action_rows if str(row["paper_id"] or "").strip())

    rec_rows: list[tuple] = []
    inserted_paper_ids: list[str] = []
    skipped_library = 0
    skipped_actioned = 0
    for idx, candidate, paper_id in staged_candidates:
        paper_status = status_by_paper.get(paper_id, "tracked")
        if paper_status in ("library", "dismissed", "removed"):
            skipped_library += 1
            continue
        if paper_id in actioned_paper_ids:
            skipped_actioned += 1
            continue
        provenance = _derive_recommendation_provenance(candidate, lens_id)
        rec_rows.append(
            (
                uuid.uuid4().hex,
                suggestion_set_id,
                lens_id,
                paper_id,
                idx,
                float(candidate["score"]),
                json.dumps(candidate.get("score_breakdown", {})),
                provenance.get("source_type"),
                provenance.get("source_api"),
                provenance.get("source_key"),
                provenance.get("branch_id"),
                provenance.get("branch_label"),
                provenance.get("branch_mode"),
                now,
            )
        )
        inserted_paper_ids.append(paper_id)
    timings_ms["filter_existing"] = int(round((perf_counter() - phase_started) * 1000))

    retrieval_summary["filters"] = {
        "ranked": len(ranked),
        "staged": len(staged_candidates),
        "skipped_library_or_sunk": skipped_library,
        "skipped_previously_actioned": skipped_actioned,
        "insertable": len(rec_rows),
    }
    _log(
        "filter_result",
        f"Lens '{lens_name}': {len(rec_rows)} recommendations remained after library/action filters",
        data=retrieval_summary["filters"],
    )
    phase_started = perf_counter()
    # Atomic swap: delete old un-actioned recommendations and insert new ones together.
    # This prevents data loss if the operation crashes during scoring above.
    db.execute("DELETE FROM recommendations WHERE lens_id = ? AND user_action IS NULL", (lens_id,))
    db.execute(
        """
        INSERT INTO suggestion_sets (
            id, lens_id, context_type, trigger_source, retrieval_summary, ranker_version, created_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        (
            suggestion_set_id,
            lens_id,
            lens["context_type"],
            trigger_source,
            json.dumps(retrieval_summary),
            "lens-v2-9signal",
            now,
        ),
    )
    if rec_rows:
        db.executemany(
            """
            INSERT INTO recommendations (
                id, suggestion_set_id, lens_id, paper_id, rank, score, score_breakdown,
                source_type, source_api, source_key, branch_id, branch_label, branch_mode,
                created_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            rec_rows,
        )
    timings_ms["recommendation_insert"] = int(round((perf_counter() - phase_started) * 1000))

    timings_ms["total"] = int(round((perf_counter() - overall_start) * 1000))
    retrieval_summary["timings_ms"] = dict(timings_ms)

    db.execute(
        "UPDATE suggestion_sets SET retrieval_summary = ? WHERE id = ?",
        (json.dumps(retrieval_summary), suggestion_set_id),
    )

    db.execute(
        "UPDATE discovery_lenses SET last_refreshed_at = ? WHERE id = ?",
        (now, lens_id),
    )
    inserted = len(rec_rows)
    _log(
        "done",
        f"Lens '{lens_name}': refresh complete with {inserted} retained recommendations",
        data={
            "inserted": inserted,
            "timings_ms": timings_ms,
            "channels": retrieval_summary["channels"],
        },
    )
    return {
        "lens_id": lens_id,
        "suggestion_set_id": suggestion_set_id,
        "context_type": lens["context_type"],
        "channels": retrieval_summary["channels"],
        "weights": channel_weights,
        "retrieval_summary": retrieval_summary,
        "inserted": inserted,
    }


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


def _load_seed_papers_for_lens(db: sqlite3.Connection, lens: dict) -> list[dict]:
    context_type = lens["context_type"]
    config = lens.get("context_config") or {}
    settings = read_settings(db)
    try:
        max_seeds = int(settings.get("lens.max_seeds", "500"))
    except (TypeError, ValueError):
        max_seeds = 500
    max_seeds = max(50, min(5000, max_seeds))
    if context_type == "library_global":
        rows = db.execute(
            """
            SELECT id, title, abstract, doi, openalex_id, authors, journal, year, cited_by_count, rating
            FROM papers
            WHERE status = 'library'
            ORDER BY COALESCE(rating, 0) DESC, COALESCE(cited_by_count, 0) DESC
            LIMIT ?
            """,
            (max_seeds,),
        ).fetchall()
        return [dict(r) for r in rows]
    if context_type == "collection":
        collection_id = str(config.get("collection_id") or "").strip()
        if not collection_id:
            return []
        rows = db.execute(
            """
            SELECT p.id, p.title, p.abstract, p.doi, p.openalex_id, p.authors, p.journal, p.year, p.cited_by_count, p.rating
            FROM papers p
            JOIN collection_items ci ON ci.paper_id = p.id
            WHERE ci.collection_id = ?
            ORDER BY COALESCE(p.rating, 0) DESC, COALESCE(p.cited_by_count, 0) DESC
            LIMIT ?
            """,
            (collection_id, max_seeds),
        ).fetchall()
        return [dict(r) for r in rows]
    if context_type == "topic_keyword":
        keyword = str(config.get("keyword") or config.get("query") or "").strip()
        if not keyword:
            return []
        pattern = f"%{keyword}%"
        rows = db.execute(
            """
            SELECT id, title, abstract, doi, openalex_id, authors, journal, year, cited_by_count, rating
            FROM papers
            WHERE title LIKE ? OR abstract LIKE ?
            ORDER BY COALESCE(cited_by_count, 0) DESC
            LIMIT ?
            """,
            (pattern, pattern, max_seeds),
        ).fetchall()
        return [dict(r) for r in rows]
    if context_type == "tag":
        tag_id = str(config.get("tag_id") or "").strip()
        tag_name = str(config.get("tag") or "").strip()
        if tag_id:
            rows = db.execute(
                """
                SELECT p.id, p.title, p.abstract, p.doi, p.openalex_id, p.authors, p.journal, p.year, p.cited_by_count, p.rating
                FROM papers p
                JOIN publication_tags pt ON pt.paper_id = p.id
                WHERE pt.tag_id = ?
                ORDER BY COALESCE(p.cited_by_count, 0) DESC
                LIMIT ?
                """,
                (tag_id, max_seeds),
            ).fetchall()
            return [dict(r) for r in rows]
        if tag_name:
            row = db.execute("SELECT id FROM tags WHERE name = ?", (tag_name,)).fetchone()
            if row:
                return _load_seed_papers_for_lens(
                    db,
                    {**lens, "context_config": {"tag_id": row["id"]}},
                )
        return []
    return []


def _extract_keywords(seeds: list[dict], explicit: Optional[list[str]] = None, max_keywords: int = 12) -> list[str]:
    stop = {
        "the",
        "and",
        "for",
        "with",
        "from",
        "using",
        "towards",
        "into",
        "between",
        "study",
        "analysis",
        "approach",
        "model",
        "data",
        "this",
        "that",
        "these",
        "those",
        "paper",
        "papers",
        "method",
        "methods",
        "result",
        "results",
        "research",
        "study",
        "studies",
        "review",
        "reviews",
        "library",
        "scientific",
    }
    counts: dict[str, int] = {}
    for seed in seeds:
        text = f"{seed.get('title', '')} {seed.get('abstract', '')}".lower()
        for token in text.replace("/", " ").replace("-", " ").split():
            t = "".join(ch for ch in token if ch.isalnum())
            if len(t) < 4 or t in stop:
                continue
            counts[t] = counts.get(t, 0) + 1
    words = [w for w, _ in sorted(counts.items(), key=lambda x: x[1], reverse=True)]
    if explicit:
        for item in explicit:
            s = (item or "").strip().lower()
            if s and s not in words:
                words.insert(0, s)
    return words[:max_keywords]


def _load_library_preference_inputs(
    db: sqlite3.Connection,
) -> tuple[list[dict], list[dict], list[dict]]:
    rows = db.execute(
        """SELECT id, title, abstract, url, doi, authors, journal, year, rating, added_at
           FROM papers
           WHERE status = 'library'
           ORDER BY COALESCE(added_at, '') DESC"""
    ).fetchall()
    library_pubs = [dict(r) for r in rows]
    positive_pubs = [
        dict(r)
        for r in rows
        if (r["rating"] or 0) >= 4 or (r["rating"] or 0) == 0
    ]
    negative_pubs = [
        dict(r)
        for r in rows
        if 1 <= int(r["rating"] or 0) <= 2
    ]
    if library_pubs and not any((r["rating"] or 0) >= 4 for r in rows):
        positive_pubs = list(library_pubs)
    return library_pubs, positive_pubs, negative_pubs


def _top_profile_terms(
    weights: dict[str, float],
    *,
    limit: int,
    min_weight: float = 0.16,
) -> list[tuple[str, float]]:
    ranked = [
        ((term or "").strip(), float(weight))
        for term, weight in weights.items()
        if (term or "").strip() and float(weight or 0.0) >= min_weight
    ]
    ranked.sort(key=lambda item: (item[1], len(item[0])), reverse=True)
    return ranked[: max(1, limit)]


def _top_preferred_authors(
    db: sqlite3.Connection,
    *,
    limit: int,
) -> list[tuple[str, float]]:
    scores: dict[str, float] = {}
    display_names: dict[str, str] = {}
    try:
        rows = db.execute(
            """
            SELECT entity_id, affinity_weight, confidence, interaction_count
            FROM preference_profiles
            WHERE entity_type = 'author'
              AND affinity_weight > 0
            ORDER BY (affinity_weight * confidence) DESC, interaction_count DESC
            LIMIT ?
            """,
            (max(limit * 3, 8),),
        ).fetchall()
        for row in rows:
            display_name = str(row["entity_id"] or "").strip()
            key = display_name.lower()
            if not key:
                continue
            score = float(row["affinity_weight"] or 0.0) * max(0.2, float(row["confidence"] or 0.0))
            current = scores.get(key, 0.0)
            if score > current:
                scores[key] = score
                display_names[key] = display_name
    except sqlite3.OperationalError:
        pass

    try:
        rows = db.execute(
            """
            SELECT a.name
            FROM followed_authors fa
            JOIN authors a ON a.id = fa.author_id
            WHERE COALESCE(TRIM(a.name), '') != ''
            LIMIT ?
            """,
            (max(limit * 2, 6),),
        ).fetchall()
        for row in rows:
            display_name = str(row["name"] or "").strip()
            key = display_name.lower()
            if not key:
                continue
            scores[key] = max(scores.get(key, 0.0), 0.7)
            display_names[key] = display_name
    except sqlite3.OperationalError:
        pass

    ranked = sorted(scores.items(), key=lambda item: item[1], reverse=True)
    return [
        (display_names.get(key, key), score)
        for key, score in ranked[: max(1, limit)]
    ]


def _recent_positive_publications(
    db: sqlite3.Connection,
    fallback: list[dict],
    *,
    limit: int,
) -> list[dict]:
    try:
        rows = db.execute(
            """
            SELECT p.id, p.title, p.abstract, p.authors, p.journal, p.year, r.action_at
            FROM recommendations r
            JOIN papers p ON p.id = r.paper_id
            WHERE r.user_action IN ('save', 'like')
            ORDER BY COALESCE(r.action_at, r.created_at, '') DESC
            LIMIT ?
            """,
            (limit,),
        ).fetchall()
        recent = [dict(r) for r in rows if str(r["title"] or "").strip()]
        if recent:
            return recent
    except sqlite3.OperationalError:
        pass
    return list(fallback[:limit])


def _build_recent_win_queries(
    db: sqlite3.Connection,
    positive_pubs: list[dict],
    *,
    limit: int,
) -> list[tuple[str, float]]:
    recent_pubs = _recent_positive_publications(db, positive_pubs, limit=max(limit * 2, 4))
    queries: list[tuple[str, float]] = []
    seen: set[str] = set()
    for idx, pub in enumerate(recent_pubs):
        terms = _extract_keywords([pub], max_keywords=5)
        query = " ".join(terms[:3]).strip()
        if not query:
            continue
        if query in seen:
            continue
        seen.add(query)
        strength = _clamp(0.92 - (idx * 0.12), 0.45, 0.92)
        queries.append((query, strength))
        if len(queries) >= limit:
            break
    return queries


def _negative_preference_context(
    db: sqlite3.Connection,
    preference_profile: Optional[dict[str, Any]],
) -> dict[str, dict[str, float]]:
    topic_weights = dict((preference_profile or {}).get("topic_weights") or {})
    journal_affinity = dict((preference_profile or {}).get("journal_affinity") or {})
    negative_topics = {
        str(term).strip().lower(): abs(float(weight))
        for term, weight in topic_weights.items()
        if str(term).strip() and float(weight or 0.0) <= -0.16
    }
    negative_journals = {
        str(journal).strip().lower(): abs(float(weight))
        for journal, weight in journal_affinity.items()
        if str(journal).strip() and float(weight or 0.0) <= -0.16
    }
    negative_authors: dict[str, float] = {}
    try:
        rows = db.execute(
            """
            SELECT entity_id, affinity_weight, confidence
            FROM preference_profiles
            WHERE entity_type = 'author'
              AND affinity_weight < 0
            ORDER BY affinity_weight ASC
            LIMIT 40
            """
        ).fetchall()
        for row in rows:
            name = str(row["entity_id"] or "").strip().lower()
            if not name:
                continue
            negative_authors[name] = abs(float(row["affinity_weight"] or 0.0)) * max(
                0.2,
                float(row["confidence"] or 0.0),
            )
    except sqlite3.OperationalError:
        pass
    return {
        "topics": negative_topics,
        "authors": negative_authors,
        "journals": negative_journals,
    }


def _top_negative_terms(
    values: dict[str, float],
    *,
    limit: int,
    field_name: str,
) -> list[dict[str, Any]]:
    ranked = [
        {field_name: key, "weight": round(float(weight), 4)}
        for key, weight in sorted(
            (
                (str(key).strip(), float(weight))
                for key, weight in values.items()
                if str(key).strip() and float(weight or 0.0) > 0.0
            ),
            key=lambda item: item[1],
            reverse=True,
        )[: max(1, limit)]
    ]
    return ranked


def _candidate_negative_preference_penalty(
    candidate: dict,
    negative_context: dict[str, dict[str, float]],
) -> float:
    if not negative_context:
        return 0.0

    penalty = 0.0
    title = str(candidate.get("title") or "").strip().lower()
    journal = str(candidate.get("journal") or "").strip().lower()
    authors_text = str(candidate.get("authors") or "").strip().lower()

    for term, weight in negative_context.get("topics", {}).items():
        if not term:
            continue
        if term in title:
            penalty += min(0.42, 0.12 + (weight * 0.28))

    for author, weight in negative_context.get("authors", {}).items():
        if author and author in authors_text:
            penalty += min(0.45, 0.18 + (weight * 0.25))

    for venue, weight in negative_context.get("journals", {}).items():
        if venue and venue == journal:
            penalty += min(0.35, 0.12 + (weight * 0.18))

    return _clamp(penalty, 0.0, 0.95)


def _clamp(value: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, value))


def _resolve_branch_temperature(
    settings: Optional[dict[str, str]] = None,
    override: Optional[float] = None,
) -> float:
    if override is not None:
        return _clamp(float(override), 0.0, 1.0)
    cfg = settings or {}
    mode = str(cfg.get("recommendation_mode", "balanced") or "balanced").strip().lower()
    default_by_mode = {
        "exploit": 0.12,
        "balanced": 0.28,
        "explore": 0.55,
    }
    fallback = default_by_mode.get(mode, 0.28)
    raw = cfg.get("branches.temperature")
    try:
        return _clamp(float(raw), 0.0, 1.0) if raw is not None else fallback
    except (TypeError, ValueError):
        return fallback


def _seed_strength(seed: dict) -> float:
    rating_raw = int(seed.get("rating") or 0)
    rating_score = 0.6 if rating_raw <= 0 else _clamp(rating_raw / 5.0, 0.0, 1.0)
    citations = float(seed.get("cited_by_count") or 0.0)
    citation_score = _clamp(citations / 200.0, 0.0, 1.0)
    year_raw = int(seed.get("year") or 0)
    current_year = datetime.utcnow().year
    recency_score = _clamp((year_raw - (current_year - 12)) / 12.0, 0.0, 1.0)
    return (rating_score * 0.6) + (citation_score * 0.25) + (recency_score * 0.15)


def _fetch_seed_embedding_vectors(
    db: sqlite3.Connection,
    seeds: list[dict],
) -> dict[str, "np.ndarray"]:
    if not _NUMPY_AVAILABLE:
        return {}
    seed_ids = [str(seed.get("id") or "").strip() for seed in seeds]
    seed_ids = [sid for sid in seed_ids if sid]
    if not seed_ids:
        return {}
    active_model = sim_module.get_active_embedding_model(db)
    placeholders = ",".join("?" for _ in seed_ids)
    try:
        rows = db.execute(
            f"""
            SELECT paper_id, embedding
            FROM publication_embeddings
            WHERE model = ? AND paper_id IN ({placeholders})
            """,
            [active_model, *seed_ids],
        ).fetchall()
    except sqlite3.OperationalError:
        return {}
    out: dict[str, "np.ndarray"] = {}
    for row in rows:
        paper_id = str(row["paper_id"] or "").strip()
        if not paper_id:
            continue
        try:
            vec = np.frombuffer(row["embedding"], dtype=np.float32).copy()
            norm = float(np.linalg.norm(vec))
            if norm <= 0.0:
                continue
            out[paper_id] = vec / norm
        except Exception:
            continue
    return out


def _cluster_seed_papers_vector(
    seeds: list[dict],
    vectors: dict[str, "np.ndarray"],
    max_clusters: int,
) -> list[dict[str, Any]]:
    seed_by_id = {str(seed.get("id") or ""): seed for seed in seeds}
    items: list[tuple[str, "np.ndarray", float]] = []
    for paper_id, vec in vectors.items():
        seed = seed_by_id.get(paper_id)
        if not seed:
            continue
        items.append((paper_id, vec, _seed_strength(seed)))

    if not items:
        return []
    if len(items) == 1:
        sid, vec, _ = items[0]
        return [{"seeds": [seed_by_id[sid]], "centroid": vec}]

    items.sort(key=lambda x: x[2], reverse=True)
    k = max(2, int(round(len(items) ** 0.5)))
    k = min(k, max(2, max_clusters), len(items))

    centers: list["np.ndarray"] = [items[0][1]]
    used_ids: set[str] = {items[0][0]}
    while len(centers) < k:
        best_idx: Optional[int] = None
        best_dist = -1.0
        for idx, (paper_id, vec, _score) in enumerate(items):
            if paper_id in used_ids:
                continue
            nearest = min(1.0 - float(np.dot(vec, c)) for c in centers)
            if nearest > best_dist:
                best_dist = nearest
                best_idx = idx
        if best_idx is None:
            break
        seed_id, seed_vec, _ = items[best_idx]
        centers.append(seed_vec)
        used_ids.add(seed_id)

    assignments: list[list[tuple[str, "np.ndarray"]]] = [[] for _ in centers]
    for _ in range(6):
        assignments = [[] for _ in centers]
        for paper_id, vec, _score in items:
            best_i = max(range(len(centers)), key=lambda i: float(np.dot(vec, centers[i])))
            assignments[best_i].append((paper_id, vec))

        new_centers: list["np.ndarray"] = []
        for i, group in enumerate(assignments):
            if not group:
                new_centers.append(centers[i])
                continue
            mat = np.vstack([v for _, v in group])
            centroid = np.mean(mat, axis=0)
            norm = float(np.linalg.norm(centroid))
            if norm <= 0.0:
                new_centers.append(centers[i])
            else:
                new_centers.append(centroid / norm)
        centers = new_centers

    clusters: list[dict[str, Any]] = []
    for i, group in enumerate(assignments):
        if not group:
            continue
        group_seeds = [seed_by_id[sid] for sid, _ in group if sid in seed_by_id]
        if not group_seeds:
            continue
        group_seeds.sort(key=_seed_strength, reverse=True)
        clusters.append({"seeds": group_seeds, "centroid": centers[i]})

    clusters.sort(
        key=lambda c: (
            sum(_seed_strength(s) for s in c["seeds"]) / max(1, len(c["seeds"])),
            len(c["seeds"]),
        ),
        reverse=True,
    )
    return clusters


def _cluster_seed_papers_lexical(
    seeds: list[dict],
    max_clusters: int,
) -> list[dict[str, Any]]:
    if not seeds:
        return []
    global_terms = _extract_keywords(seeds, max_keywords=max(16, max_clusters * 4))
    if not global_terms:
        ranked = sorted(seeds, key=_seed_strength, reverse=True)
        return [{"seeds": ranked, "centroid": None}]

    groups: dict[str, list[dict]] = defaultdict(list)
    for idx, seed in enumerate(seeds):
        paper_terms = _extract_keywords([seed], max_keywords=5)
        anchor = next((t for t in paper_terms if t in global_terms), global_terms[idx % len(global_terms)])
        groups[anchor].append(seed)

    clusters: list[dict[str, Any]] = []
    for anchor, group in groups.items():
        group_sorted = sorted(group, key=_seed_strength, reverse=True)
        clusters.append({"seeds": group_sorted, "centroid": None, "anchor": anchor})

    clusters.sort(
        key=lambda c: (
            len(c["seeds"]),
            sum(_seed_strength(s) for s in c["seeds"]) / max(1, len(c["seeds"])),
        ),
        reverse=True,
    )

    if len(clusters) > max_clusters and max_clusters > 0:
        overflow: list[dict] = []
        for cluster in clusters[max_clusters:]:
            overflow.extend(cluster["seeds"])
        clusters = clusters[:max_clusters]
        if clusters and overflow:
            clusters[-1]["seeds"].extend(overflow)
            clusters[-1]["seeds"].sort(key=_seed_strength, reverse=True)
    return clusters


def _build_seed_branches(
    db: sqlite3.Connection,
    seeds: list[dict],
    *,
    settings: Optional[dict[str, str]] = None,
    max_branches: int = 6,
    temperature: Optional[float] = None,
    lens_id: Optional[str] = None,
) -> list[dict]:
    if not seeds:
        return []
    effective_max = max(2, min(12, int(max_branches or 6)))
    effective_temp = _resolve_branch_temperature(settings, temperature)

    vectors = _fetch_seed_embedding_vectors(db, seeds)
    if len(vectors) >= 4:
        clusters = _cluster_seed_papers_vector(seeds, vectors, effective_max)
    else:
        clusters = _cluster_seed_papers_lexical(seeds, effective_max)
    if not clusters:
        return []

    global_terms = _extract_keywords(seeds, max_keywords=40)
    branches: list[dict] = []
    for i, cluster in enumerate(clusters[:effective_max], start=1):
        cluster_seeds = cluster.get("seeds") or []
        if not cluster_seeds:
            continue
        cluster_terms = _extract_keywords(cluster_seeds, max_keywords=14)
        if not cluster_terms:
            cluster_terms = global_terms

        core_count = max(2, min(4, int(round(4.0 - (effective_temp * 2.4)))))
        core_topics = cluster_terms[:core_count] if cluster_terms else []
        if not core_topics and global_terms:
            core_topics = global_terms[:2]
        if not core_topics:
            core_topics = [f"branch-{i}"]

        neighbor_terms: list[str] = []
        direction_hint: Optional[str] = None

        own_centroid = cluster.get("centroid")
        if _NUMPY_AVAILABLE and own_centroid is not None and len(clusters) > 1:
            best_idx: Optional[int] = None
            best_sim = -2.0
            for j, other in enumerate(clusters):
                if other is cluster:
                    continue
                other_centroid = other.get("centroid")
                if other_centroid is None:
                    continue
                sim = float(np.dot(own_centroid, other_centroid))
                if sim > best_sim:
                    best_sim = sim
                    best_idx = j
            if best_idx is not None:
                neighbor_terms = _extract_keywords(clusters[best_idx].get("seeds") or [], max_keywords=12)
                if neighbor_terms:
                    direction_hint = " / ".join(neighbor_terms[:2])

        if not neighbor_terms and len(clusters) > 1:
            next_cluster = clusters[(i) % len(clusters)]
            neighbor_terms = _extract_keywords(next_cluster.get("seeds") or [], max_keywords=12)
            if neighbor_terms:
                direction_hint = " / ".join(neighbor_terms[:2])

        explore_count = max(1, min(5, int(round(1.0 + (effective_temp * 4.0)))))
        explore_pool: list[str] = []
        for term in [*neighbor_terms, *global_terms]:
            if term in core_topics or term in explore_pool:
                continue
            explore_pool.append(term)
        explore_topics = explore_pool[:explore_count]

        branch_score = sum(_seed_strength(seed) for seed in cluster_seeds) / max(1, len(cluster_seeds))
        label = " / ".join(core_topics[:2]) if core_topics else f"Branch {i}"

        sample_papers: list[dict] = []
        ranked_cluster = sorted(cluster_seeds, key=_seed_strength, reverse=True)
        for seed in ranked_cluster[:3]:
            sample_papers.append(
                {
                    "paper_id": seed.get("id"),
                    "title": seed.get("title") or "Untitled",
                    "year": seed.get("year"),
                    "rating": int(seed.get("rating") or 0),
                }
            )
        seed_context: list[dict] = []
        for seed in ranked_cluster[:6]:
            abstract = (seed.get("abstract") or "").strip()
            if len(abstract) > 1400:
                abstract = abstract[:1400].rstrip() + "..."
            seed_context.append(
                {
                    "paper_id": seed.get("id"),
                    "title": seed.get("title") or "Untitled",
                    "abstract": abstract,
                    "year": seed.get("year"),
                    "rating": int(seed.get("rating") or 0),
                    "cited_by_count": int(seed.get("cited_by_count") or 0),
                }
            )

        # Derive the branch identity from the sorted set of cluster seed
        # paper IDs (scoped by lens_id). This is stable across preview /
        # refresh cycles: labels can drift, but the cluster's seed set is
        # the cluster's identity. See `_make_branch_id` for the full D-AUDIT-5
        # rationale.
        cluster_seed_ids = [str(seed.get("id") or "") for seed in cluster_seeds]
        branches.append(
            {
                "id": _make_branch_id(lens_id, cluster_seed_ids, core_topics),
                "label": label,
                "seed_count": len(cluster_seeds),
                "branch_score": round(branch_score, 4),
                "core_topics": core_topics,
                "explore_topics": explore_topics,
                "direction_hint": direction_hint,
                "sample_papers": sample_papers,
                "seed_context": seed_context,
            }
        )

    branches.sort(key=lambda b: (float(b.get("branch_score") or 0.0), int(b.get("seed_count") or 0)), reverse=True)
    return branches[:effective_max]


def preview_lens_branches(
    db: sqlite3.Connection,
    lens_id: str,
    *,
    max_branches: int = 6,
    temperature: Optional[float] = None,
) -> Optional[dict]:
    """Build an explainable branch map for one lens (for UI visualization).

    Each branch carries its `auto_weight` — the continuous multiplier the
    refresh allocator will apply to its retrieval budget based on past
    save/dismiss outcomes. Manual pin/boost/mute remain available as hard
    overrides; with no override, `auto_weight` is what shapes allocation.
    """
    lens = get_lens(db, lens_id)
    if lens is None:
        return None
    seeds = _load_seed_papers_for_lens(db, lens)
    settings = read_settings(db)
    controls = _resolve_lens_branch_controls(lens)
    effective_temp = _resolve_branch_temperature(
        settings,
        temperature if temperature is not None else controls.get("temperature"),
    )
    branches = _build_seed_branches(
        db,
        seeds,
        settings=settings,
        max_branches=max_branches,
        temperature=effective_temp,
        lens_id=lens_id,
    )
    branches = _apply_branch_controls(branches, controls)
    branch_outcomes = _load_branch_outcome_map(db, lens_id=lens_id, days=60)
    enriched_branches = _enrich_branches_with_outcomes(branches, branch_outcomes)
    return {
        "lens_id": lens_id,
        "lens_name": lens.get("name"),
        "context_type": lens.get("context_type"),
        "seed_count": len(seeds),
        "temperature": round(effective_temp, 3),
        "generated_at": datetime.utcnow().isoformat(),
        "branches": enriched_branches,
    }


def _retrieve_lexical_channel(
    db: sqlite3.Connection,
    lens: dict,
    seeds: list[dict],
    *,
    limit: int,
) -> list[dict]:
    config = lens.get("context_config") or {}
    explicit_topics = config.get("topics") if isinstance(config.get("topics"), list) else None
    if lens["context_type"] == "topic_keyword":
        keyword = str(config.get("keyword") or config.get("query") or "").strip()
        explicit_topics = [keyword] if keyword else []
    topics = _extract_keywords(seeds, explicit=explicit_topics, max_keywords=10)
    if not topics:
        return []
    return openalex_related.search_works_by_topics(topics, limit=limit, from_year=datetime.utcnow().year - 3)


def _retrieve_vector_channel(
    db: sqlite3.Connection,
    lens: dict,
    seeds: list[dict],
    *,
    limit: int,
) -> list[dict]:
    if not _NUMPY_AVAILABLE:
        return []

    seed_ids = [str(seed.get("id") or "").strip() for seed in seeds]
    seed_ids = [sid for sid in seed_ids if sid]
    if not seed_ids:
        return []

    active_model = sim_module.get_active_embedding_model(db)
    placeholders = ",".join("?" for _ in seed_ids)
    seed_rows = db.execute(
        f"""
        SELECT paper_id, embedding
        FROM publication_embeddings
        WHERE model = ? AND paper_id IN ({placeholders})
        """,
        [active_model, *seed_ids],
    ).fetchall()
    if not seed_rows:
        return []

    seed_vecs: list["np.ndarray"] = []
    for row in seed_rows:
        try:
            vec = np.frombuffer(row["embedding"], dtype=np.float32).copy()
            norm = float(np.linalg.norm(vec))
            if norm <= 0.0:
                continue
            seed_vecs.append(vec / norm)
        except Exception:
            continue
    if not seed_vecs:
        return []

    centroid = np.mean(np.vstack(seed_vecs), axis=0)
    centroid_norm = float(np.linalg.norm(centroid))
    if centroid_norm <= 0.0:
        return []
    centroid = centroid / centroid_norm

    rows = db.execute(
        """
        SELECT pe.paper_id, pe.embedding, p.title, p.authors, p.url, p.doi, p.year, p.journal, p.cited_by_count
        FROM publication_embeddings pe
        JOIN papers p ON p.id = pe.paper_id
        WHERE pe.model = ? AND p.status NOT IN ('dismissed', 'removed')
        """,
        [active_model],
    ).fetchall()

    seed_set = set(seed_ids)
    scored: list[tuple[float, dict]] = []
    max_scan = max(limit * 20, 400)
    scanned = 0
    for row in rows:
        if scanned >= max_scan:
            break
        paper_id = str(row["paper_id"] or "").strip()
        if not paper_id or paper_id in seed_set:
            continue
        scanned += 1
        try:
            vec = np.frombuffer(row["embedding"], dtype=np.float32).copy()
            if vec.shape != centroid.shape:
                continue
            norm = float(np.linalg.norm(vec))
            if norm <= 0.0:
                continue
            vec = vec / norm
            sim = float(np.dot(centroid, vec))
            score = max(0.0, (sim + 1.0) / 2.0)
            if score <= 0.0:
                continue
            scored.append(
                (
                    score,
                    {
                        "title": row["title"] or "",
                        "authors": row["authors"] or "",
                        "url": row["url"] or "",
                        "doi": row["doi"] or "",
                        "score": score,
                        "year": row["year"],
                        "journal": row["journal"] or "",
                        "cited_by_count": row["cited_by_count"] or 0,
                    },
                )
            )
        except Exception:
            continue

    scored.sort(key=lambda x: x[0], reverse=True)
    return [item for _, item in scored[: max(1, limit)]]


def _retrieve_graph_channel(
    db: sqlite3.Connection,
    lens: dict,
    seeds: list[dict],
    *,
    limit: int,
) -> tuple[list[dict], dict[str, Any]]:
    def _seed_graph_identifier(seed: dict) -> str:
        openalex_id = str(seed.get("openalex_id") or "").strip()
        if openalex_id:
            return openalex_id
        return str(seed.get("doi") or "").strip()

    graph_summary: dict[str, Any] = {
        "seed_total": len(seeds),
        "seed_local_reference_ready": 0,
        "seed_reference_backfilled": 0,
        "local_reference_candidates": 0,
        "fallback_candidates": 0,
        "fallback_used": False,
        "semantic_related_candidates": 0,
        "fallback_sources": [],
    }

    def _backfill_local_references() -> None:
        seed_rows = [
            (
                str(seed.get("id") or "").strip(),
                str(seed.get("openalex_id") or "").strip(),
            )
            for seed in seeds
            if str(seed.get("id") or "").strip()
        ]
        if not seed_rows:
            return
        seed_ids = [paper_id for paper_id, _openalex_id in seed_rows]
        placeholders = ", ".join("?" for _ in seed_ids)
        ref_counts: dict[str, int] = {}
        try:
            rows = db.execute(
                f"""
                SELECT paper_id, COUNT(*) AS ref_count
                FROM publication_references
                WHERE paper_id IN ({placeholders})
                GROUP BY paper_id
                """,
                seed_ids,
            ).fetchall()
            ref_counts = {str(row["paper_id"]): int(row["ref_count"] or 0) for row in rows}
        except sqlite3.OperationalError:
            ref_counts = {}

        graph_summary["seed_local_reference_ready"] = sum(
            1 for paper_id, _openalex_id in seed_rows if int(ref_counts.get(paper_id) or 0) > 0
        )
        missing_pairs = [
            (paper_id, openalex_id)
            for paper_id, openalex_id in seed_rows
            if openalex_id and int(ref_counts.get(paper_id) or 0) <= 0
        ]
        if not missing_pairs:
            return
        try:
            reference_map = batch_fetch_referenced_works_for_openalex_ids(
                [openalex_id for _paper_id, openalex_id in missing_pairs],
                batch_size=25,
                max_workers=4,
            )
        except Exception:
            return

        backfilled = 0
        for paper_id, openalex_id in missing_pairs:
            referenced_ids = reference_map.get(openalex_id) or []
            if not referenced_ids:
                continue
            backfilled += _upsert_referenced_works(db, paper_id, referenced_ids)
        if backfilled > 0:
            graph_summary["seed_reference_backfilled"] = backfilled
            graph_summary["seed_local_reference_ready"] = min(
                len(seed_rows),
                int(graph_summary["seed_local_reference_ready"] or 0)
                + sum(1 for _paper_id, openalex_id in missing_pairs if reference_map.get(openalex_id)),
            )

    def _local_reference_candidates() -> list[dict]:
        seed_ids = [str(seed["id"]) for seed in seeds if seed.get("id")]
        if not seed_ids:
            return []
        placeholders = ", ".join("?" for _ in seed_ids)
        try:
            rows = db.execute(
                f"""
                SELECT DISTINCT referenced_work_id
                FROM publication_references
                WHERE paper_id IN ({placeholders})
                LIMIT ?
                """,
                [*seed_ids, limit],
            ).fetchall()
        except sqlite3.OperationalError:
            return []
        work_ids = [str(r["referenced_work_id"]) for r in rows if r["referenced_work_id"]]
        if not work_ids:
            return []
        works = batch_fetch_works_by_openalex_ids(work_ids, batch_size=50, max_workers=4)
        out: list[dict] = []
        for idx, work_id in enumerate(work_ids):
            work = works.get(work_id)
            if not work:
                continue
            title = (work.get("display_name") or "").strip()
            if not title:
                continue
            authorships = work.get("authorships") or []
            authors = ", ".join((a.get("author") or {}).get("display_name", "") for a in authorships)
            primary_loc = work.get("primary_location") or {}
            source = primary_loc.get("source") or {}
            out.append(
                {
                    "openalex_id": work_id,
                    "title": title,
                    "authors": authors,
                    "url": primary_loc.get("landing_page_url") or primary_loc.get("pdf_url") or work.get("id") or "",
                    "doi": work.get("doi") or "",
                    "score": max(0.1, 1.0 - (idx / max(1, len(work_ids)))),
                    "year": work.get("publication_year"),
                    "journal": source.get("display_name") if isinstance(source, dict) else "",
                    "cited_by_count": work.get("cited_by_count") or 0,
                    "source_type": "graph_reference",
                    "source_api": "openalex",
                    "source_key": "local_references",
                }
            )
            if len(out) >= limit:
                break
        graph_summary["local_reference_candidates"] = len(out)
        return out

    _backfill_local_references()
    local_candidates = _local_reference_candidates()
    if len(local_candidates) >= limit:
        return local_candidates[:limit], graph_summary

    merged: dict[str, dict] = {}
    for item in local_candidates:
        merged[_candidate_key(item)] = item

    fallback_budget = max(limit, 8)
    identifiers = [
        identifier
        for identifier in (_seed_graph_identifier(seed) for seed in seeds[:10])
        if identifier
    ]
    seed_dois = [
        str(seed.get("doi") or "").strip()
        for seed in seeds[:10]
        if str(seed.get("doi") or "").strip()
    ]
    # Parallelize the 3-call OA fallback fan-out across all seed identifiers.
    # Pre-refactor this was up to 30 sequential OpenAlex HTTP calls; bounded
    # pool keeps peak concurrent requests at max_workers=6.
    if identifiers:
        graph_summary["fallback_used"] = True
        graph_summary["fallback_sources"] = sorted(set([*graph_summary.get("fallback_sources", []), "openalex"]))
        relation_calls = (
            ("graph_reference", openalex_related.fetch_referenced_works, 0.72),
            ("graph_citing", openalex_related.fetch_citing_works, 0.58),
            ("graph_related", openalex_related.fetch_related_works, 0.44),
        )
        call_keys: list[tuple[str, str, float]] = [
            (identifier, relation, weight)
            for identifier in identifiers
            for relation, _fn, weight in relation_calls
        ]
        fn_map = {relation: fn for relation, fn, _ in relation_calls}
        with ThreadPoolExecutor(max_workers=min(6, max(1, len(call_keys))), thread_name_prefix="graph-oa") as gpool:
            future_map = {
                gpool.submit(fn_map[rel], identifier, 6): (identifier, rel, weight)
                for identifier, rel, weight in call_keys
            }
            for fut in as_completed(future_map):
                identifier, rel, weight = future_map[fut]
                if len(merged) >= fallback_budget:
                    continue
                try:
                    items = fut.result() or []
                except Exception as exc:
                    logger.debug("graph OA fallback (%s) failed for %s: %s", rel, identifier, exc)
                    items = []
                for idx, item in enumerate(items):
                    candidate = dict(item)
                    candidate["source_type"] = rel
                    candidate["source_api"] = str(candidate.get("source_api") or "openalex")
                    candidate["source_key"] = identifier
                    base = float(candidate.get("score", 0.25) or 0.25)
                    rank_factor = _clamp(1.0 - (idx / max(1, len(items) * 1.6)), 0.12, 1.0)
                    candidate["score"] = round(_clamp((base * weight) + (rank_factor * (1.0 - weight)), 0.05, 1.0), 4)
                    key = _candidate_key(candidate)
                    existing = merged.get(key)
                    if existing is None or float(candidate.get("score") or 0.0) > float(existing.get("score") or 0.0):
                        merged[key] = candidate
                    if len(merged) >= fallback_budget:
                        break

    if len(merged) < fallback_budget and seed_dois:
        from alma.discovery import semantic_scholar

        graph_summary["fallback_used"] = True
        graph_summary["fallback_sources"] = sorted(set([*graph_summary.get("fallback_sources", []), "semantic_scholar"]))
        with ThreadPoolExecutor(max_workers=min(4, max(1, len(seed_dois))), thread_name_prefix="graph-s2") as s2pool:
            future_map = {s2pool.submit(semantic_scholar.fetch_related_papers, doi, 6): doi for doi in seed_dois}
            for fut in as_completed(future_map):
                doi = future_map[fut]
                if len(merged) >= fallback_budget:
                    continue
                try:
                    items = fut.result() or []
                except Exception as exc:
                    logger.debug("graph S2 related fetch failed for %s: %s", doi, exc)
                    items = []
                graph_summary["semantic_related_candidates"] = int(graph_summary.get("semantic_related_candidates") or 0) + len(items)
                for idx, item in enumerate(items):
                    candidate = dict(item)
                    candidate["source_type"] = "graph_semantic_related"
                    candidate["source_api"] = "semantic_scholar"
                    candidate["source_key"] = doi
                    base = float(candidate.get("score", 0.25) or 0.25)
                    rank_factor = _clamp(1.0 - (idx / max(1, len(items) * 1.5)), 0.12, 1.0)
                    candidate["score"] = round(_clamp((base * 0.52) + (rank_factor * 0.48), 0.05, 1.0), 4)
                    key = _candidate_key(candidate)
                    existing = merged.get(key)
                    if existing is None or float(candidate.get("score") or 0.0) > float(existing.get("score") or 0.0):
                        merged[key] = candidate

    ranked = sorted(merged.values(), key=lambda item: float(item.get("score", 0.0) or 0.0), reverse=True)
    graph_summary["fallback_candidates"] = max(0, len(ranked) - len(local_candidates))
    return ranked[:limit], graph_summary


def _retrieve_external_channel(
    db: sqlite3.Connection,
    lens: dict,
    seeds: list[dict],
    *,
    limit: int,
    preference_profile: Optional[dict[str, Any]] = None,
    positive_pubs: Optional[list[dict]] = None,
) -> tuple[list[dict], dict[str, Any]]:
    def _setting_bool(key: str, default: bool) -> bool:
        raw = settings.get(key)
        if raw is None:
            return default
        return str(raw).strip().lower() in {"1", "true", "yes", "on"}

    def _setting_int(key: str, default: int, lo: int, hi: int) -> int:
        raw = settings.get(key)
        if raw is None:
            return default
        try:
            value = int(raw)
        except (TypeError, ValueError):
            return default
        return max(lo, min(hi, value))

    out: list[dict] = []
    settings = read_settings(db)
    branch_enabled = _setting_bool("strategies.branch_explorer", True)
    topic_search_enabled = _setting_bool("strategies.topic_search", True)
    taste_topics_enabled = _setting_bool("strategies.taste_topics", True)
    taste_authors_enabled = _setting_bool("strategies.taste_authors", True)
    taste_venues_enabled = _setting_bool("strategies.taste_venues", True)
    recent_wins_enabled = _setting_bool("strategies.recent_wins", True)
    recommendation_mode = str(settings.get("recommendation_mode", "balanced") or "balanced").strip().lower()
    branch_controls = _resolve_lens_branch_controls(lens)
    # The old discrete pin/boost/mute "smart suggestion" panel was replaced by
    # continuous per-branch auto_weight (see _compute_branch_auto_weight),
    # which is applied directly inside the branch budget allocator below.
    # Manual branch_controls remain the only user-driven overrides.
    effective_branch_controls = branch_controls
    temperature = _resolve_branch_temperature(settings, branch_controls.get("temperature"))
    current_year = datetime.utcnow().year
    profile = preference_profile or {}
    preferred_topics = _top_profile_terms(
        dict(profile.get("topic_weights") or {}),
        limit=_setting_int("limits.taste_topic_queries", 3, 1, 6),
    )
    preferred_authors = _top_preferred_authors(
        db,
        limit=_setting_int("limits.taste_author_queries", 3, 1, 6),
    )
    preferred_venues = _top_profile_terms(
        dict(profile.get("journal_affinity") or {}),
        limit=_setting_int("limits.taste_venue_queries", 2, 1, 4),
        min_weight=0.14,
    )
    recent_win_queries = _build_recent_win_queries(
        db,
        list(positive_pubs or []),
        limit=_setting_int("limits.recent_win_queries", 2, 1, 4),
    )
    negative_context = _negative_preference_context(db, profile)

    # Cache repeated source searches within one refresh run.  Each cache entry
    # is a `Future` returned by the shared lane executor so that every
    # submission is dispatched in parallel; consumers block with `.result()`
    # at the point of use.  `_lane_timings` records per-call wall-clock and
    # `_lane_diagnostics` captures per-source timing + timeouts so that
    # `lane_runs` can surface a `duration_ms` and `slowest_source` value
    # even though each future was fired concurrently (D-AUDIT-10b / -10c).
    query_cache: dict[tuple[str, str, int, int], "Future[list[dict]]"] = {}
    _lane_timings: dict[tuple, int] = {}
    _lane_diagnostics: dict[tuple, dict[str, Any]] = {}
    lane_runs: list[dict[str, Any]] = []

    def _lane_diag_fields(cache_key: tuple) -> dict[str, Any]:
        """Pull the slowest-source + timeout info from diagnostics for a lane_run."""
        diag = _lane_diagnostics.get(cache_key) or {}
        fields: dict[str, Any] = {}
        slowest = diag.get("slowest_source")
        if isinstance(slowest, dict):
            fields["slowest_source"] = slowest.get("source")
            fields["slowest_source_ms"] = slowest.get("duration_ms")
        per_source = diag.get("per_source_ms")
        if per_source:
            fields["per_source_ms"] = dict(per_source)
        timed_out = diag.get("timed_out_sources") or []
        if timed_out:
            fields["timed_out_sources"] = list(timed_out)
        return fields

    # Keep the pool small enough that each lane's internal 5-source fan-out
    # (ThreadPoolExecutor inside `search_across_sources`) doesn't balloon total
    # concurrent HTTP connections.  6 lane workers × 5 sources = 30 peak.
    lane_executor = ThreadPoolExecutor(max_workers=6, thread_name_prefix="lens-lane")

    def _submit_source_search(
        cache_key: tuple,
        query: str,
        per_query: int,
        from_year: int,
        *,
        mode: str,
    ) -> "Future[list[dict]]":
        """Submit a `search_across_sources` call to the lane executor.

        Stores the Future in `query_cache` keyed by `cache_key` so repeat
        requests in the same refresh piggyback on the same in-flight call.
        Wraps the fn with a timer that records wall-clock duration into
        `_lane_timings` so `lane_runs` can carry `duration_ms` without a
        second timing pass.  Per-source diagnostics (which of the 5
        sources was slowest, which timed out) feed into
        `_lane_diagnostics` so `lane_runs` expose them for profiling.
        """
        existing = query_cache.get(cache_key)
        if existing is not None:
            return existing

        diagnostics: dict[str, Any] = {}

        def _timed_call() -> list[dict]:
            started = perf_counter()
            try:
                return source_search.search_across_sources(
                    query,
                    limit=per_query,
                    from_year=from_year,
                    settings=settings,
                    mode=mode,
                    temperature=temperature,
                    semantic_scholar_mode="bulk",
                    diagnostics=diagnostics,
                )
            finally:
                _lane_timings[cache_key] = int(round((perf_counter() - started) * 1000))
                _lane_diagnostics[cache_key] = diagnostics

        future = lane_executor.submit(_timed_call)
        query_cache[cache_key] = future
        return future

    def _resolve_lane(cache_entry: "Future[list[dict]] | list[dict]") -> list[dict]:
        if isinstance(cache_entry, Future):
            try:
                return cache_entry.result() or []
            except Exception as exc:
                logger.warning("lens lane source search failed: %s", exc)
                return []
        return cache_entry or []

    # Branch exploration: cluster seed papers, use AI to craft search
    # queries from seed titles+abstracts, then search across all sources.
    #
    # Two-pass structure so every lane submission is queued on the shared
    # lane_executor before any `.result()` blocks.  Pass 1 builds one
    # `branch_plan` per branch (with LLM query plans precomputed) and
    # submits every source search.  Pass 2 iterates the plans and consumes
    # results from the cache — the executor has been working on them
    # concurrently in the meantime.
    branch_plans: list[dict[str, Any]] = []
    if branch_enabled and seeds:
        max_branches = _setting_int("branches.max_clusters", 6, 2, 12)
        max_active = _setting_int("branches.max_active_for_retrieval", 4, 1, 12)
        core_variants = _setting_int("branches.query_core_variants", 2, 1, 4)
        explore_variants = _setting_int("branches.query_explore_variants", 2, 1, 4)

        branches = _build_seed_branches(
            db,
            seeds,
            settings=settings,
            max_branches=max_branches,
            temperature=temperature,
            lens_id=str(lens.get("id") or "") or None,
        )
        branches = _apply_branch_controls(branches, effective_branch_controls)
        # Enrich each branch with its outcome history so the budget allocator
        # can read `auto_weight` (continuous multiplier derived from past
        # save/dismiss patterns) alongside the manual pin/boost/mute flags.
        branch_outcome_map = _load_branch_outcome_map(
            db,
            lens_id=str(lens.get("id") or "").strip() or None,
            days=60,
        )
        branches = _enrich_branches_with_outcomes(branches, branch_outcome_map)
        active_branches = [branch for branch in branches if branch.get("is_active")]
        if active_branches:
            prioritized = active_branches[:max_active]
            branch_budget_weights = []
            for branch in prioritized:
                # auto_weight is the continuous multiplier from past outcomes
                # (range AUTO_WEIGHT_FLOOR..AUTO_WEIGHT_CEIL, neutral 1.0 when
                # signal is thin). Pin/Boost are hard floors that prevent the
                # auto-weight from starving a branch the user explicitly
                # endorsed; without an override, auto_weight rules.
                weight = float(branch.get("auto_weight") or 1.0)
                if branch.get("is_pinned"):
                    weight = max(weight, 1.65)
                elif branch.get("is_boosted"):
                    weight = max(weight, 1.3)
                branch_budget_weights.append(weight)
            core_ratio = _clamp(0.82 - (0.36 * temperature), 0.42, 0.82)
            total_branch_weight = sum(branch_budget_weights) or 1.0
            for branch, branch_weight in zip(prioritized, branch_budget_weights):
                branch_id = str(branch.get("id") or "")
                branch_label = str(branch.get("label") or branch_id)
                core_topics = list(branch.get("core_topics") or [])
                explore_topics = list(branch.get("explore_topics") or [])
                per_branch = max(4, int(round((limit * branch_weight) / total_branch_weight)))
                branch_score_bonus = 0.0
                if branch.get("is_pinned"):
                    branch_score_bonus = 0.1
                elif branch.get("is_boosted"):
                    branch_score_bonus = 0.05

                # Branch query planning is deterministic — stitches queries
                # from core / explore topics + seed titles. The LLM-backed
                # planner was removed in 2026-04 (see tasks/01_LLM_PRODUCTION_EXIT.md);
                # the deterministic stitcher is now the only path.
                query_plan = _plan_branch_queries_deterministic(
                    branch,
                    temperature=temperature,
                    max_core=core_variants,
                    max_explore=explore_variants,
                )

                core_queries = [q for q in (query_plan.get("core_queries") or []) if str(q).strip()][:core_variants]
                explore_queries = [q for q in (query_plan.get("explore_queries") or []) if str(q).strip()][:explore_variants]
                if not core_queries and core_topics:
                    core_queries = [" ".join(core_topics[:3])]
                if not explore_queries and explore_topics:
                    explore_queries = [" ".join(explore_topics[:3])]

                core_limit_total = max(1, int(round(per_branch * core_ratio)))
                explore_limit_total = max(0, per_branch - core_limit_total)

                core_per_query = (
                    max(2, core_limit_total // max(1, len(core_queries)))
                    if core_queries and core_limit_total > 0
                    else 0
                )
                explore_per_query = (
                    max(1, explore_limit_total // max(1, len(explore_queries)))
                    if explore_queries and explore_limit_total > 0
                    else 0
                )
                from_year_core = current_year - (2 if temperature <= 0.35 else 4)
                from_year_explore = current_year - (3 if temperature <= 0.35 else 6)

                # Queue all submissions for this branch onto the lane executor.
                # Submissions are non-blocking, so queuing proceeds to the next
                # branch immediately.
                if core_per_query > 0:
                    for query in core_queries:
                        _submit_source_search(
                            ("core", query, core_per_query, from_year_core),
                            query,
                            core_per_query,
                            from_year_core,
                            mode="core",
                        )
                if explore_per_query > 0:
                    for query in explore_queries:
                        _submit_source_search(
                            ("explore", query, explore_per_query, from_year_explore),
                            query,
                            explore_per_query,
                            from_year_explore,
                            mode="explore",
                        )

                branch_plans.append(
                    {
                        "branch_id": branch_id,
                        "branch_label": branch_label,
                        "core_topics": core_topics,
                        "explore_topics": explore_topics,
                        "core_queries": core_queries,
                        "explore_queries": explore_queries,
                        "core_per_query": core_per_query,
                        "explore_per_query": explore_per_query,
                        "from_year_core": from_year_core,
                        "from_year_explore": from_year_explore,
                        "branch_score_bonus": branch_score_bonus,
                    }
                )

    taste_budget_factor = {
        "explore": 0.30,
        "balanced": 0.34 if temperature <= 0.35 else 0.28,
        "exploit": 0.38,
    }.get(recommendation_mode, 0.34 if temperature <= 0.35 else 0.28)
    taste_budget_total = max(6, int(round(limit * taste_budget_factor)))
    topic_hint = preferred_topics[0][0] if preferred_topics else ""
    lane_specs: list[dict[str, Any]] = []
    explicit_topic_keyword = ""

    if topic_search_enabled and lens["context_type"] == "topic_keyword":
        config = lens.get("context_config") or {}
        explicit_topic_keyword = str(config.get("keyword") or config.get("query") or "").strip()
        if explicit_topic_keyword:
            lane_specs.append(
                {
                    "lane_type": "cold_start_topic",
                    "query": explicit_topic_keyword,
                    "source_key": explicit_topic_keyword,
                    "strength": 0.76 if not seeds else 0.58,
                    "budget": max(4, int(round(limit * (0.40 if not seeds else 0.18)))),
                    "from_year": current_year - 5,
                    "mode": "core" if not seeds else "explore",
                }
            )
        elif not seeds:
            lane_specs.append(
                {
                    "lane_type": "cold_start_topic",
                    "query": str(lens.get("name") or "").strip(),
                    "source_key": str(lens.get("name") or "").strip(),
                    "strength": 0.62,
                    "budget": max(4, int(round(limit * 0.34))),
                    "from_year": current_year - 5,
                    "mode": "core",
                }
            )

    if taste_topics_enabled:
        topic_budget = max(2, int(round(taste_budget_total * 0.34)))
        for topic, strength in preferred_topics:
            lane_specs.append(
                {
                    "lane_type": "taste_topic",
                    "query": topic,
                    "source_key": topic,
                    "strength": _clamp(0.58 + (float(strength) * 0.35), 0.45, 1.0),
                    "budget": topic_budget,
                    "from_year": current_year - 4,
                    "mode": "core",
                }
            )

    if taste_authors_enabled:
        author_budget = max(2, int(round(taste_budget_total * 0.28)))
        for author, strength in preferred_authors:
            author_query = author if not topic_hint else f"{author} {topic_hint}"
            lane_specs.append(
                {
                    "lane_type": "taste_author",
                    "query": author_query,
                    "source_key": author,
                    "strength": _clamp(0.54 + (float(strength) * 0.32), 0.42, 1.0),
                    "budget": author_budget,
                    "from_year": current_year - 4,
                    "mode": "core",
                }
            )

    if taste_venues_enabled:
        venue_budget = max(1, int(round(taste_budget_total * 0.16)))
        for venue, strength in preferred_venues:
            venue_query = venue if not topic_hint else f"{venue} {topic_hint}"
            lane_specs.append(
                {
                    "lane_type": "taste_venue",
                    "query": venue_query,
                    "source_key": venue,
                    "strength": _clamp(0.5 + (float(strength) * 0.3), 0.38, 0.94),
                    "budget": venue_budget,
                    "from_year": current_year - 5,
                    "mode": "core",
                }
            )

    if recent_wins_enabled:
        recent_budget = max(2, int(round(taste_budget_total * 0.22)))
        for query, strength in recent_win_queries:
            lane_specs.append(
                {
                    "lane_type": "recent_win",
                    "query": query,
                    "source_key": query,
                    "strength": float(strength),
                    "budget": recent_budget,
                    "from_year": current_year - 3,
                    "mode": "explore" if temperature >= 0.4 else "core",
                }
            )

    # --- Prefetch: submit every remaining source-search + followed-author
    # --- fetch + S2 recommend to the lane executor BEFORE any consumer blocks
    # --- on a future.  This unlocks cross-section parallelism (branch queries,
    # --- taste lanes, followed-author fetches, and S2 recommend all share the
    # --- same pool of workers and run concurrently).
    for lane in lane_specs:
        query = str(lane.get("query") or "").strip()
        if not query:
            continue
        per_query = max(1, int(lane.get("budget") or 1))
        from_year = int(lane.get("from_year") or current_year - 4)
        mode = str(lane.get("mode") or "core")
        cache_key = (str(lane.get("lane_type") or "taste"), query, per_query, from_year)
        _submit_source_search(cache_key, query, per_query, from_year, mode=mode)

    # S2 recommend lane — resolve seeds from DB, then submit the network call
    # as a Future so it overlaps with the other lanes.  The consume block
    # later reads the result and emits the lane_run + `out` entries.
    s2_recommend_enabled = _setting_bool("strategies.s2_recommend", True)
    s2_source_enabled = _setting_bool("sources.semantic_scholar.enabled", True)
    s2_recommend_future: "Future[list[dict]] | None" = None
    s2_recommend_budget = 0
    s2_positive_seed_ids: list[str] = []
    s2_negative_seed_ids: list[str] = []
    s2_holder: dict[str, Any] = {}
    if s2_recommend_enabled and s2_source_enabled:
        from alma.discovery import semantic_scholar as _s2_lane

        pos_rows = db.execute(
            """
            SELECT semantic_scholar_id, semantic_scholar_corpus_id, doi
            FROM papers
            WHERE status = 'library' AND COALESCE(rating, 0) >= 4
              AND (
                COALESCE(NULLIF(TRIM(semantic_scholar_id), ''), '') != ''
                OR COALESCE(NULLIF(TRIM(doi), ''), '') != ''
                OR COALESCE(NULLIF(TRIM(semantic_scholar_corpus_id), ''), '') != ''
              )
            ORDER BY COALESCE(rating, 0) DESC, COALESCE(added_at, '') DESC
            LIMIT 50
            """
        ).fetchall()
        neg_rows = db.execute(
            """
            SELECT semantic_scholar_id, semantic_scholar_corpus_id, doi
            FROM papers
            WHERE (
                status IN ('removed', 'dismissed')
                OR COALESCE(rating, 0) BETWEEN 1 AND 2
              )
              AND (
                COALESCE(NULLIF(TRIM(semantic_scholar_id), ''), '') != ''
                OR COALESCE(NULLIF(TRIM(doi), ''), '') != ''
                OR COALESCE(NULLIF(TRIM(semantic_scholar_corpus_id), ''), '') != ''
              )
            LIMIT 100
            """
        ).fetchall()

        def _row_to_s2_seed_id(row) -> str:
            s2 = str(row["semantic_scholar_id"] or "").strip()
            if s2:
                return s2
            doi = (row["doi"] or "").strip()
            if doi:
                return f"DOI:{doi}"
            corpus = str(row["semantic_scholar_corpus_id"] or "").strip()
            if corpus:
                return f"CorpusID:{corpus}"
            return ""

        s2_positive_seed_ids = [seed for row in pos_rows if (seed := _row_to_s2_seed_id(row))]
        s2_negative_seed_ids = [seed for row in neg_rows if (seed := _row_to_s2_seed_id(row))]
        s2_recommend_budget = max(6, int(round(limit * 0.18)))
        if s2_positive_seed_ids:

            def _s2_recommend_call(
                pos: list[str] = s2_positive_seed_ids,
                neg: list[str] = s2_negative_seed_ids,
                budget: int = s2_recommend_budget,
                holder: dict[str, Any] = s2_holder,
            ) -> list[dict]:
                started = perf_counter()
                try:
                    return _s2_lane.recommend_from_seeds(pos, neg, limit=budget) or []
                except Exception as exc:
                    holder["error"] = str(exc)
                    return []
                finally:
                    holder["ms"] = int(round((perf_counter() - started) * 1000))

            s2_recommend_future = lane_executor.submit(_s2_recommend_call)

    # Followed-author OpenAlex fetches — submit each in parallel, consume
    # once branches and taste lanes have been drained.
    followed_rows = db.execute(
        """
        SELECT a.openalex_id
        FROM followed_authors fa
        JOIN authors a ON a.id = fa.author_id
        WHERE a.openalex_id IS NOT NULL AND TRIM(a.openalex_id) != ''
        LIMIT 10
        """
    ).fetchall()
    year_floor = current_year - 2
    follow_budget_base = {
        "explore": 0.14,
        "balanced": 0.20 + (0.25 * temperature),
        "exploit": 0.28 + (0.18 * temperature),
    }.get(recommendation_mode, 0.20 + (0.25 * temperature))
    follow_budget = max(3, int(round(limit * follow_budget_base)))
    follow_author_limit = min(6, follow_budget)
    followed_author_futures: list[tuple[str, "Future[list[dict]]", list]] = []
    for row in followed_rows:
        openalex_id = (row["openalex_id"] or "").strip()
        if not openalex_id:
            continue
        holder: dict[str, int] = {}

        def _timed_author_fetch(
            aid: str = openalex_id,
            from_year: int = year_floor,
            per_fetch: int = follow_author_limit,
            holder: dict[str, int] = holder,
        ) -> list[dict]:
            started = perf_counter()
            try:
                return openalex_related.fetch_recent_works_for_author(
                    aid,
                    from_year=from_year,
                    limit=per_fetch,
                )
            finally:
                holder["ms"] = int(round((perf_counter() - started) * 1000))

        future = lane_executor.submit(_timed_author_fetch)
        followed_author_futures.append((openalex_id, future, holder))

    # --- Consume pass: branch plans first (each resolve blocks only on the
    # --- slowest single future because the rest are already mid-flight).
    for plan in branch_plans:
        branch_id = plan["branch_id"]
        branch_label = plan["branch_label"]
        core_topics = plan["core_topics"]
        explore_topics = plan["explore_topics"]
        core_queries = plan["core_queries"]
        explore_queries = plan["explore_queries"]
        core_per_query = plan["core_per_query"]
        explore_per_query = plan["explore_per_query"]
        from_year_core = plan["from_year_core"]
        from_year_explore = plan["from_year_explore"]
        branch_score_bonus = plan["branch_score_bonus"]

        if core_per_query > 0:
            for query in core_queries:
                cache_key = ("core", query, core_per_query, from_year_core)
                core_results = _resolve_lane(query_cache[cache_key])
                lane_runs.append(
                    {
                        "lane_type": "branch_core",
                        "branch_id": branch_id,
                        "branch_label": branch_label,
                        "query": query,
                        "from_year": from_year_core,
                        "result_count": len(core_results),
                        "duration_ms": int(_lane_timings.get(cache_key, 0)),
                        **_lane_diag_fields(cache_key),
                    }
                )
                for idx, item in enumerate(core_results):
                    rank_factor = _clamp(1.0 - (idx / max(1, core_per_query * 1.6)), 0.2, 1.0)
                    base = float(item.get("score", 0.35) or 0.35)
                    score = _clamp((base * 0.78) + (rank_factor * 0.22), 0.03, 1.0)
                    out.append(
                        {
                            **item,
                            "score": round(_clamp(score + branch_score_bonus, 0.03, 1.0), 4),
                            "source_key": str(item.get("source_key") or branch_label),
                            "branch_id": branch_id,
                            "branch_label": branch_label,
                            "branch_mode": "core",
                            "matched_query": query,
                            "branch_core_topics": list(core_topics),
                            "branch_explore_topics": list(explore_topics),
                        }
                    )

        if explore_per_query > 0:
            for query in explore_queries:
                cache_key = ("explore", query, explore_per_query, from_year_explore)
                explore_results = _resolve_lane(query_cache[cache_key])
                lane_runs.append(
                    {
                        "lane_type": "branch_explore",
                        "branch_id": branch_id,
                        "branch_label": branch_label,
                        "query": query,
                        "from_year": from_year_explore,
                        "result_count": len(explore_results),
                        "duration_ms": int(_lane_timings.get(cache_key, 0)),
                        **_lane_diag_fields(cache_key),
                    }
                )
                for idx, item in enumerate(explore_results):
                    rank_factor = _clamp(1.0 - (idx / max(1, explore_per_query * 1.8)), 0.1, 1.0)
                    base = float(item.get("score", 0.2) or 0.2)
                    score = _clamp(
                        (base * (0.45 + (0.35 * temperature)))
                        + (rank_factor * (0.18 + (0.22 * temperature))),
                        0.02,
                        0.98,
                    )
                    out.append(
                        {
                            **item,
                            "score": round(_clamp(score + branch_score_bonus, 0.02, 0.98), 4),
                            "source_key": str(item.get("source_key") or branch_label),
                            "branch_id": branch_id,
                            "branch_label": branch_label,
                            "branch_mode": "explore",
                            "matched_query": query,
                            "branch_core_topics": list(core_topics),
                            "branch_explore_topics": list(explore_topics),
                        }
                    )

    # --- Consume pass: taste lane_specs.
    for lane in lane_specs:
        query = str(lane.get("query") or "").strip()
        if not query:
            continue
        per_query = max(1, int(lane.get("budget") or 1))
        from_year = int(lane.get("from_year") or current_year - 4)
        cache_key = (str(lane.get("lane_type") or "taste"), query, per_query, from_year)
        lane_results = _resolve_lane(query_cache.get(cache_key))
        source_key = str(lane.get("source_key") or query)
        lane_runs.append(
            {
                "lane_type": str(lane.get("lane_type") or "taste"),
                "query": query,
                "source_key": source_key,
                "from_year": from_year,
                "result_count": len(lane_results),
                "duration_ms": int(_lane_timings.get(cache_key, 0)),
                **_lane_diag_fields(cache_key),
            }
        )
        lane_strength = float(lane.get("strength") or 0.5)
        lane_type = str(lane.get("lane_type") or "taste_topic")
        for idx, item in enumerate(lane_results):
            rank_factor = _clamp(1.0 - (idx / max(1, per_query * 1.8)), 0.12, 1.0)
            base = float(item.get("score", 0.22) or 0.22)
            score = _clamp((base * 0.62) + (rank_factor * 0.18) + (lane_strength * 0.20), 0.02, 1.0)
            out.append(
                {
                    **item,
                    "score": round(score, 4),
                    "source_type": lane_type,
                    "source_key": source_key,
                    "taste_strength": round(lane_strength, 4),
                    "branch_mode": str(item.get("branch_mode") or ""),
                }
            )

    # --- Consume pass: followed-author fetches.
    for openalex_id, future, holder in followed_author_futures:
        try:
            recs = future.result() or []
        except Exception as exc:
            logger.warning("followed-author works fetch failed for %s: %s", openalex_id, exc)
            recs = []
        for item in recs:
            base = float(item.get("score", 0.3) or 0.3)
            score = _clamp(base, 0.05, 1.0)
            out.append(
                {
                    **item,
                    "score": round(score, 4),
                    "source_type": "followed_author",
                    "source_key": openalex_id,
                    "branch_mode": "followed_author",
                    "source_api": "openalex",
                }
            )
        lane_runs.append(
            {
                "lane_type": "followed_author",
                "query": openalex_id,
                "source_key": openalex_id,
                "from_year": year_floor,
                "result_count": len(recs),
                "duration_ms": int(holder.get("ms") or 0),
            }
        )
        if len(out) >= (limit + follow_budget):
            break

    # S2 list-mode recommendations lane — consume the future submitted above
    # (runs concurrently with branch + taste + followed-author lanes on the
    # shared lane_executor).
    if s2_recommend_enabled and s2_source_enabled:
        recommended: list[dict] = []
        if s2_recommend_future is not None:
            try:
                recommended = s2_recommend_future.result() or []
            except Exception as exc:
                s2_holder.setdefault("error", str(exc))
                recommended = []
        for idx, item in enumerate(recommended):
            rank_factor = _clamp(
                1.0 - (idx / max(1, s2_recommend_budget * 1.4)), 0.2, 1.0
            )
            base = float(item.get("score", 0.45) or 0.45)
            score = _clamp((base * 0.68) + (rank_factor * 0.22), 0.04, 1.0)
            out.append(
                {
                    **item,
                    "score": round(score, 4),
                    "source_type": "semantic_scholar_recommend",
                    "source_key": f"pos={len(s2_positive_seed_ids)}/neg={len(s2_negative_seed_ids)}",
                    "source_api": "semantic_scholar",
                    "branch_mode": "s2_recommend",
                }
            )
        lane_runs.append(
            {
                "lane_type": "semantic_scholar_recommend",
                "query": f"pos={len(s2_positive_seed_ids)} neg={len(s2_negative_seed_ids)}",
                "source_key": "s2_recommend",
                "from_year": 0,
                "result_count": len(recommended),
                "duration_ms": int(s2_holder.get("ms") or 0),
                **({"error": s2_holder["error"]} if s2_holder.get("error") else {}),
            }
        )

    if negative_context:
        filtered: list[dict] = []
        for item in out:
            penalty = _candidate_negative_preference_penalty(item, negative_context)
            if penalty >= 0.72:
                continue
            if penalty > 0.0:
                item = dict(item)
                item["score"] = round(float(item.get("score", 0.0) or 0.0) * (1.0 - (penalty * 0.55)), 4)
                item["negative_pref_penalty"] = round(penalty, 4)
            filtered.append(item)
        out = filtered

    # Dedupe by candidate identity and keep highest score.
    merged: dict[str, dict] = {}
    for item in out:
        key = _candidate_key(item)
        if key not in merged or float(item.get("score", 0.0) or 0.0) > float(merged[key].get("score", 0.0) or 0.0):
            merged[key] = item

    ranked = sorted(merged.values(), key=lambda x: float(x.get("score", 0.0) or 0.0), reverse=True)
    summary = {
        "recommendation_mode": recommendation_mode,
        "temperature": round(temperature, 3),
        "taste_profile": {
            "topics": [
                {"term": topic, "weight": round(float(weight), 4)}
                for topic, weight in preferred_topics
            ],
            "authors": [
                {"name": author, "weight": round(float(weight), 4)}
                for author, weight in preferred_authors
            ],
            "venues": [
                {"name": venue, "weight": round(float(weight), 4)}
                for venue, weight in preferred_venues
            ],
            "recent_wins": [
                {"query": query, "strength": round(float(strength), 4)}
                for query, strength in recent_win_queries
            ],
        },
        "negative_profile": {
            "topics": _top_negative_terms(dict(negative_context.get("topics") or {}), limit=4, field_name="term"),
            "authors": _top_negative_terms(dict(negative_context.get("authors") or {}), limit=4, field_name="name"),
            "venues": _top_negative_terms(dict(negative_context.get("journals") or {}), limit=3, field_name="name"),
        },
        "budgets": {
            "taste_budget_total": int(taste_budget_total),
            "followed_author_budget": int(follow_budget),
            "branch_explorer_enabled": bool(branch_enabled),
            "branch_controls": branch_controls,
            "effective_branch_controls": effective_branch_controls,
            "taste_lanes_enabled": {
                "topics": bool(taste_topics_enabled),
                "authors": bool(taste_authors_enabled),
                "venues": bool(taste_venues_enabled),
                "recent_wins": bool(recent_wins_enabled),
            },
        },
        "lane_runs": lane_runs[:24],
        "cold_start_topic": {
            "keyword": explicit_topic_keyword,
            "seed_count": len(seeds),
            "enabled": bool(explicit_topic_keyword) or (lens["context_type"] == "topic_keyword"),
        } if lens["context_type"] == "topic_keyword" else None,
    }
    # All lane futures have been resolved above — release worker threads.
    lane_executor.shutdown(wait=False)
    return ranked[:limit], summary


def _merge_channel_candidates(
    *,
    channel_weights: dict[str, float],
    channels: dict[str, list[dict]],
) -> dict[str, dict]:
    provenance_fields = (
        "source_type",
        "source_api",
        "source_key",
        "branch_id",
        "branch_label",
        "branch_mode",
        "taste_strength",
        "negative_pref_penalty",
    )
    metadata_fields = (
        "title",
        "authors",
        "abstract",
        "url",
        "doi",
        "openalex_id",
        "semantic_scholar_id",
        "semantic_scholar_corpus_id",
        "specter2_embedding",
        "specter2_model",
        "year",
        "journal",
        # T5 — S2-only fields. Kept in the back-fill list so a later
        # S2 lane can populate them on a candidate first found by
        # OpenAlex.
        "tldr",
        "influential_citation_count",
    )

    def _blank(value: object) -> bool:
        if value is None:
            return True
        if isinstance(value, str):
            return not value.strip()
        if isinstance(value, (list, tuple, dict, set)):
            return len(value) == 0
        return False

    merged: dict[str, dict] = {}
    for channel_name, items in channels.items():
        channel_weight = float(channel_weights.get(channel_name, 0.0) or 0.0)
        if channel_weight <= 0:
            continue
        for item in items:
            key = _candidate_key(item)
            score = float(item.get("score", 0.0) or 0.0)
            weighted = score * channel_weight
            if key not in merged:
                merged[key] = {
                    "title": (item.get("title") or "").strip(),
                    "authors": (item.get("authors") or "").strip(),
                    "abstract": (item.get("abstract") or "").strip(),
                    "url": (item.get("url") or "").strip(),
                    "doi": (item.get("doi") or "").strip(),
                    "openalex_id": (item.get("openalex_id") or "").strip(),
                    "semantic_scholar_id": (item.get("semantic_scholar_id") or "").strip(),
                    "semantic_scholar_corpus_id": str(item.get("semantic_scholar_corpus_id") or "").strip(),
                    "specter2_embedding": item.get("specter2_embedding"),
                    "specter2_model": item.get("specter2_model"),
                    "year": item.get("year"),
                    "journal": item.get("journal"),
                    "cited_by_count": int(item.get("cited_by_count") or 0),
                    # T5: thread S2-origin tldr + influential count through
                    # the merge so downstream upsert_paper can persist them.
                    # Non-S2 lanes won't set these; we keep them as default.
                    "tldr": (item.get("tldr") or "").strip(),
                    "influential_citation_count": int(item.get("influential_citation_count") or 0),
                    "score": 0.0,
                    "score_breakdown": {},
                    "_primary_weighted": 0.0,
                }
                for field in provenance_fields:
                    if field in item:
                        merged[key][field] = item.get(field)
            else:
                for field in metadata_fields:
                    if _blank(merged[key].get(field)) and not _blank(item.get(field)):
                        merged[key][field] = item.get(field)
                merged[key]["cited_by_count"] = max(
                    int(merged[key].get("cited_by_count") or 0),
                    int(item.get("cited_by_count") or 0),
                )
            merged[key]["score"] += weighted
            merged[key]["score_breakdown"][channel_name] = {
                "value": score,
                "weight": channel_weight,
                "weighted": weighted,
            }
            if weighted >= float(merged[key].get("_primary_weighted", 0.0) or 0.0):
                merged[key]["_primary_weighted"] = weighted
                for field in provenance_fields:
                    if field in item:
                        merged[key][field] = item.get(field)
    for value in merged.values():
        value["score"] = round(value["score"] * 100.0, 4)
        value.pop("_primary_weighted", None)
    return merged


def _candidate_key(item: dict) -> str:
    doi = normalize_doi((item.get("doi") or "").strip())
    if doi:
        return f"doi:{doi.lower()}"
    openalex_id = (item.get("openalex_id") or "").strip().lower()
    if openalex_id:
        return f"openalex:{openalex_id}"
    title = (item.get("title") or "").strip().lower()
    return f"title:{title}"


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
) -> list[dict]:
    """Merge per-branch outcome stats (incl. auto_weight) into each branch dict.

    Looks up by branch_id first, then falls back to `label:<lower-label>` so a
    branch that lost its stable id still picks up its outcome history when the
    label survives across refreshes.
    """
    enriched: list[dict] = []
    for branch in branches:
        branch_id = str(branch.get("id") or "").strip()
        branch_label = str(branch.get("label") or "").strip()
        outcome = (
            outcome_map.get(branch_id)
            or outcome_map.get(f"label:{branch_label.lower()}")
            or {}
        )
        enriched.append({**branch, **outcome})
    return enriched


def _branch_control_state(branch_id: str, controls: dict[str, Any]) -> str:
    if branch_id in set(controls.get("muted") or []):
        return "muted"
    if branch_id in set(controls.get("pinned") or []):
        return "pinned"
    if branch_id in set(controls.get("boosted") or []):
        return "boosted"
    return "normal"


def _apply_branch_controls(
    branches: list[dict],
    controls: dict[str, Any],
) -> list[dict]:
    pinned = set(controls.get("pinned") or [])
    muted = set(controls.get("muted") or [])
    boosted = set(controls.get("boosted") or [])

    annotated: list[dict] = []
    for branch in branches:
        branch_id = str(branch.get("id") or "").strip()
        state = _branch_control_state(branch_id, controls)
        item = {
            **branch,
            "control_state": state,
            "is_pinned": branch_id in pinned,
            "is_boosted": branch_id in boosted,
            "is_muted": branch_id in muted,
            "is_active": branch_id not in muted,
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


def _build_topic_keyword_cold_start_summary(
    lens: dict[str, Any],
    *,
    seed_count: int,
    lexical_count: int,
    graph_count: int,
    external_lane_counts: dict[str, int],
) -> Optional[dict[str, Any]]:
    if str(lens.get("context_type") or "") != "topic_keyword":
        return None
    config = lens.get("context_config") or {}
    keyword = str(config.get("keyword") or config.get("query") or "").strip()
    cold_start_results = int(external_lane_counts.get("cold_start_topic") or 0)
    if seed_count <= 0 and cold_start_results >= 3:
        state = "validated"
    elif seed_count <= 0 and cold_start_results > 0:
        state = "partial"
    elif seed_count <= 0:
        state = "blocked"
    elif cold_start_results > 0:
        state = "hybrid"
    else:
        state = "seeded"
    return {
        "keyword": keyword,
        "seed_count": int(seed_count),
        "lexical_results": int(lexical_count),
        "graph_results": int(graph_count),
        "external_results": cold_start_results,
        "state": state,
    }


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
