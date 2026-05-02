"""Gap Radar: Detect blind spots in your research coverage."""

from __future__ import annotations

import logging
import sqlite3
import uuid
from datetime import datetime
from typing import Any, Optional

from alma.application.followed_authors import ensure_followed_author_contract
from alma.core.scoring_math import age_decay
from alma.openalex.client import _normalize_openalex_author_id as _norm_oaid

logger = logging.getLogger(__name__)

_REMOVE_SIGNAL_SOFT = -0.38
_REMOVE_SIGNAL_HARD = -0.82
_REMOVE_DECAY_HALF_LIFE_DAYS = 45.0
_HARD_REMOVE_HALF_LIFE_DAYS = 120.0
_HARD_REMOVE_THRESHOLD = 3
_SUPPRESSION_THRESHOLD = -0.18


def _utcnow() -> datetime:
    return datetime.utcnow()


def _parse_dt(raw: Any) -> Optional[datetime]:
    text = str(raw or "").strip()
    if not text:
        return None
    try:
        return datetime.fromisoformat(text.replace("Z", "+00:00")).replace(tzinfo=None)
    except ValueError:
        return None


def _normalize_openalex_author_id(value: str) -> str:
    text = str(value or "").strip()
    if not text:
        return ""
    normalized = _norm_oaid(text)
    return (normalized or text).strip()


def _decayed_signal(signal_value: float, age_days: float, half_life_days: float) -> float:
    if half_life_days <= 0 or age_days <= 0:
        return signal_value
    return signal_value * age_decay(age_days, half_life_days=half_life_days)


def ensure_gap_feedback_tables(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS missing_author_feedback (
            id TEXT PRIMARY KEY,
            openalex_id TEXT NOT NULL,
            action TEXT NOT NULL,
            signal_value REAL NOT NULL,
            created_at TEXT NOT NULL
        )
        """
    )
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_missing_author_feedback_author_time
        ON missing_author_feedback(openalex_id, created_at DESC)
        """
    )
    # Bucket attribution for outcome calibration (Phase 4 #3). Added
    # post-launch — guarded by try/except so existing DBs migrate
    # silently. NULL on rows from before bucket attribution shipped.
    try:
        conn.execute(
            "ALTER TABLE missing_author_feedback ADD COLUMN suggestion_bucket TEXT"
        )
    except sqlite3.OperationalError:
        pass
    # Follow-side attribution. Symmetric to `missing_author_feedback`
    # for the reject side: one row per "user followed an author from
    # the suggestion rail", carrying the originating bucket label so
    # bucket-quality calibration can reweight the rail.
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS author_suggestion_follow_log (
            id TEXT PRIMARY KEY,
            openalex_id TEXT NOT NULL,
            suggestion_bucket TEXT,
            created_at TEXT NOT NULL
        )
        """
    )
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_author_suggestion_follow_log_oid_time
        ON author_suggestion_follow_log(openalex_id, created_at DESC)
        """
    )


def get_missing_author_feedback_state(
    conn: sqlite3.Connection,
    openalex_id: str,
) -> dict[str, Any]:
    ensure_gap_feedback_tables(conn)
    normalized = _normalize_openalex_author_id(openalex_id)
    if not normalized:
        return {
            "openalex_id": "",
            "score": 0.0,
            "suppressed": False,
            "consecutive_removes": 0,
            "last_removed_at": None,
        }

    rows = conn.execute(
        """
        SELECT action, signal_value, created_at
        FROM missing_author_feedback
        WHERE openalex_id = ?
        ORDER BY created_at DESC
        LIMIT 24
        """,
        (normalized,),
    ).fetchall()
    if not rows:
        return {
            "openalex_id": normalized,
            "score": 0.0,
            "suppressed": False,
            "consecutive_removes": 0,
            "last_removed_at": None,
        }

    now = _utcnow()
    score = 0.0
    consecutive_removes = 0
    last_removed_at: Optional[str] = None

    for idx, row in enumerate(rows):
        created = _parse_dt(row["created_at"])
        age_days = max(0.0, (now - created).total_seconds() / 86400.0) if created else 0.0
        score += _decayed_signal(float(row["signal_value"] or 0.0), age_days, _REMOVE_DECAY_HALF_LIFE_DAYS)

        if idx == 0 and str(row["action"] or "") == "remove":
            last_removed_at = row["created_at"]

    for row in rows:
        if str(row["action"] or "") != "remove":
            break
        consecutive_removes += 1

    if consecutive_removes >= _HARD_REMOVE_THRESHOLD and last_removed_at:
        last_remove_dt = _parse_dt(last_removed_at)
        age_days = max(0.0, (now - last_remove_dt).total_seconds() / 86400.0) if last_remove_dt else 0.0
        score += _decayed_signal(_REMOVE_SIGNAL_HARD, age_days, _HARD_REMOVE_HALF_LIFE_DAYS)

    suppressed = score <= _SUPPRESSION_THRESHOLD
    return {
        "openalex_id": normalized,
        "score": round(score, 4),
        "suppressed": suppressed,
        "consecutive_removes": consecutive_removes,
        "last_removed_at": last_removed_at,
    }


def record_missing_author_remove(
    conn: sqlite3.Connection,
    openalex_id: str,
    *,
    hard: bool = False,
    suggestion_bucket: str | None = None,
) -> dict[str, Any]:
    """Record a remove signal against an OpenAlex author.

    `hard=True` writes the strong signal (-0.82, ~120-day half-life)
    used by the definitive "go away" actions (unfollow, hard-delete).
    Default soft signal (-0.38, ~45-day half-life) is for one-off
    suggestion rejections — a single click is enough to suppress for a
    cycle but the author can come back if they show up again later
    with new evidence.

    `suggestion_bucket` carries the originating rail bucket label
    (`library_core` / `cited_by_high_signal` / etc.) so outcome
    calibration can reweight the rail per bucket. NULL when the
    reject came from a non-rail surface (unfollow page, etc.).
    """
    ensure_gap_feedback_tables(conn)
    normalized = _normalize_openalex_author_id(openalex_id)
    if not normalized:
        raise ValueError("Invalid OpenAlex author ID")

    signal = _REMOVE_SIGNAL_HARD if hard else _REMOVE_SIGNAL_SOFT
    bucket = (suggestion_bucket or "").strip().lower() or None
    conn.execute(
        """
        INSERT INTO missing_author_feedback
            (id, openalex_id, action, signal_value, created_at, suggestion_bucket)
        VALUES (?, ?, 'remove', ?, ?, ?)
        """,
        (
            uuid.uuid4().hex,
            normalized,
            signal,
            _utcnow().isoformat(),
            bucket,
        ),
    )
    return get_missing_author_feedback_state(conn, normalized)


def record_followed_from_suggestion(
    conn: sqlite3.Connection,
    openalex_id: str,
    suggestion_bucket: str | None,
) -> None:
    """Log that the user followed an author surfaced by the suggestion rail.

    Symmetric to `record_missing_author_remove` for the positive side.
    Called by the rail's track-follow route after the actual follow
    write has succeeded — this row exists purely for outcome
    calibration. Multiple rows per author are fine (the user might
    refollow after an unfollow); calibration aggregates with time decay.
    """
    ensure_gap_feedback_tables(conn)
    normalized = _normalize_openalex_author_id(openalex_id)
    if not normalized:
        raise ValueError("Invalid OpenAlex author ID")
    bucket = (suggestion_bucket or "").strip().lower() or None
    conn.execute(
        """
        INSERT INTO author_suggestion_follow_log
            (id, openalex_id, suggestion_bucket, created_at)
        VALUES (?, ?, ?, ?)
        """,
        (uuid.uuid4().hex, normalized, bucket, _utcnow().isoformat()),
    )


def clear_missing_author_feedback(
    conn: sqlite3.Connection,
    openalex_id: str,
) -> None:
    ensure_gap_feedback_tables(conn)
    normalized = _normalize_openalex_author_id(openalex_id)
    if not normalized:
        return
    conn.execute("DELETE FROM missing_author_feedback WHERE openalex_id = ?", (normalized,))


