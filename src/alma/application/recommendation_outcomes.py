"""Canonical recommendation / paper OUTCOME projection for Insights (DRY #3).

Why this exists — finding **I-21**, locked decision **D6**: Discovery
``like`` / ``love`` / ``dislike`` write a ``feedback_events`` row plus
``papers.rating``; they do **not** set ``recommendations.user_action``
(confirmed in ``application/discovery/lens_crud.py`` —
``stamp_recommendation = action in {"save", "read", "dismiss", "seen"}``). So
every Insights/report metric that counted positive engagement via
``user_action = 'like'`` read a column that is essentially always empty and
silently under-reported the user's real positive signal.

This module is the ONE place that answers "was a paper received positively,
negatively, or neutrally?" from the authoritative sources:

* the per-paper net signal — :func:`signal_projection.compute_paper_signal_map`
  (time-decayed ``feedback_events`` via the shared normaliser + Library ratings
  + the reliably-stamped recommendation actions save/read/dismiss/seen), and
* lifecycle membership — ``status='library'`` is a deliberate save (positive);
  ``status in ('dismissed','removed')`` is a deliberate negative.

Every Insights/report call site joins the ``recommendations`` provenance
(source / branch / day) to this outcome and aggregates — none of them
re-derive "positive" on their own, so the word means the same thing
everywhere. The ``user_action`` column is still read, but ONLY for the things
it actually records: exposure (was the rec seen / acted on at all) and the
save/dismiss corroboration that already feeds the signal map.
"""

from __future__ import annotations

import sqlite3
from collections.abc import Iterable
from dataclasses import dataclass

from alma.api.helpers import table_exists
from alma.core.sql_helpers import standalone_paper_sql_for_db
from alma.application.signal_projection import compute_paper_signal_map

# Net-signal magnitude under which an outcome is "neutral" rather than having a
# sign forced onto float noise or a single weak event.
_NEUTRAL_DEADBAND = 0.05

# Lifecycle nudge: a saved paper is a deliberate positive even with no explicit
# rating/feedback; a dismissed/removed paper is a deliberate negative. Kept
# modest so an explicit strong rating/feedback still dominates the sign.
_LIFECYCLE_POSITIVE = 0.5
_LIFECYCLE_NEGATIVE = -0.5


def _classify(polarity: float) -> str:
    if polarity > _NEUTRAL_DEADBAND:
        return "positive"
    if polarity < -_NEUTRAL_DEADBAND:
        return "negative"
    return "neutral"


@dataclass(frozen=True)
class PaperOutcome:
    """The net received-outcome of one paper, from authoritative sources."""

    paper_id: str
    polarity: float  # net signed preference in [-1, 1]
    classification: str  # "positive" | "negative" | "neutral"

    @property
    def is_positive(self) -> bool:
        return self.classification == "positive"

    @property
    def is_negative(self) -> bool:
        return self.classification == "negative"


def build_paper_outcome_map(db: sqlite3.Connection) -> dict[str, PaperOutcome]:
    """Per-paper outcome: net feedback/rating signal blended with lifecycle.

    Papers with neither a preference signal nor a terminal lifecycle status are
    absent from the map; callers treat a missing paper as ``neutral`` (a
    recommendation the user never engaged with is honestly neutral, NOT a
    silent zero-like).
    """
    signals = compute_paper_signal_map(db)

    statuses: dict[str, str] = {}
    try:
        for row in db.execute(
            f"""
            SELECT id, status
            FROM papers
            WHERE status IN ('library','dismissed','removed')
              AND {standalone_paper_sql_for_db(db, 'papers')}
            """
        ).fetchall():
            pid = str((row["id"] if isinstance(row, sqlite3.Row) else row[0]) or "").strip()
            if pid:
                statuses[pid] = str((row["status"] if isinstance(row, sqlite3.Row) else row[1]) or "")
    except sqlite3.OperationalError:
        statuses = {}

    out: dict[str, PaperOutcome] = {}
    for pid in set(signals) | set(statuses):
        polarity = float(signals.get(pid, 0.0))
        status = statuses.get(pid)
        if status == "library":
            polarity += _LIFECYCLE_POSITIVE
        elif status in ("dismissed", "removed"):
            polarity += _LIFECYCLE_NEGATIVE
        polarity = max(-1.0, min(1.0, polarity))
        out[pid] = PaperOutcome(pid, round(polarity, 4), _classify(polarity))
    return out


@dataclass(frozen=True)
class RecommendationOutcome:
    """One recommendation's provenance joined to its paper outcome.

    Carries every grouping dimension the Insights/report call sites need
    (source / branch / day / mode / publication_date) so they only aggregate,
    never re-derive polarity. ``classification`` comes from the paper outcome;
    ``is_seen`` uses ``user_action`` for what it reliably records — exposure.
    """

    paper_id: str
    source_type: str
    source_api: str
    branch_id: str | None
    branch_label: str | None
    branch_mode: str | None
    score: float | None
    score_breakdown: str | None
    publication_date: str | None
    created_at: str | None
    action_at: str | None
    day: str | None
    user_action: str | None
    classification: str

    @property
    def is_positive(self) -> bool:
        return self.classification == "positive"

    @property
    def is_negative(self) -> bool:
        return self.classification == "negative"

    @property
    def is_seen(self) -> bool:
        # Exposure: any stamped action (save/read/dismiss/seen) means the rec
        # was surfaced/acted on. D6-safe — we never treat this as a "like".
        return bool((self.user_action or "").strip())


def build_recommendation_outcomes(
    db: sqlite3.Connection, *, since: str | None = None
) -> list[RecommendationOutcome]:
    """All recommendations (optionally since an ISO cutoff) with their outcome.

    One pass: the paper outcome map is built once, then each recommendation row
    is joined to it. ``since`` filters on ``COALESCE(action_at, created_at)``.
    """
    if not table_exists(db, "recommendations"):
        return []

    outcomes = build_paper_outcome_map(db)

    sql = f"""
        SELECT
            r.paper_id AS paper_id,
            COALESCE(NULLIF(r.source_type, ''), 'unknown') AS source_type,
            COALESCE(NULLIF(r.source_api, ''), 'unknown') AS source_api,
            NULLIF(r.branch_id, '') AS branch_id,
            NULLIF(r.branch_label, '') AS branch_label,
            NULLIF(r.branch_mode, '') AS branch_mode,
            r.score AS score,
            r.score_breakdown AS score_breakdown,
            p.publication_date AS publication_date,
            r.created_at AS created_at,
            r.action_at AS action_at,
            r.user_action AS user_action
        FROM recommendations r
        JOIN papers p ON p.id = r.paper_id
         AND {standalone_paper_sql_for_db(db, 'p')}
    """
    params: tuple = ()
    if since:
        sql += " WHERE COALESCE(r.action_at, r.created_at) >= ?"
        params = (since,)

    records: list[RecommendationOutcome] = []
    for row in db.execute(sql, params).fetchall():
        pid = str((row["paper_id"] if isinstance(row, sqlite3.Row) else row[0]) or "").strip()
        action_at = row["action_at"] if isinstance(row, sqlite3.Row) else row[10]
        created_at = row["created_at"] if isinstance(row, sqlite3.Row) else row[9]
        stamp = str(action_at or created_at or "")
        day = stamp[:10] if len(stamp) >= 10 else None
        outcome = outcomes.get(pid)
        records.append(
            RecommendationOutcome(
                paper_id=pid,
                source_type=str(row["source_type"]),
                source_api=str(row["source_api"]),
                branch_id=row["branch_id"],
                branch_label=row["branch_label"],
                branch_mode=row["branch_mode"],
                score=row["score"],
                score_breakdown=row["score_breakdown"],
                publication_date=row["publication_date"],
                created_at=created_at,
                action_at=action_at,
                day=day,
                user_action=row["user_action"],
                classification=outcome.classification if outcome else "neutral",
            )
        )
    return records


@dataclass(frozen=True)
class OutcomeCounts:
    """Tally over a group of recommendation outcomes.

    Decomposes the positive outcomes into ``saved`` (the deliberate library-save
    action — a subset of positives) and ``liked`` (every OTHER positive: a
    like/love/high-rating that lives in ``feedback_events``, never in
    ``user_action``). So ``saved + liked == positive`` and they never overlap —
    which keeps ``positive_rate = (saved + liked) / total`` exactly the figure
    the legacy SQL computed, only now sourced from real signal (I-21).
    """

    total: int = 0
    positive: int = 0
    negative: int = 0
    neutral: int = 0
    seen: int = 0  # any stamped action (exposure)
    saved: int = 0  # user_action == 'save'
    seen_action: int = 0  # user_action == 'seen' (explicit "shown" mark)

    @property
    def liked(self) -> int:
        # Positives that were not an explicit save — likes/loves/ratings.
        return max(0, self.positive - self.saved)

    @property
    def dismissed(self) -> int:
        return self.negative

    @property
    def unseen(self) -> int:
        return max(0, self.total - self.seen)

    @property
    def positive_rate(self) -> float:
        return round(self.positive / self.total, 3) if self.total else 0.0

    @property
    def dismiss_rate(self) -> float:
        return round(self.negative / self.total, 3) if self.total else 0.0

    @property
    def engagement_rate(self) -> float:
        """Share that drew any positive OR negative reaction (not just seen)."""
        return round((self.positive + self.negative) / self.total, 3) if self.total else 0.0


def count_outcomes(records: Iterable[RecommendationOutcome]) -> OutcomeCounts:
    """Tally a group of recommendation outcomes (one row per recommendation)."""
    total = positive = negative = neutral = seen = saved = seen_action = 0
    for rec in records:
        total += 1
        if rec.is_positive:
            positive += 1
        elif rec.is_negative:
            negative += 1
        else:
            neutral += 1
        if rec.is_seen:
            seen += 1
        action = (rec.user_action or "").strip()
        if action == "save":
            saved += 1
        elif action == "seen":
            seen_action += 1
    return OutcomeCounts(
        total=total,
        positive=positive,
        negative=negative,
        neutral=neutral,
        seen=seen,
        saved=saved,
        seen_action=seen_action,
    )
