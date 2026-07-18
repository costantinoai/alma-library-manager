"""Canonical per-author attention model — ONE source of truth.

Two surfaces answer the question "which tracked authors need attention":

* Health → System status → "Tracked authors" popup
  (``insights`` authors snapshot + operational diagnostics)
* Authors page → "Needs attention" section
  (``GET /authors/needs-attention``)

Historically each computed its own overlapping-but-different subset inline
(the popup looked at monitor health + corpus backfills, the page at identity
resolution), so the two disagreed on both membership and counts. Every
attention dimension now lives HERE and both routes compose from these
builders — a new dimension goes in this module, never inline in a route.

Dimensions:

``identity``
    The id-resolution ladder (error / no_match / needs_manual_review /
    followed-without-openalex). Predicate + severity ranking come from
    :mod:`alma.services.health` (already the shared source for assess_authors),
    so counts, rows, and ranks can never diverge.

``monitors``
    Author-type feed monitors whose health is ``degraded``. Callers pass the
    serialized monitor list from
    :func:`alma.application.feed_monitors.list_feed_monitors` — the single
    place monitor health is computed — so this module never re-derives it.

``corpus``
    Followed-author historical-backfill states. ``stale`` / ``thin`` /
    ``failed`` / ``unverified`` are actionable (a Backfill fixes them);
    ``pending`` / ``running`` are informational — queued work is not a
    problem to fix and must never grow a button.

Everything here is a pure read.
"""

from __future__ import annotations

import sqlite3
from typing import Any

from alma.application.followed_authors import get_followed_author_backfill_status
from alma.services import health as health_service

# Corpus states a Backfill action genuinely fixes vs. states that only inform.
CORPUS_ACTIONABLE_STATES = {"stale", "thin", "failed", "unverified"}
CORPUS_INFORMATIONAL_STATES = {"pending", "running"}


def _table_exists(db: sqlite3.Connection, table: str) -> bool:
    row = db.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name=?", (table,)
    ).fetchone()
    return row is not None


# ── identity ────────────────────────────────────────────────────────────────


def identity_attention_count(db: sqlite3.Connection) -> int:
    """Uncapped count of authors matching the canonical attention predicate."""
    if not _table_exists(db, "authors"):
        return 0
    where = health_service.author_attention_where_sql()
    row = db.execute(f"SELECT COUNT(*) AS c FROM authors a WHERE {where}").fetchone()
    return int((row["c"] if row else 0) or 0)


def identity_attention_rows(db: sqlite3.Connection, limit: int = 50) -> list[dict[str, Any]]:
    """Ranked identity-resolution attention rows.

    Severity ordering: error > no_match > needs_manual_review >
    followed-without-openalex; within a bucket, most-recently-seen first.
    Both the CASE and the WHERE come from the canonical ladder in
    :mod:`alma.services.health` (hardcoded SQL constants — safe to
    interpolate), the same fragments ``assess_authors`` counts with.
    """
    if not _table_exists(db, "authors"):
        return []
    severity_case = health_service.author_attention_severity_case_sql()
    where = health_service.author_attention_where_sql()
    rows = db.execute(
        f"""
        SELECT
            a.id AS author_id,
            a.name AS author_name,
            COALESCE(a.openalex_id, '') AS openalex_id,
            COALESCE(a.id_resolution_status, '') AS status,
            COALESCE(a.id_resolution_method, '') AS method,
            COALESCE(a.id_resolution_confidence, 0.0) AS confidence,
            COALESCE(a.id_resolution_reason, '') AS reason,
            COALESCE(a.id_resolution_updated_at, a.last_fetched_at, '') AS updated_at,
            {severity_case}
        FROM authors a
        WHERE {where}
        ORDER BY severity ASC, updated_at DESC, a.name ASC
        LIMIT ?
        """,
        (limit,),
    ).fetchall()
    return [dict(r) for r in rows]


# ── monitors ────────────────────────────────────────────────────────────────


def monitor_attention_rows(monitors: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Degraded author-type monitors from an already-serialized monitor list.

    ``monitors`` must be the output of
    :func:`alma.application.feed_monitors.list_feed_monitors` (or the feed
    snapshot built on it) so monitor health has exactly one computation.
    """
    return [
        {
            "author_id": str(m.get("author_id") or "") or None,
            "author_name": str(m.get("author_name") or m.get("label") or "").strip()
            or "Tracked author",
            "monitor_id": str(m.get("id") or ""),
            "health_reason": m.get("health_reason"),
            "last_error": m.get("last_error"),
            "last_checked_at": m.get("last_checked_at"),
        }
        for m in monitors
        if str(m.get("monitor_type") or "") == "author" and m.get("health") == "degraded"
    ]


# ── corpus backfills ────────────────────────────────────────────────────────


def corpus_backfill_rows(
    db: sqlite3.Connection,
) -> tuple[dict[str, int], list[dict[str, Any]]]:
    """Classify every followed author's historical-backfill state.

    Returns ``(state_counts, rows)`` where ``state_counts`` tallies ALL
    states (fresh/running/pending/stale/thin/failed/unverified — the
    uncapped truth for summary counters) and ``rows`` lists the authors in
    a non-fresh state (actionable + informational), shaped as the
    diagnostics ``corpus_health`` entries.

    Background-publication counts are pre-computed in one grouped query —
    the per-author helper would otherwise re-issue the same heavy 3-way
    JOIN per author (an N+1 that ran the diagnostics tab into ~60 s on a
    50-author corpus).
    """
    counts = {
        "fresh": 0,
        "running": 0,
        "pending": 0,
        "stale": 0,
        "thin": 0,
        "failed": 0,
        "unverified": 0,
    }
    rows: list[dict[str, Any]] = []
    if not (_table_exists(db, "followed_authors") and _table_exists(db, "authors")):
        return counts, rows

    try:
        followed_rows = db.execute(
            """
            SELECT a.id, a.name, COALESCE(a.works_count, 0) AS works_count
            FROM authors a
            JOIN followed_authors fa ON fa.author_id = a.id
            ORDER BY a.name
            """
        ).fetchall()
    except sqlite3.OperationalError:
        return counts, rows

    bg_counts: dict[str, int] = {}
    if _table_exists(db, "publication_authors") and followed_rows:
        try:
            bg_rows = db.execute(
                """
                SELECT a.id AS author_id, COUNT(DISTINCT p.id) AS c
                FROM authors a
                JOIN followed_authors fa ON fa.author_id = a.id
                JOIN publication_authors pa
                  ON lower(a.openalex_id) = lower(pa.openalex_id)
                 AND a.openalex_id IS NOT NULL
                 AND TRIM(a.openalex_id) <> ''
                JOIN papers p ON p.id = pa.paper_id
                WHERE p.status <> 'library'
                GROUP BY a.id
                """
            ).fetchall()
            bg_counts = {str(r["author_id"]): int(r["c"] or 0) for r in bg_rows}
        except sqlite3.OperationalError:
            bg_counts = {}

    for row in followed_rows:
        author_id = str(row["id"] or "").strip()
        if not author_id:
            continue
        status = get_followed_author_backfill_status(
            db,
            author_id,
            works_count=int(row["works_count"] or 0),
            background_publications=bg_counts.get(author_id, 0),
        )
        state = str(status.get("state") or "unknown")
        if state in counts:
            counts[state] += 1
        if state in CORPUS_ACTIONABLE_STATES or state in CORPUS_INFORMATIONAL_STATES:
            rows.append(
                {
                    "author_id": author_id,
                    "author_name": str(row["name"] or "").strip() or author_id,
                    "state": state,
                    "detail": str(status.get("detail") or "").strip() or None,
                    "background_publications": int(status.get("background_publications") or 0),
                    "coverage_ratio": status.get("coverage_ratio"),
                    "last_success_at": status.get("last_success_at"),
                }
            )
    return counts, rows
