"""Evidence-based display affiliation policy for authors."""

from __future__ import annotations

import math
import re
import sqlite3
from dataclasses import dataclass
from datetime import datetime
from typing import Any


_NAME_RE = re.compile(r"[^a-z0-9]+")


# A user's explicit affiliation pick is recorded as evidence under this source.
# It is NOT one of the auto sources (openalex/orcid/crossref/semantic_scholar),
# so a per-source refresh (which deletes WHERE source=<that source>) never wipes
# it — the pick survives every future hydration.
_MANUAL_SOURCE = "manual"
# Far above any auto candidate's max (orcid employment 1.0 + bonuses ≈ 1.7), so a
# manual pick is always selected, unambiguously.
_MANUAL_AFFILIATION_SCORE = 100.0


@dataclass(frozen=True)
class AffiliationDecision:
    author_id: str
    selected_affiliation: str | None
    changed: bool
    conflict: bool
    candidates: list[dict[str, Any]]


def ensure_author_affiliation_evidence_table(conn: sqlite3.Connection) -> None:
    conn.execute(
        """CREATE TABLE IF NOT EXISTS author_affiliation_evidence (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            author_id TEXT NOT NULL REFERENCES authors(id) ON DELETE CASCADE,
            source TEXT NOT NULL,
            institution_openalex_id TEXT,
            institution_ror TEXT,
            institution_name TEXT NOT NULL,
            role TEXT,
            start_date TEXT NOT NULL DEFAULT '',
            end_date TEXT,
            is_current INTEGER DEFAULT 0,
            evidence_url TEXT,
            confidence REAL,
            observed_at TEXT NOT NULL,
            UNIQUE (author_id, source, institution_name, role, start_date)
        )"""
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_author_affiliation_evidence_author "
        "ON author_affiliation_evidence(author_id, is_current DESC, observed_at DESC)"
    )


def _name_key(value: str) -> str:
    return _NAME_RE.sub("", str(value or "").lower()).strip()


def _year(value: object) -> int | None:
    text = str(value or "").strip()
    if len(text) < 4:
        return None
    try:
        return int(text[:4])
    except ValueError:
        return None


def _base_score(source: str, role: str) -> float:
    source_key = str(source or "").strip().lower()
    role_key = str(role or "").strip().lower()
    # A user's manual pick is authoritative: it must outscore every auto source
    # (and all their bonuses) so `recompute_display_affiliation` always selects
    # it and the conflict detector treats it as resolved (D-affiliation-lock).
    if source_key == _MANUAL_SOURCE:
        return _MANUAL_AFFILIATION_SCORE
    if source_key == "orcid" and role_key == "employment":
        return 1.0
    if source_key == "openalex" and role_key == "last_known_institution":
        return 0.86
    if source_key == "openalex":
        return 0.78
    if source_key == "orcid" and role_key == "education":
        return 0.72
    if source_key == "crossref":
        return 0.54
    if source_key == "semantic_scholar":
        return 0.4
    return 0.3


def _score_row(row: sqlite3.Row, *, source_count_by_name: dict[str, set[str]]) -> float:
    source = str(row["source"] or "")
    role = str(row["role"] or "")
    score = _base_score(source, role)
    confidence = row["confidence"]
    if confidence is not None:
        try:
            score *= max(0.1, min(1.0, float(confidence)))
        except (TypeError, ValueError):
            pass
    if int(row["is_current"] or 0):
        score += 0.22
    start_year = _year(row["start_date"])
    if start_year:
        current_year = datetime.utcnow().year
        age = max(0, current_year - start_year)
        score += max(0.0, 0.18 * math.exp(-age / 12.0))
    key = _name_key(str(row["institution_name"] or ""))
    if len(source_count_by_name.get(key, set())) >= 2:
        score *= 1.2
    return round(score, 6)


def _candidate_rows(
    conn: sqlite3.Connection,
    author_id: str,
    *,
    ensure: bool,
) -> list[sqlite3.Row]:
    if ensure:
        ensure_author_affiliation_evidence_table(conn)
    try:
        return conn.execute(
            """
            SELECT *
            FROM author_affiliation_evidence
            WHERE author_id = ?
              AND COALESCE(NULLIF(TRIM(institution_name), ''), '') != ''
            ORDER BY is_current DESC, observed_at DESC, id DESC
            """,
            (author_id,),
        ).fetchall()
    except sqlite3.OperationalError:
        return []


def score_affiliation_candidates(
    conn: sqlite3.Connection,
    author_id: str,
    *,
    ensure: bool = False,
) -> list[dict[str, Any]]:
    rows = _candidate_rows(conn, author_id, ensure=ensure)
    source_count_by_name: dict[str, set[str]] = {}
    for row in rows:
        key = _name_key(str(row["institution_name"] or ""))
        if key:
            source_count_by_name.setdefault(key, set()).add(str(row["source"] or ""))
    candidates: list[dict[str, Any]] = []
    for row in rows:
        item = dict(row)
        item["score"] = _score_row(row, source_count_by_name=source_count_by_name)
        candidates.append(item)
    candidates.sort(
        key=lambda item: (
            -float(item.get("score") or 0.0),
            -int(item.get("is_current") or 0),
            str(item.get("institution_name") or ""),
        )
    )
    return candidates


def _candidates_conflict(candidates: list[dict[str, Any]]) -> bool:
    """True iff the top-2 affiliation candidates genuinely disagree.

    Single source of truth for "is this an affiliation conflict?" — shared by
    `recompute_display_affiliation` and `list_affiliation_conflicts` (the logic
    used to be duplicated and could drift). A conflict is two close-scored
    candidates from different sources naming different institutions.

    A user's manual pick (source='manual') sits at the top (it outscores all
    auto evidence) and means the conflict is RESOLVED — the user decided which
    institution to show — so it is never re-flagged even though the underlying
    auto sources still disagree. This is the terminal acknowledgment that stops
    the affiliation health step blocking forever.
    """
    if not candidates:
        return False
    top = candidates[0]
    if str(top.get("source") or "").strip().lower() == _MANUAL_SOURCE:
        return False
    if len(candidates) < 2:
        return False
    second = candidates[1]
    first_score = float(top.get("score") or 0.0)
    second_score = float(second.get("score") or 0.0)
    return bool(
        first_score > 0
        and second_score >= first_score * 0.9
        and str(top.get("source") or "") != str(second.get("source") or "")
        and _name_key(str(top.get("institution_name") or "")) != _name_key(str(second.get("institution_name") or ""))
    )


def record_manual_affiliation(
    conn: sqlite3.Connection,
    author_id: str,
    *,
    institution_name: str,
    institution_openalex_id: str | None = None,
    institution_ror: str | None = None,
) -> AffiliationDecision:
    """Lock an author's display affiliation to a user-chosen institution.

    Records the pick as an authoritative ``source='manual'`` evidence row,
    reusing the evidence table + scoring primitive so the pick (a) outscores
    every auto source, (b) survives auto-refresh (per-source replace deletes
    only its own source, never 'manual'), and (c) suppresses the
    affiliation-conflict flag (the user decided). Replaces any prior manual
    pick (one lock at a time), then recomputes + persists the display
    affiliation. Caller owns the transaction (no commit here).
    """
    author_key = str(author_id or "").strip()
    name = str(institution_name or "").strip()
    if not author_key or not name:
        raise ValueError("author_id and institution_name are required")
    ensure_author_affiliation_evidence_table(conn)
    now = datetime.utcnow().isoformat()
    conn.execute(
        "DELETE FROM author_affiliation_evidence WHERE author_id = ? AND source = ?",
        (author_key, _MANUAL_SOURCE),
    )
    conn.execute(
        """
        INSERT INTO author_affiliation_evidence
            (author_id, source, institution_openalex_id, institution_ror,
             institution_name, role, start_date, end_date, is_current,
             evidence_url, confidence, observed_at)
        VALUES (?, ?, ?, ?, ?, 'manual_pick', '', NULL, 1, NULL, 1.0, ?)
        """,
        (author_key, _MANUAL_SOURCE, institution_openalex_id, institution_ror, name, now),
    )
    return recompute_display_affiliation(conn, author_key)


def recompute_display_affiliation(conn: sqlite3.Connection, author_id: str) -> AffiliationDecision:
    """Pick and persist the display affiliation from evidence rows."""
    author_key = str(author_id or "").strip()
    if not author_key:
        return AffiliationDecision("", None, False, False, [])
    candidates = score_affiliation_candidates(conn, author_key, ensure=True)
    if not candidates:
        return AffiliationDecision(author_key, None, False, False, [])

    top = candidates[0]
    selected = str(top.get("institution_name") or "").strip() or None
    current_row = conn.execute(
        "SELECT affiliation FROM authors WHERE id = ?",
        (author_key,),
    ).fetchone()
    current = str((current_row["affiliation"] if current_row else "") or "").strip()
    changed = bool(selected and selected != current)
    if selected:
        conn.execute(
            "UPDATE authors SET affiliation = ? WHERE id = ?",
            (selected, author_key),
        )

    conflict = _candidates_conflict(candidates)
    return AffiliationDecision(author_key, selected, changed, conflict, candidates)


def list_affiliation_conflicts(conn: sqlite3.Connection, *, limit: int = 100) -> list[dict[str, Any]]:
    """Return current evidence conflicts for Authors needs-attention.

    ``limit`` caps the RETURNED list only — it must NOT cap the author scan.
    The old query applied ``LIMIT`` to the candidate scan (ordered by name), so
    which conflicts were *detected* depended on the limit: a conflict on a
    late-alphabet author was never examined at ``limit=50`` but was at
    ``limit=500``. That made the Health "Affiliation conflicts" count
    (``assess_authors`` calls with limit=500) disagree with the
    ``/authors/needs-attention`` list (limit=50) — the count showed 1 while the
    drilldown showed "everything resolved". We now scan EVERY author with
    affiliation evidence and cap only the collected results, so detection is
    stable regardless of the caller's limit.
    """
    try:
        rows = conn.execute(
            """
            SELECT DISTINCT a.id, a.name, a.openalex_id, a.affiliation
            FROM authors a
            JOIN author_affiliation_evidence ev ON ev.author_id = a.id
            WHERE COALESCE(a.status, 'active') != 'removed'
            ORDER BY a.name ASC
            """
        ).fetchall()
    except sqlite3.OperationalError:
        return []
    out: list[dict[str, Any]] = []
    for row in rows:
        candidates = score_affiliation_candidates(conn, str(row["id"]))
        # Same conflict test as recompute (manual pick → resolved, not a conflict).
        if not _candidates_conflict(candidates):
            continue
        first = candidates[0]
        second = candidates[1]
        out.append(
            {
                "author_id": str(row["id"]),
                "author_name": str(row["name"] or row["id"]),
                "openalex_id": row["openalex_id"],
                "selected_affiliation": str(first.get("institution_name") or "") or None,
                "first": first,
                "second": second,
            }
        )
        if len(out) >= limit:
            break
    return out
