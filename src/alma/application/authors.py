"""Author use-cases extracted from route handlers."""

from __future__ import annotations

import json
import logging
import math
import re
import sqlite3
import unicodedata
from datetime import datetime, timedelta
from typing import Optional

from alma.application.followed_authors import (
    ensure_followed_author_contract,
    get_followed_author_backfill_status,
)
from alma.application.signal_projection import (
    ProjectedPaperSignals,
    load_projected_paper_signals,
)
from alma.core.scoring_math import (
    consensus_bonus as _shared_consensus_bonus,
    log_prevalence_weights,
)
from alma.core.utils import normalize_orcid
from alma.openalex.client import _normalize_openalex_author_id as _normalize_oaid
from . import feed_monitors as monitor_app

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Author-suggestion scoring constants.
#
# Every per-bucket scoring formula in `list_author_suggestions` and its
# helpers clamps to `_MAX_SUGGESTION_SCORE`. Downstream multipliers
# (bucket weights from `discovery_settings.author_suggestion_weights.*`,
# the multi-source consensus bonus, the network-bucket composite × 100
# scaling) are all calibrated against this band ceiling. If the band
# ever rescales, change THIS constant and every formula stays
# proportional. Magic 100.0 / 24 / 30 / 90 literals scattered through
# scoring code would all need hand-tuning otherwise.
_MAX_SUGGESTION_SCORE = 100.0

# Multi-source consensus bonus: when a candidate is independently
# surfaced by N>1 buckets, add `_CONSENSUS_BONUS_FRACTION × _MAX × sqrt(N-1)`
# to the per-bucket score. Today (FRACTION=0.12, MAX=100):
#   2 buckets → +12, 3 → ~17, 4 → ~21, 5 → ~24
# Diminishing-returns curve so 5-bucket agreement saturates near 24%
# of the band — meaningful confirmation but not enough to overrun a
# strong single-bucket signal (e.g. lead author of a 5★). Bonus is
# expressed as a fraction so it stays calibrated if `_MAX` rescales.
_CONSENSUS_BONUS_FRACTION = 0.12


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


def _followed_author_ids(db: sqlite3.Connection) -> set[str]:
    try:
        ensure_followed_author_contract(db)
    except Exception:
        pass
    if not _table_exists(db, "followed_authors"):
        return set()
    try:
        rows = db.execute("SELECT author_id FROM followed_authors").fetchall()
    except sqlite3.OperationalError:
        return set()
    return {
        str((row["author_id"] if isinstance(row, sqlite3.Row) else row[0]) or "").strip()
        for row in rows
    }


def _effective_author_type(data: dict, followed_ids: set[str]) -> str:
    author_id = str(data.get("id") or "").strip()
    if author_id in followed_ids:
        return "followed"
    if author_id.startswith("import_author_"):
        return "background"
    current = str(data.get("author_type") or "").strip().lower()
    if current in {"followed", "background"}:
        return current
    return "background"


def _author_monitor_map(db: sqlite3.Connection) -> dict[str, dict]:
    try:
        monitors = monitor_app.list_feed_monitors(db)
    except Exception:
        return {}
    out: dict[str, dict] = {}
    for monitor in monitors:
        if str(monitor.get("monitor_type") or "") != "author":
            continue
        author_id = str(monitor.get("author_id") or "").strip()
        if not author_id:
            continue
        out[author_id] = monitor
    return out


def _normalize_author_monitor_fields(
    data: dict,
    *,
    followed_ids: set[str],
    monitor_by_author: dict[str, dict],
) -> None:
    author_id = str(data.get("id") or "").strip()
    if author_id not in followed_ids:
        data["monitor_health"] = None
        data["monitor_health_reason"] = None
        data["monitor_last_checked_at"] = None
        data["monitor_last_success_at"] = None
        data["monitor_last_status"] = None
        data["monitor_last_error"] = None
        data["monitor_last_result"] = None
        data["monitor_papers_found"] = None
        data["monitor_items_created"] = None
        return

    monitor = monitor_by_author.get(author_id)
    if monitor is None:
        data["monitor_health"] = "degraded"
        data["monitor_health_reason"] = "missing_author_monitor"
        data["monitor_last_checked_at"] = None
        data["monitor_last_success_at"] = None
        data["monitor_last_status"] = None
        data["monitor_last_error"] = None
        data["monitor_last_result"] = None
        data["monitor_papers_found"] = None
        data["monitor_items_created"] = None
        return

    last_result = monitor.get("last_result")
    if isinstance(last_result, str):
        try:
            last_result = json.loads(last_result)
        except Exception:
            last_result = None
    if not isinstance(last_result, dict):
        last_result = None

    papers_found = last_result.get("papers_found") if isinstance(last_result, dict) else None
    items_created = last_result.get("items_created") if isinstance(last_result, dict) else None

    data["monitor_health"] = monitor.get("health")
    data["monitor_health_reason"] = monitor.get("health_reason")
    data["monitor_last_checked_at"] = monitor.get("last_checked_at")
    data["monitor_last_success_at"] = monitor.get("last_success_at")
    data["monitor_last_status"] = monitor.get("last_status")
    data["monitor_last_error"] = monitor.get("last_error")
    data["monitor_last_result"] = last_result
    data["monitor_papers_found"] = int(papers_found) if isinstance(papers_found, (int, float)) else None
    data["monitor_items_created"] = int(items_created) if isinstance(items_created, (int, float)) else None


def _enrich_followed_author_corpus_fields(db: sqlite3.Connection, data: dict) -> None:
    author_id = str(data.get("id") or "").strip()
    if not author_id or str(data.get("author_type") or "") != "followed":
        return
    works_count = data.get("works_count")
    try:
        backfill = get_followed_author_backfill_status(
            db,
            author_id,
            works_count=int(works_count) if works_count is not None else None,
        )
    except Exception:
        return
    data["background_corpus_state"] = backfill.get("state")
    data["background_corpus_detail"] = backfill.get("detail")
    data["background_corpus_last_success_at"] = backfill.get("last_success_at")
    data["background_corpus_age_days"] = backfill.get("age_days")
    data["background_corpus_publications"] = backfill.get("background_publications")
    data["background_corpus_coverage_ratio"] = backfill.get("coverage_ratio")


def _count_publications_via_publication_authors(
    db: sqlite3.Connection,
    *,
    openalex_id: str,
    author_name: str,
) -> int:
    if not _table_exists(db, "publication_authors"):
        return 0
    pa_columns = _table_columns(db, "publication_authors")
    if "paper_id" not in pa_columns:
        return 0

    if openalex_id and "openalex_id" in pa_columns:
        row = db.execute(
            """
            SELECT COUNT(DISTINCT pa.paper_id) AS count
            FROM publication_authors pa
            WHERE pa.openalex_id = ?
            """,
            (openalex_id,),
        ).fetchone()
        if row:
            return int((row["count"] if isinstance(row, sqlite3.Row) else row[0]) or 0)

    if author_name and "display_name" in pa_columns:
        row = db.execute(
            """
            SELECT COUNT(DISTINCT pa.paper_id) AS count
            FROM publication_authors pa
            WHERE lower(trim(pa.display_name)) = lower(trim(?))
            """,
            (author_name,),
        ).fetchone()
        if row:
            return int((row["count"] if isinstance(row, sqlite3.Row) else row[0]) or 0)
    return 0


def _legacy_papers_where_clause(
    db: sqlite3.Connection,
    *,
    author_id: str,
    author_name: str,
) -> tuple[str, list[object]]:
    if not _table_exists(db, "papers"):
        return "", []
    paper_columns = _table_columns(db, "papers")
    where: list[str] = []
    params: list[object] = []

    if author_id and "author_id" in paper_columns:
        where.append("p.author_id = ?")
        params.append(author_id)
    if author_id and "added_from" in paper_columns:
        where.append("p.added_from = ?")
        params.append(author_id)
    if author_name and "authors" in paper_columns:
        where.append("lower(COALESCE(p.authors, '')) LIKE lower(?)")
        params.append(f"%{author_name}%")
    if author_name and "author" in paper_columns:
        where.append("lower(COALESCE(p.author, '')) LIKE lower(?)")
        params.append(f"%{author_name}%")

    return (" OR ".join(where), params) if where else ("", [])


def _count_publications_via_legacy_papers(
    db: sqlite3.Connection,
    *,
    author_id: str,
    author_name: str,
) -> int:
    where, params = _legacy_papers_where_clause(db, author_id=author_id, author_name=author_name)
    if not where:
        return 0
    row = db.execute(
        f"SELECT COUNT(DISTINCT p.id) AS count FROM papers p WHERE {where}",
        params,
    ).fetchone()
    return int((row["count"] if row else 0) or 0)


def get_author_publication_count(
    db: sqlite3.Connection,
    *,
    author_id: str,
    author_name: str,
    openalex_id: str,
) -> int:
    return _count_publications_via_publication_authors(
        db,
        openalex_id=openalex_id,
        author_name=author_name,
    ) or _count_publications_via_legacy_papers(
        db,
        author_id=author_id,
        author_name=author_name,
    )


def compute_author_signal(
    db: sqlite3.Connection,
    *,
    author_id: str,
    author_name: str,
    openalex_id: str,
) -> Optional[dict]:
    """Derive a "how much we like this author" signal from their local papers.

    The signal blends three components on a 0-100 scale:
      - library_ratio (40): fraction of known papers we saved to Library
      - rating_quality (40): mean rating of Library papers (1-5 → 0-1)
      - volume (20): number of Library papers, capped at 10

    Returns None if we have no local papers for this author yet. The caller
    is expected to render "no signal" in that case rather than a zero score.
    """
    if not _table_exists(db, "papers"):
        return None
    where, params = _author_paper_clause(
        db,
        author_id=author_id,
        author_name=author_name,
        openalex_id=openalex_id,
    )
    if not where:
        return None
    rows = db.execute(
        f"SELECT p.status AS status, p.rating AS rating FROM papers p WHERE {where}",
        params,
    ).fetchall()
    total = len(rows)
    if total == 0:
        return None

    library_rows = [r for r in rows if str(r["status"] or "").strip() == "library"]
    library_count = len(library_rows)
    ratings = [int(r["rating"] or 0) for r in library_rows if int(r["rating"] or 0) > 0]
    avg_rating = (sum(ratings) / len(ratings)) if ratings else 0.0

    library_ratio = library_count / total
    rating_component = ((avg_rating - 1) / 4) if avg_rating >= 1 else 0.0
    volume_component = min(library_count, 10) / 10

    composite = (
        library_ratio * 40.0
        + rating_component * 40.0
        + volume_component * 20.0
    )

    return {
        "score": round(composite, 1),
        "library_papers": library_count,
        "total_papers": total,
        "avg_rating": round(avg_rating, 2) if avg_rating else None,
    }


def _author_paper_clause(
    db: sqlite3.Connection,
    *,
    author_id: str,
    author_name: str,
    openalex_id: str,
) -> tuple[str, list[object]]:
    clauses: list[str] = []
    params: list[object] = []
    if _table_exists(db, "publication_authors"):
        pa_columns = _table_columns(db, "publication_authors")
        if openalex_id and "openalex_id" in pa_columns:
            clauses.append(
                """
                EXISTS (
                    SELECT 1
                    FROM publication_authors pa
                    WHERE pa.paper_id = p.id
                      AND lower(trim(pa.openalex_id)) = lower(trim(?))
                )
                """
            )
            params.append(openalex_id)
        if author_name and "display_name" in pa_columns:
            clauses.append(
                """
                EXISTS (
                    SELECT 1
                    FROM publication_authors pa
                    WHERE pa.paper_id = p.id
                      AND lower(trim(pa.display_name)) = lower(trim(?))
                )
                """
            )
            params.append(author_name)

    legacy_where, legacy_params = _legacy_papers_where_clause(
        db,
        author_id=author_id,
        author_name=author_name,
    )
    if legacy_where:
        clauses.append(f"({legacy_where})")
        params.extend(legacy_params)
    return (f"({' OR '.join(clauses)})", params) if clauses else ("", [])


def _scope_clause(scope: str) -> str:
    scope_value = str(scope or "all").strip().lower()
    if scope_value == "library":
        return "AND p.status = 'library'"
    if scope_value in {"background", "non_library"}:
        return "AND p.status <> 'library'"
    return ""


def list_author_publications(
    db: sqlite3.Connection,
    author_id: str,
    *,
    scope: str = "all",
    order: str = "citations",
    limit: int = 100,
    offset: int = 0,
) -> Optional[list[dict]]:
    author = db.execute(
        "SELECT id, name, openalex_id FROM authors WHERE id = ?",
        (author_id,),
    ).fetchone()
    if not author:
        return None

    author_name = str((author["name"] if isinstance(author, sqlite3.Row) else author[1]) or "").strip()
    openalex_id = str((author["openalex_id"] if isinstance(author, sqlite3.Row) else author[2]) or "").strip()

    clause, params = _author_paper_clause(
        db,
        author_id=author_id,
        author_name=author_name,
        openalex_id=openalex_id,
    )
    if not clause:
        return []

    order_value = str(order or "citations").strip().lower()
    if order_value == "recent":
        order_sql = (
            "COALESCE(p.publication_date, printf('%04d-01-01', COALESCE(p.year, 0)), "
            "COALESCE(p.added_at, p.created_at, '')) DESC, COALESCE(p.cited_by_count, 0) DESC"
        )
    else:
        order_sql = "COALESCE(p.cited_by_count, 0) DESC, COALESCE(p.publication_date, '') DESC, COALESCE(p.year, 0) DESC"
    rows = db.execute(
        f"""
        SELECT DISTINCT p.*
        FROM papers p
        WHERE {clause}
        {_scope_clause(scope)}
        ORDER BY {order_sql}
        LIMIT ? OFFSET ?
        """,
        [*params, limit, offset],
    ).fetchall()
    return [dict(row) for row in rows]


def list_authors(
    db: sqlite3.Connection,
    *,
    search: Optional[str] = None,
    limit: int = 100,
    offset: int = 0,
) -> tuple[list[dict], int]:
    """List authors with optional name filtering."""
    where = ["1=1"]
    params: list[object] = []
    if search and search.strip():
        where.append("LOWER(name) LIKE LOWER(?)")
        params.append(f"%{search.strip()}%")

    rows = db.execute(
        f"""
        SELECT
            name, id, openalex_id, orcid, scholar_id, affiliation, email_domain,
            citedby, h_index, interests, url_picture, works_count,
            last_fetched_at, added_at, cited_by_year, institutions, author_type,
            id_resolution_status, id_resolution_reason, id_resolution_updated_at,
            id_resolution_method, id_resolution_confidence
        FROM authors
        WHERE {" AND ".join(where)}
        ORDER BY name
        LIMIT ? OFFSET ?
        """,
        [*params, limit, offset],
    ).fetchall()
    total_row = db.execute(
        f"SELECT COUNT(*) AS c FROM authors WHERE {' AND '.join(where)}",
        params,
    ).fetchone()
    total = int((total_row["c"] if total_row else 0) or 0)

    followed_ids = _followed_author_ids(db)
    monitor_by_author = _author_monitor_map(db)
    out: list[dict] = []
    for row in rows:
        data = dict(row)
        data["publication_count"] = get_author_publication_count(
            db,
            author_id=str(data.get("id") or "").strip(),
            author_name=str(data.get("name") or "").strip(),
            openalex_id=str(data.get("openalex_id") or "").strip(),
        )
        data["author_type"] = _effective_author_type(data, followed_ids)
        _normalize_author_monitor_fields(
            data,
            followed_ids=followed_ids,
            monitor_by_author=monitor_by_author,
        )
        _enrich_followed_author_corpus_fields(db, data)
        out.append(data)
    return out, total


def lookup_author_by_name(db: sqlite3.Connection, name: str) -> Optional[dict]:
    """Return a compact author record matched by normalized display name.

    Used by the author-name hover preview on paper cards. The match is
    case-insensitive and whitespace-tolerant; the first row wins when the
    authors table has multiple rows sharing a name (e.g. identity-resolution
    duplicates), which is acceptable for a preview surface.
    """
    n = (name or "").strip()
    if not n:
        return None
    row = db.execute(
        """
        SELECT
            name, id, openalex_id, orcid, scholar_id, affiliation, email_domain,
            citedby, h_index, interests, url_picture, works_count,
            last_fetched_at, added_at, cited_by_year, institutions, author_type,
            id_resolution_status, id_resolution_reason, id_resolution_updated_at,
            id_resolution_method, id_resolution_confidence
        FROM authors
        WHERE lower(trim(name)) = lower(trim(?))
        ORDER BY COALESCE(citedby, 0) DESC, COALESCE(works_count, 0) DESC
        LIMIT 1
        """,
        (n,),
    ).fetchone()
    if not row:
        return None
    data = dict(row)
    data["publication_count"] = get_author_publication_count(
        db,
        author_id=str(data.get("id") or "").strip(),
        author_name=str(data.get("name") or "").strip(),
        openalex_id=str(data.get("openalex_id") or "").strip(),
    )
    followed_ids = _followed_author_ids(db)
    data["author_type"] = _effective_author_type(data, followed_ids)
    _normalize_author_monitor_fields(
        data,
        followed_ids=followed_ids,
        monitor_by_author=_author_monitor_map(db),
    )
    _enrich_followed_author_corpus_fields(db, data)
    return data


def get_author(db: sqlite3.Connection, author_id: str) -> Optional[dict]:
    """Get one author with publication count."""
    row = db.execute(
        """
        SELECT
            name, id, openalex_id, orcid, scholar_id, affiliation, email_domain,
            citedby, h_index, interests, url_picture, works_count,
            last_fetched_at, added_at, cited_by_year, institutions, author_type,
            id_resolution_status, id_resolution_reason, id_resolution_updated_at,
            id_resolution_method, id_resolution_confidence
        FROM authors
        WHERE id = ?
        """,
        (author_id,),
    ).fetchone()
    if not row:
        return None
    data = dict(row)
    data["publication_count"] = get_author_publication_count(
        db,
        author_id=str(data.get("id") or "").strip(),
        author_name=str(data.get("name") or "").strip(),
        openalex_id=str(data.get("openalex_id") or "").strip(),
    )
    followed_ids = _followed_author_ids(db)
    data["author_type"] = _effective_author_type(data, followed_ids)
    _normalize_author_monitor_fields(
        data,
        followed_ids=followed_ids,
        monitor_by_author=_author_monitor_map(db),
    )
    _enrich_followed_author_corpus_fields(db, data)
    return data


def get_author_detail(db: sqlite3.Connection, author_id: str) -> Optional[dict]:
    """Lightweight detail bundle for the author popup.

    Returns profile + signal + top_topics + followed-author backfill state in
    one round-trip. Excludes the heavier publications / history / collaborators
    lists — those remain on ``/authors/{id}/dossier`` and ``/publications`` for
    callers that need them.
    """
    author = get_author(db, author_id)
    if author is None:
        return None

    author_name = str(author.get("name") or "").strip()
    openalex_id = str(author.get("openalex_id") or "").strip()

    signal = compute_author_signal(
        db,
        author_id=author_id,
        author_name=author_name,
        openalex_id=openalex_id,
    )

    top_topics: list[dict] = []
    if _table_exists(db, "publication_topics"):
        clause, params = _author_paper_clause(
            db,
            author_id=author_id,
            author_name=author_name,
            openalex_id=openalex_id,
        )
        if clause:
            try:
                topic_rows = db.execute(
                    f"""
                    SELECT COALESCE(t.canonical_name, pt.term, '') AS term,
                           COUNT(DISTINCT pt.paper_id) AS papers
                    FROM papers p
                    JOIN publication_topics pt ON pt.paper_id = p.id
                    LEFT JOIN topics t ON t.topic_id = pt.topic_id
                    WHERE {clause}
                      AND COALESCE(TRIM(pt.term), '') <> ''
                    GROUP BY COALESCE(t.canonical_name, pt.term, '')
                    ORDER BY papers DESC, term ASC
                    LIMIT 8
                    """,
                    params,
                ).fetchall()
                top_topics = [
                    {"term": str(row["term"] or "").strip(), "papers": int(row["papers"] or 0)}
                    for row in topic_rows
                    if str(row["term"] or "").strip()
                ]
            except sqlite3.OperationalError:
                top_topics = []

    backfill = None
    if str(author.get("author_type") or "") == "followed":
        backfill = get_followed_author_backfill_status(
            db,
            author_id,
            works_count=int(author.get("works_count") or 0) if author.get("works_count") is not None else None,
        )

    return {
        "author": author,
        "signal": signal,
        "top_topics": top_topics,
        "backfill": backfill,
    }


def get_author_dossier(db: sqlite3.Connection, author_id: str) -> Optional[dict]:
    author = get_author(db, author_id)
    if author is None:
        return None

    author_name = str(author.get("name") or "").strip()
    openalex_id = str(author.get("openalex_id") or "").strip()
    clause, params = _author_paper_clause(
        db,
        author_id=author_id,
        author_name=author_name,
        openalex_id=openalex_id,
    )
    if not clause:
        backfill = get_followed_author_backfill_status(
            db,
            author_id,
            works_count=int(author.get("works_count") or 0) if author.get("works_count") is not None else None,
        ) if str(author.get("author_type") or "") == "followed" else None
        return {
            "author": author,
            "summary": {
                "total_publications": 0,
                "library_publications": 0,
                "background_publications": 0,
                "first_year": None,
                "latest_year": None,
                "tracked_corpus_ready": False,
                "tracked_corpus_state": (backfill or {}).get("state") if backfill else "not_followed",
                "background_coverage_ratio": (backfill or {}).get("coverage_ratio") if backfill else None,
            },
            "history": [],
            "top_topics": [],
            "top_venues": [],
            "top_collaborators": [],
            "recent_publications": [],
            "background_publications": [],
            "recommended_actions": [],
            "backfill": backfill,
        }

    summary_row = db.execute(
        f"""
        SELECT
            COUNT(DISTINCT p.id) AS total_publications,
            COALESCE(SUM(CASE WHEN p.status = 'library' THEN 1 ELSE 0 END), 0) AS library_publications,
            COALESCE(SUM(CASE WHEN p.status <> 'library' THEN 1 ELSE 0 END), 0) AS background_publications,
            MIN(p.year) AS first_year,
            MAX(p.year) AS latest_year
        FROM papers p
        WHERE {clause}
        """,
        params,
    ).fetchone()

    history_rows = db.execute(
        f"""
        SELECT p.year, COUNT(DISTINCT p.id) AS count
        FROM papers p
        WHERE {clause}
          AND p.year IS NOT NULL
        GROUP BY p.year
        ORDER BY p.year ASC
        """,
        params,
    ).fetchall()

    top_topics: list[dict] = []
    if _table_exists(db, "publication_topics"):
        try:
            topic_rows = db.execute(
                f"""
                SELECT COALESCE(t.canonical_name, pt.term, '') AS term, COUNT(DISTINCT pt.paper_id) AS papers
                FROM papers p
                JOIN publication_topics pt ON pt.paper_id = p.id
                LEFT JOIN topics t ON t.topic_id = pt.topic_id
                WHERE {clause}
                  AND COALESCE(TRIM(pt.term), '') <> ''
                GROUP BY COALESCE(t.canonical_name, pt.term, '')
                ORDER BY papers DESC, term ASC
                LIMIT 8
                """,
                params,
            ).fetchall()
            top_topics = [
                {"term": str(row["term"] or "").strip(), "papers": int(row["papers"] or 0)}
                for row in topic_rows
                if str(row["term"] or "").strip()
            ]
        except sqlite3.OperationalError:
            top_topics = []

    top_venues_rows = db.execute(
        f"""
        SELECT p.journal, COUNT(DISTINCT p.id) AS papers
        FROM papers p
        WHERE {clause}
          AND COALESCE(TRIM(p.journal), '') <> ''
        GROUP BY lower(trim(p.journal)), p.journal
        ORDER BY papers DESC, p.journal ASC
        LIMIT 8
        """,
        params,
    ).fetchall()
    top_venues = [
        {"journal": str(row["journal"] or "").strip(), "papers": int(row["papers"] or 0)}
        for row in top_venues_rows
        if str(row["journal"] or "").strip()
    ]

    top_collaborators: list[dict] = []
    if _table_exists(db, "publication_authors") and openalex_id:
        try:
            collaborator_rows = db.execute(
                """
                SELECT
                    COALESCE(pa2.display_name, pa2.openalex_id, '') AS name,
                    pa2.openalex_id,
                    COUNT(DISTINCT pa.paper_id) AS shared_papers
                FROM publication_authors pa
                JOIN publication_authors pa2 ON pa2.paper_id = pa.paper_id
                WHERE lower(pa.openalex_id) = lower(trim(?))
                  AND lower(trim(COALESCE(pa2.openalex_id, ''))) <> lower(trim(?))
                  AND COALESCE(TRIM(COALESCE(pa2.display_name, pa2.openalex_id, '')), '') <> ''
                GROUP BY COALESCE(pa2.display_name, pa2.openalex_id, ''), pa2.openalex_id
                ORDER BY shared_papers DESC, name ASC
                LIMIT 8
                """,
                (openalex_id, openalex_id),
            ).fetchall()
            top_collaborators = [
                {
                    "name": str(row["name"] or "").strip(),
                    "openalex_id": str(row["openalex_id"] or "").strip() or None,
                    "shared_papers": int(row["shared_papers"] or 0),
                }
                for row in collaborator_rows
                if str(row["name"] or "").strip()
            ]
        except sqlite3.OperationalError:
            top_collaborators = []

    recent_publications = list_author_publications(
        db,
        author_id,
        scope="all",
        order="recent",
        limit=8,
        offset=0,
    ) or []
    background_publications = list_author_publications(
        db,
        author_id,
        scope="background",
        order="recent",
        limit=8,
        offset=0,
    ) or []

    background_count = int((summary_row["background_publications"] if summary_row else 0) or 0)
    summary = {
        "total_publications": int((summary_row["total_publications"] if summary_row else 0) or 0),
        "library_publications": int((summary_row["library_publications"] if summary_row else 0) or 0),
        "background_publications": background_count,
        "first_year": int(summary_row["first_year"]) if summary_row and summary_row["first_year"] is not None else None,
        "latest_year": int(summary_row["latest_year"]) if summary_row and summary_row["latest_year"] is not None else None,
        "tracked_corpus_ready": background_count > 0,
    }
    backfill = get_followed_author_backfill_status(
        db,
        author_id,
        background_publications=background_count,
        works_count=int(author.get("works_count") or 0) if author.get("works_count") is not None else None,
    ) if str(author.get("author_type") or "") == "followed" else None
    summary["tracked_corpus_state"] = (backfill or {}).get("state") if backfill else "not_followed"
    summary["background_coverage_ratio"] = (backfill or {}).get("coverage_ratio") if backfill else None

    recommended_actions: list[dict] = []
    if str(author.get("author_type") or "") == "followed" and not summary["tracked_corpus_ready"]:
        recommended_actions.append(
            {
                "id": "history_backfill",
                "label": "Run historical backfill",
                "detail": "This followed author still has little or no non-library background corpus cached.",
            }
        )
    if backfill and str(backfill.get("state") or "") in {"stale", "thin", "pending", "failed", "unverified"}:
        recommended_actions.append(
            {
                "id": "maintain_background_corpus",
                "label": "Refresh historical corpus",
                "detail": str(backfill.get("detail") or "This followed author needs a stronger historical corpus refresh."),
            }
        )
    if background_count > summary["library_publications"]:
        recommended_actions.append(
            {
                "id": "review_background_corpus",
                "label": "Review non-library papers",
                "detail": "Most known papers for this author still live outside your curated Library.",
            }
        )
    if top_collaborators:
        recommended_actions.append(
            {
                "id": "monitor_collaborators",
                "label": "Operationalize collaborator graph",
                "detail": "This author already has enough collaborator structure to expand monitoring or alerts.",
            }
        )

    return {
        "author": author,
        "summary": summary,
        "history": [
            {"year": int(row["year"]), "count": int(row["count"] or 0)}
            for row in history_rows
            if row["year"] is not None
        ],
        "top_topics": top_topics,
        "top_venues": top_venues,
        "top_collaborators": top_collaborators,
        "recent_publications": recent_publications,
        "background_publications": background_publications,
        "recommended_actions": recommended_actions,
        "backfill": backfill,
    }


_AUTHOR_NAME_NOISE_RE = re.compile(r"[^\w\s]+", re.UNICODE)


def _normalize_author_display_name(name: str) -> str:
    """Canonical form of a display name for same-human dedup.

    Lowercase + Unicode-NFKD-fold (so "Müller" and "Muller" collide
    when the source mixes them) + strip every non-word/space char +
    collapse whitespace. Returns empty string for unparseable input
    so the caller can fall back to the raw row.

    Conservatively does NOT collapse initials or strip middle names —
    "J. Smith" and "John Smith" stay distinct because false collapses
    are worse than visible duplicates: the user can dismiss a dupe,
    but cannot recover a wrongly-merged distinct author.

    Examples:
      "Olivier Collignon"   → "olivier collignon"
      "OLIVIER COLLIGNON"   → "olivier collignon"
      "Olivier  Collignon," → "olivier collignon"
      "Müller, J."          → "muller j"   (NOT merged with "Mueller, J.")
    """
    raw = str(name or "").strip()
    if not raw:
        return ""
    folded = unicodedata.normalize("NFKD", raw)
    # Drop combining marks (accents) so "Müller" → "Muller".
    folded = "".join(c for c in folded if not unicodedata.combining(c))
    cleaned = _AUTHOR_NAME_NOISE_RE.sub(" ", folded.lower())
    return " ".join(cleaned.split())


def _normalize_openalex_id(value: str) -> str:
    text = str(value or "").strip()
    if not text:
        return ""
    normalized = _normalize_oaid(text)
    return str(normalized or text).strip()


def _existing_author_lookup(db: sqlite3.Connection) -> dict[str, dict]:
    """Map lowercased OpenAlex ID → {id, name, author_type, orcid}.

    `orcid` is included so the suggestion-rail filter can detect
    "this candidate's OpenAlex ID is new to us, but the human behind
    it is already in followed_authors via a different alias" without
    a second query per candidate.
    """
    if not _table_exists(db, "authors"):
        return {}
    rows = db.execute(
        """
        SELECT id, name, openalex_id, author_type, orcid
        FROM authors
        WHERE openalex_id IS NOT NULL AND TRIM(openalex_id) <> ''
        """
    ).fetchall()
    out: dict[str, dict] = {}
    for row in rows:
        openalex_id = _normalize_openalex_id(
            row["openalex_id"] if isinstance(row, sqlite3.Row) else row[2]
        )
        if not openalex_id:
            continue
        out[openalex_id.lower()] = {
            "id": row["id"] if isinstance(row, sqlite3.Row) else row[0],
            "name": row["name"] if isinstance(row, sqlite3.Row) else row[1],
            "author_type": row["author_type"] if isinstance(row, sqlite3.Row) else row[3],
            "orcid": row["orcid"] if isinstance(row, sqlite3.Row) else row[4],
        }
    return out


def _sample_titles_for_openalex_author(
    db: sqlite3.Connection,
    openalex_id: str,
    *,
    limit: int = 3,
    topic_whitelist: Optional[set[str]] = None,
    venue_whitelist: Optional[set[str]] = None,
) -> list[str]:
    if not _table_exists(db, "publication_authors"):
        return []
    normalized = _normalize_openalex_id(openalex_id)
    if not normalized:
        return []
    rows = db.execute(
        """
        SELECT DISTINCT p.id, p.title, COALESCE(p.cited_by_count, 0) AS cited_by_count, COALESCE(p.year, 0) AS year
        FROM publication_authors pa
        JOIN papers p ON p.id = pa.paper_id
        WHERE lower(pa.openalex_id) = lower(trim(?))
          AND COALESCE(trim(p.title), '') <> ''
        ORDER BY COALESCE(p.cited_by_count, 0) DESC, COALESCE(p.year, 0) DESC
        LIMIT ?
        """,
        (normalized, max(1, limit * 4)),
    ).fetchall()
    if not rows:
        return []

    allowed_paper_ids: Optional[set[str]] = None
    if topic_whitelist and _table_exists(db, "publication_topics"):
        topic_rows = db.execute(
            """
            SELECT DISTINCT pt.paper_id
            FROM publication_topics pt
            JOIN publication_authors pa ON pa.paper_id = pt.paper_id
            WHERE lower(pa.openalex_id) = lower(trim(?))
              AND pt.term IS NOT NULL
            """,
            (normalized,),
        ).fetchall()
        paper_to_topics: dict[str, set[str]] = {}
        for row in topic_rows:
            paper_id = str(row["paper_id"] if isinstance(row, sqlite3.Row) else row[0])
            paper_to_topics.setdefault(paper_id, set())
        if paper_to_topics:
            topic_rows = db.execute(
                """
                SELECT pt.paper_id, pt.term
                FROM publication_topics pt
                JOIN publication_authors pa ON pa.paper_id = pt.paper_id
                WHERE lower(pa.openalex_id) = lower(trim(?))
                  AND pt.term IS NOT NULL
                """,
                (normalized,),
            ).fetchall()
            for row in topic_rows:
                paper_id = str(row["paper_id"] if isinstance(row, sqlite3.Row) else row[0])
                term = str(row["term"] if isinstance(row, sqlite3.Row) else row[1]).strip().lower()
                if term:
                    paper_to_topics.setdefault(paper_id, set()).add(term)
            allowed_paper_ids = {
                paper_id
                for paper_id, terms in paper_to_topics.items()
                if terms.intersection(topic_whitelist)
            }

    if venue_whitelist:
        venue_rows = db.execute(
            """
            SELECT DISTINCT p.id
            FROM publication_authors pa
            JOIN papers p ON p.id = pa.paper_id
            WHERE lower(pa.openalex_id) = lower(trim(?))
              AND lower(trim(COALESCE(p.journal, ''))) IN ({placeholders})
            """.format(placeholders=", ".join("?" for _ in venue_whitelist)),
            [normalized, *sorted(venue_whitelist)],
        ).fetchall()
        venue_paper_ids = {
            str(row["id"] if isinstance(row, sqlite3.Row) else row[0])
            for row in venue_rows
        }
        if allowed_paper_ids is None:
            allowed_paper_ids = venue_paper_ids
        else:
            allowed_paper_ids = allowed_paper_ids.intersection(venue_paper_ids) or allowed_paper_ids

    titles: list[str] = []
    for row in rows:
        paper_id = str(row["id"] if isinstance(row, sqlite3.Row) else row[0]).strip()
        if allowed_paper_ids is not None and paper_id not in allowed_paper_ids:
            continue
        title = str(row["title"] if isinstance(row, sqlite3.Row) else row[1]).strip()
        if not title:
            continue
        if title not in titles:
            titles.append(title)
        if len(titles) >= limit:
            break
    return titles


def _top_topics_for_followed_authors(db: sqlite3.Connection, *, limit: int = 12) -> dict[str, float]:
    if not _table_exists(db, "publication_topics") or not _table_exists(db, "publication_authors"):
        return {}
    try:
        rows = db.execute(
            """
            SELECT pt.term, COUNT(DISTINCT pt.paper_id) AS paper_count
            FROM publication_topics pt
            JOIN publication_authors pa ON pa.paper_id = pt.paper_id
            JOIN authors a ON lower(a.openalex_id) = lower(pa.openalex_id)
            JOIN followed_authors fa ON fa.author_id = a.id
            WHERE pt.term IS NOT NULL AND TRIM(pt.term) <> ''
            GROUP BY pt.term
            ORDER BY paper_count DESC, pt.term ASC
            LIMIT ?
            """,
            (max(4, limit),),
        ).fetchall()
    except sqlite3.OperationalError:
        return {}
    out: dict[str, float] = {}
    for row in rows:
        term = str(row["term"] if isinstance(row, sqlite3.Row) else row[0]).strip().lower()
        count = float(row["paper_count"] if isinstance(row, sqlite3.Row) else row[1] or 0.0)
        if term:
            out[term] = count
    return out


def _top_venues_for_followed_authors(db: sqlite3.Connection, *, limit: int = 8) -> dict[str, float]:
    if not _table_exists(db, "publication_authors"):
        return {}
    try:
        rows = db.execute(
            """
            SELECT p.journal, COUNT(DISTINCT p.id) AS paper_count
            FROM papers p
            JOIN publication_authors pa ON pa.paper_id = p.id
            JOIN authors a ON lower(a.openalex_id) = lower(pa.openalex_id)
            JOIN followed_authors fa ON fa.author_id = a.id
            WHERE COALESCE(TRIM(p.journal), '') <> ''
            GROUP BY lower(trim(p.journal)), p.journal
            ORDER BY paper_count DESC, p.journal ASC
            LIMIT ?
            """,
            (max(3, limit),),
        ).fetchall()
    except sqlite3.OperationalError:
        return {}
    out: dict[str, float] = {}
    for row in rows:
        journal = str(row["journal"] if isinstance(row, sqlite3.Row) else row[0]).strip()
        count = float(row["paper_count"] if isinstance(row, sqlite3.Row) else row[1] or 0.0)
        if journal:
            out[journal.lower()] = count
    return out


def _shared_followed_authors_for_candidate(
    db: sqlite3.Connection,
    openalex_id: str,
    *,
    limit: int = 3,
) -> list[str]:
    if not _table_exists(db, "publication_authors"):
        return []
    rows = db.execute(
        """
        SELECT a.name, COUNT(DISTINCT pa.paper_id) AS shared_papers
        FROM publication_authors pa
        JOIN publication_authors candidate_pa ON candidate_pa.paper_id = pa.paper_id
        JOIN authors a ON lower(a.openalex_id) = lower(pa.openalex_id)
        JOIN followed_authors fa ON fa.author_id = a.id
        WHERE lower(trim(candidate_pa.openalex_id)) = lower(trim(?))
          AND lower(trim(pa.openalex_id)) <> lower(trim(candidate_pa.openalex_id))
        GROUP BY a.name
        ORDER BY shared_papers DESC, a.name ASC
        LIMIT ?
        """,
        (_normalize_openalex_id(openalex_id), max(1, limit)),
    ).fetchall()
    return [
        str(row["name"] if isinstance(row, sqlite3.Row) else row[0]).strip()
        for row in rows
        if str(row["name"] if isinstance(row, sqlite3.Row) else row[0]).strip()
    ]


def _shared_topics_for_candidate(
    db: sqlite3.Connection,
    openalex_id: str,
    followed_topics: set[str],
    *,
    limit: int = 3,
) -> list[str]:
    if not followed_topics or not _table_exists(db, "publication_topics") or not _table_exists(db, "publication_authors"):
        return []
    rows = db.execute(
        """
        SELECT pt.term, COUNT(DISTINCT pt.paper_id) AS paper_count
        FROM publication_topics pt
        JOIN publication_authors pa ON pa.paper_id = pt.paper_id
        WHERE lower(pa.openalex_id) = lower(trim(?))
          AND pt.term IS NOT NULL
        GROUP BY pt.term
        ORDER BY paper_count DESC, pt.term ASC
        """,
        (_normalize_openalex_id(openalex_id),),
    ).fetchall()
    out: list[str] = []
    for row in rows:
        term = str(row["term"] if isinstance(row, sqlite3.Row) else row[0]).strip()
        if term and term.lower() in followed_topics and term not in out:
            out.append(term)
        if len(out) >= limit:
            break
    return out


def _shared_venues_for_candidate(
    db: sqlite3.Connection,
    openalex_id: str,
    followed_venues: set[str],
    *,
    limit: int = 3,
) -> list[str]:
    if not followed_venues or not _table_exists(db, "publication_authors"):
        return []
    rows = db.execute(
        """
        SELECT p.journal, COUNT(DISTINCT p.id) AS paper_count
        FROM papers p
        JOIN publication_authors pa ON pa.paper_id = p.id
        WHERE lower(pa.openalex_id) = lower(trim(?))
          AND COALESCE(TRIM(p.journal), '') <> ''
        GROUP BY lower(trim(p.journal)), p.journal
        ORDER BY paper_count DESC, p.journal ASC
        """,
        (_normalize_openalex_id(openalex_id),),
    ).fetchall()
    out: list[str] = []
    for row in rows:
        venue = str(row["journal"] if isinstance(row, sqlite3.Row) else row[0]).strip()
        if venue and venue.lower() in followed_venues and venue not in out:
            out.append(venue)
        if len(out) >= limit:
            break
    return out


def _top_topics_for_library(db: sqlite3.Connection, *, limit: int = 12) -> dict[str, float]:
    if not _table_exists(db, "publication_topics") or not _table_exists(db, "publication_authors"):
        return {}
    try:
        rows = db.execute(
            """
            SELECT pt.term, COUNT(DISTINCT pt.paper_id) AS paper_count
            FROM publication_topics pt
            JOIN papers p ON p.id = pt.paper_id
            WHERE p.status = 'library'
              AND pt.term IS NOT NULL
              AND TRIM(pt.term) <> ''
            GROUP BY pt.term
            ORDER BY paper_count DESC, pt.term ASC
            LIMIT ?
            """,
            (max(4, limit),),
        ).fetchall()
    except sqlite3.OperationalError:
        return {}
    out: dict[str, float] = {}
    for row in rows:
        term = str(row["term"] if isinstance(row, sqlite3.Row) else row[0]).strip().lower()
        count = float(row["paper_count"] if isinstance(row, sqlite3.Row) else row[1] or 0.0)
        if term:
            out[term] = count
    return out


def _top_venues_for_library(db: sqlite3.Connection, *, limit: int = 8) -> dict[str, float]:
    if not _table_exists(db, "publication_authors"):
        return {}
    try:
        rows = db.execute(
            """
            SELECT p.journal, COUNT(DISTINCT p.id) AS paper_count
            FROM papers p
            WHERE p.status = 'library'
              AND COALESCE(TRIM(p.journal), '') <> ''
            GROUP BY lower(trim(p.journal)), p.journal
            ORDER BY paper_count DESC, p.journal ASC
            LIMIT ?
            """,
            (max(3, limit),),
        ).fetchall()
    except sqlite3.OperationalError:
        return {}
    out: dict[str, float] = {}
    for row in rows:
        journal = str(row["journal"] if isinstance(row, sqlite3.Row) else row[0]).strip()
        count = float(row["paper_count"] if isinstance(row, sqlite3.Row) else row[1] or 0.0)
        if journal:
            out[journal.lower()] = count
    return out


def _load_dismissal_signature(
    db: sqlite3.Connection, *, lookback_days: int = 100, limit: int = 20
) -> tuple[dict[str, int], dict[str, int], dict[str, int], dict[str, int]]:
    """Return (topic, venue, coauthor, institution) signatures for dismissed authors.

    `get_missing_author_feedback_state` already suppresses a dismissed
    author from re-surfacing. That's necessary but not sufficient:
    when the user dismisses three authors who all work on
    "neuroimaging", that's a learned negative signal about the
    *cluster*, not just those three IDs. Without propagation a
    fourth neuroimaging-heavy author appears at the top of the rail
    next refresh and the user has to dismiss them too.

    The signature is multi-dimensional because dismissal reasons are
    multi-dimensional. Topics+venues catch "this kind of research";
    coauthors catch "this collaboration cluster" (a candidate who
    co-publishes with dismissed authors is almost certainly inside
    the cluster the user is rejecting); institutions catch "this lab"
    (a common reason to dismiss is institution-level — a particular
    lab's methodology you don't trust — which topic/venue alone
    miss).

    Returns four dicts keyed by lower-cased label:
      - topics:       {topic:        dismissed_author_count}
      - venues:       {venue:        dismissed_author_count}
      - coauthors:    {coauthor_oid: shared_paper_count}
                      ↑ paper count is the better signal here than
                        dismissed-author count, because collaboration
                        depth (5 papers with 1 dismissed author) is
                        more meaningful than breadth (1 paper each
                        with 5 dismissed authors who happened to
                        share a venue).
      - institutions: {institution:  dismissed_author_count}

    Any dict can be empty (no dismissals yet or schema missing) — the
    caller must tolerate empties without computing a penalty.
    """
    if not (
        _table_exists(db, "missing_author_feedback")
        and _table_exists(db, "publication_authors")
    ):
        return {}, {}, {}, {}
    cutoff = (datetime.utcnow() - timedelta(days=lookback_days)).isoformat()
    topics: dict[str, int] = {}
    venues: dict[str, int] = {}
    coauthors: dict[str, int] = {}
    institutions: dict[str, int] = {}
    try:
        rows = db.execute(
            """
            WITH dismissed AS (
                SELECT DISTINCT lower(openalex_id) AS oid
                FROM missing_author_feedback
                WHERE signal_value < 0
                  AND created_at >= ?
                  AND COALESCE(TRIM(openalex_id), '') <> ''
            )
            SELECT lower(trim(pt.term)) AS term,
                   COUNT(DISTINCT d.oid) AS dismissed_author_count
            FROM dismissed d
            JOIN publication_authors pa ON lower(pa.openalex_id) = d.oid
            JOIN publication_topics pt ON pt.paper_id = pa.paper_id
            WHERE pt.term IS NOT NULL AND TRIM(pt.term) <> ''
            GROUP BY lower(trim(pt.term))
            HAVING dismissed_author_count >= 1
            ORDER BY dismissed_author_count DESC, term ASC
            LIMIT ?
            """,
            (cutoff, max(4, limit)),
        ).fetchall()
        for row in rows:
            term = str(row["term"] if isinstance(row, sqlite3.Row) else row[0]).strip()
            n = int(row["dismissed_author_count"] if isinstance(row, sqlite3.Row) else row[1] or 0)
            if term and n > 0:
                topics[term] = n
    except sqlite3.OperationalError:
        # publication_topics absent on a fresh schema is fine.
        pass
    try:
        rows = db.execute(
            """
            WITH dismissed AS (
                SELECT DISTINCT lower(openalex_id) AS oid
                FROM missing_author_feedback
                WHERE signal_value < 0
                  AND created_at >= ?
                  AND COALESCE(TRIM(openalex_id), '') <> ''
            )
            SELECT lower(trim(p.journal)) AS venue_key,
                   COUNT(DISTINCT d.oid) AS dismissed_author_count
            FROM dismissed d
            JOIN publication_authors pa ON lower(pa.openalex_id) = d.oid
            JOIN papers p ON p.id = pa.paper_id
            WHERE COALESCE(TRIM(p.journal), '') <> ''
            GROUP BY lower(trim(p.journal))
            HAVING dismissed_author_count >= 1
            ORDER BY dismissed_author_count DESC, venue_key ASC
            LIMIT ?
            """,
            (cutoff, max(4, limit)),
        ).fetchall()
        for row in rows:
            venue = str(row["venue_key"] if isinstance(row, sqlite3.Row) else row[0]).strip()
            n = int(row["dismissed_author_count"] if isinstance(row, sqlite3.Row) else row[1] or 0)
            if venue and n > 0:
                venues[venue] = n
    except sqlite3.OperationalError:
        pass
    try:
        # Coauthors of dismissed authors. The self-join finds every
        # other author on the dismissed author's papers; we count
        # SHARED PAPERS (not distinct dismissed-author count) because
        # collaboration depth is the relevant signal — a candidate
        # appearing on 8 papers with one dismissed author is more
        # cluster-bound than one appearing on 1 paper each with 3
        # dismissed authors who happen to overlap on a single venue.
        rows = db.execute(
            """
            WITH dismissed AS (
                SELECT DISTINCT lower(openalex_id) AS oid
                FROM missing_author_feedback
                WHERE signal_value < 0
                  AND created_at >= ?
                  AND COALESCE(TRIM(openalex_id), '') <> ''
            )
            SELECT lower(trim(pa2.openalex_id)) AS coauthor_oid,
                   COUNT(DISTINCT pa.paper_id) AS shared_paper_count
            FROM dismissed d
            JOIN publication_authors pa  ON lower(pa.openalex_id)  = d.oid
            JOIN publication_authors pa2 ON pa2.paper_id = pa.paper_id
            WHERE COALESCE(TRIM(pa2.openalex_id), '') <> ''
              AND lower(trim(pa2.openalex_id)) <> d.oid
            GROUP BY lower(trim(pa2.openalex_id))
            HAVING shared_paper_count >= 1
            ORDER BY shared_paper_count DESC, coauthor_oid ASC
            LIMIT ?
            """,
            (cutoff, max(8, limit * 2)),
        ).fetchall()
        for row in rows:
            oid = str(row["coauthor_oid"] if isinstance(row, sqlite3.Row) else row[0]).strip()
            n = int(row["shared_paper_count"] if isinstance(row, sqlite3.Row) else row[1] or 0)
            if oid and n > 0:
                coauthors[oid] = n
    except sqlite3.OperationalError:
        pass
    try:
        # Institutions reuse `publication_authors.institution` (already
        # populated by the OpenAlex enrichment path — see the schema
        # in `library/enrichment.py`). We count distinct dismissed
        # authors per institution rather than papers because the
        # institution attaches to the author × paper row, so paper
        # count would over-count an author's many papers from one lab.
        rows = db.execute(
            """
            WITH dismissed AS (
                SELECT DISTINCT lower(openalex_id) AS oid
                FROM missing_author_feedback
                WHERE signal_value < 0
                  AND created_at >= ?
                  AND COALESCE(TRIM(openalex_id), '') <> ''
            )
            SELECT lower(trim(pa.institution)) AS institution_key,
                   COUNT(DISTINCT d.oid) AS dismissed_author_count
            FROM dismissed d
            JOIN publication_authors pa ON lower(pa.openalex_id) = d.oid
            WHERE COALESCE(TRIM(pa.institution), '') <> ''
            GROUP BY lower(trim(pa.institution))
            HAVING dismissed_author_count >= 1
            ORDER BY dismissed_author_count DESC, institution_key ASC
            LIMIT ?
            """,
            (cutoff, max(4, limit)),
        ).fetchall()
        for row in rows:
            inst = str(row["institution_key"] if isinstance(row, sqlite3.Row) else row[0]).strip()
            n = int(row["dismissed_author_count"] if isinstance(row, sqlite3.Row) else row[1] or 0)
            if inst and n > 0:
                institutions[inst] = n
    except sqlite3.OperationalError:
        pass
    return topics, venues, coauthors, institutions


def _candidate_top_institutions(
    db: sqlite3.Connection, openalex_id: str, *, limit: int = 5
) -> list[str]:
    """Return the candidate's top-N most-frequent institutions (lower-cased)."""
    if not _table_exists(db, "publication_authors"):
        return []
    normalized = _normalize_openalex_id(openalex_id)
    if not normalized:
        return []
    try:
        rows = db.execute(
            """
            SELECT lower(trim(pa.institution)) AS institution_key,
                   COUNT(DISTINCT pa.paper_id) AS paper_count
            FROM publication_authors pa
            WHERE lower(pa.openalex_id) = lower(trim(?))
              AND COALESCE(TRIM(pa.institution), '') <> ''
            GROUP BY lower(trim(pa.institution))
            ORDER BY paper_count DESC
            LIMIT ?
            """,
            (normalized, max(1, limit)),
        ).fetchall()
    except sqlite3.OperationalError:
        return []
    out: list[str] = []
    for row in rows:
        inst = str(row["institution_key"] if isinstance(row, sqlite3.Row) else row[0]).strip()
        if inst and inst not in out:
            out.append(inst)
    return out


# Penalty per shared dismissed-author per overlap term, expressed as
# a fraction of the score band to stay calibrated alongside the
# consensus bonus. Tuned so that:
#   - 3 shared-cluster topic hits ≈ 6+ points (visible drop)
#   - shared venue ≈ 1.5 points / hit
#   - shared institution ≈ 1 point / hit (intentionally light: many
#     candidates share institutions for non-cluster reasons, so it's
#     a tiebreaker, not a primary signal)
#   - co-authorship is the LIGHTEST per-hit penalty: just 0.8 per
#     shared paper. Rationale: a user dismissing an author often
#     means "I don't want this person", NOT "I don't want anyone
#     who's ever written with them". Co-authors of dismissed authors
#     are a noisy cluster signal — strong collaboration depth (10+
#     papers) still registers, but a single co-authorship barely
#     moves the needle. Compare topic (2.0/hit) which is far more
#     specific evidence that a candidate works in the same area.
# Total capped at `_DISMISSAL_PENALTY_CAP` (30% of band).
_DISMISSAL_TOPIC_PENALTY_PER_HIT = 0.020 * _MAX_SUGGESTION_SCORE        # 2.0 today
_DISMISSAL_VENUE_PENALTY_PER_HIT = 0.015 * _MAX_SUGGESTION_SCORE        # 1.5 today
_DISMISSAL_COAUTHOR_PENALTY_PER_HIT = 0.008 * _MAX_SUGGESTION_SCORE     # 0.8 today
_DISMISSAL_INSTITUTION_PENALTY_PER_HIT = 0.010 * _MAX_SUGGESTION_SCORE  # 1.0 today
_DISMISSAL_PENALTY_CAP = 0.30 * _MAX_SUGGESTION_SCORE                   # 30 today


def _dismissal_overlap_penalty(
    candidate_openalex_id: str,
    candidate_topic_overlap: list[str],
    candidate_venue_overlap: list[str],
    candidate_institution_overlap: list[str],
    topic_signature: dict[str, int],
    venue_signature: dict[str, int],
    coauthor_signature: dict[str, int],
    institution_signature: dict[str, int],
) -> float:
    """Sum penalty points for the candidate's overlap with dismissal cluster.

    Four dimensions:
      - Topic / venue / institution: list-overlap → sum of
        per-hit penalty × signature[term].
      - Coauthor: single-ID match — `candidate_openalex_id` directly
        looked up in `coauthor_signature`. The signature value is
        shared paper count, so the penalty scales with collaboration
        depth.

    Total capped at `_DISMISSAL_PENALTY_CAP`. Capping matters: even
    strong dismissal signal must never fully zero a candidate — the
    user can dismiss them explicitly if needed, and over-zeroing
    hides candidates from the rail with no recovery path.
    """
    if not (
        topic_signature
        or venue_signature
        or coauthor_signature
        or institution_signature
    ):
        return 0.0
    penalty = 0.0
    for term in candidate_topic_overlap or ():
        key = (term or "").strip().lower()
        if key and key in topic_signature:
            penalty += topic_signature[key] * _DISMISSAL_TOPIC_PENALTY_PER_HIT
    for venue in candidate_venue_overlap or ():
        key = (venue or "").strip().lower()
        if key and key in venue_signature:
            penalty += venue_signature[key] * _DISMISSAL_VENUE_PENALTY_PER_HIT
    if candidate_openalex_id and coauthor_signature:
        cand_key = candidate_openalex_id.strip().lower()
        if cand_key in coauthor_signature:
            penalty += coauthor_signature[cand_key] * _DISMISSAL_COAUTHOR_PENALTY_PER_HIT
    for inst in candidate_institution_overlap or ():
        key = (inst or "").strip().lower()
        if key and key in institution_signature:
            penalty += institution_signature[key] * _DISMISSAL_INSTITUTION_PENALTY_PER_HIT
    return min(_DISMISSAL_PENALTY_CAP, penalty)


def _build_prevalence_weights(counts: dict[str, float]) -> dict[str, float]:
    """Map raw library topic/venue counts to log-normalized weights ∈ (0, 1].

    Thin wrapper around `alma.core.scoring_math.log_prevalence_weights`,
    kept for legacy call sites in this module. The shared helper is
    sign-preserving (so paper-Discovery signed weights work on the same
    code path); for the all-positive counts the author rail produces,
    output is identical to the prior local implementation.
    """
    return log_prevalence_weights(counts)


def _weighted_overlap_score(
    shared_terms: list[str],
    weights: dict[str, float],
    scale: float,
    *,
    fallback: float = 0.5,
) -> float:
    """Sum prevalence weights for the candidate's overlap, × `scale`.

    `shared_terms` is the list of overlap terms returned by
    `_shared_topics_for_candidate` / `_shared_venues_for_candidate`
    (original-case strings). `weights` is the lowercased prevalence
    dict from `_build_prevalence_weights`. Terms not in `weights`
    fall back to `fallback` so a future helper that doesn't share
    casing conventions still contributes a reasonable signal
    instead of zero.
    """
    if not shared_terms or scale <= 0:
        return 0.0
    total = 0.0
    for term in shared_terms:
        key = (term or "").strip().lower()
        if not key:
            continue
        total += weights.get(key, fallback)
    return total * scale


def _projected_author_signal_adjustment(
    db: sqlite3.Connection,
    openalex_id: str,
    projected: ProjectedPaperSignals,
) -> tuple[float, dict[str, float]]:
    """Score bump/penalty from paper feedback projected onto this author."""

    oid = _normalize_openalex_id(openalex_id).lower()
    if not oid:
        return 0.0, {}

    direct = float(projected.author.get(oid, 0.0)) * 18.0
    topic = sum(float(projected.topic.get(term, 0.0)) for term in _candidate_projection_topics(db, oid)) * 5.0
    venue = sum(float(projected.venue.get(venue, 0.0)) for venue in _candidate_projection_venues(db, oid)) * 4.0
    keyword = sum(float(projected.keyword.get(keyword, 0.0)) for keyword in _candidate_projection_keywords(db, oid)) * 3.0
    tag = sum(float(projected.tag.get(tag_name, 0.0)) for tag_name in _candidate_projection_tags(db, oid)) * 4.0
    total = max(-24.0, min(24.0, direct + topic + venue + keyword + tag))
    parts = {
        "author": round(direct, 3),
        "topic": round(topic, 3),
        "venue": round(venue, 3),
        "keyword": round(keyword, 3),
        "tag": round(tag, 3),
    }
    return total, parts


def _candidate_projection_topics(db: sqlite3.Connection, openalex_id: str, *, limit: int = 12) -> list[str]:
    if not _table_exists(db, "publication_topics") or not _table_exists(db, "publication_authors"):
        return []
    try:
        rows = db.execute(
            """
            SELECT lower(trim(pt.term)) AS term, COUNT(DISTINCT pt.paper_id) AS papers
            FROM publication_topics pt
            JOIN publication_authors pa ON pa.paper_id = pt.paper_id
            WHERE lower(pa.openalex_id) = lower(trim(?))
              AND COALESCE(TRIM(pt.term), '') <> ''
            GROUP BY lower(trim(pt.term))
            ORDER BY papers DESC, term ASC
            LIMIT ?
            """,
            (_normalize_openalex_id(openalex_id), limit),
        ).fetchall()
    except sqlite3.OperationalError:
        return []
    return [str(row["term"] or "").strip() for row in rows if str(row["term"] or "").strip()]


def _candidate_projection_venues(db: sqlite3.Connection, openalex_id: str, *, limit: int = 8) -> list[str]:
    if not _table_exists(db, "papers") or not _table_exists(db, "publication_authors"):
        return []
    try:
        rows = db.execute(
            """
            SELECT lower(trim(p.journal)) AS venue, COUNT(DISTINCT p.id) AS papers
            FROM papers p
            JOIN publication_authors pa ON pa.paper_id = p.id
            WHERE lower(pa.openalex_id) = lower(trim(?))
              AND COALESCE(TRIM(p.journal), '') <> ''
            GROUP BY lower(trim(p.journal))
            ORDER BY papers DESC, venue ASC
            LIMIT ?
            """,
            (_normalize_openalex_id(openalex_id), limit),
        ).fetchall()
    except sqlite3.OperationalError:
        return []
    return [str(row["venue"] or "").strip() for row in rows if str(row["venue"] or "").strip()]


def _candidate_projection_keywords(db: sqlite3.Connection, openalex_id: str, *, limit: int = 12) -> list[str]:
    if not _table_exists(db, "papers") or not _table_exists(db, "publication_authors"):
        return []
    try:
        rows = db.execute(
            """
            SELECT p.keywords
            FROM papers p
            JOIN publication_authors pa ON pa.paper_id = p.id
            WHERE lower(pa.openalex_id) = lower(trim(?))
              AND COALESCE(TRIM(p.keywords), '') <> ''
            LIMIT 50
            """,
            (_normalize_openalex_id(openalex_id),),
        ).fetchall()
    except sqlite3.OperationalError:
        return []
    seen: list[str] = []
    for row in rows:
        raw = row["keywords"] if isinstance(row, sqlite3.Row) else row[0]
        values: list[object]
        try:
            parsed = json.loads(raw) if isinstance(raw, str) else raw
        except json.JSONDecodeError:
            parsed = raw
        if isinstance(parsed, list):
            values = parsed
        else:
            values = re.split(r"[,;]", str(parsed or ""))
        for value in values:
            keyword = str(value or "").strip().lower()
            if keyword and keyword not in seen:
                seen.append(keyword)
            if len(seen) >= limit:
                return seen
    return seen


def _candidate_projection_tags(db: sqlite3.Connection, openalex_id: str, *, limit: int = 12) -> list[str]:
    if (
        not _table_exists(db, "tags")
        or not _table_exists(db, "publication_tags")
        or not _table_exists(db, "publication_authors")
    ):
        return []
    try:
        rows = db.execute(
            """
            SELECT lower(trim(t.name)) AS tag_name, COUNT(DISTINCT pt.paper_id) AS papers
            FROM publication_tags pt
            JOIN tags t ON t.id = pt.tag_id
            JOIN publication_authors pa ON pa.paper_id = pt.paper_id
            WHERE lower(pa.openalex_id) = lower(trim(?))
              AND COALESCE(TRIM(t.name), '') <> ''
            GROUP BY lower(trim(t.name))
            ORDER BY papers DESC, tag_name ASC
            LIMIT ?
            """,
            (_normalize_openalex_id(openalex_id), limit),
        ).fetchall()
    except sqlite3.OperationalError:
        return []
    return [str(row["tag_name"] or "").strip() for row in rows if str(row["tag_name"] or "").strip()]


def _shared_library_authors_for_candidate(
    db: sqlite3.Connection,
    openalex_id: str,
    *,
    limit: int = 3,
) -> list[str]:
    if not _table_exists(db, "publication_authors"):
        return []
    try:
        rows = db.execute(
            """
            SELECT
                COALESCE(pa2.display_name, pa2.openalex_id, '') AS name,
                COUNT(DISTINCT p.id) AS shared_papers
            FROM papers p
            JOIN publication_authors pa ON pa.paper_id = p.id
            JOIN publication_authors pa2 ON pa2.paper_id = pa.paper_id
            WHERE p.status = 'library'
              AND lower(trim(pa.openalex_id)) = lower(trim(?))
              AND lower(trim(pa2.openalex_id)) <> lower(trim(pa.openalex_id))
              AND COALESCE(TRIM(COALESCE(pa2.display_name, pa2.openalex_id, '')), '') <> ''
            GROUP BY COALESCE(pa2.display_name, pa2.openalex_id, '')
            ORDER BY shared_papers DESC, name ASC
            LIMIT ?
            """,
            (_normalize_openalex_id(openalex_id), max(1, limit)),
        ).fetchall()
    except sqlite3.OperationalError:
        return []
    return [
        str(row["name"] if isinstance(row, sqlite3.Row) else row[0]).strip()
        for row in rows
        if str(row["name"] if isinstance(row, sqlite3.Row) else row[0]).strip()
    ]


def _expand_library_reference_graph(
    db: sqlite3.Connection,
    *,
    limit: int = 80,
) -> dict[str, int]:
    """Bounded citation expansion for author-adjacency suggestions.

    The goal is to materialize directly referenced works for library papers so
    adjacent-author suggestions can use the local graph even when the cited work
    was not previously present in `papers`.
    """
    if not _table_exists(db, "publication_references"):
        return {"references_inserted": 0, "materialized": 0}
    try:
        rows = db.execute(
            """
            SELECT id
            FROM papers
            WHERE status = 'library'
              AND COALESCE(TRIM(openalex_id), '') <> ''
            ORDER BY COALESCE(updated_at, created_at, publication_date, '') DESC
            LIMIT ?
            """,
            (max(8, min(int(limit or 80), 120)),),
        ).fetchall()
    except sqlite3.OperationalError:
        return {"references_inserted": 0, "materialized": 0}

    paper_ids = [
        str(row["id"] if isinstance(row, sqlite3.Row) else row[0]).strip()
        for row in rows
        if str(row["id"] if isinstance(row, sqlite3.Row) else row[0]).strip()
    ]
    if not paper_ids:
        return {"references_inserted": 0, "materialized": 0}

    try:
        from alma.openalex.client import (
            backfill_missing_publication_references,
            materialize_missing_referenced_works,
        )

        ref_summary = backfill_missing_publication_references(db, paper_ids=paper_ids, limit=max(len(paper_ids), 24))
        materialized_summary = materialize_missing_referenced_works(db, seed_paper_ids=paper_ids, limit=limit)
        return {
            "references_inserted": int(ref_summary.get("references_inserted") or 0),
            "materialized": int(materialized_summary.get("materialized") or 0),
        }
    except Exception:
        return {"references_inserted": 0, "materialized": 0}


def _semantic_similar_candidates(
    db: sqlite3.Connection,
    *,
    exclude_ids: set[str],
    limit: int,
) -> list[dict]:
    """Rank authors by cosine similarity to the Library embedding centroid.

    D12 (2026-04-24) bucket 3 of 6. Builds a Library centroid from the
    active-model rows in `publication_embeddings`, then ranks every
    openalex_id that has ≥2 embedded papers (across the full corpus, not
    just Library) by the cosine distance of its author centroid to the
    Library centroid.

    Pure local — no external calls. Skipped quietly when:
      - `publication_embeddings` doesn't exist yet (new install)
      - the Library has zero embedded papers (cold start)
      - numpy isn't importable (stripped install)

    Candidates already passed in `exclude_ids` (followed authors or
    already-surfaced openalex_ids from earlier buckets) are filtered out
    so we don't spend cycles re-ranking them.
    """
    if limit <= 0:
        return []
    if not _table_exists(db, "publication_embeddings"):
        return []
    if not _table_exists(db, "publication_authors"):
        return []
    try:
        import numpy as np  # imported lazily to keep the authors module
        # importable on minimal installs where numpy may be absent.
    except ImportError:
        logger.debug("numpy unavailable — skipping semantic_similar author bucket")
        return []

    # Avoid a circular import at module load (discovery.similarity also
    # imports application code transitively).
    from alma.discovery.similarity import get_active_embedding_model

    model = get_active_embedding_model(db)
    if not model:
        return []

    # Library centroid — mean of active-model embeddings for saved papers.
    # ``LIMIT`` bounds Python work on very large libraries; centroid is an
    # average so a 1k-paper cap is already more than enough for stable
    # direction.
    try:
        lib_rows = db.execute(
            """
            SELECT pe.embedding AS embedding
            FROM publication_embeddings pe
            JOIN papers p ON p.id = pe.paper_id
            WHERE p.status = 'library' AND pe.model = ?
            LIMIT 1000
            """,
            (model,),
        ).fetchall()
    except sqlite3.OperationalError:
        return []
    if not lib_rows:
        return []
    from alma.core.vector_blob import decode_vector, decode_vectors_uniform

    # Uniform decoder rescues legacy fp32 rows by byte length and
    # filters out anything still wrong-shape; everything that survives
    # shares the same dim so np.stack / np.mean don't crash.
    lib_matrix, _ = decode_vectors_uniform(
        row["embedding"] for row in lib_rows
    )
    if lib_matrix.size == 0:
        return []
    lib_centroid = np.mean(lib_matrix, axis=0)
    lib_norm = float(np.linalg.norm(lib_centroid))
    if lib_norm <= 0.0:
        return []
    lib_centroid = lib_centroid / lib_norm
    expected_dim = int(lib_centroid.shape[0])

    # Candidate authors — openalex_ids with ≥2 embedded papers (any
    # status) and at least some corpus mass so we can form a meaningful
    # centroid. Cap to top 200 by embedded-paper count to bound Python
    # work; the cosine-ranking pass below narrows further.
    try:
        author_rows = db.execute(
            """
            SELECT
                lower(trim(pa.openalex_id)) AS candidate_openalex_id,
                COALESCE(MAX(pa.display_name), '') AS candidate_name,
                COUNT(DISTINCT pa.paper_id) AS embedded_paper_count
            FROM publication_authors pa
            JOIN publication_embeddings pe
              ON pe.paper_id = pa.paper_id AND pe.model = ?
            WHERE COALESCE(TRIM(pa.openalex_id), '') <> ''
            GROUP BY lower(trim(pa.openalex_id))
            HAVING COUNT(DISTINCT pa.paper_id) >= 2
            ORDER BY embedded_paper_count DESC
            LIMIT 200
            """,
            (model,),
        ).fetchall()
    except sqlite3.OperationalError:
        return []

    # Single fetch of every candidate author's paper embeddings; avoids
    # N×1 round trips. Dict keyed by openalex_id → list of paper vectors.
    openalex_ids = [str(row["candidate_openalex_id"] or "").strip() for row in author_rows]
    openalex_ids = [oid for oid in openalex_ids if oid and oid not in exclude_ids]
    if not openalex_ids:
        return []
    placeholders = ",".join("?" * len(openalex_ids))
    try:
        vec_rows = db.execute(
            f"""
            SELECT lower(pa.openalex_id) AS oid, pe.embedding AS embedding
            FROM publication_authors pa
            JOIN publication_embeddings pe
              ON pe.paper_id = pa.paper_id AND pe.model = ?
            WHERE lower(pa.openalex_id) IN ({placeholders})
            """,
            (model, *openalex_ids),
        ).fetchall()
    except sqlite3.OperationalError:
        return []
    per_author_vecs: dict[str, list] = {}
    for row in vec_rows:
        oid = str(row["oid"] or "").strip()
        blob = row["embedding"]
        if not oid or not blob:
            continue
        # Pin to the library dim so legacy-fp32 rows decode correctly
        # and any genuine mismatch is silently skipped here rather
        # than blowing up the np.stack below.
        try:
            vec = decode_vector(blob, expected_dim=expected_dim)
        except Exception:
            continue
        if vec.shape[0] != expected_dim:
            continue
        per_author_vecs.setdefault(oid, []).append(vec)

    ranked: list[tuple[float, str, str, int]] = []
    for row in author_rows:
        oid = str(row["candidate_openalex_id"] or "").strip()
        if not oid or oid in exclude_ids:
            continue
        vecs = per_author_vecs.get(oid)
        if not vecs:
            continue
        author_centroid = np.mean(np.stack(vecs), axis=0)
        author_norm = float(np.linalg.norm(author_centroid))
        if author_norm <= 0.0:
            continue
        author_centroid = author_centroid / author_norm
        similarity = float(np.dot(lib_centroid, author_centroid))  # cosine in [-1, 1]
        if similarity <= 0.0:
            continue  # negatively-correlated authors aren't useful suggestions
        ranked.append((similarity, oid, str(row["candidate_name"] or ""),
                       int(row["embedded_paper_count"] or 0)))
    ranked.sort(reverse=True)

    out: list[dict] = []
    for sim, oid, name, paper_count in ranked[:limit]:
        out.append({
            "candidate_openalex_id": oid,
            "candidate_name": name,
            "suggestion_type": "semantic_similar",
            "similarity": sim,
            "embedded_paper_count": paper_count,
        })
    return out


def _cited_by_high_signal_candidates(
    db: sqlite3.Connection,
    *,
    exclude_ids: set[str],
    limit: int,
    min_rating: int = 4,
) -> list[dict]:
    """Authors whose work is cited by positively-rated Library papers.

    D12 (2026-04-24) bucket 6 of 6. Pure SQL over
    `publication_references` + `papers.rating` + `publication_authors`.
    A paper cited by several 4-or-5-star Library papers is a strong
    positive signal that its authors are worth surfacing.

    `min_rating=4` keeps the pool to the explicit-positive band (per the
    rating contract in `08_PRODUCT_DECISIONS.md` D6: 4 = `+1`, 5 = `+2`).
    Ratings 1–3 would muddy the signal — 3 is neutral and 1–2 are
    negative.
    """
    if limit <= 0:
        return []
    if not (_table_exists(db, "publication_references")
            and _table_exists(db, "publication_authors")):
        return []
    try:
        rows = db.execute(
            """
            -- `papers.openalex_id` is canonical bare W-form
            -- (e.g. ``W123``). `publication_references.referenced_work_id`
            -- stores the bare integer suffix (``123``). Restore the
            -- ``W`` prefix on the references side so the JOIN matches
            -- without forcing a function wrapper on the indexed
            -- ``papers.openalex_id`` column. Probe on user's DB:
            -- **1878ms → sub-100ms** after this style of optimization.
            WITH rated_library AS (
                SELECT p.id AS library_paper_id,
                       COALESCE(p.rating, 0) AS citing_rating
                FROM papers p
                WHERE p.status = 'library' AND COALESCE(p.rating, 0) >= ?
            ),
            cited_works AS (
                SELECT DISTINCT
                    rl.library_paper_id AS library_paper_id,
                    rl.citing_rating AS citing_rating,
                    cited.id AS cited_paper_id,
                    COALESCE(NULLIF((
                        SELECT COUNT(*) FROM publication_authors pa2
                        WHERE pa2.paper_id = cited.id
                          AND COALESCE(TRIM(pa2.openalex_id), '') <> ''
                    ), 0), 1) AS cited_author_count
                FROM rated_library rl
                JOIN publication_references pr ON pr.paper_id = rl.library_paper_id
                JOIN papers cited
                  ON cited.openalex_id = ('W' || pr.referenced_work_id)
            )
            -- `lower(trim(...))` on the SELECT output is cheap (post-
            -- aggregation) and keeps the returned candidate id in the
            -- canonical lowercase form that the caller's
            -- `exclude_ids`/`seen_candidates` sets use for dedup.
            -- Only the JOIN above had to drop the function wrap.
            --
            -- `weighted_endorsement` mirrors the library_core formula:
            --   citing_rating_w (5★ → 1.5, else 1.0) — a 5★ Library
            --     paper citing you is a stronger endorsement than a 4★;
            --   position_w (first/last → 1.5) — first/senior authors
            --     own the cited work more than middle authors;
            --   1/sqrt(N) on the cited paper's author count — a middle
            --     author on a 30-person consortium gets 0.18× weight
            --     vs. sole-author papers, killing consortium-citation
            --     spam (the same effect the library_core SUM corrects
            --     for on the co-authorship side).
            SELECT
                lower(trim(pa.openalex_id)) AS candidate_openalex_id,
                COALESCE(MAX(pa.display_name), '') AS candidate_name,
                COUNT(DISTINCT cw.library_paper_id) AS citing_library_count,
                COUNT(DISTINCT cw.cited_paper_id) AS cited_paper_count,
                SUM(
                    (CASE cw.citing_rating WHEN 5 THEN 1.5 ELSE 1.0 END)
                    * (CASE lower(COALESCE(pa.position, ''))
                        WHEN 'first' THEN 1.5
                        WHEN 'last'  THEN 1.5
                        ELSE 1.0
                       END)
                    / sqrt(CAST(cw.cited_author_count AS REAL))
                ) AS weighted_endorsement
            FROM cited_works cw
            JOIN publication_authors pa ON pa.paper_id = cw.cited_paper_id
            WHERE COALESCE(TRIM(pa.openalex_id), '') <> ''
            GROUP BY lower(trim(pa.openalex_id))
            ORDER BY weighted_endorsement DESC, citing_library_count DESC, cited_paper_count DESC
            LIMIT ?
            """,
            (int(min_rating), max(limit * 3, 12)),
        ).fetchall()
    except sqlite3.OperationalError:
        return []

    out: list[dict] = []
    for row in rows:
        oid = str(row["candidate_openalex_id"] or "").strip()
        if not oid or oid in exclude_ids:
            continue
        out.append({
            "candidate_openalex_id": oid,
            "candidate_name": str(row["candidate_name"] or ""),
            "suggestion_type": "cited_by_high_signal",
            "citing_library_count": int(row["citing_library_count"] or 0),
            "cited_paper_count": int(row["cited_paper_count"] or 0),
            "weighted_endorsement": float(row["weighted_endorsement"] or 0.0),
        })
        if len(out) >= limit:
            break
    return out


def _build_author_signals(item: dict) -> list[dict]:
    """T7: build priority-ordered evidence chips for one suggestion.

    Reads the rich per-bucket fields the helpers above produce
    (`similarity`, `citing_library_count`, `shared_paper_count`,
    `shared_followed_authors`, `shared_topics`, `shared_venues`,
    etc.) and lowers them into a stable `{kind, label, count?,
    value?, subject?}` shape. The frontend renders each as a
    neutral `StatusBadge` chip so the user can see WHY the author
    surfaced without guessing from the bucket label.

    Priority order (strongest evidence first):
      1. Bucket-specific primary (SPECTER cosine, cited-in-saved,
         shared-refs, seed-cooccurrence).
      2. Library / recent paper counts for library_core.
      3. Co-authorship with a Library author (very concrete).
      4. Shared topics / venues (weaker but useful).

    Capped at 4 — the card has limited real estate; lower-priority
    evidence is still readable from `shared_topics` / sample_titles.
    """
    kind = str(item.get("suggestion_type") or "")
    chips: list[dict] = []

    # --- 1. Bucket-specific primary evidence ---------------------
    if kind == "library_core":
        local = int(item.get("local_paper_count") or 0)
        if local > 0:
            chips.append({
                "kind": "library_paper_count",
                "label": f"{local} in library",
                "count": local,
            })
        recent = int(item.get("recent_paper_count") or 0)
        if recent > 0 and recent != local:
            chips.append({
                "kind": "recent_paper_count",
                "label": f"{recent} recent",
                "count": recent,
            })
    elif kind == "cited_by_high_signal":
        citing = int(item.get("citing_library_count") or 0)
        if citing > 0:
            chips.append({
                "kind": "cited_in_saved",
                "label": f"cited in {citing} saved",
                "count": citing,
            })
        cited = int(item.get("cited_paper_count") or 0)
        if cited > 0:
            chips.append({
                "kind": "cited_paper_count",
                "label": f"{cited} of their papers cited",
                "count": cited,
            })
    elif kind == "semantic_similar":
        sim = float(item.get("similarity") or 0.0)
        if sim > 0.0:
            chips.append({
                "kind": "specter_cosine",
                "label": f"SPECTER {sim:.2f}",
                "value": round(sim, 3),
            })
        embedded = int(item.get("embedded_paper_count") or 0)
        if embedded > 0:
            chips.append({
                "kind": "embedded_paper_count",
                "label": f"{embedded} embedded papers",
                "count": embedded,
            })
    elif kind == "adjacent":
        shared = int(item.get("shared_paper_count") or 0)
        if shared > 0:
            chips.append({
                "kind": "shared_refs",
                "label": f"{shared} shared refs",
                "count": shared,
            })
        local = int(item.get("local_paper_count") or 0)
        if local > 0:
            chips.append({
                "kind": "library_paper_count",
                "label": f"{local} in corpus",
                "count": local,
            })
    elif kind in ("openalex_related", "s2_related"):
        co = int(item.get("shared_paper_count") or 0)
        if co > 0:
            chips.append({
                "kind": "seed_cooccurrence",
                "label": f"seed match ×{co}",
                "count": co,
            })

    # --- 2. Co-author with Library authors (concrete) ------------
    coauthors = [
        c for c in (item.get("shared_followed_authors") or [])
        if isinstance(c, str) and c.strip()
    ]
    if coauthors:
        primary = coauthors[0].strip()
        if len(coauthors) == 1:
            chips.append({
                "kind": "coauthor",
                "label": f"co-author of {primary}",
                "subject": primary,
                "count": 1,
            })
        else:
            chips.append({
                "kind": "coauthor",
                "label": f"co-author of {len(coauthors)} lib authors",
                "subject": primary,
                "count": len(coauthors),
            })

    # --- 3. Shared topics / venues (weaker) ----------------------
    topics = [
        t for t in (item.get("shared_topics") or [])
        if isinstance(t, str) and t.strip()
    ]
    if topics:
        chips.append({
            "kind": "shared_topics",
            "label": (
                f"shared topic: {topics[0]}"
                if len(topics) == 1
                else f"{len(topics)} shared topics"
            ),
            "count": len(topics),
            "subject": topics[0],
        })
    venues = [
        v for v in (item.get("shared_venues") or [])
        if isinstance(v, str) and v.strip()
    ]
    if venues:
        chips.append({
            "kind": "shared_venues",
            "label": (
                f"shared venue: {venues[0]}"
                if len(venues) == 1
                else f"{len(venues)} shared venues"
            ),
            "count": len(venues),
            "subject": venues[0],
        })

    return chips[:4]


def _record_consensus(
    suggestions: list[dict], openalex_id: str, secondary_bucket: str
) -> None:
    """Note that ``openalex_id`` was also surfaced by ``secondary_bucket``.

    The first bucket to surface a candidate creates the dict and seeds
    ``consensus_buckets`` with its own label. Every subsequent bucket
    that re-surfaces the same candidate appends its label here instead
    of dropping the row (the old behaviour). The end-of-run pass in
    `list_author_suggestions` then converts that list into a bonus
    score: multi-source agreement is a confidence signal — three
    independent buckets surfacing the same author is much stronger
    evidence than one bucket alone.
    """
    for entry in suggestions:
        if entry.get("openalex_id") == openalex_id:
            buckets = entry.setdefault("consensus_buckets", [entry.get("suggestion_type") or ""])
            if secondary_bucket and secondary_bucket not in buckets:
                buckets.append(secondary_bucket)
            return


def list_author_suggestions(
    db: sqlite3.Connection,
    *,
    limit: int = 8,
) -> list[dict]:
    if not _table_exists(db, "publication_authors"):
        return []

    from alma.application.gap_radar import get_missing_author_feedback_state

    followed_ids: set[str] = set()
    if _table_exists(db, "followed_authors") and _table_exists(db, "authors"):
        followed_rows = db.execute(
            """
            SELECT a.openalex_id
            FROM followed_authors fa
            JOIN authors a ON a.id = fa.author_id
            WHERE a.openalex_id IS NOT NULL AND TRIM(a.openalex_id) <> ''
            """
        ).fetchall()
        followed_ids = {
            _normalize_openalex_id(row["openalex_id"] if isinstance(row, sqlite3.Row) else row[0]).lower()
            for row in followed_rows
            if _normalize_openalex_id(row["openalex_id"] if isinstance(row, sqlite3.Row) else row[0])
        }

    # Extend `followed_ids` with every alt OpenAlex ID that's been
    # merged into a primary. Same suppression rule applies — once the
    # user has merged "Olivier Collignon (A5041…)" into the primary,
    # OpenAlex re-discovering A5041 in any bucket should NOT re-surface
    # them as a fresh suggestion. The alias table is small (one row
    # per merged alt) so this is a cheap UNION at request time.
    try:
        from alma.application.author_merge import list_all_alt_openalex_ids

        followed_ids = followed_ids | list_all_alt_openalex_ids(db)
    except Exception:
        # Alias table missing on a fresh schema is fine — no merges
        # have happened yet, so nothing to add.
        pass

    # ORCID-based defense in depth. The OpenAlex-ID UNION above only
    # filters candidates whose alias was already discovered via
    # `record_orcid_aliases` (called on follow) or recorded by a
    # manual merge / dedup sweep. When a candidate's OpenAlex ID is
    # NEW (e.g. OpenAlex split this human's profile after the user
    # followed them, or the alias-discovery API returned an
    # incomplete list), the ID slips through. Cross-checking the
    # candidate's stored ORCID against the followed-authors' ORCID
    # set catches those bleeders. Both sides normalize through
    # `normalize_orcid` so URI/bare/uppercase variants compare equal.
    followed_orcids: set[str] = set()
    if _table_exists(db, "followed_authors") and _table_exists(db, "authors"):
        orcid_rows = db.execute(
            """
            SELECT a.orcid
            FROM followed_authors fa
            JOIN authors a ON a.id = fa.author_id
            WHERE a.orcid IS NOT NULL AND TRIM(a.orcid) <> ''
            """
        ).fetchall()
        for row in orcid_rows:
            raw = row["orcid"] if isinstance(row, sqlite3.Row) else row[0]
            canonical = normalize_orcid(raw)
            if canonical:
                followed_orcids.add(canonical)

    existing_lookup = _existing_author_lookup(db)

    def _is_followed_via_orcid(candidate_openalex_id: str) -> bool:
        """True if our `authors` row for this candidate carries an
        ORCID that matches one of the followed authors' ORCIDs.
        Returns False when we don't have an `authors` row for the
        candidate yet (we never enriched their ORCID) or when the
        candidate's ORCID is empty — those candidates fall back to
        the OpenAlex-ID UNION filter above."""
        if not followed_orcids:
            return False
        existing = existing_lookup.get(candidate_openalex_id)
        if not existing:
            return False
        canonical = normalize_orcid(existing.get("orcid"))
        return bool(canonical and canonical in followed_orcids)
    # Topic / venue prevalence weights — sharing the user's #1 topic
    # is much stronger evidence than sharing their #30. The set form
    # is kept because the existing helpers
    # (`_shared_topics_for_candidate`, `_sample_titles_...`) take a
    # set whitelist; the weights dict drives the scoring formula.
    library_topic_weights = _build_prevalence_weights(_top_topics_for_library(db, limit=12))
    library_venue_weights = _build_prevalence_weights(_top_venues_for_library(db, limit=8))
    library_topics = set(library_topic_weights.keys())
    library_venues = set(library_venue_weights.keys())
    projected_paper_signals = load_projected_paper_signals(db)

    # Dismissal cluster signature — recently dismissed authors'
    # topic / venue / coauthor / institution profiles. Candidates
    # whose attributes intersect the signature get a score penalty in
    # the post-pass below, so dismissing a few authors generalizes to
    # "stop showing me more of this kind" instead of just suppressing
    # those exact IDs. See `_load_dismissal_signature` for why each
    # dimension is shaped the way it is.
    (
        dismissal_topic_signature,
        dismissal_venue_signature,
        dismissal_coauthor_signature,
        dismissal_institution_signature,
    ) = _load_dismissal_signature(db)
    dismissal_topic_set = set(dismissal_topic_signature.keys())
    dismissal_venue_set = set(dismissal_venue_signature.keys())
    dismissal_institution_set = set(dismissal_institution_signature.keys())
    current_year = datetime.utcnow().year

    suggestions: list[dict] = []
    seen_candidates: set[str] = set()

    # `library_core` weighted aggregate. Each (candidate, library-paper)
    # pair contributes `rating_w × position_w × recency_w / sqrt(N)`,
    # where N is the paper's author count. Three forces at play:
    #   • rating_w listens to the user's explicit signal — a 5★ paper
    #     contributes 3× as much as an unrated/neutral paper, a 1★
    #     paper only 0.2× (it's still in the Library so we don't zero
    #     it, but the user said it's bad).
    #   • position_w upweights first/last authors (lead + senior)
    #     against middle authors of the same paper.
    #   • 1/sqrt(N) kills consortium spam: a middle author on a
    #     30-person paper gets 0.18× the contribution of a sole
    #     author on the same kind of paper. Consortium co-authors
    #     used to crowd the rail because COUNT(DISTINCT p.id) treated
    #     them identically to true collaborators.
    # The raw 0–100 banding is preserved by the `× 24.0` outer multi-
    # plier in Python (a sole 5★ first-author paper saturates near
    # 100; a single middle-author consortium paper lands in the low
    # single digits).
    try:
        library_rows = db.execute(
            """
            WITH paper_meta AS (
                SELECT
                    p.id AS paper_id,
                    COALESCE(p.year, 0) AS year,
                    COALESCE(p.rating, 0) AS rating,
                    COALESCE(NULLIF((
                        SELECT COUNT(*)
                        FROM publication_authors pa2
                        WHERE pa2.paper_id = p.id
                          AND COALESCE(TRIM(pa2.openalex_id), '') <> ''
                    ), 0), 1) AS author_count
                FROM papers p
                WHERE p.status = 'library'
            )
            SELECT
                lower(trim(pa.openalex_id)) AS candidate_openalex_id,
                COALESCE(MAX(pa.display_name), '') AS candidate_name,
                COUNT(DISTINCT pm.paper_id) AS local_paper_count,
                COUNT(DISTINCT CASE WHEN pm.year >= ? THEN pm.paper_id END) AS recent_paper_count,
                SUM(
                    (CASE pm.rating
                        WHEN 1 THEN 0.2
                        WHEN 2 THEN 0.5
                        WHEN 3 THEN 1.0
                        WHEN 4 THEN 2.0
                        WHEN 5 THEN 3.0
                        ELSE 1.0
                     END)
                    * (CASE lower(COALESCE(pa.position, ''))
                        WHEN 'first' THEN 1.5
                        WHEN 'last'  THEN 1.5
                        ELSE 1.0
                       END)
                    * (CASE WHEN pm.year >= ? THEN 1.3 ELSE 1.0 END)
                    / sqrt(CAST(pm.author_count AS REAL))
                ) AS weighted_contribution
            FROM publication_authors pa
            JOIN paper_meta pm ON pm.paper_id = pa.paper_id
            WHERE COALESCE(TRIM(pa.openalex_id), '') <> ''
            GROUP BY lower(trim(pa.openalex_id))
            HAVING COUNT(DISTINCT pm.paper_id) >= 1
            ORDER BY weighted_contribution DESC, recent_paper_count DESC, candidate_name ASC
            LIMIT ?
            """,
            (current_year - 3, current_year - 3, max(limit * 4, 12)),
        ).fetchall()
    except sqlite3.OperationalError:
        library_rows = []

    for row in library_rows:
        openalex_id = _normalize_openalex_id(
            row["candidate_openalex_id"] if isinstance(row, sqlite3.Row) else row[0]
        ).lower()
        if not openalex_id or openalex_id in followed_ids or _is_followed_via_orcid(openalex_id):
            continue
        if openalex_id in seen_candidates:
            # Library_core is the FIRST bucket so this branch is
            # effectively unreachable today, but kept symmetric with
            # the other buckets in case bucket ordering ever changes.
            _record_consensus(suggestions, openalex_id, "library_core")
            continue
        feedback = get_missing_author_feedback_state(db, openalex_id)
        if feedback.get("suppressed"):
            continue
        local_paper_count = int(row["local_paper_count"] if isinstance(row, sqlite3.Row) else row[2] or 0)
        recent_paper_count = int(row["recent_paper_count"] if isinstance(row, sqlite3.Row) else row[3] or 0)
        weighted_contribution = float(
            row["weighted_contribution"] if isinstance(row, sqlite3.Row) else row[4] or 0.0
        )
        if local_paper_count <= 0:
            continue
        existing = existing_lookup.get(openalex_id)
        shared_topics = _shared_topics_for_candidate(db, openalex_id, library_topics)
        shared_venues = _shared_venues_for_candidate(db, openalex_id, library_venues)
        shared_library_authors = _shared_library_authors_for_candidate(db, openalex_id)
        # Recency is already folded into `weighted_contribution` via the
        # year-CASE inside the SUM, so don't double-count it here.
        # NOTE: do NOT add `shared_library_authors × 5` here. For
        # library_core (where the candidate IS a library co-author),
        # that signal is consortium-shaped: every middle author of a
        # 30-person paper trivially shares 3 library co-authors with
        # their fellow consortium members, so the +15 would re-inflate
        # exactly the spam pattern weighted_contribution corrects for.
        # The `adjacent` bucket can use it because the candidate there
        # is NOT a library author, so the overlap is real network
        # density rather than same-paper artefact.
        # Topic/venue contributions are now prevalence-weighted: a
        # candidate sharing your #1 library topic gets ~the full 8
        # points, sharing your #30 gets ~2. Multipliers were 5/4 for
        # equal-weight counting; bumped to 8/6 so a top-topic match
        # is materially MORE valuable than under the old scheme, not
        # just a redistribution. See `_build_prevalence_weights`.
        score = min(
            _MAX_SUGGESTION_SCORE,
            (weighted_contribution * 24.0)
            + _weighted_overlap_score(shared_topics, library_topic_weights, 8.0)
            + _weighted_overlap_score(shared_venues, library_venue_weights, 6.0),
        )
        suggestions.append(
            {
                "key": f"library_core:{openalex_id}",
                "name": str(row["candidate_name"] if isinstance(row, sqlite3.Row) else row[1]).strip() or openalex_id,
                "openalex_id": openalex_id,
                "existing_author_id": existing.get("id") if existing else None,
                "known_author_type": existing.get("author_type") if existing else None,
                "suggestion_type": "library_core",
                "score": round(score, 1),
                "weighted_contribution": round(weighted_contribution, 3),
                "shared_paper_count": local_paper_count,
                "shared_followed_count": len(shared_library_authors),
                "local_paper_count": local_paper_count,
                "recent_paper_count": recent_paper_count,
                "shared_followed_authors": shared_library_authors,
                "shared_topics": shared_topics,
                "shared_venues": shared_venues,
                "sample_titles": _sample_titles_for_openalex_author(
                    db,
                    openalex_id,
                    topic_whitelist=library_topics,
                    venue_whitelist=library_venues,
                ),
                "negative_signal": float(feedback.get("score") or 0.0),
                "last_removed_at": feedback.get("last_removed_at"),
            }
        )
        seen_candidates.add(openalex_id)
        if len(suggestions) >= limit:
            break

    # NOTE: `_expand_library_reference_graph` (OpenAlex
    # `materialize_missing_referenced_works` + `backfill_missing_...`)
    # used to run HERE synchronously in the GET handler. Measured 117s
    # per request on a library without an OpenAlex API key set (401 +
    # exp-backoff retries). That violates the "API endpoints: reads vs
    # writes" rule in `lessons.md`. The materialization is a write/
    # enrichment concern and belongs on the Activity-envelope refresh
    # path (`refresh_openalex_related_network`) or a scheduled job, not
    # on every author-suggestions GET. Kept the helper around because
    # it's still useful in those contexts.

    # D12 bucket — `cited_by_high_signal`. Runs BEFORE the generic
    # `adjacent` bucket so when an author qualifies for both (e.g. they
    # wrote papers cited by a 4-star Library paper), the UI gets the
    # richer provenance label ("Cited by your 4★ papers") instead of
    # the generic "Adjacent to your Library". The adjacent bucket
    # still catches citation-graph neighbours with no rating signal.
    # Run the bucket unconditionally (no `len(suggestions) < limit`
    # gate) and pass only `followed_ids` to the helper — we want
    # candidates that already appeared in library_core to come back so
    # `_record_consensus` can mark them as multi-source. The
    # `cited_added` counter still respects the per-bucket cap on NEW
    # candidates, so we don't flood the rail.
    cited_limit = max(2, limit // 4)
    cited_rows = _cited_by_high_signal_candidates(
        db, exclude_ids=followed_ids, limit=cited_limit,
    )
    cited_added = 0
    for row in cited_rows:
        openalex_id = _normalize_openalex_id(str(row.get("candidate_openalex_id") or "")).lower()
        if not openalex_id or openalex_id in followed_ids or _is_followed_via_orcid(openalex_id):
            continue
        if openalex_id in seen_candidates:
            _record_consensus(suggestions, openalex_id, "cited_by_high_signal")
            continue
        if cited_added >= cited_limit:
            continue
        feedback = get_missing_author_feedback_state(db, openalex_id)
        if feedback.get("suppressed"):
            continue
        citing_library = int(row.get("citing_library_count") or 0)
        cited_papers = int(row.get("cited_paper_count") or 0)
        weighted_endorsement = float(row.get("weighted_endorsement") or 0.0)
        # Cited-by-high-signal is a strong positive, but the raw
        # citation count over-rewards middle authors of consortium
        # papers cited once by a 4★. `weighted_endorsement`
        # already folds (citing-paper rating × candidate position
        # × 1/sqrt(N)) — a sole/lead author of a paper cited by a
        # 5★ is worth ~30 points; a middle author of a 30-person
        # consortium cited by a 4★ is worth ~5. The small
        # `cited_papers` term keeps a tiebreak-style bonus for
        # candidates whose cited footprint spans multiple distinct
        # papers (a wider endorsement, not just one paper hit).
        score = min(_MAX_SUGGESTION_SCORE, (weighted_endorsement * 30.0) + (cited_papers * 4.0))
        shared_library_authors = _shared_library_authors_for_candidate(db, openalex_id)
        existing = existing_lookup.get(openalex_id)
        suggestions.append(
            {
                "key": f"cited_by_high_signal:{openalex_id}",
                "name": str(row.get("candidate_name") or "").strip() or openalex_id,
                "openalex_id": openalex_id,
                "existing_author_id": existing.get("id") if existing else None,
                "known_author_type": existing.get("author_type") if existing else None,
                "suggestion_type": "cited_by_high_signal",
                "score": round(score, 1),
                "weighted_endorsement": round(weighted_endorsement, 3),
                "shared_paper_count": citing_library,
                "shared_followed_count": len(shared_library_authors),
                "local_paper_count": 0,
                "recent_paper_count": 0,
                "shared_followed_authors": shared_library_authors,
                "shared_topics": [],
                "shared_venues": [],
                "sample_titles": _sample_titles_for_openalex_author(
                    db, openalex_id,
                    topic_whitelist=library_topics,
                    venue_whitelist=library_venues,
                ),
                "citing_library_count": citing_library,
                "cited_paper_count": cited_papers,
                "negative_signal": float(feedback.get("score") or 0.0),
                "last_removed_at": feedback.get("last_removed_at"),
            }
        )
        seen_candidates.add(openalex_id)
        cited_added += 1

    adjacent_rows: list[dict] = []
    if len(suggestions) < limit:
        try:
            if _table_exists(db, "publication_references"):
                # Match bare openalex_id on both sides (healed by the
                # one-shot migration in `init_db_schema`). The old
                # `lower(trim(...))` on both sides killed the planner —
                # per the SQLite query-planning lesson, a function on a
                # column disables the partial UNIQUE index. Profile on
                # the author's Library (1293 papers): **107s → sub-second**
                # after this fix.
                rows = db.execute(
                    """
                    WITH library_refs AS (
                        SELECT DISTINCT pr.paper_id AS seed_paper_id, pr.referenced_work_id
                        FROM publication_references pr
                        JOIN papers p ON p.id = pr.paper_id
                        WHERE p.status = 'library'
                    ),
                    referenced_local AS (
                        SELECT lr.seed_paper_id, rp.id AS referenced_paper_id
                        FROM library_refs lr
                        JOIN papers rp ON rp.openalex_id = ('W' || lr.referenced_work_id)
                    )
                    SELECT
                        pa.openalex_id AS candidate_openalex_id,
                        COALESCE(MAX(pa.display_name), '') AS candidate_name,
                        COUNT(DISTINCT rl.seed_paper_id) AS shared_paper_count,
                        COUNT(DISTINCT rp.id) AS local_paper_count,
                        COUNT(DISTINCT CASE WHEN COALESCE(rp.year, 0) >= ? THEN rp.id END) AS recent_paper_count
                    FROM referenced_local rl
                    JOIN papers rp ON rp.id = rl.referenced_paper_id
                    JOIN publication_authors pa ON pa.paper_id = rp.id
                    WHERE COALESCE(TRIM(pa.openalex_id), '') <> ''
                    GROUP BY pa.openalex_id
                    HAVING COUNT(DISTINCT rl.seed_paper_id) >= 1
                    ORDER BY shared_paper_count DESC, recent_paper_count DESC, local_paper_count DESC
                    LIMIT ?
                    """,
                    (current_year - 3, max(limit * 6, 18)),
                ).fetchall()
                adjacent_rows.extend(dict(row) for row in rows)
        except sqlite3.OperationalError:
            adjacent_rows = []

    if len(adjacent_rows) < max(limit * 2, 12):
        try:
            rows = db.execute(
                """
                WITH library_topics AS (
                    SELECT pt.term
                    FROM publication_topics pt
                    JOIN papers p ON p.id = pt.paper_id
                    WHERE p.status = 'library'
                      AND pt.term IS NOT NULL
                      AND TRIM(pt.term) <> ''
                    GROUP BY pt.term
                    ORDER BY COUNT(DISTINCT pt.paper_id) DESC
                    LIMIT 16
                ),
                library_venues AS (
                    SELECT lower(trim(p.journal)) AS journal_key
                    FROM papers p
                    WHERE p.status = 'library'
                      AND COALESCE(TRIM(p.journal), '') <> ''
                    GROUP BY lower(trim(p.journal))
                    ORDER BY COUNT(DISTINCT p.id) DESC
                    LIMIT 10
                )
                SELECT
                    lower(trim(pa.openalex_id)) AS candidate_openalex_id,
                    COALESCE(MAX(pa.display_name), '') AS candidate_name,
                    COUNT(DISTINCT p.id) AS local_paper_count,
                    COUNT(DISTINCT CASE WHEN lt.term IS NOT NULL THEN pt.term END) AS shared_topic_count,
                    COUNT(DISTINCT CASE WHEN lv.journal_key IS NOT NULL THEN lower(trim(p.journal)) END) AS shared_venue_count,
                    COUNT(DISTINCT CASE WHEN COALESCE(p.year, 0) >= ? THEN p.id END) AS recent_paper_count
                FROM publication_authors pa
                JOIN papers p ON p.id = pa.paper_id
                LEFT JOIN publication_topics pt ON pt.paper_id = p.id
                LEFT JOIN library_topics lt ON lt.term = pt.term
                LEFT JOIN library_venues lv ON lv.journal_key = lower(trim(p.journal))
                WHERE COALESCE(TRIM(pa.openalex_id), '') <> ''
                  AND p.status <> 'removed'
                GROUP BY lower(trim(pa.openalex_id))
                HAVING shared_topic_count >= 2 OR shared_venue_count >= 1
                ORDER BY shared_topic_count DESC, shared_venue_count DESC, recent_paper_count DESC, local_paper_count DESC
                LIMIT ?
                """,
                (current_year - 3, max(limit * 6, 18)),
            ).fetchall()
            adjacent_rows.extend(dict(row) for row in rows)
        except sqlite3.OperationalError:
            adjacent_rows = []

    adjacent_added = 0
    adjacent_cap = max(2, limit // 2)
    for row in adjacent_rows:
        openalex_id = _normalize_openalex_id(str(row.get("candidate_openalex_id") or "")).lower()
        if not openalex_id or openalex_id in followed_ids or _is_followed_via_orcid(openalex_id):
            continue
        if openalex_id in seen_candidates:
            _record_consensus(suggestions, openalex_id, "adjacent")
            continue
        if adjacent_added >= adjacent_cap:
            continue
        feedback = get_missing_author_feedback_state(db, openalex_id)
        if feedback.get("suppressed"):
            continue
        shared_paper_count = int(row.get("shared_paper_count") or 0)
        local_paper_count = int(row.get("local_paper_count") or 0)
        recent_paper_count = int(row.get("recent_paper_count") or 0)
        shared_topics = _shared_topics_for_candidate(db, openalex_id, library_topics)
        shared_venues = _shared_venues_for_candidate(db, openalex_id, library_venues)
        shared_library_authors = _shared_library_authors_for_candidate(db, openalex_id)
        if not shared_topics and not shared_venues and shared_paper_count <= 0 and local_paper_count < 1:
            continue
        existing = existing_lookup.get(openalex_id)
        score = min(
            _MAX_SUGGESTION_SCORE,
            (shared_paper_count * 20.0)
            + (local_paper_count * 8.0)
            + (recent_paper_count * 4.0),
        )
        # Adjacent already weighted topics slightly higher than
        # library_core (6 vs 5) because adjacency is a weaker primary
        # signal. Bump to 8/6 in line with the prevalence-weighted
        # design — top topic carries ~8, fringe topic ~2.
        score = min(
            _MAX_SUGGESTION_SCORE,
            score
            + _weighted_overlap_score(shared_topics, library_topic_weights, 8.0)
            + _weighted_overlap_score(shared_venues, library_venue_weights, 6.0)
            + (len(shared_library_authors) * 5.0),
        )
        suggestions.append(
            {
                "key": f"adjacent:{openalex_id}",
                "name": str(row.get("candidate_name") or "").strip() or openalex_id,
                "openalex_id": openalex_id,
                "existing_author_id": existing.get("id") if existing else None,
                "known_author_type": existing.get("author_type") if existing else None,
                "suggestion_type": "adjacent",
                "score": round(score, 1),
                "shared_paper_count": shared_paper_count,
                "shared_followed_count": len(shared_library_authors),
                "local_paper_count": local_paper_count,
                "recent_paper_count": recent_paper_count,
                "shared_followed_authors": shared_library_authors,
                "shared_topics": shared_topics,
                "shared_venues": shared_venues,
                "sample_titles": _sample_titles_for_openalex_author(
                    db,
                    openalex_id,
                    topic_whitelist=library_topics,
                    venue_whitelist=library_venues,
                ),
                "negative_signal": float(feedback.get("score") or 0.0),
                "last_removed_at": feedback.get("last_removed_at"),
            }
        )
        seen_candidates.add(openalex_id)
        adjacent_added += 1

    # D12 semantic-similar bucket. Runs AFTER adjacent / cited-by so
    # the more-explainable signals claim the slot first; semantic
    # similarity is a powerful but less-interpretable provenance
    # ("they're semantically similar to your library"), so it backfills
    # remaining slots rather than competing head-on. Now also runs
    # unconditionally so consensus tracking can see prior-bucket
    # candidates even when the rail is already at `limit`.
    semantic_limit = max(2, limit // 4)
    semantic_rows = _semantic_similar_candidates(
        db, exclude_ids=followed_ids, limit=semantic_limit,
    )
    semantic_added = 0
    for row in semantic_rows:
        openalex_id = _normalize_openalex_id(str(row.get("candidate_openalex_id") or "")).lower()
        if not openalex_id or openalex_id in followed_ids or _is_followed_via_orcid(openalex_id):
            continue
        if openalex_id in seen_candidates:
            _record_consensus(suggestions, openalex_id, "semantic_similar")
            continue
        if semantic_added >= semantic_limit:
            continue
        feedback = get_missing_author_feedback_state(db, openalex_id)
        if feedback.get("suppressed"):
            continue
        similarity = float(row.get("similarity") or 0.0)
        embedded = int(row.get("embedded_paper_count") or 0)
        # Cosine sim is [-1, 1]; positive pool already filtered in
        # helper. Map to a 0–100 band where 0.5 similarity → 50
        # score, 0.9 → 90. Volume kicks in but is capped.
        score = min(_MAX_SUGGESTION_SCORE, (similarity * 90.0) + min(embedded, 10) * 1.0)
        shared_library_authors = _shared_library_authors_for_candidate(db, openalex_id)
        existing = existing_lookup.get(openalex_id)
        suggestions.append(
            {
                "key": f"semantic_similar:{openalex_id}",
                "name": str(row.get("candidate_name") or "").strip() or openalex_id,
                "openalex_id": openalex_id,
                "existing_author_id": existing.get("id") if existing else None,
                "known_author_type": existing.get("author_type") if existing else None,
                "suggestion_type": "semantic_similar",
                "score": round(score, 1),
                "similarity": round(similarity, 3),
                "shared_paper_count": 0,
                "shared_followed_count": len(shared_library_authors),
                "local_paper_count": 0,
                "recent_paper_count": 0,
                "shared_followed_authors": shared_library_authors,
                "shared_topics": [],
                "shared_venues": [],
                "sample_titles": _sample_titles_for_openalex_author(
                    db, openalex_id,
                    topic_whitelist=library_topics,
                    venue_whitelist=library_venues,
                ),
                "embedded_paper_count": embedded,
                "negative_signal": float(feedback.get("score") or 0.0),
                "last_removed_at": feedback.get("last_removed_at"),
            }
        )
        seen_candidates.add(openalex_id)
        semantic_added += 1

    # D12 network buckets (AUTH-SUG-3, AUTH-SUG-4) — pure reads from
    # the `author_suggestion_cache` table (populated by
    # `POST /authors/suggestions/refresh-network`; this function
    # never hits the network itself, per "API endpoints: reads vs
    # writes"). The cache read is O(1) so we always run both buckets
    # regardless of how full `suggestions` is — the weighted-score
    # sort + per-bucket reservation (below) decides which candidates
    # reach the final payload. Skipping these buckets here would
    # starve the rail of new-discovery authors when library_core /
    # adjacent already fill the limit.
    from alma.application.author_network import (
        _openalex_related_candidates as _oa_rel,
        _s2_related_candidates as _s2_rel,
    )

    # Each network bucket gets up to `ceil(limit/3)` slots guaranteed
    # BEFORE weighted sort, so even a library that saturates
    # library_core still sees external suggestions. Surplus is
    # pruned by the final trim.
    network_slot_cap = max(2, (limit + 2) // 3)
    for bucket_source, bucket_reader in (
        ("openalex_related", _oa_rel),
        ("s2_related", _s2_rel),
    ):
        try:
            rows = bucket_reader(
                db,
                exclude_ids=followed_ids,
                limit=network_slot_cap,
            )
        except Exception:
            logger.debug(
                "network bucket %s read failed", bucket_source, exc_info=True
            )
            rows = []
        network_added = 0
        for row in rows:
            oid = _normalize_openalex_id(
                str(row.get("candidate_openalex_id") or "")
            ).lower()
            if not oid or oid in followed_ids:
                continue
            if oid in seen_candidates:
                _record_consensus(suggestions, oid, bucket_source)
                continue
            if network_added >= network_slot_cap:
                continue
            composite = float(row.get("composite_score") or 0.0)
            # Map composite (0..1) into the 0..100 score band used
            # everywhere else on this response.
            # Network buckets emit composite ∈ [0, 1]; rescale into the
            # shared score band so per-bucket weights apply uniformly.
            bucket_score = min(_MAX_SUGGESTION_SCORE, composite * _MAX_SUGGESTION_SCORE)
            existing = existing_lookup.get(oid)
            suggestions.append(
                {
                    "key": f"{bucket_source}:{oid}",
                    "name": str(row.get("candidate_name") or "").strip() or oid,
                    "openalex_id": oid,
                    "existing_author_id": existing.get("id") if existing else None,
                    "known_author_type": existing.get("author_type") if existing else None,
                    "suggestion_type": bucket_source,
                    "score": round(bucket_score, 1),
                    "shared_paper_count": int(row.get("seed_cooccurrence") or 0),
                    "shared_followed_count": 0,
                    "local_paper_count": 0,
                    "recent_paper_count": 0,
                    "shared_followed_authors": [],
                    "shared_topics": list(row.get("topics") or [])[:6],
                    "shared_venues": list(row.get("venues") or [])[:4],
                    "sample_titles": [],
                    "negative_signal": float(row.get("negative_signal") or 0.0),
                    "last_removed_at": row.get("last_removed_at"),
                }
            )
            seen_candidates.add(oid)
            network_added += 1

    # ── Multi-source consensus bonus ────────────────────────────────
    # `_record_consensus` has been appending each later bucket's label
    # to the matching entry's `consensus_buckets` list throughout the
    # bucket pass. Convert that list into a score bonus now, before
    # the per-bucket weight multiplier and sort. The bonus formula
    # is band-relative — see `_CONSENSUS_BONUS_FRACTION` /
    # `_MAX_SUGGESTION_SCORE` at the top of this module. Today this
    # gives +12 / +17 / +21 / +24 for 2 / 3 / 4 / 5 buckets agreeing.
    # Diminishing returns mean a candidate cited by every system
    # cannot trivially saturate against a single very-strong bucket
    # signal (e.g. lead author of a 5★), but a moderately scored
    # candidate confirmed by 3+ independent sources reliably climbs
    # the rail.
    for item in suggestions:
        buckets = item.get("consensus_buckets") or [item.get("suggestion_type") or ""]
        n = len(buckets)
        bonus = _shared_consensus_bonus(
            n,
            fraction=_CONSENSUS_BONUS_FRACTION,
            max_score=_MAX_SUGGESTION_SCORE,
        )
        if bonus > 0:
            item["score"] = round(
                min(_MAX_SUGGESTION_SCORE, float(item.get("score") or 0.0) + bonus),
                1,
            )
        # Always emit `consensus_buckets` (even singletons) for the UI
        # so downstream surfaces can render a "Suggested by N sources"
        # badge without recomputing.
        item["consensus_buckets"] = buckets
        item["consensus_count"] = n

    # ── Paper-feedback projection bump/penalty ──────────────────────
    # Paper feedback should generalize to the local graph: authors and
    # co-authors, their topics, venues, keywords, and user tags. Apply
    # this before the explicit dismissed-author cluster penalty so a
    # positive paper cannot fully rescue an author cluster the user has
    # directly removed.
    for item in suggestions:
        oid = str(item.get("openalex_id") or "")
        if not oid:
            continue
        adjustment, parts = _projected_author_signal_adjustment(db, oid, projected_paper_signals)
        if adjustment == 0.0:
            continue
        item["score"] = round(
            max(0.0, min(_MAX_SUGGESTION_SCORE, float(item.get("score") or 0.0) + adjustment)),
            1,
        )
        item["paper_signal_adjustment"] = round(adjustment, 1)
        item["paper_signal_adjustment_parts"] = parts

    # ── Dismissal cluster penalty ───────────────────────────────────
    # Apply AFTER consensus so multi-source agreement can't entirely
    # rescue a candidate who matches the dismissed-author cluster:
    # the user has explicitly said "less of this kind". Compute each
    # candidate's overlap with the dismissal topic/venue signature,
    # subtract the resulting penalty, clamp at 0. Cap is enforced
    # inside `_dismissal_overlap_penalty` (~30% of band) so a
    # candidate is never permanently zero'd by the penalty alone —
    # the user can still dismiss them explicitly. Skip the pass
    # entirely when no dismissals exist (the common case for fresh
    # libraries).
    if (
        dismissal_topic_set
        or dismissal_venue_set
        or dismissal_coauthor_signature
        or dismissal_institution_set
    ):
        for item in suggestions:
            oid = str(item.get("openalex_id") or "")
            if not oid:
                continue
            cand_topic_overlap = (
                _shared_topics_for_candidate(db, oid, dismissal_topic_set, limit=20)
                if dismissal_topic_set
                else []
            )
            cand_venue_overlap = (
                _shared_venues_for_candidate(db, oid, dismissal_venue_set, limit=20)
                if dismissal_venue_set
                else []
            )
            cand_institution_overlap = (
                [
                    inst
                    for inst in _candidate_top_institutions(db, oid, limit=5)
                    if inst in dismissal_institution_set
                ]
                if dismissal_institution_set
                else []
            )
            penalty = _dismissal_overlap_penalty(
                oid,
                cand_topic_overlap,
                cand_venue_overlap,
                cand_institution_overlap,
                dismissal_topic_signature,
                dismissal_venue_signature,
                dismissal_coauthor_signature,
                dismissal_institution_signature,
            )
            if penalty > 0:
                item["score"] = round(
                    max(0.0, float(item.get("score") or 0.0) - penalty),
                    1,
                )
                item["dismissal_penalty"] = round(penalty, 1)

    # D12 AUTH-SUG-5: apply tunable per-bucket weights from
    # `discovery_settings.author_suggestion_weights.*`, then sort by
    # weighted score. Priority-based dedup already ran during
    # orchestration, so `cited_by_high_signal > adjacent` label
    # precedence is preserved (the locked test
    # `test_list_author_suggestions_prefers_cited_by_high_signal_over_adjacent`
    # still asserts the label, not the numeric order). Weighted score
    # overwrites the `score` field so every surface reads the same
    # final number; raw bucket math is a helper intermediate.
    bucket_weights = _load_author_suggestion_weights(db)
    # Phase 4 #3 — per-bucket outcome calibration. Computed once per
    # call. Empty on a fresh DB (no follow / reject events with bucket
    # attribution yet) → multiplier 1.0 → no behavior change. After
    # enough rail-side feedback accumulates, buckets the user
    # consistently follows from get pushed toward 1.5x; buckets they
    # consistently reject get pulled toward 0.5x. Composes
    # multiplicatively with the static `bucket_weights` knob.
    from alma.application.outcome_calibration import (
        compute_author_bucket_calibration,
    )
    bucket_calibration = compute_author_bucket_calibration(db)
    _type_priority = {
        "library_core": 0,
        "cited_by_high_signal": 1,
        "adjacent": 1,
        "semantic_similar": 2,
        "openalex_related": 3,
        "s2_related": 3,
    }
    for item in suggestions:
        bucket = str(item.get("suggestion_type") or "")
        weight = float(bucket_weights.get(bucket, 1.0))
        bucket_multiplier = float(
            bucket_calibration.multipliers.get(bucket.lower(), 1.0)
        )
        raw = float(item.get("score") or 0.0)
        item["score"] = round(
            min(_MAX_SUGGESTION_SCORE, raw * weight * bucket_multiplier), 1
        )
        item["bucket_calibration_multiplier"] = round(bucket_multiplier, 4)

    suggestions.sort(
        key=lambda item: (
            -float(item.get("score") or 0.0),
            _type_priority.get(str(item.get("suggestion_type") or ""), 99),
            -int(item.get("local_paper_count") or 0),
            -int(item.get("recent_paper_count") or 0),
            str(item.get("name") or "").lower(),
        )
    )

    # ── Same-human dedup ────────────────────────────────────────────
    # OpenAlex frequently issues multiple author IDs for the same
    # person — split profiles after a name spelling change, an
    # institution move, or an ORCID drift. The bucket pass dedupes
    # by openalex_id (`seen_candidates`), which catches "same row
    # appears in two buckets" but not "different rows for the same
    # human". Live example: `author:Olivier Collignon` returns 3
    # OpenAlex IDs (A5003094142, A5041237205, A5102077551) — the
    # rail used to surface 3 separate cards for the same person.
    #
    # The only signal we have at suggestion time without paying a
    # second OpenAlex round-trip is the display name. Strategy:
    #
    # 1. Walk in score order (highest first) so the *richest* row
    #    in each name-cluster wins — its score is already a proxy
    #    for "which OpenAlex profile is the best one for this human".
    # 2. Normalize aggressively but conservatively: lowercase, strip
    #    punctuation, collapse whitespace. Do NOT collapse initials
    #    ("J. Smith" vs "John Smith") — false collapses are worse
    #    than visible duplicates because the user can act on dupes
    #    but cannot recover a wrongly-merged distinct author.
    # 3. Stash the dropped openalex_ids on the surviving row as
    #    `alt_openalex_ids` so the dossier can later surface a
    #    "this person has N OpenAlex profiles" hint if useful.
    deduped: list[dict] = []
    seen_names: dict[str, dict] = {}
    for item in suggestions:
        name_key = _normalize_author_display_name(item.get("name") or "")
        if not name_key:
            # Unparseable name — keep it; better visible than dropped.
            deduped.append(item)
            continue
        existing = seen_names.get(name_key)
        if existing is None:
            seen_names[name_key] = item
            deduped.append(item)
            continue
        # Duplicate — record the dropped openalex_id on the survivor.
        alt = str(item.get("openalex_id") or "").strip()
        if alt and alt.lower() != str(existing.get("openalex_id") or "").lower():
            existing.setdefault("alt_openalex_ids", []).append(alt)
    suggestions = deduped

    # T7: stamp per-suggestion evidence chips so the frontend can
    # render concrete reasons (SPECTER 0.83, cited in 4 saved,
    # co-author of X, etc.) instead of just the bucket label.
    for item in suggestions:
        item["signals"] = _build_author_signals(item)

    # Per-bucket diversity cap. Without this, `library_core` weight 1.0
    # × high raw scores always crowds out `openalex_related` /
    # `s2_related` (weight 0.5 × lower composites) — the rail ends up
    # looking like a pure library-co-author list even when the network
    # buckets are full of novel candidates. Reserve slots proportional
    # to the ratio of each bucket's declared weight, guaranteeing that
    # every populated bucket contributes at least one suggestion.
    return _diversify_final(suggestions, limit=limit, weights=bucket_weights)


def _diversify_final(
    suggestions: list[dict],
    *,
    limit: int,
    weights: dict[str, float],
) -> list[dict]:
    """Trim to `limit` with a per-bucket ceiling so no bucket dominates.

    Strategy: sorted suggestions already reflect weighted ranking.
    Walk them in order; accept each unless its bucket has hit its
    reserved quota; when every populated bucket has ≥1 slot, any
    leftover capacity goes to the global ranking.
    """

    if limit <= 0 or not suggestions:
        return suggestions[:0]
    # Quota per bucket: library_core gets ~60% (strongest signal the
    # user recognizes), cited_by_high_signal/adjacent each get ~15%,
    # semantic/network each get ~10%. Sum can exceed 100% — we stop
    # at `limit`; caps just prevent any one bucket from starving
    # others.
    quotas = {
        "library_core": max(1, round(limit * 0.60)),
        "cited_by_high_signal": max(1, round(limit * 0.15)),
        "adjacent": max(1, round(limit * 0.15)),
        "semantic_similar": max(1, round(limit * 0.10)),
        "openalex_related": max(1, round(limit * 0.10)),
        "s2_related": max(1, round(limit * 0.10)),
    }
    taken = {k: 0 for k in quotas}
    out: list[dict] = []
    leftover: list[dict] = []
    for item in suggestions:
        bucket = str(item.get("suggestion_type") or "")
        if taken.get(bucket, 0) < quotas.get(bucket, 0):
            out.append(item)
            taken[bucket] = taken.get(bucket, 0) + 1
            if len(out) >= limit:
                return out
        else:
            leftover.append(item)
    # Still short of limit — fill from leftover (already in weighted order).
    for item in leftover:
        if len(out) >= limit:
            break
        out.append(item)
    return out


def _load_author_suggestion_weights(db: sqlite3.Connection) -> dict[str, float]:
    """Read `author_suggestion_weights.*` from discovery_settings."""

    from alma.discovery.defaults import merge_discovery_defaults

    try:
        rows = db.execute(
            "SELECT key, value FROM discovery_settings "
            "WHERE key LIKE 'author_suggestion_weights.%'"
        ).fetchall()
        stored = {row["key"]: row["value"] for row in rows}
    except sqlite3.OperationalError:
        stored = {}
    merged = merge_discovery_defaults(stored)
    out: dict[str, float] = {}
    for key, value in merged.items():
        if not key.startswith("author_suggestion_weights."):
            continue
        bucket = key[len("author_suggestion_weights."):]
        try:
            out[bucket] = max(0.0, float(value))
        except (TypeError, ValueError):
            out[bucket] = 0.0
    return out


def delete_author(db: sqlite3.Connection, author_id: str) -> Optional[dict]:
    """Delete one author and orphaned papers linked only to them.

    Side effect: writes a hard `missing_author_feedback` remove signal
    keyed by the author's OpenAlex ID so the network suggestion rails
    (`/authors/suggestions`, OpenAlex/S2 cached buckets) suppress this
    author for ~250+ days. Without that, the next OpenAlex co-author
    expansion happily re-discovers the author and they re-appear in
    the rail one refresh later.
    """
    row = db.execute("SELECT id, name, openalex_id FROM authors WHERE id = ?", (author_id,)).fetchone()
    if not row:
        return None
    openalex_id = str((row["openalex_id"] if isinstance(row, sqlite3.Row) else "") or "").strip()
    if openalex_id:
        db.execute(
            """
            DELETE FROM papers WHERE id IN (
                SELECT pa.paper_id
                FROM publication_authors pa
                WHERE pa.openalex_id = ?
                AND NOT EXISTS (
                    SELECT 1
                    FROM publication_authors pa2
                    JOIN authors a2 ON a2.openalex_id = pa2.openalex_id
                    WHERE pa2.paper_id = pa.paper_id AND a2.id != ?
                )
            )
            """,
            (openalex_id, author_id),
        )
        db.execute("DELETE FROM publication_authors WHERE openalex_id = ?", (openalex_id,))

        # Record the hard suppression BEFORE removing the author row —
        # `missing_author_feedback` is keyed only by openalex_id so it
        # survives the row delete. Best-effort: do not fail the delete
        # if the feedback insert raises.
        try:
            from alma.application.gap_radar import record_missing_author_remove

            record_missing_author_remove(db, openalex_id, hard=True)
        except Exception:
            logger.debug(
                "missing_author_feedback insert failed during delete_author %s",
                author_id,
                exc_info=True,
            )
    db.execute("DELETE FROM authors WHERE id = ?", (author_id,))
    return {"id": row["id"], "name": row["name"]}


def touch_last_fetched(db: sqlite3.Connection, author_id: str) -> bool:
    """Mark an author as fetched now."""
    cursor = db.execute(
        "UPDATE authors SET last_fetched_at = ? WHERE id = ?",
        (datetime.utcnow().isoformat(), author_id),
    )
    return cursor.rowcount > 0
