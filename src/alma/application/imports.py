"""Import orchestration use-cases."""

from __future__ import annotations

import sqlite3

from alma.core.sql_helpers import canonical_paper_filter
from alma.library.importer import unconfirmed_staged_import_sql


def _resolution_queue_where(unresolved_only: bool) -> str:
    """SQL WHERE body shared by the import review queue and its id-only variant.

    Rows are imported papers that are NOT merged into a canonical twin. When
    ``unresolved_only`` is set, the row must EITHER still be a staged
    (review-before-save) import — surfaced regardless of OpenAlex resolution
    status so background enrichment can't silently drop it from its only review
    surface (40.2) — OR be an identified import whose OpenAlex enrichment is
    still pending. One predicate, one owner, so the queue and the resolve-all
    target set can never drift (40.6).
    """
    where = [
        """(
            COALESCE(added_from, '') = 'import'
            OR COALESCE(notes, '') LIKE 'Imported from %'
        )""",
        canonical_paper_filter("papers"),
    ]
    if unresolved_only:
        where.append(
            f"""(
                {unconfirmed_staged_import_sql()}
                OR openalex_resolution_status IS NULL
                OR openalex_resolution_status IN (
                    '',
                    'pending',
                    'unresolved',
                    'pending_enrichment',
                    'not_openalex_resolved'
                )
            )"""
        )
    return " AND ".join(where)


def list_resolution_queue(
    db: sqlite3.Connection,
    *,
    unresolved_only: bool = True,
    limit: int = 200,
) -> list[dict]:
    """List imported papers pending enrichment/resolution or awaiting review."""
    rows = db.execute(
        f"""
        SELECT
            id,
            title,
            status,
            added_from,
            doi,
            url,
            openalex_id,
            openalex_resolution_status,
            openalex_resolution_reason,
            year,
            authors,
            fetched_at
        FROM papers
        WHERE {_resolution_queue_where(unresolved_only)}
        ORDER BY COALESCE(fetched_at, '') DESC, title
        LIMIT ?
        """,
        (limit,),
    ).fetchall()
    return [dict(r) for r in rows]


def resolution_queue_ids(
    db: sqlite3.Connection,
    *,
    unresolved_only: bool = True,
    limit: int = 1000,
) -> list[str]:
    """Return just the ids of the resolution queue — the single source of target
    ids for the resolve-all OpenAlex endpoint (40.6), so it can never re-resolve
    a canonical-merged row the queue already excludes."""
    rows = db.execute(
        f"""
        SELECT id
        FROM papers
        WHERE {_resolution_queue_where(unresolved_only)}
        ORDER BY COALESCE(openalex_resolution_updated_at, fetched_at, '') DESC
        LIMIT ?
        """,
        (limit,),
    ).fetchall()
    return [r["id"] if isinstance(r, sqlite3.Row) else r[0] for r in rows]
