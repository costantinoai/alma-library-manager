"""Import orchestration use-cases."""

from __future__ import annotations

import sqlite3


def list_resolution_queue(
    db: sqlite3.Connection,
    *,
    unresolved_only: bool = True,
    limit: int = 200,
) -> list[dict]:
    """List imported papers pending enrichment/resolution."""
    where = [
        """(
            COALESCE(added_from, '') = 'import'
            OR COALESCE(notes, '') LIKE 'Imported from %'
        )"""
    ]
    if unresolved_only:
        where.append(
            """(
                openalex_resolution_status IS NULL
                OR openalex_resolution_status IN (
                    '',
                    'pending',
                    'unresolved',
                    'pending_enrichment',
                    'not_openalex_resolved'
                )
            )"""
        )

    rows = db.execute(
        f"""
        SELECT
            id,
            title,
            doi,
            url,
            openalex_id,
            openalex_resolution_status,
            openalex_resolution_reason,
            year,
            authors,
            fetched_at
        FROM papers
        WHERE {" AND ".join(where)}
        ORDER BY COALESCE(fetched_at, '') DESC, title
        LIMIT ?
        """,
        (limit,),
    ).fetchall()
    return [dict(r) for r in rows]
