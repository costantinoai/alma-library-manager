"""Which OpenAlex author id a merged-away id now means.

OpenAlex keeps splitting people, and it keeps delivering works under the split
ids, so a merge only holds if every authorship write goes through this mapping.
A merged-away id on a re-delivered paper used to be written back as it came,
which undid the merge one paper at a time.

The mapping reads `author_alt_identifiers`, but only the rows that record a
merge the user agreed to. An ORCID discovery alias (`record_orcid_aliases`)
suppresses a suggestion and says nothing about whose papers these are, so it
never redirects.
"""

from __future__ import annotations

import sqlite3

AUTHOR_MERGES_DDL = """
    CREATE TABLE IF NOT EXISTS author_merges (
        id TEXT PRIMARY KEY,
        primary_author_id TEXT NOT NULL,
        primary_openalex_id TEXT NOT NULL,
        alt_author_id TEXT,
        alt_openalex_id TEXT,
        alt_name TEXT,
        papers_moved INTEGER NOT NULL DEFAULT 0,
        snapshot_json TEXT NOT NULL,
        merged_at TEXT NOT NULL,
        undone_at TEXT
    )
"""
AUTHOR_MERGES_INDEX_DDL = (
    "CREATE INDEX IF NOT EXISTS idx_author_merges_primary ON author_merges(primary_author_id)"
)

#: Alias provenance that means "these two profiles were merged".
MERGED_ALIAS_SOURCES: tuple[str, ...] = ("manual_merge", "suggestion_import")


def merged_author_redirects(conn: sqlite3.Connection) -> dict[str, str]:
    """``lower(merged-away id) -> surviving openalex_id``, chains flattened.

    A chain ends at a row that is not itself merged away. A cycle, or a chain
    that ends on a removed row, has no survivor, so those ids do not redirect:
    keeping the id as delivered loses nothing, and Health can still see it.
    """
    marks = ",".join("?" for _ in MERGED_ALIAS_SOURCES)
    try:
        rows = conn.execute(
            f"""
            SELECT alias.alt_openalex_id AS alt, author.openalex_id AS survivor,
                   COALESCE(author.status, 'active') AS status
            FROM author_alt_identifiers alias
            JOIN authors author ON author.id = alias.primary_author_id
            WHERE alias.source IN ({marks})
              AND COALESCE(TRIM(author.openalex_id), '') <> ''
            """,  # noqa: S608 — placeholders only
            MERGED_ALIAS_SOURCES,
        ).fetchall()
    except sqlite3.OperationalError:
        return {}

    step: dict[str, tuple[str, str]] = {}
    for row in rows:
        alt = str(row[0] or "").strip().lower()
        survivor = str(row[1] or "").strip()
        if alt and alt != survivor.lower():
            step[alt] = (survivor, str(row[2]))

    redirects: dict[str, str] = {}
    for alt in step:
        seen = {alt}
        survivor, status = step[alt]
        while survivor.lower() in step:
            if survivor.lower() in seen:
                status = "removed"
                break
            seen.add(survivor.lower())
            survivor, status = step[survivor.lower()]
        if status != "removed":
            redirects[alt] = survivor
    return redirects


def canonical_author_oid(
    conn: sqlite3.Connection,
    openalex_id: str | None,
    *,
    redirects: dict[str, str] | None = None,
) -> str:
    """The id an authorship is stored under. ``redirects`` spares a loop the read."""
    oid = str(openalex_id or "").strip()
    if not oid:
        return oid
    mapping = merged_author_redirects(conn) if redirects is None else redirects
    return mapping.get(oid.lower(), oid)


def merged_ids_of(conn: sqlite3.Connection, openalex_id: str | None) -> list[str]:
    """Every merged-away id that now means this author: what a monitor must also ask for."""
    oid = str(openalex_id or "").strip().lower()
    if not oid:
        return []
    return sorted(
        alt.upper()
        for alt, survivor in merged_author_redirects(conn).items()
        if survivor.lower() == oid
    )
