"""Atomic map-selection → collection-backed Discovery lens workflow.

Map hosts select only dots in their current payload. This application boundary
re-validates that scope, resolves author dots to their in-scope papers, then
creates the collection, Library memberships, and lens in one write unit. A
failure at any step leaves none of those artifacts behind.
"""

from __future__ import annotations

import sqlite3
from collections.abc import Iterable

from alma.ai.projections import MIN_AUTHOR_PUBLICATIONS
from alma.application import discovery as discovery_app
from alma.application import library as library_app
from alma.application.graph_substrate import SUBSTRATE_SCOPE
from alma.core.db_write import run_write_unit
from alma.core.scope import Scope

_COLLECTION_COLOR = "#1E5B86"
_SQL_CHUNK_SIZE = 400


def _clean_ids(values: Iterable[str]) -> list[str]:
    return list(
        dict.fromkeys(str(value or "").strip() for value in values if str(value or "").strip())
    )


def _chunks(values: list[str]) -> Iterable[list[str]]:
    for start in range(0, len(values), _SQL_CHUNK_SIZE):
        yield values[start : start + _SQL_CHUNK_SIZE]


def _scoped_paper_ids(
    conn: sqlite3.Connection,
    ids: list[str],
    scope: Scope,
) -> list[str]:
    """Return selected paper ids in request order, rejecting scope leakage."""
    found: set[str] = set()
    scope_filter = scope.paper_filter("p")
    for chunk in _chunks(ids):
        placeholders = ",".join("?" for _ in chunk)
        rows = conn.execute(
            f"""
            SELECT p.id
            FROM papers p
            WHERE p.id IN ({placeholders})
              {scope_filter}
            """,
            chunk,
        ).fetchall()
        found.update(str(row["id"]) for row in rows)
    missing = [paper_id for paper_id in ids if paper_id not in found]
    if missing:
        raise ValueError(
            f"{len(missing)} selected paper(s) are outside the current {scope.label()} map"
        )
    return ids


def _visible_author_ids(
    conn: sqlite3.Connection,
    ids: list[str],
    scope: Scope,
) -> set[str]:
    """Mirror author-map admission and scope membership for selected ids."""
    found: set[str] = set()
    scope_filter = scope.paper_filter("scope_p")
    for chunk in _chunks(ids):
        placeholders = ",".join("?" for _ in chunk)
        rows = conn.execute(
            f"""
            WITH placeable AS (
                SELECT pa.openalex_id AS author_id
                FROM publication_authors pa
                JOIN publication_clusters pc
                  ON pc.paper_id = pa.paper_id
                 AND pc.scope = ?
                WHERE pa.openalex_id IN ({placeholders})
                  AND TRIM(COALESCE(pa.openalex_id, '')) <> ''
                GROUP BY pa.openalex_id
                HAVING COUNT(DISTINCT pc.paper_id) >= ?
            ),
            in_scope AS (
                SELECT DISTINCT scope_pa.openalex_id AS author_id
                FROM publication_authors scope_pa
                JOIN papers scope_p ON scope_p.id = scope_pa.paper_id
                WHERE scope_pa.openalex_id IN ({placeholders})
                  {scope_filter}
            )
            SELECT placeable.author_id
            FROM placeable
            JOIN in_scope USING (author_id)
            """,
            (SUBSTRATE_SCOPE, *chunk, MIN_AUTHOR_PUBLICATIONS, *chunk),
        ).fetchall()
        found.update(str(row["author_id"]) for row in rows)
    return found


def _papers_for_authors(
    conn: sqlite3.Connection,
    ids: list[str],
    scope: Scope,
) -> list[str]:
    visible = _visible_author_ids(conn, ids, scope)
    missing = [author_id for author_id in ids if author_id not in visible]
    if missing:
        raise ValueError(
            f"{len(missing)} selected author(s) are outside the current {scope.label()} map"
        )

    paper_ids: set[str] = set()
    scope_filter = scope.paper_filter("p")
    for chunk in _chunks(ids):
        placeholders = ",".join("?" for _ in chunk)
        rows = conn.execute(
            f"""
            SELECT DISTINCT p.id
            FROM publication_authors pa
            JOIN papers p ON p.id = pa.paper_id
            WHERE pa.openalex_id IN ({placeholders})
              {scope_filter}
            ORDER BY p.id
            """,
            chunk,
        ).fetchall()
        paper_ids.update(str(row["id"]) for row in rows)
    return sorted(paper_ids)


def _unique_name(conn: sqlite3.Connection, requested: str) -> str:
    base = " ".join(str(requested or "").split()).strip()
    if not base:
        raise ValueError("Lens name is required")
    existing = {
        str(row["name"]).casefold()
        for row in conn.execute(
            "SELECT name FROM collections WHERE lower(name) = lower(?) OR lower(name) LIKE lower(?)",
            (base, f"{base} %"),
        ).fetchall()
    }
    if base.casefold() not in existing:
        return base
    suffix = 2
    while f"{base} {suffix}".casefold() in existing:
        suffix += 1
    return f"{base} {suffix}"


def create_collection_lens(
    conn: sqlite3.Connection,
    *,
    name: str,
    selection_kind: str,
    ids: list[str],
    scope: Scope,
) -> dict:
    """Create collection + memberships + lens atomically from one lasso."""
    selected_ids = _clean_ids(ids)
    if not selected_ids:
        raise ValueError("Select at least one visible map item")
    if selection_kind == "papers":
        paper_ids = _scoped_paper_ids(conn, selected_ids, scope)
    elif selection_kind == "authors":
        paper_ids = _papers_for_authors(conn, selected_ids, scope)
    else:
        raise ValueError("Selection kind must be papers or authors")
    if not paper_ids:
        raise ValueError("Selection contains no papers in the current map scope")

    def _persist() -> dict:
        unique_name = _unique_name(conn, name)
        collection_id = library_app.create_collection(
            conn,
            unique_name,
            description=(
                f"Created from {len(selected_ids)} {selection_kind} selected "
                f"on the {scope.label()} map."
            ),
            color=_COLLECTION_COLOR,
        )
        for paper_id in paper_ids:
            library_app.save_paper_to_collections(
                conn,
                paper_id,
                [collection_id],
                added_from="map_selection",
            )
        lens = discovery_app.create_lens(
            conn,
            name=unique_name,
            context_type="collection",
            context_config={"collection_id": collection_id},
        )
        return {
            "collection_id": collection_id,
            "lens_id": str(lens["id"]),
            "name": unique_name,
            "paper_count": len(paper_ids),
        }

    return run_write_unit(conn, _persist, label="map_selection_create_lens")
