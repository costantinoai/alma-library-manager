"""Author neighbourhood — a per-author ego-network for the 3D explorer.

Builds the graph *around one author*: their co-authors (shared-paper edges),
their citation neighbours (authors their work cites or is cited by, via
`publication_references`), and their intellectual neighbours (authors with a
close SPECTER2 centroid). Plus the co-authorship edges *among* those
neighbours, so the result is a navigable graph rather than a bare star.

This is **lazy and bounded by design** — nothing here runs until the
neighbourhood dialog asks for it, and every relation is capped, so the cost is
a handful of indexed queries (+ one centroid pass) regardless of corpus size.
It is deliberately NOT precomputed or cached server-side: the canonical author
*signal* is the always-on aggregate; this ego-network is an on-demand view.
"""

from __future__ import annotations

import logging
import sqlite3
from typing import Optional

logger = logging.getLogger(__name__)

# Per-relation caps — keep the 3D scene legible and the queries cheap.
_LIMIT_COAUTHOR = 20
_LIMIT_CITATION = 12
_LIMIT_SIMILAR = 12

# Candidate pool for the similar-author centroid pass (top authors by embedded
# paper count, mirroring the suggestion rail's bound).
_SIMILAR_CANDIDATE_POOL = 200

# Node colour is chosen on the frontend from `relation`; priority decides the
# primary relation when an author shows up in more than one.
_RELATION_PRIORITY = {"coauthor": 0, "citation": 1, "similar": 2}


def _table_exists(db: sqlite3.Connection, table: str) -> bool:
    return (
        db.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name=?",
            (table,),
        ).fetchone()
        is not None
    )


def _resolve_author(db: sqlite3.Connection, author_id: str) -> Optional[dict]:
    row = db.execute(
        "SELECT id, name, openalex_id FROM authors WHERE id = ?",
        (author_id,),
    ).fetchone()
    if not row:
        return None
    return {
        "id": row["id"],
        "name": str(row["name"] or "").strip(),
        "openalex_id": str(row["openalex_id"] or "").strip(),
    }


def _coauthors(db: sqlite3.Connection, oid: str) -> list[dict]:
    """Authors who share a byline with this author, by shared-paper count."""
    try:
        rows = db.execute(
            """
            SELECT lower(pa2.openalex_id) AS oid,
                   MAX(pa2.display_name) AS name,
                   COUNT(DISTINCT pa2.paper_id) AS shared
            FROM publication_authors pa1
            JOIN publication_authors pa2
              ON pa2.paper_id = pa1.paper_id
             AND lower(pa2.openalex_id) <> lower(pa1.openalex_id)
            WHERE lower(pa1.openalex_id) = ?
              AND COALESCE(TRIM(pa2.openalex_id), '') <> ''
            GROUP BY lower(pa2.openalex_id)
            ORDER BY shared DESC, name ASC
            LIMIT ?
            """,
            (oid, _LIMIT_COAUTHOR),
        ).fetchall()
    except sqlite3.OperationalError:
        return []
    return [
        {"oid": str(r["oid"]), "name": str(r["name"] or ""), "relation": "coauthor",
         "weight": float(r["shared"] or 0)}
        for r in rows
        if str(r["oid"] or "").strip()
    ]


def _citation_neighbours(db: sqlite3.Connection, oid: str) -> list[dict]:
    """Authors of locally-known works this author cites OR is cited by."""
    if not _table_exists(db, "publication_references"):
        return []
    counts: dict[str, float] = {}
    names: dict[str, str] = {}

    def _collect(sql: str) -> None:
        try:
            rows = db.execute(sql, (oid, oid)).fetchall()
        except sqlite3.OperationalError:
            return
        for r in rows:
            o = str(r["oid"] or "").strip()
            if not o:
                continue
            counts[o] = counts.get(o, 0.0) + float(r["n"] or 0)
            names.setdefault(o, str(r["name"] or ""))

    # Outbound — authors of works THIS author references. `referenced_work_id`
    # is the bare integer work id; `papers.openalex_id` is W-prefixed, so the
    # join is on `openalex_id = 'W' || referenced_work_id` (rides the unique
    # idx_papers_openalex_id). Author ids match `lower(openalex_id)` exactly so
    # the self-join uses idx_pubauthors_oid_lower.
    _collect(
        """
        WITH my_papers AS (
            SELECT DISTINCT pa.paper_id
            FROM publication_authors pa
            WHERE lower(pa.openalex_id) = ?
        ),
        refs AS (
            SELECT DISTINCT pr.referenced_work_id AS wid
            FROM publication_references pr
            JOIN my_papers mp ON mp.paper_id = pr.paper_id
            WHERE pr.referenced_work_id IS NOT NULL
        )
        SELECT lower(pa.openalex_id) AS oid,
               MAX(pa.display_name) AS name,
               COUNT(DISTINCT p.id) AS n
        FROM refs r
        JOIN papers p ON p.openalex_id = ('W' || r.wid)
        JOIN publication_authors pa ON pa.paper_id = p.id
        WHERE lower(pa.openalex_id) <> ?
          AND COALESCE(TRIM(pa.openalex_id), '') <> ''
        GROUP BY lower(pa.openalex_id)
        """
    )
    # Inbound — authors of works that reference THIS author's works. Mirror of
    # the outbound shape: this author's works become bare integer ids
    # (`SUBSTR(openalex_id, 2)`) so they match `referenced_work_id` directly,
    # and the `citing` join rides idx_publication_references_ref.
    _collect(
        """
        WITH my_works AS (
            SELECT DISTINCT CAST(SUBSTR(p.openalex_id, 2) AS INTEGER) AS wid
            FROM publication_authors pa
            JOIN papers p ON p.id = pa.paper_id
            WHERE lower(pa.openalex_id) = ?
              AND p.openalex_id LIKE 'W%'
        ),
        citing AS (
            SELECT DISTINCT pr.paper_id
            FROM publication_references pr
            JOIN my_works mw ON mw.wid = pr.referenced_work_id
        )
        SELECT lower(pa.openalex_id) AS oid,
               MAX(pa.display_name) AS name,
               COUNT(DISTINCT c.paper_id) AS n
        FROM citing c
        JOIN publication_authors pa ON pa.paper_id = c.paper_id
        WHERE lower(pa.openalex_id) <> ?
          AND COALESCE(TRIM(pa.openalex_id), '') <> ''
        GROUP BY lower(pa.openalex_id)
        """
    )

    ranked = sorted(counts.items(), key=lambda kv: (-kv[1], kv[0]))[:_LIMIT_CITATION]
    return [
        {"oid": o, "name": names.get(o, ""), "relation": "citation", "weight": float(n)}
        for o, n in ranked
    ]


def _similar_authors(db: sqlite3.Connection, oid: str) -> list[dict]:
    """Authors whose SPECTER2 centroid is closest to this author's centroid."""
    if not _table_exists(db, "author_centroids"):
        return []
    try:
        import numpy as np

        from alma.core.vector_blob import cosine_similarity, decode_vector
        from alma.discovery.similarity import get_active_embedding_model
    except ImportError:
        return []
    model = get_active_embedding_model(db)
    if not model:
        return []
    try:
        seed_row = db.execute(
            """
            SELECT centroid_blob FROM author_centroids
            WHERE model = ? AND lower(author_openalex_id) = ?
            """,
            (model, oid),
        ).fetchone()
    except sqlite3.OperationalError:
        return []
    if not seed_row or not seed_row["centroid_blob"]:
        return []
    try:
        seed = decode_vector(seed_row["centroid_blob"])
    except Exception:
        return []
    dim = int(seed.shape[0])

    # Bounded candidate pool: the most-embedded authors. Join names from
    # publication_authors so the nodes have labels.
    try:
        rows = db.execute(
            """
            SELECT lower(ac.author_openalex_id) AS oid,
                   ac.centroid_blob AS blob,
                   COALESCE((SELECT MAX(pa.display_name)
                             FROM publication_authors pa
                             WHERE lower(pa.openalex_id) = lower(ac.author_openalex_id)), '') AS name
            FROM author_centroids ac
            WHERE ac.model = ?
              AND lower(ac.author_openalex_id) <> ?
            ORDER BY ac.paper_count DESC
            LIMIT ?
            """,
            (model, oid, _SIMILAR_CANDIDATE_POOL),
        ).fetchall()
    except sqlite3.OperationalError:
        return []

    scored: list[tuple[float, str, str]] = []
    for r in rows:
        o = str(r["oid"] or "").strip()
        blob = r["blob"]
        if not o or not blob:
            continue
        try:
            vec = decode_vector(blob, expected_dim=dim)
        except Exception:
            continue
        if vec.shape[0] != dim:
            continue
        sim = cosine_similarity(seed, vec)
        if sim <= 0.0:
            continue
        scored.append((sim, o, str(r["name"] or "")))
    scored.sort(reverse=True)
    return [
        {"oid": o, "name": name, "relation": "similar", "weight": round(float(sim), 4)}
        for sim, o, name in scored[:_LIMIT_SIMILAR]
    ]


def _inter_neighbour_edges(db: sqlite3.Connection, oids: list[str]) -> list[dict]:
    """Co-authorship edges *among* the selected neighbours (so the graph is a
    navigable mesh, not just a star)."""
    if len(oids) < 2:
        return []
    placeholders = ",".join("?" * len(oids))
    try:
        rows = db.execute(
            f"""
            SELECT lower(pa1.openalex_id) AS a,
                   lower(pa2.openalex_id) AS b,
                   COUNT(DISTINCT pa1.paper_id) AS w
            FROM publication_authors pa1
            JOIN publication_authors pa2 ON pa2.paper_id = pa1.paper_id
            WHERE lower(pa1.openalex_id) IN ({placeholders})
              AND lower(pa2.openalex_id) IN ({placeholders})
              AND lower(pa1.openalex_id) < lower(pa2.openalex_id)
            GROUP BY a, b
            """,
            (*oids, *oids),
        ).fetchall()
    except sqlite3.OperationalError:
        return []
    return [
        {"source": str(r["a"]), "target": str(r["b"]), "relation": "coauthor",
         "weight": float(r["w"] or 0)}
        for r in rows
    ]


def _enrich(db: sqlite3.Connection, oids: list[str]) -> dict[str, dict]:
    """Pull name / affiliation / citation count from `authors` for the
    neighbour oids (display only; falls back to publication_authors name)."""
    if not oids:
        return {}
    placeholders = ",".join("?" * len(oids))
    try:
        rows = db.execute(
            f"""
            SELECT lower(openalex_id) AS oid, name, affiliation, citedby
            FROM authors
            WHERE lower(openalex_id) IN ({placeholders})
            """,
            tuple(oids),
        ).fetchall()
    except sqlite3.OperationalError:
        return {}
    return {
        str(r["oid"]): {
            "name": str(r["name"] or "").strip(),
            "affiliation": str(r["affiliation"] or "").strip() or None,
            "citedby": int(r["citedby"]) if r["citedby"] is not None else None,
        }
        for r in rows
        if str(r["oid"] or "").strip()
    }


def build_author_neighbourhood(db: sqlite3.Connection, author_id: str) -> Optional[dict]:
    """Ego-network around ``author_id`` for the 3D explorer.

    Returns ``{center, nodes, links, counts}`` (react-force-graph shape) or
    ``None`` if the author doesn't exist. ``empty=True`` (with the center node
    only) when the author has no resolved OpenAlex ID or no graph neighbours
    yet — the UI renders a friendly empty state rather than an error.
    """
    author = _resolve_author(db, author_id)
    if author is None:
        return None
    oid = author["openalex_id"].lower()
    center_node = {
        "id": author["id"],
        "oid": oid,
        "name": author["name"] or "This author",
        "relation": "center",
        "weight": 0.0,
        "is_center": True,
    }
    if not oid or not _table_exists(db, "publication_authors"):
        return {"center": center_node, "nodes": [center_node], "links": [],
                "counts": {"coauthor": 0, "citation": 0, "similar": 0}, "empty": True}

    # Gather neighbours, dedup by oid keeping the highest-priority relation.
    by_oid: dict[str, dict] = {}
    for cand in (*_coauthors(db, oid), *_citation_neighbours(db, oid), *_similar_authors(db, oid)):
        o = cand["oid"]
        if o == oid:
            continue
        existing = by_oid.get(o)
        if existing is None or (
            _RELATION_PRIORITY[cand["relation"]] < _RELATION_PRIORITY[existing["relation"]]
        ):
            # Keep both weights so a coauthor that is also similar still ranks
            # by collaboration but the similarity is preserved for the tooltip.
            merged = dict(cand)
            if existing:
                merged["weight"] = max(cand["weight"], existing["weight"])
            by_oid[o] = merged

    neighbour_oids = list(by_oid.keys())
    enriched = _enrich(db, neighbour_oids)

    nodes = [center_node]
    counts = {"coauthor": 0, "citation": 0, "similar": 0}
    for o, cand in by_oid.items():
        meta = enriched.get(o, {})
        counts[cand["relation"]] = counts.get(cand["relation"], 0) + 1
        nodes.append({
            "id": o,
            "oid": o,
            "name": meta.get("name") or cand["name"] or o,
            "relation": cand["relation"],
            "weight": cand["weight"],
            "affiliation": meta.get("affiliation"),
            "citedby": meta.get("citedby"),
        })

    # Edges: center → each neighbour, plus co-authorship among neighbours.
    links = [
        {"source": author["id"], "target": o, "relation": cand["relation"],
         "weight": cand["weight"]}
        for o, cand in by_oid.items()
    ]
    links.extend(_inter_neighbour_edges(db, neighbour_oids))

    return {
        "center": center_node,
        "nodes": nodes,
        "links": links,
        "counts": counts,
        "empty": len(nodes) <= 1,
    }
