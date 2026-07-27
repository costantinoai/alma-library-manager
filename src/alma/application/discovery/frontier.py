"""Candidate frontier — the bounded citation neighbourhood, built offline.

Why this exists
---------------

Discovery's dense lane could only ever return papers already in the local
corpus: it kNN's over ``publication_embeddings JOIN papers``. So the single
heaviest retrieval channel structurally contributed **zero new papers**, and
every genuinely new candidate had to come from live text search inside the
request — which is why a lens refresh measured 191 s average / 792 s worst
against a <10 s budget.

The frontier separates *universe construction* from *ranking*:

* **Offline** (this module, a background job): walk the citation graph outward
  from the Library, resolve the neighbourhood's metadata and SPECTER2 vectors,
  and persist them.
* **Online** (the vector lane): one numpy matmul against a preloaded matrix.

The candidate universe is bounded by the user's own citation neighbourhood
rather than by a global index — the same shape Connected Papers uses. On the
dev corpus that is 5,178 works cited by a Library paper with no metadata yet,
against 395,560 stored reference edges: the graph needed to seed it is already
local, so tier A costs no discovery calls at all.

What is deliberately NOT here
-----------------------------

A frontier row is a **lead**, not a corpus citizen. It is not a ``papers`` row
and carries no membership state (D2), so it never enters the map, Insights
counts, dedup, or any preference query. It is promoted into ``papers`` only
when it is actually staged as a recommendation.

Schema (``discovery_frontier``, ``discovery_frontier_edges``) is Agent C's
under task 62 §9; this module reads and writes it and degrades to a no-op when
the tables are absent.
"""

from __future__ import annotations

import json
import logging
import sqlite3
from dataclasses import dataclass
from typing import Any

from alma.core.sql_helpers import standalone_paper_sql
from alma.core.time import utcnow
from alma.core.vector_blob import encode_vector
from alma.discovery import semantic_scholar

logger = logging.getLogger(__name__)

# Reference edges are stored unprefixed; OpenAlex work ids are 'W' + digits.
_WORK_PREFIX = "W"

# How many un-hydrated referenced works one build pass resolves. Bounded so a
# first run on a large corpus finishes in minutes rather than hours; the job is
# resumable and simply picks up the next slice on the following run.
DEFAULT_BUILD_LIMIT = 1500

# Identifier batch handed to the S2 transport primitive. B's
# `fetch_vectors_for_identifiers` does its own byte/id chunking beneath this
# (task 62 §4.2) — we pass intent, not chunk math.
_VECTOR_FETCH_BATCH = 400


@dataclass(frozen=True)
class FrontierBuildResult:
    """What one build pass did, for Activity reporting and tests."""

    candidates_considered: int
    rows_written: int
    vectors_written: int
    edges_written: int
    terminal_ids: int
    retryable_ids: int

    def as_dict(self) -> dict[str, int]:
        return {
            "candidates_considered": self.candidates_considered,
            "rows_written": self.rows_written,
            "vectors_written": self.vectors_written,
            "edges_written": self.edges_written,
            "terminal_ids": self.terminal_ids,
            "retryable_ids": self.retryable_ids,
        }


def frontier_tables_ready(db: sqlite3.Connection) -> bool:
    """True when C's frontier schema is present on this database."""
    rows = db.execute(
        """
        SELECT name FROM sqlite_master
        WHERE type = 'table' AND name IN ('discovery_frontier', 'discovery_frontier_edges')
        """
    ).fetchall()
    return len({str(r["name"]) for r in rows}) == 2


def coupling_candidates(
    db: sqlite3.Connection, *, limit: int = DEFAULT_BUILD_LIMIT, min_overlap: int = 1
) -> list[tuple[str, int]]:
    """Referenced works that the Library cites but the corpus does not hold.

    This is **bibliographic coupling** (Kessler 1963) computed entirely from
    local edges: a work cited by N of your Library papers shares an
    intellectual base with them, and the more of your papers cite it the
    stronger that shared base. Ranking by that overlap is why tier A needs no
    API call to decide *what* to fetch — only to fetch it.

    Args:
        db: Open connection.
        limit: Maximum works to return.
        min_overlap: Minimum number of distinct Library papers that must cite a
            work before it is worth resolving. 1 admits the long tail; 2+
            restricts to the canon.

    Returns:
        ``(unprefixed_work_id, library_overlap)`` ordered by overlap desc.
    """
    try:
        rows = db.execute(
            f"""
            SELECT pr.referenced_work_id AS rid,
                   COUNT(DISTINCT pr.paper_id) AS overlap
            FROM publication_references pr
            JOIN papers p ON p.id = pr.paper_id
            WHERE p.status = 'library'
              AND {standalone_paper_sql('p')}
              AND COALESCE(TRIM(pr.referenced_work_id), '') != ''
              AND NOT EXISTS (
                  SELECT 1 FROM papers q
                  WHERE q.openalex_id = ? || pr.referenced_work_id
              )
            GROUP BY pr.referenced_work_id
            HAVING overlap >= ?
            ORDER BY overlap DESC, pr.referenced_work_id ASC
            LIMIT ?
            """,
            (_WORK_PREFIX, int(min_overlap), int(limit)),
        ).fetchall()
    except sqlite3.OperationalError as exc:
        logger.warning("frontier coupling query failed: %s", exc)
        return []
    return [(str(r["rid"]), int(r["overlap"] or 0)) for r in rows]


def pending_vector_identifiers(
    db: sqlite3.Connection, *, limit: int = DEFAULT_BUILD_LIMIT
) -> list[str]:
    """Frontier rows that still need a SPECTER2 vector, as S2 lookup ids.

    Prefers a DOI (S2 resolves `DOI:` reliably and it is the identifier most
    frontier rows have); falls back to an S2 paper id when one is known.
    """
    if not frontier_tables_ready(db):
        return []
    # Identity lives in `metadata_json` (Agent C's schema): the table stores a
    # canonical key plus a metadata blob rather than a column per identifier.
    rows = db.execute(
        """
        SELECT json_extract(metadata_json, '$.doi')                 AS doi,
               json_extract(metadata_json, '$.semantic_scholar_id') AS s2_id
        FROM discovery_frontier
        WHERE vector IS NULL
          AND terminal_at IS NULL
        ORDER BY first_seen_at ASC
        LIMIT ?
        """,
        (int(limit),),
    ).fetchall()

    out: list[str] = []
    for row in rows:
        doi = str(row["doi"] or "").strip()
        s2_id = str(row["s2_id"] or "").strip()
        if doi:
            out.append(f"DOI:{doi}")
        elif s2_id:
            out.append(s2_id)
    return out


def fill_frontier_vectors(
    db: sqlite3.Connection, *, limit: int = DEFAULT_BUILD_LIMIT
) -> FrontierBuildResult:
    """Fetch and persist SPECTER2 vectors for frontier rows that lack one.

    Network happens here, in a background job, never in a refresh. The two
    phases are deliberately separated — gather over the network first, then
    write — because a write transaction must never be held across HTTP
    (`CLAUDE.md` → SQLite write discipline rule 2).

    Returns:
        A :class:`FrontierBuildResult`; all-zero when the schema is absent or
        nothing needed filling.
    """
    empty = FrontierBuildResult(0, 0, 0, 0, 0, 0)
    if not frontier_tables_ready(db):
        return empty

    identifiers = pending_vector_identifiers(db, limit=limit)
    if not identifiers:
        return empty

    # --- Phase 1: network only. No open transaction. ---
    outcome = semantic_scholar.fetch_vectors_for_identifiers(
        identifiers, batch_size=_VECTOR_FETCH_BATCH
    )

    updates: list[tuple[bytes, str, str, str]] = []
    now = utcnow().isoformat()
    for requested_id, paper in (outcome.papers_by_requested_id or {}).items():
        vector = semantic_scholar.extract_specter2_vector(paper)
        if not vector:
            continue
        model = str(((paper or {}).get("embedding") or {}).get("model") or "").strip()
        updates.append(
            (
                encode_vector(vector),
                model or semantic_scholar.S2_SPECTER2_MODEL,
                now,
                _strip_lookup_prefix(requested_id),
            )
        )

    # --- Phase 2: write only. ---
    written = 0
    if updates:
        db.executemany(
            """
            UPDATE discovery_frontier
               SET vector = ?, vector_model = ?, last_seen_at = ?
             WHERE LOWER(COALESCE(json_extract(metadata_json, '$.doi'), '')) = LOWER(?)
                OR COALESCE(json_extract(metadata_json, '$.semantic_scholar_id'), '') = ?
            """,
            [(blob, model, ts, ident, ident) for blob, model, ts, ident in updates],
        )
        written = len(updates)

    return FrontierBuildResult(
        candidates_considered=len(identifiers),
        rows_written=0,
        vectors_written=written,
        edges_written=0,
        terminal_ids=len(outcome.terminal_ids or ()),
        retryable_ids=len(outcome.retryable_ids or ()),
    )


def load_frontier_vectors(
    db: sqlite3.Connection, *, model: str
) -> list[tuple[str, Any, dict[str, Any]]]:
    """Load every frontier row that carries a vector in ``model``'s space.

    Returned as ``(frontier_key, vector, metadata)`` so the vector lane can
    treat frontier rows and corpus rows uniformly. Mixing vector spaces is
    refused rather than silently cosined: a vector from a different model is
    not comparable to the seed centroid and would produce confident nonsense.
    """
    if not frontier_tables_ready(db):
        return []
    from alma.core.vector_blob import decode_vector

    rows = db.execute(
        """
        SELECT candidate_key, vector, metadata_json
        FROM discovery_frontier
        WHERE vector IS NOT NULL AND vector_model = ?
        """,
        (model,),
    ).fetchall()

    out: list[tuple[str, Any, dict[str, Any]]] = []
    for row in rows:
        try:
            vector = decode_vector(row["vector"])
        except Exception:
            continue
        try:
            metadata = json.loads(row["metadata_json"] or "{}")
        except (TypeError, ValueError):
            metadata = {}
        if not isinstance(metadata, dict):
            continue
        out.append(
            (
                str(row["candidate_key"]),
                vector,
                {
                    "title": metadata.get("title") or "",
                    "authors": metadata.get("authors") or "",
                    "doi": metadata.get("doi") or "",
                    "openalex_id": metadata.get("openalex_id") or "",
                    "semantic_scholar_id": metadata.get("semantic_scholar_id") or "",
                    "year": metadata.get("year"),
                    "journal": metadata.get("journal") or metadata.get("venue") or "",
                    "cited_by_count": metadata.get("cited_by_count") or 0,
                },
            )
        )
    return out


def _strip_lookup_prefix(identifier: str) -> str:
    """`DOI:10.x/y` → `10.x/y`; a bare paperId is returned unchanged."""
    text = str(identifier or "").strip()
    for prefix in ("DOI:", "CorpusId:", "CorpusID:", "ARXIV:", "PMID:", "PMCID:"):
        if text.upper().startswith(prefix.upper()):
            return text[len(prefix) :]
    return text
