"""Offline candidate-frontier construction and local reads.

Frontier rows are leads, not corpus papers. Network collection happens only in
the background builder; Discovery refreshes read these tables locally.
"""

from __future__ import annotations

import json
import logging
import sqlite3
import uuid
from collections.abc import Iterable
from concurrent.futures import as_completed
from dataclasses import dataclass
from datetime import timedelta
from typing import Any

from alma.core.concurrency import bounded_thread_pool
from alma.core.sql_helpers import standalone_paper_sql
from alma.core.time import utcnow
from alma.core.utils import candidate_dedup_key
from alma.core.vector_blob import decode_vector, encode_vector
from alma.discovery import semantic_scholar
from alma.openalex.client import (
    _normalize_work,
    batch_fetch_works_by_openalex_ids,
    fetch_citing_works_for_openalex_id,
)

DEFAULT_BUILD_LIMIT = 1500
DEFAULT_EXPIRY_DAYS = 30
_VECTOR_FETCH_BATCH = 400
_DISCOVERY_QUERY_LIMIT = 6
_DISCOVERY_RESULTS_PER_QUERY = 40

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class FrontierBuildResult:
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


def run_frontier_maintenance(
    db: sqlite3.Connection,
    *,
    job_id: str | None = None,
) -> dict[str, dict[str, int]]:
    """THE frontier build sequence: coupling → discovery queries → vectors.

    One owner, two triggers. The periodic scheduler job and the manual
    ``POST /discovery/frontier/rebuild`` button both call this, so the order of
    the three phases and the commit discipline between them are stated once.

    Each phase gathers over HTTP with no transaction open and then performs one
    bounded write. The commit BETWEEN phases is not cosmetic: it closes the
    write txn before the next phase starts its network calls, which is
    CLAUDE.md's "never hold a write txn across network I/O".

    Args:
        db: Open connection owned by the caller (a background runner's own
            connection, never a request's).
        job_id: Activity job to narrate into, when there is one.

    Returns:
        ``{phase_name: FrontierBuildResult.as_dict()}`` for the three phases.
    """
    from alma.core.db_write import commit_unless_gated

    def _log(message: str, step: str, data: dict[str, int] | None = None) -> None:
        if not job_id:
            return
        from alma.api.scheduler import add_job_log

        add_job_log(job_id, message, step=step, data=data)

    phases: dict[str, dict[str, int]] = {}

    coupling = build_frontier(db)
    commit_unless_gated(db, label="discovery frontier build")
    phases["frontier"] = coupling.as_dict()
    _log(
        f"Bibliographic coupling: {coupling.rows_written} leads, "
        f"{coupling.edges_written} edges",
        "frontier",
        phases["frontier"],
    )

    discovery = build_discovery_frontier(db)
    commit_unless_gated(db, label="discovery source frontier build")
    phases["discovery_frontier"] = discovery.as_dict()
    _log(
        f"Query + taste frontier: {discovery.rows_written} leads",
        "discovery_frontier",
        phases["discovery_frontier"],
    )

    vectors = fill_frontier_vectors(db)
    commit_unless_gated(db, label="discovery frontier vector fill")
    phases["vectors"] = vectors.as_dict()
    _log(
        f"Vectors: {vectors.vectors_written} filled, "
        f"{vectors.terminal_ids} terminal, {vectors.retryable_ids} retryable",
        "vectors",
        phases["vectors"],
    )

    return phases


def coupling_candidates(
    db: sqlite3.Connection,
    *,
    limit: int = DEFAULT_BUILD_LIMIT,
    min_overlap: int = 1,
) -> list[tuple[str, int]]:
    """Return uncached works cited by Library papers, ordered by overlap."""

    rows = db.execute(
        f"""
        SELECT CAST(pr.referenced_work_id AS TEXT) AS rid,
               COUNT(DISTINCT pr.paper_id) AS overlap
        FROM publication_references pr
        JOIN papers p ON p.id = pr.paper_id
        WHERE p.status = 'library'
          AND {standalone_paper_sql('p')}
          AND NOT EXISTS (
              SELECT 1 FROM papers q
              WHERE q.openalex_id = 'W' || pr.referenced_work_id
          )
          AND NOT EXISTS (
              SELECT 1 FROM discovery_frontier f
              WHERE f.openalex_id = 'W' || pr.referenced_work_id
                AND f.terminal_at IS NULL
                AND (f.expires_at IS NULL OR f.expires_at > ?)
          )
        GROUP BY pr.referenced_work_id
        HAVING overlap >= ?
        ORDER BY overlap DESC, pr.referenced_work_id ASC
        LIMIT ?
        """,
        (utcnow().isoformat(), max(1, int(min_overlap)), max(1, int(limit))),
    ).fetchall()
    return [(str(row["rid"]), int(row["overlap"] or 0)) for row in rows]


def build_frontier(
    db: sqlite3.Connection,
    *,
    limit: int = DEFAULT_BUILD_LIMIT,
    min_overlap: int = 1,
) -> FrontierBuildResult:
    """Resolve one resumable bibliographic-coupling slice into the frontier.

    Phase 1 gathers over OpenAlex with no transaction. Phase 2 performs one
    bounded DB write. Vector fill is a separate network/write phase so neither
    HTTP provider is called while SQLite owns a write lock.
    """

    coupling = coupling_candidates(db, limit=limit, min_overlap=min_overlap)
    if not coupling:
        return FrontierBuildResult(0, 0, 0, 0, 0, 0)

    overlap_by_work = {f"W{work_id}": overlap for work_id, overlap in coupling}
    work_ids = list(overlap_by_work)

    # Network phase.
    raw_by_id = batch_fetch_works_by_openalex_ids(
        work_ids,
        batch_size=50,
        max_workers=4,
    )

    candidates_by_work: dict[str, dict[str, Any]] = {}
    for work_id in work_ids:
        raw = raw_by_id.get(work_id)
        if not raw:
            continue
        metadata = _frontier_metadata(_normalize_work(raw))
        if not metadata["title"]:
            continue
        metadata["coupling_count"] = int(overlap_by_work[work_id])
        metadata["source_api"] = "openalex"
        metadata["_field_provenance"] = {
            "coupling_count": ["local"],
        }
        candidates_by_work[work_id] = metadata

    # Resolve every local source edge before starting the canonical write.
    gathered: list[dict[str, Any]] = []
    if candidates_by_work:
        bare_ids = [work_id.removeprefix("W") for work_id in candidates_by_work]
        placeholders = ",".join("?" for _ in bare_ids)
        edge_rows = db.execute(
            f"""
            SELECT pr.paper_id, CAST(pr.referenced_work_id AS TEXT) AS rid
            FROM publication_references pr
            JOIN papers p ON p.id = pr.paper_id
            WHERE p.status = 'library'
              AND {standalone_paper_sql('p')}
              AND CAST(pr.referenced_work_id AS TEXT) IN ({placeholders})
            """,
            bare_ids,
        ).fetchall()
        for row in edge_rows:
            candidate = candidates_by_work.get(f"W{row['rid']}")
            if not candidate:
                continue
            gathered.append(
                {
                    **candidate,
                    "_frontier_source_key": f"paper:{row['paper_id']}",
                    "_frontier_relation": "reference",
                }
            )

    rows_written, edges_written = upsert_frontier_candidates(db, gathered)

    return FrontierBuildResult(
        candidates_considered=len(coupling),
        rows_written=rows_written,
        vectors_written=0,
        edges_written=edges_written,
        terminal_ids=0,
        retryable_ids=0,
    )


def build_discovery_frontier(
    db: sqlite3.Connection,
    *,
    query_limit: int = _DISCOVERY_QUERY_LIMIT,
    per_query_limit: int = _DISCOVERY_RESULTS_PER_QUERY,
) -> FrontierBuildResult:
    """Gather taste/query/S2 candidates offline and persist one bounded slice."""

    from alma.discovery import source_search

    settings = {
        str(row["key"]): str(row["value"])
        for row in db.execute("SELECT key, value FROM discovery_settings").fetchall()
    }
    query_rows = db.execute(
        f"""
        SELECT pt.term, SUM(COALESCE(pt.score, 0.0)) AS strength
        FROM publication_topics pt
        JOIN papers p ON p.id = pt.paper_id
        WHERE p.status = 'library'
          AND COALESCE(TRIM(pt.term), '') != ''
          AND {standalone_paper_sql('p')}
        GROUP BY LOWER(TRIM(pt.term))
        ORDER BY strength DESC, pt.term
        LIMIT ?
        """,
        (max(1, int(query_limit)),),
    ).fetchall()
    queries = [str(row["term"]).strip() for row in query_rows if row["term"]]

    gathered: list[dict] = []
    if queries:
        pool = bounded_thread_pool(
            min(3, len(queries)),
            thread_name_prefix="frontier-query",
        )
        try:
            future_map = {
                pool.submit(
                    source_search.search_across_sources,
                    query,
                    limit=max(1, int(per_query_limit)),
                    from_year=utcnow().year - 6,
                    settings=settings,
                    mode="core",
                    temperature=0.28,
                    semantic_scholar_mode="interactive",
                    lane_deadline_s=20.0,
                ): query
                for query in queries
            }
            for future in as_completed(future_map):
                query = future_map[future]
                try:
                    candidates = future.result() or []
                except Exception as exc:
                    logger.warning(
                        "Frontier query failed for %r: %s",
                        query,
                        exc,
                    )
                    candidates = []
                for candidate in candidates:
                    gathered.append(
                        {
                            **candidate,
                            "_frontier_source_key": f"query:{query}",
                            "_frontier_relation": "search",
                        }
                    )
        finally:
            pool.shutdown(wait=False)

    positive, negative = _s2_seed_identifiers(db)
    if positive:
        for candidate in semantic_scholar.recommend_from_seeds(
            positive,
            negative,
            limit=max(50, int(per_query_limit)),
        ):
            gathered.append(
                {
                    **candidate,
                    "_frontier_source_key": "s2:taste_recommend",
                    "_frontier_relation": "recommendation",
                }
            )
        for seed_id in positive[:6]:
            for candidate in semantic_scholar.recommend_for_paper(
                seed_id,
                limit=max(20, int(per_query_limit)),
                pool=semantic_scholar.RECOMMENDATION_POOL_ALL,
            ):
                gathered.append(
                    {
                        **candidate,
                        "_frontier_source_key": f"s2:all_time:{seed_id}",
                        "_frontier_relation": "related",
                    }
                )

    # Reverse citation neighbourhood: foundational/reference candidates alone
    # miss newer work that builds on the user's strongest papers.
    for seed_id in _positive_openalex_seed_ids(db, limit=4):
        for raw in fetch_citing_works_for_openalex_id(
            seed_id,
            limit=max(20, int(per_query_limit)),
        ):
            candidate = _frontier_metadata(_normalize_work(raw))
            candidate["source_api"] = "openalex"
            gathered.append(
                {
                    **candidate,
                    "_frontier_source_key": f"openalex:cites:{seed_id}",
                    "_frontier_relation": "citation",
                }
            )

    rows_written, edges_written = upsert_frontier_candidates(db, gathered)
    return FrontierBuildResult(
        candidates_considered=len(gathered),
        rows_written=rows_written,
        vectors_written=0,
        edges_written=edges_written,
        terminal_ids=0,
        retryable_ids=0,
    )


def upsert_frontier_candidates(
    db: sqlite3.Connection,
    candidates: Iterable[dict],
) -> tuple[int, int]:
    """Canonical persistence route for heterogeneous frontier candidates."""

    prepared: dict[str, dict] = {}
    edge_specs: list[tuple[str, str, str, str]] = []
    for raw in candidates:
        if not isinstance(raw, dict):
            continue
        candidate = {
            key: value
            for key, value in raw.items()
            if key
            not in {
                "retrieval_hits",
                "_frontier_source_key",
                "_frontier_relation",
            }
        }
        title = str(candidate.get("title") or "").strip()
        if not title:
            continue
        candidate["openalex_id"] = _canonical_openalex_work_id(
            candidate.get("openalex_id")
        )
        frontier_key = candidate_dedup_key(candidate)
        if frontier_key in {"url:", "title:"}:
            continue
        source_api = str(candidate.get("source_api") or "unknown").strip()
        raw_source_apis = candidate.get("source_apis")
        source_apis = [
            str(value).strip()
            for value in (
                raw_source_apis if isinstance(raw_source_apis, list) else []
            )
            if str(value).strip()
        ]
        if source_api not in source_apis:
            source_apis.append(source_api)
        candidate["source_apis"] = source_apis
        declared_provenance = raw.get("_field_provenance")
        provenance = {
            field: list(source_apis)
            for field, value in candidate.items()
            if not field.startswith("_")
            and value not in (None, "", [], {})
        }
        if isinstance(declared_provenance, dict):
            for field, values in declared_provenance.items():
                normalized = [
                    str(value).strip()
                    for value in (
                        values if isinstance(values, list) else [values]
                    )
                    if str(value).strip()
                ]
                if normalized:
                    provenance[str(field)] = normalized
        candidate["_field_provenance"] = provenance
        existing = prepared.get(frontier_key)
        prepared[frontier_key] = (
            _merge_frontier_metadata(existing, candidate)
            if existing is not None
            else candidate
        )
        edge_specs.append(
            (
                str(raw.get("_frontier_source_key") or f"api:{source_api}"),
                frontier_key,
                str(raw.get("_frontier_relation") or "discovery"),
                source_api,
            )
        )
    if not prepared:
        return 0, 0

    keys = list(prepared)
    for offset in range(0, len(keys), 400):
        chunk = keys[offset : offset + 400]
        placeholders = ",".join("?" for _ in chunk)
        rows = db.execute(
            f"""
            SELECT frontier_key, metadata, field_provenance
            FROM discovery_frontier
            WHERE frontier_key IN ({placeholders})
            """,
            chunk,
        ).fetchall()
        for row in rows:
            try:
                existing = json.loads(row["metadata"] or "{}")
            except (TypeError, ValueError):
                existing = {}
            if isinstance(existing, dict):
                key = str(row["frontier_key"])
                try:
                    existing_provenance = json.loads(
                        row["field_provenance"] or "{}"
                    )
                except (TypeError, ValueError):
                    existing_provenance = {}
                if isinstance(existing_provenance, dict):
                    existing["_field_provenance"] = existing_provenance
                prepared[key] = _merge_frontier_metadata(
                    existing,
                    prepared[key],
                )

    now = utcnow()
    observed_at = now.isoformat()
    expires_at = (now + timedelta(days=DEFAULT_EXPIRY_DAYS)).isoformat()
    frontier_rows: list[tuple[Any, ...]] = []
    for frontier_key, candidate in prepared.items():
        vector = candidate.get("specter2_embedding")
        model = str(candidate.get("specter2_model") or "").strip()
        vector_blob = (
            encode_vector(vector)
            if isinstance(vector, list)
            and vector
            and model == semantic_scholar.S2_SPECTER2_MODEL
            else None
        )
        provenance = candidate.get("_field_provenance") or {}
        stored_candidate = dict(candidate)
        stored_candidate.pop("specter2_embedding", None)
        stored_candidate.pop("_field_provenance", None)
        frontier_rows.append(
            (
                frontier_key,
                str(candidate.get("title") or "").strip(),
                str(candidate.get("authors") or "").strip(),
                str(candidate.get("doi") or "").strip(),
                str(candidate.get("openalex_id") or "").strip(),
                str(candidate.get("semantic_scholar_id") or "").strip(),
                candidate.get("year"),
                str(candidate.get("journal") or "").strip(),
                int(candidate.get("cited_by_count") or 0),
                int(candidate.get("coupling_count") or 0),
                json.dumps(
                    stored_candidate,
                    ensure_ascii=False,
                    separators=(",", ":"),
                ),
                vector_blob,
                model or None,
                json.dumps(provenance, ensure_ascii=False, separators=(",", ":")),
                observed_at,
                observed_at,
                expires_at,
            )
        )
    db.executemany(
        """
        INSERT INTO discovery_frontier (
            frontier_key, title, authors, doi, openalex_id, s2_id, year,
            venue, cited_by_count, coupling_count, metadata, vector,
            vector_model, field_provenance, first_seen_at, last_seen_at,
            expires_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(frontier_key) DO UPDATE SET
            title = excluded.title,
            authors = excluded.authors,
            doi = excluded.doi,
            openalex_id = excluded.openalex_id,
            s2_id = COALESCE(NULLIF(excluded.s2_id, ''), discovery_frontier.s2_id),
            year = excluded.year,
            venue = excluded.venue,
            cited_by_count = MAX(
                discovery_frontier.cited_by_count,
                excluded.cited_by_count
            ),
            coupling_count = MAX(
                discovery_frontier.coupling_count,
                excluded.coupling_count
            ),
            metadata = excluded.metadata,
            vector = COALESCE(excluded.vector, discovery_frontier.vector),
            vector_model = COALESCE(
                excluded.vector_model,
                discovery_frontier.vector_model
            ),
            field_provenance = excluded.field_provenance,
            last_seen_at = excluded.last_seen_at,
            expires_at = excluded.expires_at,
            terminal_at = NULL,
            last_error = NULL
        """,
        frontier_rows,
    )
    edge_rows = [
        (
            str(uuid.uuid4()),
            source_key,
            destination_key,
            relation,
            source_api,
            observed_at,
            "{}",
        )
        for source_key, destination_key, relation, source_api in edge_specs
    ]
    db.executemany(
        """
        INSERT INTO discovery_frontier_edges (
            id, source_key, destination_key, relation, source_api,
            observed_at, metadata_json
        ) VALUES (?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(source_key, destination_key, relation, source_api)
        DO UPDATE SET observed_at = excluded.observed_at
        """,
        edge_rows,
    )
    return len(frontier_rows), len(edge_rows)


def pending_vector_identifiers(
    db: sqlite3.Connection,
    *,
    limit: int = DEFAULT_BUILD_LIMIT,
) -> list[str]:
    """Return stable S2 lookup identifiers for vector-less frontier rows."""

    rows = db.execute(
        """
        SELECT doi, s2_id
        FROM discovery_frontier
        WHERE vector IS NULL
          AND terminal_at IS NULL
        ORDER BY coupling_count DESC, first_seen_at ASC
        LIMIT ?
        """,
        (max(1, int(limit)),),
    ).fetchall()
    return [
        f"DOI:{str(row['doi']).strip()}"
        if str(row["doi"] or "").strip()
        else str(row["s2_id"] or "").strip()
        for row in rows
        if str(row["doi"] or "").strip() or str(row["s2_id"] or "").strip()
    ]


def fill_frontier_vectors(
    db: sqlite3.Connection,
    *,
    limit: int = DEFAULT_BUILD_LIMIT,
) -> FrontierBuildResult:
    """Fetch one bounded S2 vector slice, then persist it without HTTP in txn."""

    identifiers = pending_vector_identifiers(db, limit=limit)
    if not identifiers:
        return FrontierBuildResult(0, 0, 0, 0, 0, 0)

    # Network phase.
    outcome = semantic_scholar.fetch_vectors_for_identifiers(
        identifiers,
        batch_size=_VECTOR_FETCH_BATCH,
    )
    now = utcnow().isoformat()
    vector_updates: list[tuple[bytes, str, str, str, str]] = []
    for requested_id, paper in outcome.papers_by_requested_id.items():
        vector = semantic_scholar.extract_specter2_vector(paper)
        if not vector:
            continue
        model = str(
            paper.get("specter2_model")
            or ((paper.get("embedding") or {}).get("model"))
            or semantic_scholar.S2_SPECTER2_MODEL
        ).strip()
        bare = _strip_lookup_prefix(requested_id)
        s2_id = str(paper.get("paperId") or "").strip()
        vector_updates.append((encode_vector(vector), model, s2_id, now, bare))

    # Write phase.
    if vector_updates:
        db.executemany(
            """
            UPDATE discovery_frontier
               SET vector = ?,
                   vector_model = ?,
                   s2_id = COALESCE(NULLIF(?, ''), s2_id),
                   last_seen_at = ?,
                   field_provenance = json_set(
                       field_provenance,
                       '$.vector', json_array('semantic_scholar'),
                       '$.vector_model', json_array('semantic_scholar')
                   ),
                   terminal_at = NULL,
                   last_error = NULL
             WHERE LOWER(COALESCE(doi, '')) = LOWER(?)
                OR COALESCE(s2_id, '') = ?
            """,
            [
                (blob, model, s2_id, seen_at, bare, bare)
                for blob, model, s2_id, seen_at, bare in vector_updates
            ],
        )
    _mark_vector_failures(
        db,
        outcome.terminal_ids,
        terminal=True,
        observed_at=now,
    )
    _mark_vector_failures(
        db,
        outcome.retryable_ids,
        terminal=False,
        observed_at=now,
    )
    return FrontierBuildResult(
        candidates_considered=len(identifiers),
        rows_written=0,
        vectors_written=len(vector_updates),
        edges_written=0,
        terminal_ids=len(outcome.terminal_ids),
        retryable_ids=len(outcome.retryable_ids),
    )


def load_frontier_vectors(
    db: sqlite3.Connection,
    *,
    model: str,
) -> list[tuple[str, Any, dict[str, Any]]]:
    """Load non-expired frontier vectors in exactly one embedding space."""

    rows = db.execute(
        """
        SELECT frontier_key, title, authors, doi, openalex_id, s2_id, year,
               venue, cited_by_count, coupling_count, metadata, vector
        FROM discovery_frontier
        WHERE vector IS NOT NULL
          AND vector_model = ?
          AND terminal_at IS NULL
          AND (expires_at IS NULL OR expires_at > ?)
        """,
        (model, utcnow().isoformat()),
    ).fetchall()
    out: list[tuple[str, Any, dict[str, Any]]] = []
    for row in rows:
        try:
            vector = decode_vector(row["vector"])
            metadata = json.loads(row["metadata"] or "{}")
        except (TypeError, ValueError):
            continue
        if not isinstance(metadata, dict):
            metadata = {}
        metadata.update(
            {
                "title": row["title"] or "",
                "authors": row["authors"] or "",
                "doi": row["doi"] or "",
                "openalex_id": row["openalex_id"] or "",
                "semantic_scholar_id": row["s2_id"] or "",
                "year": row["year"],
                "journal": row["venue"] or "",
                "cited_by_count": row["cited_by_count"] or 0,
                "coupling_count": row["coupling_count"] or 0,
            }
        )
        out.append((str(row["frontier_key"]), vector, metadata))
    return out


def load_frontier_candidates(
    db: sqlite3.Connection,
    frontier_keys: Iterable[str],
) -> dict[str, dict[str, Any]]:
    """Load rich candidate rows by canonical key in bounded SQL chunks."""

    keys = list(dict.fromkeys(str(key) for key in frontier_keys if str(key)))
    out: dict[str, dict[str, Any]] = {}
    for offset in range(0, len(keys), 400):
        chunk = keys[offset : offset + 400]
        placeholders = ",".join("?" for _ in chunk)
        rows = db.execute(
            f"""
            SELECT frontier_key, title, authors, doi, openalex_id, s2_id, year,
                   venue, cited_by_count, coupling_count, metadata,
                   field_provenance
            FROM discovery_frontier
            WHERE frontier_key IN ({placeholders})
              AND terminal_at IS NULL
              AND (expires_at IS NULL OR expires_at > ?)
            """,
            [*chunk, utcnow().isoformat()],
        ).fetchall()
        for row in rows:
            try:
                candidate = json.loads(row["metadata"] or "{}")
            except (TypeError, ValueError):
                candidate = {}
            if not isinstance(candidate, dict):
                candidate = {}
            try:
                field_provenance = json.loads(
                    row["field_provenance"] or "{}"
                )
            except (TypeError, ValueError):
                field_provenance = {}
            candidate.update(
                {
                    "title": row["title"] or "",
                    "authors": row["authors"] or "",
                    "doi": row["doi"] or "",
                    "openalex_id": row["openalex_id"] or "",
                    "semantic_scholar_id": row["s2_id"] or "",
                    "year": row["year"],
                    "journal": row["venue"] or "",
                    "cited_by_count": row["cited_by_count"] or 0,
                    "coupling_count": row["coupling_count"] or 0,
                    "frontier_key": row["frontier_key"],
                    "field_provenance": (
                        field_provenance
                        if isinstance(field_provenance, dict)
                        else {}
                    ),
                }
            )
            out[str(row["frontier_key"])] = candidate
    return out


def load_live_frontier(db: sqlite3.Connection) -> list[dict[str, Any]]:
    """Load every non-expired lead through the canonical frontier reader."""

    keys = [
        str(row["frontier_key"])
        for row in db.execute(
            """
            SELECT frontier_key
            FROM discovery_frontier
            WHERE terminal_at IS NULL
              AND (expires_at IS NULL OR expires_at > ?)
            ORDER BY coupling_count DESC, last_seen_at DESC
            """,
            (utcnow().isoformat(),),
        ).fetchall()
    ]
    return list(load_frontier_candidates(db, keys).values())


def search_frontier(
    candidates: Iterable[dict[str, Any]],
    query: str,
    *,
    limit: int,
) -> list[dict[str, Any]]:
    """Pure local query over a preloaded frontier candidate list."""

    from alma.core.scoring_math import query_match_score, query_tokens

    normalized, tokens = query_tokens(query)
    ranked: list[dict[str, Any]] = []
    for raw in candidates:
        score = query_match_score(normalized, tokens, raw)
        if score <= 0.0:
            continue
        ranked.append(
            {
                **raw,
                "score": score,
                "source_type": "frontier_search",
                "source_api": "local",
                "source_key": query,
            }
        )
    ranked.sort(key=lambda candidate: float(candidate["score"]), reverse=True)
    return ranked[: max(1, int(limit))]


def _s2_seed_identifiers(
    db: sqlite3.Connection,
) -> tuple[list[str], list[str]]:
    rows = db.execute(
        f"""
        SELECT semantic_scholar_id, semantic_scholar_corpus_id, doi,
               status, rating
        FROM papers
        WHERE (
            (status = 'library' AND COALESCE(rating, 0) >= 4)
            OR status IN ('dismissed', 'removed')
            OR COALESCE(rating, 0) BETWEEN 1 AND 2
        )
          AND (
              COALESCE(TRIM(semantic_scholar_id), '') != ''
              OR COALESCE(TRIM(semantic_scholar_corpus_id), '') != ''
              OR COALESCE(TRIM(doi), '') != ''
          )
          AND {standalone_paper_sql('papers')}
        ORDER BY COALESCE(rating, 0) DESC, added_at DESC
        LIMIT 150
        """
    ).fetchall()

    def identifier(row: sqlite3.Row) -> str:
        s2_id = str(row["semantic_scholar_id"] or "").strip()
        if s2_id:
            return s2_id
        doi = str(row["doi"] or "").strip()
        if doi:
            return f"DOI:{doi}"
        corpus_id = str(row["semantic_scholar_corpus_id"] or "").strip()
        return f"CorpusID:{corpus_id}" if corpus_id else ""

    positive: list[str] = []
    negative: list[str] = []
    for row in rows:
        value = identifier(row)
        if not value:
            continue
        if row["status"] == "library" and int(row["rating"] or 0) >= 4:
            positive.append(value)
        else:
            negative.append(value)
    return positive[:100], negative[:100]


def _positive_openalex_seed_ids(
    db: sqlite3.Connection,
    *,
    limit: int,
) -> list[str]:
    rows = db.execute(
        f"""
        SELECT openalex_id
        FROM papers
        WHERE status = 'library'
          AND COALESCE(rating, 0) >= 4
          AND openalex_id GLOB 'W[0-9]*'
          AND {standalone_paper_sql('papers')}
        ORDER BY COALESCE(rating, 0) DESC, added_at DESC
        LIMIT ?
        """,
        (max(1, int(limit)),),
    ).fetchall()
    return [
        str(row["openalex_id"])
        for row in rows
        if str(row["openalex_id"] or "").strip()
    ]


def _merge_frontier_metadata(
    existing: dict[str, Any],
    incoming: dict[str, Any],
) -> dict[str, Any]:
    """Merge fields from independent providers without winner-only loss."""

    merged = dict(existing)
    union_fields = {
        "authorships",
        "institutions",
        "topics",
        "keywords",
        "referenced_works",
        "related_works",
        "counts_by_year",
        "source_apis",
    }
    max_fields = {
        "cited_by_count",
        "influential_citation_count",
        "coupling_count",
    }
    for field, value in incoming.items():
        current = merged.get(field)
        if field == "_field_provenance":
            left = current if isinstance(current, dict) else {}
            right = value if isinstance(value, dict) else {}
            combined: dict[str, list[str]] = {}
            for name in set(left) | set(right):
                left_sources = left.get(name) or []
                right_sources = right.get(name) or []
                combined[name] = list(
                    dict.fromkeys(
                        [
                            str(source)
                            for source in [
                                *(
                                    left_sources
                                    if isinstance(left_sources, list)
                                    else [left_sources]
                                ),
                                *(
                                    right_sources
                                    if isinstance(right_sources, list)
                                    else [right_sources]
                                ),
                            ]
                            if str(source).strip()
                        ]
                    )
                )
            merged[field] = combined
        elif field in union_fields:
            left = current if isinstance(current, list) else []
            right = value if isinstance(value, list) else []
            seen = {json.dumps(item, sort_keys=True, default=str) for item in left}
            merged[field] = list(left)
            for item in right:
                key = json.dumps(item, sort_keys=True, default=str)
                if key not in seen:
                    seen.add(key)
                    merged[field].append(item)
        elif field in max_fields:
            merged[field] = max(
                float(current or 0.0),
                float(value or 0.0),
            )
        elif field == "specter2_embedding":
            if value:
                merged[field] = value
        elif isinstance(current, dict) and isinstance(value, dict):
            merged[field] = {**current, **value}
        elif current in (None, "", [], {}) and value not in (None, "", [], {}):
            merged[field] = value
    return merged


def _frontier_metadata(normalized: dict[str, Any]) -> dict[str, Any]:
    metadata = dict(normalized)
    metadata["url"] = metadata.pop("pub_url", metadata.get("url") or "")
    metadata["cited_by_count"] = int(
        metadata.pop("num_citations", metadata.get("cited_by_count") or 0) or 0
    )
    metadata["semantic_scholar_id"] = str(
        metadata.get("semantic_scholar_id") or ""
    )
    metadata["openalex_id"] = _canonical_openalex_work_id(
        metadata.get("openalex_id")
    )
    return metadata


def _canonical_openalex_work_id(value: object) -> str:
    text = str(value or "").strip().rstrip("/")
    if not text:
        return ""
    bare = text.rsplit("/", 1)[-1]
    return bare if bare.upper().startswith("W") else text


def _mark_vector_failures(
    db: sqlite3.Connection,
    identifiers: Iterable[str],
    *,
    terminal: bool,
    observed_at: str,
) -> None:
    updates = []
    for identifier in identifiers:
        bare = _strip_lookup_prefix(identifier)
        updates.append(
            (
                observed_at if terminal else None,
                "terminal_no_vector" if terminal else "retryable_vector_fetch",
                observed_at,
                bare,
                bare,
            )
        )
    if not updates:
        return
    db.executemany(
        """
        UPDATE discovery_frontier
           SET terminal_at = ?, last_error = ?, last_seen_at = ?
         WHERE LOWER(COALESCE(doi, '')) = LOWER(?)
            OR COALESCE(s2_id, '') = ?
        """,
        updates,
    )


def _strip_lookup_prefix(identifier: str) -> str:
    text = str(identifier or "").strip()
    for prefix in ("DOI:", "CorpusId:", "CorpusID:", "ARXIV:", "PMID:", "PMCID:"):
        if text.upper().startswith(prefix.upper()):
            return text[len(prefix) :]
    return text
