"""Local citation retrieval over cached edges and frontier metadata."""

from __future__ import annotations

import json
import sqlite3
from typing import Any

from alma.core.sql_helpers import standalone_paper_sql

from ..ppr import SEED_LIBRARY, SEED_LOVED, compute_ppr_variants
from ._common import FAMILY_CITATION, _candidate_key, attach_hits


def _retrieve_graph_channel(
    db: sqlite3.Connection,
    lens: dict,
    seeds: list[dict],
    *,
    limit: int,
) -> tuple[list[dict], dict[str, Any]]:
    """Retrieve graph candidates without any HTTP on refresh hot path."""

    del lens
    seed_ids = [str(seed.get("id") or "").strip() for seed in seeds]
    seed_ids = [paper_id for paper_id in seed_ids if paper_id]
    if not seed_ids:
        return [], _summary()

    direct = _direct_reference_candidates(db, seed_ids, limit=max(limit * 2, 50))
    attach_hits(
        direct,
        family=FAMILY_CITATION,
        retriever_id="graph:direct_reference",
        source_api="local",
        relation="reference",
        query_key="lens_seeds",
    )

    coupling = _bibliographic_coupling_candidates(
        db,
        seed_ids,
        limit=max(limit * 2, 50),
    )
    attach_hits(
        coupling,
        family=FAMILY_CITATION,
        retriever_id="graph:bibliographic_coupling",
        source_api="local",
        relation="bibliographic_coupling",
        query_key="lens_seeds",
    )

    cocitation = _cocitation_candidates(
        db,
        seed_ids,
        limit=max(limit * 2, 50),
    )
    attach_hits(
        cocitation,
        family=FAMILY_CITATION,
        retriever_id="graph:cocitation",
        source_api="local",
        relation="cocitation",
        query_key="lens_seeds",
    )

    ppr_scores = compute_ppr_variants(
        db,
        top_k=max(limit * 3, 100),
    )
    ppr_library = _ppr_candidates(
        db,
        ppr_scores[SEED_LIBRARY],
        score_field="ppr_library",
    )
    attach_hits(
        ppr_library,
        family=FAMILY_CITATION,
        retriever_id="graph:ppr_library",
        source_api="local",
        relation="ppr",
        seed_key=SEED_LIBRARY,
    )

    ppr_loved = _ppr_candidates(
        db,
        ppr_scores[SEED_LOVED],
        score_field="ppr_loved",
    )
    attach_hits(
        ppr_loved,
        family=FAMILY_CITATION,
        retriever_id="graph:ppr_loved",
        source_api="local",
        relation="ppr",
        seed_key=SEED_LOVED,
    )

    merged: dict[str, dict] = {}
    for run in (direct, coupling, cocitation, ppr_library, ppr_loved):
        for candidate in run:
            key = _candidate_key(candidate)
            existing = merged.get(key)
            if existing is None:
                merged[key] = candidate
                continue
            existing.setdefault("retrieval_hits", []).extend(
                candidate.get("retrieval_hits") or []
            )
            for field in (
                "ppr_library",
                "ppr_loved",
                "coupling_count",
                "cocitation_count",
                "seed_overlap",
                "reference_support_count",
            ):
                if candidate.get(field) is not None:
                    existing[field] = max(
                        float(existing.get(field) or 0.0),
                        float(candidate.get(field) or 0.0),
                    )
            if float(candidate.get("score") or 0.0) > float(
                existing.get("score") or 0.0
            ):
                existing["score"] = candidate["score"]

    ranked = sorted(
        merged.values(),
        key=lambda item: float(item.get("score") or 0.0),
        reverse=True,
    )[: max(1, int(limit))]
    return ranked, {
        "seed_total": len(seed_ids),
        "direct_reference_candidates": len(direct),
        "bibliographic_coupling_candidates": len(coupling),
        "cocitation_candidates": len(cocitation),
        "ppr_library_candidates": len(ppr_library),
        "ppr_loved_candidates": len(ppr_loved),
        "network_calls": 0,
    }


def _direct_reference_candidates(
    db: sqlite3.Connection,
    seed_ids: list[str],
    *,
    limit: int,
) -> list[dict]:
    placeholders = ",".join("?" for _ in seed_ids)
    rows = db.execute(
        f"""
        SELECT CAST(pr.referenced_work_id AS TEXT) AS rid,
               COUNT(DISTINCT pr.paper_id) AS corpus_overlap,
               SUM(CASE WHEN pr.paper_id IN ({placeholders}) THEN 1 ELSE 0 END)
                   AS seed_overlap
        FROM publication_references pr
        WHERE pr.referenced_work_id IN (
            SELECT DISTINCT referenced_work_id
            FROM publication_references
            WHERE paper_id IN ({placeholders})
        )
        GROUP BY pr.referenced_work_id
        ORDER BY corpus_overlap DESC, seed_overlap DESC, pr.referenced_work_id
        LIMIT ?
        """,
        [*seed_ids, *seed_ids, max(1, int(limit))],
    ).fetchall()
    if not rows:
        return []

    work_ids = [f"W{row['rid']}" for row in rows]
    candidates = _candidates_by_openalex_id(db, work_ids)
    top_overlap = max(int(row["corpus_overlap"] or 0) for row in rows) or 1
    out: list[dict] = []
    for row in rows:
        work_id = f"W{row['rid']}"
        candidate = candidates.get(work_id)
        if not candidate:
            continue
        corpus_overlap = int(row["corpus_overlap"] or 0)
        candidate.update(
            {
                "score": round(corpus_overlap / top_overlap, 6),
                "reference_support_count": corpus_overlap,
                "seed_overlap": int(row["seed_overlap"] or 0),
                "source_type": "graph_reference",
                "source_api": "local",
                "source_key": "lens_seeds",
            }
        )
        out.append(candidate)
    return out


def _bibliographic_coupling_candidates(
    db: sqlite3.Connection,
    seed_ids: list[str],
    *,
    limit: int,
) -> list[dict]:
    """Local papers sharing references with the lens seeds."""

    placeholders = ",".join("?" for _ in seed_ids)
    rows = db.execute(
        f"""
        WITH seed_references AS (
            SELECT DISTINCT referenced_work_id
            FROM publication_references
            WHERE paper_id IN ({placeholders})
        )
        SELECT pr.paper_id,
               COUNT(DISTINCT pr.referenced_work_id) AS overlap
        FROM publication_references pr
        JOIN seed_references sr
          ON sr.referenced_work_id = pr.referenced_work_id
        WHERE pr.paper_id NOT IN ({placeholders})
        GROUP BY pr.paper_id
        ORDER BY overlap DESC, pr.paper_id
        LIMIT ?
        """,
        [*seed_ids, *seed_ids, max(1, int(limit))],
    ).fetchall()
    candidates = _candidates_by_paper_id(
        db,
        [str(row["paper_id"]) for row in rows],
    )
    top_overlap = max((int(row["overlap"] or 0) for row in rows), default=1)
    out: list[dict] = []
    for row in rows:
        paper_id = str(row["paper_id"])
        candidate = candidates.get(paper_id)
        if not candidate:
            continue
        overlap = int(row["overlap"] or 0)
        candidate.update(
            {
                "score": round(overlap / max(1, top_overlap), 6),
                "coupling_count": overlap,
                "source_type": "graph_coupling",
                "source_api": "local",
                "source_key": "lens_seeds",
            }
        )
        out.append(candidate)
    return out


def _cocitation_candidates(
    db: sqlite3.Connection,
    seed_ids: list[str],
    *,
    limit: int,
) -> list[dict]:
    """Works cited alongside a lens seed by papers in the local corpus."""

    placeholders = ",".join("?" for _ in seed_ids)
    seed_rows = db.execute(
        f"""
        SELECT CAST(SUBSTR(openalex_id, 2) AS INTEGER) AS work_id
        FROM papers
        WHERE id IN ({placeholders})
          AND openalex_id GLOB 'W[0-9]*'
        """,
        seed_ids,
    ).fetchall()
    seed_work_ids = [
        int(row["work_id"])
        for row in seed_rows
        if row["work_id"] is not None
    ]
    if not seed_work_ids:
        return []
    work_placeholders = ",".join("?" for _ in seed_work_ids)
    rows = db.execute(
        f"""
        WITH seed_citers AS (
            SELECT DISTINCT paper_id
            FROM publication_references
            WHERE referenced_work_id IN ({work_placeholders})
        )
        SELECT CAST(pr.referenced_work_id AS TEXT) AS rid,
               COUNT(DISTINCT pr.paper_id) AS cocitation_count
        FROM publication_references pr
        JOIN seed_citers sc ON sc.paper_id = pr.paper_id
        WHERE pr.referenced_work_id NOT IN ({work_placeholders})
        GROUP BY pr.referenced_work_id
        ORDER BY cocitation_count DESC, pr.referenced_work_id
        LIMIT ?
        """,
        [*seed_work_ids, *seed_work_ids, max(1, int(limit))],
    ).fetchall()
    candidates = _candidates_by_openalex_id(
        db,
        [f"W{row['rid']}" for row in rows],
    )
    top_count = max(
        (int(row["cocitation_count"] or 0) for row in rows),
        default=1,
    )
    out: list[dict] = []
    for row in rows:
        work_id = f"W{row['rid']}"
        candidate = candidates.get(work_id)
        if not candidate:
            continue
        count = int(row["cocitation_count"] or 0)
        candidate.update(
            {
                "score": round(count / max(1, top_count), 6),
                "cocitation_count": count,
                "source_type": "graph_cocitation",
                "source_api": "local",
                "source_key": "lens_seeds",
            }
        )
        out.append(candidate)
    return out


def _candidates_by_paper_id(
    db: sqlite3.Connection,
    paper_ids: list[str],
) -> dict[str, dict]:
    """Load current-schema paper candidates in bounded chunks."""

    out: dict[str, dict] = {}
    for offset in range(0, len(paper_ids), 400):
        chunk = paper_ids[offset : offset + 400]
        placeholders = ",".join("?" for _ in chunk)
        rows = db.execute(
            f"""
            SELECT id, title, authors, abstract, url, doi, openalex_id,
                   semantic_scholar_id, year, journal, cited_by_count
            FROM papers
            WHERE id IN ({placeholders})
              AND status NOT IN ('library', 'dismissed', 'removed')
              AND {standalone_paper_sql('papers')}
            """,
            chunk,
        ).fetchall()
        for row in rows:
            out[str(row["id"])] = {
                "paper_id": row["id"],
                "title": row["title"] or "",
                "authors": row["authors"] or "",
                "abstract": row["abstract"] or "",
                "url": row["url"] or "",
                "doi": row["doi"] or "",
                "openalex_id": row["openalex_id"] or "",
                "semantic_scholar_id": row["semantic_scholar_id"] or "",
                "year": row["year"],
                "journal": row["journal"] or "",
                "cited_by_count": row["cited_by_count"] or 0,
            }
    return out


def _candidates_by_openalex_id(
    db: sqlite3.Connection,
    work_ids: list[str],
) -> dict[str, dict]:
    work_ids = list(dict.fromkeys(work_ids))
    out: dict[str, dict] = {}
    for offset in range(0, len(work_ids), 400):
        chunk = work_ids[offset : offset + 400]
        placeholders = ",".join("?" for _ in chunk)
        paper_rows = db.execute(
            f"""
            SELECT id, title, authors, abstract, url, doi, openalex_id,
                   semantic_scholar_id, year, journal, cited_by_count
            FROM papers
            WHERE openalex_id IN ({placeholders})
              AND status NOT IN ('library', 'dismissed', 'removed')
              AND {standalone_paper_sql('papers')}
            """,
            chunk,
        ).fetchall()
        for row in paper_rows:
            out[str(row["openalex_id"])] = {
                "paper_id": row["id"],
                "title": row["title"] or "",
                "authors": row["authors"] or "",
                "abstract": row["abstract"] or "",
                "url": row["url"] or "",
                "doi": row["doi"] or "",
                "openalex_id": row["openalex_id"] or "",
                "semantic_scholar_id": row["semantic_scholar_id"] or "",
                "year": row["year"],
                "journal": row["journal"] or "",
                "cited_by_count": row["cited_by_count"] or 0,
            }

        frontier_rows = db.execute(
            f"""
            SELECT frontier_key, title, authors, doi, openalex_id, s2_id, year,
                   venue, cited_by_count, coupling_count, metadata
            FROM discovery_frontier
            WHERE openalex_id IN ({placeholders})
              AND terminal_at IS NULL
              AND (expires_at IS NULL OR expires_at > datetime('now'))
            """,
            chunk,
        ).fetchall()
        for row in frontier_rows:
            work_id = str(row["openalex_id"] or "")
            if work_id in out:
                continue
            try:
                candidate = json.loads(row["metadata"] or "{}")
            except (TypeError, ValueError):
                candidate = {}
            if not isinstance(candidate, dict):
                candidate = {}
            candidate.update(
                {
                    "frontier_key": row["frontier_key"],
                    "title": row["title"] or "",
                    "authors": row["authors"] or "",
                    "doi": row["doi"] or "",
                    "openalex_id": work_id,
                    "semantic_scholar_id": row["s2_id"] or "",
                    "year": row["year"],
                    "journal": row["venue"] or "",
                    "cited_by_count": row["cited_by_count"] or 0,
                    "coupling_count": row["coupling_count"] or 0,
                }
            )
            out[work_id] = candidate
    return out


def _ppr_candidates(
    db: sqlite3.Connection,
    scores: dict[str, float],
    *,
    score_field: str,
) -> list[dict]:
    if not scores:
        return []
    by_id: dict[str, dict] = {}
    paper_ids = list(scores)
    for offset in range(0, len(paper_ids), 400):
        chunk = paper_ids[offset : offset + 400]
        placeholders = ",".join("?" for _ in chunk)
        rows = db.execute(
            f"""
            SELECT id, title, authors, abstract, url, doi, openalex_id,
                   semantic_scholar_id, year, journal, cited_by_count
            FROM papers
            WHERE id IN ({placeholders})
              AND status NOT IN ('library', 'dismissed', 'removed')
              AND {standalone_paper_sql('papers')}
            """,
            chunk,
        ).fetchall()
        for row in rows:
            by_id[str(row["id"])] = {
                "paper_id": row["id"],
                "title": row["title"] or "",
                "authors": row["authors"] or "",
                "abstract": row["abstract"] or "",
                "url": row["url"] or "",
                "doi": row["doi"] or "",
                "openalex_id": row["openalex_id"] or "",
                "semantic_scholar_id": row["semantic_scholar_id"] or "",
                "year": row["year"],
                "journal": row["journal"] or "",
                "cited_by_count": row["cited_by_count"] or 0,
            }
    out: list[dict] = []
    for paper_id, score in scores.items():
        candidate = by_id.get(paper_id)
        if not candidate:
            continue
        candidate.update(
            {
                "score": float(score),
                score_field: float(score),
                "source_type": score_field,
                "source_api": "local",
            }
        )
        out.append(candidate)
    return out


def _summary() -> dict[str, Any]:
    return {
        "seed_total": 0,
        "direct_reference_candidates": 0,
        "bibliographic_coupling_candidates": 0,
        "cocitation_candidates": 0,
        "ppr_library_candidates": 0,
        "ppr_loved_candidates": 0,
        "network_calls": 0,
    }
