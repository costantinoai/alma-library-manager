"""Graph retrieval channel — citation / reference expansion via OpenAlex.

Split out of the discovery god-module (D-9); pure move.
"""

from __future__ import annotations

import logging
import sqlite3
from alma.core.concurrency import bounded_thread_pool
from typing import Any

from alma.core.scoring_math import clamp
from alma.discovery import openalex_related
from alma.openalex.client import (
    _upsert_referenced_works,
    batch_fetch_referenced_works_for_openalex_ids,
    batch_fetch_works_by_openalex_ids,
)

from ._common import (
    _GRAPH_FALLBACK_DEADLINE_S,
    _candidate_key,
    _drain_futures_within_deadline,
)

logger = logging.getLogger(__name__)

_clamp = clamp  # D-3: canonical clamp under the legacy local name


def _retrieve_graph_channel(
    db: sqlite3.Connection,
    lens: dict,
    seeds: list[dict],
    *,
    limit: int,
) -> tuple[list[dict], dict[str, Any]]:
    def _seed_graph_identifier(seed: dict) -> str:
        openalex_id = str(seed.get("openalex_id") or "").strip()
        if openalex_id:
            return openalex_id
        return str(seed.get("doi") or "").strip()

    graph_summary: dict[str, Any] = {
        "seed_total": len(seeds),
        "seed_local_reference_ready": 0,
        "seed_reference_backfilled": 0,
        "local_reference_candidates": 0,
        "fallback_candidates": 0,
        "fallback_used": False,
        "semantic_related_candidates": 0,
        "fallback_sources": [],
    }

    def _backfill_local_references() -> None:
        seed_rows = [
            (
                str(seed.get("id") or "").strip(),
                str(seed.get("openalex_id") or "").strip(),
            )
            for seed in seeds
            if str(seed.get("id") or "").strip()
        ]
        if not seed_rows:
            return
        seed_ids = [paper_id for paper_id, _openalex_id in seed_rows]
        placeholders = ", ".join("?" for _ in seed_ids)
        ref_counts: dict[str, int] = {}
        try:
            rows = db.execute(
                f"""
                SELECT paper_id, COUNT(*) AS ref_count
                FROM publication_references
                WHERE paper_id IN ({placeholders})
                GROUP BY paper_id
                """,
                seed_ids,
            ).fetchall()
            ref_counts = {str(row["paper_id"]): int(row["ref_count"] or 0) for row in rows}
        except sqlite3.OperationalError:
            ref_counts = {}

        graph_summary["seed_local_reference_ready"] = sum(
            1 for paper_id, _openalex_id in seed_rows if int(ref_counts.get(paper_id) or 0) > 0
        )
        missing_pairs = [
            (paper_id, openalex_id)
            for paper_id, openalex_id in seed_rows
            if openalex_id and int(ref_counts.get(paper_id) or 0) <= 0
        ]
        if not missing_pairs:
            return
        try:
            reference_map = batch_fetch_referenced_works_for_openalex_ids(
                [openalex_id for _paper_id, openalex_id in missing_pairs],
                batch_size=25,
                max_workers=4,
            )
        except Exception:
            return

        backfilled = 0
        for paper_id, openalex_id in missing_pairs:
            referenced_ids = reference_map.get(openalex_id) or []
            if not referenced_ids:
                continue
            backfilled += _upsert_referenced_works(db, paper_id, referenced_ids)
        if backfilled > 0:
            graph_summary["seed_reference_backfilled"] = backfilled
            graph_summary["seed_local_reference_ready"] = min(
                len(seed_rows),
                int(graph_summary["seed_local_reference_ready"] or 0)
                + sum(1 for _paper_id, openalex_id in missing_pairs if reference_map.get(openalex_id)),
            )

    def _local_reference_candidates() -> list[dict]:
        seed_ids = [str(seed["id"]) for seed in seeds if seed.get("id")]
        if not seed_ids:
            return []
        seed_placeholders = ", ".join("?" for _ in seed_ids)
        try:
            # Lens-adjacency guarantee: a reference must be cited by at
            # least one seed paper to be considered. Within that pool,
            # rank by how many papers in the **entire local corpus**
            # cite it (not just seeds), tie-break by seed_overlap.
            #
            # Why corpus-wide instead of seeds-only: pure seed_overlap
            # systematically penalizes recent references — a 2024 paper
            # cited by 1 seed and 4 other corpus papers (corpus_overlap=5)
            # otherwise loses to a 2010 paper cited by 1 seed and 0
            # others (corpus_overlap=1) when both have seed_overlap=1.
            # Widening the count to the corpus gives newer references a
            # fair shot at the top-K. The scorer's recency_boost takes
            # over from there once OpenAlex enrichment supplies the
            # publication_date.
            rows = db.execute(
                f"""
                SELECT
                    pr.referenced_work_id,
                    COUNT(DISTINCT pr.paper_id) AS corpus_overlap,
                    SUM(CASE WHEN pr.paper_id IN ({seed_placeholders}) THEN 1 ELSE 0 END) AS seed_overlap
                FROM publication_references pr
                WHERE pr.referenced_work_id IS NOT NULL
                  AND pr.referenced_work_id IN (
                      SELECT DISTINCT referenced_work_id
                      FROM publication_references
                      WHERE paper_id IN ({seed_placeholders})
                        AND referenced_work_id IS NOT NULL
                  )
                GROUP BY pr.referenced_work_id
                ORDER BY corpus_overlap DESC, seed_overlap DESC, pr.referenced_work_id ASC
                LIMIT ?
                """,
                [*seed_ids, *seed_ids, limit],
            ).fetchall()
        except sqlite3.OperationalError:
            return []
        # publication_references stores the bare integer ID; OpenAlex
        # batch fetch expects W-prefixed strings.
        work_ids = [
            f"W{r['referenced_work_id']}"
            for r in rows
            if r["referenced_work_id"]
        ]
        if not work_ids:
            return []
        works = batch_fetch_works_by_openalex_ids(work_ids, batch_size=50, max_workers=4)
        out: list[dict] = []
        for idx, work_id in enumerate(work_ids):
            work = works.get(work_id)
            if not work:
                continue
            title = (work.get("display_name") or "").strip()
            if not title:
                continue
            authorships = work.get("authorships") or []
            authors = ", ".join((a.get("author") or {}).get("display_name", "") for a in authorships)
            primary_loc = work.get("primary_location") or {}
            source = primary_loc.get("source") or {}
            out.append(
                {
                    "openalex_id": work_id,
                    "title": title,
                    "authors": authors,
                    "url": primary_loc.get("landing_page_url") or primary_loc.get("pdf_url") or work.get("id") or "",
                    "doi": work.get("doi") or "",
                    "score": max(0.1, 1.0 - (idx / max(1, len(work_ids)))),
                    "year": work.get("publication_year"),
                    "journal": source.get("display_name") if isinstance(source, dict) else "",
                    "cited_by_count": work.get("cited_by_count") or 0,
                    "source_type": "graph_reference",
                    "source_api": "openalex",
                    "source_key": "local_references",
                }
            )
            if len(out) >= limit:
                break
        graph_summary["local_reference_candidates"] = len(out)
        return out

    _backfill_local_references()
    local_candidates = _local_reference_candidates()
    if len(local_candidates) >= limit:
        return local_candidates[:limit], graph_summary

    merged: dict[str, dict] = {}
    for item in local_candidates:
        merged[_candidate_key(item)] = item

    fallback_budget = max(limit, 8)
    identifiers = [
        identifier
        for identifier in (_seed_graph_identifier(seed) for seed in seeds[:10])
        if identifier
    ]
    seed_dois = [
        str(seed.get("doi") or "").strip()
        for seed in seeds[:10]
        if str(seed.get("doi") or "").strip()
    ]
    # Parallelize the 3-call OA fallback fan-out across all seed identifiers.
    # Pre-refactor this was up to 30 sequential OpenAlex HTTP calls; bounded
    # pool keeps peak concurrent requests at max_workers=6.
    if identifiers:
        graph_summary["fallback_used"] = True
        graph_summary["fallback_sources"] = sorted(set([*graph_summary.get("fallback_sources", []), "openalex"]))
        relation_calls = (
            ("graph_reference", openalex_related.fetch_referenced_works, 0.72),
            ("graph_citing", openalex_related.fetch_citing_works, 0.58),
            ("graph_related", openalex_related.fetch_related_works, 0.44),
        )
        call_keys: list[tuple[str, str, float]] = [
            (identifier, relation, weight)
            for identifier in identifiers
            for relation, _fn, weight in relation_calls
        ]
        fn_map = {relation: fn for relation, fn, _ in relation_calls}
        # Bounded fan-out: drain up to the deadline, abandon (shutdown
        # wait=False) any OpenAlex call still pending so a slow/429 source
        # can't stall the lane (F2).
        gpool = bounded_thread_pool(min(6, max(1, len(call_keys))), thread_name_prefix="graph-oa")
        future_map = {
            gpool.submit(fn_map[rel], identifier, 6): (identifier, rel, weight)
            for identifier, rel, weight in call_keys
        }
        done_map = _drain_futures_within_deadline(gpool, future_map, _GRAPH_FALLBACK_DEADLINE_S)
        if len(done_map) < len(future_map):
            graph_summary["oa_fallback_timed_out"] = True
        for fut, (identifier, rel, weight) in done_map.items():
            if len(merged) >= fallback_budget:
                continue
            try:
                items = fut.result() or []
            except Exception as exc:
                logger.debug("graph OA fallback (%s) failed for %s: %s", rel, identifier, exc)
                items = []
            for idx, item in enumerate(items):
                candidate = dict(item)
                candidate["source_type"] = rel
                candidate["source_api"] = str(candidate.get("source_api") or "openalex")
                candidate["source_key"] = identifier
                base = float(candidate.get("score", 0.25) or 0.25)
                rank_factor = _clamp(1.0 - (idx / max(1, len(items) * 1.6)), 0.12, 1.0)
                candidate["score"] = round(_clamp((base * weight) + (rank_factor * (1.0 - weight)), 0.05, 1.0), 4)
                key = _candidate_key(candidate)
                existing = merged.get(key)
                if existing is None or float(candidate.get("score") or 0.0) > float(existing.get("score") or 0.0):
                    merged[key] = candidate
                if len(merged) >= fallback_budget:
                    break

    if len(merged) < fallback_budget and seed_dois:
        from alma.discovery import semantic_scholar

        graph_summary["fallback_used"] = True
        graph_summary["fallback_sources"] = sorted(set([*graph_summary.get("fallback_sources", []), "semantic_scholar"]))
        # Bounded fan-out (F2): S2 is the rate-limit-prone source — abandon
        # any call still in 429 retry/cooldown at the deadline (this is the
        # exact site of the live-reproduced 7.3 min hang).
        s2pool = bounded_thread_pool(min(4, max(1, len(seed_dois))), thread_name_prefix="graph-s2")
        future_map = {s2pool.submit(semantic_scholar.fetch_related_papers, doi, 6): doi for doi in seed_dois}
        done_map = _drain_futures_within_deadline(s2pool, future_map, _GRAPH_FALLBACK_DEADLINE_S)
        if len(done_map) < len(future_map):
            graph_summary["s2_fallback_timed_out"] = True
        for fut, doi in done_map.items():
            if len(merged) >= fallback_budget:
                continue
            try:
                items = fut.result() or []
            except Exception as exc:
                logger.debug("graph S2 related fetch failed for %s: %s", doi, exc)
                items = []
            graph_summary["semantic_related_candidates"] = int(graph_summary.get("semantic_related_candidates") or 0) + len(items)
            for idx, item in enumerate(items):
                candidate = dict(item)
                candidate["source_type"] = "graph_semantic_related"
                candidate["source_api"] = "semantic_scholar"
                candidate["source_key"] = doi
                base = float(candidate.get("score", 0.25) or 0.25)
                rank_factor = _clamp(1.0 - (idx / max(1, len(items) * 1.5)), 0.12, 1.0)
                candidate["score"] = round(_clamp((base * 0.52) + (rank_factor * 0.48), 0.05, 1.0), 4)
                key = _candidate_key(candidate)
                existing = merged.get(key)
                if existing is None or float(candidate.get("score") or 0.0) > float(existing.get("score") or 0.0):
                    merged[key] = candidate

    ranked = sorted(merged.values(), key=lambda item: float(item.get("score", 0.0) or 0.0), reverse=True)
    graph_summary["fallback_candidates"] = max(0, len(ranked) - len(local_candidates))
    return ranked[:limit], graph_summary
