"""Post-import enrichment pipeline.

Resolves imported publications via OpenAlex (by DOI or title search),
then populates topics, institutions, and citation counts.
"""

from __future__ import annotations

import logging
import sqlite3
import time
import hashlib
import re
from difflib import SequenceMatcher
from typing import Optional

import requests

from alma.config import get_api_call_delay
from alma.core.resolution import resolve_paper_openalex_work
from alma.core.utils import normalize_doi, normalize_orcid, normalize_text as _normalize_text
from alma.openalex.client import (
    _ensure_schema,
    _normalize_openalex_author_id,
    _normalize_work,
    backfill_missing_publication_references,
    _upsert_institutions,
    _upsert_topics,
    _WORKS_SELECT_FIELDS,
    batch_fetch_works_by_dois,
)
from alma.openalex.http import get_client

from datetime import datetime

logger = logging.getLogger(__name__)

_OA_RESOLVED = "openalex_resolved"
_OA_NOT_RESOLVED = "not_openalex_resolved"
_OA_PENDING = "pending_enrichment"


def _set_openalex_resolution_status(
    conn: sqlite3.Connection,
    paper_id: str,
    status: str,
    reason: str = "",
) -> None:
    """Persist per-publication OpenAlex resolution state."""
    conn.execute(
        """
        UPDATE papers
        SET openalex_resolution_status = ?,
            openalex_resolution_reason = ?,
            openalex_resolution_updated_at = ?
        WHERE id = ?
        """,
        (status, (reason or "")[:1000], time.strftime("%Y-%m-%dT%H:%M:%SZ"), paper_id),
    )


def _title_variants(title: str) -> list[str]:
    base = (title or "").strip()
    if not base:
        return []
    variants = [base]
    # Remove bracketed parts and excessive punctuation.
    no_brackets = re.sub(r"[\(\[\{].*?[\)\]\}]", " ", base)
    no_brackets = re.sub(r"\s+", " ", no_brackets).strip(" .,:;")
    if no_brackets and no_brackets not in variants:
        variants.append(no_brackets)
    # Drop subtitle after colon or dash.
    for sep in (":", " - ", " – ", " — "):
        if sep in no_brackets:
            head = no_brackets.split(sep, 1)[0].strip(" .,:;")
            if len(head) >= 20 and head not in variants:
                variants.append(head)
    # Normalized plain-text variant.
    normalized = _normalize_text(base)
    if normalized and normalized not in variants:
        variants.append(normalized)
    return variants[:5]


def _doi_variants(doi_raw: str) -> list[str]:
    raw = (doi_raw or "").strip()
    if not raw:
        return []
    out: list[str] = []
    candidates = [
        raw,
        normalize_doi(raw) or "",
        re.sub(r"^https?://(dx\.)?doi\.org/", "", raw, flags=re.IGNORECASE),
        re.sub(r"^doi:\s*", "", raw, flags=re.IGNORECASE),
    ]
    seen = set()
    for c in candidates:
        v = (c or "").strip()
        if not v:
            continue
        key = v.lower()
        if key in seen:
            continue
        seen.add(key)
        out.append(v)
    return out


def _extract_arxiv_id(text: str) -> Optional[str]:
    s = (text or "").strip()
    if not s:
        return None
    patterns = [
        r"(?:arxiv[:/\s]+)(\d{4}\.\d{4,5}(?:v\d+)?)",
        r"(?:arxiv\.org/(?:abs|pdf)/)(\d{4}\.\d{4,5}(?:v\d+)?)",
        r"\b(\d{4}\.\d{4,5}(?:v\d+)?)\b",
    ]
    for pat in patterns:
        m = re.search(pat, s, flags=re.IGNORECASE)
        if m:
            return m.group(1)
    return None


def _extract_biorxiv_doi(text: str) -> Optional[str]:
    s = (text or "").strip()
    if not s:
        return None
    m = re.search(r"(10\.1101/[^\s/\"'<>]+)", s, flags=re.IGNORECASE)
    if m:
        return normalize_doi(m.group(1))
    return None


def _preprint_hints(pub: dict) -> dict:
    doi = (pub.get("doi") or "").strip()
    url = (pub.get("url") or "").strip()
    title = (pub.get("title") or "").strip()
    journal = (pub.get("journal") or "").strip()
    combined = " ".join([doi, url, title, journal])
    arxiv_id = _extract_arxiv_id(combined)
    biorxiv_doi = _extract_biorxiv_doi(combined)
    looks_preprint = bool(
        arxiv_id
        or biorxiv_doi
        or "arxiv" in combined.lower()
        or "biorxiv" in combined.lower()
    )
    synthetic_dois: list[str] = []
    if arxiv_id:
        synthetic_dois.append(f"10.48550/arXiv.{arxiv_id}")
    if biorxiv_doi:
        synthetic_dois.append(biorxiv_doi)
    return {
        "looks_preprint": looks_preprint,
        "arxiv_id": arxiv_id,
        "biorxiv_doi": biorxiv_doi,
        "synthetic_dois": synthetic_dois,
    }


def _author_tokens(authors_raw: str) -> set[str]:
    names = [p.strip() for p in (authors_raw or "").split(",") if p.strip()]
    tokens: set[str] = set()
    for name in names[:8]:
        parts = [p for p in re.split(r"\s+", name) if p]
        if not parts:
            continue
        tokens.add(parts[-1].lower())
        if len(parts) > 1:
            tokens.add(parts[0].lower())
    return tokens


def _score_work_candidate(candidate: dict, pub: dict) -> float:
    cand_title = (candidate.get("display_name") or "").strip()
    src_title = (pub.get("title") or "").strip()
    if not cand_title or not src_title:
        return 0.0

    t_a = _normalize_text(src_title)
    t_b = _normalize_text(cand_title)
    if not t_a or not t_b:
        return 0.0

    title_score = SequenceMatcher(None, t_a, t_b).ratio() * 6.0
    if t_a == t_b:
        title_score += 2.0
    elif t_a in t_b or t_b in t_a:
        title_score += 1.0

    year_score = 0.0
    src_year = pub.get("year")
    cand_year = candidate.get("publication_year")
    try:
        sy = int(src_year) if src_year is not None else None
        cy = int(cand_year) if cand_year is not None else None
        if sy is not None and cy is not None:
            if sy == cy:
                year_score = 1.5
            elif abs(sy - cy) <= 1:
                year_score = 0.7
    except Exception:
        pass

    doi_score = 0.0
    src_doi = normalize_doi(pub.get("doi") or "")
    cand_doi = normalize_doi(candidate.get("doi") or "")
    if src_doi and cand_doi and src_doi == cand_doi:
        doi_score = 3.0

    author_score = 0.0
    src_tokens = _author_tokens(pub.get("authors") or "")
    cand_authorships = candidate.get("authorships") or []
    cand_tokens: set[str] = set()
    for a in cand_authorships[:10]:
        an = ((a or {}).get("author") or {}).get("display_name") or ""
        for p in re.split(r"\s+", an.strip()):
            if p:
                cand_tokens.add(p.lower())
    if src_tokens and cand_tokens:
        overlap = len(src_tokens.intersection(cand_tokens))
        if overlap > 0:
            author_score = min(2.0, 0.8 * overlap)

    return title_score + year_score + doi_score + author_score


def _search_work_candidates(title_query: str, per_page: int = 10) -> list[dict]:
    try:
        client = get_client()
        resp = client.get(
            "/works",
            params={
                "search": title_query,
                "per-page": max(1, min(per_page, 25)),
                "select": _WORKS_SELECT_FIELDS,
            },
            timeout=20,
        )
        resp.raise_for_status()
        return (resp.json() or {}).get("results") or []
    except requests.exceptions.RequestException:
        return []
    except Exception:
        return []


def _fetch_work_by_doi_diagnostic(doi: str) -> tuple[Optional[dict], Optional[str]]:
    """DOI lookup with explicit failure diagnostics (vs silent None)."""
    if not doi:
        return None, None
    try:
        client = get_client()
        clean_doi = normalize_doi(doi) or doi
        resp = client.get(
            f"/works/doi:{clean_doi}",
            params={
                "select": _WORKS_SELECT_FIELDS,
            },
            timeout=20,
        )
        if resp.status_code == 404:
            return None, None
        if resp.status_code == 429:
            return None, "openalex_rate_limited"
        if resp.status_code >= 500:
            return None, f"openalex_http_{resp.status_code}"
        resp.raise_for_status()
        data = resp.json() or {}
        if not data.get("display_name"):
            return None, None
        return _normalize_work(data), None
    except requests.exceptions.RequestException:
        return None, "openalex_unreachable"
    except Exception:
        return None, "openalex_lookup_error"


def _fetch_work_by_title_diagnostic(title: str) -> tuple[Optional[dict], Optional[str]]:
    """Title lookup with diagnostics and slightly broader candidate sweep."""
    if not title or not title.strip():
        return None, None
    try:
        client = get_client()
        resp = client.get(
            "/works",
            params={
                "search": title.strip(),
                "per-page": 5,
                "select": _WORKS_SELECT_FIELDS,
            },
            timeout=20,
        )
        if resp.status_code == 429:
            return None, "openalex_rate_limited"
        if resp.status_code >= 500:
            return None, f"openalex_http_{resp.status_code}"
        resp.raise_for_status()
        results = (resp.json() or {}).get("results") or []
        if not results:
            return None, None

        t_norm = _normalize_text(title)
        best = None
        best_score = 0.0
        for cand in results:
            cand_title = (cand.get("display_name") or "").strip()
            if not cand_title:
                continue
            c_norm = _normalize_text(cand_title)
            if not c_norm:
                continue
            if t_norm == c_norm:
                score = 1.0
            elif t_norm in c_norm or c_norm in t_norm:
                score = 0.9
            else:
                score = SequenceMatcher(None, t_norm, c_norm).ratio()
            if score > best_score:
                best = cand
                best_score = score
        if not best or best_score < 0.72:
            return None, None
        return _normalize_work(best), None
    except requests.exceptions.RequestException:
        return None, "openalex_unreachable"
    except Exception:
        return None, "openalex_lookup_error"


def _resolve_work(
    pub: dict,
    *,
    _title_search_cache: Optional[dict[str, Optional[dict]]] = None,
    _doi_prefetch: Optional[dict[str, dict]] = None,
) -> tuple[Optional[dict], Optional[str], Optional[str]]:
    """Resolve a publication via the shared paper-resolution layer."""
    resolution = resolve_paper_openalex_work(
        pub,
        title_search_cache=_title_search_cache,
        doi_prefetch=_doi_prefetch,
    )
    if resolution.work is not None:
        return resolution.work, resolution.source, None
    return None, None, resolution.reason or "not_found"


def enrich_publication(
    paper_id: str,
    conn: sqlite3.Connection,
    *,
    _title_search_cache: Optional[dict[str, Optional[dict]]] = None,
    _doi_prefetch: Optional[dict[str, dict]] = None,
) -> dict:
    """Enrich a single publication by resolving it via OpenAlex.

    Looks up the publication in the ``papers`` table by ``id``, then attempts
    to match it in OpenAlex first by DOI, then by title.  If a match is found,
    updates cited_by_count, year, journal, abstract, url, topics, and institutions.

    Args:
        paper_id: The UUID identifier for the paper row.
        conn: Active SQLite connection to the publications database.
        _title_search_cache: Optional shared dict for deduplicating title
            searches across publications within a single enrichment run.
            Keys are normalized title strings; values are resolved work
            dicts or ``None`` for known misses.
        _doi_prefetch: Optional dict mapping normalized lowercase DOI to
            raw OpenAlex work dict from batch pre-fetch.

    Returns:
        A summary dict, e.g.
        ``{"enriched": True, "source": "doi", "topics_added": 3, "institutions_added": 2}``
        or ``{"enriched": False, "reason": "not_found"}``.
    """
    row = conn.execute(
        "SELECT * FROM papers WHERE id = ?",
        (paper_id,),
    ).fetchone()
    if row is None:
        return {"enriched": False, "reason": "publication_not_found"}

    pub = dict(row)
    doi = (pub.get("doi") or "").strip()
    title = (pub.get("title") or "").strip()
    # Robust lookup using DOI/title variants + scored candidate search.
    work, match_source, fail_reason = _resolve_work(
        pub, _title_search_cache=_title_search_cache,
        _doi_prefetch=_doi_prefetch,
    )

    if work is None:
        _set_openalex_resolution_status(
            conn,
            paper_id,
            _OA_NOT_RESOLVED,
            fail_reason or "not_found",
        )
        conn.commit()
        return {
            "enriched": False,
            "reason": fail_reason or "not_found",
            "title": title,
            "doi": doi,
            "url": (pub.get("url") or "").strip(),
        }

    # Update the papers row with enriched metadata
    updates = []
    params = []

    citations = work.get("num_citations")
    if citations is not None:
        updates.append("cited_by_count = ?")
        params.append(int(citations))

    enriched_year = work.get("year")
    if enriched_year is not None:
        updates.append("year = ?")
        params.append(enriched_year)

    enriched_journal = (work.get("journal") or "").strip()
    if enriched_journal:
        updates.append("journal = ?")
        params.append(enriched_journal)

    enriched_abstract = (work.get("abstract") or "").strip()
    if enriched_abstract and not (pub.get("abstract") or "").strip():
        updates.append("abstract = ?")
        params.append(enriched_abstract)

    enriched_url = (work.get("pub_url") or "").strip()
    if enriched_url and not (pub.get("url") or "").strip():
        updates.append("url = ?")
        params.append(enriched_url)

    enriched_doi = (work.get("doi") or "").strip()
    if enriched_doi and not doi:
        updates.append("doi = ?")
        params.append(enriched_doi)

    enriched_authors = (work.get("authors") or "").strip()
    if enriched_authors and not (pub.get("authors") or "").strip():
        updates.append("authors = ?")
        params.append(enriched_authors)

    # Extended OpenAlex enrichment fields
    oa_id = (work.get("openalex_id") or "").strip()
    if oa_id:
        updates.append("openalex_id = ?")
        params.append(oa_id)

    work_type = (work.get("type") or "").strip()
    if work_type:
        updates.append("work_type = ?")
        params.append(work_type)

    lang = (work.get("language") or "").strip()
    if lang:
        updates.append("language = ?")
        params.append(lang)

    oa_info = work.get("open_access") or {}
    if isinstance(oa_info, dict):
        updates.append("is_oa = ?")
        params.append(1 if oa_info.get("is_oa") else 0)
        oa_status = (oa_info.get("oa_status") or "").strip()
        if oa_status:
            updates.append("oa_status = ?")
            params.append(oa_status)
        oa_url = (oa_info.get("oa_url") or "").strip()
        if oa_url:
            updates.append("oa_url = ?")
            params.append(oa_url)

    if work.get("is_retracted"):
        updates.append("is_retracted = ?")
        params.append(1)

    fwci = work.get("fwci")
    if fwci is not None:
        updates.append("fwci = ?")
        params.append(float(fwci))

    pctile = work.get("cited_by_percentile") or {}
    if isinstance(pctile, dict):
        if pctile.get("min") is not None:
            updates.append("cited_by_percentile_min = ?")
            params.append(float(pctile["min"]))
        if pctile.get("max") is not None:
            updates.append("cited_by_percentile_max = ?")
            params.append(float(pctile["max"]))

    ref_count = work.get("referenced_works_count")
    if ref_count is not None:
        updates.append("referenced_works_count = ?")
        params.append(int(ref_count))

    biblio = work.get("biblio") or {}
    if isinstance(biblio, dict):
        for field in ("volume", "issue", "first_page", "last_page"):
            val = (biblio.get(field) or "").strip()
            if val:
                updates.append(f"{field} = ?")
                params.append(val)

    inst_count = work.get("institutions_distinct_count")
    if inst_count is not None:
        updates.append("institutions_count = ?")
        params.append(int(inst_count))

    country_count = work.get("countries_distinct_count")
    if country_count is not None:
        updates.append("countries_count = ?")
        params.append(int(country_count))

    kws = work.get("keywords") or []
    if kws:
        import json
        updates.append("keywords = ?")
        params.append(json.dumps(kws))

    cby = work.get("counts_by_year") or []
    if cby:
        import json
        updates.append("counts_by_year = ?")
        params.append(json.dumps(cby))

    sdgs = work.get("sdgs") or []
    if sdgs:
        import json
        updates.append("sdgs = ?")
        params.append(json.dumps(sdgs))

    if updates:
        sql = f"UPDATE papers SET {', '.join(updates)} WHERE id = ?"
        params.append(paper_id)
        conn.execute(sql, params)

    # Upsert topics, institutions, and authorships
    topics = work.get("topics") or []
    institutions = work.get("institutions") or []
    authorships = work.get("authorships") or []
    _upsert_topics(conn, paper_id, topics)
    _upsert_institutions(conn, paper_id, institutions)
    _upsert_publication_authorships(conn, paper_id, authorships)
    _set_openalex_resolution_status(
        conn,
        paper_id,
        _OA_RESOLVED,
        f"resolved_via:{match_source or 'unknown'}",
    )
    conn.commit()

    return {
        "enriched": True,
        "source": match_source,
        "topics_added": len(topics),
        "institutions_added": len(institutions),
        "authorships_added": len(authorships),
    }


def enrich_all_unenriched(
    conn: sqlite3.Connection,
    job_id: Optional[str] = None,
) -> dict:
    """Enrich all publications that have no topic entries yet.

    Queries for publications missing from ``publication_topics`` (via LEFT
    JOIN) and enriches each one.  Uses operation-scoped caching and batch
    DOI pre-fetch to minimize redundant API calls.

    Args:
        conn: Active SQLite connection to the publications database.
        job_id: Optional scheduler job ID for progress tracking.

    Returns:
        Summary dict, e.g. ``{"total": 10, "enriched": 7, "failed": 2, "skipped": 1}``.
    """
    _ensure_schema(conn)
    add_job_log = None
    if job_id:
        from alma.api.scheduler import add_job_log as _add_job_log

        add_job_log = _add_job_log
        add_job_log(job_id, "Starting enrichment pipeline", step="start")

    rows = conn.execute(
        """
        SELECT p.id, p.title, p.doi
        FROM papers p
        LEFT JOIN publication_topics pt
            ON p.id = pt.paper_id
        WHERE pt.paper_id IS NULL
            AND COALESCE(p.title, '') != ''
        """
    ).fetchall()

    total = len(rows)
    enriched = 0
    failed = 0
    skipped = 0
    graph_backfill_summary = {
        "candidates": 0,
        "fetched": 0,
        "papers_updated": 0,
        "references_inserted": 0,
    }
    delay = get_api_call_delay()

    if job_id:
        _set_job_progress(job_id, status="running", processed=0, total=total)
        if add_job_log:
            add_job_log(
                job_id,
                f"Found {total} publications needing enrichment",
                step="scan",
            )

    # --- Pre-batch DOI lookups ---
    # Collect all DOIs from unenriched rows and resolve them in bulk.
    # Results are passed directly to _resolve_work() so DOI-matched papers
    # skip individual API calls entirely.
    all_dois = [
        (dict(r).get("doi") or "").strip()
        for r in rows
    ]
    all_dois = [d for d in all_dois if d]
    doi_prefetch_count = 0
    _doi_prefetch: dict[str, dict] = {}
    if all_dois:
        if add_job_log and job_id:
            add_job_log(
                job_id,
                f"Pre-fetching {len(all_dois)} DOIs in batch mode",
                step="doi_prefetch",
            )
        try:
            _doi_prefetch = batch_fetch_works_by_dois(all_dois)
            doi_prefetch_count = len(_doi_prefetch)
            logger.info(
                "DOI batch pre-fetch resolved %d/%d DOIs",
                doi_prefetch_count,
                len(all_dois),
            )
        except Exception as e:
            logger.warning("DOI batch pre-fetch failed (will fall back to individual lookups): %s", e)

    # --- Parallel title pre-resolution for non-DOI papers ---
    # Papers whose DOIs were resolved in the batch skip API calls entirely.
    # For the rest, pre-resolve titles in parallel threads to avoid serial
    # 1-API-call-per-paper bottleneck.
    _title_search_cache: dict[str, Optional[dict]] = {}
    prefetched_doi_set = set(_doi_prefetch.keys())
    titles_to_resolve: list[tuple[str, dict]] = []  # (normalized_title, pub_dict)
    for r in rows:
        rd = dict(r)
        doi = (rd.get("doi") or "").strip()
        # Check if DOI is in prefetch
        doi_in_prefetch = False
        if doi and _doi_prefetch:
            from alma.core.utils import normalize_doi as _nd
            nd = (_nd(doi) or doi).strip().lower()
            doi_in_prefetch = nd in prefetched_doi_set
        if not doi_in_prefetch:
            title = (rd.get("title") or "").strip()
            if title:
                tnk = _normalize_text(title)
                if tnk and tnk not in _title_search_cache:
                    titles_to_resolve.append((tnk, rd))

    if titles_to_resolve:
        from concurrent.futures import ThreadPoolExecutor, as_completed

        def _pre_resolve_title(item: tuple[str, dict]) -> tuple[str, Optional[dict], Optional[str], Optional[str]]:
            tnk, pub = item
            try:
                work, match_source, fail_reason = _resolve_work(pub)
                return tnk, work, match_source, fail_reason
            except Exception:
                return tnk, None, None, "pre_resolve_error"

        title_resolve_count = 0
        if add_job_log and job_id:
            add_job_log(
                job_id,
                f"Pre-resolving {len(titles_to_resolve)} titles in parallel",
                step="title_prefetch",
            )
        # Use 3 workers to stay within OpenAlex rate limits
        with ThreadPoolExecutor(max_workers=3) as executor:
            futures = {
                executor.submit(_pre_resolve_title, item): item[0]
                for item in titles_to_resolve
            }
            for future in as_completed(futures):
                tnk, work, match_source, fail_reason = future.result()
                if work is not None:
                    _title_search_cache[tnk] = work
                    title_resolve_count += 1
                else:
                    _title_search_cache[tnk] = None
        logger.info(
            "Title parallel pre-resolve: %d/%d resolved",
            title_resolve_count,
            len(titles_to_resolve),
        )

    client = get_client()
    enriched_paper_ids: list[str] = []
    with client.operation_cache(f"enrich-all-{job_id or 'manual'}") as op_stats:
        for idx, row in enumerate(rows):
            r = dict(row)
            paper_id = r["id"]

            if not paper_id:
                skipped += 1
                continue

            try:
                result = enrich_publication(
                    paper_id,
                    conn,
                    _title_search_cache=_title_search_cache,
                    _doi_prefetch=_doi_prefetch,
                )
                used_prefetch = result.get("source") in ("doi_prefetch", "title_cached")
                if result.get("enriched"):
                    enriched += 1
                    enriched_paper_ids.append(paper_id)
                    if add_job_log and ((idx + 1) <= 5 or (idx + 1) % 25 == 0):
                        add_job_log(
                            job_id,
                            f"Enriched publication {idx + 1}/{total}: {paper_id}",
                            step="item_done",
                            data=result,
                        )
                else:
                    skipped += 1
                    used_prefetch = result.get("reason") in ("not_found_cached",)
                    if add_job_log and ((idx + 1) <= 5 or (idx + 1) % 25 == 0):
                        add_job_log(
                            job_id,
                            f"Skipped publication {idx + 1}/{total}: {paper_id} ({result.get('reason', 'unknown')})",
                            step="item_skip",
                            data=result,
                        )
            except Exception as e:
                used_prefetch = False
                logger.warning(
                    "Failed to enrich publication %s: %s", paper_id, e
                )
                failed += 1
                if add_job_log:
                    add_job_log(
                        job_id,
                        f"Failed enrichment for {paper_id}: {e}",
                        level="ERROR",
                        step="item_error",
                    )

            if job_id and (idx + 1) % 5 == 0:
                _set_job_progress(
                    job_id,
                    status="running",
                    processed=idx + 1,
                    total=total,
                    message=f"Enriched {enriched}/{idx + 1} so far",
                )
                if add_job_log:
                    add_job_log(
                        job_id,
                        f"Progress {idx + 1}/{total}: enriched={enriched}, failed={failed}, skipped={skipped}",
                        step="progress",
                    )

            # Rate-limit between OpenAlex API calls (skip for cache hits)
            if idx < total - 1 and not used_prefetch:
                time.sleep(delay)

    if enriched_paper_ids:
        try:
            graph_backfill_summary = backfill_missing_publication_references(
                conn,
                paper_ids=enriched_paper_ids,
                limit=len(enriched_paper_ids),
            )
            if add_job_log:
                add_job_log(
                    job_id,
                    "Reference backfill completed for enriched publications",
                    step="graph_backfill",
                    data=graph_backfill_summary,
                )
        except Exception as exc:
            logger.warning("Reference backfill after enrichment failed: %s", exc)
            if add_job_log:
                add_job_log(
                    job_id,
                    f"Reference backfill failed: {exc}",
                    level="WARNING",
                    step="graph_backfill_error",
                )

    summary = {
        "total": total,
        "enriched": enriched,
        "failed": failed,
        "skipped": skipped,
        "doi_prefetched": doi_prefetch_count,
        "title_cache_entries": len(_title_search_cache),
        "api_summary": op_stats.summary(),
        "graph_backfill": graph_backfill_summary,
    }

    if job_id:
        _set_job_progress(
            job_id,
            status="completed",
            processed=total,
            total=total,
            message=f"Done: {enriched} enriched, {failed} failed, {skipped} skipped",
        )
        if add_job_log:
            add_job_log(job_id, "Enrichment completed", step="done", data=summary)

    logger.info("Enrichment complete: %s", summary)
    return summary


def resolve_imported_authors(
    conn: sqlite3.Connection,
    authors_conn: sqlite3.Connection | None = None,
) -> dict:
    """Re-assign imported publications to tracked authors when possible.

    Finds publications where ``author_id = 'import'``, parses the
    comma-separated ``authors`` field, and checks if any author name matches
    a tracked author in the ``authors`` table (case-insensitive LIKE match).
    If a match is found, updates the publication's ``author_id`` to the
    matched author's ``id``.

    With the unified scholar.db, both tables live in the same database, so
    ``authors_conn`` is no longer needed (it defaults to ``conn``).

    Args:
        conn: SQLite connection to the unified scholar database.
        authors_conn: Deprecated -- ignored; kept for call-site compat.

    Returns:
        Summary dict, e.g. ``{"total": 5, "resolved": 3, "unresolved": 2}``.
    """
    # Use the same connection for both tables (unified DB)
    authors_conn = conn

    materialized_summary = materialize_imported_authors(conn)

    imports = conn.execute(
        """
        SELECT id, title, authors, author_id
        FROM papers
        WHERE author_id = 'import' OR author_id LIKE 'import_author_%'
        """
    ).fetchall()

    tracked_authors = authors_conn.execute(
        """
        SELECT id, name, openalex_id
        FROM authors
        WHERE id != 'import' AND id NOT LIKE 'import_author_%'
        """
    ).fetchall()

    if not tracked_authors:
        return {
            "total": len(imports),
            "resolved": 0,
            "unresolved": len(imports),
            "materialized": materialized_summary,
        }

    resolved = 0
    rewired_authorship_rows = 0
    tracked_name_pairs = []
    tracked_by_openalex: dict[str, list[str]] = {}
    for tracked in tracked_authors:
        tid = tracked["id"] if isinstance(tracked, sqlite3.Row) else tracked[0]
        tname = tracked["name"] if isinstance(tracked, sqlite3.Row) else tracked[1]
        toaid = tracked["openalex_id"] if isinstance(tracked, sqlite3.Row) else tracked[2]
        tracked_name_pairs.append((tid, tname or ""))
        n_oa = _normalize_openalex_author_id(toaid)
        if n_oa:
            tracked_by_openalex.setdefault(n_oa, []).append(tid)

    for pub_row in imports:
        pub = dict(pub_row)
        current_owner = (pub.get("author_id") or "").strip()
        paper_id = (pub.get("id") or "").strip()
        title = (pub.get("title") or "").strip()
        pub_author_names = _parse_author_names((pub.get("authors") or "").strip())

        score_by_author: dict[str, float] = {}

        # 1) Highest-confidence mapping via OpenAlex authorships.
        try:
            authorship_rows = conn.execute(
                """
                SELECT openalex_id, display_name, position, is_corresponding
                FROM publication_authors
                WHERE paper_id = ?
                """,
                (paper_id,),
            ).fetchall()
        except sqlite3.OperationalError:
            authorship_rows = []

        for arow in authorship_rows:
            oaid = _normalize_openalex_author_id(arow["openalex_id"])
            if oaid:
                for tid in tracked_by_openalex.get(oaid, []):
                    score_by_author[tid] = score_by_author.get(tid, 0.0) + 12.0

            disp = (arow["display_name"] or "").strip()
            for tid, tname in tracked_name_pairs:
                if disp and tname and _names_match(disp, tname):
                    bump = 3.5
                    if (arow["position"] or "").strip().lower() == "first":
                        bump += 0.4
                    if int(arow["is_corresponding"] or 0) == 1:
                        bump += 0.2
                    score_by_author[tid] = score_by_author.get(tid, 0.0) + bump

        # 2) Fallback mapping via imported authors string.
        for pub_name in pub_author_names:
            for tid, tname in tracked_name_pairs:
                if tname and _names_match(pub_name, tname):
                    score_by_author[tid] = score_by_author.get(tid, 0.0) + 2.0

        matched_author_id = None
        if score_by_author:
            ranked = sorted(score_by_author.items(), key=lambda kv: kv[1], reverse=True)
            top_id, top_score = ranked[0]
            second_score = ranked[1][1] if len(ranked) > 1 else -1.0
            if top_score >= 12.0 or (top_score - second_score) >= 1.0:
                matched_author_id = top_id

        if matched_author_id:
            moved, rewired = _move_publication_owner(
                conn=conn,
                paper_id=paper_id,
                title=title,
                old_author_id=current_owner,
                new_author_id=matched_author_id,
            )
            if moved:
                rewired_authorship_rows += rewired
                resolved += 1

    conn.commit()

    total = len(imports)
    return {
        "total": total,
        "resolved": resolved,
        "unresolved": total - resolved,
        "materialized": materialized_summary,
        "authorship_rows_rewired": rewired_authorship_rows,
    }


def materialize_imported_authors(conn: sqlite3.Connection) -> dict:
    """Create author rows from imported publication metadata and link primary author.

    For each publication with ``author_id='import'``:
    - Parse the ``authors`` field into distinct names.
    - Ensure each parsed name exists in ``authors``.
    - Link the publication to the first parsed author.
    """
    rows = conn.execute(
        "SELECT id, title, authors FROM papers WHERE author_id = 'import'"
    ).fetchall()

    created = 0
    linked = 0
    rewired_authorship_rows = 0

    for row in rows:
        paper_id = (row["id"] or "").strip()
        title = (row["title"] or "").strip()
        authors_str = (row["authors"] or "").strip()
        names = _parse_author_names(authors_str)
        if not names:
            continue

        primary_id: Optional[str] = None
        preferred_tracked_id: Optional[str] = None
        for idx, raw_name in enumerate(names):
            clean_name = _canonical_author_name(raw_name)
            if not clean_name:
                continue
            existing = _find_author_by_name(conn, clean_name)
            if existing:
                author_id = existing["id"]
            else:
                author_id = f"import_author_{hashlib.sha1(clean_name.lower().encode('utf-8')).hexdigest()[:16]}"
                inserted = conn.execute(
                    "INSERT OR IGNORE INTO authors (name, id, added_at) VALUES (?, ?, ?)",
                    (clean_name, author_id, time.strftime("%Y-%m-%dT%H:%M:%S")),
                )
                if inserted.rowcount and inserted.rowcount > 0:
                    created += 1
            if author_id != "import" and not str(author_id).startswith("import_author_"):
                preferred_tracked_id = preferred_tracked_id or author_id
            if idx == 0:
                primary_id = author_id

        if preferred_tracked_id:
            primary_id = preferred_tracked_id

        if not primary_id or not paper_id or not title:
            continue

        moved, rewired = _move_publication_owner(
            conn=conn,
            paper_id=paper_id,
            title=title,
            old_author_id="import",
            new_author_id=primary_id,
        )
        if moved:
            linked += 1
            rewired_authorship_rows += rewired

    conn.commit()
    return {
        "total_import_rows": len(rows),
        "authors_created": created,
        "publications_linked": linked,
        "authorship_rows_rewired": rewired_authorship_rows,
    }


def _move_publication_owner(
    conn: sqlite3.Connection,
    paper_id: str,
    title: str,
    old_author_id: str,
    new_author_id: str,
) -> tuple[bool, int]:
    """Move a publication row from one local author to another.

    Keeps ``papers`` (status='library'), and ``publication_authors``
    in sync so author-link and identifier resolution work on the same owner key.
    """
    pid = (paper_id or "").strip()
    ttl = (title or "").strip()
    old_id = (old_author_id or "").strip()
    new_id = (new_author_id or "").strip()
    if not pid or not old_id or not new_id or old_id == new_id:
        return False, 0

    cur = conn.execute(
        """
        UPDATE papers
        SET author_id = ?
        WHERE id = ?
        """,
        (new_id, pid),
    )
    moved = int(cur.rowcount or 0)
    if moved <= 0:
        return False, 0

    rewired = _rewire_publication_authorship_owner(
        conn=conn,
        paper_id=pid,
        old_author_id=old_id,
        new_author_id=new_id,
    )
    return True, rewired


def _rewire_publication_authorship_owner(
    conn: sqlite3.Connection,
    paper_id: str,
    old_author_id: str,
    new_author_id: str,
) -> int:
    """Re-key ``publication_authors.paper_id`` after owner reassignment."""
    pid = (paper_id or "").strip()
    old_id = (old_author_id or "").strip()
    new_id = (new_author_id or "").strip()
    if not pid or not old_id or not new_id or old_id == new_id:
        return 0

    # Since we migrated to paper_id, no need to change keys - just verify it exists
    try:
        return 0  # No rows to rewire since we use paper_id directly now
    except sqlite3.OperationalError:
        # Table may not exist yet on older/partial databases.
        return 0


def _canonical_author_name(name: str) -> str:
    text = " ".join((name or "").replace("\n", " ").split()).strip(" ,;")
    if not text:
        return ""
    if "," in text:
        parts = [p.strip() for p in text.split(",") if p.strip()]
        if len(parts) >= 2:
            base = f"{parts[1]} {parts[0]}".strip()
            suffix = " ".join(parts[2:]).strip()
            if suffix and _is_name_suffix(suffix):
                base = f"{base} {suffix}".strip()
            return base
    return text


def _find_author_by_name(conn: sqlite3.Connection, name: str) -> Optional[sqlite3.Row]:
    clean = _canonical_author_name(name)
    if not clean:
        return None
    exact = conn.execute(
        "SELECT id, name FROM authors WHERE lower(name) = lower(?) LIMIT 1",
        (clean,),
    ).fetchone()
    if exact:
        return exact

    parts = [p for p in clean.split() if p]
    last = (parts[-1] if parts else "").strip().lower()
    if not last:
        return None
    candidates = conn.execute(
        "SELECT id, name FROM authors WHERE lower(name) LIKE ? LIMIT 200",
        (f"%{last}%",),
    ).fetchall()
    if not candidates:
        return None

    clean_tokens = [tok.strip(".").lower() for tok in clean.replace(",", " ").split() if tok.strip(".")]
    if not clean_tokens:
        return None
    clean_first = clean_tokens[0]
    clean_last = clean_tokens[-1]

    scored: list[tuple[int, sqlite3.Row]] = []
    for row in candidates:
        cand_name = (row["name"] or "").strip()
        cand_tokens = [tok.strip(".").lower() for tok in cand_name.replace(",", " ").split() if tok.strip(".")]
        if not cand_tokens:
            continue
        cand_first = cand_tokens[0]
        cand_last = cand_tokens[-1]
        if cand_last != clean_last:
            continue

        score = 0
        if cand_name.lower() == clean.lower():
            score = 100
        elif cand_first == clean_first:
            score = 90
        elif cand_first[:1] and clean_first[:1] and cand_first[:1] == clean_first[:1]:
            score = 60
        elif _names_match(clean, cand_name):
            score = 40
        if score > 0:
            scored.append((score, row))

    if not scored:
        return None

    scored.sort(key=lambda item: item[0], reverse=True)
    top_score = scored[0][0]
    near_top = [row for score, row in scored if score == top_score]
    if len(near_top) > 1 and top_score < 90:
        return None
    if len(scored) > 1 and top_score < 90 and (top_score - scored[1][0]) < 20:
        return None
    if top_score < 60:
        return None
    return scored[0][1]


def _is_name_suffix(value: str) -> bool:
    token = re.sub(r"[^a-z0-9]+", "", (value or "").lower())
    if not token:
        return False
    suffixes = {
        "jr",
        "sr",
        "ii",
        "iii",
        "iv",
        "v",
        "phd",
        "md",
        "msc",
        "ms",
        "ma",
    }
    return token in suffixes


def _parse_author_names(authors: str) -> list[str]:
    text = (authors or "").strip()
    if not text:
        return []

    if " and " in text.lower():
        parts = [p.strip() for p in re.split(r"\band\b", text, flags=re.IGNORECASE) if p.strip()]
        return parts

    if ";" in text:
        return [p.strip() for p in text.split(";") if p.strip()]

    comma_parts = [p.strip() for p in text.split(",") if p.strip()]
    # Legacy imports may have comma-joined "First Last, First Last, ...".
    if len(comma_parts) >= 2 and all(" " in p for p in comma_parts):
        return comma_parts

    # Common BibTeX style: "Last, First[, Suffix], Last, First[, Suffix], ..."
    if len(comma_parts) >= 4:
        paired: list[str] = []
        idx = 0
        while idx + 1 < len(comma_parts):
            last = comma_parts[idx]
            first = comma_parts[idx + 1]
            idx += 2
            candidate = f"{first} {last}".strip()
            if idx < len(comma_parts) and _is_name_suffix(comma_parts[idx]):
                candidate = f"{candidate} {comma_parts[idx]}".strip()
                idx += 1
            paired.append(candidate)
        if idx == len(comma_parts) and len(paired) >= 2:
            return paired

    return [text]


def _upsert_publication_authorships(
    conn: sqlite3.Connection,
    paper_id: str,
    authorships: list[dict],
) -> int:
    """Store structured authorship data from OpenAlex for a publication.

    Writes one row per authorship entry into ``publication_authors``.
    Replaces existing rows for this publication (idempotent on re-enrichment).
    """
    # Ensure table exists (may already exist from schema init)
    conn.execute(
        """CREATE TABLE IF NOT EXISTS publication_authors (
            paper_id TEXT NOT NULL,
            openalex_id TEXT NOT NULL,
            display_name TEXT NOT NULL,
            orcid TEXT DEFAULT '',
            position TEXT DEFAULT '',
            is_corresponding INTEGER DEFAULT 0,
            institution TEXT DEFAULT '',
            PRIMARY KEY (paper_id, openalex_id)
        )"""
    )
    conn.execute(
        "DELETE FROM publication_authors WHERE paper_id = ?",
        (paper_id,),
    )
    count = 0
    for a in authorships:
        oa_id = (a.get("openalex_id") or "").strip()
        name = (a.get("display_name") or "").strip()
        if not oa_id or not name:
            continue
        orcid = normalize_orcid(a.get("orcid")) or ""
        position = (a.get("position") or "").strip()
        is_corresponding = 1 if a.get("is_corresponding") else 0
        institution = ""
        insts = a.get("institutions") or []
        if insts and isinstance(insts[0], dict):
            institution = (insts[0].get("name") or "").strip()
        conn.execute(
            """INSERT OR REPLACE INTO publication_authors
               (paper_id, openalex_id, display_name, orcid,
                position, is_corresponding, institution)
               VALUES (?, ?, ?, ?, ?, ?, ?)""",
            (paper_id, oa_id, name, orcid, position, is_corresponding, institution),
        )
        count += 1
    return count


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _names_match(pub_name: str, tracked_name: str) -> bool:
    """Check whether a publication author name matches a tracked author.

    Performs case-insensitive comparison.  Handles both "Last, First" and
    "First Last" orderings by checking if the tracked name's last-name
    token appears anywhere in the publication author string.
    """
    p = _canonical_author_name(pub_name).strip().lower()
    t = _canonical_author_name(tracked_name).strip().lower()
    if not p or not t:
        return False

    # Exact match
    if p == t:
        return True

    pub_tokens = [tok.strip(".") for tok in p.replace(",", " ").split() if tok.strip(".")]
    tracked_tokens = [tok.strip(".") for tok in t.replace(",", " ").split() if tok.strip(".")]
    if not pub_tokens or not tracked_tokens:
        return False

    # Last-name anchored checks (handles middle initials and token order noise).
    pub_last = pub_tokens[-1]
    tracked_last = tracked_tokens[-1]
    if pub_last == tracked_last:
        pub_first = pub_tokens[0]
        tracked_first = tracked_tokens[0]
        if pub_first == tracked_first:
            return True
        if pub_first[:1] and tracked_first[:1] and pub_first[:1] == tracked_first[:1]:
            return True

    # Token-subset fallback for compact aliases.
    if len(tracked_tokens) >= 2 and all(token in pub_tokens for token in tracked_tokens):
        return True
    if len(pub_tokens) >= 2 and all(token in tracked_tokens for token in pub_tokens):
        return True

    return False


def _set_job_progress(job_id: str, **kwargs) -> None:
    """Update job status via the scheduler, if available."""
    try:
        from alma.api.scheduler import set_job_status, add_job_log

        set_job_status(job_id, **kwargs)
        msg = kwargs.get("message")
        if msg:
            add_job_log(
                job_id,
                str(msg),
                step="enrichment_progress",
                level="INFO",
                data={"processed": kwargs.get("processed"), "total": kwargs.get("total")},
            )
    except Exception:
        pass
