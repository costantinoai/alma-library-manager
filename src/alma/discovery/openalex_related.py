"""Fetch related works from OpenAlex for discovery recommendations.

Uses the OpenAlex Works endpoint to find papers related to a given work,
identified by DOI or OpenAlex ID. This provides an external signal for
recommendations beyond local TF-IDF similarity.
"""

from __future__ import annotations

import logging
from typing import Dict, List, Optional

from alma.openalex.http import get_client
from alma.openalex.client import (
    _WORKS_SELECT_FIELDS,
    _normalize_work,
    batch_fetch_works_by_openalex_ids,
    batch_fetch_recent_works_for_authors as _client_batch_author_works,
)
from alma.core.utils import normalize_doi

logger = logging.getLogger(__name__)


def _normalize_id(doi_or_openalex_id: str) -> str:
    """Normalize a DOI or OpenAlex ID into a form suitable for the Works endpoint.

    Accepts:
    - Bare DOI: "10.1234/example"
    - DOI URL: "https://doi.org/10.1234/example"
    - OpenAlex URL: "https://openalex.org/W1234567890"
    - Bare OpenAlex ID: "W1234567890"

    Returns a string usable with GET /works/{id}.
    """
    val = doi_or_openalex_id.strip()

    # Already a full DOI URL -- use as-is (OpenAlex accepts doi URLs as IDs)
    if val.lower().startswith("https://doi.org/") or val.lower().startswith("http://doi.org/"):
        return val

    # Full OpenAlex URL -- extract the ID
    if "openalex.org/" in val.lower():
        return val.rstrip("/").split("/")[-1]

    # Bare DOI (starts with 10.)
    if val.startswith("10."):
        return f"https://doi.org/{val}"

    # Bare OpenAlex ID (starts with W)
    return val


def _extract_journal(work: dict) -> str:
    """Best-effort extraction of journal/source display name from OpenAlex work."""
    primary_loc = work.get("primary_location") or {}
    source = primary_loc.get("source") or {}
    if isinstance(source, dict):
        name = (source.get("display_name") or "").strip()
        if name:
            return name
    return ""


def _normalize_doi_url(doi_raw: str) -> str:
    doi = normalize_doi(doi_raw or "")
    return doi or (doi_raw or "")


def _score_by_rank(index: int, total: int) -> float:
    return round(max(0.0, 1.0 - (index / max(total, 1))), 4)


def _work_to_result(work: dict, score: float) -> Optional[Dict]:
    normalized = _normalize_work(work)
    title = str(normalized.get("title") or "").strip()
    if not title:
        return None

    return {
        "openalex_id": (str(normalized.get("openalex_id") or "").rstrip("/").split("/")[-1] if normalized.get("openalex_id") else ""),
        "title": title,
        "authors": str(normalized.get("authors") or "").strip(),
        "authorships": normalized.get("authorships") or [],
        "abstract": str(normalized.get("abstract") or "").strip(),
        "url": str(normalized.get("pub_url") or "").strip(),
        "doi": _normalize_doi_url(normalized.get("doi") or ""),
        "score": round(float(score), 4),
        "year": normalized.get("year"),
        "publication_date": normalized.get("publication_date"),
        "journal": str(normalized.get("journal") or "").strip() or _extract_journal(work),
        "cited_by_count": normalized.get("num_citations") or 0,
        "topics": normalized.get("topics") or [],
        "keywords": normalized.get("keywords") or [],
        "institutions": normalized.get("institutions") or [],
        "referenced_works": normalized.get("referenced_works"),
    }


def fetch_related_works(
    doi_or_openalex_id: str,
    limit: int = 10,
) -> List[Dict]:
    """Fetch related works for a given publication from OpenAlex.

    Calls GET /works/{id} to obtain the ``related_works`` field, then
    fetches basic metadata for each related work.

    Args:
        doi_or_openalex_id: DOI (bare or URL) or OpenAlex Work ID.
        limit: Maximum number of related works to return.

    Returns:
        List of dicts with keys: title, authors, url, doi, score.
        Score is computed from position in the related works list (1.0 for
        the first, linearly decreasing). Returns an empty list on failure.
    """
    if not doi_or_openalex_id or not doi_or_openalex_id.strip():
        return []

    work_id = _normalize_id(doi_or_openalex_id)
    client = get_client()

    resolved_work_id = work_id
    # Resolve to a canonical OpenAlex work ID (W...) only when needed.
    if not resolved_work_id.upper().startswith("W"):
        try:
            resolve_resp = client.get(
                f"/works/{work_id}",
                params={"select": "id"},
                timeout=20,
            )
            if resolve_resp.status_code != 200:
                logger.debug(
                    "OpenAlex work lookup failed for '%s': HTTP %d",
                    doi_or_openalex_id,
                    resolve_resp.status_code,
                )
                return []
            resolved_work_id = ((resolve_resp.json() or {}).get("id") or "").rstrip("/").split("/")[-1]
            if not resolved_work_id:
                return []
        except Exception as exc:
            logger.warning("OpenAlex work fetch failed for '%s': %s", doi_or_openalex_id, exc)
            return []

    # Fast path: use related_to filter directly (single list request).
    try:
        related_resp = client.get(
            "/works",
            params={
                "filter": f"related_to:{resolved_work_id}",
                "per-page": min(limit, 100),
                "select": "id,doi,display_name,authorships,primary_location,publication_year,publication_date,cited_by_count",
            },
            timeout=20,
        )
        if related_resp.status_code == 200:
            works = (related_resp.json() or {}).get("results") or []
            results: List[Dict] = []
            for i, w in enumerate(works[:limit]):
                mapped = _work_to_result(w, _score_by_rank(i, len(works)))
                if mapped:
                    results.append(mapped)
            if results:
                return results
        else:
            logger.debug(
                "OpenAlex related_to fetch failed for '%s': HTTP %d",
                doi_or_openalex_id,
                related_resp.status_code,
            )
    except Exception as exc:
        logger.debug("OpenAlex related_to fetch failed for '%s': %s", doi_or_openalex_id, exc)

    # Fallback path: fetch source work's related_works and batch-fetch IDs.
    try:
        resp = client.get(
            f"/works/{resolved_work_id}",
            params={"select": "id,related_works"},
            timeout=20,
        )
        if resp.status_code != 200:
            logger.debug(
                "OpenAlex work lookup failed for '%s': HTTP %d",
                doi_or_openalex_id,
                resp.status_code,
            )
            return []
        data = resp.json() or {}
    except Exception as exc:
        logger.warning("OpenAlex work fetch failed for '%s': %s", doi_or_openalex_id, exc)
        return []

    related_ids = data.get("related_works") or []
    if not related_ids:
        logger.debug("No related works found for '%s'", doi_or_openalex_id)
        return []

    # Trim to limit
    related_ids = related_ids[:limit]

    # Step 2: Fetch metadata for the related works using batched piped IDs.
    bare_ids = []
    for rid in related_ids:
        if isinstance(rid, str):
            bare = rid.rstrip("/").split("/")[-1] if "openalex.org/" in rid else rid
            bare_ids.append(bare)

    if not bare_ids:
        return []

    try:
        work_map = batch_fetch_works_by_openalex_ids(
            bare_ids,
            batch_size=100,
            max_workers=4,
        )
        if not work_map:
            return []

        # Build an ordering map so we can assign position-based scores
        id_order = {bid: idx for idx, bid in enumerate(bare_ids)}
        results: List[Dict] = []

        for w_id in bare_ids:
            w = work_map.get(w_id)
            if not w:
                continue
            position = id_order.get(w_id, len(bare_ids))
            mapped = _work_to_result(w, _score_by_rank(position, len(bare_ids)))
            if mapped:
                results.append(mapped)

        # Sort by score descending
        results.sort(key=lambda x: x["score"], reverse=True)

    except Exception as exc:
        logger.warning("OpenAlex related works batch fetch failed: %s", exc)
        return []

    return results


def fetch_referenced_works(
    doi_or_openalex_id: str,
    limit: int = 10,
) -> List[Dict]:
    """Fetch works referenced by a source publication from OpenAlex."""
    if not doi_or_openalex_id or not doi_or_openalex_id.strip():
        return []

    work_id = _normalize_id(doi_or_openalex_id)
    client = get_client()

    try:
        resp = client.get(
            f"/works/{work_id}",
            params={"select": "id,referenced_works"},
            timeout=20,
        )
        if resp.status_code != 200:
            logger.debug(
                "OpenAlex referenced works lookup failed for '%s': HTTP %d",
                doi_or_openalex_id,
                resp.status_code,
            )
            return []
        data = resp.json() or {}
    except Exception as exc:
        logger.warning("OpenAlex referenced works fetch failed for '%s': %s", doi_or_openalex_id, exc)
        return []

    referenced_ids = data.get("referenced_works") or []
    if not referenced_ids:
        return []

    bare_ids: List[str] = []
    for rid in referenced_ids[:limit]:
        if not isinstance(rid, str):
            continue
        bare_ids.append(rid.rstrip("/").split("/")[-1] if "openalex.org/" in rid else rid)

    if not bare_ids:
        return []

    try:
        work_map = batch_fetch_works_by_openalex_ids(
            bare_ids,
            batch_size=100,
            max_workers=4,
        )
    except Exception as exc:
        logger.warning("OpenAlex referenced works batch fetch failed: %s", exc)
        return []

    results: List[Dict] = []
    for idx, work_id in enumerate(bare_ids):
        work = work_map.get(work_id)
        if not work:
            continue
        mapped = _work_to_result(work, _score_by_rank(idx, len(bare_ids)))
        if mapped:
            results.append(mapped)
    return results


def search_works_by_topics(
    topics: List[str],
    limit: int = 20,
    from_year: Optional[int] = None,
) -> List[Dict]:
    """Search OpenAlex for recent works matching the given topic keywords.

    Uses the OpenAlex search endpoint to find papers related to
    the user's preferred topics. Returns external papers only.

    Args:
        topics: List of topic/keyword strings to search for.
        limit: Maximum number of results to return.
        from_year: Only include works published from this year onwards.

    Returns:
        List of dicts with keys: title, authors, url, doi, score, year.
    """
    if not topics:
        return []

    query = " OR ".join(topics[:10])  # cap at 10 terms
    return search_works(query=query, limit=limit, from_year=from_year)


def search_works(
    query: str,
    limit: int = 20,
    from_year: Optional[int] = None,
) -> List[Dict]:
    """Search OpenAlex works by free-text query.

    Args:
        query: Free-text query string.
        limit: Maximum number of works to return.
        from_year: Optional lower publication-year bound (inclusive).

    Returns:
        List of normalized candidate dicts.
    """
    query = (query or "").strip()
    if not query:
        return []

    client = get_client()
    params: Dict[str, object] = {
        "search": query,
        "per-page": min(limit, 100),
        "sort": "relevance_score:desc",
        "select": _WORKS_SELECT_FIELDS,
    }
    if from_year:
        params["filter"] = f"from_publication_date:{from_year}-01-01"

    try:
        resp = client.get("/works", params=params, timeout=30)
        if resp.status_code != 200:
            logger.debug("OpenAlex query search returned HTTP %d", resp.status_code)
            return []

        works = (resp.json() or {}).get("results") or []
        results: List[Dict] = []
        total = max(len(works), 1)
        for i, w in enumerate(works):
            mapped = _work_to_result(w, _score_by_rank(i, total))
            if mapped:
                results.append(mapped)
        return results
    except Exception as exc:
        logger.warning("OpenAlex query search failed: %s", exc)
        return []


def find_similar_works(
    query: str,
    limit: int = 12,
    from_year: Optional[int] = None,
) -> List[Dict]:
    """Use OpenAlex semantic work search when available."""
    query = (query or "").strip()
    if not query:
        return []

    client = get_client()
    try:
        resp = client.get(
            "/find/works",
            params={
                "query": query,
                "limit": min(limit, 25),
            },
            timeout=30,
        )
        if resp.status_code != 200:
            logger.debug("OpenAlex semantic find/works returned HTTP %d", resp.status_code)
            return []

        payload = resp.json() or {}
        raw_items = payload.get("results") or payload.get("data") or payload.get("works") or []
        results: List[Dict] = []
        total = max(len(raw_items), 1)
        for idx, item in enumerate(raw_items):
            if not isinstance(item, dict):
                continue
            work = item.get("work") if isinstance(item.get("work"), dict) else item
            if not isinstance(work, dict):
                continue
            year = work.get("publication_year")
            if from_year is not None:
                try:
                    if year is not None and int(year) < int(from_year):
                        continue
                except (TypeError, ValueError):
                    pass
            mapped = _work_to_result(work, float(item.get("score") or _score_by_rank(idx, total)))
            if mapped:
                mapped["source_type"] = "external_semantic_search"
                results.append(mapped)
        return results
    except Exception as exc:
        logger.debug("OpenAlex semantic find/works failed: %s", exc)
        return []


def search_works_hybrid(
    query: str,
    limit: int = 20,
    from_year: Optional[int] = None,
) -> List[Dict]:
    """Blend lexical and semantic OpenAlex search for broader new-paper retrieval."""
    lexical = search_works(query=query, limit=limit, from_year=from_year)
    semantic = find_similar_works(query=query, limit=max(4, min(limit, 12)), from_year=from_year)

    merged: dict[str, Dict] = {}
    for idx, item in enumerate(lexical):
        candidate = dict(item)
        candidate["score"] = round(max(float(candidate.get("score", 0.0) or 0.0), _score_by_rank(idx, max(len(lexical), 1)) * 0.92), 4)
        key = normalize_doi(candidate.get("doi") or "") or candidate.get("openalex_id") or (candidate.get("title") or "").strip().lower()
        if key:
            merged[str(key)] = candidate
    for idx, item in enumerate(semantic):
        candidate = dict(item)
        candidate["score"] = round(max(float(candidate.get("score", 0.0) or 0.0), _score_by_rank(idx, max(len(semantic), 1)) * 0.88), 4)
        key = normalize_doi(candidate.get("doi") or "") or candidate.get("openalex_id") or (candidate.get("title") or "").strip().lower()
        if not key:
            continue
        existing = merged.get(str(key))
        if existing is None or float(candidate.get("score", 0.0) or 0.0) > float(existing.get("score", 0.0) or 0.0):
            merged[str(key)] = candidate

    ranked = sorted(merged.values(), key=lambda item: float(item.get("score", 0.0) or 0.0), reverse=True)
    return ranked[: max(1, limit)]


def fetch_citing_works(
    doi_or_openalex_id: str,
    limit: int = 10,
) -> List[Dict]:
    """Fetch works that cite a given publication from OpenAlex.

    Uses the OpenAlex filter ``cites:{work_id}`` to find papers that reference
    the given work.  This is the reverse of ``related_works`` -- it finds
    papers that build on the user's favorites.

    Args:
        doi_or_openalex_id: DOI (bare or URL) or OpenAlex Work ID.
        limit: Maximum number of citing works to return.

    Returns:
        List of dicts with keys: title, authors, url, doi, score, year.
        Score is position-based (1st = 1.0, decays). Empty list on failure.
    """
    if not doi_or_openalex_id or not doi_or_openalex_id.strip():
        return []

    work_id = _normalize_id(doi_or_openalex_id)
    client = get_client()

    # Step 1: Resolve to an OpenAlex work ID (e.g. W1234567890) only when needed.
    openalex_id = work_id
    if not openalex_id.upper().startswith("W"):
        try:
            resp = client.get(
                f"/works/{work_id}",
                params={"select": "id"},
                timeout=20,
            )
            if resp.status_code != 200:
                logger.debug(
                    "OpenAlex work lookup failed for '%s': HTTP %d",
                    doi_or_openalex_id,
                    resp.status_code,
                )
                return []

            openalex_id = ((resp.json() or {}).get("id") or "").rstrip("/").split("/")[-1]
            if not openalex_id:
                logger.debug("Could not resolve OpenAlex ID for '%s'", doi_or_openalex_id)
                return []
        except Exception as exc:
            logger.warning("OpenAlex work resolve failed for '%s': %s", doi_or_openalex_id, exc)
            return []

    # Step 2: Fetch works that cite the resolved work
    try:
        resp = client.get(
            "/works",
            params={
                "filter": f"cites:{openalex_id}",
                "sort": "cited_by_count:desc",
                "per-page": min(limit, 100),
                "select": "id,doi,display_name,authorships,primary_location,publication_year,publication_date,cited_by_count",
            },
            timeout=30,
        )
        if resp.status_code != 200:
            logger.debug(
                "OpenAlex citing works fetch returned HTTP %d",
                resp.status_code,
            )
            return []

        works = (resp.json() or {}).get("results") or []
        results: List[Dict] = []

        for i, w in enumerate(works):
            mapped = _work_to_result(w, _score_by_rank(i, len(works)))
            if mapped:
                results.append(mapped)

        # Sort by score descending
        results.sort(key=lambda x: x["score"], reverse=True)

    except Exception as exc:
        logger.warning("OpenAlex citing works fetch failed: %s", exc)
        return []

    return results


def fetch_recent_works_for_author(
    openalex_author_id: str,
    from_year: Optional[int] = None,
    limit: int = 20,
) -> List[Dict]:
    """Fetch recent works by a specific author from OpenAlex.

    Used by the recommendation engine to find papers from followed authors
    that may not be in the local database yet.

    Args:
        openalex_author_id: OpenAlex author ID (bare or URL form).
        from_year: Only include works from this year onwards.
        limit: Maximum number of works to return.

    Returns:
        List of dicts with keys: title, authors, url, doi, score, year.
        Score is position-based. Empty list on failure.
    """
    if not openalex_author_id or not openalex_author_id.strip():
        return []

    # Normalize author ID: strip URL prefix if present
    aid = openalex_author_id.strip()
    if "openalex.org/" in aid:
        aid = aid.rstrip("/").split("/")[-1]

    client = get_client()

    # Build filter
    filter_parts = [f"author.id:{aid}"]
    if from_year:
        filter_parts.append(f"from_publication_date:{from_year}-01-01")
    filter_str = ",".join(filter_parts)

    try:
        resp = client.get(
            "/works",
            params={
                "filter": filter_str,
                "sort": "publication_year:desc",
                "per-page": min(limit, 100),
                "select": "id,doi,display_name,authorships,primary_location,publication_year,publication_date,cited_by_count",
            },
            timeout=30,
        )
        if resp.status_code != 200:
            logger.debug(
                "OpenAlex author works fetch returned HTTP %d for '%s'",
                resp.status_code,
                aid,
            )
            return []

        works = (resp.json() or {}).get("results") or []
        results: List[Dict] = []

        for i, w in enumerate(works):
            mapped = _work_to_result(w, _score_by_rank(i, len(works)))
            if mapped:
                results.append(mapped)

        # Sort by score descending
        results.sort(key=lambda x: x["score"], reverse=True)

    except Exception as exc:
        logger.warning("OpenAlex author works fetch failed for '%s': %s", aid, exc)
        return []

    return results


def batch_fetch_recent_works_for_authors(
    author_ids: List[str],
    from_year: Optional[int] = None,
    per_author_limit: int = 10,
) -> Dict[str, List[Dict]]:
    """Fetch recent works for multiple authors in a single batched operation.

    Wraps :func:`alma.openalex.client.batch_fetch_recent_works_for_authors`
    and normalizes each returned work into the standard result format used
    by the discovery engine (same shape as :func:`fetch_recent_works_for_author`).

    Args:
        author_ids: List of OpenAlex author IDs (bare or URL form).
        from_year: Only include works published from this year onwards.
        per_author_limit: Maximum works to keep per author.

    Returns:
        Dict mapping author ID -> list of result dicts with keys:
        title, authors, url, doi, score, year, journal, cited_by_count.
        Authors with no works are omitted.
    """
    if not author_ids:
        return {}

    try:
        raw_map = _client_batch_author_works(
            author_ids,
            from_year=from_year,
            per_author_limit=per_author_limit,
        )
    except Exception as exc:
        logger.warning("Batch author works fetch failed: %s", exc)
        return {}

    result: Dict[str, List[Dict]] = {}
    for aid, works in raw_map.items():
        mapped: List[Dict] = []
        for i, w in enumerate(works):
            item = _work_to_result(w, _score_by_rank(i, len(works)))
            if item:
                mapped.append(item)
        # Sort by score descending (position-based)
        mapped.sort(key=lambda x: x["score"], reverse=True)
        if mapped:
            result[aid] = mapped

    return result
