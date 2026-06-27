"""Crossref discovery source adapter."""

from __future__ import annotations

import logging
import re
from typing import List, Optional

from alma.core.http_sources import get_source_http_client
from alma.core.utils import normalize_doi

logger = logging.getLogger(__name__)

_JATS_TAG_RE = re.compile(r"<[^>]+>")


def _strip_jats(text: str) -> str:
    cleaned = _JATS_TAG_RE.sub(" ", text or "")
    return " ".join(cleaned.split()).strip()


def parent_doi_from_relation(relation: object) -> Optional[str]:
    """Parent DOI from a Crossref ``relation`` block, or ``None``.

    Only ``is-supplement-to`` is trusted: it explicitly means "this work is
    supplementary material to <DOI>", which is exactly the part-of signal we
    want for datasets / supplements (alma.core.components). ``is-part-of`` is
    deliberately NOT used — it often points at the journal / proceedings, which
    would wrongly classify a real article as a component.
    """
    if not isinstance(relation, dict):
        return None
    entries = relation.get("is-supplement-to")
    if not isinstance(entries, list):
        return None
    for entry in entries:
        if isinstance(entry, dict) and str(entry.get("id-type") or "").lower() == "doi":
            parent = normalize_doi(str(entry.get("id") or ""))
            if parent:
                return parent
    return None


def _score_by_rank(index: int, total: int) -> float:
    return round(max(0.0, 1.0 - (index / max(total, 1))), 4)


def _extract_year(item: dict) -> Optional[int]:
    for key in ("issued", "published-print", "published-online", "created"):
        block = item.get(key)
        if not isinstance(block, dict):
            continue
        parts = block.get("date-parts")
        if not isinstance(parts, list) or not parts:
            continue
        first = parts[0]
        if isinstance(first, list) and first:
            try:
                return int(first[0])
            except Exception:
                continue
    return None


def _extract_publication_date(item: dict) -> Optional[str]:
    for key in ("issued", "published-print", "published-online", "created"):
        block = item.get(key)
        if not isinstance(block, dict):
            continue
        parts = block.get("date-parts")
        if not isinstance(parts, list) or not parts:
            continue
        first = parts[0]
        if not isinstance(first, list) or not first:
            continue
        try:
            year = int(first[0])
        except Exception:
            continue
        month = None
        day = None
        try:
            if len(first) >= 2:
                month = int(first[1])
            if len(first) >= 3:
                day = int(first[2])
        except Exception:
            month = None
            day = None
        if month is not None and day is not None:
            return f"{year:04d}-{month:02d}-{day:02d}"
        if month is not None:
            return f"{year:04d}-{month:02d}-01"
        return f"{year:04d}-01-01"
    return None


def _crossref_to_candidate(item: dict, score: float) -> Optional[dict]:
    titles = item.get("title") or []
    title = ""
    if isinstance(titles, list) and titles:
        title = (titles[0] or "").strip()
    elif isinstance(titles, str):
        title = titles.strip()
    if not title:
        return None

    authors_raw = item.get("author") or []
    author_names: list[str] = []
    if isinstance(authors_raw, list):
        for author in authors_raw:
            if not isinstance(author, dict):
                continue
            given = (author.get("given") or "").strip()
            family = (author.get("family") or "").strip()
            full = " ".join(part for part in (given, family) if part).strip()
            if full:
                author_names.append(full)

    doi_raw = (item.get("DOI") or "").strip()
    doi = normalize_doi(doi_raw) or doi_raw
    url = (item.get("URL") or "").strip()
    if not url and doi:
        url = f"https://doi.org/{doi}"

    journals = item.get("container-title") or []
    journal = ""
    if isinstance(journals, list) and journals:
        journal = (journals[0] or "").strip()

    abstract_raw = item.get("abstract") or ""
    abstract = _strip_jats(abstract_raw) if isinstance(abstract_raw, str) else ""

    return {
        "title": title,
        "authors": ", ".join(author_names),
        "year": _extract_year(item),
        "publication_date": _extract_publication_date(item),
        "journal": journal,
        "doi": doi,
        "url": url,
        "cited_by_count": int(item.get("is-referenced-by-count") or 0),
        "abstract": abstract,
        # Part-of signal: the parent paper this work supplements, if Crossref
        # declares it (alma.core.components links datasets / supplements here).
        "parent_doi": parent_doi_from_relation(item.get("relation")),
        "score": round(float(score), 4),
        "source_api": "crossref",
    }


def fetch_work_by_doi(doi: str) -> Optional[dict]:
    """Fetch a single Crossref work by DOI.

    Returns a normalized candidate dict (same shape as `_crossref_to_candidate`)
    or `None` on miss / network error / non-200. Crossref's
    `/works/{doi}` endpoint is the authoritative source for abstracts on
    DOI-bearing works that OpenAlex hasn't indexed an
    `abstract_inverted_index` for — typically ARVO / Journal of Vision
    proceedings and other late-binding venues. A bare-bones single-DOI
    fetcher because the existing `search_works` is a free-text search,
    not an identifier lookup.
    """
    normalized = normalize_doi(doi or "")
    if not normalized:
        return None
    try:
        resp = get_source_http_client("crossref").get(
            f"/works/{normalized}", timeout=20
        )
    except Exception as exc:
        logger.warning("Crossref by-DOI fetch failed for %s: %s", normalized, exc)
        return None
    if resp.status_code != 200:
        if resp.status_code != 404:
            logger.debug(
                "Crossref by-DOI returned HTTP %d for %s",
                resp.status_code,
                normalized,
            )
        return None
    try:
        message = ((resp.json() or {}).get("message")) or {}
    except Exception as exc:
        logger.warning("Crossref by-DOI JSON decode failed for %s: %s", normalized, exc)
        return None
    if not isinstance(message, dict) or not message:
        return None
    return _crossref_to_candidate(message, score=1.0)


def _fetch_works_chunk(chunk: list[str]) -> dict[str, dict]:
    """Single-chunk worker for `fetch_works_by_dois`. Public via the
    parallel orchestrator below; otherwise treat as private."""
    if not chunk:
        return {}
    filter_value = ",".join(f"doi:{doi}" for doi in chunk)
    params = {
        "filter": filter_value,
        "rows": len(chunk),
        "select": (
            "DOI,title,author,abstract,container-title,issued,"
            "published-print,published-online,is-referenced-by-count,URL,relation"
        ),
    }
    try:
        resp = get_source_http_client("crossref").get("/works", params=params, timeout=30)
    except Exception as exc:
        logger.warning(
            "Crossref batch DOI lookup failed for %d DOIs: %s", len(chunk), exc
        )
        return {}
    if resp.status_code != 200:
        logger.debug(
            "Crossref batch DOI lookup returned HTTP %d for %d DOIs",
            resp.status_code,
            len(chunk),
        )
        return {}
    try:
        items = (((resp.json() or {}).get("message") or {}).get("items")) or []
    except Exception as exc:
        logger.warning("Crossref batch JSON decode failed: %s", exc)
        return {}
    out: dict[str, dict] = {}
    for item in items:
        if not isinstance(item, dict):
            continue
        doi_raw = str(item.get("DOI") or "").strip()
        if not doi_raw:
            continue
        normalized = normalize_doi(doi_raw) or doi_raw
        candidate = _crossref_to_candidate(item, score=1.0)
        if candidate:
            out[normalized.lower()] = candidate
    return out


def fetch_works_by_dois(
    dois: list[str], *, batch_size: int = 50, max_workers: int = 3
) -> dict[str, dict]:
    """Fetch multiple Crossref works in one HTTP call per chunk.

    Uses ``GET /works?filter=doi:DOI1,doi:DOI2,...&rows=N`` (Crossref
    accepts repeated `doi:` filter clauses comma-joined). Polite pool
    gives 3 RPS for list queries vs 10 RPS for singletons, but each
    list call resolves up to `batch_size` DOIs at once — a 17×
    reduction in HTTP round-trips at the documented limits, larger in
    practice once `min_interval_seconds` is factored in.

    Chunks run concurrently up to `max_workers` (default 3, matching
    Crossref's polite-pool list-query concurrency cap). The shared
    HTTP client (`SourceHttpClient` for source `crossref`) still
    enforces the per-source min interval, so concurrency above the
    cap silently serialises rather than triggering 429s.

    Phase 8b/8d of `tasks/13_END_TO_END_HYDRATION_VECTOR_CHAIN.md`.

    Args:
        dois: list of DOI strings; each is normalized via
            ``normalize_doi`` (strips URL prefixes / `DOI:`). Missing
            or malformed DOIs are silently skipped.
        batch_size: max DOIs per HTTP call. Capped at 50 to stay well
            under Crossref's 4 KB URL length cap.
        max_workers: concurrent chunk requests. Capped at 3
            (polite-pool list-query concurrency limit).

    Returns:
        dict keyed by lowercased normalized DOI → candidate dict (same
        shape as ``fetch_work_by_doi``). DOIs that didn't resolve are
        absent from the dict.
    """
    normalized_dois: list[str] = []
    seen: set[str] = set()
    for raw in dois or []:
        norm = normalize_doi(str(raw or ""))
        if not norm:
            continue
        key = norm.lower()
        if key in seen:
            continue
        seen.add(key)
        normalized_dois.append(norm)
    if not normalized_dois:
        return {}

    chunk_size = max(1, min(int(batch_size or 50), 50))
    chunks = [
        normalized_dois[i : i + chunk_size]
        for i in range(0, len(normalized_dois), chunk_size)
    ]
    workers = max(1, min(int(max_workers or 1), 3))

    out: dict[str, dict] = {}
    if workers == 1 or len(chunks) == 1:
        for chunk in chunks:
            out.update(_fetch_works_chunk(chunk))
        return out

    from concurrent.futures import as_completed

    from alma.core.concurrency import bounded_thread_pool

    with bounded_thread_pool(workers) as ex:
        futures = [ex.submit(_fetch_works_chunk, chunk) for chunk in chunks]
        for fut in as_completed(futures):
            try:
                out.update(fut.result())
            except Exception as exc:
                logger.warning("Crossref batch chunk failed in pool: %s", exc)
    return out


def search_works(
    query: str,
    *,
    limit: int = 20,
    from_year: Optional[int] = None,
) -> List[dict]:
    """Search Crossref works by free-text bibliographic query."""
    query = (query or "").strip()
    if not query:
        return []

    params: dict[str, object] = {
        "query.bibliographic": query,
        "rows": min(max(limit, 1), 100),
        "sort": "score",
        "order": "desc",
    }
    filters: list[str] = []
    if from_year:
        filters.append(f"from-pub-date:{int(from_year)}-01-01")
    if filters:
        params["filter"] = ",".join(filters)

    try:
        resp = get_source_http_client("crossref").get("/works", params=params, timeout=20)
        if resp.status_code != 200:
            logger.debug("Crossref query search returned HTTP %d", resp.status_code)
            return []

        items = (((resp.json() or {}).get("message") or {}).get("items")) or []
        total = max(len(items), 1)
        out: list[dict] = []
        for idx, item in enumerate(items):
            if not isinstance(item, dict):
                continue
            candidate = _crossref_to_candidate(item, _score_by_rank(idx, total))
            if candidate:
                out.append(candidate)
        return out
    except Exception as exc:
        logger.warning("Crossref query search failed: %s", exc)
        return []
