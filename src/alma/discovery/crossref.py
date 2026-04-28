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
        "score": round(float(score), 4),
        "source_api": "crossref",
    }


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
