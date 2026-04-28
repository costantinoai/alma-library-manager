"""arXiv discovery source adapter."""

from __future__ import annotations

import logging
import xml.etree.ElementTree as ET
from datetime import datetime
from typing import List, Optional

from alma.core.http_sources import get_source_http_client
from alma.core.utils import normalize_doi

logger = logging.getLogger(__name__)

_NS = {
    "atom": "http://www.w3.org/2005/Atom",
    "arxiv": "http://arxiv.org/schemas/atom",
}


def _score_by_rank(index: int, total: int) -> float:
    return round(max(0.0, 1.0 - (index / max(total, 1))), 4)


def _extract_year(published: str) -> Optional[int]:
    if not published:
        return None
    try:
        return datetime.fromisoformat(published.replace("Z", "+00:00")).year
    except Exception:
        try:
            return int((published or "")[:4])
        except Exception:
            return None


def _entry_to_candidate(entry: ET.Element, score: float) -> Optional[dict]:
    title = (entry.findtext("atom:title", default="", namespaces=_NS) or "").strip()
    title = " ".join(title.split())
    if not title:
        return None

    summary = (entry.findtext("atom:summary", default="", namespaces=_NS) or "").strip()
    summary = " ".join(summary.split())
    url = (entry.findtext("atom:id", default="", namespaces=_NS) or "").strip()
    published = (entry.findtext("atom:published", default="", namespaces=_NS) or "").strip()

    names = []
    for author in entry.findall("atom:author", namespaces=_NS):
        name = (author.findtext("atom:name", default="", namespaces=_NS) or "").strip()
        if name:
            names.append(name)

    doi_raw = (entry.findtext("arxiv:doi", default="", namespaces=_NS) or "").strip()
    doi = normalize_doi(doi_raw) or doi_raw
    if not url and doi:
        url = f"https://doi.org/{doi}"

    return {
        "title": title,
        "authors": ", ".join(names),
        "year": _extract_year(published),
        "publication_date": published[:10] if published else None,
        "journal": "arXiv",
        "doi": doi,
        "url": url,
        "cited_by_count": 0,
        "abstract": summary,
        "score": round(float(score), 4),
        "source_api": "arxiv",
    }


def search_works(
    query: str,
    *,
    limit: int = 20,
    from_year: Optional[int] = None,
) -> List[dict]:
    """Search arXiv by free-text query."""
    query = (query or "").strip()
    if not query:
        return []

    params = {
        "search_query": f"all:{query}",
        "start": 0,
        "max_results": min(max(limit, 1), 50),
        "sortBy": "relevance",
        "sortOrder": "descending",
    }
    try:
        resp = get_source_http_client("arxiv").get("/api/query", params=params, timeout=20)
        if resp.status_code != 200:
            logger.debug("arXiv query search returned HTTP %d", resp.status_code)
            return []

        root = ET.fromstring(resp.text)
        entries = root.findall("atom:entry", namespaces=_NS)
        total = max(len(entries), 1)
        out: list[dict] = []
        for idx, entry in enumerate(entries):
            candidate = _entry_to_candidate(entry, _score_by_rank(idx, total))
            if not candidate:
                continue
            year = candidate.get("year")
            if from_year and isinstance(year, int) and year < from_year:
                continue
            out.append(candidate)
        return out
    except ET.ParseError as exc:
        logger.warning("arXiv XML parse failed: %s", exc)
        return []
    except Exception as exc:
        logger.warning("arXiv query search failed: %s", exc)
        return []
