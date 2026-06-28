"""arXiv discovery source adapter."""

from __future__ import annotations

import logging
import xml.etree.ElementTree as ET
from datetime import datetime
from typing import List, Optional

from alma.core.http_sources import get_source_http_client
from alma.core.scoring_math import rank_score
from alma.core.utils import normalize_doi, normalize_title_key

logger = logging.getLogger(__name__)

_NS = {
    "atom": "http://www.w3.org/2005/Atom",
    "arxiv": "http://arxiv.org/schemas/atom",
}


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


def _fetch_entries(params: dict) -> List[ET.Element]:
    """Issue one arXiv `/api/query` request and return its Atom entries.

    Shared by `search_works` (free-text) and `fetch_abstract_by_id`
    (id_list) so both reuse the same HTTP + XML-parse path. Returns an
    empty list on any HTTP/parse failure (recovery is best-effort).
    """
    try:
        resp = get_source_http_client("arxiv").get("/api/query", params=params, timeout=20)
        if resp.status_code != 200:
            logger.debug("arXiv query returned HTTP %d", resp.status_code)
            return []
        root = ET.fromstring(resp.text)
        return root.findall("atom:entry", namespaces=_NS)
    except ET.ParseError as exc:
        logger.warning("arXiv XML parse failed: %s", exc)
        return []
    except Exception as exc:
        logger.warning("arXiv query failed: %s", exc)
        return []


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
    entries = _fetch_entries(params)
    total = max(len(entries), 1)
    out: list[dict] = []
    for idx, entry in enumerate(entries):
        candidate = _entry_to_candidate(entry, rank_score(idx, total))
        if not candidate:
            continue
        year = candidate.get("year")
        if from_year and isinstance(year, int) and year < from_year:
            continue
        out.append(candidate)
    return out


def fetch_abstract_by_id(arxiv_id: str) -> str:
    """Return the abstract for a known arXiv id via the `id_list` endpoint.

    Used by abstract recovery (task 05) when a paywalled paper's OA mirror
    points at an arXiv preprint twin: arXiv serves the abstract (Atom
    `summary`) unconditionally, with no paywall and no HTML scraping. Bare
    or version-suffixed ids (`1706.03762`, `1706.03762v5`) both resolve.
    Returns "" when the id is unknown or the response carries no summary.
    """
    arxiv_id = (arxiv_id or "").strip()
    if not arxiv_id:
        return ""
    entries = _fetch_entries({"id_list": arxiv_id, "max_results": 1})
    for entry in entries:
        candidate = _entry_to_candidate(entry, 0.0)
        if candidate and str(candidate.get("abstract") or "").strip():
            return str(candidate["abstract"]).strip()
    return ""


def find_abstract_for_title(
    title: str,
    *,
    year: Optional[int] = None,
    year_tolerance: int = 2,
) -> str:
    """Return the abstract of the arXiv preprint whose title matches *title*.

    The "find a twin we have no link to" path for abstract recovery: search
    arXiv by the published paper's title, then accept a hit ONLY when its
    normalized title key matches exactly (the high-precision signal
    `preprint_dedup` trusts) AND its year is within ±`year_tolerance`. This
    strictness is what keeps arXiv's huge corpus from returning a confident
    wrong match. Returns "" when nothing matches.

    Uses the **title field** (`ti:"…"`) rather than the `all:` field that
    `search_works` uses for discovery — arXiv's `all:` relevance ranking
    routinely buries the exact-title paper below loosely-related hits, so a
    title-field phrase query is required for this exact-match intent.
    """
    title = (title or "").strip()
    target_key = normalize_title_key(title)
    if not target_key:
        return ""
    # Drop embedded double-quotes so they can't break the quoted phrase query.
    phrase = title.replace('"', " ").strip()
    params = {
        "search_query": f'ti:"{phrase}"',
        "start": 0,
        "max_results": 5,
        "sortBy": "relevance",
        "sortOrder": "descending",
    }
    for entry in _fetch_entries(params):
        candidate = _entry_to_candidate(entry, 0.0)
        if not candidate or normalize_title_key(candidate.get("title")) != target_key:
            continue
        cand_year = candidate.get("year")
        if (
            year is not None
            and isinstance(cand_year, int)
            and abs(cand_year - year) > year_tolerance
        ):
            continue
        abstract = str(candidate.get("abstract") or "").strip()
        if abstract:
            return abstract
    return ""
