"""Fetch candidate papers from the Europe PMC REST API.

Europe PMC indexes PubMed/MEDLINE, PMC, preprint servers and Agricola, which
makes it the life-sciences counterpart to the CS-leaning Semantic Scholar
corpus. It is free, needs no key, and returns abstracts inline — so it is a
cheap breadth source for the frontier builder (task 62 §8).

Deliberately NOT a metadata authority: OpenAlex and Crossref stay the
identifier/metadata sources of truth. This adapter contributes *discovery*
candidates and the identifiers (DOI / PMID / PMCID) needed to reconcile them.

API reference: https://europepmc.org/RestfulWebService
"""

from __future__ import annotations

import logging
from typing import Any

from alma.core.http_sources import get_source_http_client
from alma.core.scoring_math import rank_score
from alma.core.utils import normalize_doi

logger = logging.getLogger(__name__)

SOURCE_API = "europe_pmc"

#: `resultType=core` is the only tier that includes `abstractText`, which is
#: what makes these rows useful for semantic scoring at all. `lite` would be
#: cheaper but returns no abstract, so every candidate would arrive
#: unscoreable — the same defect class as an unhydrated S2 bulk row.
_RESULT_TYPE = "core"
#: Europe PMC caps `pageSize` at 1000; we never need that many per query.
_MAX_PAGE_SIZE = 100


def _europe_pmc_to_candidate(item: dict, score: float) -> dict | None:
    """Convert one Europe PMC result to the ALMa candidate shape.

    Returns ``None`` when the row has no title (nothing downstream can use it).
    """
    title = str(item.get("title") or "").strip().rstrip(".")
    if not title:
        return None

    doi_raw = str(item.get("doi") or "").strip()
    doi = normalize_doi(doi_raw) or doi_raw

    # `journalTitle` is NOT a top-level field despite appearing in some docs —
    # verified live 2026-07-27, it is nested under `journalInfo.journal.title`.
    journal = ""
    journal_info = item.get("journalInfo")
    if isinstance(journal_info, dict):
        journal_obj = journal_info.get("journal")
        if isinstance(journal_obj, dict):
            journal = str(journal_obj.get("title") or "").strip()

    pmid = str(item.get("pmid") or "").strip()
    pmcid = str(item.get("pmcid") or "").strip()
    if doi:
        url = f"https://doi.org/{doi}"
    elif pmcid:
        url = f"https://europepmc.org/article/PMC/{pmcid}"
    elif pmid:
        url = f"https://europepmc.org/article/MED/{pmid}"
    else:
        url = ""

    try:
        year = int(item.get("pubYear")) if item.get("pubYear") else None
    except (TypeError, ValueError):
        year = None

    try:
        cited_by = int(item.get("citedByCount") or 0)
    except (TypeError, ValueError):
        cited_by = 0

    return {
        "title": title,
        # `authorString` is already a display-ready comma-joined list.
        "authors": str(item.get("authorString") or "").strip().rstrip("."),
        "year": year,
        # Never fabricate a date: Europe PMC gives a real one or nothing.
        "publication_date": str(item.get("firstPublicationDate") or "").strip() or None,
        "journal": journal,
        "doi": doi,
        "pmid": pmid,
        "pmcid": pmcid,
        "url": url,
        "cited_by_count": cited_by,
        "abstract": str(item.get("abstractText") or "").strip(),
        "is_open_access": str(item.get("isOpenAccess") or "").strip().upper() == "Y",
        "score": round(float(score), 4),
        "source_api": SOURCE_API,
    }


def search_works(
    query: str,
    *,
    limit: int = 20,
    from_year: int | None = None,
    open_access_only: bool = False,
) -> list[dict]:
    """Search Europe PMC by free-text query, newest-relevant first.

    ``from_year`` and ``open_access_only`` are pushed into the query string
    server-side rather than filtered locally, so the page we pay for is already
    the page we want.
    """
    query = (query or "").strip()
    if not query:
        return []

    terms = [query]
    if from_year:
        try:
            terms.append(f"(FIRST_PDATE:[{int(from_year)}-01-01 TO 3000-12-31])")
        except (TypeError, ValueError):
            pass
    if open_access_only:
        terms.append("(OPEN_ACCESS:y)")

    params: dict[str, Any] = {
        "query": " AND ".join(terms),
        "format": "json",
        "resultType": _RESULT_TYPE,
        "pageSize": min(max(int(limit), 1), _MAX_PAGE_SIZE),
    }

    try:
        resp = get_source_http_client("europe_pmc").get("/search", params=params, timeout=20)
        if resp.status_code != 200:
            logger.warning(
                "Europe PMC search returned HTTP %d for query %r",
                resp.status_code,
                query[:80],
            )
            return []
        payload = resp.json() or {}
    except Exception as exc:
        logger.warning("Europe PMC search failed for %r: %s", query[:80], exc)
        return []

    rows = ((payload.get("resultList") or {}).get("result")) or []
    if not isinstance(rows, list):
        return []

    out: list[dict] = []
    total = max(len(rows), 1)
    for idx, item in enumerate(rows):
        if not isinstance(item, dict):
            continue
        candidate = _europe_pmc_to_candidate(item, rank_score(idx, total))
        if candidate:
            out.append(candidate)
    return out[: max(1, int(limit))]
