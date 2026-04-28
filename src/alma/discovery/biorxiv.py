"""bioRxiv discovery source adapter."""

from __future__ import annotations

from datetime import datetime
import logging
import re
from typing import List, Optional

from alma.core.http_sources import get_source_http_client
from alma.core.utils import normalize_doi

logger = logging.getLogger(__name__)

_STOPWORDS = {
    "the",
    "and",
    "for",
    "with",
    "from",
    "into",
    "this",
    "that",
    "using",
    "study",
    "analysis",
    "based",
}


def _year_from_date(value: str) -> Optional[int]:
    raw = (value or "").strip()
    if not raw:
        return None
    try:
        return datetime.fromisoformat(raw.replace("Z", "+00:00")).year
    except Exception:
        try:
            return int(raw[:4])
        except Exception:
            return None


def _query_terms(query: str) -> list[str]:
    parts = re.findall(r"[a-z0-9]+", (query or "").lower())
    return [part for part in parts if len(part) >= 3 and part not in _STOPWORDS]


def _lexical_score(query: str, item: dict) -> float:
    terms = _query_terms(query)
    if not terms:
        return 0.0
    haystack = " ".join(
        [
            str(item.get("title") or ""),
            str(item.get("abstract") or ""),
            str(item.get("category") or ""),
        ]
    ).lower()
    matches = sum(1 for term in terms if term in haystack)
    if matches <= 0:
        return 0.0
    return min(1.0, matches / max(2, len(terms)))


def _entry_to_candidate(entry: dict, score: float, *, server: str) -> Optional[dict]:
    title = (entry.get("title") or "").strip()
    if not title:
        return None

    preprint_doi_raw = (entry.get("doi") or "").strip()
    preprint_doi = normalize_doi(preprint_doi_raw) or preprint_doi_raw
    published_doi_raw = (entry.get("published") or "").strip()
    published_doi = normalize_doi(published_doi_raw) or published_doi_raw
    if published_doi.upper() in {"NA", "N/A"}:
        published_doi = ""

    version = (entry.get("version") or "").strip() or "1"
    url = ""
    if preprint_doi:
        url = f"https://www.biorxiv.org/content/{preprint_doi}v{version}"

    year = _year_from_date(str(entry.get("date") or ""))
    server_label = "bioRxiv" if server == "biorxiv" else "medRxiv"

    candidate = {
        "title": title,
        "authors": (entry.get("authors") or "").strip(),
        "year": year,
        "publication_date": str(entry.get("date") or "").strip() or None,
        "journal": server_label,
        "doi": preprint_doi,
        "preprint_doi": preprint_doi,
        "published_doi": published_doi or None,
        "canonical_doi": published_doi or preprint_doi or None,
        "url": url,
        "cited_by_count": 0,
        "abstract": (entry.get("abstract") or "").strip(),
        "score": round(float(score), 4),
        "source_api": server,
        "preprint_source": server,
        "source_type": "preprint_lane",
        "category": (entry.get("category") or "").strip(),
    }
    return candidate


def _extract_published_record(entry: dict) -> dict[str, str | None]:
    published_doi = normalize_doi(
        str(
            entry.get("published_doi")
            or entry.get("published")
            or entry.get("publishedArticleDoi")
            or ""
        ).strip()
    )
    journal = (
        str(
            entry.get("published_journal")
            or entry.get("publishedJournal")
            or entry.get("journal")
            or ""
        ).strip()
        or None
    )
    published_date = (
        str(
            entry.get("published_date")
            or entry.get("publishedDate")
            or entry.get("publication_date")
            or entry.get("date")
            or ""
        ).strip()
        or None
    )
    return {
        "published_doi": published_doi or None,
        "published_journal": journal,
        "published_date": published_date,
    }


def _lookup_published_record(preprint_doi: str, *, server: str) -> dict[str, str | None]:
    normalized = normalize_doi(preprint_doi)
    if not normalized:
        return {}
    try:
        resp = get_source_http_client("biorxiv").get(
            f"/pubs/{server}/{normalized}/na/json",
            timeout=20,
        )
        if resp.status_code != 200:
            return {}
        entries = (resp.json() or {}).get("collection") or []
        for entry in entries:
            if not isinstance(entry, dict):
                continue
            parsed = _extract_published_record(entry)
            if parsed.get("published_doi") or parsed.get("published_journal"):
                return parsed
    except Exception as exc:
        logger.debug("bioRxiv pubs lookup failed for %s: %s", normalized, exc)
    return {}


def reconcile_published_versions(
    candidates: list[dict],
    *,
    server: str = "biorxiv",
    limit: int = 5,
) -> list[dict]:
    """Enrich top preprints with authoritative published-version metadata."""
    if not candidates:
        return candidates
    out: list[dict] = []
    remaining = max(0, int(limit))
    for candidate in candidates:
        enriched = dict(candidate)
        if remaining > 0 and enriched.get("preprint_doi"):
            published = _lookup_published_record(str(enriched.get("preprint_doi") or ""), server=server)
            if published:
                published_doi = normalize_doi(str(published.get("published_doi") or "").strip())
                if published_doi:
                    enriched["published_doi"] = published_doi
                    enriched["canonical_doi"] = published_doi
                published_journal = published.get("published_journal")
                if published_journal:
                    enriched["published_journal"] = published_journal
                published_date = published.get("published_date")
                if published_date:
                    enriched["published_date"] = published_date
                    enriched["publication_date"] = published_date
            remaining -= 1
        out.append(enriched)
    return out


def search_works(
    query: str,
    *,
    limit: int = 20,
    from_year: Optional[int] = None,
    server: str = "biorxiv",
) -> List[dict]:
    """Search recent bioRxiv entries by local lexical reranking."""
    query = (query or "").strip()
    if not query:
        return []

    now = datetime.utcnow()
    if from_year and from_year >= now.year:
        interval = "30d"
    elif from_year and from_year >= (now.year - 1):
        interval = "90d"
    else:
        interval = "180d"

    out: list[tuple[float, dict]] = []
    seen_keys: set[str] = set()
    client = get_source_http_client("biorxiv")
    for cursor in (0, 100):
        try:
            resp = client.get(f"/details/{server}/{interval}/{cursor}/json", timeout=20)
            if resp.status_code != 200:
                logger.debug("bioRxiv search returned HTTP %d", resp.status_code)
                break
            payload = resp.json() or {}
            entries = payload.get("collection") or []
            if not entries:
                break
            for entry in entries:
                if not isinstance(entry, dict):
                    continue
                score = _lexical_score(query, entry)
                if score <= 0.0:
                    continue
                candidate = _entry_to_candidate(entry, score, server=server)
                if not candidate:
                    continue
                year = candidate.get("year")
                if from_year and isinstance(year, int) and year < from_year:
                    continue
                dedupe_key = str(candidate.get("canonical_doi") or candidate.get("doi") or candidate.get("url") or "")
                if dedupe_key in seen_keys:
                    continue
                seen_keys.add(dedupe_key)
                out.append((score, candidate))
            if len(out) >= max(limit * 2, 20):
                break
            if len(entries) < 100:
                break
        except Exception as exc:
            logger.warning("bioRxiv query search failed: %s", exc)
            return []

    out.sort(key=lambda item: item[0], reverse=True)
    top = [candidate for _, candidate in out[: max(1, limit)]]
    return reconcile_published_versions(top, server=server, limit=min(5, len(top)))
