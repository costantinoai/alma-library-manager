"""Identifier resolution helpers for external author services.

This module resolves Google Scholar IDs without scraping by querying:
- Semantic Scholar Graph API (author search bridge)
- ORCID Public API (researcher links)

Scraping-based resolution is handled elsewhere and should only be used
as an explicit fallback.
"""

from __future__ import annotations

from difflib import SequenceMatcher
import logging
import re
from typing import Any, Dict, Iterable, List, Optional
from urllib.parse import parse_qs, urlparse

from alma.discovery.semantic_scholar import fetch_authors_batch
from alma.core.http_sources import get_source_http_client
from alma.core.utils import normalize_orcid, normalize_text as _normalize_text  # noqa: F401  (normalize_orcid re-exported for back-compat)

logger = logging.getLogger(__name__)

_SCHOLAR_ID_RE = re.compile(r"^[A-Za-z0-9_-]{8,20}$")


def scholar_url_for_id(scholar_id: str | None) -> str | None:
    sid = (scholar_id or "").strip()
    if not sid:
        return None
    return f"https://scholar.google.com/citations?user={sid}"


def extract_scholar_id(value: str | None) -> str | None:
    """Extract a Google Scholar profile ID from raw value or URL."""
    raw = (value or "").strip()
    if not raw:
        return None

    # Direct candidate (ID-like token)
    if _SCHOLAR_ID_RE.match(raw):
        return raw

    try:
        parsed = urlparse(raw)
        if parsed.netloc and "scholar.google." in parsed.netloc.lower():
            query = parse_qs(parsed.query or "")
            user_values = query.get("user") or []
            if user_values:
                sid = (user_values[0] or "").strip()
                if _SCHOLAR_ID_RE.match(sid):
                    return sid
    except Exception:
        pass

    # Query-fragment fallback
    m = re.search(r"(?:[?&]user=)([A-Za-z0-9_-]{8,20})", raw)
    if m:
        sid = m.group(1)
        if _SCHOLAR_ID_RE.match(sid):
            return sid

    return None


def _title_overlap(sample_titles: list[str], candidate_titles: Iterable[str]) -> int:
    sample_norm = [_normalize_text(t) for t in sample_titles if _normalize_text(t)]
    if not sample_norm:
        return 0
    cand_norm = [_normalize_text(t) for t in candidate_titles if _normalize_text(t)]
    if not cand_norm:
        return 0
    hits = 0
    for t in sample_norm:
        if any((t == ct) or (t in ct) or (ct in t) for ct in cand_norm):
            hits += 1
    return hits


def _extract_external_ids(payload: Any) -> dict[str, Any]:
    if isinstance(payload, dict):
        return payload
    if isinstance(payload, list):
        out: dict[str, Any] = {}
        for item in payload:
            if not isinstance(item, dict):
                continue
            key = str(item.get("source") or item.get("type") or item.get("name") or "").strip()
            val = item.get("id") or item.get("value") or item.get("url")
            if key and val:
                out[key] = val
        return out
    return {}


def _extract_scholar_from_external_ids(external_ids: dict[str, Any]) -> tuple[str | None, str | None]:
    for key, value in (external_ids or {}).items():
        key_norm = (str(key) or "").strip().lower()
        if "scholar" not in key_norm:
            continue

        values: list[str] = []
        if isinstance(value, str):
            values = [value]
        elif isinstance(value, list):
            values = [str(v) for v in value if isinstance(v, (str, int, float))]
        elif isinstance(value, dict):
            values = [str(v) for v in value.values() if isinstance(v, (str, int, float))]

        for raw in values:
            sid = extract_scholar_id(raw)
            if sid:
                return sid, scholar_url_for_id(sid)
    return None, None


def _extract_openalex_from_external_ids(external_ids: dict[str, Any]) -> str | None:
    for key, value in (external_ids or {}).items():
        key_norm = (str(key) or "").strip().lower()
        if "openalex" not in key_norm:
            continue
        raw = str(value or "").strip()
        if not raw:
            continue
        if raw.startswith("http"):
            raw = raw.rstrip("/").split("/")[-1]
        if raw:
            return raw
    return None


def _extract_orcid_from_external_ids(external_ids: dict[str, Any]) -> str | None:
    for key, value in (external_ids or {}).items():
        key_norm = (str(key) or "").strip().lower()
        if "orcid" not in key_norm:
            continue
        normalized = normalize_orcid(str(value or ""))
        if normalized:
            return normalized
    return None


def _fetch_semantic_paper_titles(author_id: str, timeout_seconds: float = 12.0) -> list[str]:
    if not (author_id or "").strip():
        return []
    try:
        r = get_source_http_client("semantic_scholar").get(
            f"/author/{author_id}/papers",
            params={"limit": 20, "fields": "title"},
            timeout=timeout_seconds,
        )
        r.raise_for_status()
        payload = r.json() or {}
        items = payload.get("data") or []
        return [(item.get("title") or "").strip() for item in items if (item.get("title") or "").strip()]
    except Exception as exc:
        logger.debug("Semantic Scholar papers lookup failed for %s: %s", author_id, exc)
        return []


def resolve_scholar_candidates_from_semantic_scholar(
    author_name: str,
    *,
    openalex_id: str | None = None,
    orcid: str | None = None,
    sample_titles: list[str] | None = None,
    timeout_seconds: float = 12.0,
    limit: int = 8,
) -> list[dict[str, object]]:
    """Resolve Scholar candidates from Semantic Scholar author search."""
    name = (author_name or "").strip()
    if not name:
        return []

    sample_titles = [t.strip() for t in (sample_titles or []) if (t or "").strip()][:6]
    normalized_openalex = (openalex_id or "").strip()
    if normalized_openalex.startswith("http"):
        normalized_openalex = normalized_openalex.rstrip("/").split("/")[-1]
    normalized_orcid = normalize_orcid(orcid)

    params = {
        "query": name,
        "limit": max(1, min(int(limit), 20)),
        "fields": ",".join(
            [
                "name",
                "authorId",
                "aliases",
                "affiliations",
                "homepage",
                "url",
                "externalIds",
                "paperCount",
                "citationCount",
                "hIndex",
            ]
        ),
    }

    try:
        response = get_source_http_client("semantic_scholar").get(
            "/author/search",
            params=params,
            timeout=timeout_seconds,
        )
        response.raise_for_status()
        rows = (response.json() or {}).get("data") or []
    except Exception as exc:
        logger.debug("Semantic Scholar author search failed for '%s': %s", name, exc)
        return []

    detail_by_author_id = fetch_authors_batch(
        [
            str((row or {}).get("authorId") or "").strip()
            for row in rows
        ],
        fields="authorId,name,aliases,affiliations,homepage,url,externalIds,paperCount,citationCount,hIndex",
    )

    preliminary: list[dict[str, object]] = []
    for row in rows:
        semantic_author_id = str(row.get("authorId") or "").strip()
        detail_row = detail_by_author_id.get(semantic_author_id) or {}
        merged_row = dict(row)
        merged_row.update({k: v for k, v in detail_row.items() if v not in (None, "", [], {})})

        cand_name = (merged_row.get("name") or "").strip()
        if not cand_name:
            continue

        ext_ids = _extract_external_ids(merged_row.get("externalIds"))
        scholar_id, scholar_url = _extract_scholar_from_external_ids(ext_ids)
        if not scholar_id:
            scholar_id = extract_scholar_id((merged_row.get("homepage") or "").strip())
        if not scholar_id:
            scholar_id = extract_scholar_id((merged_row.get("url") or "").strip())
        if not scholar_id:
            continue

        score = SequenceMatcher(None, _normalize_text(name), _normalize_text(cand_name)).ratio() * 6.0

        ext_openalex = _extract_openalex_from_external_ids(ext_ids)
        if normalized_openalex and ext_openalex and ext_openalex.lower() == normalized_openalex.lower():
            score += 4.0

        ext_orcid = _extract_orcid_from_external_ids(ext_ids)
        if normalized_orcid and ext_orcid and ext_orcid == normalized_orcid:
            score += 4.0

        affs = merged_row.get("affiliations") or []
        primary_aff = ""
        if isinstance(affs, list) and affs:
            first = affs[0]
            if isinstance(first, str):
                primary_aff = first.strip()

        preliminary.append(
            {
                "scholar_id": scholar_id,
                "display_name": cand_name,
                "affiliation": primary_aff,
                "score": round(score, 3),
                "source": "semantic_scholar",
                "scholar_url": scholar_url or scholar_url_for_id(scholar_id),
                "title_overlap": 0,
                "_semantic_author_id": semantic_author_id,
            }
        )

    preliminary.sort(key=lambda c: float(c.get("score") or 0.0), reverse=True)
    if sample_titles:
        for cand in preliminary[:3]:
            cand_author_id = str(cand.get("_semantic_author_id") or "").strip()
            if not cand_author_id:
                continue
            paper_titles = _fetch_semantic_paper_titles(cand_author_id, timeout_seconds=timeout_seconds)
            overlap = _title_overlap(sample_titles, paper_titles)
            cand["title_overlap"] = overlap
            cand["score"] = round(float(cand.get("score") or 0.0) + min(3.0, overlap * 1.5), 3)

    # De-duplicate by scholar_id (keep best score)
    best: dict[str, dict[str, object]] = {}
    for cand in preliminary:
        sid = str(cand.get("scholar_id") or "").strip()
        if not sid:
            continue
        current = best.get(sid)
        cleaned = dict(cand)
        cleaned.pop("_semantic_author_id", None)
        if current is None or float(cleaned.get("score") or 0.0) > float(current.get("score") or 0.0):
            best[sid] = cleaned

    out = list(best.values())
    out.sort(key=lambda c: float(c.get("score") or 0.0), reverse=True)
    return out[:8]


def _extract_orcid_urls(payload: dict[str, Any]) -> list[str]:
    urls: list[str] = []

    # /person response
    researcher_urls = ((payload.get("researcher-urls") or {}).get("researcher-url") or [])
    if isinstance(researcher_urls, list):
        for entry in researcher_urls:
            if not isinstance(entry, dict):
                continue
            value = ((entry.get("url") or {}).get("value") or "").strip()
            if value:
                urls.append(value)

    # /researcher-urls response
    if not urls and isinstance(payload.get("researcher-url"), list):
        for entry in payload.get("researcher-url") or []:
            if not isinstance(entry, dict):
                continue
            value = ((entry.get("url") or {}).get("value") or "").strip()
            if value:
                urls.append(value)

    return urls


def resolve_scholar_candidates_from_orcid(
    orcid: str,
    *,
    timeout_seconds: float = 12.0,
) -> list[dict[str, object]]:
    """Resolve Scholar IDs from ORCID public researcher links."""
    normalized = normalize_orcid(orcid)
    if not normalized:
        return []

    headers = {"Accept": "application/json"}
    payloads: list[dict[str, Any]] = []

    for suffix in ("person", "researcher-urls"):
        try:
            r = get_source_http_client("orcid").get(
                f"/{normalized}/{suffix}",
                headers=headers,
                timeout=timeout_seconds,
            )
            if r.status_code == 200:
                payloads.append(r.json() or {})
        except Exception as exc:
            logger.debug("ORCID %s lookup failed for %s: %s", suffix, normalized, exc)

    candidates: list[dict[str, object]] = []
    seen: set[str] = set()
    for payload in payloads:
        for url in _extract_orcid_urls(payload):
            sid = extract_scholar_id(url)
            if not sid or sid in seen:
                continue
            seen.add(sid)
            candidates.append(
                {
                    "scholar_id": sid,
                    "display_name": "",
                    "affiliation": "",
                    "score": 9.0,
                    "source": "orcid",
                    "scholar_url": scholar_url_for_id(sid),
                    "title_overlap": 0,
                }
            )

    return candidates


def resolve_scholar_candidates_from_sources(
    author_name: str,
    *,
    openalex_id: str | None = None,
    orcid: str | None = None,
    sample_titles: list[str] | None = None,
    use_semantic_scholar: bool = True,
    use_orcid: bool = True,
) -> list[dict[str, object]]:
    """Resolve and merge Scholar candidates from API-based providers."""
    merged: list[dict[str, object]] = []
    if use_semantic_scholar:
        merged.extend(
            resolve_scholar_candidates_from_semantic_scholar(
                author_name,
                openalex_id=openalex_id,
                orcid=orcid,
                sample_titles=sample_titles,
            )
        )
    if use_orcid and (orcid or "").strip():
        merged.extend(resolve_scholar_candidates_from_orcid(orcid or ""))

    best: dict[str, dict[str, object]] = {}
    for cand in merged:
        sid = str(cand.get("scholar_id") or "").strip()
        if not sid:
            continue
        current = best.get(sid)
        if current is None:
            best[sid] = cand
            continue

        current_score = float(current.get("score") or 0.0)
        cand_score = float(cand.get("score") or 0.0)
        if cand_score > current_score:
            updated = dict(cand)
            source_set = {
                str(current.get("source") or "").strip(),
                str(cand.get("source") or "").strip(),
            }
            updated["source"] = ",".join(sorted(s for s in source_set if s))
            best[sid] = updated
        else:
            source_set = {
                str(current.get("source") or "").strip(),
                str(cand.get("source") or "").strip(),
            }
            current["source"] = ",".join(sorted(s for s in source_set if s))

    out = list(best.values())
    out.sort(key=lambda c: float(c.get("score") or 0.0), reverse=True)
    return out[:8]
