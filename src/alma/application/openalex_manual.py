"""Manual OpenAlex search and ingest helpers for discovery/import UX."""

from __future__ import annotations

import logging
import re
import sqlite3
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from typing import Optional
from urllib.parse import parse_qs, urlparse

logger = logging.getLogger(__name__)

from alma.application import library as library_app
from alma.application.feed import _upsert_candidate_paper
from alma.core.utils import is_doi_shaped, normalize_doi, resolve_existing_paper_id
from alma.discovery import similarity as sim_module
from alma.discovery.source_search import (
    FINDADD_LANE_DEADLINE_S,
    merge_streamed_results,
    stream_across_sources,
)
from alma.discovery.engine import (
    build_preference_profile,
    get_library_papers,
    get_rated_publications,
    load_settings,
    publication_text,
    score_discovery_candidate,
)
from alma.openalex.client import (
    _WORKS_SELECT_FIELDS,
    _ensure_schema,
    _normalize_openalex_work_id,
    _normalize_work,
    _upsert_single_paper,
)
from alma.openalex.http import get_client

_OA_WORK_RE = re.compile(r"(?:https?://)?(?:www\.)?openalex\.org/(W\d+)", re.IGNORECASE)
_DOI_RE = re.compile(r"(10\.\d{4,9}/[-._;()/:A-Z0-9]+)", re.IGNORECASE)
_ARXIV_RE = re.compile(r"(?:arxiv(?:\.org)?(?:/abs|/pdf)?/?)?(\d{4}\.\d{4,5})(?:v\d+)?", re.IGNORECASE)


def _extract_openalex_work_id(text: str) -> Optional[str]:
    raw = (text or "").strip()
    if not raw:
        return None
    m = _OA_WORK_RE.search(raw)
    if m:
        return m.group(1).upper()
    if raw.upper().startswith("W") and raw[1:].isdigit():
        return raw.upper()
    return None


def _extract_doi(text: str) -> Optional[str]:
    raw = (text or "").strip()
    if not raw:
        return None

    parsed = None
    try:
        parsed = urlparse(raw)
    except Exception:
        parsed = None

    # Every branch gates on `is_doi_shaped`: `normalize_doi` is a cleaner,
    # not a validator, so without the shape check any non-empty string
    # ("title:…", "?id=42") would be returned as a "DOI" and cost a
    # guaranteed-404 `/works/doi:` round-trip downstream.
    if parsed and parsed.scheme and parsed.netloc:
        host = parsed.netloc.lower()
        path = parsed.path or ""
        if host.endswith("doi.org") and path and is_doi_shaped(path.lstrip("/")):
            return normalize_doi(path.lstrip("/"))
        qs = parse_qs(parsed.query or "")
        for key in ("doi", "article_doi", "id"):
            vals = qs.get(key) or []
            for v in vals:
                if is_doi_shaped(v):
                    return normalize_doi(v)

    if is_doi_shaped(raw):
        return normalize_doi(raw)
    m = _DOI_RE.search(raw)
    if m:
        return normalize_doi(m.group(1))
    return None


def _extract_arxiv_doi(text: str) -> Optional[str]:
    raw = (text or "").strip()
    if not raw:
        return None
    m = _ARXIV_RE.search(raw)
    if not m:
        return None
    arxiv_id = m.group(1)
    if not arxiv_id:
        return None
    return f"10.48550/arXiv.{arxiv_id}"


def _seed_sibling_resolve_cache(work: dict, resp) -> None:
    """Make a resolved work's response servable by BOTH its identifiers.

    The connector popup's ``/lookup`` preview resolves a PDF by DOI, learns
    the openalex id from the result, and the follow-up ``/save`` then
    resolves by THAT id — a different URL, so a guaranteed http-cache miss
    for a work fetched seconds earlier. Seeding the same response under the
    sibling identifier's URL turns the save's resolve into a cache hit (no
    second upstream round-trip). Honors the client cache's normal TTL and
    eviction; best-effort, never blocks the resolve itself.
    """
    try:
        client = get_client()
        params = {"select": _WORKS_SELECT_FIELDS}
        wid = str(work.get("id") or "").strip().rstrip("/").split("/")[-1].upper()
        if wid.startswith("W") and wid[1:].isdigit():
            client.seed_cache(f"/works/{wid}", params, resp)
        doi = normalize_doi(str(work.get("doi") or ""))
        if doi:
            client.seed_cache(f"/works/doi:{doi}", params, resp)
    except Exception as exc:  # pragma: no cover - cache seeding is best-effort
        logger.debug("Sibling resolve-cache seed failed: %s", exc)


def _fetch_work_by_openalex_id(openalex_work_id: str) -> Optional[dict]:
    # Canonicalize to the bare `W…` form: callers hold the id in whatever
    # shape they learned it (the /lookup preview returns the full
    # `https://openalex.org/W…` URL), and the canonical path is also what
    # `_seed_sibling_resolve_cache` keys, so a preview-resolved work is a
    # guaranteed cache hit here.
    wid = _normalize_openalex_work_id((openalex_work_id or "").strip()).upper()
    if not wid:
        return None
    client = get_client()
    resp = client.get(
        f"/works/{wid}",
        params={"select": _WORKS_SELECT_FIELDS},
        timeout=20,
    )
    if resp.status_code != 200:
        # Loud-on-error: a 404 means OpenAlex doesn't have this id, which
        # is information the user wants. A 5xx / 429 means the API is
        # struggling and "no result" would otherwise look indistinguishable
        # from "no match".
        logger.warning(
            "OpenAlex /works/%s returned HTTP %s", wid, resp.status_code
        )
        return None
    data = resp.json() or {}
    if not data.get("display_name"):
        return None
    _seed_sibling_resolve_cache(data, resp)
    return data


def _fetch_work_by_doi(doi: str) -> Optional[dict]:
    clean = normalize_doi(doi or "")
    if not clean:
        return None
    client = get_client()
    resp = client.get(
        f"/works/doi:{clean}",
        params={"select": _WORKS_SELECT_FIELDS},
        timeout=20,
    )
    if resp.status_code != 200:
        logger.warning(
            "OpenAlex /works/doi:%s returned HTTP %s", clean, resp.status_code
        )
        return None
    data = resp.json() or {}
    if not data.get("display_name"):
        return None
    _seed_sibling_resolve_cache(data, resp)
    return data


def search_authors_online(
    db: sqlite3.Connection,
    query: str,
    *,
    limit: int = 10,
) -> list[dict]:
    """Search OpenAlex /authors and shape rows for the Find & Add author rail.

    Used when the user prefixes the query with ``author:`` (or otherwise
    asks for author scope). Returns lightweight cards — we resolve the
    full author dossier only on follow. Each row carries
    ``already_followed`` so the UI can render Following/Follow without a
    second round-trip.
    """
    raw = (query or "").strip()
    if raw.lower().startswith("author:"):
        raw = raw.split(":", 1)[1].strip()
    if not raw:
        return []

    client = get_client()
    per_page = max(1, min(int(limit or 10), 25))
    resp = client.get(
        "/authors",
        params={
            "search": raw,
            "per-page": per_page,
            "select": ",".join([
                "id", "display_name", "orcid",
                "last_known_institutions",
                "works_count", "cited_by_count", "summary_stats",
                "topics",
            ]),
        },
        timeout=20,
    )
    if resp.status_code != 200:
        logger.warning(
            "OpenAlex /authors search %r returned HTTP %s — UI will show empty results",
            raw,
            resp.status_code,
        )
        return []
    results = (resp.json() or {}).get("results") or []

    out: list[dict] = []
    for item in results:
        oid = str(item.get("id") or "").strip().rstrip("/").split("/")[-1]
        if not oid:
            continue
        stats = item.get("summary_stats") or {}
        institution = ""
        lki = item.get("last_known_institutions") or []
        if isinstance(lki, list) and lki and isinstance(lki[0], dict):
            institution = (lki[0].get("display_name") or "").strip()
        topics = []
        for t in (item.get("topics") or [])[:4]:
            if isinstance(t, dict) and t.get("display_name"):
                topics.append(str(t.get("display_name") or "").strip())
        out.append({
            "openalex_id": oid,
            "name": str(item.get("display_name") or "").strip() or oid,
            "orcid": str(item.get("orcid") or "").strip() or None,
            "institution": institution or None,
            "works_count": int(item.get("works_count") or 0),
            "cited_by_count": int(item.get("cited_by_count") or 0),
            "h_index": int(stats.get("h_index") or 0),
            "i10_index": int(stats.get("i10_index") or 0),
            "top_topics": topics,
            "already_followed": False,
            "existing_author_id": None,
            "existing_author_type": None,
            "top_cited_titles": [],
        })

    # Integrate the search with the authors library: resolve each candidate
    # against local rows + the followed-identity union (direct id / merged
    # alt / ORCID) — the SAME dedup the suggestion rail applies. This makes
    # `already_followed` honest even across split/merged profiles and exposes
    # `existing_author_id` so a clicked card can open the full local detail.
    try:
        from alma.application.authors import (
            annotate_external_authors_with_local_identity,
        )

        annotations = annotate_external_authors_with_local_identity(db, out)
        for row in out:
            ann = annotations.get(str(row["openalex_id"]).strip().lower())
            if ann:
                row["already_followed"] = ann["already_followed"]
                row["existing_author_id"] = ann["existing_author_id"]
                row["existing_author_type"] = ann["existing_author_type"]
    except Exception as exc:  # pragma: no cover - annotation is best-effort
        logger.warning("Author search local-identity annotation failed: %s", exc)

    # NB: the two most-cited papers per author are fetched SEPARATELY (one
    # extra OpenAlex /works call per author) by `fetch_top_cited_titles` /
    # the `/import/search/authors/top-works` endpoint, so they never gate the
    # author list's time-to-display. OpenAlex's /authors search is already
    # 1–6 s; folding the per-author work calls in here would roughly double
    # that. The frontend renders cards immediately, then fills titles in.
    return out


def fetch_top_cited_titles_by_author(
    openalex_ids: list[str],
    *,
    per_author: int = 2,
) -> dict[str, list[str]]:
    """Titles of each author's top-N most-cited works, keyed by lower openalex id.

    One small citation-sorted ``/works`` list request per author, run
    concurrently (the shared OpenAlex client is safe for concurrent GETs —
    same pattern as the discovery lens lanes). Best-effort: any author whose
    call fails or times out simply gets an empty list.

    Called from its own endpoint (not from `search_authors_online`) so the
    per-author work calls never gate the author list's time-to-display.
    """
    ids = [i for i in dict.fromkeys(openalex_ids or []) if i]
    if not ids:
        return {}
    client = get_client()
    n = max(1, per_author)

    def _one(author_oid: str) -> tuple[str, list[str]]:
        try:
            resp = client.get(
                "/works",
                params={
                    "filter": f"authorships.author.id:{author_oid}",
                    "sort": "cited_by_count:desc",
                    "per-page": n,
                    "select": "title,cited_by_count",
                },
                timeout=10,
            )
            if resp.status_code != 200:
                return author_oid.lower(), []
            works = (resp.json() or {}).get("results") or []
            titles: list[str] = []
            for work in works:
                title = str(work.get("title") or "").strip()
                if title:
                    titles.append(title)
                if len(titles) >= n:
                    break
            return author_oid.lower(), titles
        except Exception:
            return author_oid.lower(), []

    out: dict[str, list[str]] = {}
    with ThreadPoolExecutor(
        max_workers=min(6, len(ids)), thread_name_prefix="author-works"
    ) as pool:
        for oid, titles in pool.map(_one, ids):
            out[oid] = titles
    return out


def _search_works_raw(query: str, *, limit: int = 20) -> list[dict]:
    q = (query or "").strip()
    if not q:
        return []

    client = get_client()
    per_page = max(1, min(int(limit or 20), 100))

    if q.lower().startswith("author:"):
        author_name = q.split(":", 1)[1].strip()
        if not author_name:
            return []
        author_resp = client.get(
            "/authors",
            params={"search": author_name, "per-page": 5, "select": "id,display_name"},
            timeout=20,
        )
        if author_resp.status_code != 200:
            logger.warning(
                "OpenAlex /authors lookup for %r returned HTTP %s — works search aborted",
                author_name,
                author_resp.status_code,
            )
            return []
        authors = (author_resp.json() or {}).get("results") or []
        author_ids = [
            str(a.get("id") or "").rstrip("/").split("/")[-1]
            for a in authors
            if str(a.get("id") or "").strip()
        ][:3]
        if not author_ids:
            return []
        items: list[dict] = []
        seen: set[str] = set()
        each_limit = max(3, per_page // max(1, len(author_ids)))
        for aid in author_ids:
            resp = client.get(
                "/works",
                params={
                    "filter": f"author.id:{aid}",
                    "per-page": each_limit,
                    "sort": "cited_by_count:desc",
                    "select": _WORKS_SELECT_FIELDS,
                },
                timeout=30,
            )
            if resp.status_code != 200:
                logger.warning(
                    "OpenAlex /works filter author.id:%s returned HTTP %s — skipping this author",
                    aid,
                    resp.status_code,
                )
                continue
            for work in (resp.json() or {}).get("results") or []:
                wid = str(work.get("id") or "").strip()
                if not wid or wid in seen:
                    continue
                seen.add(wid)
                items.append(work)
                if len(items) >= per_page:
                    return items
        return items

    search_q = q.split(":", 1)[1].strip() if q.lower().startswith("title:") else q
    if not search_q:
        return []
    resp = client.get(
        "/works",
        params={
            "search": search_q,
            "per-page": per_page,
            "select": _WORKS_SELECT_FIELDS,
        },
        timeout=30,
    )
    if resp.status_code != 200:
        logger.warning(
            "OpenAlex /works search %r returned HTTP %s — UI will show empty results",
            search_q,
            resp.status_code,
        )
        return []
    return (resp.json() or {}).get("results") or []


def _resolve_raw_work_for_query(query: str) -> tuple[Optional[dict], str]:
    q = (query or "").strip()
    if not q:
        return None, "empty"

    oaid = _extract_openalex_work_id(q)
    if oaid:
        work = _fetch_work_by_openalex_id(oaid)
        if work:
            return work, "openalex_id"

    doi = _extract_doi(q)
    if doi:
        work = _fetch_work_by_doi(doi)
        if work:
            return work, "doi"

    arxiv_doi = _extract_arxiv_doi(q)
    if arxiv_doi:
        work = _fetch_work_by_doi(arxiv_doi)
        if work:
            return work, "arxiv"

    # URL-shaped input that yielded no identifier above (OpenAlex id / DOI /
    # arXiv id) stops here: full-text-searching a raw URL string is the
    # slowest call on this path (a /works search with a 30s budget) and
    # `results[0]` of a URL query is more likely to be a WRONG paper than
    # the right one. Callers fall back to candidate metadata (connector
    # saves) or report not-found honestly.
    try:
        parsed = urlparse(q)
        if parsed.scheme in ("http", "https") and parsed.netloc:
            return None, "not_found"
    except Exception:
        pass

    results = _search_works_raw(q, limit=1)
    if results:
        return results[0], "search"
    return None, "not_found"


def _find_existing_paper(
    db: sqlite3.Connection,
    *,
    openalex_id: str,
    doi: str,
    title: str,
    year: Optional[int] = None,
) -> Optional[sqlite3.Row]:
    """Locate an existing paper row for search-result decoration.

    Tries the canonical triple (openalex_id → doi → year+normalized_title).
    When year is missing, falls back to a case-insensitive exact title
    match so decorated search results still surface duplicates for
    query-by-title flows.
    """
    paper_id = resolve_existing_paper_id(
        db,
        openalex_id=openalex_id,
        doi=doi,
        title=title,
        year=year,
    )
    if paper_id is None and year is None and title:
        row = db.execute(
            "SELECT id FROM papers WHERE lower(title) = lower(?) LIMIT 1",
            (title,),
        ).fetchone()
        if row:
            paper_id = row["id"] if isinstance(row, sqlite3.Row) else row[0]
    if paper_id is None:
        return None
    return db.execute(
        "SELECT id, status FROM papers WHERE id = ?",
        (paper_id,),
    ).fetchone()


def _decorate_result(db: sqlite3.Connection, normalized_work: dict) -> dict:
    openalex_id = str(normalized_work.get("openalex_id") or "").strip()
    doi = str(normalized_work.get("doi") or "").strip()
    title = str(normalized_work.get("title") or "").strip()
    year_raw = normalized_work.get("year")
    try:
        year = int(year_raw) if year_raw not in (None, "") else None
    except (TypeError, ValueError):
        year = None
    existing = _find_existing_paper(
        db,
        openalex_id=openalex_id,
        doi=doi,
        title=title,
        year=year,
    )
    return {
        "openalex_id": openalex_id,
        "title": title,
        "authors": str(normalized_work.get("authors") or "").strip(),
        "abstract": str(normalized_work.get("abstract") or "").strip(),
        "year": normalized_work.get("year"),
        "publication_date": normalized_work.get("publication_date"),
        "journal": str(normalized_work.get("journal") or "").strip(),
        "doi": doi,
        "url": str(normalized_work.get("pub_url") or "").strip(),
        "cited_by_count": int(normalized_work.get("num_citations") or 0),
        "paper_id": str((existing["id"] if existing else "") or "") or None,
        "paper_status": str((existing["status"] if existing else "") or "") or None,
        "in_library": bool(existing and str(existing["status"] or "") == "library"),
    }


def _build_personal_scorer(db: sqlite3.Connection):
    settings = load_settings(db)
    positive, negative = get_rated_publications(db)
    if not positive:
        positive = get_library_papers(db)
        negative = []

    preference_profile = build_preference_profile(
        db,
        positive,
        negative,
        settings,
    )
    positive_texts = [txt for txt in (publication_text(pub) for pub in positive) if txt]
    negative_texts = [txt for txt in (publication_text(pub) for pub in negative) if txt]

    positive_centroid = None
    negative_centroid = None
    if sim_module.has_active_embeddings(db):
        if positive:
            try:
                positive_centroid = sim_module.compute_embedding_centroid(positive, db)
            except Exception:
                positive_centroid = None
        if negative:
            try:
                negative_centroid = sim_module.compute_embedding_centroid(negative, db)
            except Exception:
                negative_centroid = None

    # Build the TF-IDF lexical profile ONCE per request — building it
    # per-candidate runs two `fit_transform` over the entire library on
    # every score call, which dominates request latency for users with
    # large libraries.
    lexical_profile = None
    try:
        lexical_profile = sim_module.build_lexical_profile(positive_texts, negative_texts)
    except Exception:
        lexical_profile = None

    return (
        settings,
        preference_profile,
        positive_centroid,
        negative_centroid,
        positive_texts,
        negative_texts,
        lexical_profile,
    )


def _score_search_result(
    *,
    db: sqlite3.Connection,
    settings: dict[str, str],
    preference_profile: dict,
    positive_centroid,
    negative_centroid,
    positive_texts: list[str],
    negative_texts: list[str],
    item: dict,
    source_relevance: float,
    source_key: str,
    lexical_profile=None,
    precomputed_lexical_details: Optional[dict] = None,
) -> tuple[float, dict]:
    candidate = {
        "title": item.get("title") or "",
        "authors": item.get("authors") or "",
        "abstract": item.get("abstract") or "",
        "year": item.get("year"),
        "journal": item.get("journal") or "",
        "cited_by_count": item.get("cited_by_count") or 0,
        "topics": item.get("topics") or [],
        "score": source_relevance,
        "source_type": "manual_search",
        "source_key": source_key,
    }
    try:
        from alma.discovery.scoring import score_candidate

        return score_candidate(
            candidate,
            preference_profile,
            positive_centroid,
            negative_centroid,
            positive_texts,
            negative_texts,
            db,
            settings,
            lexical_profile=lexical_profile,
            precomputed_lexical_details=precomputed_lexical_details,
        )
    except Exception:
        return 0.0, {}


def _decorate_candidate(db: sqlite3.Connection, candidate: dict) -> dict:
    """Attach library-state fields to a multi-source search candidate.

    Mirrors ``_decorate_result`` but accepts the candidate shape emitted
    by the multi-source merge (``rank_by_query_relevance`` /
    ``search_across_sources``), which carries ``pub_url``/``url``,
    ``num_citations``/``cited_by_count``, and an already-merged
    ``source_apis`` list from all sources that returned the paper.
    """
    openalex_id = str(candidate.get("openalex_id") or "").strip()
    doi = normalize_doi(str(candidate.get("doi") or "").strip()) or ""
    title = str(candidate.get("title") or "").strip()
    year_raw = candidate.get("year")
    try:
        year = int(year_raw) if year_raw not in (None, "") else None
    except (TypeError, ValueError):
        year = None
    existing = _find_existing_paper(
        db,
        openalex_id=openalex_id,
        doi=doi,
        title=title,
        year=year,
    )
    url = str(
        candidate.get("url") or candidate.get("pub_url") or ""
    ).strip()
    cited = candidate.get("cited_by_count")
    if cited in (None, ""):
        cited = candidate.get("num_citations") or 0
    sources = candidate.get("source_apis")
    if not isinstance(sources, list) or not sources:
        primary = str(candidate.get("source_api") or "").strip()
        sources = [primary] if primary else []
    return {
        "openalex_id": openalex_id,
        "title": title,
        "authors": str(candidate.get("authors") or "").strip(),
        "abstract": str(candidate.get("abstract") or "").strip(),
        "year": year,
        "publication_date": candidate.get("publication_date"),
        "journal": str(candidate.get("journal") or "").strip(),
        "doi": doi,
        "url": url,
        "cited_by_count": int(cited or 0),
        "topics": candidate.get("topics") or [],
        "sources": sources,
        "paper_id": str((existing["id"] if existing else "") or "") or None,
        "paper_status": str((existing["status"] if existing else "") or "") or None,
        "in_library": bool(existing and str(existing["status"] or "") == "library"),
    }


def search_online_sources(
    db: sqlite3.Connection,
    query: str,
    *,
    limit: int = 20,
    from_year: Optional[int] = None,
) -> list[dict]:
    """Run a multi-source online search, ranked by query relevance.

    Blocking variant of ``stream_online_sources`` — consumes the stream
    and returns the ``final`` event's items, so both the streaming Find &
    Add surface and the synchronous routes (`/import/search`,
    `/discovery/manual-search`) share one retrieval + ranking path.
    Items are ordered by closeness to the query (RRF + query-text match,
    see ``rank_by_query_relevance``); each carries the personal-fit
    ``like_score`` / ``score_breakdown`` for the "why" chips and the
    ``sources`` provenance chip.
    """
    for event in stream_online_sources(db, query, limit=limit, from_year=from_year):
        if event.get("type") == "final":
            return event.get("items") or []
    return []


def stream_online_sources(
    db: sqlite3.Connection,
    query: str,
    *,
    limit: int = 20,
    from_year: Optional[int] = None,
):
    """Streaming multi-source search for the Find & Add surface.

    Yields NDJSON-friendly events:

        {"type": "source_pending", "source": <name>}                # at lane start
        {"type": "source_partial", "source": <name>, "items": [...], "ms": <int>}
            — raw items decorated with cheap library-state fields
            (`in_library`, `paper_id`, `sources`) so the UI can render
            cards immediately. No personal-fit `like_score` yet.
        {"type": "source_timeout", "source": <name>, "ms": <int>}
        {"type": "source_error",   "source": <name>, "error": <str>, "ms": <int>}
        {"type": "final", "items": [...ranked, dedup'd, scored...], "total": <int>}

    Ranking contract (2026-06-04): `final` items are ordered by **query
    relevance** (`rank_by_query_relevance`: cross-source RRF + query-text
    match) — search-engine semantics, what you typed decides the order.
    Personal fit (`like_score` + `score_breakdown`) is still computed on
    the ranked union, but only feeds the per-result "why" chips and the
    optional client-side "Personal fit" sort; it does not reorder the
    default list. Per-source events stay cheap (no scoring) so the user
    sees results within a few hundred milliseconds.
    """
    raw_query = (query or "").strip()
    if not raw_query:
        yield {"type": "final", "items": [], "total": 0}
        return

    max_items = max(1, min(int(limit or 20), 100))

    source_settings: dict[str, str] = {}
    try:
        settings_rows = db.execute(
            "SELECT key, value FROM settings "
            "WHERE key LIKE 'sources.%' OR key LIKE 'strategies.%'"
        ).fetchall()
        source_settings = {
            str(row["key"]): str(row["value"] if row["value"] is not None else "")
            for row in settings_rows
        }
    except sqlite3.OperationalError:
        source_settings = {}

    raw_by_source: dict[str, list[dict]] = {}

    for event in stream_across_sources(
        raw_query,
        limit=max_items,
        from_year=from_year,
        settings=source_settings,
        # Find & Add is a streaming surface: a slow lane only delays its
        # own chip, so it gets a longer deadline than the blocking lens
        # fan-out, plus fail-fast S2 (1 retry, raise on 429) so shared-
        # pool congestion reads "rate-limited", not "timeout".
        lane_deadline_s=FINDADD_LANE_DEADLINE_S,
        s2_fail_fast=True,
    ):
        ev_type = event.get("type")
        if ev_type == "source_pending":
            yield event
        elif ev_type == "source_complete":
            source_name = event["source"]
            items = event.get("items") or []
            raw_by_source[source_name] = items
            # Cheap decoration only (no personal-fit scoring) so the UI
            # can render cards within ms of the source returning.
            decorated = [_decorate_candidate(db, item) for item in items]
            yield {
                "type": "source_partial",
                "source": source_name,
                "items": decorated,
                "ms": event.get("ms"),
            }
        elif ev_type in ("source_timeout", "source_error"):
            yield event

    # Final pass: dedup across sources and rank by query relevance
    # (RRF + query-text match). Personal-fit scoring then runs once on
    # the ranked union — chip data only, the order is NOT re-sorted.
    # The frontend replaces its preview rows with this when `final`
    # arrives.
    ranked_raw = merge_streamed_results(
        raw_by_source, raw_query, limit=max_items, settings=source_settings
    )
    if not ranked_raw:
        yield {"type": "final", "items": [], "total": 0}
        return

    (
        scorer_settings,
        preference_profile,
        positive_centroid,
        negative_centroid,
        positive_texts,
        negative_texts,
        lexical_profile,
    ) = _build_personal_scorer(db)

    decorated_items: list[tuple[int, dict, dict]] = []
    candidate_texts: dict[str, str] = {}
    for idx, candidate in enumerate(ranked_raw):
        decorated = _decorate_candidate(db, candidate)
        text = sim_module.build_similarity_text(
            {**decorated, "topics": candidate.get("topics") or []},
            conn=db,
            paper_topics=candidate.get("topics") or [],
        ) or ""
        decorated_items.append((idx, candidate, decorated))
        candidate_texts[str(idx)] = text

    lexical_details_by_idx: dict[str, dict] = {}
    if lexical_profile is not None and candidate_texts:
        try:
            lexical_details_by_idx = sim_module.batch_compute_lexical_similarity(
                candidate_texts, lexical_profile
            )
        except Exception:
            lexical_details_by_idx = {}

    scored: list[dict] = []
    total = max(1, len(decorated_items))
    for idx, candidate, decorated in decorated_items:
        source_relevance = float(candidate.get("score") or 0.0) or max(
            0.0, 1.0 - (idx / total)
        )
        like_score, score_breakdown = _score_search_result(
            db=db,
            settings=scorer_settings,
            preference_profile=preference_profile,
            positive_centroid=positive_centroid,
            negative_centroid=negative_centroid,
            positive_texts=positive_texts,
            negative_texts=negative_texts,
            item={**decorated, "topics": candidate.get("topics") or []},
            source_relevance=source_relevance,
            source_key=raw_query,
            lexical_profile=lexical_profile,
            precomputed_lexical_details=lexical_details_by_idx.get(str(idx)),
        )
        scored.append(
            {
                **decorated,
                # Query-relevance score (0–1) from `rank_by_query_relevance`
                # — the rank driver, surfaced so the client can restore
                # "Best match" order after sorting by another key.
                "relevance": round(float(candidate.get("score") or 0.0), 4),
                "like_score": round(float(like_score), 2),
                "score_breakdown": score_breakdown,
            }
        )
    # `scored` already carries rank_by_query_relevance order — do NOT
    # re-sort by like_score; personal fit is chip data, not the ranking.
    yield {"type": "final", "items": scored, "total": len(scored)}


# Shared action -> rating contract. Matches ``alma.application.feed``.
# (See CLAUDE.md D6: imports / online-search save must use the same
# add/like/love/dislike -> 3/4/5/1 mapping Feed and Discovery do.)
_ONLINE_SEARCH_ACTION_RATINGS = {"add": 3, "like": 4, "love": 5, "dislike": 1}


def _resolve_work_from_inputs(
    *,
    openalex_id: Optional[str],
    doi: Optional[str],
    link: Optional[str],
    title: Optional[str],
    query: Optional[str],
) -> tuple[Optional[dict], str]:
    """Return the best OpenAlex work for the given input fields (or None)."""
    if openalex_id and str(openalex_id).strip():
        work = _fetch_work_by_openalex_id(str(openalex_id).strip())
        if work:
            return work, "openalex_id"
    if doi and str(doi).strip():
        work = _fetch_work_by_doi(str(doi).strip())
        if work:
            return work, "doi"
    if link and str(link).strip():
        work, source = _resolve_raw_work_for_query(str(link).strip())
        if work:
            return work, source
    if title and str(title).strip():
        work, source = _resolve_raw_work_for_query(f"title:{str(title).strip()}")
        if work:
            return work, source
    if query and str(query).strip():
        return _resolve_raw_work_for_query(str(query).strip())
    return None, "not_found"


def resolve_work_metadata(
    *,
    openalex_id: Optional[str] = None,
    doi: Optional[str] = None,
    title: Optional[str] = None,
) -> Optional[dict]:
    """Resolve display metadata for a work — READ ONLY (no DB, no writes).

    Used by the browser connector's ``/lookup`` to show a paper's real
    title in the popup *before* the user saves (e.g. on a PDF, where the
    page has no metadata but the URL carries a DOI). Returns a compact
    ``{title, authors, year, journal, doi, openalex_id}`` dict, or ``None``
    when the work can't be resolved upstream.
    """
    raw_work, _src = _resolve_work_from_inputs(
        openalex_id=openalex_id, doi=doi, link=None, title=title, query=None
    )
    if not raw_work:
        return None
    n = _normalize_work(raw_work)
    return {
        "title": n.get("title") or "",
        "authors": n.get("authors") or "",
        "year": n.get("year"),
        "journal": n.get("journal") or "",
        "doi": n.get("doi") or "",
        "openalex_id": n.get("openalex_id") or "",
    }


def save_online_search_result(
    db: sqlite3.Connection,
    *,
    openalex_id: Optional[str] = None,
    doi: Optional[str] = None,
    link: Optional[str] = None,
    title: Optional[str] = None,
    query: Optional[str] = None,
    candidate: Optional[dict] = None,
    action: str = "add",
    added_from: str = "online_search",
    default_reading_status: Optional[str] = None,
    override_added_from: bool = False,
) -> dict:
    """Resolve a work + apply the shared add/like/love/dislike contract.

    Canonical ingest path for the unified Find-and-add surface (Discovery
    + Import Online tab). Maps ``action`` to the ``3/4/5/1`` rating used
    everywhere else and lands the paper in Library (for add/like/love) or
    the dismissed sink (for dislike). Already-saved papers get a
    **monotonic rating upgrade** — add-after-love never downgrades a
    loved paper to ``3``.

    Resolution order:
      1. OpenAlex via ``openalex_id`` / ``doi`` / ``link`` / ``title`` /
         ``query`` — preferred because OpenAlex is the enrichment source.
      2. When OpenAlex misses and a ``candidate`` dict is supplied (the
         multi-source search result from Semantic Scholar / Crossref /
         arXiv / bioRxiv), the paper is upserted directly from the
         candidate metadata via the shared feed upsert helper. The
         background enrichment job will top up OpenAlex-specific fields
         when available.

    ``default_reading_status`` is forwarded to ``add_to_library`` for the
    add/like/love path so a caller can land the paper directly on the
    reading list (``'reading'``) instead of the untriaged library
    (``None``). It is only applied when the paper has no reading status
    yet (``add_to_library`` never overwrites an existing one).

    ``override_added_from`` is forwarded to ``add_to_library``: when True
    the supplied ``added_from`` wins even over an existing non-empty
    provenance. Used by deliberate, explicit save surfaces (e.g. the
    browser connector) so their provenance isn't masked by the ``'feed'``
    stamp the candidate-fallback upsert applies to brand-new rows.
    """
    action = (action or "add").strip().lower()
    if action not in _ONLINE_SEARCH_ACTION_RATINGS:
        raise ValueError(f"Invalid action: {action!r}")

    raw_work, match_source = _resolve_work_from_inputs(
        openalex_id=openalex_id,
        doi=doi,
        link=link,
        title=title,
        query=query,
    )
    normalized: dict = {}
    paper_id: Optional[str] = None
    if raw_work is not None:
        normalized = _normalize_work(raw_work)
        _ensure_schema(db)
        paper_id = _upsert_single_paper(db, normalized)

    if not paper_id:
        # Fall back to the multi-source candidate when OpenAlex can't
        # resolve the paper. The feed upsert helper handles dedup via
        # the canonical triple and fills whatever metadata the source
        # provided; enrichment fills the rest later.
        if candidate:
            paper_id = _upsert_candidate_paper(
                db,
                dict(candidate),
                now=datetime.utcnow().isoformat(),
            )
            match_source = (
                str(candidate.get("source_api") or "").strip()
                or (
                    candidate.get("source_apis")[0]
                    if isinstance(candidate.get("source_apis"), list)
                    and candidate.get("source_apis")
                    else ""
                )
                or "multi_source"
            )
        if not paper_id:
            raise ValueError(
                "Could not resolve the paper — missing OpenAlex match and "
                "no candidate metadata to fall back on."
            )

    target_rating = _ONLINE_SEARCH_ACTION_RATINGS[action]
    now = datetime.utcnow().isoformat()
    current = db.execute(
        "SELECT status, rating FROM papers WHERE id = ?", (paper_id,)
    ).fetchone()
    current_status = str((current["status"] if current else "") or "").strip().lower()
    current_rating = int((current["rating"] if current else 0) or 0)
    effective_rating = target_rating

    if action in {"add", "like", "love"}:
        # Monotonic rating upgrade — never downgrade a saved paper.
        effective_rating = max(current_rating, target_rating)
        library_app.add_to_library(
            db,
            paper_id,
            rating=effective_rating,
            added_from=added_from,
            default_reading_status=default_reading_status,
            override_added_from=override_added_from,
        )
    else:  # dislike
        if current_status == library_app.LIBRARY_STATUS:
            # Respect an existing save: don't auto-remove from library just
            # because the user hit dislike on the online search surface.
            # Record the negative signal and leave the library entry alone.
            effective_rating = current_rating
        else:
            library_app.dismiss_paper(db, paper_id)
            effective_rating = target_rating

    library_app.record_paper_feedback(
        db,
        paper_id,
        action=action,
        rating=effective_rating,
        source_surface=added_from,
    )

    # NB: `publication_references` rows were already written by
    # `_upsert_single_paper` → `upsert_work_sidecars`: every resolve path
    # selects `referenced_works` (`_WORKS_SELECT_FIELDS`), so a separate
    # referenced-works fetch here would be a redundant upstream round-trip
    # on the user-facing save path.

    db.execute(
        """
        UPDATE papers
        SET openalex_resolution_status = 'openalex_resolved',
            openalex_resolution_reason = ?,
            openalex_resolution_updated_at = ?,
            updated_at = ?
        WHERE id = ?
        """,
        (f"{added_from}:{match_source}", now, now, paper_id),
    )
    library_app.sync_surface_resolution(
        db,
        paper_id,
        action=action,
        source_surface=added_from,
    )
    db.commit()

    row = db.execute("SELECT * FROM papers WHERE id = ?", (paper_id,)).fetchone()
    if not row:
        raise RuntimeError("Paper was upserted but could not be loaded")
    out = dict(row)
    out["match_source"] = match_source
    out["action"] = action
    out["rating"] = effective_rating
    return out


def add_work_to_library(
    db: sqlite3.Connection,
    *,
    openalex_id: Optional[str] = None,
    doi: Optional[str] = None,
    link: Optional[str] = None,
    title: Optional[str] = None,
    query: Optional[str] = None,
    added_from: str = "discovery_manual",
) -> dict:
    sources = [openalex_id, doi, link, title, query]
    source_value = next((str(v).strip() for v in sources if str(v or "").strip()), "")
    if not source_value:
        raise ValueError("No input provided")

    raw_work = None
    match_source = "unknown"

    if openalex_id and str(openalex_id).strip():
        raw_work = _fetch_work_by_openalex_id(str(openalex_id).strip())
        match_source = "openalex_id"
    if raw_work is None and doi and str(doi).strip():
        raw_work = _fetch_work_by_doi(str(doi).strip())
        match_source = "doi"
    if raw_work is None and link and str(link).strip():
        raw_work, match_source = _resolve_raw_work_for_query(str(link).strip())
    if raw_work is None and title and str(title).strip():
        raw_work, match_source = _resolve_raw_work_for_query(f"title:{str(title).strip()}")
    if raw_work is None:
        raw_work, match_source = _resolve_raw_work_for_query(source_value)
    if raw_work is None:
        raise ValueError("No OpenAlex work found for the provided input")

    normalized = _normalize_work(raw_work)
    _ensure_schema(db)
    paper_id = _upsert_single_paper(db, normalized)
    if not paper_id:
        raise ValueError("Resolved work is missing required title metadata")

    # `publication_references` already written by `_upsert_single_paper` →
    # `upsert_work_sidecars` (the resolve selects `referenced_works`) — no
    # separate referenced-works fetch needed.

    now = datetime.utcnow().isoformat()
    library_app.add_to_library(
        db,
        paper_id,
        added_from=added_from,
    )
    db.execute(
        """
        UPDATE papers
        SET openalex_resolution_status = 'openalex_resolved',
            openalex_resolution_reason = ?,
            openalex_resolution_updated_at = ?,
            updated_at = ?
        WHERE id = ?
        """,
        (
            f"manual_add:{match_source}",
            now,
            now,
            paper_id,
        ),
    )
    library_app.sync_surface_resolution(
        db,
        paper_id,
        action="save",
        source_surface="discovery_manual",
    )
    db.commit()

    row = db.execute("SELECT * FROM papers WHERE id = ?", (paper_id,)).fetchone()
    if not row:
        raise RuntimeError("Paper was upserted but could not be loaded")
    out = dict(row)
    out["match_source"] = match_source
    return out
