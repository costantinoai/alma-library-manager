"""OpenAlex client utilities.

Higher-level helpers that fetch authors, works, and profiles from OpenAlex
and persist them locally.  All HTTP traffic goes through the shared
:class:`~alma.openalex.http.OpenAlexClient` singleton (API-key auth,
rate-limit tracking, automatic retries).
"""

from __future__ import annotations

import logging
import sqlite3
import re
from pathlib import Path
from typing import Callable, Dict, Iterable, List, Optional, TypeVar
from datetime import datetime
from difflib import SequenceMatcher

import requests

from alma.core.utils import normalize_doi as _normalize_doi, normalize_text as _normalize_text
from alma.openalex.http import get_client

logger = logging.getLogger(__name__)
_T = TypeVar("_T")


def _session(mailto: Optional[str] = None):
    """Backward-compatible requests-like session wrapper."""
    base_params = {"mailto": mailto} if str(mailto or "").strip() else {}

    class _CompatSession:
        def __init__(self, mailto_value: Optional[str]) -> None:
            self.params = {"mailto": mailto_value} if str(mailto_value or "").strip() else {}

        def get(self, url: str, **kwargs):
            params = dict(kwargs.pop("params", {}) or {})
            for key, value in base_params.items():
                params.setdefault(key, value)
            url_text = str(url or "").strip()
            if not url_text.startswith("http"):
                url_text = f"https://api.openalex.org/{url_text.lstrip('/')}"
            return requests.get(url_text, params=params, **kwargs)

    return _CompatSession(mailto)


def find_author_id_by_name(name: str, mailto: Optional[str] = None) -> Optional[str]:
    try:
        session = _session(mailto)
        resp = session.get("https://api.openalex.org/authors", params={"search": name, "per-page": 1}, timeout=20)
        data = resp.json()
        results = data.get("results", [])
        if results:
            return results[0].get("id")
    except Exception as e:
        logger.warning(f"OpenAlex author search failed for '{name}': {e}")
    return None


def get_author_name_by_id(author_openalex_id: str, mailto: Optional[str] = None) -> Optional[str]:
    """Fetch an OpenAlex author's display name by ID.

    Returns the display name or None if not found.
    """
    try:
        session = _session(mailto)
        # Accept both bare IDs (A...) and full URLs (https://openalex.org/A...)
        aid = _normalize_openalex_author_id(author_openalex_id)
        url = str(author_openalex_id or "").strip() or f"https://openalex.org/{aid}"
        if not url.startswith("http"):
            url = f"https://openalex.org/{aid}"
        resp = session.get(url, timeout=20)
        if resp.status_code != 200:
            return None
        data = resp.json()
        return data.get("display_name")
    except Exception as e:
        logger.warning(f"OpenAlex author get failed for '{author_openalex_id}': {e}")
        return None


def find_author_by_orcid(orcid: str, mailto: Optional[str] = None) -> Optional[Dict[str, str]]:
    """Resolve an ORCID to an OpenAlex author record.

    Returns dict with keys {"id", "display_name"} if found, else None.
    """
    try:
        # Normalize ORCID (strip URL prefix)
        o = orcid.strip()
        if o.startswith('http'):
            o = o.rstrip('/').split('/')[-1]
        session = _session(mailto)
        resp = session.get(
            "https://api.openalex.org/authors",
            params={"filter": f"orcid:{o}", "per-page": 1},
            timeout=20,
        )
        if resp.status_code != 200:
            return None
        data = resp.json() or {}
        results = data.get('results') or []
        if not results:
            return None
        rec = results[0]
        return {"id": rec.get("id"), "display_name": rec.get("display_name")}
    except Exception as e:
        logger.warning(f"OpenAlex ORCID lookup failed for '{orcid}': {e}")
        return None


def _normalize_openalex_author_id(author_id: str) -> str:
    """Normalize an OpenAlex author ID to the canonical ``A...`` form.

    Accepts URL forms, URL-encoded residue from prior mangling
    (``3Aa5042972527`` — a `%3A` artefact from an earlier URL decode
    that was then persisted), lowercase variants (``a5042972527``), and
    already-bare IDs. Returns the canonical uppercase bare form.
    Returns the original value when empty/unparseable.
    """
    if not author_id:
        return author_id
    aid = author_id.strip()
    for prefix in (
        "https://openalex.org/",
        "http://openalex.org/",
        "openalex.org/",
    ):
        if aid.lower().startswith(prefix.lower()):
            aid = aid[len(prefix):].rstrip("/")
            break
    else:
        if aid.startswith("http"):
            # Generic URL fallback -- extract terminal segment
            aid = aid.rstrip("/").split("/")[-1]

    # Strip URL-encoded residue: `%3A` → `3A`. The first two chars before
    # the author prefix are the leftover after a buggy decode pass. The
    # canonical shape is `A<digits>`; anything before the first `A`/`a`
    # that isn't a real prefix can be dropped.
    if aid and aid[:2].lower() == "3a":
        aid = aid[2:]

    # Uppercase the author prefix letter (OpenAlex uses `A` + digits).
    if aid and aid[0] in ("a",):
        aid = "A" + aid[1:]

    return aid


def _normalize_openalex_work_id(work_id: str) -> str:
    """Normalize an OpenAlex work ID by stripping URL prefixes."""
    if not work_id:
        return work_id
    wid = work_id.strip()
    for prefix in (
        "https://openalex.org/",
        "http://openalex.org/",
        "openalex.org/",
    ):
        if wid.lower().startswith(prefix.lower()):
            return wid[len(prefix):].rstrip("/")
    if wid.startswith("http"):
        return wid.rstrip("/").split("/")[-1]
    return wid


def fetch_works_for_author(
    author_openalex_id: str,
    from_year: Optional[int] = None,
    mailto: Optional[str] = None,
) -> List[Dict]:
    """Fetch all works for an OpenAlex author using cursor pagination.

    This uses the official Works endpoint with the `author.id:A...` filter,
    `per-page=100`, and `cursor=*` to iterate through all of an author's works.

    Args:
        author_openalex_id: The author's OpenAlex ID. Accepts full URL or bare key (e.g., "A123...").
        from_year: Optional lower bound (inclusive) for publication year; if None, fetch full history.

    Returns:
        List of normalized work dicts with keys:
        title, authors, abstract, year, num_citations, journal, pub_url, doi
    """
    works: List[Dict] = []
    try:
        session = _session(mailto)
        # Use official works API filter: author.id:A... (recommended by OpenAlex)
        oaid = _normalize_openalex_author_id(author_openalex_id)
        filt = f"author.id:{oaid}"
        if from_year:
            # Use from_publication_date for year ranges per OpenAlex docs
            filt = f"{filt},from_publication_date:{from_year}-01-01"
        cursor = "*"
        while True:
            params = {
                "filter": filt,
                # Use dashed style per OpenAlex docs; underscore often works too
                "per-page": 100,
                "cursor": cursor,
                # Sorting is optional but helps surface high-impact first if UI streams
                "sort": "cited_by_count:desc",
                "select": _WORKS_SELECT_FIELDS,
            }
            resp = session.get("https://api.openalex.org/works", params=params, timeout=30)
            # Raise for non-200 to surface proper logging and exit
            resp.raise_for_status()
            data = resp.json() or {}
            batch = data.get("results", []) or []
            for w in batch:
                title = (w or {}).get("display_name") or ""
                year = (w or {}).get("publication_year")
                pub_date = (w or {}).get("publication_date") or None
                wtype = (w or {}).get("type")
                wtype_xref = None  # not selected; keep variable for backward-compat in checks
                # Filter out datasets/components and file-like titles
                if _looks_like_file_title(title):
                    continue
                # Accept either OpenAlex `type` or Crossref-style `type_crossref` (if present)
                allowed_types = {
                    # OpenAlex canonical types
                    "journal-article", "proceedings-article", "book-chapter", "report", "book", "preprint", "posted-content",
                    # Crossref style sometimes appears as `type`
                    "article",
                }
                # Skip if both provided type indicators are not allowed; if only one present and not allowed, skip
                if (wtype and wtype not in allowed_types) and (wtype_xref and wtype_xref not in allowed_types):
                    continue
                if (wtype and wtype not in allowed_types) and (not wtype_xref):
                    continue
                if (wtype_xref and wtype_xref not in allowed_types) and (not wtype):
                    continue
                abstract = _decode_abstract((w or {}).get("abstract_inverted_index"))
                primary_location = (w or {}).get("primary_location") or {}
                # Prefer landing page; fallback to PDF; else use the work's id (OpenAlex URL)
                url = primary_location.get("landing_page_url") or primary_location.get("pdf_url") or (w or {}).get("id")
                # Use primary_location.source.display_name as journal/source label
                src = (primary_location.get("source") or {}) if isinstance(primary_location, dict) else {}
                journal = src.get("display_name")
                doi = _normalize_doi((w or {}).get("doi"))
                cites = (w or {}).get("cited_by_count")
                # Join authors
                authorships = (w or {}).get("authorships") or []
                auths = ", ".join([ (a.get("author") or {}).get("display_name", "") for a in authorships ])
                # Extract institutions from authorships for geo stats
                insts = []
                try:
                    for a in authorships:
                        for inst in (a.get("institutions") or []):
                            inst_id = (inst.get("id") or "").strip()
                            inst_name = (inst.get("display_name") or "").strip()
                            ccode = (inst.get("country_code") or "").strip().upper()
                            if not inst_id and not inst_name and not ccode:
                                continue
                            insts.append({"id": inst_id, "name": inst_name, "country_code": ccode})
                except Exception:
                    insts = []
                # Extract topics (prefer `topics`, fallback to `concepts`)
                topics_list = []
                try:
                    raw_topics = (w or {}).get("topics") or []
                    if isinstance(raw_topics, list) and raw_topics:
                        topics_list = [
                            {
                                "term": (t.get("display_name") or "").strip(),
                                "score": t.get("score"),
                            }
                            for t in raw_topics
                            if isinstance(t, dict) and (t.get("display_name") or "").strip()
                        ]
                    elif isinstance((w or {}).get("concepts"), list):
                        raw_concepts = (w or {}).get("concepts") or []
                        topics_list = [
                            {
                                "term": (c.get("display_name") or "").strip(),
                                "score": c.get("score"),
                            }
                            for c in raw_concepts
                            if isinstance(c, dict) and (c.get("display_name") or "").strip()
                        ]
                except Exception:
                    topics_list = []

                works.append({
                    "title": title,
                    "authors": auths,
                    "abstract": abstract or "",
                    "year": year,
                    "publication_date": pub_date,
                    "num_citations": cites,
                    "journal": journal or "",
                    "pub_url": url or "",
                    "doi": doi or "",
                    "topics": topics_list,
                    "institutions": insts,
                })
            cursor = data.get("meta", {}).get("next_cursor")
            if not cursor:
                break
    except Exception as e:
        logger.error(f"OpenAlex works fetch failed for {author_openalex_id}: {e}")
    if not works:
        logger.info("OpenAlex returned 0 works for author %s (from_year=%s)", author_openalex_id, from_year)
    else:
        logger.info("OpenAlex fetched %d works for author %s", len(works), author_openalex_id)
    return works


def fetch_works_page_for_author(
    author_openalex_id: str,
    *,
    cursor: str = "*",
    per_page: int = 50,
    sort: str = "cited_by_count:desc",
    mailto: Optional[str] = None,
) -> Dict:
    """Fetch one page of an author's works from OpenAlex.

    Mirrors ``fetch_works_for_author`` but returns one cursor-paged batch
    plus the next cursor, so the UI can lazily page through a prolific
    author without blocking the dialog open on a full corpus download.

    Returns:
        ``{"results": [...], "next_cursor": str | None, "total": int | None}``
        where each result has the same normalized shape as
        ``fetch_works_for_author``.
    """
    try:
        session = _session(mailto)
        oaid = _normalize_openalex_author_id(author_openalex_id)
        filt = f"author.id:{oaid}"
        params = {
            "filter": filt,
            "per-page": max(1, min(per_page, 200)),
            "cursor": cursor or "*",
            "sort": sort,
            "select": _WORKS_SELECT_FIELDS,
        }
        resp = session.get("https://api.openalex.org/works", params=params, timeout=30)
        resp.raise_for_status()
        data = resp.json() or {}
    except Exception as exc:
        logger.error(f"OpenAlex page fetch failed for {author_openalex_id}: {exc}")
        return {"results": [], "next_cursor": None, "total": None, "error": str(exc)}

    batch = data.get("results", []) or []
    results: List[Dict] = []
    for w in batch:
        title = (w or {}).get("display_name") or ""
        if _looks_like_file_title(title):
            continue
        wtype = (w or {}).get("type")
        allowed_types = {
            "journal-article", "proceedings-article", "book-chapter",
            "report", "book", "preprint", "posted-content", "article",
        }
        if wtype and wtype not in allowed_types:
            continue
        results.append(_normalize_work(w))

    meta = data.get("meta") or {}
    next_cursor = meta.get("next_cursor")
    total_count = meta.get("count")
    return {
        "results": results,
        "next_cursor": next_cursor if isinstance(next_cursor, str) and next_cursor else None,
        "total": int(total_count) if isinstance(total_count, int) else None,
    }


# Same `select` field list used by both `fetch_author_profile` (single)
# and `batch_get_author_profiles` (pipe-filter), so the two helpers
# always observe the same OpenAlex response shape — and downstream
# consumers (`apply_author_profile_update`, `_ensure_authorship_row`,
# the local-coverage shortcut in `refresh_author_works_and_vectors`)
# can treat the two interchangeably.
_AUTHOR_PROFILE_SELECT_FIELDS = ",".join([
    "id", "display_name", "orcid",
    "last_known_institutions",  # current affiliations
    "affiliations",             # institutional history
    "works_count", "cited_by_count",
    "summary_stats",            # h_index, i10_index etc
    "topics",                   # research topics
    "counts_by_year",           # yearly citation/works counts
    "x_concepts",              # broader concepts
])


def _shape_author_profile(data: Dict) -> Dict:
    """Reshape a raw OpenAlex `/authors/{id}` JSON dict into the curated
    profile shape that callers consume.

    Extracted from `fetch_author_profile` so the batched
    `batch_get_author_profiles` returns identical dicts. Keys:
    display_name, affiliation, citedby, h_index, interests, works_count,
    orcid, cited_by_year, institutions.
    """
    last_insts = data.get("last_known_institutions") or []
    affiliation = last_insts[0].get("display_name") if last_insts else None

    summary = data.get("summary_stats") or {}
    h_index = summary.get("h_index", 0)

    topics_raw = data.get("topics") or []
    interests = [t.get("display_name") for t in topics_raw[:10] if t.get("display_name")]
    if not interests:
        concepts = data.get("x_concepts") or []
        interests = [c.get("display_name") for c in concepts[:10] if c.get("display_name")]

    counts = data.get("counts_by_year") or []
    cited_by_year = {str(c["year"]): c.get("cited_by_count", 0) for c in counts if "year" in c}

    affiliations_raw = data.get("affiliations") or []
    institutions = []
    for aff in affiliations_raw:
        inst = aff.get("institution") or {}
        years = aff.get("years") or []
        institutions.append({
            "name": inst.get("display_name", ""),
            "country": inst.get("country_code", ""),
            "years": years,
        })

    return {
        "display_name": data.get("display_name"),
        "affiliation": affiliation,
        "citedby": data.get("cited_by_count", 0),
        "h_index": h_index,
        "interests": interests,
        "works_count": data.get("works_count", 0),
        "orcid": data.get("orcid"),
        "cited_by_year": cited_by_year,
        "institutions": institutions,
    }


def fetch_author_profile(
    author_openalex_id: str,
    mailto: Optional[str] = None,
) -> Optional[Dict]:
    """Fetch full author profile from OpenAlex.

    Returns dict with: display_name, affiliation, citedby, h_index, interests/topics,
    works_count, url_picture (thumbnail), orcid, cited_by_year, institutions, email_domain
    """
    try:
        session = _session(mailto)
        aid = _normalize_openalex_author_id(author_openalex_id)
        params = {"select": _AUTHOR_PROFILE_SELECT_FIELDS}
        resp = session.get(f"https://api.openalex.org/authors/{aid}", params=params, timeout=20)
        if resp.status_code != 200:
            return None
        return _shape_author_profile(resp.json() or {})
    except Exception as e:
        logger.warning(f"OpenAlex author profile fetch failed for '{author_openalex_id}': {e}")
        return None


def batch_get_author_profiles(
    openalex_author_ids: list[str],
    batch_size: int = 50,
    max_workers: int = 4,
    mailto: Optional[str] = None,
) -> Dict[str, Dict]:
    """Batched form of `fetch_author_profile`.

    Uses ``GET /authors?filter=openalex_id:A1|A2|...&select=...`` to
    fetch up to `batch_size` profiles per HTTP call (one credit each)
    instead of one singleton lookup per author. Chunks run in parallel
    via a small ThreadPoolExecutor.

    Returns ``dict[oid_norm → profile]`` where each profile has the
    exact same keys as `fetch_author_profile`'s return — so callers
    can swap a per-author fetch for a `cache.get(oid)` lookup with no
    other code changes.

    Why: `_deep_refresh_all_impl` hits hundreds of authors; a single
    pre-flight batched roundtrip pays for itself many times over.
    """
    raw_ids = list({_normalize_openalex_author_id(aid) for aid in openalex_author_ids if aid})
    raw_ids = [aid for aid in raw_ids if aid]
    if not raw_ids:
        return {}

    session = _session(mailto)
    batch_size = max(1, min(int(batch_size or 50), 50))
    max_workers = max(1, int(max_workers or 1))
    chunks = [raw_ids[i : i + batch_size] for i in range(0, len(raw_ids), batch_size)]

    def _fetch_chunk(chunk_idx: int, chunk_ids: list[str]) -> Dict[str, Dict]:
        """Fetch one pipe-filter chunk; halves on 400 / 414 (URL too long)."""
        def _request(ids: list[str]) -> Dict[str, Dict]:
            pipe_filter = "openalex_id:" + "|".join(
                f"https://openalex.org/{aid}" for aid in ids
            )
            try:
                resp = session.get(
                    "https://api.openalex.org/authors",
                    params={
                        "filter": pipe_filter,
                        "per-page": len(ids),
                        "select": _AUTHOR_PROFILE_SELECT_FIELDS,
                    },
                    timeout=30,
                )
            except Exception as exc:
                logger.warning("Batch profile fetch errored for chunk %d: %s", chunk_idx, exc)
                return {}
            if resp.status_code == 200:
                out: Dict[str, Dict] = {}
                for item in (resp.json() or {}).get("results") or []:
                    aid_raw = (item.get("id") or "").strip()
                    if not aid_raw:
                        continue
                    aid_norm = _normalize_openalex_author_id(aid_raw)
                    if aid_norm:
                        out[aid_norm] = _shape_author_profile(item)
                return out
            if resp.status_code in {400, 414} and len(ids) > 1:
                mid = len(ids) // 2
                left = _request(ids[:mid])
                right = _request(ids[mid:])
                left.update(right)
                return left
            logger.warning(
                "Batch profile fetch returned %d for chunk %d (size=%d)",
                resp.status_code, chunk_idx, len(ids),
            )
            return {}

        return _request(chunk_ids)

    result = _run_chunked_parallel(
        chunks, max_workers, _fetch_chunk, label="Batch author profiles"
    )
    logger.info(
        "Batch author profiles: fetched %d/%d in %d batches",
        len(result), len(raw_ids), (len(raw_ids) + batch_size - 1) // batch_size,
    )
    return result


def get_author_metrics(
    author_openalex_id: str,
    mailto: Optional[str] = None,
) -> Optional[Dict[str, object]]:
    """Fetch summary metrics for an author from OpenAlex.

    Retrieves `works_count`, `cited_by_count`, and `summary_stats` (e.g., `h_index`)
    via the Authors endpoint. Useful to validate our locally-computed totals or
    to display quick stats without scanning all works.

    Args:
        author_openalex_id: OpenAlex Author ID; accepts URL or bare key.

    Returns:
        Dict with keys: id, display_name, works_count, cited_by_count, summary_stats, works_api_url
        or None on failure.
    """
    try:
        session = _session(mailto)
        aid = _normalize_openalex_author_id(author_openalex_id)
        params = {
            "select": ",".join([
                "id",
                "display_name",
                "works_count",
                "cited_by_count",
                "summary_stats",
                "works_api_url",
            ])
        }
        resp = session.get(f"https://api.openalex.org/authors/{aid}", params=params, timeout=20)
        if resp.status_code != 200:
            return None
        data = resp.json() or {}
        return {
            "id": data.get("id"),
            "display_name": data.get("display_name"),
            "works_count": data.get("works_count"),
            "cited_by_count": data.get("cited_by_count"),
            "summary_stats": data.get("summary_stats"),
            "works_api_url": data.get("works_api_url"),
        }
    except Exception as e:
        logger.warning(f"OpenAlex author metrics fetch failed for '{author_openalex_id}': {e}")
        return None


def _decode_abstract(inv_index: Optional[Dict[str, List[int]]]) -> Optional[str]:
    # OpenAlex stores abstracts as inverted indices; reconstruct string
    if not inv_index:
        return None
    # Build a list of words positioned by their first index
    max_pos = 0
    for positions in inv_index.values():
        max_pos = max(max_pos, max(positions))
    words = [None] * (max_pos + 1)
    for word, positions in inv_index.items():
        for pos in positions:
            if 0 <= pos < len(words):
                words[pos] = word
    return " ".join([w for w in words if w])


def _looks_like_file_title(title: str) -> bool:
    if not title:
        return False
    t = title.strip().lower()
    import re
    # Typical file extensions
    if re.search(r"\.(zip|tar|tar\.gz|gz|bz2|7z|rar|mat|csv|tsv|xlsx|xls|docx?|pptx?|txt)$", t):
        return True
    # Likely file-like if contains no spaces and has an extension
    if ('.' in t) and (' ' not in t):
        return True
    return False


def _ensure_schema(conn: sqlite3.Connection) -> dict:
    """Verify papers table exists (v3 schema). Returns column capability flags.

    Note: v3 schema is created by deps.py at startup. This function just verifies
    the table exists and returns capability flags for backward compatibility.
    """
    # Verify papers table exists
    try:
        cols_info = conn.execute("PRAGMA table_info(papers)").fetchall()
        if not cols_info:
            raise RuntimeError(
                "papers table does not exist. Ensure init_db_schema() was called at startup."
            )
        cols = [row[1] for row in cols_info]

        # Return capability flags (all v3 columns are guaranteed to exist)
        return {
            'has_pubdate': 'publication_date' in cols,
            'has_fetched': 'fetched_at' in cols,
            'has_source_id': 'source_id' in cols,
        }
    except sqlite3.OperationalError as e:
        logger.error("Failed to verify papers table schema: %s", e)
        raise RuntimeError(
            "papers table verification failed. Ensure init_db_schema() was called at startup."
        ) from e


def _upsert_topics(conn: sqlite3.Connection, paper_id: str, topics: List[Dict]) -> None:
    """Replace topic rows for a paper (v3 schema).

    When canonical topic dedup data is available (``topic_aliases`` table),
    each raw term is resolved to its ``topic_id`` so that publication_topics
    rows are linked to canonical topics automatically during enrichment.
    """
    if not topics:
        return
    try:
        conn.execute(
            "DELETE FROM publication_topics WHERE paper_id = ?",
            (paper_id,),
        )

        # Lazy-import resolve helper; graceful fallback if not available yet.
        _resolve = None
        try:
            from alma.library.topic_deduplication import resolve_topic_id
            _resolve = resolve_topic_id
        except Exception:
            pass

        for t in topics:
            term = (t.get("term") or "").strip() if isinstance(t, dict) else str(t)
            if not term:
                continue
            score_val = t.get("score") if isinstance(t, dict) else None
            domain = (t.get("domain") or "").strip() if isinstance(t, dict) else ""
            field = (t.get("field") or "").strip() if isinstance(t, dict) else ""
            subfield = (t.get("subfield") or "").strip() if isinstance(t, dict) else ""

            topic_id = None
            if _resolve:
                try:
                    topic_id = _resolve(conn, term)
                except Exception:
                    pass

            conn.execute(
                "INSERT OR REPLACE INTO publication_topics "
                "(paper_id, term, score, domain, field, subfield, topic_id) "
                "VALUES (?, ?, ?, ?, ?, ?, ?)",
                (paper_id, term, score_val, domain, field, subfield, topic_id),
            )
    except sqlite3.OperationalError as e:
        logger.warning("Failed to upsert topics for paper %s: %s", paper_id, e)


def _upsert_institutions(conn: sqlite3.Connection, paper_id: str, institutions: List[Dict]) -> None:
    """Replace institution rows for a paper (v3 schema)."""
    if not institutions:
        return
    try:
        conn.execute(
            "DELETE FROM publication_institutions WHERE paper_id = ?",
            (paper_id,),
        )
        seen: set = set()
        for inst in institutions:
            inst_id = (inst.get("id") or "").strip()
            inst_name = (inst.get("name") or "").strip()
            ccode = (inst.get("country_code") or "").strip().upper()
            key = inst_id or inst_name
            if not key or key in seen:
                continue
            seen.add(key)
            conn.execute(
                "INSERT OR REPLACE INTO publication_institutions "
                "(paper_id, institution_id, institution_name, country_code) "
                "VALUES (?, ?, ?, ?)",
                (paper_id, inst_id, inst_name, ccode),
            )
    except sqlite3.OperationalError as e:
        logger.warning("Failed to upsert institutions for paper %s: %s", paper_id, e)


def _upsert_referenced_works(
    conn: sqlite3.Connection,
    paper_id: str,
    referenced_work_ids: List[str],
) -> int:
    """Replace local reference rows for one paper."""
    if not paper_id:
        return 0
    try:
        conn.execute(
            "DELETE FROM publication_references WHERE paper_id = ?",
            (paper_id,),
        )
        inserted = 0
        seen: set[str] = set()
        for raw_work_id in referenced_work_ids or []:
            work_id = _normalize_openalex_work_id(str(raw_work_id or ""))
            if not work_id or work_id in seen:
                continue
            seen.add(work_id)
            conn.execute(
                """
                INSERT OR REPLACE INTO publication_references (paper_id, referenced_work_id)
                VALUES (?, ?)
                """,
                (paper_id, work_id),
            )
            inserted += 1
        return inserted
    except sqlite3.OperationalError as e:
        logger.warning("Failed to upsert references for paper %s: %s", paper_id, e)
        return 0


def _upsert_authorships(
    conn: sqlite3.Connection,
    paper_id: str,
    authorships: List[Dict],
) -> int:
    """Replace structured authorship rows for one paper."""
    if not paper_id:
        return 0
    try:
        conn.execute("DELETE FROM publication_authors WHERE paper_id = ?", (paper_id,))
        inserted = 0
        from alma.core.utils import normalize_orcid

        for a in authorships or []:
            oaid = (a.get("openalex_id") or "").strip()
            dname = (a.get("display_name") or "").strip()
            orcid = normalize_orcid(a.get("orcid")) or ""
            position = (a.get("position") or "").strip()
            is_corr = 1 if a.get("is_corresponding") else 0
            insts = a.get("institutions") or []
            inst_name = ""
            if insts and isinstance(insts, list):
                inst_name = (insts[0].get("name") or "").strip() if isinstance(insts[0], dict) else ""
            if oaid and dname:
                conn.execute(
                    """INSERT OR REPLACE INTO publication_authors
                        (paper_id, openalex_id, display_name, orcid, position, is_corresponding, institution)
                        VALUES (?, ?, ?, ?, ?, ?, ?)""",
                    (paper_id, oaid, dname, orcid, position, is_corr, inst_name),
                )
                inserted += 1
        return inserted
    except sqlite3.OperationalError as e:
        logger.warning("Failed to upsert authorships for paper %s: %s", paper_id, e)
        return 0


def upsert_work_sidecars(
    conn: sqlite3.Connection,
    paper_id: str,
    *,
    topics: Optional[List[Dict]] = None,
    institutions: Optional[List[Dict]] = None,
    authorships: Optional[List[Dict]] = None,
    referenced_works: Optional[List[str]] = None,
) -> dict[str, int]:
    """Persist structured per-paper enrichment sidecars when available."""
    summary = {
        "topics": 0,
        "institutions": 0,
        "authorships": 0,
        "references": 0,
    }
    if topics is not None:
        _upsert_topics(conn, paper_id, topics)
        summary["topics"] = len(topics or [])
    if institutions is not None:
        _upsert_institutions(conn, paper_id, institutions)
        summary["institutions"] = len(institutions or [])
    if authorships is not None:
        summary["authorships"] = _upsert_authorships(conn, paper_id, authorships)
    if referenced_works is not None:
        summary["references"] = _upsert_referenced_works(conn, paper_id, referenced_works)
    return summary


def _upsert_single_paper(conn: sqlite3.Connection, w: Dict, flags: dict) -> Optional[str]:
    """Upsert a single paper (v3 schema). Returns paper_id (UUID) if successful.

    Collision-safe since 2026-04-24: resolves existing rows via the canonical
    triple (openalex_id, doi, title+year) with blank-string → NULL boundary
    normalization, uses `INSERT OR IGNORE` on the new-row path so a parallel
    writer doesn't blow the partial UNIQUE index, and catches per-paper
    IntegrityError so a single collision doesn't abort multi-hour jobs.
    """
    import uuid
    from datetime import datetime

    from alma.core.utils import normalize_doi, resolve_existing_paper_id

    title = (w.get("title") or "").strip()
    if not title:
        return None

    # Boundary-normalize:
    #  - blank strings → None so the partial UNIQUE index on openalex_id
    #    (WHERE openalex_id IS NOT NULL) can't collide on `''`.
    #  - URL-form (`https://openalex.org/W...`) → bare `W...` so
    #    `resolve_existing_paper_id` finds a row the DB stored in bare
    #    form, and the partial UNIQUE index treats both forms as the
    #    same value.  Prior to this (D-AUDIT-10 Phase B follow-up,
    #    2026-04-25) the two forms lived side-by-side; a deep refresh
    #    that resolved by DOI/title then set openalex_id to the bare
    #    form would hit `UNIQUE constraint failed: papers.openalex_id`
    #    against any row already storing the bare form.
    openalex_id = _normalize_openalex_work_id((w.get("openalex_id") or "").strip()) or None
    doi = normalize_doi((w.get("doi") or "").strip()) or None
    url = (w.get("pub_url") or "").strip()
    journal = (w.get("journal") or "").strip()
    authors_str = (w.get("authors") or "").strip()
    abstract = (w.get("abstract") or "").strip()
    year = w.get("year")
    pub_date = w.get("publication_date")
    topics = w.get("topics") or []
    institutions = w.get("institutions") or []
    authorships = w.get("authorships") or []
    referenced_works = w.get("referenced_works") if isinstance(w.get("referenced_works"), list) else None

    try:
        cites = int(w.get("num_citations")) if w.get("num_citations") is not None else 0
    except (ValueError, TypeError):
        cites = 0

    existing_paper_id = resolve_existing_paper_id(
        conn,
        openalex_id=openalex_id or "",
        doi=doi or "",
        title=title,
        year=int(year) if isinstance(year, int) or (isinstance(year, str) and year.strip().isdigit()) else None,
    )

    now = datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%SZ')
    source_id = doi or url or title

    if existing_paper_id:
        # If the UPDATE would set a new openalex_id on a row whose own
        # openalex_id is blank, first guard against the partial UNIQUE
        # index firing on another row that already owns this openalex_id
        # (preprint/journal twin that isn't merged yet).  Skipping the
        # openalex_id overwrite for that iteration keeps the job making
        # progress; preprint_dedup will collapse the twin later.
        safe_openalex_id = openalex_id
        if openalex_id:
            twin = conn.execute(
                "SELECT id FROM papers WHERE openalex_id = ? AND id != ?",
                (openalex_id, existing_paper_id),
            ).fetchone()
            if twin is not None:
                logger.debug(
                    "Skipping openalex_id overwrite on %s — %s already owns %s",
                    existing_paper_id,
                    twin[0] if not isinstance(twin, sqlite3.Row) else twin["id"],
                    openalex_id,
                )
                safe_openalex_id = None
        # COALESCE each identifier so we don't clobber an already-hydrated row
        # with a bare Scholar record that re-resolved into an OpenAlex match.
        try:
            conn.execute(
                """UPDATE papers SET
                    title = COALESCE(NULLIF(?, ''), title),
                    authors = CASE WHEN COALESCE(authors, '') = '' THEN ? ELSE authors END,
                    year = COALESCE(year, ?),
                    journal = CASE WHEN COALESCE(journal, '') = '' THEN ? ELSE journal END,
                    abstract = CASE WHEN COALESCE(abstract, '') = '' THEN ? ELSE abstract END,
                    url = CASE WHEN COALESCE(url, '') = '' THEN ? ELSE url END,
                    doi = CASE WHEN COALESCE(doi, '') = '' AND ? IS NOT NULL THEN ? ELSE doi END,
                    publication_date = COALESCE(NULLIF(publication_date, ''), ?),
                    openalex_id = CASE WHEN COALESCE(openalex_id, '') = '' AND ? IS NOT NULL THEN ? ELSE openalex_id END,
                    cited_by_count = CASE WHEN ? > COALESCE(cited_by_count, 0) THEN ? ELSE cited_by_count END,
                    source_id = COALESCE(source_id, ?),
                    fetched_at = ?,
                    updated_at = ?
                WHERE id = ?""",
                (
                    title, authors_str, year, journal, abstract,
                    url, doi, doi, pub_date,
                    safe_openalex_id, safe_openalex_id,
                    cites, cites, source_id, now, now,
                    existing_paper_id,
                ),
            )
        except sqlite3.IntegrityError as exc:
            # Last-line defence: some other UNIQUE constraint fired
            # (e.g. a pre-heal URL-form row). Retry with all identifiers
            # scrubbed so the row's non-identifier columns still update
            # and the job keeps moving.  The identifier heal lives in
            # `init_db_schema`; don't abort a multi-hour job on one row.
            logger.warning(
                "IntegrityError on papers UPDATE (id=%s, openalex_id=%s, doi=%s): %s — retrying without identifier overwrites",
                existing_paper_id,
                openalex_id,
                doi,
                exc,
            )
            conn.execute(
                """UPDATE papers SET
                    title = COALESCE(NULLIF(?, ''), title),
                    authors = CASE WHEN COALESCE(authors, '') = '' THEN ? ELSE authors END,
                    year = COALESCE(year, ?),
                    journal = CASE WHEN COALESCE(journal, '') = '' THEN ? ELSE journal END,
                    abstract = CASE WHEN COALESCE(abstract, '') = '' THEN ? ELSE abstract END,
                    url = CASE WHEN COALESCE(url, '') = '' THEN ? ELSE url END,
                    publication_date = COALESCE(NULLIF(publication_date, ''), ?),
                    cited_by_count = CASE WHEN ? > COALESCE(cited_by_count, 0) THEN ? ELSE cited_by_count END,
                    source_id = COALESCE(source_id, ?),
                    fetched_at = ?,
                    updated_at = ?
                WHERE id = ?""",
                (
                    title, authors_str, year, journal, abstract,
                    url, pub_date,
                    cites, cites, source_id, now, now,
                    existing_paper_id,
                ),
            )
        paper_id = existing_paper_id
    else:
        paper_id = str(uuid.uuid4())
        try:
            conn.execute(
                """INSERT OR IGNORE INTO papers (
                    id, title, authors, year, journal, abstract, url, doi,
                    publication_date, openalex_id, cited_by_count, source_id,
                    fetched_at, created_at, updated_at, status
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (
                    paper_id, title, authors_str, year, journal, abstract, url, doi,
                    pub_date, openalex_id, cites, source_id,
                    now, now, now, 'tracked',
                ),
            )
        except sqlite3.IntegrityError as exc:
            # Partial UNIQUE on openalex_id can still fire if a sibling writer
            # won the race. Re-resolve and try the UPDATE path instead of
            # tearing down the whole job.
            logger.warning(
                "IntegrityError on papers insert (openalex_id=%s, doi=%s): %s",
                openalex_id,
                doi,
                exc,
            )
            existing_paper_id = resolve_existing_paper_id(
                conn,
                openalex_id=openalex_id or "",
                doi=doi or "",
                title=title,
                year=int(year) if isinstance(year, int) or (isinstance(year, str) and year.strip().isdigit()) else None,
            )
            if existing_paper_id:
                paper_id = existing_paper_id
            else:
                # Still can't find it — give up on this row, don't abort.
                return None

    upsert_work_sidecars(
        conn,
        paper_id,
        topics=topics,
        institutions=institutions,
        authorships=authorships,
        referenced_works=referenced_works,
    )

    return paper_id


def upsert_one_normalized_work(conn: sqlite3.Connection, work: Dict) -> Optional[str]:
    """Upsert one already-normalized work into an open SQLite connection."""
    flags = _ensure_schema(conn)
    return _upsert_single_paper(conn, work, flags)


def upsert_papers(works: Iterable[Dict], db_path: Path = Path("./data/scholar.db")) -> int:
    """Insert/replace works into papers DB (v3 schema).

    Args:
        works: Iterable of normalized work dicts (from _normalize_work or fetch_works_for_author).
        db_path: Path to the unified scholar.db database.

    Returns:
        Number of papers successfully upserted.
    """
    with sqlite3.connect(db_path) as conn:
        flags = _ensure_schema(conn)
        count = 0
        for w in works:
            if _upsert_single_paper(conn, w, flags):
                count += 1
        conn.commit()
        return count


def _get_author_details(
    openalex_author_id: str,
    mailto: Optional[str] = None,
) -> Optional[Dict[str, object]]:
    """Fetch minimal details for an OpenAlex author (display_name, orcid, last_known_institution).

    Returns None on failure.
    """
    try:
        session = _session(mailto)
        aid = _normalize_openalex_author_id(openalex_author_id)
        params = {
            "select": ",".join([
                "id",
                "display_name",
                "orcid",
                "last_known_institution",
                "works_count",
            ])
        }
        r = session.get(f"https://api.openalex.org/authors/{aid}", params=params, timeout=20)
        if r.status_code != 200:
            return None
        d = r.json() or {}
        inst = (d.get("last_known_institution") or {}) if isinstance(d, dict) else {}
        return {
            "id": d.get("id"),
            "display_name": d.get("display_name"),
            "orcid": d.get("orcid"),
            "institution": (inst.get("display_name") or "") if isinstance(inst, dict) else "",
        }
    except Exception as e:
        logger.warning(f"OpenAlex author details fetch failed for '{openalex_author_id}': {e}")
        return None


# Standard select fields for OpenAlex /authors requests.
_AUTHORS_SELECT_FIELDS = ",".join([
    "id", "display_name", "orcid", "ids",
    "last_known_institutions", "affiliations",
    "works_count", "cited_by_count", "summary_stats",
    "topics",
])


def _normalize_author_detail(item: dict) -> dict[str, object]:
    """Extract a rich author detail dict from a raw OpenAlex author response."""
    aid = _normalize_openalex_author_id((item.get("id") or "").strip())

    # ORCID
    orcid_raw = (item.get("orcid") or "").strip()

    # Institution — prefer last_known_institutions (new API), fall back to last_known_institution
    institution = ""
    lki = item.get("last_known_institutions") or []
    if isinstance(lki, list) and lki:
        institution = (lki[0].get("display_name") or "").strip() if isinstance(lki[0], dict) else ""
    if not institution:
        lki_old = item.get("last_known_institution") or {}
        if isinstance(lki_old, dict):
            institution = (lki_old.get("display_name") or "").strip()

    # Summary stats
    stats = item.get("summary_stats") or {}
    h_index = stats.get("h_index") or 0
    i10_index = stats.get("i10_index") or 0

    # Topics (top 5)
    topics = []
    for t in (item.get("topics") or [])[:5]:
        if isinstance(t, dict) and t.get("display_name"):
            topics.append({
                "term": (t.get("display_name") or "").strip(),
                "score": t.get("score"),
                "count": t.get("count"),
            })

    # Affiliations (institution history)
    affiliations = []
    for aff in (item.get("affiliations") or [])[:10]:
        inst_obj = aff.get("institution") or {}
        if isinstance(inst_obj, dict) and inst_obj.get("display_name"):
            affiliations.append({
                "name": (inst_obj.get("display_name") or "").strip(),
                "country_code": (inst_obj.get("country_code") or "").strip().upper(),
                "type": (inst_obj.get("type") or "").strip(),
                "years": aff.get("years") or [],
            })

    return {
        "id": aid,
        "display_name": (item.get("display_name") or "").strip(),
        "orcid": orcid_raw,
        "institution": institution,
        "works_count": item.get("works_count") or 0,
        "cited_by_count": item.get("cited_by_count") or 0,
        "h_index": h_index,
        "i10_index": i10_index,
        "topics": topics,
        "affiliations": affiliations,
    }


def _run_chunked_parallel(
    chunks: list[list[str]],
    max_workers: int,
    fetch_chunk: Callable[[int, list[str]], dict[str, _T]],
    *,
    label: str,
) -> dict[str, _T]:
    """Run chunk fetches sequentially or in parallel and merge results."""
    merged: dict[str, _T] = {}
    if not chunks:
        return merged

    workers = max(1, min(int(max_workers or 1), len(chunks)))
    if workers == 1:
        for idx, chunk in enumerate(chunks):
            try:
                merged.update(fetch_chunk(idx, chunk))
            except Exception as e:
                logger.warning("%s failed for chunk %d: %s", label, idx, e)
        return merged

    from concurrent.futures import ThreadPoolExecutor, as_completed

    with ThreadPoolExecutor(max_workers=workers) as pool:
        futures = {
            pool.submit(fetch_chunk, idx, chunk): idx
            for idx, chunk in enumerate(chunks)
        }
        for future in as_completed(futures):
            idx = futures[future]
            try:
                merged.update(future.result())
            except Exception as e:
                logger.warning("%s failed for chunk %d: %s", label, idx, e)
    return merged


def batch_get_author_details(
    openalex_author_ids: list[str],
    batch_size: int = 50,
    max_workers: int = 4,
) -> dict[str, dict[str, object]]:
    """Fetch rich details for multiple OpenAlex authors using pipe filter.

    Uses ``GET /authors?filter=id:A1|A2|...`` to fetch authors
    per request (1 credit each) instead of individual singleton lookups.

    Returns:
        Dict mapping normalized OpenAlex ID → detail dict with keys:
        id, display_name, orcid, institution, works_count, cited_by_count,
        h_index, i10_index, topics, affiliations.
    """
    raw_ids = list({_normalize_openalex_author_id(aid) for aid in openalex_author_ids if aid})
    raw_ids = [aid for aid in raw_ids if aid]
    if not raw_ids:
        return {}

    client = get_client()
    batch_size = max(1, min(batch_size, 50))
    max_workers = max(1, int(max_workers or 1))
    chunks = [raw_ids[i : i + batch_size] for i in range(0, len(raw_ids), batch_size)]

    def _fetch_chunk(chunk_idx: int, chunk_ids: list[str]) -> dict[str, dict[str, object]]:
        def _request(ids: list[str]) -> dict[str, dict[str, object]]:
            pipe_filter = "openalex_id:" + "|".join(
                f"https://openalex.org/{aid}" for aid in ids
            )
            resp = client.get(
                "/authors",
                params={
                    "filter": pipe_filter,
                    "per-page": len(ids),
                    "select": _AUTHORS_SELECT_FIELDS,
                },
                timeout=30,
            )
            if resp.status_code == 200:
                op_stats = getattr(client, "_op_stats", None)
                if op_stats is not None:
                    op_stats.batch_requests += 1
                    op_stats.batch_items += len(ids)
                out: dict[str, dict[str, object]] = {}
                for item in (resp.json() or {}).get("results") or []:
                    detail = _normalize_author_detail(item)
                    aid = detail["id"]
                    if aid:
                        out[aid] = detail
                return out
            if resp.status_code in {400, 414} and len(ids) > 1:
                mid = len(ids) // 2
                left = _request(ids[:mid])
                right = _request(ids[mid:])
                left.update(right)
                return left
            logger.warning(
                "Batch author details returned %d for chunk %d (size=%d)",
                resp.status_code,
                chunk_idx,
                len(ids),
            )
            return {}

        return _request(chunk_ids)

    result = _run_chunked_parallel(
        chunks,
        max_workers,
        _fetch_chunk,
        label="Batch author details",
    )

    logger.info(
        "Batch author details: fetched %d/%d in %d batches",
        len(result), len(raw_ids), (len(raw_ids) + batch_size - 1) // batch_size,
    )
    return result


def batch_fetch_recent_works_for_authors(
    author_openalex_ids: list[str],
    from_year: int | None = None,
    per_author_limit: int = 10,
) -> dict[str, list[dict]]:
    """Fetch recent works for multiple authors in batched queries.

    Uses pipe-separated ``author.id`` filters to resolve many authors in
    far fewer API calls than individual lookups.  Results are mapped back
    to each author by inspecting the ``authorships`` field of each returned
    work.

    Args:
        author_openalex_ids: List of OpenAlex author IDs (bare or URL form).
        from_year: Only include works published from this year onwards.
        per_author_limit: Maximum works to keep per author in the result.

    Returns:
        Dict mapping normalized author ID -> list of raw OpenAlex work dicts.
        Authors with no works are omitted from the result.
    """
    # Deduplicate and normalize IDs
    clean_ids: list[str] = []
    seen: set[str] = set()
    for aid in author_openalex_ids:
        bare = _normalize_openalex_author_id((aid or "").strip())
        if not bare:
            continue
        key = bare.lower()
        if key in seen:
            continue
        seen.add(key)
        clean_ids.append(bare)

    if not clean_ids:
        return {}

    client = get_client()
    # Use smaller batch size than DOI batches because each author can
    # return many works -- keep requests manageable.
    batch_size = 50
    chunks = [clean_ids[i : i + batch_size] for i in range(0, len(clean_ids), batch_size)]

    # Collect all works keyed by author
    author_works: dict[str, list[dict]] = {aid: [] for aid in clean_ids}

    # Include fields the Paper details popup surfaces (abstract, topics,
    # keywords, concepts, OA, language, type, fwci). Without these the feed
    # upsert only persists minimal metadata and "No abstract available" shows
    # up on every feed card. Adding these fields stays within OpenAlex's
    # free tier — it just widens the select on the same request.
    _DISCOVERY_SELECT = (
        "id,doi,display_name,authorships,primary_location,"
        "publication_year,publication_date,cited_by_count,"
        "abstract_inverted_index,topics,concepts,keywords,"
        "type,language,open_access,fwci,referenced_works_count,"
        "is_retracted"
    )

    for chunk_idx, chunk_ids in enumerate(chunks):
        # Build piped author filter
        author_filter = "author.id:" + "|".join(chunk_ids)
        filter_parts = [author_filter]
        if from_year:
            filter_parts.append(f"from_publication_date:{from_year}-01-01")
        filter_str = ",".join(filter_parts)

        # Use cursor pagination to fetch all matching works
        cursor: str | None = "*"
        works_fetched = 0
        max_works = per_author_limit * len(chunk_ids) * 2  # generous upper bound

        while cursor:
            try:
                resp = client.get(
                    "/works",
                    params={
                        "filter": filter_str,
                        "sort": "publication_date:desc",
                        "per-page": 100,
                        "cursor": cursor,
                        "select": _DISCOVERY_SELECT,
                    },
                    timeout=30,
                )
                if resp.status_code != 200:
                    logger.warning(
                        "Batch author works fetch returned %d for chunk %d (size=%d)",
                        resp.status_code,
                        chunk_idx,
                        len(chunk_ids),
                    )
                    break

                op_stats = getattr(client, "_op_stats", None)
                if op_stats is not None:
                    op_stats.batch_requests += 1
                    op_stats.batch_items += len(chunk_ids)

                data = resp.json() or {}
                results = data.get("results") or []

                if not results:
                    break

                for work in results:
                    # Map work to each author from this chunk that appears
                    # in the work's authorships.
                    authorships = work.get("authorships") or []
                    matched_aids: set[str] = set()
                    for authorship in authorships:
                        author_obj = authorship.get("author") or {}
                        work_aid_raw = (author_obj.get("id") or "").strip()
                        if not work_aid_raw:
                            continue
                        work_aid = _normalize_openalex_author_id(work_aid_raw)
                        if work_aid.lower() in seen and work_aid not in matched_aids:
                            matched_aids.add(work_aid)
                    for matched in matched_aids:
                        # Find the canonical-cased ID from clean_ids
                        canonical = matched
                        for cid in chunk_ids:
                            if cid.lower() == matched.lower():
                                canonical = cid
                                break
                        if len(author_works.get(canonical, [])) < per_author_limit:
                            author_works.setdefault(canonical, []).append(work)

                works_fetched += len(results)
                cursor = (data.get("meta") or {}).get("next_cursor")

                # Stop paginating if we have collected enough works or if
                # all authors in this chunk already have per_author_limit.
                if works_fetched >= max_works:
                    break
                all_full = all(
                    len(author_works.get(cid, [])) >= per_author_limit
                    for cid in chunk_ids
                )
                if all_full:
                    break

            except Exception as exc:
                logger.warning(
                    "Batch author works fetch failed for chunk %d: %s",
                    chunk_idx,
                    exc,
                )
                break

    # Remove empty entries
    result = {aid: works for aid, works in author_works.items() if works}
    logger.info(
        "Batch author works: fetched works for %d/%d authors",
        len(result),
        len(clean_ids),
    )
    return result


def search_works_for_title(title: str, per_title: int = 5) -> list[dict]:
    """Search OpenAlex /works for a single title. Returns raw work dicts.

    Thin wrapper for concurrent execution in the bulk resolution pipeline.
    """
    if not (title or "").strip():
        return []
    try:
        client = get_client()
        resp = client.get(
            "/works",
            params={
                "search": title.strip(),
                "per-page": max(1, min(int(per_title), 10)),
                "select": "display_name,cited_by_count,authorships",
            },
            timeout=20,
        )
        if resp.status_code != 200:
            return []
        return (resp.json() or {}).get("results") or []
    except Exception as e:
        logger.warning("Works search failed for '%s': %s", title[:80], e)
        return []


def batch_fetch_works_by_dois(
    dois: list[str],
    batch_size: int = 100,
    max_workers: int = 4,
) -> dict[str, dict]:
    """Fetch multiple works by DOI in batched pipe-filter requests.

    Uses ``GET /works?filter=doi:D1|D2|...|D100`` to resolve up to 100 DOIs
    per request (1 credit each) instead of individual singleton lookups
    (which would also be 0-credit each but incur per-request latency).

    Args:
        dois: List of DOI strings (bare or URL form).
        batch_size: Max DOIs per request (capped at 100 by OpenAlex).

    Returns:
        Dict mapping normalized DOI -> raw OpenAlex work dict.
    """
    from alma.core.utils import normalize_doi as _norm_doi

    clean_dois = []
    seen: set[str] = set()
    for d in dois:
        nd = _norm_doi(d) or (d or "").strip()
        if nd and nd.lower() not in seen:
            seen.add(nd.lower())
            clean_dois.append(nd)

    if not clean_dois:
        return {}

    client = get_client()
    batch_size = max(1, min(batch_size, 100))
    max_workers = max(1, int(max_workers or 1))
    chunks = [clean_dois[i : i + batch_size] for i in range(0, len(clean_dois), batch_size)]

    def _fetch_chunk(chunk_idx: int, chunk_dois: list[str]) -> dict[str, dict]:
        def _request(ids: list[str]) -> dict[str, dict]:
            pipe_filter = "doi:" + "|".join(ids)
            resp = client.get(
                "/works",
                params={
                    "filter": pipe_filter,
                    "per-page": len(ids),
                    "select": _WORKS_SELECT_FIELDS,
                },
                timeout=30,
            )
            if resp.status_code == 200:
                op_stats = getattr(client, "_op_stats", None)
                if op_stats is not None:
                    op_stats.batch_requests += 1
                    op_stats.batch_items += len(ids)
                out: dict[str, dict] = {}
                for work in (resp.json() or {}).get("results") or []:
                    work_doi = _norm_doi(work.get("doi") or "")
                    if work_doi:
                        out[work_doi.lower()] = work
                return out
            if resp.status_code in {400, 414} and len(ids) > 1:
                mid = len(ids) // 2
                left = _request(ids[:mid])
                right = _request(ids[mid:])
                left.update(right)
                return left
            logger.warning(
                "Batch DOI fetch returned %d for chunk %d (size=%d)",
                resp.status_code,
                chunk_idx,
                len(ids),
            )
            return {}

        return _request(chunk_dois)

    result = _run_chunked_parallel(
        chunks,
        max_workers,
        _fetch_chunk,
        label="Batch DOI fetch",
    )

    logger.info(
        "Batch DOI fetch: resolved %d/%d DOIs in %d batches",
        len(result),
        len(clean_dois),
        (len(clean_dois) + batch_size - 1) // batch_size,
    )
    return result


def batch_fetch_works_by_openalex_ids(
    work_ids: list[str],
    batch_size: int = 50,
    max_workers: int = 4,
) -> dict[str, dict]:
    """Fetch multiple works by OpenAlex IDs using piped list filters."""
    clean_ids: list[str] = []
    seen: set[str] = set()
    for wid in work_ids:
        raw = (wid or "").strip()
        if not raw:
            continue
        val = raw.rstrip("/")
        if "openalex.org/" in val.lower():
            val = val.split("/")[-1]
        key = val.lower()
        if not val or key in seen:
            continue
        seen.add(key)
        clean_ids.append(val)
    if not clean_ids:
        return {}

    client = get_client()
    batch_size = max(1, min(batch_size, 50))
    max_workers = max(1, int(max_workers or 1))
    chunks = [clean_ids[i : i + batch_size] for i in range(0, len(clean_ids), batch_size)]

    def _fetch_chunk(chunk_idx: int, chunk_ids: list[str]) -> dict[str, dict]:
        def _request(ids: list[str]) -> dict[str, dict]:
            pipe_filter = "openalex_id:" + "|".join(
                f"https://openalex.org/{wid}" for wid in ids
            )
            resp = client.get(
                "/works",
                params={
                    "filter": pipe_filter,
                    "per-page": len(ids),
                    "select": _WORKS_SELECT_FIELDS,
                },
                timeout=30,
            )
            if resp.status_code == 200:
                op_stats = getattr(client, "_op_stats", None)
                if op_stats is not None:
                    op_stats.batch_requests += 1
                    op_stats.batch_items += len(ids)
                out: dict[str, dict] = {}
                for work in (resp.json() or {}).get("results") or []:
                    work_id = (work.get("id") or "").rstrip("/").split("/")[-1]
                    if work_id:
                        out[work_id] = work
                return out
            if resp.status_code in {400, 414} and len(ids) > 1:
                mid = len(ids) // 2
                left = _request(ids[:mid])
                right = _request(ids[mid:])
                left.update(right)
                return left
            logger.warning(
                "Batch work-id fetch returned %d for chunk %d (size=%d)",
                resp.status_code,
                chunk_idx,
                len(ids),
            )
            return {}

        return _request(chunk_ids)

    result = _run_chunked_parallel(
        chunks,
        max_workers,
        _fetch_chunk,
        label="Batch work-id fetch",
    )
    logger.info(
        "Batch work-id fetch: resolved %d/%d works in %d batches",
        len(result),
        len(clean_ids),
        (len(clean_ids) + batch_size - 1) // batch_size,
    )
    return result


def batch_fetch_referenced_works_for_openalex_ids(
    work_ids: list[str],
    batch_size: int = 25,
    max_workers: int = 4,
) -> dict[str, list[str]]:
    """Fetch referenced OpenAlex work IDs for multiple source works in batches."""
    clean_ids: list[str] = []
    seen: set[str] = set()
    for wid in work_ids:
        value = _normalize_openalex_work_id(wid or "")
        key = value.lower()
        if not value or key in seen:
            continue
        seen.add(key)
        clean_ids.append(value)
    if not clean_ids:
        return {}

    client = get_client()
    batch_size = max(1, min(batch_size, 25))
    max_workers = max(1, int(max_workers or 1))
    chunks = [clean_ids[i : i + batch_size] for i in range(0, len(clean_ids), batch_size)]

    def _fetch_chunk(chunk_idx: int, chunk_ids: list[str]) -> dict[str, list[str]]:
        def _request(ids: list[str]) -> dict[str, list[str]]:
            pipe_filter = "openalex_id:" + "|".join(f"https://openalex.org/{wid}" for wid in ids)
            resp = client.get(
                "/works",
                params={
                    "filter": pipe_filter,
                    "per-page": len(ids),
                    "select": "id,referenced_works",
                },
                timeout=30,
            )
            if resp.status_code == 200:
                out: dict[str, list[str]] = {}
                for work in (resp.json() or {}).get("results") or []:
                    work_id = _normalize_openalex_work_id(str(work.get("id") or ""))
                    if not work_id:
                        continue
                    ref_ids = [
                        _normalize_openalex_work_id(str(ref_id or ""))
                        for ref_id in (work.get("referenced_works") or [])
                    ]
                    out[work_id] = [ref_id for ref_id in ref_ids if ref_id]
                return out
            if resp.status_code in {400, 414} and len(ids) > 1:
                mid = len(ids) // 2
                left = _request(ids[:mid])
                right = _request(ids[mid:])
                left.update(right)
                return left
            logger.warning(
                "Batch referenced-work fetch returned %d for chunk %d (size=%d)",
                resp.status_code,
                chunk_idx,
                len(ids),
            )
            return {}

        return _request(chunk_ids)

    result = _run_chunked_parallel(
        chunks,
        max_workers,
        _fetch_chunk,
        label="Batch referenced-work fetch",
    )
    logger.info(
        "Batch referenced-work fetch: resolved %d/%d source works in %d batches",
        len(result),
        len(clean_ids),
        (len(clean_ids) + batch_size - 1) // batch_size,
    )
    return result


def backfill_missing_publication_references(
    conn: sqlite3.Connection,
    *,
    paper_ids: list[str] | None = None,
    limit: int = 250,
    batch_size: int = 25,
    max_workers: int = 4,
) -> dict[str, int]:
    """Backfill missing `publication_references` rows from OpenAlex in batches."""
    _ensure_schema(conn)

    params: list[object] = []
    where_clauses = [
        "COALESCE(p.openalex_id, '') <> ''",
        "pr.paper_id IS NULL",
    ]
    if paper_ids:
        cleaned_ids = [str(paper_id or "").strip() for paper_id in paper_ids if str(paper_id or "").strip()]
        if not cleaned_ids:
            return {"candidates": 0, "fetched": 0, "papers_updated": 0, "references_inserted": 0}
        placeholders = ",".join("?" for _ in cleaned_ids)
        where_clauses.append(f"p.id IN ({placeholders})")
        params.extend(cleaned_ids)

    sql = f"""
        SELECT p.id, p.openalex_id
        FROM papers p
        LEFT JOIN publication_references pr ON pr.paper_id = p.id
        WHERE {' AND '.join(where_clauses)}
        GROUP BY p.id, p.openalex_id
        ORDER BY COALESCE(p.updated_at, p.created_at, p.publication_date, '') DESC
        LIMIT ?
    """
    params.append(max(1, int(limit or 250)))
    rows = conn.execute(sql, params).fetchall()
    if not rows:
        return {"candidates": 0, "fetched": 0, "papers_updated": 0, "references_inserted": 0}

    openalex_to_papers: dict[str, list[str]] = {}
    for row in rows:
        openalex_id = _normalize_openalex_work_id(str(row["openalex_id"] or ""))
        paper_id = str(row["id"] or "").strip()
        if not openalex_id or not paper_id:
            continue
        openalex_to_papers.setdefault(openalex_id, []).append(paper_id)

    references_by_work = batch_fetch_referenced_works_for_openalex_ids(
        list(openalex_to_papers.keys()),
        batch_size=batch_size,
        max_workers=max_workers,
    )
    papers_updated = 0
    references_inserted = 0
    for openalex_id, paper_group in openalex_to_papers.items():
        referenced_ids = references_by_work.get(openalex_id)
        if referenced_ids is None:
            continue
        for paper_id in paper_group:
            references_inserted += _upsert_referenced_works(conn, paper_id, referenced_ids)
            papers_updated += 1
    conn.commit()
    return {
        "candidates": len(openalex_to_papers),
        "fetched": len(references_by_work),
        "papers_updated": papers_updated,
        "references_inserted": references_inserted,
    }


def materialize_missing_referenced_works(
    conn: sqlite3.Connection,
    *,
    seed_paper_ids: list[str] | None = None,
    seed_statuses: list[str] | None = None,
    limit: int = 250,
    batch_size: int = 50,
    max_workers: int = 4,
) -> dict[str, int]:
    """Fetch and persist referenced works that are still missing from local papers.

    This turns bare `publication_references` edges into locally materialized
    papers with authorships/topics so downstream adjacency logic can operate on a
    richer local graph instead of depending on the cited work already being
    present in `papers`.
    """
    _ensure_schema(conn)
    default_statuses = seed_statuses or ["library"]
    params: list[object] = []
    where_clauses = [
        "COALESCE(pr.referenced_work_id, '') <> ''",
        "target.id IS NULL",
    ]

    if seed_paper_ids:
        clean_ids = [str(paper_id or "").strip() for paper_id in seed_paper_ids if str(paper_id or "").strip()]
        if not clean_ids:
            return {"candidates": 0, "fetched": 0, "materialized": 0}
        placeholders = ",".join("?" for _ in clean_ids)
        where_clauses.append(f"seed.id IN ({placeholders})")
        params.extend(clean_ids)
    elif default_statuses:
        placeholders = ",".join("?" for _ in default_statuses)
        where_clauses.append(f"seed.status IN ({placeholders})")
        params.extend(default_statuses)

    sql = f"""
        SELECT DISTINCT pr.referenced_work_id
        FROM publication_references pr
        JOIN papers seed ON seed.id = pr.paper_id
        LEFT JOIN papers target
          ON lower(trim(target.openalex_id)) = lower(trim(pr.referenced_work_id))
        WHERE {' AND '.join(where_clauses)}
        ORDER BY COALESCE(seed.updated_at, seed.created_at, seed.publication_date, '') DESC
        LIMIT ?
    """
    params.append(max(1, int(limit or 250)))

    try:
        rows = conn.execute(sql, params).fetchall()
    except sqlite3.OperationalError:
        return {"candidates": 0, "fetched": 0, "materialized": 0}

    target_ids = [
        _normalize_openalex_work_id(str((row["referenced_work_id"] if isinstance(row, sqlite3.Row) else row[0]) or ""))
        for row in rows
    ]
    target_ids = [target_id for target_id in target_ids if target_id]
    if not target_ids:
        return {"candidates": 0, "fetched": 0, "materialized": 0}

    raw_works = batch_fetch_works_by_openalex_ids(
        target_ids,
        batch_size=batch_size,
        max_workers=max_workers,
    )
    flags = _ensure_schema(conn)
    materialized = 0
    for work_id in target_ids:
        raw_work = raw_works.get(work_id)
        if not raw_work:
            continue
        normalized = _normalize_work(raw_work)
        if _upsert_single_paper(conn, normalized, flags):
            materialized += 1
    conn.commit()
    return {
        "candidates": len(target_ids),
        "fetched": len(raw_works),
        "materialized": materialized,
    }


def resolve_openalex_candidates_from_scholar(
    scholar_id: str,
    mailto: Optional[str] = None,
) -> List[Dict[str, object]]:
    """Resolve OpenAlex author candidates using Google Scholar metadata (name, affiliation, and one work).

    Strategy:
    1. Use `scholarly` to fetch author name, affiliation, and at least one publication title.
    2. Query OpenAlex `works` endpoint with the publication title.
    3. Collect authors from matching works whose names align with the Scholar author's name.
    4. For each candidate OpenAlex author, fetch ORCID and last_known_institution.
    5. Compute a simple confidence score and return candidates.

    Returns a list of candidates sorted by score desc.
    """
    name = None
    affiliation = None
    sample_title = None
    candidates: Dict[str, Dict[str, object]] = {}

    # Step 1: fetch from Scholar
    try:
        try:
            from scholarly import scholarly as _sch
        except Exception as e:  # scholarly may be missing
            logger.warning(f"scholarly unavailable: {e}")
            _sch = None
        if _sch is not None:
            a = _sch.search_author_id(scholar_id)
            a = _sch.fill(a)
            name = (a or {}).get("name")
            affiliation = (a or {}).get("affiliation") or (a or {}).get("organization")
            pubs = (a or {}).get("publications") or []
            if isinstance(pubs, list) and pubs:
                # Prefer the most recent filled pub title; otherwise first available title
                try:
                    # Sometimes need to fill a publication to get full title; but we avoid network here
                    sample_title = (pubs[0] or {}).get("bib", {}).get("title")
                except Exception:
                    sample_title = None
    except Exception as e:
        logger.warning(f"Scholar fetch failed for {scholar_id}: {e}")

    # No sample title? Still try author search fallback
    session = _session(mailto)
    if sample_title:
        try:
            params = {
                "search": sample_title,
                "per-page": 25,
                "select": ",".join([
                    "id",
                    "display_name",
                    "authorships",
                    "primary_location",
                    "doi",
                ]),
            }
            r = session.get("/works", params=params, timeout=30)
            r.raise_for_status()
            works = (r.json() or {}).get("results", []) or []
            for w in works:
                w_title = (w or {}).get("display_name") or ""
                # Accept loose match if title contains our sample substring (case-insensitive)
                if sample_title and sample_title.lower() not in (w_title or "").lower():
                    continue
                authorships = (w or {}).get("authorships") or []
                for a in authorships:
                    auth = (a or {}).get("author") or {}
                    aid = (auth.get("id") or "").strip()
                    aname = (auth.get("display_name") or "").strip()
                    if not aid or not aname:
                        continue

                    # FILTER: Only include authors whose name matches the Scholar author
                    # Skip co-authors with unrelated names
                    if not name:
                        continue

                    scholar_last = name.split()[-1].lower() if name else ""
                    candidate_last = aname.split()[-1].lower() if aname else ""

                    # Skip if last names don't match at all
                    if scholar_last and candidate_last and scholar_last != candidate_last:
                        continue

                    # Name alignment scoring (only for candidates that pass the filter)
                    score = 1
                    if name and aname and aname.lower() == name.lower():
                        score += 3  # Exact match
                    elif name and aname and (name.split()[0].lower() in aname.lower()):
                        score += 2  # First name match
                    # Institution overlap heuristic
                    insts = (a.get("institutions") or [])
                    inst_names = [ (i.get("display_name") or "").strip() for i in insts if isinstance(i, dict) ]
                    inst_hit = False
                    if affiliation:
                        for nm in inst_names:
                            if nm and affiliation.lower() in nm.lower():
                                inst_hit = True
                                break
                    if inst_hit:
                        score += 1
                    # Matched work is the one we used
                    matched = {
                        "title": w_title,
                        "id": (w or {}).get("id"),
                        "doi": (w or {}).get("doi"),
                        "url": ((w or {}).get("primary_location") or {}).get("landing_page_url") or ((w or {}).get("id") or ""),
                    }
                    if aid not in candidates:
                        candidates[aid] = {
                            "openalex_id": aid,
                            "display_name": aname,
                            "orcid": None,
                            "institution": inst_names[0] if inst_names else "",
                            "matched_work": matched,
                            "score": score,
                        }
                    else:
                        # Boost score if we see the same author again
                        candidates[aid]["score"] = int(candidates[aid]["score"]) + score
        except Exception as e:
            logger.warning(f"OpenAlex works search failed for '{sample_title}': {e}")

    # Fallback: author search by name only
    if not candidates and name:
        try:
            params = {"search": name, "per-page": 10, "select": "id,display_name,orcid,last_known_institution"}
            r = session.get("/authors", params=params, timeout=20)
            if r.status_code == 200:
                results = (r.json() or {}).get("results", [])
                for rec in results:
                    aid = (rec or {}).get("id")
                    aname = (rec or {}).get("display_name")
                    inst = ((rec or {}).get("last_known_institution") or {}).get("display_name") if isinstance(rec, dict) else None
                    candidates[aid] = {
                        "openalex_id": aid,
                        "display_name": aname,
                        "orcid": (rec or {}).get("orcid"),
                        "institution": inst or "",
                        "matched_work": None,
                        "score": 1,
                    }
        except Exception as e:
            logger.warning(f"OpenAlex author search failed for '{name}': {e}")

    out: List[Dict[str, object]] = []
    for aid, cand in candidates.items():
        if not cand.get("orcid") or not cand.get("institution"):
            try:
                normalized_aid = _normalize_openalex_author_id(aid)
                details_resp = session.get(f"/authors/{normalized_aid}", timeout=20)
                if details_resp.status_code == 200:
                    details = details_resp.json() or {}
                    cand["orcid"] = cand.get("orcid") or details.get("orcid")
                    if not cand.get("institution"):
                        cand["institution"] = (
                            ((details.get("last_known_institution") or {}).get("display_name"))
                            or cand.get("institution")
                            or ""
                        )
            except Exception:
                pass
        cand["openalex_url"] = f"https://openalex.org/{_normalize_openalex_author_id(aid)}"
        out.append(cand)

    out.sort(key=lambda x: int(x.get("score") or 0), reverse=True)
    # Return only top 3 candidates to avoid overwhelming the user with irrelevant matches
    return out[:3]


# Standard select fields for OpenAlex /works requests.
# Covers all data we extract in _normalize_work().
_WORKS_SELECT_FIELDS = ",".join([
    "id", "doi", "display_name", "publication_year", "publication_date",
    "abstract_inverted_index", "primary_location", "cited_by_count",
    "authorships", "topics", "concepts",
    # Extra enrichment fields for stats & insights
    "type", "language", "open_access", "is_retracted",
    "cited_by_percentile_year", "fwci", "counts_by_year",
    "keywords", "referenced_works", "referenced_works_count", "biblio",
    "sustainable_development_goals",
    "institutions_distinct_count", "countries_distinct_count",
])


def _normalize_work(w: Dict) -> Dict:
    """Normalize a raw OpenAlex work dict into a standard internal format.

    Extracts title, authors, year, abstract, citations, journal, doi, url,
    topics, institutions, open access info, and more from the raw API
    response.  Used by the enrichment pipeline and candidate resolution.
    """
    title = (w or {}).get("display_name") or ""
    year = (w or {}).get("publication_year")
    pub_date = (w or {}).get("publication_date")
    abstract = _decode_abstract((w or {}).get("abstract_inverted_index"))
    primary_location = (w or {}).get("primary_location") or {}
    url = (
        primary_location.get("landing_page_url")
        or primary_location.get("pdf_url")
        or (w or {}).get("id")
    )
    src = (primary_location.get("source") or {}) if isinstance(primary_location, dict) else {}
    journal = src.get("display_name")
    doi = _normalize_doi((w or {}).get("doi"))
    cites = (w or {}).get("cited_by_count")

    # Authors (flat string for backward compat)
    authorships = (w or {}).get("authorships") or []
    auths = ", ".join(
        (a.get("author") or {}).get("display_name", "") for a in authorships
    )

    # Institutions (from authorships)
    insts = []
    for a in authorships:
        for inst in a.get("institutions") or []:
            inst_id = (inst.get("id") or "").strip()
            inst_name = (inst.get("display_name") or "").strip()
            ccode = (inst.get("country_code") or "").strip().upper()
            if not inst_id and not inst_name and not ccode:
                continue
            insts.append({"id": inst_id, "name": inst_name, "country_code": ccode})

    # Topics (prefer `topics`, fallback to `concepts`)
    topics_list = []
    raw_topics = (w or {}).get("topics") or []
    if isinstance(raw_topics, list) and raw_topics:
        topics_list = [
            {
                "term": (t.get("display_name") or "").strip(),
                "score": t.get("score"),
                "domain": (t.get("domain") or {}).get("display_name") if isinstance(t.get("domain"), dict) else "",
                "field": (t.get("field") or {}).get("display_name") if isinstance(t.get("field"), dict) else "",
                "subfield": (t.get("subfield") or {}).get("display_name") if isinstance(t.get("subfield"), dict) else "",
            }
            for t in raw_topics
            if isinstance(t, dict) and (t.get("display_name") or "").strip()
        ]
    elif isinstance((w or {}).get("concepts"), list):
        raw_concepts = (w or {}).get("concepts") or []
        topics_list = [
            {
                "term": (c.get("display_name") or "").strip(),
                "score": c.get("score"),
            }
            for c in raw_concepts
            if isinstance(c, dict) and (c.get("display_name") or "").strip()
        ]

    # Structured authorships (preserves OpenAlex author IDs, ORCIDs, etc.)
    structured_authorships = []
    for a in authorships:
        author_obj = a.get("author") or {}
        raw_aid = (author_obj.get("id") or "").strip()
        if not raw_aid:
            continue
        structured_authorships.append({
            "openalex_id": _normalize_openalex_author_id(raw_aid),
            "display_name": (author_obj.get("display_name") or "").strip(),
            "orcid": (author_obj.get("orcid") or "").strip(),
            "position": (a.get("author_position") or "").strip(),
            "is_corresponding": bool(a.get("is_corresponding")),
            "institutions": [
                {
                    "id": (si.get("id") or "").strip(),
                    "name": (si.get("display_name") or "").strip(),
                    "ror": (si.get("ror") or "").strip(),
                    "country_code": (si.get("country_code") or "").strip().upper(),
                    "type": (si.get("type") or "").strip(),
                }
                for si in (a.get("institutions") or [])
                if (si.get("id") or si.get("display_name"))
            ],
        })

    # Open access info
    oa_raw = (w or {}).get("open_access") or {}
    open_access = {
        "is_oa": bool(oa_raw.get("is_oa")),
        "oa_status": (oa_raw.get("oa_status") or "closed").strip(),
        "oa_url": (oa_raw.get("oa_url") or "").strip(),
    } if isinstance(oa_raw, dict) else {"is_oa": False, "oa_status": "closed", "oa_url": ""}

    # Biblio (volume, issue, pages)
    biblio_raw = (w or {}).get("biblio") or {}
    biblio = {
        "volume": (biblio_raw.get("volume") or "").strip(),
        "issue": (biblio_raw.get("issue") or "").strip(),
        "first_page": (biblio_raw.get("first_page") or "").strip(),
        "last_page": (biblio_raw.get("last_page") or "").strip(),
    } if isinstance(biblio_raw, dict) else {}

    # Keywords
    keywords = [
        (kw.get("keyword") or kw.get("display_name") or "").strip()
        for kw in ((w or {}).get("keywords") or [])
        if isinstance(kw, dict) and (kw.get("keyword") or kw.get("display_name") or "").strip()
    ]

    # Citation percentile
    pctile_raw = (w or {}).get("cited_by_percentile_year") or {}
    cited_by_percentile = {
        "min": pctile_raw.get("min"),
        "max": pctile_raw.get("max"),
    } if isinstance(pctile_raw, dict) else {}

    return {
        "title": title,
        "authors": auths,
        "authorships": structured_authorships,
        "abstract": abstract or "",
        "year": year,
        "publication_date": pub_date,
        "num_citations": cites,
        "journal": journal or "",
        "pub_url": url or "",
        "doi": doi or "",
        "topics": topics_list,
        "institutions": insts,
        # Extended fields
        "openalex_id": (w or {}).get("id") or "",
        "type": (w or {}).get("type") or "",
        "language": (w or {}).get("language") or "",
        "open_access": open_access,
        "is_retracted": bool((w or {}).get("is_retracted")),
        "fwci": (w or {}).get("fwci"),
        "cited_by_percentile": cited_by_percentile,
        "counts_by_year": (w or {}).get("counts_by_year") or [],
        "keywords": keywords,
        "referenced_works": (
            [
                _normalize_openalex_work_id(str(ref_id or ""))
                for ref_id in ((w or {}).get("referenced_works") or [])
                if _normalize_openalex_work_id(str(ref_id or ""))
            ]
            if "referenced_works" in (w or {})
            else None
        ),
        "referenced_works_count": (w or {}).get("referenced_works_count") or 0,
        "biblio": biblio,
        "institutions_distinct_count": (w or {}).get("institutions_distinct_count") or 0,
        "countries_distinct_count": (w or {}).get("countries_distinct_count") or 0,
        "sdgs": [
            {
                "display_name": (s.get("display_name") or "").strip(),
                "score": s.get("score"),
            }
            for s in ((w or {}).get("sustainable_development_goals") or [])
            if isinstance(s, dict) and (s.get("display_name") or "").strip()
        ],
    }


def resolve_openalex_candidates_from_metadata(
    author_name: str,
    sample_titles: List[str],
    per_title: int = 5,
    prefetched_title_results: Optional[Dict[str, List[Dict[str, object]]]] = None,
    prefetched_author_details: Optional[Dict[str, Dict[str, object]]] = None,
) -> List[Dict[str, object]]:
    """Resolve OpenAlex author candidates from local author name + paper titles."""
    name = (author_name or "").strip()
    titles = [t.strip() for t in (sample_titles or []) if (t or "").strip()][:3]
    if not name or not titles:
        return []

    def _title_similarity(a: str, b: str) -> float:
        an = _normalize_text(a)
        bn = _normalize_text(b)
        if not an or not bn:
            return 0.0
        if an == bn:
            return 1.0
        if an in bn or bn in an:
            return 0.9
        return SequenceMatcher(None, an, bn).ratio()

    def _name_match_strength(target_name: str, candidate_name: str) -> float:
        t = _normalize_text(target_name)
        c = _normalize_text(candidate_name)
        if not t or not c:
            return 0.0
        if t == c:
            return 1.0
        t_parts = t.split()
        c_parts = c.split()
        if not t_parts or not c_parts:
            return 0.0
        t_last = t_parts[-1]
        c_last = c_parts[-1]
        if t_last != c_last:
            return 0.0
        t_first = t_parts[0]
        c_first = c_parts[0]
        if t_first == c_first:
            return 0.9
        if t_first[:1] and c_first[:1] and t_first[:1] == c_first[:1]:
            return 0.7
        return 0.55

    candidates: Dict[str, Dict[str, object]] = {}
    try:
        client = None
        for title in titles:
            works: List[Dict[str, object]] = []
            if prefetched_title_results is not None:
                works = prefetched_title_results.get(title) or []
            else:
                if client is None:
                    client = get_client()
                resp = client.get(
                    "/works",
                    params={
                        "search": title,
                        "per-page": max(1, min(int(per_title), 10)),
                        "select": "display_name,cited_by_count,authorships",
                    },
                    timeout=20,
                )
                if resp.status_code != 200:
                    continue
                works = (resp.json() or {}).get("results") or []
            for work in works:
                work_title = (work.get("display_name") or "").strip()
                sim = _title_similarity(title, work_title)
                if sim < 0.55:
                    continue
                title_points = max(1.0, round(sim * 6, 2))
                cites = int((work.get("cited_by_count") or 0))
                cite_points = min(2.0, cites / 400.0)
                for auth in (work.get("authorships") or []):
                    author = (auth or {}).get("author") or {}
                    aid_raw = (author.get("id") or "").strip()
                    aname = (author.get("display_name") or "").strip()
                    aid = _normalize_openalex_author_id(aid_raw)
                    if not aid or not aname:
                        continue
                    name_strength = _name_match_strength(name, aname)
                    if name_strength <= 0:
                        continue
                    score_add = title_points + cite_points + (name_strength * 4.0)
                    bucket = candidates.setdefault(
                        aid,
                        {
                            "openalex_id": aid,
                            "display_name": aname,
                            "score": 0.0,
                            "matches": 0,
                            "matched_titles": [],
                            "orcid": None,
                            "institution": "",
                        },
                    )
                    bucket["score"] = float(bucket["score"]) + score_add
                    bucket["matches"] = int(bucket["matches"]) + 1
                    mt = bucket["matched_titles"]
                    if isinstance(mt, list) and title not in mt:
                        mt.append(title)
    except Exception as e:
        logger.warning("OpenAlex metadata-based candidate resolution failed for '%s': %s", author_name, e)

    if not candidates:
        return []

    # Enrich only top-3 candidates (after scoring) with ORCID/institution
    # details to avoid wasting API calls on low-score matches.
    out = sorted(candidates.values(), key=lambda x: float(x.get("score") or 0.0), reverse=True)[:5]
    # Use batch fetch when we have a live client (normal path), fall back to
    # individual lookups when prefetched results are provided (test/offline).
    detail_ids = [str(c.get("openalex_id") or "") for c in out[:3] if c.get("openalex_id")]
    if detail_ids and prefetched_author_details is not None:
        for cand in out[:3]:
            aid = _normalize_openalex_author_id(str(cand.get("openalex_id") or ""))
            det = prefetched_author_details.get(aid) or {}
            cand["orcid"] = det.get("orcid")
            cand["institution"] = det.get("institution") or ""
    elif detail_ids and prefetched_title_results is None:
        # Normal path: batch-fetch in 1 API call instead of N
        try:
            details_map = batch_get_author_details(detail_ids)
            for cand in out[:3]:
                aid = _normalize_openalex_author_id(str(cand.get("openalex_id") or ""))
                det = details_map.get(aid) or {}
                cand["orcid"] = det.get("orcid")
                cand["institution"] = det.get("institution") or ""
        except Exception:
            # Graceful fallback to individual lookups
            for cand in out[:3]:
                try:
                    det = _get_author_details(str(cand.get("openalex_id") or "")) or {}
                    cand["orcid"] = det.get("orcid")
                    cand["institution"] = det.get("institution") or ""
                except Exception:
                    pass
    else:
        # Prefetched path or no IDs: use per-candidate detail lookup
        for cand in out[:3]:
            try:
                det = _get_author_details(str(cand.get("openalex_id") or "")) or {}
                cand["orcid"] = det.get("orcid")
                cand["institution"] = det.get("institution") or ""
            except Exception:
                pass
    return out
