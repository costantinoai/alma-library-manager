"""Fetch candidate papers from the Semantic Scholar Graph API."""

from __future__ import annotations

import logging
import sqlite3
from datetime import datetime
from typing import Any, Dict, List, Optional

from alma.ai.embedding_sources import EMBEDDING_SOURCE_SEMANTIC_SCHOLAR
from alma.core.http_sources import get_source_http_client
from alma.core.utils import normalize_doi
from alma.core.vector_blob import encode_vector

logger = logging.getLogger(__name__)

S2_SPECTER2_MODEL = "allenai/specter2_base"

FIELDS = (
    "paperId,corpusId,title,authors,year,journal,externalIds,citationCount,"
    "influentialCitationCount,tldr,"
    "abstract,url,publicationDate,embedding.specter_v2"
)
AUTHOR_FIELDS = "authorId,name,aliases,affiliations,homepage,url,externalIds,paperCount,citationCount,hIndex"


class SemanticScholarBatchError(RuntimeError):
    """Raised when a strict Semantic Scholar batch request fails."""

    def __init__(self, message: str, *, status_code: int | None = None) -> None:
        super().__init__(message)
        self.status_code = status_code


def _chunked(items: list[str], size: int) -> list[list[str]]:
    return [items[idx:idx + size] for idx in range(0, len(items), size)]


def _coerce_batch_rows(payload: Any) -> list[dict]:
    if isinstance(payload, list):
        return [row for row in payload if isinstance(row, dict)]
    if isinstance(payload, dict):
        for key in ("data", "items", "papers", "authors"):
            value = payload.get(key)
            if isinstance(value, list):
                return [row for row in value if isinstance(row, dict)]
    return []


def _coerce_batch_rows_with_positions(payload: Any) -> list[tuple[int, dict]]:
    """Return batch rows with their original response index preserved."""
    rows: Any = payload
    if isinstance(payload, dict):
        rows = None
        for key in ("data", "items", "papers", "authors"):
            value = payload.get(key)
            if isinstance(value, list):
                rows = value
                break
    if not isinstance(rows, list):
        return []
    return [(idx, row) for idx, row in enumerate(rows) if isinstance(row, dict)]


def extract_specter2_vector(paper: dict) -> list[float] | None:
    """Return a Semantic Scholar SPECTER2 vector when present."""
    embedding = paper.get("embedding")
    raw: Any = None
    if isinstance(embedding, dict):
        raw = embedding.get("vector") or embedding.get("specter_v2")
    if raw is None:
        raw = paper.get("embedding.specter_v2")
    if isinstance(raw, dict):
        raw = raw.get("vector")
    if not isinstance(raw, list):
        return None
    try:
        vector = [float(value) for value in raw]
    except (TypeError, ValueError):
        return None
    return vector or None


def upsert_specter2_embedding(
    conn: sqlite3.Connection,
    paper_id: str,
    candidate: dict,
) -> bool:
    """INSERT OR IGNORE the SPECTER2 vector for ``paper_id``.

    Reads ``candidate["specter2_embedding"]`` (list-of-floats produced
    by ``extract_specter2_vector``) and stores it via the canonical
    float16 blob encoder so every writer path agrees with the reader's
    decode dtype. Returns ``True`` on insert, ``False`` when the
    candidate has no vector or a row already exists for this
    (paper_id, model) pair.
    """
    vector = candidate.get("specter2_embedding")
    if not isinstance(vector, list) or not vector:
        return False
    try:
        blob = encode_vector(vector)
    except (TypeError, ValueError):
        return False
    cursor = conn.execute(
        """
        INSERT OR IGNORE INTO publication_embeddings
            (paper_id, embedding, model, source, created_at)
        VALUES (?, ?, ?, ?, ?)
        """,
        (
            paper_id,
            blob,
            S2_SPECTER2_MODEL,
            EMBEDDING_SOURCE_SEMANTIC_SCHOLAR,
            datetime.utcnow().isoformat(),
        ),
    )
    return cursor.rowcount > 0


def fetch_papers_batch(
    paper_ids: list[str],
    *,
    fields: str = FIELDS,
    batch_size: int = 100,
    raise_on_error: bool = False,
) -> dict[str, dict]:
    """Fetch multiple Semantic Scholar papers in batch by paperId."""
    normalized_ids = [str(item or "").strip() for item in paper_ids if str(item or "").strip()]
    if not normalized_ids:
        return {}

    client = get_source_http_client("semantic_scholar")
    out: dict[str, dict] = {}
    for chunk in _chunked(list(dict.fromkeys(normalized_ids)), max(1, min(int(batch_size or 100), 500))):
        try:
            resp = client.post(
                "/paper/batch",
                params={"fields": fields},
                json={"ids": chunk},
                timeout=20,
            )
            if resp.status_code != 200:
                message = (
                    f"Semantic Scholar paper batch returned HTTP {resp.status_code} "
                    f"for {len(chunk)} ids"
                )
                if raise_on_error:
                    raise SemanticScholarBatchError(message, status_code=resp.status_code)
                logger.debug(
                    "Semantic Scholar paper batch returned HTTP %d for %d ids",
                    resp.status_code,
                    len(chunk),
                )
                continue
            for idx, row in _coerce_batch_rows_with_positions(resp.json() or {}):
                paper_id = str(row.get("paperId") or "").strip()
                if paper_id:
                    if idx < len(chunk):
                        row = dict(row)
                        row["_requested_id"] = chunk[idx]
                    out[paper_id] = row
        except Exception as exc:
            if raise_on_error:
                if isinstance(exc, SemanticScholarBatchError):
                    raise
                raise SemanticScholarBatchError(str(exc)) from exc
            logger.debug("Semantic Scholar paper batch failed: %s", exc)
    return out


def fetch_authors_batch(
    author_ids: list[str],
    *,
    fields: str = AUTHOR_FIELDS,
    batch_size: int = 100,
) -> dict[str, dict]:
    """Fetch multiple Semantic Scholar authors in batch by authorId."""
    normalized_ids = [str(item or "").strip() for item in author_ids if str(item or "").strip()]
    if not normalized_ids:
        return {}

    client = get_source_http_client("semantic_scholar")
    out: dict[str, dict] = {}
    for chunk in _chunked(list(dict.fromkeys(normalized_ids)), max(1, min(int(batch_size or 100), 500))):
        try:
            resp = client.post(
                "/author/batch",
                params={"fields": fields},
                json={"ids": chunk},
                timeout=20,
            )
            if resp.status_code != 200:
                logger.debug(
                    "Semantic Scholar author batch returned HTTP %d for %d ids",
                    resp.status_code,
                    len(chunk),
                )
                continue
            for row in _coerce_batch_rows(resp.json() or {}):
                author_id = str(row.get("authorId") or "").strip()
                if author_id:
                    out[author_id] = row
        except Exception as exc:
            logger.debug("Semantic Scholar author batch failed: %s", exc)
    return out


def _s2_to_candidate(paper: dict, score: float = 0.5) -> Optional[dict]:
    """Convert a Semantic Scholar paper dict to the ALMa candidate format.

    Returns ``None`` when the paper lacks a title (skip silently).
    """
    title = (paper.get("title") or "").strip()
    if not title:
        return None

    authors_list = paper.get("authors") or []
    authors = ", ".join(
        (a.get("name") or "").strip()
        for a in authors_list
        if (a.get("name") or "").strip()
    )

    ext_ids: dict = paper.get("externalIds") or {}
    doi_raw = ext_ids.get("DOI") or ""
    doi = normalize_doi(doi_raw) or doi_raw

    url = (paper.get("url") or "").strip()
    if not url and doi:
        url = f"https://doi.org/{doi}"

    journal_obj = paper.get("journal")
    journal = ""
    if isinstance(journal_obj, dict):
        journal = (journal_obj.get("name") or "").strip()

    # S2 `tldr` is a 1-2 sentence AI summary (dense coverage in CS +
    # biomed, sparse elsewhere). Returned as `{model, text}`; we only
    # persist the text.
    tldr_obj = paper.get("tldr")
    tldr_text = ""
    if isinstance(tldr_obj, dict):
        tldr_text = (tldr_obj.get("text") or "").strip()

    influential = paper.get("influentialCitationCount")
    try:
        influential_count = int(influential) if influential is not None else 0
    except (TypeError, ValueError):
        influential_count = 0

    return {
        "semantic_scholar_id": (paper.get("paperId") or "").strip(),
        "semantic_scholar_corpus_id": str(paper.get("corpusId") or "").strip(),
        "specter2_embedding": extract_specter2_vector(paper),
        "specter2_model": S2_SPECTER2_MODEL,
        "title": title,
        "authors": authors,
        "year": paper.get("year"),
        "publication_date": (paper.get("publicationDate") or "").strip() or None,
        "journal": journal,
        "doi": doi,
        "url": url,
        "cited_by_count": paper.get("citationCount") or 0,
        "influential_citation_count": influential_count,
        "tldr": tldr_text,
        "abstract": (paper.get("abstract") or "").strip(),
        "score": round(float(score), 4),
    }


# ------------------------------------------------------------------
# Public retrieval functions
# ------------------------------------------------------------------


def search_papers(query: str, limit: int = 20) -> List[dict]:
    """Search Semantic Scholar by free-text query.

    Args:
        query: Search string (typically topic keywords).
        limit: Maximum number of results.

    Returns:
        List of candidate dicts ready for ``_merge_candidate``.
    """
    if not (query or "").strip():
        return []

    try:
        resp = get_source_http_client("semantic_scholar").get(
            "/paper/search",
            params={
                "query": query.strip(),
                "limit": min(limit, 100),
                "fields": FIELDS,
            },
            timeout=15,
        )
        if resp.status_code != 200:
            logger.debug(
                "Semantic Scholar search returned HTTP %d for query '%s'",
                resp.status_code,
                query[:80],
            )
            return []

        papers = (resp.json() or {}).get("data") or []
        results: List[dict] = []
        total = max(len(papers), 1)
        for i, p in enumerate(papers):
            score = round(max(0.0, 1.0 - (i / total)), 4)
            candidate = _s2_to_candidate(p, score=score)
            if candidate:
                results.append(candidate)
        return results

    except Exception as exc:
        logger.warning("Semantic Scholar search failed: %s", exc)
        return []


def search_papers_bulk(
    query: str,
    *,
    limit: int = 20,
    from_year: int | None = None,
    fields_of_study: Optional[list[str]] = None,
    publication_types: Optional[list[str]] = None,
    open_access_pdf: bool = False,
) -> List[dict]:
    """Search Semantic Scholar using the bulk search endpoint.

    This path is intended for monitor refreshes and other non-interactive
    workflows where we want fewer singleton search calls and broader result
    slices than the interactive search API.

    Optional server-side filters (T12, 2026-04-25) — pass-throughs to S2's
    `/paper/search/bulk` endpoint so the external source returns a tighter
    slice and the downstream scoring loop doesn't waste work on obviously
    off-topic candidates:

    * ``fields_of_study`` — list of S2 top-level field names
      (e.g. ``["Computer Science", "Mathematics"]``).  Joined with commas.
    * ``publication_types`` — list of S2 publication types
      (e.g. ``["JournalArticle", "Review"]``).  Joined with commas.
    * ``open_access_pdf`` — when True, restrict to papers with an
      accessible open-access PDF.  Flag-only parameter (no value).

    All three are no-ops when left at their default (None / False), so
    existing callers see no change.
    """
    if not (query or "").strip():
        return []

    params: dict[str, Any] = {
        "query": query.strip(),
        "limit": min(limit, 100),
        "fields": FIELDS,
    }
    # Only emit filter params when non-empty so the URL stays short in
    # the common "no filter" case.  S2 rejects requests that send
    # `fieldsOfStudy=` (empty) outright.
    fos = [str(f).strip() for f in (fields_of_study or []) if str(f).strip()]
    if fos:
        params["fieldsOfStudy"] = ",".join(fos)
    pts = [str(p).strip() for p in (publication_types or []) if str(p).strip()]
    if pts:
        params["publicationTypes"] = ",".join(pts)
    if open_access_pdf:
        params["openAccessPdf"] = ""

    try:
        resp = get_source_http_client("semantic_scholar").get(
            "/paper/search/bulk",
            params=params,
            timeout=20,
        )
        if resp.status_code != 200:
            logger.debug(
                "Semantic Scholar bulk search returned HTTP %d for query '%s'",
                resp.status_code,
                query[:80],
            )
            return search_papers(query, limit=limit)

        papers = (resp.json() or {}).get("data") or []
        hydrated_by_id = fetch_papers_batch(
            [
                str((paper or {}).get("paperId") or "").strip()
                for paper in papers[: max(1, min(limit * 2, 100))]
            ],
            fields=FIELDS,
        )
        filtered: List[dict] = []
        total = max(len(papers), 1)
        for i, paper in enumerate(papers):
            paper_id = str((paper or {}).get("paperId") or "").strip()
            if paper_id and paper_id in hydrated_by_id:
                paper = hydrated_by_id[paper_id]
            if from_year is not None:
                year = paper.get("year")
                try:
                    if year is not None and int(year) < int(from_year):
                        continue
                except (TypeError, ValueError):
                    pass
            score = round(max(0.0, 1.0 - (i / total)), 4)
            candidate = _s2_to_candidate(paper, score=score)
            if candidate:
                filtered.append(candidate)
        return filtered[: max(1, limit)]
    except Exception as exc:
        logger.warning("Semantic Scholar bulk search failed: %s", exc)
        return search_papers(query, limit=limit)


def fetch_related_papers(doi: str, limit: int = 20) -> List[dict]:
    """Fetch papers that cite or are referenced by a paper identified by DOI.

    First resolves the DOI to a Semantic Scholar paper ID, then fetches
    both references and citations, merging them into one list.

    Args:
        doi: Bare DOI string (e.g. ``10.1234/example``).
        limit: Maximum total results (split between references and citations).

    Returns:
        List of candidate dicts.
    """
    doi = (doi or "").strip()
    if not doi:
        return []

    # Resolve DOI to S2 paper ID
    try:
        resp = get_source_http_client("semantic_scholar").get(
            f"/paper/DOI:{doi}",
            params={"fields": "paperId"},
            timeout=15,
        )
        if resp.status_code != 200:
            logger.debug(
                "Semantic Scholar DOI resolve failed for '%s': HTTP %d",
                doi,
                resp.status_code,
            )
            return []
        paper_id = (resp.json() or {}).get("paperId")
        if not paper_id:
            return []
    except Exception as exc:
        logger.debug("Semantic Scholar DOI resolve failed for '%s': %s", doi, exc)
        return []

    half_limit = max(limit // 2, 5)
    relation_items: list[tuple[str, float]] = []
    client = get_source_http_client("semantic_scholar")

    # Fetch reference/citation paper IDs first, then hydrate with one batch call.
    for relation_name, path, key in (
        ("references", f"/paper/{paper_id}/references", "citedPaper"),
        ("citations", f"/paper/{paper_id}/citations", "citingPaper"),
    ):
        try:
            resp = client.get(
                path,
                params={"fields": "paperId", "limit": min(half_limit, 100)},
                timeout=15,
            )
            if resp.status_code != 200:
                continue
            rows = (resp.json() or {}).get("data") or []
            total = max(len(rows), 1)
            for idx, entry in enumerate(rows):
                paper = entry.get(key) or {}
                candidate_id = str((paper or {}).get("paperId") or "").strip()
                if not candidate_id:
                    continue
                score = round(max(0.0, 1.0 - (idx / total)), 4)
                relation_items.append((candidate_id, score))
        except Exception as exc:
            logger.debug("Semantic Scholar %s fetch failed: %s", relation_name, exc)

    if not relation_items:
        return []

    hydrated = fetch_papers_batch([paper_id for paper_id, _ in relation_items], fields=FIELDS)
    results: List[dict] = []
    seen: set[str] = set()
    for candidate_id, score in relation_items:
        paper = hydrated.get(candidate_id)
        if not paper or candidate_id in seen:
            continue
        seen.add(candidate_id)
        candidate = _s2_to_candidate(paper, score=score)
        if candidate:
            results.append(candidate)
    return results[: max(1, limit)]


def recommend_for_paper(
    seed_id: str,
    *,
    limit: int = 20,
    fields: str = FIELDS,
) -> List[dict]:
    """Call S2 `GET /recommendations/v1/papers/forpaper/{id}` (single-seed).

    ``seed_id`` may be a bare paperId, a `DOI:{doi}` string, a
    `CorpusID:{id}` string, or any other S2-accepted identifier form.
    Returns ALMa candidate dicts (same shape as `search_papers`).
    """

    seed = (seed_id or "").strip()
    if not seed:
        return []
    try:
        resp = get_source_http_client("semantic_scholar").get(
            f"https://api.semanticscholar.org/recommendations/v1/papers/forpaper/{seed}",
            params={
                "fields": fields,
                "limit": max(1, min(int(limit or 20), 100)),
            },
            timeout=15,
        )
        if resp.status_code != 200:
            logger.debug(
                "Semantic Scholar recommend-for-paper HTTP %d for %s",
                resp.status_code,
                seed,
            )
            return []
        papers = (resp.json() or {}).get("recommendedPapers") or []
    except Exception as exc:
        logger.warning("Semantic Scholar recommend-for-paper failed: %s", exc)
        return []

    results: List[dict] = []
    total = max(len(papers), 1)
    for idx, paper in enumerate(papers):
        score = round(max(0.0, 1.0 - (idx / total)), 4)
        candidate = _s2_to_candidate(paper, score=score)
        if candidate:
            results.append(candidate)
    return results


def recommend_from_seeds(
    positive_ids: list[str],
    negative_ids: list[str],
    *,
    limit: int = 50,
    fields: str = FIELDS,
) -> List[dict]:
    """Call S2 `POST /recommendations/v1/papers` with positive + negative seeds.

    Each seed id may be a bare S2 `paperId`, a `DOI:{doi}` string, a
    `CorpusID:{id}` string, or any other identifier form the S2
    recommendations API accepts (ArXiv:, PMID:, PMCID:, etc.). The API
    accepts up to 500 positive and 500 negative IDs; we cap at the same.

    The recommendations endpoint lives at a different host path than
    the rest of the graph API (`/recommendations/v1/papers` rather than
    `/graph/v1/...`), so we use the full URL to bypass the shared
    client's base-URL join.

    Returns a list of candidate dicts ready for the discovery engine's
    merge pipeline.
    """

    pos = [str(item).strip() for item in (positive_ids or []) if str(item).strip()]
    neg = [str(item).strip() for item in (negative_ids or []) if str(item).strip()]
    if not pos:
        return []

    body = {
        "positivePaperIds": list(dict.fromkeys(pos))[:500],
        "negativePaperIds": list(dict.fromkeys(neg))[:500],
    }
    client = get_source_http_client("semantic_scholar")
    try:
        resp = client.post(
            "https://api.semanticscholar.org/recommendations/v1/papers",
            params={"fields": fields, "limit": max(1, min(int(limit or 50), 500))},
            json=body,
            timeout=30,
        )
        if resp.status_code != 200:
            logger.debug(
                "Semantic Scholar recommend-from-seeds HTTP %d (pos=%d neg=%d)",
                resp.status_code,
                len(pos),
                len(neg),
            )
            return []
        papers = (resp.json() or {}).get("recommendedPapers") or []
    except Exception as exc:
        logger.warning("Semantic Scholar recommend-from-seeds failed: %s", exc)
        return []

    results: List[dict] = []
    total = max(len(papers), 1)
    for idx, paper in enumerate(papers):
        # Rank-based descending score (same convention as search_papers);
        # downstream scorer re-ranks on the 10-signal formula anyway.
        score = round(max(0.0, 1.0 - (idx / total)), 4)
        candidate = _s2_to_candidate(paper, score=score)
        if candidate:
            results.append(candidate)
    return results


def _fetch_edge_graph(
    seed_id: str,
    *,
    path_suffix: str,
    paper_key: str,
    limit: int,
) -> List[dict]:
    """Shared plumbing for `/paper/{id}/references` + `/citations`.

    Two-step fetch (mirrors `fetch_related_papers`): edge metadata +
    counterparty paperId in call 1, full hydration via `/paper/batch`
    in call 2. Each returned dict carries the hydrated candidate
    fields plus `is_influential` / `edge_contexts` / `edge_intents`
    from the S2 edge row.
    """
    seed = (seed_id or "").strip()
    if not seed:
        return []
    client = get_source_http_client("semantic_scholar")
    try:
        resp = client.get(
            f"/paper/{seed}/{path_suffix}",
            params={
                "fields": "isInfluential,contexts,intents,paperId",
                "limit": max(1, min(int(limit or 30), 100)),
            },
            timeout=10,
        )
        if resp.status_code != 200:
            logger.debug(
                "S2 %s HTTP %d for %s", path_suffix, resp.status_code, seed
            )
            return []
        rows = (resp.json() or {}).get("data") or []
    except Exception as exc:
        logger.warning("S2 %s fetch failed for %s: %s", path_suffix, seed, exc)
        return []

    edges: List[dict] = []
    for row in rows:
        peer = row.get(paper_key) or {}
        peer_id = str(peer.get("paperId") or "").strip()
        if not peer_id:
            continue
        edges.append(
            {
                "paper_id": peer_id,
                "is_influential": bool(row.get("isInfluential") or False),
                "contexts": [str(c).strip() for c in (row.get("contexts") or [])][:2],
                "intents": [str(i).strip() for i in (row.get("intents") or [])],
            }
        )
    if not edges:
        return []

    hydrated = fetch_papers_batch(
        [e["paper_id"] for e in edges], fields=FIELDS
    )

    out: List[dict] = []
    for edge in edges:
        paper = hydrated.get(edge["paper_id"])
        if not paper:
            continue
        candidate = _s2_to_candidate(paper, score=0.5)
        if not candidate:
            continue
        candidate["is_influential"] = edge["is_influential"]
        candidate["edge_contexts"] = edge["contexts"]
        candidate["edge_intents"] = edge["intents"]
        out.append(candidate)
    return out


def fetch_references_for_paper(seed_id: str, *, limit: int = 30) -> List[dict]:
    """S2 `GET /paper/{id}/references` — what the seed paper cites."""
    return _fetch_edge_graph(
        seed_id,
        path_suffix="references",
        paper_key="citedPaper",
        limit=limit,
    )


def fetch_citations_for_paper(seed_id: str, *, limit: int = 30) -> List[dict]:
    """S2 `GET /paper/{id}/citations` — what cites the seed paper."""
    return _fetch_edge_graph(
        seed_id,
        path_suffix="citations",
        paper_key="citingPaper",
        limit=limit,
    )


def fetch_author_papers(
    s2_author_id: str, limit: int = 20
) -> List[dict]:
    """Fetch recent papers by a Semantic Scholar author ID.

    Args:
        s2_author_id: Numeric Semantic Scholar author ID.
        limit: Maximum number of results.

    Returns:
        List of candidate dicts.
    """
    if not (s2_author_id or "").strip():
        return []

    try:
        resp = get_source_http_client("semantic_scholar").get(
            f"/author/{s2_author_id.strip()}/papers",
            params={"fields": FIELDS, "limit": min(limit, 100)},
            timeout=15,
        )
        if resp.status_code != 200:
            logger.debug(
                "Semantic Scholar author papers returned HTTP %d for '%s'",
                resp.status_code,
                s2_author_id,
            )
            return []

        papers = (resp.json() or {}).get("data") or []
        results: List[dict] = []
        total = max(len(papers), 1)
        for i, p in enumerate(papers):
            score = round(max(0.0, 1.0 - (i / total)), 4)
            candidate = _s2_to_candidate(p, score=score)
            if candidate:
                results.append(candidate)
        return results

    except Exception as exc:
        logger.warning("Semantic Scholar author papers fetch failed: %s", exc)
        return []
