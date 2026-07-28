"""Fetch candidate papers from the Semantic Scholar Graph API."""

from __future__ import annotations

import logging
import sqlite3
from dataclasses import dataclass
from typing import Any

from alma.ai.embedding_sources import EMBEDDING_SOURCE_SEMANTIC_SCHOLAR
from alma.core.http_sources import get_source_http_client
from alma.core.scoring_math import query_match_score, query_tokens, rank_score
from alma.core.time import utcnow
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

# --- Endpoint → allowed fields contract (task 61 F8) -------------------------
# S2 does NOT accept the same `fields` set everywhere, and asking for an
# unsupported one is a hard HTTP 400, not a silent drop. Two shipped lanes died
# this way: `/paper/search/bulk` 400'd on the full list (fixed 2026-06-01 with a
# subtractive `_BULK_UNSUPPORTED_FIELDS` patch), and then `POST
# /recommendations/v1/papers` 400'd on exactly the same two fields because
# nobody added a *second* subtractive patch — that lane has returned `[]` since
# inception.
#
# A subtractive patch on a global list fails open: a new endpoint silently
# inherits every field, including ones it rejects. This registry is additive
# instead — an endpoint declares what it allows, and `project_fields` intersects.
# A new endpoint with no entry raises rather than guessing.
#
# All rows verified live 2026-07-27 against the configured key:
#   POST /paper/batch                     full set            -> 200
#   GET  /paper/search  /search/match     full set            -> 200
#   GET  /paper/search/bulk               tldr|embedding      -> 400
#   POST /recommendations/v1/papers       tldr|embedding      -> 400, reduced -> 200
#   GET  /recommendations/.../forpaper/*  tldr|embedding      -> 400, reduced -> 200
#
# TRAP, and the reason this needs a registry rather than a probe: the field
# error only fires when the query actually MATCHES something. `GET /forpaper`
# with the full field set returns a clean `200 {"recommendedPapers": []}` on
# `from=recent` (which is empty for older seeds) and a hard 400 on
# `from=all-cs`. An endpoint can therefore look field-compatible right up
# until it starts returning results.
_ALL_PAPER_FIELDS = frozenset(FIELDS.split(","))
_NO_TLDR_OR_VECTOR = _ALL_PAPER_FIELDS - {"tldr", "embedding.specter_v2"}

#: Endpoint key -> fields that endpoint accepts. Keys are stable internal
#: labels, not URLs, so a path change does not silently orphan a row.
ENDPOINT_FIELDS: dict[str, frozenset[str]] = {
    "paper.batch": _ALL_PAPER_FIELDS,
    "paper.search": _ALL_PAPER_FIELDS,
    "paper.search.match": _ALL_PAPER_FIELDS,
    # Bulk cannot return nested data and rejects tldr + the vector.
    "paper.search.bulk": _NO_TLDR_OR_VECTOR,
    # Neither recommendation endpoint returns tldr or the vector. Candidates
    # from these lanes are hydrated by a follow-on `/paper/batch` if a vector
    # is needed.
    "recommendations.papers": _NO_TLDR_OR_VECTOR,
    "recommendations.forpaper": _NO_TLDR_OR_VECTOR,
    "author.batch": frozenset(AUTHOR_FIELDS.split(",")),
}


class SemanticScholarFieldError(ValueError):
    """Raised when a caller asks an endpoint for a field it does not accept."""


def project_fields(endpoint: str, requested: str | None = None) -> str:
    """Return the `fields` string ``endpoint`` accepts, preserving caller order.

    ``requested`` defaults to the full paper field set (or `AUTHOR_FIELDS` for
    author endpoints). Anything the endpoint does not support is dropped and
    logged at DEBUG — dropping is correct because the alternative is a 400 that
    takes the whole lane down, and every caller is asking for a superset it can
    live without.

    An unknown ``endpoint`` raises: that is a programming error, and failing
    loudly here is the entire point of the registry.
    """
    allowed = ENDPOINT_FIELDS.get(endpoint)
    if allowed is None:
        raise SemanticScholarFieldError(
            f"Unknown Semantic Scholar endpoint {endpoint!r}; add it to ENDPOINT_FIELDS"
        )
    default = AUTHOR_FIELDS if endpoint.startswith("author.") else FIELDS
    wanted = [f.strip() for f in str(requested or default).split(",") if f.strip()]
    kept = [f for f in wanted if f in allowed]
    dropped = [f for f in wanted if f not in allowed]
    if dropped:
        logger.debug(
            "Dropping %d field(s) unsupported by %s: %s",
            len(dropped),
            endpoint,
            ",".join(dropped),
        )
    return ",".join(kept)


# A 4xx that is not 429 is a CONTRACT violation, not congestion. It must never
# be swallowed by `if resp.status_code != 200: return []` — that idiom is what
# hid the recommendations 400 for the lifetime of the lane (`CLAUDE.md` → "No
# silent failures").
def _log_contract_error(endpoint: str, resp: Any, *, context: str = "") -> None:
    """WARN on a non-retryable 4xx so a broken field contract is visible."""
    status = getattr(resp, "status_code", None)
    if status is None or not (400 <= int(status) < 500) or int(status) == 429:
        return
    try:
        detail = str(resp.json())[:300]
    except Exception:
        detail = (getattr(resp, "text", "") or "")[:300]
    logger.warning(
        "Semantic Scholar CONTRACT ERROR HTTP %s on %s%s: %s",
        status,
        endpoint,
        f" ({context})" if context else "",
        detail,
    )


class SemanticScholarBatchError(RuntimeError):
    """Raised when a strict Semantic Scholar batch request fails."""

    def __init__(self, message: str, *, status_code: int | None = None) -> None:
        super().__init__(message)
        self.status_code = status_code


# --- Payload + identifier budgeting (task 61 F7) -----------------------------
# `/paper/batch` enforces THREE independent caps on one response (verbatim from
# graph/v1/swagger.json): "Can only process 500 paper ids at a time. Can only
# return up to 10 MB of data at a time. Can only return up to 9999 citations at
# a time."
#
# The id cap and the byte cap are NOT interchangeable, and conflating them is
# how the UI batch slider came to lie: one paper contributes up to two lookup
# ids (S2 id + DOI), so a "500 papers" setting emits 1000 ids and is silently
# re-chunked into two requests. Budget on both, independently.
S2_MAX_PAPER_BATCH_IDS = 500
S2_MAX_AUTHOR_BATCH_IDS = 1000  # spec: "Can only process 1,000 author ids at a time."
S2_MAX_RESPONSE_BYTES = 10 * 1024 * 1024
#: Leave headroom — bytes/row varies with abstract length, and hitting the cap
#: costs a full retry at 1 req/s.
S2_RESPONSE_SAFETY_FRACTION = 0.75

#: Measured 2026-07-27 against the live API with the full `FIELDS` set: 5 ids
#: returned 94,496 bytes = 18,899 B/row, dominated by the 768-d vector. Broken
#: into components so a narrower projection gets a correspondingly larger batch
#: instead of paying the vector's cost when it did not ask for one.
_FIELD_BYTE_COST = {
    "embedding.specter_v2": 15_500,
    "abstract": 1_800,
    "tldr": 400,
    "authors": 700,
    "externalIds": 200,
    "journal": 120,
}
_BASE_ROW_BYTES = 400


def estimated_bytes_per_row(fields: str | None) -> int:
    """Approximate response bytes for one paper row under ``fields``.

    Deliberately an estimate, not a guarantee: abstract length varies widely, so
    the caller pairs this with `S2_RESPONSE_SAFETY_FRACTION` and a split-on-size
    fallback. Returning a constant here (the previous behaviour) is what made
    the 500-id setting land at 9.4 MB — 94% of the hard cap.
    """
    requested = {f.strip() for f in str(fields or FIELDS).split(",") if f.strip()}
    return _BASE_ROW_BYTES + sum(cost for name, cost in _FIELD_BYTE_COST.items() if name in requested)


@dataclass(frozen=True)
class BatchPlan:
    """How a batch of lookup ids will actually be issued.

    Exposed so `services/eta.py` can *read* the transport's plan instead of
    reimplementing chunk math — the two disagreeing is what let the ETA quote a
    request count the runner never used.
    """

    id_count: int
    chunk_size: int
    request_count: int
    bytes_per_row: int
    limited_by: str  # "ids" | "bytes" | "caller"


def plan_paper_batch(
    id_count: int,
    *,
    fields: str | None = None,
    batch_size: int | None = None,
    max_ids: int = S2_MAX_PAPER_BATCH_IDS,
) -> BatchPlan:
    """Declare the chunking for ``id_count`` lookup ids at ``fields``."""
    per_row = estimated_bytes_per_row(fields)
    byte_limited = max(1, int((S2_MAX_RESPONSE_BYTES * S2_RESPONSE_SAFETY_FRACTION) // max(1, per_row)))
    chunk = min(max_ids, byte_limited)
    limited_by = "ids" if chunk == max_ids else "bytes"
    if batch_size is not None:
        requested = max(1, int(batch_size))
        if requested < chunk:
            chunk, limited_by = requested, "caller"
        else:
            chunk = min(chunk, requested)
    count = max(0, int(id_count))
    return BatchPlan(
        id_count=count,
        chunk_size=chunk,
        request_count=(count + chunk - 1) // chunk if count else 0,
        bytes_per_row=per_row,
        limited_by=limited_by,
    )


@dataclass(frozen=True)
class IdentifierFetchOutcome:
    """Result of a batched lookup, keyed by the id the CALLER asked for.

    `terminal_ids` are ids the API rejected on their own merits (a 4xx that
    survived splitting down to one id) — the caller may stamp them as a
    permanent miss. `retryable_ids` hit congestion or a 5xx and MUST stay
    eligible; collapsing the two is what turned transient rate limits into
    permanent `terminal_no_match` stamps.
    """

    papers_by_requested_id: dict[str, dict]
    terminal_ids: frozenset[str]
    retryable_ids: frozenset[str]
    request_count: int


def _chunked(items: list[str], size: int) -> list[list[str]]:
    return [items[idx:idx + size] for idx in range(0, len(items), size)]


def _is_response_too_large(resp: Any) -> bool:
    """True when the API refused because the response would exceed 10 MB."""
    status = getattr(resp, "status_code", None)
    if status == 413:
        return True
    if status != 400:
        return False
    try:
        detail = str(resp.json()).lower()
    except Exception:
        detail = (getattr(resp, "text", "") or "").lower()
    return "10 mb" in detail or "too large" in detail or "response size" in detail


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


def upsert_specter2_vector(
    conn: sqlite3.Connection,
    paper_id: str,
    vector: list[float],
    *,
    source: str = EMBEDDING_SOURCE_SEMANTIC_SCHOLAR,
    created_at: str | None = None,
) -> bool:
    """Store a SPECTER2 vector using canonical source-priority semantics.

    Remote S2 vectors are the authoritative rows for
    ``S2_SPECTER2_MODEL``. They overwrite local/provider fallbacks for
    the same paper+model, but an existing S2-sourced row is left intact
    to avoid redundant rewrites during repeated sweeps.
    """
    if not isinstance(vector, list) or not vector:
        return False
    try:
        blob = encode_vector(vector)
    except (TypeError, ValueError):
        return False
    cursor = conn.execute(
        """
        INSERT INTO publication_embeddings
            (paper_id, embedding, model, source, created_at)
        VALUES (?, ?, ?, ?, ?)
        ON CONFLICT(paper_id, model) DO UPDATE SET
            embedding  = excluded.embedding,
            source     = excluded.source,
            created_at = excluded.created_at
        WHERE publication_embeddings.source != ?
        """,
        (
            paper_id,
            blob,
            S2_SPECTER2_MODEL,
            source,
            created_at or utcnow().isoformat(),
            source,
        ),
    )
    return cursor.rowcount > 0


def upsert_specter2_embedding(
    conn: sqlite3.Connection,
    paper_id: str,
    candidate: dict,
) -> bool:
    """Upsert the candidate's SPECTER2 vector for ``paper_id``.

    Reads ``candidate["specter2_embedding"]`` (list-of-floats produced
    by ``extract_specter2_vector``) and stores it via the canonical
    float16 blob encoder so every writer path agrees with the reader's
    decode dtype. Returns ``True`` when a row was inserted or upgraded.
    """
    vector = candidate.get("specter2_embedding")
    return upsert_specter2_vector(
        conn,
        paper_id,
        vector,
        source=EMBEDDING_SOURCE_SEMANTIC_SCHOLAR,
    )


def fetch_papers_batch(
    paper_ids: list[str],
    *,
    fields: str = FIELDS,
    batch_size: int | None = None,
    raise_on_error: bool = False,
) -> dict[str, dict]:
    """Fetch multiple Semantic Scholar papers by lookup id, keyed by `paperId`.

    Thin adapter over `fetch_vectors_for_identifiers` — the chunking, the
    10 MB split and the terminal/retryable classification all live in that one
    primitive. ``batch_size=None`` lets the transport size the request from the
    field projection's byte cost.
    """
    normalized_ids = [str(item or "").strip() for item in paper_ids if str(item or "").strip()]
    if not normalized_ids:
        return {}

    outcome = fetch_vectors_for_identifiers(
        normalized_ids,
        batch_size=batch_size,
        fields=fields,
    )
    if raise_on_error and outcome.retryable_ids:
        # Retryable failures must reach the caller as retryable, so a sweep
        # leaves those papers eligible instead of stamping a permanent miss.
        raise SemanticScholarBatchError(
            f"Semantic Scholar paper batch deferred for {len(outcome.retryable_ids)} ids",
            status_code=429,
        )
    if raise_on_error and outcome.terminal_ids and not outcome.papers_by_requested_id:
        raise SemanticScholarBatchError(
            f"Semantic Scholar paper batch rejected {len(outcome.terminal_ids)} ids",
            status_code=400,
        )
    # Historical shape: keyed by the RETURNED paperId, `_requested_id` stamped.
    return {
        str(row.get("paperId") or "").strip(): row
        for row in outcome.papers_by_requested_id.values()
        if str(row.get("paperId") or "").strip()
    }


def fetch_authors_batch(
    author_ids: list[str],
    *,
    fields: str = AUTHOR_FIELDS,
    batch_size: int | None = None,
) -> dict[str, dict]:
    """Fetch multiple Semantic Scholar authors in batch by authorId.

    ``batch_size=None`` uses the endpoint's documented 1,000-id maximum. Author
    rows carry no vector, so the 10 MB response cap is not the binding
    constraint here the way it is for `/paper/batch`.
    """
    normalized_ids = [str(item or "").strip() for item in author_ids if str(item or "").strip()]
    if not normalized_ids:
        return {}

    projected = project_fields("author.batch", fields)
    client = get_source_http_client("semantic_scholar")
    out: dict[str, dict] = {}
    # Spec cap is 1,000 author ids per call, not 500. At S2's 1 req/s the old
    # 500 clamp doubled the request count for no reason.
    chunk_size = max(1, min(int(batch_size or S2_MAX_AUTHOR_BATCH_IDS), S2_MAX_AUTHOR_BATCH_IDS))
    for chunk in _chunked(list(dict.fromkeys(normalized_ids)), chunk_size):
        try:
            resp = client.post(
                "/author/batch",
                params={"fields": projected},
                json={"ids": chunk},
                timeout=20,
            )
            if resp.status_code != 200:
                _log_contract_error("/author/batch", resp, context=f"{len(chunk)} ids")
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


def fetch_vectors_for_identifiers(
    identifiers: list[str],
    *,
    batch_size: int = 400,
    fields: str | None = None,
) -> IdentifierFetchOutcome:
    """Batch-fetch papers by lookup identifier, isolating per-id failures.

    THE single batched-vector primitive (task 62 §4.2). `services/s2_vectors`
    and the Discovery scoring seam both call this rather than each carrying
    their own resilient-split loop — two implementations of "split the batch,
    classify the failures" is exactly the duplication `CLAUDE.md` → "fix the
    primitive" exists to prevent.

    ``identifiers`` accept any S2 lookup form: a bare `paperId`, `DOI:10.x/y`,
    `CorpusId:n`, `ARXIV:id`, `PMID:`, `PMCID:`.

    Rows are keyed by the identifier the CALLER passed (not by the returned
    `paperId`), because the caller's bookkeeping is keyed that way. Every row
    also carries `_requested_id`, `specter2_embedding` and `specter2_model`.

    Failures are split down to single ids and classified: a 4xx that survives
    to one id is terminal (that id is bad), while 429/5xx/transport errors are
    retryable and the caller must leave those papers eligible.
    """
    deduped = [str(item or "").strip() for item in identifiers if str(item or "").strip()]
    deduped = list(dict.fromkeys(deduped))
    if not deduped:
        return IdentifierFetchOutcome({}, frozenset(), frozenset(), 0)

    projected = project_fields("paper.batch", fields)
    plan = plan_paper_batch(len(deduped), fields=projected, batch_size=batch_size)
    client = get_source_http_client("semantic_scholar")

    rows: dict[str, dict] = {}
    terminal: set[str] = set()
    retryable: set[str] = set()
    requests_made = 0

    def _run(chunk: list[str]) -> None:
        nonlocal requests_made
        try:
            resp = client.post(
                "/paper/batch",
                params={"fields": projected},
                json={"ids": chunk},
                timeout=30,
            )
            requests_made += 1
        except Exception as exc:  # transport error — always retryable
            logger.debug("S2 vector batch transport error for %d ids: %s", len(chunk), exc)
            retryable.update(chunk)
            return

        if resp.status_code == 200:
            resolved_positions: set[int] = set()
            for idx, row in _coerce_batch_rows_with_positions(resp.json() or {}):
                if idx >= len(chunk):
                    continue
                resolved_positions.add(idx)
                requested_id = chunk[idx]
                enriched = dict(row)
                enriched["_requested_id"] = requested_id
                vector = extract_specter2_vector(row)
                if vector:
                    enriched["specter2_embedding"] = vector
                    enriched["specter2_model"] = S2_SPECTER2_MODEL
                rows[requested_id] = enriched
            terminal.update(
                requested_id
                for idx, requested_id in enumerate(chunk)
                if idx not in resolved_positions
            )
            return

        # Oversized response: the ids are fine, split regardless of size.
        if _is_response_too_large(resp) and len(chunk) > 1:
            midpoint = max(1, len(chunk) // 2)
            _run(chunk[:midpoint])
            _run(chunk[midpoint:])
            return

        status = int(resp.status_code)
        if status == 429 or status >= 500:
            retryable.update(chunk)
            return

        # Non-429 4xx: one of these ids is malformed. Split to find which.
        if len(chunk) > 1:
            midpoint = max(1, len(chunk) // 2)
            _run(chunk[:midpoint])
            _run(chunk[midpoint:])
            return
        _log_contract_error("/paper/batch", resp, context=f"id={chunk[0]!r}")
        terminal.update(chunk)

    for chunk in _chunked(deduped, plan.chunk_size):
        _run(chunk)

    return IdentifierFetchOutcome(
        papers_by_requested_id=rows,
        terminal_ids=frozenset(terminal),
        retryable_ids=frozenset(retryable),
        request_count=requests_made,
    )


def match_paper_by_title(title: str, *, fields: str | None = None) -> dict | None:
    """Closest title match via `GET /paper/search/match`, or ``None`` on a miss.

    Returns the **raw** S2 row — `externalIds` and `authors[].authorId` intact —
    because `application/author_identity` matches on author ids that ALMa's
    normalized candidate shape flattens away. Callers normalize; the transport
    must not (task 62 §4.2).

    Contract notes, all verified live 2026-07-27:

    * the response envelope is ``{"data": [row]}``, not a bare object;
    * ``matchScore`` is returned automatically, is **unbounded** (131.8 on an
      exact title, not a 0–1 probability) and CANNOT be requested in `fields`
      (doing so is a 400). Callers keep their own Jaccard/year validation;
    * a miss is **HTTP 404** with ``{"error": "Title match not found"}``, which
      is a normal outcome here and deliberately not logged as a contract error.

    Raises `SemanticScholarBatchError(status_code=429)` when rate-limited, so a
    caller cannot mistake congestion for "no such paper" and stamp a permanent
    `terminal_no_match` on a paper that simply was not asked for yet.
    """
    query = (title or "").strip()
    if not query:
        return None
    projected = project_fields("paper.search.match", fields)
    try:
        resp = get_source_http_client("semantic_scholar").get(
            "/paper/search/match",
            params={"query": query, "fields": projected},
            timeout=15,
        )
    except Exception as exc:
        logger.debug("Semantic Scholar title match failed for %r: %s", query[:60], exc)
        return None

    if resp.status_code == 429:
        raise SemanticScholarBatchError(
            f"Semantic Scholar /paper/search/match rate-limited for {query[:60]!r}",
            status_code=429,
        )
    if resp.status_code == 404:
        return None  # documented miss, not an error
    if resp.status_code != 200:
        _log_contract_error("/paper/search/match", resp, context=f"query={query[:60]!r}")
        return None
    try:
        payload = resp.json() or {}
    except Exception:
        return None
    rows = payload.get("data") if isinstance(payload, dict) else None
    if not isinstance(rows, list) or not rows:
        return None
    row = rows[0]
    return row if isinstance(row, dict) else None


def _match_view(paper: dict) -> dict[str, str]:
    """Flatten a raw S2 row to the text fields `query_match_score` reads."""
    authors = ", ".join(
        (a.get("name") or "").strip()
        for a in (paper.get("authors") or [])
        if isinstance(a, dict) and (a.get("name") or "").strip()
    )
    return {
        "title": str(paper.get("title") or ""),
        "authors": authors,
        "abstract": str(paper.get("abstract") or ""),
    }


def s2_to_candidate(paper: dict, score: float = 0.5) -> dict | None:
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


def search_papers(
    query: str,
    limit: int = 20,
    *,
    raise_on_rate_limit: bool = False,
    max_retries: int | None = None,
) -> list[dict]:
    """Search Semantic Scholar by free-text query.

    Args:
        query: Search string (typically topic keywords).
        limit: Maximum number of results.
        raise_on_rate_limit: When True, raises
            ``SemanticScholarBatchError(status_code=429)`` after the
            shared HTTP client exhausts its retries on a 429. Default
            False preserves the legacy "silent empty list" behaviour
            for non-critical callers (e.g. interactive search). The
            hydration / vector-rescue paths pass True so a 429 marks
            the work `retryable_error`, not `terminal_no_match`.
        max_retries: Per-call override of the shared client's retry
            budget. Interactive surfaces racing a lane deadline (Find &
            Add) pass a small value so congestion fails fast instead of
            sitting in the background-job backoff chain.

    Returns:
        List of candidate dicts ready for ``_merge_candidate``. Empty
        when no results, when the query was empty, or when a non-2xx
        response was returned and `raise_on_rate_limit=False`.
    """
    if not (query or "").strip():
        return []

    try:
        resp = get_source_http_client("semantic_scholar").get(
            "/paper/search",
            params={
                "query": query.strip(),
                # Spec: `limit` must be <= 100, and the endpoint returns at most
                # 1,000 relevance-ranked results in total across pagination.
                "limit": min(limit, 100),
                "fields": project_fields("paper.search"),
            },
            timeout=15,
            max_retries=max_retries,
        )
        if resp.status_code != 200:
            _log_contract_error("/paper/search", resp, context=f"query={query[:60]!r}")
            if resp.status_code == 429 and raise_on_rate_limit:
                raise SemanticScholarBatchError(
                    f"Semantic Scholar /paper/search rate-limited "
                    f"(HTTP 429) for query {query[:60]!r}",
                    status_code=429,
                )
            logger.debug(
                "Semantic Scholar search returned HTTP %d for query '%s'",
                resp.status_code,
                query[:80],
            )
            return []

        papers = (resp.json() or {}).get("data") or []
        results: list[dict] = []
        total = max(len(papers), 1)
        for i, p in enumerate(papers):
            score = rank_score(i, total)
            candidate = s2_to_candidate(p, score=score)
            if candidate:
                results.append(candidate)
        return results

    except SemanticScholarBatchError:
        raise
    except Exception as exc:
        logger.warning("Semantic Scholar search failed: %s", exc)
        return []


def search_papers_bulk(
    query: str,
    *,
    limit: int = 20,
    from_year: int | None = None,
    fields_of_study: list[str] | None = None,
    publication_types: list[str] | None = None,
    open_access_pdf: bool = False,
) -> list[dict]:
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
        # NOTE: `/paper/search/bulk` has NO `limit` and NO `offset` parameter —
        # verified live 2026-07-27 (`?limit=5` still returned 1,000 rows). It
        # always returns up to 1,000 plus a continuation `token`. Sending
        # `limit` was a no-op that made the call site believe it controlled the
        # slice size. Do not reintroduce it.
        #
        # It also has no relevance ordering: `sort` accepts only paperId,
        # publicationDate and citationCount, and DEFAULTS to `paperId:asc` —
        # a sha hash. That default made this lane return the 1,000
        # lowest-paperId matches out of millions, deterministically, so a
        # keyword monitor surfaced the same papers forever.
        #
        # `publicationDate:desc` makes the pool mean "newest matching work",
        # which is what a Feed/monitor lane is for (task 62 §4.3). Relevance is
        # then reconstructed locally by `query_match_score`. Deliberate
        # trade-off: the spec says records with an undefined sort value sort
        # LAST, so date-less papers fall outside the 1,000-row window. That is
        # accepted for THIS lane only — general Discovery uses `/paper/search`.
        "sort": "publicationDate:desc",
        # Bulk rejects tldr / embedding.specter_v2 (400). Request the supported
        # subset; the `/paper/batch` pass below fills the rest.
        "fields": project_fields("paper.search.bulk"),
    }
    # Push the year filter SERVER-side. This used to be applied in Python after
    # downloading all 1,000 rows, so an old-corpus query could spend its entire
    # pool on papers that were then discarded locally.
    if from_year is not None:
        try:
            params["year"] = f"{int(from_year)}-"
        except (TypeError, ValueError):
            pass
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
            _log_contract_error("/paper/search/bulk", resp, context=f"query={query[:60]!r}")
            logger.debug(
                "Semantic Scholar bulk search returned HTTP %d for query '%s'",
                resp.status_code,
                query[:80],
            )
            return []

        papers = [row for row in ((resp.json() or {}).get("data") or []) if isinstance(row, dict)]
        if not papers:
            return []

        # The pool arrives in publication-date order, which is NOT relevance.
        # Reconstruct relevance locally from the text bulk already returned
        # (title/abstract/authors are in BULK_FIELDS, so this is free) and keep
        # the best matches — instead of taking the head of a date-sorted list.
        query_norm, tokens = query_tokens(query)
        ranked = sorted(
            papers,
            key=lambda row: query_match_score(query_norm, tokens, _match_view(row)),
            reverse=True,
        )

        # Hydrate ONLY the slice we intend to return, in one `/paper/batch`.
        # That call is what carries `tldr` and the SPECTER2 vector, which bulk
        # cannot return at all — and hydrating at retrieval is what keeps the
        # scorer from seeing a vector-less candidate (task 61 F2).
        keep = max(1, int(limit))
        shortlist = ranked[: min(len(ranked), max(keep, keep * 2))]
        hydrated_by_id = fetch_papers_batch(
            [str(row.get("paperId") or "").strip() for row in shortlist],
            fields=FIELDS,
        )

        out: list[dict] = []
        total = max(len(shortlist), 1)
        for i, paper in enumerate(shortlist):
            paper_id = str(paper.get("paperId") or "").strip()
            if paper_id and paper_id in hydrated_by_id:
                paper = hydrated_by_id[paper_id]
            # Server-side `year` already applied; this is a belt-and-braces
            # guard for rows whose year disagrees with publicationDate.
            if from_year is not None:
                year = paper.get("year")
                try:
                    if year is not None and int(year) < int(from_year):
                        continue
                except (TypeError, ValueError):
                    pass
            candidate = s2_to_candidate(paper, score=rank_score(i, total))
            if candidate:
                out.append(candidate)
        return out[:keep]
    except Exception as exc:
        logger.warning("Semantic Scholar bulk search failed: %s", exc)
        return []


def fetch_related_papers(doi: str, limit: int = 20) -> list[dict]:
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

    # `/paper/{paper_id}` and its sub-resources accept `DOI:<doi>` directly
    # (spec: "The following types of IDs are supported: <sha>, CorpusId:<id>,
    # DOI:<doi>, ARXIV:<id>, ..."), so the separate resolve call this function
    # used to make was pure waste — one of S2's 1 req/s slots per seed, fanned
    # out across every seed DOI in the graph lane.
    seed_ref = f"DOI:{doi}"
    half_limit = max(limit // 2, 5)
    relation_items: list[tuple[str, float]] = []
    client = get_source_http_client("semantic_scholar")

    # Fetch reference/citation paper IDs first, then hydrate with one batch call.
    for relation_name, path, key in (
        ("references", f"/paper/{seed_ref}/references", "citedPaper"),
        ("citations", f"/paper/{seed_ref}/citations", "citingPaper"),
    ):
        try:
            resp = client.get(
                path,
                # Spec cap on citations/references is 1,000, not 100.
                params={"fields": "paperId", "limit": min(half_limit, 1000)},
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
                score = rank_score(idx, total)
                relation_items.append((candidate_id, score))
        except Exception as exc:
            logger.debug("Semantic Scholar %s fetch failed: %s", relation_name, exc)

    if not relation_items:
        return []

    hydrated = fetch_papers_batch([paper_id for paper_id, _ in relation_items], fields=FIELDS)
    results: list[dict] = []
    seen: set[str] = set()
    for candidate_id, score in relation_items:
        paper = hydrated.get(candidate_id)
        if not paper or candidate_id in seen:
            continue
        seen.add(candidate_id)
        candidate = s2_to_candidate(paper, score=score)
        if candidate:
            results.append(candidate)
    return results[: max(1, limit)]


#: Pools accepted by `GET /recommendations/v1/papers/forpaper/{id}`.
#: `recent` (the API default) only draws from very recent work, so it returns
#: NOTHING for an older seed — measured 0/20 results on a 2017 seed where
#: `all-cs` returned 20/20 (2026-07-27). This lane exists to surface
#: foundational and older related work, so it must ask for the all-time pool.
#: The POST endpoint has no `from` parameter at all (verified against
#: recommendations/v1/swagger.json) and is a new-work frontier source.
RECOMMENDATION_POOL_ALL = "all-cs"


def recommend_for_paper(
    seed_id: str,
    *,
    limit: int = 20,
    fields: str = FIELDS,
    pool: str = RECOMMENDATION_POOL_ALL,
) -> list[dict]:
    """Call S2 `GET /recommendations/v1/papers/forpaper/{id}` (single-seed).

    ``seed_id`` may be a bare paperId, a `DOI:{doi}` string, a
    `CorpusID:{id}` string, or any other S2-accepted identifier form.
    Returns ALMa candidate dicts (same shape as `search_papers`).

    This is the **all-time** related-papers lane — see `RECOMMENDATION_POOL_ALL`
    for why `pool` must not be left at the API's `recent` default.
    """

    seed = (seed_id or "").strip()
    if not seed:
        return []
    try:
        resp = get_source_http_client("semantic_scholar").get(
            f"https://api.semanticscholar.org/recommendations/v1/papers/forpaper/{seed}",
            params={
                "fields": project_fields("recommendations.forpaper", fields),
                "limit": max(1, min(int(limit or 20), 100)),
                "from": pool,
            },
            timeout=15,
        )
        if resp.status_code != 200:
            _log_contract_error(
                "/recommendations/v1/papers/forpaper", resp, context=f"seed={seed}"
            )
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

    results: list[dict] = []
    total = max(len(papers), 1)
    for idx, paper in enumerate(papers):
        score = rank_score(idx, total)
        candidate = s2_to_candidate(paper, score=score)
        if candidate:
            results.append(candidate)
    return results


def recommend_from_seeds(
    positive_ids: list[str],
    negative_ids: list[str],
    *,
    limit: int = 50,
    fields: str = FIELDS,
) -> list[dict]:
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
            params={
                # This endpoint REJECTS `tldr` and `embedding.specter_v2` with a
                # hard 400 — unlike the GET /forpaper sibling, which accepts
                # both. Sending the global FIELDS here made every call 400, and
                # `if resp.status_code != 200: return []` hid it: this lane has
                # returned zero results for its entire life (0 rows in 233).
                "fields": project_fields("recommendations.papers", fields),
                "limit": max(1, min(int(limit or 50), 500)),
            },
            json=body,
            timeout=30,
        )
        if resp.status_code != 200:
            _log_contract_error(
                "/recommendations/v1/papers", resp, context=f"pos={len(pos)} neg={len(neg)}"
            )
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

    results: list[dict] = []
    total = max(len(papers), 1)
    for idx, paper in enumerate(papers):
        # Rank-based descending score (same convention as search_papers);
        # downstream ranker re-ranks on the family prior anyway.
        score = rank_score(idx, total)
        candidate = s2_to_candidate(paper, score=score)
        if candidate:
            results.append(candidate)
    return results
