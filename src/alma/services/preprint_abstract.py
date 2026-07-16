"""Preprint-twin abstract recovery via structured arXiv / bioRxiv APIs.

Single responsibility (task 05): given a *published* paper that is missing
its abstract, recover the abstract from its preprint twin on arXiv or
bioRxiv/medRxiv. Those servers expose the abstract unconditionally through
their **structured APIs** — no paywall, no HTML scraping, far cleaner than
the landing-page `<meta>`-tag channel that runs as the fallback.

This module is pure orchestration: it parses preprint identifiers out of a
paper's DOI / OA URLs and dispatches to the existing source adapters
(`discovery/arxiv.py`, `discovery/biorxiv.py`). It performs NO database
access — the caller (`services/corpus_rehydrate._fetch_abstract_recovery`)
owns the fill-only write + ledger.

Recovery order (fastest / highest-precision first):
1. arXiv id from a `10.48550/arXiv.*` DOI or an `arxiv.org/abs|pdf/*` URL.
2. bioRxiv/medRxiv DOI from a `10.1101/*` DOI or a `*rxiv.org/content/*` URL.
3. arXiv **title search** — finds a twin we hold no link to, accepted only on
   an exact normalized-title-key match within a small year window.

bioRxiv has no keyword-search endpoint (recent-window only), so it is
direct-DOI/URL only; the title-search fallback is arXiv-specific.
"""

from __future__ import annotations

import logging
import re

from alma.application.preprint_dedup import classify_preprint_source
from alma.core.utils import normalize_doi
from alma.discovery import arxiv, biorxiv

logger = logging.getLogger(__name__)

# arXiv DOIs are `10.48550/arXiv.<id>`; the registrant prefix is fixed.
_ARXIV_DOI_PREFIX = "10.48550/arxiv"
# `arxiv.org/abs/<id>` or `arxiv.org/pdf/<id>` (id may carry a `vN` suffix or,
# for legacy ids, a category slash like `cond-mat/0211034`).
_ARXIV_URL_RE = re.compile(r"arxiv\.org/(?:abs|pdf)/(.+)$", re.IGNORECASE)
# bioRxiv/medRxiv content URL → (server, DOI). The DOI keeps the `10.1101/...`
# stem; the trailing `vN`/`.full`/`.pdf` is stripped by the caller.
_BIORXIV_URL_RE = re.compile(
    r"(?P<server>bio|med)rxiv\.org/content/(?P<doi>10\.1101/[^?#\s]+)",
    re.IGNORECASE,
)


def _arxiv_id_from_doi(doi: str | None) -> str | None:
    """Extract the bare arXiv id from a `10.48550/arXiv.<id>` DOI, else None."""
    if classify_preprint_source(doi) != "arxiv":
        return None
    normalized = (normalize_doi(doi) or "").lower()
    rest = normalized[len(_ARXIV_DOI_PREFIX):].lstrip(".:").strip()
    return rest or None


def _arxiv_id_from_url(url: str) -> str | None:
    """Extract the arXiv id from an `arxiv.org/abs|pdf/<id>` URL, else None."""
    match = _ARXIV_URL_RE.search(url or "")
    if not match:
        return None
    raw = match.group(1).split("?")[0].split("#")[0].strip()
    if raw.lower().endswith(".pdf"):
        raw = raw[:-4]
    return raw.strip("/") or None


def _biorxiv_ref_from_url(url: str) -> tuple[str, str] | None:
    """Extract `(server, doi)` from a bioRxiv/medRxiv content URL, else None."""
    match = _BIORXIV_URL_RE.search(url or "")
    if not match:
        return None
    server = "medrxiv" if match.group("server").lower() == "med" else "biorxiv"
    # Strip the version/format tail (`v2`, `.full`, `.pdf`) off the DOI stem.
    doi_raw = re.split(r"v\d+|\.full|\.pdf", match.group("doi"), maxsplit=1)[0]
    doi = normalize_doi(doi_raw)
    if not doi:
        return None
    return (server, doi)


def _arxiv_id_from_identifiers(doi: str | None, urls: list[str]) -> str | None:
    """First arXiv id found across the DOI and the OA URLs."""
    from_doi = _arxiv_id_from_doi(doi)
    if from_doi:
        return from_doi
    for url in urls:
        from_url = _arxiv_id_from_url(url)
        if from_url:
            return from_url
    return None


def _biorxiv_ref_from_identifiers(
    doi: str | None, urls: list[str]
) -> tuple[str | None, str] | None:
    """First `(server, doi)` bioRxiv/medRxiv ref across the DOI and OA URLs.

    A bare `10.1101/*` DOI doesn't reveal the server (both share the
    registrant) → `server` is None and the adapter probes both. A content
    URL names the server explicitly.
    """
    for url in urls:
        from_url = _biorxiv_ref_from_url(url)
        if from_url:
            return from_url
    if classify_preprint_source(doi) == "biorxiv":
        normalized = normalize_doi(doi)
        if normalized:
            return (None, normalized)
    return None


def recover_preprint_abstract(
    *,
    title: str | None,
    year: int | None,
    doi: str | None,
    urls: list[str] | None = None,
) -> tuple[str, str]:
    """Recover a missing abstract from the paper's preprint twin.

    Returns ``(abstract, reason)``. ``abstract`` is non-empty only when a
    usable abstract was found; ``reason`` always describes the outcome so the
    caller can stamp the recovery ledger (``arxiv_twin_abstract`` /
    ``biorxiv_twin_abstract`` / ``arxiv_title_twin_abstract`` /
    ``no_preprint_twin``). Network is performed here (via the source clients);
    no DB access.
    """
    # Lazy import breaks the corpus_rehydrate <-> preprint_abstract cycle and
    # reuses the ONE "is this a real abstract" gate (length / not-a-URL).
    from alma.services.corpus_rehydrate import _is_usable_recovered_abstract

    url_list = [str(u).strip() for u in (urls or []) if str(u or "").strip()]

    # 1. Direct arXiv id (DOI or OA URL) — structured `summary`, highest precision.
    arxiv_id = _arxiv_id_from_identifiers(doi, url_list)
    if arxiv_id:
        abstract = arxiv.fetch_abstract_by_id(arxiv_id)
        if _is_usable_recovered_abstract(abstract):
            return (abstract, "arxiv_twin_abstract")

    # 2. Direct bioRxiv/medRxiv DOI (DOI or OA URL).
    biorxiv_ref = _biorxiv_ref_from_identifiers(doi, url_list)
    if biorxiv_ref:
        server, preprint_doi = biorxiv_ref
        abstract = biorxiv.fetch_abstract_by_doi(preprint_doi, server=server)
        if _is_usable_recovered_abstract(abstract):
            return (abstract, "biorxiv_twin_abstract")

    # 3. arXiv title search — last resort; strict title-key + year match guards
    #    against a confident wrong hit from arXiv's huge corpus.
    if title:
        abstract = arxiv.find_abstract_for_title(title, year=year)
        if _is_usable_recovered_abstract(abstract):
            return (abstract, "arxiv_title_twin_abstract")

    return ("", "no_preprint_twin")
