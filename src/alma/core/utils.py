"""Core utility functions for paper processing.

This module centralizes common operations to avoid duplication across
the codebase, particularly for text normalization and data transformations.
"""

import re
import sqlite3
import uuid
from typing import Dict, Optional

from alma.plugins.base import Publication


def generate_paper_id() -> str:
    """Generate a new UUID for a paper."""
    return str(uuid.uuid4())


def normalize_text(value: str) -> str:
    """Normalize text for comparison: lowercase, strip non-alphanumeric, collapse whitespace.

    Useful for fuzzy title matching, name comparisons, and deduplication.
    """
    return " ".join(re.sub(r"[^a-z0-9]+", " ", (value or "").lower()).split())


_TITLE_KEY_NOISE_RE = re.compile(r"[^a-z0-9]+")


def normalize_title_key(title: Optional[str]) -> str:
    """Deduplication key for paper titles.

    Lowercases and strips every non-alphanumeric character (including
    whitespace). Deliberately lossy — the intended use is catching the
    same preprint indexed under two different OpenAlex work IDs
    (e.g. ArXiv.org vs arXiv (Cornell)). Real distinct papers with
    coincidentally identical keys must disambiguate by year.
    """
    if not title:
        return ""
    return _TITLE_KEY_NOISE_RE.sub("", title.strip().lower())


def resolve_existing_paper_id(
    conn: sqlite3.Connection,
    *,
    openalex_id: Optional[str] = None,
    doi: Optional[str] = None,
    title: Optional[str] = None,
    year: Optional[int] = None,
) -> Optional[str]:
    """Return the `papers.id` of an existing row matching the canonical triple.

    The triple, tried in priority order:
      1. exact ``openalex_id``
      2. case-insensitive ``doi``
      3. ``(year, normalize_title_key(title))``

    The DOI is normalized via ``normalize_doi`` before lookup. Callers
    that already hold a normalized DOI can pass it through — a second
    normalization is idempotent. Returns ``None`` when no row matches;
    the caller is expected to insert one.
    """
    oa = (openalex_id or "").strip()
    if oa:
        row = conn.execute("SELECT id FROM papers WHERE openalex_id = ?", (oa,)).fetchone()
        if row:
            return str(row["id"])

    doi_norm = normalize_doi(doi)
    if doi_norm:
        row = conn.execute(
            "SELECT id FROM papers WHERE lower(doi) = lower(?)", (doi_norm,)
        ).fetchone()
        if row:
            return str(row["id"])

    if title and year is not None:
        key = normalize_title_key(title)
        if key:
            rows = conn.execute(
                "SELECT id, title FROM papers WHERE year = ?", (year,)
            ).fetchall()
            for candidate_row in rows:
                if normalize_title_key(str(candidate_row["title"] or "")) == key:
                    return str(candidate_row["id"])

    return None


def derive_source_id(pub_dict: Dict) -> str:
    """Derive a unique source identifier from paper metadata.

    Priority: DOI > URL > title
    """
    doi = (pub_dict.get("doi") or "").strip()
    url = (pub_dict.get("pub_url") or pub_dict.get("url") or "").strip()
    title = (pub_dict.get("title") or "").strip()

    if doi:
        doi_lower = doi.lower()
        if doi_lower.startswith("https://doi.org/"):
            doi = doi[len("https://doi.org/"):]
        elif doi_lower.startswith("http://doi.org/"):
            doi = doi[len("http://doi.org/"):]

    return doi or url or title


def normalize_doi(doi_val: Optional[str]) -> Optional[str]:
    """Normalize DOI to bare format (strip URL prefixes).

    OpenAlex and Scholar may return DOIs in different formats:
    - https://doi.org/10.1234/xyz
    - doi:10.1234/xyz
    - 10.1234/xyz

    This function normalizes to the bare format: 10.1234/xyz
    """
    if not doi_val:
        return None

    d = doi_val.strip()
    d = d.replace('DOI:', '').replace('doi:', '').strip()

    d_lower = d.lower()
    if d_lower.startswith('https://doi.org/'):
        d = d[len('https://doi.org/'):]
    elif d_lower.startswith('http://doi.org/'):
        d = d[len('http://doi.org/'):]

    return d or None


_ORCID_RE = re.compile(r"^\d{4}-\d{4}-\d{4}-\d{3}[\dX]$")


def normalize_orcid(orcid_val: Optional[str]) -> Optional[str]:
    """Normalize ORCID to the bare 16-char hyphenated form.

    Single chokepoint for every write path that touches an ORCID
    column (`authors.orcid`, `publication_authors.orcid`, …). OpenAlex
    returns the URI form (``https://orcid.org/0000-…``); Semantic
    Scholar returns bare; various manual paths use either. This
    flattens them all so the partial UNIQUE indexes on
    `authors.orcid`, the merge equality checks, and the dedup sweep
    treat the same human consistently.

    Returns ``None`` for empty or malformed inputs (anything that
    doesn't match the ORCID checksum shape) — a `None` write keeps
    the partial unique index satisfied where empty-string ``''``
    would also satisfy it but pollute downstream code that checks
    for truthiness.
    """
    if not orcid_val:
        return None
    raw = str(orcid_val).strip()
    if not raw:
        return None
    if raw.startswith("http://") or raw.startswith("https://"):
        raw = raw.rstrip("/").split("/")[-1]
    elif raw.lower().startswith("orcid.org/"):
        raw = raw.split("/", 1)[1]
    raw = raw.upper()
    if not _ORCID_RE.match(raw):
        return None
    return raw


def to_publication_dataclass(pub_dict: Dict) -> Publication:
    """Convert a publication dictionary to a Publication dataclass."""
    return Publication(
        title=pub_dict.get("title", ""),
        authors=pub_dict.get("authors", ""),
        year=str(pub_dict.get("year", "")),
        abstract=pub_dict.get("abstract", ""),
        pub_url=pub_dict.get("pub_url", ""),
        journal=pub_dict.get("journal", ""),
        citations=pub_dict.get("num_citations"),
    )
