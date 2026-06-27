"""Core utility functions for paper processing.

This module centralizes common operations to avoid duplication across
the codebase, particularly for text normalization and data transformations.
"""

import html
import re
import sqlite3
import unicodedata
import urllib.parse
import uuid
from datetime import datetime
from typing import Dict, Optional

from alma.plugins.base import Publication


def generate_paper_id() -> str:
    """Generate a new UUID for a paper."""
    return str(uuid.uuid4())


def utcnow() -> datetime:
    """Naive UTC ``datetime`` used by every ledger / activity write.

    SQLite's ``DATETIME`` is timezone-naive; the rest of the codebase
    treats every timestamp as UTC implicitly. Centralizing this lets us
    swap to ``datetime.now(timezone.utc)`` later without rippling
    through every service module.
    """
    return datetime.utcnow()


def utcnow_iso() -> str:
    """ISO-8601 string of :func:`utcnow` — the canonical SQL-friendly form."""
    return utcnow().isoformat()


def normalize_id_list(values: list[str] | tuple[str, ...] | None) -> list[str]:
    """Return stable, non-empty string IDs while preserving input order."""
    if not values:
        return []
    return list(
        dict.fromkeys(str(value).strip() for value in values if str(value).strip())
    )


def normalize_text(value: str) -> str:
    """Normalize text for comparison: lowercase, strip non-alphanumeric, collapse whitespace.

    Useful for fuzzy title matching, name comparisons, and deduplication.
    """
    return " ".join(re.sub(r"[^a-z0-9]+", " ", (value or "").lower()).split())


# Dotless-ı (U+0131) immediately followed by a combining mark (U+0300–U+036F).
# This is the signature of a precomposed letter (e.g. `í`) that leaked through a
# LaTeX-aware export as a dotless-ı plus a separate combining accent.
_DOTLESS_I_PLUS_COMBINING_RE = re.compile("ı([̀-ͯ])")


def repair_display_text(value: Optional[str]) -> str:
    """Repair LaTeX-leaked dotless-ı + combining marks, then NFC-normalize.

    The durable, write-time twin of the frontend ``repairDisplayText`` (applied
    at the API-client boundary). When a name/title passes through a LaTeX-aware
    pipeline, a precomposed letter such as ``í`` can leak as a dotless-ı
    (U+0131) followed by a combining acute (U+0301), which renders as ``ı́``.
    We restore the dotted ``i`` so the combining mark composes, then NFC-
    normalize to the precomposed form.

    Lossless: diacritics are preserved (re-composed), never stripped — a no-op
    on clean or empty text. Apply at the persistence chokepoints (author names,
    paper titles, journals, author lists, abstracts) so the DB stores clean
    text. Distinct from the deliberately lossy comparison helpers
    :func:`normalize_text` / :func:`normalize_title_key` — do NOT use those for
    durable writes.
    """
    if not value:
        return value or ""
    return unicodedata.normalize("NFC", _DOTLESS_I_PLUS_COMBINING_RE.sub("i\\1", value))


# A real HTML/XML tag: ``<`` then an (optionally closing) ASCII-letter-led name.
# The letter requirement is the safety margin — it refuses to match a
# mathematical ``<`` / ``>`` (``p < 0.05``, ``N > 30``) as a tag, so scientific
# titles with inequalities survive untouched.
_HTML_TAG_RE = re.compile(r"</?[a-zA-Z][^<>]*>")


def strip_html(value: Optional[str]) -> str:
    """Remove HTML/XML markup + decode entities from a display string.

    Source titles for figures / supporting-info arrive wrapped in markup
    (``<p>Cytochrome oxidase…</p>``, ``using <i>N</i> = 6``). This decodes
    entities (``&amp;`` → ``&``), drops letter-led tags (replacing each with a
    space so words don't fuse: ``a<br>b`` → ``a b``), and collapses the
    resulting whitespace.

    A NO-OP on clean text: when the value has no ``<`` and no ``&`` it is
    returned verbatim (so clean multi-paragraph abstracts keep their
    whitespace). Mathematical ``<`` / ``>`` is preserved (see ``_HTML_TAG_RE``).
    """
    if not value or ("<" not in value and "&" not in value):
        return value or ""
    text = html.unescape(value)
    text = _HTML_TAG_RE.sub(" ", text)
    text = re.sub(r"\s+", " ", text).strip()
    # A tag sitting between a word and its punctuation (`differs</i>.`) leaves a
    # stray space (`differs .`); pull punctuation back onto the word.
    return re.sub(r"\s+([.,;:!?])", r"\1", text)


def clean_display_text(value: Optional[str]) -> str:
    """Write-time cleaner for display fields: ``strip_html`` then diacritic repair.

    The canonical composition applied at every paper/author persistence
    chokepoint for title/journal/abstract-style fields — see
    :func:`strip_html` and :func:`repair_display_text`.
    """
    return repair_display_text(strip_html(value))


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


def candidate_dedup_key(item: dict) -> str:
    """Canonical in-memory dedup key for a candidate / paper dict (D-7).

    One identity function shared by the retrieval merge (``source_search``) and
    the local skip-set (``engine``), which previously hand-rolled three
    divergent variants (DOI→title→URL vs DOI→URL→title, neither using
    ``openalex_id`` or year) — so the same paper keyed differently across them
    and dedup silently missed. Priority, strongest identity first:

        canonical_doi → doi → openalex_id → (year + normalized title) → url → title

    Callers that compare against this MUST supply the same fields (e.g. the
    local skip-set selects ``openalex_id`` and ``year``, not just title/url/doi).
    Returns a prefixed key; ``"url:"`` / ``"title:"`` (empty suffix) signal an
    identity-less item the caller should drop.
    """
    canonical_doi = normalize_doi((item.get("canonical_doi") or "").strip())
    if canonical_doi:
        return f"doi:{canonical_doi.lower()}"
    doi = normalize_doi((item.get("doi") or "").strip())
    if doi:
        return f"doi:{doi.lower()}"
    openalex_id = str(item.get("openalex_id") or "").strip()
    if openalex_id:
        return f"openalex:{openalex_id.rsplit('/', 1)[-1].lower()}"
    title_key = normalize_title_key(item.get("title"))
    year = item.get("year")
    if title_key and year not in (None, ""):
        return f"yt:{year}:{title_key}"
    url = str(item.get("url") or "").strip().lower()
    if url:
        return f"url:{url}"
    title = str(item.get("title") or "").strip().lower()
    return f"title:{title}"


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


# DOI shape per the Crossref / DataCite registry: prefix `10.<4-9 digits>`,
# slash, then any non-empty suffix. Suffixes are case-insensitive per the
# DOI spec (https://www.doi.org/the-identifier/resources/handbook/) but
# real-world implementations sometimes treat them case-sensitively, which
# is the fault we're protecting against in `canonical_lookup_doi`.
_DOI_SHAPE_RE = re.compile(r"^10\.\d{4,9}/.+$")


def is_doi_shaped(value: Optional[str]) -> bool:
    """True when *value* (after `normalize_doi`) is registry-shaped.

    `normalize_doi` deliberately does NOT validate shape (it's a cleaner,
    not a gate). Callers that treat free-form input as a *maybe*-DOI —
    e.g. query parsers deciding whether to hit a `/works/doi:` endpoint —
    use this to avoid spending an upstream round-trip on strings that
    cannot possibly resolve (`title:…`, bare words, junk query params).
    """
    norm = normalize_doi(value)
    return bool(norm and _DOI_SHAPE_RE.match(norm))
# Trailing publisher fragments observed in the wild on bibtex / RIS imports.
# Stripped only when at the end of the suffix; never inside the suffix.
_DOI_TRAILING_FRAGMENTS = ("/abstract", "/full", "/pdf", "/epdf", "/meta")


def canonical_lookup_doi(doi_val: Optional[str]) -> Optional[str]:
    """Return the lowercased, URL-decoded, fragment-stripped DOI for
    external-API lookups.

    Different from `normalize_doi`:
    - Lowercases the suffix (case-insensitive per spec; some endpoints
      reject otherwise).
    - URL-decodes (e.g. `10.1000%2Fxyz` → `10.1000/xyz`).
    - Strips trailing publisher fragments (`/full`, `/pdf`, …).

    Same as `normalize_doi`:
    - Strips `DOI:` / `doi:` prefixes and `https?://doi.org/` prefixes.

    Use this whenever you build a lookup id that goes to S2 / Crossref /
    OpenAlex DOI search. **Do NOT** use it as the persisted form in
    `papers.doi`; that's `normalize_doi`'s job.
    """
    base = normalize_doi(doi_val)
    if not base:
        return None
    try:
        decoded = urllib.parse.unquote(base.strip()).strip()
    except Exception:
        decoded = base
    lowered = decoded.lower()
    for fragment in _DOI_TRAILING_FRAGMENTS:
        if lowered.endswith(fragment):
            lowered = lowered[: -len(fragment)]
            break
    return lowered or None


def validate_doi_shape(doi_val: Optional[str]) -> bool:
    """Return True iff the DOI matches the registry-shape regex.

    Operates on the canonical-lookup form (lowercased + decoded), so
    `10.1234/Foo` and `10.1234%2FFoo` both validate. A DOI that fails
    this check is malformed and should be marked terminally as
    `bad_local_doi`, not sent to external APIs.
    """
    canonical = canonical_lookup_doi(doi_val)
    if not canonical:
        return False
    return bool(_DOI_SHAPE_RE.match(canonical))


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
