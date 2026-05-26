"""Preflight forecasts for local library imports.

The importers still own parsing and persistence. This module only inspects the
normalized records before a background import is queued so the UI can show the
likely enrichment cost up front.
"""

from __future__ import annotations

import math
import sqlite3
from dataclasses import dataclass
from typing import Any, Iterable

from alma.library import importer
from alma.services.eta import detect_auth, estimate_eta


@dataclass(frozen=True)
class ParsedImportRecords:
    """Normalized import records plus non-fatal parser errors."""

    records: list[dict[str, Any]]
    errors: list[str]


def parse_bibtex_records(content: str) -> ParsedImportRecords:
    """Parse BibTeX into the same normalized shape used by `import_bibtex`."""
    import bibtexparser

    try:
        if hasattr(bibtexparser, "parse"):
            library = bibtexparser.parse(content)
            entries = library.entries
        else:
            library = bibtexparser.loads(content)
            entries = library.entries
    except Exception as exc:
        return ParsedImportRecords([], [f"BibTeX parse error: {exc}"])

    records: list[dict[str, Any]] = []
    errors: list[str] = []
    for entry in entries:
        norm = importer._normalize_bibtex_entry(entry)  # parser's canonical normalizer
        if not norm.get("title"):
            errors.append(f"Entry missing title: {entry.get('key', '?')}")
            continue
        records.append(norm)
    return ParsedImportRecords(records, errors)


def parse_zotero_rdf_records(content: str) -> ParsedImportRecords:
    """Parse Zotero RDF into the same normalized shape used by `import_zotero_rdf`."""
    try:
        return ParsedImportRecords(importer._parse_zotero_rdf(content), [])
    except Exception as exc:
        return ParsedImportRecords([], [f"Zotero RDF parse error: {exc}"])


def fetch_zotero_records(
    library_id: str,
    api_key: str,
    *,
    library_type: str = "user",
    collection_key: str | None = None,
) -> ParsedImportRecords:
    """Fetch Zotero items and normalize them without writing Library rows."""
    from pyzotero import zotero

    try:
        zot = zotero.Zotero(library_id, library_type, api_key)
        if collection_key:
            items = zot.collection_items(collection_key, itemType="-attachment -note")
        else:
            items = zot.everything(zot.items(itemType="-attachment -note"))
    except Exception as exc:
        return ParsedImportRecords([], [f"Zotero fetch error: {exc}"])

    records: list[dict[str, Any]] = []
    errors: list[str] = []
    for item in items:
        norm = importer._normalize_zotero_item(item)
        if not norm.get("title"):
            errors.append(f"Zotero item missing title (key={item.get('key', '?')})")
            continue
        records.append(norm)
    return ParsedImportRecords(records, errors)


def summarize_records(
    conn: sqlite3.Connection,
    records: Iterable[dict[str, Any]],
    *,
    source: str,
    errors: Iterable[str] = (),
) -> dict[str, Any]:
    """Return identifier, duplicate, and likely-enrichment counts for records."""
    rows = list(records)
    error_list = list(errors)
    total = len(rows) + len(error_list)
    valid = len(rows)
    with_doi = 0
    with_url_only = 0
    title_search_needed = 0
    with_abstract = 0
    rich_metadata = 0
    existing_matches = 0
    likely_new_rows = 0

    for row in rows:
        title = str(row.get("title") or "").strip()
        doi = str(row.get("doi") or "").strip()
        url = str(row.get("url") or "").strip()
        abstract = str(row.get("abstract") or "").strip()
        authors = str(row.get("authors") or "").strip()
        year = row.get("year")

        if doi:
            with_doi += 1
        elif url:
            with_url_only += 1
        if not doi:
            title_search_needed += 1
        if abstract:
            with_abstract += 1
        if doi and abstract and authors and year:
            rich_metadata += 1

        existing = importer._find_existing_paper(conn, doi, "", title, year)
        if existing:
            existing_matches += 1
        else:
            likely_new_rows += 1

    # OpenAlex can batch DOI-bearing records, while title-only resolution is
    # one search-style request per record. This is intentionally a forecast, not
    # a promise: import promotion/dedup can shrink the actual queue.
    openalex_requests = math.ceil(with_doi / 50) + title_search_needed if valid else 0
    s2_vector_candidates = with_doi
    openalex_authed, s2_authed = detect_auth()

    return {
        "source": source,
        "total_entries": total,
        "valid_entries": valid,
        "parse_errors": len(error_list),
        "errors": error_list[:20],
        "identifiers": {
            "doi": with_doi,
            "url_only": with_url_only,
            "title_search_needed": title_search_needed,
        },
        "metadata": {
            "with_abstract": with_abstract,
            "rich_enough_to_skip_most_hydration": rich_metadata,
        },
        "dedup": {
            "existing_matches": existing_matches,
            "likely_new_rows": likely_new_rows,
        },
        "likely_source_calls": {
            "openalex": openalex_requests,
            "semantic_scholar_title_search": title_search_needed,
            "semantic_scholar_vector_batch_candidates": s2_vector_candidates,
        },
        "eta": {
            "openalex": estimate_eta(
                "corpus_metadata",
                with_doi,
                openalex_authed=openalex_authed,
                s2_authed=s2_authed,
            ),
            "title_resolution": estimate_eta(
                "title_resolution",
                title_search_needed,
                openalex_authed=openalex_authed,
                s2_authed=s2_authed,
            ),
            "s2_vector": estimate_eta(
                "s2_vector",
                s2_vector_candidates,
                openalex_authed=openalex_authed,
                s2_authed=s2_authed,
            ),
        },
    }
