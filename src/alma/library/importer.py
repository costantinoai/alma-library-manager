"""Library import from external sources (BibTeX, Zotero).

Supports importing papers from .bib files / BibTeX strings and from
Zotero personal or group libraries via the Zotero Web API.
"""

from __future__ import annotations

import logging
import re
import sqlite3
import uuid
import xml.etree.ElementTree as ET
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Dict, List, Optional

from alma.core.utils import (
    generate_paper_id,
    normalize_doi as _normalize_doi_core,
    resolve_existing_paper_id,
)

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Result container
# ---------------------------------------------------------------------------

@dataclass
class ImportResult:
    """Outcome of an import operation."""

    total: int = 0          # Total items found in source
    imported: int = 0       # Successfully imported
    skipped: int = 0        # Skipped (duplicates)
    failed: int = 0         # Failed to import
    errors: List[str] = field(default_factory=list)   # Error messages
    items: List[dict] = field(default_factory=list)    # Imported items

    def to_dict(self) -> dict:
        return {
            "total": self.total,
            "imported": self.imported,
            "skipped": self.skipped,
            "failed": self.failed,
            "errors": self.errors,
            "items": self.items,
        }


# ---------------------------------------------------------------------------
# LaTeX special-character map
# ---------------------------------------------------------------------------

_LATEX_CHARS: List[tuple[str, str]] = [
    # Umlauts
    ('{\\"a}', "\u00e4"), ('{\\"o}', "\u00f6"), ('{\\"u}', "\u00fc"),
    ('{\\"A}', "\u00c4"), ('{\\"O}', "\u00d6"), ('{\\"U}', "\u00dc"),
    ('\\"a', "\u00e4"), ('\\"o', "\u00f6"), ('\\"u', "\u00fc"),
    ('\\"A', "\u00c4"), ('\\"O', "\u00d6"), ('\\"U', "\u00dc"),
    # Accents
    ("{\\'a}", "\u00e1"), ("{\\'e}", "\u00e9"), ("{\\'i}", "\u00ed"),
    ("{\\'o}", "\u00f3"), ("{\\'u}", "\u00fa"),
    ("{\\'A}", "\u00c1"), ("{\\'E}", "\u00c9"), ("{\\'I}", "\u00cd"),
    ("{\\'O}", "\u00d3"), ("{\\'U}", "\u00da"),
    ("\\'a", "\u00e1"), ("\\'e", "\u00e9"), ("\\'i", "\u00ed"),
    ("\\'o", "\u00f3"), ("\\'u", "\u00fa"),
    ("\\'A", "\u00c1"), ("\\'E", "\u00c9"), ("\\'I", "\u00cd"),
    ("\\'O", "\u00d3"), ("\\'U", "\u00da"),
    # Grave
    ('{\\`a}', "\u00e0"), ('{\\`e}', "\u00e8"), ('{\\`i}', "\u00ec"),
    ('{\\`o}', "\u00f2"), ('{\\`u}', "\u00f9"),
    ('\\`a', "\u00e0"), ('\\`e', "\u00e8"), ('\\`i', "\u00ec"),
    ('\\`o', "\u00f2"), ('\\`u', "\u00f9"),
    # Circumflex
    ('{\\^a}', "\u00e2"), ('{\\^e}', "\u00ea"), ('{\\^i}', "\u00ee"),
    ('{\\^o}', "\u00f4"), ('{\\^u}', "\u00fb"),
    ('\\^a', "\u00e2"), ('\\^e', "\u00ea"), ('\\^i', "\u00ee"),
    ('\\^o', "\u00f4"), ('\\^u', "\u00fb"),
    # Tilde
    ('{\\~n}', "\u00f1"), ('{\\~a}', "\u00e3"), ('{\\~o}', "\u00f5"),
    ('\\~n', "\u00f1"), ('\\~a', "\u00e3"), ('\\~o', "\u00f5"),
    # Cedilla
    ('{\\c{c}}', "\u00e7"), ('{\\c c}', "\u00e7"),
    ('\\c{c}', "\u00e7"), ('\\c c', "\u00e7"),
    ('{\\c{C}}', "\u00c7"), ('\\c{C}', "\u00c7"),
    # Special
    ('{\\ss}', "\u00df"), ('\\ss', "\u00df"),
    ('{\\o}', "\u00f8"), ('\\o', "\u00f8"),
    ('{\\O}', "\u00d8"), ('\\O', "\u00d8"),
    ('{\\aa}', "\u00e5"), ('\\aa', "\u00e5"),
    ('{\\AA}', "\u00c5"), ('\\AA', "\u00c5"),
    ('{\\ae}', "\u00e6"), ('\\ae', "\u00e6"),
    ('{\\AE}', "\u00c6"), ('\\AE', "\u00c6"),
    # Common symbols
    ('\\&', '&'), ('\\%', '%'), ('\\$', '$'),
    ('\\#', '#'), ('\\_', '_'),
    # Dashes
    ('---', '\u2014'), ('--', '\u2013'),
    # Quotes
    ('``', '\u201c'), ("''", '\u201d'),
    ('`', '\u2018'), ("'", '\u2019'),
]


def _clean_latex(text: str) -> str:
    """Remove LaTeX commands and convert special characters to Unicode."""
    if not text:
        return text
    for latex, uni in _LATEX_CHARS:
        text = text.replace(latex, uni)
    # Strip remaining braces
    text = text.replace('{', '').replace('}', '')
    # Strip remaining backslash commands like \emph, \textbf, etc.
    text = re.sub(r'\\[a-zA-Z]+\s*', '', text)
    return text.strip()


# ---------------------------------------------------------------------------
# BibTeX import
# ---------------------------------------------------------------------------

def _normalize_bibtex_entry(entry: dict) -> dict:
    """Normalize a bibtexparser entry to our internal format.

    Supports both bibtexparser v1 (entries are plain dicts) and v2
    (entries have ``fields_dict`` with Field objects).

    Returns a dict with keys: title, authors, year, journal, doi, url,
    abstract, keywords, entry_type.
    """
    # v2 stores fields in fields_dict; v1 stores them directly on the entry
    fields = entry.get("fields_dict", entry)

    def _get(key: str) -> str:
        """Get a field value, handling v2 Field objects and plain strings."""
        val = fields.get(key)
        if val is None:
            return ""
        # bibtexparser v2 Field objects have a .value attribute
        if hasattr(val, "value"):
            return str(val.value).strip()
        return str(val).strip()

    # Title
    title = _clean_latex(_get("title"))

    # Authors: BibTeX format "Last1, First1 and Last2, First2"
    raw_authors = _clean_latex(_get("author"))
    if raw_authors:
        # Normalize "and"-separated list to semicolon-separated.
        # This preserves embedded commas in "Last, First" names.
        parts = [a.strip() for a in re.split(r'\s+and\s+', raw_authors) if a.strip()]
        authors = "; ".join(parts)
    else:
        authors = ""

    # Year
    year_str = _get("year")
    try:
        year = int(year_str) if year_str else None
    except (ValueError, TypeError):
        year = None

    # Journal / booktitle
    journal = _clean_latex(_get("journal") or _get("booktitle"))

    # DOI (normalize)
    doi_raw = _get("doi")
    doi = _normalize_doi_value(doi_raw)

    # URL
    url = _get("url") or _get("link")

    # Abstract
    abstract = _clean_latex(_get("abstract"))

    # Keywords
    kw = _get("keywords")
    keywords = [k.strip() for k in re.split(r'[,;]', kw) if k.strip()] if kw else []

    # Entry type (v2 uses 'entry_type', v1 uses 'ENTRYTYPE')
    entry_type = entry.get("entry_type") or entry.get("ENTRYTYPE", "misc")

    return {
        "title": title,
        "authors": authors,
        "year": year,
        "journal": journal,
        "doi": doi,
        "url": url,
        "abstract": abstract,
        "keywords": keywords,
        "entry_type": entry_type,
    }


def _normalize_doi_value(doi_val: Optional[str]) -> str:
    """Normalize a DOI string to bare format, returning empty string if invalid."""
    return _normalize_doi_core(doi_val) or ""


def import_bibtex(
    bibtex_content: str,
    conn: sqlite3.Connection,
    collection_name: Optional[str] = None,
) -> ImportResult:
    """Parse a BibTeX string and import papers into the library.

    For each BibTeX entry:
    1. Extract: title, authors, year, journal, doi, url, abstract
    2. Check for duplicates (by title similarity)
    3. Create or promote one canonical saved Library entry with import provenance
    4. Optionally add to a collection

    Args:
        bibtex_content: Raw BibTeX string.
        conn: Active SQLite connection to the publications database.
        collection_name: If provided, create/find this collection and add items.

    Returns:
        ImportResult with counts and imported items.
    """
    import bibtexparser  # lazy import to avoid hard dependency at module level

    result = ImportResult()

    # Parse — support both bibtexparser v1 (.loads) and v2 (.parse)
    try:
        if hasattr(bibtexparser, "parse"):
            library = bibtexparser.parse(bibtex_content)
            entries = library.entries
        else:
            library = bibtexparser.loads(bibtex_content)
            entries = library.entries
    except Exception as exc:
        result.errors.append(f"BibTeX parse error: {exc}")
        return result
    result.total = len(entries)

    if not entries:
        return result

    # Optionally resolve collection
    collection_id: Optional[str] = None
    if collection_name:
        collection_id = _find_or_create_collection(conn, collection_name)

    imported_refs: list[str] = []

    for entry in entries:
        try:
            norm = _normalize_bibtex_entry(entry)
            title = norm["title"]
            if not title:
                result.failed += 1
                result.errors.append(f"Entry missing title: {entry.get('key', '?')}")
                continue

            notes = f"Imported from BibTeX ({norm['entry_type']})"

            # Duplicate check by progressively weaker keys: DOI -> OpenAlex ID
            # -> exact title -> (year, normalised_title). If a tracked row
            # already exists, promote that canonical row into Library instead
            # of silently skipping the user import.
            existing_id = _find_existing_paper(conn, norm["doi"], "", title, norm.get("year"))
            if existing_id:
                paper_id, promoted = _promote_existing_import_target(
                    conn,
                    existing_id,
                    added_from="import",
                )
                if promoted:
                    imported_refs.append(paper_id)
                    result.imported += 1
                    result.items.append({"paper_id": paper_id, **norm})
                else:
                    result.skipped += 1
            else:
                paper_id = _create_library_paper(
                    conn,
                    title=title,
                    authors=norm["authors"],
                    doi=norm["doi"],
                    author_id="import",
                    notes=notes,
                    rating=3,
                    year=norm["year"],
                    journal=norm["journal"],
                    abstract=norm["abstract"],
                    url=norm["url"],
                )
                imported_refs.append(paper_id)
                result.imported += 1
                result.items.append({"paper_id": paper_id, **norm})

            # Add to collection
            if collection_id:
                _add_to_collection(
                    conn,
                    collection_id,
                    paper_id,
                )

            # Import BibTeX keywords as local tags.
            for tag_name in norm.get("keywords", []):
                _find_or_create_tag_and_assign(
                    conn,
                    tag_name,
                    paper_id,
                )

        except Exception as exc:
            result.failed += 1
            key_label = entry.get("key", "unknown")
            result.errors.append(f"Failed to import '{key_label}': {exc}")

    conn.commit()

    # Fast local author-linking step so imported rows are reassigned immediately.
    _resolve_imported_authors_inline(conn)

    # Trigger background enrichment for newly imported publications
    _trigger_background_enrichment(result, imported_refs)

    return result


def import_bibtex_file(
    file_path: str,
    conn: sqlite3.Connection,
    collection_name: Optional[str] = None,
) -> ImportResult:
    """Import papers from a .bib file.

    Args:
        file_path: Path to the .bib file.
        conn: Active SQLite connection.
        collection_name: Optional collection name for grouping.

    Returns:
        ImportResult.
    """
    try:
        with open(file_path, "r", encoding="utf-8") as fh:
            content = fh.read()
    except Exception as exc:
        result = ImportResult()
        result.errors.append(f"Cannot read file: {exc}")
        return result
    return import_bibtex(content, conn, collection_name)


# ---------------------------------------------------------------------------
# Zotero import
# ---------------------------------------------------------------------------

def _normalize_zotero_item(item: dict) -> dict:
    """Normalize a Zotero API item to our internal format.

    Args:
        item: Raw item dict from pyzotero.

    Returns:
        Normalized dict with: title, authors, year, journal, doi, url,
        abstract, keywords, item_type, zotero_tags, zotero_collections.
    """
    data: dict = item.get("data", item)

    # Title
    title = (data.get("title") or "").strip()

    # Authors
    creators = data.get("creators", [])
    author_parts: list[str] = []
    for c in creators:
        first = (c.get("firstName") or "").strip()
        last = (c.get("lastName") or "").strip()
        name = c.get("name", "").strip()
        if last:
            author_parts.append(f"{last}, {first}" if first else last)
        elif name:
            author_parts.append(name)
    authors = ", ".join(author_parts)

    # Year
    date_str = data.get("date", "")
    year: Optional[int] = None
    if date_str:
        match = re.search(r'(\d{4})', date_str)
        if match:
            year = int(match.group(1))

    # Journal
    journal = (
        data.get("publicationTitle")
        or data.get("proceedingsTitle")
        or data.get("bookTitle")
        or ""
    ).strip()

    # DOI
    doi = _normalize_doi_value(data.get("DOI", ""))

    # URL
    url = (data.get("url") or "").strip()

    # Abstract
    abstract = (data.get("abstractNote") or "").strip()

    # Tags
    tags = [t.get("tag", "") for t in data.get("tags", []) if t.get("tag")]

    # Keywords (from extra or tags)
    keywords = tags[:]

    # Collections this item belongs to
    collections = data.get("collections", [])

    # Item type
    item_type = data.get("itemType", "document")

    return {
        "title": title,
        "authors": authors,
        "year": year,
        "journal": journal,
        "doi": doi,
        "url": url,
        "abstract": abstract,
        "keywords": keywords,
        "item_type": item_type,
        "zotero_tags": tags,
        "zotero_collections": collections,
    }


def import_zotero(
    library_id: str,
    api_key: str,
    conn: sqlite3.Connection,
    library_type: str = "user",
    collection_key: Optional[str] = None,
    collection_name: Optional[str] = None,
) -> ImportResult:
    """Import papers from a Zotero library.

    Uses pyzotero to fetch items from a Zotero library or collection.

    Args:
        library_id: Zotero library/user ID.
        api_key: Zotero API key.
        conn: Active SQLite connection.
        library_type: 'user' or 'group'.
        collection_key: If set, import only items from this Zotero collection.
        collection_name: If set, add all imported items to this local collection.

    Returns:
        ImportResult.
    """
    from pyzotero import zotero  # lazy import

    result = ImportResult()

    try:
        zot = zotero.Zotero(library_id, library_type, api_key)
    except Exception as exc:
        result.errors.append(f"Zotero connection error: {exc}")
        return result

    # Resolve local collection
    local_collection_id: Optional[str] = None
    if collection_name:
        local_collection_id = _find_or_create_collection(conn, collection_name)

    # Fetch items (paginated -- pyzotero handles pagination with everything())
    try:
        if collection_key:
            items = zot.collection_items(collection_key, itemType="-attachment -note")
        else:
            items = zot.everything(zot.items(itemType="-attachment -note"))
    except Exception as exc:
        result.errors.append(f"Zotero fetch error: {exc}")
        return result

    result.total = len(items)

    # Build a map of Zotero collection keys -> names for tag import
    zotero_collection_map: dict[str, str] = {}
    try:
        for coll in zot.collections():
            cdata = coll.get("data", coll)
            zotero_collection_map[cdata.get("key", "")] = cdata.get("name", "")
    except Exception:
        pass  # non-critical

    imported_refs: list[str] = []

    for item in items:
        try:
            norm = _normalize_zotero_item(item)
            title = norm["title"]
            if not title:
                result.failed += 1
                result.errors.append(
                    f"Zotero item missing title (key={item.get('key', '?')})"
                )
                continue

            # Build notes from tags
            tag_info = ""
            if norm["zotero_tags"]:
                tag_info = f"\nTags: {', '.join(norm['zotero_tags'])}"
            zotero_coll_names = [
                zotero_collection_map.get(ck, ck)
                for ck in norm.get("zotero_collections", [])
            ]
            coll_info = ""
            if zotero_coll_names:
                coll_info = f"\nZotero collections: {', '.join(zotero_coll_names)}"

            notes = f"Imported from Zotero ({norm['item_type']}){tag_info}{coll_info}"

            # Duplicate check. Existing tracked rows should be promoted into the
            # saved Library instead of being left as non-library duplicates.
            existing_id = _find_existing_paper(conn, norm["doi"], "", title, norm.get("year"))
            if existing_id:
                paper_id, promoted = _promote_existing_import_target(
                    conn,
                    existing_id,
                    added_from="import",
                )
                if promoted:
                    imported_refs.append(paper_id)
                    result.imported += 1
                    result.items.append({"paper_id": paper_id, **norm})
                else:
                    result.skipped += 1
            else:
                paper_id = _create_library_paper(
                    conn,
                    title=title,
                    authors=norm["authors"],
                    doi=norm["doi"],
                    author_id="import",
                    notes=notes.strip(),
                    rating=3,
                    year=norm["year"],
                    journal=norm["journal"],
                    abstract=norm["abstract"],
                    url=norm["url"],
                )
                imported_refs.append(paper_id)
                result.imported += 1
                result.items.append({"paper_id": paper_id, **norm})

            # Add to local collection
            if local_collection_id:
                _add_to_collection(
                    conn,
                    local_collection_id,
                    paper_id,
                )

            # Create local collections mirroring Zotero collections
            for coll_key in norm.get("zotero_collections", []):
                coll_name = zotero_collection_map.get(coll_key)
                if coll_name:
                    mirror_coll_id = _find_or_create_collection(
                        conn, coll_name, color="#8B5CF6"
                    )
                    _add_to_collection(
                        conn,
                        mirror_coll_id,
                        paper_id,
                    )

            # Import Zotero tags as local tags
            for tag_name in norm.get("zotero_tags", []):
                _find_or_create_tag_and_assign(
                    conn,
                    tag_name,
                    paper_id,
                )

        except Exception as exc:
            result.failed += 1
            result.errors.append(
                f"Failed to import Zotero item '{item.get('key', '?')}': {exc}"
            )

    conn.commit()

    # Fast local author-linking step so imported rows are reassigned immediately.
    _resolve_imported_authors_inline(conn)

    # Trigger background enrichment for newly imported publications
    _trigger_background_enrichment(result, imported_refs)

    return result


def list_zotero_collections(
    library_id: str,
    api_key: str,
    library_type: str = "user",
) -> List[dict]:
    """List collections in a Zotero library.

    Args:
        library_id: Zotero library/user ID.
        api_key: Zotero API key.
        library_type: 'user' or 'group'.

    Returns:
        List of dicts with keys: key, name, num_items.
    """
    from pyzotero import zotero  # lazy import

    zot = zotero.Zotero(library_id, library_type, api_key)
    raw = zot.collections()
    out: list[dict] = []
    for coll in raw:
        data = coll.get("data", coll)
        meta = coll.get("meta", {})
        out.append({
            "key": data.get("key", ""),
            "name": data.get("name", ""),
            "num_items": meta.get("numItems", data.get("numItems", 0)),
            "parent": data.get("parentCollection", None),
        })
    return out


def _strip_ns(tag: str) -> str:
    """Strip XML namespace prefix from a tag."""
    if "}" in tag:
        return tag.split("}", 1)[1]
    return tag


def _parse_zotero_rdf(xml_content: str) -> list[dict]:
    """Parse a Zotero RDF export into normalized import records.

    The parser is intentionally tolerant across RDF variants and relies on
    common Dublin Core / Biblio fields.
    """
    root = ET.fromstring(xml_content)
    items: list[dict] = []

    for node in root.iter():
        # Most non-item nodes are skipped early; items generally contain title.
        title = ""
        authors: list[str] = []
        year: Optional[int] = None
        journal = ""
        doi = ""
        url = ""
        abstract = ""
        keywords: list[str] = []
        item_type = _strip_ns(node.tag)

        for child in list(node):
            ctag = _strip_ns(child.tag).lower()
            ctext = (child.text or "").strip()

            if ctag == "title" and ctext:
                title = ctext
            elif ctag == "creator" and ctext:
                authors.append(ctext)
            elif ctag in {"date", "issued", "created"} and ctext and year is None:
                match = re.search(r"(\d{4})", ctext)
                if match:
                    year = int(match.group(1))
            elif ctag in {"ispartof", "publicationtitle", "journal", "container"} and ctext and not journal:
                journal = ctext
            elif ctag in {"identifier", "doi"} and ctext:
                ctext_l = ctext.lower()
                if "doi" in ctext_l and not doi:
                    # Zotero RDF encodes DOIs as "DOI 10.xxx" or "DOI: 10.xxx"
                    # (sometimes with a URL prefix). Extract the bare DOI so the
                    # stored value matches every other import path.
                    doi_match = re.search(r"(10\.\d{3,}/[^\s\"'<>]+)", ctext)
                    if doi_match:
                        doi = _normalize_doi_value(doi_match.group(1))
                    else:
                        doi = _normalize_doi_value(ctext)
                elif "arxiv" in ctext_l and not doi:
                    m = re.search(r"(\d{4}\.\d{4,5}(?:v\d+)?)", ctext, flags=re.IGNORECASE)
                    if m:
                        doi = _normalize_doi_value(f"10.48550/arXiv.{m.group(1)}")
                elif "10.1101/" in ctext_l and not doi:
                    m = re.search(r"(10\.1101/[^\s/\"'<>]+)", ctext, flags=re.IGNORECASE)
                    if m:
                        doi = _normalize_doi_value(m.group(1))
                elif ctext_l.startswith("http") and not url:
                    url = ctext
            elif ctag in {"uri", "homepage", "link"} and not url:
                url = (child.attrib.get("{http://www.w3.org/1999/02/22-rdf-syntax-ns#}resource") or ctext or "").strip()
            elif ctag in {"description", "abstract", "abstractnote"} and ctext and not abstract:
                abstract = ctext
            elif ctag in {"subject", "keyword"} and ctext:
                keywords.append(ctext)

        if not title:
            continue

        # Filter obvious collection/container resources from RDF dumps.
        if item_type.lower() in {"description", "seq", "li"}:
            continue

        if not doi and url:
            url_l = url.lower()
            if "arxiv.org" in url_l:
                m = re.search(r"(\d{4}\.\d{4,5}(?:v\d+)?)", url, flags=re.IGNORECASE)
                if m:
                    doi = _normalize_doi_value(f"10.48550/arXiv.{m.group(1)}")
            elif "biorxiv.org" in url_l:
                m = re.search(r"(10\.1101/[^\s/\"'<>]+)", url, flags=re.IGNORECASE)
                if m:
                    doi = _normalize_doi_value(m.group(1))

        items.append({
            "title": title.strip(),
            "authors": ", ".join(a for a in authors if a).strip(),
            "year": year,
            "journal": journal.strip(),
            "doi": _normalize_doi_value(doi),
            "url": url.strip(),
            "abstract": abstract.strip(),
            "keywords": keywords,
            "item_type": item_type,
        })

    # Deduplicate by title (RDF can contain resource aliases)
    deduped: dict[str, dict] = {}
    for item in items:
        key = item["title"].strip().lower()
        if key and key not in deduped:
            deduped[key] = item
    return list(deduped.values())


def import_zotero_rdf(
    rdf_content: str,
    conn: sqlite3.Connection,
    collection_name: Optional[str] = None,
) -> ImportResult:
    """Import papers from a Zotero RDF export file content."""
    result = ImportResult()
    try:
        items = _parse_zotero_rdf(rdf_content)
    except Exception as exc:
        result.errors.append(f"Zotero RDF parse error: {exc}")
        return result

    result.total = len(items)
    if not items:
        return result

    local_collection_id: Optional[str] = None
    if collection_name:
        local_collection_id = _find_or_create_collection(conn, collection_name)

    imported_refs: list[str] = []

    for item in items:
        try:
            title = item["title"]
            notes = f"Imported from Zotero RDF ({item.get('item_type', 'item')})"
            existing_id = _find_existing_paper(conn, item.get("doi", ""), "", title, item.get("year"))
            if existing_id:
                paper_id, promoted = _promote_existing_import_target(
                    conn,
                    existing_id,
                    added_from="import",
                )
                if promoted:
                    imported_refs.append(paper_id)
                    result.imported += 1
                    result.items.append({"paper_id": paper_id, **item})
                else:
                    result.skipped += 1
            else:
                paper_id = _create_library_paper(
                    conn,
                    title=title,
                    authors=item.get("authors", ""),
                    doi=item.get("doi", ""),
                    author_id="import",
                    notes=notes,
                    rating=3,
                    year=item.get("year"),
                    journal=item.get("journal"),
                    abstract=item.get("abstract"),
                    url=item.get("url"),
                )
                imported_refs.append(paper_id)
                result.imported += 1
                result.items.append({"paper_id": paper_id, **item})

            if local_collection_id:
                _add_to_collection(
                    conn,
                    local_collection_id,
                    paper_id,
                )

            for tag_name in item.get("keywords", []):
                _find_or_create_tag_and_assign(
                    conn,
                    tag_name,
                    paper_id,
                )
        except Exception as exc:
            result.failed += 1
            result.errors.append(f"Failed to import RDF item '{item.get('title', '?')}': {exc}")

    conn.commit()
    _resolve_imported_authors_inline(conn)
    _trigger_background_enrichment(result, imported_refs)
    return result


# ---------------------------------------------------------------------------
# Database helpers
# ---------------------------------------------------------------------------

def _find_or_create_collection(
    conn: sqlite3.Connection,
    name: str,
    color: str = "#3B82F6",
) -> str:
    """Find an existing collection by name or create a new one.

    Args:
        conn: SQLite connection.
        name: Collection name.
        color: Hex color for the collection badge.

    Returns:
        Collection ID (UUID hex string).
    """
    row = conn.execute(
        "SELECT id FROM collections WHERE name = ?", (name,)
    ).fetchone()
    if row:
        return row["id"] if isinstance(row, sqlite3.Row) else row[0]

    cid = uuid.uuid4().hex
    now = datetime.utcnow().isoformat()
    conn.execute(
        "INSERT INTO collections (id, name, description, color, created_at) VALUES (?, ?, ?, ?, ?)",
        (cid, name, f"Imported collection: {name}", color, now),
    )
    return cid


def _find_existing_paper(
    conn: sqlite3.Connection,
    doi: str,
    openalex_id: str,
    title: str,
    year: Optional[int] = None,
) -> Optional[str]:
    """Find an existing paper row for import dedup.

    Delegates to ``resolve_existing_paper_id`` for the canonical triple
    (openalex_id → doi → year+normalized_title). When year is missing,
    falls back to a case-insensitive exact-title match so imports that
    only carry a title still dedupe.
    """
    hit = resolve_existing_paper_id(
        conn,
        openalex_id=openalex_id,
        doi=doi,
        title=title,
        year=year,
    )
    if hit:
        return hit

    if year is None and title:
        row = conn.execute(
            "SELECT id FROM papers WHERE LOWER(title) = LOWER(?)", (title,)
        ).fetchone()
        if row:
            return row["id"] if isinstance(row, sqlite3.Row) else row[0]

    return None


def _create_library_paper(
    conn: sqlite3.Connection,
    title: str,
    authors: str,
    doi: str = "",
    author_id: str = "import",
    notes: Optional[str] = None,
    rating: int = 3,
    year: Optional[int] = None,
    journal: Optional[str] = None,
    abstract: Optional[str] = None,
    url: Optional[str] = None,
) -> str:
    """Create a saved Library paper with import provenance.

    Args:
        conn: SQLite connection.
        title: Publication title (required).
        authors: Comma-separated author string.
        doi: Normalized DOI.
        author_id: Author ID (defaults to 'import' for external imports).
        notes: User notes.
        rating: Rating (0-5).
        year: Publication year.
        journal: Journal or venue name.
        abstract: Publication abstract.
        url: Publication URL.

    Returns:
        paper_id (UUID string).
    """
    paper_id = generate_paper_id()
    now = datetime.utcnow().isoformat()

    conn.execute(
        """INSERT INTO papers
           (id, author_id, title, year, abstract, url, doi,
            cited_by_count, journal, authors, fetched_at, status, notes, rating,
            added_at, added_from,
            openalex_resolution_status, openalex_resolution_reason, openalex_resolution_updated_at)
           VALUES (?, ?, ?, ?, ?, ?, ?, 0, ?, ?, ?, 'library', ?, ?, ?, ?, ?, ?, ?)""",
        (
            paper_id,
            author_id,
            title,
            year,
            abstract,
            url,
            doi,
            journal,
            authors,
            now,
            notes,
            rating,
            now,
            "import",
            "pending_enrichment",
            "imported_metadata_only",
            now,
        ),
    )
    conn.commit()

    return paper_id


def _promote_existing_import_target(
    conn: sqlite3.Connection,
    paper_id: str,
    *,
    added_from: str = "import",
) -> tuple[str, bool]:
    """Promote an existing tracked paper into Library for an import flow.

    Returns ``(paper_id, promoted)`` where ``promoted`` is True only when the
    row was not already in Library and the import meaningfully changed
    membership.
    """
    row = conn.execute(
        "SELECT id, status, COALESCE(rating, 0) AS rating FROM papers WHERE id = ?",
        (paper_id,),
    ).fetchone()
    if not row:
        return paper_id, False

    status = str(row["status"] or "").strip().lower() if isinstance(row, sqlite3.Row) else str(row[1] or "").strip().lower()
    if status == "library":
        return paper_id, False

    from alma.application import library as library_app

    current_rating = int((row["rating"] if isinstance(row, sqlite3.Row) else row[2]) or 0) or 3
    library_app.add_to_library(
        conn,
        paper_id,
        rating=current_rating,
        notes=None,
        added_from=added_from,
        override_added_from=True,
    )
    return paper_id, True


def _add_to_collection(
    conn: sqlite3.Connection,
    collection_id: str,
    paper_id: str,
) -> None:
    """Add a publication to a collection (ignore if already present)."""
    now = datetime.utcnow().isoformat()
    try:
        conn.execute(
            "INSERT OR IGNORE INTO collection_items (collection_id, paper_id, added_at) "
            "VALUES (?, ?, ?)",
            (collection_id, paper_id, now),
        )
    except sqlite3.IntegrityError:
        pass  # already there


def _find_or_create_tag_and_assign(
    conn: sqlite3.Connection,
    tag_name: str,
    paper_id: str,
    color: str = "#6B7280",
) -> None:
    """Find or create a tag by name and assign it to a publication."""
    row = conn.execute("SELECT id FROM tags WHERE name = ?", (tag_name,)).fetchone()
    if row:
        tid = row["id"] if isinstance(row, sqlite3.Row) else row[0]
    else:
        tid = uuid.uuid4().hex
        try:
            conn.execute(
                "INSERT INTO tags (id, name, color) VALUES (?, ?, ?)",
                (tid, tag_name, color),
            )
        except sqlite3.IntegrityError:
            # Race condition or duplicate -- fetch again
            row = conn.execute("SELECT id FROM tags WHERE name = ?", (tag_name,)).fetchone()
            tid = row["id"] if isinstance(row, sqlite3.Row) else row[0]

    try:
        conn.execute(
            "INSERT OR IGNORE INTO publication_tags (paper_id, tag_id) VALUES (?, ?)",
            (paper_id, tid),
        )
    except sqlite3.IntegrityError:
        pass


def _resolve_imported_authors_inline(conn: sqlite3.Connection) -> None:
    """Run local author reassignment synchronously after import.

    This keeps UI state consistent immediately after import while the heavier
    OpenAlex/ID-resolution pipeline continues in the background.
    """
    try:
        from alma.library.enrichment import resolve_imported_authors

        resolve_imported_authors(conn)
        conn.commit()
    except Exception as exc:
        logger.warning("Inline import author-linking failed: %s", exc)


def _lookup_import_target(
    conn: sqlite3.Connection,
    paper_id: str,
) -> Optional[str]:
    """Resolve a post-import publication row even if forms changed.

    During post-import processing, owner IDs can be remapped (``import`` ->
    tracked/import_author IDs). This helper finds the current row by ID.
    """
    pid = (paper_id or "").strip()
    if not pid:
        return None

    # Fast path: exact ID present.
    row = conn.execute(
        "SELECT id FROM papers WHERE id = ?",
        (pid,),
    ).fetchone()
    if row:
        return row["id"]

    return None


def _trigger_background_enrichment(
    result: ImportResult,
    imported_refs: Optional[list[str]] = None,
) -> None:
    """Spawn a background thread to enrich newly imported publications.

    Only runs when the import actually added new papers.  Failures are
    logged but never propagated -- enrichment is a best-effort step.
    """
    if result.imported <= 0:
        return

    def _read_setting(conn: sqlite3.Connection, key: str, default: str = "") -> str:
        try:
            row = conn.execute("SELECT value FROM discovery_settings WHERE key = ?", (key,)).fetchone()
            if row is None:
                return default
            return row["value"] if isinstance(row, sqlite3.Row) else row[0]
        except Exception:
            return default

    def _auto_resolve_author_ids(conn: sqlite3.Connection, job_id: str) -> dict:
        """Resolve OpenAlex/Scholar IDs for unresolved authors.

        Uses the optimized bulk resolver: publication-first (DB lookup) then
        concurrent fallback title search for remaining unresolved authors.
        """
        from alma.api.routes.authors import (
            _ensure_author_resolution_columns,
            _resolve_identifiers_bulk_optimized,
        )

        _ensure_author_resolution_columns(conn)
        unresolved_no_publications = int(
            conn.execute(
                """
                SELECT COUNT(*) AS c
                FROM authors a
                WHERE COALESCE(a.id_resolution_status, '') IN ('', 'unresolved', 'needs_manual_review', 'no_match', 'error')
                  AND NOT EXISTS (
                    SELECT 1
                    FROM papers p
                    WHERE p.author_id = a.id
                  )
                """
            ).fetchone()["c"] or 0
        )
        unresolved_placeholder_no_publications = int(
            conn.execute(
                """
                SELECT COUNT(*) AS c
                FROM authors a
                WHERE a.id LIKE 'import_author_%'
                  AND COALESCE(a.id_resolution_status, '') IN ('', 'unresolved', 'needs_manual_review', 'no_match', 'error')
                  AND NOT EXISTS (
                    SELECT 1
                    FROM papers p
                    WHERE p.author_id = a.id
                  )
                """
            ).fetchone()["c"] or 0
        )
        unresolved_non_placeholder_no_publications = max(
            0,
            unresolved_no_publications - unresolved_placeholder_no_publications,
        )
        rows = conn.execute(
            """
            SELECT a.id, a.name
            FROM authors a
            WHERE COALESCE(a.id_resolution_status, '') IN ('', 'unresolved', 'needs_manual_review', 'no_match', 'error')
              AND EXISTS (
                SELECT 1
                FROM papers p
                WHERE p.author_id = a.id
              )
            ORDER BY a.name
            LIMIT 5000
            """
        ).fetchall()

        if not rows:
            return {
                "total": 0,
                "resolved_auto": 0,
                "needs_manual_review": 0,
                "no_match": 0,
                "error": 0,
                "skipped_no_publications": unresolved_no_publications,
                "skipped_placeholder_no_publications": unresolved_placeholder_no_publications,
                "skipped_non_placeholder_no_publications": unresolved_non_placeholder_no_publications,
            }

        from alma.api.scheduler import add_job_log
        skipped_no_publications = unresolved_no_publications
        skip_details = []
        if unresolved_placeholder_no_publications:
            skip_details.append(f"placeholder_no_pubs={unresolved_placeholder_no_publications}")
        if unresolved_non_placeholder_no_publications:
            skip_details.append(f"other_no_pubs={unresolved_non_placeholder_no_publications}")
        skip_suffix = f" ({', '.join(skip_details)})" if skip_details else ""
        add_job_log(
            job_id,
            (
                f"Resolving identifiers for {len(rows)} unresolved authors with linked publications "
                f"(skipped={skipped_no_publications} with no publications){skip_suffix}"
            ),
            step="author_id_resolution",
        )

        authors = [(row["id"], row["name"]) for row in rows]
        summary = _resolve_identifiers_bulk_optimized(conn, authors, job_id, max_workers=10)
        summary["skipped_no_publications"] = skipped_no_publications
        summary["skipped_placeholder_no_publications"] = unresolved_placeholder_no_publications
        summary["skipped_non_placeholder_no_publications"] = unresolved_non_placeholder_no_publications
        return summary

    try:
        from alma.api.scheduler import (
            add_job_log,
            schedule_immediate,
            set_job_status,
        )
        from alma.library.enrichment import enrich_all_unenriched, enrich_publication
        from alma.library.deduplication import run_deduplication
        from alma.config import get_db_path

        job_id = f"import_postprocess_{uuid.uuid4().hex[:10]}"
        refs = imported_refs or []
        pipeline_total = 5 if refs else 4
        set_job_status(
            job_id,
            status="running",
            started_at=datetime.utcnow().isoformat(),
            operation_key="imports.postprocess",
            trigger_source="user",
            message="Post-import pipeline: enrich, resolve IDs, dedup, embeddings",
            processed=0,
            total=pipeline_total,
        )
        add_job_log(
            job_id,
            (
                f"Post-import pipeline started for {result.imported} imported items "
                "(stages: enrichment -> author linking -> author ID resolution -> dedup -> embeddings)"
            ),
            step="start",
        )

        def _subtask_job_id(stage_key: str) -> str:
            return f"{job_id}_{stage_key}"

        def _run_stage(
            conn: sqlite3.Connection,
            stage_key: str,
            stage_label: str,
            stage_index: int,
            stage_total: int,
            runner,
        ) -> tuple[str, dict]:
            subtask_id = _subtask_job_id(stage_key)
            set_job_status(
                subtask_id,
                status="running",
                started_at=datetime.utcnow().isoformat(),
                operation_key=f"imports.postprocess.{stage_key}",
                trigger_source="subtask",
                parent_job_id=job_id,
                stage=stage_key,
                stage_label=stage_label,
                stage_index=stage_index,
                stage_total=stage_total,
                processed=0,
                total=1,
                message=f"{stage_label} started",
            )
            add_job_log(
                subtask_id,
                f"Started subtask stage {stage_index}/{stage_total}: {stage_label}",
                step="start",
                data={"parent_job_id": job_id},
            )
            set_job_status(
                job_id,
                status="running",
                processed=stage_index - 1,
                total=stage_total,
                current_stage=stage_key,
                message=f"Running stage {stage_index}/{stage_total}: {stage_label}",
            )
            add_job_log(
                job_id,
                f"Subtask started: {stage_label}",
                step=f"{stage_key}_start",
                data={"subtask_job_id": subtask_id},
            )
            try:
                summary = runner(subtask_id)
            except Exception as exc:
                add_job_log(
                    subtask_id,
                    f"{stage_label} failed: {exc}",
                    level="ERROR",
                    step="failed",
                )
                set_job_status(
                    subtask_id,
                    status="failed",
                    finished_at=datetime.utcnow().isoformat(),
                    error=str(exc),
                    message=f"{stage_label} failed",
                    parent_job_id=job_id,
                    stage=stage_key,
                    stage_label=stage_label,
                    stage_index=stage_index,
                    stage_total=stage_total,
                )
                add_job_log(
                    job_id,
                    f"Subtask failed: {stage_label}: {exc}",
                    level="ERROR",
                    step=f"{stage_key}_failed",
                    data={"subtask_job_id": subtask_id},
                )
                raise

            set_job_status(
                subtask_id,
                status="completed",
                finished_at=datetime.utcnow().isoformat(),
                message=f"{stage_label} completed",
                result=summary,
                parent_job_id=job_id,
                stage=stage_key,
                stage_label=stage_label,
                stage_index=stage_index,
                stage_total=stage_total,
            )
            add_job_log(
                subtask_id,
                f"{stage_label} completed",
                step="done",
                data=summary if isinstance(summary, dict) else {"summary": str(summary)},
            )
            set_job_status(
                job_id,
                status="running",
                processed=stage_index,
                total=stage_total,
                message=f"Completed stage {stage_index}/{stage_total}: {stage_label}",
            )
            add_job_log(
                job_id,
                f"Subtask completed: {stage_label}",
                step=f"{stage_key}_done",
                data={"subtask_job_id": subtask_id},
            )
            return subtask_id, summary if isinstance(summary, dict) else {"value": summary}

        def _run_targeted_enrichment_stage(
            conn: sqlite3.Connection,
            valid_refs: list[str],
            stage_job_id: str,
        ) -> dict:
            from alma.openalex.client import backfill_missing_publication_references

            seen: set[str] = set()
            total = max(1, len(valid_refs))
            processed = 0
            enriched = 0
            skipped = 0
            failed = 0
            enriched_targets: list[str] = []
            skip_reasons: dict[str, int] = {}
            skip_examples: dict[str, list[str]] = {}
            add_job_log(
                stage_job_id,
                f"Running targeted enrichment for {len(valid_refs)} references",
                step="targeted_enrichment_start",
            )
            set_job_status(stage_job_id, status="running", processed=0, total=total)

            for paper_id in valid_refs:
                if paper_id in seen:
                    continue
                seen.add(paper_id)
                try:
                    target = _lookup_import_target(conn, paper_id)
                    if not target:
                        out = {"enriched": False, "reason": "publication_not_found_after_import"}
                    else:
                        out = enrich_publication(target, conn)
                    if out.get("enriched"):
                        enriched += 1
                        if target:
                            enriched_targets.append(target)
                    else:
                        skipped += 1
                        reason = str(out.get("reason", "unknown"))
                        skip_reasons[reason] = skip_reasons.get(reason, 0) + 1
                        title_hint = str(out.get("title") or paper_id or "").strip()
                        if reason not in skip_examples:
                            skip_examples[reason] = []
                        if title_hint and len(skip_examples[reason]) < 3:
                            skip_examples[reason].append(title_hint[:180])
                except Exception as exc:
                    failed += 1
                    logger.warning(
                        "Targeted enrichment failed for %s: %s",
                        paper_id, exc,
                    )
                    add_job_log(
                        stage_job_id,
                        f"Targeted enrichment error for paper '{paper_id}': {exc}",
                        level="ERROR",
                        step="targeted_enrichment_item",
                    )
                processed += 1
                if processed % 25 == 0 or processed == total:
                    top_reasons = sorted(
                        skip_reasons.items(),
                        key=lambda kv: kv[1],
                        reverse=True,
                    )[:4]
                    reason_text = ", ".join(f"{k}={v}" for k, v in top_reasons) if top_reasons else "none"
                    add_job_log(
                        stage_job_id,
                        (
                            f"Targeted enrichment progress {processed}/{total} "
                            f"(enriched={enriched}, skipped={skipped}, failed={failed}; reasons: {reason_text})"
                        ),
                        step="targeted_enrichment_progress",
                        data={
                            "skip_reasons": dict(skip_reasons),
                            "skip_examples": {k: v for k, v in skip_examples.items()},
                        },
                    )
                    set_job_status(
                        stage_job_id,
                        status="running",
                        processed=processed,
                        total=total,
                    )

            graph_backfill = {
                "candidates": 0,
                "fetched": 0,
                "papers_updated": 0,
                "references_inserted": 0,
            }
            if enriched_targets:
                try:
                    graph_backfill = backfill_missing_publication_references(
                        conn,
                        paper_ids=enriched_targets,
                        limit=len(enriched_targets),
                    )
                    add_job_log(
                        stage_job_id,
                        "Reference backfill completed for targeted enrichment",
                        step="targeted_enrichment_graph_backfill",
                        data=graph_backfill,
                    )
                except Exception as exc:
                    add_job_log(
                        stage_job_id,
                        f"Reference backfill failed after targeted enrichment: {exc}",
                        level="WARNING",
                        step="targeted_enrichment_graph_backfill_error",
                    )

            summary = {
                "total": len(seen),
                "processed": processed,
                "enriched": enriched,
                "skipped": skipped,
                "failed": failed,
                "skip_reasons": dict(skip_reasons),
                "skip_examples": {k: v for k, v in skip_examples.items()},
                "graph_backfill": graph_backfill,
            }
            add_job_log(
                stage_job_id,
                "Targeted enrichment summary",
                step="targeted_enrichment_summary",
                data=summary,
            )
            return summary

        def _bg_enrich() -> dict:
            from alma.api.deps import open_db_connection

            conn = open_db_connection()
            try:
                subtask_jobs: dict[str, str] = {}
                if refs:
                    from alma.library.enrichment import resolve_imported_authors

                    valid_refs = [r for r in refs if r]
                    stage_plan = [
                        (
                            "enrich",
                            "Targeted OpenAlex enrichment",
                            lambda sid: _run_targeted_enrichment_stage(conn, valid_refs, sid),
                        ),
                        (
                            "author_linking",
                            "Author linking to tracked profiles",
                            lambda sid: resolve_imported_authors(conn),
                        ),
                        (
                            "resolve_ids",
                            "Author ID resolution",
                            lambda sid: _auto_resolve_author_ids(conn, sid),
                        ),
                        (
                            "dedup",
                            "Deduplicate database entities",
                            lambda sid: run_deduplication(conn, job_id=sid),
                        ),
                    ]
                    stage_total = len(stage_plan)
                    stage_results: dict[str, dict] = {}
                    for idx, (stage_key, stage_label, runner) in enumerate(stage_plan, start=1):
                        sid, summary = _run_stage(
                            conn,
                            stage_key=stage_key,
                            stage_label=stage_label,
                            stage_index=idx,
                            stage_total=stage_total,
                            runner=runner,
                        )
                        subtask_jobs[stage_key] = sid
                        stage_results[stage_key] = summary

                    add_job_log(
                        job_id,
                        "Post-import pipeline completed",
                        step="done",
                        data={"subtasks": subtask_jobs},
                    )
                    enrich_summary = stage_results.get("enrich", {})
                    return {
                        "enrichment": enrich_summary,
                        "resolved_authors": stage_results.get("author_linking", {}),
                        "resolved_author_ids": stage_results.get("resolve_ids", {}),
                        "dedup": stage_results.get("dedup", {}),
                        "targeted_enrichment_total": int(enrich_summary.get("total") or 0),
                        "targeted_enriched": int(enrich_summary.get("enriched") or 0),
                        "subtasks": subtask_jobs,
                    }

                add_job_log(
                    job_id,
                    "No explicit refs found; running full post-import pipeline",
                    step="full_enrichment",
                )
                stage_plan = [
                    (
                        "enrich",
                        "Full OpenAlex enrichment",
                        lambda sid: enrich_all_unenriched(conn, job_id=sid),
                    ),
                    (
                        "resolve_ids",
                        "Author ID resolution",
                        lambda sid: _auto_resolve_author_ids(conn, sid),
                    ),
                    (
                        "dedup",
                        "Deduplicate database entities",
                        lambda sid: run_deduplication(conn, job_id=sid),
                    ),
                ]
                stage_total = len(stage_plan)
                stage_results: dict[str, dict] = {}
                for idx, (stage_key, stage_label, runner) in enumerate(stage_plan, start=1):
                    sid, summary = _run_stage(
                        conn,
                        stage_key=stage_key,
                        stage_label=stage_label,
                        stage_index=idx,
                        stage_total=stage_total,
                        runner=runner,
                    )
                    subtask_jobs[stage_key] = sid
                    stage_results[stage_key] = summary

                add_job_log(
                    job_id,
                    "Post-import pipeline completed",
                    step="done",
                    data={"subtasks": subtask_jobs},
                )
                return {
                    "enrichment": stage_results.get("enrich", {}),
                    "resolved_author_ids": stage_results.get("resolve_ids", {}),
                    "dedup": stage_results.get("dedup", {}),
                    "embeddings": stage_results.get("embeddings", {}),
                    "subtasks": subtask_jobs,
                }
            finally:
                conn.close()

        schedule_immediate(job_id, _bg_enrich)
        logger.info(
            "Background post-import processing triggered for %d imported papers (job=%s)",
            result.imported,
            job_id,
        )
    except Exception as e:
        logger.warning("Failed to trigger background post-import processing: %s", e)
