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
from difflib import SequenceMatcher
from typing import Any, Dict, List, Optional

from alma.core.db_write import commit_unless_gated, run_write_unit, write_section
from alma.core.sql_helpers import standalone_paper_sql
from alma.core.utils import (
    clean_display_text,
    generate_paper_id,
    normalize_text,
    normalize_doi as _normalize_doi_core,
    resolve_existing_paper_id,
    strong_identifiers_conflict,
)

logger = logging.getLogger(__name__)

LOW_CONFIDENCE_IMPORT_TITLE_THRESHOLD = 0.70

# ---------------------------------------------------------------------------
# Result container
# ---------------------------------------------------------------------------

@dataclass
class ImportResult:
    """Outcome of an import operation."""

    total: int = 0          # Total items found in source
    imported: int = 0       # Successfully imported
    staged: int = 0         # Held for explicit review before Library save
    skipped: int = 0        # Skipped (duplicates)
    failed: int = 0         # Failed to import
    parse_duplicates: int = 0  # Canonical-identity collapses dropped in parsing
    errors: List[str] = field(default_factory=list)   # Error messages
    items: List[dict] = field(default_factory=list)    # Imported items

    def to_dict(self) -> dict:
        return {
            "total": self.total,
            "imported": self.imported,
            "staged": self.staged,
            "skipped": self.skipped,
            "failed": self.failed,
            "parse_duplicates": self.parse_duplicates,
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
    ('\\textbar', '|'),
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
    # Strip any HTML, then repair LaTeX-leaked dotless-ı + combining marks and
    # NFC-normalize so the imported name/title is stored clean (e.g.
    # `Rodrı́guez` → `Rodríguez`).
    return clean_display_text(text.strip())


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

    # Target collection is created INSIDE the write gate below. `write_section`
    # does a `conn.rollback()` on entry, so a row inserted here (before the gate)
    # would be discarded — the collection must be born within the committed unit.
    collection_id: Optional[str] = None

    postprocess_refs: list[str] = []

    # Pre-pass (NO write gate): normalize entries and pre-compute fuzzy-title
    # matches. The O(pool·items) dedup scan is read-only + CPU and must run
    # before we hold the writer gate (40.4; write rule 2: gather then write).
    prepared: list[tuple[dict, dict]] = []  # (entry, norm)
    for entry in entries:
        try:
            norm = _normalize_bibtex_entry(entry)
        except Exception as exc:
            result.failed += 1
            result.errors.append(f"Failed to import '{entry.get('key', 'unknown')}': {exc}")
            continue
        if not norm["title"]:
            result.failed += 1
            result.errors.append(f"Entry missing title: {entry.get('key', '?')}")
            continue
        prepared.append((entry, norm))
    fuzzy_matches = _precompute_fuzzy_matches(conn, [n for _, n in prepared])

    # BibTeX was parsed locally above (no network) — the insert loop is the
    # write window; gate it (BEGIN IMMEDIATE + writer gate) instead of a raw
    # end-of-loop commit.
    with write_section(conn, label="import bibtex"):
        # Create/find the target collection here (inside the gate) so its row
        # commits atomically with the imported papers.
        if collection_name:
            collection_id = _find_or_create_collection(conn, collection_name)
        for (entry, norm), fuzzy in zip(prepared, fuzzy_matches):
            try:
                title = norm["title"]
                notes = f"Imported from BibTeX ({norm['entry_type']})"

                paper_id, outcome = _import_or_stage_paper(
                    conn,
                    title=title,
                    authors=norm["authors"],
                    doi=norm["doi"],
                    notes=notes,
                    year=norm["year"],
                    journal=norm["journal"],
                    abstract=norm["abstract"],
                    url=norm["url"],
                    fuzzy_match=fuzzy,
                )
                _record_import_outcome(result, postprocess_refs, paper_id, outcome, norm)

                # Add to collection. Staged (low-confidence) rows are included:
                # collection_items holds the membership as a deferred link and
                # get_collection_papers hides it until the row is confirmed into
                # the Library, so the collection target survives staging.
                if collection_id:
                    _add_to_collection(
                        conn,
                        collection_id,
                        paper_id,
                    )

                # Import BibTeX keywords as local tags.
                if outcome != "staged":
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

    # Fast local author-linking step so imported rows are reassigned immediately.
    _resolve_imported_authors_inline(conn)

    # Trigger background enrichment for newly imported publications
    _trigger_background_enrichment(result, postprocess_refs)

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

def _build_zotero_collection_paths(
    collections: List[Dict[str, Any]],
) -> Dict[str, str]:
    """Resolve Zotero collection keys to "Parent / Child" path strings.

    ALMa's ``collections.name`` column is flat and ``UNIQUE``, so we encode
    the Zotero hierarchy in the name itself. This preserves nesting visually
    and prevents two same-named Zotero collections in different parents from
    silently merging into one local collection.

    Args:
        collections: Iterable of dicts with ``key``, ``name``, ``parent_key``.
            ``parent_key`` is the key of the parent collection or None/False
            for top-level collections.

    Returns:
        Mapping of collection key to its resolved path string. Paths use
        ``" / "`` as the separator. Cycles (malformed inputs) are broken
        defensively so the function never recurses forever.
    """
    name_by_key: Dict[str, str] = {}
    parent_by_key: Dict[str, Optional[str]] = {}
    for c in collections:
        key = (c.get("key") or "").strip()
        if not key:
            continue
        name_by_key[key] = (c.get("name") or "").strip()
        parent = c.get("parent_key")
        if parent is False or not parent:
            parent_by_key[key] = None
        else:
            parent_by_key[key] = str(parent).strip() or None

    def _build(key: str, seen: frozenset) -> str:
        name = name_by_key.get(key, "")
        if key in seen:
            # Cycle in input: stop unwinding and return the local name only.
            return name
        parent = parent_by_key.get(key)
        if parent and parent in name_by_key:
            parent_path = _build(parent, seen | {key})
            if parent_path and name:
                return f"{parent_path} / {name}"
            if parent_path:
                return parent_path
        return name

    return {key: _build(key, frozenset()) for key in name_by_key}


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

    # Target collection is created INSIDE the write gate below (see the RDF/BibTeX
    # importers): `write_section` rolls back on entry, so a pre-gate INSERT is lost.
    local_collection_id: Optional[str] = None

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

    # Build a map of Zotero collection keys -> "Parent / Child" path strings.
    # We resolve hierarchy here (rather than at every item) so that mirrored
    # local collections preserve nesting and same-name siblings under
    # different parents don't silently merge into one local collection.
    zotero_collections_meta: list[dict] = []
    try:
        for coll in zot.collections():
            cdata = coll.get("data", coll)
            ckey = (cdata.get("key") or "").strip()
            if not ckey:
                continue
            zotero_collections_meta.append({
                "key": ckey,
                "name": cdata.get("name") or "",
                "parent_key": cdata.get("parentCollection"),
            })
    except Exception:
        pass  # non-critical
    zotero_collection_path_map: Dict[str, str] = _build_zotero_collection_paths(
        zotero_collections_meta
    )

    postprocess_refs: list[str] = []

    # Pre-pass (NO write gate): normalize items and pre-compute fuzzy-title
    # matches before we hold the writer gate (40.4; gather then write).
    prepared: list[tuple[dict, dict]] = []  # (item, norm)
    for item in items:
        try:
            norm = _normalize_zotero_item(item)
        except Exception as exc:
            result.failed += 1
            result.errors.append(f"Failed to import Zotero item '{item.get('key', '?')}': {exc}")
            continue
        if not norm["title"]:
            result.failed += 1
            result.errors.append(f"Zotero item missing title (key={item.get('key', '?')})")
            continue
        prepared.append((item, norm))
    fuzzy_matches = _precompute_fuzzy_matches(conn, [n for _, n in prepared])

    # Zotero items were fetched above (network); the insert loop is local —
    # gate it (BEGIN IMMEDIATE + writer gate) instead of a raw end-of-loop
    # commit.
    with write_section(conn, label="import zotero"):
        # Create/find the target collection here (inside the gate) so its row
        # commits atomically with the imported papers.
        if collection_name:
            local_collection_id = _find_or_create_collection(conn, collection_name)
        for (item, norm), fuzzy in zip(prepared, fuzzy_matches):
            try:
                title = norm["title"]

                # Resolve membership keys to full "Parent / Child" paths. Drop any
                # blanks (orphan keys whose collection metadata wasn't returned).
                resolved_paths: list[str] = []
                seen_paths: set[str] = set()
                for ck in norm.get("zotero_collections", []):
                    path = zotero_collection_path_map.get(ck, "").strip()
                    if path and path not in seen_paths:
                        resolved_paths.append(path)
                        seen_paths.add(path)
                norm["zotero_collections"] = resolved_paths

                # Build notes from tags
                tag_info = ""
                if norm["zotero_tags"]:
                    tag_info = f"\nTags: {', '.join(norm['zotero_tags'])}"
                coll_info = ""
                if resolved_paths:
                    coll_info = f"\nZotero collections: {', '.join(resolved_paths)}"

                notes = f"Imported from Zotero ({norm['item_type']}){tag_info}{coll_info}"

                paper_id, outcome = _import_or_stage_paper(
                    conn,
                    title=title,
                    authors=norm["authors"],
                    doi=norm["doi"],
                    notes=notes.strip(),
                    year=norm["year"],
                    journal=norm["journal"],
                    abstract=norm["abstract"],
                    url=norm["url"],
                    fuzzy_match=fuzzy,
                )
                _record_import_outcome(result, postprocess_refs, paper_id, outcome, norm)

                # Add to local collection. Staged rows included (deferred
                # membership, hidden until confirmed — see BibTeX loop above).
                if local_collection_id:
                    _add_to_collection(
                        conn,
                        local_collection_id,
                        paper_id,
                    )

                # Create local collections mirroring Zotero collections. Names
                # carry the full "Parent / Child" path so the Zotero hierarchy is
                # preserved (ALMa's collections.name is flat + UNIQUE).
                for coll_path in resolved_paths:
                    mirror_coll_id = _find_or_create_collection(
                        conn, coll_path, color="#8B5CF6"
                    )
                    _add_to_collection(
                        conn,
                        mirror_coll_id,
                        paper_id,
                    )

                # Import Zotero tags as local tags
                if outcome != "staged":
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

    # Fast local author-linking step so imported rows are reassigned immediately.
    _resolve_imported_authors_inline(conn)

    # Trigger background enrichment for newly imported publications
    _trigger_background_enrichment(result, postprocess_refs)

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


_RDF_NS = "{http://www.w3.org/1999/02/22-rdf-syntax-ns#}"
_RDF_ABOUT = f"{_RDF_NS}about"
_RDF_RESOURCE = f"{_RDF_NS}resource"
_DOI_IN_TEXT_RE = re.compile(r"(10\.\d{3,9}/[^\s\"']+)", flags=re.IGNORECASE)

_ZOTERO_RDF_SKIP_TAGS = {
    "attachment",
    "collection",
    "journal",
    "li",
    "memo",
    "seq",
}
_ZOTERO_RDF_BIBLIO_TAGS = {
    "article",
    "book",
    "booksection",
    "chapter",
    "document",
    "proceedings",
    "report",
    "thesis",
}
_ZOTERO_RDF_SKIP_ITEM_TYPES = {
    "annotation",
    "attachment",
    "note",
}


def _rdf_child_text(node: ET.Element, local_name: str) -> str:
    for child in list(node):
        if _strip_ns(child.tag).lower() == local_name.lower():
            return (child.text or "").strip()
    return ""


def _rdf_child_texts(node: ET.Element, local_names: set[str]) -> list[str]:
    names = {n.lower() for n in local_names}
    values: list[str] = []
    for child in list(node):
        if _strip_ns(child.tag).lower() in names and (child.text or "").strip():
            values.append((child.text or "").strip())
    return values


def _rdf_descendant_texts(node: ET.Element, local_names: set[str]) -> list[str]:
    names = {n.lower() for n in local_names}
    values: list[str] = []
    for child in node.iter():
        if child is node:
            continue
        if _strip_ns(child.tag).lower() in names and (child.text or "").strip():
            values.append((child.text or "").strip())
    return values


def _rdf_all_texts(node: ET.Element) -> list[str]:
    values: list[str] = []
    for child in node.iter():
        text = (child.text or "").strip()
        if text:
            values.append(text)
    return values


def _doi_from_text(text: str) -> str:
    raw = (text or "").strip()
    if not raw:
        return ""
    raw_l = raw.lower()

    if _rdf_is_http_url(raw):
        if "doi.org/" in raw_l:
            doi_match = _DOI_IN_TEXT_RE.search(raw)
            if doi_match:
                return _normalize_doi_value(doi_match.group(1).rstrip(".,;"))
        if "arxiv.org" in raw_l:
            match = re.search(r"(\d{4}\.\d{4,5}(?:v\d+)?)", raw, flags=re.IGNORECASE)
            if match:
                return _normalize_doi_value(f"10.48550/arXiv.{match.group(1)}")
        if "biorxiv.org" in raw_l or "medrxiv.org" in raw_l:
            match = re.search(r"(10\.1101/[^\s/\"'<>]+)", raw, flags=re.IGNORECASE)
            if match:
                return _normalize_doi_value(match.group(1))
        return ""

    doi_match = _DOI_IN_TEXT_RE.search(raw)
    if doi_match:
        return _normalize_doi_value(doi_match.group(1).rstrip(".,;"))

    if "arxiv" in raw_l:
        match = re.search(r"(\d{4}\.\d{4,5}(?:v\d+)?)", raw, flags=re.IGNORECASE)
        if match:
            return _normalize_doi_value(f"10.48550/arXiv.{match.group(1)}")
    if "10.1101/" in raw_l:
        match = re.search(r"(10\.1101/[^\s/\"'<>]+)", raw, flags=re.IGNORECASE)
        if match:
            return _normalize_doi_value(match.group(1))
    return ""


def _rdf_is_http_url(value: str) -> bool:
    return value.lower().startswith(("http://", "https://"))


def _rdf_identifier_metadata(node: ET.Element) -> tuple[str, str]:
    """Return ``(doi, url)`` from Zotero RDF identifier-like fields."""
    doi_candidates: list[tuple[int, str]] = []
    url = ""

    def _ranked_doi(value: str) -> None:
        doi_value = _doi_from_text(value)
        if not doi_value:
            return
        value_l = value.lower()
        rank = 0
        if _rdf_is_http_url(value) and "doi.org/" not in value_l:
            rank = 1
        doi_candidates.append((rank, doi_value))

    for child in list(node):
        ctag = _strip_ns(child.tag).lower()
        if ctag in {"identifier", "doi"}:
            candidates = [child.attrib.get(_RDF_RESOURCE, "").strip()]
            candidates.extend(_rdf_all_texts(child))
            for value in candidates:
                if not value:
                    continue
                _ranked_doi(value)
                if not url and _rdf_is_http_url(value):
                    url = value
        elif ctag in {"uri", "homepage"}:
            value = (child.attrib.get(_RDF_RESOURCE) or child.text or "").strip()
            if value and not url and _rdf_is_http_url(value):
                url = value
            if value:
                _ranked_doi(value)

    about = node.attrib.get(_RDF_ABOUT, "").strip()
    if about:
        if not url and _rdf_is_http_url(about):
            url = about
        _ranked_doi(about)
    doi = sorted(doi_candidates, key=lambda item: item[0])[0][1] if doi_candidates else ""
    return doi, url


def _rdf_person_name(person: ET.Element) -> str:
    surname = ""
    given = ""
    display = ""
    for child in person.iter():
        if child is person:
            continue
        ctag = _strip_ns(child.tag).lower()
        ctext = (child.text or "").strip()
        if not ctext:
            continue
        if ctag in {"surname", "familyname", "family-name", "family", "lastname"} and not surname:
            surname = ctext
        elif ctag in {"givenname", "given-name", "given", "firstname"} and not given:
            given = ctext
        elif ctag == "name" and not display:
            display = ctext
    if surname:
        return clean_display_text(f"{surname}, {given}".strip().rstrip(","))
    return clean_display_text(display)


def _rdf_author_names(node: ET.Element) -> list[str]:
    authors: list[str] = []
    seen: set[str] = set()

    def _append(name: str) -> None:
        cleaned = clean_display_text((name or "").strip())
        if cleaned and cleaned not in seen:
            authors.append(cleaned)
            seen.add(cleaned)

    for child in list(node):
        ctag = _strip_ns(child.tag).lower()
        if ctag in {"creator", "author"} and (child.text or "").strip():
            _append(child.text or "")
        if ctag in {"authors", "creator", "contributor"}:
            for person in child.iter():
                if _strip_ns(person.tag).lower() == "person":
                    _append(_rdf_person_name(person))

    return authors


def _is_zotero_rdf_bibliographic_node(node: ET.Element) -> bool:
    tag = _strip_ns(node.tag).lower()
    item_type = _rdf_child_text(node, "itemType").strip().lower()
    if tag in _ZOTERO_RDF_SKIP_TAGS or item_type in _ZOTERO_RDF_SKIP_ITEM_TYPES:
        return False
    # Zotero API imports every non-attachment/non-note item. RDF exports put
    # some valid items in generic rdf:Description nodes, so itemType is the
    # canonical signal when present.
    if item_type:
        return True
    return tag in _ZOTERO_RDF_BIBLIO_TAGS


# Top-level Zotero RDF resources that act as an item's *container* (venue) when
# referenced via ``dcterms:isPartOf`` — a journal for articles, a book/
# proceedings for chapters. Kept broader than the item-skip set so a book that
# other items are part of can be recognised as their container (40.1).
_ZOTERO_RDF_CONTAINER_TAGS = {"journal", "book", "periodical", "proceedings"}


def _zotero_rdf_container_map(root: ET.Element) -> dict[str, dict[str, str]]:
    """Map top-level container resources referenced by dcterms:isPartOf.

    Zotero RDF stores a paper's venue as a separate top-level resource
    (``bib:Journal`` for an article, ``bib:Book``/``bib:Proceedings`` for a
    chapter) that items point at via ``dcterms:isPartOf rdf:resource="<uri>"``.
    A container may legitimately carry a DOI; whether an item may *inherit* that
    DOI is decided later by how many items share the container (40.1), not here.
    """
    containers: dict[str, dict[str, str]] = {}
    for node in root:
        tag = _strip_ns(node.tag).lower()
        if tag not in _ZOTERO_RDF_CONTAINER_TAGS:
            continue
        about = node.attrib.get(_RDF_ABOUT, "").strip()
        if not about:
            continue
        doi, url = _rdf_identifier_metadata(node)
        containers[about] = {
            "title": _rdf_child_text(node, "title"),
            "doi": doi,
            "url": url,
        }
    return containers


def _rdf_ispartof_container_refs(node: ET.Element) -> list[str]:
    """Return the container URIs a bibliographic node references via
    ``dcterms:isPartOf rdf:resource``.

    Used to count how many items share each container so a shared container's
    DOI is never inherited (40.1) — inheriting it would stamp the same DOI on
    every sibling chapter and collapse them under the ``doi:`` dedup key.
    """
    refs: list[str] = []
    for child in list(node):
        if _strip_ns(child.tag).lower() != "ispartof":
            continue
        ref = child.attrib.get(_RDF_RESOURCE, "").strip()
        if ref:
            refs.append(ref)
    return refs


def _rdf_ispartof_metadata(
    node: ET.Element,
    containers_by_uri: dict[str, dict[str, str]],
    container_ref_counts: Optional[dict[str, int]] = None,
) -> tuple[str, str, str]:
    """Return ``(journal_or_container, doi, url)`` from dcterms:isPartOf.

    A referenced container's DOI is inherited as the item's own DOI ONLY when
    that container is referenced by exactly one bibliographic item in the file
    (40.1). A container shared by multiple items (one book with several
    chapters, one journal issue with several articles) still contributes its
    title/url as venue metadata but NEVER its DOI — otherwise every sharing item
    inherits the same DOI and the ``doi:`` dedup key silently collapses them
    into one, dropping the rest. Nested (inline) containers are inherently
    unique to the item and are always inherited.
    """
    counts = container_ref_counts or {}
    journal = ""
    doi = ""
    url = ""
    for child in list(node):
        ctag = _strip_ns(child.tag).lower()
        if ctag not in {"ispartof", "publicationtitle", "journal", "container"}:
            continue
        if (child.text or "").strip() and not journal:
            journal = (child.text or "").strip()

        ref = child.attrib.get(_RDF_RESOURCE, "").strip()
        if ref and ref in containers_by_uri:
            container = containers_by_uri[ref]
            journal = journal or container.get("title", "")
            url = url or container.get("url", "")
            # Only a uniquely-referenced container may lend its DOI to the item.
            if not doi and counts.get(ref, 0) <= 1:
                doi = container.get("doi", "")
            continue

        title_candidates = _rdf_descendant_texts(child, {"title"})
        if title_candidates and not journal:
            journal = title_candidates[0]
        child_doi, child_url = _rdf_identifier_metadata(child)
        doi = doi or child_doi
        url = url or child_url
        for container_node in child.iter():
            if container_node is child:
                continue
            if _strip_ns(container_node.tag).lower() not in {
                "book",
                "journal",
                "periodical",
                "proceedings",
            }:
                continue
            if not journal:
                journal = _rdf_child_text(container_node, "title")
            container_doi, container_url = _rdf_identifier_metadata(container_node)
            doi = doi or container_doi
            url = url or container_url
    return journal.strip(), _normalize_doi_value(doi), url.strip()


def _parse_zotero_rdf_collections(root: ET.Element) -> tuple[Dict[str, str], Dict[str, list[str]]]:
    """Extract collection hierarchy + item membership from a Zotero RDF tree.

    Zotero's RDF export represents collections as ``<z:Collection>`` elements
    whose ``rdf:about`` is the collection URI. Each collection has a
    ``<dc:title>`` and zero or more ``<dcterms:hasPart rdf:resource="..."/>``
    children. ``hasPart`` references can be either items (papers) or
    sub-collections — sub-collections are detected by matching the resource
    URI against another collection's ``rdf:about``.

    Returns:
        ``(path_by_key, item_membership)`` where:
        - ``path_by_key`` maps each collection URI to its full
          ``"Parent / Child"`` path string.
        - ``item_membership`` maps each item URI to a list of collection
          paths it belongs to.
    """
    raw_collections: list[dict] = []
    for node in root:
        if _strip_ns(node.tag).lower() != "collection":
            continue
        key = node.attrib.get(_RDF_ABOUT, "").strip()
        if not key:
            continue
        name = ""
        members: list[str] = []
        for child in list(node):
            ctag = _strip_ns(child.tag).lower()
            if ctag == "title" and child.text:
                name = child.text.strip()
            elif ctag == "haspart":
                ref = child.attrib.get(_RDF_RESOURCE, "").strip()
                if ref:
                    members.append(ref)
        if not name:
            continue
        raw_collections.append({"key": key, "name": name, "members": members})

    if not raw_collections:
        return {}, {}

    # Sub-collection detection: a member URI that is itself a collection key
    # marks a parent/child collection edge; everything else is an item ref.
    collection_keys = {c["key"] for c in raw_collections}
    parent_by_key: Dict[str, Optional[str]] = {c["key"]: None for c in raw_collections}
    item_membership: Dict[str, list[str]] = {}
    for c in raw_collections:
        for ref in c["members"]:
            if ref in collection_keys:
                parent_by_key[ref] = c["key"]
            else:
                item_membership.setdefault(ref, []).append(c["key"])

    path_by_key = _build_zotero_collection_paths([
        {"key": c["key"], "name": c["name"], "parent_key": parent_by_key.get(c["key"])}
        for c in raw_collections
    ])

    # Convert membership values from collection keys to full path strings,
    # dropping blanks/duplicates so callers can treat the list as canonical.
    paths_by_item: Dict[str, list[str]] = {}
    for item_uri, keys in item_membership.items():
        seen: set[str] = set()
        ordered: list[str] = []
        for k in keys:
            p = path_by_key.get(k, "").strip()
            if p and p not in seen:
                seen.add(p)
                ordered.append(p)
        if ordered:
            paths_by_item[item_uri] = ordered

    return path_by_key, paths_by_item


def _parse_zotero_rdf(xml_content: str) -> tuple[list[dict], int]:
    """Parse a Zotero RDF export into normalized import records.

    The parser is intentionally tolerant across RDF variants and relies on
    common Dublin Core / Biblio fields. ``<z:Collection>`` nodes are parsed
    separately to attach each item's collection memberships (as resolved
    "Parent / Child" path strings) to the returned records.

    Returns ``(items, parse_duplicates)`` where ``parse_duplicates`` counts the
    canonical-identity collapses dropped during dedup — surfaced so silent data
    loss can never hide behind a smaller count (40.1; "No silent failures").
    """
    root = ET.fromstring(xml_content)
    _, paths_by_item = _parse_zotero_rdf_collections(root)
    containers_by_uri = _zotero_rdf_container_map(root)

    # Count how many bibliographic items reference each container. A container
    # referenced by an item is a venue/parent (not an import candidate itself),
    # and a container shared by more than one item must not lend its DOI (40.1).
    container_ref_counts: dict[str, int] = {}
    for node in root:
        if not _is_zotero_rdf_bibliographic_node(node):
            continue
        for ref in _rdf_ispartof_container_refs(node):
            if ref in containers_by_uri:
                container_ref_counts[ref] = container_ref_counts.get(ref, 0) + 1
    referenced_container_uris = {u for u, c in container_ref_counts.items() if c > 0}

    items: list[dict] = []

    for node in root:
        # Zotero RDF mixes bibliographic items with attachment nodes, notes,
        # collection taxonomy, and journal/container resources. Only the
        # top-level bibliographic items should become import candidates.
        if not _is_zotero_rdf_bibliographic_node(node):
            continue

        # A book/proceedings other items are part of is their container, not its
        # own paper — surfaced as venue metadata on the children, skipped here.
        about_uri = node.attrib.get(_RDF_ABOUT, "").strip()
        if about_uri and about_uri in referenced_container_uris:
            continue

        title = ""
        year: Optional[int] = None
        journal = ""
        abstract = ""
        keywords: list[str] = []
        item_type = _strip_ns(node.tag)
        zotero_item_type = _rdf_child_text(node, "itemType").strip()
        doi, url = _rdf_identifier_metadata(node)
        authors = _rdf_author_names(node)
        partof_journal, partof_doi, partof_url = _rdf_ispartof_metadata(
            node,
            containers_by_uri,
            container_ref_counts,
        )
        journal = partof_journal
        doi = doi or partof_doi
        url = url or partof_url

        for child in list(node):
            ctag = _strip_ns(child.tag).lower()
            ctext = (child.text or "").strip()

            if ctag == "title" and ctext:
                title = ctext
            elif ctag in {"date", "issued", "created"} and ctext and year is None:
                match = re.search(r"(\d{4})", ctext)
                if match:
                    year = int(match.group(1))
            elif ctag in {"ispartof", "publicationtitle", "journal", "container"} and ctext and not journal:
                journal = ctext
            elif ctag in {"identifier", "doi"} and ctext:
                if not doi:
                    doi = _doi_from_text(ctext)
                if ctext.lower().startswith("http") and not url:
                    url = ctext
            elif ctag in {"uri", "homepage", "link"} and not url:
                candidate_url = (child.attrib.get(_RDF_RESOURCE) or ctext or "").strip()
                if _rdf_is_http_url(candidate_url):
                    url = candidate_url
            elif ctag in {"description", "abstract", "abstractnote"} and ctext and not abstract:
                abstract = ctext
            elif ctag in {"subject", "keyword"} and ctext:
                keywords.append(ctext)

        if not title:
            continue

        if not doi and url:
            doi = _doi_from_text(url)

        # Resolve this item's collection memberships from the URI map built
        # at the top of the function. Items without an rdf:about (rare) get
        # no memberships.
        item_uri = node.attrib.get(_RDF_ABOUT, "").strip()
        zotero_collections = list(paths_by_item.get(item_uri, [])) if item_uri else []

        items.append({
            "title": title.strip(),
            "authors": ", ".join(a for a in authors if a).strip(),
            "year": year,
            "journal": journal.strip(),
            "doi": _normalize_doi_value(doi),
            "url": url.strip(),
            "abstract": abstract.strip(),
            "keywords": keywords,
            "item_type": zotero_item_type or item_type,
            "zotero_collections": zotero_collections,
        })

    # Deduplicate by canonical identity (RDF can contain resource aliases).
    # DOI-backed records with the same title but different DOIs are distinct
    # works/releases and must not be collapsed. Each real collapse is counted so
    # the caller can surface it — silent parse-time loss is the defect (40.1).
    deduped: dict[str, dict] = {}
    parse_duplicates = 0
    for item in items:
        doi_key = _normalize_doi_value(item.get("doi"))
        title_key = normalize_text(item.get("title", ""))
        year_key = item.get("year") if item.get("year") is not None else ""
        key = f"doi:{doi_key.lower()}" if doi_key else f"title:{year_key}:{title_key}"
        if not key:
            continue
        if key not in deduped:
            deduped[key] = item
            continue
        parse_duplicates += 1
        existing_paths = deduped[key].get("zotero_collections") or []
        seen = set(existing_paths)
        for path in item.get("zotero_collections") or []:
            if path and path not in seen:
                existing_paths.append(path)
                seen.add(path)
        deduped[key]["zotero_collections"] = existing_paths
    return list(deduped.values()), parse_duplicates


def import_zotero_rdf(
    rdf_content: str,
    conn: sqlite3.Connection,
    collection_name: Optional[str] = None,
) -> ImportResult:
    """Import papers from a Zotero RDF export file content."""
    result = ImportResult()
    try:
        items, parse_duplicates = _parse_zotero_rdf(rdf_content)
    except Exception as exc:
        result.errors.append(f"Zotero RDF parse error: {exc}")
        return result

    result.total = len(items)
    result.parse_duplicates = parse_duplicates
    if not items:
        return result

    # Target collection is created INSIDE the write gate below: `write_section`
    # rolls back any pending txn on entry, so a pre-gate INSERT would be discarded.
    local_collection_id: Optional[str] = None

    postprocess_refs: list[str] = []

    # Items are already normalized dicts; pre-compute fuzzy-title matches BEFORE
    # the write gate (40.4). No DOI/OpenAlex-id items get a match; the rest None.
    fuzzy_matches = _precompute_fuzzy_matches(conn, items)

    # RDF was parsed locally above (no network); gate the insert loop instead
    # of a raw end-of-loop commit.
    with write_section(conn, label="import zotero rdf"):
        # Create/find the target collection here (inside the gate) so its row
        # commits atomically with the imported papers.
        if collection_name:
            local_collection_id = _find_or_create_collection(conn, collection_name)
        for item, fuzzy in zip(items, fuzzy_matches):
            try:
                title = item["title"]
                notes = f"Imported from Zotero RDF ({item.get('item_type', 'item')})"
                paper_id, outcome = _import_or_stage_paper(
                    conn,
                    title=title,
                    authors=item.get("authors", ""),
                    doi=item.get("doi", ""),
                    notes=notes,
                    year=item.get("year"),
                    journal=item.get("journal"),
                    abstract=item.get("abstract"),
                    url=item.get("url"),
                    fuzzy_match=fuzzy,
                )
                _record_import_outcome(result, postprocess_refs, paper_id, outcome, item)

                # Staged rows included (deferred membership, hidden until
                # confirmed — see BibTeX loop for the rationale).
                if local_collection_id:
                    _add_to_collection(
                        conn,
                        local_collection_id,
                        paper_id,
                    )

                # Mirror each Zotero collection the item belongs to as a local
                # collection (purple chip; same color as the Web API path). Names
                # carry the full "Parent / Child" path so nesting is preserved
                # and same-named siblings under different parents stay distinct.
                # The single paper_id is reused, so a paper in N Zotero
                # collections produces 1 papers row + N collection_items rows
                # (never N papers rows).
                for coll_path in item.get("zotero_collections") or []:
                    if not coll_path:
                        continue
                    mirror_coll_id = _find_or_create_collection(
                        conn, coll_path, color="#8B5CF6"
                    )
                    _add_to_collection(
                        conn,
                        mirror_coll_id,
                        paper_id,
                    )

                if outcome != "staged":
                    for tag_name in item.get("keywords", []):
                        _find_or_create_tag_and_assign(
                            conn,
                            tag_name,
                            paper_id,
                        )
            except Exception as exc:
                result.failed += 1
                result.errors.append(f"Failed to import RDF item '{item.get('title', '?')}': {exc}")

    _resolve_imported_authors_inline(conn)
    _trigger_background_enrichment(result, postprocess_refs)
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

    Thin importer-facing wrapper over the single application-layer helper so
    there is exactly one find-or-create-by-name implementation. Stamps the
    "Imported collection:" description on newly created rows.
    """
    from alma.application import library as library_app

    return library_app.find_or_create_collection(
        conn, name, color=color, description=f"Imported collection: {name}"
    )


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
        # Year-less exact-title fallback. It must obey the SAME conflicting-
        # strong-identifier guard as resolve_existing_paper_id's title/year
        # branch (40.3): an import with DOI X must never merge into a same-title
        # row carrying a different DOI Y. Subordinate (dedup-twin) rows are
        # excluded from matching but followed up to their canonical paper.
        rows = conn.execute(
            """
            SELECT id, doi, openalex_id, COALESCE(canonical_paper_id, '') AS canonical_paper_id
            FROM papers
            WHERE LOWER(title) = LOWER(?)
            """,
            (title,),
        ).fetchall()
        for row in rows:
            if strong_identifiers_conflict(
                incoming_doi=doi,
                incoming_openalex_id=openalex_id,
                candidate_doi=row["doi"],
                candidate_openalex_id=row["openalex_id"],
            ):
                continue
            canonical = str(row["canonical_paper_id"] or "").strip()
            return canonical or (row["id"] if isinstance(row, sqlite3.Row) else row[0])

    return None


@dataclass
class _FuzzyTitleMatch:
    """Closest existing standalone paper to a title-only import candidate."""

    confidence: float = 0.0
    paper_id: Optional[str] = None


def _best_fuzzy_title_match(
    conn: sqlite3.Connection,
    title: str,
    year: Optional[int] = None,
) -> _FuzzyTitleMatch:
    """Return the closest existing REAL paper by fuzzy title, with its id (40.4).

    The candidate pool is gated to savable, first-class rows only:
      - standalone (``standalone_paper_sql`` — no canonical/component twin), so a
        merged-away duplicate can't be the match;
      - ``status`` in ``library``/``tracked`` — never a removed/dismissed row;
      - NOT an unconfirmed staged import — junk from a prior bad import can't
        push a new junk title over the threshold.
    A stable ``ORDER BY`` under the ``LIMIT`` makes it deterministic past 5k
    papers. This is read-only + CPU, so it is meant to run in a PRE-PASS before
    the import write gate, never inside it (write rule 2: gather then write).
    """
    title_norm = normalize_text(title)
    if not title_norm:
        return _FuzzyTitleMatch()

    params: list[Any] = []
    year_clause = ""
    if year is not None:
        year_clause = "AND (p.year IS NULL OR ABS(p.year - ?) <= 1)"
        params.append(year)

    rows = conn.execute(
        f"""
        SELECT p.id, p.title
        FROM papers p
        WHERE COALESCE(TRIM(p.title), '') <> ''
          AND {standalone_paper_sql("p")}
          AND LOWER(COALESCE(p.status, '')) IN ('library', 'tracked')
          AND NOT {unconfirmed_staged_import_sql("p")}
          {year_clause}
        ORDER BY COALESCE(p.added_at, '') DESC, p.id
        LIMIT 5000
        """,
        tuple(params),
    ).fetchall()
    best = _FuzzyTitleMatch()
    for row in rows:
        candidate = row["title"] if isinstance(row, sqlite3.Row) else row[1]
        candidate_norm = normalize_text(str(candidate or ""))
        if not candidate_norm:
            continue
        ratio = SequenceMatcher(None, title_norm, candidate_norm).ratio()
        if ratio > best.confidence:
            best = _FuzzyTitleMatch(
                confidence=ratio,
                paper_id=str(row["id"] if isinstance(row, sqlite3.Row) else row[0]),
            )
        if best.confidence >= 1.0:
            break
    return best


def _precompute_fuzzy_matches(
    conn: sqlite3.Connection,
    records: List[dict],
) -> List[Optional[_FuzzyTitleMatch]]:
    """Fuzzy-match every title-only record BEFORE the gated write section (40.4).

    The O(pool·items) ``SequenceMatcher`` scan is read-only + CPU; running it
    while the import holds the writer gate stalls every other writer. Returns a
    list aligned to ``records``; identified records (DOI or OpenAlex id) map to
    ``None`` because their staging decision never needs a fuzzy score.
    """
    out: List[Optional[_FuzzyTitleMatch]] = []
    for rec in records:
        doi = _normalize_doi_value(rec.get("doi"))
        openalex_id = str(rec.get("openalex_id") or "").strip()
        title = str(rec.get("title") or "").strip()
        if doi or openalex_id or not title:
            out.append(None)
            continue
        out.append(_best_fuzzy_title_match(conn, title, rec.get("year")))
    return out


def _should_stage_import(
    conn: sqlite3.Connection,
    *,
    title: str,
    doi: str,
    openalex_id: str = "",
    year: Optional[int] = None,
    fuzzy_match: Optional[_FuzzyTitleMatch] = None,
) -> tuple[bool, _FuzzyTitleMatch]:
    """Decide whether a title-only import must be STAGED for review.

    D4 default: identified imports (a DOI or OpenAlex id) always save directly
    to Library. For a title-only import (no strong identifier) the fuzzy title
    match decides (40.5, reverted):
      - a CONFIDENT match (>= ``LOW_CONFIDENCE_IMPORT_TITLE_THRESHOLD`` to an
        existing paper) is resolved as a DUPLICATE by the caller — linked under
        that paper (hidden as a separate card) and reversible via the paper
        popup's "Not a duplicate" action — so it is NOT staged;
      - a weak / no match is STAGED for review (the reviewer decides).
    ``fuzzy_match`` may be precomputed OUTSIDE the write gate (40.4); when absent
    it is computed here (read-only).
    """
    if _normalize_doi_value(doi) or (openalex_id or "").strip():
        return False, _FuzzyTitleMatch(confidence=1.0)
    match = fuzzy_match if fuzzy_match is not None else _best_fuzzy_title_match(conn, title, year)
    confident_duplicate = bool(match.paper_id) and match.confidence >= LOW_CONFIDENCE_IMPORT_TITLE_THRESHOLD
    return (not confident_duplicate), match


def _create_staged_import_paper(
    conn: sqlite3.Connection,
    *,
    title: str,
    authors: str,
    doi: str = "",
    notes: Optional[str] = None,
    year: Optional[int] = None,
    journal: Optional[str] = None,
    abstract: Optional[str] = None,
    url: Optional[str] = None,
    title_confidence: float = 0.0,
    matched_paper_id: Optional[str] = None,
) -> str:
    paper_id = generate_paper_id()
    now = datetime.utcnow().isoformat()
    if matched_paper_id and title_confidence >= LOW_CONFIDENCE_IMPORT_TITLE_THRESHOLD:
        # 40.5: a title-only import that fuzzy-matches an existing paper is held
        # for review (never auto-saved beside its likely duplicate). Record the
        # matched row id so the ImportsTab reviewer sees the candidate.
        reason = f"near_duplicate_candidate:{matched_paper_id}"
    else:
        reason = (
            "low_confidence_import_staged:"
            f"no_doi_no_openalex_title_confidence_{title_confidence:.2f}_below_"
            f"{LOW_CONFIDENCE_IMPORT_TITLE_THRESHOLD:.2f}"
        )
    # 44.7: route through the canonical dynamic-kwargs helper instead of a
    # hand-rolled ~19-column INSERT. `added_at` stays unset (a staged import is
    # NOT yet saved to Library) — create_paper doesn't default it, so it's NULL.
    from alma.application import library as library_app

    library_app.create_paper(
        conn,
        id=paper_id,
        author_id="import",
        title=title,
        year=year,
        abstract=abstract,
        url=url,
        doi=doi,
        cited_by_count=0,
        journal=journal,
        authors=authors,
        fetched_at=now,
        status="tracked",
        notes=notes,
        rating=0,
        added_from="import",
        openalex_resolution_status="unresolved",
        openalex_resolution_reason=reason,
        openalex_resolution_updated_at=now,
    )
    # create_paper deliberately doesn't own the commit — keep this site's wrapper
    # (no-ops inside the import loop's write_section; commits when standalone).
    commit_unless_gated(conn, label="_create_staged_import_paper")
    return paper_id


def _existing_import_row(conn: sqlite3.Connection, paper_id: str) -> Optional[sqlite3.Row]:
    return conn.execute(
        """
        SELECT id, status, added_from, added_at, openalex_resolution_reason,
               doi, openalex_id, authors, year, journal, abstract, url
        FROM papers
        WHERE id = ?
        """,
        (paper_id,),
    ).fetchone()


# Resolution-reason prefixes stamped on a staged (review-before-save) import.
# `low_confidence_import_staged:` is the original narrow-threshold marker;
# `near_duplicate_candidate:` is the 40.5 near-duplicate stage.
_STAGED_IMPORT_REASON_PREFIXES = (
    "low_confidence_import_staged:",
    "near_duplicate_candidate:",
)


def unconfirmed_staged_import_sql(alias: str = "") -> str:
    """SQL predicate: an import row still STAGED for review (not yet saved).

    Mirrors ``_is_unconfirmed_staged_import`` for queue / pool queries (40.2,
    40.4): imported provenance, not in Library, and either never stamped with an
    ``added_at`` (staging inserts NULL) or still carrying a staging resolution
    reason. This is the SINGLE source of truth for "row is staged" in SQL — the
    import queue lists it and the fuzzy pool excludes it, so both agree.
    ``alias`` is the papers-table alias ('' when selecting from unaliased
    ``papers``).
    """
    p = f"{alias}." if alias else ""
    like_clauses = " OR ".join(
        f"COALESCE({p}openalex_resolution_reason, '') LIKE '{prefix}%'"
        for prefix in _STAGED_IMPORT_REASON_PREFIXES
    )
    return f"""(
        LOWER(COALESCE({p}status, '')) <> 'library'
        AND LOWER(COALESCE({p}added_from, '')) = 'import'
        AND (
            COALESCE(TRIM({p}added_at), '') = ''
            OR {like_clauses}
        )
    )"""


def _is_unconfirmed_staged_import(row: sqlite3.Row) -> bool:
    status = str(row["status"] or "").strip().lower()
    added_from = str(row["added_from"] or "").strip().lower()
    added_at = str(row["added_at"] or "").strip()
    reason = str(row["openalex_resolution_reason"] or "").strip()
    return (
        status != "library"
        and added_from == "import"
        and (not added_at or reason.startswith(_STAGED_IMPORT_REASON_PREFIXES))
    )


def _merge_missing_import_metadata(
    conn: sqlite3.Connection,
    paper_id: str,
    *,
    authors: str,
    doi: str = "",
    year: Optional[int] = None,
    journal: Optional[str] = None,
    abstract: Optional[str] = None,
    url: Optional[str] = None,
    openalex_id: str = "",
) -> None:
    """Fill blank metadata on an existing non-Library import target."""
    row = _existing_import_row(conn, paper_id)
    if not row:
        return

    updates: list[str] = []
    params: list[Any] = []

    def _fill(column: str, value: Any) -> None:
        if value is None:
            return
        if isinstance(value, str):
            value = value.strip()
            if not value:
                return
        current = row[column]
        if current is None or (isinstance(current, str) and not current.strip()):
            updates.append(f"{column} = ?")
            params.append(value)

    _fill("authors", authors)
    _fill("doi", _normalize_doi_value(doi))
    _fill("openalex_id", openalex_id)
    _fill("year", year)
    _fill("journal", journal)
    _fill("abstract", abstract)
    _fill("url", url)

    if not updates:
        return
    updates.append("updated_at = ?")
    params.append(datetime.utcnow().isoformat())
    conn.execute(
        f"UPDATE papers SET {', '.join(updates)} WHERE id = ?",
        (*params, paper_id),
    )


def _import_or_stage_paper(
    conn: sqlite3.Connection,
    *,
    title: str,
    authors: str,
    doi: str = "",
    notes: Optional[str] = None,
    rating: int = 3,
    year: Optional[int] = None,
    journal: Optional[str] = None,
    abstract: Optional[str] = None,
    url: Optional[str] = None,
    openalex_id: str = "",
    fuzzy_match: Optional[_FuzzyTitleMatch] = None,
) -> tuple[str, str]:
    """Apply D4 import semantics and return ``(paper_id, outcome)``.

    Outcomes are ``imported`` (new Library row or promoted tracked row),
    ``staged`` (low-confidence title-only row held for confirmation), and
    ``skipped`` (already saved Library row).
    """
    existing_id = _find_existing_paper(conn, doi, openalex_id, title, year)
    if existing_id:
        existing_row = _existing_import_row(conn, existing_id)
        incoming_identified = bool(_normalize_doi_value(doi) or (openalex_id or "").strip())
        if existing_row and _is_unconfirmed_staged_import(existing_row) and not incoming_identified:
            return existing_id, "skipped"
        if existing_row and str(existing_row["status"] or "").strip().lower() != "library":
            _merge_missing_import_metadata(
                conn,
                existing_id,
                authors=authors,
                doi=doi,
                year=year,
                journal=journal,
                abstract=abstract,
                url=url,
                openalex_id=openalex_id,
            )
        paper_id, promoted = _promote_existing_import_target(
            conn,
            existing_id,
            added_from="import",
        )
        return paper_id, "imported" if promoted else "skipped"

    from alma.application import library as library_app

    duplicate_id = library_app.find_library_duplicate_for_metadata(
        conn,
        title=title,
        authors=authors,
        year=year,
        doi=doi,
        openalex_id=openalex_id,
    )
    if duplicate_id:
        return duplicate_id, "skipped"

    should_stage, match = _should_stage_import(
        conn,
        title=title,
        doi=doi,
        openalex_id=openalex_id,
        year=year,
        fuzzy_match=fuzzy_match,
    )
    if should_stage:
        paper_id = _create_staged_import_paper(
            conn,
            title=title,
            authors=authors,
            doi=doi,
            notes=notes,
            year=year,
            journal=journal,
            abstract=abstract,
            url=url,
            title_confidence=match.confidence,
            matched_paper_id=match.paper_id,
        )
        return paper_id, "staged"

    # A confident (>= threshold) title-only fuzzy match resolves as a DUPLICATE
    # (40.5, reverted). Create the import row carrying its own metadata, link it
    # UNDER the matched paper via canonical_paper_id (so it's hidden as a
    # separate Library card but stays detachable via "Not a duplicate"), and
    # promote the matched paper into Library — the user imported this work
    # intending to save it. The twin is NOT postprocessed (see
    # `_record_import_outcome`): enriching a title-only duplicate could hand it a
    # DOI and split it back out by accident.
    incoming_identified = bool(_normalize_doi_value(doi) or (openalex_id or "").strip())
    if not incoming_identified and match.paper_id and match.confidence >= LOW_CONFIDENCE_IMPORT_TITLE_THRESHOLD:
        twin_id = _create_library_paper(
            conn,
            title=title,
            authors=authors,
            doi=doi,
            author_id="import",
            notes=notes,
            rating=rating,
            year=year,
            journal=journal,
            abstract=abstract,
            url=url,
        )
        library_app.mark_duplicate_paper_ignored(
            conn,
            twin_id,
            match.paper_id,
            reason=f"import_title_duplicate:{match.paper_id}",
        )
        _promote_existing_import_target(conn, match.paper_id, added_from="import")
        return twin_id, "linked_duplicate"

    paper_id = _create_library_paper(
        conn,
        title=title,
        authors=authors,
        doi=doi,
        author_id="import",
        notes=notes,
        rating=rating,
        year=year,
        journal=journal,
        abstract=abstract,
        url=url,
    )
    return paper_id, "imported"


def _record_import_outcome(
    result: ImportResult,
    postprocess_refs: list[str],
    paper_id: str,
    outcome: str,
    item: dict,
) -> None:
    if outcome == "imported":
        postprocess_refs.append(paper_id)
        result.imported += 1
    elif outcome == "linked_duplicate":
        # Resolved as a duplicate of an existing paper (40.5): counted as
        # imported, but the merged twin is deliberately NOT postprocessed —
        # enriching a title-only duplicate could hand it a DOI and split it out.
        result.imported += 1
    elif outcome == "staged":
        postprocess_refs.append(paper_id)
        result.staged += 1
    else:
        result.skipped += 1
    result.items.append({"paper_id": paper_id, "outcome": outcome, **item})


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

    # 44.7: route through the canonical dynamic-kwargs helper instead of a
    # hand-rolled ~19-column INSERT. This site sets added_at=now (it IS a saved
    # Library row) and status='library'.
    from alma.application import library as library_app

    library_app.create_paper(
        conn,
        id=paper_id,
        author_id=author_id,
        title=title,
        year=year,
        abstract=abstract,
        url=url,
        doi=doi,
        cited_by_count=0,
        journal=journal,
        authors=authors,
        fetched_at=now,
        status="library",
        notes=notes,
        rating=rating,
        added_at=now,
        added_from="import",
        openalex_resolution_status="pending_enrichment",
        openalex_resolution_reason="imported_metadata_only",
        openalex_resolution_updated_at=now,
    )
    # Caller-owns-transaction: inside the import loops' write_section this
    # no-ops (the loop owns the commit); standalone callers commit with retry.
    # create_paper deliberately doesn't own the commit, so this wrapper stays.
    commit_unless_gated(conn, label="_create_library_paper")

    # Same chain hook the Library / Feed / Discovery insert sites use:
    # write a pending `paper_enrichment_status` row and auto-schedule
    # the rehydration sweep. Without this, BibTeX-imported papers
    # would never enter the chain and stay vector-less even when their
    # DOI is fully indexable. Phase 5 / 8c of
    # `tasks/13_END_TO_END_HYDRATION_VECTOR_CHAIN.md`.
    try:
        from alma.services.corpus_rehydrate import enqueue_pending_hydration

        enqueue_pending_hydration(conn, paper_id)
    except Exception as exc:
        logger.debug(
            "Importer enqueue_pending_hydration skipped for %s: %s", paper_id, exc
        )

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
    save_id, duplicate_ignored = library_app.resolve_library_save_target(conn, paper_id)
    if duplicate_ignored:
        return save_id, False

    library_app.add_to_library(
        conn,
        save_id,
        rating=current_rating,
        notes=None,
        added_from=added_from,
        override_added_from=True,
    )
    return save_id, True


def _add_to_collection(
    conn: sqlite3.Connection,
    collection_id: str,
    paper_id: str,
) -> None:
    """Add a publication to a collection (ignore if already present).

    Routes through the single collection_items writer in the application layer.
    Unlike ``library_app.add_to_collection`` this does NOT require the paper to
    be a saved Library row — the importer adds staged (``tracked``) papers here
    for deferred membership, and ``get_collection_papers`` hides them until they
    are confirmed into the Library.
    """
    from alma.application import library as library_app

    library_app.insert_collection_item(conn, collection_id, paper_id)


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

        # resolve_imported_authors is caller-owns-transaction (network-free
        # bulk local writes); gate the whole window here.
        with write_section(conn, label="import author-linking (inline)"):
            resolve_imported_authors(conn)
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
    if result.imported + result.staged <= 0:
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
        from alma.api.routes.authors import _resolve_identifiers_bulk_optimized

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
                            lambda sid: run_write_unit(
                                conn,
                                lambda: resolve_imported_authors(conn),
                                label="import author-linking (staged)",
                            ),
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
