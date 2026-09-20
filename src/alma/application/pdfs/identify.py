"""Which paper is this PDF? Identity hints read from the file itself.

PDF-first import has only the file. This module turns what ``pypdf`` read
(:class:`~alma.application.pdfs.verify.PdfFacts`) plus the uploaded filename
into ordered :class:`IdentityHint` rows, most trustworthy first:

1. DOIs in the PDF's own metadata (Info dict / XMP) — set by the publisher;
2. DOIs printed in the first pages' text, BEFORE any "References" heading
   (a cited work's DOI must never identify the citing paper);
3. an arXiv / bioRxiv identifier in that text;
4. a DOI or arXiv id in the filename ("10.1016_j.cell.2020.01.001.pdf");
5. a plausible metadata title.

Free text is read by the canonical text reader
(:func:`alma.application.inbound_capture.identifiers_in_text`) — one reader
for "identifiers in text", whichever transport the text came from. The
caller resolves hints in order and verifies the match against the file.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from pathlib import PurePath

from alma.application.pdfs.verify import PdfFacts, plausible_title, text_before_references
from alma.core.utils import find_dois_in_text

# First-page DOIs considered: the article's own is near the top or in the
# footer; more than a few means we are into a reference-like list.
_MAX_TEXT_DOIS = 3
@dataclass(frozen=True)
class IdentityHint:
    """One way to look the paper up; exactly one identifier field is set."""

    source: str  # pdf_metadata | first_page | filename | pdf_title
    doi: str | None = None
    arxiv_id: str | None = None
    openalex_id: str | None = None
    title: str | None = None

    def describe(self) -> str:
        value = self.doi or self.arxiv_id or self.openalex_id or self.title or ""
        return f"{self.source}: {value}"


def _identifiers_in(text: str) -> tuple[str | None, str | None, str | None]:
    """(doi, arxiv_id, openalex_id) the canonical text reader finds in ``text``."""
    from alma.application.inbound_capture import identifiers_in_text

    found = identifiers_in_text(text)
    return found.doi, found.arxiv_id, found.openalex_id


def identify(facts: PdfFacts, *, filename: str = "") -> list[IdentityHint]:
    """Ordered, de-duplicated identity hints for a PDF (possibly empty)."""
    hints: list[IdentityHint] = []
    seen: set[tuple] = set()

    def add(hint: IdentityHint) -> None:
        key = (
            (hint.doi or "").lower(),
            (hint.arxiv_id or "").lower(),
            hint.openalex_id or "",
            (hint.title or "").lower(),
        )
        if key != ("", "", "", "") and key not in seen:
            seen.add(key)
            hints.append(hint)

    for doi in facts.metadata_dois:
        add(IdentityHint(source="pdf_metadata", doi=doi))

    body = text_before_references(facts.text)
    for doi in find_dois_in_text(body)[:_MAX_TEXT_DOIS]:
        add(IdentityHint(source="first_page", doi=doi))
    doi, arxiv_id, openalex_id = _identifiers_in(body)
    if arxiv_id:
        add(IdentityHint(source="first_page", doi=doi, arxiv_id=arxiv_id))
    elif doi and doi.lower().startswith("10.1101/"):
        add(IdentityHint(source="first_page", doi=doi))  # bioRxiv/medRxiv stamp
    if openalex_id:
        add(IdentityHint(source="first_page", openalex_id=openalex_id))

    stem = PurePath(filename or "").stem
    if stem:
        # Downloaded files often carry their DOI with "/" swapped for "_".
        as_doi = re.sub(r"^(10\.\d{4,9})_", r"\1/", stem)
        f_doi, f_arxiv, _ = _identifiers_in(f"{stem} {as_doi}")
        if f_doi or f_arxiv:
            add(IdentityHint(source="filename", doi=f_doi, arxiv_id=f_arxiv))

    title = plausible_title(facts.metadata_title, filename=filename)
    if title:
        add(IdentityHint(source="pdf_title", title=title))
    return hints
