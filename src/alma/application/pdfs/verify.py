"""Is it a PDF — and is it THIS paper? Pure CPU: no network, no database.

Two questions, answered from the bytes, never from what a server claims:

1. **What kind of body is this?** Content-Type lies both ways (a PDF from S3
   arrives as ``binary/octet-stream``; a captcha wall arrives as HTTP 200
   ``text/html``), so :func:`classify_head` sniffs the first KB and
   :func:`is_bot_wall` / :func:`looks_like_challenge` recognise bot walls.
2. **Which paper is it?** :func:`read_facts` has ``pypdf`` — in the sandboxed
   worker (``pdfs.sandbox``), never in this process — read page count,
   metadata and pages 1–2; :func:`verify_identity` looks for the paper's DOI there,
   then its title. A file whose text names a different paper is a
   ``MISMATCH``; a file with no extractable text (a scan) is ``UNVERIFIED``.
"""

from __future__ import annotations

import logging
import re
import unicodedata
from collections.abc import Iterable
from dataclasses import dataclass
from enum import StrEnum
from pathlib import Path, PurePath

from alma.application.pdf_schema import Verification
from alma.core.utils import (
    canonical_lookup_doi,
    find_dois_in_text,
    normalize_title_key,
    title_tokens,
)

logger = logging.getLogger(__name__)

#: A real article PDF is never this small; anything below is an error page,
#: a stub or a login redirect that happens to start with ``%PDF-``.
MIN_PDF_BYTES = 10_000
#: Pages read for identity (title block + first-page footer DOI).
VERIFY_PAGES = 2
_TEXT_CAP = 40_000
_TITLE_KEY_MIN = 12
_TOKEN_CONTAINMENT = 0.8

# Markup only a bot-wall interstitial carries: the challenge scripts and
# widgets of DDoS-Guard, Cloudflare and ALTCHA. A content page never loads them.
_WALL_SIGNATURES: tuple[str, ...] = (
    "/.well-known/ddos-guard/js-challenge",
    "/cdn-cgi/challenge-platform",
    "cf-chl-",
    "_cf_chl_opt",
    "<altcha-widget",
    "altcha.min.js",
)
# What a wall calls itself in its <title>. Matched in the title only: a paper
# about CAPTCHAs, or a page that merely mentions its CDN ("for DDOS-GUARD
# caching" sits in every Anna's Archive page), is not a wall.
_WALL_TITLES: tuple[str, ...] = (
    "just a moment",
    "checking your browser",
    "ddos-guard",
    "attention required",
    "are you a robot",
    "are you are robot",
    "verify you are human",
    "access denied",
    "security check",
)
# Softer words, trusted only on a SHORT page that advertises no PDF (a wall
# is a few KB; a real landing page is tens or hundreds).
_WALL_PHRASES: tuple[str, ...] = (
    "captcha",
    "are you a robot",
    "checking your browser",
    "ddos-guard",
    "access denied",
)
_SHORT_PAGE_CHARS = 20_000
# A wall names itself early; a hostile page must not make the title search
# slow, so it is bounded both in where it looks and in how far each match runs.
_WALL_SCAN_CHARS = 65_536
_TITLE_RE = re.compile(r"<title[^>]{0,200}>([^<]{0,300})", re.IGNORECASE)


class BodyKind(StrEnum):
    PDF = "pdf"
    HTML = "html"
    CHALLENGE = "challenge"
    OTHER = "other"


def classify_head(head: bytes) -> BodyKind:
    """Classify a body from its first bytes (~1 KB).

    ``%PDF-`` may follow a few junk bytes, so it is searched in the first KB
    rather than required at byte 0.
    """
    if b"%PDF-" in head[:1024]:
        return BodyKind.PDF
    text = head[:4096].decode("utf-8", errors="ignore").lower()
    if is_bot_wall(text):
        return BodyKind.CHALLENGE
    if "<html" in text or "<!doctype html" in text or "<head" in text:
        return BodyKind.HTML
    return BodyKind.OTHER


def is_bot_wall(text: str) -> bool:
    """True when ``text`` is certainly a bot-wall interstitial.

    Strict on purpose — it runs on the first KB of EVERY body, including real
    landing pages — so only a wall's own scripts/widgets or its <title> count.
    """
    lowered = (text or "")[:_WALL_SCAN_CHARS].lower()
    if any(signature in lowered for signature in _WALL_SIGNATURES):
        return True
    title = _TITLE_RE.search(lowered)
    return bool(title) and any(phrase in title.group(1) for phrase in _WALL_TITLES)


def looks_like_challenge(text: str) -> bool:
    """True for a whole HTML page that is (probably) a bot wall.

    For a page already known to advertise no PDF: :func:`is_bot_wall`, or a
    short page using a wall's words. Never used to discard a page that links
    a PDF — a wall has nothing to link.
    """
    if is_bot_wall(text):
        return True
    lowered = (text or "").lower()
    return len(lowered) < _SHORT_PAGE_CHARS and any(phrase in lowered for phrase in _WALL_PHRASES)


@dataclass(frozen=True)
class PdfFacts:
    """What ``pypdf`` could read from a file."""

    readable: bool
    pages: int | None = None
    text: str = ""  # pages 1–2, NFKC-normalised (ligatures unfolded)
    metadata_title: str = ""
    metadata_dois: tuple[str, ...] = ()
    encrypted: bool = False


def read_facts(path: Path, *, pages: int = VERIFY_PAGES) -> PdfFacts:
    """Page count, metadata and the first pages' text of ``path`` — read in the sandbox.

    ``pypdf`` never runs in this process (``pdfs.sandbox``). Never raises: a
    file that cannot be opened (or read safely) is ``readable=False``; a
    password-protected one too. An encrypted file that opens with the empty
    password (owner-only protection) is read normally.
    """
    from alma.application.pdfs.sandbox import inspect_pdf

    return facts_from(inspect_pdf(path, pages=pages))


def facts_from(inspection) -> PdfFacts:
    """The identity facts in a sandbox ``PdfInspection`` (text NFKC-normalised here)."""
    if not inspection.readable or inspection.locked:
        return PdfFacts(readable=False, encrypted=inspection.encrypted)
    return PdfFacts(
        readable=True,
        pages=inspection.pages,
        text=unicodedata.normalize("NFKC", inspection.text)[:_TEXT_CAP],
        metadata_title=unicodedata.normalize("NFKC", inspection.metadata_title).strip(),
        metadata_dois=tuple(find_dois_in_text(" ".join(inspection.metadata_strings))),
        encrypted=inspection.encrypted,
    )


def title_matches(title: str, text: str) -> bool:
    """Does ``text`` carry ``title``?

    First the normalised title key as a substring of the text's key — robust
    to case, punctuation, line breaks and hyphenation, because the key strips
    every non-alphanumeric character. Short titles are too ambiguous for that,
    so the fallback is token containment: at least 80% of the title's tokens
    appear in the text (needs 3+ tokens).
    """
    key = normalize_title_key(title)
    if len(key) >= _TITLE_KEY_MIN and key in normalize_title_key(text):
        return True
    wanted = title_tokens(title)
    if len(wanted) < 3:
        return False
    return len(wanted & title_tokens(text)) / len(wanted) >= _TOKEN_CONTAINMENT


_JUNK_TITLE_RE = re.compile(r"^(microsoft word|untitled|document\d*|\s*$)", re.IGNORECASE)
_FILELIKE_RE = re.compile(r"\.(pdf|docx?|tex|dvi|ps|odt)$", re.IGNORECASE)


def plausible_title(value: str, *, filename: str = "") -> str | None:
    """A metadata ``/Title`` worth searching for, or ``None``.

    Word/LaTeX tooling fills ``/Title`` with junk ("Microsoft Word - draft3",
    "untitled", the file name); those identify nothing.
    """
    title = " ".join((value or "").split())
    if len(title) < 12 or len(title.split()) < 3:
        return None
    if _JUNK_TITLE_RE.match(title) or _FILELIKE_RE.search(title):
        return None
    stem = PurePath(filename or "").stem.lower()
    if stem and title.lower() == stem:
        return None
    return title


_REFERENCES_RE = re.compile(
    r"(?:^|\n)\s*(references|bibliography|literature cited|works cited|reference list)\s*:?\s*(?:\n|$)",
    re.IGNORECASE,
)


def text_before_references(text: str) -> str:
    """Exclude bibliography identifiers from both discovery and verification."""
    match = _REFERENCES_RE.search(text or "")
    return (text or "")[:match.start()] if match else (text or "")


def verify_identity(facts: PdfFacts, *, dois: Iterable[str], title: str) -> Verification:
    """Verify publisher metadata first; incidental citation DOIs are not identity.

    Metadata identifiers outrank body identifiers. Without them, conflicting
    metadata titles or multiple body DOIs need title corroboration. Bibliography
    text never supplies identity evidence. Group DOIs still support preprints.
    """
    wanted = {canonical_lookup_doi(d) for d in dois if d} - {None}
    metadata = {canonical_lookup_doi(d) for d in facts.metadata_dois} - {None}
    body = text_before_references(facts.text)
    if metadata and wanted:
        if wanted & metadata:
            return Verification.DOI
        return Verification.MISMATCH
    # Tool-generated titles are not evidence of either identity or conflict.
    metadata_title = plausible_title(facts.metadata_title)
    if metadata_title and title and not title_matches(title, metadata_title):
        return Verification.MISMATCH
    present = {canonical_lookup_doi(d) for d in find_dois_in_text(body)} - {None}
    corroborated = bool(title and title_matches(title, metadata_title or body))
    if wanted & present and (len(present) == 1 or corroborated):
        return Verification.DOI
    if corroborated:
        return Verification.TITLE
    if not body.strip() and not metadata and not metadata_title:
        return Verification.UNVERIFIED
    return Verification.MISMATCH


#: Hint sources whose identifier was READ OUT OF the body text (see
#: :func:`verify_chosen_identity`). ``identify`` stamps them on the hint.
CIRCULAR_EVIDENCE = frozenset({"first_page"})


def verify_chosen_identity(facts: PdfFacts, *, dois: Iterable[str], title: str, evidence: str) -> Verification:
    """Verification for a paper the file itself CHOSE (import), not one it was fetched for.

    Fetching and attaching know the paper already, so a DOI in the body is
    independent confirmation. Importing does not: the paper was looked up
    *from* that same body DOI, so finding it there again proves nothing — a
    manuscript citing someone else's DOI in its introduction would "confirm"
    as that paper and take over its stored file.

    So when the hint came out of the body (``evidence`` in
    :data:`CIRCULAR_EVIDENCE`), a DOI needs support that did NOT pick the
    paper: publisher metadata, or the title reading as this paper's.
    """
    verification = verify_identity(facts, dois=dois, title=title)
    if verification is not Verification.DOI or evidence not in CIRCULAR_EVIDENCE:
        return verification
    wanted = {canonical_lookup_doi(d) for d in dois if d} - {None}
    if wanted & ({canonical_lookup_doi(d) for d in facts.metadata_dois} - {None}):
        return verification  # the publisher's own stamp, not the body
    metadata_title = plausible_title(facts.metadata_title)
    if title and title_matches(title, metadata_title or text_before_references(facts.text)):
        return verification  # the page reads as this paper, DOI and all
    return Verification.MISMATCH
