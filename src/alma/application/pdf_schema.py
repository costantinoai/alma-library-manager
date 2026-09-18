"""Paper PDFs — THE contract between the core feature and its sources.

ALMa keeps at most one PDF per paper on the server and serves it to any device.
A PDF arrives one of three ways (``Origin``): **fetched** on the user's tap
through a chain of sources, **uploaded** onto a paper, or **imported**
PDF-first (the paper is identified from the file). The core owns the pipeline,
the file store, verification and serving; it knows no provider.

Sources are contributed by plugins that declare the ``pdf_source`` capability
(``alma.plugins``). A source does network work only — it never touches the
database — and returns ``PdfCandidate`` rows the core then downloads,
validates and verifies. Open sources (``tier="open"``) always run before
shadow-library sources (``tier="shadow"``).

Everything here is plain data + one Protocol: no transport, no DB.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from enum import StrEnum
from typing import TYPE_CHECKING, Literal, Protocol, runtime_checkable

if TYPE_CHECKING:
    import requests

    from alma.core.http_sources import SourceHttpClient

SourceTier = Literal["open", "shadow"]

#: Run order across tiers: every open source is tried before any shadow one.
TIER_ORDER: dict[str, int] = {"open": 0, "shadow": 1}


class Origin(StrEnum):
    """How the stored PDF reached ALMa."""

    FETCHED = "fetched"
    UPLOADED = "uploaded"
    IMPORTED = "imported"


class Verification(StrEnum):
    """What the file's own text proves about which paper it is.

    ``MISMATCH`` is only ever stored for a file the USER attached to a paper
    by hand — their association is authoritative, so the file is kept and
    flagged. A fetched file that mismatches is rejected instead.
    """

    DOI = "doi"
    TITLE = "title"
    UNVERIFIED = "unverified"  # no extractable text (scan) or unreadable
    MISMATCH = "mismatch"


class Outcome(StrEnum):
    """Result of one source's attempt for one paper (the attempts ledger)."""

    FOUND = "found"
    NO_CANDIDATE = "no_candidate"  # the source had nothing for this paper
    NOT_PDF = "not_pdf"  # got a body, it was not a PDF (and no PDF link in it)
    BLOCKED = "blocked"  # captcha / bot wall / challenge page
    HTTP_ERROR = "http_error"
    TOO_LARGE = "too_large"
    MISMATCH = "mismatch"  # a PDF, but of a different paper
    REJECTED = "rejected"  # the user marked this exact file wrong before
    SKIPPED = "skipped"  # not configured (no key / email / mirror)
    ERROR = "error"  # transport failure or unexpected error


@dataclass(frozen=True)
class PaperRef:
    """Everything a source may use to find a paper's PDF.

    Built from the ROOT paper of a paper group (a preprint child resolves to
    its published root), with the group's other identifiers alongside so a
    source can fall back to the preprint (``group_dois`` / ``group_arxiv_ids``).
    ``doi`` is the canonical lookup form (lower-case, bare).
    """

    paper_id: str
    title: str
    year: int | None = None
    authors: str = ""
    doi: str = ""
    openalex_id: str = ""
    semantic_scholar_id: str = ""
    arxiv_id: str = ""
    url: str = ""
    oa_url: str = ""
    group_dois: tuple[str, ...] = ()
    group_arxiv_ids: tuple[str, ...] = ()


@dataclass(frozen=True, eq=False)
class PdfCandidate:
    """One URL a source believes serves this paper's PDF.

    The core downloads it: through the built-in source client named by
    ``client`` (e.g. ``"arxiv"`` so arXiv's 1-request-per-3-s budget is
    shared), or through a plugin-owned ``transport``. Either way the core
    validates every redirect hop and may follow ONE HTML page to the PDF it
    advertises. ``opener`` is the escape hatch for a trusted transport that is
    not a plain URL GET (OpenAlex's paid content host): it returns a STREAMED
    response and owns its own redirects.
    """

    url: str
    source_id: str
    version: str = ""  # publishedVersion | acceptedVersion | submittedVersion | ""
    license: str = ""
    referer: str = ""
    client: str = "publisher"
    transport: SourceHttpClient | None = None
    opener: Callable[[], requests.Response] | None = None


@runtime_checkable
class PdfSource(Protocol):
    """A provider of PDF candidates. Network only — never the database."""

    id: str
    label: str
    tier: SourceTier

    def candidates(self, ref: PaperRef) -> list[PdfCandidate]:
        """Candidate URLs for ``ref``, best first; ``[]`` when none.

        Raise ``SourceSkippedError`` when the source cannot run at all (missing key,
        email or mirror) so the attempt is recorded as skipped, not as a miss.
        Transport errors propagate and are recorded as ``error``.
        """
        ...


class SourceSkippedError(Exception):
    """A source is not configured to run (no key / email / mirror)."""


__all__ = [
    "TIER_ORDER",
    "Origin",
    "Outcome",
    "PaperRef",
    "PdfCandidate",
    "PdfSource",
    "SourceSkippedError",
    "SourceTier",
    "Verification",
]
