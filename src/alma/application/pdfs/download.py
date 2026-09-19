"""Download one PDF candidate — safely, and judged by its bytes.

A candidate URL comes from third-party metadata, a publisher page or a
mirror that may be hostile, so nothing it sends is trusted:

* the request goes through a GUARDED source client (``guard_addresses``):
  the URL and every redirect hop pass ``core.url_safety.vet_url`` (public
  addresses only — never loopback, private, link-local or the tailnet's
  100.64.0.0/10), the connection is pinned to the checked addresses, and the
  transport never follows a redirect on its own. A client that does not vet
  addresses is refused outright;
* the body streams under the byte cap AND an overall deadline into the
  store's staging area; a connection that breaks mid-body is an outcome, not
  a crash;
* what came back is decided from the bytes (:func:`verify.classify_head`): a
  PDF is staged; an HTML page (decoded only in a safe charset) gets ONE hop
  to the PDF it advertises (``core.html_meta``). A page that advertises none
  is a bot wall (``blocked``), a bounce to the site's home page or the
  source's declared "not here" page (``no_candidate``), or ``not_pdf``;
* every word stored about the attempt is ALMa's own or passed through
  ``clean_remote_text``, and the stored URL is bounded.

Network only — never the database, never a PDF parser (the caller hands the
staged file to the sandboxed inspector). Returns a :class:`DownloadResult`.
"""

from __future__ import annotations

import dataclasses
import itertools
import logging
import time
from collections.abc import Iterable, Iterator
from dataclasses import dataclass
from urllib.parse import urlsplit

from alma.application.pdf_schema import Outcome, PdfCandidate
from alma.application.pdfs import store
from alma.application.pdfs.verify import (
    MIN_PDF_BYTES,
    BodyKind,
    classify_head,
    looks_like_challenge,
)
from alma.core.html_meta import parse_html, pdf_urls
from alma.core.http_sources import describe_transport_error, iter_body
from alma.core.redaction import redact_sensitive_text
from alma.core.url_safety import MAX_URL_CHARS, UnsafeUrlError, clean_remote_text, safe_charset

logger = logging.getLogger(__name__)

DOWNLOAD_TIMEOUT = 60.0  # per network read (connect / between bytes)
#: Wall-clock budget for one candidate INCLUDING its landing-page hop: a
#: server that trickles bytes cannot hold a fetch job for longer.
DOWNLOAD_DEADLINE = 180.0
HTML_MAX_BYTES = 2_000_000
#: PDF links followed from one landing page (most authoritative first).
HTML_HOP_LINKS = 3
_ACCEPT = "application/pdf,text/html;q=0.9,*/*;q=0.5"


class DownloadTimeoutError(TimeoutError):
    """The candidate's :data:`DOWNLOAD_DEADLINE` passed mid-body."""


@dataclass(frozen=True)
class DownloadResult:
    outcome: Outcome
    staged: store.StagedFile | None = None
    http_status: int | None = None
    detail: str = ""
    final_url: str = ""

    def __post_init__(self) -> None:
        # The last gate before these words reach the attempts ledger and the UI.
        object.__setattr__(self, "detail", clean_remote_text(self.detail))

    @property
    def safe_url(self) -> str:
        """``final_url`` fit to store: http(s), bounded, credential-like values redacted."""
        url = self.final_url or ""
        if len(url) > MAX_URL_CHARS or urlsplit(url).scheme not in ("http", "https"):
            return ""
        return redact_sensitive_text(url)


def download_candidate(
    candidate: PdfCandidate,
    *,
    max_bytes: int = store.MAX_PDF_BYTES,
    html_hops: int = 1,
    deadline: float | None = None,
) -> DownloadResult:
    """Fetch ``candidate`` and stage it if it is a PDF (see module docstring)."""
    deadline = deadline if deadline is not None else time.monotonic() + DOWNLOAD_DEADLINE
    if candidate.opener is not None:
        # A trusted provider API that is not a plain GET (OpenAlex content);
        # it owns its own transport. Its bytes are still judged below.
        try:
            response = candidate.opener()
        except Exception as exc:  # noqa: BLE001 — a transport failure is an outcome
            return DownloadResult(Outcome.ERROR, detail=describe_transport_error(exc), final_url=candidate.url)
        final_url = candidate.url
    else:
        opened = _open(candidate)
        if isinstance(opened, DownloadResult):
            return opened
        response, final_url = opened
    try:
        return _consume(response, candidate, final_url, max_bytes=max_bytes, html_hops=html_hops, deadline=deadline)
    except DownloadTimeoutError:
        return DownloadResult(Outcome.ERROR, detail="The download took too long", final_url=final_url)
    except Exception as exc:  # noqa: BLE001 — a body that breaks mid-way is an outcome
        return DownloadResult(Outcome.ERROR, detail=describe_transport_error(exc), final_url=final_url)
    finally:
        response.close()


def _open(candidate: PdfCandidate):
    """GET through a guarded client; returns ``(response, final_url)`` or a result."""
    from alma.core.http_sources import get_source_http_client

    client = candidate.transport or get_source_http_client(candidate.client)
    if not client.guards_addresses:
        # Misuse made impossible: an unguarded client would follow whatever a
        # third party redirects to. Every PDF source client must vet addresses.
        return DownloadResult(
            Outcome.ERROR, detail="This source's client does not check addresses", final_url=candidate.url
        )
    headers = {"Accept": _ACCEPT}
    if candidate.referer:
        headers["Referer"] = candidate.referer
    try:
        response = client.get(
            candidate.url,
            headers=headers,
            stream=True,
            allow_redirects=True,
            timeout=DOWNLOAD_TIMEOUT,
            max_retries=1,
        )
    except UnsafeUrlError as exc:
        return DownloadResult(Outcome.BLOCKED, detail=str(exc), final_url=candidate.url)
    except Exception as exc:  # noqa: BLE001 — transport failure is an outcome
        return DownloadResult(Outcome.ERROR, detail=describe_transport_error(exc), final_url=candidate.url)
    return response, str(response.url or candidate.url)


def _before(deadline: float, chunks: Iterable[bytes]) -> Iterator[bytes]:
    """``chunks``, stopping with :class:`DownloadTimeoutError` once ``deadline`` passes."""
    for chunk in chunks:
        if time.monotonic() > deadline:
            raise DownloadTimeoutError("download deadline passed")
        yield chunk


def _consume(
    response, candidate: PdfCandidate, final_url: str, *, max_bytes: int, html_hops: int, deadline: float
) -> DownloadResult:
    status = int(response.status_code)
    chunks = _before(deadline, iter_body(response))
    head = b""
    for chunk in chunks:
        head += chunk
        if len(head) >= 1024:
            break
    kind = classify_head(head)

    if status >= 400:
        if kind is BodyKind.CHALLENGE:
            return DownloadResult(Outcome.BLOCKED, http_status=status, detail="Bot check page", final_url=final_url)
        return DownloadResult(Outcome.HTTP_ERROR, http_status=status, detail=f"HTTP {status}", final_url=final_url)

    # A 200 page that looks like a wall from its first KB still goes through
    # the landing step when it may hop: a mirror page for a paper titled
    # "Are you a robot?" is a paper page, and only its lack of a PDF link
    # makes it a wall.
    if kind is BodyKind.CHALLENGE and html_hops > 0:
        kind = BodyKind.HTML

    if kind is BodyKind.PDF:
        try:
            staged = store.stage_chunks(itertools.chain([head], chunks), max_bytes=max_bytes)
        except store.PdfTooLargeError as exc:
            return DownloadResult(Outcome.TOO_LARGE, http_status=status, detail=str(exc), final_url=final_url)
        if staged.bytes < MIN_PDF_BYTES:
            staged.discard()
            return DownloadResult(Outcome.NOT_PDF, http_status=status, detail="PDF stub too small", final_url=final_url)
        return DownloadResult(Outcome.FOUND, staged=staged, http_status=status, final_url=final_url)

    if kind is BodyKind.CHALLENGE:
        return DownloadResult(Outcome.BLOCKED, http_status=status, detail="Bot check page", final_url=final_url)

    if kind is BodyKind.HTML and html_hops > 0:
        return _follow_landing_page(
            response, head, chunks, candidate, final_url, max_bytes=max_bytes, deadline=deadline
        )

    return DownloadResult(Outcome.NOT_PDF, http_status=status, detail="Not a PDF", final_url=final_url)


def _follow_landing_page(
    response, head: bytes, chunks, candidate: PdfCandidate, page_url: str, *, max_bytes: int, deadline: float
) -> DownloadResult:
    """Read an HTML page (capped) and try the PDF links it advertises — once.

    Links come first: a page that links a PDF is not a wall, whatever words it
    uses. Only a page with no PDF link is classified — a bot wall, a bounce to
    the site's home page or the source's declared "not here" page (this
    source does not have the paper), or a page that simply has no PDF. The
    page is decoded only in a charset :func:`safe_charset` allows.
    """
    body = bytearray(head)
    for chunk in chunks:
        body += chunk
        if len(body) >= HTML_MAX_BYTES:
            break
    text = bytes(body[:HTML_MAX_BYTES]).decode(safe_charset(response.encoding), errors="replace")
    snapshot = parse_html(text)
    links = [
        link
        for link in pdf_urls(snapshot, page_url, include_anchors=candidate.follow_anchors)
        if link != page_url
    ][:HTML_HOP_LINKS]
    if not links:
        status = response.status_code
        if looks_like_challenge(text):
            return DownloadResult(Outcome.BLOCKED, http_status=status, detail="Bot check page", final_url=page_url)
        if _bounced_home(candidate.url, page_url):
            return DownloadResult(
                Outcome.NO_CANDIDATE, http_status=status,
                detail="Sent to the site's home page — it does not have this paper", final_url=page_url,
            )
        if any(phrase in snapshot.title.lower() for phrase in candidate.absent_titles):
            return DownloadResult(
                Outcome.NO_CANDIDATE, http_status=status, detail="It says it does not have this paper",
                final_url=page_url,
            )
        return DownloadResult(Outcome.NOT_PDF, http_status=status, detail="Page advertises no PDF", final_url=page_url)
    last: DownloadResult | None = None
    for link in links:
        hop = dataclasses.replace(candidate, url=link, referer=page_url, opener=None)
        last = download_candidate(hop, max_bytes=max_bytes, html_hops=0, deadline=deadline)
        if last.outcome is Outcome.FOUND:
            return last
    return last


def _bounced_home(requested_url: str, final_url: str) -> bool:
    """True when a request for a specific page was redirected to the site root.

    Mirrors and publishers answer "no such paper" by sending you home.
    """
    try:
        return urlsplit(final_url).path in ("", "/") and urlsplit(requested_url).path not in ("", "/")
    except ValueError:
        return False
