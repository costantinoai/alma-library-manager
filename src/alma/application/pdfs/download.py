"""Download one PDF candidate — safely, and judged by its bytes.

A candidate URL comes from third-party metadata or HTML, so this module:

* refuses any URL whose host is not a public internet address — loopback,
  private, link-local and the carrier-grade range Tailscale uses
  (100.64.0.0/10) are the user's own machines — and checks EVERY redirect
  hop, following redirects itself rather than letting the transport do it;
* downloads through the shared source client (pacing, retries, network
  switch), streamed under the byte cap into the store's staging area;
* decides what came back from the bytes (:func:`verify.classify_head`): a PDF
  is staged; a bot wall is ``blocked``; an HTML page gets ONE hop to the PDF
  it advertises (``citation_pdf_url`` & co., via ``core.html_meta``). A page
  that advertises none is a bot wall (``blocked``), a redirect to the site's
  home page (the source does not have the paper: ``no_candidate``), or just
  ``not_pdf``;
* reports a connection failure in words (``describe_transport_error``), not
  as a raw curl / urllib3 message.

Network only — never the database. Returns a :class:`DownloadResult`; the
caller verifies the staged file against the paper and stores it.
"""

from __future__ import annotations

import dataclasses
import ipaddress
import itertools
import logging
import socket
from dataclasses import dataclass
from urllib.parse import urljoin, urlsplit

from alma.application.pdf_schema import Outcome, PdfCandidate
from alma.application.pdfs import store
from alma.application.pdfs.verify import (
    MIN_PDF_BYTES,
    BodyKind,
    classify_head,
    looks_like_challenge,
)
from alma.core.html_meta import parse_html, pdf_urls
from alma.core.http_sources import describe_transport_error
from alma.core.redaction import redact_sensitive_text

logger = logging.getLogger(__name__)

MAX_REDIRECTS = 5
DOWNLOAD_TIMEOUT = 60.0
HTML_MAX_BYTES = 2_000_000
#: PDF links followed from one landing page (most authoritative first).
HTML_HOP_LINKS = 3
_CHUNK = 64 * 1024
_REDIRECT_STATUSES = frozenset({301, 302, 303, 307, 308})
_ACCEPT = "application/pdf,text/html;q=0.9,*/*;q=0.5"


class UnsafeUrlError(ValueError):
    """A URL that must not be fetched (not http(s), or not a public address)."""


@dataclass(frozen=True)
class DownloadResult:
    outcome: Outcome
    staged: store.StagedFile | None = None
    http_status: int | None = None
    detail: str = ""
    final_url: str = ""

    @property
    def safe_url(self) -> str:
        """``final_url`` with credential-like query values redacted (for storage/logs)."""
        return redact_sensitive_text(self.final_url)


def check_public_url(url: str) -> None:
    """Raise :class:`UnsafeUrlError` unless ``url`` is http(s) to a public host.

    A host that does not resolve is let through — the request will fail on its
    own, and a name that cannot be resolved cannot reach a private address.
    """
    parts = urlsplit(url or "")
    if parts.scheme not in ("http", "https") or not parts.hostname:
        raise UnsafeUrlError(f"Not a web address: {url!r}")
    host = parts.hostname
    try:
        addresses = {ipaddress.ip_address(host)}
    except ValueError:
        try:
            infos = socket.getaddrinfo(host, parts.port or None, proto=socket.IPPROTO_TCP)
        except (socket.gaierror, UnicodeError):
            return
        addresses = {ipaddress.ip_address(info[4][0].split("%", 1)[0]) for info in infos}
    for address in addresses:
        if not address.is_global:
            raise UnsafeUrlError(f"Refused {host}: not a public internet address")


def download_candidate(
    candidate: PdfCandidate,
    *,
    max_bytes: int = store.MAX_PDF_BYTES,
    html_hops: int = 1,
) -> DownloadResult:
    """Fetch ``candidate`` and stage it if it is a PDF (see module docstring)."""
    if candidate.opener is not None:
        try:
            response = candidate.opener()
        except Exception as exc:  # noqa: BLE001 — a transport failure is an outcome
            return DownloadResult(Outcome.ERROR, detail=describe_transport_error(exc), final_url=candidate.url)
        final_url = candidate.url
    else:
        opened = _open_following_redirects(candidate)
        if isinstance(opened, DownloadResult):
            return opened
        response, final_url = opened
    try:
        return _consume(response, candidate, final_url, max_bytes=max_bytes, html_hops=html_hops)
    finally:
        response.close()


def _open_following_redirects(candidate: PdfCandidate):
    """GET with every hop checked; returns ``(response, final_url)`` or a result."""
    from alma.core.http_sources import get_source_http_client

    client = candidate.transport or get_source_http_client(candidate.client)
    url = candidate.url
    headers = {"Accept": _ACCEPT}
    if candidate.referer:
        headers["Referer"] = candidate.referer
    for _hop in range(MAX_REDIRECTS + 1):
        try:
            check_public_url(url)
        except UnsafeUrlError as exc:
            return DownloadResult(Outcome.BLOCKED, detail=str(exc), final_url=url)
        try:
            response = client.get(
                url,
                headers=headers,
                stream=True,
                allow_redirects=False,
                timeout=DOWNLOAD_TIMEOUT,
                max_retries=1,
            )
        except Exception as exc:  # noqa: BLE001 — transport failure is an outcome
            return DownloadResult(Outcome.ERROR, detail=describe_transport_error(exc), final_url=url)
        location = response.headers.get("Location") if response.status_code in _REDIRECT_STATUSES else None
        if not location:
            return response, url
        response.close()
        url = urljoin(url, location)
    return DownloadResult(Outcome.ERROR, detail="Too many redirects", final_url=url)


def _consume(response, candidate: PdfCandidate, final_url: str, *, max_bytes: int, html_hops: int) -> DownloadResult:
    status = int(response.status_code)
    chunks = response.iter_content(_CHUNK)
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
        return _follow_landing_page(response, head, chunks, candidate, final_url, max_bytes=max_bytes)

    return DownloadResult(Outcome.NOT_PDF, http_status=status, detail="Not a PDF", final_url=final_url)


def _follow_landing_page(response, head: bytes, chunks, candidate: PdfCandidate, page_url: str, *, max_bytes: int):
    """Read an HTML page (capped) and try the PDF links it advertises — once.

    Links come first: a page that links a PDF is not a wall, whatever words it
    uses. Only a page with no PDF link is classified — a bot wall, a bounce to
    the site's home page (this source does not have the paper), or a page
    that simply has no PDF.
    """
    body = bytearray(head)
    for chunk in chunks:
        body += chunk
        if len(body) >= HTML_MAX_BYTES:
            break
    text = bytes(body[:HTML_MAX_BYTES]).decode(response.encoding or "utf-8", errors="replace")
    snapshot = parse_html(text)
    links = [link for link in pdf_urls(snapshot, page_url) if link != page_url][:HTML_HOP_LINKS]
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
        last = download_candidate(hop, max_bytes=max_bytes, html_hops=0)
        if last.outcome is Outcome.FOUND:
            return last
    return last


def _bounced_home(requested_url: str, final_url: str) -> bool:
    """True when a request for a specific page was redirected to the site root.

    Mirrors and publishers answer "no such paper" by sending you home.
    """
    return urlsplit(final_url).path in ("", "/") and urlsplit(requested_url).path not in ("", "/")
