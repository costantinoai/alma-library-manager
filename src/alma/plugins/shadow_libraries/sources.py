"""Shadow-library PDF sources — tried only after every open-access source.

Both sources are ``tier="shadow"``, so the core runs them after the whole open
tier, and only when the user asked for a PDF. Every mirror — official or an
unofficial copy — is treated as hostile: its pages are read, never run, and
nothing it says is used before the core has checked it.

* Transport: mirrors sit behind DDoS-Guard and refuse plain ``requests``, so
  each source has a ``curl_cffi`` session presenting a browser's TLS
  fingerprint, wrapped in a GUARDED ``SourcePolicy`` (``guard_addresses``):
  every request and redirect hop is vetted and pinned to public addresses
  (``core.url_safety``), bodies are capped, and ALMa's pacing, retries,
  diagnostics and network switch apply. The session speaks http(s) only,
  ignores proxy variables and alt-svc, and the two sources never share one —
  the Anna's Archive member cookie never reaches a Sci-Hub mirror.
* Pages: a mirror page is a candidate; the core's download step follows its
  PDF element (``#pdf`` iframe / embed / object — never its advertising
  anchors), classifies walls and "not here" pages, and hands the file to the
  sandboxed inspector, which removes active content before it is kept.
"""

from __future__ import annotations

import functools
import re

from alma.application.pdf_schema import (
    PaperRef,
    PdfCandidate,
    SourceBlockedError,
    SourceSkippedError,
)

POLICY_NAMES = {"scihub": "shadow_libraries.scihub", "annas": "shadow_libraries.annas"}
#: Largest body read from a mirror outside a PDF download (a page, API JSON).
PAGE_MAX_BYTES = 2_000_000
_MD5_RE = re.compile(r"/md5/([0-9a-f]{32})")
# Sci-Hub's "not in my database" page (it links Sci-Net instead of a PDF),
# as titled on sci-hub.su on 2026-09-19.
_SCIHUB_ABSENT_TITLES = ("not available through sci-hub",)


def curl_available() -> bool:
    from alma.ai.import_state import module_available

    return module_available("curl_cffi")


def _mirror_session():
    """A browser-fingerprinted curl session that can only speak http(s), directly."""
    from curl_cffi import CurlOpt
    from curl_cffi import requests as curl_requests

    return curl_requests.Session(
        impersonate="chrome",
        trust_env=False,  # never an environment proxy: the address checks must hold
        # (No ALTSVC_CTRL: the impersonating libcurl rejects it. An alt-svc
        # move to another host would still be refused — the client checks
        # that every connection's peer is an address it vetted.)
        curl_options={
            CurlOpt.PROTOCOLS_STR: "http,https",
            CurlOpt.REDIR_PROTOCOLS_STR: "http,https",
            CurlOpt.NOPROXY: "*",
        },
    )


@functools.cache
def _policy(kind: str):
    from curl_cffi.requests.exceptions import RequestException

    from alma.core.http_sources import SourcePolicy

    return SourcePolicy(
        name=POLICY_NAMES[kind],
        base_url="",
        min_interval_seconds=2.0,  # one request every 2 s per source, whichever mirror
        max_concurrency=1,
        max_retries=1,
        default_timeout=30.0,
        session_factory=_mirror_session,
        transport_errors=(RequestException,),
        send_app_user_agent=False,  # keep the impersonated browser's own UA
        guard_addresses=True,
    )


def transport(kind: str = "scihub"):
    """The guarded client of one source (``scihub`` or ``annas``)."""
    from alma.core.http_sources import client_for_policy

    if not curl_available():
        raise SourceSkippedError("curl_cffi is not installed — these mirrors need it")
    return client_for_policy(_policy(kind))


def parse_addresses(raw: str) -> list[str]:
    """User-entered mirror list → ordered, de-duplicated ``scheme://host`` bases.

    Anything ``url_safety.vet_url`` refuses on syntax alone (credentials,
    odd ports, disguised hosts, control characters) is dropped; addresses are
    resolved and checked again on every request.
    """
    from urllib.parse import urlsplit

    from alma.core.url_safety import UnsafeUrlError, vet_url

    out: list[str] = []
    for token in re.split(r"[\s,;]+", raw or ""):
        value = token.strip().rstrip("/")
        if not value:
            continue
        if "://" not in value:
            value = f"https://{value}"
        try:
            vetted = vet_url(value, resolve=False)
        except UnsafeUrlError:
            continue
        parts = urlsplit(vetted.url)
        base = f"{parts.scheme}://{parts.netloc}"
        if base not in out:
            out.append(base)
    return out


def _doi_path(doi: str) -> str:
    """A DOI as a URL path: unusual characters are percent-encoded, ``/`` and ``()`` kept."""
    from urllib.parse import quote

    return quote(doi, safe="/()")


class SciHubSource:
    id = "scihub"
    label = "Sci-Hub"
    tier = "shadow"

    def __init__(self, mirrors: list[str]) -> None:
        self.mirrors = mirrors

    def candidates(self, ref: PaperRef) -> list[PdfCandidate]:
        if not ref.doi:
            return []
        if not self.mirrors:
            raise SourceSkippedError("no mirror address yet — add one in Settings → Plugins")
        client = transport("scihub")
        # One candidate per mirror, in the user's order: the core tries the
        # next mirror when one is down, walled, or lacks the paper. A mirror
        # page's plain anchors are advertising — only its viewer is followed.
        return [
            PdfCandidate(url=f"{mirror}/{_doi_path(ref.doi)}", source_id=self.id, referer=f"{mirror}/",
                         transport=client, absent_titles=_SCIHUB_ABSENT_TITLES, follow_anchors=False)
            for mirror in self.mirrors
        ]


class AnnasArchiveSource:
    """Anna's Archive SciDB: ``{domain}/scidb/{doi}`` shows the paper's PDF.

    SciDB sits behind a browser check that a script cannot pass unless it is
    logged in as a member. With a member key the source logs in first (the
    client's session is per thread, so the core's download reuses the
    cookie), reads the paper's md5 from the SciDB page and asks the member API
    for a direct link; the SciDB page itself follows as a fallback. Without a
    key the SciDB page is the candidate, and the core reports the wall.
    """

    id = "annas_archive"
    label = "Anna's Archive"
    tier = "shadow"

    def __init__(self, domains: list[str], member_key: str) -> None:
        self.domains = domains
        self.member_key = member_key

    def candidates(self, ref: PaperRef) -> list[PdfCandidate]:
        if not ref.doi:
            return []
        if not self.domains:
            raise SourceSkippedError("no address yet — add one in Settings → Plugins")
        client = transport("annas")
        if not self.member_key:
            return [
                PdfCandidate(url=f"{domain}/scidb/{_doi_path(ref.doi)}", source_id=self.id, referer=f"{domain}/",
                             transport=client, follow_anchors=False)
                for domain in self.domains
            ]
        out: list[PdfCandidate] = []
        failures: list[Exception] = []
        for domain in self.domains:
            try:
                out.extend(self._member_candidates(client, domain, ref.doi))
            except Exception as exc:  # noqa: BLE001 — one dead domain must not hide the next
                failures.append(exc)
        if not out and failures:
            raise failures[-1]
        return out

    def _member_candidates(self, client, domain: str, doi: str) -> list[PdfCandidate]:
        """Log in, read the SciDB page's md5, ask the member API for a download link.

        Every call goes through the guarded client: vetted and pinned
        addresses, redirects checked hop by hop (a redirected login never
        re-sends the key), bodies capped at :data:`PAGE_MAX_BYTES`.
        """
        from alma.application.pdfs.verify import is_bot_wall
        from alma.core.url_safety import clean_remote_text, vet_url

        page_url = f"{domain}/scidb/{_doi_path(doi)}"
        # The login form posts the secret key as ``key``; the reply sets the
        # member cookie on this thread's session.
        client.post(f"{domain}/account/", data={"key": self.member_key}, timeout=30)
        page = client.get(page_url, headers={"Accept": "text/html"}, timeout=30, max_body_bytes=PAGE_MAX_BYTES)
        if is_bot_wall(page.text or ""):
            raise SourceBlockedError(
                f"{domain} still asked for a browser check after logging in — check the member key"
            )
        if page.status_code != 200:
            return []
        page_candidate = PdfCandidate(url=page_url, source_id=self.id, referer=f"{domain}/", transport=client,
                                      follow_anchors=False)
        match = _MD5_RE.search(page.text or "")
        if not match:
            return [page_candidate]
        api = client.get(
            f"{domain}/dyn/api/fast_download.json",
            params={"md5": match.group(1), "key": self.member_key},
            timeout=30,
            max_body_bytes=PAGE_MAX_BYTES,
        )
        try:
            payload = api.json()
        except ValueError:
            payload = None
        payload = payload if isinstance(payload, dict) else {}
        url = vet_url(str(payload.get("download_url") or ""), resolve=False).url if payload.get("download_url") else ""
        if not url:
            # "Invalid secret key", "Not a member", "No downloads left" … — say which, in safe words.
            reason = clean_remote_text(payload.get("error"), 80) or f"HTTP {api.status_code}"
            raise RuntimeError(f"member download refused: {reason}")
        return [
            PdfCandidate(url=url, source_id=self.id, referer=f"{domain}/", transport=client),
            page_candidate,
        ]
