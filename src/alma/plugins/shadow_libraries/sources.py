"""Shadow-library PDF sources — tried only after every open-access source.

Both sources are ``tier="shadow"``, so the core runs them after the whole open
tier, and only when the user asked for a PDF. Their mirrors sit behind
DDoS-Guard and refuse plain ``requests``, so they share ONE plugin-owned
transport: a ``curl_cffi`` session that presents a browser's TLS fingerprint,
wrapped in a ``SourcePolicy`` so it still gets ALMa's pacing, retries,
diagnostics and network switch (``core.http_sources.client_for_policy``).

Neither source parses a mirror's HTML itself: a mirror page is a candidate,
and the core's download step checks the address, follows the page's PDF
element (``#pdf`` iframe / embed / object) and classifies bot walls — the
same code every other landing page goes through.
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

POLICY_NAME = "shadow_libraries"
_MD5_RE = re.compile(r"/md5/([0-9a-f]{32})")
# Sci-Hub's "not in my database" page (it links Sci-Net instead of a PDF),
# as titled on sci-hub.su on 2026-09-19.
_SCIHUB_ABSENT_TITLES = ("not available through sci-hub",)


def curl_available() -> bool:
    from alma.ai.import_state import module_available

    return module_available("curl_cffi")


@functools.cache
def _policy():
    from curl_cffi import requests as curl_requests
    from curl_cffi.requests.exceptions import RequestException

    from alma.core.http_sources import SourcePolicy

    return SourcePolicy(
        name=POLICY_NAME,
        base_url="",
        min_interval_seconds=2.0,  # one mirror request every 2 s, whichever mirror
        max_concurrency=1,
        max_retries=1,
        default_timeout=30.0,
        session_factory=lambda: curl_requests.Session(impersonate="chrome"),
        transport_errors=(RequestException,),
        send_app_user_agent=False,  # keep the impersonated browser's own UA
    )


def transport():
    """The one shared client for every shadow-library request."""
    from alma.core.http_sources import client_for_policy

    if not curl_available():
        raise SourceSkippedError("curl_cffi is not installed — these mirrors need it")
    return client_for_policy(_policy())


def parse_addresses(raw: str) -> list[str]:
    """User-entered mirror list → ordered, de-duplicated ``https://host`` bases."""
    out: list[str] = []
    for token in re.split(r"[\s,;]+", raw or ""):
        value = token.strip().rstrip("/")
        if not value:
            continue
        if "://" not in value:
            value = f"https://{value}"
        if value.startswith(("http://", "https://")) and value not in out:
            out.append(value)
    return out


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
        client = transport()
        # One candidate per mirror, in the user's order: the core tries the
        # next mirror when one is down, walled, or lacks the paper.
        return [
            PdfCandidate(url=f"{mirror}/{ref.doi}", source_id=self.id, referer=f"{mirror}/", transport=client,
                         absent_titles=_SCIHUB_ABSENT_TITLES)
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
        client = transport()
        if not self.member_key:
            return [
                PdfCandidate(url=f"{domain}/scidb/{ref.doi}", source_id=self.id, referer=f"{domain}/",
                             transport=client)
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
        """Log in, read the SciDB page's md5, ask the member API for a download link."""
        from alma.application.pdfs.download import check_public_url
        from alma.application.pdfs.verify import is_bot_wall

        page_url = f"{domain}/scidb/{doi}"
        check_public_url(page_url)
        # The login form posts the secret key as ``key``; the reply sets the
        # member cookie on this thread's session.
        client.post(f"{domain}/account/", data={"key": self.member_key}, timeout=30)
        page = client.get(page_url, headers={"Accept": "text/html"}, timeout=30)
        if is_bot_wall(page.text or ""):
            raise SourceBlockedError(
                f"{domain} still asked for a browser check after logging in — check the member key"
            )
        if page.status_code != 200:
            return []
        page_candidate = PdfCandidate(url=page_url, source_id=self.id, referer=f"{domain}/", transport=client)
        match = _MD5_RE.search(page.text or "")
        if not match:
            return [page_candidate]
        api = client.get(
            f"{domain}/dyn/api/fast_download.json",
            params={"md5": match.group(1), "key": self.member_key},
            timeout=30,
        )
        payload = api.json() or {}
        url = str(payload.get("download_url") or "")
        if not url:
            # "Invalid secret key", "Not a member", "No downloads left" … — say which.
            raise RuntimeError(f"member download refused: {payload.get('error') or f'HTTP {api.status_code}'}")
        return [
            PdfCandidate(url=url, source_id=self.id, referer=f"{domain}/", transport=client),
            page_candidate,
        ]
