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

from alma.application.pdf_schema import PaperRef, PdfCandidate, SourceSkippedError

POLICY_NAME = "shadow_libraries"
_MD5_RE = re.compile(r"/md5/([0-9a-f]{32})")


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
            PdfCandidate(url=f"{mirror}/{ref.doi}", source_id=self.id, referer=f"{mirror}/", transport=client)
            for mirror in self.mirrors
        ]


class AnnasArchiveSource:
    id = "annas_archive"
    label = "Anna's Archive"
    tier = "shadow"

    def __init__(self, domains: list[str], member_key: str) -> None:
        self.domains = domains
        self.member_key = member_key

    def candidates(self, ref: PaperRef) -> list[PdfCandidate]:
        from alma.application.pdfs.download import check_public_url

        if not ref.doi:
            return []
        if not self.domains:
            raise SourceSkippedError("no address yet — add one in Settings → Plugins")
        client = transport()
        out: list[PdfCandidate] = []
        failures: list[Exception] = []
        for domain in self.domains:
            page_url = f"{domain}/scidb/{ref.doi}"
            if not self.member_key:
                # The SciDB page links or embeds the file; the core follows it.
                out.append(PdfCandidate(url=page_url, source_id=self.id, referer=f"{domain}/", transport=client))
                continue
            try:
                check_public_url(page_url)
                page = client.get(page_url, headers={"Accept": "text/html"}, timeout=30)
                match = _MD5_RE.search(page.text or "") if page.status_code == 200 else None
                if not match:
                    continue
                api = client.get(
                    f"{domain}/dyn/api/fast_download.json",
                    params={"md5": match.group(1), "key": self.member_key},
                    timeout=30,
                )
                url = str((api.json() or {}).get("download_url") or "") if api.status_code == 200 else ""
                if url:
                    out.append(PdfCandidate(url=url, source_id=self.id, referer=f"{domain}/", transport=client))
            except Exception as exc:  # noqa: BLE001 — one dead domain must not hide the next
                failures.append(exc)
        if not out and failures:
            raise failures[-1]
        return out
