"""Shadow libraries — Sci-Hub and Anna's Archive as a separate, opt-in PDF source.

A ``pdf_source`` plugin kept apart from the open-access one on purpose: it is
off by default, has no addresses until the user adds them, runs only after
every open-access source has failed, and only on an explicit tap. Mirror
domains change often, so they are plain user settings; the Anna's Archive
member key (optional, for direct downloads) lives in the secret store.
"""

from __future__ import annotations

import sqlite3

from pydantic import Field

from alma.core.secrets import SECRET_ANNAS_ARCHIVE_KEY
from alma.plugins.configs import StrictPluginConfig, settings_backed_config
from alma.plugins.manifest import PDF_SOURCE, PluginManifest

PLUGIN_ID = "shadow_libraries"


# Where a reader finds working addresses. Third-party status pages and
# encyclopaedia entries outlive any one mirror domain, so the app links those;
# the guide (a docs-relative path) keeps the current list and the how-to.
_GUIDE = "/user-guide/reading-pdfs/#shadow-libraries"
_SCIHUB_LINKS = [
    {"label": "Which addresses are up now (SLUM)", "url": "https://open-slum.org/scihub.html"},
    {"label": "Official addresses (Wikipedia)", "url": "https://en.wikipedia.org/wiki/Sci-Hub"},
    {"label": "How to add them", "url": _GUIDE},
]
_ANNAS_LINKS = [
    {"label": "Which addresses are up now (SLUM)", "url": "https://open-slum.org/annas.html"},
    {"label": "Official addresses (Wikipedia)", "url": "https://en.wikipedia.org/wiki/Anna%27s_Archive"},
    {"label": "How to add them", "url": _GUIDE},
]
_MEMBER_KEY_LINKS = [
    {"label": "Where to find your key", "url": "/user-guide/reading-pdfs/#annas-archive-member-key"},
]


class ShadowLibrariesConfig(StrictPluginConfig):
    scihub_mirrors: str = Field(
        "",
        title="Sci-Hub mirrors",
        description=(
            "Addresses tried in order, comma-separated — for example https://sci-hub.ru, "
            "https://sci-net.xyz. Sci-Net is Sci-Hub's own site for papers it lacks, "
            "newer ones especially. Mirrors come and go; press Test after saving."
        ),
        json_schema_extra={"x-alma-order": 10, "x-alma-links": _SCIHUB_LINKS},
    )
    annas_archive_domains: str = Field(
        "",
        title="Anna's Archive addresses",
        description=(
            "Addresses tried in order, comma-separated — for example https://annas-archive.gl. "
            "Its paper pages sit behind a browser check that only a member key gets past."
        ),
        json_schema_extra={"x-alma-order": 20, "x-alma-links": _ANNAS_LINKS},
    )
    member_key: str = Field(
        "",
        title="Anna's Archive member key",
        description=(
            "Optional: the secret key of a paid Anna's Archive membership. ALMa logs in "
            "with it to open paper pages and download directly. Kept in the secret store."
        ),
        json_schema_extra={
            "x-alma-secret": True,
            "x-alma-order": 30,
            "x-alma-advanced": True,
            "x-alma-links": _MEMBER_KEY_LINKS,
        },
    )


read_config, write_config = settings_backed_config(
    PLUGIN_ID, ShadowLibrariesConfig, secrets={"member_key": SECRET_ANNAS_ARCHIVE_KEY}
)


def _settings() -> tuple[list[str], list[str], str]:
    from alma.config import get_setting
    from alma.core.secrets import get_secret
    from alma.plugins.shadow_libraries.sources import parse_addresses

    mirrors = parse_addresses(str(get_setting("plugins.shadow_libraries.scihub_mirrors") or ""))
    domains = parse_addresses(str(get_setting("plugins.shadow_libraries.annas_archive_domains") or ""))
    return mirrors, domains, str(get_secret(SECRET_ANNAS_ARCHIVE_KEY) or "")


def _sources():
    from alma.plugins.shadow_libraries.sources import AnnasArchiveSource, SciHubSource

    mirrors, domains, key = _settings()
    return [AnnasArchiveSource(domains, key), SciHubSource(mirrors)]


def status(_db: sqlite3.Connection | None) -> dict:
    from alma.plugins.shadow_libraries.sources import curl_available

    mirrors, domains, key = _settings()
    curl = curl_available()
    configured = bool(mirrors or domains)
    notes = []
    if not configured:
        notes.append("Add at least one mirror address.")
    if domains and not key:
        notes.append("Anna's Archive paper pages need a member key to get past their browser check.")
    if not curl:
        notes.append("curl_cffi is not installed; these mirrors refuse plain requests.")
    return {
        "configured": configured,
        "can_send": False,
        "can_receive": False,
        "can_fetch": configured and curl,
        "scihub_mirrors": len(mirrors),
        "annas_archive_domains": len(domains),
        "member_key": bool(key),
        "curl_cffi": curl,
        "notes": notes,
    }


#: The test fetches one well-known paywalled paper through every address,
#: exactly as a real fetch would: LeCun, Bengio & Hinton, "Deep learning",
#: Nature 2015 — in Sci-Hub, Sci-Net and SciDB alike.
PROBE_DOI = "10.1038/nature14539"
PROBE_TITLE = "Deep learning"
_PROBE_STATES = {
    "found": "serves PDFs",
    "blocked": "behind a bot check",
    "no_candidate": "up, but does not have the test paper",
    "not_pdf": "up, but gave no PDF for the test paper",
}


def _probe_row(source: str, address: str, result) -> dict:
    """One test line: the address, what it did, and how bad that is (lib/severity)."""
    from alma.application.pdf_schema import Outcome

    if result.outcome is Outcome.HTTP_ERROR:
        state, severity = f"refused (HTTP {result.http_status})", "critical"
    elif result.outcome is Outcome.ERROR:
        state, severity = f"unreachable — {result.detail}", "critical"
    else:
        state = _PROBE_STATES.get(str(result.outcome), result.detail or str(result.outcome))
        severity = "ok" if result.outcome is Outcome.FOUND else "warning"
    return {"source": source, "address": address, "state": state, "severity": severity}


def _address(candidate) -> str:
    from urllib.parse import urlsplit

    parts = urlsplit(candidate.referer or candidate.url)
    return f"{parts.scheme}://{parts.netloc}"


async def test_connection() -> dict:
    """Fetch a known paper through every configured address, as a real fetch would.

    Each address is asked for :data:`PROBE_DOI` through the plugin's own
    sources and the core's download step (the same bot-wall, redirect and
    PDF checks), so "serves PDFs" means a fetch through it can succeed —
    not merely that its home page loads. The downloaded file is discarded.
    """
    from alma.application.pdf_schema import PaperRef, SourceBlockedError, SourceSkippedError
    from alma.application.pdfs.download import download_candidate
    from alma.core.http_sources import describe_transport_error

    ref = PaperRef(paper_id="connection-test", title=PROBE_TITLE, doi=PROBE_DOI)
    results: list[dict] = []
    for source in _sources():
        try:
            candidates = source.candidates(ref)
        except SourceSkippedError:
            continue  # nothing configured for this source
        except SourceBlockedError as exc:
            results.append({"source": source.label, "address": source.label,
                            "state": f"behind a bot check — {exc}", "severity": "warning"})
            continue
        except Exception as exc:  # noqa: BLE001 — a probe reports, never raises
            results.append({"source": source.label, "address": source.label,
                            "state": f"unreachable — {describe_transport_error(exc)}", "severity": "critical"})
            continue
        for candidate in candidates:
            result = download_candidate(candidate)
            if result.staged is not None:
                result.staged.discard()
            results.append(_probe_row(source.label, _address(candidate), result))
    working = [r["address"] for r in results if r["severity"] == "ok"]
    if not results:
        message = "No addresses configured"
    elif working:
        message = f"{len(working)} of {len(results)} addresses serve PDFs: {', '.join(working)}"
    else:
        message = f"None of {len(results)} addresses served the test paper — see each one below"
    return {"ok": bool(working), "working": working, "results": results, "message": message}


SHADOW_LIBRARIES_PLUGIN = PluginManifest(
    id=PLUGIN_ID,
    display_name="Shadow libraries",
    version="1.0.0",
    description=(
        "Try the Sci-Hub and Anna's Archive mirrors you add — only after every "
        "open-access source has failed, and only when you ask for a PDF. Whether "
        "this is legal depends on where you live."
    ),
    kind="integration",
    capabilities=(PDF_SOURCE,),
    config_model=ShadowLibrariesConfig,
    read_config=read_config,
    write_config=write_config,
    status_factory=status,
    docs_path="/user-guide/reading-pdfs/",
    connection_tester=test_connection,
    pdf_source_factory=_sources,
    action_ids=("test",),
)

__all__ = ["SHADOW_LIBRARIES_PLUGIN", "ShadowLibrariesConfig"]
