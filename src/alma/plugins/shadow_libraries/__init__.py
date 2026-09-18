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


class ShadowLibrariesConfig(StrictPluginConfig):
    scihub_mirrors: str = Field(
        "",
        title="Sci-Hub mirrors",
        description=(
            "Mirror addresses, comma-separated, tried in order (for example "
            "https://sci-hub.ru). Mirrors come and go; check which are up first."
        ),
        json_schema_extra={"x-alma-order": 10},
    )
    annas_archive_domains: str = Field(
        "",
        title="Anna's Archive addresses",
        description="Comma-separated, tried in order.",
        json_schema_extra={"x-alma-order": 20},
    )
    member_key: str = Field(
        "",
        title="Anna's Archive member key",
        description="Optional: a member key gives direct downloads. Stored in the secret store.",
        json_schema_extra={"x-alma-secret": True, "x-alma-order": 30, "x-alma-advanced": True},
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


async def test_connection() -> dict:
    """Probe every configured address: up, behind a bot check, or unreachable."""
    from alma.application.pdfs.verify import looks_like_challenge
    from alma.plugins.shadow_libraries.sources import transport

    mirrors, domains, _key = _settings()
    client = transport()
    results = []
    for address in [*mirrors, *domains]:
        try:
            resp = client.get(f"{address}/", timeout=15, max_retries=0)
            if resp.status_code < 400 and not looks_like_challenge(resp.text or ""):
                state = "up"
            elif looks_like_challenge(resp.text or ""):
                state = "behind a bot check"
            else:
                state = f"HTTP {resp.status_code}"
        except Exception as exc:  # noqa: BLE001 — a probe reports, never raises
            state = f"unreachable ({exc.__class__.__name__})"
        results.append({"address": address, "state": state})
    up = [r["address"] for r in results if r["state"] == "up"]
    summary = "; ".join(f"{r['address']}: {r['state']}" for r in results) or "No addresses configured"
    return {"ok": bool(up), "results": results, "message": summary}


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
