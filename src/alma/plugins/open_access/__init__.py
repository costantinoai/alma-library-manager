"""Open-access PDFs — the legal PDF sources for the core paper-PDF feature.

A ``pdf_source`` plugin: it contributes the open-access sources in
:mod:`.sources` (arXiv, PubMed Central, OpenAlex, Unpaywall, Crossref, the
publisher's page, optionally OpenAlex's paid full-text copies, Semantic
Scholar). The core runs them on the user's tap, in that order, and keeps the
first file that verifies as the paper. Off until the user switches it on.
"""

from __future__ import annotations

import sqlite3

from pydantic import Field

from alma.plugins.configs import StrictPluginConfig, settings_backed_config
from alma.plugins.manifest import PDF_SOURCE, PluginManifest

PLUGIN_ID = "open_access"


class OpenAccessConfig(StrictPluginConfig):
    use_openalex_content: bool = Field(
        False,
        title="Use OpenAlex's cached PDFs",
        description=(
            "Last resort when a publisher blocks downloads: OpenAlex keeps copies "
            "of many open-access papers. Each PDF costs $0.01 of your OpenAlex "
            "budget and needs an OpenAlex API key."
        ),
        json_schema_extra={"x-alma-order": 10, "x-alma-advanced": True},
    )


read_config, write_config = settings_backed_config(PLUGIN_ID, OpenAccessConfig)


def _sources():
    from alma.plugins.open_access.sources import build_sources

    config = read_config(None)
    return build_sources(use_openalex_content=bool(config.use_openalex_content))


def status(_db: sqlite3.Connection | None) -> dict:
    """Which sources can run right now (none of them strictly needs setup)."""
    from alma.config import get_contact_email, get_openalex_api_key

    config = read_config(None)
    has_email = bool(get_contact_email())
    return {
        "configured": True,
        "can_send": False,
        "can_receive": False,
        "can_fetch": True,
        "sources": [source.label for source in _sources()],
        "unpaywall_ready": has_email,
        "openalex_content_ready": bool(config.use_openalex_content and get_openalex_api_key()),
        "notes": [] if has_email else ["Add a contact email (Settings → Connections) to use Unpaywall."],
    }


OPEN_ACCESS_PLUGIN = PluginManifest(
    id=PLUGIN_ID,
    display_name="Open-access PDFs",
    version="1.0.0",
    description=(
        "Find a paper's PDF on tap from open-access sources — arXiv, PubMed Central, "
        "OpenAlex, Unpaywall, Crossref and the publisher's own page."
    ),
    kind="integration",
    capabilities=(PDF_SOURCE,),
    config_model=OpenAccessConfig,
    read_config=read_config,
    write_config=write_config,
    status_factory=status,
    docs_path="/plugins/open-access-pdfs/",
    pdf_source_factory=_sources,
)

__all__ = ["OPEN_ACCESS_PLUGIN", "OpenAccessConfig"]
