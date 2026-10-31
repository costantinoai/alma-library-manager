"""The one door to Google Scholar for identity work (D14).

Scraping Scholar is opt-in. Every identity path that wants the `scholarly`
client asks here, so the two questions are answered once: does a setting allow
this kind of contact, and is outbound network allowed at all.

``auto`` is work nobody asked for in the moment: maintenance, a refresh, identity
resolution. ``manual`` is a person pressing a button. Each has its own setting,
and an absent key means off whatever a caller elsewhere assumes.

The legacy `backend=scholar` publication fetcher in `core/fetcher.py` is the
other, separate opt-in (D14), and it asks `require_network_access` itself.
"""

from __future__ import annotations

import logging
from typing import Any, Literal

from alma.config import get_setting
from alma.core.network_policy import network_access_enabled

logger = logging.getLogger(__name__)

ScholarIntent = Literal["auto", "manual"]

_SETTING_BY_INTENT: dict[str, str] = {
    "auto": "id_resolution_scholar_scrape_auto_enabled",
    "manual": "id_resolution_scholar_scrape_manual_enabled",
}


def scholar_contact_allowed(intent: ScholarIntent) -> bool:
    """Whether this kind of Scholar contact may happen right now."""
    return get_setting(_SETTING_BY_INTENT[intent], False) is True and network_access_enabled()


def scholarly_client(intent: ScholarIntent) -> Any | None:
    """Return the `scholarly` client, or None when contact is not allowed.

    None is an ordinary answer, not an error: identity carries on through
    OpenAlex, ORCID and Semantic Scholar.
    """
    if not scholar_contact_allowed(intent):
        return None
    try:
        from scholarly import scholarly
    except ImportError as exc:
        logger.warning("scholarly unavailable: %s", exc)
        return None
    return scholarly
