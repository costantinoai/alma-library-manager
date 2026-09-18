"""Unpaywall adapter — every open-access location Unpaywall knows for a DOI.

One call, one parse, shared by every flow that needs Unpaywall: abstract
recovery reads the landing pages, the PDF pipeline reads the PDF links.

Unpaywall (rebuilt on OpenAlex's "Walden" backend since May 2025) often sets
``best_oa_location.url_for_pdf`` to null while another location in
``oa_locations`` carries a PDF, so callers get EVERY location — best first —
and decide for themselves. Transport goes through the shared ``unpaywall``
source client (polite-pool email, pacing, retries, network switch).
"""

from __future__ import annotations

import logging
from dataclasses import dataclass

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class OaLocation:
    """One open-access copy of a work, as Unpaywall reports it."""

    url: str
    url_for_pdf: str
    url_for_landing_page: str
    version: str  # publishedVersion | acceptedVersion | submittedVersion | ""
    license: str
    host_type: str  # publisher | repository | ""
    is_best: bool


def unpaywall_available() -> bool:
    """Unpaywall requires a contact email; without one ALMa never calls it."""
    from alma.config import get_contact_email

    return bool(get_contact_email())


def fetch_oa_locations(doi: str) -> list[OaLocation]:
    """All OA locations for ``doi``, the best location first, deduplicated.

    Returns ``[]`` when no contact email is configured, when Unpaywall has no
    record (404) or rejects the DOI (422). Raises ``requests.RequestException``
    (including ``HTTPError`` for a 429/5xx that survived the client's retries)
    so a caller can tell "the service is down" from "no open copy exists".
    """
    from alma.core.http_sources import get_source_http_client

    clean = (doi or "").strip()
    if not clean or not unpaywall_available():
        return []
    resp = get_source_http_client("unpaywall").get(f"/{clean}", timeout=20)
    if resp.status_code in (404, 422):
        return []
    resp.raise_for_status()
    try:
        payload = resp.json() or {}
    except ValueError:
        logger.debug("Unpaywall returned a non-JSON body for %s", clean)
        return []

    raw: list[tuple[dict, bool]] = []
    best = payload.get("best_oa_location")
    if isinstance(best, dict):
        raw.append((best, True))
    for loc in payload.get("oa_locations") or []:
        if isinstance(loc, dict):
            raw.append((loc, False))

    locations: list[OaLocation] = []
    seen: set[tuple[str, str]] = set()
    for loc, is_best in raw:
        location = OaLocation(
            url=str(loc.get("url") or "").strip(),
            url_for_pdf=str(loc.get("url_for_pdf") or "").strip(),
            url_for_landing_page=str(loc.get("url_for_landing_page") or "").strip(),
            version=str(loc.get("version") or "").strip(),
            license=str(loc.get("license") or "").strip(),
            host_type=str(loc.get("host_type") or "").strip(),
            is_best=is_best,
        )
        key = (location.url_for_pdf, location.url_for_landing_page or location.url)
        if key in seen:
            continue
        seen.add(key)
        locations.append(location)
    return locations


__all__ = ["OaLocation", "fetch_oa_locations", "unpaywall_available"]
