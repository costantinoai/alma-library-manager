"""ORCID public API adapter for author metadata hydration."""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any

from alma.core.http_sources import get_source_http_client
from alma.core.utils import normalize_orcid

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class ORCIDAffiliation:
    institution_name: str
    role: str
    start_date: str | None = None
    end_date: str | None = None
    is_current: bool = False
    institution_ror: str | None = None
    evidence_url: str | None = None


@dataclass(frozen=True)
class ORCIDRecord:
    orcid: str
    given_names: str | None = None
    family_name: str | None = None
    credit_name: str | None = None
    other_names: tuple[str, ...] = ()
    country: str | None = None
    affiliations: tuple[ORCIDAffiliation, ...] = ()
    raw: dict[str, Any] | None = None


def _date_from_parts(block: dict[str, Any] | None) -> str | None:
    if not isinstance(block, dict):
        return None
    year = ((block.get("year") or {}).get("value") or "").strip()
    if not year:
        return None
    month = ((block.get("month") or {}).get("value") or "").strip()
    day = ((block.get("day") or {}).get("value") or "").strip()
    if month and day:
        return f"{int(year):04d}-{int(month):02d}-{int(day):02d}"
    if month:
        return f"{int(year):04d}-{int(month):02d}"
    return f"{int(year):04d}"


def _text_value(block: Any) -> str:
    if isinstance(block, dict):
        value = block.get("value")
        if value is not None:
            return str(value).strip()
    if isinstance(block, str):
        return block.strip()
    return ""


def _extract_person(payload: dict[str, Any]) -> dict[str, Any]:
    person = payload.get("person") or {}
    name = person.get("name") or {}
    other_names_raw = (((person.get("other-names") or {}).get("other-name")) or [])
    other_names: list[str] = []
    if isinstance(other_names_raw, list):
        for item in other_names_raw:
            value = _text_value((item or {}).get("content") if isinstance(item, dict) else item)
            if value:
                other_names.append(value)
    addresses = (((person.get("addresses") or {}).get("address")) or [])
    country = None
    if isinstance(addresses, list) and addresses:
        country = _text_value((addresses[0] or {}).get("country"))
    return {
        "given_names": _text_value(name.get("given-names")),
        "family_name": _text_value(name.get("family-name")),
        "credit_name": _text_value(name.get("credit-name")),
        "other_names": tuple(dict.fromkeys(other_names)),
        "country": country,
    }


def _extract_affiliation_summary(summary: dict[str, Any], *, role: str) -> ORCIDAffiliation | None:
    org = summary.get("organization") or {}
    name = str(org.get("name") or "").strip()
    if not name:
        return None
    ror = None
    disambig = org.get("disambiguated-organization") or {}
    if isinstance(disambig, dict):
        source = str(disambig.get("disambiguation-source") or "").strip().upper()
        ident = str(disambig.get("disambiguated-organization-identifier") or "").strip()
        if source == "ROR" and ident:
            ror = ident
    start_date = _date_from_parts(summary.get("start-date"))
    end_date = _date_from_parts(summary.get("end-date"))
    url = _text_value(summary.get("url"))
    return ORCIDAffiliation(
        institution_name=name,
        role=role,
        start_date=start_date,
        end_date=end_date,
        is_current=not bool(end_date),
        institution_ror=ror,
        evidence_url=url or None,
    )


def _extract_affiliation_groups(payload: dict[str, Any], section: str, role: str) -> list[ORCIDAffiliation]:
    activities = payload.get("activities-summary") or {}
    block = activities.get(section) or {}
    groups = block.get("affiliation-group") or []
    out: list[ORCIDAffiliation] = []
    if not isinstance(groups, list):
        return out
    summary_key = "employment-summary" if section == "employments" else "education-summary"
    for group in groups:
        summaries = (group or {}).get("summaries") or []
        if not isinstance(summaries, list):
            continue
        for wrapper in summaries:
            summary = (wrapper or {}).get(summary_key) or {}
            if not isinstance(summary, dict):
                continue
            affiliation = _extract_affiliation_summary(summary, role=role)
            if affiliation:
                out.append(affiliation)
    return out


def parse_orcid_record(orcid: str, payload: dict[str, Any]) -> ORCIDRecord:
    person = _extract_person(payload)
    affiliations = (
        _extract_affiliation_groups(payload, "employments", "employment")
        + _extract_affiliation_groups(payload, "educations", "education")
    )
    return ORCIDRecord(
        orcid=normalize_orcid(orcid) or str(orcid or "").strip(),
        given_names=person["given_names"] or None,
        family_name=person["family_name"] or None,
        credit_name=person["credit_name"] or None,
        other_names=person["other_names"],
        country=person["country"] or None,
        affiliations=tuple(affiliations),
        raw=payload,
    )


def fetch_record_by_orcid(orcid: str) -> ORCIDRecord | None:
    """Fetch and parse one public ORCID record."""
    normalized = normalize_orcid(orcid or "")
    if not normalized:
        return None
    try:
        resp = get_source_http_client("orcid").get(f"/{normalized}/record", timeout=25)
    except Exception as exc:
        logger.warning("ORCID record fetch failed for %s: %s", normalized, exc)
        return None
    if resp.status_code == 404:
        return None
    if resp.status_code != 200:
        logger.warning("ORCID record fetch returned HTTP %d for %s", resp.status_code, normalized)
        return None
    try:
        payload = resp.json() or {}
    except Exception as exc:
        logger.warning("ORCID record JSON decode failed for %s: %s", normalized, exc)
        return None
    if not isinstance(payload, dict):
        return None
    return parse_orcid_record(normalized, payload)


def fetch_employments(orcid: str) -> list[ORCIDAffiliation]:
    """Return employment/education affiliations for one ORCID iD."""
    record = fetch_record_by_orcid(orcid)
    return list(record.affiliations) if record else []

