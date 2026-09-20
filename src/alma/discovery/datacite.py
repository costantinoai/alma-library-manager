"""DataCite relation lookup — the part-of signal for registered datasets.

Crossref registers articles; DataCite registers the datasets, software and
supplements that hang off them (Zenodo, figshare, Dryad, OSF). `papers` has
carried a `relation_parent_doi` input since components landed, and
`core.components.resolve_component` has always accepted it, but only Crossref
ever produced one — so a deposited dataset with an explicit "supplement to
<article>" relation stayed a standalone paper in the corpus, counted and ranked
as if it were a work of its own.

Only ``IsSupplementTo`` is trusted, the same decision
`crossref.parent_doi_from_relation` documents: ``IsPartOf`` usually names the
collection or the journal, which would turn a real article into a component.
"""

from __future__ import annotations

import logging
from typing import Any

from alma.core.http_sources import get_source_http_client
from alma.core.utils import normalize_doi

logger = logging.getLogger(__name__)

#: The one relation that means "this work is material belonging to that work".
SUPPLEMENT_RELATION = "issupplementto"


def parent_doi_from_related_identifiers(related: object) -> str | None:
    """Parent DOI from a DataCite ``relatedIdentifiers`` block, or ``None``.

    PURE: no network, no DB — so the rule is testable without either.
    """
    if not isinstance(related, list):
        return None
    for entry in related:
        if not isinstance(entry, dict):
            continue
        if str(entry.get("relationType") or "").strip().lower() != SUPPLEMENT_RELATION:
            continue
        if str(entry.get("relatedIdentifierType") or "").strip().lower() != "doi":
            continue
        parent = normalize_doi(str(entry.get("relatedIdentifier") or ""))
        if parent:
            return parent
    return None


def fetch_parent_dois(dois: list[str]) -> dict[str, str]:
    """``{doi: parent_doi}`` for the DOIs DataCite knows and calls a supplement.

    One request per DOI (``GET /dois/{doi}``): DataCite's bulk query is a
    fielded search rather than an id filter, and the caller only ever passes the
    tail of DOIs Crossref did not resolve. A DOI DataCite does not have (404) or
    that carries no supplement relation is simply absent from the result — never
    an entry that says "no parent", which a caller could mistake for an answer.

    Raises nothing: a registry being down must not fail the metadata sweep that
    calls it. Failures are logged and the DOI is left for the next run.
    """
    found: dict[str, str] = {}
    if not dois:
        return found
    client = get_source_http_client("datacite")
    for raw in dois:
        doi = normalize_doi(str(raw or ""))
        if not doi:
            continue
        try:
            response = client.get(f"/dois/{doi}", timeout=20)
            if response.status_code == 404:
                continue
            response.raise_for_status()
            payload: dict[str, Any] = response.json() or {}
        except Exception as exc:
            logger.warning("DataCite lookup failed for %s: %s", doi, exc)
            continue
        attributes = ((payload.get("data") or {}).get("attributes") or {})
        parent = parent_doi_from_related_identifiers(attributes.get("relatedIdentifiers"))
        if parent and parent.lower() != doi.lower():
            found[doi.lower()] = parent
    return found
