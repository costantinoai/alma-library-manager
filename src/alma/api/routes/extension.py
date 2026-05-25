"""Browser-extension connector endpoints.

A Firefox/Chrome connector (see the repo's ``extension/`` directory) lets
the user save the paper open in their browser straight into ALMa,
mirroring the Zotero connector. The connector scrapes citation metadata
from the page — DOI plus Highwire (``citation_*``), Dublin Core
(``dc.*`` / ``DC.*``), PRISM (``prism.*``) and Open Graph (``og:*``)
meta tags — and POSTs it here.

Design intent: this surface is a **thin** connector entry point. All
save / dedup / rating logic is delegated to the canonical
``alma.application.openalex_manual.save_online_search_result`` — the same
helper the Find-&-Add surface uses — stamped with
``added_from='browser_extension'``. DOI → OpenAlex resolution is the
primary path (best metadata); the scraped ``candidate`` metadata is the
fallback the helper uses when the page has no DOI (e.g. some preprints).

The only connector-specific choices layered on top of the shared helper
are:

* **action** ∈ ``{add, like, love}`` → the shared ``3/4/5`` star ratings.
  ``dislike`` is intentionally not offered — you don't open a paper in
  your browser in order to dislike it.
* **destination** ∈ ``{library, reading_list}`` → maps to the D2 reading
  axis. ``reading_list`` lands the paper on the reading list
  (``reading_status='reading'`` — note D2 has no separate ``queued``
  step); ``library`` leaves it untriaged in the library.

Both endpoints are pure with respect to GET semantics: ``/ping`` only
reads, ``/save`` is the single mutation.
"""

import logging
import sqlite3
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, Field

from alma.api.deps import get_current_user, get_db

logger = logging.getLogger(__name__)

router = APIRouter()

# Bumped when the request/response contract changes in a way that an
# already-installed connector would need to know about. The connector
# reads this from /ping to detect an ALMa build it can't talk to.
CONNECTOR_API_VERSION = 1

_VALID_ACTIONS = {"add", "like", "love"}
_VALID_DESTINATIONS = {"library", "reading_list"}

# destination → D2 reading-axis value forwarded to add_to_library.
# 'reading' = on the reading list; None = untriaged library row.
_DESTINATION_READING_STATUS = {
    "library": None,
    "reading_list": "reading",
}


class ExtensionSaveRequest(BaseModel):
    """A single paper scraped from the page the user has open.

    At least one resolvable identifier is required (``doi`` /
    ``openalex_id`` / ``title``). The remaining scraped fields are passed
    through as the ``candidate`` fallback so a DOI-less page (some
    preprints, working papers) still saves with the metadata we could
    read off the page.
    """

    action: str = Field("add", description="add | like | love → 3/4/5 stars")
    destination: str = Field(
        "library",
        description="library (untriaged) | reading_list (reading_status='reading')",
    )
    # Identifiers / resolution inputs (DOI preferred — OpenAlex enriches it).
    doi: Optional[str] = None
    openalex_id: Optional[str] = None
    title: Optional[str] = None
    # Scraped metadata — used to build the candidate fallback + as the
    # page link when there's no DOI.
    url: Optional[str] = None
    authors: Optional[str] = None
    year: Optional[int] = None
    journal: Optional[str] = None
    abstract: Optional[str] = None


@router.get(
    "/ping",
    summary="Connector handshake / health check",
    description=(
        "Lets the browser connector confirm it is talking to an ALMa "
        "build that exposes this endpoint and that the save contract is "
        "compatible. Pure read."
    ),
)
def ping(_user: dict = Depends(get_current_user)):
    # Lazy import to avoid a circular import at module load (app.py
    # imports this router while it is still initializing).
    try:
        from alma.api.app import API_VERSION
    except Exception:  # pragma: no cover - defensive only
        API_VERSION = None
    return {
        "ok": True,
        "service": "alma",
        "alma_version": API_VERSION,
        "connector_version": CONNECTOR_API_VERSION,
    }


@router.post(
    "/save",
    status_code=201,
    summary="Save the paper open in the browser into ALMa",
    description=(
        "Resolves the scraped paper (DOI → OpenAlex preferred, scraped "
        "metadata as fallback) and applies the shared add/like/love → "
        "3/4/5 contract via the canonical save helper, stamped "
        "`added_from='browser_extension'`. `destination='reading_list'` "
        "additionally lands the paper on the reading list."
    ),
)
def save_from_extension(
    req: ExtensionSaveRequest,
    db: sqlite3.Connection = Depends(get_db),
    _user: dict = Depends(get_current_user),
):
    from alma.application.openalex_manual import save_online_search_result

    action = (req.action or "add").strip().lower()
    if action not in _VALID_ACTIONS:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid action {action!r}. Must be one of: {sorted(_VALID_ACTIONS)}",
        )

    destination = (req.destination or "library").strip().lower()
    if destination not in _VALID_DESTINATIONS:
        raise HTTPException(
            status_code=400,
            detail=(
                f"Invalid destination {destination!r}. "
                f"Must be one of: {sorted(_VALID_DESTINATIONS)}"
            ),
        )

    # Require at least one resolvable identifier so we don't create an
    # empty row from a non-article page.
    if not any(
        str(v or "").strip() for v in (req.doi, req.openalex_id, req.title)
    ):
        raise HTTPException(
            status_code=400,
            detail="One of doi / openalex_id / title is required",
        )

    # The candidate fallback mirrors the feed-candidate shape that
    # `_upsert_candidate_paper` reads (title/authors/year/journal/
    # abstract/url/doi). Used only when OpenAlex can't resolve the DOI.
    candidate = {
        "title": req.title,
        "authors": req.authors,
        "year": req.year,
        "journal": req.journal,
        "abstract": req.abstract,
        "url": req.url,
        "doi": req.doi,
        "openalex_id": req.openalex_id,
        "source_api": "browser_extension",
    }

    try:
        row = save_online_search_result(
            db,
            openalex_id=req.openalex_id,
            doi=req.doi,
            link=req.url,
            title=req.title,
            candidate=candidate,
            action=action,
            added_from="browser_extension",
            default_reading_status=_DESTINATION_READING_STATUS[destination],
            # An explicit connector save is a strong user signal — its
            # provenance should win even over a row first seen via feed
            # (and over the 'feed' stamp the candidate-fallback upsert
            # applies to brand-new rows). Same rationale as D4 imports.
            override_added_from=True,
        )
    except ValueError as exc:
        # Could not resolve the paper from any input.
        raise HTTPException(status_code=422, detail=str(exc)) from exc
    except HTTPException:
        raise
    except Exception as exc:
        logger.warning("Browser-extension save failed: %s", exc)
        raise HTTPException(
            status_code=502,
            detail="Could not resolve the paper upstream. Try again.",
        ) from exc

    return {
        "paper_id": row.get("id"),
        "action": row.get("action"),
        "rating": row.get("rating"),
        "status": row.get("status"),
        "reading_status": row.get("reading_status"),
        "destination": destination,
        "match_source": row.get("match_source"),
        "added_from": row.get("added_from"),
        "title": row.get("title"),
    }
