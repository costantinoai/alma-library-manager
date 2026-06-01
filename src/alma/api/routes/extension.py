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
from datetime import datetime
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, Field

from alma.api.deps import get_current_user, get_db
from alma.core.utils import resolve_existing_paper_id

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
        # Stable identity of THIS ALMa instance so the connector can be sure
        # an offline-queued capture is delivered to the database it was meant
        # for (dev / bare-metal / docker may each be reachable at different
        # times on the same localhost port). Deterministic + read-only — no
        # write on this GET. See tasks/28_EXTENSION_OFFLINE_CAPTURE.md.
        "instance": _instance_identity(),
    }


def _instance_identity() -> dict:
    """`{profile, db_fingerprint}` — which ALMa DB is behind this server.

    ``profile`` is ``prod``/``dev`` (the ``ALMA_ENV`` namespace);
    ``db_fingerprint`` is a short hash of the resolved DB path. Together they
    distinguish dev (``alma-dev`` profile), bare-metal prod, and docker
    (different container path) with no persisted state and no DB write — so
    it's safe to compute on a pure-read ``/ping``.
    """
    import hashlib
    import os

    from alma.config import get_db_path, get_env_profile

    try:
        profile = get_env_profile()
    except Exception:  # pragma: no cover - defensive only
        profile = "prod"
    fingerprint = ""
    try:
        real = os.path.realpath(str(get_db_path()))
        fingerprint = hashlib.sha256(real.encode("utf-8")).hexdigest()[:12]
    except Exception:  # pragma: no cover - defensive only
        fingerprint = ""
    return {"profile": profile, "db_fingerprint": fingerprint}


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

    # Capture the paper's prior state for Undo — best-effort, no writes.
    # If the paper doesn't exist yet, prior stays None and Undo reverts the
    # newly-created row to a bare tracked row (out of the Library).
    prior = None
    try:
        existing_id = resolve_existing_paper_id(
            db,
            openalex_id=req.openalex_id,
            doi=req.doi,
            title=req.title,
            year=req.year,
        )
        if existing_id:
            prow = db.execute(
                "SELECT status, rating, reading_status, added_from, added_at "
                "FROM papers WHERE id = ?",
                (existing_id,),
            ).fetchone()
            if prow:
                prior = dict(prow)
    except Exception:
        prior = None

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
        # Token the connector echoes back to /undo to reverse this save.
        "undo": {"paper_id": row.get("id"), "prior": prior},
    }


class ExtensionLookupRequest(BaseModel):
    """Identify a paper to check membership + (optionally) resolve metadata."""

    doi: Optional[str] = None
    openalex_id: Optional[str] = None
    title: Optional[str] = None
    year: Optional[int] = None
    # When True and the paper isn't in the local corpus (or has no title),
    # resolve display metadata from OpenAlex so the popup can show the real
    # title before the user saves. Off by default to keep the call local
    # and fast when the page already gave us a title.
    resolve: bool = False


def _resolve_preview(req: "ExtensionLookupRequest") -> Optional[dict]:
    """Best-effort upstream metadata resolve for the popup preview."""
    from alma.application.openalex_manual import resolve_work_metadata

    try:
        return resolve_work_metadata(
            openalex_id=req.openalex_id, doi=req.doi, title=req.title
        )
    except Exception as exc:  # network / upstream hiccup — preview only
        logger.info("Extension lookup preview resolve failed: %s", exc)
        return None


@router.post(
    "/lookup",
    summary="Check membership + resolve a paper's metadata (read-only)",
    description=(
        "Resolves the scraped identifiers against the local corpus and "
        "reports membership ('already in Library / Reading list'). Returns "
        "the title/authors/year/journal from the local row when present; "
        "with `resolve=true` it also fetches them from OpenAlex when the "
        "paper isn't local yet (so a PDF's title can be shown before "
        "saving). Pure read — never writes."
    ),
)
def lookup_from_extension(
    req: ExtensionLookupRequest,
    db: sqlite3.Connection = Depends(get_db),
    _user: dict = Depends(get_current_user),
):
    if not any(str(v or "").strip() for v in (req.doi, req.openalex_id, req.title)):
        return {"found": False}

    paper_id = resolve_existing_paper_id(
        db,
        openalex_id=req.openalex_id,
        doi=req.doi,
        title=req.title,
        year=req.year,
    )

    if paper_id:
        row = db.execute(
            "SELECT status, rating, reading_status, title, authors, year, journal "
            "FROM papers WHERE id = ?",
            (paper_id,),
        ).fetchone()
        if row:
            status = str((row["status"] or "")).strip().lower()
            out = {
                "found": True,
                "paper_id": paper_id,
                "status": status,
                "in_library": status == "library",
                "reading_status": row["reading_status"],
                "rating": row["rating"],
                "title": row["title"] or "",
                "authors": row["authors"] or "",
                "year": row["year"],
                "journal": row["journal"] or "",
            }
            # Local row exists but has no title yet — try upstream if asked.
            if req.resolve and not str(out["title"]).strip():
                meta = _resolve_preview(req)
                if meta:
                    for k in ("title", "authors", "year", "journal"):
                        if meta.get(k):
                            out[k] = meta[k]
            return out

    # Not in the local corpus. Resolve a preview from OpenAlex if asked.
    if req.resolve:
        meta = _resolve_preview(req)
        if meta:
            return {
                "found": False,
                "in_library": False,
                "title": meta.get("title") or "",
                "authors": meta.get("authors") or "",
                "year": meta.get("year"),
                "journal": meta.get("journal") or "",
                "doi": meta.get("doi") or "",
                "openalex_id": meta.get("openalex_id") or "",
            }

    return {"found": False}


class ExtensionUndoRequest(BaseModel):
    """Reverse a connector save using the token returned by /save."""

    paper_id: str
    # The paper's column values before the save, or null if the save created
    # the row (then Undo reverts it to a bare tracked row, out of Library).
    prior: Optional[dict] = None


@router.post(
    "/undo",
    summary="Reverse a connector save",
    description=(
        "Restores the paper to the state captured before the save (or, for "
        "a row the save created, reverts it to a tracked, non-Library row) "
        "and removes the positive feedback signal the save recorded. Never "
        "hard-deletes (D3)."
    ),
)
def undo_from_extension(
    req: ExtensionUndoRequest,
    db: sqlite3.Connection = Depends(get_db),
    _user: dict = Depends(get_current_user),
):
    paper_id = (req.paper_id or "").strip()
    if not paper_id:
        raise HTTPException(status_code=400, detail="paper_id is required")
    if not db.execute("SELECT id FROM papers WHERE id = ?", (paper_id,)).fetchone():
        raise HTTPException(status_code=404, detail="Paper not found")

    now = datetime.utcnow().isoformat()

    # Drop the positive feedback signal this save wrote (the most recent
    # browser_extension paper_action for this paper) so an undone save
    # doesn't keep teaching the recommender. The surface lives in
    # context_json, so match on it.
    try:
        db.execute(
            """
            DELETE FROM feedback_events
            WHERE id = (
                SELECT id FROM feedback_events
                WHERE entity_type = 'publication' AND entity_id = ?
                  AND event_type = 'paper_action'
                  AND context_json LIKE '%browser_extension%'
                ORDER BY created_at DESC, rowid DESC
                LIMIT 1
            )
            """,
            (paper_id,),
        )
    except Exception as exc:  # pragma: no cover - defensive
        logger.warning("Undo: could not remove feedback event for %s: %s", paper_id, exc)

    prior = req.prior or None
    if prior:
        db.execute(
            """
            UPDATE papers
            SET status = ?, rating = ?, reading_status = ?,
                added_from = ?, added_at = ?, updated_at = ?
            WHERE id = ?
            """,
            (
                prior.get("status"),
                prior.get("rating"),
                prior.get("reading_status"),
                prior.get("added_from"),
                prior.get("added_at"),
                now,
                paper_id,
            ),
        )
        result = "restored"
    else:
        db.execute(
            """
            UPDATE papers
            SET status = 'tracked', rating = 0, reading_status = NULL,
                added_from = NULL, added_at = NULL, updated_at = ?
            WHERE id = ?
            """,
            (now, paper_id),
        )
        result = "removed_from_library"

    db.commit()
    return {"ok": True, "paper_id": paper_id, "result": result}
