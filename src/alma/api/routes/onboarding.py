"""Onboarding routes — thin orchestrators over existing use-cases.

The first-run flow is almost entirely a guided UI over machinery that already
exists (follow + backfill, author suggestions, keyword monitors, library lens,
lens refresh, paper triage). This router owns only the small pieces with no
home elsewhere:

- onboarding *state* (`onboarding.completed`, `user.name`) in the
  `discovery_settings` key/value table;
- the "you at the centre" owner-ingest (resolve identity → follow → mark owner
  → schedule the historical backfill → promote the owner's papers to Library);
- paper-level triage from a non-feed / non-discovery surface (the "react to the
  papers we just fetched" step), applying the D6 rating contract.

Everything else (keys, follows, monitors, lenses, recommendations) is driven by
the frontend against the existing endpoints — see ``tasks/02_ONBOARDING.md``.
"""

import logging
import sqlite3
from datetime import datetime
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, Field

from alma.api.deps import get_db, get_current_user
from alma.application import library as library_app
from alma.application.discovery.lens_crud import upsert_setting
from alma.application.followed_authors import (
    apply_follow_state,
    resolve_canonical_author_id,
    schedule_followed_author_historical_backfill,
)
from alma.core.db_write import run_write_unit
from alma.core.utils import normalize_orcid
from alma.openalex.client import (
    _normalize_openalex_author_id,
    fetch_author_profile,
    find_author_by_orcid,
)

logger = logging.getLogger(__name__)

router = APIRouter(
    prefix="/onboarding",
    tags=["onboarding"],
    dependencies=[Depends(get_current_user)],
    responses={401: {"description": "Unauthorized"}},
)

_COMPLETED_KEY = "onboarding.completed"
_COMPLETED_AT_KEY = "onboarding.completed_at"
_USER_NAME_KEY = "user.name"


# --------------------------------------------------------------------------- #
# Helpers
# --------------------------------------------------------------------------- #
def _get_kv(db: sqlite3.Connection, key: str) -> Optional[str]:
    """Read one raw ``discovery_settings`` value (no default-merge).

    ``lens_crud.read_settings`` merges discovery defaults over the row set, so
    it is the wrong reader for ``onboarding.*`` / ``user.*`` keys. A direct
    SELECT is the contract here.
    """
    try:
        row = db.execute(
            "SELECT value FROM discovery_settings WHERE key = ?", (key,)
        ).fetchone()
    except sqlite3.OperationalError:
        return None
    if not row:
        return None
    value = row["value"] if isinstance(row, sqlite3.Row) else row[0]
    return None if value is None else str(value)


def _truthy(value: Optional[str]) -> bool:
    return bool(value) and str(value).strip().lower() in ("1", "true", "yes")


def _count(db: sqlite3.Connection, sql: str) -> int:
    try:
        return int(db.execute(sql).fetchone()[0])
    except Exception:
        return 0


# --------------------------------------------------------------------------- #
# Models
# --------------------------------------------------------------------------- #
class OnboardingStatusResponse(BaseModel):
    completed: bool
    has_owner: bool
    library_count: int
    followed_count: int
    user_name: Optional[str] = None


class ProfileRequest(BaseModel):
    name: str = Field(min_length=1, max_length=120)


class ResolveOwnerRequest(BaseModel):
    orcid: Optional[str] = None
    openalex_id: Optional[str] = None


class OwnerProfileResponse(BaseModel):
    openalex_id: str
    name: Optional[str] = None
    institution: Optional[str] = None
    works_count: int = 0
    cited_by_count: int = 0
    orcid: Optional[str] = None


class IngestOwnerRequest(BaseModel):
    openalex_id: str = Field(min_length=1)
    name: Optional[str] = None


class IngestOwnerResponse(BaseModel):
    author_id: str
    openalex_id: str
    job_id: Optional[str] = None


class PromoteOwnerResponse(BaseModel):
    promoted: int


_PAPER_ACTIONS = {"add", "like", "love", "dislike", "dismiss"}
_ACTION_RATING = {"add": 3, "like": 4, "love": 5}


class PaperFeedbackRequest(BaseModel):
    paper_id: str = Field(min_length=1)
    action: str


class PaperFeedbackResponse(BaseModel):
    paper_id: str
    action: str
    status: Optional[str] = None
    rating: Optional[int] = None


# --------------------------------------------------------------------------- #
# State
# --------------------------------------------------------------------------- #
@router.get("/status", response_model=OnboardingStatusResponse)
def get_onboarding_status(
    db: sqlite3.Connection = Depends(get_db),
    user: dict = Depends(get_current_user),
):
    """First-run state + the counts the gate and step copy read."""
    return OnboardingStatusResponse(
        completed=_truthy(_get_kv(db, _COMPLETED_KEY)),
        has_owner=_count(db, "SELECT COUNT(*) FROM followed_authors WHERE is_owner = 1") > 0,
        library_count=_count(db, "SELECT COUNT(*) FROM papers WHERE status = 'library'"),
        followed_count=_count(db, "SELECT COUNT(*) FROM followed_authors"),
        user_name=_get_kv(db, _USER_NAME_KEY),
    )


@router.post("/profile", status_code=204)
def set_onboarding_profile(
    payload: ProfileRequest,
    db: sqlite3.Connection = Depends(get_db),
    user: dict = Depends(get_current_user),
):
    """Store the user's display name (local greeting; no external call)."""
    name = payload.name.strip()

    run_write_unit(
        db,
        lambda: upsert_setting(db, _USER_NAME_KEY, name),
        label="onboarding_profile",
    )


@router.post("/complete", status_code=204)
def complete_onboarding(
    db: sqlite3.Connection = Depends(get_db),
    user: dict = Depends(get_current_user),
):
    """Mark onboarding done so the gate stops showing, then start the
    MANDATORY convergence chain (audit 39 finding #4): the coordinator walks
    the maintenance registry's dependency order — identity → metadata →
    vectors → local embeddings → centroids → reference graph → cluster
    labels → topic normalization — until nothing actionable remains, so the
    fresh library ends healthy, not "queued". Its `auto:onboarding_complete`
    trigger is user-facing (never yields to the idle gate) and survives
    restarts via the orphan-resume path."""
    completed_at = datetime.utcnow().isoformat()

    def _persist() -> None:
        upsert_setting(db, _COMPLETED_KEY, "true")
        upsert_setting(db, _COMPLETED_AT_KEY, completed_at)

    run_write_unit(db, _persist, label="onboarding_complete")

    # Post-commit: scheduling in-transaction self-deadlocks against the
    # scheduler's own connection (see ingest_owner).
    try:
        from alma.services.maintenance import schedule_onboarding_convergence

        job_id = schedule_onboarding_convergence()
        if job_id:
            logger.info("onboarding complete: convergence chain queued (job %s)", job_id)
    except Exception:
        # Completion itself must never fail on the kick — the durable
        # pending ledger + idle drain still converge the library later.
        logger.warning("onboarding-complete convergence kick failed", exc_info=True)


@router.post("/reset", status_code=204)
def reset_onboarding(
    db: sqlite3.Connection = Depends(get_db),
    user: dict = Depends(get_current_user),
):
    """Clear the completed flag so Settings → Restart re-shows the flow."""
    try:
        run_write_unit(
            db,
            lambda: db.execute(
                "DELETE FROM discovery_settings WHERE key IN (?, ?)",
                (_COMPLETED_KEY, _COMPLETED_AT_KEY),
            ),
            label="onboarding_reset",
        )
    except sqlite3.OperationalError:
        pass


# --------------------------------------------------------------------------- #
# Owner identity (you, at the centre)
# --------------------------------------------------------------------------- #
@router.post("/resolve-owner", response_model=OwnerProfileResponse)
def resolve_owner_identity(
    payload: ResolveOwnerRequest,
    db: sqlite3.Connection = Depends(get_db),
    user: dict = Depends(get_current_user),
):
    """Resolve an ORCID or OpenAlex id to a profile for the confirm card.

    Read-only — no follow, no write. The frontend shows the resolved name +
    affiliation + works count, then calls ``/ingest-owner`` on confirm.
    """
    openalex_id = ""
    fallback_name: Optional[str] = None

    raw_openalex = (payload.openalex_id or "").strip()
    raw_orcid = (payload.orcid or "").strip()

    if raw_openalex:
        openalex_id = _normalize_openalex_author_id(raw_openalex)
    elif raw_orcid:
        # normalize_orcid validates the checksum; fall back to the raw value
        # (find_author_by_orcid does its own light prefix strip) so a slightly
        # off ORCID still gets one real lookup attempt.
        lookup = normalize_orcid(raw_orcid) or raw_orcid
        rec = find_author_by_orcid(lookup)
        if not rec or not rec.get("id"):
            raise HTTPException(
                status_code=404,
                detail="No OpenAlex author found for that ORCID.",
            )
        openalex_id = _normalize_openalex_author_id(str(rec.get("id")))
        fallback_name = rec.get("display_name")
    else:
        raise HTTPException(
            status_code=400, detail="Provide an ORCID or an OpenAlex id."
        )

    if not openalex_id:
        raise HTTPException(status_code=404, detail="Could not resolve an author.")

    profile = fetch_author_profile(openalex_id) or {}
    profile_orcid = profile.get("orcid")
    return OwnerProfileResponse(
        openalex_id=openalex_id,
        name=profile.get("display_name") or fallback_name,
        institution=profile.get("affiliation"),
        works_count=int(profile.get("works_count") or 0),
        cited_by_count=int(profile.get("citedby") or 0),
        orcid=normalize_orcid(profile_orcid) if profile_orcid else (normalize_orcid(raw_orcid) or None),
    )


@router.post("/ingest-owner", response_model=IngestOwnerResponse)
def ingest_owner(
    payload: IngestOwnerRequest,
    db: sqlite3.Connection = Depends(get_db),
    user: dict = Depends(get_current_user),
):
    """Follow the user's own author profile + schedule the full backfill.

    Reuses the canonical follow path (``apply_follow_state`` +
    ``schedule_followed_author_historical_backfill``) so all three follow
    tables and the deep-refresh job behave exactly as a normal follow — then
    marks the row as the single owner. The owner's papers are promoted to the
    Library by ``/promote-owner-papers`` once the backfill has landed them.
    """
    openalex_id = _normalize_openalex_author_id(payload.openalex_id.strip())
    if not openalex_id:
        raise HTTPException(status_code=400, detail="Invalid OpenAlex id.")

    def _persist() -> tuple[str, bool]:
        # One atomic follow unit (writer gate + BEGIN IMMEDIATE + retry),
        # mirroring follow_author: resolve/create the author, apply follow
        # state, and stamp the single-owner flag. The hydration SWEEP is
        # scheduled only AFTER commit — scheduling in-transaction
        # self-deadlocks against the scheduler's own connection.
        cid = resolve_canonical_author_id(
            db,
            openalex_id,
            create_if_missing=True,
            fallback_name=(payload.name or "").strip() or openalex_id,
        )
        if not cid:
            raise HTTPException(status_code=400, detail="Could not create author row.")
        needs_sweep = apply_follow_state(db, cid, followed=True)
        # Single owner: clear any prior owner before setting this one so the
        # partial unique index (is_owner = 1) never collides.
        db.execute("UPDATE followed_authors SET is_owner = 0 WHERE is_owner = 1")
        db.execute(
            "UPDATE followed_authors SET is_owner = 1 WHERE author_id = ?",
            (cid,),
        )
        return cid, needs_sweep

    canonical_id, needs_hydration_sweep = run_write_unit(
        db, _persist, label="onboarding_ingest_owner"
    )

    # Post-commit: scheduler-connection writes can't contend with us now
    # (see apply_follow_state on the in-transaction self-deadlock).
    if needs_hydration_sweep:
        try:
            from alma.services.author_hydrate import (
                schedule_pending_author_hydration_sweep,
            )

            schedule_pending_author_hydration_sweep(
                reason="author_follow",
                target_author_ids=[canonical_id],
            )
        except Exception:
            pass

    envelope = schedule_followed_author_historical_backfill(
        canonical_id, trigger="onboarding_owner"
    )
    job_id = str((envelope or {}).get("job_id") or "") or None
    return IngestOwnerResponse(
        author_id=canonical_id, openalex_id=openalex_id, job_id=job_id
    )


@router.post("/promote-owner-papers", response_model=PromoteOwnerResponse)
def promote_owner_papers(
    db: sqlite3.Connection = Depends(get_db),
    user: dict = Depends(get_current_user),
):
    """Save the owner's backfilled papers into the Library (idempotent).

    Called by the frontend once the owner backfill job completes. Selects every
    paper attributed to the owner via ``publication_authors`` and routes it
    through ``add_to_library`` (monotonic rating, provenance preserved).
    """
    try:
        rows = db.execute(
            """
            SELECT DISTINCT pa.paper_id AS paper_id
            FROM publication_authors pa
            JOIN authors a ON lower(a.openalex_id) = lower(pa.openalex_id)
            JOIN followed_authors fa ON fa.author_id = a.id
            WHERE fa.is_owner = 1
            """
        ).fetchall()
    except sqlite3.OperationalError:
        rows = []

    def _persist() -> int:
        promoted = 0
        for row in rows:
            paper_id = str(row["paper_id"] if isinstance(row, sqlite3.Row) else row[0])
            if not paper_id:
                continue
            try:
                if library_app.add_to_library(
                    db, paper_id, rating=3, added_from="onboarding"
                ):
                    promoted += 1
            except Exception as exc:  # pragma: no cover - defensive
                logger.debug("owner paper promote skipped for %s: %s", paper_id, exc)
        return promoted

    promoted = run_write_unit(db, _persist, label="onboarding_promote_owner_papers")
    return PromoteOwnerResponse(promoted=promoted)


# --------------------------------------------------------------------------- #
# Paper triage (react to the papers we just fetched)
# --------------------------------------------------------------------------- #
@router.post("/paper-feedback", response_model=PaperFeedbackResponse)
def onboarding_paper_feedback(
    payload: PaperFeedbackRequest,
    db: sqlite3.Connection = Depends(get_db),
    user: dict = Depends(get_current_user),
):
    """Apply the D6 triage contract to any corpus paper.

    Fills the gap for paper-level triage from a surface that is neither Feed nor
    Discovery. Delegates to the canonical library use-cases so ratings stay
    monotonic and Feed/Discovery rows for the same paper stay reconciled:

    - ``add``  → save (rating 3)
    - ``like`` → save (rating 4)
    - ``love`` → save (rating 5)
    - ``dislike`` → signal only (rating 1, stays in corpus)
    - ``dismiss`` → hide (status ``dismissed``, rating 1)
    - ``undo`` → reverse the above: back to a neutral corpus row (tracked,
      rating 0) AND delete the onboarding-generated signal, so re-clicking an
      applied action toggles it fully off.
    """
    action = payload.action.strip().lower()
    if action != "undo" and action not in _PAPER_ACTIONS:
        raise HTTPException(status_code=400, detail=f"Unknown action: {action}")

    paper_id = payload.paper_id.strip()
    exists = db.execute(
        "SELECT 1 FROM papers WHERE id = ? LIMIT 1", (paper_id,)
    ).fetchone()
    if not exists:
        raise HTTPException(status_code=404, detail="Paper not found.")

    if action == "undo":
        # Surface-independent: a paper has one signal, so undo clears it all
        # (the canonical use-case), not just onboarding-tagged events.
        result = run_write_unit(
            db,
            lambda: library_app.undo_paper_feedback(db, paper_id),
            label="onboarding_undo_feedback",
        )
        return PaperFeedbackResponse(
            paper_id=paper_id,
            action="undo",
            status=result.get("status"),
            rating=result.get("rating"),
        )

    def _persist() -> None:
        # One atomic triage unit: membership/sink/dismiss + signal event +
        # cross-surface reconciliation. add_to_library defers its enrichment
        # scheduling past the writer gate.
        if action in _ACTION_RATING:
            rating = _ACTION_RATING[action]
            library_app.add_to_library(db, paper_id, rating=rating, added_from="onboarding")
            library_app.record_paper_feedback(
                db, paper_id, action=action, rating=rating, source_surface="onboarding"
            )
        elif action == "dislike":
            library_app.sink_disliked_paper(db, paper_id)
            library_app.record_paper_feedback(
                db, paper_id, action="dislike",
                rating=library_app.DISLIKE_RATING, source_surface="onboarding",
            )
        else:  # dismiss
            library_app.dismiss_paper(db, paper_id)
            library_app.record_paper_feedback(
                db, paper_id, action="dismiss",
                rating=library_app.DISLIKE_RATING, source_surface="onboarding",
            )
        library_app.sync_surface_resolution(
            db, paper_id, action=action, source_surface="onboarding"
        )

    run_write_unit(db, _persist, label="onboarding_paper_feedback")

    row = db.execute(
        "SELECT status, rating FROM papers WHERE id = ?", (paper_id,)
    ).fetchone()
    return PaperFeedbackResponse(
        paper_id=paper_id,
        action=action,
        status=str(row["status"]) if row and row["status"] is not None else None,
        rating=int(row["rating"]) if row and row["rating"] is not None else None,
    )
