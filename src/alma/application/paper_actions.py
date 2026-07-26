"""Inbox membership transitions — the two verbs that move a paper in and out.

The Inbox is a **buffer** on the membership axis (D2), between `tracked` and
`library`:

    tracked ──promote──▶ inbox ──triage──▶ library / dismissed
                           │
                           └──defer (the X button)──▶ tracked

Both verbs here are deliberately **valence-free**. Neither writes
`papers.rating`, neither records a `feedback_events` row, and neither shifts the
Discovery centroid (every centroid query is scoped to `status='library'`). A
paper can sit in the Inbox all morning and be X'd out at noon having left no
mark on your recommendations — which is the property that makes a two-second
phone flick a safe gesture.

This module will also host `apply_paper_action`, the single owner of the
valence verbs (save / like / love / dislike) and the scoped `dismiss` registry,
once `application/library.py` is free to delegate to it. Until then it owns only
the two transitions the Inbox itself needs.
"""

from __future__ import annotations

import logging
import sqlite3

from alma.core.time import utcnow

logger = logging.getLogger(__name__)

#: Membership values, mirrored from `application.library` rather than imported,
#: to keep this module importable from the capture pipeline without dragging in
#: the whole Library surface. `library.py` owns the canonical constants; these
#: are asserted equal by `tests/test_inbox_membership.py`.
TRACKED_STATUS = "tracked"
INBOX_STATUS = "inbox"
LIBRARY_STATUS = "library"

#: Membership states a capture is allowed to lift INTO the Inbox.
#:
#: `library` is absent on purpose and that is the important part: re-sending a
#: paper you already saved must never demote it out of your Library. Such a
#: capture is reported as `duplicate` and the row is left untouched.
#:
#: `dismissed` / `removed` ARE promotable — deliberately sending a paper again
#: is you reconsidering it, and the Inbox is where that reconsideration belongs.
#: An empty status covers rows written by upserts that never stamped one.
PROMOTABLE_STATUSES = frozenset({TRACKED_STATUS, "candidate", "dismissed", "removed", ""})


#: THE rating contract. One action → one star value, spelled ONCE.
#:
#: This lived in four places (`library._PAPER_TRIAGE_RATINGS`,
#: `openalex_manual._ONLINE_SEARCH_ACTION_RATINGS`, and twice inside
#: `feed.apply_feed_action`). Four copies of a contract means changing it
#: requires finding all four, and the one you miss fails silently — a Feed
#: "Like" and a Discovery "Like" quietly disagreeing about what a like is worth.
ACTION_RATINGS: dict[str, int] = {"add": 3, "like": 4, "love": 5, "dislike": 1}

#: Action → preference signal. `add` is 0 on purpose: saving is a membership
#: decision, and 3★ is the neutral star (`signal_valence.RATING_NEUTRAL`), so a
#: plain save must not read as praise.
ACTION_SIGNAL_VALUES: dict[str, int] = {"add": 0, "like": 1, "love": 2, "dislike": -1}

#: Actions that express an opinion. Everything else (`dismiss`, `defer`,
#: `read`, `seen`) is visibility or membership and writes no valence.
VALENCE_ACTIONS: frozenset[str] = frozenset(ACTION_RATINGS)


def apply_valence(
    db: sqlite3.Connection,
    paper_id: str,
    action: str,
    *,
    added_from: str,
    collection_ids: list[str] | None = None,
) -> dict:
    """Apply the membership + rating half of a valence action. THE owner.

    Every surface — Feed, Discovery, Library, Map, Inbox, online search — routes
    its add/like/love/dislike through here, so the rating contract cannot drift
    between them. Surfaces keep their OWN event recording afterwards, because
    the context genuinely differs (Feed carries monitor provenance, Discovery
    carries the lens); only this shared half is centralised.

    Rating is monotonically upgraded — `add_to_library`'s SQL does
    ``CASE WHEN rating > ? THEN rating ELSE ?`` — so add-after-love never
    demotes a loved paper to 3. `dislike` is the exception: it is an explicit
    new opinion, sets rating 1, and deliberately leaves membership alone so a
    disliked paper stays visible where it was.

    Returns ``{"rating": int, "status": str | None}`` — the paper's state AFTER
    the write, so callers report the truth rather than what they intended.

    Caller owns the transaction.
    """
    from alma.application import library as library_app

    normalized = str(action or "").strip().lower()
    if normalized not in VALENCE_ACTIONS:
        raise ValueError(
            f"{normalized!r} is not a valence action; "
            f"expected one of {sorted(VALENCE_ACTIONS)}"
        )

    paper_id = str(paper_id or "").strip()
    if not paper_id:
        raise ValueError("paper_id is required")

    if normalized == "dislike":
        # Preference only: rating 1, membership untouched (D6).
        library_app.sink_disliked_paper(db, paper_id)
    else:
        target = ACTION_RATINGS[normalized]
        if collection_ids:
            library_app.save_paper_to_collections(
                db, paper_id, collection_ids, rating=target, added_from=added_from
            )
        else:
            library_app.add_to_library(
                db, paper_id, rating=target, added_from=added_from
            )

    row = db.execute(
        "SELECT status, rating FROM papers WHERE id = ?", (paper_id,)
    ).fetchone()
    return {
        "rating": int((row["rating"] if row else 0) or 0),
        "status": str(row["status"]) if row and row["status"] else None,
    }


def promote_to_inbox(
    db: sqlite3.Connection,
    paper_id: str,
    *,
    source: str,
) -> bool:
    """Park an already-in-corpus paper in the Inbox. Returns True if it moved.

    Writes membership and provenance only — no rating, no feedback event. The
    paper is a full corpus citizen either way: enrichment, dedup, search and the
    semantic map all walk `papers` without filtering on status, so an Inbox
    paper is hydrated and placed on the map while it waits for you.

    ``source`` is the delivery channel (``'slack'``, ``'email'``, …). It is
    stamped as `added_from` ONLY when the row has no provenance yet: a paper
    first seen via Feed that you later send yourself is still originally a Feed
    paper, and the capture's own provenance lives on its `inbox_messages` row.

    The caller owns the transaction (`run_write_unit` / `write_section`).
    """
    paper_id = str(paper_id or "").strip()
    if not paper_id:
        return False

    row = db.execute(
        "SELECT status FROM papers WHERE id = ?", (paper_id,)
    ).fetchone()
    if row is None:
        return False

    status = str(row["status"] or "").strip().lower()
    if status not in PROMOTABLE_STATUSES:
        # Already `library` (or some future membership we must not clobber).
        return False

    now = utcnow().isoformat()
    cursor = db.execute(
        """
        UPDATE papers
        SET status = ?,
            added_from = CASE
                WHEN COALESCE(TRIM(added_from), '') = '' THEN ?
                ELSE added_from
            END,
            updated_at = ?
        WHERE id = ?
        """,
        (INBOX_STATUS, source, now, paper_id),
    )
    return cursor.rowcount > 0


def defer_from_inbox(db: sqlite3.Connection, paper_id: str) -> bool:
    """The X button: drop a paper OUT of the Inbox, keeping it in the corpus.

    Means "I looked at this and have no action for it" — emphatically NOT "this
    is bad". So it writes membership and nothing else: no rating, no
    `feedback_events` row, no lens signal. The paper returns to `tracked`, its
    ordinary corpus resting state.

    This is why it cannot reuse `dismiss`. `library.dismiss_paper` sets
    `status='dismissed'` AND `rating=1`, and the triage primitive pairs it with
    a negative feedback event — so routing the X button through it would teach
    Discovery to avoid papers whose only sin was being uninteresting on a
    Tuesday. Under the amended D6, negative opinion is `dislike`; "bad and gone"
    is dislike + dismiss, two verbs the user composes deliberately.

    Idempotent: a paper not in the Inbox is left alone and False is returned.
    The caller owns the transaction.
    """
    paper_id = str(paper_id or "").strip()
    if not paper_id:
        return False

    now = utcnow().isoformat()
    cursor = db.execute(
        "UPDATE papers SET status = ?, updated_at = ? WHERE id = ? AND status = ?",
        (TRACKED_STATUS, now, paper_id, INBOX_STATUS),
    )
    return cursor.rowcount > 0


# ---------------------------------------------------------------------------
# The one entry point for every paper action, on every surface
# ---------------------------------------------------------------------------

#: Surfaces that own a per-row artifact and therefore need a `scope_ref`:
#: Feed settles its `feed_items` row, Discovery its `recommendations` row.
#: Everything else acts on the paper alone.
SCOPED_SURFACES: frozenset[str] = frozenset({"feed", "discovery"})

#: Every surface allowed to act. A surface not listed here cannot mutate a
#: paper — the route rejects it — so a typo becomes a 400, not a silent
#: mis-attributed feedback event.
VALID_SURFACES: frozenset[str] = frozenset(
    {"feed", "discovery", "inbox", "map", "papers", "library", "onboarding"}
)

#: `save` is Discovery's word for `add`. Normalised here so the wire contract
#: has ONE vocabulary and the surfaces keep their internal spelling.
_ACTION_ALIASES = {"save": "add"}


def apply_paper_action(
    db: sqlite3.Connection,
    paper_id: str,
    action: str,
    *,
    surface: str,
    scope_ref: str | None = None,
    collection_ids: list[str] | None = None,
    undo_aspect: str = "all",
) -> dict:
    """Apply ONE user action to ONE paper, from whichever surface raised it.

    The single entry point behind ``POST /papers/{paper_id}/action``. Feed,
    Discovery, Inbox, Map, Library and onboarding all come through here, so
    "what does Like mean" has exactly one answer.

    **Shared vs scoped.** The valence half (membership + rating) is shared —
    every path lands in :func:`apply_valence`, the owner of the rating
    contract. What is genuinely per-surface is *resolution*: Feed settles its
    own `feed_items` row and records monitor provenance; Discovery settles one
    `recommendations` row and records a lens signal. Those adapters keep that
    context, because flattening it would break outcome calibration — it is
    real difference, not duplication.

    ``scope_ref`` is the surface's own row id (feed item / recommendation) and
    is REQUIRED for those surfaces: acting on "the paper" from Feed without
    saying which feed row is ambiguous the moment two monitors surface it.

    Returns ``{paper_id, action, surface, status, rating}`` plus whatever the
    surface adapter reports under ``surface_result``.

    The caller owns the transaction for generic surfaces; the Feed and
    Discovery adapters open their own write unit (they always did).
    """
    from alma.application import library as library_app

    normalized = str(action or "").strip().lower()
    normalized = _ACTION_ALIASES.get(normalized, normalized)
    surface = str(surface or "").strip().lower()

    if surface not in VALID_SURFACES:
        raise ValueError(
            f"Unknown surface {surface!r}; expected one of {sorted(VALID_SURFACES)}"
        )
    if surface in SCOPED_SURFACES and not str(scope_ref or "").strip():
        raise ValueError(
            f"surface={surface!r} requires scope_ref "
            f"({'feed item id' if surface == 'feed' else 'recommendation id'}) "
            "— it settles that row, not the paper globally"
        )

    surface_result: dict | None = None

    if surface == "feed":
        from alma.application import feed as feed_app

        if normalized == "undo":
            surface_result = feed_app.undo_feed_dismiss(db, str(scope_ref))
        else:
            surface_result = feed_app.apply_feed_action(
                db, str(scope_ref), normalized, collection_ids=collection_ids
            )
        if surface_result is None:
            raise LookupError("Feed item not found")

    elif surface == "discovery":
        from alma.application.discovery import mark_recommendation_action

        # Discovery keeps `save` as its stored `user_action` vocabulary.
        rec_action = "save" if normalized == "add" else normalized
        surface_result = mark_recommendation_action(
            db, str(scope_ref), rec_action, collection_ids=collection_ids
        )
        if surface_result is None:
            raise LookupError("Recommendation not found")

    else:
        # Generic corpus surfaces — Inbox, Map, paper detail, Library,
        # onboarding. `apply_corpus_paper_feedback` owns membership + rating +
        # signal + cross-surface reconciliation for them.
        if normalized == "undo":
            surface_result = library_app.undo_paper_feedback(
                db, paper_id, undo_aspect
            )
        else:
            surface_result = library_app.apply_corpus_paper_feedback(
                db, paper_id, normalized, source_surface=surface
            )

    resolved_id = str(
        (surface_result or {}).get("paper_id") or paper_id or ""
    ).strip()
    row = (
        db.execute(
            "SELECT status, rating FROM papers WHERE id = ?", (resolved_id,)
        ).fetchone()
        if resolved_id
        else None
    )
    return {
        "paper_id": resolved_id or None,
        "action": normalized,
        "surface": surface,
        "status": str(row["status"]) if row and row["status"] else None,
        "rating": int((row["rating"] if row else 0) or 0),
        "surface_result": surface_result,
    }
