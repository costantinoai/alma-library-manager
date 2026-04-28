"""Author soft-removal / garbage-collection helpers.

Mirrors the lifecycle pattern used for `papers.status` (see
`tasks/PRODUCT_DECISIONS.md` D3): when an author is no longer
attached to any "live" paper and is not explicitly followed, we set
`authors.status = 'removed'` instead of hard-deleting the row.

Why soft-remove instead of DELETE:
  - Discovery still benefits from the row as a *negative* signal (the
    user previously interacted with this author, then dropped them —
    don't surface them again).
  - We keep provenance: existing publication_authors rows on
    soft-removed papers still resolve to a known display name + ORCID
    + institution, so paper dossiers stay legible.
  - Re-following / re-appearing on a fresh library paper just flips
    the row back to status='active' — no row recreation, no lost ID
    resolution metadata.

What "orphan" means here:
  An author is orphan iff
    1. NOT in `followed_authors` (not explicitly tracked), AND
    2. Has no `publication_authors` row pointing to a paper whose
       status is one of the *live* paper states (anything except
       'removed' / 'dismissed').
  Authors without ANY paper attachment also count as orphan.

Triggers:
  - Eager: paper soft-remove / unfollow / paper hard-delete cascades
    call `gc_author_if_orphan` for each affected author_id.
  - Periodic: `garbage_collect_orphan_authors` walks every active
    author and applies the rule. Exposed via the
    `POST /authors/garbage-collect-orphans` endpoint.

Audited:
  Every soft-removal writes an `operation_logs` entry with
  `step='gc_author_soft_removed'` so the Activity feed shows what
  was collected, when, and why.
"""

from __future__ import annotations

import logging
import sqlite3
from datetime import datetime
from typing import Iterable, Optional

logger = logging.getLogger(__name__)


# Paper states that count as a "reason to keep" the author. Anything
# OUTSIDE this set (currently 'removed' and 'dismissed') is treated as
# a dead attachment for GC purposes — see D3.
_LIVE_PAPER_STATES = ("library", "tracked", "queued", "candidate", "saved")


def is_orphan_author(db: sqlite3.Connection, author_id: str) -> bool:
    """Return True iff the author has no live attachments and is not followed.

    Implementation detail — the join is on `lower(trim(openalex_id))`
    because that's how `publication_authors` is canonicalised
    everywhere else in the codebase (see lesson "Paper deduplication"
    + canonical-triple discussion). For placeholder authors with no
    openalex_id, we also check display_name fallback so the rule still
    catches imported-only rows.
    """
    aid = (author_id or "").strip()
    if not aid:
        return False

    # Followed → never orphan.
    followed_row = db.execute(
        "SELECT 1 FROM followed_authors WHERE author_id = ? LIMIT 1", (aid,),
    ).fetchone()
    if followed_row is not None:
        return False

    # Pull the author's openalex_id + name once so we can run the
    # attachment lookup against both keys (publication_authors uses
    # openalex_id when known, display_name when not).
    row = db.execute(
        "SELECT openalex_id, name FROM authors WHERE id = ?", (aid,),
    ).fetchone()
    if row is None:
        return False
    oid_norm = (row["openalex_id"] or "").strip().lower()
    name_norm = (row["name"] or "").strip().lower()

    placeholders = ",".join("?" for _ in _LIVE_PAPER_STATES)
    query = f"""
        SELECT 1
        FROM publication_authors pa
        JOIN papers p ON p.id = pa.paper_id AND p.status IN ({placeholders})
        WHERE (
              (? <> '' AND lower(trim(pa.openalex_id))   = ?)
           OR (? <> '' AND lower(trim(pa.display_name))  = ?)
        )
        LIMIT 1
    """
    params = (*_LIVE_PAPER_STATES, oid_norm, oid_norm, name_norm, name_norm)
    live_row = db.execute(query, params).fetchone()
    return live_row is None


def soft_remove_author(
    db: sqlite3.Connection,
    author_id: str,
    *,
    reason: str,
    job_id: Optional[str] = None,
) -> bool:
    """Mark an author as `status='removed'` and write an audit log.

    Idempotent: re-running on an already-removed row updates nothing
    and writes no log. Returns True when a transition actually
    happened, False otherwise.

    `reason` is a short human-readable phrase explaining the trigger
    (e.g. "library paper X removed", "unfollow", "manual sweep") —
    surfaces in the Activity row so the user can audit what got
    collected.
    """
    aid = (author_id or "").strip()
    if not aid:
        return False

    row = db.execute(
        "SELECT name, status FROM authors WHERE id = ?", (aid,),
    ).fetchone()
    if row is None:
        return False
    if (row["status"] or "active") == "removed":
        return False

    now = datetime.utcnow().isoformat()
    db.execute(
        "UPDATE authors SET status = 'removed' WHERE id = ?", (aid,),
    )
    # `_persist_job_log` opens its own connection so we don't have to
    # commit here — but the caller usually does, both for the GC's own
    # sake and because most callers are inside a write-batch.
    try:
        from alma.api.scheduler import add_job_log

        log_job = job_id or "author_gc_eager"
        add_job_log(
            log_job,
            f"Soft-removed orphan author {row['name'] or aid} ({aid}) — {reason}",
            step="gc_author_soft_removed",
            data={"author_id": aid, "name": row["name"], "reason": reason, "at": now},
        )
    except Exception:
        # Audit logging is best-effort; do not fail the GC because of it.
        logger.debug("Audit log for soft-remove failed for %s", aid, exc_info=True)
    return True


def gc_author_if_orphan(
    db: sqlite3.Connection,
    author_id: str,
    *,
    reason: str,
    job_id: Optional[str] = None,
) -> bool:
    """Eager helper: soft-remove the author iff they're orphan now.

    Safe to call from any cascade trigger (paper-remove, unfollow,
    hard-delete). Returns True when the author was collected.
    """
    if not is_orphan_author(db, author_id):
        return False
    return soft_remove_author(db, author_id, reason=reason, job_id=job_id)


def gc_authors_if_orphan(
    db: sqlite3.Connection,
    author_ids: Iterable[str],
    *,
    reason: str,
    job_id: Optional[str] = None,
) -> int:
    """Cascade helper: GC every distinct author_id that's now orphan.

    Used by paper-remove triggers — iterate the paper's
    publication_authors rows, dedup, and call this once. Returns the
    number of authors actually collected.
    """
    seen: set[str] = set()
    collected = 0
    for raw in author_ids:
        aid = (raw or "").strip()
        if not aid or aid in seen:
            continue
        seen.add(aid)
        if gc_author_if_orphan(db, aid, reason=reason, job_id=job_id):
            collected += 1
    return collected


def find_authors_for_paper(db: sqlite3.Connection, paper_id: str) -> list[str]:
    """Return the `authors.id` values that match this paper's authorships.

    `publication_authors` stores the co-author's OpenAlex ID + display
    name; the local `authors` table uses an internal id (often
    `import_author_*`). The cascade triggers need the local ids so
    they can ask `is_orphan_author` about each. Match via openalex_id
    when present, falling back to display_name. Distinct-by-`a.id`.
    """
    pid = (paper_id or "").strip()
    if not pid:
        return []
    rows = db.execute(
        """
        SELECT DISTINCT a.id
        FROM publication_authors pa
        JOIN authors a ON (
                 (COALESCE(NULLIF(TRIM(pa.openalex_id), ''), '') <> ''
                  AND lower(trim(a.openalex_id)) = lower(trim(pa.openalex_id)))
              OR (COALESCE(NULLIF(TRIM(pa.display_name), ''), '') <> ''
                  AND lower(trim(a.name)) = lower(trim(pa.display_name)))
        )
        WHERE pa.paper_id = ?
        """,
        (pid,),
    ).fetchall()
    return [str(r["id"]) for r in rows if r["id"]]


def cascade_gc_for_paper(
    db: sqlite3.Connection,
    paper_id: str,
    *,
    reason: str,
    job_id: Optional[str] = None,
) -> int:
    """Eager cascade: after a paper transitions to a non-live status,
    walk its co-authors and soft-remove anyone newly orphan.

    Safe to call inside the same transaction as the paper status
    update — `is_orphan_author` reads through the open transaction so
    it sees the just-updated paper status.
    """
    affected = find_authors_for_paper(db, paper_id)
    if not affected:
        return 0
    return gc_authors_if_orphan(db, affected, reason=reason, job_id=job_id)


def garbage_collect_orphan_authors(
    db: sqlite3.Connection,
    *,
    dry_run: bool = False,
    job_id: Optional[str] = None,
) -> dict:
    """Sweep: find every still-active author that's now orphan.

    Returns a summary dict suitable for the scheduler's terminal-
    message contract:
      {success, scanned, collected, dry_run, sample, message}

    `sample` is the first 25 collected (id + name) so the Activity row
    can show *what* got collected without having to spool every entry
    through `add_job_log`. Dry-run mode skips the UPDATE but still
    populates `collected` + `sample` so the user can preview before
    pulling the trigger.
    """
    candidates = db.execute(
        """
        SELECT a.id, a.name
        FROM authors a
        LEFT JOIN followed_authors fa ON fa.author_id = a.id
        WHERE COALESCE(a.status, 'active') = 'active'
          AND fa.author_id IS NULL
        """
    ).fetchall()
    scanned = len(candidates)
    sample: list[dict] = []
    collected = 0
    for row in candidates:
        aid = (row["id"] or "").strip()
        if not aid:
            continue
        if not is_orphan_author(db, aid):
            continue
        if dry_run:
            collected += 1
            if len(sample) < 25:
                sample.append({"author_id": aid, "name": row["name"]})
            continue
        if soft_remove_author(
            db,
            aid,
            reason="manual sweep",
            job_id=job_id,
        ):
            collected += 1
            if len(sample) < 25:
                sample.append({"author_id": aid, "name": row["name"]})
    if not dry_run and db.in_transaction:
        db.commit()
    summary = {
        "success": True,
        "scanned": scanned,
        "collected": collected,
        "dry_run": bool(dry_run),
        "sample": sample,
        "message": (
            f"Author GC ({'dry-run' if dry_run else 'live'}): "
            f"{collected} collected of {scanned} candidates"
        ),
    }
    return summary
