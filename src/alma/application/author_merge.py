"""Merge multiple OpenAlex profiles for the same human into one.

Background — OpenAlex frequently splits a single researcher into several
author IDs (different name spellings, mid-career institution moves,
ORCID drift). The suggestion-rail dedup catches these at *display*
time (`tasks/lessons.md`: "Author suggestion same-human dedup"); the
needs-attention surface flags them so the user can decide. THIS module
is the actual mitigation: collapse the alt rows into a chosen primary.

What "merge" means here:
  - The primary `authors` row stays exactly as-is.
  - Each alt `authors` row gets:
      * its `publication_authors` rows reassigned to the primary's
        openalex_id (DELETE-on-collision, UPDATE the rest);
      * its `followed_authors` row deleted (the primary is the only
        followed entry going forward);
      * its `feed_monitors` row dropped via `sync_author_monitors`;
      * `status='removed'` so it stays in the table for provenance
        but disappears from suggestions / lists (D3 lifecycle);
      * a row in `author_alt_identifiers` recording that this
        openalex_id is now an alias of the primary.
  - The primary's centroid cache is invalidated so the next
    centroid recompute picks up the newly attached papers.

Why the alias table — `author_alt_identifiers`:
  - Lets `list_author_suggestions` extend its `followed_ids` filter
    to also exclude alt openalex_ids (so they never resurface).
  - Lets the dossier render "this person also has these OpenAlex
    profiles".
  - Cheap GC if the user ever wants to undo a merge (delete the
    row + revive the alt's `authors.status`).

Audited via `operation_logs` (step="author_merged") so the Activity
feed shows what was collapsed and when.
"""

from __future__ import annotations

import logging
import sqlite3
import uuid
from datetime import datetime
from typing import Iterable, Optional

logger = logging.getLogger(__name__)


def ensure_alt_identifiers_table(db: sqlite3.Connection) -> None:
    """Idempotent — runs at the start of every merge call."""
    db.execute(
        """
        CREATE TABLE IF NOT EXISTS author_alt_identifiers (
            id TEXT PRIMARY KEY,
            primary_author_id TEXT NOT NULL,
            alt_openalex_id TEXT NOT NULL,
            alt_author_id TEXT,
            source TEXT NOT NULL DEFAULT 'manual_merge',
            created_at TEXT NOT NULL,
            UNIQUE (primary_author_id, alt_openalex_id)
        )
        """
    )
    db.execute(
        "CREATE INDEX IF NOT EXISTS idx_alt_identifiers_alt_oid "
        "ON author_alt_identifiers(lower(alt_openalex_id))"
    )
    # Conflicts surface from the field-union step of merge — when both
    # rows hold a different non-null value for a hard identifier
    # (orcid, scholar_id, semantic_scholar_id), the merge keeps the
    # primary's value and stores the alt's here so the user can
    # resolve via the needs-attention surface.
    db.execute(
        """
        CREATE TABLE IF NOT EXISTS author_merge_conflicts (
            id TEXT PRIMARY KEY,
            primary_author_id TEXT NOT NULL,
            alt_openalex_id TEXT NOT NULL,
            field TEXT NOT NULL,
            primary_value TEXT,
            alt_value TEXT,
            status TEXT NOT NULL DEFAULT 'unresolved',
            created_at TEXT NOT NULL,
            resolved_at TEXT,
            UNIQUE (primary_author_id, alt_openalex_id, field)
        )
        """
    )


# Profile fields that get *unioned* into the primary during merge.
# `_kind` describes the union strategy:
#   - "fill"        : copy from alt only if primary's value is empty
#   - "max"         : take the larger of the two (numeric)
#   - "union_json"  : both decoded as JSON lists, dedup'd, primary's
#                     order preserved + alt-only items appended
# Hard identifiers (orcid / scholar_id / semantic_scholar_id) get a
# different treatment in `_detect_conflicts` — they raise a flag if
# both sides hold different non-null values.
_MERGE_PROFILE_FIELDS: tuple[tuple[str, str], ...] = (
    ("affiliation", "fill"),
    ("url_picture", "fill"),
    ("email_domain", "fill"),
    ("citedby", "max"),
    ("h_index", "max"),
    ("works_count", "max"),
    ("interests", "union_json"),
    ("institutions", "union_json"),
    ("orcid", "fill"),
    ("scholar_id", "fill"),
    ("semantic_scholar_id", "fill"),
)

# Hard-identifier fields that flag a conflict when both sides are
# non-null AND the values differ. The merge still proceeds (primary's
# value wins); the conflict gets logged for review.
_HARD_IDENTIFIER_FIELDS: tuple[str, ...] = (
    "orcid",
    "scholar_id",
    "semantic_scholar_id",
)


def _union_json_list(primary_raw: object, alt_raw: object) -> Optional[str]:
    """Merge two JSON-list-shaped strings, preserving primary's order."""
    import json as _json

    def _parse(raw: object) -> list:
        if not raw:
            return []
        try:
            value = _json.loads(str(raw))
        except (ValueError, TypeError):
            return []
        return value if isinstance(value, list) else []

    primary_list = _parse(primary_raw)
    alt_list = _parse(alt_raw)
    if not alt_list:
        # Nothing to add — return None so the caller can skip the UPDATE.
        return None
    seen: set[str] = set()
    merged: list = []
    for item in primary_list + alt_list:
        key = _json.dumps(item, sort_keys=True) if not isinstance(item, str) else item
        if key in seen:
            continue
        seen.add(key)
        merged.append(item)
    if merged == primary_list:
        return None  # No new items, skip update.
    return _json.dumps(merged)


def _merge_profile_fields(
    db: sqlite3.Connection,
    primary_row: sqlite3.Row,
    alt_row: sqlite3.Row,
) -> dict[str, object]:
    """Compute + apply the field-level union of alt → primary.

    Returns a dict of `{field: new_value}` for every field actually
    updated. The primary's row is UPDATEd in-place; caller commits
    via the surrounding transaction.
    """
    updates: dict[str, object] = {}
    for field, kind in _MERGE_PROFILE_FIELDS:
        primary_val = primary_row[field] if field in primary_row.keys() else None
        alt_val = alt_row[field] if field in alt_row.keys() else None
        if alt_val in (None, "", 0):
            continue
        if kind == "fill":
            if primary_val in (None, "", 0):
                updates[field] = alt_val
        elif kind == "max":
            try:
                pv = int(primary_val or 0)
                av = int(alt_val or 0)
                if av > pv:
                    updates[field] = av
            except (TypeError, ValueError):
                continue
        elif kind == "union_json":
            unioned = _union_json_list(primary_val, alt_val)
            if unioned is not None:
                updates[field] = unioned
    if updates:
        # Hard identifiers (orcid / scholar_id / semantic_scholar_id) sit
        # behind partial UNIQUE indexes on the `authors` table. Copying
        # one from alt to primary while alt still holds it would collide
        # (the indexes don't care about `status='removed'`). Clear those
        # values on the alt FIRST so the primary UPDATE has a free slot.
        hard_id_clears = [k for k in updates if k in _HARD_IDENTIFIER_FIELDS]
        if hard_id_clears:
            clear_clause = ", ".join(f"{k} = NULL" for k in hard_id_clears)
            db.execute(
                f"UPDATE authors SET {clear_clause} WHERE id = ?",
                (alt_row["id"],),
            )
        set_clause = ", ".join(f"{k} = ?" for k in updates)
        db.execute(
            f"UPDATE authors SET {set_clause} WHERE id = ?",
            list(updates.values()) + [primary_row["id"]],
        )
    return updates


def _detect_conflicts(
    db: sqlite3.Connection,
    primary_row: sqlite3.Row,
    alt_row: sqlite3.Row,
    alt_openalex_id: str,
) -> list[dict]:
    """Persist + return any hard-identifier conflicts.

    Conflict = both sides hold a non-null value AND they differ
    (case-insensitive for string ids). Primary's value is always the
    one kept by the merge — the conflict row records the alt's value
    so the user can later resolve via the needs-attention surface.
    """
    out: list[dict] = []
    now = datetime.utcnow().isoformat()
    for field in _HARD_IDENTIFIER_FIELDS:
        if field not in primary_row.keys() or field not in alt_row.keys():
            continue
        primary_val = str(primary_row[field] or "").strip()
        alt_val = str(alt_row[field] or "").strip()
        if not primary_val or not alt_val:
            continue
        if primary_val.lower() == alt_val.lower():
            continue
        cur = db.execute(
            """
            INSERT OR IGNORE INTO author_merge_conflicts
                (id, primary_author_id, alt_openalex_id, field,
                 primary_value, alt_value, status, created_at)
            VALUES (?, ?, ?, ?, ?, ?, 'unresolved', ?)
            """,
            (
                uuid.uuid4().hex,
                primary_row["id"],
                alt_openalex_id,
                field,
                primary_val,
                alt_val,
                now,
            ),
        )
        if cur.rowcount:
            out.append(
                {
                    "field": field,
                    "primary_value": primary_val,
                    "alt_value": alt_val,
                }
            )
    return out


def list_unresolved_conflicts(db: sqlite3.Connection) -> list[dict]:
    """All conflicts that haven't been resolved or dismissed yet.

    Joined to `authors` so the needs-attention surface can render the
    primary's display name without a second round-trip.
    """
    ensure_alt_identifiers_table(db)
    try:
        rows = db.execute(
            """
            SELECT
                c.id,
                c.primary_author_id,
                c.alt_openalex_id,
                c.field,
                c.primary_value,
                c.alt_value,
                c.created_at,
                a.name AS primary_name,
                a.openalex_id AS primary_openalex_id
            FROM author_merge_conflicts c
            LEFT JOIN authors a ON a.id = c.primary_author_id
            WHERE c.status = 'unresolved'
            ORDER BY c.created_at DESC
            """
        ).fetchall()
    except sqlite3.OperationalError:
        return []
    return [dict(r) for r in rows]


def list_alt_openalex_ids(
    db: sqlite3.Connection, primary_author_id: str
) -> list[str]:
    """All alt OpenAlex IDs already merged into this primary."""
    ensure_alt_identifiers_table(db)
    rows = db.execute(
        "SELECT alt_openalex_id FROM author_alt_identifiers "
        "WHERE primary_author_id = ? ORDER BY created_at ASC",
        (primary_author_id,),
    ).fetchall()
    return [str(r["alt_openalex_id"]) for r in rows if r["alt_openalex_id"]]


def list_all_alt_openalex_ids(db: sqlite3.Connection) -> set[str]:
    """Every alt OpenAlex ID across all merges, lowercased.

    Used by `list_author_suggestions` to extend its `followed_ids`
    filter so already-merged alts never resurface in the rail.
    """
    ensure_alt_identifiers_table(db)
    try:
        rows = db.execute(
            "SELECT alt_openalex_id FROM author_alt_identifiers"
        ).fetchall()
    except sqlite3.OperationalError:
        return set()
    return {
        str(r["alt_openalex_id"]).strip().lower()
        for r in rows
        if r["alt_openalex_id"]
    }


def merge_author_profiles(
    db: sqlite3.Connection,
    primary_author_id: str,
    alt_author_ids: Iterable[str],
    *,
    job_id: Optional[str] = None,
) -> dict:
    """Collapse `alt_author_ids` into `primary_author_id`.

    Returns a summary dict suitable for the API response:
      {
        "primary_author_id": str,
        "primary_openalex_id": str,
        "alts_processed": int,
        "alts_skipped": int,
        "papers_reassigned": int,
        "papers_dropped_as_dup": int,
        "alt_openalex_ids": [str, ...],   # what got recorded
        "alt_author_ids": [str, ...],     # the source rows we soft-removed
      }

    Raises:
      ValueError if the primary doesn't exist or has no openalex_id.
    """
    ensure_alt_identifiers_table(db)

    primary_id = (primary_author_id or "").strip()
    if not primary_id:
        raise ValueError("primary_author_id is required")
    primary_row = db.execute(
        "SELECT id, name, openalex_id FROM authors WHERE id = ?", (primary_id,),
    ).fetchone()
    if primary_row is None:
        raise ValueError(f"primary author {primary_id} not found")
    primary_oid = (primary_row["openalex_id"] or "").strip()
    if not primary_oid:
        raise ValueError(
            f"primary author {primary_id} has no OpenAlex ID — "
            "set one via /authors/{id}/identifiers before merging"
        )

    # De-dup + drop the primary itself if it slipped into the alt list.
    seen: set[str] = set()
    alt_ids_clean: list[str] = []
    for raw in alt_author_ids:
        aid = (raw or "").strip()
        if not aid or aid == primary_id or aid in seen:
            continue
        seen.add(aid)
        alt_ids_clean.append(aid)

    summary = {
        "primary_author_id": primary_id,
        "primary_openalex_id": primary_oid,
        "alts_processed": 0,
        "alts_skipped": 0,
        "papers_reassigned": 0,
        "papers_dropped_as_dup": 0,
        "alt_openalex_ids": [],
        "alt_author_ids": [],
        "fields_unioned": {},
        "conflicts": [],
    }

    now = datetime.utcnow().isoformat()
    for alt_id in alt_ids_clean:
        # Pull the FULL alt row so the field-union step has every
        # column to draw from. SELECT * keeps this resilient to
        # future schema additions — `_merge_profile_fields` filters
        # to the known field set.
        alt_row = db.execute(
            "SELECT * FROM authors WHERE id = ?", (alt_id,),
        ).fetchone()
        if alt_row is None:
            summary["alts_skipped"] += 1
            continue
        alt_oid = (alt_row["openalex_id"] or "").strip()
        if not alt_oid or alt_oid.lower() == primary_oid.lower():
            # Nothing to reassign — but still soft-remove the alt
            # row so it's out of the followed list.
            _drop_follow_and_soft_remove(db, alt_id)
            summary["alts_skipped"] += 1
            continue

        # 1. Drop alt's `publication_authors` rows that would collide
        #    with primary's existing rows on the same paper. Without
        #    this, the UPDATE below would hit the (paper_id, openalex_id)
        #    UNIQUE constraint.
        drop_cur = db.execute(
            """
            DELETE FROM publication_authors
            WHERE openalex_id = ?
              AND paper_id IN (
                  SELECT paper_id FROM publication_authors
                  WHERE openalex_id = ?
              )
            """,
            (alt_oid, primary_oid),
        )
        summary["papers_dropped_as_dup"] += drop_cur.rowcount or 0

        # 2. Reassign the rest of the alt's papers to the primary.
        upd_cur = db.execute(
            "UPDATE publication_authors SET openalex_id = ? WHERE openalex_id = ?",
            (primary_oid, alt_oid),
        )
        summary["papers_reassigned"] += upd_cur.rowcount or 0

        # 3. Field-union: copy missing profile fields from alt to
        #    primary (affiliation, h_index, citedby, works_count,
        #    interests, institutions, identifiers...). Conflicts on
        #    hard identifiers (orcid / scholar_id /
        #    semantic_scholar_id) get persisted to
        #    `author_merge_conflicts` for needs-attention surfacing
        #    — the merge still proceeds (primary's value wins).
        primary_row_full = db.execute(
            "SELECT * FROM authors WHERE id = ?", (primary_id,),
        ).fetchone()
        if primary_row_full is not None:
            updates = _merge_profile_fields(db, primary_row_full, alt_row)
            if updates:
                summary["fields_unioned"][alt_oid] = list(updates.keys())
            conflicts = _detect_conflicts(db, primary_row_full, alt_row, alt_oid)
            for c in conflicts:
                summary["conflicts"].append({**c, "alt_openalex_id": alt_oid})

        # 4. Drop the alt from followed_authors and soft-remove the row.
        _drop_follow_and_soft_remove(db, alt_id)

        # 5. Record the alias so the rail / dossier knows.
        db.execute(
            """
            INSERT OR IGNORE INTO author_alt_identifiers
                (id, primary_author_id, alt_openalex_id, alt_author_id, source, created_at)
            VALUES (?, ?, ?, ?, 'manual_merge', ?)
            """,
            (uuid.uuid4().hex, primary_id, alt_oid, alt_id, now),
        )

        # 5. Audit log entry — best effort.
        try:
            from alma.api.scheduler import add_job_log

            add_job_log(
                job_id or "author_merge",
                f"Merged author {alt_row['name'] or alt_id} ({alt_oid}) "
                f"→ {primary_row['name'] or primary_id} ({primary_oid})",
                step="author_merged",
                data={
                    "primary_author_id": primary_id,
                    "primary_openalex_id": primary_oid,
                    "alt_author_id": alt_id,
                    "alt_openalex_id": alt_oid,
                    "papers_reassigned": upd_cur.rowcount or 0,
                    "papers_dropped_as_dup": drop_cur.rowcount or 0,
                },
            )
        except Exception:
            logger.debug("Audit log failed for merge of %s", alt_id, exc_info=True)

        summary["alts_processed"] += 1
        summary["alt_openalex_ids"].append(alt_oid)
        summary["alt_author_ids"].append(alt_id)

    # Mirror the followed_authors deletes into feed_monitors in one
    # sync at the end (cheaper than calling per-alt).
    if summary["alts_processed"] > 0:
        from alma.application.feed_monitors import sync_author_monitors

        sync_author_monitors(db)

        # Invalidate the primary's centroid cache so the next centroid
        # recompute picks up the newly attached papers. Cheapest:
        # delete the row; the recompute path will rebuild on demand.
        try:
            db.execute(
                "DELETE FROM author_centroids WHERE author_id = ?", (primary_id,),
            )
        except sqlite3.OperationalError:
            # Table may not exist on older schemas — non-fatal.
            pass
        try:
            from alma.services.author_hydrate import enqueue_pending_author_hydration

            enqueue_pending_author_hydration(
                db,
                primary_id,
                priority="high",
                reason="author_merge",
            )
        except Exception as exc:
            logger.debug("author hydration enqueue skipped after merge %s: %s", primary_id, exc)

    db.commit()
    return summary


def discover_aliases_via_orcid(
    primary_openalex_id: str,
    *,
    mailto: Optional[str] = None,
    limit: int = 10,
) -> dict:
    """Look up every OpenAlex author profile sharing the primary's ORCID.

    Same human, multiple OpenAlex profiles is the failure mode this
    addresses. ORCID is the only authoritative human-level ID OpenAlex
    exposes — when two profiles list the same ORCID, they're the same
    researcher with very high confidence (false-positive rate is
    essentially nil because ORCIDs are issued per-person and verified
    through a third party).

    Returns:
        {
          "primary_openalex_id": str,
          "orcid": str | None,
          "aliases": [
              {"openalex_id": str, "display_name": str,
               "institution": str, "works_count": int}, ...
          ],
        }

    Aliases EXCLUDE the primary id itself. Empty list when:
      - The primary has no ORCID on OpenAlex.
      - OpenAlex is unreachable (caller should treat as "no aliases
        discovered yet"; the helper does not raise).
      - The ORCID is uniquely held by the primary.
    """
    from alma.openalex.client import _session
    from alma.openalex.client import _normalize_openalex_author_id as _norm_oaid

    primary_oid = (primary_openalex_id or "").strip()
    if not primary_oid:
        return {"primary_openalex_id": "", "orcid": None, "aliases": []}
    primary_oid_norm = _norm_oaid(primary_oid) or primary_oid

    try:
        session = _session(mailto)
        # Step 1 — fetch the primary's ORCID.
        primary_resp = session.get(
            f"https://api.openalex.org/authors/{primary_oid_norm}",
            params={"select": "id,display_name,orcid"},
            timeout=20,
        )
        if primary_resp.status_code != 200:
            return {
                "primary_openalex_id": primary_oid_norm,
                "orcid": None,
                "aliases": [],
            }
        primary_data = primary_resp.json() or {}
        orcid_url = (primary_data.get("orcid") or "").strip()
        if not orcid_url:
            return {
                "primary_openalex_id": primary_oid_norm,
                "orcid": None,
                "aliases": [],
            }
        # OpenAlex returns the ORCID as a full URL — strip to bare id.
        orcid_bare = orcid_url
        for prefix in ("https://orcid.org/", "http://orcid.org/", "orcid.org/"):
            if orcid_bare.lower().startswith(prefix):
                orcid_bare = orcid_bare[len(prefix):]
                break

        # Step 2 — query all OpenAlex authors with the same ORCID.
        per_page = max(1, min(int(limit or 10), 25))
        resp = session.get(
            "https://api.openalex.org/authors",
            params={
                "filter": f"orcid:{orcid_bare}",
                "per-page": per_page,
                "select": "id,display_name,last_known_institutions,works_count",
            },
            timeout=20,
        )
        if resp.status_code != 200:
            return {
                "primary_openalex_id": primary_oid_norm,
                "orcid": orcid_bare,
                "aliases": [],
            }
        rows = (resp.json() or {}).get("results") or []
    except Exception as exc:
        logger.warning(
            "discover_aliases_via_orcid failed for %s: %s", primary_oid_norm, exc,
        )
        return {
            "primary_openalex_id": primary_oid_norm,
            "orcid": None,
            "aliases": [],
        }

    aliases: list[dict] = []
    for row in rows:
        oid_raw = str(row.get("id") or "").strip()
        if not oid_raw:
            continue
        oid = _norm_oaid(oid_raw) or oid_raw
        if oid.lower() == primary_oid_norm.lower():
            continue
        # Last known institution may come back as a list (new API) or
        # absent. Pick the first display_name available.
        institution = ""
        lki = row.get("last_known_institutions") or []
        if isinstance(lki, list) and lki and isinstance(lki[0], dict):
            institution = str(lki[0].get("display_name") or "").strip()
        aliases.append(
            {
                "openalex_id": oid,
                "display_name": str(row.get("display_name") or "").strip(),
                "institution": institution,
                "works_count": int(row.get("works_count") or 0),
            }
        )
    return {
        "primary_openalex_id": primary_oid_norm,
        "orcid": orcid_bare,
        "aliases": aliases,
    }


def record_orcid_aliases(
    db: sqlite3.Connection,
    primary_author_id: str,
    *,
    mailto: Optional[str] = None,
) -> dict:
    """Preventive ORCID-based alias recording.

    Called fire-and-forget from `apply_follow_state(followed=True)`
    so that as soon as the user follows an author, every other
    OpenAlex profile sharing the same ORCID is recorded in
    `author_alt_identifiers`. The suggestion rail's `followed_ids`
    UNION (see `list_author_suggestions`) then filters those alts
    out automatically — the user never sees the duplicates as
    fresh suggestions.

    Does NOT auto-merge. Two reasons:
      1. Merging is destructive (papers reattach, alt rows
         soft-remove); user consent matters.
      2. The user might genuinely want both profiles followed
         independently for some reason — recording the alias
         only suppresses suggestions, not the follow itself.

    Returns the same shape as `discover_aliases_via_orcid` plus
    `recorded` (count of new alias rows actually inserted; existing
    rows hit the UNIQUE constraint and are skipped).
    """
    ensure_alt_identifiers_table(db)

    primary_id = (primary_author_id or "").strip()
    if not primary_id:
        return {"primary_author_id": "", "orcid": None, "aliases": [], "recorded": 0}

    row = db.execute(
        "SELECT openalex_id FROM authors WHERE id = ?", (primary_id,),
    ).fetchone()
    if row is None:
        return {"primary_author_id": primary_id, "orcid": None, "aliases": [], "recorded": 0}
    primary_oid = (row["openalex_id"] or "").strip()
    if not primary_oid:
        return {
            "primary_author_id": primary_id,
            "orcid": None,
            "aliases": [],
            "recorded": 0,
        }

    discovery = discover_aliases_via_orcid(primary_oid, mailto=mailto)
    aliases = discovery.get("aliases") or []
    recorded = 0
    now = datetime.utcnow().isoformat()
    for alias in aliases:
        alt_oid = str(alias.get("openalex_id") or "").strip()
        if not alt_oid:
            continue
        # Try to find a local `authors` row with this openalex_id —
        # gives us a useful back-pointer for later merge UX. None is
        # fine; the row is keyed by primary + alt_openalex_id.
        local_row = db.execute(
            "SELECT id FROM authors WHERE lower(openalex_id) = lower(?) LIMIT 1",
            (alt_oid,),
        ).fetchone()
        local_alt_id = str(local_row["id"]) if local_row else None
        cur = db.execute(
            """
            INSERT OR IGNORE INTO author_alt_identifiers
                (id, primary_author_id, alt_openalex_id, alt_author_id, source, created_at)
            VALUES (?, ?, ?, ?, 'orcid_discovery', ?)
            """,
            (uuid.uuid4().hex, primary_id, alt_oid, local_alt_id, now),
        )
        recorded += cur.rowcount or 0
    if recorded:
        db.commit()
    discovery["primary_author_id"] = primary_id
    discovery["recorded"] = recorded
    return discovery


def dedup_followed_authors_by_orcid(
    db: sqlite3.Connection,
    *,
    mailto: Optional[str] = None,
    sleep_between_calls: float = 0.05,
    job_id: Optional[str] = None,
) -> dict:
    """Sweep every followed author for ORCID-based split profiles.

    For each followed author with an OpenAlex ID:
      1. Call OpenAlex `/authors/{id}` → `/authors?filter=orcid:X` to
         discover every other profile sharing the same ORCID.
      2. For each alias openalex_id:
         a. If another currently-followed author already holds that
            openalex_id, AUTO-MERGE the two. Primary = the one with
            more works_count (richer profile). Tie-break: lex order
            on author_id.
         b. Else, record the alias in `author_alt_identifiers` so
            the suggestion rail filters it out.

    Returns a summary suitable for the Activity envelope:
      {
        "success": True,
        "scanned": int,
        "merged": int,
        "aliases_recorded": int,
        "errors": int,
        "skipped_no_orcid": int,
        "sample": [
            {"action": "merge", "primary_id": ..., "primary_oid": ...,
             "alt_id": ..., "alt_oid": ..., "papers_reassigned": ...},
            {"action": "alias", "primary_id": ..., "alt_oid": ...},
        ][:25],
        "message": str,
      }

    The destructive step (`merge_author_profiles`) is run with the
    SAME `db` connection so the whole sweep stays in the operation's
    audit trail. Caller is responsible for committing — we commit
    inside the merge helper, so by sweep-end every effect is
    durable.
    """
    ensure_alt_identifiers_table(db)

    # Build a `by_oid` lookup of every ACTIVE local author with an
    # openalex_id — followed AND background. The sweep iterates only
    # over followed authors (the outer loop, below) but consumes the
    # full map so that an ORCID-discovered alias which exists locally
    # as a non-followed background row (e.g. a co-author from a saved
    # paper) gets merged into the followed primary too. Without this,
    # the background author's `publication_authors` rows would stay
    # attributed to a different openalex_id and the followed primary's
    # centroid would miss those papers.
    all_rows = db.execute(
        """
        SELECT
            a.id AS author_id,
            a.openalex_id,
            a.name,
            COALESCE(a.works_count, 0) AS works_count,
            CASE WHEN fa.author_id IS NOT NULL THEN 1 ELSE 0 END AS is_followed
        FROM authors a
        LEFT JOIN followed_authors fa ON fa.author_id = a.id
        WHERE COALESCE(a.openalex_id, '') <> ''
          AND COALESCE(a.status, 'active') = 'active'
        """
    ).fetchall()

    by_oid: dict[str, dict] = {}
    followed_rows: list[dict] = []
    for r in all_rows:
        oid = str(r["openalex_id"]).strip().lower()
        if not oid:
            continue
        entry = {
            "author_id": str(r["author_id"]),
            "openalex_id": str(r["openalex_id"]),
            "name": str(r["name"] or ""),
            "works_count": int(r["works_count"] or 0),
            "is_followed": bool(r["is_followed"]),
        }
        by_oid[oid] = entry
        if entry["is_followed"]:
            followed_rows.append(entry)

    summary = {
        "success": True,
        "scanned": 0,
        "merged": 0,
        "aliases_recorded": 0,
        "errors": 0,
        "skipped_no_orcid": 0,
        "sample": [],
        "message": "",
    }

    # Iterate followed authors only (we never want to "discover
    # aliases" starting from a random co-author background row — that
    # would be expensive and surface clusters the user never asked
    # to track). The merge cascade still folds in non-followed locals
    # found via the ORCID lookup.
    initial_targets = list(followed_rows)
    import time

    for target in initial_targets:
        # Short-circuit if the target was already merged AWAY in a
        # previous iteration of this sweep.
        if target["openalex_id"].lower() not in by_oid:
            continue

        summary["scanned"] += 1
        try:
            discovery = discover_aliases_via_orcid(
                target["openalex_id"], mailto=mailto,
            )
        except Exception as exc:  # pragma: no cover — best-effort
            logger.warning("ORCID discovery failed for %s: %s", target["author_id"], exc)
            summary["errors"] += 1
            continue
        if not discovery.get("orcid"):
            summary["skipped_no_orcid"] += 1
            continue

        for alias in discovery.get("aliases") or []:
            alt_oid = str(alias.get("openalex_id") or "").strip()
            if not alt_oid:
                continue
            alt_oid_key = alt_oid.lower()

            existing_local = by_oid.get(alt_oid_key)
            if existing_local and existing_local["author_id"] != target["author_id"]:
                # Same human, multiple local rows — auto-merge.
                # Primary picker:
                #   * If only one is followed, the followed one wins
                #     unconditionally — we never subordinate a
                #     deliberately-followed author to a random
                #     background co-author row.
                #   * Else (both followed, or both background) →
                #     richer profile (works_count) wins; tie-break
                #     by lex order on author_id so results are stable.
                a = target
                b = existing_local
                if a["is_followed"] and not b["is_followed"]:
                    primary, alt = a, b
                elif b["is_followed"] and not a["is_followed"]:
                    primary, alt = b, a
                elif (b["works_count"], b["author_id"]) > (a["works_count"], a["author_id"]):
                    primary, alt = b, a
                else:
                    primary, alt = a, b
                try:
                    merge_result = merge_author_profiles(
                        db, primary["author_id"], [alt["author_id"]],
                        job_id=job_id,
                    )
                except Exception as exc:  # pragma: no cover
                    logger.warning(
                        "Auto-merge failed for %s ← %s: %s",
                        primary["author_id"], alt["author_id"], exc,
                    )
                    summary["errors"] += 1
                    continue
                summary["merged"] += 1
                if len(summary["sample"]) < 25:
                    summary["sample"].append(
                        {
                            "action": "merge",
                            "primary_id": primary["author_id"],
                            "primary_oid": primary["openalex_id"],
                            "primary_name": primary["name"],
                            "alt_id": alt["author_id"],
                            "alt_oid": alt["openalex_id"],
                            "papers_reassigned": int(merge_result.get("papers_reassigned") or 0),
                            "papers_dropped_as_dup": int(merge_result.get("papers_dropped_as_dup") or 0),
                        }
                    )
                # Drop the merged-away author from the live map so
                # the next outer-loop iteration skips it cleanly.
                by_oid.pop(alt["openalex_id"].lower(), None)
                # If the *target* was the one that got merged away,
                # bail out of the alias loop for this iteration.
                if alt["author_id"] == target["author_id"]:
                    break
            else:
                # Not currently followed — record as a known alias of
                # the target so the suggestion rail filters it out
                # forever (UNIQUE constraint makes this idempotent).
                local_row = db.execute(
                    "SELECT id FROM authors WHERE lower(openalex_id) = lower(?) LIMIT 1",
                    (alt_oid,),
                ).fetchone()
                local_alt_id = str(local_row["id"]) if local_row else None
                cur = db.execute(
                    """
                    INSERT OR IGNORE INTO author_alt_identifiers
                        (id, primary_author_id, alt_openalex_id, alt_author_id, source, created_at)
                    VALUES (?, ?, ?, ?, 'orcid_sweep', ?)
                    """,
                    (
                        uuid.uuid4().hex,
                        target["author_id"],
                        alt_oid,
                        local_alt_id,
                        datetime.utcnow().isoformat(),
                    ),
                )
                if cur.rowcount:
                    summary["aliases_recorded"] += 1
                    if len(summary["sample"]) < 25:
                        summary["sample"].append(
                            {
                                "action": "alias",
                                "primary_id": target["author_id"],
                                "primary_oid": target["openalex_id"],
                                "primary_name": target["name"],
                                "alt_oid": alt_oid,
                            }
                        )

        if sleep_between_calls > 0:
            time.sleep(sleep_between_calls)

    db.commit()
    summary["message"] = (
        f"Author dedup sweep: {summary['merged']} merged, "
        f"{summary['aliases_recorded']} aliases recorded across "
        f"{summary['scanned']} authors "
        f"({summary['skipped_no_orcid']} had no ORCID, "
        f"{summary['errors']} errored)"
    )
    return summary


def _drop_follow_and_soft_remove(db: sqlite3.Connection, author_id: str) -> None:
    """DELETE from followed_authors + flip authors.status='removed'.

    Idempotent. Used by the merge cascade — the alt's identity is
    preserved (row stays in `authors`) but it stops being followed
    and is filtered out of suggestion / list endpoints.
    """
    db.execute("DELETE FROM followed_authors WHERE author_id = ?", (author_id,))
    db.execute(
        "UPDATE authors SET status = 'removed', author_type = 'background' "
        "WHERE id = ?",
        (author_id,),
    )
