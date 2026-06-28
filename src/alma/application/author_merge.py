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
from typing import Iterable, Mapping, Optional

from alma.core.author_names import (
    affiliations_corroborate,
    name_match_confidence,
    parse_person_name,
)
from alma.core.db_write import run_after_gate_release, write_section
from alma.core.utils import normalize_orcid

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

# Fields the manual merge dialog may ask the user to resolve. `openalex_id`
# is intentionally excluded: the primary OpenAlex ID stays canonical and the
# alt OpenAlex ID is recorded in `author_alt_identifiers`.
_MANUAL_CHOICE_FIELDS: frozenset[str] = frozenset(
    {
        "name",
        "affiliation",
        "email_domain",
        "url_picture",
        "citedby",
        "h_index",
        "works_count",
        "interests",
        "institutions",
        "orcid",
        "scholar_id",
        "semantic_scholar_id",
    }
)


def record_author_alias(
    db: sqlite3.Connection,
    primary_author_id: str,
    alt_openalex_id: str,
    *,
    alt_author_id: Optional[str] = None,
    source: str,
) -> bool:
    """Record one ``author_alt_identifiers`` row; return True if newly inserted.

    The single writer for author aliases — merge (row + row-less paths),
    ORCID-discovery hydration, and the ORCID dedup sweep all funnel here so the
    column order, id/timestamp generation, and the idempotent
    ``INSERT OR IGNORE`` (UNIQUE on ``primary_author_id, alt_openalex_id``) live
    in ONE place. Pass the per-call provenance via ``source`` (e.g.
    ``manual_merge`` / ``orcid_discovery`` / ``orcid_sweep``). Caller owns the
    transaction — this does not commit.
    """
    cur = db.execute(
        """
        INSERT OR IGNORE INTO author_alt_identifiers
            (id, primary_author_id, alt_openalex_id, alt_author_id, source, created_at)
        VALUES (?, ?, ?, ?, ?, ?)
        """,
        (
            uuid.uuid4().hex,
            primary_author_id,
            alt_openalex_id,
            alt_author_id,
            source,
            datetime.utcnow().isoformat(),
        ),
    )
    return bool(cur.rowcount)


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
    *,
    skip_fields: set[str] | None = None,
) -> dict[str, object]:
    """Compute + apply the field-level union of alt → primary.

    Returns a dict of `{field: new_value}` for every field actually
    updated. The primary's row is UPDATEd in-place; caller commits
    via the surrounding transaction.
    """
    updates: dict[str, object] = {}
    skipped = skip_fields or set()
    for field, kind in _MERGE_PROFILE_FIELDS:
        if field in skipped:
            continue
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


def _apply_manual_field_choices(
    db: sqlite3.Connection,
    primary_row: sqlite3.Row,
    alt_row: sqlite3.Row,
    choices: Mapping[str, str] | None,
) -> dict[str, object]:
    """Apply explicit user field choices from the merge-confirm dialog.

    `choices` maps author profile field -> side (`primary` or `alt`). Choosing
    `primary` is represented by no database update; choosing `alt` overwrites
    the primary field with the alt value, including blank/null when the user
    deliberately selected the blank side in the discrepancy dialog.
    """
    if not choices:
        return {}

    updates: dict[str, object] = {}
    primary_keys = set(primary_row.keys())
    alt_keys = set(alt_row.keys())
    for field, side in choices.items():
        if field not in _MANUAL_CHOICE_FIELDS:
            continue
        if side != "alt":
            continue
        if field not in primary_keys or field not in alt_keys:
            continue
        primary_val = primary_row[field]
        alt_val = alt_row[field]
        if primary_val == alt_val:
            continue
        updates[field] = alt_val

    if not updates:
        return {}

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
    *,
    skip_fields: set[str] | None = None,
) -> list[dict]:
    """Persist + return any hard-identifier conflicts.

    Conflict = both sides hold a non-null value AND they differ
    (case-insensitive for string ids). Primary's value is always the
    one kept by the merge — the conflict row records the alt's value
    so the user can later resolve via the needs-attention surface.
    """
    out: list[dict] = []
    now = datetime.utcnow().isoformat()
    skipped = skip_fields or set()
    for field in _HARD_IDENTIFIER_FIELDS:
        if field in skipped:
            continue
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
    alt_openalex_ids: Iterable[str] | None = None,
    field_choices: Mapping[str, Mapping[str, str]] | None = None,
    job_id: Optional[str] = None,
) -> dict:
    """Collapse `alt_author_ids` into `primary_author_id`.

    ``alt_openalex_ids`` absorbs identities that have NO local `authors`
    row (e.g. a suggestion-rail candidate the user recognises as a
    duplicate of someone already curated): their `publication_authors`
    rows reattach to the primary and the id is recorded as an alias —
    no throwaway row is created. An id in this list that turns out to
    HAVE a local row is routed through the normal row path instead.

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

    # Resolve row-less OpenAlex identities. Ids that actually have a local
    # row are promoted into the normal row path (so soft-remove / follow
    # cleanup / field union still happen); the rest are absorbed below as
    # pure aliases.
    from alma.openalex.client import _normalize_openalex_author_id as _norm_oaid

    rowless_oids: list[str] = []
    seen_oids: set[str] = set()
    for raw in alt_openalex_ids or []:
        oid = _norm_oaid((raw or "").strip()) or (raw or "").strip()
        if not oid or oid.lower() == primary_oid.lower() or oid.lower() in seen_oids:
            continue
        seen_oids.add(oid.lower())
        row = db.execute(
            "SELECT id FROM authors WHERE lower(openalex_id) = lower(?) LIMIT 1",
            (oid,),
        ).fetchone()
        if row is not None:
            rid = str(row["id"] if isinstance(row, sqlite3.Row) else row[0])
            if rid != primary_id and rid not in seen:
                seen.add(rid)
                alt_ids_clean.append(rid)
            continue
        rowless_oids.append(oid)

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

    choices_by_alt = field_choices or {}
    now = datetime.utcnow().isoformat()
    # Audit-log payloads collected during the loop and flushed AFTER commit
    # (each add_job_log opens its own connection — keep it out of our open
    # write transaction). Tuples of (message, data).
    pending_audit_logs: list[tuple[str, dict]] = []
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
            alt_choices = dict(
                choices_by_alt.get(alt_id)
                or choices_by_alt.get(alt_oid)
                or choices_by_alt.get(alt_oid.lower())
                or {}
            )
            chosen_fields = {
                field
                for field, side in alt_choices.items()
                if field in _MANUAL_CHOICE_FIELDS and side in {"primary", "alt"}
            }
            manual_updates = _apply_manual_field_choices(
                db,
                primary_row_full,
                alt_row,
                alt_choices,
            )
            if manual_updates:
                summary["fields_unioned"][alt_oid] = list(manual_updates.keys())
            primary_after_choices = db.execute(
                "SELECT * FROM authors WHERE id = ?", (primary_id,),
            ).fetchone()
            updates = _merge_profile_fields(
                db,
                primary_after_choices or primary_row_full,
                alt_row,
                skip_fields=chosen_fields,
            )
            if updates:
                summary["fields_unioned"][alt_oid] = sorted(
                    set(summary["fields_unioned"].get(alt_oid, [])) | set(updates.keys())
                )
            conflict_base = db.execute(
                "SELECT * FROM authors WHERE id = ?", (primary_id,),
            ).fetchone()
            conflicts = _detect_conflicts(
                db,
                conflict_base or primary_after_choices or primary_row_full,
                alt_row,
                alt_oid,
                skip_fields=chosen_fields,
            )
            for c in conflicts:
                summary["conflicts"].append({**c, "alt_openalex_id": alt_oid})

        # 4. Drop the alt from followed_authors and soft-remove the row.
        _drop_follow_and_soft_remove(db, alt_id)

        # 5. Record the alias so the rail / dossier knows (one alias writer).
        record_author_alias(
            db, primary_id, alt_oid, alt_author_id=alt_id, source="manual_merge"
        )

        # 5. Audit log entry — DEFERRED to after commit. add_job_log opens
        #    its OWN connection (operation_logs); emitting it here, while
        #    this merge still holds the write lock, makes that connection
        #    contend with us — wasted busy_timeout + "database is locked"
        #    noise. Collect the payload and flush once we've committed.
        pending_audit_logs.append(
            (
                f"Merged author {alt_row['name'] or alt_id} ({alt_oid}) "
                f"→ {primary_row['name'] or primary_id} ({primary_oid})",
                {
                    "primary_author_id": primary_id,
                    "primary_openalex_id": primary_oid,
                    "alt_author_id": alt_id,
                    "alt_openalex_id": alt_oid,
                    "papers_reassigned": upd_cur.rowcount or 0,
                    "papers_dropped_as_dup": drop_cur.rowcount or 0,
                },
            )
        )

        summary["alts_processed"] += 1
        summary["alt_openalex_ids"].append(alt_oid)
        summary["alt_author_ids"].append(alt_id)

    # ---- Row-less aliases (suggestion absorb) ----------------------------
    # Same paper-reattachment contract as the row path, minus everything
    # that needs an `authors` row (no soft-remove, no follow/monitor drop,
    # no field union — there's nothing local to union from). The alias row
    # is what keeps the suggestion rail from resurfacing this identity.
    for alt_oid in rowless_oids:
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

        upd_cur = db.execute(
            "UPDATE publication_authors SET openalex_id = ? WHERE openalex_id = ?",
            (primary_oid, alt_oid),
        )
        summary["papers_reassigned"] += upd_cur.rowcount or 0

        record_author_alias(db, primary_id, alt_oid, source="manual_merge")

        pending_audit_logs.append(
            (
                f"Merged row-less OpenAlex identity {alt_oid} "
                f"→ {primary_row['name'] or primary_id} ({primary_oid})",
                {
                    "primary_author_id": primary_id,
                    "primary_openalex_id": primary_oid,
                    "alt_author_id": None,
                    "alt_openalex_id": alt_oid,
                    "papers_reassigned": upd_cur.rowcount or 0,
                    "papers_dropped_as_dup": drop_cur.rowcount or 0,
                },
            )
        )

        summary["alts_processed"] += 1
        summary["alt_openalex_ids"].append(alt_oid)

    # Mirror the followed_authors deletes into feed_monitors in one
    # sync at the end (cheaper than calling per-alt). All of the below run
    # on `db` (our connection) so they're part of the same transaction.
    should_schedule_sweep = False
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

            # Enqueue on `db` (same transaction); the sweep that PROCESSES
            # this row is scheduled AFTER commit so its background worker
            # can't start contending for the writer while we still hold it.
            should_schedule_sweep = bool(
                enqueue_pending_author_hydration(
                    db,
                    primary_id,
                    priority="high",
                    reason="author_merge",
                )
            )
        except Exception as exc:
            logger.debug("author hydration enqueue skipped after merge %s: %s", primary_id, exc)

    # Caller owns the transaction — the route wraps this in `run_write_unit`
    # and the dedup sweep wraps each merge in a `write_section`, so the whole
    # merge commits atomically as ONE unit (no mid-unit commit here).

    # Post-commit, best-effort side effects, DEFERRED until the writer gate
    # (and the SQLite write lock) is released. `add_job_log` and the hydration
    # sweep persist through their OWN connections, so running them while we
    # still held the writer would contend / self-deadlock (write-discipline
    # rule #3). `run_after_gate_release` fires them right after the caller's
    # commit — or immediately if no gate is held.
    def _post_commit() -> None:
        if pending_audit_logs:
            try:
                from alma.api.scheduler import add_job_log

                for message, data in pending_audit_logs:
                    add_job_log(job_id or "author_merge", message, step="author_merged", data=data)
                # One clear summary line so the outcome is legible in Activity:
                # what folded in, how many papers moved, conflicts to review.
                if summary["alts_processed"]:
                    add_job_log(
                        job_id or "author_merge",
                        (
                            f"Merge complete → {primary_row['name'] or primary_id}: "
                            f"{summary['alts_processed']} identit(ies) folded, "
                            f"{summary['papers_reassigned']} papers reassigned, "
                            f"{summary['papers_dropped_as_dup']} dropped as duplicate, "
                            f"{len(summary['conflicts'])} conflict(s) to review"
                        ),
                        step="author_merge_summary",
                        data={
                            "primary_author_id": primary_id,
                            "alt_openalex_ids": summary["alt_openalex_ids"],
                            "conflicts": summary["conflicts"],
                            "fields_unioned": summary["fields_unioned"],
                        },
                    )
            except Exception:
                logger.debug("Audit log flush failed after merge", exc_info=True)

        if should_schedule_sweep:
            try:
                from alma.services.author_hydrate import schedule_pending_author_hydration_sweep

                schedule_pending_author_hydration_sweep(
                    reason="author_merge",
                    target_author_ids=[primary_id],
                )
            except Exception as exc:
                logger.debug("author hydration sweep skip after merge %s: %s", primary_id, exc)

    run_after_gate_release(_post_commit)
    return summary


def discover_aliases_via_orcid(
    primary_openalex_id: str,
    *,
    mailto: Optional[str] = None,
    limit: int = 10,
    known_orcid: Optional[str] = None,
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
    from alma.openalex.client import _normalize_openalex_author_id as _norm_oaid
    from alma.openalex.http import get_client

    primary_oid = (primary_openalex_id or "").strip()
    if not primary_oid:
        return {"primary_openalex_id": "", "orcid": None, "aliases": []}
    primary_oid_norm = _norm_oaid(primary_oid) or primary_oid

    try:
        # All OpenAlex HTTP goes through the shared client singleton (API-key
        # auth, rate-limit tracking, retries) — the same path every other client
        # helper uses. The legacy module-level `_session(mailto)` was removed;
        # importing it here was a dead reference that made EVERY ORCID discovery
        # raise ImportError, silently zeroing the whole dedup sweep. `mailto`
        # stays on the signature for callers; the polite pool is the client's job.
        client = get_client()
        orcid_bare = normalize_orcid(known_orcid or "") or ""
        if not orcid_bare:
            # Step 1 — fetch the primary's ORCID.
            primary_resp = client.get(
                f"/authors/{primary_oid_norm}",
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
            # OpenAlex returns the ORCID as a full URL — funnel through the
            # canonical helper so the OpenAlex `orcid:` filter and the local
            # `authors.orcid` column see the same form.
            orcid_bare = normalize_orcid(orcid_url) or ""
        if not orcid_bare:
            return {
                "primary_openalex_id": primary_oid_norm,
                "orcid": None,
                "aliases": [],
            }

        # Step 2 — query all OpenAlex authors with the same ORCID.
        per_page = max(1, min(int(limit or 10), 25))
        resp = client.get(
            "/authors",
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
    known_orcid: Optional[str] = None,
) -> dict:
    """Preventive ORCID-based alias recording.

    Used by author metadata hydration so that every other OpenAlex
    profile sharing the same ORCID is recorded in `author_alt_identifiers`.
    The suggestion rail's `followed_ids` UNION (see
    `list_author_suggestions`) then filters those alts out automatically —
    the user never sees the duplicates as fresh suggestions.

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

    # Network discovery runs FIRST, before any writes, so the writer lock is
    # never held across the OpenAlex calls.
    discovery = discover_aliases_via_orcid(primary_oid, mailto=mailto, known_orcid=known_orcid)
    aliases = discovery.get("aliases") or []
    recorded = 0
    # Gate the alias write loop in ONE BEGIN IMMEDIATE section, opened only AFTER
    # the network discovery above. Self-gating (the old contract left the commit
    # to the caller's per-source loop) makes this correct no matter who calls it:
    # a raw DEFERRED commit can lose the read→write lock-upgrade race under
    # concurrency → "database is locked"; the writer gate + IMMEDIATE removes
    # that. The single caller (_hydrate_openalex) holds no gate at this point.
    if aliases:
        with write_section(db, label="record_orcid_aliases"):
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
                if record_author_alias(
                    db, primary_id, alt_oid, alt_author_id=local_alt_id, source="orcid_discovery"
                ):
                    recorded += 1
    discovery["primary_author_id"] = primary_id
    discovery["recorded"] = recorded
    return discovery


# How long an ORCID sweep result stays "fresh" before the author re-arms as
# pending. The sweep depends on OpenAlex author disambiguation, which keeps
# splitting/merging profiles over time, so a once-swept author should be
# re-checked periodically — but NOT every page load (that was the old bug: the
# count never dropped). 30 days is a maintenance-freshness cadence, not a
# correctness deadline. Compared against ``orcid_swept_at`` via SQLite
# ``datetime('now', …)`` so storage + comparison share one format (see
# ``_stamp_orcid_swept``).
ORCID_RESWEEP_WINDOW = "-30 days"

# The ONE predicate for "this followed author still needs an ORCID sweep" —
# never scanned, or last scanned longer ago than the re-sweep window. Used
# identically by the Health pending count (`count_dedup_orcid_candidates`) and
# the sweep's own target selection, so the card's number == what a run scans ==
# what a dry-run previews. ``a`` must be aliased to the ``authors`` row; the
# caller binds ``ORCID_RESWEEP_WINDOW`` as the single ``?`` parameter.
_SWEEP_PENDING_SQL = (
    "(a.orcid_swept_at IS NULL OR a.orcid_swept_at < datetime('now', ?))"
)


def count_dedup_orcid_candidates(db: sqlite3.Connection) -> int:
    """Followed authors still PENDING an ORCID sweep — never scanned, or stale
    past the re-sweep window. Drives the Health card's pending count + ETA.

    This is deliberately NOT "every followed author with an OpenAlex id" (the set
    the sweep *walks*): that number equals the whole followed list and is
    invariant under the operation, so the card was stuck showing "all authors"
    forever. Counting the unswept remainder instead means a full run drives it to
    zero, and newly-followed or stale authors re-arm it — the honest "remaining
    work" signal a repair card promises (see migration 24 / ``orcid_swept_at``)."""
    try:
        row = db.execute(
            f"""
            SELECT COUNT(DISTINCT fa.author_id) AS c
            FROM followed_authors fa
            JOIN authors a ON a.id = fa.author_id
            WHERE COALESCE(a.status, 'active') = 'active'
              AND COALESCE(NULLIF(TRIM(a.openalex_id), ''), '') <> ''
              AND {_SWEEP_PENDING_SQL}
            """,
            (ORCID_RESWEEP_WINDOW,),
        ).fetchone()
        return int((row["c"] if row else 0) or 0)
    except sqlite3.OperationalError:
        return 0


def _papers_for_oid(db: sqlite3.Connection, openalex_id: Optional[str]) -> int:
    """Read-only count of authorship rows for an OpenAlex id — the papers a merge
    would reassign. 0 when the row has no usable openalex_id."""
    oid = (openalex_id or "").strip()
    if not oid:
        return 0
    return int(
        (db.execute(
            "SELECT COUNT(*) AS n FROM publication_authors WHERE lower(openalex_id) = lower(?)",
            (oid,),
        ).fetchone() or {"n": 0})["n"] or 0
    )


def _pick_primary(a: dict, b: dict) -> tuple[dict, dict]:
    """The ONE primary picker, shared by every detector: a deliberately-followed
    author is never subordinated to a background row; else the richer profile
    (works_count) wins; lex tie-break on author_id so (primary, alt) is the SAME
    regardless of which side we discovered from (→ UNIQUE makes re-scans
    idempotent)."""
    if a["is_followed"] and not b["is_followed"]:
        return a, b
    if b["is_followed"] and not a["is_followed"]:
        return b, a
    if (b["works_count"], b["author_id"]) > (a["works_count"], a["author_id"]):
        return b, a
    return a, b


def _is_auto_mergeable(source: str, confidence: Optional[str], primary: dict, alt: dict) -> bool:
    """Confidence policy for AUTOMATIC resolution (D-decision 2026-06-28):
      - ORCID is authoritative → always auto-merge;
      - a name·high match (same full name, modulo case/diacritics) auto-merges ONLY
        when the two ALSO share a discriminating affiliation token — so two
        different "John Smith" at unknown/different institutions are NOT silently
        merged;
      - everything else (uncorroborated high, medium, low) waits for human review.
    """
    if source == "orcid":
        return True
    if source == "name" and confidence == "high":
        return affiliations_corroborate(primary.get("affiliation"), alt.get("affiliation"))
    return False


def _consider_merge_pair(
    db: sqlite3.Connection,
    anchor: dict,
    other: dict,
    *,
    source: str,
    confidence: Optional[str],
    shared_orcid: Optional[str],
    dry_run: bool,
    summary: dict,
    merged_away: set[str],
    job_id: Optional[str] = None,
) -> None:
    """Shared pair → resolution path for BOTH detectors (ORCID + name): pick the
    primary/alt, skip self / already-merged-away / any user-REJECTED pair (never
    resurface), then EITHER auto-merge high-confidence pairs (``_is_auto_mergeable``)
    or record the rest as a candidate for manual review. Each write is its own
    gated section (background scan); the sample carries source/confidence."""
    if anchor["author_id"] == other["author_id"]:
        return
    # A side merged away earlier in THIS scan is gone — skip stale pairs.
    if anchor["author_id"] in merged_away or other["author_id"] in merged_away:
        return
    primary, alt = _pick_primary(anchor, other)
    if is_pair_rejected(db, primary["author_id"], alt["author_id"]):
        return
    papers_estimate = _papers_for_oid(db, alt["openalex_id"])

    if _is_auto_mergeable(source, confidence, primary, alt):
        if not dry_run:
            try:
                with write_section(db, label="author_dedup_auto_merge"):
                    merge_author_profiles(
                        db, primary["author_id"], [alt["author_id"]], job_id=job_id
                    )
            except Exception as exc:  # pragma: no cover — best-effort
                logger.warning(
                    "Auto-merge failed for %s ← %s: %s",
                    primary["author_id"], alt["author_id"], exc,
                )
                summary["errors"] += 1
                return
        merged_away.add(alt["author_id"])
        summary["auto_merged"] += 1
        if len(summary["sample"]) < 25:
            summary["sample"].append(
                {
                    "action": "auto_merge",
                    "source": source,
                    "confidence": confidence,
                    "primary_id": primary["author_id"],
                    "primary_name": primary["name"],
                    "alt_id": alt["author_id"],
                    "alt_name": alt["name"],
                    "papers_reassigned": papers_estimate,
                }
            )
        return

    # Manual review path — record the candidate.
    if dry_run:
        recorded = db.execute(
            "SELECT 1 FROM author_merge_candidates "
            "WHERE primary_author_id = ? AND alt_author_id = ? LIMIT 1",
            (primary["author_id"], alt["author_id"]),
        ).fetchone() is None
    else:
        with write_section(db, label="author_dedup_record_candidate"):
            recorded = _record_merge_candidate(
                db,
                primary["author_id"],
                alt["author_id"],
                alt_openalex_id=alt["openalex_id"],
                shared_orcid=shared_orcid,
                papers_estimate=papers_estimate,
                source=source,
                confidence=confidence,
            )
    if recorded:
        summary["candidates_found"] += 1
        summary[f"{source}_pairs"] = int(summary.get(f"{source}_pairs", 0)) + 1
    if len(summary["sample"]) < 25:
        summary["sample"].append(
            {
                "action": "candidate",
                "source": source,
                "confidence": confidence,
                "primary_id": primary["author_id"],
                "primary_name": primary["name"],
                "alt_id": alt["author_id"],
                "alt_name": alt["name"],
                "alt_oid": alt["openalex_id"],
                "papers_estimate": papers_estimate,
            }
        )


def _detect_name_match_candidates(
    db: sqlite3.Connection,
    active_authors: list[dict],
    *,
    dry_run: bool,
    summary: dict,
    merged_away: Optional[set[str]] = None,
    job_id: Optional[str] = None,
) -> None:
    """Network-free pass: flag pairs of authors with compatible names (same
    surname, given names that line up allowing initials — "E. van Hove" ≈ "Emily
    van Hove"). Anchored on FOLLOWED authors (so we surface duplicates of the
    identities the user actually tracks, not noise among background co-authors).
    Surname-blocked so it stays cheap on a large corpus. Shares ``_consider_merge_pair``
    with the ORCID detector, so the picker / rejected-skip / auto-vs-manual split
    are identical."""
    if merged_away is None:
        merged_away = set()
    blocks: dict[str, list[dict]] = {}
    for entry in active_authors:
        parsed = parse_person_name(entry["name"])
        if parsed is None or not parsed.surname:
            continue
        blocks.setdefault(parsed.surname, []).append(entry)

    for anchor in active_authors:
        if not anchor["is_followed"] or anchor["author_id"] in merged_away:
            continue
        parsed = parse_person_name(anchor["name"])
        if parsed is None or not parsed.surname:
            continue
        for other in blocks.get(parsed.surname, []):
            if other["author_id"] == anchor["author_id"]:
                continue
            confidence = name_match_confidence(anchor["name"], other["name"])
            if not confidence:
                continue
            _consider_merge_pair(
                db, anchor, other,
                source="name", confidence=confidence, shared_orcid=None,
                dry_run=dry_run, summary=summary, merged_away=merged_away, job_id=job_id,
            )


def scan_duplicate_candidates(
    db: sqlite3.Connection,
    *,
    mailto: Optional[str] = None,
    sleep_between_calls: float = 0.05,
    limit: int,
    job_id: Optional[str] = None,
    dry_run: bool = False,
) -> dict:
    """SCAN followed authors for duplicate profiles and RECORD what to merge.

    Non-destructive discovery — it never merges. The user reviews + applies the
    recorded pairs via ``apply_merge_candidates`` / the per-candidate
    routes. Two detectors feed the same ``author_merge_candidates`` queue (both
    via ``_consider_merge_pair``, so the primary-picker, rejected-skip and
    recording are identical):

      1. **ORCID** (authoritative, network) — for each followed author PENDING a
         check (never swept / stale past ``ORCID_RESWEEP_WINDOW`` — the set
         ``count_dedup_orcid_candidates`` reports as scan coverage), ask OpenAlex
         for profiles sharing the ORCID; a sharing LOCAL row → candidate
         (source='orcid'); an external profile → ``author_alt_identifiers`` alias.
         Then stamp ``orcid_swept_at`` so coverage drops.
      2. **Name/initials** (heuristic, local) — ``_detect_name_match_candidates``
         flags compatible names ("E. van Hove" ≈ "Emily van Hove",
         source='name', confidence high/medium/low). Runs every scan over all
         followed authors (cheap, no swept gating).

    HIGH-confidence pairs are AUTO-RESOLVED here (``_is_auto_mergeable``): an
    ORCID match (authoritative) or a name·high match whose two rows ALSO share an
    affiliation. Everything less certain (uncorroborated name·high, name·medium,
    name·low) is recorded for MANUAL review — that's what the merge card counts.

    Any pair the user permanently REJECTED is skipped by both detectors and never
    resurfaced. Network I/O stays OUTSIDE every ``write_section``; each merge /
    record / swept stamp is its own gated write. ``dry_run=True`` discovers but
    writes NOTHING — for tests. Returns a summary for the Activity envelope:
      {"success", "dry_run", "scanned", "auto_merged", "candidates_found",
       "orcid_pairs", "name_pairs", "aliases_recorded", "errors",
       "skipped_no_orcid", "sample"[:25], "message"}.
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
    # `sweep_pending` is computed in SQL (via `_SWEEP_PENDING_SQL`) so the
    # never-swept/stale decision uses the same `datetime('now', …)` comparison
    # the count does — never a Python isoformat-vs-SQLite string compare. The
    # full `by_oid` map still spans EVERY active author (followed + background)
    # so an ORCID-discovered alias that exists locally as a non-followed row
    # still folds into its followed primary; only the scan TARGETS are filtered
    # to the pending set.
    # Load EVERY active author once (followed + background). `active_authors`
    # feeds the name detector (which must see no-openalex_id rows too); `by_oid`
    # is the subset with an openalex_id that the ORCID detector keys on so a
    # discovered alias mapping to a local background row folds into its followed
    # primary. `sweep_pending` is computed in SQL via `_SWEEP_PENDING_SQL` so the
    # never-swept/stale test uses the same `datetime('now', …)` comparison the
    # count does (never a Python isoformat-vs-SQLite string compare).
    all_rows = db.execute(
        f"""
        SELECT
            a.id AS author_id,
            a.openalex_id,
            a.name,
            COALESCE(a.affiliation, '') AS affiliation,
            COALESCE(a.works_count, 0) AS works_count,
            CASE WHEN fa.author_id IS NOT NULL THEN 1 ELSE 0 END AS is_followed,
            CASE WHEN {_SWEEP_PENDING_SQL} THEN 1 ELSE 0 END AS sweep_pending
        FROM authors a
        LEFT JOIN followed_authors fa ON fa.author_id = a.id
        WHERE COALESCE(a.status, 'active') = 'active'
        """,
        (ORCID_RESWEEP_WINDOW,),
    ).fetchall()

    by_oid: dict[str, dict] = {}
    active_authors: list[dict] = []
    followed_rows: list[dict] = []
    for r in all_rows:
        entry = {
            "author_id": str(r["author_id"]),
            "openalex_id": str(r["openalex_id"] or ""),
            "name": str(r["name"] or ""),
            "affiliation": str(r["affiliation"] or ""),
            "works_count": int(r["works_count"] or 0),
            "is_followed": bool(r["is_followed"]),
            "sweep_pending": bool(r["sweep_pending"]),
        }
        active_authors.append(entry)
        oid = entry["openalex_id"].strip().lower()
        if oid:
            by_oid[oid] = entry
        if entry["is_followed"]:
            followed_rows.append(entry)

    summary = {
        "success": True,
        "dry_run": bool(dry_run),
        "scanned": 0,
        "auto_merged": 0,
        "candidates_found": 0,
        "orcid_pairs": 0,
        "name_pairs": 0,
        "aliases_recorded": 0,
        "errors": 0,
        "skipped_no_orcid": 0,
        "sample": [],
        "message": "",
    }
    # author_ids merged away earlier in THIS scan — so a later pair touching one
    # is skipped (the row is gone). Shared across both detectors.
    merged_away: set[str] = set()

    # Iterate followed authors that are PENDING a scan only — never swept or
    # stale (`sweep_pending`, computed in SQL above). This is the same set the
    # scan-coverage count reports, so a run clears exactly the pending backlog
    # and the coverage count falls to zero. (We never start discovery from a
    # random co-author background row; a discovered alias that maps to a
    # non-followed local row is still recorded as a candidate against it.)
    initial_targets = [e for e in followed_rows if e["sweep_pending"]]
    import time

    # Total budget (REQUIRED, explicit): bound the number of ORCID/OpenAlex
    # discovery scans this run — the unit is one followed author, one network
    # round-trip each. The maintenance tick passes the registry cap; the admin
    # sweep route passes its full-sweep budget. The remainder carries over to the
    # next run. There is no unbounded mode.
    cap = max(1, int(limit))

    for target in initial_targets:
        if summary["scanned"] >= cap:
            break

        summary["scanned"] += 1
        try:
            discovery = discover_aliases_via_orcid(
                target["openalex_id"], mailto=mailto,
            )
        except Exception as exc:  # pragma: no cover — best-effort
            # A network/lookup FAILURE is the only case we leave unstamped, so the
            # author is retried next run (don't `continue` past the stamp for the
            # success cases below).
            logger.warning("ORCID discovery failed for %s: %s", target["author_id"], exc)
            summary["errors"] += 1
            continue
        shared_orcid = discovery.get("orcid")
        # A successful scan that finds no ORCID is still a completed scan — it gets
        # stamped (below) so we don't re-walk it every run; only the alias-matching
        # work is skipped.
        aliases = (discovery.get("aliases") or []) if shared_orcid else []
        if not shared_orcid:
            summary["skipped_no_orcid"] += 1

        for alias in aliases:
            alt_oid = str(alias.get("openalex_id") or "").strip()
            if not alt_oid:
                continue
            alt_oid_key = alt_oid.lower()

            existing_local = by_oid.get(alt_oid_key)
            if existing_local and existing_local["author_id"] != target["author_id"]:
                # Same human, two local rows — a mergeable pair. Record it through
                # the shared path (picker / rejected-skip / record identical to
                # the name detector).
                _consider_merge_pair(
                    db, target, existing_local,
                    source="orcid", confidence=None, shared_orcid=shared_orcid,
                    dry_run=dry_run, summary=summary, merged_away=merged_away, job_id=job_id,
                )
            else:
                # Not a local author row — record as a known external alias of
                # the target so the suggestion rail filters it out forever
                # (UNIQUE constraint makes this idempotent).
                local_row = db.execute(
                    "SELECT id FROM authors WHERE lower(openalex_id) = lower(?) LIMIT 1",
                    (alt_oid,),
                ).fetchone()
                local_alt_id = str(local_row["id"]) if local_row else None
                if dry_run:
                    already = db.execute(
                        "SELECT 1 FROM author_alt_identifiers "
                        "WHERE primary_author_id = ? AND lower(alt_openalex_id) = lower(?) "
                        "LIMIT 1",
                        (target["author_id"], alt_oid),
                    ).fetchone()
                    inserted = already is None
                else:
                    with write_section(db, label="author_dedup_alias"):
                        inserted = record_author_alias(
                            db,
                            target["author_id"],
                            alt_oid,
                            alt_author_id=local_alt_id,
                            source="orcid_sweep",
                        )
                if inserted:
                    summary["aliases_recorded"] += 1

        # Stamp this target as freshly scanned so the scan-coverage count drops
        # (real runs only — dry-run writes nothing). Stored via SQLite
        # `datetime('now')` so storage + `_SWEEP_PENDING_SQL` share one format.
        # Own gated section: the writer is released before the next target's
        # network call (write discipline).
        if not dry_run:
            with write_section(db, label="author_dedup_swept"):
                db.execute(
                    "UPDATE authors SET orcid_swept_at = datetime('now') WHERE id = ?",
                    (target["author_id"],),
                )

        if sleep_between_calls > 0:
            time.sleep(sleep_between_calls)

    # Name/initials pass — local, network-free, over ALL followed authors (not
    # just the ORCID-pending ones), so it runs even when coverage is already 0.
    _detect_name_match_candidates(
        db, active_authors, dry_run=dry_run, summary=summary,
        merged_away=merged_away, job_id=job_id,
    )

    # No final commit: every auto-merge/candidate/alias/swept write was its own
    # gated write_section above (network stayed outside each section).
    auto = summary["auto_merged"]
    pairs = summary["candidates_found"]
    summary["message"] = (
        f"Duplicate scan: auto-merged {auto} high-confidence; "
        f"{pairs} pair{'' if pairs == 1 else 's'} need review "
        f"({summary['orcid_pairs']} ORCID, {summary['name_pairs']} name)"
        f"{' (preview)' if dry_run else ''}; {summary['aliases_recorded']} aliases "
        f"across {summary['scanned']} authors scanned "
        f"({summary['skipped_no_orcid']} had no ORCID, {summary['errors']} errored)"
    )
    return summary


# Detection-source strength, so a stronger signal upgrades a weaker one on the
# same pair (ORCID always beats a name guess; high name beats low) and a weaker
# re-find never downgrades. Bigger = stronger.
_SOURCE_RANK = {
    ("orcid", None): 100,
    ("name", "high"): 3,
    ("name", "medium"): 2,
    ("name", "low"): 1,
}


def _candidate_rank(source: Optional[str], confidence: Optional[str]) -> int:
    if source == "orcid":
        return 100
    return _SOURCE_RANK.get((source or "name", confidence), 1)


def _record_merge_candidate(
    db: sqlite3.Connection,
    primary_author_id: str,
    alt_author_id: str,
    *,
    alt_openalex_id: Optional[str],
    shared_orcid: Optional[str],
    papers_estimate: int,
    source: str,
    confidence: Optional[str] = None,
) -> bool:
    """UPSERT one pending merge pair into ``author_merge_candidates``.

    Returns True when the pair is NEW (so the scan counts it as a fresh find),
    False when it already existed (we refresh its estimate/timestamp, and upgrade
    its ``source``/``confidence`` only if this find is STRONGER — ORCID beats a
    name guess — but never double-count and never downgrade). UNIQUE(primary, alt)
    + the canonical primary-picker make a re-scan idempotent. Caller holds the
    writer gate."""
    existing = db.execute(
        "SELECT id, source, confidence FROM author_merge_candidates "
        "WHERE primary_author_id = ? AND alt_author_id = ?",
        (primary_author_id, alt_author_id),
    ).fetchone()
    if existing:
        keep_source, keep_conf = str(existing["source"] or "name"), existing["confidence"]
        if _candidate_rank(source, confidence) > _candidate_rank(keep_source, keep_conf):
            keep_source, keep_conf = source, confidence
        db.execute(
            "UPDATE author_merge_candidates SET alt_openalex_id = ?, shared_orcid = ?, "
            "papers_estimate = ?, source = ?, confidence = ?, discovered_at = datetime('now') "
            "WHERE id = ?",
            (alt_openalex_id, shared_orcid, int(papers_estimate), keep_source, keep_conf, str(existing["id"])),
        )
        return False
    db.execute(
        "INSERT INTO author_merge_candidates "
        "(id, primary_author_id, alt_author_id, alt_openalex_id, shared_orcid, "
        " papers_estimate, source, confidence, discovered_at) "
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, datetime('now'))",
        (
            uuid.uuid4().hex,
            primary_author_id,
            alt_author_id,
            alt_openalex_id,
            shared_orcid,
            int(papers_estimate),
            source,
            confidence,
        ),
    )
    return True


def _pair(author_id_a: str, author_id_b: str) -> tuple[str, str]:
    """Canonical (lo, hi) ordering so a rejected/known pair is direction-free."""
    return (author_id_a, author_id_b) if author_id_a <= author_id_b else (author_id_b, author_id_a)


def is_pair_rejected(db: sqlite3.Connection, author_id_a: str, author_id_b: str) -> bool:
    """Did the user permanently reject merging these two? Every detector checks
    this before proposing, so a rejected pair is never resurfaced."""
    lo, hi = _pair(author_id_a, author_id_b)
    try:
        row = db.execute(
            "SELECT 1 FROM author_merge_rejections WHERE author_id_lo = ? AND author_id_hi = ? LIMIT 1",
            (lo, hi),
        ).fetchone()
        return row is not None
    except sqlite3.OperationalError:
        return False


def record_merge_rejection(
    db: sqlite3.Connection,
    author_id_a: str,
    author_id_b: str,
    *,
    source: Optional[str] = None,
    reason: Optional[str] = None,
) -> None:
    """Persist a permanent "these two are NOT the same person" verdict. Idempotent
    (UNIQUE on the canonical pair). Caller holds the writer gate."""
    lo, hi = _pair(author_id_a, author_id_b)
    db.execute(
        "INSERT OR IGNORE INTO author_merge_rejections "
        "(id, author_id_lo, author_id_hi, source, reason, rejected_at) "
        "VALUES (?, ?, ?, ?, ?, datetime('now'))",
        (uuid.uuid4().hex, lo, hi, source, reason),
    )


def reject_merge_candidate(db: sqlite3.Connection, candidate_id: str) -> dict:
    """Reject one pending candidate: record the permanent rejection AND drop the
    candidate row so it leaves the queue and is never re-proposed.

    RAW writes — the caller owns the gated write window (``run_write_unit`` in the
    foreground reject route), matching ``merge_author_profiles``'s contract."""
    row = db.execute(
        "SELECT primary_author_id, alt_author_id, source FROM author_merge_candidates WHERE id = ?",
        (candidate_id,),
    ).fetchone()
    if not row:
        return {"success": False, "reason": "not_found"}
    record_merge_rejection(
        db,
        str(row["primary_author_id"]),
        str(row["alt_author_id"]),
        source=str(row["source"] or ""),
        reason="user_rejected",
    )
    db.execute("DELETE FROM author_merge_candidates WHERE id = ?", (candidate_id,))
    return {"success": True, "rejected_pair": [row["primary_author_id"], row["alt_author_id"]]}


def import_suggested_author(
    db: sqlite3.Connection, primary_author_id: str, suggested_openalex_id: str
) -> dict:
    """Fold an EXTERNAL suggested author (an OpenAlex id from the suggestion rail,
    no local row required) into an existing followed author — the "merge into your
    existing author" action for a name-duplicate suggestion. Reassigns the
    suggested profile's papers to the primary and records its OpenAlex id as an
    alias so it stops being re-suggested. RAW — caller owns the gated window
    (``run_write_unit`` in the foreground route)."""
    oid = (suggested_openalex_id or "").strip()
    if not oid:
        return {"success": False, "reason": "missing_openalex_id"}
    primary = db.execute(
        "SELECT 1 FROM authors WHERE id = ? AND COALESCE(status,'active')='active' LIMIT 1",
        (primary_author_id,),
    ).fetchone()
    if not primary:
        return {"success": False, "reason": "primary_not_found"}
    result = merge_author_profiles(db, primary_author_id, [], alt_openalex_ids=[oid])
    record_author_alias(db, primary_author_id, oid, source="suggestion_import")
    return {
        "success": True,
        "primary_author_id": primary_author_id,
        "papers_reassigned": int(result.get("papers_reassigned") or 0),
    }


def merge_one_candidate(db: sqlite3.Connection, candidate_id: str) -> dict:
    """Apply ONE pending candidate from the review dialog (the per-row ✓ Merge).

    RAW writes — the caller owns the gated window (``run_write_unit`` in the
    foreground route). Reuses ``_apply_one_candidate`` (shared with the bulk
    apply) and prunes any pair this merge made stale."""
    cand = db.execute(
        "SELECT id, primary_author_id, alt_author_id FROM author_merge_candidates WHERE id = ?",
        (candidate_id,),
    ).fetchone()
    if not cand:
        return {"success": False, "reason": "not_found"}
    result = _apply_one_candidate(db, cand)
    prune_stale_merge_candidates(db)
    return {
        "success": True,
        "primary_author_id": cand["primary_author_id"],
        "papers_reassigned": int(result.get("papers_reassigned") or 0),
    }


# One predicate for "this candidate is still actionable" — both sides are still
# active author rows (a row removed by an earlier merge makes the pair stale).
_CANDIDATE_ACTIVE_SQL = (
    "COALESCE(p.status, 'active') = 'active' AND COALESCE(a.status, 'active') = 'active'"
)


def count_merge_candidates(db: sqlite3.Connection) -> int:
    """Number of pending merge pairs (any source) whose BOTH rows are still active
    — the truthful "N duplicate profiles to merge" the Health merge card shows.
    Zero until a scan finds duplicates; back to zero once they're applied (never
    the whole author list)."""
    try:
        row = db.execute(
            f"""
            SELECT COUNT(*) AS c
            FROM author_merge_candidates c
            JOIN authors p ON p.id = c.primary_author_id
            JOIN authors a ON a.id = c.alt_author_id
            WHERE {_CANDIDATE_ACTIVE_SQL}
            """
        ).fetchone()
        return int((row["c"] if row else 0) or 0)
    except sqlite3.OperationalError:
        return 0


def list_merge_candidates(
    db: sqlite3.Connection, *, limit: int = 200
) -> list[dict]:
    """The pending merge pairs, joined to author names + paper impact, for the
    review dialog. Each carries its `source` ('orcid'/'name') + `confidence` so
    the UI badges how much to trust it. ORCID first, then by paper impact, so the
    authoritative + consequential merges lead and weak name guesses sink."""
    try:
        rows = db.execute(
            f"""
            SELECT
                c.id,
                c.primary_author_id, p.name AS primary_name, p.openalex_id AS primary_oid,
                c.alt_author_id, a.name AS alt_name, c.alt_openalex_id,
                c.shared_orcid, COALESCE(c.papers_estimate, 0) AS papers_estimate,
                COALESCE(c.source, 'orcid') AS source, c.confidence,
                c.discovered_at
            FROM author_merge_candidates c
            JOIN authors p ON p.id = c.primary_author_id
            JOIN authors a ON a.id = c.alt_author_id
            WHERE {_CANDIDATE_ACTIVE_SQL}
            ORDER BY (CASE WHEN COALESCE(c.source,'orcid') = 'orcid' THEN 0 ELSE 1 END),
                     c.papers_estimate DESC, c.discovered_at DESC
            LIMIT ?
            """,
            (int(limit),),
        ).fetchall()
        return [dict(r) for r in rows]
    except sqlite3.OperationalError:
        return []


def _apply_one_candidate(
    db: sqlite3.Connection, cand: Mapping, *, job_id: Optional[str] = None
) -> dict:
    """Merge a candidate's alt → primary and consume the candidate row.

    RAW — the caller owns the write window (``write_section`` in the background
    bulk apply, ``run_write_unit`` in the foreground per-candidate route). Returns
    the ``merge_author_profiles`` result."""
    result = merge_author_profiles(
        db, str(cand["primary_author_id"]), [str(cand["alt_author_id"])], job_id=job_id
    )
    db.execute("DELETE FROM author_merge_candidates WHERE id = ?", (str(cand["id"]),))
    return result


def prune_stale_merge_candidates(db: sqlite3.Connection) -> None:
    """Drop candidates whose primary OR alt a prior merge soft-removed — keeps the
    count honest without a re-scan. RAW; caller owns the write window."""
    db.execute(
        """
        DELETE FROM author_merge_candidates
        WHERE primary_author_id IN (SELECT id FROM authors WHERE status = 'removed')
           OR alt_author_id IN (SELECT id FROM authors WHERE status = 'removed')
        """
    )


def apply_merge_candidates(
    db: sqlite3.Connection,
    *,
    limit: int,
    candidate_ids: Optional[Iterable[str]] = None,
    job_id: Optional[str] = None,
) -> dict:
    """Apply pending merge candidates — the DESTRUCTIVE half (any source), gated
    behind the Health review dialog. For each candidate (both rows still active)
    merge ``alt`` → ``primary`` and consume the row via ``_apply_one_candidate``;
    a stale pair is pruned, not merged. Each merge is its own gated
    ``write_section`` (background); idempotent on re-run."""
    summary = {
        "success": True,
        "merged": 0,
        "skipped": 0,
        "errors": 0,
        "sample": [],
        "message": "",
    }
    candidates = list_merge_candidates(db, limit=max(1, int(limit)))
    if candidate_ids is not None:
        wanted = {str(cid) for cid in candidate_ids}
        candidates = [c for c in candidates if str(c["id"]) in wanted]

    for cand in candidates:
        try:
            with write_section(db, label="author_merge_apply"):
                merge_result = _apply_one_candidate(db, cand, job_id=job_id)
        except Exception as exc:  # pragma: no cover — best-effort
            logger.warning("Apply merge candidate %s failed: %s", cand["id"], exc)
            summary["errors"] += 1
            continue
        summary["merged"] += 1
        if len(summary["sample"]) < 25:
            summary["sample"].append(
                {
                    "primary_id": cand["primary_author_id"],
                    "primary_name": cand["primary_name"],
                    "alt_id": cand["alt_author_id"],
                    "alt_name": cand["alt_name"],
                    "papers_reassigned": int(merge_result.get("papers_reassigned") or 0),
                }
            )

    with write_section(db, label="author_merge_prune_stale"):
        prune_stale_merge_candidates(db)

    merged_n = summary["merged"]
    errored_n = summary["errors"]
    msg = f"Applied {merged_n} author merge{'' if merged_n == 1 else 's'}"
    if errored_n:
        msg += f", {errored_n} errored"
    summary["message"] = msg
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
