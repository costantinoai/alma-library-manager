"""What one author merge changed, kept so the merge can be taken back.

A merge moves papers, Feed rows, evidence and aliases onto the survivor, and
afterwards nothing on those rows says where they came from. So each merged
profile writes one `author_merges` row with exactly what moved, and
`undo_author_merge` replays it backwards. Merges made before this ledger have no
row and cannot be undone.

Every function here writes on the caller's connection and never commits: the
merge and the undo are each one transaction, owned by the caller.
"""

from __future__ import annotations

import json
import sqlite3
import uuid
from typing import Any

from alma.core.paper_groups import strongest_feed_status
from alma.core.time import utcnow

_AUTHOR_MONITOR_PREFIX = "author:"
_PRIMARY_FIELDS = (
    "name", "affiliation", "email_domain", "url_picture", "citedby", "h_index",
    "works_count", "interests", "institutions", "orcid", "scholar_id", "semantic_scholar_id",
)
_HARD_IDS = ("orcid", "scholar_id", "semantic_scholar_id")


def snapshot_before_merge(
    db: sqlite3.Connection,
    primary_id: str,
    primary_oid: str,
    alt_row: sqlite3.Row | None,
    alt_oid: str,
) -> dict[str, Any]:
    """Read everything the merge is about to change for this one profile."""
    primary = db.execute("SELECT * FROM authors WHERE id = ?", (primary_id,)).fetchone()
    colliding = db.execute(
        """
        SELECT * FROM publication_authors
        WHERE lower(openalex_id) = lower(?)
          AND paper_id IN (SELECT paper_id FROM publication_authors WHERE lower(openalex_id) = lower(?))
        """,
        (alt_oid, primary_oid),
    ).fetchall() if alt_oid else []
    colliding_ids = {str(row["paper_id"]) for row in colliding}
    moved = [
        str(row[0])
        for row in db.execute(
            "SELECT paper_id FROM publication_authors WHERE lower(openalex_id) = lower(?)",
            (alt_oid,),
        )
        if str(row[0]) not in colliding_ids
    ] if alt_oid else []
    alt_id = str(alt_row["id"]) if alt_row is not None else None
    follow = db.execute(
        "SELECT followed_at, notify_new_papers, is_owner FROM followed_authors WHERE author_id = ?",
        (alt_id,),
    ).fetchone() if alt_id else None
    return {
        "primary_id": primary_id,
        "primary_oid": primary_oid,
        "alt_id": alt_id,
        "alt_oid": alt_oid,
        "alt_name": str(alt_row["name"] or "") if alt_row is not None else "",
        "primary_before": {field: primary[field] for field in _PRIMARY_FIELDS},
        "alt_before": dict(alt_row) if alt_row is not None else None,
        "follow": dict(follow) if follow else None,
        "moved_papers": moved,
        "dropped_authorships": [dict(row) for row in colliding],
    }


def transfer_side_tables(db: sqlite3.Connection, snapshot: dict[str, Any]) -> None:
    """Hand the survivor what only the alt's row held, noting each move for undo."""
    primary_id, alt_id = snapshot["primary_id"], snapshot["alt_id"]
    moved: dict[str, list] = {"evidence": [], "aliases": [], "conflicts": [], "rejections": []}
    if alt_id:
        for row in db.execute(
            "SELECT id, source, institution_name, role, start_date "
            "FROM author_affiliation_evidence WHERE author_id = ?",
            (alt_id,),
        ).fetchall():
            cursor = db.execute(
                "UPDATE OR IGNORE author_affiliation_evidence SET author_id = ? WHERE id = ?",
                (primary_id, row["id"]),
            )
            if cursor.rowcount:
                moved["evidence"].append(row["id"])
        # An earlier merge into the alt now means the survivor, so no chain forms.
        for row in db.execute(
            "SELECT id FROM author_alt_identifiers WHERE primary_author_id = ?", (alt_id,)
        ).fetchall():
            cursor = db.execute(
                "UPDATE OR IGNORE author_alt_identifiers SET primary_author_id = ? WHERE id = ?",
                (primary_id, row["id"]),
            )
            if cursor.rowcount:
                moved["aliases"].append(row["id"])
        for row in db.execute(
            "SELECT id FROM author_merge_conflicts WHERE primary_author_id = ?", (alt_id,)
        ).fetchall():
            cursor = db.execute(
                "UPDATE OR IGNORE author_merge_conflicts SET primary_author_id = ? WHERE id = ?",
                (primary_id, row["id"]),
            )
            if cursor.rowcount:
                moved["conflicts"].append(row["id"])
        # "Not the same person as X" was said of a person, who is now the survivor.
        from alma.application.author_merge import _pair, is_pair_rejected, record_merge_rejection

        for row in db.execute(
            "SELECT author_id_lo, author_id_hi FROM author_merge_rejections "
            "WHERE author_id_lo = ? OR author_id_hi = ?",
            (alt_id, alt_id),
        ).fetchall():
            other = row["author_id_hi"] if row["author_id_lo"] == alt_id else row["author_id_lo"]
            if other != primary_id and not is_pair_rejected(db, primary_id, other):
                record_merge_rejection(db, primary_id, other, source="merge", reason="carried")
                moved["rejections"].append(list(_pair(primary_id, other)))
    if snapshot["alt_oid"]:
        db.execute(
            "DELETE FROM author_centroids WHERE lower(author_openalex_id) = lower(?)",
            (snapshot["alt_oid"],),
        )
    snapshot["moved"] = moved


def carry_follow(db: sqlite3.Connection, primary_id: str, follow: dict | None) -> None:
    """The survivor keeps the earlier follow date and the alert the alt had on."""
    if not follow:
        return
    db.execute(
        "UPDATE followed_authors SET followed_at = MIN(followed_at, ?), "
        "notify_new_papers = MAX(COALESCE(notify_new_papers, 0), ?) WHERE author_id = ?",
        (follow["followed_at"], int(follow["notify_new_papers"] or 0), primary_id),
    )


def repoint_feed_rows(db: sqlite3.Connection, snapshot: dict[str, Any]) -> None:
    """Move the alt's Feed rows to the survivor's monitor, verdicts included.

    Runs before `sync_author_monitors`, which deletes whatever rows a dropped
    monitor still owns. A paper already in the survivor's Feed keeps one row with
    the stronger verdict of the two.
    """
    primary_id, alt_id = snapshot["primary_id"], snapshot["alt_id"]
    snapshot["feed_moved"], snapshot["feed_dropped"] = [], []
    if not alt_id:
        return
    monitor_id = f"{_AUTHOR_MONITOR_PREFIX}{primary_id}"
    monitor = db.execute(
        "SELECT id, label FROM feed_monitors WHERE id = ?", (monitor_id,)
    ).fetchone()
    if monitor is None:
        return
    for row in db.execute(
        "SELECT * FROM feed_items WHERE author_id = ? OR monitor_id = ?",
        (alt_id, f"{_AUTHOR_MONITOR_PREFIX}{alt_id}"),
    ).fetchall():
        kept = db.execute(
            "SELECT id, status FROM feed_items WHERE paper_id = ? AND author_id = ?",
            (row["paper_id"], primary_id),
        ).fetchone()
        if kept is None:
            db.execute(
                "UPDATE feed_items SET author_id = ?, monitor_id = ?, monitor_label = ? WHERE id = ?",
                (primary_id, monitor_id, monitor["label"], row["id"]),
            )
            snapshot["feed_moved"].append(
                {key: row[key] for key in ("id", "author_id", "monitor_id", "monitor_label")}
            )
        else:
            db.execute(
                "UPDATE feed_items SET status = ? WHERE id = ?",
                (strongest_feed_status({row["status"], kept["status"]}), kept["id"]),
            )
            db.execute("DELETE FROM feed_items WHERE id = ?", (row["id"],))
            snapshot["feed_dropped"].append(dict(row))


def write_ledger_row(db: sqlite3.Connection, snapshot: dict[str, Any]) -> str:
    """Record the merge once everything it moved is known."""
    merge_id = uuid.uuid4().hex
    primary = db.execute(
        "SELECT * FROM authors WHERE id = ?", (snapshot["primary_id"],)
    ).fetchone()
    snapshot["primary_after"] = {field: primary[field] for field in _PRIMARY_FIELDS}
    db.execute(
        """
        INSERT INTO author_merges
            (id, primary_author_id, primary_openalex_id, alt_author_id, alt_openalex_id,
             alt_name, papers_moved, snapshot_json, merged_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            merge_id,
            snapshot["primary_id"],
            snapshot["primary_oid"],
            snapshot["alt_id"],
            snapshot["alt_oid"],
            snapshot["alt_name"],
            len(snapshot["moved_papers"]),
            json.dumps(snapshot, default=str),
            utcnow().isoformat(),
        ),
    )
    return merge_id


def list_author_merges(db: sqlite3.Connection, author_id: str) -> list[dict]:
    """Merges into this author, newest first. Pure read, no snapshot."""
    try:
        rows = db.execute(
            """
            SELECT id, primary_author_id, alt_author_id, alt_openalex_id, alt_name,
                   papers_moved, merged_at, undone_at
            FROM author_merges WHERE primary_author_id = ?
            ORDER BY merged_at DESC
            """,
            (author_id,),
        ).fetchall()
    except sqlite3.OperationalError:
        return []
    return [dict(row) for row in rows]


def _restore_primary_fields(db: sqlite3.Connection, snapshot: dict[str, Any]) -> None:
    """Put back what the merge wrote, unless something changed that value since."""
    primary_id = snapshot["primary_id"]
    current = db.execute("SELECT * FROM authors WHERE id = ?", (primary_id,)).fetchone()
    before, after = snapshot["primary_before"], snapshot["primary_after"]
    changed = [
        field for field in _PRIMARY_FIELDS
        if before.get(field) != after.get(field) and current[field] == after.get(field)
    ]
    if changed:
        db.execute(
            f"UPDATE authors SET {', '.join(f'{field} = ?' for field in changed)} WHERE id = ?",  # noqa: S608
            [*(before.get(field) for field in changed), primary_id],
        )


def _restore_alt_row(db: sqlite3.Connection, snapshot: dict[str, Any]) -> None:
    alt = snapshot["alt_before"]
    if not alt:
        return
    db.execute(
        "UPDATE authors SET status = ?, author_type = ?, "
        + ", ".join(f"{field} = COALESCE({field}, ?)" for field in _HARD_IDS)
        + " WHERE id = ?",
        (alt.get("status") or "active", alt.get("author_type"),
         *(alt.get(field) for field in _HARD_IDS), alt["id"]),
    )


def undo_author_merge(db: sqlite3.Connection, merge_id: str) -> dict:
    """Take one merge back and remember the two are different people.

    Papers the ledger moved go back. Papers fetched since were written under the
    survivor, and their next refresh corrects them, because the alias is gone. The
    undo records a "not the same person" verdict, or the next scan would merge an
    ORCID match straight back; that verdict can be removed like any other.
    """
    from alma.application.author_merge import MergeRefused, _pair, record_merge_rejection
    from alma.application.feed_monitors import sync_author_monitors
    from alma.application.followed_authors import apply_follow_state

    row = db.execute("SELECT * FROM author_merges WHERE id = ?", (merge_id,)).fetchone()
    if row is None:
        raise ValueError("merge not found")
    if row["undone_at"]:
        raise MergeRefused("This merge was already undone.")
    snapshot = json.loads(row["snapshot_json"])
    primary_id, alt_id = snapshot["primary_id"], snapshot["alt_id"]
    primary_oid, alt_oid = snapshot["primary_oid"], snapshot["alt_oid"]
    primary = db.execute("SELECT name, status FROM authors WHERE id = ?", (primary_id,)).fetchone()
    if primary is None or str(primary["status"] or "active") == "removed":
        raise MergeRefused(
            "The surviving author was merged or removed since. Undo that first."
        )

    # The hard ids first: the unique indexes hold them on one row at a time.
    _restore_primary_fields(db, snapshot)
    _restore_alt_row(db, snapshot)

    moved = snapshot["moved_papers"]
    for start in range(0, len(moved), 400):
        chunk = moved[start:start + 400]
        db.execute(
            f"UPDATE OR IGNORE publication_authors SET openalex_id = ? "  # noqa: S608
            f"WHERE lower(openalex_id) = lower(?) AND paper_id IN ({','.join('?' for _ in chunk)})",
            (alt_oid, primary_oid, *chunk),
        )
    for authorship in snapshot["dropped_authorships"]:
        columns = list(authorship)
        db.execute(
            f"INSERT OR IGNORE INTO publication_authors ({', '.join(columns)}) "  # noqa: S608
            f"VALUES ({', '.join('?' for _ in columns)})",
            list(authorship.values()),
        )

    db.execute(
        "DELETE FROM author_alt_identifiers WHERE primary_author_id = ? "
        "AND lower(alt_openalex_id) = lower(?)",
        (primary_id, alt_oid),
    )
    side = snapshot.get("moved") or {}
    if alt_id:
        for evidence_id in side.get("evidence", []):
            db.execute(
                "UPDATE OR IGNORE author_affiliation_evidence SET author_id = ? WHERE id = ?",
                (alt_id, evidence_id),
            )
        for alias_id in side.get("aliases", []):
            db.execute(
                "UPDATE OR IGNORE author_alt_identifiers SET primary_author_id = ? WHERE id = ?",
                (alt_id, alias_id),
            )
        for conflict_id in side.get("conflicts", []):
            db.execute(
                "UPDATE OR IGNORE author_merge_conflicts SET primary_author_id = ? WHERE id = ?",
                (alt_id, conflict_id),
            )
    for lo, hi in side.get("rejections", []):
        db.execute(
            "DELETE FROM author_merge_rejections WHERE author_id_lo = ? AND author_id_hi = ?",
            (lo, hi),
        )

    follow = snapshot["follow"]
    if follow and alt_id:
        apply_follow_state(db, alt_id, followed=True)
        db.execute(
            "UPDATE followed_authors SET followed_at = ?, notify_new_papers = ?, is_owner = ? "
            "WHERE author_id = ?",
            (follow["followed_at"], follow["notify_new_papers"], follow["is_owner"], alt_id),
        )
    for item in snapshot.get("feed_moved", []):
        db.execute(
            "UPDATE OR IGNORE feed_items SET author_id = ?, monitor_id = ?, monitor_label = ? WHERE id = ?",
            (item["author_id"], item["monitor_id"], item["monitor_label"], item["id"]),
        )
    for item in snapshot.get("feed_dropped", []):
        columns = list(item)
        db.execute(
            f"INSERT OR IGNORE INTO feed_items ({', '.join(columns)}) "  # noqa: S608
            f"VALUES ({', '.join('?' for _ in columns)})",
            list(item.values()),
        )
    sync_author_monitors(db)

    other = alt_id or alt_oid
    lo, hi = _pair(primary_id, other)
    record_merge_rejection(db, lo, hi, source="merge_undo", reason="merge undone")
    db.execute("DELETE FROM author_centroids WHERE lower(author_openalex_id) = lower(?)", (primary_oid,))
    db.execute(
        "UPDATE author_merges SET undone_at = ? WHERE id = ?", (utcnow().isoformat(), merge_id)
    )
    return {
        "merge_id": merge_id,
        "primary_author_id": primary_id,
        "alt_author_id": alt_id,
        "alt_openalex_id": alt_oid,
        "papers_returned": len(moved),
    }
