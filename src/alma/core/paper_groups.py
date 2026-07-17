"""Journal-first paper-group identity and consolidation.

One logical work has one first-class root.  Same-work/version rows use
``canonical_paper_id``; part-of rows use ``parent_paper_id`` plus
``component_type``.  The axes stay distinct, but both always terminate at the
same direct standalone root.  This module owns root selection and the atomic
rewrite used by ingest, duplicate collapse, and corpus reconciliation.
"""

from __future__ import annotations

import logging
import sqlite3
from collections.abc import Iterable, Mapping
from datetime import datetime
from typing import Any

from alma.core.utils import normalize_title_key

logger = logging.getLogger(__name__)


PREPRINT_DOI_PREFIXES: dict[str, str] = {
    "10.48550/arxiv": "arxiv",
    "10.1101/": "biorxiv",
    "10.31234/": "psyrxiv",
    "10.31219/": "osf",
    "10.26434/chemrxiv": "chemrxiv",
    "10.20944/preprints": "mdpi_preprints",
}

_PREPRINT_WORK_TYPES = frozenset({"preprint", "posted-content", "posted_content"})
_COMPONENT_WORK_TYPES = frozenset(
    {"dataset", "peer-review", "supplementary-materials", "paratext"}
)

# Rows from a preprint are useful on the published paper. Other subordinate rows
# are inert pointers and must not donate behavior or metadata to the root.
_PREPRINT_TO_PAPER_MIGRATE_TABLES = (
    "publication_authors",
    "publication_topics",
    "publication_references",
    "publication_embeddings",
    "publication_tags",
    "publication_institutions",
    "collection_items",
    "tag_suggestions",
    "recommendations",
    "feed_items",
    "lens_signals",
    "alerted_publications",
    "alert_history",
)

_ALL_PAPER_SIDECAR_TABLES = frozenset(
    {
        *_PREPRINT_TO_PAPER_MIGRATE_TABLES,
        "publication_clusters",
        "publication_embedding_fetch_status",
        "paper_enrichment_status",
        "paper_network_cache",
    }
)


class PaperGroupIntegrityError(ValueError):
    """A relationship cannot be followed safely (cycle/dangling/wrong root)."""


def _value(row: Mapping[str, Any] | sqlite3.Row, key: str) -> Any:
    try:
        return row[key]
    except (KeyError, IndexError):
        return None


def classify_preprint_source(
    doi: str | None,
    *,
    preprint_source: str | None = None,
    work_type: str | None = None,
) -> str | None:
    """Return a preprint source using persisted evidence before DOI heuristics."""
    persisted = str(preprint_source or "").strip().lower()
    if persisted:
        return persisted
    wt = str(work_type or "").strip().lower()
    if wt in _PREPRINT_WORK_TYPES:
        return wt.replace("_", "-")
    lowered = str(doi or "").strip().lower()
    for prefix, source in PREPRINT_DOI_PREFIXES.items():
        if lowered.startswith(prefix):
            return source
    return None


def is_preprint_row(row: Mapping[str, Any] | sqlite3.Row) -> bool:
    return bool(
        classify_preprint_source(
            _value(row, "doi"),
            preprint_source=_value(row, "preprint_source"),
            work_type=_value(row, "work_type"),
        )
    )


def is_component_row(row: Mapping[str, Any] | sqlite3.Row) -> bool:
    if str(_value(row, "component_type") or "").strip():
        return True
    if classify_preprint_source(
        _value(row, "doi"),
        preprint_source=_value(row, "preprint_source"),
        work_type=_value(row, "work_type"),
    ):
        return False
    return str(_value(row, "work_type") or "").strip().lower() in _COMPONENT_WORK_TYPES


def _root_rank(row: Mapping[str, Any] | sqlite3.Row) -> int:
    """Published/regular papers always outrank preprints; components never root."""
    if is_component_row(row):
        return 0
    if is_preprint_row(row):
        return 100
    return 300


def resolve_paper_root_id(
    conn: sqlite3.Connection,
    paper_id: str,
    *,
    strict: bool = True,
) -> str:
    """Follow both relationship axes to a root with dangling/cycle protection."""
    current = str(paper_id or "").strip()
    if not current:
        return current
    seen: set[str] = set()
    while current:
        if current in seen:
            if strict:
                raise PaperGroupIntegrityError(f"paper relationship cycle at {current}")
            return str(paper_id)
        seen.add(current)
        row = conn.execute(
            "SELECT id, canonical_paper_id, parent_paper_id, component_type "
            "FROM papers WHERE id = ?",
            (current,),
        ).fetchone()
        if row is None:
            if strict:
                raise PaperGroupIntegrityError(f"dangling paper relationship: {current}")
            return str(paper_id)
        canonical = str(row["canonical_paper_id"] or "").strip()
        parent = str(row["parent_paper_id"] or "").strip()
        next_id = canonical or parent
        if not next_id:
            return current
        current = next_id
    return str(paper_id)


def resolve_action_paper_id(conn: sqlite3.Connection, paper_id: str) -> str | None:
    """Resolve a UI/action id to a standalone root; reject an orphan component."""
    root_id = resolve_paper_root_id(conn, paper_id, strict=False)
    row = conn.execute(
        "SELECT canonical_paper_id, parent_paper_id, component_type "
        "FROM papers WHERE id = ?",
        (root_id,),
    ).fetchone()
    if row is None:
        return None
    if str(row["canonical_paper_id"] or "").strip():
        return None
    if str(row["parent_paper_id"] or "").strip():
        return None
    if str(row["component_type"] or "").strip():
        # A linked component would have resolved through parent_paper_id.  What
        # remains is an orphan and must not become independently actionable.
        return None
    return root_id


def _paper_rows(conn: sqlite3.Connection, ids: Iterable[str]) -> dict[str, sqlite3.Row]:
    normalized = sorted({str(pid).strip() for pid in ids if str(pid).strip()})
    if not normalized:
        return {}
    placeholders = ",".join("?" for _ in normalized)
    rows = conn.execute(
        f"SELECT * FROM papers WHERE id IN ({placeholders})", normalized
    ).fetchall()
    return {str(row["id"]): row for row in rows}


def collect_paper_group_ids(conn: sqlite3.Connection, *paper_ids: str) -> set[str]:
    """Return the undirected connected component around one or more paper ids."""
    found = {str(pid).strip() for pid in paper_ids if str(pid).strip()}
    frontier = set(found)
    while frontier:
        rows = _paper_rows(conn, frontier)
        outgoing: set[str] = set()
        for row in rows.values():
            for key in ("canonical_paper_id", "parent_paper_id"):
                target = str(row[key] or "").strip()
                if target and target not in found:
                    outgoing.add(target)
        placeholders = ",".join("?" for _ in frontier)
        incoming: set[str] = set()
        if placeholders:
            inbound_rows = conn.execute(
                f"SELECT id FROM papers WHERE canonical_paper_id IN ({placeholders}) "
                f"OR parent_paper_id IN ({placeholders})",
                [*frontier, *frontier],
            ).fetchall()
            incoming = {str(row["id"]) for row in inbound_rows if str(row["id"]) not in found}
        frontier = (outgoing | incoming) - found
        found.update(frontier)
    return found


def choose_paper_group_root(
    rows: Mapping[str, sqlite3.Row],
    *,
    preferred_id: str | None = None,
) -> str:
    """Select a deterministic root; caller preference only breaks equal-rank ties."""
    candidates = [row for row in rows.values() if _root_rank(row) > 0]
    if not candidates:
        raise PaperGroupIntegrityError("paper group contains no root-capable paper")

    def key(row: sqlite3.Row) -> tuple[int, int, int, str]:
        pid = str(row["id"])
        standalone = not str(row["canonical_paper_id"] or "").strip() and not str(
            row["parent_paper_id"] or ""
        ).strip()
        return (
            _root_rank(row),
            1 if pid == preferred_id else 0,
            1 if standalone else 0,
            pid,
        )

    return str(max(candidates, key=key)["id"])


def _table_has_paper_id(conn: sqlite3.Connection, table: str) -> bool:
    try:
        return any(str(row[1]) == "paper_id" for row in conn.execute(f"PRAGMA table_info({table})"))
    except sqlite3.OperationalError:
        return False


def _repoint_table(
    conn: sqlite3.Connection, table: str, loser_id: str, root_id: str
) -> int:
    if not _table_has_paper_id(conn, table):
        return 0
    try:
        conn.execute(
            f"UPDATE OR IGNORE {table} SET paper_id = ? WHERE paper_id = ?",
            (root_id, loser_id),
        )
        deleted = conn.execute(
            f"DELETE FROM {table} WHERE paper_id = ?", (loser_id,)
        ).rowcount
        return max(0, int(deleted or 0))
    except sqlite3.OperationalError as exc:
        logger.debug("paper-group repoint skipped on %s: %s", table, exc)
        return 0


def _delete_table_rows(conn: sqlite3.Connection, table: str, paper_id: str) -> int:
    if not _table_has_paper_id(conn, table):
        return 0
    try:
        return max(
            0,
            int(
                conn.execute(
                    f"DELETE FROM {table} WHERE paper_id = ?", (paper_id,)
                ).rowcount
                or 0
            ),
        )
    except sqlite3.OperationalError:
        return 0


def _merge_feedback(conn: sqlite3.Connection, loser_id: str, root_id: str) -> int:
    try:
        cursor = conn.execute(
            "UPDATE feedback_events SET entity_id = ? "
            "WHERE entity_type IN ('publication', 'paper') AND entity_id = ?",
            (root_id, loser_id),
        )
        return max(0, int(cursor.rowcount or 0))
    except sqlite3.OperationalError:
        return 0


def _merge_preference_profile(conn: sqlite3.Connection, loser_id: str, root_id: str) -> int:
    try:
        loser = conn.execute(
            "SELECT affinity_weight, confidence, interaction_count, last_updated "
            "FROM preference_profiles WHERE entity_type IN ('publication', 'paper') "
            "AND entity_id = ? ORDER BY CASE entity_type WHEN 'publication' THEN 0 ELSE 1 END LIMIT 1",
            (loser_id,),
        ).fetchone()
        if loser is None:
            return 0
        root = conn.execute(
            "SELECT affinity_weight, confidence, interaction_count, last_updated "
            "FROM preference_profiles WHERE entity_type = 'publication' AND entity_id = ?",
            (root_id,),
        ).fetchone()
        loser_n = max(0, int(loser["interaction_count"] or 0))
        root_n = max(0, int(root["interaction_count"] or 0)) if root else 0
        total = loser_n + root_n
        if total:
            affinity = (
                float(loser["affinity_weight"] or 0.0) * loser_n
                + (float(root["affinity_weight"] or 0.0) * root_n if root else 0.0)
            ) / total
        else:
            affinity = max(
                float(loser["affinity_weight"] or 0.0),
                float(root["affinity_weight"] or 0.0) if root else 0.0,
                key=abs,
            )
        confidence = max(
            float(loser["confidence"] or 0.0),
            float(root["confidence"] or 0.0) if root else 0.0,
        )
        last_updated = max(
            str(loser["last_updated"] or ""),
            str(root["last_updated"] or "") if root else "",
        ) or datetime.utcnow().isoformat()
        conn.execute(
            """
            INSERT INTO preference_profiles
                (entity_type, entity_id, affinity_weight, confidence,
                 interaction_count, last_updated)
            VALUES ('publication', ?, ?, ?, ?, ?)
            ON CONFLICT(entity_type, entity_id) DO UPDATE SET
                affinity_weight=excluded.affinity_weight,
                confidence=excluded.confidence,
                interaction_count=excluded.interaction_count,
                last_updated=excluded.last_updated
            """,
            (root_id, affinity, confidence, total, last_updated),
        )
        conn.execute(
            "DELETE FROM preference_profiles WHERE entity_type IN ('publication', 'paper') "
            "AND entity_id = ?",
            (loser_id,),
        )
        return 1
    except sqlite3.OperationalError:
        return 0


def _absorbs_into_root(row: Mapping[str, Any] | sqlite3.Row, root: Mapping[str, Any] | sqlite3.Row) -> bool:
    """Only preprint -> published-paper promotion absorbs child state."""
    return is_preprint_row(row) and not is_component_row(row) and not is_preprint_row(root)


def _upgrade_root_scalars(
    conn: sqlite3.Connection, loser: sqlite3.Row, root: sqlite3.Row
) -> None:
    """Consolidate preprint evidence/user state into a published root."""
    columns = {str(row[1]) for row in conn.execute("PRAGMA table_info(papers)")}
    root_id = str(root["id"])
    live_root = conn.execute("SELECT * FROM papers WHERE id = ?", (root_id,)).fetchone() or root
    updates: dict[str, Any] = {}
    if not _absorbs_into_root(loser, root):
        return
    fill_fields: list[str] = []
    fill_fields.extend(
        [
            "authors",
            "abstract",
            "keywords",
            "tldr",
            "notes",
            "added_at",
            "added_from",
            "reading_status",
        ]
    )
    for field in fill_fields:
        if field not in columns:
            continue
        root_value = _value(live_root, field)
        loser_value = _value(loser, field)
        if (root_value is None or str(root_value).strip() == "") and loser_value not in (None, ""):
            updates[field] = loser_value
    for field in ("cited_by_count", "influential_citation_count", "rating"):
        if field in columns:
            updates[field] = max(
                int(_value(live_root, field) or 0),
                int(_value(loser, field) or 0),
            )
    if "status" in columns and str(_value(loser, "status") or "") == "library":
        updates["status"] = "library"
    if not updates:
        return
    updates["updated_at"] = datetime.utcnow().isoformat()
    assignments = ", ".join(f"{field} = ?" for field in updates)
    conn.execute(
        f"UPDATE papers SET {assignments} WHERE id = ?",
        [*updates.values(), root_id],
    )


def _invalidate_group_caches(conn: sqlite3.Connection, root_id: str) -> int:
    cleaned = _delete_table_rows(conn, "paper_network_cache", root_id)
    try:
        cleaned += max(0, int(conn.execute("DELETE FROM similarity_cache").rowcount or 0))
    except sqlite3.OperationalError:
        pass
    # The relationship rewrite touches papers.updated_at, so registered view
    # fingerprints become stale. Variant graph rows use caller fingerprints;
    # clearing them prevents a direct cache hit between rewrite and next read.
    try:
        cleaned += max(
            0,
            int(
                conn.execute(
                    "DELETE FROM materialized_views WHERE view_key LIKE 'graph:%:v=%'"
                ).rowcount
                or 0
            ),
        )
    except sqlite3.OperationalError:
        pass
    return cleaned


def _group_is_normalized(
    conn: sqlite3.Connection, rows: Mapping[str, sqlite3.Row], root_id: str
) -> bool:
    root = rows[root_id]
    if (
        str(root["canonical_paper_id"] or "").strip()
        or str(root["parent_paper_id"] or "").strip()
        or str(root["component_type"] or "").strip()
    ):
        return False
    subordinate_ids: list[str] = []
    for pid, row in rows.items():
        if pid == root_id:
            continue
        subordinate_ids.append(pid)
        if is_component_row(row):
            if str(row["parent_paper_id"] or "").strip() != root_id:
                return False
            if str(row["canonical_paper_id"] or "").strip():
                return False
        else:
            if str(row["canonical_paper_id"] or "").strip() != root_id:
                return False
            if str(row["parent_paper_id"] or "").strip():
                return False
    if not subordinate_ids:
        return False
    placeholders = ",".join("?" for _ in subordinate_ids)
    for table in _ALL_PAPER_SIDECAR_TABLES:
        if not _table_has_paper_id(conn, table):
            continue
        try:
            if conn.execute(
                f"SELECT 1 FROM {table} WHERE paper_id IN ({placeholders}) LIMIT 1",
                subordinate_ids,
            ).fetchone():
                return False
        except sqlite3.OperationalError:
            continue
    try:
        if conn.execute(
            f"SELECT 1 FROM feedback_events WHERE entity_type IN ('publication', 'paper') "
            f"AND entity_id IN ({placeholders}) LIMIT 1",
            subordinate_ids,
        ).fetchone():
            return False
    except sqlite3.OperationalError:
        pass
    try:
        if conn.execute(
            f"SELECT 1 FROM preference_profiles WHERE entity_type IN ('publication', 'paper') "
            f"AND entity_id IN ({placeholders}) LIMIT 1",
            subordinate_ids,
        ).fetchone():
            return False
    except sqlite3.OperationalError:
        pass
    return True


def purge_orphan_subordinate_state(conn: sqlite3.Connection, paper_id: str) -> int:
    """Strip every app sidecar from an unlinked subordinate row.

    There is no root to receive the state yet.  Pointer metadata stays on the
    ``papers`` row; everything that could make it independently interactive is
    removed.  A later authoritative parent link starts from this inert state.
    """
    cleaned = 0
    for table in _ALL_PAPER_SIDECAR_TABLES:
        cleaned += _delete_table_rows(conn, table, paper_id)
    try:
        cleaned += max(
            0,
            int(
                conn.execute(
                    "DELETE FROM feedback_events WHERE entity_type IN ('publication', 'paper') "
                    "AND entity_id = ?",
                    (paper_id,),
                ).rowcount
                or 0
            ),
        )
    except sqlite3.OperationalError:
        pass
    try:
        cleaned += max(
            0,
            int(
                conn.execute(
                    "DELETE FROM preference_profiles WHERE entity_type IN ('publication', 'paper') "
                    "AND entity_id = ?",
                    (paper_id,),
                ).rowcount
                or 0
            ),
        )
    except sqlite3.OperationalError:
        pass
    cleaned += _invalidate_group_caches(conn, paper_id)
    return cleaned


def absorb_paper_group(
    conn: sqlite3.Connection,
    loser_id: str,
    keeper_id: str,
    *,
    reason: str | None = None,
) -> dict[str, Any]:
    """Atomically normalize the connected group containing a duplicate pair.

    ``keeper_id`` is a tie-break only.  A published paper always overrides a
    preprint keeper; a component can never win.  The caller owns the surrounding
    transaction/write gate.
    """
    if not loser_id or not keeper_id or loser_id == keeper_id:
        return {"skipped": True, "reason": "same_id"}
    group_ids = collect_paper_group_ids(conn, loser_id, keeper_id)
    rows = _paper_rows(conn, group_ids)
    if loser_id not in rows:
        return {"skipped": True, "reason": "loser_missing"}
    if keeper_id not in rows:
        return {"skipped": True, "reason": "keeper_missing"}
    root_id = choose_paper_group_root(rows, preferred_id=keeper_id)
    root = rows[root_id]
    if _group_is_normalized(conn, rows, root_id):
        return {
            "skipped": True,
            "reason": "already_merged",
        }
    now = datetime.utcnow().isoformat()
    migrated: dict[str, int] = {}
    cleaned = feedback_migrated = preferences_migrated = 0
    journal_promoted = root_id != keeper_id and _root_rank(root) > _root_rank(rows[keeper_id])

    for pid, row in rows.items():
        if pid == root_id:
            continue
        absorbs = _absorbs_into_root(row, root)
        _upgrade_root_scalars(conn, row, root)
        if not absorbs:
            cleaned += purge_orphan_subordinate_state(conn, pid)
            continue
        tables = _PREPRINT_TO_PAPER_MIGRATE_TABLES
        for table in tables:
            count = _repoint_table(conn, table, pid, root_id)
            migrated[table] = migrated.get(table, 0) + count
        feedback_migrated += _merge_feedback(conn, pid, root_id)
        preferences_migrated += _merge_preference_profile(conn, pid, root_id)
        # Anything not deliberately migrated must not remain on an inert child.
        for table in _ALL_PAPER_SIDECAR_TABLES - set(tables):
            cleaned += _delete_table_rows(conn, table, pid)

    # Root identity first, then flatten every member directly to it.
    conn.execute(
        "UPDATE papers SET canonical_paper_id = NULL, parent_paper_id = NULL, "
        "component_type = NULL, updated_at = ? WHERE id = ?",
        (now, root_id),
    )
    versions = components = reparented = 0
    for pid, row in rows.items():
        if pid == root_id:
            continue
        if is_component_row(row):
            old_parent = str(row["parent_paper_id"] or "").strip()
            conn.execute(
                "UPDATE papers SET canonical_paper_id = NULL, parent_paper_id = ?, "
                "updated_at = ? WHERE id = ?",
                (root_id, now, pid),
            )
            components += 1
            if old_parent != root_id:
                reparented += 1
        else:
            conn.execute(
                "UPDATE papers SET canonical_paper_id = ?, parent_paper_id = NULL, "
                "component_type = NULL, "
                "openalex_resolution_reason = CASE "
                "WHEN ? <> '' AND COALESCE(TRIM(openalex_resolution_reason), '') = '' "
                "THEN ? ELSE openalex_resolution_reason END, updated_at = ? WHERE id = ?",
                (root_id, reason or "", reason or "", now, pid),
            )
            versions += 1

    # Recompute all root-derived state after its evidence/user state changed.
    cleaned += _invalidate_group_caches(conn, root_id)
    return {
        "skipped": False,
        "loser_id": loser_id,
        "keeper_id": keeper_id,
        "root_id": root_id,
        "journal_promoted": journal_promoted,
        "versions": versions,
        "components": components,
        "reparented": reparented,
        "fk_migrated": migrated,
        "feedback_migrated": feedback_migrated,
        "preferences_migrated": preferences_migrated,
        "cleaned_sidecars": cleaned,
    }


def relationship_integrity_counts(conn: sqlite3.Connection) -> dict[str, int]:
    """Return relationship defects used by Health and reconciliation previews."""
    counts = {
        "dangling_canonical": 0,
        "dangling_parent": 0,
        "self_links": 0,
        "cycles": 0,
        "chains": 0,
        "component_roots": 0,
        "published_under_preprint": 0,
        "orphan_components": 0,
        "subordinate_sidecars": 0,
    }
    rows = conn.execute(
        "SELECT id, doi, work_type, preprint_source, canonical_paper_id, "
        "parent_paper_id, component_type FROM papers"
    ).fetchall()
    by_id = {str(row["id"]): row for row in rows}
    for row in rows:
        pid = str(row["id"])
        canonical = str(row["canonical_paper_id"] or "").strip()
        parent = str(row["parent_paper_id"] or "").strip()
        if canonical and canonical not in by_id:
            counts["dangling_canonical"] += 1
        if parent and parent not in by_id:
            counts["dangling_parent"] += 1
        if pid in {canonical, parent}:
            counts["self_links"] += 1
        target = by_id.get(canonical or parent)
        if target is not None:
            if str(target["canonical_paper_id"] or "").strip() or str(
                target["parent_paper_id"] or ""
            ).strip():
                counts["chains"] += 1
            if is_component_row(target):
                counts["component_roots"] += 1
            if canonical and not is_preprint_row(row) and is_preprint_row(target):
                counts["published_under_preprint"] += 1
        if is_component_row(row) and not parent:
            counts["orphan_components"] += 1
        try:
            resolve_paper_root_id(conn, pid)
        except PaperGroupIntegrityError as exc:
            if "cycle" in str(exc):
                counts["cycles"] += 1

    subordinate_ids = [
        str(row["id"])
        for row in rows
        if str(row["canonical_paper_id"] or "").strip() or is_component_row(row)
    ]
    for table in _ALL_PAPER_SIDECAR_TABLES:
        if not subordinate_ids or not _table_has_paper_id(conn, table):
            continue
        placeholders = ",".join("?" for _ in subordinate_ids)
        try:
            counts["subordinate_sidecars"] += int(
                conn.execute(
                    f"SELECT COUNT(*) FROM {table} WHERE paper_id IN ({placeholders})",
                    subordinate_ids,
                ).fetchone()[0]
                or 0
            )
        except sqlite3.OperationalError:
            continue
    return counts


def promote_matching_preprints(
    conn: sqlite3.Connection,
    published_paper_id: str,
    *,
    year_tolerance: int = 2,
) -> dict[str, int]:
    """Promote a newly-arrived published row over exact-title preprint twins.

    This is the cheap ingest-time path.  Corpus reconciliation adds semantic
    candidate detection; the foreground write only uses the high-precision
    normalized-title + year rule and persisted/DOI preprint evidence.
    """
    published = conn.execute(
        "SELECT id, title, year, doi, work_type, preprint_source, component_type "
        "FROM papers WHERE id = ?",
        (published_paper_id,),
    ).fetchone()
    if published is None or is_component_row(published) or is_preprint_row(published):
        return {"candidates": 0, "merged": 0, "reparented": 0}
    title_key = normalize_title_key(str(published["title"] or ""))
    if not title_key:
        return {"candidates": 0, "merged": 0, "reparented": 0}
    year = published["year"]
    candidates = conn.execute(
        """
        SELECT id, title, year, doi, work_type, preprint_source, component_type
        FROM papers
        WHERE id != ?
          AND COALESCE(TRIM(component_type), '') = ''
        """,
        (published_paper_id,),
    ).fetchall()
    matches: list[str] = []
    for row in candidates:
        if not is_preprint_row(row):
            continue
        if normalize_title_key(str(row["title"] or "")) != title_key:
            continue
        if year is not None and row["year"] is not None:
            if abs(int(year) - int(row["year"])) > max(0, int(year_tolerance)):
                continue
        matches.append(str(row["id"]))
    merged = reparented = 0
    for preprint_id in matches:
        result = absorb_paper_group(
            conn,
            preprint_id,
            published_paper_id,
            reason="journal_publication_promotion",
        )
        if not result.get("skipped"):
            merged += 1
            reparented += int(result.get("reparented") or 0)
    return {"candidates": len(matches), "merged": merged, "reparented": reparented}
