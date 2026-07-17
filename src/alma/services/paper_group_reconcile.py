"""Corpus-wide paper-group reconciliation.

This service is the manual convergence pass behind the Health/Settings button.
It composes the journal-first group primitives instead of maintaining a second
dedup model: components are linked/purged, preprint twins collapse into the
journal paper when present, existing chains are flattened, and orphan child
state is stripped.

Caller owns the write transaction. Do not commit here.
"""

from __future__ import annotations

import sqlite3
from typing import Any

from alma.application.preprint_dedup import (
    find_preprint_twin_candidates,
    merge_preprint_into_canonical,
)
from alma.core.components import backfill_components
from alma.core.paper_groups import (
    PaperGroupIntegrityError,
    absorb_paper_group,
    collect_paper_group_ids,
    is_component_row,
    is_preprint_row,
    promote_matching_preprints,
    purge_orphan_subordinate_state,
    relationship_integrity_counts,
)


def _integrity_defect_total(counts: dict[str, int]) -> int:
    return sum(max(0, int(value or 0)) for value in counts.values())


def _count_component_candidates(conn: sqlite3.Connection) -> int:
    from alma.core.components import classify_component

    try:
        rows = conn.execute(
            """
            SELECT doi, work_type, component_type, parent_paper_id
            FROM papers
            """
        ).fetchall()
    except sqlite3.OperationalError:
        return 0
    pending = 0
    for row in rows:
        if str(row["component_type"] or "").strip():
            if not str(row["parent_paper_id"] or "").strip():
                pending += 1
            continue
        component_type, parent_doi = classify_component(row["doi"], row["work_type"])
        if component_type or parent_doi:
            pending += 1
    return pending


def count_paper_group_reconcile_candidates(conn: sqlite3.Connection) -> int:
    """Approximate pending work for the manual group reconciliation operation."""
    try:
        integrity = relationship_integrity_counts(conn)
    except sqlite3.OperationalError:
        integrity = {}
    try:
        preprint_twins = len(find_preprint_twin_candidates(conn, scope="corpus"))
    except Exception:
        preprint_twins = 0
    return _integrity_defect_total(integrity) + preprint_twins + _count_component_candidates(conn)


def _repair_dangling_relationships(conn: sqlite3.Connection) -> dict[str, int]:
    """Handle links whose target row no longer exists.

    A component with a missing parent remains an inert orphan and has app state
    purged. A root-capable version/preprint with a dangling canonical pointer is
    restored as a standalone candidate because the target paper is not present.
    """
    repaired_versions = purged_orphans = 0
    rows = conn.execute(
        """
        SELECT p.*
        FROM papers p
        LEFT JOIN papers canonical ON canonical.id = p.canonical_paper_id
        LEFT JOIN papers parent ON parent.id = p.parent_paper_id
        WHERE (
            COALESCE(NULLIF(TRIM(p.canonical_paper_id), ''), '') != ''
            AND canonical.id IS NULL
        ) OR (
            COALESCE(NULLIF(TRIM(p.parent_paper_id), ''), '') != ''
            AND parent.id IS NULL
        )
        """
    ).fetchall()
    for row in rows:
        pid = str(row["id"])
        if is_component_row(row):
            conn.execute(
                "UPDATE papers SET canonical_paper_id = NULL, parent_paper_id = NULL WHERE id = ?",
                (pid,),
            )
            purged_orphans += purge_orphan_subordinate_state(conn, pid)
        else:
            conn.execute(
                "UPDATE papers SET canonical_paper_id = NULL, parent_paper_id = NULL WHERE id = ?",
                (pid,),
            )
            repaired_versions += 1
    return {
        "dangling_versions_restored": repaired_versions,
        "dangling_orphan_sidecars_purged": purged_orphans,
    }


def _normalize_existing_groups(conn: sqlite3.Connection) -> dict[str, int]:
    groups_normalized = reparented = cleaned_sidecars = journal_promotions = 0
    rootless_groups = orphaned_components = 0
    seen_groups: set[frozenset[str]] = set()
    rows = conn.execute(
        """
        SELECT id, canonical_paper_id, parent_paper_id
        FROM papers
        WHERE COALESCE(NULLIF(TRIM(canonical_paper_id), ''), '') != ''
           OR COALESCE(NULLIF(TRIM(parent_paper_id), ''), '') != ''
        """
    ).fetchall()
    for row in rows:
        pid = str(row["id"])
        target = str(row["canonical_paper_id"] or row["parent_paper_id"] or "").strip()
        if not target:
            continue
        group_ids = collect_paper_group_ids(conn, pid, target)
        group_key = frozenset(group_ids)
        if group_key in seen_groups:
            continue
        seen_groups.add(group_key)
        try:
            result = absorb_paper_group(conn, pid, target, reason="paper_group_reconcile")
        except PaperGroupIntegrityError:
            placeholders = ",".join("?" for _ in group_ids)
            if not placeholders:
                continue
            group_rows = conn.execute(
                f"SELECT * FROM papers WHERE id IN ({placeholders})",
                sorted(group_ids),
            ).fetchall()
            changed = 0
            for group_row in group_rows:
                if not is_component_row(group_row):
                    continue
                component_id = str(group_row["id"])
                conn.execute(
                    "UPDATE papers SET canonical_paper_id = NULL, parent_paper_id = NULL "
                    "WHERE id = ?",
                    (component_id,),
                )
                cleaned_sidecars += purge_orphan_subordinate_state(conn, component_id)
                changed += 1
            if changed:
                rootless_groups += 1
                orphaned_components += changed
            continue
        if result.get("skipped"):
            continue
        groups_normalized += 1
        reparented += int(result.get("reparented") or 0)
        cleaned_sidecars += int(result.get("cleaned_sidecars") or 0)
        if result.get("journal_promoted"):
            journal_promotions += 1
    return {
        "groups_normalized": groups_normalized,
        "reparented": reparented,
        "cleaned_sidecars": cleaned_sidecars,
        "journal_promotions": journal_promotions,
        "rootless_groups": rootless_groups,
        "orphaned_components": orphaned_components,
    }


def _promote_available_journals(conn: sqlite3.Connection) -> dict[str, int]:
    candidates = conn.execute(
        """
        SELECT id, doi, work_type, preprint_source, component_type
        FROM papers
        WHERE COALESCE(NULLIF(TRIM(canonical_paper_id), ''), '') = ''
          AND COALESCE(NULLIF(TRIM(parent_paper_id), ''), '') = ''
        """
    ).fetchall()
    scanned = merged = reparented = 0
    for row in candidates:
        if is_component_row(row) or is_preprint_row(row):
            continue
        scanned += 1
        result = promote_matching_preprints(conn, str(row["id"]))
        merged += int(result.get("merged") or 0)
        reparented += int(result.get("reparented") or 0)
    return {
        "published_scanned": scanned,
        "preprints_promoted": merged,
        "preprint_children_reparented": reparented,
    }


def _merge_preprint_twins(conn: sqlite3.Connection, *, limit: int | None = None) -> dict[str, int]:
    try:
        candidates = find_preprint_twin_candidates(conn, limit=limit, scope="corpus")
    except Exception:
        candidates = []
    merged = skipped = errors = journal_promotions = 0
    for pair in candidates:
        try:
            result = merge_preprint_into_canonical(
                conn,
                str(pair["preprint_id"]),
                str(pair["canonical_id"]),
            )
            if result.get("skipped"):
                skipped += 1
            else:
                merged += 1
                if result.get("journal_promoted"):
                    journal_promotions += 1
        except Exception:
            errors += 1
    return {
        "preprint_candidates": len(candidates),
        "preprint_twins_merged": merged,
        "preprint_twins_skipped": skipped,
        "preprint_twin_errors": errors,
        "journal_promotions": journal_promotions,
    }


def reconcile_paper_groups(
    conn: sqlite3.Connection,
    *,
    limit: int | None = None,
) -> dict[str, Any]:
    """Run an idempotent corpus-wide paper group reconciliation pass."""
    before = relationship_integrity_counts(conn)
    dangling = _repair_dangling_relationships(conn)
    components = backfill_components(conn)
    normalized = _normalize_existing_groups(conn)
    twins = _merge_preprint_twins(conn, limit=limit)
    promoted = _promote_available_journals(conn)
    # A final normalize pass catches groups formed by the twin/promotion phases
    # and ensures every child points directly at the chosen root.
    final_normalized = _normalize_existing_groups(conn)
    after = relationship_integrity_counts(conn)
    return {
        "before": before,
        "after": after,
        "defects_before": _integrity_defect_total(before),
        "defects_after": _integrity_defect_total(after),
        "dangling": dangling,
        "components": components,
        "normalized": normalized,
        "preprints": twins,
        "promotions": promoted,
        "final_normalized": final_normalized,
    }
