"""Corpus-wide paper-group reconciliation.

This service is the manual convergence pass behind the Health/Settings button.
It composes the journal-first group primitives instead of maintaining a second
dedup model: components are linked/purged, preprint twins collapse into the
journal paper when present, existing chains are flattened, and orphan child
state is stripped.

Caller owns the write transaction. Do not commit here — either wrap the whole call
in one write unit, or pass ``section=`` to scope a write unit to each PHASE (what
the background maintenance runner does, so an 8-minute pass no longer holds the
single SQLite writer end to end).
"""

from __future__ import annotations

import sqlite3
from collections.abc import Callable
from contextlib import AbstractContextManager, nullcontext
from typing import Any

from alma.application.preprint_dedup import (
    find_preprint_twin_candidates,
    merge_preprint_into_canonical,
)
from alma.core.components import backfill_components, count_linkable_orphan_components
from alma.core.paper_groups import (
    PaperGroupIntegrityError,
    absorb_paper_group,
    build_preprint_title_index,
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
    """Rows the backfill would newly CLASSIFY as components.

    Deliberately excludes already-classified orphans: those are the
    ``orphan_components`` integrity defect and are counted there (via the linkable
    subset), so counting them here too double-billed every orphan.
    """
    from alma.core.components import classify_component

    try:
        rows = conn.execute(
            """
            SELECT doi, work_type
            FROM papers
            WHERE component_type IS NULL
            """
        ).fetchall()
    except sqlite3.OperationalError:
        return 0
    pending = 0
    for row in rows:
        component_type, parent_doi = classify_component(row["doi"], row["work_type"])
        if component_type or parent_doi:
            pending += 1
    return pending


def count_paper_group_reconcile_candidates(conn: sqlite3.Connection) -> int:
    """Pending work for the group reconciliation operation — REPAIRABLE defects only.

    A count that includes defects this pass cannot fix never reaches zero, so the
    operation stays `readiness='ready'` forever and every maintenance cycle
    reschedules a run that repairs nothing. `orphan_components` is exactly that
    case: an orphan whose parent paper is absent from the corpus is terminal, so
    only the LINKABLE subset counts (`count_linkable_orphan_components`).
    """
    try:
        integrity = dict(relationship_integrity_counts(conn))
    except sqlite3.OperationalError:
        integrity = {}
    if integrity.get("orphan_components"):
        integrity["orphan_components"] = count_linkable_orphan_components(conn)
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
    """Let every standalone published paper absorb its preprint twins.

    The preprint index is built ONCE and handed to every call: this loop runs over
    the whole published corpus, and letting `promote_matching_preprints` do its own
    scan per row made the phase quadratic (see `PreprintTitleIndex`).
    """
    candidates = conn.execute(
        """
        SELECT id, doi, work_type, preprint_source, component_type
        FROM papers
        WHERE COALESCE(NULLIF(TRIM(canonical_paper_id), ''), '') = ''
          AND COALESCE(NULLIF(TRIM(parent_paper_id), ''), '') = ''
        """
    ).fetchall()
    preprint_index = build_preprint_title_index(conn)
    scanned = merged = reparented = 0
    for row in candidates:
        if is_component_row(row) or is_preprint_row(row):
            continue
        scanned += 1
        result = promote_matching_preprints(conn, str(row["id"]), preprint_index=preprint_index)
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
    section: Callable[[str], AbstractContextManager[Any]] | None = None,
    on_phase: Callable[[str, dict[str, int]], None] | None = None,
) -> dict[str, Any]:
    """Run an idempotent corpus-wide paper group reconciliation pass.

    ``section`` is an optional per-phase write scope, given the phase name. A caller
    that already owns an enclosing write transaction (the importer, the Settings
    route) omits it and gets the historical single-transaction behaviour; the
    background maintenance runner passes `write_section` so the writer gate is
    RELEASED between phases instead of being held for the whole pass. Write units
    never nest, so exactly one of the two owns the transaction.

    ``on_phase(name, counts)`` reports each phase as it finishes — this pass is long
    and used to log nothing at all between "started" and "completed".
    """
    scope = section if section is not None else (lambda _name: nullcontext())

    def run(name: str, phase: Callable[[], dict[str, int]]) -> dict[str, int]:
        with scope(name):
            counts = phase()
        if on_phase is not None:
            on_phase(name, counts)
        return counts

    before = relationship_integrity_counts(conn)
    dangling = run("dangling", lambda: _repair_dangling_relationships(conn))
    components = run("components", lambda: backfill_components(conn))
    normalized = run("normalize", lambda: _normalize_existing_groups(conn))
    twins = run("preprint_twins", lambda: _merge_preprint_twins(conn, limit=limit))
    promoted = run("journal_promotion", lambda: _promote_available_journals(conn))
    # A final normalize pass catches groups formed by the twin/promotion phases
    # and ensures every child points directly at the chosen root.
    final_normalized = run("normalize_final", lambda: _normalize_existing_groups(conn))
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
