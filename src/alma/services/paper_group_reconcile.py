"""Corpus-wide paper-group reconciliation.

This service is the manual convergence pass behind the Health/Settings button.
It composes the journal-first group primitives instead of maintaining a second
dedup model: components are linked/purged, preprint twins collapse into the
journal paper when present, existing chains are flattened, and orphan child
state is stripped.

The background and import runners pass a per-group write scope. Scans and match
planning happen outside that scope; one failing group rolls back independently.
Callers already inside a write unit omit the scope. No nested write units.
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

    rows = conn.execute(
        """
        SELECT doi, work_type
        FROM papers
        WHERE component_type IS NULL
        """
    ).fetchall()
    pending = 0
    for row in rows:
        component_type, parent_doi = classify_component(row["doi"], row["work_type"])
        if component_type or parent_doi:
            pending += 1
    return pending


#: How many ambiguous matches (and errors) a single run spells out. The counts
#: stay exact; only the per-item detail is sampled, so one bad corpus cannot
#: turn an Activity row into a megabyte of JSON.
_AMBIGUOUS_REPORT_LIMIT = 25


def count_paper_group_reconcile_candidates(conn: sqlite3.Connection) -> int:
    """Pending work for the group reconciliation operation — REPAIRABLE defects only.

    A count that includes defects this pass cannot fix never reaches zero, so the
    operation stays `readiness='ready'` forever and every maintenance cycle
    reschedules a run that repairs nothing. `orphan_components` is exactly that
    case: an orphan whose parent paper is absent from the corpus is terminal, so
    only the LINKABLE subset counts (`count_linkable_orphan_components`).
    """
    integrity = dict(relationship_integrity_counts(conn))
    if integrity.get("orphan_components"):
        integrity["orphan_components"] = count_linkable_orphan_components(conn)
    integrity.pop("ambiguous_preprints", None)  # review evidence, never auto-work
    preprint_twins = len(find_preprint_twin_candidates(conn, scope="corpus"))
    return _integrity_defect_total(integrity) + preprint_twins + _count_component_candidates(conn)


def _repair_dangling_relationships(conn: sqlite3.Connection, *, unit) -> dict[str, int]:
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
        def repair():
            conn.execute(
                "UPDATE papers SET canonical_paper_id = NULL, parent_paper_id = NULL WHERE id = ?",
                (pid,),
            )
            return purge_orphan_subordinate_state(conn, pid) if is_component_row(row) else None
        ok, cleaned = unit("dangling", pid, repair)
        if ok:
            if is_component_row(row):
                purged_orphans += int(cleaned or 0)
            else:
                repaired_versions += 1
    return {
        "dangling_versions_restored": repaired_versions,
        "dangling_orphan_sidecars_purged": purged_orphans,
    }


def _normalize_existing_groups(conn: sqlite3.Connection, *, unit) -> dict[str, int]:
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
        def normalize_group():
            try:
                return absorb_paper_group(conn, pid, target, reason="paper_group_reconcile")
            except PaperGroupIntegrityError:
                placeholders = ",".join("?" for _ in group_ids)
                if not placeholders:
                    return {"skipped": True}
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
                    purge_orphan_subordinate_state(conn, component_id)
                    changed += 1
                return {"rootless": changed, "skipped": not changed}
        ok, result = unit("normalize", pid, normalize_group)
        if not ok:
            continue
        if result.get("rootless"):
            rootless_groups += 1
            orphaned_components += result["rootless"]
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


def reconcile_paper_groups(
    conn: sqlite3.Connection,
    *,
    limit: int | None = None,
    section: Callable[[str], AbstractContextManager[Any]] | None = None,
    on_phase: Callable[[str, dict[str, Any]], None] | None = None,
) -> dict[str, Any]:
    """Repair through one plan, with independently atomic group writes.

    Health, Settings and post-import use this owner. `limit` bounds new title
    matches; classification and existing relationship repair scan the corpus.
    Ambiguous matches are reported separately and never count as automatic work.
    `on_phase` is called only outside a write scope, including error events.
    """
    scope = section or (lambda _name: nullcontext())
    errors: list[dict[str, str]] = []
    changes = 0

    def report(name, counts):
        if on_phase:
            on_phase(name, counts)

    def error(phase, paper_id, exc):
        detail = {"phase": phase, "paper_id": paper_id,
                  "cause": f"{type(exc).__name__}: {exc}",
                  "recovery": "Retry Reconcile paper groups; if it fails again, inspect this paper's versions."}
        errors.append(detail)
        report("error", detail)

    def unit(phase, paper_id, action):
        nonlocal changes
        try:
            with scope(f"{phase}:{paper_id}"):
                # A savepoint also protects callers already inside a write unit.
                conn.execute("SAVEPOINT paper_group_unit")
                before = conn.total_changes
                try:
                    result = action()
                except BaseException:
                    conn.execute("ROLLBACK TO paper_group_unit")
                    conn.execute("RELEASE paper_group_unit")
                    raise
                conn.execute("RELEASE paper_group_unit")
                changed = conn.total_changes - before
            changes += changed
            if changed:
                report("group", {"phase": phase, "paper_id": paper_id, "changed_rows": changed,
                                 "message": f"Repaired paper group {paper_id} ({phase})."})
            return True, result
        except Exception as exc:
            error(phase, paper_id, exc)
            return False, None

    report("scan", {"message": "Scanning paper relationships; Library remains available."})
    before = relationship_integrity_counts(conn)
    dangling = _repair_dangling_relationships(conn, unit=unit)
    report("dangling", dangling)
    # Classification plans are prepared outside the writer. Components are made
    # inert by the following group pass (and orphan pass) through the same owner.
    report("classify", {"message": "Classifying components and checking parent links."})
    components = backfill_components(
        conn, section=scope, normalize=False,
        on_error=lambda pid, exc: error("components", pid, exc),
    )
    changes += components["classified"] + components["linked"] + components["cleaned"]
    report("components", components)
    normalized = _normalize_existing_groups(conn, unit=unit)
    orphans = conn.execute(
        "SELECT id FROM papers WHERE component_type IS NOT NULL "
        "AND COALESCE(parent_paper_id, '') = '' AND COALESCE(canonical_paper_id, '') = ''"
    ).fetchall()
    for row in orphans:
        pid = str(row["id"])
        unit("orphans", pid, lambda: purge_orphan_subordinate_state(conn, pid))
    report("normalize", normalized)

    # Freeze the WHOLE candidate graph before any mutation: picking matches
    # after earlier merges can make an ambiguous component appear unique.
    report("match", {"message": "Checking complete title-match groups before merging."})
    pairs = build_preprint_title_index(conn).pairs(require_doi=True)
    ambiguous = [p for p in pairs if p["ambiguous"]]
    safe = [p for p in pairs if not p["ambiguous"]]
    selected = safe if limit is None else safe[:limit]
    merged = skipped = 0
    for pair in selected:
        ok, result = unit("preprint_twins", pair["preprint_id"], lambda: merge_preprint_into_canonical(
            conn, pair["preprint_id"], pair["canonical_id"]))
        if ok:
            skipped += int(bool(result.get("skipped")))
            merged += int(not result.get("skipped"))
    for pair in ambiguous[:_AMBIGUOUS_REPORT_LIMIT]:
        report("ambiguous", {"paper_id": pair["preprint_id"], "title": pair["title"],
                             "candidates": pair["canonical_candidates"],
                             "message": "Multiple plausible versions; left separate. Inspect identifiers before merging."})
    if len(ambiguous) > _AMBIGUOUS_REPORT_LIMIT:
        # Health's paper-group breakdown carries the full count; Activity gets a
        # readable sample rather than one line per pair on a large corpus.
        report("ambiguous", {
            "message": f"{len(ambiguous) - _AMBIGUOUS_REPORT_LIMIT} further ambiguous matches "
                       "left separate; see Health → paper groups for the full count.",
            "remaining": len(ambiguous) - _AMBIGUOUS_REPORT_LIMIT,
        })
    twins = {"preprint_candidates": len(safe), "preprint_twins_merged": merged,
             "preprint_twins_skipped": skipped, "ambiguous": len(ambiguous),
             "remaining": max(0, len(safe) - len(selected)), "limit": limit}
    report("preprint_twins", twins)
    after = relationship_integrity_counts(conn)
    message = (f"Paper groups: {merged} preprints merged; {normalized['groups_normalized']} groups repaired; "
               f"{len(ambiguous)} ambiguous (left separate); {len(errors)} failed; "
               f"{twins['remaining']} title matches remain. "
               f"Title-match limit: {limit if limit is not None else 'all'}; existing-group scan: corpus-wide.")
    # The result is stored on the Activity row: keep a sample, not a corpus dump.
    result = {"before": before, "after": after,
              "defects_before": _integrity_defect_total(before), "defects_after": _integrity_defect_total(after),
              "dangling": dangling, "components": components, "normalized": normalized,
              "preprints": twins, "changes": changes,
              "errors": errors[:_AMBIGUOUS_REPORT_LIMIT], "errors_total": len(errors),
              "ambiguous": ambiguous[:_AMBIGUOUS_REPORT_LIMIT], "ambiguous_total": len(ambiguous),
              "message": message}
    report("summary", {"message": message, "changes": changes, "errors": len(errors)})
    return result
