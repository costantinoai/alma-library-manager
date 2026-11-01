"""One-time repairs of what older code left behind, previewed before they run.

`plan` writes nothing and returns the counts. `apply` runs the same steps, in
order, then the ordinary reconcile pass, which owns every paper merge. Nothing
calls this at startup. Run it on a database at the current schema, after reading
the counts:

    python -m alma.services.merge_repairs --db /path/scholar.db            # counts only
    python -m alma.services.merge_repairs --db /path/scholar.db --list list.csv
    python -m alma.services.merge_repairs --db /path/scholar.db --apply
"""

from __future__ import annotations

import argparse
import csv
import difflib
import json
import logging
import re
import sqlite3
import sys
import tempfile
from pathlib import Path
from typing import Any

from alma.application.feed_monitors import sync_author_monitors
from alma.core.author_identity import merged_author_redirects
from alma.core.components import classify_component
from alma.core.sqlite_config import apply_busy_timeout
from alma.core.paper_groups import (
    _MEMBERSHIP_RANK,
    _strongest,
    better_title,
    not_component_ids,
    paper_group_defect_map,
    resolve_paper_root_id,
)
from alma.core.time import utcnow
from alma.core.utils import bare_openalex_work_id, normalize_doi, normalize_title_key
from alma.library.deduplication import _keeper_score, plan_same_work
from alma.services.paper_group_reconcile import preview_paper_group_repairs, reconcile_paper_groups

logger = logging.getLogger(__name__)

#: Merging fills a title or a DOI, which can reveal one more twin.
_MERGE_PASSES = 3
#: The user's own profile, merged both ways on 2026-06-17 and 2026-07-29 (A4).
OWNER_SURVIVOR = "A5042972527"
OWNER_MERGED_ROW = "A5114957962"


def _stale_dois(conn: sqlite3.Connection) -> list[tuple[str, str]]:
    return [
        (str(row["id"]), normalize_doi(row["doi"]) or "")
        for row in conn.execute("SELECT id, doi FROM papers WHERE COALESCE(doi, '') <> ''")
        if normalize_doi(row["doi"]) != row["doi"]
    ]


def _url_form_openalex_ids(conn: sqlite3.Connection) -> tuple[list[tuple[str, str]], int]:
    """Rows to rewrite, and how many keep their spelling because the id is taken."""
    rows = conn.execute(
        "SELECT id, openalex_id FROM papers WHERE openalex_id LIKE '%openalex.org%'"
    ).fetchall()
    owned = {
        str(row[0])
        for row in conn.execute(
            "SELECT openalex_id FROM papers WHERE openalex_id IS NOT NULL "
            "AND openalex_id NOT LIKE '%openalex.org%'"
        )
    }
    rewrite: list[tuple[str, str]] = []
    taken = 0
    for row in rows:
        bare = bare_openalex_work_id(row["openalex_id"])
        if bare in owned:
            taken += 1
        else:
            owned.add(bare)
            rewrite.append((str(row["id"]), bare))
    return rewrite, taken


def _other_orphans(conn: sqlite3.Connection) -> list[str]:
    """Rows hidden only because OpenAlex typed them `other`, with no way back."""
    return [
        str(row["id"])
        for row in conn.execute(
            "SELECT id, doi, title FROM papers WHERE work_type = 'other' "
            "AND COALESCE(component_type, '') <> '' AND COALESCE(parent_paper_id, '') = ''"
        )
        if classify_component(row["doi"], "other", row["title"])[0] is None
    ]


def _alias_case_duplicates(conn: sqlite3.Connection) -> list[str]:
    rows = conn.execute(
        """
        SELECT id FROM author_alt_identifiers a
        WHERE EXISTS (
            SELECT 1 FROM author_alt_identifiers b
            WHERE b.primary_author_id = a.primary_author_id
              AND lower(b.alt_openalex_id) = lower(a.alt_openalex_id)
              AND (b.source = 'manual_merge' AND a.source <> 'manual_merge'
                   OR (b.source = a.source AND b.id < a.id))
        )
        """
    ).fetchall()
    return [str(row[0]) for row in rows]


def _owner_needs_repair(conn: sqlite3.Connection) -> bool:
    row = conn.execute(
        "SELECT status FROM authors WHERE id = ?", (OWNER_SURVIVOR,)
    ).fetchone()
    if row is None:
        return False
    owner = conn.execute("SELECT 1 FROM followed_authors WHERE is_owner = 1").fetchone()
    return row["status"] == "removed" or owner is None


def _authorships_on_merged_ids(conn: sqlite3.Connection, redirects: dict[str, str]) -> dict[str, int]:
    counts: dict[str, int] = {}
    for alt in redirects:
        total = conn.execute(
            "SELECT COUNT(*) FROM publication_authors WHERE lower(openalex_id) = ?", (alt,)
        ).fetchone()[0]
        if total:
            counts[alt] = int(total)
    return counts


def _redirects_after_owner_repair(conn: sqlite3.Connection) -> dict[str, str]:
    """The mapping as it will stand: the owner's cycle hides his merged ids until repaired."""
    redirects = merged_author_redirects(conn)
    if _owner_needs_repair(conn):
        for row in conn.execute(
            "SELECT alt_openalex_id FROM author_alt_identifiers WHERE primary_author_id IN (?, ?) "
            "AND source IN ('manual_merge', 'suggestion_import')",
            (OWNER_SURVIVOR, OWNER_MERGED_ROW),
        ):
            if str(row[0]).lower() != OWNER_SURVIVOR.lower():
                redirects[str(row[0]).lower()] = OWNER_SURVIVOR
    return redirects


def plan(conn: sqlite3.Connection) -> dict[str, Any]:
    """What `apply` would do. Writes nothing."""
    rewrite, taken = _url_form_openalex_ids(conn)
    redirects = _redirects_after_owner_repair(conn)
    authorships = _authorships_on_merged_ids(conn, redirects)
    groups = preview_paper_group_repairs(conn)
    stale = _stale_dois(conn)
    return {
        "dois_to_normalise": len(stale),
        "doi_examples": [re.sub(r"\s+", " ", doi)[:60] for _, doi in stale[:5]],
        "openalex_ids_to_bare_form": len(rewrite),
        "openalex_ids_already_owned": taken,
        "other_type_orphans_to_show": len(_other_orphans(conn)),
        "alias_case_duplicates": len(_alias_case_duplicates(conn)),
        "owner_profile_repair": _owner_needs_repair(conn),
        "authorships_to_return": sum(authorships.values()),
        "merged_ids_with_authorships": len(authorships),
        "paper_groups": {
            key: groups[key]
            for key in (
                "repairable", "relationships_to_repair", "components_to_classify",
                "title_matches_ready", "merges_by_evidence", "ambiguous", "terminal_orphans",
            )
        },
    }


#: Loosest evidence first: what a person should read before anything else.
_LOOSEST_FIRST = (
    "title_and_authors", "deposit_or_abstract", "server_copy", "preprint_title",
    "semantic_scholar", "relation", "version", "openalex", "doi",
)
LIST_COLUMNS = (
    "repair", "reason", "title_similarity", "surviving_title", "merged_title", "surviving_year",
    "merged_year", "surviving_doi", "merged_doi", "surviving_status", "merged_status",
    "final_status", "surviving_id", "merged_id",
)


def _paper_row(conn: sqlite3.Connection, paper_id: str) -> sqlite3.Row:
    return conn.execute("SELECT * FROM papers WHERE id = ?", (paper_id,)).fetchone()


def _merge_rows(conn: sqlite3.Connection) -> list[dict[str, Any]]:
    from alma.core.paper_groups import choose_paper_group_root

    plan_result = plan_same_work(conn)
    rows: list[dict[str, Any]] = []
    for group in plan_result["groups"]:
        full = {str(row["id"]): _paper_row(conn, str(row["id"])) for row in group}
        keeper = max(group, key=_keeper_score)["id"]
        root_id = choose_paper_group_root(full, preferred_id=keeper)
        root = full[root_id]
        # What the root reads once the whole group landed, in the order the merge folds it.
        title = root["title"]
        statuses = {str(row["status"] or "") for row in full.values()}
        for paper_id, row in full.items():
            if paper_id != root_id:
                title = better_title(title, row["title"])
        final_status = _strongest(statuses, _MEMBERSHIP_RANK)
        for paper_id, row in full.items():
            if paper_id == root_id:
                continue
            ratio = difflib.SequenceMatcher(
                None, normalize_title_key(row["title"])[:200], normalize_title_key(title)[:200]
            ).ratio()
            rows.append({
                "repair": "same_work_merge",
                "reason": plan_result["reasons"].get(paper_id, ""),
                "title_similarity": round(ratio, 2),
                "surviving_title": title, "merged_title": row["title"],
                "surviving_year": root["year"], "merged_year": row["year"],
                "surviving_doi": root["doi"], "merged_doi": row["doi"],
                "surviving_status": root["status"], "merged_status": row["status"],
                "final_status": final_status,
                "surviving_id": root_id, "merged_id": paper_id,
            })
    rank = {reason: index for index, reason in enumerate(_LOOSEST_FIRST)}
    rows.sort(key=lambda row: (rank.get(row["reason"], 0), row["title_similarity"]))
    return rows


def _other_rows(conn: sqlite3.Connection) -> list[dict[str, Any]]:
    """Everything else `apply` changes, one row per change."""
    rows: list[dict[str, Any]] = []
    survivors = {
        str(row["openalex_id"]).lower(): str(row["name"])
        for row in conn.execute("SELECT name, openalex_id FROM authors WHERE openalex_id IS NOT NULL")
    }
    for alt, survivor in sorted(_redirects_after_owner_repair(conn).items()):
        for row in conn.execute(
            "SELECT pa.paper_id, pa.display_name, p.title, p.year, p.doi FROM publication_authors pa "
            "JOIN papers p ON p.id = pa.paper_id WHERE lower(pa.openalex_id) = ?",
            (alt,),
        ):
            rows.append({
                "repair": "authorship_returned",
                "reason": f"{row['display_name']} ({alt.upper()}) to "
                          f"{survivors.get(survivor.lower(), survivor)} ({survivor})",
                "merged_title": row["title"], "merged_year": row["year"], "merged_doi": row["doi"],
                "merged_id": row["paper_id"],
            })
    for paper_id in _other_orphans(conn):
        row = _paper_row(conn, paper_id)
        rows.append({
            "repair": "other_type_shown", "reason": "typed other, hidden with no parent",
            "merged_title": row["title"], "merged_year": row["year"], "merged_doi": row["doi"],
            "merged_status": row["status"], "merged_id": paper_id,
        })
    for paper_id, defects in paper_group_defect_map(conn).items():
        kinds = sorted(set(defects) - {"orphan_components", "ambiguous_preprints"})
        if kinds:
            row = _paper_row(conn, paper_id)
            root = _paper_row(conn, resolve_paper_root_id(conn, paper_id, strict=False))
            rows.append({
                "repair": "group_relationship", "reason": ", ".join(kinds),
                "surviving_title": root["title"], "surviving_year": root["year"],
                "surviving_doi": root["doi"], "surviving_status": root["status"], "surviving_id": root["id"],
                "merged_title": row["title"], "merged_year": row["year"], "merged_doi": row["doi"],
                "merged_status": row["status"], "merged_id": paper_id,
            })
    for item in plan_same_work(conn)["editions"]:
        row, other = _paper_row(conn, item["paper_id"]), _paper_row(conn, item["candidate_id"])
        rows.append({
            "repair": "review_edition", "reason": "one Semantic Scholar id, published 5+ years apart",
            "surviving_title": other["title"], "surviving_year": other["year"], "surviving_doi": other["doi"],
            "surviving_status": other["status"], "surviving_id": other["id"],
            "merged_title": row["title"], "merged_year": row["year"], "merged_doi": row["doi"],
            "merged_status": row["status"], "merged_id": row["id"],
        })
    kept = not_component_ids(conn)
    for row in conn.execute("SELECT * FROM papers WHERE component_type IS NULL").fetchall():
        component, _ = classify_component(row["doi"], row["work_type"], row["title"])
        if component and row["id"] not in kept:
            rows.append({
                "repair": "component_classified", "reason": component,
                "merged_title": row["title"], "merged_year": row["year"], "merged_doi": row["doi"],
                "merged_status": row["status"], "merged_id": row["id"],
            })
    return rows


def _merges_as_apply_finds_them(source: Path) -> list[dict[str, Any]]:
    """Replay the repair on a throwaway copy, so the sheet lists what `apply` will do.

    The steps before the merges change what the ladder sees: a normalised DOI joins
    two rows, a restored orphan is a row again, a filled title meets its twin.
    """
    with tempfile.TemporaryDirectory() as folder:
        copy = Path(folder) / "scholar.db"
        with sqlite3.connect(f"file:{source}?mode=ro", uri=True) as live, sqlite3.connect(copy) as target:
            live.backup(target)
        conn = sqlite3.connect(copy, isolation_level=None, check_same_thread=False)
        conn.row_factory = sqlite3.Row
        try:
            _prepare(conn)
            reconcile_paper_groups(conn, same_work=False)
            rows: list[dict[str, Any]] = []
            for _ in range(_MERGE_PASSES):
                found = _merge_rows(conn)
                if not found:
                    break
                rows.extend(found)
                reconcile_paper_groups(conn)
            return rows
        finally:
            conn.close()


def write_list(conn: sqlite3.Connection, path: Path, *, source: Path) -> dict[str, int]:
    """The whole repair as one sheet, loosest merges first. Writes nothing to ``source``."""
    rows = [*_merges_as_apply_finds_them(source), *_other_rows(conn)]
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=LIST_COLUMNS)
        writer.writeheader()
        writer.writerows(rows)
    counts: dict[str, int] = {}
    for row in rows:
        counts[row["repair"]] = counts.get(row["repair"], 0) + 1
    return counts


def _repair_owner(conn: sqlite3.Connection) -> None:
    """A5042972527 survives; the cycle goes; the follow it already has becomes the owner's."""
    if not _owner_needs_repair(conn):
        return
    conn.execute(
        "UPDATE authors SET status = 'active', author_type = 'followed' WHERE id = ?",
        (OWNER_SURVIVOR,),
    )
    conn.execute(
        "DELETE FROM author_alt_identifiers WHERE primary_author_id = ? AND lower(alt_openalex_id) = lower(?)",
        (OWNER_MERGED_ROW, OWNER_SURVIVOR),
    )
    conn.execute(
        "UPDATE OR IGNORE author_alt_identifiers SET primary_author_id = ? WHERE primary_author_id = ?",
        (OWNER_SURVIVOR, OWNER_MERGED_ROW),
    )
    conn.execute(
        "DELETE FROM author_alt_identifiers WHERE primary_author_id = ?", (OWNER_MERGED_ROW,)
    )
    conn.execute(
        "UPDATE followed_authors SET is_owner = 1 WHERE author_id = ?", (OWNER_SURVIVOR,)
    )
    sync_author_monitors(conn)


def _return_authorships(conn: sqlite3.Connection) -> int:
    moved = 0
    for alt, survivor in merged_author_redirects(conn).items():
        moved += conn.execute(
            "UPDATE OR IGNORE publication_authors SET openalex_id = ? WHERE lower(openalex_id) = ?",
            (survivor, alt),
        ).rowcount
        conn.execute("DELETE FROM publication_authors WHERE lower(openalex_id) = ?", (alt,))
    return moved


def _prepare(conn: sqlite3.Connection) -> dict[str, Any]:
    """Everything before the merges, one committed unit each."""
    now = utcnow().isoformat()
    report: dict[str, Any] = {}
    with conn:
        stale = _stale_dois(conn)
        conn.executemany("UPDATE papers SET doi = ?, updated_at = ? WHERE id = ?",
                         [(doi, now, paper_id) for paper_id, doi in stale])
        report["dois"] = len(stale)
    with conn:
        rewrite, _ = _url_form_openalex_ids(conn)
        conn.executemany("UPDATE papers SET openalex_id = ? WHERE id = ?",
                         [(bare, paper_id) for paper_id, bare in rewrite])
        report["openalex_ids"] = len(rewrite)
    with conn:
        orphans = _other_orphans(conn)
        conn.executemany("UPDATE papers SET component_type = NULL, updated_at = ? WHERE id = ?",
                         [(now, paper_id) for paper_id in orphans])
        report["other_orphans"] = len(orphans)
    with conn:
        duplicates = _alias_case_duplicates(conn)
        conn.executemany("DELETE FROM author_alt_identifiers WHERE id = ?", [(i,) for i in duplicates])
        report["alias_duplicates"] = len(duplicates)
    with conn:
        _repair_owner(conn)
        report["authorships_returned"] = _return_authorships(conn)
    return report


def apply(conn: sqlite3.Connection) -> dict[str, Any]:
    """Run every repair, then reconcile until nothing is left to merge."""
    report = _prepare(conn)
    merged = errors = 0
    for _ in range(_MERGE_PASSES):
        result = reconcile_paper_groups(conn)
        report["paper_groups"] = result["message"]
        errors += result["errors_total"]
        merged += result["preprints"]["preprint_twins_merged"]
        if not plan_same_work(conn)["merges"]:
            break
    report["merges"] = merged
    report["errors"] = errors
    return report


def main() -> int:
    parser = argparse.ArgumentParser(description="Preview or apply the one-time merge repairs.")
    parser.add_argument("--db", type=Path, required=True)
    parser.add_argument("--apply", action="store_true")
    parser.add_argument("--list", type=Path, help="write the whole repair to this CSV, nothing else")
    args = parser.parse_args()
    if not args.db.is_file():
        parser.error(f"no database at {args.db}")
    uri = f"file:{args.db}" + ("" if args.apply else "?mode=ro")
    conn = sqlite3.connect(uri, uri=True, isolation_level=None if args.apply else "", check_same_thread=False)
    conn.row_factory = sqlite3.Row
    apply_busy_timeout(conn)
    if args.list:
        json.dump(write_list(conn, args.list, source=args.db), sys.stdout, indent=2)
    else:
        json.dump(apply(conn) if args.apply else plan(conn), sys.stdout, indent=2, default=str)
    print()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
