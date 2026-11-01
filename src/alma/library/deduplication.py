"""Database deduplication and stable ID assignment.

This module centralizes author/paper deduplication logic for v3 schema.

v3 Schema Changes:
- papers table uses UUID `id` as PK (not composite key)
- Library membership is encoded by papers.status = 'library' (no separate junction table)
- All junction tables use paper_id instead of publication_key
- publication_authors table stores author associations (no author_id on papers)
"""

from __future__ import annotations

import hashlib
import logging
import sqlite3
from collections.abc import Iterable

from alma.core.author_names import parse_author_names, surname
from alma.core.db_write import write_section
from alma.core.paper_groups import (
    PaperGroupIntegrityError,
    absorb_paper_group,
    is_component_row,
)
from alma.core.sql_helpers import standalone_paper_sql
from alma.core.time import utcnow
from alma.core.utils import (
    canonical_lookup_doi,
    normalize_openalex_id,
    normalize_orcid,
    normalize_title_key,
)

logger = logging.getLogger(__name__)


def _norm_text(value: str | None) -> str:
    return " ".join((value or "").strip().lower().split())


def _norm_orcid(value: str | None) -> str:
    """Dedup-friendly ORCID form. Returns ``""`` for invalid input.

    Wraps the canonical :func:`alma.core.utils.normalize_orcid` so dedup
    output matches the value persisted in ``authors.orcid``.
    """
    return normalize_orcid(value) or ""


def _norm_openalex_author(value: str | None) -> str:
    # Delegate to the canonical normalizer (43.4). The old local copy uppercased
    # the whole string and never repaired `%3A` residue, so it diverged from the
    # OpenAlex client on exactly the malformed ids dedup needs to match.
    return normalize_openalex_id(value)


def _norm_doi(value: str | None) -> str:
    """Dedup-friendly DOI form. Returns ``""`` for invalid input.

    Wraps :func:`alma.core.utils.canonical_lookup_doi` (lowercased,
    URL-decoded, fragment-stripped) so dedup is robust to publisher
    fragments (``/abstract``, ``/pdf``, …) that ``papers.doi`` would
    otherwise treat as distinct rows.
    """
    return canonical_lookup_doi(value) or ""


def _stable_hash(prefix: str, payload: str) -> str:
    return f"{prefix}_{hashlib.sha1(payload.encode('utf-8')).hexdigest()[:16]}"


def _author_uid(row: sqlite3.Row) -> str:
    oa = _norm_openalex_author(row["openalex_id"] if "openalex_id" in row.keys() else None)
    if oa:
        return f"author_oa_{oa}"
    oc = _norm_orcid(row["orcid"] if "orcid" in row.keys() else None)
    if oc:
        return f"author_orcid_{oc}"
    base = f"{_norm_text(row['name'])}|{_norm_text(row['id'])}"
    return _stable_hash("author", base)


def ensure_stable_ids(conn: sqlite3.Connection) -> dict:
    """Ensure stable ID columns exist and are populated (v3 schema - authors only).

    v3 papers already have UUID ids, so no stable ID assignment needed.
    Authors still use the author_uid pattern for deduplication.
    """
    changed_authors = 0

    # `authors.author_uid` is guaranteed by the startup schema
    # (bootstrap DDL + core.migrations); this pass only recomputes values.
    for row in conn.execute("SELECT rowid, * FROM authors").fetchall():
        uid = _author_uid(row)
        if row["author_uid"] != uid:
            conn.execute("UPDATE authors SET author_uid = ? WHERE rowid = ?", (uid, row["rowid"]))
            changed_authors += 1

    # Enforce unique constraints where meaningful.
    conn.execute(
        "CREATE UNIQUE INDEX IF NOT EXISTS ux_authors_uid ON authors(author_uid)"
    )
    conn.execute(
        "CREATE UNIQUE INDEX IF NOT EXISTS ux_authors_openalex_norm "
        "ON authors(lower(openalex_id)) "
        "WHERE openalex_id IS NOT NULL AND trim(openalex_id) <> ''"
    )
    conn.execute(
        "CREATE UNIQUE INDEX IF NOT EXISTS ux_authors_orcid_norm "
        "ON authors(lower(orcid)) "
        "WHERE orcid IS NOT NULL AND trim(orcid) <> ''"
    )
    # Mirror the authors-side normalised index on the junction table so
    # `JOIN ... ON lower(a.openalex_id) = lower(pa.openalex_id)` can use
    # both sides. Without this, the followed-author background-prior
    # queries in compute_preference_profile fall back to a full scan and
    # take 12-30s on a real corpus.
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_publication_authors_openalex_norm "
        "ON publication_authors(lower(openalex_id))"
    )
    return {"authors_updated": changed_authors}


def _merge_author_metadata(conn: sqlite3.Connection, keeper_id: str, dup_id: str) -> None:
    k = conn.execute("SELECT * FROM authors WHERE id = ?", (keeper_id,)).fetchone()
    d = conn.execute("SELECT * FROM authors WHERE id = ?", (dup_id,)).fetchone()
    if not k or not d:
        return
    updates: dict[str, object] = {}
    for col in ("openalex_id", "orcid", "affiliation", "interests", "url_picture", "institutions", "email_domain"):
        if not (k[col] or "").strip() and (d[col] or "").strip():
            updates[col] = d[col]
    updates["citedby"] = max(int(k["citedby"] or 0), int(d["citedby"] or 0))
    updates["h_index"] = max(int(k["h_index"] or 0), int(d["h_index"] or 0))
    updates["works_count"] = max(int(k["works_count"] or 0), int(d["works_count"] or 0))
    if updates:
        sets = ", ".join(f"{c} = ?" for c in updates.keys())
        conn.execute(f"UPDATE authors SET {sets} WHERE id = ?", (*updates.values(), keeper_id))


def _update_publication_authors_for_merge(conn: sqlite3.Connection, old_author_id: str, new_author_id: str) -> int:
    """Update publication_authors junction table when merging authors (v3 schema).

    v3 schema: papers don't have author_id column. Author associations are in
    the publication_authors junction table.
    """
    moved = 0
    try:
        # Move all authorship records from old author to new author
        rows = conn.execute(
            "SELECT * FROM publication_authors WHERE openalex_id IN "
            "(SELECT openalex_id FROM authors WHERE id = ?)",
            (old_author_id,),
        ).fetchall()

        for row in rows:
            # Try to insert into new author (may already exist)
            conn.execute(
                """INSERT OR IGNORE INTO publication_authors
                   (paper_id, openalex_id, display_name, orcid, position, is_corresponding, institution)
                   VALUES (?, ?, ?, ?, ?, ?, ?)""",
                (
                    row["paper_id"],
                    row["openalex_id"],
                    row["display_name"],
                    row["orcid"],
                    row["position"],
                    row["is_corresponding"],
                    row["institution"],
                ),
            )
            moved += 1
    except Exception as exc:
        # publication_authors is the only authorship link we have for
        # papers in v3 schema. Silently dropping a failure here means
        # papers lose their author attribution after a merge — log it
        # so a re-run or manual re-link can recover.
        logger.warning(
            "Failed to move publication_authors rows for author %s → %s: %s",
            old_author_id,
            new_author_id,
            exc,
        )

    return moved


def _merge_author_publications(conn: sqlite3.Connection, dup_id: str, keeper_id: str) -> int:
    """Merge author associations when deduplicating authors (v3 schema).

    v3 schema: papers don't have author_id column. Author associations are in
    publication_authors junction table. We need to update that table.
    """
    moved = _update_publication_authors_for_merge(conn, dup_id, keeper_id)
    return moved


def deduplicate_authors(conn: sqlite3.Connection) -> dict:
    """Deduplicate authors by OpenAlex ID, ORCID, or normalized name (v3 schema)."""
    rows = conn.execute("SELECT * FROM authors ORDER BY id").fetchall()
    if not rows:
        return {"groups": 0, "merged_authors": 0, "moved_authorship_records": 0}

    groups: dict[str, list[str]] = {}
    by_id: dict[str, sqlite3.Row] = {r["id"]: r for r in rows}
    for r in rows:
        keys: list[str] = []
        oa = _norm_openalex_author(r["openalex_id"])
        oc = _norm_orcid(r["orcid"])
        nm = _norm_text(r["name"])
        if oa:
            keys.append(f"oa:{oa}")
        if oc:
            keys.append(f"orcid:{oc}")
        if nm:
            keys.append(f"name:{nm}")
        key = keys[0] if keys else f"id:{r['id']}"
        groups.setdefault(key, []).append(r["id"])

    merged_authors = 0
    moved_authorship_records = 0
    merged_groups = 0

    for _, ids in groups.items():
        # Exact duplicate IDs from multiple keys are not expected here.
        uniq = []
        seen = set()
        for i in ids:
            if i not in seen:
                uniq.append(i)
                seen.add(i)
        if len(uniq) <= 1:
            continue

        merged_groups += 1
        # Prefer rows with OA/ORCID + richer metadata.
        def _score(aid: str) -> int:
            r = by_id[aid]
            return (
                (1 if _norm_openalex_author(r["openalex_id"]) else 0) * 1000
                + (1 if _norm_orcid(r["orcid"]) else 0) * 900
                + int(r["works_count"] or 0)
                + int(r["citedby"] or 0)
            )

        keeper = sorted(uniq, key=_score, reverse=True)[0]
        for dup in uniq:
            if dup == keeper:
                continue
            # One gated write window per merged author (commit-per-unit-of-work;
            # all-local, no network — the gate releases between merges so a
            # concurrent foreground write isn't starved for the whole sweep).
            with write_section(conn, label="dedup author merge"):
                _merge_author_metadata(conn, keeper, dup)
                moved_authorship_records += _merge_author_publications(conn, dup, keeper)

                # followed_authors should keep one row.
                try:
                    row = conn.execute(
                        "SELECT followed_at, notify_new_papers FROM followed_authors WHERE author_id = ?",
                        (dup,),
                    ).fetchone()
                    if row:
                        conn.execute(
                            "INSERT OR IGNORE INTO followed_authors (author_id, followed_at, notify_new_papers) VALUES (?, ?, ?)",
                            (keeper, row["followed_at"], row["notify_new_papers"]),
                        )
                        conn.execute("DELETE FROM followed_authors WHERE author_id = ?", (dup,))
                except Exception:
                    pass

                conn.execute("DELETE FROM authors WHERE id = ?", (dup,))
            merged_authors += 1

    return {
        "groups": merged_groups,
        "merged_authors": merged_authors,
        "moved_authorship_records": moved_authorship_records,
    }


# A title key shorter than this ("Introduction", "Theory of mind") names many works.
_MIN_TITLE_KEY_CHARS = 20
# Below this share of the title's characters the key is noise: it keeps only
# a-z0-9, so a Chinese title with "fMRI" in it would reduce to "fmri".
_MIN_TITLE_KEY_SHARE = 0.5
_REVIEW_SAMPLE = 20


def _author_surnames(authors: str | None) -> set[str]:
    return {surname(name).casefold() for name in parse_author_names(authors or "")}


def _same_authors(a: sqlite3.Row, b: sqlite3.Row) -> bool:
    """Both rows list authors, and at least half of the shorter list is shared."""
    first, second = _author_surnames(a["authors"]), _author_surnames(b["authors"])
    shared = first & second
    return bool(shared) and len(shared) * 2 >= min(len(first), len(second))


def _title_key(row: sqlite3.Row) -> tuple[str, int] | None:
    """The (title, year) identity. A row with no year has none (`core.utils`)."""
    title = normalize_title_key(row["title"] or "")
    return (title, int(row["year"])) if title and row["year"] else None


def _title_identifies_a_work(row: sqlite3.Row) -> bool:
    title = row["title"] or ""
    key = normalize_title_key(title)
    letters = sum(ch.isalnum() for ch in title)
    return len(key) >= _MIN_TITLE_KEY_CHARS and len(key) >= letters * _MIN_TITLE_KEY_SHARE


def _duplicate_groups(
    rows: Iterable[sqlite3.Row],
) -> tuple[list[list[sqlite3.Row]], list[dict[str, str]]]:
    """Rows that are one work, and the pairs left for a person to judge.

    A DOI decides on its own. A URL is not an identity:
    `openreview.net/pdf?id=...` is one path for every paper on that site. A row
    with no DOI joins by title and year only on strong evidence: the title
    identifies a work, both rows list authors and they agree, and one candidate
    group fits, because a preprint and its journal version carry two DOIs under
    one title. Anything weaker is reported and left separate, like an ambiguous
    preprint match. A component row is never a merge candidate, whatever the
    stored columns say (`is_component_row`).
    """
    by_doi: dict[str, list[sqlite3.Row]] = {}
    no_doi: list[sqlite3.Row] = []
    for row in rows:
        if is_component_row(row):
            continue
        doi = _norm_doi(row["doi"])
        if doi:
            by_doi.setdefault(doi, []).append(row)
        else:
            no_doi.append(row)

    groups = list(by_doi.values())
    groups_by_title: dict[tuple[str, int], list[list[sqlite3.Row]]] = {}
    for group in groups:
        for key in {_title_key(row) for row in group} - {None}:
            groups_by_title.setdefault(key, []).append(group)

    review: list[dict[str, str]] = []
    for row in no_doi:
        key = _title_key(row)
        if key is None:
            continue
        candidates = groups_by_title.get(key, [])
        if not candidates:
            new_group = [row]
            groups.append(new_group)
            groups_by_title[key] = [new_group]
            continue
        fitting = [g for g in candidates if all(_same_authors(row, member) for member in g)]
        if len(fitting) == 1 and _title_identifies_a_work(row):
            fitting[0].append(row)
        else:
            review.append(
                {"paper_id": row["id"], "title": row["title"] or "", "candidate_id": candidates[0][0]["id"]}
            )
    return [group for group in groups if len(group) > 1], review


def _keeper_score(row: sqlite3.Row) -> tuple[int, int]:
    """The saved row first, because its id is what the user's work hangs on."""
    richness = (
        int(bool((row["doi"] or "").strip())) * 1000
        + int(bool((row["openalex_id"] or "").strip())) * 500
        + int(bool((row["url"] or "").strip())) * 100
        + int(bool((row["abstract"] or "").strip())) * 50
        + int(row["cited_by_count"] or 0)
    )
    return (int(row["status"] == "library"), richness)


def deduplicate_papers(conn: sqlite3.Connection) -> dict:
    """Merge standalone rows of one work through the paper-group owner.

    A soft merge (D3): the loser stays as a pointer to the survivor, and its
    feedback, Feed and Discovery state, collections and tags move there
    (`absorb_paper_group`). Rows already in a group are not looked at.
    """
    rows = conn.execute(f"SELECT * FROM papers p WHERE {standalone_paper_sql('p')}").fetchall()
    groups, review = _duplicate_groups(rows)

    merged = 0
    rewired = 0
    failed = 0
    for group in groups:
        keeper_id = max(group, key=_keeper_score)["id"]
        for dup in group:
            if dup["id"] == keeper_id:
                continue
            try:
                # One gated write window per merged paper (commit-per-unit-of-work,
                # all-local).
                with write_section(conn, label="dedup paper merge"):
                    result = absorb_paper_group(conn, dup["id"], keeper_id, reason="library_dedup")
            except PaperGroupIntegrityError as exc:
                # A broken pointer chain is Health's repair; the rest of the pass goes on.
                logger.warning("Dedup skipped %s → %s: %s", dup["id"], keeper_id, exc)
                failed += 1
                continue
            if result.get("skipped"):
                continue
            merged += 1
            rewired += sum(result.get("fk_migrated", {}).values())

    return {
        "merged_papers": merged,
        "rewired_references": rewired,
        "failed": failed,
        "needs_review": len(review),
        "review_sample": review[:_REVIEW_SAMPLE],
    }


def run_deduplication(conn: sqlite3.Connection, job_id: str | None = None) -> dict:
    """Run full deduplication pass and stable-ID assignment (v3 schema)."""
    started = utcnow().isoformat()

    if job_id:
        try:
            from alma.api.scheduler import add_job_log

            add_job_log(job_id, "Starting deduplication", step="dedup_start")
        except Exception:
            pass

    # deduplicate_authors commits each merge in its own write_section.
    author_summary = deduplicate_authors(conn)
    if job_id:
        try:
            from alma.api.scheduler import add_job_log

            add_job_log(job_id, f"Author dedup complete: {author_summary}", step="dedup_authors")
        except Exception:
            pass

    # deduplicate_papers commits each merge in its own write_section.
    paper_summary = deduplicate_papers(conn)
    if job_id:
        try:
            from alma.api.scheduler import add_job_log

            add_job_log(
                job_id,
                f"Paper dedup complete: {paper_summary}",
                step="dedup_papers",
            )
        except Exception:
            pass

    # ensure_stable_ids does only local writes (no network) — gate the window.
    with write_section(conn, label="dedup stable ids"):
        id_summary = ensure_stable_ids(conn)
    if job_id:
        try:
            from alma.api.scheduler import add_job_log

            add_job_log(job_id, f"Stable ID pass complete: {id_summary}", step="dedup_ids")
        except Exception:
            pass

    summary = {
        "started_at": started,
        "finished_at": utcnow().isoformat(),
        "authors": author_summary,
        "papers": paper_summary,
        "stable_ids": id_summary,
    }
    logger.info("Database deduplication complete: %s", summary)
    return summary
