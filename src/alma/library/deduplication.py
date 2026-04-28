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
from datetime import datetime
from typing import Optional
from urllib.parse import urlsplit, urlunsplit

logger = logging.getLogger(__name__)


def _norm_text(value: Optional[str]) -> str:
    return " ".join((value or "").strip().lower().split())


def _norm_orcid(value: Optional[str]) -> str:
    s = (value or "").strip().lower()
    if s.startswith("https://orcid.org/"):
        s = s[len("https://orcid.org/"):]
    if s.startswith("http://orcid.org/"):
        s = s[len("http://orcid.org/"):]
    return s


def _norm_openalex_author(value: Optional[str]) -> str:
    s = (value or "").strip()
    if not s:
        return ""
    ls = s.lower()
    for pfx in ("https://openalex.org/", "http://openalex.org/", "openalex.org/"):
        if ls.startswith(pfx):
            return s[len(pfx):].strip().upper()
    return s.strip().upper()


def _norm_doi(value: Optional[str]) -> str:
    s = (value or "").strip()
    if not s:
        return ""
    s = s.replace("DOI:", "").replace("doi:", "").strip()
    ls = s.lower()
    for pfx in ("https://doi.org/", "http://doi.org/"):
        if ls.startswith(pfx):
            s = s[len(pfx):]
            break
    return s.strip().lower()


def _norm_url(value: Optional[str]) -> str:
    s = (value or "").strip()
    if not s:
        return ""
    try:
        parsed = urlsplit(s)
        netloc = parsed.netloc.lower()
        path = parsed.path.rstrip("/")
        # Drop fragment/query for dedup stability.
        return urlunsplit((parsed.scheme.lower(), netloc, path, "", ""))
    except Exception:
        return s.rstrip("/").lower()


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


def _paper_identity(title: str, doi: str, url: str, year: Optional[int]) -> str:
    """Generate stable identity string for a paper (v3 schema - no author_id)."""
    d = _norm_doi(doi)
    if d:
        return f"doi:{d}"
    u = _norm_url(url)
    if u:
        return f"url:{u}"
    t = _norm_text(title)
    y = str(year or "")
    return f"title:{t}|year:{y}"


def ensure_stable_ids(conn: sqlite3.Connection) -> dict:
    """Ensure stable ID columns exist and are populated (v3 schema - authors only).

    v3 papers already have UUID ids, so no stable ID assignment needed.
    Authors still use the author_uid pattern for deduplication.
    """
    changed_authors = 0

    try:
        cols = [r[1] for r in conn.execute("PRAGMA table_info(authors)").fetchall()]
        if "author_uid" not in cols:
            conn.execute("ALTER TABLE authors ADD COLUMN author_uid TEXT")
    except sqlite3.OperationalError:
        pass

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


def _rewire_paper_refs(conn: sqlite3.Connection, old_paper_id: str, new_paper_id: str) -> None:
    """Update all junction tables to point to the new paper_id (v3 schema)."""
    if old_paper_id == new_paper_id:
        return

    # All tables that reference paper_id
    tables = [
        "collection_items",
        "publication_tags",
        "alerted_publications",
        "publication_embeddings",
        "publication_topics",
        "publication_authors",
        "publication_institutions",
        "publication_references",
        "publication_clusters",
        "tag_suggestions",
        "feed_items",
        "recommendations",
        "lens_signals",
    ]

    for table in tables:
        try:
            conn.execute(
                f"UPDATE OR IGNORE {table} SET paper_id = ? WHERE paper_id = ?",
                (new_paper_id, old_paper_id),
            )
            conn.execute(f"DELETE FROM {table} WHERE paper_id = ?", (old_paper_id,))
        except Exception:
            continue


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
    except Exception:
        pass

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
            conn.commit()
            merged_authors += 1

    return {
        "groups": merged_groups,
        "merged_authors": merged_authors,
        "moved_authorship_records": moved_authorship_records,
    }


def deduplicate_papers(conn: sqlite3.Connection) -> dict:
    """Deduplicate papers by normalized title, DOI, or URL (v3 schema).

    v3 schema: papers have UUID id as PK. Junction tables use paper_id.
    Duplicates are identified by title/doi/url, and all references are rewired
    to the keeper paper.
    """
    rows = conn.execute("SELECT * FROM papers").fetchall()
    groups: dict[str, list[sqlite3.Row]] = {}

    for r in rows:
        key = _paper_identity(r["title"], r["doi"], r["url"], r["year"])
        groups.setdefault(key, []).append(r)

    merged = 0
    rewired = 0

    for _, grp in groups.items():
        if len(grp) <= 1:
            continue

        def _score(r: sqlite3.Row) -> int:
            return (
                int(bool((r["doi"] or "").strip())) * 1000
                + int(bool((r["openalex_id"] or "").strip())) * 500
                + int(bool((r["url"] or "").strip())) * 100
                + int(bool((r["abstract"] or "").strip())) * 50
                + int(r["cited_by_count"] or 0)
            )

        keeper = sorted(grp, key=_score, reverse=True)[0]
        keeper_id = keeper["id"]

        for dup in grp:
            if dup["id"] == keeper_id:
                continue

            # Rewire all junction tables to point to keeper
            _rewire_paper_refs(conn, dup["id"], keeper_id)

            # Merge metadata into keeper (keep richer data)
            conn.execute(
                """UPDATE papers
                   SET cited_by_count = MAX(COALESCE(cited_by_count, 0), ?),
                       year = COALESCE(year, ?),
                       abstract = COALESCE(NULLIF(abstract, ''), ?),
                       url = COALESCE(NULLIF(url, ''), ?),
                       doi = COALESCE(NULLIF(doi, ''), ?),
                       journal = COALESCE(NULLIF(journal, ''), ?),
                       authors = COALESCE(NULLIF(authors, ''), ?),
                       openalex_id = COALESCE(NULLIF(openalex_id, ''), ?),
                       fwci = COALESCE(fwci, ?),
                       status = CASE
                           WHEN status = 'library' OR ? = 'library' THEN 'library'
                           ELSE status
                       END,
                       rating = MAX(COALESCE(rating, 0), ?),
                       notes = CASE
                           WHEN LENGTH(COALESCE(notes, '')) >= LENGTH(COALESCE(?, '')) THEN notes
                           ELSE ?
                       END
                   WHERE id = ?""",
                (
                    dup["cited_by_count"] or 0,
                    dup["year"],
                    dup["abstract"],
                    dup["url"],
                    dup["doi"],
                    dup["journal"],
                    dup["authors"],
                    dup["openalex_id"],
                    dup["fwci"],
                    dup["status"],
                    dup["rating"] or 0,
                    dup["notes"],
                    dup["notes"],
                    keeper_id,
                ),
            )

            # Delete duplicate
            conn.execute("DELETE FROM papers WHERE id = ?", (dup["id"],))
            conn.commit()
            merged += 1
            rewired += 1

    return {"merged_papers": merged, "rewired_references": rewired}


def run_deduplication(conn: sqlite3.Connection, job_id: Optional[str] = None) -> dict:
    """Run full deduplication pass and stable-ID assignment (v3 schema)."""
    started = datetime.utcnow().isoformat()

    if job_id:
        try:
            from alma.api.scheduler import add_job_log

            add_job_log(job_id, "Starting deduplication", step="dedup_start")
        except Exception:
            pass

    author_summary = deduplicate_authors(conn)
    conn.commit()
    if job_id:
        try:
            from alma.api.scheduler import add_job_log

            add_job_log(job_id, f"Author dedup complete: {author_summary}", step="dedup_authors")
        except Exception:
            pass

    paper_summary = deduplicate_papers(conn)
    conn.commit()
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

    id_summary = ensure_stable_ids(conn)
    conn.commit()
    if job_id:
        try:
            from alma.api.scheduler import add_job_log

            add_job_log(job_id, f"Stable ID pass complete: {id_summary}", step="dedup_ids")
        except Exception:
            pass

    summary = {
        "started_at": started,
        "finished_at": datetime.utcnow().isoformat(),
        "authors": author_summary,
        "papers": paper_summary,
        "stable_ids": id_summary,
    }
    logger.info("Database deduplication complete: %s", summary)
    return summary
