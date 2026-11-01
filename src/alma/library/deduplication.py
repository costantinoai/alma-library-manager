"""Paper deduplication and stable author ids.

Authors are not merged here: `application.author_merge` owns that, with the
record of who decided what.

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

from alma.core.db_write import write_section
from alma.core.paper_groups import (
    PaperGroupIntegrityError,
    PreprintTitleIndex,
    _root_rank,
    absorb_paper_group,
    authors_agree,
    is_component_row,
    is_preprint_row,
    rejected_paper_pairs,
    titles_alike,
)
from alma.core.sql_helpers import standalone_paper_sql
from alma.core.time import utcnow
from alma.core.utils import (
    MIN_IDENTIFYING_TITLE_CHARS,
    bare_openalex_work_id,
    canonical_lookup_doi,
    doi_work_stem,
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


_REVIEW_SAMPLE = 20


def _same_authors(a: sqlite3.Row, b: sqlite3.Row) -> bool:
    return authors_agree(a["authors"], b["authors"])


def _title_key(row: sqlite3.Row) -> tuple[str, int] | None:
    """The (title, year) identity. A row with no year has none (`core.utils`)."""
    title = normalize_title_key(row["title"] or "")
    return (title, int(row["year"])) if title and row["year"] else None


def _title_identifies_a_work(row: sqlite3.Row) -> bool:
    return len(normalize_title_key(row["title"] or "")) >= MIN_IDENTIFYING_TITLE_CHARS


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
        + int(bool(row["has_abstract"])) * 50
        + int(row["cited_by_count"] or 0)
    )
    return (int(row["status"] == "library"), richness)


_PLAN_COLUMNS = (
    "p.id, p.title, p.year, p.doi, p.journal, p.authors, p.url, p.openalex_id, p.work_type, "
    "p.preprint_source, p.component_type, p.status, p.cited_by_count, p.semantic_scholar_id, "
    "COALESCE(p.abstract, '') <> '' AS has_abstract"
)


class _WorkGroups:
    """Union-find over standalone rows that refuses to join a pair the user split."""

    def __init__(self, rejected: set[tuple[str, str]]) -> None:
        self._parent: dict[str, str] = {}
        self._members: dict[str, set[str]] = {}
        self._rejected = rejected
        self.levels: dict[str, int] = {}
        #: The strongest evidence that tied each row in. The ladder runs strongest first.
        self.reasons: dict[str, str] = {}

    def _find(self, paper_id: str) -> str:
        self._parent.setdefault(paper_id, paper_id)
        self._members.setdefault(paper_id, {paper_id})
        while self._parent[paper_id] != paper_id:
            self._parent[paper_id] = self._parent[self._parent[paper_id]]
            paper_id = self._parent[paper_id]
        return paper_id

    def _split_between(self, first: set[str], second: set[str]) -> bool:
        return any(
            (a, b) in self._rejected or (b, a) in self._rejected for a in first for b in second
        )

    def join(self, first: str, second: str, level: str) -> None:
        one, two = self._find(first), self._find(second)
        if one == two or self._split_between(self._members[one], self._members[two]):
            return
        self._parent[two] = one
        self._members[one] |= self._members.pop(two)
        self.levels[level] = self.levels.get(level, 0) + 1
        self.reasons.setdefault(first, level)
        self.reasons.setdefault(second, level)

    def groups(self) -> list[set[str]]:
        return [members for members in self._members.values() if len(members) > 1]


def _same_key(groups: _WorkGroups, rows: Iterable[sqlite3.Row], key, level: str) -> None:
    seen: dict[str, str] = {}
    for row in rows:
        value = key(row)
        if value:
            groups.join(seen.setdefault(value, row["id"]), row["id"], level)


def _join_lesser_copies(groups: _WorkGroups, rows: list[sqlite3.Row]) -> None:
    """Same title and authors: a deposit or meeting abstract joins its one paper,
    and a preprint or deposit joins its copy on another server."""
    buckets: dict[str, list[sqlite3.Row]] = {}
    for row in rows:
        if not is_component_row(row) and _title_identifies_a_work(row):
            buckets.setdefault(normalize_title_key(row["title"]), []).append(row)
    for bucket in buckets.values():
        if len(bucket) < 2:
            continue
        papers = [row for row in bucket if _root_rank(row) == 300]
        if len({doi_work_stem(row["doi"]) or row["id"] for row in papers}) == 1:
            for row in bucket:
                if 100 < _root_rank(row) < 300 and _same_authors(row, papers[0]):
                    groups.join(papers[0]["id"], row["id"], "deposit_or_abstract")
        copies = [row for row in bucket if is_preprint_row(row) or _root_rank(row) == 200]
        for index, first in enumerate(copies):
            for second in copies[index + 1:]:
                near = first["year"] and second["year"] and abs(first["year"] - second["year"]) <= 2
                if near and _same_authors(first, second):
                    groups.join(first["id"], second["id"], "server_copy")


def _year_spread(group: list[sqlite3.Row]) -> int:
    years = [int(row["year"]) for row in group if row["year"]]
    return max(years) - min(years) if years else 0


def plan_same_work(conn: sqlite3.Connection) -> dict:
    """Every group of standalone rows that are one work, strongest evidence first.

    The one corpus-wide ladder, read by the preview, the repair and the job, so
    they cannot disagree: same DOI, same OpenAlex id, one DOI's versions, a
    registered relation, a Semantic Scholar id confirmed by title and authors, a
    journal paper's preprints, then the same title and authors. Writes nothing.
    A preprint that fits two published works is reported and left alone.
    """
    rows = conn.execute(
        f"SELECT {_PLAN_COLUMNS} FROM papers p WHERE {standalone_paper_sql('p')}"
    ).fetchall()
    rejected = rejected_paper_pairs(conn)
    groups = _WorkGroups(rejected)
    papers = [row for row in rows if not is_component_row(row)]

    _same_key(groups, papers, lambda r: _norm_doi(r["doi"]), "doi")
    _same_key(groups, papers, lambda r: bare_openalex_work_id(r["openalex_id"]).upper(), "openalex")
    _same_key(groups, papers, lambda r: doi_work_stem(r["doi"]), "version")

    by_stem: dict[str, str] = {}
    for row in papers:
        stem = doi_work_stem(row["doi"])
        if stem:
            by_stem.setdefault(stem, row["id"])
    known = {row["id"] for row in papers}
    try:
        for relation in conn.execute("SELECT paper_id, related_doi FROM paper_relations"):
            other = by_stem.get(doi_work_stem(relation[1]))
            if other and relation[0] in known:
                groups.join(relation[0], other, "relation")
    except sqlite3.OperationalError:
        pass

    by_s2: dict[str, sqlite3.Row] = {}
    for row in papers:
        s2_id = str(row["semantic_scholar_id"] or "").strip()
        if not s2_id:
            continue
        first = by_s2.setdefault(s2_id, row)
        if first is not row and titles_alike(first["title"], row["title"]) and _same_authors(first, row):
            groups.join(first["id"], row["id"], "semantic_scholar")

    index = PreprintTitleIndex(list(papers), rejected)
    pairs = index.pairs(require_doi=True)
    for pair in pairs:
        if not pair["ambiguous"]:
            groups.join(pair["preprint_id"], pair["canonical_id"], "preprint_title")

    titled, review = _duplicate_groups(papers)
    for group in titled:
        for row in group[1:]:
            groups.join(group[0]["id"], row["id"], "title_and_authors")
    _join_lesser_copies(groups, papers)

    by_id = {row["id"]: row for row in rows}
    planned = [[by_id[paper_id] for paper_id in sorted(members)] for members in groups.groups()]
    # A capped run spends its budget on the closest matches first.
    planned.sort(key=lambda group: (_year_spread(group), group[0]["id"]))
    return {
        "groups": planned,
        "merges": sum(len(group) - 1 for group in planned),
        "levels": groups.levels,
        "reasons": groups.reasons,
        "ambiguous": [pair for pair in pairs if pair["ambiguous"]],
        "review": review,
    }


def merge_planned_group(conn: sqlite3.Connection, group: list[sqlite3.Row], *, unit) -> dict:
    """Absorb one planned group, each merge in its own ``unit``. Local, no network."""
    keeper_id = max(group, key=_keeper_score)["id"]
    merged = rewired = failed = 0
    for row in group:
        if row["id"] == keeper_id:
            continue
        try:
            with unit():
                result = absorb_paper_group(conn, row["id"], keeper_id, reason="same_work")
        except PaperGroupIntegrityError as exc:
            # A broken pointer chain is Health's repair; the rest of the pass goes on.
            logger.warning("Dedup skipped %s → %s: %s", row["id"], keeper_id, exc)
            failed += 1
            continue
        if not result.get("skipped"):
            merged += 1
            rewired += sum(result.get("fk_migrated", {}).values())
    return {"merged": merged, "rewired": rewired, "failed": failed}


def deduplicate_papers(conn: sqlite3.Connection) -> dict:
    """Merge standalone rows of one work through the paper-group owner.

    A soft merge (D3): the loser stays as a pointer to the survivor, and its
    feedback, Feed and Discovery state, collections and tags move there
    (`absorb_paper_group`). Rows already in a group are not looked at.
    """
    plan = plan_same_work(conn)
    merged = rewired = failed = 0
    for group in plan["groups"]:
        counts = merge_planned_group(
            conn, group, unit=lambda: write_section(conn, label="dedup paper merge")
        )
        merged += counts["merged"]
        rewired += counts["rewired"]
        failed += counts["failed"]
    return {
        "merged_papers": merged,
        "rewired_references": rewired,
        "failed": failed,
        "levels": plan["levels"],
        "needs_review": len(plan["review"]),
        "review_sample": plan["review"][:_REVIEW_SAMPLE],
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
        "papers": paper_summary,
        "stable_ids": id_summary,
    }
    logger.info("Database deduplication complete: %s", summary)
    return summary
