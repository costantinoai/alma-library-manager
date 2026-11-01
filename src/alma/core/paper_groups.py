"""Journal-first paper-group identity and consolidation.

One logical work has one first-class root.  Same-work/version rows use
``canonical_paper_id``; part-of rows use ``parent_paper_id`` plus
``component_type``.  The axes stay distinct, but both always terminate at the
same direct standalone root.  This module owns root selection and the atomic
rewrite used by ingest, duplicate collapse, and corpus reconciliation.
"""

from __future__ import annotations

import difflib
import logging
import re
import sqlite3
import uuid
from collections.abc import Iterable, Mapping
from typing import Any

from alma.core import addons
from alma.core.author_names import parse_author_names, surname
from alma.core.time import utcnow
from alma.core.utils import doi_version, doi_work_stem, normalize_title_key

logger = logging.getLogger(__name__)


PREPRINT_DOI_PREFIXES: dict[str, str] = {
    "10.48550/arxiv": "arxiv",
    "10.1101/": "biorxiv",
    "10.64898/": "biorxiv",
    "10.31234/": "psyrxiv",
    "10.31219/": "osf",
    "10.31222/": "metaarxiv",
    "10.31235/": "socarxiv",
    "10.26434/chemrxiv": "chemrxiv",
    "10.36227/techrxiv": "techrxiv",
    "10.20944/preprints": "mdpi_preprints",
    "10.21203/rs": "research_square",
    "10.2139/ssrn": "ssrn",
    "10.22541/au": "authorea",
    "10.7287/peerj.preprints": "peerj_preprints",
    "10.2196/preprints": "jmir_preprints",
    "10.1038/npre": "nature_precedings",
}
# Cold Spring Harbor publishes journals under bioRxiv's prefix: `10.1101/lm...`
# is Learning & Memory. bioRxiv's own DOIs start with a digit.
_BIORXIV_DOI_RE = re.compile(r"^10\.1101/\d")

# A venue that only hosts preprints, for rows with no telling DOI.
_PREPRINT_VENUE_RE = re.compile(
    r"rxiv|research square|\bssrn\b|osf preprints|preprints\.org|authorea|nature precedings",
    re.IGNORECASE,
)
# A venue that keeps copies of other people's papers. OpenAlex types many of
# those copies as preprints, and calls some of them journal articles.
_REPOSITORY_VENUE_RE = re.compile(
    r"zenodo|figshare|repositor|dspace|\bhal\b|pubmed|doaj|\bmpg\.pure\b|scholarlycommons|repec"
    r"|open science framework|publication database|research online|\bpure\b",
    re.IGNORECASE,
)
_REPOSITORY_DOI_PREFIXES = (
    "10.5281/", "10.17605/", "10.6084/", "10.17863/", "10.25673/", "10.17169/", "10.21942/",
    "10.17615/", "10.5167/", "10.15496/", "10.34657/", "10.6082/", "10.3204/", "10.48620/",
)
# Meeting abstracts: Journal of Vision (VSS) and Cognitive Computational Neuroscience.
_ABSTRACT_DOI_PREFIXES = ("10.1167/", "10.32470/")

_PREPRINT_WORK_TYPES = frozenset({"preprint", "posted-content", "posted_content"})
COMPONENT_WORK_TYPES = frozenset(
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
    "recommendations",
    "feed_items",
    "lens_signals",
    # A preprint's kept PDF serves its published root until the root has its
    # own (repointing is UPDATE OR IGNORE, so the root's file wins); the
    # user's "wrong PDF" verdicts follow the paper too.
    "paper_pdfs",
    "paper_pdf_rejections",
)

_ALL_PAPER_SIDECAR_TABLES = frozenset(
    {
        *_PREPRINT_TO_PAPER_MIGRATE_TABLES,
        # Both are artifacts of features that left main: the map (D24) and the
        # semantic partition (D25). Their tables stay on existing installs and
        # nothing reads them, but a merge or a purge must still take their rows
        # with the paper, or dead state outlives the row it described.
        "publication_clusters",
        "semantic_partition_members",
        "publication_embedding_fetch_status",
        "paper_enrichment_status",
        "paper_network_cache",
        # Per-source fetch outcomes describe the child's identifiers; the
        # root retries fresh instead of inheriting them.
        "paper_pdf_attempts",
    }
)


# What a duplicate row hands its survivor: the user's own marks on the paper.
# A duplicate is the same work, so nothing the user did to it may be lost, but
# unlike a preprint it donates no metadata (D15).
_DUPLICATE_USER_STATE_TABLES = (
    "collection_items",
    "publication_tags",
    "recommendations",
    "feed_items",
    "lens_signals",
    # A kept PDF is a file the user has. On a collision the survivor's row wins
    # and `pdfs.store.prune_orphan_files` removes the loser's unreferenced file.
    "paper_pdfs",
    "paper_pdf_rejections",
)

# Strongest first. A merge keeps the strongest state any twin holds.
_FEED_STATUS_RANK = ("love", "like", "add", "dislike", "dismissed", "new")
_MEMBERSHIP_RANK = ("library", "inbox", "removed", "dismissed", "tracked")


def _migrate_tables() -> tuple[str, ...]:
    """Tables a preprint moves onto its published root, add-ons included."""
    return (*_PREPRINT_TO_PAPER_MIGRATE_TABLES, *addons.paper_tables())


def _duplicate_user_state_tables() -> tuple[str, ...]:
    """Tables a duplicate hands to its survivor, add-ons that follow a merge included."""
    return (*_DUPLICATE_USER_STATE_TABLES, *addons.paper_tables(follows_merge=True))


def _sidecar_tables() -> frozenset[str]:
    """Every table holding per-paper state, add-ons included."""
    return _ALL_PAPER_SIDECAR_TABLES | set(addons.paper_tables())


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
    journal: str | None = None,
) -> str | None:
    """Return the preprint source, or ``None`` for a published row.

    The DOI decides first, then the venue. OpenAlex's type comes last and loses
    to a real journal: it calls some published articles preprints, and it calls
    bioRxiv's newest DOIs articles. A stored source of plain ``preprint`` is that
    same type copied onto the row, so it is no evidence of its own.
    """
    lowered = str(doi or "").strip().lower()
    for prefix, source in PREPRINT_DOI_PREFIXES.items():
        if lowered.startswith(prefix) and (prefix != "10.1101/" or _BIORXIV_DOI_RE.match(lowered)):
            return source
    venue = str(journal or "").strip()
    if _PREPRINT_VENUE_RE.search(venue):
        return "preprint_venue"
    persisted = str(preprint_source or "").strip().lower()
    if persisted and persisted not in _PREPRINT_WORK_TYPES:
        return persisted
    wt = str(work_type or "").strip().lower()
    if wt in _PREPRINT_WORK_TYPES and (not venue or _REPOSITORY_VENUE_RE.search(venue)):
        return wt.replace("_", "-")
    return None


def is_preprint_row(row: Mapping[str, Any] | sqlite3.Row) -> bool:
    return bool(
        classify_preprint_source(
            _value(row, "doi"),
            preprint_source=_value(row, "preprint_source"),
            work_type=_value(row, "work_type"),
            journal=_value(row, "journal"),
        )
    )


def is_repository_row(row: Mapping[str, Any] | sqlite3.Row) -> bool:
    """A deposited copy: real, but never the version a group should show."""
    doi = str(_value(row, "doi") or "").strip().lower()
    venue = str(_value(row, "journal") or "")
    return doi.startswith(_REPOSITORY_DOI_PREFIXES) or bool(_REPOSITORY_VENUE_RE.search(venue))


def is_meeting_abstract_row(row: Mapping[str, Any] | sqlite3.Row) -> bool:
    return str(_value(row, "doi") or "").strip().lower().startswith(_ABSTRACT_DOI_PREFIXES)


def is_component_row(row: Mapping[str, Any] | sqlite3.Row) -> bool:
    if str(_value(row, "component_type") or "").strip():
        return True
    if is_preprint_row(row):
        return False
    return str(_value(row, "work_type") or "").strip().lower() in COMPONENT_WORK_TYPES


def _root_rank(row: Mapping[str, Any] | sqlite3.Row) -> int:
    """Published beats a meeting abstract, a deposit and a preprint. Components never root."""
    if is_component_row(row):
        return 0
    if is_preprint_row(row):
        return 100
    if is_repository_row(row):
        return 200
    if is_meeting_abstract_row(row):
        return 250
    return 300


def _title_reads_as_a_title(row: Mapping[str, Any] | sqlite3.Row) -> bool:
    """A capture that fell back on its link, or a publisher's file name, never shows."""
    title = str(_value(row, "title") or "").strip().lower()
    return bool(title) and not title.startswith("10.") and "accepted manuscript" not in title


def _version_rank(row: Mapping[str, Any] | sqlite3.Row) -> float:
    """Within one rank the newest version shows. An unnumbered eLife DOI is the record."""
    doi = _value(row, "doi")
    versioned = doi_version(doi)
    if versioned:
        return float(versioned[1])
    return float("inf") if str(doi or "").lower().startswith("10.7554/elife.") else 0.0


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


def is_standalone_paper(conn: sqlite3.Connection, paper_id: str) -> bool:
    """Whether this row is a paper in its own right, by the one read gate.

    A merged version or a component is inert: writers skip it, or the repair
    deletes what they wrote and the next refresh writes it again.
    """
    # Local import: `core.sql_helpers` imports `core.components`, which imports
    # THIS module.
    from alma.core.sql_helpers import standalone_paper_sql

    row = conn.execute(
        f"SELECT {standalone_paper_sql('p')} AS standalone FROM papers p WHERE p.id = ?",
        (paper_id,),
    ).fetchone()
    return bool(row is not None and row[0])


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

    def key(row: sqlite3.Row) -> tuple[int, int, float, int, int, str]:
        pid = str(row["id"])
        standalone = not str(row["canonical_paper_id"] or "").strip() and not str(
            row["parent_paper_id"] or ""
        ).strip()
        return (
            _root_rank(row),
            int(_title_reads_as_a_title(row)),
            _version_rank(row),
            1 if standalone else 0,
            1 if pid == preferred_id else 0,
            pid,
        )

    return str(max(candidates, key=key)["id"])


#: The schema's own guard on the two relationship pointers. SQLite cannot add a
#: CHECK or a foreign key to an existing table without rebuilding it (`papers` is
#: the largest table in the database), so the rule is expressed as triggers —
#: the one form SQLite can add in place.
#:
#: Two things are refused, matching the invariant this module exists to hold:
#: a row that names ITSELF as its version or parent, and a pointer to a paper
#: that is not in the corpus. Both used to be reachable, and both survive as
#: silent corruption: `resolve_paper_root_id` walks a self-link forever without
#: its cycle guard, and a dangling pointer hides a paper from every standalone
#: read while nothing owns it.
#:
#: The UPDATE trigger fires only when a pointer actually CHANGES. A database
#: repaired later must stay writable in the meantime: rating a paper that
#: already carries a legacy dangling pointer is not the moment to refuse a
#: write. Reconcile clears those, and Health counts them until it does.
_POINTER_VIOLATION = (
    "paper group pointer must name a different paper that exists "
    "(see Health -> paper groups)"
)
_POINTER_INVALID_SQL = """
    (NEW.{column} IS NOT NULL AND TRIM(NEW.{column}) <> '' AND (
        NEW.{column} = NEW.id
        OR NOT EXISTS (SELECT 1 FROM papers WHERE id = NEW.{column})
    ))
"""
PAPER_GROUP_POINTER_DDL: tuple[str, ...] = (
    "DROP TRIGGER IF EXISTS papers_group_pointer_insert",
    "DROP TRIGGER IF EXISTS papers_group_pointer_update",
    f"""CREATE TRIGGER IF NOT EXISTS papers_group_pointer_insert
        BEFORE INSERT ON papers
        WHEN {_POINTER_INVALID_SQL.format(column='canonical_paper_id')}
          OR {_POINTER_INVALID_SQL.format(column='parent_paper_id')}
        BEGIN
            SELECT RAISE(ABORT, '{_POINTER_VIOLATION}');
        END""",
    f"""CREATE TRIGGER IF NOT EXISTS papers_group_pointer_update
        BEFORE UPDATE OF canonical_paper_id, parent_paper_id ON papers
        WHEN (NEW.canonical_paper_id IS NOT OLD.canonical_paper_id
              OR NEW.parent_paper_id IS NOT OLD.parent_paper_id)
         AND ({_POINTER_INVALID_SQL.format(column='canonical_paper_id')}
              OR {_POINTER_INVALID_SQL.format(column='parent_paper_id')})
        BEGIN
            SELECT RAISE(ABORT, '{_POINTER_VIOLATION}');
        END""",
)


def install_paper_group_pointer_guards(conn: sqlite3.Connection) -> None:
    """Create the pointer triggers. Idempotent; used by bootstrap and migration."""
    for statement in PAPER_GROUP_POINTER_DDL:
        conn.execute(statement)


def table_has_paper_id(conn: sqlite3.Connection, table: str) -> bool:
    """True when the table exists with a ``paper_id``: add-on tables may not."""
    try:
        return any(str(row[1]) == "paper_id" for row in conn.execute(f"PRAGMA table_info({table})"))
    except sqlite3.OperationalError:
        return False


def _repoint_table(
    conn: sqlite3.Connection, table: str, loser_id: str, root_id: str
) -> int:
    if not table_has_paper_id(conn, table):
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


def _strongest(values: Iterable[str], rank: tuple[str, ...]) -> str:
    return min(values, key=lambda value: rank.index(value) if value in rank else len(rank))


def strongest_feed_status(statuses: Iterable[str | None]) -> str:
    """The verdict one Feed row keeps when several rows become one."""
    return _strongest({str(status or "new") for status in statuses}, _FEED_STATUS_RANK)


def _settle_feed_status_before_repoint(
    conn: sqlite3.Connection, loser_id: str, root_id: str
) -> None:
    """Give the root's Feed rows the strongest verdict of both papers.

    `feed_items` is UNIQUE(paper_id, author_id), so when both twins came from
    the same monitor the loser's row cannot move and is dropped with its status.
    Every row of a paper shows one verdict (`apply_feed_action` settles them
    together), so writing the strongest one onto the root keeps it. Call before
    the loser's rows are repointed; the rows that do move take it afterwards.
    """
    statuses = {
        str(row[0] or "new")
        for row in conn.execute(
            "SELECT status FROM feed_items WHERE paper_id IN (?, ?)", (loser_id, root_id)
        )
    }
    if len(statuses) > 1:
        conn.execute(
            "UPDATE feed_items SET status = ? WHERE paper_id IN (?, ?)",
            (_strongest(statuses, _FEED_STATUS_RANK), loser_id, root_id),
        )


def _carry_recommendation_actions(
    conn: sqlite3.Connection, loser_id: str, root_id: str
) -> None:
    """Keep a verdict on a Discovery card whose row cannot move.

    One suggestion set holds a paper once, so a loser's row colliding with the
    root's is dropped by the repoint; its action goes to the root's row first.
    """
    conn.execute(
        """
        UPDATE recommendations AS keep
        SET user_action = gone.user_action, action_at = gone.action_at
        FROM recommendations AS gone
        WHERE keep.paper_id = ? AND gone.paper_id = ?
          AND keep.lens_id = gone.lens_id
          AND keep.suggestion_set_id = gone.suggestion_set_id
          AND COALESCE(TRIM(keep.user_action), '') = ''
          AND COALESCE(TRIM(gone.user_action), '') <> ''
        """,
        (root_id, loser_id),
    )


def _repoint_user_state(
    conn: sqlite3.Connection, table: str, loser_id: str, root_id: str
) -> int:
    """Move one table's rows to the root without losing a verdict on a collision."""
    if table == "feed_items":
        _settle_feed_status_before_repoint(conn, loser_id, root_id)
    elif table == "recommendations":
        _carry_recommendation_actions(conn, loser_id, root_id)
    return _repoint_table(conn, table, loser_id, root_id)


def _delete_table_rows(conn: sqlite3.Connection, table: str, paper_id: str) -> int:
    if not table_has_paper_id(conn, table):
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
        ) or utcnow().isoformat()
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


def _is_duplicate_of_root(row: Mapping[str, Any] | sqlite3.Row, root: Mapping[str, Any] | sqlite3.Row) -> bool:
    """A second row of the same work: it hands over what the user did to it."""
    return not is_component_row(row) and not _absorbs_into_root(row, root)


def _carry_user_scalars(
    live_root: Mapping[str, Any] | sqlite3.Row, loser: Mapping[str, Any] | sqlite3.Row
) -> dict[str, Any]:
    """What the loser's curation changes on the root.

    Membership takes the stronger of the two. A Library row's date is the save
    and goes with it, because the root's own date is at best an ingest date.
    """
    updates: dict[str, Any] = {}
    for field in ("notes", "reading_status"):
        root_value = _value(live_root, field)
        loser_value = _value(loser, field)
        if (root_value is None or str(root_value).strip() == "") and loser_value not in (None, ""):
            updates[field] = loser_value
    root_notes = str(_value(live_root, "notes") or "").strip()
    loser_notes = str(_value(loser, "notes") or "").strip()
    if root_notes and loser_notes and loser_notes not in root_notes:
        updates["notes"] = root_notes + "\n\n" + loser_notes
    updates["rating"] = max(int(_value(live_root, "rating") or 0), int(_value(loser, "rating") or 0))

    root_status = str(_value(live_root, "status") or "")
    loser_status = str(_value(loser, "status") or "")
    status = _strongest({root_status, loser_status}, _MEMBERSHIP_RANK)
    if status != root_status:
        updates["status"] = status
        for field in ("added_at", "added_from"):
            if _value(loser, field):
                updates[field] = _value(loser, field)
    return updates


def _upgrade_root_scalars(
    conn: sqlite3.Connection, loser: sqlite3.Row, root: sqlite3.Row
) -> None:
    """Consolidate a child's user state, and a preprint's evidence, into the root."""
    root_id = str(root["id"])
    live_root = conn.execute("SELECT * FROM papers WHERE id = ?", (root_id,)).fetchone() or root
    updates: dict[str, Any] = {}
    if _absorbs_into_root(loser, root):
        for field in ("authors", "abstract", "keywords", "tldr", "added_at", "added_from"):
            root_value = _value(live_root, field)
            loser_value = _value(loser, field)
            if (root_value is None or str(root_value).strip() == "") and loser_value not in (None, ""):
                updates[field] = loser_value
        for field in ("cited_by_count", "influential_citation_count"):
            updates[field] = max(int(_value(live_root, field) or 0), int(_value(loser, field) or 0))
    elif not _is_duplicate_of_root(loser, root):
        return
    updates.update(_carry_user_scalars(live_root, loser))
    updates["updated_at"] = utcnow().isoformat()
    assignments = ", ".join(f"{field} = ?" for field in updates)
    conn.execute(
        f"UPDATE papers SET {assignments} WHERE id = ?",
        [*updates.values(), root_id],
    )


def _invalidate_paper_caches(conn: sqlite3.Connection, paper_id: str) -> int:
    """Discard the per-paper caches keyed on this row."""
    return _delete_table_rows(conn, "paper_network_cache", paper_id) + _delete_table_rows(
        conn, "scoring_cache", paper_id
    )


def _discard_group_derived_views(conn: sqlite3.Connection) -> int:
    """Discard the corpus-wide stored reads that a group change makes wrong.

    These reads do not check fingerprints, and a calibration costs minutes to
    rebuild, so this runs only for a real change: see `_group_state`. The existing
    materialized-view workers own rebuilding, never a GET.
    """
    cleaned = 0
    try:
        cleaned += max(0, int(conn.execute("DELETE FROM similarity_cache").rowcount or 0))
    except sqlite3.OperationalError:
        pass
    try:
        cleaned += max(
            0,
            int(
                conn.execute(
                    "DELETE FROM materialized_views WHERE view_key IN "
                    "('scoring:calibration', 'scoring:outcome_eval')"
                ).rowcount
                or 0
            ),
        )
    except sqlite3.OperationalError:
        pass
    return cleaned


def _group_state(conn: sqlite3.Connection, paper_ids: Iterable[str]) -> list[tuple[Any, ...]]:
    """What the group means to a reader: who its root is and what the user said.

    Stored views read these columns. A stray sidecar on an inert child is not in
    them, because no reader sees that row, so clearing one must leave the views
    alone.
    """
    ids = sorted({str(pid) for pid in paper_ids})
    if not ids:
        return []
    marks = ",".join("?" for _ in ids)
    return [
        tuple(row)
        for row in conn.execute(
            "SELECT id, canonical_paper_id, parent_paper_id, component_type, "
            f"status, rating, reading_status FROM papers WHERE id IN ({marks}) ORDER BY id",
            ids,
        )
    ]


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
        if (str(_value(row, "status") or "") != "tracked"
                or int(_value(row, "rating") or 0)
                or str(_value(row, "reading_status") or "").strip()
                or str(_value(row, "notes") or "").strip()):
            return False
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
    for table in _sidecar_tables():
        if not table_has_paper_id(conn, table):
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


def _clear_subordinate_user_state(conn: sqlite3.Connection, paper_id: str) -> int:
    """Remove independent curation state after any allowed transfer to the root.

    `tracked` is neutral: removing a child is not a negative user verdict.
    The predicate also makes repeated repairs a true no-op.
    """
    return conn.execute(
        "UPDATE papers SET status = 'tracked', rating = 0, reading_status = NULL, "
        "notes = NULL, updated_at = ? WHERE id = ? AND "
        "(status <> 'tracked' OR COALESCE(rating, 0) <> 0 OR "
        "COALESCE(reading_status, '') <> '' OR COALESCE(notes, '') <> '')",
        (utcnow().isoformat(), paper_id),
    ).rowcount


def purge_orphan_subordinate_state(conn: sqlite3.Connection, paper_id: str) -> int:
    """Strip every app sidecar from an unlinked subordinate row.

    There is no root to receive the state yet.  Pointer metadata stays on the
    ``papers`` row; everything that could make it independently interactive is
    removed.  A later authoritative parent link starts from this inert state.
    """
    # What a stored view could have read. Everything else on this row is a stray
    # sidecar that no reader sees.
    signal = _clear_subordinate_user_state(conn, paper_id)
    cleaned = signal
    for table in _ALL_PAPER_SIDECAR_TABLES:
        deleted = _delete_table_rows(conn, table, paper_id)
        cleaned += deleted
        if table == "recommendations":
            signal += deleted
    try:
        deleted = max(
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
        cleaned += deleted
        signal += deleted
    except sqlite3.OperationalError:
        pass
    try:
        deleted = max(
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
        cleaned += deleted
        signal += deleted
    except sqlite3.OperationalError:
        pass
    if cleaned:
        cleaned += _invalidate_paper_caches(conn, paper_id)
    if signal:
        cleaned += _discard_group_derived_views(conn)
    return cleaned


PAPER_GROUP_REJECTIONS_DDL = """
    CREATE TABLE IF NOT EXISTS paper_group_rejections (
        id TEXT PRIMARY KEY,
        paper_id_lo TEXT NOT NULL,
        paper_id_hi TEXT NOT NULL,
        child_id TEXT NOT NULL,
        kind TEXT NOT NULL,
        created_at TEXT NOT NULL,
        UNIQUE (paper_id_lo, paper_id_hi)
    )
"""
PAPER_RELATIONS_DDL = """
    CREATE TABLE IF NOT EXISTS paper_relations (
        paper_id TEXT NOT NULL,
        relation TEXT NOT NULL,
        related_doi TEXT NOT NULL,
        source TEXT NOT NULL,
        created_at TEXT NOT NULL,
        PRIMARY KEY (paper_id, relation, related_doi)
    )
"""
PAPER_RELATIONS_INDEX_DDL = (
    "CREATE INDEX IF NOT EXISTS idx_paper_relations_doi ON paper_relations(related_doi)"
)


def record_paper_relations(
    conn: sqlite3.Connection,
    paper_id: str,
    relations: Iterable[tuple[str, str]] | None,
    *,
    source: str,
) -> int:
    """Store registered same-work links, then let them merge what is already here.

    The other DOI is often not in the corpus yet, so the link waits here and
    `_settle_same_work` finds it when that paper arrives. Inside a write unit.
    """
    now = utcnow().isoformat()
    written = 0
    for relation, doi in relations or []:
        stem = doi_work_stem(doi)
        if stem:
            written += conn.execute(
                "INSERT OR IGNORE INTO paper_relations "
                "(paper_id, relation, related_doi, source, created_at) VALUES (?, ?, ?, ?, ?)",
                (paper_id, relation, stem, source, now),
            ).rowcount
    if written:
        _settle_same_work(conn, resolve_paper_root_id(conn, paper_id, strict=False))
    return written


def _ordered_pair(first: str, second: str) -> tuple[str, str]:
    return (first, second) if first <= second else (second, first)


def rejected_paper_pairs(conn: sqlite3.Connection) -> set[tuple[str, str]]:
    """Every pair the user split, in canonical order."""
    try:
        rows = conn.execute("SELECT paper_id_lo, paper_id_hi FROM paper_group_rejections").fetchall()
    except sqlite3.OperationalError:
        return set()
    return {(str(row[0]), str(row[1])) for row in rows}


def is_split(conn: sqlite3.Connection, first_id: str, second_id: str) -> bool:
    """Did the user say these two are not one work? A keyed read for write sections."""
    try:
        return conn.execute(
            "SELECT 1 FROM paper_group_rejections WHERE paper_id_lo = ? AND paper_id_hi = ?",
            _ordered_pair(first_id, second_id),
        ).fetchone() is not None
    except sqlite3.OperationalError:
        return False


def not_component_ids(conn: sqlite3.Connection) -> set[str]:
    """Rows the user said are papers, whatever their DOI suffix or type says."""
    try:
        rows = conn.execute(
            "SELECT child_id FROM paper_group_rejections WHERE kind = 'not_component'"
        ).fetchall()
    except sqlite3.OperationalError:
        return set()
    return {str(row[0]) for row in rows}


def record_paper_split(
    conn: sqlite3.Connection, child_id: str, root_id: str, *, kind: str
) -> None:
    """Remember "these are not one work". Every matcher and `absorb` honours it."""
    lo, hi = _ordered_pair(child_id, root_id)
    conn.execute(
        "INSERT OR REPLACE INTO paper_group_rejections "
        "(id, paper_id_lo, paper_id_hi, child_id, kind, created_at) VALUES (?, ?, ?, ?, ?, ?)",
        (uuid.uuid4().hex, lo, hi, child_id, kind, utcnow().isoformat()),
    )


def remove_paper_split(conn: sqlite3.Connection, first_id: str, second_id: str) -> bool:
    """Take the verdict back. The next repair may join the two again."""
    lo, hi = _ordered_pair(first_id, second_id)
    return bool(
        conn.execute(
            "DELETE FROM paper_group_rejections WHERE paper_id_lo = ? AND paper_id_hi = ?", (lo, hi)
        ).rowcount
    )


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
    if not loser_id or not keeper_id:
        return {"skipped": True, "reason": "same_id"}
    if loser_id == keeper_id:
        # A row that is its own canonical/parent. Skipping here left `self_links`
        # permanently unrepairable — `_normalize_existing_groups` calls us with
        # (pid, pid) for exactly this row and `continue`d on the skip, so one
        # self-link pinned `self_links` + `cycles` + `chains` forever. Root
        # resolution is meaningless on a self-pointer; clear it and it is a
        # standalone paper again.
        cleared = conn.execute(
            "UPDATE papers SET "
            "canonical_paper_id = CASE WHEN canonical_paper_id = id THEN NULL "
            "ELSE canonical_paper_id END, "
            "parent_paper_id = CASE WHEN parent_paper_id = id THEN NULL "
            "ELSE parent_paper_id END, "
            "updated_at = ? "
            "WHERE id = ? AND (canonical_paper_id = id OR parent_paper_id = id)",
            (utcnow().isoformat(), loser_id),
        ).rowcount
        if cleared:
            return {"skipped": False, "reason": "self_link_cleared", "self_links_cleared": 1}
        return {"skipped": True, "reason": "same_id"}
    if is_split(conn, loser_id, keeper_id):
        return {"skipped": True, "reason": "rejected"}
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
    state_before = _group_state(conn, rows)
    now = utcnow().isoformat()
    migrated: dict[str, int] = {}
    cleaned = feedback_migrated = preferences_migrated = 0
    journal_promoted = root_id != keeper_id and _root_rank(root) > _root_rank(rows[keeper_id])

    for pid, row in rows.items():
        if pid == root_id:
            continue
        _upgrade_root_scalars(conn, row, root)
        if is_component_row(row):
            cleaned += purge_orphan_subordinate_state(conn, pid)
            continue
        absorbs = _absorbs_into_root(row, root)
        tables = _migrate_tables() if absorbs else _duplicate_user_state_tables()
        for table in tables:
            count = _repoint_user_state(conn, table, pid, root_id)
            migrated[table] = migrated.get(table, 0) + count
        feedback_migrated += _merge_feedback(conn, pid, root_id)
        preferences_migrated += _merge_preference_profile(conn, pid, root_id)
        cleaned += _clear_subordinate_user_state(conn, pid)
        # Anything not deliberately migrated must not remain on an inert child.
        for table in _sidecar_tables() - set(tables):
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

    # Recompute root-derived state, and let the corpus-wide views go only when
    # the group really changed: an absorb that only clears strays must not cost
    # a calibration rebuild on every run.
    cleaned += _invalidate_paper_caches(conn, root_id)
    if (
        _group_state(conn, rows) != state_before
        or feedback_migrated
        or preferences_migrated
        or migrated.get("recommendations")
    ):
        cleaned += _discard_group_derived_views(conn)
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


def settle_new_paper_group(
    conn: sqlite3.Connection,
    paper_id: str,
    *,
    component_type: str | None = None,
    parent_paper_id: str | None = None,
    doi: str | None = None,
    reason: str,
) -> str:
    """Place a freshly-written paper row into its group. Returns the ROOT id.

    The ONE ingest-time settle step, shared by every writer that puts a paper in
    the corpus — Library create, Feed ingest, the importer, and the OpenAlex
    upsert. It was copy-pasted (and drifted) across those four: the importer
    handled only the component half, so importing a published paper never
    promoted the preprint already in the corpus, and importing a parent never
    adopted the orphan figure/SI rows that arrived before it. Both leave exactly
    the defects Health's "Paper group integrity" dimension then reports.

    Two mutually exclusive cases:

    - **Component row** (chapter / figure / dataset / erratum): absorb into its
      parent when known, else purge the orphan's sidecar state so an unattached
      fragment can't carry vectors, graph rows or preferences.
    - **Root-capable row**: promote it over any preprint twin already present
      (published wins), then adopt orphan components whose DOI re-derives to
      THIS paper. Both use the resolved root, not the incoming id, so a row that
      just got absorbed adopts on behalf of the surviving paper.

    Caller owns the write transaction — this composes primitives, it never
    commits.
    """
    # Local import: `core.components` imports THIS module (component classification
    # builds on the group primitives), so the dependency only goes one way at
    # module level.
    from alma.core.components import link_orphan_components

    pid = str(paper_id or "").strip()
    if not pid:
        return pid
    if str(component_type or "").strip():
        parent = str(parent_paper_id or "").strip()
        if parent:
            absorb_paper_group(conn, pid, parent, reason=reason)
            return resolve_paper_root_id(conn, pid, strict=False)
        purge_orphan_subordinate_state(conn, pid)
        return pid

    # Promote on the ROOT, not on whatever id we were handed: a row that is
    # already subordinate must not become a second root of its own group.
    root_id = resolve_paper_root_id(conn, pid, strict=False)
    merged = _settle_same_work(conn, root_id)
    merged += int(promote_matching_preprints(conn, root_id)["merged"])
    root_id = resolve_paper_root_id(conn, root_id, strict=False)
    if merged:
        _note_ingest_merge(conn, root_id, merged, reason)
    # Adopt against the ROOT's own DOI: after a promotion the surviving row may
    # not be the one we were handed, and its DOI is what the orphans' suffixes
    # derive from.
    root_doi = str(doi or "").strip()
    if root_id != pid or not root_doi:
        row = conn.execute("SELECT doi FROM papers WHERE id = ?", (root_id,)).fetchone()
        root_doi = str((_value(row, "doi") if row is not None else "") or "").strip()
    if root_doi:
        link_orphan_components(conn, parent_paper_id=root_id, parent_doi=root_doi)
    return root_id


#: S2 gives one id to a preprint and its journal version. Title and authors confirm it.
S2_TITLE_RATIO = 0.9


def authors_agree(first: str | None, second: str | None) -> bool:
    """Both list authors, and at least half of the shorter list is shared."""
    left = {surname(name).casefold() for name in parse_author_names(first or "")}
    right = {surname(name).casefold() for name in parse_author_names(second or "")}
    shared = left & right
    return bool(shared) and len(shared) * 2 >= min(len(left), len(right))


def titles_alike(first: str | None, second: str | None) -> bool:
    """Same title but for wording a journal changed."""
    return (
        difflib.SequenceMatcher(
            None, normalize_title_key(first)[:200], normalize_title_key(second)[:200]
        ).ratio()
        >= S2_TITLE_RATIO
    )


def _settle_same_work(conn: sqlite3.Connection, paper_id: str) -> int:
    """Merge what identifiers alone prove to be this work. Returns the merges.

    The strong rungs of the same-work ladder, readable from indexes, so they are
    cheap inside a write section. The title rung is `promote_matching_preprints`;
    the corpus-wide pass is `library.deduplication.plan_same_work`.
    """
    from alma.core.sql_helpers import standalone_paper_sql

    row = conn.execute(
        "SELECT id, doi, title, authors, semantic_scholar_id FROM papers WHERE id = ?", (paper_id,)
    ).fetchone()
    if row is None:
        return 0
    standalone = standalone_paper_sql("p")
    matches: dict[str, str] = {}
    stem = doi_work_stem(row["doi"])
    if stem:
        for other in conn.execute(
            f"SELECT p.id, p.doi FROM papers p WHERE p.doi >= ? AND p.doi < ? AND {standalone}",
            (stem, f"{stem}~"),
        ):
            if doi_work_stem(other["doi"]) == stem:
                matches[str(other["id"])] = "version"
        for other in conn.execute(
            f"""
            SELECT p.id FROM paper_relations r JOIN papers p ON p.id = r.paper_id
            WHERE r.related_doi = ? AND {standalone}
            UNION
            SELECT p.id FROM paper_relations r JOIN papers p ON p.doi = r.related_doi
            WHERE r.paper_id = ? AND {standalone}
            """,
            (stem, paper_id),
        ):
            matches[str(other["id"])] = "relation"
    s2_id = str(row["semantic_scholar_id"] or "").strip()
    if s2_id:
        for other in conn.execute(
            f"SELECT p.id, p.title, p.authors FROM papers p "
            f"WHERE p.semantic_scholar_id = ? AND {standalone}",
            (s2_id,),
        ):
            if titles_alike(row["title"], other["title"]) and authors_agree(row["authors"], other["authors"]):
                matches.setdefault(str(other["id"]), "semantic_scholar")
    matches.pop(paper_id, None)
    merged = 0
    for other_id, level in matches.items():
        result = absorb_paper_group(conn, other_id, paper_id, reason=f"same_work:{level}")
        merged += int(not result.get("skipped"))
    return merged


def _note_ingest_merge(conn: sqlite3.Connection, root_id: str, merged: int, reason: str) -> None:
    """One Activity row for a merge nobody asked for. Written after the commit."""
    from alma.core.db_write import run_after_gate_release
    from alma.core.operations.activity import record_foreground_action

    title = conn.execute("SELECT title FROM papers WHERE id = ?", (root_id,)).fetchone()
    message = (
        f"Merged {merged} {'version' if merged == 1 else 'versions'} of "
        f"\"{str(title[0] if title else '')[:80]}\""
    )

    def write() -> None:
        record_foreground_action(
            conn,
            operation_key="papers.action.merge",
            message=message,
            result={"paper_id": root_id, "merged": merged, "reason": reason},
            job_id=f"paper_merge_{uuid.uuid4().hex[:10]}",
        )

    run_after_gate_release(write, conn=conn, label="paper merge note")


# The relationship defect vocabulary, in the order Health explains it. Health
# owns the user-facing labels/explanations; this module owns the DETECTION and
# the key names, so the ledger and the counts can never drift apart.
PAPER_GROUP_DEFECT_KEYS: tuple[str, ...] = (
    "dangling_canonical",
    "dangling_parent",
    "self_links",
    "cycles",
    "chains",
    "component_roots",
    "published_under_preprint",
    "orphan_components",
    "subordinate_sidecars",
    "subordinate_user_state",
    "ambiguous_preprints",
)


def _has_pointer_cycle(paper_id: str, by_id: dict[str, Any]) -> bool:
    """In-memory equivalent of ``resolve_paper_root_id``'s cycle guard.

    Same walk (canonical first, then parent), but over the rows we already
    fetched instead of one SELECT per hop — the whole scan is a single query.
    A dangling hop is NOT a cycle: it ends the walk and is counted by its own
    ``dangling_*`` defect.
    """
    seen: set[str] = set()
    current = paper_id
    while current:
        if current in seen:
            return True
        seen.add(current)
        row = by_id.get(current)
        if row is None:
            return False
        nxt = str(row["canonical_paper_id"] or "").strip() or str(
            row["parent_paper_id"] or ""
        ).strip()
        if not nxt:
            return False
        current = nxt
    return False


def paper_group_defect_map(
    conn: sqlite3.Connection, *, preprint_index: PreprintTitleIndex | None = None
) -> dict[str, dict[str, int]]:
    """Row-level relationship-defect ledger: ``paper_id -> {defect_key: count}``.

    The single detection pass behind BOTH ``relationship_integrity_counts`` (its
    per-key sum) and the Health drilldown that lists *which* papers are broken —
    so the card's number and the list it opens can't disagree.

    Every defect counts one occurrence per paper except ``subordinate_sidecars``,
    which counts the child's leftover sidecar ROWS (a single child can hold many).
    """
    ledger: dict[str, dict[str, int]] = {}

    def flag(paper_id: str, defect: str, occurrences: int = 1) -> None:
        if occurrences <= 0:
            return
        entry = ledger.setdefault(paper_id, {})
        entry[defect] = entry.get(defect, 0) + occurrences

    rows = conn.execute(
        "SELECT id, doi, journal, work_type, preprint_source, canonical_paper_id, "
        "parent_paper_id, component_type, status, rating, reading_status, notes FROM papers"
    ).fetchall()
    by_id = {str(row["id"]): row for row in rows}
    kept_as_papers = not_component_ids(conn)
    for row in rows:
        pid = str(row["id"])
        canonical = str(row["canonical_paper_id"] or "").strip()
        parent = str(row["parent_paper_id"] or "").strip()
        if canonical and canonical not in by_id:
            flag(pid, "dangling_canonical")
        if parent and parent not in by_id:
            flag(pid, "dangling_parent")
        if pid in {canonical, parent}:
            flag(pid, "self_links")
        if (canonical or parent or is_component_row(row)) and (
            str(row["status"] or "") != "tracked" or int(row["rating"] or 0)
            or str(row["reading_status"] or "").strip() or str(row["notes"] or "").strip()
        ):
            flag(pid, "subordinate_user_state")
        target = by_id.get(canonical or parent)
        if target is not None:
            if str(target["canonical_paper_id"] or "").strip() or str(
                target["parent_paper_id"] or ""
            ).strip():
                flag(pid, "chains")
            if is_component_row(target):
                flag(pid, "component_roots")
            if canonical and not is_preprint_row(row) and is_preprint_row(target):
                flag(pid, "published_under_preprint")
        if is_component_row(row) and not parent and pid not in kept_as_papers:
            flag(pid, "orphan_components")
        if _has_pointer_cycle(pid, by_id):
            flag(pid, "cycles")

    # Leftover sidecar state on subordinate rows — counted per row so the fix
    # ("purge N sidecar rows") is measured in the units it actually removes.
    subordinate_ids = [
        str(row["id"])
        for row in rows
        if str(row["canonical_paper_id"] or "").strip() or is_component_row(row)
    ]
    for table in _sidecar_tables():
        if not subordinate_ids or not table_has_paper_id(conn, table):
            continue
        placeholders = ",".join("?" for _ in subordinate_ids)
        try:
            sidecar_rows = conn.execute(
                f"SELECT paper_id, COUNT(*) AS c FROM {table} "
                f"WHERE paper_id IN ({placeholders}) GROUP BY paper_id",
                subordinate_ids,
            ).fetchall()
        except sqlite3.OperationalError:
            continue
        for sidecar in sidecar_rows:
            flag(str(sidecar["paper_id"]), "subordinate_sidecars", int(sidecar["c"] or 0))
    # The caller may already hold the index (the repair preview builds one to
    # plan with): scanning and re-normalizing every title a second time is the
    # most expensive thing this function could do twice.
    index = preprint_index if preprint_index is not None else build_preprint_title_index(conn)
    for pair in index.pairs(require_doi=True):
        if pair["ambiguous"]:
            flag(pair["preprint_id"], "ambiguous_preprints")
    return ledger


def relationship_integrity_counts(
    conn: sqlite3.Connection, *, preprint_index: PreprintTitleIndex | None = None
) -> dict[str, int]:
    """Return relationship defects used by Health and reconciliation previews."""
    counts = {key: 0 for key in PAPER_GROUP_DEFECT_KEYS}
    for defects in paper_group_defect_map(conn, preprint_index=preprint_index).values():
        for key, occurrences in defects.items():
            counts[key] += int(occurrences or 0)
    return counts


class PreprintTitleIndex:
    """Standalone title buckets shared by ingest, repair and pending counts.

    A preprint pairs with a published row when that is the only published work it
    fits. Several preprints of one journal paper are the ordinary case, not a
    tie. A preprint that fits two published works is review evidence, never a tie
    to break. Linked versions cannot be stolen from an existing group, and a pair
    the user split is never offered.
    """

    def __init__(self, rows: list[Any], rejected: set[tuple[str, str]] | None = None) -> None:
        self._by_title: dict[str, list[Any]] = {}
        self._rejected = rejected or set()
        for row in rows:
            if not is_component_row(row):
                key = normalize_title_key(str(row["title"] or ""))
                if key:
                    self._by_title.setdefault(key, []).append(row)

    def candidates(self, title_key: str) -> list[Any]:
        return [r for r in self._by_title.get(title_key, []) if is_preprint_row(r)]

    def forget(self, preprint_id: str) -> None:
        for rows in self._by_title.values():
            for i, row in enumerate(rows):
                if str(row["id"]) == str(preprint_id):
                    del rows[i]
                    return

    def _fits(self, preprint: Any, published: Any, year_tolerance: int) -> bool:
        if _ordered_pair(str(preprint["id"]), str(published["id"])) in self._rejected:
            return False
        if preprint["year"] is None or published["year"] is None:
            return False
        gap = int(published["year"]) - int(preprint["year"])
        if abs(gap) <= year_tolerance:
            return True
        # Publication can take years. The authors say it is still this work.
        return 0 < gap <= PUBLICATION_LAG_YEARS and authors_agree(preprint["authors"], published["authors"])

    def pairs(
        self,
        *,
        year_tolerance: int = 2,
        title_key: str | None = None,
        require_doi: bool = False,
    ) -> list[dict[str, Any]]:
        """Return safe pairs and explicit ambiguous candidates from one snapshot.

        ``require_doi`` keeps a registered identifier on BOTH sides. Every path
        that merges without a person looking passes it: a shared title and year
        alone is how two different conference papers become one. The ingest
        promotion keeps the looser rule it has always had, because there the
        published row is the one just written and its twin is being claimed
        immediately, not swept up corpus-wide.
        """
        result = []
        buckets = self._by_title.values() if title_key is None else [self._by_title.get(title_key, [])]
        for rows in buckets:
            if require_doi:
                rows = [r for r in rows if str(r["doi"] or "").strip()]
            preprints = [r for r in rows if is_preprint_row(r)]
            journals = [r for r in rows if not is_preprint_row(r)]
            for p in preprints:
                matches = [j for j in journals if self._fits(p, j, year_tolerance)]
                if not matches:
                    continue
                # A deposit or an abstract beside the journal row is no rival.
                best = max(_root_rank(j) for j in matches)
                top = [j for j in matches if _root_rank(j) == best]
                works = {doi_work_stem(j["doi"]) or str(j["id"]) for j in top}
                unique = len(works) == 1
                j = top[0] if unique else None
                result.append({
                    "preprint_id": str(p["id"]),
                    "canonical_id": str(j["id"]) if j is not None else None,
                    "canonical_candidates": sorted(str(m["id"]) for m in matches),
                    "ambiguous": not unique,
                    "preprint_doi": p["doi"],
                    "canonical_doi": j["doi"] if j is not None else None,
                    "preprint_source": classify_preprint_source(
                        p["doi"], preprint_source=p["preprint_source"], work_type=p["work_type"],
                        journal=_value(p, "journal")),
                    "title": str(p["title"] or "")[:240],
                    "year": int(p["year"]),
                    "confidence": round(1 - abs(int(p["year"]) - int(j["year"])) * .05, 3)
                                  if j is not None else None,
                    "library": p["status"] == "library" or any(m["status"] == "library" for m in matches),
                })
        # Safe pairs first, best evidence first: a capped run must spend its
        # budget on the most confident matches, not on whatever sorted earliest.
        return sorted(
            result,
            key=lambda p: (
                1 if p["ambiguous"] else 0,
                -(p["confidence"] or 0.0),
                p["year"],
                p["preprint_id"],
            ),
        )


def build_preprint_title_index(conn: sqlite3.Connection) -> PreprintTitleIndex:
    """Read the complete eligible pool once, before any matching mutation."""
    from alma.core.sql_helpers import standalone_paper_sql

    rows = conn.execute(
        f"SELECT {_PREPRINT_INDEX_COLUMNS} FROM papers p WHERE {standalone_paper_sql('p')}"
    ).fetchall()
    return PreprintTitleIndex(rows, rejected_paper_pairs(conn))


_PREPRINT_INDEX_COLUMNS = (
    "id, title, year, doi, journal, authors, work_type, preprint_source, component_type, status"
)
#: How long after its preprint a journal version still counts when the authors agree.
PUBLICATION_LAG_YEARS = 6


def _preprint_candidate_sql(alias: str = "p") -> str:
    """SQL prune for rows that can be preprints. It only prunes.

    `is_preprint_row` still decides. This is the three kinds of evidence
    `classify_preprint_source` reads, so a preprint cannot fall out of it. It is
    also the predicate of `PREPRINT_YEAR_INDEX_SQL`: SQLite uses a partial index
    only for a query that repeats its predicate, so both come from here.
    """
    col = f"{alias}." if alias else ""
    work_types = ", ".join(f"'{wt}'" for wt in sorted(_PREPRINT_WORK_TYPES))
    prefixes = " OR ".join(
        f"LOWER(TRIM(COALESCE({col}doi, ''))) LIKE '{prefix}%'"
        for prefix in PREPRINT_DOI_PREFIXES
    )
    return (
        f"(COALESCE(TRIM({col}preprint_source), '') <> '' "
        f"OR LOWER(TRIM(COALESCE({col}work_type, ''))) IN ({work_types}) "
        f"OR {prefixes} "
        f"OR LOWER(COALESCE({col}journal, '')) LIKE '%rxiv%' "
        f"OR LOWER(COALESCE({col}journal, '')) LIKE '%research square%' "
        f"OR LOWER(COALESCE({col}journal, '')) LIKE '%ssrn%' "
        f"OR LOWER(COALESCE({col}journal, '')) LIKE '%preprint%' "
        f"OR LOWER(COALESCE({col}journal, '')) LIKE '%authorea%' "
        f"OR LOWER(COALESCE({col}journal, '')) LIKE '%precedings%')"
    )


# Asserted by the bootstrap DDL. Promotion runs once per ingested work inside a
# write section, and each of its reads has to touch the few rows it needs, not
# walk `papers`, whose rows carry abstracts.
PREPRINT_YEAR_INDEX_SQL = (
    "CREATE INDEX IF NOT EXISTS idx_papers_preprint_year ON papers(year) "
    f"WHERE {_preprint_candidate_sql('')}"
)


def preprint_index_for_paper(
    conn: sqlite3.Connection,
    paper_id: str,
    *,
    year_tolerance: int = 2,
) -> PreprintTitleIndex:
    """The slice of the title index that ONE paper's promotion can reach.

    Every ingest settles inside a write section, so its cost is paid while the
    writer gate is held. Building the whole index there read and normalized the
    corpus once per work: 0.22 s a work on 18,000 papers, 46 s for one 200-work
    chunk of an author refresh, with every foreground write queued behind it
    (2026-10-31). A write unit must stay short however large the corpus grows.

    The slice gives the same answer as the full index, from either side: a
    journal row looks back for its preprints, a preprint looks forward for its
    journal row. Rows with no year form no edge.

    Almost no ingest has a twin, so the first query reads only the few rows that
    could share the title and returns empty when none does.
    """
    from alma.core.sql_helpers import standalone_paper_sql

    empty = PreprintTitleIndex([])
    row = conn.execute(
        f"SELECT {_PREPRINT_INDEX_COLUMNS} FROM papers WHERE id = ?", (paper_id,)
    ).fetchone()
    if row is None or row["year"] is None or is_component_row(row):
        return empty
    title_key = normalize_title_key(str(row["title"] or ""))
    if not title_key:
        return empty

    year = int(row["year"])
    standalone = standalone_paper_sql("p")
    arrived_preprint = is_preprint_row(row)
    if arrived_preprint:
        # A preprint arriving after its journal version: look forward in time,
        # and let SQLite drop what shares no word with this title.
        word = max(re.findall(r"[^\W\d_]{5,}", str(row["title"] or "")) or [""], key=len)
        if not word:
            return empty
        lo, hi = year - year_tolerance, year + PUBLICATION_LAG_YEARS
        narrow = "p.title LIKE ?"
        narrow_params: tuple[str, ...] = (f"%{word}%",)
    else:
        lo, hi = year - PUBLICATION_LAG_YEARS, year + year_tolerance
        narrow = _preprint_candidate_sql("p")
        narrow_params = ()
    # One string, not one row per title: each fetched row is a round trip through
    # the GIL, and this runs per work inside a write section.
    titles = conn.execute(
        f"SELECT group_concat(p.title, char(31)) FROM papers p WHERE {standalone} "
        f"AND p.id <> ? AND p.year BETWEEN ? AND ? AND {narrow}",
        (paper_id, lo, hi, *narrow_params),
    ).fetchone()[0]
    if not any(normalize_title_key(title) == title_key for title in (titles or "").split("\x1f")):
        return empty

    # Every row the pairing reads: both ends of an edge can sit a full lag apart.
    rows = conn.execute(
        f"SELECT {_PREPRINT_INDEX_COLUMNS} FROM papers p WHERE {standalone} "
        "AND p.year BETWEEN ? AND ?",
        (year - PUBLICATION_LAG_YEARS - year_tolerance, year + PUBLICATION_LAG_YEARS + year_tolerance),
    ).fetchall()
    return PreprintTitleIndex(
        [r for r in rows if normalize_title_key(str(r["title"] or "")) == title_key],
        rejected_paper_pairs(conn),
    )


def promote_matching_preprints(
    conn: sqlite3.Connection,
    published_paper_id: str,
    *,
    year_tolerance: int = 2,
    preprint_index: PreprintTitleIndex | None = None,
) -> dict[str, int]:
    """Apply only unambiguous standalone matches through the group owner.

    Runs inside write units, so with no ``preprint_index`` it reads the small
    slice from `preprint_index_for_paper`, never the whole corpus.
    """
    index = (
        preprint_index
        if preprint_index is not None
        else preprint_index_for_paper(conn, published_paper_id, year_tolerance=year_tolerance)
    )
    row = conn.execute("SELECT title FROM papers WHERE id = ?", (published_paper_id,)).fetchone()
    if row is None:
        return {"candidates": 0, "merged": 0, "reparented": 0, "ambiguous": 0}
    pairs = [p for p in index.pairs(year_tolerance=year_tolerance, title_key=normalize_title_key(row["title"] or ""))
             if published_paper_id in (p["preprint_id"], *p["canonical_candidates"])]
    merged = reparented = ambiguous = 0
    for pair in pairs:
        if pair["ambiguous"]:
            ambiguous += 1
            continue
        result = absorb_paper_group(conn, pair["preprint_id"], pair["canonical_id"],
                                    reason="journal_publication_promotion")
        index.forget(pair["preprint_id"])
        if not result.get("skipped"):
            merged += 1
            reparented += int(result.get("reparented") or 0)
    return {"candidates": len(pairs), "merged": merged, "reparented": reparented,
            "ambiguous": ambiguous}
