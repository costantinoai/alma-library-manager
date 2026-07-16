"""Preprint↔journal dedup engine.

Single responsibility: collapse pairs where the same work exists as both
a preprint (arXiv / bioRxiv / psyRxiv / chemRxiv / OSF) row and a
published journal row into one canonical (journal) row, migrating all
FK data so Library + Discovery surfaces see exactly one card.

Detection signals (in decreasing confidence):

1. **Preprint-DOI prefix + canonical title-key + year-proximity.**
   ``10.48550/arXiv.*`` / ``10.1101/*`` / ``10.31234/*`` / ``10.26434/chemrxiv*``
   / ``10.31219/*`` are the vendor-specific DOI prefixes. Title normalised
   via ``normalize_title_key`` (punctuation- and whitespace-insensitive)
   and year within ±2 is the main high-precision signal.
2. **SPECTER2 cosine ≥ 0.98** when both rows have vectors in
   ``publication_embeddings`` for the active model. Used as a
   disambiguator when the title match is weak (e.g. title changed
   between preprint and published version).

Every merge is idempotent:
- Preprint row keeps its UUID (FK integrity for historical feedback).
- Preprint row gets ``canonical_paper_id = <journal_id>`` stamped.
- Library / Discovery reads filter ``canonical_paper_id IS NULL``.
- FK children (publication_authors, publication_topics,
  publication_references, publication_embeddings, recommendations,
  feedback_events, collection_items, publication_tags) migrate from
  preprint → canonical via ``UPDATE OR IGNORE`` patterns so duplicates
  collapse cleanly.

Monotonic-signal fields (``cited_by_count``, ``rating``,
``reading_status``) take the stronger value. Author-supplied fields
(``notes``, ``added_from``) keep the Library-saved row's value when
only one side is in Library.
"""

from __future__ import annotations

import logging
import sqlite3
from datetime import datetime
from typing import Any

from alma.core.db_write import write_section
from alma.core.sql_helpers import canonical_paper_filter
from alma.core.utils import normalize_title_key

logger = logging.getLogger(__name__)


# DOI prefixes that identify a preprint venue. The first segment after
# `10.` is the registrant — each preprint server has a fixed one.
PREPRINT_DOI_PREFIXES: dict[str, str] = {
    "10.48550/arxiv": "arxiv",
    "10.1101/": "biorxiv",  # Covers both bioRxiv and medRxiv (same registrant).
    "10.31234/": "psyrxiv",
    "10.31219/": "osf",
    "10.26434/chemrxiv": "chemrxiv",
    "10.20944/preprints": "mdpi_preprints",
}

# Year proximity allowed between preprint and published version.
_YEAR_TOLERANCE = 2

# Default title-key threshold. We use an exact match on the normalised
# key by default — the key already collapses punctuation/whitespace, so
# any non-match almost certainly means genuinely different works.
_TITLE_EXACT = 1.0


def classify_preprint_source(doi: str | None) -> str | None:
    """Return the preprint source tag for a DOI, or None for published journals."""
    if not doi:
        return None
    lowered = doi.strip().lower()
    for prefix, tag in PREPRINT_DOI_PREFIXES.items():
        if lowered.startswith(prefix):
            return tag
    return None


# -- detection ----------------------------------------------------------------


def find_preprint_twin_candidates(
    conn: sqlite3.Connection,
    *,
    limit: int | None = None,
    scope: str = "corpus",
) -> list[dict[str, Any]]:
    """Return candidate twin pairs to merge.

    Pair shape: ``{preprint_id, canonical_id, preprint_doi,
    canonical_doi, title, year, confidence}``. Only papers without an
    existing ``canonical_paper_id`` are eligible — idempotent reruns.

    ``scope`` narrows the pool:
    - ``library`` — only pairs where at least one side has
      ``status = 'library'``.
    - ``corpus`` — every pair (default).
    """
    scope = (scope or "corpus").strip().lower()
    if scope not in {"library", "corpus"}:
        scope = "corpus"

    sql = f"""
        SELECT id, title, year, doi, status, canonical_paper_id
        FROM papers
        WHERE {canonical_paper_filter('papers')}
          AND COALESCE(title, '') <> ''
          AND year IS NOT NULL
          AND doi IS NOT NULL AND TRIM(doi) <> ''
    """
    rows = conn.execute(sql).fetchall()

    preprint_rows: dict[str, list[sqlite3.Row]] = {}
    canonical_rows: dict[str, list[sqlite3.Row]] = {}
    for row in rows:
        key = (normalize_title_key(row["title"]), int(row["year"]))
        if not key[0]:
            continue
        source = classify_preprint_source(row["doi"])
        bucket = preprint_rows if source else canonical_rows
        bucket.setdefault(key[0], []).append(row)

    candidates: list[dict[str, Any]] = []
    for tkey, preprints in preprint_rows.items():
        for preprint in preprints:
            best_canonical = None
            best_year_delta = None
            for candidate in canonical_rows.get(tkey, []):
                delta = abs(int(candidate["year"]) - int(preprint["year"]))
                if delta > _YEAR_TOLERANCE:
                    continue
                if best_year_delta is None or delta < best_year_delta:
                    best_year_delta = delta
                    best_canonical = candidate
            if not best_canonical:
                continue
            # Library-scope filter: at least one side must be saved.
            if scope == "library" and not (
                str(preprint["status"] or "") == "library"
                or str(best_canonical["status"] or "") == "library"
            ):
                continue
            confidence = _TITLE_EXACT - (best_year_delta or 0) * 0.05
            candidates.append(
                {
                    "preprint_id": preprint["id"],
                    "canonical_id": best_canonical["id"],
                    "preprint_doi": preprint["doi"],
                    "canonical_doi": best_canonical["doi"],
                    "preprint_source": classify_preprint_source(preprint["doi"]),
                    "title": str(preprint["title"] or "").strip()[:240],
                    "year": int(preprint["year"]),
                    "confidence": round(confidence, 3),
                }
            )
    candidates.sort(key=lambda c: (-c["confidence"], c["year"]))
    if limit is not None:
        candidates = candidates[:limit]
    return candidates


# -- merge --------------------------------------------------------------------


_FK_TABLES_TO_MIGRATE: list[tuple[str, tuple[str, ...]]] = [
    # (table, unique-key columns used to collapse duplicates on migration)
    ("publication_authors", ("paper_id", "openalex_id")),
    ("publication_topics", ("paper_id", "term")),
    ("publication_references", ("paper_id", "referenced_work_id")),
    ("publication_embeddings", ("paper_id", "model", "source")),
    ("publication_tags", ("paper_id", "tag_id")),
    ("publication_institutions", ("paper_id", "openalex_id")),
    ("collection_items", ("collection_id", "paper_id")),
    ("publication_clusters", ("paper_id",)),
]

# Tables with a standalone unique ``id`` PK (NOT a paper-scoped composite key):
# the ``_migrate_fk_rows`` INSERT-copy helper would clone the loser's ``id`` and
# self-collide on the PK (OR IGNORE → row dropped, then DELETE loses it), so
# these repoint ``paper_id`` in place via UPDATE OR IGNORE + DELETE instead.
#  - recommendations: UNIQUE(lens_id, paper_id, suggestion_set_id)
#  - feed_items: UNIQUE(paper_id, author_id). MUST migrate — the Feed read does
#    NOT filter canonical rows, so an un-migrated feed_item keeps the hidden
#    loser visible in the inbox (the v0.19.0 duplicate symptom).
_ID_PK_TABLES_TO_REPOINT: tuple[str, ...] = ("recommendations", "feed_items")


def _repoint_paper_id(conn: sqlite3.Connection, table: str, loser_id: str, keeper_id: str) -> None:
    """Move a standalone-``id`` table's rows loser → keeper in place.

    UPDATE OR IGNORE rewrites ``paper_id`` (keeping each row's own PK ``id``);
    a UNIQUE collision leaves the loser row, which the DELETE then drops."""
    try:
        conn.execute(
            f"UPDATE OR IGNORE {table} SET paper_id = ? WHERE paper_id = ?",
            (keeper_id, loser_id),
        )
        conn.execute(f"DELETE FROM {table} WHERE paper_id = ?", (loser_id,))
    except sqlite3.OperationalError as exc:
        logger.debug("repoint skipped on %s: %s", table, exc)


def _migrate_fk_rows(
    conn: sqlite3.Connection,
    preprint_id: str,
    canonical_id: str,
    *,
    table: str,
    unique_cols: tuple[str, ...],
) -> int:
    """Re-point FK rows from preprint_id to canonical_id.

    Uses `INSERT OR IGNORE ... SELECT` to copy only rows the canonical
    doesn't already have, then DELETE the preprint-side rows.
    """
    try:
        cols = [
            r[1]
            for r in conn.execute(f"PRAGMA table_info({table})").fetchall()
        ]
    except sqlite3.OperationalError:
        return 0
    if not cols:
        return 0

    col_csv = ", ".join(cols)
    select_cols = ", ".join(
        "?" if c == "paper_id" else c for c in cols
    )
    sql_insert = (
        f"INSERT OR IGNORE INTO {table} ({col_csv}) "
        f"SELECT {select_cols} FROM {table} WHERE paper_id = ?"
    )
    try:
        # Insert the canonical-side copies first, then drop preprint-side rows.
        conn.execute(sql_insert, (canonical_id, preprint_id))
        deleted = conn.execute(
            f"DELETE FROM {table} WHERE paper_id = ?",
            (preprint_id,),
        ).rowcount
        return int(deleted or 0)
    except sqlite3.OperationalError as exc:
        logger.debug("fk migrate skipped on %s: %s", table, exc)
        return 0


def _upgrade_canonical_from_preprint(
    conn: sqlite3.Connection,
    preprint_id: str,
    canonical_id: str,
) -> None:
    """Copy missing fields from preprint → canonical via COALESCE.

    The canonical row (journal version) has priority for every field.
    Only populate canonical fields that are currently empty/null.
    Library status and rating are upgraded: if the preprint was in
    Library, the canonical gets promoted too.
    """
    try:
        conn.execute(
            """
            UPDATE papers
            SET
                abstract = CASE
                    WHEN COALESCE(abstract, '') = ''
                    THEN (SELECT abstract FROM papers WHERE id = ?)
                    ELSE abstract
                END,
                url = CASE
                    WHEN COALESCE(url, '') = ''
                    THEN (SELECT url FROM papers WHERE id = ?)
                    ELSE url
                END,
                cited_by_count = MAX(
                    COALESCE(cited_by_count, 0),
                    COALESCE((SELECT cited_by_count FROM papers WHERE id = ?), 0)
                ),
                rating = MAX(
                    COALESCE(rating, 0),
                    COALESCE((SELECT rating FROM papers WHERE id = ?), 0)
                ),
                notes = CASE
                    WHEN COALESCE(notes, '') = ''
                    THEN (SELECT notes FROM papers WHERE id = ?)
                    ELSE notes
                END,
                status = CASE
                    WHEN status = 'library' THEN status
                    WHEN (SELECT status FROM papers WHERE id = ?) = 'library' THEN 'library'
                    ELSE status
                END,
                added_at = COALESCE(added_at, (SELECT added_at FROM papers WHERE id = ?)),
                added_from = COALESCE(added_from, (SELECT added_from FROM papers WHERE id = ?)),
                updated_at = ?
            WHERE id = ?
            """,
            (
                preprint_id, preprint_id, preprint_id, preprint_id,
                preprint_id, preprint_id, preprint_id, preprint_id,
                datetime.utcnow().isoformat(), canonical_id,
            ),
        )
    except sqlite3.OperationalError as exc:
        logger.warning("canonical upgrade failed: %s", exc)


def merge_duplicate_paper_rows(
    conn: sqlite3.Connection,
    loser_id: str,
    keeper_id: str,
    *,
    reason: str | None = None,
) -> dict[str, Any]:
    """Soft-merge a duplicate paper row (``loser_id``) into its keeper.

    The ONE generic FK-rewiring collapse shared by every duplicate-pair
    merge: preprint↔journal twins (:func:`merge_preprint_into_canonical`),
    same-``openalex_id`` duplicate-identity pairs discovered during title
    resolution (``title_resolution`` / ``corpus_rehydrate``), and the
    retro-collapse repair (:func:`run_duplicate_identity_collapse`).

    SOFT by construction (product decision D3): the loser row is NEVER
    hard-deleted — it is stamped ``canonical_paper_id = keeper_id`` so every
    read-side ``standalone_paper_sql`` / ``canonical_paper_filter`` hides it
    while the Corpus explorer keeps its provenance and Discovery still reads
    its ``removed`` signal. Idempotent — a second call on an already-merged
    pair is a no-op.

    Migrates every FK child + ``feedback_events`` + ``recommendations`` from
    loser → keeper, fills the keeper's empty scalars from the loser (keeper
    wins on conflict), then stamps the pointer. ``reason`` (when given) is
    recorded in the loser's ``openalex_resolution_reason`` for the Corpus
    explorer without overwriting an existing reason.

    Returns a summary dict; ``{"skipped": True, "reason": ...}`` when the pair
    is invalid or already merged.
    """
    if not loser_id or not keeper_id or loser_id == keeper_id:
        return {"skipped": True, "reason": "same_id"}

    row = conn.execute(
        "SELECT canonical_paper_id FROM papers WHERE id = ?",
        (loser_id,),
    ).fetchone()
    if row is None:
        return {"skipped": True, "reason": "loser_missing"}
    if str(row["canonical_paper_id"] or "").strip() == keeper_id:
        return {"skipped": True, "reason": "already_merged"}
    if conn.execute(
        "SELECT 1 FROM papers WHERE id = ?", (keeper_id,)
    ).fetchone() is None:
        return {"skipped": True, "reason": "keeper_missing"}

    # 1. Migrate FK children first (INSERT OR IGNORE + DELETE per table) so the
    #    keeper acquires everything before its scalar upgrade.
    migrated: dict[str, int] = {}
    for table, unique_cols in _FK_TABLES_TO_MIGRATE:
        migrated[table] = _migrate_fk_rows(
            conn, loser_id, keeper_id, table=table, unique_cols=unique_cols
        )

    # 2. feedback_events keyed by (entity_type, entity_id) — NOT a `paper_id`
    #    column — so the loser's likes/dismisses move to the keeper via a bare
    #    UPDATE on entity_id (not unique, so no collision to guard).
    try:
        conn.execute(
            "UPDATE feedback_events SET entity_id = ? "
            "WHERE entity_type IN ('publication', 'paper') AND entity_id = ?",
            (keeper_id, loser_id),
        )
    except sqlite3.OperationalError:
        pass

    # 3. Standalone-`id` tables (recommendations, feed_items): repoint paper_id
    #    in place so the row keeps its own PK (see _ID_PK_TABLES_TO_REPOINT).
    for table in _ID_PK_TABLES_TO_REPOINT:
        _repoint_paper_id(conn, table, loser_id, keeper_id)

    # 4. Scalar upgrade: copy the loser's fields into the keeper's empty ones.
    _upgrade_canonical_from_preprint(conn, loser_id, keeper_id)

    # 5. Stamp the soft-merge pointer (no hard delete — D3).
    conn.execute(
        """
        UPDATE papers
        SET canonical_paper_id = ?,
            openalex_resolution_reason = CASE
                WHEN ? <> '' AND COALESCE(TRIM(openalex_resolution_reason), '') = ''
                THEN ? ELSE openalex_resolution_reason
            END,
            updated_at = ?
        WHERE id = ?
        """,
        (keeper_id, reason or "", reason or "", datetime.utcnow().isoformat(), loser_id),
    )
    return {
        "skipped": False,
        "loser_id": loser_id,
        "keeper_id": keeper_id,
        "fk_migrated": migrated,
    }


def merge_preprint_into_canonical(
    conn: sqlite3.Connection,
    preprint_id: str,
    canonical_id: str,
) -> dict[str, Any]:
    """Collapse the preprint into the canonical journal row.

    Thin preprint-flavored wrapper over :func:`merge_duplicate_paper_rows`
    (the generic soft-merge): the only preprint-specific step is tagging the
    collapsed row with its venue ``preprint_source``. Idempotent — calling
    again on an already-merged pair is a no-op.
    """
    if preprint_id == canonical_id:
        return {"skipped": True, "reason": "same_id"}

    # Infer the venue source tag before the merge (needs the loser's DOI).
    preprint_row = conn.execute(
        "SELECT id, doi, preprint_source FROM papers WHERE id = ?",
        (preprint_id,),
    ).fetchone()
    if not preprint_row:
        return {"skipped": True, "reason": "preprint_missing"}
    preprint_source = (
        str(preprint_row["preprint_source"] or "").strip()
        or classify_preprint_source(preprint_row["doi"])
        or "unknown"
    )

    result = merge_duplicate_paper_rows(conn, preprint_id, canonical_id, reason=None)
    if result.get("skipped"):
        return result

    # Preprint-specific: stamp the venue source on the collapsed row.
    conn.execute(
        "UPDATE papers SET preprint_source = COALESCE(NULLIF(preprint_source, ''), ?) "
        "WHERE id = ?",
        (preprint_source, preprint_id),
    )
    return {
        "skipped": False,
        "preprint_id": preprint_id,
        "canonical_id": canonical_id,
        "preprint_source": preprint_source,
        "fk_migrated": result.get("fk_migrated", {}),
    }


# -- lookup helper ------------------------------------------------------------


def resolve_canonical_paper_id(
    conn: sqlite3.Connection,
    paper_id: str,
) -> str:
    """Return the canonical paper_id, following the preprint pointer if set.

    Callers that receive a ``paper_id`` from URL params, feedback
    payloads, or recommendation rows should run their write-path lookups
    through this helper so a user's save on the preprint card resolves
    to the journal row transparently.
    """
    if not paper_id:
        return paper_id
    row = conn.execute(
        "SELECT canonical_paper_id FROM papers WHERE id = ?",
        (paper_id,),
    ).fetchone()
    if not row:
        return paper_id
    canonical = str(row["canonical_paper_id"] or "").strip()
    return canonical or paper_id


def count_preprint_twins(conn: sqlite3.Connection, scope: str = "library") -> int:
    """How many preprint↔journal twin pairs the dedup would collapse for ``scope``.
    Drives the Health card's pending count (local DB scan — no ETA)."""
    try:
        return len(find_preprint_twin_candidates(conn, scope=scope))
    except Exception:
        return 0


# -- batch runner -------------------------------------------------------------


def run_preprint_dedup(
    db_path: str,
    *,
    ctx: Any | None = None,
    limit: int | None = None,
    scope: str = "corpus",
) -> dict[str, Any]:
    """Activity-envelope runner: detect candidates + merge them per-pair.

    Per-pair commits so a collision halfway through doesn't roll back
    earlier merges. `ctx.log_step` gets per-pair progress for the
    Activity UI.
    """
    import sqlite3 as _sqlite3  # local to avoid mis-shadowing

    def _log(step: str, message: str, **kwargs: Any) -> None:
        if ctx is None:
            return
        try:
            ctx.log_step(step, message=message, **kwargs)
        except Exception:
            logger.debug("log_step failed on %s", step, exc_info=True)

    conn = _sqlite3.connect(db_path, check_same_thread=False, timeout=30.0)
    conn.row_factory = _sqlite3.Row
    # This runner merges/deletes rows; it must wait for the single SQLite
    # writer like every other lane instead of failing instantly on a lock.
    # Mirrors the contract in alma.api.deps.open_db_connection.
    conn.execute("PRAGMA busy_timeout=30000")
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    conn.execute("PRAGMA foreign_keys=ON")
    summary: dict[str, Any] = {"candidates": 0, "merged": 0, "skipped": 0, "errors": 0, "scope": scope}
    try:
        candidates = find_preprint_twin_candidates(conn, limit=limit, scope=scope)
        summary["candidates"] = len(candidates)
        _log(
            "detect",
            f"Found {len(candidates)} preprint↔journal twin candidates (scope={scope})",
            processed=0,
            total=len(candidates),
        )

        for idx, pair in enumerate(candidates, start=1):
            try:
                # Local merge (no network) — one gated write window per pair so
                # the sweep serializes against foreground writes and releases the
                # gate between merges (commit-per-unit-of-work).
                with write_section(conn, label="preprint dedup merge"):
                    result = merge_preprint_into_canonical(
                        conn,
                        pair["preprint_id"],
                        pair["canonical_id"],
                    )
                if result.get("skipped"):
                    summary["skipped"] += 1
                else:
                    summary["merged"] += 1
                _log(
                    "merge",
                    (
                        f"{idx}/{len(candidates)}: {pair['preprint_source']} → journal "
                        f"({pair['title'][:80]}, conf={pair['confidence']})"
                    ),
                    processed=idx,
                    total=len(candidates),
                )
            except Exception as exc:
                conn.rollback()
                summary["errors"] += 1
                logger.warning(
                    "preprint dedup failed for %s→%s: %s",
                    pair["preprint_id"],
                    pair["canonical_id"],
                    exc,
                )
                _log(
                    "error",
                    f"{idx}/{len(candidates)}: failed — {exc}",
                    processed=idx,
                    total=len(candidates),
                )
    finally:
        conn.close()
    return summary


# -- duplicate-identity collapse (legacy backlog) -----------------------------


def find_duplicate_identity_pairs(conn: sqlite3.Connection) -> list[dict[str, str]]:
    """Legacy same-``openalex_id`` duplicate pairs still awaiting collapse.

    Before the at-source merge landed (title_resolution / corpus_rehydrate),
    a title match to an OpenAlex work already owned by another paper row was
    stamped ``duplicate_identity:<owner>`` (terminal) and LEFT both rows
    canonical — the merge was deferred to the never-auto ``library_dedup``,
    so the pair accumulated and both cards showed in Feed / Discovery. This
    finds those still-split pairs (loser still canonical, keeper still
    present) so the retro-collapse can fold each loser into its ``<owner>``
    keeper. Returns ``[{"loser_id", "keeper_id"}]``.
    """
    try:
        rows = conn.execute(
            """
            SELECT DISTINCT es.paper_id AS loser_id, es.reason AS reason
            FROM paper_enrichment_status es
            JOIN papers p ON p.id = es.paper_id
            WHERE es.reason LIKE 'duplicate_identity:%'
              AND es.status = 'terminal_no_match'
              AND COALESCE(TRIM(p.canonical_paper_id), '') = ''
            """
        ).fetchall()
    except sqlite3.OperationalError:
        return []
    pairs: list[dict[str, str]] = []
    seen: set[str] = set()
    for r in rows:
        loser_id = str(r["loser_id"] or "").strip()
        keeper_id = str(r["reason"] or "").split("duplicate_identity:", 1)[-1].strip()
        if not loser_id or not keeper_id or loser_id == keeper_id or loser_id in seen:
            continue
        if conn.execute(
            "SELECT 1 FROM papers WHERE id = ?", (keeper_id,)
        ).fetchone() is None:
            continue
        seen.add(loser_id)
        pairs.append({"loser_id": loser_id, "keeper_id": keeper_id})
    return pairs


def run_duplicate_identity_collapse(
    db_path: str,
    *,
    ctx: Any | None = None,
    limit: int | None = None,
) -> dict[str, Any]:
    """Activity-envelope runner: collapse the LEGACY duplicate-identity backlog.

    Folds each still-split ``duplicate_identity:<owner>`` pair into its owner
    via the generic soft-merge (non-destructive — D3). Per-pair commit +
    idempotent, mirroring :func:`run_preprint_dedup`. The at-source merge in
    title resolution / corpus rehydrate prevents NEW splits, so this pool only
    shrinks and drains to zero.
    """
    import sqlite3 as _sqlite3  # local to avoid mis-shadowing

    def _log(step: str, message: str, **kwargs: Any) -> None:
        if ctx is None:
            return
        try:
            ctx.log_step(step, message=message, **kwargs)
        except Exception:
            logger.debug("log_step failed on %s", step, exc_info=True)

    conn = _sqlite3.connect(db_path, check_same_thread=False, timeout=30.0)
    conn.row_factory = _sqlite3.Row
    conn.execute("PRAGMA busy_timeout=30000")
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    conn.execute("PRAGMA foreign_keys=ON")
    summary: dict[str, Any] = {"candidates": 0, "merged": 0, "skipped": 0, "errors": 0}
    try:
        pairs = find_duplicate_identity_pairs(conn)
        if limit is not None:
            pairs = pairs[:limit]
        summary["candidates"] = len(pairs)
        _log(
            "detect",
            f"Found {len(pairs)} duplicate-identity pairs to collapse",
            processed=0,
            total=len(pairs),
        )
        for idx, pair in enumerate(pairs, start=1):
            try:
                with write_section(conn, label="duplicate-identity collapse"):
                    result = merge_duplicate_paper_rows(
                        conn,
                        loser_id=pair["loser_id"],
                        keeper_id=pair["keeper_id"],
                        reason=f"duplicate_identity:{pair['keeper_id']}",
                    )
                if result.get("skipped"):
                    summary["skipped"] += 1
                else:
                    summary["merged"] += 1
                _log(
                    "merge",
                    f"{idx}/{len(pairs)}: {pair['loser_id']} → {pair['keeper_id']}",
                    processed=idx,
                    total=len(pairs),
                )
            except Exception as exc:
                conn.rollback()
                summary["errors"] += 1
                logger.warning(
                    "duplicate-identity collapse failed for %s→%s: %s",
                    pair["loser_id"],
                    pair["keeper_id"],
                    exc,
                )
                _log(
                    "error",
                    f"{idx}/{len(pairs)}: failed — {exc}",
                    processed=idx,
                    total=len(pairs),
                )
    finally:
        conn.close()
    return summary


def count_duplicate_identity_pairs(conn: sqlite3.Connection) -> int:
    """How many legacy duplicate-identity pairs the collapse would fold.
    Drives the maintenance op's pending count (local scan — no ETA)."""
    try:
        return len(find_duplicate_identity_pairs(conn))
    except Exception:
        return 0
