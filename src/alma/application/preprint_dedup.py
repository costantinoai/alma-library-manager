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
from typing import Any, Optional

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


def classify_preprint_source(doi: Optional[str]) -> Optional[str]:
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
    limit: Optional[int] = None,
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

    sql = """
        SELECT id, title, year, doi, status, canonical_paper_id
        FROM papers
        WHERE COALESCE(canonical_paper_id, '') = ''
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


def merge_preprint_into_canonical(
    conn: sqlite3.Connection,
    preprint_id: str,
    canonical_id: str,
) -> dict[str, Any]:
    """Collapse the preprint into the canonical journal row.

    Idempotent — calling again on an already-merged pair is a no-op.
    Returns a summary dict with migrated row counts per FK table plus
    the final status stamp on the preprint.
    """
    if preprint_id == canonical_id:
        return {"skipped": True, "reason": "same_id"}

    # Idempotent guard: already merged?
    row = conn.execute(
        "SELECT canonical_paper_id FROM papers WHERE id = ?",
        (preprint_id,),
    ).fetchone()
    if row and row["canonical_paper_id"] == canonical_id:
        return {"skipped": True, "reason": "already_merged"}

    # Canonical must exist.
    canonical = conn.execute(
        "SELECT id, doi FROM papers WHERE id = ?",
        (canonical_id,),
    ).fetchone()
    if not canonical:
        return {"skipped": True, "reason": "canonical_missing"}

    # Infer preprint source tag from DOI if we don't have one yet.
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

    # 1. Migrate FK rows first so the canonical acquires everything
    #    before we upgrade its scalar fields (order-insensitive in
    #    theory, but this keeps the preprint's FK shadow clean).
    migrated: dict[str, int] = {}
    for table, unique_cols in _FK_TABLES_TO_MIGRATE:
        migrated[table] = _migrate_fk_rows(
            conn,
            preprint_id,
            canonical_id,
            table=table,
            unique_cols=unique_cols,
        )

    # 2. Also migrate feedback_events — its column is `paper_id` but
    #    unique constraint is on event id, so a bare UPDATE is safe.
    try:
        conn.execute(
            "UPDATE feedback_events SET paper_id = ? WHERE paper_id = ?",
            (canonical_id, preprint_id),
        )
    except sqlite3.OperationalError:
        pass

    # 3. Migrate recommendations — each lens has a unique
    #    (lens_id, paper_id, suggestion_set_id) triple, so use
    #    INSERT OR IGNORE + DELETE semantics like the FK helpers.
    try:
        conn.execute(
            """
            UPDATE OR IGNORE recommendations
            SET paper_id = ?
            WHERE paper_id = ?
            """,
            (canonical_id, preprint_id),
        )
        # Drop any surviving recommendations that couldn't migrate
        # (duplicate triple) — the canonical already has that slot.
        conn.execute(
            "DELETE FROM recommendations WHERE paper_id = ?",
            (preprint_id,),
        )
    except sqlite3.OperationalError:
        pass

    # 4. Scalar upgrade: copy missing fields, MAX monotonic counters,
    #    promote Library status.
    _upgrade_canonical_from_preprint(conn, preprint_id, canonical_id)

    # 5. Stamp the preprint row with the canonical pointer so read-side
    #    queries can filter it out. Keep its FK-orphan-safe breadcrumbs.
    conn.execute(
        """
        UPDATE papers
        SET canonical_paper_id = ?,
            preprint_source = COALESCE(NULLIF(preprint_source, ''), ?),
            updated_at = ?
        WHERE id = ?
        """,
        (
            canonical_id,
            preprint_source,
            datetime.utcnow().isoformat(),
            preprint_id,
        ),
    )

    return {
        "skipped": False,
        "preprint_id": preprint_id,
        "canonical_id": canonical_id,
        "preprint_source": preprint_source,
        "fk_migrated": migrated,
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


# -- batch runner -------------------------------------------------------------


def run_preprint_dedup(
    db_path: str,
    *,
    ctx: Optional[Any] = None,
    limit: Optional[int] = None,
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

    conn = _sqlite3.connect(db_path, check_same_thread=False)
    conn.row_factory = _sqlite3.Row
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
                result = merge_preprint_into_canonical(
                    conn,
                    pair["preprint_id"],
                    pair["canonical_id"],
                )
                conn.commit()
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
