"""Preprint↔journal dedup engine.

Single responsibility: collapse pairs where the same work exists as both
a preprint (arXiv / bioRxiv / psyRxiv / chemRxiv / OSF) row and a
published journal row into one canonical (journal) row. This is the only
paper-group collapse that absorbs metadata, feedback, and preferences from
the child row into the root.

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
- Library / Discovery reads filter subordinate rows via ``standalone_paper_sql``.
- Preprint sidecars, feedback, and preference profiles migrate from preprint →
  canonical via ``UPDATE OR IGNORE`` / merge patterns. Other duplicate/component
  child rows are pointer-only and donate no state.

Monotonic-signal fields (``cited_by_count``, ``rating``,
``reading_status``) take the stronger value. Author-supplied fields
(``notes``, ``added_from``) keep the Library-saved row's value when
only one side is in Library.
"""

from __future__ import annotations

import logging
import sqlite3
from typing import Any

from alma.core.db_write import write_section
from alma.core.paper_groups import (
    absorb_paper_group,
    classify_preprint_source,
    resolve_paper_root_id,
)
from alma.core.sql_helpers import standalone_paper_sql
from alma.core.utils import normalize_title_key

logger = logging.getLogger(__name__)


# Year proximity allowed between preprint and published version.
_YEAR_TOLERANCE = 2

# Default title-key threshold. We use an exact match on the normalised
# key by default — the key already collapses punctuation/whitespace, so
# any non-match almost certainly means genuinely different works.
_TITLE_EXACT = 1.0


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
        SELECT id, title, year, doi, status, canonical_paper_id,
               preprint_source, work_type, component_type
        FROM papers
        WHERE {standalone_paper_sql('papers')}
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
        source = classify_preprint_source(
            row["doi"],
            preprint_source=row["preprint_source"],
            work_type=row["work_type"],
        )
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
                    "preprint_source": classify_preprint_source(
                        preprint["doi"],
                        preprint_source=preprint["preprint_source"],
                        work_type=preprint["work_type"],
                    ),
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


def merge_duplicate_paper_rows(
    conn: sqlite3.Connection,
    loser_id: str,
    keeper_id: str,
    *,
    reason: str | None = None,
) -> dict[str, Any]:
    """Soft-merge a duplicate paper row (``loser_id``) into its keeper.

    The ONE generic relationship collapse shared by every duplicate-pair
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

    Only preprint→published-paper promotion absorbs metadata, sidecars,
    feedback, and preference profiles. Other losers are stamped as inert
    pointers and their behavioral sidecars are purged. ``reason`` (when given)
    is recorded in the loser's ``openalex_resolution_reason`` for the Corpus
    explorer without overwriting an existing reason.

    Returns a summary dict; ``{"skipped": True, "reason": ...}`` when the pair
    is invalid or already merged.
    """
    return absorb_paper_group(conn, loser_id, keeper_id, reason=reason)


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
        "SELECT id, doi, preprint_source, work_type FROM papers WHERE id = ?",
        (preprint_id,),
    ).fetchone()
    if not preprint_row:
        return {"skipped": True, "reason": "preprint_missing"}
    preprint_source = (
        str(preprint_row["preprint_source"] or "").strip()
        or classify_preprint_source(
            preprint_row["doi"], work_type=preprint_row["work_type"]
        )
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
        "canonical_id": result.get("root_id") or canonical_id,
        "preprint_source": preprint_source,
        "fk_migrated": result.get("fk_migrated", {}),
        "reparented": result.get("reparented", 0),
        "cleaned_sidecars": result.get("cleaned_sidecars", 0),
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
    return resolve_paper_root_id(conn, paper_id, strict=False)


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
