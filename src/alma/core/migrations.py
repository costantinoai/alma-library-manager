"""Versioned schema migrations — the ONLY place schema mutations live.

Forward-only contract (see CLAUDE.md → "Schema & config changes" and
``tasks/04_MIGRATION_CONSOLIDATION.md`` §A):

- General code assumes the CURRENT schema. No inline ``ALTER TABLE`` /
  ``ADD COLUMN`` / column-existence guards anywhere else.
- ``init_db_schema`` (``alma.api.deps``) owns the current-shape bootstrap
  DDL (``CREATE TABLE/INDEX IF NOT EXISTS``).
- This module owns the path FROM any older v0.x shape TO the current
  shape. It is the single greppable record of every schema change.

How it runs
-----------
``apply_pending_migrations(conn)`` is called once at startup from
``init_db_schema``, right after the connection pragmas and BEFORE the
bootstrap DDL asserts indexes — legacy identifier heals must run before
the partial UNIQUE indexes are (re-)created, otherwise
``CREATE UNIQUE INDEX`` raises ``IntegrityError`` on legacy duplicates.

The schema version is tracked via ``PRAGMA user_version`` (SQLite owns
the counter; no home-grown version table):

- A FRESH database (no ``papers`` table yet) skips every migration —
  the bootstrap DDL creates the current shape directly and
  ``stamp_schema_version`` pins ``user_version = SCHEMA_VERSION`` at the
  end of ``init_db_schema``.
- An EXISTING pre-versioning database sits at ``user_version = 0`` and
  runs every migration in order. Each migration is idempotent/guarded,
  because a v0.x DB may already carry any subset of these changes from
  the old scattered lazy ALTERs.
- Each migration commits individually and advances ``user_version`` as
  it lands; a failure aborts startup loudly and leaves ``user_version``
  at the last completed migration (re-running resumes from there).

Adding a migration
------------------
Append a ``(version, name, fn)`` entry to ``MIGRATIONS`` with the next
integer version, bump nothing else — ``SCHEMA_VERSION`` derives from the
list. Update the bootstrap DDL in ``init_db_schema`` in the same change
so fresh installs get the new shape directly.
"""

from __future__ import annotations

import logging
import sqlite3
from typing import Callable

logger = logging.getLogger("alma.core.migrations")


# ============================================================================
# Guarded helpers — ONLY for use inside migrations. General code must never
# need these: after startup the schema is guaranteed to be current.
# ============================================================================

def _table_exists(conn: sqlite3.Connection, table: str) -> bool:
    row = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name=?",
        (table,),
    ).fetchone()
    return row is not None


def _table_columns(conn: sqlite3.Connection, table: str) -> set[str]:
    try:
        rows = conn.execute(f"PRAGMA table_info({table})").fetchall()
    except sqlite3.OperationalError:
        return set()
    return {str(row[1]) for row in rows}


def _add_columns(
    conn: sqlite3.Connection,
    table: str,
    columns: dict[str, str],
) -> None:
    """Add each missing column. Skips silently when the table is absent
    (bootstrap DDL will create it in its current shape)."""
    if not _table_exists(conn, table):
        return
    existing = _table_columns(conn, table)
    for name, ddl in columns.items():
        if name in existing:
            continue
        conn.execute(f"ALTER TABLE {table} ADD COLUMN {name} {ddl}")


# ============================================================================
# Migrations — ported verbatim from the pre-consolidation lazy call sites
# (api/deps.py init_db_schema, api/routes/authors.py, application/*,
# library/*). Original rationale comments preserved where they matter.
# ============================================================================

def _m_0001_papers_columns(conn: sqlite3.Connection) -> None:
    """Bring ``papers`` to the current column set (pre-v1 lazy ALTERs)."""
    _add_columns(
        conn,
        "papers",
        {
            "semantic_scholar_id": "TEXT",
            "semantic_scholar_corpus_id": "TEXT",
            # Preprint↔journal dedup (2026-04-24): when the same work
            # exists as both a preprint and a published journal row, the
            # preprint points to the journal row here and gets filtered
            # out of Library + Discovery lists. NULL = canonical.
            "canonical_paper_id": "TEXT",
            "preprint_source": "TEXT",  # arxiv / biorxiv / psyrxiv / osf / chemrxiv
            "reading_status": "TEXT DEFAULT NULL",
            # S2 tldr: 1-2 sentence AI summary; dense in CS + biomed.
            "tldr": "TEXT",
            # S2's learned "this citation mattered" count.
            "influential_citation_count": "INTEGER DEFAULT 0",
        },
    )


def _m_0002_papers_status_relabels(conn: sqlite3.Connection) -> None:
    """One-shot lifecycle relabels (D2): candidate→tracked, legacy import
    promotion, disliked→tracked, queued→reading."""
    if not _table_exists(conn, "papers"):
        return
    conn.execute("UPDATE papers SET status = 'tracked' WHERE status = 'candidate'")
    conn.execute(
        """
        UPDATE papers
        SET status = 'library',
            added_from = COALESCE(NULLIF(TRIM(added_from), ''), 'import'),
            added_at = COALESCE(added_at, fetched_at, datetime('now'))
        WHERE status = 'tracked'
          AND (
            added_from = 'import'
            OR notes LIKE 'Imported from %'
          )
        """
    )
    conn.execute("UPDATE papers SET status = 'tracked' WHERE status = 'disliked'")
    # D2 (2026-04-26): collapse the `queued` reading-state into `reading`.
    conn.execute(
        "UPDATE papers SET reading_status = 'reading' WHERE reading_status = 'queued'"
    )


def _m_0003_papers_identifier_heals(conn: sqlite3.Connection) -> None:
    """Identifier heals that MUST precede the partial UNIQUE indexes.

    (1) Fold URL-form ``papers.openalex_id`` to bare form — three-step
    twin-safe normalization (null the URL-form value when a bare twin
    exists; dedupe URL-form rows sharing a bare id; normalize the rest).
    (2) Coerce blank ('' / whitespace) identifiers to NULL so the partial
    UNIQUE indexes can't collide on the empty string.
    """
    if not _table_exists(conn, "papers"):
        return
    cols = _table_columns(conn, "papers")
    nulled_twin = conn.execute(
        """
        UPDATE papers SET openalex_id = NULL
        WHERE openalex_id LIKE 'https://openalex.org/%'
          AND EXISTS (
            SELECT 1 FROM papers p2
            WHERE p2.id != papers.id
              AND p2.openalex_id = SUBSTR(papers.openalex_id, 22)
          )
        """
    ).rowcount
    nulled_intra = conn.execute(
        """
        UPDATE papers SET openalex_id = NULL
        WHERE id IN (
          SELECT id FROM papers WHERE openalex_id LIKE 'https://openalex.org/%'
          EXCEPT
          SELECT MIN(id) FROM papers
           WHERE openalex_id LIKE 'https://openalex.org/%'
           GROUP BY SUBSTR(openalex_id, 22)
        )
        """
    ).rowcount
    normalized = conn.execute(
        "UPDATE papers "
        "SET openalex_id = SUBSTR(openalex_id, 22) "
        "WHERE openalex_id LIKE 'https://openalex.org/%'"
    ).rowcount
    total = nulled_twin + nulled_intra + normalized
    if total:
        logger.info(
            "papers.openalex_id URL heal: nulled_twin=%d, nulled_intra=%d, normalized=%d",
            nulled_twin,
            nulled_intra,
            normalized,
        )
    for col in ("openalex_id", "doi", "semantic_scholar_id"):
        if col in cols:
            conn.execute(
                f"UPDATE papers SET {col} = NULL "
                f"WHERE {col} IS NOT NULL AND TRIM({col}) = ''"
            )


def _m_0004_authors_columns(conn: sqlite3.Connection) -> None:
    """Bring ``authors`` to the current column set.

    Folds together the four pre-consolidation lazy sources: the deps.py
    ensure block, routes/authors.py ``_ensure_author_resolution_columns``
    (which alone added ``status``), author_identity.py (Phase D resolver
    columns), and library/deduplication.py (``author_uid``).
    """
    _add_columns(
        conn,
        "authors",
        {
            "openalex_id": "TEXT",
            "orcid": "TEXT",
            "scholar_id": "TEXT",
            "affiliation": "TEXT",
            "citedby": "INTEGER DEFAULT 0",
            "h_index": "INTEGER DEFAULT 0",
            "interests": "TEXT",
            "url_picture": "TEXT",
            "works_count": "INTEGER DEFAULT 0",
            "last_fetched_at": "TEXT",
            "cited_by_year": "TEXT",
            "institutions": "TEXT",
            "email_domain": "TEXT",
            "added_at": "TEXT",
            "author_type": "TEXT DEFAULT 'followed'",
            "id_resolution_status": "TEXT",
            "id_resolution_reason": "TEXT",
            "id_resolution_updated_at": "TEXT",
            # Phase D hierarchical-resolver columns (2026-04-24).
            "id_resolution_method": "TEXT",
            "id_resolution_confidence": "REAL",
            "id_resolution_evidence": "TEXT",
            "semantic_scholar_id": "TEXT",
            # Soft-removal lifecycle (2026-04-26) — mirrors papers.status
            # (D3): 'removed' rows stay readable as a negative signal but
            # are filtered out of bulk refresh and the canonical list.
            "status": "TEXT DEFAULT 'active'",
            # Stable dedup identity (library/deduplication.ensure_stable_ids
            # recomputes the VALUES; the column itself lives here).
            "author_uid": "TEXT",
        },
    )
    if _table_exists(conn, "authors"):
        conn.execute("UPDATE authors SET status = 'active' WHERE status IS NULL")


def _m_0005_authors_openalex_id_normalization(conn: sqlite3.Connection) -> None:
    """Normalize ``authors.openalex_id`` to the canonical ``A…`` bare form.

    Three legacy drifts seen in real DBs as of 2026-04-24: (a) lowercase
    ``a…`` prefix, (b) ``3A…`` residue from a buggy ``%3A`` URL-decode
    pass, (c) URL-form ``https://openalex.org/A…``. Any of these makes
    ``fetch_author_profile`` 404 silently on every deep refresh.

    The partial UNIQUE index ``ux_authors_openalex_norm`` on
    ``lower(openalex_id)`` means a naive bulk UPDATE can collide with a
    twin row holding the canonical form — in that case the corrupt value
    is nulled so the row stays eligible for re-resolution.
    """
    if not _table_exists(conn, "authors"):
        return
    if "openalex_id" not in _table_columns(conn, "authors"):
        return
    heals = {"url_form": 0, "triple_a": 0, "lower_a": 0, "collisions_nulled": 0}
    heals["url_form"] = conn.execute(
        "UPDATE authors SET openalex_id = SUBSTR(openalex_id, 22) "
        "WHERE openalex_id LIKE 'https://openalex.org/%' "
        "AND NOT EXISTS ("
        "  SELECT 1 FROM authors twin "
        "  WHERE lower(twin.openalex_id) = lower(SUBSTR(authors.openalex_id, 22)) "
        "  AND twin.id != authors.id"
        ")"
    ).rowcount
    heals["triple_a"] = conn.execute(
        "UPDATE authors SET openalex_id = SUBSTR(openalex_id, 3) "
        "WHERE openalex_id GLOB '3[Aa]*' "
        "AND NOT EXISTS ("
        "  SELECT 1 FROM authors twin "
        "  WHERE lower(twin.openalex_id) = lower(SUBSTR(authors.openalex_id, 3)) "
        "  AND twin.id != authors.id"
        ")"
    ).rowcount
    heals["collisions_nulled"] = conn.execute(
        "UPDATE authors SET openalex_id = NULL WHERE openalex_id GLOB '3[Aa]*'"
    ).rowcount
    heals["lower_a"] = conn.execute(
        "UPDATE authors SET openalex_id = 'A' || SUBSTR(openalex_id, 2) "
        "WHERE openalex_id LIKE 'a%' "
        "AND NOT EXISTS ("
        "  SELECT 1 FROM authors twin "
        "  WHERE lower(twin.openalex_id) = lower('A' || SUBSTR(authors.openalex_id, 2)) "
        "  AND twin.id != authors.id"
        ")"
    ).rowcount
    total = sum(heals.values())
    if total:
        logger.info("authors.openalex_id heal: %s (total %d rows touched)", heals, total)


def _m_0006_followed_authors_is_owner(conn: sqlite3.Connection) -> None:
    """Onboarding owner flag — at most one owner row (partial UNIQUE
    index asserted by the bootstrap DDL)."""
    _add_columns(
        conn,
        "followed_authors",
        {"is_owner": "INTEGER NOT NULL DEFAULT 0"},
    )


def _m_0007_enrichment_status_columns(conn: sqlite3.Connection) -> None:
    """Ledger-table column adds (paper + author enrichment status)."""
    shared = {
        "lookup_key": "TEXT NOT NULL DEFAULT ''",
        "fields_key": "TEXT NOT NULL DEFAULT ''",
        "reason": "TEXT",
        "fields_requested_json": "TEXT",
        "fields_filled_json": "TEXT",
        "attempts": "INTEGER NOT NULL DEFAULT 0",
        "last_attempt_at": "TEXT",
        "next_retry_at": "TEXT",
        "updated_at": "TEXT NOT NULL DEFAULT (datetime('now'))",
    }
    _add_columns(conn, "author_enrichment_status", shared)
    _add_columns(conn, "paper_enrichment_status", shared)


def _m_0008_affiliation_evidence_columns(conn: sqlite3.Connection) -> None:
    _add_columns(
        conn,
        "author_affiliation_evidence",
        {
            "institution_openalex_id": "TEXT",
            "institution_ror": "TEXT",
            "role": "TEXT",
            "start_date": "TEXT NOT NULL DEFAULT ''",
            "end_date": "TEXT",
            "is_current": "INTEGER DEFAULT 0",
            "evidence_url": "TEXT",
            "confidence": "REAL",
            "observed_at": "TEXT NOT NULL DEFAULT (datetime('now'))",
        },
    )


def _m_0009_feed_items(conn: sqlite3.Connection) -> None:
    """feed_items: score_breakdown + monitor provenance columns, plus the
    one-shot relabel of retired statuses back to the chronological inbox."""
    _add_columns(
        conn,
        "feed_items",
        {
            "score_breakdown": "TEXT DEFAULT NULL",
            "monitor_id": "TEXT",
            "monitor_type": "TEXT",
            "monitor_label": "TEXT",
        },
    )
    if _table_exists(conn, "feed_items"):
        conn.execute(
            """
            UPDATE feed_items
            SET status = 'new'
            WHERE status IN ('deferred', 'discovery', 'signal_lab')
            """
        )


def _m_0010_lenses_branch_controls(conn: sqlite3.Connection) -> None:
    _add_columns(conn, "discovery_lenses", {"branch_controls": "TEXT"})


def _m_0011_recommendations_columns(conn: sqlite3.Connection) -> None:
    """recommendations: explanation + the six provenance columns (D-10 —
    this migration is the single source of truth for their existence;
    keep in sync with ``_derive_recommendation_provenance``), and drop
    the legacy ``query_plan_used_ai`` column (LLM planner removed
    2026-04; needs SQLite >= 3.35 for DROP COLUMN, hence the guard)."""
    _add_columns(
        conn,
        "recommendations",
        {
            "explanation": "TEXT",
            "source_type": "TEXT",
            "source_api": "TEXT",
            "source_key": "TEXT",
            "branch_id": "TEXT",
            "branch_label": "TEXT",
            "branch_mode": "TEXT",
        },
    )
    try:
        if "query_plan_used_ai" in _table_columns(conn, "recommendations"):
            conn.execute("ALTER TABLE recommendations DROP COLUMN query_plan_used_ai")
    except sqlite3.OperationalError:
        logger.warning(
            "Could not drop recommendations.query_plan_used_ai (SQLite < 3.35?); "
            "harmless leftover column",
        )


def _m_0012_recommendations_hygiene(conn: sqlite3.Connection) -> None:
    """D-AUDIT-6: delete FK-orphan recommendation rows and dedupe
    (lens_id, paper_id, suggestion_set_id) duplicates. The UNIQUE index
    that blocks the duplicate path forward is asserted by the bootstrap
    DDL right after migrations run."""
    if not _table_exists(conn, "recommendations") or not _table_exists(conn, "papers"):
        return
    orphans = conn.execute(
        "DELETE FROM recommendations WHERE paper_id NOT IN (SELECT id FROM papers)"
    ).rowcount
    if orphans:
        logger.info("recommendations heal: deleted %d orphan rows", orphans)
    dupes = conn.execute(
        """
        DELETE FROM recommendations
        WHERE id NOT IN (
            SELECT id FROM (
                SELECT id,
                       ROW_NUMBER() OVER (
                           PARTITION BY COALESCE(lens_id, ''),
                                        paper_id,
                                        COALESCE(suggestion_set_id, '')
                           ORDER BY COALESCE(action_at, created_at, '') DESC,
                                    created_at DESC,
                                    id
                       ) AS rn
                FROM recommendations
            )
            WHERE rn = 1
        )
        """
    ).rowcount
    if dupes:
        logger.info("recommendations heal: removed %d duplicate rows", dupes)


def _m_0013_publication_topics_columns(conn: sqlite3.Connection) -> None:
    _add_columns(
        conn,
        "publication_topics",
        {
            "domain": "TEXT DEFAULT ''",
            "field": "TEXT DEFAULT ''",
            "subfield": "TEXT DEFAULT ''",
            "topic_id": "TEXT",
        },
    )


def _m_0014_publication_embeddings_pk(conn: sqlite3.Connection) -> None:
    """publication_embeddings: add ``source`` and rebuild the legacy
    PRIMARY KEY (paper_id) → (paper_id, model) so multiple embedding
    models can coexist per paper. Preserves each legacy row under the
    model name already recorded against it."""
    if not _table_exists(conn, "publication_embeddings"):
        return
    from alma.ai.embedding_sources import EMBEDDING_SOURCE_UNKNOWN

    _add_columns(
        conn,
        "publication_embeddings",
        {"source": "TEXT NOT NULL DEFAULT 'unknown'"},
    )
    pk_cols = [
        row[1]
        for row in conn.execute("PRAGMA table_info(publication_embeddings)")
        if row[5] > 0  # pk ordinal
    ]
    if pk_cols != ["paper_id"]:
        return
    logger.info("Migrating publication_embeddings PK from (paper_id) to (paper_id, model)")
    conn.execute(
        "ALTER TABLE publication_embeddings RENAME TO publication_embeddings_legacy"
    )
    conn.execute(
        """CREATE TABLE publication_embeddings (
            paper_id TEXT NOT NULL REFERENCES papers(id) ON DELETE CASCADE,
            embedding BLOB NOT NULL,
            model TEXT NOT NULL,
            source TEXT NOT NULL DEFAULT 'unknown',
            created_at TEXT NOT NULL,
            PRIMARY KEY (paper_id, model)
        )"""
    )
    conn.execute(
        """
        INSERT INTO publication_embeddings (paper_id, embedding, model, source, created_at)
        SELECT paper_id,
               embedding,
               NULLIF(TRIM(model), ''),
               ?,
               created_at
        FROM publication_embeddings_legacy
        WHERE NULLIF(TRIM(model), '') IS NOT NULL
        """,
        (EMBEDDING_SOURCE_UNKNOWN,),
    )
    conn.execute("DROP TABLE publication_embeddings_legacy")


def _m_0015_vector_blobs_float16(conn: sqlite3.Connection) -> None:
    """Re-encode legacy float32 vector blobs as float16 (canonical
    ``core.vector_blob.encode_vector`` form). A float32 row decodes as a
    phantom double-length float16 vector and crashes np.dot/np.stack."""
    from alma.core.vector_blob import migrate_blob_column_to_float16

    if _table_exists(conn, "publication_embeddings"):
        try:
            migrate_blob_column_to_float16(
                conn,
                table="publication_embeddings",
                blob_col="embedding",
                key_cols=("paper_id",),
                model_col="model",
            )
        except Exception:
            logger.warning("publication_embeddings dtype migration skipped", exc_info=True)
    if _table_exists(conn, "author_centroids"):
        try:
            migrate_blob_column_to_float16(
                conn,
                table="author_centroids",
                blob_col="centroid_blob",
                key_cols=("author_openalex_id", "model"),
                model_col="model",
            )
        except Exception:
            logger.warning("author_centroids dtype migration skipped", exc_info=True)


def _m_0016_feedback_events_mode_heal(conn: sqlite3.Connection) -> None:
    """Signal Lab consolidation (2026-04-24): stamp missing
    ``context.mode`` from event_type and rename retired mode values onto
    the consolidated surface (swipe / authors / topics / tier_sort /
    feed / library)."""
    if not _table_exists(conn, "feedback_events"):
        return
    heals = 0
    missing_mode_map = {
        "swipe": "swipe",
        "triage_pick": "swipe",
        "author_pref": "authors",
        "topic_pref": "topics",
        "source_pref": "tier_sort",
        "method_match": "swipe",
        "abstract_highlight": "swipe",
        "feed_action": "feed",
        "rating": "library",
        "paper_action": "library",
        "tier_sort": "tier_sort",
    }
    for etype, mode in missing_mode_map.items():
        heals += conn.execute(
            """
            UPDATE feedback_events
            SET context_json = json_set(
                COALESCE(context_json, '{}'),
                '$.mode', ?
            )
            WHERE event_type = ?
              AND (
                    context_json IS NULL
                 OR COALESCE(json_extract(context_json, '$.mode'), '') = ''
              )
            """,
            (mode, etype),
        ).rowcount
    legacy_mode_rename = {
        "triage": "swipe",
        "author_duel": "authors",
        "method_match": "swipe",
        "source_sprint": "tier_sort",
        "abstract_highlight": "swipe",
    }
    for old, new in legacy_mode_rename.items():
        heals += conn.execute(
            """
            UPDATE feedback_events
            SET context_json = json_set(context_json, '$.mode', ?)
            WHERE json_extract(context_json, '$.mode') = ?
            """,
            (new, old),
        ).rowcount
    if heals:
        logger.info("feedback_events.mode heal: consolidated %d rows", heals)


def _m_0017_settings_legacy_cleanup(conn: sqlite3.Connection) -> None:
    """Drop retired LLM/Ollama settings keys and normalize removed
    embedding-provider options onto the canonical local/SPECTER2 path
    (LLM production exit, 2026-04)."""
    if not _table_exists(conn, "discovery_settings"):
        return
    from alma.discovery.semantic_scholar import S2_SPECTER2_MODEL

    conn.execute(
        "UPDATE discovery_settings SET value = 'local' "
        "WHERE key = 'ai.provider' AND value = 'minilm'"
    )
    conn.execute(
        """
        UPDATE discovery_settings
        SET value = 'specter2-base'
        WHERE key = 'ai.local_model'
          AND value IN ('minilm-l6', 'bge-base', 'bge-large')
        """
    )
    conn.execute(
        """
        UPDATE discovery_settings
        SET value = ?
        WHERE key = 'embedding_model'
          AND value IN (
            'all-MiniLM-L6-v2',
            'BAAI/bge-base-en-v1.5',
            'BAAI/bge-large-en-v1.5'
          )
        """,
        (S2_SPECTER2_MODEL,),
    )
    for legacy_key in (
        "ai.llm_provider",
        "ai.llm_model",
        "ai.hf_llm_model",
        "ai.openai_llm_model",
        "ai.anthropic_model",
        "ai.auto_compute",
        "ai.ollama_url",
        "ai.ollama_model",
        "strategies.ai_query_planner",
    ):
        conn.execute("DELETE FROM discovery_settings WHERE key = ?", (legacy_key,))
    conn.execute(
        """
        UPDATE discovery_settings
        SET value = 'none'
        WHERE key = 'ai.provider'
          AND value NOT IN ('none', 'local', 'openai')
        """
    )


def _m_0018_gap_feedback_suggestion_bucket(conn: sqlite3.Connection) -> None:
    """Bucket attribution for outcome calibration (Phase 4 #3). The
    ``missing_author_feedback`` table is created lazily by
    ``application.gap_radar`` (its CREATE now includes the column);
    this covers pre-existing tables only."""
    _add_columns(conn, "missing_author_feedback", {"suggestion_bucket": "TEXT"})


def _m_0019_follow_state_heal(conn: sqlite3.Connection) -> None:
    """One-shot heal for drift between authors.author_type='followed' and
    the ``followed_authors`` table (canonical source of truth). Legacy
    import pipelines stamped 'followed' without inserting the follow row,
    leaving phantom-followed authors with no Feed monitor. Demote them,
    then mirror feed_monitors one last time."""
    if not _table_exists(conn, "authors") or not _table_exists(conn, "followed_authors"):
        return
    demoted = conn.execute(
        """
        UPDATE authors
        SET author_type = 'background'
        WHERE author_type = 'followed'
          AND id NOT IN (SELECT author_id FROM followed_authors)
        """
    ).rowcount
    if demoted:
        logger.info(
            "follow-state heal: demoted %d phantom-followed authors", demoted
        )
    try:
        from alma.application.feed_monitors import sync_author_monitors

        sync_author_monitors(conn)
    except Exception:
        logger.warning("follow-state heal: monitor mirror sync skipped", exc_info=True)


def _m_0020_topic_aliases_legacy_shape(conn: sqlite3.Connection) -> None:
    """Rebuild a legacy two-column ``topic_aliases`` (alias_term,
    canonical_term) into the current topic_id-keyed shape, preserving the
    alias mappings. Ported from the old lazy
    ``topic_deduplication._migrate_legacy_aliases``."""
    if not _table_exists(conn, "topic_aliases"):
        return
    cols = _table_columns(conn, "topic_aliases")
    if "alias_term" not in cols or "topic_id" in cols:
        return
    from datetime import datetime

    from alma.library.topic_deduplication import (
        _topic_id_from_normalized,
        normalize_topic,
    )

    old_rows = conn.execute(
        "SELECT alias_term, canonical_term FROM topic_aliases"
    ).fetchall()
    conn.execute("DROP TABLE topic_aliases")
    conn.execute(
        """CREATE TABLE topic_aliases (
            alias_id INTEGER PRIMARY KEY AUTOINCREMENT,
            topic_id TEXT NOT NULL REFERENCES topics(topic_id),
            raw_term TEXT NOT NULL,
            normalized_term TEXT NOT NULL,
            source TEXT DEFAULT 'auto',
            confidence REAL DEFAULT 1.0,
            created_at TEXT DEFAULT (datetime('now')),
            UNIQUE(normalized_term)
        )"""
    )
    migrated = 0
    for row in old_rows:
        raw = (row[0] or "").strip()
        canonical = (row[1] or "").strip()
        if not raw or not canonical:
            continue
        normalized = normalize_topic(raw)
        canonical_normalized = normalize_topic(canonical)
        topic_id = _topic_id_from_normalized(canonical_normalized)
        conn.execute(
            """INSERT OR IGNORE INTO topics
               (topic_id, canonical_name, normalized_name, source, created_at)
               VALUES (?, ?, ?, 'auto', ?)""",
            (topic_id, canonical, canonical_normalized, datetime.utcnow().isoformat()),
        )
        conn.execute(
            """INSERT OR IGNORE INTO topic_aliases
               (topic_id, raw_term, normalized_term, source, confidence, created_at)
               VALUES (?, ?, ?, 'auto', 1.0, ?)""",
            (topic_id, raw, normalized, datetime.utcnow().isoformat()),
        )
        migrated += 1
    if migrated:
        logger.info("topic_aliases legacy-shape rebuild: migrated %d aliases", migrated)


def _m_0021_publication_clusters_scope(conn: sqlite3.Connection) -> None:
    """Key the cluster-layout cache by graph SCOPE (I-1).

    PRIMARY KEY (paper_id) → (paper_id, scope) so a Library layout and a
    Corpus layout coexist per paper. Before this, a Corpus rebuild
    (corpus ⊇ library) overwrote Library cluster assignments, and a Library
    GET then served corpus-space positions/clusters. Existing rows ARE the
    Library layout, so they migrate under scope='library'.
    """
    if not _table_exists(conn, "publication_clusters"):
        return
    if "scope" in _table_columns(conn, "publication_clusters"):
        return  # already current shape (fresh DB or re-run)
    logger.info(
        "Migrating publication_clusters PK from (paper_id) to (paper_id, scope)"
    )
    conn.execute(
        "ALTER TABLE publication_clusters RENAME TO publication_clusters_legacy"
    )
    conn.execute(
        """CREATE TABLE publication_clusters (
            paper_id TEXT NOT NULL REFERENCES papers(id) ON DELETE CASCADE,
            scope TEXT NOT NULL DEFAULT 'library',
            cluster_id INTEGER NOT NULL,
            label TEXT DEFAULT '',
            x REAL DEFAULT 0.5,
            y REAL DEFAULT 0.5,
            updated_at TEXT DEFAULT (datetime('now')),
            PRIMARY KEY (paper_id, scope)
        )"""
    )
    conn.execute(
        """
        INSERT INTO publication_clusters
            (paper_id, scope, cluster_id, label, x, y, updated_at)
        SELECT paper_id, 'library', cluster_id, label, x, y, updated_at
        FROM publication_clusters_legacy
        """
    )
    conn.execute("DROP TABLE publication_clusters_legacy")


def _m_0022_papers_component_columns(conn: sqlite3.Connection) -> None:
    """Paper components (parent/child, 2026-06-27): a figure / supporting-info
    file / author-response / dataset is a *part of* a paper, not a paper to
    read. ``component_type`` (figure | supplementary | peer_review | dataset)
    marks it; ``parent_paper_id`` links it to its parent row. Distinct from
    ``canonical_paper_id`` (dedup — "same work twice"); this is part-of. Both
    NULL for a normal paper. See alma.core.components."""
    _add_columns(
        conn,
        "papers",
        {
            "parent_paper_id": "TEXT",
            "component_type": "TEXT",
        },
    )


def _m_0023_influential_citation_count_heal(conn: sqlite3.Connection) -> None:
    """Heal legacy NULL ``influential_citation_count`` to its DDL default (0).

    The column is declared ``INTEGER DEFAULT 0`` and ``PaperResponse`` requires a
    non-null int, but a handful of legacy rows (papers never touched by the S2
    enrichment path) hold NULL — which 500s ``GET /papers/{id}/details`` (the
    popup). Forward-only: bring old data to the current contract once."""
    if not _table_exists(conn, "papers"):
        return
    conn.execute(
        "UPDATE papers SET influential_citation_count = 0 "
        "WHERE influential_citation_count IS NULL"
    )


def _m_0024_authors_orcid_swept_at(conn: sqlite3.Connection) -> None:
    """ORCID-dedup sweep freshness marker (2026-06-28).

    The Health "Dedup authors by ORCID" card used to count *every* followed
    author with an OpenAlex id — the set the sweep WALKS — so its pending count
    equalled the whole followed list and never dropped after a run (running the
    sweep doesn't unfollow anyone). ``orcid_swept_at`` records when the sweep
    last scanned each author, so the card can count only authors NOT yet scanned
    (or stale) and fall to zero once everyone's been checked. NULL = never
    swept = pending — the correct default for both legacy and fresh rows, so no
    data heal is needed."""
    _add_columns(conn, "authors", {"orcid_swept_at": "TEXT"})


def _m_0025_author_merge_candidates(conn: sqlite3.Connection) -> None:
    """ORCID-dedup review queue (2026-06-28).

    The ORCID sweep no longer auto-merges. It SCANS (network discovery) and
    RECORDS each mergeable split-profile pair here as a pending candidate; the
    Health "Merge ORCID duplicates" card counts these (the truthful "N to merge"
    number — never the author total) and lists them for review before the user
    applies the destructive merge. A row is consumed (deleted) once its merge is
    applied. UNIQUE(primary,alt) keeps a re-scan idempotent.
    """
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS author_merge_candidates (
            id TEXT PRIMARY KEY,
            primary_author_id TEXT NOT NULL,
            alt_author_id TEXT NOT NULL,
            alt_openalex_id TEXT,
            shared_orcid TEXT,
            papers_estimate INTEGER DEFAULT 0,
            discovered_at TEXT,
            UNIQUE (primary_author_id, alt_author_id)
        )
        """
    )


def _m_0026_merge_candidate_source_and_rejections(conn: sqlite3.Connection) -> None:
    """Name-match dedup + a permanent reject system (2026-06-28).

    Duplicate detection now has TWO sources — authoritative ORCID and a
    name/initials heuristic ("E. van Hove" ≈ "Emily van Hove") — so each
    candidate carries its `source` + `confidence` for the review badge. And the
    user can REJECT a wrong suggestion: the unordered author pair goes into
    `author_merge_rejections` (canonical lo/hi ordering, permanent — no decay) and
    every detector skips a rejected pair forever, so it is never resurfaced."""
    _add_columns(
        conn,
        "author_merge_candidates",
        {
            # 'orcid' (shared ORCID, authoritative) | 'name' (name/initials match)
            "source": "TEXT DEFAULT 'orcid'",
            # 'high' | 'medium' | 'low' — only meaningful for source='name'
            "confidence": "TEXT",
        },
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS author_merge_rejections (
            id TEXT PRIMARY KEY,
            author_id_lo TEXT NOT NULL,
            author_id_hi TEXT NOT NULL,
            source TEXT,
            reason TEXT,
            rejected_at TEXT,
            UNIQUE (author_id_lo, author_id_hi)
        )
        """
    )


def _m_0027_suggestion_not_duplicate(conn: sqlite3.Connection) -> None:
    """"Not a duplicate" verdicts for the suggestion rail (2026-06-28).

    When a suggested author is flagged as a possible name-duplicate of someone you
    follow but the user says "no, different person", we record the suggested
    OpenAlex id here. `_annotate_duplicate_suggestions` then stops flagging it, so
    it returns to the normal "follow a new author" rail instead of being
    suppressed entirely."""
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS author_suggestion_not_duplicate (
            openalex_id TEXT PRIMARY KEY,
            created_at TEXT
        )
        """
    )


MIGRATIONS: list[tuple[int, str, Callable[[sqlite3.Connection], None]]] = [
    (1, "papers_columns", _m_0001_papers_columns),
    (2, "papers_status_relabels", _m_0002_papers_status_relabels),
    (3, "papers_identifier_heals", _m_0003_papers_identifier_heals),
    (4, "authors_columns", _m_0004_authors_columns),
    (5, "authors_openalex_id_normalization", _m_0005_authors_openalex_id_normalization),
    (6, "followed_authors_is_owner", _m_0006_followed_authors_is_owner),
    (7, "enrichment_status_columns", _m_0007_enrichment_status_columns),
    (8, "affiliation_evidence_columns", _m_0008_affiliation_evidence_columns),
    (9, "feed_items", _m_0009_feed_items),
    (10, "lenses_branch_controls", _m_0010_lenses_branch_controls),
    (11, "recommendations_columns", _m_0011_recommendations_columns),
    (12, "recommendations_hygiene", _m_0012_recommendations_hygiene),
    (13, "publication_topics_columns", _m_0013_publication_topics_columns),
    (14, "publication_embeddings_pk", _m_0014_publication_embeddings_pk),
    (15, "vector_blobs_float16", _m_0015_vector_blobs_float16),
    (16, "feedback_events_mode_heal", _m_0016_feedback_events_mode_heal),
    (17, "settings_legacy_cleanup", _m_0017_settings_legacy_cleanup),
    (18, "gap_feedback_suggestion_bucket", _m_0018_gap_feedback_suggestion_bucket),
    (19, "follow_state_heal", _m_0019_follow_state_heal),
    (20, "topic_aliases_legacy_shape", _m_0020_topic_aliases_legacy_shape),
    (21, "publication_clusters_scope", _m_0021_publication_clusters_scope),
    (22, "papers_component_columns", _m_0022_papers_component_columns),
    (23, "influential_citation_count_heal", _m_0023_influential_citation_count_heal),
    (24, "authors_orcid_swept_at", _m_0024_authors_orcid_swept_at),
    (25, "author_merge_candidates", _m_0025_author_merge_candidates),
    (26, "merge_candidate_source_and_rejections", _m_0026_merge_candidate_source_and_rejections),
    (27, "suggestion_not_duplicate", _m_0027_suggestion_not_duplicate),
]

#: The schema version a fully-migrated (or freshly-bootstrapped) DB carries.
SCHEMA_VERSION: int = MIGRATIONS[-1][0]


def _get_user_version(conn: sqlite3.Connection) -> int:
    return int(conn.execute("PRAGMA user_version").fetchone()[0])


def stamp_schema_version(conn: sqlite3.Connection) -> None:
    """Pin ``user_version`` to ``SCHEMA_VERSION`` without running anything.

    Called at the end of ``init_db_schema`` so a FRESH database (whose
    bootstrap DDL just created the current shape) never replays the
    legacy migrations. No-op on an already-stamped DB.
    """
    if _get_user_version(conn) < SCHEMA_VERSION:
        conn.execute(f"PRAGMA user_version = {SCHEMA_VERSION}")


def apply_pending_migrations(conn: sqlite3.Connection) -> None:
    """Run every migration above the DB's current ``user_version``.

    Call once at startup, before the bootstrap DDL asserts indexes.
    Fresh databases (no ``papers`` table) are skipped entirely — the
    bootstrap DDL creates the current shape and ``stamp_schema_version``
    pins the version afterwards.

    Each migration commits individually and advances ``user_version``;
    a failure raises (startup aborts loudly) and leaves the version at
    the last completed migration.
    """
    version = _get_user_version(conn)
    if version >= SCHEMA_VERSION:
        if version > SCHEMA_VERSION:
            logger.warning(
                "DB user_version %d is ahead of this build's SCHEMA_VERSION %d "
                "(database written by a newer ALMa?)",
                version,
                SCHEMA_VERSION,
            )
        return
    if not _table_exists(conn, "papers"):
        # Fresh database: nothing to migrate FROM. Bootstrap DDL owns it.
        return
    for mig_version, name, fn in MIGRATIONS:
        if mig_version <= version:
            continue
        logger.info("Applying schema migration %04d_%s", mig_version, name)
        try:
            fn(conn)
            conn.execute(f"PRAGMA user_version = {mig_version}")
            conn.commit()
        except Exception:
            conn.rollback()
            logger.error(
                "Schema migration %04d_%s FAILED; user_version stays at %d",
                mig_version,
                name,
                version,
            )
            raise
        version = mig_version
