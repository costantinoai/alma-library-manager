"""Dependency injection for FastAPI routes.

This module provides reusable dependencies for database connections,
plugin registry access, authentication, and other shared resources.

IMPORTANT: All path resolution is delegated to alma.config module.
Do not add hardcoded paths here.

The codebase uses a **single** unified database (scholar.db).
The canonical dependency is ``get_db()``.

Schema initialisation is performed **once** at startup via
``init_db_schema()``, called from the FastAPI lifespan handler.
``get_db()`` is a lightweight per-request connection provider that
does no DDL work, eliminating lock contention with background jobs.

v3 Schema: UUID-based papers table, discovery lenses, feed items,
digest-based alerts.  Clean reset — no migration code.
"""

import os
import sqlite3
import logging
import threading
from typing import Generator, Optional
from pathlib import Path

from fastapi import Depends, HTTPException, status, Header
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials

from alma.plugins.registry import PluginRegistry, get_global_registry
from alma.config import (
    get_data_dir,
    get_db_path,
)
from alma.ai.embedding_sources import EMBEDDING_SOURCE_UNKNOWN
from alma.discovery.defaults import DISCOVERY_SETTINGS_DEFAULTS
from alma.discovery.semantic_scholar import S2_SPECTER2_MODEL

logger = logging.getLogger(__name__)
_schema_init_lock = threading.Lock()
_schema_initialized = False
_schema_initialized_path: str | None = None


# Security
security = HTTPBearer(auto_error=False)

# Configuration helpers (now delegated to config module)
def _data_dir() -> str:
    """Get data directory path as string.

    DEPRECATED: Use alma.config.get_data_dir() directly.
    Kept for backward compatibility.
    """
    return str(get_data_dir())


def _db_path() -> str:
    """Get the unified database path as string."""
    return str(get_db_path())


def normalize_author_id(raw_id: str | None) -> str | None:
    """Normalize an OpenAlex author ID to bare form (e.g. 'A1234567890').

    Handles full URLs like 'https://openalex.org/A1234567890' and
    returns just the ID portion. Returns None for empty/invalid input.
    """
    if not raw_id:
        return None
    s = raw_id.strip()
    if s.startswith("https://openalex.org/"):
        s = s[len("https://openalex.org/"):]
    elif s.startswith("http://openalex.org/"):
        s = s[len("http://openalex.org/"):]
    return s if s else None


# API key for authentication (None or empty = no auth required)
_api_key_raw = os.getenv("API_KEY", "")
API_KEY = _api_key_raw.strip() if _api_key_raw else None


# ============================================================================
# Database Schema Initialisation (run once at startup)
# ============================================================================

def _detect_default_python_env() -> tuple[str, str]:
    """Pick a sensible default Python environment for AI dependencies.

    The Docker images bundle a fully-populated venv at ``/opt/venv``;
    when that's present, point the AI dependency resolver at it so the
    UI shows a green-light environment out of the box. Bare-metal
    installs fall back to whatever Python is invoking us.
    """
    docker_venv = "/opt/venv"
    if os.path.isfile(os.path.join(docker_venv, "bin", "python")):
        return ("venv", docker_venv)
    return ("system", "")


_default_env_type, _default_env_path = _detect_default_python_env()

_DEFAULT_DISCOVERY_SETTINGS = dict(DISCOVERY_SETTINGS_DEFAULTS)
_DEFAULT_DISCOVERY_SETTINGS.update(
    {
        # ai.provider gates LOCAL embedding computation. "none" means
        # "don't compute locally" — Semantic Scholar's pre-computed
        # SPECTER2 vectors still get fetched for any paper with a DOI
        # via the separate fetch_source layer, which is the zero-config
        # default. Users who want local encoding for missing papers
        # flip this to "local" in Settings → AI & embeddings (requires
        # the normal Docker variant or a local install with the AI
        # extras).
        "ai.provider": "none",
        "ai.python_env_type": _default_env_type,
        "ai.python_env_path": _default_env_path,
    }
)


def _table_columns(conn: sqlite3.Connection, table: str) -> set[str]:
    try:
        rows = conn.execute(f"PRAGMA table_info({table})").fetchall()
    except sqlite3.OperationalError:
        return set()
    return {str(row[1]) for row in rows}


def _ensure_columns(
    conn: sqlite3.Connection,
    table: str,
    columns: dict[str, str],
) -> None:
    existing = _table_columns(conn, table)
    for name, ddl in columns.items():
        if name in existing:
            continue
        try:
            conn.execute(f"ALTER TABLE {table} ADD COLUMN {name} {ddl}")
        except sqlite3.OperationalError:
            continue


def _safe_execute(conn: sqlite3.Connection, sql: str) -> None:
    try:
        conn.execute(sql)
    except sqlite3.OperationalError:
        logger.debug("Skipping schema statement: %s", sql, exc_info=True)


def _heal_papers_openalex_id_url_form(conn: sqlite3.Connection) -> None:
    """One-shot heal: fold URL-form ``papers.openalex_id`` to bare form.

    Must run BEFORE the partial UNIQUE index on ``openalex_id`` is
    (re-)asserted, otherwise ``CREATE UNIQUE INDEX`` raises
    ``IntegrityError`` on any URL-form / bare-form duplicate pair.

    Three-step twin-safe normalization:
      (1) null the URL-form value on rows whose bare-form twin already
          exists (keeps the bare canonical — preprint_dedup can later
          collapse the preprint side),
      (2) de-duplicate URL-form rows that share the same bare id among
          themselves (no pre-existing bare twin): keep the lowest rowid,
          null the rest,
      (3) normalize the remaining (now-unique) URL-form rows to bare.

    Idempotent: a second call is a no-op because step (3) eliminates
    every URL-form row.
    """
    try:
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
    except Exception:
        logger.debug("papers.openalex_id URL heal skipped", exc_info=True)


def _heal_papers_blank_identifiers(conn: sqlite3.Connection) -> None:
    """One-shot heal: coerce blank identifiers (``''`` / whitespace) on
    ``papers.openalex_id`` / ``papers.doi`` / ``papers.semantic_scholar_id``
    to ``NULL`` so the partial UNIQUE indexes can't collide on the empty
    string (which is NOT NULL under the index predicate).

    Must run BEFORE the partial UNIQUE index creation for the same
    reason as the URL heal.  Idempotent.
    """
    try:
        for col in ("openalex_id", "doi", "semantic_scholar_id"):
            conn.execute(
                f"UPDATE papers SET {col} = NULL "
                f"WHERE {col} IS NOT NULL AND TRIM({col}) = ''"
            )
    except Exception:
        logger.debug("papers blank-identifier heal skipped", exc_info=True)


def _migrate_publication_embeddings_to_float16(conn: sqlite3.Connection) -> int:
    """Re-encode any float32 ``publication_embeddings`` blobs as float16.

    For each ``model``, the modal blob length is taken as the canonical
    (float16) length. Rows with a blob exactly twice that length and
    divisible by 4 are interpreted as the legacy float32 encoding from
    writers that bypassed ``core.vector_blob.encode_vector``; they are
    decoded as float32 and re-encoded through the canonical helper.
    Returns the number of rows fixed. Idempotent.
    """
    try:
        rows = conn.execute(
            "SELECT model, length(embedding) AS n, COUNT(*) AS c "
            "FROM publication_embeddings GROUP BY model, length(embedding)"
        ).fetchall()
    except sqlite3.OperationalError:
        return 0

    counts: dict[tuple[str, int], int] = {
        (str(r["model"]), int(r["n"])): int(r["c"]) for r in rows
    }
    if not counts:
        return 0

    modal_len: dict[str, int] = {}
    for (model, n), c in counts.items():
        if model not in modal_len or c > counts[(model, modal_len[model])]:
            modal_len[model] = n

    import numpy as np
    from alma.core.vector_blob import encode_vector

    fixed = 0
    for model, mod_len in modal_len.items():
        target_len = mod_len * 2
        if target_len % 4 != 0 or counts.get((model, target_len), 0) == 0:
            continue
        broken = conn.execute(
            "SELECT paper_id, embedding FROM publication_embeddings "
            "WHERE model = ? AND length(embedding) = ?",
            (model, target_len),
        ).fetchall()
        for row in broken:
            try:
                vec = np.frombuffer(row["embedding"], dtype=np.float32)
                new_blob = encode_vector(vec)
            except Exception:
                continue
            conn.execute(
                "UPDATE publication_embeddings SET embedding = ? "
                "WHERE paper_id = ? AND model = ?",
                (new_blob, row["paper_id"], model),
            )
            fixed += 1

    if fixed:
        logger.info(
            "Re-encoded %d publication_embeddings rows from float32 to float16",
            fixed,
        )
    return fixed


def init_db_schema() -> None:
    """Create all tables and seed defaults.

    Call this **once** during application startup (from the FastAPI lifespan
    handler).  This keeps heavy DDL work out of per-request ``get_db()``,
    eliminating SQLite lock contention with background jobs.

    v3 schema: clean reset with UUID-based papers, discovery lenses,
    feed items, and digest-based alerts.
    """
    global _schema_initialized, _schema_initialized_path

    with _schema_init_lock:
        db_path = Path(_db_path())
        db_path_str = str(db_path)
        if _schema_initialized and _schema_initialized_path == db_path_str:
            return
        db_path.parent.mkdir(parents=True, exist_ok=True)

        db = db_path_str
        conn = sqlite3.connect(db, check_same_thread=False)
        conn.row_factory = sqlite3.Row
        try:
            # ---- SQLite performance pragmas ----
            # auto_vacuum=INCREMENTAL must be set BEFORE any tables are
            # created — its mode is locked once the file has content.
            # The scheduled `db_maintenance` job calls
            # `PRAGMA incremental_vacuum` daily to release freed pages.
            conn.execute("PRAGMA auto_vacuum=INCREMENTAL")
            conn.execute("PRAGMA journal_mode=WAL")
            conn.execute("PRAGMA busy_timeout=30000")
            conn.execute("PRAGMA foreign_keys=ON")

            # ==============================================================
            # CORE: Papers
            # ==============================================================
            conn.execute(
                """CREATE TABLE IF NOT EXISTS papers (
                    id TEXT PRIMARY KEY,

                    -- Core bibliographic data
                    title TEXT NOT NULL,
                    authors TEXT,
                    year INTEGER,
                    journal TEXT,
                    abstract TEXT,
                    url TEXT,
                    doi TEXT,
                    publication_date TEXT,

                    -- OpenAlex metadata
                    openalex_id TEXT,
                    work_type TEXT,
                    language TEXT,
                    is_oa INTEGER DEFAULT 0,
                    oa_status TEXT,
                    oa_url TEXT,
                    is_retracted INTEGER DEFAULT 0,
                    fwci REAL,
                    cited_by_count INTEGER DEFAULT 0,
                    cited_by_percentile_min REAL,
                    cited_by_percentile_max REAL,
                    referenced_works_count INTEGER DEFAULT 0,

                    -- Bibliographic details
                    volume TEXT,
                    issue TEXT,
                    first_page TEXT,
                    last_page TEXT,
                    institutions_count INTEGER DEFAULT 0,
                    countries_count INTEGER DEFAULT 0,

                    -- JSON fields
                    keywords TEXT,
                    sdgs TEXT,
                    counts_by_year TEXT,

                    -- Status lifecycle: tracked | library | dismissed | removed
                    status TEXT NOT NULL DEFAULT 'tracked',

                    -- Library metadata (populated when status = 'library')
                    rating INTEGER DEFAULT 0,
                    notes TEXT,
                    added_at TEXT,
                    added_from TEXT,

                    -- Signal aggregation
                    global_signal_score REAL DEFAULT 0.0,

                    -- Resolution tracking (for imports)
                    openalex_resolution_status TEXT,
                    openalex_resolution_reason TEXT,
                    openalex_resolution_updated_at TEXT,

                    -- Provenance and ownership
                    author_id TEXT DEFAULT '',
                    source_id TEXT DEFAULT '',
                    fetched_at TEXT,
                    created_at TEXT DEFAULT (datetime('now')),
                    updated_at TEXT DEFAULT (datetime('now'))
                )"""
            )
            _ensure_columns(
                conn,
                "papers",
                {
                    "authors": "TEXT",
                    "year": "INTEGER",
                    "journal": "TEXT",
                    "abstract": "TEXT",
                    "url": "TEXT",
                    "doi": "TEXT",
                    "publication_date": "TEXT",
                    "openalex_id": "TEXT",
                    "semantic_scholar_id": "TEXT",
                    "semantic_scholar_corpus_id": "TEXT",
                    "work_type": "TEXT",
                    "language": "TEXT",
                    "is_oa": "INTEGER DEFAULT 0",
                    "oa_status": "TEXT",
                    "oa_url": "TEXT",
                    "is_retracted": "INTEGER DEFAULT 0",
                    "fwci": "REAL",
                    "cited_by_count": "INTEGER DEFAULT 0",
                    "cited_by_percentile_min": "REAL",
                    "cited_by_percentile_max": "REAL",
                    "referenced_works_count": "INTEGER DEFAULT 0",
                    "volume": "TEXT",
                    "issue": "TEXT",
                    "first_page": "TEXT",
                    "last_page": "TEXT",
                    "institutions_count": "INTEGER DEFAULT 0",
                    "countries_count": "INTEGER DEFAULT 0",
                    "keywords": "TEXT",
                    "sdgs": "TEXT",
                    "counts_by_year": "TEXT",
                    "status": "TEXT NOT NULL DEFAULT 'tracked'",
                    "rating": "INTEGER DEFAULT 0",
                    "notes": "TEXT",
                    "added_at": "TEXT",
                    "added_from": "TEXT",
                    "global_signal_score": "REAL DEFAULT 0.0",
                    "openalex_resolution_status": "TEXT",
                    "openalex_resolution_reason": "TEXT",
                    "openalex_resolution_updated_at": "TEXT",
                    "author_id": "TEXT DEFAULT ''",
                    "source_id": "TEXT DEFAULT ''",
                    "fetched_at": "TEXT",
                    "created_at": "TEXT DEFAULT (datetime('now'))",
                    "updated_at": "TEXT DEFAULT (datetime('now'))",
                    # Preprint↔journal dedup (2026-04-24): when the same
                    # work exists as both a preprint and a published journal
                    # row, the preprint points to the journal row here and
                    # gets filtered out of Library + Discovery lists. NULL
                    # = canonical. See alma.application.preprint_dedup.
                    "canonical_paper_id": "TEXT",
                    "preprint_source": "TEXT",  # arxiv / biorxiv / psyrxiv / osf / chemrxiv
                },
            )
            # Identifier heals must run BEFORE the partial UNIQUE indexes
            # are asserted — otherwise `CREATE UNIQUE INDEX` raises
            # `IntegrityError: UNIQUE constraint failed` on any legacy
            # duplicate and init_db_schema crashes hard (swallowed
            # `OperationalError` in `_safe_execute` does NOT cover
            # IntegrityError).  Pre-2026-04-25 these heals lived far
            # below the index-create block, so a DB with legacy
            # URL-form / blank identifier duplicates would never boot
            # past schema init.  Running them here is safe because:
            # the `papers` table exists (just created), any legacy rows
            # predate this init call, and the heals are idempotent.
            _heal_papers_openalex_id_url_form(conn)
            _heal_papers_blank_identifiers(conn)
            for idx_sql in [
                "CREATE INDEX IF NOT EXISTS idx_papers_status ON papers(status)",
                "CREATE UNIQUE INDEX IF NOT EXISTS idx_papers_openalex_id ON papers(openalex_id) WHERE openalex_id IS NOT NULL",
                "CREATE INDEX IF NOT EXISTS idx_papers_semantic_scholar_id ON papers(semantic_scholar_id)",
                "CREATE INDEX IF NOT EXISTS idx_papers_doi ON papers(doi)",
                "CREATE INDEX IF NOT EXISTS idx_papers_year ON papers(year DESC)",
                "CREATE INDEX IF NOT EXISTS idx_papers_title ON papers(title)",
                "CREATE INDEX IF NOT EXISTS idx_papers_added_at ON papers(added_at DESC)",
                "CREATE INDEX IF NOT EXISTS idx_papers_signal ON papers(global_signal_score DESC)",
                "CREATE INDEX IF NOT EXISTS idx_papers_resolution ON papers(openalex_resolution_status)",
                "CREATE INDEX IF NOT EXISTS idx_papers_canonical ON papers(canonical_paper_id) WHERE canonical_paper_id IS NOT NULL",
            ]:
                _safe_execute(conn, idx_sql)

            _ensure_columns(
                conn,
                "papers",
                {
                    "reading_status": "TEXT DEFAULT NULL",
                    # T5: S2 tldr is a 1-2 sentence AI summary of the
                    # paper; dense coverage in CS + biomedicine, sparse
                    # elsewhere. PaperCard renders it italic under the
                    # abstract when present.
                    "tldr": "TEXT",
                    # T5: S2's learned "this citation mattered" count.
                    # Supplements raw `cited_by_count` in the
                    # `citation_quality` scoring signal.
                    "influential_citation_count": "INTEGER DEFAULT 0",
                },
            )
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

            # D2 lifecycle change (2026-04-26): collapse the `queued`
            # reading-state into `reading`. The queue/reading split was
            # a v1 distinction; v3 treats reading-list membership as
            # the reading state — anything on the list is `reading`.
            # Idempotent: rows already at `reading` are unaffected.
            conn.execute(
                "UPDATE papers SET reading_status = 'reading' "
                "WHERE reading_status = 'queued'"
            )

            # ==============================================================
            # CORE: Authors
            # ==============================================================
            conn.execute(
                """CREATE TABLE IF NOT EXISTS authors (
                    id TEXT PRIMARY KEY,
                    name TEXT NOT NULL,
                    openalex_id TEXT,
                    orcid TEXT,
                    scholar_id TEXT,
                    affiliation TEXT,
                    citedby INTEGER DEFAULT 0,
                    h_index INTEGER DEFAULT 0,
                    interests TEXT,
                    url_picture TEXT,
                    works_count INTEGER DEFAULT 0,
                    last_fetched_at TEXT,
                    cited_by_year TEXT,
                    institutions TEXT,
                    email_domain TEXT,
                    added_at TEXT,

                    -- Author classification
                    author_type TEXT DEFAULT 'followed',

                    -- Identity resolution
                    id_resolution_status TEXT,
                    id_resolution_reason TEXT,
                    id_resolution_updated_at TEXT
                )"""
            )
            _ensure_columns(
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
                },
            )

            conn.execute(
                """CREATE TABLE IF NOT EXISTS followed_authors (
                    author_id TEXT PRIMARY KEY,
                    followed_at TEXT NOT NULL,
                    notify_new_papers INTEGER DEFAULT 1
                )"""
            )

            # ==============================================================
            # FEED: Author-based inbox
            # ==============================================================
            conn.execute(
                """CREATE TABLE IF NOT EXISTS feed_items (
                    id TEXT PRIMARY KEY,
                    paper_id TEXT NOT NULL REFERENCES papers(id) ON DELETE CASCADE,
                    author_id TEXT NOT NULL,
                    monitor_id TEXT,
                    monitor_type TEXT,
                    monitor_label TEXT,
                    fetched_at TEXT DEFAULT (datetime('now')),
                    status TEXT NOT NULL DEFAULT 'new',
                    signal_value INTEGER DEFAULT 0,
                    score_breakdown TEXT DEFAULT NULL,
                    UNIQUE(paper_id, author_id)
                )"""
            )
            # Migration: add score_breakdown column if missing (existing DBs)
            try:
                conn.execute("ALTER TABLE feed_items ADD COLUMN score_breakdown TEXT DEFAULT NULL")
            except Exception:
                pass  # column already exists
            for alter_sql in [
                "ALTER TABLE feed_items ADD COLUMN monitor_id TEXT",
                "ALTER TABLE feed_items ADD COLUMN monitor_type TEXT",
                "ALTER TABLE feed_items ADD COLUMN monitor_label TEXT",
            ]:
                try:
                    conn.execute(alter_sql)
                except Exception:
                    pass
            conn.execute(
                """
                UPDATE feed_items
                SET status = 'new'
                WHERE status IN ('deferred', 'discovery', 'signal_lab')
                """
            )
            for idx_sql in [
                "CREATE INDEX IF NOT EXISTS idx_feed_status ON feed_items(status)",
                "CREATE INDEX IF NOT EXISTS idx_feed_fetched ON feed_items(fetched_at DESC)",
                "CREATE INDEX IF NOT EXISTS idx_feed_author ON feed_items(author_id)",
                "CREATE INDEX IF NOT EXISTS idx_feed_monitor ON feed_items(monitor_id)",
            ]:
                conn.execute(idx_sql)
            conn.execute(
                """CREATE TABLE IF NOT EXISTS feed_monitors (
                    id TEXT PRIMARY KEY,
                    monitor_type TEXT NOT NULL,
                    monitor_key TEXT NOT NULL,
                    label TEXT NOT NULL,
                    author_id TEXT,
                    config_json TEXT,
                    enabled INTEGER DEFAULT 1,
                    created_at TEXT DEFAULT (datetime('now')),
                    updated_at TEXT DEFAULT (datetime('now')),
                    last_checked_at TEXT,
                    last_success_at TEXT,
                    last_status TEXT,
                    last_error TEXT,
                    last_result_json TEXT
                )"""
            )
            for idx_sql in [
                "CREATE UNIQUE INDEX IF NOT EXISTS idx_feed_monitors_type_key ON feed_monitors(monitor_type, monitor_key)",
                "CREATE INDEX IF NOT EXISTS idx_feed_monitors_author ON feed_monitors(author_id)",
                "CREATE INDEX IF NOT EXISTS idx_feed_monitors_enabled ON feed_monitors(enabled)",
            ]:
                conn.execute(idx_sql)

            # ==============================================================
            # DISCOVERY: Lenses + Suggestion Sets + Recommendations
            # ==============================================================
            conn.execute(
                """CREATE TABLE IF NOT EXISTS discovery_lenses (
                    id TEXT PRIMARY KEY,
                    name TEXT NOT NULL,
                    context_type TEXT NOT NULL,
                    context_config TEXT,
                    weights TEXT,
                    branch_controls TEXT,
                    preference_profile TEXT,
                    created_at TEXT DEFAULT (datetime('now')),
                    last_refreshed_at TEXT,
                    is_active INTEGER DEFAULT 1
                )"""
            )
            try:
                conn.execute("ALTER TABLE discovery_lenses ADD COLUMN branch_controls TEXT")
            except Exception:
                pass
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_lenses_type ON discovery_lenses(context_type)"
            )

            conn.execute(
                """CREATE TABLE IF NOT EXISTS suggestion_sets (
                    id TEXT PRIMARY KEY,
                    lens_id TEXT NOT NULL REFERENCES discovery_lenses(id) ON DELETE CASCADE,
                    context_type TEXT NOT NULL,
                    trigger_source TEXT NOT NULL,
                    retrieval_summary TEXT,
                    ranker_version TEXT,
                    created_at TEXT DEFAULT (datetime('now'))
                )"""
            )
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_suggestion_sets_lens ON suggestion_sets(lens_id)"
            )

            conn.execute(
                """CREATE TABLE IF NOT EXISTS recommendations (
                    id TEXT PRIMARY KEY,
                    suggestion_set_id TEXT REFERENCES suggestion_sets(id) ON DELETE CASCADE,
                    lens_id TEXT REFERENCES discovery_lenses(id) ON DELETE CASCADE,
                    paper_id TEXT NOT NULL REFERENCES papers(id) ON DELETE CASCADE,
                    rank INTEGER,
                    score REAL NOT NULL,
                    score_breakdown TEXT,
                    user_action TEXT,
                    action_at TEXT,
                    created_at TEXT DEFAULT (datetime('now')),
                    explanation TEXT
                )"""
            )
            for idx_sql in [
                "CREATE INDEX IF NOT EXISTS idx_recs_set ON recommendations(suggestion_set_id)",
                "CREATE INDEX IF NOT EXISTS idx_recs_lens ON recommendations(lens_id)",
                "CREATE INDEX IF NOT EXISTS idx_recs_paper ON recommendations(paper_id)",
                "CREATE INDEX IF NOT EXISTS idx_recs_action ON recommendations(user_action)",
            ]:
                conn.execute(idx_sql)

            # Migration: add explanation column if missing (existing DBs)
            try:
                conn.execute("ALTER TABLE recommendations ADD COLUMN explanation TEXT")
            except Exception:
                pass  # column already exists
            for ddl in (
                "ALTER TABLE recommendations ADD COLUMN source_type TEXT",
                "ALTER TABLE recommendations ADD COLUMN source_api TEXT",
                "ALTER TABLE recommendations ADD COLUMN source_key TEXT",
                "ALTER TABLE recommendations ADD COLUMN branch_id TEXT",
                "ALTER TABLE recommendations ADD COLUMN branch_label TEXT",
                "ALTER TABLE recommendations ADD COLUMN branch_mode TEXT",
            ):
                try:
                    conn.execute(ddl)
                except Exception:
                    pass

            # Migration: drop the legacy `query_plan_used_ai` column from
            # `recommendations`. The LLM-backed branch query planner was
            # removed in 2026-04 (see tasks/01_LLM_PRODUCTION_EXIT.md), so
            # the column no longer carries meaning. SQLite >= 3.35 supports
            # DROP COLUMN directly; we ignore failures because (a) older
            # SQLite versions raise OperationalError and (b) fresh DBs
            # never had the column in the first place.
            try:
                cols = _table_columns(conn, "recommendations")
                if "query_plan_used_ai" in cols:
                    conn.execute("ALTER TABLE recommendations DROP COLUMN query_plan_used_ai")
            except Exception:
                pass

            conn.execute(
                """CREATE TABLE IF NOT EXISTS lens_signals (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    lens_id TEXT NOT NULL REFERENCES discovery_lenses(id) ON DELETE CASCADE,
                    paper_id TEXT NOT NULL REFERENCES papers(id) ON DELETE CASCADE,
                    signal_value INTEGER NOT NULL,
                    source TEXT NOT NULL DEFAULT 'user',
                    created_at TEXT DEFAULT (datetime('now')),
                    UNIQUE(lens_id, paper_id, source)
                )"""
            )
            for idx_sql in [
                "CREATE INDEX IF NOT EXISTS idx_lens_signals_lens ON lens_signals(lens_id)",
                "CREATE INDEX IF NOT EXISTS idx_lens_signals_paper ON lens_signals(paper_id)",
            ]:
                conn.execute(idx_sql)

            # ==============================================================
            # LIBRARY: Collections + Tags
            # ==============================================================
            conn.execute(
                """CREATE TABLE IF NOT EXISTS collections (
                    id TEXT PRIMARY KEY,
                    name TEXT NOT NULL UNIQUE,
                    description TEXT,
                    color TEXT DEFAULT '#3B82F6',
                    created_at TEXT NOT NULL
                )"""
            )
            conn.execute(
                """CREATE TABLE IF NOT EXISTS collection_items (
                    collection_id TEXT NOT NULL REFERENCES collections(id) ON DELETE CASCADE,
                    paper_id TEXT NOT NULL REFERENCES papers(id) ON DELETE CASCADE,
                    added_at TEXT NOT NULL,
                    PRIMARY KEY (collection_id, paper_id)
                )"""
            )

            conn.execute(
                """CREATE TABLE IF NOT EXISTS tags (
                    id TEXT PRIMARY KEY,
                    name TEXT NOT NULL UNIQUE,
                    color TEXT DEFAULT '#6B7280'
                )"""
            )
            conn.execute(
                """CREATE TABLE IF NOT EXISTS publication_tags (
                    paper_id TEXT NOT NULL REFERENCES papers(id) ON DELETE CASCADE,
                    tag_id TEXT NOT NULL REFERENCES tags(id) ON DELETE CASCADE,
                    PRIMARY KEY (paper_id, tag_id)
                )"""
            )

            conn.execute(
                """CREATE TABLE IF NOT EXISTS tag_suggestions (
                    paper_id TEXT NOT NULL REFERENCES papers(id) ON DELETE CASCADE,
                    tag TEXT NOT NULL,
                    tag_id TEXT,
                    confidence REAL NOT NULL,
                    source TEXT NOT NULL,
                    accepted INTEGER DEFAULT 0,
                    created_at TEXT DEFAULT (datetime('now')),
                    PRIMARY KEY (paper_id, tag)
                )"""
            )

            # ==============================================================
            # TOPICS: OpenAlex taxonomy
            # ==============================================================
            conn.execute(
                """CREATE TABLE IF NOT EXISTS topics (
                    topic_id TEXT PRIMARY KEY,
                    canonical_name TEXT NOT NULL,
                    normalized_name TEXT NOT NULL UNIQUE,
                    source TEXT DEFAULT 'auto',
                    created_at TEXT DEFAULT (datetime('now'))
                )"""
            )
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_topics_normalized ON topics(normalized_name)"
            )

            conn.execute(
                """CREATE TABLE IF NOT EXISTS topic_aliases (
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
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_topic_aliases_topic ON topic_aliases(topic_id)"
            )
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_topic_aliases_normalized ON topic_aliases(normalized_term)"
            )

            conn.execute(
                """CREATE TABLE IF NOT EXISTS publication_topics (
                    paper_id TEXT NOT NULL REFERENCES papers(id) ON DELETE CASCADE,
                    term TEXT NOT NULL,
                    score REAL,
                    domain TEXT DEFAULT '',
                    field TEXT DEFAULT '',
                    subfield TEXT DEFAULT '',
                    topic_id TEXT,
                    PRIMARY KEY (paper_id, term)
                )"""
            )
            _ensure_columns(
                conn,
                "publication_topics",
                {
                    "domain": "TEXT DEFAULT ''",
                    "field": "TEXT DEFAULT ''",
                    "subfield": "TEXT DEFAULT ''",
                    "topic_id": "TEXT",
                },
            )
            _safe_execute(
                conn,
                "CREATE INDEX IF NOT EXISTS idx_pub_topics_topic_id ON publication_topics(topic_id)"
            )

            # ==============================================================
            # AUTHORSHIP: Structured author data from OpenAlex
            # ==============================================================
            conn.execute(
                """CREATE TABLE IF NOT EXISTS publication_authors (
                    paper_id TEXT NOT NULL REFERENCES papers(id) ON DELETE CASCADE,
                    openalex_id TEXT NOT NULL,
                    display_name TEXT NOT NULL,
                    orcid TEXT DEFAULT '',
                    position TEXT DEFAULT '',
                    is_corresponding INTEGER DEFAULT 0,
                    institution TEXT DEFAULT '',
                    PRIMARY KEY (paper_id, openalex_id)
                )"""
            )

            conn.execute(
                """CREATE TABLE IF NOT EXISTS publication_institutions (
                    paper_id TEXT NOT NULL REFERENCES papers(id) ON DELETE CASCADE,
                    institution_id TEXT DEFAULT '',
                    institution_name TEXT DEFAULT '',
                    country_code TEXT DEFAULT ''
                )"""
            )
            conn.execute(
                "CREATE UNIQUE INDEX IF NOT EXISTS ux_pub_inst "
                "ON publication_institutions(paper_id, COALESCE(institution_id, institution_name))"
            )

            conn.execute(
                # OpenAlex work IDs are stored as the bare integer suffix
                # (e.g. ``W65738273`` → ``65738273``). The W-prefix is
                # restored at query time. Storing as INTEGER + WITHOUT
                # ROWID roughly halves on-disk size relative to
                # rowid+TEXT: WITHOUT ROWID merges the table and its
                # autoindex into a single B-tree (no duplication), and
                # an INTEGER referenced_work_id costs ~5 bytes versus
                # ~10 bytes for the W-prefixed string form.
                """CREATE TABLE IF NOT EXISTS publication_references (
                    paper_id TEXT NOT NULL REFERENCES papers(id) ON DELETE CASCADE,
                    referenced_work_id INTEGER NOT NULL,
                    PRIMARY KEY (paper_id, referenced_work_id)
                ) WITHOUT ROWID"""
            )
            # The PK is (paper_id, referenced_work_id) so paper-side
            # lookups use it directly. The corpus-overlap query in the
            # graph retrieval lane filters by referenced_work_id IN
            # (subquery), which without this index becomes O(N²) on a
            # 5k+ paper corpus with dense reference graphs.
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_publication_references_ref "
                "ON publication_references(referenced_work_id)"
            )

            # ==============================================================
            # AI/ML: Embeddings + Clustering
            # ==============================================================
            # The PK is (paper_id, model) so multiple embedding models can
            # coexist per paper (e.g. local SPECTER2 alongside OpenAI
            # vectors). Read paths filter by the active model from
            # discovery_settings.embedding_model; switching models does not
            # destroy vectors produced under the previous model.
            conn.execute(
                """CREATE TABLE IF NOT EXISTS publication_embeddings (
                    paper_id TEXT NOT NULL REFERENCES papers(id) ON DELETE CASCADE,
                    embedding BLOB NOT NULL,
                    model TEXT NOT NULL,
                    source TEXT NOT NULL DEFAULT 'unknown',
                    created_at TEXT NOT NULL,
                    PRIMARY KEY (paper_id, model)
                )"""
            )
            _ensure_columns(
                conn,
                "publication_embeddings",
                {
                    "source": "TEXT NOT NULL DEFAULT 'unknown'",
                },
            )

            # Legacy PK migration: old schema used PRIMARY KEY (paper_id)
            # with a column-level model default. Detect via PRAGMA
            # table_info and rebuild in place, preserving each legacy row
            # under the model name already recorded against it.
            pk_cols = [
                row["name"]
                for row in conn.execute("PRAGMA table_info(publication_embeddings)")
                if row["pk"] > 0
            ]
            if pk_cols == ["paper_id"]:
                logger.info(
                    "Migrating publication_embeddings PK from (paper_id) to (paper_id, model)"
                )
                conn.execute("ALTER TABLE publication_embeddings RENAME TO publication_embeddings_legacy")
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
                    """
                    ,
                    (EMBEDDING_SOURCE_UNKNOWN,),
                )
                conn.execute("DROP TABLE publication_embeddings_legacy")
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_publication_embeddings_model ON publication_embeddings(model)"
            )
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_publication_embeddings_model_source "
                "ON publication_embeddings(model, source)"
            )

            # One-shot dtype migration: prior writers in
            # ``services/s2_vectors`` and ``application/discovery`` packed
            # SPECTER2 vectors as float32 instead of going through
            # ``core.vector_blob.encode_vector`` (float16). The reader
            # decodes as float16 — so a 768-dim float32 row (3072 bytes)
            # came back looking like a 1536-dim vector and broke any
            # ``np.stack`` over a mixed-encoding pool. Detection is
            # per-model: rows whose blob length is exactly twice the
            # modal length (and divisible by 4) are float32-encoded;
            # decode and re-encode through the canonical helper. Once
            # every row matches the canonical length the modal-doubling
            # check stops finding anything, so this is idempotent.
            try:
                _migrate_publication_embeddings_to_float16(conn)
            except Exception:
                logger.warning(
                    "publication_embeddings dtype migration skipped",
                    exc_info=True,
                )

            conn.execute(
                """CREATE TABLE IF NOT EXISTS publication_embedding_fetch_status (
                    paper_id TEXT NOT NULL REFERENCES papers(id) ON DELETE CASCADE,
                    model TEXT NOT NULL,
                    source TEXT NOT NULL,
                    status TEXT NOT NULL,
                    reason TEXT,
                    lookup_key TEXT,
                    lookup_ids_json TEXT,
                    attempts INTEGER NOT NULL DEFAULT 0,
                    updated_at TEXT NOT NULL,
                    PRIMARY KEY (paper_id, model, source)
                )"""
            )
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_embedding_fetch_status_model "
                "ON publication_embedding_fetch_status(model, source, status)"
            )

            conn.execute(
                """CREATE TABLE IF NOT EXISTS paper_enrichment_status (
                    paper_id TEXT NOT NULL REFERENCES papers(id) ON DELETE CASCADE,
                    source TEXT NOT NULL,
                    purpose TEXT NOT NULL,
                    lookup_key TEXT NOT NULL DEFAULT '',
                    fields_key TEXT NOT NULL DEFAULT '',
                    status TEXT NOT NULL,
                    reason TEXT,
                    fields_requested_json TEXT,
                    fields_filled_json TEXT,
                    attempts INTEGER NOT NULL DEFAULT 0,
                    last_attempt_at TEXT,
                    next_retry_at TEXT,
                    updated_at TEXT NOT NULL,
                    PRIMARY KEY (paper_id, source, purpose)
                )"""
            )
            _ensure_columns(
                conn,
                "paper_enrichment_status",
                {
                    "lookup_key": "TEXT NOT NULL DEFAULT ''",
                    "fields_key": "TEXT NOT NULL DEFAULT ''",
                    "reason": "TEXT",
                    "fields_requested_json": "TEXT",
                    "fields_filled_json": "TEXT",
                    "attempts": "INTEGER NOT NULL DEFAULT 0",
                    "last_attempt_at": "TEXT",
                    "next_retry_at": "TEXT",
                    "updated_at": "TEXT NOT NULL DEFAULT (datetime('now'))",
                },
            )
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_paper_enrichment_status_lookup "
                "ON paper_enrichment_status(source, purpose, lookup_key, fields_key, status)"
            )
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_paper_enrichment_status_retry "
                "ON paper_enrichment_status(source, purpose, status, next_retry_at)"
            )

            conn.execute(
                """CREATE TABLE IF NOT EXISTS publication_clusters (
                    paper_id TEXT NOT NULL REFERENCES papers(id) ON DELETE CASCADE,
                    cluster_id INTEGER NOT NULL,
                    label TEXT DEFAULT '',
                    x REAL DEFAULT 0.5,
                    y REAL DEFAULT 0.5,
                    updated_at TEXT DEFAULT (datetime('now')),
                    PRIMARY KEY (paper_id)
                )"""
            )

            # ==============================================================
            # ALERTS: Digest-based system (rules + digests + assignments)
            # ==============================================================
            conn.execute(
                """CREATE TABLE IF NOT EXISTS alert_rules (
                    id TEXT PRIMARY KEY,
                    name TEXT NOT NULL,
                    rule_type TEXT NOT NULL,
                    rule_config TEXT NOT NULL,
                    channels TEXT NOT NULL,
                    enabled INTEGER DEFAULT 1,
                    created_at TEXT NOT NULL
                )"""
            )

            conn.execute(
                """CREATE TABLE IF NOT EXISTS alerts (
                    id TEXT PRIMARY KEY,
                    name TEXT NOT NULL,
                    channels TEXT NOT NULL,
                    schedule TEXT NOT NULL DEFAULT 'manual',
                    schedule_config TEXT,
                    format TEXT DEFAULT 'grouped',
                    enabled INTEGER DEFAULT 1,
                    created_at TEXT NOT NULL,
                    last_evaluated_at TEXT
                )"""
            )

            conn.execute(
                """CREATE TABLE IF NOT EXISTS alert_rule_assignments (
                    alert_id TEXT NOT NULL REFERENCES alerts(id) ON DELETE CASCADE,
                    rule_id TEXT NOT NULL REFERENCES alert_rules(id) ON DELETE CASCADE,
                    PRIMARY KEY (alert_id, rule_id)
                )"""
            )

            conn.execute(
                """CREATE TABLE IF NOT EXISTS alert_history (
                    id TEXT PRIMARY KEY,
                    rule_id TEXT,
                    alert_id TEXT,
                    channel TEXT NOT NULL,
                    paper_id TEXT,
                    sent_at TEXT NOT NULL,
                    status TEXT NOT NULL,
                    message_preview TEXT,
                    publications TEXT,
                    publication_count INTEGER DEFAULT 0,
                    error_message TEXT
                )"""
            )

            conn.execute(
                """CREATE TABLE IF NOT EXISTS alerted_publications (
                    id TEXT PRIMARY KEY,
                    alert_id TEXT NOT NULL REFERENCES alerts(id) ON DELETE CASCADE,
                    paper_id TEXT NOT NULL,
                    alerted_at TEXT NOT NULL,
                    UNIQUE(alert_id, paper_id)
                )"""
            )
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_alerted_alert_paper ON alerted_publications(alert_id, paper_id)"
            )

            # ==============================================================
            # FEEDBACK + PREFERENCES
            # ==============================================================
            conn.execute(
                """CREATE TABLE IF NOT EXISTS feedback_events (
                    id TEXT PRIMARY KEY,
                    event_type TEXT NOT NULL,
                    entity_type TEXT NOT NULL,
                    entity_id TEXT NOT NULL,
                    value TEXT,
                    context_json TEXT,
                    created_at TEXT DEFAULT (datetime('now'))
                )"""
            )
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_feedback_events_entity ON feedback_events(entity_type, entity_id)"
            )
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_feedback_events_type ON feedback_events(event_type, created_at DESC)"
            )

            conn.execute(
                """CREATE TABLE IF NOT EXISTS preference_profiles (
                    entity_type TEXT NOT NULL,
                    entity_id TEXT NOT NULL,
                    affinity_weight REAL DEFAULT 0.0,
                    confidence REAL DEFAULT 0.0,
                    interaction_count INTEGER DEFAULT 0,
                    last_updated TEXT DEFAULT (datetime('now')),
                    PRIMARY KEY (entity_type, entity_id)
                )"""
            )

            # ==============================================================
            # SETTINGS + CACHING
            # ==============================================================
            conn.execute(
                """CREATE TABLE IF NOT EXISTS discovery_settings (
                    key TEXT PRIMARY KEY,
                    value TEXT NOT NULL,
                    updated_at TEXT DEFAULT (datetime('now'))
                )"""
            )
            for k, v in _DEFAULT_DISCOVERY_SETTINGS.items():
                conn.execute(
                    "INSERT OR IGNORE INTO discovery_settings (key, value) VALUES (?, ?)",
                    (k, v),
                )
            # One-way cleanup of removed local embedding options. Runtime code
            # only knows the canonical `local`/SPECTER2 provider.
            conn.execute(
                """
                UPDATE discovery_settings
                SET value = 'local'
                WHERE key = 'ai.provider' AND value = 'minilm'
                """
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
            # LLM settings were removed in 2026-04 (see
            # tasks/01_LLM_PRODUCTION_EXIT.md) — drop every leftover
            # ai.llm_* / ai.openai_llm_* / ai.anthropic_* /
            # strategies.ai_query_planner key from existing DBs in one
            # pass. Ollama (both LLM and embedding) was removed in the
            # same wave: drop ai.ollama_* and migrate any rows where
            # ai.provider = 'ollama' to 'none'. Keep the embedding-side
            # keys (ai.provider, ai.local_model, ai.python_env_*) untouched.
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
                conn.execute(
                    "DELETE FROM discovery_settings WHERE key = ?",
                    (legacy_key,),
                )
            conn.execute(
                """
                UPDATE discovery_settings
                SET value = 'none'
                WHERE key = 'ai.provider'
                  AND value NOT IN ('none', 'local', 'openai')
                """
            )

            conn.execute(
                """CREATE TABLE IF NOT EXISTS graph_cache (
                    graph_type TEXT PRIMARY KEY,
                    data TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                )"""
            )

            # LLM-generated cluster labels, cached per cluster signature.
            # Signature = hash of sorted member IDs, so a relabel triggers
            # only when the cluster composition actually changes.
            conn.execute(
                """CREATE TABLE IF NOT EXISTS graph_cluster_labels (
                    graph_type TEXT NOT NULL,
                    scope TEXT NOT NULL,
                    cluster_signature TEXT NOT NULL,
                    label TEXT NOT NULL,
                    description TEXT DEFAULT '',
                    top_terms TEXT DEFAULT '[]',
                    model TEXT DEFAULT '',
                    updated_at TEXT NOT NULL,
                    PRIMARY KEY (graph_type, scope, cluster_signature)
                )"""
            )
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_graph_cluster_labels_lookup "
                "ON graph_cluster_labels(graph_type, scope)"
            )

            conn.execute(
                """CREATE TABLE IF NOT EXISTS similarity_cache (
                    cache_key TEXT PRIMARY KEY,
                    paper_ids TEXT NOT NULL,
                    results TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    expires_at TEXT NOT NULL
                )"""
            )

            # T6b — cache for `/papers/{id}/prior-works` + `derivative-works`
            # S2 fallback. Keyed by (local paper_id, direction). 24 h
            # TTL is long enough that repeat dialog opens hit the cache
            # but short enough that newly-published citations eventually
            # surface. Payload is the full merged + deduped works list
            # so the read path doesn't need to re-merge on every hit.
            conn.execute(
                """CREATE TABLE IF NOT EXISTS paper_network_cache (
                    paper_id TEXT NOT NULL,
                    direction TEXT NOT NULL,
                    fetched_at TEXT NOT NULL,
                    expires_at TEXT NOT NULL,
                    payload_json TEXT NOT NULL,
                    PRIMARY KEY (paper_id, direction)
                )"""
            )
            _safe_execute(
                conn,
                "CREATE INDEX IF NOT EXISTS idx_paper_network_cache_expires "
                "ON paper_network_cache(expires_at)",
            )

            # ==============================================================
            # OPERATIONS: Background jobs
            # ==============================================================
            conn.execute(
                """CREATE TABLE IF NOT EXISTS operation_status (
                    job_id TEXT PRIMARY KEY,
                    status TEXT NOT NULL,
                    message TEXT,
                    error TEXT,
                    started_at TEXT,
                    finished_at TEXT,
                    updated_at TEXT NOT NULL,
                    processed INTEGER,
                    total INTEGER,
                    current_author TEXT,
                    operation_key TEXT,
                    trigger_source TEXT,
                    cancel_requested INTEGER DEFAULT 0,
                    result_json TEXT,
                    metadata_json TEXT
                )"""
            )
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_operation_status_updated ON operation_status(updated_at DESC)"
            )
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_operation_status_key_status ON operation_status(operation_key, status, updated_at DESC)"
            )

            conn.execute(
                """CREATE TABLE IF NOT EXISTS operation_logs (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    job_id TEXT NOT NULL,
                    timestamp TEXT NOT NULL,
                    level TEXT NOT NULL,
                    step TEXT,
                    message TEXT NOT NULL,
                    data_json TEXT
                )"""
            )
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_operation_logs_job_time ON operation_logs(job_id, timestamp DESC)"
            )

            # ==============================================================
            # CACHING: Library-derived artifact cache
            # ==============================================================
            conn.execute(
                """CREATE TABLE IF NOT EXISTS scoring_cache (
                    cache_key TEXT PRIMARY KEY,
                    cache_type TEXT NOT NULL,
                    fingerprint TEXT NOT NULL,
                    value_json TEXT,
                    value_blob BLOB,
                    created_at TEXT DEFAULT (datetime('now'))
                )"""
            )

            # D12 AUTH-SUG: corpus-wide per-author SPECTER2 centroid cache.
            # Populated by `author_backfill.refresh_author_works_and_vectors`
            # and maintained incrementally when new `publication_embeddings`
            # rows arrive. Centroid is over ALL of an author's papers (not
            # Library-scoped — see lesson "feature vectors should be built
            # from the widest available ground truth for that entity").
            conn.execute(
                """CREATE TABLE IF NOT EXISTS author_centroids (
                    author_openalex_id TEXT NOT NULL,
                    model              TEXT NOT NULL,
                    centroid_blob      BLOB NOT NULL,
                    paper_count        INTEGER NOT NULL,
                    updated_at         TEXT NOT NULL,
                    PRIMARY KEY (author_openalex_id, model)
                )"""
            )

            # D12 AUTH-SUG-3/4: TTL cache for network-backed author
            # suggestion buckets. One row per (source, seed-set hash).
            # `payload_json` is an already-scored + sorted candidate list
            # written by the refresh runners; `list_author_suggestions`
            # only reads it. Stale rows are served synchronously while a
            # background refresh recomputes (stale-cache synchronous).
            conn.execute(
                """CREATE TABLE IF NOT EXISTS author_suggestion_cache (
                    source       TEXT NOT NULL,
                    cache_key    TEXT NOT NULL,
                    payload_json TEXT NOT NULL,
                    seed_count   INTEGER NOT NULL DEFAULT 0,
                    computed_at  TEXT NOT NULL,
                    expires_at   TEXT NOT NULL,
                    PRIMARY KEY (source, cache_key)
                )"""
            )
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_author_suggestion_cache_expires "
                "ON author_suggestion_cache(source, expires_at)"
            )

            try:
                from alma.application.followed_authors import ensure_followed_author_contract

                ensure_followed_author_contract(conn)
            except Exception:
                logger.debug("followed_authors canonicalization skipped during init", exc_info=True)

            # URL-form heal: moved to the pre-index block (see
            # `_heal_papers_openalex_id_url_form`).  This slot is kept
            # intentionally empty so the authors-heal below still lands
            # in a predictable location relative to its comments; the
            # new call site ahead of the partial UNIQUE index creation
            # is the authoritative one.

            # One-shot heal: normalize `authors.openalex_id` to the
            # canonical `A...` bare form. Three legacy drifts seen in
            # real DBs as of 2026-04-24: (a) lowercase `a...` prefix,
            # (b) `3A...` residue from a buggy `%3A` URL-decode pass
            # that persisted the URL-encoded colon, (c) URL-form
            # `https://openalex.org/A...`. Any of these makes
            # `fetch_author_profile` return 404 because OpenAlex only
            # accepts the canonical form, which silently fails every
            # deep refresh for that author.
            #
            # The partial UNIQUE index `ux_authors_openalex_norm` on
            # `lower(openalex_id)` means a naive bulk UPDATE can
            # collide with a twin row that holds the canonical form.
            # In that case we null out the corrupt value so the row
            # stays eligible for re-resolution instead of losing the
            # canonical twin. Each heal is wrapped individually so a
            # single bad row can't block the rest.
            try:
                author_heals = {"url_form": 0, "triple_a": 0, "lower_a": 0, "collisions_nulled": 0}

                author_heals["url_form"] = conn.execute(
                    "UPDATE authors SET openalex_id = SUBSTR(openalex_id, 22) "
                    "WHERE openalex_id LIKE 'https://openalex.org/%' "
                    "AND NOT EXISTS ("
                    "  SELECT 1 FROM authors twin "
                    "  WHERE lower(twin.openalex_id) = lower(SUBSTR(authors.openalex_id, 22)) "
                    "  AND twin.id != authors.id"
                    ")"
                ).rowcount
                # `3A<x>` case: stripping gives `<x>`; if a twin already
                # has `<x>` or its uppercase sibling, null out this row's
                # corrupt id instead of colliding.
                author_heals["triple_a"] = conn.execute(
                    "UPDATE authors SET openalex_id = SUBSTR(openalex_id, 3) "
                    "WHERE openalex_id GLOB '3[Aa]*' "
                    "AND NOT EXISTS ("
                    "  SELECT 1 FROM authors twin "
                    "  WHERE lower(twin.openalex_id) = lower(SUBSTR(authors.openalex_id, 3)) "
                    "  AND twin.id != authors.id"
                    ")"
                ).rowcount
                author_heals["collisions_nulled"] = conn.execute(
                    "UPDATE authors SET openalex_id = NULL "
                    "WHERE openalex_id GLOB '3[Aa]*'"
                ).rowcount
                # Uppercase leading `a` → `A`. Again twin-safe.
                author_heals["lower_a"] = conn.execute(
                    "UPDATE authors SET openalex_id = 'A' || SUBSTR(openalex_id, 2) "
                    "WHERE openalex_id LIKE 'a%' "
                    "AND NOT EXISTS ("
                    "  SELECT 1 FROM authors twin "
                    "  WHERE lower(twin.openalex_id) = lower('A' || SUBSTR(authors.openalex_id, 2)) "
                    "  AND twin.id != authors.id"
                    ")"
                ).rowcount
                total = sum(author_heals.values())
                if total:
                    logger.info(
                        "authors.openalex_id heal: %s (total %d rows touched)",
                        author_heals,
                        total,
                    )
            except Exception:
                logger.debug("authors.openalex_id heal skipped during init", exc_info=True)

            # Blank-identifier heal: moved to the pre-index block (see
            # `_heal_papers_blank_identifiers`). Kept empty here for
            # layout stability.

            # Signal Lab consolidation heal (2026-04-24): unify legacy
            # mode_breakdown keys onto the consolidated surface (swipe /
            # authors / topics / tier_sort / feed / library). Stamps
            # `context.mode` on `(none)` rows and re-labels stale mode
            # values from retired modes so the UI breakdown is legible.
            try:
                mode_heals = 0
                # (1) Stamp missing `context.mode` based on event_type.
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
                    mode_heals += conn.execute(
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
                # (2) Rename legacy mode values to the consolidated surface.
                legacy_mode_rename = {
                    "triage": "swipe",        # absorbed by Swipe (count>=2)
                    "author_duel": "authors",  # renamed for clarity
                    "method_match": "swipe",   # retired; counted as swipe
                    "source_sprint": "tier_sort",
                    "abstract_highlight": "swipe",
                }
                for old, new in legacy_mode_rename.items():
                    mode_heals += conn.execute(
                        """
                        UPDATE feedback_events
                        SET context_json = json_set(context_json, '$.mode', ?)
                        WHERE json_extract(context_json, '$.mode') = ?
                        """,
                        (new, old),
                    ).rowcount
                if mode_heals:
                    logger.info(
                        "feedback_events.mode heal: consolidated %d rows to the 3-mode surface",
                        mode_heals,
                    )
            except Exception:
                logger.debug("feedback_events.mode heal skipped during init", exc_info=True)

            # One-shot heal for drift between authors.author_type='followed' and
            # followed_authors. Legacy import pipelines used to stamp rows as
            # 'followed' without inserting into the followed_authors table,
            # leaving ~1k phantom-followed authors that have no Feed monitor and
            # can't be unfollowed from the UI. The canonical source of truth is
            # followed_authors — rows outside it get demoted to 'background'.
            # Also mirror feed_monitors one last time so any followed_authors
            # row that lacks a monitor picks one up.
            try:
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
                        "follow-state heal: demoted %d authors with author_type='followed' "
                        "but missing from followed_authors",
                        demoted,
                    )
                from alma.application.feed_monitors import sync_author_monitors

                sync_author_monitors(conn)
            except Exception:
                logger.debug("follow-state heal skipped during init", exc_info=True)

            # D-AUDIT-6 heal: recommendations.paper_id foreign-key drift.
            # The schema declares
            #   paper_id TEXT NOT NULL REFERENCES papers(id) ON DELETE CASCADE
            # and every write connection runs PRAGMA foreign_keys=ON, but a
            # live probe found 151 / 201 rec rows whose paper_id was not in
            # papers.id. The drift is explained by refresh pipelines writing
            # rec rows from a stale connection or with foreign_keys temporarily
            # off during bulk inserts (sqlite silently accepts the row; ON
            # DELETE CASCADE never fires because the paper was never committed
            # under the FK-enabled connection). This heal deletes orphans and
            # also dedupes any rows that share the same
            # (lens_id, paper_id, suggestion_set_id) triple — refresh cycles
            # inserted duplicates when the same paper surfaced repeatedly. A
            # subsequent UNIQUE index blocks the duplicate path going forward.
            try:
                # Inline table-check (helpers.table_exists would add a
                # circular import — deps.py is imported by helpers.py).
                rec_table = conn.execute(
                    "SELECT name FROM sqlite_master WHERE type='table' AND name='recommendations'"
                ).fetchone()
                if rec_table:
                    orphans = conn.execute(
                        "DELETE FROM recommendations "
                        "WHERE paper_id NOT IN (SELECT id FROM papers)"
                    ).rowcount
                    if orphans:
                        logger.info(
                            "recommendations heal: deleted %d orphan rows "
                            "with paper_id not in papers",
                            orphans,
                        )
                    # Dedupe by (lens_id, paper_id, COALESCE(suggestion_set_id,''))
                    # keeping the row with the most recent created_at. NULL
                    # suggestion_set_id gets treated as '' for the grouping so
                    # pre-lens recommendations don't all collapse.
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
                        logger.info(
                            "recommendations heal: removed %d duplicate rows "
                            "(same lens_id, paper_id, suggestion_set_id)",
                            dupes,
                        )
                    # Add the unique index after dedupe. ``IF NOT EXISTS`` keeps
                    # repeated init_db_schema calls cheap.
                    conn.execute(
                        "CREATE UNIQUE INDEX IF NOT EXISTS "
                        "idx_recs_lens_paper_set_unique "
                        "ON recommendations(lens_id, paper_id, suggestion_set_id)"
                    )
            except Exception:
                logger.debug("recommendations heal skipped during init", exc_info=True)

            conn.commit()
            _schema_initialized = True
            _schema_initialized_path = db
            logger.info("Database schema initialised (WAL mode, %s)", db)

        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()


# ============================================================================
# Database Dependencies (lightweight per-request provider)
# ============================================================================

def open_db_connection() -> sqlite3.Connection:
    """Open one configured SQLite connection for app code."""
    conn = sqlite3.connect(
        _db_path(),
        check_same_thread=False,
        timeout=30.0,
    )
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA busy_timeout=30000")
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


def get_db() -> Generator[sqlite3.Connection, None, None]:
    """Provide a database connection for a single request.

    Declared as a sync generator so FastAPI dispatches it through the anyio
    threadpool instead of the event loop. Every request hits this dependency,
    and the sync SQLite open / commit / close calls inside can block under
    writer-lock contention — running them on the event loop starves concurrent
    requests. The sync-def form is the forward-looking contract after the
    async-to-sync route conversion (see ``tasks/lessons.md`` — "Async route
    handlers must not call sync code directly").

    Schema must already be initialised via ``init_db_schema()`` at startup.
    This generator does **no** DDL work -- it only opens a connection, sets
    pragmas needed per-connection, yields it, and cleans up.
    """
    conn = open_db_connection()
    try:
        yield conn
        conn.commit()
    except Exception as e:
        conn.rollback()
        logger.error("Database error: %s", e)
        raise
    finally:
        conn.close()


# ============================================================================
# Plugin Registry Dependency
# ============================================================================

def get_plugin_registry() -> PluginRegistry:
    """Get the global plugin registry."""
    return get_global_registry()


# ============================================================================
# Authentication Dependencies
# ============================================================================

def verify_api_key(
    x_api_key: Optional[str] = Header(None),
    credentials: Optional[HTTPAuthorizationCredentials] = Depends(security)
) -> bool:
    """Verify API key from header or bearer token."""
    if API_KEY is None:
        return True

    if x_api_key and x_api_key == API_KEY:
        return True

    if credentials and credentials.credentials == API_KEY:
        return True

    raise HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Invalid or missing API key",
        headers={"WWW-Authenticate": "Bearer"},
    )


def get_current_user(authenticated: bool = Depends(verify_api_key)) -> dict:
    """Get the current authenticated user."""
    return {
        "username": "api_user",
        "authenticated": authenticated
    }


# ============================================================================
# Optional Dependencies for Testing
# ============================================================================

def get_test_mode() -> bool:
    """Check if running in test mode."""
    return os.getenv("TEST_MODE", "false").lower() == "true"
