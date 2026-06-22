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
from alma.core.migrations import apply_pending_migrations, stamp_schema_version
from alma.discovery.defaults import DISCOVERY_SETTINGS_DEFAULTS

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
    source installs default to the repo-local ``.venv`` documented in
    the install guide, so Settings points at the same environment users
    create for torch / transformers / adapters.
    """
    docker_venv = "/opt/venv"
    if os.path.isfile(os.path.join(docker_venv, "bin", "python")):
        return ("venv", docker_venv)
    repo_root = Path(__file__).resolve().parents[3]
    return ("venv", str(repo_root / ".venv"))


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


def _safe_execute(conn: sqlite3.Connection, sql: str) -> None:
    try:
        conn.execute(sql)
    except sqlite3.OperationalError:
        logger.debug("Skipping schema statement: %s", sql, exc_info=True)


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

            # Versioned migrations bring any pre-existing older DB to the
            # current shape BEFORE the bootstrap DDL below asserts indexes
            # (legacy identifier heals must precede the partial UNIQUE
            # indexes). Fresh DBs skip this entirely — the bootstrap DDL
            # creates the current shape and `stamp_schema_version` pins
            # PRAGMA user_version at the end. Every schema mutation lives
            # in alma.core.migrations; the DDL below is current-shape only.
            apply_pending_migrations(conn)

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
                    -- Semantic Scholar identifiers
                    semantic_scholar_id TEXT,
                    semantic_scholar_corpus_id TEXT,
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

                    -- Reading lifecycle (D2: orthogonal to membership)
                    reading_status TEXT DEFAULT NULL,

                    -- S2 enrichments: tldr is a 1-2 sentence AI summary
                    -- (dense in CS + biomedicine); influential_citation_count
                    -- supplements raw cited_by_count in citation_quality.
                    tldr TEXT,
                    influential_citation_count INTEGER DEFAULT 0,

                    -- Preprint↔journal dedup (2026-04-24): the preprint row
                    -- points at its published journal twin and is filtered
                    -- out of Library + Discovery lists. NULL = canonical.
                    -- See alma.application.preprint_dedup.
                    canonical_paper_id TEXT,
                    preprint_source TEXT, -- arxiv / biorxiv / psyrxiv / osf / chemrxiv

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

                    -- Identity resolution (+ Phase D hierarchical-resolver
                    -- columns, 2026-04-24: method / confidence / evidence)
                    id_resolution_status TEXT,
                    id_resolution_reason TEXT,
                    id_resolution_updated_at TEXT,
                    id_resolution_method TEXT,
                    id_resolution_confidence REAL,
                    id_resolution_evidence TEXT,
                    semantic_scholar_id TEXT,

                    -- Soft-removal lifecycle (2026-04-26) — mirrors
                    -- papers.status (D3): 'removed' rows stay readable as
                    -- a negative signal but leave bulk refresh + the
                    -- canonical author list.
                    status TEXT DEFAULT 'active',

                    -- Stable dedup identity; values recomputed by
                    -- library/deduplication.ensure_stable_ids.
                    author_uid TEXT
                )"""
            )

            # `is_owner`: onboarding marks the user's own author profile as
            # the "owner". Single-user app, so the partial unique index
            # enforces at most one owner row at the DB level. Used by
            # /onboarding for has_owner detection and the "this is you" badge.
            conn.execute(
                """CREATE TABLE IF NOT EXISTS followed_authors (
                    author_id TEXT PRIMARY KEY,
                    followed_at TEXT NOT NULL,
                    notify_new_papers INTEGER DEFAULT 1,
                    is_owner INTEGER NOT NULL DEFAULT 0
                )"""
            )
            _safe_execute(
                conn,
                "CREATE UNIQUE INDEX IF NOT EXISTS idx_followed_authors_one_owner "
                "ON followed_authors(is_owner) WHERE is_owner = 1",
            )

            conn.execute(
                """CREATE TABLE IF NOT EXISTS author_enrichment_status (
                    author_id TEXT NOT NULL REFERENCES authors(id) ON DELETE CASCADE,
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
                    PRIMARY KEY (author_id, source, purpose)
                )"""
            )
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_author_enrichment_status_lookup "
                "ON author_enrichment_status(source, purpose, lookup_key, fields_key, status)"
            )
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_author_enrichment_status_retry "
                "ON author_enrichment_status(source, purpose, status, next_retry_at)"
            )
            conn.execute(
                """CREATE TABLE IF NOT EXISTS author_affiliation_evidence (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    author_id TEXT NOT NULL REFERENCES authors(id) ON DELETE CASCADE,
                    source TEXT NOT NULL,
                    institution_openalex_id TEXT,
                    institution_ror TEXT,
                    institution_name TEXT NOT NULL,
                    role TEXT,
                    start_date TEXT NOT NULL DEFAULT '',
                    end_date TEXT,
                    is_current INTEGER DEFAULT 0,
                    evidence_url TEXT,
                    confidence REAL,
                    observed_at TEXT NOT NULL,
                    UNIQUE (author_id, source, institution_name, role, start_date)
                )"""
            )
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_author_affiliation_evidence_author "
                "ON author_affiliation_evidence(author_id, is_current DESC, observed_at DESC)"
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
                # Provenance columns (source_* / branch_*) — keep in sync
                # with `_derive_recommendation_provenance` in
                # application/discovery (D-10); the legacy add-column path
                # lives in core/migrations.
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
                    explanation TEXT,
                    source_type TEXT,
                    source_api TEXT,
                    source_key TEXT,
                    branch_id TEXT,
                    branch_label TEXT,
                    branch_mode TEXT
                )"""
            )
            for idx_sql in [
                "CREATE INDEX IF NOT EXISTS idx_recs_set ON recommendations(suggestion_set_id)",
                "CREATE INDEX IF NOT EXISTS idx_recs_lens ON recommendations(lens_id)",
                "CREATE INDEX IF NOT EXISTS idx_recs_paper ON recommendations(paper_id)",
                "CREATE INDEX IF NOT EXISTS idx_recs_action ON recommendations(user_action)",
                # Diagnostics aggregations: branch_quality groups by
                # (branch_id, source_type) to compute the source_mix in a
                # single query instead of N+1 per-branch sub-selects, and
                # source_quality groups by (source_type, source_api).
                "CREATE INDEX IF NOT EXISTS idx_recs_branch_source ON recommendations(branch_id, source_type)",
                "CREATE INDEX IF NOT EXISTS idx_recs_source_api ON recommendations(source_type, source_api)",
                # D-AUDIT-6: blocks duplicate (lens, paper, set) rows going
                # forward; legacy duplicates are removed by the
                # recommendations_hygiene migration before this asserts.
                "CREATE UNIQUE INDEX IF NOT EXISTS idx_recs_lens_paper_set_unique "
                "ON recommendations(lens_id, paper_id, suggestion_set_id)",
            ]:
                conn.execute(idx_sql)

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
            # The PK (paper_id, openalex_id) covers paper-side lookups, but the
            # author-corpus joins key on openalex_id ALONE and case-folded:
            # get_followed_author_backfill_status and the author-suggestion
            # candidate fan-out both join
            #   papers ⋈ publication_authors ⋈ authors
            #     ON lower(a.openalex_id) = lower(pa.openalex_id)
            # openalex_id is the trailing PK column (not a usable left-prefix)
            # and the lower() wrap defeats the autoindex anyway, so without this
            # expression index every author triggers a full scan of all
            # publication_authors rows (N+1). On a 7.2k-paper / 33k-pa corpus
            # that made GET /authors ~1.3s and GET /authors/suggestions ~3.3s;
            # this single index drops them to ~120ms / ~0.8s.
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_pubauthors_oid_lower "
                "ON publication_authors(lower(openalex_id))"
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
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_publication_embeddings_model ON publication_embeddings(model)"
            )
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_publication_embeddings_model_source "
                "ON publication_embeddings(model, source)"
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
            # When a paper's identifiers are rewritten (typically by a
            # corpus rehydration step that updates `doi` /
            # `semantic_scholar_id` from a fresh OpenAlex/S2/Crossref
            # response), drop any terminal `publication_embedding_fetch_status`
            # rows so the next S2 vector sweep retries the paper. The
            # terminal states (`unmatched`, `missing_vector`,
            # `lookup_error`, `bad_local_doi`) were tied to identifiers
            # that no longer apply. Trigger fires only on actual changes
            # — a rewrite to the same value is a no-op. Phase 2 of
            # `tasks/13_END_TO_END_HYDRATION_VECTOR_CHAIN.md`.
            conn.execute(
                """
                CREATE TRIGGER IF NOT EXISTS papers_clear_fetch_status_on_id_change
                AFTER UPDATE OF doi, semantic_scholar_id ON papers
                WHEN (
                    COALESCE(NULLIF(TRIM(NEW.doi), ''), '') !=
                        COALESCE(NULLIF(TRIM(OLD.doi), ''), '')
                    OR COALESCE(NULLIF(TRIM(NEW.semantic_scholar_id), ''), '') !=
                        COALESCE(NULLIF(TRIM(OLD.semantic_scholar_id), ''), '')
                )
                BEGIN
                    DELETE FROM publication_embedding_fetch_status
                    WHERE paper_id = NEW.id
                      AND status IN (
                          'unmatched',
                          'missing_vector',
                          'lookup_error',
                          'bad_local_doi'
                      );
                END
                """
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
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_paper_enrichment_status_lookup "
                "ON paper_enrichment_status(source, purpose, lookup_key, fields_key, status)"
            )
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_paper_enrichment_status_retry "
                "ON paper_enrichment_status(source, purpose, status, next_retry_at)"
            )

            conn.execute(
                # Keyed by (paper_id, scope) so a Library layout and a Corpus
                # layout coexist per paper (I-1). Before this, a Corpus rebuild
                # (corpus ⊇ library) overwrote Library cluster assignments and a
                # Library GET served corpus-space positions/clusters.
                """CREATE TABLE IF NOT EXISTS publication_clusters (
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
            # Source installs used to inherit Docker's `/opt/venv`
            # default when settings were created in a container, then
            # later run from the clone. If `/opt/venv` is not present,
            # repoint that stale default to the source-install default
            # (`<repo>/.venv`) without disturbing explicit user choices.
            if (
                _default_env_path != "/opt/venv"
                and not os.path.isfile("/opt/venv/bin/python")
            ):
                conn.execute(
                    """
                    UPDATE discovery_settings
                    SET value = ?
                    WHERE key = 'ai.python_env_path'
                      AND value = '/opt/venv'
                    """,
                    (_default_env_path,),
                )
                conn.execute(
                    """
                    UPDATE discovery_settings
                    SET value = ?
                    WHERE key = 'ai.python_env_type'
                      AND EXISTS (
                          SELECT 1 FROM discovery_settings p
                          WHERE p.key = 'ai.python_env_path'
                            AND p.value = ?
                      )
                    """,
                    (_default_env_type, _default_env_path),
                )
            conn.execute(
                """CREATE TABLE IF NOT EXISTS graph_cache (
                    graph_type TEXT PRIMARY KEY,
                    data TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                )"""
            )

            # Generalised cache for any expensive read view (Insights,
            # graphs, …). Lookup is keyed by the view name; the
            # `fingerprint` is a hash of the view's input state at the
            # time `payload` was computed. A GET serves `payload`
            # immediately; if the current fingerprint differs, the
            # route enqueues a background rebuild and returns the stale
            # payload so the user never blocks on recomputation. See
            # `alma.application.materialized_views` for the registry.
            conn.execute(
                """CREATE TABLE IF NOT EXISTS materialized_views (
                    view_key       TEXT PRIMARY KEY,
                    fingerprint    TEXT NOT NULL,
                    payload        TEXT NOT NULL,
                    computed_at    TEXT NOT NULL,
                    compute_ms     INTEGER,
                    build_status   TEXT NOT NULL DEFAULT 'ok',
                    build_error    TEXT,
                    rebuild_job_id TEXT
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

            # Forward-only maintenance config migration. This runs at startup,
            # never from a GET: legacy enabled/daily_cap/batch_size keys are
            # split into validated auto/manual/request controls and destructive
            # auto-enable intent is forcibly disabled.
            try:
                from alma.services.maintenance import migrate_maintenance_config

                corrections = migrate_maintenance_config(conn)
                if corrections:
                    logger.warning(
                        "Corrected %d unsafe/invalid maintenance setting(s): %s",
                        len(corrections),
                        corrections,
                    )
            except Exception:
                logger.exception("maintenance config migration failed")
                raise

            # Pin PRAGMA user_version for fresh databases — the bootstrap
            # DDL above just created the current shape, so the legacy
            # migrations must never replay against it. No-op on DBs that
            # `apply_pending_migrations` already brought to HEAD.
            stamp_schema_version(conn)

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
    """Open one configured SQLite connection for app code.

    This is the ONE connection contract for every foreground request and
    background runner. SQLite is single-writer; these pragmas are what keep
    concurrent writers from failing with "database is locked":

    - ``journal_mode=WAL``  — readers never block the single writer. It is
      persisted in the DB header (set once at ``init_db_schema``), but we
      re-assert and *read it back* on every connection so a filesystem that
      silently refuses WAL (some network/overlay mounts) surfaces loudly
      instead of degrading to rollback-journal (which serialises readers
      against writers and is the classic Docker lock trap).
    - ``busy_timeout=30000`` — wait up to 30s for the writer instead of
      erroring immediately. Belt-and-suspenders foreground retry lives in
      ``alma.core.db_retry`` for the rare lock that outlives even this.
    - ``synchronous=NORMAL`` — safe under WAL and cuts fsyncs sharply, so
      each write transaction is held for less wall-clock time.
    """
    conn = sqlite3.connect(
        _db_path(),
        check_same_thread=False,
        timeout=30.0,
    )
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA busy_timeout=30000")
    mode = conn.execute("PRAGMA journal_mode=WAL").fetchone()
    resolved = (mode[0] if mode else "") or ""
    if resolved.lower() != "wal":
        # The DB file's filesystem refused WAL. Everything still works, but
        # writer/reader contention will be far worse — make it visible.
        logger.warning(
            "SQLite journal_mode is %r, not WAL — expect heavier lock "
            "contention. Check the data volume's filesystem.",
            resolved,
        )
    conn.execute("PRAGMA synchronous=NORMAL")
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
