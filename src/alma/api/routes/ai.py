"""AI status and configuration endpoints."""

import sqlite3
import uuid
from datetime import datetime
from pathlib import Path
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel, Field

from alma.api.deps import get_db, get_current_user
from alma.api.helpers import table_exists
from alma.ai.embedding_sources import (
    EMBEDDING_SOURCE_LOCAL,
    EMBEDDING_SOURCE_SEMANTIC_SCHOLAR,
    EMBEDDING_SOURCE_UNKNOWN,
)
from alma.ai.environment import (
    activate_dependency_environment,
    check_packages_in_environment,
    resolve_dependency_environment,
)
from alma.core.secrets import (
    SECRET_OPENAI_API_KEY,
    delete_secret,
    set_secret,
)

router = APIRouter(
    dependencies=[Depends(get_current_user)],
    responses={401: {"description": "Unauthorized"}},
)


# ---------------------------------------------------------------------------
# Request / Response models
# ---------------------------------------------------------------------------


class AIConfigureRequest(BaseModel):
    """Request body for POST /configure."""

    provider: Optional[str] = Field(
        None, description="Embedding provider name (none, local, openai)"
    )
    openai_api_key: Optional[str] = Field(None, description="OpenAI API key")
    local_model: Optional[str] = Field(
        None, description="Local embedding model key (specter2-base)"
    )
    python_env_type: Optional[str] = Field(
        None,
        description="Dependency environment type (system, venv, uv, conda, miniconda, miniforge)",
    )
    python_env_path: Optional[str] = Field(
        None,
        description="Path to dependency environment folder or python executable",
    )


_VALID_EMBEDDING_PROVIDERS = {"none", "local", "openai"}


class ComputeEmbeddingsResponse(BaseModel):
    """Response body for POST /compute-embeddings."""

    job_id: str
    operation_id: Optional[str] = None
    status: Optional[str] = None
    activity_url: Optional[str] = None
    operation_key: Optional[str] = None
    scope: Optional[str] = None
    message: str


class DeleteInactiveEmbeddingsResponse(BaseModel):
    """Response body for DELETE /embeddings/inactive."""

    status: str
    active_model: str
    deleted: int
    message: str


# ---------------------------------------------------------------------------
# Dependency helpers
# ---------------------------------------------------------------------------

# Packages to check for the /dependencies endpoint.
# Maps display name -> importable module name.
_DEPENDENCY_PACKAGES: dict[str, str] = {
    "requests": "requests",
    "numpy": "numpy",
    "scikit-learn": "sklearn",
    "umap-learn": "umap",
    "hdbscan": "hdbscan",
    "transformers": "transformers",
    "torch": "torch",
    "adapters": "adapters",
}

_DIST_NAME_MAP = {
    "sklearn": "scikit-learn",
    "umap": "umap-learn",
}


def _read_setting(conn: sqlite3.Connection, key: str, default: str = "") -> str:
    """Read a single value from the discovery_settings table."""
    try:
        row = conn.execute("SELECT value FROM discovery_settings WHERE key = ?", (key,)).fetchone()
        if row is not None:
            return row["value"] if isinstance(row, sqlite3.Row) else row[0]
    except sqlite3.OperationalError:
        pass
    return default


def _write_setting(conn: sqlite3.Connection, key: str, value: str) -> None:
    """Write a single value into the discovery_settings table."""
    conn.execute(
        "INSERT OR REPLACE INTO discovery_settings (key, value, updated_at) VALUES (?, ?, ?)",
        (key, value, datetime.utcnow().isoformat()),
    )


def _build_dependency_setup_suggestions(
    env_type: str,
    dependencies: dict[str, dict],
    env_valid: bool,
    provider_name: str,
) -> list[str]:
    """Return actionable setup steps for missing dependencies."""
    missing = [
        pkg
        for pkg, info in dependencies.items()
        if not isinstance(info, dict) or not bool(info.get("installed"))
    ]
    if not missing:
        return ["All AI dependencies are available in the active environment."]

    suggestions: list[str] = []
    if not env_valid and env_type != "system":
        suggestions.append(
            "Fix the environment path/type first so checks stop falling back to the server environment."
        )

    provider = (provider_name or "none").strip().lower()
    # Provider-specific install guidance (more reliable than piecemeal installs).
    if provider == "local":
        provider_install_line = (
            "pip install \"numpy==1.26.4\" \"scipy==1.11.4\" \"scikit-learn==1.4.2\" "
            "\"transformers~=4.51.3\" \"adapters>=1.0.0\" "
            "\"torch==2.4.1\" \"umap-learn\" \"hdbscan\" requests"
        )
        suggestions.append(
            "Selected provider is 'local': install the full SPECTER2 stack in ONE command to avoid binary mismatches."
        )
    elif provider == "openai":
        # `openai` package is no longer bundled — embeddings still work
        # via the OpenAI HTTP API; the Python SDK was dropped as part of
        # the LLM exit (tasks/01_LLM_PRODUCTION_EXIT.md).
        provider_install_line = "pip install requests numpy"
    else:
        missing_pkg_list = " ".join(sorted(missing))
        provider_install_line = f"pip install {missing_pkg_list}"

    if env_type in {"conda", "miniconda", "miniforge"}:
        suggestions.extend(
            [
                "Create a clean env: conda create -n scholarbot-ai python=3.11 -y",
                "Activate it: conda activate scholarbot-ai",
                f"Install packages: {provider_install_line}",
                "Set this env folder in Settings (e.g., <conda_base>/envs/scholarbot-ai).",
                "Restart backend after install so Python reloads compiled dependencies from this env.",
            ]
        )
    elif env_type == "uv":
        suggestions.extend(
            [
                "Create a clean env: uv venv .venv",
                "Activate it: source .venv/bin/activate",
                f"Install packages: uv {provider_install_line}",
                "Set the .venv folder in Settings.",
                "Restart backend after install so Python reloads compiled dependencies from this env.",
            ]
        )
    else:
        suggestions.extend(
            [
                "Create a clean env: python -m venv .venv",
                "Activate it: source .venv/bin/activate",
                f"Install packages: {provider_install_line}",
                "Set the .venv folder in Settings.",
                "Restart backend after install so Python reloads compiled dependencies from this env.",
            ]
        )
    return suggestions


def _feature(
    *,
    feature_id: str,
    group: str,
    label: str,
    status: str,
    dependency: str,
    detail: str,
    action: Optional[str] = None,
) -> dict:
    return {
        "id": feature_id,
        "group": group,
        "label": label,
        "status": status,
        "dependency": dependency,
        "detail": detail,
        "action": action,
    }


def _build_ai_feature_registry(
    *,
    embedding_count: int,
    up_to_date_embeddings: int,
    downloaded_vector_count: int,
    total_papers: int,
    active_provider_available: bool,
) -> dict:
    """Build one cross-cutting AI feature map for Settings and Insights."""
    embedding_ready = embedding_count > 0
    provider_or_vectors = active_provider_available or embedding_ready
    graph_ready = embedding_count >= 5

    items = [
        _feature(
            feature_id="downloaded_s2_vectors",
            group="embeddings",
            label="Downloaded S2/SPECTER2 vectors",
            status="ready" if downloaded_vector_count > 0 else "empty",
            dependency="semantic_scholar",
            detail=(
                f"{downloaded_vector_count}/{total_papers} papers have API-sourced SPECTER2 vectors."
                if total_papers
                else "No papers available for vector coverage."
            ),
            action="Fetch S2 Vectors" if downloaded_vector_count == 0 else None,
        ),
        _feature(
            feature_id="active_embedding_model",
            group="embeddings",
            label="Active embedding model",
            status="ready" if up_to_date_embeddings > 0 else ("available" if active_provider_available else "empty"),
            dependency="embeddings",
            detail=(
                f"{up_to_date_embeddings}/{total_papers} papers are ready for the active model."
                if total_papers
                else "No papers available for active-model coverage."
            ),
            action="Compute Missing" if active_provider_available and up_to_date_embeddings < total_papers else None,
        ),
        _feature(
            feature_id="discovery_vector_channel",
            group="discovery",
            label="Discovery vector channel",
            status="ready" if embedding_ready else ("available" if active_provider_available else "fallback"),
            dependency="embeddings",
            detail=(
                "Uses cached vectors for branch/lens candidate retrieval."
                if embedding_ready
                else "Discovery still runs with lexical, graph, and external channels."
            ),
            action="Fetch S2 Vectors" if not provider_or_vectors else None,
        ),
        _feature(
            feature_id="semantic_ranking",
            group="discovery",
            label="Semantic ranking",
            status="ready" if embedding_ready else "fallback",
            dependency="embeddings",
            detail=(
                "Ranking can use active-model vectors and accumulated feedback-learning signals."
                if embedding_ready
                else "Ranking falls back to lexical/topic/author/citation signals."
            ),
            action="Compute Missing" if active_provider_available and not embedding_ready else None,
        ),
        _feature(
            feature_id="semantic_search",
            group="library",
            label="Semantic search",
            status="ready" if active_provider_available and embedding_ready else ("available" if active_provider_available else "fallback"),
            dependency="embedding_provider",
            detail=(
                "Library search can embed queries and compare against active-model vectors."
                if active_provider_available and embedding_ready
                else "Search remains keyword-first unless a live provider can embed the query."
            ),
            action="Configure Embedding Provider" if not active_provider_available else None,
        ),
        _feature(
            feature_id="graph_projection",
            group="insights",
            label="Graph projection and clustering",
            status="ready" if graph_ready else "fallback",
            dependency="embeddings",
            detail=(
                "Enough vectors exist for embedding-backed maps."
                if graph_ready
                else "Graph views fall back or need at least 5 active-model vectors."
            ),
            action="Compute Missing" if active_provider_available and not graph_ready else None,
        ),
        _feature(
            feature_id="tag_suggestions",
            group="metadata",
            label="Tag suggestions",
            status="ready" if embedding_ready else "fallback",
            dependency="embeddings",
            detail=(
                "Tag suggestions can propagate labels through embedding neighbors."
                if embedding_ready
                else "Tag suggestions use topic and TF-IDF fallback signals."
            ),
        ),
        _feature(
            feature_id="feedback_learning",
            group="feedback",
            label="Feedback learning",
            status="ready",
            dependency="none",
            detail="Saved-paper ratings, discovery actions, feed actions, and interaction history feed Discovery scoring.",
        ),
    ]

    group_labels = {
        "embeddings": "Embeddings",
        "discovery": "Discovery",
        "library": "Library",
        "insights": "Insights",
        "metadata": "Metadata",
        "feedback": "Feedback Learning",
    }
    groups = [
        {
            "id": group_id,
            "label": label,
            "items": [item for item in items if item["group"] == group_id],
        }
        for group_id, label in group_labels.items()
        if any(item["group"] == group_id for item in items)
    ]
    summary = {
        "ready": sum(1 for item in items if item["status"] == "ready"),
        "fallback": sum(1 for item in items if item["status"] == "fallback"),
        "blocked": sum(1 for item in items if item["status"] == "blocked"),
        "empty": sum(1 for item in items if item["status"] == "empty"),
        "available": sum(1 for item in items if item["status"] == "available"),
        "off": sum(1 for item in items if item["status"] == "off"),
        "total": len(items),
    }
    return {"summary": summary, "groups": groups, "items": items}


def _dependency_ready(dependencies: dict[str, dict], name: str) -> bool:
    info = dependencies.get(name) or {}
    if "runtime_importable" in info:
        return bool(info.get("runtime_importable"))
    return bool(info.get("installed"))


def _build_s2_backfill_status(conn: sqlite3.Connection, model: str) -> dict:
    """Return S2 vector backfill eligibility counts for the corpus."""
    fetch_status_available = table_exists(conn, "publication_embedding_fetch_status")
    try:
        if fetch_status_available:
            row = conn.execute(
                """
                SELECT
                    COUNT(*) AS total_missing,
                    SUM(
                        CASE
                            WHEN (
                                COALESCE(NULLIF(TRIM(p.semantic_scholar_id), ''), '') != ''
                                OR COALESCE(NULLIF(TRIM(p.doi), ''), '') != ''
                            )
                            THEN 1 ELSE 0
                        END
                    ) AS eligible_missing,
                    SUM(
                        CASE
                            WHEN (
                                COALESCE(NULLIF(TRIM(p.semantic_scholar_id), ''), '') = ''
                                AND COALESCE(NULLIF(TRIM(p.doi), ''), '') = ''
                            )
                            THEN 1 ELSE 0
                        END
                    ) AS ineligible_missing
                FROM papers p
                LEFT JOIN publication_embedding_fetch_status fs
                  ON fs.paper_id = p.id
                 AND fs.model = ?
                 AND fs.source = 'semantic_scholar'
                 AND fs.lookup_key = lower(trim(COALESCE(p.semantic_scholar_id, ''))) || '|' || lower(trim(COALESCE(p.doi, '')))
                WHERE NOT EXISTS (
                    SELECT 1 FROM publication_embeddings pe
                    WHERE pe.paper_id = p.id AND pe.model = ?
                )
                AND COALESCE(fs.status, '') NOT IN ('unmatched', 'missing_vector', 'lookup_error')
                """,
                (model, model),
            ).fetchone()
        else:
            row = conn.execute(
                """
                SELECT
                    COUNT(*) AS total_missing,
                    SUM(
                        CASE
                            WHEN (
                                COALESCE(NULLIF(TRIM(p.semantic_scholar_id), ''), '') != ''
                                OR COALESCE(NULLIF(TRIM(p.doi), ''), '') != ''
                            )
                            THEN 1 ELSE 0
                        END
                    ) AS eligible_missing,
                    SUM(
                        CASE
                            WHEN (
                                COALESCE(NULLIF(TRIM(p.semantic_scholar_id), ''), '') = ''
                                AND COALESCE(NULLIF(TRIM(p.doi), ''), '') = ''
                            )
                            THEN 1 ELSE 0
                        END
                    ) AS ineligible_missing
                FROM papers p
                WHERE NOT EXISTS (
                    SELECT 1 FROM publication_embeddings pe
                    WHERE pe.paper_id = p.id AND pe.model = ?
                )
                """,
                (model,),
            ).fetchone()
    except sqlite3.OperationalError:
        row = None

    try:
        if not fetch_status_available:
            raise sqlite3.OperationalError("publication_embedding_fetch_status missing")
        terminal_row = conn.execute(
            """
            SELECT
                SUM(CASE WHEN fs.status = 'unmatched' THEN 1 ELSE 0 END) AS unmatched,
                SUM(CASE WHEN fs.status = 'missing_vector' THEN 1 ELSE 0 END) AS missing_vector,
                SUM(CASE WHEN fs.status = 'lookup_error' THEN 1 ELSE 0 END) AS lookup_error,
                SUM(CASE WHEN fs.status = 'error' THEN 1 ELSE 0 END) AS error
            FROM papers p
            JOIN publication_embedding_fetch_status fs
              ON fs.paper_id = p.id
             AND fs.model = ?
             AND fs.source = 'semantic_scholar'
             AND fs.lookup_key = lower(trim(COALESCE(p.semantic_scholar_id, ''))) || '|' || lower(trim(COALESCE(p.doi, '')))
            WHERE NOT EXISTS (
                SELECT 1 FROM publication_embeddings pe
                WHERE pe.paper_id = p.id AND pe.model = ?
            )
            """,
            (model, model),
        ).fetchone()
    except sqlite3.OperationalError:
        terminal_row = None

    try:
        if fetch_status_available:
            status_rows = conn.execute(
                """
                SELECT
                    COALESCE(NULLIF(p.status, ''), 'unknown') AS status,
                    COUNT(*) AS total_missing,
                    SUM(
                        CASE
                            WHEN (
                                COALESCE(NULLIF(TRIM(p.semantic_scholar_id), ''), '') != ''
                                OR COALESCE(NULLIF(TRIM(p.doi), ''), '') != ''
                            )
                            THEN 1 ELSE 0
                        END
                    ) AS eligible_missing
                FROM papers p
                LEFT JOIN publication_embedding_fetch_status fs
                  ON fs.paper_id = p.id
                 AND fs.model = ?
                 AND fs.source = 'semantic_scholar'
                 AND fs.lookup_key = lower(trim(COALESCE(p.semantic_scholar_id, ''))) || '|' || lower(trim(COALESCE(p.doi, '')))
                WHERE NOT EXISTS (
                    SELECT 1 FROM publication_embeddings pe
                    WHERE pe.paper_id = p.id AND pe.model = ?
                )
                AND COALESCE(fs.status, '') NOT IN ('unmatched', 'missing_vector', 'lookup_error')
                GROUP BY COALESCE(NULLIF(p.status, ''), 'unknown')
                """,
                (model, model),
            ).fetchall()
        else:
            status_rows = conn.execute(
                """
                SELECT
                    COALESCE(NULLIF(p.status, ''), 'unknown') AS status,
                    COUNT(*) AS total_missing,
                    SUM(
                        CASE
                            WHEN (
                                COALESCE(NULLIF(TRIM(p.semantic_scholar_id), ''), '') != ''
                                OR COALESCE(NULLIF(TRIM(p.doi), ''), '') != ''
                            )
                            THEN 1 ELSE 0
                        END
                    ) AS eligible_missing
                FROM papers p
                WHERE NOT EXISTS (
                    SELECT 1 FROM publication_embeddings pe
                    WHERE pe.paper_id = p.id AND pe.model = ?
                )
                GROUP BY COALESCE(NULLIF(p.status, ''), 'unknown')
                """,
                (model,),
            ).fetchall()
        by_status = {
            str(item["status"]): {
                "total_missing": int(item["total_missing"] or 0),
                "eligible_missing": int(item["eligible_missing"] or 0),
                "ineligible_missing": max(
                    0,
                    int(item["total_missing"] or 0) - int(item["eligible_missing"] or 0),
                ),
            }
            for item in status_rows
        }
    except sqlite3.OperationalError:
        by_status = {}

    total_missing = int((row["total_missing"] if row else 0) or 0)
    eligible_missing = int((row["eligible_missing"] if row else 0) or 0)
    ineligible_missing = int((row["ineligible_missing"] if row else 0) or 0)
    terminal_unmatched = int((terminal_row["unmatched"] if terminal_row else 0) or 0)
    terminal_missing_vector = int((terminal_row["missing_vector"] if terminal_row else 0) or 0)
    terminal_lookup_error = int((terminal_row["lookup_error"] if terminal_row else 0) or 0)
    terminal_error = int((terminal_row["error"] if terminal_row else 0) or 0)
    try:
        local_compute_row = conn.execute(
            """
            SELECT COUNT(*) AS c
            FROM papers p
            WHERE NOT EXISTS (
                SELECT 1 FROM publication_embeddings pe
                WHERE pe.paper_id = p.id AND pe.model = ?
            )
            AND COALESCE(NULLIF(TRIM(p.title), ''), '') != ''
            AND COALESCE(NULLIF(TRIM(p.abstract), ''), '') != ''
            """,
            (model,),
        ).fetchone()
    except sqlite3.OperationalError:
        local_compute_row = None
    local_compute_candidates = int((local_compute_row["c"] if local_compute_row else 0) or 0)
    all_missing_without_vector = (
        total_missing + terminal_unmatched + terminal_missing_vector + terminal_lookup_error
    )
    local_compute_blocked_missing_text = max(
        0,
        all_missing_without_vector - local_compute_candidates,
    )
    return {
        "model": model,
        "total_missing": total_missing,
        "eligible_missing": eligible_missing,
        "ineligible_missing": ineligible_missing,
        "terminal_unmatched": terminal_unmatched,
        "terminal_missing_vector": terminal_missing_vector,
        "terminal_lookup_error": terminal_lookup_error,
        "terminal_error": terminal_error,
        "local_compute_candidates": local_compute_candidates,
        "local_compute_blocked_missing_text": local_compute_blocked_missing_text,
        "by_status": by_status,
    }


def _infer_env_type_from_path(env_path: str) -> str:
    """Infer dependency environment type from a configured path."""
    normalized = (env_path or "").strip()
    if not normalized:
        return "system"

    candidate = Path(normalized).expanduser()
    candidate_str = str(candidate).lower()

    # Strong markers first.
    if candidate.is_dir():
        if (candidate / "conda-meta").is_dir():
            return "conda"
        if (candidate / "pyvenv.cfg").is_file():
            return "venv"

    # Heuristic fallback when marker files are unavailable.
    if any(token in candidate_str for token in ("miniforge", "miniconda", "anaconda", "conda")):
        return "conda"

    return "venv"


def _embedding_source_counts(conn: sqlite3.Connection, model: str) -> dict[str, int]:
    try:
        rows = conn.execute(
            """
            SELECT COALESCE(NULLIF(TRIM(source), ''), ?) AS source, COUNT(*) AS vectors
            FROM publication_embeddings
            WHERE model = ?
            GROUP BY COALESCE(NULLIF(TRIM(source), ''), ?)
            """,
            (EMBEDDING_SOURCE_UNKNOWN, model, EMBEDDING_SOURCE_UNKNOWN),
        ).fetchall()
    except sqlite3.OperationalError:
        return {}
    return {
        str(row["source"] or EMBEDDING_SOURCE_UNKNOWN): int(row["vectors"] or 0)
        for row in rows
        if int(row["vectors"] or 0) > 0
    }


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------


@router.get("/status")
def ai_status(
    db: sqlite3.Connection = Depends(get_db),
    _user: dict = Depends(get_current_user),
) -> dict:
    """Return the current AI/ML system status.

    Includes embedding provider info, embedding coverage statistics, and
    dependency availability.
    """
    from alma.ai.providers import DEFAULT_LOCAL_MODEL, get_active_provider, list_available_providers
    from alma.discovery.semantic_scholar import S2_SPECTER2_MODEL

    dependency_env = activate_dependency_environment(db)
    dependencies, dependency_check_warning = check_packages_in_environment(
        _DEPENDENCY_PACKAGES,
        _DIST_NAME_MAP,
        dependency_env,
    )

    # Providers
    providers_info = list_available_providers(db)
    configured_provider = _read_setting(db, "ai.provider", "none").strip().lower() or "none"
    if configured_provider not in _VALID_EMBEDDING_PROVIDERS:
        configured_provider = "none"
    active_provider = get_active_provider(db)

    for info in providers_info:
        info["active"] = info["name"] == configured_provider

    try:
        pub_count = db.execute("SELECT COUNT(*) FROM papers").fetchone()[0]
    except sqlite3.OperationalError:
        pub_count = 0

    configured_embedding_model = _read_setting(db, "embedding_model", S2_SPECTER2_MODEL)
    expected_embedding_model = configured_embedding_model
    if active_provider is not None:
        expected_embedding_model = active_provider.model_name

    try:
        embedding_count = db.execute(
            "SELECT COUNT(*) FROM publication_embeddings WHERE model = ?",
            (expected_embedding_model,),
        ).fetchone()[0]
    except sqlite3.OperationalError:
        embedding_count = 0

    try:
        lifecycle_row = db.execute(
            """
            SELECT
                COUNT(*) AS total_papers,
                SUM(CASE WHEN active_pe.paper_id IS NULL THEN 1 ELSE 0 END) AS missing_embeddings,
                SUM(
                    CASE
                        WHEN active_pe.paper_id IS NULL
                         AND EXISTS (
                            SELECT 1 FROM publication_embeddings other_pe
                            WHERE other_pe.paper_id = p.id AND other_pe.model <> ?
                         )
                        THEN 1 ELSE 0
                    END
                ) AS stale_embeddings,
                SUM(CASE WHEN active_pe.paper_id IS NOT NULL THEN 1 ELSE 0 END) AS up_to_date_embeddings
            FROM papers p
            LEFT JOIN publication_embeddings active_pe
              ON active_pe.paper_id = p.id AND active_pe.model = ?
            """,
            (expected_embedding_model, expected_embedding_model),
        ).fetchone()
    except sqlite3.OperationalError:
        lifecycle_row = None

    total_papers = int((lifecycle_row["total_papers"] if lifecycle_row else pub_count) or pub_count or 0)
    missing_embeddings = int((lifecycle_row["missing_embeddings"] if lifecycle_row else 0) or 0)
    stale_embeddings = int((lifecycle_row["stale_embeddings"] if lifecycle_row else 0) or 0)
    up_to_date_embeddings = int((lifecycle_row["up_to_date_embeddings"] if lifecycle_row else 0) or 0)

    coverage_pct = round(embedding_count / total_papers * 100, 1) if total_papers > 0 else 0.0
    up_to_date_pct = round(up_to_date_embeddings / total_papers * 100, 1) if total_papers > 0 else 0.0

    canonical_source_counts = _embedding_source_counts(db, S2_SPECTER2_MODEL)
    canonical_total = sum(canonical_source_counts.values())
    downloaded_total = int(canonical_source_counts.get(EMBEDDING_SOURCE_SEMANTIC_SCHOLAR, 0) or 0)
    local_total = int(canonical_source_counts.get(EMBEDDING_SOURCE_LOCAL, 0) or 0)
    unknown_total = int(canonical_source_counts.get(EMBEDDING_SOURCE_UNKNOWN, 0) or 0)

    try:
        status_rows = db.execute(
            """
            SELECT
                COALESCE(NULLIF(p.status, ''), 'unknown') AS status,
                COUNT(DISTINCT p.id) AS total,
                COUNT(DISTINCT active_pe.paper_id) AS up_to_date,
                COUNT(DISTINCT canonical_pe.paper_id) AS canonical_total,
                COUNT(DISTINCT CASE WHEN canonical_pe.source = ? THEN canonical_pe.paper_id END) AS downloaded_total,
                COUNT(DISTINCT CASE WHEN canonical_pe.source = ? THEN canonical_pe.paper_id END) AS local_total,
                COUNT(DISTINCT CASE WHEN COALESCE(NULLIF(TRIM(canonical_pe.source), ''), ?) = ? THEN canonical_pe.paper_id END) AS unknown_total
            FROM papers p
            LEFT JOIN publication_embeddings active_pe
              ON active_pe.paper_id = p.id AND active_pe.model = ?
            LEFT JOIN publication_embeddings canonical_pe
              ON canonical_pe.paper_id = p.id AND canonical_pe.model = ?
            GROUP BY COALESCE(NULLIF(p.status, ''), 'unknown')
            """,
            (
                EMBEDDING_SOURCE_SEMANTIC_SCHOLAR,
                EMBEDDING_SOURCE_LOCAL,
                EMBEDDING_SOURCE_UNKNOWN,
                EMBEDDING_SOURCE_UNKNOWN,
                expected_embedding_model,
                S2_SPECTER2_MODEL,
            ),
        ).fetchall()
        coverage_by_status = {
            str(row["status"]): {
                "total": int(row["total"] or 0),
                "up_to_date": int(row["up_to_date"] or 0),
                "missing": max(0, int(row["total"] or 0) - int(row["up_to_date"] or 0)),
                "canonical_total": int(row["canonical_total"] or 0),
                "downloaded_total": int(row["downloaded_total"] or 0),
                "local_total": int(row["local_total"] or 0),
                "unknown_total": int(row["unknown_total"] or 0),
            }
            for row in status_rows
        }
    except sqlite3.OperationalError:
        coverage_by_status = {}

    def _stale_count_for_model(model: str) -> int:
        if total_papers <= 0:
            return 0
        try:
            row = db.execute(
                """
                SELECT COUNT(*) AS c
                FROM papers p
                WHERE NOT EXISTS (
                    SELECT 1 FROM publication_embeddings active_pe
                    WHERE active_pe.paper_id = p.id AND active_pe.model = ?
                )
                  AND EXISTS (
                    SELECT 1 FROM publication_embeddings other_pe
                    WHERE other_pe.paper_id = p.id AND other_pe.model <> ?
                  )
                """,
                (model, model),
            ).fetchone()
            return int((row["c"] if row else 0) or 0)
        except sqlite3.OperationalError:
            return 0

    try:
        model_rows = db.execute(
            """
            SELECT model, COUNT(*) AS vectors, MAX(created_at) AS last_created_at
            FROM publication_embeddings
            GROUP BY model
            ORDER BY vectors DESC, model ASC
            """
        ).fetchall()
        model_source_rows = db.execute(
            """
            SELECT
                model,
                COALESCE(NULLIF(TRIM(source), ''), ?) AS source,
                COUNT(*) AS vectors
            FROM publication_embeddings
            GROUP BY model, COALESCE(NULLIF(TRIM(source), ''), ?)
            ORDER BY model ASC, vectors DESC, source ASC
            """,
            (EMBEDDING_SOURCE_UNKNOWN, EMBEDDING_SOURCE_UNKNOWN),
        ).fetchall()
        model_sources: dict[str, dict[str, int]] = {}
        for row in model_source_rows:
            model = str(row["model"] or "")
            if not model:
                continue
            model_sources.setdefault(model, {})[str(row["source"] or EMBEDDING_SOURCE_UNKNOWN)] = int(
                row["vectors"] or 0
            )
        model_coverage = [
            {
                "model": str(row["model"] or ""),
                "vectors": int(row["vectors"] or 0),
                "coverage_pct": round(int(row["vectors"] or 0) / total_papers * 100, 1) if total_papers > 0 else 0.0,
                "last_created_at": row["last_created_at"],
                "stale": _stale_count_for_model(str(row["model"] or "")),
                "active": str(row["model"] or "") == expected_embedding_model,
                "source": (
                    next(iter(model_sources.get(str(row["model"] or ""), {})), EMBEDDING_SOURCE_UNKNOWN)
                    if len(model_sources.get(str(row["model"] or ""), {})) == 1
                    else "mixed"
                ),
                "sources": model_sources.get(str(row["model"] or ""), {}),
            }
            for row in model_rows
            if str(row["model"] or "").strip()
        ]
    except sqlite3.OperationalError:
        model_coverage = []
    if expected_embedding_model and not any(row["model"] == expected_embedding_model for row in model_coverage):
        model_coverage.append(
            {
                "model": expected_embedding_model,
                "vectors": 0,
                "coverage_pct": 0.0,
                "last_created_at": None,
                "stale": _stale_count_for_model(expected_embedding_model),
                "active": True,
                "source": EMBEDDING_SOURCE_UNKNOWN,
                "sources": {},
            }
        )
    if not any(row["model"] == S2_SPECTER2_MODEL for row in model_coverage):
        model_coverage.append(
            {
                "model": S2_SPECTER2_MODEL,
                "vectors": 0,
                "coverage_pct": 0.0,
                "last_created_at": None,
                "stale": _stale_count_for_model(S2_SPECTER2_MODEL),
                "active": expected_embedding_model == S2_SPECTER2_MODEL,
                "source": EMBEDDING_SOURCE_UNKNOWN,
                "sources": {},
            }
        )

    embeddings_info = {
        "total": embedding_count,
        "coverage_pct": coverage_pct,
        "model": expected_embedding_model,
        "configured_model": configured_embedding_model,
        "up_to_date": up_to_date_embeddings,
        "up_to_date_pct": up_to_date_pct,
        "missing": missing_embeddings,
        "stale": stale_embeddings,
        "canonical_model": S2_SPECTER2_MODEL,
        "canonical_total": canonical_total,
        "canonical_coverage_pct": round(canonical_total / total_papers * 100, 1) if total_papers > 0 else 0.0,
        "downloaded_total": downloaded_total,
        "downloaded_coverage_pct": round(downloaded_total / total_papers * 100, 1) if total_papers > 0 else 0.0,
        "local_total": local_total,
        "local_coverage_pct": round(local_total / total_papers * 100, 1) if total_papers > 0 else 0.0,
        "unknown_total": unknown_total,
        "unknown_coverage_pct": round(unknown_total / total_papers * 100, 1) if total_papers > 0 else 0.0,
        "coverage_scope": "corpus",
        "coverage_by_status": coverage_by_status,
        "models": model_coverage,
        "s2_backfill": _build_s2_backfill_status(db, S2_SPECTER2_MODEL),
    }

    any_embedding_provider_available = any(bool(p.get("available")) for p in providers_info)
    tier1_enabled = configured_provider != "none" or embedding_count > 0
    tier1_ready = active_provider is not None or embedding_count > 0
    features = _build_ai_feature_registry(
        embedding_count=embedding_count,
        up_to_date_embeddings=up_to_date_embeddings,
        downloaded_vector_count=downloaded_total,
        total_papers=total_papers,
        active_provider_available=active_provider is not None,
    )

    return {
        "providers": providers_info,
        "embeddings": embeddings_info,
        "capability_tiers": {
            "tier1_embeddings": {
                "enabled": tier1_enabled,
                "ready": tier1_ready,
                "available": any_embedding_provider_available or embedding_count > 0,
                "active_provider": configured_provider,
                "active_model": expected_embedding_model,
            },
        },
        "features": features,
        "local_model": _read_setting(db, "ai.local_model", DEFAULT_LOCAL_MODEL),
        "dependencies": dependencies,
        "dependency_environment": dependency_env.as_dict(),
        "dependency_check_warning": dependency_check_warning,
        "dependency_setup_suggestions": _build_dependency_setup_suggestions(
            dependency_env.configured_type,
            dependencies,
            dependency_env.valid,
            configured_provider,
        ),
    }


@router.post("/configure")
def ai_configure(
    body: AIConfigureRequest,
    db: sqlite3.Connection = Depends(get_db),
    _user: dict = Depends(get_current_user),
) -> dict:
    """Update AI/ML configuration settings and return the updated status."""
    env_type_updated = body.python_env_type is not None
    env_path_updated = body.python_env_path is not None

    if env_type_updated or env_path_updated:
        current_type = _read_setting(db, "ai.python_env_type", "system")
        current_path = _read_setting(db, "ai.python_env_path", "")

        resolved_path = (body.python_env_path or "").strip() if env_path_updated else current_path
        inferred_type = _infer_env_type_from_path(resolved_path)
        resolved_type = (
            (body.python_env_type or "").strip().lower() if env_type_updated else inferred_type
        ) or current_type

        env_check = resolve_dependency_environment(resolved_type, resolved_path)
        if not env_check.valid:
            raise HTTPException(
                status_code=400,
                detail=(
                    f"Invalid dependency environment: {env_check.message} "
                    "Use a valid folder before saving."
                ),
            )

    if body.provider is not None:
        provider = (body.provider or "").strip().lower()
        if provider not in _VALID_EMBEDDING_PROVIDERS:
            raise HTTPException(
                status_code=400,
                detail=(
                    "Invalid embedding provider. "
                    f"Expected one of: {', '.join(sorted(_VALID_EMBEDDING_PROVIDERS))}"
                ),
            )
        _write_setting(db, "ai.provider", provider)

    if body.local_model is not None:
        from alma.ai.providers import LOCAL_MODELS
        if body.local_model not in LOCAL_MODELS:
            raise HTTPException(
                status_code=400,
                detail=f"Unknown local model: {body.local_model}. Valid: {', '.join(LOCAL_MODELS.keys())}",
            )
        _write_setting(db, "ai.local_model", body.local_model)

    # After handling any provider/model-related fields above, re-derive
    # the canonical embedding model identifier and persist it to
    # `embedding_model`. This is the single string used by every read
    # path to filter publication_embeddings by the active model, so it
    # must always match what the active provider writes into the `model`
    # column.
    if body.provider is not None or body.local_model is not None:
        from alma.ai.providers import DEFAULT_LOCAL_MODEL, LOCAL_MODELS, OpenAIProvider
        active_provider = _read_setting(db, "ai.provider", "none").strip().lower()
        if active_provider == "local":
            model_key = _read_setting(db, "ai.local_model", DEFAULT_LOCAL_MODEL)
            config = LOCAL_MODELS.get(model_key) or LOCAL_MODELS[DEFAULT_LOCAL_MODEL]
            _write_setting(db, "embedding_model", config.hf_id)
        elif active_provider == "openai":
            _write_setting(db, "embedding_model", OpenAIProvider.MODEL_NAME)

    if body.openai_api_key is not None:
        clean_key = (body.openai_api_key or "").strip()
        if clean_key:
            set_secret(SECRET_OPENAI_API_KEY, clean_key)
        else:
            delete_secret(SECRET_OPENAI_API_KEY)
        # Keep legacy setting scrubbed.
        _write_setting(db, "ai.openai_api_key", "")

    if env_type_updated or env_path_updated:
        next_path = (
            (body.python_env_path or "").strip()
            if env_path_updated
            else _read_setting(db, "ai.python_env_path", "")
        )
        next_type = (
            (body.python_env_type or "").strip().lower()
            if env_type_updated
            else _infer_env_type_from_path(next_path)
        )
        if not next_type:
            next_type = "system"
        if next_type == "system":
            next_path = ""

        _write_setting(db, "ai.python_env_type", next_type)
        _write_setting(db, "ai.python_env_path", next_path)

    db.commit()

    return ai_status(db=db, _user=_user)


@router.post("/recheck-environment")
def recheck_environment(
    db: sqlite3.Connection = Depends(get_db),
    _user: dict = Depends(get_current_user),
) -> dict:
    """Refresh dependency import caches and return current AI status."""
    import importlib

    activate_dependency_environment(db)
    importlib.invalidate_caches()
    return ai_status(db=db, _user=_user)


@router.post("/compute-embeddings", response_model=ComputeEmbeddingsResponse)
def compute_embeddings(
    scope: str = Query(
        "missing_stale",
        description="Which papers to embed: missing, stale, missing_stale, or all.",
    ),
    db: sqlite3.Connection = Depends(get_db),
    _user: dict = Depends(get_current_user),
) -> ComputeEmbeddingsResponse:
    """Trigger batch embedding computation as a background job.

    For every publication that does not yet have an entry in
    ``publication_embeddings``, computes and stores the embedding using
    the active provider.
    """
    from alma.ai.providers import get_active_provider
    from alma.api.scheduler import activity_envelope, find_active_job

    active_provider = get_active_provider(db)
    normalized_scope = str(scope or "missing_stale").strip().lower()
    if normalized_scope not in {"missing", "stale", "missing_stale", "all"}:
        raise HTTPException(status_code=400, detail="Invalid embedding scope")
    if active_provider is None:
        configured_provider = _read_setting(db, "ai.provider", "none").strip().lower()
        local_model = _read_setting(db, "ai.local_model", "").strip().lower()
        message = "No embedding provider is active. Configure one via POST /configure first."
        if configured_provider == "local" and local_model == "specter2-base":
            message = (
                "Local SPECTER2 is selected but unavailable. Install `adapters`, "
                "`transformers`, `torch`, and `numpy` in the selected AI environment."
            )
        return ComputeEmbeddingsResponse(
            job_id="",
            operation_id=None,
            status="noop",
            activity_url=None,
            operation_key=None,
            scope=normalized_scope,
            message=message,
        )

    operation_key = "ai.compute_embeddings" if normalized_scope == "missing_stale" else f"ai.compute_embeddings.{normalized_scope}"
    existing = find_active_job(operation_key)
    if existing:
        env = activity_envelope(
            str(existing.get("job_id") or ""),
            status="already_running",
            operation_key=operation_key,
            message=f"Embedding computation already running ({normalized_scope})",
        )
        return ComputeEmbeddingsResponse(
            job_id=env["job_id"],
            operation_id=env.get("operation_id"),
            status=env.get("status"),
            activity_url=env.get("activity_url"),
            operation_key=env.get("operation_key"),
            scope=normalized_scope,
            message=env.get("message") or "Embedding computation already running",
        )

    job_id = f"compute_embeddings_{normalized_scope}_{uuid.uuid4().hex[:8]}"

    from alma.api.scheduler import schedule_immediate
    from alma.api.scheduler import set_job_status

    set_job_status(
        job_id,
        status="queued",
        operation_key=operation_key,
        trigger_source="user",
        message=f"AI embedding compute queued ({normalized_scope}); may use local CPU/GPU",
        started_at=datetime.utcnow().isoformat(),
    )

    schedule_immediate(
        job_id,
        _run_embedding_computation,
        job_id,
        normalized_scope,
    )

    env = activity_envelope(
        job_id,
        status="queued",
        operation_key=operation_key,
        message=f"Embedding computation started with provider '{active_provider.name}' ({normalized_scope})",
    )
    return ComputeEmbeddingsResponse(
        job_id=env["job_id"],
        operation_id=env.get("operation_id"),
        status=env.get("status"),
        activity_url=env.get("activity_url"),
        operation_key=env.get("operation_key"),
        scope=normalized_scope,
        message=env.get("message") or f"Embedding computation started with provider '{active_provider.name}'",
    )


@router.post("/backfill-s2-vectors", response_model=ComputeEmbeddingsResponse)
def backfill_s2_vectors(
    limit: int = Query(5000, ge=1, le=5000),
    db: sqlite3.Connection = Depends(get_db),
    _user: dict = Depends(get_current_user),
) -> ComputeEmbeddingsResponse:
    """Fetch missing API-sourced Semantic Scholar SPECTER2 vectors."""
    from alma.api.scheduler import activity_envelope, find_active_job, schedule_immediate, set_job_status

    operation_key = "ai.backfill_s2_vectors"
    existing = find_active_job(operation_key)
    if existing:
        env = activity_envelope(
            str(existing.get("job_id") or ""),
            status="already_running",
            operation_key=operation_key,
            message="S2/SPECTER2 vector fetch already running",
        )
        return ComputeEmbeddingsResponse(
            job_id=env["job_id"],
            operation_id=env.get("operation_id"),
            status=env.get("status"),
            activity_url=env.get("activity_url"),
            operation_key=env.get("operation_key"),
            scope="s2_specter2",
            message=env.get("message") or "S2/SPECTER2 vector fetch already running",
        )

    job_id = f"backfill_s2_vectors_{uuid.uuid4().hex[:8]}"
    set_job_status(
        job_id,
        status="queued",
        operation_key=operation_key,
        trigger_source="user",
        message="S2/SPECTER2 vector fetch queued; remote API only, no local AI compute",
        started_at=datetime.utcnow().isoformat(),
    )
    schedule_immediate(job_id, _run_s2_vector_backfill, job_id, int(limit))
    env = activity_envelope(
        job_id,
        status="queued",
        operation_key=operation_key,
        message="S2/SPECTER2 vector fetch started; remote API only, no local AI compute",
    )
    return ComputeEmbeddingsResponse(
        job_id=env["job_id"],
        operation_id=env.get("operation_id"),
        status=env.get("status"),
        activity_url=env.get("activity_url"),
        operation_key=env.get("operation_key"),
        scope="s2_specter2",
        message=env.get("message") or "S2/SPECTER2 vector fetch started",
    )


@router.delete("/embeddings/inactive", response_model=DeleteInactiveEmbeddingsResponse)
def delete_inactive_embeddings(
    db: sqlite3.Connection = Depends(get_db),
    _user: dict = Depends(get_current_user),
) -> DeleteInactiveEmbeddingsResponse:
    """Delete vectors for models other than the active embedding model."""
    from alma.ai.providers import get_active_provider

    from alma.discovery.semantic_scholar import S2_SPECTER2_MODEL

    active_model = _read_setting(db, "embedding_model", S2_SPECTER2_MODEL).strip() or S2_SPECTER2_MODEL
    active_provider = get_active_provider(db)
    if active_provider is not None:
        active_model = active_provider.model_name

    try:
        cursor = db.execute("DELETE FROM publication_embeddings WHERE model <> ?", (active_model,))
        db.commit()
        deleted = int(cursor.rowcount or 0)
    except sqlite3.OperationalError:
        deleted = 0

    return DeleteInactiveEmbeddingsResponse(
        status="ok",
        active_model=active_model,
        deleted=deleted,
        message=f"Deleted {deleted} inactive embedding vectors.",
    )


@router.get("/dependencies")
def ai_dependencies(
    db: sqlite3.Connection = Depends(get_db),
    _user: dict = Depends(get_current_user),
) -> dict:
    """Return install status and version for each AI/ML dependency."""
    dependency_env = activate_dependency_environment(db)
    dependencies, warning = check_packages_in_environment(
        _DEPENDENCY_PACKAGES,
        _DIST_NAME_MAP,
        dependency_env,
    )
    return {
        "environment": dependency_env.as_dict(),
        "dependencies": dependencies,
        "warning": warning,
    }


# ---------------------------------------------------------------------------
# Background task
# ---------------------------------------------------------------------------


def _run_embedding_computation(job_id: str, scope: str = "missing_stale") -> None:
    """Background wrapper for the shared embedding worker."""
    from alma.api.scheduler import add_job_log, is_cancellation_requested, set_job_status
    from alma.services.embeddings import run_embedding_computation

    run_embedding_computation(
        job_id,
        scope=scope,
        set_job_status=set_job_status,
        add_job_log=add_job_log,
        is_cancellation_requested=is_cancellation_requested,
    )


def _run_s2_vector_backfill(job_id: str, limit: int = 200) -> None:
    """Background wrapper for S2/SPECTER2 vector backfill."""
    from alma.api.scheduler import add_job_log, is_cancellation_requested, set_job_status
    from alma.services.s2_vectors import run_s2_vector_backfill

    run_s2_vector_backfill(
        job_id,
        limit=limit,
        set_job_status=set_job_status,
        add_job_log=add_job_log,
        is_cancellation_requested=is_cancellation_requested,
    )
