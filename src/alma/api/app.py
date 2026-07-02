"""FastAPI application for ALMa REST API.

This module provides the main FastAPI application with all routes,
middleware, exception handlers, and OpenAPI documentation.
"""

import os
import sqlite3
import sys
import time
import logging
from contextlib import asynccontextmanager
import uuid

from fastapi import FastAPI, HTTPException, Request, status
from fastapi.responses import FileResponse, JSONResponse
from fastapi.middleware.cors import CORSMiddleware
from fastapi.exceptions import RequestValidationError
from fastapi.staticfiles import StaticFiles

from alma.api.models import HealthResponse, VersionResponse, ErrorResponse, StatisticsResponse
from alma.version import get_app_version
from alma.api.routes import authors_router, papers_router, plugins_router
from alma.api.routes.settings import router as settings_router
from alma.api.routes.operations import router as operations_router
from alma.api.routes.library import router as library_router
from alma.api.routes.alerts import router as alerts_router
from alma.api.routes.discovery import router as discovery_router
from alma.api.routes.feed import router as feed_router
from alma.api.routes.imports import router as imports_router
from alma.api.routes.scheduler import router as scheduler_router
from alma.api.routes.insights import router as insights_router
from alma.api.routes.health import router as health_router
from alma.api.routes.library_mgmt import router as library_mgmt_router
from alma.api.routes.logs import router as logs_router, install_log_handler
from alma.api.routes.activity import router as activity_router
from alma.api.routes.ai import router as ai_router
from alma.api.routes.graphs import router as graphs_router
from alma.api.routes.tags import router as tags_router
from alma.api.routes.topics import router as topics_router
from alma.api.routes.feedback import router as feedback_router
from alma.api.routes.lenses import router as lenses_router
from alma.api.routes.search import router as search_router
from alma.api.routes.backup import router as backup_router
from alma.api.routes.reports import router as reports_router
from alma.api.routes.bootstrap import router as bootstrap_router
from alma.api.routes.extension import router as extension_router
from alma.api.routes.onboarding import router as onboarding_router
from alma.api.deps import get_db, get_plugin_registry, open_db_connection
from alma.api.scheduler import setup_scheduler, shutdown_scheduler

logger = logging.getLogger(__name__)

# Application metadata. API_VERSION is the HTTP contract version (hand-bumped on
# breaking API changes); APP_VERSION tracks the release and derives from the
# single source of truth in pyproject.toml — see alma.version.
API_VERSION = "1.0.0"
APP_VERSION = get_app_version()
START_TIME = time.time()


# ============================================================================
# Lifespan Management
# ============================================================================

@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifespan manager.

    Handles startup and shutdown events.
    """
    # Startup
    logger.info(f"Starting ALMa API v{API_VERSION}")

    # Install the in-memory ring-buffer log handler for the /logs endpoint
    install_log_handler()

    # Validate storage locations and migrate any legacy ./data before the DB
    # is opened at its canonical (OS-standard / Docker-pinned) path. No-op in
    # Docker (DATA_DIR=/app/data is already the data) and on an existing
    # install; only acts when legacy data is found at the old location.
    try:
        from alma.core.storage_migration import validate_and_migrate_storage
        validate_and_migrate_storage()
    except Exception as e:
        # A halt (StorageMigrationHalt) is deliberately fatal — surface it.
        logger.error("Storage validation failed: %s", e)
        raise

    # Initialise database schema ONCE (all DDL, seeds).
    # This keeps per-request get_db() lightweight and avoids lock contention.
    from alma.api.deps import init_db_schema
    init_db_schema()

    # Initialize plugin registry and register plugins
    try:
        # Use the canonical Slack plugin (man-in-the-middle over old slack_bot)
        from alma.plugins.slack import SlackPlugin
        registry = get_plugin_registry()
        registry.register(SlackPlugin)
        logger.info("Registered Slack plugin")
    except Exception as e:
        logger.warning(f"Failed to register Slack plugin: {e}")

    # Start scheduler with periodic alert evaluation and author refresh jobs
    try:
        setup_scheduler()
    except Exception as e:
        logger.warning(f"Failed to start scheduler: {e}")

    yield

    # Shutdown
    logger.info("Shutting down ALMa API")
    try:
        shutdown_scheduler()
    except Exception as e:
        logger.warning(f"Scheduler shutdown error: {e}")


# ============================================================================
# FastAPI Application
# ============================================================================

app = FastAPI(
    title="ALMa API",
    description="""
    REST API for ALMa — Another Library Manager.

    This API provides endpoints for:
    - **Authors**: Manage monitored authors
    - **Papers**: Query and filter papers
    - **Plugins**: Configure messaging platform plugins
    - **System**: Health checks and statistics

    ## Authentication

    The API supports optional authentication via API key:
    - Set the `API_KEY` environment variable to enable authentication
    - Provide the key via `X-API-Key` header or `Bearer` token
    - If no `API_KEY` is set, all requests are allowed (development mode)

    ## Rate Limiting

    Rate limiting will be implemented in a future version.

    ## Examples

    ### List all authors
    ```bash
    curl http://localhost:8000/api/v1/authors
    ```

    ### Add a new author
    ```bash
    curl -X POST http://localhost:8000/api/v1/authors \\
         -H "Content-Type: application/json" \\
         -d '{"scholar_id": "abc123xyz"}'
    ```

    ### Query papers
    ```bash
    curl "http://localhost:8000/api/v1/papers?min_year=2023&min_citations=10"
    ```

    ### Configure a plugin
    ```bash
    curl -X PUT http://localhost:8000/api/v1/plugins/slack/config \\
         -H "Content-Type: application/json" \\
         -d '{"config": {"api_token": "replace-with-real-slack-token", "channel": "#general"}}'
    ```
    """,
    version=API_VERSION,
    lifespan=lifespan,
    docs_url="/docs",
    redoc_url="/redoc",
    openapi_url="/openapi.json",
    contact={
        "name": "ALMa",
        "url": "https://github.com/costantinoai/alma-library-manager",
    },
    license_info={
        "name": "CC BY-NC 4.0",
        "url": "https://creativecommons.org/licenses/by-nc/4.0/",
    },
)


# ============================================================================
# Middleware
# ============================================================================

# CORS - Allow all origins for now (restrict in production)
app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "http://localhost:3000",
        "http://localhost:5173",
        "http://localhost:8000",
    ],
    # Browser-connector origins (see extension/). Firefox sends
    # `moz-extension://<uuid>` and Chromium `chrome-extension://<id>`;
    # the per-install UUID can't be hard-listed, so match the scheme.
    # (The connector fetches from a context with host_permissions for
    # the API, which already bypasses CORS — this is a belt-and-braces
    # allowance so a direct popup fetch works too.)
    allow_origin_regex=r"^(moz-extension|chrome-extension)://.*$",
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# Request logging middleware
@app.middleware("http")
async def log_requests(request: Request, call_next):
    """Log all incoming requests."""
    start_time = time.time()
    request_id = request.headers.get("X-Request-ID") or str(uuid.uuid4())

    # Idle-gating clock (task 37 A): stamp the in-memory "last user activity" time
    # for any user-initiated request, skipping background status polls. In-memory
    # only — never a DB write, so this is safe on a GET. Background
    # health/maintenance ops defer until this goes idle.
    #
    # Two skip signals (41.1): the app-wide GET /activity poll is excluded by
    # path, and any request carrying `X-Alma-Poll: 1` is a timer-driven refetch
    # from an open tab (feed/discovery/status polling) — NOT user presence. Using
    # the header instead of a path allow-list means real navigation to those same
    # endpoints still counts, so an open-but-untouched tab no longer pins the app
    # "active" and starves background enrichment.
    from alma.core.user_activity import is_user_activity_path, touch_user_activity

    if not request.headers.get("X-Alma-Poll") and is_user_activity_path(request.url.path):
        touch_user_activity()

    # Process request
    response = await call_next(request)

    # Log request details
    process_time = time.time() - start_time
    logger.info(
        f"{request.method} {request.url.path} "
        f"status={response.status_code} "
        f"duration={process_time:.3f}s "
        f"request_id={request_id}"
    )

    # Add custom header
    response.headers["X-Process-Time"] = str(process_time)
    response.headers["X-Request-ID"] = request_id

    return response


# ============================================================================
# Exception Handlers
# ============================================================================

@app.exception_handler(RequestValidationError)
async def validation_exception_handler(request: Request, exc: RequestValidationError):
    """Handle validation errors."""
    logger.warning(f"Validation error: {exc}")
    return JSONResponse(
        status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
        content={
            "error": "ValidationError",
            "message": "Request validation failed",
            "detail": exc.errors()
        }
    )


@app.exception_handler(sqlite3.OperationalError)
async def sqlite_lock_exception_handler(request: Request, exc: sqlite3.OperationalError):
    """Surface transient SQLite write-lock contention as retryable (503).

    A "database is locked/busy" that escapes ``run_with_lock_retry`` /
    ``run_write_unit`` is a *transient* condition — the client should retry,
    not show a fatal error. 503 + ``Retry-After`` is the truthful status;
    the frontend api layer retries these automatically. Every other
    OperationalError (corruption, readonly, schema) is a real bug and falls
    through to the generic 500 path.
    """
    from alma.core.db_retry import is_transient_lock_error

    if is_transient_lock_error(exc):
        logger.warning(
            f"Transient SQLite lock escaped retries on "
            f"{request.method} {request.url.path} — returning 503"
        )
        return JSONResponse(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            headers={"Retry-After": "1"},
            content={
                "error": "DatabaseBusy",
                "message": "Database briefly locked — retry",
                "detail": "Database briefly locked — retry",
            },
        )
    return await general_exception_handler(request, exc)


@app.exception_handler(Exception)
async def general_exception_handler(request: Request, exc: Exception):
    """Handle uncaught exceptions."""
    logger.error(f"Unhandled exception: {exc}", exc_info=True)
    return JSONResponse(
        status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
        content={
            "error": "InternalServerError",
            "message": "An unexpected error occurred",
            "detail": None,
        }
    )


# ============================================================================
# Root Routes
# ============================================================================

@app.get("/api")
async def api_root():
    """API root endpoint."""
    return {
        "name": "ALMa API",
        "version": API_VERSION,
        "status": "operational",
        "documentation": "/docs",
        "endpoints": {
            "health": "/api/v1/health",
            "version": "/api/v1/version",
            "authors": "/api/v1/authors",
            "papers": "/api/v1/papers",
            "plugins": "/api/v1/plugins",
            "stats": "/api/v1/stats",
        }
    }


# ============================================================================
# System Routes
# ============================================================================

@app.get(
    "/api/v1/health",
    response_model=HealthResponse,
    summary="Health check",
    description="Check the health status of the API and its dependencies.",
    tags=["system"]
)
def health_check():
    """Health check endpoint.

    Runs as a sync handler so the SQLite probe goes through the anyio
    threadpool instead of the event loop. A blocked event loop would make
    health checks lie about the rest of the system being responsive.
    """
    # Check database connection (unified scholar.db)
    database_ok = True
    try:
        db = open_db_connection()
        try:
            db.execute("SELECT 1").fetchone()
        finally:
            db.close()
    except Exception as e:
        logger.error(f"Database health check failed: {e}")
        database_ok = False

    # Determine overall status
    if database_ok:
        service_status = "healthy"
    else:
        service_status = "unhealthy"

    uptime = time.time() - START_TIME

    return HealthResponse(
        status=service_status,
        version=API_VERSION,
        uptime_seconds=uptime,
        database_ok=database_ok
    )


@app.get(
    "/api/v1/version",
    response_model=VersionResponse,
    summary="Version information",
    description="Get version information for the API and application.",
    tags=["system"]
)
async def version_info():
    """Get version information.

    Returns:
        VersionResponse: Version details

    Example:
        ```bash
        curl http://localhost:8000/api/v1/version
        ```
    """
    return VersionResponse(
        api_version=API_VERSION,
        app_version=APP_VERSION,
        python_version=f"{sys.version_info.major}.{sys.version_info.minor}.{sys.version_info.micro}"
    )


@app.get(
    "/api/v1/stats",
    response_model=StatisticsResponse,
    summary="Overall statistics",
    description="Get aggregate statistics about the system.",
    tags=["system"]
)
def get_statistics():
    """Get overall system statistics.

    Sync handler: the aggregate COUNTs go through the anyio threadpool so they
    do not block the event loop under concurrent load.
    """
    try:
        # Get all counts from the unified DB
        db = open_db_connection()
        try:
            cursor = db.execute("SELECT COUNT(*) as count FROM authors")
            total_authors = cursor.fetchone()["count"]

            cursor = db.execute("SELECT COUNT(*) as count FROM papers")
            total_publications = cursor.fetchone()["count"]

            cursor = db.execute("SELECT COALESCE(SUM(cited_by_count), 0) as total FROM papers")
            total_citations = cursor.fetchone()["total"] or 0
        finally:
            db.close()

        # Get plugin stats
        registry = get_plugin_registry()
        configured_plugins = len([p for p in registry.list_plugins()
                                  if registry.get_instance(p) is not None])

        return StatisticsResponse(
            total_authors=total_authors,
            total_publications=total_publications,
            total_citations=int(total_citations),
            active_jobs=0,  # TODO: Implement jobs
            configured_plugins=configured_plugins
        )

    except Exception as e:
        logger.error(f"Error retrieving statistics: {e}")
        return StatisticsResponse(
            total_authors=0,
            total_publications=0,
            total_citations=0,
            active_jobs=0,
            configured_plugins=0
        )


# ============================================================================
# API Routes
# ============================================================================

# Mount API routers with v1 prefix
app.include_router(authors_router, prefix="/api/v1")
app.include_router(papers_router, prefix="/api/v1")
app.include_router(plugins_router, prefix="/api/v1")
app.include_router(operations_router, prefix="/api/v1")
app.include_router(settings_router, prefix="/api/v1")
app.include_router(library_router, prefix="/api/v1/library", tags=["library"])
app.include_router(imports_router, prefix="/api/v1/library", tags=["library-import"])
app.include_router(alerts_router, prefix="/api/v1/alerts", tags=["alerts"])
app.include_router(discovery_router, prefix="/api/v1/discovery", tags=["discovery"])
app.include_router(feed_router, prefix="/api/v1/feed", tags=["feed"])
app.include_router(lenses_router, prefix="/api/v1/lenses", tags=["discovery-lenses"])
app.include_router(scheduler_router, prefix="/api/v1/scheduler", tags=["scheduler"])
app.include_router(insights_router, prefix="/api/v1/insights", tags=["insights"])
app.include_router(health_router, prefix="/api/v1/health", tags=["health"])
app.include_router(library_mgmt_router, prefix="/api/v1/library-mgmt", tags=["library-management"])
app.include_router(logs_router, prefix="/api/v1/logs", tags=["logs"])
app.include_router(activity_router, prefix="/api/v1/activity", tags=["activity"])
app.include_router(ai_router, prefix="/api/v1/ai", tags=["ai"])
app.include_router(graphs_router, prefix="/api/v1/graphs", tags=["graphs"])
app.include_router(tags_router, prefix="/api/v1/tags", tags=["tags"])
app.include_router(topics_router, prefix="/api/v1/topics", tags=["topics"])
app.include_router(feedback_router, prefix="/api/v1/feedback", tags=["feedback"])
app.include_router(search_router, prefix="/api/v1", tags=["search"])
app.include_router(backup_router, prefix="/api/v1", tags=["backup"])
app.include_router(reports_router, prefix="/api/v1/reports", tags=["reports"])
app.include_router(bootstrap_router, prefix="/api/v1", tags=["bootstrap"])
app.include_router(extension_router, prefix="/api/v1/extension", tags=["extension"])
app.include_router(onboarding_router, prefix="/api/v1", tags=["onboarding"])

# ============================================================================
# React Frontend (Production Build)
# ============================================================================

# Resolve the frontend dist directory (development layout or Docker layout)
_frontend_dist = os.path.join(os.path.dirname(__file__), "../../../frontend/dist")
if not os.path.exists(_frontend_dist):
    _frontend_dist = "/app/frontend/dist"

if os.path.exists(_frontend_dist):
    # Serve static assets (JS, CSS, images) under /assets
    _assets_dir = os.path.join(_frontend_dist, "assets")
    if os.path.exists(_assets_dir):
        app.mount("/assets", StaticFiles(directory=_assets_dir), name="frontend-assets")

    # Catch-all: serve real dist files when they exist, else 404.
    #
    # The SPA uses hash-based client routing (`#/feed`, `#/library`,
    # ...), so the HTTP path is always `/` for in-app navigation —
    # the route lives in the URL fragment, which never reaches the
    # server. That means a defensive fallback to index.html for
    # arbitrary unknown paths has no legitimate consumer: the only
    # things that hit it are (a) external clients probing wrong
    # paths and (b) frontend bugs calling an unprefixed API path,
    # both of which would silently receive `text/html` and log as
    # 200 OK — masking the bug and bloating the access log. We
    # 404 those paths instead so misuse is visible.
    #
    # Vite puts frontend/public/ files at the root of dist
    # (favicon.svg, manifest.webmanifest, mask-icon.svg, brand/,
    # ...), so resolving by file existence covers every public
    # asset uniformly without per-prefix mounts.
    #
    # Path-traversal guard: rebuild the requested path under
    # _frontend_dist with realpath and verify it stays inside.
    @app.get("/{full_path:path}")
    async def serve_frontend(full_path: str):
        """Serve a real dist file if it exists, else 404."""
        if full_path.startswith(("api/", "docs", "redoc", "openapi", "static")):
            raise HTTPException(status_code=404, detail="Not found")

        if not full_path:
            index_path = os.path.join(_frontend_dist, "index.html")
            if os.path.exists(index_path):
                return FileResponse(index_path)
            raise HTTPException(status_code=404, detail="Frontend not built")

        candidate = os.path.realpath(os.path.join(_frontend_dist, full_path))
        dist_root = os.path.realpath(_frontend_dist)
        if (
            candidate.startswith(dist_root + os.sep)
            and os.path.isfile(candidate)
        ):
            return FileResponse(candidate)

        raise HTTPException(status_code=404, detail="Not found")

    logger.info("React frontend mounted from %s", _frontend_dist)


# ============================================================================
# Main Entry Point
# ============================================================================

if __name__ == "__main__":
    import uvicorn

    # Configure logging
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )

    # Run server
    uvicorn.run(
        "app:app",
        host=os.getenv("API_HOST", "0.0.0.0"),
        port=int(os.getenv("API_PORT", 8000)),
        reload=os.getenv("DEBUG", "false").lower() == "true",
        log_level="info"
    )
