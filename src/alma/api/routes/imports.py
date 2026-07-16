"""Library import API endpoints: BibTeX file/text and Zotero integration.

All four user-triggered import endpoints default to a background Activity job
when the scheduler is available so large imports do not block the UI. Callers
that need the legacy synchronous behavior (tests, minimal environments) can
force it with ``?background=false``. The async response shape matches Feed
and Authors (``{status, job_id, operation_key, activity_url, ...}``).
"""

import asyncio
import hashlib
import logging
import sqlite3
import uuid
from datetime import datetime

from fastapi import APIRouter, Depends, File, Form, HTTPException, Query, UploadFile
from fastapi.responses import StreamingResponse
from pydantic import BaseModel, Field

from alma.api.deps import get_current_user, get_db, open_db_connection
from alma.api.helpers import background_mode_requested
from alma.api.models import (
    BibtexTextImportRequest,
    ImportResultResponse,
    ZoteroCollectionResponse,
    ZoteroCollectionsRequest,
    ZoteroImportRequest,
)
from alma.application import import_preflight
from alma.application import imports as imports_app
from alma.application import library as library_app
from alma.core.db_write import run_write_unit
from alma.core.operations import OperationOutcome, OperationRunner
from alma.core.redaction import redact_sensitive_text
from alma.core.secrets import (
    SECRET_ZOTERO_API_KEY,
    get_secret,
    set_secret,
)
from alma.library.importer import (
    ImportResult,
    import_bibtex,
    import_zotero,
    import_zotero_rdf,
    list_zotero_collections,
)

logger = logging.getLogger(__name__)

router = APIRouter(
    dependencies=[Depends(get_current_user)],
    responses={401: {"description": "Unauthorized"}},
)


class PublicationRef(BaseModel):
    paper_id: str


class ResolveImportedRequest(BaseModel):
    items: list[PublicationRef] = Field(default_factory=list)
    unresolved_only: bool = True
    limit: int = 1000
    background: bool = True


class ImportPreflightResponse(BaseModel):
    source: str
    total_entries: int
    valid_entries: int
    parse_errors: int
    errors: list[str] = Field(default_factory=list)
    identifiers: dict
    metadata: dict
    dedup: dict
    likely_source_calls: dict
    eta: dict


class ConfirmStagedImportResponse(BaseModel):
    status: str
    paper_id: str


# Phase C — online source search import.
class OnlineSearchRequest(BaseModel):
    query: str = Field(
        ...,
        min_length=1,
        max_length=500,
        description=(
            "Free-form search: paper title, DOI, OpenAlex URL/ID, "
            "`author:<name>`, or `title:<fragment>`."
        ),
    )
    limit: int = Field(default=20, ge=1, le=50)
    year_min: int | None = Field(default=None, ge=1800, le=2100)
    year_max: int | None = Field(default=None, ge=1800, le=2100)


class OnlineAuthorSearchResult(BaseModel):
    """One author candidate from `/library/import/search/authors`."""
    openalex_id: str
    name: str
    orcid: str | None = None
    institution: str | None = None
    works_count: int = 0
    cited_by_count: int = 0
    h_index: int = 0
    i10_index: int = 0
    top_topics: list[str] = []
    already_followed: bool = False
    # Local-library integration (dedup / identity resolution). Resolved
    # against the same followed-identity union the suggestion rail uses
    # (direct OpenAlex id / merged alt ids / ORCID). `existing_author_id`
    # is the canonical local `authors.id` when this human already has a row
    # (followed OR background) — it lets the search card open the full
    # author detail popup instead of the OpenAlex-only fallback.
    existing_author_id: str | None = None
    existing_author_type: str | None = None
    # Titles of the author's two most-cited works (from OpenAlex), shown on
    # the search card so the user can recognise the right person at a glance.
    top_cited_titles: list[str] = []


class OnlineSearchSaveRequest(BaseModel):
    action: str = Field(
        ...,
        description="One of add | like | love | dislike (shared 3/4/5/1 contract).",
    )
    openalex_id: str | None = None
    doi: str | None = None
    link: str | None = None
    title: str | None = None
    query: str | None = None
    candidate: dict | None = Field(
        default=None,
        description=(
            "Full multi-source search candidate (fields as returned by "
            "`/library/import/search`). Used as a fallback when OpenAlex "
            "cannot resolve the paper but another source (Semantic Scholar, "
            "Crossref, arXiv, bioRxiv) already provided full metadata."
        ),
    )
    collection_name: str | None = Field(
        default=None,
        description=(
            "Optional local collection name. When set and the paper lands in "
            "Library (add/like/love), the collection is created/found and the "
            "paper is added to it. Ignored for dislike."
        ),
    )
    collection_ids: list[str] = Field(
        default_factory=list,
        description="Existing collection ids selected by the paper-card chooser.",
    )


def _redact_exception_message(exc: Exception, secrets: list[str]) -> str:
    msg = redact_sensitive_text(str(exc))
    for secret in secrets:
        token = (secret or "").strip()
        if token and len(token) >= 4:
            msg = msg.replace(token, "***")
    return msg


def _fetch_zotero_collections_safe(
    library_id: str,
    api_key: str,
    library_type: str,
) -> list[ZoteroCollectionResponse]:
    try:
        collections = list_zotero_collections(library_id, api_key, library_type)
    except Exception as exc:
        safe_msg = _redact_exception_message(exc, [api_key])
        logger.error(
            "Zotero collections fetch failed for library_id=%s type=%s: %s",
            library_id,
            library_type,
            safe_msg,
        )
        raise HTTPException(
            status_code=502,
            detail="Failed to connect to Zotero",
        )
    return [ZoteroCollectionResponse(**c) for c in collections]


def _resolve_zotero_api_key(raw_key: str | None) -> str:
    provided = (raw_key or "").strip()
    if provided:
        set_secret(SECRET_ZOTERO_API_KEY, provided)
        return provided
    stored = get_secret(SECRET_ZOTERO_API_KEY)
    if stored:
        return stored
    raise HTTPException(status_code=400, detail="Zotero API key is required")


# ===================================================================
# Shared sync / async runners (Phase A background-first contract)
# ===================================================================

def _run_import_sync(
    *,
    db: sqlite3.Connection,
    user: dict,
    operation_key: str,
    message: str,
    import_callable,
    secrets: list[str] | None = None,
) -> ImportResultResponse:
    """Execute an importer inline through OperationRunner.

    ``import_callable`` takes no arguments and must return an ``ImportResult``.
    The OperationRunner records an Activity row synchronously so the sync path
    stays observable alongside background jobs.
    """
    runner = OperationRunner(db)
    captured: dict = {}

    def _handler(_ctx):
        result: ImportResult = import_callable()
        payload = result.to_dict()
        captured["payload"] = payload
        return OperationOutcome(
            status="completed",
            message=message,
            result={
                "imported": payload.get("imported", 0),
                "staged": payload.get("staged", 0),
                "skipped": payload.get("skipped", 0),
                "failed": payload.get("failed", 0),
                "total": payload.get("total", 0),
            },
        )

    try:
        runner.run(
            operation_key=operation_key,
            handler=_handler,
            trigger_source="user",
            actor=str(user.get("username") or "api_user"),
        )
    except Exception as exc:
        safe_msg = _redact_exception_message(exc, secrets or [])
        logger.error("Import %s failed: %s", operation_key, safe_msg)
        raise HTTPException(status_code=500, detail=f"{message.split(' completed')[0]} failed")

    return ImportResultResponse(**captured["payload"])


def _queue_import_background(
    *,
    operation_key: str,
    queued_message: str,
    running_message: str,
    import_callable,
    secrets: list[str] | None = None,
) -> dict:
    """Enqueue an importer as a background Activity job.

    ``import_callable`` takes a single ``sqlite3.Connection`` argument (opened
    fresh on the worker thread) and returns an ``ImportResult``. The response
    is an Activity envelope the frontend can poll via ``activity_url``.
    """
    from alma.api.scheduler import (
        activity_envelope,
        add_job_log,
        find_active_job,
        schedule_immediate,
        set_job_status,
    )

    existing = find_active_job(operation_key)
    if existing:
        return activity_envelope(
            str(existing.get("job_id") or ""),
            status="already_running",
            operation_key=operation_key,
            message=f"{queued_message} — already running",
        )

    job_id = f"import_{uuid.uuid4().hex[:10]}"
    set_job_status(
        job_id,
        status="queued",
        operation_key=operation_key,
        trigger_source="user",
        started_at=datetime.utcnow().isoformat(),
        processed=0,
        total=0,
        message=queued_message,
    )
    add_job_log(job_id, queued_message, step="queued")

    safe_secrets = [s for s in (secrets or []) if s]

    def _runner() -> None:
        conn = open_db_connection()
        try:
            set_job_status(
                job_id,
                status="running",
                message=running_message,
                operation_key=operation_key,
                trigger_source="user",
            )
            result = import_callable(conn)
            payload = result.to_dict()
            total = int(payload.get("total") or 0)
            imported = int(payload.get("imported") or 0)
            staged = int(payload.get("staged") or 0)
            skipped = int(payload.get("skipped") or 0)
            failed = int(payload.get("failed") or 0)
            errors = list(payload.get("errors") or [])
            summary = {
                "total": total,
                "imported": imported,
                "staged": staged,
                "skipped": skipped,
                "failed": failed,
                "errors": errors[:20],
                "error_count": len(errors),
            }
            final_status = "noop" if (imported == 0 and staged == 0 and failed == 0 and total == 0) else "completed"
            final_message = (
                f"Import finished: imported {imported}, staged {staged}, skipped {skipped}, "
                f"failed {failed} of {total} entries"
            )
            add_job_log(job_id, final_message, step="summary", data=summary)
            set_job_status(
                job_id,
                status=final_status,
                finished_at=datetime.utcnow().isoformat(),
                processed=total,
                total=total,
                message=final_message,
                result=summary,
                operation_key=operation_key,
                trigger_source="user",
            )
        except Exception as exc:
            safe_msg = _redact_exception_message(exc, safe_secrets)
            add_job_log(
                job_id,
                f"Import failed: {safe_msg}",
                level="ERROR",
                step="failed",
            )
            set_job_status(
                job_id,
                status="failed",
                finished_at=datetime.utcnow().isoformat(),
                message=f"Import failed: {safe_msg}",
                error=safe_msg,
                operation_key=operation_key,
                trigger_source="user",
            )
        finally:
            conn.close()

    schedule_immediate(job_id, _runner)
    return activity_envelope(
        job_id,
        status="queued",
        operation_key=operation_key,
        message=queued_message,
    )


# ===================================================================
# Import Preflight
# ===================================================================

@router.post(
    "/import/preflight/bibtex",
    response_model=ImportPreflightResponse,
    summary="Preview enrichment cost for a BibTeX file upload",
)
async def preflight_bibtex_file_endpoint(
    file: UploadFile = File(..., description="A .bib file to preview"),
    db: sqlite3.Connection = Depends(get_db),
):
    """Parse a BibTeX file without writing rows and forecast enrichment cost."""
    try:
        raw = await file.read()
        content = raw.decode("utf-8", errors="replace")
    except Exception as exc:
        logger.warning("Failed reading uploaded BibTeX file for preflight: %s", redact_sensitive_text(str(exc)))
        raise HTTPException(status_code=400, detail="Cannot read uploaded file")
    parsed = import_preflight.parse_bibtex_records(content)
    return import_preflight.summarize_records(
        db,
        parsed.records,
        source="bibtex",
        errors=parsed.errors,
    )


@router.post(
    "/import/preflight/bibtex/text",
    response_model=ImportPreflightResponse,
    summary="Preview enrichment cost for pasted BibTeX text",
)
def preflight_bibtex_text_endpoint(
    req: BibtexTextImportRequest,
    db: sqlite3.Connection = Depends(get_db),
):
    """Parse pasted BibTeX without writing rows and forecast enrichment cost."""
    if not req.content or not req.content.strip():
        raise HTTPException(status_code=400, detail="BibTeX content is empty")
    parsed = import_preflight.parse_bibtex_records(req.content)
    return import_preflight.summarize_records(
        db,
        parsed.records,
        source="bibtex",
        errors=parsed.errors,
    )


@router.post(
    "/import/preflight/zotero/rdf",
    response_model=ImportPreflightResponse,
    summary="Preview enrichment cost for a Zotero RDF export",
)
async def preflight_zotero_rdf_file_endpoint(
    file: UploadFile = File(..., description="A Zotero RDF (.rdf) export file to preview"),
    db: sqlite3.Connection = Depends(get_db),
):
    """Parse Zotero RDF without writing rows and forecast enrichment cost."""
    try:
        raw = await file.read()
        content = raw.decode("utf-8", errors="replace")
    except Exception as exc:
        logger.warning("Failed reading uploaded Zotero RDF file for preflight: %s", redact_sensitive_text(str(exc)))
        raise HTTPException(status_code=400, detail="Cannot read uploaded file")
    parsed = import_preflight.parse_zotero_rdf_records(content)
    return import_preflight.summarize_records(
        db,
        parsed.records,
        source="zotero_rdf",
        errors=parsed.errors,
        parse_duplicates=parsed.parse_duplicates,
    )


@router.post(
    "/import/preflight/zotero",
    response_model=ImportPreflightResponse,
    summary="Preview enrichment cost for a Zotero library import",
)
def preflight_zotero_endpoint(
    req: ZoteroImportRequest,
    db: sqlite3.Connection = Depends(get_db),
):
    """Fetch Zotero metadata without writing rows and forecast enrichment cost."""
    api_key = _resolve_zotero_api_key(req.api_key)
    parsed = import_preflight.fetch_zotero_records(
        req.library_id,
        api_key,
        library_type=req.library_type,
        collection_key=req.collection_key,
    )
    return import_preflight.summarize_records(
        db,
        parsed.records,
        source="zotero",
        errors=parsed.errors,
    )


# ===================================================================
# BibTeX Import
# ===================================================================

@router.post(
    "/import/bibtex",
    response_model=None,
    summary="Import from a BibTeX file upload",
)
async def import_bibtex_file_endpoint(
    file: UploadFile = File(..., description="A .bib file to import"),
    collection_name: str = Form(None, description="Optional target collection name"),
    background: bool | None = Query(
        None,
        description="Run as a background Activity job when the scheduler is enabled",
    ),
    db: sqlite3.Connection = Depends(get_db),
    user: dict = Depends(get_current_user),
):
    """Upload a .bib file and import every entry into Saved Library.

    The file contents are read from the request before any work starts so the
    background worker does not depend on request-scoped state. Duplicates that
    already have a canonical `papers` row are promoted into Library instead of
    silently skipped. Tags on each BibTeX entry become local tags.

    By default the import runs as a background Activity job when the scheduler
    is available and the response is a queued envelope with `job_id` and
    `activity_url`. Pass `?background=false` to force inline execution and
    receive the full `ImportResultResponse` in the response body (used by
    tests and minimal environments).
    """
    try:
        raw = await file.read()
        content = raw.decode("utf-8", errors="replace")
    except Exception as exc:
        logger.warning("Failed reading uploaded BibTeX file: %s", redact_sensitive_text(str(exc)))
        raise HTTPException(status_code=400, detail="Cannot read uploaded file")

    fingerprint = hashlib.sha1(content.encode("utf-8", errors="replace")).hexdigest()[:10]
    operation_key = f"imports.bibtex.file:{fingerprint}"

    if not background_mode_requested(background):
        # Sync path (opt-in via ?background=false, used by tests). Parsing +
        # DB writes can run for seconds on large imports, so dispatch through
        # the anyio threadpool rather than blocking the event loop.
        return await asyncio.to_thread(
            _run_import_sync,
            db=db,
            user=user,
            operation_key=operation_key,
            message="BibTeX import completed",
            import_callable=lambda: import_bibtex(content, db, collection_name=collection_name),
        )

    byte_len = len(raw)
    return _queue_import_background(
        operation_key=operation_key,
        queued_message=f"Queued BibTeX import ({byte_len} bytes)",
        running_message="Importing BibTeX entries",
        import_callable=lambda conn: import_bibtex(content, conn, collection_name=collection_name),
    )


@router.post(
    "/import/bibtex/text",
    response_model=None,
    summary="Import from pasted BibTeX text",
)
def import_bibtex_text_endpoint(
    req: BibtexTextImportRequest,
    background: bool | None = Query(
        None,
        description="Run as a background Activity job when the scheduler is enabled",
    ),
    db: sqlite3.Connection = Depends(get_db),
    user: dict = Depends(get_current_user),
):
    """Import papers from a BibTeX string pasted into the UI.

    Same contract as the file-upload variant (background-first, sync fallback
    via `?background=false`). Empty strings are rejected up front.
    """
    if not req.content or not req.content.strip():
        raise HTTPException(status_code=400, detail="BibTeX content is empty")

    fingerprint = hashlib.sha1(req.content.encode("utf-8", errors="replace")).hexdigest()[:10]
    operation_key = f"imports.bibtex.text:{fingerprint}"

    if not background_mode_requested(background):
        return _run_import_sync(
            db=db,
            user=user,
            operation_key=operation_key,
            message="BibTeX text import completed",
            import_callable=lambda: import_bibtex(req.content, db, collection_name=req.collection_name),
        )

    return _queue_import_background(
        operation_key=operation_key,
        queued_message=f"Queued BibTeX text import ({len(req.content)} chars)",
        running_message="Importing BibTeX entries",
        import_callable=lambda conn: import_bibtex(req.content, conn, collection_name=req.collection_name),
    )


# ===================================================================
# Zotero Import
# ===================================================================

@router.post(
    "/import/zotero",
    response_model=None,
    summary="Import from a Zotero library",
)
def import_zotero_endpoint(
    req: ZoteroImportRequest,
    background: bool | None = Query(
        None,
        description="Run as a background Activity job when the scheduler is enabled",
    ),
    db: sqlite3.Connection = Depends(get_db),
    user: dict = Depends(get_current_user),
):
    """Import from a Zotero personal or group library.

    Requires `library_id` and either a provided `api_key` or a previously stored
    Zotero key. Optional `collection_key` filters to a single Zotero collection.
    Optional `collection_name` groups the result into a local collection.

    Zotero tags are imported as local tags; Zotero collections are mirrored as
    local collections. The API key is resolved in-request (before enqueueing)
    so the background worker never touches `_resolve_zotero_api_key`.
    """
    api_key = _resolve_zotero_api_key(req.api_key)
    fingerprint = hashlib.sha1(
        "|".join([
            req.library_id,
            req.library_type,
            req.collection_key or "",
            req.collection_name or "",
        ]).encode("utf-8", errors="replace")
    ).hexdigest()[:10]
    operation_key = f"imports.zotero:{req.library_type}:{fingerprint}"

    if not background_mode_requested(background):
        return _run_import_sync(
            db=db,
            user=user,
            operation_key=operation_key,
            message="Zotero import completed",
            secrets=[api_key],
            import_callable=lambda: import_zotero(
                library_id=req.library_id,
                api_key=api_key,
                conn=db,
                library_type=req.library_type,
                collection_key=req.collection_key,
                collection_name=req.collection_name,
            ),
        )

    queued_message = (
        f"Queued Zotero import (library={req.library_id}, type={req.library_type}"
        + (f", collection={req.collection_key}" if req.collection_key else "")
        + ")"
    )
    return _queue_import_background(
        operation_key=operation_key,
        queued_message=queued_message,
        running_message="Fetching items from Zotero",
        secrets=[api_key],
        import_callable=lambda conn: import_zotero(
            library_id=req.library_id,
            api_key=api_key,
            conn=conn,
            library_type=req.library_type,
            collection_key=req.collection_key,
            collection_name=req.collection_name,
        ),
    )


@router.post(
    "/import/zotero/collections",
    response_model=list[ZoteroCollectionResponse],
    summary="List Zotero collections",
)
def list_zotero_collections_post_endpoint(
    req: ZoteroCollectionsRequest,
):
    """Fetch Zotero collections via POST to avoid API key in query strings.

    POST-only by design (43.3): the deleted GET wrapper took the API key as a
    query parameter and PERSISTED it via `_resolve_zotero_api_key` → `set_secret`
    — a secret written from a URL on a read verb. The frontend only ever called
    this POST; persisting the key belongs to the request body, not a query
    string.
    """
    api_key = _resolve_zotero_api_key(req.api_key)
    return _fetch_zotero_collections_safe(
        req.library_id,
        api_key,
        req.library_type,
    )


@router.post(
    "/import/zotero/rdf",
    response_model=None,
    summary="Import from a Zotero RDF export file",
)
async def import_zotero_rdf_file_endpoint(
    file: UploadFile = File(..., description="A Zotero RDF (.rdf) export file"),
    collection_name: str = Form(None, description="Optional target collection name"),
    background: bool | None = Query(
        None,
        description="Run as a background Activity job when the scheduler is enabled",
    ),
    db: sqlite3.Connection = Depends(get_db),
    user: dict = Depends(get_current_user),
):
    """Upload a Zotero RDF export and import every item into Saved Library."""
    try:
        raw = await file.read()
        content = raw.decode("utf-8", errors="replace")
    except Exception as exc:
        logger.warning("Failed reading uploaded Zotero RDF file: %s", redact_sensitive_text(str(exc)))
        raise HTTPException(status_code=400, detail="Cannot read uploaded file")

    fingerprint = hashlib.sha1(content.encode("utf-8", errors="replace")).hexdigest()[:10]
    operation_key = f"imports.zotero.rdf:{fingerprint}"

    if not background_mode_requested(background):
        # Sync path: parsing + DB writes run through the anyio threadpool so
        # they do not block the event loop under `?background=false`.
        return await asyncio.to_thread(
            _run_import_sync,
            db=db,
            user=user,
            operation_key=operation_key,
            message="Zotero RDF import completed",
            import_callable=lambda: import_zotero_rdf(content, db, collection_name=collection_name),
        )

    byte_len = len(raw)
    return _queue_import_background(
        operation_key=operation_key,
        queued_message=f"Queued Zotero RDF import ({byte_len} bytes)",
        running_message="Parsing Zotero RDF entries",
        import_callable=lambda conn: import_zotero_rdf(content, conn, collection_name=collection_name),
    )


# ===================================================================
# Post-Import Enrichment
# ===================================================================

@router.post(
    "/import/enrich",
    summary="Enrich imported publications",
    description="Resolve imported publications via OpenAlex to populate topics, institutions, and citations.",
)
def enrich_imports(
    background: bool = Query(True, description="Run enrichment in background"),
    user: dict = Depends(get_current_user),
    pub_db: sqlite3.Connection = Depends(get_db),
):
    """Trigger enrichment of all unenriched publications.

    Publications that were imported from BibTeX or Zotero often lack topics,
    institutions, and accurate citation counts.  This endpoint resolves each
    one via OpenAlex (by DOI or title search) and fills in the gaps.

    When ``background=True`` (default), enrichment runs asynchronously and
    a ``job_id`` is returned for progress tracking.  When ``background=False``,
    the request blocks until enrichment is complete and returns the result
    directly.
    """
    from alma.library.enrichment import enrich_all_unenriched

    if not background:
        result = enrich_all_unenriched(pub_db)
        return result

    # Background execution via scheduler
    job_id = f"enrich_{uuid.uuid4().hex[:12]}"

    try:
        from alma.api.scheduler import (
            activity_envelope,
            add_job_log,
            find_active_job,
            schedule_immediate,
            set_job_status,
        )

        operation_key = "imports.enrich_all"
        existing = find_active_job(operation_key)
        if existing:
            return activity_envelope(
                str(existing.get("job_id") or ""),
                status="already_running",
                operation_key=operation_key,
                message="Import enrichment already running",
            )

        set_job_status(
            job_id,
            status="queued",
            operation_key=operation_key,
            trigger_source="user",
            started_at=datetime.utcnow().isoformat(),
            message="Enrichment queued",
        )
        add_job_log(job_id, "Queued enrichment for imported publications", step="queued")

        def _run_enrichment():
            conn = open_db_connection()
            try:
                enrich_all_unenriched(conn, job_id=job_id)
            finally:
                conn.close()

        schedule_immediate(job_id, _run_enrichment)
    except Exception as exc:
        logger.warning("Failed to schedule background enrichment: %s", exc)
        # Fallback to synchronous
        result = enrich_all_unenriched(pub_db)
        return result

    return activity_envelope(
        job_id,
        status="queued",
        operation_key="imports.enrich_all",
        message="Enrichment started in background",
    )


@router.get(
    "/import/unresolved",
    summary="List publications not resolved via OpenAlex",
)
def list_unresolved_imported_publications(
    limit: int = Query(200, ge=1, le=5000),
    db: sqlite3.Connection = Depends(get_db),
    user: dict = Depends(get_current_user),
):
    rows = imports_app.list_resolution_queue(db, unresolved_only=True, limit=limit)
    return {"total": len(rows), "items": rows}


@router.post(
    "/import/staged/{paper_id}/confirm",
    response_model=ConfirmStagedImportResponse,
    summary="Save a staged low-confidence import to Library",
)
def confirm_staged_import(
    paper_id: str,
    db: sqlite3.Connection = Depends(get_db),
    user: dict = Depends(get_current_user),
):
    """Promote a reviewed low-confidence import through the Library write path."""
    target_id = (paper_id or "").strip()
    if not target_id:
        raise HTTPException(status_code=400, detail="paper_id is required")

    def _persist() -> str:
        row = db.execute(
            """
            SELECT id, status, COALESCE(added_from, '') AS added_from, COALESCE(notes, '') AS notes
            FROM papers
            WHERE id = ?
            """,
            (target_id,),
        ).fetchone()
        if not row:
            raise HTTPException(status_code=404, detail="Imported paper not found")

        imported = row["added_from"] == "import" or str(row["notes"] or "").startswith("Imported from ")
        if not imported:
            raise HTTPException(status_code=400, detail="Only imported papers can be confirmed")

        if str(row["status"] or "").strip().lower() == library_app.LIBRARY_STATUS:
            return "already_saved"

        save_id, duplicate_ignored = library_app.resolve_library_save_target(db, target_id)
        if duplicate_ignored:
            library_app.sync_surface_resolution(
                db,
                target_id,
                action="save",
                source_surface="import",
            )
            library_app.sync_surface_resolution(
                db,
                save_id,
                action="save",
                source_surface="import",
            )
            return "duplicate_ignored"

        ok = library_app.add_to_library(
            db,
            save_id,
            rating=library_app.DEFAULT_LIBRARY_RATING,
            notes=None,
            added_from="import",
            override_added_from=True,
        )
        if not ok:
            raise HTTPException(status_code=404, detail="Imported paper not found")

        library_app.sync_surface_resolution(
            db,
            save_id,
            action="save",
            source_surface="import",
        )
        return "saved"

    status_value = run_write_unit(db, _persist, label="import_confirm_staged")
    paper_id_value = (
        library_app.find_library_duplicate_for_paper(db, target_id)
        if status_value == "duplicate_ignored"
        else target_id
    )
    return {"status": status_value, "paper_id": paper_id_value or target_id}


@router.post(
    "/import/resolve-openalex",
    summary="Resolve selected or unresolved publications via OpenAlex",
)
def resolve_imported_publications_openalex(
    req: ResolveImportedRequest,
    db: sqlite3.Connection = Depends(get_db),
    user: dict = Depends(get_current_user),
):
    from alma.api.scheduler import (
        activity_envelope,
        add_job_log,
        find_active_job,
        is_cancellation_requested,
        schedule_immediate,
        set_job_status,
    )
    from alma.library.enrichment import enrich_publication

    def _collect_targets(conn: sqlite3.Connection) -> list[str]:
        if req.items:
            out: list[str] = []
            seen: set[str] = set()
            for item in req.items:
                paper_id = (item.paper_id or "").strip()
                if not paper_id or paper_id in seen:
                    continue
                seen.add(paper_id)
                out.append(paper_id)
            return out

        if req.unresolved_only:
            # 40.6: source ids from the ONE resolution-queue predicate instead of
            # a hand-rolled copy that omitted the canonical-merged filter (which
            # made resolve-all waste OpenAlex calls re-resolving deduped twins).
            lim = max(1, min(int(req.limit or 1000), 10000))
            return imports_app.resolution_queue_ids(conn, unresolved_only=True, limit=lim)

        lim = max(1, min(int(req.limit or 1000), 10000))
        rows = conn.execute(
            "SELECT id FROM papers ORDER BY COALESCE(fetched_at, '') DESC LIMIT ?",
            (lim,),
        ).fetchall()
        return [r["id"] for r in rows]

    targets = _collect_targets(db)
    total = len(targets)
    if total == 0:
        return {"status": "noop", "message": "No target publications found", "total": 0}
    target_fingerprint = hashlib.sha1(
        "|".join(targets[:500]).encode("utf-8")
    ).hexdigest()[:12]
    operation_key = f"imports.resolve_openalex:{target_fingerprint}:{total}"
    existing = find_active_job(operation_key)
    if existing:
        return activity_envelope(
            str(existing.get("job_id") or ""),
            status="already_running",
            operation_key=operation_key,
            message="OpenAlex resolve already running for same workset",
            total=total,
        )

    def _run(conn: sqlite3.Connection, job_id: str) -> dict:
        enriched = 0
        skipped = 0
        failed = 0
        reasons: dict[str, int] = {}
        for idx, paper_id in enumerate(targets, start=1):
            if is_cancellation_requested(job_id):
                summary = {
                    "total": total,
                    "enriched": enriched,
                    "skipped": skipped,
                    "failed": failed,
                    "reasons": reasons,
                    "cancelled": True,
                    "processed": idx - 1,
                }
                add_job_log(job_id, "Resolve cancelled by user", step="cancelled", data=summary)
                set_job_status(
                    job_id,
                    status="cancelled",
                    finished_at=datetime.utcnow().isoformat(),
                    processed=idx - 1,
                    total=total,
                    message="Publication resolution cancelled",
                    result=summary,
                )
                return summary

            try:
                out = enrich_publication(paper_id, conn)
                if out.get("enriched"):
                    enriched += 1
                else:
                    skipped += 1
                    reason = str(out.get("reason", "unknown"))
                    reasons[reason] = reasons.get(reason, 0) + 1
            except Exception as exc:
                failed += 1
                reasons["error"] = reasons.get("error", 0) + 1
                if idx <= 5:
                    add_job_log(job_id, f"Resolve error for {paper_id}: {exc}", level="ERROR", step="resolve_item_error")
            if idx % 25 == 0 or idx == total:
                add_job_log(
                    job_id,
                    f"Resolve progress {idx}/{total} (enriched={enriched}, skipped={skipped}, failed={failed})",
                    step="resolve_progress",
                    data={"reasons": dict(reasons)},
                )
                set_job_status(job_id, status="running", processed=idx, total=total)
        summary = {"total": total, "enriched": enriched, "skipped": skipped, "failed": failed, "reasons": reasons}
        add_job_log(job_id, "Resolve completed", step="resolve_done", data=summary)
        return summary

    if not req.background:
        runner = OperationRunner(db)
        captured: dict = {}

        def _handler(_ctx):
            summary = _run(db, job_id=f"manual_resolve_inline_{uuid.uuid4().hex[:8]}")
            captured["summary"] = summary
            return OperationOutcome(
                status="completed",
                message="OpenAlex resolution completed",
                result=summary,
            )

        runner.run(
            operation_key=f"{operation_key}:inline",
            handler=_handler,
            trigger_source="user",
            actor=str(user.get("username") or "api_user"),
        )
        return {"status": "completed", "summary": captured.get("summary", {})}

    job_id = f"resolve_openalex_{uuid.uuid4().hex[:10]}"
    set_job_status(
        job_id,
        status="queued",
        operation_key=operation_key,
        trigger_source="user",
        started_at=datetime.utcnow().isoformat(),
        processed=0,
        total=total,
        message="Queued OpenAlex resolution for publications",
    )
    add_job_log(job_id, f"Queued OpenAlex resolution for {total} publications", step="queued")

    def _bg():
        conn = open_db_connection()
        try:
            set_job_status(job_id, status="running", message="Resolving publications via OpenAlex")
            summary = _run(conn, job_id=job_id)
            if not summary.get("cancelled"):
                set_job_status(
                    job_id,
                    status="completed",
                    processed=total,
                    total=total,
                    message="Publication resolution completed",
                    result=summary,
                )
        except Exception as exc:
            add_job_log(job_id, f"Resolve job failed: {exc}", level="ERROR", step="failed")
            set_job_status(job_id, status="failed", message=f"Publication resolution failed: {exc}")
        finally:
            conn.close()

    schedule_immediate(job_id, _bg)
    return activity_envelope(
        job_id,
        status="queued",
        operation_key=operation_key,
        message="Queued OpenAlex resolution for imported publications",
        total=total,
    )


# ===================================================================
# Phase C — Online source search import
# ===================================================================

@router.post(
    "/import/search",
    summary="Search online sources (OpenAlex + S2 + Crossref + arXiv + bioRxiv)",
    description=(
        "Return a list of papers matching the query across all enabled "
        "discovery sources, ranked by query relevance (cross-source RRF + "
        "query-text match — search-engine semantics). Results are "
        "cross-source deduplicated (canonical triple) and each is decorated "
        "with `in_library`, `paper_id`, `sources` (provenance chip), "
        "`relevance`, and a personal `like_score` for the why-chips. "
        "No writes."
    ),
)
def online_source_search(
    req: OnlineSearchRequest,
    db: sqlite3.Connection = Depends(get_db),
    _user: dict = Depends(get_current_user),
):
    from alma.application.openalex_manual import search_online_sources

    try:
        items = search_online_sources(
            db,
            req.query,
            limit=req.limit,
            from_year=req.year_min,
        )
    except HTTPException:
        raise
    except Exception as exc:
        logger.warning("Online source search failed for query=%r: %s", req.query, exc)
        raise HTTPException(
            status_code=502,
            detail="Upstream search failed. Try again or refine the query.",
        ) from exc

    if req.year_min is not None:
        items = [it for it in items if (it.get("year") or 0) >= req.year_min]
    if req.year_max is not None:
        items = [it for it in items if (it.get("year") or 0) <= req.year_max]

    return {
        "query": req.query,
        "filters": {"year_min": req.year_min, "year_max": req.year_max},
        "total": len(items),
        "items": items,
    }


@router.post(
    "/import/search/authors",
    response_model=list[OnlineAuthorSearchResult],
    summary="Search OpenAlex /authors for the Find & Add author scope",
    description=(
        "Returns up to `limit` author candidates matching the query. The "
        "frontend uses this when the user prefixes the query with `author:` "
        "(scope=author) so the result list shows actionable author cards "
        "with a Follow button instead of paper cards. Pure read."
    ),
)
def online_author_search(
    req: OnlineSearchRequest,
    db: sqlite3.Connection = Depends(get_db),
    _user: dict = Depends(get_current_user),
):
    from alma.application.openalex_manual import search_authors_online

    try:
        return search_authors_online(db, req.query, limit=req.limit)
    except Exception as exc:
        logger.warning("Online author search failed for query=%r: %s", req.query, exc)
        raise HTTPException(
            status_code=502,
            detail="Upstream author search failed.",
        ) from exc


class AuthorTopWorksRequest(BaseModel):
    """Resolve the two most-cited papers for a set of OpenAlex author ids."""
    openalex_ids: list[str] = Field(default_factory=list, max_length=25)
    per_author: int = Field(default=2, ge=1, le=5)


@router.post(
    "/import/search/authors/top-works",
    summary="Top-cited paper titles for a set of OpenAlex authors",
    description=(
        "Returns `{openalex_id: [titles]}` — the N most-cited works per "
        "author (one small OpenAlex /works call each, run concurrently). "
        "Split from the author-search endpoint so it can be fetched "
        "non-blocking: the search cards render immediately and the titles "
        "fill in after. Pure read."
    ),
)
def author_top_cited_works(
    req: AuthorTopWorksRequest,
    _user: dict = Depends(get_current_user),
):
    from alma.application.openalex_manual import fetch_top_cited_titles_by_author

    if not req.openalex_ids:
        return {}
    try:
        return fetch_top_cited_titles_by_author(req.openalex_ids, per_author=req.per_author)
    except Exception as exc:
        logger.warning("Author top-works fetch failed: %s", exc)
        # Best-effort enrichment — never fail the surface over missing titles.
        return {}


@router.post(
    "/import/search/stream",
    summary="Streaming variant of /import/search (NDJSON, per-source events)",
    description=(
        "Same fan-out as `/import/search` but yields per-source events as "
        "each lane returns so the UI can render skeletons → partial results "
        "→ final query-relevance-ranked list incrementally. Each newline-"
        "delimited JSON object carries a `type` field — `source_pending`, "
        "`source_partial`, `source_timeout`, `source_error`, or `final`."
    ),
)
def online_source_search_stream(
    req: OnlineSearchRequest,
    _user: dict = Depends(get_current_user),
):
    import json as _json

    def _generate():
        # Each request gets its own short-lived connection. We can't use
        # the request-scoped `Depends(get_db)` connection because the
        # generator runs after the request handler returns.
        from alma.application.openalex_manual import stream_online_sources

        conn = open_db_connection()
        try:
            for event in stream_online_sources(
                conn,
                req.query,
                limit=req.limit,
                from_year=req.year_min,
            ):
                if req.year_min is not None or req.year_max is not None:
                    if event.get("type") in ("source_partial", "final"):
                        items = event.get("items") or []
                        if req.year_min is not None:
                            items = [it for it in items if (it.get("year") or 0) >= req.year_min]
                        if req.year_max is not None:
                            items = [it for it in items if (it.get("year") or 0) <= req.year_max]
                        event = {**event, "items": items}
                yield _json.dumps(event, default=str) + "\n"
        except Exception as exc:
            logger.warning("Streaming online source search failed for query=%r: %s", req.query, exc)
            yield _json.dumps({"type": "error", "error": str(exc)}) + "\n"
        finally:
            conn.close()

    return StreamingResponse(_generate(), media_type="application/x-ndjson")


@router.post(
    "/import/search/save",
    summary="Save an online search result with the add/like/love/dislike contract",
    description=(
        "Resolves one OpenAlex work and applies the shared `3/4/5/1` rating "
        "contract. add/like/love land in Library with `added_from='online_search'`; "
        "dislike writes a negative feedback event and marks the paper dismissed "
        "(unless it's already in Library — then the library entry is preserved "
        "and only the signal is recorded)."
    ),
)
def online_source_search_save(
    req: OnlineSearchSaveRequest,
    db: sqlite3.Connection = Depends(get_db),
    _user: dict = Depends(get_current_user),
):
    from alma.application.openalex_manual import save_online_search_result

    if not any(
        str(v or "").strip()
        for v in (req.openalex_id, req.doi, req.link, req.title, req.query)
    ):
        raise HTTPException(
            status_code=400,
            detail="One of openalex_id / doi / link / title / query is required",
        )

    try:
        row = save_online_search_result(
            db,
            openalex_id=req.openalex_id,
            doi=req.doi,
            link=req.link,
            title=req.title,
            query=req.query,
            candidate=req.candidate,
            action=req.action,
            collection_name=req.collection_name,
            collection_ids=req.collection_ids,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except HTTPException:
        raise
    except Exception as exc:
        logger.warning("Online source save failed: %s", exc)
        raise HTTPException(
            status_code=502,
            detail="Upstream resolve failed. Try again.",
        ) from exc

    return {
        "paper_id": row.get("id"),
        "action": row.get("action"),
        "rating": row.get("rating"),
        "status": row.get("status"),
        "match_source": row.get("match_source"),
        "added_from": row.get("added_from"),
        "title": row.get("title"),
    }
