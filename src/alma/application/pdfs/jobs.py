"""Paper PDF jobs — every PDF operation is ONE enveloped background Activity job.

A route never does PDF work inside the request. It streams the upload (if
any) into the store's staging area — no database — and calls a ``request_*``
function here, which returns the canonical Activity envelope
(``activity_envelope``: job id, status, ``activity_url``). The job then runs
the use case from ``service`` on its own DB connection, logging every step to
Activity, and ends with the runner's ``message`` as its terminal line.

Operation keys (namespace ``pdf``, policy in ``core.job_policy``) make each
operation idempotent while it runs: ``pdf.attach:<paper>``,
``pdf.import:<token>``, ``pdf.fetch:<paper>``.
"""

from __future__ import annotations

import logging
import sqlite3
from collections.abc import Callable
from typing import Any

from alma.application.pdfs import store
from alma.application.pdfs.identify import IdentityHint
from alma.core.time import utcnow

logger = logging.getLogger(__name__)

#: A job body: ``(conn, log) -> result`` where ``result["message"]`` is the
#: terminal Activity line. Raising :class:`JobRefusedError` ends the job as failed
#: with a user-facing message (bad input), anything else as an error.
JobBody = Callable[[sqlite3.Connection, Callable[..., None]], dict]


class JobRefusedError(Exception):
    """The job cannot do what was asked (not a PDF, paper gone…): fail visibly."""


def _fail(job_id: str, message: str, result: dict | None = None) -> None:
    from alma.api.scheduler import set_job_status

    set_job_status(
        job_id,
        status="failed",
        finished_at=utcnow().isoformat(),
        message=message,
        error=message,
        result={"success": False, **(result or {})},
    )


def _queue(
    *,
    operation_key: str,
    job_id_prefix: str,
    queued_message: str,
    body: JobBody,
    on_already_running: Callable[[], None] | None = None,
    extra: dict[str, Any] | None = None,
) -> dict:
    """Schedule ``body`` as an Activity job and return its envelope.

    If the same operation is already running, return ITS envelope with status
    ``already_running`` (and let the caller drop what it staged).
    """
    from alma.api.deps import open_db_connection
    from alma.api.helpers import ActivityJobContext
    from alma.api.scheduler import activity_envelope, find_active_job
    from alma.core.job_envelope import schedule_with_envelope

    existing = find_active_job(operation_key)
    if existing:
        if on_already_running is not None:
            on_already_running()
        return activity_envelope(
            str(existing.get("job_id") or ""),
            status="already_running",
            operation_key=operation_key,
            message=f"{queued_message} — already running",
            **(extra or {}),
        )

    def runner_factory(job_id: str) -> Callable[[], dict | None]:
        def _run() -> dict | None:
            log = ActivityJobContext(job_id).log_step
            conn = open_db_connection()
            try:
                return body(conn, log)
            except JobRefusedError as exc:
                log("refused", str(exc), level="WARNING")
                _fail(job_id, str(exc))
                return None
            except Exception as exc:  # noqa: BLE001 — surface it in Activity, not only the log
                logger.exception("PDF job %s failed", job_id)
                log("error", f"Failed: {exc}", level="ERROR")
                _fail(job_id, f"PDF operation failed: {exc}")
                return None
            finally:
                conn.close()

        return _run

    job_id = schedule_with_envelope(
        operation_key=operation_key,
        job_id_prefix=job_id_prefix,
        trigger_source="user",
        queued_message=queued_message,
        runner_factory=runner_factory,
        log_message=queued_message,
    )
    return activity_envelope(
        job_id or "",
        status="queued",
        operation_key=operation_key,
        message=queued_message,
        **(extra or {}),
    )


# ---------------------------------------------------------------------------
# Fetch on tap
# ---------------------------------------------------------------------------


def request_fetch(*, paper_id: str, title: str) -> dict:
    """Queue ``pdf.fetch:<paper_id>``: try every enabled PDF source in order."""
    from alma.application.pdfs import service

    job_ref: dict[str, str] = {}

    def body(conn: sqlite3.Connection, log: Callable[..., None]) -> dict:
        try:
            result = service.fetch_for_paper(conn, paper_id=paper_id, job_id=job_ref.get("id"), log=log)
        except service.FetchUnavailableError as exc:
            raise JobRefusedError(str(exc)) from exc
        if result["found"]:
            message = f"PDF found via {result['source']} for “{result['title']}”"
        else:
            message = f"No PDF found for “{result['title']}” — tried {', '.join(result['tried'])}"
        return {**result, "message": message}

    envelope = _queue(
        operation_key=f"pdf.fetch:{paper_id}",
        job_id_prefix="pdf_fetch",
        queued_message=f"Finding a PDF for “{title}”",
        body=body,
        extra={"paper_id": paper_id},
    )
    job_ref["id"] = envelope["job_id"]
    return envelope


# ---------------------------------------------------------------------------
# Attach an uploaded PDF to a paper
# ---------------------------------------------------------------------------


def request_attach(*, paper_id: str, title: str, staged: store.StagedFile, filename: str) -> dict:
    """Queue ``pdf.attach:<paper_id>`` for an upload already staged by the route."""
    from alma.application.pdfs import service

    def body(conn: sqlite3.Connection, log: Callable[..., None]) -> dict:
        try:
            result = service.attach_upload(conn, paper_id=paper_id, staged=staged, filename=filename, log=log)
        except (service.NotAPdfError, LookupError) as exc:
            store.release_staged(staged)
            raise JobRefusedError(str(exc)) from exc
        note = "" if result["verification"] != "mismatch" else " — its text names a different paper"
        return {**result, "message": f"PDF attached to “{result['title']}”{note}"}

    return _queue(
        operation_key=f"pdf.attach:{paper_id}",
        job_id_prefix="pdf_attach",
        queued_message=f"Attaching {filename or 'a PDF'} to “{title}”",
        body=body,
        on_already_running=lambda: store.release_staged(staged),
        extra={"paper_id": paper_id},
    )


# ---------------------------------------------------------------------------
# PDF-first import (→ Library)
# ---------------------------------------------------------------------------


def request_import(
    *,
    staged: store.StagedFile,
    filename: str,
    user_hint: IdentityHint | None = None,
) -> dict:
    """Queue ``pdf.import:<token>`` for a staged upload (first try or a retry)."""
    from alma.application.pdfs import service

    token = store.staged_token(staged)

    def body(conn: sqlite3.Connection, log: Callable[..., None]) -> dict:
        try:
            result = service.import_upload(conn, staged=staged, filename=filename, user_hint=user_hint, log=log)
        except service.NotAPdfError as exc:
            store.release_staged(staged)
            raise JobRefusedError(str(exc)) from exc
        outcome = result["outcome"]
        if outcome == "imported":
            message = f"Imported “{result['title']}” into the Library with its PDF"
        elif outcome == "attached":
            message = f"PDF attached to “{result['title']}”, already in ALMa"
        elif outcome == "already_stored":
            message = f"This PDF is already stored for “{result['title']}”"
        else:
            message = f"Could not identify {filename or 'the PDF'} — add its DOI or title to retry"
        return {**result, "message": message}

    return _queue(
        operation_key=f"pdf.import:{token}",
        job_id_prefix="pdf_import",
        queued_message=f"Importing {filename or 'a PDF'}",
        body=body,
        # NOT "*_token": Activity redaction treats such keys as credentials.
        extra={"upload_id": token, "filename": filename},
    )
