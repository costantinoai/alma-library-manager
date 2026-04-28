"""Discovery lens routes (transport-only wrappers over application layer)."""

from __future__ import annotations

import logging
import sqlite3
import uuid

from fastapi import APIRouter, Depends, HTTPException, Query, status

from alma.api.deps import get_current_user, get_db
from alma.api.models import (
    LensBranchPreviewResponse,
    LensCreate,
    LensResponse,
    LensUpdate,
    RecommendationResponse,
)
from alma.application import discovery as discovery_app
from alma.core.operations import OperationOutcome, OperationRunner
from alma.api.helpers import raise_internal

logger = logging.getLogger(__name__)

router = APIRouter(
    dependencies=[Depends(get_current_user)],
    responses={401: {"description": "Unauthorized"}},
)


@router.get("", response_model=list[LensResponse], summary="List discovery lenses")
def list_lenses(
    is_active: bool | None = Query(default=None),
    limit: int = Query(default=100, ge=1, le=500),
    offset: int = Query(default=0, ge=0),
    db: sqlite3.Connection = Depends(get_db),
):
    try:
        return discovery_app.list_lenses(db, is_active=is_active, limit=limit, offset=offset)
    except Exception as exc:
        raise_internal("Failed to list discovery lenses", exc)


@router.post("", response_model=LensResponse, status_code=status.HTTP_201_CREATED, summary="Create a discovery lens")
def create_lens(
    body: LensCreate,
    db: sqlite3.Connection = Depends(get_db),
    user: dict = Depends(get_current_user),
):
    runner = OperationRunner(db)

    def _handler(_ctx):
        lens = discovery_app.create_lens(
            db,
            name=body.name,
            context_type=body.context_type,
            context_config=body.context_config,
            weights=body.weights,
        )
        return OperationOutcome(
            status="completed",
            message=f"Created discovery lens '{body.name}'",
            result={"lens_id": lens["id"]},
        )

    try:
        op = runner.run(
            operation_key=f"discovery.lens.create:{body.context_type}",
            handler=_handler,
            trigger_source="user",
            actor=str(user.get("username") or "api_user"),
            queued=False,
        )
        lens_id = str((op.get("result") or {}).get("lens_id") or "")
        lens = discovery_app.get_lens(db, lens_id)
        if lens is None:
            raise HTTPException(status_code=500, detail="Lens created but could not be reloaded")
        return lens
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except HTTPException:
        raise
    except Exception as exc:
        raise_internal("Failed to create discovery lens", exc)


@router.get("/{lens_id}", response_model=LensResponse, summary="Get discovery lens")
def get_lens(
    lens_id: str,
    db: sqlite3.Connection = Depends(get_db),
):
    try:
        lens = discovery_app.get_lens(db, lens_id)
        if lens is None:
            raise HTTPException(status_code=404, detail="Lens not found")
        return lens
    except HTTPException:
        raise
    except Exception as exc:
        raise_internal("Failed to load discovery lens", exc)


@router.put("/{lens_id}", response_model=LensResponse, summary="Update discovery lens")
def update_lens(
    lens_id: str,
    body: LensUpdate,
    db: sqlite3.Connection = Depends(get_db),
    user: dict = Depends(get_current_user),
):
    runner = OperationRunner(db)

    def _handler(_ctx):
        lens = discovery_app.update_lens(
            db,
            lens_id,
            name=body.name,
            context_config=body.context_config,
            weights=body.weights,
            branch_controls=body.branch_controls,
            is_active=body.is_active,
        )
        if lens is None:
            return OperationOutcome(status="noop", message="Lens not found", result={"lens_id": lens_id})
        return OperationOutcome(status="completed", message="Lens updated", result={"lens_id": lens_id})

    try:
        op = runner.run(
            operation_key=f"discovery.lens.update:{lens_id}",
            handler=_handler,
            trigger_source="user",
            actor=str(user.get("username") or "api_user"),
            queued=False,
        )
        if op["status"] == "noop":
            raise HTTPException(status_code=404, detail="Lens not found")
        lens = discovery_app.get_lens(db, lens_id)
        if lens is None:
            raise HTTPException(status_code=404, detail="Lens not found")
        return lens
    except HTTPException:
        raise
    except Exception as exc:
        raise_internal("Failed to update discovery lens", exc)


@router.delete("/{lens_id}", summary="Delete discovery lens")
def delete_lens(
    lens_id: str,
    db: sqlite3.Connection = Depends(get_db),
    user: dict = Depends(get_current_user),
):
    runner = OperationRunner(db)

    def _handler(_ctx):
        deleted = discovery_app.delete_lens(db, lens_id)
        if not deleted:
            return OperationOutcome(status="noop", message="Lens not found", result={"deleted": False})
        return OperationOutcome(status="completed", message="Lens deleted", result={"deleted": True})

    try:
        op = runner.run(
            operation_key=f"discovery.lens.delete:{lens_id}",
            handler=_handler,
            trigger_source="user",
            actor=str(user.get("username") or "api_user"),
            queued=False,
        )
        if op["status"] == "noop":
            raise HTTPException(status_code=404, detail="Lens not found")
        return {"success": True, "operation": op}
    except HTTPException:
        raise
    except Exception as exc:
        raise_internal("Failed to delete discovery lens", exc)


@router.get("/{lens_id}/signals", summary="List lens signals")
def list_lens_signals(
    lens_id: str,
    limit: int = Query(default=100, ge=1, le=500),
    db: sqlite3.Connection = Depends(get_db),
):
    try:
        lens = discovery_app.get_lens(db, lens_id)
        if lens is None:
            raise HTTPException(status_code=404, detail="Lens not found")
        signals = discovery_app.list_lens_signals(db, lens_id, limit=limit)
        return {"lens_id": lens_id, "signals": signals}
    except HTTPException:
        raise
    except Exception as exc:
        raise_internal("Failed to list lens signals", exc)


@router.get("/{lens_id}/recommendations", response_model=list[RecommendationResponse], summary="List recommendations for a lens")
def list_lens_recommendations(
    lens_id: str,
    limit: int = Query(default=100, ge=1, le=500),
    offset: int = Query(default=0, ge=0),
    db: sqlite3.Connection = Depends(get_db),
):
    try:
        lens = discovery_app.get_lens(db, lens_id)
        if lens is None:
            raise HTTPException(status_code=404, detail="Lens not found")
        rows = discovery_app.list_lens_recommendations(db, lens_id, limit=limit, offset=offset)
        return [RecommendationResponse(**r) for r in rows]
    except HTTPException:
        raise
    except Exception as exc:
        raise_internal("Failed to list lens recommendations", exc)


@router.get(
    "/{lens_id}/branches",
    response_model=LensBranchPreviewResponse,
    summary="Preview branch map for a lens",
)
def preview_lens_branches(
    lens_id: str,
    max_branches: int = Query(default=6, ge=2, le=12),
    temperature: float | None = Query(default=None, ge=0.0, le=1.0),
    db: sqlite3.Connection = Depends(get_db),
):
    try:
        preview = discovery_app.preview_lens_branches(
            db,
            lens_id,
            max_branches=max_branches,
            temperature=temperature,
        )
        if preview is None:
            raise HTTPException(status_code=404, detail="Lens not found")
        return LensBranchPreviewResponse(**preview)
    except HTTPException:
        raise
    except Exception as exc:
        raise_internal("Failed to build lens branch preview", exc)


@router.post("/{lens_id}/refresh", summary="Refresh recommendations for a lens")
def refresh_lens(
    lens_id: str,
    limit: int = Query(default=50, ge=1, le=200),
    db: sqlite3.Connection = Depends(get_db),
    user: dict = Depends(get_current_user),
):
    """Queue a lens refresh on the APS scheduler pool.

    Per the Activity-envelope migration pattern (see `tasks/lessons.md`): the
    request thread validates synchronously (404 on missing lens, dedup via
    `find_active_job`), seeds `operation_status`, schedules the runner, and
    returns the queued envelope in ~100 ms. The runner opens its own SQLite
    connection (don't reuse the request-scoped `db` inside the scheduler
    thread) and writes the final `result=` payload via `set_job_status`.

    Frontends poll `GET /activity/{job_id}` for status; `useOperationToasts`
    auto-invalidates `lens-recommendations` on `discovery.*` completion, so
    no per-call cache wiring is needed.
    """
    from datetime import datetime as _dt

    from alma.api.deps import open_db_connection
    from alma.api.scheduler import (
        activity_envelope,
        find_active_job,
        schedule_immediate,
        set_job_status,
    )

    try:
        lens_row = discovery_app.get_lens(db, lens_id)
    except Exception:
        lens_row = None
    if lens_row is None:
        raise HTTPException(status_code=404, detail="Lens not found")
    lens_label = f"'{lens_row['name']}'"

    operation_key = f"discovery.lens.refresh:{lens_id}"
    existing = find_active_job(operation_key)
    if existing:
        return activity_envelope(
            str(existing.get("job_id") or ""),
            status="already_running",
            operation_key=operation_key,
            message=f"Lens refresh ({lens_label}) already running",
            lens_id=lens_id,
        )

    job_id = f"lens_refresh_{uuid.uuid4().hex[:10]}"
    set_job_status(
        job_id,
        status="queued",
        operation_key=operation_key,
        trigger_source="user",
        started_at=_dt.utcnow().isoformat(),
        message=f"Lens refresh ({lens_label}): queued",
        processed=0,
        total=int(limit),
    )

    actor = str(user.get("username") or "api_user")

    def _runner():
        # Scheduler thread owns its own connection — never reuse the
        # request-scoped `db` here (see lessons → "Activity envelope migration
        # pattern" point 4).
        runner_conn = open_db_connection()
        try:
            class _LogCtx:
                """Minimal `ctx` shim that forwards `log_step` to operation_status."""

                def log_step(self, step: str, message: str, **kwargs):  # noqa: ARG002
                    processed = kwargs.get("processed")
                    total = kwargs.get("total")
                    fields: dict = {"message": message}
                    if processed is not None:
                        fields["processed"] = int(processed)
                    if total is not None:
                        fields["total"] = int(total)
                    set_job_status(job_id, status="running", **fields)

            ctx = _LogCtx()
            ctx.log_step(
                "load_lens",
                f"Lens refresh ({lens_label}): loading lens and seed papers",
            )
            result = discovery_app.refresh_lens_recommendations(
                runner_conn,
                lens_id,
                trigger_source="user",
                limit=limit,
                ctx=ctx,
            )
            runner_conn.commit()

            if result is None:
                # Race: lens deleted between the pre-check and the runner.
                set_job_status(
                    job_id,
                    status="failed",
                    finished_at=_dt.utcnow().isoformat(),
                    message=f"Lens {lens_label} not found",
                    error="lens_not_found",
                    result={"lens_id": lens_id, "reason": "lens_not_found"},
                )
                return

            inserted = int(result.get("inserted") or 0)
            terminal_message = (
                f"Lens {lens_label}: {inserted} recommendations generated"
                if inserted
                else f"Lens {lens_label}: no new recommendations"
            )
            set_job_status(
                job_id,
                status="completed",
                finished_at=_dt.utcnow().isoformat(),
                processed=inserted,
                total=int(limit),
                message=terminal_message,
                result=result,
            )
        except Exception as exc:
            logger.exception("Lens refresh runner failed for %s", lens_id)
            set_job_status(
                job_id,
                status="failed",
                finished_at=_dt.utcnow().isoformat(),
                message=f"Lens refresh ({lens_label}) failed",
                error=str(exc),
            )
        finally:
            try:
                runner_conn.close()
            except Exception:
                pass

    try:
        schedule_immediate(job_id, _runner)
    except Exception as exc:
        raise_internal("Failed to schedule lens refresh", exc)

    return activity_envelope(
        job_id,
        status="queued",
        operation_key=operation_key,
        message=f"Lens refresh ({lens_label}) queued",
        lens_id=lens_id,
        actor=actor,
    )
