"""Canonical operation runner with lifecycle logging."""

from __future__ import annotations

import sqlite3
import uuid
from collections.abc import Callable
from typing import Any

from alma.core.db_write import run_write_unit

from .activity import persist_operation_log, persist_operation_status, write_lifecycle
from .models import OperationContext, OperationOutcome, utc_now_iso

OperationCallable = Callable[[OperationContext], dict[str, Any] | OperationOutcome | None]


class OperationRunner:
    """Run operation handlers and persist consistent lifecycle state."""

    def __init__(self, db: sqlite3.Connection):
        self._db = db

    def run(
        self,
        operation_key: str,
        handler: OperationCallable,
        *,
        trigger_source: str = "user",
        actor: str = "system",
        correlation_id: str | None = None,
        queued: bool = False,
        queued_message: str = "Operation queued",
        running_message: str = "Operation started",
        write_unit: bool = False,
    ) -> dict[str, Any]:
        """Execute one operation and return its response envelope.

        ``write_unit=True`` runs ``handler`` as one `run_write_unit`, for a
        handler that only writes this database. It must do no network I/O and
        must not open a unit of its own. The default leaves the handler
        ungated, because other handlers do network and gate their own writes.
        """
        ctx = OperationContext(
            operation_key=operation_key,
            trigger_source=trigger_source,  # type: ignore[arg-type]
            actor=actor,
            correlation_id=correlation_id or uuid.uuid4().hex,
            status="queued" if queued else "running",
            _db=self._db,
        )

        if queued:
            ctx.message = queued_message
            self._persist(ctx, step="queued", message=queued_message)

        ctx.status = "running"
        ctx.started_at = utc_now_iso()
        ctx.message = running_message
        self._persist(ctx, step="running", message=running_message)

        try:
            if write_unit:
                raw_outcome = run_write_unit(
                    self._db, lambda: handler(ctx), label=operation_key
                )
            else:
                raw_outcome = handler(ctx)
            outcome = self._normalize_outcome(raw_outcome)
            ctx.status = outcome.status
            ctx.message = outcome.message or self._default_terminal_message(outcome.status)
            ctx.result = outcome.result
            ctx.error = outcome.error
            ctx.finished_at = utc_now_iso()

            self._persist(
                ctx,
                level="ERROR" if outcome.status == "failed" else "INFO",
                step=outcome.status,
                message=ctx.message,
                data={"result": ctx.result, "error": ctx.error},
            )
            return ctx.to_envelope()
        except Exception as exc:
            ctx.status = "failed"
            ctx.finished_at = utc_now_iso()
            ctx.message = "Operation failed"
            ctx.error = {"type": exc.__class__.__name__, "message": str(exc)}
            # A failed handler's staged writes must not ride on its failure row.
            self._db.rollback()
            self._persist(
                ctx,
                level="ERROR",
                step="failed",
                message=ctx.message,
                data={"error": ctx.error},
            )
            raise

    def _persist(
        self,
        ctx: OperationContext,
        *,
        step: str,
        message: str,
        level: str = "INFO",
        data: dict[str, Any] | None = None,
    ) -> None:
        """Write one lifecycle snapshot and its log line as one gated unit."""

        def _write() -> None:
            persist_operation_status(self._db, ctx)
            persist_operation_log(
                self._db,
                operation_id=ctx.operation_id,
                level=level,
                step=step,
                message=message,
                data=data,
            )

        write_lifecycle(self._db, _write, label=f"operation {step}")

    @staticmethod
    def _default_terminal_message(status: str) -> str:
        if status == "noop":
            return "Operation had no effect"
        if status == "cancelled":
            return "Operation cancelled"
        if status == "failed":
            return "Operation failed"
        return "Operation completed"

    @staticmethod
    def _normalize_outcome(raw: dict[str, Any] | OperationOutcome | None) -> OperationOutcome:
        if isinstance(raw, OperationOutcome):
            return raw
        if raw is None:
            return OperationOutcome(status="noop", message="No changes applied", result={"changed": False})
        if isinstance(raw, dict):
            status = raw.get("status")
            if status in {"completed", "failed", "cancelled", "noop"}:
                return OperationOutcome(
                    status=status,
                    message=raw.get("message"),
                    result=raw.get("result"),
                    error=raw.get("error"),
                )
            return OperationOutcome(status="completed", result=raw)
        return OperationOutcome(status="completed", result={"value": raw})
