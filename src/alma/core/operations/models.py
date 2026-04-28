"""Typed models for operation lifecycle tracking."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Literal
import uuid


OperationStatus = Literal[
    "queued",
    "running",
    "completed",
    "failed",
    "cancelled",
    "noop",
]

TerminalOperationStatus = Literal["completed", "failed", "cancelled", "noop"]
TriggerSource = Literal["user", "scheduler", "system"]


def utc_now_iso() -> str:
    """Return current UTC timestamp in ISO-8601 format."""
    return datetime.utcnow().isoformat()


@dataclass(slots=True)
class OperationContext:
    """Canonical operation context shared across layers."""

    operation_key: str
    trigger_source: TriggerSource
    actor: str
    correlation_id: str
    operation_id: str = field(default_factory=lambda: uuid.uuid4().hex)
    requested_at: str = field(default_factory=utc_now_iso)
    started_at: str | None = None
    finished_at: str | None = None
    status: OperationStatus = "queued"
    message: str | None = None
    result: dict[str, Any] | None = None
    error: dict[str, Any] | None = None
    _db: Any = field(default=None, repr=False)

    def log_step(
        self,
        step: str,
        message: str,
        *,
        data: dict[str, Any] | None = None,
        processed: int | None = None,
        total: int | None = None,
    ) -> None:
        """Log an intermediate step and update the operation's status message.

        Call from inside operation handlers to provide visible progress.
        """
        if self._db is None:
            return
        from .activity import persist_operation_log, persist_operation_status

        self.message = message
        persist_operation_status(self._db, self, processed=processed, total=total)
        persist_operation_log(
            self._db,
            operation_id=self.operation_id,
            step=step,
            message=message,
            data=data,
        )
        try:
            self._db.commit()
        except Exception:
            pass

    def to_envelope(self) -> dict[str, Any]:
        """Serialize operation state to a response envelope."""
        return {
            "job_id": self.operation_id,
            "operation_id": self.operation_id,
            "operation_key": self.operation_key,
            "trigger_source": self.trigger_source,
            "actor": self.actor,
            "requested_at": self.requested_at,
            "started_at": self.started_at,
            "finished_at": self.finished_at,
            "status": self.status,
            "message": self.message,
            "result": self.result,
            "error": self.error,
            "correlation_id": self.correlation_id,
            "activity_url": f"/api/v1/activity/{self.operation_id}/logs",
        }


@dataclass(slots=True)
class OperationOutcome:
    """Normalized terminal outcome returned by operation handlers."""

    status: TerminalOperationStatus = "completed"
    message: str | None = None
    result: dict[str, Any] | None = None
    error: dict[str, Any] | None = None
