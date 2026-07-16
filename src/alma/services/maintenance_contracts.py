"""Typed contracts shared by maintenance planning, runners, API, and UI.

This module deliberately contains no database or scheduler code.  It is the
stable vocabulary for the maintenance subsystem; callers must not reinterpret
limits, safety, order, or lifecycle state independently.
"""

from __future__ import annotations

import hashlib
import json
from collections.abc import Callable
from dataclasses import dataclass, field
from enum import StrEnum
from threading import Lock
from typing import Any

from pydantic import BaseModel, ConfigDict, Field, field_validator


class MaintenanceValidationError(ValueError):
    """A user/config value violates the task's declared contract."""


class MaintenanceStage(StrEnum):
    AUTHOR_IDENTITY = "author_identity"
    AUTHOR_CANONICALIZATION = "author_canonicalization"
    AUTHOR_WORKS = "author_works"
    PAPER_IDENTITY = "paper_identity"
    PAPER_METADATA = "paper_metadata"
    PAPER_CANONICALIZATION = "paper_canonicalization"
    REMOTE_VECTORS = "remote_vectors"
    LOCAL_EMBEDDINGS = "local_embeddings"
    DERIVED = "derived"
    CLEANUP = "cleanup"
    HOUSEKEEPING = "housekeeping"


class MaintenanceUnit(StrEnum):
    PAPER = "paper"
    AUTHOR = "author"
    AUTHOR_SOURCE_ATTEMPT = "author_source_attempt"
    PAIR = "pair"
    LOOKUP_ID = "lookup_id"
    ARTIFACT = "artifact"
    OPERATION = "operation"


class TargetKind(StrEnum):
    PAPER = "paper"
    AUTHOR = "author"
    PAIR = "pair"
    NONE = "none"


class MaintenanceTrigger(StrEnum):
    USER = "user"
    ACTION = "action"
    IDLE = "idle"
    SCHEDULER = "scheduler"


@dataclass(frozen=True, slots=True)
class BatchSpec:
    """One upstream endpoint's request-payload bound."""

    unit: MaintenanceUnit
    default: int
    maximum: int

    def validate(self, value: int | None) -> int:
        chosen = self.default if value is None else int(value)
        if chosen < 1 or chosen > self.maximum:
            raise MaintenanceValidationError(
                f"request_batch_size must be between 1 and {self.maximum}"
            )
        return chosen


@dataclass(frozen=True, slots=True)
class ScopeSpec:
    options: tuple[str, ...]
    default: str

    def validate(self, value: str | None) -> str:
        chosen = (value or self.default).strip()
        if chosen not in self.options:
            raise MaintenanceValidationError(
                f"scope must be one of: {', '.join(self.options)}"
            )
        return chosen


class MaintenanceRunSpec(BaseModel):
    """Atomic invocation sent by UI/API and consumed unchanged by the runner."""

    model_config = ConfigDict(extra="forbid")

    trigger: MaintenanceTrigger = MaintenanceTrigger.USER
    scope: str | None = None
    target_ids: list[str] = Field(default_factory=list)
    max_items: int = Field(ge=1)
    request_batch_size: int | None = Field(default=None, ge=1)
    dry_run: bool = False
    force: bool = False
    confirmation_token: str | None = None
    plan_fingerprint: str | None = None

    @field_validator("target_ids")
    @classmethod
    def _normalize_targets(cls, values: list[str]) -> list[str]:
        # Preserve display/claim order while removing blanks and duplicates.
        return list(dict.fromkeys(str(value).strip() for value in values if str(value).strip()))


@dataclass(frozen=True, slots=True)
class PlanDependency:
    key: str
    label: str
    pending: int
    required: bool = True


@dataclass(frozen=True, slots=True)
class StageBudget:
    stage: str
    allocated: int
    unit: MaintenanceUnit


@dataclass(frozen=True, slots=True)
class MaintenanceRunPlan:
    task_key: str
    spec: MaintenanceRunSpec
    pending: int
    selected: int
    unit: MaintenanceUnit
    dependencies: tuple[PlanDependency, ...] = ()
    expected_requests: dict[str, int] = field(default_factory=dict)
    stage_allocations: tuple[StageBudget, ...] = ()
    fingerprint: str = ""
    confirmation_token: str | None = None
    # ETA for the BOUNDED run (computed from ``selected``, not the whole backlog),
    # so estimate and launch read one number that reflects ``max_items``. ``None``
    # for local-compute / nothing-pending tasks. This is the single source the
    # ``/estimate`` endpoint returns — no secondary recomputation downstream.
    eta: dict[str, Any] | None = None

    def to_wire(self) -> dict[str, Any]:
        return {
            "task_key": self.task_key,
            "spec": self.spec.model_dump(mode="json"),
            "candidates_pending": self.pending,
            "selected_items": self.selected,
            "unit": self.unit.value,
            "dependencies": [
                {
                    "key": dep.key,
                    "label": dep.label,
                    "pending": dep.pending,
                    "required": dep.required,
                }
                for dep in self.dependencies
            ],
            "expected_requests": dict(self.expected_requests),
            "stage_allocations": [
                {"stage": row.stage, "allocated": row.allocated, "unit": row.unit.value}
                for row in self.stage_allocations
            ],
            "plan_fingerprint": self.fingerprint,
            "confirmation_token": self.confirmation_token,
            "eta": self.eta,
        }


def fingerprint_plan(payload: dict[str, Any]) -> str:
    encoded = json.dumps(payload, sort_keys=True, separators=(",", ":"), default=str)
    return hashlib.sha256(encoded.encode("utf-8")).hexdigest()[:24]


class SharedBudget:
    """Thread-safe total logical-run budget shared by phases/continuations."""

    def __init__(self, total: int, unit: MaintenanceUnit):
        self.total = max(0, int(total))
        self.unit = unit
        self._claimed = 0
        self._lock = Lock()

    @property
    def claimed(self) -> int:
        with self._lock:
            return self._claimed

    @property
    def remaining(self) -> int:
        with self._lock:
            return max(0, self.total - self._claimed)

    @property
    def exhausted(self) -> bool:
        return self.remaining == 0

    def claim(self, requested: int) -> int:
        with self._lock:
            amount = min(max(0, int(requested)), max(0, self.total - self._claimed))
            self._claimed += amount
            return amount


@dataclass(slots=True)
class MaintenanceRunResult:
    selected: int = 0
    attempted: int = 0
    changed: int = 0
    unchanged: int = 0
    terminal: int = 0
    retryable: int = 0
    failed: int = 0
    remaining: int = 0
    actual_requests: dict[str, int] = field(default_factory=dict)
    stage_results: list[dict[str, Any]] = field(default_factory=list)
    message: str = ""

    def to_wire(self) -> dict[str, Any]:
        return {
            "selected": self.selected,
            "attempted": self.attempted,
            "changed": self.changed,
            "unchanged": self.unchanged,
            "terminal": self.terminal,
            "retryable": self.retryable,
            "failed": self.failed,
            "remaining": self.remaining,
            "actual_requests": dict(self.actual_requests),
            "stage_results": list(self.stage_results),
            "message": self.message,
        }


MaintenanceRunner = Callable[..., dict[str, Any] | None]
CountFn = Callable[..., int]


@dataclass(frozen=True, slots=True)
class MaintenanceTask:
    """One backend-owned maintenance operation and its complete UI contract."""

    key: str
    label: str
    description: str
    stage: MaintenanceStage
    order: int
    unit: MaintenanceUnit
    runner: MaintenanceRunner
    health_dimensions: tuple[str, ...]
    candidate_path: str
    operation_key: str
    job_id_prefix: str
    cost: str
    target_kind: TargetKind = TargetKind.NONE
    supports_targets: bool = False
    prerequisites: tuple[str, ...] = ()
    unlocks: tuple[str, ...] = ()
    optional: bool = False
    manual_gate: bool = False
    default_auto_enabled: bool = False
    default_auto_daily_cap: int = 200
    max_auto_daily_cap: int = 100_000
    default_manual_limit: int = 200
    max_manual_limit: int = 5_000
    auto_chunk_size: int = 50
    sources: tuple[str, ...] = ()
    local_compute: bool = False
    destructive: bool = False
    count_fn: CountFn | None = None
    scope: ScopeSpec | None = None
    supports_dry_run: bool = False
    supports_force: bool = False
    request_batch: BatchSpec | None = None
    eta_key: str | None = None

    def validate_max_items(self, value: int) -> int:
        chosen = int(value)
        if chosen < 1 or chosen > self.max_manual_limit:
            raise MaintenanceValidationError(
                f"max_items must be between 1 and {self.max_manual_limit} for {self.key}"
            )
        return chosen

    def validate_auto_daily_cap(self, value: int) -> int:
        chosen = int(value)
        if chosen < 1 or chosen > self.max_auto_daily_cap:
            raise MaintenanceValidationError(
                f"auto_daily_cap must be between 1 and {self.max_auto_daily_cap} for {self.key}"
            )
        return chosen

