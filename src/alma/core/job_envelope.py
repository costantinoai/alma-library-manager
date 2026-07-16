"""Idempotent Activity-job envelope.

Every background sweep (paper hydration, author hydration, S2 vector
backfill, local SPECTER2 fill) shares the same lifecycle:

1. Look up an active job for ``operation_key`` — if one is in flight,
   return its id (idempotent against double-fire).
2. Otherwise mint a new ``{prefix}_{nonce}`` job id, stamp it with
   ``set_job_status(status='queued', operation_key=...)``, log the
   queued event, and call ``schedule_immediate`` with the runner.

The scheduler module is imported lazily so this helper stays importable
from CLI / test contexts where the FastAPI app isn't wired. Returns
``None`` when the scheduler can't be loaded; callers treat that as a
no-op.
"""
from __future__ import annotations

import hashlib
import uuid
from collections.abc import Callable, Iterable, Mapping
from typing import Any


def target_scoped_operation_key(
    operation_key: str,
    target_ids: Iterable[str] | None,
    *,
    scope: str = "target",
) -> str:
    """Return a stable target-scoped key, or the base key for bulk work.

    Bulk jobs and targeted action jobs must not dedup each other: otherwise a
    broad backlog drain can hide a newly inserted or followed target. The target
    set is sorted so callers get the same key for the same set regardless of
    order, while Activity payloads can still preserve the original display order.
    """
    ids = sorted({str(item).strip() for item in (target_ids or []) if str(item).strip()})
    if not ids:
        return operation_key
    digest = hashlib.sha1("|".join(ids).encode("utf-8")).hexdigest()[:12]
    return f"{operation_key}:{scope}:{digest}"


def schedule_with_envelope(
    *,
    operation_key: str,
    job_id_prefix: str,
    trigger_source: str,
    queued_message: str,
    runner_factory: Callable[[str], Callable[..., Any]],
    chain_id: str | None = None,
    chain_step: str | None = None,
    log_step: str = "queued",
    log_message: str | None = None,
    log_data: Mapping[str, Any] | None = None,
    extra_status_fields: Mapping[str, Any] | None = None,
) -> str | None:
    """Schedule ``runner_factory(job_id)`` under an idempotent Activity job.

    Returns the active or newly-queued ``job_id``; returns ``None`` when
    the scheduler is not importable (e.g., during tests or CLI tools).

    Optional kwargs let callers stamp:

    - ``chain_id`` / ``chain_step`` on the Activity row (chain coordinator).
    - extra ``set_job_status`` kwargs via ``extra_status_fields`` —
      sweep callers use this for ``started_at`` / ``processed`` / ``total``.
    - extra ``add_job_log`` payload via ``log_data``.
    - a custom ``log_message`` / ``log_step`` for the queued log event.
    """
    try:
        from alma.api.scheduler import (
            add_job_log,
            find_active_job,
            schedule_immediate,
            set_job_status,
        )
    except Exception:
        return None

    existing = find_active_job(operation_key)
    if existing:
        return str(existing.get("job_id") or "") or None

    job_id = f"{job_id_prefix}_{uuid.uuid4().hex[:10]}"

    status_kwargs: dict[str, Any] = {
        "status": "queued",
        "operation_key": operation_key,
        "trigger_source": trigger_source,
        "message": queued_message,
    }
    if chain_id is not None:
        status_kwargs["chain_id"] = chain_id
    if chain_step is not None:
        status_kwargs["chain_step"] = chain_step
    if extra_status_fields:
        status_kwargs.update(extra_status_fields)
    set_job_status(job_id, **status_kwargs)

    log_payload: dict[str, Any] = {}
    if chain_id is not None:
        log_payload["chain_id"] = chain_id
    if chain_step is not None:
        log_payload["chain_step"] = chain_step
    if log_data:
        log_payload.update(log_data)
    add_job_log(
        job_id,
        log_message or "Auto-queued",
        step=log_step,
        data=log_payload,
    )

    schedule_immediate(job_id, runner_factory(job_id))
    return job_id
