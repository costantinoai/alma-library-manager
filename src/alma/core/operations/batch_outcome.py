"""What a batch job actually achieved, for its terminal Activity row.

A sweep walks many items and keeps going past one that fails, so returning
normally proves nothing about how the batch went. A runner states it by putting
``batch_outcome(...)`` in its result under the key ``"batch"``;
``set_job_status`` then settles the terminal row from it, so a self-stamping
sweep and one finalized by ``schedule_immediate`` cross the same rule:

* nothing failed: ``completed``.
* some failed: still ``completed``, with a message that opens ``Partial:`` and
  gives the counts, and ``result.batch.state == "partial"`` for the UI. There is
  no ``partial`` status on purpose: every reader of the ledger (Home's pills,
  ``last_success_at``, Health's failed-job count) treats ``completed`` as "ran",
  and a partial sweep is routine, its failures are retried by a later run.
* every attempted item failed: ``failed``.
"""

from __future__ import annotations

from typing import Any, Literal

BatchState = Literal["clean", "partial", "failed"]


def batch_outcome(
    *,
    attempted: int,
    failed: int,
    noun: str,
    incomplete: int = 0,
) -> dict[str, Any]:
    """Describe one batch. Goes in the job's result as ``result["batch"]``.

    ``attempted`` counts items the run actually tried, not what it planned to
    reach, so a cancelled run is not reported against its whole backlog.
    ``failed`` items achieved nothing. ``incomplete`` items did their main work
    and missed a later step: they make the batch partial, never failed.
    """
    state: BatchState = "clean"
    if attempted > 0 and failed >= attempted:
        state = "failed"
    elif failed or incomplete:
        state = "partial"
    return {
        "attempted": attempted,
        "succeeded": max(0, attempted - failed),
        "failed": failed,
        "incomplete": incomplete,
        "noun": noun,
        "state": state,
    }


def _headline(batch: dict[str, Any]) -> str:
    """The one line that says how the batch went."""
    noun = batch["noun"]
    done = f"{batch['succeeded']} of {batch['attempted']} {noun} done"
    if batch["state"] == "failed":
        return f"All {batch['attempted']} {noun} failed"
    if batch["state"] == "clean":
        return done
    parts = [done]
    if batch["failed"]:
        parts.append(f"{batch['failed']} failed")
    if batch["incomplete"]:
        parts.append(f"{batch['incomplete']} incomplete")
    return "Partial: " + ", ".join(parts)


def settle_batch_fields(fields: dict[str, Any]) -> dict[str, Any]:
    """Apply a batch outcome to the fields of a terminal ``completed`` write.

    Only ``completed`` is settled: a run that was cancelled or raised has already
    said what happened to it. Fields without a batch pass through unchanged.
    """
    result = fields.get("result")
    batch = result.get("batch") if isinstance(result, dict) else None
    if fields.get("status") != "completed" or not isinstance(batch, dict):
        return fields

    settled = dict(fields)
    headline = _headline(batch)
    message = str(fields.get("message") or "").strip()
    if batch["state"] == "clean":
        settled["message"] = message or headline
        return settled
    settled["message"] = f"{headline} · {message}" if message else headline
    if batch["state"] == "failed":
        settled["status"] = "failed"
        settled["error"] = headline
    return settled
