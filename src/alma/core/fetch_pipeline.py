"""Producer/consumer pipeline for per-item network sweeps.

Decouples the **fetch channel** (concurrent, network-only, rate-limited at
the source HTTP client) from the **write channel** (single-threaded,
batched, brief writer-gate). Built for background sweeps whose per-item
cost is dominated by a remote round-trip — title-resolution
(OpenAlex/S2 ``/works?search``), abstract recovery (landing pages), etc.

Why this exists
---------------
The naive shape — ``for item: result = fetch(item); write(result)`` — runs
fetch and write **interleaved on one thread**, so throughput is pinned to
per-item network latency (~1 item/s for OpenAlex search) and emits one
tiny write transaction (and one status row) per item. A 500-item run then
takes ~20 minutes, long enough that a uvicorn ``--reload`` orphans it
mid-flight (see ``tasks/11``).

Task 11 separated the two channels (one concurrent fetch pool → bounded
window → single writer). **Task 38 generalizes the FETCH side from one
fetch-fn into N independent source STAGES** wired as a small DAG:

    backlog ─▶ [stage 0 pool @ rate₀] ──hit──────────────────┐
                       │                                      │
                       └──miss──▶ [stage 1 pool @ rate₁] ─────┤──▶ [single writer]
                                          (fallback queue)     │      caller's thread,
                                  (stage 2, …) ────────────────┘      batched write_section

Each stage runs its **own** bounded, rate-limited pool, drawing items from
the previous stage's MISSES (the fallback queue) and emitting either an
advance-to-the-next-stage (another miss) or a terminal result for the
writer. Crucially the stages run **concurrently**, so a slow/blocked source
(an OpenAlex 429) never stalls a sibling source or the writer — item A's S2
fallback fetch runs while item B's OpenAlex fetch is still in flight. This
is the decoupling task 38 adds on top of task 11's fetch↔write split.

The WRITER stays on the **caller's job thread** (it owns the DB connection,
batches ``write_section``, and remains the hard-kill / cancellation target —
``kill_job_thread`` targets the job thread). Only the *sources* became
independent; no cross-thread SQLite was introduced.

Contract (read before using)
----------------------------
- A stage's ``fetch_one(item) -> result`` runs on a **worker thread**. It
  must do **network only** and **NEVER touch the DB / writer gate** — doing
  so reintroduces the cross-thread lock contention this design removes. Any
  exception it raises is captured and delivered to ``write_batch`` wrapped
  in :class:`FetchError`, so one bad item never kills the pool.
- A stage's ``advance_on(item, result) -> Optional[next_item]`` (pure, no
  I/O) decides routing: return the item to hand to the NEXT stage on a
  fallback-worthy miss, or ``None`` to route ``result`` to the writer (a
  terminal outcome). The LAST stage's ``advance_on`` is ignored — every
  result there is terminal. ``FetchError`` is always terminal (never
  advanced — a network error is not a "miss" to fall back on).
- ``write_batch(results) -> None`` runs on the **caller's thread** (the
  single writer). It owns the DB connection and wraps its own
  ``write_section``; it receives a list of completed terminal results (a
  mix of ``fetch_one`` returns and :class:`FetchError`). All DB writes in
  the whole sweep happen here, on one thread — so there is no cross-thread
  SQLite, no new writer-gate contention, and the existing hard-kill
  cancellation keeps working.
- ``on_progress(processed, total)`` is optional and also runs on the
  caller's thread, once per flush (throttled). Status-row writes belong
  here — never on the worker threads.

Rate limiting is **not** re-implemented here: the per-source
``SourceHttpClient`` already enforces min-interval + concurrency + 429
cooldown (S2 self-serialises to 1 RPS even inside a pool; Crossref to 3;
etc.). Each stage's pool width additionally rides the running job's
``bounded_thread_pool`` fan-out budget, so a maintenance job can't exceed
its declared ceiling.

Composition with task 37 (background-ops governance)
----------------------------------------------------
Both the **pause** (Part A — yield when the user is active) and the **credit
reserve** (Part B — leave ≥N provider calls for the user) compose through the
ONE ``is_cancelled`` seam this pipeline already consumes: task 37's
``scheduler.make_background_cancel_check`` folds the user-cancel probe, the
pause tripwire (``paused_for_user``), and the ``provider_budget_ok`` reserve
(``credit_limit``) into a single callback that a background sweep passes as
``is_cancelled``. When it trips, the pipeline stops *submitting* at the next
checkpoint, drains in-flight fetches, and returns ``cancelled=True``. Work
that had not reached a terminal write is left **unstamped** — a mid-flight
miss bound for a downstream stage is *dropped*, never written as a
half-resolved terminal — so it stays eligible and resumes when idle. A
user-triggered run never yields (the tripwire no-ops there), so its full
quota is preserved. The pipeline needs nothing source-specific for this; it
just honors whatever ``is_cancelled`` it is handed.

Stopping: the pipeline stops *submitting* new work when ``deadline``
elapses or ``is_cancelled()`` returns True, then drains the in-flight
futures, flushes the remainder, and returns a :class:`PipelineResult`.
``stopped_early`` tells the caller whether work remained (so it can
schedule a continuation).
"""

from __future__ import annotations

import logging
import time
from collections.abc import Callable, Iterable
from concurrent.futures import FIRST_COMPLETED, Future, wait
from contextlib import ExitStack
from dataclasses import dataclass, field
from typing import Any

from alma.core.concurrency import bounded_thread_pool
from alma.core.http_sources import bind_source_diagnostics

logger = logging.getLogger(__name__)


@dataclass
class FetchError:
    """A fetch that raised — delivered to ``write_batch`` for classification.

    The writer decides whether ``error`` is retryable (defer) or terminal
    (stamp no-match); the pipeline itself never interprets it. Always routed
    to the writer (never advanced to a fallback stage — a network error is
    not a content "miss").
    """

    item: Any
    error: BaseException


@dataclass
class PipelineResult:
    """Outcome of one pipeline run."""

    processed: int
    stopped_early: bool
    deadline_hit: bool
    cancelled: bool
    # Intermediate results dropped unwritten because the run stopped mid-flight
    # (42.2). They stay eligible and are retried next run — the defect was the
    # SILENCE, not the drop, so the count is surfaced for a truthful message.
    dropped: int = 0


def _always_terminal(item: Any, result: Any) -> Any | None:
    """Default ``advance_on``: every result is terminal (single-stage shape)."""
    return None


@dataclass
class FetchStage:
    """One independent source stage in a staged fetch pipeline.

    Each stage runs its OWN bounded, rate-limited worker pool. ``fetch_one``
    is network-only (see the module contract); ``advance_on`` routes each
    result to the next stage (a fallback miss) or to the writer (terminal).
    """

    # Network-only worker: ``fetch_one(item) -> result``. Never touches the DB.
    fetch_one: Callable[[Any], Any]
    # Display name (logs / thread label fallback).
    name: str = "fetch"
    # Requested pool width; clamped down to the running job's fan-out budget.
    workers: int = 6
    thread_name_prefix: str = "alma-fetch"
    # Miss router: ``(item, result) -> next_item | None``. Non-None advances
    # the item to the NEXT stage; None makes ``result`` terminal (→ writer).
    # Ignored for the last stage (everything terminal there). Default: terminal.
    advance_on: Callable[[Any, Any], Any | None] = field(default=_always_terminal)


def make_deadline(seconds: float | None) -> float | None:
    """Return a ``time.monotonic`` deadline ``seconds`` from now (or None)."""
    if seconds is None or seconds <= 0:
        return None
    return time.monotonic() + float(seconds)


def _safe_stage(fetch_one: Callable[[Any], Any]) -> Callable[[Any], Any]:
    """Wrap a stage's ``fetch_one`` with source-diagnostics binding + an
    exception trap, so one bad item becomes a :class:`FetchError` result
    instead of tearing down the pool."""
    bound = bind_source_diagnostics(fetch_one)

    def _run(item: Any) -> Any:
        try:
            return bound(item)
        except BaseException as exc:  # noqa: BLE001 - delivered to the writer
            return FetchError(item, exc)

    return _run


def run_staged_fetch_pipeline(
    items: Iterable[Any],
    *,
    stages: list[FetchStage],
    write_batch: Callable[[list[Any]], None],
    batch_size: int = 100,
    flush_interval_s: float = 2.0,
    deadline: float | None = None,
    is_cancelled: Callable[[], bool] | None = None,
    on_progress: Callable[[int, int], None] | None = None,
    in_flight_cap: int | None = None,
) -> PipelineResult:
    """Run ``items`` through a DAG of independent fetch ``stages``, draining
    terminal results into ``write_batch`` (the single writer) on the caller's
    thread. See the module docstring for the full contract.

    Stage 0 consumes the backlog; each later stage consumes the previous
    stage's misses. Every stage's pool runs concurrently, so a slow source
    never stalls a sibling source or the writer.
    """
    items = list(items)
    total = len(items)
    if total == 0:
        return PipelineResult(0, False, False, False)
    if not stages:
        raise ValueError("run_staged_fetch_pipeline requires at least one stage")

    n = len(stages)
    batch_size = max(1, int(batch_size))
    flush_interval_s = max(0.05, float(flush_interval_s))
    safe_fetch = [_safe_stage(s.fetch_one) for s in stages]

    deadline_hit = False
    cancelled = False

    def _should_stop() -> bool:
        nonlocal deadline_hit, cancelled
        if is_cancelled is not None:
            try:
                if is_cancelled():
                    cancelled = True
                    return True
            except Exception:  # cancellation probe must never crash the sweep
                pass
        if deadline is not None and time.monotonic() >= deadline:
            deadline_hit = True
            return True
        return False

    processed = 0
    dropped = 0
    pending: list[Any] = []
    last_flush = time.monotonic()

    def _flush(force: bool = False) -> None:
        nonlocal processed, pending, last_flush
        if not pending:
            return
        now = time.monotonic()
        if not (force or len(pending) >= batch_size or (now - last_flush) >= flush_interval_s):
            return
        batch = pending
        pending = []
        # The writer owns its own write_section; network is already done.
        write_batch(batch)
        processed += len(batch)
        last_flush = time.monotonic()
        if on_progress is not None:
            try:
                on_progress(processed, total)
            except Exception as exc:  # progress is best-effort
                logger.debug("staged fetch pipeline progress callback failed: %s", exc)

    # Global in-flight window across ALL stages. Bounds memory and gives
    # back-pressure: when a slow downstream stage (S2 @ 1 RPS) backs up, the
    # window fills and stage-0 refill pauses until it drains. The acceptance
    # direction (an OpenAlex 429 must not stall S2 / the writer) is unaffected —
    # that is about a slow UPSTREAM stage, and the writer flushes every loop.
    stage0_workers = max(1, int(stages[0].workers))
    cap = max(sum(max(1, int(s.workers)) for s in stages), int(in_flight_cap or stage0_workers * 4))

    futures: dict[Future, tuple[int, Any]] = {}
    pools: list = []

    def _submit(stage_i: int, item: Any) -> None:
        futures[pools[stage_i].submit(safe_fetch[stage_i], item)] = (stage_i, item)

    def _route(stage_i: int, item: Any, result: Any) -> None:
        # FetchError and last-stage results are terminal → writer. Otherwise
        # consult the stage's miss router; a non-None next-item advances to the
        # next stage — UNLESS we're stopping, in which case the intermediate
        # result is dropped unwritten so the item stays eligible and is retried
        # next run (never a half-resolved terminal stamp).
        nonlocal dropped
        nxt = stage_i + 1
        if not isinstance(result, FetchError) and nxt < n:
            advanced = stages[stage_i].advance_on(item, result)
            if advanced is not None:
                if _should_stop():
                    dropped += 1  # 42.2: count the drop so the run can report it
                    return  # don't start a NEW downstream fetch on the way out
                _submit(nxt, advanced)
                return
        pending.append(result)

    idx = 0
    with ExitStack() as stack:
        for s in stages:
            pools.append(
                stack.enter_context(
                    bounded_thread_pool(
                        max(1, int(s.workers)),
                        thread_name_prefix=s.thread_name_prefix or "alma-fetch",
                    )
                )
            )

        # Prime the stage-0 window.
        while idx < total and len(futures) < cap and not _should_stop():
            _submit(0, items[idx])
            idx += 1

        while futures:
            done, _ = wait(set(futures), timeout=flush_interval_s, return_when=FIRST_COMPLETED)
            for fut in done:
                stage_i, item = futures.pop(fut)
                try:
                    result = fut.result()
                except BaseException as exc:  # noqa: BLE001 - defensive; _safe_stage traps
                    result = FetchError(item, exc)
                _route(stage_i, item, result)
            _flush()
            # Refill the stage-0 window unless we've been told to stop submitting.
            if not _should_stop():
                while idx < total and len(futures) < cap:
                    _submit(0, items[idx])
                    idx += 1
        # Drain whatever is left after the last in-flight fetch completed.
        _flush(force=True)

    stopped_early = idx < total or deadline_hit or cancelled
    return PipelineResult(
        processed=processed,
        stopped_early=stopped_early,
        deadline_hit=deadline_hit,
        cancelled=cancelled,
        dropped=dropped,
    )


def run_fetch_write_pipeline(
    items: Iterable[Any],
    *,
    fetch_one: Callable[[Any], Any],
    write_batch: Callable[[list[Any]], None],
    fetch_workers: int = 6,
    batch_size: int = 100,
    flush_interval_s: float = 2.0,
    deadline: float | None = None,
    is_cancelled: Callable[[], bool] | None = None,
    on_progress: Callable[[int, int], None] | None = None,
    in_flight_cap: int | None = None,
    thread_name_prefix: str = "alma-fetch",
) -> PipelineResult:
    """Single-stage fetch→write pipeline (the task-11 shape).

    A thin convenience wrapper over :func:`run_staged_fetch_pipeline` with one
    terminal stage — kept so the existing single-source callers (corpus
    abstract recovery, …) read unchanged. For a cross-source fallback DAG
    (OpenAlex → S2), build explicit :class:`FetchStage` stages instead.

    See the module docstring for the full contract. ``deadline`` is a
    ``time.monotonic()`` timestamp (use :func:`make_deadline`).
    """
    return run_staged_fetch_pipeline(
        items,
        stages=[
            FetchStage(
                fetch_one=fetch_one,
                workers=fetch_workers,
                thread_name_prefix=thread_name_prefix,
            )
        ],
        write_batch=write_batch,
        batch_size=batch_size,
        flush_interval_s=flush_interval_s,
        deadline=deadline,
        is_cancelled=is_cancelled,
        on_progress=on_progress,
        in_flight_cap=in_flight_cap,
    )
