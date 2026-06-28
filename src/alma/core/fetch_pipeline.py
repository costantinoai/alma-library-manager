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

This primitive separates the two channels:

    items ─▶ [fetch pool: K workers]  ─▶ bounded window ─▶ [single writer]
              pure network, NO DB          backpressure       caller's thread,
              rate-limited per source                         batched write_section

Contract (read before using)
----------------------------
- ``fetch_one(item) -> result`` runs on a **worker thread**. It must do
  **network only** and **NEVER touch the DB / writer gate** — doing so
  reintroduces the cross-thread lock contention this design removes. Any
  exception it raises is captured and delivered to ``write_batch`` wrapped
  in :class:`FetchError`, so one bad item never kills the pool.
- ``write_batch(results) -> None`` runs on the **caller's thread** (the
  single writer). It owns the DB connection and wraps its own
  ``write_section``; it receives a list of completed results (a mix of
  ``fetch_one`` returns and :class:`FetchError`). All DB writes in the
  whole sweep happen here, on one thread — so there is no cross-thread
  SQLite, no new writer-gate contention, and the existing hard-kill
  cancellation (which targets the job thread) keeps working.
- ``on_progress(processed, total)`` is optional and also runs on the
  caller's thread, once per flush (throttled). Status-row writes belong
  here — never on the worker threads.

Rate limiting is **not** re-implemented here: the per-source
``SourceHttpClient`` already enforces min-interval + concurrency + 429
cooldown (S2 self-serialises to 1 RPS even inside the pool; Crossref to 3;
etc.). The fetch pool width additionally rides the running job's
``bounded_thread_pool`` fan-out budget, so a maintenance job can't exceed
its declared ceiling.

Stopping: the pipeline stops *submitting* new work when ``deadline``
elapses or ``is_cancelled()`` returns True, then drains the in-flight
futures, flushes the remainder, and returns a :class:`PipelineResult`.
``stopped_early`` tells the caller whether work remained (so it can
schedule a continuation).
"""

from __future__ import annotations

import logging
import time
from concurrent.futures import FIRST_COMPLETED, Future, wait
from dataclasses import dataclass
from typing import Any, Callable, Iterable, Optional

from alma.core.concurrency import bounded_thread_pool
from alma.core.http_sources import bind_source_diagnostics

logger = logging.getLogger(__name__)


@dataclass
class FetchError:
    """A fetch that raised — delivered to ``write_batch`` for classification.

    The writer decides whether ``error`` is retryable (defer) or terminal
    (stamp no-match); the pipeline itself never interprets it.
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


def make_deadline(seconds: Optional[float]) -> Optional[float]:
    """Return a ``time.monotonic`` deadline ``seconds`` from now (or None)."""
    if seconds is None or seconds <= 0:
        return None
    return time.monotonic() + float(seconds)


def run_fetch_write_pipeline(
    items: Iterable[Any],
    *,
    fetch_one: Callable[[Any], Any],
    write_batch: Callable[[list[Any]], None],
    fetch_workers: int = 6,
    batch_size: int = 100,
    flush_interval_s: float = 2.0,
    deadline: Optional[float] = None,
    is_cancelled: Optional[Callable[[], bool]] = None,
    on_progress: Optional[Callable[[int, int], None]] = None,
    in_flight_cap: Optional[int] = None,
    thread_name_prefix: str = "alma-fetch",
) -> PipelineResult:
    """Run ``fetch_one`` concurrently over ``items``; drain results into
    ``write_batch`` in bounded batches on the caller's thread.

    See the module docstring for the full contract. ``deadline`` is a
    ``time.monotonic()`` timestamp (use :func:`make_deadline`).
    """
    items = list(items)
    total = len(items)
    if total == 0:
        return PipelineResult(0, False, False, False)

    fetch_workers = max(1, int(fetch_workers))
    batch_size = max(1, int(batch_size))
    flush_interval_s = max(0.05, float(flush_interval_s))
    # Sliding window of in-flight fetches: enough to keep the pool fed
    # without buffering the whole backlog in memory.
    cap = max(fetch_workers, int(in_flight_cap or fetch_workers * 4))

    # Propagate the per-operation source-diagnostics collector into the
    # worker threads, and turn any fetch exception into a FetchError result
    # so a single failure never tears down the pool.
    bound_fetch = bind_source_diagnostics(fetch_one)

    def _safe_fetch(item: Any) -> Any:
        try:
            return bound_fetch(item)
        except BaseException as exc:  # noqa: BLE001 - delivered to the writer
            return FetchError(item, exc)

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
                logger.debug("fetch pipeline progress callback failed: %s", exc)

    idx = 0
    futures: set[Future] = set()
    with bounded_thread_pool(fetch_workers, thread_name_prefix=thread_name_prefix) as pool:
        # Prime the window.
        while idx < total and len(futures) < cap and not _should_stop():
            futures.add(pool.submit(_safe_fetch, items[idx]))
            idx += 1

        while futures:
            done, futures = wait(futures, timeout=flush_interval_s, return_when=FIRST_COMPLETED)
            for fut in done:
                try:
                    pending.append(fut.result())
                except BaseException as exc:  # noqa: BLE001 - defensive; _safe_fetch already traps
                    pending.append(FetchError(None, exc))
            _flush()
            # Refill the window unless we've been told to stop submitting.
            if not _should_stop():
                while idx < total and len(futures) < cap:
                    futures.add(pool.submit(_safe_fetch, items[idx]))
                    idx += 1
        # Drain whatever is left after the last in-flight fetch completed.
        _flush(force=True)

    stopped_early = idx < total or deadline_hit or cancelled
    return PipelineResult(
        processed=processed,
        stopped_early=stopped_early,
        deadline_hit=deadline_hit,
        cancelled=cancelled,
    )
