"""Per-job nested fan-out budgeting (task-29 follow-up).

`api.scheduler._scheduler_max_workers` caps how many BACKGROUND JOBS run at once
(default 5) — the bound that keeps a burst of jobs from monopolising SQLite's
single writer. But each job may itself fan out across a nested
`ThreadPoolExecutor` (discovery retrieval lanes, S2 `/paper/batch`, per-author
works expansion, library enrichment). Nothing bounded that inner width, so N
concurrent jobs each spawning a 12-worker pool put N×12 threads on the network /
the writer at once.

This module bounds that *inner* fan-out to the running job's policy budget
(`JobPolicy.fanout_budget`) — but ONLY when the code runs inside a background
job. Interactive request-path fan-out (a user clicking Discover / Find & Add)
keeps its full width, because there latency, not writer contention, is what
matters.

Mechanism (forward-only, no shims):

- The scheduler binds the running job's budget to a `contextvars.ContextVar`
  for the duration of the runner (`enter_job_fanout`).
- `bounded_thread_pool` is a drop-in for `ThreadPoolExecutor(max_workers=N)` at
  every nested fan-out site. It reads the contextvar and clamps `max_workers`;
  off the job path the var is unset and nothing is clamped (true no-op).
- The pool's worker `initializer` re-publishes the budget into each worker
  thread, because contextvars do NOT auto-propagate across threads — so a
  *nested* pool created inside a fanned-out task (feed refresh → per-monitor
  search → multi-source search) still inherits the same ceiling.

The durable win is the contract, not the (deliberately conservative) clamp:
every fan-out site funnels through one greppable primitive, and a structural
test fails CI if a new raw `ThreadPoolExecutor(max_workers=…)` appears in a
fan-out module without going through here.
"""

from __future__ import annotations

import contextvars
from collections.abc import Iterator
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager

from alma.core.job_policy import policy_for

# The running background job's nested fan-out budget, or None on the interactive
# request path (where no clamp applies). Default None = "no active job context".
_job_fanout_budget: contextvars.ContextVar[int | None] = contextvars.ContextVar(
    "alma_job_fanout_budget", default=None
)


@contextmanager
def enter_job_fanout(operation_key: str | None) -> Iterator[None]:
    """Bind the current (scheduler worker) thread to its policy's fan-out budget.

    Called by the scheduler around a background job's runner. The budget is
    resolved from the job-policy catalog by the job's ``operation_key``
    namespace; a job with no operation_key or an unclassified namespace leaves
    the budget unset, so its fan-out is not clamped (fail-open — we never want
    budgeting to be the thing that breaks an un-cataloged job).
    """
    policy = policy_for(operation_key) if operation_key else None
    budget = policy.fanout_budget if policy is not None else None
    token = _job_fanout_budget.set(budget)
    try:
        yield
    finally:
        _job_fanout_budget.reset(token)


def current_fanout_budget() -> int | None:
    """The active per-job fan-out budget, or None on the interactive path."""
    return _job_fanout_budget.get()


def bounded_max_workers(requested: int) -> int:
    """Clamp a requested pool width to the running job's fan-out budget.

    Off the background-job path (budget unset) the request passes through
    unchanged. Always returns at least 1.
    """
    requested = max(1, int(requested))
    budget = _job_fanout_budget.get()
    if budget is None:
        return requested
    return max(1, min(requested, int(budget)))


def _publish_budget(budget: int | None) -> None:
    """`ThreadPoolExecutor` initializer: re-bind the captured budget in a worker
    thread so a *nested* `bounded_thread_pool` created inside a fanned-out task
    inherits the same ceiling (contextvars don't cross the thread boundary)."""
    _job_fanout_budget.set(budget)


def bounded_thread_pool(
    requested_workers: int, *, thread_name_prefix: str = ""
) -> ThreadPoolExecutor:
    """A `ThreadPoolExecutor` whose width is clamped to the running job's
    fan-out budget. Drop-in replacement for `ThreadPoolExecutor(max_workers=N)`
    at every nested fan-out site; a true no-op (full requested width) off the
    background-job path."""
    budget = _job_fanout_budget.get()
    workers = bounded_max_workers(requested_workers)
    return ThreadPoolExecutor(
        max_workers=workers,
        thread_name_prefix=thread_name_prefix or "alma-fanout",
        # Propagate the budget so nested pools created by these workers stay capped.
        initializer=_publish_budget,
        initargs=(budget,),
    )
