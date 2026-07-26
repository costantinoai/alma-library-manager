"""Canonical SQLite write unit: writer gate + BEGIN IMMEDIATE + retry.

Why this module exists — SQLite is single-writer, and ALMa runs foreground
HTTP writes alongside background runner threads over one database file.
Three findings from the 2026-06 "database is locked" audit:

1. Python's sqlite3 default (deferred) transactions can fail with
   SQLITE_BUSY *instantly*, ignoring ``busy_timeout``, when a transaction
   that started reading tries to upgrade to a write while another writer
   is active (waiting would break snapshot isolation). The fix the
   ecosystem converged on (Rails 8 default, Django 5.1
   ``transaction_mode="IMMEDIATE"``) is to open write transactions with
   ``BEGIN IMMEDIATE`` so the write lock is taken up front — then
   ``busy_timeout`` applies and the upgrade hazard is gone.

2. ``busy_timeout`` polling is not a fair queue: a busy background writer
   that commits and immediately re-acquires can starve a foreground write
   for the whole timeout. Serializing writers *in the application* makes
   write-write SQLITE_BUSY structurally impossible within the process —
   and costs no real parallelism, because SQLite serializes writers at the
   engine level anyway. ALMa runs a single uvicorn worker, so one
   process-wide gate covers all contention; ``busy_timeout`` + retry stay
   as the cross-process belt (CLI scripts, future multi-worker).

3. Reads need no gate: WAL gives readers MVCC snapshots that never block
   on the writer. Only the few-milliseconds write windows serialize.

Usage::

    from alma.core.db_write import run_write_unit

    def _unit() -> str:
        db.execute("INSERT ...", (...,))
        return some_id

    result = run_write_unit(db, _unit, label="follow_author")

Side effects that write through a SECOND connection (job scheduling, Activity
rows) need one of the two guards, because a second connection cannot take the
write lock while this thread holds it:

* :func:`run_after_gate_release` — caller side. Defers past the enclosing
  unit's commit, or commits the caller's ungated transaction first. It takes
  the caller's connection because **an open transaction, not the gate, is the
  hazard** (2026-07-26 audit; see ``tasks/lessons.md``).
* :func:`run_gated_or_deferred` — callee side, for a writer that owns its own
  connection. Makes the writer queue on the gate instead of busy-polling
  SQLite, so a call site that forgets the guard degrades to a wait, not a
  dropped row.

The unit function performs writes on an open connection and returns a
value; ``run_write_unit`` wraps it in gate → rollback → BEGIN IMMEDIATE →
unit → commit, retried via :func:`alma.core.db_retry.run_with_lock_retry`.
The unit must be safe to re-run on a clean transaction (same contract as
``run_with_lock_retry``). Keep network I/O OUT of units — gather first,
then write (see ``tasks/lessons.md`` → SQLite write discipline).
"""

from __future__ import annotations

import logging
import sqlite3
import threading
import time
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from typing import TypeVar

from alma.core.db_retry import commit_with_retry, run_with_lock_retry

logger = logging.getLogger(__name__)

T = TypeVar("T")

# One gate for the whole process. Not an RLock on purpose: a unit that
# tries to nest another unit is a design error (it would hold the writer
# across the inner unit's retries) and should deadlock loudly in dev
# rather than silently serialize twice.
_WRITER_GATE = threading.Lock()

# Per-thread flag + deferred-callback stack for run_after_gate_release().
# `held` is True between gate acquire and release on THIS thread;
# `deferred` collects callbacks that must not run while the gate (and the
# SQLite write lock) is held — they execute right after release.
_GATE_STATE = threading.local()


def gate_held_by_current_thread() -> bool:
    """True while the calling thread is inside a write unit / section."""
    return bool(getattr(_GATE_STATE, "held", False))


def _defer(fn: Callable[[], None]) -> None:
    """Queue ``fn`` on this thread's post-gate-release stack."""
    queue = getattr(_GATE_STATE, "deferred", None)
    if queue is None:
        queue = []
        _GATE_STATE.deferred = queue
    queue.append(fn)


def run_after_gate_release(
    fn: Callable[[], None],
    *,
    conn: sqlite3.Connection,
    label: str = "deferred callback",
) -> None:
    """Run ``fn`` only once THIS thread's SQLite write lock is released.

    ``fn`` is a side effect that writes through a **second connection** — job
    scheduling (``schedule_with_envelope`` → ``find_active_job`` /
    ``set_job_status`` / ``add_job_log``) persists Activity state on the
    scheduler's own connection. A second connection cannot take the write lock
    while this thread still holds it, so firing ``fn`` too early does not fail
    fast: it busy-waits for the whole ``busy_timeout`` and then drops the row.

    There are TWO ways this thread can be holding the write lock, and both are
    checked here — the second one is why ``conn`` is a required argument:

    1. **The process writer gate is held by this thread** — we are inside a
       :func:`run_write_unit` / :func:`write_section`. Defer ``fn`` to the
       drain that runs right after the unit commits. (Caught live 2026-06-05
       via /health/threads: ``_upsert_single_paper`` scheduling a hydration
       sweep from inside a gated works-upsert section.)
    2. **An open transaction on the caller's own connection**, with no gate
       held. A writer that bypasses the gate — a legacy standalone helper, the
       importer's ``_create_library_paper`` path, a route writing straight on
       its request connection — leaves the gate FREE, so the gate check alone
       says "safe to run now" while ``conn`` still owns the write lock. That is
       the general form of the ``enqueue_pending_hydration`` defect
       (2026-07-26): 5 s of busy-wait per Activity write, then a dropped row.
       There is no commit hook to defer past in this case, and the staged rows
       must be durable before the second connection acts on them anyway, so we
       commit ``conn`` (with lock retry) and then run ``fn``.

    The gate check comes first: inside a gated unit the enclosing unit owns the
    commit, so we must never commit here (it would break the unit's atomicity
    and downgrade its ``BEGIN IMMEDIATE`` — see :func:`commit_unless_gated`).

    Args:
        fn: fire-and-forget side effect that writes on another connection.
        conn: the caller's connection — the one that may be holding the write
            lock. Required: passing it is what makes case 2 detectable.
        label: short description for commit-retry log lines.

    Errors from deferred callbacks are logged, never raised — they are
    fire-and-forget side effects, and the write unit they rode on has already
    committed.
    """
    if gate_held_by_current_thread():
        _defer(fn)
        return
    if conn.in_transaction:
        # Ungated caller with staged, uncommitted writes. Close the window
        # before the second connection needs the lock. (A caller that wants
        # atomicity across the side effect should be using run_write_unit —
        # then branch 1 handles it and nothing commits early.)
        logger.debug(
            "%s: committing the caller's ungated transaction before running a "
            "second-connection side effect",
            label,
        )
        commit_with_retry(conn, label=label)
    fn()


def run_gated_or_deferred(fn: Callable[[], None], *, label: str) -> None:
    """Run ``fn`` — a self-contained write on its OWN connection — under the
    process writer gate, or defer it if this thread already holds the gate.

    The counterpart to :func:`run_after_gate_release` for the *callee* side.
    ``run_after_gate_release`` fixes call sites one by one; this fixes the
    writer itself, so a site that forgets the wrapper still queues instead of
    busy-polling.

    Used by the scheduler's Activity persistence (``operation_status`` /
    ``operation_logs``). Those writes used to bypass the gate deliberately
    (task-29 §8.2) and rely on ``BEGIN IMMEDIATE`` + ``busy_timeout`` to
    serialize. That is a *busy-poll*, not a queue: when another writer held the
    lock for longer than the timeout the row was silently dropped, and when the
    holder was THIS thread it could never be released in time at all. Routing
    through the same gate makes the wait cooperative and bounded by the actual
    holder, and makes the self-hold case (defer) structurally impossible to
    deadlock.

    ``fn`` must open and commit its own connection and must not touch the
    caller's — the gate serializes writers, it does not share transactions.
    """
    if gate_held_by_current_thread():
        # Writing a second connection now would deadlock against the write
        # lock this thread holds. The drain fires right after the commit.
        _defer(fn)
        return
    with _writer_gate(label):
        fn()


def _drain_deferred() -> None:
    queue = getattr(_GATE_STATE, "deferred", None)
    if not queue:
        return
    _GATE_STATE.deferred = []
    for fn in queue:
        try:
            fn()
        except Exception:
            logger.warning("deferred post-gate callback failed", exc_info=True)


# Gate waits longer than this are logged so sustained contention shows up
# in the server log instead of presenting as intermittent mystery latency.
_GATE_WAIT_LOG_THRESHOLD_S = 0.25


@contextmanager
def _writer_gate(label: str) -> Iterator[None]:
    """Hold the process writer gate for the duration of the block.

    The one place the gate is acquired: marks the thread as holder (so nested
    helpers can detect it), clears deferred side effects if the block failed
    (their write rolled back, so scheduling a job for rows that no longer exist
    would be wrong), and drains them once the gate — and with it the SQLite
    write lock — is released.
    """
    waited_from = time.monotonic()
    try:
        with _WRITER_GATE:
            waited = time.monotonic() - waited_from
            if waited > _GATE_WAIT_LOG_THRESHOLD_S:
                logger.warning(
                    "%s waited %.0fms for the writer gate — sustained waits mean "
                    "a long write unit upstream (check background runner batches)",
                    label,
                    waited * 1000,
                )
            _GATE_STATE.held = True
            try:
                yield
            finally:
                _GATE_STATE.held = False
    except BaseException:
        _GATE_STATE.deferred = []
        raise
    _drain_deferred()


def commit_unless_gated(conn: sqlite3.Connection, *, label: str = "db write") -> None:
    """Caller-owns-transaction commit for SHARED write helpers.

    A write helper that may run EITHER standalone OR nested inside a
    :func:`run_write_unit` / :func:`write_section` calls this instead of a raw
    ``conn.commit()``:

    * **gate held by this thread** → no-op. The enclosing unit owns the commit,
      so committing here would (a) break the unit's atomicity and (b) silently
      downgrade its ``BEGIN IMMEDIATE`` to a DEFERRED transaction for the rest of
      the section — re-introducing the read→write upgrade hazard the section
      exists to prevent. We also assert the gate-holder actually opened a
      transaction: a held gate with no open txn means the unit forgot its
      ``BEGIN`` and this helper's writes would be silently lost.
    * **gate NOT held** → ``commit_with_retry``. A standalone (legacy) caller
      owns the implicit transaction; commit it with transient-lock retry +
      logging instead of a bare, un-retried commit.

    This replaces the fragile ``if conn.in_transaction: conn.commit()`` idiom,
    which committed in *both* cases and thereby broke any enclosing gated unit.
    The same gate-aware shape was already hand-rolled at
    ``application/library.add_to_library`` and
    ``application/feed_monitors.sync_author_monitors``; this is the DRY
    extraction.

    NOTE — scope: this is the correct, complete fix for the *nested* case. For
    the standalone case it is a safety net (retry + logging), NOT a substitute
    for the caller running inside a ``write_section`` / ``run_write_unit``: the
    standalone helper's writes already ran on a DEFERRED implicit transaction,
    so the read→write upgrade can still raise SQLITE_BUSY on the staged write,
    before this commit is ever reached. Background batch writers that interleave
    writes with network I/O must still be restructured onto ``write_section``.
    """
    if gate_held_by_current_thread():
        assert conn.in_transaction, (
            f"{label}: writer gate is held but no transaction is open — the "
            "enclosing run_write_unit/write_section did not BEGIN; this helper's "
            "writes would be silently lost"
        )
        return
    commit_with_retry(conn, label=label)


def run_write_unit(
    conn: sqlite3.Connection,
    unit: Callable[[], T],
    *,
    label: str = "db write",
    attempts: int = 4,
    base_delay: float = 0.05,
) -> T:
    """Run ``unit`` as one serialized, IMMEDIATE, retried write transaction.

    Layers (outermost first):

    * **writer gate** — process-wide mutex so only one thread attempts the
      SQLite write lock at a time (no intra-process SQLITE_BUSY, fair
      cooperative queueing instead of busy-polling);
    * **retry** — :func:`run_with_lock_retry` re-runs the whole unit on a
      *transient* lock error (cross-process contention only, e.g. a CLI
      script holding the writer);
    * **transaction** — ``rollback`` clears any aborted state, ``BEGIN
      IMMEDIATE`` takes the write lock up front (busy_timeout applies,
      no deferred-upgrade hazard), ``commit`` on success / ``rollback``
      on failure.

    Args:
        conn: open connection the unit writes through.
        unit: zero-arg callable performing the writes; its return value is
            passed through. Must tolerate re-running on a clean
            transaction (idempotent or pure-write).
        label: short description for gate/retry log lines.
        attempts/base_delay: forwarded to :func:`run_with_lock_retry`.
    """

    def _transaction() -> T:
        # rollback() first so a retry (or a dirty inherited connection)
        # starts from a clean autocommit state — BEGIN IMMEDIATE would
        # otherwise raise "cannot start a transaction within a transaction".
        conn.rollback()
        conn.execute("BEGIN IMMEDIATE")
        try:
            result = unit()
            conn.commit()
            return result
        except BaseException:
            conn.rollback()
            raise

    # `_writer_gate` owns the gate, the holder flag, and the deferred-callback
    # lifecycle: side effects fire only after commit + release, and are dropped
    # if the unit rolled back. See run_after_gate_release.
    with _writer_gate(label):
        result = run_with_lock_retry(
            _transaction, attempts=attempts, base_delay=base_delay, label=label
        )
    return result


@contextmanager
def write_section(
    conn: sqlite3.Connection,
    *,
    label: str = "db write section",
) -> Iterator[sqlite3.Connection]:
    """Writer-gated ``BEGIN IMMEDIATE … COMMIT`` block for batch writers.

    The background-runner counterpart of :func:`run_write_unit`: wrap each
    *write window* of a chunked job (the stretch between "all data for this
    chunk is gathered" and the chunk commit) so that

    * the process writer gate serializes it against foreground writes — a
      user's click never busy-polls against a runner's batch;
    * ``BEGIN IMMEDIATE`` takes the write lock up front (no deferred
      read→write upgrade hazard);
    * commit/rollback are structural, not sprinkled through the loop.

    Deliberately NO retry layer: background jobs are idempotent and re-run
    on the next sweep (see ``db_retry`` module docstring), so a transient
    cross-process lock simply propagates and the sweep self-heals.

    Usage — gather first, then write::

        fetched = fetch_chunk_from_network(ids)     # network OUTSIDE
        with write_section(conn, label="s2_vectors chunk"):
            for row in fetched:
                conn.execute("INSERT ...", row)     # short, local-only

    Never perform network I/O or model inference inside the section — that
    holds both the gate and the SQLite write lock across the slow work,
    which is precisely the starvation this module exists to prevent.
    """
    with _writer_gate(label):
        conn.rollback()
        conn.execute("BEGIN IMMEDIATE")
        try:
            yield conn
            conn.commit()
        except BaseException:
            conn.rollback()
            raise
