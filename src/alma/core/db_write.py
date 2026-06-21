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
from contextlib import contextmanager
from typing import Callable, Iterator, TypeVar

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


def run_after_gate_release(fn: Callable[[], None]) -> None:
    """Run ``fn`` now — or, if this thread holds the writer gate, defer it
    until the gate (and the SQLite write lock) is released.

    Why this exists: job scheduling (``schedule_with_envelope`` →
    ``find_active_job`` / ``set_job_status`` / ``add_job_log``) persists
    Activity state through the scheduler's OWN connection. Called from
    inside an open write transaction, that second connection blocks on the
    very write lock the calling thread holds — a same-thread self-deadlock
    that only "resolves" via busy-timeout failures (caught live 2026-06-05
    via /health/threads: ``_upsert_single_paper`` scheduling a hydration
    sweep from inside a gated works-upsert section). Wrapping the
    scheduling call in this function keeps call sites unchanged while
    moving the scheduler write to just after commit.

    Errors from deferred callbacks are logged, never raised — they are
    fire-and-forget side effects (job scheduling), and the write unit they
    rode on has already committed.
    """
    if not gate_held_by_current_thread():
        fn()
        return
    queue = getattr(_GATE_STATE, "deferred", None)
    if queue is None:
        queue = []
        _GATE_STATE.deferred = queue
    queue.append(fn)


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


# Gate waits longer than this are logged so sustained contention shows up
# in the server log instead of presenting as intermittent mystery latency.
_GATE_WAIT_LOG_THRESHOLD_S = 0.25


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
                result = run_with_lock_retry(
                    _transaction, attempts=attempts, base_delay=base_delay, label=label
                )
            finally:
                _GATE_STATE.held = False
    except BaseException:
        # The unit failed and rolled back — its deferred side effects
        # (job scheduling for rows that no longer exist) must not fire.
        _GATE_STATE.deferred = []
        raise
    # Deferred side effects (job scheduling etc.) run with the gate and the
    # SQLite write lock both released — see run_after_gate_release.
    _drain_deferred()
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
                conn.rollback()
                conn.execute("BEGIN IMMEDIATE")
                try:
                    yield conn
                    conn.commit()
                except BaseException:
                    conn.rollback()
                    raise
            finally:
                _GATE_STATE.held = False
    except BaseException:
        # Section failed and rolled back — drop its deferred side effects.
        _GATE_STATE.deferred = []
        raise
    # Job scheduling etc. deferred from inside the section runs only now,
    # with the gate and the SQLite write lock released.
    _drain_deferred()
