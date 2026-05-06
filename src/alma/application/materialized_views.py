"""Materialised-view layer for expensive read aggregates.

Endpoints like ``/insights`` and ``/graphs/*`` produce JSON payloads that
are slow to compute (dozens of aggregate SQL queries, embedding scans,
clustering / UMAP). Recomputing on every GET makes the UI feel locked
during background activity. This module offers a single primitive — a
named view backed by the ``materialized_views`` table — that:

* serves the cached payload immediately (~1 ms) when its inputs have not
  changed;
* compares the current input fingerprint against the cached one on each
  GET, and when they differ enqueues a background rebuild without
  blocking the response;
* serves the *stale* payload meanwhile and flags ``stale=True`` /
  ``rebuilding=True`` so the UI can show a "Refreshing…" indicator;
* is robust to build failures: a stale row is preferred over a 500.

The contract is pull-based: writers don't have to know about views.
Each view declares a small fingerprint SQL (a single SELECT returning a
tuple of values that change exactly when the view's payload should
change). Collisions across views are avoided by namespacing
``view_key``: ``insights:overview``, ``graph:paper_map:library``, etc.

A second tier — explicit user-triggered "Rebuild" — bypasses the
fingerprint and forces a full recompute. That path is exposed by the
hosting route, not here, but it ultimately calls :func:`rebuild`.
"""

from __future__ import annotations

import hashlib
import json
import logging
import sqlite3
import uuid
from dataclasses import dataclass
from datetime import datetime
from time import perf_counter
from typing import Any, Callable, Optional

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Registry
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class View:
    """One named view definition.

    Attributes
    ----------
    key:
        Stable namespaced name, e.g. ``"insights:overview"``.
    fingerprint_sql:
        A single SELECT that returns one row of values.  When any of
        those values changes, the cached payload is treated as stale
        and a background rebuild is enqueued. Should be cheap (a
        handful of ``MAX``/``COUNT`` aggregates).
    build_fn:
        Callable ``(conn) -> dict`` that produces the payload from
        scratch.  Receives a connection that already has the SQLite
        pragmas the rest of the app expects.
    operation_key:
        APScheduler operation key for dedup.  Concurrent rebuild
        requests collapse to one running job.
    """

    key: str
    fingerprint_sql: str
    build_fn: Callable[[sqlite3.Connection], dict]
    operation_key: str


_REGISTRY: dict[str, View] = {}


def register(view: View) -> None:
    """Add a view to the registry. Idempotent on identical re-register."""
    existing = _REGISTRY.get(view.key)
    if existing is not None and existing is not view:
        # Re-registering with a different definition would silently
        # change behaviour for everything that already imported the
        # registry. Loud-fail instead.
        raise RuntimeError(
            f"materialized view {view.key!r} is already registered "
            f"with a different definition"
        )
    _REGISTRY[view.key] = view


def get_view(view_key: str) -> View:
    try:
        return _REGISTRY[view_key]
    except KeyError as exc:
        raise KeyError(f"unknown materialized view: {view_key!r}") from exc


# ---------------------------------------------------------------------------
# Fingerprint
# ---------------------------------------------------------------------------


def _compute_fingerprint(conn: sqlite3.Connection, view: View) -> str:
    """Hash the row returned by ``view.fingerprint_sql`` to a hex string.

    Returns the literal string ``"__error__:<msg>"`` if the fingerprint
    SQL fails.  We never silently treat a failed fingerprint as "match"
    — that would freeze the cache.  Instead the mismatch will trigger a
    rebuild on the next call, which will surface the underlying issue.
    """
    try:
        row = conn.execute(view.fingerprint_sql).fetchone()
    except Exception as exc:  # noqa: BLE001 — surface root cause via fingerprint
        logger.warning(
            "materialized_views: fingerprint SQL failed for %s: %s",
            view.key,
            exc,
        )
        return f"__error__:{exc}"
    # ``row`` is sqlite3.Row or tuple-like. Normalise to a tuple of strs
    # so the hash is order-stable.
    if row is None:
        values: tuple[str, ...] = ()
    else:
        values = tuple("" if v is None else str(v) for v in tuple(row))
    blob = "|".join(values).encode("utf-8")
    return hashlib.sha1(blob).hexdigest()


# ---------------------------------------------------------------------------
# Cache row I/O
# ---------------------------------------------------------------------------


def _read_row(conn: sqlite3.Connection, view_key: str) -> Optional[dict]:
    try:
        row = conn.execute(
            "SELECT view_key, fingerprint, payload, computed_at, compute_ms, "
            "       build_status, build_error, rebuild_job_id "
            "FROM materialized_views WHERE view_key = ?",
            (view_key,),
        ).fetchone()
    except sqlite3.OperationalError:
        # Table missing — schema init should always have created it,
        # but be defensive: a missing cache row is equivalent to "no
        # cache yet".
        return None
    if row is None:
        return None
    if isinstance(row, sqlite3.Row):
        return dict(row)
    keys = (
        "view_key",
        "fingerprint",
        "payload",
        "computed_at",
        "compute_ms",
        "build_status",
        "build_error",
        "rebuild_job_id",
    )
    return dict(zip(keys, row))


def _write_row(
    conn: sqlite3.Connection,
    *,
    view_key: str,
    fingerprint: str,
    payload: dict,
    compute_ms: int,
    build_status: str = "ok",
    build_error: Optional[str] = None,
    rebuild_job_id: Optional[str] = None,
) -> None:
    conn.execute(
        """
        INSERT INTO materialized_views (
            view_key, fingerprint, payload, computed_at, compute_ms,
            build_status, build_error, rebuild_job_id
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(view_key) DO UPDATE SET
            fingerprint    = excluded.fingerprint,
            payload        = excluded.payload,
            computed_at    = excluded.computed_at,
            compute_ms     = excluded.compute_ms,
            build_status   = excluded.build_status,
            build_error    = excluded.build_error,
            rebuild_job_id = excluded.rebuild_job_id
        """,
        (
            view_key,
            fingerprint,
            json.dumps(payload, default=str),
            datetime.utcnow().isoformat(),
            int(compute_ms),
            build_status,
            build_error,
            rebuild_job_id,
        ),
    )
    if conn.in_transaction:
        conn.commit()


def _set_rebuild_job_id(
    conn: sqlite3.Connection,
    view_key: str,
    job_id: Optional[str],
) -> None:
    """Record / clear the in-flight rebuild job id without disturbing payload."""
    try:
        conn.execute(
            "UPDATE materialized_views SET rebuild_job_id = ? WHERE view_key = ?",
            (job_id, view_key),
        )
        if conn.in_transaction:
            conn.commit()
    except sqlite3.OperationalError:
        pass


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def get(conn: sqlite3.Connection, view_key: str) -> dict[str, Any]:
    """Return a stale-while-revalidate envelope for ``view_key``.

    The envelope shape is::

        {
            "payload":     <view's dict>,        # always present on success
            "stale":       bool,                  # True when serving prior payload
            "rebuilding":  bool,                  # True when a job is in flight
            "computed_at": str,                   # ISO timestamp of cached row
            "fingerprint": str,                   # current input fingerprint
        }

    Behaviour:

    * If no cached row exists → build synchronously (first-time cost).
    * If fingerprint matches the cached row → return it, ``stale=False``.
    * If fingerprint differs → enqueue background rebuild (deduped via
      ``operation_key``) and return the cached payload with
      ``stale=True``, ``rebuilding=True``.
    * If a synchronous build fails and a stale row exists → return the
      stale row with ``stale=True``, ``rebuilding=False``.  If no stale
      row exists, the exception propagates so the route returns 5xx.
    """
    view = get_view(view_key)
    current_fp = _compute_fingerprint(conn, view)
    row = _read_row(conn, view_key)

    if row is None:
        # First time — build synchronously so the user sees data.
        payload = _run_build(conn, view, fingerprint=current_fp)
        return _envelope(
            payload=payload,
            fingerprint=current_fp,
            computed_at=datetime.utcnow().isoformat(),
            stale=False,
            rebuilding=False,
        )

    cached_payload = _decode_payload(row.get("payload"))

    if row.get("fingerprint") == current_fp and cached_payload is not None:
        return _envelope(
            payload=cached_payload,
            fingerprint=current_fp,
            computed_at=str(row.get("computed_at") or ""),
            stale=False,
            rebuilding=False,
        )

    # Mismatch (or undecodable cache) → kick off a background rebuild
    # and serve the stale row.  When the cache is undecodable we still
    # try to enqueue a rebuild — the next GET after it completes will
    # observe a fresh row.
    rebuilding = _enqueue_rebuild(conn, view)

    if cached_payload is None:
        # We had a row but couldn't decode the payload — fall back to a
        # synchronous rebuild so the user sees something usable.
        payload = _run_build(conn, view, fingerprint=current_fp)
        return _envelope(
            payload=payload,
            fingerprint=current_fp,
            computed_at=datetime.utcnow().isoformat(),
            stale=False,
            rebuilding=False,
        )

    return _envelope(
        payload=cached_payload,
        fingerprint=str(row.get("fingerprint") or ""),
        computed_at=str(row.get("computed_at") or ""),
        stale=True,
        rebuilding=rebuilding,
    )


def rebuild(conn: sqlite3.Connection, view_key: str) -> dict[str, Any]:
    """Synchronously rebuild ``view_key`` and persist the new payload.

    Used by the user-triggered "Rebuild" action and by the background
    job runner. Returns the freshly computed payload (not the
    envelope) so the rebuild job's result message can mention size /
    timing.
    """
    view = get_view(view_key)
    fp = _compute_fingerprint(conn, view)
    return _run_build(conn, view, fingerprint=fp)


def enqueue_rebuild(view_key: str) -> Optional[str]:
    """Schedule a rebuild of ``view_key`` to run in the background.

    Returns the scheduled ``job_id``, or ``None`` if a rebuild is
    already in flight (deduped by ``operation_key``).
    """
    view = get_view(view_key)
    # Tiny throwaway connection just for the dedup query / status row.
    from alma.api.deps import open_db_connection

    conn = open_db_connection()
    try:
        return _enqueue_rebuild_internal(conn, view)
    finally:
        try:
            conn.close()
        except Exception:
            pass


# ---------------------------------------------------------------------------
# Internals
# ---------------------------------------------------------------------------


def _envelope(
    *,
    payload: dict,
    fingerprint: str,
    computed_at: str,
    stale: bool,
    rebuilding: bool,
) -> dict[str, Any]:
    return {
        "payload": payload,
        "stale": stale,
        "rebuilding": rebuilding,
        "computed_at": computed_at,
        "fingerprint": fingerprint,
    }


def _decode_payload(raw: Any) -> Optional[dict]:
    if raw is None:
        return None
    if isinstance(raw, dict):
        return raw
    try:
        decoded = json.loads(raw)
    except (TypeError, ValueError):
        return None
    return decoded if isinstance(decoded, dict) else None


def _run_build(
    conn: sqlite3.Connection,
    view: View,
    *,
    fingerprint: str,
) -> dict:
    """Run the build function and persist its result.

    Build failures bubble up to the caller. Persistence failures are
    logged but never mask a successful build.
    """
    started = perf_counter()
    payload = view.build_fn(conn)
    if not isinstance(payload, dict):
        raise TypeError(
            f"build_fn for {view.key!r} returned {type(payload).__name__}, expected dict"
        )
    compute_ms = int(round((perf_counter() - started) * 1000))
    try:
        _write_row(
            conn,
            view_key=view.key,
            fingerprint=fingerprint,
            payload=payload,
            compute_ms=compute_ms,
            build_status="ok",
            build_error=None,
            rebuild_job_id=None,
        )
    except Exception:  # noqa: BLE001 — cache write should never break the response
        logger.exception("materialized_views: failed to persist payload for %s", view.key)
    return payload


def _enqueue_rebuild(conn: sqlite3.Connection, view: View) -> bool:
    """Enqueue a background rebuild. Returns True if a rebuild is in flight."""
    return _enqueue_rebuild_internal(conn, view) is not None or _has_active_job(view)


def _has_active_job(view: View) -> bool:
    from alma.api.scheduler import find_active_job

    return find_active_job(view.operation_key) is not None


def _enqueue_rebuild_internal(
    conn: sqlite3.Connection,
    view: View,
) -> Optional[str]:
    """Schedule the rebuild via APScheduler with operation_key dedup.

    Returns the new job_id, or ``None`` if a rebuild is already running
    for this view.
    """
    from alma.api.scheduler import (
        find_active_job,
        schedule_immediate,
        set_job_status,
    )

    if find_active_job(view.operation_key) is not None:
        return None

    job_id = f"materialize_{view.key.replace(':', '_')}_{uuid.uuid4().hex[:8]}"

    set_job_status(
        job_id,
        status="queued",
        operation_key=view.operation_key,
        trigger_source="auto",
        started_at=datetime.utcnow().isoformat(),
        message=f"Rebuilding {view.key}",
    )
    _set_rebuild_job_id(conn, view.key, job_id)

    def _runner() -> dict:
        from alma.api.deps import open_db_connection

        runner_conn = open_db_connection()
        try:
            payload = rebuild(runner_conn, view.key)
            # Clear the in-flight job id once the new row is persisted.
            _set_rebuild_job_id(runner_conn, view.key, None)
            size_hint = ""
            if isinstance(payload, dict):
                # Lightweight size summary for the Activity terminal message.
                if "summary" in payload and isinstance(payload["summary"], dict):
                    total = payload["summary"].get("total_papers")
                    if isinstance(total, int):
                        size_hint = f" ({total} papers)"
            return {
                "view_key": view.key,
                "message": f"Materialized {view.key}{size_hint}",
            }
        finally:
            try:
                runner_conn.close()
            except Exception:
                pass

    try:
        schedule_immediate(job_id, _runner)
    except Exception:
        logger.exception("materialized_views: failed to schedule rebuild for %s", view.key)
        _set_rebuild_job_id(conn, view.key, None)
        return None
    return job_id
