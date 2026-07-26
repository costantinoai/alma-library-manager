"""Process-isolated execution for expensive graph recomputation.

APScheduler runs ordinary ALMa jobs in threads. That is appropriate for
network-heavy maintenance, but graph clustering/projection is CPU-heavy native
and Python work. Running it in the API process can starve FastAPI's request
threads even though the route already returned ``202``.

This module is the process boundary for graph work:

* the API/scheduler process owns Activity state and waits on a tiny supervisor
  thread;
* a fresh Python child opens its own read connection, computes the graph, and
  persists the completed materialized view;
* the prior materialized payload remains readable until the child performs the
  final short cache write.

The child protocol is intentionally a small JSON spec rather than a pickled
closure. It works with the ``spawn``-equivalent semantics used by containers,
keeps graph builders explicit, and avoids inheriting live SQLite connections or
thread locks.
"""

from __future__ import annotations

import argparse
import base64
import json
import logging
import os
import subprocess
import sys
import time
from collections.abc import Mapping
from typing import Any

logger = logging.getLogger(__name__)

_RESULT_PREFIX = "ALMA_GRAPH_RESULT="
_POLL_SECONDS = 0.2


def _encode_spec(spec: Mapping[str, Any]) -> str:
    raw = json.dumps(dict(spec), separators=(",", ":"), default=str).encode("utf-8")
    return base64.urlsafe_b64encode(raw).decode("ascii")


def _decode_spec(raw: str) -> dict[str, Any]:
    decoded = base64.urlsafe_b64decode(raw.encode("ascii")).decode("utf-8")
    payload = json.loads(decoded)
    if not isinstance(payload, dict):
        raise TypeError("graph process spec must decode to an object")
    return payload


def _child_result(stdout: str) -> dict[str, Any]:
    for line in reversed(stdout.splitlines()):
        if not line.startswith(_RESULT_PREFIX):
            continue
        value = json.loads(line[len(_RESULT_PREFIX) :])
        if isinstance(value, dict):
            return value
        raise TypeError("graph worker result must be an object")
    raise RuntimeError("graph worker exited without a result envelope")


def run_graph_process(
    spec: Mapping[str, Any],
    *,
    job_id: str | None = None,
) -> dict[str, Any]:
    """Run one graph build in a child process and return its small summary.

    Only the scheduler worker waits. The API process remains free to serve
    Home, Suggestions, cached graph reads, and foreground mutations. Under
    pytest the spec executes inline by default so unit tests remain fast and
    monkeypatch-friendly; the subprocess boundary has its own focused test.
    """
    if os.getenv("PYTEST_CURRENT_TEST") and os.getenv("ALMA_TEST_GRAPH_SUBPROCESS") != "1":
        return _run_graph_spec(dict(spec))

    command = [
        sys.executable,
        "-m",
        "alma.application.graph_process",
        "--spec",
        _encode_spec(spec),
    ]
    proc = subprocess.Popen(  # noqa: S603 — fixed interpreter/module, encoded data arg
        command,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        env=os.environ.copy(),
    )
    stdout = ""
    stderr = ""
    try:
        while True:
            try:
                stdout, stderr = proc.communicate(timeout=_POLL_SECONDS)
                break
            except subprocess.TimeoutExpired:
                if not job_id:
                    continue
                from alma.api.scheduler import is_cancellation_requested

                if is_cancellation_requested(job_id):
                    from alma.api.scheduler import JobCancelled

                    proc.terminate()
                    try:
                        stdout, stderr = proc.communicate(timeout=5)
                    except subprocess.TimeoutExpired:
                        proc.kill()
                        stdout, stderr = proc.communicate()
                    raise JobCancelled("graph build cancelled")
        if proc.returncode != 0:
            detail = (stderr or stdout).strip()[-4000:]
            raise RuntimeError(
                f"graph worker exited with status {proc.returncode}"
                + (f": {detail}" if detail else "")
            )
        if stderr.strip():
            logger.debug("graph worker stderr: %s", stderr.strip()[-4000:])
        return _child_result(stdout)
    finally:
        if proc.poll() is None:
            proc.kill()
            proc.wait()


def _run_graph_spec(spec: dict[str, Any]) -> dict[str, Any]:
    """Child-side dispatcher. Every branch opens and closes its own DB handle."""
    # Importing the routes registers the graph materialized views and gives this
    # worker the exact same builder functions as the API process.
    from alma.api.deps import open_db_connection
    from alma.api.routes import graphs
    from alma.application import materialized_views as mv
    from alma.core.scope import Scope

    kind = str(spec.get("kind") or "")
    conn = open_db_connection()
    try:
        if kind == "registered_view":
            view_key = str(spec["view_key"])
            payload = mv.rebuild(conn, view_key)
            return {
                "view_key": view_key,
                "nodes": len(payload.get("nodes") or []),
                "message": f"Materialized {view_key}",
            }

        if kind == "full_scope":
            scope = Scope.parse(str(spec.get("scope") or "library"))
            return graphs._rebuild_graphs_impl(
                conn,
                scope=scope,
                job_id=str(spec.get("job_id") or "") or None,
            )

        if kind == "variant":
            view_key = str(spec["view_key"])
            graph_type = str(spec["graph_type"])
            scope = Scope.parse(str(spec.get("scope") or "library"))
            options = spec.get("options") or {}
            if not isinstance(options, dict):
                raise TypeError("variant options must be an object")
            started = time.perf_counter()
            payload = graphs._build_graph_variant_payload(
                conn,
                graph_type=graph_type,
                scope=scope,
                options=options,
            )
            compute_ms = int(round((time.perf_counter() - started) * 1000))
            fingerprint = graphs._paper_scope_gauge(conn, scope).signature(conn)
            mv.persist_variant_payload(
                conn,
                view_key=view_key,
                fingerprint=fingerprint,
                payload=payload,
                compute_ms=compute_ms,
            )
            return {
                "view_key": view_key,
                "nodes": len(payload.get("nodes") or []),
                "message": f"Built {graph_type} variant ({compute_ms} ms)",
            }

        raise ValueError(f"unknown graph process kind: {kind!r}")
    finally:
        conn.close()


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="ALMa isolated graph worker")
    parser.add_argument("--spec", required=True)
    args = parser.parse_args(argv)
    result = _run_graph_spec(_decode_spec(args.spec))
    print(f"{_RESULT_PREFIX}{json.dumps(result, separators=(',', ':'), default=str)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
