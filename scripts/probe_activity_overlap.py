#!/usr/bin/env python
"""Activity/concurrency probe harness.

Starts a chosen heavy background job against a running ALMa backend and, in
parallel, measures the latency of a panel of page reads, a short user write,
and Activity reads. Prints a per-endpoint summary at the end.

This is the tool called for in ``tasks/STATUS.md`` and
``tasks/10_ACTIVITY_CONCURRENCY.md`` to prove the Activity/background
contract across every heavy operation on the real database, not only Feed
refresh.

Usage examples (backend must already be running on ``--base-url``):

    # Probe while a discovery refresh runs (default duration 20s).
    python scripts/probe_activity_overlap.py --job discovery

    # Probe every heavy job in sequence; shorter duration per job.
    python scripts/probe_activity_overlap.py --job all --duration 15

    # Probe Activity only, without starting any job (baseline).
    python scripts/probe_activity_overlap.py --job none --duration 10

The harness never hard-deletes data. Short-write probes hit
``POST /feed/{id}/add``, which is idempotent for rows already in that state.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import statistics
import sys
import time
from dataclasses import dataclass, field
from typing import Any, Callable, Optional

import httpx


# ---------------------------------------------------------------------------
# Job catalog
# ---------------------------------------------------------------------------

# Mapping: job slug -> (method, path, json_body, description).
# Every entry returns an Activity envelope (``{"job_id": ..., "status": ...}``)
# when the backend decides to run it in the background.
JOBS: dict[str, tuple[str, str, Optional[dict[str, Any]], str]] = {
    "embeddings": (
        "POST",
        "/api/v1/ai/compute-embeddings",
        {"scope": "missing_stale"},
        "AI embeddings backfill",
    ),
    "enrich": (
        "POST",
        "/api/v1/library/import/enrich",
        None,
        "Import enrichment (OpenAlex resolution for imported papers)",
    ),
    "openalex-authors": (
        "POST",
        "/api/v1/authors/resolve-identifiers",
        {"limit": 100, "only_unresolved": True, "background": True},
        "Authors OpenAlex identifier resolution",
    ),
    "graphs-rebuild": (
        "POST",
        "/api/v1/graphs/rebuild",
        None,
        "Graph cache rebuild",
    ),
    "graphs-backfill": (
        "POST",
        "/api/v1/graphs/reference-backfill",
        None,
        "Graph reference backfill",
    ),
    "discovery": (
        "POST",
        "/api/v1/discovery/refresh",
        None,
        "Discovery recommendations refresh",
    ),
    "tags": (
        "POST",
        "/api/v1/tags/suggestions/generate",
        None,
        "Bulk tag suggestion generation",
    ),
    "dedup": (
        "POST",
        "/api/v1/library-mgmt/deduplicate",
        None,
        "Library deduplication",
    ),
    "authors-refresh": (
        "POST",
        "/api/v1/authors/deep-refresh-all",
        None,
        "Deep refresh of all followed authors",
    ),
    "feed-refresh": (
        "POST",
        "/api/v1/feed/refresh?background=true",
        None,
        "Feed inbox refresh (user reported Library/Authors freeze during run)",
    ),
}


# ---------------------------------------------------------------------------
# Probe definitions
# ---------------------------------------------------------------------------

@dataclass
class ProbeSpec:
    name: str
    method: str
    # Path can be a callable so we can substitute dynamic values (e.g. feed id)
    path: str | Callable[[dict[str, Any]], str | None]
    interval_s: float = 1.0
    json_body: Optional[dict[str, Any]] = None


@dataclass
class ProbeResult:
    name: str
    samples: list[float] = field(default_factory=list)
    status_counts: dict[int, int] = field(default_factory=dict)
    errors: list[str] = field(default_factory=list)

    def record(self, status: int, latency: float) -> None:
        self.samples.append(latency)
        self.status_counts[status] = self.status_counts.get(status, 0) + 1

    def record_error(self, message: str) -> None:
        self.errors.append(message)

    def summary(self) -> dict[str, Any]:
        if not self.samples:
            return {
                "name": self.name,
                "samples": 0,
                "errors": self.errors[:3],
            }
        samples_sorted = sorted(self.samples)
        n = len(samples_sorted)

        def pct(p: float) -> float:
            idx = min(n - 1, max(0, int(round((n - 1) * p))))
            return samples_sorted[idx]

        return {
            "name": self.name,
            "samples": n,
            "status": self.status_counts,
            "p50_ms": round(pct(0.50) * 1000, 1),
            "p95_ms": round(pct(0.95) * 1000, 1),
            "max_ms": round(max(samples_sorted) * 1000, 1),
            "mean_ms": round(statistics.fmean(samples_sorted) * 1000, 1),
            "errors": self.errors[:3],
        }


async def _probe_once(
    client: httpx.AsyncClient,
    spec: ProbeSpec,
    context: dict[str, Any],
    result: ProbeResult,
) -> None:
    path = spec.path(context) if callable(spec.path) else spec.path
    if not path:
        return  # skip this probe (e.g. no feed_item available for short-write)
    start = time.perf_counter()
    try:
        response = await client.request(
            spec.method,
            path,
            json=spec.json_body,
            timeout=10.0,
        )
        latency = time.perf_counter() - start
        result.record(response.status_code, latency)
    except httpx.HTTPError as exc:
        result.record_error(f"{type(exc).__name__}: {exc}")


async def _run_probe_loop(
    client: httpx.AsyncClient,
    spec: ProbeSpec,
    context: dict[str, Any],
    result: ProbeResult,
    stop_event: asyncio.Event,
) -> None:
    while not stop_event.is_set():
        await _probe_once(client, spec, context, result)
        try:
            await asyncio.wait_for(stop_event.wait(), timeout=spec.interval_s)
        except asyncio.TimeoutError:
            pass


# ---------------------------------------------------------------------------
# Fixture helpers
# ---------------------------------------------------------------------------

async def _pick_feed_item_id(client: httpx.AsyncClient) -> Optional[str]:
    """Best-effort lookup of a feed_item id so we have a short-write target."""
    try:
        r = await client.get("/api/v1/feed", params={"limit": 1}, timeout=10.0)
        if r.status_code != 200:
            return None
        data = r.json()
        items = data.get("items") if isinstance(data, dict) else data
        if not items:
            return None
        first = items[0]
        return first.get("id") or first.get("feed_item_id") or first.get("paper_id")
    except Exception:
        return None


# ---------------------------------------------------------------------------
# Job runner
# ---------------------------------------------------------------------------

async def _start_job(
    client: httpx.AsyncClient, method: str, path: str, body: Optional[dict[str, Any]]
) -> dict[str, Any]:
    response = await client.request(method, path, json=body, timeout=30.0)
    try:
        payload = response.json()
    except Exception:
        payload = {"raw": response.text}
    return {"status_code": response.status_code, "payload": payload}


async def _wait_for_job(
    client: httpx.AsyncClient, job_id: str, duration_s: float
) -> dict[str, Any]:
    """Poll /scheduler/status/{job_id} until the duration elapses or job ends."""
    deadline = time.monotonic() + duration_s
    last: dict[str, Any] = {}
    while time.monotonic() < deadline:
        try:
            r = await client.get(f"/api/v1/scheduler/status/{job_id}", timeout=5.0)
            if r.status_code == 200:
                last = r.json()
                status = str(last.get("status") or "").lower()
                if status in {"completed", "failed", "noop", "cancelled"}:
                    return last
        except httpx.HTTPError:
            pass
        await asyncio.sleep(1.0)
    return last


# ---------------------------------------------------------------------------
# Orchestration
# ---------------------------------------------------------------------------

async def run_probes_for_job(
    base_url: str,
    job_slug: str,
    duration_s: float,
) -> dict[str, Any]:
    if job_slug == "none":
        method, path, body, description = "GET", "", None, "(baseline, no job)"
    else:
        method, path, body, description = JOBS[job_slug]
    async with httpx.AsyncClient(base_url=base_url) as client:
        feed_item_id = await _pick_feed_item_id(client)

        specs: list[ProbeSpec] = [
            ProbeSpec("GET /feed", "GET", "/api/v1/feed?limit=25"),
            ProbeSpec("GET /authors", "GET", "/api/v1/authors?limit=25"),
            ProbeSpec("GET /activity", "GET", "/api/v1/activity?limit=25"),
            ProbeSpec("GET /library/saved", "GET", "/api/v1/library/saved?limit=25"),
            ProbeSpec("GET /papers/stats", "GET", "/api/v1/papers/stats?top_limit=5"),
            # Library / Authors page-mount reads. These are what the user
            # sees freeze during feed.refresh_inbox (reported 2026-04-22);
            # measuring them is how we prove the fix.
            ProbeSpec(
                "GET /library/workflow-summary",
                "GET",
                "/api/v1/library/workflow-summary",
            ),
            ProbeSpec(
                "GET /library/followed-authors",
                "GET",
                "/api/v1/library/followed-authors",
            ),
            ProbeSpec(
                "GET /authors/suggestions",
                "GET",
                "/api/v1/authors/suggestions?limit=8",
            ),
        ]

        if feed_item_id:
            short_write_path = f"/api/v1/feed/{feed_item_id}/add"
            specs.append(
                ProbeSpec(
                    f"POST {short_write_path}",
                    "POST",
                    short_write_path,
                    interval_s=2.0,
                )
            )

        results = {spec.name: ProbeResult(spec.name) for spec in specs}
        stop_event = asyncio.Event()
        context: dict[str, Any] = {"feed_item_id": feed_item_id}

        probe_tasks = [
            asyncio.create_task(
                _run_probe_loop(client, spec, context, results[spec.name], stop_event)
            )
            for spec in specs
        ]

        # Take a quick baseline before starting the job.
        baseline_start = time.perf_counter()
        await asyncio.sleep(min(3.0, max(1.0, duration_s / 6)))
        baseline_cut = time.perf_counter() - baseline_start

        if job_slug != "none":
            print(f"[probe] Starting job: {description} ({method} {path})")
            start = await _start_job(client, method, path, body)
            if start["status_code"] >= 400:
                print(
                    f"[probe] Job rejected ({start['status_code']}): "
                    f"{json.dumps(start['payload'])[:400]}"
                )
                stop_event.set()
                await asyncio.gather(*probe_tasks, return_exceptions=True)
                return {
                    "job_slug": job_slug,
                    "job_start": start,
                    "baseline_duration_s": round(baseline_cut, 2),
                    "probes": [r.summary() for r in results.values()],
                }

            payload = start["payload"]
            job_id = None
            if isinstance(payload, dict):
                job_id = payload.get("job_id") or payload.get("id")
            if job_id:
                print(f"[probe] Job scheduled with job_id={job_id}; waiting up to {duration_s:.0f}s")
                last = await _wait_for_job(client, str(job_id), duration_s)
                job_final = {"payload": payload, "final_status": last}
            else:
                print(
                    "[probe] No job_id returned; treating response as synchronous and "
                    f"probing for {duration_s:.0f}s"
                )
                await asyncio.sleep(duration_s)
                job_final = {"payload": payload, "final_status": None}
        else:
            print(f"[probe] Baseline mode (no job) — probing for {duration_s:.0f}s")
            await asyncio.sleep(duration_s)
            job_final = {"payload": None, "final_status": None}

        stop_event.set()
        await asyncio.gather(*probe_tasks, return_exceptions=True)

        return {
            "job_slug": job_slug,
            "description": description,
            "job_path": path,
            "job_final": job_final,
            "feed_item_id": feed_item_id,
            "baseline_duration_s": round(baseline_cut, 2),
            "probes": [r.summary() for r in results.values()],
        }


# ---------------------------------------------------------------------------
# Reporting
# ---------------------------------------------------------------------------

def _print_probe_table(summaries: list[dict[str, Any]]) -> None:
    header = f"{'endpoint':28}{'n':>5}  {'p50 ms':>8}  {'p95 ms':>8}  {'max ms':>8}  status"
    print(header)
    print("-" * len(header))
    for s in summaries:
        if not s.get("samples"):
            errs = "; ".join(s.get("errors", [])) or "no samples"
            print(f"{s['name']:28}{0:>5}  {'-':>8}  {'-':>8}  {'-':>8}  ({errs})")
            continue
        print(
            f"{s['name']:28}{s['samples']:>5}  "
            f"{s['p50_ms']:>8.1f}  {s['p95_ms']:>8.1f}  {s['max_ms']:>8.1f}  "
            f"{s['status']}"
        )
        if s.get("errors"):
            print(f"  errors: {'; '.join(s['errors'])}")


def _check_budget(
    summaries: list[dict[str, Any]],
    *,
    max_p95_ms: float | None,
    max_timeouts: int | None,
    job_slug: str,
) -> list[str]:
    """Return a list of budget violations (empty if all probes passed)."""
    violations: list[str] = []
    for s in summaries:
        if not s.get("samples"):
            if max_timeouts is not None and s.get("errors"):
                violations.append(
                    f"[{job_slug}] {s['name']}: no successful samples, errors: "
                    f"{'; '.join(s['errors'])}"
                )
            continue
        if max_p95_ms is not None and s["p95_ms"] > max_p95_ms:
            violations.append(
                f"[{job_slug}] {s['name']}: p95 {s['p95_ms']:.0f} ms exceeds "
                f"budget {max_p95_ms:.0f} ms"
            )
        if max_timeouts is not None:
            err_count = len(s.get("errors", []))
            if err_count > max_timeouts:
                violations.append(
                    f"[{job_slug}] {s['name']}: {err_count} client errors "
                    f"(budget: {max_timeouts})"
                )
    return violations


async def _main_async(args: argparse.Namespace) -> int:
    job_list = list(JOBS) if args.job == "all" else [args.job]
    all_violations: list[str] = []
    for slug in job_list:
        if slug != "none" and slug not in JOBS:
            print(f"[probe] Unknown job slug: {slug!r}", file=sys.stderr)
            return 2
        print(f"\n=== Probe run: job={slug} duration={args.duration:.0f}s ===")
        summary = await run_probes_for_job(args.base_url, slug, args.duration)
        _print_probe_table(summary["probes"])
        if summary.get("job_final", {}).get("final_status"):
            final = summary["job_final"]["final_status"]
            print(
                f"\njob final: status={final.get('status')} "
                f"processed={final.get('processed')} total={final.get('total')} "
                f"message={final.get('message')}"
            )
        if args.json_out:
            with open(args.json_out, "a", encoding="utf-8") as fh:
                fh.write(json.dumps(summary) + "\n")

        all_violations.extend(
            _check_budget(
                summary["probes"],
                max_p95_ms=args.max_p95,
                max_timeouts=args.max_timeouts,
                job_slug=slug,
            )
        )

    if all_violations:
        print("\nBUDGET VIOLATIONS:", file=sys.stderr)
        for v in all_violations:
            print(f"  - {v}", file=sys.stderr)
        return 1
    if args.max_p95 is not None or args.max_timeouts is not None:
        print("\nAll probes within budget.")
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__ or "")
    parser.add_argument(
        "--base-url",
        default="http://127.0.0.1:8000",
        help="Base URL of the running ALMa backend.",
    )
    parser.add_argument(
        "--job",
        default="discovery",
        choices=["none", "all", *JOBS.keys()],
        help="Which heavy job to run during the probe window.",
    )
    parser.add_argument(
        "--duration",
        type=float,
        default=20.0,
        help="Seconds to probe after the job is scheduled (default 20).",
    )
    parser.add_argument(
        "--json-out",
        default=None,
        help="If set, append per-run JSON summaries to this file.",
    )
    parser.add_argument(
        "--max-p95",
        type=float,
        default=None,
        help=(
            "If set, the script exits non-zero when any probe endpoint's "
            "p95 exceeds this budget in milliseconds. Intended for periodic "
            "regression runs."
        ),
    )
    parser.add_argument(
        "--max-timeouts",
        type=int,
        default=None,
        help=(
            "If set, the script exits non-zero when any probe endpoint "
            "accumulates more than this many client errors (timeouts, "
            "connection failures). Use 0 to require zero errors."
        ),
    )
    args = parser.parse_args()
    return asyncio.run(_main_async(args))


if __name__ == "__main__":
    raise SystemExit(main())
