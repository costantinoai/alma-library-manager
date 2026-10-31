#!/usr/bin/env python3
"""The D27 smoke: core works with an add-on missing from the build.

Runs against a private temporary profile, always on a brand-new database, so an
add-on's tables truly do not exist. Nothing reaches the network or a real
profile. Two modes:

    addon_smoke.py describe                    what each add-on declares, as JSON
    addon_smoke.py check <id> --expect <json>  drive core with that add-on gone

``--block`` makes the add-on unimportable with a ``sys.meta_path`` finder, for
the fast in-repo test. Without it the caller has deleted the folder from a copy
and points ``PYTHONPATH`` at that copy; ``--expect-root`` proves which tree ran.
"""

from __future__ import annotations

import argparse
import json
import os
import sqlite3
import sys
import tempfile
import time
from pathlib import Path

#: Every core read a page makes on load. A 2xx is the bar: a stored view that is
#: not built yet answers 202, which is the normal state of a fresh database.
CORE_READS = (
    "/api/v1/bootstrap",
    "/api/v1/home/brief",
    "/api/v1/feed",
    "/api/v1/feed/monitors",
    "/api/v1/feed/status",
    "/api/v1/lenses",
    "/api/v1/discovery/settings",
    "/api/v1/discovery/status",
    "/api/v1/authors",
    "/api/v1/authors/suggestions",
    "/api/v1/authors/needs-attention",
    "/api/v1/library/saved",
    "/api/v1/library/collections",
    "/api/v1/library/tags",
    "/api/v1/library/workflow-summary",
    "/api/v1/library-mgmt/info",
    "/api/v1/health",
    "/api/v1/health/operations",
    "/api/v1/insights",
    "/api/v1/insights/diagnostics",
    "/api/v1/settings",
    "/api/v1/settings/export",
    "/api/v1/plugins",
    "/api/v1/addons",
)


def isolate() -> None:
    """Point every location at a temp dir. Must run before any `alma` import."""
    base = Path(tempfile.mkdtemp(prefix="alma-addon-smoke-"))
    os.environ.update(
        {
            "DB_PATH": str(base / "scholar.db"),
            "DATA_DIR": str(base),
            "ALMA_CONFIG_DIR": str(base),
            "ALMA_SETTINGS_PATH": str(base / "settings.json"),
            "ALMA_SECRETS_PATH": str(base / "secrets.json"),
            "ALMA_ENV": "addonsmoke",
            "ALMA_DISABLE_NETWORK": "1",
            "ALMA_DISABLE_IDLE_MAINTENANCE": "1",
            "ALMA_DISABLE_AUTH": "1",
            "SCHEDULER_ENABLED": "true",
        }
    )


def block(addon_id: str) -> None:
    """Make `alma.addons.<id>` raise as if its folder were deleted."""

    class Blocker:
        name = f"alma.addons.{addon_id}"

        def find_spec(self, fullname, path=None, target=None):
            if fullname == self.name or fullname.startswith(self.name + "."):
                raise ModuleNotFoundError(f"No module named {fullname!r}", name=fullname)
            return None

    sys.meta_path.insert(0, Blocker())


def describe() -> int:
    isolate()
    from alma.core import addons

    print(
        json.dumps(
            {
                addon.id: {
                    "prefixes": [prefix for _router, prefix, _tags in addon.routers()],
                    "jobs": [spec.job_id for spec in addon.jobs],
                    "tables": list(addon.tables),
                    "plugins": [manifest.id for manifest in addon.plugins()],
                }
                for addon in addons.present()
            }
        )
    )
    return 0


def seed_paper(db_path: str) -> str:
    paper_id = "00000000-0000-0000-0000-00000000d027"
    conn = sqlite3.connect(db_path)
    conn.execute(
        "INSERT INTO papers (id, title, year, status) VALUES (?, 'D27 smoke paper', 2026, 'tracked')",
        (paper_id,),
    )
    conn.commit()
    conn.close()
    return paper_id


def wait_for_job(client, job_id: str) -> str:
    deadline = time.monotonic() + 60
    while time.monotonic() < deadline:
        response = client.get(f"/api/v1/activity/{job_id}")
        if response.status_code == 200:
            state = response.json().get("status")
            if state in {"completed", "failed", "noop", "cancelled"}:
                return str(state)
        time.sleep(0.2)
    return "timed out"


def check(args: argparse.Namespace) -> int:
    isolate()
    if args.block:
        block(args.addon)

    import alma

    root = str(Path(alma.__file__).resolve())
    if args.expect_root and not root.startswith(str(Path(args.expect_root).resolve())):
        print(f"FAILED: ran the wrong tree: {root}")
        return 1

    from fastapi.testclient import TestClient

    from alma.api.app import app
    from alma.api.scheduler import get_running_scheduler
    from alma.core import addons

    expected = json.loads(args.expect)
    failures: list[str] = []
    if any(addon.id == args.addon for addon in addons.present()):
        print(f"FAILED: {args.addon} is still loaded")
        return 1

    # The origin guard only knows local names. A 500 must read as a failure line.
    with TestClient(app, base_url="http://localhost", raise_server_exceptions=False) as client:
        names = {
            row[0]
            for row in sqlite3.connect(os.environ["DB_PATH"]).execute(
                "SELECT name FROM sqlite_master WHERE type = 'table'"
            )
        }
        for table in expected["tables"]:
            if table in names:
                failures.append(f"core bootstrap created add-on table {table}")

        for path in CORE_READS:
            response = client.get(path)
            if response.status_code >= 300:
                failures.append(f"GET {path} -> {response.status_code}")

        paper_id = seed_paper(os.environ["DB_PATH"])
        saved = client.post(
            f"/api/v1/papers/{paper_id}/action", json={"action": "add", "surface": "library"}
        )
        if saved.status_code >= 300:
            failures.append(f"save a paper -> {saved.status_code} {saved.text[:200]}")
        listed = client.get("/api/v1/library/saved").text
        if paper_id not in listed:
            failures.append("the saved paper is not in the Library")
        created = client.post("/api/v1/library/collections", json={"name": "D27"})
        if created.status_code >= 300:
            failures.append(f"create a collection -> {created.status_code}")

        queued = client.post("/api/v1/library-mgmt/deduplicate")
        if queued.status_code >= 300:
            failures.append(f"deduplicate -> {queued.status_code}")
        else:
            outcome = wait_for_job(client, queued.json()["job_id"])
            if outcome != "completed":
                failures.append(f"deduplicate job -> {outcome}")

        for prefix in expected["prefixes"]:
            response = client.get(prefix)
            if response.status_code != 404:
                failures.append(f"GET {prefix} -> {response.status_code}, wanted 404")
        plugin_ids = {entry["id"] for entry in client.get("/api/v1/plugins").json()}
        for plugin in expected["plugins"]:
            if plugin in plugin_ids:
                failures.append(f"plugin {plugin} is still listed")
        addon_ids = {entry["id"] for entry in client.get("/api/v1/addons").json()}
        if args.addon in addon_ids:
            failures.append(f"{args.addon} is still listed in /addons")

        sched = get_running_scheduler()
        if sched is None:
            failures.append("the scheduler did not start")
        else:
            for job in expected["jobs"]:
                if sched.get_job(job) is not None:
                    failures.append(f"job {job} is still scheduled")

    for failure in failures:
        print(f"FAILED: {failure}")
    print(f"{args.addon}: {len(CORE_READS)} core reads, {len(failures)} failures, from {root}")
    return int(bool(failures))


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)
    sub.add_parser("describe")
    check_parser = sub.add_parser("check")
    check_parser.add_argument("addon")
    check_parser.add_argument("--expect", required=True, help="this add-on's `describe` entry")
    check_parser.add_argument("--block", action="store_true")
    check_parser.add_argument("--expect-root")
    args = parser.parse_args()
    return describe() if args.command == "describe" else check(args)


if __name__ == "__main__":
    sys.exit(main())
