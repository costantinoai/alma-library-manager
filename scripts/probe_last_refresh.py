"""Probe: /feed/status and /discovery/status return last successful refresh."""

from __future__ import annotations

import asyncio
import os
import sys
import tempfile
from pathlib import Path

import httpx


def main() -> int:
    tmp = tempfile.TemporaryDirectory()
    db_path = Path(tmp.name) / "scholar.db"
    os.environ["DB_PATH"] = str(db_path)
    os.environ["AUTHORS_DB_PATH"] = str(db_path)
    os.environ["PUBLICATIONS_DB_PATH"] = str(db_path)
    os.environ.setdefault("ALMA_API_KEY", "")

    from alma.api import deps as api_deps
    from alma.api.app import app
    from alma.core.operations.activity import persist_operation_status
    from alma.core.operations.models import OperationContext

    api_deps._schema_initialized = False
    api_deps._schema_initialized_path = None
    api_deps.init_db_schema()

    db = api_deps.open_db_connection()

    def seed(op_key: str, finished_at: str) -> None:
        ctx = OperationContext(
            operation_key=op_key,
            trigger_source="user",
            actor="probe",
            correlation_id="probe",
        )
        ctx.started_at = finished_at
        ctx.finished_at = finished_at
        ctx.status = "completed"
        persist_operation_status(db, ctx)
        db.commit()

    seed("feed.refresh_inbox", "2026-04-22T14:30:00")
    seed("feed.refresh_inbox", "2026-04-22T15:10:00")
    seed("discovery.refresh_recommendations", "2026-04-22T13:05:00")

    async def run() -> tuple[dict, dict]:
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(transport=transport, base_url="http://testserver") as client:
            a = await client.get("/api/v1/feed/status")
            b = await client.get("/api/v1/discovery/status")
            return a.json(), b.json()

    feed_body, disc_body = asyncio.run(run())
    print("feed/status:", feed_body)
    print("discovery/status:", disc_body)

    ok = (
        feed_body.get("last_refresh_at") == "2026-04-22T15:10:00"
        and disc_body.get("last_refresh_at") == "2026-04-22T13:05:00"
    )
    print("PASS" if ok else "FAIL")
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
