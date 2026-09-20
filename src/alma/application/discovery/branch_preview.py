"""Stored branch previews; explicit writes use the central async variant owner."""

from __future__ import annotations

import hashlib
import json
import sqlite3
from datetime import datetime

from alma.application import materialized_views as mv
from alma.core.time import utcnow

from .lens_crud import get_lens
from .seed_profile import preview_lens_branches

# Preview is an interactive inspection, not a ranking input. Revalidate at most
# once a minute on opening; an explicit Preview bypasses this reuse window.
REUSE_SECONDS = 60
PREVIEW_VERSION = 1


def preview_key(db: sqlite3.Connection, lens_id: str, options: dict) -> str:
    lens = get_lens(db, lens_id)
    if lens is None:
        raise KeyError(lens_id)
    identity = [PREVIEW_VERSION, options, lens.get("branch_controls"),
                lens.get("context_type"), lens.get("context_config"), lens.get("last_suggestion_set_id")]
    digest = hashlib.sha256(json.dumps(identity, sort_keys=True).encode()).hexdigest()[:24]
    return f"discovery:branches:{lens_id}:v={digest}"


def read_preview(db: sqlite3.Connection, lens_id: str, options: dict) -> dict | None:
    row = mv.read_variant_row(db, preview_key(db, lens_id, options))
    return json.loads(row["payload"]) if row and row.get("payload") else None


def request_preview(
    db: sqlite3.Connection, lens_id: str, options: dict, *, force: bool = False,
) -> dict:
    """Only schedule here; the request connection never computes branches."""
    from alma.api.scheduler import find_active_job

    key = preview_key(db, lens_id, options)
    previous = mv.read_variant_row(db, key)

    def is_fresh(stamp: str) -> bool:
        if force:
            return False
        return (utcnow() - datetime.fromisoformat(stamp)).total_seconds() < REUSE_SECONDS

    def build(conn: sqlite3.Connection) -> dict:
        result = preview_lens_branches(conn, lens_id, **options)
        if result is None:
            raise ValueError("Lens no longer exists")
        return result

    result = mv.get_or_enqueue_variant(
        db, view_key=key, build_fn=build,
        make_fingerprint=lambda conn: utcnow().isoformat(),
        is_fresh=is_fresh, job_label="Discovery branch preview",
    )
    job = find_active_job(f"materialize.variant:{key}")
    if result is None and job is None:
        published = mv.read_variant_row(db, key)
        if not published or (previous and published["fingerprint"] == previous["fingerprint"]):
            raise RuntimeError("Branch preview could not be scheduled; retry")
    return {"job_id": job.get("job_id") if job else None}
