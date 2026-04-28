"""Helpers for canonical followed-author identity management."""

from __future__ import annotations

import logging
import math
import sqlite3
import uuid
from datetime import datetime
from typing import Any, Optional

logger = logging.getLogger(__name__)
_AUTHOR_BACKFILL_STALE_AFTER_DAYS = 45
_AUTHOR_BACKFILL_MIN_EXPECTED = 5
_AUTHOR_BACKFILL_EXPECTED_RATIO = 0.20


def normalize_openalex_author_id(value: str | None) -> str:
    """Return a normalized bare OpenAlex author ID when possible."""
    raw = str(value or "").strip()
    if not raw:
        return ""
    lower = raw.lower()
    for prefix in ("https://openalex.org/", "http://openalex.org/"):
        if lower.startswith(prefix):
            raw = raw.rstrip("/").split("/")[-1]
            break
    return raw.strip()


def _table_exists(db: sqlite3.Connection, table: str) -> bool:
    row = db.execute(
        "SELECT name FROM sqlite_master WHERE type = 'table' AND name = ?",
        (table,),
    ).fetchone()
    return row is not None


def _table_columns(db: sqlite3.Connection, table: str) -> set[str]:
    try:
        rows = db.execute(f"PRAGMA table_info({table})").fetchall()
    except sqlite3.OperationalError:
        return set()
    return {str(r[1]) for r in rows}


def _parse_iso_datetime(value: object) -> Optional[datetime]:
    text = str(value or "").strip()
    if not text:
        return None
    try:
        return datetime.fromisoformat(text)
    except Exception:
        return None


def _insert_author_row(
    db: sqlite3.Connection,
    *,
    author_id: str,
    name: str,
    openalex_id: Optional[str] = None,
) -> None:
    columns = _table_columns(db, "authors")
    if "id" not in columns or "name" not in columns:
        return

    payload: dict[str, object] = {
        "id": author_id,
        "name": name,
    }
    if "openalex_id" in columns and openalex_id:
        payload["openalex_id"] = openalex_id
    if "author_type" in columns:
        payload["author_type"] = "followed"
    if "added_at" in columns:
        payload["added_at"] = datetime.utcnow().isoformat()
    if "id_resolution_status" in columns:
        payload["id_resolution_status"] = "resolved_manual" if openalex_id else "unresolved"
    if "id_resolution_reason" in columns:
        payload["id_resolution_reason"] = "Created from followed_authors canonicalization"
    if "id_resolution_updated_at" in columns:
        payload["id_resolution_updated_at"] = datetime.utcnow().isoformat()

    col_sql = ", ".join(payload.keys())
    placeholder_sql = ", ".join("?" for _ in payload)
    db.execute(
        f"INSERT OR IGNORE INTO authors ({col_sql}) VALUES ({placeholder_sql})",
        list(payload.values()),
    )


def resolve_canonical_author_id(
    db: sqlite3.Connection,
    author_ref: str,
    *,
    create_if_missing: bool = False,
    fallback_name: Optional[str] = None,
) -> Optional[str]:
    """Resolve any author reference to the canonical ``authors.id`` contract."""
    if not _table_exists(db, "authors"):
        return str(author_ref or "").strip() or None

    raw = str(author_ref or "").strip()
    if not raw:
        return None

    try:
        row = db.execute(
            "SELECT id FROM authors WHERE id = ? LIMIT 1",
            (raw,),
        ).fetchone()
        if row:
            return str(row["id"] if isinstance(row, sqlite3.Row) else row[0])
    except sqlite3.OperationalError:
        return raw

    columns = _table_columns(db, "authors")
    normalized_oa = normalize_openalex_author_id(raw)
    if "openalex_id" in columns and normalized_oa:
        row = db.execute(
            "SELECT id FROM authors WHERE lower(openalex_id) = lower(?) LIMIT 1",
            (normalized_oa,),
        ).fetchone()
        if row:
            return str(row["id"] if isinstance(row, sqlite3.Row) else row[0])

    if not create_if_missing:
        return None

    canonical_id = normalized_oa or raw
    name = (fallback_name or normalized_oa or raw).strip() or canonical_id
    try:
        _insert_author_row(
            db,
            author_id=canonical_id,
            name=name,
            openalex_id=normalized_oa or None,
        )
    except Exception as exc:
        logger.warning("Failed to create canonical author row for '%s': %s", raw, exc)
        return None
    return canonical_id


def ensure_followed_author_contract(db: sqlite3.Connection) -> int:
    """Backfill ``followed_authors`` so every row references a real ``authors.id``."""
    if not _table_exists(db, "followed_authors"):
        return 0

    rows = db.execute(
        "SELECT author_id, followed_at, notify_new_papers FROM followed_authors"
    ).fetchall()
    changed = 0

    for row in rows:
        raw_author_id = str(row["author_id"] if isinstance(row, sqlite3.Row) else row[0]).strip()
        followed_at = str(row["followed_at"] if isinstance(row, sqlite3.Row) else row[1] or "")
        notify_new = int((row["notify_new_papers"] if isinstance(row, sqlite3.Row) else row[2]) or 0)
        canonical_id = resolve_canonical_author_id(
            db,
            raw_author_id,
            create_if_missing=True,
            fallback_name=normalize_openalex_author_id(raw_author_id) or raw_author_id,
        )
        if not canonical_id:
            continue
        if canonical_id == raw_author_id:
            continue

        existing = db.execute(
            "SELECT followed_at, notify_new_papers FROM followed_authors WHERE author_id = ?",
            (canonical_id,),
        ).fetchone()
        if existing:
            existing_followed_at = str(existing["followed_at"] if isinstance(existing, sqlite3.Row) else existing[0] or "")
            merged_followed_at = min(
                [v for v in (followed_at, existing_followed_at) if v],
                default=datetime.utcnow().isoformat(),
            )
            merged_notify = max(
                notify_new,
                int((existing["notify_new_papers"] if isinstance(existing, sqlite3.Row) else existing[1]) or 0),
            )
            db.execute(
                "UPDATE followed_authors SET followed_at = ?, notify_new_papers = ? WHERE author_id = ?",
                (merged_followed_at, merged_notify, canonical_id),
            )
            db.execute("DELETE FROM followed_authors WHERE author_id = ?", (raw_author_id,))
        else:
            db.execute(
                "UPDATE followed_authors SET author_id = ? WHERE author_id = ?",
                (canonical_id, raw_author_id),
            )
        changed += 1

    return changed


def apply_follow_state(
    db: sqlite3.Connection,
    author_id: str,
    *,
    followed: bool,
) -> None:
    """Atomically flip one author's follow state across all three tables.

    Three tables encode follow state and they must stay in lock-step:

    1. ``followed_authors`` — the canonical list surfaced on the Authors
       page. Primary source of truth.
    2. ``authors.author_type`` — denormalized flag read by list/detail
       endpoints (``followed`` vs ``background``).
    3. ``feed_monitors`` (``monitor_type='author'``) — the monitor row
       that powers Feed refresh for this author.

    Historically these drifted because each write path touched a subset
    (import pipelines set ``author_type='followed'`` without inserting
    into ``followed_authors``; ``_sync_follow_state`` in the authors route
    updated two of three and left the monitor for the next Feed refresh).
    This helper is the single entry point: callers flip follow state
    through here and every table lands in the same state, same
    transaction, same request.
    """
    ensure_followed_author_contract(db)
    # ``followed_authors`` is the primary key in this module; the other
    # two are mirrors.
    db.execute(
        """CREATE TABLE IF NOT EXISTS followed_authors (
            author_id TEXT PRIMARY KEY,
            followed_at TEXT NOT NULL,
            notify_new_papers INTEGER DEFAULT 1
        )"""
    )
    now = datetime.utcnow().isoformat()
    if followed:
        db.execute(
            """
            INSERT OR IGNORE INTO followed_authors (author_id, followed_at, notify_new_papers)
            VALUES (?, ?, 1)
            """,
            (author_id, now),
        )
        db.execute("UPDATE authors SET author_type = 'followed' WHERE id = ?", (author_id,))
    else:
        db.execute("DELETE FROM followed_authors WHERE author_id = ?", (author_id,))
        db.execute("UPDATE authors SET author_type = 'background' WHERE id = ?", (author_id,))

    # Mirror into feed_monitors. Import here to avoid a circular dep at
    # module load — feed_monitors imports from followed_authors as well.
    from alma.application.feed_monitors import sync_author_monitors

    sync_author_monitors(db)

    # Preventive ORCID alias recording on the follow path. As soon as
    # the user follows an author, query OpenAlex for every other
    # author profile sharing the same ORCID and INSERT each into
    # `author_alt_identifiers`. The suggestion rail's `followed_ids`
    # UNION (see `list_author_suggestions`) then filters those alts
    # out automatically — the user never sees them as fresh
    # suggestions, and a future follow attempt on one of those alts
    # will already know it's a known split. Fire-and-forget on a
    # short-lived background thread so the follow API call stays
    # snappy even when OpenAlex is slow. Best-effort: any failure is
    # silently swallowed; the worst case is the user sees a
    # duplicate suggestion they could have been spared.
    if followed:
        _schedule_orcid_alias_recording(author_id)

    # Eager GC after unfollow: the author may now be orphan (no live
    # paper attachment AND no longer followed). Audited soft-remove.
    # Skipped on the follow path because following an author is by
    # definition a "reason to keep" — `is_orphan_author` would
    # short-circuit anyway, but skipping the call is cheaper.
    if not followed:
        from alma.application.author_lifecycle import gc_author_if_orphan
        from alma.application.gap_radar import record_missing_author_remove

        gc_author_if_orphan(db, author_id, reason="unfollowed")

        # Suppress this author from /authors/suggestions. Without this,
        # an unfollowed author whose corpus already lives in the local
        # publication_authors / publication_references graph immediately
        # re-surfaces in `library_core` / `adjacent` / `cited_by_high_signal`
        # buckets — the author rail uses `missing_author_feedback` as the
        # single suppression mechanism (`get_missing_author_feedback_state`
        # → `suppressed`), and unfollow used to skip writing it.
        # Hard signal (-0.82, 120d half-life) because unfollow is a
        # deliberate "stop showing me this person" action, stronger than
        # a one-off suggestion dismiss.
        oid_row = db.execute(
            "SELECT openalex_id FROM authors WHERE id = ?", (author_id,),
        ).fetchone()
        oid = str((oid_row["openalex_id"] if oid_row else "") or "").strip()
        if oid:
            try:
                record_missing_author_remove(db, oid, hard=True)
            except ValueError:
                pass


def _schedule_orcid_alias_recording(author_id: str) -> None:
    """Fire-and-forget worker that records ORCID-discovered aliases.

    Runs on the shared scheduler thread pool (`schedule_immediate`)
    with its own short-lived DB connection. Wrapped in broad
    try/except so an OpenAlex outage never poisons the follow flow
    that triggered it.
    """
    try:
        from alma.api.scheduler import schedule_immediate
    except Exception:
        # Scheduler unavailable (probe / test harness without app
        # context). Skip silently — the alias table just won't get
        # the proactive entries.
        logger.debug("scheduler unavailable, skipping ORCID alias discovery")
        return

    def _runner() -> dict:
        from alma.api.deps import open_db_connection
        from alma.application.author_merge import record_orcid_aliases

        conn = open_db_connection()
        try:
            try:
                result = record_orcid_aliases(conn, author_id)
            except Exception as exc:  # pragma: no cover - best-effort
                logger.warning(
                    "ORCID alias recording failed for %s: %s", author_id, exc,
                )
                return {"error": str(exc)}
            return result
        finally:
            conn.close()

    job_id = f"orcid_aliases_{uuid.uuid4().hex[:10]}"
    try:
        schedule_immediate(job_id, _runner)
    except Exception:
        # Scheduler may reject during shutdown; non-fatal.
        logger.debug("schedule_immediate refused ORCID alias job", exc_info=True)


def schedule_followed_author_historical_backfill(
    author_id: str,
    *,
    trigger: str = "follow",
) -> Optional[dict]:
    """Queue a full-history author refresh for a followed author.

    Consolidated 2026-04-24: this used to create an `authors.history_backfill`
    operation_key distinct from `authors.deep_refresh:{id}`, which meant the
    two jobs didn't dedup and could race on the same author's papers (the 2h
    double-failure under a `UNIQUE constraint failed: papers.openalex_id`).
    Both entry points now share `authors.deep_refresh:{author_id}` as their
    canonical operation_key, so the Activity envelope's `find_active_job`
    rejects the second queuing and returns the in-flight job.
    """
    author_key = str(author_id or "").strip()
    if not author_key:
        return None

    from alma.api.scheduler import (
        activity_envelope,
        add_job_log,
        find_active_job,
        schedule_immediate,
        set_job_status,
    )
    from alma.api.deps import open_db_connection

    operation_key = f"authors.deep_refresh:{author_key}"
    existing = find_active_job(operation_key)
    if existing:
        return activity_envelope(
            str(existing.get("job_id") or ""),
            status="already_running",
            operation_key=operation_key,
            message=f"Author refresh already running for {author_key}",
        )

    job_id = f"author_deep_refresh_{uuid.uuid4().hex[:10]}"
    started_at = datetime.utcnow().isoformat()
    set_job_status(
        job_id,
        status="queued",
        started_at=started_at,
        updated_at=started_at,
        operation_key=operation_key,
        trigger_source=trigger,
        message=f"Queued historical backfill for followed author {author_key}",
        current_author=author_key,
    )

    def _runner() -> dict:
        conn = open_db_connection()
        try:
            from alma.api.routes.authors import _refresh_author_cache_impl

            add_job_log(
                job_id,
                f"Historical corpus backfill started for {author_key}",
                step="history_backfill_start",
                data={"author_id": author_key, "trigger": trigger},
            )
            result = _refresh_author_cache_impl(
                conn,
                author_key,
                mode="deep",
                job_id=job_id,
            )
            add_job_log(
                job_id,
                f"Historical corpus backfill finished for {author_key}",
                step="history_backfill_done",
                data=result,
            )
            conn.commit()
            return result
        finally:
            conn.close()

    schedule_immediate(job_id, _runner)
    return activity_envelope(
        job_id,
        status="queued",
        operation_key=operation_key,
        message=f"Queued historical corpus backfill for {author_key}",
    )


def get_followed_author_backfill_status(
    db: sqlite3.Connection,
    author_id: str,
    *,
    background_publications: Optional[int] = None,
    works_count: Optional[int] = None,
) -> dict[str, Any]:
    """Return freshness and maintenance status for a followed-author corpus."""
    author_key = str(author_id or "").strip()
    base = {
        "author_id": author_key,
        "is_followed": False,
        "state": "not_followed",
        "stale": False,
        "thin": False,
        "background_publications": int(background_publications or 0),
        "works_count": int(works_count or 0),
        "coverage_ratio": None,
        "expected_background_floor": 0,
        "last_job_id": None,
        "last_status": None,
        "last_started_at": None,
        "last_finished_at": None,
        "last_success_at": None,
        "last_error": None,
        "age_days": None,
        "detail": "Author is not currently followed.",
        "recent_runs": [],
    }
    if not author_key or not _table_exists(db, "followed_authors"):
        return base

    followed_row = db.execute(
        "SELECT 1 FROM followed_authors WHERE author_id = ? LIMIT 1",
        (author_key,),
    ).fetchone()
    if not followed_row:
        return base

    background_count = int(background_publications or 0)
    if background_publications is None and _table_exists(db, "publication_authors"):
        try:
            row = db.execute(
                """
                SELECT COUNT(DISTINCT p.id) AS c
                FROM papers p
                JOIN publication_authors pa ON pa.paper_id = p.id
                JOIN authors a ON lower(trim(a.openalex_id)) = lower(trim(pa.openalex_id))
                WHERE a.id = ?
                  AND p.status <> 'library'
                """,
                (author_key,),
            ).fetchone()
            background_count = int((row["c"] if row else 0) or 0)
        except sqlite3.OperationalError:
            background_count = int(background_publications or 0)

    total_works = int(works_count or 0)
    if works_count is None and _table_exists(db, "authors"):
        try:
            row = db.execute(
                "SELECT COALESCE(works_count, 0) AS works_count FROM authors WHERE id = ?",
                (author_key,),
            ).fetchone()
            total_works = int((row["works_count"] if row else 0) or 0)
        except sqlite3.OperationalError:
            total_works = int(works_count or 0)

    expected_floor = 0
    coverage_ratio = None
    if total_works > 0:
        expected_floor = min(
            25,
            max(_AUTHOR_BACKFILL_MIN_EXPECTED, int(math.ceil(total_works * _AUTHOR_BACKFILL_EXPECTED_RATIO))),
        )
        coverage_ratio = round(background_count / float(total_works), 3)

    rows: list[sqlite3.Row] = []
    if _table_exists(db, "operation_status"):
        try:
            rows = db.execute(
                """
                SELECT
                    job_id,
                    status,
                    message,
                    error,
                    started_at,
                    finished_at,
                    updated_at,
                    operation_key,
                    trigger_source
                FROM operation_status
                WHERE (operation_key = 'authors.history_backfill' AND current_author = ?)
                   OR operation_key = ?
                ORDER BY COALESCE(finished_at, updated_at, started_at) DESC
                LIMIT 8
                """,
                (author_key, f"authors.deep_refresh:{author_key}"),
            ).fetchall()
        except sqlite3.OperationalError:
            rows = []

    latest = rows[0] if rows else None
    recent_runs: list[dict[str, Any]] = []
    latest_success: Optional[sqlite3.Row] = None
    latest_failure: Optional[sqlite3.Row] = None
    active_run: Optional[sqlite3.Row] = None
    for row in rows:
        status = str(row["status"] or "").strip().lower()
        if active_run is None and status in {"queued", "running", "cancelling"}:
            active_run = row
        if latest_success is None and status == "completed":
            latest_success = row
        if latest_failure is None and status == "failed":
            latest_failure = row
        recent_runs.append(
            {
                "job_id": str(row["job_id"] or "").strip(),
                "status": status or "unknown",
                "message": str(row["message"] or "").strip() or None,
                "started_at": str(row["started_at"] or "").strip() or None,
                "finished_at": str(row["finished_at"] or "").strip() or None,
                "operation_key": str(row["operation_key"] or "").strip() or None,
                "trigger_source": str(row["trigger_source"] or "").strip() or None,
            }
        )

    success_at = _parse_iso_datetime((latest_success["finished_at"] if latest_success else None) or (latest_success["updated_at"] if latest_success else None))
    age_days: Optional[int] = None
    if success_at is not None:
        age_days = max(0, int((datetime.utcnow() - success_at).total_seconds() // 86400))

    thin = bool(expected_floor and background_count < expected_floor)
    stale = bool(success_at is not None and age_days is not None and age_days >= _AUTHOR_BACKFILL_STALE_AFTER_DAYS)

    if active_run is not None:
        state = "running"
        detail = "Historical corpus backfill is currently running."
    elif latest_success is None and background_count <= 0:
        state = "pending"
        detail = "Historical corpus has not been built yet for this followed author."
    elif latest_success is None and background_count > 0:
        state = "unverified"
        detail = "Background corpus exists, but ALMa has no successful full-history backfill recorded yet."
    elif stale:
        state = "stale"
        detail = f"Last full-history backfill is {age_days} days old."
    elif thin:
        state = "thin"
        if total_works > 0:
            detail = (
                f"Background corpus looks thin ({background_count}/{total_works} papers cached)."
            )
        else:
            detail = "Background corpus exists but still looks thin."
    elif latest_failure is not None and latest_success is None:
        state = "failed"
        detail = "The last historical corpus attempt failed."
    else:
        state = "fresh"
        detail = "Historical corpus is present and recent enough for downstream learning."

    return {
        "author_id": author_key,
        "is_followed": True,
        "state": state,
        "stale": stale,
        "thin": thin,
        "background_publications": background_count,
        "works_count": total_works,
        "coverage_ratio": coverage_ratio,
        "expected_background_floor": expected_floor,
        "last_job_id": str(latest["job_id"] or "").strip() if latest else None,
        "last_status": str(latest["status"] or "").strip().lower() if latest else None,
        "last_started_at": str(latest["started_at"] or "").strip() if latest and str(latest["started_at"] or "").strip() else None,
        "last_finished_at": str(latest["finished_at"] or "").strip() if latest and str(latest["finished_at"] or "").strip() else None,
        "last_success_at": success_at.isoformat() if success_at is not None else None,
        "last_error": str(latest_failure["error"] or latest_failure["message"] or "").strip() if latest_failure else None,
        "age_days": age_days,
        "detail": detail,
        "recent_runs": recent_runs[:4],
    }
