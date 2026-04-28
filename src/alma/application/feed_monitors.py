"""Feed monitor management and diagnostics helpers."""

from __future__ import annotations

from datetime import datetime
import json
import sqlite3
import uuid
from typing import Any

from alma.application.followed_authors import ensure_followed_author_contract
from alma.application.feed_query_language import normalize_keyword_expression


NON_AUTHOR_MONITOR_TYPES = {"query", "topic", "venue", "preprint", "branch"}
AUTHOR_MONITOR_PREFIX = "author:"


def _table_exists(db: sqlite3.Connection, table: str) -> bool:
    row = db.execute(
        "SELECT name FROM sqlite_master WHERE type = 'table' AND name = ?",
        (table,),
    ).fetchone()
    return row is not None


def _json_loads(raw: str | None) -> dict[str, Any]:
    if not raw:
        return {}
    try:
        value = json.loads(raw)
        return value if isinstance(value, dict) else {}
    except Exception:
        return {}


def _json_dumps(value: dict[str, Any] | None) -> str | None:
    if not value:
        return None
    return json.dumps(value, ensure_ascii=False, default=str)


def _clear_monitor_feed_items(db: sqlite3.Connection, monitor_id: str) -> int:
    from . import feed as feed_app

    return int(feed_app.clear_feed_items_for_monitor(db, monitor_id) or 0)


def _canonical_monitor_key(monitor_type: str, query: str) -> str:
    raw = " ".join((query or "").strip().split())
    if monitor_type == "query":
        return raw.lower()
    if monitor_type in {"topic", "venue", "preprint", "branch"}:
        return raw.lower()
    return raw


def _fetch_monitor_row(db: sqlite3.Connection, monitor_id: str) -> sqlite3.Row | None:
    return db.execute(
        """
        SELECT
            fm.*,
            a.name AS author_name,
            a.openalex_id,
            a.scholar_id,
            a.orcid
        FROM feed_monitors fm
        LEFT JOIN authors a ON a.id = fm.author_id
        WHERE fm.id = ?
        """,
        (monitor_id,),
    ).fetchone()


def _serialize_monitor_row(row: sqlite3.Row) -> dict[str, Any]:
    health, health_reason = _monitor_health(row)
    return {
        "id": row["id"],
        "monitor_type": row["monitor_type"],
        "monitor_key": row["monitor_key"],
        "label": row["label"],
        "enabled": bool(int(row["enabled"] or 0)),
        "author_id": row["author_id"],
        "author_name": row["author_name"],
        "openalex_id": row["openalex_id"],
        "scholar_id": row["scholar_id"],
        "orcid": row["orcid"],
        "config": _json_loads(row["config_json"]),
        "created_at": row["created_at"],
        "updated_at": row["updated_at"],
        "last_checked_at": row["last_checked_at"],
        "last_success_at": row["last_success_at"],
        "last_status": row["last_status"],
        "last_error": row["last_error"],
        "last_result": _json_loads(row["last_result_json"]),
        "health": health,
        "health_reason": health_reason,
    }


def sync_author_monitors(db: sqlite3.Connection) -> None:
    """Mirror followed authors into the unified feed_monitors table."""
    if not _table_exists(db, "feed_monitors"):
        return

    ensure_followed_author_contract(db)
    rows = db.execute(
        """
        SELECT fa.author_id, fa.followed_at, a.name
        FROM followed_authors fa
        LEFT JOIN authors a ON a.id = fa.author_id
        """
    ).fetchall()

    now = datetime.utcnow().isoformat()
    active_ids: set[str] = set()
    for row in rows:
        author_id = str(row["author_id"] or "").strip()
        if not author_id:
            continue
        monitor_id = f"{AUTHOR_MONITOR_PREFIX}{author_id}"
        active_ids.add(monitor_id)
        label = str(row["name"] or author_id).strip() or author_id
        created_at = str(row["followed_at"] or now)
        db.execute(
            """
            INSERT INTO feed_monitors (
                id, monitor_type, monitor_key, label, author_id,
                config_json, enabled, created_at, updated_at
            )
            VALUES (?, 'author', ?, ?, ?, NULL, 1, ?, ?)
            ON CONFLICT(id) DO UPDATE SET
                monitor_key = excluded.monitor_key,
                label = excluded.label,
                author_id = excluded.author_id,
                updated_at = excluded.updated_at
            """,
            (monitor_id, author_id, label, author_id, created_at, now),
        )

    stale_rows = db.execute(
        "SELECT id FROM feed_monitors WHERE monitor_type = 'author'"
    ).fetchall()
    for row in stale_rows:
        monitor_id = str(row["id"] or "").strip()
        if monitor_id and monitor_id not in active_ids:
            _clear_monitor_feed_items(db, monitor_id)
            db.execute("DELETE FROM feed_monitors WHERE id = ?", (monitor_id,))

    db.commit()


def _monitor_health(row: sqlite3.Row) -> tuple[str, str | None]:
    enabled = bool(int(row["enabled"] or 0))
    if not enabled:
        return "disabled", "monitor_disabled"

    monitor_type = str(row["monitor_type"] or "").strip()
    if monitor_type == "author":
        author_id = str(row["author_id"] or "").strip()
        if not author_id:
            return "degraded", "missing_author_id"
        if not str(row["openalex_id"] or "").strip():
            if str(row["scholar_id"] or "").strip():
                return "degraded", "missing_openalex_id_for_scholar_monitor"
            return "degraded", "missing_openalex_id"

    last_status = str(row["last_status"] or "").strip().lower()
    if last_status == "failed":
        return "degraded", str(row["last_error"] or "operation_failed").strip() or "operation_failed"
    return "ready", None


def list_feed_monitors(db: sqlite3.Connection) -> list[dict[str, Any]]:
    # NOTE: this is a pure read. Do NOT call sync_author_monitors from here —
    # every follow/unfollow mutation and every refresh already syncs the mirror
    # table, and running a write inside a frequently-polled GET was the source
    # of 500s under SQLite lock contention and of slow Feed-page loads.
    if not _table_exists(db, "feed_monitors"):
        return []

    rows = db.execute(
        """
        SELECT
            fm.*,
            a.name AS author_name,
            a.openalex_id,
            a.scholar_id,
            a.orcid
        FROM feed_monitors fm
        LEFT JOIN authors a ON a.id = fm.author_id
        ORDER BY
            CASE fm.monitor_type WHEN 'author' THEN 0 WHEN 'topic' THEN 1 ELSE 2 END,
            lower(fm.label),
            fm.created_at DESC
        """
    ).fetchall()

    out: list[dict[str, Any]] = []
    for row in rows:
        out.append(_serialize_monitor_row(row))
    return out


def create_feed_monitor(
    db: sqlite3.Connection,
    *,
    monitor_type: str,
    query: str,
    label: str | None = None,
    config: dict[str, Any] | None = None,
) -> dict[str, Any]:
    monitor_type = str(monitor_type or "").strip().lower()
    if monitor_type not in NON_AUTHOR_MONITOR_TYPES:
        raise ValueError(f"Unsupported monitor type: {monitor_type}")

    normalized_query = " ".join((query or "").strip().split())
    if monitor_type == "query":
        normalized_query = normalize_keyword_expression(normalized_query)
    if not normalized_query:
        raise ValueError("Monitor query cannot be empty")
    monitor_key = _canonical_monitor_key(monitor_type, normalized_query)
    monitor_label = " ".join((label or normalized_query).strip().split()) or normalized_query

    existing = db.execute(
        "SELECT id FROM feed_monitors WHERE monitor_type = ? AND lower(monitor_key) = lower(?)",
        (monitor_type, monitor_key),
    ).fetchone()
    if existing:
        raise sqlite3.IntegrityError("Feed monitor already exists")

    now = datetime.utcnow().isoformat()
    monitor_id = uuid.uuid4().hex
    payload = {"query": normalized_query, **(config or {})}
    db.execute(
        """
        INSERT INTO feed_monitors (
            id, monitor_type, monitor_key, label, config_json,
            enabled, created_at, updated_at
        )
        VALUES (?, ?, ?, ?, ?, 1, ?, ?)
        """,
        (monitor_id, monitor_type, monitor_key, monitor_label, _json_dumps(payload), now, now),
    )
    db.commit()
    created = _fetch_monitor_row(db, monitor_id)
    assert created is not None
    return _serialize_monitor_row(created)


def update_feed_monitor(
    db: sqlite3.Connection,
    monitor_id: str,
    *,
    query: str | None = None,
    label: str | None = None,
    config: dict[str, Any] | None = None,
    enabled: bool | None = None,
) -> dict[str, Any] | None:
    row = _fetch_monitor_row(db, monitor_id)
    if row is None:
        return None

    monitor_type = str(row["monitor_type"] or "").strip().lower()
    definition_changed = query is not None or label is not None or config is not None
    current_enabled = int(row["enabled"] or 0)
    next_enabled = int(enabled) if enabled is not None else int(row["enabled"] or 0)

    if monitor_type == "author":
        if definition_changed:
            raise ValueError("Author monitors can only be enabled or disabled from Feed settings")
        now = datetime.utcnow().isoformat()
        if current_enabled != next_enabled and not next_enabled:
            _clear_monitor_feed_items(db, monitor_id)
        if current_enabled != next_enabled and next_enabled:
            db.execute(
                """
                UPDATE feed_monitors
                SET enabled = ?,
                    last_status = NULL,
                    last_error = NULL,
                    updated_at = ?
                WHERE id = ?
                """,
                (next_enabled, now, monitor_id),
            )
        else:
            db.execute(
                """
                UPDATE feed_monitors
                SET enabled = ?,
                    updated_at = ?
                WHERE id = ?
                """,
                (next_enabled, now, monitor_id),
            )
        db.commit()
        updated = _fetch_monitor_row(db, monitor_id)
        assert updated is not None
        return _serialize_monitor_row(updated)

    current_config = _json_loads(row["config_json"])
    next_config = dict(current_config)
    if config:
        next_config.update(config)

    current_query = str(next_config.get("query") or row["monitor_key"] or "").strip()
    next_query = current_query if query is None else " ".join(str(query or "").strip().split())
    if not next_query:
        raise ValueError("Monitor query cannot be empty")
    if monitor_type == "query":
        next_query = normalize_keyword_expression(next_query)

    next_label = str(label if label is not None else row["label"] or "").strip()
    next_label = " ".join(next_label.split()) or next_query
    next_config["query"] = next_query
    next_monitor_key = _canonical_monitor_key(monitor_type, next_query)

    duplicate = db.execute(
        """
        SELECT id
        FROM feed_monitors
        WHERE monitor_type = ? AND lower(monitor_key) = lower(?) AND id <> ?
        """,
        (monitor_type, next_monitor_key, monitor_id),
    ).fetchone()
    if duplicate:
        raise sqlite3.IntegrityError("Feed monitor already exists")

    now = datetime.utcnow().isoformat()
    source_definition_changed = (
        query is not None
        or config is not None
        or next_monitor_key != str(row["monitor_key"] or "").strip()
        or str(next_config.get("query") or "").strip() != str(current_config.get("query") or row["monitor_key"] or "").strip()
    )
    if source_definition_changed or (current_enabled != next_enabled and not next_enabled):
        _clear_monitor_feed_items(db, monitor_id)
    if definition_changed:
        db.execute(
            """
            UPDATE feed_monitors
            SET monitor_key = ?,
                label = ?,
                config_json = ?,
                enabled = ?,
                last_checked_at = NULL,
                last_success_at = NULL,
                last_status = NULL,
                last_error = NULL,
                last_result_json = NULL,
                updated_at = ?
            WHERE id = ?
            """,
            (next_monitor_key, next_label, _json_dumps(next_config), next_enabled, now, monitor_id),
        )
    else:
        if current_enabled != next_enabled and next_enabled:
            db.execute(
                """
                UPDATE feed_monitors
                SET enabled = ?,
                    last_status = NULL,
                    last_error = NULL,
                    updated_at = ?
                WHERE id = ?
                """,
                (next_enabled, now, monitor_id),
            )
        else:
            db.execute(
                """
                UPDATE feed_monitors
                SET enabled = ?,
                    updated_at = ?
                WHERE id = ?
                """,
                (next_enabled, now, monitor_id),
            )
    db.commit()

    updated = _fetch_monitor_row(db, monitor_id)
    assert updated is not None
    return _serialize_monitor_row(updated)


def delete_feed_monitor(db: sqlite3.Connection, monitor_id: str) -> bool:
    row = db.execute(
        "SELECT monitor_type FROM feed_monitors WHERE id = ?",
        (monitor_id,),
    ).fetchone()
    if not row:
        return False
    if str(row["monitor_type"] or "") == "author":
        raise ValueError("Author monitors are owned by the Authors page")
    _clear_monitor_feed_items(db, monitor_id)
    db.execute("DELETE FROM feed_monitors WHERE id = ?", (monitor_id,))
    db.commit()
    return True


def update_feed_monitor_result(
    db: sqlite3.Connection,
    monitor_id: str,
    *,
    status: str,
    result: dict[str, Any] | None = None,
    error: str | None = None,
) -> None:
    now = datetime.utcnow().isoformat()
    db.execute(
        """
        UPDATE feed_monitors
        SET last_checked_at = ?,
            last_success_at = CASE WHEN ? IN ('completed', 'noop') THEN ? ELSE last_success_at END,
            last_status = ?,
            last_error = ?,
            last_result_json = ?,
            updated_at = ?
        WHERE id = ?
        """,
        (
            now,
            status,
            now,
            status,
            error,
            _json_dumps(result),
            now,
            monitor_id,
        ),
    )


def get_monitor_query(monitor: dict[str, Any]) -> str:
    config = monitor.get("config") if isinstance(monitor.get("config"), dict) else {}
    monitor_type = str(monitor.get("monitor_type") or "").strip().lower()
    raw_query = str((config or {}).get("query") or monitor.get("monitor_key") or "").strip()
    if not raw_query:
        return ""
    if monitor_type == "venue":
        return f"\"{raw_query}\""
    if monitor_type == "preprint":
        return raw_query
    if monitor_type == "branch":
        branch_hint = str((config or {}).get("branch_label") or "").strip()
        return f"{raw_query} {branch_hint}".strip()
    return raw_query
