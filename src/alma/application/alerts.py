"""Alert use-cases extracted from route handlers."""

from __future__ import annotations

import json
import sqlite3
import uuid
from datetime import datetime, timedelta
from typing import Any, Optional

from alma.slack.client import get_slack_notifier

VALID_RULE_TYPES = {
    "author",
    "collection",
    "keyword",
    "topic",
    "similarity",
    "discovery_lens",
    "feed_monitor",
    "branch",
    "library_workflow",
}


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


def list_rules(db: sqlite3.Connection) -> list[dict]:
    """List alert rules in newest-first order."""
    rows = db.execute("SELECT * FROM alert_rules ORDER BY created_at DESC").fetchall()
    return [_to_rule_dict(r) for r in rows]


def get_rule(db: sqlite3.Connection, rule_id: str) -> Optional[dict]:
    """Fetch one rule by ID."""
    row = db.execute("SELECT * FROM alert_rules WHERE id = ?", (rule_id,)).fetchone()
    return _to_rule_dict(row) if row else None


def _validate_rule_config(rule_type: str, rule_config: dict) -> None:
    """Validate that ``rule_config`` carries the keys this rule_type needs.

    Raises ``ValueError`` on missing required fields. The route layer
    converts the ValueError into a 400 response so the user sees a
    precise error rather than a silently-empty rule that matches nothing.
    """
    if rule_type == "feed_monitor":
        # Either monitor_id or monitor_name must be present so
        # _resolve_feed_monitor_id can target a specific monitor.
        monitor_id = str((rule_config or {}).get("monitor_id") or "").strip()
        monitor_name = str((rule_config or {}).get("monitor_name") or "").strip()
        if not monitor_id and not monitor_name:
            raise ValueError(
                "feed_monitor rules require rule_config.monitor_id "
                "(or rule_config.monitor_name)"
            )


def create_rule(
    db: sqlite3.Connection,
    *,
    name: str,
    rule_type: str,
    rule_config: dict,
    channels: list[str],
    enabled: bool,
) -> dict:
    """Create a new rule."""
    if rule_type not in VALID_RULE_TYPES:
        raise ValueError(f"Unsupported rule_type: {rule_type}")
    _validate_rule_config(rule_type, rule_config)
    rid = uuid.uuid4().hex
    now = datetime.utcnow().isoformat()
    db.execute(
        """
        INSERT INTO alert_rules (id, name, rule_type, rule_config, channels, enabled, created_at)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        (rid, name, rule_type, json.dumps(rule_config), json.dumps(channels), int(enabled), now),
    )
    return {
        "id": rid,
        "name": name,
        "rule_type": rule_type,
        "rule_config": rule_config,
        "channels": channels,
        "enabled": enabled,
        "created_at": now,
    }


def update_rule(
    db: sqlite3.Connection,
    rule_id: str,
    *,
    name: str,
    rule_type: str,
    rule_config: dict,
    channels: list[str],
    enabled: bool,
) -> Optional[dict]:
    """Update a rule by ID."""
    if rule_type not in VALID_RULE_TYPES:
        raise ValueError(f"Unsupported rule_type: {rule_type}")
    _validate_rule_config(rule_type, rule_config)
    existing = get_rule(db, rule_id)
    if existing is None:
        return None
    db.execute(
        """
        UPDATE alert_rules
        SET name = ?, rule_type = ?, rule_config = ?, channels = ?, enabled = ?
        WHERE id = ?
        """,
        (name, rule_type, json.dumps(rule_config), json.dumps(channels), int(enabled), rule_id),
    )
    existing.update(
        {
            "name": name,
            "rule_type": rule_type,
            "rule_config": rule_config,
            "channels": channels,
            "enabled": enabled,
        }
    )
    return existing


def delete_rule(db: sqlite3.Connection, rule_id: str) -> bool:
    """Delete a rule."""
    cursor = db.execute("DELETE FROM alert_rules WHERE id = ?", (rule_id,))
    return cursor.rowcount > 0


def toggle_rule(db: sqlite3.Connection, rule_id: str) -> Optional[dict]:
    """Toggle enabled state for a rule."""
    existing = get_rule(db, rule_id)
    if existing is None:
        return None
    new_enabled = not bool(existing["enabled"])
    db.execute("UPDATE alert_rules SET enabled = ? WHERE id = ?", (1 if new_enabled else 0, rule_id))
    existing["enabled"] = new_enabled
    return existing


def list_history(
    db: sqlite3.Connection,
    *,
    rule_id: str | None = None,
    alert_id: str | None = None,
    limit: int = 100,
    offset: int = 0,
) -> list[dict]:
    """List alert history with optional filters."""
    conditions = []
    params: list[Any] = []
    if rule_id:
        conditions.append("rule_id = ?")
        params.append(rule_id)
    if alert_id:
        conditions.append("alert_id = ?")
        params.append(alert_id)
    where_clause = f"WHERE {' AND '.join(conditions)}" if conditions else ""
    rows = db.execute(
        f"SELECT * FROM alert_history {where_clause} ORDER BY sent_at DESC LIMIT ? OFFSET ?",
        [*params, limit, offset],
    ).fetchall()

    out: list[dict] = []
    for row in rows:
        rd = dict(row)
        pubs = _loads(rd.get("publications"))
        out.append(
            {
                "id": rd["id"],
                "rule_id": rd.get("rule_id"),
                "alert_id": rd.get("alert_id"),
                "channel": rd["channel"],
                "paper_id": rd.get("paper_id"),
                "publications": pubs if isinstance(pubs, list) else None,
                "publication_count": rd.get("publication_count", 0),
                "sent_at": rd["sent_at"],
                "status": rd["status"],
                "message_preview": rd.get("message_preview"),
                "error_message": rd.get("error_message"),
            }
        )
    return out


def test_fire_rule(db: sqlite3.Connection, rule_id: str) -> Optional[dict]:
    """Dry-run one rule by querying matching papers only."""
    rule = get_rule(db, rule_id)
    if rule is None:
        return None

    papers = _evaluate_rule(rule, db)
    matches = [str((p or {}).get("title") or "") for p in papers if (p or {}).get("title")]

    return {
        "rule_id": rule_id,
        "rule_type": rule["rule_type"],
        "matches_found": len(matches),
        "matches": matches[:20],
        "note": "This is a dry-run; no notifications were sent.",
    }


def list_alerts(db: sqlite3.Connection) -> list[dict]:
    """List alerts with their assigned rules."""
    rows = db.execute("SELECT * FROM alerts ORDER BY created_at DESC").fetchall()
    return [build_alert_response(db, dict(r)) for r in rows]


def list_alert_templates(db: sqlite3.Connection) -> list[dict]:
    """Suggest alert automations derived from current monitor, branch, and workflow state."""
    templates: list[dict[str, Any]] = []

    try:
        from alma.application import feed_monitors as monitor_app

        author_monitor_templates: list[dict[str, Any]] = []
        for monitor in monitor_app.list_feed_monitors(db):
            last_result = monitor.get("last_result") if isinstance(monitor.get("last_result"), dict) else {}
            items_created = int(last_result.get("items_created") or 0)
            papers_found = int(last_result.get("papers_found") or 0)
            if monitor.get("health") != "ready" or items_created <= 0:
                continue
            if str(monitor.get("monitor_type") or "") == "author" and monitor.get("author_id"):
                author_monitor_templates.append(
                    {
                        "key": f"author:{monitor['author_id']}",
                        "category": "author",
                        "title": f"Author watch for {monitor['label']}",
                        "description": "Create a weekly digest for one monitored author.",
                        "rationale": f"{monitor['label']} most recently produced {items_created} new items.",
                        "metrics": {
                            "items_created": items_created,
                            "papers_found": papers_found,
                            "health": monitor.get("health"),
                        },
                        "rule": {
                            "name": f"Author watch: {monitor['label']}",
                            "rule_type": "author",
                            "rule_config": {
                                "author_id": monitor["author_id"],
                                "openalex_id": monitor.get("openalex_id"),
                            },
                            "channels": ["slack"],
                            "enabled": True,
                        },
                        "alert": {
                            "name": f"Weekly author watch: {monitor['label']}",
                            "channels": ["slack"],
                            "schedule": "weekly",
                            "schedule_config": {"day": "monday", "time": "09:00"},
                            "format": "text",
                            "enabled": True,
                        },
                    }
                )
                continue
            templates.append(
                {
                    "key": f"feed-monitor:{monitor['id']}",
                    "category": "feed_monitor",
                    "title": f"Monitor digest for {monitor['label']}",
                    "description": "Create a daily digest for one productive Feed monitor.",
                    "rationale": f"{monitor['label']} most recently produced {items_created} new items from {papers_found} retrieved papers.",
                    "metrics": {
                        "items_created": items_created,
                        "papers_found": papers_found,
                        "health": monitor.get("health"),
                    },
                    "rule": {
                        "name": f"Feed monitor: {monitor['label']}",
                        "rule_type": "feed_monitor",
                        "rule_config": {
                            "monitor_id": monitor["id"],
                            "include_statuses": ["new"],
                            "lookback_days": 14,
                        },
                        "channels": ["slack"],
                        "enabled": True,
                    },
                    "alert": {
                        "name": f"Daily monitor digest: {monitor['label']}",
                        "channels": ["slack"],
                        "schedule": "daily",
                        "schedule_config": {"time": "09:00"},
                        "format": "text",
                        "enabled": True,
                    },
                }
            )
        templates.extend(author_monitor_templates[:3])
    except Exception:
        pass

    try:
        collection_rows = db.execute(
            """
            SELECT
                c.id,
                c.name,
                COUNT(ci.paper_id) AS item_count,
                ROUND(COALESCE(AVG(CASE WHEN p.rating > 0 THEN p.rating END), 0), 2) AS avg_rating
            FROM collections c
            LEFT JOIN collection_items ci ON ci.collection_id = c.id
            LEFT JOIN papers p ON p.id = ci.paper_id
            GROUP BY c.id, c.name
            HAVING COUNT(ci.paper_id) > 0
            ORDER BY item_count DESC, avg_rating DESC, lower(c.name)
            LIMIT 3
            """
        ).fetchall()
        for row in collection_rows:
            item_count = int(row["item_count"] or 0)
            templates.append(
                {
                    "key": f"collection:{row['id']}",
                    "category": "collection",
                    "title": f"Collection watch for {row['name']}",
                    "description": "Create a digest grounded in one curated collection.",
                    "rationale": f"{row['name']} currently contains {item_count} papers and can anchor a recurring collection-specific watch.",
                    "metrics": {
                        "item_count": item_count,
                        "avg_rating": float(row["avg_rating"] or 0.0),
                    },
                    "rule": {
                        "name": f"Collection watch: {row['name']}",
                        "rule_type": "collection",
                        "rule_config": {
                            "collection_id": row["id"],
                        },
                        "channels": ["slack"],
                        "enabled": True,
                    },
                    "alert": {
                        "name": f"Weekly collection watch: {row['name']}",
                        "channels": ["slack"],
                        "schedule": "weekly",
                        "schedule_config": {"day": "friday", "time": "10:00"},
                        "format": "text",
                        "enabled": True,
                    },
                }
            )
    except Exception:
        pass

    try:
        branch_rows = db.execute(
            """
            SELECT
                COALESCE(NULLIF(branch_id, ''), NULL) AS branch_id,
                COALESCE(NULLIF(branch_label, ''), NULL) AS branch_label,
                COUNT(*) AS count,
                COALESCE(SUM(CASE WHEN user_action = 'like' THEN 1 ELSE 0 END), 0) AS liked,
                COALESCE(SUM(CASE WHEN user_action = 'dismiss' THEN 1 ELSE 0 END), 0) AS dismissed
            FROM recommendations
            WHERE COALESCE(branch_id, '') <> '' OR COALESCE(branch_label, '') <> ''
            GROUP BY COALESCE(NULLIF(branch_id, ''), NULL), COALESCE(NULLIF(branch_label, ''), NULL)
            ORDER BY liked DESC, count DESC
            LIMIT 4
            """
        ).fetchall()
        for row in branch_rows:
            count = int(row["count"] or 0)
            liked = int(row["liked"] or 0)
            dismissed = int(row["dismissed"] or 0)
            engagement = round((liked + dismissed) / count, 3) if count else 0.0
            if count < 3 or engagement < 0.2:
                continue
            branch_id = str(row["branch_id"] or "").strip()
            branch_label = str(row["branch_label"] or branch_id or "Branch").strip() or "Branch"
            templates.append(
                {
                    "key": f"branch:{branch_id or branch_label}",
                    "category": "branch",
                    "title": f"Branch watch for {branch_label}",
                    "description": "Create a weekly digest for a branch that is already generating engagement.",
                    "rationale": f"{branch_label} has {count} recommendations with {(engagement * 100):.0f}% engagement.",
                    "metrics": {
                        "count": count,
                        "liked": liked,
                        "dismissed": dismissed,
                        "engagement_rate": engagement,
                    },
                    "rule": {
                        "name": f"Branch watch: {branch_label}",
                        "rule_type": "branch",
                        "rule_config": {
                            "branch_id": branch_id,
                            "branch_label": branch_label,
                            "min_score": 0.55,
                        },
                        "channels": ["slack"],
                        "enabled": True,
                    },
                    "alert": {
                        "name": f"Weekly branch watch: {branch_label}",
                        "channels": ["slack"],
                        "schedule": "weekly",
                        "schedule_config": {"day": "monday", "time": "09:00"},
                        "format": "text",
                        "enabled": True,
                    },
                }
            )
    except Exception:
        pass

    workflow = _library_workflow_counts(db)
    if int(workflow["untriaged_count"]) > 0:
        templates.append(
            {
                "key": "library-workflow:untriaged",
                "category": "library_workflow",
                "title": "Untriaged acquisition cleanup",
                "description": "Create a daily digest for new library papers that still have no reading state.",
                "rationale": f"{workflow['untriaged_count']} library papers are still untriaged.",
                "metrics": {
                    "untriaged_count": workflow["untriaged_count"],
                    "queued_count": workflow["queued_count"],
                },
                "rule": {
                    "name": "Library workflow: untriaged acquisitions",
                    "rule_type": "library_workflow",
                    "rule_config": {"workflow": "untriaged", "limit": 20},
                    "channels": ["slack"],
                    "enabled": True,
                },
                "alert": {
                    "name": "Daily library cleanup",
                    "channels": ["slack"],
                    "schedule": "daily",
                    "schedule_config": {"time": "17:30"},
                    "format": "text",
                    "enabled": True,
                },
            }
        )
    if int(workflow["queued_count"]) > 0:
        templates.append(
            {
                "key": "library-workflow:queued",
                "category": "library_workflow",
                "title": "Queued reading reminder",
                "description": "Create a weekly digest for the current reading queue.",
                "rationale": f"{workflow['queued_count']} papers are queued for reading.",
                "metrics": {
                    "queued_count": workflow["queued_count"],
                    "reading_count": workflow["reading_count"],
                },
                "rule": {
                    "name": "Library workflow: queued reading",
                    "rule_type": "library_workflow",
                    "rule_config": {"workflow": "queued", "limit": 15},
                    "channels": ["slack"],
                    "enabled": True,
                },
                "alert": {
                    "name": "Weekly reading queue reminder",
                    "channels": ["slack"],
                    "schedule": "weekly",
                    "schedule_config": {"day": "friday", "time": "16:00"},
                    "format": "text",
                    "enabled": True,
                },
            }
        )

    return templates[:10]


def create_alert(
    db: sqlite3.Connection,
    *,
    name: str,
    channels: list[str],
    schedule: str,
    schedule_config: Optional[dict],
    format_value: str,
    enabled: bool,
    rule_ids: list[str],
) -> dict:
    """Create an alert plus optional rule assignments."""
    aid = uuid.uuid4().hex
    now = datetime.utcnow().isoformat()
    db.execute(
        """
        INSERT INTO alerts (id, name, channels, schedule, schedule_config, format, enabled, created_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            aid,
            name,
            json.dumps(channels),
            schedule,
            json.dumps(schedule_config) if schedule_config else None,
            format_value,
            int(enabled),
            now,
        ),
    )

    if rule_ids:
        for rule_id in sorted(set(rule_ids)):
            exists = db.execute("SELECT 1 FROM alert_rules WHERE id = ?", (rule_id,)).fetchone()
            if exists:
                db.execute(
                    "INSERT OR IGNORE INTO alert_rule_assignments (alert_id, rule_id) VALUES (?, ?)",
                    (aid, rule_id),
                )

    row = db.execute("SELECT * FROM alerts WHERE id = ?", (aid,)).fetchone()
    return build_alert_response(db, dict(row))


def get_alert(db: sqlite3.Connection, alert_id: str) -> Optional[dict]:
    """Get one alert by ID with rule payload."""
    row = db.execute("SELECT * FROM alerts WHERE id = ?", (alert_id,)).fetchone()
    return build_alert_response(db, dict(row)) if row else None


def update_alert(
    db: sqlite3.Connection,
    alert_id: str,
    *,
    name: Optional[str] = None,
    channels: Optional[list[str]] = None,
    schedule: Optional[str] = None,
    schedule_config: Optional[dict] = None,
    format_value: Optional[str] = None,
    enabled: Optional[bool] = None,
) -> Optional[dict]:
    """Partially update one alert."""
    row = db.execute("SELECT * FROM alerts WHERE id = ?", (alert_id,)).fetchone()
    if not row:
        return None
    current = dict(row)
    db.execute(
        """
        UPDATE alerts
        SET name = ?, channels = ?, schedule = ?, schedule_config = ?, format = ?, enabled = ?
        WHERE id = ?
        """,
        (
            name if name is not None else current["name"],
            json.dumps(channels) if channels is not None else current["channels"],
            schedule if schedule is not None else current["schedule"],
            json.dumps(schedule_config) if schedule_config is not None else current.get("schedule_config"),
            format_value if format_value is not None else current.get("format", "grouped"),
            int(enabled) if enabled is not None else current["enabled"],
            alert_id,
        ),
    )
    updated = db.execute("SELECT * FROM alerts WHERE id = ?", (alert_id,)).fetchone()
    return build_alert_response(db, dict(updated))


def delete_alert(db: sqlite3.Connection, alert_id: str) -> bool:
    """Delete alert and dependent assignments/history links."""
    db.execute("DELETE FROM alert_rule_assignments WHERE alert_id = ?", (alert_id,))
    db.execute("DELETE FROM alerted_publications WHERE alert_id = ?", (alert_id,))
    cursor = db.execute("DELETE FROM alerts WHERE id = ?", (alert_id,))
    return cursor.rowcount > 0


def assign_rules(db: sqlite3.Connection, alert_id: str, rule_ids: list[str]) -> Optional[dict]:
    """Add a set of rules to an alert."""
    row = db.execute("SELECT id FROM alerts WHERE id = ?", (alert_id,)).fetchone()
    if not row:
        return None
    assigned: list[str] = []
    for rule_id in rule_ids:
        exists = db.execute("SELECT 1 FROM alert_rules WHERE id = ?", (rule_id,)).fetchone()
        if not exists:
            continue
        db.execute(
            "INSERT OR IGNORE INTO alert_rule_assignments (alert_id, rule_id) VALUES (?, ?)",
            (alert_id, rule_id),
        )
        assigned.append(rule_id)
    return {"alert_id": alert_id, "assigned_rule_ids": assigned}


def unassign_rule(db: sqlite3.Connection, alert_id: str, rule_id: str) -> bool:
    """Remove one assigned rule from alert."""
    cursor = db.execute(
        "DELETE FROM alert_rule_assignments WHERE alert_id = ? AND rule_id = ?",
        (alert_id, rule_id),
    )
    return cursor.rowcount > 0


async def evaluate_digest(
    db: sqlite3.Connection,
    digest_id: str,
    *,
    trigger_source: str = "user",
) -> Optional[dict]:
    """Evaluate one digest and send notifications."""
    alert_row = db.execute("SELECT * FROM alerts WHERE id = ?", (digest_id,)).fetchone()
    if not alert_row:
        return None

    alert = dict(alert_row)
    channels = _loads(alert["channels"]) or []
    rule_rows = _get_assigned_enabled_rules(digest_id, db)

    # Cold-start watermark (D-AL-9): the alert "starts caring" from the
    # moment it was created. Combined with the per-rule 30-day publication-
    # date window (D-AL-3), this ensures a brand-new alert never floods
    # the user with backfilled papers that were already in the feed before
    # the alert opted in.
    alert_created_at = str(alert.get("created_at") or "").strip() or None

    all_papers: list = []
    for rule in rule_rows:
        all_papers.extend(_evaluate_rule(rule, db, alert_created_at=alert_created_at))

    unique_papers = _deduplicate_papers(all_papers)
    already_alerted = _get_already_alerted_keys(digest_id, db)
    new_papers = [(key, paper) for key, paper in unique_papers if key not in already_alerted]

    papers_sent = 0
    papers_failed = 0
    now = datetime.utcnow().isoformat()
    channel_results: dict[str, dict] = {}

    for channel_name in channels:
        if channel_name == "slack":
            notifier = get_slack_notifier()
            if not notifier.is_configured:
                channel_results[channel_name] = {"status": "skipped", "error": "Slack token not configured"}
                continue
            if not new_papers:
                channel_results[channel_name] = {"status": "empty", "error": None}
                continue
            payload = [paper for _, paper in new_papers]
            try:
                ok = await notifier.send_paper_alert(channel=None, papers=payload, alert_name=alert["name"])
                if ok:
                    papers_sent = len(new_papers)
                    channel_results[channel_name] = {"status": "sent", "error": None}
                else:
                    papers_failed = len(new_papers)
                    channel_results[channel_name] = {"status": "failed", "error": "Slack API returned failure"}
            except Exception as exc:
                papers_failed = len(new_papers)
                channel_results[channel_name] = {"status": "failed", "error": str(exc)}
        else:
            channel_results[channel_name] = {
                "status": "skipped",
                "error": f"Unsupported channel type: {channel_name}",
            }

    any_sent = any(result.get("status") == "sent" for result in channel_results.values())
    if any_sent:
        for key, _paper in new_papers:
            db.execute(
                """
                INSERT OR IGNORE INTO alerted_publications (id, alert_id, paper_id, alerted_at)
                VALUES (?, ?, ?, ?)
                """,
                (uuid.uuid4().hex, digest_id, key, now),
            )

    publication_ids = [key for key, _ in new_papers]
    for channel_name in channels:
        ch_result = channel_results.get(channel_name, {"status": "unknown", "error": None})
        status_value = ch_result["status"]
        error_msg = ch_result.get("error")
        db.execute(
            """
            INSERT INTO alert_history (
                id, alert_id, channel, sent_at, status, publications, publication_count, message_preview, error_message
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                uuid.uuid4().hex,
                digest_id,
                channel_name,
                now,
                status_value,
                json.dumps(publication_ids),
                len(new_papers),
                (
                    f"Sent {papers_sent} papers via {channel_name}"
                    if status_value == "sent"
                    else f"{status_value}: {error_msg or 'no new papers'}"
                ),
                error_msg,
            ),
        )

    db.execute("UPDATE alerts SET last_evaluated_at = ? WHERE id = ?", (now, digest_id))
    return {
        "alert_id": digest_id,
        "alert_name": alert["name"],
        "digest_id": digest_id,
        "digest_name": alert["name"],
        "papers_found": len(unique_papers),
        "papers_new": len(new_papers),
        "papers_sent": papers_sent,
        "papers_failed": papers_failed,
        "matched_rules": len(rule_rows),
        "channels": channels,
        "channel_results": channel_results,
        "trigger_source": trigger_source,
        "dry_run": False,
    }


def dry_run_digest(db: sqlite3.Connection, digest_id: str) -> Optional[dict]:
    """Evaluate digest without sending or persistence side-effects."""
    alert_row = db.execute("SELECT * FROM alerts WHERE id = ?", (digest_id,)).fetchone()
    if not alert_row:
        return None

    alert = dict(alert_row)
    channels = _loads(alert["channels"]) or []
    rule_rows = _get_assigned_enabled_rules(digest_id, db)
    alert_created_at = str(alert.get("created_at") or "").strip() or None
    all_papers: list = []
    for rule in rule_rows:
        all_papers.extend(_evaluate_rule(rule, db, alert_created_at=alert_created_at))
    unique_papers = _deduplicate_papers(all_papers)
    already_alerted = _get_already_alerted_keys(digest_id, db)
    new_papers = [(key, paper) for key, paper in unique_papers if key not in already_alerted]
    paper_details = [
        {
            "paper_id": key,
            "title": paper.get("title", ""),
            "authors": paper.get("authors", ""),
            "year": paper.get("year"),
            "publication_date": paper.get("publication_date"),
            "url": paper.get("url", ""),
        }
        for key, paper in new_papers
    ]

    return {
        "alert_id": digest_id,
        "alert_name": alert["name"],
        "digest_id": digest_id,
        "digest_name": alert["name"],
        "papers_found": len(unique_papers),
        "papers_new": len(new_papers),
        "papers_sent": 0,
        "matched_rules": len(rule_rows),
        "channels": channels,
        "dry_run": True,
        "papers": paper_details,
    }
def build_alert_response(db: sqlite3.Connection, alert_row: dict) -> dict:
    """Build alert payload with assigned rules."""
    rows = db.execute(
        """
        SELECT ar.*
        FROM alert_rules ar
        JOIN alert_rule_assignments ara ON ara.rule_id = ar.id
        WHERE ara.alert_id = ?
        """,
        (alert_row["id"],),
    ).fetchall()
    return {
        "id": alert_row["id"],
        "name": alert_row["name"],
        "channels": _loads(alert_row["channels"]) or [],
        "schedule": alert_row["schedule"],
        "schedule_config": _loads(alert_row.get("schedule_config")),
        "format": alert_row.get("format", "grouped"),
        "enabled": bool(alert_row["enabled"]),
        "created_at": alert_row["created_at"],
        "last_evaluated_at": alert_row.get("last_evaluated_at"),
        "rules": [_to_rule_dict(r) for r in rows],
    }


def _to_rule_dict(row: sqlite3.Row | dict) -> dict:
    data = dict(row)
    return {
        "id": data["id"],
        "name": data["name"],
        "rule_type": data["rule_type"],
        "rule_config": _loads(data["rule_config"]) or {},
        "channels": _loads(data["channels"]) or [],
        "enabled": bool(data["enabled"]),
        "created_at": data["created_at"],
    }


def _get_assigned_enabled_rules(digest_id: str, db: sqlite3.Connection) -> list[dict]:
    rows = db.execute(
        """
        SELECT ar.*
        FROM alert_rules ar
        JOIN alert_rule_assignments ara ON ara.rule_id = ar.id
        WHERE ara.alert_id = ? AND ar.enabled = 1
        """,
        (digest_id,),
    ).fetchall()
    return [dict(r) for r in rows]


def _deduplicate_papers(all_papers: list[dict]) -> list[tuple[str, dict]]:
    """Dedupe papers by id, preserving every distinct ``alert_source``.

    When the same paper matches more than one assigned rule (e.g., it
    appears in two monitors covered by one alert), the user still wants
    to know all the sources that triggered the match. The first
    encountered paper dict wins for general fields, but its
    ``alert_source`` is rewritten to the comma-joined union of every
    source seen for that id.
    """
    seen_ids: dict[str, dict] = {}
    seen_sources: dict[str, list[str]] = {}
    order: list[str] = []
    for paper in all_papers:
        paper_id = str((paper or {}).get("id") or "").strip()
        if not paper_id:
            continue
        source = str((paper or {}).get("alert_source") or "").strip()
        if paper_id not in seen_ids:
            seen_ids[paper_id] = paper
            seen_sources[paper_id] = [source] if source else []
            order.append(paper_id)
        elif source and source not in seen_sources[paper_id]:
            seen_sources[paper_id].append(source)

    unique_papers: list[tuple[str, dict]] = []
    for paper_id in order:
        paper = seen_ids[paper_id]
        sources = seen_sources.get(paper_id) or []
        if sources:
            paper["alert_source"] = ", ".join(sources)
        unique_papers.append((paper_id, paper))
    return unique_papers


def _get_already_alerted_keys(digest_id: str, db: sqlite3.Connection) -> set[str]:
    rows = db.execute(
        "SELECT paper_id FROM alerted_publications WHERE alert_id = ?",
        (digest_id,),
    ).fetchall()
    return {str(r["paper_id"]) for r in rows if r["paper_id"]}


def _resolve_feed_monitor_id(db: sqlite3.Connection, config: dict[str, Any]) -> str:
    monitor_id = str(config.get("monitor_id") or "").strip()
    if monitor_id:
        return monitor_id
    monitor_ref = str(config.get("monitor_key") or config.get("label") or "").strip()
    if not monitor_ref:
        return ""
    row = db.execute(
        """
        SELECT id
        FROM feed_monitors
        WHERE id = ?
           OR lower(monitor_key) = lower(?)
           OR lower(label) = lower(?)
        LIMIT 1
        """,
        (monitor_ref, monitor_ref, monitor_ref),
    ).fetchone()
    return str((row["id"] if row else "") or "").strip()


def _library_workflow_counts(db: sqlite3.Connection) -> dict[str, int]:
    # Guard: papers table may not exist yet on a fresh database
    exists = db.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name='papers'"
    ).fetchone()
    if not exists:
        return {"queued_count": 0, "reading_count": 0, "done_count": 0, "excluded_count": 0, "untriaged_count": 0}
    row = db.execute(
        """
        SELECT
            COALESCE(SUM(CASE WHEN reading_status = 'queued' THEN 1 ELSE 0 END), 0) AS queued_count,
            COALESCE(SUM(CASE WHEN reading_status = 'reading' THEN 1 ELSE 0 END), 0) AS reading_count,
            COALESCE(SUM(CASE WHEN reading_status = 'done' THEN 1 ELSE 0 END), 0) AS done_count,
            COALESCE(SUM(CASE WHEN reading_status = 'excluded' THEN 1 ELSE 0 END), 0) AS excluded_count,
            COALESCE(SUM(
                CASE
                    WHEN status = 'library'
                     AND (reading_status IS NULL OR TRIM(reading_status) = '')
                    THEN 1 ELSE 0
                END
            ), 0) AS untriaged_count
        FROM papers
        """
    ).fetchone()
    return {
        "queued_count": int((row["queued_count"] if row else 0) or 0),
        "reading_count": int((row["reading_count"] if row else 0) or 0),
        "done_count": int((row["done_count"] if row else 0) or 0),
        "excluded_count": int((row["excluded_count"] if row else 0) or 0),
        "untriaged_count": int((row["untriaged_count"] if row else 0) or 0),
    }


def _evaluate_rule(
    rule_row: dict,
    db: sqlite3.Connection,
    *,
    alert_created_at: Optional[str] = None,
) -> list[dict]:
    """Run a single rule's match query and return the matching paper rows.

    Args:
        rule_row: A row from ``alert_rules``.
        db: Open SQLite connection.
        alert_created_at: When the rule is being evaluated as part of an
            alert (digest), this is the alert's ``created_at`` timestamp.
            For the ``feed_monitor`` rule type it gates Layer 2 of the
            cold-start filter (D-AL-9): only feed_items fetched after
            the alert opted in are eligible. Pass ``None`` from
            ``test_fire_rule`` so the test reports the broader pre-
            watermark match set.
    """
    rule_type = str(rule_row.get("rule_type") or "").strip()
    config = _loads(rule_row.get("rule_config")) if isinstance(rule_row.get("rule_config"), str) else rule_row.get("rule_config")
    if not isinstance(config, dict):
        config = {}

    if rule_type == "author":
        author_ref = str(config.get("author_id") or config.get("openalex_id") or "").strip()
        if not author_ref:
            return []
        author_name = ""
        openalex_id = author_ref
        if _table_exists(db, "authors"):
            author_columns = _table_columns(db, "authors")
            select_fields = ["id", "name"]
            if "openalex_id" in author_columns:
                select_fields.append("openalex_id")
            where_clauses = ["id = ?"]
            params: list[object] = [author_ref]
            if "openalex_id" in author_columns:
                where_clauses.append("openalex_id = ?")
                params.append(author_ref)
            resolved = db.execute(
                f"""
                SELECT {", ".join(select_fields)}
                FROM authors
                WHERE {" OR ".join(where_clauses)}
                LIMIT 1
                """,
                params,
            ).fetchone()
            if resolved:
                author_name = str(resolved["name"] or "").strip()
                if "openalex_id" in author_columns:
                    openalex_id = str(resolved["openalex_id"] or openalex_id).strip()

        if _table_exists(db, "publication_authors") and openalex_id:
            pa_columns = _table_columns(db, "publication_authors")
            where_clauses: list[str] = []
            params = []
            if "openalex_id" in pa_columns:
                where_clauses.append("pa.openalex_id = ?")
                params.append(openalex_id)
            if author_name and "display_name" in pa_columns:
                where_clauses.append("lower(trim(pa.display_name)) = lower(trim(?))")
                params.append(author_name)
            if where_clauses:
                rows = db.execute(
                    f"""
                    SELECT DISTINCT p.*
                    FROM papers p
                    JOIN publication_authors pa ON pa.paper_id = p.id
                    WHERE {" OR ".join(where_clauses)}
                    ORDER BY COALESCE(p.year, 0) DESC, COALESCE(p.cited_by_count, 0) DESC
                    LIMIT 500
                    """,
                    params,
                ).fetchall()
                matches = [dict(r) for r in rows]
                if matches:
                    return matches

        if not _table_exists(db, "papers"):
            return []
        paper_columns = _table_columns(db, "papers")
        where_clauses = []
        params = []
        if "author_id" in paper_columns:
            where_clauses.append("p.author_id = ?")
            params.append(author_ref)
        if "added_from" in paper_columns:
            where_clauses.append("p.added_from = ?")
            params.append(author_ref)
        if author_name and "authors" in paper_columns:
            where_clauses.append("lower(COALESCE(p.authors, '')) LIKE lower(?)")
            params.append(f"%{author_name}%")
        if author_name and "author" in paper_columns:
            where_clauses.append("lower(COALESCE(p.author, '')) LIKE lower(?)")
            params.append(f"%{author_name}%")
        if not where_clauses:
            return []
        rows = db.execute(
            f"""
            SELECT DISTINCT p.*
            FROM papers p
            WHERE {" OR ".join(where_clauses)}
            ORDER BY COALESCE(p.year, 0) DESC, COALESCE(p.cited_by_count, 0) DESC
            LIMIT 500
            """,
            params,
        ).fetchall()
        return [dict(r) for r in rows]

    if rule_type == "collection":
        collection_ref = str(config.get("collection_id") or config.get("collection_name") or "").strip()
        if not collection_ref:
            return []
        row = db.execute(
            """
            SELECT id
            FROM collections
            WHERE id = ? OR lower(name) = lower(?)
            LIMIT 1
            """,
            (collection_ref, collection_ref),
        ).fetchone()
        collection_id = str((row["id"] if row else "") or "").strip()
        if not collection_id:
            return []
        rows = db.execute(
            """
            SELECT p.*
            FROM collection_items ci
            JOIN papers p ON p.id = ci.paper_id
            WHERE ci.collection_id = ?
            ORDER BY COALESCE(p.rating, 0) DESC, COALESCE(p.added_at, '') DESC
            LIMIT 500
            """,
            (collection_id,),
        ).fetchall()
        return [dict(r) for r in rows]

    if rule_type == "keyword":
        keywords = [str(k).strip() for k in (config.get("keywords") or []) if str(k).strip()]
        if not keywords:
            single = str(config.get("keyword") or "").strip()
            if single:
                keywords = [single]
        if not keywords:
            return []
        where = " OR ".join(["LOWER(p.title) LIKE ? OR LOWER(p.abstract) LIKE ?" for _ in keywords])
        params: list[object] = []
        for kw in keywords:
            pattern = f"%{kw.lower()}%"
            params.extend([pattern, pattern])
        rows = db.execute(
            f"""
            SELECT DISTINCT p.*
            FROM papers p
            WHERE {where}
            ORDER BY COALESCE(p.year, 0) DESC, COALESCE(p.cited_by_count, 0) DESC
            LIMIT 500
            """,
            params,
        ).fetchall()
        return [dict(r) for r in rows]

    if rule_type == "topic":
        topic = str(config.get("topic") or config.get("term") or "").strip()
        if not topic:
            return []
        pattern = f"%{topic.lower()}%"
        rows = db.execute(
            """
            SELECT DISTINCT p.*
            FROM papers p
            LEFT JOIN publication_topics pt ON pt.paper_id = p.id
            LEFT JOIN topic_aliases ta ON LOWER(ta.raw_term) = LOWER(pt.term)
            LEFT JOIN topics t ON t.topic_id = ta.topic_id
            WHERE LOWER(COALESCE(pt.term, '')) LIKE ?
               OR LOWER(COALESCE(t.canonical_name, '')) LIKE ?
               OR LOWER(COALESCE(p.title, '')) LIKE ?
               OR LOWER(COALESCE(p.abstract, '')) LIKE ?
            ORDER BY COALESCE(p.year, 0) DESC, COALESCE(p.cited_by_count, 0) DESC
            LIMIT 500
            """,
            (pattern, pattern, pattern, pattern),
        ).fetchall()
        return [dict(r) for r in rows]

    if rule_type == "similarity":
        min_score = float(config.get("min_score", 60) or 0)
        if min_score <= 1.0:
            min_score *= 100.0
        lens_id = str(config.get("lens_id") or "").strip()
        params: list[object] = [min_score]
        lens_clause = ""
        if lens_id:
            lens_clause = "AND r.lens_id = ?"
            params.append(lens_id)
        rows = db.execute(
            f"""
            SELECT p.*
            FROM recommendations r
            JOIN papers p ON p.id = r.paper_id
            WHERE r.score >= ?
              AND COALESCE(r.user_action, '') != 'dismiss'
              {lens_clause}
            ORDER BY r.score DESC, r.created_at DESC
            LIMIT 500
            """,
            params,
        ).fetchall()
        return [dict(r) for r in rows]

    if rule_type == "discovery_lens":
        lens_id = str(config.get("lens_id") or "").strip()
        if not lens_id:
            return []
        min_score = float(config.get("min_score", 0) or 0)
        if min_score <= 1.0:
            min_score *= 100.0
        rows = db.execute(
            """
            SELECT p.*
            FROM recommendations r
            JOIN papers p ON p.id = r.paper_id
            WHERE r.lens_id = ?
              AND r.score >= ?
              AND COALESCE(r.user_action, '') != 'dismiss'
            ORDER BY r.score DESC, r.created_at DESC
            LIMIT 500
            """,
            (lens_id, min_score),
        ).fetchall()
        return [dict(r) for r in rows]

    if rule_type == "feed_monitor":
        monitor_id = _resolve_feed_monitor_id(db, config)
        if not monitor_id:
            return []
        # Resolve a human-readable label up front so each returned paper
        # carries its own provenance (which monitor triggered the match).
        # The Slack render uses this to add a "Source: ..." line per paper,
        # which is essential when one alert spans multiple monitors.
        monitor_label_row = db.execute(
            "SELECT label, monitor_type, monitor_key FROM feed_monitors WHERE id = ?",
            (monitor_id,),
        ).fetchone()
        if monitor_label_row is not None:
            label = str(monitor_label_row["label"] or "").strip()
            monitor_type_str = str(monitor_label_row["monitor_type"] or "").strip()
            display_label = label or monitor_label_row["monitor_key"] or monitor_id
            if monitor_type_str:
                alert_source = f"Monitor ({monitor_type_str}): {display_label}"
            else:
                alert_source = f"Monitor: {display_label}"
        else:
            alert_source = f"Monitor: {monitor_id}"

        statuses = [
            str(status_value).strip().lower()
            for status_value in (config.get("include_statuses") or ["new"])
            if str(status_value).strip()
        ] or ["new"]
        # Existing per-rule "lookback" gates which feed_items rows are
        # eligible by `fetched_at`. Kept for back-compat (test-fire et al).
        lookback_days = max(1, int(config.get("lookback_days", 14) or 14))
        since = (datetime.utcnow() - timedelta(days=lookback_days)).isoformat()
        status_placeholders = ", ".join("?" for _ in statuses)

        # Layer 1 (D-AL-3): publication-date window. Default 30 days.
        # Drops historical backfill that's recent in fetch terms but old
        # in world terms. NULL pub_date papers are excluded — per
        # `lessons.md:260`, we don't fabricate timestamps.
        max_age_days = max(1, int(config.get("max_age_days", 30) or 30))

        # Layer 2 (D-AL-9): cold-start watermark. Only fires for digest
        # evaluations that pass an `alert_created_at`; test-fire skips it
        # so the user sees the broader pre-watermark match set.
        watermark_clause = ""
        params: list[object] = [monitor_id, since, max_age_days, *statuses]
        if alert_created_at:
            watermark_clause = " AND fi.fetched_at >= ?"
            params.append(alert_created_at)

        rows = db.execute(
            f"""
            SELECT DISTINCT p.*
            FROM feed_items fi
            JOIN papers p ON p.id = fi.paper_id
            WHERE fi.monitor_id = ?
              AND fi.fetched_at >= ?
              AND p.publication_date IS NOT NULL
              AND TRIM(p.publication_date) != ''
              AND p.publication_date >= date('now', '-' || ? || ' days')
              AND lower(COALESCE(fi.status, 'new')) IN ({status_placeholders})
              {watermark_clause}
            ORDER BY p.publication_date DESC, fi.fetched_at DESC
            LIMIT 500
            """,
            params,
        ).fetchall()
        results: list[dict] = []
        for r in rows:
            paper = dict(r)
            paper["alert_source"] = alert_source
            results.append(paper)
        return results

    if rule_type == "branch":
        branch_id = str(config.get("branch_id") or "").strip()
        branch_label = str(config.get("branch_label") or "").strip()
        lens_id = str(config.get("lens_id") or "").strip()
        if not branch_id and not branch_label:
            return []
        min_score = float(config.get("min_score", 0) or 0)
        if min_score <= 1.0:
            min_score *= 100.0
        clauses = ["r.score >= ?", "COALESCE(r.user_action, '') != 'dismiss'"]
        params: list[Any] = [min_score]
        if branch_id:
            clauses.append("r.branch_id = ?")
            params.append(branch_id)
        elif branch_label:
            clauses.append("r.branch_label = ?")
            params.append(branch_label)
        if lens_id:
            clauses.append("r.lens_id = ?")
            params.append(lens_id)
        rows = db.execute(
            f"""
            SELECT p.*
            FROM recommendations r
            JOIN papers p ON p.id = r.paper_id
            WHERE {' AND '.join(clauses)}
            ORDER BY r.score DESC, r.created_at DESC
            LIMIT 500
            """,
            params,
        ).fetchall()
        return [dict(r) for r in rows]

    if rule_type == "library_workflow":
        workflow = str(config.get("workflow") or config.get("state") or "").strip().lower()
        limit = max(1, min(500, int(config.get("limit", 25) or 25)))
        if workflow == "untriaged":
            where = "p.status = 'library' AND (p.reading_status IS NULL OR TRIM(p.reading_status) = '')"
        elif workflow in {"queued", "reading", "done", "excluded"}:
            where = "p.reading_status = ?"
        else:
            return []
        params = [workflow] if workflow in {"queued", "reading", "done", "excluded"} else []
        rows = db.execute(
            f"""
            SELECT p.*
            FROM papers p
            WHERE {where}
            ORDER BY COALESCE(p.added_at, '') DESC, COALESCE(p.rating, 0) DESC
            LIMIT ?
            """,
            [*params, limit],
        ).fetchall()
        return [dict(r) for r in rows]

    return []


def _loads(value: Optional[str]) -> Optional[object]:
    if not value:
        return None
    try:
        return json.loads(value)
    except Exception:
        return None
