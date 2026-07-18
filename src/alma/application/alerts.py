"""Alert use-cases extracted from route handlers."""

from __future__ import annotations

import json
import sqlite3
import uuid
from datetime import datetime, timedelta
from typing import Any

from alma.core.sql_helpers import standalone_paper_sql
from alma.mailer.client import get_email_notifier
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


def list_rules(db: sqlite3.Connection) -> list[dict]:
    """List alert rules in newest-first order."""
    rows = db.execute("SELECT * FROM alert_rules ORDER BY created_at DESC").fetchall()
    return [_to_rule_dict(r) for r in rows]


def get_rule(db: sqlite3.Connection, rule_id: str) -> dict | None:
    """Fetch one rule by ID."""
    row = db.execute("SELECT * FROM alert_rules WHERE id = ?", (rule_id,)).fetchone()
    return _to_rule_dict(row) if row else None


def _validate_rule_config(rule_type: str, rule_config: dict) -> None:
    """Validate that ``rule_config`` carries the keys this rule_type needs.

    Raises ``ValueError`` on missing required fields. The route layer
    converts the ValueError into a 422 response so the user sees a
    precise error rather than a silently-empty rule that matches nothing.

    The accepted keys/aliases mirror exactly what ``_evaluate_rule`` reads
    for each type — a config that passes here is guaranteed to reach a
    real match query instead of degrading to ``return []``.
    """
    cfg = rule_config or {}

    def _has(*keys: str) -> bool:
        return any(str(cfg.get(k) or "").strip() for k in keys)

    def _require(condition: bool, message: str) -> None:
        if not condition:
            raise ValueError(message)

    if rule_type == "author":
        _require(_has("author_id", "openalex_id"), "author rules require rule_config.author_id (or openalex_id)")
    elif rule_type == "collection":
        _require(
            _has("collection_id", "collection_name"),
            "collection rules require rule_config.collection_id (or collection_name)",
        )
    elif rule_type == "keyword":
        keywords = [str(k).strip() for k in (cfg.get("keywords") or []) if str(k).strip()]
        _require(
            bool(keywords) or _has("keyword"),
            "keyword rules require a non-empty rule_config.keywords list (or keyword)",
        )
    elif rule_type == "topic":
        _require(_has("topic", "term"), "topic rules require rule_config.topic (or term)")
    elif rule_type == "similarity":
        # min_score is optional (evaluation defaults it), but when present it
        # must be a non-negative number, not free text.
        raw = cfg.get("min_score")
        if raw is not None and not isinstance(raw, bool):
            try:
                _require(float(raw) >= 0, "similarity rules require min_score >= 0")
            except (TypeError, ValueError):
                raise ValueError("similarity rules require a numeric min_score")
        elif isinstance(raw, bool):
            raise ValueError("similarity rules require a numeric min_score")
    elif rule_type == "discovery_lens":
        _require(_has("lens_id"), "discovery_lens rules require rule_config.lens_id")
    elif rule_type == "feed_monitor":
        # Either monitor_id or monitor_key/label must be present so
        # _resolve_feed_monitor_id can target a specific monitor.
        _require(
            _has("monitor_id", "monitor_key", "label"),
            "feed_monitor rules require rule_config.monitor_id (or monitor_key / label)",
        )
    elif rule_type == "branch":
        _require(_has("branch_id", "branch_label"), "branch rules require rule_config.branch_id (or branch_label)")
    elif rule_type == "library_workflow":
        workflow = str(cfg.get("workflow") or cfg.get("state") or "").strip().lower()
        _require(
            workflow in {"reading", "done", "excluded"},
            "library_workflow rules require rule_config.workflow in {reading, done, excluded}",
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
) -> dict | None:
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


def toggle_rule(db: sqlite3.Connection, rule_id: str) -> dict | None:
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


def test_fire_rule(db: sqlite3.Connection, rule_id: str) -> dict | None:
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
    # One grouped query for the latest outcome of every alert (avoids one
    # history query per alert inside build_alert_response).
    outcomes = _latest_history_outcomes(db, [str(r["id"]) for r in rows])
    return [
        build_alert_response(db, dict(r), last_outcome=outcomes.get(str(r["id"])))
        for r in rows
    ]


#: Chip severity: when one evaluation writes rows for several channels, the
#: card shows the WORST outcome so a failure is never hidden behind a success.
_OUTCOME_SEVERITY = {"failed": 0, "skipped": 1, "pending": 2, "sent": 3, "empty": 4}


def _latest_history_outcomes(db: sqlite3.Connection, alert_ids: list[str]) -> dict[str, str]:
    """Worst status among each alert's most recent evaluation batch."""
    if not alert_ids:
        return {}
    placeholders = ", ".join("?" for _ in alert_ids)
    rows = db.execute(
        f"SELECT alert_id, status, sent_at FROM alert_history WHERE alert_id IN ({placeholders})",
        alert_ids,
    ).fetchall()
    latest: dict[str, tuple[str, list[str]]] = {}
    for r in rows:
        aid = str(r["alert_id"] or "")
        ts = str(r["sent_at"] or "")
        if aid not in latest or ts > latest[aid][0]:
            latest[aid] = (ts, [str(r["status"] or "")])
        elif ts == latest[aid][0]:
            latest[aid][1].append(str(r["status"] or ""))
    return {
        aid: min(statuses, key=lambda s: _OUTCOME_SEVERITY.get(s, 99))
        for aid, (_, statuses) in latest.items()
    }


def _configured_channels() -> list[str]:
    """Delivery channels with a working notifier right now."""
    channels: list[str] = []
    for name, sender in _CHANNEL_SENDERS.items():
        try:
            if sender["notifier"]().is_configured:
                channels.append(name)
        except Exception:
            continue
    return channels


def list_alert_templates(db: sqlite3.Connection) -> list[dict]:
    """Suggest alert automations derived from current monitor, branch, and workflow state.

    Suggestions are delivery-aware: digests propose exactly the channels that
    are actually configured, and when NO channel is configured the list is
    empty — a one-click automation that could never deliver is a half-working
    fallback we don't offer (AI-is-opt-in analogue).
    Rule payloads carry ``channels: []`` — delivery channels belong to the
    digest; the rule-level column is vestigial.
    """
    delivery_channels = _configured_channels()
    if not delivery_channels:
        return []
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
                            "channels": [],
                            "enabled": True,
                        },
                        "alert": {
                            "name": f"Weekly author watch: {monitor['label']}",
                            "channels": delivery_channels,
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
                        "channels": [],
                        "enabled": True,
                    },
                    "alert": {
                        "name": f"Daily monitor digest: {monitor['label']}",
                        "channels": delivery_channels,
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
            f"""
            SELECT
                c.id,
                c.name,
                COUNT(p.id) AS item_count,
                ROUND(COALESCE(AVG(CASE WHEN p.rating > 0 THEN p.rating END), 0), 2) AS avg_rating
            FROM collections c
            LEFT JOIN collection_items ci ON ci.collection_id = c.id
            LEFT JOIN papers p ON p.id = ci.paper_id AND p.status = 'library'
              AND {standalone_paper_sql('p')}
            GROUP BY c.id, c.name
            HAVING COUNT(p.id) > 0
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
                        "channels": [],
                        "enabled": True,
                    },
                    "alert": {
                        "name": f"Weekly collection watch: {row['name']}",
                        "channels": delivery_channels,
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
            f"""
            SELECT
                COALESCE(NULLIF(r.branch_id, ''), NULL) AS branch_id,
                COALESCE(NULLIF(r.branch_label, ''), NULL) AS branch_label,
                COUNT(*) AS count,
                COALESCE(SUM(CASE WHEN r.user_action = 'like' THEN 1 ELSE 0 END), 0) AS liked,
                COALESCE(SUM(CASE WHEN r.user_action = 'dismiss' THEN 1 ELSE 0 END), 0) AS dismissed
            FROM recommendations r
            JOIN papers p ON p.id = r.paper_id
            WHERE (COALESCE(r.branch_id, '') <> '' OR COALESCE(r.branch_label, '') <> '')
              AND {standalone_paper_sql('p')}
            GROUP BY COALESCE(NULLIF(r.branch_id, ''), NULL), COALESCE(NULLIF(r.branch_label, ''), NULL)
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
                        "channels": [],
                        "enabled": True,
                    },
                    "alert": {
                        "name": f"Weekly branch watch: {branch_label}",
                        "channels": delivery_channels,
                        "schedule": "weekly",
                        "schedule_config": {"day": "monday", "time": "09:00"},
                        "format": "text",
                        "enabled": True,
                    },
                }
            )
    except Exception:
        pass

    # D2: saving a paper is NOT a reading chore. The `queued` reading-state and the
    # derived `untriaged` backlog were removed ("saved means saved"), so this
    # surface no longer suggests untriaged-cleanup / reading-queue alert
    # automations. (Reading/done/excluded stay available as opt-in `library_workflow`
    # rule values for users who DO track reading — just not nagged-about by default.)

    # Suggestions the user already materialized are dropped: keeping them
    # listed would make "Create Automation" a one-click duplicate factory.
    existing = _existing_rule_identities(db)
    templates = [
        t
        for t in templates
        if _rule_identity(t["rule"]["rule_type"], t["rule"]["rule_config"]) not in existing
    ]
    return templates[:10]


def _rule_identity(rule_type: str, rule_config: dict | None) -> tuple[str, str] | None:
    """(rule_type, primary target) identity for suggestion dedup.

    Two rules with the same identity watch the same entity, so an existing
    rule makes the matching template suggestion redundant. Returns None for
    rule types without a single-entity target (keyword, topic, ...), which
    are never suggested as templates anyway.
    """
    cfg = rule_config or {}
    primary_key = {
        "author": "author_id",
        "collection": "collection_id",
        "feed_monitor": "monitor_id",
        "discovery_lens": "lens_id",
    }.get(rule_type)
    if primary_key:
        ref = str(cfg.get(primary_key) or "").strip()
        return (rule_type, ref) if ref else None
    if rule_type == "branch":
        ref = str(cfg.get("branch_id") or cfg.get("branch_label") or "").strip()
        return (rule_type, ref) if ref else None
    return None


def _existing_rule_identities(db: sqlite3.Connection) -> set[tuple[str, str]]:
    """Identities of every stored rule, for filtering template suggestions."""
    identities: set[tuple[str, str]] = set()
    for row in db.execute("SELECT rule_type, rule_config FROM alert_rules").fetchall():
        config = _loads(row["rule_config"])
        identity = _rule_identity(str(row["rule_type"] or ""), config if isinstance(config, dict) else {})
        if identity:
            identities.add(identity)
    return identities


def apply_alert_template(db: sqlite3.Connection, template_key: str) -> dict | None:
    """Materialize one suggested automation: create its rule + digest atomically.

    Recomputes the current template list server-side (never trusts a
    client-forged payload) and applies the template matching ``template_key``.
    Both inserts run on the caller's connection inside one transaction, so a
    failure can never leave an orphan rule without its digest. Returns None
    when the key no longer resolves — e.g. already applied or the underlying
    monitor stopped qualifying.
    """
    template = next((t for t in list_alert_templates(db) if t["key"] == template_key), None)
    if template is None:
        return None
    rule_payload = template["rule"]
    alert_payload = template["alert"]
    rule = create_rule(
        db,
        name=rule_payload["name"],
        rule_type=rule_payload["rule_type"],
        rule_config=rule_payload["rule_config"],
        channels=rule_payload["channels"],
        enabled=rule_payload["enabled"],
    )
    alert = create_alert(
        db,
        name=alert_payload["name"],
        channels=alert_payload["channels"],
        schedule=alert_payload["schedule"],
        schedule_config=alert_payload.get("schedule_config"),
        format_value=alert_payload.get("format", "text"),
        enabled=alert_payload["enabled"],
        rule_ids=[rule["id"]],
    )
    return {"template_key": template_key, "template_title": template["title"], "rule": rule, "alert": alert}


def _is_unscheduled(schedule: str | None) -> bool:
    """True for schedules that carry no time slot (manual / immediate).

    Unscheduled digests must persist ``schedule_config = NULL`` — any stored
    day/time is stale noise the UI would keep rendering as badges.
    """
    return str(schedule or "").strip().lower() in {"manual", "immediate"}


# ── Schedule-slot math (single owner) ──────────────────────────────────────
# The sweep's due-ness check and the API's "next run" field must agree, so
# the slot computation lives here once; `alma.api.scheduler._is_due`
# delegates to `is_due`.

_WEEKDAYS = {"mon": 0, "tue": 1, "wed": 2, "thu": 3, "fri": 4, "sat": 5, "sun": 6}


def _parse_schedule_time(raw: str) -> tuple[int, int]:
    try:
        hour_s, minute_s = raw.strip().split(":", 1)
        return max(0, min(23, int(hour_s))), max(0, min(59, int(minute_s)))
    except Exception:
        return 9, 0


def reference_slot(
    schedule: str, schedule_config: dict | None, now: datetime
) -> datetime | None:
    """The slot boundary that decides due-ness at ``now``, or None.

    Daily: today's slot once it has passed; None before it (a daily digest
    never fires early, even when it has no evaluation history — shipped
    behaviour, pinned by tests). Weekly: the most recent target-day slot at
    or before ``now`` (rolls back across the week boundary). Manual /
    immediate / unknown: None (the sweep never fires them).
    """
    schedule_norm = str(schedule or "").strip().lower()
    config = schedule_config if isinstance(schedule_config, dict) else {}
    hour, minute = _parse_schedule_time(str(config.get("time") or "09:00"))

    if schedule_norm == "daily":
        slot = now.replace(hour=hour, minute=minute, second=0, microsecond=0)
        return None if now < slot else slot
    if schedule_norm == "weekly":
        target = _WEEKDAYS.get(str(config.get("day") or "monday").strip().lower()[:3], 0)
        slot_today = now.replace(hour=hour, minute=minute, second=0, microsecond=0)
        slot = slot_today - timedelta(days=(now.weekday() - target) % 7)
        if now < slot:
            slot -= timedelta(days=7)
        return slot
    return None


def next_slot(schedule: str, schedule_config: dict | None, now: datetime) -> datetime | None:
    """Next slot boundary strictly after ``now``; None for unscheduled."""
    schedule_norm = str(schedule or "").strip().lower()
    config = schedule_config if isinstance(schedule_config, dict) else {}
    hour, minute = _parse_schedule_time(str(config.get("time") or "09:00"))

    if schedule_norm == "daily":
        slot = now.replace(hour=hour, minute=minute, second=0, microsecond=0)
        return slot if now < slot else slot + timedelta(days=1)
    if schedule_norm == "weekly":
        target = _WEEKDAYS.get(str(config.get("day") or "monday").strip().lower()[:3], 0)
        slot_today = now.replace(hour=hour, minute=minute, second=0, microsecond=0)
        slot = slot_today + timedelta(days=(target - now.weekday()) % 7)
        if slot <= now:
            slot += timedelta(days=7)
        return slot
    return None


def is_due(
    *,
    schedule: str,
    schedule_config: dict | None,
    last_evaluated_at: str | None,
    now: datetime,
) -> bool:
    """True when the current schedule slot has not been processed yet."""
    slot = reference_slot(schedule, schedule_config, now)
    if slot is None:
        return False
    if not last_evaluated_at:
        return True
    try:
        last_dt = datetime.fromisoformat(last_evaluated_at)
    except (ValueError, TypeError):
        return True
    return last_dt < slot


def create_alert(
    db: sqlite3.Connection,
    *,
    name: str,
    channels: list[str],
    schedule: str,
    schedule_config: dict | None,
    format_value: str,
    enabled: bool,
    rule_ids: list[str],
) -> dict:
    """Create an alert plus optional rule assignments."""
    if _is_unscheduled(schedule):
        schedule_config = None
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


def get_alert(db: sqlite3.Connection, alert_id: str) -> dict | None:
    """Get one alert by ID with rule payload."""
    row = db.execute("SELECT * FROM alerts WHERE id = ?", (alert_id,)).fetchone()
    return build_alert_response(db, dict(row)) if row else None


def update_alert(
    db: sqlite3.Connection,
    alert_id: str,
    *,
    name: str | None = None,
    channels: list[str] | None = None,
    schedule: str | None = None,
    schedule_config: dict | None = None,
    format_value: str | None = None,
    enabled: bool | None = None,
) -> dict | None:
    """Partially update one alert."""
    row = db.execute("SELECT * FROM alerts WHERE id = ?", (alert_id,)).fetchone()
    if not row:
        return None
    current = dict(row)
    # Resolve the post-update schedule first: switching to manual must CLEAR
    # any stored schedule_config, otherwise the old day/time keeps rendering
    # as badges on a digest that no longer runs on a schedule.
    final_schedule = schedule if schedule is not None else current["schedule"]
    if schedule_config is not None:
        final_config_json: str | None = json.dumps(schedule_config)
    else:
        final_config_json = current.get("schedule_config")
    if _is_unscheduled(final_schedule):
        final_config_json = None
    db.execute(
        """
        UPDATE alerts
        SET name = ?, channels = ?, schedule = ?, schedule_config = ?, format = ?, enabled = ?
        WHERE id = ?
        """,
        (
            name if name is not None else current["name"],
            json.dumps(channels) if channels is not None else current["channels"],
            final_schedule,
            final_config_json,
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


def assign_rules(db: sqlite3.Connection, alert_id: str, rule_ids: list[str]) -> dict | None:
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


# Channel dispatch table: notifier factory, send call, not-configured message.
# The Slack and email flows are identical except for these three things, so
# they live here once instead of as two copy-pasted branches.
_CHANNEL_SENDERS: dict[str, dict] = {
    # `notifier` thunks resolve the module-level factory BY NAME at call time
    # (not a captured reference) so tests can monkeypatch
    # `alerts.get_slack_notifier` / `alerts.get_email_notifier`.
    "slack": {
        "notifier": lambda: get_slack_notifier(),
        "send": lambda n, papers, name: n.send_paper_alert(channel=None, papers=papers, alert_name=name),
        "unconfigured": "Slack token not configured",
        "failure": "Slack API returned failure",
    },
    "email": {
        "notifier": lambda: get_email_notifier(),
        "send": lambda n, papers, name: n.send_paper_alert(recipients=None, papers=papers, alert_name=name),
        "unconfigured": "Email/SMTP not configured",
        "failure": "Email send returned failure",
    },
}


def _gather_digest_matches(
    db: sqlite3.Connection, alert: dict
) -> tuple[list[dict], list[tuple[str, dict]], dict[str, list[tuple[str, dict]]]]:
    """Shared evaluate/dry-run read phase.

    Returns (assigned enabled rules, deduped matches, per-channel NEW papers).
    Dedup is per (alert, channel) — a paper delivered on Slack stays eligible
    for email until email actually receives it.
    """
    channels = _loads(alert["channels"]) or []
    rule_rows = _get_assigned_enabled_rules(alert["id"], db)

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
    alerted = _already_alerted_by_channel(alert["id"], db)
    new_by_channel = {
        channel: [(key, paper) for key, paper in unique_papers if key not in alerted.get(channel, set())]
        for channel in channels
    }
    return rule_rows, unique_papers, new_by_channel


async def evaluate_digest(
    db: sqlite3.Connection,
    digest_id: str,
    *,
    trigger_source: str = "user",
) -> dict | None:
    """Evaluate one digest and send notifications.

    Two phases, per the SQLite write discipline: phase 1 gathers matches and
    performs every network send WITHOUT touching the DB; phase 2 records
    dedup rows / history / last_evaluated_at. The caller owns the commit.
    """
    alert_row = db.execute("SELECT * FROM alerts WHERE id = ?", (digest_id,)).fetchone()
    if not alert_row:
        return None

    alert = dict(alert_row)
    channels = _loads(alert["channels"]) or []
    rule_rows, unique_papers, new_by_channel = _gather_digest_matches(db, alert)

    # ── Phase 1: deliver (network only, no writes) ─────────────────────────
    channel_results: dict[str, dict] = {}
    for channel_name in channels:
        new_papers = new_by_channel.get(channel_name, [])
        sender = _CHANNEL_SENDERS.get(channel_name)
        if sender is None:
            channel_results[channel_name] = {
                "status": "skipped",
                "error": f"Unsupported channel type: {channel_name}",
                "papers_new": len(new_papers),
                "papers_sent": 0,
            }
            continue
        notifier = sender["notifier"]()
        if not notifier.is_configured:
            channel_results[channel_name] = {
                "status": "skipped",
                "error": sender["unconfigured"],
                "papers_new": len(new_papers),
                "papers_sent": 0,
            }
            continue
        if not new_papers:
            channel_results[channel_name] = {"status": "empty", "error": None, "papers_new": 0, "papers_sent": 0}
            continue
        payload = [paper for _, paper in new_papers]
        try:
            ok = await sender["send"](notifier, payload, alert["name"])
            if ok:
                channel_results[channel_name] = {
                    "status": "sent",
                    "error": None,
                    "papers_new": len(new_papers),
                    "papers_sent": len(new_papers),
                }
            else:
                channel_results[channel_name] = {
                    "status": "failed",
                    "error": sender["failure"],
                    "papers_new": len(new_papers),
                    "papers_sent": 0,
                }
        except Exception as exc:
            channel_results[channel_name] = {
                "status": "failed",
                "error": str(exc),
                "papers_new": len(new_papers),
                "papers_sent": 0,
            }

    # ── Phase 2: record (writes only, network done) ────────────────────────
    now = datetime.utcnow().isoformat()
    for channel_name in channels:
        ch_result = channel_results.get(channel_name, {"status": "unknown", "error": None})
        new_papers = new_by_channel.get(channel_name, [])
        if ch_result["status"] == "sent":
            for key, _paper in new_papers:
                db.execute(
                    """
                    INSERT OR IGNORE INTO alerted_publications (id, alert_id, paper_id, channel, alerted_at)
                    VALUES (?, ?, ?, ?, ?)
                    """,
                    (uuid.uuid4().hex, digest_id, key, channel_name, now),
                )
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
                ch_result["status"],
                json.dumps([key for key, _ in new_papers]),
                len(new_papers),
                (
                    f"Sent {len(new_papers)} papers via {channel_name}"
                    if ch_result["status"] == "sent"
                    else f"{ch_result['status']}: {error_msg or 'no new papers'}"
                ),
                error_msg,
            ),
        )

    db.execute("UPDATE alerts SET last_evaluated_at = ? WHERE id = ?", (now, digest_id))

    # Top-level counters are DISTINCT papers across channels; per-channel
    # detail lives in channel_results.
    new_ids = {key for papers in new_by_channel.values() for key, _ in papers}
    sent_ids = {
        key
        for channel_name, papers in new_by_channel.items()
        if channel_results.get(channel_name, {}).get("status") == "sent"
        for key, _ in papers
    }
    failed_ids = {
        key
        for channel_name, papers in new_by_channel.items()
        if channel_results.get(channel_name, {}).get("status") == "failed"
        for key, _ in papers
    }
    return {
        "alert_id": digest_id,
        "alert_name": alert["name"],
        "digest_id": digest_id,
        "digest_name": alert["name"],
        "papers_found": len(unique_papers),
        "papers_new": len(new_ids),
        "papers_sent": len(sent_ids),
        "papers_failed": len(failed_ids),
        "matched_rules": len(rule_rows),
        "channels": channels,
        "channel_results": channel_results,
        "trigger_source": trigger_source,
        "dry_run": False,
    }


def dry_run_digest(db: sqlite3.Connection, digest_id: str) -> dict | None:
    """Evaluate digest without sending or persistence side-effects."""
    alert_row = db.execute("SELECT * FROM alerts WHERE id = ?", (digest_id,)).fetchone()
    if not alert_row:
        return None

    alert = dict(alert_row)
    channels = _loads(alert["channels"]) or []
    rule_rows, unique_papers, new_by_channel = _gather_digest_matches(db, alert)

    # Union across channels, first-seen order — what at least one channel
    # would deliver. A zero-channel digest truthfully previews nothing new.
    seen: set[str] = set()
    new_papers: list[tuple[str, dict]] = []
    for channel_papers in new_by_channel.values():
        for key, paper in channel_papers:
            if key not in seen:
                seen.add(key)
                new_papers.append((key, paper))
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
_OUTCOME_NOT_PRECOMPUTED = object()


def build_alert_response(
    db: sqlite3.Connection,
    alert_row: dict,
    *,
    last_outcome: object = _OUTCOME_NOT_PRECOMPUTED,
) -> dict:
    """Build alert payload with assigned rules, last outcome, and next run.

    ``last_outcome`` may be precomputed by ``list_alerts`` (one grouped query
    for all alerts); single-alert callers leave it unset and this fn fetches
    it here.
    """
    rows = db.execute(
        """
        SELECT ar.*
        FROM alert_rules ar
        JOIN alert_rule_assignments ara ON ara.rule_id = ar.id
        WHERE ara.alert_id = ?
        """,
        (alert_row["id"],),
    ).fetchall()
    if last_outcome is _OUTCOME_NOT_PRECOMPUTED:
        last_outcome = _latest_history_outcomes(db, [str(alert_row["id"])]).get(str(alert_row["id"]))
    schedule = alert_row["schedule"]
    schedule_config = _loads(alert_row.get("schedule_config"))
    upcoming = next_slot(
        schedule,
        schedule_config if isinstance(schedule_config, dict) else {},
        datetime.utcnow(),
    )
    return {
        "id": alert_row["id"],
        "name": alert_row["name"],
        "channels": _loads(alert_row["channels"]) or [],
        "schedule": schedule,
        "schedule_config": schedule_config,
        "format": alert_row.get("format", "grouped"),
        "enabled": bool(alert_row["enabled"]),
        "created_at": alert_row["created_at"],
        "last_evaluated_at": alert_row.get("last_evaluated_at"),
        "last_outcome": last_outcome,
        # When the hourly sweep can next fire it; None for manual digests
        # or when the digest is disabled (a disabled digest never fires).
        "next_due_at": upcoming.isoformat() if upcoming and bool(alert_row["enabled"]) else None,
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


def _already_alerted_by_channel(digest_id: str, db: sqlite3.Connection) -> dict[str, set[str]]:
    """Paper ids already delivered for this alert, keyed by channel."""
    rows = db.execute(
        "SELECT channel, paper_id FROM alerted_publications WHERE alert_id = ?",
        (digest_id,),
    ).fetchall()
    out: dict[str, set[str]] = {}
    for r in rows:
        if r["paper_id"]:
            out.setdefault(str(r["channel"] or ""), set()).add(str(r["paper_id"]))
    return out


#: History horizon. Insights diagnostics reads a 90-day weekly trend from
#: `alert_history`, so retention must never drop below 90 days.
ALERT_HISTORY_RETENTION_DAYS = 180
_ALERT_HISTORY_RETENTION_FLOOR_DAYS = 90


def prune_alert_history(
    db: sqlite3.Connection,
    *,
    retention_days: int = ALERT_HISTORY_RETENTION_DAYS,
    now: datetime | None = None,
) -> int:
    """Delete alert_history rows older than the retention window.

    Returns the number of rows removed. Called from the hourly scheduled-
    alerts sweep; the caller owns the commit.
    """
    retention_days = max(int(retention_days), _ALERT_HISTORY_RETENTION_FLOOR_DAYS)
    cutoff = ((now or datetime.utcnow()) - timedelta(days=retention_days)).isoformat()
    cursor = db.execute("DELETE FROM alert_history WHERE sent_at < ?", (cutoff,))
    return cursor.rowcount


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


def _evaluate_rule(
    rule_row: dict,
    db: sqlite3.Connection,
    *,
    alert_created_at: str | None = None,
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
            resolved = db.execute(
                """
                SELECT id, name, openalex_id
                FROM authors
                WHERE id = ? OR openalex_id = ?
                LIMIT 1
                """,
                (author_ref, author_ref),
            ).fetchone()
            if resolved:
                author_name = str(resolved["name"] or "").strip()
                openalex_id = str(resolved["openalex_id"] or openalex_id).strip()

        if _table_exists(db, "publication_authors") and openalex_id:
            where_clauses: list[str] = ["pa.openalex_id = ?"]
            params: list[object] = [openalex_id]
            if author_name:
                where_clauses.append("lower(trim(pa.display_name)) = lower(trim(?))")
                params.append(author_name)
            rows = db.execute(
                f"""
                SELECT DISTINCT p.*
                FROM papers p
                JOIN publication_authors pa ON pa.paper_id = p.id
                WHERE ({" OR ".join(where_clauses)})
                  AND {standalone_paper_sql('p')}
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
        where_clauses = ["p.author_id = ?", "p.added_from = ?"]
        params = [author_ref, author_ref]
        if author_name:
            where_clauses.append("lower(COALESCE(p.authors, '')) LIKE lower(?)")
            params.append(f"%{author_name}%")
        rows = db.execute(
            f"""
            SELECT DISTINCT p.*
            FROM papers p
            WHERE ({" OR ".join(where_clauses)})
              AND {standalone_paper_sql('p')}
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
            f"""
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
            f"""
            SELECT p.*
            FROM collection_items ci
            JOIN papers p ON p.id = ci.paper_id
            WHERE ci.collection_id = ?
              AND p.status = 'library'
              AND {standalone_paper_sql('p')}
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
            WHERE ({where})
              AND {standalone_paper_sql('p')}
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
            f"""
            SELECT DISTINCT p.*
            FROM papers p
            LEFT JOIN publication_topics pt ON pt.paper_id = p.id
            LEFT JOIN topic_aliases ta ON LOWER(ta.raw_term) = LOWER(pt.term)
            LEFT JOIN topics t ON t.topic_id = ta.topic_id
            WHERE (
                LOWER(COALESCE(pt.term, '')) LIKE ?
                OR LOWER(COALESCE(t.canonical_name, '')) LIKE ?
                OR LOWER(COALESCE(p.title, '')) LIKE ?
                OR LOWER(COALESCE(p.abstract, '')) LIKE ?
            ) AND {standalone_paper_sql('p')}
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
              AND {standalone_paper_sql('p')}
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
            f"""
            SELECT p.*
            FROM recommendations r
            JOIN papers p ON p.id = r.paper_id
            WHERE r.lens_id = ?
              AND r.score >= ?
              AND COALESCE(r.user_action, '') != 'dismiss'
              AND {standalone_paper_sql('p')}
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
        # Resolve a human-readable "what inside the monitor did this paper
        # match" string. Per D-AL-10: when an alert covers multiple rules
        # (e.g., two follow-author monitors + a keyword monitor) and the
        # same paper triggers more than one of them, the Slack card needs
        # to list ALL the matched entities -- not the abstract monitor
        # type / id -- so the user can see at a glance why the paper
        # surfaced.
        #
        # For an `author` monitor the matched entity is the author name.
        # For other monitor types we fall back to the monitor's label,
        # which is the human display string the user picked for it.
        # `_deduplicate_papers` joins these with ", " when one paper
        # matches multiple rules in the same alert.
        monitor_label_row = db.execute(
            "SELECT label, monitor_type, monitor_key FROM feed_monitors WHERE id = ?",
            (monitor_id,),
        ).fetchone()
        if monitor_label_row is not None:
            label = str(monitor_label_row["label"] or "").strip()
            display_label = label or monitor_label_row["monitor_key"] or monitor_id
            alert_source = display_label
        else:
            alert_source = monitor_id

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
              AND {standalone_paper_sql('p')}
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
        clauses = [
            "r.score >= ?",
            "COALESCE(r.user_action, '') != 'dismiss'",
            standalone_paper_sql("p"),
        ]
        params: list[Any] = [min_score]
        if branch_id:
            # The rule form stores whatever the branch Select carried, which
            # falls back to the label when the diagnostics row has no
            # branch_id. Match the label column too (only for rows without
            # an id, so a real id can't collide with someone's label).
            clauses.append("(r.branch_id = ? OR (COALESCE(r.branch_id, '') = '' AND r.branch_label = ?))")
            params.extend([branch_id, branch_id])
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
        # D2: `queued` and the derived `untriaged` backlog were removed — only the
        # opt-in reading states remain valid rule values. An obsolete rule that
        # still names queued/untriaged matches nothing (returns []), so a stale
        # saved rule degrades silently rather than resurrecting the chore.
        workflow = str(config.get("workflow") or config.get("state") or "").strip().lower()
        limit = max(1, min(500, int(config.get("limit", 25) or 25)))
        if workflow not in {"reading", "done", "excluded"}:
            return []
        where = "p.reading_status = ?"
        params = [workflow]
        rows = db.execute(
            f"""
            SELECT p.*
            FROM papers p
            WHERE {where}
              AND {standalone_paper_sql('p')}
            ORDER BY COALESCE(p.added_at, '') DESC, COALESCE(p.rating, 0) DESC
            LIMIT ?
            """,
            [*params, limit],
        ).fetchall()
        return [dict(r) for r in rows]

    return []


def _loads(value: str | None) -> object | None:
    if not value:
        return None
    try:
        return json.loads(value)
    except Exception:
        return None
