"""Alerts tables. Created on first enable, never by core bootstrap."""

from __future__ import annotations

import sqlite3

#: Drop order: children first.
TABLES: tuple[str, ...] = (
    "alerted_publications",
    "alert_rule_assignments",
    "alert_history",
    "alerts",
    "alert_rules",
)


#: Current shape, so a database that already has these tables is left as it is.
#: A database old enough to predate per-channel dedup is upgraded by core
#: migration 28, which stays frozen and is guarded on the table existing.
DDL: tuple[str, ...] = (
    """CREATE TABLE IF NOT EXISTS alert_rules (
        id TEXT PRIMARY KEY,
        name TEXT NOT NULL,
        rule_type TEXT NOT NULL,
        rule_config TEXT NOT NULL,
        channels TEXT NOT NULL,
        enabled INTEGER DEFAULT 1,
        created_at TEXT NOT NULL
    )""",
    """CREATE TABLE IF NOT EXISTS alerts (
        id TEXT PRIMARY KEY,
        name TEXT NOT NULL,
        channels TEXT NOT NULL,
        schedule TEXT NOT NULL DEFAULT 'manual',
        schedule_config TEXT,
        format TEXT DEFAULT 'grouped',
        enabled INTEGER DEFAULT 1,
        created_at TEXT NOT NULL,
        last_evaluated_at TEXT
    )""",
    """CREATE TABLE IF NOT EXISTS alert_rule_assignments (
        alert_id TEXT NOT NULL REFERENCES alerts(id) ON DELETE CASCADE,
        rule_id TEXT NOT NULL REFERENCES alert_rules(id) ON DELETE CASCADE,
        PRIMARY KEY (alert_id, rule_id)
    )""",
    """CREATE TABLE IF NOT EXISTS alert_history (
        id TEXT PRIMARY KEY,
        rule_id TEXT,
        alert_id TEXT,
        channel TEXT NOT NULL,
        paper_id TEXT,
        sent_at TEXT NOT NULL,
        status TEXT NOT NULL,
        message_preview TEXT,
        publications TEXT,
        publication_count INTEGER DEFAULT 0,
        error_message TEXT
    )""",
    """CREATE TABLE IF NOT EXISTS alerted_publications (
        id TEXT PRIMARY KEY,
        alert_id TEXT NOT NULL REFERENCES alerts(id) ON DELETE CASCADE,
        paper_id TEXT NOT NULL,
        channel TEXT NOT NULL,
        alerted_at TEXT NOT NULL,
        UNIQUE(alert_id, paper_id, channel)
    )""",
    "CREATE INDEX IF NOT EXISTS idx_alerted_alert_paper ON alerted_publications(alert_id, paper_id)",
)


def has_user_data(conn: sqlite3.Connection) -> bool:
    """True when someone has set Alerts up, so an upgrade must keep it on.

    History alone does not count: a person with no rules and no digests is not
    using Alerts, and the rows stay on disk either way.
    """
    existing = {
        row[0]
        for row in conn.execute(
            "SELECT name FROM sqlite_master WHERE type = 'table' AND name IN ('alerts', 'alert_rules')"
        ).fetchall()
    }
    return any(
        conn.execute(f"SELECT 1 FROM {table} LIMIT 1").fetchone() is not None  # noqa: S608
        for table in sorted(existing)
    )
