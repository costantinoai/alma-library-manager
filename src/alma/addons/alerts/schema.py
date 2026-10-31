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
