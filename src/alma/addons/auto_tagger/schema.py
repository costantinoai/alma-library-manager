"""Tag suggestion table. Created on first enable, never by core bootstrap."""

from __future__ import annotations

import sqlite3
from typing import Any

TABLES: tuple[str, ...] = ("tag_suggestions",)

#: A database that already holds this table, in either of the two shapes older
#: code created, is left as it is. Their columns match; only a default and the
#: paper foreign key differ.
DDL: tuple[str, ...] = (
    """CREATE TABLE IF NOT EXISTS tag_suggestions (
        paper_id TEXT NOT NULL REFERENCES papers(id) ON DELETE CASCADE,
        tag TEXT NOT NULL,
        tag_id TEXT,
        confidence REAL NOT NULL,
        source TEXT NOT NULL,
        accepted INTEGER DEFAULT 0,
        created_at TEXT DEFAULT (datetime('now')),
        PRIMARY KEY (paper_id, tag)
    )""",
)


def has_user_data(conn: sqlite3.Connection) -> bool:
    """True when suggestions exist, so an upgrade must keep the tagger on."""
    table = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = 'tag_suggestions'"
    ).fetchone()
    if table is None:
        return False
    return conn.execute("SELECT 1 FROM tag_suggestions LIMIT 1").fetchone() is not None


def move_to_merged_tag(conn: sqlite3.Connection, source: Any, target: Any) -> None:
    """Point suggestions at the surviving tag. The caller owns the write unit."""
    conn.execute(
        """
        INSERT OR IGNORE INTO tag_suggestions
            (paper_id, tag, tag_id, confidence, source, accepted, created_at)
        SELECT paper_id, ?, ?, confidence, source, accepted, created_at
        FROM tag_suggestions
        WHERE tag_id = ?
        """,
        (target["name"], target["id"], source["id"]),
    )
    conn.execute("DELETE FROM tag_suggestions WHERE tag_id = ?", (source["id"],))
