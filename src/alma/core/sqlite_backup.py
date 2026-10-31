"""The one way ALMa copies a live SQLite database.

A running database is three files, and committed rows can sit only in the WAL.
``shutil.copy`` loses those rows. Worse, opening and closing the database file
in this process drops the POSIX locks of every SQLite connection the process
holds. SQLite's backup API reads through a normal connection instead, so it
sees every committed transaction and leaves those locks alone.
"""

from __future__ import annotations

import sqlite3
from pathlib import Path

from alma.core.sqlite_config import SQLITE_CONNECT_TIMEOUT_S, apply_busy_timeout


def online_sqlite_backup(src_path: Path, dst_path: Path) -> None:
    """Copy ``src_path`` to ``dst_path`` as one consistent snapshot.

    The source opens read-only: a backup can never write to it, and a missing
    source raises instead of being created empty.
    """
    src_conn = sqlite3.connect(
        f"{Path(src_path).resolve().as_uri()}?mode=ro",
        uri=True,
        timeout=SQLITE_CONNECT_TIMEOUT_S,
    )
    try:
        apply_busy_timeout(src_conn)
        dst_conn = sqlite3.connect(str(dst_path))
        try:
            src_conn.backup(dst_conn)
        finally:
            dst_conn.close()
    finally:
        src_conn.close()
