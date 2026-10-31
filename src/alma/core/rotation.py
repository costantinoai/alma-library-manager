"""Bounded picks from a ranked pool that never silence the same items.

A hard limit (an external API's, or a network budget) on a fixed order leaves
the same papers out on every run. The fix has one shape everywhere: keep the
strongest half of the budget, and walk the rest of the pool over successive
runs, so every item is used within ``ceil(ring / rotated share)`` runs.

The ring is ordered by the hash of each item, and the cursor is the last hash
visited. A count would shift whenever the pool grows and could step over an
item; a hash position cannot, so a new paper joins the ring without any other
paper losing its turn.

A window is a pure function of ``(ranked pool, budget, cursor)``. The caller
records :meth:`RotationWindow.record` in the job's result, which is what makes
a run reproducible, and saves the cursor with :func:`save_cursors` only after
the run served (:meth:`RotationWindow.served_by`). A completed lookup serves,
empty or not; a failed call retries the same window.
"""

from __future__ import annotations

import bisect
import hashlib
import math
import sqlite3
from collections.abc import Callable, Iterable, Sequence
from dataclasses import dataclass
from typing import Generic, TypeVar

T = TypeVar("T")

_CURSOR_KEY_PREFIX = "rotation."


def _ring_position(item_key: str) -> str:
    return hashlib.sha256(item_key.encode("utf-8")).hexdigest()


@dataclass(frozen=True)
class RotationWindow(Generic[T]):
    """One run's pick: the strongest kept, then the next stretch of the ring."""

    name: str
    picked: tuple[T, ...]
    kept: int
    rotated: int
    pool: int
    cursor_before: str
    cursor_after: str

    @property
    def runs_per_lap(self) -> int:
        """Runs until the ring returns to this window; 1 when the pool fits."""
        if not self.rotated:
            return 1
        return math.ceil((self.pool - self.kept) / self.rotated)

    def served_by(self, completed: Iterable[T]) -> bool:
        """Whether this run's completed lookups move the window on.

        The cursor walks only the rotated share, and the kept items are asked
        again on every run, so only the rotated share decides. One completed
        lookup there is enough: an item that failed comes back within a lap,
        while waiting for all of them would let one broken item hold the ring.
        When no rotated item completed the turn is kept, so strong seeds that
        answered before a rate limit hit cannot walk past items never asked.
        """
        done = set(completed)
        return any(item in done for item in self.picked[self.kept :] or self.picked)

    def record(self) -> dict[str, int | str]:
        """What a job result stores so this exact pick can be reproduced."""
        return {
            "pool": self.pool,
            "kept": self.kept,
            "rotated": self.rotated,
            "cursor_before": self.cursor_before,
            "cursor_after": self.cursor_after,
        }


def take_window(
    ranked: Sequence[T],
    *,
    name: str,
    budget: int,
    cursor: str,
    key: Callable[[T], str] = str,
) -> RotationWindow[T]:
    """Pick at most ``budget`` items from ``ranked``, strongest first.

    Args:
        ranked: The whole pool, strongest first.
        name: The rotation this window belongs to.
        budget: The true limit. A pool that fits is returned whole and leaves
            the cursor where it was.
        cursor: The saved ring position, ``""`` on the first run.
        key: The item's stable identity.

    Returns:
        The window. ``picked`` lists the kept items, then the rotated ones.
    """
    budget = max(1, int(budget))
    pool = len(ranked)
    if pool <= budget:
        return RotationWindow(name, tuple(ranked), pool, 0, pool, cursor, cursor)

    keep = budget // 2
    kept = list(ranked[:keep])
    ring = sorted((_ring_position(key(item)), index) for index, item in enumerate(ranked[keep:], keep))
    positions = [position for position, _ in ring]
    start = bisect.bisect_right(positions, cursor)
    share = budget - keep
    stretch = [ring[(start + step) % len(ring)] for step in range(share)]
    rotated = [ranked[index] for _, index in stretch]
    return RotationWindow(
        name,
        tuple(kept + rotated),
        len(kept),
        len(rotated),
        pool,
        cursor,
        stretch[-1][0],
    )


def load_cursor(db: sqlite3.Connection, name: str) -> str:
    row = db.execute(
        "SELECT value FROM discovery_settings WHERE key = ?",
        (f"{_CURSOR_KEY_PREFIX}{name}",),
    ).fetchone()
    return str(row[0]) if row else ""


def save_cursors(db: sqlite3.Connection, windows: Iterable[RotationWindow]) -> None:
    """Advance each window's rotation. The caller owns the write transaction."""
    for window in windows:
        if window.cursor_after == window.cursor_before:
            continue
        db.execute(
            """
            INSERT INTO discovery_settings (key, value, updated_at)
            VALUES (?, ?, datetime('now'))
            ON CONFLICT(key) DO UPDATE SET
                value = excluded.value,
                updated_at = excluded.updated_at
            """,
            (f"{_CURSOR_KEY_PREFIX}{window.name}", window.cursor_after),
        )
