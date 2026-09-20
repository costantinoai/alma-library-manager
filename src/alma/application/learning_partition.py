"""The one background refresh of the semantic partition.

Two callers, one operation key (`semantic.partition.refresh`): the periodic
scheduler tick and the Health repair *learning_partition*.
"""

from __future__ import annotations

import sqlite3


def refresh_learning_partition(conn: sqlite3.Connection) -> dict:
    """Build the partition if it never was, drop memberships whose vector is
    gone, assign newly embedded papers, and check the regions' freshness."""
    from alma.application import super_regions
    from alma.application.semantic_partition import (
        assign_missing_members,
        build_partition,
        prune_vectorless_members,
        read_state,
    )

    built = False
    if read_state(conn) is None:
        if build_partition(conn) is None:
            return {"message": "Not enough embedded papers for semantic groups"}
        built = True
    pruned = prune_vectorless_members(conn)
    assigned = assign_missing_members(conn)
    envelope = super_regions.ensure_regions_fresh(conn) or {}
    return {
        "built": built,
        "pruned": pruned,
        **assigned,
        "regions_rebuilding": bool(envelope.get("rebuilding") or envelope.get("stale")),
        "message": "Semantic membership refreshed; region freshness checked",
    }
