"""Canonical per-author attention model — ONE source of truth.

Two surfaces answer the question "which tracked authors need attention":

* Health → System status → "Tracked authors" popup
  (``insights`` authors snapshot + operational diagnostics)
* Authors page → "Needs attention" section
  (``GET /authors/needs-attention``)

Historically each computed its own overlapping-but-different subset inline
(the popup looked at monitor health + corpus backfills, the page at identity
resolution), so the two disagreed on both membership and counts. Every
attention dimension now lives HERE and both routes compose from these
builders — a new dimension goes in this module, never inline in a route.

Dimensions:

``identity``
    The id-resolution ladder (error / no_match / needs_manual_review /
    followed-without-openalex). Predicate + severity ranking come from
    :mod:`alma.services.health` (already the shared source for assess_authors),
    so counts, rows, and ranks can never diverge.

``monitors``
    Author-type feed monitors whose health is ``degraded``. Callers pass the
    serialized monitor list from
    :func:`alma.application.feed_monitors.list_feed_monitors` — the single
    place monitor health is computed — so this module never re-derives it.

``corpus``
    Followed-author historical-backfill states. ``stale`` / ``thin`` /
    ``failed`` / ``unverified`` are actionable (a Backfill fixes them);
    ``pending`` / ``running`` are informational — queued work is not a
    problem to fix and must never grow a button.

Everything here is a pure read.
"""

from __future__ import annotations

import sqlite3
from typing import Any

from alma.application.feed_monitors import author_monitor_can_fetch
from alma.application.followed_authors import get_followed_author_backfill_status
from alma.services import health as health_service

# Corpus states a Backfill action genuinely fixes vs. states that only inform.
CORPUS_ACTIONABLE_STATES = {"stale", "thin", "failed", "unverified"}
CORPUS_INFORMATIONAL_STATES = {"pending", "running"}


def _table_exists(db: sqlite3.Connection, table: str) -> bool:
    row = db.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name=?", (table,)
    ).fetchone()
    return row is not None


# ── identity ────────────────────────────────────────────────────────────────


def identity_attention_count(db: sqlite3.Connection) -> int:
    """Uncapped count of authors matching the canonical attention predicate."""
    if not _table_exists(db, "authors"):
        return 0
    where = health_service.author_attention_where_sql()
    row = db.execute(f"SELECT COUNT(*) AS c FROM authors a WHERE {where}").fetchone()
    return int((row["c"] if row else 0) or 0)


def identity_attention_rows(db: sqlite3.Connection, limit: int = 50) -> list[dict[str, Any]]:
    """Ranked identity-resolution attention rows.

    Severity ordering: error > no_match > needs_manual_review >
    followed-without-openalex; within a bucket, most-recently-seen first.
    Both the CASE and the WHERE come from the canonical ladder in
    :mod:`alma.services.health` (hardcoded SQL constants — safe to
    interpolate), the same fragments ``assess_authors`` counts with.
    """
    if not _table_exists(db, "authors"):
        return []
    severity_case = health_service.author_attention_severity_case_sql()
    where = health_service.author_attention_where_sql()
    rows = db.execute(
        f"""
        SELECT
            a.id AS author_id,
            a.name AS author_name,
            COALESCE(a.openalex_id, '') AS openalex_id,
            COALESCE(a.id_resolution_status, '') AS status,
            COALESCE(a.id_resolution_method, '') AS method,
            COALESCE(a.id_resolution_confidence, 0.0) AS confidence,
            COALESCE(a.id_resolution_reason, '') AS reason,
            COALESCE(a.id_resolution_updated_at, a.last_fetched_at, '') AS updated_at,
            {severity_case}
        FROM authors a
        WHERE {where}
        ORDER BY severity ASC, updated_at DESC, a.name ASC
        LIMIT ?
        """,
        (limit,),
    ).fetchall()
    return [dict(r) for r in rows]


# ── monitors ────────────────────────────────────────────────────────────────


def monitor_attention_rows(monitors: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Degraded author-type monitors from an already-serialized monitor list.

    ``monitors`` must be the output of
    :func:`alma.application.feed_monitors.list_feed_monitors` (or the feed
    snapshot built on it) so monitor health has exactly one computation.
    """
    return [
        {
            "author_id": str(m.get("author_id") or "") or None,
            "author_name": str(m.get("author_name") or m.get("label") or "").strip()
            or "Tracked author",
            "monitor_id": str(m.get("id") or ""),
            "health_reason": m.get("health_reason"),
            "last_error": m.get("last_error"),
            "last_checked_at": m.get("last_checked_at"),
            "can_fetch": author_monitor_can_fetch(m),
        }
        for m in monitors
        if str(m.get("monitor_type") or "") == "author" and m.get("health") == "degraded"
    ]


# ── corpus backfills ────────────────────────────────────────────────────────


def corpus_backfill_rows(
    db: sqlite3.Connection,
) -> tuple[dict[str, int], list[dict[str, Any]]]:
    """Classify every followed author's historical-backfill state.

    Returns ``(state_counts, rows)`` where ``state_counts`` tallies ALL
    states (fresh/running/pending/stale/thin/failed/unverified — the
    uncapped truth for summary counters) and ``rows`` lists the authors in
    a non-fresh state (actionable + informational), shaped as the
    diagnostics ``corpus_health`` entries.

    An author with no OpenAlex id is left out: no backfill can run for them, and
    their identity row owns the fix.

    Background-publication counts are pre-computed in one grouped query —
    the per-author helper would otherwise re-issue the same heavy 3-way
    JOIN per author (an N+1 that ran the diagnostics tab into ~60 s on a
    50-author corpus).
    """
    counts = {
        "fresh": 0,
        "running": 0,
        "pending": 0,
        "stale": 0,
        "thin": 0,
        "failed": 0,
        "unverified": 0,
    }
    rows: list[dict[str, Any]] = []
    followed_rows = db.execute(
        """
        SELECT a.id, a.name, COALESCE(a.works_count, 0) AS works_count
        FROM authors a
        JOIN followed_authors fa ON fa.author_id = a.id
        WHERE COALESCE(TRIM(a.openalex_id), '') <> ''
        ORDER BY a.name
        """
    ).fetchall()

    bg_counts: dict[str, int] = {}
    library_counts: dict[str, int] = {}
    if followed_rows:
        bg_rows = db.execute(
            """
            SELECT a.id AS author_id, p.status = 'library' AS in_library, COUNT(DISTINCT p.id) AS c
            FROM authors a
            JOIN followed_authors fa ON fa.author_id = a.id
            JOIN publication_authors pa
              ON lower(a.openalex_id) = lower(pa.openalex_id)
             AND a.openalex_id IS NOT NULL
             AND TRIM(a.openalex_id) <> ''
            JOIN papers p ON p.id = pa.paper_id
            GROUP BY a.id, in_library
            """
        ).fetchall()
        for r in bg_rows:
            target = library_counts if r["in_library"] else bg_counts
            target[str(r["author_id"])] = int(r["c"] or 0)

    for row in followed_rows:
        author_id = str(row["id"] or "").strip()
        if not author_id:
            continue
        status = get_followed_author_backfill_status(
            db,
            author_id,
            works_count=int(row["works_count"] or 0),
            background_publications=bg_counts.get(author_id, 0),
            library_publications=library_counts.get(author_id, 0),
        )
        state = str(status.get("state") or "unknown")
        if state in counts:
            counts[state] += 1
        if state in CORPUS_ACTIONABLE_STATES or state in CORPUS_INFORMATIONAL_STATES:
            rows.append(
                {
                    "author_id": author_id,
                    "author_name": str(row["name"] or "").strip() or author_id,
                    "state": state,
                    "detail": str(status.get("detail") or "").strip() or None,
                    "background_publications": int(status.get("background_publications") or 0),
                    "coverage_ratio": status.get("coverage_ratio"),
                    "last_success_at": status.get("last_success_at"),
                }
            )
    return counts, rows


#: Suggested actions ALMa can run unattended. Spelled once: "Fix all" takes them,
#: and every count of what needs a person is what is left.
#: `manual_search` is not one: the automatic search already found nothing.
AUTO_FIX_CODES = frozenset(
    {"refresh", "retry_refresh", "resolve_now", "refresh_monitor", "backfill_author"}
)


def _identity_action(status: str, has_openalex: bool) -> dict[str, str]:
    """Map the row shape to a user-facing action."""
    if status == "error":
        return {"code": "retry_refresh", "label": "Resolve identity",
                "hint": "The last refresh hit an exception. Retry it, paste an identifier, or accept that they can't be identified."}
    if status == "no_match":
        return {"code": "manual_search", "label": "Resolve identity",
                "hint": "Automatic search returned zero hits. Search the candidates, paste an identifier, or accept that they can't be identified."}
    if status == "needs_manual_review":
        return {"code": "review_candidates", "label": "Review candidates",
                "hint": "Multiple OpenAlex candidates scored too close to each other. Pick the right one."}
    if not has_openalex:
        return {"code": "resolve_now", "label": "Resolve identity",
                "hint": "Followed author with no OpenAlex identifier. Retry the resolver, paste an identifier, or accept that they can't be identified."}
    return {"code": "refresh", "label": "Refresh",
            "hint": "Re-run the full refresh pipeline."}


#: The identity statuses only a person can settle. Read from `_identity_action`,
#: so the count cannot hold a second opinion about which rows are decisions.
_IDENTITY_DECISION_STATUSES = tuple(
    status
    for status in health_service.AUTHOR_ATTENTION_STATUSES
    if _identity_action(status, True)["code"] not in AUTO_FIX_CODES
)


def _split_profile_clusters(db: sqlite3.Connection) -> list[list[sqlite3.Row]]:
    """Followed authors sharing a name across OpenAlex ids: one person, split.

    Each cluster is ``[primary, *alts]``: the most recently active member that
    still has an open question, and the members it may be the same person as.
    A pair the user called "not the same person" (D17) is out for good, so a
    cluster with nothing left to ask is no cluster.

    Names are NFKD-folded, lowercased and stripped of punctuation (the form the
    suggestion-rail dedup uses), so "Müller" / "MÜLLER" / "Muller" collapse but
    "Müller" / "Mueller" stay distinct. Only clusters the user followed at least
    twice count: every author-table cluster would be too noisy on a fresh import.
    """
    from alma.application.author_merge import _pair, rejected_merge_pairs
    from alma.application.authors import _normalize_author_display_name

    cluster_rows = db.execute(
        """
        SELECT a.id, a.name, a.openalex_id, a.id_resolution_updated_at, a.last_fetched_at
        FROM authors a
        JOIN followed_authors fa ON fa.author_id = a.id
        WHERE COALESCE(a.openalex_id, '') <> ''
        """
    ).fetchall()

    clusters: dict[str, list[sqlite3.Row]] = {}
    for cr in cluster_rows:
        name_key = _normalize_author_display_name(str(cr["name"] or ""))
        if not name_key:
            continue
        clusters.setdefault(name_key, []).append(cr)

    rejected = rejected_merge_pairs(db)
    open_clusters: list[list[sqlite3.Row]] = []
    for members in clusters.values():
        # Most recent activity first, then id, so the primary is stable.
        members.sort(
            key=lambda r: (
                str(r["id_resolution_updated_at"] or r["last_fetched_at"] or ""),
                str(r["id"]),
            ),
            reverse=True,
        )
        for primary in members:
            alts = [
                other
                for other in members
                if other["id"] != primary["id"]
                and _pair(str(primary["id"]), str(other["id"])) not in rejected
            ]
            if alts:
                open_clusters.append([primary, *alts])
                break
    return open_clusters


def _orcid_owner(db: sqlite3.Connection, field: str, value: str, author_id: str) -> str | None:
    """The active author already holding a conflict's ORCID: "use alt" cannot win then."""
    if field != "orcid":
        return None
    row = db.execute(
        """
        SELECT name FROM authors
        WHERE lower(trim(orcid)) = lower(?) AND id != ? AND COALESCE(status, 'active') != 'removed'
        LIMIT 1
        """,
        (value, author_id),
    ).fetchone()
    return str(row["name"]) if row else None


_CONFLICT_FIELD_LABELS = {
    "orcid": "ORCID",
    "scholar_id": "Google Scholar ID",
    "semantic_scholar_id": "Semantic Scholar ID",
}


def _people_rows(db: sqlite3.Connection, *, limit: int) -> list[dict[str, Any]]:
    """Merge conflicts, affiliation conflicts, split profiles, then identity rows."""
    if not _table_exists(db, "authors"):
        return []
    # Identity rows come from the canonical attention builders in
    # alma.services.author_attention — the SAME module the Health popup's
    # diagnostics snapshot composes from, so the two surfaces can never
    # select or rank different buckets. (The severity CASE + WHERE inside
    # it are the shared alma.services.health ladder assess_authors uses.)
    ranked = identity_attention_rows(db, limit=limit)

    split_profiles: list[dict] = []
    for primary, *others in _split_profile_clusters(db):
        alts = [
            {
                "author_id": str(r["id"]),
                "openalex_id": str(r["openalex_id"]),
                "display_name": str(r["name"] or ""),
            }
            for r in others
        ]
        primary_name = str(primary["name"] or "").strip() or str(primary["id"])
        primary_oid = str(primary["openalex_id"] or "")
        split_profiles.append(
            {
                "author_id": str(primary["id"]),
                "author_name": primary_name,
                "openalex_id": primary_oid or None,
                "status": "split_profiles",
                "method": None,
                "confidence": 0.0,
                "reason_code": "split_profiles",
                "reason": f"Multiple OpenAlex profiles for the same name ({len(others) + 1} found)",
                "reason_detail": (
                    f"Primary: {primary_oid}; alternates: "
                    + ", ".join(a["openalex_id"] for a in alts)
                ),
                "alt_profiles": alts,
                "suggested_action": {
                    "code": "review_profiles",
                    "label": "Review profiles",
                    "hint": (
                        "Same person split across multiple OpenAlex IDs. "
                        "Merge them to keep one follow and one corpus, or "
                        "say they are not the same person, which you can undo from the Merge dialog."
                    ),
                },
                "updated_at": str(
                    primary["id_resolution_updated_at"]
                    or primary["last_fetched_at"]
                    or ""
                )
                or None,
            }
        )

    def _reason_text(status: str, raw_reason: str) -> str:
        """Friendly user-facing summary."""
        if status == "resolved_manual":
            # Followed, manual identifiers saved, and none of them an OpenAlex id.
            return "Identifiers saved, but no OpenAlex ID found yet, so no papers can be fetched."
        if raw_reason:
            return raw_reason
        if status == "error":
            return "Last refresh raised an exception."
        if status == "no_match":
            return "OpenAlex found no author matching this name."
        if status == "needs_manual_review":
            return "Automatic candidates were too close to decide."
        return "Identifier not yet resolved."

    def _detail_text(
        status: str,
        method: str,
        confidence: float,
        has_oa: bool,
    ) -> str | None:
        """Concrete second-line context: which resolver step ran, how
        confidently, whether an ID at least exists. The frontend renders
        this under the main reason so the user can see *what failed
        and how* before clicking the action button.

        Examples:
        - "Last attempted: openalex_search · confidence 0.42 — too low to auto-pick"
        - "ORCID resolved (A5012345678) but OpenAlex still missing"
        - "No resolver step has run yet"
        """
        bits: list[str] = []
        if method and method != "none":
            bits.append(f"Last step: {method}")
        if confidence > 0.0 and status != "error":
            bits.append(f"confidence {confidence:.2f}")
        if status == "needs_manual_review" and confidence > 0.0:
            bits.append("too low to auto-pick")
        if has_oa and status in ("error", "no_match"):
            bits.append("OpenAlex ID is set; refresh failed against it")
        if not bits:
            return "No resolver step has run yet" if status == "unresolved" else None
        return " · ".join(bits)

    out: list[dict] = []
    for row in ranked:
        status = str(row["status"] or "")
        openalex_id = str(row["openalex_id"] or "").strip()
        has_oa = bool(openalex_id)
        method = str(row["method"] or "")
        confidence = float(row["confidence"] or 0.0)
        action = _identity_action(status, has_oa)
        out.append(
            {
                "author_id": str(row["author_id"]),
                "author_name": str(row["author_name"] or row["author_id"]),
                "openalex_id": openalex_id or None,
                # A manual save with no OpenAlex id is not resolved, whatever it stored.
                "status": "unresolved" if status in ("", "resolved_manual") else status,
                "method": method or None,
                "confidence": confidence,
                "reason_code": action["code"],
                "reason": _reason_text(status, str(row["reason"] or "")),
                "reason_detail": _detail_text(status, method, confidence, has_oa),
                "suggested_action": action,
                "updated_at": str(row["updated_at"] or "") or None,
            }
        )

    # Merge conflicts — emitted when a previous merge_author_profiles
    # call had a hard-identifier conflict (orcid / scholar_id /
    # semantic_scholar_id) where both rows held a different non-null
    # value. The merge proceeded (primary's value won), but the alt's
    # value is preserved here so the user can review and decide which
    # is correct. Surfaces ABOVE split_profiles because conflicts
    # carry concrete evidence of a likely data-integrity issue.
    from alma.application.author_merge import list_unresolved_conflicts

    merge_conflicts: list[dict] = []
    for c in list_unresolved_conflicts(db):
        primary_name = str(c.get("primary_name") or c.get("primary_author_id") or "")
        field = str(c.get("field") or "")
        field_label = _CONFLICT_FIELD_LABELS.get(field, field)
        primary_val = str(c.get("primary_value") or "")
        alt_val = str(c.get("alt_value") or "")
        merge_conflicts.append(
            {
                "author_id": str(c.get("primary_author_id") or ""),
                "author_name": primary_name,
                "openalex_id": c.get("primary_openalex_id"),
                "status": "merge_conflict",
                "method": None,
                "confidence": 0.0,
                "reason_code": "merge_conflict",
                "reason": f"Merge kept one of two {field_label} values",
                "reason_detail": (
                    f"Primary: {primary_val} · merged-in alt "
                    f"({c.get('alt_openalex_id')}) had {alt_val}. "
                    "Pick which value to keep."
                ),
                "conflict_id": str(c.get("id") or ""),
                "conflict_field": field,
                "conflict_primary_value": primary_val,
                "conflict_alt_value": alt_val,
                "conflict_alt_owner": _orcid_owner(db, field, alt_val, str(c.get("primary_author_id") or "")),
                "alt_openalex_id": c.get("alt_openalex_id"),
                "suggested_action": {
                    "code": "resolve_conflict",
                    "label": "Resolve",
                    "hint": (
                        "Two profiles got merged but disagreed on a "
                        "hard identifier. Confirm which value should "
                        "stick — or dismiss if both are wrong."
                    ),
                },
                "updated_at": str(c.get("created_at") or "") or None,
            }
        )

    # Split-profile clusters surface above the existing rows (severity
    # bucket "split" sits above the unresolved bucket but below true
    # errors/no_match — they are actionable but not blocking like a
    # missing identifier on a followed author).
    from alma.application.author_affiliation import list_affiliation_conflicts

    affiliation_conflicts: list[dict] = []
    for c in list_affiliation_conflicts(db, limit=limit):
        first = c.get("first") or {}
        second = c.get("second") or {}
        first_name = str(first.get("institution_name") or "")
        second_name = str(second.get("institution_name") or "")
        affiliation_conflicts.append(
            {
                "author_id": str(c.get("author_id") or ""),
                "author_name": str(c.get("author_name") or ""),
                "openalex_id": c.get("openalex_id"),
                "status": "affiliation_conflict",
                "method": None,
                "confidence": 0.0,
                "reason_code": "affiliation_conflict",
                "reason": "Affiliation evidence disagrees across sources",
                "reason_detail": (
                    f"Top evidence points to {first_name}; "
                    f"near-tie evidence points to {second_name}."
                ),
                "selected_affiliation": c.get("selected_affiliation"),
                "suggested_action": {
                    "code": "pick_affiliation",
                    "label": "Review affiliation",
                    "hint": (
                        "Open the evidence list and choose which "
                        "institution should be displayed for this author."
                    ),
                },
                "updated_at": str(first.get("observed_at") or second.get("observed_at") or "") or None,
            }
        )

    return merge_conflicts + affiliation_conflicts + split_profiles + out


def _operational_rows(db: sqlite3.Connection) -> list[dict[str, Any]]:
    """Degraded author monitors and corpus backfills, none of them a decision."""
    # ── Operational attention: degraded monitors + corpus backfills ──────
    # The Health popup has always shown these; the page must show the SAME
    # rows or the two surfaces disagree (the bug this composition fixes).
    # Sourced from the same canonical builders as the diagnostics snapshot.
    from alma.application.feed_monitors import list_feed_monitors

    operational: list[dict] = []
    for m in monitor_attention_rows(list_feed_monitors(db)):
        if not m.get("author_id"):
            continue
        # With no OpenAlex id a refresh cannot fetch anything. That is an
        # identity gap: its row owns the fix, and accepting the author as
        # unidentified must end the warning.
        if not m.get("can_fetch"):
            continue
        reason = str(m.get("health_reason") or "").strip()
        last_error = str(m.get("last_error") or "").strip()
        operational.append(
            {
                "author_id": str(m["author_id"]),
                "author_name": m["author_name"],
                "openalex_id": None,
                "status": "degraded_monitor",
                "method": None,
                "confidence": 0.0,
                "reason_code": "degraded_monitor",
                "reason": "Feed monitor is degraded — refreshes are not landing",
                "reason_detail": (
                    (f"Reason: {reason}. " if reason else "")
                    + (f"Last error: {last_error}. " if last_error and last_error != reason else "")
                    or None
                ),
                "monitor_id": m.get("monitor_id"),
                "suggested_action": {
                    "code": "refresh_monitor",
                    "label": "Refresh monitor",
                    "hint": "Queue a refresh for this author's feed monitor now.",
                },
                "updated_at": str(m.get("last_checked_at") or "") or None,
            }
        )

    _corpus_state_reason = {
        "stale": "Historical backfill is stale — new papers since the last successful run",
        "thin": "Historical backfill looks thin — fewer papers than this author has published",
        "failed": "Last historical backfill failed",
        "unverified": "Historical backfill never verified — coverage unknown",
    }
    _counts, corpus_rows = corpus_backfill_rows(db)
    for c in corpus_rows:
        state = str(c.get("state") or "")
        if state not in CORPUS_ACTIONABLE_STATES:
            continue  # pending/running inform in Health; they are not fixable rows
        operational.append(
            {
                "author_id": str(c["author_id"]),
                "author_name": c["author_name"],
                "openalex_id": None,
                "status": f"corpus_{state}",
                "method": None,
                "confidence": 0.0,
                "reason_code": "corpus_backfill",
                "reason": _corpus_state_reason.get(state, f"Historical backfill {state}"),
                "reason_detail": str(c.get("detail") or "") or None,
                "suggested_action": {
                    "code": "backfill_author",
                    "label": "Backfill corpus",
                    "hint": "Queue a historical-corpus backfill for this author.",
                },
                "updated_at": str(c.get("last_success_at") or "") or None,
            }
        )

    return operational


def _marked(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [
        {**row, "auto_fixable": row["suggested_action"]["code"] in AUTO_FIX_CODES}
        for row in rows
    ]


#: No cap for a count: `limit` bounds what one page renders.
_EVERY_ROW = 1_000_000


def needs_attention_rows(db: sqlite3.Connection, *, limit: int = 50) -> list[dict[str, Any]]:
    """The one list of authors that need attention: the Authors page shows it."""
    return _marked(_people_rows(db, limit=limit) + _operational_rows(db))


def needs_attention_page(db: sqlite3.Connection, *, limit: int) -> tuple[int, list[dict[str, Any]]]:
    """The true row count and the first ``limit`` rows, for a list that says when it is cut."""
    rows = needs_attention_rows(db, limit=_EVERY_ROW)
    return len(rows), rows[:limit]


def author_decision_count(db: sqlite3.Connection) -> int:
    """Rows of `needs_attention_rows` only a person can settle, without building them.

    Home reads this on every load. Each term counts one group of `_people_rows`
    from the same source, and the monitor and corpus rows are never decisions.
    """
    if not _table_exists(db, "authors"):
        return 0
    from alma.application.author_affiliation import list_affiliation_conflicts
    from alma.application.author_merge import list_unresolved_conflicts

    where = health_service.author_attention_where_sql()
    statuses = ", ".join(f"'{status}'" for status in _IDENTITY_DECISION_STATUSES)
    identity = db.execute(
        f"""
        SELECT COUNT(*) AS c
        FROM authors a
        WHERE ({where})
          AND COALESCE(a.id_resolution_status, '') IN ({statuses})
        """
    ).fetchone()["c"]
    return (
        int(identity)
        + len(list_unresolved_conflicts(db))
        + len(list_affiliation_conflicts(db, limit=None))
        + len(_split_profile_clusters(db))
    )


def needs_attention_counts(db: sqlite3.Connection) -> dict[str, int]:
    """What Health and the Authors page say about the same rows."""
    rows = needs_attention_rows(db, limit=_EVERY_ROW)
    automatic = sum(row["auto_fixable"] for row in rows)
    return {
        "authors": len({row["author_id"] for row in rows}),
        "automatic": automatic,
        "decisions": len(rows) - automatic,
    }
