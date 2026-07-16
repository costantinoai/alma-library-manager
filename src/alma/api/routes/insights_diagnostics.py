"""Diagnostics tab for the Insights page — split into per-section MVs.

The diagnostics endpoint used to compute its full payload on every GET
(~50 SQL queries, an N+1 in the branch source-mix loop, and 5+ trend-
series scans). The cost showed up as a slow first paint of Insights →
Diagnostics. We now split the payload into eight named sections, each
registered as a fingerprint-based materialised view (see
``alma.application.materialized_views``):

    feed, discovery, ai, authors, alerts, feedback, operational, evaluation

Each section's fingerprint touches only the tables that influence its
slice of the payload, so:

* On a GET, only sections whose inputs changed rebuild in the
  background; the rest are served from cache (~1 ms).
* The eight section endpoints let the frontend stream cards in
  independently with skeletons, instead of waiting for a single
  monolithic response.
* The legacy ``/diagnostics`` endpoint composes the eight section
  payloads through ``mv.get``, so existing consumers keep working
  until they migrate to the section endpoints.

Section dependencies:
    feed, discovery, ai, authors, alerts, feedback are independent.
    operational depends on ai/authors/alerts/feed (issue derivation).
    evaluation depends on every section above (composes scorecards
    and recommended actions).

Both downstream sections read upstream sections through ``mv.get`` so
they ride the cache instead of recomputing.
"""

from __future__ import annotations

import sqlite3
from collections import defaultdict
from datetime import datetime, timedelta
from typing import Any

from fastapi import Depends, HTTPException

from alma.ai.graph_versions import INSIGHTS_LOGIC_VERSION, with_version
from alma.api.deps import get_current_user, get_db
from alma.api.helpers import raise_internal, safe_div, table_exists
from alma.api.routes.insights import (
    _aggregate_http_source_diagnostics,
    _aggregate_openalex_usage,
    _build_ai_snapshot,
    _build_alert_history_trend,
    _build_alert_quality_snapshot,
    _build_author_follow_trend,
    _build_authors_snapshot,
    _build_branch_trends,
    _build_cold_start_topic_validation,
    _build_operational_snapshot,
    _build_recommendation_action_trend,
    _build_refresh_trend,
    _build_signal_lab_snapshot,
    _build_signal_lab_trend,
    _library_workflow_snapshot,
    _load_recent_operations,
    router,
)
from alma.application import materialized_views as mv
from alma.application.diagnostics_stats import (
    MIN_RATE_SAMPLE,
    RateEstimate,
)
from alma.application.recommendation_outcomes import (
    build_recommendation_outcomes,
    count_outcomes,
)

# ── Section keys ----------------------------------------------------------

DIAGNOSTICS_SECTION_KEYS: tuple[str, ...] = (
    "feed",
    "discovery",
    "ai",
    "authors",
    "alerts",
    "feedback",
    "operational",
    "evaluation",
)

_SECTION_VIEW_KEY_PREFIX = "insights:diag:"


def _section_view_key(section: str) -> str:
    return f"{_SECTION_VIEW_KEY_PREFIX}{section}"


# ── Shared helpers --------------------------------------------------------


def _list_monitors(db: sqlite3.Connection) -> list[dict[str, Any]]:
    """Return the feed_monitors list with health/last_result hydrated.

    Imported lazily — ``feed_monitors`` pulls in the broader application
    layer at registration time which would create a circular import
    chain through ``alma.api.deps``.
    """
    from alma.application import feed_monitors as monitor_app

    if not table_exists(db, "feed_monitors"):
        return []
    return monitor_app.list_feed_monitors(db)


def _shape_monitor_rows(monitors: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Project the feed_monitors list into the shape the UI expects.

    Pre-computes ``yield_rate`` from the monitor's last result, then
    sorts so degraded > disabled > ready, breaking ties on items
    created and label. Mirrors the original ordering at the top of
    ``get_insights_diagnostics``.
    """
    rows: list[dict[str, Any]] = []
    for monitor in monitors:
        last_result = monitor.get("last_result") if isinstance(monitor.get("last_result"), dict) else {}
        papers_found = last_result.get("papers_found") if isinstance(last_result, dict) else None
        items_created = last_result.get("items_created") if isinstance(last_result, dict) else None
        yield_rate: float | None = None
        if (
            isinstance(papers_found, (int, float))
            and int(papers_found) > 0
            and isinstance(items_created, (int, float))
        ):
            yield_rate = round(float(items_created) / float(papers_found), 3)
        rows.append(
            {
                "id": monitor.get("id"),
                "label": monitor.get("label"),
                "monitor_type": monitor.get("monitor_type"),
                "author_id": monitor.get("author_id"),
                "author_name": monitor.get("author_name"),
                "health": monitor.get("health"),
                "health_reason": monitor.get("health_reason"),
                "last_checked_at": monitor.get("last_checked_at"),
                "last_success_at": monitor.get("last_success_at"),
                "last_status": monitor.get("last_status"),
                "last_error": monitor.get("last_error"),
                "papers_found": int(papers_found) if isinstance(papers_found, (int, float)) else 0,
                "items_created": int(items_created) if isinstance(items_created, (int, float)) else 0,
                "yield_rate": yield_rate,
            }
        )
    rows.sort(
        key=lambda item: (
            0 if item["health"] == "degraded" else 1 if item["health"] == "disabled" else 2,
            -(item.get("items_created") or 0),
            str(item.get("label") or "").lower(),
        )
    )
    return rows


def _fetch_branch_source_mix(db: sqlite3.Connection) -> dict[str, list[dict[str, Any]]]:
    """Replace the per-branch source-mix N+1 with a single GROUP BY.

    The legacy code ran one ``SELECT … WHERE branch_id = ?`` per branch
    inside the branch_quality loop; with N branches that's N+1 queries
    plus all the planner overhead. This grouped query produces the same
    bucket distribution in one round-trip; we then sort each branch's
    bucket list in Python (``count`` desc, ``source_type`` asc) to
    match the previous ORDER BY.

    Index ``idx_recs_branch_source ON recommendations(branch_id, source_type)``
    (added in ``deps.py``) covers this scan.
    """
    if not table_exists(db, "recommendations"):
        return {}
    try:
        rows = db.execute(
            """
            SELECT
                COALESCE(NULLIF(branch_id, ''), '') AS branch_id,
                COALESCE(NULLIF(branch_label, ''), '') AS branch_label,
                COALESCE(NULLIF(source_type, ''), 'unknown') AS source_type,
                COUNT(*) AS count
            FROM recommendations
            WHERE COALESCE(branch_id, '') <> '' OR COALESCE(branch_label, '') <> ''
            GROUP BY
                COALESCE(NULLIF(branch_id, ''), ''),
                COALESCE(NULLIF(branch_label, ''), ''),
                COALESCE(NULLIF(source_type, ''), 'unknown')
            """
        ).fetchall()
    except sqlite3.OperationalError:
        return {}
    by_branch: dict[str, list[dict[str, Any]]] = {}
    for row in rows:
        # The legacy code preferred branch_id as the lookup key, falling
        # back to branch_label only when branch_id was NULL or empty.
        # Mirror that here so labels stay consistent across reloads.
        key = row["branch_id"] or row["branch_label"]
        bucket = by_branch.setdefault(key, [])
        bucket.append(
            {
                "source_type": str(row["source_type"] or "unknown"),
                "count": int(row["count"] or 0),
            }
        )
    for mix in by_branch.values():
        mix.sort(key=lambda item: (-int(item["count"]), str(item["source_type"])))
    return by_branch


def _safe_int(value: Any) -> int:
    try:
        return int(value or 0)
    except (TypeError, ValueError):
        return 0


def _safe_float(value: Any) -> float:
    try:
        return float(value or 0.0)
    except (TypeError, ValueError):
        return 0.0


# ── Section: feed ---------------------------------------------------------


def _build_diag_feed(db: sqlite3.Connection) -> dict[str, Any]:
    """Feed monitors snapshot, recent intake refreshes, daily intake trend.

    Excluded on purpose: anything that lives on the recommendations
    table — that belongs to the discovery section so the two cache
    independently.
    """
    monitors = _list_monitors(db)
    monitor_rows = _shape_monitor_rows(monitors)
    total_monitors = len(monitors)
    ready = [m for m in monitors if m.get("health") == "ready"]
    degraded = [m for m in monitors if m.get("health") == "degraded"]
    disabled = [m for m in monitors if m.get("health") == "disabled"]

    # I-26: the average yield must cover ALL monitors with a measurable yield,
    # not just the first 20 we keep for the UI table below. Computing it here
    # (over the full monitor_rows) and stashing it on the summary means the
    # evaluation scorecard reads a true population average instead of
    # re-deriving it from the sliced `monitors` list (which was the bug).
    all_yields = [r["yield_rate"] for r in monitor_rows if r["yield_rate"] is not None]
    avg_yield_rate = round(sum(all_yields) / len(all_yields), 3) if all_yields else 0.0

    refresh_ops = _load_recent_operations(
        db, operation_key="feed.refresh_inbox", limit=45
    )
    recent_refreshes: list[dict[str, Any]] = []
    for op in refresh_ops:
        result = op.get("result") or {}
        recent_refreshes.append(
            {
                "job_id": op["job_id"],
                "status": op["status"],
                "finished_at": op.get("finished_at") or op.get("updated_at"),
                "items_created": _safe_int(result.get("items_created")),
                "papers_found": _safe_int(result.get("papers_found")),
                "monitors_total": _safe_int(result.get("monitors_total")),
                "monitors_degraded": _safe_int(result.get("monitors_degraded")),
            }
        )

    feed_refresh_trend = _build_refresh_trend(
        refresh_ops,
        primary_key="items_created",
        secondary_key="papers_found",
    )

    return {
        "summary": {
            "total_monitors": total_monitors,
            "ready_monitors": len(ready),
            "degraded_monitors": len(degraded),
            "disabled_monitors": len(disabled),
            "author_monitors": sum(
                1 for m in monitors if m.get("monitor_type") == "author"
            ),
            "topic_monitors": sum(
                1 for m in monitors if m.get("monitor_type") == "topic"
            ),
            "query_monitors": sum(
                1 for m in monitors if m.get("monitor_type") == "query"
            ),
            # Population-wide yield (over every monitor that reported one), used
            # by the evaluation scorecard — not the sliced table below (I-26).
            "avg_yield_rate": avg_yield_rate,
            "monitors_with_yield": len(all_yields),
        },
        "monitors": monitor_rows[:20],
        "recent_refreshes": recent_refreshes,
        "feed_refresh_trend": feed_refresh_trend,
    }


# ── Section: discovery ----------------------------------------------------


def _build_diag_discovery(db: sqlite3.Connection) -> dict[str, Any]:
    """Recommendations totals, source/branch quality, branch trends, refresh history.

    Source diagnostics + OpenAlex usage are aggregated from the most
    recent feed AND discovery refresh operations because both refresh
    paths emit ``source_diagnostics`` envelopes; aggregating both gives
    a complete picture of HTTP transport behaviour.
    """
    discovery_ops = _load_recent_operations(
        db, operation_key="discovery.refresh_recommendations", limit=45
    )
    feed_ops = _load_recent_operations(
        db, operation_key="feed.refresh_inbox", limit=45
    )

    recent_refreshes: list[dict[str, Any]] = []
    for op in discovery_ops:
        result = op.get("result") or {}
        recent_refreshes.append(
            {
                "job_id": op["job_id"],
                "status": op["status"],
                "finished_at": op.get("finished_at") or op.get("updated_at"),
                "new_recommendations": _safe_int(result.get("new_recommendations")),
                "total_recommendations": _safe_int(result.get("total_recommendations")),
            }
        )

    combined_ops = [*feed_ops, *discovery_ops]
    combined_results = [
        (op.get("result") or {})
        for op in combined_ops
        if isinstance(op.get("result"), dict)
    ]
    # Source diagnostics take the full op rows — the aggregator date-stamps each
    # source (first/last seen, last error) from the op timestamps.
    source_diagnostics = _aggregate_http_source_diagnostics(combined_ops)
    openalex_usage = _aggregate_openalex_usage(combined_results)

    discovery_refresh_trend = _build_refresh_trend(
        discovery_ops,
        primary_key="new_recommendations",
        secondary_key="total_recommendations",
    )
    recommendation_action_trend = _build_recommendation_action_trend(db, days=30)
    branch_trends = _build_branch_trends(db, days=30)
    cold_start = _build_cold_start_topic_validation(db)

    recommendation_totals = {"total": 0, "active_unseen": 0}
    source_quality: list[dict[str, Any]] = []
    branch_quality: list[dict[str, Any]] = []

    if table_exists(db, "recommendations"):
        recent_publication_cutoff = (
            datetime.utcnow() - timedelta(days=365)
        ).date().isoformat()

        # I-21/D6: positive/negative engagement is sourced from the canonical
        # outcome projection (feedback/ratings/lifecycle), never the like/dismiss
        # user_action that D6 never stamps. `active_unseen` is genuine exposure
        # (no stamped action at all), which IS reliable, so it stays.
        outcomes = build_recommendation_outcomes(db)
        recommendation_totals = {
            "total": len(outcomes),
            "active_unseen": sum(1 for o in outcomes if not o.is_seen),
        }

        source_groups: dict[tuple[str, str], list] = defaultdict(list)
        for o in outcomes:
            source_groups[(o.source_type, o.source_api)].append(o)
        for (source_type, source_api), recs in source_groups.items():
            counts = count_outcomes(recs)
            scores = [r.score for r in recs if r.score is not None]
            avg_score = round(sum(scores) / len(scores), 3) if scores else 0.0
            source_quality.append(
                {
                    "source_type": source_type,
                    "source_api": source_api,
                    "count": counts.total,
                    "avg_score": avg_score,
                    # `liked` = all positive outcomes for this source (this view
                    # has no separate `saved` column); `dismissed` = negatives.
                    "liked": counts.positive,
                    "dismissed": counts.dismissed,
                    "seen": counts.seen_action,
                    "engagement_rate": counts.engagement_rate,
                }
            )
        source_quality.sort(key=lambda s: (-s["count"], -s["avg_score"]))

        # Single grouped query for ALL branch source mixes — replaces
        # the per-branch sub-select that previously ran inside the
        # ``for row in branch_rows`` loop (N+1).
        source_mix_by_branch = _fetch_branch_source_mix(db)

        branch_groups: dict[tuple[Any, Any], list] = defaultdict(list)
        for o in outcomes:
            if not (o.branch_id or o.branch_label):
                continue
            branch_groups[(o.branch_id, o.branch_label)].append(o)
        for (branch_id, branch_label_raw), recs in branch_groups.items():
            counts = count_outcomes(recs)
            count = counts.total
            # `saved` = deliberate library-save action; `liked` = the OTHER
            # positives (likes/loves/ratings, which live in feedback_events).
            # saved + liked == positives, so positive_rate stays
            # (saved+liked)/count — now sourced from real signal (I-21).
            saved = counts.saved
            liked = counts.liked
            dismissed = counts.dismissed
            unseen = counts.unseen
            scores = [r.score for r in recs if r.score is not None]
            avg_score = round(sum(scores) / len(scores), 3) if scores else 0.0
            core_count = sum(1 for r in recs if (r.branch_mode or "") == "core")
            explore_count = sum(1 for r in recs if (r.branch_mode or "") == "explore")
            recent_count = sum(
                1 for r in recs
                if (r.publication_date or "")[:10] >= recent_publication_cutoff
            )
            unique_sources = len({r.source_type for r in recs})
            positive_rate = safe_div(liked + saved, count)
            dismiss_rate = safe_div(dismissed, count)
            recent_share = safe_div(recent_count, count)
            dominant_mode = "core" if core_count >= explore_count else "explore"
            branch_label = branch_label_raw or branch_id or "Unnamed branch"
            mix_key = branch_id or branch_label
            source_mix = (source_mix_by_branch.get(mix_key) or [])[:4]

            # I-25: prescriptive "boost"/"cool" verdicts must clear an uncertainty
            # bar, not fire at 3-4 outcomes. We require a sufficient sample AND the
            # conservative Wilson bound (not the point estimate) to clear the
            # threshold, and we state the numerator/denominator as the evidence.
            dismiss_est = RateEstimate(dismissed, count)
            positive_est = RateEstimate(liked + saved, count)
            if dismiss_est.confidently_above(0.40):
                quality_state = "cool"
                tuning_hint = (
                    f"Mute or cool this branch — {dismissed}/{count} dismissed "
                    "(confidently too high)."
                )
            elif positive_est.confidently_above(0.28) and recent_share >= 0.35:
                quality_state = "strong"
                tuning_hint = (
                    f"Boost this branch — {liked + saved}/{count} positive and recent."
                )
            elif not positive_est.sufficient and positive_est.rate >= 0.34:
                # Promising point estimate but too thin for a prescriptive call;
                # surface the thinness instead of advising a budget change.
                quality_state = "underexplored"
                tuning_hint = (
                    f"Promising but thin — {liked + saved}/{count} positive. Needs "
                    f"{MIN_RATE_SAMPLE} outcomes for a confident verdict."
                )
            elif positive_est.sufficient and unique_sources <= 1 and positive_est.rate >= 0.20:
                quality_state = "narrow"
                tuning_hint = (
                    "Diversify source mix. Branch quality is decent but too "
                    "concentrated."
                )
            elif not positive_est.sufficient:
                quality_state = "monitor"
                tuning_hint = (
                    f"Too few outcomes to tune — {count} of {MIN_RATE_SAMPLE} needed."
                )
            else:
                quality_state = "monitor"
                tuning_hint = "Monitor this branch. It needs clearer user feedback."
            branch_quality.append(
                {
                    "branch_id": branch_id,
                    "branch_label": branch_label,
                    "count": count,
                    "avg_score": avg_score,
                    "liked": liked,
                    "saved": saved,
                    "dismissed": dismissed,
                    "unseen": unseen,
                    "engagement_rate": (
                        round((liked + saved + dismissed) / count, 3)
                        if count
                        else 0.0
                    ),
                    "positive_rate": round(positive_rate, 3),
                    "dismiss_rate": round(dismiss_rate, 3),
                    "recent_share": round(recent_share, 3),
                    "dominant_mode": dominant_mode,
                    "core_count": core_count,
                    "explore_count": explore_count,
                    "unique_sources": unique_sources,
                    "source_mix": source_mix,
                    "quality_state": quality_state,
                    "tuning_hint": tuning_hint,
                }
            )
        branch_quality.sort(key=lambda b: (-b["count"], -b["avg_score"]))

    return {
        "summary": recommendation_totals,
        "source_quality": source_quality,
        "branch_quality": branch_quality,
        "branch_trends": branch_trends,
        "cold_start_topic_validation": cold_start,
        "source_diagnostics": source_diagnostics,
        "openalex_usage": openalex_usage,
        "recent_refreshes": recent_refreshes,
        "discovery_refresh_trend": discovery_refresh_trend,
        "recommendation_action_trend": recommendation_action_trend,
    }


# ── Section: ai -----------------------------------------------------------


def _build_diag_ai(db: sqlite3.Connection) -> dict[str, Any]:
    from alma.application import discovery as discovery_app

    discovery_settings = discovery_app.read_settings(db)
    return _build_ai_snapshot(db, discovery_settings=discovery_settings)


# ── Section: authors ------------------------------------------------------


def _build_diag_authors(db: sqlite3.Connection) -> dict[str, Any]:
    monitors = _list_monitors(db)
    snapshot = _build_authors_snapshot(db, monitors)
    author_follow_trend = _build_author_follow_trend(db, days=30)
    return {**snapshot, "author_follow_trend": author_follow_trend}


# ── Section: alerts -------------------------------------------------------


def _build_diag_alerts(db: sqlite3.Connection) -> dict[str, Any]:
    snapshot = _build_alert_quality_snapshot(db, days=30)
    alert_history_trend = _build_alert_history_trend(db, days=30)
    weekly_90d = (snapshot.get("long_horizon") or {}).get("weekly_trend") or []
    return {
        **snapshot,
        "alert_history_trend": alert_history_trend,
        "alert_history_weekly_90d": weekly_90d,
    }


# ── Section: feedback (signal lab) ----------------------------------------


def _build_diag_feedback(db: sqlite3.Connection) -> dict[str, Any]:
    snapshot = _build_signal_lab_snapshot(db)
    feedback_learning_trend = _build_signal_lab_trend(db, days=30)
    return {**snapshot, "feedback_learning_trend": feedback_learning_trend}


# ── Section: operational --------------------------------------------------


def _build_diag_operational(db: sqlite3.Connection) -> dict[str, Any]:
    """Operational health.

    Reads the ai/authors/alerts payloads through ``mv.get`` so we ride
    the cache instead of recomputing those snapshots. ``mv.get`` is
    stale-while-revalidate: when an upstream view is being rebuilt we
    still get its prior payload, which is fine for derivation —
    downstream rebuilds again the next time its own fingerprint
    advances (which it does, because operational's fingerprint
    depends on the upstream sections' fingerprints).
    """
    from alma.application import discovery as discovery_app

    monitors = _list_monitors(db)
    discovery_settings = discovery_app.read_settings(db)
    ai_payload = (mv.get(db, _section_view_key("ai")).get("payload")) or {}
    authors_payload = (mv.get(db, _section_view_key("authors")).get("payload")) or {}
    alerts_payload = (mv.get(db, _section_view_key("alerts")).get("payload")) or {}

    return _build_operational_snapshot(
        db,
        monitors=monitors,
        discovery_settings=discovery_settings,
        alert_snapshot=alerts_payload,
        authors_snapshot=authors_payload,
        ai_snapshot=ai_payload,
    )


# ── Section: evaluation ---------------------------------------------------


def _score_band(score: int) -> str:
    """Map a 0..100 diagnostic score to a status band (only when we HAVE data)."""
    return "good" if score >= 75 else "attention" if score >= 50 else "critical"


def _make_scorecard(
    *,
    id: str,
    label: str,
    sample_size: int,
    min_sample: int,
    score: int | None,
    summary: str,
    detail: str,
    insufficient_summary: str | None = None,
    measures: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    """Build one diagnostics scorecard with an HONEST empty/low-data state.

    Findings I-23 / I-26: a scorecard must never present a misleading number
    when there is nothing (or almost nothing) to measure. A 0 reads as
    "critical" and an optimistic default reads as "good" — both lie when the
    underlying population is empty (no monitors, no alert runs, no recs). So:

    * ``sample_size`` is the size of the population this card scores; when it is
      below ``min_sample`` the card returns ``status='insufficient_data'`` with
      ``score=None`` and an explanatory summary, instead of a graded number.
    * ``score=None`` WITH a sufficient sample marks an "observed" card — one that
      reports separate ``measures`` rather than a single composite grade. This is
      the I-23 shape for AI Retrieval Quality, whose old composite both conflated
      unrelated quantities and capped at 87 (its weights summed to 0.87).
    * Otherwise the score is banded good / attention / critical as before.

    ``sample_size`` is always emitted so the UI can show "based on N …".
    """
    card: dict[str, Any] = {
        "id": id,
        "label": label,
        "sample_size": sample_size,
        "summary": summary,
        "detail": detail,
    }
    if measures is not None:
        card["measures"] = measures

    if sample_size < min_sample:
        card["status"] = "insufficient_data"
        card["score"] = None
        card["summary"] = insufficient_summary or (
            f"Not enough data yet — {sample_size} observed, {min_sample} needed."
        )
    elif score is None:
        # Measures-only card: observed, deliberately not reduced to one grade.
        card["status"] = "observed"
        card["score"] = None
    else:
        clamped = max(0, min(100, int(score)))
        card["status"] = _score_band(clamped)
        card["score"] = clamped
    return card


def _build_diag_evaluation(db: sqlite3.Connection) -> dict[str, Any]:
    """Composes all section scorecards, recommended actions, automation tips.

    Reads upstream sections via ``mv.get`` so it never blocks on
    recompute. The scorecards / actions logic is lifted from the
    legacy ``get_insights_diagnostics`` body unchanged — only the
    *source* of the inputs changed (cache instead of inline compute).
    """
    feed_payload = (mv.get(db, _section_view_key("feed")).get("payload")) or {}
    discovery_payload = (
        mv.get(db, _section_view_key("discovery")).get("payload")
    ) or {}
    ai_payload = (mv.get(db, _section_view_key("ai")).get("payload")) or {}
    authors_payload = (mv.get(db, _section_view_key("authors")).get("payload")) or {}
    alerts_payload = (mv.get(db, _section_view_key("alerts")).get("payload")) or {}
    feedback_payload = (mv.get(db, _section_view_key("feedback")).get("payload")) or {}
    operational_payload = (
        mv.get(db, _section_view_key("operational")).get("payload")
    ) or {}

    feed_summary = feed_payload.get("summary") or {}
    total_monitors = _safe_int(feed_summary.get("total_monitors"))
    ready_count = _safe_int(feed_summary.get("ready_monitors"))
    degraded_count = _safe_int(feed_summary.get("degraded_monitors"))
    ready_ratio = safe_div(ready_count, total_monitors or 1)
    # I-26: population-wide yield, computed in `_build_diag_feed` over EVERY
    # monitor (not the 20-row UI slice this section used to re-average).
    avg_monitor_yield = _safe_float(feed_summary.get("avg_yield_rate"))
    feed_score = max(
        0,
        min(
            100,
            round(
                ((ready_ratio * 0.7) + (min(avg_monitor_yield * 1.5, 1.0) * 0.3))
                * 100
            ),
        ),
    )

    source_quality = discovery_payload.get("source_quality") or []
    branch_quality = discovery_payload.get("branch_quality") or []
    total_source_count = sum(_safe_int(item.get("count")) for item in source_quality)
    total_source_liked = sum(_safe_int(item.get("liked")) for item in source_quality)
    total_source_dismissed = sum(
        _safe_int(item.get("dismissed")) for item in source_quality
    )
    total_source_engaged = total_source_liked + total_source_dismissed
    discovery_score = max(
        0,
        min(
            100,
            round(
                (
                    (safe_div(total_source_engaged, total_source_count or 1) * 0.45)
                    + (safe_div(total_source_liked, total_source_engaged or 1) * 0.35)
                    + (
                        (1.0 - safe_div(total_source_dismissed, total_source_engaged or 1))
                        * 0.20
                    )
                )
                * 100
            ),
        ),
    )

    active_branch_rows = [
        item for item in branch_quality if _safe_int(item.get("count")) > 0
    ]
    branch_score = max(
        0,
        min(
            100,
            round(
                (
                    (
                        safe_div(
                            sum(
                                _safe_float(item.get("positive_rate"))
                                for item in active_branch_rows
                            ),
                            max(1, len(active_branch_rows)),
                        )
                        * 0.50
                    )
                    + (
                        safe_div(
                            sum(
                                _safe_float(item.get("recent_share"))
                                for item in active_branch_rows
                            ),
                            max(1, len(active_branch_rows)),
                        )
                        * 0.20
                    )
                    + (
                        safe_div(
                            sum(
                                min(_safe_int(item.get("unique_sources")), 3) / 3.0
                                for item in active_branch_rows
                            ),
                            max(1, len(active_branch_rows)),
                        )
                        * 0.10
                    )
                    + (
                        safe_div(
                            sum(
                                (1.0 - _safe_float(item.get("dismiss_rate")))
                                for item in active_branch_rows
                            ),
                            max(1, len(active_branch_rows)),
                        )
                        * 0.20
                    )
                )
                * 100
            ),
        ),
    )

    workflow_snapshot = _library_workflow_snapshot(db)
    workflow_total = _safe_int(workflow_snapshot.get("total_library"))
    reading_count = _safe_int(workflow_snapshot.get("reading_count"))
    done_count = _safe_int(workflow_snapshot.get("done_count"))
    # D2/I-22: a save is NOT a reading chore, so reading/done are OPT-IN activity,
    # never a backlog to grade. The old 0–100 "workflow score" turned an unread
    # library red (e.g. 17/100) — exactly the obligation D2 removed. Report the
    # reading-progress DISTRIBUTION as OBSERVED measures instead (no composite
    # grade), mirroring the AI Retrieval Quality card (I-23). Denominator is the
    # saved library; each measure carries its own sample size.
    workflow_measures = [
        {
            "key": "reading",
            "label": "Currently reading",
            "value": reading_count,
            "unit": "",
            "sample_size": workflow_total,
            "sufficient": workflow_total > 0,
            "detail": f"{reading_count} of {workflow_total} saved papers are being read.",
        },
        {
            "key": "done",
            "label": "Completed",
            "value": done_count,
            "unit": "",
            "sample_size": workflow_total,
            "sufficient": workflow_total > 0,
            "detail": f"{done_count} of {workflow_total} saved papers are marked done.",
        },
        {
            "key": "saved",
            "label": "Saved library",
            "value": workflow_total,
            "unit": "",
            "sample_size": workflow_total,
            "sufficient": workflow_total > 0,
            "detail": f"{workflow_total} papers saved — reading is opt-in, not required.",
        },
    ]

    authors_summary = authors_payload.get("summary") or {}
    tracked_authors = max(1, _safe_int(authors_summary.get("tracked_authors")))
    ready_tracked = _safe_int(authors_summary.get("ready_tracked"))
    bridge_gap_count = _safe_int(authors_summary.get("bridge_gap_count"))
    stale_backfills = _safe_int(authors_summary.get("stale_backfills"))
    thin_backfills = _safe_int(authors_summary.get("thin_backfills"))
    pending_backfills = _safe_int(authors_summary.get("pending_backfills"))
    authors_score = max(
        0,
        min(
            100,
            round(
                (
                    (safe_div(ready_tracked, tracked_authors) * 0.55)
                    + ((1.0 - safe_div(bridge_gap_count, tracked_authors)) * 0.20)
                    + (
                        (1.0 - safe_div(stale_backfills + thin_backfills + pending_backfills, tracked_authors))
                        * 0.25
                    )
                )
                * 100
            ),
        ),
    )

    alerts_summary = alerts_payload.get("summary") or {}
    sent_runs_30d = _safe_int(alerts_summary.get("sent_runs_30d"))
    failed_runs_30d = _safe_int(alerts_summary.get("failed_runs_30d"))
    empty_runs_30d = _safe_int(alerts_summary.get("empty_runs_30d"))
    alerts_score = max(
        0,
        min(
            100,
            round(
                (
                    (
                        (1.0 - safe_div(failed_runs_30d, max(1, sent_runs_30d + failed_runs_30d)))
                        * 0.45
                    )
                    + (
                        (1.0 - safe_div(empty_runs_30d, max(1, sent_runs_30d + empty_runs_30d)))
                        * 0.30
                    )
                    + (
                        min(_safe_float(alerts_summary.get("avg_papers_per_sent")) / 4.0, 1.0)
                        * 0.25
                    )
                )
                * 100
            ),
        ),
    )

    feedback_summary = feedback_payload.get("summary") or {}
    signal_score = max(
        0,
        min(
            100,
            round(
                (
                    (min(_safe_int(feedback_summary.get("week_interactions")) / 10.0, 1.0) * 0.40)
                    + (min(_safe_int(feedback_summary.get("source_diversity_7d")) / 4.0, 1.0) * 0.15)
                    + (min(_safe_int(feedback_summary.get("topic_coverage")) / 8.0, 1.0) * 0.15)
                    + (
                        min(_safe_float(feedback_summary.get("recommendation_engagement_rate")) / 0.45, 1.0)
                        * 0.30
                    )
                )
                * 100
            ),
        ),
    )

    # I-23: NO composite AI score. The old 0..100 grade summed weights to 0.87
    # (so a perfect system reported 87) AND conflated four unrelated quantities
    # behind one number. Instead we surface the observed diagnostics separately,
    # each with its own population so the UI can mark "insufficient data" per
    # measure. These describe the AI retrieval substrate; they are NOT graded.
    ai_summary = ai_payload.get("summary") or {}
    ai_total_papers = _safe_int(ai_summary.get("total_papers"))
    ai_up_to_date = _safe_int(ai_summary.get("up_to_date_embeddings"))
    ai_stale = _safe_int(ai_summary.get("stale_embeddings"))
    ai_embedded = ai_up_to_date + ai_stale
    ai_recent_analyzed = _safe_int(ai_summary.get("recent_recommendations_analyzed"))
    ai_measures: list[dict[str, Any]] = [
        {
            "key": "embedding_coverage",
            "label": "Embedding coverage",
            "value": round(_safe_float(ai_summary.get("embedding_coverage_pct")), 1),
            "unit": "%",
            "sample_size": ai_total_papers,
            "sufficient": ai_total_papers > 0,
            "detail": f"{ai_up_to_date} of {ai_total_papers} papers embedded with the active model.",
        },
        {
            "key": "stale_embeddings",
            "label": "Stale embeddings",
            "value": round(safe_div(ai_stale, ai_embedded or 1) * 100.0, 1),
            "unit": "%",
            "sample_size": ai_embedded,
            "sufficient": ai_embedded > 0,
            "detail": f"{ai_stale} of {ai_embedded} embedded papers are on an older model.",
        },
        {
            "key": "avg_text_similarity",
            "label": "Avg retrieval similarity",
            "value": round(_safe_float(ai_summary.get("avg_text_similarity")), 3),
            "unit": "",
            "sample_size": ai_recent_analyzed,
            "sufficient": ai_recent_analyzed >= MIN_RATE_SAMPLE,
            "detail": f"Mean text similarity over {ai_recent_analyzed} recently scored recommendations.",
        },
    ]

    operational_summary = operational_payload.get("summary") or {}
    operational_score = max(
        0,
        min(
            100,
            round(
                (
                    (
                        (1.0 - safe_div(_safe_int(operational_summary.get("critical_count")), 3))
                        * 0.45
                    )
                    + (
                        (1.0 - safe_div(_safe_int(operational_summary.get("warning_count")), 6))
                        * 0.30
                    )
                    + (safe_div(_safe_int(operational_summary.get("healthy_checks")), 7) * 0.25)
                )
                * 100
            ),
        ),
    )

    # Sample sizes — the population each card actually scores. When a population
    # is empty the card reports "insufficient data" instead of a misleading
    # number (I-26). Rate cards (discovery / branch) need a real sample before a
    # rate is trustworthy; configuration/progress cards (monitors / library /
    # authors) just need one entity; alerts need a few runs (they run rarely).
    branch_total = sum(_safe_int(item.get("count")) for item in active_branch_rows)
    raw_tracked_authors = _safe_int(authors_summary.get("tracked_authors"))
    alert_runs_30d = (
        sent_runs_30d
        + failed_runs_30d
        + empty_runs_30d
        + _safe_int(alerts_summary.get("skipped_runs_30d"))
    )
    week_interactions = _safe_int(feedback_summary.get("week_interactions"))
    operational_checks = (
        _safe_int(operational_summary.get("healthy_checks"))
        + _safe_int(operational_summary.get("warning_count"))
        + _safe_int(operational_summary.get("critical_count"))
    )

    scorecards: list[dict[str, Any]] = [
        _make_scorecard(
            id="feed_monitor_health",
            label="Feed Monitor Health",
            sample_size=total_monitors,
            min_sample=1,
            score=feed_score,
            summary=f"{ready_count} of {total_monitors} monitors are ready.",
            detail=(
                f"Average recent yield is {avg_monitor_yield:.2f} and "
                f"{degraded_count} monitors are degraded."
            ),
            insufficient_summary="No feed monitors configured yet.",
        ),
        _make_scorecard(
            id="discovery_quality",
            label="Discovery Quality",
            sample_size=total_source_count,
            min_sample=MIN_RATE_SAMPLE,
            score=discovery_score,
            summary=(
                f"{total_source_liked} likes and {total_source_dismissed} "
                f"dismisses across {total_source_count} recommendations."
            ),
            detail=(
                f"Engagement is {safe_div(total_source_engaged, total_source_count or 1) * 100:.0f}% "
                "across tracked source groups."
            ),
            insufficient_summary=(
                f"Only {total_source_count} recommendations so far — need "
                f"{MIN_RATE_SAMPLE} before scoring discovery quality."
            ),
        ),
        _make_scorecard(
            id="branch_signal_quality",
            label="Branch Signal Quality",
            sample_size=branch_total,
            min_sample=MIN_RATE_SAMPLE,
            score=branch_score,
            summary=(
                f"{len(active_branch_rows)} branches with {branch_total} tracked "
                "recommendation outcomes."
            ),
            detail=(
                "Branch score reflects positive outcomes, recency share, "
                "source diversity, and dismiss pressure."
            ),
            insufficient_summary=(
                f"Only {branch_total} branch-attributed outcomes — need "
                f"{MIN_RATE_SAMPLE} before scoring branch quality."
            ),
        ),
        _make_scorecard(
            id="library_workflow",
            label="Library Workflow",
            sample_size=workflow_total,
            min_sample=1,
            score=None,
            measures=workflow_measures,
            summary=(
                f"{reading_count} reading, {done_count} done of "
                f"{workflow_total} saved papers."
            ),
            detail=(
                "Observed reading-progress distribution. Reading and completion are "
                "opt-in — a saved paper is not a reading chore (D2) — so this is "
                "reported, not scored."
            ),
            insufficient_summary="No saved library papers yet.",
        ),
        # I-23: AI Retrieval Quality is an OBSERVED card (no composite grade) —
        # see `ai_measures` above. `score=None` + a sufficient sample renders the
        # measures; zero papers renders "insufficient data".
        _make_scorecard(
            id="ai_quality",
            label="AI Retrieval Quality",
            sample_size=ai_total_papers,
            min_sample=1,
            score=None,
            measures=ai_measures,
            summary=(
                f"{round(_safe_float(ai_summary.get('embedding_coverage_pct')), 1)}% embedding "
                f"coverage; {ai_recent_analyzed} recent recommendations analyzed."
            ),
            detail=(
                "Observed retrieval-substrate diagnostics, reported separately "
                "rather than as one grade — each carries its own sample size."
            ),
            insufficient_summary="No papers to embed yet.",
        ),
        _make_scorecard(
            id="authors_monitoring",
            label="Authors Monitoring",
            sample_size=raw_tracked_authors,
            min_sample=1,
            score=authors_score,
            summary=(
                f"{ready_tracked} of {raw_tracked_authors} tracked authors are refresh-ready."
            ),
            detail=(
                f"{bridge_gap_count} tracked authors still need a stronger identity bridge."
            ),
            insufficient_summary="No tracked authors yet.",
        ),
        _make_scorecard(
            id="alert_automation_quality",
            label="Alert Automation Quality",
            sample_size=alert_runs_30d,
            min_sample=3,
            score=alerts_score,
            summary=(
                f"{sent_runs_30d} sent runs, {failed_runs_30d} failed, "
                f"{empty_runs_30d} empty in the last 30 days."
            ),
            detail=(
                "Alert score balances delivery reliability, non-empty output, "
                "and papers delivered per successful run."
            ),
            # I-26: with no runs the old formula scored ~75 ("good"). Now no
            # runs (or too few) is honestly "insufficient data".
            insufficient_summary=(
                f"Only {alert_runs_30d} alert runs in 30 days — too few to judge automation quality."
            ),
        ),
        _make_scorecard(
            id="feedback_learning",
            label="Feedback Learning",
            sample_size=week_interactions,
            min_sample=1,
            score=signal_score,
            summary=(
                f"{week_interactions} interactions this week with "
                f"{_safe_int(feedback_summary.get('source_diversity_7d'))} source groups touched."
            ),
            detail=(
                "Learning health reflects recent interaction depth, source diversity, "
                "topic coverage, and recommendation engagement."
            ),
            insufficient_summary="No feedback interactions this week.",
        ),
        _make_scorecard(
            id="operational_health",
            label="Operational Health",
            sample_size=operational_checks,
            min_sample=1,
            score=operational_score,
            summary=(
                f"{_safe_int(operational_summary.get('issues_total'))} active issues "
                "across embeddings, monitors, sources, alerts, and plugins."
            ),
            detail=(
                "Operational health focuses on degraded capabilities that directly "
                "affect retrieval quality, delivery, and observability."
            ),
            insufficient_summary="No operational checks have run yet.",
        ),
    ]

    # Recommended actions: lifted from the legacy endpoint body.
    recommended_actions: list[dict[str, Any]] = []
    if degraded_count:
        recommended_actions.append(
            {
                "id": "repair_degraded_monitors",
                "title": "Repair degraded monitors",
                "detail": (
                    f"{degraded_count} Feed monitors are degraded and reducing intake coverage."
                ),
                "page": "authors",
                "params": {"filter": "", "followed": "true"},
                "priority": "high",
            }
        )
    # Skip the `lens_retrieval / unknown` catch-all: that bucket holds
    # candidates whose producer didn't stamp `source_type`/`source_api`,
    # so it's a tagging gap rather than a tunable source. Surfacing it
    # here pointed users at Settings → Discovery, which has no knob for
    # it. If a producer leaks into this bucket, fix the producer.
    noisy_source = next(
        (
            item
            for item in sorted(
                source_quality,
                key=lambda row: (
                    safe_div(_safe_float(row.get("dismissed")), max(1.0, _safe_float(row.get("count")))),
                    -_safe_float(row.get("count")),
                ),
                reverse=True,
            )
            # I-25: only flag a source as noisy when its dismiss rate is
            # CONFIDENTLY high (sufficient sample + Wilson lower bound ≥ 0.35),
            # not after 4 dismissals that could be noise.
            if RateEstimate(
                _safe_int(item.get("dismissed")), _safe_int(item.get("count"))
            ).confidently_above(0.35)
            and not (
                str(item.get("source_type") or "") == "lens_retrieval"
                and str(item.get("source_api") or "") == "unknown"
            )
        ),
        None,
    )
    if noisy_source:
        recommended_actions.append(
            {
                "id": "tune_noisy_sources",
                "title": "Tune noisy discovery sources",
                "detail": (
                    f"{noisy_source['source_type']} via {noisy_source['source_api']} "
                    "is over-producing dismissals."
                ),
                "page": "settings",
                "params": {"section": "discovery"},
                "priority": "medium",
            }
        )
    best_branch = next(
        (
            item
            for item in sorted(
                branch_quality,
                key=lambda row: (_safe_float(row.get("engagement_rate")), _safe_float(row.get("count"))),
                reverse=True,
            )
            # I-25: only "operationalize" a branch once it has a defensible
            # sample, not after 3 outcomes.
            if _safe_int(item.get("count")) >= MIN_RATE_SAMPLE
            and _safe_float(item.get("engagement_rate")) >= 0.25
        ),
        None,
    )
    if best_branch:
        recommended_actions.append(
            {
                "id": "operationalize_branch",
                "title": "Operationalize a strong branch",
                "detail": (
                    f"{best_branch['branch_label']} is already engaging well "
                    "enough to turn into an alert or branch watch."
                ),
                "page": "alerts",
                "params": {"section": "rules"},
                "priority": "medium",
            }
        )
    weak_branch = next(
        (
            item
            for item in sorted(
                branch_quality,
                key=lambda row: (_safe_float(row.get("dismiss_rate")), -_safe_float(row.get("count"))),
                reverse=True,
            )
            # I-25: only advise cooling when dismissals are CONFIDENTLY high
            # (sufficient sample + Wilson lower bound ≥ 0.35).
            if RateEstimate(
                _safe_int(item.get("dismissed")), _safe_int(item.get("count"))
            ).confidently_above(0.35)
        ),
        None,
    )
    if weak_branch:
        recommended_actions.append(
            {
                "id": "cool_weak_branch",
                "title": "Cool a weak branch",
                "detail": (
                    f"{weak_branch['branch_label']} is producing too many dismissals "
                    "and should be muted, cooled, or rebalanced."
                ),
                "page": "discovery",
                "params": {},
                "priority": "medium",
            }
        )
    # I-25: the old "expand an underexplored branch" action fired on branches
    # with count <= 3 — exactly the underpowered regime where a positive_rate is
    # statistical noise. Acting on 1-3 outcomes is what I-25 forbids, so this
    # prescriptive recommendation is removed. The branch card still surfaces the
    # observational "underexplored" state (with its thin sample stated) for the
    # user to notice without the system advising a budget change on noise.
    # D2/I-22: no "triage the library backlog" action — saving a paper is not a
    # reading chore, so an unread save is not a problem to nag about.
    background_corpus_papers = _safe_int(authors_summary.get("background_corpus_papers"))
    tracked_total = _safe_int(authors_summary.get("tracked_authors"))
    if background_corpus_papers <= max(3, tracked_total):
        recommended_actions.append(
            {
                "id": "grow_followed_author_corpus",
                "title": "Grow followed-author background corpus",
                "detail": (
                    "Tracked authors still have a thin non-library historical corpus. "
                    "Run more author backfills and review dossier coverage."
                ),
                "page": "authors",
                "params": {"followed": "true"},
                "priority": "high",
            }
        )
    for item in (ai_payload.get("recommendations") or [])[:3]:
        recommended_actions.append(
            {
                "id": str(item.get("id") or "ai_recommendation"),
                "title": str(item.get("label") or "Tune AI layer"),
                "detail": str(item.get("detail") or "").strip() or "AI health needs review.",
                "page": "settings",
                "params": {"section": "ai"},
                "priority": "high"
                if str(item.get("severity") or "") == "critical"
                else "medium",
            }
        )
    if _safe_int(workflow_snapshot.get("uncollected_count")) >= 5:
        recommended_actions.append(
            {
                "id": "organize_library_structure",
                "title": "Organize uncategorized library papers",
                "detail": (
                    f"{_safe_int(workflow_snapshot.get('uncollected_count'))} "
                    "library papers are not yet assigned to any collection."
                ),
                "page": "library",
                "params": {"tab": "collections"},
                "priority": "medium",
            }
        )
    if bridge_gap_count > 0:
        recommended_actions.append(
            {
                "id": "repair_author_bridges",
                "title": "Repair author identity bridges",
                "detail": (
                    f"{bridge_gap_count} tracked authors are still missing a clean "
                    "OpenAlex bridge for reliable refresh."
                ),
                "page": "authors",
                "params": {"followed": "true"},
                "priority": "high",
            }
        )
    low_usefulness_alert = next(
        (
            item
            for item in (alerts_payload.get("top_alerts") or [])
            if _safe_int(item.get("total_runs")) >= 3
            and _safe_int(item.get("usefulness_score")) <= 55
        ),
        None,
    )
    if low_usefulness_alert is not None:
        recommended_actions.append(
            {
                "id": "tune_low_usefulness_alerts",
                "title": "Tune low-usefulness alerts",
                "detail": (
                    f"{low_usefulness_alert.get('alert_name', 'This alert')} is "
                    "generating too many empty or low-yield runs."
                ),
                "page": "alerts",
                "params": {"section": "history"},
                "priority": "medium",
            }
        )
    if _safe_int(feedback_summary.get("week_interactions")) < 8:
        recommended_actions.append(
            {
                "id": "grow_feedback_learning_coverage",
                "title": "Grow feedback-learning coverage",
                "detail": (
                    "Recent interactions are still too shallow for a strong "
                    "learning loop. Use Discovery, Feed, and Library actions "
                    "more deliberately."
                ),
                "page": "discovery",
                "params": {},
                "priority": "medium",
            }
        )
    if _safe_int(operational_summary.get("issues_total")) > 0:
        recommended_actions.append(
            {
                "id": "resolve_operational_issues",
                "title": "Resolve degraded operational states",
                "detail": (
                    f"{_safe_int(operational_summary.get('issues_total'))} active "
                    "issues are reducing product quality or delivery reliability."
                ),
                "page": "settings",
                "params": {"section": "operations"},
                "priority": "high"
                if _safe_int(operational_summary.get("critical_count")) > 0
                else "medium",
            }
        )

    automation_opportunities: list[dict[str, Any]] = []
    try:
        from alma.application import alerts as alerts_app

        automation_opportunities = alerts_app.list_alert_templates(db)[:4]
    except Exception:
        automation_opportunities = []

    return {
        "scorecards": scorecards,
        "recommended_actions": recommended_actions[:8],
        "automation_opportunities": automation_opportunities,
        "library_workflow": workflow_snapshot,
    }


# ── Materialised-view registrations ---------------------------------------
#
# Each fingerprint SQL is a single SELECT returning a tuple of values
# that change exactly when the section's payload should change.
# Downstream sections (operational, evaluation) compose their
# fingerprint from upstream sections' cached fingerprints — when an
# upstream view rebuilds, its fingerprint changes, which propagates
# automatically to the downstream view on the next GET.

_FEED_FINGERPRINT_SQL = """
    SELECT
      COALESCE((SELECT COUNT(*) FROM feed_monitors), 0),
      COALESCE((SELECT MAX(updated_at) FROM feed_monitors), ''),
      COALESCE((SELECT MAX(last_checked_at) FROM feed_monitors), ''),
      COALESCE((SELECT MAX(last_success_at) FROM feed_monitors), ''),
      COALESCE((
        SELECT MAX(COALESCE(finished_at, updated_at, started_at))
        FROM operation_status
        WHERE operation_key = 'feed.refresh_inbox'
      ), '')
"""

_DISCOVERY_FINGERPRINT_SQL = """
    SELECT
      COALESCE((SELECT COUNT(*) FROM recommendations), 0),
      COALESCE((SELECT MAX(action_at) FROM recommendations), ''),
      COALESCE((SELECT MAX(created_at) FROM recommendations), ''),
      COALESCE((
        SELECT MAX(COALESCE(finished_at, updated_at, started_at))
        FROM operation_status
        WHERE operation_key = 'discovery.refresh_recommendations'
      ), ''),
      COALESCE((SELECT MAX(created_at) FROM suggestion_sets), '')
"""

_AI_FINGERPRINT_SQL = """
    SELECT
      COALESCE((SELECT COUNT(*) FROM papers), 0),
      COALESCE((SELECT COUNT(*) FROM publication_embeddings), 0),
      COALESCE((SELECT MAX(created_at) FROM publication_embeddings), ''),
      COALESCE((SELECT value FROM discovery_settings WHERE key = 'embedding_model'), ''),
      COALESCE((SELECT MAX(created_at) FROM recommendations), '')
"""

_AUTHORS_FINGERPRINT_SQL = """
    SELECT
      COALESCE((SELECT COUNT(*) FROM authors), 0),
      COALESCE((SELECT MAX(last_fetched_at) FROM authors), ''),
      COALESCE((SELECT MAX(added_at) FROM authors), ''),
      COALESCE((SELECT COUNT(*) FROM followed_authors), 0),
      COALESCE((SELECT MAX(followed_at) FROM followed_authors), ''),
      COALESCE((SELECT MAX(last_checked_at) FROM feed_monitors WHERE monitor_type = 'author'), '')
"""

_ALERTS_FINGERPRINT_SQL = """
    SELECT
      COALESCE((SELECT COUNT(*) FROM alerts), 0),
      COALESCE((SELECT COUNT(*) FROM alert_rules), 0),
      COALESCE((SELECT COUNT(*) FROM alert_history), 0),
      COALESCE((SELECT MAX(sent_at) FROM alert_history), '')
"""

_FEEDBACK_FINGERPRINT_SQL = """
    SELECT
      COALESCE((SELECT COUNT(*) FROM feedback_events), 0),
      COALESCE((SELECT MAX(created_at) FROM feedback_events), '')
"""

_OPERATIONAL_FINGERPRINT_SQL = """
    SELECT
      'op-logic-v2',  -- logic-version token: bump when the states-list logic changes
                      -- (data fingerprints can't see code changes — task 24 de-dupe).
      COALESCE((SELECT fingerprint FROM materialized_views WHERE view_key = 'insights:diag:feed'), ''),
      COALESCE((SELECT fingerprint FROM materialized_views WHERE view_key = 'insights:diag:ai'), ''),
      COALESCE((SELECT fingerprint FROM materialized_views WHERE view_key = 'insights:diag:authors'), ''),
      COALESCE((SELECT fingerprint FROM materialized_views WHERE view_key = 'insights:diag:alerts'), ''),
      COALESCE((SELECT MAX(COALESCE(finished_at, updated_at, started_at))
                FROM operation_status WHERE status = 'failed'), ''),
      COALESCE((SELECT value FROM discovery_settings WHERE key = 'sources.openalex.enabled'), ''),
      COALESCE((SELECT value FROM discovery_settings WHERE key = 'sources.semantic_scholar.enabled'), ''),
      COALESCE((SELECT value FROM discovery_settings WHERE key = 'sources.crossref.enabled'), ''),
      COALESCE((SELECT value FROM discovery_settings WHERE key = 'sources.arxiv.enabled'), ''),
      COALESCE((SELECT value FROM discovery_settings WHERE key = 'sources.biorxiv.enabled'), '')
"""

_EVALUATION_FINGERPRINT_SQL = """
    SELECT
      COALESCE((SELECT fingerprint FROM materialized_views WHERE view_key = 'insights:diag:feed'), ''),
      COALESCE((SELECT fingerprint FROM materialized_views WHERE view_key = 'insights:diag:discovery'), ''),
      COALESCE((SELECT fingerprint FROM materialized_views WHERE view_key = 'insights:diag:ai'), ''),
      COALESCE((SELECT fingerprint FROM materialized_views WHERE view_key = 'insights:diag:authors'), ''),
      COALESCE((SELECT fingerprint FROM materialized_views WHERE view_key = 'insights:diag:alerts'), ''),
      COALESCE((SELECT fingerprint FROM materialized_views WHERE view_key = 'insights:diag:feedback'), ''),
      COALESCE((SELECT fingerprint FROM materialized_views WHERE view_key = 'insights:diag:operational'), ''),
      COALESCE((SELECT fingerprint FROM materialized_views WHERE view_key = 'health:corpus'), ''),
      COALESCE((SELECT COUNT(*) FROM papers WHERE status = 'library'), 0)
"""


_SECTION_BUILDS: dict[str, tuple[str, Any]] = {
    "feed": (_FEED_FINGERPRINT_SQL, _build_diag_feed),
    "discovery": (_DISCOVERY_FINGERPRINT_SQL, _build_diag_discovery),
    "ai": (_AI_FINGERPRINT_SQL, _build_diag_ai),
    "authors": (_AUTHORS_FINGERPRINT_SQL, _build_diag_authors),
    "alerts": (_ALERTS_FINGERPRINT_SQL, _build_diag_alerts),
    "feedback": (_FEEDBACK_FINGERPRINT_SQL, _build_diag_feedback),
    "operational": (_OPERATIONAL_FINGERPRINT_SQL, _build_diag_operational),
    "evaluation": (_EVALUATION_FINGERPRINT_SQL, _build_diag_evaluation),
}

for _section, (_fp_sql, _build_fn) in _SECTION_BUILDS.items():
    mv.register(
        mv.View(
            key=_section_view_key(_section),
            # I-4: stamp the Insights logic version into every diagnostics-section
            # fingerprint so a CODE fix to a section's computation (e.g. the I-24
            # embedding-dimension fix) invalidates its cached payload — input data
            # alone can't, which is why a corrected metric used to stay stale.
            fingerprint_sql=with_version(_fp_sql, INSIGHTS_LOGIC_VERSION),
            build_fn=_build_fn,
            operation_key=f"materialize.insights.diag.{_section}",
        )
    )


# ── Section endpoints -----------------------------------------------------


def _envelope_to_response(envelope: dict[str, Any]) -> dict[str, Any]:
    """Flatten the SWR envelope into a top-level dict the frontend expects.

    The materialised-view layer wraps the payload with ``stale``,
    ``rebuilding``, ``computed_at``, ``fingerprint``. The frontend
    pulls the data fields directly off the response, so we spread
    payload at the top level and add the SWR metadata alongside.
    """
    payload = envelope.get("payload") or {}
    return {
        **payload,
        "stale": envelope.get("stale", False),
        "rebuilding": envelope.get("rebuilding", False),
        "computed_at": envelope.get("computed_at"),
    }


@router.get(
    "/diagnostics/sections/{section}",
    summary="Get one diagnostics section",
    description=(
        "Returns just one section of the Insights diagnostics payload. "
        "Each section is cached as a materialised view with a narrow "
        "fingerprint, so cards stream in independently and stay fresh "
        "without recomputing unrelated work."
    ),
)
def get_diagnostics_section(
    section: str,
    db: sqlite3.Connection = Depends(get_db),
    user: dict = Depends(get_current_user),
):
    if section not in DIAGNOSTICS_SECTION_KEYS:
        raise HTTPException(
            status_code=404,
            detail=(
                f"Unknown diagnostics section: {section!r}. "
                f"Valid sections: {', '.join(DIAGNOSTICS_SECTION_KEYS)}."
            ),
        )
    try:
        envelope = mv.get(db, _section_view_key(section))
    except Exception as exc:  # noqa: BLE001 — surface a 5xx with context
        raise_internal(f"Failed to compute diagnostics section {section!r}", exc)
    return _envelope_to_response(envelope)


def compose_legacy_diagnostics_payload(db: sqlite3.Connection) -> dict[str, Any]:
    """Re-assemble the legacy ``InsightsDiagnostics`` shape from cached sections.

    Public so the legacy ``/diagnostics`` endpoint in ``insights.py``
    can stay a thin adapter. We always go through ``mv.get`` so the
    legacy endpoint also benefits from caching + SWR.
    """
    feed_payload = (mv.get(db, _section_view_key("feed")).get("payload")) or {}
    discovery_payload = (
        mv.get(db, _section_view_key("discovery")).get("payload")
    ) or {}
    ai_payload = (mv.get(db, _section_view_key("ai")).get("payload")) or {}
    authors_payload = (mv.get(db, _section_view_key("authors")).get("payload")) or {}
    alerts_payload = (mv.get(db, _section_view_key("alerts")).get("payload")) or {}
    feedback_payload = (mv.get(db, _section_view_key("feedback")).get("payload")) or {}
    operational_payload = (
        mv.get(db, _section_view_key("operational")).get("payload")
    ) or {}
    evaluation_payload = (
        mv.get(db, _section_view_key("evaluation")).get("payload")
    ) or {}

    feed_section_scorecards = [
        c
        for c in (evaluation_payload.get("scorecards") or [])
        if c.get("id") == "feed_monitor_health"
    ]
    discovery_section_scorecards = [
        c
        for c in (evaluation_payload.get("scorecards") or [])
        if c.get("id") in {"discovery_quality", "branch_signal_quality"}
    ]
    library_section_scorecards = [
        c
        for c in (evaluation_payload.get("scorecards") or [])
        if c.get("id") == "library_workflow"
    ]

    return {
        "generated_at": datetime.utcnow().isoformat(),
        "feed": {
            "summary": feed_payload.get("summary") or {},
            "monitors": feed_payload.get("monitors") or [],
            "recent_refreshes": feed_payload.get("recent_refreshes") or [],
            "scorecards": feed_section_scorecards,
        },
        "discovery": {
            "summary": discovery_payload.get("summary") or {},
            "source_quality": discovery_payload.get("source_quality") or [],
            "branch_quality": discovery_payload.get("branch_quality") or [],
            "branch_trends": discovery_payload.get("branch_trends") or [],
            "cold_start_topic_validation": discovery_payload.get(
                "cold_start_topic_validation"
            )
            or {},
            "source_diagnostics": discovery_payload.get("source_diagnostics") or [],
            "openalex_usage": discovery_payload.get("openalex_usage") or {},
            "recent_refreshes": discovery_payload.get("recent_refreshes") or [],
            "scorecards": discovery_section_scorecards,
        },
        "library": {
            "workflow": evaluation_payload.get("library_workflow") or {},
            "scorecards": library_section_scorecards,
        },
        "authors": {
            "summary": authors_payload.get("summary") or {},
            "degraded": authors_payload.get("degraded") or [],
            "suggestions": authors_payload.get("suggestions") or [],
            "corpus_health": authors_payload.get("corpus_health") or [],
        },
        "alerts": {
            "summary": alerts_payload.get("summary") or {},
            "top_alerts": alerts_payload.get("top_alerts") or [],
            "long_horizon": alerts_payload.get("long_horizon") or {},
        },
        "feedback_learning": {
            "summary": feedback_payload.get("summary") or {},
            "top_topics": feedback_payload.get("top_topics") or [],
            "top_authors": feedback_payload.get("top_authors") or [],
            "next_actions": feedback_payload.get("next_actions") or [],
        },
        "ai": ai_payload,
        "operational": operational_payload,
        "trends": {
            "window_days": 30,
            "feed_refresh_daily": feed_payload.get("feed_refresh_trend") or [],
            "discovery_refresh_daily": discovery_payload.get(
                "discovery_refresh_trend"
            )
            or [],
            "recommendation_actions_daily": discovery_payload.get(
                "recommendation_action_trend"
            )
            or [],
            "alert_history_daily": alerts_payload.get("alert_history_trend") or [],
            "alert_history_weekly_90d": alerts_payload.get(
                "alert_history_weekly_90d"
            )
            or [],
            "author_follows_daily": authors_payload.get("author_follow_trend") or [],
            "feedback_learning_daily": feedback_payload.get(
                "feedback_learning_trend"
            )
            or [],
        },
        "evaluation": {
            "scorecards": evaluation_payload.get("scorecards") or [],
            "recommended_actions": evaluation_payload.get("recommended_actions")
            or [],
            "automation_opportunities": evaluation_payload.get(
                "automation_opportunities"
            )
            or [],
        },
    }
