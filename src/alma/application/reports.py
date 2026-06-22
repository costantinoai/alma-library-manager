"""Reports: generate structured research intelligence reports from library data."""

import logging
import sqlite3
from datetime import datetime, timedelta
from typing import Any

logger = logging.getLogger(__name__)


def weekly_research_brief(conn: sqlite3.Connection) -> dict[str, Any]:
    """Weekly summary: new papers, trending topics, active authors."""
    now = datetime.utcnow()
    week_ago = (now - timedelta(days=7)).isoformat()

    # Papers added this week
    new_papers = conn.execute(
        "SELECT COUNT(*) AS c FROM papers WHERE added_at >= ? AND status = 'library'",
        (week_ago,),
    ).fetchone()["c"]

    # Total library size
    total_library = conn.execute(
        "SELECT COUNT(*) AS c FROM papers WHERE status = 'library'"
    ).fetchone()["c"]

    # Papers rated this week
    rated_this_week = conn.execute(
        "SELECT COUNT(*) AS c FROM papers WHERE added_at >= ? AND rating > 0",
        (week_ago,),
    ).fetchone()["c"]

    # Trending topics (most-added topics this week)
    trending_topics = []
    try:
        rows = conn.execute(
            """SELECT pt.term, COUNT(DISTINCT pt.paper_id) AS cnt
               FROM publication_topics pt
               JOIN papers p ON p.id = pt.paper_id
               WHERE p.added_at >= ? AND p.status = 'library'
               GROUP BY pt.term
               ORDER BY cnt DESC
               LIMIT 10""",
            (week_ago,),
        ).fetchall()
        trending_topics = [{"topic": r["term"], "papers": r["cnt"]} for r in rows]
    except Exception as exc:
        # Loud-on-degrade: each report section is independently
        # try-wrapped because schema variations can leave one table
        # missing without taking the whole report down. The log makes
        # those silent-empty sections visible to operators.
        logger.warning("weekly_research_brief: trending_topics query failed: %s", exc)

    # Most active followed authors (papers fetched this week)
    active_authors = []
    try:
        rows = conn.execute(
            """SELECT a.name, COUNT(DISTINCT fi.paper_id) AS new_papers
               FROM feed_items fi
               JOIN authors a ON a.id = fi.author_id
               WHERE fi.fetched_at >= ?
               GROUP BY a.name
               ORDER BY new_papers DESC
               LIMIT 10""",
            (week_ago,),
        ).fetchall()
        active_authors = [{"name": r["name"], "new_papers": r["new_papers"]} for r in rows]
    except Exception as exc:
        logger.warning("weekly_research_brief: active_authors query failed: %s", exc)

    # Recommendations engagement this week. I-21/D6: `liked`/`dismissed` come
    # from the canonical outcome projection (feedback/ratings/lifecycle), not the
    # like/dismiss user_action that D6 never stamps.
    rec_stats = {"total": 0, "liked": 0, "dismissed": 0}
    try:
        from alma.application.recommendation_outcomes import (
            build_recommendation_outcomes,
            count_outcomes,
        )

        counts = count_outcomes(build_recommendation_outcomes(conn, since=week_ago))
        rec_stats = {
            "total": counts.total,
            "liked": counts.positive,
            "dismissed": counts.dismissed,
        }
    except Exception as exc:
        logger.warning("weekly_research_brief: rec_stats query failed: %s", exc)

    return {
        "report_type": "weekly_brief",
        "period": {"from": week_ago, "to": now.isoformat()},
        "new_papers": new_papers,
        "total_library": total_library,
        "rated_this_week": rated_this_week,
        "trending_topics": trending_topics,
        "active_authors": active_authors,
        "recommendations": rec_stats,
    }


def collection_intelligence(conn: sqlite3.Connection) -> dict[str, Any]:
    """Per-collection analysis: growth, citation impact, topic diversity."""
    collections = []
    try:
        rows = conn.execute(
            """SELECT c.id, c.name, c.color,
                      COUNT(ci.paper_id) AS paper_count,
                      COALESCE(AVG(p.cited_by_count), 0) AS avg_citations,
                      COALESCE(AVG(CASE WHEN p.rating > 0 THEN p.rating END), 0) AS avg_rating,
                      MAX(ci.added_at) AS last_added
               FROM collections c
               LEFT JOIN collection_items ci ON ci.collection_id = c.id
               LEFT JOIN papers p ON p.id = ci.paper_id
               GROUP BY c.id, c.name, c.color
               ORDER BY paper_count DESC"""
        ).fetchall()

        for r in rows:
            # Topic diversity for this collection
            topics = []
            try:
                topic_rows = conn.execute(
                    """SELECT pt.term, COUNT(*) AS cnt
                       FROM publication_topics pt
                       JOIN collection_items ci ON ci.paper_id = pt.paper_id
                       WHERE ci.collection_id = ?
                       GROUP BY pt.term
                       ORDER BY cnt DESC
                       LIMIT 5""",
                    (r["id"],),
                ).fetchall()
                topics = [{"topic": tr["term"], "papers": tr["cnt"]} for tr in topic_rows]
            except Exception as exc:
                logger.warning(
                    "collection_intelligence: top_topics for collection %s failed: %s",
                    r["id"], exc,
                )

            # Year range
            year_range = {"min": None, "max": None}
            try:
                yr = conn.execute(
                    """SELECT MIN(p.year) AS mn, MAX(p.year) AS mx
                       FROM papers p
                       JOIN collection_items ci ON ci.paper_id = p.id
                       WHERE ci.collection_id = ? AND p.year IS NOT NULL""",
                    (r["id"],),
                ).fetchone()
                if yr:
                    year_range = {"min": yr["mn"], "max": yr["mx"]}
            except Exception as exc:
                logger.warning(
                    "collection_intelligence: year_range for collection %s failed: %s",
                    r["id"], exc,
                )

            collections.append({
                "id": r["id"],
                "name": r["name"],
                "color": r["color"],
                "paper_count": r["paper_count"],
                "avg_citations": round(r["avg_citations"], 1),
                "avg_rating": round(r["avg_rating"], 1),
                "last_added": r["last_added"],
                "top_topics": topics,
                "year_range": year_range,
                "topic_diversity": len(topics),
            })
    except Exception as e:
        logger.warning("Failed to compute collection intelligence: %s", e)

    return {
        "report_type": "collection_intelligence",
        "collections": collections,
        "total_collections": len(collections),
    }


def topic_drift(conn: sqlite3.Connection) -> dict[str, Any]:
    """Track how the user's research topics shift over time windows."""
    now = datetime.utcnow()
    current_year = now.year

    windows = []
    for label, years_back in [("recent", 1), ("mid", 3), ("early", 6)]:
        from_year = current_year - years_back
        to_year = current_year if label == "recent" else current_year - (years_back - (3 if label == "mid" else 3))

        try:
            rows = conn.execute(
                """SELECT pt.term, COUNT(DISTINCT pt.paper_id) AS cnt
                   FROM publication_topics pt
                   JOIN papers p ON p.id = pt.paper_id
                   WHERE p.status = 'library' AND p.year >= ? AND p.year <= ?
                   GROUP BY pt.term
                   ORDER BY cnt DESC
                   LIMIT 15""",
                (from_year, current_year if label == "recent" else from_year + (2 if label == "mid" else 5)),
            ).fetchall()
            topics = [{"topic": r["term"], "papers": r["cnt"]} for r in rows]
        except Exception as exc:
            logger.warning(
                "topic_drift: topics query for window %s (years %s-%s) failed: %s",
                label, from_year, current_year, exc,
            )
            topics = []

        windows.append({
            "label": label,
            "from_year": from_year,
            "to_year": current_year if label == "recent" else from_year + (2 if label == "mid" else 5),
            "top_topics": topics,
        })

    # Compute emerging vs fading topics
    recent_set = {t["topic"] for t in (windows[0]["top_topics"] if windows else [])}
    early_set = {t["topic"] for t in (windows[2]["top_topics"] if len(windows) > 2 else [])}

    emerging = sorted(recent_set - early_set)
    fading = sorted(early_set - recent_set)

    return {
        "report_type": "topic_drift",
        "windows": windows,
        "emerging_topics": emerging,
        "fading_topics": fading,
    }


def signal_impact(conn: sqlite3.Connection) -> dict[str, Any]:
    """Analyze which scoring signals correlate with liked vs dismissed papers."""
    signal_keys = [
        "source_relevance", "topic_score", "text_similarity",
        "author_affinity", "journal_affinity", "recency_boost",
        "citation_quality", "feedback_adj", "preference_affinity",
    ]

    liked_signals: dict[str, list[float]] = {k: [] for k in signal_keys}
    dismissed_signals: dict[str, list[float]] = {k: [] for k in signal_keys}

    # I-21/D6: bucket by the authoritative POSITIVE / NEGATIVE outcome
    # (feedback/ratings/lifecycle), not `user_action IN ('like','dismiss')` —
    # like/dislike never land in user_action, so the old query only ever saw the
    # dismiss side and silently dropped every real positive.
    try:
        import json

        from alma.application.recommendation_outcomes import build_recommendation_outcomes

        for rec in build_recommendation_outcomes(conn):
            breakdown = (rec.score_breakdown or "").strip()
            if not breakdown:
                continue
            if rec.is_positive:
                target = liked_signals
            elif rec.is_negative:
                target = dismissed_signals
            else:
                continue
            try:
                bd = json.loads(breakdown)
            except (json.JSONDecodeError, TypeError):
                continue
            for key in signal_keys:
                sig = bd.get(key)
                if isinstance(sig, dict) and "weighted" in sig:
                    target[key].append(sig["weighted"])
                elif isinstance(sig, (int, float)):
                    target[key].append(float(sig))
    except Exception as e:
        logger.warning("Failed to compute signal impact: %s", e)

    def _avg(vals: list[float]) -> float:
        return round(sum(vals) / len(vals), 4) if vals else 0.0

    signal_comparison = []
    for key in signal_keys:
        liked_avg = _avg(liked_signals[key])
        dismissed_avg = _avg(dismissed_signals[key])
        delta = round(liked_avg - dismissed_avg, 4)
        signal_comparison.append({
            "signal": key,
            "liked_avg": liked_avg,
            "dismissed_avg": dismissed_avg,
            "delta": delta,
            "impact": "positive" if delta > 0.01 else ("negative" if delta < -0.01 else "neutral"),
        })

    # Sort by absolute delta descending
    signal_comparison.sort(key=lambda x: abs(x["delta"]), reverse=True)

    return {
        "report_type": "signal_impact",
        "liked_count": len(next((v for v in liked_signals.values() if v), [])),
        "dismissed_count": len(next((v for v in dismissed_signals.values() if v), [])),
        "signals": signal_comparison,
    }
