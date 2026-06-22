"""Reports: structured research-intelligence reports from library data.

Phase 4 correctness (findings **I-28..I-31**): every report now declares its
population and (where it makes a claim) its uncertainty, scopes organization to
the Library (**D5**), canonicalizes topic names, and never presents an
underpowered delta as a directional trend. The shared small-sample statistics
(Wilson intervals, Welch mean comparison, Shannon evenness) live in
``application/diagnostics_stats.py`` so "enough data" and "distinctiveness" mean
the same thing here as in the diagnostics scorecards.

Every section is independently try-wrapped: a schema variation can leave one
table missing without taking the whole report down, and the ``logger.warning``
makes a silent-empty section visible to operators (no silent failures).
"""

import json
import logging
import sqlite3
from collections import defaultdict
from datetime import datetime, timedelta
from typing import Any

from alma.api.helpers import table_exists
from alma.application import diagnostics_stats as stats

logger = logging.getLogger(__name__)

# ── Topic-drift tuning (I-30) ──────────────────────────────────────────────
# A topic must appear in at least this many papers within a window to be
# counted at all — drops one-off tags that would otherwise read as a "trend".
_MIN_TOPIC_SUPPORT = 2
# A window needs at least this many library papers before its prevalences are
# trustworthy enough to compare; below it the window is "insufficient".
_MIN_WINDOW_PAPERS = 5
# Minimum change in normalized prevalence (share of the window's papers) for a
# topic to count as emerging/fading — an effect-size floor, not a rank shuffle.
_MIN_DRIFT_EFFECT = 0.05


# ── Weekly research brief ──────────────────────────────────────────────────


def _count_papers_rated_since(conn: sqlite3.Connection, since_iso: str) -> int:
    """Distinct papers that received a positive rating EVENT since ``since_iso``.

    I-28: the metric is "papers I rated this week", which is a property of the
    rating *action's* timestamp, not the paper's ``added_at``. Ratings land in
    ``feedback_events`` (``entity_type`` publication/paper, ``value`` JSON
    carrying the numeric ``rating`` — see ``library.record_paper_feedback``), so
    we count distinct papers with a positive rating event in the window. The old
    ``added_at >= week_ago AND rating > 0`` answered a different question (newly
    *added* papers that happen to be rated) — it missed a rating applied to an
    older paper and miscounted an add-now-rate-later paper.
    """
    if not table_exists(conn, "feedback_events"):
        return 0
    try:
        rows = conn.execute(
            """SELECT entity_id, value
               FROM feedback_events
               WHERE entity_type IN ('publication', 'paper') AND created_at >= ?""",
            (since_iso,),
        ).fetchall()
    except sqlite3.OperationalError:
        return 0
    rated: set[str] = set()
    for row in rows:
        pid = str((row["entity_id"]) or "").strip()
        if not pid:
            continue
        raw = row["value"]
        try:
            payload = json.loads(raw) if isinstance(raw, str) else (raw or {})
        except (json.JSONDecodeError, TypeError):
            continue
        rating = payload.get("rating") if isinstance(payload, dict) else None
        if isinstance(rating, (int, float)) and rating > 0:
            rated.add(pid)
    return len(rated)


def weekly_research_brief(conn: sqlite3.Connection) -> dict[str, Any]:
    """Weekly summary: new papers, trending topics, active authors, engagement."""
    now = datetime.utcnow()
    week_ago = (now - timedelta(days=7)).isoformat()

    # Papers added this week (library only).
    new_papers = conn.execute(
        "SELECT COUNT(*) AS c FROM papers WHERE added_at >= ? AND status = 'library'",
        (week_ago,),
    ).fetchone()["c"]

    total_library = conn.execute(
        "SELECT COUNT(*) AS c FROM papers WHERE status = 'library'"
    ).fetchone()["c"]

    # I-28: rating EVENTS this week, derived from feedback-event timestamps.
    rated_this_week = _count_papers_rated_since(conn, week_ago)

    # Trending topics (most-added topics this week). Canonical names so merged
    # OpenAlex aliases collapse to one row (task-10 display hygiene).
    trending_topics = []
    try:
        rows = conn.execute(
            """SELECT COALESCE(t.canonical_name, pt.term) AS topic,
                      COUNT(DISTINCT pt.paper_id) AS cnt
               FROM publication_topics pt
               JOIN papers p ON p.id = pt.paper_id
               LEFT JOIN topics t ON t.topic_id = pt.topic_id
               WHERE p.added_at >= ? AND p.status = 'library'
               GROUP BY topic
               ORDER BY cnt DESC
               LIMIT 10""",
            (week_ago,),
        ).fetchall()
        trending_topics = [{"topic": r["topic"], "papers": r["cnt"]} for r in rows]
    except Exception as exc:
        logger.warning("weekly_research_brief: trending_topics query failed: %s", exc)

    # Most active followed authors (papers fetched this week).
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


# ── Collection intelligence ────────────────────────────────────────────────


def collection_intelligence(conn: sqlite3.Connection) -> dict[str, Any]:
    """Per-collection analysis over LIBRARY papers (D5): growth, impact, diversity.

    I-29: three corrections over the legacy version —
    * **Library scope** — every paper join requires ``p.status = 'library'`` so
      soft-removed / candidate rows can't inflate counts (D5: organization
      applies to saved Library papers only).
    * **Real diversity** — ``topic_diversity`` is the normalized Shannon evenness
      of the collection's full topic distribution (0 = one topic dominates, 1 =
      perfectly even), not ``len(top_five)`` which maxed at 5 and ignored the
      distribution. ``distinct_topics`` carries the raw count for display.
    * **Batched SQL** — one grouped topic query for ALL collections replaces the
      per-collection topic + year sub-selects (the old N+1).
    """
    scope = "library"
    if not table_exists(conn, "collections"):
        return {
            "report_type": "collection_intelligence",
            "collections": [],
            "total_collections": 0,
            "scope": scope,
        }

    collections: list[dict[str, Any]] = []
    try:
        # 1) Collections joined to LIBRARY papers only. The ``AND p.status =
        #    'library'`` is on the JOIN (not WHERE) so a collection with zero
        #    library papers still appears with paper_count = 0 rather than
        #    vanishing.
        rows = conn.execute(
            """SELECT c.id, c.name, c.color,
                      COUNT(p.id) AS paper_count,
                      COALESCE(AVG(p.cited_by_count), 0) AS avg_citations,
                      COALESCE(AVG(CASE WHEN p.rating > 0 THEN p.rating END), 0) AS avg_rating,
                      MAX(CASE WHEN p.id IS NOT NULL THEN ci.added_at END) AS last_added,
                      MIN(p.year) AS min_year,
                      MAX(p.year) AS max_year
               FROM collections c
               LEFT JOIN collection_items ci ON ci.collection_id = c.id
               LEFT JOIN papers p ON p.id = ci.paper_id AND p.status = 'library'
               GROUP BY c.id, c.name, c.color
               ORDER BY paper_count DESC"""
        ).fetchall()

        # 2) One grouped query for every collection's canonical-topic distribution
        #    over library papers (replaces the per-collection N+1).
        topic_dist: dict[str, list[tuple[str, int]]] = defaultdict(list)
        if table_exists(conn, "publication_topics"):
            try:
                trows = conn.execute(
                    """SELECT ci.collection_id AS cid,
                              COALESCE(t.canonical_name, pt.term) AS topic,
                              COUNT(DISTINCT pt.paper_id) AS cnt
                       FROM publication_topics pt
                       JOIN collection_items ci ON ci.paper_id = pt.paper_id
                       JOIN papers p ON p.id = pt.paper_id AND p.status = 'library'
                       LEFT JOIN topics t ON t.topic_id = pt.topic_id
                       GROUP BY ci.collection_id, topic"""
                ).fetchall()
                for r in trows:
                    topic_dist[str(r["cid"])].append(
                        (str(r["topic"]), int(r["cnt"] or 0))
                    )
            except sqlite3.OperationalError as exc:
                logger.warning("collection_intelligence: topic distribution failed: %s", exc)

        for r in rows:
            cid = str(r["id"])
            dist = sorted(topic_dist.get(cid, []), key=lambda kv: (-kv[1], kv[0]))
            top_topics = [{"topic": t, "papers": c} for t, c in dist[:5]]
            collections.append({
                "id": r["id"],
                "name": r["name"],
                "color": r["color"],
                "paper_count": int(r["paper_count"] or 0),
                "avg_citations": round(r["avg_citations"], 1),
                "avg_rating": round(r["avg_rating"], 1),
                "last_added": r["last_added"],
                "top_topics": top_topics,
                "year_range": {"min": r["min_year"], "max": r["max_year"]},
                "distinct_topics": len(dist),
                "topic_diversity": stats.shannon_evenness([c for _, c in dist]),
            })
    except Exception as e:
        logger.warning("Failed to compute collection intelligence: %s", e)

    return {
        "report_type": "collection_intelligence",
        "collections": collections,
        "total_collections": len(collections),
        "scope": scope,
    }


# ── Topic drift ────────────────────────────────────────────────────────────


def _library_papers_in_years(conn: sqlite3.Connection, y0: int, y1: int) -> int:
    """Distinct library papers published within ``[y0, y1]`` — the prevalence
    denominator for a topic-drift window."""
    try:
        row = conn.execute(
            "SELECT COUNT(*) AS c FROM papers WHERE status = 'library' AND year >= ? AND year <= ?",
            (y0, y1),
        ).fetchone()
        return int((row["c"] if row else 0) or 0)
    except sqlite3.OperationalError:
        return 0


def topic_drift(conn: sqlite3.Connection) -> dict[str, Any]:
    """How research topics shift across NON-overlapping publication-year windows.

    I-30 rebuild — the old version had three overlapping windows (last 1y / 3y /
    6y), raw counts, uncanonicalized terms, and called any top-15 set difference
    a "trend". Now:
    * **Non-overlapping 2-year windows** so a topic counted in one window is not
      also in another.
    * **Normalized prevalence** = share of the window's library papers that carry
      the topic, so a busy window can't dominate a quiet one by volume alone.
    * **Canonical topic names**, **minimum support**, and an **effect-size floor**
      on the prevalence change before a topic is called emerging/fading.
    * An explicit **insufficient** state when either compared window is too thin
      to support a claim, instead of inventing a trend from a handful of papers.
    """
    now = datetime.utcnow()
    cy = now.year
    window_defs = [
        ("recent", cy - 1, cy),
        ("mid", cy - 3, cy - 2),
        ("early", cy - 5, cy - 4),
    ]

    windows: list[dict[str, Any]] = []
    prevalence_by_window: dict[str, dict[str, float]] = {}
    for label, y0, y1 in window_defs:
        total_papers = _library_papers_in_years(conn, y0, y1)
        topics: list[dict[str, Any]] = []
        if total_papers > 0 and table_exists(conn, "publication_topics"):
            try:
                rows = conn.execute(
                    """SELECT COALESCE(t.canonical_name, pt.term) AS topic,
                              COUNT(DISTINCT pt.paper_id) AS cnt
                       FROM publication_topics pt
                       JOIN papers p ON p.id = pt.paper_id
                       LEFT JOIN topics t ON t.topic_id = pt.topic_id
                       WHERE p.status = 'library' AND p.year >= ? AND p.year <= ?
                       GROUP BY topic
                       HAVING cnt >= ?
                       ORDER BY cnt DESC
                       LIMIT 15""",
                    (y0, y1, _MIN_TOPIC_SUPPORT),
                ).fetchall()
            except sqlite3.OperationalError as exc:
                logger.warning(
                    "topic_drift: window %s (%s-%s) failed: %s", label, y0, y1, exc
                )
                rows = []
            for r in rows:
                topics.append({
                    "topic": r["topic"],
                    "papers": int(r["cnt"]),
                    "prevalence": round(int(r["cnt"]) / total_papers, 4),
                })
            prevalence_by_window[label] = {t["topic"]: t["prevalence"] for t in topics}

        windows.append({
            "label": label,
            "from_year": y0,
            "to_year": y1,
            "paper_count": total_papers,
            "top_topics": topics,
            "sufficient": stats.is_sufficient(total_papers, minimum=_MIN_WINDOW_PAPERS),
        })

    # Emerging / fading compares the recent vs early window — only when BOTH are
    # adequately powered. Effect = normalized-prevalence change; surfaced so the
    # user sees how big a shift it is, not just its sign.
    recent_ok = windows[0]["sufficient"]
    early_ok = windows[2]["sufficient"]
    insufficient = not (recent_ok and early_ok)

    emerging: list[dict[str, Any]] = []
    fading: list[dict[str, Any]] = []
    if not insufficient:
        recent_prev = prevalence_by_window.get("recent", {})
        early_prev = prevalence_by_window.get("early", {})
        deltas: list[dict[str, Any]] = []
        for topic in set(recent_prev) | set(early_prev):
            rp = recent_prev.get(topic, 0.0)
            ep = early_prev.get(topic, 0.0)
            delta = round(rp - ep, 4)
            if abs(delta) >= _MIN_DRIFT_EFFECT:
                deltas.append({
                    "topic": topic,
                    "recent_prevalence": rp,
                    "early_prevalence": ep,
                    "delta": delta,
                })
        emerging = sorted([d for d in deltas if d["delta"] > 0], key=lambda d: -d["delta"])[:10]
        fading = sorted([d for d in deltas if d["delta"] < 0], key=lambda d: d["delta"])[:10]

    note = None
    if insufficient:
        note = (
            "Not enough dated library papers in the compared windows to identify a "
            f"reliable trend (each window needs at least {_MIN_WINDOW_PAPERS} papers)."
        )

    return {
        "report_type": "topic_drift",
        "windows": windows,
        "emerging_topics": emerging,
        "fading_topics": fading,
        "insufficient": insufficient,
        "note": note,
    }


# ── Signal impact ──────────────────────────────────────────────────────────


def signal_impact(conn: sqlite3.Connection) -> dict[str, Any]:
    """ASSOCIATION between scoring signals and positive vs negative outcomes.

    I-31 rebuild — this is descriptive association, NOT causal impact, and it is
    explicit about that. For each scoring component we compare its mean value
    between positively- and negatively-received papers using a Welch difference
    of means with a 95% CI and Cohen's d (``diagnostics_stats.compare_means``).
    A signal is only called "positive"/"negative" when the cohorts are
    adequately powered AND the CI excludes zero — replacing the old ±0.01 cutoff
    on a delta with no sample size or uncertainty. The cohort is the canonical
    current outcome projection (I-21/D6), and per-signal N + the cohort sizes are
    reported so the user can judge how much to trust each row.
    """
    signal_keys = [
        "source_relevance", "topic_score", "text_similarity",
        "author_affinity", "journal_affinity", "recency_boost",
        "citation_quality", "feedback_adj", "preference_affinity",
    ]

    liked_signals: dict[str, list[float]] = {k: [] for k in signal_keys}
    dismissed_signals: dict[str, list[float]] = {k: [] for k in signal_keys}
    pos_cohort = neg_cohort = neutral_cohort = 0

    try:
        from alma.application.recommendation_outcomes import build_recommendation_outcomes

        for rec in build_recommendation_outcomes(conn):
            # Cohort sizes count every classified recommendation (the true
            # population), independent of whether it carried a score breakdown.
            if rec.is_positive:
                pos_cohort += 1
            elif rec.is_negative:
                neg_cohort += 1
            else:
                neutral_cohort += 1

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
                    target[key].append(float(sig["weighted"]))
                elif isinstance(sig, (int, float)):
                    target[key].append(float(sig))
    except Exception as e:
        logger.warning("Failed to compute signal impact: %s", e)

    signal_comparison: list[dict[str, Any]] = []
    for key in signal_keys:
        cmp = stats.compare_means(liked_signals[key], dismissed_signals[key])
        # direction → user-facing verdict; "neutral" covers both inconclusive
        # (CI spans 0) and underpowered (sample below the floor).
        impact = (
            "positive" if cmp.direction == "higher"
            else "negative" if cmp.direction == "lower"
            else "neutral"
        )
        signal_comparison.append({
            "signal": key,
            "liked_avg": cmp.mean_a,
            "dismissed_avg": cmp.mean_b,
            "liked_n": cmp.n_a,
            "dismissed_n": cmp.n_b,
            "delta": cmp.diff,
            "ci_low": cmp.ci_low,
            "ci_high": cmp.ci_high,
            "effect_size": cmp.cohens_d,
            "sufficient": cmp.sufficient,
            "direction": cmp.direction,
            "impact": impact,
        })

    # Confident associations first, then by |effect size|, then |delta|.
    signal_comparison.sort(
        key=lambda x: (
            0 if x["impact"] != "neutral" else 1,
            -abs(float(x["effect_size"])),
            -abs(float(x["delta"])),
        )
    )

    total_cohort = pos_cohort + neg_cohort + neutral_cohort
    return {
        "report_type": "signal_impact",
        "method": "association",
        # Association only — not causal. Subject to exposure/selection bias: the
        # user only reacts to recommendations they were shown, in the order shown.
        "note": (
            "Association, not causation. Compares scoring components between "
            "positively- and negatively-received papers; subject to exposure and "
            "ranking-position selection bias."
        ),
        "liked_count": pos_cohort,
        "dismissed_count": neg_cohort,
        "cohort": {
            "positive": pos_cohort,
            "negative": neg_cohort,
            "neutral": neutral_cohort,
            "total": total_cohort,
        },
        "sufficient": (
            pos_cohort >= stats.MIN_GROUP_SAMPLE and neg_cohort >= stats.MIN_GROUP_SAMPLE
        ),
        "signals": signal_comparison,
    }
