"""Which retrieval channels surface papers the user keeps.

A lens fuses four retrieval channels (lexical / vector / graph / external) by
weights that were only ever set by hand. Every refresh records, for each
candidate it SURFACED, which channels found it
(``discovery_ranking_candidates.retrieval_hits``), and the outcome owner knows
which papers the user later kept. Joining the two gives each channel's
**yield**: of the papers it helped surface, the share that was kept.

The yield scales the lens's channel weights — what gets *retrieved*. It never
touches the score: how a paper reached the user is exposure, and exposure stays
out of the reward model (``ranker.py``). This is the retrieval-side replacement
for the per-source score multiplier that left the scoring path on 2026-07-27.

**Honest at small n, with no tuned constant.** Rates are shrunk toward the
pooled rate by a strength derived from the data
(``scoring_math.empirical_bayes_rates``): when the channels' rates differ by no
more than their sample sizes explain, every multiplier is exactly 1.0 and the
hand-set weights act alone. A paper found by several channels counts for each —
it is evidence for each. Surfaced-and-ignored is the denominator, not explicit
rejection: rejections are too rare (and too old) to estimate anything from.
"""

from __future__ import annotations

import json
import sqlite3
from collections import defaultdict
from typing import Any

from alma.ai.graph_versions import with_version
from alma.application import materialized_views as mv
from alma.application.outcome_calibration import MULTIPLIER_BAND
from alma.core.scoring_math import clamp, empirical_bayes_rates
from alma.core.sql_helpers import standalone_paper_sql

from .retrieval._common import CHANNEL_BY_FAMILY

CHANNEL_YIELD_VIEW_KEY = "discovery:channel_yield"
CHANNEL_YIELD_VERSION = "2026.09-2"
#: The switch, with the other retrieval strategies in Settings → Discovery.
SETTING_KEY = "strategies.adaptive_channels"


def build_channel_yield(conn: sqlite3.Connection) -> dict[str, Any]:
    """Join surfaced candidates to outcomes; one row per channel."""
    from alma.application.recommendation_outcomes import build_paper_outcome_map

    try:
        rows = conn.execute(
            f"""
            SELECT rc.paper_id, rc.retrieval_hits FROM discovery_ranking_candidates rc
            JOIN papers p ON p.id = rc.paper_id
            WHERE selected = 1 AND {standalone_paper_sql('p')}
            """
        ).fetchall()
    except sqlite3.OperationalError:  # fresh DB: no ranking snapshots yet
        rows = []

    # A paper surfaced by several refreshes is ONE paper per channel.
    channels_by_paper: dict[str, set[str]] = defaultdict(set)
    for row in rows:
        try:
            hits = json.loads(row["retrieval_hits"] or "[]")
        except (TypeError, ValueError):
            continue
        for hit in hits:
            channel = CHANNEL_BY_FAMILY.get(str((hit or {}).get("family") or ""))
            if channel:
                channels_by_paper[str(row["paper_id"])].add(channel)

    outcomes = build_paper_outcome_map(conn)
    counts: dict[str, list[int]] = {channel: [0, 0] for channel in CHANNEL_BY_FAMILY.values()}
    for paper_id, channels in channels_by_paper.items():
        outcome = outcomes.get(paper_id)
        kept = bool(outcome is not None and outcome.classification == "positive")
        for channel in channels:
            counts[channel][1] += 1
            counts[channel][0] += int(kept)

    pooled, strength, shrunk = empirical_bayes_rates({c: (k, n) for c, (k, n) in counts.items()})
    low, high = MULTIPLIER_BAND
    channels: dict[str, dict[str, Any]] = {}
    for channel, (kept, surfaced) in counts.items():
        rate = shrunk.get(channel)
        multiplier = clamp(rate / pooled, low, high) if (rate is not None and pooled > 0 and strength is not None) else 1.0
        channels[channel] = {
            "surfaced": surfaced,
            "kept": kept,
            "rate": round(kept / surfaced, 4) if surfaced else None,
            "shrunk_rate": round(rate, 4) if rate is not None else None,
            "multiplier": round(multiplier, 4),
        }
    return {
        "version": CHANNEL_YIELD_VERSION,
        "papers_surfaced": len(channels_by_paper),
        "pooled_rate": round(pooled, 4),
        # None = the channels do not differ beyond sampling noise: all 1.0.
        "shrinkage_strength": round(strength, 2) if strength is not None else None,
        "channels": channels,
    }


_FINGERPRINT_SQL = with_version(
    f"""
    SELECT (SELECT COUNT(*) FROM discovery_ranking_candidates WHERE selected = 1),
           (SELECT COUNT(*) / 5 FROM papers WHERE {standalone_paper_sql('papers')} AND status = 'library'),
           (SELECT COUNT(*) / 10 FROM feedback_events)
    """,
    CHANNEL_YIELD_VERSION,
)

mv.register(
    mv.View(
        key=CHANNEL_YIELD_VIEW_KEY,
        fingerprint_sql=_FINGERPRINT_SQL,
        build_fn=build_channel_yield,
        operation_key="materialize.discovery.channel_yield",
    )
)


def adaptive_channels_enabled(settings: dict[str, str] | None) -> bool:
    return str((settings or {}).get(SETTING_KEY, "true")).lower() == "true"


def load_channel_yield(conn: sqlite3.Connection) -> dict[str, Any] | None:
    """The stored yield. Pure row read — for request paths."""
    envelope = mv.get_stored(conn, CHANNEL_YIELD_VIEW_KEY)
    return envelope["payload"] if envelope else None


def channel_multipliers(conn: sqlite3.Connection, settings: dict[str, str] | None) -> dict[str, float]:
    """For a refresh job: the current multipliers ({} when switched off).

    ``mv.get`` builds the view the first time and refreshes it in the
    background when its inputs moved, serving the previous one meanwhile.
    """
    if not adaptive_channels_enabled(settings):
        return {}
    payload = mv.get(conn, CHANNEL_YIELD_VIEW_KEY)["payload"]
    return {c: float(v["multiplier"]) for c, v in (payload.get("channels") or {}).items()}


def apply_channel_multipliers(weights: dict[str, float], multipliers: dict[str, float]) -> dict[str, float]:
    """Scale normalised channel weights by their yield and renormalise."""
    scaled = {c: float(w) * float(multipliers.get(c, 1.0)) for c, w in weights.items()}
    total = sum(scaled.values())
    return {c: v / total for c, v in scaled.items()} if total > 0 else dict(weights)
