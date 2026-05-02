"""Project paper feedback into reusable discovery signals.

Paper actions are the most concrete preference events we have. This
module turns them into a small graph of related signals so one liked or
dismissed paper can influence papers, authors, topics, venues, semantic
neighbors, keywords, and tags through the same calibrated value.
"""

from __future__ import annotations

import json
import math
import sqlite3
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Iterable

from alma.core.scoring_math import age_decay, clamp as _shared_clamp


_POSITIVE_ACTIONS = {
    "add",
    "add_to_library",
    "keep",
    "like",
    "liked",
    "love",
    "save",
    "saved",
    "swipe_right",
    "triage_pick",
}
_NEGATIVE_ACTIONS = {
    "dismiss",
    "dismissed",
    "dislike",
    "remove",
    "removed",
    "swipe_left",
}
_POSITION_WEIGHTS = {
    "first": 1.25,
    "last": 1.25,
    "corresponding": 1.35,
}
_EVENT_HALF_LIFE_DAYS = 180.0
_EVENT_MAX_AGE_DAYS = 730.0


@dataclass
class ProjectedPaperSignals:
    """Feedback projected from papers to related discovery dimensions.

    Values are normalized to roughly [-1, 1]. Positive values mean
    "more like this"; negative values mean "less like this".
    """

    paper: dict[str, float] = field(default_factory=dict)
    author: dict[str, float] = field(default_factory=dict)
    author_name: dict[str, float] = field(default_factory=dict)
    topic: dict[str, float] = field(default_factory=dict)
    venue: dict[str, float] = field(default_factory=dict)
    keyword: dict[str, float] = field(default_factory=dict)
    tag: dict[str, float] = field(default_factory=dict)
    semantic_neighbor: dict[str, float] = field(default_factory=dict)
    citation_neighbor: dict[str, float] = field(default_factory=dict)


def normalize_feedback_event_value(event_type: str, raw_value: Any = None) -> float:
    """Return a signed preference value in [-1, 1] for one feedback event."""

    event = str(event_type or "").strip().lower()
    value = _coerce_value(raw_value)

    if isinstance(value, dict):
        signal_value = _float_or_none(value.get("signal_value"))
        if signal_value is not None and signal_value != 0:
            # `record_paper_feedback` stores -1, 0, +1, +2. Divide by 2
            # so a 5-star paper is the strongest positive and 4-star is
            # a moderate positive.
            return _clamp(signal_value / 2.0, -1.0, 1.0)

        rating = _float_or_none(value.get("rating"))
        if rating is not None:
            return _rating_to_signal(rating)

        action = str(value.get("action") or "").strip().lower()
        if action:
            mapped = _action_to_signal(action)
            if mapped != 0.0:
                return mapped

    mapped = _action_to_signal(event)
    if mapped != 0.0:
        return mapped
    return 0.0


def load_projected_paper_signals(
    db: sqlite3.Connection,
    *,
    half_life_days: float = _EVENT_HALF_LIFE_DAYS,
    max_age_days: float = _EVENT_MAX_AGE_DAYS,
) -> ProjectedPaperSignals:
    """Load paper feedback and project it onto adjacent ranking signals.

    Feedback events are the canonical source. Library ratings
    (`papers.rating`) and legacy recommendation actions
    (`recommendations.user_action`) are also folded in at reduced
    weight so the projection layer reflects every per-paper preference
    statement the user has made — not just events emitted after the
    canonical write path landed. The downstream per-paper projections
    (authors / topics / venues / keywords / tags / semantic / citation
    neighbours) fan out automatically; no per-source duplication.
    """

    now = datetime.now(timezone.utc)
    paper_events: dict[str, float] = defaultdict(float)

    try:
        rows = db.execute(
            """
            SELECT entity_id, event_type, value, created_at
            FROM feedback_events
            WHERE entity_type IN ('publication', 'paper')
            ORDER BY created_at DESC
            LIMIT 5000
            """
        ).fetchall()
    except sqlite3.OperationalError:
        rows = []

    for row in rows:
        paper_id = str(row["entity_id"] if isinstance(row, sqlite3.Row) else row[0] or "").strip()
        if not paper_id:
            continue
        signal = normalize_feedback_event_value(
            row["event_type"] if isinstance(row, sqlite3.Row) else row[1],
            row["value"] if isinstance(row, sqlite3.Row) else row[2],
        )
        if signal == 0.0:
            continue
        created_at = row["created_at"] if isinstance(row, sqlite3.Row) else row[3]
        age_days = _days_since(created_at, now)
        if age_days is not None and age_days > max_age_days:
            continue
        decay = age_decay(age_days, half_life_days=half_life_days)
        paper_events[paper_id] += signal * decay

    _add_rating_signals(db, paper_events)
    _add_recommendation_signals(
        db,
        paper_events,
        now=now,
        half_life_days=half_life_days,
        max_age_days=max_age_days,
    )

    out = ProjectedPaperSignals()
    paper_ids = list(paper_events.keys())
    paper_strength = dict(paper_events)

    for paper_id, signal in paper_strength.items():
        _add(out.paper, paper_id, signal)

    if paper_strength:
        _project_authors(db, paper_ids, paper_strength, out)
        _project_topics(db, paper_ids, paper_strength, out)
        _project_venues_keywords(db, paper_ids, paper_strength, out)
        _project_tags(db, paper_ids, paper_strength, out)
        _project_semantic_neighbors(db, paper_ids, paper_strength, out)
        _project_citation_neighbors(db, paper_ids, paper_strength, out)

    _project_author_feedback(db, out)

    out.paper = _squash_map(out.paper, saturation=1.5)
    out.author = _squash_map(out.author, saturation=1.5)
    out.author_name = _squash_map(out.author_name, saturation=1.5)
    out.topic = _squash_map(out.topic, saturation=1.5)
    out.venue = _squash_map(out.venue, saturation=1.5)
    out.keyword = _squash_map(out.keyword, saturation=1.5)
    out.tag = _squash_map(out.tag, saturation=1.5)
    out.semantic_neighbor = _squash_map(out.semantic_neighbor, saturation=1.5)
    out.citation_neighbor = _squash_map(out.citation_neighbor, saturation=1.5)
    return out


_RATING_SIGNAL_WEIGHT = 0.6
_RECOMMENDATION_SIGNAL_WEIGHT = 0.5


def _add_rating_signals(
    db: sqlite3.Connection,
    paper_events: dict[str, float],
) -> None:
    """Fold per-paper Library ratings into ``paper_events``.

    Ratings are a continuous statement (a 5★ paper is *still* a 5★
    paper today), so no time decay is applied. Weight is held below
    1.0 to avoid double-counting when the same paper also has an
    explicit `paper_action` event with the same rating.
    """
    try:
        rows = db.execute(
            "SELECT id, rating FROM papers WHERE rating IS NOT NULL AND rating > 0"
        ).fetchall()
    except sqlite3.OperationalError:
        return
    for row in rows:
        paper_id = str(row["id"] if isinstance(row, sqlite3.Row) else row[0] or "").strip()
        if not paper_id:
            continue
        rating = _float_or_none(row["rating"] if isinstance(row, sqlite3.Row) else row[1])
        if rating is None:
            continue
        signal = _rating_to_signal(rating)
        if signal == 0.0:
            continue
        paper_events[paper_id] += signal * _RATING_SIGNAL_WEIGHT


def _add_recommendation_signals(
    db: sqlite3.Connection,
    paper_events: dict[str, float],
    *,
    now: datetime,
    half_life_days: float,
    max_age_days: float,
) -> None:
    """Fold legacy ``recommendations.user_action`` history into ``paper_events``.

    Pre-`feedback_events` recommendation feedback lives in this
    table. It carries the same signed actions (`save`/`like`/`dismiss`)
    and an `action_at` timestamp, so we apply the same decay window
    used by `feedback_events`. Weight is the lowest of the three
    sources because some rows here have already been replayed into
    `feedback_events` by newer write paths.
    """
    try:
        rows = db.execute(
            """
            SELECT paper_id, user_action, action_at
            FROM recommendations
            WHERE user_action IS NOT NULL AND TRIM(user_action) <> ''
            """
        ).fetchall()
    except sqlite3.OperationalError:
        return
    for row in rows:
        paper_id = str(row["paper_id"] if isinstance(row, sqlite3.Row) else row[0] or "").strip()
        if not paper_id:
            continue
        action = row["user_action"] if isinstance(row, sqlite3.Row) else row[1]
        signal = _action_to_signal(str(action or ""))
        if signal == 0.0:
            continue
        action_at = row["action_at"] if isinstance(row, sqlite3.Row) else row[2]
        age_days = _days_since(action_at, now)
        if age_days is not None and age_days > max_age_days:
            continue
        decay = age_decay(age_days, half_life_days=half_life_days)
        paper_events[paper_id] += signal * decay * _RECOMMENDATION_SIGNAL_WEIGHT


def _project_authors(
    db: sqlite3.Connection,
    paper_ids: list[str],
    paper_strength: dict[str, float],
    out: ProjectedPaperSignals,
) -> None:
    try:
        for chunk in _chunks(paper_ids, 500):
            placeholders = ",".join("?" for _ in chunk)
            rows = db.execute(
                f"""
                SELECT
                    paper_id,
                    lower(trim(openalex_id)) AS openalex_id,
                    lower(trim(display_name)) AS display_name,
                    lower(trim(position)) AS position,
                    COALESCE(is_corresponding, 0) AS is_corresponding,
                    COUNT(*) OVER (PARTITION BY paper_id) AS author_count
                FROM publication_authors
                WHERE paper_id IN ({placeholders})
                  AND (
                    COALESCE(TRIM(openalex_id), '') <> ''
                    OR COALESCE(TRIM(display_name), '') <> ''
                  )
                """,
                chunk,
            ).fetchall()
            for row in rows:
                paper_id = str(row["paper_id"])
                signal = paper_strength.get(paper_id, 0.0)
                if signal == 0.0:
                    continue
                author_count = max(1, int(row["author_count"] or 1))
                position = str(row["position"] or "")
                pos_weight = _POSITION_WEIGHTS.get(position, 1.0)
                if int(row["is_corresponding"] or 0):
                    pos_weight = max(pos_weight, _POSITION_WEIGHTS["corresponding"])
                contribution = signal * 0.70 * pos_weight / math.sqrt(author_count)
                openalex_id = str(row["openalex_id"] or "").strip()
                display_name = str(row["display_name"] or "").strip()
                if openalex_id:
                    _add(out.author, openalex_id, contribution)
                if display_name:
                    _add(out.author_name, display_name, contribution)
    except sqlite3.OperationalError:
        return


def _project_topics(
    db: sqlite3.Connection,
    paper_ids: list[str],
    paper_strength: dict[str, float],
    out: ProjectedPaperSignals,
) -> None:
    try:
        for chunk in _chunks(paper_ids, 500):
            placeholders = ",".join("?" for _ in chunk)
            rows = db.execute(
                f"""
                SELECT paper_id, lower(trim(term)) AS term, COALESCE(score, 0.5) AS score
                FROM publication_topics
                WHERE paper_id IN ({placeholders}) AND COALESCE(TRIM(term), '') <> ''
                """,
                chunk,
            ).fetchall()
            for row in rows:
                term = str(row["term"] or "").strip()
                if not term:
                    continue
                score = _clamp(float(row["score"] or 0.5), 0.1, 1.0)
                _add(out.topic, term, paper_strength.get(str(row["paper_id"]), 0.0) * 0.65 * score)
    except sqlite3.OperationalError:
        return


def _project_venues_keywords(
    db: sqlite3.Connection,
    paper_ids: list[str],
    paper_strength: dict[str, float],
    out: ProjectedPaperSignals,
) -> None:
    try:
        for chunk in _chunks(paper_ids, 500):
            placeholders = ",".join("?" for _ in chunk)
            rows = db.execute(
                f"""
                SELECT id, lower(trim(journal)) AS journal, keywords
                FROM papers
                WHERE id IN ({placeholders})
                """,
                chunk,
            ).fetchall()
            for row in rows:
                paper_id = str(row["id"])
                signal = paper_strength.get(paper_id, 0.0)
                if signal == 0.0:
                    continue
                journal = str(row["journal"] or "").strip()
                if journal:
                    _add(out.venue, journal, signal * 0.45)
                for keyword in _parse_keywords(row["keywords"]):
                    _add(out.keyword, keyword, signal * 0.45)
    except sqlite3.OperationalError:
        return


def _project_tags(
    db: sqlite3.Connection,
    paper_ids: list[str],
    paper_strength: dict[str, float],
    out: ProjectedPaperSignals,
) -> None:
    try:
        for chunk in _chunks(paper_ids, 500):
            placeholders = ",".join("?" for _ in chunk)
            rows = db.execute(
                f"""
                SELECT pt.paper_id, lower(trim(t.name)) AS name
                FROM publication_tags pt
                JOIN tags t ON t.id = pt.tag_id
                WHERE pt.paper_id IN ({placeholders})
                  AND COALESCE(TRIM(t.name), '') <> ''
                """,
                chunk,
            ).fetchall()
            for row in rows:
                tag = str(row["name"] or "").strip()
                if tag:
                    _add(out.tag, tag, paper_strength.get(str(row["paper_id"]), 0.0) * 0.75)
    except sqlite3.OperationalError:
        return


def _project_semantic_neighbors(
    db: sqlite3.Connection,
    paper_ids: list[str],
    paper_strength: dict[str, float],
    out: ProjectedPaperSignals,
    *,
    min_similarity: float = 0.68,
    top_k: int = 24,
) -> None:
    """Project paper feedback to close embedding neighbours only."""

    try:
        import numpy as np
    except ImportError:
        return
    try:
        from alma.core.vector_blob import decode_vector
        from alma.discovery.similarity import get_active_embedding_model

        model = get_active_embedding_model(db)
        seed_placeholders = ",".join("?" for _ in paper_ids)
        seed_rows = db.execute(
            f"""
            SELECT paper_id, embedding
            FROM publication_embeddings
            WHERE model = ? AND paper_id IN ({seed_placeholders})
            """,
            (model, *paper_ids),
        ).fetchall()
        if not seed_rows:
            return
        all_rows = db.execute(
            """
            SELECT paper_id, embedding
            FROM publication_embeddings
            WHERE model = ?
            """,
            (model,),
        ).fetchall()
    except (sqlite3.OperationalError, ValueError):
        return

    seeds: list[tuple[str, Any]] = []
    for row in seed_rows:
        paper_id = str(row["paper_id"] or "")
        signal = paper_strength.get(paper_id, 0.0)
        if signal == 0.0:
            continue
        vec = decode_vector(row["embedding"])
        norm = float(np.linalg.norm(vec))
        if norm > 0.0:
            seeds.append((paper_id, vec / norm))
    if not seeds:
        return

    candidates: list[tuple[str, Any]] = []
    for row in all_rows:
        paper_id = str(row["paper_id"] or "")
        if not paper_id or paper_id in paper_strength:
            continue
        vec = decode_vector(row["embedding"])
        norm = float(np.linalg.norm(vec))
        if norm > 0.0:
            candidates.append((paper_id, vec / norm))
    if not candidates:
        return

    for seed_id, seed_vec in seeds:
        signal = paper_strength.get(seed_id, 0.0)
        scored: list[tuple[float, str]] = []
        for candidate_id, candidate_vec in candidates:
            similarity = float(np.dot(seed_vec, candidate_vec))
            if similarity >= min_similarity:
                scored.append((similarity, candidate_id))
        scored.sort(reverse=True)
        for similarity, candidate_id in scored[:top_k]:
            # Square the [0,1] similarity so only genuinely close
            # neighbours inherit meaningful preference signal.
            normalized = _clamp((similarity + 1.0) / 2.0, 0.0, 1.0)
            _add(out.semantic_neighbor, candidate_id, signal * 0.40 * normalized * normalized)


def _project_citation_neighbors(
    db: sqlite3.Connection,
    paper_ids: list[str],
    paper_strength: dict[str, float],
    out: ProjectedPaperSignals,
) -> None:
    """Project paper feedback through local outgoing and incoming citations."""

    try:
        for chunk in _chunks(paper_ids, 500):
            placeholders = ",".join("?" for _ in chunk)
            outgoing_rows = db.execute(
                f"""
                SELECT pr.paper_id AS seed_id, rp.id AS neighbour_id
                FROM publication_references pr
                JOIN papers rp
                  ON lower(trim(rp.openalex_id)) = lower('W' || pr.referenced_work_id)
                  OR lower(trim(rp.openalex_id)) = lower(CAST(pr.referenced_work_id AS TEXT))
                WHERE pr.paper_id IN ({placeholders})
                  AND rp.id <> pr.paper_id
                """,
                chunk,
            ).fetchall()
            for row in outgoing_rows:
                _add(
                    out.citation_neighbor,
                    str(row["neighbour_id"]),
                    paper_strength.get(str(row["seed_id"]), 0.0) * 0.35,
                )

            incoming_rows = db.execute(
                f"""
                SELECT fp.id AS seed_id, cp.id AS neighbour_id
                FROM papers fp
                JOIN publication_references pr
                  ON lower('W' || pr.referenced_work_id) = lower(trim(fp.openalex_id))
                  OR lower(CAST(pr.referenced_work_id AS TEXT)) = lower(trim(fp.openalex_id))
                JOIN papers cp ON cp.id = pr.paper_id
                WHERE fp.id IN ({placeholders})
                  AND cp.id <> fp.id
                """,
                chunk,
            ).fetchall()
            for row in incoming_rows:
                _add(
                    out.citation_neighbor,
                    str(row["neighbour_id"]),
                    paper_strength.get(str(row["seed_id"]), 0.0) * 0.30,
                )
    except sqlite3.OperationalError:
        return


def _project_author_feedback(db: sqlite3.Connection, out: ProjectedPaperSignals) -> None:
    author_signals: dict[str, float] = defaultdict(float)

    try:
        rows = db.execute(
            """
            SELECT lower(trim(a.openalex_id)) AS openalex_id, lower(trim(a.name)) AS name
            FROM followed_authors fa
            JOIN authors a ON a.id = fa.author_id
            WHERE COALESCE(TRIM(a.openalex_id), '') <> ''
            """
        ).fetchall()
        for row in rows:
            oid = str(row["openalex_id"] or "").strip()
            if not oid:
                continue
            author_signals[oid] += 0.75
            _add(out.author, oid, 0.75)
            _add(out.author_name, str(row["name"] or ""), 0.45)
    except sqlite3.OperationalError:
        pass

    try:
        rows = db.execute(
            """
            SELECT lower(trim(openalex_id)) AS openalex_id, signal_value, created_at
            FROM missing_author_feedback
            WHERE COALESCE(TRIM(openalex_id), '') <> ''
            ORDER BY created_at DESC
            LIMIT 1000
            """
        ).fetchall()
    except sqlite3.OperationalError:
        rows = []

    now = datetime.now(timezone.utc)
    for row in rows:
        oid = str(row["openalex_id"] or "").strip()
        if not oid:
            continue
        signal = _float_or_none(row["signal_value"])
        if signal is None or signal == 0.0:
            continue
        age_days = _days_since(row["created_at"], now)
        decay = age_decay(age_days, half_life_days=120.0)
        contribution = _clamp(signal, -1.0, 1.0) * decay
        author_signals[oid] += contribution
        _add(out.author, oid, contribution)

    if author_signals:
        author_signal_map = dict(author_signals)
        _project_author_profiles(db, author_signal_map, out)
        _project_author_coauthors(db, author_signal_map, out)
        _project_author_institutions(db, author_signal_map, out)


def _project_author_profiles(
    db: sqlite3.Connection,
    author_signals: dict[str, float],
    out: ProjectedPaperSignals,
) -> None:
    author_ids = [oid for oid, signal in author_signals.items() if oid and signal != 0.0]
    if not author_ids:
        return

    try:
        for chunk in _chunks(author_ids, 300):
            placeholders = ",".join("?" for _ in chunk)
            topic_rows = db.execute(
                f"""
                SELECT lower(trim(pa.openalex_id)) AS openalex_id,
                       lower(trim(pt.term)) AS term,
                       COALESCE(pt.score, 0.5) AS score
                FROM publication_authors pa
                JOIN publication_topics pt ON pt.paper_id = pa.paper_id
                WHERE lower(trim(pa.openalex_id)) IN ({placeholders})
                  AND COALESCE(TRIM(pt.term), '') <> ''
                """,
                chunk,
            ).fetchall()
            for row in topic_rows:
                signal = author_signals.get(str(row["openalex_id"] or ""), 0.0)
                term = str(row["term"] or "").strip()
                if term:
                    _add(out.topic, term, signal * 0.25 * _clamp(float(row["score"] or 0.5), 0.1, 1.0))

            venue_rows = db.execute(
                f"""
                SELECT lower(trim(pa.openalex_id)) AS openalex_id,
                       lower(trim(p.journal)) AS venue,
                       p.keywords AS keywords
                FROM publication_authors pa
                JOIN papers p ON p.id = pa.paper_id
                WHERE lower(trim(pa.openalex_id)) IN ({placeholders})
                """,
                chunk,
            ).fetchall()
            for row in venue_rows:
                signal = author_signals.get(str(row["openalex_id"] or ""), 0.0)
                venue = str(row["venue"] or "").strip()
                if venue:
                    _add(out.venue, venue, signal * 0.18)
                for keyword in _parse_keywords(row["keywords"]):
                    _add(out.keyword, keyword, signal * 0.16)

            tag_rows = db.execute(
                f"""
                SELECT lower(trim(pa.openalex_id)) AS openalex_id,
                       lower(trim(t.name)) AS tag_name
                FROM publication_authors pa
                JOIN publication_tags pt ON pt.paper_id = pa.paper_id
                JOIN tags t ON t.id = pt.tag_id
                WHERE lower(trim(pa.openalex_id)) IN ({placeholders})
                  AND COALESCE(TRIM(t.name), '') <> ''
                """,
                chunk,
            ).fetchall()
            for row in tag_rows:
                signal = author_signals.get(str(row["openalex_id"] or ""), 0.0)
                _add(out.tag, str(row["tag_name"] or ""), signal * 0.22)
    except sqlite3.OperationalError:
        return


def _project_author_coauthors(
    db: sqlite3.Connection,
    author_signals: dict[str, float],
    out: ProjectedPaperSignals,
) -> None:
    """Spill author signal to direct coauthors of followed / rejected authors.

    A followed author's frequent collaborators are weak preference
    candidates; a rejected author's collaborators are weak negatives.
    Per-paper damping by ``1 / sqrt(author_count)`` mirrors the seed
    paper-event projection so a 50-author consortium paper can't flood
    the graph. The overall weight is intentionally below the direct
    follow / reject signal — coauthorship is correlation, not the
    same statement.
    """
    seed_ids = [oid for oid, signal in author_signals.items() if oid and signal != 0.0]
    if not seed_ids:
        return
    try:
        for chunk in _chunks(seed_ids, 300):
            placeholders = ",".join("?" for _ in chunk)
            rows = db.execute(
                f"""
                SELECT
                    lower(trim(seed.openalex_id)) AS seed_id,
                    lower(trim(other.openalex_id)) AS coauthor_id,
                    lower(trim(other.display_name)) AS coauthor_name,
                    COUNT(*) OVER (PARTITION BY seed.paper_id) AS author_count
                FROM publication_authors seed
                JOIN publication_authors other
                  ON other.paper_id = seed.paper_id
                 AND lower(trim(other.openalex_id)) <> lower(trim(seed.openalex_id))
                WHERE lower(trim(seed.openalex_id)) IN ({placeholders})
                  AND COALESCE(TRIM(other.openalex_id), '') <> ''
                """,
                chunk,
            ).fetchall()
            for row in rows:
                signal = author_signals.get(str(row["seed_id"] or ""), 0.0)
                if signal == 0.0:
                    continue
                author_count = max(1, int(row["author_count"] or 1))
                contribution = signal * 0.20 / math.sqrt(author_count)
                coauthor_id = str(row["coauthor_id"] or "").strip()
                if coauthor_id:
                    _add(out.author, coauthor_id, contribution)
                coauthor_name = str(row["coauthor_name"] or "").strip()
                if coauthor_name:
                    _add(out.author_name, coauthor_name, contribution * 0.8)
    except sqlite3.OperationalError:
        return


def _project_author_institutions(
    db: sqlite3.Connection,
    author_signals: dict[str, float],
    out: ProjectedPaperSignals,
    *,
    max_institution_size: int = 400,
) -> None:
    """Spill author signal to other authors at the same institutions.

    A followed author's home institution is a weak prior — researchers
    at the same lab are slightly more likely to be relevant. Cap by
    institution size so a "Harvard" or "MIT" affiliation doesn't flood
    the graph. Mega-institutions above ``max_institution_size`` are
    skipped because the signal-to-noise is too low.
    """
    seed_ids = [oid for oid, signal in author_signals.items() if oid and signal != 0.0]
    if not seed_ids:
        return
    try:
        institutions: dict[str, float] = defaultdict(float)
        for chunk in _chunks(seed_ids, 300):
            placeholders = ",".join("?" for _ in chunk)
            seed_rows = db.execute(
                f"""
                SELECT
                    lower(trim(openalex_id)) AS seed_id,
                    lower(trim(institution)) AS institution
                FROM publication_authors
                WHERE lower(trim(openalex_id)) IN ({placeholders})
                  AND COALESCE(TRIM(institution), '') <> ''
                """,
                chunk,
            ).fetchall()
            for row in seed_rows:
                signal = author_signals.get(str(row["seed_id"] or ""), 0.0)
                if signal == 0.0:
                    continue
                inst = str(row["institution"] or "").strip()
                if inst:
                    # Strongest seed signal wins per institution to
                    # avoid one repeat-affiliated author dominating the
                    # institution prior.
                    if abs(signal) > abs(institutions.get(inst, 0.0)):
                        institutions[inst] = signal

        if not institutions:
            return

        inst_keys = list(institutions.keys())
        for chunk in _chunks(inst_keys, 200):
            placeholders = ",".join("?" for _ in chunk)
            rows = db.execute(
                f"""
                SELECT
                    lower(trim(institution)) AS institution,
                    lower(trim(openalex_id)) AS openalex_id,
                    COUNT(*) OVER (PARTITION BY lower(trim(institution))) AS inst_size
                FROM publication_authors
                WHERE lower(trim(institution)) IN ({placeholders})
                  AND COALESCE(TRIM(openalex_id), '') <> ''
                """,
                chunk,
            ).fetchall()
            for row in rows:
                inst = str(row["institution"] or "").strip()
                if not inst:
                    continue
                inst_size = max(1, int(row["inst_size"] or 1))
                if inst_size > max_institution_size:
                    continue
                signal = institutions.get(inst, 0.0)
                if signal == 0.0:
                    continue
                # 0.06 weight × 1/sqrt(inst_size) damping. A 1-person
                # institution becomes a strong locality prior; a 100-
                # person one becomes a faint nudge.
                contribution = signal * 0.06 / math.sqrt(inst_size)
                coauthor_id = str(row["openalex_id"] or "").strip()
                if coauthor_id:
                    _add(out.author, coauthor_id, contribution)
    except sqlite3.OperationalError:
        return


def _coerce_value(raw_value: Any) -> Any:
    if isinstance(raw_value, (dict, list)):
        return raw_value
    if raw_value is None:
        return None
    if isinstance(raw_value, bytes):
        raw_value = raw_value.decode("utf-8", errors="replace")
    if isinstance(raw_value, str):
        text = raw_value.strip()
        if not text:
            return None
        try:
            return json.loads(text)
        except json.JSONDecodeError:
            return text
    return raw_value


def _rating_to_signal(rating: float) -> float:
    if rating >= 5:
        return 1.0
    if rating >= 4:
        return 0.5
    if rating <= 2 and rating > 0:
        return -0.5
    return 0.0


def _action_to_signal(action: str) -> float:
    normalized = str(action or "").strip().lower()
    if normalized in _POSITIVE_ACTIONS:
        return 0.7
    if normalized in _NEGATIVE_ACTIONS:
        return -0.7
    return 0.0


def _float_or_none(value: Any) -> float | None:
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _days_since(raw: Any, now: datetime) -> float | None:
    if not raw:
        return None
    text = str(raw).strip()
    if not text:
        return None
    if text.endswith("Z"):
        text = f"{text[:-1]}+00:00"
    try:
        dt = datetime.fromisoformat(text)
    except ValueError:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return max(0.0, (now - dt.astimezone(timezone.utc)).total_seconds() / 86400.0)


def _parse_keywords(raw: Any) -> list[str]:
    value = _coerce_value(raw)
    if isinstance(value, list):
        items = value
    elif isinstance(value, str):
        items = value.replace(";", ",").split(",")
    else:
        items = []
    out: list[str] = []
    for item in items:
        text = str(item or "").strip().lower()
        if text:
            out.append(text)
    return out


def _add(target: dict[str, float], key: str, value: float) -> None:
    normalized = str(key or "").strip().lower()
    if normalized and value != 0.0:
        target[normalized] = target.get(normalized, 0.0) + value


def _squash_map(values: dict[str, float], *, saturation: float) -> dict[str, float]:
    if not values:
        return {}
    return {key: math.tanh(value / saturation) for key, value in values.items()}


def _chunks(values: list[str], size: int) -> Iterable[list[str]]:
    for idx in range(0, len(values), size):
        yield values[idx : idx + size]


def _clamp(value: float, lo: float, hi: float) -> float:
    return _shared_clamp(value, lo, hi)
