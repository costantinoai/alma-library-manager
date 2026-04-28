"""Composite paper-signal score — blends every local signal we have.

Used anywhere we need a single "how strong is this paper as a signal
right now" number. Today that's seed selection for the D12 network
author-suggestion buckets; the same helper is reusable from Discovery
lens seeding and Signal Lab priority queueing.

Design rationale (2026-04-24):
  - Rating alone is too discrete (five buckets) and misses every
    paper a user hasn't bothered to rate yet. The composite lifts
    unrated papers that still look strong on topic / embedding /
    author signals.
  - Every component is independent and normalized to [0, 1]. When a
    component is missing (no vector, unrated, no publication date)
    its weight is redistributed proportionally to present components
    — so an unrated paper with a good embedding + strong topic
    alignment still scores well, rather than being zeroed out.
  - Weights live in `DISCOVERY_SETTINGS_DEFAULTS` under
    `paper_signal_weights.*` so they're tunable without a code
    change.

Signals blended:
  rating          — (rating - 3) / 2 clamped to [0, 1]; 0 if unrated
  topic_alignment — mean of library topic-weights over paper's topics
  embedding_sim   — cos(paper_vec, library_centroid), [-1,1]→[0,1]
  author_alignment— max cos(author_centroid, library_centroid) over
                    this paper's authors that have cached centroids
  signal_lab      — net decayed positive feedback events on the paper
  recency         — half-life decay over publication_date (2yr)
"""

from __future__ import annotations

import logging
import math
import sqlite3
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Optional

logger = logging.getLogger(__name__)


# -- tunable defaults -------------------------------------------------
# These are just fallbacks used when the settings table is empty; the
# live source of truth is always `DISCOVERY_SETTINGS_DEFAULTS` merged
# with user overrides.
_COMPONENT_NAMES: tuple[str, ...] = (
    "rating",
    "topic_alignment",
    "embedding_sim",
    "author_alignment",
    "signal_lab",
    "recency",
)

_RECENCY_HALF_LIFE_DAYS = 730.0  # ~2 years
_SIGNAL_LAB_DECAY_HALF_LIFE_DAYS = 180.0
_SIGNAL_LAB_SATURATION = 5.0  # net events at which signal_lab = 1.0
_POSITIVE_EVENTS = {"love", "like", "add", "reaction_positive", "swipe_right", "triage_pick"}
_NEGATIVE_EVENTS = {"dislike", "dismiss", "remove", "reaction_negative", "swipe_left"}


# -- library state (shared across a batch of papers) ------------------

@dataclass
class LibraryState:
    """Snapshot of library-level features used by `score_papers_batch`.

    Recomputed once per "refresh pass" — not per paper. Carrying a
    numpy array in a dataclass is fine because we only hold one
    reference per scoring call.
    """

    model: str = ""
    centroid: Optional["object"] = None  # numpy.ndarray | None
    topic_weights: dict[str, float] = field(default_factory=dict)
    author_centroid_sim: dict[str, float] = field(
        default_factory=dict
    )  # openalex_id (lowercased) → cos(centroid, lib_centroid)

    def has_embeddings(self) -> bool:
        return self.centroid is not None

    def has_topics(self) -> bool:
        return bool(self.topic_weights)

    def has_author_centroids(self) -> bool:
        return bool(self.author_centroid_sim)


def load_library_state(db: sqlite3.Connection) -> LibraryState:
    """Compute the library-level snapshot once per refresh pass."""

    state = LibraryState()
    try:
        from alma.discovery.similarity import get_active_embedding_model

        state.model = get_active_embedding_model(db) or ""
    except Exception:
        logger.debug("active embedding model unavailable", exc_info=True)
        state.model = ""

    state.centroid = _load_library_centroid(db, state.model)
    state.topic_weights = _load_library_topic_weights(db)

    if state.centroid is not None:
        state.author_centroid_sim = _load_author_centroid_similarities(
            db, state.model, state.centroid
        )
    return state


def _load_library_centroid(db: sqlite3.Connection, model: str):
    """Mean active-model SPECTER2 vector over Library papers."""

    if not model:
        return None
    try:
        import numpy as np
    except ImportError:
        return None
    try:
        rows = db.execute(
            """
            SELECT pe.embedding AS embedding
            FROM publication_embeddings pe
            JOIN papers p ON p.id = pe.paper_id
            WHERE p.status = 'library' AND pe.model = ?
            LIMIT 2000
            """,
            (model,),
        ).fetchall()
    except sqlite3.OperationalError:
        return None
    from alma.core.vector_blob import decode_vector

    vectors = [
        decode_vector(row["embedding"])
        for row in rows
        if row["embedding"]
    ]
    if not vectors:
        return None
    centroid = np.mean(np.stack(vectors), axis=0)
    norm = float(np.linalg.norm(centroid))
    if norm <= 0.0:
        return None
    return (centroid / norm).astype(np.float32)


def _load_library_topic_weights(db: sqlite3.Connection) -> dict[str, float]:
    """L1-normalized `{term: weight}` over Library papers' topics."""

    try:
        rows = db.execute(
            """
            SELECT pt.term AS term, SUM(COALESCE(pt.score, 0)) AS w
            FROM publication_topics pt
            JOIN papers p ON p.id = pt.paper_id
            WHERE p.status = 'library' AND COALESCE(pt.term, '') <> ''
            GROUP BY pt.term
            """
        ).fetchall()
    except sqlite3.OperationalError:
        return {}
    raw: dict[str, float] = {}
    total = 0.0
    for row in rows:
        term = str(row["term"] or "").strip().lower()
        weight = float(row["w"] or 0.0)
        if not term or weight <= 0.0:
            continue
        raw[term] = weight
        total += weight
    if total <= 0.0:
        return {}
    return {term: w / total for term, w in raw.items()}


def _load_author_centroid_similarities(
    db: sqlite3.Connection,
    model: str,
    lib_centroid,
) -> dict[str, float]:
    """Pre-compute cos(author_centroid, lib_centroid) for every cached author.

    Reads `author_centroids` (populated by the backfill job) and
    returns the cosine similarity so the per-paper hot loop only does
    dictionary lookups.
    """

    try:
        import numpy as np
    except ImportError:
        return {}
    try:
        rows = db.execute(
            """
            SELECT author_openalex_id AS oid, centroid_blob AS blob
            FROM author_centroids
            WHERE model = ?
            """,
            (model,),
        ).fetchall()
    except sqlite3.OperationalError:
        return {}
    from alma.core.vector_blob import decode_vector

    out: dict[str, float] = {}
    for row in rows:
        oid = str(row["oid"] or "").strip().lower()
        blob = row["blob"]
        if not oid or not blob:
            continue
        vec = decode_vector(blob)
        norm = float(np.linalg.norm(vec))
        if norm <= 0.0:
            continue
        vec = vec / norm
        sim = float(np.dot(lib_centroid, vec))  # both unit-normed
        out[oid] = (sim + 1.0) / 2.0  # map [-1, 1] → [0, 1]
    return out


# -- scoring ----------------------------------------------------------

def _resolve_weights(db: sqlite3.Connection) -> dict[str, float]:
    """Read `paper_signal_weights.*` from discovery_settings."""

    from alma.discovery.defaults import merge_discovery_defaults

    try:
        settings_rows = db.execute(
            "SELECT key, value FROM discovery_settings"
        ).fetchall()
        stored = {row["key"]: row["value"] for row in settings_rows}
    except sqlite3.OperationalError:
        stored = {}
    merged = merge_discovery_defaults(stored)
    out: dict[str, float] = {}
    for name in _COMPONENT_NAMES:
        raw = merged.get(f"paper_signal_weights.{name}", "0.0")
        try:
            out[name] = max(0.0, float(raw))
        except (TypeError, ValueError):
            out[name] = 0.0
    return out


def _redistribute(
    component_scores: dict[str, float],
    present: dict[str, bool],
    weights: dict[str, float],
) -> float:
    """Sum component_score × weight, redistributing missing weight.

    If a component is missing (`present[name]` is False), its weight
    is reallocated proportionally across the present components. This
    preserves a meaningful score when (e.g.) a paper has no vector:
    the other signals still contribute up to their full fraction of
    the final number.
    """

    present_weight = sum(weights[n] for n in _COMPONENT_NAMES if present.get(n))
    missing_weight = sum(weights[n] for n in _COMPONENT_NAMES if not present.get(n))
    if present_weight <= 0.0:
        return 0.0
    bonus_factor = 1.0 + (missing_weight / present_weight)
    score = 0.0
    for name in _COMPONENT_NAMES:
        if not present.get(name):
            continue
        score += component_scores[name] * weights[name] * bonus_factor
    return max(0.0, min(1.0, score))


def score_papers_batch(
    db: sqlite3.Connection,
    paper_ids: list[str],
    state: Optional[LibraryState] = None,
    *,
    now: Optional[datetime] = None,
) -> dict[str, float]:
    """Composite paper-signal score for each id in `paper_ids`.

    Returns a dict `{paper_id: score in [0, 1]}`. Papers not found in
    the DB are silently dropped from the returned map (caller can
    default missing entries to 0).

    Batched to avoid N+1 DB round trips: one query per data source
    (papers, embeddings, topics, authors, feedback_events), joined in
    Python by paper_id.
    """

    if not paper_ids:
        return {}
    if state is None:
        state = load_library_state(db)
    if now is None:
        now = datetime.now(timezone.utc)
    weights = _resolve_weights(db)

    try:
        import numpy as np
    except ImportError:
        np = None  # type: ignore[assignment]

    placeholders = ",".join("?" * len(paper_ids))

    # --- papers: rating + publication_date ---------------------------
    try:
        rows = db.execute(
            f"""
            SELECT id, rating, publication_date
            FROM papers
            WHERE id IN ({placeholders})
            """,
            paper_ids,
        ).fetchall()
    except sqlite3.OperationalError:
        return {}
    paper_meta: dict[str, dict] = {}
    for row in rows:
        paper_meta[str(row["id"])] = {
            "rating": int(row["rating"] or 0),
            "publication_date": str(row["publication_date"] or "") or None,
        }
    if not paper_meta:
        return {}

    # --- embeddings --------------------------------------------------
    embed_sim: dict[str, float] = {}
    if state.has_embeddings() and np is not None:
        try:
            emb_rows = db.execute(
                f"""
                SELECT paper_id, embedding
                FROM publication_embeddings
                WHERE model = ? AND paper_id IN ({placeholders})
                """,
                (state.model, *paper_ids),
            ).fetchall()
        except sqlite3.OperationalError:
            emb_rows = []
        for row in emb_rows:
            blob = row["embedding"]
            if not blob:
                continue
            vec = np.frombuffer(blob, dtype=np.float32)
            norm = float(np.linalg.norm(vec))
            if norm <= 0.0:
                continue
            vec = vec / norm
            sim = float(np.dot(state.centroid, vec))
            embed_sim[str(row["paper_id"])] = (sim + 1.0) / 2.0

    # --- topic alignment ---------------------------------------------
    topic_align: dict[str, float] = {}
    if state.has_topics():
        try:
            topic_rows = db.execute(
                f"""
                SELECT paper_id, term, COALESCE(score, 0) AS score
                FROM publication_topics
                WHERE paper_id IN ({placeholders})
                """,
                paper_ids,
            ).fetchall()
        except sqlite3.OperationalError:
            topic_rows = []
        by_paper: dict[str, list[tuple[str, float]]] = {}
        for row in topic_rows:
            pid = str(row["paper_id"])
            term = str(row["term"] or "").strip().lower()
            if not term:
                continue
            by_paper.setdefault(pid, []).append((term, float(row["score"] or 0.0)))
        for pid, pairs in by_paper.items():
            # Weighted mean of library_weights[term] using each topic's
            # own score as the weighting — a topic the paper is tagged
            # with at score 0.9 counts more than one at 0.2.
            total_w = sum(max(0.0, score) for _, score in pairs)
            if total_w <= 0.0:
                continue
            num = 0.0
            for term, score in pairs:
                num += state.topic_weights.get(term, 0.0) * max(0.0, score)
            alignment = num / total_w
            # Library-weight values are probabilities (they sum to 1),
            # so alignment is typically small. Scale into a usable
            # range by comparing to the top-percentile weight.
            top_ref = max(state.topic_weights.values(), default=0.0) or 1.0
            topic_align[pid] = max(0.0, min(1.0, alignment / top_ref))

    # --- author alignment --------------------------------------------
    author_align: dict[str, float] = {}
    if state.has_author_centroids():
        try:
            pa_rows = db.execute(
                f"""
                SELECT paper_id, lower(trim(openalex_id)) AS oid
                FROM publication_authors
                WHERE paper_id IN ({placeholders})
                  AND COALESCE(TRIM(openalex_id), '') <> ''
                """,
                paper_ids,
            ).fetchall()
        except sqlite3.OperationalError:
            pa_rows = []
        for row in pa_rows:
            pid = str(row["paper_id"])
            oid = str(row["oid"] or "")
            sim = state.author_centroid_sim.get(oid)
            if sim is None:
                continue
            prev = author_align.get(pid, 0.0)
            if sim > prev:
                author_align[pid] = sim

    # --- signal-lab (feedback_events) --------------------------------
    sig_lab: dict[str, float] = {}
    try:
        fe_rows = db.execute(
            f"""
            SELECT entity_id, event_type, created_at
            FROM feedback_events
            WHERE entity_type = 'paper' AND entity_id IN ({placeholders})
            """,
            paper_ids,
        ).fetchall()
    except sqlite3.OperationalError:
        fe_rows = []
    for row in fe_rows:
        ev = str(row["event_type"] or "").strip().lower()
        polarity = (
            1.0 if ev in _POSITIVE_EVENTS
            else -1.0 if ev in _NEGATIVE_EVENTS
            else 0.0
        )
        if polarity == 0.0:
            continue
        age_days = _days_since(row["created_at"], now)
        decay = (
            math.pow(0.5, age_days / _SIGNAL_LAB_DECAY_HALF_LIFE_DAYS)
            if age_days is not None
            else 1.0
        )
        sig_lab[str(row["entity_id"])] = (
            sig_lab.get(str(row["entity_id"]), 0.0) + polarity * decay
        )

    # --- assemble per-paper -----------------------------------------
    results: dict[str, float] = {}
    for pid, meta in paper_meta.items():
        components: dict[str, float] = {n: 0.0 for n in _COMPONENT_NAMES}
        present: dict[str, bool] = {n: False for n in _COMPONENT_NAMES}

        rating = meta["rating"]
        if rating > 0:
            components["rating"] = max(0.0, min(1.0, (rating - 3.0) / 2.0))
            present["rating"] = True

        if pid in topic_align:
            components["topic_alignment"] = topic_align[pid]
            present["topic_alignment"] = True

        if pid in embed_sim:
            components["embedding_sim"] = embed_sim[pid]
            present["embedding_sim"] = True

        if pid in author_align:
            components["author_alignment"] = author_align[pid]
            present["author_alignment"] = True

        if pid in sig_lab:
            raw = sig_lab[pid] / _SIGNAL_LAB_SATURATION
            components["signal_lab"] = max(0.0, min(1.0, (raw + 1.0) / 2.0))
            present["signal_lab"] = True

        if meta["publication_date"]:
            age = _days_since(meta["publication_date"], now)
            if age is not None and age >= 0:
                components["recency"] = math.pow(0.5, age / _RECENCY_HALF_LIFE_DAYS)
                present["recency"] = True

        if not any(present.values()):
            results[pid] = 0.0
            continue
        results[pid] = _redistribute(components, present, weights)

    return results


def compute_paper_signal_score(
    db: sqlite3.Connection,
    paper_id: str,
    state: Optional[LibraryState] = None,
) -> float:
    """Single-paper convenience wrapper over `score_papers_batch`."""

    return score_papers_batch(db, [paper_id], state).get(paper_id, 0.0)


# -- helpers ----------------------------------------------------------

def _days_since(ts: Optional[str], now: datetime) -> Optional[float]:
    """Return days between `ts` (ISO-ish) and `now`; None if unparseable."""

    if not ts:
        return None
    raw = str(ts).strip()
    if not raw:
        return None
    for fmt in ("%Y-%m-%dT%H:%M:%S.%f%z", "%Y-%m-%dT%H:%M:%S%z",
                "%Y-%m-%dT%H:%M:%S", "%Y-%m-%d %H:%M:%S", "%Y-%m-%d"):
        try:
            parsed = datetime.strptime(raw, fmt)
            break
        except ValueError:
            continue
    else:
        # SQLite's default "YYYY-MM-DD HH:MM:SS" may miss tz; fall back
        # to fromisoformat which is permissive in Py 3.11+.
        try:
            parsed = datetime.fromisoformat(raw.replace("Z", "+00:00"))
        except ValueError:
            return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    delta = now - parsed
    return delta.total_seconds() / 86400.0
