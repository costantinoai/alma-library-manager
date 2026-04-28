"""Signal Lab service: feedback recording, preference profiles, and triage.

Manages the gamification layer for discovery: triage picks, swipe-style
decisions, topic preferences, and the materialized preference_profiles
table that feeds back into the discovery scoring engine.
"""

from __future__ import annotations

import json
import logging
import math
import sqlite3
import uuid
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple

logger = logging.getLogger(__name__)

# Half-life for exponential time decay on affinity weights (days).
DEFAULT_DECAY_HALF_LIFE_DAYS = 30

# Minimum interactions before preference_profiles influence scoring.
MIN_INTERACTIONS_FOR_SCORING = 2

# D11 (locked 2026-04-24): Signal Lab candidate pool is a labelled blend of
# three buckets. Default bias toward active suggestions — Signal Lab's main
# job is sharpening the recommender — but surface Library and occasional
# corpus papers too so the user can signal on the full space. Every
# candidate carries `source_bucket ∈ SIGNAL_LAB_BUCKETS`, and every
# resulting feedback event stamps that bucket into `context_json` so we can
# audit which bucket is driving signal (`mode_breakdown` companion metric).
# Weights can be overridden at call time — callers should only do that
# deliberately (e.g. a future "only suggestions" lens-tuning mode).
SIGNAL_LAB_BUCKET_WEIGHTS: Dict[str, float] = {
    "suggestion": 0.60,
    "library": 0.30,
    "corpus": 0.10,
}
SIGNAL_LAB_BUCKETS = tuple(SIGNAL_LAB_BUCKET_WEIGHTS.keys())

# Simple gamification tuning for Signal Lab.
_CHALLENGE_CONFIG: Dict[str, Dict[str, Any]] = {
    "interactions": {"goal": 12, "xp_reward": 20, "label": "12 interactions"},
    "tier": {"goal": 8, "xp_reward": 22, "label": "sort 8 cards into tiers"},
    "swipes": {"goal": 10, "xp_reward": 20, "label": "10 swipes"},
    "topic_tunes": {"goal": 4, "xp_reward": 16, "label": "4 topic tunes"},
    "superlikes": {"goal": 2, "xp_reward": 12, "label": "2 superlikes"},
}

_SWIPE_XP: Dict[str, int] = {"pass": 3, "like": 8, "superlike": 12}


# ---------------------------------------------------------------------------
# Feedback recording
# ---------------------------------------------------------------------------

def record_feedback(
    conn: sqlite3.Connection,
    event_type: str,
    entity_type: str,
    entity_id: str,
    value: Optional[Dict[str, Any]] = None,
    context: Optional[Dict[str, Any]] = None,
) -> str:
    """Insert a feedback event and update preference_profiles.

    Args:
        conn: Database connection.
        event_type: One of 'triage_pick', 'swipe', 'rating',
                    'topic_pref', 'author_pref', 'feed_action'.
        entity_type: 'publication', 'topic', or 'author'.
        entity_id: Identifier for the entity.
        value: JSON-serialisable dict (e.g. ``{"choice": "like"}``).
        context: Optional session context.

    Returns:
        The generated event ID.
    """
    event_id = uuid.uuid4().hex

    # Resolve the recommendation target BEFORE insert so we can stamp
    # source_key + source_label onto the event's context. This kills the
    # N+1 weekly source-diversity lookup in compute_signal_stats /
    # get_signal_results_summary — read paths just read context.
    rec_target = None
    paper_target_id: Optional[str] = None
    enriched_context: Optional[Dict[str, Any]] = dict(context) if context else None
    if entity_type == "publication":
        rec_target = _resolve_recommendation_target(conn, entity_id)
        if rec_target is not None:
            paper_target_id = str(rec_target["paper_id"] or "")
            src_key, src_label = _derive_source_fields(rec_target)
            if src_key or src_label:
                enriched_context = dict(enriched_context or {})
                # Do NOT overwrite caller-supplied values — callers may stamp
                # their own source hints (e.g. Feed writes context.mode='feed').
                enriched_context.setdefault("source_key", src_key)
                # Upgrade the label when the resolver finds a real display name
                # (OpenAlex / import_author rows), keeping the rec's branch
                # label as the baseline fallback.
                resolved_label = src_label
                if src_key and (not resolved_label or resolved_label == src_key):
                    author_name = _resolve_author_label(conn, src_key)
                    if author_name:
                        resolved_label = author_name
                enriched_context.setdefault("source_label", resolved_label or src_key)
        elif str((context or {}).get("paper_id") or "").strip():
            paper_target_id = str((context or {}).get("paper_id") or "").strip()
        else:
            try:
                row = conn.execute(
                    "SELECT id FROM papers WHERE id = ? LIMIT 1",
                    (entity_id,),
                ).fetchone()
                if row is not None:
                    paper_target_id = str(row["id"] or "")
            except sqlite3.OperationalError:
                paper_target_id = None

        # D11: stamp `source_bucket` on publication events so Signal Lab
        # stats can audit which bucket is driving signal. Callers from the
        # Library / corpus buckets pass it in `context`; for rec-backed
        # events we default to `suggestion`. Non-Signal-Lab callers (Feed,
        # rating endpoints) pass nothing and we leave the context
        # untouched — `source_bucket` is a Signal-Lab concept only. Raw
        # caller values are clamped to the canonical set to keep stats
        # joinable.
        if enriched_context is not None:
            raw_bucket = str(enriched_context.get("source_bucket") or "").strip()
            if raw_bucket in SIGNAL_LAB_BUCKETS:
                enriched_context["source_bucket"] = raw_bucket
            elif rec_target is not None:
                enriched_context["source_bucket"] = "suggestion"
            elif raw_bucket:
                # Caller tried to stamp an unknown bucket; drop it rather
                # than persist a value that breaks the canonical set.
                enriched_context.pop("source_bucket", None)

    conn.execute(
        """INSERT INTO feedback_events
           (id, event_type, entity_type, entity_id, value, context_json)
           VALUES (?, ?, ?, ?, ?, ?)""",
        (
            event_id,
            event_type,
            entity_type,
            entity_id,
            json.dumps(value) if value else None,
            json.dumps(enriched_context) if enriched_context else None,
        ),
    )

    if entity_type == "publication":
        _apply_recommendation_feedback(conn, event_type, rec_target, value or {})

    # Compute the affinity delta from this event
    delta = _affinity_delta(event_type, value, context)
    if entity_type == "publication":
        if paper_target_id:
            _update_preference_profile(conn, entity_type, paper_target_id, delta)
    else:
        _update_preference_profile(conn, entity_type, entity_id, delta)

    # For publication feedback, also propagate to related topics/authors
    if entity_type == "publication" and paper_target_id:
        _propagate_to_topics_and_authors(conn, paper_target_id, delta)
        _record_publication_lens_signal(conn, rec_target, event_type, value or {}, delta)

    conn.commit()
    return event_id


def _resolve_recommendation_target(
    conn: sqlite3.Connection,
    recommendation_id: str,
) -> Optional[sqlite3.Row]:
    rid = str(recommendation_id or "").strip()
    if not rid:
        return None
    try:
        return conn.execute(
            """
            SELECT id, paper_id, lens_id,
                   source_key, source_type, branch_label
            FROM recommendations
            WHERE id = ?
            LIMIT 1
            """,
            (rid,),
        ).fetchone()
    except sqlite3.OperationalError:
        return None


def _derive_source_fields(rec_row: Optional[sqlite3.Row]) -> tuple[str, str]:
    """Derive (source_key, source_label) from a recommendation row.

    The source_key is the stable identifier used for grouping / dedup in
    Signal Lab stats. The source_label is the human-readable string shown
    in `/results-summary` and the Signal Lab UI.
    """
    if rec_row is None:
        return "", ""
    source_key = (
        (rec_row["source_key"] if "source_key" in rec_row.keys() else "") or ""
    ).strip()
    if not source_key:
        source_key = (
            (rec_row["source_type"] if "source_type" in rec_row.keys() else "") or ""
        ).strip()
    if not source_key:
        source_key = (
            (rec_row["lens_id"] if "lens_id" in rec_row.keys() else "") or ""
        ).strip()
    branch_label = (
        (rec_row["branch_label"] if "branch_label" in rec_row.keys() else "") or ""
    ).strip()
    # Branch labels like "brain / functional" are already user-friendly; prefer
    # them over raw identifiers. Fallback humanises the source_key.
    label = branch_label or source_key.replace("_", " ").strip()
    return source_key, label


def _resolve_source_breakdown(
    conn: sqlite3.Connection,
    raw_counts: Dict[str, int],
    stamped_labels: Dict[str, str],
) -> Dict[str, int]:
    """Merge raw source_key counts into a {label: count} dict.

    Labels come from (in priority order):
      1. `stamped_labels` — the `context.source_label` written at event time
         (S-AUDIT-5, events written 2026-04-24+).
      2. A batch lookup in `authors` for legacy events whose source_key is
         an OpenAlex (`A\\d+` / `W\\d+`) or `import_author_*` identifier.
      3. A humanised form of the raw source_key (replace `_` with space).
    """
    if not raw_counts:
        return {}

    unresolved = [k for k in raw_counts.keys() if k not in stamped_labels]
    resolved: Dict[str, str] = {}

    # Batch-resolve import_author_* keys in one IN query.
    import_keys = [k for k in unresolved if k.startswith("import_author_")]
    if import_keys:
        placeholders = ",".join(["?"] * len(import_keys))
        try:
            for row in conn.execute(
                f"SELECT id, name FROM authors WHERE id IN ({placeholders})",
                import_keys,
            ).fetchall():
                name = str(row["name"] or "").strip()
                if name:
                    resolved[str(row["id"])] = name
        except sqlite3.OperationalError:
            pass

    # Batch-resolve OpenAlex author/work IDs (A123, W123). OpenAlex IDs live
    # as full URLs in `authors.openalex_id`, so match the suffix with LIKE.
    openalex_keys = [
        k for k in unresolved
        if len(k) > 1 and k[0] in "AW" and k[1:].isdigit()
    ]
    for key in openalex_keys:
        try:
            row = conn.execute(
                "SELECT name FROM authors WHERE openalex_id LIKE ? LIMIT 1",
                (f"%{key}",),
            ).fetchone()
        except sqlite3.OperationalError:
            row = None
        if row is not None:
            name = str(row["name"] or "").strip()
            if name:
                resolved[key] = name

    out: Dict[str, int] = {}
    for source_key, count in raw_counts.items():
        label = (
            stamped_labels.get(source_key)
            or resolved.get(source_key)
            or source_key.replace("_", " ").strip()
            or source_key
        )
        out[label] = out.get(label, 0) + count
    return out


def _resolve_author_label(conn: sqlite3.Connection, source_key: str) -> Optional[str]:
    """Resolve an OpenAlex-author-shaped (A12345) or import_author_* key to a display name."""
    key = (source_key or "").strip()
    if not key:
        return None
    try:
        if key.startswith("import_author_"):
            row = conn.execute(
                "SELECT name FROM authors WHERE id = ? LIMIT 1",
                (key,),
            ).fetchone()
        elif len(key) > 1 and key[0] in "AW" and key[1:].isdigit():
            # OpenAlex author IDs are stored as the full URL — match the suffix.
            row = conn.execute(
                "SELECT name FROM authors WHERE openalex_id LIKE ? LIMIT 1",
                (f"%{key}",),
            ).fetchone()
        else:
            return None
    except sqlite3.OperationalError:
        return None
    if row is None:
        return None
    name = str(row["name"] or "").strip()
    return name or None


def _apply_recommendation_feedback(
    conn: sqlite3.Connection,
    event_type: str,
    recommendation_row: Optional[sqlite3.Row],
    value: Dict[str, Any],
) -> None:
    """Update recommendations.user_action/action_at based on gameplay feedback."""
    if recommendation_row is None:
        return

    recommendation_id = str(recommendation_row["id"] or "")
    if not recommendation_id:
        return

    action: Optional[str] = None
    if event_type == "swipe":
        choice = str(value.get("choice") or "").strip().lower()
        if choice in {"like", "superlike"}:
            action = "like"
        elif choice == "pass":
            action = "dismiss"
        else:
            action = "seen"
    elif event_type == "tier_sort":
        # Map the user's drop-tier back to the like/dismiss action axis so
        # recommendations get the same lifecycle stamp as a swipe. S/A =
        # positive pick; B = neutral-seen; C = explicit negative.
        tier = str(value.get("tier") or "").strip().upper()
        if tier in {"S", "A"}:
            action = "like"
        elif tier == "C":
            action = "dismiss"
        else:
            action = "seen"
    elif event_type == "rating":
        action = "seen"

    if action:
        conn.execute(
            "UPDATE recommendations SET user_action = ?, action_at = ? WHERE id = ?",
            (action, datetime.utcnow().isoformat(), recommendation_id),
        )


def _record_publication_lens_signal(
    conn: sqlite3.Connection,
    recommendation_row: Optional[sqlite3.Row],
    event_type: str,
    value: Dict[str, Any],
    delta: float,
) -> None:
    """Write per-lens publication feedback to lens_signals."""
    if recommendation_row is None:
        return

    lens_id = str(recommendation_row["lens_id"] or "").strip()
    paper_id = str(recommendation_row["paper_id"] or "").strip()
    if not lens_id or not paper_id:
        return

    signal_value = 0
    if event_type == "swipe":
        choice = str(value.get("choice") or "").strip().lower()
        if choice in {"like", "superlike"}:
            signal_value = 1
        elif choice == "pass":
            signal_value = -1
    elif event_type == "tier_sort":
        tier = str(value.get("tier") or "").strip().upper()
        if tier in {"S", "A"}:
            signal_value = 1
        elif tier == "C":
            signal_value = -1
    elif delta > 0:
        signal_value = 1
    elif delta < 0:
        signal_value = -1

    conn.execute(
        """
        INSERT INTO lens_signals (lens_id, paper_id, signal_value, source)
        VALUES (?, ?, ?, ?)
        ON CONFLICT(lens_id, paper_id, source) DO UPDATE SET
            signal_value = excluded.signal_value,
            created_at = datetime('now')
        """,
        (lens_id, paper_id, int(signal_value), "signal_lab"),
    )


def _split_authors(authors_raw: str) -> list[str]:
    return [
        a.strip()
        for a in str(authors_raw or "").split(",")
        if a and a.strip()
    ]


def _reaction_multiplier(reaction_ms_raw: Any) -> float:
    """Return a bounded multiplier from reaction time telemetry."""
    try:
        reaction_ms = float(reaction_ms_raw)
    except (TypeError, ValueError):
        return 1.0
    if reaction_ms <= 0:
        return 1.0
    if reaction_ms < 600:
        return 0.7
    if reaction_ms < 1200:
        return 0.85
    if reaction_ms <= 30_000:
        return 1.0
    if reaction_ms <= 90_000:
        return 0.9
    return 0.75


def _affinity_delta(
    event_type: str,
    value: Optional[Dict[str, Any]],
    context: Optional[Dict[str, Any]] = None,
) -> float:
    """Compute affinity weight delta from an event + optional behavior context."""
    if value is None:
        return 0.0

    choice = value.get("choice", "")
    pref = value.get("pref", "")
    rating = value.get("rating")
    confidence_raw = value.get("confidence")
    confidence_mult = 1.0
    try:
        if confidence_raw is not None:
            confidence = float(confidence_raw)
            confidence_mult = max(0.7, min(1.35, 0.7 + 0.2 * confidence))
    except (TypeError, ValueError):
        confidence_mult = 1.0

    reaction_mult = _reaction_multiplier((context or {}).get("reaction_ms"))

    if event_type == "swipe":
        mapping = {"like": 1.0, "superlike": 2.0, "pass": -0.5}
        return round(mapping.get(choice, 0.0) * confidence_mult * reaction_mult, 4)

    if event_type == "tier_sort":
        # Tier drop emits 4-bit signal — strongest positive (S) to explicit
        # negative (C) in a single drag. Mapping matches the S/A/B/C piles
        # users see in TierSortMode.
        tier = str(value.get("tier") or "").strip().upper()
        mapping = {"S": 2.0, "A": 1.0, "B": 0.3, "C": -1.0}
        return round(mapping.get(tier, 0.0) * confidence_mult * reaction_mult, 4)

    if event_type == "rating":
        if rating is not None:
            # Map 1-5 -> [-1, 1]
            return round(((float(rating) - 3.0) / 2.0) * confidence_mult, 4)
        return 0.0

    if event_type in ("topic_pref", "author_pref", "source_pref"):
        pref_mapping = {"more": 1.0, "less": -1.0, "mute": -3.0}
        return round(pref_mapping.get(pref, 0.0) * confidence_mult, 4)

    if event_type == "feed_action":
        action = str(value.get("action") or "").strip().lower()
        mapping = {
            "add": 0.5,
            "like": 1.0,
            "love": 2.0,
            "dislike": -1.0,
        }
        return round(mapping.get(action, 0.0) * confidence_mult * reaction_mult, 4)

    return 0.0


def _update_preference_profile(
    conn: sqlite3.Connection,
    entity_type: str,
    entity_id: str,
    delta: float,
) -> None:
    """Upsert the preference_profiles row with time-decayed weight."""
    now_str = datetime.utcnow().isoformat()

    existing = conn.execute(
        "SELECT affinity_weight, confidence, interaction_count, last_updated "
        "FROM preference_profiles WHERE entity_type = ? AND entity_id = ?",
        (entity_type, entity_id),
    ).fetchone()

    if existing:
        old_weight = existing["affinity_weight"]
        old_count = existing["interaction_count"]
        last_updated = existing["last_updated"]

        # Apply time decay to old weight
        decayed = _apply_decay(old_weight, last_updated)
        new_weight = decayed + delta
        new_count = old_count + 1
        confidence = min(1.0, new_count / 20.0)

        conn.execute(
            """UPDATE preference_profiles
               SET affinity_weight = ?, confidence = ?,
                   interaction_count = ?, last_updated = ?
               WHERE entity_type = ? AND entity_id = ?""",
            (round(new_weight, 4), round(confidence, 4), new_count, now_str,
             entity_type, entity_id),
        )
    else:
        confidence = min(1.0, 1 / 20.0)
        conn.execute(
            """INSERT INTO preference_profiles
               (entity_type, entity_id, affinity_weight, confidence,
                interaction_count, last_updated)
               VALUES (?, ?, ?, ?, 1, ?)""",
            (entity_type, entity_id, round(delta, 4), round(confidence, 4), now_str),
        )


def _apply_decay(
    weight: float,
    last_updated_str: str,
    half_life_days: int = DEFAULT_DECAY_HALF_LIFE_DAYS,
) -> float:
    """Apply exponential time decay to a weight."""
    try:
        last_updated = datetime.fromisoformat(last_updated_str)
        age_days = (datetime.utcnow() - last_updated).total_seconds() / 86400.0
        decay = math.exp(-0.693 * age_days / half_life_days)  # ln(2) ~ 0.693
        return weight * decay
    except (ValueError, TypeError):
        return weight


def _propagate_to_topics_and_authors(
    conn: sqlite3.Connection,
    publication_entity_id: str,
    delta: float,
) -> None:
    """Propagate a publication feedback to its topics and authors.

    publication_entity_id is the paper UUID.
    """
    paper_id = publication_entity_id.strip()
    if not paper_id:
        return

    propagation_weight = 0.3  # reduced weight for transitive signals

    # Topics
    try:
        topic_rows = conn.execute(
            "SELECT term FROM publication_topics WHERE paper_id = ?",
            (paper_id,),
        ).fetchall()
        for tr in topic_rows:
            term = (tr["term"] or "").strip().lower()
            if term:
                _update_preference_profile(
                    conn, "topic", term, delta * propagation_weight
                )
    except Exception:
        pass

    # Authors
    try:
        author_rows = conn.execute(
            "SELECT display_name FROM publication_authors WHERE paper_id = ?",
            (paper_id,),
        ).fetchall()
        for ar in author_rows:
            name = (ar["display_name"] or "").strip().lower()
            if name:
                _update_preference_profile(
                    conn, "author", name, delta * propagation_weight
                )
    except Exception:
        pass


# ---------------------------------------------------------------------------
# Triage set selection
# ---------------------------------------------------------------------------

def _safe_parse_created_at(raw: Any) -> Optional[datetime]:
    if raw is None:
        return None
    s = str(raw).strip()
    if not s:
        return None
    if s.endswith("Z"):
        s = s[:-1]
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M:%S.%f"):
        try:
            return datetime.strptime(s, fmt)
        except ValueError:
            continue
    try:
        return datetime.fromisoformat(s)
    except ValueError:
        return None


def _recent_source_counts(conn: sqlite3.Connection, days: int = 7) -> Dict[str, int]:
    """Count recent publication-feedback interactions per recommendation source."""
    cutoff_dt = datetime.utcnow() - timedelta(days=max(1, days))
    counts: Dict[str, int] = {}
    for ev in _iter_feedback_events(conn):
        if ev.get("entity_type") != "publication":
            continue
        dt = ev.get("dt")
        if dt is None or dt < cutoff_dt:
            continue
        ctx = ev.get("context") or {}
        source_key = str(ctx.get("source_key") or "").strip()
        if not source_key:
            source_key = str(ctx.get("source_type") or "").strip()
        if not source_key:
            source_key = str(ctx.get("lens_id") or "").strip()
        if not source_key:
            rec_target = _resolve_recommendation_target(conn, str(ev.get("entity_id") or ""))
            if rec_target is not None:
                try:
                    rec_row = conn.execute(
                        """
                        SELECT COALESCE(NULLIF(source_key, ''), NULLIF(source_type, ''), NULLIF(lens_id, ''), 'unknown') AS source_key
                        FROM recommendations
                        WHERE id = ?
                        LIMIT 1
                        """,
                        (str(rec_target["id"] or ""),),
                    ).fetchone()
                    source_key = str((rec_row["source_key"] if rec_row else "unknown") or "unknown")
                except sqlite3.OperationalError:
                    source_key = "unknown"
        if not source_key:
            source_key = "unknown"
        counts[source_key] = counts.get(source_key, 0) + 1
    return counts


def _session_source_counts(conn: sqlite3.Connection, session_id: str) -> Dict[str, int]:
    """Count source exposure in the current gameplay session."""
    sid = (session_id or "").strip()
    if not sid:
        return {}
    try:
        rows = conn.execute(
            """
            SELECT context_json, entity_id
            FROM feedback_events
            WHERE event_type IN ('swipe', 'tier_sort', 'triage_pick')
            ORDER BY created_at DESC
            LIMIT 500
            """
        ).fetchall()
    except sqlite3.OperationalError:
        return {}

    counts: Dict[str, int] = {}
    for row in rows:
        ctx = _safe_json_loads(row["context_json"])
        if str(ctx.get("session_id") or "") != sid:
            continue
        source_key = str(ctx.get("source_key") or "").strip()
        if not source_key:
            source_key = str(ctx.get("source_type") or "").strip()
        if not source_key:
            source_key = str(ctx.get("lens_id") or "").strip()
        if not source_key:
            try:
                rec = conn.execute(
                    """
                    SELECT COALESCE(
                        NULLIF(source_key, ''),
                        NULLIF(source_type, ''),
                        NULLIF(lens_id, ''),
                        'unknown'
                    ) AS source_key
                    FROM recommendations
                    WHERE id = ?
                    """,
                    (row["entity_id"],),
                ).fetchone()
                source_key = str((rec["source_key"] if rec else "unknown") or "unknown")
            except sqlite3.OperationalError:
                source_key = "unknown"
        counts[source_key] = counts.get(source_key, 0) + 1
    return counts


def _parse_score_breakdown(raw: Any) -> Dict[str, Any]:
    if isinstance(raw, dict):
        return raw
    if raw is None:
        return {}
    try:
        parsed = json.loads(raw)
        return parsed if isinstance(parsed, dict) else {}
    except Exception:
        return {}


def _active_recommendation_candidates(
    conn: sqlite3.Connection,
    *,
    lens_id: Optional[str] = None,
    limit: int = 240,
) -> list[dict]:
    where = ["r.user_action IS NULL"]
    params: list[Any] = []
    lid = str(lens_id or "").strip()
    if lid:
        where.append("r.lens_id = ?")
        params.append(lid)

    # Exclude papers that were already actioned in ANY recommendation row.
    # The same paper_id can appear across multiple recommendation rows (from
    # different lens refreshes).  Once actioned, never re-present in games.
    where.append("""
        r.paper_id NOT IN (
            SELECT DISTINCT r2.paper_id
            FROM recommendations r2
            WHERE r2.user_action IS NOT NULL
        )
    """)

    query = f"""
        SELECT
            r.*,
            r.id AS recommendation_id,
            p.title,
            p.authors,
            p.abstract,
            p.url,
            p.doi,
            p.year,
            p.journal
        FROM recommendations r
        LEFT JOIN papers p ON p.id = r.paper_id
        WHERE {" AND ".join(where)}
        ORDER BY r.created_at DESC
        LIMIT ?
    """
    params.append(max(1, int(limit)))
    try:
        rows = conn.execute(query, params).fetchall()
    except sqlite3.OperationalError:
        return []
    return [dict(r) for r in rows]


def _library_candidates(
    conn: sqlite3.Connection,
    *,
    limit: int = 80,
    exclude_paper_ids: Optional[set[str]] = None,
) -> list[dict]:
    """Return Library-paper candidates for the Signal Lab blend (D11 bucket 2).

    Library rows are the user's strongest positive signal — using them in
    Signal Lab lets us sharpen author / topic / embedding affinities on the
    papers we already know the user cares about. We shape each row to match
    the `_active_recommendation_candidates` schema so downstream code
    (priority ranking, payload shaping, feedback recording) treats them
    uniformly. The `recommendation_id` field is set to the paper's `id`:
    there is no matching `recommendations` row, so `record_feedback`'s rec
    lookup will miss and fall through to its `papers` fallback — exactly
    the behaviour we want, because Signal Lab events on Library papers are
    signal-only and must not write to `recommendations`.
    """
    if not _table_exists(conn, "papers"):
        return []
    exclude = {pid for pid in (exclude_paper_ids or set()) if pid}
    limit = max(0, int(limit))
    if limit <= 0:
        return []
    # Pull a small random sample biased toward higher ratings so stronger
    # positive signals dominate the bucket. `ORDER BY RANDOM()` is cheap at
    # Library scale (<10k rows) and avoids a deterministic cycle across
    # gameplay sessions.
    try:
        rows = conn.execute(
            """
            SELECT
                id AS paper_id,
                title, authors, abstract, url, doi, year, journal,
                rating, added_at
            FROM papers
            WHERE status = 'library'
            ORDER BY COALESCE(rating, 0) DESC, RANDOM()
            LIMIT ?
            """,
            (limit * 3,),
        ).fetchall()
    except sqlite3.OperationalError:
        return []

    now_iso = datetime.utcnow().isoformat()
    out: list[dict] = []
    for row in rows:
        paper_id = str(row["paper_id"] or "").strip()
        if not paper_id or paper_id in exclude:
            continue
        rating = int(row["rating"] or 0)
        # Baseline score so Library rows compete with suggestion scores
        # (which cluster 0.4–0.9). A 4-star paper scores 0.82; a 3-star
        # paper 0.70; an unrated Library paper 0.58.
        score = 0.58 + min(max(rating, 0), 5) * 0.06
        out.append({
            "recommendation_id": paper_id,  # frontend-visible id
            "paper_id": paper_id,
            "lens_id": "library",
            "source_type": "library",
            "source_key": "library_sample",
            "source_label": "From Library",
            "source_api": None,
            "branch_id": None,
            "branch_label": None,
            "branch_mode": None,
            "source_bucket": "library",
            "title": row["title"],
            "authors": row["authors"],
            "abstract": row["abstract"],
            "url": row["url"],
            "doi": row["doi"],
            "year": row["year"],
            "journal": row["journal"],
            "score": score,
            "score_breakdown": None,
            "user_action": None,
            "created_at": row["added_at"] or now_iso,
        })
        if len(out) >= limit:
            break
    return out


def _corpus_candidates(
    conn: sqlite3.Connection,
    *,
    limit: int = 30,
    exclude_paper_ids: Optional[set[str]] = None,
) -> list[dict]:
    """Return tracked-but-not-recommended papers for the blend (D11 bucket 3).

    Only surface a small fraction — the purpose is "let the user signal on
    a paper we know about but haven't promoted yet", not to flood the deck.
    Excludes Library (bucket 2) and any paper_ids already carried by the
    suggestion bucket so the same row never appears twice under different
    labels.
    """
    if not _table_exists(conn, "papers"):
        return []
    exclude = {pid for pid in (exclude_paper_ids or set()) if pid}
    limit = max(0, int(limit))
    if limit <= 0:
        return []
    try:
        rows = conn.execute(
            """
            SELECT
                id AS paper_id,
                title, authors, abstract, url, doi, year, journal
            FROM papers
            WHERE status = 'tracked'
            ORDER BY RANDOM()
            LIMIT ?
            """,
            (limit * 3,),
        ).fetchall()
    except sqlite3.OperationalError:
        return []
    now_iso = datetime.utcnow().isoformat()
    out: list[dict] = []
    for row in rows:
        paper_id = str(row["paper_id"] or "").strip()
        if not paper_id or paper_id in exclude:
            continue
        out.append({
            "recommendation_id": paper_id,
            "paper_id": paper_id,
            "lens_id": "corpus",
            "source_type": "corpus",
            "source_key": "corpus_sample",
            "source_label": "Corpus sample",
            "source_api": None,
            "branch_id": None,
            "branch_label": None,
            "branch_mode": None,
            "source_bucket": "corpus",
            "title": row["title"],
            "authors": row["authors"],
            "abstract": row["abstract"],
            "url": row["url"],
            "doi": row["doi"],
            "year": row["year"],
            "journal": row["journal"],
            # Modest floor so corpus rows don't flood but can win priority
            # when the suggestion pool is thin.
            "score": 0.45,
            "score_breakdown": None,
            "user_action": None,
            "created_at": now_iso,
        })
        if len(out) >= limit:
            break
    return out


def _signal_lab_candidates(
    conn: sqlite3.Connection,
    *,
    lens_id: Optional[str] = None,
    recommendation_limit: int = 240,
    bucket_weights: Optional[Dict[str, float]] = None,
) -> list[dict]:
    """Return a blended candidate pool for Signal Lab gameplay (D11).

    Blends three buckets — active suggestions (primary), Library papers,
    and occasional corpus samples — with configurable weights (default
    `SIGNAL_LAB_BUCKET_WEIGHTS` 60/30/10). Each returned row carries
    `source_bucket ∈ SIGNAL_LAB_BUCKETS`; downstream code threads that
    label into the feedback event's context so we can audit which bucket
    is driving signal.

    Dedup is strict on `paper_id`: a paper already in the suggestion
    bucket is never re-surfaced under `library` or `corpus` labels.
    Caller is still responsible for priority-ranking / shuffling; this
    function just assembles the candidate pool.

    Signal Lab's D11 contract: this pool's composition must remain
    auditable, and callers MUST NOT mutate `papers.status` /
    `papers.rating` / `papers.reading_status` in response to a
    candidate surfacing here. All state change goes through
    `record_feedback`.
    """
    weights = {**SIGNAL_LAB_BUCKET_WEIGHTS, **(bucket_weights or {})}
    total = max(1, int(recommendation_limit))

    # Apportion slots per bucket. Suggestion gets the floor of the ratio
    # so an empty Library/corpus never starves suggestions; library + corpus
    # collapse into zero-slot if the user has no Library yet.
    sug_slots = max(1, int(round(total * weights.get("suggestion", 0.0))))
    lib_slots = max(0, int(round(total * weights.get("library", 0.0))))
    cor_slots = max(0, int(round(total * weights.get("corpus", 0.0))))

    # Bucket 1 — active suggestions.
    suggestions = _active_recommendation_candidates(
        conn, lens_id=lens_id, limit=sug_slots,
    )
    for row in suggestions:
        row["source_bucket"] = "suggestion"

    seen_paper_ids: set[str] = {
        str(row.get("paper_id") or "").strip()
        for row in suggestions
        if str(row.get("paper_id") or "").strip()
    }

    # Bucket 2 — Library papers.
    library = _library_candidates(
        conn, limit=lib_slots, exclude_paper_ids=seen_paper_ids,
    )
    seen_paper_ids.update(
        str(row.get("paper_id") or "").strip()
        for row in library
        if str(row.get("paper_id") or "").strip()
    )

    # Bucket 3 — tracked corpus sample.
    corpus = _corpus_candidates(
        conn, limit=cor_slots, exclude_paper_ids=seen_paper_ids,
    )

    return [*suggestions, *library, *corpus]


def _table_exists(conn: sqlite3.Connection, name: str) -> bool:
    try:
        row = conn.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name=? LIMIT 1",
            (name,),
        ).fetchone()
    except sqlite3.OperationalError:
        return False
    return row is not None


def _recommendation_payload(row: dict) -> dict:
    action = str(row.get("user_action") or "")
    source_type = str(row.get("source_type") or "").strip() or "lens_retrieval"
    source_key = (
        str(row.get("source_key") or "").strip()
        or source_type
        or str(row.get("lens_id") or "").strip()
        or "unknown"
    )
    source_label = str(row.get("branch_label") or "").strip() or source_key.replace("_", " ")
    # D11: every candidate carries a bucket label so the UI can show
    # provenance ("Suggestion" / "From Library" / "Corpus sample") and the
    # feedback POST can round-trip it back into the event context.
    raw_bucket = str(row.get("source_bucket") or "").strip()
    source_bucket = raw_bucket if raw_bucket in SIGNAL_LAB_BUCKETS else "suggestion"
    return {
        "id": str(row.get("recommendation_id") or ""),
        "paper_id": str(row.get("paper_id") or ""),
        "lens_id": str(row.get("lens_id") or "unknown"),
        "source_type": source_type,
        "source_key": source_key,
        "source_label": source_label,
        "source_bucket": source_bucket,
        "source_api": str(row.get("source_api") or "").strip() or None,
        "branch_id": str(row.get("branch_id") or "").strip() or None,
        "branch_label": str(row.get("branch_label") or "").strip() or None,
        "branch_mode": str(row.get("branch_mode") or "").strip() or None,
        "recommended_title": str(row.get("title") or ""),
        "recommended_authors": row.get("authors") or "",
        "recommended_abstract": row.get("abstract") or "",
        "recommended_url": row.get("url") or "",
        "recommended_doi": row.get("doi") or "",
        "recommended_year": row.get("year"),
        "recommended_journal": row.get("journal") or "",
        "score": float(row.get("score") or 0.0),
        "score_breakdown": _parse_score_breakdown(row.get("score_breakdown")),
        "seen": bool(action),
        "liked": action == "like",
        "dismissed": action == "dismiss",
        "created_at": row.get("created_at") or datetime.utcnow().isoformat(),
    }


def _candidate_priority(
    row: dict,
    recent_counts: Dict[str, int],
    session_counts: Optional[Dict[str, int]] = None,
) -> float:
    """Coverage-aware priority for recommendation gameplay candidates."""
    source_key = (
        str(row.get("source_key") or "").strip()
        or str(row.get("source_type") or "").strip()
        or str(row.get("lens_id") or "").strip()
        or "unknown"
    )
    base_score = float(row.get("score") or 0.0)
    created_at = _safe_parse_created_at(row.get("created_at"))
    age_days = ((datetime.utcnow() - created_at).total_seconds() / 86400.0) if created_at else 30.0
    recency = max(0.0, min(1.0, 1.0 - (age_days / 21.0)))

    recent_n = recent_counts.get(source_key, 0)
    lens_balance = 1.0 / (1.0 + recent_n)

    session_penalty = 0.0
    if session_counts:
        session_penalty = min(0.45, 0.12 * float(session_counts.get(source_key, 0)))

    return (base_score * 0.65) + (lens_balance * 0.25) + (recency * 0.10) - session_penalty


def get_triage_set(
    conn: sqlite3.Connection,
    count: int = 3,
    *,
    exclude_ids: Optional[set[str]] = None,
    lens_id: Optional[str] = None,
) -> List[dict]:
    """Select diverse papers for forced-choice triage.

    Picks from recent unseen recommendations, maximising source diversity.

    Returns:
        List of recommendation dicts (up to ``count``).
    """
    rows = _signal_lab_candidates(conn, lens_id=lens_id, recommendation_limit=180)
    if not rows:
        return []

    excluded = {str(i).strip() for i in (exclude_ids or set()) if str(i).strip()}
    candidates: list[dict] = []
    for row in rows:
        rec_id = str(row.get("recommendation_id") or "")
        if rec_id and rec_id in excluded:
            continue
        candidates.append(row)

    if not candidates:
        return []

    recent_counts = _recent_source_counts(conn, days=7)
    for cand in candidates:
        cand["_priority"] = _candidate_priority(cand, recent_counts)

    candidates.sort(key=lambda x: float(x.get("_priority") or 0.0), reverse=True)

    result: List[dict] = []
    used_sources: set[str] = set()
    while candidates and len(result) < count:
        best_idx = 0
        best_val = -10_000.0
        for idx, cand in enumerate(candidates[:40]):
            source_key = (
                str(cand.get("source_key") or "").strip()
                or str(cand.get("source_type") or "").strip()
                or str(cand.get("lens_id") or "").strip()
                or "unknown"
            )
            diversity_bonus = 0.08 if source_key not in used_sources else 0.0
            val = float(cand.get("_priority") or 0.0) + diversity_bonus
            if val > best_val:
                best_val = val
                best_idx = idx
        picked = candidates.pop(best_idx)
        picked.pop("_priority", None)
        result.append(_recommendation_payload(picked))
        used_sources.add(
            str(picked.get("source_key") or "").strip()
            or str(picked.get("source_type") or "").strip()
            or str(picked.get("lens_id") or "").strip()
            or "unknown"
        )

    return result


# ---------------------------------------------------------------------------
# Swipe mode
# ---------------------------------------------------------------------------

def get_next_swipe(
    conn: sqlite3.Connection,
    *,
    exclude_ids: Optional[set[str]] = None,
    session_id: Optional[str] = None,
    lens_id: Optional[str] = None,
) -> Optional[dict]:
    """Get the next paper for swipe mode (highest scoring unseen).

    Returns:
        A recommendation dict, or None if none available.
    """
    rows = _signal_lab_candidates(conn, lens_id=lens_id, recommendation_limit=220)
    if not rows:
        return None

    excluded = {str(i).strip() for i in (exclude_ids or set()) if str(i).strip()}
    recent_counts = _recent_source_counts(conn, days=7)
    session_counts = _session_source_counts(conn, session_id or "")

    best: Optional[dict] = None
    best_score = -10_000.0
    for cand in rows:
        rec_id = str(cand.get("recommendation_id") or "")
        if rec_id and rec_id in excluded:
            continue
        pri = _candidate_priority(cand, recent_counts, session_counts)
        if pri > best_score:
            best_score = pri
            best = cand
    return _recommendation_payload(best) if best else None


def _recent_entity_counts(conn: sqlite3.Connection, entity_type: str, days: int = 30) -> Dict[str, int]:
    """Count recent interactions for an entity type from feedback_events."""
    cutoff = (datetime.utcnow() - timedelta(days=max(1, days))).strftime("%Y-%m-%d %H:%M:%S")
    try:
        rows = conn.execute(
            """
            SELECT entity_id, COUNT(*) AS cnt
            FROM feedback_events
            WHERE entity_type = ? AND datetime(created_at) >= datetime(?)
            GROUP BY entity_id
            """,
            (entity_type, cutoff),
        ).fetchall()
    except sqlite3.OperationalError:
        return {}
    return {str(r["entity_id"]): int(r["cnt"] or 0) for r in rows}


def get_author_duel(
    conn: sqlite3.Connection,
    count: int = 2,
    *,
    exclude_authors: Optional[set[str]] = None,
    lens_id: Optional[str] = None,
) -> List[dict]:
    """Return an author-focused duel set from active recommendations."""
    rows = _signal_lab_candidates(conn, lens_id=lens_id, recommendation_limit=260)
    if not rows:
        return []

    excluded = {a.lower().strip() for a in (exclude_authors or set()) if a and a.strip()}
    recent_counts = _recent_entity_counts(conn, "author", days=30)

    candidates: Dict[str, dict] = {}
    for row in rows:
        rec_id = str(row.get("recommendation_id") or "").strip()
        row_lens = str(row.get("lens_id") or "unknown")
        title = str(row.get("title") or "").strip()
        score = float(row.get("score") or 0.0)
        for author in _split_authors(str(row.get("authors") or "")):
            key = author.lower()
            if key in excluded:
                continue
            c = candidates.get(key)
            if c is None:
                c = {
                    "author": author,
                    "author_key": key,
                    "paper_count": 0,
                    "avg_score": 0.0,
                    "lens_ids": set(),
                    "sample_title": title,
                    "sample_rec_id": rec_id,
                }
                candidates[key] = c
            c["paper_count"] += 1
            c["avg_score"] += score
            c["lens_ids"].add(row_lens)
            if score > float(c.get("best_score") or -1):
                c["best_score"] = score
                c["sample_title"] = title
                c["sample_rec_id"] = rec_id

    prepared: list[dict] = []
    for key, c in candidates.items():
        if c["paper_count"] <= 0:
            continue
        avg_score = c["avg_score"] / c["paper_count"]
        lens_div = len(c["lens_ids"])
        recent_n = recent_counts.get(key, 0)
        priority = (avg_score * 0.65) + (min(3, lens_div) * 0.12) + (1.0 / (1.0 + recent_n))
        prepared.append(
            {
                "author": c["author"],
                "author_key": key,
                "paper_count": int(c["paper_count"]),
                "avg_score": round(avg_score, 4),
                "source_diversity": lens_div,
                "sample_title": c["sample_title"],
                "sample_rec_id": c["sample_rec_id"],
                "_priority": priority,
            }
        )

    prepared.sort(key=lambda x: float(x.get("_priority") or 0.0), reverse=True)
    out: List[dict] = []
    for item in prepared:
        item.pop("_priority", None)
        out.append(item)
        if len(out) >= max(1, count):
            break
    return out


# ---------------------------------------------------------------------------
# Signal stats
# ---------------------------------------------------------------------------

def compute_signal_stats(conn: sqlite3.Connection) -> dict:
    """Aggregate feedback events into user-facing stats.

    Returns:
        Dict with total_interactions, event_breakdown, topic_coverage,
        streak_days, top_topics, top_authors.
    """
    stats: Dict[str, Any] = {
        "total_interactions": 0,
        "today_interactions": 0,
        "week_interactions": 0,
        "event_breakdown": {},
        "topic_coverage": 0,
        "streak_days": 0,
        "top_topics": [],
        "top_authors": [],
        "xp": 0,
        "level": 1,
        "current_level_xp": 0,
        "next_level_xp": _xp_for_level(2),
        "xp_to_next_level": _xp_for_level(2),
        "level_progress_pct": 0.0,
        "background_corpus_papers": 0,
        "background_corpus_authors": 0,
        "daily_challenges": {
            key: {
                "label": cfg["label"],
                "goal": cfg["goal"],
                "progress": 0,
                "completed": False,
                "xp_reward": cfg["xp_reward"],
            }
            for key, cfg in _CHALLENGE_CONFIG.items()
        },
        "behavioral_metrics": {
            "avg_reaction_ms": None,
            "median_reaction_ms": None,
            "fast_decision_rate": 0.0,
            "deliberate_rate": 0.0,
            "source_diversity_7d": 0,
            "mode_breakdown": {},
        },
    }

    try:
        # SQL-backed aggregates: do the counting / breakdowns in SQLite rather
        # than loading every feedback event into Python on each call. The only
        # full-table Python pass that remains is the XP calculation below,
        # because `_event_xp` depends on per-event confidence + reaction
        # multipliers that don't express cleanly in SQL.
        stats["total_interactions"] = int(
            conn.execute("SELECT COUNT(*) AS c FROM feedback_events").fetchone()["c"] or 0
        )
        stats["today_interactions"] = int(
            conn.execute(
                "SELECT COUNT(*) AS c FROM feedback_events WHERE date(created_at) = date('now')"
            ).fetchone()["c"]
            or 0
        )
        stats["week_interactions"] = int(
            conn.execute(
                "SELECT COUNT(*) AS c FROM feedback_events "
                "WHERE datetime(created_at) >= datetime('now', '-7 days')"
            ).fetchone()["c"]
            or 0
        )
        event_breakdown: Dict[str, int] = {
            str(r["event_type"]): int(r["c"] or 0)
            for r in conn.execute(
                "SELECT event_type, COUNT(*) AS c FROM feedback_events GROUP BY event_type"
            ).fetchall()
        }
        stats["event_breakdown"] = event_breakdown

        mode_breakdown: Dict[str, int] = {
            str(r["mode"]): int(r["c"] or 0)
            for r in conn.execute(
                """
                SELECT json_extract(context_json, '$.mode') AS mode, COUNT(*) AS c
                FROM feedback_events
                WHERE json_extract(context_json, '$.mode') IS NOT NULL
                  AND json_extract(context_json, '$.mode') <> ''
                GROUP BY mode
                """
            ).fetchall()
            if r["mode"]
        }

        reaction_samples: list[float] = [
            float(r["ms"])
            for r in conn.execute(
                """
                SELECT CAST(json_extract(context_json, '$.reaction_ms') AS REAL) AS ms
                FROM feedback_events
                WHERE datetime(created_at) >= datetime('now', '-30 days')
                  AND json_extract(context_json, '$.reaction_ms') IS NOT NULL
                """
            ).fetchall()
            if r["ms"] is not None and float(r["ms"]) > 0
        ]

        topic_count = conn.execute(
            "SELECT COUNT(*) as cnt FROM preference_profiles "
            "WHERE entity_type = 'topic' AND affinity_weight > 0"
        ).fetchone()
        stats["topic_coverage"] = topic_count["cnt"] if topic_count else 0

        top_topics = conn.execute(
            """SELECT entity_id, affinity_weight, interaction_count
               FROM preference_profiles
               WHERE entity_type = 'topic' AND affinity_weight > 0
               ORDER BY affinity_weight DESC LIMIT 10"""
        ).fetchall()
        stats["top_topics"] = [
            {
                "topic": r["entity_id"],
                "weight": round(r["affinity_weight"], 3),
                "interactions": r["interaction_count"],
            }
            for r in top_topics
        ]

        top_authors = conn.execute(
            """SELECT entity_id, affinity_weight, interaction_count
               FROM preference_profiles
               WHERE entity_type = 'author' AND affinity_weight > 0
               ORDER BY affinity_weight DESC LIMIT 10"""
        ).fetchall()
        stats["top_authors"] = [
            {
                "author": r["entity_id"],
                "weight": round(r["affinity_weight"], 3),
                "interactions": r["interaction_count"],
            }
            for r in top_authors
        ]

        streak_days = _compute_streak(conn)
        stats["streak_days"] = streak_days

        try:
            bg_row = conn.execute(
                """
                SELECT
                    COUNT(DISTINCT p.id) AS papers,
                    COUNT(DISTINCT fa.author_id) AS authors
                FROM papers p
                JOIN publication_authors pa ON pa.paper_id = p.id
                JOIN authors a ON a.openalex_id = pa.openalex_id
                JOIN followed_authors fa ON fa.author_id = a.id
                WHERE p.status <> 'library'
                """
            ).fetchone()
            if bg_row:
                stats["background_corpus_papers"] = int(bg_row["papers"] or 0)
                stats["background_corpus_authors"] = int(bg_row["authors"] or 0)
        except sqlite3.OperationalError:
            pass

        # XP is a full-table Python pass — `_event_xp` depends on bounded
        # confidence + reaction multipliers that don't express cleanly in SQL.
        # Acceptable at current scale (~330 events → <1 ms).
        all_events = _iter_feedback_events(conn)
        base_xp = sum(_event_xp(ev) for ev in all_events)
        streak_bonus = min(40, streak_days * 4)
        total_xp = base_xp + streak_bonus
        level, level_floor_xp, next_level_xp = _level_from_xp(total_xp)
        stats["xp"] = total_xp
        stats["level"] = level
        stats["current_level_xp"] = level_floor_xp
        stats["next_level_xp"] = next_level_xp
        stats["xp_to_next_level"] = max(0, next_level_xp - total_xp)
        span = max(1, next_level_xp - level_floor_xp)
        stats["level_progress_pct"] = round(
            max(0.0, min(100.0, ((total_xp - level_floor_xp) * 100.0) / span)), 1
        )

        # Daily challenge counts — small (today-only) SQL aggregate.
        challenge_rows = conn.execute(
            """
            SELECT
                event_type,
                COUNT(*) AS total,
                SUM(CASE WHEN json_extract(value, '$.choice') = 'superlike' THEN 1 ELSE 0 END) AS superlikes
            FROM feedback_events
            WHERE date(created_at) = date('now')
            GROUP BY event_type
            """
        ).fetchall()
        today_total = 0
        tier_today = 0
        swipes_today = 0
        topic_tunes_today = 0
        superlikes_today = 0
        for r in challenge_rows:
            ev_type = str(r["event_type"] or "")
            n = int(r["total"] or 0)
            today_total += n
            if ev_type == "tier_sort":
                tier_today += n
            elif ev_type == "swipe":
                swipes_today += n
                superlikes_today += int(r["superlikes"] or 0)
            elif ev_type == "topic_pref":
                topic_tunes_today += n
        challenge_counts = {
            "interactions": today_total,
            "tier": tier_today,
            "swipes": swipes_today,
            "topic_tunes": topic_tunes_today,
            "superlikes": superlikes_today,
        }
        completed_challenges = 0
        for key, cfg in _CHALLENGE_CONFIG.items():
            progress = int(challenge_counts.get(key, 0))
            goal = int(cfg["goal"])
            completed = progress >= goal
            if completed:
                completed_challenges += 1
            stats["daily_challenges"][key]["progress"] = min(progress, goal)
            stats["daily_challenges"][key]["completed"] = completed
        stats["daily_challenges"]["completed_count"] = completed_challenges
        stats["daily_challenges"]["total_count"] = len(_CHALLENGE_CONFIG)

        avg_reaction = (sum(reaction_samples) / len(reaction_samples)) if reaction_samples else None
        median_reaction = _median(reaction_samples)
        fast_count = sum(1 for r in reaction_samples if r < 1500)
        deliberate_count = sum(1 for r in reaction_samples if r > 8000)
        denom = len(reaction_samples) if reaction_samples else 1

        # Source diversity — one SELECT DISTINCT over the 7d publication
        # window. Relies on `context.source_key` stamped at write time
        # (S-AUDIT-5); legacy events without the key fall back to the raw
        # context.source_type / lens_id chain inlined in COALESCE.
        diversity_row = conn.execute(
            """
            SELECT COUNT(DISTINCT
                COALESCE(
                    NULLIF(json_extract(context_json, '$.source_key'), ''),
                    NULLIF(json_extract(context_json, '$.source_type'), ''),
                    NULLIF(json_extract(context_json, '$.lens_id'), '')
                )
            ) AS n
            FROM feedback_events
            WHERE entity_type = 'publication'
              AND datetime(created_at) >= datetime('now', '-7 days')
            """
        ).fetchone()
        source_diversity = int((diversity_row["n"] if diversity_row else 0) or 0)

        stats["behavioral_metrics"] = {
            "avg_reaction_ms": round(avg_reaction, 1) if avg_reaction is not None else None,
            "median_reaction_ms": round(median_reaction, 1) if median_reaction is not None else None,
            "fast_decision_rate": round(fast_count / denom, 3),
            "deliberate_rate": round(deliberate_count / denom, 3),
            "source_diversity_7d": source_diversity,
            "mode_breakdown": mode_breakdown,
        }
    except sqlite3.OperationalError as exc:
        logger.warning("Signal stats query failed: %s", exc)

    return stats


def get_signal_results_summary(conn: sqlite3.Connection, days: int = 14) -> dict:
    """Return a compact, export-friendly summary of Signal Lab outcomes."""
    period_days = max(1, min(int(days), 365))
    cutoff_dt = datetime.utcnow() - timedelta(days=period_days)
    cutoff = cutoff_dt.strftime("%Y-%m-%d %H:%M:%S")
    events = _iter_feedback_events(conn)
    period_events = [ev for ev in events if ev.get("dt") and ev["dt"] >= cutoff_dt]

    mode_breakdown: Dict[str, int] = {}
    # Collect (source_key, source_label?) per period event so we can resolve
    # legacy events (written before S-AUDIT-5) in a single batch pass.
    raw_source_counts: Dict[str, int] = {}
    stamped_labels: Dict[str, str] = {}
    reaction_samples: list[float] = []
    for ev in period_events:
        ctx = ev.get("context") or {}
        mode = str(ctx.get("mode") or "").strip() or "unknown"
        mode_breakdown[mode] = mode_breakdown.get(mode, 0) + 1
        source_key = str(ctx.get("source_key") or "").strip()
        if not source_key:
            source_key = str(ctx.get("source_type") or "").strip()
        if not source_key:
            source_key = str(ctx.get("lens_id") or "").strip()
        if source_key:
            raw_source_counts[source_key] = raw_source_counts.get(source_key, 0) + 1
            stamped = str(ctx.get("source_label") or "").strip()
            if stamped and source_key not in stamped_labels:
                stamped_labels[source_key] = stamped
        try:
            r = float(ctx.get("reaction_ms"))
            if r > 0:
                reaction_samples.append(r)
        except (TypeError, ValueError):
            pass

    # Resolve labels for source_keys without a stamped label in one batch
    # (legacy events pre-date the S-AUDIT-5 write-time enrichment). Keyed by
    # the friendly label so the UI renders "DiCarlo, James: 226" instead of
    # "import_author_3fde30c82f40d4c9: 226".
    source_breakdown = _resolve_source_breakdown(
        conn, raw_source_counts, stamped_labels
    )

    try:
        pref_rows = conn.execute(
            """
            SELECT entity_type, entity_id, affinity_weight, interaction_count
            FROM preference_profiles
            WHERE entity_type IN ('topic', 'author', 'source')
            ORDER BY affinity_weight DESC
            """
        ).fetchall()
    except sqlite3.OperationalError:
        pref_rows = []

    top_positive = []
    top_negative = []
    for r in pref_rows:
        row = {
            "entity_type": str(r["entity_type"]),
            "entity_id": str(r["entity_id"]),
            "weight": round(float(r["affinity_weight"] or 0.0), 4),
            "interactions": int(r["interaction_count"] or 0),
        }
        if row["weight"] >= 0:
            top_positive.append(row)
        else:
            top_negative.append(row)

    top_positive = top_positive[:10]
    top_negative = sorted(top_negative, key=lambda x: x["weight"])[:10]

    try:
        rec_outcome_row = conn.execute(
            """
            SELECT
              SUM(CASE WHEN user_action = 'like' THEN 1 ELSE 0 END) AS liked,
              SUM(CASE WHEN user_action = 'dismiss' THEN 1 ELSE 0 END) AS dismissed,
              SUM(CASE WHEN user_action IS NOT NULL THEN 1 ELSE 0 END) AS seen,
              COUNT(*) AS total
            FROM recommendations
            """
        ).fetchone()
    except sqlite3.OperationalError:
        rec_outcome_row = None

    total = int(rec_outcome_row["total"] or 0) if rec_outcome_row else 0
    liked = int(rec_outcome_row["liked"] or 0) if rec_outcome_row else 0
    dismissed = int(rec_outcome_row["dismissed"] or 0) if rec_outcome_row else 0
    seen = int(rec_outcome_row["seen"] or 0) if rec_outcome_row else 0

    avg_reaction = (sum(reaction_samples) / len(reaction_samples)) if reaction_samples else None
    med_reaction = _median(reaction_samples)

    summary = {
        "period_days": period_days,
        "since": cutoff,
        "event_count": len(period_events),
        "mode_breakdown": mode_breakdown,
        "source_breakdown": source_breakdown,
        "reaction_metrics": {
            "avg_reaction_ms": round(avg_reaction, 1) if avg_reaction is not None else None,
            "median_reaction_ms": round(med_reaction, 1) if med_reaction is not None else None,
        },
        "recommendation_outcomes": {
            "total": total,
            "seen": seen,
            "liked": liked,
            "dismissed": dismissed,
            "engagement_rate": round((liked / seen), 4) if seen > 0 else 0.0,
        },
        "top_positive_preferences": top_positive,
        "top_negative_preferences": top_negative,
    }

    next_actions: list[str] = []
    if len(source_breakdown) < 3:
        next_actions.append("Play Source Sprint to diversify lens coverage.")
    if sum(1 for x in top_positive if x["entity_type"] == "author") < 3:
        next_actions.append("Run Author Duel rounds to strengthen author preferences.")
    if avg_reaction is None:
        next_actions.append("Play Swipe/Triage to collect reaction telemetry.")
    if not next_actions:
        next_actions.append("Refresh recommendations and continue gameplay for incremental gains.")
    summary["next_actions"] = next_actions
    return summary


def _compute_streak(conn: sqlite3.Connection) -> int:
    """Compute the current consecutive-day streak of feedback."""
    try:
        rows = conn.execute(
            "SELECT DISTINCT DATE(created_at) as day FROM feedback_events ORDER BY day DESC"
        ).fetchall()
    except Exception:
        return 0

    if not rows:
        return 0

    today = datetime.utcnow().date()
    streak = 0
    for r in rows:
        try:
            day = datetime.strptime(r["day"], "%Y-%m-%d").date()
        except (ValueError, TypeError):
            break
        expected = today - timedelta(days=streak)
        if day == expected:
            streak += 1
        else:
            break

    return streak


def _parse_event_datetime(raw: Any) -> Optional[datetime]:
    """Parse feedback event timestamp from SQLite string formats."""
    if raw is None:
        return None
    s = str(raw).strip()
    if not s:
        return None
    if s.endswith("Z"):
        s = s[:-1]
    try:
        return datetime.fromisoformat(s)
    except ValueError:
        pass
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M:%S.%f"):
        try:
            return datetime.strptime(s, fmt)
        except ValueError:
            continue
    return None


def _safe_json_loads(raw: Any) -> Dict[str, Any]:
    if raw is None:
        return {}
    try:
        parsed = json.loads(raw)
        return parsed if isinstance(parsed, dict) else {}
    except Exception:
        return {}


def _iter_feedback_events(conn: sqlite3.Connection) -> list[dict]:
    """Load feedback events with parsed value/context payloads."""
    try:
        rows = conn.execute(
            """
            SELECT event_type, entity_type, entity_id, value, context_json, created_at
            FROM feedback_events
            ORDER BY created_at DESC
            """
        ).fetchall()
    except sqlite3.OperationalError:
        return []

    events: list[dict] = []
    for row in rows:
        value = _safe_json_loads(row["value"])
        context = _safe_json_loads(row["context_json"])
        events.append(
            {
                "event_type": row["event_type"],
                "entity_type": row["entity_type"],
                "entity_id": row["entity_id"],
                "value": value,
                "context": context,
                "created_at": row["created_at"],
                "dt": _parse_event_datetime(row["created_at"]),
            }
        )
    return events


def _event_xp(event: dict) -> int:
    """Return gamified XP for one feedback event.

    Single source of truth for XP per event_type (S-AUDIT-9 contract,
    2026-04-24). Active types each get an explicit branch; anything
    else — including legacy types (``triage_pick`` / ``method_match``
    / ``abstract_highlight``) and passive types (``external_link_click``
    / ``abstract_engagement`` / ``search_query``) — earns 0 XP so
    replays and passive signal don't inflate the level counter.
    """
    event_type = str(event.get("event_type") or "")
    value = event.get("value") or {}
    context = event.get("context") or {}

    if event_type == "swipe":
        base = _SWIPE_XP.get(str(value.get("choice") or ""), 0)
    elif event_type == "tier_sort":
        # Per-card XP scales with tier — S/A are higher-signal drops
        # than B/C but every drop still emits useful ordinal data, so
        # even the bottom tier earns baseline XP.
        tier = str(value.get("tier") or "").strip().upper()
        base = {"S": 14, "A": 10, "B": 6, "C": 8}.get(tier, 6)
    elif event_type == "rating":
        rating_val = value.get("rating")
        try:
            rating = float(rating_val)
            base = max(4, min(12, int(round(4 + rating * 1.6))))
        except (TypeError, ValueError):
            base = 8
    elif event_type == "topic_pref":
        base = 5
    elif event_type == "author_pref":
        base = 6
    elif event_type == "source_pref":
        base = 6
    elif event_type == "feed_action":
        # Small reward — passive-ish; scales with the action type.
        action = str(value.get("action") or "").strip().lower()
        base = {"add": 3, "like": 5, "love": 7, "dislike": 3}.get(action, 3)
    else:
        # Legacy / passive / unknown — zero XP to keep the ladder honest.
        return 0

    confidence_mult = 1.0
    try:
        confidence = float(value.get("confidence")) if value.get("confidence") is not None else None
        if confidence is not None:
            confidence_mult = max(0.8, min(1.35, 0.75 + 0.2 * confidence))
    except (TypeError, ValueError):
        confidence_mult = 1.0

    reaction_mult = _reaction_multiplier(context.get("reaction_ms"))
    xp = int(round(base * confidence_mult * reaction_mult))
    # Active-type floor of 1 prevents a borderline confidence/reaction
    # combination from leaving an active event with 0 XP.
    return max(1, xp)


def _xp_for_level(level: int) -> int:
    """Cumulative XP threshold required to reach ``level`` (1-indexed)."""
    if level <= 1:
        return 0
    n = level - 1
    return int((n * 120) + (n * n * 30))


def _level_from_xp(total_xp: int) -> tuple[int, int, int]:
    """Return (level, current_level_floor_xp, next_level_xp)."""
    xp = max(0, int(total_xp))
    level = 1
    while xp >= _xp_for_level(level + 1):
        level += 1
    return level, _xp_for_level(level), _xp_for_level(level + 1)


def _median(values: list[float]) -> Optional[float]:
    if not values:
        return None
    vals = sorted(values)
    mid = len(vals) // 2
    if len(vals) % 2 == 1:
        return vals[mid]
    return (vals[mid - 1] + vals[mid]) / 2.0


# ---------------------------------------------------------------------------
# Topic preferences
# ---------------------------------------------------------------------------

def get_topic_preferences(conn: sqlite3.Connection) -> List[dict]:
    """Get all topics with their preference state.

    Returns topic prefs from preference_profiles enriched with
    publication_topics data.
    """
    topics: Dict[str, dict] = {}

    # Seed from publication_topics (all known topics)
    try:
        rows = conn.execute(
            """SELECT term, COUNT(*) as paper_count
               FROM publication_topics
               GROUP BY term
               ORDER BY paper_count DESC
               LIMIT 50"""
        ).fetchall()
        for r in rows:
            term = (r["term"] or "").strip().lower()
            if term:
                topics[term] = {
                    "topic": term,
                    "paper_count": r["paper_count"],
                    "pref": "neutral",
                    "weight": 0.0,
                }
    except sqlite3.OperationalError:
        pass

    # Overlay preference_profiles
    try:
        pref_rows = conn.execute(
            """SELECT entity_id, affinity_weight
               FROM preference_profiles
               WHERE entity_type = 'topic'"""
        ).fetchall()
        for pr in pref_rows:
            eid = pr["entity_id"]
            w = pr["affinity_weight"]
            if eid in topics:
                topics[eid]["weight"] = round(w, 3)
                topics[eid]["pref"] = _weight_to_pref(w)
            else:
                topics[eid] = {
                    "topic": eid,
                    "paper_count": 0,
                    "pref": _weight_to_pref(w),
                    "weight": round(w, 3),
                }
    except sqlite3.OperationalError:
        pass

    return sorted(topics.values(), key=lambda t: -t["weight"])


def _weight_to_pref(w: float) -> str:
    """Map a numeric weight to a human pref label."""
    if w <= -2.0:
        return "mute"
    if w < -0.3:
        return "less"
    if w > 0.3:
        return "more"
    return "neutral"


def update_topic_preference(
    conn: sqlite3.Connection,
    topic: str,
    pref: str,
    context: Optional[Dict[str, Any]] = None,
) -> None:
    """Record a topic preference change (more/less/mute/neutral).

    Also creates a feedback event for the audit trail.
    """
    record_feedback(
        conn,
        event_type="topic_pref",
        entity_type="topic",
        entity_id=topic.strip().lower(),
        value={"pref": pref},
        context=context,
    )


# ---------------------------------------------------------------------------
# Preference-based scoring signal for discovery engine
# ---------------------------------------------------------------------------

def preload_preference_profile_maps(
    conn: sqlite3.Connection,
) -> Optional[Dict[str, Any]]:
    """One-pass prefetch for `get_preference_affinity_signal` hot-loop callers.

    D-AUDIT-10a (2026-04-24). `get_preference_affinity_signal` issues
    four DB round trips per candidate: (1) `SUM(interaction_count)`, (2)
    topic-affinity lookup, (3) `publication_authors` fetch, (4)
    author-affinity lookup, plus (5) source-affinity. Across 500
    candidates that's ~2 500 trips under the writer lock — a measurable
    tail on top of the scoring-loop topic work. This helper runs every
    read ONCE and hands back a bundle:

    - ``total`` — sum of `interaction_count` across the whole table.
    - ``profiles`` — `{(entity_type, entity_id): (affinity_weight,
      confidence)}` for every `preference_profiles` row.
    - ``authors_by_paper`` — `{paper_id: [display_name, ...]}` for a
      supplied set of paper_ids (passed separately for batching).

    Callers in the lens-refresh scoring loop pass this bundle via
    ``preloaded`` on every `get_preference_affinity_signal(...)` call;
    callers outside (ad-hoc scoring, tests) can omit it and fall back
    to the per-call DB path. Returns ``None`` when
    `preference_profiles` is missing or empty — keeps ``preloaded``
    compatibility for the `None`-guarded hot path in `score_candidate`.
    """
    try:
        row = conn.execute(
            "SELECT SUM(interaction_count) AS total FROM preference_profiles"
        ).fetchone()
    except sqlite3.OperationalError:
        return None
    total = int((row["total"] if row and row["total"] else 0) or 0)
    if total < MIN_INTERACTIONS_FOR_SCORING:
        return {"total": total, "profiles": {}, "authors_by_paper": {}}

    profiles: Dict[Tuple[str, str], Tuple[float, float]] = {}
    try:
        prof_rows = conn.execute(
            "SELECT entity_type, entity_id, affinity_weight, confidence "
            "FROM preference_profiles"
        ).fetchall()
        for r in prof_rows:
            etype = str(r["entity_type"] or "").strip()
            # Normalise entity_ids the same way the candidate lookup
            # does — lower-case topic/author terms, raw source keys.
            eid_raw = str(r["entity_id"] or "")
            eid = eid_raw.strip().lower() if etype in ("topic", "author") else eid_raw.strip()
            if not etype or not eid:
                continue
            profiles[(etype, eid)] = (
                float(r["affinity_weight"] or 0.0),
                float(r["confidence"] or 0.0),
            )
    except sqlite3.OperationalError:
        return {"total": total, "profiles": {}, "authors_by_paper": {}}

    return {"total": total, "profiles": profiles, "authors_by_paper": {}}


def preload_candidate_authors(
    conn: sqlite3.Connection,
    paper_ids: List[str],
) -> Dict[str, List[str]]:
    """Batch-fetch `publication_authors.display_name` for many papers.

    Pairs with `preload_preference_profile_maps` as part of D-AUDIT-10a.
    One `IN (?, ?, …)` query replaces the per-candidate `SELECT` inside
    `get_preference_affinity_signal`. Returns `{paper_id:
    [lower-cased display_name, …]}`; papers with no authorship rows
    are absent from the dict (caller falls back to the comma-separated
    `candidate.authors` string).
    """
    out: Dict[str, List[str]] = {}
    clean_ids = [pid for pid in (str(p).strip() for p in paper_ids) if pid]
    if not clean_ids:
        return out
    # Chunk to stay under SQLite's parameter ceiling (999).
    for chunk_start in range(0, len(clean_ids), 500):
        chunk = clean_ids[chunk_start:chunk_start + 500]
        placeholders = ",".join("?" * len(chunk))
        try:
            rows = conn.execute(
                f"SELECT paper_id, display_name FROM publication_authors "
                f"WHERE paper_id IN ({placeholders})",
                chunk,
            ).fetchall()
        except sqlite3.OperationalError:
            return out
        for row in rows:
            pid = str(row["paper_id"] or "").strip()
            name = str(row["display_name"] or "").strip().lower()
            if not pid or not name:
                continue
            out.setdefault(pid, []).append(name)
    return out


def get_preference_affinity_signal(
    conn: sqlite3.Connection,
    candidate: dict,
    *,
    preloaded: Optional[Dict[str, Any]] = None,
) -> float:
    """Compute a preference affinity score for a discovery candidate.

    Aggregates topic and author affinity from preference_profiles.
    Returns a value in [-1, 1] (normalised).

    Only produces a non-zero signal when the user has enough interactions
    (>= MIN_INTERACTIONS_FOR_SCORING).  Uses graduated volume scaling
    that ramps from 0.3 to 1.0 over 2-10 interactions.

    When called from `refresh_lens_recommendations`' scoring loop,
    `preloaded` should be the bundle returned by
    `preload_preference_profile_maps` (plus `authors_by_paper` from
    `preload_candidate_authors`). The hot path then does dict lookups
    only — no DB round trips per candidate. Callers outside the loop
    can omit `preloaded` and pay the legacy per-call cost.
    """
    # Check total interaction count (from preload when available).
    if preloaded is not None:
        total = int(preloaded.get("total") or 0)
    else:
        try:
            row = conn.execute(
                "SELECT SUM(interaction_count) as total FROM preference_profiles"
            ).fetchone()
            total = int(row["total"] if row and row["total"] else 0)
        except sqlite3.OperationalError:
            return 0.0

    if total < MIN_INTERACTIONS_FOR_SCORING:
        return 0.0

    # Graduated volume scaling: ramps 0.3→1.0 over 2-10 interactions
    volume_scale = min(1.0, 0.3 + (total - 2) * 0.0875)

    score = 0.0
    components = 0

    profiles_map: Optional[Dict[Tuple[str, str], Tuple[float, float]]] = (
        preloaded.get("profiles") if preloaded is not None else None
    )

    # Topic signal
    title = (candidate.get("title") or candidate.get("recommended_title") or "").strip().lower()
    topics = candidate.get("topics", [])
    topic_terms = [t.get("term", "").lower() for t in topics if isinstance(t, dict)]
    if not topic_terms and title:
        # Fallback: words from title as pseudo-topics
        topic_terms = [w.strip(".,;:!?()[]{}\"'") for w in title.split() if len(w) > 3]

    if topic_terms:
        if profiles_map is not None:
            for term in topic_terms:
                hit = profiles_map.get(("topic", term))
                if hit is not None:
                    score += hit[0] * hit[1]
                    components += 1
        else:
            placeholders = ",".join("?" * len(topic_terms))
            try:
                rows = conn.execute(
                    f"SELECT entity_id, affinity_weight, confidence "
                    f"FROM preference_profiles "
                    f"WHERE entity_type = 'topic' AND entity_id IN ({placeholders})",
                    topic_terms,
                ).fetchall()
                for r in rows:
                    score += r["affinity_weight"] * r["confidence"]
                    components += 1
            except sqlite3.OperationalError:
                pass

    # Author signal — prefer structured publication_authors table
    paper_id = candidate.get("id") or candidate.get("paper_id") or ""
    author_names: list[str] = []
    if paper_id:
        if preloaded is not None:
            author_names = list(preloaded.get("authors_by_paper", {}).get(paper_id, []))
        else:
            try:
                pa_rows = conn.execute(
                    "SELECT display_name FROM publication_authors WHERE paper_id = ?",
                    (paper_id,),
                ).fetchall()
                author_names = [r["display_name"].strip().lower() for r in pa_rows if r["display_name"]]
            except (sqlite3.OperationalError, AttributeError):
                pass

    if not author_names:
        # Fallback to comma-separated authors string
        authors_str = (candidate.get("authors") or candidate.get("recommended_authors") or "").strip()
        if authors_str:
            author_names = [a.strip().lower() for a in authors_str.split(",") if a.strip()]

    if author_names:
        if profiles_map is not None:
            for name in author_names:
                hit = profiles_map.get(("author", name))
                if hit is not None:
                    score += hit[0] * hit[1]
                    components += 1
        else:
            placeholders = ",".join("?" * len(author_names))
            try:
                rows = conn.execute(
                    f"SELECT entity_id, affinity_weight, confidence "
                    f"FROM preference_profiles "
                    f"WHERE entity_type = 'author' AND entity_id IN ({placeholders})",
                    author_names,
                ).fetchall()
                for r in rows:
                    score += r["affinity_weight"] * r["confidence"]
                    components += 1
            except sqlite3.OperationalError:
                pass

    # Source signal
    source_key = str(candidate.get("source_key") or "").strip()
    source_type = str(candidate.get("source_type") or "").strip()
    source_entity = source_key or source_type
    if source_entity:
        if profiles_map is not None:
            hit = profiles_map.get(("source", source_entity))
            if hit is not None:
                score += hit[0] * hit[1]
                components += 1
        else:
            try:
                row = conn.execute(
                    """
                    SELECT affinity_weight, confidence
                    FROM preference_profiles
                    WHERE entity_type = 'source' AND entity_id = ?
                    """,
                    (source_entity,),
                ).fetchone()
                if row:
                    score += float(row["affinity_weight"]) * float(row["confidence"])
                    components += 1
            except sqlite3.OperationalError:
                pass

    if components == 0:
        return 0.0

    # Normalise to [-1, 1] and apply volume scaling
    avg = score / components
    return max(-1.0, min(1.0, avg * volume_scale))
