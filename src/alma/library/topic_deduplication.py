"""Canonical topic identity and synonym deduplication.

Normalizes raw topic strings from OpenAlex into canonical forms, groups
synonyms together, and optionally uses AI embeddings to propose additional
merges.  The deterministic pass (``build_canonical_topics``) handles:

- Unicode NFKD normalisation + lowercase + punctuation stripping
- Acronym expansion via a built-in dictionary
- Exact-match grouping of normalized forms
- Token-overlap similarity for fuzzy merge candidates

The optional AI pass (``find_ai_merge_candidates``) uses the configured
embedding provider to compute cosine similarity between canonical topics
and surface near-duplicates for manual review.
"""

from __future__ import annotations

import hashlib
import logging
import re
import sqlite3
import unicodedata
from collections import Counter, defaultdict
from datetime import datetime
from typing import Optional

logger = logging.getLogger(__name__)

# ============================================================================
# Built-in acronym map (bidirectional)
# ============================================================================

_ACRONYM_MAP: dict[str, str] = {
    "nlp": "natural language processing",
    "ml": "machine learning",
    "dl": "deep learning",
    "cv": "computer vision",
    "rl": "reinforcement learning",
    "ai": "artificial intelligence",
    "nn": "neural network",
    "nns": "neural networks",
    "cnn": "convolutional neural network",
    "cnns": "convolutional neural networks",
    "rnn": "recurrent neural network",
    "rnns": "recurrent neural networks",
    "lstm": "long short term memory",
    "gan": "generative adversarial network",
    "gans": "generative adversarial networks",
    "llm": "large language model",
    "llms": "large language models",
    "iot": "internet of things",
    "hci": "human computer interaction",
    "ir": "information retrieval",
    "fmri": "functional magnetic resonance imaging",
    "eeg": "electroencephalography",
    "mri": "magnetic resonance imaging",
    "gwas": "genome wide association study",
    "pca": "principal component analysis",
    "svm": "support vector machine",
    "svms": "support vector machines",
    "gnn": "graph neural network",
    "gnns": "graph neural networks",
    "vae": "variational autoencoder",
    "vaes": "variational autoencoders",
    "nlg": "natural language generation",
    "nlu": "natural language understanding",
    "ner": "named entity recognition",
    "ocr": "optical character recognition",
    "bert": "bidirectional encoder representations from transformers",
    "gpt": "generative pre trained transformer",
}

# Build reverse map: full phrase -> acronym (for normalization)
_REVERSE_ACRONYM_MAP: dict[str, str] = {v: k for k, v in _ACRONYM_MAP.items()}


# ============================================================================
# Deterministic normalization
# ============================================================================


def normalize_topic(term: str) -> str:
    """Normalize a raw topic string to a canonical form.

    Steps:
    1. Unicode NFKD decomposition + strip combining characters
    2. Lowercase
    3. Strip punctuation (keep alphanumeric, spaces, hyphens)
    4. Collapse whitespace
    5. Normalize common separators (hyphens, underscores → spaces)
    6. Expand known acronyms to full form for grouping

    Args:
        term: Raw topic string from OpenAlex or user input.

    Returns:
        Normalized string suitable for exact-match grouping.
    """
    if not term:
        return ""

    # 1. Unicode NFKD + strip combining marks
    s = unicodedata.normalize("NFKD", term)
    s = "".join(c for c in s if not unicodedata.combining(c))

    # 2. Lowercase
    s = s.lower()

    # 3. Strip punctuation except hyphens and apostrophes (keep word structure)
    s = re.sub(r"[^\w\s\-]", " ", s)

    # 4. Normalize separators: hyphens and underscores to spaces
    s = s.replace("-", " ").replace("_", " ")

    # 5. Collapse whitespace
    s = " ".join(s.split()).strip()

    if not s:
        return ""

    # 6. Expand known acronyms if the entire string is an acronym
    if s in _ACRONYM_MAP:
        s = _ACRONYM_MAP[s]

    return s


def _topic_id_from_normalized(normalized: str) -> str:
    """Generate a stable topic_id from a normalized name."""
    h = hashlib.sha1(normalized.encode("utf-8")).hexdigest()[:16]
    return f"topic_{h}"


def _token_set(text: str) -> set[str]:
    """Split normalized text into token set for similarity."""
    return {t for t in text.split() if len(t) >= 2}


def token_overlap_similarity(a: str, b: str) -> float:
    """Compute Jaccard-like token overlap between two normalized strings.

    Returns a value in [0, 1].
    """
    tokens_a = _token_set(a)
    tokens_b = _token_set(b)
    if not tokens_a or not tokens_b:
        return 0.0
    intersection = tokens_a & tokens_b
    union = tokens_a | tokens_b
    return len(intersection) / len(union) if union else 0.0


# ============================================================================
# Build canonical topics from existing data
# ============================================================================


def build_canonical_topics(conn: sqlite3.Connection) -> dict:
    """Scan publication_topics, normalize, group, and populate topics + topic_aliases.

    This is the deterministic dedup pass.  It:
    1. Reads all distinct ``term`` values from ``publication_topics``.
    2. Normalizes each term.
    3. Groups terms by normalized form.
    4. Picks the most common raw form as the canonical name.
    5. Inserts into ``topics`` and ``topic_aliases``.
    6. Links ``publication_topics.topic_id`` to the canonical topic.

    Args:
        conn: Active SQLite connection.

    Returns:
        Summary dict with counts.
    """
    _ensure_topic_tables(conn)

    # 1. Read all distinct terms
    try:
        rows = conn.execute(
            "SELECT term, COUNT(*) as cnt FROM publication_topics GROUP BY term"
        ).fetchall()
    except sqlite3.OperationalError:
        return {"topics_created": 0, "aliases_created": 0, "links_updated": 0}

    if not rows:
        return {"topics_created": 0, "aliases_created": 0, "links_updated": 0}

    # 2. Normalize and group
    # normalized_form -> {raw_term: count}
    groups: dict[str, dict[str, int]] = defaultdict(dict)
    for row in rows:
        raw = (row["term"] or "").strip()
        if not raw:
            continue
        normalized = normalize_topic(raw)
        if not normalized:
            continue
        groups[normalized][raw] = row["cnt"]

    topics_created = 0
    aliases_created = 0
    links_updated = 0

    for normalized, raw_counts in groups.items():
        # Pick the most common raw form as canonical name
        canonical_name = max(raw_counts, key=lambda k: raw_counts[k])
        topic_id = _topic_id_from_normalized(normalized)

        # Upsert into topics table
        conn.execute(
            """INSERT INTO topics (topic_id, canonical_name, normalized_name, source, created_at)
               VALUES (?, ?, ?, 'auto', ?)
               ON CONFLICT(topic_id) DO UPDATE SET
                   canonical_name = excluded.canonical_name,
                   normalized_name = excluded.normalized_name""",
            (topic_id, canonical_name, normalized, datetime.utcnow().isoformat()),
        )
        topics_created += 1

        # Insert aliases for all raw forms
        for raw_term in raw_counts:
            raw_normalized = normalize_topic(raw_term)
            try:
                conn.execute(
                    """INSERT INTO topic_aliases
                       (topic_id, raw_term, normalized_term, source, confidence, created_at)
                       VALUES (?, ?, ?, 'auto', 1.0, ?)
                       ON CONFLICT(normalized_term) DO UPDATE SET
                           topic_id = excluded.topic_id,
                           raw_term = excluded.raw_term""",
                    (topic_id, raw_term, raw_normalized, datetime.utcnow().isoformat()),
                )
                aliases_created += 1
            except sqlite3.IntegrityError:
                pass

        # Link publication_topics rows
        for raw_term in raw_counts:
            updated = conn.execute(
                """UPDATE publication_topics SET topic_id = ?
                   WHERE term = ? AND (topic_id IS NULL OR topic_id != ?)""",
                (topic_id, raw_term, topic_id),
            ).rowcount
            links_updated += updated or 0

    # Also migrate the legacy topic_aliases table data if it has the old schema
    _migrate_legacy_aliases(conn)

    conn.commit()
    return {
        "topics_created": topics_created,
        "aliases_created": aliases_created,
        "links_updated": links_updated,
    }


def _migrate_legacy_aliases(conn: sqlite3.Connection) -> int:
    """Migrate data from old-format topic_aliases (alias_term, canonical_term)
    into the new schema if it exists alongside the new table."""
    migrated = 0
    try:
        cols = [r[1] for r in conn.execute("PRAGMA table_info(topic_aliases)").fetchall()]
        if "alias_term" in cols and "topic_id" not in cols:
            # Old schema -- read and migrate
            old_rows = conn.execute(
                "SELECT alias_term, canonical_term FROM topic_aliases"
            ).fetchall()
            # Drop and recreate
            conn.execute("DROP TABLE topic_aliases")
            _ensure_topic_tables(conn)
            for row in old_rows:
                raw = (row["alias_term"] or "").strip()
                canonical = (row["canonical_term"] or "").strip()
                if not raw or not canonical:
                    continue
                normalized = normalize_topic(raw)
                canonical_normalized = normalize_topic(canonical)
                topic_id = _topic_id_from_normalized(canonical_normalized)
                # Ensure the canonical topic exists
                conn.execute(
                    """INSERT OR IGNORE INTO topics
                       (topic_id, canonical_name, normalized_name, source, created_at)
                       VALUES (?, ?, ?, 'auto', ?)""",
                    (topic_id, canonical, canonical_normalized, datetime.utcnow().isoformat()),
                )
                conn.execute(
                    """INSERT OR IGNORE INTO topic_aliases
                       (topic_id, raw_term, normalized_term, source, confidence, created_at)
                       VALUES (?, ?, ?, 'auto', 1.0, ?)""",
                    (topic_id, raw, normalized, datetime.utcnow().isoformat()),
                )
                migrated += 1
    except sqlite3.OperationalError:
        pass
    return migrated


# ============================================================================
# Fuzzy similarity candidates
# ============================================================================


def find_similar_topics(
    conn: sqlite3.Connection,
    threshold: float = 0.85,
) -> list[dict]:
    """Find pairs of canonical topics with high token-overlap similarity.

    Returns a list of merge candidate dicts:
    ``{"topic_a": ..., "topic_b": ..., "similarity": float, "source": "token_overlap"}``

    Args:
        conn: Active SQLite connection.
        threshold: Minimum Jaccard similarity to include.

    Returns:
        List of merge candidate dicts, sorted by similarity descending.
    """
    _ensure_topic_tables(conn)

    rows = conn.execute(
        "SELECT topic_id, canonical_name, normalized_name FROM topics"
    ).fetchall()

    if len(rows) < 2:
        return []

    topics = [(r["topic_id"], r["canonical_name"], r["normalized_name"]) for r in rows]
    candidates = []

    # Also check acronym expansions
    for i in range(len(topics)):
        for j in range(i + 1, len(topics)):
            tid_a, name_a, norm_a = topics[i]
            tid_b, name_b, norm_b = topics[j]

            sim = token_overlap_similarity(norm_a, norm_b)

            # Check if one is an acronym of the other
            if sim < threshold:
                expanded_a = _ACRONYM_MAP.get(norm_a, norm_a)
                expanded_b = _ACRONYM_MAP.get(norm_b, norm_b)
                if expanded_a != norm_a or expanded_b != norm_b:
                    sim = max(sim, token_overlap_similarity(expanded_a, expanded_b))

            if sim >= threshold:
                candidates.append({
                    "topic_a": {"topic_id": tid_a, "canonical_name": name_a},
                    "topic_b": {"topic_id": tid_b, "canonical_name": name_b},
                    "similarity": round(sim, 4),
                    "source": "token_overlap",
                })

    candidates.sort(key=lambda c: c["similarity"], reverse=True)
    return candidates


# ============================================================================
# AI-assisted dedup
# ============================================================================


def find_ai_merge_candidates(
    conn: sqlite3.Connection,
    threshold: float = 0.85,
    max_pairs: int = 50,
) -> list[dict]:
    """Use embedding similarity to find additional alias candidates.

    Only proposes merges (does not auto-apply).  Results include
    ``source='ai'`` and a confidence score.

    Args:
        conn: Active SQLite connection.
        threshold: Minimum cosine similarity to propose.
        max_pairs: Maximum number of pairs to return.

    Returns:
        List of merge candidate dicts.
    """
    _ensure_topic_tables(conn)

    rows = conn.execute(
        "SELECT topic_id, canonical_name, normalized_name FROM topics"
    ).fetchall()

    if len(rows) < 2:
        return []

    # Try to get the embedding provider
    try:
        from alma.ai.providers import get_active_provider

        provider = get_active_provider(conn)
        if provider is None:
            logger.info("AI provider not configured; skipping AI dedup")
            return []
    except Exception as e:
        logger.info("AI provider not available for topic dedup: %s", e)
        return []

    topics = [(r["topic_id"], r["canonical_name"], r["normalized_name"]) for r in rows]
    texts = [t[1] for t in topics]  # use canonical names for embedding

    try:
        embeddings = provider.embed(texts)
    except Exception as e:
        logger.warning("Failed to compute topic embeddings: %s", e)
        return []

    if not embeddings or len(embeddings) != len(topics):
        return []

    # Compute pairwise cosine similarities
    import numpy as np

    emb_matrix = np.array(embeddings)
    # Normalize
    norms = np.linalg.norm(emb_matrix, axis=1, keepdims=True)
    norms = np.where(norms == 0, 1, norms)
    emb_matrix = emb_matrix / norms

    candidates = []
    for i in range(len(topics)):
        for j in range(i + 1, len(topics)):
            sim = float(np.dot(emb_matrix[i], emb_matrix[j]))
            if sim >= threshold:
                candidates.append({
                    "topic_a": {"topic_id": topics[i][0], "canonical_name": topics[i][1]},
                    "topic_b": {"topic_id": topics[j][0], "canonical_name": topics[j][1]},
                    "similarity": round(sim, 4),
                    "source": "ai",
                })

    candidates.sort(key=lambda c: c["similarity"], reverse=True)
    return candidates[:max_pairs]


# ============================================================================
# Merge operations
# ============================================================================


def merge_topics(
    conn: sqlite3.Connection,
    keep_topic_id: str,
    merge_topic_id: str,
) -> dict:
    """Merge one canonical topic into another.

    Moves all aliases and publication_topics references from
    ``merge_topic_id`` to ``keep_topic_id``, then deletes the merged topic.

    Args:
        conn: Active SQLite connection.
        keep_topic_id: The topic to keep.
        merge_topic_id: The topic to merge into keep_topic_id.

    Returns:
        Summary dict.
    """
    _ensure_topic_tables(conn)

    # Verify both exist
    keep = conn.execute(
        "SELECT * FROM topics WHERE topic_id = ?", (keep_topic_id,)
    ).fetchone()
    merge = conn.execute(
        "SELECT * FROM topics WHERE topic_id = ?", (merge_topic_id,)
    ).fetchone()

    if not keep:
        return {"error": f"Topic {keep_topic_id} not found"}
    if not merge:
        return {"error": f"Topic {merge_topic_id} not found"}

    # Move aliases
    aliases_moved = conn.execute(
        "UPDATE topic_aliases SET topic_id = ? WHERE topic_id = ?",
        (keep_topic_id, merge_topic_id),
    ).rowcount or 0

    # Add the merged topic's canonical name as an alias
    merge_normalized = normalize_topic(merge["canonical_name"])
    try:
        conn.execute(
            """INSERT OR IGNORE INTO topic_aliases
               (topic_id, raw_term, normalized_term, source, confidence, created_at)
               VALUES (?, ?, ?, 'manual', 1.0, ?)""",
            (keep_topic_id, merge["canonical_name"], merge_normalized,
             datetime.utcnow().isoformat()),
        )
    except sqlite3.IntegrityError:
        pass

    # Move publication_topics references
    links_moved = conn.execute(
        "UPDATE publication_topics SET topic_id = ? WHERE topic_id = ?",
        (keep_topic_id, merge_topic_id),
    ).rowcount or 0

    # Delete the merged topic
    conn.execute("DELETE FROM topics WHERE topic_id = ?", (merge_topic_id,))

    conn.commit()
    return {
        "kept": keep_topic_id,
        "merged": merge_topic_id,
        "aliases_moved": aliases_moved,
        "links_moved": links_moved,
    }


# ============================================================================
# Lookup helper for enrichment pipeline
# ============================================================================


def resolve_topic_id(conn: sqlite3.Connection, raw_term: str) -> Optional[str]:
    """Look up the canonical topic_id for a raw term.

    Checks the topic_aliases table for a matching normalized term.

    Args:
        conn: Active SQLite connection.
        raw_term: Raw topic string.

    Returns:
        The topic_id if found, None otherwise.
    """
    normalized = normalize_topic(raw_term)
    if not normalized:
        return None

    try:
        row = conn.execute(
            "SELECT topic_id FROM topic_aliases WHERE normalized_term = ?",
            (normalized,),
        ).fetchone()
        if row:
            return row["topic_id"]
    except sqlite3.OperationalError:
        pass

    return None


# ============================================================================
# Schema helpers
# ============================================================================


def _ensure_topic_tables(conn: sqlite3.Connection) -> None:
    """Ensure the topics and topic_aliases tables exist with the new schema."""
    conn.execute(
        """CREATE TABLE IF NOT EXISTS topics (
            topic_id TEXT PRIMARY KEY,
            canonical_name TEXT NOT NULL,
            normalized_name TEXT NOT NULL UNIQUE,
            source TEXT DEFAULT 'auto',
            created_at TEXT DEFAULT (datetime('now'))
        )"""
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_topics_normalized ON topics(normalized_name)"
    )

    # Check if topic_aliases has the new schema
    try:
        cols = [r[1] for r in conn.execute("PRAGMA table_info(topic_aliases)").fetchall()]
        if cols and "topic_id" not in cols:
            # Old schema -- will be migrated by _migrate_legacy_aliases
            return
    except sqlite3.OperationalError:
        pass

    conn.execute(
        """CREATE TABLE IF NOT EXISTS topic_aliases (
            alias_id INTEGER PRIMARY KEY AUTOINCREMENT,
            topic_id TEXT NOT NULL REFERENCES topics(topic_id),
            raw_term TEXT NOT NULL,
            normalized_term TEXT NOT NULL,
            source TEXT DEFAULT 'auto',
            confidence REAL DEFAULT 1.0,
            created_at TEXT DEFAULT (datetime('now')),
            UNIQUE(normalized_term)
        )"""
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_topic_aliases_topic ON topic_aliases(topic_id)"
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_topic_aliases_normalized ON topic_aliases(normalized_term)"
    )

    # Ensure publication_topics has topic_id column
    try:
        pt_cols = [r[1] for r in conn.execute("PRAGMA table_info(publication_topics)").fetchall()]
        if "topic_id" in pt_cols:
            return
        conn.execute("ALTER TABLE publication_topics ADD COLUMN topic_id TEXT")
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_pub_topics_topic_id ON publication_topics(topic_id)"
        )
    except sqlite3.OperationalError:
        pass
