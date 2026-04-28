"""Tag suggestion system for publications.

Generates tag suggestions using two strategies:
1. Embedding similarity: find nearest tagged papers via cosine similarity,
   propagate their tags weighted by similarity score.
2. TF-IDF / topic fallback: use topic overlap from publication_topics
   table and text similarity via scikit-learn TfidfVectorizer.

Suggestions can be generated for individual papers or in bulk for all
untagged publications.
"""

import logging
import sqlite3
from datetime import datetime
from typing import Optional

logger = logging.getLogger(__name__)

# Defensive imports for optional dependencies
try:
    import numpy as np

    _NUMPY_AVAILABLE = True
except ImportError:
    np = None  # type: ignore[assignment]
    _NUMPY_AVAILABLE = False

try:
    from sklearn.feature_extraction.text import TfidfVectorizer
    from sklearn.metrics.pairwise import cosine_similarity as sklearn_cosine_similarity

    _SKLEARN_AVAILABLE = True
except ImportError:
    _SKLEARN_AVAILABLE = False


def _cosine_similarity_np(a: "np.ndarray", b: "np.ndarray") -> float:
    """Compute cosine similarity between two 1-D numpy vectors."""
    norm_a = np.linalg.norm(a)
    norm_b = np.linalg.norm(b)
    if norm_a == 0.0 or norm_b == 0.0:
        return 0.0
    return float(np.dot(a, b) / (norm_a * norm_b))


def _get_existing_tags(
    paper_id: str, conn: sqlite3.Connection,
) -> set[str]:
    """Return the set of tag IDs already assigned to a publication."""
    try:
        rows = conn.execute(
            "SELECT tag_id FROM publication_tags WHERE paper_id = ?",
            (paper_id,),
        ).fetchall()
        return {row["tag_id"] for row in rows}
    except sqlite3.OperationalError:
        return set()


def _get_existing_suggestions(
    paper_id: str, conn: sqlite3.Connection,
) -> set[str]:
    """Return the set of tags already suggested for a publication."""
    try:
        rows = conn.execute(
            "SELECT tag FROM tag_suggestions WHERE paper_id = ?",
            (paper_id,),
        ).fetchall()
        return {row["tag"] for row in rows}
    except sqlite3.OperationalError:
        return set()


def _embedding_based_suggestions(
    paper_id: str,
    conn: sqlite3.Connection,
    max_tags: int = 5,
) -> list[dict]:
    """Suggest tags by finding nearest tagged papers via embedding cosine similarity.

    Loads the target paper's embedding, compares against all tagged papers'
    embeddings, and propagates tags weighted by similarity score.

    Args:
        paper_id: UUID of the target paper.
        conn: Open SQLite connection to the publications database.
        max_tags: Maximum number of tag suggestions to return.

    Returns:
        List of suggestion dicts: {tag, tag_id, confidence, source}.
    """
    if not _NUMPY_AVAILABLE:
        return []

    from alma.discovery.similarity import get_active_embedding_model

    active_model = get_active_embedding_model(conn)

    try:
        target_row = conn.execute(
            "SELECT embedding FROM publication_embeddings "
            "WHERE paper_id = ? AND model = ?",
            (paper_id, active_model),
        ).fetchone()
    except sqlite3.OperationalError:
        return []

    if target_row is None:
        return []

    from alma.core.vector_blob import decode_vector
    target_emb = decode_vector(target_row["embedding"])
    target_norm = np.linalg.norm(target_emb)
    if target_norm == 0.0:
        return []
    target_emb = target_emb / target_norm

    # Only compare against tagged papers that also have an embedding
    # under the active model, so target and candidate vectors share the
    # same dimensionality.
    try:
        tagged_rows = conn.execute(
            """
            SELECT DISTINCT pt.tag_id, t.name AS tag_name,
                   pe.embedding, pe.paper_id
            FROM publication_tags pt
            JOIN tags t ON t.id = pt.tag_id
            JOIN publication_embeddings pe
              ON pe.paper_id = pt.paper_id AND pe.model = ?
            WHERE COALESCE(pt.paper_id, '') != ''
            """,
            (active_model,),
        ).fetchall()
    except sqlite3.OperationalError as exc:
        logger.debug("Could not query tagged papers with embeddings: %s", exc)
        return []

    if not tagged_rows:
        return []

    # Get tags already assigned to avoid re-suggesting
    existing_tag_ids = _get_existing_tags(paper_id, conn)

    # Compute similarity and accumulate tag scores
    tag_scores: dict[str, float] = {}  # tag_id -> accumulated weighted score
    tag_names: dict[str, str] = {}  # tag_id -> tag name
    tag_counts: dict[str, int] = {}  # tag_id -> number of contributing papers

    for row in tagged_rows:
        tag_id = row["tag_id"]
        tag_name = row["tag_name"]

        # Skip tags already assigned
        if tag_id in existing_tag_ids:
            continue

        try:
            emb = decode_vector(row["embedding"])
            emb_norm = np.linalg.norm(emb)
            if emb_norm == 0.0:
                continue
            emb = emb / emb_norm
            sim = float(np.dot(target_emb, emb))
        except Exception:
            continue

        if sim <= 0.0:
            continue

        tag_scores[tag_id] = tag_scores.get(tag_id, 0.0) + sim
        tag_names[tag_id] = tag_name
        tag_counts[tag_id] = tag_counts.get(tag_id, 0) + 1

    if not tag_scores:
        return []

    # Normalize scores: average similarity per tag, then scale to [0, 1]
    suggestions: list[dict] = []
    for tag_id, total_sim in tag_scores.items():
        count = tag_counts[tag_id]
        avg_sim = total_sim / count
        confidence = min(1.0, max(0.0, avg_sim))

        suggestions.append({
            "tag": tag_names[tag_id],
            "tag_id": tag_id,
            "confidence": round(confidence, 4),
            "source": "embedding",
        })

    # Sort by confidence descending
    suggestions.sort(key=lambda x: x["confidence"], reverse=True)
    return suggestions[:max_tags]


def _tfidf_based_suggestions(
    paper_id: str,
    conn: sqlite3.Connection,
    max_tags: int = 5,
) -> list[dict]:
    """Suggest tags using topic overlap and TF-IDF text similarity.

    Uses the publication_topics table for topic-based matching and
    scikit-learn TfidfVectorizer for text similarity against tagged papers.

    Args:
        paper_id: UUID of the target paper.
        conn: Open SQLite connection to the publications database.
        max_tags: Maximum number of tag suggestions to return.

    Returns:
        List of suggestion dicts: {tag, tag_id, confidence, source}.
    """
    # Get target publication text
    pub_row = conn.execute(
        "SELECT title, abstract FROM papers WHERE id = ?",
        (paper_id,),
    ).fetchone()

    if pub_row is None:
        return []

    target_text = f"{pub_row['title'] or ''} {pub_row['abstract'] or ''}".strip()
    if not target_text:
        return []

    # Get existing tags to avoid re-suggesting
    existing_tag_ids = _get_existing_tags(paper_id, conn)

    suggestions: list[dict] = []

    # Strategy A: Topic overlap from publication_topics
    try:
        # Get topics for target paper
        target_topics = conn.execute(
            "SELECT term, score FROM publication_topics WHERE paper_id = ?",
            (paper_id,),
        ).fetchall()

        if target_topics:
            target_topic_map = {
                row["term"].lower(): row["score"]
                for row in target_topics
            }

            # Find tagged papers with overlapping topics
            tagged_rows = conn.execute(
                """
                SELECT DISTINCT pt.tag_id, t.name AS tag_name,
                       p.id AS paper_id
                FROM publication_tags pt
                JOIN tags t ON t.id = pt.tag_id
                JOIN papers p ON p.id = pt.paper_id
                WHERE COALESCE(pt.paper_id, '') != ''
                """
            ).fetchall()

            tag_overlap_scores: dict[str, float] = {}
            tag_names: dict[str, str] = {}

            for trow in tagged_rows:
                tag_id = trow["tag_id"]
                if tag_id in existing_tag_ids:
                    continue

                tag_names[tag_id] = trow["tag_name"]

                # Get topics for this tagged paper
                other_topics = conn.execute(
                    "SELECT term, score FROM publication_topics WHERE paper_id = ?",
                    (trow["paper_id"],),
                ).fetchall()

                if not other_topics:
                    continue

                # Compute weighted topic overlap
                overlap = 0.0
                max_possible = 0.0
                for ot in other_topics:
                    term = ot["term"].lower()
                    ot_score = ot["score"]
                    if term in target_topic_map:
                        overlap += target_topic_map[term] * ot_score
                    max_possible += ot_score

                if max_possible > 0:
                    normalized = overlap / max_possible
                    current = tag_overlap_scores.get(tag_id, 0.0)
                    tag_overlap_scores[tag_id] = max(current, normalized)

            for tag_id, score in tag_overlap_scores.items():
                if score > 0.05:
                    suggestions.append({
                        "tag": tag_names[tag_id],
                        "tag_id": tag_id,
                        "confidence": round(min(1.0, score), 4),
                        "source": "topic",
                    })
    except sqlite3.OperationalError as exc:
        logger.debug("Topic-based suggestion query failed: %s", exc)

    # Strategy B: TF-IDF text similarity
    if _SKLEARN_AVAILABLE:
        try:
            tagged_pubs = conn.execute(
                """
                SELECT DISTINCT pt.tag_id, t.name AS tag_name,
                       p.title, p.abstract
                FROM publication_tags pt
                JOIN tags t ON t.id = pt.tag_id
                JOIN papers p ON p.id = pt.paper_id
                WHERE COALESCE(pt.paper_id, '') != ''
                """
            ).fetchall()

            if tagged_pubs:
                # Build texts for TF-IDF
                tagged_texts: list[str] = []
                tagged_info: list[tuple[str, str]] = []  # (tag_id, tag_name)

                for tp in tagged_pubs:
                    if tp["tag_id"] in existing_tag_ids:
                        continue
                    text = f"{tp['title'] or ''} {tp['abstract'] or ''}".strip()
                    if text:
                        tagged_texts.append(text)
                        tagged_info.append((tp["tag_id"], tp["tag_name"]))

                if tagged_texts:
                    all_texts = [target_text] + tagged_texts
                    vectorizer = TfidfVectorizer(
                        max_features=5000,
                        stop_words="english",
                        min_df=1,
                        max_df=0.95,
                        sublinear_tf=True,
                    )
                    try:
                        tfidf_matrix = vectorizer.fit_transform(all_texts)
                        target_vec = tfidf_matrix[0:1]
                        tagged_vecs = tfidf_matrix[1:]

                        sims = sklearn_cosine_similarity(target_vec, tagged_vecs).flatten()

                        tfidf_tag_scores: dict[str, float] = {}
                        tfidf_tag_names: dict[str, str] = {}

                        for idx, (tag_id, tag_name) in enumerate(tagged_info):
                            sim = float(sims[idx])
                            if sim > 0.05:
                                current = tfidf_tag_scores.get(tag_id, 0.0)
                                tfidf_tag_scores[tag_id] = max(current, sim)
                                tfidf_tag_names[tag_id] = tag_name

                        for tag_id, score in tfidf_tag_scores.items():
                            suggestions.append({
                                "tag": tfidf_tag_names[tag_id],
                                "tag_id": tag_id,
                                "confidence": round(min(1.0, score), 4),
                                "source": "tfidf",
                            })
                    except ValueError:
                        pass

        except sqlite3.OperationalError as exc:
            logger.debug("TF-IDF suggestion query failed: %s", exc)

    if not suggestions:
        return []

    # Deduplicate: keep highest confidence per tag_id
    best: dict[str, dict] = {}
    for s in suggestions:
        tid = s["tag_id"]
        if tid not in best or s["confidence"] > best[tid]["confidence"]:
            best[tid] = s

    result = sorted(best.values(), key=lambda x: x["confidence"], reverse=True)
    return result[:max_tags]


def suggest_tags(
    paper_id: str,
    conn: sqlite3.Connection,
    max_tags: int = 5,
) -> list[dict]:
    """Suggest up to max_tags tags for a publication.

    Strategy 1 (with embeddings): find nearest tagged papers via cosine
    similarity, propagate their tags weighted by similarity.

    Strategy 2 (without embeddings): use topic overlap from
    publication_topics table + TF-IDF similarity.

    Each suggestion contains:
        - tag: str -- tag display name
        - tag_id: str -- tag UUID
        - confidence: float -- score between 0 and 1
        - source: str -- 'embedding', 'tfidf', or 'topic'

    Args:
        paper_id: UUID of the target paper.
        conn: Open SQLite connection to the publications database.
        max_tags: Maximum number of suggestions.

    Returns:
        List of tag suggestion dicts, sorted by confidence descending.
    """
    # Try embedding-based suggestions first
    suggestions = _embedding_based_suggestions(paper_id, conn, max_tags)

    if len(suggestions) < max_tags:
        # Supplement with TF-IDF / topic-based suggestions
        remaining = max_tags - len(suggestions)
        fallback = _tfidf_based_suggestions(
            paper_id, conn, remaining,
        )

        # Merge: avoid duplicates by tag_id
        existing_ids = {s["tag_id"] for s in suggestions}
        for fb in fallback:
            if fb["tag_id"] not in existing_ids:
                suggestions.append(fb)
                existing_ids.add(fb["tag_id"])
                if len(suggestions) >= max_tags:
                    break

    return suggestions[:max_tags]


def bulk_suggest_tags(conn: sqlite3.Connection, progress_callback=None) -> dict:
    """Generate tag suggestions for all papers that don't have suggestions yet.

    Stores results in the ``tag_suggestions`` table.

    Args:
        conn: Open SQLite connection to the publications database.

    Returns:
        Dict with keys: total (int), generated (int), errors (int).
    """
    total = 0
    generated = 0
    errors = 0

    try:
        # Ensure tag_suggestions table exists
        conn.execute(
            """CREATE TABLE IF NOT EXISTS tag_suggestions (
                paper_id TEXT NOT NULL,
                tag TEXT NOT NULL,
                tag_id TEXT,
                confidence REAL NOT NULL,
                source TEXT NOT NULL,
                accepted INTEGER DEFAULT 0,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (paper_id, tag)
            )"""
        )

        # Find papers that have no suggestions yet
        pubs = conn.execute(
            """
            SELECT p.id AS paper_id
            FROM papers p
            WHERE NOT EXISTS (
                SELECT 1 FROM tag_suggestions ts
                WHERE ts.paper_id = p.id
            )
            """
        ).fetchall()

        total = len(pubs)
        now = datetime.utcnow().isoformat()

        for idx, pub_row in enumerate(pubs, start=1):
            pid = pub_row["paper_id"]

            try:
                suggestions = suggest_tags(pid, conn, max_tags=5)

                for s in suggestions:
                    conn.execute(
                        """
                        INSERT OR IGNORE INTO tag_suggestions
                            (paper_id, tag, tag_id, confidence, source, created_at)
                        VALUES (?, ?, ?, ?, ?, ?)
                        """,
                        (pid, s["tag"], s["tag_id"], s["confidence"], s["source"], now),
                    )

                if suggestions:
                    generated += 1

            except Exception as exc:
                logger.warning(
                    "Failed to generate tag suggestions for %s: %s",
                    pid, exc,
                )
                errors += 1

            if progress_callback and (idx == 1 or idx % 25 == 0 or idx == total):
                progress_callback(idx, total, generated, errors, pid)

        conn.commit()

    except Exception as exc:
        logger.exception("Bulk tag suggestion failed: %s", exc)
        errors += 1

    return {"total": total, "generated": generated, "errors": errors}
