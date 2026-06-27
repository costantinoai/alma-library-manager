"""Vector retrieval channel — SPECTER2 nearest-neighbour search.

Split out of the discovery god-module (D-9); pure move. Re-establishes the
module-level numpy guard the orchestrator relies on so the path degrades
gracefully when numpy is unavailable.
"""

from __future__ import annotations

from alma.core.components import not_component_sql
from alma.discovery import similarity as sim_module

try:
    import numpy as np

    _NUMPY_AVAILABLE = True
except Exception:
    np = None  # type: ignore[assignment]
    _NUMPY_AVAILABLE = False


def _retrieve_vector_channel(
    db: sqlite3.Connection,
    lens: dict,
    seeds: list[dict],
    *,
    limit: int,
) -> list[dict]:
    if not _NUMPY_AVAILABLE:
        return []

    seed_ids = [str(seed.get("id") or "").strip() for seed in seeds]
    seed_ids = [sid for sid in seed_ids if sid]
    if not seed_ids:
        return []

    active_model = sim_module.get_active_embedding_model(db)
    placeholders = ",".join("?" for _ in seed_ids)
    seed_rows = db.execute(
        f"""
        SELECT paper_id, embedding
        FROM publication_embeddings
        WHERE model = ? AND paper_id IN ({placeholders})
        """,
        [active_model, *seed_ids],
    ).fetchall()
    if not seed_rows:
        return []

    from alma.core.vector_blob import decode_vector
    seed_vecs: list["np.ndarray"] = []
    for row in seed_rows:
        try:
            vec = decode_vector(row["embedding"])
            norm = float(np.linalg.norm(vec))
            if norm <= 0.0:
                continue
            seed_vecs.append(vec / norm)
        except Exception:
            continue
    if not seed_vecs:
        return []

    centroid = np.mean(np.vstack(seed_vecs), axis=0)
    centroid_norm = float(np.linalg.norm(centroid))
    if centroid_norm <= 0.0:
        return []
    centroid = centroid / centroid_norm

    rows = db.execute(
        """
        SELECT pe.paper_id, pe.embedding, p.title, p.authors, p.url, p.doi, p.year, p.journal, p.cited_by_count
        FROM publication_embeddings pe
        JOIN papers p ON p.id = pe.paper_id
        WHERE pe.model = ? AND p.status NOT IN ('dismissed', 'removed')
          AND """ + not_component_sql("p") + """
        """,
        [active_model],
    ).fetchall()

    # Score every embedded paper against the centroid — there used to
    # be a `max_scan` cap that stopped after `limit*20` rows in
    # arbitrary SQLite row order, which meant the lane returned the
    # best-N-of-an-arbitrary-1000 rather than the best-N-of-the-
    # corpus. With float16-encoded vectors and numpy dot, scoring 5–10k
    # rows takes well under a second; the previous "performance" cap
    # was actively producing worse retrieval at noticeable cost to
    # quality.
    seed_set = set(seed_ids)
    scored: list[tuple[float, dict]] = []
    for row in rows:
        paper_id = str(row["paper_id"] or "").strip()
        if not paper_id or paper_id in seed_set:
            continue
        try:
            vec = decode_vector(row["embedding"])
            if vec.shape != centroid.shape:
                continue
            norm = float(np.linalg.norm(vec))
            if norm <= 0.0:
                continue
            vec = vec / norm
            sim = float(np.dot(centroid, vec))
            score = max(0.0, (sim + 1.0) / 2.0)
            if score <= 0.0:
                continue
            scored.append(
                (
                    score,
                    {
                        # paper_id MUST be carried so the downstream
                        # embedding lookup can short-circuit (the lookup
                        # falls back to openalex_id/doi/s2_id resolution
                        # but those won't match for purely-internal
                        # corpus papers that haven't been backfilled
                        # with their OpenAlex ID yet). Without this the
                        # vector lane's own candidates couldn't get
                        # their cached embeddings reused at scoring,
                        # collapsing text_similarity_mode to "lexical".
                        "paper_id": paper_id,
                        # `source_type` drives the diversity_interleave
                        # round-robin so the vector lane gets fair air
                        # time. We deliberately leave `source_key` unset
                        # — the per-source-key cap is meant for
                        # external-query identifiers (taste_author:smith,
                        # taste_topic:visual_cortex), not for lane
                        # labels. With `source_key=""` the diversity
                        # cap skips these candidates entirely.
                        "source_type": "vector",
                        "title": row["title"] or "",
                        "authors": row["authors"] or "",
                        "url": row["url"] or "",
                        "doi": row["doi"] or "",
                        "score": score,
                        "year": row["year"],
                        "journal": row["journal"] or "",
                        "cited_by_count": row["cited_by_count"] or 0,
                    },
                )
            )
        except Exception:
            continue

    scored.sort(key=lambda x: x[0], reverse=True)
    return [item for _, item in scored[: max(1, limit)]]
