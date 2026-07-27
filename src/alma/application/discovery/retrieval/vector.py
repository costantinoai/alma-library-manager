"""Vector retrieval channel — SPECTER2 nearest-neighbour search.

Two changes over the original single-centroid, corpus-only implementation:

**Per-branch centroids.** The lane used to average every seed into ONE centroid
and search around it. For a multi-topic library that point is not in any of the
topics — the mean of a bimodal distribution sits in the empty middle. A reader
working on vision *and* on methods got neither; they got the midpoint, which
matches nothing. The lane now builds deterministic spherical branch centroids
and searches around each one, so every region of the library gets
representation without invoking layout machinery on the request path.

**Corpus ∪ frontier.** The lane queried `publication_embeddings JOIN papers`,
so it could only ever return a paper already in the local corpus — the heaviest
retrieval channel structurally contributed zero new papers. It now also scans
the frontier (`application/discovery/frontier.py`), whose rows are resolved
offline. Both sources are scored in the same pass because they live in the same
vector space; the only difference is that a frontier hit is a paper the user has
never seen.
"""

from __future__ import annotations

import math
import sqlite3
from dataclasses import dataclass
from typing import Any

from alma.core.sql_helpers import standalone_paper_sql
from alma.discovery import similarity as sim_module

from ..frontier import load_frontier_vectors
from ._common import FAMILY_SEMANTIC, attach_hits

try:
    import numpy as np

    _NUMPY_AVAILABLE = True
except Exception:
    np = None  # type: ignore[assignment]
    _NUMPY_AVAILABLE = False


# Per-mode pull strength when blending an adopted direction into a centroid.
# `pin` commits to the region harder than a `boost` nudge.
_DIRECTION_BLEND_WEIGHT = {"pin": 0.5, "boost": 0.3}

# Ceiling on branch centroids searched per refresh. Each is a full scan of the
# vector matrix; at ~10k rows that is milliseconds, but the cap keeps a
# pathological cluster count from multiplying the work.
_MAX_BRANCH_CENTROIDS = 6

# Minimum seeds before branching is worth it. Below this the seed set is one
# topic by definition and a single centroid is the honest summary.
_MIN_SEEDS_FOR_BRANCHING = 8

@dataclass(frozen=True)
class _VectorPool:
    keys: tuple[str, ...]
    matrix: Any
    metadata: tuple[dict, ...]


def _retrieve_vector_channel(
    db: sqlite3.Connection,
    lens: dict,
    seeds: list[dict],
    *,
    limit: int,
) -> list[dict]:
    """Return dense-retrieval candidates from the corpus and the frontier."""
    if not _NUMPY_AVAILABLE:
        return []

    active_model = sim_module.get_active_embedding_model(db)
    seed_vectors = _load_seed_vectors(db, seeds, active_model)
    if not seed_vectors:
        return []

    centroids = _branch_centroids(db, lens, seeds, seed_vectors, active_model)
    if not centroids:
        return []

    pool = _load_search_pool(db, active_model, exclude=set(seed_vectors))
    if pool is None:
        return []

    # Each centroid runs its own kNN and contributes its own ranked list, so a
    # candidate near two branches accumulates two hits — genuine multi-region
    # evidence rather than a single averaged score.
    per_centroid = max(1, int(round(limit / max(1, len(centroids)))) + 5)
    merged: dict[str, dict] = {}
    for branch_id, centroid in centroids:
        ranked = _search(centroid, pool, limit=per_centroid)
        attach_hits(
            ranked,
            family=FAMILY_SEMANTIC,
            retriever_id="vector:branch_centroid",
            source_api="local",
            branch_id=branch_id,
            query_key=branch_id,
        )
        for item in ranked:
            key = str(item.get("_pool_key") or "")
            existing = merged.get(key)
            if existing is None:
                merged[key] = item
            else:
                existing.setdefault("retrieval_hits", []).extend(item.get("retrieval_hits") or [])
                if float(item.get("score") or 0.0) > float(existing.get("score") or 0.0):
                    existing["score"] = item["score"]

    for item in merged.values():
        item.pop("_pool_key", None)

    ranked = sorted(merged.values(), key=lambda c: float(c.get("score") or 0.0), reverse=True)
    return ranked[: max(1, limit)]


def _load_seed_vectors(
    db: sqlite3.Connection, seeds: list[dict], active_model: str
) -> dict[str, Any]:
    """Seed paper id → unit-normalised vector, for seeds that have one."""
    seed_ids = [str(seed.get("id") or "").strip() for seed in seeds]
    seed_ids = [sid for sid in seed_ids if sid]
    if not seed_ids:
        return {}

    from alma.core.vector_blob import decode_vector

    placeholders = ",".join("?" for _ in seed_ids)
    rows = db.execute(
        f"""
        SELECT pe.paper_id, pe.embedding
        FROM publication_embeddings pe
        JOIN papers p ON p.id = pe.paper_id
        WHERE pe.model = ? AND pe.paper_id IN ({placeholders})
          AND {standalone_paper_sql('p')}
        """,
        [active_model, *seed_ids],
    ).fetchall()

    out: dict[str, Any] = {}
    for row in rows:
        try:
            vec = decode_vector(row["embedding"])
            norm = float(np.linalg.norm(vec))
            if norm > 0.0:
                out[str(row["paper_id"])] = vec / norm
        except Exception:
            continue
    return out


def _branch_centroids(
    db: sqlite3.Connection,
    lens: dict,
    seeds: list[dict],
    seed_vectors: dict[str, Any],
    active_model: str,
) -> list[tuple[str, Any]]:
    """Return ``(branch_id, unit centroid)`` for each region of the seed set.

    Falls back to a single global centroid when the seed set is too small to
    branch meaningfully — one topic does not need splitting, and clustering
    noise would only fragment the query.
    """
    centroids: list[tuple[str, Any]] = []
    embedded_seed_ids = sorted(
        str(seed.get("id") or "")
        for seed in seeds
        if str(seed.get("id") or "") in seed_vectors
    )
    if len(embedded_seed_ids) >= _MIN_SEEDS_FOR_BRANCHING:
        branch_vectors = _spherical_branch_centroids(
            {paper_id: seed_vectors[paper_id] for paper_id in embedded_seed_ids},
            max_centroids=_MAX_BRANCH_CENTROIDS,
        )
        centroids = [
            (f"vector_branch_{idx}", centroid)
            for idx, centroid in enumerate(branch_vectors)
        ]

    if not centroids:
        stacked = np.vstack(list(seed_vectors.values()))
        unit = _unit(np.mean(stacked, axis=0))
        if unit is None:
            return []
        centroids = [("vector_global", unit)]

    # Adopted custom directions (task 47 §8) pull EVERY centroid toward the
    # region the user adopted from the map. Members are stored, never raw
    # vectors, so the direction is recomputed from live embeddings each refresh
    # and cannot go stale.
    directions = _direction_centroids(db, lens, active_model)
    if directions:
        centroids = [(bid, _blend(vec, directions)) for bid, vec in centroids]
    return centroids


def _spherical_branch_centroids(
    vectors: dict[str, Any],
    *,
    max_centroids: int,
) -> list[Any]:
    """Build several dense query directions without layout/JIT machinery.

    Retrieval needs semantic branch centroids, not a 2-D visualization.
    Farthest-first spherical k-means is deterministic, keeps every seed in a
    branch, and runs in milliseconds for a personal library.
    """

    if not vectors:
        return []
    ordered_ids = sorted(vectors)
    matrix = np.vstack([vectors[paper_id] for paper_id in ordered_ids]).astype(
        np.float32,
        copy=False,
    )
    if len(matrix) == 1:
        return [matrix[0]]

    desired = max(2, int(round(math.sqrt(len(matrix) / 8.0))))
    cluster_count = min(max(1, int(max_centroids)), desired, len(matrix))

    global_centroid = _unit(np.mean(matrix, axis=0))
    if global_centroid is None:
        return []
    first = int(np.argmin(matrix @ global_centroid))
    centroids = [matrix[first]]
    while len(centroids) < cluster_count:
        similarities = matrix @ np.vstack(centroids).T
        next_idx = int(np.argmin(np.max(similarities, axis=1)))
        centroids.append(matrix[next_idx])

    centroid_matrix = np.vstack(centroids)
    assignments = np.full(len(matrix), -1, dtype=np.int32)
    for _ in range(12):
        updated_assignments = np.argmax(matrix @ centroid_matrix.T, axis=1)
        if np.array_equal(assignments, updated_assignments):
            break
        assignments = updated_assignments
        updated: list[Any] = []
        for cluster_idx in range(cluster_count):
            members = matrix[assignments == cluster_idx]
            centroid = _unit(np.mean(members, axis=0)) if len(members) else None
            updated.append(
                centroid
                if centroid is not None
                else centroid_matrix[cluster_idx]
            )
        centroid_matrix = np.vstack(updated)
    return [row for row in centroid_matrix]


def _direction_centroids(
    db: sqlite3.Connection, lens: dict, active_model: str
) -> list[tuple[float, Any]]:
    """``(weight, unit centroid)`` for each adopted custom direction."""
    try:
        from ..lens_crud import _resolve_lens_branch_controls

        directions = _resolve_lens_branch_controls(lens).get("custom_directions") or []
    except Exception:
        return []
    if not directions:
        return []

    from alma.core.vector_blob import decode_vector

    out: list[tuple[float, Any]] = []
    for direction in directions:
        members = [str(m).strip() for m in (direction.get("member_paper_ids") or []) if str(m or "").strip()]
        if not members:
            continue
        placeholders = ",".join("?" for _ in members)
        rows = db.execute(
            f"""
            SELECT pe.embedding
            FROM publication_embeddings pe
            JOIN papers p ON p.id = pe.paper_id
            WHERE pe.model = ? AND pe.paper_id IN ({placeholders})
              AND {standalone_paper_sql('p')}
            """,
            [active_model, *members],
        ).fetchall()
        vecs = []
        for row in rows:
            try:
                unit = _unit(decode_vector(row["embedding"]))
                if unit is not None:
                    vecs.append(unit)
            except Exception:
                continue
        if not vecs:
            continue
        centroid = _unit(np.mean(np.vstack(vecs), axis=0))
        if centroid is None:
            continue
        weight = _DIRECTION_BLEND_WEIGHT.get(str(direction.get("mode") or "boost"), 0.3)
        out.append((weight, centroid))
    return out


def _load_search_pool(
    db: sqlite3.Connection, active_model: str, *, exclude: set[str]
) -> _VectorPool | None:
    """Every searchable vector: corpus rows plus frontier rows.

    Frontier entries carry no ``paper_id`` — they are not corpus rows yet —
    which is exactly what marks them as genuinely new suggestions downstream.
    """
    from alma.core.vector_blob import decode_vector

    keys: list[str] = []
    vectors: list[Any] = []
    metadata: list[dict] = []

    rows = db.execute(
        f"""
        SELECT pe.paper_id, pe.embedding, p.title, p.authors, p.url, p.doi,
               p.year, p.journal, p.cited_by_count, p.openalex_id
        FROM publication_embeddings pe
        JOIN papers p ON p.id = pe.paper_id
        WHERE pe.model = ? AND p.status NOT IN ('dismissed', 'removed')
          AND {standalone_paper_sql('p')}
        """,
        [active_model],
    ).fetchall()
    for row in rows:
        paper_id = str(row["paper_id"] or "")
        if not paper_id or paper_id in exclude:
            continue
        try:
            unit = _unit(decode_vector(row["embedding"]))
        except Exception:
            continue
        if unit is None:
            continue
        keys.append(paper_id)
        vectors.append(unit)
        metadata.append(
            {
                "paper_id": paper_id,
                "title": row["title"] or "",
                "authors": row["authors"] or "",
                "url": row["url"] or "",
                "doi": row["doi"] or "",
                "openalex_id": row["openalex_id"] or "",
                "year": row["year"],
                "journal": row["journal"] or "",
                "cited_by_count": row["cited_by_count"] or 0,
            }
        )

    for frontier_key, vector, meta in load_frontier_vectors(db, model=active_model):
        unit = _unit(vector)
        if unit is None:
            continue
        keys.append(f"frontier:{frontier_key}")
        vectors.append(unit)
        metadata.append(
            {
                **meta,
                "frontier_key": frontier_key,
                # Reuse the already-loaded exact-model vector during
                # scoring and semantic MMR. Dropping it here forced a
                # frontier hit back to lexical-only scoring even though
                # dense retrieval had just used the vector.
                "specter2_embedding": vector.tolist(),
                "specter2_model": active_model,
            }
        )

    if not vectors:
        return None
    return _VectorPool(
        keys=tuple(keys),
        matrix=np.vstack(vectors).astype(np.float32, copy=False),
        metadata=tuple(metadata),
    )


def _search(centroid: Any, pool: _VectorPool, *, limit: int) -> list[dict]:
    """Cosine-rank the whole pool against one centroid.

    Scores every row rather than sampling: a previous `max_scan` cap stopped
    after an arbitrary slice in SQLite row order, so the lane returned the
    best-N-of-an-arbitrary-1000 instead of the best-N-of-the-corpus. With
    float16 vectors and a numpy dot this is milliseconds.
    """
    if not pool.keys:
        return []
    sims = pool.matrix @ centroid

    top_idx = np.argsort(sims)[::-1][: max(1, limit)]
    out: list[dict] = []
    for idx in top_idx:
        score = float((sims[idx] + 1.0) / 2.0)
        if score <= 0.0:
            continue
        key = pool.keys[int(idx)]
        meta = pool.metadata[int(idx)]
        out.append(
            {
                **meta,
                "_pool_key": key,
                # `source_type` drives the diversity round-robin. `source_key`
                # is deliberately unset — the per-source-key cap is for external
                # query identifiers, not lane labels.
                "source_type": "vector",
                "score": round(score, 6),
            }
        )
    return out


def _unit(vector: Any) -> Any | None:
    """Return ``vector`` scaled to unit length, or None when degenerate."""
    if vector is None:
        return None
    norm = float(np.linalg.norm(vector))
    return vector / norm if norm > 0.0 else None


def _blend(centroid: Any, directions: list[tuple[float, Any]]) -> Any:
    """Pull ``centroid`` toward each adopted direction and re-normalise."""
    combined = centroid.copy()
    for weight, direction in directions:
        combined = combined + (weight * direction)
    blended = _unit(combined)
    # Explicit None check: `x or y` on a numpy array raises
    # "truth value of an array with more than one element is ambiguous".
    return centroid if blended is None else blended
