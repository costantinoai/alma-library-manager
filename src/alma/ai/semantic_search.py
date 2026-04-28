"""Semantic paper search helpers.

Normal paper search remains keyword-only on GET routes. Live semantic query
embedding is reserved for explicit Activity-backed actions.
"""

import logging
import sqlite3

from alma.discovery.semantic_scholar import S2_SPECTER2_MODEL

logger = logging.getLogger(__name__)

SPECTER2_ADHOC_QUERY_ADAPTER = "allenai/specter2_adhoc_query"
SPECTER2_ADHOC_QUERY_ADAPTER_KEY = "specter2_adhoc_query"

# Defensive imports for optional dependencies
try:
    import numpy as np

    _NUMPY_AVAILABLE = True
except ImportError:
    np = None  # type: ignore[assignment]
    _NUMPY_AVAILABLE = False


def embed_specter2_adhoc_query(query: str) -> "np.ndarray":
    """Embed a short search query with the SPECTER2 adhoc-query adapter."""
    if not _NUMPY_AVAILABLE:
        raise RuntimeError("SPECTER2 semantic search requires numpy")
    text = str(query or "").strip()
    if not text:
        raise ValueError("query is required")

    from alma.discovery.similarity import SpecterEmbedder, prepare_query_text

    embedder = SpecterEmbedder.get_instance(
        model_name=S2_SPECTER2_MODEL,
        embedding_dim=768,
        max_length=128,
        adapter_name=SPECTER2_ADHOC_QUERY_ADAPTER,
        adapter_key=SPECTER2_ADHOC_QUERY_ADAPTER_KEY,
    )
    prepared = prepare_query_text(text, max_tokens=128, query_prefix="")
    return np.asarray(embedder.encode_single(prepared), dtype=np.float32)


def _semantic_search_with_query_vector(
    query_vec: "np.ndarray",
    conn: sqlite3.Connection,
    *,
    scope: str = "library",
    limit: int = 20,
    model: str = S2_SPECTER2_MODEL,
) -> list[dict]:
    """Find nearest papers for an already-computed query vector."""
    if not _NUMPY_AVAILABLE:
        logger.debug("numpy not available; semantic search disabled")
        return []
    query_norm = np.linalg.norm(query_vec)
    if query_norm == 0.0:
        return []
    query_vec = query_vec / query_norm

    scope_value = str(scope or "library").strip().lower()
    where = ""
    params: list[object] = [model]
    if scope_value == "library":
        where = "AND p.status = 'library'"
    elif scope_value == "all":
        where = ""
    else:
        raise ValueError("scope must be 'library' or 'all'")

    try:
        emb_rows = conn.execute(
            f"""
            SELECT
                p.*,
                pe.embedding AS _embedding
            FROM publication_embeddings pe
            JOIN papers p ON p.id = pe.paper_id
            WHERE pe.model = ?
            {where}
            """,
            params,
        ).fetchall()
    except sqlite3.OperationalError:
        logger.debug("publication_embeddings table not found")
        return []

    if not emb_rows:
        return []

    scored: list[tuple[str, float]] = []
    for emb_row in emb_rows:
        try:
            embedding = np.frombuffer(emb_row["_embedding"], dtype=np.float32).copy()
            if embedding.shape != query_vec.shape:
                continue
            emb_norm = np.linalg.norm(embedding)
            if emb_norm == 0.0:
                continue
            embedding = embedding / emb_norm
            sim = float(np.dot(query_vec, embedding))
            scored.append((emb_row["id"], sim))
        except Exception:
            continue

    # Sort by similarity descending and take top results
    scored.sort(key=lambda x: x[1], reverse=True)
    top_ids = {paper_id for paper_id, _ in scored[:limit]}
    score_by_id = {paper_id: score for paper_id, score in scored[:limit]}

    if not top_ids:
        return []

    results: list[dict] = []
    for row in emb_rows:
        paper_id = str(row["id"])
        if paper_id not in top_ids:
            continue
        pub = dict(row)
        pub.pop("_embedding", None)
        pub["score"] = max(0.0, float(score_by_id.get(paper_id) or 0.0))
        pub["match_type"] = "semantic"
        pub["embedding_model"] = model
        pub["query_model"] = SPECTER2_ADHOC_QUERY_ADAPTER
        results.append(pub)
    results.sort(key=lambda item: item.get("score", 0.0), reverse=True)

    return results


def specter2_semantic_search(
    query: str,
    conn: sqlite3.Connection,
    *,
    scope: str = "library",
    limit: int = 20,
) -> list[dict]:
    """Run explicit SPECTER2 semantic search over cached S2 paper vectors."""
    query_vec = embed_specter2_adhoc_query(query)
    return _semantic_search_with_query_vector(
        query_vec,
        conn,
        scope=scope,
        limit=limit,
        model=S2_SPECTER2_MODEL,
    )
