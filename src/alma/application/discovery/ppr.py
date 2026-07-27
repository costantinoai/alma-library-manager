"""Personalized PageRank over the local citation graph.

What it answers
---------------

"How close is this paper to the part of the literature I already care about?",
using citation structure alone. A random walker starts on the user's Library
(or just their loved papers), follows citation edges, and restarts from the
seeds with probability ``1 - alpha``. The stationary distribution scores every
reachable paper by proximity-to-taste in one pass.

Why this and not just the embedding
-----------------------------------

SPECTER2 is *itself* a citation-trained embedding, so dense retrieval already
encodes citation structure — smoothed and globally. PPR is the exact, local
view: it knows that *this* paper is two hops from *that* specific paper you
loved, which a 768-d average cannot represent. They fail differently, which is
precisely why fusing them is worth more than either alone.

Cost: the dev corpus has 395,560 reference edges. Power iteration on a scipy
CSR matrix converges in ~30 iterations, well under 100 ms — cheap enough to run
inside a refresh, unlike every network lane.

Geometry contract
-----------------

Seeding a walk on Library membership reads membership as a node *attribute* to
decide where to start; it never writes a coordinate, a cluster, or a community
assignment. That is ranking, not geometry, and is permitted under
``CLAUDE.md`` → "Geometry is corpus-intrinsic". This module must stay OUT of
the layout builders' import closure so
``tests/test_geometry_admission_contract.py`` keeps passing by construction.
"""

from __future__ import annotations

import logging
import sqlite3

from alma.core.sql_helpers import standalone_paper_sql

logger = logging.getLogger(__name__)

# Restart probability complement. 0.85 is the classic PageRank damping factor;
# lower values keep the walk tighter around the seeds.
DEFAULT_ALPHA = 0.85

# Power-iteration bounds. Convergence on a citation graph of this size is fast;
# the cap exists so a pathological graph cannot stall a refresh.
MAX_ITERATIONS = 60
CONVERGENCE_TOL = 1e-8

# Seed scopes. `library` = everything you kept; `loved` = only what you rated
# 4+. The two answer different questions and both are useful as features, so
# they are computed separately rather than blended into one number.
SEED_LIBRARY = "library"
SEED_LOVED = "loved"


def compute_ppr(
    db: sqlite3.Connection,
    *,
    seed_scope: str = SEED_LIBRARY,
    alpha: float = DEFAULT_ALPHA,
    top_k: int = 2000,
) -> dict[str, float]:
    """Return ``{paper_id: ppr_score}`` for the top ``top_k`` non-seed papers.

    Scores are normalised so the highest is 1.0, which makes them directly
    usable as a retrieval score without the caller needing to know the graph's
    size. Seeds themselves are excluded — a paper you already have is not a
    recommendation.

    Args:
        db: Open connection.
        seed_scope: :data:`SEED_LIBRARY` or :data:`SEED_LOVED`.
        alpha: Probability of following an edge rather than restarting.
        top_k: How many scored papers to return.

    Returns:
        ``{paper_id: score}``, empty when numpy/scipy are unavailable, the
        graph is empty, or there are no seeds.
    """
    # Validate the argument BEFORE any work: a caller passing a bad scope has a
    # bug, and returning an empty dict would hide it behind "the graph was
    # empty". Fail fast, loudly, at the boundary.
    if seed_scope not in (SEED_LIBRARY, SEED_LOVED):
        raise ValueError(
            f"seed_scope must be {SEED_LIBRARY!r} or {SEED_LOVED!r}, got {seed_scope!r}"
        )

    try:
        import numpy as np
        from scipy import sparse
    except ImportError:
        logger.debug("PPR unavailable: numpy/scipy not installed")
        return {}

    edges = _load_edges(db)
    if not edges:
        return {}
    seeds = _load_seed_ids(db, seed_scope)
    if not seeds:
        return {}

    # Build a compact index over every node that appears in the graph.
    nodes: dict[str, int] = {}
    for src, dst in edges:
        if src not in nodes:
            nodes[src] = len(nodes)
        if dst not in nodes:
            nodes[dst] = len(nodes)
    seed_idx = [nodes[s] for s in seeds if s in nodes]
    if not seed_idx:
        return {}

    n = len(nodes)
    rows = np.fromiter((nodes[s] for s, _ in edges), dtype=np.int32, count=len(edges))
    cols = np.fromiter((nodes[d] for _, d in edges), dtype=np.int32, count=len(edges))
    data = np.ones(len(edges), dtype=np.float32)

    # Citation edges are directed, but influence flows both ways for
    # similarity: a paper is close to my taste whether it cites my Library or
    # is cited by it. Symmetrising is what makes this a proximity measure
    # rather than a prestige measure — an asymmetric walk would drift toward
    # highly-cited hubs, which is exactly the prestige signal that measured
    # NEGATIVE for this user.
    adjacency = sparse.coo_matrix((data, (rows, cols)), shape=(n, n)).tocsr()
    adjacency = adjacency + adjacency.T

    # Column-normalise to a stochastic matrix. Dangling nodes (no out-edges)
    # keep a zero column; their mass is returned to the seeds by the restart
    # term rather than leaking out of the system.
    out_degree = np.asarray(adjacency.sum(axis=0)).ravel()
    inv = np.divide(1.0, out_degree, out=np.zeros_like(out_degree), where=out_degree > 0)
    transition = adjacency @ sparse.diags(inv)

    restart = np.zeros(n, dtype=np.float64)
    restart[seed_idx] = 1.0 / len(seed_idx)

    rank = restart.copy()
    for _ in range(MAX_ITERATIONS):
        nxt = alpha * (transition @ rank) + (1.0 - alpha) * restart
        total = nxt.sum()
        if total > 0:
            nxt /= total
        if np.abs(nxt - rank).sum() < CONVERGENCE_TOL:
            rank = nxt
            break
        rank = nxt

    seed_set = set(seeds)
    index_to_node = {idx: node for node, idx in nodes.items()}
    scored = [
        (index_to_node[i], float(rank[i]))
        for i in np.argsort(rank)[::-1]
        if index_to_node[i] not in seed_set and rank[i] > 0.0
    ][: max(1, top_k)]
    if not scored:
        return {}

    top = scored[0][1] or 1.0
    return {node: round(score / top, 6) for node, score in scored}


def _load_edges(db: sqlite3.Connection) -> list[tuple[str, str]]:
    """Citation edges as ``(citing_paper_id, cited_paper_id)`` pairs.

    Only edges where BOTH endpoints are local papers are used: an edge to a
    work we hold no row for cannot be scored or recommended, and including it
    would inflate the graph with millions of dangling nodes for no gain.
    """
    try:
        rows = db.execute(
            f"""
            SELECT pr.paper_id AS src, q.id AS dst
            FROM publication_references pr
            JOIN papers q ON q.openalex_id = 'W' || pr.referenced_work_id
            JOIN papers p ON p.id = pr.paper_id
            WHERE COALESCE(TRIM(pr.referenced_work_id), '') != ''
              AND {standalone_paper_sql('p')}
              AND {standalone_paper_sql('q')}
            """
        ).fetchall()
    except sqlite3.OperationalError as exc:
        logger.warning("PPR edge load failed: %s", exc)
        return []
    return [(str(r["src"]), str(r["dst"])) for r in rows if r["src"] and r["dst"]]


def _load_seed_ids(db: sqlite3.Connection, seed_scope: str) -> list[str]:
    """Paper ids the walk restarts from."""
    where = (
        "p.status = 'library' AND COALESCE(p.rating, 0) >= 4"
        if seed_scope == SEED_LOVED
        else "p.status = 'library'"
    )
    rows = db.execute(
        f"SELECT p.id FROM papers p WHERE {where} AND {standalone_paper_sql('p')}"
    ).fetchall()
    return [str(r["id"]) for r in rows if r["id"]]
