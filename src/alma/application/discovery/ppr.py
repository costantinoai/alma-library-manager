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

import sqlite3

from alma.core.sql_helpers import standalone_paper_sql

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
        ``{paper_id: score}``, empty when the graph is empty or there are no
        seeds.
    """
    return compute_ppr_variants(
        db,
        seed_scopes=(seed_scope,),
        alpha=alpha,
        top_k=top_k,
    )[seed_scope]


def compute_ppr_variants(
    db: sqlite3.Connection,
    *,
    seed_scopes: tuple[str, ...] = (SEED_LIBRARY, SEED_LOVED),
    alpha: float = DEFAULT_ALPHA,
    top_k: int = 2000,
) -> dict[str, dict[str, float]]:
    """Compute several personalized walks over one shared graph substrate."""

    invalid = [
        scope
        for scope in seed_scopes
        if scope not in (SEED_LIBRARY, SEED_LOVED)
    ]
    if invalid:
        raise ValueError(
            f"seed_scope must be {SEED_LIBRARY!r} or {SEED_LOVED!r}, "
            f"got {invalid[0]!r}"
        )
    if not seed_scopes:
        return {}

    import numpy as np
    from scipy import sparse

    edges = _load_edges(db)
    if not edges:
        return {scope: {} for scope in seed_scopes}

    nodes: dict[str, int] = {}
    for src, dst in edges:
        if src not in nodes:
            nodes[src] = len(nodes)
        if dst not in nodes:
            nodes[dst] = len(nodes)
    node_by_index = [""] * len(nodes)
    for node, index in nodes.items():
        node_by_index[index] = node

    rows = np.fromiter(
        (nodes[src] for src, _ in edges),
        dtype=np.int32,
        count=len(edges),
    )
    cols = np.fromiter(
        (nodes[dst] for _, dst in edges),
        dtype=np.int32,
        count=len(edges),
    )
    data = np.ones(len(edges), dtype=np.float32)
    adjacency = sparse.coo_matrix(
        (data, (rows, cols)),
        shape=(len(nodes), len(nodes)),
    ).tocsr()
    adjacency = adjacency + adjacency.T

    # Column-normalise to a stochastic matrix. Dangling mass returns through
    # the personalized restart distribution, not a uniform/global substitute.
    out_degree = np.asarray(adjacency.sum(axis=0)).ravel()
    inv = np.divide(
        1.0,
        out_degree,
        out=np.zeros_like(out_degree),
        where=out_degree > 0,
    )
    transition = adjacency @ sparse.diags(inv)
    dangling = out_degree == 0

    output: dict[str, dict[str, float]] = {}
    for scope in dict.fromkeys(seed_scopes):
        seeds = _load_seed_ids(db, scope)
        seed_idx = [nodes[seed] for seed in seeds if seed in nodes]
        if not seed_idx:
            output[scope] = {}
            continue

        restart = np.zeros(len(nodes), dtype=np.float64)
        restart[seed_idx] = 1.0 / len(seed_idx)
        rank = restart.copy()
        for _ in range(MAX_ITERATIONS):
            dangling_mass = float(rank[dangling].sum())
            nxt = alpha * (
                (transition @ rank) + (dangling_mass * restart)
            ) + (1.0 - alpha) * restart
            total = nxt.sum()
            if total > 0:
                nxt /= total
            if np.abs(nxt - rank).sum() < CONVERGENCE_TOL:
                rank = nxt
                break
            rank = nxt

        seed_set = set(seeds)
        scored = [
            (node_by_index[index], float(rank[index]))
            for index in np.argsort(rank)[::-1]
            if node_by_index[index] not in seed_set and rank[index] > 0.0
        ][: max(1, top_k)]
        if not scored:
            output[scope] = {}
            continue
        top = scored[0][1] or 1.0
        output[scope] = {
            node: round(score / top, 6)
            for node, score in scored
        }
    return output


def _load_edges(db: sqlite3.Connection) -> list[tuple[str, str]]:
    """Citation edges as ``(citing_paper_id, cited_paper_id)`` pairs.

    Only edges where BOTH endpoints are local papers are used: an edge to a
    work we hold no row for cannot be scored or recommended, and including it
    would inflate the graph with millions of dangling nodes for no gain.
    """
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
