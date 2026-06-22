"""2D projection of publication embeddings for visualization."""

from collections import defaultdict
import json
import logging
import math
import sqlite3
from typing import Any

import numpy as np

from alma.core.scope import Scope

logger = logging.getLogger(__name__)

# Optional dependency
try:
    import umap as _umap

    _UMAP_AVAILABLE = True
except ImportError:
    _umap = None
    _UMAP_AVAILABLE = False


def _table_exists(conn: sqlite3.Connection, table_name: str) -> bool:
    row = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name = ?",
        (table_name,),
    ).fetchone()
    return row is not None


def _safe_norm_rows(matrix: np.ndarray) -> np.ndarray:
    norms = np.linalg.norm(matrix, axis=1, keepdims=True)
    norms[norms == 0] = 1.0
    return matrix / norms


def project_embeddings(
    embeddings: dict[str, list[float]],
    method: str = "auto",
) -> dict[str, tuple[float, float]]:
    """Project embeddings to 2D for visualization.

    Args:
        embeddings: Map of paper_id -> embedding vector.
        method: "umap", "tsne", or "auto" (UMAP if available, else t-SNE).

    Returns:
        Map of paper_id -> (x, y) coordinates.
    """
    if not embeddings or len(embeddings) < 3:
        # Not enough points to project
        return {k: (0.0, 0.0) for k in embeddings}

    keys = list(embeddings.keys())
    vectors = np.array([embeddings[k] for k in keys])

    if method == "auto":
        method = "umap" if _UMAP_AVAILABLE else "tsne"

    if method == "umap" and _UMAP_AVAILABLE:
        reducer = _umap.UMAP(
            n_components=2,
            n_neighbors=min(15, len(keys) - 1),
            min_dist=0.1,
            metric="cosine",
            random_state=42,
        )
        coords_2d = reducer.fit_transform(vectors)
    else:
        from sklearn.manifold import TSNE

        perplexity = min(30, max(2, len(keys) // 4))
        tsne = TSNE(
            n_components=2,
            perplexity=perplexity,
            random_state=42,
            max_iter=1000,
        )
        coords_2d = tsne.fit_transform(vectors)

    # Normalize to [0, 1] range for easier rendering
    mins = coords_2d.min(axis=0)
    maxs = coords_2d.max(axis=0)
    ranges = maxs - mins
    ranges[ranges == 0] = 1  # avoid division by zero
    coords_2d = (coords_2d - mins) / ranges

    return {
        keys[i]: (float(coords_2d[i, 0]), float(coords_2d[i, 1]))
        for i in range(len(keys))
    }


def build_topic_cooccurrence(
    conn: sqlite3.Connection, min_cooccurrence: int = 2
) -> dict:
    """Build a topic co-occurrence graph from publication_topics.

    This is the fallback when no embeddings are available.
    No AI dependencies needed -- works purely from existing topic associations.

    The ``publication_topics`` table uses columns ``term`` and ``score``
    (not ``topic_name``).

    Returns dict with:
        nodes: list of {id, name, count, x, y}
        edges: list of {source, target, weight}
    """
    has_topics_table = _table_exists(conn, "topics")

    # Get topic frequencies (canonical-aware when topics table is available)
    try:
        if has_topics_table:
            topics = conn.execute(
                """
                SELECT COALESCE(t.canonical_name, pt.term) AS term,
                       COUNT(DISTINCT pt.paper_id) AS count
                FROM publication_topics pt
                LEFT JOIN topics t ON pt.topic_id = t.topic_id
                GROUP BY COALESCE(t.canonical_name, pt.term)
                HAVING count >= 2
                ORDER BY count DESC
                LIMIT 100
                """
            ).fetchall()
        else:
            topics = conn.execute(
                """
                SELECT term, COUNT(DISTINCT paper_id) AS count
                FROM publication_topics
                GROUP BY term
                HAVING count >= 2
                ORDER BY count DESC
                LIMIT 100
                """
            ).fetchall()
    except Exception as exc:
        logger.warning("Could not query publication_topics: %s", exc)
        return {"nodes": [], "edges": []}

    if not topics:
        return {"nodes": [], "edges": []}

    topic_names = [t[0] if not isinstance(t, sqlite3.Row) else t["term"] for t in topics]
    topic_counts = {
        name: (t[1] if not isinstance(t, sqlite3.Row) else t["count"])
        for name, t in zip(topic_names, topics)
    }

    # Build co-occurrence: pairs of terms that appear on the same publication
    placeholders = ",".join(["?"] * len(topic_names))
    try:
        if has_topics_table:
            cooccurrences = conn.execute(
                f"""
                WITH canonical_topics AS (
                    SELECT pt.paper_id,
                           COALESCE(t.canonical_name, pt.term) AS term
                    FROM publication_topics pt
                    LEFT JOIN topics t ON pt.topic_id = t.topic_id
                ),
                paper_terms AS (
                    SELECT DISTINCT paper_id, term
                    FROM canonical_topics
                )
                SELECT a.term, b.term, COUNT(*) AS co_count
                FROM paper_terms a
                JOIN paper_terms b
                  ON a.paper_id = b.paper_id
                WHERE a.term < b.term
                  AND a.term IN ({placeholders})
                  AND b.term IN ({placeholders})
                GROUP BY a.term, b.term
                HAVING co_count >= ?
                """,
                topic_names + topic_names + [min_cooccurrence],
            ).fetchall()
        else:
            cooccurrences = conn.execute(
                f"""
                SELECT a.term, b.term, COUNT(*) AS co_count
                FROM publication_topics a
                JOIN publication_topics b
                    ON a.paper_id = b.paper_id
                WHERE a.term < b.term
                    AND a.term IN ({placeholders})
                    AND b.term IN ({placeholders})
                GROUP BY a.term, b.term
                HAVING co_count >= ?
                """,
                topic_names + topic_names + [min_cooccurrence],
            ).fetchall()
    except Exception as exc:
        logger.warning("Could not compute topic co-occurrence: %s", exc)
        cooccurrences = []

    # Simple circular layout (frontend will do the real force layout)
    max_count = max(topic_counts.values()) if topic_counts else 1
    nodes = []
    for i, name in enumerate(topic_names):
        angle = 2 * math.pi * i / len(topic_names)
        radius = 0.3 + 0.2 * (1 - topic_counts[name] / max_count)
        nodes.append(
            {
                "id": name,
                "name": name,
                "count": topic_counts[name],
                "x": 0.5 + radius * math.cos(angle),
                "y": 0.5 + radius * math.sin(angle),
            }
        )

    edges = []
    for row in cooccurrences:
        source = row[0] if not isinstance(row, sqlite3.Row) else row["term"]
        target = row[1]
        weight = row[2] if not isinstance(row, sqlite3.Row) else row["co_count"]
        # Handle sqlite3.Row properly -- positional access is safest for
        # multi-column SELECT with aliases
        if isinstance(row, sqlite3.Row):
            source = row[0]
            target = row[1]
            weight = row[2]
        edges.append(
            {
                "source": source,
                "target": target,
                "weight": weight,
            }
        )

    return {"nodes": nodes, "edges": edges}


def _top_k_pairs_per_node(
    pairs: dict[tuple[str, str], int], k: int
) -> dict[tuple[str, str], int]:
    """Keep only each node's ``k`` strongest pairs (by weight) from an
    undirected pair→weight map.

    Sparsifies a dense relational layer (e.g. bibliographic coupling, where
    every author in a field couples with nearly every other) the same way
    mutual-kNN sparsifies the semantic layer: each node retains a bounded set
    of its strongest connections, so the rendered graph stays legible
    regardless of corpus density. An edge survives if EITHER endpoint ranks it
    in its top-k (union), so strong asymmetric links aren't dropped.
    """
    if not pairs:
        return {}
    by_node: dict[str, list[tuple[int, tuple[str, str]]]] = defaultdict(list)
    for pair, weight in pairs.items():
        a, b = pair
        by_node[a].append((weight, pair))
        by_node[b].append((weight, pair))
    keep: set[tuple[str, str]] = set()
    for ranked in by_node.values():
        ranked.sort(key=lambda item: item[0], reverse=True)
        for _weight, pair in ranked[:k]:
            keep.add(pair)
    return {pair: pairs[pair] for pair in keep}


def build_coauthor_network(
    conn: sqlite3.Connection,
    *,
    scope: str = "library",
) -> dict:
    """Build a multi-signal "research neighbourhood" author network (I-11).

    Mirrors the paper map and reuses the SAME primitives (DRY): nodes are
    positioned + clustered from each author's mean SPECTER2 embedding
    (``cluster_publications``, eom + retained outliers), and edges come in three
    TYPED, filterable layers — none of them productivity stats:
      • semantic               — mutual k-NN over the 768-d author embeddings
      • co_authorship          — shared papers
      • bibliographic_coupling — shared references (co-citation signal),
                                 sparsified to each author's top-k partners
    Citation / h-index / works_count stay node metadata for display only — they
    never enter edge geometry or the clustering features.

    Author identity is rooted in ``publication_authors.openalex_id`` —
    the v3 schema's source of truth for paper ↔ author links. The
    legacy ``papers.author_id`` column is empty under v3 and is no
    longer consulted. Authors are enriched from the ``authors`` table
    when a matching row exists (via OpenAlex ID); otherwise we fall
    back to the display name carried by ``publication_authors``.

    When scope == "library" (default), nodes/edges are restricted to
    Library-status papers; scope == "corpus" includes every stored
    paper.

    Returns dict with:
        nodes: list of {id, name, pub_count, citation_count, x, y, ...}
        edges: list of {source, target, weight, shared_topics, shared_papers}
        clusters: list of {id, label, size, member_ids}
        method: clustering method tag
    """
    has_topics_table = _table_exists(conn, "topics")
    status_filter = Scope.parse(scope).paper_filter("p")

    # Authors keyed by OpenAlex ID, with publication stats + (optional)
    # profile enrichment from the `authors` table.
    try:
        authors_data = conn.execute(
            f"""
            SELECT pa.openalex_id AS author_id,
                   COALESCE(NULLIF(a.name, ''), MIN(pa.display_name)) AS author_name,
                   COUNT(DISTINCT pa.paper_id) AS pub_count,
                   COALESCE(SUM(p.cited_by_count), 0) AS citation_count,
                   COALESCE(a.affiliation, '') AS affiliation,
                   COALESCE(a.citedby, 0) AS author_citedby,
                   COALESCE(a.h_index, 0) AS h_index,
                   COALESCE(a.works_count, 0) AS works_count,
                   COALESCE(a.orcid, '') AS orcid,
                   COALESCE(NULLIF(a.openalex_id, ''), pa.openalex_id) AS openalex_id,
                   COALESCE(a.interests, '') AS interests
            FROM publication_authors pa
            JOIN papers p ON p.id = pa.paper_id
            LEFT JOIN authors a ON lower(a.openalex_id) = lower(pa.openalex_id)
            WHERE pa.openalex_id <> ''{status_filter}
            GROUP BY pa.openalex_id,
                     a.name,
                     a.affiliation,
                     a.citedby,
                     a.h_index,
                     a.works_count,
                     a.orcid,
                     a.openalex_id,
                     a.interests
            ORDER BY pub_count DESC
            """
        ).fetchall()
    except Exception as exc:
        logger.warning("Could not query publication_authors for author network: %s", exc)
        return {"nodes": [], "edges": []}

    if len(authors_data) < 2:
        return {"nodes": [], "edges": []}

    author_ids = [r["author_id"] if isinstance(r, sqlite3.Row) else r[0] for r in authors_data]
    author_names = {
        (r["author_id"] if isinstance(r, sqlite3.Row) else r[0]): (
            r["author_name"] if isinstance(r, sqlite3.Row) else r[1]
        )
        for r in authors_data
    }
    pub_counts = {
        (r["author_id"] if isinstance(r, sqlite3.Row) else r[0]): int(
            r["pub_count"] if isinstance(r, sqlite3.Row) else r[2]
        )
        for r in authors_data
    }
    citation_counts = {
        (r["author_id"] if isinstance(r, sqlite3.Row) else r[0]): int(
            r["citation_count"] if isinstance(r, sqlite3.Row) else r[3]
        )
        for r in authors_data
    }
    affiliations = {
        (r["author_id"] if isinstance(r, sqlite3.Row) else r[0]): (
            r["affiliation"] if isinstance(r, sqlite3.Row) else r[4]
        )
        for r in authors_data
    }
    author_citedby = {
        (r["author_id"] if isinstance(r, sqlite3.Row) else r[0]): int(
            r["author_citedby"] if isinstance(r, sqlite3.Row) else r[5]
        )
        for r in authors_data
    }
    h_indices = {
        (r["author_id"] if isinstance(r, sqlite3.Row) else r[0]): int(
            r["h_index"] if isinstance(r, sqlite3.Row) else r[6]
        )
        for r in authors_data
    }
    works_counts = {
        (r["author_id"] if isinstance(r, sqlite3.Row) else r[0]): int(
            r["works_count"] if isinstance(r, sqlite3.Row) else r[7]
        )
        for r in authors_data
    }
    orcids = {
        (r["author_id"] if isinstance(r, sqlite3.Row) else r[0]): (
            r["orcid"] if isinstance(r, sqlite3.Row) else r[8]
        )
        for r in authors_data
    }
    openalex_ids = {
        (r["author_id"] if isinstance(r, sqlite3.Row) else r[0]): (
            r["openalex_id"] if isinstance(r, sqlite3.Row) else r[9]
        )
        for r in authors_data
    }
    interests_raw = {
        (r["author_id"] if isinstance(r, sqlite3.Row) else r[0]): (
            r["interests"] if isinstance(r, sqlite3.Row) else r[10]
        )
        for r in authors_data
    }
    n_authors = len(author_ids)
    author_index = {aid: i for i, aid in enumerate(author_ids)}

    # Build topic-weight vectors per author (alias-aware).
    topic_weights_by_author: dict[str, dict[str, float]] = {
        aid: {} for aid in author_ids
    }
    if _table_exists(conn, "publication_topics"):
        placeholders = ",".join(["?"] * len(author_ids))
        topic_status_filter = Scope.parse(scope).paper_filter("p")
        try:
            if has_topics_table:
                topic_rows = conn.execute(
                    f"""
                    SELECT pa.openalex_id AS author_id,
                           COALESCE(t.canonical_name, pt.term) AS term,
                           SUM(COALESCE(pt.score, 1.0)) AS weight
                    FROM publication_topics pt
                    JOIN publication_authors pa ON pa.paper_id = pt.paper_id
                    JOIN papers p ON p.id = pa.paper_id
                    LEFT JOIN topics t ON pt.topic_id = t.topic_id
                    WHERE pa.openalex_id IN ({placeholders}){topic_status_filter}
                    GROUP BY pa.openalex_id, COALESCE(t.canonical_name, pt.term)
                    """,
                    author_ids,
                ).fetchall()
            else:
                topic_rows = conn.execute(
                    f"""
                    SELECT pa.openalex_id AS author_id,
                           pt.term,
                           SUM(COALESCE(pt.score, 1.0)) AS weight
                    FROM publication_topics pt
                    JOIN publication_authors pa ON pa.paper_id = pt.paper_id
                    JOIN papers p ON p.id = pa.paper_id
                    WHERE pa.openalex_id IN ({placeholders}){topic_status_filter}
                    GROUP BY pa.openalex_id, pt.term
                    """,
                    author_ids,
                ).fetchall()
            for row in topic_rows:
                aid = row["author_id"] if isinstance(row, sqlite3.Row) else row[0]
                term = row["term"] if isinstance(row, sqlite3.Row) else row[1]
                weight = float(row["weight"] if isinstance(row, sqlite3.Row) else row[2])
                if aid in topic_weights_by_author and term:
                    topic_weights_by_author[aid][term] = weight
        except Exception as exc:
            logger.warning("Could not compute author topic vectors: %s", exc)

    top_topic_by_author: dict[str, str] = {}
    for aid, topic_map in topic_weights_by_author.items():
        if topic_map:
            top_topic_by_author[aid] = max(topic_map.items(), key=lambda kv: kv[1])[0]

    author_interests: dict[str, list[str]] = {}
    for aid, raw in interests_raw.items():
        parsed: list[str] = []
        val = (raw or "").strip()
        if val:
            try:
                j = json.loads(val)
                if isinstance(j, list):
                    parsed = [str(x).strip() for x in j if str(x).strip()]
            except Exception:
                parsed = [x.strip() for x in val.split(",") if x.strip()]
        author_interests[aid] = parsed[:8]

    # NOTE (Phase 3 / I-11): author topic vectors are kept ONLY for cluster
    # labelling (via score_cluster_terms below) + the per-author top_topic.
    # Topic similarity no longer drives edges — the semantic edge layer is now
    # mutual k-NN over the 768-d author embeddings, and productivity stats never
    # enter edge geometry (they stay node metadata).

    # Direct co-authorship signal: how many papers two authors share.
    # Under the v3 schema this is a clean junction-table lookup —
    # ``publication_authors`` already tracks every (paper, author) link.
    shared_pairs: dict[tuple[str, str], int] = {}
    max_shared = 0
    try:
        placeholders = ",".join(["?"] * len(author_ids))
        shared_status_filter = Scope.parse(scope).paper_filter("p")
        shared_rows = conn.execute(
            f"""
            SELECT pa1.openalex_id AS a1,
                   pa2.openalex_id AS a2,
                   COUNT(DISTINCT pa1.paper_id) AS shared
            FROM publication_authors pa1
            JOIN publication_authors pa2
              ON pa1.paper_id = pa2.paper_id
             AND pa1.openalex_id < pa2.openalex_id
            JOIN papers p ON p.id = pa1.paper_id
            WHERE pa1.openalex_id IN ({placeholders})
              AND pa2.openalex_id IN ({placeholders}){shared_status_filter}
            GROUP BY pa1.openalex_id, pa2.openalex_id
            """,
            author_ids + author_ids,
        ).fetchall()
        for row in shared_rows:
            a1 = row["a1"] if isinstance(row, sqlite3.Row) else row[0]
            a2 = row["a2"] if isinstance(row, sqlite3.Row) else row[1]
            shared = int(row["shared"] if isinstance(row, sqlite3.Row) else row[2])
            shared_pairs[(a1, a2)] = shared
            max_shared = max(max_shared, shared)
    except Exception as exc:
        logger.warning("Could not compute shared-paper overlap: %s", exc)

    # Bibliographic coupling: authors whose papers cite the same works.
    bib_pairs, max_bib = _author_bibliographic_coupling(conn, author_ids, scope=scope)

    # Author embeddings (768-d SPECTER2 mean) — the basis for the semantic
    # neighbourhood AND the clustering/layout below (Phase 3 / I-11).
    author_embeddings = _author_mean_embeddings(conn, author_ids)
    embedded_ids = [aid for aid in author_ids if aid in author_embeddings]

    # ── Typed, filterable edge LAYERS (I-11) ──────────────────────────────
    # Replaces the single combined_similarity — which folded productivity STATS
    # into edge geometry (a locked-rule violation) and collapsed everything into
    # one top-k weight. Each layer now means one thing and carries its edge_type
    # so the UI can filter; citation/h-index stay node metadata only.
    edges: list[dict[str, Any]] = []
    edge_layers: dict[str, int] = {}
    seen_edges: set[tuple[str, str, str]] = set()

    def _emit_edge(a1: str, a2: str, weight: float, edge_type: str, **extra: Any) -> None:
        if a1 == a2:
            return
        key = (a1, a2, edge_type) if a1 < a2 else (a2, a1, edge_type)
        if key in seen_edges:
            return
        seen_edges.add(key)
        edges.append({
            "source": key[0],
            "target": key[1],
            "weight": round(float(weight), 3),
            "edge_type": edge_type,
            **extra,
        })
        edge_layers[edge_type] = edge_layers.get(edge_type, 0) + 1

    # 1) Semantic neighbourhood — mutual k-NN over author embeddings.
    from alma.ai.neighbor_graph import mutual_knn_edges

    for a1, a2, sim in mutual_knn_edges(
        {aid: author_embeddings[aid].tolist() for aid in embedded_ids},
        k=6,
        min_similarity=0.5,
    ):
        _emit_edge(a1, a2, sim, "semantic")

    # 2) Co-authorship — shared papers.
    for (a1, a2), shared in shared_pairs.items():
        weight = 0.5 + 0.5 * (shared / max_shared if max_shared else 0.0)
        _emit_edge(a1, a2, weight, "co_authorship", shared_papers=shared)

    # 3) Bibliographic coupling — shared references. Authors in one field
    # couple with almost everyone (all-pairs ≥1 shared ref is O(n²) and would
    # flood the graph — 15k+ edges on this library). Keep only each author's
    # top-k strongest coupling partners so the layer stays sparse + readable,
    # the same sparsification idea as the mutual-kNN semantic layer.
    for (a1, a2), shared in _top_k_pairs_per_node(bib_pairs, k=4).items():
        weight = 0.4 + 0.5 * (shared / max_bib if max_bib else 0.0)
        _emit_edge(a1, a2, weight, "bibliographic_coupling", shared_refs=shared)

    # ── Honest clustering over author embeddings (Phase 2 recipe, I-11) ──────
    # Unifies with the paper map: HDBSCAN-eom + retained outliers via
    # cluster_publications (no silhouette-kmeans, no forced-K). Authors HDBSCAN
    # can't confidently place stay Unclustered (cluster_id=None) — same as
    # authors with no embedding. Citation/h-index never enter the feature
    # vector (display only); position + clusters come from SPECTER2-mean alone.
    from alma.ai.clustering import cluster_publications, score_cluster_terms

    cluster_ids = np.full(n_authors, -1, dtype=np.int32)
    coords_by_author: dict[str, tuple[float, float]] = {}
    clustering_method = "no_embeddings"
    clustering_panel: dict[str, Any] = {}

    if len(embedded_ids) >= 3:
        emb_map = {aid: author_embeddings[aid].tolist() for aid in embedded_ids}
        result = cluster_publications(emb_map)
        clustering_method = result.method
        for c_idx, cluster in enumerate(result.clusters):
            for aid in cluster.member_keys:
                cluster_ids[author_index[aid]] = c_idx
        clustering_panel = {
            "method": result.method,
            "n_clusters": result.n_clusters,
            "outlier_count": len(result.outliers),
            "coverage": round(result.coverage, 4),
            "params": result.params,
        }
        try:
            # 2-D layout from the same SPECTER2 input via the cosine UMAP path,
            # so visual neighbourhood and clusters share one geometry.
            coords_by_author.update(project_embeddings(emb_map))
        except Exception as exc:
            logger.warning("Author embedding projection failed; using fallback layout: %s", exc)

    cluster_members: dict[int, list[int]] = defaultdict(list)
    for idx, cid in enumerate(cluster_ids):
        if int(cid) >= 0:
            cluster_members[int(cid)].append(idx)

    unplaced_indices = [
        i for i, aid in enumerate(author_ids) if aid not in author_embeddings
    ]

    # Cluster labels — prevalence-weighted c-TF-IDF over each cluster's authors'
    # topic terms (shared scorer; same fix as the paper map). A label term must
    # recur across the cluster's AUTHORS, not sit in one prolific author.
    member_topic_docs = {
        cid: [
            " ".join(topic_weights_by_author.get(author_ids[idx], {}).keys())
            for idx in members
        ]
        for cid, members in cluster_members.items()
    }
    scored_terms = score_cluster_terms(member_topic_docs, ngram_range=(1, 2), top_k=4)
    cluster_topic_labels: dict[int, str] = {}
    for cid in cluster_members:
        terms = [term for term, _w in scored_terms.get(cid, [])][:2]
        cluster_topic_labels[cid] = ", ".join(terms) if terms else f"Cluster {cid + 1}"

    # Ensure coords exist for every author. Authors without embeddings are
    # scattered around the edge so they don't pile on top of each other.
    n_unplaced = len(unplaced_indices)
    for offset, idx in enumerate(unplaced_indices):
        angle = 2 * math.pi * offset / max(1, n_unplaced)
        coords_by_author[author_ids[idx]] = (
            0.5 + 0.48 * math.cos(angle),
            0.5 + 0.48 * math.sin(angle),
        )
    for aid in author_ids:
        coords_by_author.setdefault(aid, (0.5, 0.5))

    nodes = []
    for idx, aid in enumerate(author_ids):
        x_raw, y_raw = coords_by_author[aid]
        x = float(min(0.98, max(0.02, x_raw)))
        y = float(min(0.98, max(0.02, y_raw)))
        raw_cid = int(cluster_ids[idx])
        cid = raw_cid if raw_cid >= 0 else None
        nodes.append(
            {
                "id": aid,
                "name": author_names.get(aid, aid),
                "pub_count": pub_counts.get(aid, 0),
                "citation_count": citation_counts.get(aid, 0),
                "affiliation": affiliations.get(aid, ""),
                "author_citedby": author_citedby.get(aid, 0),
                "h_index": h_indices.get(aid, 0),
                "works_count": works_counts.get(aid, 0),
                "orcid": orcids.get(aid, ""),
                "openalex_id": openalex_ids.get(aid, ""),
                "top_topic": top_topic_by_author.get(aid),
                "interests": author_interests.get(aid, []),
                "cluster_id": cid,
                "cluster_label": (
                    cluster_topic_labels.get(cid) if cid is not None else None
                ),
                "is_outlier": cid is None,
                "x": x,
                "y": y,
            }
        )

    clusters = [
        {
            "id": int(cid),
            "label": cluster_topic_labels.get(cid, f"Cluster {cid + 1}"),
            "size": len(members),
            "member_ids": [author_ids[idx] for idx in members],
        }
        for cid, members in sorted(cluster_members.items(), key=lambda kv: kv[0])
    ]

    return {
        "nodes": nodes,
        "edges": edges,
        "clusters": clusters,
        "method": clustering_method,
        "edge_layers": edge_layers,
        "clustering": clustering_panel,
    }


def _author_bibliographic_coupling(
    conn: sqlite3.Connection,
    author_ids: list[str],
    *,
    scope: str = "library",
) -> tuple[dict[tuple[str, str], int], int]:
    """Count shared references between every pair of authors.

    Bibliographic coupling (BC) = number of distinct works cited by
    *both* authors. Returns ({(a1, a2): count}, max_count) with
    a1 < a2 for deterministic keys. Silently returns ({}, 0) when the
    ``publication_references`` table is missing.
    """
    if not author_ids or not _table_exists(conn, "publication_references"):
        return {}, 0

    placeholders = ",".join(["?"] * len(author_ids))
    status_filter = Scope.parse(scope).paper_filter("p")
    try:
        rows = conn.execute(
            f"""
            WITH author_refs AS (
                SELECT DISTINCT pa.openalex_id AS author_id,
                                r.referenced_work_id AS ref
                FROM publication_references r
                JOIN papers p ON p.id = r.paper_id
                JOIN publication_authors pa ON pa.paper_id = r.paper_id
                WHERE pa.openalex_id IN ({placeholders}){status_filter}
            )
            SELECT ar1.author_id AS a1,
                   ar2.author_id AS a2,
                   COUNT(*) AS shared_refs
            FROM author_refs ar1
            JOIN author_refs ar2 ON ar1.ref = ar2.ref AND ar1.author_id < ar2.author_id
            GROUP BY ar1.author_id, ar2.author_id
            """,
            author_ids,
        ).fetchall()
    except sqlite3.OperationalError as exc:
        logger.warning("Bibliographic coupling query failed: %s", exc)
        return {}, 0

    pairs: dict[tuple[str, str], int] = {}
    max_shared = 0
    for row in rows:
        a1 = row["a1"] if isinstance(row, sqlite3.Row) else row[0]
        a2 = row["a2"] if isinstance(row, sqlite3.Row) else row[1]
        count = int(row["shared_refs"] if isinstance(row, sqlite3.Row) else row[2])
        pairs[(a1, a2)] = count
        if count > max_shared:
            max_shared = count
    return pairs, max_shared


def _author_mean_embeddings(
    conn: sqlite3.Connection,
    author_ids: list[str],
) -> dict[str, np.ndarray]:
    """Return the mean publication embedding for each author.

    Uses the currently active embedding model (e.g. SPECTER2). The author
    vector is built from *every* stored paper by that author — not just
    the library subset — because the author's research identity lives in
    their full output, and restricting the mean to the user's Library
    would make an author's point in the embedding space move every time
    the user saved or removed a paper. Authors with zero embedded papers
    are omitted so the caller can place them in a fallback cluster.
    """
    if not author_ids:
        return {}

    try:
        from alma.discovery.similarity import get_active_embedding_model
    except Exception as exc:
        logger.warning("Could not import active embedding model helper: %s", exc)
        return {}

    active_model = get_active_embedding_model(conn)
    placeholders = ",".join(["?"] * len(author_ids))
    try:
        rows = conn.execute(
            f"""
            SELECT pa.openalex_id AS author_id, pe.embedding
            FROM publication_embeddings pe
            JOIN publication_authors pa ON pa.paper_id = pe.paper_id
            WHERE pa.openalex_id IN ({placeholders})
              AND pe.model = ?
            """,
            list(author_ids) + [active_model],
        ).fetchall()
    except sqlite3.OperationalError:
        return {}

    # Decode through the canonical helper — vectors are stored as
    # float16 since commit 918e5fc, so the old struct-unpack-as-float32
    # path produced half-dim garbage. We pass through every blob once
    # to find the modal byte length, derive the canonical dim from it
    # (assuming float16 storage — same target_dim works for legacy
    # float32 rows because byte length is exactly 2× there), then
    # decode each blob with `expected_dim` so legacy rows auto-rescue.
    from collections import Counter
    from alma.core.vector_blob import decode_vector

    pairs: list[tuple[str, bytes]] = []
    for row in rows:
        if isinstance(row, sqlite3.Row):
            aid = row["author_id"]
            blob = row["embedding"]
        else:
            aid = row[0]
            blob = row[1]
        if not blob:
            continue
        pairs.append((aid, blob))

    if not pairs:
        return {}
    modal_len = Counter(len(b) for _, b in pairs).most_common(1)[0][0]
    if modal_len % 2 != 0:
        return {}
    target_dim = modal_len // 2  # canonical float16 width

    sums: dict[str, np.ndarray] = {}
    counts: dict[str, int] = {}
    for aid, blob in pairs:
        try:
            vec = decode_vector(blob, expected_dim=target_dim)
        except Exception:
            continue
        if vec.shape[0] != target_dim:
            continue
        if aid in sums:
            sums[aid] += vec
            counts[aid] += 1
        else:
            sums[aid] = vec.copy()
            counts[aid] = 1

    return {aid: sums[aid] / counts[aid] for aid in sums if counts[aid] > 0}
