"""2D projection of publication embeddings for visualization."""

from collections import defaultdict
import json
import logging
import math
import sqlite3

import numpy as np

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


def build_coauthor_network(
    conn: sqlite3.Connection,
    *,
    scope: str = "library",
) -> dict:
    """Build a co-authorship + topic-similarity author network.

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
    status_filter = " AND p.status = 'library'" if scope == "library" else ""

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
        topic_status_filter = " AND p.status = 'library'" if scope == "library" else ""
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

    # Build TF-IDF-like matrix across authors.
    all_terms: list[str] = sorted(
        {term for terms in topic_weights_by_author.values() for term in terms.keys()}
    )
    if all_terms:
        term_index = {t: i for i, t in enumerate(all_terms)}
        matrix = np.zeros((n_authors, len(all_terms)), dtype=np.float32)
        for aid, term_weights in topic_weights_by_author.items():
            i = author_index[aid]
            for term, weight in term_weights.items():
                matrix[i, term_index[term]] = float(weight)

        df = np.count_nonzero(matrix > 0, axis=0)
        idf = np.log((1.0 + n_authors) / (1.0 + df)) + 1.0
        topic_matrix = _safe_norm_rows(matrix * idf)
        topic_similarity = np.clip(topic_matrix @ topic_matrix.T, 0.0, 1.0)
    else:
        topic_similarity = np.zeros((n_authors, n_authors), dtype=np.float32)

    # Direct co-authorship signal: how many papers two authors share.
    # Under the v3 schema this is a clean junction-table lookup —
    # ``publication_authors`` already tracks every (paper, author) link.
    shared_pairs: dict[tuple[str, str], int] = {}
    max_shared = 0
    try:
        placeholders = ",".join(["?"] * len(author_ids))
        shared_status_filter = " AND p.status = 'library'" if scope == "library" else ""
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

    stat_features = np.array(
        [
            [
                math.log1p(pub_counts.get(aid, 0)),
                math.log1p(citation_counts.get(aid, 0)),
            ]
            for aid in author_ids
        ],
        dtype=np.float32,
    )
    stats_similarity = np.eye(n_authors, dtype=np.float32)
    for i in range(n_authors):
        for j in range(i + 1, n_authors):
            delta_pub = abs(float(stat_features[i, 0] - stat_features[j, 0]))
            delta_cit = abs(float(stat_features[i, 1] - stat_features[j, 1]))
            sim = 1.0 / (1.0 + delta_pub + 0.75 * delta_cit)
            stats_similarity[i, j] = sim
            stats_similarity[j, i] = sim

    # Bibliographic coupling: two authors are more similar when their
    # papers cite the same works. Uses publication_references if present.
    bib_pairs, max_bib = _author_bibliographic_coupling(conn, author_ids, scope=scope)

    combined_similarity = 0.80 * topic_similarity + 0.15 * stats_similarity
    for i in range(n_authors):
        for j in range(i + 1, n_authors):
            topic_sim = float(topic_similarity[i, j])
            stats_sim = float(stats_similarity[i, j])
            a1 = author_ids[i]
            a2 = author_ids[j]
            key = (a1, a2) if (a1, a2) in shared_pairs else (a2, a1)
            shared_norm = (
                shared_pairs.get(key, 0) / max_shared if max_shared > 0 else 0.0
            )
            bib_key = (a1, a2) if (a1, a2) in bib_pairs else (a2, a1)
            bib_norm = (
                bib_pairs.get(bib_key, 0) / max_bib if max_bib > 0 else 0.0
            )
            combined = (
                0.55 * topic_sim
                + 0.10 * stats_sim
                + 0.15 * shared_norm
                + 0.20 * bib_norm
            )
            combined_similarity[i, j] = combined
            combined_similarity[j, i] = combined

    # Build sparse but informative edges.
    min_edge_weight = 0.45
    top_k = 3 if n_authors >= 6 else 2
    edge_pairs: set[tuple[int, int]] = set()

    for i in range(n_authors):
        sims = [(j, float(combined_similarity[i, j])) for j in range(n_authors) if j != i]
        sims = [pair for pair in sims if pair[1] > 0]
        sims.sort(key=lambda x: x[1], reverse=True)
        for j, sim in sims[:top_k]:
            if sim > 0:
                edge_pairs.add((min(i, j), max(i, j)))

    for i in range(n_authors):
        for j in range(i + 1, n_authors):
            a1 = author_ids[i]
            a2 = author_ids[j]
            has_shared_pair = (a1, a2) in shared_pairs or (a2, a1) in shared_pairs
            has_topic_overlap = float(topic_similarity[i, j]) > 0.05
            if (
                float(combined_similarity[i, j]) >= min_edge_weight
                and (has_shared_pair or has_topic_overlap)
            ):
                edge_pairs.add((i, j))

    edges = []
    for i, j in sorted(edge_pairs):
        a1 = author_ids[i]
        a2 = author_ids[j]
        key = (a1, a2) if (a1, a2) in shared_pairs else (a2, a1)
        shared = shared_pairs.get(key, 0)
        shared_topics = len(
            set(topic_weights_by_author[a1].keys()) & set(topic_weights_by_author[a2].keys())
        )
        weight = max(0.05, float(combined_similarity[i, j]))
        edges.append(
            {
                "source": a1,
                "target": a2,
                "weight": round(weight, 3),
                "shared_topics": shared_topics,
                "shared_papers": shared,
            }
        )

    # Cluster + position authors from the mean embedding of their papers
    # (the active publication-embedding model — e.g. SPECTER2). Mixing
    # citation counts into the feature vector produced clusters dominated
    # by "how well-cited" rather than "what the author works on", so
    # metadata like citations / h-index is kept for display only.
    author_embeddings = _author_mean_embeddings(conn, author_ids)
    embedded_ids = [aid for aid in author_ids if aid in author_embeddings]

    cluster_ids = np.zeros(n_authors, dtype=np.int32)
    coords_by_author: dict[str, tuple[float, float]] = {}
    clustering_method = "author_embedding_mean"

    if len(embedded_ids) >= 3:
        matrix = np.stack([author_embeddings[aid] for aid in embedded_ids]).astype(np.float32)
        normalized = _safe_norm_rows(matrix)
        try:
            from sklearn.cluster import MiniBatchKMeans

            from alma.ai.clustering import _silhouette_optimal_k

            # Silhouette-driven k. Ceiling scales with author count so a
            # large corpus isn't forced into eight buckets, but the UI stays
            # legible; floor stays at 2 so tiny corpora still cluster.
            upper = max(3, min(12, int(round(math.sqrt(len(embedded_ids)) * 1.5))))
            n_clusters = _silhouette_optimal_k(normalized, min_k=2, max_k=upper)
            n_clusters = min(n_clusters, max(2, len(embedded_ids) - 1))
            model = MiniBatchKMeans(
                n_clusters=n_clusters,
                random_state=42,
                n_init=5,
                batch_size=min(64, len(embedded_ids)),
            )
            embedded_cluster_ids = model.fit_predict(normalized)
        except Exception as exc:
            logger.warning("Author embedding clustering failed; assigning a single cluster: %s", exc)
            embedded_cluster_ids = np.zeros(len(embedded_ids), dtype=np.int32)

        for aid, cid in zip(embedded_ids, embedded_cluster_ids):
            cluster_ids[author_index[aid]] = int(cid)

        try:
            projected = project_embeddings(
                {aid: author_embeddings[aid].tolist() for aid in embedded_ids}
            )
            coords_by_author.update(projected)
        except Exception as exc:
            logger.warning("Author embedding projection failed; using fallback layout: %s", exc)
    else:
        clustering_method = "topic_similarity_fallback"

    # Authors without embeddings end up in their own "Unplaced" cluster so
    # the consumer can still render them without polluting the semantic
    # clusters that came from real embedding geometry.
    unplaced_indices = [i for i, aid in enumerate(author_ids) if aid not in author_embeddings]
    if unplaced_indices:
        max_cluster = int(cluster_ids.max()) if len(embedded_ids) > 0 else -1
        unplaced_cid = max_cluster + 1
        for idx in unplaced_indices:
            cluster_ids[idx] = unplaced_cid

    unique_clusters = sorted(int(x) for x in np.unique(cluster_ids))
    cluster_map = {old: new for new, old in enumerate(unique_clusters)}
    cluster_ids = np.array([cluster_map[int(cid)] for cid in cluster_ids], dtype=np.int32)

    cluster_members: dict[int, list[int]] = defaultdict(list)
    for idx, cid in enumerate(cluster_ids):
        cluster_members[int(cid)].append(idx)

    cluster_topic_labels: dict[int, str] = {}
    for cid, members in cluster_members.items():
        term_scores: dict[str, float] = defaultdict(float)
        for idx in members:
            aid = author_ids[idx]
            for term, score in topic_weights_by_author.get(aid, {}).items():
                term_scores[term] += score
        top_terms = sorted(term_scores.items(), key=lambda kv: kv[1], reverse=True)[:2]
        if top_terms:
            cluster_topic_labels[cid] = ", ".join(t for t, _ in top_terms)
        else:
            cluster_topic_labels[cid] = f"Cluster {cid + 1}"

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
        cid = int(cluster_ids[idx])
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
                "cluster_label": cluster_topic_labels.get(cid, f"Cluster {cid + 1}"),
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
    status_filter = " AND p.status = 'library'" if scope == "library" else ""
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
