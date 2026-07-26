"""2D projection of publication embeddings for visualization."""

import hashlib
import json
import logging
import math
import sqlite3
from collections import defaultdict
from typing import Any

import numpy as np

from alma.ai.clustering import cluster_preprojected, score_cluster_terms
from alma.application.graph_substrate import SUBSTRATE_SCOPE
from alma.core.scope import Scope
from alma.core.sql_helpers import standalone_paper_sql

logger = logging.getLogger(__name__)

# Minimum in-scope papers for an author to appear in the network. A one-paper
# author has too thin a semantic basis to place meaningfully. The shared corpus
# substrate also needs three placeable authors for an honest density grouping.
# Export both constants so the
# API's cheap empty-cache guard cannot drift from the builder's admission rules.
MIN_AUTHOR_PUBLICATIONS = 2
MIN_AUTHOR_LAYOUT_NODES = 3

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
    *,
    precomputed_knn=None,
) -> dict[str, tuple[float, float]]:
    """Project embeddings to 2D for visualization.

    Args:
        embeddings: Map of paper_id -> embedding vector.
        method: "umap", "tsne", or "auto" (UMAP if available, else t-SNE).
        precomputed_knn: Optional ``(knn_indices, knn_dists)`` cosine graph shared
            with the clustering fit (task #21) so the corpus build runs one
            neighbour search instead of two. ``None`` lets UMAP build its own.
            UMAP/GPU dispatch + bounded epochs live in :mod:`alma.ai.accel`.

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
        from alma.ai import accel

        coords_2d = accel.umap_fit(
            vectors,
            n_components=2,
            n_neighbors=min(15, len(keys) - 1),
            min_dist=0.1,
            metric="cosine",
            random_state=42,
            precomputed_knn=precomputed_knn,
        )
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


def fuse_layout(
    embeddings: dict[str, list[float]],
    coauth_pairs: dict[tuple[str, str], int],
    bib_pairs: dict[tuple[str, str], int],
    *,
    weights: dict[str, float],
    cocite_pairs: dict[tuple[str, str], int] | None = None,
    init_coords: dict[str, tuple[float, float]] | None = None,
) -> dict[str, tuple[float, float]]:
    """PROTOTYPE multi-view layout (task 19) — fuse several relationship signals
    into ONE 2-D layout so the *geometry* reflects the chosen blend.

    Builds a per-signal pairwise DISTANCE matrix — semantic (cosine on the
    embeddings), co-authorship (shared authors), bibliographic coupling (shared
    references) and co-citation (shared citers, the citation fabric's forward
    view) — blends them by ``weights`` and runs UMAP(metric="precomputed").

    * **Weights are INDEPENDENT** (not renormalized): each is the raw 0..1
      contribution of that source, used together. semantic 1 / coauth 1 / bib 0
      means "semantic and co-authorship equally". All-zero falls back to semantic.
    * **``init_coords`` anchors the layout.** Passing the semantic 2-D coords as
      the UMAP init makes every blend START from the same arrangement and only
      move points by what the blend changes — so dragging a weight slider nudges
      the map instead of reshuffling it, and the pure-semantic blend ≈ the
      default map (no discontinuity).

    Dense O(N²): LIBRARY scale only (the corpus needs the sparse fuzzy-graph
    union, still task 19). Returns {} on failure → caller keeps the semantic map.
    """
    ids = list(embeddings)
    n = len(ids)
    if n < 3:
        return {pid: (0.5, 0.5) for pid in ids}

    # Independent (un-normalized) weights — each source's raw contribution.
    w_sem = max(0.0, float(weights.get("semantic", 1.0) or 0.0))
    w_co = max(0.0, float(weights.get("coauthorship", 0.0) or 0.0))
    w_bib = max(0.0, float(weights.get("bibliographic_coupling", 0.0) or 0.0))
    w_cocite = max(0.0, float(weights.get("co_citation", 0.0) or 0.0))
    if w_sem + w_co + w_bib + w_cocite <= 0:
        w_sem = 1.0  # all-zero is degenerate → pure semantic

    idx = {pid: i for i, pid in enumerate(ids)}
    matrix = np.asarray([embeddings[pid] for pid in ids], dtype=np.float64)
    norms = np.linalg.norm(matrix, axis=1, keepdims=True)
    norms[norms == 0] = 1.0
    normalized = matrix / norms
    # Semantic distance in [0, 1] (cosine sim → distance).
    d_sem = np.clip(
        (1.0 - np.clip(normalized @ normalized.T, -1.0, 1.0)) / 2.0,
        0.0,
        1.0,
    )

    def _pair_distance(pairs: dict[tuple[str, str], int]) -> np.ndarray:
        # Affinity (shared count, normalized to the strongest pair) → distance.
        # No shared signal ⇒ distance 1 (maximally far on this axis).
        d = np.ones((n, n), dtype=np.float64)
        np.fill_diagonal(d, 0.0)
        if not pairs:
            return d
        mx = max(pairs.values()) or 1
        for (a, b), c in pairs.items():
            if a in idx and b in idx:
                aff = min(1.0, c / mx)
                d[idx[a], idx[b]] = d[idx[b], idx[a]] = 1.0 - aff
        return d

    d = w_sem * d_sem
    if w_co > 0:
        d = d + w_co * _pair_distance(coauth_pairs)
    if w_bib > 0:
        d = d + w_bib * _pair_distance(bib_pairs)
    if w_cocite > 0 and cocite_pairs:
        d = d + w_cocite * _pair_distance(cocite_pairs)
    np.fill_diagonal(d, 0.0)
    d = (d + d.T) / 2.0  # enforce exact symmetry for the precomputed metric

    # Anchor the optimization at the semantic layout (when provided) so every
    # blend starts from the same arrangement and only moves points by what the
    # blend changes — stable across slider steps, continuous from the default.
    init: Any = "spectral"
    if init_coords:
        init = np.asarray(
            [init_coords.get(pid, (0.5, 0.5)) for pid in ids], dtype=np.float64
        )

    try:
        if not _UMAP_AVAILABLE:
            return {}
        reducer = _umap.UMAP(
            n_components=2,
            n_neighbors=min(15, n - 1),
            min_dist=0.1,
            metric="precomputed",
            init=init,
            random_state=42,
        )
        coords_2d = np.asarray(reducer.fit_transform(d), dtype=np.float64)
    except Exception:
        return {}

    if init_coords:
        # A UMAP layout is only defined up to rotation / reflection / scale /
        # translation, so even with the same init two blends land in different
        # frames and a raw comparison looks like a total reshuffle. Procrustes-
        # align each blend onto the SEMANTIC frame: now every blend shares the
        # default's orientation, so adjacent slider steps differ by a small,
        # meaningful nudge instead of a rigid flip.
        try:
            from scipy.linalg import orthogonal_procrustes

            target = np.asarray(
                [init_coords.get(pid, (0.5, 0.5)) for pid in ids], dtype=np.float64
            )
            a_c = coords_2d - coords_2d.mean(axis=0)
            b_c = target - target.mean(axis=0)
            rot, _ = orthogonal_procrustes(a_c, b_c)
            aligned = a_c @ rot
            sa = float(np.linalg.norm(a_c))
            sb = float(np.linalg.norm(b_c))
            if sa > 0:
                aligned = aligned * (sb / sa)
            coords_2d = np.clip(aligned + target.mean(axis=0), 0.0, 1.0)
            return {ids[i]: (float(coords_2d[i, 0]), float(coords_2d[i, 1])) for i in range(n)}
        except Exception:
            pass  # fall through to plain min-max normalization

    mins = coords_2d.min(axis=0)
    maxs = coords_2d.max(axis=0)
    ranges = maxs - mins
    ranges[ranges == 0] = 1
    coords_2d = (coords_2d - mins) / ranges
    return {ids[i]: (float(coords_2d[i, 0]), float(coords_2d[i, 1])) for i in range(n)}


def _author_candidate_count(
    conn: sqlite3.Connection,
    *,
    scope: str,
) -> int:
    """Count two-paper authors, including those hidden for thin embeddings."""
    scope_filter = Scope.parse(scope).paper_filter("p")
    try:
        row = conn.execute(
            f"""
            SELECT COUNT(*) FROM (
                SELECT pa.openalex_id
                FROM publication_authors pa
                JOIN papers p ON p.id = pa.paper_id
                WHERE TRIM(COALESCE(pa.openalex_id, '')) <> ''{scope_filter}
                GROUP BY pa.openalex_id
                HAVING COUNT(DISTINCT pa.paper_id) >= ?
            )
            """,
            (MIN_AUTHOR_PUBLICATIONS,),
        ).fetchone()
    except sqlite3.OperationalError:
        return 0
    return int((row[0] if row else 0) or 0)


def _author_coordinate_jitter(author_id: str) -> tuple[float, float]:
    """Small stable offset so identical co-author centroids remain clickable."""
    digest = hashlib.sha1(author_id.encode()).hexdigest()
    angle = (int(digest[:8], 16) / float(16**8)) * 2.0 * math.pi
    radius = 0.004 + (int(digest[8:12], 16) / float(16**4)) * 0.006
    return math.cos(angle) * radius, math.sin(angle) * radius


def build_coauthor_network(
    conn: sqlite3.Connection,
    *,
    scope: str = "library",
    cluster_resolution: float = 1.0,
    layout_weights: dict[str, float] | None = None,
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
    status_filter = Scope.parse(scope).paper_filter("p")

    # Authors keyed by OpenAlex ID, with publication stats + (optional)
    # profile enrichment from the `authors` table.
    try:
        authors_data = conn.execute(
            f"""
            WITH placeable AS (
                SELECT pa.openalex_id AS author_id,
                       AVG(pc.x) AS substrate_x,
                       AVG(pc.y) AS substrate_y
                FROM publication_authors pa
                JOIN papers p ON p.id = pa.paper_id
                JOIN publication_clusters pc
                  ON pc.paper_id = pa.paper_id
                 AND pc.scope = ?
                WHERE TRIM(COALESCE(pa.openalex_id, '')) <> ''{status_filter}
                GROUP BY pa.openalex_id
                HAVING COUNT(DISTINCT pc.paper_id) >= ?
            )
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
                   COALESCE(a.interests, '') AS interests,
                   placeable.substrate_x,
                   placeable.substrate_y
            FROM publication_authors pa
            JOIN papers p ON p.id = pa.paper_id
            JOIN placeable ON placeable.author_id = pa.openalex_id
            LEFT JOIN authors a ON lower(a.openalex_id) = lower(pa.openalex_id)
            WHERE pa.openalex_id <> ''{status_filter}
              AND {standalone_paper_sql('p')}
            GROUP BY pa.openalex_id,
                     a.name,
                     a.affiliation,
                     a.citedby,
                     a.h_index,
                     a.works_count,
                     a.orcid,
                     a.openalex_id,
                     a.interests,
                     placeable.substrate_x,
                     placeable.substrate_y
            ORDER BY pub_count DESC
            """,
            (SUBSTRATE_SCOPE, MIN_AUTHOR_PUBLICATIONS),
        ).fetchall()
    except Exception as exc:
        logger.warning("Could not query publication_authors for author network: %s", exc)
        # Every exit carries `omitted_unplaced`: it is part of the payload
        # contract the legend reads, and an absent key is not the same claim as
        # "nothing was omitted".
        return {"nodes": [], "edges": [], "omitted_unplaced": 0}

    if not authors_data:
        return {
            "nodes": [],
            "edges": [],
            "clusters": [],
            "method": "no_substrate_positions",
            "edge_layers": {},
            "clustering": {},
            "omitted_unplaced": _author_candidate_count(conn, scope=scope),
        }

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
    coords_by_author = {
        (r["author_id"] if isinstance(r, sqlite3.Row) else r[0]): (
            float(r["substrate_x"] if isinstance(r, sqlite3.Row) else r[11]),
            float(r["substrate_y"] if isinstance(r, sqlite3.Row) else r[12]),
        )
        for r in authors_data
    }
    n_authors = len(author_ids)
    author_index = {aid: i for i, aid in enumerate(author_ids)}

    # Library membership per author (for corpus-scope dimming): an author is
    # "in library" iff they co-author >=1 paper with status='library'. In library
    # scope every author already qualifies (the node query is already filtered to
    # library papers), so we skip the extra lookup; in corpus scope we resolve it
    # so the UI can dim background-only co-authors to half opacity.
    if Scope.parse(scope) is Scope.library:
        library_author_ids: set[str] = set(author_ids)
    else:
        placeholders = ",".join("?" * len(author_ids))
        lib_rows = conn.execute(
            f"""
            SELECT DISTINCT pa.openalex_id AS aid
            FROM publication_authors pa
            JOIN papers p ON p.id = pa.paper_id
            WHERE pa.openalex_id IN ({placeholders})
              AND p.status = 'library'
              AND {standalone_paper_sql('p')}
            """,
            author_ids,
        ).fetchall()
        library_author_ids = {
            (r["aid"] if isinstance(r, sqlite3.Row) else r[0]) for r in lib_rows
        }

    # Per-author TEXT for cluster labelling — the concatenated titles of each
    # author's papers. This replaces the old per-author `publication_topics` vector
    # (the OpenAlex/S2 topic vocabulary is noisy + ineffective, AND that 4-table
    # query was a corpus bottleneck). Labels now come from the SAME source as the
    # paper map — real title text through the shared c-TF-IDF scorer — so the two
    # graphs label identically. Titles (not abstracts) keep an author's "document"
    # concise: a prolific author would otherwise concatenate dozens of abstracts.
    author_text_by_id: dict[str, str] = {aid: "" for aid in author_ids}
    placeholders = ",".join(["?"] * len(author_ids))
    text_status_filter = Scope.parse(scope).paper_filter("p")
    try:
        title_rows = conn.execute(
            f"""
            SELECT pa.openalex_id AS aid, p.title AS title
            FROM publication_authors pa
            JOIN papers p ON p.id = pa.paper_id
            WHERE pa.openalex_id IN ({placeholders}){text_status_filter}
              AND {standalone_paper_sql('p')}
              AND TRIM(COALESCE(p.title, '')) <> ''
            """,
            author_ids,
        ).fetchall()
        title_parts: dict[str, list[str]] = defaultdict(list)
        for row in title_rows:
            aid = row["aid"] if isinstance(row, sqlite3.Row) else row[0]
            title = row["title"] if isinstance(row, sqlite3.Row) else row[1]
            if aid in author_text_by_id and title:
                title_parts[str(aid)].append(str(title))
        for aid, titles in title_parts.items():
            author_text_by_id[aid] = " ".join(titles)
    except Exception as exc:
        logger.warning("Could not load author title text for labelling: %s", exc)

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

    # Author placement is an AGGREGATION over the ONE durable paper substrate:
    # the mean of an author's in-scope paper coordinates. The former pipeline
    # fitted a second 5-D + 2-D UMAP over 6k+ author means, which took minutes
    # and produced a different coordinate frame despite the UI calling it the
    # "same space". A bounded HDBSCAN pass over these existing 2-D centroids
    # preserves the cluster-detail knob without repeating projection work.
    placed_ids = list(author_ids)

    cluster_ids = np.full(n_authors, -1, dtype=np.int32)
    clustering_method = "no_substrate_positions"
    clustering_panel: dict[str, Any] = {}
    cluster_topic_labels: dict[int, str] = {}
    cluster_word_clouds: dict[int, list[dict[str, Any]]] = {}
    cluster_member_ids: dict[int, list[str]] = {}
    edges: list[dict[str, Any]] = []
    edge_layers: dict[str, int] = {}

    if len(placed_ids) >= MIN_AUTHOR_LAYOUT_NODES:
        clustering = cluster_preprojected(
            {aid: coords_by_author[aid] for aid in placed_ids},
            resolution=cluster_resolution,
        )
        clustering_method = clustering.method
        cluster_member_ids = {
            int(cluster.cluster_id): list(cluster.member_keys)
            for cluster in clustering.clusters
        }
        for cluster_id, members in cluster_member_ids.items():
            for aid in members:
                if aid in author_index:
                    cluster_ids[author_index[aid]] = cluster_id
        member_text_docs = {
            cluster_id: [author_text_by_id.get(aid, "") for aid in members]
            for cluster_id, members in cluster_member_ids.items()
        }
        scored = score_cluster_terms(
            member_text_docs,
            ngram_range=(1, 2),
            top_k=10,
        )
        for cluster_id, ranked in scored.items():
            terms = [term for term, _weight in ranked[:2]]
            cluster_topic_labels[cluster_id] = (
                ", ".join(terms) if terms else f"Cluster {cluster_id + 1}"
            )
            cluster_word_clouds[cluster_id] = [
                {"term": term, "weight": round(float(weight), 4)}
                for term, weight in ranked[:10]
            ]
        clustering_panel = {
            "method": clustering.method,
            "n_clusters": clustering.n_clusters,
            "outlier_count": len(clustering.outliers),
            "coverage": round(clustering.coverage, 4),
            "stability": clustering.stability,
            "params": clustering.params,
        }

    # Authors with no placed paper have NO semantic position. They used to be
    # scattered on a radius-0.48 ring about the centre (and any residual piled
    # exactly at 0.5/0.5) — invented geometry that read as real structure: a
    # halo of unplaceable authors framing the corpus view, in the one region a
    # semantic map claims nothing lives (user call 2026-07-26). A semantic map
    # places by meaning or not at all, so they are OMITTED here and COUNTED, and
    # the count rides the payload so the UI can say how many are missing instead
    # of dropping them silently.
    omitted_unplaced = max(
        0,
        _author_candidate_count(conn, scope=scope) - len(author_ids),
    )

    nodes = []
    for idx, aid in enumerate(author_ids):
        placed = coords_by_author.get(aid)
        if placed is None:
            continue
        x_raw, y_raw = placed
        jitter_x, jitter_y = _author_coordinate_jitter(aid)
        x = float(min(0.98, max(0.02, x_raw + jitter_x)))
        y = float(min(0.98, max(0.02, y_raw + jitter_y)))
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
                "interests": author_interests.get(aid, []),
                "in_library": aid in library_author_ids,
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
            "word_cloud": cluster_word_clouds.get(cid, []),
            "member_ids": list(members),
        }
        for cid, members in sorted(cluster_member_ids.items(), key=lambda kv: kv[0])
    ]

    return {
        "nodes": nodes,
        "edges": edges,
        "clusters": clusters,
        "method": clustering_method,
        "edge_layers": edge_layers,
        "clustering": clustering_panel,
        # How many in-scope authors could not be placed (no embedded paper).
        # Surfaced in the map legend — an omission the reader can see.
        "omitted_unplaced": omitted_unplaced,
    }


def _author_referenced_works(
    conn: sqlite3.Connection,
    author_ids: list[str],
    *,
    scope: str = "library",
) -> dict[str, set[str]]:
    """Map each author → the set of works their papers cite.

    The entity→features input the shared pipeline pairs into bibliographic-coupling
    edges (one indexed scan; the pairing + df cap live in ``cooccurrence_pairs``).
    Returns {} when ``publication_references`` is missing.
    """
    out: dict[str, set[str]] = defaultdict(set)
    if not author_ids or not _table_exists(conn, "publication_references"):
        return out
    placeholders = ",".join(["?"] * len(author_ids))
    status_filter = Scope.parse(scope).paper_filter("p")
    try:
        rows = conn.execute(
            f"""
            SELECT DISTINCT pa.openalex_id AS author_id, r.referenced_work_id AS ref
            FROM publication_references r
            JOIN papers p ON p.id = r.paper_id
            JOIN publication_authors pa ON pa.paper_id = r.paper_id
            WHERE pa.openalex_id IN ({placeholders}){status_filter}
              AND {standalone_paper_sql('p')}
              AND TRIM(COALESCE(r.referenced_work_id, '')) <> ''
            """,
            author_ids,
        ).fetchall()
    except sqlite3.OperationalError as exc:
        logger.warning("Author referenced-works query failed: %s", exc)
        return out
    for row in rows:
        aid = row["author_id"] if isinstance(row, sqlite3.Row) else row[0]
        ref = row["ref"] if isinstance(row, sqlite3.Row) else row[1]
        out[str(aid)].add(str(ref))
    return out


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
            JOIN papers p ON p.id = pe.paper_id
            WHERE pa.openalex_id IN ({placeholders})
              AND pe.model = ?
              AND {standalone_paper_sql('p')}
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
