"""The ONE embedding-graph machine, shared by the paper map AND the author network.

Both graphs are the same pipeline over different entities: L2-normalised SPECTER2
vectors → cluster → 2-D project → typed edges → c-TF-IDF labels. They used to be
two separate implementations (`graphs.py::_build_embedding_paper_map`,
`projections.py::build_coauthor_network`) that drifted every time one was fixed —
shared kNN, the co-occurrence primitive, the labelling source, topic removal all
had to be applied twice. This module is the single pipeline; the two graphs are
thin adapters that only load entity vectors/text/metadata and shape node payloads.

A graph is fully described by:
  * ``embeddings``  — id → SPECTER2 vector (per-paper, or per-author mean)
  * ``node_text``   — id → text for c-TF-IDF cluster labels (title text; the noisy
    OpenAlex/S2 topic vocabulary is gone — labels come from real text everywhere)
  * ``coupling_specs`` — the typed structural edge layers (co-authorship,
    bibliographic coupling), each a :class:`CouplingSpec` whose ``entity_features``
    the adapter builds with one indexed scan and the pipeline pairs via the shared
    ``cooccurrence_pairs``.

The semantic mutual-kNN layer + clustering + projection (+ optional fused layout)
are identical for both. ``build_typed_edges`` is also exposed on its own so the
paper map's incremental-cache modes (which reuse a persisted layout rather than
re-clustering) can still rebuild edges through the same code.
"""

from __future__ import annotations

from collections import defaultdict
from collections.abc import Iterable, Mapping
from dataclasses import dataclass, field
from typing import Any

from alma.ai.clustering import cluster_publications, score_cluster_terms
from alma.ai.cooccurrence import cooccurrence_pairs
from alma.ai.neighbor_graph import mutual_knn_edges

# ── Inputs ───────────────────────────────────────────────────────────────────

@dataclass
class CouplingSpec:
    """One typed structural edge layer (co-authorship / bibliographic coupling).

    ``entity_features`` maps each node id to its features (a paper's authors, an
    author's papers, …); the pipeline pairs them through ``cooccurrence_pairs``
    (inverted index + df cap) and emits one edge per sharing pair. Edge weight is
    ``weight_floor + weight_span * (shared / max_shared)`` (normalised) or, when
    ``linear_capped``, ``min(1.0, weight_floor + weight_span * shared)``.
    """

    edge_type: str
    # Supply EITHER entity_features (the pipeline pairs them via cooccurrence_pairs)
    # OR precomputed pairs (when the adapter already computed them — e.g. the paper
    # map reuses the same pair dict for edges AND its post-persist fused layout).
    entity_features: Mapping[str, Iterable[str]] = field(default_factory=dict)
    pairs: dict[tuple[str, str], int] | None = None
    min_shared: int = 1
    max_feature_df: int | None = None
    weight_floor: float = 0.4
    weight_span: float = 0.5
    weight_mode: str = "normalized"  # "normalized" | "linear_capped"
    top_k_per_node: int | None = None  # sparsify to each node's strongest k
    use_for_fusion: bool = False  # feed this layer's pairs into fuse_layout

    def shared_pairs(self) -> dict[tuple[str, str], int]:
        """The coupling pairs — precomputed if given, else paired on demand."""
        if self.pairs is not None:
            return self.pairs
        return cooccurrence_pairs(
            self.entity_features, min_shared=self.min_shared, max_feature_df=self.max_feature_df
        )


@dataclass
class EmbeddingGraphResult:
    """Everything the machine produces; adapters read what they need."""

    cluster_ids: dict[str, int]  # node id → dense cluster id (-1 = outlier)
    cluster_members: dict[int, list[str]]
    outliers: list[str]
    coords: dict[str, tuple[float, float]]
    probabilities: dict[str, float]
    labels_by_cluster: dict[int, str]
    word_clouds: dict[int, list[dict[str, Any]]]
    edges: list[dict[str, Any]]
    edge_layers: dict[str, int]
    clustering_meta: dict[str, Any]


# ── Edge construction (shared; also used standalone by the incremental path) ──

def _top_k_pairs_per_node(
    pairs: dict[tuple[str, str], int], k: int
) -> dict[tuple[str, str], int]:
    """Keep only each node's ``k`` strongest pairs (symmetric union).

    A dense coupling layer (every author in a field couples with everyone) floods
    the graph; capping per-node keeps it sparse + readable, like the mutual-kNN
    semantic layer. A pair survives if it is in the top-k of EITHER endpoint.
    """
    if k <= 0 or not pairs:
        return dict(pairs)
    by_node: dict[str, list[tuple[int, tuple[str, str]]]] = defaultdict(list)
    for pair, shared in pairs.items():
        by_node[pair[0]].append((shared, pair))
        by_node[pair[1]].append((shared, pair))
    keep: set[tuple[str, str]] = set()
    for entries in by_node.values():
        entries.sort(key=lambda sp: sp[0], reverse=True)
        for _shared, pair in entries[:k]:
            keep.add(pair)
    return {pair: shared for pair, shared in pairs.items() if pair in keep}


def build_typed_edges(
    embeddings: Mapping[str, list[float]],
    *,
    coupling_specs: Iterable[CouplingSpec] = (),
    semantic_k: int = 8,
    semantic_min_similarity: float = 0.45,
    exclude_ids: frozenset[str] = frozenset(),
) -> tuple[list[dict[str, Any]], dict[str, int]]:
    """Build the typed edge layers shared by both graphs.

    Layer 1 is always the semantic mutual-kNN over the embeddings; the rest come
    from ``coupling_specs``. ``exclude_ids`` (e.g. retracted papers) keep their
    node but get no edges, so they're never drawn as a hub. One edge per unordered
    pair PER type — a pair can appear in several layers and the UI filters by type.
    Returns ``(edges, edge_layers)`` where edge_layers is the per-type count.
    """
    edges: list[dict[str, Any]] = []
    edge_layers: dict[str, int] = {}
    seen: set[tuple[str, str, str]] = set()

    def _emit(a: str, b: str, weight: float, edge_type: str) -> None:
        if a == b or a in exclude_ids or b in exclude_ids:
            return
        key = (a, b, edge_type) if a < b else (b, a, edge_type)
        if key in seen:
            return
        seen.add(key)
        edges.append({
            "source": key[0],
            "target": key[1],
            "weight": round(float(weight), 3),
            "edge_type": edge_type,
        })
        edge_layers[edge_type] = edge_layers.get(edge_type, 0) + 1

    # 1) Semantic neighbourhood — mutual k-NN in the 768-d SPECTER2 space.
    semantic_input = {k: v for k, v in embeddings.items() if k not in exclude_ids}
    for a, b, sim in mutual_knn_edges(
        semantic_input, k=semantic_k, min_similarity=semantic_min_similarity
    ):
        _emit(a, b, sim, "semantic")

    # 2..N) Structural coupling layers via the shared co-occurrence primitive.
    for spec in coupling_specs:
        pairs = spec.shared_pairs()
        if spec.top_k_per_node:
            pairs = _top_k_pairs_per_node(pairs, spec.top_k_per_node)
        max_shared = max(pairs.values(), default=0)
        for (a, b), shared in pairs.items():
            if spec.weight_mode == "linear_capped":
                weight = min(1.0, spec.weight_floor + spec.weight_span * shared)
            else:  # normalized
                frac = (shared / max_shared) if max_shared else 0.0
                weight = spec.weight_floor + spec.weight_span * frac
            _emit(a, b, weight, spec.edge_type)

    return edges, edge_layers


# ── The full machine ─────────────────────────────────────────────────────────

def build_embedding_graph(
    embeddings: dict[str, list[float]],
    *,
    node_text: Mapping[str, str],
    resolution: float = 1.0,
    layout_weights: dict | None = None,
    coupling_specs: Iterable[CouplingSpec] = (),
    semantic_k: int = 8,
    semantic_min_similarity: float = 0.45,
    exclude_from_edges: frozenset[str] = frozenset(),
    compute_stability: bool = False,
    label_top_k: int = 2,
    word_cloud_top_k: int = 10,
) -> EmbeddingGraphResult:
    """Run the full embedding-graph machine for any entity set.

    Pipeline: shared cosine kNN → HDBSCAN-eom clustering (retained outliers) →
    2-D cosine projection (optionally fused with the structural layers) → typed
    edges → prevalence-weighted c-TF-IDF labels + word clouds from ``node_text``.
    Coordinates are normalised to [0, 1]. Layout/edge/label params let the two
    adapters keep their (small) historical differences without forking the code.
    """
    from alma.ai import accel
    from alma.ai.projections import fuse_layout, project_embeddings

    coupling_specs = list(coupling_specs)
    ids = list(embeddings)

    # One cosine kNN graph shared by the clustering substrate AND the 2-D fit.
    shared_knn = accel.shared_cosine_knn(embeddings)

    clustering = cluster_publications(
        embeddings,
        resolution=resolution,
        compute_stability=compute_stability,
        precomputed_knn=shared_knn,
    )

    cluster_ids: dict[str, int] = {}
    cluster_members: dict[int, list[str]] = {}
    for cluster in clustering.clusters:
        cid = int(cluster.cluster_id)
        cluster_members[cid] = list(cluster.member_keys)
        for key in cluster.member_keys:
            cluster_ids[key] = cid
    for key in clustering.outliers:
        cluster_ids[key] = -1

    # 2-D projection (shared kNN), optionally fused with the structural layers.
    coords = project_embeddings(embeddings, precomputed_knn=shared_knn)
    lw = layout_weights or {}
    fused_on = float(lw.get("coauthorship", 0) or 0) > 0 or float(
        lw.get("bibliographic_coupling", 0) or 0
    ) > 0
    if fused_on and len(ids) <= 1500:
        try:
            fusion = {
                s.edge_type: s.shared_pairs() for s in coupling_specs if s.use_for_fusion
            }
            fused = fuse_layout(
                embeddings,
                fusion.get("co_authorship", {}),
                fusion.get("bibliographic_coupling", {}),
                weights=lw,
                init_coords=coords,
            )
            if fused:
                coords = fused
        except Exception:  # pragma: no cover - fused layout is best-effort
            pass

    edges, edge_layers = build_typed_edges(
        embeddings,
        coupling_specs=coupling_specs,
        semantic_k=semantic_k,
        semantic_min_similarity=semantic_min_similarity,
        exclude_ids=exclude_from_edges,
    )

    # Labels + word clouds — prevalence-weighted c-TF-IDF over each cluster's
    # member TEXT (the same scorer + real-text source for every graph).
    member_text_docs = {
        cid: [str(node_text.get(key, "") or "") for key in members]
        for cid, members in cluster_members.items()
    }
    scored = score_cluster_terms(member_text_docs, ngram_range=(1, 2), top_k=word_cloud_top_k)
    labels_by_cluster: dict[int, str] = {}
    word_clouds: dict[int, list[dict[str, Any]]] = {}
    for cid in cluster_members:
        ranked = scored.get(cid, [])
        terms = [term for term, _w in ranked][:label_top_k]
        labels_by_cluster[cid] = ", ".join(terms) if terms else f"Cluster {cid + 1}"
        word_clouds[cid] = [
            {"term": term, "weight": round(weight, 4)} for term, weight in ranked[:word_cloud_top_k]
        ]

    return EmbeddingGraphResult(
        cluster_ids=cluster_ids,
        cluster_members=cluster_members,
        outliers=list(clustering.outliers),
        coords=coords,
        probabilities=dict(clustering.probabilities),
        labels_by_cluster=labels_by_cluster,
        word_clouds=word_clouds,
        edges=edges,
        edge_layers=edge_layers,
        clustering_meta={
            "method": clustering.method,
            "n_clusters": clustering.n_clusters,
            "outlier_count": len(clustering.outliers),
            "coverage": round(clustering.coverage, 4),
            "stability": clustering.stability,
            "params": clustering.params,
        },
    )
