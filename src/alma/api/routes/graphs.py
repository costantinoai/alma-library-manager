"""Graph visualization API endpoints."""

import json
import logging
import math
import sqlite3
import uuid
import hashlib
from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Callable, Optional

import numpy as np
from fastapi import APIRouter, Depends, Query
from pydantic import BaseModel

from alma.api.deps import get_current_user, get_db, open_db_connection
from alma.api.helpers import table_exists
from alma.application import materialized_views as mv
from alma.ai.graph_versions import (
    CLUSTERING_ALGO_VERSION,
    LABELLING_VERSION,
    PROJECTION_ALGO_VERSION,
    with_version,
)
from alma.config import get_db_path
from alma.core.scope import Scope

logger = logging.getLogger(__name__)

router = APIRouter(
    dependencies=[Depends(get_current_user)],
    responses={401: {"description": "Unauthorized"}},
)


# ---------------------------------------------------------------------------
# Response models
# ---------------------------------------------------------------------------


class GraphNode(BaseModel):
    id: str
    name: str
    x: float = 0.5
    y: float = 0.5
    cluster_id: Optional[int] = None
    color: Optional[str] = None
    size: float = 1.0
    node_type: str = "paper"  # "paper" or "topic"
    metadata: dict = {}


class GraphEdge(BaseModel):
    source: str
    target: str
    weight: float = 1.0
    # Typed edge layer (Phase 3 / I-11): "semantic" (mutual-kNN in embedding
    # space), "bibliographic_coupling" (shared references), "co_authorship"
    # (shared authors), or "topic" (paper↔topic overlay). The UI filters by this.
    edge_type: str = "semantic"


class GraphData(BaseModel):
    nodes: list[GraphNode]
    edges: list[GraphEdge]
    metadata: dict = {}


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------


# ---------------------------------------------------------------------------
# Durable, proportionally-invalidated variant cache (task #20)
# ---------------------------------------------------------------------------
#
# DEFAULT-options maps ride the materialized-view layer (exact fingerprint + a
# CHEAP incremental layout, so new papers appear promptly). CUSTOM variants — a
# non-default cluster_resolution, a fused-layout weight mix, a colour/size encoding
# — are a FULL re-cluster (8k papers on the corpus), so recomputing on every slider
# tick or every paper insert is pure waste. They are cached durably in the
# `materialized_views` table via `mv.get_or_build_variant`, keyed by their option
# hash and invalidated PROPORTIONALLY: a cached variant is served while the
# underlying data has drifted below a threshold and rebuilt once a real proportion
# changed (or any algo version changed). This replaces the prior in-process LRU —
# it now survives restarts and tracks real data change instead of a wall-clock TTL.

# Rebuild a variant once more than this fraction of the shown data has changed
# (added / removed / modified) since it was built. Small enough to stay honest on
# a growing library, large enough that one-off inserts don't churn an expensive
# full re-cluster. The default view does NOT use this (its incremental rebuild is
# cheap and should reflect new papers immediately).
_VARIANT_DRIFT_THRESHOLD = 0.10


@dataclass(frozen=True)
class _VariantDataGauge:
    """Proportional freshness gauge for a durable graph variant.

    DRY across the paper map AND the author network: both render structure derived
    from the SAME papers-in-scope (the author graph's clusters/edges come from
    co-authorship), so one gauge over paper drift serves both. (Follow-state only
    restyles author nodes — a cheap visual overlay — so it deliberately does NOT
    force an expensive full author re-cluster; the default author view still picks
    follow changes up exactly.)

    The gauge encodes the build-time data size + watermark + algo versions into the
    stored fingerprint string. A cached variant is FRESH iff:
      * every algo/model version still matches (a code or model change always
        invalidates — proportional tolerance is for DATA, never for logic), and
      * data drift — ``max(net count change, items modified since build) /
        build-time count`` — is below :data:`_VARIANT_DRIFT_THRESHOLD`.
    """

    versions: tuple[str, ...]
    count_sql: str
    watermark_sql: str
    changed_since_sql: str

    def _scalar(self, conn: sqlite3.Connection, sql: str, params: tuple = ()) -> Any:
        row = conn.execute(sql, params).fetchone()
        return row[0] if row else None

    def signature(self, conn: sqlite3.Connection) -> str:
        """Build-time signature to persist (current size + watermark + versions)."""
        return json.dumps(
            {
                "v": list(self.versions),
                "n": int(self._scalar(conn, self.count_sql) or 0),
                "t": str(self._scalar(conn, self.watermark_sql) or ""),
            },
            sort_keys=True,
        )

    def is_fresh(self, conn: sqlite3.Connection, stored: str) -> bool:
        """True when the cached variant is still within the drift tolerance."""
        try:
            meta = json.loads(stored)
        except (TypeError, ValueError):
            return False
        if tuple(meta.get("v") or []) != self.versions:
            return False  # algo/model version changed → always rebuild
        built_n = int(meta.get("n") or 0)
        current_n = int(self._scalar(conn, self.count_sql) or 0)
        if built_n <= 0:
            return current_n == 0  # built on empty data; fresh only if still empty
        changed = int(self._scalar(conn, self.changed_since_sql, (str(meta.get("t") or ""),)) or 0)
        drift = max(abs(current_n - built_n), changed) / built_n
        return drift < _VARIANT_DRIFT_THRESHOLD


def _paper_scope_gauge(conn: sqlite3.Connection, scope: Scope) -> _VariantDataGauge:
    """Proportional gauge over papers-in-scope — shared by both graph variant caches.

    The version tuple folds in the active embedding model so a model switch
    invalidates variants exactly (not proportionally); data drift is measured on
    the scoped ``papers`` set (count + ``updated_at`` watermark), which captures
    adds, removes, and edits.
    """
    filt = scope.paper_filter("p", leading_and=False)
    where = f" WHERE {filt}" if filt else ""
    try:
        from alma.discovery.similarity import get_active_embedding_model

        model = get_active_embedding_model(conn) or ""
    except Exception:
        model = ""
    return _VariantDataGauge(
        versions=(CLUSTERING_ALGO_VERSION, PROJECTION_ALGO_VERSION, LABELLING_VERSION, model),
        count_sql=f"SELECT COUNT(*) FROM papers p{where}",
        watermark_sql=f"SELECT COALESCE(MAX(p.updated_at), '') FROM papers p{where}",
        changed_since_sql="SELECT COUNT(*) FROM papers p WHERE p.updated_at > ?"
        + (f" AND {filt}" if filt else ""),
    )


def _variant_view_key(base_view_key: str, signature: tuple) -> str:
    """Durable cache key for a graph variant: ``<base>:v=<hash-of-options>``.

    Only the layout/render options go into the hash; the DATA drift is tracked
    separately by the gauge, so corpus growth invalidates a variant without
    changing its key (the row is reused across rebuilds, keeping rows bounded).
    """
    blob = "|".join(str(x) for x in signature).encode("utf-8")
    return f"{base_view_key}:v={hashlib.sha1(blob).hexdigest()[:16]}"


def _serve_graph_variant(
    conn: sqlite3.Connection,
    *,
    base_view_key: str,
    options: tuple,
    scope: Scope,
    build_fn: "Callable[[sqlite3.Connection], dict]",
) -> GraphData:
    """Serve a graph variant from the durable, proportionally-invalidated cache.

    The ONE path shared by the paper-map and author-network variant routes (DRY):
    key the variant by its options, gauge freshness by papers-in-scope drift, and
    return a validated GraphData. ``build_fn(conn) -> payload dict`` does the
    cache-miss build (a pure read of the underlying layout, persisting only the
    variant payload — never the resolution-1.0 publication layout, I-2).
    """
    variant_key = _variant_view_key(base_view_key, options)
    gauge = _paper_scope_gauge(conn, scope)
    payload = mv.get_or_build_variant(
        conn,
        view_key=variant_key,
        build_fn=build_fn,
        make_fingerprint=lambda: gauge.signature(conn),
        is_fresh=lambda stored: gauge.is_fresh(conn, stored),
    )
    return GraphData(**payload)


@router.get("/paper-map", response_model=GraphData)
def get_paper_map(
    label_mode: str = Query("cluster", description="Label mode: cluster or topic"),
    color_by: str = Query("cluster", description="Color by: cluster, year, rating, citations"),
    size_by: str = Query("citations", description="Size by: citations, uniform, rating"),
    show_edges: bool = Query(True, description="Show edges between nodes"),
    show_topics: bool = Query(False, description="Show topic nodes overlaid on paper map"),
    scope: str = Query("library", description="library (default: Library-only papers) or corpus (every stored paper)"),
    cluster_resolution: float = Query(1.0, ge=0.5, le=3.0, description="Cluster detail: >1 finer (more clusters), <1 coarser"),
    w_semantic: float = Query(1.0, ge=0.0, le=1.0, description="PROTOTYPE: semantic-similarity weight in the fused layout"),
    w_coauthorship: float = Query(0.0, ge=0.0, le=1.0, description="PROTOTYPE: co-authorship weight in the fused layout (0 = pure semantic)"),
    w_bibliographic: float = Query(0.0, ge=0.0, le=1.0, description="PROTOTYPE: bibliographic-coupling weight in the fused layout"),
    conn: sqlite3.Connection = Depends(get_db),
):
    """Get paper map visualization data.

    Default options (cluster labels, cluster colour, citation size, edges
    on, no topic overlay, resolution 1.0) are served via the materialised-view
    layer: cache hit returns instantly, fingerprint mismatch enqueues a
    background rebuild and serves the prior payload meanwhile. Custom
    option combinations (incl. a non-default cluster_resolution) bypass the
    cache and build inline — those are rare, ad-hoc views where caching every
    variant would be wasteful.
    """
    scope = Scope.parse(scope)
    # A fused layout (any non-zero non-semantic weight) is a custom, uncached view.
    fused_layout = w_coauthorship > 0 or w_bibliographic > 0
    is_default_options = (
        label_mode == "cluster"
        and color_by == "cluster"
        and size_by == "citations"
        and show_edges
        and not show_topics
        and abs(cluster_resolution - 1.0) < 1e-6
        and not fused_layout
    )

    if is_default_options:
        view_key = scope.view_key("paper_map")
        envelope = mv.get(conn, view_key)
        return _graph_data_from_envelope(envelope)

    # Custom-options path: build live, but cache durably + proportionally (task
    # #20) so a repeat request (the same slider position, a fused layout you already
    # viewed) returns instantly, AND the cache survives restarts + invalidates once a
    # real proportion of the corpus has changed — not on every paper insert.
    def _build_variant(c: sqlite3.Connection) -> dict:
        ai_state = _get_graph_ai_state(c)
        graph_options = {
            "label_mode": label_mode,
            "color_by": color_by,
            "size_by": size_by,
            "show_edges": show_edges,
            "scope": scope,
            "cluster_resolution": cluster_resolution,
            # PROTOTYPE (task 19): fused multi-view layout weights — INDEPENDENT (not
            # renormalized). {coauth: 0, bib: 0} ⇒ pure semantic (and is_default).
            "layout_weights": {
                "semantic": w_semantic,
                "coauthorship": w_coauthorship,
                "bibliographic_coupling": w_bibliographic,
            },
        }
        embeddings = _load_embeddings(c, scope=scope)
        if embeddings and len(embeddings) >= 5:
            # I-2: a variant build is a pure read of publication_clusters — build the
            # layout in-memory and do NOT persist it (the MV rebuild path is the only
            # writer of the resolution-1.0 incremental layout). Only the variant
            # PAYLOAD is persisted, to the materialized_views cache.
            result = _build_embedding_paper_map(
                c, embeddings, ai_state=ai_state, graph_options=graph_options, persist=False
            )
        else:
            result = _build_text_paper_map(c, scope=scope, ai_state=ai_state)
        if show_topics:
            result = _add_topic_overlay(c, result)
        return result.model_dump()

    return _serve_graph_variant(
        conn,
        base_view_key=scope.view_key("paper_map"),
        options=(
            label_mode, color_by, size_by, show_topics,
            round(cluster_resolution, 3), round(w_semantic, 3),
            round(w_coauthorship, 3), round(w_bibliographic, 3),
        ),
        scope=scope,
        build_fn=_build_variant,
    )


@router.get("/author-network", response_model=GraphData)
def get_author_network(
    scope: str = Query("library", description="library (default) or corpus"),
    cluster_resolution: float = Query(
        1.0, ge=0.5, le=3.0, description="Cluster detail: >1 finer (more clusters), <1 coarser"
    ),
    w_semantic: float = Query(1.0, ge=0.0, le=1.0, description="PROTOTYPE: semantic weight in the fused author layout"),
    w_coauthorship: float = Query(0.0, ge=0.0, le=1.0, description="PROTOTYPE: co-authorship weight in the fused author layout"),
    w_bibliographic: float = Query(0.0, ge=0.0, le=1.0, description="PROTOTYPE: bibliographic-coupling weight in the fused author layout"),
    conn: sqlite3.Connection = Depends(get_db),
):
    """Get author network visualization data.

    The default (resolution 1.0, pure-semantic layout) is served from the
    materialised view. A non-default cluster_resolution OR a fused layout (a
    non-zero co-authorship / bib-coupling weight) is a variant — served from the
    SAME durable, proportionally-invalidated cache as the paper map (task #20):
    build once, reuse across restarts, rebuild only when a real proportion of the
    underlying papers has changed. The build itself is a pure read (no
    publication-layout persistence), mirroring the paper map (I-2).
    """
    scope = Scope.parse(scope)
    fused = w_coauthorship > 0 or w_bibliographic > 0
    if abs(cluster_resolution - 1.0) < 1e-6 and not fused:
        view_key = scope.view_key("author_network")
        envelope = mv.get(conn, view_key)
        return _graph_data_from_envelope(envelope)

    def _build_variant(c: sqlite3.Connection) -> dict:
        return _build_author_network_payload(
            c,
            scope=scope,
            cluster_resolution=cluster_resolution,
            layout_weights={
                "semantic": w_semantic,
                "coauthorship": w_coauthorship,
                "bibliographic_coupling": w_bibliographic,
            },
        )

    # Same durable, proportionally-invalidated cache as the paper map — gauged on
    # papers-in-scope drift (the author graph derives from co-authorship).
    return _serve_graph_variant(
        conn,
        base_view_key=scope.view_key("author_network"),
        options=(round(cluster_resolution, 3), round(w_semantic, 3),
                 round(w_coauthorship, 3), round(w_bibliographic, 3)),
        scope=scope,
        build_fn=_build_variant,
    )


def _build_author_network_payload(
    conn: sqlite3.Connection,
    *,
    scope: str,
    cluster_resolution: float = 1.0,
    layout_weights: Optional[dict] = None,
) -> dict:
    """Compute the author-network GraphData (as a dict) for the given scope.

    This is the original `get_author_network` body, lifted out so the
    materialised-view layer can call it on cache miss / rebuild.
    """
    from alma.ai.cluster_labels import compute_cluster_signature, fetch_cached_labels
    from alma.ai.projections import build_coauthor_network

    raw = build_coauthor_network(
        conn,
        scope=scope,
        cluster_resolution=cluster_resolution,
        layout_weights=layout_weights,
    )

    author_cluster_signatures: dict[int, str] = {}
    for cluster in raw.get("clusters", []):
        member_ids = cluster.get("member_ids") or []
        if member_ids:
            author_cluster_signatures[int(cluster["id"])] = compute_cluster_signature(member_ids)

    cached_labels = fetch_cached_labels(
        conn,
        graph_type="author_network",
        scope=scope,
        signatures=set(author_cluster_signatures.values()),
    )
    cluster_label_override: dict[int, str] = {}
    for cid, sig in author_cluster_signatures.items():
        entry = cached_labels.get(sig)
        if entry and entry.get("label"):
            cluster_label_override[cid] = str(entry["label"]).strip()

    nodes = [
        GraphNode(
            id=n["id"],
            name=n["name"],
            x=n["x"],
            y=n["y"],
            cluster_id=n.get("cluster_id"),
            color=CLUSTER_COLORS[n["cluster_id"] % len(CLUSTER_COLORS)]
            if n.get("cluster_id") is not None
            else OUTLIER_COLOR,  # Unclustered authors render neutral, not blue (I-6)
            size=max(1.0, n.get("pub_count", 1) / 6),
            metadata={
                "pub_count": n.get("pub_count", 0),
                "citation_count": n.get("citation_count", 0),
                "h_index": n.get("h_index", 0),
                "works_count": n.get("works_count", 0),
                "author_citedby": n.get("author_citedby", 0),
                "affiliation": n.get("affiliation", ""),
                "orcid": n.get("orcid", ""),
                "openalex_id": n.get("openalex_id", ""),
                "top_topic": n.get("top_topic"),
                "interests": n.get("interests", []),
                "is_outlier": bool(n.get("is_outlier")),
                "cluster_label": cluster_label_override.get(
                    int(n["cluster_id"]) if n.get("cluster_id") is not None else -1,
                    n.get("cluster_label"),
                ),
            },
        )
        for n in raw["nodes"]
    ]

    edges = [
        GraphEdge(
            source=e["source"],
            target=e["target"],
            weight=e["weight"],
            edge_type=e.get("edge_type", "semantic"),
        )
        for e in raw["edges"]
    ]

    enriched_clusters: list[dict[str, object]] = []
    for cluster in raw.get("clusters", []):
        cid = int(cluster["id"])
        sig = author_cluster_signatures.get(cid, "")
        entry = cached_labels.get(sig, {}) if sig else {}
        merged = dict(cluster)
        merged["cluster_signature"] = sig
        if entry.get("label"):
            merged["label"] = entry["label"]
        merged["description"] = entry.get("description", "") if entry else ""
        merged["label_model"] = entry.get("model", "") if entry else ""
        # Strip member_ids from the wire payload — only the refresher
        # worker needs it server-side.
        merged.pop("member_ids", None)
        enriched_clusters.append(merged)

    result = GraphData(
        nodes=nodes,
        edges=edges,
        metadata={
            "type": "author_network",
            "method": raw.get("method", "author_embedding_mean"),
            "clusters": enriched_clusters,
            # Typed edge-layer counts + clustering diagnostics, same shape as the
            # paper map so the UI's filter chips + method panel work here too.
            "edge_layers": raw.get("edge_layers", {}),
            "clustering": raw.get("clustering", {}),
        },
    )
    return result.model_dump()


@router.get("/topic-map", response_model=GraphData)
def get_topic_map(conn: sqlite3.Connection = Depends(get_db)):
    """Get topic co-occurrence map visualization data, served via materialised view."""
    envelope = mv.get(conn, "graph:topic_map")
    return _graph_data_from_envelope(envelope)


def _build_topic_map_payload(conn: sqlite3.Connection) -> dict:
    """Compute the topic-cooccurrence GraphData (as a dict).

    Lifted out of `get_topic_map` so the materialised-view layer can
    invoke it on cache miss / rebuild.
    """
    from alma.ai.projections import build_topic_cooccurrence

    raw = build_topic_cooccurrence(conn)

    max_count = max((n["count"] for n in raw["nodes"]), default=1)
    nodes = [
        GraphNode(
            id=n["id"],
            name=n["name"],
            x=n["x"],
            y=n["y"],
            size=max(0.5, n["count"] / max_count * 3),
            metadata={"count": n["count"]},
        )
        for n in raw["nodes"]
    ]

    edges = [
        GraphEdge(
            source=e["source"],
            target=e["target"],
            weight=e["weight"],
        )
        for e in raw["edges"]
    ]

    return GraphData(
        nodes=nodes, edges=edges, metadata={"type": "topic_map"}
    ).model_dump()


def _rebuild_graphs_impl(
    conn: sqlite3.Connection, *, scope: Scope = Scope.library, job_id: str | None = None
) -> dict:
    """Rebuild the graph caches for ``scope`` phase-by-phase.

    Each phase (clear / reference backfill / paper_map / author_network /
    topic_map) commits before the next one begins, so the SQLite writer
    lock is released between phases. Before this change the whole rebuild
    ran under one implicit transaction and concurrent reads showed p95 of
    ~3.5s during the job (see ``tasks/10_ACTIVITY_CONCURRENCY.md``).

    I-3: paper_map + author_network rebuild the view for the requested
    ``scope`` (not a hardcoded ``:library``), so clicking "Rebuild" while
    viewing Corpus actually refreshes the Corpus graph the user is looking
    at. ``topic_map`` is scope-agnostic and always rebuilt.
    """
    from alma.api.scheduler import add_job_log, is_cancellation_requested, set_job_status
    from alma.openalex.client import backfill_missing_publication_references

    def _flush() -> None:
        if conn.in_transaction:
            conn.commit()

    def _cancelled() -> bool:
        return bool(job_id and is_cancellation_requested(job_id))

    rebuilt: list[str] = []
    phases = ["clear_cache", "reference_backfill", "paper_map", "author_network", "topic_map"]
    total_phases = len(phases)

    def _mark_progress(phase_idx: int, phase_name: str) -> None:
        if not job_id:
            return
        set_job_status(
            job_id,
            status="running",
            processed=phase_idx,
            total=total_phases,
            message=f"Rebuilding graphs: {phase_name}",
        )

    # Phase 1: clear cache. Short write that would otherwise hold the
    # writer lock across the remote reference backfill that follows.
    _mark_progress(0, "clear_cache")
    try:
        conn.execute("DELETE FROM graph_cache")
        # Force a full RE-CLUSTER for this scope: drop its cached
        # publication_clusters layout so the paper_map phase re-runs
        # cluster_publications with the CURRENT algorithm. Without this the
        # incremental path reused the stale cached layout (I-7), so a clustering
        # change (I-5: eom / no-forced-K) never reached the rebuilt map — i.e.
        # "Rebuild" didn't actually re-cluster. I-1: the cache is now scope-keyed,
        # so we clear exactly this scope's rows (no papers join needed).
        try:
            conn.execute(
                "DELETE FROM publication_clusters WHERE scope = ?", (str(scope),)
            )
        except sqlite3.OperationalError:
            pass
        _flush()
        if job_id:
            add_job_log(job_id, "Cleared graph cache + scope layout (forces full recluster)", step="clear_cache")
    except sqlite3.OperationalError:
        if job_id:
            add_job_log(job_id, "Graph cache table missing; skipping clear step", step="clear_cache")

    if _cancelled():
        if job_id:
            add_job_log(job_id, "Cancellation requested before reference backfill", step="cancelled")
        return {"rebuilt": rebuilt, "count": 0, "cancelled": True}

    # Phase 2: reference backfill (remote fetches + its own commit).
    _mark_progress(1, "reference_backfill")
    graph_backfill = {
        "candidates": 0,
        "fetched": 0,
        "papers_updated": 0,
        "references_inserted": 0,
    }
    try:
        graph_backfill = backfill_missing_publication_references(conn, limit=500)
        _flush()
        if job_id:
            add_job_log(job_id, "Backfilled missing publication references", step="reference_backfill", data=graph_backfill)
    except Exception as e:
        _flush()
        logger.warning("Failed to backfill publication references before graph rebuild: %s", e)
        if job_id:
            add_job_log(job_id, f"Reference backfill failed: {e}", level="WARNING", step="reference_backfill")

    if _cancelled():
        if job_id:
            add_job_log(job_id, "Cancellation requested before paper map", step="cancelled")
        return {"rebuilt": rebuilt, "count": 0, "cancelled": True, "reference_backfill": graph_backfill}

    # Phase 3: paper_map — reads embeddings, runs clustering/projection,
    # writes publication_clusters and the materialised-view payload.
    # Flush when done so the next phase starts with no pending writer
    # lock. Forced through `mv.rebuild` (not the GET-side `mv.get`) so
    # the rebuild fires unconditionally even if the fingerprint happens
    # to match the cached row.
    _mark_progress(2, "paper_map")
    try:
        if job_id:
            add_job_log(job_id, f"Rebuilding paper map ({scope.label()})", step="paper_map")
        mv.rebuild(conn, scope.view_key("paper_map"))
        _flush()
        rebuilt.append(scope.view_key("paper_map"))
    except Exception as e:
        _flush()
        logger.warning("Failed to rebuild paper_map: %s", e)
        if job_id:
            add_job_log(job_id, f"Failed rebuilding paper_map: {e}", level="ERROR", step="paper_map")

    if _cancelled():
        if job_id:
            add_job_log(job_id, "Cancellation requested before author network", step="cancelled")
        return {"rebuilt": rebuilt, "count": len(rebuilt), "cancelled": True, "reference_backfill": graph_backfill}

    # Phase 4: author_network
    _mark_progress(3, "author_network")
    try:
        if job_id:
            add_job_log(job_id, f"Rebuilding author network ({scope.label()})", step="author_network")
        mv.rebuild(conn, scope.view_key("author_network"))
        _flush()
        rebuilt.append(scope.view_key("author_network"))
    except Exception as e:
        _flush()
        logger.warning("Failed to rebuild author_network: %s", e)
        if job_id:
            add_job_log(job_id, f"Failed rebuilding author_network: {e}", level="ERROR", step="author_network")

    if _cancelled():
        if job_id:
            add_job_log(job_id, "Cancellation requested before topic map", step="cancelled")
        return {"rebuilt": rebuilt, "count": len(rebuilt), "cancelled": True, "reference_backfill": graph_backfill}

    # Phase 5: topic_map
    _mark_progress(4, "topic_map")
    try:
        if job_id:
            add_job_log(job_id, "Rebuilding topic map", step="topic_map")
        mv.rebuild(conn, "graph:topic_map")
        _flush()
        rebuilt.append("topic_map")
    except Exception as e:
        _flush()
        logger.warning("Failed to rebuild topic_map: %s", e)
        if job_id:
            add_job_log(job_id, f"Failed rebuilding topic_map: {e}", level="ERROR", step="topic_map")

    summary = {"rebuilt": rebuilt, "count": len(rebuilt), "reference_backfill": graph_backfill}
    if job_id:
        add_job_log(job_id, f"Graph rebuild completed: {len(rebuilt)} rebuilt", step="done", data=summary)
    return summary


def _backfill_references_impl(conn: sqlite3.Connection, *, job_id: str | None = None) -> dict:
    """Backfill missing local publication references without rebuilding graph caches."""
    from alma.api.scheduler import add_job_log
    from alma.openalex.client import backfill_missing_publication_references

    if job_id:
        add_job_log(job_id, "Starting publication-reference backfill", step="reference_backfill")
    summary = backfill_missing_publication_references(conn, limit=500)
    conn.commit()
    if job_id:
        add_job_log(job_id, "Publication-reference backfill completed", step="done", data=summary)
    return summary


def _cluster_label_refresh_impl(
    conn: sqlite3.Connection,
    *,
    graph_type: str,
    scope: str,
    job_id: str | None = None,
) -> dict[str, Any]:
    """Regenerate cluster labels for one graph + scope using TF-IDF.

    Reads the current cluster membership from the in-memory computation
    (`get_paper_map` / `get_author_network`) so the labels always reflect
    what the UI is about to render, then runs `label_clusters_tfidf` over
    each cluster's representative titles + abstracts. The LLM-backed path
    was removed in 2026-04 (see `tasks/01_LLM_PRODUCTION_EXIT.md`); the
    endpoint stays so users can still trigger a refresh, but the labels
    are now deterministic top-term strings written with `model='tfidf'`.

    Invalidates the matching `graph_cache` row at the end so the next GET
    renders with the new labels.
    """
    from alma.ai.cluster_labels import compute_cluster_signature, store_label
    from alma.ai.clustering import Cluster, label_clusters_tfidf
    from alma.api.scheduler import add_job_log, is_cancellation_requested, set_job_status

    if graph_type == "paper_map":
        graph = get_paper_map(conn=conn, scope=scope)
        view_key = Scope.parse(scope).view_key("paper_map")
    elif graph_type == "author_network":
        graph = get_author_network(conn=conn, scope=scope)
        view_key = Scope.parse(scope).view_key("author_network")
    else:
        raise ValueError(f"Unsupported graph_type: {graph_type}")

    clusters = graph.metadata.get("clusters", []) if hasattr(graph, "metadata") else []

    # Member lookup. For paper_map nodes are papers (we already have member ids);
    # for author_network nodes are authors and we read paper context per member.
    nodes_by_cluster: dict[int, list[dict[str, Any]]] = defaultdict(list)
    for node in graph.nodes:
        if node.cluster_id is None:
            continue
        payload = {
            "id": node.id,
            "name": node.name,
            "metadata": dict(node.metadata or {}),
        }
        nodes_by_cluster[int(node.cluster_id)].append(payload)

    total_clusters = len(clusters)
    processed = 0
    labeled = 0
    skipped = 0

    # Build a (Cluster, key, titles, abstracts, signature, top_terms) tuple
    # per cluster so we can invoke `label_clusters_tfidf` once with a
    # synthetic key per cluster — the tfidf pass needs the full set in
    # parallel because it computes inverse-document-frequency across
    # clusters.
    refresh_entries: list[dict[str, Any]] = []
    for cluster in clusters:
        cluster_id = int(cluster.get("id", 0))
        members = nodes_by_cluster.get(cluster_id, [])
        member_ids = [m["id"] for m in members if m.get("id")]
        if not member_ids:
            skipped += 1
            continue

        if graph_type == "paper_map":
            titles, abstracts = _collect_paper_cluster_context(
                conn, member_ids, limit=6
            )
        else:
            titles, abstracts = _collect_author_cluster_context(
                conn, member_ids, limit=6, scope=scope
            )

        synthetic_key = f"cluster::{cluster_id}"
        joined_text = " ".join(
            f"{(title or '').strip()} {(abstract or '').strip()}"
            for title, abstract in zip(titles, abstracts)
        ).strip()

        refresh_entries.append(
            {
                "cluster_id": cluster_id,
                "synthetic_key": synthetic_key,
                "joined_text": joined_text,
                "signature": compute_cluster_signature(member_ids),
                "top_terms": list(cluster.get("top_topics") or []),
                "size": int(cluster.get("size") or len(member_ids)),
            }
        )

    # Compose synthetic Cluster objects + a text map keyed by `synthetic_key`
    # so `label_clusters_tfidf` can compute IDF across the full cluster set.
    synthetic_clusters = [
        Cluster(cluster_id=entry["cluster_id"], member_keys=[entry["synthetic_key"]])
        for entry in refresh_entries
    ]
    cluster_texts = {
        entry["synthetic_key"]: entry["joined_text"] or "(empty)"
        for entry in refresh_entries
    }
    tfidf_labels = (
        label_clusters_tfidf(synthetic_clusters, cluster_texts) if synthetic_clusters else []
    )

    for entry, tfidf_label in zip(refresh_entries, tfidf_labels):
        if job_id and is_cancellation_requested(job_id):
            add_job_log(job_id, "Cancellation requested", step="cancelled")
            break

        label = (tfidf_label or "").strip() or f"Cluster {entry['cluster_id'] + 1}"
        store_label(
            conn,
            graph_type=graph_type,
            scope=scope,
            signature=entry["signature"],
            label=label,
            description="",
            top_terms=entry["top_terms"],
            model="tfidf",
        )
        labeled += 1
        if job_id:
            add_job_log(
                job_id,
                f"Labelled cluster {entry['cluster_id'] + 1}: {label}",
                step="cluster_labeled",
                data={"signature": entry["signature"][:10], "size": entry["size"]},
            )

        processed += 1
        if job_id:
            set_job_status(
                job_id,
                status="running",
                processed=processed,
                total=total_clusters,
                message=f"Labelling clusters ({processed}/{total_clusters})",
            )

    # Force a rebuild of the matching materialised view so the next GET
    # renders with the new labels. We rebuild eagerly (rather than just
    # invalidating) because the label-refresh job already runs in the
    # background and the user expects the new labels to be live the next
    # time they look at the graph.
    try:
        mv.rebuild(conn, view_key)
    except Exception:
        logger.exception("cluster-label refresh: failed to rebuild %s", view_key)

    summary = {
        "graph_type": graph_type,
        "scope": scope,
        "total_clusters": total_clusters,
        "labeled": labeled,
        "skipped": skipped,
    }
    if job_id:
        add_job_log(job_id, "Cluster-label refresh complete", step="done", data=summary)
    return summary


def _collect_paper_cluster_context(
    conn: sqlite3.Connection,
    paper_ids: list[str],
    *,
    limit: int = 6,
) -> tuple[list[str], list[str]]:
    """Return representative titles + abstracts for an LLM label prompt."""
    if not paper_ids:
        return [], []
    placeholders = ",".join("?" * len(paper_ids))
    rows = conn.execute(
        f"""
        SELECT title, abstract, cited_by_count
        FROM papers
        WHERE id IN ({placeholders})
        ORDER BY COALESCE(cited_by_count, 0) DESC,
                 COALESCE(publication_date, '') DESC
        LIMIT ?
        """,
        [*paper_ids, limit],
    ).fetchall()
    titles: list[str] = []
    abstracts: list[str] = []
    for row in rows:
        title = row["title"] if isinstance(row, sqlite3.Row) else row[0]
        abstract = row["abstract"] if isinstance(row, sqlite3.Row) else row[1]
        titles.append(str(title or "").strip() or "(untitled)")
        abstracts.append(str(abstract or "").strip())
    return titles, abstracts


def _collect_author_cluster_context(
    conn: sqlite3.Connection,
    author_ids: list[str],
    *,
    limit: int = 6,
    scope: str = "library",
) -> tuple[list[str], list[str]]:
    """Fetch top papers across the cluster's member authors for labelling.

    The cluster's ``member_ids`` are OpenAlex AUTHOR ids — the node
    identity used by :func:`build_coauthor_network`, which keys every node
    by ``publication_authors.openalex_id`` (see projections.py). They are
    NOT local ``authors.id`` UUIDs. (Bug I-12, 2026-06-22: this function
    used to filter ``WHERE a.id IN (<openalex ids>)`` via an
    ``authors`` bridge join — a type mismatch that matched zero rows, so
    every author cluster fell back to an empty-context "Cluster N" label.)

    We therefore filter ``publication_authors.openalex_id`` DIRECTLY (no
    bridge join needed) and dedupe ``papers.id`` so a multi-author paper is
    counted once even when several of its authors belong to the same
    cluster. The ``lower(...)`` filter rides the index on
    ``publication_authors.openalex_id``; do NOT add ``trim()`` — see the
    2026-04-26 lesson on expression-index defeats.
    """
    if not author_ids:
        return [], []
    # member_ids are OpenAlex author ids; lowercase to match lower(openalex_id).
    lowered = [str(a).lower() for a in author_ids if a]
    if not lowered:
        return [], []
    placeholders = ",".join("?" * len(lowered))

    def _fetch(scope_filter: str) -> list:
        return conn.execute(
            f"""
            SELECT p.title, p.abstract,
                   MAX(COALESCE(p.cited_by_count, 0)) AS cby,
                   MAX(COALESCE(p.publication_date, '')) AS pdate
            FROM papers p
            JOIN publication_authors pa ON pa.paper_id = p.id
            WHERE lower(pa.openalex_id) IN ({placeholders}){scope_filter}
            GROUP BY p.id
            ORDER BY cby DESC, pdate DESC
            LIMIT ?
            """,
            [*lowered, limit],
        ).fetchall()

    rows = _fetch(" AND p.status = 'library'") if scope == "library" else _fetch("")
    # Fallback: if a cluster's authors have no library-scope papers
    # (e.g. they're background co-authors only), draw from the wider
    # corpus rather than emitting a placeholder label. The labels are
    # advisory chrome, not curation, so widening here is harmless.
    if not rows and scope == "library":
        rows = _fetch("")
    titles: list[str] = []
    abstracts: list[str] = []
    for row in rows:
        title = row["title"] if isinstance(row, sqlite3.Row) else row[0]
        abstract = row["abstract"] if isinstance(row, sqlite3.Row) else row[1]
        titles.append(str(title or "").strip() or "(untitled)")
        abstracts.append(str(abstract or "").strip())
    return titles, abstracts


class ClusterLabelRefreshRequest(BaseModel):
    graph_type: str = "paper_map"
    scope: str = "library"


@router.post("/cluster-labels/refresh")
def refresh_cluster_labels(
    payload: ClusterLabelRefreshRequest,
    conn: sqlite3.Connection = Depends(get_db),
    _user: dict = Depends(get_current_user),
):
    """Regenerate representative cluster labels for one graph + scope.

    Runs in the Activity envelope so the UI can track per-cluster
    progress without blocking the GET path. The GET route picks up
    cached labels from `graph_cluster_labels` on the next refresh —
    this endpoint invalidates `graph_cache` at the end to force that
    rebuild.
    """
    from alma.api.scheduler import (
        activity_envelope,
        add_job_log,
        find_active_job,
        schedule_immediate,
        set_job_status,
    )

    graph_type = payload.graph_type if payload.graph_type in {"paper_map", "author_network"} else "paper_map"
    scope = Scope.parse(payload.scope)
    operation_key = f"graphs.cluster_labels:{graph_type}:{scope}"
    existing = find_active_job(operation_key)
    if existing:
        return activity_envelope(
            str(existing.get("job_id") or ""),
            status="already_running",
            operation_key=operation_key,
            message="Cluster-label refresh already running",
        )

    job_id = f"graph_labels_{uuid.uuid4().hex[:10]}"
    set_job_status(
        job_id,
        status="queued",
        operation_key=operation_key,
        trigger_source="user",
        started_at=datetime.utcnow().isoformat(),
        message=f"Refreshing cluster labels ({graph_type}, {scope})",
    )
    add_job_log(
        job_id,
        f"Queued cluster-label refresh for {graph_type}/{scope}",
        step="queued",
    )

    def _runner() -> dict:
        bg_conn = open_db_connection()
        try:
            return _cluster_label_refresh_impl(
                bg_conn,
                graph_type=graph_type,
                scope=scope,
                job_id=job_id,
            )
        finally:
            bg_conn.close()

    schedule_immediate(job_id, _runner)
    return activity_envelope(
        job_id,
        status="queued",
        operation_key=operation_key,
        message="Cluster-label refresh queued",
    )


@router.post("/rebuild")
def rebuild_graphs(
    scope: str = Query("library", description="library (default) or corpus — the scope to rebuild"),
    background: bool = Query(True, description="Run rebuild in background and track in Activity"),
    conn: sqlite3.Connection = Depends(get_db),
    _user: dict = Depends(get_current_user),
):
    """Rebuild the graph caches for the requested scope (I-3: scope-aware)."""
    from alma.api.scheduler import activity_envelope, find_active_job, schedule_immediate, set_job_status

    scope_obj = Scope.parse(scope)
    if not background:
        return _rebuild_graphs_impl(conn, scope=scope_obj)

    # Scope-specific operation key so a Library rebuild and a Corpus rebuild are
    # distinct in-flight jobs (one must not dedupe against the other).
    operation_key = f"graphs.rebuild_all:{scope_obj}"
    existing = find_active_job(operation_key)
    if existing:
        return activity_envelope(
            str(existing.get("job_id") or ""),
            status="already_running",
            operation_key=operation_key,
            message="Graph rebuild already running",
        )

    job_id = f"graph_rebuild_{uuid.uuid4().hex[:10]}"
    set_job_status(
        job_id,
        status="queued",
        operation_key=operation_key,
        trigger_source="user",
        started_at=datetime.utcnow().isoformat(),
        message="Rebuilding graph cache",
    )

    def _runner() -> dict:
        bg_conn = open_db_connection()
        try:
            return _rebuild_graphs_impl(bg_conn, scope=scope_obj, job_id=job_id)
        finally:
            bg_conn.close()

    schedule_immediate(job_id, _runner)
    return activity_envelope(
        job_id,
        status="queued",
        operation_key=operation_key,
        message="Graph rebuild queued",
    )


@router.post("/reference-backfill")
def backfill_graph_references(
    background: bool = Query(True, description="Run backfill in background and track in Activity"),
    conn: sqlite3.Connection = Depends(get_db),
    _user: dict = Depends(get_current_user),
):
    """Backfill missing publication references from OpenAlex without rebuilding caches."""
    from alma.api.scheduler import activity_envelope, find_active_job, schedule_immediate, set_job_status

    if not background:
        return _backfill_references_impl(conn)

    operation_key = "graphs.reference_backfill"
    existing = find_active_job(operation_key)
    if existing:
        return activity_envelope(
            str(existing.get("job_id") or ""),
            status="already_running",
            operation_key=operation_key,
            message="Reference backfill already running",
        )

    job_id = f"graph_ref_backfill_{uuid.uuid4().hex[:10]}"
    set_job_status(
        job_id,
        status="queued",
        operation_key=operation_key,
        trigger_source="user",
        started_at=datetime.utcnow().isoformat(),
        message="Backfilling publication references",
    )

    def _runner() -> dict:
        bg_conn = open_db_connection()
        try:
            return _backfill_references_impl(bg_conn, job_id=job_id)
        finally:
            bg_conn.close()

    schedule_immediate(job_id, _runner)
    return activity_envelope(
        job_id,
        status="queued",
        operation_key=operation_key,
        message="Reference backfill queued",
    )


# ---------------------------------------------------------------------------
# Helper functions
# ---------------------------------------------------------------------------

CLUSTER_COLORS = [
    "#3B82F6",
    "#8B5CF6",
    "#10B981",
    "#F59E0B",
    "#06B6D4",
    "#EC4899",
    "#6366F1",
    "#F97316",
    "#EF4444",
    "#84CC16",
    "#14B8A6",
    "#A855F7",
]

# Outlier group (I-6): papers HDBSCAN judged to be density noise are retained as
# a distinct "Unclustered" group rather than force-merged into a real cluster.
# A negative id keeps them out of the dense 0..N-1 cluster colour ramp, and the
# neutral slate fill signals "no confident topic" instead of a misleading colour.
OUTLIER_CLUSTER_ID = -1
OUTLIER_LABEL = "Unclustered"
OUTLIER_COLOR = "#94A3B8"  # slate-400: the same neutral used for "no cluster"

# Incremental layout: a brand-new paper is only attached to an existing cluster
# centroid when it is genuinely close (cosine ≥ this). A novel paper that sits
# far from every centroid stays Unclustered until the next full rebuild, instead
# of being jittered into whichever centroid happens to be least-far (I-6/I-7).
_INCREMENTAL_MIN_COSINE = 0.10


def _get_graph_ai_state(conn: sqlite3.Connection) -> dict:
    provider = "none"
    try:
        row = conn.execute(
            "SELECT value FROM discovery_settings WHERE key = 'embedding_provider'"
        ).fetchone()
        if row:
            provider = (row["value"] if isinstance(row, sqlite3.Row) else row[0]) or "none"
    except sqlite3.OperationalError:
        provider = "none"

    ai_active = provider.lower() not in ("", "none")

    emb_count = 0
    try:
        if table_exists(conn, "publication_embeddings"):
            from alma.discovery.similarity import get_active_embedding_model

            active_model = get_active_embedding_model(conn)
            r = conn.execute(
                "SELECT COUNT(*) AS c FROM publication_embeddings WHERE model = ?",
                (active_model,),
            ).fetchone()
            emb_count = int(r["c"] if isinstance(r, sqlite3.Row) else r[0])
    except Exception:
        emb_count = 0

    pub_count = 0
    try:
        r = conn.execute("SELECT COUNT(*) AS c FROM papers").fetchone()
        pub_count = int(r["c"] if isinstance(r, sqlite3.Row) else r[0])
    except Exception:
        pub_count = 0

    coverage = round((emb_count / pub_count * 100.0), 1) if pub_count > 0 else 0.0
    return {
        "ai_active": ai_active,
        "embedding_provider": provider,
        "embeddings_count": emb_count,
        "embedding_coverage_pct": coverage,
    }


def _build_text_paper_map(
    conn: sqlite3.Connection,
    *,
    scope: str,
    ai_state: Optional[dict] = None,
) -> GraphData:
    """Paper-map response when SPECTER2 embeddings are unavailable.

    Principled text-only fallback per the locked product rule
    (2026-05-07): when no embeddings exist, cluster on the *paper's
    own text* (title + abstract) via TF-IDF — never on
    ``publication_topics`` (OpenAlex's coarse topic vocabulary), the
    venue, or author names. Uses the same silhouette-driven k sweep
    and the same c-TF-IDF labeller as the embedding path so the
    fallback feels continuous with the embedded experience.

    When fewer than 5 papers carry meaningful text, degrade to an
    unclustered grid layout — no fake clusters.

    Args:
        scope: ``"library"`` (default) or ``"corpus"``.
        ai_state: optional payload of AI-state metadata to merge into
            the graph's ``metadata`` block (provider, embedding count,
            coverage pct) so the frontend can show the right empty-
            state CTA.
    """
    from alma.ai.clustering import _silhouette_optimal_k, label_clusters_tfidf, Cluster
    from alma.ai.projections import project_embeddings as _project_embeddings
    from sklearn.cluster import MiniBatchKMeans
    if scope == "library":
        rows = conn.execute(
            """
            SELECT id, title, abstract, year, journal, cited_by_count, rating,
                   publication_date, authors
            FROM papers
            WHERE status = 'library'
            ORDER BY COALESCE(cited_by_count, 0) DESC,
                     COALESCE(publication_date, '') DESC
            """
        ).fetchall()
    else:
        rows = conn.execute(
            """
            SELECT id, title, abstract, year, journal, cited_by_count, rating,
                   publication_date, authors
            FROM papers
            ORDER BY COALESCE(cited_by_count, 0) DESC,
                     COALESCE(publication_date, '') DESC
            """
        ).fetchall()

    paper_ids: list[str] = []
    docs: list[str] = []
    paper_meta: dict[str, dict] = {}
    for row in rows:
        paper_id = row["id"] if isinstance(row, sqlite3.Row) else row[0]
        title = (row["title"] if isinstance(row, sqlite3.Row) else row[1]) or ""
        abstract = (row["abstract"] if isinstance(row, sqlite3.Row) else row[2]) or ""
        year = row["year"] if isinstance(row, sqlite3.Row) else row[3]
        journal = (row["journal"] if isinstance(row, sqlite3.Row) else row[4]) or ""
        cited_by = (row["cited_by_count"] if isinstance(row, sqlite3.Row) else row[5]) or 0
        rating = (row["rating"] if isinstance(row, sqlite3.Row) else row[6]) or 0
        publication_date = (row["publication_date"] if isinstance(row, sqlite3.Row) else row[7]) or None
        authors = (row["authors"] if isinstance(row, sqlite3.Row) else row[8]) or ""

        paper_ids.append(paper_id)
        # Title + abstract only. Journal and authors are NOT topical
        # signal — including them gives clusters dominated by venue or
        # author cliques. publication_topics is not consulted here per
        # the locked product rule.
        docs.append(f"{title}. {abstract}".strip())
        paper_meta[paper_id] = {
            "title": title,
            "year": year,
            "publication_date": publication_date,
            "journal": journal,
            "authors": authors,
            "cited_by_count": int(cited_by or 0),
            "rating": int(rating or 0),
        }

    n_papers = len(paper_ids)
    method_tag = "text_tfidf"
    cluster_assignments: dict[str, int] = {}
    coords: dict[str, tuple[float, float]] = {}
    similarity_matrix: Optional[np.ndarray] = None
    cluster_labels_by_cid: dict[int, str] = {}
    cluster_sizes: dict[int, int] = {}

    has_text = any(doc.strip() for doc in docs)

    if n_papers >= 5 and has_text:
        try:
            vectorizer = TfidfVectorizer(
                max_features=4000,
                stop_words="english",
                ngram_range=(1, 2),
                min_df=2 if n_papers >= 10 else 1,
                max_df=0.9,
                token_pattern=r"(?u)\b[a-zA-Z][a-zA-Z]+\b",
                lowercase=True,
            )
            tfidf = vectorizer.fit_transform(docs)
            matrix = tfidf.toarray().astype(np.float32)
            if matrix.shape[1] == 0:
                raise ValueError("TF-IDF vocabulary is empty after stop-word filtering")

            n_clusters = _silhouette_optimal_k(matrix, min_k=2, max_k=30)
            n_clusters = min(n_clusters, max(2, n_papers - 1))
            km = MiniBatchKMeans(
                n_clusters=n_clusters,
                random_state=42,
                n_init=5,
                batch_size=min(256, max(32, n_papers * 2)),
            )
            km_labels = km.fit_predict(matrix)

            members_by_cid: dict[int, list[str]] = defaultdict(list)
            for idx, cid in enumerate(km_labels):
                members_by_cid[int(cid)].append(paper_ids[idx])

            # Renumber dense + size-descending so cluster 0 is the largest.
            sorted_old_cids = sorted(
                members_by_cid.keys(),
                key=lambda c: len(members_by_cid[c]),
                reverse=True,
            )
            cid_map = {old: new for new, old in enumerate(sorted_old_cids)}
            cluster_assignments = {
                pid: cid_map[int(km_labels[idx])]
                for idx, pid in enumerate(paper_ids)
            }
            cluster_sizes = {
                cid_map[old]: len(members_by_cid[old]) for old in sorted_old_cids
            }

            synthetic_clusters = [
                Cluster(
                    cluster_id=cid_map[old],
                    member_keys=members_by_cid[old],
                )
                for old in sorted_old_cids
            ]
            label_strings = label_clusters_tfidf(
                synthetic_clusters,
                {pid: docs[i] for i, pid in enumerate(paper_ids)},
            )
            for c, lbl in zip(synthetic_clusters, label_strings):
                cluster_labels_by_cid[int(c.cluster_id)] = lbl

            # 2D layout: pretend each TF-IDF row is an embedding for projection.
            # ``project_embeddings`` falls back gracefully when UMAP isn't
            # installed (TSNE) or when n is very small (centred at origin).
            try:
                tfidf_embeddings = {
                    paper_ids[i]: matrix[i].tolist() for i in range(n_papers)
                }
                coords = _project_embeddings(tfidf_embeddings, method="auto")
            except Exception:
                coords = {}

            # Cosine similarity for kNN edges; reuse for the edge step below.
            norms = np.linalg.norm(matrix, axis=1, keepdims=True)
            norms[norms == 0] = 1.0
            normed = matrix / norms
            similarity_matrix = np.clip(normed @ normed.T, 0.0, 1.0)
        except Exception as exc:
            logger.warning(
                "Text TF-IDF clustering failed; falling back to unclustered grid: %s",
                exc,
            )
            cluster_assignments = {}
            coords = {}
            similarity_matrix = None
            method_tag = "no_clustering"

    if not cluster_assignments:
        method_tag = "no_clustering"
        cluster_labels_by_cid = {}
        cluster_sizes = {}
        # Deterministic grid layout when we can't cluster.
        side = max(1, int(math.ceil(math.sqrt(max(1, n_papers)))))
        for idx, pid in enumerate(paper_ids):
            gx = (idx % side) / max(1, side - 1) if side > 1 else 0.5
            gy = (idx // side) / max(1, side - 1) if side > 1 else 0.5
            coords[pid] = (
                float(0.05 + 0.9 * gx),
                float(0.05 + 0.9 * gy),
            )

    nodes: list[GraphNode] = []
    for pid in paper_ids:
        meta = paper_meta[pid]
        x, y = coords.get(pid, (0.5, 0.5))
        x = float(min(0.98, max(0.02, x)))
        y = float(min(0.98, max(0.02, y)))
        cid = cluster_assignments.get(pid)
        nodes.append(
            GraphNode(
                id=pid,
                name=str(meta["title"] or "(untitled)"),
                x=x,
                y=y,
                cluster_id=cid,
                color=(
                    CLUSTER_COLORS[cid % len(CLUSTER_COLORS)]
                    if cid is not None
                    else None
                ),
                size=max(1.0, math.log1p(meta["cited_by_count"])),
                metadata={
                    "title": meta["title"],
                    "year": meta["year"],
                    "publication_date": meta["publication_date"],
                    "journal": meta["journal"],
                    "authors": meta["authors"],
                    "cited_by_count": meta["cited_by_count"],
                    "rating": meta["rating"],
                    "paper_id": pid,
                    "cluster_label": cluster_labels_by_cid.get(cid)
                    if cid is not None
                    else None,
                },
            )
        )

    edges: list[GraphEdge] = []
    if similarity_matrix is not None and n_papers >= 2:
        # Top-k nearest neighbour graph by cosine similarity. k scales
        # with corpus size so a small library doesn't get an opaque hairball.
        top_k = 4 if n_papers >= 25 else 3
        seen: set[tuple[int, int]] = set()
        for i in range(n_papers):
            row = similarity_matrix[i].copy()
            row[i] = 0.0
            top_idx = np.argpartition(-row, min(top_k, n_papers - 1))[:top_k]
            for j in top_idx:
                if j == i:
                    continue
                a, b = (int(i), int(j)) if i < j else (int(j), int(i))
                if (a, b) in seen:
                    continue
                seen.add((a, b))
                weight = float(row[j])
                if weight <= 0.05:
                    continue
                edges.append(
                    GraphEdge(
                        source=paper_ids[a],
                        target=paper_ids[b],
                        weight=round(weight, 3),
                    )
                )

    clusters_payload = [
        {
            "id": cid,
            "label": cluster_labels_by_cid.get(cid, f"Cluster {cid + 1}"),
            "size": size,
        }
        for cid, size in sorted(cluster_sizes.items())
    ]

    metadata = {
        "type": "paper_map",
        "method": method_tag,
        "clusters": clusters_payload,
        "scope": scope,
        **(ai_state or {}),
    }
    if method_tag == "no_clustering":
        metadata["message"] = (
            "Not enough text to cluster. Save more papers or compute "
            "SPECTER2 embeddings in Settings → AI."
        )
    elif method_tag == "text_tfidf":
        metadata["note"] = (
            "Clustered on title + abstract (TF-IDF). Compute SPECTER2 "
            "embeddings in Settings → AI for sharper semantic clusters."
        )
    return GraphData(nodes=nodes, edges=edges, metadata=metadata)


def _load_publication_topic_signals(
    conn: sqlite3.Connection,
) -> dict[str, list[tuple[str, float]]]:
    if not table_exists(conn, "publication_topics"):
        return {}

    has_topics = table_exists(conn, "topics")
    try:
        if has_topics:
            rows = conn.execute(
                """
                SELECT pt.paper_id,
                       COALESCE(t.canonical_name, pt.term) AS term,
                       MAX(COALESCE(pt.score, 1.0)) AS score
                FROM publication_topics pt
                LEFT JOIN topics t ON pt.topic_id = t.topic_id
                GROUP BY pt.paper_id, COALESCE(t.canonical_name, pt.term)
                """
            ).fetchall()
        else:
            rows = conn.execute(
                """
                SELECT paper_id, term, MAX(COALESCE(score, 1.0)) AS score
                FROM publication_topics
                GROUP BY paper_id, term
                """
            ).fetchall()
    except Exception:
        return {}

    by_key: dict[str, list[tuple[str, float]]] = defaultdict(list)
    for row in rows:
        paper_id = row["paper_id"] if isinstance(row, sqlite3.Row) else row[0]
        term = row["term"] if isinstance(row, sqlite3.Row) else row[1]
        score = float(row["score"] if isinstance(row, sqlite3.Row) else row[2])
        if not term:
            continue
        by_key[paper_id].append((term, score))

    for key, vals in by_key.items():
        vals.sort(key=lambda x: x[1], reverse=True)
        dedup: dict[str, float] = {}
        for term, score in vals:
            if term not in dedup or score > dedup[term]:
                dedup[term] = score
        by_key[key] = sorted(dedup.items(), key=lambda x: x[1], reverse=True)
    return dict(by_key)


def _build_cluster_detail(
    cluster_id: int,
    members: list[str],
    *,
    paper_meta: dict[str, dict[str, Any]],
    coords: dict[str, tuple[float, float]],
    label: str | None = None,
    cached_labels: dict[str, dict[str, object]] | None = None,
) -> dict[str, Any]:
    xs = [coords[paper_id][0] for paper_id in members if paper_id in coords]
    ys = [coords[paper_id][1] for paper_id in members if paper_id in coords]
    topic_counts: Counter[str] = Counter()
    citations: list[int] = []
    ratings: list[int] = []
    years: list[int] = []
    publication_dates: list[str] = []
    sample_rows: list[dict[str, Any]] = []

    for paper_id in members:
        meta = paper_meta.get(paper_id, {})
        for key in ("topics", "openalex_topics", "keywords"):
            for term in meta.get(key, []) or []:
                normalized = str(term or "").strip()
                if normalized:
                    topic_counts[normalized] += 1

        citations.append(int(meta.get("cited_by_count") or 0))
        rating_value = int(meta.get("rating") or 0)
        if rating_value > 0:
            ratings.append(rating_value)

        try:
            year_value = meta.get("year")
            if year_value is not None:
                years.append(int(year_value))
        except Exception:
            pass

        publication_date = str(meta.get("publication_date") or "").strip()
        if publication_date:
            publication_dates.append(publication_date)

        sample_rows.append(
            {
                "paper_id": paper_id,
                "title": str(meta.get("title") or "").strip() or paper_id,
                "year": meta.get("year"),
                "publication_date": publication_date or None,
                "cited_by_count": int(meta.get("cited_by_count") or 0),
                "journal": str(meta.get("journal") or "").strip() or None,
            }
        )

    sample_rows.sort(
        key=lambda item: (
            int(item.get("cited_by_count") or 0),
            str(item.get("publication_date") or ""),
            int(item.get("year") or 0),
            str(item.get("title") or ""),
        ),
        reverse=True,
    )
    top_topics = [term for term, _ in topic_counts.most_common(6)]
    from alma.ai.cluster_labels import compute_cluster_signature

    cluster_signature = compute_cluster_signature(members)
    cached_entry = (cached_labels or {}).get(cluster_signature)
    cached_label = str(cached_entry.get("label") or "").strip() if cached_entry else ""
    cached_description = str(cached_entry.get("description") or "").strip() if cached_entry else ""
    cached_model = str(cached_entry.get("model") or "").strip() if cached_entry else ""

    if cached_label:
        resolved_label = cached_label
    else:
        resolved_label = label or (top_topics[0] if top_topics else f"Cluster {cluster_id + 1}")
    topic_text = " · ".join(top_topics[:2]) if top_topics else resolved_label
    return {
        "id": int(cluster_id),
        "label": resolved_label,
        "topic_text": topic_text,
        "description": cached_description,
        "label_model": cached_model,
        "cluster_signature": cluster_signature,
        "size": len(members),
        "x": round(float(np.mean(xs)), 4) if xs else 0.5,
        "y": round(float(np.mean(ys)), 4) if ys else 0.5,
        "top_topics": top_topics,
        "sample_papers": sample_rows[:4],
        "avg_citations": round(float(np.mean(citations)), 1) if citations else 0.0,
        "avg_rating": round(float(np.mean(ratings)), 2) if ratings else 0.0,
        "year_range": {
            "min": min(years) if years else None,
            "max": max(years) if years else None,
        },
        "publication_date_range": {
            "min": min(publication_dates) if publication_dates else None,
            "max": max(publication_dates) if publication_dates else None,
        },
    }


def _build_cluster_info(
    cluster_members: dict[int, list[str]],
    *,
    paper_meta: dict[str, dict[str, Any]],
    coords: dict[str, tuple[float, float]],
    labels_by_cluster: dict[int, str] | None = None,
    cached_labels: dict[str, dict[str, object]] | None = None,
    cluster_texts: dict[str, str] | None = None,
) -> list[dict[str, Any]]:
    labels = labels_by_cluster or {}
    word_clouds: dict[int, list[dict[str, Any]]] = {}
    if cluster_texts:
        word_clouds = _build_word_clouds_for_clusters(cluster_members, cluster_texts)
    details = []
    for cid, members in sorted(cluster_members.items(), key=lambda kv: kv[0]):
        detail = _build_cluster_detail(
            int(cid),
            list(members),
            paper_meta=paper_meta,
            coords=coords,
            label=labels.get(int(cid), f"Cluster {int(cid) + 1}"),
            cached_labels=cached_labels,
        )
        detail["word_cloud"] = word_clouds.get(int(cid), [])
        details.append(detail)
    return details


def _load_paper_map_cached_labels(
    conn: sqlite3.Connection,
    cluster_members: dict[int, list[str]],
    *,
    scope: str,
) -> dict[str, dict[str, object]]:
    """Fetch cached cluster labels (TF-IDF) for every cluster in the paper map."""
    from alma.ai.cluster_labels import compute_cluster_signature, fetch_cached_labels

    signatures = {
        compute_cluster_signature(members)
        for members in cluster_members.values()
        if members
    }
    if not signatures:
        return {}
    return fetch_cached_labels(
        conn,
        graph_type="paper_map",
        scope=scope,
        signatures=signatures,
    )


def _build_word_clouds_for_clusters(
    cluster_members: dict[int, list[str]],
    cluster_texts: dict[str, str],
    *,
    top_n: int = 10,
) -> dict[int, list[dict[str, Any]]]:
    """Per-cluster word clouds via the shared prevalence-weighted c-TF-IDF.

    Uses the SAME ``score_cluster_terms`` scorer as the cluster labels (DRY), so
    a word cloud surfaces terms that are both distinctive to the cluster AND
    shared across its papers — not vocabulary that appears a lot in one verbose
    paper but in no others (the non-co-occurring-words artefact).
    """
    from alma.ai.clustering import score_cluster_terms

    sorted_cids = sorted(cluster_members.keys())
    if not sorted_cids:
        return {}

    member_texts = {
        cid: [cluster_texts.get(paper_id, "") for paper_id in cluster_members[cid]]
        for cid in sorted_cids
    }
    scored = score_cluster_terms(member_texts, ngram_range=(1, 2), top_k=top_n)
    return {
        cid: [{"term": term, "weight": round(weight, 4)} for term, weight in ranked[:top_n]]
        for cid, ranked in scored.items()
    }


def _load_embeddings(
    conn: sqlite3.Connection,
    *,
    scope: str = "library",
) -> dict[str, list[float]]:
    """Load embeddings produced by the active model.

    Vectors produced by a previously-configured model are filtered out
    at the SQL layer so every returned vector shares the same
    dimensionality.

    When scope == "library" (default), only embeddings for papers the
    user has saved to the Library are returned. scope == "corpus" returns
    every embedding regardless of paper status.
    """
    from alma.discovery.similarity import get_active_embedding_model

    active_model = get_active_embedding_model(conn)
    try:
        if scope == "library":
            rows = conn.execute(
                """
                SELECT pe.paper_id, pe.embedding
                FROM publication_embeddings pe
                JOIN papers p ON p.id = pe.paper_id
                WHERE pe.model = ? AND p.status = 'library'
                """,
                (active_model,),
            ).fetchall()
        else:
            rows = conn.execute(
                "SELECT paper_id, embedding FROM publication_embeddings WHERE model = ?",
                (active_model,),
            ).fetchall()
    except sqlite3.OperationalError:
        return {}

    # Always decode through the canonical helper — `publication_embeddings`
    # stores float16 since commit 918e5fc, so the old struct-unpack path
    # interpreted bytes as float32 and returned half-dim garbage vectors.
    # `decode_vector` upcasts to runtime float32 and (when given an
    # `expected_dim`) auto-rescues legacy float32 rows by byte length.
    from alma.core.vector_blob import decode_vector

    embeddings: dict[str, list[float]] = {}
    for row in rows:
        if isinstance(row, sqlite3.Row):
            paper_id = row["paper_id"]
            blob = row["embedding"]
        else:
            paper_id = row[0]
            blob = row[1]
        if not blob:
            continue
        try:
            vec = decode_vector(blob)
        except Exception:
            continue
        embeddings[paper_id] = vec.tolist()
    return embeddings


def _build_embedding_paper_map(
    conn: sqlite3.Connection,
    embeddings: dict[str, list[float]],
    *,
    ai_state: Optional[dict] = None,
    graph_options: Optional[dict] = None,
    persist: bool = True,
) -> GraphData:
    """Build paper map using embeddings, with incremental clustering/layout reuse.

    I-2 (GET purity): ``persist`` controls whether the computed layout is written
    back to ``publication_clusters``. The materialized-view REBUILD path (a
    background job) persists (default True) so subsequent rebuilds can reuse the
    incremental layout; the synchronous custom-options GET passes ``persist=False``
    so a read request never writes + commits mid-response.
    """
    from alma.ai.clustering import cluster_publications, label_clusters_tfidf
    from alma.ai.neighbor_graph import mutual_knn_edges
    from alma.ai.projections import project_embeddings

    opts = graph_options or {}
    label_mode = opts.get("label_mode", "cluster")
    color_by = opts.get("color_by", "cluster")
    size_by = opts.get("size_by", "citations")
    show_edges = opts.get("show_edges", True)
    # I-1: the cluster-layout cache is keyed by scope, so a Corpus build never
    # overwrites the Library layout (and vice-versa). Read + persist only this
    # scope's rows.
    layout_scope = str(opts.get("scope", "library") or "library")

    def _cosine(a: np.ndarray, b: np.ndarray) -> float:
        na = float(np.linalg.norm(a))
        nb = float(np.linalg.norm(b))
        if na <= 0 or nb <= 0:
            return 0.0
        return float(np.dot(a, b) / (na * nb))

    def _cluster_jitter(paper_id: str, cluster_id: int, index: int) -> tuple[float, float]:
        digest = hashlib.sha1(f"{paper_id}:{cluster_id}:{index}".encode("utf-8")).hexdigest()
        angle = (int(digest[:8], 16) / float(16**8)) * (2.0 * np.pi)
        radius = 0.035 + 0.01 * (index % 3)
        return float(np.cos(angle) * radius), float(np.sin(angle) * radius)

    paper_ids = list(embeddings.keys())
    vectors_by_id = {
        paper_id: np.asarray(vec, dtype=np.float32)
        for paper_id, vec in embeddings.items()
    }

    # Load topic signals for all papers
    topic_signals = _load_publication_topic_signals(conn)

    # Fetch per-paper text payloads (for labels and node metadata).
    texts: dict[str, str] = {}
    paper_meta: dict[str, dict] = {}
    for paper_id in embeddings:
        row = conn.execute(
            """
            SELECT title, abstract, cited_by_count, year, rating, journal, authors, publication_date
            FROM papers
            WHERE id = ?
            """,
            (paper_id,),
        ).fetchone()
        if row:
            title = row["title"] if isinstance(row, sqlite3.Row) else row[0]
            abstract = row["abstract"] if isinstance(row, sqlite3.Row) else row[1]
            cited_by_count = row["cited_by_count"] if isinstance(row, sqlite3.Row) else row[2]
            year = row["year"] if isinstance(row, sqlite3.Row) else row[3]
            rating = row["rating"] if isinstance(row, sqlite3.Row) else row[4]
            journal = row["journal"] if isinstance(row, sqlite3.Row) else row[5]
            authors = row["authors"] if isinstance(row, sqlite3.Row) else row[6]
            publication_date = row["publication_date"] if isinstance(row, sqlite3.Row) else row[7]
            top_topics = [term for term, _ in topic_signals.get(paper_id, [])[:3]]
            texts[paper_id] = f"{title or ''}. {abstract or ''}"
            paper_meta[paper_id] = {
                "title": title or "",
                "cited_by_count": int(cited_by_count or 0),
                "year": year,
                "rating": int(rating or 0),
                "journal": journal or "",
                "authors": authors or "",
                "publication_date": publication_date,
                "topics": top_topics,
            }
        else:
            texts[paper_id] = ""
            paper_meta[paper_id] = {
                "title": "",
                "cited_by_count": 0,
                "year": None,
                "rating": 0,
                "journal": "",
                "authors": "",
                "publication_date": None,
                "topics": [],
            }

    # Read embedding freshness and previously materialized layout rows.
    embedding_created_at: dict[str, str] = {}
    layout_rows: dict[str, dict] = {}
    try:
        from alma.discovery.similarity import get_active_embedding_model

        active_model = get_active_embedding_model(conn)
        rows = conn.execute(
            "SELECT paper_id, created_at FROM publication_embeddings WHERE model = ?",
            (active_model,),
        ).fetchall()
        for row in rows:
            pid = row["paper_id"] if isinstance(row, sqlite3.Row) else row[0]
            created_at = row["created_at"] if isinstance(row, sqlite3.Row) else row[1]
            if pid in vectors_by_id:
                embedding_created_at[pid] = str(created_at or "")
    except sqlite3.OperationalError:
        embedding_created_at = {}
    try:
        rows = conn.execute(
            "SELECT paper_id, cluster_id, label, x, y, updated_at "
            "FROM publication_clusters WHERE scope = ?",
            (layout_scope,),
        ).fetchall()
        for row in rows:
            pid = row["paper_id"] if isinstance(row, sqlite3.Row) else row[0]
            if pid not in vectors_by_id:
                continue
            layout_rows[pid] = {
                "cluster_id": int((row["cluster_id"] if isinstance(row, sqlite3.Row) else row[1]) or 0),
                "label": (row["label"] if isinstance(row, sqlite3.Row) else row[2]) or "",
                "x": float((row["x"] if isinstance(row, sqlite3.Row) else row[3]) or 0.5),
                "y": float((row["y"] if isinstance(row, sqlite3.Row) else row[4]) or 0.5),
                "updated_at": str((row["updated_at"] if isinstance(row, sqlite3.Row) else row[5]) or ""),
            }
    except sqlite3.OperationalError:
        layout_rows = {}

    stale_ids: list[str] = []
    stable_ids: list[str] = []
    for paper_id in paper_ids:
        cached = layout_rows.get(paper_id)
        if not cached:
            stale_ids.append(paper_id)
            continue
        emb_ts = embedding_created_at.get(paper_id, "")
        layout_ts = cached.get("updated_at", "")
        if emb_ts and (not layout_ts or layout_ts < emb_ts):
            stale_ids.append(paper_id)
        else:
            stable_ids.append(paper_id)

    assignments: dict[str, int] = {}
    coords: dict[str, tuple[float, float]] = {}
    labels_by_cluster: dict[int, str] = {}
    cluster_members: dict[int, list[str]] = defaultdict(list)
    layout_mode = "embeddings_full"
    # Clustering diagnostics (I-4/I-6) — populated by the full-rebuild path; the
    # cached/incremental paths derive their counts from the loaded layout below.
    clustering_meta: dict[str, Any] = {}
    node_probabilities: dict[str, float] = {}

    # A non-default cluster_resolution must actually RE-CLUSTER: the cached
    # publication_clusters layout was built at resolution 1.0, so reusing it
    # would silently ignore the requested detail level. Force a full recompute.
    if abs(float(opts.get("cluster_resolution", 1.0) or 1.0) - 1.0) > 1e-6:
        stale_ids = list(paper_ids)
        stable_ids = []

    # 1) Fully fresh cache: render directly from persisted layout.
    if not stale_ids and len(stable_ids) == len(paper_ids):
        layout_mode = "embeddings_cached"
        for paper_id in paper_ids:
            cached = layout_rows[paper_id]
            cid = int(cached["cluster_id"])
            assignments[paper_id] = cid
            coords[paper_id] = (float(cached["x"]), float(cached["y"]))
            cluster_members[cid].append(paper_id)
            if cached.get("label"):
                labels_by_cluster[cid] = str(cached["label"])

    # 2) Partial refresh: update only new/stale papers by nearest cached centroids.
    elif stable_ids and stale_ids and len(stale_ids) <= max(3, int(round(len(paper_ids) * 0.25))):
        layout_mode = "embeddings_incremental"
        for paper_id in stable_ids:
            cached = layout_rows[paper_id]
            cid = int(cached["cluster_id"])
            assignments[paper_id] = cid
            coords[paper_id] = (float(cached["x"]), float(cached["y"]))
            cluster_members[cid].append(paper_id)
            if cached.get("label"):
                labels_by_cluster[cid] = str(cached["label"])

        centroid_vectors: dict[int, np.ndarray] = {}
        centroid_coords: dict[int, tuple[float, float]] = {}
        for cid, members in cluster_members.items():
            member_vectors = [vectors_by_id[pid] for pid in members if pid in vectors_by_id]
            if not member_vectors:
                continue
            centroid_vectors[cid] = np.mean(np.stack(member_vectors), axis=0)
            xs = [coords[pid][0] for pid in members]
            ys = [coords[pid][1] for pid in members]
            centroid_coords[cid] = (float(np.mean(xs)), float(np.mean(ys)))

        # If centroid bootstrap fails, fall back to a full recompute.
        if not centroid_vectors:
            stale_ids = paper_ids
            stable_ids = []
            cluster_members = defaultdict(list)
            assignments = {}
            coords = {}
            labels_by_cluster = {}
            layout_mode = "embeddings_full"
        else:
            for paper_id in stale_ids:
                vec = vectors_by_id[paper_id]
                best_cid = max(
                    centroid_vectors.keys(),
                    key=lambda cid: _cosine(vec, centroid_vectors[cid]),
                )
                best_sim = _cosine(vec, centroid_vectors[best_cid])
                # I-6/I-7: only attach a new paper to a cluster it is genuinely
                # close to. A novel paper far from every centroid stays
                # Unclustered (honest) instead of being jittered into the
                # least-far centroid as if it belonged.
                if best_sim < _INCREMENTAL_MIN_COSINE:
                    assignments[paper_id] = OUTLIER_CLUSTER_ID
                    cluster_members[OUTLIER_CLUSTER_ID].append(paper_id)
                else:
                    assignments[paper_id] = int(best_cid)
                    cluster_members[int(best_cid)].append(paper_id)

            # Place incremental nodes around cluster centroids with deterministic jitter.
            stale_idx_by_cluster: dict[int, int] = defaultdict(int)
            for paper_id in stale_ids:
                cid = assignments[paper_id]
                cx, cy = centroid_coords.get(cid, (0.5, 0.5))
                idx = stale_idx_by_cluster[cid]
                stale_idx_by_cluster[cid] += 1
                jx, jy = _cluster_jitter(paper_id, cid, idx)
                coords[paper_id] = (
                    min(0.98, max(0.02, cx + jx)),
                    min(0.98, max(0.02, cy + jy)),
                )

    # 3) Full rebuild: clustering + 2D projection.
    if layout_mode == "embeddings_full":
        # task #21 perf: the 5-D clustering substrate and the 2-D display
        # projection are two UMAP fits over the SAME cosine neighbourhood. At
        # corpus scale the k-NN search dominates and is identical for both, so we
        # build it ONCE here and hand it to both via `precomputed_knn`, halving the
        # neighbour search. The shared graph is the same neighbour graph either fit
        # would have built (the 2-D layout differs only by a random orientation,
        # immaterial for a cached viz); the win is pure wall-clock. Small libraries
        # skip it (their
        # search is already cheap and n_neighbors can differ from the shared width).
        # `embeddings` iteration order is stable and shared by both fits, so the
        # graph's row indices align.
        from alma.ai import accel

        shared_knn = None
        if len(embeddings) >= accel.SHARED_KNN_MIN_N:
            knn_vectors = np.array(list(embeddings.values()), dtype=np.float32)
            shared_knn = accel.cosine_knn(knn_vectors, n_neighbors=accel.SHARED_KNN_NEIGHBORS)

        # Stability re-fits UMAP several times, so only pay for it on the
        # persisting REBUILD path — never on a synchronous custom-options GET.
        # `cluster_resolution` (default 1.0) is the user-facing detail knob.
        clustering = cluster_publications(
            embeddings,
            compute_stability=persist,
            resolution=float(opts.get("cluster_resolution", 1.0) or 1.0),
            precomputed_knn=shared_knn,
        )
        clusters = clustering.clusters
        node_probabilities = clustering.probabilities
        labels = label_clusters_tfidf(clusters, texts)
        for cluster, label in zip(clusters, labels):
            cluster.label = label
            labels_by_cluster[int(cluster.cluster_id)] = str(label or "")
        coords = project_embeddings(embeddings, precomputed_knn=shared_knn)
        for cluster in clusters:
            cid = int(cluster.cluster_id)
            cluster_members[cid] = list(cluster.member_keys)
            for paper_id in cluster.member_keys:
                assignments[paper_id] = cid
        # I-6: density-noise papers are NOT forced into a cluster — collect them
        # as the explicit Unclustered group so each renders honestly.
        if clustering.outliers:
            cluster_members[OUTLIER_CLUSTER_ID] = list(clustering.outliers)
            labels_by_cluster[OUTLIER_CLUSTER_ID] = OUTLIER_LABEL
            for paper_id in clustering.outliers:
                assignments[paper_id] = OUTLIER_CLUSTER_ID
        clustering_meta = {
            "method": clustering.method,
            "n_clusters": clustering.n_clusters,
            "outlier_count": len(clustering.outliers),
            "coverage": round(clustering.coverage, 4),
            "stability": clustering.stability,
            "params": clustering.params,
        }

    # Ensure every cluster has a label after incremental assignment as well.
    if cluster_members and (layout_mode != "embeddings_full" or not labels_by_cluster):
        class _Cluster:
            def __init__(self, cluster_id: int, member_keys: list[str]):
                self.cluster_id = cluster_id
                self.member_keys = member_keys

        synthetic_clusters = [
            _Cluster(cluster_id=cid, member_keys=members)
            for cid, members in sorted(cluster_members.items(), key=lambda kv: kv[0])
            if cid >= 0  # never TF-IDF-label the Unclustered group — it has no topic
        ]
        generated_labels = label_clusters_tfidf(synthetic_clusters, texts)
        for cluster, label in zip(synthetic_clusters, generated_labels):
            labels_by_cluster[int(cluster.cluster_id)] = str(label or labels_by_cluster.get(int(cluster.cluster_id), ""))
        # The outlier group always carries the fixed Unclustered label.
        if OUTLIER_CLUSTER_ID in cluster_members:
            labels_by_cluster[OUTLIER_CLUSTER_ID] = OUTLIER_LABEL

    # Persist computed layout rows so subsequent refreshes can be incremental.
    # I-2: ONLY on the rebuild path (persist=True). A GET request never reaches
    # this block (the custom-options GET passes persist=False), so reads stay pure.
    # Commit in bounded batches to release the SQLite writer lock between chunks
    # (1000+ upserts under one txn held the lock for seconds — see
    # ``tasks/10_ACTIVITY_CONCURRENCY.md``). NOTE: the residual raw commit here is
    # the background-rebuild write that task 09 (_MIGRATION_BACKLOG) gates via
    # write_section — left to 09 per the I-2↔09 cross-reference; do not double-fix.
    if persist:
        now_iso = datetime.now().isoformat()
        _CLUSTER_BATCH_SIZE = 200
        try:
            for batch_start in range(0, len(paper_ids), _CLUSTER_BATCH_SIZE):
                batch = paper_ids[batch_start:batch_start + _CLUSTER_BATCH_SIZE]
                for paper_id in batch:
                    # Default to the Unclustered group (not cluster 0) for any
                    # paper without an assignment — honest "we don't know" (I-6).
                    cid = int(assignments.get(paper_id, OUTLIER_CLUSTER_ID))
                    x, y = coords.get(paper_id, (0.5, 0.5))
                    label = labels_by_cluster.get(cid) or (
                        OUTLIER_LABEL if cid < 0 else f"Cluster {cid + 1}"
                    )
                    conn.execute(
                        """
                        INSERT INTO publication_clusters (paper_id, scope, cluster_id, label, x, y, updated_at)
                        VALUES (?, ?, ?, ?, ?, ?, ?)
                        ON CONFLICT(paper_id, scope) DO UPDATE SET
                            cluster_id = excluded.cluster_id,
                            label = excluded.label,
                            x = excluded.x,
                            y = excluded.y,
                            updated_at = excluded.updated_at
                        """,
                        (paper_id, layout_scope, cid, label, float(x), float(y), now_iso),
                    )
                if conn.in_transaction:
                    conn.commit()
        except sqlite3.OperationalError:
            if conn.in_transaction:
                conn.rollback()

    # PROTOTYPE (task 19): fused multi-view layout. Clusters stay SEMANTIC
    # (computed/cached above — stable), but POSITIONS are re-blended from
    # semantic + co-authorship + bibliographic-coupling per the requested
    # weights. Applied AFTER persist so the cached layout stays pure-semantic.
    # Dense O(N²) ⇒ library-scale only; we hard-cap to avoid the corpus blowing
    # up (the sparse fuzzy-graph path is the corpus answer, still task 19).
    _FUSED_MAX_PAPERS = 1500
    layout_weights = opts.get("layout_weights")
    if (
        layout_weights
        and len(paper_ids) <= _FUSED_MAX_PAPERS
        and (
            float(layout_weights.get("coauthorship", 0) or 0) > 0
            or float(layout_weights.get("bibliographic_coupling", 0) or 0) > 0
        )
    ):
        try:
            from alma.ai.projections import fuse_layout

            fused_coords = fuse_layout(
                embeddings,
                _paper_coauthorship(conn, paper_ids),
                _paper_bibliographic_coupling(conn, paper_ids),
                weights=layout_weights,
                # Anchor at the semantic layout we just computed so adjacent
                # weight steps nudge the map instead of reshuffling it.
                init_coords=coords,
            )
            if fused_coords:
                coords = fused_coords
                layout_mode = "fused"
        except Exception as exc:  # never let a prototype break the map
            logger.warning("fused layout failed; keeping semantic layout: %s", exc)

    # Compute year range for color scaling
    all_years = [int(paper_meta[pid].get("year") or 0) for pid in embeddings if paper_meta[pid].get("year")]
    min_year = min(all_years) if all_years else 2000
    max_year = max(all_years) if all_years else 2026
    year_range = max(1, max_year - min_year)

    # Max citations for scaling
    max_citations = max((paper_meta[pid].get("cited_by_count", 0) for pid in embeddings), default=1) or 1

    # Build nodes.
    nodes: list[GraphNode] = []
    for paper_id in embeddings:
        meta = paper_meta.get(paper_id, {"title": "", "cited_by_count": 0, "year": None, "rating": 0, "topics": []})
        cid = assignments.get(paper_id)
        x, y = coords.get(paper_id, (0.5, 0.5))

        # Determine node color
        if color_by == "year" and meta.get("year"):
            yr = int(meta["year"])
            t = (yr - min_year) / year_range
            # Blue (old) → Green (new)
            r = int(59 * (1 - t) + 16 * t)
            g = int(130 * (1 - t) + 185 * t)
            b = int(246 * (1 - t) + 129 * t)
            node_color = f"#{r:02x}{g:02x}{b:02x}"
        elif color_by == "rating" and meta.get("rating"):
            rating_colors = {0: "#94A3B8", 1: "#EF4444", 2: "#F97316", 3: "#F59E0B", 4: "#10B981", 5: "#3B82F6"}
            node_color = rating_colors.get(int(meta["rating"]), "#94A3B8")
        elif color_by == "citations":
            cite_ratio = min(1.0, int(meta.get("cited_by_count", 0)) / max_citations)
            r = int(148 * (1 - cite_ratio) + 59 * cite_ratio)
            g = int(163 * (1 - cite_ratio) + 130 * cite_ratio)
            b = int(184 * (1 - cite_ratio) + 246 * cite_ratio)
            node_color = f"#{r:02x}{g:02x}{b:02x}"
        elif cid is None or int(cid) < 0:
            # Unclustered / no-cluster papers: neutral slate, never a topic colour.
            node_color = OUTLIER_COLOR
        else:
            node_color = CLUSTER_COLORS[cid % len(CLUSTER_COLORS)]

        # Determine node size
        if size_by == "uniform":
            node_size = 1.0
        elif size_by == "rating":
            node_size = max(0.5, min(3.0, (int(meta.get("rating") or 0)) / 2 + 0.3))
        else:  # citations
            node_size = max(0.5, min(3.0, int(meta.get("cited_by_count") or 0) / 50 + 0.5))

        # Determine display label
        is_outlier = cid is None or int(cid) < 0
        if label_mode == "topic" and meta.get("topics"):
            display_label = ", ".join(meta["topics"][:2])
        elif is_outlier:
            display_label = OUTLIER_LABEL
        else:
            display_label = labels_by_cluster.get(int(cid))

        # HDBSCAN membership strength [0,1] — the per-node clustering confidence
        # that the old force-merge discarded (I-6). None when unavailable (cached
        # layout / k-means fallback).
        confidence = node_probabilities.get(paper_id) if node_probabilities else None

        nodes.append(
            GraphNode(
                id=paper_id,
                name=(meta.get("title") or "") or paper_id,
                x=x,
                y=y,
                cluster_id=cid,
                color=node_color,
                size=node_size,
                metadata={
                    "paper_id": paper_id,
                    "cited_by_count": int(meta.get("cited_by_count") or 0),
                    "year": meta.get("year"),
                    "publication_date": meta.get("publication_date"),
                    "rating": meta.get("rating", 0),
                    "journal": meta.get("journal"),
                    "authors": meta.get("authors"),
                    "cluster_label": display_label,
                    "is_outlier": is_outlier,
                    "cluster_confidence": (
                        round(float(confidence), 3) if confidence is not None else None
                    ),
                    "topics": meta.get("topics", []),
                },
            )
        )

    # Typed, filterable edge LAYERS (Phase 3 / I-11). The old intra-cluster
    # cliques asserted a relationship that was really just "same HDBSCAN
    # cluster"; we replace them with edges that each MEAN something specific and
    # carry their own `edge_type` so the UI can show/hide each layer:
    #   • semantic               — mutual k-NN in the 768-d SPECTER2 space
    #   • bibliographic_coupling — papers that share ≥3 references
    #   • co_authorship          — papers that share ≥1 author
    # Citation/h-index stats NEVER enter edge geometry — they are node metadata.
    edges: list[GraphEdge] = []
    edge_layers: dict[str, int] = {}
    if show_edges:
        # Retracted papers must not anchor neighbourhoods — they stay as nodes
        # but get no edges, so a retracted work is never drawn as central.
        retracted = _retracted_paper_ids(conn, paper_ids)
        semantic_embeddings = {
            pid: vec for pid, vec in embeddings.items() if pid not in retracted
        }
        seen_pairs: set[tuple[str, str, str]] = set()

        def _add_edge(a: str, b: str, weight: float, edge_type: str) -> None:
            # One edge per unordered pair PER TYPE: a pair may be connected in
            # more than one layer (e.g. both semantic and shared-author), and the
            # UI filters per type, so we keep them distinct rather than merging.
            key = (a, b, edge_type) if a < b else (b, a, edge_type)
            if a == b or key in seen_pairs:
                return
            seen_pairs.add(key)
            edges.append(
                GraphEdge(
                    source=key[0], target=key[1],
                    weight=round(float(weight), 3), edge_type=edge_type,
                )
            )
            edge_layers[edge_type] = edge_layers.get(edge_type, 0) + 1

        # 1) Semantic neighbourhood — mutual k-NN in embedding space.
        for a, b, sim in mutual_knn_edges(semantic_embeddings, k=8, min_similarity=0.45):
            _add_edge(a, b, sim, "semantic")

        # 2) Bibliographic coupling — shared references (same literature base).
        coupling = _paper_bibliographic_coupling(conn, paper_ids, min_shared_refs=3)
        max_shared = max(coupling.values(), default=0)
        for (a, b), shared in coupling.items():
            if a in retracted or b in retracted:
                continue
            weight = 0.4 + 0.5 * (shared / max_shared if max_shared else 0.0)
            _add_edge(a, b, weight, "bibliographic_coupling")

        # 3) Co-authorship — papers that share at least one author.
        coauthor = _paper_coauthorship(conn, paper_ids, min_shared_authors=1)
        for (a, b), shared in coauthor.items():
            if a in retracted or b in retracted:
                continue
            weight = min(1.0, 0.4 + 0.2 * shared)
            _add_edge(a, b, weight, "co_authorship")

    # Topic clusters only — the Unclustered group is reported as a count in the
    # clustering metadata, never as a pseudo-topic with a TF-IDF label (I-6).
    topic_cluster_members = {
        cid: members for cid, members in cluster_members.items() if cid >= 0
    }
    cached_labels = _load_paper_map_cached_labels(
        conn,
        topic_cluster_members,
        scope=opts.get("scope", "library"),
    )
    cluster_info = _build_cluster_info(
        topic_cluster_members,
        paper_meta=paper_meta,
        coords=coords,
        labels_by_cluster=labels_by_cluster,
        cached_labels=cached_labels,
        cluster_texts=texts,
    )

    # Unified clustering diagnostics (I-4/I-6) for the method/uncertainty panel.
    # The full-rebuild path supplies fresh figures (method, stability, params);
    # the cached/incremental paths derive counts from the loaded layout so the
    # panel is honest in every mode.
    outlier_count = clustering_meta.get(
        "outlier_count", len(cluster_members.get(OUTLIER_CLUSTER_ID, []))
    )
    total_points = len(paper_ids)
    coverage = clustering_meta.get("coverage")
    if coverage is None and total_points:
        coverage = round((total_points - outlier_count) / total_points, 4)
    clustering_panel = {
        "method": clustering_meta.get("method", layout_mode),
        "n_clusters": clustering_meta.get("n_clusters", len(topic_cluster_members)),
        "outlier_count": outlier_count,
        "coverage": coverage,
        "stability": clustering_meta.get("stability"),
        "params": clustering_meta.get("params", {}),
    }

    result = GraphData(
        nodes=nodes,
        edges=edges,
        metadata={
            "type": "paper_map",
            "method": layout_mode,
            "stale_papers": len(stale_ids),
            "stable_papers": len(stable_ids),
            "clusters": cluster_info,
            "clustering": clustering_panel,
            # Per-layer edge counts so the UI can build filter toggles (I-11).
            "edge_layers": edge_layers,
            **(ai_state or {}),
        },
    )
    return result



def _get_cached_graph(
    conn: sqlite3.Connection, graph_type: str
) -> Optional[GraphData]:
    """Get cached graph data if not expired (1 hour TTL)."""
    try:
        row = conn.execute(
            "SELECT data, updated_at FROM graph_cache WHERE graph_type = ?",
            (graph_type,),
        ).fetchone()
    except sqlite3.OperationalError:
        return None

    if not row:
        return None

    if isinstance(row, sqlite3.Row):
        data_str = row["data"]
        updated = row["updated_at"]
    else:
        data_str = row[0]
        updated = row[1]

    # Check TTL (1 hour)
    try:
        updated_dt = datetime.fromisoformat(updated)
        if (datetime.now() - updated_dt).total_seconds() > 3600:
            return None
    except (ValueError, TypeError):
        return None

    try:
        raw = json.loads(data_str)
        return GraphData(**raw)
    except Exception:
        return None


def _cache_graph(
    conn: sqlite3.Connection, graph_type: str, data: GraphData
) -> None:
    """Cache graph data."""
    try:
        conn.execute(
            """INSERT OR REPLACE INTO graph_cache (graph_type, data, updated_at)
               VALUES (?, ?, ?)""",
            (graph_type, data.model_dump_json(), datetime.now().isoformat()),
        )
    except sqlite3.OperationalError:
        pass  # Table might not exist yet


def _retracted_paper_ids(conn: sqlite3.Connection, paper_ids: list[str]) -> set[str]:
    """The subset of ``paper_ids`` flagged ``is_retracted`` (Phase 3 / I-11).

    Retracted works must not anchor research-neighbourhood edges, so the caller
    keeps them as nodes but draws no edges to/from them. Returns {} when the
    column/table is unavailable."""
    if not paper_ids:
        return set()
    placeholders = ",".join(["?"] * len(paper_ids))
    try:
        rows = conn.execute(
            f"SELECT id FROM papers WHERE is_retracted = 1 AND id IN ({placeholders})",
            list(paper_ids),
        ).fetchall()
    except sqlite3.OperationalError:
        return set()
    return {
        str(row["id"] if isinstance(row, sqlite3.Row) else row[0]) for row in rows
    }


def _paper_coauthorship(
    conn: sqlite3.Connection,
    paper_ids: list[str],
    *,
    min_shared_authors: int = 1,
) -> dict[tuple[str, str], int]:
    """Pairs of papers that share at least ``min_shared_authors`` authors.

    Co-authorship edge layer for the paper map (Phase 3 / I-11): a self-join of
    ``publication_authors`` on the (case-folded) OpenAlex author id, counting
    distinct shared authors per paper pair. Uses the ``idx_pubauthors_oid_lower``
    expression index. Silently returns {} when the table is missing.
    """
    if not paper_ids or not table_exists(conn, "publication_authors"):
        return {}
    placeholders = ",".join(["?"] * len(paper_ids))
    try:
        rows = conn.execute(
            f"""
            SELECT pa1.paper_id AS a, pa2.paper_id AS b, COUNT(*) AS shared
            FROM publication_authors pa1
            JOIN publication_authors pa2
              ON lower(pa1.openalex_id) = lower(pa2.openalex_id)
             AND pa1.paper_id < pa2.paper_id
            WHERE pa1.paper_id IN ({placeholders})
              AND pa2.paper_id IN ({placeholders})
              AND TRIM(COALESCE(pa1.openalex_id, '')) <> ''
            GROUP BY pa1.paper_id, pa2.paper_id
            HAVING shared >= ?
            """,
            list(paper_ids) + list(paper_ids) + [min_shared_authors],
        ).fetchall()
    except sqlite3.OperationalError:
        return {}
    pairs: dict[tuple[str, str], int] = {}
    for r in rows:
        a = r["a"] if isinstance(r, sqlite3.Row) else r[0]
        b = r["b"] if isinstance(r, sqlite3.Row) else r[1]
        pairs[(a, b)] = int(r["shared"] if isinstance(r, sqlite3.Row) else r[2])
    return pairs


def _paper_bibliographic_coupling(
    conn: sqlite3.Connection,
    paper_ids: list[str],
    *,
    min_shared_refs: int = 3,
    max_ref_df: int = 50,
) -> dict[tuple[str, str], int]:
    """Return pairs of papers that share at least `min_shared_refs` references.

    Bibliographic coupling signal for the paper map. Silently returns {}
    when the publication_references table is missing.

    PERF + QUALITY (the corpus self-join was 372s = 93% of the build): a
    reference cited by ``df`` papers in the set produces ``df²`` join rows, so
    field-defining works cited by thousands of papers each generate MILLIONS of
    pairs — and those couplings are non-discriminative noise (everyone cites the
    famous review). We drop references with ``df > max_ref_df`` BEFORE the
    self-join via a document-frequency CTE — the bibliographic analogue of IDF.
    This caps the blow-up (372s → well under a second on the corpus) and makes
    coupling mean "shared SPECIALISED references". Tiny sets (the library) are
    unaffected since few references there clear the cap.
    """
    if not paper_ids:
        return {}
    try:
        row = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='publication_references'"
        ).fetchone()
    except sqlite3.OperationalError:
        return {}
    if not row:
        return {}

    # Pull (paper, ref) once via the indexed paper_id scan, then build the
    # coupling in Python with an inverted index (ref → papers). A SQL self-join
    # can't be made reliably fast here: on the base table a hub reference cited
    # by thousands of papers still explodes (df² rows), and pushing it through a
    # CTE to cap the df loses the referenced_work_id index and gets WORSE. In
    # Python we cap each reference's fan-out explicitly, so the work is bounded by
    # `max_ref_df²` per reference regardless of the planner.
    placeholders = ",".join(["?"] * len(paper_ids))
    try:
        rows = conn.execute(
            f"""
            SELECT paper_id, referenced_work_id
            FROM publication_references
            WHERE paper_id IN ({placeholders})
              AND TRIM(COALESCE(referenced_work_id, '')) <> ''
            """,
            list(paper_ids),
        ).fetchall()
    except sqlite3.OperationalError:
        return {}

    ref_papers: dict[str, set[str]] = defaultdict(set)
    for r in rows:
        pid = r["paper_id"] if isinstance(r, sqlite3.Row) else r[0]
        ref = r["referenced_work_id"] if isinstance(r, sqlite3.Row) else r[1]
        ref_papers[str(ref)].add(str(pid))

    pairs: dict[tuple[str, str], int] = defaultdict(int)
    for members in ref_papers.values():
        n = len(members)
        # Skip singletons (no pair) and over-common references (non-discriminative
        # AND the O(n²) blow-up) — the bibliographic analogue of IDF.
        if n < 2 or n > max_ref_df:
            continue
        ordered = sorted(members)
        for i in range(n):
            a = ordered[i]
            for j in range(i + 1, n):
                pairs[(a, ordered[j])] += 1
    return {pair: shared for pair, shared in pairs.items() if shared >= min_shared_refs}


def _add_topic_overlay(
    conn: sqlite3.Connection,
    graph_data: GraphData,
    min_papers_per_topic: int = 3,
) -> GraphData:
    """Add topic nodes to an existing paper map graph.

    For each topic that appears in at least min_papers_per_topic papers:
    1. Create a topic node positioned at the centroid of connected papers
    2. Create edges from papers to their topics
    """
    if not table_exists(conn, "publication_topics"):
        return graph_data

    # Get paper IDs from the graph
    paper_ids = [n.id for n in graph_data.nodes if n.node_type == "paper"]
    if not paper_ids:
        return graph_data

    # Load topics for these papers (using canonical names if available)
    has_topics = table_exists(conn, "topics")
    placeholders = ",".join("?" for _ in paper_ids)

    if has_topics:
        rows = conn.execute(
            f"""
            SELECT pt.paper_id,
                   COALESCE(t.canonical_name, pt.term) AS topic_name,
                   MAX(COALESCE(pt.score, 1.0)) AS score
            FROM publication_topics pt
            LEFT JOIN topics t ON pt.topic_id = t.topic_id
            WHERE pt.paper_id IN ({placeholders})
            GROUP BY pt.paper_id, COALESCE(t.canonical_name, pt.term)
            """,
            paper_ids,
        ).fetchall()
    else:
        rows = conn.execute(
            f"""
            SELECT paper_id, term AS topic_name, MAX(COALESCE(score, 1.0)) AS score
            FROM publication_topics
            WHERE paper_id IN ({placeholders})
            GROUP BY paper_id, term
            """,
            paper_ids,
        ).fetchall()

    # Build topic -> [paper_ids] mapping
    topic_papers: dict[str, list[str]] = defaultdict(list)
    for row in rows:
        paper_id = row["paper_id"] if isinstance(row, sqlite3.Row) else row[0]
        topic_name = row["topic_name"] if isinstance(row, sqlite3.Row) else row[1]
        if topic_name:
            topic_papers[topic_name].append(paper_id)

    # Filter topics by minimum paper count
    eligible_topics = {
        topic: papers
        for topic, papers in topic_papers.items()
        if len(papers) >= min_papers_per_topic
    }

    if not eligible_topics:
        return graph_data

    # Build position lookup for existing paper nodes
    paper_positions = {n.id: (n.x, n.y) for n in graph_data.nodes if n.node_type == "paper"}

    # Create topic nodes and edges
    topic_nodes: list[GraphNode] = []
    topic_edges: list[GraphEdge] = []

    for topic_name, connected_papers in eligible_topics.items():
        # Calculate centroid position
        positions = [paper_positions[pid] for pid in connected_papers if pid in paper_positions]
        if not positions:
            continue

        centroid_x = sum(x for x, y in positions) / len(positions)
        centroid_y = sum(y for x, y in positions) / len(positions)

        # Create topic node
        topic_id = f"topic:{topic_name}"
        topic_nodes.append(
            GraphNode(
                id=topic_id,
                name=topic_name,
                x=centroid_x,
                y=centroid_y,
                node_type="topic",
                color="#F59E0B",  # Orange color for topics
                size=max(1.0, min(3.0, len(connected_papers) / 5)),
                metadata={
                    "count": len(connected_papers),
                    "type": "topic",
                },
            )
        )

        # Create edges from papers to this topic
        for paper_id in connected_papers:
            if paper_id in paper_positions:
                topic_edges.append(
                    GraphEdge(
                        source=paper_id,
                        target=topic_id,
                        weight=0.3,
                        edge_type="topic",
                    )
                )

    # Append to existing graph
    return GraphData(
        nodes=graph_data.nodes + topic_nodes,
        edges=graph_data.edges + topic_edges,
        metadata={
            **graph_data.metadata,
            "topic_nodes_count": len(topic_nodes),
            "topics_shown": True,
        },
    )


# ---------------------------------------------------------------------------
# Materialised-view registrations
# ---------------------------------------------------------------------------
#
# Each public graph endpoint registers a view here so a cache hit returns
# in <10 ms on the GET path. The fingerprint captures every input that
# should change the rendered graph: corpus / library paper count, last
# Library mutation, embedding count + active model (paper_map),
# followed-author count + last follow time (author_network), and topic
# coverage (topic_map). On fingerprint mismatch the prior payload is
# served immediately and a background rebuild job runs under
# `materialize.graph.<view>` — `useOperationToasts` invalidates the
# matching React Query roots when it completes.


def _graph_data_from_envelope(envelope: dict) -> GraphData:
    """Reconstruct a GraphData from a materialised-view envelope.

    The cached payload is a JSON-decoded dict with `nodes`, `edges`,
    `metadata`. We re-validate it through Pydantic so the response stays
    typed (the route still declares ``response_model=GraphData``), and
    the SWR flags ride along inside ``metadata`` so existing frontend
    code that only reads ``nodes`` / ``edges`` keeps working.
    """
    payload = envelope.get("payload") or {}
    metadata = dict(payload.get("metadata") or {})
    metadata["stale"] = bool(envelope.get("stale", False))
    metadata["rebuilding"] = bool(envelope.get("rebuilding", False))
    if envelope.get("computed_at"):
        metadata["computed_at"] = envelope["computed_at"]
    return GraphData(
        nodes=payload.get("nodes") or [],
        edges=payload.get("edges") or [],
        metadata=metadata,
    )


def _build_paper_map_payload(conn: sqlite3.Connection, *, scope: str) -> dict:
    """Build the default-options paper-map payload (as a dict).

    Mirrors the path inside ``get_paper_map`` for default options:
    SPECTER2-embedding-based clustering when ≥ 5 vectors are
    available; otherwise the principled text-TF-IDF fallback in
    ``_build_text_paper_map`` (clusters on title + abstract only —
    never on ``publication_topics``, journal, or author names).
    Topic overlay is intentionally excluded — it's a non-default
    option and is rendered live, not cached.
    """
    ai_state = _get_graph_ai_state(conn)
    graph_options = {
        "label_mode": "cluster",
        "color_by": "cluster",
        "size_by": "citations",
        "show_edges": True,
        "scope": scope,
    }
    embeddings = _load_embeddings(conn, scope=scope)
    if embeddings and len(embeddings) >= 5:
        result = _build_embedding_paper_map(
            conn, embeddings, ai_state=ai_state, graph_options=graph_options
        )
    else:
        result = _build_text_paper_map(
            conn, scope=scope, ai_state=ai_state
        )
    return result.model_dump()


# Paper map (per scope). Fingerprint covers Library/corpus paper count
# and last update, embedding count for the active model, and the active
# model itself — any of these change → cached layout is stale.
_PAPER_MAP_LIBRARY_FP_SQL = """
    SELECT
      (SELECT COUNT(*) FROM papers WHERE status = 'library'),
      (SELECT COALESCE(MAX(updated_at), '') FROM papers WHERE status = 'library'),
      (SELECT COUNT(*) FROM publication_embeddings pe
         JOIN papers p ON p.id = pe.paper_id
         WHERE p.status = 'library'),
      (SELECT COALESCE(value, '') FROM discovery_settings WHERE key = 'embedding_model')
"""

_PAPER_MAP_CORPUS_FP_SQL = """
    SELECT
      (SELECT COUNT(*) FROM papers),
      (SELECT COALESCE(MAX(updated_at), '') FROM papers),
      (SELECT COUNT(*) FROM publication_embeddings),
      (SELECT COALESCE(value, '') FROM discovery_settings WHERE key = 'embedding_model')
"""

# Author network. Fingerprint covers paper edges (which authors
# co-author together is derived from the publication graph) and follow
# state (followed authors get a different visual treatment).
_AUTHOR_NETWORK_LIBRARY_FP_SQL = """
    SELECT
      (SELECT COUNT(*) FROM papers WHERE status = 'library'),
      (SELECT COALESCE(MAX(updated_at), '') FROM papers WHERE status = 'library'),
      (SELECT COUNT(*) FROM followed_authors),
      (SELECT COALESCE(MAX(followed_at), '') FROM followed_authors)
"""

_AUTHOR_NETWORK_CORPUS_FP_SQL = """
    SELECT
      (SELECT COUNT(*) FROM papers),
      (SELECT COALESCE(MAX(updated_at), '') FROM papers),
      (SELECT COUNT(*) FROM followed_authors),
      (SELECT COALESCE(MAX(followed_at), '') FROM followed_authors)
"""

# Topic map. Fingerprint covers paper count + last update; topic
# extraction is derived from paper records.
_TOPIC_MAP_FP_SQL = """
    SELECT
      (SELECT COUNT(*) FROM papers),
      (SELECT COALESCE(MAX(updated_at), '') FROM papers)
"""


# I-4: stamp the clustering/projection/labelling versions into the paper-map +
# author-network fingerprints so a CHANGE to the ML (e.g. the I-5 eom/no-forced-K
# clustering fix) invalidates the cached layout — input data alone can't, so a
# corrected algorithm would otherwise keep serving the old manufactured clusters.
_GRAPH_ML_VERSIONS = (CLUSTERING_ALGO_VERSION, PROJECTION_ALGO_VERSION, LABELLING_VERSION)
mv.register(mv.View(
    key="graph:paper_map:library",
    fingerprint_sql=with_version(_PAPER_MAP_LIBRARY_FP_SQL, *_GRAPH_ML_VERSIONS),
    build_fn=lambda conn: _build_paper_map_payload(conn, scope="library"),
    operation_key="materialize.graph.paper_map.library",
))
mv.register(mv.View(
    key="graph:paper_map:corpus",
    fingerprint_sql=with_version(_PAPER_MAP_CORPUS_FP_SQL, *_GRAPH_ML_VERSIONS),
    build_fn=lambda conn: _build_paper_map_payload(conn, scope="corpus"),
    operation_key="materialize.graph.paper_map.corpus",
))
mv.register(mv.View(
    key="graph:author_network:library",
    fingerprint_sql=with_version(_AUTHOR_NETWORK_LIBRARY_FP_SQL, *_GRAPH_ML_VERSIONS),
    build_fn=lambda conn: _build_author_network_payload(conn, scope="library"),
    operation_key="materialize.graph.author_network.library",
))
mv.register(mv.View(
    key="graph:author_network:corpus",
    fingerprint_sql=with_version(_AUTHOR_NETWORK_CORPUS_FP_SQL, *_GRAPH_ML_VERSIONS),
    build_fn=lambda conn: _build_author_network_payload(conn, scope="corpus"),
    operation_key="materialize.graph.author_network.corpus",
))
mv.register(mv.View(
    key="graph:topic_map",
    fingerprint_sql=_TOPIC_MAP_FP_SQL,
    build_fn=_build_topic_map_payload,
    operation_key="materialize.graph.topic_map",
))
