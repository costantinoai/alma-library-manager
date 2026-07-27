"""Graph visualization API endpoints."""

import hashlib
import json
import logging
import math
import sqlite3
import uuid
from collections import defaultdict
from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime
from typing import Any

import numpy as np
from fastapi import APIRouter, Depends, Header, HTTPException, Query, Response
from fastapi.responses import JSONResponse
from pydantic import BaseModel

from alma.ai.cooccurrence import cooccurrence_pairs
from alma.ai.embedding_graph import CouplingSpec, build_typed_edges
from alma.ai.graph_versions import (
    CLUSTERING_ALGO_VERSION,
    LABELLING_VERSION,
    PROJECTION_ALGO_VERSION,
    with_version,
)
from alma.api.deps import get_current_user, get_db, open_db_connection
from alma.api.helpers import table_exists
from alma.api.models import MapSelectionLensCreate, MapSelectionLensResponse
from alma.application import map_selection
from alma.application import materialized_views as mv
from alma.application.graph_process import graph_build_in_flight
from alma.application.graph_substrate import (
    ADMISSION_PERCENTILE,
    MAX_ADMISSION_COSINE,
    MIN_ADMISSION_SAMPLE,
    OUTLIER_CLUSTER_ID,
    OUTLIER_LABEL,
    PLACEMENT_INTERPOLATED,
    PLACEMENT_LAYOUT,
    SUBSTRATE_CLUSTER_RESOLUTION,
    SUBSTRATE_SCOPE,
    PlacementContext,
    PlacementField,
    SubstrateUnavailableError,
    place_vectors,
)
from alma.core.db_write import write_section
from alma.core.scope import Scope
from alma.core.sql_helpers import standalone_paper_sql
from alma.core.time import utcnow

logger = logging.getLogger(__name__)

router = APIRouter(
    dependencies=[Depends(get_current_user)],
    responses={401: {"description": "Unauthorized"}},
)

# Author-only layout contract. Bump when placement semantics change so existing
# materialized/variant payloads cannot keep serving obsolete geometry. The
# 2026-07-26 v2 aggregates authors over the durable paper substrate and admits
# only authors with at least two embedded/placed papers.
# v3: placement, eligibility, communities and labels are computed over the whole
# corpus and scope only filters which nodes are emitted. A v2 payload placed each
# author at the centroid of their IN-SCOPE papers, so its coordinates are wrong
# under the subset rule and must not survive the change.
_AUTHOR_NETWORK_LAYOUT_VERSION = "author-layout-one-space-v3"


def _author_network_placeable_count(
    conn: sqlite3.Connection,
    scope: Scope,
) -> int:
    """Return authors with at least two in-scope substrate papers.

    Graph GETs deliberately skip full fingerprint freshness work. An empty
    stored author payload is nevertheless invalid when the current corpus has
    enough placeable authors to fit a layout. This mirrors the builder's exact
    admission rule: two papers already embedded and placed on the ONE corpus
    substrate.
    """
    from alma.ai.projections import MIN_AUTHOR_PUBLICATIONS

    scope_filter = scope.paper_filter("p")
    try:
        row = conn.execute(
            f"""
            SELECT COUNT(*) FROM (
                SELECT pa.openalex_id AS author_id
                FROM publication_authors pa
                JOIN papers p ON p.id = pa.paper_id
                JOIN publication_clusters pc
                  ON pc.paper_id = pa.paper_id
                 AND pc.scope = ?
                WHERE TRIM(COALESCE(pa.openalex_id, '')) <> ''
                  {scope_filter}
                GROUP BY pa.openalex_id
                HAVING COUNT(DISTINCT pc.paper_id) >= ?
            )
            """,
            (SUBSTRATE_SCOPE, MIN_AUTHOR_PUBLICATIONS),
        ).fetchone()
    except sqlite3.OperationalError:
        return 0
    return int((row[0] if row else 0) or 0)


# ---------------------------------------------------------------------------
# Response models
# ---------------------------------------------------------------------------


class GraphNode(BaseModel):
    id: str
    name: str
    x: float = 0.5
    y: float = 0.5
    cluster_id: int | None = None
    color: str | None = None
    size: float = 1.0
    node_type: str = "paper"  # "paper" or "topic"
    # True when this node belongs to the Library (paper: status='library';
    # author: has >=1 library paper). In a corpus-scope graph the UI dims
    # non-library nodes to half opacity; in a library-scope graph every node
    # is in-library, so the default True is correct and nothing is dimmed.
    in_library: bool = True
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


@router.post(
    "/selection/lens",
    response_model=MapSelectionLensResponse,
    summary="Create a collection-backed lens from a visible map selection",
)
def create_selection_lens(
    body: MapSelectionLensCreate,
    conn: sqlite3.Connection = Depends(get_db),
):
    """Atomically save selected papers into a new collection and lens."""
    try:
        return map_selection.create_collection_lens(
            conn,
            name=body.name,
            selection_kind=body.selection_kind,
            ids=body.ids,
            scope=Scope(body.scope),
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


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

    def is_fresh(
        self,
        conn: sqlite3.Connection,
        stored: str,
        *,
        threshold: float = _VARIANT_DRIFT_THRESHOLD,
    ) -> bool:
        """True when the cached variant is still within the drift tolerance.

        ``threshold`` lets the scheduled maintenance job apply its own (looser)
        full-rebuild tolerance over the same gauge (task 50 M1) — one drift
        definition, two policies.
        """
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
        return drift < threshold


def _paper_scope_gauge(conn: sqlite3.Connection, scope: Scope) -> _VariantDataGauge:
    """Proportional gauge over the EMBEDDING SET in scope (task 50 M1).

    The old gauge measured ``papers.updated_at`` drift — but the hydration
    pipeline touches ~75% of rows weekly (metadata fills bump ``updated_at``),
    so every cached graph read as stale and rebuilt. Embedding-derived
    artifacts (layout, cluster structure, semantic edges) change when the
    VECTOR SET changes: rows added/removed for the active model, or the model
    itself switching (the model rides in the version tuple → exact
    invalidation). Node cosmetics (a corrected title) ride along on scheduled
    rebuilds instead of invalidating a multi-second fit. See
    ``tasks/lessons.md`` → "Semantic maps".
    """
    filt = scope.paper_filter("p", leading_and=False)
    where_extra = f" AND {filt}" if filt else ""
    try:
        from alma.discovery.similarity import get_active_embedding_model

        model = get_active_embedding_model(conn) or ""
    except Exception:
        model = ""
    model_sql = model.replace("'", "''")
    base = (
        "FROM publication_embeddings pe JOIN papers p ON p.id = pe.paper_id "
        f"WHERE pe.model = '{model_sql}'{where_extra}"
    )
    return _VariantDataGauge(
        versions=(CLUSTERING_ALGO_VERSION, PROJECTION_ALGO_VERSION, LABELLING_VERSION, model),
        count_sql=f"SELECT COUNT(*) {base}",
        watermark_sql=f"SELECT COALESCE(MAX(pe.created_at), '') {base}",
        changed_since_sql=f"SELECT COUNT(*) {base} AND pe.created_at > ?",
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
    job_label: str,
    process_spec: dict[str, Any] | None = None,
    prefetch: bool = False,
) -> GraphData | None:
    """Serve a graph variant from the durable cache, or enqueue its build.

    The ONE path shared by the paper-map and author-network variant routes
    (DRY): key the variant by its options, gauge freshness by embedding-set
    drift, and return a validated GraphData. Task 50 M1: a cache miss NEVER
    builds inline (a variant is a full re-cluster/re-layout — the >200 s
    author-network case) — it enqueues one background build and returns
    ``None`` so the route answers 202 and the client polls.
    """
    variant_key = _variant_view_key(base_view_key, options)
    if prefetch and mv.read_variant_row(conn, variant_key) is None:
        # A speculative warm-up must never START a variant build. A variant is a
        # full re-cluster/re-layout (the >200 s author-network case), and the
        # sidebar prefetch fires on hover — with a non-default blend saved in
        # sessionStorage, brushing the nav item would have queued exactly that
        # (finding C-9, 2026-07-26). Nothing cached, nothing to warm: 202.
        return None
    gauge = _paper_scope_gauge(conn, scope)
    payload = mv.get_or_enqueue_variant(
        conn,
        view_key=variant_key,
        build_fn=build_fn,
        make_fingerprint=lambda c: _paper_scope_gauge(c, scope).signature(c),
        is_fresh=lambda stored: gauge.is_fresh(conn, stored),
        job_label=job_label,
        process_spec=process_spec,
        # Single-flight across all layout work: a tuning slider must not stack a
        # second UMAP on top of a running corpus fit. Returning None here keeps
        # the route on its normal 202 + poll path.
        may_enqueue=lambda: graph_build_in_flight(conn) is None,
    )
    if payload is None:
        return None
    graph = GraphData(**payload)
    graph.metadata = {
        **(graph.metadata or {}),
        "delivery": {
            "source": "variant_cache",
            "stale": False,
            "rebuilding": False,
        },
    }
    return graph


def _enqueue_graph_view_build(conn: sqlite3.Connection, view_key: str) -> dict:
    """First-run bootstrap for a registered graph view: enqueue ONE background
    build (deduped by the view's operation key) and describe it for the 202.

    Single-flight across ALL layout work: while another fit is running this
    reports "queued" without adding a second one. The client polls every few
    seconds, so the build starts the moment the machine is free — no queue to
    keep, and never two UMAP children on the same corpus.
    """
    busy = graph_build_in_flight(conn)
    if busy:
        return {"job_id": "", "message": "Queued behind another layout build…"}
    job_id = mv.enqueue_rebuild(view_key)
    return {"job_id": job_id or "", "message": "Building the graph…"}


def _graph_etag(view_key: str, version: dict, annotations: dict) -> str:
    """A weak validator derived from the artifact identity, not from its bytes.

    Hashing the rendered response is what the HTTP-cache middleware does, and
    it is why a revalidation still cost a full render: decode 25 MB, rebuild
    every node, re-serialize, hash, then answer 304 with no body. Everything
    that can change this response is cheap to read — the stored fingerprint /
    computed_at plus the live annotations — so the validator is computed from
    those and a matching request never touches the payload.
    """
    blob = json.dumps(
        {
            "view": view_key,
            "fingerprint": version.get("fingerprint"),
            "computed_at": version.get("computed_at"),
            "annotations": annotations,
        },
        sort_keys=True,
        default=str,
    ).encode("utf-8")
    return 'W/"' + hashlib.sha1(blob).hexdigest() + '"'


def _etag_matches(if_none_match: str | None, etag: str) -> bool:
    """RFC-7232 If-None-Match test against ONE validator."""
    if not if_none_match:
        return False
    candidates = {item.strip() for item in if_none_match.split(",") if item.strip()}
    return etag in candidates or "*" in candidates


_GRAPH_CACHE_HEADERS = {"Cache-Control": "private, no-cache"}


def _serve_stored_graph(
    conn: sqlite3.Connection,
    *,
    view_key: str,
    annotations: dict,
    if_none_match: str | None,
    envelope: dict | None = None,
) -> Response | None:
    """Serve a durable graph artifact, or ``None`` when nothing is stored yet.

    Two deliberate departures from the ordinary route return:

    * **Conditional first.** The validator comes from `_graph_etag`, so an
      unchanged layout answers 304 without reading its payload at all.
    * **No Pydantic round-trip.** The stored payload was produced by
      ``GraphData.model_dump()`` at build time and validating it again on every
      read costs ~0.5 s on the corpus map for no new information. The
      annotations are merged into the decoded dict and sent as-is.

    Pass ``envelope`` when the caller already had to decode the payload for its
    own gate (the author network inspects its layout version); that skips the
    cheap-read path rather than decoding twice.
    """
    if envelope is None:
        version = mv.stored_version(conn, view_key)
        if version is None:
            return None
        etag = _graph_etag(view_key, version, annotations)
        if _etag_matches(if_none_match, etag):
            return Response(status_code=304, headers={"ETag": etag, **_GRAPH_CACHE_HEADERS})
        envelope = mv.get_stored(conn, view_key)
        if envelope is None:
            return None
    else:
        etag = _graph_etag(
            view_key,
            {
                "fingerprint": envelope.get("fingerprint"),
                "computed_at": envelope.get("computed_at"),
            },
            annotations,
        )
        if _etag_matches(if_none_match, etag):
            return Response(status_code=304, headers={"ETag": etag, **_GRAPH_CACHE_HEADERS})

    payload = dict(envelope.get("payload") or {})
    stored_meta = dict(payload.get("metadata") or {})
    # One-level MERGE, never replace: `layout` carries the payload's own
    # coordinate-frame declaration (which the terrain overlay depends on) AND
    # the live freshness read. A flat overwrite dropped the frame.
    merged_annotations = {
        key: (
            {**(stored_meta.get(key) or {}), **value}
            if isinstance(value, dict) and isinstance(stored_meta.get(key), dict)
            else value
        )
        for key, value in annotations.items()
    }
    payload["metadata"] = {
        **stored_meta,
        **merged_annotations,
        "stale": bool(envelope.get("stale", False)),
        "rebuilding": bool(envelope.get("rebuilding", False)),
        "computed_at": str(envelope.get("computed_at") or ""),
        "delivery": {
            "source": "materialized_view",
            "computed_at": str(envelope.get("computed_at") or ""),
            "compute_ms": int(envelope.get("compute_ms") or 0),
            "stale": bool(envelope.get("stale", False)),
            "rebuilding": bool(envelope.get("rebuilding", False)),
        },
    }
    return JSONResponse(
        content=payload,
        headers={"ETag": etag, **_GRAPH_CACHE_HEADERS},
    )


# Coordinate frames a paper-map payload can be expressed in. The terrain
# overlay keys on this: a valence field drawn at SUBSTRATE coordinates is
# meaningless on a layout that was fitted somewhere else (user report
# 2026-07-26 — "give shared authorship more weight and the terrain stays put").
LAYOUT_FRAME_SUBSTRATE = "substrate"
LAYOUT_FRAME_OWN = "own"


def _layout_frame(
    *,
    layout_mode: str,
    requested_resolution: float,
    layout_weights: dict | None,
    node_count: int,
) -> dict:
    """Declare which coordinate space this payload's x/y live in.

    ``substrate`` means the coordinates ARE the durable ``publication_clusters``
    layout — read from it, or placed against its centroids, which is the same
    frame. ``own`` means this build fitted its own projection (a non-default
    cluster detail re-fits UMAP; a layout blend re-solves positions from fused
    distances) and nothing outside this payload has a position in it.

    Hosts use this to decide whether the space-owned signal field can be splatted
    at its stored coordinates or must be joined onto these nodes by paper id.
    """
    blend = {
        key: float((layout_weights or {}).get(key) or 0.0)
        for key in ("semantic", "coauthorship", "bibliographic_coupling", "co_citation")
    }
    # The MODE is the whole truth about the frame, and the only honest source:
    # a non-default cluster detail marks every paper stale (→ a full re-fit),
    # and an applied blend stamps ``fused``. Deriving this from the REQUESTED
    # options instead would lie whenever the fused path declined (over its paper
    # cap, or it raised) and the substrate coordinates actually survived.
    substrate_frame = layout_mode in ("embeddings_cached", "embeddings_incremental")
    return {
        "frame": LAYOUT_FRAME_SUBSTRATE if substrate_frame else LAYOUT_FRAME_OWN,
        "method": layout_mode,
        "blend_applied": layout_mode == "fused",
        "cluster_resolution": round(float(requested_resolution), 3),
        "blend": blend,
        "node_count": int(node_count),
    }


def _layout_freshness(conn: sqlite3.Connection, scope: Scope, computed_at: str) -> dict:
    """Honest staleness annotation for a stored graph payload: when it was
    built and how many in-scope vectors arrived since (those papers are on the
    substrate via incremental placement but not yet in this cached payload —
    the scheduled maintenance rebuild folds them in)."""
    gauge = _paper_scope_gauge(conn, scope)
    try:
        new_since = int(
            conn.execute(gauge.changed_since_sql, (computed_at or "",)).fetchone()[0] or 0
        )
    except sqlite3.OperationalError:
        new_since = 0
    return {"computed_at": computed_at, "new_vectors_since_build": new_since}


def _build_graph_variant_payload(
    conn: sqlite3.Connection,
    *,
    graph_type: str,
    scope: Scope,
    options: dict[str, Any],
) -> dict:
    """Build one graph variant without persisting substrate coordinates.

    This top-level, JSON-parameterized entry point is shared by the request's
    cache setup and the isolated graph worker. Keeping the child protocol free
    of closures is what lets every expensive variant execute outside the API
    process while preserving one canonical implementation.
    """
    if graph_type == "author_network":
        return _build_author_network_payload(
            conn,
            scope=scope,
            cluster_resolution=float(options.get("cluster_resolution") or 1.0),
            layout_weights={
                "semantic": float(options.get("w_semantic") or 0.0),
                "coauthorship": float(options.get("w_coauthorship") or 0.0),
                "bibliographic_coupling": float(options.get("w_bibliographic") or 0.0),
            },
        )

    if graph_type != "paper_map":
        raise ValueError(f"unsupported graph variant type: {graph_type!r}")

    ai_state = _get_graph_ai_state(conn)
    graph_options = {
        "label_mode": str(options.get("label_mode") or "cluster"),
        "color_by": str(options.get("color_by") or "cluster"),
        "size_by": str(options.get("size_by") or "citations"),
        "show_edges": bool(options.get("show_edges", True)),
        "scope": scope,
        "cluster_resolution": float(
            options.get("cluster_resolution") or SUBSTRATE_CLUSTER_RESOLUTION
        ),
        "layout_weights": {
            "semantic": float(options.get("w_semantic") or 0.0),
            "coauthorship": float(options.get("w_coauthorship") or 0.0),
            "bibliographic_coupling": float(options.get("w_bibliographic") or 0.0),
            "co_citation": float(options.get("w_cocitation") or 0.0),
        },
    }
    embeddings = _load_embeddings(conn, scope=scope)
    if embeddings and len(embeddings) >= 5:
        result = _build_embedding_paper_map(
            conn,
            embeddings,
            ai_state=ai_state,
            graph_options=graph_options,
            persist=False,
        )
        # The embedding map omits vector-less papers. Keep that omission
        # explicit even on a process-built variant.
        try:
            total = int(
                conn.execute(
                    f"SELECT COUNT(*) FROM papers p WHERE "
                    f"{scope.paper_filter('p', leading_and=False)}"
                ).fetchone()[0]
                or 0
            )
            meta = dict(result.metadata or {})
            meta["vector_coverage"] = {"shown": len(embeddings), "total": total}
            result.metadata = meta
        except Exception:
            logger.debug("paper-map vector-coverage annotation skipped", exc_info=True)
    else:
        result = _build_text_paper_map(conn, scope=scope, ai_state=ai_state)
    return result.model_dump()


@router.get("/paper-map", response_model=GraphData)
def get_paper_map(
    label_mode: str = Query(
        "cluster", description="Label mode: cluster (c-TF-IDF over title text)"
    ),
    color_by: str = Query("cluster", description="Color by: cluster, year, rating, citations"),
    size_by: str = Query("citations", description="Size by: citations, uniform, rating"),
    show_edges: bool = Query(True, description="Show edges between nodes"),
    scope: str = Query(
        "library",
        description="library (default: Library-only papers) or corpus (every stored paper)",
    ),
    cluster_resolution: float = Query(
        SUBSTRATE_CLUSTER_RESOLUTION,
        ge=0.5,
        le=3.0,
        description="Cluster detail (default = the substrate resolution, 1.5): >1 finer (more clusters), <1 coarser. Non-default builds a variant in the background (202 while building).",
    ),
    w_semantic: float = Query(
        1.0, ge=0.0, le=1.0, description="PROTOTYPE: semantic-similarity weight in the fused layout"
    ),
    w_coauthorship: float = Query(
        0.0,
        ge=0.0,
        le=1.0,
        description="PROTOTYPE: co-authorship weight in the fused layout (0 = pure semantic)",
    ),
    w_bibliographic: float = Query(
        0.0,
        ge=0.0,
        le=1.0,
        description="PROTOTYPE: bibliographic-coupling weight in the fused layout",
    ),
    w_cocitation: float = Query(
        0.0,
        ge=0.0,
        le=1.0,
        description="Citation influence: co-citation weight in the fused layout (shared citers)",
    ),
    prefetch: bool = Query(
        False,
        description=(
            "Read-only warm-up. A prefetch NEVER enqueues a layout build: it "
            "reports 'building' and returns. Set by speculative callers "
            "(sidebar hover) so brushing a nav item cannot start minutes of "
            "background work the user never asked for."
        ),
    ),
    if_none_match: str | None = Header(
        default=None,
        alias="if-none-match",
        description=(
            "Conditional read. A layout is an immutable artifact, so a matching "
            "validator answers 304 without ever decoding the stored payload."
        ),
    ),
    conn: sqlite3.Connection = Depends(get_db),
):
    """Get paper map visualization data.

    Default options (cluster labels, cluster colour, citation size, edges
    on, resolution 1.0) are served via the materialised-view
    layer: cache hit returns instantly, fingerprint mismatch enqueues a
    background rebuild and serves the prior payload meanwhile. Custom
    option combinations (incl. a non-default cluster_resolution) bypass the
    cache and build inline — those are rare, ad-hoc views where caching every
    variant would be wasteful.
    """
    scope = Scope.parse(scope)
    # A fused layout (any non-zero non-semantic weight) is a custom, uncached view.
    fused_layout = w_coauthorship > 0 or w_bibliographic > 0 or w_cocitation > 0
    is_default_options = (
        label_mode == "cluster"
        and color_by == "cluster"
        and size_by == "citations"
        and show_edges
        # The frontend default MUST equal the substrate resolution — a
        # mismatch silently routes every visit down the variant path (the
        # 1.5-vs-1.0 bug, tasks/lessons.md "Semantic maps").
        and abs(cluster_resolution - SUBSTRATE_CLUSTER_RESOLUTION) < 1e-6
        and not fused_layout
    )

    if is_default_options:
        # Task 50 M1: a GET is a pure stored read. Freshness is owned by the
        # scheduled graph-layout maintenance job (embedding-set drift), never
        # by the request path — no fingerprint compute, no rebuild enqueue,
        # and NEVER an inline UMAP.
        view_key = scope.view_key("paper_map")
        version = mv.stored_version(conn, view_key)
        if version is not None:
            # Citation coverage and layout freshness are cheap live stats, not
            # cached data — computed on the read so they track reference
            # backfills and newly vectored papers immediately. Both ride in the
            # validator, so a change in either still invalidates the client's
            # copy. Pure reads.
            response = _serve_stored_graph(
                conn,
                view_key=view_key,
                annotations={
                    "citation_coverage": _citation_edge_coverage(conn, scope),
                    "layout": _layout_freshness(conn, scope, str(version.get("computed_at") or "")),
                },
                if_none_match=if_none_match,
            )
            if response is not None:
                return response
        return JSONResponse(
            status_code=202,
            content=(
                {"status": "building", "message": "Building the graph…"}
                if prefetch
                else {"status": "building", **_enqueue_graph_view_build(conn, view_key)}
            ),
        )

    # Custom-options path: build live, but cache durably + proportionally (task
    # #20) so a repeat request (the same slider position, a fused layout you already
    # viewed) returns instantly, AND the cache survives restarts + invalidates once a
    # real proportion of the corpus has changed — not on every paper insert.
    variant_options = {
        "label_mode": label_mode,
        "color_by": color_by,
        "size_by": size_by,
        "show_edges": show_edges,
        "cluster_resolution": cluster_resolution,
        "w_semantic": w_semantic,
        "w_coauthorship": w_coauthorship,
        "w_bibliographic": w_bibliographic,
        "w_cocitation": w_cocitation,
    }

    def _build_variant(c: sqlite3.Connection) -> dict:
        return _build_graph_variant_payload(
            c,
            graph_type="paper_map",
            scope=scope,
            options=variant_options,
        )

    graph = _serve_graph_variant(
        conn,
        base_view_key=scope.view_key("paper_map"),
        options=(
            label_mode,
            color_by,
            size_by,
            round(cluster_resolution, 3),
            round(w_semantic, 3),
            round(w_coauthorship, 3),
            round(w_bibliographic, 3),
            round(w_cocitation, 3),
        ),
        scope=scope,
        prefetch=prefetch,
        build_fn=_build_variant,
        job_label=f"paper map variant ({scope.label()})",
        process_spec={
            "graph_type": "paper_map",
            "scope": str(scope),
            "options": variant_options,
        },
    )
    if graph is None:
        return JSONResponse(
            status_code=202,
            content={"status": "building", "message": "Building this map variant…"},
        )
    # Citation-edge coverage is a cheap live stat, computed on the read so it's
    # correct even when the variant payload was cached before this annotation
    # existed (and so it tracks reference backfills immediately). Pure read.
    graph.metadata = {
        **(graph.metadata or {}),
        "citation_coverage": _citation_edge_coverage(conn, scope),
    }
    return graph


def _enqueue_corpus_layout_build(conn: sqlite3.Connection) -> dict:
    """Enqueue a background corpus-scope graph rebuild so the frontier map has
    persisted 2-D coordinates. Deduped by operation key (won't double-schedule),
    mirrors the rebuild-graphs enqueue. Returns the job envelope bits."""
    from alma.api.scheduler import add_job_log, find_active_job, schedule_immediate, set_job_status

    operation_key = f"graphs.rebuild_all:{Scope.corpus}"
    existing = find_active_job(operation_key)
    if existing:
        return {
            "job_id": str(existing.get("job_id") or ""),
            "message": "Building the semantic layout…",
        }
    # Single-flight (see `_enqueue_graph_view_build`): the frontier's substrate
    # is the corpus paper map, so if that fit is already running this request
    # has nothing to add — the client's poll picks up the finished layout.
    busy = graph_build_in_flight(conn, exclude_operation_key=operation_key)
    if busy:
        return {"job_id": "", "message": "Queued behind another layout build…"}

    job_id = f"frontier_layout_{uuid.uuid4().hex[:10]}"
    set_job_status(
        job_id,
        status="queued",
        operation_key=operation_key,
        trigger_source="auto:frontier",
        started_at=utcnow().isoformat(),
        message="Building corpus semantic layout for the frontier map",
    )
    add_job_log(job_id, "Queued corpus layout build for the frontier map", step="queued")

    def _runner() -> dict:
        from alma.application.graph_process import run_graph_process

        return run_graph_process(
            {
                "kind": "full_scope",
                "scope": str(Scope.corpus),
                "job_id": job_id,
            },
            job_id=job_id,
        )

    schedule_immediate(job_id, _runner)
    return {"job_id": job_id, "message": "Building the semantic layout…"}


def _score_seen_candidates(
    conn: sqlite3.Connection,
    centroid,
    *,
    exclude_ids: set[str],
    coords: dict[str, tuple[float, float]],
    limit: int,
) -> tuple[list[tuple[float, str, object, object]], int]:
    """Top-N embedded standalone papers by cosine to the library centroid,
    restricted to papers that HAVE a corpus map coordinate and are not library/
    removed/dismissed or in ``exclude_ids``. Reuses the discovery dense-fallback
    candidate shape. Returns (taken[:limit], total_pool)."""
    import numpy as np

    from alma.core.vector_blob import decode_vector
    from alma.discovery.similarity import get_active_embedding_model

    model = get_active_embedding_model(conn)
    if model is None or centroid is None:
        return [], 0
    centroid = np.asarray(centroid, dtype=float)
    cnorm = float(np.linalg.norm(centroid))
    if cnorm == 0:
        return [], 0

    rows = conn.execute(
        f"""
        SELECT p.id, p.title, p.year, pe.embedding
        FROM publication_embeddings pe
        JOIN papers p ON pe.paper_id = p.id
        WHERE pe.model = ?
          AND COALESCE(p.status, '') NOT IN ('library', 'removed', 'dismissed')
          AND {standalone_paper_sql("p")}
        """,
        (model,),
    ).fetchall()

    scored: list[tuple[float, str, object, object]] = []
    for r in rows:
        pid = str(r["id"])
        if pid in exclude_ids or pid not in coords:
            continue
        vec = decode_vector(r["embedding"])
        if vec is None:
            continue
        vec = np.asarray(vec, dtype=float)
        vnorm = float(np.linalg.norm(vec))
        if vnorm == 0:
            continue
        sim = float(np.dot(centroid, vec) / (cnorm * vnorm))
        scored.append((sim, pid, r["title"], r["year"]))

    scored.sort(reverse=True, key=lambda t: t[0])
    return scored[:limit], len(scored)


@router.get("/frontier")
def get_frontier(
    lens_id: str = Query(..., description="Discovery lens whose CURRENT suggestions to plot"),
    seen_limit: int = Query(
        0,
        ge=0,
        le=1000,
        description="Top-N seen-but-unacted papers by centroid similarity (0 = hide the seen layer)",
    ),
    include_edges: bool = Query(
        False,
        description="Also return coupling + co-citation edges between the placed nodes (off by default)",
    ),
    conn: sqlite3.Connection = Depends(get_db),
):
    """Layered semantic-map nodes for the Discovery frontier view (task 47 P3).

    Three layers over the corpus-scope 2-D layout: (a) all library papers
    (`layer=library`), (b) the lens's CURRENT suggestion set, branch-stamped
    (`layer=rec`), (c) opt-in top-N seen papers by cosine to the library
    centroid (`layer=seen`). `dismissed`/`removed` papers are never returned
    (D6/D3). Pure read except the one-time corpus-layout build, which returns
    202 `{status:'building'}` (the sanctioned cache-build path).
    """
    coord_count = int(
        conn.execute("SELECT COUNT(*) FROM publication_clusters WHERE scope = 'corpus'").fetchone()[
            0
        ]
        or 0
    )
    if coord_count == 0:
        return JSONResponse(
            status_code=202, content={"status": "building", **_enqueue_corpus_layout_build(conn)}
        )

    coords: dict[str, tuple[float, float]] = {}
    # Corpus cluster identity travels with the coordinates so the map can offer
    # "group by corpus clusters" as an ALTERNATIVE to branch colouring (47-H:
    # one grouping shown at a time, never both). -1 is the Unclustered bucket
    # and is deliberately kept — it's an honest "this didn't cluster", not a
    # topic. Labels come from the same c-TF-IDF pass the graph uses.
    clusters: dict[str, dict] = {}
    for r in conn.execute(
        "SELECT paper_id, x, y, cluster_id, label FROM publication_clusters WHERE scope = 'corpus'"
    ):
        pid = str(r["paper_id"])
        coords[pid] = (float(r["x"]), float(r["y"]))
        cid = r["cluster_id"]
        if cid is not None:
            clusters[pid] = {"cluster_id": int(cid), "cluster_label": r["label"]}

    nodes: list[dict] = []

    # (a) Library layer.
    lib_rows = conn.execute(
        f"SELECT id, title, year FROM papers p WHERE p.status = 'library' AND {standalone_paper_sql('p')}"
    ).fetchall()
    library_ids = [str(r["id"]) for r in lib_rows]
    for r in lib_rows:
        c = coords.get(str(r["id"]))
        if c:
            nodes.append(
                {
                    "paper_id": str(r["id"]),
                    "x": c[0],
                    "y": c[1],
                    "in_library": True,
                    "layer": "library",
                    "title": r["title"],
                    "year": r["year"],
                    **clusters.get(str(r["id"]), {}),
                }
            )
    library_shown = len(nodes)

    # (b) Recommendation layer — the lens's CURRENT suggestion set.
    rec_rows = conn.execute(
        f"""
        SELECT r.paper_id, r.branch_id, r.branch_label, r.score, p.title, p.year
        FROM recommendations r
        JOIN papers p ON p.id = r.paper_id
        WHERE r.lens_id = ?
          AND r.suggestion_set_id = (
              SELECT id FROM suggestion_sets WHERE lens_id = ? ORDER BY created_at DESC LIMIT 1
          )
          AND COALESCE(r.user_action, '') NOT IN ('dismiss', 'dismissed', 'remove', 'removed')
          AND COALESCE(p.status, '') NOT IN ('dismissed', 'removed')
          AND {standalone_paper_sql("p")}
        """,
        (lens_id, lens_id),
    ).fetchall()
    rec_ids: set[str] = set()
    recs_unplaced = 0
    recs_shown = 0
    for r in rec_rows:
        pid = str(r["paper_id"])
        rec_ids.add(pid)
        c = coords.get(pid)
        if c:
            nodes.append(
                {
                    "paper_id": pid,
                    "x": c[0],
                    "y": c[1],
                    "in_library": False,
                    "layer": "rec",
                    "branch_id": r["branch_id"],
                    "branch_label": r["branch_label"],
                    "score": r["score"],
                    "title": r["title"],
                    "year": r["year"],
                    **clusters.get(pid, {}),
                }
            )
            recs_shown += 1
        else:
            recs_unplaced += 1

    # (c) Seen layer — opt-in top-N by cosine to a centroid.
    #
    # The centroid is the LENS's own seeds when the lens has them, falling back
    # to the whole library otherwise. That matters: "nearest to my library" and
    # "nearest to what THIS lens is chasing" are different frontiers, and the
    # map is always shown in the context of one lens. `seen_ranked_by` tells the
    # UI which one it got, so the legend can say so instead of guessing.
    seen_shown = 0
    seen_total = 0
    seen_ranked_by = "library"
    if seen_limit > 0 and library_ids:
        from alma.discovery.scoring import compute_centroid_from_ids

        centroid = None
        try:
            from alma.application.discovery.lens_crud import get_lens
            from alma.application.discovery.seed_profile import _load_seed_papers_for_lens

            lens = get_lens(conn, lens_id)
            if lens is not None:
                seed_ids = [
                    str(s.get("id") or "").strip()
                    for s in (_load_seed_papers_for_lens(conn, lens) or [])
                ]
                seed_ids = [sid for sid in seed_ids if sid]
                if seed_ids:
                    centroid = compute_centroid_from_ids(conn, seed_ids)
                    if centroid is not None:
                        seen_ranked_by = "lens"
        except Exception:
            logger.debug("lens-centroid seen ranking unavailable; using library", exc_info=True)
        if centroid is None:
            centroid = compute_centroid_from_ids(conn, library_ids)
        if centroid is not None:
            taken, seen_total = _score_seen_candidates(
                conn,
                centroid,
                exclude_ids=rec_ids | set(library_ids),
                coords=coords,
                limit=seen_limit,
            )
            for _sim, pid, title, year in taken:
                c = coords[pid]
                nodes.append(
                    {
                        "paper_id": pid,
                        "x": c[0],
                        "y": c[1],
                        "in_library": False,
                        "layer": "seen",
                        "title": title,
                        "year": year,
                        **clusters.get(pid, {}),
                    }
                )
                seen_shown += 1

    # (d) Optional citation-fabric edges — the coupling (shared references) +
    # co-citation (shared citers) layers, so the frontier map can draw the
    # citation structure that links its terrain to its recommendations. Off by
    # default (extra work + visual density); computed on the SAME cooccurrence
    # primitive as the paper map. Restricted to the library + rec nodes (the
    # faint seen layer would otherwise dominate with seen↔seen noise) — the
    # useful signal is how the suggestions connect to what you already have.
    edges: list[dict] = []
    if include_edges:
        focus_ids = [n["paper_id"] for n in nodes if n["layer"] != "seen"]
    if include_edges and len(focus_ids) >= 2:
        for (a, b), c in _paper_bibliographic_coupling(conn, focus_ids, min_shared_refs=3).items():
            edges.append(
                {
                    "source": a,
                    "target": b,
                    "weight": round(min(1.0, 0.4 + 0.1 * c), 3),
                    "edge_type": "bibliographic_coupling",
                }
            )
        for (a, b), c in _paper_cocitation(conn, focus_ids, min_shared_citers=2).items():
            edges.append(
                {
                    "source": a,
                    "target": b,
                    "weight": round(min(1.0, 0.4 + 0.1 * c), 3),
                    "edge_type": "co_citation",
                }
            )

    return {
        "status": "ready",
        "nodes": nodes,
        "edges": edges,
        "counts": {
            "library": library_shown,
            "recs": recs_shown,
            "recs_unplaced": recs_unplaced,
            "seen_shown": seen_shown,
            "seen_total": seen_total,
            "edges": len(edges),
        },
        # Which centroid ranked the seen layer — "lens" or "library". The
        # legend states it rather than letting the user assume.
        "seen_ranked_by": seen_ranked_by,
        # Cluster hues belong to the SPACE, not to this deck. Discovery draws a
        # different subset of the substrate than the Map page does, so both read
        # the same ranking instead of each ranking what it happens to render.
        "cluster_hues": {str(cid): index for cid, index in substrate_cluster_hues(conn).items()},
    }


class RegionDescribeRequest(BaseModel):
    paper_ids: list[str]


@router.post("/region/describe")
def describe_region(
    body: RegionDescribeRequest,
    conn: sqlite3.Connection = Depends(get_db),
):
    """Characterise an arbitrary set of papers by its dominant vocabulary — the
    read behind selecting a region on the frontier map (task 47 §8, "Directions").

    POST only because the body carries up to ~300 paper ids; it is a **pure read**
    (verified by the GET-purity suite). Reuses the SAME c-TF-IDF labeler the
    cluster labels use — no second TF-IDF. Returns the region's label, top terms,
    three sample titles, honest membership counts (library / current suggestions /
    seen), and a `sufficient` flag (false below 5 papers → the UI shows "too few
    papers to characterize" and disables actions).
    """
    ids = list(dict.fromkeys(str(p).strip() for p in (body.paper_ids or []) if str(p).strip()))[
        :300
    ]
    sufficient = len(ids) >= 5
    if not ids:
        return {
            "label": "",
            "top_terms": [],
            "sample": [],
            "counts": {"library": 0, "recs": 0, "seen": 0},
            "sufficient": False,
        }

    placeholders = ",".join("?" for _ in ids)
    rows = conn.execute(
        f"SELECT id, title, abstract, status FROM papers WHERE id IN ({placeholders})",
        ids,
    ).fetchall()
    texts: dict[str, str] = {}
    titles: list[str] = []
    lib_ids: set[str] = set()
    for r in rows:
        pid = str(r["id"])
        title = str(r["title"] or "").strip()
        abstract = str(r["abstract"] or "").strip()
        texts[pid] = f"{title}. {abstract}".strip(". ").strip()
        if title:
            titles.append(title)
        if str(r["status"] or "") == "library":
            lib_ids.add(pid)
    library = len(lib_ids)

    # Membership counts mirror the frontier's mutually-exclusive layers:
    # library first, then "suggestions here" (current recommendations that are
    # NOT already in the library), then "seen" (everything else selected).
    non_lib = [i for i in ids if i not in lib_ids]
    recs = 0
    if non_lib:
        nl_placeholders = ",".join("?" for _ in non_lib)
        recs = int(
            conn.execute(
                f"""
                SELECT COUNT(DISTINCT paper_id) FROM recommendations
                WHERE paper_id IN ({nl_placeholders})
                  AND COALESCE(user_action, '') NOT IN ('dismiss', 'dismissed', 'remove', 'removed')
                """,
                non_lib,
            ).fetchone()[0]
            or 0
        )
    seen = max(0, len(ids) - library - recs)

    label = ""
    top_terms: list[str] = []
    if sufficient and texts:
        try:
            from alma.ai.clustering import (
                Cluster,
                label_clusters_tfidf,
                score_cluster_terms,
            )

            cluster = Cluster(cluster_id=0, member_keys=list(texts.keys()), label="", centroid=None)
            labels = label_clusters_tfidf([cluster], texts, top_n=4)
            label = labels[0] if labels else ""
            scored = score_cluster_terms({0: [t for t in texts.values() if t]}, top_k=10)
            top_terms = [term for term, _ in scored.get(0, [])][:8]
        except Exception:
            logger.debug("region describe labeling failed", exc_info=True)

    return {
        "label": label or "Selected region",
        "top_terms": top_terms,
        "sample": titles[:3],
        "counts": {"library": library, "recs": recs, "seen": seen},
        "sufficient": sufficient,
    }


@router.get("/author-network", response_model=GraphData)
def get_author_network(
    scope: str = Query("library", description="library (default) or corpus"),
    cluster_resolution: float = Query(
        # Author network stays at 1.0 (the precomputed MV layout). Unlike the
        # paper map, its non-default variant re-clusters AND re-lays-out the
        # whole graph live — too slow to default on a large corpus.
        1.0,
        ge=0.5,
        le=3.0,
        description="Cluster detail: >1 finer (more clusters), <1 coarser",
    ),
    w_semantic: float = Query(
        1.0, ge=0.0, le=1.0, description="PROTOTYPE: semantic weight in the fused author layout"
    ),
    w_coauthorship: float = Query(
        0.0,
        ge=0.0,
        le=1.0,
        description="PROTOTYPE: co-authorship weight in the fused author layout",
    ),
    w_bibliographic: float = Query(
        0.0,
        ge=0.0,
        le=1.0,
        description="PROTOTYPE: bibliographic-coupling weight in the fused author layout",
    ),
    prefetch: bool = Query(
        False,
        description=(
            "Read-only warm-up. A prefetch NEVER enqueues a layout build: it "
            "reports 'building' and returns. Set by speculative callers "
            "(sidebar hover) so brushing a nav item cannot start minutes of "
            "background work the user never asked for."
        ),
    ),
    if_none_match: str | None = Header(
        default=None,
        alias="if-none-match",
        description=(
            "Conditional read. A layout is an immutable artifact, so a matching "
            "validator answers 304 without ever decoding the stored payload."
        ),
    ),
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
        # Task 50 M1: stored read only — freshness is the maintenance job's,
        # never the GET's (see get_paper_map).
        view_key = scope.view_key("author_network")
        envelope = mv.get_stored(conn, view_key)
        stored_layout_version = (((envelope or {}).get("payload") or {}).get("metadata") or {}).get(
            "layout_version"
        )
        cached_nodes = ((envelope or {}).get("payload") or {}).get("nodes") or []
        from alma.ai.projections import MIN_AUTHOR_LAYOUT_NODES

        # While an empty/obsolete view is already rebuilding, polling must be a
        # constant-time status read. Re-running the placeability aggregation
        # every 2.5 s caused each poll to take seconds under build contention.
        if envelope is not None and not cached_nodes and envelope.get("rebuilding"):
            return JSONResponse(
                status_code=202,
                content={"status": "building", "message": "Building the graph…"},
            )
        empty_cache_is_invalid = (
            envelope is not None
            and not cached_nodes
            and _author_network_placeable_count(conn, scope) >= MIN_AUTHOR_LAYOUT_NODES
        )
        if (
            envelope is None
            or stored_layout_version != _AUTHOR_NETWORK_LAYOUT_VERSION
            or empty_cache_is_invalid
        ):
            # One-time compatibility gate, not a freshness calculation: a
            # pre-fix payload contains fabricated radial coordinates and is not
            # valid data under the current semantic-placement contract. The
            # same applies to a poisoned empty payload when current inputs can
            # place a real network. Treat either like a missing view and enqueue
            # the normal background build.
            return JSONResponse(
                status_code=202,
                content=(
                    {"status": "building", "message": "Building the graph…"}
                    if prefetch
                    else {"status": "building", **_enqueue_graph_view_build(conn, view_key)}
                ),
            )
        # The compatibility gate above already needed the decoded payload, so
        # hand it over rather than reading it a second time.
        return _serve_stored_graph(
            conn,
            view_key=view_key,
            annotations={},
            if_none_match=if_none_match,
            envelope=envelope,
        )

    variant_options = {
        "cluster_resolution": cluster_resolution,
        "w_semantic": w_semantic,
        "w_coauthorship": w_coauthorship,
        "w_bibliographic": w_bibliographic,
    }

    def _build_variant(c: sqlite3.Connection) -> dict:
        return _build_graph_variant_payload(
            c,
            graph_type="author_network",
            scope=scope,
            options=variant_options,
        )

    # Same durable cache as the paper map — gauged on embedding-set drift. A
    # miss enqueues the (formerly >200 s inline) re-layout in the background.
    graph = _serve_graph_variant(
        conn,
        base_view_key=scope.view_key("author_network"),
        options=(
            _AUTHOR_NETWORK_LAYOUT_VERSION,
            round(cluster_resolution, 3),
            round(w_semantic, 3),
            round(w_coauthorship, 3),
            round(w_bibliographic, 3),
        ),
        scope=scope,
        prefetch=prefetch,
        build_fn=_build_variant,
        job_label=f"author network variant ({scope.label()})",
        process_spec={
            "graph_type": "author_network",
            "scope": str(scope),
            "options": variant_options,
        },
    )
    if graph is None:
        return JSONResponse(
            status_code=202,
            content={"status": "building", "message": "Building this network variant…"},
        )
    return graph


@router.get("/signal-field")
def get_signal_field(conn: sqlite3.Connection = Depends(get_db)):
    """The SPACE-OWNED preference field over the corpus substrate.

    One valence per signal-carrying paper, at its substrate coordinates —
    independent of which dots any view renders (user call 2026-07-25: the
    stats of the space belong to the space; toggling layers must never
    change the terrain). Every map host splats THIS field for its Heat
    mode, so Discovery and the Map page show the same landscape.

    The valence hierarchy and every weight live in ONE place:
    `alma.core.signal_valence` (strongest user signal wins; engine
    evidence at reduced authority; no-signal papers carry
    VALENCE_NO_SIGNAL so EVERY substrate point has a value — the
    terrain has no holes, user call 2026-07-25).

    Since 2026-07-27 the field is a FIELD: where a paper carries no signal
    of its own, `alma.application.terrain` predicts one from its
    neighbourhood in embedding space and reports how much it trusts the
    prediction. Every point therefore carries `c` (confidence, 0–1) and
    `src` (what produced the value) so hosts can render a guess
    differently from a fact. See that module for the method.

    Pure read; the substrate is the durable corpus layout.
    """
    from alma.application.signal_lab.map_terms import (
        apply_lab_map_tint,
        load_lab_map_context,
    )
    from alma.application.terrain import build_terrain_field

    field = build_terrain_field(conn)
    lab_context = load_lab_map_context(conn)

    # The Signal Lab tint is the top layer over whatever the base resolved to,
    # observed or predicted — it adjusts a super-region, not a paper's evidence.
    points: list[dict] = []
    vmin = float("inf")
    vmax = float("-inf")
    vsum = 0.0
    for point in field.points:
        v = apply_lab_map_tint(
            point.value,
            paper_id=point.paper_id,
            cluster_id=point.cluster_id,
            context=lab_context,
        )
        points.append(
            {
                "id": point.paper_id,
                "x": point.x,
                "y": point.y,
                "v": round(v, 3),
                # How much this value is to be believed: 1.0 for something you
                # said, the GP's explained-variance fraction for something we
                # inferred, 0.0 for a point we know nothing about. Hosts fade
                # the splat by this so unworked territory looks unworked.
                "c": round(point.confidence, 3),
                "src": point.source,
                # Raw internal score (0-100, latest recommendation) rides
                # along so hosts colour Score mode LIVE — scores move with
                # every refresh while the cached layout payload does not,
                # and a stale materialized view must never grey the dots.
                "score": float(point.rec_score) if point.rec_score is not None else None,
            }
        )
        vmin = min(vmin, v)
        vmax = max(vmax, v)
        vsum += v

    stats = (
        {
            "min": round(vmin, 3),
            "max": round(vmax, 3),
            "mean": round(vsum / len(points), 3),
            "count": len(points),
        }
        if points
        else None
    )
    return {
        "status": "ready",
        "points": points,
        "stats": stats,
        "model": field.model.as_dict(),
    }


def _author_space_coordinates(conn: sqlite3.Connection) -> dict[str, tuple[float, float]]:
    """Author id (folded) → position in THE author space.

    The author space is the corpus author layout — the only one there is. A
    Library view draws a subset of its nodes, so anything that describes the
    space (the terrain) must read the whole thing from here rather than from
    whichever nodes the current payload happens to carry.

    Read from the durable corpus materialized view; empty while it has never
    been built, which simply means no terrain yet.
    """
    envelope = mv.get_stored(conn, Scope.corpus.view_key("author_network"))
    if not envelope:
        return {}
    out: dict[str, tuple[float, float]] = {}
    for node in (envelope.get("payload") or {}).get("nodes") or []:
        aid = str(node.get("id") or "").strip().lower()
        if not aid:
            continue
        try:
            out[aid] = (float(node["x"]), float(node["y"]))
        except (KeyError, TypeError, ValueError):
            continue
    return out


@router.get("/author-field")
def get_author_field(conn: sqlite3.Connection = Depends(get_db)):
    """The LIVE preference + score field for the author map.

    The author analogue of `/graphs/signal-field`, and deliberately built from
    the SAME contract: an author's valence aggregates `paper_valence` evidence
    over the papers of theirs you have an opinion about. User evidence carries
    full confidence, engine-only evidence is weak, and a neutral prior prevents
    one paper from reading as a settled author verdict. So "how do I feel about
    this author" is derived from "how do I feel about their papers" — one owner
    of the hierarchy, confidence, and weights (`alma.core.signal_valence`).

    **Not scoped.** How you feel about an author is a fact about the author, not
    about which view is open, so this is computed over ALL of their papers. It
    used to take a scope and average only the in-scope ones, which recoloured 48
    authors when you switched between Corpus and Library (measured 2026-07-26) —
    two different opinions about the same person depending on where you stood.

    Two reasons this is an endpoint rather than a field baked into the network
    payload (user catch 2026-07-26 — "terrain is all yellow and no scores"):

    * **Live.** The author network is a materialized view; the score baked into
      it goes stale the moment Discovery re-scores, which greyed the dots and
      flattened the terrain exactly like the paper map's baked score did.
    * **Coverage.** The old terrain used the mean recommendation score alone and
      substituted 0 for every author without one — in corpus scope that is ~90%
      of them, and a flood of hard zeros diluted the splat's local mean to
      nothing: uniform yellow. Valence covers every author carrying ANY signal
      (saved / rated / dismissed / scored), and authors with NO signal are
      returned with `v: null` so the host can omit them from the splat instead
      of drowning it. Pale paper where you have no opinion is the honest render.

    Keyed by author id (the OpenAlex id, as the network's nodes are) rather than
    by coordinates: unlike the paper substrate there is no off-view author to
    account for — the author map always draws every author in scope — so an
    id-keyed field is view-independent for exactly the same reason.

    Pure read.
    """
    from alma.application.signal_lab.fit import author_match_keys
    from alma.application.signal_lab.map_terms import (
        apply_lab_author_tint,
        load_lab_map_context,
    )
    from alma.core.signal_valence import (
        NEGATIVE_REC_ACTIONS,
        aggregate_author_valence,
        paper_valence_evidence,
    )

    neg_actions_sql = ",".join(f"'{a}'" for a in NEGATIVE_REC_ACTIONS)
    try:
        rows = conn.execute(
            f"""
            SELECT pa.openalex_id AS aid,
                   MIN(pa.display_name) OVER (PARTITION BY pa.openalex_id) AS aname,
                   p.status AS status,
                   COALESCE(p.rating, 0) AS rating,
                   latest.score AS rec_score,
                   COALESCE(neg.n_neg, 0) AS n_neg,
                   COALESCE(clicks.n_click, 0) AS n_click
            FROM publication_authors pa
            JOIN papers p ON p.id = pa.paper_id
            LEFT JOIN (
                SELECT paper_id, score, MAX(created_at)
                FROM recommendations GROUP BY paper_id
            ) latest ON latest.paper_id = pa.paper_id
            LEFT JOIN (
                SELECT paper_id, COUNT(*) AS n_neg
                FROM recommendations
                WHERE COALESCE(user_action, '') IN ({neg_actions_sql})
                GROUP BY paper_id
            ) neg ON neg.paper_id = pa.paper_id
            LEFT JOIN (
                SELECT entity_id, COUNT(*) AS n_click
                FROM feedback_events
                WHERE event_type = 'external_link_click' AND entity_type = 'publication'
                GROUP BY entity_id
            ) clicks ON clicks.entity_id = pa.paper_id
            WHERE TRIM(COALESCE(pa.openalex_id, '')) <> ''
            """
        ).fetchall()
    except sqlite3.OperationalError:
        rows = []

    # Valence only sees papers that carry evidence; an unread back catalogue
    # must not dilute it. Confidence and neutral shrinkage are owned centrally.
    agg: dict[str, dict] = defaultdict(
        lambda: {
            "evidence": [],
            "user_signal_papers": 0,
            "s_sum": 0.0,
            "s_n": 0.0,
            "papers": 0,
        }
    )
    names: dict[str, str] = {}
    for row in rows:
        entry = agg[str(row["aid"])]
        if row["aname"]:
            names.setdefault(str(row["aid"]), str(row["aname"]))
        entry["papers"] += 1
        rec_score = row["rec_score"]
        if rec_score is not None:
            entry["s_sum"] += float(rec_score)
            entry["s_n"] += 1
        evidence = paper_valence_evidence(
            status=str(row["status"] or ""),
            rating=int(row["rating"] or 0),
            n_negative_actions=int(row["n_neg"] or 0),
            n_engagements=int(row["n_click"] or 0),
            rec_score=rec_score,
        )
        if evidence is not None:
            entry["evidence"].append(evidence)
            if evidence.is_user_signal:
                entry["user_signal_papers"] += 1

    # Coordinates come from the ONE author space (the corpus layout), so the
    # field covers every placed author whether or not the current view draws
    # them. Same contract as `/graphs/signal-field` for papers: the terrain is a
    # property of the space, and a Library view is a subset of its DOTS only.
    coords = _author_space_coordinates(conn)

    # Signal Lab's author head bends this terrain the same way its region head
    # bends the paper terrain — same context, same `map_tint_strength` gate, so
    # answering a round moves both maps or neither.
    lab = load_lab_map_context(conn)

    authors: list[dict] = []
    vmin = float("inf")
    vmax = float("-inf")
    vsum = 0.0
    n_valenced = 0
    for aid, entry in agg.items():
        raw_v = aggregate_author_valence(entry["evidence"])
        keys = tuple(author_match_keys(names.get(aid, ""))) + (aid.strip().lower(),)
        raw_v = apply_lab_author_tint(raw_v, match_keys=keys, context=lab)
        v = round(raw_v, 3) if raw_v is not None else None
        point = coords.get(aid.strip().lower())
        authors.append(
            {
                "id": aid,
                "v": v,
                "score": round(entry["s_sum"] / entry["s_n"], 1) if entry["s_n"] else None,
                # How much evidence the valence rests on — the hover card says so
                # rather than presenting a 1-paper opinion as an author verdict.
                "signal_papers": len(entry["evidence"]),
                "user_signal_papers": int(entry["user_signal_papers"]),
                "papers": int(entry["papers"]),
                "x": point[0] if point else None,
                "y": point[1] if point else None,
            }
        )
        if v is not None:
            vmin = min(vmin, v)
            vmax = max(vmax, v)
            vsum += v
            n_valenced += 1

    stats = (
        {
            "min": round(vmin, 3),
            "max": round(vmax, 3),
            "mean": round(vsum / n_valenced, 3),
            "count": n_valenced,
        }
        if n_valenced
        else None
    )
    return {"status": "ready", "authors": authors, "stats": stats}


def _build_author_network_payload(
    conn: sqlite3.Connection,
    *,
    scope: str,
    cluster_resolution: float = 1.0,
    layout_weights: dict | None = None,
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

    # Author-level internal score: the mean of each author's papers' LATEST
    # recommendation scores (same engine criteria as Discovery; user call
    # 2026-07-25). Authors whose papers were never scored have no entry —
    # the map renders them recessive, never a fake neutral.
    author_scores: dict[str, float] = {}
    try:
        for row in conn.execute(
            """
            SELECT pa.openalex_id, AVG(latest.score)
            FROM publication_authors pa
            JOIN (
                SELECT paper_id, score, MAX(created_at)
                FROM recommendations
                GROUP BY paper_id
            ) latest ON latest.paper_id = pa.paper_id
            WHERE pa.openalex_id <> ''
            GROUP BY pa.openalex_id
            """
        ).fetchall():
            if row[1] is not None:
                author_scores[str(row[0])] = round(float(row[1]), 1)
    except sqlite3.OperationalError:
        pass

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
            in_library=bool(n.get("in_library", True)),
            metadata={
                "pub_count": n.get("pub_count", 0),
                "citation_count": n.get("citation_count", 0),
                "score": author_scores.get(str(n["id"])),
                "h_index": n.get("h_index", 0),
                "works_count": n.get("works_count", 0),
                "author_citedby": n.get("author_citedby", 0),
                "affiliation": n.get("affiliation", ""),
                "orcid": n.get("orcid", ""),
                "openalex_id": n.get("openalex_id", ""),
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

    # The author map has NO link layer (user call 2026-07-26). Its position is
    # the centroid of each author's papers on the corpus substrate; drawing the
    # paper-level relationships again as thousands of author lines would bury
    # the dots and add a large payload without adding information.
    edges: list[GraphEdge] = []

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
            "layout_version": _AUTHOR_NETWORK_LAYOUT_VERSION,
            "clusters": enriched_clusters,
            # No author link payload; the clustering panel describes the
            # bounded density pass over durable paper-substrate centroids.
            "edge_layers": raw.get("edge_layers", {}),
            "clustering": raw.get("clustering", {}),
            # In-scope two-paper authors with fewer than two embedded/placed
            # papers. Hidden before clustering; counted so the legend owns the
            # omission rather than inventing geometry.
            "omitted_unplaced": int(raw.get("omitted_unplaced", 0)),
        },
    )
    return result.model_dump()


def _rebuild_graphs_impl(
    conn: sqlite3.Connection, *, scope: Scope = Scope.library, job_id: str | None = None
) -> dict:
    """Rebuild the graph caches for ``scope`` without invalidating live reads.

    The caller runs this whole function in the isolated graph process. Existing
    materialized payloads and substrate rows stay available until their
    replacements are complete; only short final writes touch SQLite.

    I-3: paper_map + author_network rebuild the view for the requested
    ``scope`` (not a hardcoded ``:library``), so clicking "Rebuild" while
    viewing Corpus actually refreshes the Corpus graph the user is looking at.
    """
    from alma.api.scheduler import add_job_log, is_cancellation_requested, set_job_status

    def _cancelled() -> bool:
        return bool(job_id and is_cancellation_requested(job_id))

    rebuilt: list[str] = []
    phases = ["clear_cache", "paper_map", "author_network"]
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

    # Phase 1: clear only the retired graph_cache. Never delete the live
    # publication substrate before a replacement exists: Authors, Suggestions,
    # Home, and the maps read those coordinates while this job runs.
    _mark_progress(0, "clear_cache")
    try:
        with write_section(conn, label="graphs rebuild: clear_cache"):
            conn.execute("DELETE FROM graph_cache")
        if job_id:
            add_job_log(
                job_id,
                "Cleared legacy graph cache; retained the live substrate",
                step="clear_cache",
            )
    except sqlite3.OperationalError:
        if job_id:
            add_job_log(
                job_id, "Graph cache table missing; skipping clear step", step="clear_cache"
            )

    if _cancelled():
        if job_id:
            add_job_log(job_id, "Cancellation requested before paper map", step="cancelled")
        return {"rebuilt": rebuilt, "count": 0, "cancelled": True}

    # Reference enrichment has its own `/graphs/reference-backfill` job and
    # maintenance cadence. A layout rebuild is local compute; coupling it to 500
    # remote fetches made this button unpredictably long and writer-heavy.
    _mark_progress(1, "paper_map")
    try:
        if job_id:
            add_job_log(job_id, f"Rebuilding paper map ({scope.label()})", step="paper_map")
        mv.rebuild(
            conn,
            scope.view_key("paper_map"),
            build_fn=lambda c: _build_paper_map_payload(
                c,
                scope=str(scope),
                force_full_rebuild=scope is Scope.corpus,
            ),
        )
        rebuilt.append(scope.view_key("paper_map"))
    except Exception as e:
        logger.warning("Failed to rebuild paper_map: %s", e)
        if job_id:
            add_job_log(
                job_id, f"Failed rebuilding paper_map: {e}", level="ERROR", step="paper_map"
            )

    if _cancelled():
        if job_id:
            add_job_log(job_id, "Cancellation requested before author network", step="cancelled")
        return {"rebuilt": rebuilt, "count": len(rebuilt), "cancelled": True}

    _mark_progress(2, "author_network")
    try:
        if job_id:
            add_job_log(
                job_id, f"Rebuilding author network ({scope.label()})", step="author_network"
            )
        mv.rebuild(conn, scope.view_key("author_network"))
        rebuilt.append(scope.view_key("author_network"))
    except Exception as e:
        logger.warning("Failed to rebuild author_network: %s", e)
        if job_id:
            add_job_log(
                job_id,
                f"Failed rebuilding author_network: {e}",
                level="ERROR",
                step="author_network",
            )

    summary = {
        "rebuilt": rebuilt,
        "count": len(rebuilt),
        "message": f"Rebuilt {len(rebuilt)} {scope.label()} graph view(s)",
    }
    if job_id:
        add_job_log(
            job_id, f"Graph rebuild completed: {len(rebuilt)} rebuilt", step="done", data=summary
        )
    return summary


def _backfill_references_impl(conn: sqlite3.Connection, *, job_id: str | None = None) -> dict:
    """Backfill missing local publication references without rebuilding graph caches."""
    from alma.api.scheduler import add_job_log
    from alma.openalex.client import backfill_missing_publication_references

    if job_id:
        add_job_log(job_id, "Starting publication-reference backfill", step="reference_backfill")
    # `backfill_missing_publication_references` already gathers OpenAlex outside
    # the lock then commits its inserts in its own `write_section` — no caller
    # commit needed (and a raw one here would just be ungated).
    summary = backfill_missing_publication_references(conn, limit=500)
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
            titles, abstracts = _collect_paper_cluster_context(conn, member_ids, limit=6)
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
        entry["synthetic_key"]: entry["joined_text"] or "(empty)" for entry in refresh_entries
    }
    background_df, background_n = _load_cluster_term_background(conn)
    tfidf_labels = (
        label_clusters_tfidf(
            synthetic_clusters,
            cluster_texts,
            background_doc_freq=background_df,
            background_doc_count=background_n,
        )
        if synthetic_clusters
        else []
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
        from alma.application.graph_process import run_graph_process

        run_graph_process(
            {"kind": "registered_view", "view_key": view_key},
            job_id=job_id,
        )
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


def _load_vectors_for(conn: sqlite3.Connection, paper_ids: list[str]) -> dict[str, "np.ndarray"]:
    """Decode active-model embedding vectors for specific paper ids (I-13 helper).

    The representative selector needs the members' vectors; this loads + decodes
    just the requested ids through the canonical ``decode_vector`` (so float16 /
    legacy float32 are handled identically to ``_load_embeddings``). Returns the
    subset that actually has a vector — callers fall back when it's too sparse.
    """
    if not paper_ids:
        return {}
    from alma.core.vector_blob import decode_vector
    from alma.discovery.similarity import get_active_embedding_model

    try:
        model = get_active_embedding_model(conn)
        placeholders = ",".join("?" * len(paper_ids))
        rows = conn.execute(
            f"""
            SELECT pe.paper_id, pe.embedding
            FROM publication_embeddings pe
            JOIN papers p ON p.id = pe.paper_id
            WHERE pe.model = ?
              AND pe.paper_id IN ({placeholders})
              AND {standalone_paper_sql("p")}
            """,
            [model, *paper_ids],
        ).fetchall()
    except sqlite3.OperationalError:
        return {}
    out: dict[str, np.ndarray] = {}
    for r in rows:
        pid = r["paper_id"] if isinstance(r, sqlite3.Row) else r[0]
        blob = r["embedding"] if isinstance(r, sqlite3.Row) else r[1]
        if not blob:
            continue
        try:
            out[pid] = decode_vector(blob)
        except Exception:
            continue
    return out


def _collect_paper_cluster_context(
    conn: sqlite3.Connection,
    paper_ids: list[str],
    *,
    limit: int = 6,
) -> tuple[list[str], list[str]]:
    """Representative titles + abstracts for a paper cluster's label context.

    I-13: the representatives are the cluster's centroid-nearest + diverse members
    (``select_representatives`` over their embeddings), NOT the top-cited/recent
    papers — so the label reflects the cluster's topical core rather than its most
    famous members. Falls back to citation/recency order only when the members
    have too few usable vectors (the text-fallback / un-embedded case).
    """
    if not paper_ids:
        return [], []
    from alma.ai.clustering import select_representatives

    vectors = _load_vectors_for(conn, paper_ids)
    if len(vectors) >= 2:
        rep_ids = select_representatives(list(paper_ids), vectors, k=limit, diversity=0.3)
    else:
        placeholders = ",".join("?" * len(paper_ids))
        rows = conn.execute(
            f"""
            SELECT id FROM papers WHERE id IN ({placeholders})
            ORDER BY COALESCE(cited_by_count, 0) DESC, COALESCE(publication_date, '') DESC
            LIMIT ?
            """,
            [*paper_ids, limit],
        ).fetchall()
        rep_ids = [str(r["id"] if isinstance(r, sqlite3.Row) else r[0]) for r in rows]
    if not rep_ids:
        return [], []

    # Fetch the representatives' text and emit it in the selected order.
    placeholders = ",".join("?" * len(rep_ids))
    rows = conn.execute(
        f"SELECT id, title, abstract FROM papers WHERE id IN ({placeholders})",
        list(rep_ids),
    ).fetchall()
    by_id: dict[str, tuple[str, str]] = {}
    for row in rows:
        rid = str(row["id"] if isinstance(row, sqlite3.Row) else row[0])
        title = row["title"] if isinstance(row, sqlite3.Row) else row[1]
        abstract = row["abstract"] if isinstance(row, sqlite3.Row) else row[2]
        by_id[rid] = (str(title or "").strip() or "(untitled)", str(abstract or "").strip())

    titles: list[str] = []
    abstracts: list[str] = []
    for rid in rep_ids:
        if rid in by_id:
            t, a = by_id[rid]
            titles.append(t)
            abstracts.append(a)
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
              AND {standalone_paper_sql("p")}
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

    graph_type = (
        payload.graph_type if payload.graph_type in {"paper_map", "author_network"} else "paper_map"
    )
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
        started_at=utcnow().isoformat(),
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
    from alma.api.scheduler import (
        activity_envelope,
        find_active_job,
        schedule_immediate,
        set_job_status,
    )

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
        started_at=utcnow().isoformat(),
        message="Rebuilding graph cache",
    )

    def _runner() -> dict:
        from alma.application.graph_process import run_graph_process

        return run_graph_process(
            {
                "kind": "full_scope",
                "scope": str(scope_obj),
                "job_id": job_id,
            },
            job_id=job_id,
        )

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
    from alma.api.scheduler import (
        activity_envelope,
        find_active_job,
        schedule_immediate,
        set_job_status,
    )

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
        started_at=utcnow().isoformat(),
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

# Outlier group (I-6): ids/labels live in graph_substrate (the substrate owns
# cluster identity); only the render colour is a route concern.
OUTLIER_COLOR = "#94A3B8"  # slate-400: the same neutral used for "no cluster"

# Cluster-stability (mean pairwise ARI across UMAP seeds) re-fits UMAP+HDBSCAN
# ``n_seeds`` (5) extra times. That's cheap for a personal library but multiplies
# the corpus clustering cost ~5× (the corpus build the team optimised to ~16s),
# so we only measure stability when the scope is small enough to afford it. Larger
# scopes report stability as n/a (honest: not measured) rather than paying for it
# on every rebuild.
_STABILITY_MAX_NODES = 2000

# In-process, read-only cache for corpus-background cluster-term IDF. The key
# carries the DB path + corpus count/watermark + labelling version, so labels
# refresh when either the corpus text or the scorer logic changes without adding
# a database table or a write-on-GET side effect.
_CLUSTER_TERM_BACKGROUND_CACHE: dict[
    tuple[str, int, str, int, str], tuple[dict[str, int], int]
] = {}


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
                f"""
                SELECT COUNT(*) AS c
                FROM publication_embeddings pe
                JOIN papers p ON p.id = pe.paper_id
                WHERE pe.model = ?
                  AND {standalone_paper_sql("p")}
                """,
                (active_model,),
            ).fetchone()
            emb_count = int(r["c"] if isinstance(r, sqlite3.Row) else r[0])
    except Exception:
        emb_count = 0

    pub_count = 0
    try:
        r = conn.execute(
            f"SELECT COUNT(*) AS c FROM papers p WHERE {standalone_paper_sql('p')}"
        ).fetchone()
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


def _connection_cache_identity(conn: sqlite3.Connection) -> str:
    try:
        row = conn.execute("PRAGMA database_list").fetchone()
        path = str(row["file"] if isinstance(row, sqlite3.Row) else row[2] or "")
        return path or f"memory:{id(conn)}"
    except Exception:
        return f"connection:{id(conn)}"


def _load_cluster_term_background(
    conn: sqlite3.Connection,
    *,
    max_features: int = 4000,
) -> tuple[dict[str, int], int]:
    """Return corpus-wide paper DF for the same terms the cluster labeller uses.

    The background is always the whole standalone corpus, even for a Library map:
    cluster TF/prevalence stay scoped to the rendered cluster, while IDF answers
    "is this phrase distinctive against everything ALMa knows about?"
    """
    try:
        row = conn.execute(
            f"""
            SELECT COUNT(*) AS n, COALESCE(MAX(COALESCE(p.updated_at, p.created_at, '')), '') AS watermark
            FROM papers p
            WHERE {standalone_paper_sql("p")}
            """
        ).fetchone()
        corpus_n = int(row["n"] if isinstance(row, sqlite3.Row) else row[0] or 0)
        watermark = str(row["watermark"] if isinstance(row, sqlite3.Row) else row[1] or "")
    except sqlite3.OperationalError:
        return {}, 0
    if corpus_n <= 0:
        return {}, 0

    cache_key = (
        _connection_cache_identity(conn),
        corpus_n,
        watermark,
        int(max_features),
        LABELLING_VERSION,
    )
    cached = _CLUSTER_TERM_BACKGROUND_CACHE.get(cache_key)
    if cached is not None:
        return cached

    try:
        rows = conn.execute(
            f"""
            SELECT COALESCE(p.title, '') AS title, COALESCE(p.abstract, '') AS abstract
            FROM papers p
            WHERE {standalone_paper_sql("p")}
              AND (
                COALESCE(TRIM(p.title), '') <> ''
                OR COALESCE(TRIM(p.abstract), '') <> ''
              )
            """
        ).fetchall()
    except sqlite3.OperationalError:
        return {}, 0
    docs = [
        f"{row['title'] if isinstance(row, sqlite3.Row) else row[0]}. "
        f"{row['abstract'] if isinstance(row, sqlite3.Row) else row[1]}".strip()
        for row in rows
    ]
    docs = [doc for doc in docs if doc.strip()]
    if not docs:
        return {}, 0

    try:
        from sklearn.feature_extraction.text import CountVectorizer

        from alma.ai.clustering import _build_label_stop_words

        vectorizer = CountVectorizer(
            stop_words=_build_label_stop_words(),
            ngram_range=(1, 2),
            min_df=1,
            max_df=1.0,
            token_pattern=r"(?u)\b[a-zA-Z][a-zA-Z]+\b",
            lowercase=True,
            max_features=max_features,
        )
        counts = vectorizer.fit_transform(docs)
    except ValueError:
        return {}, 0

    binary = counts.copy()
    binary.data = np.ones_like(binary.data)
    df = np.asarray(binary.sum(axis=0)).ravel()
    feature_names = vectorizer.get_feature_names_out()
    result = ({str(term): int(df[idx]) for idx, term in enumerate(feature_names)}, len(docs))
    if len(_CLUSTER_TERM_BACKGROUND_CACHE) > 8:
        _CLUSTER_TERM_BACKGROUND_CACHE.clear()
    _CLUSTER_TERM_BACKGROUND_CACHE[cache_key] = result
    return result


def _build_text_paper_map(
    conn: sqlite3.Connection,
    *,
    scope: str,
    ai_state: dict | None = None,
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
    from sklearn.cluster import MiniBatchKMeans
    from sklearn.feature_extraction.text import TfidfVectorizer

    from alma.ai.clustering import Cluster, _silhouette_optimal_k, label_clusters_tfidf
    from alma.ai.projections import project_embeddings as _project_embeddings

    # Standalone gate (43.5): mirror `_load_embeddings` — exclude component rows
    # (figures / SI / datasets) and merged-away preprint twins (which KEEP
    # status='library' per preprint_dedup). This text-fallback path is the
    # DEFAULT MV build whenever <5 active-model embeddings exist (the no-AI
    # default), so without the gate those subordinate rows render as graph
    # nodes — including duplicate Library nodes.
    if scope == "library":
        rows = conn.execute(
            f"""
            SELECT id, title, abstract, year, journal, cited_by_count, rating,
                   publication_date, authors, status
            FROM papers p
            WHERE status = 'library'
              AND {standalone_paper_sql("p")}
            ORDER BY COALESCE(cited_by_count, 0) DESC,
                     COALESCE(publication_date, '') DESC
            """
        ).fetchall()
    else:
        rows = conn.execute(
            f"""
            SELECT id, title, abstract, year, journal, cited_by_count, rating,
                   publication_date, authors, status
            FROM papers p
            WHERE {standalone_paper_sql("p")}
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
        publication_date = (
            row["publication_date"] if isinstance(row, sqlite3.Row) else row[7]
        ) or None
        authors = (row["authors"] if isinstance(row, sqlite3.Row) else row[8]) or ""
        status = (row["status"] if isinstance(row, sqlite3.Row) else row[9]) or ""

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
            "in_library": status == "library",
        }

    n_papers = len(paper_ids)
    method_tag = "text_tfidf"
    cluster_assignments: dict[str, int] = {}
    coords: dict[str, tuple[float, float]] = {}
    similarity_matrix: np.ndarray | None = None
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
                pid: cid_map[int(km_labels[idx])] for idx, pid in enumerate(paper_ids)
            }
            cluster_sizes = {cid_map[old]: len(members_by_cid[old]) for old in sorted_old_cids}

            synthetic_clusters = [
                Cluster(
                    cluster_id=cid_map[old],
                    member_keys=members_by_cid[old],
                )
                for old in sorted_old_cids
            ]
            background_df, background_n = _load_cluster_term_background(conn)
            label_strings = label_clusters_tfidf(
                synthetic_clusters,
                {pid: docs[i] for i, pid in enumerate(paper_ids)},
                background_doc_freq=background_df,
                background_doc_count=background_n,
            )
            for c, lbl in zip(synthetic_clusters, label_strings):
                cluster_labels_by_cid[int(c.cluster_id)] = lbl

            # 2D layout: pretend each TF-IDF row is an embedding for projection.
            # ``project_embeddings`` falls back gracefully when UMAP isn't
            # installed (TSNE) or when n is very small (centred at origin).
            try:
                tfidf_embeddings = {paper_ids[i]: matrix[i].tolist() for i in range(n_papers)}
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
                color=(CLUSTER_COLORS[cid % len(CLUSTER_COLORS)] if cid is not None else None),
                size=max(1.0, math.log1p(meta["cited_by_count"])),
                in_library=bool(meta.get("in_library", True)),
                metadata={
                    "title": meta["title"],
                    "year": meta["year"],
                    "publication_date": meta["publication_date"],
                    "journal": meta["journal"],
                    "authors": meta["authors"],
                    "cited_by_count": meta["cited_by_count"],
                    "rating": meta["rating"],
                    "paper_id": pid,
                    "cluster_label": cluster_labels_by_cid.get(cid) if cid is not None else None,
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
        # The text fallback fits its OWN arrangement (TF-IDF / grid), which has
        # nothing to do with the embedding substrate. Say so, or an overlay
        # drawn at substrate coordinates would land on unrelated dots.
        "layout": {
            "frame": LAYOUT_FRAME_OWN,
            "method": method_tag,
            "node_count": len(nodes),
        },
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


def _build_cluster_detail(
    cluster_id: int,
    members: list[str],
    *,
    paper_meta: dict[str, dict[str, Any]],
    coords: dict[str, tuple[float, float]],
    label: str | None = None,
    cached_labels: dict[str, dict[str, object]] | None = None,
    member_vectors: dict[str, Any] | None = None,
) -> dict[str, Any]:
    xs = [coords[paper_id][0] for paper_id in members if paper_id in coords]
    ys = [coords[paper_id][1] for paper_id in members if paper_id in coords]
    citations: list[int] = []
    ratings: list[int] = []
    years: list[int] = []
    publication_dates: list[str] = []
    sample_rows: list[dict[str, Any]] = []

    for paper_id in members:
        meta = paper_meta.get(paper_id, {})
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

    # I-13: representatives are the papers nearest the cluster CENTROID, made
    # diverse via MMR — NOT citation/recency rank (which biased the samples AND the
    # labels toward famous/recent members rather than the cluster's topical core).
    # Falls back to citation order only when no embedding vectors are available
    # (the text-fallback map / tiny clusters).
    from alma.ai.clustering import cluster_cohesion, select_representatives

    row_by_id = {row["paper_id"]: row for row in sample_rows}
    cohesion = cluster_cohesion(members, member_vectors) if member_vectors else None
    if member_vectors and any(pid in member_vectors for pid in members):
        rep_ids = select_representatives(members, member_vectors, k=4, diversity=0.3)
        sample_papers = [row_by_id[pid] for pid in rep_ids if pid in row_by_id]
        representative_selection = "centroid_mmr"
    else:
        sample_rows.sort(
            key=lambda item: (
                int(item.get("cited_by_count") or 0),
                str(item.get("publication_date") or ""),
                int(item.get("year") or 0),
                str(item.get("title") or ""),
            ),
            reverse=True,
        )
        sample_papers = sample_rows[:4]
        representative_selection = "citation_rank"

    from alma.ai.cluster_labels import compute_cluster_signature

    cluster_signature = compute_cluster_signature(members)
    cached_entry = (cached_labels or {}).get(cluster_signature)
    cached_label = str(cached_entry.get("label") or "").strip() if cached_entry else ""
    cached_description = str(cached_entry.get("description") or "").strip() if cached_entry else ""
    cached_model = str(cached_entry.get("model") or "").strip() if cached_entry else ""

    resolved_label = cached_label or label or f"Cluster {cluster_id + 1}"
    # Top terms come from the text-based c-TF-IDF cluster label (the noisy
    # OpenAlex/S2 topic vocabulary is gone), split into the individual phrases the
    # cluster-detail chips render. A bare "Cluster N" placeholder yields no chips.
    top_topics = (
        [t.strip() for t in resolved_label.replace(" · ", ",").split(",") if t.strip()]
        if resolved_label and not resolved_label.startswith("Cluster ")
        else []
    )
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
        "sample_papers": sample_papers,
        # I-13: which papers represent the cluster + how they were chosen + a real
        # coherence metric (mean cosine to centroid), surfaced in the method panel.
        "representative_ids": [row["paper_id"] for row in sample_papers],
        "representative_selection": representative_selection,
        "cohesion": cohesion,
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


def _annotate_cluster_hues(
    conn: sqlite3.Connection,
    cluster_info: list[dict[str, Any]],
    *,
    cluster_members: dict[int, list[str]],
    substrate_frame: bool,
) -> None:
    """Stamp each cluster with its hue rank IN THE SPACE (mutates in place).

    A cluster's colour identifies WHICH region of the space it is, so it must be
    the same colour in every view of that space. Hosts used to rank clusters by
    their size among the RENDERED nodes, which meant the Library view — a subset
    of the very same corpus layout — recoloured every cluster: measured
    2026-07-26, the largest Library cluster was hue #0 in Library and hue #194 in
    Corpus, so switching scope reshuffled the whole map's palette.

    The ranking therefore comes from the space, never from the selection:

    * substrate frame — cluster sizes over the whole ``publication_clusters``
      corpus layout, so a Library payload and a Corpus payload agree exactly;
    * own frame — this payload's own membership, which for a corpus-fitted
      variant IS the whole space.

    Outliers are excluded: the Unclustered group has a fixed neutral colour, not
    a place on the hue ramp.
    """
    hue_index = (
        substrate_cluster_hues(conn)
        if substrate_frame
        else _hue_order(
            {int(cid): len(members) for cid, members in cluster_members.items() if int(cid) >= 0}
        )
    )
    for detail in cluster_info:
        cid = int(detail.get("id", OUTLIER_CLUSTER_ID))
        if cid in hue_index:
            detail["hue_index"] = hue_index[cid]


def _hue_order(sizes: dict[int, int]) -> dict[int, int]:
    """Cluster id → hue rank. Size DESC, then id ASC.

    The id tiebreak is not cosmetic: without it two equal-sized clusters could
    swap hues between two reads of the SAME space.
    """
    order = sorted(sizes.items(), key=lambda kv: (-kv[1], kv[0]))
    return {cid: index for index, (cid, _) in enumerate(order)}


def substrate_cluster_hues(conn: sqlite3.Connection) -> dict[int, int]:
    """Hue rank per cluster over the WHOLE corpus substrate.

    The one owner of "which colour is this cluster", shared by every host that
    paints substrate clusters — the paper map (both scopes) and the Discovery
    frontier. Each of them draws a different subset of the same space, and each
    used to rank the clusters it happened to be drawing, so one cluster wore
    three different colours depending on where you met it.
    """
    try:
        sizes = {
            int(row[0]): int(row[1])
            for row in conn.execute(
                "SELECT cluster_id, COUNT(*) FROM publication_clusters "
                "WHERE scope = ? AND cluster_id >= 0 GROUP BY cluster_id",
                (SUBSTRATE_SCOPE,),
            ).fetchall()
        }
    except sqlite3.OperationalError:
        return {}
    return _hue_order(sizes)


def _build_cluster_info(
    cluster_members: dict[int, list[str]],
    *,
    paper_meta: dict[str, dict[str, Any]],
    coords: dict[str, tuple[float, float]],
    labels_by_cluster: dict[int, str] | None = None,
    cached_labels: dict[str, dict[str, object]] | None = None,
    cluster_texts: dict[str, str] | None = None,
    vectors_by_id: dict[str, Any] | None = None,
    background_doc_freq: dict[str, int] | None = None,
    background_doc_count: int | None = None,
) -> list[dict[str, Any]]:
    labels = labels_by_cluster or {}
    word_clouds: dict[int, list[dict[str, Any]]] = {}
    if cluster_texts:
        word_clouds = _build_word_clouds_for_clusters(
            cluster_members,
            cluster_texts,
            background_doc_freq=background_doc_freq,
            background_doc_count=background_doc_count,
        )
    details = []
    for cid, members in sorted(cluster_members.items(), key=lambda kv: kv[0]):
        detail = _build_cluster_detail(
            int(cid),
            list(members),
            paper_meta=paper_meta,
            coords=coords,
            label=labels.get(int(cid), f"Cluster {int(cid) + 1}"),
            cached_labels=cached_labels,
            # I-13: the member vectors let the detail pick centroid-nearest +
            # diverse representatives (select_representatives filters by membership).
            member_vectors=vectors_by_id,
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
        compute_cluster_signature(members) for members in cluster_members.values() if members
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
    background_doc_freq: dict[str, int] | None = None,
    background_doc_count: int | None = None,
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
    scored = score_cluster_terms(
        member_texts,
        ngram_range=(1, 2),
        top_k=top_n,
        background_doc_freq=background_doc_freq,
        background_doc_count=background_doc_count,
    )
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
        # A subordinate row (dedup twin / part-of component) is never a graph
        # node — it must not be a point, cluster member, centroid input, or edge
        # endpoint. Both scopes join papers and apply the shared standalone gate;
        # the corpus query in particular MUST join (it used to read
        # publication_embeddings directly, so a leftover component vector leaked
        # straight into the map).
        if scope == "library":
            rows = conn.execute(
                f"""
                SELECT pe.paper_id, pe.embedding
                FROM publication_embeddings pe
                JOIN papers p ON p.id = pe.paper_id
                WHERE pe.model = ? AND p.status = 'library'
                  AND {standalone_paper_sql("p")}
                """,
                (active_model,),
            ).fetchall()
        else:
            rows = conn.execute(
                f"""
                SELECT pe.paper_id, pe.embedding
                FROM publication_embeddings pe
                JOIN papers p ON p.id = pe.paper_id
                WHERE pe.model = ? AND {standalone_paper_sql("p")}
                """,
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


def _latest_recommendation_scores(
    conn: sqlite3.Connection,
    paper_ids: list[str],
) -> dict[str, float]:
    """Latest internal recommendation score per paper (0-100), chunked.

    The map's Score colour mode shows the ENGINE's relevance, not the user's
    star rating (user call 2026-07-25). SQLite's bare-column-with-MAX rule
    picks ``score`` from the row holding ``MAX(created_at)`` — the newest
    suggestion wins. Papers never recommended simply have no entry.
    """
    scores: dict[str, float] = {}
    ids = list(dict.fromkeys(paper_ids))
    for start in range(0, len(ids), 400):
        chunk = ids[start : start + 400]
        placeholders = ",".join("?" for _ in chunk)
        try:
            rows = conn.execute(
                f"""
                SELECT paper_id, score, MAX(created_at)
                FROM recommendations
                WHERE paper_id IN ({placeholders})
                GROUP BY paper_id
                """,
                chunk,
            ).fetchall()
        except sqlite3.OperationalError:
            return scores
        for row in rows:
            paper_id = row["paper_id"] if isinstance(row, sqlite3.Row) else row[0]
            score = row["score"] if isinstance(row, sqlite3.Row) else row[1]
            if score is not None:
                scores[str(paper_id)] = float(score)
    return scores


def _load_paper_map_metadata(
    conn: sqlite3.Connection,
    paper_ids: list[str],
) -> tuple[dict[str, str], dict[str, dict]]:
    """Load paper-map text and node metadata in bounded SQL batches.

    Graph builds can contain thousands of embeddings. Loading one paper row per
    embedding made this phase an N+1 query; chunks of 400 stay below SQLite's
    common variable limit while preserving the same missing-row defaults.
    """
    ids = list(dict.fromkeys(paper_ids))
    texts = {paper_id: "" for paper_id in ids}
    rec_scores = _latest_recommendation_scores(conn, ids)
    paper_meta = {
        paper_id: {
            "title": "",
            "cited_by_count": 0,
            "year": None,
            "rating": 0,
            "score": rec_scores.get(paper_id),
            "journal": "",
            "authors": "",
            "publication_date": None,
            "in_library": False,
        }
        for paper_id in ids
    }
    for start in range(0, len(ids), 400):
        chunk = ids[start : start + 400]
        placeholders = ",".join("?" for _ in chunk)
        rows = conn.execute(
            f"""
            SELECT id, title, abstract, cited_by_count, year, rating, journal,
                   authors, publication_date, status
            FROM papers
            WHERE id IN ({placeholders})
            """,
            chunk,
        ).fetchall()
        for row in rows:
            paper_id = row["id"] if isinstance(row, sqlite3.Row) else row[0]
            title = row["title"] if isinstance(row, sqlite3.Row) else row[1]
            abstract = row["abstract"] if isinstance(row, sqlite3.Row) else row[2]
            cited_by_count = row["cited_by_count"] if isinstance(row, sqlite3.Row) else row[3]
            year = row["year"] if isinstance(row, sqlite3.Row) else row[4]
            rating = row["rating"] if isinstance(row, sqlite3.Row) else row[5]
            journal = row["journal"] if isinstance(row, sqlite3.Row) else row[6]
            authors = row["authors"] if isinstance(row, sqlite3.Row) else row[7]
            publication_date = row["publication_date"] if isinstance(row, sqlite3.Row) else row[8]
            status = row["status"] if isinstance(row, sqlite3.Row) else row[9]
            texts[paper_id] = f"{title or ''}. {abstract or ''}"
            paper_meta[paper_id] = {
                "title": title or "",
                "cited_by_count": int(cited_by_count or 0),
                "year": year,
                "rating": int(rating or 0),
                "score": rec_scores.get(paper_id),
                "journal": journal or "",
                "authors": authors or "",
                "publication_date": publication_date,
                "in_library": status == "library",
            }
    return texts, paper_meta


def _build_embedding_paper_map(
    conn: sqlite3.Connection,
    embeddings: dict[str, list[float]],
    *,
    ai_state: dict | None = None,
    graph_options: dict | None = None,
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
    from alma.ai.projections import project_embeddings

    opts = graph_options or {}
    color_by = opts.get("color_by", "cluster")
    size_by = opts.get("size_by", "citations")
    show_edges = opts.get("show_edges", True)
    # 50-G: ONE substrate. Layout rows are read + persisted for the corpus
    # scope only, regardless of which papers this build renders — a Library
    # build FILTERS the corpus layout, it never fits its own.
    layout_scope = SUBSTRATE_SCOPE
    # Whether this build may run a FULL re-cluster/re-projection when the
    # cached substrate can't serve it: True for the corpus substrate build and
    # for ad-hoc in-memory variants (persist=False); False for the default
    # Library build, which must assemble from the substrate (the caller
    # ensures the substrate exists first and retries).
    allow_full_rebuild = bool(opts.get("allow_full_rebuild", True))
    force_full_rebuild = bool(opts.get("force_full_rebuild", False))

    def _cosine(a: np.ndarray, b: np.ndarray) -> float:
        na = float(np.linalg.norm(a))
        nb = float(np.linalg.norm(b))
        if na <= 0 or nb <= 0:
            return 0.0
        return float(np.dot(a, b) / (na * nb))

    # Deterministic placement jitter is owned by graph_substrate (shared with
    # the standalone placement sweep) — see `graph_substrate.place_vectors`.

    paper_ids = list(embeddings.keys())
    vectors_by_id = {
        paper_id: np.asarray(vec, dtype=np.float32) for paper_id, vec in embeddings.items()
    }

    # Fetch per-paper text payloads (for labels and node metadata). The cluster
    # labels are c-TF-IDF over this title+abstract text — the noisy OpenAlex/S2
    # topic vocabulary is no longer loaded or attached to nodes.
    texts, paper_meta = _load_paper_map_metadata(conn, list(embeddings))

    background_df, background_n = _load_cluster_term_background(conn)

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
    stored_placement: dict[str, str | None] = {}
    try:
        rows = conn.execute(
            "SELECT paper_id, cluster_id, label, x, y, updated_at, placement "
            "FROM publication_clusters WHERE scope = ?",
            (layout_scope,),
        ).fetchall()
        for row in rows:
            pid = row["paper_id"] if isinstance(row, sqlite3.Row) else row[0]
            if pid not in vectors_by_id:
                continue
            stored_placement[str(pid)] = (
                row["placement"] if isinstance(row, sqlite3.Row) else row[6]
            ) or None
            layout_rows[pid] = {
                "cluster_id": int(
                    (row["cluster_id"] if isinstance(row, sqlite3.Row) else row[1]) or 0
                ),
                "label": (row["label"] if isinstance(row, sqlite3.Row) else row[2]) or "",
                "x": float((row["x"] if isinstance(row, sqlite3.Row) else row[3]) or 0.5),
                "y": float((row["y"] if isinstance(row, sqlite3.Row) else row[4]) or 0.5),
                "updated_at": str(
                    (row["updated_at"] if isinstance(row, sqlite3.Row) else row[5]) or ""
                ),
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
    # Papers THIS build positions by interpolation rather than by the fit. Drives
    # both the persisted `placement` stamp and the honesty count in the payload.
    interpolated_ids: set[str] = set()
    # Clustering diagnostics (I-4/I-6) — populated by the full-rebuild path; the
    # cached/incremental paths derive their counts from the loaded layout below.
    clustering_meta: dict[str, Any] = {}
    node_probabilities: dict[str, float] = {}

    # A non-substrate cluster_resolution must actually RE-CLUSTER: the cached
    # publication_clusters layout was built at SUBSTRATE_CLUSTER_RESOLUTION, so
    # reusing it would silently ignore the requested detail level. Force a full
    # recompute (variant builds only — they pass persist=False).
    requested_resolution = float(
        opts.get("cluster_resolution", SUBSTRATE_CLUSTER_RESOLUTION) or SUBSTRATE_CLUSTER_RESOLUTION
    )
    if abs(requested_resolution - SUBSTRATE_CLUSTER_RESOLUTION) > 1e-6:
        stale_ids = list(paper_ids)
        stable_ids = []
    if force_full_rebuild:
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

    # 2) Partial refresh: update only new/stale papers by nearest cached
    # centroids. Full-rebuild-capable builds cap this at 25% drift (beyond
    # that a fresh fit is more honest); substrate-only builds (the default
    # Library assembly) place EVERY missing paper incrementally — a full fit
    # is never theirs to run.
    elif (
        stable_ids
        and stale_ids
        and (not allow_full_rebuild or len(stale_ids) <= max(3, int(round(len(paper_ids) * 0.25))))
    ):
        layout_mode = "embeddings_incremental"
        for paper_id in stable_ids:
            cached = layout_rows[paper_id]
            cid = int(cached["cluster_id"])
            assignments[paper_id] = cid
            coords[paper_id] = (float(cached["x"]), float(cached["y"]))
            cluster_members[cid].append(paper_id)
            if cached.get("label"):
                labels_by_cluster[cid] = str(cached["label"])

        # Build the SAME placement context the standalone sweep uses, but from
        # the layout this build is holding in memory rather than from a second
        # read of publication_clusters (the rows here may not be persisted yet).
        centroid_vectors: dict[int, np.ndarray] = {}
        centroid_coords: dict[int, tuple[float, float]] = {}
        admission: dict[int, float] = {}
        field_ids: list[str] = []
        for cid, members in cluster_members.items():
            member_vectors = [vectors_by_id[pid] for pid in members if pid in vectors_by_id]
            if not member_vectors:
                continue
            centroid = np.mean(np.stack(member_vectors), axis=0)
            centroid_vectors[cid] = centroid
            xs = [coords[pid][0] for pid in members]
            ys = [coords[pid][1] for pid in members]
            centroid_coords[cid] = (float(np.mean(xs)), float(np.mean(ys)))
            if cid >= 0:
                field_ids.extend(pid for pid in members if pid in vectors_by_id)
                if len(member_vectors) >= MIN_ADMISSION_SAMPLE:
                    member_cos = np.array(
                        [_cosine(v, centroid) for v in member_vectors], dtype=np.float64
                    )
                    admission[cid] = min(
                        MAX_ADMISSION_COSINE,
                        float(np.percentile(member_cos, ADMISSION_PERCENTILE)),
                    )

        placement_field = None
        if field_ids:
            field_matrix = np.stack([vectors_by_id[pid] for pid in field_ids]).astype(np.float32)
            field_matrix /= np.clip(np.linalg.norm(field_matrix, axis=1, keepdims=True), 1e-9, None)
            placement_field = PlacementField(
                ids=tuple(field_ids),
                matrix=field_matrix,
                coords=np.asarray([coords[pid] for pid in field_ids], dtype=np.float32),
            )

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
            # THE shared placement rule (graph_substrate.place_vectors): the
            # cluster's own admission radius decides membership, the paper's
            # nearest already-placed neighbours decide position, and the two are
            # decided separately. Identical to the standalone sweep, so a paper
            # can't land somewhere else depending on which path reached it.
            placements = place_vectors(
                {pid: vectors_by_id[pid] for pid in stale_ids if pid in vectors_by_id},
                PlacementContext(
                    centroid_vectors=centroid_vectors,
                    centroid_coords=centroid_coords,
                    admission=admission,
                    field=placement_field,
                ),
            )
            for paper_id, placement in placements.items():
                assignments[paper_id] = placement.cluster_id
                cluster_members[placement.cluster_id].append(paper_id)
                coords[paper_id] = (placement.x, placement.y)
                interpolated_ids.add(paper_id)

    # 3) Full rebuild: clustering + 2D projection.
    if layout_mode == "embeddings_full" and not allow_full_rebuild:
        # A substrate-only build (default Library assembly) landed here, which
        # means the corpus substrate is missing/empty. The caller must build
        # the substrate first and retry — loud, never a silent library-only fit
        # persisted as the substrate (50-G).
        raise SubstrateUnavailableError(
            "corpus substrate missing; build the corpus layout before assembling a library view"
        )
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

        shared_knn = accel.shared_cosine_knn(embeddings)

        # Stability re-fits UMAP several times, so only pay for it on the
        # persisting REBUILD path (never a synchronous custom-options GET) AND only
        # when the scope is small enough to afford the ~5× clustering cost
        # (_STABILITY_MAX_NODES) — corpus rebuilds skip it and report n/a.
        # `cluster_resolution` (default 1.0) is the user-facing detail knob.
        clustering = cluster_publications(
            embeddings,
            compute_stability=persist and len(embeddings) <= _STABILITY_MAX_NODES,
            resolution=requested_resolution,
            precomputed_knn=shared_knn,
        )
        clusters = clustering.clusters
        node_probabilities = clustering.probabilities
        labels = label_clusters_tfidf(
            clusters,
            texts,
            background_doc_freq=background_df,
            background_doc_count=background_n,
        )
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
        generated_labels = label_clusters_tfidf(
            synthetic_clusters,
            texts,
            background_doc_freq=background_df,
            background_doc_count=background_n,
        )
        for cluster, label in zip(synthetic_clusters, generated_labels):
            labels_by_cluster[int(cluster.cluster_id)] = str(
                label or labels_by_cluster.get(int(cluster.cluster_id), "")
            )
        # The outlier group always carries the fixed Unclustered label.
        if OUTLIER_CLUSTER_ID in cluster_members:
            labels_by_cluster[OUTLIER_CLUSTER_ID] = OUTLIER_LABEL

    # Persist computed layout rows so subsequent refreshes can be incremental.
    # I-2: ONLY on the rebuild path (persist=True). A GET request never reaches
    # this block (the custom-options GET passes persist=False), so reads stay pure.
    # Gather-then-write: assignments/coords are computed above; persist each
    # bounded batch in its OWN gated `write_section` so a 1000+-upsert rebuild
    # takes BEGIN IMMEDIATE per chunk and releases the SQLite writer lock between
    # chunks (one txn over every upsert held the lock for seconds — see
    # ``tasks/10_ACTIVITY_CONCURRENCY.md``). Task 09: routes the old raw per-batch
    # commit through the central writer-gate primitive.
    if persist:
        # Full rebuilds persist every row (the layout moved); cached/incremental
        # builds persist ONLY the newly placed papers — rewriting 8k unchanged
        # rows per rebuild bumped their updated_at for nothing.
        persist_ids = list(paper_ids) if layout_mode == "embeddings_full" else list(stale_ids)
        now_iso = datetime.now().isoformat()
        cluster_batch_size = 200
        try:
            for batch_start in range(0, len(persist_ids), cluster_batch_size):
                batch = persist_ids[batch_start : batch_start + cluster_batch_size]
                with write_section(conn, label="graphs paper_map: persist clusters"):
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
                            INSERT INTO publication_clusters
                                (paper_id, scope, cluster_id, label, x, y, updated_at, placement)
                            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                            ON CONFLICT(paper_id, scope) DO UPDATE SET
                                cluster_id = excluded.cluster_id,
                                label = excluded.label,
                                x = excluded.x,
                                y = excluded.y,
                                updated_at = excluded.updated_at,
                                placement = excluded.placement
                            """,
                            (
                                paper_id,
                                layout_scope,
                                cid,
                                label,
                                float(x),
                                float(y),
                                now_iso,
                                (
                                    PLACEMENT_INTERPOLATED
                                    if paper_id in interpolated_ids
                                    else PLACEMENT_LAYOUT
                                ),
                            ),
                        )
        except sqlite3.OperationalError:
            # A transient lock means this pass didn't fully cache the layout; the MV
            # row still persists and the next rebuild/refresh retries. write_section
            # already rolled back the in-flight batch.
            logger.warning(
                "paper_map cluster persist hit a lock; layout not fully cached this pass"
            )

    # PROTOTYPE (task 19): fused multi-view layout. Clusters stay SEMANTIC
    # (computed/cached above — stable), but POSITIONS are re-blended from
    # semantic + co-authorship + bibliographic-coupling per the requested
    # weights. Applied AFTER persist so the cached layout stays pure-semantic.
    # Dense O(N²) ⇒ library-scale only; we hard-cap to avoid the corpus blowing
    # up (the sparse fuzzy-graph path is the corpus answer, still task 19).
    fused_max_papers = 1500
    layout_weights = opts.get("layout_weights")
    if (
        layout_weights
        and len(paper_ids) <= fused_max_papers
        and (
            float(layout_weights.get("coauthorship", 0) or 0) > 0
            or float(layout_weights.get("bibliographic_coupling", 0) or 0) > 0
            or float(layout_weights.get("co_citation", 0) or 0) > 0
        )
    ):
        try:
            from alma.ai.projections import fuse_layout

            fused_coords = fuse_layout(
                embeddings,
                _paper_coauthorship(conn, paper_ids),
                _paper_bibliographic_coupling(conn, paper_ids),
                weights=layout_weights,
                cocite_pairs=_paper_cocitation(conn, paper_ids),
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
    all_years = [
        int(paper_meta[pid].get("year") or 0) for pid in embeddings if paper_meta[pid].get("year")
    ]
    min_year = min(all_years) if all_years else 2000
    max_year = max(all_years) if all_years else 2026
    year_range = max(1, max_year - min_year)

    # Max citations for scaling
    max_citations = (
        max((paper_meta[pid].get("cited_by_count", 0) for pid in embeddings), default=1) or 1
    )

    def _placement_of(paper_id: str) -> str | None:
        """Coordinate provenance for a rendered paper.

        What THIS build decided wins; otherwise whatever the substrate row
        already carried. Rows written before the column existed stay ``None``
        — genuinely unknown, never claimed as either.
        """
        if paper_id in interpolated_ids:
            return PLACEMENT_INTERPOLATED
        if layout_mode == "embeddings_full":
            return PLACEMENT_LAYOUT
        return stored_placement.get(paper_id)

    # Build nodes.
    nodes: list[GraphNode] = []
    for paper_id in embeddings:
        meta = paper_meta.get(
            paper_id, {"title": "", "cited_by_count": 0, "year": None, "rating": 0}
        )
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
            rating_colors = {
                0: "#94A3B8",
                1: "#EF4444",
                2: "#F97316",
                3: "#F59E0B",
                4: "#10B981",
                5: "#3B82F6",
            }
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

        # Determine display label — the cluster's c-TF-IDF text label (or the
        # Unclustered label for density-noise nodes). The old topic-vocabulary
        # label mode is gone; `label_mode` is retained for API stability but the
        # only label source now is the text-based cluster label.
        is_outlier = cid is None or int(cid) < 0
        display_label = OUTLIER_LABEL if is_outlier else labels_by_cluster.get(int(cid))

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
                in_library=bool(meta.get("in_library", True)),
                metadata={
                    "paper_id": paper_id,
                    "cited_by_count": int(meta.get("cited_by_count") or 0),
                    "year": meta.get("year"),
                    "publication_date": meta.get("publication_date"),
                    "rating": meta.get("rating", 0),
                    "score": meta.get("score"),
                    "journal": meta.get("journal"),
                    "authors": meta.get("authors"),
                    "cluster_label": display_label,
                    "is_outlier": is_outlier,
                    "cluster_confidence": (
                        round(float(confidence), 3) if confidence is not None else None
                    ),
                    # How this dot's (x, y) were obtained — 'layout' (the fit),
                    # 'interpolated' (approximated between rebuilds), or None
                    # (placed before provenance was tracked).
                    "placement": _placement_of(paper_id),
                },
            )
        )

    # Typed, filterable edge LAYERS (Phase 3 / I-11) — built through the SHARED
    # embedding_graph.build_typed_edges, the SAME edge code the author network uses
    # (semantic mutual-kNN + structural coupling layers, one edge per pair per type
    # so the UI can filter). Retracted papers keep their node but get no edges, so a
    # retracted work is never drawn as a hub. Citation/h-index NEVER enter edge
    # geometry — they are node metadata. The coupling pair dicts are reused for the
    # post-persist fused layout above, so we hand them in precomputed.
    edges: list[GraphEdge] = []
    edge_layers: dict[str, int] = {}
    if show_edges:
        retracted = _retracted_paper_ids(conn, paper_ids)
        edge_dicts, edge_layers = build_typed_edges(
            embeddings,
            coupling_specs=[
                # Every structural layer is sparsified to each node's strongest
                # ~10 ties (like the semantic mutual-kNN at k=8) — a coupling
                # layer left dense is a payload bomb, not information (the
                # corpus map shipped 1.43M edges / 200 MB before these caps).
                CouplingSpec(
                    edge_type="bibliographic_coupling",
                    pairs=_paper_bibliographic_coupling(conn, paper_ids, min_shared_refs=3),
                    weight_floor=0.4,
                    weight_span=0.5,
                    top_k_per_node=10,
                ),
                CouplingSpec(
                    edge_type="co_authorship",
                    pairs=_paper_coauthorship(conn, paper_ids, min_shared_authors=1),
                    weight_floor=0.4,
                    weight_span=0.2,
                    weight_mode="linear_capped",
                    top_k_per_node=10,
                ),
                # Co-citation: papers cited together by ≥2 other papers (shared
                # reception). The forward-looking twin of bibliographic coupling
                # (shared references / shared past). Same cooccurrence primitive.
                CouplingSpec(
                    edge_type="co_citation",
                    pairs=_paper_cocitation(conn, paper_ids, min_shared_citers=2),
                    weight_floor=0.4,
                    weight_span=0.5,
                    top_k_per_node=10,
                ),
            ],
            semantic_k=8,
            semantic_min_similarity=0.45,
            exclude_ids=frozenset(retracted),
        )
        edges = [GraphEdge(**e) for e in edge_dicts]

    # Topic clusters only — the Unclustered group is reported as a count in the
    # clustering metadata, never as a pseudo-topic with a TF-IDF label (I-6).
    topic_cluster_members = {cid: members for cid, members in cluster_members.items() if cid >= 0}
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
        # I-13: medoid/diverse representative selection runs on these vectors.
        vectors_by_id=vectors_by_id,
        background_doc_freq=background_df,
        background_doc_count=background_n,
    )
    _annotate_cluster_hues(
        conn,
        cluster_info,
        cluster_members=cluster_members,
        substrate_frame=layout_mode in ("embeddings_cached", "embeddings_incremental"),
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
            # Honesty counters, same contract as the author map's
            # `omitted_unplaced`: an approximation the reader can see. Dots whose
            # position was interpolated since the last full fit, and dots placed
            # before provenance was tracked.
            "approximate_positions": sum(
                1 for pid in embeddings if _placement_of(pid) == PLACEMENT_INTERPOLATED
            ),
            "unknown_placement": sum(1 for pid in embeddings if _placement_of(pid) is None),
            "clusters": cluster_info,
            "clustering": clustering_panel,
            "layout": _layout_frame(
                layout_mode=layout_mode,
                requested_resolution=requested_resolution,
                layout_weights=layout_weights,
                node_count=len(nodes),
            ),
            # Per-layer edge counts so the UI can build filter toggles (I-11).
            "edge_layers": edge_layers,
            **(ai_state or {}),
        },
    )
    return result


def _get_cached_graph(conn: sqlite3.Connection, graph_type: str) -> GraphData | None:
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


def _cache_graph(conn: sqlite3.Connection, graph_type: str, data: GraphData) -> None:
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
    return {str(row["id"] if isinstance(row, sqlite3.Row) else row[0]) for row in rows}


def _paper_coauthorship(
    conn: sqlite3.Connection,
    paper_ids: list[str],
    *,
    min_shared_authors: int = 1,
    max_author_df: int | None = 50,
) -> dict[tuple[str, str], int]:
    """Pairs of papers that share at least ``min_shared_authors`` authors.

    Co-authorship edge layer for the paper map (Phase 3 / I-11): papers keyed by
    their (case-folded) OpenAlex author ids, paired via the shared
    ``cooccurrence_pairs`` primitive (one indexed scan + inverted index, NOT a
    self-join).

    ``max_author_df`` drops authors appearing on more than N in-set papers
    before pairing. A followed author with 500 corpus papers is *legitimately*
    on all of them — but C(500,2) ≈ 125k clique edges carry zero visual
    information and were the payload bomb the 2026-07-25 audit found on the
    corpus map (1.33M co-authorship edges, 200 MB of JSON). Same philosophy as
    the bib-coupling hub-reference cap and the author-network mega-consortium
    cap (2026.07-4/-6). Silently returns {} when the table is missing.
    """
    if not paper_ids or not table_exists(conn, "publication_authors"):
        return {}
    placeholders = ",".join(["?"] * len(paper_ids))
    try:
        rows = conn.execute(
            f"""
            SELECT paper_id, lower(openalex_id) AS oid
            FROM publication_authors
            WHERE paper_id IN ({placeholders})
              AND TRIM(COALESCE(openalex_id, '')) <> ''
            """,
            list(paper_ids),
        ).fetchall()
    except sqlite3.OperationalError:
        return {}
    paper_authors: dict[str, list[str]] = defaultdict(list)
    for r in rows:
        pid = r["paper_id"] if isinstance(r, sqlite3.Row) else r[0]
        oid = r["oid"] if isinstance(r, sqlite3.Row) else r[1]
        paper_authors[str(pid)].append(str(oid))
    return cooccurrence_pairs(
        paper_authors, min_shared=min_shared_authors, max_feature_df=max_author_df
    )


def _paper_bibliographic_coupling(
    conn: sqlite3.Connection,
    paper_ids: list[str],
    *,
    min_shared_refs: int = 3,
    max_ref_df: int = 50,
) -> dict[tuple[str, str], int]:
    """Return pairs of papers that share at least `min_shared_refs` references.

    Bibliographic coupling signal for the paper map (papers keyed by their
    referenced works, paired via the shared ``cooccurrence_pairs`` primitive).
    Silently returns {} when the publication_references table is missing.

    PERF + QUALITY (the corpus self-join was 372s = 93% of the build): the
    ``max_ref_df`` cap drops references cited by more than that many papers — the
    O(df²) pair explosion AND non-discriminative noise (everyone cites the famous
    review). The cap, the bibliographic analogue of IDF, lives in the shared
    primitive now; one indexed paper_id scan builds the paper→refs map it pairs.
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

    paper_refs: dict[str, list[str]] = defaultdict(list)
    for r in rows:
        pid = r["paper_id"] if isinstance(r, sqlite3.Row) else r[0]
        ref = r["referenced_work_id"] if isinstance(r, sqlite3.Row) else r[1]
        paper_refs[str(pid)].append(str(ref))
    return cooccurrence_pairs(paper_refs, min_shared=min_shared_refs, max_feature_df=max_ref_df)


def _paper_cocitation(
    conn: sqlite3.Connection,
    paper_ids: list[str],
    *,
    min_shared_citers: int = 2,
    max_citer_df: int = 50,
) -> dict[tuple[str, str], int]:
    """Return pairs of corpus papers that are *co-cited* — cited together by at
    least ``min_shared_citers`` other papers.

    Co-citation is the inverse of bibliographic coupling: coupling groups papers
    that share the same REFERENCES (a shared past), co-citation groups papers
    that share the same CITERS (a shared reception). Two papers B and C are
    co-cited whenever some third paper A cites both — so the graph entity is the
    cited *corpus* paper and the shared feature is the citing paper. Built on the
    same ``cooccurrence_pairs`` primitive as coupling (one indexed scan +
    inverted index, no self-join).

    Only cited works that resolve to a corpus paper in ``paper_ids`` become
    entities, so every emitted pair is an edge between two visible graph nodes.
    The cited/local mapping is ``papers.openalex_id = 'W' || referenced_work_id``
    (verified live 2026-07-25). ``max_citer_df`` is the IDF-analogue cap: a
    review that cites hundreds of corpus papers co-cites every pair among them
    (O(df²) explosion + non-discriminative noise), so citers held by more than
    that many entities are dropped before pairing. Silently returns {} when
    ``publication_references`` is missing.
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

    placeholders = ",".join(["?"] * len(paper_ids))
    # Map the visible corpus papers' OpenAlex ids → local paper id, keyed by the
    # numeric work-id form stored in publication_references.referenced_work_id.
    refwork_to_local: dict[str, str] = {}
    id_rows = conn.execute(
        f"""
        SELECT id, openalex_id
        FROM papers
        WHERE id IN ({placeholders})
          AND TRIM(COALESCE(openalex_id, '')) <> ''
        """,
        list(paper_ids),
    ).fetchall()
    for r in id_rows:
        pid = r["id"] if isinstance(r, sqlite3.Row) else r[0]
        oa = str(r["openalex_id"] if isinstance(r, sqlite3.Row) else r[1]).strip()
        # 'W1002902372' → '1002902372'; skip anything not in that canonical shape.
        if oa[:1] in ("W", "w") and oa[1:].isdigit():
            refwork_to_local[oa[1:]] = str(pid)
    if not refwork_to_local:
        return {}

    # Pull every reference row whose cited work is one of our visible papers.
    ref_placeholders = ",".join(["?"] * len(refwork_to_local))
    try:
        rows = conn.execute(
            f"""
            SELECT paper_id AS citing, referenced_work_id AS ref
            FROM publication_references
            WHERE CAST(referenced_work_id AS TEXT) IN ({ref_placeholders})
            """,
            list(refwork_to_local.keys()),
        ).fetchall()
    except sqlite3.OperationalError:
        return {}

    # Entity = cited corpus paper; feature = the paper that cites it.
    cited_citers: dict[str, list[str]] = defaultdict(list)
    for r in rows:
        citing = r["citing"] if isinstance(r, sqlite3.Row) else r[0]
        ref = str(r["ref"] if isinstance(r, sqlite3.Row) else r[1])
        local = refwork_to_local.get(ref)
        if local is not None:
            cited_citers[local].append(str(citing))
    return cooccurrence_pairs(
        cited_citers, min_shared=min_shared_citers, max_feature_df=max_citer_df
    )


def _citation_edge_coverage(conn: sqlite3.Connection, scope: Any) -> dict[str, Any] | None:
    """How much of the scope has the reference rows that citation edges need.

    ``covered`` = standalone papers in scope with ≥1 ``publication_references``
    row; ``total`` = standalone papers in scope; ``pct`` in [0, 100]. This is the
    honest denominator for "citation edges cover N% of corpus" — coupling and
    co-citation can only connect papers whose references we actually hold.
    Returns None on any measurement error (never blocks a graph build).
    """
    try:
        sc = scope if isinstance(scope, Scope) else Scope.parse(scope or "library")
        where = sc.paper_filter("p", leading_and=False)
        total = int(conn.execute(f"SELECT COUNT(*) FROM papers p WHERE {where}").fetchone()[0] or 0)
        covered = int(
            conn.execute(
                f"""
                SELECT COUNT(*) FROM papers p
                WHERE {where}
                  AND EXISTS (
                    SELECT 1 FROM publication_references pr WHERE pr.paper_id = p.id
                  )
                """
            ).fetchone()[0]
            or 0
        )
        return {
            "covered": covered,
            "total": total,
            "pct": round(100.0 * covered / total, 1) if total else None,
        }
    except Exception:
        logger.debug("citation-edge coverage annotation skipped", exc_info=True)
        return None


# ---------------------------------------------------------------------------
# Materialised-view registrations
# ---------------------------------------------------------------------------
#
# Each public graph endpoint registers a view here so a cache hit returns
# in <10 ms on the GET path. The fingerprint captures every input that
# should change the rendered graph: corpus / library paper count, last
# Library mutation, embedding count + active model (paper_map), and
# followed-author count + last follow time (author_network). On
# fingerprint mismatch the prior payload is
# served immediately and a background rebuild job runs under
# `materialize.graph.<view>` — `useOperationToasts` invalidates the
# matching React Query roots when it completes.


def _build_paper_map_payload(
    conn: sqlite3.Connection,
    *,
    scope: str,
    force_full_rebuild: bool = False,
) -> dict:
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
        "cluster_resolution": SUBSTRATE_CLUSTER_RESOLUTION,
        # 50-G: only the corpus build may fit a layout — it IS the substrate
        # build. The library build assembles (filters + incremental placement)
        # from the substrate.
        "allow_full_rebuild": scope == SUBSTRATE_SCOPE,
        # User-triggered Corpus rebuilds must actually refit without deleting
        # the live substrate first. The old rows stay readable until the fresh
        # coordinates are persisted in short batches.
        "force_full_rebuild": force_full_rebuild and scope == SUBSTRATE_SCOPE,
    }
    embeddings = _load_embeddings(conn, scope=scope)
    if embeddings and len(embeddings) >= 5:
        try:
            result = _build_embedding_paper_map(
                conn, embeddings, ai_state=ai_state, graph_options=graph_options
            )
        except SubstrateUnavailableError:
            # Library assembly found no substrate (fresh DB / post-migration).
            # Build the corpus substrate once — the ONE full fit — then retry
            # the assembly, which now reads it.
            logger.info("paper_map(library): substrate missing — building corpus layout first")
            mv.rebuild(conn, Scope.corpus.view_key("paper_map"))
            result = _build_embedding_paper_map(
                conn, embeddings, ai_state=ai_state, graph_options=graph_options
            )
    else:
        result = _build_text_paper_map(conn, scope=scope, ai_state=ai_state)
    return result.model_dump()


# Paper map (per scope). Fingerprint covers Library/corpus paper count
# and last update, embedding count for the active model, and the active
# model itself — any of these change → cached layout is stale.
# I-4: the fingerprint must shift on EVERY input the rendered map reads, not just
# the paper set. Two were missing and are added here:
#   * the embedding-recompute watermark (MAX created_at) — a re-embed that keeps the
#     same row count and doesn't touch papers.updated_at would otherwise serve a map
#     built on the OLD vectors; node positions + clusters depend on the vectors;
#   * the reference count — the bibliographic-coupling edge layer is derived from
#     publication_references, so a reference backfill changes the edges.
# (Topics are deliberately NOT here: the noisy OpenAlex/S2 topic vocabulary was
# removed from the machines in LABELLING_VERSION 2026.07-6 — labels come from title
# text now, so publication_topics is no longer a graph input.)
_PAPER_MAP_LIBRARY_FP_SQL = f"""
    SELECT
      (SELECT COUNT(*) FROM papers p WHERE p.status = 'library' AND {standalone_paper_sql("p")}),
      (SELECT COALESCE(MAX(p.updated_at), '') FROM papers p WHERE p.status = 'library' AND {standalone_paper_sql("p")}),
      (SELECT COUNT(*) FROM publication_embeddings pe
         JOIN papers p ON p.id = pe.paper_id
         WHERE p.status = 'library' AND {standalone_paper_sql("p")}),
      (SELECT COALESCE(value, '') FROM discovery_settings WHERE key = 'embedding_model'),
      (SELECT COALESCE(MAX(pe.created_at), '') FROM publication_embeddings pe
         JOIN papers p ON p.id = pe.paper_id WHERE p.status = 'library' AND {standalone_paper_sql("p")}),
      (SELECT COUNT(*) FROM publication_references pr
         JOIN papers p ON p.id = pr.paper_id WHERE p.status = 'library' AND {standalone_paper_sql("p")})
"""

_PAPER_MAP_CORPUS_FP_SQL = f"""
    SELECT
      (SELECT COUNT(*) FROM papers p WHERE {standalone_paper_sql("p")}),
      (SELECT COALESCE(MAX(p.updated_at), '') FROM papers p WHERE {standalone_paper_sql("p")}),
      (SELECT COUNT(*) FROM publication_embeddings pe
         JOIN papers p ON p.id = pe.paper_id WHERE {standalone_paper_sql("p")}),
      (SELECT COALESCE(value, '') FROM discovery_settings WHERE key = 'embedding_model'),
      (SELECT COALESCE(MAX(pe.created_at), '') FROM publication_embeddings pe
         JOIN papers p ON p.id = pe.paper_id WHERE {standalone_paper_sql("p")}),
      (SELECT COUNT(*) FROM publication_references pr
         JOIN papers p ON p.id = pr.paper_id WHERE {standalone_paper_sql("p")})
"""

# Author network. The graph's structure is derived from the publication graph —
# co-authorship (publication_authors) + bibliographic coupling (publication_references)
# + the author-embedding semantic layer — and restyled by follow state. I-4: the
# old fingerprint tracked only paper count/watermark + follows, so a re-attribution
# of authorships, a reference backfill, or a re-embed (none of which need touch
# papers.updated_at) served a stale network. Added: authorship count, reference
# count, and the embedding-recompute watermark.
_AUTHOR_NETWORK_LIBRARY_FP_SQL = f"""
    SELECT
      (SELECT COUNT(*) FROM papers p WHERE p.status = 'library' AND {standalone_paper_sql("p")}),
      (SELECT COALESCE(MAX(p.updated_at), '') FROM papers p WHERE p.status = 'library' AND {standalone_paper_sql("p")}),
      (SELECT COUNT(*) FROM followed_authors),
      (SELECT COALESCE(MAX(followed_at), '') FROM followed_authors),
      (SELECT COUNT(*) FROM publication_authors pa
         JOIN papers p ON p.id = pa.paper_id WHERE p.status = 'library' AND {standalone_paper_sql("p")}),
      (SELECT COUNT(*) FROM publication_references pr
         JOIN papers p ON p.id = pr.paper_id WHERE p.status = 'library' AND {standalone_paper_sql("p")}),
      (SELECT COALESCE(MAX(pe.created_at), '') FROM publication_embeddings pe
         JOIN papers p ON p.id = pe.paper_id WHERE p.status = 'library' AND {standalone_paper_sql("p")})
"""

_AUTHOR_NETWORK_CORPUS_FP_SQL = f"""
    SELECT
      (SELECT COUNT(*) FROM papers p WHERE {standalone_paper_sql("p")}),
      (SELECT COALESCE(MAX(p.updated_at), '') FROM papers p WHERE {standalone_paper_sql("p")}),
      (SELECT COUNT(*) FROM followed_authors),
      (SELECT COALESCE(MAX(followed_at), '') FROM followed_authors),
      (SELECT COUNT(*) FROM publication_authors pa
         JOIN papers p ON p.id = pa.paper_id WHERE {standalone_paper_sql("p")}),
      (SELECT COUNT(*) FROM publication_references pr
         JOIN papers p ON p.id = pr.paper_id WHERE {standalone_paper_sql("p")}),
      (SELECT COALESCE(MAX(pe.created_at), '') FROM publication_embeddings pe
         JOIN papers p ON p.id = pe.paper_id WHERE {standalone_paper_sql("p")})
"""

# I-4: stamp the clustering/projection/labelling versions into the paper-map +
# author-network fingerprints so a CHANGE to the ML (e.g. the I-5 eom/no-forced-K
# clustering fix) invalidates the cached layout — input data alone can't, so a
# corrected algorithm would otherwise keep serving the old manufactured clusters.
_GRAPH_ML_VERSIONS = (CLUSTERING_ALGO_VERSION, PROJECTION_ALGO_VERSION, LABELLING_VERSION)
mv.register(
    mv.View(
        key="graph:paper_map:library",
        fingerprint_sql=with_version(_PAPER_MAP_LIBRARY_FP_SQL, *_GRAPH_ML_VERSIONS),
        build_fn=lambda conn: _build_paper_map_payload(conn, scope="library"),
        operation_key="materialize.graph.paper_map.library",
        isolate_build=True,
    )
)
mv.register(
    mv.View(
        key="graph:paper_map:corpus",
        fingerprint_sql=with_version(_PAPER_MAP_CORPUS_FP_SQL, *_GRAPH_ML_VERSIONS),
        build_fn=lambda conn: _build_paper_map_payload(conn, scope="corpus"),
        operation_key="materialize.graph.paper_map.corpus",
        isolate_build=True,
    )
)
mv.register(
    mv.View(
        key="graph:author_network:library",
        fingerprint_sql=with_version(
            _AUTHOR_NETWORK_LIBRARY_FP_SQL,
            *_GRAPH_ML_VERSIONS,
            _AUTHOR_NETWORK_LAYOUT_VERSION,
        ),
        build_fn=lambda conn: _build_author_network_payload(conn, scope="library"),
        operation_key="materialize.graph.author_network.library",
        isolate_build=True,
    )
)
mv.register(
    mv.View(
        key="graph:author_network:corpus",
        fingerprint_sql=with_version(
            _AUTHOR_NETWORK_CORPUS_FP_SQL,
            *_GRAPH_ML_VERSIONS,
            _AUTHOR_NETWORK_LAYOUT_VERSION,
        ),
        build_fn=lambda conn: _build_author_network_payload(conn, scope="corpus"),
        operation_key="materialize.graph.author_network.corpus",
        isolate_build=True,
    )
)
