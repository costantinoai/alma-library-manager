"""Algorithm/logic version stamps for graph & Insights materialized views.

DRY primitive #2 for the Insights surface (task 04, finding I-4).

Why this exists: a materialized view only rebuilds when its *fingerprint*
changes, and the fingerprint hashes INPUT DATA (row counts, max timestamps).
A pure CODE fix — a corrected formula, a relabelled cluster, a fixed embedding
dimension — leaves the inputs identical, so the stale cached payload keeps
serving and the bug looks unfixed. Caught live 2026-06-22: the I-24 embedding-
dimension fix sat behind a cached diagnostics MV and kept reporting 384.

The fix: stamp a version literal into each view's fingerprint. Bump the
constant for a logic family whenever that family's BUILD logic changes, and
every dependent view's fingerprint shifts → background rebuild on next read.
One version per family so a clustering change doesn't needlessly rebuild the
reports, and vice-versa.

Usage — wrap a view's existing fingerprint SQL at registration time::

    from alma.ai.graph_versions import with_version, CLUSTERING_ALGO_VERSION
    mv.register(mv.View(
        key="graph:paper_map:library",
        fingerprint_sql=with_version(_PAPER_MAP_LIBRARY_FP_SQL,
                                     CLUSTERING_ALGO_VERSION, PROJECTION_ALGO_VERSION),
        ...
    ))
"""

from __future__ import annotations

# ── Version constants — bump the relevant one when its family's logic changes ──

# 2-D projection / node-layout logic (ai/projections.py, the UMAP/static map)
# AND the graph EDGE topology (the rendered paper-map structure).
# 2026.07-2: Phase 3 / I-11 — typed edge layers (semantic mutual-kNN in 768-d,
#            bibliographic coupling, co-authorship) replace intra-cluster cliques;
#            retracted papers excluded from edges.
# 2026.07-3: author network gets the same treatment — typed mutual-kNN/co-author/
#            coupling layers (stats out of edge geometry) + honest eom clustering
#            with retained outliers, replacing topic-TFIDF+stats + silhouette-kmeans.
# 2026.07-4: corpus PERF — bibliographic coupling now uses a Python inverted index
#            with a document-frequency cap that drops hub references cited by
#            >50 papers (372s→<1s on the corpus). Hub-ref couplings (everyone
#            cites the famous review) were non-discriminative noise anyway, so the
#            corpus edge set changes slightly — the cached corpus map must rebuild.
# 2026.07-5: corpus PERF (task #21) — the 2-D projection now runs through the
#            alma.ai.accel dispatch (GPU when present; optimised CPU otherwise) with
#            a bounded n_epochs (200 for the display layout, down from umap's <10k
#            default of 500) and a kNN graph shared with the clustering fit. The
#            shared graph is the same neighbour graph, but the bounded epochs +
#            shared-SGD orientation shift the layout marginally, so the cached
#            corpus map rebuilds once.
# 2026.07-6: co-occurrence DRY (all four coupling/co-authorship layers now go
#            through one alma.ai.cooccurrence primitive). The author co-authorship
#            self-join became an inverted index WITH a mega-consortium df cap
#            (papers with >100 authors no longer couple all their authors), so the
#            author-network edge set changes — the cached author networks rebuild.
#            Paper-map edges are unchanged (co-authorship has no cap; bib coupling
#            logic is identical), but the shared version forces one idempotent
#            paper-map rebuild too.
PROJECTION_ALGO_VERSION = "2026.07-6"

# Clustering algorithm + parameters (ai/clustering.py): HDBSCAN/k-means choice,
# outlier handling, forced-K removal, etc. Bump on any clustering behavior change.
# 2026.07-2: HDBSCAN leaf→eom + removed the forced-K≥4 rescue (I-5).
# 2026.07-3: retain density noise as an explicit Unclustered group instead of
#            force-merging it to the nearest centroid; ClusteringResult carries
#            per-point membership probability + coverage + stability (I-6).
# 2026.07-5: corpus PERF (task #21) — the 5-D clustering substrate now runs
#            through alma.ai.accel with a shared kNN and a bounded n_epochs (300
#            for the substrate; chosen because a shared-kNN SGD needs ~300 epochs
#            to recover the own-kNN/500 coverage — 0.741 on the corpus — whereas
#            200 under-settles it to 0.723). Coverage is preserved; the layout
#            shifts marginally, so the cached clustering rebuilds once.
CLUSTERING_ALGO_VERSION = "2026.07-5"

# Cluster-label generation (ai/clustering.py score_cluster_terms): c-TF-IDF
# term selection + word clouds, and the label-signature content hash.
# 2026.07-2: prevalence-weighted c-TF-IDF — terms must recur across the
#            cluster's papers, not just be frequent in one (fixes non-co-
#            occurring words in labels + word clouds).
# 2026.07-6: noisy OpenAlex/S2 topics removed from the machines — BOTH graphs now
#            label clusters from real title text via the shared embedding_graph
#            pipeline (the author network previously labelled from publication_topics).
# 2026.07-7: I-13 — cluster representatives (label context + the cluster-detail
#            sample papers) are now centroid-nearest + MMR-diverse via
#            clustering.select_representatives, NOT citation/recency rank (which
#            biased labels toward famous members). The cluster-detail payload also
#            gains a cohesion metric + a representative_selection marker; re-key so
#            the cached label + the default graph payload rebuild with the new picks.
# 2026.07-8: task 10 scale — score_cluster_terms can use corpus-background
#            document frequencies for IDF while keeping per-cluster TF/prevalence
#            local; paper-map payloads also carry corpus-navigation metadata.
LABELLING_VERSION = "2026.07-8"

# Insights overview + diagnostics COMPUTATION (insights.py / insights_diagnostics.py):
# any corrected metric formula (papers-per-author, institution grouping, embedding
# dimension, the outcome projection swap, removed obsolete semantics). Bump to
# invalidate the diagnostics/overview MVs cached with the pre-fix math.
# 2026.07-2: float16 embedding dim, papers-per-author, institution grouping.
# 2026.07-3: I-21 canonical outcome projection (engagement no longer reads the
#            always-empty user_action='like') + I-22 removed queued/untriaged.
# 2026.07-4: Phase 4 — I-23 AI card → separate measures (no composite); I-26
#            scorecards carry sample_size + an insufficient_data state (no
#            monitors / no alert runs no longer misgraded) + full-population feed
#            yield; I-25 prescriptive branch/source advice gated behind a sample
#            + Wilson bound.
# 2026.07-5: overview summary gains median_citations_per_paper (the outlier-robust
#            companion to the mean) — new payload field, so the cached overview MV
#            must rebuild to carry it.
# 2026.07-6: Library Workflow scorecard → OBSERVED card (D2/I-22). The graded
#            0–100 "workflow score" turned an unread library red; reading/done are
#            opt-in, not an obligation, so it's now reading-progress measures with
#            no composite grade. Evaluation MV payload shape changed → rebuild.
INSIGHTS_LOGIC_VERSION = "2026.07-6"


def with_version(fingerprint_sql: str, *versions: str) -> str:
    """Append logic-version literal(s) to a fingerprint SELECT.

    Wraps the original fingerprint SQL as a subquery and adds the version
    strings as extra selected columns, so a logic change (not just a data
    change) shifts the hashed fingerprint row and forces a rebuild. Robust to
    the original's shape (with or without FROM/WHERE). No-op-safe: passing no
    versions returns the original unchanged.
    """
    if not versions:
        return fingerprint_sql
    literals = ", ".join(f"'{v}'" for v in versions)
    return f"SELECT *, {literals} AS _logic_version FROM (\n{fingerprint_sql.strip()}\n)"
