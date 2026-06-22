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

# 2-D projection / node-layout logic (ai/projections.py, the UMAP/static map).
PROJECTION_ALGO_VERSION = "2026.07-1"

# Clustering algorithm + parameters (ai/clustering.py): HDBSCAN/k-means choice,
# outlier handling, forced-K removal, etc. Bump on any clustering behavior change.
# 2026.07-2: HDBSCAN leaf→eom + removed the forced-K≥4 rescue (I-5).
# 2026.07-3: retain density noise as an explicit Unclustered group instead of
#            force-merging it to the nearest centroid; ClusteringResult carries
#            per-point membership probability + coverage + stability (I-6).
CLUSTERING_ALGO_VERSION = "2026.07-3"

# Cluster-label generation (ai/cluster_labels.py): representative selection,
# c-TF-IDF terms, the label-signature content hash.
LABELLING_VERSION = "2026.07-1"

# Insights overview + diagnostics COMPUTATION (insights.py / insights_diagnostics.py):
# any corrected metric formula (papers-per-author, institution grouping, embedding
# dimension, the outcome projection swap, removed obsolete semantics). Bumped to
# 2026.07-2 to invalidate the diagnostics/overview MVs cached with the pre-fix math.
INSIGHTS_LOGIC_VERSION = "2026.07-2"


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
