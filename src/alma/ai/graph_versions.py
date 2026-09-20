"""Algorithm/logic version stamps for semantic, learning & Insights materialized views.

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

    from alma.ai.graph_versions import with_version, LABELLING_VERSION
    mv.register(mv.View(
        key="graph:cluster_labels",
        fingerprint_sql=with_version(cluster_fingerprint_sql(), LABELLING_VERSION),
        ...
    ))
"""

from __future__ import annotations

# ── Version constants — bump the relevant one when its family's logic changes ──

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

INSIGHTS_LOGIC_VERSION = "2026.07-8"


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
