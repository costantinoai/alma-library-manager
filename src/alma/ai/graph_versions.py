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

    from alma.ai.graph_versions import with_version, SUPER_REGION_VERSION
    mv.register(mv.View(
        key="semantic:regions",
        fingerprint_sql=with_version(partition_fingerprint_sql(), SUPER_REGION_VERSION),
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

# Semantic partition (application/semantic_partition.py, task 67 C2): the
# core-owned membership + state tables and the incremental assignment rule.
# Stamped as `algorithm_version` on every published generation.
# 2026.09-1: initial — legacy layout import + nearest-centroid assignment.
PARTITION_VERSION = "2026.09-1"

# Super-region aggregation (application/super_regions.py, task 54): how the
# substrate's clusters are agglomerated into the ~32 regions the Signal Lab
# samples from, the adjacency rule, and the identity-carrying remap. Bump on
# any change to grouping/adjacency/remap logic so the cached
# `graph:super_regions` payload rebuilds — its data fingerprint (cluster rows)
# can't see code fixes.
# 2026.07-1: initial — average-link cosine agglomeration to ≤32 regions,
#            mutual-kNN(4) adjacency, cosine≥0.9 greedy identity remap.
# 2026.09-1: coordinate-free — centroids come from the core semantic partition
#            (membership + vectors only); the payload no longer carries x/y,
#            which nothing read. Identities carry through the cosine remap.
# 2026.09-2: regions read the core partition tables (`semantic:regions` key);
#            masses, labels and rings come from memberships, never layouts.
SUPER_REGION_VERSION = "2026.09-2"

# Signal Lab model fit (application/signal_lab/fit.py, task 54): the pure
# rounds→model recompute — head formulas, shrinkage, holdout metrics, γ gate.
# Bump on any fit-logic change; the rounds themselves are the data half of the
# fingerprint. POLICY version covers the round-generation side (sampling
# weights, ε, BALD scoring) — stamped on each round row AND in the model
# fingerprint, since a policy change alters what future rounds mean.
# 2026.07-1: initial (M0 harness; no heads promoted).
# 2026.07-2: persist the game-only utility delta separately from its Library
#            prior, so ranking and terrain do not count the prior twice.
# 2026.07-3: deduplicate accidental repeated query sets; fit the diagonal
#            metric ensemble used by full-outcome odd-one-out acquisition.
# 2026.07-4: the author head (`author_offsets`). MUST bump: the payload gained
#            a key, and the view's fingerprint keys only on round count/max-id,
#            so without this every existing install keeps a model with no
#            `author_offsets` until the next answered round — the head would be
#            fitted and then read by nobody.
# 2026.07-5: the venue head (`venue_offsets`), fitted from matched-pair rounds
#            only. MUST bump for the same reason as -4: the payload gained a
#            key, and the fingerprint keys only on round count/max-id, so
#            otherwise an install that already has a model would keep one with
#            no `venue_offsets` until the next answered round.
# 2026.09-1: task 67 B2 — the view's fingerprint now covers EVERY input the fit
#            reads (round content, active embedding model, shown vectors /
#            clusters / author+venue metadata, the super-region payload, the
#            Library prior set and the `signal_lab.*` tuning knobs), not just
#            round count/max-id, and the prior's row selection became
#            deterministic (`ORDER BY` before `LIMIT`). MUST bump: the stored
#            fingerprint is in the old shape, so without it an install keeps
#            serving the model it has until the next qualifying round, which is
#            precisely the staleness this fixes.
# 2026.09-2: prior = the feedback profile's Rocchio direction (one owner with
#            Discovery); payload publishes `region_evidence` (wins/votes).
# 2026.09-3: fingerprint active vector content and canonical prior membership;
#            aggregate counts/ratings missed exchanged preferences and vectors.
SIGNAL_LAB_FIT_VERSION = "2026.09-3"
# Signal Lab eval (application/signal_lab/eval.py): the replay that reports
# what the fitted heads DO to a live deck. Separate from the fit version because
# a changed probe must rebuild the eval view without refitting the model.
# 2026.09-1: task 67 C1 — replaced the private 2.5-point what-if bonus (which
#            ignored the runtime evidence damper and the real settings) with a
#            replay of each lens's latest immutable ranking snapshots through
#            the canonical ranker, at the actual settings; missing or pre-v4
#            snapshots report as insufficient evidence instead of guessed inputs.
# 2026.09-2: disabled Lab has zero active points in the runtime replay too.
SIGNAL_LAB_EVAL_VERSION = "2026.09-2"
# v2: full-pool candidates, full-outcome EIG, true staleness, recent-query
# cooldown, posterior edge priority, and deck-conditioned diversity.
# 3: valence-aware allocation — regions by sign uncertainty of their fitted
#    votes, boundary edges by the probability they separate liked from disliked.
# 4: incompatible retained posterior dimensions use cold-start acquisition.
SIGNAL_LAB_POLICY_VERSION = 4

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
# 2026.07-7: Analytics timeline + topics (task 47 Phase 4). `publications_by_year`
#            rows gain median_citations / seminal_count / top_paper_* so the chart
#            can default to the outlier-robust centre and name each year's most
#            cited work; the payload gains `cluster_topics` — the library's OWN
#            c-TF-IDF cluster labels, which replace the OpenAlex taxonomy as the
#            Overview's "topics". New payload fields → cached overview rebuilds.
# 2026.07-8: task 50 M1 — `cluster_topics` now reads the ONE corpus substrate
#            (scope='corpus', filtered to library rows) instead of the deleted
#            library-scope layout; the vocabulary becomes the corpus cluster
#            labels (same words as the map). Cached overview must rebuild.
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
