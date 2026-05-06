---
title: Insights
description: Charts, geography, topics, journals, authors, and a clustered SPECTER2 graph of your Library — read-only analytics, never curation.
---

# Insights

The **Insights** page projects your data into charts, maps, and a
2D embedding graph. Read-only — Insights is for understanding your
corpus, not editing it.

![Insights page with the Stats / Diagnostics tabs](../screenshots/desktop-insights.png)

## Tabs

### Overview

The default tab. Aggregated metrics:

* **Summary** — total papers, total followed authors, total
  collections, total tags.
* **Publications by year** — bar chart of saved-papers volume.
* **Top topics** — most-saved topics, with counts.
* **Top journals** — venue mix.
* **Top institutions** — institutional geography of your saved
  authors.
* **Authors rail** — the most-published / most-cited authors in your
  Library, with paper counts and h-index.
* **Recommendations engagement** — Discovery-side stats: total
  recs surfaced, seen, liked (positive action), dismissed, plus
  engagement rate.
* **Library** — total saved, average rating, total collections,
  total followed authors.

All Overview blocks are **Library-scoped** — they reflect the saved
corpus, not the entire tracked set.

### Graph

A 2D projection of your Library's SPECTER2 vectors. Requires that
embeddings have been computed (either pulled from Semantic Scholar
or generated locally). When no embeddings are available the page
falls back to a principled text-TF-IDF clustering on title +
abstract; it never clusters on `publication_topics` (OpenAlex's
coarse topic vocabulary), journal, or author names.

#### Pipeline (BERTopic recipe)

```
SPECTER2 vectors (768-d)
    │
    ├─▶ L2-normalise rows (cosine geometry — what SPECTER2 was trained for)
    │
    ├─▶ UMAP n_components=5  (cosine, n_neighbors=15)   ── clustering substrate
    │       │
    │       └─▶ HDBSCAN(metric='euclidean', leaf)       ── density clusters
    │
    └─▶ UMAP n_components=2  (cosine)                   ── 2-d display layout
```

L2-normalising puts every vector on the unit sphere, so euclidean on
the reduced space is rank-equivalent to cosine in the original 768-d
space — letting HDBSCAN/UMAP/kmeans use their fast euclidean code
paths without leaving the geometry SPECTER2 was trained for.
UMAP-reducing to 5-d before HDBSCAN solves the curse of
dimensionality: density estimates are unreliable in 768-d at our
scale (50–500 papers) but tractable at 5-d. Both the clustering
substrate and the display layout read the same L2-normalised input
through cosine UMAP, so visual proximity and cluster boundaries
agree by construction — neighbouring papers in the layout are also
in the same cluster.

#### Behaviour

* **Auto-k clustering** — HDBSCAN with `cluster_selection_method='leaf'`
  picks the cluster count automatically; no fiddling with `k`.
  `min_cluster_size = max(3, min(12, ⌈√n × 0.5⌉))` so a 50-paper
  library produces 5–8 well-balanced clusters and a 300-paper library
  produces 15–25.
* **Distinctive cluster labels** — class-based TF-IDF (the BERTopic
  c-TF-IDF formula) over (1, 2)-grams of each cluster's member titles
  + abstracts. An English + academic-domain stop-list (`study`,
  `method`, `result`, …) is removed before scoring, and a bigram
  absorbs its constituent unigrams in the final phrase so labels read
  as topics (`"visual cortex, object recognition"`) rather than
  bag-of-keywords. Labels persist in `graph_cluster_labels` keyed by
  the cluster's member-set signature; the **Refresh cluster labels**
  job recomputes them in the background and pushes the result
  through the same materialised-view layer.
* **Hover detail** — paper title, year, journal, rating.

Graph data is cached server-side via the materialised-view layer
(fingerprint-keyed, see [Performance](../operations/performance.md)).
Re-clustering is opt-in via Settings → Operational status →
**Rebuild graphs**.

#### Fallbacks

* **UMAP unavailable / N < 15** → cluster on the L2-normalised raw
  vectors with HDBSCAN. Same geometry, just no dimensionality
  reduction.
* **HDBSCAN unavailable** → silhouette-driven `MiniBatchKMeans`
  with `k ∈ [2, 30]` on the reduced space.
* **HDBSCAN collapses to ≤ 3 clusters on N ≥ 18** → same kmeans
  rescue so the paper map is never reduced to a few mega-clusters.
* **No embeddings at all** → text-TF-IDF clustering on title +
  abstract. Never `publication_topics`, never journal/authors as
  topical features. Falls back to an unclustered grid when text is
  too sparse.

### Reports

Time-window summaries:

* **Weekly brief** — what was added, what shifted, what surfaced.
* **Collection intelligence** — growth, coverage, and density by collection.
* **Topic drift** — how topic mix changes over time.
* **Signal impact** — which ranking signals correlate with useful outcomes.

### Diagnostics

The honest underbelly:

* **Feed health** — per-monitor status (healthy / degraded /
  failing), last refresh timestamp, yield rate.
* **Discovery branch quality** — per-branch engagement stats so
  you can see which branches are producing useful recs and which
  are noise.
* **Embedding coverage** — how much of your Library has SPECTER2
  vectors.
* **Resolution status** — how many imports / authors are still
  unresolved.

## How fresh is what I'm seeing?

The Insights page and the three graphs (Paper Map, Author Network,
Topic Map) are served from a fingerprint-keyed cache: each GET
returns the previously-computed payload in <10 ms as long as nothing
the view depends on has changed. When you save / edit / unfollow /
import, the next page load detects the change automatically — the
displayed values are the *previous* snapshot for a few seconds while
the cache rebuilds in the background, then the page silently swaps
to the fresh values when the background job completes. The
**Refreshing…** pill in the header lights up whenever any tab is in
that swap window.

The **Diagnostics** tab is split into eight separately-cached
sections: `feed`, `discovery`, `ai`, `authors`, `alerts`,
`feedback`, `operational`, `evaluation`. Each card is fed by exactly
one section and renders as soon as that section's response lands —
fast sections (`ai`, `alerts`, `feedback`) typically come back in a
few hundred milliseconds even on a cold first visit, slower sections
(`authors`, with its citation-neighbour suggestion projection) keep
their card in skeleton until ready. After the first build every
cache-hit section returns ~1 ms, and individual sections only
rebuild when their own inputs change — saving a paper invalidates
authors + evaluation, not feed or discovery.

You don't usually need to do anything. If you want to force a fresh
graph layout (full re-clustering and re-projection — the layout may
shift), the **Rebuild graphs** button under Settings → Operational
status triggers it explicitly.

## Activity panel

Not strictly part of Insights, but always docked at the bottom of
the screen on every page:

* **Operations tab** — running and completed background jobs with
  progress, per-source timing, and a Cancel button on long-running
  jobs.
* **Logs tab** — real-time application logs filtered by level
  (`ERROR / WARNING / INFO / DEBUG`).

The Activity panel is where the [observable system](../vision.md#observable-system)
principle actually lives. Every meaningful operation has a job
envelope here; if it doesn't, that's a bug.

## API

```
GET /api/v1/insights                                      # full overview
GET /api/v1/insights/diagnostics                          # composed payload (8 sections)
GET /api/v1/insights/diagnostics/sections/{section}       # one section, cached independently
                                                          # section ∈ {feed, discovery, ai,
                                                          #   authors, alerts, feedback,
                                                          #   operational, evaluation}
GET /api/v1/insights/discovery/branch-action
GET /api/v1/graphs/library
GET /api/v1/reports/weekly-brief
GET /api/v1/reports/collection-intelligence
GET /api/v1/reports/topic-drift
GET /api/v1/reports/signal-impact
```
