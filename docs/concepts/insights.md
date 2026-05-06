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
embeddings have been computed (either pulled from S2 or generated
locally).

* **Auto-k clustering** — HDBSCAN with silhouette-sweep tuning
  picks the cluster count automatically; no fiddling with `k`.
* **Representative cluster labels** — short labels built from the
  strongest titles / abstracts in each cluster.
* **Word-cloud overlay** — per-cluster keyword cloud, derived from
  member-paper titles / abstracts.
* **Hover detail** — paper title, year, journal, rating.

Graph data is cached server-side. Re-clustering is opt-in (it costs
real time on a large corpus).

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
