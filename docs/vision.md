---
title: Vision & philosophy
description: Why ALMa exists, the three pillars it's built on (Monitor, Discovery, Library), the design principles it tries to keep, and the lifecycle model that holds the whole UI together.
---

# Vision & philosophy

## Why ALMa exists

Staying current with the literature is really three jobs. Keeping
the papers that matter. Watching for new work from people and
topics that are relevant. Finding adjacent papers you don't yet
know exist. Those jobs usually live in separate tools, and the
separation creates most of the friction.

Citation managers (Zotero, Mendeley, EndNote, Paperpile) are good
at the first job. They hold a collection and format a
bibliography. They don't surface anything new, and they have no
opinion about which paper in a "to read" pile is closest to the
rest of the collection.

Discovery tools (Connected Papers, Semantic Scholar, Inciteful,
Litmaps) are good at the third job, but only one seed paper at a
time. They show the neighbourhood of a starting point. They don't
see the rest of the library, don't remember you liked
last week, and don't track an author or a topic over months. Each
visit starts fresh.

The second job, watching, is the worst served of the three.
Journal alerts are noisy and per-source. Per-author and
per-keyword alerts have to be configured one at a time across
different services, and the results land scattered in an inbox,
disconnected from the rest of the work.

The deeper issue is that the three jobs need each other. A
recommender ranks better when it knows which papers were kept and
which were dismissed. A watcher is more useful when the system
can up-weight authors that keep showing up in saved papers.
Curation is less work when new candidates arrive pre-filtered by
something that has been watching the user's actual choices. With
the jobs spread across three tools, each one is blind to the
other two.

ALMa puts all three on one database so they can inform each
other.

* **Monitor.** Follow specific authors and topics. ALMa watches
  OpenAlex, Crossref, arXiv, bioRxiv, and Semantic Scholar in the
  background and surfaces new work in a single chronological
  feed.
* **Discovery.** A recommender that ranks papers against the
  actual taste signal in the library. Every save, like, love,
  dismiss, and remove is a labelled training point.
  Recommendations are pushed across the *branches* of the library
  so the results don't collapse onto whichever topic was fed in
  most recently.
* **Library.** The curated collection that anchors the other two.
  Notes, ratings, reading state, organisation. The same actions
  that organise it also train the recommender, with no extra
  effort.

The library trains discovery. The monitors fill the inbox.
Discovery suggests what the monitors should next subscribe to.
Each of the three sharpens the other two.

## A personal project

ALMa started because I wanted one place that did all three jobs
for my own work. Find new papers near my interests, follow the
people and topics I care about, hold my reading list, and tell me
when something worth my time appears, without me babysitting five
tabs across three different services.

Building it for one user makes a lot of choices straightforward
that are hard at multi-tenant scale. The recommender can train on
one person's signals. The embedding space can be shaped around
one person's corpus. The insights can be about one person's gaps.
Schema decisions don't need to defend against migration paths for
thousands of accounts.

What's underneath is mostly off-the-shelf:

* Scientific embeddings (SPECTER2) for semantic similarity, the
  same vector space Semantic Scholar publishes, available locally
  or via the API.
* Co-citation and citation-neighbour graphs for graph-structural
  recommendations: papers that the papers in the library tend to
  cite.
* Author and topic monitors over OpenAlex, Crossref, arXiv,
  bioRxiv, and Semantic Scholar, joined into one feed.
* A branch decomposition of the library, a stable interest tree
  learned from saved papers, so discovery can push across all of
  the active research directions instead of collapsing onto
  whichever direction was fed in most recently.
* Library insights: clusters of what was actually read, the
  source mix, the temporal shape of the collection, gaps where
  neighbouring work isn't represented.
* A ranker tuned by save / like / love / dismiss / remove. The
  same actions that organise the library are the ones that train
  discovery; the controls that drive it (lens weights, source
  mix, recency tilt) are exposed rather than hidden.

None of these techniques are individually new.

## Self-hosted, single user

ALMa runs on the user's own machine (laptop, home server,
whatever's available) as a long-running backend with a web UI in
the browser and background jobs that fetch new papers between
sessions. The code is open source under CC BY-NC 4.0; the data
lives in one SQLite file (`data/scholar.db`) on disk.

There is no auth, no accounts, no multi-tenant database, no
telemetry. Settings are one JSON blob. Heavy work (embeddings,
clustering, label generation) runs locally or against API keys
the user has configured. The only outbound calls are to public
scholarly APIs (OpenAlex, Crossref, arXiv, bioRxiv, Semantic
Scholar) and, optionally, an LLM provider the user has set up.

Imports come from Zotero, Mendeley, RIS, and BibTeX, and exports
go back out the same way. The library is portable in both
directions.

## The lifecycle model

## Design principles

These are the rules that recur across the codebase. Most are
recorded as engineering invariants rather than aspirations.

### One intent per action

Every user action in the UI maps to exactly one canonical backend
use-case. Two buttons that mean the same thing eventually drift
apart, so ALMa removes the duplicate rather than maintaining two
paths.

### Observable system

Every meaningful operation surfaces in the **Activity** panel:
running jobs, completed jobs, terminal states with messages,
per-source timing. If something happened, you can see that it
happened, why, and how long it took.

### Truthful UI

A label or toggle in the UI represents the actual behaviour of
the underlying call. If a setting is not yet wired up, it does
not appear. If a feature requires an API key you haven't
configured, it is hidden or disabled with an honest reason. No
silent fallbacks.

### AI is opt-in

ALMa works without any AI provider configured. Discovery still
ranks, clustering falls back to non-AI strategies,
recommendations still flow. AI features (semantic similarity,
cluster labels, auto-tags, LLM query planning) light up only when
the user has explicitly enabled and configured the underlying
provider. No half-working fallbacks that quietly degrade quality.

### No silent failures

Failures are loud, either blocking with a clear error, or logged
prominently with the chosen fallback explained. The
[Activity panel](concepts/insights.md#activity-panel) and
`/api/v1/logs` are the two surfaces that keep this honest.

## In practice

The whole backend is one FastAPI app with about 25 route modules;
the database is one file; the frontend is one Vite SPA.

Because the same database holds the monitors, the library, and
the feedback events, every part can read every other one without
a network hop or a schema bridge. Discovery scoring sees the
saved papers. Insights see the reading state. Monitors see what's
already been imported and don't re-surface it. Everything sits on
the same database, so there's no integration layer to maintain.

The cost is that everything runs at personal scale. ALMa is a personal tool that
gets sharper the more it's used.
