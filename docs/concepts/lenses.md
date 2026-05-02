---
title: Lenses
description: Lenses are per-context recommendation pipelines. The whole Library, one collection, a topic keyword, or a single tag — each gets its own ranked suggestions.
---

# Lenses

A **lens** is a saved configuration of *what* to recommend papers
*for*. The default lens treats your entire Library as the seed —
"find papers like the things I've saved overall". Other lenses
narrow the scope.

## Lens contexts

Each lens has one of four context types:

| Context | Seed for the recommender |
|---|---|
| `library_global` | All papers with `status='library'`. The default. |
| `collection` | Only papers in a chosen [Collection](library.md#collections). |
| `topic_keyword` | Saved papers tagged with a specific topic. |
| `tag` | Saved papers carrying a specific [Tag](library.md#tags). |

You define lenses from the Discovery page header. Each lens persists
its own:

* Weights (override the global Discovery weight defaults)
* Branch controls (pinned / muted / boosted topics)
* Last-refresh timestamp + recommendation count
* Signal counts (positive / negative feedback collected on the lens)

## Why lenses exist

A single global recommender produces one ranking for "papers like
my Library". That's useful, but it averages across all your
research interests. Lenses let you ask narrower questions:

* *"What's new in chapter 2's territory?"* — collection lens scoped
  to the "Thesis chapter 2" collection.
* *"What's adjacent to my methods papers?"* — tag lens scoped to
  the `method` tag.
* *"What's happening in transformer interpretability specifically?"*
  — topic-keyword lens scoped to that topic.

Each lens runs the same retrieval + ranking pipeline as the global
lens — it just seeds it with a smaller, more focused set of papers,
which yields a sharper top-N.

## Refresh model

Each lens carries its own refresh state. Refreshing one lens does
not refresh others — they're independent pipelines and independent
caches.

* **Cache** — recommendations from the last refresh are cached
  per-lens, so re-opening Discovery is instant.
* **Manual refresh** — the Discovery header has a Refresh button per
  lens. Triggers an Activity-backed job.
* **Background refresh** — when scheduled, runs in the APScheduler
  loop on a configurable cadence.

## Branches inside a lens

Each lens's candidates are clustered into [Branches](discovery.md#branches).
Branch labels, modes (`core` / `explore`), and tuning hints belong
to the lens — re-clustering happens at refresh time, not at view
time.

You can pin a branch (always include it), mute one (drop it
entirely), or boost one (give it a higher rank). Those controls are
saved per-lens and feed into the next refresh. They survive across
seed-set drift via lineage matching: when K-means reshuffles a
branch by even one paper, the new cluster gets a new `branch_id`
but inherits any pin / mute / boost from a past branch whose seed
set overlaps ≥ 70 %.

The system also intervenes on branches automatically:

* When a branch's `auto_weight` drops below ~0.65, its
  `core_topics` and `explore_topics` are *swapped* for the next
  refresh — the system probes the explore-angle while the core
  angle has been accumulating dismisses. Self-correcting on the
  refresh after.
* When `auto_weight` drops below ~0.55, the branch is auto-muted
  (zero external lane budget). Cluster seeds still influence
  ranking through their centroid + author / topic affinities — the
  system just stops asking external APIs for more like it.

User-set pin / boost takes precedence over both. See
[Discovery → Branches → Auto lifecycle](discovery.md#auto-lifecycle-rotate-then-auto-mute)
for the full state machine.

## Signals on a lens

When you act on a recommendation (Save / Dismiss / Like…), the
event is recorded against `feedback_events` with the lens id stamped
into `context_json`. Two consequences:

1. **Lens-scoped recall** — Discovery queries can show "all positive
   signals on this lens" or "all dismissals on this lens" via the
   lens signal counters.
2. **Lens-scoped ranking** — future refreshes weight feedback within
   the same lens more heavily than feedback elsewhere. Dismissing a
   topic in one lens doesn't permanently mute it everywhere.

## API

```
GET    /api/v1/lenses              # list lenses
POST   /api/v1/lenses              # create
PUT    /api/v1/lenses/{id}         # update weights / branch controls
DELETE /api/v1/lenses/{id}         # delete

POST   /api/v1/discovery/lenses/{id}/refresh   # Activity-backed refresh
GET    /api/v1/discovery/recommendations?lens_id={id}
```

See [REST API](../reference/api.md) for the full set.
