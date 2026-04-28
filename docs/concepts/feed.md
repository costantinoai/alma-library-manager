---
title: Feed
description: A chronological inbox of new papers, populated deterministically by the monitors you've configured.
---

# Feed

The Feed is your **chronological inbox**. It surfaces papers you
haven't seen yet from sources you've explicitly told ALMa to watch.
Unlike Discovery (which is probabilistic and ranks by relevance),
the Feed is deterministic and orders strictly by time.

## What populates the Feed

A `feed_items` row is created when one of your **monitors** finds a
paper that's new to ALMa. Three monitor types exist:

| Monitor | Source |
|---|---|
| **Author monitor** | A followed author publishes a new work (OpenAlex). |
| **Topic monitor** | OpenAlex returns a new work matching a topic / concept query. |
| **Query monitor** | A free-text query against OpenAlex search returns a new work. |

Monitors run on a schedule (default: every few hours) via the
APScheduler background loop. You can also trigger them manually from
**Settings → Discovery weights → Feed monitor defaults** or from the
per-author "Refresh now" action.

## Window and ordering

The Feed is bounded to roughly the **last 60 days** by publication
date. Older items aren't deleted — they remain queryable from
Library (if saved), Discovery (as candidates), and the Corpus
Explorer (everything) — but they fall out of the Feed view to keep
the inbox fresh.

When `publication_date` is missing, ALMa falls back to
`fetched_at` (when the monitor first saw the paper) so items still
order correctly. There is no `YYYY-01-01` fabrication for missing
dates.

## Actions on a Feed item

Each card carries the standard rating vocabulary:

| Action | What it does |
|---|---|
| **Save** | Transitions the paper to `library`, default rating 3. |
| **Like** | Saves with rating 4. |
| **Love** | Saves with rating 5. |
| **Dislike** | Records a negative signal (rating 1). The paper **stays visible** in the Feed — chronological truth is preserved. |
| **Queue** (reading status select) | Adds the paper to the Reading list. Independent of saving. |

There is no "Dismiss" in the Feed. Dismiss is a Discovery verb
(hide-from-recommender). The Feed is your inbox; you read it
chronologically and act on individual items.

## Refresh contract

Feed refresh is the heaviest scheduled job in ALMa. It runs each
monitor in parallel, deduplicates results (a single new paper across
two monitors creates one row), and writes a single `feed_items`
batch.

While a refresh is running:

* Other reads (`/api/v1/library/saved`, `/api/v1/feed`,
  `/api/v1/authors`) stay responsive — they don't block on the
  refresh.
* The **Activity panel** shows a job envelope with per-source
  timing.
* If a source fails (e.g. OpenAlex returns 5xx), the failure is
  recorded against that source only; other sources still complete.

## Read endpoints

Both endpoints below are pure reads. They do not write to mirror
tables or sync state.

```
GET /api/v1/feed?limit=&since_days=
GET /api/v1/feed/monitors
```

See the [REST API reference](../reference/api.md) for the full
parameter set.

## What the Feed is not

* It is **not** a recommendation surface. The Feed shows you what
  your monitors found. If you want "papers like the ones I've saved",
  use [Discovery](discovery.md).
* It is **not** a search interface. To search, use the global search
  box (top of the app) or query the Library / Corpus Explorer.
* It is **not** infinite. The 60-day window keeps it scannable. To
  see older items, use Library (saved) or Discovery (candidates).
