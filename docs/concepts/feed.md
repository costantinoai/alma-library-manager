---
title: Feed
description: A chronological inbox of new papers, populated deterministically by the monitors you've configured.
---

# Feed

The Feed is your **chronological inbox**. It surfaces papers you
haven't seen yet from sources you've explicitly told ALMa to watch.
Unlike Discovery (which is probabilistic and ranks by relevance),
the Feed is deterministic and orders strictly by time.

![Feed page with the empty-state when no monitors have produced items yet](../screenshots/desktop-feed.png)

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

### Monitor health

The Feed status strip surfaces a **"{N} degraded"** count when one or
more monitors are unhealthy. Hovering it opens a tooltip that lists
each degraded monitor by label, with its `health_reason` (or
`last_error`) when available — capped at 8 entries, with a
"+N more — see Settings" line when there are more. The values come from
the monitor's `health` / `health_reason` / `last_error` fields, so the
strip is a quick read on which sources are failing without leaving the
Feed.

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

## New markers

The **New** marker is tied to the latest completed Feed refresh, not
to whether this browser session has rendered a row. The check is
**per-paper**, not per-row — a paper credited to multiple followed
authors has multiple `feed_items` rows, each with its own
`fetched_at`. A paper is marked new only when:

* at least one of its rows still has `status = 'new'`, AND
* the **earliest** `fetched_at` across all of its rows falls inside
  the latest completed refresh window.

So a paper that was first surfaced in a previous fetch under author
A and re-surfaced this fetch under author B is **not** new — the
user has already seen it. Older untriaged rows still appear in the
Feed; they're just not badged. The sidebar bubble counts distinct
papers (not rows) using the same per-paper rule, so it tracks the
real "new this fetch" count.

## Actions on a Feed item

Each card shows the paper's metadata alongside a one-line **TL;DR**
(`tldr`) and an **influential-citation count**
(`influential_citation_count`) when those are available — both are now
carried on the wire by the feed list query, so the card reflects the
same enriched content as the rest of the app.

Each card carries the standard rating vocabulary:

| Action | What it does |
|---|---|
| **Save** | Transitions the paper to `library`, default rating 3. |
| **Like** | Saves with rating 4. |
| **Love** | Saves with rating 5. |
| **Dislike** | Down-weights the paper — records a negative signal and stamps rating 1. The paper **stays visible** in the Feed; chronological truth is preserved. |
| **Dismiss** | **Hides the paper from the Feed for good** — settles every `feed_items` row for the paper to `status = 'dismissed'`, which the list query excludes permanently. Sends a small negative signal (no rating stamp), and offers an **undo** right after. |
| **Queue** (reading status select) | Adds the paper to the Reading list. Independent of saving. |

The Dislike-vs-Dismiss split is the core nuance (D6). **Dislike** is the
soft verb: it lowers the paper's standing without removing it, so the
inbox keeps its complete chronological record. **Dismiss** is the one
"forever" verb in the Feed: it hides the paper from the inbox for good —
but because that's a heavy action, it always carries a transient **Undo**
that restores the rows to `new` and drops the negative signal. Both
actions also feed Discovery (Dislike and Dismiss both down-weight what
the recommender shows).

Dismiss applies per card and in bulk: the per-card control and the
selection bar's **Dismiss** button both settle the chosen items to
`dismissed`.

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

A refresh runs in the background job pool — the triggering `POST`
returns immediately — and while it's running, an in-page
**RefreshRunningBanner** shows on the Feed so you know a lens/feed
refresh is in flight without watching the Activity panel.

Near the top of the Feed, a collapsed **ConceptCallout** explains the
action contract in one place: Add / Like / Love save the paper (Love
rates it 5★), Dislike down-weights it but keeps it visible, and Dismiss
hides it for good (with undo). This keeps the Dislike-vs-Dismiss split
discoverable without per-button tooltips.

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
* It is **not** infinite. The bound is a **60-day time window** by
  publication date — not a 60-item cap. The inbox keeps it scannable;
  to see items older than 60 days, use Library (saved) or Discovery
  (candidates).

The inbox isn't hard-capped at one page either. It loads the first
**60 items** and offers a **"Load more · N of TOTAL"** button that
grows the list a page (60) at a time, all within the 60-day window —
`TOTAL` is the full count the backend reports for that window. The
button is hidden when an author filter is active, since that view
already shows the complete filtered set.
