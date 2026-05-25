---
title: Health
description: Is my data healthy, and what do I do about it — corpus-data dimensions with per-paper drilldowns, the maintenance operations + opt-in healer that fix them, and the operational status of the running system.
---

# Health

The **Health** page answers one question: *is my data healthy, and what do
I do about it?* It reads a single canonical health layer, so every number
agrees with the rest of the app. A thin **vitals ribbon** (a stacked bar of
every dimension by severity) over a scoreboard (critical / warnings / embedding
coverage / total papers) sits on top for at-a-glance triage, then two tabs:

## Data

*Your corpus's data* — completeness, per paper, fixable.

Each **dimension** is one measurable gap: unresolved identity, missing
abstract / references / topics / authors / DOI / date / URL, embedding
coverage, fetchable S2 vectors, locally-computable embeddings, and the retry
ledger. Every card states the **problem**, **why it matters**, the **metric**,
a **severity** badge, **when it was last fixed**, and the **actions** that
repair it. Healthy dimensions collapse into a single "all clear" row.

### Drilldown — which papers, and act on them

Click any dimension card to open a **centered modal** listing the affected
papers. Select rows to **Fix N** (a targeted maintenance run for exactly those
papers) or **Remove N** (soft-remove from Library); per row you can **add an
abstract** / **edit authors** inline, remove, or open the paper at its source.
The header carries the bulk "fix the whole dimension" action. (Edit/remove
apply to Library papers; tracked-but-unsaved papers are shown read-only.)

### Maintenance operations

Below the dimensions: the bounded background jobs that do the fixing —
corpus-metadata rehydration, S2-vector fetch, local embedding compute, identity
resolution. Each shows what it repairs, how many papers are pending, when it
last ran (and last *succeeded*), plus **Run now**, an **Auto-repair** toggle
(opt-in, off by default), and a **Daily cap**. A *cost* tag flags *local* /
*network* / *compute*.

The **idle healer** repairs the corpus in the background — but only the tasks
you opt in, never beyond their daily cap, one task per tick, worst-severity
first. It is off by default end to end; `ALMA_DISABLE_IDLE_MAINTENANCE=1` is a
global kill switch (see [Background jobs](../operations/background-jobs.md)).

## Status

*Operational health of the running system* — what's degraded, failing, or
needs a fix right now: feed monitors, upstream sources, plugins, background-job
failures, with one-click remediation. This is the actionable health view; it
deliberately holds no charts.

!!! note "Trends live in Insights"
    The subsystem *analytics* — how feed / discovery / alerts / authors perform
    over time — are **not** here. They moved to **[Insights → Activity](insights.md)**.
    Health is "what's wrong + fix it"; Insights is "understand the data."

## How fresh is this?

Everything is served from the fingerprint-keyed materialised-view cache (see
[Performance](../operations/performance.md)): a GET returns the last snapshot in
<10 ms and rebuilds in the background when the data changes. The header shows
when the snapshot was last assessed.

## API

```
GET  /api/v1/insights/health                # canonical dimensions + totals
GET  /api/v1/health/operations              # maintenance tasks + config + last run
POST /api/v1/health/operations/{key}/run    # run a task now (optional {target_paper_ids})
POST /api/v1/health/operations/{key}/config # { enabled?, daily_cap? }
GET  /api/v1/health/dimensions/{key}/items   # affected papers (drilldown, paginated)
```
