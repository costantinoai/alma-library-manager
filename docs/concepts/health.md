---
title: Health
description: Is my data healthy, and what do I do about it — one scrollable page that diagnoses corpus-data gaps and repairs them, shows the operational status of every system component, and runs an opt-in idle healer. Health owns all repair; Settings is configuration only.
---

# Health

The **Health** page answers one question: *is my data healthy — and what do I
do about it?* It reads a single canonical health layer, so every number agrees
with the rest of the app, and it is **one scrollable page, no tabs**: the status
of something and the operation that repairs it live in the *same* card.

Health owns **all diagnosis and repair**. Settings is configuration only — when
something is fixable, Settings points you here.

From top to bottom:

## Vitals ribbon

A thin **vitals ribbon** — a stacked bar across every data dimension, colored by
severity (critical / warning / info / healthy) — gives the whole corpus's data
health at a glance. It spans **one unified set of dimensions**: corpus/paper
gaps *and* author-identity gaps (the snapshot folds `assess_corpus` and
`assess_authors` together), so author health is counted right alongside paper
health. A legend shows the per-severity counts, and a slim caption carries the
two corpus facts worth a glance: total papers assessed and **embedding
coverage** (with a readiness chip that flips on at ≥80%).

The ribbon is the one place semantic color spans the width — the deliberate
exception to the calm off-white surfaces, because it *is* the triage.

## System status

Sharing the same panel as the ribbon — directly below it — is a **one-line
strip** of clickable **component chips**, one per running-system component:
**Feed monitors**, **Upstream sources** (OpenAlex / Crossref / Semantic
Scholar), **AI & embeddings**, **Tracked authors**, and — when relevant —
**Plugins**, **Alerts**, and **Background jobs**. Each chip is compact: a
colored **status dot** (healthy / warning / critical), an icon, the component
name, and its metric ("2 with errors", "maintenance due", "all healthy").
Worst-first, so a degraded component leads.

Every chip is **clickable** — it opens a **centered popup** (no route change)
explaining that component:

- **Healthy** — a plain-English note on what "healthy" means here and how the
  component is configured.
- **Degraded** — the issues, each with the same **one-click remediation** as
  before (refresh a monitor, repair or backfill an author, re-enable a source,
  refresh stale embeddings / clear the similarity cache, re-run an alert, test a
  plugin), plus a jump to the component's owner page.

This consolidates what used to be three overlapping surfaces (a scoreboard of
counts, a subsystem list, and a separate "degraded right now" list) into one
strip fed from a single diagnostics source, so everything agrees. No charts live
here — subsystem *trends and analytics* are in
**[Insights → Activity](insights.md)**.

## Repair operations

The bulk of the page: the bounded background jobs that actually fix the corpus,
grouped **Corpus & embeddings** / **Authors** / **Other maintenance**,
worst-first, with healthy + idle ops collapsed into an "All clear" strip.

The card unit is the **operation**, not the dimension, because the mapping is
many-to-many — `corpus_metadata` rehydration alone repairs seven data
dimensions. Each **RepairCard** shows:

- **The gaps it heals** as status rows — unresolved identity, missing abstract /
  references / topics / authors / DOI / date / URL, embedding coverage,
  fetchable S2 vectors, locally-computable embeddings, the retry ledger — each
  with a **severity** and a click-through to the affected papers.
- **The controls to act**, once per operation: **Run now** processes a bounded
  batch; **Auto-repair** is an opt-in toggle within a **daily cap**; network
  operations expose a **scope** selector, a **dry-run** preview, and (where it
  matters) an API **batch size**.
- A **cost** tag — *local* (your database), *network* (OpenAlex / Crossref /
  Semantic Scholar), or *compute* (local SPECTER2).

### ETAs

Every **network** operation shows an **ETA** — how long it will take at the
source API's rate limit. The estimate is computed from one rate model
(`requests = ceil(items / batch_size)`, `seconds = requests / rps`) and is:

- **auth-aware** — OpenAlex runs ~10 req/s with a key vs ~1 req/s without; a
  Semantic Scholar key buys reliability, not a higher rate (still 1 req/s).
- **scope- and batch-aware** — it recomputes live as you change an operation's
  scope or batch size (e.g. `s2_vector` over 569 papers: ~2 s at batch 500, ~3 s
  at batch 250; `title_resolution` over ~1,800 papers ≈ 30 min at 1 req/s).

Local SPECTER2 compute shows no ETA — it is fast and not rate-limited.

### Drilldown — act on the affected items

Every status row is clickable, drilling into the items it affects:

- **Paper** dimensions open a **centered modal** listing the affected papers.
  Select rows to **Fix N** (a targeted run for exactly those papers) or
  **Remove N** (soft-remove from Library); per row you can **add an abstract** /
  **edit authors** inline, remove, or open the paper at its source. The header
  carries the bulk "fix the whole dimension" action. (Edit / remove apply to
  Library papers; tracked-but-unsaved papers are read-only.)
- **Author** dimensions jump to the **[Authors](authors.md)** page's
  needs-attention section — the canonical place to repair / merge those authors —
  rather than duplicating author management in a health modal.

## Observed — no automatic repair

Data gaps that have no one-click fix are listed read-only at the bottom, so
nothing a dimension surfaces is hidden just because there's no operation for it.

## The idle healer

The same operations can run unattended: the **idle healer** repairs the corpus
in the background — but only the tasks you opt in, never beyond their daily cap,
one task per tick, worst-severity first. It is **off by default end to end**;
`ALMA_DISABLE_IDLE_MAINTENANCE=1` is a global kill switch (see
[Background jobs](../operations/background-jobs.md)).

## How fresh is this?

Everything is served from the fingerprint-keyed materialised-view cache (see
[Performance](../operations/performance.md)): a GET returns the last snapshot in
<10 ms and rebuilds in the background when the data changes. The header shows
when the snapshot was last assessed.

## API

```
GET  /api/v1/insights/health                                   # canonical data dimensions + totals (the ribbon)
GET  /api/v1/insights/diagnostics/sections/operational         # system-component states + remediation targets
GET  /api/v1/health/operations                                 # ops: status, ETA, params_spec, config, last run
GET  /api/v1/health/operations/{key}/estimate?scope=&dry_run=&batch_size=  # scope/batch-aware count + ETA
POST /api/v1/health/operations/{key}/run                       # run now ({ target_paper_ids?, params? })
POST /api/v1/health/operations/{key}/config                    # { enabled?, daily_cap?, batch_size? }
GET  /api/v1/health/dimensions/{key}/items                     # affected papers (drilldown, paginated)
```
