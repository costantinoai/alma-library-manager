---
title: Health
description: Is my data healthy, and what do I do about it — a page that diagnoses corpus-data gaps and repairs them, shows the operational status of every system component, records what the system has been doing, and runs an opt-in idle healer. Health owns all repair; Settings is configuration only.
---

# Health

The **Health** page answers one question: *is my data healthy — and what do I
do about it?* It reads a single canonical health layer, so every number agrees
with the rest of the app, and the status of something and the operation that
repairs it always live in the *same* card.

Health owns **all diagnosis and repair**. Settings is configuration only — when
something is fixable, Settings points you here.

A **persistent band** at the top — the vitals ribbon, the system-status chips,
and the API budget — answers "is everything OK?" without making you pick a tab.
The page reads durable snapshots prepared at startup, after relevant mutations
and completed jobs, and every five minutes. Opening Health never runs the
corpus-wide assessment; **Re-assess** queues that work in Activity and replaces
the stored snapshot when it finishes.
Two tabs below carry the work, most actionable first:

| Tab | What's in it |
|---|---|
| **Repairs** | The recommended next step, the repair-operation cards worst-first, and the observed gaps that have no automatic fix. What you can *do*. |
| **Activity** | What already *ran*: failed background operations (expand a row for its error and step log), the evaluation scorecards, and the latest Feed / Discovery refreshes. |

!!! note "Activity used to live in Insights"
    Operational telemetry moved here because it belongs beside the repairs it
    explains. The five passive trend charts that surrounded it (intake,
    discovery action, alert delivery, followed-author growth, feedback
    activity) were deleted rather than moved — they plotted history nobody
    acted on. Old `#/insights?tab=activity` links redirect here, and the
    "Background jobs" status popup deep-links straight to the failing run
    (`#/health?tab=activity&focus=failed`).

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
  plugin), plus a jump to the component's owner page. The strip is fed from a
  stale-while-revalidate snapshot, so a listed issue can occasionally be one that
  was already resolved; remediating such a phantom (its target returns 404)
  quietly clears it from the strip rather than failing.

This consolidates what used to be three overlapping surfaces (a scoreboard of
counts, a subsystem list, and a separate "degraded right now" list) into one
strip fed from a single diagnostics source, so everything agrees. No charts
live here: what the system has been *doing* is in the **Activity** tab, and
corpus *analytics* are in [Library → Analytics](analytics.md).

## Repair operations

The bulk of the page: the bounded background jobs that actually fix the corpus.
Grouping and order come **entirely from the backend plan** (`GET
/health/operations` returns ordered `stages`) — the full repair DAG, 18
operations across 11 dependency-ordered stages (author identity → canonicalization
→ works → paper identity → metadata → canonicalize → S2 vectors → local embeddings
→ derived data → cleanup → housekeeping). The frontend renders those stages
verbatim with no hard-coded task lists; worst-first within a stage, healthy + idle
ops collapsed into an "All clear" strip.

A **Recommended next** banner points at the first actionable, non-blocked,
**safe** operation in dependency order (never a destructive or manual-gate op).
**Run step** runs just that one; **Run sequence** auto-advances through the safe
steps — re-planning as each finishes and stopping at the first manual-review gate.

The card unit is the **operation**, not the dimension, because the mapping is
many-to-many — `corpus_metadata` rehydration alone repairs seven data
dimensions. Each **RepairCard** shows:

- **The gaps it heals** as status rows — unresolved identity, missing abstract /
  references / topics / authors / DOI / date / URL, embedding coverage,
  fetchable S2 vectors, locally-computable embeddings, the retry ledger — each
  with a **severity** and a click-through to the affected papers.
- **The controls to act**, once per operation, as three SEPARATE numbers (no
  more "daily cap" doubling as run size): a **manual run limit** (units for one
  click, sent atomically with Run), an **auto daily cap** (unattended units per
  UTC day), and a **request batch size** (upstream IDs per HTTP call, endpoint-
  bounded). Invalid values are **rejected (422), never silently clamped**.
  Destructive operations (dedup/merge, orphan GC, library dedup) have **no
  Auto-repair toggle** and require an explicit confirmation before applying.
- A **cost** tag — *local* (your database), *network* (OpenAlex / Crossref /
  Semantic Scholar), or *compute* (local SPECTER2).

The persistent **System status** band also carries the application-wide
**External network access** switch beside the API budget. It saves immediately:
turning it off blocks outbound APIs and integrations while leaving local work
available. An `ALMA_DISABLE_NETWORK=1` operations override forces the displayed
state off and disables the switch until the override is removed.

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
  rather than duplicating author management in a health modal. Both surfaces
  (and the Insights authors snapshot) compose from one attention model
  (`alma.services.author_attention`: identity ladder, degraded monitors,
  corpus-backfill states), so their counts and rows can never disagree.

### Suggested but unplaceable

`authors.unplaceable` counts authors the engine is currently suggesting that
the corpus holds **fewer than two papers** for. Below two papers an author is
invisible three times over: no dot on the [author map](analytics.md) (the
layout needs two papers to place someone), no sample titles on their suggestion
card (those are read from your local corpus, so "No sample title" is a missing
*paper*, not missing metadata), and no score. The engine recommends them while
showing you nothing to judge them on.

**Seed their papers** repairs it: their most-cited first-author works are
fetched and landed as tracked corpus papers — enough to place them, caption
them, and score them. Papers save as `tracked`, never to your Library.

Some authors can never clear the bar: OpenAlex genuinely holds fewer than two
works for them. Those are recorded as **tried and terminal** and reported
separately as `exhausted`, so the row converges to zero outstanding instead of
nagging forever with a repair that can't move it. This is the same
tried-and-terminal treatment papers get when Semantic Scholar has no vector for
them.

## Observed — no automatic repair

Data gaps that have no one-click fix are listed read-only at the bottom, so
nothing a dimension surfaces is hidden just because there's no operation for it.

## The idle healer

The same operations can run unattended: the **idle healer** repairs the corpus
in the background — but only the tasks you opt in, never beyond their daily cap,
one task per tick, worst-severity first. Background work **yields to you entirely**
(task 37): a sweep only *starts* when **no other operation is running** and the app
has been **idle for 3 minutes** (no user request — the activity clock is in-memory,
so it never writes on a read), and a sweep already running **pauses the moment you
do anything** (open a page, save, or start an operation manually). A paused or
quota-stopped sweep leaves its remaining work queued and the idle healer resumes it
once you're idle again. Background sweeps that call an external provider also
**reserve 200 API calls for your manual work** — they stop before consuming
OpenAlex's daily quota past that floor and report it on the page (the **OpenAlex API
budget** tile shows the live remaining count, and a notice when the last background
run stopped to protect your reserve). Each external repair card forecasts its
bounded request/credit cost before launch. When known remaining quota cannot
cover that plan, the card explains the shortfall and disables **Run now**;
automatic repair skips the same plan. Your own manual operations are never paused
and may use the full remaining quota, but cannot exceed the provider's hard pool.
Both knobs — the idle-wait and the reserve —
are adjustable in **Settings → Data & system → Background operations**. The healer is
**off by default end to end**;
`ALMA_DISABLE_IDLE_MAINTENANCE=1` is a global kill switch (see
[Background jobs](../operations/background-jobs.md)).

### Stopping a healer run means stopping it

If you stop an unattended sweep from the Activity panel — either verb — that
task goes on a **24-hour cooldown**. Neither the hourly healer tick nor the
startup orphan-resume will schedule it again while the cooldown holds, so the
job you stopped does not quietly reappear an hour later under a new id.

The card says so out loud: the auto-repair label reads **(paused)** and a
*Paused by you until …* line appears next to it, with a **Resume** control that
lifts the hold immediately. Running the task yourself lifts it too — that is the
natural "I want this again" gesture — and the stamp lapses on its own after 24 h.

Your **Auto-repair** toggle is deliberately left alone. The cooldown is a
temporary hold, not a change to the policy you configured, so the switch keeps
showing what you chose and the pause line explains why nothing is running right
now. A sweep the *app* paused to get out of your way writes no cooldown at all —
that one resumes by itself on the next idle tick.

## How fresh is this?

Everything is served from the fingerprint-keyed materialised-view cache (see
[Performance](../operations/performance.md)): a GET returns the last snapshot in
<10 ms and rebuilds in the background when the data changes. The header shows
when the snapshot was last assessed.

## API

```
GET  /api/v1/insights/health                                   # canonical data dimensions + totals (the ribbon)
GET  /api/v1/insights/diagnostics/sections/operational         # system-component states + remediation targets
GET  /api/v1/health/operations                                 # ordered stages + recommended_next + ops (status, ETA, config, last run)
GET  /api/v1/health/operations/{key}/estimate?scope=&dry_run=&batch_size=  # scope/batch-aware count + ETA
POST /api/v1/health/operations/{key}/run                       # run now — atomic spec { max_items?, target_ids?, request_batch_size?, scope?, dry_run?, confirmation_token?, plan_fingerprint? }
POST /api/v1/health/operations/{key}/config                    # { auto_enabled?, auto_daily_cap?, remembered_manual_limit?, request_batch_size? } — invalid → 422
POST /api/v1/health/operations/{key}/resume                    # lift the 24h cooldown a manual stop left on this task
GET  /api/v1/health/dimensions/{key}/items                     # affected papers (drilldown, paginated)
```
