---
title: Home
description: A quiet daily research desk — today's activity, unfinished review, balanced highlights, reading continuity, and blockers that need you.
---

# Home

**Home** is where ALMa opens. It is a daily research desk rather than a
dashboard: it summarizes what arrived, selects a few explainable highlights,
and hands each item to the page that owns it.

The brief uses the browser's local timezone. Refreshing Home never changes its
counts or consumes unread state.

## How the page is laid out

The page has two halves, and the split is meaning, not decoration:

- **The blotter** — one raised panel at the top carrying your whole *situation*:
  the greeting and date, the two workflow shortcuts, one slim status line, and
  today's figures. It is the pad you work on, and it is always the same shape.
- **The desk** — the research itself, as loose sheets below: one optional
  Signal Lab calibration, Inbox, Picked for you, Reading list. Each paper
  section is its own **collapsible block** — fold away what
  isn't today's business and the rest comes up to meet you.

Reading order follows urgency: what is my situation (blotter) → one cheap taste
calibration when available → what did I send myself (Inbox) → what did ALMa
find (Picked for you) → what am I already reading.

Signal Lab serves a deck of at least ten signed rounds immediately above Inbox
when the feature is active and the super-region graph exists. Its distinct game
board explains the task, shows deck progress, and adds quiet evidence rows for
directions, boundaries, daily/total rounds, unique and fitted observations,
fit freshness, and super-region/edge coverage. It never presents the
combinatorial triplet universe as progress. Disabling the feature hides this
section and ignores retained signals without deleting them.

## The status line

One slim line under the greeting, with no heading of its own, carrying two
groups split by a hairline:

- **left — the machinery**: one dot per core subsystem/capability (see
  [System status](#system-status) below);
- **right — what wants you**: one chip per pending decision (see
  [Needs you](#needs-you)).

Both answer "what is my situation" before a single number is read, and a dot with
a name says it without a label announcing it. Every item links to the surface
that owns the fix. A chip that would read zero is absent, so the line's own
length is information.

## Today in ALMa

An editorial **scoreboard** — bare figures split by hairlines, not a grid of
boxes. Six cells, and each label says its own scope, because the row mixes
today's arrivals with what is standing on the desk:

| Figure | Scope | Links to |
|---|---|---|
| **new Feed papers** (distinct papers) | today | [Feed](feed.md) |
| **new suggestions** across active lenses | today | [Discovery](discovery.md) |
| **alerts delivered successfully** | today | Alerts → History |
| **waiting in Inbox** | now | scrolls to the Inbox below |
| **on your reading list** | now | Library → Reading |
| **in the last 7 days** | the week | — |

The last cell is the **inflow chart**: one small stacked column per local day,
Feed at the base and Discovery above it, with the week's total set as the cell's
number and the exact figures on hover. It sits where the other cells put their
numeral, so the week's shape reads as one more figure rather than a chart bolted
on beside the heading. A zero-Feed morning means nothing on its own and
everything next to six busy days; empty days are a tick on the baseline, because
the absence is the information.

A **monitor-mix ribbon** under the row splits today's Feed intake across
authors / journals / other, so you can tell an author-driven day from a
journal-driven one at a glance. It renders only when something arrived.

Older, still-unreviewed Feed and Discovery items appear separately as
**carryover**. Carryover clears only when you visit the owning Feed or
Discovery lens. `GET /home/brief` is a pure read: Home has no “seen” endpoint
and never stamps owner review state.

## Picked for you

Home chooses deterministic, source-balanced highlights — one of each kind
first:

- a monitored Feed paper;
- a top pending Discovery match;
- a paper representing an active followed author or journal;

then tops the row up with the next-best Discovery matches. The section is
always **exactly one row** of the tile grid, so it is a shortlist rather than a
truncated list and carries no "Show more".

Discovery tiles show their **lens match score** (0–100) as a bar. Feed picks
show none: they are selected by what you monitor and when they appeared, not by
a score, and Home does not invent one. Every tile carries a **Why this is
here** popover stating the rule that selected it.

Every paper on Home renders as a tile in a grid. Tiles in a row share one
height, so scanning is spatial and no paper wins by having a longer abstract,
and the column count follows the measured width of the page — a narrow window
or a phone gets fewer, full-width tiles rather than compressed slivers. Each
tile states why it was selected and shows a short TLDR or abstract excerpt.
Today's material is preferred. On quiet days ALMa may use the previous seven
days, clearly labelled **Last 7 days**. The selection is structured curation,
not AI-generated prose.

Home is navigation-only. Opening a highlight takes you to its Feed monitor,
Discovery lens, or paper detail on the owner page; all save, rate, read, and
dismiss actions remain there.

Home paper tiles deliberately read as calm sticky notes on the desk. One
central category palette owns every use—tile paper, count pills, small chart
marks, and title symbols: Inbox green, Reading violet, Picked gold, Discovery
blue-cyan, and Feed magenta. The page title words remain navy; only the symbol
before each section title carries category colour.

## Inbox

Papers you sent yourself from another device — Slack today, any channel that
honours the capture contract. See [Inbox](inbox.md).

Home is the Inbox's **owning surface**, which makes it the one deliberate
exception to Home being read-only: triage happens here, because there is
nowhere else for it to happen. Captures render as the same measured tiles as
every other section, each carrying the normal actions plus **Not now** (the ✕),
which drops the paper out of the Inbox, keeps it in your corpus, and records
**no opinion at all** — distinct from **Dislike**, which is how you tell ALMa a
paper is wrong for you. Every button posts to the one canonical action route,
and every outcome is confirmed by name: "Cleared from your Inbox, kept in your
corpus" reads differently from "Saved to your library", on purpose.

Each tile shows where the paper came from — a **channel chip** (Slack today)
and when you sent it. Captures are ordered by **when you sent them**, not by
when the paper first entered your corpus: a paper your Feed collected two years
ago, which you re-sent yourself this morning, belongs at the top of the queue it
just joined.

The Inbox shelf remains in the page order with a zero count when empty, folded
shut and without empty-state copy. Its position therefore stays predictable
without nagging. Papers are not meant to accumulate here.

While a capture waits it is a full corpus member — enriched, embedded, mapped,
deduplicated — but it is excluded from every preference query, so an untriaged
paper cannot skew your recommendations.

## Reading list

The most recently added reading-list papers, newest first, with **Show more**
for the rest. Reading state is independent of Library membership, so an unsaved
paper marked **Reading** still appears.

The section shows two full rows of the measured tile grid while collapsed — up
to eight tiles on a wide desk and two on a phone — so it never ends on a ragged
half-row.

## System status

The left half of the status line carries compact pills such as Automation,
Feed, Discovery, the active embedding stack, Inbox, and Alerts. Only the
severity dot and owner name are visible; metric, explanation, and check time
live on hover.

Inbox and Alerts are core capabilities, so Home never labels either pill
“Slack”, “SMTP”, or another provider. Their provider-independent state is:

- green — at least one supporting integration is configured, active, and
  working;
- red — configured but the direction is failing;
- grey — no integration is configured.

Provider controls remain under **Settings → Plugins**. Every status pill links
to the page or Settings section that owns its remedy; where no owner route
exists, Home uses the shared popup primitive for the repair action.

States come from local configuration, domain state, and the **operation
ledger**, not from live probes. Home therefore says what happened the last time
ALMa used a capability. A cancelled operation sets no health verdict. This
keeps `GET /home/brief` a pure read.

## Needs you

The right half of the status line — the other half of "what is my situation".
Chips appear only for user-fixable decisions or blockers, ordered by severity
rather than by which part of ALMa raised them, so something broken and staying
broken outranks a queue that is merely waiting:

- actionable critical Health findings;
- Feed monitors that need relinking (a broken monitor silently stops
  delivering, so it is a break, not a pending decision);
- captured messages that resolved to no paper (a link with no DOI, or an
  upstream failure) — recorded rather than dropped, so they ask for a human
  here;
- author identities that need a decision;
- imported papers that need review.

Each chip is the same one [Health](health.md) uses in its system-status band —
a severity dot, what it is, and how many — in a slimmer weight, so a
component-and-its-state reads identically on both pages. No heading announces
them: a chip reading "Health · 1 critical issue" already says it.

Routine background work and healthy-state reassurance stay off Home.

## Start a workflow

The header offers two shortcuts: **Find papers** and **Follow author**. They
use ALMa's primary action treatment, open the corresponding owner workflow, and do not
duplicate its controls on Home.

---

Home is the default route: an empty address (`#/`) lands here, and every section
links back to the page that owns its data and actions.
