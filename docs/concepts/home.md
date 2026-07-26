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

## Today in ALMa

Three stable figures report activity since local midnight:

| Figure | Links to |
|---|---|
| **new Feed papers** (distinct papers, split by source type) | [Feed](feed.md) |
| **new suggestions** across active lenses | [Discovery](discovery.md) |
| **alerts delivered successfully** | Alerts → History |

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

## Inbox

Papers you sent yourself from another device — Slack today, any channel that
honours the capture contract. See [Inbox](inbox.md).

Home is the Inbox's **owning surface**, which makes it the one deliberate
exception to Home being read-only: triage happens here, because there is
nowhere else for it to happen. Each card carries the normal actions, plus
**Not now** (the ✕) which drops the paper out of the Inbox, keeps it in your
corpus, and records **no opinion at all** — distinct from **Dislike**, which is
how you tell ALMa a paper is wrong for you.

The section renders only when something is waiting, so an empty Inbox
disappears rather than nagging. Papers are not meant to accumulate here.

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

## Needs attention

This section appears only for user-fixable decisions or blockers:

- imported papers that need review;
- Feed monitors that need relinking;
- author identities that need a decision;
- captured messages that resolved to no paper (a link with no DOI, or an
  upstream failure) — recorded rather than dropped, so they ask for a human
  here;
- actionable critical Health findings.

Routine background work and healthy-state reassurance stay off Home.

## Start a workflow

The header offers two shortcuts: **Find papers** and **Follow author**. They
use ALMa's primary action treatment, open the corresponding owner workflow, and do not
duplicate its controls on Home.

---

Home is the default route: an empty address (`#/`) lands here, and every section
links back to the page that owns its data and actions.
