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

## Worth your attention

Home chooses up to three deterministic, source-balanced highlights:

- a monitored Feed paper;
- a top pending Discovery match;
- a paper representing an active followed author or journal.

Each row states why it was selected and shows a short TLDR or abstract excerpt.
Today's material is preferred. On quiet days ALMa may use the previous seven
days, clearly labelled **Last 7 days**. The selection is structured curation,
not AI-generated prose.

Home is navigation-only. Opening a highlight takes you to its Feed monitor,
Discovery lens, or paper detail on the owner page; all save, rate, read, and
dismiss actions remain there.

## Continue reading

Up to three papers from the reading list provide continuity with work already
in progress. Reading state is independent of Library membership, so an
unsaved paper marked **Reading** still appears.

## Needs attention

This section appears only for user-fixable decisions or blockers:

- imported papers that need review;
- Feed monitors that need relinking;
- author identities that need a decision;
- actionable critical Health findings.

Routine background work and healthy-state reassurance stay off Home.

## Start a workflow

The header offers two shortcuts: **Find papers** and **Follow author**. They
use ALMa's primary action treatment, open the corresponding owner workflow, and do not
duplicate its controls on Home.

---

Home is the default route: an empty address (`#/`) lands here, and every section
links back to the page that owns its data and actions.
