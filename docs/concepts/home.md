---
title: Home
description: The landing page — what arrived since your last visit, what needs you, and one suggestion worth a look. Quiet when everything is fine.
---

# Home

**Home** is where ALMa opens. It answers two questions — *what happened while I
was away*, and *what needs me* — and then gets out of the way. It is a note left
on your desk, not a dashboard: there are no charts, no gauges, and nothing that
exists only to look busy.

Three modules, in this order, and nothing else.

## 1. The brief

A row of figures for the window since you were last here:

| Figure | Links to |
|---|---|
| **new papers** in Feed | [Feed](feed.md) |
| **suggestions** from Discovery | [Discovery](discovery.md) |
| **alerts** delivered | [Alerts](alerts.md) |
| **to read** in your list | Library → Reading list |

Each figure is a door — click it and you land on the surface that owns it.

A count of **zero stays in the row** rather than disappearing: it's still true,
the row keeps the same shape between visits, and the numbers that *did* change
are the ones that stand out. The window itself is stated in plain words
("since Tuesday", "since yesterday").

On your **very first visit** there is no last-visit stamp, so ALMa doesn't
invent one. The heading reads *"Here's where things stand"* and the figures
cover the last 60 days — the same horizon the Feed inbox is bounded to.

!!! note "How the window is tracked"
    `GET /home/brief` computes the brief and is a **pure read**. The visit is
    stamped separately by `POST /home/seen`, fired *after* the page renders.
    That split matters: if loading the page stamped the visit, refreshing would
    silently destroy the very window you were reading.

## 2. Newest in your Feed

The four most recent untriaged arrivals, over the same window as the brief. A
count tells you something happened; a title tells you *what*, which is the
difference between a page you read and one you pass through. Clicking a line
opens the paper. Absent when nothing arrived.

## 3. Still reading

What you already committed to — up to three papers from your reading list — so
Home closes that loop instead of only opening new ones. Absent when the list is
empty.

## 4. Needs you

Actionable rows, each with the control that resolves it:

* **Imported papers waiting to be matched** → Review (Library → Imports)
* **Monitors that stopped and need re-linking** → Fix (Settings → Feed monitors)

When there is nothing to do, **this module renders nothing at all**. ALMa never
shows an "all good" card — a healthy system should be silent, so anything you
see on Home is something you can act on.

## 5. One to look at

The top-ranked suggestion you haven't acted on yet, from the most recent
Discovery refresh. It's a normal paper card, so **Save** and **Dismiss** here do
exactly what they do in Feed and Discovery — including writing the preference
signal that shapes the next refresh. If there are no pending suggestions, the
module is absent.

---

Home is the default route: an empty address (`#/`) lands here, and Feed is one
click away in the sidebar. Every existing deep link is unchanged.
