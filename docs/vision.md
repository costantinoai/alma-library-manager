---
title: Vision & philosophy
description: Why ALMa exists, the design principles it tries to keep, and the lifecycle model that holds the whole UI together.
---

# Vision & philosophy

## Why ALMa exists

Most academic literature tools fall into one of two camps:

1. **Citation managers** (Zotero, Mendeley, EndNote) — excellent at
   storing and citing what you've already collected. They do not
   surface anything new.
2. **Discovery aggregators** (Semantic Scholar feeds, ResearchGate
   alerts, Connected Papers) — surface new papers, but they don't
   feel like a library. There's no place that holds *your* corpus,
   no notes carrying forward, and no way to tune the ranker against
   your specific reading patterns: your saved papers, your ratings,
   the things you keep dismissing.

A working researcher needs both, joined at the hip. The papers you
have already chosen to keep are *the* most informative signal about
what you'll want to read next, and the papers you discover are
worthless if there's no friction-free way to keep them.

ALMa's premise is that for a single user, on a single machine, the
two halves can be the same system: a curated library that doubles as
a preference model, feeding a recommender that pours into the same
library.

## Single user, by design

ALMa is deliberately not a multi-tenant product. Every architectural
choice that flows from "single user" — no auth, file-backed SQLite,
no notion of accounts, settings stored as one JSON blob — pays back
as simpler code, faster iteration, and no privacy surface area.

The user owns their data because the data lives on their disk. The
user owns their compute because all heavy work (embeddings,
clustering, label generation) runs locally or against an API key
they configured.

## The lifecycle model

The single most important idea in ALMa, and the one that makes the
whole UI navigable, is that **every paper has two independent state
dimensions**:

| Axis | Values | What it means |
|---|---|---|
| **Membership** | `tracked` · `library` · `dismissed` · `removed` | Where the paper sits in your curation flow |
| **Reading** | *(none)* · `queued` · `reading` · `done` · `excluded` | Where the paper sits in your reading flow |

The two never overload. Saving a paper does not queue it for reading.
Queueing a paper for reading does not save it (it can sit in the
queue while still being a candidate). Marking a paper "done" does not
remove it from the library.

This sounds bureaucratic until you've fought a tool that conflates
the axes — at which point the separation feels obvious. ALMa exists
because that fight took years.

### Why not "starred" / "favourites" / "liked"?

Those terms collapse three independent ideas:

* "I've kept this paper" → **Saved** (membership = `library`)
* "I rate this 4/5 stars" → **Like** (rating)
* "I've finished reading this" → **Done** (reading = `done`)

ALMa keeps them apart on purpose. The vocabulary in the UI maps
exactly: **Save / Like / Love / Dislike** are the rating verbs (a
single mutually-exclusive state per paper, 1–5 stars), and they are
distinct from the **Saved** library and the reading list.

## Design principles

These are the rules that recur across the codebase. Most are recorded
as engineering invariants rather than aspirations.

### One intent per action

Every user action in the UI maps to exactly one canonical backend
use-case. Two buttons that mean the same thing eventually drift apart,
so ALMa removes the duplicate rather than maintaining two paths.

### Observable system

Every meaningful operation surfaces in the **Activity** panel —
running jobs, completed jobs, terminal states with messages,
per-source timing. If something happened, you can see that it
happened, why, and how long it took.

### Truthful UI

A label or toggle in the UI represents the actual behaviour of the
underlying call. If a setting is not yet wired up, it does not appear.
If a feature requires an API key you haven't configured, it is hidden
or disabled with an honest reason — not enabled with a silent
fallback.

### AI is opt-in

ALMa works without any AI provider configured. Discovery still ranks,
clustering falls back to non-AI strategies, recommendations still
flow. AI features (semantic similarity, cluster labels, auto-tags,
LLM query planning) light up only when you've explicitly enabled and
configured the underlying provider. There are no half-working
fallbacks that quietly degrade quality.

### No silent failures

Failures are loud — either blocking with a clear error, or logged
prominently with the chosen fallback explained. The
[Activity panel](concepts/insights.md#activity-panel) and `/api/v1/logs`
are the two surfaces that make this honest.

### Forward-looking codebase

Renames, schema changes, and contract changes are one-shot migrations,
not multi-version compatibility shims. If `/library/likes` is renamed
to `/library/saved`, the old endpoint is removed in the same change —
the codebase never carries two paths for the same intent.

## What that buys you

A small, comprehensible system. ALMa's whole backend is a single
FastAPI app with ~25 route modules; the database is one file; the
frontend is one Vite SPA. You can read the whole stack in a weekend
and rebuild any part of it on Monday.

The cost is that everything runs at "personal" scale. Don't try to
share your `scholar.db` with three colleagues, don't expect
horizontal scaling, don't expect uptime SLAs. Treat ALMa like your
text editor: a personal tool that you trust because you can see
exactly what it does.
