---
title: First run
description: A 5-minute checklist to take a fresh ALMa install from empty to a working personal feed.
---

# First run

After installing and starting the backend, work through this short
checklist. Everything below assumes you have <http://localhost:8000>
open in a browser.

![Library on first run, before any saves](../screenshots/desktop-library.png)

## 1. Set your API keys

OpenAlex **requires an API key** (since 2026-02-13 — keyless requests get
100 credits/day, then HTTP 409). A [Semantic Scholar key](https://www.semanticscholar.org/product/api)
is strongly recommended too — without it S2 shares the anonymous worldwide
pool and 429s often, stalling Discovery.

Go to **Settings → Connections**, paste your [OpenAlex key](https://openalex.org/settings/api)
into **OpenAlex** and (optionally) your S2 key into **Semantic Scholar**,
then **Save connection settings**. You should see a green "OK" indicator
next to OpenAlex in the **Settings → Status** card.

If you started the container with `-e OPENALEX_API_KEY=...` (or
`env_file:`), this is already set — no UI step needed.

![Settings page — External APIs / Backend tab](../screenshots/desktop-settings.png)

## 2. Follow your first author

Go to **Authors → Add author**. You can search by:

* Name (partial match against OpenAlex)
* OpenAlex author ID (`A1234567890`)
* ORCID
* (Optional, if `scholarly` is installed) Google Scholar ID

When you confirm an author, ALMa runs a backfill: their recent works
are pulled from OpenAlex, deduplicated, and inserted as `tracked`
papers (not yet in your Library — see [Paper lifecycle](../concepts/paper-lifecycle.md)).

You can watch this happen in the **Activity panel** at the bottom of
the screen.

## 3. Open the Feed

Once the backfill completes (usually under a minute for a single
author with a few hundred works), open **Feed**. You should see a
chronological list of recent papers from that author. The Feed window
is bounded to roughly the last 60 days by default; older papers
remain queryable from Library and Discovery.

## 4. Save a few papers

Click **Save** on a few papers in the Feed. They move into your
**Library**. Optionally, give them a rating (Like / Love) — these
ratings feed into Discovery's preference model.

## 5. Try Discovery

Open **Discovery**. With even a small Library, the recommender will
have something to work with. Click **Refresh lens** on the canonical
lens to see the first batch of recommendations.

A first lens refresh against a fresh Library can take a minute or
two — the recommender is doing multi-source retrieval, ranking, and
caching. Subsequent refreshes are fast. The
[Performance](../operations/performance.md) page documents the
expected budget.

## 6. (Optional) Configure an AI provider

For semantic similarity, cluster labels, and auto-tagging, configure
at least one AI provider in **Settings → AI & embeddings**:

* **Semantic Scholar** — no setup required; ALMa fetches pre-computed
  SPECTER2 vectors from S2 for any paper that has one.
* **Local SPECTER2** — runs `allenai/specter2_base` locally. Good for
  papers S2 doesn't have. Requires the `[ai]` extras.
* **OpenAI** — optional cloud embedding provider if you already use
  OpenAI.

The settings page surfaces what's installed and what isn't, with
"Recheck" buttons that re-introspect the chosen runtime.

## 7. (Optional) Import an existing library

If you have a Zotero or BibTeX export, **Library → Imports → Import
papers** will pull it in. Imports go straight into your Library
(`status='library'`, `added_from='import'`) — they are treated as
explicit save intents, not staged candidates.

See [Importing from Zotero / BibTeX](../user-guide/importing.md) for
the full flow.

## You're done

You now have:

* Authors generating Feed items
* Saved papers building your preference model
* Discovery recommendations refreshing on demand

The next thing worth reading is [Vision & philosophy](../vision.md) —
it explains the membership × reading lifecycle that everything in the
UI is built around.
