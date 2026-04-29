---
title: Discovery
description: Discovery is ALMa's recommendation engine — probabilistic, ranked, and explainable. Each suggestion comes with the signals that produced it.
---

# Discovery

**Discovery** answers: *given what I have saved, liked, disliked, or
followed, which papers might I want to add next?*

![Discovery page with the Find & Add bar, lenses, and Branch Studio](../screenshots/desktop-discovery.png)

It is explicitly different from [Feed](feed.md):

| Feed | Discovery |
|---|---|
| Deterministic monitoring | Probabilistic recommendation |
| Chronological | Ranked by relevance |
| One source = one row | Multi-source retrieval, deduplicated |
| Window: ~60 days | Window: open |

## How a recommendation is produced

Discovery is organised around **lenses** — context-scoped pipelines.
The default "global Library" lens treats your entire saved
collection as the seed. You can also define lenses scoped to a
[collection](library.md#collections), a topic keyword, or a tag (see
[Lenses](lenses.md)).

For each lens, refresh runs in three phases:

1. **Retrieval** — fan out across multiple sources to assemble a
   candidate set.
2. **Ranking** — score each candidate via a 10-weight hybrid
   formula.
3. **Branch grouping** — cluster results into themed sub-groups
   ("Branches") for navigation.

### Retrieval channels

| Channel | Source | What it returns |
|---|---|---|
| **OpenAlex related works** | OpenAlex `/works/{id}/related-works` | Papers OpenAlex itself flags as related to your saved papers. |
| **OpenAlex topic search** | OpenAlex `/works?filter=topics.id:…` | New papers in topics you've saved into. |
| **Followed-author works** | OpenAlex `/works?filter=author.id:…` | Recent works from authors on your follow list. |
| **Co-author network** | OpenAlex graph | Papers by frequent co-authors of your saved authors. |
| **Citation chain** | OpenAlex / S2 | Papers that cite your highly-rated papers. |
| **Semantic Scholar related** | S2 `/recommendations` | S2's own recommender, with optional filters. |
| **SPECTER2 cosine** | local cache | Top-k cosine neighbours of your library centroid (if embeddings are enabled). |

Channels can be enabled / disabled / weighted in **Settings →
Discovery weights**. Each channel runs with a per-lane deadline so a
single slow source can't stall the whole refresh.

### Ranking signals

The hybrid scorer combines (default weights configurable):

* **Source relevance** — how strong was the signal that produced the
  candidate (high for related-works, lower for broad topic search).
* **Topic score** — overlap between the candidate's topics and your
  preferred topics.
* **Text similarity** — semantic (SPECTER2 cosine if available) +
  lexical fallback (TF-IDF + character n-grams + scholarly term
  overlap).
* **Author affinity** — has the candidate's author appeared in your
  Library or follow list?
* **Journal affinity** — does the candidate's venue appear often in
  your Library?
* **Recency boost** — newer papers get a small boost.
* **Citation quality** — log-scaled citation count.
* **Feedback adjustment** — penalise candidates whose authors /
  topics / venues you've previously dismissed.
* **Preference affinity** — distance from your `preference_profiles`
  centroid.
* **Usefulness boost** — explicit per-source bonus.

Each candidate's `score_breakdown` is exposed in the API so the UI
can show the signals that pushed a recommendation up or down.

### Branches

Recommendations are clustered into **Branches** — small themed
groupings within a lens. A branch has:

* A label derived from the dominant topics and representative titles.
* A score and quality state (`strong / cool / underexplored /
  narrow / monitor`).
* A tuning hint that explains why the branch is in that state.
* A handful of representative papers.

The Branch Studio UI lets you pin / mute / boost a branch — those
controls feed back into the next lens refresh's ranking.

## Actions on a Discovery card

| Action | What it does |
|---|---|
| **Save / Like / Love** | Transitions to `library` with the matching rating. |
| **Dismiss** | Hides the card from this lens **and** writes a negative signal. The recommender will not re-suggest it. |
| **Pivot** | Treats the dismissed paper as a seed for a new branch (find more like this, but I haven't saved it). |
| **Open details** | Opens the shared Paper detail panel — abstract, topics, prior / derivative works, full provenance. |

The Paper detail panel shows **Prior works** (papers this one cites)
and **Derivative works** (papers that cite this one). For papers
already in our corpus, those panels link directly; for S2 rows we
haven't imported, they show a stub with citation intent metadata.

## Performance

A canonical lens refresh against ~330 saved papers completes in
about **76 seconds** end-to-end. Subsequent refreshes against the
same lens use cached candidates and are dramatically faster. See
[Performance](../operations/performance.md) for the full budget
table and how to profile your own refresh.

## What Discovery is not

* It is **not a search engine.** Discovery does not take a free-text
  query; it operates over your saved corpus. To search arbitrary
  papers, use the global search box or the Online search panel
  inside the Import dialog.
* It is **not deterministic.** Two refreshes of the same lens with
  the same Library can produce slightly different orderings — the
  signals shift as you save / dismiss things.
* It is **not infallible.** Read the score breakdown. If a branch
  looks wrong, dismiss its core papers — that's the loop that tunes
  the model.

## Read more

* [Lenses](lenses.md) — per-context pipelines
* [Scoring formulas](../reference/scoring.md) — the weight reference
* [Tuning Discovery](../user-guide/tuning-discovery.md) — practical
  guide
