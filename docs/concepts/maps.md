---
title: Semantic maps
description: How ALMa's Paper, Discovery, and Author maps share one semantic space, interaction language, and background layout lifecycle.
---

# Semantic maps

ALMa uses maps to answer spatial questions that lists cannot:

- Which papers or authors work on similar ideas?
- Where does your Library sit inside the wider corpus?
- Which recommendations extend familiar ground, and which open a new direction?
- How does your feedback vary across the research landscape?

The top-level **Map** page, the map above [Discovery](discovery.md), and the
map on [Authors](authors.md) all use the same canvas renderer and control
language. Position means semantic proximity; colour, fill, outlines, size, and
terrain each carry one separate meaning.

## One durable paper space

Paper coordinates come from one corpus-scope SPECTER2 substrate. Opening or
revisiting a page reads the stored layout—it does not run UMAP or clustering.
Viewport and display controls survive route changes, and the browser keeps
layout payloads for the session. Live scores and preference fields are small
separate queries, so reacting to a paper updates colour and Terrain without
rebuilding coordinates.

Layout maintenance is idle-gated. Missing layouts, explicit rebuilds, version
changes, and Advanced option variants run in a separate worker process. The API
continues serving Home, Suggestions, and the last-good map while the
replacement computes; Activity shows phase progress and can cancel the worker.
Reference enrichment is a different background operation.

## The visual language

The exact legend varies by host, but the channels do not:

| Channel | Meaning |
|---|---|
| Position | Semantic similarity |
| Fill | Yours: a saved paper or an author connected to your saved work |
| Hollow/faint | Suggested or contextual material, as named by the host legend |
| Gold outline | An author currently offered by the suggestion engine |
| Dashed halo | A secondary fact such as followed/new, never a replacement colour |
| Colour | The selected grouping or score mode |
| Size | The selected magnitude, such as citations or publication count |
| Terrain | A smoothed live preference field from likes, loves, dislikes, saves, and dismissals |

Cluster words and place names share one collision-aware label pass. Advanced
controls tune cluster detail, dot size, dot opacity, word size, and words per
cluster consistently across all hosts.

## Paper interaction

Clicking a paper dot opens a compact anchored card. Identity, the internal
score bar, state chips, and dislike/like/love, Library, reading, and collection
actions remain immediately available. TLDR, cluster, neighbour, venue, and
citation context sit behind **More context**. Opening the full paper is always
an explicit button.

On Discovery, clicking a dot never scrolls or jumps the list. **Go to paper**
is the only dot-to-list navigation. On the top-level Map page, selection also
populates the two-column drilldown below the plate with paper, cluster, and
author context.

## Region interaction

**Select region** lassos an area without changing coordinates:

- Paper/Discovery regions get a shared vocabulary description, member counts,
  and representative titles.
- Discovery can adopt a selected region as a lens direction.
- Author regions derive a local community digest, top authors, followed and
  suggested counts, and can follow the eligible members in one action.

Selection is explicit and view-local. It never silently changes a lens,
membership, or follow state.

## Author placement

Authors do not get a second manifold projection. An author is eligible only
when at least two of their papers are already placed on the corpus paper
substrate; their coordinate is the centroid of those paper positions. This
gate runs before community clustering and payload construction.

Thin suggestions remain visible in the suggestion rail and omission count
until the background paper-seeding and embedding chain provides enough
evidence. ALMa never invents radial or rank-based coordinates.
