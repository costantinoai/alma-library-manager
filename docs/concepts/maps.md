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

A layout is built ahead of you, once, and then served:

- **Prepared before you ask.** A warm-up shortly after startup builds any
  layout that is missing or overdue, so the first visit to Map, Authors, or
  Discovery reads a finished artifact instead of starting the fit.
- **One fit at a time.** Automatic triggers — a cache miss, a tuning slider,
  the maintenance tick — queue behind a running build rather than starting a
  second one. An explicit **Rebuild layout** always runs.
- **A quiet build is a live build.** Long fits report through Activity and a
  liveness heartbeat, and the worker runs in its own detached process, so
  neither a silent clustering phase nor a backend restart cancels work that is
  still progressing.
- **Revalidation is free.** Layout responses carry a validator derived from the
  stored artifact's identity, so an unchanged layout answers "not modified"
  without the server re-reading or re-serializing the payload.

## Papers that arrive between rebuilds

A full fit is a rare, deliberate event, but papers gain vectors continuously. A
paper that arrives in between is placed **by interpolation**: its position is the
similarity-weighted mean of the coordinates of its nearest already-placed
neighbours. It lands where the papers it most resembles already sit, keeping the
structure inside a region rather than collapsing to that region's average.

Membership and position are decided **separately**, and the distinction matters:

- **Position** is always interpolated, for every paper.
- **Membership** must be earned. A paper joins a region only if it sits as close
  to that region's centre as the region's own weakest members do. A paper that
  does not clear that bar is labelled **Unclustered** — but it still keeps its
  interpolated position. Belonging to no named region is not the same as having
  no place, so an unclustered paper stays with the work it resembles instead of
  being parked in the middle of the plate.

Interpolation is an approximation, and the map says so: the legend reports how
many dots are **placed approximately**, and each carries its own provenance
(`layout` from the fit, `interpolated` in between). Measured against hidden
ground truth on the live corpus, interpolation lands **7× closer** than the
previous rule (median error 0.006 vs 0.043 of the plate), and among the papers
it does admit to a region it is also more accurate — because it declines to
guess on the ~12% it is unsure about. The next full rebuild makes every position
exact.

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
| Terrain | A smoothed live preference field from likes, loves, dislikes, saves, removals, and engine scores |

Cluster words and place names share one collision-aware label pass. Advanced
controls tune cluster detail, dot size, dot opacity, **Terrain opacity**, word
size, and words per cluster consistently across all hosts.

Terrain uses one fixed semantic scale, `-1` to `+1`, on every host. A weak
positive population cannot stretch its own small maximum to full green.
Terrain is a translucent overlay on the ordinary map-paper background; it does
not replace the whole plate with the ramp's yellow midpoint. The shared
opacity control changes only the overlay, never dots, labels, or positions.

### Terrain follows the layout

Terrain is a property of the space, not of the visible layer: toggling a layer
never changes the landscape, and a Library-only view still shows the colour of
territory whose dots are hidden.

When an Advanced control re-fits the layout — a non-default cluster detail, or
a layout blend that gives shared authorship, shared references, or co-citation
weight — the papers move to a new arrangement, and the terrain is redrawn in
that arrangement from the same live preference values. Only papers with a place
in the re-fitted layout can appear in it, and the legend says how many that is.

Signal Lab's learned region offset can bend paper-space Terrain at read time
when the feature is active. It never changes paper or author coordinates:
preference is an overlay on semantic meaning, not replacement geometry.

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
- Every popup can **Create lens from selection**. ALMa creates a collection,
  saves the selected in-scope papers into it, and creates a collection-backed
  Discovery lens in one transaction. Selecting authors resolves their papers
  in the current Library/Corpus scope.

Only dots in the current rendered payload can enter the lasso: a Library map
cannot leak Corpus papers, and changing scope or a Discovery layer clears a
stale selection. The gesture itself writes nothing. The explicit lens button
performs the collection, Library promotion, membership, and lens writes
atomically; a failure leaves none of them behind.

## Author placement

Papers and authors are two views of ONE territory, so they live on **one page**
behind a Papers / Authors switcher (Map, 2026-07-27) — same masthead, same
guide, same plate; only the substrate changes. The Authors page is
people-management: who you follow, who to follow next, and which identities
need a decision. The identity lookups both surfaces need (who is followed, who
is currently suggested) come from one hook, `useAuthorIdentity`, over the same
query caches, so the map and the page cannot disagree.

Authors do not get a second manifold projection. An author is eligible only
when at least two of their papers are already placed on the corpus paper
substrate; their coordinate is the centroid of those paper positions. This
gate runs before community clustering and payload construction.

Thin suggestions remain visible in the suggestion rail and omission count
until the background paper-seeding and embedding chain provides enough
evidence. ALMa never invents radial or rank-based coordinates.

Author preference uses the same per-paper valence hierarchy, then applies an
author-specific confidence aggregate. Explicit user signals carry full
evidence, engine-only scores carry `0.25` evidence mass, and a neutral prior has
the strength of two papers. One engine-ranked paper stays close to zero instead
of painting a person strongly green; repeated consistent user evidence
gradually overcomes the prior.
