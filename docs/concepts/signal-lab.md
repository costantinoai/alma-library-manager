---
title: Signal Lab
description: Reversible calibration games that sharpen ranking and taste terrain without touching your Library.
---

# Signal Lab

Signal Lab is a first-class ALMa intelligence feature built from small
**calibration games**. It is not an integration plugin. Its daily interaction
lives on Home immediately above Inbox; its controls and evidence live under
**Settings → Intelligence → Signal Lab**.

Each round shows three papers and asks one cheap question. Answers train a
model that can sharpen paper scores and paper-map taste terrain. It is
**signal-only**: a round never changes Library membership, ratings, reading
state, feedback profiles, or semantic coordinates.

## The two rounds

| Game | Question | What it teaches |
|---|---|---|
| **Best / worst** | Which would you read first — and which would you skip? | What should score high: region taste offsets and a utility direction. |
| **Odd one out** | Which paper does not belong with the other two? | Relative semantic distance and region-boundary overrides. |

Always answerable with **Can't tell**; a skip records no verdict. Home queues a
deck of at least ten signed rounds, shows progress through it, and can fetch
another deck without leaving the page. Best/worst is the ordinary game; every
third UTC calendar day uses odd-one-out.

## The band on Home

The same band every other Home section is: a `PageSection` (chevron, icon,
title, count pill) whose one grey line is the question itself, over a
`PaperTileGrid` of the three papers. The verdicts ride each tile's `actions`
strip — the slot Home's Inbox already uses for triage, for the same reason:
this surface owns the decision and has nowhere to hand the paper off to. The
tiles carry no `href`; the paper is the subject of a question, not a link.

It stays about the height of one paper card, because it is a five-second ritual
above the Inbox, not a section you settle into.

The verdicts carry the app's own valence colours (the `PaperActionBar` tone
map): **success** for *Read first*, **critical** for *Skip*, and **accent** for
*Odd one out*, which is a categorical call rather than a good/bad one. The whole
cell takes the tint of the verdict it holds, so an answered round reads without
being parsed.

Best/worst shows both buttons on every paper, so both verdicts are visible at
once and each is independently revisable; the round records itself when both are
given, because the pair is the datum. Odd-one-out shows one button and records
on the single mark. Giving a paper one verdict lifts any verdict it holds
elsewhere: no paper is both the one you would read and the one you would skip.

At the foot, live evidence explains what the answers are doing: regions moving
up/down, boundaries getting sharper, rounds today/total, the fitted evidence
behind the current model, and region/edge coverage. This is evidence, not a
second diagnostics product — the knobs, the model and the eval live in
**Settings → Intelligence → Signal Lab**.

The ledger deliberately does **not** say “7 of 4.1 billion triplets”. For a
region with \(n\) papers, \(\binom{n}{3}\) correctly counts unordered
three-paper sets, but one giant region makes that number enormous and it says
nothing about learning. ALMa reports:

- recorded answers and skips;
- unique unordered query sets and accidental repeats;
- observations and constraints consumed by the current fit;
- rounds awaiting the next fit;
- super-regions and adjacent edges reached by answered rounds.

Paper order remains available for position-bias analysis, but does not create a
new statistical query.

## Where rounds come from

Rounds sample the corpus map's **super-regions**, starting around your Library
and expanding outward as inner regions are learned. Only judgeable papers
appear: a title plus an abstract or TLDR.

One request designs the whole 10–30-round sheet:

1. ALMa loads every eligible paper in every super-region. There is no
   first-800 SQL prefix. Region priority combines the Library-outward ring
   prior, sub-linear affected mass, current posterior disagreement, coverage,
   answer/skip rate, elapsed-time staleness, and a protected ring-uniform
   exploration share.
2. Best/worst candidates are sampled across the complete chosen region.
   Odd-one-out candidates combine low-margin papers on an adjacent edge with a
   25% broad-pool escape route, so a mistaken current boundary cannot trap the
   sampler.
3. With fitted bootstrap heads, ALMa evaluates all six ordered MaxDiff outcomes
   or all three odd-one-out outcomes and scores their expected information
   gain. Cold start uses a D-optimal contrast-geometry score. Best/worst also
   favours comparisons whose posterior ordering can still change.
4. Later sheet rows are chosen conditionally: a small posterior-member
   log-determinant gain discourages redundant directions, while paper,
   super-region, and edge exposure penalties spread attention. This is
   BatchBALD-style batch diversity, not a claim that the response outcomes are
   independent.
5. The same unordered query cannot repeat within a sheet or within the latest
   500 recorded answers/skips. Repeated-measure trials require a future,
   explicit noise-estimation policy; reloads never create them accidentally.

This follows the core ideas behind
[BALD](https://arxiv.org/abs/1112.5745) and
[BatchBALD](https://arxiv.org/abs/1906.08158), adapted to ALMa's hierarchical
region/edge goals and multi-outcome games. It is goal-directed active design,
not a promise that one heuristic wins on every corpus.

The card remains hidden until the stored `graph:super_regions` view exists.
Graph-layout maintenance builds it, or an explicit graph rebuild can trigger
the chain.

## Model and effects

Every answer writes exactly one `signal_lab_rounds` row. The
`signal_lab:model` materialized view is recomputed wholesale from that history;
there is no incremental accumulator.

The model has:

- James–Stein-shrunk per-region taste offsets;
- a Bradley–Terry utility direction with a bootstrap ensemble;
- non-negative diagonal semantic-distance weights, shrunk strongly toward the
  identity metric, with a bootstrap ensemble;
- region-boundary overrides from odd-one-out votes;
- held-out pairwise and metric accuracy for the fitted heads.

Query tokens carry a nonce and bind the game, papers, and region context. An
answer retry with the same nonce and content returns the existing row; a
different answer for an already-used nonce is rejected. Presentation writes
nothing. The first valid answer writes one row.

The region head can bend paper-space Terrain at read time. The utility head
stores only its delta from the Library prior, projects that direction onto
super-region centroids, then mass-centres the projection so weak evidence
cannot wash the whole map green. Confidence grows with answered preferences.
Neither head moves map positions: semantic coordinates describe what papers
mean, while learned offsets describe how you feel about that territory. Tint
strength is bounded, and adjusted terrain remains in the canonical
`[-1,+1]` domain.

Ranking terms are separately promotion-gated.
`weights.lab_region_offset` and `weights.lab_utility` default to zero; when
promoted they add bounded points through the same scorer used by Discovery and
Feed. Each term is limited to 2.5 points, so the combined game nudge stays in
the same small additive evidence band as sibling scoring sources. Settings
shows the held-out and churn evidence used to decide.

## Disable is not purge

The **Active** switch is reversible. When off:

- Home serves no game and the answer endpoint rejects writes;
- Discovery, Feed, and maps do not load or apply the retained model;
- rounds, fitted heads, metrics, and settings remain untouched.

Re-enabling makes that retained evidence consumable again.

**Purge signals** is separate, destructive, and explicit. It deletes all round
rows and invalidates the model in one write unit. Library, ratings, ordinary
feedback, activation, and knobs remain unchanged.

## API

```text
GET  /api/v1/signal-lab/games
GET  /api/v1/signal-lab/settings
PUT  /api/v1/signal-lab/settings
GET  /api/v1/signal-lab/{game}/queue?count=12
POST /api/v1/signal-lab/{game}/round/answer
GET  /api/v1/signal-lab/summary
GET  /api/v1/signal-lab/model
GET  /api/v1/signal-lab/eval
POST /api/v1/signal-lab/purge
```

The settings model strictly validates activation, map tint, promotion points,
ring decay, exploration, coverage, refit cadence, holdout share, and override
votes.

## Evaluation evidence

`scripts/simulate_signal_lab.py` drives the shipping full-outcome EIG and
wholesale-fit primitives against stratified-random and margin baselines.
`--corpus` uses a seeded 1,000-paper sample of the real super-region geometry
and one seeded 768→64 Gaussian projection so repeated fits stay bounded;
production always uses the full embeddings.

The checked-in corpus report is `tasks/54_stage0_report.json`. At 200 answered
rounds, EIG reached 0.6525 pairwise accuracy versus 0.6250 for
stratified-random and passed the predeclared +2-point late gate; it did not beat
random in the early checkpoints. That mixed result is kept visible: it
supports posterior-aware acquisition as one component, not EIG-only selection.
The runtime also uses ring/edge goals, answerability, staleness, protected
exploration, cold-start design, and conditional sheet diversity, which this
small utility-only simulator does not model.

Production cost is much smaller than the simulator because a user request does
not replay policies or refit hundreds of histories. On the 2026-07-27
full-vector benchmark (768 dimensions), an exact 2,000-paper corpus with 200
recorded rounds built a 12-round posterior-aware sheet in **1.278 s wall /
3.954 CPU-seconds**. One 200-round wholesale fit took **0.674 s wall /
8.330 CPU-seconds** and runs in the background every five answers. These are
brief multi-core bursts, not a sustained simulation.
