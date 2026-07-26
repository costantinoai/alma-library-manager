---
title: Signal Lab
description: Reversible calibration minigames that sharpen ranking and region boundaries without ever touching your Library.
---

# Signal Lab

Signal Lab is a layer of small **calibration minigames**. Each round shows
three papers and asks one cheap question; the answers train a model that can
sharpen paper scores and the map's region boundaries. It is **signal-only**:
a round never changes Library membership, ratings, or reading state.

## The two rounds

| Game | Question | What it teaches |
|---|---|---|
| **Best / worst** | Which would you read first — and which would you skip? | What should score high (region taste offsets + a utility direction). |
| **Odd one out** | Which paper doesn't belong with the other two? | What counts as *near* — region boundaries, the thing neighbour suggestions run on. |

Always answerable with **"Can't tell"** — a skip records no verdict, ever.
One round per Home visit; dismissible for the day.

## Where rounds come from

Rounds sample the corpus map's **super-regions**, starting around your
Library and expanding outward as inner regions are learned (a competence
gate, not a timer). 20% of rounds explore uniformly so the model can never
become self-confirming. Only judgeable papers appear: a title plus an
abstract or TLDR.

## The model, and when it matters

Everything derived from your answers lives in one fitted model with three
heads: per-region taste offsets, a utility direction, and region-boundary
overrides. **None of it affects ranking until promoted**: the
`weights.lab_region_offset` / `weights.lab_utility` settings default to 0,
and Settings → Intelligence → Signal Lab shows the promotion evidence —
held-out accuracy per head, and what a hypothetical promotion would change
in your current top suggestions.

## One table, honest purge

The only durable state is the round history (`signal_lab_rounds`); the model
is recomputed from it wholesale. **Purge Signal Lab** (Settings →
Intelligence) deletes the rounds and the model in one transaction — total,
immediate, irreversible — and touches nothing else: your Library, ratings,
and feedback history stay exactly as they were.

## API

```
GET  /api/v1/signal-lab/games                  # the explicit roster
GET  /api/v1/signal-lab/{game}/round           # one round (no writes)
POST /api/v1/signal-lab/{game}/round/answer    # one signed answer
GET  /api/v1/signal-lab/model                  # fitted heads + metrics
GET  /api/v1/signal-lab/eval                   # promotion evidence
POST /api/v1/signal-lab/purge                  # delete everything
```

Tuning knobs live in discovery settings under `signal_lab.*`
(exploration share, ring decay, coverage target, refit debounce, holdout
share, override votes).
