---
title: Tuning Discovery
description: How to improve recommendation quality with ratings, dismissals, weights, branch controls, and lens scope.
---

# Tuning Discovery

Discovery is designed to improve as you curate. It is normal for a new
library to feel noisy at first. The fastest path to better results is to
give the recommender clearer evidence.

## Quick wins

1. Save more papers. Discovery is data-starved with a tiny library.
2. Use **Like** and **Love** deliberately. Those ratings carry more
   positive weight than a neutral save.
3. Dismiss obviously wrong recommendations so the lens stops recycling
   them.
4. Use **Dislike** when the paper should stay visible in normal flows
   but should teach the ranker "less like this".
5. Refresh after making changes. Weights and actions affect the next
   refresh, not already-materialized rows.

## Weights

**Settings → Discovery weights** exposes the hybrid ranking signals
documented in [Scoring formulas](../reference/scoring.md).

Common adjustments:

| Goal | Adjustment |
|---|---|
| More recent papers | Raise `recency_boost`. |
| Less old-hit citation bias | Lower `citation_quality`. |
| Stronger influence from your ratings | Raise `feedback_adj` and `preference_affinity`. |
| Fewer same-author recommendations | Lower `author_affinity`. |

The weights are stored in ALMa's `discovery_settings` store and apply on
the next lens refresh.

## What Feedback Teaches

Paper feedback is projected onto the surrounding scholarly graph:

| Signal you give | Ranking effect |
|---|---|
| Like / Love / high rating | Raises the paper, its main authors and co-authors, dominant topics, venue, keywords, tags, close semantic neighbours, and local citation neighbours. |
| Dislike / Dismiss / low rating | Lowers the same connected signals without deleting papers or unfollowing authors. |
| Follow author | Adds a positive author signal to Discovery and weakly boosts that author's profile. |
| Dismiss / remove author | Adds a negative author signal to Discovery and weakly lowers that author's profile. |
| Repeated feedback in one area | Accumulates into stronger topic / venue / author priors, decayed over time. |

Author suggestions listen to the same paper-feedback projection. If
you love papers by an author, that author and nearby candidates get a
reasonable bump. If you dismiss papers from a topic or venue, authors
connected to that pattern lose rank unless other evidence outweighs it.

## Multi-source consensus

A candidate found by more than one retrieval lane gets a small,
diminishing-returns score bonus on top of its weighted-signal score.
The "Why this surfaced" panel surfaces this as a **Suggested by N
sources** chip when at least 2 lanes independently agreed. The chip
is informational — the bonus is already in the score. It exists so
you know *why* a card outranked another that looked similar in the
ten signals.

## Outcome calibration

ALMa quietly tracks whether each retrieval source's recommendations
end up Saved versus Dismissed and reweights the source on subsequent
refreshes. A source where dismisses dominate gets pulled toward 0.5×;
a source where saves dominate gets pushed toward 1.5×. Three axes
calibrate independently — the API the candidate came from, the lane
mode (`core`/`explore`/`safe`), and the specific branch — composed
multiplicatively so a single hot axis can't push past the per-axis
ceiling. On a fresh DB the multiplier is 1.0 (no behavior change);
the system warms up over a few weeks of normal use. The score
breakdown carries `source_calibration_multiplier` and per-axis
components so you can audit what moved.

## Retrieval strategies

The Settings card also lets you toggle retrieval lanes on and off.
Current strategies include:

* `related_works`
* `topic_search`
* `followed_authors`
* `coauthor_network`
* `citation_chain`
* `semantic_scholar`
* `branch_explorer`
* `taste_topics`
* `taste_authors`
* `taste_venues`
* `recent_wins`

Disabling a strategy removes it from the candidate pool entirely.

## Branch controls

Inside a lens, Branch view gives you the sharpest local controls:

* **Pin** — keep this branch important
* **Boost** — increase its weight
* **Mute** — suppress it
* **Cool / reset** — clear earlier tuning

These controls feed into the next refresh of that lens.

## Dismiss vs Dislike

| Action | Effect |
|---|---|
| **Dismiss** | Hides the recommendation from that lens and records a negative signal. |
| **Dislike** | Records a negative signal without using the same hide semantics. |

Use **Dismiss** when the paper is wrong for the lens. Use **Dislike**
when you want to teach the ranker without making as strong a visibility
decision.

## The tuning loop on `main`

There is no separate training page in the current public build. The
learning loop comes from the normal product surfaces:

* Save / Like / Love from Feed or Discovery
* Dismiss / Dislike in Discovery
* Reading history
* Passive interaction signals such as abstract views and outbound clicks

That keeps the public product simpler while still letting Discovery
adapt over time.
