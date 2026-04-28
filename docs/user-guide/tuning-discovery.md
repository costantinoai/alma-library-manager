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
4. Refresh after making changes. Weights and actions affect the next
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
