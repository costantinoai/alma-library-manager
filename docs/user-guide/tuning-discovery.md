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

## What gets shown vs hidden

Two filters apply on top of all the ranking math, and only two:

* **Saved papers never reappear.** Once a paper is in your Library
  (`status='library'`), Discovery permanently excludes it. That's
  the canonical "you already have this" signal.
* **Dismissed and disliked papers never reappear.** Explicit negative
  actions block re-surfacing.

Everything else in your corpus is fair game on a refresh — including
papers your other workflows already pulled in but that you haven't
saved yet (status `tracked`). They might have been overshadowed by a
dominant author or topic the first time around; with new feedback
they can re-rank into the top-K and get a second look.

If the same paper keeps re-appearing and you don't want it to,
**Dismiss** it — that's the correct lever, not Save.

## Single-author dominance

If your library is heavily skewed toward one author (e.g. your PI),
you'll feel two things without intervention:

1. The author affinity signal would saturate around that one author
   and bury everyone else.
2. The "taste-author" external retrieval lane would fire explicit
   author-name searches at OpenAlex and Semantic Scholar, which just
   amplifies that same author back into the candidate pool.

Discovery counters both:

* **Log-prevalence affinity weighting.** A co-author appearing on 5
  of 100 saved papers used to score `0.05` against the dominant
  author's `1.0`; with log-prevalence they score around `0.4` —
  meaningful enough to compete on merit when other signals agree.
* **Dominant authors don't drive external queries.** Any author who
  appears on more than 40% of your saved papers is excluded from the
  taste-author lane's explicit search list. They still get full
  ranking credit through `author_affinity` on candidates pulled in
  by other lanes — they just stop being the explicit search query.
* **Per-author cap in the staged top-K.** No single first/last
  author is allowed more than two slots in a refresh. If the ranker
  produces three, the third moves to an overflow queue and is only
  shown if there are slots left after the rest of the top-K is
  filled.
* **Per-source-key cap.** No single external query (one taste-author,
  one taste-topic, etc.) can supply more than ~25% of the staged
  set. Forces lateral diversity across queries.

## Weights

Every signal that feeds the ranker is weighted, normalized to sum to
1.0, and configurable. **Settings → Discovery weights** exposes the
ten signals documented in [Scoring formulas](../reference/scoring.md).
The defaults give roughly:

| Signal | Default share |
|---|---|
| `topic_score` | ~17% |
| `text_similarity` (SPECTER2 + lexical blend) | ~17% |
| `source_relevance` | ~13% |
| `author_affinity` | ~13% |
| `recency_boost` | ~9% |
| `feedback_adj` | ~9% |
| `preference_affinity` | ~9% |
| `usefulness_boost` | ~5% |
| `journal_affinity` | ~4% |
| `citation_quality` | ~4% |

So a perfect SPECTER2 cosine of 1.00 contributes at most ~17% of the
final score — it influences the ranking but does not dominate it.
The other 83% comes from the eight other signals together. If you
want SPECTER2 to influence less, lower `weights.text_similarity`;
if you want it to dominate, raise it. The ranker re-normalizes
against the budget every refresh.

Common adjustments:

| Goal | Adjustment |
|---|---|
| More recent papers | Raise `recency_boost`. Or switch `recommendation_mode` to `explore`, which auto-multiplies `recency_boost` by 1.5×. |
| Less old-hit citation bias | Lower `citation_quality`. |
| Stronger influence from your ratings | Raise `feedback_adj` and `preference_affinity`. |
| Fewer same-author recommendations | Lower `author_affinity` (the diversity cap already prevents >2/author in the top-K, so usually no further tuning needed). |
| Less semantic dominance | Lower `weights.text_similarity`. |

The weights are stored in ALMa's `discovery_settings` store and apply
on the next lens refresh. Each lens can also carry its own override
that merges on top of the global defaults.

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
The "Why this surfaced" panel surfaces this as a **Found by N
sources** chip (in brand-blue) when at least 2 lanes independently
agreed — the chip is the *first* in the chip row because it's the
strongest "why" signal we have. The chip is informational — the
bonus is already in the score. It exists so you know *why* a card
outranked another that looked similar in the ten signals.

## Refreshing and visible cards

A lens refresh stages 50 cards on the page after all filters and
diversity caps. To keep the initial scroll focused, only the first
20 cards render by default — click *Show all 50 recommendations*
below the list to expand the rest. Switching lenses resets to the
curated 20.

The four retrieval lanes (lexical, vector, graph, external) each
emit their own row in the Activity panel under the parent
*Lens refresh* row, so you can see exactly which lane was slow or
which one failed without digging through one combined log stream.

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
| **Dismiss** | Hides the recommendation and records a stronger negative signal with slow cooldown. Repeat dismissals increase the penalty. |
| **Dislike** | Sets a 1-star rating and records a negative signal. The recommendation stays visible. |

Use **Dismiss** when the paper is wrong for the lens. Use **Dislike**
when you want to teach the ranker without making as strong a visibility
decision. Like and Love follow the same rule on the positive side:
they rate the paper but do not save or hide it.

## The tuning loop on `main`

There is no separate training page in the current public build. The
learning loop comes from the normal product surfaces:

* Save / Like / Love from Feed
* Save / Reading list / Like / Love from Discovery
* Dismiss / Dislike in Discovery
* Reading history
* Passive interaction signals such as abstract views and outbound clicks

That keeps the public product simpler while still letting Discovery
adapt over time.
