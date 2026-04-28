---
title: Saving papers
description: When to Save vs Like vs Love vs Dislike, and how saving relates to rating.
---

# Saving papers

Anywhere you see a paper card in ALMa — Feed, Discovery, Authors,
Online search, Library — you'll find the same four rating verbs:

| Verb | Effect |
|---|---|
| **Save** (or **Add**) | Adds the paper to your Library at rating 3 (neutral). |
| **Like** | Adds to Library at rating 4 (positive). |
| **Love** | Adds to Library at rating 5 (strongly positive). |
| **Dislike** | Records a negative signal at rating 1. **Does not** save the paper unless it was already saved (in which case the existing rating is preserved — see below). |

The vocabulary is identical across surfaces because the underlying
operation is the same. The recommender treats a Like in Feed and a
Like in Discovery identically.

## When to use which

* **Save** — "this seems worth keeping; I'll triage later." The
  workhorse verb.
* **Like** — "this is good; I want the recommender to surface more
  like it."
* **Love** — "this is exemplary; treat its authors / topics / venue
  as strong positive signals."
* **Dislike** — "do not surface this kind of paper again." Used in
  Discovery to teach the recommender what's noise; used in Feed to
  triage without deleting.

## Monotonic upgrade

Re-saving never downgrades. If you've Loved a paper (rating 5) and
later click Save on the same row, the rating stays at 5. The only
ways to lower a rating are:

* Explicit Dislike → sets rating to 1.
* Manual rating change in the Library star control.

This is intentional: the system trusts your explicit positive
signals and treats the implicit "Save again" action as
re-engagement, not as a re-grade.

## Dislike on a saved paper

If you Dislike a paper that's **already in your Library**, ALMa:

* Records a `feedback_events` row with negative signal.
* **Does not** demote your Library entry.
* **Does not** remove it from Library.

The existing surface lifecycle (Library remove = explicit intent)
is the only thing that downgrades a saved entry's status.

If you want to actually remove a paper from your Library, use
**Remove** on the Library row (which transitions it to
`status='removed'`, see [Paper lifecycle](../concepts/paper-lifecycle.md#membership-axis)).

## Discovery's two negative verbs

Discovery has two ways to express "no":

| Verb | Effect |
|---|---|
| **Dismiss** | Hides the card from the lens **and** writes a negative signal. The recommender will not re-suggest the paper in this lens. |
| **Dislike** | Writes a negative signal but keeps the card visible. Useful when you want the system to learn but want to keep evaluating the paper. |

Feed has only **Dislike**. The Feed is chronological — it does not
hide things — so a Dismiss verb wouldn't make sense there.

## Saving from Online search

The Online search panel inside the Import dialog is also a save
surface. Same vocabulary, same monotonic rule, same Dislike
semantics on already-saved papers. Internally these flow through
`alma.application.openalex_manual.save_online_search_result`, the
canonical helper that all surfaces use.

## Bulk

In the Library Saved tab, select multiple rows to use the bulk
actions in the floating toolbar:

* **Add to collection**
* **Remove from Library**
* **Clear rating** (sets rating to 0; paper stays saved)

Bulk operations are atomic per row; partial failures are reported
in the toast.
