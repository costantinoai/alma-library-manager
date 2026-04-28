---
title: Reading workflow
description: Use the Reading list independently of saving. Mark papers as reading, done, or excluded.
---

# Reading workflow

Reading state is **independent of saving**. A paper can be saved to your
Library without entering the Reading list, and a paper can be in the
Reading workflow without changing its rating.

See [Paper lifecycle](../concepts/paper-lifecycle.md) for the underlying
model.

## States

| State | Meaning | Where it surfaces |
|---|---|---|
| *(none)* | No reading intent set. | Saved tab and normal library views. |
| `reading` | You're actively reading it. | Reading list tab and the landing-page summary. |
| `done` | You've finished it. | Reading history. |
| `excluded` | You evaluated it and decided not to read it. | Reading history and Library filters. |

## How to set reading state

* On any paper card or row, use the **Reading status** control.
* In bulk, select rows in the Saved tab and use the bulk action menu.
* You do not need to change the paper's rating to move it through the
  reading workflow.

## The Reading list tab

Library → Reading list groups papers by their current reading status:

* **Reading**
* **Done**
* **Excluded**

This keeps active reading work separate from long-term curation.

## Why the axes are separate

ALMa keeps three ideas apart on purpose:

* **Saved**: I want to keep this paper in my curated collection.
* **Rated**: I think this paper is good, bad, or neutral as a signal.
* **Reading**: I am reading this paper, have finished it, or decided not to.

Tools that collapse those ideas into one list usually end up with a
library full of ambiguous rows. ALMa keeps each control honest by
mapping it to one state dimension only.

## Done is not Excluded

These two look similar because both leave the active reading queue:

* **Done** means you actually read the paper.
* **Excluded** means you decided not to.

Neither one removes the paper from the Library. That decision belongs
to the membership axis, not the reading axis.
