# Signal Lab

The Signal Lab — the minigames that asked you to compare papers, the model
fitted from your answers, and the ranking heads it fed — is **not part of this
line of ALMa**. ALMa's core is Feed, Discovery and Library (product decision
D24); the Lab was extracted under **D25** so the core carries no game surface,
no fitted-model view and no additive taste heads on the score.

Nothing was lost:

- The complete implementation is preserved on the git branch
  `feature/signal-lab`, at its best measured state.
- Your answers are preserved. The `signal_lab_rounds` table and its migrations
  are left in place; nothing reads them here, and nothing deletes them.

## Why it left

It was measured before it was removed, and the measurements are the reason:

- **Nobody played it.** Every organic round was answered in a two-day window in
  late July 2026. None in the eight weeks that followed.
- **The fit was starved by design.** One three-way comparison carries roughly
  one bit of information. A few dozen rounds were being asked to fit several
  hundred free parameters — one offset per region, one per venue, plus a
  768-dimensional direction. An honest estimator reports silence on that ratio,
  and it did: the region head was silent by its own shrinkage, the author head
  had no reach, the utility head measured null-to-negative.
- **The instrument could not settle it either way.** ALMa's outcome evaluation
  resolves differences of about ±0.011 AUC, bounded by how many papers you have
  *rejected* — not by how much you played. Every head's effect was smaller than
  that. More rounds would not have changed the answer.

## What stayed

The shared machinery the Lab used but did not own is still here: the
beta-binomial shrinkage in `core/scoring_math.py`, the per-install scoring
calibration, and the outcome evaluation that fits family weights from what you
actually keep and reject.

The semantic partition and its regions left with it — see
[Maps](maps.md) for that history — because with the Lab gone they had no
consumer, and a clustering job that feeds nothing is not worth running.

## If it comes back

It would not come back in this shape. The design that would earn its place is
written down: three parameters instead of three hundred, matched pairs that
isolate one axis, questions chosen for how much they would actually teach, and
output scoped to a single Discovery branch rather than applied globally. That
plan lives with the task notes, not in the shipped docs.

What is already decided is where its output would go: into **branch-local**
ranking weights (**D26**), never a global taste term.
