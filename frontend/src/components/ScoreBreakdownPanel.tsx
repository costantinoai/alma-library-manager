import * as React from 'react'
import { ChevronRight } from 'lucide-react'

import type {
  ScoreAdjustment,
  ScoreAdjustmentAtom,
  ScoreAtom,
  ScoreBreakdown,
  ScoreFamily,
} from '@/api/client'
import { EyebrowLabel } from '@/components/ui/eyebrow-label'
import { Meter } from '@/components/ui/meter'
import { StatusBadge } from '@/components/ui/status-badge'
import { SubPanel } from '@/components/ui/sub-panel'
import { FAMILY_COLORS, SIGNAL_FALLBACK_COLOR } from '@/lib/palette'
import {
  contributingFamilies,
  isFamilyDegraded,
  scoreExplanation,
  SOURCE_TYPE_LABELS,
} from '@/lib/signals'
import { cn } from '@/lib/utils'

/**
 * ScoreBreakdownPanel — THE explanation of a paper's score, on every surface.
 *
 * Shows only, and all of, what produced the number: the ranking families that
 * contributed, any bounded adjustment (a retraction, which only ever subtracts;
 * signed adjustments, which can lift a paper as well as sink
 * it), and the clipping term. Those sum to the final score exactly — the
 * backend guarantees that invariant
 * (`ranker.repaired_prior_score`) and this panel renders it rather than
 * computing a parallel decomposition of its own.
 *
 * That is not a stylistic preference. Until 2026-07-28 the card drew nine bars
 * decomposing a composite the ranker had already discarded, beside a final
 * score computed from a different feature set with different weights — a
 * picture of a calculation nobody was running.
 *
 * Colour is IDENTITY (which family), never valence: a large red `feedback` bar
 * is your own strong approval, not a warning. Adjustment rows are the one
 * exception, because an adjustment has no identity hue — there its colour IS
 * the sign (critical below zero, success above), which is what the number
 * already says.
 */

/** One decimal is the resolution at which a gap between two families means
 *  something; more digits invite reading noise as signal. */
function points(value: number): string {
  return value.toFixed(1)
}

function familyColor(key: string): string {
  return FAMILY_COLORS[key] ?? SIGNAL_FALLBACK_COLOR
}

/** One measured input, inside an expanded family.
 *
 * Grouped atoms share a single payment, so a measured one can still contribute
 * nothing. Those stay visible but dimmed and tagged: seeing that the exemplar
 * similarity was passed over for the centroid is exactly the diagnostic worth
 * having, and hiding it would leave the family's value looking unexplained.
 *
 * Whether an atom was paid comes from the scorer (`counted`) rather than being
 * recomputed here. It used to be re-derived in this component as "the highest
 * value in the group wins", which was true of every group until one began
 * paying all of its measured members — at which point a duplicated rule would
 * have started drawing "lost" beside a value that did reach the score.
 */
function AtomRow({ atom }: { atom: ScoreAtom }) {
  // `counted` is absent on rows persisted by an older ranker; fall back to the
  // rule that held when they were written.
  const counted = atom.counted ?? atom.role !== 'max'
  const spent = atom.available && counted
  return (
    <div className="flex items-baseline gap-2 py-0.5 text-[11px]">
      <span
        className={cn('min-w-0 flex-1 truncate', spent ? 'text-slate-600' : 'text-slate-400')}
        title={atom.label}
      >
        {atom.label}
      </span>
      {atom.role === 'penalty' && (
        <span className="shrink-0 font-mono text-[10px] uppercase tracking-wide text-critical-600">
          −{atom.weight}×
        </span>
      )}
      {atom.role === 'max' && atom.available && !counted && (
        <span
          className="shrink-0 font-mono text-[10px] uppercase tracking-wide text-slate-500"
          title="Measured, but another member of its group was paid instead — one piece of evidence is only counted once."
        >
          not used
        </span>
      )}
      <span
        className={cn(
          'w-12 shrink-0 text-right font-mono tabular-nums',
          spent ? 'text-slate-700' : 'text-slate-400',
        )}
      >
        {atom.available ? atom.value.toFixed(2) : '—'}
      </span>
    </div>
  )
}

function FamilyRow({
  family,
  share,
  degraded,
}: {
  family: ScoreFamily
  share: number
  degraded: boolean
}) {
  const [open, setOpen] = React.useState(false)
  const expandable = family.atoms.length > 1
  const color = familyColor(family.key)

  return (
    <div>
      <button
        type="button"
        onClick={
          expandable
            ? (event) => {
                // The panel lives inside a clickable paper card. Without this,
                // expanding a family also opens the paper detail popup.
                event.stopPropagation()
                setOpen((prev) => !prev)
              }
            : undefined
        }
        aria-expanded={expandable ? open : undefined}
        disabled={!expandable}
        title={family.description}
        className={cn(
          'flex w-full items-center gap-2 rounded-sm px-1.5 py-1 text-left',
          expandable && 'hover:bg-control-quiet-hover',
        )}
      >
        <ChevronRight
          className={cn(
            'h-3 w-3 shrink-0 text-slate-500 transition-transform',
            !expandable && 'invisible',
            open && 'rotate-90',
          )}
          aria-hidden
        />
        <span className={cn('h-2.5 w-2.5 shrink-0 rounded-sm', color)} aria-hidden />
        <span className="flex min-w-0 flex-1 items-center gap-1.5">
          <span className="truncate text-xs font-medium text-slate-700">{family.label}</span>
          {family.imputed && (
            <StatusBadge
              tone="neutral"
              size="sm"
              title={`Not measured for this paper, so it was scored at the corpus average (${Math.round(
                (family.prior_mean ?? 0) * 100,
              )}%). Unknown neither helps nor hurts.`}
            >
              estimated
            </StatusBadge>
          )}
          {degraded && (
            <StatusBadge tone="warning" size="sm">
              keyword
            </StatusBadge>
          )}
        </span>
        {/* The arithmetic, legibly. This rendered at `text-slate-300` on cream
            until 2026-07-28: present in the DOM, invisible on screen. */}
        <span className="hidden shrink-0 font-mono text-[11px] tabular-nums text-slate-500 sm:inline">
          {Math.round(family.value * 100)}% × {family.weight.toFixed(2)}
        </span>
        <span className="w-14 shrink-0">
          <Meter value={Math.round(share * 100)} fillClassName={color} size="sm" decorative />
        </span>
        <span className="w-9 shrink-0 text-right font-mono text-xs font-semibold tabular-nums text-slate-800">
          {points(family.points)}
        </span>
      </button>
      {open && (
        <div className="mb-1 ml-6 border-l border-edge-2 pl-3">
          {family.atoms.map((atom) => (
            <AtomRow key={atom.key} atom={atom} />
          ))}
        </div>
      )}
    </div>
  )
}

/** Signed points, with an explicit `+` so a lift reads as a lift. */
function signedPoints(value: number): string {
  return value > 0 ? `+${points(value)}` : points(value)
}

/** One signed head input, inside an expanded adjustment: the reading, the
 *  ceiling it was scaled by, and the points that produced. Same shape as
 *  `AtomRow`, without the family-only `max` / `penalty` tags — an adjustment
 *  atom is always paid, so there is nothing to mark as used or lost. */
function AdjustmentAtomRow({ atom }: { atom: ScoreAdjustmentAtom }) {
  const tone = atom.available ? 'text-slate-600' : 'text-slate-400'
  return (
    <div className="flex items-baseline gap-2 py-0.5 text-[11px]" title={atom.description}>
      <span className={cn('min-w-0 flex-1 truncate', tone)} title={atom.label}>
        {atom.label}
      </span>
      <span className={cn('w-12 shrink-0 text-right font-mono tabular-nums', tone)}>
        {atom.available ? atom.value.toFixed(2) : '—'}
      </span>
      <span className="shrink-0 font-mono text-[10px] text-slate-500">
        × {atom.weight.toFixed(1)}
      </span>
      <span
        className={cn(
          'w-9 shrink-0 text-right font-mono tabular-nums',
          atom.available ? 'text-slate-700' : 'text-slate-400',
        )}
      >
        {atom.available ? signedPoints(atom.points) : '—'}
      </span>
    </div>
  )
}

/** A post-family adjustment. Coloured by SIGN — a positive nudge
 *  drawn in red would claim a lift was a loss. Expands to its atoms when the
 *  ranker sent any (older persisted rows carry none). */
function AdjustmentRow({ adjustment }: { adjustment: ScoreAdjustment }) {
  const [open, setOpen] = React.useState(false)
  const atoms = adjustment.atoms ?? []
  const expandable = atoms.length > 0
  const tone = adjustment.points < 0 ? 'text-critical-700' : 'text-success-700'

  return (
    <div>
      <button
        type="button"
        onClick={
          expandable
            ? (event) => {
                // Same reason as FamilyRow: the panel sits inside a clickable
                // paper card, and expanding is not a request to open it.
                event.stopPropagation()
                setOpen((prev) => !prev)
              }
            : undefined
        }
        aria-expanded={expandable ? open : undefined}
        disabled={!expandable}
        title={adjustment.description}
        className={cn(
          'flex w-full items-center gap-2 rounded-sm px-1.5 py-0.5 text-left text-xs',
          expandable && 'hover:bg-control-quiet-hover',
        )}
      >
        <ChevronRight
          className={cn(
            'h-3 w-3 shrink-0 text-slate-500 transition-transform',
            !expandable && 'invisible',
            open && 'rotate-90',
          )}
          aria-hidden
        />
        <span className={cn('flex-1 truncate font-medium', tone)}>{adjustment.label}</span>
        <span className={cn('w-9 shrink-0 text-right font-mono font-semibold tabular-nums', tone)}>
          {signedPoints(adjustment.points)}
        </span>
      </button>
      {open && (
        <div className="mb-1 ml-6 border-l border-edge-2 pl-3">
          {atoms.map((atom) => (
            <AdjustmentAtomRow key={atom.key} atom={atom} />
          ))}
        </div>
      )}
    </div>
  )
}

export interface ScoreBreakdownPanelProps {
  breakdown?: ScoreBreakdown | null
  /** Optional prose "why this surfaced", shown above the arithmetic. */
  explanation?: string | null
  className?: string
}

export function ScoreBreakdownPanel({
  breakdown,
  explanation,
  className,
}: ScoreBreakdownPanelProps) {
  const decomposition = scoreExplanation(breakdown)
  const families = contributingFamilies(decomposition)
  const unmeasured = (decomposition?.families ?? []).filter((family) => !family.available)
  const adjustments = (decomposition?.adjustments ?? []).filter(
    (adjustment) => adjustment.points !== 0,
  )
  const clipped = decomposition?.clipped ?? 0
  const maxPoints = Math.max(...families.map((family) => family.points), 0.01)

  if (!decomposition) {
    return (
      <div className={cn('space-y-2 py-2', className)}>
        {explanation && <p className="text-xs italic text-slate-600">{explanation}</p>}
        <p className="text-xs text-slate-500">
          No score breakdown stored for this paper — it was ranked before the current
          ranker, and will gain one on the next refresh.
        </p>
      </div>
    )
  }

  const sourceLabel = breakdown?.source_type
    ? SOURCE_TYPE_LABELS[breakdown.source_type] ?? breakdown.source_type
    : null

  return (
    <SubPanel
      padded={false}
      className={cn('space-y-3 p-3', className)}
      // The panel is rendered inside a clickable paper card. Reading the
      // breakdown is not a request to open the paper, so no click inside it
      // reaches the card.
      onClick={(event) => event.stopPropagation()}
    >
      <div className="flex items-center justify-between gap-2">
        <EyebrowLabel tone="muted">What made this score</EyebrowLabel>
        {sourceLabel && (
          <StatusBadge tone="neutral" size="sm">
            {sourceLabel}
          </StatusBadge>
        )}
      </div>

      {explanation && (
        <p className="border-b border-edge-2 pb-2 text-xs italic leading-relaxed text-slate-600">
          {explanation}
        </p>
      )}

      {/* Composition ribbon: each segment is a family's real contribution, so
          the whole bar IS the final score, not a normalised likeness of it. */}
      <Meter
        segments={families.map((family) => ({
          value: family.points,
          fillClassName: familyColor(family.key),
        }))}
        className="h-3 rounded-sm ring-1 ring-control-edge"
        label={`Score composition: ${families
          .map((family) => `${family.label} ${points(family.points)}`)
          .join(', ')}`}
      />

      <div className="space-y-0.5">
        {families.map((family) => (
          <FamilyRow
            key={family.key}
            family={family}
            share={family.points / maxPoints}
            degraded={isFamilyDegraded(family.key, breakdown ?? undefined)}
          />
        ))}
      </div>

      {(adjustments.length > 0 || clipped !== 0) && (
        <div className="space-y-0.5 border-t border-edge-2 pt-2">
          {adjustments.map((adjustment) => (
            <AdjustmentRow key={adjustment.key} adjustment={adjustment} />
          ))}
          {clipped !== 0 && (
            <div
              className="flex items-center gap-2 px-1.5 text-xs"
              title="The parts summed past the end of the 0–100 band, so the total was capped."
            >
              <span className="flex-1 text-slate-600">Capped to the 0–100 band</span>
              <span className="w-9 text-right font-mono tabular-nums text-slate-600">
                {points(clipped)}
              </span>
            </div>
          )}
        </div>
      )}

      <div className="flex items-center justify-between gap-2 border-t border-gold-300/50 px-1.5 pt-2">
        <span className="text-xs font-medium text-slate-700">Final score</span>
        <span className="font-brand text-base font-semibold tabular-nums text-alma-900">
          {decomposition.final_score.toFixed(1)}
        </span>
      </div>

      {unmeasured.length > 0 && (
        <p className="px-1.5 text-[11px] leading-relaxed text-slate-500">
          <span className="font-medium text-slate-600">Estimated:</span>{' '}
          {unmeasured.map((family) => family.label).join(', ')} could not be measured for
          this paper, so each was scored at its corpus average. Not knowing something
          neither helps nor hurts, and every paper is scored on the same ten families —
          so this score is directly comparable with any other.
        </p>
      )}
    </SubPanel>
  )
}

/** Compact top-N summary for hover previews and card teasers.
 *  Reads the same explanation as the full panel, so the two cannot disagree. */
export function ScoreBreakdownTeaser({
  breakdown,
  explanation,
  limit = 3,
}: {
  breakdown?: ScoreBreakdown | null
  explanation?: string | null
  limit?: number
}) {
  const families = contributingFamilies(scoreExplanation(breakdown)).slice(0, limit)

  if (families.length === 0 && !explanation) {
    return <p className="text-xs text-slate-500">No score breakdown for this paper.</p>
  }

  return (
    <div className="space-y-2">
      <EyebrowLabel tone="muted">Why this paper</EyebrowLabel>
      {explanation && (
        <p className="text-xs italic leading-relaxed text-slate-600">{explanation}</p>
      )}
      {families.length > 0 && (
        <ul className="space-y-1.5">
          {families.map((family) => (
            <li key={family.key} className="flex items-center justify-between gap-3 text-xs">
              <span className="flex items-center gap-1.5 text-slate-700">
                <span
                  className={cn('inline-block h-2 w-2 rounded-full', familyColor(family.key))}
                  aria-hidden
                />
                {family.label}
              </span>
              <span className="font-mono tabular-nums text-slate-600">
                {points(family.points)}
              </span>
            </li>
          ))}
        </ul>
      )}
      <p className="border-t border-edge-2 pt-1.5 text-[11px] text-slate-500">
        Click for the full breakdown
      </p>
    </div>
  )
}
