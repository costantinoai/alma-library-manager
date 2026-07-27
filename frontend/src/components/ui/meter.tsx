import * as React from 'react'
import { cn } from '@/lib/utils'

/**
 * Meter — THE horizontal bar. Score bars, coverage bars, signal-component
 * bars, the Health vitals ribbon.
 *
 * Before this primitive a dozen components hand-rolled the same
 * `overflow-hidden rounded-full` rail plus an inner fill, and they had
 * drifted into four different rail colours (`parchment-200`,
 * `parchment-200/70`, `surface-2`, `alma-100`), three heights, a mix of
 * `<div>` and `<span>`, and inconsistent a11y — some announced a value,
 * most were invisible to a screen reader.
 *
 * The rail is `control-track`: a translucent ink wash, not a step on the
 * paper ladder, so the bar reads identically on a card, a panel, a popover
 * or the desk (see "controls are ink, surfaces are paper" in index.css).
 *
 * Two shapes:
 *   <Meter value={62} tone="success" label="62% coverage" />
 *   <Meter segments={[{ value: 3, tone: 'critical' }, …]} label="…" />
 *
 * `segments` divides the rail proportionally — use it when the bar shows a
 * BREAKDOWN (how the whole splits) rather than a level. Values are treated
 * as relative weights, so callers pass raw counts.
 */
export type MeterTone =
  | 'accent'
  | 'success'
  | 'warning'
  | 'critical'
  | 'info'
  | 'neutral'
  /** Present but weak — a filled bar that deliberately doesn't claim attention. */
  | 'muted'

/**
 * Semantic tone → fill.
 *
 * ONE step for every tone — `-600` — instead of the old mix of `-600`
 * (success), `-500` (warning, critical, info) and `alma-500`, which made three
 * bars sitting in the same list read as three different weights of statement.
 *
 * `-600` and not the chips' `-700`: a chip is a wash behind TEXT, whereas a
 * bar is 4–6px of bare colour and nothing else, so it has to hold the hue on
 * its own. The score bars on a paper card are the test — at `-700` the
 * red/amber/green went muddy and stopped reading as a traffic light, which is
 * their entire job (user report 2026-07-27).
 *
 * Categorical fills (per-signal, per-branch, per-Home-category) have no token:
 * those callers pass `fillClassName` sourced from `lib/palette.ts`.
 */
const TONE_FILL: Record<MeterTone, string> = {
  accent: 'bg-alma-folio',
  success: 'bg-success-600',
  warning: 'bg-warning-600',
  critical: 'bg-critical-600',
  info: 'bg-info-600',
  // Neutral and muted stay on the ink ramp: they are the ABSENCE of valence,
  // and a 700-weight grey rail would out-shout the coloured ones.
  neutral: 'bg-alma-500',
  muted: 'bg-alma-300',
}

const SIZE_RAIL: Record<'xs' | 'sm' | 'md', string> = {
  xs: 'h-1',
  sm: 'h-1.5',
  md: 'h-2',
}

export interface MeterSegment {
  /** Relative weight of this segment (raw counts are fine). */
  value: number
  tone?: MeterTone
  /** Categorical fill from `lib/palette.ts`, when no semantic tone applies. */
  fillClassName?: string
}

export interface MeterProps extends Omit<React.HTMLAttributes<HTMLDivElement>, 'children'> {
  /** Level, 0–100. Ignored when `segments` is given. */
  value?: number
  tone?: MeterTone
  /** Categorical fill from `lib/palette.ts`, when no semantic tone applies. */
  fillClassName?: string
  /** Breakdown mode — proportional slices instead of one level. */
  segments?: MeterSegment[]
  size?: 'xs' | 'sm' | 'md'
  /** Screen-reader description. Always supply one unless an adjacent element
   *  already states the number, in which case pass `decorative`. */
  label?: string
  /** The value is already written next to the bar — hide the bar from AT. */
  decorative?: boolean
}

function clampPercent(n: number): number {
  if (!Number.isFinite(n)) return 0
  return Math.max(0, Math.min(100, n))
}

function Meter({
  value = 0,
  tone = 'accent',
  fillClassName,
  segments,
  size = 'sm',
  label,
  decorative = false,
  className,
  ...props
}: MeterProps) {
  const a11y = decorative
    ? ({ 'aria-hidden': true } as const)
    : ({ role: 'img', 'aria-label': label } as const)

  return (
    <div
      className={cn(
        'flex w-full overflow-hidden rounded-full bg-control-track',
        SIZE_RAIL[size],
        className,
      )}
      {...a11y}
      {...props}
    >
      {segments ? (
        <Segments segments={segments} />
      ) : (
        <div
          className={cn('rounded-full transition-all', fillClassName ?? TONE_FILL[tone])}
          style={{ width: `${clampPercent(value)}%` }}
        />
      )}
    </div>
  )
}

/** Proportional slices. Zero-weight segments are dropped so they can't
 *  contribute a hairline sliver of colour to a breakdown they're absent from. */
function Segments({ segments }: { segments: MeterSegment[] }) {
  const total = segments.reduce((sum, s) => sum + Math.max(0, s.value), 0)
  if (total <= 0) return null
  return (
    <>
      {segments.map((seg, i) =>
        seg.value > 0 ? (
          <div
            key={i}
            className={cn('transition-all', seg.fillClassName ?? TONE_FILL[seg.tone ?? 'neutral'])}
            style={{ width: `${(seg.value / total) * 100}%` }}
          />
        ) : null,
      )}
    </>
  )
}

export { Meter }
