import { useLayoutEffect, useRef, useState } from 'react'
import { Sparkles } from 'lucide-react'

import { SignalChip } from '@/components/shared/SignalChip'
import { Tooltip, TooltipContent, TooltipProvider, TooltipTrigger } from '@/components/ui/tooltip'
import { cn } from '@/lib/utils'
import { buildChips, orderChips, type ProvenanceSignals } from './provenanceChips'

/** Minimal wrapper that wires a Tooltip around the query text only when the
 * rendered line overflows. Keeps the surface silent when there's nothing
 * clipped — no redundant "hover for more" affordance on short queries. */
function QueryLine({ query }: { query: string }) {
  const ref = useRef<HTMLSpanElement | null>(null)
  const [overflows, setOverflows] = useState(false)

  useLayoutEffect(() => {
    const el = ref.current
    if (!el) return
    setOverflows(el.scrollWidth > el.clientWidth)
  }, [query])

  const text = (
    <span
      ref={ref}
      className="block truncate font-mono text-caption leading-snug text-slate-600"
    >
      {query}
    </span>
  )

  if (!overflows) {
    return text
  }
  return (
    <TooltipProvider delayDuration={150}>
      <Tooltip>
        <TooltipTrigger asChild>
          <span className="block cursor-help">{text}</span>
        </TooltipTrigger>
        <TooltipContent side="top" align="start" className="max-w-md break-words font-mono text-caption">
          {query}
        </TooltipContent>
      </Tooltip>
    </TooltipProvider>
  )
}

/** A small-caps row label ("Query", "Core", "Explore") — matches the
 * typographic voice of `<p className="text-caption font-semibold uppercase
 * tracking-wide text-slate-400">Score Breakdown</p>` used elsewhere in
 * PaperCard so this block reads as part of the same family. */
function RowLabel({ children }: { children: React.ReactNode }) {
  return (
    <span className="shrink-0 pt-[2px] text-2xs font-semibold uppercase tracking-[0.08em] text-slate-400">
      {children}
    </span>
  )
}

function TopicPill({
  label,
  variant,
}: {
  label: string
  variant: 'core' | 'explore'
}) {
  return (
    <span
      className={cn(
        'inline-flex items-center rounded-full px-2 py-[1px] text-2xs font-medium leading-[1.35] tracking-tight',
        variant === 'core'
          ? 'bg-surface-1 text-slate-700 ring-1 ring-inset ring-control-edge'
          : 'border border-dashed border-divider text-slate-500',
      )}
    >
      {label}
    </span>
  )
}

/** Explains *why* a Discovery recommendation surfaced. Rendered through
 * PaperCard's `children` slot so the card's own chrome (title, authors,
 * score bar, action bar) stays untouched.
 *
 * Three stacked registers, all optional — the component collapses whole
 * sections when their signal is missing rather than leaving empty labels
 * behind:
 *  1. Chip row: branch / mode / source / api / AI-planner flag.
 *  2. Query row: the actual search string (mono, tooltip on overflow).
 *  3. Topics row: branch core topics (filled) + explore topics (dashed).
 *
 * Returns `null` when there's nothing to show so empty slots don't leave
 * a ghost panel on cards that lack provenance (seeded similarity results,
 * legacy rows written before the provenance fields existed).
 */
export function RecommendationProvenance({
  signals,
  className,
  variant = 'panel',
  chipCounts,
}: {
  signals: ProvenanceSignals
  className?: string
  /** How many cards in the visible list carry each chip (`countChipKeys`). */
  chipCounts?: Map<string, number>
  /** `panel` (default) — full bordered section with header label and a
   *  detail block underneath. Used in Discovery extended view + the paper
   *  detail popup. `inline` — a single chip row (no header, no detail
   *  block) that lives in the card's metadata strip. Used by Discovery
   *  normal view to keep the card dense without losing the why-signal. */
  variant?: 'panel' | 'inline'
}) {
  const chips = orderChips(buildChips(signals), chipCounts)
  const query = (signals.matchedQuery || '').trim()
  const coreTopics = (signals.branchCoreTopics || []).filter(
    (t) => typeof t === 'string' && t.trim().length > 0,
  )
  const exploreTopics = (signals.branchExploreTopics || []).filter(
    (t) => typeof t === 'string' && t.trim().length > 0,
  )

  const hasDetail = query.length > 0 || coreTopics.length > 0 || exploreTopics.length > 0
  if (chips.length === 0 && !hasDetail) {
    return null
  }

  // Inline variant — the dense form rendered inside Discovery normal-view
  // cards. We keep only the chip row (the cheap, scannable signal) and drop
  // the section chrome and the query/topic detail block. Users who want the
  // full breakdown click into the paper detail panel which still uses the
  // panel variant. It carries no margin of its own: it lands in the card's
  // signal row (`PaperCard` → `metaSlot`), which owns the spacing.
  if (variant === 'inline') {
    if (chips.length === 0) return null
    return (
      <div
        aria-label="Recommendation provenance"
        className={cn('flex flex-wrap items-center gap-1.5', className)}
      >
        {chips.map((chip) => (
          <SignalChip key={chip.key} kind={chip.kind} size="sm" explain={chip.explain}>
            {chip.label}
          </SignalChip>
        ))}
      </div>
    )
  }

  return (
    <section
      aria-label="Recommendation provenance"
      className={cn(
        'mt-3 rounded-card border border-slate-100 bg-surface-2/50 px-3 py-2.5',
        className,
      )}
    >
      <header className="flex items-center gap-1.5">
        <Sparkles className="h-3 w-3 text-slate-400" aria-hidden />
        <span className="text-2xs font-semibold uppercase tracking-[0.08em] text-slate-400">
          Why this surfaced
        </span>
      </header>

      {chips.length > 0 && (
        <div className="mt-2 flex flex-wrap gap-1.5">
          {chips.map((chip) => (
            <SignalChip key={chip.key} kind={chip.kind} size="sm" explain={chip.explain}>
              {chip.label}
            </SignalChip>
          ))}
        </div>
      )}

      {hasDetail && (
        <dl
          className={cn(
            'mt-2 space-y-1.5 text-caption leading-snug text-slate-500',
            chips.length > 0 && 'border-t border-slate-100 pt-2',
          )}
        >
          {query && (
            <div className="flex items-baseline gap-2">
              <dt>
                <RowLabel>Query</RowLabel>
              </dt>
              <dd className="min-w-0 flex-1">
                <QueryLine query={query} />
              </dd>
            </div>
          )}
          {coreTopics.length > 0 && (
            <div className="flex items-start gap-2">
              <dt>
                <RowLabel>Core</RowLabel>
              </dt>
              <dd className="flex min-w-0 flex-1 flex-wrap gap-1">
                {coreTopics.slice(0, 5).map((topic) => (
                  <TopicPill key={`core-${topic}`} label={topic} variant="core" />
                ))}
              </dd>
            </div>
          )}
          {exploreTopics.length > 0 && (
            <div className="flex items-start gap-2">
              <dt>
                <RowLabel>Explore</RowLabel>
              </dt>
              <dd className="flex min-w-0 flex-1 flex-wrap gap-1">
                {exploreTopics.slice(0, 4).map((topic) => (
                  <TopicPill key={`exp-${topic}`} label={topic} variant="explore" />
                ))}
              </dd>
            </div>
          )}
        </dl>
      )}
    </section>
  )
}
