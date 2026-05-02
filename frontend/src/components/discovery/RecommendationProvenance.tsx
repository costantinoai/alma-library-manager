import { useLayoutEffect, useRef, useState } from 'react'
import { Sparkles } from 'lucide-react'

import { StatusBadge, type StatusBadgeTone } from '@/components/ui/status-badge'
import { Tooltip, TooltipContent, TooltipProvider, TooltipTrigger } from '@/components/ui/tooltip'
import { cn } from '@/lib/utils'

/** Shape read from `recommendation.score_breakdown`. All fields optional —
 * the component renders nothing when every "why" signal is missing. */
export interface ProvenanceSignals {
  branchLabel?: string | null
  branchMode?: string | null
  sourceType?: string | null
  sourceApi?: string | null
  matchedQuery?: string | null
  branchCoreTopics?: string[] | null
  branchExploreTopics?: string[] | null
  // T4 numeric provenance (stamped into `score_breakdown.provenance`
  // at retrieval time). All optional — the component skips any chip
  // whose value is missing or zero, so legacy pre-T4 rows render
  // silently without ghost chips.
  specterCosine?: number | null
  lexicalSimilarity?: number | null
  sharedAuthorsCount?: number | null
  sharedAuthorsSample?: string | null
  /** True when the candidate's embedding landed close to a
   *  dismissed / disliked / removed seed. Rendered as a warning-tone
   *  chip so the user can weigh the hit. */
  negativeHit?: number | null
  /** Normalised 0..1 final score — distinct from the 0-100 ScoreBar
   *  at the top of the card. Used only if the card caller wants to
   *  echo the ranking here too; usually omitted. */
  scorePct?: number | null
  /** Number of independent retrieval sources that surfaced this
   *  candidate (channels + distinct external source APIs). Renders as
   *  a "Suggested by N sources" chip when N ≥ 2. */
  consensusCount?: number | null
  /** Signed projected-feedback adjustment in [-1, 1]: net pull from
   *  the user's saved / dismissed papers, their authors, topics,
   *  venues, etc. Positive → "matches what you keep"; negative →
   *  "near things you've dismissed". Rendered only when the
   *  magnitude clears a small noise floor. */
  projectedFeedbackRaw?: number | null
}

interface Chip {
  key: string
  label: string
  tone: StatusBadgeTone
}

function buildChips(signals: ProvenanceSignals): Chip[] {
  const chips: Chip[] = []

  // Lead with the consensus chip — multi-source agreement is the
  // strongest "why this surfaced" signal we have. When N≥2 distinct
  // retrieval channels independently surface the same candidate, we
  // want the user to see that *first*, then the per-signal scores.
  if (typeof signals.consensusCount === 'number' && signals.consensusCount >= 2) {
    chips.push({
      key: 'consensus',
      label: `Found by ${signals.consensusCount} sources`,
      tone: 'accent',
    })
  }

  // Numeric "why" chips (T4) — truthful evidence users want.
  // Rendered only when the underlying signal cleared its threshold at
  // scoring time.
  if (typeof signals.specterCosine === 'number' && signals.specterCosine > 0) {
    chips.push({
      key: 'specter',
      label: `SPECTER ${signals.specterCosine.toFixed(2)}`,
      tone: 'neutral',
    })
  }
  if (typeof signals.lexicalSimilarity === 'number' && signals.lexicalSimilarity > 0.05) {
    chips.push({
      key: 'lexical',
      label: `Lexical ${signals.lexicalSimilarity.toFixed(2)}`,
      tone: 'neutral',
    })
  }
  if (
    typeof signals.sharedAuthorsCount === 'number' &&
    signals.sharedAuthorsCount > 0
  ) {
    const sample = (signals.sharedAuthorsSample || '').trim()
    const label =
      signals.sharedAuthorsCount === 1 && sample
        ? `co-author: ${sample}`
        : `${signals.sharedAuthorsCount} shared authors`
    chips.push({ key: 'authors', label, tone: 'neutral' })
  }
  if (typeof signals.negativeHit === 'number' && signals.negativeHit >= 0.35) {
    chips.push({
      key: 'neg-hit',
      label: `Near a disliked paper (${signals.negativeHit.toFixed(2)})`,
      tone: 'warning',
    })
  }
  // (Consensus chip moved to lead position above.)
  // Projected feedback: the signed pull from the user's per-paper
  // history (saves / ratings / dismisses → authors / topics / venues
  // / keywords / tags / semantic + citation neighbours). Threshold of
  // 0.05 keeps the chip silent when the signal is essentially zero.
  if (
    typeof signals.projectedFeedbackRaw === 'number' &&
    Math.abs(signals.projectedFeedbackRaw) >= 0.05
  ) {
    const positive = signals.projectedFeedbackRaw > 0
    chips.push({
      key: 'projected',
      label: positive
        ? `+${signals.projectedFeedbackRaw.toFixed(2)} from your saves`
        : `${signals.projectedFeedbackRaw.toFixed(2)} from past rejects`,
      tone: positive ? 'positive' : 'warning',
    })
  }

  // Existing categorical chips (branch / source / planner).
  if (signals.branchLabel) {
    chips.push({ key: 'branch', label: signals.branchLabel, tone: 'info' })
  }
  if (signals.branchMode) {
    chips.push({
      key: 'mode',
      label: signals.branchMode.replace(/_/g, ' '),
      tone: 'neutral',
    })
  }
  if (signals.sourceType) {
    chips.push({
      key: 'source',
      label: signals.sourceType.replace(/_/g, ' '),
      tone: 'neutral',
    })
  }
  if (signals.sourceApi) {
    chips.push({ key: 'api', label: signals.sourceApi, tone: 'neutral' })
  }
  return chips
}

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
      className="block truncate font-mono text-[11px] leading-snug text-slate-600"
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
        <TooltipContent side="top" align="start" className="max-w-md break-words font-mono text-[11px]">
          {query}
        </TooltipContent>
      </Tooltip>
    </TooltipProvider>
  )
}

/** A small-caps row label ("Query", "Core", "Explore") — matches the
 * typographic voice of `<p className="text-[11px] font-semibold uppercase
 * tracking-wide text-slate-400">Score Breakdown</p>` used elsewhere in
 * PaperCard so this block reads as part of the same family. */
function RowLabel({ children }: { children: React.ReactNode }) {
  return (
    <span className="shrink-0 pt-[2px] text-[10px] font-semibold uppercase tracking-[0.08em] text-slate-400">
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
        'inline-flex items-center rounded-full px-2 py-[1px] text-[10.5px] font-medium leading-[1.35] tracking-tight',
        variant === 'core'
          ? 'bg-alma-chrome text-slate-700 shadow-[inset_0_0_0_1px_theme(colors.slate.200)]'
          : 'border border-dashed border-[var(--color-border)] text-slate-500',
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
}: {
  signals: ProvenanceSignals
  className?: string
  /** `panel` (default) — full bordered section with header label and a
   *  detail block underneath. Used in Discovery extended view + the paper
   *  detail popup. `inline` — a single chip row (no header, no detail
   *  block) that lives in the card's metadata strip. Used by Discovery
   *  normal view to keep the card dense without losing the why-signal. */
  variant?: 'panel' | 'inline'
}) {
  const chips = buildChips(signals)
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
  // panel variant.
  if (variant === 'inline') {
    if (chips.length === 0) return null
    return (
      <div
        aria-label="Recommendation provenance"
        className={cn('mt-2 flex flex-wrap items-center gap-1.5', className)}
      >
        {chips.map((chip) => (
          <StatusBadge key={chip.key} tone={chip.tone} size="sm">
            {chip.label}
          </StatusBadge>
        ))}
      </div>
    )
  }

  return (
    <section
      aria-label="Recommendation provenance"
      className={cn(
        'mt-3 rounded-md border border-slate-100 bg-parchment-50/50 px-3 py-2.5',
        className,
      )}
    >
      <header className="flex items-center gap-1.5">
        <Sparkles className="h-3 w-3 text-slate-400" aria-hidden />
        <span className="text-[10px] font-semibold uppercase tracking-[0.08em] text-slate-400">
          Why this surfaced
        </span>
      </header>

      {chips.length > 0 && (
        <div className="mt-2 flex flex-wrap gap-1.5">
          {chips.map((chip) => (
            <StatusBadge key={chip.key} tone={chip.tone} size="sm">
              {chip.label}
            </StatusBadge>
          ))}
        </div>
      )}

      {hasDetail && (
        <dl
          className={cn(
            'mt-2 space-y-1.5 text-[11px] leading-snug text-slate-500',
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
