import { Info } from 'lucide-react'

import type { AuthorSignal, AuthorSignalComponent } from '@/api/client'
import {
  Tooltip,
  TooltipContent,
  TooltipProvider,
  TooltipTrigger,
} from '@/components/ui/tooltip'
import { cn } from '@/lib/utils'

interface AuthorSignalBarProps {
  signal: AuthorSignal | null
  className?: string
  showCaption?: boolean
  /** How to expose the per-component breakdown:
   *  - `'none'`   — composite bar only
   *  - `'hover'`  — composite bar + an info trigger revealing the breakdown
   *                 in a tooltip (compact cards)
   *  - `'inline'` — composite bar + the breakdown rendered below it
   *                 (the detail popup, where there's room) */
  breakdown?: 'none' | 'hover' | 'inline'
}

/** Composite fill: colour by strength of a (non-negative) like-signal. */
function signalFillClass(score: number): string {
  if (score >= 60) return 'bg-success-500'
  if (score >= 30) return 'bg-warning-500'
  if (score > 0) return 'bg-slate-400'
  return 'bg-slate-300'
}

/** Component fill: colour by direction (positive/negative/neutral). */
function toneFillClass(tone: AuthorSignalComponent['tone']): string {
  if (tone === 'positive') return 'bg-success-500'
  if (tone === 'negative') return 'bg-critical-500'
  return 'bg-slate-300'
}

function ComponentRow({ component }: { component: AuthorSignalComponent }) {
  const width = Math.max(0, Math.min(100, component.score))
  const sign = component.tone === 'negative' ? '−' : ''
  return (
    <div className="flex items-center gap-2">
      <span className="w-[4.5rem] shrink-0 text-[11px] text-slate-500">{component.label}</span>
      <div className="relative h-1 flex-1 overflow-hidden rounded-full bg-parchment-200">
        <div
          className={cn('absolute inset-y-0 left-0 rounded-full', toneFillClass(component.tone))}
          style={{ width: `${width}%` }}
        />
      </div>
      <span className="w-6 shrink-0 text-right text-[11px] tabular-nums text-slate-600">
        {sign}
        {Math.round(component.score)}
      </span>
    </div>
  )
}

function Breakdown({ components }: { components: AuthorSignalComponent[] }) {
  return (
    <div className="space-y-1.5">
      {components.map((c) => (
        <ComponentRow key={c.key} component={c} />
      ))}
    </div>
  )
}

/**
 * Single, shared signal meter used across suggestion cards, followed cards,
 * and the detail dialog. One visually coherent language for "how much do we
 * like this author", reading the canonical `AuthorSignal` (library + rating +
 * interaction + similarity + neighborhood). Pass `breakdown` to expose the
 * per-component story on hover (cards) or inline (detail popup).
 */
export function AuthorSignalBar({
  signal,
  className,
  showCaption = true,
  breakdown = 'none',
}: AuthorSignalBarProps) {
  if (!signal) {
    return (
      <div className={cn('inline-flex items-center gap-2 text-[11px] text-slate-400', className)}>
        <span className="inline-block h-1 w-14 rounded-full bg-parchment-200" />
        <span>no signal yet</span>
      </div>
    )
  }
  const pct = Math.max(0, Math.min(100, signal.score))
  const fill = signalFillClass(pct)
  const components = signal.components ?? []
  const hasBreakdown = breakdown !== 'none' && components.length > 0

  return (
    <div className={cn('space-y-1', className)}>
      <div className="flex items-center gap-2">
        <div className="relative h-1 w-full overflow-hidden rounded-full bg-parchment-200">
          <div
            className={cn('absolute inset-y-0 left-0 rounded-full transition-all', fill)}
            style={{ width: `${pct}%` }}
          />
        </div>
        <span className="shrink-0 text-[11px] font-semibold tabular-nums text-slate-700">
          {Math.round(pct)}
        </span>
        {hasBreakdown && breakdown === 'hover' ? (
          <TooltipProvider delayDuration={150}>
            <Tooltip>
              <TooltipTrigger
                type="button"
                aria-label="Signal breakdown"
                // The card itself is clickable — keep hovering/clicking the
                // info dot from opening the detail dialog.
                onClick={(event) => event.stopPropagation()}
                className="shrink-0 text-slate-400 transition hover:text-accent focus:outline-none focus-visible:ring-2 focus-visible:ring-accent"
              >
                <Info className="h-3.5 w-3.5" aria-hidden />
              </TooltipTrigger>
              <TooltipContent
                className="w-56 border border-edge-3 bg-surface-3 p-3 text-slate-700 shadow-md"
                sideOffset={6}
              >
                <Breakdown components={components} />
              </TooltipContent>
            </Tooltip>
          </TooltipProvider>
        ) : null}
      </div>
      {showCaption ? (
        <p className="text-[11px] text-slate-500">
          {signal.library_papers} lib / {signal.total_papers} total
          {signal.avg_rating != null ? ` · ★${signal.avg_rating.toFixed(1)}` : null}
        </p>
      ) : null}
      {hasBreakdown && breakdown === 'inline' ? (
        <div className="pt-1">
          <Breakdown components={components} />
        </div>
      ) : null}
    </div>
  )
}
