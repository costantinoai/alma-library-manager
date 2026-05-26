import { Clock, Info } from 'lucide-react'

import { Popover, PopoverContent, PopoverTrigger } from '@/components/ui/popover'
import type { MaintenanceEta } from '@/api/client'
import { cn } from '@/lib/utils'

interface EtaHintProps {
  /** The ETA payload from the backend, or null/undefined to render nothing. */
  eta: MaintenanceEta | null | undefined
  className?: string
}

/**
 * Compact "ETA ~Xm" hint for a repair button. The estimate is how long the op
 * needs to drain its eligible backlog at the relevant API's rate limit; a small
 * info popover explains the math (request count, rate, whether a key changes it).
 *
 * Brand discipline: this lives next to alma-grey buttons on off-white cards, so
 * it's a quiet slate caption (never a saturated semantic tone) with tabular-nums
 * numerals and the same popover chrome as `JargonHint`. Renders nothing when
 * there's no ETA (local-only ops, or nothing pending) so callers can drop it in
 * unconditionally.
 */
export function EtaHint({ eta, className }: EtaHintProps) {
  if (!eta || !eta.label) return null

  const keyNote = eta.auth_affects_rate
    ? eta.authenticated
      ? `Using your ${eta.source} API key.`
      : `Add a ${eta.source} API key to go faster.`
    : 'An API key improves reliability but not this rate.'

  return (
    <Popover>
      <PopoverTrigger asChild>
        <button
          type="button"
          aria-label={`Estimated time ${eta.label}. How is this computed?`}
          className={cn(
            'inline-flex items-center gap-1 rounded-sm px-1.5 py-0.5 text-[11px] font-medium text-slate-500',
            'transition-colors hover:bg-alma-50 hover:text-slate-700',
            'focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-alma-folio/50',
            className,
          )}
        >
          <Clock className="h-3 w-3 shrink-0" aria-hidden />
          <span className="tabular-nums">ETA {eta.label}</span>
          <Info className="h-2.5 w-2.5 shrink-0 opacity-50" aria-hidden />
        </button>
      </PopoverTrigger>
      <PopoverContent side="top" align="start" className="w-72 max-w-xs p-3 text-xs leading-relaxed">
        <p className="text-sm font-semibold text-alma-800">Estimated time · {eta.label}</p>
        <p className="mt-1.5 text-slate-600">{eta.basis}</p>
        <p className="mt-2 text-[11px] text-slate-500">{keyNote}</p>
      </PopoverContent>
    </Popover>
  )
}
