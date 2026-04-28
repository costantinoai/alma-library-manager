import { Info } from 'lucide-react'
import * as React from 'react'

import { Popover, PopoverContent, PopoverTrigger } from '@/components/ui/popover'
import { cn } from '@/lib/utils'

interface JargonHintProps {
  /** The term being explained. Shown as the popover heading + aria-label. */
  title: string
  /** Plain-English explanation. ReactNode so callers can embed links / emphasis. */
  description: React.ReactNode
  /** Optional extra className on the trigger button (e.g. to tweak margin). */
  className?: string
  /** Popover side — defaults to top so hints open upward above cramped rows. */
  side?: React.ComponentProps<typeof PopoverContent>['side']
  /** Popover alignment — defaults to start. */
  align?: React.ComponentProps<typeof PopoverContent>['align']
}

/**
 * A subtle info-icon trigger that opens a small Popover with a plain-English
 * definition. Used inline next to jargon terms on Discovery / Insights
 * so new users don't have to guess what "Branch Studio" or
 * "Exploration Temperature" mean. Clicking anywhere outside the popover, or
 * pressing Escape, dismisses it — no manual overlay wiring.
 */
export function JargonHint({ title, description, className, side = 'top', align = 'start' }: JargonHintProps) {
  return (
    <Popover>
      <PopoverTrigger asChild>
        <button
          type="button"
          aria-label={`Learn about ${title}`}
          className={cn(
            'inline-flex size-4 shrink-0 items-center justify-center rounded-full text-slate-400 transition-colors',
            'hover:bg-parchment-100 hover:text-slate-600',
            'focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-alma-500/60 focus-visible:ring-offset-1',
            className,
          )}
        >
          <Info className="size-3.5" aria-hidden />
        </button>
      </PopoverTrigger>
      <PopoverContent className="w-72 max-w-xs p-3 text-xs leading-relaxed" side={side} align={align}>
        <p className="text-sm font-semibold text-alma-800">{title}</p>
        <div className="mt-1.5 text-slate-600">{description}</div>
      </PopoverContent>
    </Popover>
  )
}
