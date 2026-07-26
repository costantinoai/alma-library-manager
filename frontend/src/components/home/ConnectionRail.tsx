import { ArrowRight } from 'lucide-react'

import type { HomeConnection } from '@/api/client'
import { Tooltip, TooltipContent, TooltipTrigger } from '@/components/ui/tooltip'
import { cn, formatRelativeShort } from '@/lib/utils'

/**
 * Connection state → dot fill + how loudly it reads.
 *
 * Only `failed` is allowed to be alarming. `not_configured` is a choice the
 * user has not made yet, not a fault, and `unknown` is honest ignorance —
 * dressing either as a warning would train the user to ignore the rail, which
 * would cost them the one state that matters.
 */
const STATE_STYLE: Record<
  HomeConnection['state'],
  { dot: string; text: string; word: string }
> = {
  ok: { dot: 'bg-success-600', text: 'text-slate-500', word: 'working' },
  failed: { dot: 'bg-critical-500', text: 'text-critical-700 font-medium', word: 'failing' },
  running: { dot: 'bg-info-500 animate-pulse motion-reduce:animate-none', text: 'text-slate-500', word: 'checking now' },
  // Inert states take the neutral ink rail, not a slate fill: a dot is a mark
  // on paper, and the ink ladder is what reads the same at every elevation.
  unknown: { dot: 'bg-control-track', text: 'text-slate-500', word: 'not used yet' },
  not_configured: { dot: 'bg-control-track', text: 'text-slate-400', word: 'not set up' },
}

function summary(connection: HomeConnection): string {
  const when = connection.checked_at
    ? ` · last used ${formatRelativeShort(connection.checked_at)}`
    : ''
  const stake = connection.state === 'failed' ? ` ${connection.stake}` : ''
  return `${connection.detail}${stake}${when}`
}

export interface ConnectionRailProps {
  connections: HomeConnection[]
  className?: string
}

/**
 * The instrument panel: one dot per outside dependency ALMa needs to keep
 * working while you are not looking.
 *
 * These three failures are the ones that never announce themselves — a revoked
 * Slack token, a rejected API key, a provider outage — and their only symptom
 * is a page that quietly stops filling up. The rail is always present so its
 * silence is meaningful: you learn where it lives while everything is green,
 * which is the only way you will notice the day one turns red.
 *
 * States come from the operation ledger, so a dot claims "last time we used
 * this, it worked" and never "it works right now". The tooltip always says
 * when. Live re-probing lives in Settings → Connections, one click away.
 */
export function ConnectionRail({ connections, className }: ConnectionRailProps) {
  if (connections.length === 0) return null
  const broken = connections.filter((c) => c.state === 'failed')

  return (
    <div className={cn('space-y-2', className)}>
      <div className="flex flex-wrap items-center gap-x-4 gap-y-1.5">
        <span className="text-[11px] font-medium uppercase tracking-wide text-slate-400">
          Connections
        </span>
        {connections.map((connection) => {
          const style = STATE_STYLE[connection.state]
          return (
            <Tooltip key={connection.key}>
              <TooltipTrigger asChild>
                <a
                  href={connection.href}
                  className={cn(
                    'flex items-center gap-1.5 rounded-sm text-xs transition-colors hover:text-alma-folio',
                    'focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-alma-folio',
                    style.text,
                  )}
                >
                  <span className={cn('h-1.5 w-1.5 shrink-0 rounded-full', style.dot)} aria-hidden />
                  {connection.label}
                  <span className="sr-only"> — {style.word}</span>
                </a>
              </TooltipTrigger>
              <TooltipContent side="bottom" className="max-w-xs">
                {summary(connection)}
              </TooltipContent>
            </Tooltip>
          )
        })}
      </div>
      {/* A red dot is easy to miss on a rail you have learned to skim past, so
          a real failure also states itself in words and offers the fix. */}
      {broken.map((connection) => (
        <a
          key={connection.key}
          href={connection.href}
          className="flex items-start gap-2 rounded-sm border border-critical-500/25 bg-critical-700/10 px-3 py-2 text-xs text-critical-700 transition-colors hover:bg-critical-700/15 focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-alma-folio"
        >
          <span className="min-w-0">
            <span className="font-medium">{connection.label} is failing.</span>{' '}
            {connection.stake} {connection.detail}
          </span>
          <ArrowRight className="mt-px h-3.5 w-3.5 shrink-0" aria-hidden />
        </a>
      ))}
    </div>
  )
}
