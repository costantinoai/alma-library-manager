import { AlertTriangle } from 'lucide-react'

import type { HomeConnection } from '@/api/client'
import { StatusChip } from '@/components/shared/StatusChip'
import { Alert, AlertDescription } from '@/components/ui/alert'
import { cn, formatRelativeShort } from '@/lib/utils'

/**
 * Connection state → its severity dot and the word a screen reader hears.
 *
 * Only `failed` is allowed to be alarming. `not_configured` is a choice the
 * user has not made yet, not a fault, and `unknown` is honest ignorance —
 * dressing either as a warning would train the user to ignore the rail, which
 * would cost them the one state that matters.
 */
const STATE: Record<HomeConnection['state'], { severity: string; word: string }> = {
  ok: { severity: 'ok', word: 'working' },
  failed: { severity: 'critical', word: 'failing' },
  running: { severity: 'info', word: 'checking now' },
  unknown: { severity: 'unknown', word: 'not used yet' },
  not_configured: { severity: 'unknown', word: 'not set up' },
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
 * **Deliberately slim** — dots and names on one line under the greeting, with
 * no heading of its own: it reports on the machinery, not on your work, so it
 * must not compete with the masthead above it or the figures below. It shares
 * that line with the needs-you chips (`AttentionChips`), because "is the
 * machinery up" and "does anything want me" are one glance.
 *
 * Each dot links to the surface that owns the fix. States come from the
 * operation ledger, so a dot claims "last time we used this, it worked" and
 * never "it works right now" — the tooltip always says when. Live re-probing
 * lives in Settings → Connections.
 *
 * Returns a FRAGMENT of chips plus any failure banners, so the host owns the
 * line they sit on.
 */
export function ConnectionRail({ connections, className }: ConnectionRailProps) {
  if (connections.length === 0) return null
  const broken = connections.filter((c) => c.state === 'failed')

  return (
    <div className={cn('contents', className)}>
      {connections.map((connection) => {
        const state = STATE[connection.state]
        return (
          <StatusChip
            key={connection.key}
            variant="slim"
            severity={state.severity}
            name={connection.label}
            href={connection.href}
            title={summary(connection)}
            ariaLabel={`${connection.label}: ${state.word}`}
          />
        )
      })}
      {/* A red dot is easy to miss on a rail you have learned to skim past, so
          a real failure also states itself in words and offers the fix. Same
          Alert shell Health uses for a subsystem it could not read. */}
      {broken.map((connection) => (
        <Alert key={connection.key} variant="warning" className="basis-full p-3">
          <AlertTriangle className="h-4 w-4" />
          <AlertDescription className="text-xs">
            <a href={connection.href} className="hover:underline">
              <span className="font-medium">{connection.label} is failing.</span>{' '}
              {connection.stake} {connection.detail}
            </a>
          </AlertDescription>
        </Alert>
      ))}
    </div>
  )
}
