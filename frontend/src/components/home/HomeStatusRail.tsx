import type { HomeStatusPill } from '@/api/client'
import { StatusChip } from '@/components/shared/StatusChip'
import { cn, formatRelativeShort } from '@/lib/utils'

/**
 * State → the word a screen reader hears. Dot severity is backend-owned so the
 * same subsystem cannot acquire a different meaning on Home and Health.
 */
const STATE_WORD: Record<HomeStatusPill['state'], string> = {
  ok: 'working',
  warning: 'needs attention',
  failed: 'failing',
  running: 'running',
  unknown: 'not known yet',
  off: 'switched off',
}

function summary(status: HomeStatusPill): string {
  const when = status.checked_at
    ? ` · last checked ${formatRelativeShort(status.checked_at)}`
    : ''
  return `${status.metric}. ${status.detail}${when}`
}

export interface HomeStatusRailProps {
  status: HomeStatusPill[]
  className?: string
}

/**
 * Home's backend-owned subsystem line.
 *
 * The server decides which tiers appear, their severity, metric, explanation,
 * and destination. This component only renders that contract. Keeping those
 * decisions out of React prevents Home from drifting from Health or plugin
 * activation state.
 */
export function HomeStatusRail({ status, className }: HomeStatusRailProps) {
  if (status.length === 0) return null

  return (
    <div className={cn('contents', className)}>
      {status.map((item) => (
        <StatusChip
          key={item.key}
          variant="slim"
          severity={item.severity}
          name={item.label}
          href={item.href}
          title={summary(item)}
          ariaLabel={`${item.label}: ${STATE_WORD[item.state]}; ${item.metric}`}
        />
      ))}
    </div>
  )
}
