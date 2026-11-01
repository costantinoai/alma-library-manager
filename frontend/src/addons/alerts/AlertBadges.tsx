import { CheckCircle, Clock, XCircle } from 'lucide-react'
import { StatusBadge as Pill } from '@/components/ui/status-badge'

export function RuleTypeBadge({ type }: { type: string }) {
  const variantMap: Record<string, 'neutral' | 'warning'> = {
    author: 'neutral',
    collection: 'neutral',
    keyword: 'warning',
    topic: 'neutral',
    similarity: 'neutral',
    discovery_lens: 'neutral',
    feed_monitor: 'neutral',
    branch: 'neutral',
    library_workflow: 'warning',
  }
  return <Pill tone={variantMap[type] ?? 'neutral'}>{type}</Pill>
}

export function StatusBadge({ status }: { status: string }) {
  switch (status) {
    case 'sent':
      return (
        <Pill tone="positive">
          <CheckCircle className="mr-1 h-3 w-3" />
          Sent
        </Pill>
      )
    case 'failed':
      return (
        <Pill tone="negative">
          <XCircle className="mr-1 h-3 w-3" />
          Failed
        </Pill>
      )
    case 'pending':
      return (
        <Pill tone="warning">
          <Clock className="mr-1 h-3 w-3" />
          Pending
        </Pill>
      )
    case 'empty':
      return <Pill tone="neutral">No new papers</Pill>
    case 'skipped':
      return <Pill tone="neutral">Skipped</Pill>
    default:
      return <Pill tone="neutral">{status}</Pill>
  }
}
