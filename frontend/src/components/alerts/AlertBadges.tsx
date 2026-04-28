import { CheckCircle, Clock, XCircle } from 'lucide-react'
import { Badge } from '@/components/ui/badge'

export function RuleTypeBadge({ type }: { type: string }) {
  const variantMap: Record<string, 'default' | 'secondary' | 'warning'> = {
    author: 'default',
    collection: 'default',
    keyword: 'warning',
    topic: 'secondary',
    similarity: 'secondary',
    discovery_lens: 'default',
    feed_monitor: 'default',
    branch: 'secondary',
    library_workflow: 'warning',
  }
  return <Badge variant={variantMap[type] ?? 'secondary'}>{type}</Badge>
}

export function StatusBadge({ status }: { status: string }) {
  switch (status) {
    case 'sent':
      return (
        <Badge variant="success">
          <CheckCircle className="mr-1 h-3 w-3" />
          Sent
        </Badge>
      )
    case 'failed':
      return (
        <Badge variant="destructive">
          <XCircle className="mr-1 h-3 w-3" />
          Failed
        </Badge>
      )
    case 'pending':
      return (
        <Badge variant="warning">
          <Clock className="mr-1 h-3 w-3" />
          Pending
        </Badge>
      )
    case 'empty':
      return <Badge variant="secondary">No new papers</Badge>
    case 'skipped':
      return <Badge variant="secondary">Skipped</Badge>
    default:
      return <Badge variant="secondary">{status}</Badge>
  }
}
