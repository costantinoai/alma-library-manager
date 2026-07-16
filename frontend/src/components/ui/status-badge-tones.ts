import type { StatusBadgeTone } from '@/components/ui/status-badge'

export function monitorHealthTone(health?: string | null): StatusBadgeTone {
  if (health === 'ready') return 'positive'
  if (health === 'disabled') return 'neutral'
  return 'warning'
}

export function severityTone(severity?: string | null): StatusBadgeTone {
  if (severity === 'critical') return 'negative'
  if (severity === 'warning') return 'warning'
  return 'info'
}

export function scoreStatusTone(status?: string | null): StatusBadgeTone {
  if (status === 'good') return 'positive'
  if (status === 'critical') return 'negative'
  if (status === 'insufficient_data' || status === 'observed') return 'neutral'
  return 'warning'
}
