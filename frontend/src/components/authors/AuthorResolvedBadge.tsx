import { BadgeCheck, AlertCircle, HelpCircle, AlertTriangle } from 'lucide-react'

import { StatusBadge, type StatusBadgeTone } from '@/components/ui/status-badge'

export interface AuthorResolvedBadgeAuthor {
  id_resolution_status?: string | null
  id_resolution_method?: string | null
  id_resolution_confidence?: number | null
}

/**
 * Small emoji/icon badge summarising the author's identity-resolution state.
 * Surfaces on every author card (followed grid, corpus table, suggestion rail)
 * so the user can see at a glance which rows are fully resolved vs still
 * pending attention.
 */
export function AuthorResolvedBadge({
  author,
  size = 'sm',
  showLabel = false,
}: {
  author: AuthorResolvedBadgeAuthor
  size?: 'sm' | 'md'
  showLabel?: boolean
}) {
  const { tone, icon: Icon, label, title } = resolvedBadgeSpec(author)
  return (
    <StatusBadge
      tone={tone}
      size={size === 'md' ? 'md' : 'sm'}
      className="gap-1"
      title={title}
    >
      <Icon className="h-3 w-3" aria-hidden />
      {showLabel ? <span>{label}</span> : null}
    </StatusBadge>
  )
}

type BadgeSpec = {
  tone: StatusBadgeTone
  icon: typeof BadgeCheck
  label: string
  title: string
}

export function resolvedBadgeSpec(author: AuthorResolvedBadgeAuthor): BadgeSpec {
  const status = (author.id_resolution_status || '').toLowerCase()
  const confidence = author.id_resolution_confidence ?? 0
  const method = author.id_resolution_method || ''

  if (status === 'resolved_auto' || status === 'resolved_manual') {
    return {
      tone: 'positive',
      icon: BadgeCheck,
      label: 'Resolved',
      title:
        method && confidence > 0
          ? `Resolved via ${method} (${(confidence * 100).toFixed(0)}% confidence)`
          : 'Identity resolved — OpenAlex / ORCID / Scholar IDs confirmed.',
    }
  }
  if (status === 'needs_manual_review') {
    return {
      tone: 'warning',
      icon: AlertTriangle,
      label: 'Review',
      title: 'Multiple candidates — click the author to pick the right one.',
    }
  }
  if (status === 'no_match') {
    return {
      tone: 'warning',
      icon: HelpCircle,
      label: 'No match',
      title: 'OpenAlex returned no match for this name.',
    }
  }
  if (status === 'error') {
    return {
      tone: 'negative',
      icon: AlertCircle,
      label: 'Error',
      title: 'Last refresh raised an exception. Retry from the author card.',
    }
  }
  return {
    tone: 'neutral',
    icon: HelpCircle,
    label: 'Unresolved',
    title: 'Identity not yet resolved. Run refresh from the author card.',
  }
}
