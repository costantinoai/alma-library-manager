import { AlertCircle, AlertTriangle, BadgeCheck, HelpCircle, History, RssIcon } from 'lucide-react'
import type { StatusBadgeTone } from '@/components/ui/status-badge'

export interface AuthorResolvedBadgeAuthor {
  id_resolution_status?: string | null
  id_resolution_method?: string | null
  id_resolution_confidence?: number | null
}

interface BadgeSpec {
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
  // Operational attention rows from /authors/needs-attention (same canonical
  // source as the Health popup) — not id-resolution states, but they flow
  // through the same badge slot on the needs-attention list.
  if (status === 'degraded_monitor') {
    return {
      tone: 'warning',
      icon: RssIcon,
      label: 'Monitor',
      title: "This author's feed monitor is degraded — refreshes are not landing.",
    }
  }
  if (status.startsWith('corpus_')) {
    return {
      tone: 'warning',
      icon: History,
      label: 'Corpus',
      title: 'The historical-corpus backfill for this author needs maintenance.',
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
