import { StatusBadge } from '@/components/ui/status-badge'
import {
  resolvedBadgeSpec,
  type AuthorResolvedBadgeAuthor,
} from '@/components/authors/authorResolvedBadgeSpec'

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
      size={size === 'md' ? 'default' : 'sm'}
      className="gap-1"
      title={title}
    >
      <Icon className="h-3 w-3" aria-hidden />
      {showLabel ? <span>{label}</span> : null}
    </StatusBadge>
  )
}

export type { AuthorResolvedBadgeAuthor } from '@/components/authors/authorResolvedBadgeSpec'
