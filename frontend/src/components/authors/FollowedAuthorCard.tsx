import { AlertTriangle, ChevronRight } from 'lucide-react'

import type { Author, AuthorNeedsAttentionRow, AuthorSignal } from '@/api/client'
import { Button } from '@/components/ui/button'
import { Card } from '@/components/ui/card'
import { StatusBadge } from '@/components/ui/status-badge'
import { monitorHealthTone } from '@/components/ui/status-badge-tones'
import { AuthorSignalBar } from '@/components/authors/AuthorSignalBar'
import { AuthorResolvedBadge } from '@/components/authors/AuthorResolvedBadge'
import { formatNumber, formatTimestamp } from '@/lib/utils'

interface FollowedAuthorCardProps {
  author: Author
  signal?: AuthorSignal | null
  onClick: () => void
  /** Marks the user's own author profile (set during onboarding). */
  isOwner?: boolean
  /** Set when the author currently appears in the
   *  `/authors/needs-attention` list. Drives the orange-reddish
   *  warning triangle in the header. */
  attentionRow?: AuthorNeedsAttentionRow | null
  /** Click handler for the warning triangle. Bypasses the card's
   *  detail dialog and routes directly to the matching
   *  needs-attention sub-dialog (review / resolve-conflict /
   *  add-identifier / refresh) via the shared router. */
  onAttentionClick?: () => void
}

function monitorLabel(health?: string | null): string {
  if (!health) return 'Monitor unknown'
  if (health === 'ready') return 'Monitor ready'
  if (health === 'disabled') return 'Monitor disabled'
  return 'Monitor attention'
}

export function FollowedAuthorCard({
  author,
  signal,
  onClick,
  isOwner,
  attentionRow,
  onAttentionClick,
}: FollowedAuthorCardProps) {
  const lastCheck = author.monitor_last_success_at ?? author.monitor_last_checked_at
  const newPapers = author.monitor_items_created ?? 0
  const hasChecked = lastCheck != null

  return (
    <Card
      interactive
      onClick={onClick}
      className="group flex h-full flex-col gap-3 p-4"
    >
      <header className="flex items-start justify-between gap-3">
        <div className="min-w-0">
          <div className="flex items-center gap-1.5">
            <h3 className="truncate text-sm font-semibold text-alma-800">{author.name}</h3>
            {isOwner ? (
              <StatusBadge tone="accent" size="sm" className="shrink-0">
                This is you
              </StatusBadge>
            ) : null}
            <AuthorResolvedBadge author={author} size="sm" />
          </div>
          {author.affiliation ? (
            <p className="mt-0.5 truncate text-caption text-slate-500">{author.affiliation}</p>
          ) : null}
        </div>
        <div className="flex shrink-0 items-center gap-1.5">
          {/* Warning triangle — surfaces any /authors/needs-attention
              row (split_profiles, merge_conflict, affiliation_conflict,
              retry_refresh, ...) directly on the card so the user
              doesn't have to scroll to the section to act. Click
              short-circuits the card's detail dialog and routes to
              the matching sub-dialog via the shared attention router.
              Orange-reddish (rose) per user direction — louder than
              the section's amber `tone="warning"` chip because here
              it's the only attention signal on the card. */}
          {attentionRow && onAttentionClick ? (
            <Button
              type="button"
              variant="danger-quiet"
              size="icon-sm"
              onClick={(event) => {
                event.stopPropagation()
                onAttentionClick()
              }}
              title={
                [attentionRow.reason, attentionRow.reason_detail]
                  .filter(Boolean)
                  .join(' — ') || attentionRow.suggested_action.hint
              }
              aria-label={`Resolve ${attentionRow.reason} for ${author.name}`}
            >
              <AlertTriangle className="h-3.5 w-3.5" aria-hidden />
            </Button>
          ) : null}
          {/* Quiet when healthy: 89 identical "ready" badges say nothing, and
              one that speaks only on trouble can be seen from across the grid. */}
          {author.monitor_health !== 'ready' ? (
            <StatusBadge tone={monitorHealthTone(author.monitor_health)} size="sm">
              {monitorLabel(author.monitor_health)}
            </StatusBadge>
          ) : null}
        </div>
      </header>

      <div className="flex flex-wrap gap-3 text-caption text-slate-500">
        {author.h_index != null ? <span>h-index {author.h_index}</span> : null}
        {author.citedby != null ? <span>{formatNumber(author.citedby)} citations</span> : null}
        {author.works_count != null ? <span>{formatNumber(author.works_count)} works</span> : null}
        {(author.publication_count ?? 0) > 0 ? (
          <span>{author.publication_count} in DB</span>
        ) : null}
      </div>

      <AuthorSignalBar signal={signal ?? null} breakdown="hover" />

      <div className="mt-auto flex items-center justify-between gap-3 border-t border-edge-2 pt-2 text-caption text-slate-500">
        <span>
          {newPapers > 0 ? <span className="font-medium text-alma-800">{newPapers} new · </span> : null}
          {hasChecked ? `Checked ${formatTimestamp(lastCheck)}` : 'No refresh yet'}
        </span>
        <ChevronRight className="h-3.5 w-3.5 text-slate-400 transition group-hover:text-accent" />
      </div>
    </Card>
  )
}
