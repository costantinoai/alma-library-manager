import { AlertTriangle, ChevronRight } from 'lucide-react'

import type { Author, AuthorNeedsAttentionRow, AuthorSignal } from '@/api/client'
import { StatusBadge, monitorHealthTone } from '@/components/ui/status-badge'
import { AuthorSignalBar } from '@/components/authors/AuthorSignalBar'
import { AuthorResolvedBadge } from '@/components/authors/AuthorResolvedBadge'
import { formatNumber, formatTimestamp } from '@/lib/utils'

interface FollowedAuthorCardProps {
  author: Author
  signal?: AuthorSignal | null
  onClick: () => void
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
  attentionRow,
  onAttentionClick,
}: FollowedAuthorCardProps) {
  const lastCheck = author.monitor_last_success_at ?? author.monitor_last_checked_at
  const lastYieldParts = [
    author.monitor_papers_found != null ? `${author.monitor_papers_found} found` : null,
    author.monitor_items_created != null ? `${author.monitor_items_created} new` : null,
  ].filter(Boolean)

  return (
    <article
      onClick={onClick}
      className="group flex h-full cursor-pointer flex-col gap-3 rounded-sm border border-[var(--color-border)] bg-alma-chrome-elev p-4 shadow-paper-sm shadow-sm transition hover:border-alma-300 hover:shadow-md"
    >
      <header className="flex items-start justify-between gap-3">
        <div className="min-w-0">
          <div className="flex items-center gap-1.5">
            <h3 className="truncate text-sm font-semibold text-alma-800">{author.name}</h3>
            <AuthorResolvedBadge author={author} size="sm" />
          </div>
          {author.affiliation ? (
            <p className="mt-0.5 truncate text-[11px] text-slate-500">{author.affiliation}</p>
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
            <button
              type="button"
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
              className="inline-flex h-6 w-6 items-center justify-center rounded-full border border-rose-200 bg-rose-50 text-rose-600 transition hover:border-rose-300 hover:bg-rose-100 hover:text-rose-700 focus:outline-none focus-visible:ring-2 focus-visible:ring-rose-300"
            >
              <AlertTriangle className="h-3.5 w-3.5" aria-hidden />
            </button>
          ) : null}
          <StatusBadge tone={monitorHealthTone(author.monitor_health)} size="sm">
            {monitorLabel(author.monitor_health)}
          </StatusBadge>
        </div>
      </header>

      <div className="flex flex-wrap gap-3 text-[11px] text-slate-500">
        {author.h_index != null ? <span>h-index {author.h_index}</span> : null}
        {author.citedby != null ? <span>{formatNumber(author.citedby)} citations</span> : null}
        {author.works_count != null ? <span>{formatNumber(author.works_count)} works</span> : null}
        {(author.publication_count ?? 0) > 0 ? (
          <span>{author.publication_count} in DB</span>
        ) : null}
      </div>

      <AuthorSignalBar signal={signal ?? null} />

      <div className="flex items-center justify-between gap-3 border-t border-slate-100 pt-2 text-[11px] text-slate-500">
        <span>
          {lastYieldParts.length > 0 ? lastYieldParts.join(' · ') : 'No refresh yet'}
          {lastCheck ? ` · ${formatTimestamp(lastCheck)}` : null}
        </span>
        <ChevronRight className="h-3.5 w-3.5 text-slate-400 transition group-hover:text-alma-600" />
      </div>
    </article>
  )
}
