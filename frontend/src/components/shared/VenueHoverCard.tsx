import { useState } from 'react'
import { useQuery } from '@tanstack/react-query'
import { BookOpen, BookmarkCheck, Loader2 } from 'lucide-react'

import { HoverCard, HoverCardContent, HoverCardTrigger } from '@/components/ui/hover-card'
import { Button } from '@/components/ui/button'
import { StatusBadge } from '@/components/ui/status-badge'
import { Input } from '@/components/ui/input'
import { venueSearch, type VenueSearchResult } from '@/api/client'

/** Journals above this many total works are "big" — following everything would
 * flood the Journals feed, so we surface an optional keyword filter. (Total,
 * not per-year; a coarse but honest proxy for "this venue is high-volume".) */
const BIG_VENUE_WORKS = 5000

interface VenueHoverCardProps {
  journal: string
  isFollowed?: boolean
  followPending?: boolean
  onFollow?: (args: { sourceId: string; displayName: string; keywords?: string[] }) => void
  children: React.ReactNode
}

function formatCount(value: number | null | undefined): string {
  if (!value) return '—'
  if (value >= 1000) return `${(value / 1000).toFixed(1)}k`
  return String(value)
}

export function VenueHoverCard({
  journal,
  isFollowed = false,
  followPending = false,
  onFollow,
  children,
}: VenueHoverCardProps) {
  const [opened, setOpened] = useState(false)
  const [keywords, setKeywords] = useState('')

  const lookup = useQuery({
    queryKey: ['venue-lookup', journal.toLowerCase()],
    queryFn: () => venueSearch(journal),
    enabled: opened && journal.trim().length > 0,
    retry: false,
    staleTime: 60_000,
  })

  // Best match = first result (OpenAlex relevance order).
  const match: VenueSearchResult | undefined = lookup.data?.results?.[0]
  const isBigVenue = (match?.works_count ?? 0) > BIG_VENUE_WORKS
  const notFound = opened && !lookup.isLoading && !match

  const doFollow = () => {
    if (!match || !onFollow) return
    const parsed = keywords
      .split(',')
      .map((k) => k.trim())
      .filter(Boolean)
    onFollow({
      sourceId: match.source_id,
      displayName: match.display_name,
      keywords: parsed.length > 0 ? parsed : undefined,
    })
  }

  return (
    <HoverCard openDelay={250} closeDelay={120} onOpenChange={setOpened}>
      <HoverCardTrigger asChild>{children}</HoverCardTrigger>
      <HoverCardContent
        side="top"
        align="start"
        className="w-80 p-3"
        onClick={(e) => e.stopPropagation()}
      >
        <div className="space-y-2.5">
          {/* Header */}
          <div className="flex items-start justify-between gap-3">
            <div className="min-w-0 flex-1">
              <p className="flex items-center gap-1.5 truncate text-sm font-semibold text-alma-800">
                <BookOpen className="h-3.5 w-3.5 shrink-0 text-alma-folio" />
                {match?.display_name || journal}
              </p>
              {match?.type && (
                <p className="text-[11px] capitalize text-slate-500">
                  {match.type}
                  {match.issn_l ? ` · ISSN ${match.issn_l}` : ''}
                </p>
              )}
            </div>
            {/* Same positive "you chose this" state as the author card. */}
            {isFollowed && (
              <StatusBadge tone="positive" size="sm" icon={BookmarkCheck} className="shrink-0">
                Following
              </StatusBadge>
            )}
          </div>

          {lookup.isLoading && (
            <p className="inline-flex items-center gap-1.5 text-[11px] text-slate-400">
              <Loader2 className="h-3 w-3 animate-spin" /> Finding this journal…
            </p>
          )}

          {match && (
            <div className="grid grid-cols-2 gap-2 rounded-md border border-slate-100 bg-surface-2/70 px-2 py-1.5 text-center">
              <div>
                <p className="text-[10px] uppercase tracking-wide text-slate-500">Works</p>
                <p className="text-sm font-semibold tabular-nums text-alma-800">
                  {formatCount(match.works_count)}
                </p>
              </div>
              <div>
                <p className="text-[10px] uppercase tracking-wide text-slate-500">h-index</p>
                <p className="text-sm font-semibold tabular-nums text-alma-800">
                  {match.summary_stats?.h_index ?? '—'}
                </p>
              </div>
            </div>
          )}

          {notFound && (
            <p className="rounded-md border border-dashed border-slate-200 bg-surface-2/60 px-2 py-1.5 text-[11px] text-slate-500">
              No OpenAlex journal matched “{journal}”. Try following it from Settings → Feed Monitor
              Controls.
            </p>
          )}

          {/* Big-venue keyword nudge */}
          {match && !isFollowed && isBigVenue && (
            <div className="space-y-1.5 rounded-md border border-[var(--color-border)] bg-surface-2/60 p-2">
              <p className="text-[11px] text-slate-500">
                This journal publishes a lot. Add keywords to keep the Journals feed focused, or
                follow everything.
              </p>
              <Input
                value={keywords}
                onChange={(e) => setKeywords(e.target.value)}
                placeholder="e.g. attention, perception"
                className="h-7 text-xs"
                onClick={(e) => e.stopPropagation()}
              />
            </div>
          )}

          {/* Action */}
          {!isFollowed && (
            <div className="flex items-center gap-2 border-t border-slate-100 pt-2">
              <Button
                type="button"
                variant="default"
                size="sm"
                className="h-7 px-2 text-xs"
                onClick={(e) => {
                  e.preventDefault()
                  e.stopPropagation()
                  doFollow()
                }}
                disabled={followPending || !match || !onFollow}
              >
                {followPending ? (
                  <Loader2 className="mr-1 h-3 w-3 animate-spin" />
                ) : (
                  <BookOpen className="mr-1 h-3 w-3" />
                )}
                Follow journal
              </Button>
            </div>
          )}
        </div>
      </HoverCardContent>
    </HoverCard>
  )
}
