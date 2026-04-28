import { useState } from 'react'
import { useQuery } from '@tanstack/react-query'
import { ExternalLink, Loader2, UserPlus, UserRoundCheck } from 'lucide-react'

import { HoverCard, HoverCardContent, HoverCardTrigger } from '@/components/ui/hover-card'
import { Button } from '@/components/ui/button'
import { Badge } from '@/components/ui/badge'
import { lookupAuthorByName, type Author } from '@/api/client'

interface AuthorHoverCardProps {
  name: string
  isFollowed?: boolean
  followPending?: boolean
  onFollow?: () => void
  onUnfollow?: () => void
  children: React.ReactNode
}

function formatCount(value: number | null | undefined): string {
  if (!value) return '—'
  if (value >= 1000) return `${(value / 1000).toFixed(1)}k`
  return String(value)
}

export function AuthorHoverCard({
  name,
  isFollowed = false,
  followPending = false,
  onFollow,
  onUnfollow,
  children,
}: AuthorHoverCardProps) {
  const [opened, setOpened] = useState(false)

  const lookup = useQuery({
    queryKey: ['author-lookup', name.toLowerCase()],
    queryFn: () => lookupAuthorByName(name),
    enabled: opened && name.trim().length > 0,
    retry: false,
    staleTime: 60_000,
  })

  const author: Author | undefined = lookup.data
  const topInterests = (author?.interests ?? []).slice(0, 3)
  const notInCorpus = opened && lookup.isError

  return (
    <HoverCard openDelay={250} closeDelay={100} onOpenChange={setOpened}>
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
              <p className="truncate text-sm font-semibold text-alma-800">{name}</p>
              {author?.affiliation && (
                <p className="truncate text-[11px] text-slate-500" title={author.affiliation}>
                  {author.affiliation}
                </p>
              )}
            </div>
            {isFollowed && (
              <Badge variant="outline" size="sm" className="shrink-0">
                <UserRoundCheck className="mr-1 h-3 w-3" /> Following
              </Badge>
            )}
          </div>

          {/* Metrics */}
          {lookup.isLoading && (
            <p className="inline-flex items-center gap-1.5 text-[11px] text-slate-400">
              <Loader2 className="h-3 w-3 animate-spin" /> Looking up corpus record…
            </p>
          )}

          {author && (
            <>
              <div className="grid grid-cols-3 gap-2 rounded-md border border-slate-100 bg-parchment-50/70 px-2 py-1.5 text-center">
                <div>
                  <p className="text-[10px] uppercase tracking-wide text-slate-500">h-index</p>
                  <p className="text-sm font-semibold tabular-nums text-alma-800">
                    {author.h_index ?? '—'}
                  </p>
                </div>
                <div>
                  <p className="text-[10px] uppercase tracking-wide text-slate-500">Papers</p>
                  <p className="text-sm font-semibold tabular-nums text-alma-800">
                    {formatCount(author.works_count ?? author.publication_count)}
                  </p>
                </div>
                <div>
                  <p className="text-[10px] uppercase tracking-wide text-slate-500">Cites</p>
                  <p className="text-sm font-semibold tabular-nums text-alma-800">
                    {formatCount(author.citedby)}
                  </p>
                </div>
              </div>

              {topInterests.length > 0 && (
                <div className="flex flex-wrap gap-1">
                  {topInterests.map((topic) => (
                    <Badge key={topic} variant="secondary" size="sm" className="font-normal">
                      {topic}
                    </Badge>
                  ))}
                </div>
              )}
            </>
          )}

          {notInCorpus && (
            <p className="rounded-md border border-dashed border-slate-200 bg-parchment-50/60 px-2 py-1.5 text-[11px] text-slate-500">
              Not in your authors corpus yet. Follow to start tracking their new papers.
            </p>
          )}

          {/* Actions */}
          <div className="flex items-center gap-2 border-t border-slate-100 pt-2">
            {isFollowed ? (
              <Button
                type="button"
                variant="outline"
                size="sm"
                className="h-7 px-2 text-xs"
                onClick={(e) => {
                  e.preventDefault()
                  e.stopPropagation()
                  onUnfollow?.()
                }}
                disabled={followPending || !onUnfollow}
              >
                {followPending && <Loader2 className="mr-1 h-3 w-3 animate-spin" />}
                Unfollow
              </Button>
            ) : (
              <Button
                type="button"
                variant="default"
                size="sm"
                className="h-7 px-2 text-xs"
                onClick={(e) => {
                  e.preventDefault()
                  e.stopPropagation()
                  onFollow?.()
                }}
                disabled={followPending || !onFollow}
              >
                {followPending ? (
                  <Loader2 className="mr-1 h-3 w-3 animate-spin" />
                ) : (
                  <UserPlus className="mr-1 h-3 w-3" />
                )}
                Follow
              </Button>
            )}
            <a
              href={`#/authors?q=${encodeURIComponent(name)}`}
              className="ml-auto inline-flex items-center gap-0.5 text-[11px] font-medium text-alma-700 hover:text-alma-900"
              onClick={(e) => e.stopPropagation()}
            >
              Open in Authors
              <ExternalLink className="h-3 w-3" />
            </a>
          </div>
        </div>
      </HoverCardContent>
    </HoverCard>
  )
}
