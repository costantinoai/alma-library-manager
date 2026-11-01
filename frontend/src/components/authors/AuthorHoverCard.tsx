import { useQuery } from '@tanstack/react-query'
import { ExternalLink, Loader2, UserPlus, UserRoundCheck } from 'lucide-react'

import { HoverCard, HoverCardContent, HoverCardTrigger } from '@/components/ui/hover-card'
import { Button } from '@/components/ui/button'
import { SignalChip } from '@/components/shared/SignalChip'
import { StatusBadge } from '@/components/ui/status-badge'
import { lookupAuthorByName, type Author } from '@/api/client'
import { SubPanel } from '@/components/ui/sub-panel'
import { usePaperAuthorFollow } from '@/hooks/usePaperAuthorFollow'
import { normalizeAuthorName } from '@/lib/utils'

interface AuthorHoverCardProps {
  name: string
  /** The paper this name was read from. Following a name with no author row
   *  yet needs it: the backend resolves the person from that paper. */
  paperId?: string
  children: React.ReactNode
}

function formatCount(value: number | null | undefined): string {
  if (!value) return '—'
  if (value >= 1000) return `${(value / 1000).toFixed(1)}k`
  return String(value)
}

/**
 * The card under a name. It owns Follow and Unfollow itself: the buttons used to
 * depend on each caller passing handlers, and PaperCard never passed Unfollow
 * and most surfaces passed no Follow, so they sat greyed out. It mounts only
 * while open, so a page of 300 names holds no queries.
 */
function AuthorHoverBody({ name, paperId }: { name: string; paperId?: string }) {
  const {
    followedAuthorIds,
    followedAuthorIdByName,
    pendingAuthorName,
    followAuthor,
    unfollowAuthor,
  } = usePaperAuthorFollow()

  const lookup = useQuery({
    queryKey: ['author-lookup', name.toLowerCase()],
    queryFn: () => lookupAuthorByName(name),
    enabled: name.trim().length > 0,
    retry: false,
    staleTime: 60_000,
  })

  const author: Author | undefined = lookup.data
  const topInterests = (author?.interests ?? []).slice(0, 3)
  const notInCorpus = lookup.isError
  const normalizedName = normalizeAuthorName(name)
  // A paper may spell a followed author differently, so the resolved id counts too.
  const followedId =
    followedAuthorIdByName.get(normalizedName) ??
    (author && followedAuthorIds.has(author.id) ? author.id : undefined)
  const isFollowed = followedId !== undefined
  const isPending = pendingAuthorName === normalizedName

  return (
    <div className="space-y-2.5">
      {/* Header */}
      <div className="flex items-start justify-between gap-3">
        <div className="min-w-0 flex-1">
          <p className="truncate text-sm font-semibold text-alma-800">{name}</p>
          {author?.affiliation && (
            <p className="truncate text-caption text-slate-500" title={author.affiliation}>
              {author.affiliation}
            </p>
          )}
        </div>
        {/* Following is an ACTIVE state you chose — it reads as positive,
            not as grey chrome (the app-wide chip contract: colour = "is
            this good news"). */}
        {isFollowed && (
          <StatusBadge tone="positive" size="sm" icon={UserRoundCheck} className="shrink-0">
            Following
          </StatusBadge>
        )}
      </div>

      {/* Metrics */}
      {lookup.isLoading && (
        <p className="inline-flex items-center gap-1.5 text-caption text-slate-400">
          <Loader2 className="h-3 w-3 animate-spin" /> Looking up corpus record…
        </p>
      )}

      {author && (
        <>
          <SubPanel variant="flat" padded={false}  className="grid grid-cols-3 gap-2 px-2 py-1.5 text-center">
            <div>
              <p className="text-2xs uppercase tracking-wide text-slate-500">h-index</p>
              <p className="text-sm font-semibold tabular-nums text-alma-800">
                {author.h_index ?? '—'}
              </p>
            </div>
            <div>
              <p className="text-2xs uppercase tracking-wide text-slate-500">Papers</p>
              <p className="text-sm font-semibold tabular-nums text-alma-800">
                {formatCount(author.works_count ?? author.publication_count)}
              </p>
            </div>
            <div>
              <p className="text-2xs uppercase tracking-wide text-slate-500">Cites</p>
              <p className="text-sm font-semibold tabular-nums text-alma-800">
                {formatCount(author.citedby)}
              </p>
            </div>
          </SubPanel>

          {topInterests.length > 0 && (
            <div className="flex flex-wrap gap-1">
              {topInterests.map((topic, i) => (
                <SignalChip
                  key={topic}
                  kind="topic"
                  hideIcon={i > 0}
                  title={`Research interest: ${topic}`}
                  className="font-normal"
                >
                  {topic}
                </SignalChip>
              ))}
            </div>
          )}
        </>
      )}

      {notInCorpus && (
        <SubPanel variant="flat" padded={false}  className="border-dashed px-2 py-1.5 text-caption text-slate-500">
          Not in your authors corpus yet. Follow to start tracking their new papers.
        </SubPanel>
      )}

      {/* Actions */}
      <div className="flex items-center gap-2 border-t border-slate-100 pt-2">
        {isFollowed ? (
          <Button
            type="button"
            variant="outline"
            size="sm"
            className="h-7 px-2 text-xs"
            onClick={() => unfollowAuthor(followedId, name)}
            disabled={isPending}
          >
            {isPending && <Loader2 className="mr-1 h-3 w-3 animate-spin" />}
            Unfollow
          </Button>
        ) : paperId ? (
          <Button
            type="button"
            variant="default"
            size="sm"
            className="h-7 px-2 text-xs"
            onClick={() => followAuthor(name, paperId)}
            disabled={isPending}
          >
            {isPending ? (
              <Loader2 className="mr-1 h-3 w-3 animate-spin" />
            ) : (
              <UserPlus className="mr-1 h-3 w-3" />
            )}
            Follow
          </Button>
        ) : null}
        <a
          href={`#/authors?q=${encodeURIComponent(name)}`}
          className="ml-auto inline-flex items-center gap-0.5 text-caption font-medium text-alma-700 hover:text-alma-900"
          onClick={(e) => e.stopPropagation()}
        >
          Open in Authors
          <ExternalLink className="h-3 w-3" />
        </a>
      </div>
    </div>
  )
}

export function AuthorHoverCard({ name, paperId, children }: AuthorHoverCardProps) {
  return (
    <HoverCard openDelay={250} closeDelay={100}>
      <HoverCardTrigger asChild>{children}</HoverCardTrigger>
      <HoverCardContent
        side="top"
        align="start"
        className="w-80 p-3"
        onClick={(e) => e.stopPropagation()}
      >
        <AuthorHoverBody name={name} paperId={paperId} />
      </HoverCardContent>
    </HoverCard>
  )
}
