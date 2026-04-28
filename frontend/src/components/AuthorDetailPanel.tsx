import { type ComponentType, useState } from 'react'
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query'
import {
  AlertCircle,
  BookOpen,
  Compass,
  Database,
  ExternalLink,
  Loader2,
  Newspaper,
  RefreshCw,
  Trash2,
  UserMinus,
  UserPlus,
} from 'lucide-react'

import {
  addToLibrary,
  api,
  followAuthor,
  getAuthorDetail,
  listAuthorOpenAlexWorks,
  saveOpenAlexWork,
  unfollowAuthor,
  updateReadingStatus,
  type Author,
  type AuthorDetail,
  type AuthorSuggestion,
  type OpenAlexWork,
  type Publication,
} from '@/api/client'
import { PaperActionBar, type PaperReaction } from '@/components/discovery/PaperActionBar'
import { PaperCard, type PaperCardPaper } from '@/components/shared'
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from '@/components/ui/select'
import { Alert, AlertDescription } from '@/components/ui/alert'
import { Badge } from '@/components/ui/badge'
import { Button } from '@/components/ui/button'
import { LoadingState } from '@/components/ui/LoadingState'
import {
  Dialog,
  DialogContent,
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from '@/components/ui/dialog'
import { Tabs, TabsContent, TabsList, TabsTrigger } from '@/components/ui/tabs'
import { StatusBadge, monitorHealthTone } from '@/components/ui/status-badge'
import { AuthorSignalBar } from '@/components/authors/AuthorSignalBar'
import { AuthorIdentifierResolution } from '@/components/authors/AuthorIdentifierResolution'
import { StarRating } from '@/components/StarRating'
import { useToast, errorToast } from '@/hooks/useToast'
import { buildHashRoute, navigateTo } from '@/lib/hashRoute'
import { invalidateQueries } from '@/lib/queryHelpers'
import { formatDate, formatNumber, truncate } from '@/lib/utils'

interface AuthorDetailPanelProps {
  author: Author | null
  open: boolean
  onOpenChange: (open: boolean) => void
  /**
   * When the dialog is opened from the Suggested rail, no local author row
   * exists in the DB yet, so the detail fetch will 404. Passing the
   * originating suggestion lets the overview stay populated (sample titles,
   * shared topics) and hides the error state.
   */
  suggestion?: AuthorSuggestion | null
  /**
   * Invoked after a destructive action (delete) resolves. The parent is
   * expected to close the dialog and invalidate its author queries.
   */
  onDeleted?: (author: Author) => void
}

type Scope = 'all' | 'library' | 'background' | 'openalex'

function MetricCard({
  label,
  value,
  icon: Icon,
}: {
  label: string
  value: string | number
  icon: ComponentType<{ className?: string }>
}) {
  return (
    <div className="rounded-sm border border-[var(--color-border)] bg-[#FFFEF7] p-3 shadow-paper-sm">
      <div className="flex items-center gap-2 text-xs font-medium uppercase tracking-wide text-slate-500">
        <Icon className="h-3.5 w-3.5" />
        {label}
      </div>
      <p className="mt-2 text-xl font-semibold text-alma-800">{value}</p>
    </div>
  )
}

function scopeDescription(scope: Scope): string {
  if (scope === 'library') return 'Curated library papers only.'
  if (scope === 'background') return 'Tracked background corpus outside the curated library.'
  if (scope === 'openalex')
    return "Full bibliography pulled live from OpenAlex. Save individual works to add them to your local DB."
  return 'All papers in your local DB for this author.'
}

function ratingToReaction(rating: number | null | undefined): PaperReaction {
  // Mirror the canonical add/like/love/dislike → 3/4/5/1 contract. Rating 3
  // ("add") is the baseline save state and does not light up a reaction
  // pill.
  if (rating === 4) return 'like'
  if (rating === 5) return 'love'
  if (rating === 1) return 'dislike'
  return null
}

function PublicationRow({
  publication,
  onRate,
  onReading,
}: {
  publication: Publication
  onRate: (paperId: string, rating: number) => void
  onReading: (paperId: string, status: 'reading' | 'done' | 'excluded' | null) => void
}) {
  const isSaved = publication.status === 'library'
  const reaction = ratingToReaction(publication.rating)
  const cardPaper: PaperCardPaper = {
    id: publication.id,
    title: publication.title,
    authors: publication.authors,
    year: publication.year ?? null,
    journal: publication.journal ?? undefined,
    url: publication.url ?? undefined,
    doi: publication.doi ?? undefined,
    publication_date: publication.publication_date ?? undefined,
    cited_by_count: publication.cited_by_count ?? 0,
    rating: publication.rating ?? undefined,
    tldr: publication.tldr ?? null,
    influential_citation_count: publication.influential_citation_count ?? 0,
    global_signal_score: publication.global_signal_score ?? 0,
    status: publication.status ?? undefined,
  }
  const readingStatusSlot = (
    <Select
      value={publication.reading_status ?? '__none__'}
      onValueChange={(v) =>
        onReading(
          publication.id,
          v === '__none__' ? null : (v as 'reading' | 'done' | 'excluded'),
        )
      }
    >
      <SelectTrigger className="h-7 w-[130px] text-xs">
        <SelectValue placeholder="Reading" />
      </SelectTrigger>
      <SelectContent>
        <SelectItem value="__none__">Not on list</SelectItem>
        <SelectItem value="reading">Reading</SelectItem>
        <SelectItem value="done">Done</SelectItem>
        <SelectItem value="excluded">Excluded</SelectItem>
      </SelectContent>
    </Select>
  )
  return (
    <PaperCard
      paper={cardPaper}
      size="compact"
      isSaved={isSaved}
      reaction={reaction}
      readingStatusSlot={readingStatusSlot}
      onAdd={() => onRate(publication.id, 3)}
      onLike={() => onRate(publication.id, 4)}
      onLove={() => onRate(publication.id, 5)}
    />
  )
}

function OpenAlexWorkRow({
  work,
  onSave,
  pending,
}: {
  work: OpenAlexWork
  onSave: (action: 'add' | 'like' | 'love') => void
  pending: boolean
}) {
  const savedReaction: PaperReaction = work.already_in_db
    ? ratingToReaction(work.local_rating)
    : null
  const savedInLibrary = work.already_in_db && work.local_status === 'library'

  return (
    <div className="space-y-2 rounded-lg border border-slate-100 bg-[#FFFEF7] p-3 shadow-sm">
      <div className="flex items-start justify-between gap-3">
        <div className="min-w-0 flex-1">
          <p className="text-sm font-medium text-alma-800">
            {truncate(work.title ?? 'Untitled', 140)}
          </p>
          <div className="mt-1 flex flex-wrap items-center gap-2 text-xs text-slate-500">
            {/* Date renders as inline text (same shape as PaperCard's
                meta line: "Feb 2024" via en-GB short month). Dropping
                the date Badge keeps the meta strip flat — bubbles in
                this row are reserved for membership state, not
                metadata. */}
            {(() => {
              const pubDate = (work.publication_date || '').trim()
              if (pubDate) {
                const parsed = new Date(pubDate)
                if (!isNaN(parsed.getTime())) {
                  return (
                    <span className="tabular-nums" title={pubDate}>
                      {parsed.toLocaleDateString('en-GB', { month: 'short', year: 'numeric' })}
                    </span>
                  )
                }
              }
              if (work.year != null) return <span className="tabular-nums">{work.year}</span>
              return null
            })()}
            {work.journal ? <span>{truncate(work.journal, 40)}</span> : null}
            {(work.cited_by_count ?? 0) > 0 ? (
              <span>{formatNumber(work.cited_by_count ?? 0)} cited</span>
            ) : null}
            {/* Membership chips ride the brand Folio-blue translucent
                tone (`info`) so they read as "metadata stamp" against
                the off-white row, consistent with the rest of the
                Authors / Suggestions surfaces. */}
            {work.already_in_db ? (
              <StatusBadge tone="info" size="sm">
                In DB{work.local_status === 'library' ? ' · Library' : ''}
              </StatusBadge>
            ) : (
              <StatusBadge tone="info" size="sm">
                OpenAlex
              </StatusBadge>
            )}
          </div>
        </div>
        {work.url ? (
          <a
            href={work.url}
            target="_blank"
            rel="noopener noreferrer"
            className="shrink-0 rounded-md p-1 text-slate-400 hover:text-alma-600"
            aria-label="Open work on OpenAlex"
          >
            <ExternalLink className="h-3.5 w-3.5" />
          </a>
        ) : null}
      </div>
      <div className="flex items-center justify-between gap-2 pt-1">
        <PaperActionBar
          compact
          isSaved={savedInLibrary}
          reaction={savedReaction}
          onAdd={() => onSave('add')}
          onLike={() => onSave('like')}
          onLove={() => onSave('love')}
          disabled={pending}
        />
      </div>
    </div>
  )
}

/**
 * Author detail primitive — the single place every Authors surface
 * (Suggested / Followed / Corpus) opens on row click. Mirrors
 * `PaperDetailPanel`: controlled open state, lazy fetch on open, graceful
 * fallback to the caller-provided Author record if the detail request
 * fails.
 *
 * Three tabs:
 *   Overview      — profile metrics, signal bar, top topics, monitor state
 *   Publications  — existing publications list with scope filter
 *   Identifiers   — OpenAlex / Scholar candidate resolution diagnostics
 */
export function AuthorDetailPanel({
  author,
  open,
  onOpenChange,
  suggestion,
  onDeleted,
}: AuthorDetailPanelProps) {
  const queryClient = useQueryClient()
  const { toast } = useToast()

  // If this dialog was opened from a Suggested card, the `author` object is
  // synthesized from the suggestion and has no row in the local DB — the
  // detail and publications endpoints will 404. Skip those fetches but
  // keep the OpenAlex bibliography scope accessible so the user can
  // browse their work before following.
  const isSuggestionOnly = !!suggestion && !author?.added_at
  const [scope, setScope] = useState<Scope>(isSuggestionOnly ? 'openalex' : 'all')

  const detailQuery = useQuery({
    queryKey: ['author-detail', author?.id],
    queryFn: () => getAuthorDetail(author!.id),
    enabled: open && !!author?.id && !isSuggestionOnly,
    retry: false,
  })

  const pubsQuery = useQuery({
    queryKey: ['author-publications', author?.id, scope],
    queryFn: () =>
      api.get<Publication[]>(
        `/authors/${encodeURIComponent(author!.id)}/publications?scope=${scope}&order=recent`,
      ),
    // Only fetch local publications for the three local scopes; 'openalex'
    // has its own query below and suggestion-opened dialogs have no local
    // author row at all.
    enabled: open && !!author?.id && !isSuggestionOnly && scope !== 'openalex',
    retry: false,
  })

  const openalexQuery = useQuery({
    queryKey: ['author-openalex-works', author?.id],
    queryFn: () => listAuthorOpenAlexWorks(author!.id, { perPage: 50 }),
    // OpenAlex lookup works from a bare author OpenAlex id too, so we
    // allow suggestion-opened dialogs to fetch it (unlike the local
    // /publications endpoint).
    enabled: open && !!author?.id && scope === 'openalex',
    retry: false,
    staleTime: 60_000,
  })

  const fallbackAuthor = author
  const detail: AuthorDetail | null = detailQuery.data ?? null
  const resolved: Author = detail?.author ?? (fallbackAuthor as Author)
  const isFollowed = resolved?.author_type === 'followed'

  const followMutation = useMutation({
    mutationFn: () => followAuthor(resolved.id),
    onSuccess: () => {
      void invalidateQueries(
        queryClient,
        ['authors'],
        ['library-followed-authors'],
        ['author-suggestions'],
        ['author-detail', resolved.id],
      )
      toast({ title: 'Followed', description: `${resolved.name} is now followed.` })
    },
    onError: () => errorToast('Error', 'Failed to follow author.'),
  })

  const unfollowMutation = useMutation({
    mutationFn: () => unfollowAuthor(resolved.id),
    onSuccess: () => {
      void invalidateQueries(
        queryClient,
        ['authors'],
        ['library-followed-authors'],
        ['author-detail', resolved.id],
      )
      toast({ title: 'Unfollowed', description: `${resolved.name} is no longer followed.` })
    },
    onError: () => errorToast('Error', 'Failed to unfollow author.'),
  })

  // Deep refresh consolidation (2026-04-24): `deep-refresh` and
  // `history-backfill` now share the same backend operation_key
  // (`authors.deep_refresh:{id}`) and route through the modern
  // OpenAlex-backed backfill. One button, one job per author.
  const refreshMutation = useMutation({
    mutationFn: () =>
      api.post<{ status?: string; job_id?: string }>(
        `/authors/${encodeURIComponent(resolved.id)}/deep-refresh`,
      ),
    onSuccess: (data) => {
      void invalidateQueries(
        queryClient,
        ['authors'],
        ['author-detail', resolved.id],
        ['author-publications', resolved.id],
        ['activity-operations'],
      )
      toast({
        title: data?.status === 'already_running' ? 'Refresh already running' : 'Refresh queued',
        description: data?.job_id
          ? `Job ${data.job_id} active for ${resolved.name}.`
          : undefined,
      })
    },
    onError: () => errorToast('Error', 'Failed to start author refresh.'),
  })

  const deleteMutation = useMutation({
    mutationFn: () => api.delete(`/authors/${encodeURIComponent(resolved.id)}`),
    onSuccess: () => {
      void invalidateQueries(queryClient, ['authors'], ['library-followed-authors'])
      toast({ title: 'Deleted', description: `Author ${resolved.name} has been deleted.` })
      onDeleted?.(resolved)
      onOpenChange(false)
    },
    onError: () => errorToast('Error', 'Failed to delete author.'),
  })

  const rateMutation = useMutation({
    mutationFn: ({ paperId, rating }: { paperId: string; rating: number }) =>
      addToLibrary(paperId, rating),
    onSuccess: (_data, { rating }) => {
      void invalidateQueries(
        queryClient,
        ['author-publications', resolved?.id],
        ['papers'],
        ['likes'],
        ['library-workflow'],
      )
      const label = rating === 5 ? 'Loved' : rating === 4 ? 'Liked' : 'Saved to Library'
      toast({ title: label })
    },
    onError: () => errorToast('Error', 'Failed to update rating.'),
  })

  const saveOpenAlexMutation = useMutation({
    mutationFn: ({
      work,
      action,
    }: {
      work: OpenAlexWork
      action: 'add' | 'like' | 'love'
    }) =>
      saveOpenAlexWork({
        openalex_id: work.openalex_id ?? work.id ?? null,
        doi: work.doi ?? null,
        action,
      }),
    onSuccess: (_data, { action }) => {
      void invalidateQueries(
        queryClient,
        ['author-openalex-works', resolved?.id],
        ['author-publications', resolved?.id],
        ['papers'],
        ['likes'],
        ['library-workflow'],
      )
      const label = action === 'love' ? 'Loved' : action === 'like' ? 'Liked' : 'Saved to Library'
      toast({ title: label })
    },
    onError: () => errorToast('Error', 'Failed to save from OpenAlex.'),
  })

  const readingMutation = useMutation({
    mutationFn: ({
      paperId,
      status,
    }: {
      paperId: string
      status: 'reading' | 'done' | 'excluded' | null
    }) => updateReadingStatus(paperId, status),
    onSuccess: () => {
      void invalidateQueries(
        queryClient,
        ['author-publications', resolved?.id],
        ['papers'],
        ['library-workflow'],
      )
    },
    onError: () => errorToast('Error', 'Failed to update reading status.'),
  })

  if (!author) return null

  const monitorHealth = resolved.monitor_health ?? (isFollowed ? 'degraded' : undefined)
  const topTopics = detail?.top_topics ?? []
  const publications = pubsQuery.data ?? []
  const backfill = detail?.backfill ?? null

  return (
    <Dialog open={open} onOpenChange={onOpenChange}>
      <DialogContent className="max-w-3xl">
        <DialogHeader>
          {/* `pr-12` on the inner row leaves clearance for the Dialog
              primitive's auto-injected close (X) button at right-4 top-4
              — without it the Follow / Unfollow chip overlaps the X. */}
          <div className="flex items-start justify-between gap-4 pr-12">
            <div className="min-w-0 flex-1">
              <DialogTitle className="text-lg leading-tight">{resolved.name}</DialogTitle>
              <p className="mt-1 text-sm text-slate-500">
                {resolved.affiliation ?? 'No affiliation on record'}
              </p>
            </div>
            <div className="shrink-0">
              {isFollowed ? (
                <Button
                  size="sm"
                  variant="outline"
                  onClick={() => unfollowMutation.mutate()}
                  disabled={unfollowMutation.isPending}
                  className="border-rose-200 text-rose-700 hover:bg-rose-50 hover:text-rose-800"
                >
                  {unfollowMutation.isPending ? (
                    <Loader2 className="h-4 w-4 animate-spin" />
                  ) : (
                    <UserMinus className="h-4 w-4" />
                  )}
                  Unfollow
                </Button>
              ) : (
                <Button
                  size="sm"
                  onClick={() => followMutation.mutate()}
                  disabled={followMutation.isPending}
                  className="bg-alma-folio text-white hover:bg-alma-folio/90"
                >
                  {followMutation.isPending ? (
                    <Loader2 className="h-4 w-4 animate-spin" />
                  ) : (
                    <UserPlus className="h-4 w-4" />
                  )}
                  Follow
                </Button>
              )}
            </div>
          </div>
        </DialogHeader>

        <Tabs defaultValue="overview" className="max-h-[70vh] flex-col">
          <TabsList className={isSuggestionOnly ? 'grid w-full grid-cols-2' : 'grid w-full grid-cols-3'}>
            <TabsTrigger value="overview">Overview</TabsTrigger>
            <TabsTrigger value="publications">Publications</TabsTrigger>
            {!isSuggestionOnly ? <TabsTrigger value="identifiers">Identifiers</TabsTrigger> : null}
          </TabsList>

          <TabsContent value="overview" className="mt-4 space-y-4 overflow-y-auto pr-1">
            {detailQuery.isLoading ? (
              <LoadingState message="Loading detail..." />
            ) : (
              <>
                {isSuggestionOnly ? (
                  <Alert variant="info" className="px-3 py-2">
                    <AlertDescription className="text-xs">
                      Not yet in your DB — shown from the suggestion payload. Follow to pull the full profile and publications.
                    </AlertDescription>
                  </Alert>
                ) : null}
                <div className="rounded-sm border border-[var(--color-border)] bg-[#FFFEF7] p-4 shadow-paper-sm">
                  <div className="flex items-center justify-between gap-3">
                    <p className="text-xs font-medium uppercase tracking-wide text-slate-500">
                      Author signal
                    </p>
                    {monitorHealth ? (
                      <StatusBadge tone={monitorHealthTone(monitorHealth)} size="sm">
                        Monitor {monitorHealth}
                      </StatusBadge>
                    ) : null}
                  </div>
                  <AuthorSignalBar signal={detail?.signal ?? null} className="mt-3" />
                </div>

                <div className="grid gap-3 md:grid-cols-4">
                  <MetricCard label="h-index" value={resolved.h_index ?? '—'} icon={Database} />
                  <MetricCard
                    label="Citations"
                    value={resolved.citedby != null ? formatNumber(resolved.citedby) : '—'}
                    icon={BookOpen}
                  />
                  <MetricCard
                    label="Works"
                    value={resolved.works_count != null ? formatNumber(resolved.works_count) : '—'}
                    icon={Newspaper}
                  />
                  <MetricCard
                    label="In DB"
                    value={resolved.publication_count ?? 0}
                    icon={Database}
                  />
                </div>

                <div className="rounded-sm border border-[var(--color-border)] bg-[#FFFEF7] p-4 shadow-paper-sm">
                  <p className="text-xs font-medium uppercase tracking-wide text-slate-500">
                    {isSuggestionOnly ? 'Shared topics' : 'Top topics'}
                  </p>
                  <div className="mt-3 flex flex-wrap gap-1.5">
                    {topTopics.length > 0 ? (
                      topTopics.map((t) => (
                        <Badge key={t.term} variant="outline">
                          {t.term} · {t.papers}
                        </Badge>
                      ))
                    ) : isSuggestionOnly && suggestion && suggestion.shared_topics.length > 0 ? (
                      suggestion.shared_topics.map((t) => (
                        <Badge key={t} variant="outline">
                          {t}
                        </Badge>
                      ))
                    ) : (
                      <span className="text-sm text-slate-400">No topic profile yet.</span>
                    )}
                  </div>
                </div>

                {isSuggestionOnly && suggestion && suggestion.sample_titles.length > 0 ? (
                  <div className="rounded-sm border border-[var(--color-border)] bg-[#FFFEF7] p-4 shadow-paper-sm">
                    <p className="text-xs font-medium uppercase tracking-wide text-slate-500">
                      Sample titles
                    </p>
                    <ul className="mt-3 space-y-1.5 text-sm text-slate-700">
                      {suggestion.sample_titles.slice(0, 5).map((title) => (
                        <li key={title} className="line-clamp-2">
                          {title}
                        </li>
                      ))}
                    </ul>
                  </div>
                ) : null}

                {backfill ? (
                  <div className="rounded-sm border border-[var(--color-border)] bg-[#FFFEF7] p-4 shadow-paper-sm">
                    <div className="flex items-center justify-between gap-3">
                      <p className="text-xs font-medium uppercase tracking-wide text-slate-500">
                        Background corpus
                      </p>
                      {backfill.state ? (
                        <StatusBadge
                          tone={
                            backfill.state === 'fresh'
                              ? 'positive'
                              : backfill.state === 'running'
                                ? 'info'
                                : 'warning'
                          }
                          size="sm"
                        >
                          {backfill.state.replace(/_/g, ' ')}
                        </StatusBadge>
                      ) : null}
                    </div>
                    {backfill.detail ? (
                      <p className="mt-2 text-sm text-slate-600">{backfill.detail}</p>
                    ) : null}
                    <div className="mt-2 flex flex-wrap gap-3 text-xs text-slate-500">
                      {backfill.last_success_at ? (
                        <span>Last success: {formatDate(backfill.last_success_at)}</span>
                      ) : null}
                      {backfill.coverage_ratio != null ? (
                        <span>Coverage: {(backfill.coverage_ratio * 100).toFixed(0)}%</span>
                      ) : null}
                    </div>
                  </div>
                ) : null}

                <div className="flex flex-wrap gap-2">
                  {resolved.scholar_id ? (
                    <a
                      href={`https://scholar.google.com/citations?user=${resolved.scholar_id}`}
                      target="_blank"
                      rel="noopener noreferrer"
                      className="inline-flex items-center gap-1 rounded-md border border-[var(--color-border)] bg-[#FFFEF7] px-3 py-1.5 text-xs font-medium text-slate-700 hover:bg-parchment-50"
                    >
                      Google Scholar <ExternalLink className="h-3 w-3" />
                    </a>
                  ) : null}
                  {resolved.openalex_id ? (
                    <a
                      href={`https://openalex.org/${resolved.openalex_id}`}
                      target="_blank"
                      rel="noopener noreferrer"
                      className="inline-flex items-center gap-1 rounded-md border border-[var(--color-border)] bg-[#FFFEF7] px-3 py-1.5 text-xs font-medium text-slate-700 hover:bg-parchment-50"
                    >
                      OpenAlex <ExternalLink className="h-3 w-3" />
                    </a>
                  ) : null}
                </div>
              </>
            )}
          </TabsContent>

          <TabsContent value="publications" className="mt-4 overflow-y-auto pr-1">
            <div className="mb-3 space-y-2">
              <p className="text-xs text-slate-500">{scopeDescription(scope)}</p>
              <div className="flex flex-wrap gap-2">
                {(['all', 'library', 'background', 'openalex'] as Scope[]).map((value) => {
                  // Suggestion-opened dialogs have no local author row, so
                  // the local scopes would always return empty. Keep only
                  // the OpenAlex button active.
                  const disabled = isSuggestionOnly && value !== 'openalex'
                  return (
                    <Button
                      key={value}
                      size="sm"
                      variant={scope === value ? 'default' : 'outline'}
                      onClick={() => setScope(value)}
                      disabled={disabled}
                      title={disabled ? 'Not in your DB yet — follow first, or use OpenAlex' : undefined}
                    >
                      {value === 'openalex' ? 'OpenAlex (all)' : value}
                    </Button>
                  )
                })}
              </div>
              {openalexQuery.data?.total != null && scope === 'openalex' ? (
                <p className="text-[11px] text-slate-500">
                  Showing {openalexQuery.data.results.length} of {formatNumber(openalexQuery.data.total)}
                  {' '}works from OpenAlex
                </p>
              ) : null}
            </div>

            {scope === 'openalex' ? (
              openalexQuery.isLoading ? (
                <div className="flex items-center justify-center py-6">
                  <Loader2 className="h-5 w-5 animate-spin text-alma-600" />
                  <span className="ml-2 text-sm text-slate-500">Fetching from OpenAlex...</span>
                </div>
              ) : openalexQuery.isError ? (
                <div className="flex items-center gap-2 rounded-lg border border-red-200 bg-red-50 px-3 py-4">
                  <AlertCircle className="h-4 w-4 text-red-500" />
                  <span className="text-sm text-red-700">
                    Failed to fetch OpenAlex bibliography. This author may not have an OpenAlex ID.
                  </span>
                </div>
              ) : (openalexQuery.data?.results ?? []).length === 0 ? (
                <div className="py-6 text-center">
                  <BookOpen className="mx-auto h-8 w-8 text-slate-300" />
                  <p className="mt-2 text-sm text-slate-400">
                    OpenAlex returned no works for this author.
                  </p>
                </div>
              ) : (
                <div className="max-h-[60vh] space-y-2">
                  {(openalexQuery.data?.results ?? []).map((work, idx) => (
                    <OpenAlexWorkRow
                      key={work.openalex_id ?? work.id ?? work.doi ?? idx}
                      work={work}
                      pending={
                        saveOpenAlexMutation.isPending &&
                        saveOpenAlexMutation.variables?.work.openalex_id ===
                          (work.openalex_id ?? work.id)
                      }
                      onSave={(action) => saveOpenAlexMutation.mutate({ work, action })}
                    />
                  ))}
                </div>
              )
            ) : pubsQuery.isLoading ? (
              <div className="flex items-center justify-center py-6">
                <Loader2 className="h-5 w-5 animate-spin text-alma-600" />
                <span className="ml-2 text-sm text-slate-500">Loading publications...</span>
              </div>
            ) : pubsQuery.isError ? (
              <div className="flex items-center gap-2 rounded-lg border border-red-200 bg-red-50 px-3 py-4">
                <AlertCircle className="h-4 w-4 text-red-500" />
                <span className="text-sm text-red-700">Failed to load publications.</span>
              </div>
            ) : publications.length === 0 ? (
              <div className="py-6 text-center">
                <BookOpen className="mx-auto h-8 w-8 text-slate-300" />
                <p className="mt-2 text-sm text-slate-400">
                  No publications found for this author in this scope.
                </p>
              </div>
            ) : (
              <div className="max-h-[60vh] space-y-2">
                {publications.map((pub) => (
                  <PublicationRow
                    key={pub.id}
                    publication={pub}
                    onRate={(paperId, rating) => rateMutation.mutate({ paperId, rating })}
                    onReading={(paperId, status) => readingMutation.mutate({ paperId, status })}
                  />
                ))}
              </div>
            )}
          </TabsContent>

          <TabsContent value="identifiers" className="mt-4 overflow-y-auto pr-1">
            <AuthorIdentifierResolution author={resolved} />
          </TabsContent>
        </Tabs>

        <DialogFooter className="mt-2 flex flex-wrap items-center gap-2">
          <Button
            size="sm"
            variant="outline"
            onClick={() => refreshMutation.mutate()}
            disabled={refreshMutation.isPending}
            title="Re-fetch profile + publications from OpenAlex; backfills missing SPECTER2 vectors and recomputes the author centroid."
          >
            {refreshMutation.isPending ? (
              <Loader2 className="h-4 w-4 animate-spin" />
            ) : (
              <RefreshCw className="h-4 w-4" />
            )}
            Refresh author
          </Button>
          <Button
            size="sm"
            variant="outline"
            onClick={() => navigateTo('discovery', { query: `author:${resolved.name}` })}
          >
            <Compass className="h-4 w-4" />
            Open in Discovery
          </Button>
          <div className="ml-auto">
            <Button
              size="sm"
              variant="outline"
              onClick={() => deleteMutation.mutate()}
              disabled={deleteMutation.isPending}
              className="text-red-600 hover:bg-red-50 hover:text-red-700"
            >
              {deleteMutation.isPending ? (
                <Loader2 className="h-4 w-4 animate-spin" />
              ) : (
                <Trash2 className="h-4 w-4" />
              )}
              Delete
            </Button>
          </div>
        </DialogFooter>
      </DialogContent>
    </Dialog>
  )
}

// buildHashRoute is re-exported here so the caller can deep-link to the
// dialog if needed; keeping the import prevents tree-shake from removing
// it when only navigateTo is used above.
export { buildHashRoute }
