import { FolderOpen, Layers, Search, Sparkles, Tags } from 'lucide-react'

import type { Recommendation } from '@/api/client'
import { Badge } from '@/components/ui/badge'
import { Button } from '@/components/ui/button'
import { Card, CardContent } from '@/components/ui/card'
import { ErrorState } from '@/components/ui/ErrorState'
import { LoadingState } from '@/components/ui/LoadingState'

import { AuthorSuggestionCard } from './AuthorSuggestionCard'
import { type AuthorSuggestion } from './constants'
import { DiscoveryResultCard } from './DiscoveryResultCard'

// ── Empty states ──

function EmptyState() {
  return (
    <div className="py-16 text-center">
      <Sparkles className="mx-auto h-12 w-12 text-slate-300" />
      <h3 className="mt-4 text-lg font-medium text-slate-700">No recommendations yet</h3>
      <p className="mt-2 text-sm text-slate-400">
        Click "Generate Recommendations" to discover papers based on your library and followed authors.
      </p>
    </div>
  )
}

function NoFilterResults() {
  return (
    <div className="py-12 text-center">
      <Search className="mx-auto h-10 w-10 text-slate-300" />
      <h3 className="mt-3 text-base font-medium text-slate-600">No matching recommendations</h3>
      <p className="mt-1 text-sm text-slate-400">Try adjusting your filters or search text.</p>
    </div>
  )
}

// ── Paginated card grid ──

interface CardGridProps {
  title: string
  description: string
  totalCount: number
  emptyMessage: string
  recs: Recommendation[]
  visibleCount: number
  onLoadMore: () => void
  onLike: (id: string) => void
  onDismiss: (id: string) => void
  onSeen: (id: string) => void
  likeLoadingId: string | undefined
  dismissLoadingId: string | undefined
}

function RecommendationSection({
  title,
  description,
  totalCount,
  emptyMessage,
  recs,
  visibleCount,
  onLoadMore,
  onLike,
  onDismiss,
  onSeen,
  likeLoadingId,
  dismissLoadingId,
}: CardGridProps) {
  const visibleRecs = recs.slice(0, visibleCount)
  const hasMore = visibleCount < recs.length

  return (
    <Card className="border-slate-200">
      <CardContent className="p-4">
        <div className="mb-3 flex items-center justify-between gap-2">
          <div>
            <h3 className="text-sm font-semibold text-alma-800">{title}</h3>
            <p className="text-xs text-slate-500">{description}</p>
          </div>
          <Badge variant="outline">{totalCount}</Badge>
        </div>
        {recs.length === 0 ? (
          <p className="rounded-md border border-[var(--color-border)] bg-parchment-50 px-3 py-2 text-xs text-slate-500">
            {emptyMessage}
          </p>
        ) : (
          <>
            <div className="grid grid-cols-1 gap-4 xl:grid-cols-2">
              {visibleRecs.map((rec) => (
                <DiscoveryResultCard
                  key={rec.id}
                  rec={rec}
                  onLike={onLike}
                  onDismiss={onDismiss}
                  onSeen={onSeen}
                  likeLoading={likeLoadingId === rec.id}
                  dismissLoading={dismissLoadingId === rec.id}
                />
              ))}
            </div>
            {hasMore && (
              <div className="mt-4 flex justify-center">
                <Button
                  variant="outline"
                  size="sm"
                  onClick={onLoadMore}
                >
                  Load more ({recs.length - visibleCount} remaining)
                </Button>
              </div>
            )}
          </>
        )}
      </CardContent>
    </Card>
  )
}

// ── Author suggestions section ──

interface AuthorSuggestionsSectionProps {
  suggestions: AuthorSuggestion[]
  visibleCount: number
  onLoadMore: () => void
}

function AuthorSuggestionsSection({ suggestions, visibleCount, onLoadMore }: AuthorSuggestionsSectionProps) {
  const visibleItems = suggestions.slice(0, visibleCount)
  const hasMore = visibleCount < suggestions.length

  return (
    <Card className="border-slate-200">
      <CardContent className="p-4">
        <div className="mb-3 flex items-center justify-between gap-2">
          <div>
            <h3 className="text-sm font-semibold text-alma-800">Recommended Authors to Follow</h3>
            <p className="text-xs text-slate-500">
              Author suggestions derived from repeated, high-scoring recommendations.
            </p>
          </div>
          <Badge variant="outline">{suggestions.length}</Badge>
        </div>
        {suggestions.length === 0 ? (
          <p className="rounded-md border border-[var(--color-border)] bg-parchment-50 px-3 py-2 text-xs text-slate-500">
            No author suggestions yet. Generate more recommendations to populate this section.
          </p>
        ) : (
          <>
            <div className="grid grid-cols-1 gap-3">
              {visibleItems.map((s) => (
                <AuthorSuggestionCard key={s.key} suggestion={s} />
              ))}
            </div>
            {hasMore && (
              <div className="mt-4 flex justify-center">
                <Button
                  variant="outline"
                  size="sm"
                  onClick={onLoadMore}
                >
                  Load more author suggestions ({suggestions.length - visibleCount} remaining)
                </Button>
              </div>
            )}
          </>
        )}
      </CardContent>
    </Card>
  )
}

interface SignalSuggestion {
  label: string
  count: number
}

interface SignalSuggestionSectionProps {
  tags: SignalSuggestion[]
  topics: SignalSuggestion[]
  collections: SignalSuggestion[]
  onApplySuggestion: (label: string) => void
}

function SignalSuggestionsSection({
  tags,
  topics,
  collections,
  onApplySuggestion,
}: SignalSuggestionSectionProps) {
  const groups: Array<{
    key: string
    title: string
    description: string
    icon: typeof Tags
    items: SignalSuggestion[]
  }> = [
    {
      key: 'tags',
      title: 'Tag suggestions',
      description: 'Quick filters derived from your tags.',
      icon: Tags,
      items: tags,
    },
    {
      key: 'topics',
      title: 'Topic suggestions',
      description: 'Quick filters derived from your canonical topics.',
      icon: Layers,
      items: topics,
    },
    {
      key: 'collections',
      title: 'Collection suggestions',
      description: 'Quick filters derived from your collections.',
      icon: FolderOpen,
      items: collections,
    },
  ]

  return (
    <Card className="border-slate-200">
      <CardContent className="p-4">
        <div className="mb-3">
          <h3 className="text-sm font-semibold text-alma-800">Signal-Based Suggestions</h3>
          <p className="text-xs text-slate-500">
            Suggestions based on your tags, topics, and collections. Click a chip to apply a discovery filter.
          </p>
        </div>
        <div className="grid grid-cols-1 gap-3 xl:grid-cols-3">
          {groups.map((group) => (
            <div key={group.key} className="rounded-sm border border-[var(--color-border)] bg-parchment-50 p-3">
              <div className="mb-2 flex items-center gap-2">
                <group.icon className="h-4 w-4 text-slate-600" />
                <h4 className="text-xs font-semibold text-alma-800">{group.title}</h4>
              </div>
              <p className="mb-2 text-[11px] text-slate-500">{group.description}</p>
              {group.items.length === 0 ? (
                <p className="text-[11px] text-slate-500">No suggestions available.</p>
              ) : (
                <div className="flex flex-wrap gap-1.5">
                  {group.items.map((item) => (
                    <button
                      key={`${group.key}:${item.label}`}
                      type="button"
                      onClick={() => onApplySuggestion(item.label)}
                      className="inline-flex items-center gap-1 rounded-full border border-[var(--color-border)] bg-alma-chrome px-2 py-1 text-[11px] text-slate-700 transition-colors hover:bg-parchment-100"
                      title={`Filter discovery by "${item.label}"`}
                    >
                      <span className="max-w-[130px] truncate">{item.label}</span>
                      <span className="rounded-full bg-parchment-100 px-1 text-[10px] text-slate-600">
                        {item.count}
                      </span>
                    </button>
                  ))}
                </div>
              )}
            </div>
          ))}
        </div>
      </CardContent>
    </Card>
  )
}

// ── Main results section ──

interface DiscoveryResultsSectionProps {
  isLoading: boolean
  isError: boolean
  allRecs: Recommendation[]
  filteredRecs: Recommendation[]
  followedSignalRecs: Recommendation[]
  engineRecs: Recommendation[]
  authorSuggestions: AuthorSuggestion[]
  tagSuggestions: SignalSuggestion[]
  topicSuggestions: SignalSuggestion[]
  collectionSuggestions: SignalSuggestion[]
  visibleFollowedCount: number
  visibleEngineCount: number
  visibleAuthorSuggestionCount: number
  onLoadMoreFollowed: () => void
  onLoadMoreEngine: () => void
  onLoadMoreAuthorSuggestions: () => void
  onLike: (id: string) => void
  onDismiss: (id: string) => void
  onSeen: (id: string) => void
  likeLoadingId: string | undefined
  dismissLoadingId: string | undefined
  statusFilter: string
  searchText: string
  onApplySuggestion: (label: string) => void
}

export function DiscoveryResultsSection({
  isLoading,
  isError,
  allRecs,
  filteredRecs,
  followedSignalRecs,
  engineRecs,
  authorSuggestions,
  tagSuggestions,
  topicSuggestions,
  collectionSuggestions,
  visibleFollowedCount,
  visibleEngineCount,
  visibleAuthorSuggestionCount,
  onLoadMoreFollowed,
  onLoadMoreEngine,
  onLoadMoreAuthorSuggestions,
  onLike,
  onDismiss,
  onSeen,
  likeLoadingId,
  dismissLoadingId,
  statusFilter,
  searchText,
  onApplySuggestion,
}: DiscoveryResultsSectionProps) {
  if (isLoading) {
    return <LoadingState message="Loading recommendations..." />
  }

  if (isError) {
    return <ErrorState message="Failed to load recommendations. Is the backend running?" />
  }

  if (allRecs.length === 0) {
    return <EmptyState />
  }

  if (filteredRecs.length === 0) {
    return <NoFilterResults />
  }

  return (
    <>
      {/* Results count */}
      <div className="text-xs text-slate-400">
        Showing {filteredRecs.length} recommendations
        {statusFilter !== 'all' || searchText ? ` (filtered from ${allRecs.length} total)` : ''}
      </div>

      <div className="space-y-6">
        <RecommendationSection
          title="New Papers from Followed Authors and Searches"
          description="Recommendations generated from your followed-author signals and topic-search signals."
          totalCount={followedSignalRecs.length}
          emptyMessage="No followed-signal papers for the current filters."
          recs={followedSignalRecs}
          visibleCount={visibleFollowedCount}
          onLoadMore={onLoadMoreFollowed}
          onLike={onLike}
          onDismiss={onDismiss}
          onSeen={onSeen}
          likeLoadingId={likeLoadingId}
          dismissLoadingId={dismissLoadingId}
        />

        <RecommendationSection
          title="Engine-Recommended Papers (Not in Library)"
          description="Discovery engine candidates that are not already in your local library."
          totalCount={engineRecs.length}
          emptyMessage="No engine recommendations for the current filters."
          recs={engineRecs}
          visibleCount={visibleEngineCount}
          onLoadMore={onLoadMoreEngine}
          onLike={onLike}
          onDismiss={onDismiss}
          onSeen={onSeen}
          likeLoadingId={likeLoadingId}
          dismissLoadingId={dismissLoadingId}
        />

        <AuthorSuggestionsSection
          suggestions={authorSuggestions}
          visibleCount={visibleAuthorSuggestionCount}
          onLoadMore={onLoadMoreAuthorSuggestions}
        />

        <SignalSuggestionsSection
          tags={tagSuggestions}
          topics={topicSuggestions}
          collections={collectionSuggestions}
          onApplySuggestion={onApplySuggestion}
        />
      </div>
    </>
  )
}
