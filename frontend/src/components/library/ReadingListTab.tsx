import { useMemo, useState } from 'react'
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query'
import { BookOpen, Search, XCircle } from 'lucide-react'

import {
  addToLibrary,
  getReadingQueue,
  type Publication,
  updateReadingStatus,
} from '@/api/client'
import { ErrorState } from '@/components/ui/ErrorState'
import { LoadingState } from '@/components/ui/LoadingState'
import { Badge } from '@/components/ui/badge'
import { Button } from '@/components/ui/button'
import { Input } from '@/components/ui/input'
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from '@/components/ui/select'
import { MetricTile, PaperCard, type PaperCardPaper } from '@/components/shared'
import { useToast, errorToast} from '@/hooks/useToast'
import { navigateTo } from '@/lib/hashRoute'
import { invalidateQueries } from '@/lib/queryHelpers'
import { formatDate } from '@/lib/utils'

type ReadingStatusValue = 'clear' | 'reading' | 'done' | 'excluded'

const STATUS_OPTIONS: Array<{ value: ReadingStatusValue; label: string }> = [
  { value: 'reading', label: 'Reading' },
  { value: 'done', label: 'Done' },
  { value: 'excluded', label: 'Excluded' },
  { value: 'clear', label: 'Remove from Reading List' },
]

const SECTION_ORDER: Array<{
  key: 'reading' | 'done' | 'excluded'
  title: string
  description: string
}> = [
  {
    key: 'reading',
    title: 'Reading',
    description: 'Papers on the reading list.',
  },
  {
    key: 'done',
    title: 'Done',
    description: 'Papers you have already finished.',
  },
  {
    key: 'excluded',
    title: 'Excluded',
    description: 'Papers you decided not to pursue.',
  },
]

function readingListStatusLabel(paper: Publication): string {
  return paper.status === 'library' ? 'Saved' : 'Tracked only'
}

function includesSearch(paper: Publication, query: string): boolean {
  const haystack = [
    paper.title,
    paper.authors,
    paper.journal,
    paper.added_from,
    paper.reading_status,
  ]
    .filter(Boolean)
    .join(' ')
    .toLowerCase()
  return haystack.includes(query)
}

export function ReadingListTab() {
  const [search, setSearch] = useState('')
  const queryClient = useQueryClient()
  const { toast } = useToast()

  const readingQueueQuery = useQuery({
    queryKey: ['reading-queue'],
    queryFn: getReadingQueue,
    staleTime: 30_000,
    retry: 1,
  })

  const updateReadingMutation = useMutation({
    mutationFn: ({ paperId, nextStatus }: { paperId: string; nextStatus: Exclude<ReadingStatusValue, 'clear'> | null }) =>
      updateReadingStatus(paperId, nextStatus),
    onSuccess: async () => {
      await invalidateQueries(queryClient, ['reading-queue'], ['library-workflow-summary'], ['papers'], ['library-saved'])
    },
    onError: () => {
      errorToast('Error', 'Failed to update reading status.')
    },
  })

  const saveToLibraryMutation = useMutation({
    mutationFn: (paperId: string) => addToLibrary(paperId, 0),
    onSuccess: async () => {
      await invalidateQueries(queryClient, ['reading-queue'], ['library-workflow-summary'], ['papers'], ['library-saved'])
      toast({ title: 'Saved', description: 'Paper added to the saved library.' })
    },
    onError: () => {
      errorToast('Error', 'Failed to save this paper to the library.')
    },
  })

  const normalizedSearch = search.trim().toLowerCase()
  const sections = useMemo(() => {
    const queue = readingQueueQuery.data
    const buckets = {
      reading: queue?.reading ?? [],
      done: queue?.done ?? [],
      excluded: queue?.excluded ?? [],
    }
    return SECTION_ORDER.map((section) => ({
      ...section,
      items: normalizedSearch
        ? buckets[section.key].filter((paper) => includesSearch(paper, normalizedSearch))
        : buckets[section.key],
      total: buckets[section.key].length,
    }))
  }, [normalizedSearch, readingQueueQuery.data])

  const totalVisible = sections.reduce((sum, section) => sum + section.items.length, 0)
  const totalTracked = sections.reduce((sum, section) => sum + section.total, 0)

  if (readingQueueQuery.isLoading) {
    return <LoadingState message="Loading reading list..." />
  }

  if (readingQueueQuery.isError) {
    return <ErrorState message="Failed to load the reading list." />
  }

  return (
    <div className="space-y-4">
      {/* Bucket scoreboard — three MetricTile align="center" tiles for
          parity with the Library landing's metadata strip. Reading uses
          the Folio-blue accent when > 0 so the active workload reads as
          the eye-catch of this tab; Done is success-emerald (a small,
          satisfying green); Excluded stays neutral. */}
      <div className="grid gap-2 sm:grid-cols-3">
        {sections.map((section) => {
          const tone =
            section.key === 'reading'
              ? section.total > 0
                ? 'accent'
                : 'neutral'
              : section.key === 'done'
                ? 'success'
                : 'neutral'
          return (
            <MetricTile
              key={section.key}
              align="center"
              tone={tone}
              label={section.title}
              value={section.total}
              hint={section.description}
            />
          )
        })}
      </div>

      <div className="flex flex-col gap-3 sm:flex-row sm:items-center sm:justify-between">
        <div className="relative max-w-md flex-1">
          <Search className="absolute left-3 top-1/2 h-4 w-4 -translate-y-1/2 text-slate-400" />
          <Input
            value={search}
            onChange={(event) => setSearch(event.target.value)}
            placeholder="Search reading-list papers..."
            className="pl-9"
          />
        </div>
        <p className="text-sm text-slate-500">
          {normalizedSearch
            ? `Showing ${totalVisible} of ${totalTracked} reading-list papers`
            : `${totalTracked} reading-list papers`}
        </p>
      </div>

      {totalVisible === 0 ? (
        <div className="rounded-sm border border-[var(--color-border)] bg-parchment-50 px-6 py-12 text-center">
          <BookOpen className="mx-auto h-10 w-10 text-slate-300" />
          <p className="mt-4 text-sm font-medium text-slate-600">No reading-list papers match this view.</p>
          <p className="mt-1 text-xs text-slate-500">
            Queue papers from Saved Library or Corpus to turn reading status into a real workflow.
          </p>
        </div>
      ) : (
        <div className="space-y-6">
          {sections.map((section) => (
            <div key={section.key} className="space-y-3">
              <div className="flex items-end justify-between gap-3">
                <div>
                  <h3 className="text-lg font-semibold text-alma-800">{section.title}</h3>
                  <p className="text-sm text-slate-500">{section.description}</p>
                </div>
                <Badge variant="outline">{section.items.length}</Badge>
              </div>

              {section.items.length === 0 ? (
                <div className="rounded-lg border border-dashed border-slate-200 px-4 py-6 text-sm text-slate-400">
                  No papers in this bucket.
                </div>
              ) : (
                <div className="space-y-3">
                  {section.items.map((paper) => {
                    const cardPaper: PaperCardPaper = {
                      id: paper.id,
                      title: paper.title,
                      authors: paper.authors,
                      year: paper.year ?? null,
                      journal: paper.journal ?? undefined,
                      url: paper.url ?? undefined,
                      doi: paper.doi ?? undefined,
                      publication_date: paper.publication_date ?? undefined,
                      cited_by_count: paper.cited_by_count ?? 0,
                      abstract: paper.abstract ?? undefined,
                      rating: paper.rating ?? undefined,
                      status: paper.status ?? undefined,
                      tldr: paper.tldr ?? null,
                      influential_citation_count: paper.influential_citation_count ?? 0,
                      global_signal_score: paper.global_signal_score ?? 0,
                    }
                    const readingStatusSlot = (
                      <div className="flex flex-wrap items-center gap-2">
                        <Badge variant={paper.status === 'library' ? 'default' : 'secondary'}>
                          {readingListStatusLabel(paper)}
                        </Badge>
                        {paper.added_from && <Badge variant="outline">{paper.added_from}</Badge>}
                        {paper.added_at && <Badge variant="outline">Added {formatDate(paper.added_at)}</Badge>}
                        <Select
                          value={(paper.reading_status as ReadingStatusValue | null) ?? 'clear'}
                          onValueChange={(value) => {
                            const nextStatus = value === 'clear' ? null : (value as Exclude<ReadingStatusValue, 'clear'>)
                            updateReadingMutation.mutate({ paperId: paper.id, nextStatus })
                          }}
                        >
                          <SelectTrigger className="h-8 w-40 text-xs">
                            <SelectValue />
                          </SelectTrigger>
                          <SelectContent>
                            {STATUS_OPTIONS.map((option) => (
                              <SelectItem key={option.value} value={option.value}>
                                {option.label}
                              </SelectItem>
                            ))}
                          </SelectContent>
                        </Select>
                        {paper.status !== 'library' && (
                          <Button
                            size="sm"
                            variant="outline"
                            onClick={() => saveToLibraryMutation.mutate(paper.id)}
                            disabled={saveToLibraryMutation.isPending}
                          >
                            Save to Library
                          </Button>
                        )}
                        <Button
                          size="sm"
                          variant="ghost"
                          className="text-slate-500 hover:text-slate-700"
                          onClick={() => updateReadingMutation.mutate({ paperId: paper.id, nextStatus: null })}
                          disabled={updateReadingMutation.isPending}
                        >
                          <XCircle className="mr-1 h-4 w-4" />
                          Remove from List
                        </Button>
                      </div>
                    )
                    return (
                      <PaperCard
                        key={paper.id}
                        paper={cardPaper}
                        readingStatusSlot={readingStatusSlot}
                        onPivot={() => navigateTo('discovery', {
                          seed: cardPaper.id,
                          seedTitle: cardPaper.title,
                        })}
                      />
                    )
                  })}
                </div>
              )}
            </div>
          ))}
        </div>
      )}
    </div>
  )
}
