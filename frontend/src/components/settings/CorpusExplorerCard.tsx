/**
 * Corpus explorer — lives inside Settings → Data & system.
 *
 * This is the D1 home for full-database inspection (per
 * `tasks/08_PRODUCT_DECISIONS.md`). It is a diagnostic surface — not a
 * reading list — that must expose every paper in the DB, including
 * provenance, membership, reading state, and per-row actions to soft-
 * remove or open the canonical detail panel.
 *
 * Implementation notes:
 * - Built on the shared `DataTable` primitive (same as Feed compact,
 *   Library Favorites, and `CorpusAuthorsTable`), so column visibility,
 *   resize, drag-reorder, and sort stay consistent across the app.
 * - The backend `/papers` endpoint caps a single request at limit=1000,
 *   so corpora larger than one page paginate via "Load more" rather
 *   than a virtual scroller (simpler and avoids a new npm dep for the
 *   single-user scale ALMa targets).
 * - Row click opens the canonical `PaperDetailPanel` (same detail panel
 *   the Feed, Discovery, and Library surfaces use — single click route).
 * - Title cell has a HoverCard with diagnostic fields (paper_id,
 *   openalex_id, doi, resolution reason, etc.) for quick peek without
 *   opening the full panel.
 * - Per-row trailing actions: open URL, soft-remove (confirmation via
 *   AlertDialog). Soft-remove goes through `DELETE /papers/{id}`, which
 *   sets `status='removed'` per D3 — never a hard delete.
 */

import { useMemo, useState } from 'react'
import type { ColumnDef } from '@tanstack/react-table'
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query'
import {
  Database,
  ExternalLink,
  Link2,
  RefreshCw,
  Search,
  Trash2,
} from 'lucide-react'

import { api, listPapers, type Publication } from '@/api/client'
import { AsyncButton } from '@/components/settings/primitives'
import { PaperDetailPanel } from '@/components/discovery'
import {
  AlertDialog,
  AlertDialogAction,
  AlertDialogCancel,
  AlertDialogContent,
  AlertDialogDescription,
  AlertDialogFooter,
  AlertDialogHeader,
  AlertDialogTitle,
} from '@/components/ui/alert-dialog'
import { Button } from '@/components/ui/button'
import { Card, CardDescription, CardHeader, CardTitle } from '@/components/ui/card'
import { DataTable } from '@/components/ui/data-table'
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogHeader,
  DialogTitle,
} from '@/components/ui/dialog'
import { EmptyState } from '@/components/ui/empty-state'
import { ErrorState } from '@/components/ui/ErrorState'
import { HoverCard, HoverCardContent, HoverCardTrigger } from '@/components/ui/hover-card'
import { Input } from '@/components/ui/input'
import { Label } from '@/components/ui/label'
import { LoadingState } from '@/components/ui/LoadingState'
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from '@/components/ui/select'
import { StatusBadge, type StatusBadgeTone } from '@/components/ui/status-badge'
import { useDebounce } from '@/hooks/useDebounce'
import { useToast, errorToast } from '@/hooks/useToast'
import { invalidateQueries } from '@/lib/queryHelpers'
import { formatDate, formatPublicationDate, truncate } from '@/lib/utils'

// ---------------------------------------------------------------------------
// Filters
// ---------------------------------------------------------------------------

type CorpusStatus = 'all' | 'tracked' | 'library' | 'dismissed' | 'removed'
type PresenceFilter = 'all' | 'yes' | 'no'

const STATUS_OPTIONS: Array<{ value: CorpusStatus; label: string }> = [
  { value: 'all', label: 'All statuses' },
  { value: 'library', label: 'Library' },
  { value: 'tracked', label: 'Tracked' },
  { value: 'dismissed', label: 'Dismissed' },
  { value: 'removed', label: 'Removed' },
]

/** Max rows the backend `/papers` endpoint returns per call (`Query(..., le=1000)`).
 *  For corpora larger than one page, "Load more" advances offset. */
const PAGE_SIZE = 500

function presenceValue(value: PresenceFilter): boolean | undefined {
  if (value === 'yes') return true
  if (value === 'no') return false
  return undefined
}

function statusTone(status?: string | null): StatusBadgeTone {
  switch (status) {
    case 'library':
      return 'positive'
    case 'removed':
      return 'negative'
    case 'dismissed':
      return 'neutral'
    case 'tracked':
    default:
      return 'warning'
  }
}

// ---------------------------------------------------------------------------
// Hover card content — diagnostic peek (paper_id, openalex_id, doi, resolution)
// ---------------------------------------------------------------------------

function CorpusHoverContent({ paper }: { paper: Publication }) {
  const pubDate = paper.publication_date
    ? formatDate(paper.publication_date)
    : paper.year
      ? String(paper.year)
      : '—'
  return (
    <HoverCardContent
      align="start"
      side="bottom"
      sideOffset={6}
      className="w-[26rem] space-y-3 p-4 text-xs"
    >
      <div className="space-y-1">
        <p className="text-sm font-semibold leading-snug text-alma-800">
          {paper.title || 'Untitled'}
        </p>
        <p className="text-xs text-slate-600">{paper.authors || 'Unknown authors'}</p>
        <div className="flex flex-wrap items-center gap-x-3 gap-y-1 pt-1 text-[11px] text-slate-500">
          {paper.journal && <span>{paper.journal}</span>}
          <span className="tabular-nums">{pubDate}</span>
          <span className="tabular-nums">
            {(paper.cited_by_count ?? 0).toLocaleString()} citations
          </span>
          {typeof paper.rating === 'number' && paper.rating > 0 && (
            <span>rating {paper.rating}/5</span>
          )}
        </div>
      </div>

      {paper.abstract && (
        <p className="line-clamp-5 border-t border-slate-100 pt-2 leading-relaxed text-slate-600">
          {paper.abstract}
        </p>
      )}

      <div className="grid gap-1.5 border-t border-slate-100 pt-2 text-[11px]">
        <KeyRow label="paper_id" value={<span className="font-mono">{paper.id}</span>} />
        {paper.openalex_id && (
          <KeyRow
            label="openalex_id"
            value={
              <a
                href={`https://openalex.org/${paper.openalex_id}`}
                target="_blank"
                rel="noopener noreferrer"
                onClick={(e) => e.stopPropagation()}
                className="inline-flex items-center gap-1 font-mono text-alma-700 hover:underline"
              >
                {paper.openalex_id} <Link2 className="size-3" />
              </a>
            }
          />
        )}
        {paper.doi && (
          <KeyRow
            label="doi"
            value={
              <a
                href={`https://doi.org/${paper.doi}`}
                target="_blank"
                rel="noopener noreferrer"
                onClick={(e) => e.stopPropagation()}
                className="inline-flex items-center gap-1 font-mono text-alma-700 hover:underline"
              >
                {paper.doi} <Link2 className="size-3" />
              </a>
            }
          />
        )}
        {paper.added_from && (
          <KeyRow label="added_from" value={<span className="font-mono">{paper.added_from}</span>} />
        )}
        {paper.added_at && (
          <KeyRow label="added_at" value={<span className="tabular-nums">{formatDate(paper.added_at)}</span>} />
        )}
        {paper.updated_at && (
          <KeyRow
            label="updated_at"
            value={<span className="tabular-nums">{formatDate(paper.updated_at)}</span>}
          />
        )}
        {paper.openalex_resolution_reason && (
          <KeyRow
            label="resolution"
            value={<span className="italic text-slate-600">{paper.openalex_resolution_reason}</span>}
          />
        )}
      </div>
    </HoverCardContent>
  )
}

function KeyRow({ label, value }: { label: string; value: React.ReactNode }) {
  return (
    <div className="flex items-start justify-between gap-3">
      <span className="shrink-0 text-[10px] font-semibold uppercase tracking-wide text-slate-400">
        {label}
      </span>
      <span className="min-w-0 flex-1 text-right text-slate-700">{value}</span>
    </div>
  )
}

// ---------------------------------------------------------------------------
// Launcher card + modal popup
// ---------------------------------------------------------------------------

export function CorpusExplorerCard() {
  const [open, setOpen] = useState(false)
  return (
    <Card>
      <CardHeader className="gap-2">
        <div className="flex flex-wrap items-start justify-between gap-3">
          <div className="flex items-start gap-3">
            <div className="flex size-9 shrink-0 items-center justify-center rounded-lg bg-parchment-100 text-slate-600">
              <Database className="size-4" />
            </div>
            <div className="space-y-1">
              <CardTitle className="text-base">Corpus explorer</CardTitle>
              <CardDescription className="max-w-2xl">
                Every paper ALMa has ever seen — saved, tracked, dismissed, removed, or
                imported. A diagnostic table, not a reading list: confirm provenance,
                audit imports, or inspect OpenAlex resolution state. Click a row for the
                full paper detail; hover the title for a compact diagnostic peek.
              </CardDescription>
            </div>
          </div>
          <Button size="sm" variant="outline" onClick={() => setOpen(true)}>
            <ExternalLink className="size-3.5" />
            Open explorer
          </Button>
        </div>
      </CardHeader>

      <Dialog open={open} onOpenChange={setOpen}>
        <DialogContent className="flex h-[90vh] max-w-6xl flex-col overflow-hidden p-0">
          <DialogHeader className="border-b border-slate-200 px-6 py-4">
            <DialogTitle className="flex items-center gap-2 text-base">
              <Database className="size-4 text-slate-500" />
              Corpus explorer
            </DialogTitle>
            <DialogDescription>
              Every paper in the database, with status, provenance, reading
              state, and resolution. Click a row for the full detail panel.
            </DialogDescription>
          </DialogHeader>
          <div className="flex-1 overflow-y-auto px-6 py-5">
            <CorpusExplorerBody />
          </div>
        </DialogContent>
      </Dialog>
    </Card>
  )
}

// ---------------------------------------------------------------------------
// Body — filters, DataTable, pagination, row actions
// ---------------------------------------------------------------------------

function CorpusExplorerBody() {
  const [search, setSearch] = useState('')
  const [status, setStatus] = useState<CorpusStatus>('all')
  const [addedFrom, setAddedFrom] = useState('')
  const [hasTopics, setHasTopics] = useState<PresenceFilter>('all')
  const [hasTags, setHasTags] = useState<PresenceFilter>('all')
  const [pageCount, setPageCount] = useState(1)
  const [selectedPaper, setSelectedPaper] = useState<Publication | null>(null)
  const [detailOpen, setDetailOpen] = useState(false)
  const [removeTarget, setRemoveTarget] = useState<Publication | null>(null)
  const debouncedSearch = useDebounce(search, 250)
  const queryClient = useQueryClient()
  const { toast } = useToast()

  const currentLimit = PAGE_SIZE * pageCount

  // Reset pagination when any filter changes — keeps the "Load more" count
  // honest when the filter shrinks the result set below the current page.
  const filterKey = `${debouncedSearch}|${status}|${addedFrom}|${hasTopics}|${hasTags}`
  const [lastFilterKey, setLastFilterKey] = useState(filterKey)
  if (filterKey !== lastFilterKey) {
    setLastFilterKey(filterKey)
    setPageCount(1)
  }

  const corpusQuery = useQuery({
    queryKey: [
      'library-corpus',
      debouncedSearch,
      status,
      addedFrom,
      hasTopics,
      hasTags,
      currentLimit,
    ],
    queryFn: () =>
      listPapers({
        scope: 'all',
        search: debouncedSearch || undefined,
        status: status === 'all' ? undefined : status,
        addedFrom: addedFrom.trim() || undefined,
        hasTopics: presenceValue(hasTopics),
        hasTags: presenceValue(hasTags),
        order: 'recent',
        limit: Math.min(currentLimit, 1000 * pageCount),
      }),
    retry: 1,
  })

  const removeMutation = useMutation({
    mutationFn: (paperId: string) => api.delete<void>(`/papers/${paperId}`),
    onSuccess: () => {
      toast({
        title: 'Paper removed',
        description: 'Soft-removed (status → removed). Row stays in the DB.',
      })
      setRemoveTarget(null)
      void invalidateQueries(
        queryClient,
        ['library-corpus'],
        ['library-saved'],
        ['library-workflow-summary'],
        ['papers'],
      )
    },
    onError: () => {
      errorToast('Could not remove', 'The paper could not be removed. Check Activity logs.')
      setRemoveTarget(null)
    },
  })

  const papers = useMemo(() => corpusQuery.data ?? [], [corpusQuery.data])

  // Whether a "Load more" button makes sense: if the last page came back
  // full, there could be more rows. The backend caps at 1000 per call, so
  // after we've reached the total, fewer-than-requested rows return.
  const lastPageSize = papers.length - PAGE_SIZE * (pageCount - 1)
  const hasMore = lastPageSize >= PAGE_SIZE

  const filtersActive =
    !!debouncedSearch ||
    status !== 'all' ||
    !!addedFrom ||
    hasTopics !== 'all' ||
    hasTags !== 'all'

  const columns: ColumnDef<Publication>[] = useMemo(
    () => [
      {
        id: 'title',
        accessorKey: 'title',
        header: 'Paper',
        size: 420,
        cell: ({ row }) => {
          const p = row.original
          return (
            <HoverCard openDelay={250} closeDelay={120}>
              <HoverCardTrigger asChild>
                <div className="min-w-0 space-y-0.5">
                  <p className="line-clamp-2 font-medium leading-snug text-alma-800">
                    {p.title || 'Untitled'}
                  </p>
                  <p className="line-clamp-1 text-xs text-slate-500">
                    {truncate(p.authors || 'Unknown authors', 120)}
                  </p>
                </div>
              </HoverCardTrigger>
              <CorpusHoverContent paper={p} />
            </HoverCard>
          )
        },
      },
      {
        id: 'status',
        accessorKey: 'status',
        header: 'Status',
        size: 110,
        enableSorting: true,
        meta: { cellOverflow: 'none' },
        cell: ({ row }) => (
          <StatusBadge size="sm" tone={statusTone(row.original.status)} className="capitalize">
            {row.original.status || 'tracked'}
          </StatusBadge>
        ),
      },
      {
        id: 'added_from',
        accessorKey: 'added_from',
        header: 'Provenance',
        size: 150,
        cell: ({ row }) => (
          <span className="font-mono text-xs text-slate-600">
            {row.original.added_from || '—'}
          </span>
        ),
      },
      {
        id: 'rating',
        accessorKey: 'rating',
        header: 'Rating',
        size: 80,
        cell: ({ row }) => (
          <span className="tabular-nums text-xs text-slate-700">
            {row.original.rating ?? 0}
          </span>
        ),
      },
      {
        id: 'reading_status',
        accessorKey: 'reading_status',
        header: 'Reading',
        size: 110,
        cell: ({ row }) => (
          <span className="text-xs capitalize text-slate-600">
            {row.original.reading_status || '—'}
          </span>
        ),
      },
      {
        id: 'date',
        accessorFn: (row) => row.publication_date ?? (row.year != null ? String(row.year) : ''),
        header: 'Date',
        size: 110,
        cell: ({ row }) => (
          <span className="tabular-nums text-xs text-slate-600">
            {formatPublicationDate(row.original) || '—'}
          </span>
        ),
      },
      {
        id: 'cites',
        accessorKey: 'cited_by_count',
        header: 'Cites',
        size: 80,
        cell: ({ row }) => (
          <span className="tabular-nums text-xs text-slate-600">
            {row.original.cited_by_count ?? 0}
          </span>
        ),
      },
      {
        id: 'resolution',
        accessorKey: 'openalex_resolution_status',
        header: 'Resolution',
        size: 160,
        cell: ({ row }) => (
          <span className="rounded bg-parchment-100 px-1.5 py-0.5 font-mono text-[11px] text-slate-600">
            {row.original.openalex_resolution_status || '—'}
          </span>
        ),
      },
      {
        id: 'actions',
        header: '',
        size: 90,
        enableSorting: false,
        meta: { cellOverflow: 'none' },
        cell: ({ row }) => {
          const p = row.original
          return (
            <div className="flex items-center justify-end gap-1">
              {p.url ? (
                <a
                  href={p.url}
                  target="_blank"
                  rel="noopener noreferrer"
                  onClick={(e) => e.stopPropagation()}
                  className="inline-flex rounded-md p-1 text-slate-400 transition-colors hover:bg-parchment-100 hover:text-alma-700"
                  aria-label="Open paper URL"
                  title="Open paper URL"
                >
                  <ExternalLink className="size-3.5" />
                </a>
              ) : null}
              <button
                type="button"
                onClick={(e) => {
                  e.stopPropagation()
                  setRemoveTarget(p)
                }}
                className="inline-flex rounded-md p-1 text-slate-400 transition-colors hover:bg-rose-50 hover:text-rose-700"
                aria-label="Remove paper (soft delete)"
                title="Remove paper (status → removed, row stays in DB)"
              >
                <Trash2 className="size-3.5" />
              </button>
            </div>
          )
        },
      },
    ],
    [],
  )

  return (
    <div className="space-y-4">
      {/* Toolbar: count + refresh */}
      <div className="flex items-center justify-between">
        <StatusBadge tone="neutral" size="sm" className="tabular-nums">
          {papers.length} loaded{hasMore ? ' (more available)' : ''}
        </StatusBadge>
        <AsyncButton
          size="sm"
          variant="outline"
          icon={<RefreshCw className="size-3.5" />}
          pending={corpusQuery.isFetching}
          onClick={() => void invalidateQueries(queryClient, ['library-corpus'])}
        >
          Refresh
        </AsyncButton>
      </div>

      {/* Filter row */}
      <div className="grid gap-3 md:grid-cols-[minmax(16rem,1fr)_11rem_11rem_9rem_9rem]">
        <div className="relative">
          <Search className="pointer-events-none absolute left-3 top-1/2 size-4 -translate-y-1/2 text-slate-400" />
          <Input
            value={search}
            onChange={(e) => setSearch(e.target.value)}
            placeholder="Search title, abstract, authors…"
            className="pl-9"
            aria-label="Corpus search"
          />
        </div>
        <FilterSlot label="Status">
          <Select value={status} onValueChange={(v) => setStatus(v as CorpusStatus)}>
            <SelectTrigger className="h-9">
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
        </FilterSlot>
        <FilterSlot label="added_from">
          <Input
            value={addedFrom}
            onChange={(e) => setAddedFrom(e.target.value)}
            placeholder="e.g. import, online_search"
            className="font-mono text-xs"
          />
        </FilterSlot>
        <FilterSlot label="Topics">
          <Select value={hasTopics} onValueChange={(v) => setHasTopics(v as PresenceFilter)}>
            <SelectTrigger className="h-9">
              <SelectValue placeholder="Topics" />
            </SelectTrigger>
            <SelectContent>
              <SelectItem value="all">Any topics</SelectItem>
              <SelectItem value="yes">Has topics</SelectItem>
              <SelectItem value="no">No topics</SelectItem>
            </SelectContent>
          </Select>
        </FilterSlot>
        <FilterSlot label="Tags">
          <Select value={hasTags} onValueChange={(v) => setHasTags(v as PresenceFilter)}>
            <SelectTrigger className="h-9">
              <SelectValue placeholder="Tags" />
            </SelectTrigger>
            <SelectContent>
              <SelectItem value="all">Any tags</SelectItem>
              <SelectItem value="yes">Has tags</SelectItem>
              <SelectItem value="no">No tags</SelectItem>
            </SelectContent>
          </Select>
        </FilterSlot>
      </div>

      {filtersActive && (
        <div className="flex items-center justify-end">
          <Button
            variant="ghost"
            size="sm"
            onClick={() => {
              setSearch('')
              setStatus('all')
              setAddedFrom('')
              setHasTopics('all')
              setHasTags('all')
              setPageCount(1)
            }}
            className="text-xs"
          >
            Clear filters
          </Button>
        </div>
      )}

      {/* Table */}
      {corpusQuery.isLoading ? (
        <LoadingState />
      ) : corpusQuery.isError ? (
        <ErrorState message="Failed to load the corpus." />
      ) : papers.length === 0 ? (
        <EmptyState
          icon={Database}
          title="No corpus rows match these filters"
          description="Try widening the status filter, clearing added_from, or searching for a known title."
        />
      ) : (
        <>
          <DataTable<Publication>
            data={papers}
            columns={columns}
            storageKey="settings.corpus-explorer"
            getRowId={(row) => row.id}
            onRowClick={(row) => {
              setSelectedPaper(row)
              setDetailOpen(true)
            }}
            footerCaption={`${papers.length} paper${papers.length !== 1 ? 's' : ''}`}
          />
          {hasMore && (
            <div className="flex justify-center pt-2">
              <AsyncButton
                variant="outline"
                size="sm"
                pending={corpusQuery.isFetching}
                onClick={() => setPageCount((n) => n + 1)}
              >
                Load more
              </AsyncButton>
            </div>
          )}
        </>
      )}

      {/* Detail panel — canonical single click route (same panel Feed,
          Discovery, and Library use). */}
      <PaperDetailPanel
        paper={selectedPaper}
        open={detailOpen}
        onOpenChange={setDetailOpen}
      />

      {/* Soft-remove confirmation. D3: never hard-delete on normal remove. */}
      <AlertDialog open={!!removeTarget} onOpenChange={(o) => !o && setRemoveTarget(null)}>
        <AlertDialogContent>
          <AlertDialogHeader>
            <AlertDialogTitle>Remove this paper?</AlertDialogTitle>
            <AlertDialogDescription>
              Sets <code className="rounded bg-parchment-100 px-1 py-0.5 font-mono text-xs">status='removed'</code>{' '}
              on{' '}
              <span className="font-medium text-alma-800">
                {removeTarget?.title || 'this paper'}
              </span>
              . The row stays in the database (provenance preserved) and is hidden from
              Library / Feed. Discovery reads it as a negative signal.
            </AlertDialogDescription>
          </AlertDialogHeader>
          <AlertDialogFooter>
            <AlertDialogCancel disabled={removeMutation.isPending}>Cancel</AlertDialogCancel>
            <AlertDialogAction
              disabled={removeMutation.isPending}
              onClick={() => {
                if (removeTarget) removeMutation.mutate(removeTarget.id)
              }}
            >
              {removeMutation.isPending ? 'Removing…' : 'Remove'}
            </AlertDialogAction>
          </AlertDialogFooter>
        </AlertDialogContent>
      </AlertDialog>
    </div>
  )
}

function FilterSlot({
  label,
  children,
}: {
  label: string
  children: React.ReactNode
}) {
  return (
    <div className="space-y-1">
      <Label className="text-[10px] font-semibold uppercase tracking-wide text-slate-400">
        {label}
      </Label>
      {children}
    </div>
  )
}
