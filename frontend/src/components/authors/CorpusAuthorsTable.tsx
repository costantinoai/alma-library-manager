import { useMemo, useState } from 'react'
import type { ColumnDef } from '@tanstack/react-table'
import { ChevronDown, ChevronRight, Database } from 'lucide-react'

import type { Author } from '@/api/client'
import { Collapsible, CollapsibleContent, CollapsibleTrigger } from '@/components/ui/collapsible'
import { DataTable } from '@/components/ui/data-table'
import { Input } from '@/components/ui/input'
import { StatusBadge } from '@/components/ui/status-badge'
import { AuthorResolvedBadge } from '@/components/authors/AuthorResolvedBadge'
import { formatNumber } from '@/lib/utils'

interface CorpusAuthorsTableProps {
  authors: Author[]
  followedIds: Set<string>
  onSelect: (author: Author) => void
}

interface CorpusRow extends Author {
  isFollowed: boolean
}

/**
 * Compact read-only table of every author in the corpus. Uses the shared
 * ``DataTable`` primitive (same component Feed and Library use for their
 * compact views) so column visibility, resize, and drag-reorder stay
 * consistent across surfaces.
 */
export function CorpusAuthorsTable({ authors, followedIds, onSelect }: CorpusAuthorsTableProps) {
  const [open, setOpen] = useState(false)
  const [search, setSearch] = useState('')

  const rows: CorpusRow[] = useMemo(() => {
    const q = search.trim().toLowerCase()
    const filtered = q
      ? authors.filter(
          (a) =>
            a.name.toLowerCase().includes(q) ||
            (a.affiliation?.toLowerCase().includes(q) ?? false) ||
            a.id.toLowerCase().includes(q),
        )
      : authors
    return filtered.map((a) => ({ ...a, isFollowed: followedIds.has(a.id) }))
  }, [authors, followedIds, search])

  const columns: ColumnDef<CorpusRow>[] = useMemo(
    () => [
      {
        id: 'name',
        accessorKey: 'name',
        header: 'Name',
        size: 260,
        cell: ({ row }) => (
          <span className="flex items-center gap-1.5">
            <span className="font-medium text-alma-800">{row.original.name}</span>
            <AuthorResolvedBadge author={row.original} size="sm" />
          </span>
        ),
      },
      {
        id: 'affiliation',
        accessorKey: 'affiliation',
        header: 'Affiliation',
        size: 280,
        cell: ({ row }) => (
          <span className="text-slate-600" title={row.original.affiliation ?? undefined}>
            {row.original.affiliation ?? '—'}
          </span>
        ),
      },
      {
        id: 'h_index',
        accessorKey: 'h_index',
        header: 'h-index',
        size: 90,
        cell: ({ row }) => (
          <span className="tabular-nums text-slate-600">{row.original.h_index ?? '—'}</span>
        ),
      },
      {
        id: 'works_count',
        accessorKey: 'works_count',
        header: 'Works',
        size: 90,
        cell: ({ row }) => (
          <span className="tabular-nums text-slate-600">
            {row.original.works_count != null ? formatNumber(row.original.works_count) : '—'}
          </span>
        ),
      },
      {
        id: 'publication_count',
        accessorKey: 'publication_count',
        header: 'In DB',
        size: 80,
        cell: ({ row }) => (
          <span className="tabular-nums text-slate-600">
            {row.original.publication_count != null
              ? formatNumber(row.original.publication_count)
              : '—'}
          </span>
        ),
      },
      {
        id: 'status',
        accessorKey: 'isFollowed',
        header: 'Status',
        size: 110,
        enableSorting: false,
        meta: { cellOverflow: 'none' },
        cell: ({ row }) => (
          <StatusBadge tone={row.original.isFollowed ? 'positive' : 'neutral'} size="sm">
            {row.original.isFollowed ? 'Followed' : 'Corpus'}
          </StatusBadge>
        ),
      },
    ],
    [],
  )

  return (
    <Collapsible open={open} onOpenChange={setOpen} className="space-y-3">
      <header className="flex flex-wrap items-center justify-between gap-3">
        <CollapsibleTrigger className="group flex items-center gap-2 text-left">
          {open ? (
            <ChevronDown className="h-4 w-4 text-slate-500 transition" />
          ) : (
            <ChevronRight className="h-4 w-4 text-slate-500 transition" />
          )}
          <Database className="h-4 w-4 text-slate-500" />
          <div>
            <h2 className="text-sm font-semibold text-alma-800">Corpus authors</h2>
            <p className="text-xs text-slate-500">{authors.length} in DB — click to browse</p>
          </div>
        </CollapsibleTrigger>
        {open ? (
          <Input
            value={search}
            onChange={(e) => setSearch(e.target.value)}
            placeholder="Search corpus..."
            className="h-8 max-w-xs"
          />
        ) : null}
      </header>

      <CollapsibleContent>
        <DataTable<CorpusRow>
          data={rows}
          columns={columns}
          storageKey="authors.corpus"
          getRowId={(row) => row.id}
          onRowClick={(row) => onSelect(row)}
          footerCaption={`${rows.length} author${rows.length !== 1 ? 's' : ''}`}
          emptyState={
            <div className="py-8 text-center text-sm text-slate-400">
              No authors match that search.
            </div>
          }
        />
      </CollapsibleContent>
    </Collapsible>
  )
}
