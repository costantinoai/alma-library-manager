/**
 * Collection intelligence — per-collection analytics, shown at the bottom of
 * the Library → Collections tab (task 47 Phase 4 step 8).
 *
 * It lived in the Reports tab, one page away from the collections it describes.
 * Analytics about a thing belong beside the thing, so it moved here. Loading is
 * still generate-on-demand: it's an expensive aggregate and most visits to this
 * tab are about creating or editing a collection, not measuring one.
 */
import { useMemo, useState } from 'react'
import { useQuery } from '@tanstack/react-query'
import { FolderOpen, Loader2 } from 'lucide-react'
import type { ColumnDef } from '@tanstack/react-table'

import { getCollectionIntelligence, type CollectionIntelligenceData } from '@/api/client'
import { ActionCardHeader } from '@/components/shared'
import { Badge } from '@/components/ui/badge'
import { Button } from '@/components/ui/button'
import { Card, CardContent } from '@/components/ui/card'
import { DataTable } from '@/components/ui/data-table'
import { formatPercent } from '@/lib/format'
import { truncate } from '@/lib/utils'

type CollectionRow = CollectionIntelligenceData['collections'][number]

export function CollectionIntelligenceCard() {
  const [requested, setRequested] = useState(false)

  const { data, isLoading } = useQuery({
    queryKey: ['collection-intelligence'],
    queryFn: getCollectionIntelligence,
    enabled: requested,
    staleTime: 5 * 60_000,
  })

  const columns = useMemo<ColumnDef<CollectionRow>[]>(
    () => [
      {
        id: 'name',
        accessorKey: 'name',
        header: 'Collection',
        size: 220,
        meta: { cellOverflow: 'none' },
        cell: ({ row }) => (
          <div className="flex min-w-0 items-center gap-2">
            <span
              className="inline-block h-2.5 w-2.5 shrink-0 rounded-full"
              style={{ backgroundColor: row.original.color || 'var(--color-slate-400)' }}
            />
            <span
              className="min-w-0 flex-1 truncate font-medium text-alma-800"
              title={row.original.name}
            >
              {row.original.name}
            </span>
          </div>
        ),
      },
      {
        id: 'paper_count',
        accessorKey: 'paper_count',
        header: 'Papers',
        size: 90,
        meta: { cellOverflow: 'none' },
        cell: ({ row }) => (
          <span className="block text-right tabular-nums text-slate-700">
            {row.original.paper_count}
          </span>
        ),
      },
      {
        id: 'avg_citations',
        accessorKey: 'avg_citations',
        header: 'Avg Cit.',
        size: 100,
        meta: { cellOverflow: 'none' },
        cell: ({ row }) => (
          <span className="block text-right tabular-nums text-slate-700">
            {row.original.avg_citations.toFixed(1)}
          </span>
        ),
      },
      {
        id: 'avg_rating',
        accessorKey: 'avg_rating',
        header: 'Avg Rating',
        size: 110,
        meta: { cellOverflow: 'none' },
        cell: ({ row }) => (
          <span className="block text-right tabular-nums text-slate-700">
            {row.original.avg_rating > 0 ? row.original.avg_rating.toFixed(1) : '—'}
          </span>
        ),
      },
      {
        id: 'year_range',
        header: 'Years',
        size: 110,
        enableSorting: false,
        cell: ({ row }) => {
          const { min, max } = row.original.year_range
          return <span className="text-xs text-slate-500">{min && max ? `${min}–${max}` : '—'}</span>
        },
      },
      {
        // I-29: normalized topic evenness (0..1) plus the raw distinct-topic
        // count — a real diversity figure, not the old len(top5) that maxed at 5.
        id: 'topic_diversity',
        accessorKey: 'topic_diversity',
        header: 'Diversity',
        size: 110,
        meta: { cellOverflow: 'none' },
        cell: ({ row }) => (
          <span
            className="block text-right tabular-nums text-slate-700"
            title={`Topic evenness ${formatPercent(row.original.topic_diversity, 0)} across ${row.original.distinct_topics} distinct topics`}
          >
            {row.original.distinct_topics > 1
              ? formatPercent(row.original.topic_diversity, 0)
              : '—'}
            <span className="ml-1 text-xs text-slate-400">/ {row.original.distinct_topics}</span>
          </span>
        ),
      },
      {
        id: 'top_topics',
        header: 'Top Topics',
        size: 240,
        enableSorting: false,
        meta: { cellOverflow: 'wrap' },
        cell: ({ row }) => (
          <div className="flex flex-wrap gap-1">
            {row.original.top_topics.slice(0, 3).map((t) => (
              <Badge key={t.topic} variant="secondary" className="text-xs" title={t.topic}>
                {truncate(t.topic, 20)}
              </Badge>
            ))}
          </div>
        ),
      },
    ],
    [],
  )

  return (
    <Card>
      <ActionCardHeader
        icon={FolderOpen}
        accent="text-accent"
        title="Collection intelligence"
        description="Size, impact, year span, and topic diversity for each collection."
        action={
          <Button
            size="sm"
            variant="outline"
            disabled={isLoading}
            onClick={() => setRequested(true)}
          >
            {isLoading ? <Loader2 className="h-4 w-4 animate-spin" /> : 'Generate'}
          </Button>
        }
      />
      {data && (
        <CardContent>
          {data.collections.length === 0 ? (
            <p className="text-sm text-slate-400">No collections found.</p>
          ) : (
            <DataTable<CollectionRow>
              data={data.collections}
              columns={columns}
              storageKey="library.collection-intelligence"
              getRowId={(row) => row.id}
            />
          )}
        </CardContent>
      )}
    </Card>
  )
}
