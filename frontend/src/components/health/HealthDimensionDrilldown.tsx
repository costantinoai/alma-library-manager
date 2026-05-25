/**
 * HealthDimensionDrilldown — a right-side Sheet that answers "which papers?"
 * for a Data Health dimension and offers per-issue fixes.
 *
 * Per the task-24 drilldown spec: the auto-fix runs as a bulk header action
 * (reuses the maintenance run endpoint), while per-row operations are the
 * manual, one-at-a-time ones the user asked for — Add abstract / Edit authors
 * (inline, via PUT /library/saved), Remove from library (soft remove), and an
 * Open link to the source. Edit/Remove apply only to Library papers.
 */
import { useState } from 'react'
import { useInfiniteQuery, useMutation, useQueryClient } from '@tanstack/react-query'
import { ExternalLink, Loader2, Pencil, Plus, Trash2, Wrench } from 'lucide-react'

import {
  Sheet,
  SheetContent,
  SheetDescription,
  SheetHeader,
  SheetTitle,
} from '@/components/ui/sheet'
import { StatusBadge } from '@/components/ui/status-badge'
import { Button } from '@/components/ui/button'
import { Textarea } from '@/components/ui/textarea'
import { Input } from '@/components/ui/input'
import { AsyncButton } from '@/components/settings/primitives'
import {
  getHealthDimensionItems,
  removeFromLibrary,
  updateSavedPaper,
  type HealthDimension,
  type HealthDimensionItem,
} from '@/api/client'
import { invalidateQueries } from '@/lib/queryHelpers'
import { useToast, errorToast } from '@/hooks/useToast'
import { cn } from '@/lib/utils'
import { dimensionBadgeTone, severityLabel } from './healthFormat'

const PAGE = 20

interface HealthDimensionDrilldownProps {
  /** The dimension to drill into; null closes the sheet. */
  dim: HealthDimension | null
  open: boolean
  onOpenChange: (open: boolean) => void
  /** Bulk fix (reuses the page's run mutation). */
  onRun: (operationKey: string) => void
  runningKey: string | null
}

/** Which single field this dimension lets the user fix inline. */
function inlineEditField(key?: string): 'abstract' | 'authors' | null {
  if (key === 'papers.missing_abstract') return 'abstract'
  if (key === 'papers.missing_authorships') return 'authors'
  return null
}

function sourceUrl(item: HealthDimensionItem): string | null {
  if (item.doi) return `https://doi.org/${item.doi.replace(/^https?:\/\/(dx\.)?doi\.org\//, '')}`
  if (item.openalex_id) return `https://openalex.org/${item.openalex_id.replace(/^https?:\/\/openalex\.org\//, '')}`
  return null
}

function yearOf(date?: string | null): string {
  if (!date) return ''
  return date.slice(0, 4)
}

export function HealthDimensionDrilldown({
  dim,
  open,
  onOpenChange,
  onRun,
  runningKey,
}: HealthDimensionDrilldownProps) {
  const queryClient = useQueryClient()
  const { toast } = useToast()
  const editField = inlineEditField(dim?.key)
  const [editingId, setEditingId] = useState<string | null>(null)
  const [draft, setDraft] = useState('')

  const itemsQuery = useInfiniteQuery({
    queryKey: ['health-dim-items', dim?.key],
    initialPageParam: 0,
    queryFn: async ({ pageParam }) => {
      const offset = typeof pageParam === 'number' ? pageParam : 0
      const res = await getHealthDimensionItems(dim!.key, PAGE, offset)
      return { items: res.items, nextOffset: res.items.length === PAGE ? offset + PAGE : undefined }
    },
    getNextPageParam: (last) => last.nextOffset,
    enabled: open && !!dim,
  })
  const items = itemsQuery.data?.pages.flatMap((p) => p.items) ?? []

  const refreshHealth = () =>
    invalidateQueries(
      queryClient,
      ['health-dim-items', dim?.key],
      ['health', 'snapshot'],
      ['health', 'operations'],
    )

  const saveMutation = useMutation({
    mutationFn: ({ id, field, value }: { id: string; field: 'abstract' | 'authors'; value: string }) =>
      updateSavedPaper(id, { [field]: value }),
    onSuccess: async () => {
      setEditingId(null)
      setDraft('')
      await refreshHealth()
      toast({ title: 'Saved', description: 'The paper was updated.' })
    },
    onError: (err) => errorToast('Could not save', String(err)),
  })

  const removeMutation = useMutation({
    mutationFn: (id: string) => removeFromLibrary(id),
    onSuccess: async () => {
      await refreshHealth()
      toast({ title: 'Removed from Library', description: 'The paper was soft-removed.' })
    },
    onError: (err) => errorToast('Could not remove', String(err)),
  })

  const runActions = dim?.actions.filter((a) => a.kind === 'run_now') ?? []

  return (
    <Sheet open={open} onOpenChange={onOpenChange}>
      <SheetContent
        side="right"
        className="flex w-full flex-col gap-0 overflow-hidden bg-alma-chrome p-0 sm:max-w-2xl"
      >
        {dim ? (
          <>
            <SheetHeader className="space-y-2 border-b border-[var(--color-border)] bg-alma-content px-5 py-4">
              <div className="flex items-center gap-2">
                <StatusBadge tone={dimensionBadgeTone(dim.severity)} className="capitalize">
                  {severityLabel(dim.severity)}
                </StatusBadge>
                <SheetTitle className="text-alma-900">{dim.label}</SheetTitle>
              </div>
              <SheetDescription className="text-slate-600">{dim.explanation}</SheetDescription>
              {dim.impact ? <p className="text-xs text-slate-500">{dim.impact}</p> : null}
              {runActions.length > 0 ? (
                <div className="flex flex-wrap gap-2 pt-1">
                  {runActions.map((action) => (
                    <AsyncButton
                      key={action.operation_key}
                      size="sm"
                      variant="outline"
                      icon={<Wrench className="h-4 w-4" />}
                      pending={runningKey === action.operation_key}
                      disabled={runningKey != null && runningKey !== action.operation_key}
                      className="border-alma-200 text-alma-700 hover:bg-alma-50"
                      onClick={() => onRun(action.operation_key)}
                    >
                      {action.label} (all)
                    </AsyncButton>
                  ))}
                </div>
              ) : null}
            </SheetHeader>

            <div className="min-h-0 flex-1 space-y-2 overflow-y-auto px-5 py-4">
              {itemsQuery.isLoading ? (
                <div className="flex items-center gap-2 text-sm text-slate-500">
                  <Loader2 className="h-4 w-4 animate-spin" /> Loading affected papers…
                </div>
              ) : items.length === 0 ? (
                <p className="text-sm text-slate-500">No affected papers found.</p>
              ) : (
                items.map((item) => {
                  const isLibrary = item.status === 'library'
                  const url = sourceUrl(item)
                  const year = yearOf(item.publication_date)
                  const editing = editingId === item.paper_id
                  return (
                    <div
                      key={item.paper_id}
                      className="rounded-sm border border-[var(--color-border)] bg-alma-content-elev p-3 shadow-paper-sm"
                    >
                      <div className="flex items-start justify-between gap-3">
                        <div className="min-w-0">
                          <p className="truncate text-sm font-medium text-alma-800" title={item.title}>
                            {url ? (
                              <a
                                href={url}
                                target="_blank"
                                rel="noreferrer"
                                className="inline-flex items-center gap-1 hover:text-alma-folio hover:underline"
                              >
                                {item.title}
                                <ExternalLink className="h-3 w-3 shrink-0 opacity-60" />
                              </a>
                            ) : (
                              item.title
                            )}
                          </p>
                          <p className="mt-0.5 truncate text-xs text-slate-500">
                            {year ? `${year} · ` : ''}
                            {item.authors || 'no authors'}
                          </p>
                        </div>
                        <StatusBadge tone="neutral" size="sm" className="shrink-0">
                          {item.detail}
                        </StatusBadge>
                      </div>

                      {/* Inline editor */}
                      {editing && editField ? (
                        <div className="mt-2 space-y-2">
                          {editField === 'abstract' ? (
                            <Textarea
                              autoFocus
                              rows={4}
                              value={draft}
                              onChange={(e) => setDraft(e.target.value)}
                              placeholder="Paste the abstract…"
                            />
                          ) : (
                            <Input
                              autoFocus
                              value={draft}
                              onChange={(e) => setDraft(e.target.value)}
                              placeholder="Author 1, Author 2, …"
                            />
                          )}
                          <div className="flex justify-end gap-2">
                            <Button
                              size="sm"
                              variant="ghost"
                              onClick={() => {
                                setEditingId(null)
                                setDraft('')
                              }}
                            >
                              Cancel
                            </Button>
                            <AsyncButton
                              size="sm"
                              pending={saveMutation.isPending}
                              disabled={!draft.trim()}
                              onClick={() =>
                                saveMutation.mutate({
                                  id: item.paper_id,
                                  field: editField,
                                  value: draft.trim(),
                                })
                              }
                            >
                              Save
                            </AsyncButton>
                          </div>
                        </div>
                      ) : (
                        <div className="mt-2 flex flex-wrap gap-2">
                          {isLibrary && editField ? (
                            <Button
                              size="sm"
                              variant="outline"
                              className="border-alma-200 text-alma-700 hover:bg-alma-50"
                              onClick={() => {
                                setEditingId(item.paper_id)
                                setDraft(editField === 'authors' ? item.authors ?? '' : '')
                              }}
                            >
                              {editField === 'abstract' ? (
                                <>
                                  <Plus className="h-4 w-4" /> Add abstract
                                </>
                              ) : (
                                <>
                                  <Pencil className="h-4 w-4" /> Edit authors
                                </>
                              )}
                            </Button>
                          ) : null}
                          {isLibrary ? (
                            <AsyncButton
                              size="sm"
                              variant="ghost"
                              icon={<Trash2 className="h-4 w-4" />}
                              pending={removeMutation.isPending && removeMutation.variables === item.paper_id}
                              className="text-slate-500 hover:text-rose-700"
                              onClick={() => removeMutation.mutate(item.paper_id)}
                            >
                              Remove
                            </AsyncButton>
                          ) : (
                            <span className="text-[11px] text-slate-400">
                              Tracked (not in Library) — edit/remove unavailable
                            </span>
                          )}
                        </div>
                      )}
                    </div>
                  )
                })
              )}

              {itemsQuery.hasNextPage ? (
                <div className="pt-1">
                  <Button
                    size="sm"
                    variant="ghost"
                    className={cn('w-full', itemsQuery.isFetchingNextPage && 'opacity-70')}
                    disabled={itemsQuery.isFetchingNextPage}
                    onClick={() => void itemsQuery.fetchNextPage()}
                  >
                    {itemsQuery.isFetchingNextPage ? 'Loading…' : 'Load more'}
                  </Button>
                </div>
              ) : null}
            </div>
          </>
        ) : null}
      </SheetContent>
    </Sheet>
  )
}
