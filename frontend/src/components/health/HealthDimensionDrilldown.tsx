/**
 * HealthDimensionDrilldown — a CENTERED modal that answers "which papers?" for a
 * Data-health dimension and lets you act on them.
 *
 * Actionable: select rows → batch **Fix selected** (targeted maintenance run for
 * exactly those papers) or **Remove selected** (soft-remove from Library); plus
 * per-row inline edit (add abstract / edit authors where the issue allows),
 * remove, and an open-at-source link. The header carries the bulk "fix the whole
 * dimension" action. Edit/remove apply to Library papers only.
 */
import { useEffect, useMemo, useState } from 'react'
import { useInfiniteQuery, useMutation, useQueryClient } from '@tanstack/react-query'
import { ExternalLink, Loader2, Pencil, Plus, Trash2, Wrench } from 'lucide-react'

import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogHeader,
  DialogTitle,
} from '@/components/ui/dialog'
import { StatusBadge } from '@/components/ui/status-badge'
import { Button } from '@/components/ui/button'
import { Checkbox } from '@/components/ui/checkbox'
import { Textarea } from '@/components/ui/textarea'
import { Input } from '@/components/ui/input'
import { AsyncButton } from '@/components/settings/primitives'
import {
  bulkRemoveFromLibrary,
  getHealthDimensionItems,
  removeFromLibrary,
  runMaintenanceOperation,
  updateSavedPaper,
  type HealthDimension,
  type HealthDimensionItem,
} from '@/api/client'
import { invalidateQueries } from '@/lib/queryHelpers'
import { useToast, errorToast } from '@/hooks/useToast'
import { dimensionBadgeTone, severityLabel } from './healthFormat'

const PAGE = 20

interface HealthDimensionDrilldownProps {
  dim: HealthDimension | null
  open: boolean
  onOpenChange: (open: boolean) => void
  /** Bulk fix the whole dimension (reuses the page's run mutation). */
  onRun: (operationKey: string) => void
  runningKey: string | null
}

function inlineEditField(key?: string): 'abstract' | 'authors' | null {
  if (key === 'papers.missing_abstract') return 'abstract'
  if (key === 'papers.missing_authorships') return 'authors'
  return null
}

function sourceUrl(item: HealthDimensionItem): string | null {
  if (item.doi) return `https://doi.org/${item.doi.replace(/^https?:\/\/(dx\.)?doi\.org\//, '')}`
  if (item.openalex_id)
    return `https://openalex.org/${item.openalex_id.replace(/^https?:\/\/openalex\.org\//, '')}`
  return null
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
  const [selected, setSelected] = useState<Set<string>>(new Set())

  // Reset transient UI state whenever we open a different dimension.
  useEffect(() => {
    setSelected(new Set())
    setEditingId(null)
    setDraft('')
  }, [dim?.key])

  const itemsQuery = useInfiniteQuery({
    queryKey: ['health-dim-items', dim?.key],
    initialPageParam: 0,
    queryFn: async ({ pageParam }) => {
      const offset = typeof pageParam === 'number' ? pageParam : 0
      const res = await getHealthDimensionItems(dim!.key, PAGE, offset)
      // H-11: trust the backend's has_more, not "page was full" — the latter
      // shows a dead "Load more" on an exact final page.
      return { items: res.items, nextOffset: res.has_more ? offset + PAGE : undefined }
    },
    getNextPageParam: (last) => last.nextOffset,
    enabled: open && !!dim,
  })
  const items = useMemo(
    () => itemsQuery.data?.pages.flatMap((p) => p.items) ?? [],
    [itemsQuery.data],
  )

  // The maintenance task a "Fix" should run for this dimension (first run_now action).
  const fixActionKey = dim?.actions.find((a) => a.kind === 'run_now')?.operation_key ?? null
  const runActions = dim?.actions.filter((a) => a.kind === 'run_now') ?? []

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

  const fixSelectedMutation = useMutation({
    // Targeted run: exactly the selected papers, with max_items bound to that
    // count so the backend never processes beyond the user's selection.
    mutationFn: (ids: string[]) =>
      runMaintenanceOperation(fixActionKey!, { target_ids: ids, max_items: ids.length }),
    onSuccess: async (res) => {
      setSelected(new Set())
      await refreshHealth()
      toast({
        title: res.job_id ? 'Fix queued' : 'Nothing to run',
        description: res.job_id ? `${res.key} on the selected papers (${res.job_id}).` : undefined,
      })
    },
    onError: (err) => errorToast('Could not queue fix', String(err)),
  })

  const removeSelectedMutation = useMutation({
    mutationFn: (ids: string[]) => bulkRemoveFromLibrary(ids),
    onSuccess: async (res) => {
      setSelected(new Set())
      await refreshHealth()
      toast({ title: 'Removed from Library', description: `${res.affected} papers soft-removed.` })
    },
    onError: (err) => errorToast('Could not remove selected', String(err)),
  })

  const toggle = (id: string) =>
    setSelected((prev) => {
      const next = new Set(prev)
      next.has(id) ? next.delete(id) : next.add(id)
      return next
    })
  const allShownSelected = items.length > 0 && items.every((i) => selected.has(i.paper_id))
  const toggleAll = () =>
    setSelected(allShownSelected ? new Set() : new Set(items.map((i) => i.paper_id)))

  const selectedIds = [...selected]
  const selectedLibraryIds = items
    .filter((i) => selected.has(i.paper_id) && i.status === 'library')
    .map((i) => i.paper_id)

  return (
    <Dialog open={open} onOpenChange={onOpenChange}>
      <DialogContent className="flex max-h-[85vh] w-full max-w-3xl flex-col gap-0 overflow-hidden bg-surface-1 p-0">
        {dim ? (
          <>
            <DialogHeader className="space-y-2 border-b border-[var(--color-border)] bg-surface-1 px-5 py-4 text-left">
              <div className="flex items-center gap-2">
                <StatusBadge tone={dimensionBadgeTone(dim.severity)} className="capitalize">
                  {severityLabel(dim.severity)}
                </StatusBadge>
                <DialogTitle className="text-alma-900">{dim.label}</DialogTitle>
              </div>
              <DialogDescription className="text-slate-600">{dim.explanation}</DialogDescription>
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
            </DialogHeader>

            {/* Selection toolbar */}
            {items.length > 0 ? (
              <div className="flex flex-wrap items-center gap-3 border-b border-[var(--color-border)] bg-surface-2 px-5 py-2 text-sm">
                <label className="flex items-center gap-2 text-slate-600">
                  <Checkbox
                    checked={allShownSelected}
                    onCheckedChange={() => toggleAll()}
                  />
                  {selected.size > 0 ? `${selected.size} selected` : 'Select all shown'}
                </label>
                {selected.size > 0 ? (
                  <div className="flex flex-wrap gap-2">
                    {fixActionKey ? (
                      <AsyncButton
                        size="sm"
                        variant="outline"
                        icon={<Wrench className="h-4 w-4" />}
                        pending={fixSelectedMutation.isPending}
                        className="border-alma-200 text-alma-700 hover:bg-alma-50"
                        onClick={() => fixSelectedMutation.mutate(selectedIds)}
                      >
                        Fix {selected.size}
                      </AsyncButton>
                    ) : null}
                    {selectedLibraryIds.length > 0 ? (
                      <AsyncButton
                        size="sm"
                        variant="ghost"
                        icon={<Trash2 className="h-4 w-4" />}
                        pending={removeSelectedMutation.isPending}
                        className="text-slate-500 hover:text-critical-700"
                        onClick={() => removeSelectedMutation.mutate(selectedLibraryIds)}
                      >
                        Remove {selectedLibraryIds.length}
                      </AsyncButton>
                    ) : null}
                  </div>
                ) : null}
              </div>
            ) : null}

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
                  const year = (item.publication_date || '').slice(0, 4)
                  const editing = editingId === item.paper_id
                  return (
                    <div
                      key={item.paper_id}
                      className="rounded-sm border border-[var(--color-border)] bg-surface-2 p-3 shadow-paper-sm"
                    >
                      <div className="flex items-start gap-3">
                        <Checkbox
                          className="mt-0.5"
                          checked={selected.has(item.paper_id)}
                          onCheckedChange={() => toggle(item.paper_id)}
                        />
                        <div className="min-w-0 flex-1">
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

                      {editing && editField ? (
                        <div className="mt-2 space-y-2 pl-7">
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
                        <div className="mt-2 flex flex-wrap gap-2 pl-7">
                          {isLibrary && editField ? (
                            <Button
                              size="sm"
                              variant="outline"
                              className="border-alma-200 text-alma-700 hover:bg-alma-50"
                              onClick={() => {
                                setEditingId(item.paper_id)
                                setDraft(editField === 'authors' ? (item.authors ?? '') : '')
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
                              className="text-slate-500 hover:text-critical-700"
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
                    className="w-full"
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
      </DialogContent>
    </Dialog>
  )
}
