import { useState } from 'react'
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query'
import {
  FolderOpen,
  Plus,
  Trash2,
  Edit3,
  Loader2,
  ChevronDown,
  ChevronRight,
  BookOpen,
  X,
  AlertCircle,
  Compass,
} from 'lucide-react'
import { api, createLens, type Collection } from '@/api/client'
import { Card, CardContent } from '@/components/ui/card'
import { Button } from '@/components/ui/button'
import { ErrorState } from '@/components/ui/ErrorState'
import { Input } from '@/components/ui/input'
import { Badge } from '@/components/ui/badge'
import { LoadingState } from '@/components/ui/LoadingState'
import { PaperCard, type PaperCardPaper } from '@/components/shared'
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from '@/components/ui/dialog'
import { useToast, errorToast} from '@/hooks/useToast'
import { navigateTo } from '@/lib/hashRoute'
import { invalidateQueries } from '@/lib/queryHelpers'
import { formatDate, formatRelativeTime, truncate } from '@/lib/utils'
import { type CollectionItemData, PRESET_COLORS } from './types'
import { ConfirmDialog } from './ConfirmDialog'
import { ColorPicker } from './ColorPicker'

export function CollectionsTab() {
  const [createOpen, setCreateOpen] = useState(false)
  const [editingCollection, setEditingCollection] = useState<Collection | null>(null)
  const [deleteId, setDeleteId] = useState<string | null>(null)
  const [expandedId, setExpandedId] = useState<string | null>(null)

  // Form state
  const [formName, setFormName] = useState('')
  const [formDescription, setFormDescription] = useState('')
  const [formColor, setFormColor] = useState(PRESET_COLORS[0])

  const queryClient = useQueryClient()
  const { toast } = useToast()

  const collectionsQuery = useQuery({
    queryKey: ['library-collections'],
    queryFn: () => api.get<Collection[]>('/library/collections'),
    retry: 1,
  })

  const collectionItemsQuery = useQuery({
    queryKey: ['library-collection-items', expandedId],
    queryFn: () =>
      expandedId
        ? api.get<CollectionItemData[]>(`/library/collections/${expandedId}/items`)
        : Promise.resolve([]),
    enabled: !!expandedId,
    retry: 1,
  })

  const createMutation = useMutation({
    mutationFn: (body: { name: string; description?: string; color?: string }) =>
      api.post<Collection>('/library/collections', body),
    onSuccess: () => {
      void invalidateQueries(queryClient, ['library-collections'])
      setCreateOpen(false)
      resetForm()
      toast({ title: 'Created', description: 'Collection created successfully.' })
    },
    onError: () => {
      errorToast('Error', 'Failed to create collection.')
    },
  })

  const updateMutation = useMutation({
    mutationFn: ({ id, body }: { id: string; body: { name: string; description?: string; color?: string } }) =>
      api.put<Collection>(`/library/collections/${id}`, body),
    onSuccess: () => {
      void invalidateQueries(queryClient, ['library-collections'])
      setEditingCollection(null)
      resetForm()
      toast({ title: 'Updated', description: 'Collection updated.' })
    },
    onError: () => {
      errorToast('Error', 'Failed to update collection.')
    },
  })

  const deleteMutation = useMutation({
    mutationFn: (id: string) => api.delete(`/library/collections/${id}`),
    onSuccess: () => {
      void invalidateQueries(queryClient, ['library-collections'])
      if (expandedId === deleteId) setExpandedId(null)
      setDeleteId(null)
      toast({ title: 'Deleted', description: 'Collection deleted.' })
    },
    onError: () => {
      errorToast('Error', 'Failed to delete collection.')
    },
  })

  const removeItemMutation = useMutation({
    mutationFn: ({ collectionId, paperId }: { collectionId: string; paperId: string }) =>
      api.delete(`/library/collections/${collectionId}/items/${paperId}`),
    onSuccess: async () => {
      await invalidateQueries(queryClient, ['library-collection-items', expandedId], ['library-collections'])
      toast({ title: 'Removed', description: 'Item removed from collection.' })
    },
    onError: () => {
      errorToast('Error', 'Failed to remove item.')
    },
  })

  // T8 — "Turn into Discovery feed" mutation. Creates a new lens with
  // `context_type='collection'`; every refresh re-reads the collection
  // members (backend `_load_seed_papers_for_lens` live-links collection
  // context). Navigates to Discovery with `?lens=<new-id>` so the
  // selector snaps straight to it.
  const toLensMutation = useMutation({
    mutationFn: (coll: Collection) =>
      createLens({
        name: `${coll.name} (Discovery)`,
        context_type: 'collection',
        context_config: { collection_id: coll.id },
      }),
    onSuccess: async (lens) => {
      await invalidateQueries(queryClient, ['lenses'])
      toast({
        title: 'Discovery lens created',
        description: 'Refresh it in Discovery to populate recommendations.',
      })
      navigateTo('discovery', { lens: lens.id })
    },
    onError: () => {
      errorToast('Lens creation failed', 'Could not turn this collection into a Discovery feed.')
    },
  })

  const collections = collectionsQuery.data ?? []
  const collectionItems = collectionItemsQuery.data ?? []

  function resetForm() {
    setFormName('')
    setFormDescription('')
    setFormColor(PRESET_COLORS[0])
  }

  function openCreate() {
    resetForm()
    setCreateOpen(true)
  }

  function openEdit(coll: Collection) {
    setEditingCollection(coll)
    setFormName(coll.name)
    setFormDescription(coll.description ?? '')
    setFormColor(coll.color)
  }

  function handleCreate() {
    if (!formName.trim()) return
    createMutation.mutate({
      name: formName.trim(),
      description: formDescription.trim() || undefined,
      color: formColor,
    })
  }

  function handleUpdate() {
    if (!editingCollection || !formName.trim()) return
    updateMutation.mutate({
      id: editingCollection.id,
      body: {
        name: formName.trim(),
        description: formDescription.trim() || undefined,
        color: formColor,
      },
    })
  }

  const formContent = (
    <div className="space-y-4 py-4">
      <div className="space-y-2">
        <label className="text-sm font-medium text-slate-700">Name</label>
        <Input
          placeholder="e.g., Deep Learning Foundations"
          value={formName}
          onChange={(e) => setFormName(e.target.value)}
        />
      </div>
      <div className="space-y-2">
        <label className="text-sm font-medium text-slate-700">Description</label>
        <textarea
          value={formDescription}
          onChange={(e) => setFormDescription(e.target.value)}
          placeholder="A short description of this collection..."
          rows={2}
          className="flex w-full rounded-lg border border-[var(--color-border)] bg-alma-chrome px-3 py-2 text-sm text-alma-800 placeholder:text-slate-400 focus:border-transparent focus:outline-none focus:ring-2 focus:ring-alma-500"
        />
      </div>
      <div className="space-y-2">
        <label className="text-sm font-medium text-slate-700">Color</label>
        <ColorPicker value={formColor} onChange={setFormColor} />
      </div>
    </div>
  )

  return (
    <div className="space-y-4">
      {/* Header */}
      <div className="flex items-start justify-between gap-3">
        <div>
          <p className="text-sm text-slate-500">
            {collections.length} collection{collections.length !== 1 ? 's' : ''}
          </p>
          <p className="text-xs text-slate-500">
            Collections are arbitrary paper groups you define yourself. Use them as project shelves, reading buckets, or any other manual grouping.
          </p>
        </div>
        <Button onClick={openCreate}>
          <Plus className="h-4 w-4" />
          New Collection
        </Button>
      </div>

      {/* Content */}
      {collectionsQuery.isLoading ? (
        <LoadingState message="Loading collections..." />
      ) : collectionsQuery.isError ? (
        <ErrorState message="Failed to load collections." />
      ) : collections.length === 0 ? (
        <div className="py-16 text-center">
          <FolderOpen className="mx-auto h-12 w-12 text-slate-300" />
          <p className="mt-4 text-sm font-medium text-slate-500">No collections yet</p>
          <p className="mt-1 text-xs text-slate-400">
            Create one to organize your papers.
          </p>
        </div>
      ) : (
        <div className="space-y-3">
          {collections.map((coll) => {
            const isExpanded = expandedId === coll.id
            return (
              <Card key={coll.id} className="overflow-hidden transition-shadow hover:shadow-md">
                <CardContent className="p-0">
                  {/* Collection header */}
                  <div className="flex items-center gap-4 p-5">
                    <div
                      className="h-4 w-4 rounded-full shrink-0 shadow-sm"
                      style={{ backgroundColor: coll.color }}
                    />
                    <div className="min-w-0 flex-1">
                      <div className="flex items-center gap-2 flex-wrap">
                        <h3 className="font-medium text-alma-800">{coll.name}</h3>
                        <Badge variant="secondary">{coll.item_count} papers</Badge>
                        {coll.activity_status === 'fresh' && (
                          <Badge className="bg-green-100 text-green-700 hover:bg-green-100">Fresh</Badge>
                        )}
                        {coll.activity_status === 'active' && (
                          <Badge className="bg-blue-100 text-blue-700 hover:bg-blue-100">Active</Badge>
                        )}
                        {coll.activity_status === 'stale' && (
                          <Badge className="bg-yellow-100 text-yellow-700 hover:bg-yellow-100">Stale</Badge>
                        )}
                        {coll.activity_status === 'dormant' && (
                          <Badge className="bg-parchment-100 text-slate-600 hover:bg-parchment-100">Dormant</Badge>
                        )}
                        {coll.avg_citations != null && coll.avg_citations > 10 && (
                          <Badge variant="outline" className="text-slate-600">
                            ~{Math.round(coll.avg_citations)} cites avg
                          </Badge>
                        )}
                      </div>
                      {coll.description && (
                        <p className="mt-0.5 text-xs text-slate-500">
                          {coll.description}
                        </p>
                      )}
                      <div className="mt-1 flex items-center gap-3 text-xs text-slate-400">
                        <span>Created {formatDate(coll.created_at)}</span>
                        {coll.last_added_at && (
                          <span>• Updated {formatRelativeTime(coll.last_added_at)}</span>
                        )}
                      </div>
                    </div>
                    <div className="flex items-center gap-1 shrink-0">
                      <Button
                        variant="ghost"
                        size="icon"
                        onClick={() => setExpandedId(isExpanded ? null : coll.id)}
                        title={isExpanded ? 'Collapse' : 'View items'}
                      >
                        {isExpanded ? (
                          <ChevronDown className="h-4 w-4 text-slate-500" />
                        ) : (
                          <ChevronRight className="h-4 w-4 text-slate-500" />
                        )}
                      </Button>
                      <Button
                        variant="ghost"
                        size="icon"
                        onClick={() => toLensMutation.mutate(coll)}
                        disabled={toLensMutation.isPending}
                        title="Turn this collection into a Discovery feed (live-linked)"
                      >
                        {toLensMutation.isPending && toLensMutation.variables?.id === coll.id ? (
                          <Loader2 className="h-4 w-4 animate-spin text-alma-500" />
                        ) : (
                          <Compass className="h-4 w-4 text-alma-500" />
                        )}
                      </Button>
                      <Button
                        variant="ghost"
                        size="icon"
                        onClick={() => openEdit(coll)}
                        title="Edit collection"
                      >
                        <Edit3 className="h-4 w-4 text-slate-500" />
                      </Button>
                      <Button
                        variant="ghost"
                        size="icon"
                        onClick={() => setDeleteId(coll.id)}
                        title="Delete collection"
                      >
                        <Trash2 className="h-4 w-4 text-red-400" />
                      </Button>
                    </div>
                  </div>

                  {/* Expanded items */}
                  {isExpanded && (
                    <div className="border-t border-slate-100 bg-parchment-50 px-5 py-4">
                      {collectionItemsQuery.isLoading ? (
                        <div className="flex items-center justify-center py-4">
                          <Loader2 className="h-5 w-5 animate-spin text-alma-600" />
                          <span className="ml-2 text-xs text-slate-500">Loading items...</span>
                        </div>
                      ) : collectionItems.length === 0 ? (
                        <p className="py-4 text-center text-xs text-slate-400">
                          No papers in this collection yet. Add papers from the Favorites tab.
                        </p>
                      ) : (
                        <div className="space-y-2">
                          {collectionItems.map((item) => {
                            const cardPaper: PaperCardPaper = {
                              id: item.id,
                              title: item.title ?? item.id,
                              authors: item.authors ?? undefined,
                              year: item.year ?? null,
                            }
                            const trailingHeader = (
                              <Button
                                variant="ghost"
                                size="icon"
                                onClick={() =>
                                  removeItemMutation.mutate({
                                    collectionId: coll.id,
                                    paperId: item.id,
                                  })
                                }
                                disabled={removeItemMutation.isPending}
                                title="Remove from collection"
                                aria-label="Remove from collection"
                              >
                                <X className="h-3.5 w-3.5 text-red-400" />
                              </Button>
                            )
                            return (
                              <PaperCard
                                key={item.id}
                                paper={cardPaper}
                                size="compact"
                                trailingHeader={trailingHeader}
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
                  )}
                </CardContent>
              </Card>
            )
          })}
        </div>
      )}

      {/* Create Dialog */}
      <Dialog open={createOpen} onOpenChange={setCreateOpen}>
        <DialogContent>
          <DialogHeader>
            <DialogTitle>Create Collection</DialogTitle>
            <DialogDescription>
              Create a new collection to organize your papers.
            </DialogDescription>
          </DialogHeader>
          {formContent}
          {createMutation.isError && (
            <div className="flex items-center gap-2 rounded-lg border border-red-200 bg-red-50 p-3">
              <AlertCircle className="h-4 w-4 text-red-500" />
              <span className="text-sm text-red-700">Failed to create collection. The name may already exist.</span>
            </div>
          )}
          <DialogFooter>
            <Button variant="outline" onClick={() => setCreateOpen(false)}>
              Cancel
            </Button>
            <Button
              onClick={handleCreate}
              disabled={!formName.trim() || createMutation.isPending}
            >
              {createMutation.isPending && <Loader2 className="h-4 w-4 animate-spin" />}
              Create
            </Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>

      {/* Edit Dialog */}
      <Dialog
        open={!!editingCollection}
        onOpenChange={(open) => {
          if (!open) {
            setEditingCollection(null)
            resetForm()
          }
        }}
      >
        <DialogContent>
          <DialogHeader>
            <DialogTitle>Edit Collection</DialogTitle>
            <DialogDescription>
              Update the collection details.
            </DialogDescription>
          </DialogHeader>
          {formContent}
          {updateMutation.isError && (
            <div className="flex items-center gap-2 rounded-lg border border-red-200 bg-red-50 p-3">
              <AlertCircle className="h-4 w-4 text-red-500" />
              <span className="text-sm text-red-700">Failed to update collection.</span>
            </div>
          )}
          <DialogFooter>
            <Button
              variant="outline"
              onClick={() => {
                setEditingCollection(null)
                resetForm()
              }}
            >
              Cancel
            </Button>
            <Button
              onClick={handleUpdate}
              disabled={!formName.trim() || updateMutation.isPending}
            >
              {updateMutation.isPending && <Loader2 className="h-4 w-4 animate-spin" />}
              Save Changes
            </Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>

      {/* Delete Confirmation */}
      <ConfirmDialog
        open={!!deleteId}
        onOpenChange={(open) => {
          if (!open) setDeleteId(null)
        }}
        title="Delete Collection"
        description="Are you sure you want to delete this collection? All items inside will be removed. This action cannot be undone."
        onConfirm={() => deleteId && deleteMutation.mutate(deleteId)}
        isPending={deleteMutation.isPending}
      />
    </div>
  )
}
