import { useState, useMemo, useCallback, useEffect } from 'react'
import { useInfiniteQuery, useQuery, useMutation, useQueryClient } from '@tanstack/react-query'
import {
  Compass,
  Heart,
  FolderOpen,
  Search,
  Trash2,
  Edit3,
  Loader2,
  X,
  Sparkles,
  HeartOff,
  ChevronDown,
  ExternalLink,
  ThumbsUp,
} from 'lucide-react'
import {
  api,
  applyPaperAction,
  bulkAddToCollection,
  bulkRemoveFromLibrary,
  bulkClearRating,
  discoverSimilar,
  getApiErrorMessage,
  listCollections,
  listSavedPapers,
  onlineImportSave,
  type Publication,
  type SimilarityResultItem,
  type SimilarityResponse,
  removeFromLibrary,
  updateSavedPaper,
  updateReadingStatus,
} from '@/api/client'
import { DisclosurePanel } from '@/components/ui/disclosure-panel'
import { Button } from '@/components/ui/button'
import { ErrorState } from '@/components/ui/ErrorState'
import { Input } from '@/components/ui/input'
import { Textarea } from '@/components/ui/textarea'
import { StatusBadge } from '@/components/ui/status-badge'
import { LoadingState } from '@/components/ui/LoadingState'
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from '@/components/ui/select'
import { IdentityChip } from '@/components/shared'
import { PaperPdfLink } from '@/components/pdf/PaperPdfLink'
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from '@/components/ui/dialog'
import { StarRating } from '@/components/StarRating'
import { DataTable } from '@/components/ui/data-table'
import type { ColumnDef, SortingState } from '@tanstack/react-table'
import { useDebounce } from '@/hooks/useDebounce'
import { useToast, errorToast} from '@/hooks/useToast'
import { useOfferUndo, usePaperUndo } from '@/hooks/usePaperUndo'
import { formatAuthorNames } from '@/lib/authorNames'
import { reactionFromRating } from '@/lib/reactions'
import { SOURCE_COLORS, SOURCE_FALLBACK_COLOR } from '@/lib/palette'
import {
  invalidateAfterPaperMutation,
  invalidateQueries,
} from '@/lib/queryHelpers'
import { formatPaperSource, plural } from '@/lib/format'
import { navigateTo } from '@/lib/hashRoute'
import { cn, formatDate, formatPublicationDate, truncate } from '@/lib/utils'
import { ConfirmDialog } from './ConfirmDialog'
import { SimilarResultsDialog } from './SimilarResultsDialog'
import { RowButton } from '@/components/ui/row-button'
import { SubPanel } from '@/components/ui/sub-panel'

type CompactSortKey =
  | 'title'
  | 'authors'
  | 'year'
  | 'journal'
  | 'rating'
  // paper_signal composite — "how relevant is this paper to my taste
  // right now". Distinct from `rating`, which is the user's 0-5 star
  // curation. See `SavedSortOption` docs.
  | 'signal'
  | 'cited_by_count'
  | 'added_at'
type SortDir = 'asc' | 'desc'
type SavedPage = {
  items: Publication[]
  nextOffset?: number
}

const STORAGE_KEYS = {
  prefsVersion: 'alma.library.saved.prefsVersion',
  compactSortKey: 'alma.library.saved.compactSortKey',
  compactSortDir: 'alma.library.saved.compactSortDir',
}
const PREFS_VERSION = '2026-03-07.list-default.v1'

const COMPACT_SORT_KEYS: readonly CompactSortKey[] = [
  'title',
  'authors',
  'year',
  'journal',
  'rating',
  'signal',
  'cited_by_count',
  'added_at',
]
const SORT_DIRECTIONS: readonly SortDir[] = ['asc', 'desc']
const SAVED_PAGE_SIZE = 50
type ReadingStatus = '' | 'reading' | 'done' | 'excluded'

const READING_STATUS_OPTIONS: Array<{ value: 'not_on_list' | 'reading' | 'done' | 'excluded'; label: string }> = [
  { value: 'not_on_list', label: 'Not on reading list' },
  { value: 'reading', label: 'Reading' },
  { value: 'done', label: 'Done' },
  { value: 'excluded', label: 'Excluded' },
]

function readingStatusFromSelect(value: string): ReadingStatus {
  return value === 'not_on_list' ? '' : (value as ReadingStatus)
}


function readStoredValue<T extends string>(key: string, validValues: readonly T[], fallback: T): T {
  if (typeof window === 'undefined') return fallback
  try {
    const stored = window.localStorage.getItem(key)
    if (stored && validValues.includes(stored as T)) return stored as T
  } catch {
    // Ignore localStorage access issues and use defaults.
  }
  return fallback
}

function hasCurrentPrefsVersion(versionKey: string): boolean {
  if (typeof window === 'undefined') return false
  try {
    return window.localStorage.getItem(versionKey) === PREFS_VERSION
  } catch {
    return false
  }
}

function readStoredValueVersioned<T extends string>(
  key: string,
  validValues: readonly T[],
  fallback: T,
  versionKey: string,
): T {
  if (!hasCurrentPrefsVersion(versionKey)) return fallback
  return readStoredValue(key, validValues, fallback)
}


interface SavedTabProps {
  onOpenDetails?: (paper: Publication) => void
}

export function SavedTab({ onOpenDetails }: SavedTabProps = {}) {
  const [search, setSearch] = useState('')
  const [compactSortKey, setCompactSortKey] = useState<CompactSortKey>(() => readStoredValueVersioned(STORAGE_KEYS.compactSortKey, COMPACT_SORT_KEYS, 'year', STORAGE_KEYS.prefsVersion))
  const [compactSortDir, setCompactSortDir] = useState<SortDir>(() => readStoredValueVersioned(STORAGE_KEYS.compactSortDir, SORT_DIRECTIONS, 'desc', STORAGE_KEYS.prefsVersion))
  const [editingLike, setEditingLike] = useState<Publication | null>(null)
  const [editNotes, setEditNotes] = useState('')
  const [editRating, setEditRating] = useState(0)
  const [deleteKey, setDeleteKey] = useState<string | null>(null)
  const [addToCollectionKey, setAddToCollectionKey] = useState<string | null>(null)
  const [bulkCollectionOpen, setBulkCollectionOpen] = useState(false)
  const [bulkDeleteOpen, setBulkDeleteOpen] = useState(false)

  // Multi-select state — always-on pattern matching Feed + AllPapersTab.
  // The checkbox column / hover-overlay is visible at all times; the bulk
  // action bar appears whenever selectedKeys.size > 0. No explicit
  // "select mode" toggle — users just click a checkbox to start selecting.
  const [selectedKeys, setSelectedKeys] = useState<Set<string>>(new Set())
  const [similarOpen, setSimilarOpen] = useState(false)
  const [similarData, setSimilarData] = useState<SimilarityResponse | null>(null)
  const [similarLoading, setSimilarLoading] = useState(false)

  const debouncedSearch = useDebounce(search, 300)
  const queryClient = useQueryClient()
  const { toast } = useToast()
  const queryOrder = useMemo(() => {
    switch (compactSortKey) {
      case 'year':
        return 'date'
      case 'cited_by_count':
        return 'citations'
      case 'signal':
        return 'signal'
      default:
        return compactSortKey
    }
  }, [compactSortKey])
  useEffect(() => {
    try {
      window.localStorage.setItem(STORAGE_KEYS.prefsVersion, PREFS_VERSION)
    } catch {
      // Ignore localStorage access issues.
    }
  }, [])

  useEffect(() => {
    try {
      window.localStorage.setItem(STORAGE_KEYS.compactSortKey, compactSortKey)
    } catch {
      // Ignore localStorage access issues.
    }
  }, [compactSortKey])

  useEffect(() => {
    try {
      window.localStorage.setItem(STORAGE_KEYS.compactSortDir, compactSortDir)
    } catch {
      // Ignore localStorage access issues.
    }
  }, [compactSortDir])

  const likesQuery = useInfiniteQuery({
    queryKey: ['library-saved', debouncedSearch, queryOrder, compactSortDir],
    initialPageParam: 0,
    queryFn: async ({ pageParam }): Promise<SavedPage> => {
      const offset = typeof pageParam === 'number' ? pageParam : 0
      const page = await listSavedPapers({
        search: debouncedSearch || undefined,
        order: queryOrder,
        orderDir: compactSortDir,
        limit: SAVED_PAGE_SIZE + 1,
        offset,
      })
      return {
        items: page.slice(0, SAVED_PAGE_SIZE),
        nextOffset: page.length > SAVED_PAGE_SIZE ? offset + SAVED_PAGE_SIZE : undefined,
      }
    },
    getNextPageParam: (lastPage) => lastPage.nextOffset,
    retry: 1,
  })

  const collectionsQuery = useQuery({
    queryKey: ['library-collections'],
    queryFn: listCollections,
    enabled: !!addToCollectionKey || bulkCollectionOpen,
    retry: 1,
  })

  const likes = useMemo(
    () => likesQuery.data?.pages.flatMap((page) => page.items) ?? [],
    [likesQuery.data],
  )
  const collections = useMemo(() => collectionsQuery.data ?? [], [collectionsQuery.data])
  const visibleLikes = likes
  const hasMore = likesQuery.hasNextPage ?? false

  const { offerUndo, offerReadingUndo } = useOfferUndo()

  const unlikeMutation = useMutation({
    mutationFn: (pubKey: string) => removeFromLibrary(pubKey),
    onSuccess: (_data, pubKey) => {
      void invalidateAfterPaperMutation(queryClient)
      setDeleteKey(null)
      toast({
        title: 'Removed',
        description: 'Publication removed from the saved library.',
        action: offerUndo(pubKey),
      })
    },
    onError: () => {
      errorToast('Error', 'Failed to remove from the saved library.')
    },
  })

  const updateLikeMutation = useMutation({
    mutationFn: ({ pubKey, notes, rating }: { pubKey: string; notes?: string; rating?: number }) =>
      updateSavedPaper(pubKey, { notes, rating }),
    onSuccess: (_data, variables) => {
      void (
        variables.rating === undefined
          ? invalidateQueries(queryClient, ['library-saved'])
          : Promise.all([
              invalidateQueries(queryClient, ['library-saved']),
            ])
      )
      setEditingLike(null)
      toast({ title: 'Updated', description: 'Saved paper updated successfully.' })
    },
    onError: () => {
      errorToast('Error', 'Failed to update saved paper.')
    },
  })

  const undoMutation = usePaperUndo()

  // Like and Love are the `like` / `love` actions, as on every other surface:
  // the one action route owns the rating and records the verb. The star widget
  // and the edit dialog stay on `updateLikeMutation`, because an explicit star
  // value is a `rate`, not a reaction.
  const reactMutation = useMutation({
    mutationFn: ({ paperId, action }: { paperId: string; action: 'like' | 'love' }) =>
      applyPaperAction(paperId, action, { surface: 'library' }),
    onSuccess: (_data, { paperId, action }) => {
      void invalidateAfterPaperMutation(queryClient)
      toast({
        title: action === 'love' ? 'Loved' : 'Liked',
        description: 'Rating updated.',
        action: offerUndo(paperId),
      })
    },
    onError: (err: unknown, { action }) => {
      errorToast(`Could not ${action} the paper`, getApiErrorMessage(err))
    },
  })

  const addToCollectionMutation = useMutation({
    mutationFn: ({ collectionId, pubKey }: { collectionId: string; pubKey: string }) =>
      api.post(`/library/collections/${collectionId}/items`, { paper_id: pubKey }),
    onSuccess: () => {
      void invalidateQueries(queryClient, ['library-collections'])
      setAddToCollectionKey(null)
      toast({ title: 'Added', description: 'Publication added to collection.' })
    },
    onError: () => {
      errorToast('Error', 'Failed to add to collection. It may already be there.')
    },
  })

  // Bulk mutations
  const bulkClearRatingMutation = useMutation({
    mutationFn: (ids: string[]) => bulkClearRating(ids),
    onSuccess: (data) => {
      void Promise.all([
        invalidateQueries(queryClient, ['library-saved']),
      ])
      clearSelection()
      toast({ title: 'Rating cleared', description: `${plural(data.affected, 'paper')} set to no rating.` })
    },
    onError: () => errorToast('Error', 'Bulk clear rating failed.'),
  })

  const bulkRemoveMutation = useMutation({
    mutationFn: (ids: string[]) => bulkRemoveFromLibrary(ids),
    onSuccess: (data) => {
      void invalidateAfterPaperMutation(queryClient)
      clearSelection()
      setBulkDeleteOpen(false)
      toast({ title: 'Removed', description: `${plural(data.affected, 'paper')} removed from library.` })
    },
    onError: () => errorToast('Error', 'Bulk remove failed.'),
  })

  const bulkCollectionMutation = useMutation({
    mutationFn: ({ ids, collectionId }: { ids: string[]; collectionId: string }) =>
      bulkAddToCollection(ids, collectionId),
    onSuccess: (data) => {
      void invalidateQueries(queryClient, ['library-collections'])
      setBulkCollectionOpen(false)
      clearSelection()
      toast({ title: 'Added', description: `${plural(data.affected, 'paper')} added to collection.` })
    },
    onError: () => errorToast('Error', 'Bulk add to collection failed.'),
  })

  const readingStatusMutation = useMutation({
    mutationFn: ({ paperId, readingStatus }: { paperId: string; readingStatus: '' | 'reading' | 'done' | 'excluded' }) =>
      updateReadingStatus(paperId, readingStatus || null),
    onSuccess: (_data, { paperId }) => {
      void invalidateAfterPaperMutation(queryClient)
      const previous = likes.find((like) => like.id === paperId)?.reading_status
      toast({ title: 'Reading status updated', action: offerReadingUndo(paperId, previous) })
    },
    onError: () => errorToast('Error', 'Failed to update reading status.'),
  })

  const selectAll = useCallback(() => {
    const allKeys = visibleLikes.map((l) => l.id)
    setSelectedKeys(new Set(allKeys))
  }, [visibleLikes])

  const clearSelection = useCallback(() => {
    setSelectedKeys(new Set())
  }, [])

  const handleDiscoverSimilar = useCallback(async (force = false) => {
    if (selectedKeys.size === 0) return
    setSimilarOpen(true)
    setSimilarLoading(true)
    try {
      const result = await discoverSimilar(Array.from(selectedKeys), 20, force)
      setSimilarData(result)
    } catch {
      errorToast('Error', 'Failed to discover similar papers.')
    } finally {
      setSimilarLoading(false)
    }
  }, [selectedKeys])

  // "Save to library" on a similar paper is the `add` action, as everywhere
  // else. A local candidate goes through the one paper-action route; a
  // network-only candidate through the Find & Add save route, which resolves it
  // and applies the same contract.
  const handleSaveSimilar = useCallback((item: SimilarityResultItem) => {
    const save = item.paper_id
      ? applyPaperAction(item.paper_id, 'add', { surface: 'library' })
      : onlineImportSave({ action: 'add', doi: item.doi, link: item.url, title: item.title })
    save.then((result) => {
      void invalidateAfterPaperMutation(queryClient)
      const paperId = item.paper_id ?? result.paper_id
      toast({
        title: 'Saved to Library',
        description: `"${item.title}" added to the library.`,
        action: paperId ? offerUndo(paperId) : undefined,
      })
    }).catch((err: unknown) => {
      errorToast('Could not save the paper', getApiErrorMessage(err))
    })
  }, [offerUndo, queryClient, toast])

  useEffect(() => {
    setSelectedKeys((prev) => {
      if (prev.size === 0) return prev
      const visibleIds = new Set(visibleLikes.map((like) => like.id))
      const next = new Set(Array.from(prev).filter((id) => visibleIds.has(id)))
      return next.size === prev.size ? prev : next
    })
  }, [visibleLikes])

  useEffect(() => {
    setSelectedKeys(new Set())
  }, [debouncedSearch, queryOrder, compactSortDir])

  function openEdit(like: Publication) {
    setEditingLike(like)
    setEditNotes(like.notes ?? '')
    setEditRating(like.rating ?? 0)
  }

  function handleSaveEdit() {
    if (!editingLike) return
    updateLikeMutation.mutate({
      pubKey: editingLike.id,
      notes: editNotes,
      rating: editRating,
    })
  }

  return (
    <div className="space-y-4">
      {/* Controls */}
      <div className="flex flex-col gap-3 sm:flex-row sm:items-center">
        <div className="relative flex-1 max-w-md" data-tour="library-card">
          <Search className="absolute left-3 top-1/2 h-4 w-4 -translate-y-1/2 text-slate-400" />
          <Input
            placeholder="Search saved papers..."
            value={search}
            onChange={(e) => setSearch(e.target.value)}
            className="pl-9"
          />
        </div>
      </div>

      {/* Content */}
      {likesQuery.isLoading ? (
        <LoadingState message="Loading saved papers..." />
      ) : likesQuery.isError ? (
        <ErrorState message="Failed to load saved papers." />
      ) : visibleLikes.length === 0 ? (
        <div className="py-16 text-center">
          <Heart className="mx-auto h-12 w-12 text-slate-300" />
          <p className="mt-4 text-sm font-medium text-slate-500">{debouncedSearch ? 'No saved papers match your search' : 'No saved papers yet'}</p>
          <p className="mt-1 text-xs text-slate-400">
            {debouncedSearch ? 'Try another title or author, or clear the search.' : 'Save papers from Feed, Discovery, or similar-results workflows to build the library.'}
          </p>
        </div>
      ) : (
        <SavedCompactTable
          visibleLikes={visibleLikes}
          hasMore={hasMore}
          compactSortKey={compactSortKey}
          compactSortDir={compactSortDir}
          selectedKeys={selectedKeys}
          onSelectionChange={setSelectedKeys}
          onSortChange={(key, dir) => { setCompactSortKey(key); setCompactSortDir(dir) }}
          onRate={(paperId, rating) => updateLikeMutation.mutate({ pubKey: paperId, rating })}
          onReadingStatus={(paperId, readingStatus) => readingStatusMutation.mutate({ paperId, readingStatus })}
          onReact={(paperId, action) => reactMutation.mutate({ paperId, action })}
          onUndoRating={(paperId) => undoMutation.mutate({ paperId, aspect: 'rating' })}
          onEdit={openEdit}
          onAddToCollection={setAddToCollectionKey}
          onOpenDetails={onOpenDetails}
        />
      )}

      {hasMore && !likesQuery.isLoading && !likesQuery.isError && (
        <div className="flex justify-center pt-2">
          <Button
            variant="outline"
            onClick={() => void likesQuery.fetchNextPage()}
            disabled={likesQuery.isFetchingNextPage}
          >
            {likesQuery.isFetchingNextPage ? (
              <Loader2 className="h-4 w-4 animate-spin" />
            ) : (
              <ChevronDown className="h-4 w-4" />
            )}
            {likesQuery.isFetchingNextPage ? 'Loading More' : 'Load More'}
          </Button>
        </div>
      )}

      {/* Floating action bar — always-on selection, so the bar appears
          whenever at least one paper is selected. Matches Feed + AllPapers. */}
      {selectedKeys.size > 0 && (
        <SubPanel variant="flat" padded={false}  className="sticky bottom-dock z-sticky flex flex-wrap items-center gap-2 px-4 py-3 shadow-paper-md">
          <span className="text-sm font-medium text-slate-700">
            {selectedKeys.size} selected
          </span>
          <Button size="sm" variant="outline" onClick={selectAll}>
            Select Loaded
          </Button>
          <Button size="sm" variant="outline" onClick={() => handleDiscoverSimilar()}>
            <Sparkles className="h-4 w-4" />
            Similar
          </Button>
          <Button
            size="sm"
            variant="outline"
            onClick={() => setBulkCollectionOpen(true)}
          >
            <FolderOpen className="h-4 w-4" />
            Collection
          </Button>
          <DisclosurePanel title="Maintain selected papers" contentClassName="flex flex-wrap gap-2 p-3">
          <Button
            size="sm"
            variant="outline"
            onClick={() => bulkClearRatingMutation.mutate(Array.from(selectedKeys))}
            disabled={bulkClearRatingMutation.isPending}
          >
            <HeartOff className="h-4 w-4" />
            Clear Rating
          </Button>
          <Button
            size="sm"
            variant="danger-quiet"
            onClick={() => setBulkDeleteOpen(true)}
          >
            <Trash2 className="h-4 w-4" />
            Remove
          </Button>
          </DisclosurePanel>
          {bulkClearRatingMutation.isPending && <span role="status">Clearing ratings…</span>}
          {bulkClearRatingMutation.isError && <span role="alert">Could not clear ratings. Try again.</span>}
          <Button size="sm" variant="ghost" onClick={clearSelection} aria-label="Clear selection">
            <X className="h-4 w-4" />
          </Button>
        </SubPanel>
      )}

      {/* Similar Results Dialog */}
      <SimilarResultsDialog
        open={similarOpen}
        onOpenChange={setSimilarOpen}
        data={similarData}
        isLoading={similarLoading}
        onRefresh={() => handleDiscoverSimilar(true)}
        onSave={handleSaveSimilar}
      />

      {/* Edit Dialog */}
      <Dialog
        open={!!editingLike}
        onOpenChange={(open) => {
          if (!open) setEditingLike(null)
        }}
      >
        <DialogContent>
          <DialogHeader>
            <DialogTitle>Edit Saved Paper</DialogTitle>
            <DialogDescription>
              {editingLike ? truncate(editingLike.title, 60) : ''}
            </DialogDescription>
          </DialogHeader>
          <div className="space-y-4 py-4">
            <div className="space-y-2">
              <label className="text-sm font-medium text-slate-700">Rating</label>
              <div>
                <StarRating value={editRating} onChange={setEditRating} size="md" />
              </div>
            </div>
            <div className="space-y-2">
              <label className="text-sm font-medium text-slate-700">Notes</label>
              <Textarea
                value={editNotes}
                onChange={(e) => setEditNotes(e.target.value)}
                placeholder="Add personal notes about this paper..."
                rows={3}
              />
            </div>
          </div>
          <DialogFooter>
            <Button variant="outline" onClick={() => setEditingLike(null)}>
              Cancel
            </Button>
            <Button onClick={handleSaveEdit} disabled={updateLikeMutation.isPending}>
              {updateLikeMutation.isPending && <Loader2 className="h-4 w-4 animate-spin" />}
              Save Changes
            </Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>

      {/* Add to Collection Dialog (single) */}
      <Dialog
        open={!!addToCollectionKey}
        onOpenChange={(open) => {
          if (!open) setAddToCollectionKey(null)
        }}
      >
        <DialogContent>
          <DialogHeader>
            <DialogTitle>Add to Collection</DialogTitle>
            <DialogDescription>
              Choose a collection for this publication.
            </DialogDescription>
          </DialogHeader>
          <div className="space-y-2 py-4">
            {collections.length === 0 ? (
              <p className="py-4 text-center text-sm text-slate-500">
                No collections yet. Create one in the Collections tab first.
              </p>
            ) : (
              collections.map((coll) => (
                <RowButton
                  key={coll.id}
                  onClick={() => {
                    if (addToCollectionKey) {
                      addToCollectionMutation.mutate({
                        collectionId: coll.id,
                        pubKey: addToCollectionKey,
                      })
                    }
                  }}
                  disabled={addToCollectionMutation.isPending}
                  className="gap-3 rounded-control border border-control-edge px-4 py-3 hover:border-control-edge-strong hover:bg-control-quiet"
                >
                  <div
                    className="h-3 w-3 rounded-full shrink-0"
                    style={{ backgroundColor: coll.color }}
                  />
                  <div className="min-w-0 flex-1">
                    <p className="text-sm font-medium text-alma-800">{coll.name}</p>
                    {coll.description && (
                      <p className="text-xs text-slate-500">{truncate(coll.description, 50)}</p>
                    )}
                  </div>
                  <StatusBadge tone="neutral">{coll.item_count} items</StatusBadge>
                </RowButton>
              ))
            )}
          </div>
          <DialogFooter>
            <Button variant="outline" onClick={() => setAddToCollectionKey(null)}>
              Cancel
            </Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>

      {/* Bulk Add to Collection Dialog */}
      <Dialog open={bulkCollectionOpen} onOpenChange={setBulkCollectionOpen}>
        <DialogContent>
          <DialogHeader>
            <DialogTitle>Add {selectedKeys.size} Papers to Collection</DialogTitle>
            <DialogDescription>Choose a collection.</DialogDescription>
          </DialogHeader>
          <div className="space-y-2 py-4">
            {collections.length === 0 ? (
              <p className="py-4 text-center text-sm text-slate-500">
                No collections yet. Create one in the Collections tab first.
              </p>
            ) : (
              collections.map((coll) => (
                <RowButton
                  key={coll.id}
                  onClick={() => bulkCollectionMutation.mutate({ ids: Array.from(selectedKeys), collectionId: coll.id })}
                  disabled={bulkCollectionMutation.isPending}
                  className="gap-3 rounded-control border border-control-edge px-4 py-3 hover:border-control-edge-strong hover:bg-control-quiet"
                >
                  <div className="h-3 w-3 rounded-full shrink-0" style={{ backgroundColor: coll.color }} />
                  <div className="min-w-0 flex-1">
                    <p className="text-sm font-medium text-alma-800">{coll.name}</p>
                  </div>
                  <StatusBadge tone="neutral">{coll.item_count} items</StatusBadge>
                </RowButton>
              ))
            )}
          </div>
          <DialogFooter>
            <Button variant="outline" onClick={() => setBulkCollectionOpen(false)}>Cancel</Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>

      {/* Delete Confirmation (single) */}
      <ConfirmDialog
        open={!!deleteKey}
        onOpenChange={(open) => {
          if (!open) setDeleteKey(null)
        }}
        title="Remove from Library"
        description="Are you sure you want to remove this publication from your saved library?"
        onConfirm={() => deleteKey && unlikeMutation.mutate(deleteKey)}
        isPending={unlikeMutation.isPending}
      />

      {/* Bulk Delete Confirmation */}
      <ConfirmDialog
        open={bulkDeleteOpen}
        onOpenChange={setBulkDeleteOpen}
        title={`Remove ${selectedKeys.size} Papers`}
        description={`Remove ${plural(selectedKeys.size, 'paper')} from your library? They will transition to removed status and leave saved Library views.`}
        onConfirm={() => bulkRemoveMutation.mutate(Array.from(selectedKeys))}
        isPending={bulkRemoveMutation.isPending}
      />
    </div>
  )
}

/**
 * Compact table for saved papers, built on the shared `DataTable` primitive
 * so column drag-reorder, visibility, resize, 2-state sort, and row
 * selection all come for free. The parent keeps owning selection, sort,
 * and mutation state — this component only renders and forwards events.
 */
interface SavedCompactTableProps {
  visibleLikes: Publication[]
  hasMore: boolean
  compactSortKey: CompactSortKey
  compactSortDir: SortDir
  selectedKeys: Set<string>
  onSelectionChange: (next: Set<string>) => void
  onSortChange: (key: CompactSortKey, dir: SortDir) => void
  onRate: (paperId: string, rating: number) => void
  onReadingStatus: (paperId: string, readingStatus: ReadingStatus) => void
  onReact: (paperId: string, action: 'like' | 'love') => void
  onUndoRating: (paperId: string) => void
  onEdit: (paper: Publication) => void
  onAddToCollection: (paperId: string) => void
  onOpenDetails?: (paper: Publication) => void
}

function SavedCompactTable({
  visibleLikes,
  hasMore,
  compactSortKey,
  compactSortDir,
  selectedKeys,
  onSelectionChange,
  onSortChange,
  onRate,
  onReadingStatus,
  onReact,
  onUndoRating,
  onEdit,
  onAddToCollection,
  onOpenDetails,
}: SavedCompactTableProps) {
  // Bridge the local (key, dir) tuple state into tanstack's SortingState so
  // the DataTable header indicators stay driven by the same source of truth.
  const sorting: SortingState = [{ id: compactSortKey, desc: compactSortDir === 'desc' }]
  const setSorting = (updater: SortingState | ((prev: SortingState) => SortingState)) => {
    const next = typeof updater === 'function' ? updater(sorting) : updater
    const first = next[0]
    if (!first) return
    onSortChange(first.id as CompactSortKey, first.desc ? 'desc' : 'asc')
  }

  const columns = useMemo<ColumnDef<Publication>[]>(() => [
    {
      id: 'title',
      accessorKey: 'title',
      header: 'Title',
      size: 320,
      // Custom flex layout (title text + trailing external-link icon) — we
      // manage truncation here via `min-w-0` on the inner span so the
      // shrink behaviour is correct inside the flex row.
      meta: { cellOverflow: 'none' },
      cell: ({ row }) => (
        <div className="flex min-w-0 items-center gap-1.5">
          <span className="min-w-0 flex-1 truncate font-medium text-alma-800" title={row.original.title}>
            {row.original.title}
          </span>
          <PaperPdfLink paperId={row.original.id} className="shrink-0 text-slate-400 hover:text-alma-600" />
          {row.original.url && (
            <a
              href={row.original.url}
              target="_blank"
              rel="noopener noreferrer"
              onClick={(e) => e.stopPropagation()}
              className="shrink-0 text-slate-400 hover:text-alma-600"
            >
              <ExternalLink className="h-3 w-3" />
            </a>
          )}
        </div>
      ),
    },
    {
      id: 'authors',
      accessorKey: 'authors',
      header: 'Authors',
      size: 200,
      cell: ({ row }) => (
        <span className="text-slate-600" title={row.original.authors}>
          {formatAuthorNames(row.original.authors)}
        </span>
      ),
    },
    {
      id: 'year',
      accessorFn: (row) => row.publication_date ?? (row.year != null ? String(row.year) : ''),
      header: 'Published',
      size: 110,
      cell: ({ row }) => <span className="whitespace-nowrap text-slate-600">{formatPublicationDate(row.original)}</span>,
    },
    {
      id: 'journal',
      accessorKey: 'journal',
      header: 'Journal',
      size: 160,
      cell: ({ row }) => (
        <span className="text-slate-500" title={row.original.journal ?? ''}>
          {row.original.journal ?? ''}
        </span>
      ),
    },
    {
      id: 'rating',
      accessorFn: (row) => row.rating ?? 0,
      header: 'Rating',
      size: 130,
      meta: { cellOverflow: 'none' },
      cell: ({ row }) => (
        <div onClick={(e) => e.stopPropagation()}>
          <StarRating value={row.original.rating ?? 0} onChange={(r) => onRate(row.original.id, r)} size="sm" />
        </div>
      ),
    },
    {
      id: 'signal',
      accessorFn: (row) => row.global_signal_score ?? 0,
      // Header rendered as a JSX node so we can attach an explanatory
      // tooltip without subclassing the DataTable header component.
      // Distinct from the star "Rating" column above (user curation) —
      // this is the paper_signal composite, the app's view of how
      // relevant this paper is right now.
      header: () => (
        <span title="paper_signal composite — rating + topic + SPECTER2 + author + feedback learning + recency. 0-100. Distinct from your star Rating.">
          Ranking
        </span>
      ),
      size: 100,
      meta: { cellOverflow: 'none', label: 'Ranking' },
      cell: ({ row }) => {
        const raw = row.original.global_signal_score
        if (raw == null || raw <= 0) {
          return (
            <span
              className="block text-center text-caption tabular-nums text-slate-300"
              title="Not yet scored — sort by Ranking once to populate."
            >
              —
            </span>
          )
        }
        const pct = Math.round(Number(raw) * 100)
        return (
          <span
            className="block text-center tabular-nums text-slate-700"
            title={`paper_signal = ${pct}%`}
          >
            {pct}
          </span>
        )
      },
    },
    {
      id: 'cited_by_count',
      accessorFn: (row) => row.cited_by_count ?? 0,
      header: 'Cites',
      size: 80,
      meta: { cellOverflow: 'none' },
      cell: ({ row }) => (
        <span className="block text-center tabular-nums text-slate-600">
          {row.original.cited_by_count ?? 0}
        </span>
      ),
    },
    {
      id: 'added_from',
      header: 'Source',
      size: 110,
      enableSorting: false,
      meta: { cellOverflow: 'none' },
      cell: ({ row }) => {
        const src = row.original.added_from
        if (!src) return <span className="text-xs text-slate-300">—</span>
        return (
          <IdentityChip
            size="sm"
            chipClassName={SOURCE_COLORS[src] ?? SOURCE_FALLBACK_COLOR}
            title="Where this paper came from"
          >
            {formatPaperSource(src)}
          </IdentityChip>
        )
      },
    },
    {
      id: 'added_at',
      accessorKey: 'added_at',
      header: 'Added',
      size: 120,
      cell: ({ row }) => <span className="whitespace-nowrap text-slate-400">{formatDate(row.original.added_at ?? '')}</span>,
    },
    {
      id: 'reading',
      header: 'Read',
      size: 140,
      enableSorting: false,
      meta: { cellOverflow: 'none' },
      cell: ({ row }) => (
        <div onClick={(e) => e.stopPropagation()}>
          <Select
            value={row.original.reading_status || 'not_on_list'}
            onValueChange={(value) => onReadingStatus(row.original.id, readingStatusFromSelect(value))}
          >
            <SelectTrigger className="h-8 text-xs">
              <SelectValue />
            </SelectTrigger>
            <SelectContent>
              {READING_STATUS_OPTIONS.map((option) => (
                <SelectItem key={option.value} value={option.value}>{option.label}</SelectItem>
              ))}
            </SelectContent>
          </Select>
        </div>
      ),
    },
    {
      id: 'actions',
      header: () => <span className="sr-only">Actions</span>,
      size: 176,
      enableSorting: false,
      enableHiding: false,
      meta: { cellOverflow: 'none' },
      cell: ({ row }) => {
        const reaction = reactionFromRating(row.original.rating)
        return (
        <div className="flex items-center justify-end gap-0.5" onClick={(e) => e.stopPropagation()}>
          <Button
            size="icon-sm"
            variant="ghost"
            onClick={() => (reaction === 'like' ? onUndoRating(row.original.id) : onReact(row.original.id, 'like'))}
            title={reaction === 'like' ? 'Remove Like' : 'Like: a positive signal'}
            aria-label="Like"
            aria-pressed={reaction === 'like'}
          >
            <ThumbsUp className={cn('size-3.5 text-success-600', reaction === 'like' && 'fill-current')} />
          </Button>
          <Button
            size="icon-sm"
            variant="ghost"
            onClick={() => (reaction === 'love' ? onUndoRating(row.original.id) : onReact(row.original.id, 'love'))}
            title={reaction === 'love' ? 'Remove Love' : 'Love: a strong positive signal'}
            aria-label="Love"
            aria-pressed={reaction === 'love'}
          >
            <Heart className={cn('size-3.5 text-success-600', reaction === 'love' && 'fill-current')} />
          </Button>
          <Button
            size="icon-sm"
            variant="ghost"
            onClick={() => navigateTo('discovery', { seed: row.original.id, seedTitle: row.original.title })}
            title="Discover similar papers"
            aria-label="Discover similar papers"
          >
            <Compass className="size-3.5" />
          </Button>
          <Button
            size="icon-sm"
            variant="ghost"
            onClick={() => onEdit(row.original)}
            title="Edit notes and rating"
            aria-label="Edit"
          >
            <Edit3 className="size-3.5" />
          </Button>
          <Button
            size="icon-sm"
            variant="ghost"
            onClick={() => onAddToCollection(row.original.id)}
            title="Add to collection"
            aria-label="Add to collection"
          >
            <FolderOpen className="size-3.5" />
          </Button>
        </div>
        )
      },
    },
  ], [onRate, onReadingStatus, onReact, onUndoRating, onEdit, onAddToCollection])

  return (
    <DataTable<Publication>
      data={visibleLikes}
      columns={columns}
      storageKey="library.saved.compact"
      getRowId={(row) => row.id}
      sorting={sorting}
      onSortingChange={setSorting}
      manualSorting
      selectedIds={selectedKeys}
      onSelectionChange={onSelectionChange}
      onRowClick={onOpenDetails}
      footerCaption={
        hasMore
          ? `Loaded ${visibleLikes.length} saved paper${visibleLikes.length !== 1 ? 's' : ''}`
          : `${visibleLikes.length} saved paper${visibleLikes.length !== 1 ? 's' : ''}`
      }
    />
  )
}
