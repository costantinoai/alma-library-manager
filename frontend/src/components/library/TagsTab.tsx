import { useMemo, useState } from 'react'
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query'
import {
  Tags,
  Plus,
  Trash2,
  Loader2,
  Star,
  BookOpen,
  AlertCircle,
  GitMerge,
} from 'lucide-react'
import {
  api,
  getTagMergeSuggestions,
  mergeTags,
  type Publication,
  type Tag,
} from '@/api/client'
import { AddonSlot } from '@/addons/AddonSlot'
import { DisclosurePanel } from '@/components/ui/disclosure-panel'
import { Card, CardContent } from '@/components/ui/card'
import { Button } from '@/components/ui/button'
import { ErrorState } from '@/components/ui/ErrorState'
import { Input } from '@/components/ui/input'
import { LoadingState } from '@/components/ui/LoadingState'
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from '@/components/ui/dialog'
import { useToast, errorToast} from '@/hooks/useToast'
import { formatPercent } from '@/lib/format'
import { invalidateQueries } from '@/lib/queryHelpers'
import { PRESET_COLORS } from './types'
import { ConfirmDialog } from './ConfirmDialog'
import { ColorPicker } from './ColorPicker'
import { RowButton } from '@/components/ui/row-button'
import { cn } from '@/lib/utils'
import { Alert } from '@/components/ui/alert'

export function TagsTab() {
  const [tuneOpen, setTuneOpen] = useState(false)
  const [createOpen, setCreateOpen] = useState(false)
  const [deleteId, setDeleteId] = useState<string | null>(null)
  const [assignTagId, setAssignTagId] = useState<string | null>(null)
  const [assignPubKey, setAssignPubKey] = useState('')

  // Form state
  const [formName, setFormName] = useState('')
  const [formColor, setFormColor] = useState(PRESET_COLORS[2])

  const queryClient = useQueryClient()
  const { toast } = useToast()

  const tagsQuery = useQuery({
    queryKey: ['library-tags'],
    queryFn: () => api.get<Tag[]>('/library/tags'),
    retry: 1,
  })

  const likesQuery = useQuery({
    queryKey: ['library-saved', 'tag-picker'],
    queryFn: () => api.get<Publication[]>('/library/saved?limit=300'),
    enabled: !!assignTagId,
    retry: 1,
  })

  const mergeSuggestionsQuery = useQuery({
    queryKey: ['tag-merge-suggestions'],
    queryFn: () => getTagMergeSuggestions(12, 0.8),
    enabled: tuneOpen,
    retry: 1,
  })

  const createMutation = useMutation({
    mutationFn: (body: { name: string; color?: string }) => api.post<Tag>('/library/tags', body),
    onSuccess: () => {
      void invalidateQueries(queryClient, ['library-tags'], ['tag-merge-suggestions'])
      setCreateOpen(false)
      resetForm()
      toast({ title: 'Created', description: 'Tag created successfully.' })
    },
    onError: () => {
      errorToast('Error', 'Failed to create tag. The name may already exist.')
    },
  })

  const deleteMutation = useMutation({
    mutationFn: (id: string) => api.delete(`/library/tags/${id}`),
    onSuccess: () => {
      void invalidateQueries(queryClient, ['library-tags'], ['tag-merge-suggestions'])
      setDeleteId(null)
      toast({ title: 'Deleted', description: 'Tag deleted.' })
    },
    onError: () => {
      errorToast('Error', 'Failed to delete tag.')
    },
  })

  const assignMutation = useMutation({
    mutationFn: (body: { paper_id: string; tag_id: string }) => api.post('/library/tags/assign', body),
    onSuccess: () => {
      setAssignTagId(null)
      setAssignPubKey('')
      void invalidateQueries(queryClient, ['library-tags'])
      toast({ title: 'Assigned', description: 'Tag assigned to publication.' })
    },
    onError: () => {
      errorToast('Error')
    },
  })

  const mergeMutation = useMutation({
    mutationFn: ({ sourceTagId, targetTagId }: { sourceTagId: string; targetTagId: string }) =>
      mergeTags(sourceTagId, targetTagId),
    onSuccess: () => {
      void invalidateQueries(queryClient, ['library-tags'], ['tag-merge-suggestions'])
      toast({ title: 'Merged', description: 'Tags merged successfully.' })
    },
    onError: () => {
      errorToast('Error', 'Failed to merge tags.')
    },
  })

  const tags = tagsQuery.data ?? []
  const likes = useMemo(() => likesQuery.data ?? [], [likesQuery.data])

  function resetForm() {
    setFormName('')
    setFormColor(PRESET_COLORS[2])
  }

  function handleCreate() {
    if (!formName.trim()) return
    createMutation.mutate({
      name: formName.trim(),
      color: formColor,
    })
  }

  function handleAssign() {
    if (!assignTagId || !assignPubKey) return
    assignMutation.mutate({
      paper_id: assignPubKey,
      tag_id: assignTagId,
    })
  }

  return (
    <div className="space-y-4">
      {/* Header */}
      <div className="flex flex-wrap items-center justify-between gap-2">
        <div className="space-y-1">
          <div className="flex items-center gap-2">
            <p className="text-sm text-slate-500">
              {tags.length} tag{tags.length !== 1 ? 's' : ''}
            </p>

          </div>
          <p className="text-xs text-slate-500">
            Keywords for search and signals. They do not need to be official topics, and each paper may have at most 5 tags.
          </p>
        </div>
        <div className="flex items-center gap-2">
          <Button onClick={() => { resetForm(); setCreateOpen(true) }}>
            <Plus className="h-4 w-4" />
            New Tag
          </Button>
        </div>
      </div>

      {mergeMutation.isPending && <p role="status">Updating tags…</p>}
      {(likesQuery.isError || mergeSuggestionsQuery.isError) && <ErrorState message="Merge suggestions or paper choices could not be loaded." actionLabel="Retry" onAction={() => { void likesQuery.refetch(); if (tuneOpen) void mergeSuggestionsQuery.refetch(); else setTuneOpen(true) }} />}
      <DisclosurePanel title="Tune tags" description="Tidy duplicate and overlapping tags." open={tuneOpen} onOpenChange={setTuneOpen}>
      {tuneOpen && <AddonSlot name="tags.tune" />}
      <Card>
        <CardContent className="space-y-3 p-4">
          <div>
            <h3 className="text-sm font-semibold text-slate-800">Merge Suggestions</h3>
            <p className="text-xs text-slate-500">Consolidate duplicate tags.</p>
          </div>
          {mergeSuggestionsQuery.isLoading ? (
            <div className="flex items-center gap-2 text-xs text-slate-500">
              <Loader2 className="h-3.5 w-3.5 animate-spin" />
              Loading merge suggestions...
            </div>
          ) : (mergeSuggestionsQuery.data ?? []).length === 0 ? (
            <p className="text-xs text-slate-500">No merge candidates detected.</p>
          ) : (
            <div className="space-y-2">
              {mergeSuggestionsQuery.data?.map((item) => (
                <div
                  key={`${item.source_tag_id}:${item.target_tag_id}`}
                  className="flex flex-wrap items-center justify-between gap-2 rounded-md border border-[var(--color-border)] bg-surface-1 px-3 py-2"
                >
                  <div className="text-sm text-slate-700">
                    <span className="font-medium">{item.source_tag}</span>
                    <span className="mx-2 text-slate-400">→</span>
                    <span className="font-medium">{item.target_tag}</span>
                    <span className="ml-2 text-xs text-slate-500">
                      {formatPercent(item.confidence, 0)} · {item.reason}
                    </span>
                  </div>
                  <Button
                    size="sm"
                    variant="outline"
                    onClick={() => mergeMutation.mutate({ sourceTagId: item.source_tag_id, targetTagId: item.target_tag_id })}
                    disabled={mergeMutation.isPending}
                  >
                    <GitMerge className="h-3.5 w-3.5" />
                    Merge
                  </Button>
                </div>
              ))}
            </div>
          )}
        </CardContent>
      </Card>

      </DisclosurePanel>

      {/* Content */}
      {tagsQuery.isLoading ? (
        <LoadingState message="Loading tags..." />
      ) : tagsQuery.isError ? (
        <ErrorState message="Failed to load tags." />
      ) : tags.length === 0 ? (
        <div className="py-16 text-center">
          <Tags className="mx-auto h-12 w-12 text-slate-300" />
          <p className="mt-4 text-sm font-medium text-slate-500">No tags yet</p>
          <p className="mt-1 text-xs text-slate-400">
            Create tags to categorize papers and improve recommendations.
          </p>
        </div>
      ) : (
        <div className="grid grid-cols-1 gap-3 sm:grid-cols-2 lg:grid-cols-3">
          {tags.map((tag) => (
            <Card key={tag.id} className="transition-shadow hover:shadow-paper-sheet-hover">
              <CardContent className="p-4">
                <div className="flex items-center justify-between gap-3">
                  <div className="min-w-0 flex items-center gap-2.5">
                    <div
                      className="h-3.5 w-3.5 shrink-0 rounded-full shadow-paper-sm"
                      style={{ backgroundColor: tag.color }}
                    />
                    <span className="truncate font-medium text-alma-800">{tag.name}</span>
                  </div>
                  <div className="flex shrink-0 items-center gap-1">
                    <Button
                      variant="ghost"
                      size="icon"
                      onClick={() => {
                        setAssignTagId(tag.id)
                        setAssignPubKey('')
                      }}
                      title="Assign to publication"
                    >
                      <Plus className="h-4 w-4 text-alma-500" />
                    </Button>
                    <Button
                      variant="ghost"
                      size="icon"
                      onClick={() => setDeleteId(tag.id)}
                      title="Delete tag"
                    >
                      <Trash2 className="h-4 w-4 text-critical-500" />
                    </Button>
                  </div>
                </div>
              </CardContent>
            </Card>
          ))}
        </div>
      )}

      {/* Create Tag Dialog */}
      <Dialog open={createOpen} onOpenChange={setCreateOpen}>
        <DialogContent>
          <DialogHeader>
            <DialogTitle>Create Tag</DialogTitle>
            <DialogDescription>Create a new tag to categorize your publications.</DialogDescription>
          </DialogHeader>
          <div className="space-y-4 py-4">
            <div className="space-y-2">
              <label className="text-sm font-medium text-slate-700">Name</label>
              <Input
                placeholder="e.g., Machine Learning, NLP, Must Read"
                value={formName}
                onChange={(e) => setFormName(e.target.value)}
              />
            </div>
            <div className="space-y-2">
              <label className="text-sm font-medium text-slate-700">Color</label>
              <ColorPicker value={formColor} onChange={setFormColor} />
            </div>
          </div>
          {createMutation.isError && (
            <Alert variant="negative" layout="row">
              <AlertCircle className="h-4 w-4 text-critical-500" />
              <span className="text-sm text-critical-700">Failed to create tag.</span>
            </Alert>
          )}
          <DialogFooter>
            <Button variant="outline" onClick={() => setCreateOpen(false)}>
              Cancel
            </Button>
            <Button onClick={handleCreate} disabled={!formName.trim() || createMutation.isPending}>
              {createMutation.isPending && <Loader2 className="h-4 w-4 animate-spin" />}
              Create Tag
            </Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>

      {/* Assign Tag Dialog */}
      <Dialog
        open={!!assignTagId}
        onOpenChange={(open) => {
          if (!open) {
            setAssignTagId(null)
            setAssignPubKey('')
          }
        }}
      >
        <DialogContent>
          <DialogHeader>
            <DialogTitle>Assign Tag to Publication</DialogTitle>
            <DialogDescription>Select a publication from your library to tag.</DialogDescription>
          </DialogHeader>
          <div className="space-y-3 py-4">
            {likes.length === 0 ? (
              <p className="py-4 text-center text-sm text-slate-500">No library publications found.</p>
            ) : (
              <div className="max-h-64 space-y-2 overflow-y-auto">
                {likes.map((like) => (
                  <RowButton selected={assignPubKey === like.id}
                    key={like.id}
                    onClick={() => setAssignPubKey(like.id)}
                    className={cn('gap-3 rounded-lg border px-3 py-2.5', assignPubKey === like.id ? 'border-alma-folio text-inherit' : 'border-control-edge hover:border-control-edge-strong hover:bg-control-quiet')}
                  >
                    <BookOpen className="h-4 w-4 shrink-0 text-slate-400" />
                    <div className="min-w-0 flex-1">
                      <p className="truncate text-sm font-medium text-slate-800">{like.title}</p>
                      <p className="text-xs text-slate-500">{like.authors}</p>
                    </div>
                    <div className="flex items-center gap-0.5">
                      {Array.from({ length: like.rating ?? 0 }).map((_, i) => (
                        <Star key={i} className="h-3 w-3 fill-warning-500 text-warning-500" />
                      ))}
                    </div>
                  </RowButton>
                ))}
              </div>
            )}
          </div>
          <DialogFooter>
            <Button
              variant="outline"
              onClick={() => {
                setAssignTagId(null)
                setAssignPubKey('')
              }}
            >
              Cancel
            </Button>
            <Button onClick={handleAssign} disabled={!assignPubKey || assignMutation.isPending}>
              {assignMutation.isPending && <Loader2 className="h-4 w-4 animate-spin" />}
              Assign Tag
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
        title="Delete Tag"
        description="Are you sure you want to delete this tag? All publication assignments will be removed. This action cannot be undone."
        onConfirm={() => deleteId && deleteMutation.mutate(deleteId)}
        isPending={deleteMutation.isPending}
      />
    </div>
  )
}
