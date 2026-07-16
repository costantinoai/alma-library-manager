import { useMemo, useState } from 'react'
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query'
import { FolderPlus, Plus } from 'lucide-react'
import { Popover, PopoverContent, PopoverTrigger } from '@/components/ui/popover'
import { Button } from '@/components/ui/button'
import { Input } from '@/components/ui/input'
import { Checkbox } from '@/components/ui/checkbox'
import { ScrollArea } from '@/components/ui/scroll-area'
import { createCollection, listCollections } from '@/api/client'
import { invalidateQueries } from '@/lib/queryHelpers'
import { useToast } from '@/hooks/useToast'

interface AddToCollectionMenuProps {
  /** Called with the chosen collection ids once the user confirms. The caller
   *  performs the actual save (e.g. `saveRecommendation(recId, undefined, ids)`)
   *  so this control stays agnostic to whether the paper is a rec or a saved
   *  Library row. */
  onConfirm: (collectionIds: string[]) => void | Promise<void>
  /** Disables the trigger (e.g. while a save is already in flight). */
  disabled?: boolean
  /** Compact icon-only trigger for dense card action rows. */
  compact?: boolean
  defaultSelectedIds?: string[]
  isSaved?: boolean
}

/**
 * Paper-card affordance to add a paper to Library AND file it into one or more
 * collections in a single action. Lists existing collections as a multi-select
 * and lets the user create a new collection inline. Emits the selected
 * collection ids through `onConfirm`; the caller wires the save.
 */
export function AddToCollectionMenu({
  onConfirm,
  disabled,
  compact,
  defaultSelectedIds = [],
  isSaved = false,
}: AddToCollectionMenuProps) {
  const [open, setOpen] = useState(false)
  const [selected, setSelected] = useState<Set<string>>(
    new Set(defaultSelectedIds.filter(Boolean)),
  )
  const [newName, setNewName] = useState('')
  const [submitting, setSubmitting] = useState(false)
  const queryClient = useQueryClient()
  const { toast } = useToast()

  const collectionsQuery = useQuery({
    queryKey: ['library-collections'],
    queryFn: listCollections,
    enabled: open,
  })
  const collections = useMemo(() => collectionsQuery.data ?? [], [collectionsQuery.data])

  const createMutation = useMutation({
    mutationFn: (name: string) => createCollection({ name }),
    onSuccess: async (created) => {
      await invalidateQueries(queryClient, ['library-collections'])
      setSelected((prev) => new Set(prev).add(created.id))
      setNewName('')
    },
    onError: () => {
      toast({ title: 'Error', description: 'Could not create collection (name may already exist).' })
    },
  })

  const toggle = (id: string) => {
    setSelected((prev) => {
      const next = new Set(prev)
      if (next.has(id)) next.delete(id)
      else next.add(id)
      return next
    })
  }

  const reset = () => {
    setSelected(new Set(defaultSelectedIds.filter(Boolean)))
    setNewName('')
  }

  const handleConfirm = async () => {
    const ids = Array.from(selected)
    if (ids.length === 0) return
    setSubmitting(true)
    try {
      await onConfirm(ids)
      await invalidateQueries(
        queryClient,
        ['library-collections'],
        ['library-saved'],
        ['papers'],
        ['bootstrap'],
      )
      toast({
        title: isSaved ? 'Added to collections' : 'Saved and added to collections',
        description: `${ids.length} collection${ids.length === 1 ? '' : 's'} updated.`,
      })
      setOpen(false)
      reset()
    } catch {
      // Keep the popover + selection intact so the user can retry. Without this
      // catch a rejected save escaped as an unhandled promise and failed with no
      // visible feedback.
      toast({
        title: 'Could not add to collections',
        description: 'Nothing changed. Try again.',
        variant: 'destructive',
      })
    } finally {
      setSubmitting(false)
    }
  }

  return (
    <Popover
      open={open}
      onOpenChange={(next) => {
        setOpen(next)
        reset()
      }}
    >
      <PopoverTrigger asChild>
        <Button
          variant="ghost"
          size="sm"
          disabled={disabled}
          title="Save to Library and add to collections"
          aria-label={compact ? 'Collections' : undefined}
          className="gap-1.5 rounded-md border border-[var(--color-border)] bg-surface-1 text-slate-700 hover:bg-accent-soft hover:text-alma-folio"
          onClick={(e) => e.stopPropagation()}
        >
          <FolderPlus className="h-4 w-4" />
          {!compact && <span>Collections</span>}
        </Button>
      </PopoverTrigger>
      <PopoverContent
        align="start"
        className="w-72 p-3"
        onClick={(e) => e.stopPropagation()}
      >
        <p className="mb-2 text-xs font-semibold uppercase tracking-wide text-slate-500">
          Add to collection(s)
        </p>
        <p className="mb-2 text-xs text-slate-500">
          Adding to a collection also saves this paper to Library.
        </p>

        {collections.length > 0 ? (
          <ScrollArea className="max-h-48 pr-2">
            <div className="space-y-1">
              {collections.map((c) => (
                <label
                  key={c.id}
                  className="flex cursor-pointer items-center gap-2 rounded-md px-1.5 py-1 text-sm hover:bg-surface-2"
                >
                  <Checkbox
                    checked={selected.has(c.id)}
                    onCheckedChange={() => toggle(c.id)}
                  />
                  <span className="truncate text-slate-800">{c.name}</span>
                  <span className="ml-auto text-xs text-slate-400">{c.item_count}</span>
                </label>
              ))}
            </div>
          </ScrollArea>
        ) : (
          <p className="px-1 py-2 text-sm text-slate-400">
            {collectionsQuery.isLoading ? 'Loading…' : 'No collections yet.'}
          </p>
        )}

        <div className="mt-2 flex items-center gap-1.5 border-t border-edge-2 pt-2">
          <Input
            value={newName}
            onChange={(e) => setNewName(e.target.value)}
            placeholder="New collection…"
            className="h-8 text-sm"
            onKeyDown={(e) => {
              if (e.key === 'Enter' && newName.trim()) {
                e.preventDefault()
                createMutation.mutate(newName.trim())
              }
            }}
          />
          <Button
            variant="ghost"
            size="sm"
            className="shrink-0"
            disabled={!newName.trim() || createMutation.isPending}
            onClick={() => createMutation.mutate(newName.trim())}
            title="Create collection"
          >
            <Plus className="h-4 w-4" />
          </Button>
        </div>

        <div className="mt-3 flex justify-end gap-2">
          <Button variant="ghost" size="sm" onClick={() => setOpen(false)}>
            Cancel
          </Button>
          <Button
            size="sm"
            disabled={selected.size === 0 || submitting}
            onClick={handleConfirm}
          >
            {submitting
              ? 'Adding…'
              : `${isSaved ? 'Add' : 'Save & add'}${selected.size ? ` (${selected.size})` : ''}`}
          </Button>
        </div>
      </PopoverContent>
    </Popover>
  )
}
