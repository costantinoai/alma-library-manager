import { useState } from 'react'
import { Check, Plus, Trash2, X } from 'lucide-react'

import type { Lens } from '@/api/client'
import { Button } from '@/components/ui/button'
import { Input } from '@/components/ui/input'
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from '@/components/ui/select'
import { EyebrowLabel } from '@/components/ui/eyebrow-label'
import { EmptyState } from '@/components/ui/empty-state'
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
import { cn } from '@/lib/utils'

interface LensManagerProps {
  lenses: Lens[]
  selectedLensId: string | null
  onSelectLens: (lensId: string) => void
  onCreate: (payload: {
    name: string
    context_type: Lens['context_type']
    context_config?: Record<string, unknown>
  }) => void
  onDelete: (lensId: string) => void
}

const CONTEXT_OPTIONS: Array<{ label: string; value: Lens['context_type'] }> = [
  { label: 'Library', value: 'library_global' },
  { label: 'Collection', value: 'collection' },
  { label: 'Topic Keyword', value: 'topic_keyword' },
  { label: 'Tag', value: 'tag' },
]

/**
 * LensManager — unified lens browser. The thing that owns "which lens
 * are we currently viewing?" plus the affordances to create / rename /
 * delete a lens. Sits permanently above the recommendations list so
 * it's always clear that the results below belong to the selected
 * lens (clicking a chip dynamically respawns the recommendations,
 * branch settings, and lens diagnostics via the React Query cache key).
 *
 * Visual: chrome card with a chip row. Active lens highlighted with a
 * Folio-blue ring + check; non-active lenses use the outline button
 * style. Each chip has a small `×` on hover for delete (with a confirm
 * to avoid the v2 "click anywhere on a chip = delete" trap). A "+ New
 * lens" toggles open the inline create form.
 */
export function LensManager({
  lenses,
  selectedLensId,
  onSelectLens,
  onCreate,
  onDelete,
}: LensManagerProps) {
  const [showCreateForm, setShowCreateForm] = useState(false)
  const [name, setName] = useState('')
  const [contextType, setContextType] = useState<Lens['context_type']>('library_global')
  const [contextValue, setContextValue] = useState('')
  const [pendingDeleteLens, setPendingDeleteLens] = useState<Lens | null>(null)

  const submit = () => {
    const cleanName = name.trim()
    if (!cleanName) return
    const config: Record<string, unknown> = {}
    if (contextType === 'collection' && contextValue.trim()) config.collection_id = contextValue.trim()
    if (contextType === 'topic_keyword' && contextValue.trim()) config.keyword = contextValue.trim()
    if (contextType === 'tag' && contextValue.trim()) config.tag_id = contextValue.trim()
    onCreate({ name: cleanName, context_type: contextType, context_config: Object.keys(config).length > 0 ? config : undefined })
    setName('')
    setContextValue('')
    setShowCreateForm(false)
  }

  const cancelCreate = () => {
    setName('')
    setContextValue('')
    setShowCreateForm(false)
  }

  const handleDeleteClick = (lens: Lens) => {
    setPendingDeleteLens(lens)
  }

  const confirmDelete = () => {
    if (pendingDeleteLens) {
      onDelete(pendingDeleteLens.id)
      setPendingDeleteLens(null)
    }
  }

  return (
    <section className="space-y-3 rounded-sm border border-[var(--color-border)] bg-alma-chrome p-4 shadow-paper-sheet">
      <div className="flex flex-wrap items-center justify-between gap-3">
        <div className="flex flex-col gap-0.5">
          <EyebrowLabel tone="muted">Lenses</EyebrowLabel>
          <p className="text-xs text-slate-500">
            Pick a lens to drive the recommendations and branch settings below — switching lenses respawns everything.
          </p>
        </div>
        <Button
          type="button"
          size="sm"
          variant={showCreateForm ? 'ghost' : 'outline'}
          onClick={() => (showCreateForm ? cancelCreate() : setShowCreateForm(true))}
        >
          {showCreateForm ? (
            <>
              <X className="mr-1 h-4 w-4" /> Cancel
            </>
          ) : (
            <>
              <Plus className="mr-1 h-4 w-4" /> New lens
            </>
          )}
        </Button>
      </div>

      {lenses.length === 0 ? (
        <EmptyState
          title="No lenses yet"
          description="Create one to start context-aware discovery."
        />
      ) : (
        <div className="flex flex-wrap gap-2">
          {lenses.map((lens) => {
            const isActive = selectedLensId === lens.id
            return (
              <div
                key={lens.id}
                className={cn(
                  // Two-button chip: select on the lens-name button,
                  // delete on the trash button — never overload the
                  // whole chip click area onto delete (the v2 trap).
                  'group inline-flex items-stretch overflow-hidden rounded-sm border transition-colors',
                  isActive
                    ? 'border-alma-folio bg-alma-folio-soft'
                    : 'border-[var(--color-border)] bg-alma-chrome-elev hover:border-parchment-400',
                )}
              >
                <button
                  type="button"
                  onClick={() => onSelectLens(lens.id)}
                  className={cn(
                    'inline-flex items-center gap-1.5 px-3 py-1.5 text-sm font-medium',
                    isActive ? 'text-alma-folio' : 'text-alma-800',
                  )}
                  aria-pressed={isActive}
                  title={
                    isActive
                      ? `${lens.name} (active — refreshing this lens reloads recommendations)`
                      : `Switch to ${lens.name}`
                  }
                >
                  {isActive && <Check className="h-3.5 w-3.5" aria-hidden />}
                  {lens.name}
                </button>
                <button
                  type="button"
                  onClick={() => handleDeleteClick(lens)}
                  className={cn(
                    'inline-flex items-center justify-center border-l px-2 transition-colors',
                    isActive
                      ? 'border-alma-folio/40 text-alma-folio/70 hover:bg-rose-50 hover:text-rose-700'
                      : 'border-[var(--color-border)] text-slate-400 hover:bg-rose-50 hover:text-rose-700',
                  )}
                  aria-label={`Delete ${lens.name}`}
                  title={`Delete ${lens.name}`}
                >
                  <Trash2 className="h-3.5 w-3.5" />
                </button>
              </div>
            )
          })}
        </div>
      )}

      {showCreateForm && (
        <div className="grid gap-2 border-t border-[var(--color-border)] pt-3 md:grid-cols-[1fr_auto_1fr_auto]">
          <Input
            value={name}
            onChange={(e) => setName(e.target.value)}
            placeholder="New lens name"
            onKeyDown={(e) => {
              if (e.key === 'Enter') submit()
            }}
          />
          <Select value={contextType} onValueChange={(value) => setContextType(value as Lens['context_type'])}>
            <SelectTrigger>
              <SelectValue />
            </SelectTrigger>
            <SelectContent>
              {CONTEXT_OPTIONS.map((opt) => (
                <SelectItem key={opt.value} value={opt.value}>
                  {opt.label}
                </SelectItem>
              ))}
            </SelectContent>
          </Select>
          <Input
            value={contextValue}
            onChange={(e) => setContextValue(e.target.value)}
            placeholder="Context value (optional)"
            onKeyDown={(e) => {
              if (e.key === 'Enter') submit()
            }}
          />
          <Button type="button" size="sm" onClick={submit} disabled={!name.trim()}>
            <Plus className="mr-1 h-4 w-4" />
            Create
          </Button>
        </div>
      )}

      <AlertDialog
        open={pendingDeleteLens !== null}
        onOpenChange={(open) => {
          if (!open) setPendingDeleteLens(null)
        }}
      >
        <AlertDialogContent>
          <AlertDialogHeader>
            <AlertDialogTitle>Delete lens "{pendingDeleteLens?.name}"?</AlertDialogTitle>
            <AlertDialogDescription>
              This permanently removes the lens, its weight settings, and any
              cached recommendations attached to it. Saved papers in your
              Library are not affected.
            </AlertDialogDescription>
          </AlertDialogHeader>
          <AlertDialogFooter>
            <AlertDialogCancel>Cancel</AlertDialogCancel>
            <AlertDialogAction
              onClick={confirmDelete}
              className="bg-rose-600 text-white hover:bg-rose-700"
            >
              Delete lens
            </AlertDialogAction>
          </AlertDialogFooter>
        </AlertDialogContent>
      </AlertDialog>
    </section>
  )
}
