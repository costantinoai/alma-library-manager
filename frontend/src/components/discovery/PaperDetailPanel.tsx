import { useEffect, useState } from 'react'
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query'
import {
  ChevronDown,
  Edit3,
  ExternalLink,
  FileText,
  Loader2,
  MoreHorizontal,
  RefreshCw,
  Star,
  Trash2,
} from 'lucide-react'

import { Button } from '@/components/ui/button'
import { Collapsible, CollapsibleContent, CollapsibleTrigger } from '@/components/ui/collapsible'
import { Dialog, DialogContent, DialogFooter, DialogHeader, DialogTitle } from '@/components/ui/dialog'
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
import {
  DropdownMenu,
  DropdownMenuContent,
  DropdownMenuItem,
  DropdownMenuSeparator,
  DropdownMenuTrigger,
} from '@/components/ui/dropdown-menu'
import { EyebrowLabel } from '@/components/ui/eyebrow-label'
import { Input } from '@/components/ui/input'
import { Label } from '@/components/ui/label'
import { StatusBadge } from '@/components/ui/status-badge'
import { Textarea } from '@/components/ui/textarea'
import {
  api,
  getDerivativeWorks,
  getPriorWorks,
  onlineImportSave,
  removeFromLibrary,
  resolveImportedPublicationsOpenAlex,
  updateReadingStatus,
  updateSavedPaper,
  type Publication,
  type RelatedWork,
} from '@/api/client'
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from '@/components/ui/select'
import type { PaperReaction } from '@/components/discovery/PaperActionBar'
import { PaperCard, type PaperCardPaper } from '@/components/shared'
import { AuthorHoverCard } from '@/components/authors/AuthorHoverCard'
import { errorToast, useToast } from '@/hooks/useToast'
import { navigateTo } from '@/lib/hashRoute'
import { invalidateQueries } from '@/lib/queryHelpers'
import { cn, formatPublicationDate, truncate } from '@/lib/utils'

interface PaperTopic {
  term: string
  score?: number | null
  domain?: string | null
  field?: string | null
  subfield?: string | null
  topic_id?: string | null
}

interface PaperDetails extends Publication {
  topics?: PaperTopic[] | null
  is_retracted?: boolean
  referenced_works_count?: number
}

interface PaperDetailPanelProps {
  paper: Publication | null
  open: boolean
  onOpenChange: (open: boolean) => void
}

/**
 * Paper details primitive.
 *
 * Every surface that renders a paper card (Feed, Discovery, Library) opens
 * this dialog on row click. When the dialog opens it lazily pulls the full
 * paper record from `/api/v1/papers/{id}/details` (which includes the
 * semantic topics from `publication_topics`). If the fetch fails or 404s,
 * the dialog still renders the fields already present on the `Publication`
 * handed in by the caller, so the popup never goes blank.
 *
 * Notes are editable inline for papers with `status='library'` — per D5 the
 * organization primitives (notes, ratings, tags, collections) are scoped to
 * saved Library papers. Candidates render notes read-only when present.
 */
export function PaperDetailPanel({ paper, open, onOpenChange }: PaperDetailPanelProps) {
  const [details, setDetails] = useState<PaperDetails | null>(null)
  const [loading, setLoading] = useState(false)
  const [notesDraft, setNotesDraft] = useState('')
  const [priorOpen, setPriorOpen] = useState(false)
  const [derivativeOpen, setDerivativeOpen] = useState(false)
  const queryClient = useQueryClient()
  const { toast } = useToast()

  // T6: lazy-fetch prior / derivative works only when their collapsible
  // is expanded. First expand triggers a network call; tanstack caches
  // the result for the rest of the dialog's lifetime (default 5 min
  // stale time). Close + reopen of the collapsible doesn't re-fetch.
  const priorWorksQuery = useQuery({
    queryKey: ['paper-prior-works', paper?.id],
    queryFn: () => getPriorWorks(paper!.id, 30),
    enabled: !!paper?.id && open && priorOpen,
    staleTime: 5 * 60_000,
  })
  const derivativeWorksQuery = useQuery({
    queryKey: ['paper-derivative-works', paper?.id],
    queryFn: () => getDerivativeWorks(paper!.id, 30),
    enabled: !!paper?.id && open && derivativeOpen,
    staleTime: 5 * 60_000,
  })

  // Reset collapsed state + cancel fetches when the dialog closes or
  // switches to a different paper. Prevents stale "Extended by" counts
  // from flashing when the user hops between papers.
  useEffect(() => {
    if (!open) {
      setPriorOpen(false)
      setDerivativeOpen(false)
    }
  }, [open, paper?.id])

  useEffect(() => {
    if (!open || !paper?.id) {
      setDetails(null)
      return
    }
    let cancelled = false
    setLoading(true)
    api
      .get<PaperDetails>(`/papers/${encodeURIComponent(paper.id)}/details`)
      .then((data) => { if (!cancelled) setDetails(data) })
      .catch(() => { if (!cancelled) setDetails(null) })
      .finally(() => { if (!cancelled) setLoading(false) })
    return () => { cancelled = true }
  }, [open, paper?.id])

  // Prefer the freshly fetched record; fall back to the caller-provided one.
  const p: PaperDetails | null = details ?? (paper as PaperDetails | null)

  // Sync the draft whenever the source record changes. The draft is the
  // single source of truth while the dialog is open — saves flow back into
  // `details` so a re-open stays in sync without a refetch.
  useEffect(() => {
    setNotesDraft(p?.notes ?? '')
  }, [p?.id, p?.notes])

  const doiUrl = p?.doi ? `https://doi.org/${p.doi.replace(/^https?:\/\/doi\.org\//, '')}` : null
  const publishedLabel = formatPublicationDate(p) || null
  const topics = (details?.topics ?? []).filter((t) => t && t.term)
  const keywords = Array.isArray(p?.keywords) ? (p?.keywords as string[]).filter(Boolean) : []
  const isLibraryPaper = p?.status === 'library'
  const hasUnsavedNotes = (p?.notes ?? '') !== notesDraft

  const notesMutation = useMutation({
    mutationFn: ({ paperId, notes }: { paperId: string; notes: string }) =>
      updateSavedPaper(paperId, { notes }),
    onSuccess: (updated, vars) => {
      setDetails((prev) => (prev ? { ...prev, notes: vars.notes } : prev))
      invalidateQueries(queryClient, ['likes'], ['papers'], ['library-workflow'])
      toast({ title: 'Notes saved', description: 'Your notes have been updated.' })
    },
    onError: () => {
      toast({
        title: 'Failed to save notes',
        description: 'Check your connection and try again.',
        variant: 'destructive',
      })
    },
  })

  // ── ... advanced menu state ─────────────────────────────────────
  // Soft edit + add-abstract share one inline editor; the submenu
  // command flag says which fields are visible. Soft remove + re-fetch
  // each have their own AlertDialog confirm.
  const [editMode, setEditMode] = useState<null | 'soft' | 'abstract'>(null)
  const [editTitle, setEditTitle] = useState('')
  const [editAuthors, setEditAuthors] = useState('')
  const [editAbstract, setEditAbstract] = useState('')
  const [removeConfirmOpen, setRemoveConfirmOpen] = useState(false)
  const [refetchConfirmOpen, setRefetchConfirmOpen] = useState(false)

  const openSoftEdit = () => {
    setEditTitle(p?.title ?? '')
    setEditAuthors(p?.authors ?? '')
    setEditAbstract(p?.abstract ?? '')
    setEditMode('soft')
  }
  const openAddAbstract = () => {
    setEditAbstract(p?.abstract ?? '')
    setEditMode('abstract')
  }

  const editMutation = useMutation({
    mutationFn: (body: { title?: string; authors?: string; abstract?: string }) =>
      updateSavedPaper(p!.id, body),
    onSuccess: (updated) => {
      setDetails((prev) => (prev ? { ...prev, ...updated } as PaperDetails : updated as PaperDetails))
      invalidateQueries(queryClient, ['papers'], ['library-saved'], ['library-workflow'])
      toast({ title: 'Paper updated' })
      setEditMode(null)
    },
    onError: () => errorToast('Error', 'Could not save edits.'),
  })

  const removeMutation = useMutation({
    mutationFn: () => removeFromLibrary(p!.id),
    onSuccess: () => {
      invalidateQueries(
        queryClient,
        ['papers'],
        ['library-saved'],
        ['library-workflow'],
        ['library-info'],
      )
      toast({ title: 'Removed', description: 'Paper soft-removed from Library.' })
      setRemoveConfirmOpen(false)
      onOpenChange(false)
    },
    onError: () => errorToast('Error', 'Could not remove paper.'),
  })

  const refetchMutation = useMutation({
    // Re-runs OpenAlex enrichment for this single paper. We pass the
    // explicit `items` list so the backend re-resolves regardless of
    // the row's current `openalex_resolution_status` (the
    // `unresolved_only=false` flag means "yes, even if you already
    // think this is resolved").
    mutationFn: () =>
      resolveImportedPublicationsOpenAlex({
        items: [{ paper_id: p!.id }],
        unresolved_only: false,
        background: false,
      }),
    onSuccess: () => {
      invalidateQueries(queryClient, ['papers'], ['library-saved'], ['library-workflow'])
      toast({
        title: 'Re-fetch queued',
        description: 'OpenAlex enrichment will run for this paper.',
      })
      setRefetchConfirmOpen(false)
    },
    onError: () => errorToast('Error', 'Could not queue re-fetch.'),
  })

  return (
    <Dialog open={open} onOpenChange={onOpenChange}>
      <DialogContent className="max-w-3xl">
        <DialogHeader>
          <div className="flex items-start justify-between gap-3 pr-12">
            <DialogTitle className="text-base leading-snug">
              {p?.title || 'Paper details'}
            </DialogTitle>
            {/* Advanced ... menu — sits next to the title (with pr-12
                to clear the Dialog primitive's auto X close button).
                All four actions only make sense for an actual paper
                row, so the trigger is hidden when `p` is null. */}
            {p ? (
              <DropdownMenu>
                <DropdownMenuTrigger asChild>
                  <Button
                    type="button"
                    size="icon-sm"
                    variant="ghost"
                    aria-label="More actions"
                    className="shrink-0 text-slate-500 hover:text-alma-700"
                  >
                    <MoreHorizontal className="h-4 w-4" />
                  </Button>
                </DropdownMenuTrigger>
                <DropdownMenuContent align="end" className="w-56">
                  <DropdownMenuItem onClick={openSoftEdit} disabled={!isLibraryPaper}>
                    <Edit3 className="mr-2 h-4 w-4" />
                    Edit (title / authors / abstract)
                  </DropdownMenuItem>
                  <DropdownMenuItem onClick={openAddAbstract} disabled={!isLibraryPaper}>
                    <FileText className="mr-2 h-4 w-4" />
                    {p.abstract ? 'Edit abstract' : 'Add abstract'}
                  </DropdownMenuItem>
                  <DropdownMenuSeparator />
                  <DropdownMenuItem onClick={() => setRefetchConfirmOpen(true)}>
                    <RefreshCw className="mr-2 h-4 w-4" />
                    Re-fetch metadata
                  </DropdownMenuItem>
                  <DropdownMenuSeparator />
                  <DropdownMenuItem
                    onClick={() => setRemoveConfirmOpen(true)}
                    disabled={!isLibraryPaper}
                    className="text-rose-700 focus:bg-rose-50 focus:text-rose-800"
                  >
                    <Trash2 className="mr-2 h-4 w-4" />
                    Remove from Library
                  </DropdownMenuItem>
                </DropdownMenuContent>
              </DropdownMenu>
            ) : null}
          </div>
        </DialogHeader>
        {p ? (
          <div className="max-h-[75vh] space-y-4 overflow-y-auto pr-1 text-sm">
            {p.authors && (
              // Each author name is a clickable button wrapped in
              // AuthorHoverCard — same primitive PaperCard uses in
              // Library / Feed / Discovery rows. Click navigates to
               // the Authors page filtered by the name; hover shows
              // the dossier (h-index, citations, top topics) with a
              // Follow button. Names parsed by splitting on commas;
              // OpenAlex's canonical separator.
              <div className="flex flex-wrap gap-x-1 gap-y-0.5 text-slate-700">
                {p.authors
                  .split(',')
                  .map((n) => n.trim())
                  .filter(Boolean)
                  .map((name, idx, arr) => (
                    <span key={`${name}-${idx}`} className="inline-flex items-center">
                      <AuthorHoverCard name={name}>
                        <a
                          href={`#/authors?q=${encodeURIComponent(name)}`}
                          className="rounded-sm px-0.5 transition-colors hover:bg-alma-folio/10 hover:text-alma-folio"
                          onClick={(e) => e.stopPropagation()}
                        >
                          {name}
                        </a>
                      </AuthorHoverCard>
                      {idx < arr.length - 1 && <span className="text-slate-400">,</span>}
                    </span>
                  ))}
              </div>
            )}

            {/* Venue + publishing facets */}
            <div className="flex flex-wrap items-center gap-2">
              {/* Metadata bubbles all ride the brand Folio-blue
                  translucent (`tone="info"`) so the meta strip reads
                  as one calm row of evidence. The only exception is
                  `Retracted` — that's a true alarm, not metadata,
                  and stays on the deep-rose `negative` tone so it
                  can't be mistaken for ordinary chrome. */}
              {p.journal && <StatusBadge tone="info">{p.journal}</StatusBadge>}
              {publishedLabel && (
                <StatusBadge tone="info">Published {publishedLabel}</StatusBadge>
              )}
              {p.cited_by_count != null && p.cited_by_count > 0 && (
                <StatusBadge tone="info">
                  {p.cited_by_count.toLocaleString()} citations
                </StatusBadge>
              )}
              {p.fwci != null && (
                <StatusBadge tone="info" title="Field-Weighted Citation Impact">
                  FWCI {p.fwci.toFixed(2)}
                </StatusBadge>
              )}
              {p.work_type && <StatusBadge tone="info">{p.work_type}</StatusBadge>}
              {p.language && <StatusBadge tone="info">{p.language.toUpperCase()}</StatusBadge>}
              {p.is_oa && (
                <StatusBadge tone="info">
                  Open access{p.oa_status ? ` · ${p.oa_status}` : ''}
                </StatusBadge>
              )}
              {p.is_retracted && (
                <StatusBadge tone="negative">Retracted</StatusBadge>
              )}
              {p.status && (
                <StatusBadge tone="info" className="capitalize">{p.status}</StatusBadge>
              )}
            </div>

            {/* External links */}
            <div className="flex flex-wrap items-center gap-3 text-xs">
              {p.url && (
                <a
                  href={p.url}
                  target="_blank"
                  rel="noopener noreferrer"
                  className="inline-flex items-center gap-1 text-alma-700 hover:text-alma-800 hover:underline"
                >
                  <ExternalLink className="h-3 w-3" />
                  Open source
                </a>
              )}
              {p.oa_url && p.oa_url !== p.url && (
                <a
                  href={p.oa_url}
                  target="_blank"
                  rel="noopener noreferrer"
                  className="inline-flex items-center gap-1 text-alma-700 hover:text-alma-800 hover:underline"
                >
                  <ExternalLink className="h-3 w-3" />
                  Open access copy
                </a>
              )}
              {doiUrl && (
                <a
                  href={doiUrl}
                  target="_blank"
                  rel="noopener noreferrer"
                  className="inline-flex items-center gap-1 text-alma-700 hover:text-alma-800 hover:underline"
                >
                  <ExternalLink className="h-3 w-3" />
                  DOI: {p.doi}
                </a>
              )}
              {p.openalex_id && (
                <span className="text-slate-500">OpenAlex: {p.openalex_id}</span>
              )}
              {p.referenced_works_count != null && p.referenced_works_count > 0 && (
                <span className="text-slate-500">{p.referenced_works_count} references</span>
              )}
            </div>

            {/* Abstract */}
            <section>
              <EyebrowLabel tone="muted" className="mb-1 inline-flex items-center gap-2">
                Abstract
                {loading && <Loader2 className="h-3 w-3 animate-spin text-slate-400" />}
              </EyebrowLabel>
              {p.abstract ? (
                // Abstract block uses the warm cream "content" tone
                // (alma-content #FDFBF3) so it reads as paper-with-
                // ink-on-it against the cooler chrome-elev dialog
                // body. Distinct surface signals "this is the work
                // itself" vs the chrome around it.
                <div className="max-h-96 overflow-auto whitespace-pre-wrap rounded-md border border-[var(--color-border)] bg-alma-content p-4 leading-relaxed text-alma-900">
                  {p.abstract.length > 200 && (
                    <span
                      className="float-left mr-2 mt-1 font-brand text-[44px] font-semibold leading-none text-alma-800"
                      aria-hidden
                    >
                      {p.abstract[0]}
                    </span>
                  )}
                  {p.abstract.length > 200 ? p.abstract.slice(1) : p.abstract}
                </div>
              ) : (
                <div className="rounded-md border border-dashed border-[var(--color-border)] p-3 text-xs italic text-slate-400">
                  No abstract available for this paper.
                </div>
              )}
            </section>

            {/* T6 — Prior Works ("Builds on") */}
            <RelatedWorksSection
              direction="prior"
              open={priorOpen}
              onOpenChange={setPriorOpen}
              onPaperClick={(childId, childTitle) => {
                onOpenChange(false)
                navigateTo('discovery', { seed: childId, seedTitle: childTitle })
              }}
              isLoading={priorWorksQuery.isLoading}
              works={priorWorksQuery.data?.works ?? []}
              localCount={priorWorksQuery.data?.local_count ?? 0}
            />

            {/* T6 — Derivative Works ("Extended by") */}
            <RelatedWorksSection
              direction="derivative"
              open={derivativeOpen}
              onOpenChange={setDerivativeOpen}
              onPaperClick={(childId, childTitle) => {
                onOpenChange(false)
                navigateTo('discovery', { seed: childId, seedTitle: childTitle })
              }}
              isLoading={derivativeWorksQuery.isLoading}
              works={derivativeWorksQuery.data?.works ?? []}
              localCount={derivativeWorksQuery.data?.local_count ?? 0}
            />

            {/* Semantic topics (OpenAlex) */}
            {topics.length > 0 && (
              <section>
                <EyebrowLabel tone="muted" className="mb-1">Topics</EyebrowLabel>
                <div className="flex flex-wrap gap-1.5">
                  {topics.map((t) => {
                    const hierarchy = [t.domain, t.field, t.subfield].filter(Boolean).join(' › ')
                    const title = hierarchy
                      ? `${hierarchy}${t.score != null ? ` · score ${Number(t.score).toFixed(2)}` : ''}`
                      : t.term
                    return (
                      <StatusBadge
                        key={`${t.term}-${t.topic_id ?? ''}`}
                        tone="info"
                        size="sm"
                        title={title}
                      >
                        {t.term}
                        {t.score != null && (
                          <span className="ml-1 text-[10px] tabular-nums opacity-70">
                            {(Number(t.score) * 100).toFixed(0)}
                          </span>
                        )}
                      </StatusBadge>
                    )
                  })}
                </div>
              </section>
            )}

            {/* Keywords (OpenAlex) */}
            {keywords.length > 0 && (
              <section>
                <EyebrowLabel tone="muted" className="mb-1">Keywords</EyebrowLabel>
                <div className="flex flex-wrap gap-1.5">
                  {keywords.map((kw) => (
                    <StatusBadge key={kw} tone="info" size="sm">{kw}</StatusBadge>
                  ))}
                </div>
              </section>
            )}

            {/* Notes — editable for Library papers, read-only elsewhere */}
            {isLibraryPaper ? (
              <section>
                <div className="mb-1 flex items-center justify-between">
                  <EyebrowLabel tone="muted">Notes</EyebrowLabel>
                  {hasUnsavedNotes && <span className="text-[10px] font-normal italic text-amber-600">Unsaved changes</span>}
                </div>
                <Textarea
                  value={notesDraft}
                  onChange={(e) => setNotesDraft(e.target.value)}
                  placeholder="Add personal notes about this paper…"
                  rows={4}
                  // Override the default warm Textarea bg with a cool
                  // grayish off-white so the notes box reads as a
                  // neutral input field, distinct from the warm paper
                  // surfaces around it.
                  className="bg-slate-50 text-sm text-slate-700"
                />
                <div className="mt-2 flex items-center justify-end gap-2">
                  <Button
                    variant="ghost"
                    size="sm"
                    onClick={() => setNotesDraft(p.notes ?? '')}
                    disabled={!hasUnsavedNotes || notesMutation.isPending}
                  >
                    Reset
                  </Button>
                  <Button
                    size="sm"
                    onClick={() => notesMutation.mutate({ paperId: p.id, notes: notesDraft })}
                    disabled={!hasUnsavedNotes || notesMutation.isPending}
                  >
                    {notesMutation.isPending && <Loader2 className="h-3 w-3 animate-spin" />}
                    Save notes
                  </Button>
                </div>
              </section>
            ) : (
              p.notes && (
                <section>
                  <div className="mb-1 text-[11px] font-semibold uppercase tracking-wide text-slate-500">
                    Notes
                  </div>
                  <div className="rounded-md border border-[var(--color-border)] bg-[#FFFEF7] p-3 text-slate-700 whitespace-pre-wrap">
                    {p.notes}
                  </div>
                </section>
              )
            )}
          </div>
        ) : (
          <div className="text-sm text-slate-500">No paper selected.</div>
        )}
      </DialogContent>

      {/* Soft-edit / add-abstract sub-dialog. Shares one inline editor;
          `editMode` says which field set is visible. The library
          backend's PUT /library/saved/{id} accepts any subset of
          {title, authors, abstract, notes, rating} so partial edits
          don't clobber sibling fields. */}
      <Dialog open={editMode !== null} onOpenChange={(o) => !o && setEditMode(null)}>
        <DialogContent>
          <DialogHeader>
            <DialogTitle>
              {editMode === 'abstract' ? 'Add / edit abstract' : 'Edit paper metadata'}
            </DialogTitle>
          </DialogHeader>
          <div className="space-y-3">
            {editMode === 'soft' ? (
              <>
                <div className="space-y-1">
                  <Label htmlFor="paper-edit-title">Title</Label>
                  <Input
                    id="paper-edit-title"
                    value={editTitle}
                    onChange={(e) => setEditTitle(e.target.value)}
                  />
                </div>
                <div className="space-y-1">
                  <Label htmlFor="paper-edit-authors">Authors</Label>
                  <Input
                    id="paper-edit-authors"
                    value={editAuthors}
                    onChange={(e) => setEditAuthors(e.target.value)}
                    placeholder="e.g. Smith J, Doe A, Lee K"
                  />
                </div>
                <div className="space-y-1">
                  <Label htmlFor="paper-edit-abstract">Abstract</Label>
                  <Textarea
                    id="paper-edit-abstract"
                    rows={6}
                    value={editAbstract}
                    onChange={(e) => setEditAbstract(e.target.value)}
                    className="bg-slate-50 text-sm text-slate-700"
                  />
                </div>
              </>
            ) : (
              <div className="space-y-1">
                <Label htmlFor="paper-edit-abstract-only">Abstract</Label>
                <Textarea
                  id="paper-edit-abstract-only"
                  rows={10}
                  value={editAbstract}
                  onChange={(e) => setEditAbstract(e.target.value)}
                  placeholder="Paste the paper's abstract here…"
                  className="bg-slate-50 text-sm text-slate-700"
                />
              </div>
            )}
          </div>
          <DialogFooter>
            <Button variant="ghost" onClick={() => setEditMode(null)} disabled={editMutation.isPending}>
              Cancel
            </Button>
            <Button
              onClick={() => {
                if (editMode === 'abstract') {
                  editMutation.mutate({ abstract: editAbstract })
                } else {
                  editMutation.mutate({
                    title: editTitle,
                    authors: editAuthors,
                    abstract: editAbstract,
                  })
                }
              }}
              disabled={editMutation.isPending}
            >
              {editMutation.isPending && <Loader2 className="mr-1 h-3.5 w-3.5 animate-spin" />}
              Save
            </Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>

      {/* Soft-remove confirm — D3 lifecycle: row stays in `papers` with
          status='removed' so Discovery can read it as a negative
          signal. The user-facing copy says "Remove" rather than
          "Delete" because nothing is hard-deleted. */}
      <AlertDialog open={removeConfirmOpen} onOpenChange={setRemoveConfirmOpen}>
        <AlertDialogContent>
          <AlertDialogHeader>
            <AlertDialogTitle>Remove this paper from your Library?</AlertDialogTitle>
            <AlertDialogDescription>
              The paper stays in the corpus (Discovery uses it as a negative signal —
              it won't be re-suggested). You can re-add it from the Corpus explorer
              at any time. Notes, rating, and tags will be preserved.
            </AlertDialogDescription>
          </AlertDialogHeader>
          <AlertDialogFooter>
            <AlertDialogCancel disabled={removeMutation.isPending}>Cancel</AlertDialogCancel>
            <AlertDialogAction
              onClick={(e) => {
                e.preventDefault()
                removeMutation.mutate()
              }}
              className="bg-rose-600 text-white hover:bg-rose-700"
              disabled={removeMutation.isPending}
            >
              {removeMutation.isPending && <Loader2 className="mr-1 h-3.5 w-3.5 animate-spin" />}
              Remove
            </AlertDialogAction>
          </AlertDialogFooter>
        </AlertDialogContent>
      </AlertDialog>

      {/* Re-fetch confirm — re-runs the OpenAlex enrichment pipeline
          for this single paper. Uses the existing
          /library/import/resolve-openalex endpoint with an explicit
          items list + unresolved_only=false so the row gets re-resolved
          regardless of its current status. */}
      <AlertDialog open={refetchConfirmOpen} onOpenChange={setRefetchConfirmOpen}>
        <AlertDialogContent>
          <AlertDialogHeader>
            <AlertDialogTitle>Re-fetch metadata from OpenAlex?</AlertDialogTitle>
            <AlertDialogDescription>
              Re-runs the OpenAlex enrichment pipeline for this paper.
              Title, authors, abstract, journal, and OpenAlex ID will
              be overwritten with the latest from OpenAlex if the
              lookup succeeds. Manual edits via "Edit" will be lost.
            </AlertDialogDescription>
          </AlertDialogHeader>
          <AlertDialogFooter>
            <AlertDialogCancel disabled={refetchMutation.isPending}>Cancel</AlertDialogCancel>
            <AlertDialogAction
              onClick={(e) => {
                e.preventDefault()
                refetchMutation.mutate()
              }}
              disabled={refetchMutation.isPending}
            >
              {refetchMutation.isPending && <Loader2 className="mr-1 h-3.5 w-3.5 animate-spin" />}
              Re-fetch
            </AlertDialogAction>
          </AlertDialogFooter>
        </AlertDialogContent>
      </AlertDialog>
    </Dialog>
  )
}


// ============================================================================
// T6 — Prior / Derivative Works section
// ============================================================================

/**
 * One collapsible section rendering either:
 *   - `direction="prior"` → "Builds on" (papers referenced by this one)
 *   - `direction="derivative"` → "Extended by" (papers citing this one)
 *
 * Collapsed by default so the dialog stays fast to open; first expand
 * triggers the backend fetch (lazy-loaded via tanstack's `enabled`
 * flag on the caller's `useQuery`).
 */
function RelatedWorksSection({
  direction,
  open,
  onOpenChange,
  onPaperClick,
  isLoading,
  works,
  localCount,
}: {
  direction: 'prior' | 'derivative'
  open: boolean
  onOpenChange: (value: boolean) => void
  onPaperClick: (paperId: string, title: string) => void
  isLoading: boolean
  works: RelatedWork[]
  localCount: number
}) {
  const heading = direction === 'prior' ? 'Builds on' : 'Extended by'
  const sublabel = direction === 'prior'
    ? 'Papers this one cites (from your corpus)'
    : 'Papers in your corpus that cite this one'
  const emptyCopy = direction === 'prior'
    ? 'No referenced papers from this one are in your corpus yet. Refresh the lens that surfaced it, or import references manually.'
    : 'Nothing in your corpus cites this paper yet. Papers saved or tracked later may fill this in.'
  const count = works.length

  return (
    <Collapsible open={open} onOpenChange={onOpenChange}>
      <CollapsibleTrigger asChild>
        <button
          type="button"
          className="group/rel flex w-full items-center justify-between gap-2 rounded-md border border-[var(--color-border)] bg-[#FFFEF7] px-3 py-2 text-left transition hover:bg-[#FFFEF7]/70"
        >
          <div className="flex min-w-0 items-center gap-2">
            <span className="text-[11px] font-semibold uppercase tracking-wide text-slate-500">
              {heading}
            </span>
            {(isLoading || count > 0 || localCount > 0) && (
              <StatusBadge tone="info">
                {isLoading
                  ? 'loading…'
                  : count > 0
                    ? String(count)
                    : String(localCount)}
              </StatusBadge>
            )}
            <span className="truncate text-[11px] text-slate-400">{sublabel}</span>
          </div>
          <ChevronDown
            className={cn(
              'h-4 w-4 shrink-0 text-slate-400 transition-transform duration-150',
              open && 'rotate-180',
            )}
          />
        </button>
      </CollapsibleTrigger>
      <CollapsibleContent className="mt-2 space-y-2">
        {isLoading ? (
          <div className="flex items-center justify-center py-4 text-xs text-slate-400">
            <Loader2 className="mr-2 h-3 w-3 animate-spin" /> Loading…
          </div>
        ) : works.length === 0 ? (
          <div className="rounded-md border border-dashed border-slate-200 px-3 py-3 text-xs italic text-slate-400">
            {emptyCopy}
          </div>
        ) : (
          works.map((work, idx) => (
            <RelatedWorkRow
              key={work.paper_id || `${direction}-${idx}`}
              work={work}
              direction={direction}
              onPivotClick={onPaperClick}
            />
          ))
        )}
      </CollapsibleContent>
    </Collapsible>
  )
}


/**
 * Single row in a related-works section. Renders the canonical
 * `<PaperCard size="compact">` so Builds-on / Extended-by reads in
 * the exact same shape and tone as Library / Feed / Discovery rows.
 * Click-to-open opens the parent dialog on this paper; the brand
 * Discover-similar Compass icon (PaperCard's `onPivot` slot) takes
 * over what this surface used to call "Pivot" — the word is gone.
 *
 * Action contract is the canonical Add / Like / Love / Dislike
 * (3/4/5/1) wired through `onlineImportSave` — same path Find &
 * Add uses, which means non-local rows (only an OpenAlex ID +
 * title) get auto-resolved + saved on first action without a
 * separate "import then save" step. Local rows route through the
 * same endpoint; the backend dedupes by openalex_id / paper_id.
 *
 * Local rows additionally get the reading-status select via
 * PaperCard's `quickActions` slot (parity with Library landing /
 * AuthorDetailPanel publications tab).
 */
function RelatedWorkRow({
  work,
  direction,
  onPivotClick,
}: {
  work: RelatedWork
  direction: 'prior' | 'derivative'
  onPivotClick: (paperId: string, title: string) => void
}) {
  const queryClient = useQueryClient()
  const { toast } = useToast()
  // Per-row reaction + saved state. The backend response from
  // onlineImportSave is authoritative — we mirror its `status`
  // (library vs other) and the reaction from the action that fired.
  const ratingToReaction = (rating?: number | null): PaperReaction => {
    if (rating === 4) return 'like'
    if (rating === 5) return 'love'
    if (rating === 1) return 'dislike'
    return null
  }
  const [reaction, setReaction] = useState<PaperReaction>(ratingToReaction(work.rating))
  const [isSaved, setIsSaved] = useState<boolean>(!!work.paper_id && work.rating != null)
  const [readingStatus, setReadingStatus] = useState<string>('__none__')
  const hasLocal = !!work.paper_id
  // Project the trimmed RelatedWork shape onto the canonical
  // PaperCardPaper shape. Use the RelatedWork's openalex_id as the
  // card id when there's no local row, so the key stays stable.
  const cardPaper: PaperCardPaper = {
    id: work.paper_id || `related-${work.title}`,
    title: work.title,
    authors: work.authors ?? '',
    year: work.year ?? null,
    journal: work.journal ?? undefined,
    doi: work.doi ?? undefined,
    url: work.url ?? undefined,
    cited_by_count: work.cited_by_count ?? 0,
    influential_citation_count: work.influential_citation_count ?? 0,
    rating: work.rating ?? undefined,
    tldr: work.tldr ?? null,
    status: isSaved ? 'library' : undefined,
  }

  const actionMutation = useMutation({
    mutationFn: (action: 'add' | 'like' | 'love' | 'dislike') =>
      onlineImportSave({
        action,
        openalex_id: (work as { openalex_id?: string }).openalex_id ?? undefined,
        doi: work.doi ?? undefined,
        title: work.title,
      }),
    onSuccess: (resp, action) => {
      setIsSaved(resp.status === 'library')
      setReaction(action === 'add' ? null : action)
      invalidateQueries(
        queryClient,
        ['papers'],
        ['library-saved'],
        ['library-workflow'],
        ['paper-prior-works'],
        ['paper-derivative-works'],
      )
      toast({
        title:
          action === 'dislike'
            ? resp.status === 'library'
              ? 'Signal recorded'
              : 'Dismissed'
            : 'Saved to Library',
        description: resp.title || work.title,
      })
    },
    onError: () => errorToast('Error', 'Action failed'),
  })

  const readingMutation = useMutation({
    mutationFn: (status: string) =>
      updateReadingStatus(
        work.paper_id!,
        status === '__none__' ? null : (status as 'reading' | 'done' | 'excluded'),
      ),
    onSuccess: () => {
      invalidateQueries(queryClient, ['papers'], ['library-saved'], ['library-workflow'])
    },
    onError: () => errorToast('Error', 'Could not update reading status'),
  })

  const readingStatusSlot = hasLocal ? (
    <Select
      value={readingStatus}
      onValueChange={(v) => {
        setReadingStatus(v)
        readingMutation.mutate(v)
      }}
    >
      <SelectTrigger className="h-7 w-[120px] text-xs">
        <SelectValue placeholder="Reading" />
      </SelectTrigger>
      <SelectContent>
        <SelectItem value="__none__">Not on list</SelectItem>
        <SelectItem value="reading">Reading</SelectItem>
        <SelectItem value="done">Done</SelectItem>
        <SelectItem value="excluded">Excluded</SelectItem>
      </SelectContent>
    </Select>
  ) : undefined

  return (
    <PaperCard
      paper={cardPaper}
      size="compact"
      // Lighter shade than the dialog body — matches the "more
      // forefront = lighter" rule (the dialog body now sits on warm
      // parchment, the cards inside lift to off-white).
      className="bg-[#FFFEF7]"
      isSaved={isSaved}
      reaction={reaction}
      actionDisabled={actionMutation.isPending}
      onAdd={() => actionMutation.mutate('add')}
      onLike={() => actionMutation.mutate('like')}
      onLove={() => actionMutation.mutate('love')}
      onDislike={() => actionMutation.mutate('dislike')}
      readingStatusSlot={readingStatusSlot}
      // Click-to-open re-seeds the parent dialog with this paper
      // (only when the row is in the local corpus — non-local
      // rows have no detail page to open, so the click is no-op).
      onDetails={hasLocal ? () => onPivotClick(work.paper_id!, work.title) : undefined}
      // Discover-similar Compass action — same primitive PaperCard
      // exposes everywhere (Feed / Discovery / Library). No
      // "Pivot" word in the UI; the icon + tooltip carries the
      // affordance.
      onPivot={
        hasLocal ? () => onPivotClick(work.paper_id!, work.title) : undefined
      }
      // Influential-citation gold star is a derivative-only signal;
      // surface it via PaperCard's trailingHeader slot so it sits at
      // the top-right of the card instead of inline with the title.
      trailingHeader={
        direction === 'derivative' && work.is_influential ? (
          <span
            className="inline-flex shrink-0"
            title="S2 classified this citation as influential"
            aria-label="Influential citation"
          >
            <Star className="h-3.5 w-3.5 fill-amber-400 text-amber-500" />
          </span>
        ) : undefined
      }
    />
  )
}
