import { useMemo, useState } from 'react'
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query'
import { AnimatePresence, motion, useReducedMotion } from 'framer-motion'
import { Plus, Users } from 'lucide-react'

import {
  api,
  listAuthorsNeedsAttention,
  listFollowedAuthors,
  type Author,
  type AuthorNeedsAttentionRow,
  type AuthorSuggestion,
} from '@/api/client'
import { Alert, AlertDescription } from '@/components/ui/alert'
import { Button } from '@/components/ui/button'
import { EmptyState } from '@/components/ui/empty-state'
import { Skeleton } from '@/components/ui/skeleton'
import { AuthorDetailPanel } from '@/components/AuthorDetailPanel'
import { AddAuthorDialog, type AddAuthorPayload } from '@/components/authors/AddAuthorDialog'
import { CorpusAuthorsTable } from '@/components/authors/CorpusAuthorsTable'
import { FollowedAuthorCard } from '@/components/authors/FollowedAuthorCard'
import { SuggestedAuthorsRail } from '@/components/authors/SuggestedAuthorsRail'
import {
  AuthorsNeedsAttentionSection,
  useAuthorAttentionRouter,
} from '@/components/authors/AuthorsNeedsAttentionSection'
import { invalidateQueries } from '@/lib/queryHelpers'
import { useToast, errorToast } from '@/hooks/useToast'

/**
 * Authors page — three-section product model (2026-04-23):
 *
 *   1. Suggested (top)   — 5-card rail with enter/exit animations. Reject
 *                          writes a negative signal so the author is never
 *                          re-suggested; Follow promotes into section 2.
 *   2. Followed (middle) — grid of followed-author cards with monitor
 *                          health and the shared AuthorSignalBar.
 *   3. Corpus (bottom)   — compact table of every author in the DB. Row
 *                          click opens the same detail dialog.
 *
 * Every card / row opens the shared AuthorDetailPanel dialog, which
 * bundles overview + publications + identifier-resolution into one
 * controlled popup (replaces the old inline-expansion panel).
 */
export function AuthorsPage() {
  const queryClient = useQueryClient()
  const { toast } = useToast()
  const reducedMotion = useReducedMotion()

  const [selectedAuthor, setSelectedAuthor] = useState<Author | null>(null)
  const [selectedSuggestion, setSelectedSuggestion] = useState<AuthorSuggestion | null>(null)
  const [detailOpen, setDetailOpen] = useState(false)
  const [addAuthorOpen, setAddAuthorOpen] = useState(false)

  const authorsQuery = useQuery({
    queryKey: ['authors'],
    queryFn: () => api.get<Author[]>('/authors'),
    retry: 1,
  })

  const followedAuthorsQuery = useQuery({
    queryKey: ['library-followed-authors'],
    queryFn: listFollowedAuthors,
    retry: 1,
  })

  // Hoisted from AuthorsNeedsAttentionSection so the followed-author
  // grid can mark cards whose authors are in the needs-attention list.
  // React Query dedups the cache key, so the section stays in sync
  // with no second network request.
  const needsAttentionQuery = useQuery({
    queryKey: ['authors-needs-attention'],
    queryFn: () => listAuthorsNeedsAttention(50),
    staleTime: 60_000,
  })

  const addAuthorMutation = useMutation({
    mutationFn: (payload: AddAuthorPayload) => api.post<Author>('/authors', payload),
    onSuccess: () => {
      void invalidateQueries(queryClient, ['authors'], ['library-followed-authors'])
      setAddAuthorOpen(false)
      toast({ title: 'Author added', description: 'They will contribute to Feed on the next refresh.' })
    },
    onError: () => {
      errorToast('Could not add author', 'Check the provided identifier and try again.')
    },
  })

  // Bulk identifier resolution lives in Settings → Corpus maintenance
  // (2026-04-24). One-off user flows hit the per-author resolve inside
  // AuthorDetailPanel; the old header "Resolve IDs" button was removed
  // to keep the Authors page focused on exploration + triage.

  const authors = authorsQuery.data ?? []
  const followedIds = useMemo(
    () => new Set((followedAuthorsQuery.data ?? []).map((item) => item.author_id)),
    [followedAuthorsQuery.data],
  )
  const followedAuthors = useMemo(
    () =>
      authors
        .filter((a) => followedIds.has(a.id))
        .sort((a, b) => a.name.localeCompare(b.name)),
    [authors, followedIds],
  )
  const authorsById = useMemo(() => {
    const map = new Map<string, Author>()
    for (const a of authors) map.set(a.id, a)
    return map
  }, [authors])

  const attentionRows = needsAttentionQuery.data?.items ?? []
  // Map keyed by `authors.id` so each followed-author card can render
  // its own warning triangle in O(1). Background-author rows from the
  // needs-attention list still appear in the dedicated section below
  // — they just don't have a card to decorate.
  const attentionByAuthor = useMemo(() => {
    const map = new Map<string, AuthorNeedsAttentionRow>()
    for (const row of attentionRows) map.set(row.author_id, row)
    return map
  }, [attentionRows])

  const openDetail = (author: Author) => {
    setSelectedSuggestion(null)
    setSelectedAuthor(author)
    setDetailOpen(true)
  }

  // Single shared router for the needs-attention sub-dialogs. The
  // section's row buttons AND each followed-author card's warning
  // triangle dispatch through `router.openForRow`, so dialog state
  // never duplicates and `review_candidates` / `manual_search` action
  // codes route into this page's `openDetail`.
  const attentionRouter = useAuthorAttentionRouter({
    authorsById,
    onOpenDetail: openDetail,
  })

  const openSuggestionDetail = (s: AuthorSuggestion) => {
    // If the suggestion is already backed by a local author row, open that
    // directly — full detail / publications / identifiers work.
    if (s.existing_author_id) {
      const existing = authors.find((a) => a.id === s.existing_author_id)
      if (existing) {
        openDetail(existing)
        return
      }
    }
    // Otherwise synthesize a minimal Author so the dialog header renders,
    // and pass the suggestion so the dialog can populate Overview from its
    // payload instead of trying (and failing) to fetch detail.
    const synth: Author = {
      id: s.existing_author_id ?? s.openalex_id ?? s.key,
      name: s.name,
      openalex_id: s.openalex_id ?? undefined,
      author_type: 'background',
    }
    setSelectedSuggestion(s)
    setSelectedAuthor(synth)
    setDetailOpen(true)
  }

  const isLoading = authorsQuery.isLoading || followedAuthorsQuery.isLoading
  const hasError = authorsQuery.isError || followedAuthorsQuery.isError

  return (
    <div className="space-y-8 p-6">
      <header className="flex flex-wrap items-start justify-between gap-3">
        <div>
          <h1 className="text-2xl font-semibold text-alma-800">Authors</h1>
          <p className="text-sm text-slate-500">
            Suggestions drawn from your Library, followed authors that own their Feed monitor, and
            the full corpus view.
          </p>
        </div>
        <div className="flex items-center gap-2">
          <Button size="sm" onClick={() => setAddAuthorOpen(true)}>
            <Plus className="h-4 w-4" />
            Add author
          </Button>
        </div>
      </header>

      {hasError ? (
        <Alert variant="negative">
          <AlertDescription>Could not load authors. Try reloading.</AlertDescription>
        </Alert>
      ) : null}

      <SuggestedAuthorsRail onOpenDetail={openSuggestionDetail} />

      <section className="space-y-3">
        <header className="flex items-center gap-2">
          <Users className="h-4 w-4 text-alma-600" />
          <h2 className="text-sm font-semibold text-alma-800">Followed authors</h2>
          <span className="text-xs text-slate-500">
            {followedAuthors.length} followed · monitors run on Feed refresh
          </span>
        </header>

        {isLoading ? (
          <div className="grid gap-3 md:grid-cols-2 xl:grid-cols-3">
            {Array.from({ length: 3 }).map((_, i) => (
              <Skeleton key={i} className="h-40 rounded-lg" />
            ))}
          </div>
        ) : followedAuthors.length === 0 ? (
          <EmptyState
            icon={Users}
            title="No followed authors yet."
            description="Follow a suggestion above or add an author by OpenAlex / ORCID."
          />
        ) : (
          <div className="grid gap-3 md:grid-cols-2 xl:grid-cols-3">
            <AnimatePresence mode="popLayout" initial={false}>
              {followedAuthors.map((author) => (
                <motion.div
                  key={author.id}
                  layout
                  layoutId={`author-${author.openalex_id || author.id}`}
                  initial={{ opacity: 0, scale: 0.96 }}
                  animate={{ opacity: 1, scale: 1 }}
                  exit={{ opacity: 0, scale: 0.96 }}
                  transition={{ duration: reducedMotion ? 0 : 0.25, ease: 'easeOut' }}
                >
                  <FollowedAuthorCard
                    author={author}
                    signal={null}
                    onClick={() => openDetail(author)}
                    attentionRow={attentionByAuthor.get(author.id) ?? null}
                    onAttentionClick={() => {
                      const row = attentionByAuthor.get(author.id)
                      if (row) attentionRouter.openForRow(row)
                    }}
                  />
                </motion.div>
              ))}
            </AnimatePresence>
          </div>
        )}
      </section>

      <CorpusAuthorsTable authors={authors} followedIds={followedIds} onSelect={openDetail} />

      <AuthorsNeedsAttentionSection
        rows={attentionRows}
        isLoading={needsAttentionQuery.isLoading}
        isError={needsAttentionQuery.isError}
        router={attentionRouter}
      />

      {attentionRouter.dialogs}

      <AuthorDetailPanel
        author={selectedAuthor}
        suggestion={selectedSuggestion}
        open={detailOpen}
        onOpenChange={(next) => {
          setDetailOpen(next)
          if (!next) setSelectedSuggestion(null)
        }}
        onDeleted={() => {
          void invalidateQueries(queryClient, ['authors'], ['library-followed-authors'])
        }}
      />

      <AddAuthorDialog
        open={addAuthorOpen}
        onOpenChange={setAddAuthorOpen}
        onSubmit={(payload) => addAuthorMutation.mutate(payload)}
        isPending={addAuthorMutation.isPending}
        isError={addAuthorMutation.isError}
      />
    </div>
  )
}
