import { useMemo, useState, type ReactNode } from 'react'
import { useMutation, useQueryClient } from '@tanstack/react-query'
import { AlertTriangle, ArrowRight, ExternalLink, GitMerge, Loader2, RefreshCw } from 'lucide-react'

import {
  api,
  discoverAuthorAliases,
  mergeAuthorProfiles,
  resolveMergeConflict,
  setAuthorIdentifiers,
  type Author,
  type AuthorAlternateProfile,
  type AuthorNeedsAttentionRow,
  type DiscoverAliasesResponse,
} from '@/api/client'
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from '@/components/ui/dialog'
import { Button } from '@/components/ui/button'
import { EmptyState } from '@/components/ui/empty-state'
import { Input } from '@/components/ui/input'
import { Label } from '@/components/ui/label'
import { LoadingState } from '@/components/ui/LoadingState'
import { StatusBadge } from '@/components/ui/status-badge'
import { resolvedBadgeSpec } from '@/components/authors/AuthorResolvedBadge'
import { useToast, errorToast } from '@/hooks/useToast'
import { invalidateQueries } from '@/lib/queryHelpers'
import { formatTimestamp } from '@/lib/utils'

/**
 * Router returned by `useAuthorAttentionRouter`. One instance per
 * surface owns all three dialogs + the refresh mutation. Both the
 * needs-attention section and the followed-author warning triangle
 * funnel through `openForRow` so dialog state never duplicates.
 */
export interface AuthorAttentionRouter {
  /** Dispatch by `row.suggested_action.code`: opens the matching
   *  dialog, defers to `onOpenDetail` for review/manual_search, or
   *  fires the deep-refresh mutation as the default. */
  openForRow: (row: AuthorNeedsAttentionRow) => void
  /** True while the deep-refresh mutation is in flight for `authorId`. */
  isRefreshingFor: (authorId: string) => boolean
  /** Render once near the top of the consuming page so the dialogs
   *  can mount alongside other modals. */
  dialogs: ReactNode
}

interface UseAuthorAttentionRouterOpts {
  /** Map of `authors.id` → `Author` so `review_candidates` /
   *  `manual_search` can hand off to `AuthorDetailPanel`. */
  authorsById?: Map<string, Author>
  onOpenDetail?: (author: Author) => void
}

/**
 * Centralised router for needs-attention actions. Owns the three
 * sub-dialog states (`reviewRow`, `identifierRow`, `conflictRow`) and
 * the deep-refresh mutation so multiple entry points (the
 * needs-attention section AND the per-card warning triangle on
 * `FollowedAuthorCard`) dispatch through one source of truth — no
 * duplicated dialog state, no double mutation queues.
 */
export function useAuthorAttentionRouter(
  opts: UseAuthorAttentionRouterOpts = {},
): AuthorAttentionRouter {
  const queryClient = useQueryClient()
  const { toast } = useToast()
  const [reviewRow, setReviewRow] = useState<AuthorNeedsAttentionRow | null>(null)
  const [identifierRow, setIdentifierRow] = useState<AuthorNeedsAttentionRow | null>(null)
  const [conflictRow, setConflictRow] = useState<AuthorNeedsAttentionRow | null>(null)

  const refreshMutation = useMutation({
    mutationFn: (authorId: string) =>
      api.post<{ status?: string; job_id?: string }>(
        `/authors/${encodeURIComponent(authorId)}/deep-refresh`,
      ),
    onSuccess: (data, authorId) => {
      void invalidateQueries(
        queryClient,
        ['authors'],
        ['authors-needs-attention'],
        ['activity-operations'],
        ['author-detail', authorId],
      )
      toast({
        title:
          data?.status === 'already_running' ? 'Refresh already running' : 'Refresh queued',
        description: data?.job_id ? `Job ${data.job_id} will update this author.` : undefined,
      })
    },
    onError: () => errorToast('Error', 'Could not queue refresh.'),
  })

  const openForRow = (row: AuthorNeedsAttentionRow) => {
    const code = row.suggested_action.code
    if (code === 'review_profiles') {
      setReviewRow(row)
      return
    }
    if (code === 'resolve_conflict') {
      setConflictRow(row)
      return
    }
    if (code === 'review_candidates' || code === 'manual_search') {
      const author = opts.authorsById?.get(row.author_id)
      if (author && opts.onOpenDetail) opts.onOpenDetail(author)
      return
    }
    if (code === 'resolve_now') {
      setIdentifierRow(row)
      return
    }
    refreshMutation.mutate(row.author_id)
  }

  const isRefreshingFor = (authorId: string) =>
    refreshMutation.isPending && refreshMutation.variables === authorId

  const dialogs = (
    <>
      <ReviewProfilesDialog row={reviewRow} onClose={() => setReviewRow(null)} />
      <AddIdentifierDialog row={identifierRow} onClose={() => setIdentifierRow(null)} />
      <ResolveConflictDialog row={conflictRow} onClose={() => setConflictRow(null)} />
    </>
  )

  return { openForRow, isRefreshingFor, dialogs }
}

interface AuthorsNeedsAttentionSectionProps {
  rows: AuthorNeedsAttentionRow[]
  isLoading: boolean
  isError: boolean
  router: AuthorAttentionRouter
}

/**
 * Authors page "Needs attention" section.
 *
 * Surfaces authors that the automatic resolver couldn't finish — error,
 * no_match, needs_manual_review, or followed-without-openalex. Each row
 * carries a friendly reason + a single primary action button.
 *
 * Reads from the `/authors/needs-attention` endpoint, which already
 * ranks by severity (error > no_match > review > unresolved). The
 * action button dispatches either the modal detail panel (so the user
 * can review candidates manually) or the per-author deep-refresh
 * endpoint for retries.
 */
export function AuthorsNeedsAttentionSection({
  rows,
  isLoading,
  isError,
  router,
}: AuthorsNeedsAttentionSectionProps) {
  if (isLoading) {
    return (
      <section className="space-y-2">
        <SectionHeader total={null} />
        <LoadingState message="Loading needs-attention rows..." />
      </section>
    )
  }
  if (isError) {
    return (
      <section className="space-y-2">
        <SectionHeader total={null} />
        <p className="text-xs text-rose-600">
          Could not load needs-attention rows. Try reloading.
        </p>
      </section>
    )
  }
  if (!rows.length) {
    return (
      <section className="space-y-2">
        <SectionHeader total={0} />
        <EmptyState title="No authors need manual attention right now. Everything resolved 🎉" />
      </section>
    )
  }

  return (
    <section className="space-y-2">
      <SectionHeader total={rows.length} />
      <ul className="space-y-2">
        {rows.map((row) => (
          <NeedsAttentionRow
            key={row.author_id}
            row={row}
            onAction={() => router.openForRow(row)}
            isRefreshing={router.isRefreshingFor(row.author_id)}
          />
        ))}
      </ul>
    </section>
  )
}

function SectionHeader({ total }: { total: number | null }) {
  return (
    <header className="flex items-center gap-2">
      <AlertTriangle className="h-4 w-4 text-amber-600" />
      <h2 className="text-sm font-semibold text-alma-800">Needs attention</h2>
      <span className="text-xs text-slate-500">
        {total == null ? 'Checking…' : total === 0 ? 'All clear' : `${total} author${total === 1 ? '' : 's'}`}
      </span>
    </header>
  )
}

function NeedsAttentionRow({
  row,
  onAction,
  isRefreshing,
}: {
  row: AuthorNeedsAttentionRow
  onAction: () => void
  isRefreshing: boolean
}) {
  const spec = resolvedBadgeSpec(row)
  const actionCode = row.suggested_action.code

  // Icon picker for the action button — telegraph what kind of
  // dialog will open before the user clicks.
  const actionIcon =
    actionCode === 'review_profiles' ? (
      <GitMerge className="h-3.5 w-3.5" />
    ) : actionCode === 'resolve_now' ? (
      <ArrowRight className="h-3.5 w-3.5" />
    ) : actionCode === 'review_candidates' || actionCode === 'manual_search' ? (
      <ArrowRight className="h-3.5 w-3.5" />
    ) : isRefreshing ? (
      <Loader2 className="h-3.5 w-3.5 animate-spin" />
    ) : (
      <RefreshCw className="h-3.5 w-3.5" />
    )

  return (
    <li className="rounded-sm border border-[var(--color-border)] bg-white p-3 shadow-paper-sm shadow-sm hover:border-[var(--color-border)]">
      <div className="flex flex-wrap items-start justify-between gap-3">
        <div className="min-w-0 flex-1">
          <div className="flex items-center gap-2">
            <StatusBadge tone={spec.tone} size="sm">
              <spec.icon className="h-3 w-3" aria-hidden />
              <span>{spec.label}</span>
            </StatusBadge>
            <button
              type="button"
              className="truncate text-sm font-semibold text-alma-800 hover:text-alma-700"
              onClick={onAction}
              disabled={isRefreshing}
            >
              {row.author_name}
            </button>
          </div>
          {row.updated_at ? (
            <p className="mt-1 text-[10px] text-slate-400">Last seen {formatTimestamp(row.updated_at)}</p>
          ) : null}
        </div>
        <div className="flex shrink-0 items-center gap-2">
          <Button
            size="sm"
            variant="outline"
            onClick={onAction}
            disabled={isRefreshing}
            title={row.suggested_action.hint}
          >
            {actionIcon}
            {row.suggested_action.label}
          </Button>
        </div>
      </div>
      {/* Warning bubble row, lower-right — same pattern as the Library
          needs-attention list. The full reason_detail (resolver step
          + confidence + OpenAlex-id presence) lives in the chip's
          title attribute for hover-to-reveal context, keeping the
          row scannable. */}
      <div className="mt-2 flex flex-wrap justify-end gap-1.5">
        <StatusBadge
          tone="warning"
          size="sm"
          title={[row.reason_detail, row.suggested_action.hint].filter(Boolean).join(' — ')}
        >
          {row.reason}
        </StatusBadge>
      </div>
    </li>
  )
}

/**
 * Review-profiles sub-dialog. Surfaces the alt OpenAlex IDs the
 * suggestion-rail dedup pass collapsed under the same canonical
 * name, so the user can confirm "yes, same human, please merge"
 * (deferred to a follow-up when the merge endpoint lands) or
 * "actually different humans, dismiss this warning" (also deferred).
 *
 * Built on the existing Dialog primitive — no new components.
 * Each alt-profile row is a flat `<li>` with the OpenAlex ID, the
 * display name OpenAlex has on file, and an external link to the
 * profile on openalex.org for visual verification.
 */
function ReviewProfilesDialog({
  row,
  onClose,
}: {
  row: AuthorNeedsAttentionRow | null
  onClose: () => void
}) {
  const open = row !== null
  const queryClient = useQueryClient()
  const { toast } = useToast()
  const baseAlts: AuthorAlternateProfile[] = row?.alt_profiles ?? []
  // Aliases discovered via ORCID land here. Merged into the rendered
  // list with a distinct chip tone so the user can tell which alts
  // came from the local-followed cluster vs. an OpenAlex round-trip.
  const [orcidDiscovery, setOrcidDiscovery] = useState<DiscoverAliasesResponse | null>(null)

  const discoverMutation = useMutation({
    mutationFn: () => discoverAuthorAliases(row!.author_id),
    onSuccess: (data) => {
      setOrcidDiscovery(data)
      if (!data.orcid) {
        toast({
          title: 'No ORCID on OpenAlex',
          description:
            'Cannot use ORCID-based alias discovery for this author — paste an ORCID in the dossier first if you have one.',
        })
      } else if (data.aliases.length === 0) {
        toast({
          title: 'No additional aliases',
          description: `OpenAlex shows only one profile for ORCID ${data.orcid}.`,
        })
      } else {
        toast({
          title: `${data.aliases.length} alias${data.aliases.length === 1 ? '' : 'es'} found`,
          description: `Via ORCID ${data.orcid}.`,
        })
      }
    },
    onError: () => errorToast('Error', 'ORCID lookup failed.'),
  })

  // Discovered aliases that are NOT already tracked locally — those
  // can't be merged via authors.id (we have no row), but recording
  // their openalex_id will be a future enhancement (preventive
  // suppression). For now we render them as "untracked" so the user
  // sees the full picture.
  const localAltIds = new Set(baseAlts.map((a) => a.openalex_id.toLowerCase()))
  const orcidExtras = (orcidDiscovery?.aliases ?? []).filter(
    (a) => !localAltIds.has(a.openalex_id.toLowerCase()),
  )

  const mergeMutation = useMutation({
    mutationFn: () =>
      mergeAuthorProfiles(
        row!.author_id,
        baseAlts.map((a) => a.author_id),
      ),
    onSuccess: (data) => {
      invalidateQueries(
        queryClient,
        ['authors'],
        ['authors-needs-attention'],
        ['library-followed-authors'],
        ['author-suggestions'],
        ['author-detail', row!.author_id],
        ['feed-monitors'],
      )
      toast({
        title: 'Profiles merged',
        description:
          `${data.alts_processed} alt profile${data.alts_processed === 1 ? '' : 's'} ` +
          `collapsed · ${data.papers_reassigned} paper${data.papers_reassigned === 1 ? '' : 's'} reattached` +
          (data.papers_dropped_as_dup > 0
            ? ` · ${data.papers_dropped_as_dup} dropped as duplicates`
            : ''),
      })
      onClose()
    },
    onError: () => errorToast('Error', 'Merge failed.'),
  })

  return (
    <Dialog open={open} onOpenChange={(v) => !v && onClose()}>
      <DialogContent className="max-w-xl">
        <DialogHeader>
          <DialogTitle>Review duplicate profiles</DialogTitle>
          <DialogDescription>
            {row?.author_name ? (
              <>
                <span className="font-medium">{row.author_name}</span> appears under
                multiple OpenAlex IDs in your followed authors. Same human, or
                different people who happen to share a name?
              </>
            ) : null}
          </DialogDescription>
        </DialogHeader>
        <div className="space-y-2">
          {row?.openalex_id ? (
            <ProfileRow
              label="Primary"
              openalexId={row.openalex_id}
              displayName={row.author_name}
              tone="primary"
            />
          ) : null}
          {baseAlts.length === 0 && orcidExtras.length === 0 ? (
            <p className="text-xs italic text-slate-500">No alternate profiles.</p>
          ) : (
            <>
              {baseAlts.map((alt) => (
                <ProfileRow
                  key={alt.openalex_id}
                  label="Followed"
                  openalexId={alt.openalex_id}
                  displayName={alt.display_name}
                />
              ))}
              {orcidExtras.map((alt) => (
                <ProfileRow
                  key={`orcid-${alt.openalex_id}`}
                  label={`Via ORCID${alt.institution ? ` · ${alt.institution}` : ''}`}
                  openalexId={alt.openalex_id}
                  displayName={alt.display_name || alt.openalex_id}
                />
              ))}
            </>
          )}
          <div className="border-t border-[var(--color-border)] pt-2">
            <Button
              variant="ghost"
              size="sm"
              onClick={() => discoverMutation.mutate()}
              disabled={discoverMutation.isPending || !row}
              className="text-xs"
              title="Looks up the primary's ORCID on OpenAlex and queries every profile sharing it. ORCID is person-level — same ORCID = same human, with very high confidence."
            >
              {discoverMutation.isPending ? (
                <Loader2 className="h-3.5 w-3.5 animate-spin" />
              ) : (
                <GitMerge className="h-3.5 w-3.5" />
              )}
              Discover more profiles via ORCID
            </Button>
          </div>
        </div>
        <DialogFooter className="flex-col gap-2 sm:flex-row">
          <Button variant="ghost" onClick={onClose} disabled={mergeMutation.isPending}>
            These are different people
          </Button>
          <Button
            onClick={() => mergeMutation.mutate()}
            disabled={
              baseAlts.length === 0 || mergeMutation.isPending
            }
            title={
              baseAlts.length === 0
                ? 'Nothing to merge — only followed alts can be merged in this dialog.'
                : `Merge ${baseAlts.length} followed alt profile${baseAlts.length === 1 ? '' : 's'} into the primary.`
            }
          >
            {mergeMutation.isPending ? (
              <Loader2 className="mr-1 h-3.5 w-3.5 animate-spin" />
            ) : (
              <GitMerge className="mr-1 h-3.5 w-3.5" />
            )}
            Merge as same person
          </Button>
        </DialogFooter>
      </DialogContent>
    </Dialog>
  )
}

function ProfileRow({
  label,
  openalexId,
  displayName,
  tone = 'alt',
}: {
  label: string
  openalexId: string
  displayName: string
  tone?: 'primary' | 'alt'
}) {
  return (
    <div className="flex items-center justify-between gap-3 rounded-sm border border-[var(--color-border)] bg-[#FFFEF7] p-2.5">
      <div className="min-w-0">
        <StatusBadge tone={tone === 'primary' ? 'info' : 'neutral'} size="sm">
          {label}
        </StatusBadge>
        <p className="mt-1 truncate text-sm font-medium text-alma-800">{displayName}</p>
        <p className="truncate font-mono text-[11px] text-slate-500">{openalexId}</p>
      </div>
      <a
        href={`https://openalex.org/${openalexId}`}
        target="_blank"
        rel="noopener noreferrer"
        className="inline-flex items-center gap-1 rounded-sm border border-[var(--color-border)] bg-white px-2 py-1 text-[11px] text-alma-700 hover:border-alma-300 hover:text-alma-800"
      >
        Open
        <ExternalLink className="h-3 w-3" />
      </a>
    </div>
  )
}

/**
 * Add-identifier sub-dialog. Used by the `resolve_now` action
 * (followed author with no OpenAlex ID) so the user can paste an
 * authoritative identifier directly without opening the full
 * AuthorDetailPanel. Calls POST /authors/{id}/identifiers, which
 * marks the row as `id_resolution_status='resolved_manual'` and
 * clears the needs-attention flag on the next refresh.
 *
 * All three identifier types are accepted; the backend uses
 * whatever subset arrives. Saved on submit, dialog closes.
 */
function AddIdentifierDialog({
  row,
  onClose,
}: {
  row: AuthorNeedsAttentionRow | null
  onClose: () => void
}) {
  const open = row !== null
  const queryClient = useQueryClient()
  const { toast } = useToast()
  const [orcid, setOrcid] = useState('')
  const [openalexInput, setOpenalexInput] = useState('')
  const [scholar, setScholar] = useState('')

  // Reset draft fields each time the dialog opens for a new row.
  // Without this, switching from author A to author B would
  // pre-fill A's draft into B's form.
  useMemo(() => {
    if (open) {
      setOrcid('')
      setOpenalexInput('')
      setScholar('')
    }
    return null
  }, [open, row?.author_id])

  const mutation = useMutation({
    mutationFn: () =>
      setAuthorIdentifiers(row!.author_id, {
        orcid: orcid.trim() || undefined,
        openalex_id: openalexInput.trim() || undefined,
        scholar_id: scholar.trim() || undefined,
      }),
    onSuccess: () => {
      invalidateQueries(
        queryClient,
        ['authors'],
        ['authors-needs-attention'],
        ['author-detail', row!.author_id],
      )
      toast({ title: 'Identifiers saved', description: row?.author_name ?? '' })
      onClose()
    },
    onError: () => errorToast('Error', 'Could not save identifiers.'),
  })

  const canSubmit = !!(orcid.trim() || openalexInput.trim() || scholar.trim())

  return (
    <Dialog open={open} onOpenChange={(v) => !v && onClose()}>
      <DialogContent>
        <DialogHeader>
          <DialogTitle>Add identifiers</DialogTitle>
          <DialogDescription>
            Paste any authoritative identifier you have for{' '}
            <span className="font-medium">{row?.author_name}</span>. Whichever one
            you provide will be used to short-circuit the resolver.
          </DialogDescription>
        </DialogHeader>
        <div className="space-y-3">
          <div className="space-y-1">
            <Label htmlFor="add-id-orcid">ORCID iD</Label>
            <Input
              id="add-id-orcid"
              placeholder="0000-0000-0000-0000"
              value={orcid}
              onChange={(e) => setOrcid(e.target.value)}
              autoComplete="off"
            />
          </div>
          <div className="space-y-1">
            <Label htmlFor="add-id-openalex">OpenAlex ID</Label>
            <Input
              id="add-id-openalex"
              placeholder="A1234567890 or https://openalex.org/A123…"
              value={openalexInput}
              onChange={(e) => setOpenalexInput(e.target.value)}
              autoComplete="off"
            />
          </div>
          <div className="space-y-1">
            <Label htmlFor="add-id-scholar">Google Scholar ID</Label>
            <Input
              id="add-id-scholar"
              placeholder="abcDEFgh123"
              value={scholar}
              onChange={(e) => setScholar(e.target.value)}
              autoComplete="off"
            />
          </div>
        </div>
        <DialogFooter>
          <Button variant="ghost" onClick={onClose} disabled={mutation.isPending}>
            Cancel
          </Button>
          <Button
            onClick={() => mutation.mutate()}
            disabled={!canSubmit || mutation.isPending}
          >
            {mutation.isPending && <Loader2 className="mr-1 h-3.5 w-3.5 animate-spin" />}
            Save
          </Button>
        </DialogFooter>
      </DialogContent>
    </Dialog>
  )
}

/**
 * Resolve-conflict dialog. Surfaces the disagreement that the merge
 * recorded in `author_merge_conflicts` and gives the user three
 * choices: keep the primary's value, overwrite with the alt's
 * value, or dismiss the warning without changing the row. Only
 * applies to hard-identifier fields (orcid / scholar_id /
 * semantic_scholar_id) — these are person-level IDs where a
 * disagreement is real evidence that something is wrong.
 */
function ResolveConflictDialog({
  row,
  onClose,
}: {
  row: AuthorNeedsAttentionRow | null
  onClose: () => void
}) {
  const open = row !== null
  const queryClient = useQueryClient()
  const { toast } = useToast()

  const mutation = useMutation({
    mutationFn: (choice: 'primary' | 'alt' | 'dismiss') =>
      resolveMergeConflict(row!.conflict_id!, choice),
    onSuccess: (_data, choice) => {
      invalidateQueries(
        queryClient,
        ['authors'],
        ['authors-needs-attention'],
        ['author-detail', row!.author_id],
      )
      toast({
        title: 'Conflict resolved',
        description:
          choice === 'primary'
            ? "Kept the primary's value."
            : choice === 'alt'
              ? "Overwrote with the alt's value."
              : 'Dismissed.',
      })
      onClose()
    },
    onError: () => errorToast('Error', 'Could not resolve conflict.'),
  })

  return (
    <Dialog open={open} onOpenChange={(v) => !v && onClose()}>
      <DialogContent>
        <DialogHeader>
          <DialogTitle>Resolve merge conflict</DialogTitle>
          <DialogDescription>
            <span className="font-medium">{row?.author_name}</span> — the
            merge kept this <span className="font-mono">{row?.conflict_field}</span>{' '}
            value, but the merged-in alt profile (
            <span className="font-mono">{row?.alt_openalex_id}</span>) had a
            different one. Pick which is correct.
          </DialogDescription>
        </DialogHeader>
        <div className="space-y-2">
          <ProfileRow
            label="Currently kept"
            openalexId={row?.openalex_id ?? ''}
            displayName={row?.conflict_primary_value ?? ''}
            tone="primary"
          />
          <ProfileRow
            label="Alt had"
            openalexId={row?.alt_openalex_id ?? ''}
            displayName={row?.conflict_alt_value ?? ''}
          />
        </div>
        <DialogFooter className="flex-col gap-2 sm:flex-row">
          <Button
            variant="ghost"
            onClick={() => mutation.mutate('dismiss')}
            disabled={mutation.isPending}
          >
            Dismiss
          </Button>
          <Button
            variant="outline"
            onClick={() => mutation.mutate('alt')}
            disabled={mutation.isPending}
          >
            Use alt's value
          </Button>
          <Button
            onClick={() => mutation.mutate('primary')}
            disabled={mutation.isPending}
          >
            {mutation.isPending && <Loader2 className="mr-1 h-3.5 w-3.5 animate-spin" />}
            Keep primary's value
          </Button>
        </DialogFooter>
      </DialogContent>
    </Dialog>
  )
}
