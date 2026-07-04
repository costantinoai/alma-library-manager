import { useMemo, useState, type ReactNode } from 'react'
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query'
import { AlertTriangle, ArrowRight, Building2, Check, ExternalLink, GitMerge, Loader2, RefreshCw } from 'lucide-react'

import {
  acceptAuthorUnidentified,
  api,
  discoverAuthorAliases,
  getAuthorAffiliations,
  pickAuthorAffiliation,
  resolveMergeConflict,
  setAuthorIdentifiers,
  type Author,
  type AuthorAffiliationItem,
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
import { Card } from '@/components/ui/card'
import { EmptyState } from '@/components/ui/empty-state'
import { EyebrowLabel } from '@/components/ui/eyebrow-label'
import { Input } from '@/components/ui/input'
import { Label } from '@/components/ui/label'
import { LoadingState } from '@/components/ui/LoadingState'
import { StatusBadge } from '@/components/ui/status-badge'
import { SubPanel } from '@/components/ui/sub-panel'
import { AuthorMergeDialog } from '@/components/authors/AuthorMergeDialog'
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
   *  fires the identity/profile refresh mutation as the default. */
  openForRow: (row: AuthorNeedsAttentionRow) => void
  /** True while the identity/profile refresh mutation is in flight for `authorId`. */
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
 * the identity/profile refresh mutation so multiple entry points (the
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
  const [affiliationRow, setAffiliationRow] = useState<AuthorNeedsAttentionRow | null>(null)

  const refreshMutation = useMutation({
    mutationFn: (authorId: string) =>
      api.post<{ status?: string; job_id?: string }>(
        `/authors/${encodeURIComponent(authorId)}/identity-profile-refresh`,
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
    if (code === 'pick_affiliation') {
      setAffiliationRow(row)
      return
    }
    if (code === 'review_candidates') {
      // Disambiguating among close OpenAlex candidates needs the detail panel's
      // candidate picker — the right tool for "pick the correct match".
      const author = opts.authorsById?.get(row.author_id)
      if (author && opts.onOpenDetail) opts.onOpenDetail(author)
      return
    }
    // Every other identity failure — no_match (manual_search), transient error
    // (retry_refresh), followed-without-id (resolve_now) — opens the one focused
    // identity card: retry auto-resolution, paste an authoritative id, or accept
    // it can't be identified.
    if (code === 'manual_search' || code === 'resolve_now' || code === 'retry_refresh') {
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
      <AffiliationPickerDialog row={affiliationRow} onClose={() => setAffiliationRow(null)} />
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
 * can review candidates manually) or the per-author identity/profile
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
        <p className="text-xs text-critical-600">
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
      <AlertTriangle className="h-4 w-4 text-warning-600" />
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
  // `AuthorNeedsAttentionRow` exposes the resolver state under flat
  // names (`status` / `method` / `confidence`) while `resolvedBadgeSpec`
  // takes the dossier-shaped `id_resolution_*` triple. Same fields,
  // different naming at the API boundary — adapt at the call site.
  const spec = resolvedBadgeSpec({
    id_resolution_status: row.status,
    id_resolution_method: row.method,
    id_resolution_confidence: row.confidence,
  })
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
    <li className="rounded-sm border border-edge-1 bg-surface-1 p-3 shadow-paper-sm">
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
  const primaryAuthor: Author | null = row
    ? {
        id: row.author_id,
        name: row.author_name,
        openalex_id: row.openalex_id ?? undefined,
      }
    : null

  return (
    <AuthorMergeDialog
      open={open}
      onOpenChange={(next) => {
        if (!next) onClose()
      }}
      primaryAuthor={primaryAuthor}
      allowedTargetIds={baseAlts.map((alt) => alt.author_id)}
      initialTargetId={baseAlts[0]?.author_id ?? null}
      title="Review duplicate profiles"
      description={
        row?.author_name ? (
          <>
            <span className="font-medium">{row.author_name}</span> appears under multiple OpenAlex
            IDs in your followed authors. Confirm the duplicate and choose which metadata survives.
          </>
        ) : null
      }
      emptyCandidateMessage="No followed alternate profiles are available to merge from this warning."
      onMerged={onClose}
      contextSlot={
        <SubPanel variant="flat" padded={false} className="px-3 py-2">
          <div className="flex flex-wrap items-center gap-x-3 gap-y-2">
            <EyebrowLabel tone="muted" className="shrink-0">
              In this warning
            </EyebrowLabel>
            {row?.openalex_id ? (
              <ProfileChip
                tone="primary"
                openalexId={row.openalex_id}
                label={row.author_name}
              />
            ) : null}
            {baseAlts.length === 0 && orcidExtras.length === 0 ? (
              <span className="text-xs italic text-slate-500">No alternate profiles.</span>
            ) : (
              <>
                {baseAlts.map((alt) => (
                  <ProfileChip
                    key={alt.openalex_id}
                    tone="alt"
                    openalexId={alt.openalex_id}
                    label={alt.display_name}
                    title="Followed alt profile under the same canonical name"
                  />
                ))}
                {orcidExtras.map((alt) => (
                  <ProfileChip
                    key={`orcid-${alt.openalex_id}`}
                    tone="orcid"
                    openalexId={alt.openalex_id}
                    label={alt.display_name || alt.openalex_id}
                    title={`Via ORCID${alt.institution ? ` · ${alt.institution}` : ''}`}
                  />
                ))}
              </>
            )}
            <Button
              variant="ghost"
              size="sm"
              onClick={() => discoverMutation.mutate()}
              disabled={discoverMutation.isPending || !row}
              className="ml-auto h-7 px-2 text-[11px]"
              title="Looks up the primary's ORCID on OpenAlex and queries every profile sharing it. ORCID is person-level — same ORCID = same human, with very high confidence."
            >
              {discoverMutation.isPending ? (
                <Loader2 className="h-3.5 w-3.5 animate-spin" />
              ) : (
                <GitMerge className="h-3.5 w-3.5" />
              )}
              Discover via ORCID
            </Button>
          </div>
        </SubPanel>
      }
    />
  )
}

/**
 * Inline profile chip used in the merge dialog's contextSlot. The
 * vertical-card pattern (`ProfileRow`) makes the warning context look
 * like a second dialog stacked on the merge UI; the chip variant flows
 * inline inside a `SubPanel`, telegraphing "this is the warning's
 * frame, not its content".
 *
 * Tones distinguish provenance at a glance:
 *   - `primary` — the row's canonical author (folio-blue)
 *   - `alt`     — followed alt profile under the same canonical name
 *   - `orcid`   — discovered via ORCID round-trip
 */
function ProfileChip({
  openalexId,
  label,
  tone,
  title,
}: {
  openalexId: string
  label: string
  tone: 'primary' | 'alt' | 'orcid'
  title?: string
}) {
  const palette =
    tone === 'primary'
      ? 'border-accent bg-accent-soft text-alma-800'
      : tone === 'orcid'
        ? 'border-warning-100 bg-warning-50/70 text-alma-800'
        : 'border-edge-2 bg-surface-2 text-alma-800'
  const dotTone =
    tone === 'primary' ? 'bg-accent' : tone === 'orcid' ? 'bg-warning-500' : 'bg-slate-400'
  return (
    <a
      href={`https://openalex.org/${openalexId}`}
      target="_blank"
      rel="noopener noreferrer"
      title={title}
      className={`group inline-flex max-w-[260px] items-center gap-1.5 rounded-sm border px-2 py-1 text-xs transition hover:shadow-paper-sm ${palette}`}
    >
      <span className={`h-1.5 w-1.5 shrink-0 rounded-full ${dotTone}`} />
      <span className="truncate font-medium">{label}</span>
      <span className="hidden font-mono text-[10px] text-slate-500 sm:inline">
        {openalexId}
      </span>
      <ExternalLink className="h-3 w-3 shrink-0 opacity-50 transition group-hover:opacity-100" />
    </a>
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
    <Card className="flex items-center justify-between gap-3 p-2.5">
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
        className="inline-flex items-center gap-1 rounded-sm border border-edge-1 bg-surface-1 px-2 py-1 text-[11px] text-alma-700 hover:border-alma-300 hover:text-alma-800"
      >
        Open
        <ExternalLink className="h-3 w-3" />
      </a>
    </Card>
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

  // Terminal "give up gracefully": some authors are genuinely not in any index
  // (students, industry, non-indexed). Accepting marks them dismissed so they
  // stop nagging — the blocking fix — without fabricating an identifier.
  const acceptMutation = useMutation({
    mutationFn: () => acceptAuthorUnidentified(row!.author_id),
    onSuccess: () => {
      invalidateQueries(
        queryClient,
        ['authors'],
        ['authors-needs-attention'],
        ['author-detail', row!.author_id],
        ['health'],
      )
      toast({ title: 'Marked as unidentifiable', description: row?.author_name ?? '' })
      onClose()
    },
    onError: () => errorToast('Error', 'Could not update the author.'),
  })

  // Auto path: re-run the hierarchical resolver. Cheap, and the right first try
  // for a transient `error`; for a true `no_match` it usually fails again, which
  // is exactly when the manual paste / accept below earn their place.
  const retryMutation = useMutation({
    mutationFn: () =>
      api.post(`/authors/${encodeURIComponent(row!.author_id)}/identity-profile-refresh`),
    onSuccess: () => {
      invalidateQueries(
        queryClient,
        ['authors'],
        ['authors-needs-attention'],
        ['activity-operations'],
        ['author-detail', row!.author_id],
      )
      toast({ title: 'Refresh queued', description: 'Auto-resolution is re-running.' })
      onClose()
    },
    onError: () => errorToast('Error', 'Could not queue the refresh.'),
  })

  const busy = mutation.isPending || acceptMutation.isPending || retryMutation.isPending
  const canSubmit = !!(orcid.trim() || openalexInput.trim() || scholar.trim())

  return (
    <Dialog open={open} onOpenChange={(v) => !v && onClose()}>
      <DialogContent>
        <DialogHeader>
          <DialogTitle>Resolve identity</DialogTitle>
          <DialogDescription>
            Paste any authoritative identifier you have for{' '}
            <span className="font-medium">{row?.author_name}</span> to short-circuit
            the resolver — or, if they genuinely can&apos;t be identified, accept that
            so they stop showing here.
          </DialogDescription>
        </DialogHeader>
        <div className="space-y-3">
          <SubPanel className="flex items-center justify-between gap-2 p-2.5">
            <span className="text-xs text-slate-500">Try the automatic resolver again first</span>
            <Button variant="outline" size="sm" onClick={() => retryMutation.mutate()} disabled={busy}>
              {retryMutation.isPending ? (
                <Loader2 className="mr-1 h-3.5 w-3.5 animate-spin" />
              ) : (
                <RefreshCw className="mr-1 h-3.5 w-3.5" />
              )}
              Retry auto-resolve
            </Button>
          </SubPanel>
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
        <DialogFooter className="sm:justify-between">
          <Button
            variant="ghost"
            className="text-slate-500 hover:text-slate-700"
            onClick={() => acceptMutation.mutate()}
            disabled={busy}
          >
            {acceptMutation.isPending && <Loader2 className="mr-1 h-3.5 w-3.5 animate-spin" />}
            Can&apos;t identify
          </Button>
          <div className="flex gap-2">
            <Button variant="ghost" onClick={onClose} disabled={busy}>
              Cancel
            </Button>
            <Button onClick={() => mutation.mutate()} disabled={!canSubmit || busy}>
              {mutation.isPending && <Loader2 className="mr-1 h-3.5 w-3.5 animate-spin" />}
              Save
            </Button>
          </div>
        </DialogFooter>
      </DialogContent>
    </Dialog>
  )
}

interface AffiliationOption {
  institution_name: string
  score: number
  sources: string[]
  isCurrent: boolean
  openalexId?: string | null
  ror?: string | null
}

/** Collapse the raw per-source evidence rows into distinct institutions:
 *  dedupe by case-insensitive name, keep the best score, union the sources
 *  (so the user sees "OpenAlex · ORCID"), and surface the highest-signal
 *  options first. The synthetic `manual` source is the user's own prior pick,
 *  represented by the highlighted current row — never offered as a choice. */
function groupAffiliationOptions(items: AuthorAffiliationItem[]): AffiliationOption[] {
  const byKey = new Map<string, AffiliationOption>()
  for (const it of items) {
    const name = (it.institution_name || '').trim()
    if (!name || it.source === 'manual') continue
    const key = name.toLowerCase()
    const score = it.score ?? 0
    const existing = byKey.get(key)
    if (!existing) {
      byKey.set(key, {
        institution_name: name,
        score,
        sources: [it.source],
        isCurrent: !!it.is_current,
        openalexId: it.institution_openalex_id,
        ror: it.institution_ror,
      })
    } else {
      existing.score = Math.max(existing.score, score)
      if (!existing.sources.includes(it.source)) existing.sources.push(it.source)
      existing.isCurrent = existing.isCurrent || !!it.is_current
      existing.openalexId = existing.openalexId || it.institution_openalex_id
      existing.ror = existing.ror || it.institution_ror
    }
  }
  return Array.from(byKey.values()).sort((a, b) => b.score - a.score)
}

/**
 * Affiliation picker. Terminal resolution for the `pick_affiliation` action
 * (reason_code `affiliation_conflict`): the auto sources name different
 * institutions (the author moved), so no refresh can reconcile them — the user
 * must choose. Reuses the existing `/authors/{id}/affiliations` evidence + the
 * `pickAuthorAffiliation` primitive, which records the choice as an
 * authoritative manual evidence row that outranks every source, survives
 * refreshes, and clears the conflict for good. A free-text fallback covers the
 * case where none of the evidence is the right display name.
 */
function AffiliationPickerDialog({
  row,
  onClose,
}: {
  row: AuthorNeedsAttentionRow | null
  onClose: () => void
}) {
  const open = row !== null
  const queryClient = useQueryClient()
  const { toast } = useToast()
  const [custom, setCustom] = useState('')

  useMemo(() => {
    if (open) setCustom('')
    return null
  }, [open, row?.author_id])

  const { data, isLoading } = useQuery({
    queryKey: ['author-affiliations', row?.author_id],
    queryFn: () => getAuthorAffiliations(row!.author_id),
    enabled: open,
  })

  const mutation = useMutation({
    mutationFn: (institution_name: string) =>
      pickAuthorAffiliation(row!.author_id, { institution_name }),
    onSuccess: (res) => {
      invalidateQueries(
        queryClient,
        ['authors'],
        ['authors-needs-attention'],
        ['author-detail', row!.author_id],
        ['author-affiliations', row!.author_id],
        ['health'],
      )
      toast({ title: 'Affiliation set', description: res.affiliation ?? row?.author_name ?? '' })
      onClose()
    },
    onError: () => errorToast('Error', 'Could not set the affiliation.'),
  })

  const options = useMemo(() => groupAffiliationOptions(data?.items ?? []), [data])
  const current = (data?.display_affiliation ?? '').trim()
  const pending = mutation.isPending
  const picking = pending ? (mutation.variables as string) : null

  return (
    <Dialog open={open} onOpenChange={(v) => !v && onClose()}>
      <DialogContent>
        <DialogHeader>
          <DialogTitle>Choose affiliation</DialogTitle>
          <DialogDescription>
            Sources disagree on the institution for{' '}
            <span className="font-medium">{row?.author_name}</span> — usually because they
            moved. Pick the one to display; it sticks and won&apos;t be flagged again.
          </DialogDescription>
        </DialogHeader>
        {/* min-w-0: DialogContent is display:grid, whose items default to
            min-width:auto and won't shrink below their content's max-content — a
            long institution name would otherwise widen the whole dialog past
            max-w-lg and push the "Use this" button off-screen (horizontal
            scroll). min-w-0 lets the name truncate instead, so the row (and its
            button) always fit. */}
        <div className="min-w-0 space-y-2">
          {isLoading ? (
            <LoadingState message="Loading affiliation evidence..." />
          ) : options.length === 0 ? (
            <p className="text-xs text-slate-500">No affiliation evidence on file — type one below.</p>
          ) : (
            <div className="max-h-72 min-w-0 space-y-1.5 overflow-y-auto pr-1">
              {options.map((opt) => {
                const isCurrent = current && opt.institution_name.toLowerCase() === current.toLowerCase()
                return (
                  <SubPanel key={opt.institution_name} className="flex w-full min-w-0 items-center gap-2 p-2.5">
                    <Building2 className="h-4 w-4 shrink-0 text-slate-400" />
                    <div className="min-w-0 flex-1">
                      <p className="truncate text-sm text-slate-700" title={opt.institution_name}>
                        {opt.institution_name}
                      </p>
                      <div className="mt-0.5 flex flex-wrap items-center gap-1">
                        {opt.sources.map((s) => (
                          <StatusBadge key={s} tone="neutral" size="sm">
                            {s}
                          </StatusBadge>
                        ))}
                        {opt.isCurrent && (
                          <StatusBadge tone="info" size="sm">
                            current role
                          </StatusBadge>
                        )}
                      </div>
                    </div>
                    {isCurrent ? (
                      <span className="flex shrink-0 items-center gap-1 text-xs font-medium text-accent-600">
                        <Check className="h-3.5 w-3.5" /> Selected
                      </span>
                    ) : (
                      <Button
                        size="sm"
                        variant="outline"
                        className="shrink-0"
                        onClick={() => mutation.mutate(opt.institution_name)}
                        disabled={pending}
                      >
                        {picking === opt.institution_name && (
                          <Loader2 className="mr-1 h-3.5 w-3.5 animate-spin" />
                        )}
                        Use this
                      </Button>
                    )}
                  </SubPanel>
                )
              })}
            </div>
          )}
          <div className="space-y-1 pt-1">
            <Label htmlFor="affiliation-custom">Or type an institution</Label>
            <div className="flex min-w-0 gap-2">
              <Input
                id="affiliation-custom"
                placeholder="e.g. Harvard University"
                value={custom}
                onChange={(e) => setCustom(e.target.value)}
                autoComplete="off"
              />
              <Button
                variant="outline"
                className="shrink-0"
                onClick={() => mutation.mutate(custom.trim())}
                disabled={!custom.trim() || pending}
              >
                Use
              </Button>
            </div>
          </div>
        </div>
        <DialogFooter>
          <Button variant="ghost" onClick={onClose} disabled={pending}>
            Cancel
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
