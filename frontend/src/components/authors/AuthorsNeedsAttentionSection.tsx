import { useEffect, useMemo, useState, type ReactNode } from 'react'
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query'
import { AlertTriangle, ArrowRight, Building2, Check, ExternalLink, GitMerge, Loader2, RefreshCw, Search, Wrench } from 'lucide-react'

import {
  api,
  discoverAuthorAliases,
  getApiErrorMessage,
  getAuthorAffiliations,
  mergeAuthorProfiles,
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
import { useAcceptUnidentified } from '@/components/authors/useAcceptUnidentified'
import { AuthorMergeDialog } from '@/components/authors/AuthorMergeDialog'
import { ConfirmDialog } from '@/components/library'
import { DeleteAuthorButton } from '@/components/authors/DeleteAuthorButton'
import { resolvedBadgeSpec } from '@/components/authors/authorResolvedBadgeSpec'
import type { AuthorAttentionRouter } from '@/components/authors/useAuthorAttentionRouter'
import { useToast, errorToast } from '@/hooks/useToast'
import { invalidateAfterAuthorChange } from '@/lib/queryHelpers'
import { formatTimestamp } from '@/lib/utils'
import { RowButton } from '@/components/ui/row-button'

interface AuthorsNeedsAttentionSectionProps {
  rows: AuthorNeedsAttentionRow[]
  /** Every row there is: the list says so when `rows` is a part of them. */
  total?: number
  isLoading: boolean
  isError: boolean
  onRetry?: () => void
  router: AuthorAttentionRouter
}

/**
 * Authors page "Needs attention" section.
 *
 * Every row of `/authors/needs-attention`: identity gaps, merge and affiliation
 * conflicts, split profiles, degraded monitors and thin corpora, worst first.
 * Each row says why it is here and offers one action; `router` opens that
 * action's dialog or queues its job.
 */
export function AuthorsNeedsAttentionSection({
  rows,
  total = rows.length,
  isLoading,
  isError,
  onRetry,
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
        <div className="flex items-center gap-2">
          <p className="text-xs text-critical-600">Could not load needs-attention rows.</p>
          {onRetry ? (
            <Button size="sm" variant="outline" onClick={onRetry}>
              Retry
            </Button>
          ) : null}
        </div>
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

  // "Fix all" only claims the rows whose fix needs no decision from the user —
  // the button names that number so it can't imply it clears the whole list.
  const autoFixable = router.countAutoFixable(rows)

  return (
    <section className="space-y-2">
      <SectionHeader
        total={new Set(rows.map((row) => row.author_id)).size}
        action={
          autoFixable > 0 ? (
            <Button
              size="sm"
              variant="outline"
              disabled={router.isFixingAll}
              onClick={() => router.fixAll(rows)}
              title="Re-run the automatic resolver / monitor refresh / backfill for every row that has one. Rows needing your decision are left alone."
            >
              {router.isFixingAll ? (
                <Loader2 className="h-3.5 w-3.5 animate-spin" />
              ) : (
                <Wrench className="h-3.5 w-3.5" />
              )}
              Fix all ({autoFixable})
            </Button>
          ) : null
        }
      />
      {rows.length < total ? (
        <p className="text-xs text-slate-500">
          Showing the first {rows.length} of {total} rows. Fix these and the rest move up.
        </p>
      ) : null}
      <ul className="space-y-2">
        {rows.map((row) => (
          <NeedsAttentionRow
            // author_id alone collides now that one author can carry several
            // attention dimensions (identity + monitor + corpus).
            key={`${row.author_id}-${row.reason_code}-${row.conflict_id ?? ''}`}
            row={row}
            onOpenAuthor={() => router.openAuthor(row)}
            onAction={() => router.openForRow(row)}
            isRefreshing={router.isRefreshingFor(row.author_id)}
          />
        ))}
      </ul>
    </section>
  )
}

function SectionHeader({ total, action }: { total: number | null; action?: ReactNode }) {
  // Tour anchor (`onboarding/tours.ts`) sits on the HEADER, not the section:
  // the section runs thousands of pixels tall once rows accumulate, and a
  // spotlight taller than the screen dims nothing.
  return (
    <header data-tour="authors-attention-header" className="flex items-center gap-2">
      <AlertTriangle className="h-4 w-4 text-warning-600" />
      <h2 className="text-sm font-semibold text-alma-800">Needs attention</h2>
      <span className="text-xs text-slate-500">
        {total == null ? 'Checking…' : total === 0 ? 'All clear' : `${total} author${total === 1 ? '' : 's'}`}
      </span>
      {action ? <div className="ml-auto">{action}</div> : null}
    </header>
  )
}

function NeedsAttentionRow({
  row,
  onOpenAuthor,
  onAction,
  isRefreshing,
}: {
  row: AuthorNeedsAttentionRow
  onOpenAuthor: () => void
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

  // The icon tells what a click does: a job starts now, or a choice opens.
  const startsJob = ['refresh', 'refresh_monitor', 'backfill_author'].includes(actionCode)
  const actionIcon = isRefreshing ? (
    <Loader2 className="h-3.5 w-3.5 animate-spin" />
  ) : startsJob ? (
    <RefreshCw className="h-3.5 w-3.5" />
  ) : actionCode === 'review_profiles' ? (
    <GitMerge className="h-3.5 w-3.5" />
  ) : (
    <ArrowRight className="h-3.5 w-3.5" />
  )

  return (
    <li className="rounded-card border border-edge-1 bg-surface-1 p-3 shadow-paper-sm">
      <div className="flex flex-wrap items-start justify-between gap-3">
        {/* basis-64: on a phone the buttons wrap below, and the name keeps the line. */}
        <div className="min-w-0 flex-1 basis-64">
          <div className="flex items-center gap-2">
            <StatusBadge tone={spec.tone} size="sm" title={spec.title} className="shrink-0">
              <spec.icon className="h-3 w-3" aria-hidden />
              <span>{spec.label}</span>
            </StatusBadge>
            <RowButton
              title={row.author_name}
              className="block w-auto min-w-0 truncate text-sm font-semibold text-alma-800 hover:text-alma-700"
              onClick={onOpenAuthor}
            >
              {row.author_name}
            </RowButton>
          </div>
          {row.reason_detail ? (
            <p className="mt-1 break-words text-xs text-slate-500">{row.reason_detail}</p>
          ) : null}
          {row.updated_at ? (
            <p className="mt-1 text-2xs text-slate-400">Updated {formatTimestamp(row.updated_at)}</p>
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
          {/* Last resort, available on every row (some actions fire a job and
              open no dialog): drop the author from the corpus entirely. */}
          <DeleteAuthorButton
            authorId={row.author_id}
            authorName={row.author_name}
            disabled={isRefreshing}
          />
        </div>
      </div>
      {/* Warning bubble row, lower-right — same pattern as the Library
          needs-attention list. The action's hint stays on hover. */}
      <div className="mt-2 flex flex-wrap justify-end gap-1.5">
        <StatusBadge tone="warning" size="sm" title={row.suggested_action.hint}>
          {row.reason}
        </StatusBadge>
      </div>
    </li>
  )
}

/**
 * Review-profiles sub-dialog: the followed profiles sharing a canonical name.
 * Two answers end the warning: merge them, or "not the same person" (D17,
 * permanent). ORCID discovery can add profiles nobody followed; those merge by
 * OpenAlex id, as an alias of the row's author.
 *
 * Built on the existing Dialog primitive — no new components.
 * Each alt-profile row is a flat `<li>` with the OpenAlex ID, the
 * display name OpenAlex has on file, and an external link to the
 * profile on openalex.org for visual verification.
 */
export function ReviewProfilesDialog({
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
  const queryClient = useQueryClient()

  // The dialog stays mounted: one row's discovery must not show on the next.
  const activeAuthorId = row?.author_id
  useEffect(() => {
    setOrcidDiscovery(null)
  }, [activeAuthorId])

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
    onError: (err) => errorToast('ORCID lookup failed', getApiErrorMessage(err)),
  })

  // An alias with no local row merges by OpenAlex id: an alias of the row's
  // author, papers reattached. One with a row joins the merge list above, where
  // fields can be compared. A rejected pair is shown and offers nothing.
  const localAltIds = new Set(baseAlts.map((a) => a.openalex_id.toLowerCase()))
  const orcidExtras = (orcidDiscovery?.aliases ?? []).filter(
    (a) => !localAltIds.has(a.openalex_id.toLowerCase()),
  )
  const absorbable = orcidExtras.filter((alt) => !alt.local_author_id && !alt.rejected)
  const localExtraIds = orcidExtras.flatMap((alt) =>
    alt.local_author_id && !alt.rejected ? [alt.local_author_id] : [],
  )
  const [confirmingAbsorb, setConfirmingAbsorb] = useState(false)
  const absorbExtrasMutation = useMutation({
    mutationFn: () =>
      mergeAuthorProfiles(
        row!.author_id,
        [],
        undefined,
        absorbable.map((alt) => alt.openalex_id),
      ),
    onSuccess: (data) => {
      void invalidateAfterAuthorChange(queryClient, activeAuthorId)
      setOrcidDiscovery(null)
      setConfirmingAbsorb(false)
      toast({
        title: 'ORCID profiles merged',
        description: `${data.alts_processed} profile${data.alts_processed === 1 ? '' : 's'} recorded · ${data.papers_reassigned} paper${data.papers_reassigned === 1 ? '' : 's'} reattached`,
      })
    },
    onError: (err) => {
      setConfirmingAbsorb(false)
      errorToast('Could not merge the ORCID profiles', getApiErrorMessage(err))
    },
  })
  const primaryAuthor: Author | null = row
    ? {
        id: row.author_id,
        name: row.author_name,
        openalex_id: row.openalex_id ?? undefined,
      }
    : null

  return (
    <>
    <ConfirmDialog
      open={confirmingAbsorb}
      onOpenChange={setConfirmingAbsorb}
      variant="default"
      title={`Merge ${absorbable.length} ORCID profile${absorbable.length === 1 ? '' : 's'}?`}
      description={`Same ORCID, same person. ${absorbable.map((alt) => alt.display_name || alt.openalex_id).join(', ')} become aliases of ${row?.author_name ?? 'this author'} and their papers reattach. A merge has no undo.`}
      isPending={absorbExtrasMutation.isPending}
      onConfirm={() => absorbExtrasMutation.mutate()}
    />
    <AuthorMergeDialog
      open={open}
      onOpenChange={(next) => {
        if (!next) onClose()
      }}
      primaryAuthor={primaryAuthor}
      allowedTargetIds={[...baseAlts.map((alt) => alt.author_id), ...localExtraIds]}
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
      offerNotSamePerson
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
                    title={
                      alt.rejected
                        ? 'You marked this as a different person'
                        : `Via ORCID${alt.institution ? ` · ${alt.institution}` : ''}`
                    }
                  />
                ))}
              </>
            )}
            <Button
              variant="ghost"
              size="sm"
              onClick={() => discoverMutation.mutate()}
              disabled={discoverMutation.isPending || !row}
              className="ml-auto h-7 px-2 text-caption"
              title="Looks up the primary's ORCID on OpenAlex and queries every profile sharing it. ORCID is person-level — same ORCID = same human, with very high confidence."
            >
              {discoverMutation.isPending ? (
                <Loader2 className="h-3.5 w-3.5 animate-spin" />
              ) : (
                <GitMerge className="h-3.5 w-3.5" />
              )}
              Discover via ORCID
            </Button>
            {absorbable.length > 0 ? (
              <Button
                variant="outline"
                size="sm"
                onClick={() => setConfirmingAbsorb(true)}
                disabled={absorbExtrasMutation.isPending}
                className="h-7 px-2 text-caption"
                title="Same ORCID, same person. Records these as aliases of this author and reattaches their papers."
              >
                {absorbExtrasMutation.isPending ? (
                  <Loader2 className="h-3.5 w-3.5 animate-spin" />
                ) : (
                  <GitMerge className="h-3.5 w-3.5" />
                )}
                Merge {absorbable.length} ORCID profile{absorbable.length === 1 ? '' : 's'}
              </Button>
            ) : null}
          </div>
        </SubPanel>
      }
    />
    </>
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
        ? 'border-warning-700/20 bg-warning-700/[0.06] text-alma-800'
        : 'border-edge-2 bg-surface-2 text-alma-800'
  const dotTone =
    tone === 'primary' ? 'bg-accent' : tone === 'orcid' ? 'bg-warning-500' : 'bg-slate-400'
  return (
    <a
      href={`https://openalex.org/${openalexId}`}
      target="_blank"
      rel="noopener noreferrer"
      title={title}
      className={`group inline-flex max-w-[260px] items-center gap-1.5 rounded-control border px-2 py-1 text-xs transition hover:shadow-paper-sm ${palette}`}
    >
      <span className={`h-1.5 w-1.5 shrink-0 rounded-full ${dotTone}`} />
      <span className="truncate font-medium">{label}</span>
      <span className="hidden font-mono text-2xs text-slate-500 sm:inline">
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
        <p className="truncate font-mono text-caption text-slate-500">{openalexId}</p>
      </div>
      {openalexId ? (
        <a
          href={`https://openalex.org/${openalexId}`}
          target="_blank"
          rel="noopener noreferrer"
          className="inline-flex items-center gap-1 rounded-control border border-control-edge bg-control-well px-2 py-1 text-caption text-alma-700 hover:border-control-edge-strong hover:bg-control-quiet hover:text-alma-800"
        >
          Open
          <ExternalLink className="h-3 w-3" />
        </a>
      ) : null}
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
 * whatever subset arrives. A malformed or already-owned identifier comes back
 * as the server's message and the dialog stays open on the typed text.
 * While a call is running the dialog cannot close, so its result always lands.
 */
export function AddIdentifierDialog({
  row,
  onClose,
  onSearchCandidates,
}: {
  row: AuthorNeedsAttentionRow | null
  onClose: () => void
  /** Leaves this dialog for the author's real OpenAlex and Scholar candidate search. */
  onSearchCandidates?: (row: AuthorNeedsAttentionRow) => void
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
  const activeAuthorId = row?.author_id
  useEffect(() => {
    if (open && activeAuthorId) {
      setOrcid('')
      setOpenalexInput('')
      setScholar('')
    }
  }, [activeAuthorId, open])

  const mutation = useMutation({
    mutationFn: () =>
      setAuthorIdentifiers(row!.author_id, {
        orcid: orcid.trim() || undefined,
        openalex_id: openalexInput.trim() || undefined,
        scholar_id: scholar.trim() || undefined,
      }),
    onSuccess: (saved) => {
      void invalidateAfterAuthorChange(queryClient, activeAuthorId)
      toast(
        saved.openalex_id
          ? { title: 'Identifiers saved', description: row?.author_name ?? '' }
          : {
              title: 'Saved, still no OpenAlex ID',
              description: `${row?.author_name ?? 'This author'} stays on the list until an OpenAlex ID is found: papers are fetched with it.`,
            },
      )
      onClose()
    },
    onError: (err) => errorToast('Could not save identifiers', getApiErrorMessage(err)),
  })

  // Terminal "give up gracefully": some authors are genuinely not in any index
  // (students, industry, non-indexed). Accepting marks them dismissed so they
  // stop nagging — the blocking fix — without fabricating an identifier.
  const acceptMutation = useAcceptUnidentified(onClose)

  // Auto path: re-run the hierarchical resolver. Cheap, and the right first try
  // for a transient `error`; for a true `no_match` it usually fails again, which
  // is exactly when the manual paste / accept below earn their place.
  const retryMutation = useMutation({
    mutationFn: () =>
      api.post(`/authors/${encodeURIComponent(row!.author_id)}/identity-profile-refresh`),
    onSuccess: () => {
      void invalidateAfterAuthorChange(queryClient, activeAuthorId)
      toast({ title: 'Refresh queued', description: 'Auto-resolution is re-running.' })
      onClose()
    },
    onError: (err) => errorToast('Could not queue the refresh', getApiErrorMessage(err)),
  })

  const busy = mutation.isPending || acceptMutation.isPending || retryMutation.isPending
  const canSubmit = !!(orcid.trim() || openalexInput.trim() || scholar.trim())

  return (
    <Dialog open={open} onOpenChange={(v) => !v && !busy && onClose()}>
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
          <SubPanel className="flex flex-wrap items-center justify-between gap-2 p-2.5">
            <span className="text-xs text-slate-500">Try these first</span>
            <div className="flex flex-wrap justify-end gap-2">
              {onSearchCandidates && row ? (
                <Button variant="outline" size="sm" onClick={() => onSearchCandidates(row)} disabled={busy}>
                  <Search className="mr-1 h-3.5 w-3.5" />
                  Search candidates
                </Button>
              ) : null}
              <Button variant="outline" size="sm" onClick={() => retryMutation.mutate()} disabled={busy}>
                {retryMutation.isPending ? (
                  <Loader2 className="mr-1 h-3.5 w-3.5 animate-spin" />
                ) : (
                  <RefreshCw className="mr-1 h-3.5 w-3.5" />
                )}
                Retry auto-resolve
              </Button>
            </div>
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
          {/* The two terminal exits, weakest first: keep the author but stop the
              nagging, or remove them from the corpus altogether. */}
          <div className="flex items-center gap-1">
            <Button
              variant="muted"
              onClick={() => acceptMutation.mutate({ id: row!.author_id, name: row!.author_name })}
              disabled={busy || !row}
            >
              {acceptMutation.isPending && <Loader2 className="mr-1 h-3.5 w-3.5 animate-spin" />}
              Can&apos;t identify
            </Button>
            {row ? (
              <DeleteAuthorButton
                authorId={row.author_id}
                authorName={row.author_name}
                variant="labelled"
                disabled={busy}
                onDeleted={onClose}
              />
            ) : null}
          </div>
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
export function AffiliationPickerDialog({
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

  const activeAuthorId = row?.author_id
  useEffect(() => {
    if (open && activeAuthorId) setCustom('')
  }, [activeAuthorId, open])

  const { data, isLoading, isError, error, refetch } = useQuery({
    queryKey: ['author-affiliations', row?.author_id],
    queryFn: () => getAuthorAffiliations(row!.author_id),
    enabled: open,
  })

  const mutation = useMutation({
    mutationFn: (institution_name: string) =>
      pickAuthorAffiliation(row!.author_id, { institution_name }),
    onSuccess: (res) => {
      void invalidateAfterAuthorChange(queryClient, activeAuthorId)
      toast({ title: 'Affiliation set', description: res.affiliation ?? row?.author_name ?? '' })
      onClose()
    },
    onError: (err) => errorToast('Could not set the affiliation', getApiErrorMessage(err)),
  })

  const options = useMemo(() => groupAffiliationOptions(data?.items ?? []), [data])
  const current = (data?.display_affiliation ?? '').trim()
  const pending = mutation.isPending
  const picking = pending ? (mutation.variables as string) : null

  return (
    <Dialog open={open} onOpenChange={(v) => !v && !pending && onClose()}>
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
          ) : isError ? (
            <div className="flex items-center justify-between gap-2">
              <p className="min-w-0 text-xs text-critical-600">
                Could not load the evidence: {getApiErrorMessage(error)}
              </p>
              <Button size="sm" variant="outline" className="shrink-0" onClick={() => void refetch()}>
                Retry
              </Button>
            </div>
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
                    {/* The shown value is a choice too: keeping it settles the
                        conflict just as a new pick does. */}
                    <Button
                      size="sm"
                      variant="outline"
                      className="shrink-0"
                      onClick={() => mutation.mutate(opt.institution_name)}
                      disabled={pending}
                    >
                      {picking === opt.institution_name ? (
                        <Loader2 className="mr-1 h-3.5 w-3.5 animate-spin" />
                      ) : isCurrent ? (
                        <Check className="mr-1 h-3.5 w-3.5" />
                      ) : null}
                      {isCurrent ? 'Keep this' : 'Use this'}
                    </Button>
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
                onKeyDown={(e) => {
                  if (e.key === 'Enter' && custom.trim() && !pending) mutation.mutate(custom.trim())
                }}
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
export function ResolveConflictDialog({
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
      void invalidateAfterAuthorChange(queryClient, row?.author_id)
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
    onError: (err) => errorToast('Could not resolve the conflict', getApiErrorMessage(err)),
  })
  const picking = mutation.isPending ? mutation.variables : null
  const spinner = <Loader2 className="mr-1 h-3.5 w-3.5 animate-spin" />

  return (
    <Dialog open={open} onOpenChange={(v) => !v && !mutation.isPending && onClose()}>
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
            title="Keep the row as it is and stop flagging this."
          >
            {picking === 'dismiss' && spinner}
            Dismiss
          </Button>
          <Button
            variant="outline"
            onClick={() => mutation.mutate('alt')}
            disabled={mutation.isPending}
          >
            {picking === 'alt' && spinner}
            Use alt's value
          </Button>
          <Button
            onClick={() => mutation.mutate('primary')}
            disabled={mutation.isPending}
          >
            {picking === 'primary' && spinner}
            Keep primary's value
          </Button>
        </DialogFooter>
      </DialogContent>
    </Dialog>
  )
}
