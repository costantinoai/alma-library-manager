import { useState } from 'react'
import { useMutation, useQueryClient } from '@tanstack/react-query'
import { Fingerprint, Hammer, Users2, GitMerge, Trash2, RefreshCw } from 'lucide-react'

import {
  dedupAuthorsByOrcid,
  dedupPreprints,
  garbageCollectOrphanAuthors,
  rehydrateAuthorMetadata,
  rehydrateCorpusMetadata,
  refreshAllAuthors,
  type CorpusScope,
} from '@/api/client'
import { AsyncButton, SettingsCard, SettingsSection, SettingsSections } from '@/components/settings/primitives'
import { Alert, AlertDescription } from '@/components/ui/alert'
import { useToast, errorToast } from '@/hooks/useToast'
import { invalidateQueries } from '@/lib/queryHelpers'

// `corpus` is intentionally absent from the UI options (still callable
// via API). 2026-04-26 lifecycle decision: soft-removed authors stay
// in the table for Discovery's negative-signal reads, so a literal
// "every row" sweep is misleading and usually unnecessary — the
// `followed_plus_library` scope captures every author you actually
// engage with.
type ResolveScope = Exclude<CorpusScope, 'corpus' | 'needs_metadata'>
type DedupScope = 'library' | 'corpus'

const SCOPE_HELP: Record<ResolveScope, string> = {
  followed:
    'Only followed authors. Fast — usually under a minute. Default for routine refreshes.',
  followed_plus_library:
    'Followed authors plus every co-author of any saved Library paper. The signal Discovery uses to surface adjacent work — costs more (≈10–30 min on a typical Library) but keeps coauthor centroids fresh.',
  library:
    'Every co-author of any saved Library paper (drops followed authors who have no Library paper). Useful when you only care about the Library graph.',
}

const DEDUP_HELP: Record<DedupScope, string> = {
  library: 'Only twin pairs where one side is a saved Library paper. Recommended for everyday use.',
  corpus: 'Every candidate pair across the whole stored corpus. Slower on large libraries.',
}

export function CorpusMaintenanceCard() {
  const queryClient = useQueryClient()
  const { toast } = useToast()

  const [resolveScope, setResolveScope] = useState<ResolveScope>('followed')
  const [dedupScope, setDedupScope] = useState<DedupScope>('library')

  const rehydrateMutation = useMutation({
    mutationFn: async () => {
      const [papers, authors] = await Promise.all([
        rehydrateCorpusMetadata({ force: false }),
        rehydrateAuthorMetadata({ force: false }),
      ])
      return { papers, authors }
    },
    onSuccess: (data) => {
      void invalidateQueries(
        queryClient,
        ['papers'],
        ['authors'],
        ['authors-needs-attention'],
        ['ai-status'],
        ['activity-operations'],
      )
      if (data.papers?.status === 'already_running' && data.authors?.status === 'already_running') {
        toast({
          title: 'Already running',
          description: 'Paper and author metadata rehydration jobs are already in progress. Watch Activity.',
        })
        return
      }
      toast({
        title: 'Rehydration queued',
        description: 'Paper metadata and author profile/affiliation hydration jobs are queued in Activity.',
      })
    },
    onError: () => errorToast('Error', 'Failed to queue corpus metadata rehydration.'),
  })

  // Canonical bulk refresh — DRY with the popup card's "Refresh author"
  // button (2026-04-24 consolidation). Runs the full pipeline per author:
  // hierarchical identity resolve → OpenAlex profile (name, affiliation,
  // institutions, citations, h_index, works_count, topics, ORCID) →
  // works + SPECTER2 vectors backfill → centroid recompute.
  const refreshMutation = useMutation({
    mutationFn: () => refreshAllAuthors({ scope: resolveScope }),
    onSuccess: (data) => {
      void invalidateQueries(
        queryClient,
        ['authors'],
        ['authors-needs-attention'],
        ['activity-operations'],
      )
      if (data?.status === 'already_running') {
        toast({
          title: 'Already running',
          description: 'An author refresh job is already in progress. Watch Activity.',
        })
        return
      }
      toast({
        title: 'Refresh queued',
        description: data?.job_id
          ? `Job ${data.job_id} started — per-author progress in Activity.`
          : 'Job queued.',
      })
    },
    onError: () => errorToast('Error', 'Failed to queue author refresh.'),
  })

  // Two separate mutations so dry-run and live-sweep buttons can show
  // independent pending states. Defining each with `useMutation`
  // directly (no helper-fn wrapper) keeps React's rules-of-hooks
  // happy — hooks must be called unconditionally and in a stable
  // order on every render.
  const gcDryRunMutation = useMutation({
    mutationFn: () => garbageCollectOrphanAuthors({ dryRun: true }),
    onSuccess: (data) => {
      void invalidateQueries(queryClient, ['authors'], ['activity-operations'])
      if (data?.status === 'already_running') {
        toast({
          title: 'Already running',
          description: 'An author GC sweep is already in progress. Watch Activity.',
        })
        return
      }
      toast({
        title: 'GC dry-run queued',
        description: data?.job_id
          ? `Job ${data.job_id} started — Activity will report what was eligible.`
          : 'Job queued.',
      })
    },
    onError: () => errorToast('Error', 'Failed to queue author GC sweep.'),
  })
  const gcLiveMutation = useMutation({
    mutationFn: () => garbageCollectOrphanAuthors({ dryRun: false }),
    onSuccess: (data) => {
      void invalidateQueries(
        queryClient,
        ['authors'],
        ['authors-needs-attention'],
        ['activity-operations'],
      )
      if (data?.status === 'already_running') {
        toast({
          title: 'Already running',
          description: 'An author GC sweep is already in progress. Watch Activity.',
        })
        return
      }
      toast({
        title: 'GC sweep queued',
        description: data?.job_id
          ? `Job ${data.job_id} started — Activity will report what was collected.`
          : 'Job queued.',
      })
    },
    onError: () => errorToast('Error', 'Failed to queue author GC sweep.'),
  })

  const orcidDedupMutation = useMutation({
    mutationFn: () => dedupAuthorsByOrcid(),
    onSuccess: (data) => {
      void invalidateQueries(
        queryClient,
        ['authors'],
        ['authors-needs-attention'],
        ['library-followed-authors'],
        ['author-suggestions'],
        ['activity-operations'],
      )
      if (data?.status === 'already_running') {
        toast({
          title: 'Already running',
          description: 'An author ORCID dedup sweep is already in progress. Watch Activity.',
        })
        return
      }
      toast({
        title: 'ORCID dedup queued',
        description: data?.job_id
          ? `Job ${data.job_id} started — auto-merges + alias records appear in Activity.`
          : 'Job queued.',
      })
    },
    onError: () => errorToast('Error', 'Failed to queue ORCID dedup.'),
  })

  const dedupMutation = useMutation({
    mutationFn: () => dedupPreprints({ scope: dedupScope }),
    onSuccess: (data) => {
      void invalidateQueries(queryClient, ['papers'], ['library'], ['activity-operations'])
      if (data?.status === 'already_running') {
        toast({
          title: 'Already running',
          description: 'A preprint dedup job is already in progress. Watch Activity.',
        })
        return
      }
      toast({
        title: 'Dedup queued',
        description: data?.job_id
          ? `Job ${data.job_id} started — track progress in Activity.`
          : 'Job queued.',
      })
    },
    onError: () => errorToast('Error', 'Failed to queue preprint dedup.'),
  })

  return (
    <SettingsCard
      icon={Hammer}
      title="Corpus maintenance"
      description="Bulk metadata repair jobs. Both queue via Activity with per-row commits so you can safely refresh the page."
    >
      <SettingsSections>
      <SettingsSection
        title={
          <span className="inline-flex items-center gap-2">
            <RefreshCw className="h-4 w-4 text-slate-500" />
            Rehydrate corpus metadata
          </span>
        }
        description="Batched repair for stored papers and authors that need metadata. Papers use OpenAlex, Semantic Scholar, and Crossref fallbacks for DOI, abstract, TLDR, URL, publication date, authorships, topics, and references. Authors use OpenAlex, ORCID, Semantic Scholar, and Crossref to fill profile fields and affiliation evidence."
      >
        <div className="space-y-3">
          <p className="text-[11px] leading-snug text-slate-500">
            Runs all eligible papers and metadata-needing authors. Re-run safely — unchanged,
            already-enriched, and recently exhausted rows are skipped automatically.
          </p>
          <AsyncButton
            variant="outline"
            icon={<RefreshCw className="h-4 w-4" />}
            pending={rehydrateMutation.isPending}
            onClick={() => rehydrateMutation.mutate()}
          >
            Rehydrate corpus metadata
          </AsyncButton>
        </div>
      </SettingsSection>

      <SettingsSection
        title={
          <span className="inline-flex items-center gap-2">
            <Users2 className="h-4 w-4 text-slate-500" />
            Refresh authors
          </span>
        }
        description="Runs the same full pipeline as the popup card's Refresh button — hierarchical ID resolver (ORCID → OpenAlex → Semantic Scholar → title + co-author triangulation), OpenAlex profile update (name, affiliation, institutions, citations, h-index, works_count, topics, ORCID), works + SPECTER2 vectors backfill, and author centroid recompute. Per-author commits so the job survives partial failures."
      >
        <div className="space-y-3">
          <ScopeRadio<ResolveScope>
            value={resolveScope}
            onChange={setResolveScope}
            options={[
              { value: 'followed', label: 'Followed' },
              { value: 'followed_plus_library', label: 'Followed + Library' },
              { value: 'library', label: 'Library only' },
            ]}
          />
          <p className="text-[11px] leading-snug text-slate-500">{SCOPE_HELP[resolveScope]}</p>
          {resolveScope === 'followed_plus_library' || resolveScope === 'library' ? (
            <Alert variant="warning" className="text-xs">
              <AlertDescription>
                Library-scope refresh walks every co-author of every saved paper. Expect 30 min – a
                few hours on a typical Library, with per-author commits so earlier progress is
                safe. Runs in the background — feel free to close this page.
              </AlertDescription>
            </Alert>
          ) : null}
          <AsyncButton
            variant="outline"
            icon={<Users2 className="h-4 w-4" />}
            pending={refreshMutation.isPending}
            onClick={() => refreshMutation.mutate()}
          >
            Refresh authors ({resolveScope})
          </AsyncButton>
        </div>
      </SettingsSection>

      <SettingsSection
        title={
          <span className="inline-flex items-center gap-2">
            <Trash2 className="h-4 w-4 text-slate-500" />
            Garbage-collect orphan authors
          </span>
        }
        description="Soft-removes (status='removed') any author who isn't followed AND has no publication_authors row pointing to a paper in a live state (anything other than 'removed' / 'dismissed'). Mirrors the paper lifecycle (D3): the row stays in the table so Discovery can read it as a negative signal but it's filtered out of bulk refresh and the canonical author list. Eager triggers (paper-remove, unfollow) already cover steady-state drift — this sweep catches up on history."
      >
        <div className="space-y-3">
          <p className="text-[11px] leading-snug text-slate-500">
            Two passes available: a <strong>dry-run</strong> reports what would be collected
            without writing, and a <strong>live</strong> pass actually flips the rows.
          </p>
          <div className="flex flex-wrap gap-2">
            <AsyncButton
              variant="outline"
              icon={<Trash2 className="h-4 w-4" />}
              pending={gcDryRunMutation.isPending}
              onClick={() => gcDryRunMutation.mutate()}
            >
              Preview (dry-run)
            </AsyncButton>
            <AsyncButton
              variant="outline"
              icon={<Trash2 className="h-4 w-4" />}
              pending={gcLiveMutation.isPending}
              onClick={() => gcLiveMutation.mutate()}
            >
              Run sweep
            </AsyncButton>
          </div>
        </div>
      </SettingsSection>

      <SettingsSection
        title={
          <span className="inline-flex items-center gap-2">
            <Fingerprint className="h-4 w-4 text-slate-500" />
            Dedup followed authors via ORCID
          </span>
        }
        description="Walks every followed author with an OpenAlex ID, queries OpenAlex for every author profile sharing the same ORCID, and either auto-merges (when another currently-followed author already holds that profile — richer-profile-wins) or records the alias so suggestions stop re-surfacing the duplicate. Profile fields merge (missing fields from the alt fill in; numeric metrics take MAX; lists union). Hard-identifier conflicts (orcid / scholar_id / semantic_scholar_id) get flagged in Authors → Needs Attention for manual resolution."
      >
        <div className="space-y-3">
          <p className="text-[11px] leading-snug text-slate-500">
            Safe to re-run — UNIQUE constraints make alias inserts idempotent and
            already-merged authors short-circuit on the next pass.
          </p>
          <AsyncButton
            variant="outline"
            icon={<Fingerprint className="h-4 w-4" />}
            pending={orcidDedupMutation.isPending}
            onClick={() => orcidDedupMutation.mutate()}
          >
            Run ORCID dedup sweep
          </AsyncButton>
        </div>
      </SettingsSection>

      <SettingsSection
        title={
          <span className="inline-flex items-center gap-2">
            <GitMerge className="h-4 w-4 text-slate-500" />
            Dedup preprint ↔ journal twins
          </span>
        }
        description="Detect pairs where the same work exists as both a preprint (arXiv / bioRxiv / psyRxiv / chemRxiv / OSF) and a published journal row, then collapse each pair into the journal version. FK rows (authors, topics, references, embeddings, recommendations, feedback) migrate to the canonical row automatically; Library + Discovery stop double-rendering."
      >
        <div className="space-y-3">
          <ScopeRadio<DedupScope>
            value={dedupScope}
            onChange={setDedupScope}
            options={[
              { value: 'library', label: 'Pairs that touch Library' },
              { value: 'corpus', label: 'Whole corpus' },
            ]}
          />
          <p className="text-[11px] leading-snug text-slate-500">{DEDUP_HELP[dedupScope]}</p>
          {dedupScope === 'corpus' ? (
            <Alert variant="warning" className="text-xs">
              <AlertDescription>
                Whole-corpus dedup inspects every candidate pair. Safe to run repeatedly — merged
                pairs are skipped automatically on reruns. Per-pair commits so a partial failure
                preserves every merge that landed before it.
              </AlertDescription>
            </Alert>
          ) : null}
          <AsyncButton
            variant="outline"
            icon={<GitMerge className="h-4 w-4" />}
            pending={dedupMutation.isPending}
            onClick={() => dedupMutation.mutate()}
          >
            Dedup twins ({dedupScope})
          </AsyncButton>
        </div>
      </SettingsSection>
      </SettingsSections>
    </SettingsCard>
  )
}

interface ScopeRadioProps<T extends string> {
  value: T
  onChange: (value: T) => void
  options: Array<{ value: T; label: string }>
}

function ScopeRadio<T extends string>({ value, onChange, options }: ScopeRadioProps<T>) {
  return (
    <div className="flex flex-wrap gap-1.5">
      {options.map((option) => {
        const active = option.value === value
        return (
          <button
            key={option.value}
            type="button"
            onClick={() => onChange(option.value)}
            className={`rounded-full border px-3 py-1 text-xs font-medium transition ${
              active
                ? 'border-alma-500 bg-alma-50 text-alma-700'
                : 'border-slate-200 bg-alma-chrome text-slate-600 hover:border-[var(--color-border)] hover:bg-parchment-50'
            }`}
            aria-pressed={active}
          >
            {option.label}
          </button>
        )
      })}
    </div>
  )
}
