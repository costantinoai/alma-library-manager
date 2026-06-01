import { useEffect, useRef, useState } from 'react'
import { useMutation, useQueryClient } from '@tanstack/react-query'
import { Building2, Check, IdCard, Loader2, RotateCcw, Search, UserRoundCheck } from 'lucide-react'
import { Button } from '@/components/ui/button'
import { Input } from '@/components/ui/input'
import { Label } from '@/components/ui/label'
import { SubPanel } from '@/components/ui/sub-panel'
import { formatNumber } from '@/lib/utils'
import { invalidateQueries } from '@/lib/queryHelpers'
import { errorToast } from '@/hooks/useToast'
import {
  ingestOwner,
  onlineAuthorSearch,
  promoteOwnerPapers,
  resolveOwnerIdentity,
  type OnlineAuthorSearchResult,
  type OwnerProfile,
} from '@/api/client'
import { StepShell, StepNav } from '../StepShell'
import type { StepContext } from '../types'

type Mode = 'id' | 'name'

interface Candidate {
  openalex_id: string
  name: string
  institution?: string | null
  works_count?: number
  cited_by_count?: number
}

const ORCID_RE = /^\d{4}-\d{4}-\d{4}-\d{3}[\dxX]$/

function classifyIdentifier(raw: string): { orcid?: string; openalex_id?: string } {
  const s = raw.trim()
  if (/orcid\.org/i.test(s) || ORCID_RE.test(s)) return { orcid: s }
  return { openalex_id: s }
}

function fromProfile(p: OwnerProfile): Candidate {
  return {
    openalex_id: p.openalex_id,
    name: p.name ?? p.openalex_id,
    institution: p.institution,
    works_count: p.works_count,
    cited_by_count: p.cited_by_count,
  }
}

function fromSearch(r: OnlineAuthorSearchResult): Candidate {
  return {
    openalex_id: r.openalex_id,
    name: r.name,
    institution: r.institution,
    works_count: r.works_count,
    cited_by_count: r.cited_by_count,
  }
}

export function StepIdentity({ state, patch, next, back }: StepContext) {
  const qc = useQueryClient()
  const [mode, setMode] = useState<Mode>('id')
  const [idValue, setIdValue] = useState('')
  const [nameValue, setNameValue] = useState(state.name)
  const [candidate, setCandidate] = useState<Candidate | null>(null)
  const [results, setResults] = useState<Candidate[]>([])
  const [searched, setSearched] = useState(false)

  const owner = state.owner

  const resolveMut = useMutation({
    mutationFn: () => resolveOwnerIdentity(classifyIdentifier(idValue)),
    onSuccess: (p) => setCandidate(fromProfile(p)),
    onError: () => errorToast("Couldn't find that profile", 'Check the ORCID or OpenAlex id and try again.'),
  })

  const searchMut = useMutation({
    mutationFn: () => onlineAuthorSearch({ query: nameValue.trim(), limit: 8 }),
    onSuccess: (rows) => {
      setResults(rows.map(fromSearch))
      setSearched(true)
    },
    onError: () => errorToast('Search failed', 'Try again in a moment.'),
  })

  const ingestMut = useMutation({
    mutationFn: (c: Candidate) => ingestOwner({ openalex_id: c.openalex_id, name: c.name }),
    onSuccess: (res, c) => {
      patch({
        owner: { author_id: res.author_id, openalex_id: res.openalex_id, name: c.name },
        ownerJobId: res.job_id,
      })
      invalidateQueries(qc, ['bootstrap'], ['onboarding-status'], ['authors'], ['library-followed-authors'])
    },
    onError: () => errorToast('Could not add your profile', 'Please try again.'),
  })

  // Background promotion of the owner's backfilled papers into the Library.
  // The deep backfill is async; we poll promote (idempotent) a few times so the
  // count climbs as works land, then stop. The flow never blocks on it.
  const [promoted, setPromoted] = useState(0)
  const [gathering, setGathering] = useState(false)
  const ownerKey = owner?.author_id
  useEffect(() => {
    if (!ownerKey) return
    let cancelled = false
    let timer: ReturnType<typeof setTimeout>
    let attempts = 0
    let stable = 0
    let last = -1
    setGathering(true)
    const tick = async () => {
      if (cancelled) return
      attempts += 1
      try {
        const r = await promoteOwnerPapers()
        if (cancelled) return
        setPromoted(r.promoted)
        if (r.promoted === last) stable += 1
        else {
          stable = 0
          last = r.promoted
        }
      } catch {
        /* ignore — retry on the next tick */
      }
      if (cancelled) return
      if (attempts >= 12 || (last > 0 && stable >= 2)) {
        setGathering(false)
        invalidateQueries(qc, ['bootstrap'], ['onboarding-status'], ['library-papers'], ['papers'])
        return
      }
      timer = setTimeout(tick, 6000)
    }
    timer = setTimeout(tick, 4000)
    return () => {
      cancelled = true
      clearTimeout(timer)
    }
  }, [ownerKey, qc])

  const startOver = () => {
    patch({ owner: null, ownerJobId: null })
    setCandidate(null)
    setPromoted(0)
  }

  // ---- Confirmed owner view -------------------------------------------------
  if (owner) {
    return (
      <StepShell
        eyebrow="Your identity"
        title="You're at the centre."
        lead="Everything ALMa suggests radiates out from your own work. We're pulling your papers into your library now — this can keep running while you carry on."
        footer={<StepNav onBack={back} onContinue={next} continueLabel="Continue" />}
      >
        <SubPanel variant="accent" className="space-y-3">
          <div className="flex items-start gap-3">
            <span className="mt-0.5 grid h-9 w-9 shrink-0 place-items-center rounded-sm bg-alma-folio text-alma-cream">
              <UserRoundCheck className="h-5 w-5" aria-hidden />
            </span>
            <div className="min-w-0">
              <p className="font-brand text-lg font-semibold text-alma-800">{owner.name}</p>
              <p className="text-sm text-slate-600">Set as the owner of this library.</p>
            </div>
          </div>
          <div className="flex items-center gap-2 text-sm text-slate-600">
            {gathering ? (
              <>
                <Loader2 className="h-4 w-4 animate-spin text-alma-folio" aria-hidden />
                <span>
                  Gathering your papers…{' '}
                  {promoted > 0 ? <span className="font-medium text-alma-800">{promoted} saved so far</span> : 'this can take a moment'}
                </span>
              </>
            ) : (
              <>
                <Check className="h-4 w-4 text-success-600" aria-hidden />
                <span>
                  {promoted > 0 ? (
                    <>
                      <span className="font-medium text-alma-800">{promoted}</span> of your papers are in
                      your library.
                    </>
                  ) : (
                    'No papers found yet — they may still be arriving in the background.'
                  )}
                </span>
              </>
            )}
          </div>
          <Button variant="ghost" size="sm" onClick={startOver}>
            <RotateCcw className="h-4 w-4" /> That's not me — start over
          </Button>
        </SubPanel>
      </StepShell>
    )
  }

  // ---- Picker view ----------------------------------------------------------
  return (
    <StepShell
      eyebrow="Your identity"
      title="Let's start with you."
      lead="ALMa works best when it knows your own research. Tell us who you are and we'll pull your publications in as the seed for everything else."
      footer={<StepNav onBack={back} onSkip={next} skipLabel="I'll do this later" />}
    >
      <div className="space-y-5">
        <div className="inline-flex rounded-sm border border-[var(--color-border)] bg-surface-2 p-1">
          <button
            type="button"
            onClick={() => setMode('id')}
            className={
              'flex items-center gap-1.5 rounded-sm px-3 py-1.5 text-sm font-medium transition-colors ' +
              (mode === 'id' ? 'bg-alma-folio-soft text-alma-folio' : 'text-slate-500 hover:text-alma-800')
            }
          >
            <IdCard className="h-4 w-4" /> ORCID / OpenAlex
          </button>
          <button
            type="button"
            onClick={() => setMode('name')}
            className={
              'flex items-center gap-1.5 rounded-sm px-3 py-1.5 text-sm font-medium transition-colors ' +
              (mode === 'name' ? 'bg-alma-folio-soft text-alma-folio' : 'text-slate-500 hover:text-alma-800')
            }
          >
            <Search className="h-4 w-4" /> Search by name
          </button>
        </div>

        {mode === 'id' ? (
          <div className="space-y-3">
            <div className="space-y-2">
              <Label htmlFor="ob-identity-id" className="text-slate-600">
                ORCID or OpenAlex author id
              </Label>
              <div className="flex gap-2">
                <Input
                  id="ob-identity-id"
                  placeholder="0000-0002-1825-0097  or  A5023888391"
                  value={idValue}
                  onChange={(e) => setIdValue(e.target.value)}
                  onKeyDown={(e) => {
                    if (e.key === 'Enter' && idValue.trim()) resolveMut.mutate()
                  }}
                />
                <Button
                  variant="accent"
                  onClick={() => resolveMut.mutate()}
                  disabled={!idValue.trim()}
                  loading={resolveMut.isPending}
                >
                  Find me
                </Button>
              </div>
              <p className="text-xs text-slate-500">
                Using your ORCID is the most reliable — it maps to exactly one profile.
              </p>
            </div>

            {candidate ? (
              <CandidateConfirm
                candidate={candidate}
                pending={ingestMut.isPending}
                onConfirm={() => ingestMut.mutate(candidate)}
                onReject={() => setCandidate(null)}
              />
            ) : null}
          </div>
        ) : (
          <div className="space-y-3">
            <div className="space-y-2">
              <Label htmlFor="ob-identity-name" className="text-slate-600">
                Your full name
              </Label>
              <div className="flex gap-2">
                <Input
                  id="ob-identity-name"
                  placeholder="e.g. Andrea Costantino"
                  value={nameValue}
                  onChange={(e) => setNameValue(e.target.value)}
                  onKeyDown={(e) => {
                    if (e.key === 'Enter' && nameValue.trim()) searchMut.mutate()
                  }}
                />
                <Button
                  variant="accent"
                  onClick={() => searchMut.mutate()}
                  disabled={!nameValue.trim()}
                  loading={searchMut.isPending}
                >
                  Search
                </Button>
              </div>
              <p className="text-xs text-slate-500">Pick the profile that's you — match the institution and topics.</p>
            </div>

            {searched && results.length === 0 ? (
              <p className="rounded-sm border border-[var(--color-border)] bg-surface-2 px-4 py-3 text-sm text-slate-500">
                No author profiles matched that name. Try a different spelling, or switch to ORCID.
              </p>
            ) : null}

            <div className="space-y-2">
              {results.map((c) => (
                <ResultRow
                  key={c.openalex_id}
                  candidate={c}
                  pending={ingestMut.isPending && ingestMut.variables?.openalex_id === c.openalex_id}
                  onPick={() => ingestMut.mutate(c)}
                />
              ))}
            </div>
          </div>
        )}
      </div>
    </StepShell>
  )
}

function MetaLine({ candidate }: { candidate: Candidate }) {
  return (
    <div className="flex flex-wrap items-center gap-x-3 gap-y-1 text-xs text-slate-500">
      {candidate.institution ? (
        <span className="inline-flex items-center gap-1">
          <Building2 className="h-3.5 w-3.5" /> {candidate.institution}
        </span>
      ) : null}
      {typeof candidate.works_count === 'number' ? <span>{formatNumber(candidate.works_count)} works</span> : null}
      {typeof candidate.cited_by_count === 'number' ? <span>{formatNumber(candidate.cited_by_count)} citations</span> : null}
    </div>
  )
}

function CandidateConfirm({
  candidate,
  pending,
  onConfirm,
  onReject,
}: {
  candidate: Candidate
  pending: boolean
  onConfirm: () => void
  onReject: () => void
}) {
  return (
    <SubPanel variant="accent" className="space-y-3">
      <div>
        <p className="font-brand text-lg font-semibold text-alma-800">{candidate.name}</p>
        <MetaLine candidate={candidate} />
      </div>
      <div className="flex items-center gap-2">
        <Button variant="accent" onClick={onConfirm} loading={pending}>
          <Check className="h-4 w-4" /> Yes, that's me
        </Button>
        <Button variant="ghost" onClick={onReject} disabled={pending}>
          Not me
        </Button>
      </div>
    </SubPanel>
  )
}

function ResultRow({
  candidate,
  pending,
  onPick,
}: {
  candidate: Candidate
  pending: boolean
  onPick: () => void
}) {
  return (
    <div className="flex items-center justify-between gap-3 rounded-sm border border-[var(--color-border)] bg-surface-2 px-4 py-3">
      <div className="min-w-0">
        <p className="truncate text-sm font-semibold text-alma-800">{candidate.name}</p>
        <MetaLine candidate={candidate} />
      </div>
      <Button variant="accent" size="sm" onClick={onPick} loading={pending} className="shrink-0">
        This is me
      </Button>
    </div>
  )
}
