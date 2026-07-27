import { useEffect, useRef, useState } from 'react'
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query'
import { CheckCircle2, Loader2, XCircle } from 'lucide-react'
import { Input } from '@/components/ui/input'
import { Label } from '@/components/ui/label'
import { StatusBadge } from '@/components/ui/status-badge'
import { ConceptCallout } from '@/components/ui/concept-callout'
import { invalidateQueries } from '@/lib/queryHelpers'
import { errorToast } from '@/hooks/useToast'
import {
  api,
  getApiErrorMessage,
  type OpenAlexStatus,
  type SemanticScholarStatus,
  type Settings,
} from '@/api/client'
import { StepShell, StepNav } from '../StepShell'
import type { StepContext } from '../types'

function ConnectionDot({
  configured,
  valid,
  loading,
}: {
  configured?: boolean
  valid?: boolean | null
  loading?: boolean
}) {
  if (loading) {
    return (
      <StatusBadge tone="neutral" size="sm">
        <Loader2 className="h-3 w-3 animate-spin" /> Checking
      </StatusBadge>
    )
  }
  if (!configured) {
    return (
      <StatusBadge tone="neutral" size="sm">
        Not set
      </StatusBadge>
    )
  }
  if (valid) {
    return (
      <StatusBadge tone="positive" size="sm">
        <CheckCircle2 className="h-3 w-3" /> Connected
      </StatusBadge>
    )
  }
  return (
    <StatusBadge tone="negative" size="sm">
      <XCircle className="h-3 w-3" /> Check key
    </StatusBadge>
  )
}

export function StepConnect({ next, back }: StepContext) {
  const qc = useQueryClient()
  const current = useQuery({
    queryKey: ['settings'],
    queryFn: () => api.get<Settings>('/settings'),
    staleTime: 60_000,
  })

  const [email, setEmail] = useState('')
  const [openalexKey, setOpenalexKey] = useState('')
  const [s2Key, setS2Key] = useState('')
  const emailSeeded = useRef(false)
  const emailDirty = useRef(false)

  // Seed email once from loaded settings, without preventing the user from
  // intentionally clearing an existing value.
  useEffect(() => {
    if (emailSeeded.current || emailDirty.current || !current.data) return
    emailSeeded.current = true
    setEmail(current.data.openalex_email ?? '')
  }, [current.data])

  const openalexStatus = useQuery({
    queryKey: ['openalex-status'],
    queryFn: () => api.get<OpenAlexStatus>('/settings/openalex/status'),
    staleTime: 60_000,
    retry: 1,
    refetchOnWindowFocus: false,
  })
  const s2Status = useQuery({
    queryKey: ['semantic-scholar-status'],
    queryFn: () => api.get<SemanticScholarStatus>('/settings/semantic-scholar/status'),
    staleTime: 60_000,
    retry: 1,
    refetchOnWindowFocus: false,
  })

  const save = useMutation({
    mutationFn: async () => {
      const payload: Partial<Settings> = {
        backend: 'openalex',
        openalex_email: email.trim(),
      }
      if (openalexKey.trim()) payload.openalex_api_key = openalexKey.trim()
      if (s2Key.trim()) payload.semantic_scholar_api_key = s2Key.trim()
      return api.put<Settings>('/settings', payload)
    },
    onSuccess: () => {
      invalidateQueries(
        qc,
        ['settings'],
        ['openalex-status'],
        ['semantic-scholar-status'],
        ['openalex-usage'],
      )
    },
  })

  const saveAndContinue = async () => {
    try {
      await save.mutateAsync()
    } catch (err) {
      errorToast('Could not save API settings', getApiErrorMessage(err))
    }
    next()
  }

  return (
    <StepShell
      eyebrow="Connect your sources"
      title="Two free keys, about a minute."
      lead="ALMa reads public metadata from OpenAlex and Semantic Scholar. OpenAlex now REQUIRES a key; Semantic Scholar works without one but badly. Both are free and take under a minute."
      footer={
        <StepNav
          onBack={back}
          onSkip={next}
          onContinue={saveAndContinue}
          continueLabel="Save & continue"
          continueLoading={save.isPending}
          hint="Skip if you like — ALMa will run, but OpenAlex stops after 100 credits a day and Semantic Scholar will stall on shared-pool rate limits. Add keys any time in Settings → Connections."
        />
      }
    >
      <div className="space-y-5">
        <ConceptCallout
          eyebrow="Why these keys, and what happens without them"
          summary="OpenAlex requires a key — without one you get 100 credits a day, then errors. Semantic Scholar works keyless but shares one global pool, which is what makes Discovery crawl."
        >
          <p className="mb-2">
            Both are open, free APIs — no payment, no approval wait. The keys are about
            <span className="font-medium text-alma-800"> rate limits</span>, which decide whether
            your library arrives in seconds or minutes.
          </p>
          <p className="mb-2">
            <span className="font-medium text-alma-800">OpenAlex — required.</span> Since
            February 2026 every request needs a key. Without one you get 100 credits per day and
            then hard <span className="font-mono text-[0.9em]">HTTP 409</span> errors, which stops
            metadata and author lookups mid-import. A free key raises that to 100,000 credits a day.
          </p>
          <p className="mb-2">
            <span className="font-medium text-alma-800">Semantic Scholar — optional but
            recommended.</span> Keyless requests share one anonymous pool with every other
            anonymous client worldwide, so 429s are constant — they are the direct cause of
            multi-minute stalls in Discovery. A key gives you a dedicated allowance.
          </p>
          <p>
            The contact email is courtesy, not a rate limit — OpenAlex retired its "polite pool".
            It identifies your requests to OpenAlex and Crossref and is sent
            <span className="font-medium text-alma-800"> only</span> to them, never anywhere else.
          </p>
        </ConceptCallout>

        <div className="space-y-2">
          <div className="flex items-center justify-between">
            <Label htmlFor="ob-email" className="text-slate-600">
              Contact email <span className="text-slate-400">(recommended)</span>
            </Label>
          </div>
          <Input
            id="ob-email"
            type="email"
            placeholder="you@university.edu"
            value={email}
            onChange={(e) => {
              emailDirty.current = true
              setEmail(e.target.value)
            }}
          />
        </div>

        <div className="space-y-2">
          <div className="flex items-center justify-between">
            <Label htmlFor="ob-oa-key" className="text-slate-600">
              OpenAlex API key <span className="text-critical-700">(required)</span>{' '}
              <a
                href="https://openalex.org/settings/api"
                target="_blank"
                rel="noreferrer"
                className="ml-1 font-normal text-alma-folio underline underline-offset-2"
              >
                Get an OpenAlex key ↗
              </a>
            </Label>
            <ConnectionDot
              configured={openalexStatus.data?.configured}
              valid={openalexStatus.data?.valid}
              loading={openalexStatus.isLoading || openalexStatus.isFetching}
            />
          </div>
          <Input
            id="ob-oa-key"
            type="password"
            placeholder={current.data?.openalex_api_key ? 'Saved — leave blank to keep' : 'openalex-...'}
            value={openalexKey}
            onChange={(e) => setOpenalexKey(e.target.value)}
            autoComplete="off"
          />
        </div>

        <div className="space-y-2">
          <div className="flex items-center justify-between">
            <Label htmlFor="ob-s2-key" className="text-slate-600">
              Semantic Scholar key <span className="text-slate-400">(optional)</span>{' '}
              <a
                href="https://www.semanticscholar.org/product/api"
                target="_blank"
                rel="noreferrer"
                className="ml-1 font-normal text-alma-folio underline underline-offset-2"
              >
                Request a Semantic Scholar key ↗
              </a>
            </Label>
            <ConnectionDot
              configured={s2Status.data?.configured}
              valid={s2Status.data?.valid}
              loading={s2Status.isLoading || s2Status.isFetching}
            />
          </div>
          <Input
            id="ob-s2-key"
            type="password"
            placeholder={current.data?.semantic_scholar_api_key ? 'Saved — leave blank to keep' : 's2-...'}
            value={s2Key}
            onChange={(e) => setS2Key(e.target.value)}
            autoComplete="off"
          />
        </div>
      </div>
    </StepShell>
  )
}
