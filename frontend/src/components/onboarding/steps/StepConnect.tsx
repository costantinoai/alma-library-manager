import { useState } from 'react'
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query'
import { CheckCircle2, Loader2, XCircle } from 'lucide-react'
import { Input } from '@/components/ui/input'
import { Label } from '@/components/ui/label'
import { StatusBadge } from '@/components/ui/status-badge'
import { ConceptCallout } from '@/components/ui/concept-callout'
import { invalidateQueries } from '@/lib/queryHelpers'
import {
  api,
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
  // Seed email once from the loaded settings (only if the user hasn't typed).
  const seededEmail = current.data?.openalex_email ?? ''
  const emailValue = email || seededEmail

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
        openalex_email: emailValue.trim(),
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
    } catch {
      /* surfaced by the dots; don't block the flow */
    }
    next()
  }

  return (
    <StepShell
      eyebrow="Connect your sources"
      title="A couple of keys make this much faster."
      lead="ALMa reads public metadata from OpenAlex and Semantic Scholar. A free OpenAlex key and a contact email get you the fast lane; a Semantic Scholar key is optional but helps."
      footer={
        <StepNav
          onBack={back}
          onSkip={next}
          onContinue={saveAndContinue}
          continueLabel="Save & continue"
          continueLoading={save.isPending}
          hint="You can skip this — but without a key, downloading your papers and suggestions can be much slower. You can always add keys later in Settings."
        />
      }
    >
      <div className="space-y-5">
        <ConceptCallout
          eyebrow="Why a key?"
          summary="OpenAlex now expects an API key for reliable access; the email joins their polite pool."
        >
          <p>
            OpenAlex and Semantic Scholar are open, free APIs. A key authenticates you for a higher,
            steadier rate limit, and a contact email puts your requests in the "polite pool" — both
            mean your library and suggestions arrive in seconds rather than minutes.
          </p>
          <p>
            Your email is sent <span className="font-medium text-alma-800">only</span> to OpenAlex /
            Semantic Scholar alongside those lookups. It is never shared anywhere else. Get a free key
            at{' '}
            <a
              href="https://openalex.org/"
              target="_blank"
              rel="noreferrer"
              className="text-alma-folio underline underline-offset-2"
            >
              openalex.org
            </a>
            .
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
            value={emailValue}
            onChange={(e) => setEmail(e.target.value)}
          />
        </div>

        <div className="space-y-2">
          <div className="flex items-center justify-between">
            <Label htmlFor="ob-oa-key" className="text-slate-600">
              OpenAlex API key
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
              Semantic Scholar key <span className="text-slate-400">(optional)</span>
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
