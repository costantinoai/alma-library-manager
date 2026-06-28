import { useEffect, useState } from 'react'
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query'
import { Gauge, Save } from 'lucide-react'

import {
  getGovernanceSettings,
  updateGovernanceSettings,
  type GovernanceSettings,
} from '@/api/client'
import {
  AsyncButton,
  SettingsCard,
  SettingsNumberField,
} from '@/components/settings/primitives'
import { ConceptCallout } from '@/components/ui/concept-callout'
import { invalidateQueries } from '@/lib/queryHelpers'
import { useToast, errorToast } from '@/hooks/useToast'

const DEFAULT_GOVERNANCE: GovernanceSettings = {
  idle_wait_minutes: 3,
  reserved_api_calls: 200,
}

/**
 * Background operations — how the unattended health/maintenance sweeps behave so
 * they never compete with you (task 37). Two knobs:
 *  - how long the app must be idle before a background sweep starts (it also pauses
 *    the moment you do anything), and
 *  - how many external-API calls it always leaves for your own manual operations.
 * KV-backed; the gate reads them live, so changes take effect without a restart.
 */
export function BackgroundOpsCard() {
  const queryClient = useQueryClient()
  const { toast } = useToast()

  const settingsQuery = useQuery({
    queryKey: ['governance-settings'],
    queryFn: getGovernanceSettings,
    staleTime: 30_000,
    retry: 1,
  })

  const [idleWaitMinutes, setIdleWaitMinutes] = useState(DEFAULT_GOVERNANCE.idle_wait_minutes)
  const [reservedApiCalls, setReservedApiCalls] = useState(DEFAULT_GOVERNANCE.reserved_api_calls)

  // Sync local form state when the server value arrives / changes.
  useEffect(() => {
    if (settingsQuery.data) {
      setIdleWaitMinutes(settingsQuery.data.idle_wait_minutes)
      setReservedApiCalls(settingsQuery.data.reserved_api_calls)
    }
  }, [settingsQuery.data])

  const dirty =
    !!settingsQuery.data &&
    (idleWaitMinutes !== settingsQuery.data.idle_wait_minutes ||
      reservedApiCalls !== settingsQuery.data.reserved_api_calls)

  const saveMutation = useMutation({
    mutationFn: () =>
      updateGovernanceSettings({
        idle_wait_minutes: idleWaitMinutes,
        reserved_api_calls: reservedApiCalls,
      }),
    onSuccess: async () => {
      await invalidateQueries(queryClient, ['governance-settings'], ['health', 'operations'])
      toast({
        title: 'Background operations updated',
        description: `Sweeps wait for ${idleWaitMinutes} min idle and reserve ${reservedApiCalls} API calls for you.`,
      })
    },
    onError: () => errorToast('Could not update background operations'),
  })

  return (
    <SettingsCard
      icon={Gauge}
      title="Background Operations"
      description="How the unattended health & maintenance sweeps yield to you."
    >
      <ConceptCallout
        eyebrow="What is this?"
        summary="Background sweeps run only while you're away, and always leave you some API budget."
      >
        ALMa's health/maintenance sweeps (identity resolution, metadata + vector
        backfill) run in the background. They never compete with you: a sweep starts
        only after the app has been idle for the wait below and no other operation is
        running, and it pauses the moment you open a page or start an operation —
        resuming once you're idle again. Sweeps that call an external provider also
        keep the reserve below free for your own manual work; if the quota nears that
        floor a sweep stops and the Health page tells you.
      </ConceptCallout>

      <SettingsNumberField
        label="Wait for idle (minutes)"
        description="How long the app must be idle before a background sweep may run. 0 = run as soon as nothing else is active."
        value={idleWaitMinutes}
        min={0}
        max={120}
        onChange={setIdleWaitMinutes}
      />

      <SettingsNumberField
        label="Reserve API calls for you"
        description="Calls a background sweep always leaves for your manual operations (OpenAlex daily quota)."
        value={reservedApiCalls}
        min={0}
        max={100000}
        onChange={setReservedApiCalls}
      />

      <div className="flex justify-end">
        <AsyncButton
          type="button"
          icon={<Save className="h-4 w-4" />}
          pending={saveMutation.isPending}
          disabled={!dirty || settingsQuery.isLoading}
          onClick={() => saveMutation.mutate()}
        >
          Save
        </AsyncButton>
      </div>
    </SettingsCard>
  )
}
