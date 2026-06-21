import { useEffect, useState } from 'react'
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query'
import { RefreshCw, Save } from 'lucide-react'

import { getFeedSettings, updateFeedSettings, type FeedSettings } from '@/api/client'
import {
  AsyncButton,
  SettingsCard,
  SettingsNumberField,
  ToggleRow,
} from '@/components/settings/primitives'
import { ConceptCallout } from '@/components/ui/concept-callout'
import { invalidateQueries } from '@/lib/queryHelpers'
import { useToast, errorToast } from '@/hooks/useToast'

const DEFAULT_FEED_SETTINGS: FeedSettings = {
  auto_refresh_enabled: false,
  refresh_interval_hours: 6,
}

/**
 * Feed auto-refresh settings — the detailed control surface for the opt-in
 * background feed refresh (the page-level toggle lives on the Feed page; this
 * card owns the interval and mirrors the same setting).
 *
 * Off by default. When enabled, the backend scheduler refreshes the inbox on
 * the interval without blocking the UI — the run shows in Activity and new
 * items appear automatically.
 */
export function FeedAutoRefreshCard() {
  const queryClient = useQueryClient()
  const { toast } = useToast()

  const settingsQuery = useQuery({
    queryKey: ['feed-settings'],
    queryFn: getFeedSettings,
    staleTime: 30_000,
    retry: 1,
  })

  const [enabled, setEnabled] = useState(DEFAULT_FEED_SETTINGS.auto_refresh_enabled)
  const [intervalHours, setIntervalHours] = useState(DEFAULT_FEED_SETTINGS.refresh_interval_hours)

  // Sync local form state when the server value arrives / changes.
  useEffect(() => {
    if (settingsQuery.data) {
      setEnabled(settingsQuery.data.auto_refresh_enabled)
      setIntervalHours(settingsQuery.data.refresh_interval_hours)
    }
  }, [settingsQuery.data])

  const dirty =
    !!settingsQuery.data &&
    (enabled !== settingsQuery.data.auto_refresh_enabled ||
      intervalHours !== settingsQuery.data.refresh_interval_hours)

  const saveMutation = useMutation({
    mutationFn: () =>
      updateFeedSettings({
        auto_refresh_enabled: enabled,
        refresh_interval_hours: intervalHours,
      }),
    onSuccess: async (saved) => {
      await invalidateQueries(queryClient, ['feed-settings'], ['activity-operations'])
      toast({
        title: 'Feed auto-refresh updated',
        description: saved.auto_refresh_enabled
          ? `The feed inbox will refresh in the background every ${saved.refresh_interval_hours}h.`
          : 'Automatic feed refresh is off.',
      })
    },
    onError: () => errorToast('Could not update feed auto-refresh'),
  })

  return (
    <SettingsCard
      icon={RefreshCw}
      title="Feed Auto-Refresh"
      description="Let ALMa check your monitors for new papers on a schedule instead of refreshing by hand."
    >
      <ConceptCallout
        eyebrow="What is this?"
        summary="Opt-in background refresh of the feed inbox — off by default, never blocks the page."
      >
        When enabled, ALMa fetches new matches from your active monitors every few
        hours in the background. The run appears in Activity and new items show up in
        the inbox automatically — you never have to wait on it. Leave it off to keep
        refreshing manually with the Refresh button on the Feed page.
      </ConceptCallout>

      <ToggleRow
        title="Auto-refresh the feed inbox"
        description="Check active monitors for new papers on the interval below."
        checked={enabled}
        disabled={settingsQuery.isLoading}
        onCheckedChange={setEnabled}
      />

      <SettingsNumberField
        label="Refresh Interval (Hours)"
        description="How often to refresh when auto-refresh is enabled."
        value={intervalHours}
        min={0}
        max={168}
        onChange={setIntervalHours}
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
