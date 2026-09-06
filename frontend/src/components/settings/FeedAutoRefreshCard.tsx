import { useEffect, useState } from 'react'
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query'
import { RefreshCw, Save } from 'lucide-react'

import { getFeedSettings, updateFeedSettings, type FeedSettings } from '@/api/client'
import { AsyncButton } from '@/components/settings/primitives'
import { ErrorState } from '@/components/ui/ErrorState'
import { PageSection } from '@/components/ui/page-section'
import { Input } from '@/components/ui/input'
import { Switch } from '@/components/ui/switch'
import { invalidateQueries } from '@/lib/queryHelpers'
import { useToast, errorToast } from '@/hooks/useToast'

const DEFAULT_FEED_SETTINGS: FeedSettings = {
  auto_refresh_enabled: false,
  refresh_interval_hours: 6,
}

/** The one schedule editor, hosted by Feed's Tune monitors disclosure. */
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
    <PageSection id="feed-refresh-schedule" title="Refresh schedule" icon={RefreshCw}>
      {settingsQuery.isError && <ErrorState message="Feed schedule could not be loaded." actionLabel="Retry" onAction={() => { void settingsQuery.refetch() }} />}
      <div className="flex flex-wrap items-center gap-x-6 gap-y-3">
        <label className="inline-flex items-center gap-2 text-sm text-alma-800">
          <Switch checked={enabled} onCheckedChange={setEnabled} disabled={!settingsQuery.data || saveMutation.isPending} />
          Automatic refresh
        </label>
        <label className="inline-flex items-center gap-2 text-sm text-slate-600">
          Every
          <Input aria-label="Refresh interval in hours" type="number" className="w-20" min={1} max={168}
            value={intervalHours} onChange={event => setIntervalHours(Number(event.target.value))}
            disabled={!settingsQuery.data || saveMutation.isPending} />
          hours
        </label>
        <AsyncButton type="button" size="sm" variant="outline" icon={<Save className="h-3.5 w-3.5" />}
          pending={saveMutation.isPending}
          disabled={!dirty || !settingsQuery.data || !Number.isFinite(intervalHours) || intervalHours < 1 || intervalHours > 168}
          onClick={() => saveMutation.mutate()}>Save schedule</AsyncButton>
        <span className="text-xs text-slate-500" role="status">
          {dirty ? 'Unsaved schedule changes' : enabled ? 'Checks run in the background.' : 'Refresh manually whenever you want.'}
        </span>
      </div>
    </PageSection>
  )
}
