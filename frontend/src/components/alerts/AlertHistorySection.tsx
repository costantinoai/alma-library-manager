import { useEffect, useState } from 'react'
import { keepPreviousData, useQuery } from '@tanstack/react-query'
import { Clock, Hash, Loader2 } from 'lucide-react'
import { api, type Alert, type AlertHistory } from '@/api/client'
import { Card, CardContent } from '@/components/ui/card'
import { Button } from '@/components/ui/button'
import { ErrorState } from '@/components/ui/ErrorState'
import { Badge } from '@/components/ui/badge'
import { LoadingState } from '@/components/ui/LoadingState'
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from '@/components/ui/select'
import { formatDate } from '@/lib/utils'
import { StatusBadge } from './AlertBadges'

// Page size for "Load More"; the API caps `limit` at 1000, so growth stops
// there instead of tripping a 422 after enough clicks.
const HISTORY_PAGE_SIZE = 20
const HISTORY_MAX_LIMIT = 1000

interface AlertHistorySectionProps {
  /** Pre-filter to one digest (set when the user clicks a digest's outcome
   * chip). The user can still change the filter afterwards. */
  initialAlertId?: string | null
}

export function AlertHistorySection({ initialAlertId }: AlertHistorySectionProps) {
  const [filterAlertId, setFilterAlertId] = useState(initialAlertId ?? '')
  const [limit, setLimit] = useState(HISTORY_PAGE_SIZE)

  // Follow later chip clicks too — each targets a (possibly different) digest.
  useEffect(() => {
    if (initialAlertId != null) {
      setFilterAlertId(initialAlertId)
      setLimit(HISTORY_PAGE_SIZE)
    }
  }, [initialAlertId])

  const alertsQuery = useQuery({
    queryKey: ['alerts'],
    queryFn: () => api.get<Alert[]>('/alerts/'),
    retry: 1,
  })

  const historyQuery = useQuery({
    queryKey: ['alert-history', filterAlertId, limit],
    queryFn: () => {
      let path = `/alerts/history?limit=${limit}&offset=0`
      if (filterAlertId) path += `&alert_id=${encodeURIComponent(filterAlertId)}`
      return api.get<AlertHistory[]>(path)
    },
    retry: 1,
    // "Load More" changes the query key; keep the current page on screen
    // instead of flashing the whole list back to a spinner.
    placeholderData: keepPreviousData,
  })

  const alerts = alertsQuery.data ?? []
  const history = historyQuery.data ?? []

  return (
    <div className="space-y-4">
      <div className="flex flex-col gap-4 sm:flex-row sm:items-center sm:justify-between">
        <h2 className="text-lg font-semibold text-alma-800">History</h2>
        <Select value={filterAlertId || 'all'} onValueChange={(value) => { setFilterAlertId(value === 'all' ? '' : value); setLimit(HISTORY_PAGE_SIZE) }}>
          <SelectTrigger className="w-56">
            <SelectValue placeholder="All Alerts" />
          </SelectTrigger>
          <SelectContent>
            <SelectItem value="all">All Alerts</SelectItem>
            {alerts.map((a) => (
              <SelectItem key={a.id} value={a.id}>{a.name}</SelectItem>
            ))}
          </SelectContent>
        </Select>
      </div>

      {historyQuery.isLoading ? (
        <LoadingState />
      ) : historyQuery.isError ? (
        <ErrorState message="Failed to load alert history." />
      ) : history.length === 0 ? (
        <div className="py-12 text-center">
          <Clock className="mx-auto h-12 w-12 text-slate-300" />
          <p className="mt-4 text-sm text-slate-500">No alert history yet</p>
        </div>
      ) : (
        <div className="space-y-3">
          {history.map((item) => (
            <Card key={item.id} className="transition-shadow hover:shadow-md">
              <CardContent className="p-4">
                <div className="flex items-start justify-between gap-4">
                  <div className="min-w-0 flex-1">
                    <div className="flex items-center gap-2">
                      <StatusBadge status={item.status} />
                      <Badge variant="outline">
                        <Hash className="mr-1 h-3 w-3" />
                        {item.channel}
                      </Badge>
                      {item.publication_count != null && (
                        <span className="text-xs text-slate-500">
                          {item.publication_count} paper{item.publication_count !== 1 ? 's' : ''}
                        </span>
                      )}
                    </div>
                    {item.message_preview && (
                      <p className="mt-2 text-sm text-slate-600">{item.message_preview}</p>
                    )}
                    {item.error_message && (
                      <p className="mt-1 text-xs text-critical-500">{item.error_message}</p>
                    )}
                    <p className="mt-1.5 text-xs text-slate-400">
                      {formatDate(item.sent_at)}
                    </p>
                  </div>
                </div>
              </CardContent>
            </Card>
          ))}

          {history.length >= limit && limit < HISTORY_MAX_LIMIT && (
            <div className="flex justify-center pt-2">
              <Button
                variant="outline"
                onClick={() => setLimit((prev) => Math.min(prev + HISTORY_PAGE_SIZE, HISTORY_MAX_LIMIT))}
                disabled={historyQuery.isFetching}
              >
                {historyQuery.isFetching && <Loader2 className="h-4 w-4 animate-spin" />}
                Load More
              </Button>
            </div>
          )}
        </div>
      )}
    </div>
  )
}
