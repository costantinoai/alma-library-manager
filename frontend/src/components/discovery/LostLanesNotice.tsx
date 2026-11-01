import { AlertTriangle } from 'lucide-react'

import { Alert, AlertDescription, AlertTitle } from '@/components/ui/alert'

export interface LostLane {
  lane: string
  label: string
  reason: string
}

/** Says a deck is missing a retrieval family, when the last refresh lost one. */
export function LostLanesNotice({ lanes }: { lanes: LostLane[] | undefined }) {
  if (!lanes?.length) return null
  const names = lanes.map((lane) => lane.label.split(' (')[0]).join(', ')
  return (
    <Alert variant="warning">
      <AlertTriangle className="h-4 w-4" />
      <AlertTitle>This deck is incomplete</AlertTitle>
      <AlertDescription>
        The last refresh was built without {names}, so papers only those lanes
        would find are missing. Refresh the lens to try again; Activity has the reason.
      </AlertDescription>
    </Alert>
  )
}
