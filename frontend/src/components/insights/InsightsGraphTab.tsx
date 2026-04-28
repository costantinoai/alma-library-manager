import { Brain, Settings } from 'lucide-react'

import { GraphPanel } from '@/components/graphs/GraphPanel'
import { Button } from '@/components/ui/button'
import { EmptyState } from '@/components/ui/empty-state'

interface InsightsGraphTabProps {
  embeddingsReady: boolean
}

export function InsightsGraphTab({ embeddingsReady }: InsightsGraphTabProps) {
  if (embeddingsReady) {
    return <GraphPanel />
  }

  return (
    <EmptyState
      icon={Brain}
      title="AI Embeddings Required"
      description="Graph visualizations require an embedding provider to compute semantic relationships between papers, authors, and topics."
      action={
        <Button
          variant="outline"
          size="sm"
          onClick={() => (window.location.hash = '#/settings')}
        >
          <Settings className="mr-2 h-4 w-4" />
          Configure AI Provider in Settings
        </Button>
      }
    />
  )
}
