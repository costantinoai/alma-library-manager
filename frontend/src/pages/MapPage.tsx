/**
 * MapPage — the top-level corpus map (task 50 M3 / 50-A), on the ONE map
 * stack (50-K): `GraphMapView` → `SemanticMap`, the same toolbar/legend
 * idioms as the Discovery frontier and the Authors network. No second
 * renderer, no separate physics, no bespoke knob panel.
 *
 * Primary controls sit in the toolbar (scope, links, names, search);
 * everything occasional lives in ONE Advanced popover (cluster detail →
 * background variant build, Rebuild layout, Refresh labels) — progressive
 * disclosure, still all reachable (50-I).
 */
import { useState } from 'react'
import { useMutation, useQueryClient } from '@tanstack/react-query'
import { Map as MapIcon, Settings2 } from 'lucide-react'

import {
  api,
  getPaperById,
  refreshClusterLabels,
  type GraphNode,
  type Publication,
} from '@/api/client'
import { PaperDetailPanel } from '@/components/discovery'
import { GraphMapView } from '@/components/map/GraphMapView'
import { MapModeSwitch } from '@/components/map/MapChrome'
import { Button } from '@/components/ui/button'
import { Popover, PopoverContent, PopoverTrigger } from '@/components/ui/popover'
import { invalidateQueries } from '@/lib/queryHelpers'
import { useToast } from '@/hooks/useToast'

// The substrate is clustered at 1.5 (graph_substrate.SUBSTRATE_CLUSTER_
// RESOLUTION); other detail levels build a variant in the background (202).
const RESOLUTIONS = [1.0, 1.5, 2.0, 2.5] as const

export function MapPage() {
  const [scope, setScope] = useState<'library' | 'corpus'>('corpus')
  const [resolution, setResolution] = useState<number>(1.5)
  const [selectedPaper, setSelectedPaper] = useState<Publication | null>(null)
  const [detailOpen, setDetailOpen] = useState(false)
  const queryClient = useQueryClient()
  const { toast } = useToast()

  const rebuildMutation = useMutation({
    mutationFn: () => api.post<{ status?: string; job_id?: string }>(`/graphs/rebuild?scope=${scope}`),
    onSuccess: (result) => {
      void invalidateQueries(queryClient, ['graph'])
      toast({
        title: result?.status === 'queued' ? `Layout rebuild queued (${scope})` : `Layout rebuilt (${scope})`,
      })
    },
  })
  const relabelMutation = useMutation({
    mutationFn: () => refreshClusterLabels({ graph_type: 'paper_map', scope }),
    onSuccess: () => {
      void invalidateQueries(queryClient, ['graph'])
      toast({ title: 'Cluster relabelling queued', description: 'Watch Activity for progress.' })
    },
  })

  return (
    <div className="space-y-4 p-6">
      <header>
        <h1 className="flex items-center gap-2 text-2xl font-semibold text-alma-800">
          <MapIcon className="h-6 w-6 text-alma-600" />
          Map
        </h1>
        <p className="text-sm text-slate-500">
          Your corpus as territory — clusters, citation links, and where your library sits in it.
        </p>
      </header>

      <GraphMapView
        endpoint="paper-map"
        params={{ scope, cluster_resolution: String(resolution) }}
        nodeKind={(n) => (n.in_library === false ? 'corpus' : 'library')}
        onOpenNode={async (n: GraphNode) => {
          const pid = String(n.metadata?.paper_id ?? n.id)
          try {
            setSelectedPaper(await getPaperById(pid))
            setDetailOpen(true)
          } catch {
            /* stale node id — the next scheduled rebuild trues the payload */
          }
        }}
        hoverCard={(n) => (
          <>
            <p className="line-clamp-2 font-medium text-alma-800">{n.name}</p>
            <p className="mt-0.5 text-slate-500">
              {n.in_library === false ? 'Tracked' : 'In your library'}
              {n.metadata?.year ? ` · ${n.metadata.year}` : ''}
              {typeof n.metadata?.cited_by_count === 'number' ? ` · ${n.metadata.cited_by_count} citations` : ''}
            </p>
          </>
        )}
        height={620}
        toolbarExtras={
          <>
            <MapModeSwitch
              value={scope}
              onChange={setScope}
              options={[
                { value: 'corpus', label: 'Corpus', title: 'Every tracked paper' },
                { value: 'library', label: 'Library', title: 'Only papers you saved' },
              ]}
            />
            <Popover>
              <PopoverTrigger asChild>
                <button
                  type="button"
                  className="inline-flex items-center gap-1.5 rounded-sm border border-control-edge bg-control-well px-2.5 py-1 text-xs font-medium text-slate-600 hover:bg-control-quiet"
                  title="Occasional controls — cluster detail, rebuilds"
                >
                  <Settings2 className="h-3.5 w-3.5" />
                  Advanced
                </button>
              </PopoverTrigger>
              <PopoverContent align="start" className="w-64 space-y-3 text-xs">
                <div>
                  <p className="mb-1 font-medium text-alma-800">Cluster detail</p>
                  <div className="flex gap-1">
                    {RESOLUTIONS.map((r) => (
                      <button
                        key={r}
                        type="button"
                        onClick={() => setResolution(r)}
                        className={
                          resolution === r
                            ? 'rounded-sm bg-accent-soft px-2 py-1 font-medium text-alma-folio'
                            : 'rounded-sm border border-control-edge bg-control-well px-2 py-1 text-slate-600 hover:bg-control-quiet'
                        }
                      >
                        {r.toFixed(1)}×
                      </button>
                    ))}
                  </div>
                  <p className="mt-1 text-[11px] text-slate-400">
                    Non-default levels build in the background, then appear.
                  </p>
                </div>
                <div className="space-y-1.5 border-t border-[var(--color-border)] pt-2">
                  <Button size="sm" variant="outline" className="w-full" disabled={rebuildMutation.isPending} onClick={() => rebuildMutation.mutate()}>
                    Rebuild layout ({scope})
                  </Button>
                  <Button size="sm" variant="outline" className="w-full" disabled={relabelMutation.isPending} onClick={() => relabelMutation.mutate()}>
                    Refresh cluster labels
                  </Button>
                </div>
              </PopoverContent>
            </Popover>
          </>
        }
      />

      <PaperDetailPanel paper={selectedPaper} open={detailOpen} onOpenChange={setDetailOpen} />
    </div>
  )
}
