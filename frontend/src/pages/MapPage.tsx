/**
 * MapPage — the top-level corpus map (task 50 M3, decision 50-A).
 *
 * The structure browser was buried three clicks deep (Library › Analytics ›
 * Map); it now owns a nav slot under Explore. v1 hosts the full corpus
 * explorer (GraphPanel locked to the paper map — scope toggle, cluster
 * drilldowns, citation-link layers, rebuild). The SemanticMap-based overlays
 * (cluster gap report, recency, reading coverage — 50-D) layer onto this
 * page next; the author network lives on the Authors page (50-C).
 */
import { Map as MapIcon } from 'lucide-react'

import { GraphPanel } from '@/components/graphs/GraphPanel'

export function MapPage() {
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
      <GraphPanel initialView="paper-map" lockedView />
    </div>
  )
}
