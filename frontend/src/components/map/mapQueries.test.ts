import { describe, expect, it } from 'vitest'

import type { GraphData } from '@/api/client'
import {
  graphQueryKey,
  retainReadyMapPayload,
  type MapBuildStatus,
} from './mapQueries'
import {
  FITTED_CAMERA,
  cameraToViewport,
  viewportToCamera,
} from './useMapViewport'

const graph: GraphData = {
  nodes: [],
  edges: [],
  metadata: { computed_at: '2026-07-26T12:00:00Z' },
}

describe('semantic map lifecycle primitives', () => {
  it('retains only the exact query key payload during a background build', () => {
    const build: MapBuildStatus = {
      status: 'building',
      job_id: 'job-1',
    }
    expect(retainReadyMapPayload({ payload: graph }, build)).toEqual({
      payload: graph,
      build,
    })
    expect(retainReadyMapPayload<GraphData>(undefined, build)).toEqual({
      payload: undefined,
      build,
    })
  })

  it('replaces a retained payload once the ready response arrives', () => {
    const next = { ...graph, metadata: { computed_at: 'later' } }
    expect(
      retainReadyMapPayload(
        {
          payload: graph,
          build: { status: 'building' },
        },
        next,
      ),
    ).toEqual({ payload: next })
  })

  it('canonicalises graph parameters so equivalent options share one cache entry', () => {
    expect(
      graphQueryKey('paper-map', {
        scope: 'corpus',
        cluster_resolution: '1.5',
      }),
    ).toEqual(
      graphQueryKey('paper-map', {
        cluster_resolution: '1.5',
        scope: 'corpus',
      }),
    )
  })

  it('round-trips a camera independently of plate dimensions', () => {
    const camera = { centerX: 0.33, centerY: 0.61, zoom: 3.2 }
    const wide = cameraToViewport(camera, 1200, 520)
    const restored = viewportToCamera(wide, 1200, 520)
    expect(restored.centerX).toBeCloseTo(camera.centerX)
    expect(restored.centerY).toBeCloseTo(camera.centerY)
    expect(restored.zoom).toBeCloseTo(camera.zoom)

    const resized = cameraToViewport(restored, 700, 700)
    expect(viewportToCamera(resized, 700, 700)).toEqual(restored)
    expect(cameraToViewport(FITTED_CAMERA, 700, 520).scale).toBe(456)
  })
})
