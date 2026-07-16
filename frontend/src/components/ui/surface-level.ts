import { createContext, useContext } from 'react'

export type SurfaceLevel = 0 | 1 | 2 | 3 | 4

export const SurfaceLevelContext = createContext<SurfaceLevel>(0)

export function useSurfaceLevel(): SurfaceLevel {
  return useContext(SurfaceLevelContext)
}

export function nextLevel(level: SurfaceLevel): SurfaceLevel {
  return Math.min(level + 1, 4) as SurfaceLevel
}

// Literal class names are required so Tailwind can retain every ladder rung.
export const SURFACE_BG: Record<SurfaceLevel, string> = {
  0: 'bg-surface-0',
  1: 'bg-surface-1',
  2: 'bg-surface-2',
  3: 'bg-surface-3',
  4: 'bg-surface-4',
}

export const SURFACE_BORDER: Record<SurfaceLevel, string> = {
  0: 'border-edge-0',
  1: 'border-edge-1',
  2: 'border-edge-2',
  3: 'border-edge-3',
  4: 'border-edge-4',
}
