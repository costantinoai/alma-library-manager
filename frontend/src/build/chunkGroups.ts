const GROUPS: ReadonlyArray<readonly [chunk: string, packages: readonly string[]]> = [
  [
    'vendor-graph',
    [
      'react-force-graph-2d',
      'force-graph',
      'float-tooltip',
      'three',
      'three-forcegraph',
      'three-render-objects',
    ],
  ],
  [
    'vendor-charts',
    [
      'recharts',
      'recharts-scale',
      'react-smooth',
      'victory-vendor',
      'decimal.js-light',
    ],
  ],
  [
    'vendor-d3',
    [
      'd3-array',
      'd3-binarytree',
      'd3-color',
      'd3-dispatch',
      'd3-drag',
      'd3-ease',
      'd3-force-3d',
      'd3-format',
      'd3-interpolate',
      'd3-octree',
      'd3-path',
      'd3-quadtree',
      'd3-scale',
      'd3-scale-chromatic',
      'd3-selection',
      'd3-shape',
      'd3-time',
      'd3-time-format',
      'd3-timer',
      'd3-transition',
      'd3-zoom',
    ],
  ],
  ['vendor-motion', ['framer-motion', 'motion-dom', 'motion-utils']],
  ['vendor-query', ['@tanstack/react-query', '@tanstack/query-core']],
  [
    'vendor-ui',
    ['@radix-ui', 'cmdk', 'lucide-react', 'next-themes', 'sonner', 'vaul'],
  ],
  ['vendor-forms', ['@hookform/resolvers', 'react-hook-form', 'zod']],
  ['vendor-dnd', ['@dnd-kit']],
  [
    'vendor-react',
    ['react', 'react-dom', 'react-router-dom', 'scheduler', 'use-sync-external-store'],
  ],
]

function containsPackage(moduleId: string, packageName: string): boolean {
  return moduleId.includes(`/node_modules/${packageName}/`)
}

/** Stable package-family boundaries used by Vite's Rollup build. */
export function vendorChunk(rawModuleId: string): string | undefined {
  const moduleId = rawModuleId.replace(/\\/g, '/')
  if (!moduleId.includes('/node_modules/')) return undefined

  for (const [chunk, packages] of GROUPS) {
    if (packages.some((packageName) => containsPackage(moduleId, packageName))) return chunk
  }
  return 'vendor-common'
}
