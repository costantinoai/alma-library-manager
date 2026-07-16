import { describe, expect, it } from 'vitest'
import { vendorChunk } from './chunkGroups'

describe('vendorChunk', () => {
  it.each([
    ['/repo/node_modules/react/index.js', 'vendor-react'],
    ['/repo/node_modules/@radix-ui/react-dialog/dist/index.js', 'vendor-ui'],
    ['/repo/node_modules/recharts/es6/index.js', 'vendor-charts'],
    ['/repo/node_modules/d3-scale/src/index.js', 'vendor-d3'],
    ['/repo/node_modules/react-force-graph-2d/dist/index.js', 'vendor-graph'],
    ['/repo/node_modules/three/build/three.module.js', 'vendor-graph'],
    ['/repo/node_modules/@tanstack/react-query/build/index.js', 'vendor-query'],
    ['/repo/node_modules/framer-motion/dist/index.js', 'vendor-motion'],
    ['/repo/node_modules/zod/index.js', 'vendor-forms'],
    ['/repo/node_modules/clsx/dist/clsx.js', 'vendor-common'],
  ])('maps %s to %s', (moduleId, expected) => {
    expect(vendorChunk(moduleId)).toBe(expected)
  })

  it('leaves application modules in their route chunks', () => {
    expect(vendorChunk('/repo/src/pages/LibraryPage.tsx')).toBeUndefined()
  })
})
