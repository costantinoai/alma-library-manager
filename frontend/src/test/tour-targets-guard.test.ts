import { describe, it, expect } from 'vitest'
import { readdirSync, readFileSync, statSync } from 'node:fs'
import { join } from 'node:path'

import { AUTHORS_TOUR, DISCOVERY_TOUR, FEED_TOUR, HOME_TOUR, LIBRARY_TOUR } from '@/components/onboarding/tours'

/**
 * tour-targets-guard — every page-tour step points at something a component
 * actually renders.
 *
 * The Discovery tour kept a step for `[data-tour="discovery-map"]` ("lasso a
 * region to explore it as a Direction") after the Map left this line
 * (43c3041): the tour described a feature that no longer exists, anchored on
 * an element no component renders. A removed or renamed surface must take its
 * tour step with it.
 */
const ROOT = join(process.cwd(), 'src')

function tsxSources(dir: string, acc: string[] = []): string[] {
  for (const entry of readdirSync(dir)) {
    const full = join(dir, entry)
    if (statSync(full).isDirectory()) tsxSources(full, acc)
    else if (full.endsWith('.tsx')) acc.push(full)
  }
  return acc
}

describe('tour-targets-guard', () => {
  it('every tour target is rendered by some component', () => {
    const source = tsxSources(ROOT).map((file) => readFileSync(file, 'utf8')).join('\n')
    const steps = [...HOME_TOUR, ...FEED_TOUR, ...DISCOVERY_TOUR, ...LIBRARY_TOUR, ...AUTHORS_TOUR]
    const missing = steps
      .map((step) => /data-tour="([^"]+)"/.exec(step.target ?? '')?.[1])
      .filter((key): key is string => !!key)
      .filter((key) => !source.includes(`"${key}"`) && !source.includes(`'${key}'`))
    expect(missing, `tour steps target elements nothing renders: ${missing.join(', ')}`).toEqual([])
  })
})
