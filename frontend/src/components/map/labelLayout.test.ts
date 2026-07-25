/**
 * 50-H: map words never overlap, placement is deterministic, and a label that
 * finds no ground is dropped — never stacked on a neighbour.
 */
import { describe, expect, it } from 'vitest'

import { placeLabels, toponymTerms, type LabelInput } from './labelLayout'

const box = (id: string, x: number, y: number, priority: number, w = 100, h = 14): LabelInput => ({
  id,
  x,
  y,
  width: w,
  height: h,
  priority,
})

function overlapping(a: { left: number; top: number; width: number; height: number }, b: typeof a) {
  return (
    a.left < b.left + b.width &&
    a.left + a.width > b.left &&
    a.top < b.top + b.height &&
    a.top + a.height > b.top
  )
}

describe('placeLabels', () => {
  it('places non-competing labels at their anchors', () => {
    const placed = placeLabels([box('a', 100, 100, 5), box('b', 400, 300, 3)], 800, 600)
    expect(placed).toHaveLength(2)
    expect(placed.every((p) => p.slot === 'center')).toBe(true)
  })

  it('never returns two overlapping boxes', () => {
    // A pile of labels fighting for the same spot.
    const inputs = Array.from({ length: 12 }, (_, i) => box(`l${i}`, 200 + i * 3, 200 + i * 2, 12 - i))
    const placed = placeLabels(inputs, 800, 600)
    for (let i = 0; i < placed.length; i++) {
      for (let j = i + 1; j < placed.length; j++) {
        const a = { left: placed[i].left, top: placed[i].top, width: placed[i].width, height: placed[i].height }
        const b = { left: placed[j].left, top: placed[j].top, width: placed[j].width, height: placed[j].height }
        expect(overlapping(a, b)).toBe(false)
      }
    }
    // And the losers were dropped, not stacked.
    expect(placed.length).toBeLessThan(inputs.length)
  })

  it('the highest-priority label always gets its preferred spot', () => {
    const placed = placeLabels(
      [box('small', 300, 300, 1), box('big', 300, 300, 100)],
      800,
      600,
    )
    const big = placed.find((p) => p.id === 'big')
    expect(big?.slot).toBe('center')
  })

  it('is deterministic — same input, same plate, regardless of input order', () => {
    const inputs = [box('b', 210, 200, 5), box('a', 200, 200, 5), box('c', 220, 205, 4)]
    const one = placeLabels(inputs, 800, 600)
    const two = placeLabels([...inputs].reverse(), 800, 600)
    expect(one).toEqual(two)
  })

  it('drops labels that would land outside the viewport', () => {
    const placed = placeLabels([box('edge', 4, 4, 10, 300, 20)], 200, 100)
    // 300px wide label near the corner of a 200px screen: no candidate fits.
    expect(placed).toHaveLength(0)
  })
})

describe('toponymTerms', () => {
  it('splits into SEPARATE terms — never a joined string', () => {
    expect(toponymTerms('lateral, visual, stream, level')).toEqual([
      'lateral',
      'visual',
      'stream',
    ])
  })
  it('drops fully redundant later terms', () => {
    expect(toponymTerms('visual, visual, stream')).toEqual(['visual', 'stream'])
    // "level visual" brings a new word after "visual" → kept.
    expect(toponymTerms('visual, level visual, stream')).toEqual([
      'visual',
      'level visual',
      'stream',
    ])
  })
  it('caps each term on a word edge', () => {
    const [t] = toponymTerms('anterior temporal lobe semantics, x')
    expect(t.length).toBeLessThanOrEqual(17)
    expect(t).toBe('anterior')
  })
})
