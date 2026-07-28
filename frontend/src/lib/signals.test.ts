import { describe, expect, it } from 'vitest'

import type { ScoreBreakdown, ScoreExplanation } from '@/api/client'
import { contributingFamilies, scoreExplanation, topFamilies } from './signals'

function family(key: string, points: number, available = true) {
  return {
    key,
    label: key,
    description: '',
    value: 0.5,
    weight: 0.1,
    points,
    available,
    atoms: [],
  }
}

const explanation: ScoreExplanation = {
  ranker_version: 'discovery-v4-family-prior',
  final_score: 30,
  families: [
    family('topic', 12),
    family('semantic', 18),
    // Never measured — must not render as a signal that scored zero.
    family('venue', 0, false),
    family('recency', 0),
  ],
  adjustments: [],
  clipped: 0,
}

describe('scoreExplanation', () => {
  it('returns null for a breakdown persisted before the current ranker', () => {
    expect(scoreExplanation({ final_score: 67 } as ScoreBreakdown)).toBeNull()
    expect(scoreExplanation(null)).toBeNull()
  })

  it('returns the explanation when present', () => {
    expect(scoreExplanation({ explanation } as ScoreBreakdown)).toBe(explanation)
  })
})

describe('contributingFamilies', () => {
  it('drops unmeasured and zero-point families, strongest first', () => {
    expect(contributingFamilies(explanation).map((f) => f.key)).toEqual(['semantic', 'topic'])
  })

  it('sums to the final score for a fully-contributing explanation', () => {
    const total = contributingFamilies(explanation).reduce((sum, f) => sum + f.points, 0)
    expect(total).toBe(explanation.final_score)
  })
})

describe('topFamilies', () => {
  it('limits to the strongest N', () => {
    expect(topFamilies({ explanation } as ScoreBreakdown, 1).map((f) => f.key)).toEqual([
      'semantic',
    ])
  })
})
