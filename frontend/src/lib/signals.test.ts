import { describe, expect, it } from 'vitest'

import { scoreSignalEntries } from './signals'

describe('scoreSignalEntries', () => {
  it('keeps signal objects and ignores diagnostic scalar/null fields', () => {
    const topic = { value: 0.8, weight: 0.5, weighted: 0.4 }

    expect(scoreSignalEntries({
      topic_score: topic,
      final_score: 67,
      text_similarity_mode: 'hybrid',
      optional_diagnostic: null,
      incomplete: { weighted: 0.2 },
    })).toEqual([['topic_score', topic]])
  })
})
