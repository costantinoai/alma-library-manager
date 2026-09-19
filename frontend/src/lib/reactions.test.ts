import { describe, expect, it } from 'vitest'

import { reactionFromRating } from '@/lib/reactions'

describe('reactionFromRating', () => {
  it('mirrors the backend valence thresholds', () => {
    expect(reactionFromRating(5)).toBe('love')
    expect(reactionFromRating(4)).toBe('like')
    expect(reactionFromRating(3)).toBeNull() // plain save: membership, not praise
    expect(reactionFromRating(2)).toBe('dislike')
    expect(reactionFromRating(1)).toBe('dislike')
  })

  it('treats a missing rating as no reaction', () => {
    expect(reactionFromRating(0)).toBeNull()
    expect(reactionFromRating(null)).toBeNull()
    expect(reactionFromRating(undefined)).toBeNull()
  })
})
