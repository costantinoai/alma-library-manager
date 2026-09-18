import { QueryClient } from '@tanstack/react-query'
import { describe, expect, it } from 'vitest'

import { invalidateAfterSignalLabMutation } from './queryHelpers'

describe('Signal Lab invalidation', () => {
  it.each([false, true])('refreshes learned state; reset decks = %s', async (resetDecks) => {
    const client = new QueryClient()
    const affected = [
      ['signal-lab', 'summary'], ['signal-lab', 'model'], ['signal-lab', 'eval'],
      ['home-brief'], ['home'], ['signal-field', 'corpus'], ['author-field'],
    ]
    const queue = ['signal-lab', 'queue', 'triplet_best_worst', 12]
    const graph = ['graph', 'corpus']
    for (const key of [...affected, queue, graph]) client.setQueryData(key, { ready: true })
    await invalidateAfterSignalLabMutation(client, { resetDecks })
    for (const key of affected) expect(client.getQueryState(key)?.isInvalidated).toBe(true)
    expect(client.getQueryState(queue)?.isInvalidated).toBe(resetDecks)
    expect(client.getQueryState(graph)?.isInvalidated).toBe(false)
    client.clear()
  })
})
