import { QueryClient, QueryClientProvider } from '@tanstack/react-query'
import { fireEvent, render, screen } from '@testing-library/react'
import { beforeEach, describe, expect, it, vi } from 'vitest'

import { LensWeightsPanel } from './LensWeightsPanel'

const get = vi.fn()

vi.mock('@/api/client', async (importOriginal) => ({
  ...(await importOriginal<typeof import('@/api/client')>()),
  api: { get: (...args: unknown[]) => get(...args) },
}))

describe('LensWeightsPanel', () => {
  beforeEach(() => {
    get.mockReset().mockResolvedValue({ capability_tiers: { tier1_embeddings: { ready: true } } })
  })

  it('shows lens base weights without claiming a global learned multiplier', async () => {
    const onSave = vi.fn()
    const client = new QueryClient({ defaultOptions: { queries: { retry: false } } })
    render(
      <QueryClientProvider client={client}>
        <LensWeightsPanel
          lens={{ id: 'lens-1', name: 'Lens', context_type: 'library_global', created_at: '', is_active: true, signal_count: 0, recommendation_count: 0, weights: { lexical: 0.4, vector: 0.3, graph: 0.2, external: 0.1 } }}
          onSave={onSave}
        />
      </QueryClientProvider>,
    )

    expect(await screen.findByText('Base mix of retrieval channels for this lens.')).toBeInTheDocument()
    expect(get).toHaveBeenCalledWith('/ai/status')
    expect(get).toHaveBeenCalledTimes(1)
    expect(screen.queryByTestId('channel-yield-note')).not.toBeInTheDocument()

    fireEvent.click(screen.getByRole('button', { name: 'Save Weights' }))
    expect(onSave).toHaveBeenCalledWith({ lexical: 0.4, vector: 0.3, graph: 0.2, external: 0.1 })
  })
})
