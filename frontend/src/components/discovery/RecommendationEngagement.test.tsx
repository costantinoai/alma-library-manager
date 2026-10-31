import { QueryClient, QueryClientProvider } from '@tanstack/react-query'
import { render, screen } from '@testing-library/react'
import { describe, expect, it, vi } from 'vitest'

import { api } from '@/api/client'

import { RecommendationEngagement } from './RecommendationEngagement'

vi.mock('@/api/client', async (importOriginal) => {
  const actual = await importOriginal<typeof import('@/api/client')>()
  return { ...actual, api: { ...actual.api, get: vi.fn() } }
})

describe('RecommendationEngagement', () => {
  it('prints a 0 to 100 score as it is', async () => {
    vi.mocked(api.get).mockResolvedValue({
      total: 50,
      seen: 40,
      liked: 3,
      dismissed: 2,
      engagement_rate: 0.1,
      by_lens: [{ lens_id: 'l1', count: 50, avg_score: 59.58 }],
    })
    render(
      <QueryClientProvider client={new QueryClient()}>
        <RecommendationEngagement lensNames={new Map([['l1', 'Chess']])} />
      </QueryClientProvider>,
    )

    expect(await screen.findByText('avg 60%')).toBeInTheDocument()
  })
})
