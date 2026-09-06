/** The Discovery weights card is truthful while editing, not only after save.
 *
 * The bug this file exists for (2026-09-06): the slider total was memoised on
 * `values.weights`, an object react-hook-form mutates in place, so the total
 * never re-computed and every row's live "share" was read over the PRE-edit
 * total. Typing 0.9 into Recency showed 82% (0.9 / 1.1) instead of 47%
 * (0.9 / 1.9) until the next save.
 */
import { QueryClient, QueryClientProvider } from '@tanstack/react-query'
import { fireEvent, render, screen, waitFor, within } from '@testing-library/react'
import { beforeEach, describe, expect, it, vi } from 'vitest'

import { DiscoveryWeightsCard } from './DiscoveryWeightsCard'

const get = vi.fn()
const put = vi.fn()

// PARTIAL mock: keep every other client export and drive only the two calls
// the card makes.
vi.mock('@/api/client', async (importOriginal) => ({
  ...(await importOriginal<typeof import('@/api/client')>()),
  api: {
    get: (...args: unknown[]) => get(...args),
    put: (...args: unknown[]) => put(...args),
    post: vi.fn(),
  },
}))

// The shipped defaults (sum 1.1) and what the ranker makes of them.
const WEIGHTS = {
  source_relevance: 0.15,
  topic_score: 0.2,
  text_similarity: 0.2,
  author_affinity: 0.15,
  journal_affinity: 0.05,
  recency_boost: 0.1,
  citation_quality: 0.05,
  feedback_adj: 0.1,
  preference_affinity: 0.1,
}
const SAVED = {
  weights: WEIGHTS,
  effective_weights: {
    semantic: 0.1273,
    lexical: 0.0545,
    topic: 0.1818,
    retrieval: 0.1364,
    author: 0.1364,
    recency: 0.0909,
    citation: 0.0455,
    feedback: 0.0909,
    preference: 0.0909,
    venue: 0.0455,
  },
  reference_score: 55.0,
}
// Recency raised to 0.9: the total becomes 1.9 and the reference drops.
const RAISED = {
  weights: { ...WEIGHTS, recency_boost: 0.9 },
  effective_weights: { ...SAVED.effective_weights, recency: 0.4737, topic: 0.1053 },
  reference_score: 45.3,
}

function renderCard() {
  const client = new QueryClient({ defaultOptions: { queries: { retry: false } } })
  return render(
    <QueryClientProvider client={client}>
      <DiscoveryWeightsCard />
    </QueryClientProvider>,
  )
}

/** The slider row for one family: its title paragraph and its number input. */
function row(label: string) {
  const title = screen.getByText((_, el) => el?.tagName === 'P' && (el.textContent ?? '').startsWith(label))
  const box = title.parentElement!.parentElement!.parentElement!
  return { title, input: within(box).getByRole('spinbutton') }
}

describe('DiscoveryWeightsCard', () => {
  beforeEach(() => {
    get.mockReset().mockResolvedValue(SAVED)
    put.mockReset().mockResolvedValue(RAISED)
  })

  it('reads the saved shares and the reference from the API', async () => {
    renderCard()
    await screen.findByText('Typical paper scores 55')
    expect(row('Recency').title).toHaveTextContent(/^Recency\s*share 9%$/)
    expect(row('Topic').title).toHaveTextContent(/^Topic\s*share 18%$/)
  })

  it('recomputes every share live over the NEW total while editing', async () => {
    renderCard()
    await screen.findByText('Typical paper scores 55')

    fireEvent.change(row('Recency').input, { target: { value: '0.9' } })

    await waitFor(() => expect(row('Recency').title).toHaveTextContent('share 47% · saved 9%'))
    // Not this row alone: the others shrink with the same total, and each
    // discloses what it still gets under the saved weights.
    expect(row('Topic').title).toHaveTextContent('share 11% · saved 18%')
    // The header reports SAVED weights until a save happens.
    expect(screen.getByText('Typical paper scores 55')).toBeInTheDocument()
  })

  it('after save the card re-reads what the ranker now uses', async () => {
    renderCard()
    await screen.findByText('Typical paper scores 55')
    fireEvent.change(row('Recency').input, { target: { value: '0.9' } })
    await waitFor(() => expect(row('Recency').title).toHaveTextContent('saved 9%'))

    fireEvent.click(screen.getByRole('button', { name: 'Save Discovery Settings' }))

    await screen.findByText('Typical paper scores 45')
    expect(put).toHaveBeenCalledWith(
      '/discovery/settings',
      expect.objectContaining({ weights: expect.objectContaining({ recency_boost: 0.9 }) }),
    )
    // Saved share now equals the share, so the suffix goes away.
    expect(row('Recency').title).toHaveTextContent(/^Recency\s*share 47%$/)
  })
})
