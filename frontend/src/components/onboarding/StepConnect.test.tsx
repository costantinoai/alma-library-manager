/**
 * The API-key step has to be actionable and truthful.
 *
 * Onboarding is the only place most people will ever read about these keys, so
 * three things must survive refactors:
 *
 *   1. **A way to get each key.** The step used to link the OpenAlex homepage
 *      (not the key page) and offered no Semantic Scholar link at all, so
 *      "get a key" meant "go and find it yourself".
 *   2. **What actually breaks without one.** OpenAlex is REQUIRED since
 *      2026-02-13 — keyless means 100 credits/day then HTTP 409 — and keyless
 *      S2 shares one global anonymous pool, which is the documented cause of
 *      multi-minute Discovery stalls. The step previously said the key was
 *      merely "expected" and that the email joined a "polite pool" OpenAlex has
 *      since retired: a reassurance that was no longer true.
 *   3. **A way out.** Neither key blocks setup; both are addable later.
 *
 * `docs/reference/external-apis.md` is the source of truth for all three.
 */
import { QueryClient, QueryClientProvider } from '@tanstack/react-query'
import { render, screen } from '@testing-library/react'
import { describe, expect, it, vi } from 'vitest'

import { StepConnect } from './steps/StepConnect'

vi.mock('@/api/client', async (importOriginal) => ({
  ...(await importOriginal<typeof import('@/api/client')>()),
  api: {
    get: vi.fn().mockResolvedValue({}),
    put: vi.fn().mockResolvedValue({}),
    post: vi.fn().mockResolvedValue({}),
  },
}))

function renderStep() {
  const client = new QueryClient({ defaultOptions: { queries: { retry: false } } })
  return render(
    <QueryClientProvider client={client}>
      <StepConnect
        state={{ step: 3 } as never}
        patch={vi.fn()}
        next={vi.fn()}
        back={vi.fn()}
        finish={vi.fn()}
        finishing={false}
        total={8}
      />
    </QueryClientProvider>,
  )
}

describe('onboarding: connect your sources', () => {
  it('links straight to both key pages', () => {
    renderStep()
    expect(screen.getByRole('link', { name: /Get an OpenAlex key/ })).toHaveAttribute(
      'href',
      'https://openalex.org/settings/api',
    )
    expect(
      screen.getByRole('link', { name: /Request a Semantic Scholar key/ }),
    ).toHaveAttribute('href', 'https://www.semanticscholar.org/product/api')
  })

  it('marks which key is required WITHOUT needing the fold opened', () => {
    renderStep()
    expect(screen.getByText(/\(required\)/)).toBeInTheDocument()
    expect(screen.getByText(/\(optional\)/)).toBeInTheDocument()
  })

  it('states the cost of skipping each key in the always-visible summary', () => {
    renderStep()
    // The detail lives behind the explainer fold; the SUMMARY is what everyone
    // reads, so the consequence has to be there and not one click away.
    const summary = screen.getByText(/OpenAlex requires a key/)
    expect(summary).toHaveTextContent(/100 credits a day, then errors/)
    expect(summary).toHaveTextContent(/shares one global pool/)
    expect(summary).toHaveTextContent(/makes Discovery crawl/)
  })

  it('explains the detail once the fold is opened', async () => {
    const { default: userEvent } = await import('@testing-library/user-event')
    renderStep()
    await userEvent.click(screen.getByText(/Why these keys/))
    expect(screen.getByText(/HTTP 409/)).toBeInTheDocument()
    expect(screen.getByText(/429s are constant/)).toBeInTheDocument()
    // …and does not resurrect the polite pool OpenAlex has retired.
    expect(screen.getByText(/retired its "polite pool"/)).toBeInTheDocument()
  })

  it('can be skipped, and says where to add the keys later', () => {
    renderStep()
    expect(screen.getByRole('button', { name: /Skip/i })).toBeInTheDocument()
    expect(screen.getByText(/Settings → Connections/)).toBeInTheDocument()
  })
})
