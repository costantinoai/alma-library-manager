import { render, screen } from '@testing-library/react'
import { QueryClient, QueryClientProvider } from '@tanstack/react-query'
import { expect, it, vi } from 'vitest'

import { AboutCard } from './AboutCard'

vi.mock('@/api/client', async (original) => ({
  ...(await original<typeof import('@/api/client')>()),
  getBootstrap: vi.fn(async () => ({ app: { version: '0.24.2' } })),
}))

it('shows the running version and the real name', async () => {
  render(
    <QueryClientProvider client={new QueryClient()}>
      <AboutCard />
    </QueryClientProvider>,
  )

  expect(await screen.findByText(/0\.24\.2/)).toBeInTheDocument()
  expect(screen.getByText(/Another Library Manager/)).toBeInTheDocument()
  expect(screen.queryByText(/2\.0\.0/)).not.toBeInTheDocument()
})
