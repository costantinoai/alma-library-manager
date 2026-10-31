import { QueryClient, QueryClientProvider } from '@tanstack/react-query'
import { fireEvent, render, screen, waitFor } from '@testing-library/react'
import { beforeEach, describe, expect, it, vi } from 'vitest'

import { AddonsSection } from './AddonsSection'

const client = vi.hoisted(() => ({
  listAddons: vi.fn(),
  getAddon: vi.fn(),
  setAddonEnabled: vi.fn(),
  removeAddon: vi.fn(),
}))

vi.mock('@/api/client', async (importOriginal) => ({
  ...(await importOriginal<typeof import('@/api/client')>()),
  ...client,
}))

const addon = {
  id: 'fake',
  name: 'Fake',
  description: 'A fake add-on.',
  docs_path: 'concepts/fake/',
  default_enabled: false,
  requires: [],
  enabled: true,
  blocked: null,
  has_data: true,
  disable_warning: null as string | null,
}

function renderSection() {
  const queryClient = new QueryClient({ defaultOptions: { queries: { retry: false } } })
  render(
    <QueryClientProvider client={queryClient}>
      <AddonsSection />
    </QueryClientProvider>,
  )
}

describe('AddonsSection', () => {
  beforeEach(() => {
    vi.clearAllMocks()
    client.setAddonEnabled.mockResolvedValue({ ...addon, enabled: false })
  })

  it('switches off at once when it costs nothing', async () => {
    client.listAddons.mockResolvedValue([addon])
    renderSection()
    fireEvent.click(await screen.findByRole('switch', { name: 'Switch off Fake' }))
    await waitFor(() => expect(client.setAddonEnabled).toHaveBeenCalledWith('fake', false))
  })

  it('states the cost first, and Cancel changes nothing', async () => {
    client.listAddons.mockResolvedValue([{ ...addon, disable_warning: 'Nothing computes vectors.' }])
    renderSection()
    fireEvent.click(await screen.findByRole('switch', { name: 'Switch off Fake' }))
    expect(await screen.findByText('Nothing computes vectors.')).toBeInTheDocument()
    fireEvent.click(screen.getByRole('button', { name: 'Cancel' }))
    expect(client.setAddonEnabled).not.toHaveBeenCalled()

    fireEvent.click(await screen.findByRole('switch', { name: 'Switch off Fake' }))
    fireEvent.click(await screen.findByRole('button', { name: 'Switch off' }))
    await waitFor(() => expect(client.setAddonEnabled).toHaveBeenCalledWith('fake', false))
  })

  it('names rows in core tables in the Remove dialog', async () => {
    client.listAddons.mockResolvedValue([{ ...addon, enabled: false }])
    client.getAddon.mockResolvedValue({
      ...addon,
      footprint: {
        tables: [],
        rows: 0,
        core_rows: [{ label: 'Fake vectors (fake-model)', table: 'publication_embeddings', rows: 1200 }],
        files: 0,
        bytes: 0,
        settings: [],
        secrets: [],
      },
    })
    renderSection()
    fireEvent.click(await screen.findByRole('button', { name: 'Remove' }))
    expect(await screen.findByText('Fake vectors (fake-model)')).toBeInTheDocument()
    expect(screen.getByText(`${(1200).toLocaleString()} rows`)).toBeInTheDocument()
  })
})
