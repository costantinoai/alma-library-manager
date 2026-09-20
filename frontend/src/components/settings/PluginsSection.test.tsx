/** The plugin catalogue: off is off, all the way down.
 *
 * The contract this file pins is not cosmetic. Until 2026-09-20 a switched-off
 * plugin still rendered its whole setup form, still fetched its configuration,
 * and still offered "Test connection" — a button that really did post to Slack
 * or send mail. A row for a plugin that is off must offer none of that.
 */
import { QueryClient, QueryClientProvider } from '@tanstack/react-query'
import { fireEvent, render, screen, waitFor } from '@testing-library/react'
import { beforeEach, describe, expect, it, vi } from 'vitest'

import { PluginsSection } from './PluginsSection'
import type { PluginInfo } from '@/api/client'

const listPlugins = vi.fn()
const getPluginConfig = vi.fn()
const setPluginEnabled = vi.fn()

vi.mock('@/api/client', async (importOriginal) => ({
  ...(await importOriginal<typeof import('@/api/client')>()),
  listPlugins: () => listPlugins(),
  getPluginConfig: (id: string) => getPluginConfig(id),
  setPluginEnabled: (id: string, enabled: boolean) => setPluginEnabled(id, enabled),
}))

function plugin(overrides: Partial<PluginInfo> = {}): PluginInfo {
  return {
    id: 'slack',
    display_name: 'Slack',
    version: '3.0.0',
    description: 'Post alert digests and capture papers sent from Slack.',
    kind: 'integration',
    capabilities: ['send', 'receive'],
    enabled: false,
    configured: true,
    can_send: true,
    can_receive: true,
    config_schema: {
      properties: {
        bot_token: { title: 'Bot token', type: 'string', 'x-alma-secret': true, 'x-alma-order': 10 },
      },
    },
    status: {},
    actions: ['test', 'capture'],
    docs_path: '/user-guide/connecting-slack/',
    ...overrides,
  } as PluginInfo
}

function renderCatalogue() {
  const client = new QueryClient({ defaultOptions: { queries: { retry: false } } })
  return render(
    <QueryClientProvider client={client}>
      <PluginsSection />
    </QueryClientProvider>,
  )
}

beforeEach(() => {
  vi.clearAllMocks()
  getPluginConfig.mockResolvedValue({ plugin_id: 'slack', config: { bot_token: '' } })
  setPluginEnabled.mockImplementation(async (id: string, enabled: boolean) =>
    plugin({ id, enabled }),
  )
})

describe('PluginsSection', () => {
  it('shows a plugin that is off as a row with nothing to set up', async () => {
    listPlugins.mockResolvedValue([plugin()])
    renderCatalogue()

    expect(await screen.findByText('Slack')).toBeInTheDocument()
    expect(screen.getByText('Off')).toBeInTheDocument()
    expect(screen.queryByLabelText('Bot token')).not.toBeInTheDocument()
    expect(screen.queryByRole('button', { name: /save plugin settings/i })).not.toBeInTheDocument()
    expect(screen.queryByRole('button', { name: /test connection/i })).not.toBeInTheDocument()
    // Configuration of an inactive plugin is not even read.
    expect(getPluginConfig).not.toHaveBeenCalled()
  })

  it('switching one on is one call, and says so to the server', async () => {
    listPlugins.mockResolvedValue([plugin()])
    renderCatalogue()

    fireEvent.click(await screen.findByLabelText('Switch on Slack'))
    await waitFor(() => expect(setPluginEnabled).toHaveBeenCalledWith('slack', true))
    expect(setPluginEnabled).toHaveBeenCalledTimes(1)
  })

  it('reveals setup, and only then reads the configuration, once it is on', async () => {
    listPlugins.mockResolvedValue([plugin({ enabled: true })])
    renderCatalogue()

    expect(await screen.findByLabelText('Bot token')).toBeInTheDocument()
    expect(screen.getByRole('button', { name: /save plugin settings/i })).toBeInTheDocument()
    expect(screen.getByRole('button', { name: /test connection/i })).toBeInTheDocument()
    expect(screen.getByRole('link', { name: /guide/i })).toHaveAttribute(
      'href',
      expect.stringContaining('/user-guide/connecting-slack/'),
    )
    expect(screen.getByText('Ready')).toBeInTheDocument()
    await waitFor(() => expect(getPluginConfig).toHaveBeenCalledWith('slack'))
  })

  it('says a plugin that is on but not working yet needs setting up', async () => {
    listPlugins.mockResolvedValue([
      plugin({ enabled: true, configured: false, can_send: false, can_receive: false }),
    ])
    renderCatalogue()

    expect(await screen.findByText('Needs setup')).toBeInTheDocument()
  })
})
