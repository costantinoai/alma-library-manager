/** Field status renders the backend's answer and never invents one. */
import { QueryClient, QueryClientProvider } from '@tanstack/react-query'
import { render, screen } from '@testing-library/react'
import { beforeEach, describe, expect, it, vi } from 'vitest'

import { ConnectionLine, FieldStatus, SectionStatus } from './ConnectionStatus'
import type { ConnectionField, ConnectionRecord } from '@/api/client'

const getConnections = vi.fn()

vi.mock('@/api/client', async (importOriginal) => ({
  ...(await importOriginal<typeof import('@/api/client')>()),
  getConnections: () => getConnections(),
}))

function record(overrides: Partial<ConnectionRecord>): ConnectionRecord {
  return {
    id: 'openalex',
    label: 'OpenAlex',
    section: 'connections',
    state: 'unverified',
    severity: 'info',
    status_label: 'Set, not checked',
    detail: 'Saved, but never tested.',
    action: null,
    required: true,
    source: 'saved',
    checkable: true,
    checked_at: null,
    last_worked_at: null,
    last_used_at: null,
    used_by: [],
    results: [],
    fields: [],
    ...overrides,
  }
}

function field(overrides: Partial<ConnectionField>): ConnectionField {
  return { name: 'key', label: 'Key', is_set: false, required: false, adds: '', ...overrides }
}

function mount(node: React.ReactNode, rows: ConnectionRecord[], sections = {}) {
  getConnections.mockResolvedValue({ connections: rows, sections })
  const client = new QueryClient({ defaultOptions: { queries: { retry: false } } })
  return render(<QueryClientProvider client={client}>{node}</QueryClientProvider>)
}

beforeEach(() => vi.clearAllMocks())

describe('ConnectionLine', () => {
  it('says set, not checked and offers the check, never green', async () => {
    mount(<ConnectionLine id="openalex" />, [record({})])
    const badge = await screen.findByText('Set, not checked')
    expect(badge.className).not.toMatch(/success|positive/)
    expect(screen.getByRole('button', { name: /check/i })).toBeInTheDocument()
  })

  it('gives the real error and what to do', async () => {
    mount(<ConnectionLine id="openalex" />, [
      record({
        state: 'failing',
        severity: 'critical',
        status_label: 'Not working',
        detail: 'OpenAlex refused the key (HTTP 401).',
        action: 'Paste a new key.',
        checked_at: new Date().toISOString(),
      }),
    ])
    expect(await screen.findByText(/refused the key.*Paste a new key.*Checked/)).toBeInTheDocument()
  })

  it('does not claim a check it could not make', async () => {
    mount(<ConnectionLine id="openalex" />, [
      record({
        state: 'cannot_check',
        severity: 'unknown',
        status_label: "Can't check",
        detail: 'External network access is off.',
        checkable: false,
        checked_at: new Date().toISOString(),
      }),
    ])
    const line = await screen.findByText('External network access is off.')
    expect(line.textContent).not.toMatch(/Checked/)
    expect(screen.queryByRole('button', { name: /check/i })).not.toBeInTheDocument()
  })

  it('says when an environment value wins', async () => {
    mount(<ConnectionLine id="openalex" />, [record({ source: 'environment' })])
    expect(await screen.findByText(/Set in the environment/)).toBeInTheDocument()
  })
})

describe('FieldStatus', () => {
  it('names an empty required field', async () => {
    mount(<FieldStatus id="openalex" field="key" />, [record({ fields: [field({ required: true })] })])
    expect(await screen.findByText('Needs setting')).toBeInTheDocument()
  })

  it('says what an empty optional field adds', async () => {
    mount(<FieldStatus id="openalex" field="key" />, [record({ fields: [field({ adds: 'more credits' })] })])
    expect(await screen.findByText(/Optional: adds more credits/)).toBeInTheDocument()
  })

  it('stays quiet on a saved field and speaks up for unsaved typing', async () => {
    const rows = [record({ fields: [field({ is_set: true })] })]
    const { unmount } = mount(<FieldStatus id="openalex" field="key" dirty />, rows)
    expect(await screen.findByText('Not saved')).toBeInTheDocument()
    unmount()
    const { container } = mount(<FieldStatus id="openalex" field="key" />, rows)
    await vi.waitFor(() => expect(getConnections).toHaveBeenCalled())
    expect(container.textContent).toBe('')
  })
})

describe('SectionStatus', () => {
  it('shows the section summary from the server', async () => {
    mount(<SectionStatus section="connections" />, [], {
      connections: { state: 'failing', severity: 'critical', status_label: '1 not working' },
    })
    expect(await screen.findByText('1 not working')).toBeInTheDocument()
  })
})
