import { describe, expect, it } from 'vitest'

import { pluginState } from './pluginState'
import type { PluginInfo } from '@/api/client'

function plugin(overrides: Partial<PluginInfo> = {}): PluginInfo {
  return {
    id: 'slack',
    display_name: 'Slack',
    version: '1.0.0',
    description: '',
    kind: 'integration',
    capabilities: ['send', 'receive'],
    enabled: false,
    configured: false,
    can_send: false,
    can_receive: false,
    config_schema: { properties: {} },
    status: {},
    actions: [],
    docs_path: '/development/integrations/',
    ...overrides,
  } as PluginInfo
}

describe('pluginState', () => {
  it('is off whenever the switch is off, however well it is configured', () => {
    expect(pluginState(plugin({ configured: true, can_send: true }))).toBe('off')
  })

  it('needs setup when it is on but cannot do any of its jobs yet', () => {
    expect(pluginState(plugin({ enabled: true, configured: true }))).toBe('needs-setup')
  })

  it('is ready as soon as one of its capabilities works', () => {
    expect(pluginState(plugin({ enabled: true, can_send: true }))).toBe('ready')
    expect(pluginState(plugin({ enabled: true, can_receive: true }))).toBe('ready')
  })

  it('falls back to `configured` for a capability with no readiness flag', () => {
    const pdf = { capabilities: ['pdf_source'], enabled: true }
    expect(pluginState(plugin({ ...pdf, configured: true }))).toBe('ready')
    expect(pluginState(plugin({ ...pdf, configured: false }))).toBe('needs-setup')
  })
})
