import { describe, expect, it } from 'vitest'

import type { Alert, AlertRule } from '@/api/client'
import { orphanRuleIds } from './alertDerive'

function rule(id: string): AlertRule {
  return {
    id,
    name: `Rule ${id}`,
    rule_type: 'keyword',
    rule_config: { keywords: ['x'] },
    channels: [],
    enabled: true,
    created_at: '2026-07-01T00:00:00',
  }
}

function alert(id: string, rules: AlertRule[]): Alert {
  return {
    id,
    name: `Alert ${id}`,
    channels: ['slack'],
    schedule: 'manual',
    format: 'text',
    enabled: true,
    created_at: '2026-07-01T00:00:00',
    rules,
  }
}

describe('orphanRuleIds', () => {
  it('flags rules assigned to no digest', () => {
    const r1 = rule('r1')
    const r2 = rule('r2')
    const r3 = rule('r3')
    const orphans = orphanRuleIds([r1, r2, r3], [alert('a1', [r2])])
    expect(orphans).toEqual(new Set(['r1', 'r3']))
  })

  it('handles digests without a rules payload and empty inputs', () => {
    const r1 = rule('r1')
    const bare = { ...alert('a1', []), rules: undefined }
    expect(orphanRuleIds([r1], [bare])).toEqual(new Set(['r1']))
    expect(orphanRuleIds([], [])).toEqual(new Set())
  })

  it('is empty when every rule is assigned somewhere', () => {
    const r1 = rule('r1')
    const r2 = rule('r2')
    expect(orphanRuleIds([r1, r2], [alert('a1', [r1]), alert('a2', [r2])])).toEqual(new Set())
  })
})
