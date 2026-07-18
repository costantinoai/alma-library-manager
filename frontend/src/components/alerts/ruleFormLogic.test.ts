import { describe, expect, it } from 'vitest'

import type { AlertRule } from '@/api/client'
import {
  EMPTY_FORM_VALUES,
  buildRuleConfig,
  describeRuleConfig,
  ruleFormSchema,
  ruleToFormValues,
  type RuleFormValues,
} from './ruleFormLogic'

function values(overrides: Partial<RuleFormValues>): RuleFormValues {
  return { ...EMPTY_FORM_VALUES, name: 'Rule', ...overrides }
}

function rule(overrides: Partial<AlertRule>): AlertRule {
  return {
    id: 'r1',
    name: 'Rule',
    rule_type: 'author',
    rule_config: {},
    channels: ['slack'],
    enabled: true,
    created_at: '2026-07-01T00:00:00',
    ...overrides,
  }
}

describe('ruleFormSchema', () => {
  it('requires the primary field for the active rule_type only', () => {
    // author selected, author_id empty → invalid on author_id.
    const bad = ruleFormSchema.safeParse(values({ rule_type: 'author', author_id: '' }))
    expect(bad.success).toBe(false)
    if (!bad.success) {
      expect(bad.error.issues.map((i) => i.path[0])).toContain('author_id')
    }
    // The stale keyword field never blocks an author rule.
    const ok = ruleFormSchema.safeParse(
      values({ rule_type: 'author', author_id: 'A1', keywords: '' }),
    )
    expect(ok.success).toBe(true)
  })

  it('rejects out-of-range or non-numeric similarity scores', () => {
    for (const min_score of ['', 'abc', '-1', '101']) {
      const parsed = ruleFormSchema.safeParse(values({ rule_type: 'similarity', min_score }))
      expect(parsed.success).toBe(false)
    }
    expect(
      ruleFormSchema.safeParse(values({ rule_type: 'similarity', min_score: '70' })).success,
    ).toBe(true)
  })

  it('requires at least one non-empty keyword', () => {
    expect(
      ruleFormSchema.safeParse(values({ rule_type: 'keyword', keywords: ' , , ' })).success,
    ).toBe(false)
    expect(
      ruleFormSchema.safeParse(values({ rule_type: 'keyword', keywords: 'nlp, vision' })).success,
    ).toBe(true)
  })
})

describe('buildRuleConfig', () => {
  it('builds the per-type shape on create (no base config)', () => {
    expect(buildRuleConfig(values({ rule_type: 'author', author_id: ' A1 ' }))).toEqual({
      author_id: 'A1',
    })
    expect(
      buildRuleConfig(values({ rule_type: 'keyword', keywords: ' nlp , vision ,, ' })),
    ).toEqual({ keywords: ['nlp', 'vision'] })
    expect(buildRuleConfig(values({ rule_type: 'feed_monitor', monitor_id: 'm1' }))).toEqual({
      monitor_id: 'm1',
      include_statuses: ['new'],
      lookback_days: 14,
    })
    expect(buildRuleConfig(values({ rule_type: 'branch', branch_id: 'b1' }))).toEqual({
      branch_id: 'b1',
      min_score: 0.55,
    })
    expect(buildRuleConfig(values({ rule_type: 'library_workflow', workflow: 'reading' }))).toEqual(
      { workflow: 'reading', limit: 20 },
    )
  })

  it('preserves config extras the form does not surface when editing', () => {
    // A template-created feed_monitor rule carries custom lookback/statuses;
    // editing (same monitor) must not silently reset them to defaults.
    const base = { monitor_id: 'm1', include_statuses: ['new', 'seen'], lookback_days: 30 }
    expect(buildRuleConfig(values({ rule_type: 'feed_monitor', monitor_id: 'm1' }), base)).toEqual(
      base,
    )
    // A similarity rule scoped to a lens keeps its lens when only the score changes.
    expect(
      buildRuleConfig(
        values({ rule_type: 'similarity', min_score: '80' }),
        { min_score: 60, lens_id: 'L1' },
      ),
    ).toEqual({ min_score: 80, lens_id: 'L1' })
    // library_workflow keeps a custom limit across workflow changes.
    expect(
      buildRuleConfig(
        values({ rule_type: 'library_workflow', workflow: 'done' }),
        { workflow: 'reading', limit: 50 },
      ),
    ).toEqual({ workflow: 'done', limit: 50 })
  })

  it('drops entangled extras when the primary target changes', () => {
    // openalex_id describes the OLD author — keeping it would make the rule
    // silently keep matching the previous person.
    const authorBase = { author_id: 'A1', openalex_id: 'OA1' }
    expect(
      buildRuleConfig(values({ rule_type: 'author', author_id: 'A2' }), authorBase),
    ).toEqual({ author_id: 'A2' })
    expect(
      buildRuleConfig(values({ rule_type: 'author', author_id: 'A1' }), authorBase),
    ).toEqual({ author_id: 'A1', openalex_id: 'OA1' })

    const branchBase = { branch_id: 'b1', branch_label: 'Old label', min_score: 0.7 }
    expect(buildRuleConfig(values({ rule_type: 'branch', branch_id: 'b2' }), branchBase)).toEqual({
      branch_id: 'b2',
      min_score: 0.55,
    })
    expect(buildRuleConfig(values({ rule_type: 'branch', branch_id: 'b1' }), branchBase)).toEqual({
      branch_id: 'b1',
      branch_label: 'Old label',
      min_score: 0.7,
    })
  })
})

describe('describeRuleConfig', () => {
  it('summarizes each type, resolving ids through lookups', () => {
    const lookups = {
      monitors: new Map([['m1', 'Predictive coding feed']]),
      lenses: new Map([['L1', 'Core lens']]),
      collections: new Map([['c1', 'Vision']]),
      authors: new Map([['A1', 'A. Clark']]),
    }
    expect(
      describeRuleConfig(rule({ rule_type: 'author', rule_config: { author_id: 'A1' } }), lookups),
    ).toBe('Author: A. Clark')
    expect(
      describeRuleConfig(
        rule({ rule_type: 'feed_monitor', rule_config: { monitor_id: 'm1' } }),
        lookups,
      ),
    ).toBe('Monitor: Predictive coding feed')
    expect(
      describeRuleConfig(
        rule({ rule_type: 'collection', rule_config: { collection_id: 'c1' } }),
        lookups,
      ),
    ).toBe('Collection: Vision')
    expect(
      describeRuleConfig(
        rule({ rule_type: 'keyword', rule_config: { keywords: ['nlp', 'vision'] } }),
      ),
    ).toBe('Keywords: nlp, vision')
    expect(
      describeRuleConfig(
        rule({ rule_type: 'similarity', rule_config: { min_score: 70, lens_id: 'L1' } }),
        lookups,
      ),
    ).toBe('Score ≥ 70 in Core lens')
    expect(
      describeRuleConfig(
        rule({ rule_type: 'branch', rule_config: { branch_id: 'b1', branch_label: 'Memory' } }),
      ),
    ).toBe('Branch: Memory')
    expect(
      describeRuleConfig(
        rule({ rule_type: 'library_workflow', rule_config: { workflow: 'reading' } }),
      ),
    ).toBe('Workflow: On reading list')
  })

  it('falls back to the raw id when a lookup misses — never a blank line', () => {
    expect(
      describeRuleConfig(rule({ rule_type: 'author', rule_config: { author_id: 'A9' } }), {}),
    ).toBe('Author: A9')
    expect(
      describeRuleConfig(rule({ rule_type: 'discovery_lens', rule_config: { lens_id: 'Lx' } })),
    ).toBe('Lens: Lx')
  })
})

describe('ruleToFormValues', () => {
  it('round-trips each type through the form and back', () => {
    const cases: Array<{ r: AlertRule; expectConfig: Record<string, unknown> }> = [
      {
        r: rule({ rule_type: 'author', rule_config: { author_id: 'A1', openalex_id: 'OA1' } }),
        expectConfig: { author_id: 'A1', openalex_id: 'OA1' },
      },
      {
        r: rule({
          rule_type: 'feed_monitor',
          rule_config: { monitor_id: 'm1', include_statuses: ['new'], lookback_days: 7 },
        }),
        expectConfig: { monitor_id: 'm1', include_statuses: ['new'], lookback_days: 7 },
      },
      {
        r: rule({ rule_type: 'keyword', rule_config: { keywords: ['nlp', 'vision'] } }),
        expectConfig: { keywords: ['nlp', 'vision'] },
      },
      {
        r: rule({ rule_type: 'library_workflow', rule_config: { workflow: 'done', limit: 50 } }),
        expectConfig: { workflow: 'done', limit: 50 },
      },
    ]
    for (const { r, expectConfig } of cases) {
      const formValues = ruleToFormValues(r)
      // Unedited round-trip must be lossless: form → config equals stored config.
      expect(buildRuleConfig(formValues, r.rule_config)).toEqual(expectConfig)
    }
  })

  it('reads evaluator aliases (state, branch_label, collection_name)', () => {
    expect(
      ruleToFormValues(rule({ rule_type: 'library_workflow', rule_config: { state: 'done' } }))
        .workflow,
    ).toBe('done')
    expect(
      ruleToFormValues(rule({ rule_type: 'branch', rule_config: { branch_label: 'B' } })).branch_id,
    ).toBe('B')
    expect(
      ruleToFormValues(rule({ rule_type: 'collection', rule_config: { collection_name: 'C' } }))
        .collection_id,
    ).toBe('C')
  })
})
