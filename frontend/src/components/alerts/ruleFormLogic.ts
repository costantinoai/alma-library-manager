import { z } from 'zod'

import type { AlertRule } from '@/api/client'

// ── Rule-type registry ─────────────────────────────────────────────────────
// One source of truth for the 9 rule types: human label, form schema, and
// the two conversions (form ⇄ rule_config). Pure module — no React — so the
// whole translation layer is unit-testable.

export const RULE_TYPES = [
  'author',
  'collection',
  'keyword',
  'topic',
  'similarity',
  'discovery_lens',
  'feed_monitor',
  'branch',
  'library_workflow',
] as const
export type RuleType = (typeof RULE_TYPES)[number]

export const RULE_TYPE_LABEL: Record<RuleType, string> = {
  author: 'Author',
  collection: 'Collection',
  keyword: 'Keyword',
  topic: 'Topic',
  similarity: 'Similarity Score',
  discovery_lens: 'Discovery Lens',
  feed_monitor: 'Feed Monitor',
  branch: 'Branch',
  library_workflow: 'Library Workflow',
}

// ── Schema ────────────────────────────────────────────────────────────────
// Flat form schema with per-type validation via superRefine. A
// z.discriminatedUnion would give us a tighter compile-time guarantee but
// makes react-hook-form's `reset` awkward (the branch switches shape on
// rule_type change); flat + superRefine is the pragmatic tradeoff for
// what the form needs.
//
// All type-specific fields default to '' so switching rule_type never
// leaves a prior field in an inconsistent state — the form instance just
// validates the field that the current rule_type actually reads, and
// buildRuleConfig() (below) picks that single field on submit.

// Note: rules deliberately carry NO channel field — delivery channels belong
// to the digest a rule is assigned to; evaluation ignores rule-level
// channels entirely (task 46 §3.2, truthful UI).
export const ruleFormSchema = z
  .object({
    name: z.string().trim().min(1, 'Name is required'),
    rule_type: z.enum(RULE_TYPES),
    enabled: z.boolean(),
    // Per-type config fields. Required only for whichever rule_type the
    // user picked (enforced in superRefine). Plain `z.string()` instead of
    // `.default('')` because zod 4's `.default()` splits input/output
    // types, which produces a Resolver shape that no longer matches
    // `useForm<TFieldValues>`'s expected single shape. `defaultValues`
    // already seeds every field with '', so the runtime behaviour is
    // identical.
    author_id: z.string(),
    collection_id: z.string(),
    keywords: z.string(),
    topic: z.string(),
    min_score: z.string(),
    lens_id: z.string(),
    monitor_id: z.string(),
    branch_id: z.string(),
    workflow: z.string(),
  })
  .superRefine((data, ctx) => {
    const addRequired = (field: keyof typeof data, message: string) => {
      ctx.addIssue({ code: z.ZodIssueCode.custom, path: [field], message })
    }
    switch (data.rule_type) {
      case 'author':
        if (!data.author_id.trim()) addRequired('author_id', 'Author ID is required.')
        break
      case 'collection':
        if (!data.collection_id) addRequired('collection_id', 'Pick a collection.')
        break
      case 'keyword': {
        const kws = data.keywords.split(',').map((k) => k.trim()).filter(Boolean)
        if (kws.length === 0) addRequired('keywords', 'At least one keyword is required.')
        break
      }
      case 'topic':
        if (!data.topic.trim()) addRequired('topic', 'Topic text is required.')
        break
      case 'similarity': {
        const raw = data.min_score.trim()
        if (!raw) {
          addRequired('min_score', 'Minimum score is required.')
        } else {
          const n = Number(raw)
          if (Number.isNaN(n) || n < 0 || n > 100) {
            addRequired('min_score', 'Score must be a number between 0 and 100.')
          }
        }
        break
      }
      case 'discovery_lens':
        if (!data.lens_id) addRequired('lens_id', 'Pick a discovery lens.')
        break
      case 'feed_monitor':
        if (!data.monitor_id) addRequired('monitor_id', 'Pick a feed monitor.')
        break
      case 'branch':
        if (!data.branch_id) addRequired('branch_id', 'Pick a branch.')
        break
      case 'library_workflow':
        if (!data.workflow) addRequired('workflow', 'Pick a workflow state.')
        break
    }
  })

export type RuleFormValues = z.infer<typeof ruleFormSchema>

export const EMPTY_FORM_VALUES: RuleFormValues = {
  name: '',
  rule_type: 'author',
  enabled: true,
  author_id: '',
  collection_id: '',
  keywords: '',
  topic: '',
  min_score: '',
  lens_id: '',
  monitor_id: '',
  branch_id: '',
  workflow: '',
}

// ── Form → API shape conversion ───────────────────────────────────────────
// The API accepts a single `rule_config` object whose shape depends on
// `rule_type`. This switch is the ONLY place we translate form fields into
// that shape — so if a form field goes stale after a rule_type switch, it
// is never sent to the API.
//
// `baseConfig` is the rule's existing rule_config when editing. The form
// only surfaces the primary target field per type, so extras the config
// legitimately carries (a template's lookback_days, a similarity rule's
// lens_id, ...) must survive an edit instead of being silently reset.
// Merge policy:
//   - extras that are independent knobs are always preserved;
//   - extras entangled with the primary target (author → openalex_id,
//     branch → branch_label, collection → collection_name) are dropped
//     when the target changes, because they would still describe the OLD
//     entity and mislead evaluation.

function baseIfSameTarget(
  baseConfig: Record<string, unknown>,
  targetKey: string,
  targetValue: string,
): Record<string, unknown> {
  return baseConfig[targetKey] === targetValue ? baseConfig : {}
}

export function buildRuleConfig(
  values: RuleFormValues,
  baseConfig: Record<string, unknown> = {},
): Record<string, unknown> {
  switch (values.rule_type) {
    case 'author': {
      const author_id = values.author_id.trim()
      return { ...baseIfSameTarget(baseConfig, 'author_id', author_id), author_id }
    }
    case 'collection': {
      const collection_id = values.collection_id
      return { ...baseIfSameTarget(baseConfig, 'collection_id', collection_id), collection_id }
    }
    case 'keyword':
      return {
        ...baseConfig,
        keywords: values.keywords.split(',').map((k) => k.trim()).filter(Boolean),
      }
    case 'topic':
      return { ...baseConfig, topic: values.topic.trim() }
    case 'similarity':
      return { ...baseConfig, min_score: Number(values.min_score) }
    case 'discovery_lens': {
      const lens_id = values.lens_id
      return { ...baseIfSameTarget(baseConfig, 'lens_id', lens_id), lens_id }
    }
    case 'feed_monitor':
      // lookback / statuses are user prefs, valid across monitors — keep them.
      return {
        include_statuses: ['new'],
        lookback_days: 14,
        ...baseConfig,
        monitor_id: values.monitor_id,
      }
    case 'branch': {
      const branch_id = values.branch_id
      return { min_score: 0.55, ...baseIfSameTarget(baseConfig, 'branch_id', branch_id), branch_id }
    }
    case 'library_workflow':
      return { limit: 20, ...baseConfig, workflow: values.workflow }
  }
}

// ── Card summary ──────────────────────────────────────────────────────────
// Human line for the rule card: WHAT the rule watches, without opening the
// edit dialog. Entity ids resolve through the lookup maps the section
// already fetched for the form's Selects; unknown ids fall back to the raw
// value so the line is never blank or lying.

export interface RuleDescribeLookups {
  monitors?: Map<string, string>
  lenses?: Map<string, string>
  collections?: Map<string, string>
  authors?: Map<string, string>
}

const WORKFLOW_LABEL: Record<string, string> = {
  reading: 'On reading list',
  done: 'Finished',
  excluded: 'Excluded',
}

export function describeRuleConfig(rule: AlertRule, lookups: RuleDescribeLookups = {}): string {
  const cfg = rule.rule_config
  const str = (v: unknown) => String(v ?? '').trim()
  const named = (map: Map<string, string> | undefined, id: string) => map?.get(id) ?? id
  switch (rule.rule_type as RuleType) {
    case 'author': {
      const id = str(cfg.author_id) || str(cfg.openalex_id)
      return `Author: ${named(lookups.authors, id)}`
    }
    case 'collection': {
      const id = str(cfg.collection_id)
      return `Collection: ${id ? named(lookups.collections, id) : str(cfg.collection_name)}`
    }
    case 'keyword': {
      const keywords = Array.isArray(cfg.keywords)
        ? (cfg.keywords as unknown[]).map(str).filter(Boolean)
        : [str(cfg.keyword)].filter(Boolean)
      return `Keywords: ${keywords.join(', ')}`
    }
    case 'topic':
      return `Topic: ${str(cfg.topic) || str(cfg.term)}`
    case 'similarity': {
      const score = str(cfg.min_score) || '60'
      const lens = str(cfg.lens_id)
      return `Score ≥ ${score}${lens ? ` in ${named(lookups.lenses, lens)}` : ''}`
    }
    case 'discovery_lens':
      return `Lens: ${named(lookups.lenses, str(cfg.lens_id))}`
    case 'feed_monitor':
      return `Monitor: ${named(lookups.monitors, str(cfg.monitor_id) || str(cfg.monitor_key) || str(cfg.label))}`
    case 'branch':
      return `Branch: ${str(cfg.branch_label) || str(cfg.branch_id)}`
    case 'library_workflow': {
      const workflow = str(cfg.workflow) || str(cfg.state)
      return `Workflow: ${WORKFLOW_LABEL[workflow] ?? workflow}`
    }
    default:
      return ''
  }
}

// Reverse direction: populate form state from an existing AlertRule when
// opening the edit dialog. Any fields the rule's rule_type doesn't use are
// left at their empty defaults.
export function ruleToFormValues(rule: AlertRule): RuleFormValues {
  const cfg = rule.rule_config
  const base: RuleFormValues = {
    ...EMPTY_FORM_VALUES,
    name: rule.name,
    rule_type: rule.rule_type as RuleType,
    enabled: rule.enabled,
  }
  switch (rule.rule_type as RuleType) {
    case 'author':
      return { ...base, author_id: String(cfg.author_id ?? '') }
    case 'collection':
      return { ...base, collection_id: String(cfg.collection_id ?? cfg.collection_name ?? '') }
    case 'keyword':
      return {
        ...base,
        keywords: Array.isArray(cfg.keywords) ? (cfg.keywords as unknown[]).join(', ') : '',
      }
    case 'topic':
      return { ...base, topic: String(cfg.topic ?? '') }
    case 'similarity':
      return { ...base, min_score: String(cfg.min_score ?? '') }
    case 'discovery_lens':
      return { ...base, lens_id: String(cfg.lens_id ?? '') }
    case 'feed_monitor':
      return { ...base, monitor_id: String(cfg.monitor_id ?? '') }
    case 'branch':
      return { ...base, branch_id: String(cfg.branch_id ?? cfg.branch_label ?? '') }
    case 'library_workflow':
      return { ...base, workflow: String(cfg.workflow ?? cfg.state ?? '') }
  }
}
