import type { Alert, AlertRule } from '@/api/client'

/**
 * Rules not assigned to ANY digest. An orphan rule never runs — evaluation
 * only walks digest→rule assignments — so the UI must flag it instead of
 * letting it look healthy (task 46 §1.1).
 */
export function orphanRuleIds(rules: AlertRule[], alerts: Alert[]): Set<string> {
  const assigned = new Set<string>()
  for (const alert of alerts) {
    for (const rule of alert.rules ?? []) {
      assigned.add(rule.id)
    }
  }
  return new Set(rules.filter((r) => !assigned.has(r.id)).map((r) => r.id))
}
