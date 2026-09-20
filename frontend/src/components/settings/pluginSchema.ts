import type { PluginInfo } from '@/api/client'

/** One value in a plugin's generated configuration form. */
export type ConfigValue = string | number | boolean | null

/** A plugin's configuration fields, in the order its manifest asked for. */
export function schemaFields(plugin: PluginInfo) {
  return Object.entries(plugin.config_schema.properties ?? {}).sort(
    ([, a], [, b]) => (a['x-alma-order'] ?? 999) - (b['x-alma-order'] ?? 999),
  )
}
