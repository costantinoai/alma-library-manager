import type { PluginInfo } from '@/api/client'

/**
 * What a plugin row says about itself, in the order a reader meets it:
 * `off` (the default for everything ALMa ships), `needs-setup` (switched on
 * but it cannot do its job yet), `ready` (switched on and able to work).
 */
export type PluginState = 'off' | 'needs-setup' | 'ready'

/** Which status flag says a given capability can actually do its job. */
const READINESS: Record<string, keyof PluginInfo> = {
  send: 'can_send',
  receive: 'can_receive',
}

/**
 * Derive a plugin's state from what the API already reports.
 *
 * Keyed on the per-capability readiness flags rather than `configured`: a
 * plugin is free to call itself configured the moment it has nothing left to
 * ask for (a PDF source with no credentials, say), and "Ready" has to mean it
 * can really deliver an alert or receive a capture, not merely that its form
 * is complete.
 */
export function pluginState(plugin: PluginInfo): PluginState {
  if (!plugin.enabled) return 'off'
  const known = plugin.capabilities.filter((capability) => capability in READINESS)
  if (known.length === 0) return plugin.configured ? 'ready' : 'needs-setup'
  const working = known.some((capability) => Boolean(plugin[READINESS[capability]]))
  return working ? 'ready' : 'needs-setup'
}
