import { useEffect, useMemo, useState } from 'react'
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query'
import { ExternalLink, FileSearch, LibraryBig, Mail, MessageSquare, PlugZap, TestTube2 } from 'lucide-react'

import {
  getApiErrorMessage,
  getPluginConfig,
  listPlugins,
  setPluginEnabled,
  sweepInboxNow,
  testPluginConnection,
  updatePluginConfig,
  type PluginInfo,
} from '@/api/client'
import { SettingsCard } from '@/components/settings/primitives'
import { StatusRow } from '@/components/shared/StatusRow'
import { SchemaField } from '@/components/settings/PluginSchemaField'
import { schemaFields, type ConfigValue } from '@/components/settings/pluginSchema'
import { pluginState, type PluginState } from '@/components/settings/pluginState'
import { StatusBadge } from '@/components/ui/status-badge'
import { Button } from '@/components/ui/button'
import { Switch } from '@/components/ui/switch'
import { errorToast, useToast } from '@/hooks/useToast'
import { docsUrl } from '@/lib/docs'
import { invalidateQueries } from '@/lib/queryHelpers'

/** Identity glyph per plugin — which one it is, never how good it is. */
const ICONS = {
  slack: MessageSquare,
  email: Mail,
  open_access: FileSearch,
  shadow_libraries: LibraryBig,
} as const

/** What each runtime seam means to a reader, instead of the raw capability id. */
const CAPABILITY_LABELS: Record<string, string> = {
  send: 'Sends alerts',
  receive: 'Captures papers',
  pdf_source: 'Finds PDFs',
}

const STATE_PILL: Record<PluginState, { label: string; tone: 'neutral' | 'warning' | 'positive' }> = {
  off: { label: 'Off', tone: 'neutral' },
  'needs-setup': { label: 'Needs setup', tone: 'warning' },
  ready: { label: 'Ready', tone: 'positive' },
}

/**
 * The plugin catalogue: everything ALMa can plug into, one row each.
 *
 * Every plugin ships switched OFF and reaches nothing until you switch it on
 * — so a row is a name, what it does, its state and a switch. Its setup is
 * mounted only once it is on, which is also why an inactive plugin's
 * configuration is never even fetched.
 */
export function PluginsSection() {
  const pluginsQuery = useQuery({
    queryKey: ['plugins'],
    queryFn: listPlugins,
  })

  if (pluginsQuery.isLoading) {
    return <p className="text-sm text-slate-500">Loading plugins…</p>
  }
  if (pluginsQuery.isError) {
    return (
      <p className="text-sm text-critical-600">
        Plugins could not be loaded: {getApiErrorMessage(pluginsQuery.error)}
      </p>
    )
  }

  const plugins = pluginsQuery.data ?? []

  return (
    <SettingsCard
      icon={PlugZap}
      title="Plugins"
      description={
        `Everything ALMa can plug into. All of them ship switched off — an inactive plugin ` +
        `reaches nothing and is never asked to. Switch one on to set it up; switching it ` +
        `off again keeps what you typed.`
      }
    >
      <ul className="divide-y divide-[var(--color-border)]">
        {plugins.map((plugin) => (
          <PluginRow key={plugin.id} plugin={plugin} />
        ))}
      </ul>
    </SettingsCard>
  )
}

function PluginRow({ plugin }: { plugin: PluginInfo }) {
  const queryClient = useQueryClient()
  const Icon = ICONS[plugin.id as keyof typeof ICONS] ?? PlugZap
  const pill = STATE_PILL[pluginState(plugin)]

  const activationMutation = useMutation({
    mutationFn: (enabled: boolean) => setPluginEnabled(plugin.id, enabled),
    onSuccess: async () => {
      await invalidateQueries(
        queryClient,
        ['plugins'],
        ['home'],
        ['home-brief'],
        ['signal-lab'],
        ['inbox-status'],
      )
    },
    onError: (error) => errorToast('Could not change plugin state', getApiErrorMessage(error)),
  })

  return (
    <li className="py-3 first:pt-0 last:pb-0">
      <div className="flex items-start gap-3">
        <span className="mt-0.5 flex h-7 w-7 shrink-0 items-center justify-center rounded-sm bg-control-quiet text-slate-500">
          <Icon className="h-4 w-4" aria-hidden />
        </span>
        <div className="min-w-0 flex-1">
          <p className="text-sm font-medium text-alma-900">
            {plugin.display_name}
            <span className="ml-2 text-xs font-normal text-slate-500">
              {plugin.capabilities.map((c) => CAPABILITY_LABELS[c] ?? c).join(' · ')}
            </span>
          </p>
          <p className="mt-0.5 text-xs text-slate-500">{plugin.description}</p>
        </div>
        <div className="flex shrink-0 items-center gap-2">
          <StatusBadge tone={pill.tone} size="sm">{pill.label}</StatusBadge>
          <Switch
            checked={plugin.enabled}
            disabled={activationMutation.isPending}
            onCheckedChange={(enabled) => activationMutation.mutate(enabled)}
            aria-label={`${plugin.enabled ? 'Switch off' : 'Switch on'} ${plugin.display_name}`}
          />
        </div>
      </div>
      {plugin.enabled && <PluginSetup plugin={plugin} />}
    </li>
  )
}

/** Setup for a plugin that is ON. Never mounted otherwise — so never fetched. */
function PluginSetup({ plugin }: { plugin: PluginInfo }) {
  const queryClient = useQueryClient()
  const { toast } = useToast()
  const [form, setForm] = useState<Record<string, ConfigValue>>({})
  const [advancedOpen, setAdvancedOpen] = useState(false)

  const configQuery = useQuery({
    queryKey: ['plugins', plugin.id, 'config'],
    queryFn: () => getPluginConfig(plugin.id),
  })
  useEffect(() => {
    if (configQuery.data) {
      setForm(configQuery.data.config as Record<string, ConfigValue>)
    }
  }, [configQuery.data])

  const saveMutation = useMutation({
    mutationFn: () => updatePluginConfig(plugin.id, form),
    onSuccess: async (result) => {
      setForm(result.config as Record<string, ConfigValue>)
      await invalidateQueries(queryClient, ['plugins'], ['settings'], ['home'], ['home-brief'])
      toast({ title: `${plugin.display_name} settings saved` })
    },
    onError: (error) => errorToast('Plugin settings were not saved', getApiErrorMessage(error)),
  })

  const testMutation = useMutation({
    mutationFn: () => testPluginConnection(plugin.id),
    onSuccess: (result) => {
      if (result.ok) {
        toast({ title: `${plugin.display_name} test passed`, description: result.message })
      } else {
        errorToast(`${plugin.display_name} test failed`, result.error || result.message)
      }
    },
    onError: (error) => errorToast(`${plugin.display_name} test failed`, getApiErrorMessage(error)),
  })

  const captureMutation = useMutation({
    mutationFn: sweepInboxNow,
    onSuccess: (result) => {
      const captured = result?.captured ?? 0
      toast({
        title: captured
          ? `Captured ${captured} paper${captured === 1 ? '' : 's'}`
          : 'Slack capture reached',
        description: captured
          ? 'New papers are waiting in the Inbox on Home.'
          : 'No new paper links were waiting.',
      })
    },
    onError: (error) => errorToast('Slack capture failed', getApiErrorMessage(error)),
  })

  const fields = useMemo(() => schemaFields(plugin), [plugin])
  const visibleFields = fields.filter(([, schema]) => advancedOpen || !schema['x-alma-advanced'])
  const hasAdvanced = fields.some(([, schema]) => schema['x-alma-advanced'])

  return (
    <div className="mt-3 ml-10 space-y-4 border-l border-[var(--color-border)] pl-4">
      {configQuery.isError && (
        <p className="text-xs text-critical-600">
          Configuration unavailable: {getApiErrorMessage(configQuery.error)}
        </p>
      )}
      {visibleFields.map(([name, schema]) => (
        <SchemaField
          key={name}
          name={name}
          schema={schema}
          value={form[name] ?? (schema.default as ConfigValue) ?? ''}
          onChange={(value) => setForm((current) => ({ ...current, [name]: value }))}
        />
      ))}
      {hasAdvanced && (
        <Button variant="ghost" size="sm" onClick={() => setAdvancedOpen((open) => !open)}>
          {advancedOpen ? 'Hide advanced controls' : 'Show advanced controls'}
        </Button>
      )}
      <div className="flex flex-wrap items-center gap-2">
        <Button
          size="sm"
          onClick={() => saveMutation.mutate()}
          disabled={configQuery.isLoading || saveMutation.isPending}
        >
          {saveMutation.isPending ? 'Saving…' : 'Save plugin settings'}
        </Button>
        {plugin.actions.includes('test') && (
          <Button
            variant="outline"
            size="sm"
            onClick={() => testMutation.mutate()}
            disabled={testMutation.isPending}
          >
            <TestTube2 className="h-4 w-4" />
            {testMutation.isPending ? 'Testing…' : 'Test connection'}
          </Button>
        )}
        {plugin.actions.includes('capture') && (
          <Button
            variant="outline"
            size="sm"
            onClick={() => captureMutation.mutate()}
            disabled={captureMutation.isPending}
          >
            {captureMutation.isPending ? 'Checking…' : 'Check capture now'}
          </Button>
        )}
        <span className="ml-auto inline-flex items-center gap-3 text-xs text-slate-500">
          <span>v{plugin.version}</span>
          {plugin.docs_path && (
            <a
              className="inline-flex items-center gap-1 text-accent-700 hover:underline"
              href={docsUrl(plugin.docs_path)}
              target="_blank"
              rel="noreferrer"
            >
              Guide <ExternalLink className="h-3 w-3" aria-hidden />
            </a>
          )}
        </span>
      </div>
      {testMutation.data?.results && testMutation.data.results.length > 0 && (
        <div className="space-y-1.5" aria-label={`${plugin.display_name} test results`}>
          {testMutation.data.results.map((row) => (
            <StatusRow
              key={`${row.source ?? ''}:${row.address}`}
              severity={row.severity}
              label={row.address}
              title={row.source}
              metric={<span className="shrink-0 text-xs text-slate-500">{row.state}</span>}
            />
          ))}
        </div>
      )}
    </div>
  )
}
