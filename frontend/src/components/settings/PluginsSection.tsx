import { useEffect, useMemo, useState } from 'react'
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query'
import { Mail, MessageSquare, PlugZap, TestTube2 } from 'lucide-react'

import {
  getApiErrorMessage,
  getPluginConfig,
  listPlugins,
  setPluginEnabled,
  sweepInboxNow,
  testPluginConnection,
  updatePluginConfig,
  type PluginInfo,
  type PluginSchemaProperty,
} from '@/api/client'
import { SettingsCard } from '@/components/settings/primitives'
import { Button } from '@/components/ui/button'
import { Input } from '@/components/ui/input'
import { Label } from '@/components/ui/label'
import { Switch } from '@/components/ui/switch'
import { errorToast, useToast } from '@/hooks/useToast'
import { invalidateQueries } from '@/lib/queryHelpers'

type ConfigValue = string | number | boolean | null

const ICONS = {
  slack: MessageSquare,
  email: Mail,
} as const

function schemaFields(plugin: PluginInfo) {
  return Object.entries(plugin.config_schema.properties ?? {}).sort(
    ([, a], [, b]) => (a['x-alma-order'] ?? 999) - (b['x-alma-order'] ?? 999),
  )
}

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

  return (
    <div className="space-y-5">
      {(pluginsQuery.data ?? []).map((plugin) => (
        <PluginCard key={plugin.id} plugin={plugin} />
      ))}
    </div>
  )
}

function PluginCard({ plugin }: { plugin: PluginInfo }) {
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

  const saveMutation = useMutation({
    mutationFn: () => updatePluginConfig(plugin.id, form),
    onSuccess: async (result) => {
      setForm(result.config as Record<string, ConfigValue>)
      await invalidateQueries(
        queryClient,
        ['plugins'],
        ['settings'],
        ['home'],
        ['home-brief'],
      )
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
  const visibleFields = fields.filter(([, schema]) => (
    advancedOpen || !schema['x-alma-advanced']
  ))
  const hasAdvanced = fields.some(([, schema]) => schema['x-alma-advanced'])
  const Icon = ICONS[plugin.id as keyof typeof ICONS] ?? PlugZap

  return (
    <SettingsCard
      icon={Icon}
      title={plugin.display_name}
      description={plugin.description}
      action={
        <div className="flex items-center gap-2">
          <span className="text-xs text-slate-500">
            {plugin.enabled ? 'Active' : 'Inactive'}
          </span>
          <Switch
            checked={plugin.enabled}
            disabled={activationMutation.isPending}
            onCheckedChange={(enabled) => activationMutation.mutate(enabled)}
            aria-label={`${plugin.enabled ? 'Deactivate' : 'Activate'} ${plugin.display_name}`}
          />
        </div>
      }
      footer={
        <div className="flex flex-wrap items-center justify-between gap-2">
          <span className="text-xs text-slate-500">
            {plugin.capabilities.join(' · ')} · v{plugin.version}
          </span>
          <Button
            size="sm"
            onClick={() => saveMutation.mutate()}
            disabled={configQuery.isLoading || saveMutation.isPending}
          >
            {saveMutation.isPending ? 'Saving…' : 'Save plugin settings'}
          </Button>
        </div>
      }
    >
      <div className="space-y-4">
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
        <div className="flex flex-wrap gap-2 border-t border-edge-1 pt-3">
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
              disabled={!plugin.enabled || captureMutation.isPending}
            >
              {captureMutation.isPending ? 'Checking…' : 'Check capture now'}
            </Button>
          )}
        </div>
      </div>
    </SettingsCard>
  )
}

function SchemaField({
  name,
  schema,
  value,
  onChange,
}: {
  name: string
  schema: PluginSchemaProperty
  value: ConfigValue
  onChange: (value: ConfigValue) => void
}) {
  const id = `plugin-field-${name}`
  if (schema.type === 'boolean') {
    return (
      <div className="flex items-start justify-between gap-4">
        <div>
          <Label htmlFor={id}>{schema.title ?? name}</Label>
          {schema.description && <p className="mt-1 text-xs text-slate-500">{schema.description}</p>}
        </div>
        <Switch id={id} checked={Boolean(value)} onCheckedChange={onChange} />
      </div>
    )
  }

  const numeric = schema.type === 'number' || schema.type === 'integer'
  return (
    <div className="space-y-1.5">
      <Label htmlFor={id}>{schema.title ?? name}</Label>
      <Input
        id={id}
        type={schema['x-alma-secret'] ? 'password' : numeric ? 'number' : 'text'}
        value={String(value ?? '')}
        min={schema.minimum}
        max={schema.maximum}
        step={schema.type === 'integer' ? 1 : schema['x-alma-step']}
        onChange={(event) => {
          if (!numeric) {
            onChange(event.target.value)
            return
          }
          const parsed = schema.type === 'integer'
            ? Number.parseInt(event.target.value, 10)
            : Number.parseFloat(event.target.value)
          onChange(Number.isFinite(parsed) ? parsed : 0)
        }}
      />
      {schema.description && <p className="text-xs text-slate-500">{schema.description}</p>}
    </div>
  )
}
