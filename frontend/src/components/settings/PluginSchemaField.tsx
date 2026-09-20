import { Input } from '@/components/ui/input'
import { Label } from '@/components/ui/label'
import { Switch } from '@/components/ui/switch'
import type { PluginSchemaProperty } from '@/api/client'
import type { ConfigValue } from '@/components/settings/pluginSchema'

/**
 * One configuration field, rendered from the plugin's own JSON schema — the
 * backend's Pydantic model is the single source of truth for what a plugin
 * can be asked, so nothing here is written per plugin.
 */
export function SchemaField({
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
