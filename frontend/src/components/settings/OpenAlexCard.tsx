import { useEffect } from 'react'
import { useForm } from 'react-hook-form'
import { zodResolver } from '@hookform/resolvers/zod'
import { z } from 'zod'
import { useQuery } from '@tanstack/react-query'
import { CheckCircle, Globe, RefreshCw, Save } from 'lucide-react'

import { api, type Settings, type OpenAlexUsage } from '@/api/client'
import { AsyncButton, SettingsCard, StatTile } from '@/components/settings/primitives'
import {
  Form,
  FormControl,
  FormDescription,
  FormField,
  FormItem,
  FormLabel,
  FormMessage,
} from '@/components/ui/form'
import { Input } from '@/components/ui/input'

const openalexSchema = z.object({
  openalex_email: z
    .string()
    .refine((v) => !v || /.+@.+\..+/.test(v), {
      message: 'Enter a valid email address.',
    }),
  openalex_api_key: z.string(),
})

type OpenAlexForm = z.infer<typeof openalexSchema>

interface OpenAlexCardProps {
  formData: Settings
  onFormDataChange: (updater: (prev: Settings) => Settings) => void
  onSave: () => void
  isSaving: boolean
  saveSuccess: boolean
}

export function OpenAlexCard({
  formData,
  onFormDataChange,
  onSave,
  isSaving,
  saveSuccess,
}: OpenAlexCardProps) {
  const form = useForm<OpenAlexForm>({
    resolver: zodResolver(openalexSchema),
    defaultValues: {
      openalex_email: formData.openalex_email ?? '',
      openalex_api_key: formData.openalex_api_key ?? '',
    },
    mode: 'onBlur',
  })

  useEffect(() => {
    form.reset({
      openalex_email: formData.openalex_email ?? '',
      openalex_api_key: formData.openalex_api_key ?? '',
    })
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [formData.openalex_email, formData.openalex_api_key])

  useEffect(() => {
    const sub = form.watch((values) => {
      onFormDataChange((prev) => ({
        ...prev,
        openalex_email: values.openalex_email ?? '',
        openalex_api_key: values.openalex_api_key ?? '',
      }))
    })
    return () => sub.unsubscribe()
  }, [form, onFormDataChange])

  const openalexUsageQuery = useQuery({
    queryKey: ['openalex-usage'],
    queryFn: () => api.get<OpenAlexUsage>('/settings/openalex/usage'),
    retry: 1,
    refetchInterval: 60_000,
    enabled: formData.backend === 'openalex',
  })

  if (formData.backend !== 'openalex') return null

  const handleSave = async () => {
    const valid = await form.trigger()
    if (!valid) return
    onSave()
  }

  const usage = openalexUsageQuery.data

  return (
    <SettingsCard
      icon={Globe}
      title="OpenAlex Configuration"
      description="API credentials for OpenAlex data access."
    >
      <Form {...form}>
        <form
          className="space-y-4"
          onSubmit={(event) => {
            event.preventDefault()
            void handleSave()
          }}
        >
          <FormField
            control={form.control}
            name="openalex_email"
            render={({ field }) => (
              <FormItem>
                <FormLabel>Email</FormLabel>
                <FormControl>
                  <Input type="email" placeholder="you@example.com" {...field} />
                </FormControl>
                <FormDescription>Contact email for OpenAlex API identification.</FormDescription>
                <FormMessage />
              </FormItem>
            )}
          />

          <FormField
            control={form.control}
            name="openalex_api_key"
            render={({ field }) => (
              <FormItem>
                <FormLabel>API Key</FormLabel>
                <FormControl>
                  <Input type="password" placeholder="openalex-..." {...field} />
                </FormControl>
                <FormDescription>
                  Required for API access. Free tier: 100k credits/day. Get your key at{' '}
                  <a
                    href="https://openalex.org"
                    target="_blank"
                    rel="noopener noreferrer"
                    className="text-alma-600 hover:text-alma-800"
                  >
                    openalex.org
                  </a>
                </FormDescription>
                <FormMessage />
              </FormItem>
            )}
          />

          <div className="rounded-sm border border-[var(--color-border)] bg-parchment-50 p-3">
            <div className="mb-3 flex items-center justify-between gap-2">
              <p className="text-sm font-medium text-slate-700">OpenAlex Usage</p>
              <AsyncButton
                type="button"
                variant="outline"
                size="sm"
                icon={<RefreshCw className="h-3 w-3" />}
                pending={openalexUsageQuery.isFetching}
                onClick={() => openalexUsageQuery.refetch()}
                className="h-7 px-2 text-xs"
              >
                Refresh
              </AsyncButton>
            </div>
            {openalexUsageQuery.isLoading ? (
              <p className="text-xs text-slate-500">Loading usage...</p>
            ) : openalexUsageQuery.isError ? (
              <p className="text-xs text-red-600">Could not load usage stats.</p>
            ) : (
              <div className="grid grid-cols-1 gap-2 sm:grid-cols-3 lg:grid-cols-7">
                <StatTile label="Requests" value={usage?.request_count ?? 0} />
                <StatTile label="Retries" value={usage?.retry_count ?? 0} />
                <StatTile
                  label="429 Events"
                  value={usage?.rate_limited_events ?? 0}
                  tone={(usage?.rate_limited_events ?? 0) > 0 ? 'warning' : 'neutral'}
                />
                <StatTile label="Cache Saved" value={usage?.calls_saved_by_cache ?? 0} />
                {/* "No calls yet" vs "—" draws the real distinction: the
                    backend returns source="no_calls_yet" when the client
                    hasn't made any request (so no X-RateLimit-* headers
                    captured and no API-key-authenticated /rate-limit call).
                    An em-dash is the "no data for this field" fallback for
                    calls-made-but-field-missing. Never show literal "unknown". */}
                <StatTile
                  label="Credits Used"
                  value={usage?.credits_used ?? (usage?.source === 'no_calls_yet' ? 'No calls yet' : '—')}
                />
                <StatTile
                  label="Credits Remaining"
                  value={usage?.credits_remaining ?? (usage?.source === 'no_calls_yet' ? 'No calls yet' : '—')}
                />
                <StatTile
                  label="Credits Limit"
                  value={usage?.credits_limit ?? (usage?.source === 'no_calls_yet' ? 'No calls yet' : '—')}
                />
              </div>
            )}
            {(usage?.resets_in_seconds || usage?.reset_at) && (
              <p className="mt-2 text-xs text-slate-500">
                Reset: {usage?.resets_in_seconds ? `${usage.resets_in_seconds}s` : ''}
                {usage?.resets_in_seconds && usage?.reset_at ? ' • ' : ''}
                {usage?.reset_at ?? ''}
              </p>
            )}
            {usage?.source && (
              <p className="mt-1 text-[11px] text-slate-400">Source: {usage.source}</p>
            )}
            {usage?.summary && <p className="mt-2 text-xs text-slate-500">{usage.summary}</p>}
          </div>

          <div className="flex items-center justify-end gap-3 pt-2">
            {saveSuccess && (
              <div className="flex items-center gap-1.5 text-sm text-green-600">
                <CheckCircle className="h-4 w-4" />
                Saved
              </div>
            )}
            <AsyncButton
              type="submit"
              icon={<Save className="h-4 w-4" />}
              pending={isSaving}
            >
              Save OpenAlex Settings
            </AsyncButton>
          </div>
        </form>
      </Form>
    </SettingsCard>
  )
}
