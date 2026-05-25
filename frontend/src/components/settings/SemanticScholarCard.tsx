import { useEffect } from 'react'
import { useForm } from 'react-hook-form'
import { zodResolver } from '@hookform/resolvers/zod'
import { z } from 'zod'
import { useQuery } from '@tanstack/react-query'
import { Network, RefreshCw } from 'lucide-react'

import { api, type Settings, type SemanticScholarStatus } from '@/api/client'
import { AsyncButton, SettingsCard } from '@/components/settings/primitives'
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
import { cn } from '@/lib/utils'

const s2Schema = z.object({
  semantic_scholar_api_key: z.string(),
})

type S2Form = z.infer<typeof s2Schema>

interface SemanticScholarCardProps {
  formData: Settings
  onFormDataChange: (updater: (prev: Settings) => Settings) => void
}

/**
 * Resolve the connection-dot appearance from the live key-validity probe.
 * Green = the key works; red = S2 rejected it; amber = set but unverified;
 * grey = not configured / still checking.
 */
function dotState(status: SemanticScholarStatus | undefined, loading: boolean) {
  if (loading) return { dot: 'bg-alma-300', label: 'Checking…', text: 'text-slate-500' }
  if (!status || !status.configured)
    return { dot: 'bg-alma-300', label: 'Not set', text: 'text-slate-500' }
  if (status.valid === true)
    return { dot: 'bg-emerald-500', label: 'Connected', text: 'text-emerald-700' }
  if (status.valid === false)
    return { dot: 'bg-rose-500', label: 'Key rejected', text: 'text-rose-700' }
  return { dot: 'bg-amber-500', label: "Couldn't verify", text: 'text-amber-700' }
}

/**
 * Semantic Scholar credential card. S2 supplies SPECTER2 vectors and
 * paper/author recommendations as a secondary source (always on, not
 * gated on the active backend like OpenAlex). The key is strongly
 * recommended: without it S2 falls back to the shared anonymous pool and
 * 429s frequently, which stalls the Discovery graph lane.
 *
 * The key field writes into the shared `formData`; persistence happens via
 * the section's "Save connection settings" footer. After a save the
 * SettingsPage invalidates `semantic-scholar-status`, so the connection dot
 * re-probes and reflects the new key. The backend masks the stored value
 * (`****<suffix>`) on GET and skips re-rotation when the masked echo is
 * submitted unchanged.
 */
export function SemanticScholarCard({
  formData,
  onFormDataChange,
}: SemanticScholarCardProps) {
  const form = useForm<S2Form>({
    resolver: zodResolver(s2Schema),
    defaultValues: {
      semantic_scholar_api_key: formData.semantic_scholar_api_key ?? '',
    },
    mode: 'onBlur',
  })

  useEffect(() => {
    form.reset({ semantic_scholar_api_key: formData.semantic_scholar_api_key ?? '' })
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [formData.semantic_scholar_api_key])

  useEffect(() => {
    const sub = form.watch((values) => {
      onFormDataChange((prev) => ({
        ...prev,
        semantic_scholar_api_key: values.semantic_scholar_api_key ?? '',
      }))
    })
    return () => sub.unsubscribe()
  }, [form, onFormDataChange])

  // On-demand validity probe (one cheap S2 call). No polling — the 1 req/s
  // budget is precious — so it refetches only on mount, manual refresh, or
  // when the SettingsPage invalidates the key after a save.
  const statusQuery = useQuery({
    queryKey: ['semantic-scholar-status'],
    queryFn: () => api.get<SemanticScholarStatus>('/settings/semantic-scholar/status'),
    retry: 1,
    refetchOnWindowFocus: false,
    staleTime: 60_000,
  })

  const state = dotState(statusQuery.data, statusQuery.isLoading || statusQuery.isFetching)

  return (
    <SettingsCard
      icon={Network}
      title="Semantic Scholar"
      description="Secondary source for SPECTER2 vectors and paper/author recommendations."
    >
      <Form {...form}>
        <form className="space-y-4" onSubmit={(e) => e.preventDefault()}>
          <div className="flex items-center justify-between gap-2 rounded-md bg-parchment-50 px-3 py-2">
            <span className="inline-flex items-center gap-2 text-sm">
              <span className={cn('h-2 w-2 rounded-full', state.dot)} aria-hidden />
              <span className={cn('font-medium', state.text)}>{state.label}</span>
              {statusQuery.data?.detail && (
                <span className="text-xs text-slate-500">— {statusQuery.data.detail}</span>
              )}
            </span>
            <AsyncButton
              type="button"
              variant="outline"
              size="sm"
              icon={<RefreshCw className="h-3 w-3" />}
              pending={statusQuery.isFetching}
              onClick={() => statusQuery.refetch()}
              className="h-7 px-2 text-xs"
            >
              Re-check
            </AsyncButton>
          </div>

          <FormField
            control={form.control}
            name="semantic_scholar_api_key"
            render={({ field }) => (
              <FormItem>
                <FormLabel>API Key</FormLabel>
                <FormControl>
                  <Input type="password" placeholder="s2-..." {...field} />
                </FormControl>
                <FormDescription>
                  Strongly recommended. Without a key, S2 shares the anonymous
                  worldwide rate pool and returns frequent 429s (which stall
                  Discovery). Get a free key at{' '}
                  <a
                    href="https://www.semanticscholar.org/product/api"
                    target="_blank"
                    rel="noopener noreferrer"
                    className="text-alma-600 hover:text-alma-800"
                  >
                    semanticscholar.org/product/api
                  </a>
                  . Saved with the connection settings below, then re-checked.
                </FormDescription>
                <FormMessage />
              </FormItem>
            )}
          />
        </form>
      </Form>
    </SettingsCard>
  )
}
