import { useEffect } from 'react'
import { useForm } from 'react-hook-form'
import { zodResolver } from '@hookform/resolvers/zod'
import { z } from 'zod'
import { useQuery } from '@tanstack/react-query'
import { RefreshCw } from 'lucide-react'

import { api, type Settings, type SemanticScholarStatus } from '@/api/client'
import { AsyncButton, ConnectionPill, SettingsSection } from '@/components/settings/primitives'
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

const s2Schema = z.object({
  semantic_scholar_api_key: z.string(),
})

type S2Form = z.infer<typeof s2Schema>

interface SemanticScholarSectionProps {
  formData: Settings
  onFormDataChange: (updater: (prev: Settings) => Settings) => void
}

/**
 * Semantic Scholar credentials, rendered as a collapsible sub-section of the
 * External APIs panel (see `ExternalApisCard`). S2 supplies SPECTER2 vectors
 * and paper/author recommendations as a secondary source (always on, not
 * gated on the active backend like OpenAlex). The key is strongly
 * recommended: without it S2 falls back to the shared anonymous pool and
 * 429s frequently, which stalls the Discovery graph lane.
 *
 * The key field writes into the shared `formData`; persistence happens via
 * the Connections section's "Save connection settings" footer. After a save
 * the SettingsPage invalidates `semantic-scholar-status`, so the connection
 * dot re-probes and reflects the new key. The backend masks the stored value
 * (`****<suffix>`) on GET and skips re-rotation when the masked echo is
 * submitted unchanged.
 */
export function SemanticScholarSection({
  formData,
  onFormDataChange,
}: SemanticScholarSectionProps) {
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

  return (
    <SettingsSection
      title={
        <span className="inline-flex items-center gap-2">
          Semantic Scholar
          <ConnectionPill valid={statusQuery.data?.valid} loading={statusQuery.isLoading} />
        </span>
      }
      description="Secondary source for SPECTER2 vectors and paper/author recommendations."
      defaultOpen={false}
    >
      <Form {...form}>
        <form className="space-y-4" onSubmit={(e) => e.preventDefault()}>
          {/* The green/red pill lives in the section header (visible collapsed);
              this box reveals the specific probe reason + the Re-check action,
              which can't sit in the header (it's a button inside the trigger). */}
          <div className="flex items-center justify-between gap-2 rounded-md border border-[var(--color-border)] bg-surface-2 px-3 py-2">
            <span className="text-xs text-slate-500">
              {statusQuery.data?.detail ?? 'Checking connection…'}
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
    </SettingsSection>
  )
}
