import { useEffect } from 'react'
import { useForm } from 'react-hook-form'
import { zodResolver } from '@hookform/resolvers/zod'
import { z } from 'zod'
import { useQuery } from '@tanstack/react-query'

import { api, type Settings } from '@/api/client'
import { ConnectionBadge, ConnectionLine, FieldStatus } from '@/components/settings/ConnectionStatus'
import { SettingsSection } from '@/components/settings/primitives'
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
 * the Connections section's "Save connection settings" footer, and that save
 * checks the key if it changed. The backend masks the stored value
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

  // What the server last sent (masked), so "typed but not saved" is a fact.
  const savedQuery = useQuery({
    queryKey: ['settings'],
    queryFn: () => api.get<Settings>('/settings'),
    enabled: false,
  })
  const savedKey = savedQuery.data?.semantic_scholar_api_key ?? ''

  return (
    <SettingsSection
      title={
        <span className="inline-flex items-center gap-2">
          Semantic Scholar
          <ConnectionBadge id="semantic_scholar" />
        </span>
      }
      description="Secondary source for SPECTER2 vectors and paper/author recommendations."
      defaultOpen={false}
    >
      <Form {...form}>
        <form className="space-y-4" onSubmit={(e) => e.preventDefault()}>
          <ConnectionLine id="semantic_scholar" />

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
                <FieldStatus
                  id="semantic_scholar"
                  field="semantic_scholar_api_key"
                  dirty={field.value !== savedKey}
                />
                <FormMessage />
              </FormItem>
            )}
          />
        </form>
      </Form>
    </SettingsSection>
  )
}
