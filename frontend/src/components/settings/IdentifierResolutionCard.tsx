import { useEffect } from 'react'
import { useForm } from 'react-hook-form'
import { zodResolver } from '@hookform/resolvers/zod'
import { z } from 'zod'
import { Search } from 'lucide-react'

import type { Settings } from '@/api/client'
import { SettingsCard, ToggleRow } from '@/components/settings/primitives'
import { Checkbox } from '@/components/ui/checkbox'
import { Form, FormControl, FormField, FormItem } from '@/components/ui/form'

const identifierSchema = z.object({
  id_resolution_semantic_scholar_enabled: z.boolean(),
  id_resolution_orcid_enabled: z.boolean(),
  id_resolution_scholar_scrape_auto_enabled: z.boolean(),
  id_resolution_scholar_scrape_manual_enabled: z.boolean(),
})

type IdentifierForm = z.infer<typeof identifierSchema>

interface IdentifierResolutionCardProps {
  formData: Settings
  onFormDataChange: (updater: (prev: Settings) => Settings) => void
}

interface Toggle {
  name: keyof IdentifierForm
  title: string
  description: string
  tone: 'default' | 'warning'
}

const TOGGLES: Toggle[] = [
  {
    name: 'id_resolution_semantic_scholar_enabled',
    title: 'Semantic Scholar bridge',
    description: 'Use Semantic Scholar API to bridge OpenAlex/ORCID to Scholar IDs.',
    tone: 'default',
  },
  {
    name: 'id_resolution_orcid_enabled',
    title: 'ORCID link lookup',
    description: 'Parse ORCID public researcher links for Scholar profile URLs.',
    tone: 'default',
  },
  {
    name: 'id_resolution_scholar_scrape_auto_enabled',
    title: 'Auto Scholar scraping fallback',
    description: 'When enabled, background resolution may use `scholarly` scraping.',
    tone: 'warning',
  },
  {
    name: 'id_resolution_scholar_scrape_manual_enabled',
    title: 'Manual Scholar search in Authors page',
    description:
      'Expose "Search Google Scholar" button for explicit user-triggered scraping.',
    tone: 'default',
  },
]

export function IdentifierResolutionCard({
  formData,
  onFormDataChange,
}: IdentifierResolutionCardProps) {
  const form = useForm<IdentifierForm>({
    resolver: zodResolver(identifierSchema),
    defaultValues: {
      id_resolution_semantic_scholar_enabled: !!formData.id_resolution_semantic_scholar_enabled,
      id_resolution_orcid_enabled: !!formData.id_resolution_orcid_enabled,
      id_resolution_scholar_scrape_auto_enabled:
        !!formData.id_resolution_scholar_scrape_auto_enabled,
      id_resolution_scholar_scrape_manual_enabled:
        !!formData.id_resolution_scholar_scrape_manual_enabled,
    },
  })

  useEffect(() => {
    form.reset({
      id_resolution_semantic_scholar_enabled: !!formData.id_resolution_semantic_scholar_enabled,
      id_resolution_orcid_enabled: !!formData.id_resolution_orcid_enabled,
      id_resolution_scholar_scrape_auto_enabled:
        !!formData.id_resolution_scholar_scrape_auto_enabled,
      id_resolution_scholar_scrape_manual_enabled:
        !!formData.id_resolution_scholar_scrape_manual_enabled,
    })
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [
    formData.id_resolution_semantic_scholar_enabled,
    formData.id_resolution_orcid_enabled,
    formData.id_resolution_scholar_scrape_auto_enabled,
    formData.id_resolution_scholar_scrape_manual_enabled,
  ])

  useEffect(() => {
    const sub = form.watch((values) => {
      onFormDataChange((prev) => ({
        ...prev,
        id_resolution_semantic_scholar_enabled:
          !!values.id_resolution_semantic_scholar_enabled,
        id_resolution_orcid_enabled: !!values.id_resolution_orcid_enabled,
        id_resolution_scholar_scrape_auto_enabled:
          !!values.id_resolution_scholar_scrape_auto_enabled,
        id_resolution_scholar_scrape_manual_enabled:
          !!values.id_resolution_scholar_scrape_manual_enabled,
      }))
    })
    return () => sub.unsubscribe()
  }, [form, onFormDataChange])

  return (
    <SettingsCard
      icon={Search}
      title="Identifier Resolution"
      description="Configure how Scholar IDs are resolved: API-first by default, scraping only when enabled."
    >
      <Form {...form}>
        <form className="space-y-3" onSubmit={(e) => e.preventDefault()}>
          {TOGGLES.map((toggle) => (
            <FormField
              key={toggle.name}
              control={form.control}
              name={toggle.name}
              render={({ field }) => (
                <FormItem className="m-0">
                  <ToggleRow
                    tone={toggle.tone}
                    title={toggle.title}
                    description={toggle.description}
                    control={
                      <FormControl>
                        <Checkbox
                          checked={!!field.value}
                          onCheckedChange={(checked) => field.onChange(checked === true)}
                        />
                      </FormControl>
                    }
                  />
                </FormItem>
              )}
            />
          ))}

          <p className="rounded-md bg-parchment-50 px-3 py-2 text-xs text-slate-600">
            Default recommended strategy: OpenAlex + Semantic Scholar + ORCID automatically,
            Google Scholar scraping only on manual action.
          </p>
        </form>
      </Form>
    </SettingsCard>
  )
}
