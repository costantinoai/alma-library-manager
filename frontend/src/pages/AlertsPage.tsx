import { useState } from 'react'
import { AlertTemplatesSection } from '@/components/alerts/AlertTemplatesSection'
import { AlertRulesSection } from '@/components/alerts/AlertRulesSection'
import { AlertsDeliverySection } from '@/components/alerts/AlertsDeliverySection'
import { AlertHistorySection } from '@/components/alerts/AlertHistorySection'
import { ConceptCallout } from '@/components/ui/concept-callout'
import { Tabs, TabsContent, TabsList, TabsTrigger } from '@/components/ui/tabs'

type SectionId = 'rules' | 'alerts' | 'history'

const SECTIONS: { id: SectionId; label: string }[] = [
  { id: 'rules', label: 'Rules' },
  { id: 'alerts', label: 'Digests' },
  { id: 'history', label: 'History' },
]

export function AlertsPage() {
  const [activeSection, setActiveSection] = useState<SectionId>('rules')
  // Digest whose history the user asked to see (via a digest card's outcome
  // chip). Passed down so the History tab opens pre-filtered.
  const [historyAlertId, setHistoryAlertId] = useState<string | null>(null)

  return (
    <div className="space-y-6">
      <p className="text-sm text-slate-500">
        Configure digest rules, delivery schedules, and review history.
      </p>

      <ConceptCallout
        eyebrow="How does this work?"
        summary="Rules detect papers; digests deliver them on a schedule."
      >
        <p>
          A <strong>rule</strong> describes what to watch — an author, a feed monitor, a
          collection, keywords, a Discovery lens… A rule on its own does nothing: it must be
          assigned to a <strong>digest</strong>, which owns the delivery side (Slack / email
          channels and a manual, daily, or weekly schedule).
        </p>
        <p className="mt-2">
          When a digest runs, it combines the papers matched by all of its rules, drops
          anything it already delivered on that channel (each digest sends a given paper
          once per channel), and sends the rest. Every run is recorded in{' '}
          <strong>History</strong>.
        </p>
      </ConceptCallout>

      <AlertTemplatesSection />

      <Tabs
        value={activeSection}
        onValueChange={(value) => setActiveSection(value as SectionId)}
      >
        <TabsList>
          {SECTIONS.map((section) => (
            <TabsTrigger key={section.id} value={section.id}>
              {section.label}
            </TabsTrigger>
          ))}
        </TabsList>

        <TabsContent value="rules">
          <AlertRulesSection onGoToDigests={() => setActiveSection('alerts')} />
        </TabsContent>
        <TabsContent value="alerts">
          <AlertsDeliverySection
            onShowHistory={(alertId) => {
              setHistoryAlertId(alertId)
              setActiveSection('history')
            }}
          />
        </TabsContent>
        <TabsContent value="history">
          <AlertHistorySection initialAlertId={historyAlertId} />
        </TabsContent>
      </Tabs>
    </div>
  )
}
