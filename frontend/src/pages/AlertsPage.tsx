import { useState } from 'react'
import { AlertTemplatesSection } from '@/components/alerts/AlertTemplatesSection'
import { AlertRulesSection } from '@/components/alerts/AlertRulesSection'
import { AlertsDeliverySection } from '@/components/alerts/AlertsDeliverySection'
import { AlertHistorySection } from '@/components/alerts/AlertHistorySection'
import { Tabs, TabsContent, TabsList, TabsTrigger } from '@/components/ui/tabs'

type SectionId = 'rules' | 'alerts' | 'history'

const SECTIONS: { id: SectionId; label: string }[] = [
  { id: 'rules', label: 'Rules' },
  { id: 'alerts', label: 'Digests' },
  { id: 'history', label: 'History' },
]

export function AlertsPage() {
  const [activeSection, setActiveSection] = useState<SectionId>('rules')

  return (
    <div className="space-y-6">
      <p className="text-sm text-slate-500">
        Configure digest rules, delivery schedules, and review history.
      </p>

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
          <AlertRulesSection />
        </TabsContent>
        <TabsContent value="alerts">
          <AlertsDeliverySection />
        </TabsContent>
        <TabsContent value="history">
          <AlertHistorySection />
        </TabsContent>
      </Tabs>
    </div>
  )
}
