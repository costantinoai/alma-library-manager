import { Bell } from 'lucide-react'

import type { AddonPageManifest } from '../registry'

const alerts: AddonPageManifest = {
  id: 'alerts',
  label: 'Alerts',
  navGroup: 'Manage',
  icon: Bell,
  load: () => import('./AlertsPage').then((m) => ({ default: m.AlertsPage })),
  keywords: ['digests', 'rules'],
  operations: {
    prefix: 'alerts.',
    label: 'Alerts',
    queryRoots: ['alerts', 'alert-rules', 'alert-history', 'alert-templates', 'insights-diagnostics'],
  },
}

export default alerts
