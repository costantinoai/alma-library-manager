import { useQuery } from '@tanstack/react-query'
import { Info } from 'lucide-react'

import { getBootstrap } from '@/api/client'
import { SettingsCard } from '@/components/settings/primitives'

export function AboutCard() {
  const bootstrap = useQuery({ queryKey: ['bootstrap'], queryFn: () => getBootstrap() })

  return (
    <SettingsCard icon={Info} title="About">
      <div className="space-y-2 text-sm text-slate-600">
        <p>
          <span className="font-medium">Version:</span> {bootstrap.data?.app?.version ?? '—'}
        </p>
        <p>
          ALMa (Another Library Manager) helps you track academic publications and
          discover new research.
        </p>
        <div className="flex gap-3 pt-2">
          <a
            href="https://github.com/costantinoai/alma-library-manager"
            target="_blank"
            rel="noopener noreferrer"
            className="text-xs text-alma-600 hover:text-alma-800"
          >
            GitHub Repository
          </a>
          <a
            href="https://openalex.org"
            target="_blank"
            rel="noopener noreferrer"
            className="text-xs text-alma-600 hover:text-alma-800"
          >
            OpenAlex
          </a>
          <a
            href="https://scholar.google.com"
            target="_blank"
            rel="noopener noreferrer"
            className="text-xs text-alma-600 hover:text-alma-800"
          >
            Google Scholar
          </a>
        </div>
      </div>
    </SettingsCard>
  )
}
