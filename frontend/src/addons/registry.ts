import { lazy, type ComponentType, type LazyExoticComponent } from 'react'

type PageModule = { default: ComponentType }

/** What one add-on tells the shell. Everything else in its folder is private. */
export interface AddonManifest {
  /** The backend add-on id. It is also the hash route: `#/<id>`. */
  id: string
  label: string
  navGroup: 'Explore' | 'Manage' | 'Control'
  icon: ComponentType<{ className?: string }>
  /** Loads the page chunk. A switched-off add-on never fetches it. */
  load: () => Promise<PageModule>
  /** Extra tokens for the command palette's fuzzy match. */
  keywords?: string[]
  /** Toast name and queries to refresh when one of its operations ends. */
  operations?: { prefix: string; label: string; queryRoots: string[] }
}

export interface AddonUi extends AddonManifest {
  page: LazyExoticComponent<ComponentType>
}

// The glob runs at build time, and it is the only discovery on this side: an
// explicit import would fail the build the moment a folder is deleted, and the
// D27 test deletes folders.
const modules = import.meta.glob<{ default: AddonManifest }>('./*/index.ts', { eager: true })

export const ADDONS: AddonUi[] = Object.values(modules)
  .map(({ default: manifest }) => ({ ...manifest, page: lazy(manifest.load) }))
  .sort((a, b) => a.label.localeCompare(b.label))

export function findAddon(id: string): AddonUi | null {
  return ADDONS.find((addon) => addon.id === id) ?? null
}
