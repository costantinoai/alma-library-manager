import { lazy, type ComponentType, type LazyExoticComponent } from 'react'

type PageModule = { default: ComponentType }

/** Places in core pages that an add-on may fill. Core names the place, never the add-on. */
export type AddonSlotName = 'tags.tune'

/** What one add-on tells the shell. Everything else in its folder is private. */
export interface AddonManifest {
  /** The backend add-on id. */
  id: string
  /** Toast name and queries to refresh when one of its operations ends. */
  operations?: { prefix: string; label: string; queryRoots: string[] }
  /** Lazy parts for core pages. A switched-off add-on never fetches them. */
  slots?: Partial<Record<AddonSlotName, () => Promise<PageModule>>>
}

/** An add-on with a page of its own, at the hash route `#/<id>`. */
export interface AddonPageManifest extends AddonManifest {
  label: string
  navGroup: 'Explore' | 'Manage' | 'Control'
  icon: ComponentType<{ className?: string }>
  /** Loads the page chunk. A switched-off add-on never fetches it. */
  load: () => Promise<PageModule>
  /** Extra tokens for the command palette's fuzzy match. */
  keywords?: string[]
}

export interface AddonUi extends AddonPageManifest {
  page: LazyExoticComponent<ComponentType>
}

export interface AddonSlotEntry {
  addonId: string
  Component: LazyExoticComponent<ComponentType>
}

// The glob runs at build time, and it is the only discovery on this side: an
// explicit import would fail the build the moment a folder is deleted, and the
// D27 test deletes folders.
const modules = import.meta.glob<{ default: AddonManifest | AddonPageManifest }>('./*/index.ts', {
  eager: true,
})

function hasPage(manifest: AddonManifest): manifest is AddonPageManifest {
  return 'load' in manifest
}

export const ADDONS: AddonManifest[] = Object.values(modules).map(({ default: manifest }) => manifest)

export const ADDON_PAGES: AddonUi[] = ADDONS.filter(hasPage)
  .map((manifest) => ({ ...manifest, page: lazy(manifest.load) }))
  .sort((a, b) => a.label.localeCompare(b.label))

export function findAddon(id: string): AddonUi | null {
  return ADDON_PAGES.find((addon) => addon.id === id) ?? null
}

const SLOTS: Partial<Record<AddonSlotName, AddonSlotEntry[]>> = {}
for (const manifest of ADDONS) {
  for (const [name, load] of Object.entries(manifest.slots ?? {})) {
    const entries = (SLOTS[name as AddonSlotName] ??= [])
    entries.push({ addonId: manifest.id, Component: lazy(load) })
  }
}

export function slotEntries(name: AddonSlotName): AddonSlotEntry[] {
  return SLOTS[name] ?? []
}
