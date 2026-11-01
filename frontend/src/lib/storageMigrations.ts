/**
 * Browser storage migrator. `alma.storage.version` is the validator: when it is
 * current nothing runs, and forward-only code reads only the current keys.
 */
const VERSION_KEY = 'alma.storage.version'
const CURRENT_VERSION = '1'

/** Left by the list views removed on 2026-11-01. Nothing reads them. */
const REMOVED_KEYS = [
  'alma.library.saved.viewMode',
  'alma.library.saved.cardSort',
  'alma.datatable.feed.compact',
  'alma.datatable.discovery.compact',
]

export function migrateLocalStorage(storage: Storage = window.localStorage): void {
  try {
    if (storage.getItem(VERSION_KEY) === CURRENT_VERSION) return
    for (const key of REMOVED_KEYS) storage.removeItem(key)
    storage.setItem(VERSION_KEY, CURRENT_VERSION)
  } catch {
    // Storage can be blocked; a leftover key is harmless, so the app goes on.
  }
}
