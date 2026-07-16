import { toast as sonnerToast } from 'sonner'
import { ConnectorToastCard } from '@/components/connector/ConnectorToastCard'
import type { ConnectorNotice } from '@/lib/connector'

/**
 * The startup connector notice — the "linked badge" treatment, tuned for the
 * one case it ever fires: the installed connector and this ALMa build no
 * longer speak the same save-contract. A healthy connector is silent, so this
 * card always means "something needs updating".
 *
 * It mirrors the app's standard toast chrome (warm `alma-chrome` surface, gold
 * left-edge ribbon, `shadow-paper-lg`). The mark is a small gold chip with a
 * single entrance pulse — emphasis without dropping a saturated alarm colour
 * into a warm card (per the surface-contrast discipline). Built via
 * `sonner.custom` so we own the layout while reusing the Toaster's stacking,
 * positioning, and dismissal.
 */

/** Fire the connector notice as a sonner toast; `onDismiss` records the
 *  dismissal so the same notice does not nag on the next reload. */
export function showConnectorToast(notice: ConnectorNotice, onDismiss: () => void) {
  sonnerToast.custom(
    (id) => (
      <ConnectorToastCard
        notice={notice}
        onClose={() => {
          onDismiss()
          sonnerToast.dismiss(id)
        }}
      />
    ),
    { duration: 14000 },
  )
}
