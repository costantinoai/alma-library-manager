/**
 * Browser-connector detection helpers.
 *
 * The ALMa Firefox connector (the repo `extension/` add-on) injects two
 * data-attributes on <html> at document_start when its content script runs on
 * an ALMa UI origin:
 *
 *   data-alma-connector           -> connector release version (e.g. "0.14.0")
 *   data-alma-connector-contract  -> the save-contract version it was built for
 *
 * The web app reads the marker, then pings the backend for ITS current
 * save-contract version (`connector_version` from /extension/ping). The only
 * truthful, reliable compatibility signal is the contract version — the app's
 * release version (pyproject) and its API version (app.py `API_VERSION`) use
 * separate numbering and are not comparable, so we never surface an "update
 * available" off a raw version string. A contract mismatch is the one
 * actionable condition: the /save shape changed and one side must update to
 * keep saving. Same contract -> compatible -> the app stays silent.
 */

export interface ConnectorMarker {
  /** Connector release version (manifest) — shown to the user, not compared. */
  version: string
  /** Save-contract version the installed connector was built against. */
  contract: number
}

export interface ConnectorPing {
  /** Backend's CURRENT save-contract version (CONNECTOR_API_VERSION). */
  connector_version?: number | null
  alma_version?: string | null
}

export type ConnectorNoticeKind = 'connector_outdated' | 'alma_outdated'

export interface ConnectorNotice {
  kind: ConnectorNoticeKind
  connectorVersion: string
  connectorContract: number
  backendContract: number
  /**
   * Stable id for this exact situation — a dismissed notice with the same
   * signature stays silent until the versions actually change.
   */
  signature: string
}

/** Read the connector marker off <html>, or null when it is not installed. */
export function readConnectorMarker(doc: Document = document): ConnectorMarker | null {
  const el = doc.documentElement
  const version = el?.dataset?.almaConnector
  if (!version) return null
  const contract = Number.parseInt(el.dataset.almaConnectorContract ?? '', 10)
  return { version, contract: Number.isFinite(contract) ? contract : NaN }
}

/**
 * Decide whether to surface a notice. Returns null when the connector is
 * compatible (same contract — the healthy case stays silent) or when either
 * contract is unknown (never guess).
 */
export function decideConnectorNotice(
  marker: ConnectorMarker,
  ping: ConnectorPing,
): ConnectorNotice | null {
  // Number(null) is 0 (finite!), so reject null/undefined before coercing —
  // an unknown backend contract must read as "unknown", never as contract 0.
  if (ping?.connector_version == null) return null
  const backendContract = Number(ping.connector_version)
  if (!Number.isFinite(marker.contract) || !Number.isFinite(backendContract)) return null
  if (marker.contract === backendContract) return null // compatible -> silent

  const kind: ConnectorNoticeKind =
    marker.contract < backendContract ? 'connector_outdated' : 'alma_outdated'
  return {
    kind,
    connectorVersion: marker.version,
    connectorContract: marker.contract,
    backendContract,
    signature: `${marker.version}|${marker.contract}->${backendContract}`,
  }
}
