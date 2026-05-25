import { describe, it, expect, afterEach } from 'vitest'
import {
  decideConnectorNotice,
  readConnectorMarker,
  type ConnectorMarker,
} from './connector'

const marker = (version: string, contract: number): ConnectorMarker => ({ version, contract })

describe('decideConnectorNotice', () => {
  it('stays silent when the contract matches (healthy connector)', () => {
    expect(decideConnectorNotice(marker('0.14.0', 1), { connector_version: 1 })).toBeNull()
  })

  it('flags the connector as outdated when its contract is behind the backend', () => {
    const notice = decideConnectorNotice(marker('0.14.0', 1), { connector_version: 2 })
    expect(notice?.kind).toBe('connector_outdated')
    expect(notice?.signature).toBe('0.14.0|1->2')
  })

  it('flags ALMa as outdated when the connector contract is ahead', () => {
    const notice = decideConnectorNotice(marker('0.16.0', 3), { connector_version: 2 })
    expect(notice?.kind).toBe('alma_outdated')
  })

  it('never guesses when a contract version is missing or unknown', () => {
    expect(decideConnectorNotice(marker('0.14.0', NaN), { connector_version: 1 })).toBeNull()
    expect(decideConnectorNotice(marker('0.14.0', 1), { connector_version: null })).toBeNull()
    expect(decideConnectorNotice(marker('0.14.0', 1), {})).toBeNull()
  })

  it('builds a signature that changes with the versions so dismissals reset', () => {
    const a = decideConnectorNotice(marker('0.14.0', 1), { connector_version: 2 })
    const b = decideConnectorNotice(marker('0.15.0', 1), { connector_version: 2 })
    expect(a?.signature).not.toBe(b?.signature)
  })
})

describe('readConnectorMarker', () => {
  afterEach(() => {
    document.documentElement.removeAttribute('data-alma-connector')
    document.documentElement.removeAttribute('data-alma-connector-contract')
  })

  it('returns null when no marker is present (connector not installed)', () => {
    expect(readConnectorMarker()).toBeNull()
  })

  it('reads the release version and contract off <html>', () => {
    document.documentElement.dataset.almaConnector = '0.14.0'
    document.documentElement.dataset.almaConnectorContract = '1'
    expect(readConnectorMarker()).toEqual({ version: '0.14.0', contract: 1 })
  })

  it('marks the contract NaN when the attribute is absent or non-numeric', () => {
    document.documentElement.dataset.almaConnector = '0.14.0'
    expect(readConnectorMarker()?.contract).toBeNaN()
  })
})
