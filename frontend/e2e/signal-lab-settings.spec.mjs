/** Real Settings page: ordinary use, advanced controls and honest failure states.
 * Requires the isolated dev stack. Only reads the backend; edits remain local,
 * and the reset confirmation is cancelled. No answers or settings are changed.
 */
import assert from 'node:assert/strict'
import { chromium } from 'playwright'

const base = process.env.ALMA_URL ?? 'http://127.0.0.1:5173'
const browser = await chromium.launch({
  executablePath: process.env.ALMA_BROWSER_EXECUTABLE || undefined,
})
const mutations = []
const errors = []
try {
  const page = await browser.newPage({ viewport: { width: 1440, height: 1100 } })
  page.on('pageerror', (error) => errors.push(String(error)))
  page.on('request', (request) => {
    if (request.url().includes('/signal-lab/') && request.method() !== 'GET') {
      mutations.push(`${request.method()} ${request.url()}`)
    }
  })
  async function openSettings() {
    await page.goto(`${base}/#settings`, { waitUntil: 'domcontentloaded' })
    const card = page.locator('#signal-lab')
    await card.getByRole('switch').waitFor({ timeout: 30_000 })
    await card.scrollIntoViewIfNeeded()
    return card
  }
  const card = await openSettings()
  const toggle = card.getByRole('switch')
  await page.waitForFunction(() => {
    const control = document.querySelector('#signal-lab [role="switch"]')
    return control && !control.hasAttribute('disabled')
  })
  assert.equal(await toggle.isEnabled(), true)
  assert.equal(await card.getByRole('button', { name: 'Reset Signal Lab', exact: true }).isVisible(), false)
  assert.equal(await card.getByRole('button', { name: 'Save Signal Lab', exact: true }).count(), 0)
  assert.equal(await card.getByLabel('Region nudge (points)', { exact: true }).isVisible(), false)
  await page.screenshot({ path: '/tmp/alma-lab-settings-default.png' })

  const summary = card.locator('summary', { hasText: 'Advanced settings and evidence' })
  await summary.focus()
  await page.keyboard.press('Enter')
  const weight = card.getByLabel('Region nudge (points)', { exact: true })
  await weight.waitFor({ state: 'visible' })
  assert.equal(await card.getByRole('button', { name: 'Reset Signal Lab', exact: true }).isVisible(), true)
  const current = Number(await weight.inputValue())
  await weight.fill(String(current === 0 ? 1 : 0))
  await summary.click()
  assert.equal(await weight.isVisible(), false)
  assert.equal(await card.getByText('Unsaved changes to advanced settings.').isVisible(), true)
  assert.equal(await card.getByRole('button', { name: 'Save Signal Lab', exact: true }).isEnabled(), true)
  await summary.click()
  await weight.fill(String(current))
  await card.getByRole('button', { name: 'Reset Signal Lab', exact: true }).click()
  const dialog = page.getByRole('alertdialog')
  await dialog.waitFor({ state: 'visible' })
  assert.match(await dialog.innerText(), /Library, ratings, and ordinary/)
  await dialog.getByRole('button', { name: 'Cancel', exact: true }).click()
  await summary.scrollIntoViewIfNeeded()
  await page.screenshot({ path: '/tmp/alma-lab-settings-advanced.png' })
  await summary.click()

  await page.setViewportSize({ width: 390, height: 844 })
  await card.scrollIntoViewIfNeeded()
  assert.equal(await toggle.isVisible(), true)
  assert.equal(await card.getByRole('button', { name: 'Reset Signal Lab', exact: true }).isVisible(), false)
  const bounds = await card.boundingBox()
  assert.ok(bounds && bounds.x >= 0 && bounds.x + bounds.width <= 390, 'Lab card fits the mobile viewport')
  await page.screenshot({ path: '/tmp/alma-lab-settings-mobile.png' })

  // Controlled failures over the real page verify disclosure cannot conceal
  // an unavailable service or claim that retained learning data is absent.
  await page.route('**/api/v1/signal-lab/model', (route) => route.fulfill({
    status: 503, contentType: 'application/json', body: JSON.stringify({ detail: 'Model unavailable' }),
  }))
  await page.reload({ waitUntil: 'domcontentloaded' })
  await card.getByText('Learning status is unavailable. Try again shortly.').waitFor({ timeout: 30_000 })
  assert.equal(await card.getByText('No fitted model yet — play rounds on Home.').count(), 0)
  await page.route('**/api/v1/signal-lab/settings', (route) => route.fulfill({
    status: 503, contentType: 'application/json', body: JSON.stringify({ detail: 'Settings unavailable' }),
  }))
  await page.reload({ waitUntil: 'domcontentloaded' })
  await card.getByRole('alert').waitFor({ timeout: 30_000 })
  assert.equal(await toggle.isEnabled(), false)
  await page.unroute('**/api/v1/signal-lab/model')
  await page.unroute('**/api/v1/signal-lab/settings')
  await card.getByRole('button', { name: 'Retry loading Signal Lab' }).click()
  await page.waitForFunction(() => {
    const control = document.querySelector('#signal-lab [role="switch"]')
    return control && !control.hasAttribute('disabled')
  })
  await card.getByRole('button', { name: 'Retry loading Signal Lab' }).waitFor({ state: 'hidden' })
  assert.deepEqual(mutations, [], 'browser verification must not mutate Signal Lab data')
  assert.deepEqual(errors, [], 'no JavaScript errors on the Settings page')
  console.log('PASS: default/advanced disclosure, retained edits, reset confirmation, mobile, failure and retry states; no writes')
} finally {
  await browser.close()
}
