import assert from 'node:assert/strict'
import { chromium } from 'playwright'

const base = process.env.ALMA_URL ?? 'http://127.0.0.1:5173'
assert.ok(['localhost', '127.0.0.1'].includes(new URL(base).hostname))
const browser = await chromium.launch({ executablePath: process.env.ALMA_BROWSER_EXECUTABLE })
try {
  const page = await browser.newPage({ viewport: { width: 1440, height: 1000 } })
  const identity = await (await page.request.get(`${base}/api/v1/extension/ping`)).json()
  assert.equal(identity.instance.profile, 'dev')
  const requests = []
  const errors = []
  page.on('pageerror', error => errors.push(String(error)))
  page.on('request', request => {
    if (/\/insights(?:\/recommendations)?$|\/ai\/status$/.test(new URL(request.url()).pathname)) requests.push(request.url())
  })
  let failEngagement = true
  await page.route('**/api/v1/insights/recommendations', route => failEngagement
    ? route.fulfill({ status: 503, contentType: 'application/json', body: '{"detail":"Temporary test failure"}' })
    : route.continue())
  await page.goto(`${base}/#/discovery`, { waitUntil: 'domcontentloaded' })
  const performance = page.locator('[data-tour="discovery-performance"]')
  const tuning = page.locator('details').filter({ has: page.locator('summary', { hasText: 'Tune this lens' }) })
  await performance.locator('summary').waitFor({ timeout: 60000 })
  await page.waitForTimeout(1500)
  assert.deepEqual(requests, [], 'closed diagnostics make no detail requests')
  const tour = page.getByRole('button', { name: /Skip tour/i })
  if (await tour.count()) await tour.first().click()
  await performance.locator('summary').click()
  await performance.getByRole('alert').waitFor({ timeout: 15000 })
  failEngagement = false
  const loaded = page.waitForResponse(r => r.url().endsWith('/insights/recommendations') && r.status() === 200)
  await performance.getByRole('button', { name: 'Retry', exact: true }).click()
  const payload = await (await loaded).json()
  assert.ok(typeof payload.total === 'number')
  await performance.getByText('Recommendation engagement', { exact: true }).waitFor()
  await performance.locator('summary').click()
  await tuning.locator('summary').click()
  const slider = tuning.getByRole('slider', { name: 'lexical weight' })
  await slider.focus()
  await slider.press('ArrowRight')
  const draft = await slider.getAttribute('aria-valuenow')
  await tuning.locator('summary').click()
  const closedCount = requests.length
  // Simulate cache becoming stale and window refocus while both folds are closed.
  await page.clock.install()
  await page.clock.fastForward(65000)
  await page.evaluate(() => { document.dispatchEvent(new Event('visibilitychange')); window.dispatchEvent(new Event('focus')) })
  await page.waitForTimeout(500)
  assert.equal(requests.length, closedCount, 'closed folds do not refetch stale queries')
  await page.clock.resume()
  await tuning.locator('summary').click()
  assert.equal(await slider.getAttribute('aria-valuenow'), draft, 'unsaved draft survives closing')
  await tuning.scrollIntoViewIfNeeded()
  await page.screenshot({ path: '/tmp/alma71-diagnostics-desktop.png' })
  await page.setViewportSize({ width: 390, height: 844 })
  await tuning.scrollIntoViewIfNeeded()
  await page.screenshot({ path: '/tmp/alma71-diagnostics-mobile.png' })
  assert.ok(await page.evaluate(() => document.documentElement.scrollWidth <= innerWidth + 1), 'mobile overflow')
  assert.ok(!requests.some(url => url.endsWith('/insights')), 'Discovery never loads the full Insights view')
  assert.deepEqual(errors, [])
  console.log(JSON.stringify({ result: 'PASS', closedRequests: 0, engagementTotal: payload.total, draft, retry: 'recovered' }))
} finally {
  await browser.close()
}
