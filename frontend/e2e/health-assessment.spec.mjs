// Browser smoke for the real RepairCard, with controlled assessment input.
// Run with Vite on :5173. No backend or user data is touched.
import assert from 'node:assert/strict'
import { chromium } from 'playwright'

const browser = await chromium.launch({
  headless: true,
  executablePath: process.env.ALMA_BROWSER_EXECUTABLE || undefined,
})
try {
  const page = await browser.newPage({ viewport: { width: 1150, height: 900 } })
  const errors = []
  page.on('pageerror', (error) => { errors.push(error.message); console.error(error.message) })
  await page.route('**/health-assessment-smoke', (route) => route.fulfill({
    contentType: 'text/html',
    body: '<html><head></head><body><main id="root" style="max-width:960px;margin:32px auto"></main></body></html>',
  }))
  await page.goto('http://127.0.0.1:5173/health-assessment-smoke')
  await page.evaluate(async () => {
    const { mountHealthFixture } = await import('/e2e/health-assessment-harness.js')
    await mountHealthFixture()
  })
  await page.getByText('Unknown', { exact: true }).waitFor()
  assert(await page.getByRole('button', { name: 'Run now', exact: true }).isDisabled())
  assert(await page.getByRole('button', { name: 'Preview', exact: true }).isDisabled())
  assert.match(await page.getByRole('alert').innerText(), /Re-assess/)
  await page.screenshot({ path: '/tmp/alma-health-assessment-error.png', fullPage: true })
  await page.evaluate(() => window.showAssessment(true))
  await page.getByText('5', { exact: true }).waitFor()
  assert(await page.getByRole('button', { name: 'Run now', exact: true }).isEnabled())
  assert.equal(await page.getByRole('alert').count(), 0)
  await page.evaluate(() => window.showSeedPlacement())
  await page.getByText('Suggested authors awaiting map placement', { exact: true }).waitFor()
  assert(await page.getByRole('button', { name: 'Run now', exact: true }).isDisabled())
  await page.getByText(/No more paper seeding is needed/).waitFor()
  await page.screenshot({ path: '/tmp/alma-health-seed-placement.png', fullPage: true })
  assert.deepEqual(errors, [])
  console.log('PASS: assessment failure/recovery and separate seed/map-placement states')
} finally {
  await browser.close()
}
