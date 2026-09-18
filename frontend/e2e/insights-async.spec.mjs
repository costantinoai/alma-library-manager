import assert from 'node:assert/strict'
import { chromium } from 'playwright'

const base = process.env.ALMA_URL ?? 'http://127.0.0.1:5173'
assert.ok(['localhost', '127.0.0.1'].includes(new URL(base).hostname))
const browser = await chromium.launch({ executablePath: process.env.ALMA_BROWSER_EXECUTABLE })
let probeId
try {
  const page = await browser.newPage({ viewport: { width: 1440, height: 1000 } })
  assert.equal((await (await page.request.get(`${base}/api/v1/extension/ping`)).json()).instance.profile, 'dev')
  const errors = []
  page.on('pageerror', error => errors.push(String(error)))
  // Force an observable queue failure, then exercise recovery through the real route.
  let fail = true
  await page.route('**/api/v1/insights/refresh?*', route => fail
    ? route.fulfill({ status: 503, contentType: 'application/json', body: '{"detail":"Refresh temporarily unavailable"}' })
    : route.continue())
  await page.goto(`${base}/#/library?tab=analytics`, { waitUntil: 'domcontentloaded' })
  const retry = page.getByRole('button', { name: 'Retry', exact: true })
  await retry.waitFor({ timeout: 60000 })
  const tour = page.getByRole('button', { name: /Skip tour/i })
  if (await tour.count()) await tour.first().click()
  fail = false
  const queued = page.waitForResponse(r => r.url().includes('/insights/refresh') && r.request().method() === 'POST' && r.status() === 202)
  await retry.click()
  const job = await (await queued).json()
  assert.ok(job.job_id)
  let start = performance.now()
  const read = await page.request.get(`${base}/api/v1/insights`)
  const readMs = Math.round(performance.now() - start)
  assert.equal(read.status(), 200)
  start = performance.now()
  const save = await page.request.post(`${base}/api/v1/lenses`, { data: { name: 'Task72 temporary concurrency probe', context_type: 'topic_keyword', context_config: { keyword: 'Task72 probe' } } })
  assert.ok(save.ok(), await save.text())
  probeId = (await save.json()).id
  const saveMs = Math.round(performance.now() - start)
  assert.ok(readMs < 3000 && saveMs < 3000, JSON.stringify({ readMs, saveMs }))
  await page.getByRole('button', { name: 'Refresh analytics', exact: true }).waitFor({ timeout: 120000 })
  assert.equal(await retry.count(), 0)
  const snapshot = await (await page.request.get(`${base}/api/v1/insights`)).json()
  assert.ok(snapshot?.summary)
  await page.screenshot({ path: '/tmp/alma72-insights-desktop.png' })
  await page.setViewportSize({ width: 390, height: 844 })
  await page.screenshot({ path: '/tmp/alma72-insights-mobile.png' })
  assert.ok(await page.evaluate(() => document.documentElement.scrollWidth <= innerWidth + 1), 'mobile overflow')
  assert.deepEqual(errors, [])
  console.log(JSON.stringify({ result: 'PASS', readMs, saveMs, job: job.job_id, retry: 'recovered' }))
} finally {
  if (probeId) {
    const context = await browser.newContext()
    assert.ok((await context.request.delete(`${base}/api/v1/lenses/${probeId}`)).ok())
  }
  await browser.close()
}
