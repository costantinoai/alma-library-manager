import assert from 'node:assert/strict'
import { chromium } from 'playwright'

const base = process.env.ALMA_URL ?? 'http://127.0.0.1:5173'
assert.ok(['localhost', '127.0.0.1'].includes(new URL(base).hostname))
const browser = await chromium.launch({ executablePath: process.env.ALMA_BROWSER_EXECUTABLE })
let probeId
try {
  const page = await browser.newPage({ viewport: { width: 1440, height: 1000 } })
  const identity = await (await page.request.get(`${base}/api/v1/extension/ping`)).json()
  assert.equal(identity.instance.profile, 'dev')
  const requests = []
  const errors = []
  page.on('pageerror', error => errors.push(String(error)))
  page.on('request', request => {
    if (/\/lenses\/[^/]+\/branches/.test(request.url())) requests.push({ url: request.url(), method: request.method() })
  })
  await page.goto(`${base}/#/discovery`, { waitUntil: 'domcontentloaded' })
  const studio = page.locator('[data-tour="discovery-branches"]')
  await studio.locator('summary').first().waitFor({ timeout: 60000 })
  await page.waitForTimeout(1800)
  assert.equal(requests.length, 0, 'closed Branch Studio makes no preview requests')
  const tour = page.getByRole('button', { name: /Skip tour/i })
  if (await tour.count()) await tour.first().click()
  const queued = page.waitForResponse(r => r.request().method() === 'POST' && r.url().includes('/branches/refresh'))
  await studio.locator('summary').first().click()
  const response = await queued
  assert.equal(response.status(), 202)
  const job = await response.json()
  const previewUrl = response.url().replace('/branches/refresh', '/branches')

  // Exercise a real, reversible foreground write while the preview worker runs.
  let start = performance.now()
  const save = await page.request.post(`${base}/api/v1/lenses`, { data: { name: 'Task70 temporary concurrency probe', context_type: 'topic_keyword', context_config: { keyword: 'Task70 probe' } } })
  assert.ok(save.ok(), await save.text())
  probeId = (await save.json()).id
  const saveMs = Math.round(performance.now() - start)
  start = performance.now()
  const during = await page.request.get(previewUrl)
  assert.equal(during.status(), 200)
  const duringMs = Math.round(performance.now() - start)
  assert.ok(duringMs < 3000, `cached GET stalled: ${duringMs}ms`)
  assert.ok(saveMs < 3000, `foreground write stalled: ${saveMs}ms`)
  if (job.job_id) {
    let status
    for (let n = 0; n < 120; n++) {
      status = await (await page.request.get(`${base}/api/v1/activity/${job.job_id}`)).json()
      if (['completed', 'failed', 'cancelled'].includes(status.status)) break
      await page.waitForTimeout(1000)
    }
    assert.equal(status.status, 'completed', JSON.stringify(status))
  }
  await studio.getByRole('button', { name: 'Preview', exact: true }).waitFor()
  await page.waitForFunction(() => !document.querySelector('[data-tour="discovery-branches"]')?.textContent.includes('Building branch studio'))
  const preview = await (await page.request.get(previewUrl)).json()
  assert.ok(preview && preview.branches.length > 0)
  assert.equal(await studio.getByRole('alert').count(), 0)
  await studio.scrollIntoViewIfNeeded()
  await page.screenshot({ path: '/tmp/alma70-branches-desktop.png' })
  await studio.locator('summary').first().click()
  const count = requests.length
  await page.waitForTimeout(2500)
  assert.equal(requests.length, count, 'closing stops preview requests')
  await page.setViewportSize({ width: 390, height: 844 })
  await studio.locator('summary').first().click()
  await studio.scrollIntoViewIfNeeded()
  await page.screenshot({ path: '/tmp/alma70-branches-mobile.png' })
  assert.ok(await page.evaluate(() => document.documentElement.scrollWidth <= innerWidth + 1), 'mobile overflow')
  assert.deepEqual(errors, [])
  console.log(JSON.stringify({ result: 'PASS', branches: preview.branches.length, foregroundSaveMs: saveMs, cachedGetMs: duringMs, job: job.job_id, closedRequests: 0 }))
} finally {
  if (probeId) {
    const context = await browser.newContext()
    const response = await context.request.delete(`${base}/api/v1/lenses/${probeId}`)
    assert.ok(response.ok(), 'temporary lens cleanup failed')
  }
  await browser.close()
}
