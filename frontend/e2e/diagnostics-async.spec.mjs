import assert from 'node:assert/strict'
import { chromium } from 'playwright'

const base = process.env.ALMA_URL ?? 'http://127.0.0.1:5173'
assert.ok(['localhost', '127.0.0.1'].includes(new URL(base).hostname))
const browser = await chromium.launch({ executablePath: process.env.ALMA_BROWSER_EXECUTABLE })
let probeId
try {
  const page = await browser.newPage({ viewport: { width: 1440, height: 1000 } })
  assert.equal((await (await page.request.get(`${base}/api/v1/extension/ping`)).json()).instance.profile, 'dev')
  let posts = 0
  let fail = true
  const errors = []
  page.on('pageerror', error => errors.push(String(error)))
  await page.route('**/api/v1/insights/diagnostics/refresh?*', route => {
    posts++
    return fail
      ? route.fulfill({ status: 503, contentType: 'application/json', body: '{"detail":"Diagnostic refresh temporarily unavailable"}' })
      : route.continue()
  })
  await page.goto(`${base}/#/health`, { waitUntil: 'domcontentloaded' })
  const retry = page.getByRole('button', { name: 'Retry diagnostics', exact: true })
  await retry.waitFor({ timeout: 60000 })
  assert.equal(posts, 1, 'one refresh owner across Health and System status')
  const tour = page.getByRole('button', { name: /Skip tour/i })
  if (await tour.count()) await tour.first().click()
  fail = false
  const queued = page.waitForResponse(r => r.url().includes('/diagnostics/refresh') && r.status() === 202)
  await retry.click()
  const job = await (await queued).json()
  assert.ok(job.job_id)
  let start = performance.now()
  assert.equal((await page.request.get(`${base}/api/v1/insights/diagnostics`)).status(), 200)
  const readMs = Math.round(performance.now() - start)
  start = performance.now()
  const save = await page.request.post(`${base}/api/v1/lenses`, { data: { name: 'Task73 temporary write probe', context_type: 'topic_keyword', context_config: { keyword: 'Task73 probe' } } })
  assert.ok(save.ok(), await save.text())
  probeId = (await save.json()).id
  const saveMs = Math.round(performance.now() - start)
  assert.ok(readMs < 3000 && saveMs < 3000, JSON.stringify({ readMs, saveMs }))
  const refresh = page.getByRole('button', { name: 'Refresh diagnostics', exact: true })
  await refresh.waitFor({ timeout: 180000 })
  assert.equal(await retry.count(), 0)
  const data = await (await page.request.get(`${base}/api/v1/insights/diagnostics`)).json()
  assert.ok(data?.evaluation?.scorecards?.length)
  await refresh.scrollIntoViewIfNeeded()
  await page.screenshot({ path: '/tmp/alma73-diagnostics-desktop.png' })
  await page.setViewportSize({ width: 390, height: 844 })
  await refresh.scrollIntoViewIfNeeded()
  await page.screenshot({ path: '/tmp/alma73-diagnostics-mobile.png' })
  assert.ok(await page.evaluate(() => document.documentElement.scrollWidth <= innerWidth + 1), 'mobile overflow')
  assert.deepEqual(errors, [])
  console.log(JSON.stringify({ result: 'PASS', readMs, saveMs, posts, scorecards: data.evaluation.scorecards.length, job: job.job_id }))
} finally {
  if (probeId) {
    const context = await browser.newContext()
    assert.ok((await context.request.delete(`${base}/api/v1/lenses/${probeId}`)).ok())
  }
  await browser.close()
}
