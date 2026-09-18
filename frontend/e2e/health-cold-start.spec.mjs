import assert from 'node:assert/strict'
import { chromium } from 'playwright'

const base = process.env.ALMA_URL ?? 'http://127.0.0.1:5173'
assert.ok(['localhost', '127.0.0.1'].includes(new URL(base).hostname))
const browser = await chromium.launch({ executablePath: process.env.ALMA_BROWSER_EXECUTABLE })
try {
  const page = await browser.newPage({ viewport: { width: 1440, height: 1000 } })
  assert.equal((await (await page.request.get(`${base}/api/v1/extension/ping`)).json()).instance.profile, 'dev')
  const counts = new Map()
  for (const path of ['/insights/health', '/health/operations']) {
    counts.set(path, 0)
    await page.route(`**/api/v1${path}`, async route => {
      const response = await route.fetch()
      const payload = await response.json()
      const count = counts.get(path) + 1
      counts.set(path, count)
      // Simulate first startup before the background publisher has a snapshot.
      await route.fulfill({ response, json: count === 1 ? { ...payload, generated_at: null } : payload })
    })
  }
  await page.goto(`${base}/#/health`, { waitUntil: 'domcontentloaded' })
  await page.getByText('Assessment pending', { exact: true }).waitFor()
  await page.getByText(/Last assessed/).first().waitFor({ timeout: 15000 })
  assert.ok([...counts.values()].every(count => count >= 2), JSON.stringify([...counts]))
  await page.screenshot({ path: '/tmp/alma76-health-cold.png' })
  console.log(JSON.stringify({ result: 'PASS', requests: [...counts] }))
} finally {
  await browser.close()
}
