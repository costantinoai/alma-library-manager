/**
 * The plugin catalogue, driven for real.
 *
 * What only a browser can show: that a plugin which is OFF offers no setup, no
 * Save and no Test, and that its configuration is never even fetched — the
 * three things that were all live on a deactivated plugin until 2026-09-20.
 * Then that switching one on reveals its setup, reads its config exactly once,
 * and that switching it off again puts it away.
 *
 * Needs a dev stack on a FRESH profile (every plugin off).
 * Run:  ALMA_URL=http://127.0.0.1:5198 npm run e2e:plugins
 * Screenshots go to $SHOTS_DIR (default /tmp) — LOOK at them.
 */
import { chromium } from 'playwright'
const BASE = process.env.ALMA_URL ?? 'http://127.0.0.1:5173'
const SHOTS = process.env.SHOTS_DIR ?? '/tmp'
const fails = []
const check = (ok, msg) => { console.log(`${ok ? '  ok  ' : '  FAIL'} ${msg}`); if (!ok) fails.push(msg) }

const browser = await chromium.launch()
const ctx = await browser.newContext({ viewport: { width: 1280, height: 1000 } })
await ctx.addInitScript(() => {
  for (const p of ['home', 'feed', 'authors', 'library', 'discovery']) localStorage.setItem(`alma.tour.${p}.completed`, 'done')
})
const page = await ctx.newPage()
const configCalls = []
page.on('request', (r) => { if (/\/plugins\/[^/]+\/config/.test(r.url())) configCalls.push(r.url()) })
page.on('pageerror', (e) => check(false, `page error: ${String(e).slice(0, 160)}`))

await page.goto(`${BASE}/#/settings?anchor=plugins`, { waitUntil: 'networkidle' })
await page.waitForTimeout(1500)
const card = page.locator('[data-anchor="plugins"]')
await card.scrollIntoViewIfNeeded()
await page.waitForTimeout(500)

check(await card.getByText('Slack', { exact: true }).count() === 1, 'the catalogue lists Slack as a row')
check(await card.getByText('Email', { exact: true }).count() === 1, 'the catalogue lists Email as a row')
check(await card.getByText('Off', { exact: true }).count() === 2, 'both ship switched off')
check(await card.getByRole('button', { name: /save plugin settings/i }).count() === 0, 'an off plugin offers no Save')
check(await card.getByRole('button', { name: /test connection/i }).count() === 0, 'an off plugin offers no Test')
check(await card.locator('input[type="password"], input[type="text"]').count() === 0, 'an off plugin shows no setup fields')
check(configCalls.length === 0, `no configuration is read while everything is off (${configCalls.length} call(s))`)
await page.screenshot({ path: `${SHOTS}/plugins-catalogue-off.png`, clip: await card.boundingBox() })
{
  const box = await card.boundingBox()
  await page.screenshot({
    path: `${SHOTS}/plugins-in-context.png`,
    clip: { x: 0, y: Math.max(0, box.y - 320), width: 1280, height: Math.min(1000, box.height + 640) },
  })
}

await card.getByLabel('Switch on Slack').click()
await page.waitForTimeout(1500)
check(await card.getByRole('button', { name: /save plugin settings/i }).count() === 1, 'switching it on reveals its setup')
check(await card.getByRole('button', { name: /test connection/i }).count() === 1, 'and its Test')
check(await card.getByRole('link', { name: /guide/i }).count() === 1, 'and a link to its guide')
check(configCalls.length === 1, `its configuration is read exactly once, now (${configCalls.length})`)
check(await card.getByText('Needs setup').count() === 1, 'on but unconfigured reads "Needs setup"')
await page.screenshot({ path: `${SHOTS}/plugins-catalogue-on.png`, clip: await card.boundingBox() })

await card.getByLabel('Switch off Slack').click()
await page.waitForTimeout(1200)
check(await card.getByRole('button', { name: /save plugin settings/i }).count() === 0, 'switching it off hides the setup again')

// The Alerts delivery picker shares the ['plugins'] query key: it must still
// see every send-capable plugin, marked inactive rather than hidden.
await page.goto(`${BASE}/#/alerts`, { waitUntil: 'networkidle' })
await page.waitForTimeout(1500)
check((await page.getByText(/slack/i).count()) > 0, 'Alerts still knows about Slack while it is off')

await browser.close()
console.log(fails.length ? `\n${fails.length} FAILED` : '\nall checks passed')
process.exit(fails.length ? 1 : 0)
