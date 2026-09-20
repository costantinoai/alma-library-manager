/**
 * The two promises this branch makes that only a real browser can check:
 *
 *   1. a PDF ALMa already keeps is reachable from any device with EVERY fetch
 *      plugin off — the reader opens the served file, and a paper with no file
 *      says so and offers Attach instead of spinning;
 *   2. an import ALMa cannot identify is DURABLE — the row and the DOI you
 *      typed survive switching import tabs, closing the dialog and reloading
 *      the page, because the upload itself lives on the server.
 *
 * Needs a dev stack with NO PDF source plugin on and:
 *   PDF_PAPER     a paper id that HAS a kept PDF
 *   NO_PDF_PAPER  a paper id with no PDF and no attempts
 *   SCAN_PDF      path to a PDF with no readable text (an unidentifiable one)
 * Run:  ALMA_URL=http://127.0.0.1:5199 PDF_PAPER=… NO_PDF_PAPER=… SCAN_PDF=… \
 *       node e2e/pdf-local-and-imports.spec.mjs
 * Screenshots go to $SHOTS_DIR (default /tmp) — LOOK at them.
 */
import { chromium, devices } from 'playwright'

const BASE = process.env.ALMA_URL ?? 'http://127.0.0.1:5173'
const PDF_PAPER = process.env.PDF_PAPER
const NO_PDF_PAPER = process.env.NO_PDF_PAPER
const SCAN_PDF = process.env.SCAN_PDF
const SHOTS = process.env.SHOTS_DIR ?? '/tmp'
if (!PDF_PAPER || !NO_PDF_PAPER || !SCAN_PDF) {
  console.error('Set PDF_PAPER, NO_PDF_PAPER and SCAN_PDF.')
  process.exit(2)
}

const failures = []
const check = (ok, message) => {
  console.log(`${ok ? '  ok  ' : '  FAIL'} ${message}`)
  if (!ok) failures.push(message)
}

const browser = await chromium.launch()
const errors = []

// The first-visit tour lays a full-page scrim that swallows every click.
const skipTours = (target) =>
  target.addInitScript(() => {
    for (const page of ['home', 'feed', 'authors', 'library', 'discovery']) {
      localStorage.setItem(`alma.tour.${page}.completed`, 'done')
    }
  })

// ── 1. Phone, every fetch plugin off: the kept file still opens ──
const phone = await browser.newContext({ ...devices['iPhone 13'] })
await skipTours(phone)
const reader = await phone.newPage()
reader.on('pageerror', (e) => errors.push(String(e).slice(0, 200)))
const served = reader.waitForResponse((r) => r.url().includes(`/papers/${PDF_PAPER}/pdf`), { timeout: 20000 })
await reader.goto(`${BASE}/#/read?paper=${PDF_PAPER}`)
const response = await served
check(response.status() === 200, `the kept PDF is served with no source plugin on (HTTP ${response.status()})`)
check((response.headers()['content-type'] ?? '').startsWith('application/pdf'), 'served as application/pdf')
check((response.headers()['content-disposition'] ?? '').startsWith('inline'), 'served inline, for the phone viewer')

// ── 2. Phone: a paper with no file says so, and offers to attach one ──
const empty = await phone.newPage()
empty.on('pageerror', (e) => errors.push(String(e).slice(0, 200)))
await empty.goto(`${BASE}/#/read?paper=${NO_PDF_PAPER}`, { waitUntil: 'networkidle' })
await empty.waitForTimeout(1500)
check(
  (await empty.getByText(/no pdf source is switched on/i).count()) === 1,
  'with every source off the reader says so instead of searching',
)
check((await empty.getByRole('button', { name: /attach a pdf/i }).count()) === 1, 'it offers Attach a PDF')
check((await empty.getByRole('status').count()) === 0, 'it is not left spinning')
await empty.screenshot({ path: `${SHOTS}/pdf-reader-no-source-390.png`, fullPage: true })
await phone.close()

// ── 3. Desktop: an unidentified import survives tabs, the dialog and a reload ──
const deskContext = await browser.newContext({ viewport: { width: 1440, height: 1000 } })
await skipTours(deskContext)
const desk = await deskContext.newPage()
desk.on('pageerror', (e) => errors.push(String(e).slice(0, 200)))

async function openPdfImportTab(page, { reload = false } = {}) {
  // A hash change is NOT a reload: to prove the upload outlives the PAGE, the
  // whole document has to come back, module state and all.
  if (reload) await page.reload({ waitUntil: 'networkidle' })
  else await page.goto(`${BASE}/#/library`, { waitUntil: 'networkidle' })
  await page.waitForTimeout(800)
  await page.getByRole('button', { name: /import papers/i }).first().click()
  await page.getByRole('tab', { name: /pdf files/i }).click()
  await page.waitForTimeout(400)
}

await openPdfImportTab(desk)
await desk.setInputFiles('input[type="file"]', SCAN_PDF)
const row = desk.getByText(/could not tell which paper this is/i).first()
await row.waitFor({ timeout: 60000 })
check(true, 'an unidentifiable PDF comes back as a row asking for a DOI or title')

const hint = desk.getByRole('textbox', { name: /doi or title/i }).first()
await hint.fill('10.5555/typed.while.you.waited')

// Switch to another import tab and back: the row and the hint must survive.
await desk.getByRole("tab", { name: /bibtex/i }).first().click()
await desk.waitForTimeout(400)
await desk.getByRole('tab', { name: /pdf files/i }).click()
await desk.waitForTimeout(600)
check((await desk.getByText(/could not tell which paper this is/i).count()) === 1, 'the row survives a tab switch')
check(
  (await desk.getByRole('textbox', { name: /doi or title/i }).first().inputValue()) === '10.5555/typed.while.you.waited',
  'what you typed survives a tab switch',
)
await desk.screenshot({ path: `${SHOTS}/pdf-import-after-tab-switch.png` })

// Close the dialog and reload the whole page: the upload lives on the server,
// so the row comes back from ITS pending list, not from anything in memory.
await desk.keyboard.press('Escape')
await desk.waitForTimeout(300)
await openPdfImportTab(desk, { reload: true })
await desk.waitForTimeout(1000)
check((await desk.getByText(/could not tell which paper this is/i).count()) === 1, 'the upload is still waiting after a real page reload')
check((await desk.getByText(/kept (just now|.* ago)/i).count()) === 1, 'it says how long the file has been kept')
check(
  (await desk.getByRole('textbox', { name: /doi or title/i }).first().inputValue()) === '',
  'the typed hint does not come back from the server (only the file is durable)',
)
const giveUp = desk.getByRole('button', { name: /give up/i }).first()
check((await giveUp.count()) === 1, 'a waiting upload can be given up')
await desk.screenshot({ path: `${SHOTS}/pdf-import-after-reload.png` })
await giveUp.click()
await desk.waitForTimeout(800)
check((await desk.getByRole('button', { name: /give up/i }).count()) === 0, 'giving up removes it')

check(errors.length === 0, `no page errors (${errors.join(' | ') || 'none'})`)
await browser.close()
console.log(failures.length ? `\n${failures.length} FAILED` : '\nall checks passed')
process.exit(failures.length ? 1 : 0)
