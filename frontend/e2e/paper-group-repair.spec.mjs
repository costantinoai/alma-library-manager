/** Real isolated app: canonical ingest, Settings repair, Activity and responsive saves. */
import assert from 'node:assert/strict'
import { chromium } from 'playwright'
const base = process.env.ALMA_URL ?? 'http://127.0.0.1:5194'
const api = process.env.ALMA_API ?? 'http://127.0.0.1:8022/api/v1'
const shotDir = process.env.ALMA_SHOT_DIR ?? '/tmp'
async function request(path, body) {
  const response = await fetch(`${api}${path}`, body === undefined ? {} : {
    method: 'POST', headers: {'Content-Type': 'application/json'}, body: JSON.stringify(body),
  })
  assert.ok(response.ok, `${path}: ${response.status} ${await response.clone().text()}`)
  return response.json()
}
const browser = await chromium.launch()
try {
  const page = await browser.newPage({viewport: {width: 1440, height: 1100}})
  await page.addInitScript(() => {
    for (const name of ['home','settings','library','discovery','health','feed']) localStorage.setItem(`alma.tour.${name}.completed`, 'done')
  })
  const errors = []
  page.on('pageerror', e => errors.push(String(e)))
  const nonce = Date.now()
  const title = `Paper group browser proof ${nonce}`
  const pre = await request('/library/saved', {title, doi:`10.1101/${nonce}`, year:2026, rating:5, notes:'Preserve this note'})
  await request(`/papers/${pre.id}/action`, {action:'read', surface:'papers'})
  const published = await request('/library/saved', {title, doi:`10.1000/${nonce}`, year:2026, rating:5})
  const list = await request(`/papers?search=${encodeURIComponent(title)}&scope=library`)
  assert.equal(list.length, 1, 'one logical paper in Library')
  assert.notEqual(published.id, pre.id, 'distinct DOI must create published row')
  assert.equal(list[0].id, published.id)
  assert.equal(list[0].reading_status, 'reading')
  assert.match(list[0].notes, /Preserve this note/)
  await page.goto(`${base}/#/settings?anchor=library-management`, {waitUntil:'domcontentloaded'})
  await page.getByRole('button', {name:'Reconcile Paper Groups', exact:true}).waitFor({timeout:30000})
  await page.getByRole('button', {name:'Reconcile Paper Groups', exact:true}).scrollIntoViewIfNeeded()
  const [launchResponse] = await Promise.all([
    page.waitForResponse(r => r.url().includes('/health/operations/paper_group_reconcile/run') && r.request().method()==='POST'),
    page.getByRole('button', {name:'Reconcile Paper Groups', exact:true}).click(),
  ])
  assert.equal(launchResponse.status(), 200)
  const launch = await launchResponse.json()
  assert.ok(launch.job_id, JSON.stringify(launch))
  const durations = []
  for (let i=0;i<8;i++) {
    const start = performance.now()
    await request(`/papers/${published.id}/action`, {action:'like', surface:'papers'})
    durations.push(performance.now()-start)
  }
  let job
  for (let i=0;i<90;i++) {
    job = (await request('/activity')).find(j => j.job_id === launch.job_id)
    if (['completed','failed','cancelled'].includes(job?.status)) break
    await page.waitForTimeout(1000)
  }
  assert.equal(job?.status, 'completed', JSON.stringify(job))
  assert.match(job.message, /ambiguous/)
  const logs = await request(`/activity/${launch.job_id}/logs?limit=200`)
  assert.ok(logs.some(l => l.step === 'reconcile_summary'), 'summary visible in Activity')
  await page.screenshot({path:`${shotDir}/paper-groups-settings.png`,fullPage:true})
  await page.goto(`${base}/#/library`, {waitUntil:'domcontentloaded'})
  await page.waitForTimeout(2000)
  await page.screenshot({path:`${shotDir}/paper-groups-library.png`,fullPage:true})
  assert.deepEqual(errors, [])
  console.log(JSON.stringify({job_id:launch.job_id, message:job.message, foreground_ms:durations.map(Math.round), log_count:logs.length, published:published.id}))
} finally { await browser.close() }
