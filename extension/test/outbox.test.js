/*
 * Node test suite for the connector's offline outbox
 * (extension/lib/outbox.js). No browser: an in-memory storage stub stands
 * in for browser.storage.local, and fake ping/save deps drive flush.
 *
 * The central guarantee under test: a queued capture is delivered ONLY to a
 * server whose live /ping identity matches the instance it was queued for —
 * never to a different database (dev / bare-metal / docker).
 *
 * Run: node extension/test/outbox.test.js
 */
"use strict";

const assert = require("assert");

// In-memory storage stub, installed BEFORE requiring the module (it reads
// globalThis.browser at load).
const store = {};
globalThis.browser = {
  storage: {
    local: {
      async get(keys) {
        const out = {};
        (Array.isArray(keys) ? keys : [keys]).forEach((k) => { if (k in store) out[k] = store[k]; });
        return out;
      },
      async set(obj) { Object.assign(store, obj); },
      async remove(keys) { (Array.isArray(keys) ? keys : [keys]).forEach((k) => delete store[k]); },
    },
  },
};
const OB = require("../lib/outbox.js");

let passed = 0, failed = 0;
async function test(name, fn) {
  try { await fn(); passed++; console.log("  ok  - " + name); }
  catch (e) { failed++; console.error("FAIL  - " + name + "\n        " + (e && e.message)); process.exitCode = 1; }
}
function reset() { for (const k of Object.keys(store)) delete store[k]; }

const DEV = { profile: "dev", db_fingerprint: "6ba21501b521" };
const PROD = { profile: "prod", db_fingerprint: "c227f1e4ba38" };
const body = (doi) => ({ action: "add", destination: "library", doi, title: "T " + doi, url: "https://x/" + doi });

(async () => {
  await test("clientId stable + distinct", () => {
    assert.strictEqual(OB.clientId(body("10.1/a")), OB.clientId(body("10.1/a")));
    assert.notStrictEqual(OB.clientId(body("10.1/a")), OB.clientId(body("10.1/b")));
  });

  await test("identityEquals", () => {
    assert.ok(OB.identityEquals(DEV, { profile: "dev", db_fingerprint: "6ba21501b521" }));
    assert.ok(!OB.identityEquals(DEV, PROD));
    assert.ok(!OB.identityEquals(DEV, null));
  });

  await test("decideDelivery: wait / URL-trust / adopt / deliver / conflict", () => {
    assert.strictEqual(OB.decideDelivery({ identity: DEV }, null, false), "wait");     // server down
    assert.strictEqual(OB.decideDelivery({ identity: DEV }, null, true), "deliver");   // reachable, no identity (old ALMa) → URL-trust
    assert.strictEqual(OB.decideDelivery({ identity: null }, PROD, true), "adopt");    // reachable, identity, none pinned
    assert.strictEqual(OB.decideDelivery({ identity: DEV }, DEV, true), "deliver");    // identity matches
    assert.strictEqual(OB.decideDelivery({ identity: DEV }, PROD, true), "conflict");  // identity differs
  });

  await test("enqueue binds last-known identity + dedups", async () => {
    reset();
    await OB.recordIdentity("http://localhost:8001", DEV);
    const r1 = await OB.enqueue("http://localhost:8001", body("10.1/a"));
    assert.ok(!r1.deduped);
    assert.deepStrictEqual(r1.item.identity, DEV);
    const r2 = await OB.enqueue("http://localhost:8001", body("10.1/a"));
    assert.ok(r2.deduped);
    assert.strictEqual(await OB.count(), 1);
  });

  await test("flush NEVER delivers to a different DB; delivers when the right one returns", async () => {
    reset();
    await OB.recordIdentity("http://localhost:8001", DEV); // dev last seen at :8001
    await OB.enqueue("http://localhost:8001", body("10.1/dev"));
    const saved = [];
    // A DIFFERENT instance (prod) now answers at :8001 → must hold, not send.
    let res = await OB.flush({
      ping: async () => ({ ok: true, info: { instance: PROD } }),
      save: async (url, key, b) => { saved.push(b); return { ok: true, status: 200 }; },
    });
    assert.strictEqual(saved.length, 0, "must not deliver to a different DB");
    assert.strictEqual(res.conflicts, 1);
    assert.strictEqual(await OB.count(), 1, "conflict item held");
    // The intended instance (dev) comes back → deliver + drain.
    res = await OB.flush({
      ping: async () => ({ ok: true, info: { instance: DEV } }),
      save: async (url, key, b) => { saved.push(b); return { ok: true, status: 200 }; },
    });
    assert.strictEqual(saved.length, 1, "delivered to the matching instance");
    assert.strictEqual(res.delivered, 1);
    assert.strictEqual(await OB.count(), 0, "drained after delivery");
  });

  await test("flush waits while down; adopts + pins identity when none was known", async () => {
    reset();
    await OB.enqueue("http://localhost:8000", body("10.1/new")); // no prior identity
    let res = await OB.flush({ ping: async () => ({ ok: false }), save: async () => ({ ok: true, status: 200 }) });
    assert.strictEqual(res.waiting, 1);
    assert.strictEqual(await OB.count(), 1);
    let saved = 0;
    res = await OB.flush({
      ping: async () => ({ ok: true, info: { instance: PROD } }),
      save: async () => { saved++; return { ok: true, status: 200 }; },
    });
    assert.strictEqual(saved, 1, "adopted the first instance that answered");
    assert.strictEqual(res.delivered, 1);
    assert.strictEqual(await OB.count(), 0);
    assert.deepStrictEqual(await OB.getIdentity("http://localhost:8000"), PROD, "identity pinned on adopt");
  });

  await test("flush delivers to a reachable server that reports NO identity (pre-0.16.0 ALMa)", async () => {
    reset();
    await OB.enqueue("http://localhost:8000", body("10.1/old")); // no prior identity for this URL
    let saved = 0;
    const res = await OB.flush({
      ping: async () => ({ ok: true, info: { ok: true } }), // reachable, but no `instance` field
      save: async () => { saved++; return { ok: true, status: 200 }; },
    });
    assert.strictEqual(saved, 1, "URL-trust delivery against an identity-less server");
    assert.strictEqual(res.delivered, 1);
    assert.strictEqual(await OB.count(), 0);
  });

  await test("flush keeps the item (with attempt count) on a save error", async () => {
    reset();
    await OB.recordIdentity("http://localhost:8001", DEV);
    await OB.enqueue("http://localhost:8001", body("10.1/x"));
    const res = await OB.flush({
      ping: async () => ({ ok: true, info: { instance: DEV } }),
      save: async () => ({ ok: false, status: 500 }),
    });
    assert.strictEqual(res.delivered, 0);
    assert.strictEqual(await OB.count(), 1);
    assert.strictEqual((await OB.list())[0].attempts, 1);
  });

  console.log("\n" + passed + " passed, " + failed + " failed");
})();
