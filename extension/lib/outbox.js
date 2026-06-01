/* ALMa Connector — offline outbox.
 *
 * When the target ALMa server is down, a Save is queued here
 * (storage.local — survives browser restarts) BOUND TO THE INTENDED
 * INSTANCE: the active server URL plus its last-known identity
 * ({profile, db_fingerprint} from /ping). On flush we re-probe that URL and
 * deliver the capture ONLY to a server whose live identity matches what we
 * saw when it was queued — so a capture can never land in the wrong database
 * (dev / bare-metal / docker). Delivery always goes through the normal
 * POST /extension/save (the one canonical ingestion path); nothing here ever
 * touches a DB directly.
 *
 * Persisted (storage.local):
 *   almaOutbox:         [ item ]          queued captures
 *   almaServerIdentity: { [url]: id }     last-seen identity per server URL
 *
 * item = { id, url, identity|null, body, createdAt, attempts, lastError, status }
 *   status: "queued"   deliver when the matching instance is reachable
 *           "conflict" the URL now serves a DIFFERENT db — held, never sent
 */
(function (root) {
  "use strict";

  const api = globalThis.browser || globalThis.chrome;
  const OUTBOX_KEY = "almaOutbox";
  const IDENTITY_KEY = "almaServerIdentity";
  const MAX_ITEMS = 200;

  const norm = (s) => String(s == null ? "" : s);

  // Stable per-capture id so re-queuing the same paper never duplicates and
  // replays stay idempotent (paired with the server's DOI / year+title dedup).
  function clientId(body) {
    const key = (
      norm(body && body.doi) ||
      norm(body && body.url) ||
      norm(body && body.title) + "|" + norm(body && body.year)
    ).toLowerCase();
    let h = 5381;
    for (let i = 0; i < key.length; i++) h = ((h << 5) + h + key.charCodeAt(i)) >>> 0;
    return "ob_" + h.toString(36);
  }

  function identityEquals(a, b) {
    if (!a || !b) return false;
    return norm(a.profile) === norm(b.profile) &&
           norm(a.db_fingerprint) === norm(b.db_fingerprint);
  }

  // PURE decision (unit-tested): given a queued item, the LIVE identity from a
  // fresh /ping of item.url, and whether that ping succeeded, what do we do?
  //   "wait"     server not reachable → leave queued, try later
  //   "deliver"  reachable + identity matches the snapshot; OR reachable but
  //              the server reports NO identity (a pre-0.16.0 ALMa) → fall
  //              back to trusting the URL (the user's chosen target), exactly
  //              as the connector did before identities existed
  //   "adopt"    reachable + a live identity, but we had no snapshot (URL
  //              never reached before) → pin the first instance, then send
  //   "conflict" reachable + a live identity that DIFFERS from the snapshot →
  //              the URL now serves a different db → hold, never send
  function decideDelivery(item, liveIdentity, reachable) {
    if (!reachable) return "wait";
    if (!liveIdentity) return "deliver";       // older ALMa, no identity to verify → URL-trust
    if (!item || !item.identity) return "adopt";
    return identityEquals(item.identity, liveIdentity) ? "deliver" : "conflict";
  }

  // ---- storage helpers (no-op-safe outside a browser, e.g. node tests) ----
  async function _get(key, dflt) {
    try {
      const s = await api.storage.local.get([key]);
      return s[key] == null ? dflt : s[key];
    } catch (e) { return dflt; }
  }
  async function _set(key, val) {
    try { await api.storage.local.set({ [key]: val }); } catch (e) { /* ignore */ }
  }

  async function list() { const v = await _get(OUTBOX_KEY, []); return Array.isArray(v) ? v : []; }
  async function count() { return (await list()).length; }
  async function _write(items) { await _set(OUTBOX_KEY, items.slice(0, MAX_ITEMS)); }

  async function getIdentity(url) { const m = await _get(IDENTITY_KEY, {}); return (m && m[url]) || null; }
  async function recordIdentity(url, identity) {
    if (!url || !identity) return;
    const m = await _get(IDENTITY_KEY, {});
    m[url] = { profile: norm(identity.profile), db_fingerprint: norm(identity.db_fingerprint) };
    await _set(IDENTITY_KEY, m);
  }

  // Queue (or refresh) a capture for `url`, bound to that URL's last-known
  // identity. Returns { item, deduped, full }.
  async function enqueue(url, body) {
    const items = await list();
    const id = clientId(body);
    const existing = items.find((x) => x.id === id && x.url === url);
    if (existing) {
      existing.body = body; existing.status = "queued"; existing.lastError = "";
      await _write(items);
      return { item: existing, deduped: true, full: false };
    }
    if (items.length >= MAX_ITEMS) return { item: null, deduped: false, full: true };
    const item = {
      id, url, identity: await getIdentity(url), body,
      createdAt: Date.now(), attempts: 0, lastError: "", status: "queued",
    };
    items.push(item);
    await _write(items);
    return { item, deduped: false, full: false };
  }

  async function remove(id, url) {
    const items = (await list()).filter((x) => !(x.id === id && (!url || x.url === url)));
    await _write(items);
  }
  async function clear() { await _write([]); }

  // Flush queued captures. `deps`:
  //   ping(url, apiKey)        -> { ok, info }    (almaSettings.pingUrl)
  //   save(url, apiKey, body)  -> { ok, status }  (POST /extension/save)
  //   apiKeyFor(url)           -> string          (optional)
  // Returns { delivered, conflicts, waiting, remaining }. Conflicts are held,
  // never delivered — a capture cannot land in a database other than the one
  // it was queued for.
  async function flush(deps) {
    const items = await list();
    if (!items.length) return { delivered: 0, conflicts: 0, waiting: 0, remaining: 0 };

    const keyFor = async (url) => (deps.apiKeyFor ? await deps.apiKeyFor(url) : "");
    const live = {}, reachable = {};
    for (const url of Array.from(new Set(items.map((x) => x.url)))) {
      const r = await deps.ping(url, await keyFor(url));
      reachable[url] = !!(r && r.ok);
      live[url] = r && r.ok && r.info ? (r.info.instance || null) : null;
      if (live[url]) await recordIdentity(url, live[url]);
    }

    let delivered = 0, conflicts = 0, waiting = 0;
    const keep = [];
    for (const it of items) {
      const decision = decideDelivery(it, live[it.url], reachable[it.url]);
      if (decision === "wait") { it.status = "queued"; waiting++; keep.push(it); continue; }
      if (decision === "conflict") { it.status = "conflict"; conflicts++; keep.push(it); continue; }
      let res = null;
      try { res = await deps.save(it.url, await keyFor(it.url), it.body); } catch (e) { res = null; }
      if (res && res.ok) {
        if (decision === "adopt" && live[it.url]) await recordIdentity(it.url, live[it.url]);
        delivered++; // dropped from the queue
      } else {
        it.attempts = (it.attempts || 0) + 1;
        it.lastError = res ? ("HTTP " + res.status) : "network";
        keep.push(it);
      }
    }
    await _write(keep);
    return { delivered, conflicts, waiting, remaining: keep.length };
  }

  root.almaOutbox = {
    OUTBOX_KEY, IDENTITY_KEY, MAX_ITEMS,
    clientId, identityEquals, decideDelivery,
    list, count, getIdentity, recordIdentity, enqueue, remove, clear, flush,
  };
  if (typeof module !== "undefined" && module.exports) module.exports = root.almaOutbox;
})(typeof globalThis !== "undefined" ? globalThis : this);
