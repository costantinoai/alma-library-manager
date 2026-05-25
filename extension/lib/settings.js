/* ALMa Connector — shared settings + server probing.
 *
 * Loaded by both the popup and the options page (and the Node-free
 * render harness). Owns the persisted shape and the /ping probe so the
 * two surfaces never drift.
 *
 * Persisted shape (storage.local):
 *   servers:   [{ url, apiKey }]   // user-configured instances
 *   activeUrl: string             // the one the popup saves into now
 *
 * WELL_KNOWN are the standard local ports (Docker :8000, dev :8001) that
 * the manifest already grants host permission for. The popup probes them
 * even when not configured, so a second instance that comes online is
 * auto-offered in the dropdown — "select, don't type".
 */
(function (root) {
  "use strict";

  const api = globalThis.browser || globalThis.chrome;
  const WELL_KNOWN = ["http://localhost:8000", "http://localhost:8001"];
  const DEFAULT_SERVERS = [{ url: "http://localhost:8000", apiKey: "" }];

  function normUrl(u) {
    return String(u || "").trim().replace(/\/+$/, "");
  }

  function shortHost(url) {
    try {
      const u = new URL(url);
      const local = u.hostname === "localhost" || u.hostname === "127.0.0.1";
      return local ? (u.port ? ":" + u.port : u.protocol === "https:" ? ":443" : ":80") : u.host;
    } catch (e) {
      return url || "";
    }
  }

  function cleanServers(list) {
    const seen = new Set();
    const out = [];
    (list || []).forEach((x) => {
      const url = normUrl(x && x.url);
      if (!url || seen.has(url)) return;
      seen.add(url);
      out.push({ url, apiKey: String((x && x.apiKey) || "") });
    });
    return out;
  }

  async function load() {
    let s = {};
    try {
      s = await api.storage.local.get(["servers", "activeUrl", "baseUrl", "apiKey"]);
    } catch (e) { /* defaults */ }

    let servers = Array.isArray(s.servers) && s.servers.length ? cleanServers(s.servers) : null;
    if (!servers || !servers.length) {
      // Migrate the old single-server shape, else seed the default.
      if (s.baseUrl) servers = [{ url: normUrl(s.baseUrl), apiKey: String(s.apiKey || "") }];
      else servers = DEFAULT_SERVERS.map((x) => ({ ...x }));
    }

    let activeUrl = normUrl(s.activeUrl);
    if (!activeUrl || !servers.some((x) => x.url === activeUrl)) {
      activeUrl = servers[0] ? servers[0].url : WELL_KNOWN[0];
    }
    return { servers, activeUrl };
  }

  async function save(state) {
    const servers = cleanServers(state.servers);
    let activeUrl = normUrl(state.activeUrl);
    if (!servers.some((x) => x.url === activeUrl)) activeUrl = servers[0] ? servers[0].url : "";
    await api.storage.local.set({ servers, activeUrl });
    try { await api.storage.local.remove(["baseUrl", "apiKey"]); } catch (e) { /* ignore */ }
    return { servers, activeUrl };
  }

  // Switch the active target. If the chosen url was only *detected* (not
  // yet configured), promote it into the saved list so the choice sticks.
  async function setActive(url) {
    url = normUrl(url);
    const st = await load();
    if (url && !st.servers.some((x) => x.url === url)) st.servers.push({ url, apiKey: "" });
    st.activeUrl = url;
    return save(st);
  }

  function originPattern(url) {
    try {
      const u = new URL(url);
      return u.protocol + "//" + u.host + "/*";
    } catch (e) {
      return null;
    }
  }

  // Make sure we hold host permission for this origin (host AND port).
  // localhost:8000/:8001 are in the manifest and return true without a
  // prompt; any other address is requested on demand (needs a user
  // gesture, so call this from a click handler). Returns true if granted.
  async function ensureOrigin(url) {
    const pattern = originPattern(url);
    if (!pattern || !api.permissions) return true;
    try {
      if (await api.permissions.contains({ origins: [pattern] })) return true;
      return await api.permissions.request({ origins: [pattern] });
    } catch (e) {
      return true; // older browsers — let the fetch try anyway
    }
  }

  async function pingUrl(url, apiKey, timeoutMs) {
    url = normUrl(url);
    if (!url) return { ok: false };
    const ctrl = new AbortController();
    const timer = setTimeout(() => ctrl.abort(), timeoutMs || 1500);
    try {
      const headers = {};
      if (apiKey) headers["X-API-Key"] = apiKey;
      const res = await fetch(url + "/api/v1/extension/ping", {
        method: "GET", headers, signal: ctrl.signal,
      });
      clearTimeout(timer);
      if (!res.ok) return { ok: false };
      let info = {};
      try { info = await res.json(); } catch (e) { /* ignore */ }
      return { ok: true, info };
    } catch (e) {
      clearTimeout(timer);
      return { ok: false };
    }
  }

  root.almaSettings = {
    WELL_KNOWN, normUrl, shortHost, cleanServers,
    load, save, setActive, pingUrl, originPattern, ensureOrigin,
  };
  if (typeof module !== "undefined" && module.exports) module.exports = root.almaSettings;
})(typeof globalThis !== "undefined" ? globalThis : this);
