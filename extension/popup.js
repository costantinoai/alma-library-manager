/* ALMa Connector — popup controller.
 *
 * Server selection:
 *   On open the popup probes every configured server PLUS the standard
 *   local ports (Docker :8000, dev :8001) and shows them in a dropdown on
 *   the connection pill, each with a live status dot. You pick the target
 *   instead of typing an address. A running instance on a standard port is
 *   auto-offered even if it was never configured.
 *
 * Identification (unchanged) runs in three layers so something always
 * works: URL baseline → injected DOM read → injected PDF byte-scan.
 *
 * The save is delegated to ALMa: POST /api/v1/extension/save with the DOI
 * (preferred) + scraped metadata fallback. All dynamic UI is built with
 * DOM nodes + textContent — no innerHTML.
 */
(function () {
  "use strict";

  const api = globalThis.browser || globalThis.chrome;
  const A = globalThis.almaExtract;
  const S = globalThis.almaSettings;
  const SUI = globalThis.almaServersUI;
  const NS = "http://www.w3.org/2000/svg";

  const RESTRICTED = /^(about:|moz-extension:|chrome:|chrome-extension:|view-source:|resource:|edge:|data:)/i;

  const el = (id) => document.getElementById(id);
  const $conn = el("conn"), $connLabel = el("conn-label");
  const $menu = el("srv-menu"), $connWrap = el("conn-wrap");
  const $empty = el("empty"), $emptyTitle = el("empty-title"), $emptySub = el("empty-sub");
  const $main = el("main");
  const $title = el("title"), $badges = el("badges"), $meta = el("meta");
  const $detectedText = el("detected-text");
  const $dest = el("dest"), $actions = el("actions"), $result = el("result");
  const $settings = el("settings"), $serverManager = el("server-manager"), $settingsBack = el("settings-back");

  let candidates = [];          // [{ url, apiKey, online, version, configured }]
  let active = null;            // the chosen candidate
  let detected = null;          // the merged paper object
  let destination = "library";
  let menuOpen = false;
  let mainView = "main";        // 'main' | 'empty' — what to restore when leaving Settings
  let managerCtl = null;        // mounted server-manager controller

  const connected = () => !!(active && active.online);

  // ---- tiny DOM/SVG helpers (no innerHTML) ----
  function clear(node) { while (node.firstChild) node.removeChild(node.firstChild); }
  function svg(opts) {
    const s = document.createElementNS(NS, "svg");
    s.setAttribute("viewBox", opts.viewBox || "0 0 24 24");
    s.setAttribute("fill", opts.fill || "none");
    s.setAttribute("stroke", opts.stroke || "currentColor");
    s.setAttribute("stroke-width", opts.sw || "2");
    s.setAttribute("stroke-linecap", "round");
    s.setAttribute("stroke-linejoin", "round");
    if (opts.cls) s.setAttribute("class", opts.cls);
    (opts.circles || []).forEach((c) => {
      const e = document.createElementNS(NS, "circle");
      e.setAttribute("cx", c[0]); e.setAttribute("cy", c[1]); e.setAttribute("r", c[2]);
      s.appendChild(e);
    });
    (opts.paths || []).forEach((d) => {
      const p = document.createElementNS(NS, "path");
      p.setAttribute("d", d);
      if (opts.pathFill) p.setAttribute("fill", opts.pathFill);
      s.appendChild(p);
    });
    return s;
  }

  function authHeaders(extra) {
    const h = Object.assign({ "Content-Type": "application/json" }, extra || {});
    if (active && active.apiKey) h["X-API-Key"] = active.apiKey;
    return h;
  }

  // -------------------------------------------------------------------
  // Server probing + selection
  // -------------------------------------------------------------------
  // autoFallback: when the saved active server is offline, fall back to a
  // running one for THIS session (without overwriting the saved default),
  // so a user running only the dev server isn't nagged about :8000. After
  // an explicit pick we pass false so the chosen server is respected.
  async function probeServers(autoFallback) {
    const { servers, activeUrl } = await S.load();
    const configured = servers.map((s) => ({ ...s, configured: true }));
    const extras = S.WELL_KNOWN
      .filter((u) => !servers.some((s) => s.url === u))
      .map((u) => ({ url: u, apiKey: "", configured: false }));
    candidates = configured.concat(extras);

    await Promise.all(candidates.map(async (c) => {
      const r = await S.pingUrl(c.url, c.apiKey);
      c.online = r.ok;
      c.version = r.info && r.info.alma_version;
    }));

    let chosen = candidates.find((c) => c.url === activeUrl) || null;
    if (autoFallback && (!chosen || !chosen.online)) {
      chosen = candidates.find((c) => c.configured && c.online)
        || candidates.find((c) => c.online)
        || chosen || candidates[0] || null;
    } else if (!chosen) {
      chosen = candidates[0] || null;
    }
    active = chosen;
    renderConn();
    refreshActionState();
  }

  function visibleCandidates() {
    // Configured servers always show; detected (well-known) ones only when online.
    return candidates.filter((c) => c.configured || c.online);
  }

  function renderConn() {
    if (!active) { setConnLabel("off", "no server"); return; }
    setConnLabel(active.online ? "on" : "off", S.shortHost(active.url));
    $conn.title = active.online
      ? "Connected to " + active.url + (active.version ? " (ALMa " + active.version + ")" : "")
      : "Not reachable at " + active.url + " — is ALMa running?";
    renderMenu();
  }

  function setConnLabel(state, label) {
    // preserve the dot + caret; only swap the state class + label text
    $conn.className = "conn is-" + state;
    $connLabel.textContent = label;
  }

  function renderMenu() {
    clear($menu);
    visibleCandidates().forEach((c) => {
      const item = document.createElement("button");
      item.className = "srv-item" + (active && c.url === active.url ? " is-active" : "");
      item.setAttribute("role", "option");
      item.setAttribute("data-url", c.url);
      item.setAttribute("aria-selected", String(!!(active && c.url === active.url)));

      const dot = document.createElement("span");
      dot.className = "dot is-" + (c.online ? "on" : "off");
      item.appendChild(dot);

      const host = document.createElement("span");
      host.className = "srv-host"; host.textContent = S.shortHost(c.url);
      item.appendChild(host);

      const url = document.createElement("span");
      url.className = "srv-url"; url.textContent = c.url.replace(/^https?:\/\//, "");
      item.appendChild(url);

      if (!c.configured) {
        const tag = document.createElement("span");
        tag.className = "srv-tag"; tag.textContent = "detected";
        item.appendChild(tag);
      }
      if (active && c.url === active.url) {
        item.appendChild(svg({ cls: "srv-check", sw: "2.4", paths: ["M20 6 9 17l-5-5"] }));
      }
      $menu.appendChild(item);
    });

    const manage = document.createElement("button");
    manage.className = "srv-manage";
    manage.textContent = "Manage servers…";
    manage.addEventListener("click", showSettings);
    $menu.appendChild(manage);
  }

  function toggleMenu(open) {
    menuOpen = open === undefined ? !menuOpen : open;
    $menu.hidden = !menuOpen;
    $conn.setAttribute("aria-expanded", String(menuOpen));
  }

  async function selectServer(url) {
    const chosen = candidates.find((c) => c.url === url);
    if (!chosen) return;
    active = chosen;
    if (!chosen.configured) chosen.configured = true; // it'll be saved into the list
    toggleMenu(false);
    renderConn();
    refreshActionState();
    try { await S.setActive(url); } catch (e) { /* best effort */ }
  }

  // -------------------------------------------------------------------
  // Identification
  // -------------------------------------------------------------------
  function urlBaseline(url) {
    const arxivId = A.arxivIdFromUrl(url);
    const doi = A.doiFromUrl(url) || (arxivId ? A.arxivToDoi(arxivId) : "");
    const via = [];
    if (A.doiFromUrl(url)) via.push("page URL");
    else if (arxivId) via.push("arXiv id");
    return {
      url, doi, arxivId, openalexId: "",
      title: "", authors: "", year: null, journal: "", abstract: "",
      isPdf: A.looksLikePdfUrl(url), detectedVia: via,
    };
  }

  async function injectDomExtract(tabId) {
    try {
      await api.scripting.executeScript({ target: { tabId }, files: ["lib/extract.js"] });
      const out = await api.scripting.executeScript({
        target: { tabId },
        func: () => globalThis.almaExtract.extractFromDocument(document, location.href),
      });
      return (out && out[0] && out[0].result) || null;
    } catch (e) {
      return null; // privileged page / PDF viewer — fall back to URL baseline
    }
  }

  async function injectPdfScan(tabId) {
    try {
      const out = await api.scripting.executeScript({
        target: { tabId },
        func: async () => {
          try {
            const r = await fetch(location.href);
            const buf = await r.arrayBuffer();
            return globalThis.almaExtract.extractDoiFromPdfBytes(buf);
          } catch (e) { return ""; }
        },
      });
      return (out && out[0] && out[0].result) || "";
    } catch (e) { return ""; }
  }

  function merge(base, dom) {
    if (!dom) return base;
    const pick = (a, b) => (a && String(a).trim() ? a : b);
    const via = [];
    (dom.detectedVia || []).forEach((v) => via.push(v));
    (base.detectedVia || []).forEach((v) => { if (!via.includes(v)) via.push(v); });
    return {
      url: base.url,
      doi: pick(dom.doi, base.doi),
      arxivId: pick(dom.arxivId, base.arxivId),
      openalexId: pick(dom.openalexId, base.openalexId),
      title: pick(dom.title, base.title),
      authors: pick(dom.authors, base.authors),
      year: dom.year || base.year,
      journal: pick(dom.journal, base.journal),
      abstract: pick(dom.abstract, base.abstract),
      isPdf: dom.isPdf || base.isPdf,
      detectedVia: via,
    };
  }

  async function identify(tab) {
    const base = urlBaseline(tab.url || "");
    const dom = await injectDomExtract(tab.id);
    let paper = merge(base, dom);
    if (!paper.doi && paper.isPdf) {
      const pdfDoi = A.cleanDoi(await injectPdfScan(tab.id));
      if (pdfDoi) {
        paper.doi = pdfDoi;
        if (!paper.detectedVia.includes("PDF metadata")) paper.detectedVia.push("PDF metadata");
      }
    }
    return paper;
  }

  // -------------------------------------------------------------------
  // Rendering paper
  // -------------------------------------------------------------------
  function showEmpty(title, sub) {
    mainView = "empty";
    $main.classList.add("hide");
    $empty.classList.add("show");
    if (title) $emptyTitle.textContent = title;
    if (sub) $emptySub.textContent = sub;
  }

  const STAR_PATH = "M12 2l3 6.5 7 .8-5.2 4.8L18.4 22 12 18.3 5.6 22l1.6-7.9L2 9.3l7-.8z";
  function renderStars() {
    document.querySelectorAll(".chip").forEach((chip) => {
      const n = parseInt(chip.getAttribute("data-stars"), 10) || 0;
      const box = chip.querySelector(".stars");
      clear(box);
      for (let i = 1; i <= 5; i++) {
        const filled = i <= n;
        box.appendChild(svg({
          paths: [STAR_PATH], sw: "1.6", cls: filled ? "star-on" : "star-off",
          fill: filled ? "currentColor" : "none", pathFill: filled ? "currentColor" : "none",
        }));
      }
    });
  }

  function badge(text, cls) {
    const span = document.createElement("span");
    span.className = "badge " + (cls || "");
    span.textContent = text;
    span.title = text;
    return span;
  }

  function renderPaper(p) {
    mainView = "main";
    $empty.classList.remove("show");
    $main.classList.remove("hide");

    if (p.title) {
      $title.textContent = p.title;
      $title.classList.remove("is-untitled");
    } else if (p.doi || p.arxivId) {
      $title.textContent = "Title resolves from the identifier on save";
      $title.classList.add("is-untitled");
    } else {
      $title.textContent = "Couldn't read a title";
      $title.classList.add("is-untitled");
    }

    clear($badges);
    if (p.arxivId) $badges.appendChild(badge("arXiv:" + p.arxivId, "arxiv"));
    if (p.doi) $badges.appendChild(badge(p.doi, "doi"));
    if (p.isPdf) $badges.appendChild(badge("PDF", "pdf"));

    const bits = [];
    if (p.authors) bits.push(shorten(p.authors, 80));
    if (p.year) bits.push(String(p.year));
    if (p.journal) bits.push(p.journal);
    clear($meta);
    bits.forEach((b, i) => {
      if (i > 0) {
        const sep = document.createElement("span");
        sep.className = "sep"; sep.textContent = "·";
        $meta.appendChild(sep);
      }
      $meta.appendChild(document.createTextNode(b));
    });
    $meta.style.display = bits.length ? "" : "none";

    const via = (p.detectedVia || []).filter(Boolean);
    $detectedText.textContent = via.length
      ? "Detected via " + via.join(", ")
      : "No identifier found on this page";
    refreshActionState();
  }

  function saveable() {
    return !!(detected && (detected.doi || detected.openalexId || (detected.title && detected.title.trim())));
  }

  function refreshActionState() {
    const ok = connected() && saveable();
    document.querySelectorAll(".chip").forEach((c) => (c.disabled = !ok));
    if (!connected()) {
      if (detected) result("error",
        active
          ? "Can't reach ALMa at " + S.shortHost(active.url) + ". Pick a running server in "
          : "No ALMa server reachable. Open ",
        { link: { text: "Servers", action: showSettings } });
    } else if (detected && !saveable()) {
      result("info", "No DOI, identifier, or title found — open the paper's article page and try again.");
    } else {
      hideResult();
    }
  }

  // -------------------------------------------------------------------
  // Save
  // -------------------------------------------------------------------
  async function save(action) {
    if (!saveable() || !connected()) return;
    setChipsBusy(true);
    result("loading", "Saving to " + destLabel(destination) + " on " + S.shortHost(active.url) + "…");

    const body = {
      action: action,
      destination: destination,
      doi: detected.doi || null,
      openalex_id: detected.openalexId || null,
      title: detected.title || null,
      url: detected.url || null,
      authors: detected.authors || null,
      year: detected.year || null,
      journal: detected.journal || null,
      abstract: detected.abstract || null,
    };

    try {
      const res = await fetch(active.url + "/api/v1/extension/save", {
        method: "POST", headers: authHeaders(), body: JSON.stringify(body),
      });
      let data = {};
      try { data = await res.json(); } catch (e) { /* ignore */ }

      if (res.ok) {
        const rating = data.rating ? data.rating + "★" : "";
        result("success",
          "Saved to " + destLabel(data.destination || destination) + (rating ? " · " + rating : "") + " on " + S.shortHost(active.url),
          { title: data.title || detected.title || "", link: { text: "Open ALMa", href: active.url + "/" } });
      } else if (res.status === 422) {
        result("error", (data && data.detail) || "Couldn't identify this paper. Try opening its article page (not a search result).");
        setChipsBusy(false);
      } else if (res.status === 400) {
        result("error", (data && data.detail) || "Invalid request.");
        setChipsBusy(false);
      } else {
        result("error", "ALMa returned an error (" + res.status + "). Try again.");
        setChipsBusy(false);
      }
    } catch (e) {
      active.online = false;
      renderConn();
      result("error", "Lost the connection to ALMa at " + S.shortHost(active.url) + ". Is it still running?");
      setChipsBusy(false);
    }
  }

  function setChipsBusy(busy) {
    document.querySelectorAll(".chip").forEach((c) => (c.disabled = busy));
  }

  // -------------------------------------------------------------------
  // Result strip (DOM-built)
  // -------------------------------------------------------------------
  function resultIcon(kind) {
    if (kind === "loading") return svg({ cls: "spin", paths: ["M21 12a9 9 0 1 1-6.2-8.5"] });
    if (kind === "success") return svg({ sw: "2.2", paths: ["M20 6 9 17l-5-5"] });
    if (kind === "info") return svg({ circles: [[12, 12, 9]], paths: ["M12 16v-4", "M12 8h.01"] });
    return svg({ circles: [[12, 12, 9]], paths: ["M12 8v5", "M12 16h.01"] }); // error
  }

  function result(kind, text, opts) {
    opts = opts || {};
    $result.className = "result show is-" + kind;
    clear($result);
    $result.appendChild(resultIcon(kind));

    const box = document.createElement("div");
    if (opts.title) {
      const t = document.createElement("div");
      t.className = "r-title"; t.textContent = opts.title;
      box.appendChild(t);
    }
    const sub = document.createElement("div");
    sub.className = "r-sub";
    sub.appendChild(document.createTextNode(text));
    if (opts.link) {
      const a = document.createElement("a");
      a.textContent = opts.link.text;
      if (opts.link.href) { a.href = opts.link.href; a.target = "_blank"; a.rel = "noopener"; }
      if (opts.link.action) a.addEventListener("click", (ev) => { ev.preventDefault(); opts.link.action(); });
      sub.appendChild(document.createTextNode(" "));
      sub.appendChild(a);
    }
    box.appendChild(sub);
    $result.appendChild(box);
  }

  function hideResult() { $result.className = "result"; clear($result); }

  // -------------------------------------------------------------------
  // Helpers
  // -------------------------------------------------------------------
  function destLabel(d) { return d === "reading_list" ? "Reading list" : "Library"; }
  function shorten(s, n) { s = String(s || ""); return s.length > n ? s.slice(0, n - 1) + "…" : s; }
  function showSettings() {
    toggleMenu(false);
    $empty.classList.remove("show");
    $main.classList.add("hide");
    $settings.classList.remove("hide");
    if (!managerCtl) managerCtl = SUI.mount($serverManager, { onActiveChange: () => probeServers(false) });
    else managerCtl.refresh();
  }
  function hideSettings() {
    $settings.classList.add("hide");
    if (mainView === "empty") $empty.classList.add("show");
    else $main.classList.remove("hide");
    probeServers(false);
  }

  // -------------------------------------------------------------------
  // Wire-up
  // -------------------------------------------------------------------
  function wire() {
    el("gear").addEventListener("click", showSettings);
    $settingsBack.addEventListener("click", hideSettings);
    $conn.addEventListener("click", (e) => { e.stopPropagation(); toggleMenu(); });
    $menu.addEventListener("click", (e) => {
      const item = e.target.closest(".srv-item");
      if (item) { e.stopPropagation(); selectServer(item.getAttribute("data-url")); }
    });
    document.addEventListener("click", (e) => {
      if (menuOpen && $connWrap && !$connWrap.contains(e.target)) toggleMenu(false);
    });
    $dest.addEventListener("click", (e) => {
      const btn = e.target.closest("button[data-dest]");
      if (!btn) return;
      destination = btn.getAttribute("data-dest");
      $dest.querySelectorAll("button").forEach((b) =>
        b.setAttribute("aria-pressed", String(b === btn)));
    });
    $actions.addEventListener("click", (e) => {
      const chip = e.target.closest(".chip");
      if (!chip || chip.disabled) return;
      save(chip.getAttribute("data-action"));
    });
  }

  async function init() {
    renderStars();
    wire();
    probeServers(true); // async; auto-falls back to a running server

    let tab;
    try {
      const tabs = await api.tabs.query({ active: true, currentWindow: true });
      tab = tabs && tabs[0];
    } catch (e) { /* no tab */ }

    if (!tab || !tab.url || RESTRICTED.test(tab.url)) {
      showEmpty("Nothing to save here",
        "Open a paper's page — a publisher article, an arXiv abstract, or a PDF — then click the ALMa button.");
      return;
    }

    detected = await identify(tab);
    if (!saveable() && !detected.isPdf && !detected.title && !detected.doi) {
      showEmpty("No paper detected here",
        "This page has no citation metadata or DOI. Try the article's main page instead of a listing or search result.");
      return;
    }
    renderPaper(detected);
  }

  // Exposed for the offline render harness (test/popup-render.html).
  globalThis.__alma = { renderPaper, renderStars, wire, renderConn, result, toggleMenu };

  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", init);
  } else {
    init();
  }
})();
