/* ALMa Connector — server manager UI (shared).
 *
 * `almaServersUI.mount(container, { onActiveChange })` renders an
 * interactive list of ALMa servers into `container`:
 *   - configured servers + auto-detected running instances on the
 *     standard ports (DETECTED tag),
 *   - a live status dot per row (probed via /ping),
 *   - click a row to make it the active target,
 *   - remove (×) saved servers, Add detected ones,
 *   - an "Add a server" form (url + optional API key) with a host
 *     permission request for non-default origins.
 *
 * Used by the popup's in-panel Settings view and by the standalone
 * options page, so the two never drift. All DOM is built with nodes +
 * textContent — no innerHTML.
 */
(function (root) {
  "use strict";

  const S = root.almaSettings;
  const NS = "http://www.w3.org/2000/svg";

  function clear(n) { while (n.firstChild) n.removeChild(n.firstChild); }
  function elem(tag, cls, text) {
    const e = document.createElement(tag);
    if (cls) e.className = cls;
    if (text != null) e.textContent = text;
    return e;
  }
  function svgEl(paths, cls, sw) {
    const s = document.createElementNS(NS, "svg");
    s.setAttribute("viewBox", "0 0 24 24");
    s.setAttribute("fill", "none");
    s.setAttribute("stroke", "currentColor");
    s.setAttribute("stroke-width", sw || "2");
    s.setAttribute("stroke-linecap", "round");
    s.setAttribute("stroke-linejoin", "round");
    if (cls) s.setAttribute("class", cls);
    (paths || []).forEach((d) => {
      const p = document.createElementNS(NS, "path");
      p.setAttribute("d", d);
      s.appendChild(p);
    });
    return s;
  }

  function mount(container, opts) {
    opts = opts || {};
    const onActiveChange = opts.onActiveChange || function () {};

    let candidates = [];   // [{url, apiKey, configured, online, version, probing}]
    let activeUrl = "";

    async function refresh() {
      const { servers, activeUrl: a } = await S.load();
      activeUrl = a;
      const configured = servers.map((s) => ({ ...s, configured: true, probing: true }));
      const extras = S.WELL_KNOWN
        .filter((u) => !servers.some((s) => s.url === u))
        .map((u) => ({ url: u, apiKey: "", configured: false, probing: true }));
      candidates = configured.concat(extras);
      render(); // show rows with probing dots immediately

      await Promise.all(candidates.map(async (c) => {
        const r = await S.pingUrl(c.url, c.apiKey);
        c.online = r.ok;
        c.version = r.info && r.info.alma_version;
        c.probing = false;
      }));
      render();
    }

    function visible() {
      // configured always; detected (well-known) only when online or still probing
      return candidates.filter((c) => c.configured || c.online || c.probing);
    }

    async function setActive(url) {
      await S.setActive(url);
      onActiveChange(url);
      await refresh();
    }
    async function addDetected(url) { await setActive(url); }
    async function removeServer(url) {
      const st = await S.load();
      st.servers = st.servers.filter((s) => s.url !== url);
      if (st.activeUrl === url) st.activeUrl = st.servers[0] ? st.servers[0].url : "";
      await S.save(st);
      onActiveChange(st.activeUrl);
      await refresh();
    }

    function render() {
      clear(container);

      // toolbar: count + recheck
      const bar = elem("div", "sm-bar");
      bar.appendChild(elem("span", "sm-count",
        visible().length + (visible().length === 1 ? " server" : " servers")));
      const recheck = elem("button", "sm-recheck");
      recheck.appendChild(svgEl(["M23 4v6h-6", "M1 20v-6h6",
        "M3.5 9a9 9 0 0 1 14.9-3.4L23 10", "M1 14l4.6 4.4A9 9 0 0 0 20.5 15"], "", "1.8"));
      recheck.appendChild(document.createTextNode("Recheck"));
      recheck.addEventListener("click", refresh);
      bar.appendChild(recheck);
      container.appendChild(bar);

      // list
      const list = elem("div", "sm-list");
      visible().forEach((c) => list.appendChild(row(c)));
      container.appendChild(list);

      container.appendChild(addForm());
    }

    function row(c) {
      const isActive = c.url === activeUrl;
      const r = elem("div", "sm-row" + (isActive ? " is-active" : ""));
      if (!isActive) {
        r.setAttribute("role", "button");
        r.setAttribute("tabindex", "0");
        r.addEventListener("click", () => setActive(c.url));
        r.addEventListener("keydown", (e) => {
          if (e.key === "Enter" || e.key === " ") { e.preventDefault(); setActive(c.url); }
        });
      }

      const dot = elem("span", "dot " + (c.probing ? "is-checking" : c.online ? "is-on" : "is-off"));
      r.appendChild(dot);

      const host = elem("span", "sm-host", S.shortHost(c.url));
      r.appendChild(host);

      const url = elem("span", "sm-url", c.url.replace(/^https?:\/\//, ""));
      url.title = c.url + (c.version ? "  (ALMa " + c.version + ")" : "");
      r.appendChild(url);

      // right side: status/role + action
      if (isActive) {
        r.appendChild(elem("span", "sm-tag sm-active", "Active"));
      } else if (!c.configured) {
        r.appendChild(elem("span", "sm-tag sm-detected", "detected"));
      } else {
        r.appendChild(elem("span", "sm-tag sm-spacer", ""));
      }

      if (!c.configured) {
        const add = elem("button", "sm-add-btn", "Add");
        add.title = "Save this server";
        add.addEventListener("click", (e) => { e.stopPropagation(); addDetected(c.url); });
        r.appendChild(add);
      } else {
        const rm = elem("button", "sm-remove");
        rm.title = "Remove server";
        rm.setAttribute("aria-label", "Remove " + c.url);
        rm.appendChild(svgEl(["M18 6 6 18", "M6 6l12 12"], "", "2.2"));
        rm.addEventListener("click", (e) => { e.stopPropagation(); removeServer(c.url); });
        r.appendChild(rm);
      }
      return r;
    }

    function addForm() {
      const form = elem("form", "sm-form");
      form.appendChild(elem("div", "sm-form-label", "Add a server"));

      const urlIn = elem("input", "sm-input");
      urlIn.type = "text"; urlIn.spellcheck = false; urlIn.autocomplete = "off";
      urlIn.placeholder = "http://localhost:8000 or a LAN address";

      const keyIn = elem("input", "sm-input sm-input-key");
      keyIn.type = "password"; keyIn.spellcheck = false; keyIn.autocomplete = "off";
      keyIn.placeholder = "API key (optional)";

      const errorEl = elem("div", "sm-error");
      errorEl.hidden = true;

      const addBtn = elem("button", "sm-submit", "Add");
      addBtn.type = "submit";

      const rowWrap = elem("div", "sm-form-row");
      rowWrap.appendChild(urlIn);
      rowWrap.appendChild(addBtn);
      form.appendChild(rowWrap);
      form.appendChild(keyIn);
      form.appendChild(errorEl);

      form.addEventListener("submit", async (e) => {
        e.preventDefault();
        errorEl.hidden = true;
        let url = String(urlIn.value || "").trim();
        if (!url) { showErr("Enter a server address."); return; }
        if (!/^https?:\/\//i.test(url)) url = "http://" + url;
        url = S.normUrl(url);
        if (!S.originPattern(url)) { showErr("That doesn't look like a valid URL."); return; }

        addBtn.disabled = true;
        const granted = await S.ensureOrigin(url);
        if (!granted) { addBtn.disabled = false; showErr("Permission to reach that host was denied."); return; }

        const st = await S.load();
        const key = String(keyIn.value || "");
        if (st.servers.some((s) => s.url === url)) {
          st.servers = st.servers.map((s) => (s.url === url ? { url, apiKey: key || s.apiKey } : s));
        } else {
          st.servers.push({ url, apiKey: key });
        }
        st.activeUrl = url; // a freshly added server becomes the target
        await S.save(st);
        urlIn.value = ""; keyIn.value = "";
        addBtn.disabled = false;
        onActiveChange(url);
        await refresh();
      });

      function showErr(msg) { errorEl.textContent = msg; errorEl.hidden = false; }
      return form;
    }

    refresh();
    return { refresh };
  }

  root.almaServersUI = { mount };
  if (typeof module !== "undefined" && module.exports) module.exports = root.almaServersUI;
})(typeof globalThis !== "undefined" ? globalThis : this);
