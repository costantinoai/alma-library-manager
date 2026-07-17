/* ALMa Connector — background (toolbar badge).
 *
 * Lights a small green dot on the toolbar icon when the current tab looks
 * like a savable paper, so you can tell at a glance without opening the
 * popup. The dot is a bullet glyph on a transparent badge background (no
 * pill) — Firefox's badge background accepts an [r,g,b,a] array, so alpha
 * 0 leaves just the coloured dot.
 *
 * Detection is URL-only (DOI in the URL, arXiv, preprint/publisher paths,
 * via the same `almaExtract` helpers the popup uses) — no page injection,
 * no per-page network calls, so it needs only the `tabs` permission to
 * read tab URLs. Pages that hide the DOI in `<meta>` tags only won't badge
 * (that needs the popup's page read on click), but the strong URL signals
 * — arXiv, doi.org, /doi/…, biorxiv/medRxiv, DOI-bearing PDFs — all do.
 */
(function () {
  "use strict";

  const api = globalThis.browser || globalThis.chrome;
  const A = globalThis.almaExtract;
  const S = globalThis.almaSettings;
  const OB = globalThis.almaOutbox;
  const DOT = "●";          // U+25CF — full round dot (rendered green on a transparent badge)
  // Brightened brand sage so the dot stays legible on light AND dark toolbars.
  const GREEN = "#4FA45E";
  const TRANSPARENT = [0, 0, 0, 0]; // [r,g,b,a] — no badge pill, just the dot

  function hasPaper(url) {
    if (!url || !/^https?:/i.test(url) || !A) return false;
    return !!(A.doiFromUrl(url) || A.arxivIdFromUrl(url));
  }

  function setBadge(tabId, on) {
    if (tabId == null) return;
    try {
      api.action.setBadgeText({ tabId, text: on ? DOT : "" });
      api.action.setTitle({
        tabId,
        title: on ? "Save this paper to ALMa — paper detected on this page" : "Save this paper to ALMa",
      });
      if (on) {
        // Transparent background → only the green dot shows, no pill.
        api.action.setBadgeBackgroundColor({ tabId, color: TRANSPARENT });
        if (api.action.setBadgeTextColor) api.action.setBadgeTextColor({ tabId, color: GREEN });
      }
    } catch (e) { /* action API unavailable for this tab */ }
  }

  function update(tab) {
    if (tab && tab.id != null) setBadge(tab.id, hasPaper(tab.url));
  }

  // Page finished loading or navigated within the tab.
  api.tabs.onUpdated.addListener((tabId, changeInfo, tab) => {
    if (changeInfo.status === "complete" || changeInfo.url) {
      setBadge(tabId, hasPaper((tab && tab.url) || changeInfo.url));
    }
  });

  // Switched tabs — reflect the now-active tab.
  api.tabs.onActivated.addListener(async ({ tabId }) => {
    try { update(await api.tabs.get(tabId)); } catch (e) { /* ignore */ }
  });

  // Initial pass for the tab that's already active when we load.
  api.tabs.query({ active: true, currentWindow: true })
    .then((tabs) => { if (tabs && tabs[0]) update(tabs[0]); })
    .catch(() => {});

  // --- Offline outbox drainer ---------------------------------------------
  // Deliver captures queued while a server was down (lib/outbox.js), each to
  // the instance it was queued for — verified by /ping identity, so a capture
  // can never land in the wrong database. Runs on load and on a periodic
  // alarm, so the queue drains even if the popup is never opened (the browser
  // just has to be running while the target ALMa is up).
  async function flushOutbox() {
    if (!S || !OB) return;
    try {
      if ((await OB.count()) === 0) return;
      await OB.flushVia(S);
    } catch (e) { /* best effort */ }
  }

  const FLUSH_ALARM = "alma-outbox-flush";
  try {
    if (api.alarms) {
      api.alarms.create(FLUSH_ALARM, { periodInMinutes: 5 });
      api.alarms.onAlarm.addListener((a) => { if (a && a.name === FLUSH_ALARM) flushOutbox(); });
    }
  } catch (e) { /* alarms API unavailable */ }
  flushOutbox();
})();
