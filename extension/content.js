/* ALMa Connector — page announce (content script).
 *
 * Injected at document_start on the ALMa web-UI origins (localhost :8000
 * Docker / :8001 dev backend / :5173 Vite). It stamps two data-attributes on
 * <html> so the ALMa web app can tell — with NO network round-trip to the
 * extension — that the connector is installed and whether it is still
 * compatible with this ALMa build:
 *
 *   data-alma-connector           the connector's release version (manifest)
 *   data-alma-connector-contract  the save-contract version this connector
 *                                 was built against (see CONTRACT below)
 *
 * The web app compares `contract` against the backend's CURRENT contract
 * version (GET /api/v1/extension/ping -> connector_version). A mismatch means
 * the /save request/response shape changed and one side needs an update; a
 * match means the pair is compatible and the app stays silent.
 *
 * Content scripts run in an isolated JS world but share the page DOM, so a
 * data-attribute on <html> is the simplest reliable channel the page can
 * read. document_start guarantees the marker is present before the ALMa
 * React app mounts and runs its startup check.
 */
(function () {
  "use strict";

  const api = globalThis.browser || globalThis.chrome;

  // The save-contract version this connector speaks. MUST stay in lockstep
  // with the backend's CONNECTOR_API_VERSION (src/alma/api/routes/extension.py):
  // bump BOTH together whenever the /save request or response shape changes.
  const CONTRACT = 1;

  function version() {
    try { return api.runtime.getManifest().version; }
    catch (e) { return ""; }
  }

  function announce() {
    const el = document.documentElement;
    if (!el) { requestAnimationFrame(announce); return; } // pre-<html>; retry
    try {
      el.dataset.almaConnector = version();
      el.dataset.almaConnectorContract = String(CONTRACT);
    } catch (e) { /* nothing readable to announce on */ }
  }

  announce();
})();
