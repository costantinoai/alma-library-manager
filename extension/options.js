/* ALMa Connector — options page.
 *
 * The standalone (about:addons → Preferences) view of the server
 * manager. Mounts the same shared component the popup's in-panel
 * Settings uses, so the two never drift.
 */
(function () {
  "use strict";
  const SUI = globalThis.almaServersUI;
  const container = document.getElementById("server-manager");
  if (SUI && container) SUI.mount(container, { onActiveChange: function () {} });
})();
