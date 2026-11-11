// Adapted from electron-webext/framework/extensions/_management/panel.js.
// Metadata is always rendered as text; management mutations require trusted clicks.
(() => {
  if (window.__ewManagementPanel || window !== window.top) return;
  const host = document.createElement("div");
  host.id = "octo-enhancer-management";
  const root = host.attachShadow({ mode: "closed" });
  root.innerHTML = `
    <style>
      :host, #panel { all: initial; }
      #panel { position: fixed; top: 62px; right: 16px; width: 360px; max-height: 72vh;
        overflow: auto; z-index: 2147483647; display: none; box-sizing: border-box;
        padding: 14px; border: 1px solid #333; border-radius: 12px; background: #1f2228;
        color: #e6e6e6; font: 13px/1.45 ui-sans-serif,system-ui,sans-serif;
        box-shadow: 0 10px 40px rgba(0,0,0,.45); }
      #panel.open { display: block; }
      header { display: flex; align-items: center; justify-content: space-between; gap: 12px; }
      h2 { margin: 0; font-size: 15px; }
      #count { color: #aaa; font-size: 11px; }
      #list { margin-top: 10px; }
      .plugin { display: grid; grid-template-columns: 1fr auto; gap: 2px 8px;
        padding: 9px 2px; border-top: 1px solid #373a42; }
      .name { overflow-wrap: anywhere; font-weight: 600; }
      .version { color: #999; font-size: 11px; }
      .description { grid-column: 1 / -1; color: #b9bbc1; overflow-wrap: anywhere; }
      .error { grid-column: 1 / -1; color: #ff9b9b; white-space: pre-wrap; overflow-wrap: anywhere; }
      button { border: 1px solid #4a4e58; border-radius: 7px; padding: 5px 9px;
        background: #2a2f38; color: #eee; font: inherit; cursor: pointer; }
      button:hover { background: #353b46; }
      button:disabled { opacity: .45; cursor: default; }
      #close { padding: 2px 8px; font-size: 16px; }
      #status { min-height: 1em; margin-top: 8px; color: #ff9b9b; white-space: pre-wrap; }
    </style>
    <section id="panel" aria-label="OctoBuddy plugin management">
      <header><h2>OctoBuddy plugins <span id="count"></span></h2><button id="close" type="button" aria-label="Close">×</button></header>
      <div id="list"></div>
      <div id="status" role="status"></div>
    </section>
  `;
  document.documentElement.appendChild(host);

  const panel = root.querySelector("#panel");
  const list = root.querySelector("#list");
  const count = root.querySelector("#count");
  const status = root.querySelector("#status");
  const close = root.querySelector("#close");

  function toggle(open) {
    const willOpen = open ?? !panel.classList.contains("open");
    panel.classList.toggle("open", willOpen);
    if (willOpen) void refresh();
  }

  const togglePanel = () => toggle();
  Object.defineProperty(window, "__ewToggleManagementPanel", {
    configurable: true,
    enumerable: false,
    value: togglePanel,
  });

  function showError(error) {
    status.textContent = error instanceof Error ? error.message : String(error);
  }

  async function refresh() {
    try {
      const plugins = await chrome.management.getAll();
      count.textContent = `(${plugins.length})`;
      list.replaceChildren();
      for (const plugin of plugins) {
        const row = document.createElement("article");
        row.className = "plugin";
        const name = document.createElement("div");
        name.className = "name";
        name.textContent = typeof plugin.name === "string" ? plugin.name : plugin.id;
        const version = document.createElement("div");
        version.className = "version";
        version.textContent = typeof plugin.version === "string" ? plugin.version : "";
        const description = document.createElement("div");
        description.className = "description";
        description.textContent = typeof plugin.description === "string" ? plugin.description : "";
        const action = document.createElement("button");
        action.type = "button";
        action.textContent = plugin.enabled ? "Disable" : "Enable";
        action.disabled = plugin.id === "_management";
        action.title = plugin.id === "_management" ? "The management UI is always enabled" : "";
        action.addEventListener("click", async (event) => {
          if (!event.isTrusted || action.disabled) return;
          status.textContent = "";
          try { await chrome.management.setEnabled(plugin.id, !plugin.enabled); }
          catch (error) { showError(error); }
          await refresh();
        });
        row.append(name, action, version, description);
        for (const error of Array.isArray(plugin.errors) ? plugin.errors : []) {
          const line = document.createElement("div");
          line.className = "error";
          line.textContent = String(error);
          row.appendChild(line);
        }
        list.appendChild(row);
      }
    } catch (error) {
      showError(error);
    }
  }

  const closePanel = (event) => { if (event.isTrusted) toggle(false); };
  close.addEventListener("click", closePanel);
  chrome.management.onEnabled.addListener(() => { void refresh(); });
  chrome.management.onDisabled.addListener(() => { void refresh(); });
  chrome.management.onErrors.addListener((id, messages) => {
    const message = Array.isArray(messages) ? messages.map(String).join("\n") : String(messages);
    status.textContent = `[${String(id)}] ${message}`;
    void refresh();
  });

  window.__ewManagementPanel = host;
  window.__ewHostRegisterDispose(() => {
    close.removeEventListener("click", closePanel);
    if (window.__ewToggleManagementPanel === togglePanel) delete window.__ewToggleManagementPanel;
    host.remove();
    if (window.__ewManagementPanel === host) delete window.__ewManagementPanel;
  });
  void refresh();
})();
