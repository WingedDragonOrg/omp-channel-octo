// Adapted from electron-webext/framework/extensions/_management/toolbar.js.
(() => {
  if (window.__ewManagementShortcut || window !== window.top) return;
  const toggle = (event) => {
    if (!event.isTrusted || !event.metaKey || !event.altKey || !event.shiftKey || event.ctrlKey || event.code !== "KeyP" || event.repeat) return;
    event.preventDefault();
    window.__ewToggleManagementPanel?.();
  };
  window.addEventListener("keydown", toggle, true);
  window.__ewManagementShortcut = toggle;
  window.__ewHostRegisterDispose(() => {
    window.removeEventListener("keydown", toggle, true);
    if (window.__ewManagementShortcut === toggle) delete window.__ewManagementShortcut;
  });
})();
