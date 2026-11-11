(() => {
  const INSTANCE_KEY = "__octobuddyZedPluginInstanceV1";
  const ACTION_ATTRIBUTE = "data-octobuddy-zed-action";
  const existing = window[INSTANCE_KEY];
  if (existing && existing.active) return;

  if (window.top !== window || window.location.protocol !== "file:") return;
  const pagePath = window.location.pathname;
  const allowedPage = pagePath.endsWith("/Contents/Resources/communication-renderer/index.html")
    || pagePath.endsWith("/Contents/Resources/app.asar/out/renderer/index.html");
  if (!allowedPage) return;

  const capability = window.octoEnhancer;
  if (!capability || typeof capability.openZed !== "function" || typeof capability.onDispose !== "function") return;

  const entries = new Map();
  const focusStyle = document.createElement("style");
  focusStyle.textContent = `[${ACTION_ATTRIBUTE}]:focus-visible { outline: 2px solid currentColor !important; outline-offset: 2px; }`;
  let disposed = false;
  let instance;
  const urlStatus = document.createElement("span");
  urlStatus.setAttribute("role", "status");
  const openZedUrl = async (event) => {
    if (disposed || !event.isTrusted || !(event.target instanceof Element)) return;
    const anchor = event.target.closest("a[href]");
    const href = anchor?.getAttribute("href");
    const sanitized = href === "" && anchor.hasAttribute("node");
    if (!sanitized && (!href || !/^zed:\/\/[^/\s]+/i.test(href))) return;
    event.preventDefault();
    event.stopImmediatePropagation();
    urlStatus.remove();
    const linkId = sanitized ? crypto.randomUUID() : undefined;
    if (linkId) anchor.setAttribute("data-octobuddy-zed-click", linkId);
    try {
      await capability.openZed(linkId ? { linkId } : { path: href });
    } catch {
      if (!disposed && anchor.isConnected) {
        urlStatus.textContent = "Zed could not open this link.";
        anchor.insertAdjacentElement("afterend", urlStatus);
      }
    } finally {
      if (linkId) anchor.removeAttribute("data-octobuddy-zed-click");
    }
  };
  window.addEventListener("click", openZedUrl, true);

  function parseFileLink(anchor) {
    const href = anchor.getAttribute("href");
    if (typeof href !== "string") return null;
    let resource = href.trim();
    if (!resource || resource.startsWith("//") || resource.includes("?")) return null;

    let line;
    const hashLine = resource.match(/#L([1-9]\d*)$/);
    if (hashLine) {
      line = Number(hashLine[1]);
      resource = resource.slice(0, -hashLine[0].length);
    } else if (resource.includes("#")) {
      return null;
    } else {
      const colonLine = resource.match(/:([1-9]\d*)$/);
      if (colonLine) {
        line = Number(colonLine[1]);
        resource = resource.slice(0, -colonLine[0].length);
      }
    }
    if (line !== undefined && !Number.isSafeInteger(line)) return null;

    try {
      let path;
      if (/^file:/i.test(resource)) {
        const url = new URL(resource);
        if (url.protocol !== "file:" || (url.hostname !== "" && url.hostname !== "localhost")
          || url.username || url.password || url.search || url.hash) return null;
        path = decodeURIComponent(url.pathname);
      } else {
        if (!resource.startsWith("/") || resource.startsWith("//") || /^[a-z][a-z\d+.-]*:/i.test(resource)) return null;
        path = resource;
      }
      if (!path.startsWith("/") || path.startsWith("//") || path.includes("\0")) return null;
      return { path, ...(line === undefined ? {} : { line }) };
    } catch {
      return null;
    }
  }

  function removeEntry(anchor) {
    const entry = entries.get(anchor);
    if (!entry) return;
    entry.button.removeEventListener("click", entry.onClick);
    entry.button.remove();
    entry.status.remove();
    entries.delete(anchor);
  }

  function createEntry(anchor) {
    const button = document.createElement("button");
    button.type = "button";
    button.setAttribute(ACTION_ATTRIBUTE, "");
    button.setAttribute("aria-label", "Open in Zed");
    button.textContent = "Open in Zed";
    button.style.font = "inherit";
    button.style.fontSize = "0.85em";
    button.style.marginInlineStart = "0.5em";
    button.style.padding = "0.12em 0.45em";
    button.style.border = "1px solid currentColor";
    button.style.borderRadius = "4px";
    button.style.background = "transparent";
    button.style.color = "inherit";
    button.style.cursor = "pointer";
    button.style.verticalAlign = "baseline";

    const status = document.createElement("span");
    status.setAttribute("role", "status");
    status.setAttribute("aria-live", "polite");
    status.style.marginInlineStart = "0.4em";
    status.style.fontSize = "0.85em";
    status.style.color = "inherit";

    const entry = { button, status, onClick: null };
    entry.onClick = async (event) => {
      if (disposed || !event.isTrusted || button.disabled) return;
      const currentTarget = anchor.isConnected ? parseFileLink(anchor) : null;
      if (!currentTarget) {
        removeEntry(anchor);
        return;
      }
      event.preventDefault();
      event.stopPropagation();
      button.disabled = true;
      status.textContent = "";
      try {
        await capability.openZed({ ...currentTarget });
      } catch {
        if (!disposed) status.textContent = "Zed could not open this file.";
      } finally {
        if (!disposed) button.disabled = false;
      }
    };
    button.addEventListener("click", entry.onClick);
    anchor.insertAdjacentElement("afterend", button);
    button.insertAdjacentElement("afterend", status);
    entries.set(anchor, entry);
  }

  function scan() {
    if (disposed) return;
    for (const [anchor, entry] of entries) {
      const target = anchor.isConnected ? parseFileLink(anchor) : null;
      if (!target || !anchor.parentNode) {
        removeEntry(anchor);
        continue;
      }
      if (entry.button.previousElementSibling !== anchor) anchor.insertAdjacentElement("afterend", entry.button);
      if (entry.status.previousElementSibling !== entry.button) entry.button.insertAdjacentElement("afterend", entry.status);
    }
    for (const anchor of document.querySelectorAll("a[href]")) {
      if (entries.has(anchor)) continue;
      const target = parseFileLink(anchor);
      if (target) createEntry(anchor);
    }
  }

  const observer = new MutationObserver(scan);

  function dispose() {
    if (disposed) return;
    disposed = true;
    observer.disconnect();
    window.removeEventListener("click", openZedUrl, true);
    urlStatus.remove();
    for (const anchor of entries.keys()) removeEntry(anchor);
    focusStyle.remove();
    if (window[INSTANCE_KEY] === instance) delete window[INSTANCE_KEY];
  }

  instance = { active: true, dispose };
  Object.defineProperty(window, INSTANCE_KEY, { configurable: true, value: instance });
  try {
    capability.onDispose(dispose);
    if (!disposed) {
      document.head.append(focusStyle);
      observer.observe(document.documentElement, {
        childList: true,
        subtree: true,
        attributes: true,
        attributeFilter: ["href"],
      });
      scan();
    }
  } catch (error) {
    dispose();
    throw error;
  }
})();
