(() => {
  if (window.__octobuddyStatusNavigation) return;

  let active = true;
  let timer;
  let store;
  let previousType;
  let previousId;
  let published = false;

  function publish(force = false) {
    if (!store) return;
    const channel = store.openChannel;
    const channelType = channel?.channelType ?? null;
    const channelId = channel?.channelID ?? null;
    if (!force && published && channelType === previousType && channelId === previousId) return;
    previousType = channelType;
    previousId = channelId;
    published = true;
    window.postMessage({ type: "octobuddy-enhancer:chat", channelType, channelId }, "*");
  }

  function onMessage(event) {
    if (event.source === window && event.data?.type === "octobuddy-enhancer:chat-request") publish(true);
  }

  async function start() {
    const link = [...document.querySelectorAll('link[rel="modulepreload"]')]
      .find(element => /\/App-[^/]+\.js$/.test(element.href));
    const module = await import(link.href);
    if (!active) return;
    store = module.i.shared;
    publish();
    timer = window.setInterval(publish, 250);
  }

  const observer = {
    dispose() {
      active = false;
      window.clearInterval(timer);
      window.removeEventListener("message", onMessage);
      document.removeEventListener("DOMContentLoaded", start);
      if (window.__octobuddyStatusNavigation === observer) delete window.__octobuddyStatusNavigation;
    },
  };
  window.__octobuddyStatusNavigation = observer;
  window.addEventListener("message", onMessage);
  if (document.readyState === "loading") document.addEventListener("DOMContentLoaded", start, { once: true });
  else void start();
})();
