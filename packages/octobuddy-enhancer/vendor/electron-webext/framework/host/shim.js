// Adapted from electron-webext/framework/host/shim.js at the pinned upstream revision.
// This fork runs only in host-created isolated worlds and exposes a narrow, real API subset.
(() => {
  const previous = window.__ewShimState;
  if (previous) {
    previous.activate();
    return;
  }
  let bridgeSend;

  function captureBridgeSend() {
    const binding = window.__ewSend;
    if (typeof binding !== "function") return;
    try { delete window.__ewSend; } catch { /* Try replacing an immutable binding below. */ }
    if (typeof window.__ewSend === "function") {
      try {
        Object.defineProperty(window, "__ewSend", { configurable: true, enumerable: false, writable: false, value: undefined });
      } catch {
        try { window.__ewSend = undefined; } catch { /* Fail closed below if the binding is immutable. */ }
      }
    }
    bridgeSend = typeof window.__ewSend === "function" ? undefined : binding;
  }

  captureBridgeSend();

  const pending = new Map();
  const listeners = new Map();
  const disposers = [];
  let callId = 0;
  let active = true;
  let trustedClickDepth = 0;
  let executedScripts = 0;
  const wrappedListeners = new WeakMap();
  const managedTimers = new Map();
  const automaticListeners = new Set();
  const listenersByTarget = new WeakMap();
  const revocationErrors = [];
  const createdNodes = new Set();
  let cleanupTrackingReliable = true;

  function timerError(error) {
    return error instanceof Error ? error.message : String(error);
  }

  function installTimerGuard(setName, clearName, repeating = false) {
    const originalSet = window[setName];
    const originalClear = window[clearName];
    if (typeof originalSet !== "function" || typeof originalClear !== "function") return;
    window[setName] = function (callback, ...args) {
      if (!active) return -1;
      if (typeof callback !== "function") throw new TypeError("plugin timers require a function callback");
      let id;
      const guarded = function (...callbackArgs) {
        if (!repeating) managedTimers.delete(id);
        if (active) return callback.apply(window, callbackArgs);
      };
      id = originalSet.call(this, guarded, ...args);
      managedTimers.set(id, () => originalClear.call(window, id));
      return id;
    };
    window[clearName] = function (id) {
      const cancel = managedTimers.get(id);
      if (!cancel) return originalClear.call(this, id);
      managedTimers.delete(id);
      return cancel();
    };
  }

  function clearManagedTimers() {
    for (const cancel of managedTimers.values()) {
      try { cancel(); }
      catch (error) { revocationErrors.push(timerError(error)); }
    }
    managedTimers.clear();
  }

  function isConnectedToDocument(node) {
    try {
      if (typeof node.isConnected === "boolean") return node.isConnected;
    } catch { /* Use parent traversal when the browser does not expose isConnected. */ }
    for (let current = node; current; current = current.parentNode) {
      if (current === document || current === document.documentElement) return true;
    }
    return false;
  }

  function countResidualNodes() {
    let count = 0;
    for (const node of createdNodes) {
      if (isConnectedToDocument(node)) count += 1;
      else createdNodes.delete(node);
    }
    return count;
  }

  function forgetListener(entry) {
    automaticListeners.delete(entry);
    const entries = listenersByTarget.get(entry.target);
    entries?.delete(entry);
    if (entries?.size === 0) listenersByTarget.delete(entry.target);
  }

  function clearManagedListeners(errors) {
    for (const entry of [...automaticListeners]) {
      try {
        entry.remove();
        forgetListener(entry);
      } catch (error) {
        errors.push(timerError(error));
      }
    }
  }

  installTimerGuard("setTimeout", "clearTimeout");
  installTimerGuard("setInterval", "clearInterval", true);
  installTimerGuard("requestAnimationFrame", "cancelAnimationFrame");
  installTimerGuard("requestIdleCallback", "cancelIdleCallback");

  const defineHidden = (name, value) => Object.defineProperty(window, name, {
    configurable: false,
    enumerable: false,
    writable: false,
    value,
  });

  function resolve(id, result, isError) {
    const entry = pending.get(id);
    if (!entry) return;
    pending.delete(id);
    if (isError) entry.reject(new Error(typeof result === "string" ? result : JSON.stringify(result)));
    else entry.resolve(result);
  }

  function revoke() {
    active = false;
    clearManagedTimers();
    for (const entry of pending.values()) entry.reject(new Error("plugin is disabled"));
    pending.clear();
  }

  function dispose() {
    const errors = revocationErrors.splice(0);
    const callbacks = disposers.splice(0);
    for (const callback of callbacks) {
      try { callback(); }
      catch (error) { errors.push(timerError(error)); }
    }
    const listenerCount = automaticListeners.size;
    clearManagedListeners(errors);
    let eventListenerCount = 0;
    for (const set of listeners.values()) {
      eventListenerCount += set.size;
      set.clear();
    }
    listeners.clear();
    const ran = executedScripts;
    executedScripts = 0;
    const residualNodes = countResidualNodes();
    const residualListeners = automaticListeners.size;
    const residualTimers = managedTimers.size;
    const uncertain = ran > 0 && !cleanupTrackingReliable;
    if (uncertain) errors.push("effect cleanup tracking could not be installed");
    return {
      count: callbacks.length + listenerCount + eventListenerCount,
      errors,
      ran,
      residualNodes,
      residualListeners,
      residualTimers,
      uncertain,
    };
  }

  function activate() {
    captureBridgeSend();
    active = true;
  }

  function call(api, method, args) {
    if (!active) return Promise.reject(new Error("plugin is disabled"));
    const id = ++callId;
    const { promise, resolve: accept, reject } = Promise.withResolvers();
    pending.set(id, { resolve: accept, reject });
    try {
      if (typeof bridgeSend !== "function") throw new Error("host bridge is unavailable");
      bridgeSend(JSON.stringify({
        callId: id,
        api,
        method,
        args: args || [],
        trustedClick: trustedClickDepth > 0,
      }));
    } catch (error) {
      pending.delete(id);
      reject(new Error(`bridge unavailable: ${error instanceof Error ? error.message : String(error)}`));
    }
    return promise;
  }

  function emit(name, payload) {
    const set = listeners.get(name);
    if (!set) return;
    for (const listener of [...set]) {
      try {
        if (name === "management.onErrors") listener(payload.id, payload.messages);
        else listener(payload);
      } catch (error) {
        console.error("[electron-webext event]", name, error);
      }
    }
  }

  function makeEvent(name) {
    const set = listeners.get(name) || new Set();
    listeners.set(name, set);
    return Object.freeze({
      addListener: (listener) => { if (typeof listener === "function") set.add(listener); },
      removeListener: (listener) => set.delete(listener),
      hasListener: (listener) => set.has(listener),
    });
  }

  function wrapEventTarget(target) {
    if (!target || typeof target.addEventListener !== "function" || wrappedListeners.has(target)) return;
    const originalAdd = target.addEventListener;
    const originalRemove = target.removeEventListener;
    if (typeof originalRemove !== "function") cleanupTrackingReliable = false;
    const wrappers = new WeakMap();
    const add = function (type, listener, options) {
      const callable = typeof listener === "function" || typeof listener?.handleEvent === "function";
      if (!callable) return originalAdd.call(this, type, listener, options);
      let byType = wrappers.get(listener);
      if (!byType) { byType = new Map(); wrappers.set(listener, byType); }
      let wrapper = byType.get(type);
      if (!wrapper) {
        wrapper = function (event) {
          const wasTrusted = trustedClickDepth;
          if (type === "click" && event && event.isTrusted === true) trustedClickDepth += 1;
          try {
            return typeof listener === "function"
              ? listener.call(this, event)
              : listener.handleEvent.call(listener, event);
          } finally { trustedClickDepth = wasTrusted; }
        };
        byType.set(type, wrapper);
      }
      const result = originalAdd.call(this, type, wrapper, options);
      const capture = options === true || (typeof options === "object" && options?.capture === true);
      const entries = listenersByTarget.get(this) || new Set();
      const duplicate = [...entries].some((entry) =>
        entry.type === type && entry.listener === listener && entry.capture === capture);
      if (!duplicate) {
        const entry = {
          target: this,
          type,
          listener,
          wrapper,
          options,
          capture,
          remove: () => {
            if (typeof originalRemove !== "function") throw new Error("event listener cannot be removed");
            originalRemove.call(this, type, wrapper, options);
          },
        };
        entries.add(entry);
        listenersByTarget.set(this, entries);
        automaticListeners.add(entry);
      }
      return result;
    };
    const remove = typeof originalRemove === "function"
      ? function (type, listener, options) {
        const wrapper = wrappers.get(listener)?.get(type);
        const result = originalRemove.call(this, type, wrapper || listener, options);
        const capture = options === true || (typeof options === "object" && options?.capture === true);
        for (const entry of [...(listenersByTarget.get(this) || [])]) {
          if (entry.type === type && entry.listener === listener && entry.capture === capture) {
            forgetListener(entry);
          }
        }
        return result;
      }
      : undefined;
    try {
      target.addEventListener = add;
      if (remove) target.removeEventListener = remove;
      if (target.addEventListener !== add || (remove && target.removeEventListener !== remove)) {
        cleanupTrackingReliable = false;
        return;
      }
      wrappedListeners.set(target, true);
    } catch {
      cleanupTrackingReliable = false;
    }
  }

  const eventTargetPrototype = window.EventTarget?.prototype;
  function wrapNodeFactory(name) {
    const original = document && document[name];
    if (typeof original !== "function") return;
    const factory = function (...args) {
      const node = original.apply(this, args);
      if (node && typeof node === "object") createdNodes.add(node);
      if (!eventTargetPrototype) wrapEventTarget(node);
      return node;
    };
    try {
      document[name] = factory;
      if (document[name] !== factory) cleanupTrackingReliable = false;
    } catch {
      cleanupTrackingReliable = false;
    }
  }

  if (eventTargetPrototype) {
    wrapEventTarget(eventTargetPrototype);
  } else {
    wrapEventTarget(document);
    wrapEventTarget(window);
  }
  for (const name of ["createElement", "createElementNS", "createTextNode"]) wrapNodeFactory(name);

  function registerDispose(callback) {
    if (typeof callback !== "function") throw new TypeError("onDispose requires a function");
    if (!active) throw new Error("plugin is disabled");
    disposers.push(callback);
  }

  function markScriptRan() { executedScripts += 1; }

  defineHidden("__ewResolve", resolve);
  defineHidden("__ewEmit", emit);
  defineHidden("__ewRevoke", revoke);
  defineHidden("__ewHostDispose", dispose);
  defineHidden("__ewHostActivate", activate);
  defineHidden("__ewHostRegisterDispose", registerDispose);
  defineHidden("__ewHostMarkScript", markScriptRan);
  defineHidden("__ewShimState", Object.freeze({ activate }));

  const identity = window.__ewSelfInfo;
  if (identity?.management === true) {
    const management = Object.freeze({
      getAll: () => call("management", "getAll", []),
      get: (id) => call("management", "get", [id]),
      getSelf: () => call("management", "getSelf", []),
      setEnabled: (id, enabled) => call("management", "setEnabled", [id, enabled]),
      onEnabled: makeEvent("management.onEnabled"),
      onDisabled: makeEvent("management.onDisabled"),
      onErrors: makeEvent("management.onErrors"),
    });
    const chrome = Object.freeze({ management });
    Object.defineProperty(window, "chrome", { configurable: false, enumerable: false, value: chrome });
    if (!window.browser) Object.defineProperty(window, "browser", { configurable: false, enumerable: false, value: chrome });
  } else {
    const api = Object.freeze({
      openZed: (options) => call("zed", "openZed", [options]),
      getChannelStatus: () => call("status", "getChannelStatus", []),
      getChatStatus: (target, requestId) => call("status", "getChatStatus", [target, requestId]),
      cancelChatStatus: (requestId) => call("status", "cancelChatStatus", [requestId]),
      getChatDetails: (target, requestId) => call("status", "getChatDetails", [target, requestId]),
      cancelChatDetails: (requestId) => call("status", "cancelChatDetails", [requestId]),
      onDispose: registerDispose,
    });
    Object.defineProperty(window, "octoEnhancer", { configurable: false, enumerable: false, value: api });
  }
})();
