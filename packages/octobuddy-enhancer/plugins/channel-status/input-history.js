(() => {
  const INSTANCE_KEY = "__octobuddyInputHistoryPluginInstanceV1";
  const CHAT_MESSAGE_TYPE = "octobuddy-enhancer:chat";
  const CHAT_REQUEST_TYPE = "octobuddy-enhancer:chat-request";
  const LEGACY_HISTORY_KEY = "octobuddy.inputHistory.v1";
  const LEGACY_LIMIT_KEY = "octobuddy.inputHistory.limit";
  const DEFAULT_LIMIT = 100;
  const SUBMISSION_WINDOW_MS = 15_000;
  const existing = window[INSTANCE_KEY];
  if (existing && existing.active) return;

  if (window.top !== window || window.location.protocol !== "file:") return;
  if (!window.location.pathname.endsWith("/Contents/Resources/communication-renderer/index.html")) return;

  const capability = window.octoEnhancer;
  if (!capability || typeof capability.onDispose !== "function") return;

  let disposed = false;
  let disposedOnce = false;
  let editor;
  let editorResizeObserver;
  let currentChat;
  let chatKey;
  let chatGeneration = 0;
  let composing = false;
  let menuVisible = false;
  let selectedIndex = 0;
  let filteredEntries = [];
  let pendingSubmission;
  let updateScheduled = false;
  let observer;
  let historyStates = new Map();
  let bridgeQueue = Promise.resolve();
  let limitRequestVersion = 0;
  let limitIntent = DEFAULT_LIMIT;
  let instance;

  const style = document.createElement("style");
  style.textContent = `
    [data-octobuddy-input-history-root] {
      all: initial !important;
      position: fixed !important;
      z-index: 2147483647 !important;
      display: flex !important;
      flex-direction: column !important;
      background: #ffffff !important;
      border: 1px solid #e3e4ea !important;
      border-radius: 12px !important;
      box-shadow: 0 12px 32px rgba(31, 35, 41, 0.12), 0 2px 8px rgba(31, 35, 41, 0.05) !important;
      color: #262832 !important;
      box-sizing: border-box !important;
      font-family: -apple-system, BlinkMacSystemFont, "PingFang SC", "Segoe UI", sans-serif !important;
      font-size: 13px !important;
      line-height: 1.45 !important;
      max-height: min(320px, calc(100vh - 24px)) !important;
      overflow: hidden !important;
    }
    [data-octobuddy-input-history-root][hidden] { display: none !important; }
    [data-octobuddy-input-history-root] *,
    [data-octobuddy-input-history-root] *::before,
    [data-octobuddy-input-history-root] *::after { box-sizing: border-box !important; }
    [data-octobuddy-input-history-header] {
      align-items: center !important;
      display: flex !important;
      flex: none !important;
      justify-content: space-between !important;
      padding: 11px 14px 8px !important;
    }
    [data-octobuddy-input-history-heading] {
      color: #343640 !important;
      font-size: 13px !important;
      font-weight: 600 !important;
    }
    [data-octobuddy-input-history-shortcuts] {
      color: #8b8e99 !important;
      font-size: 11px !important;
      white-space: nowrap !important;
    }
    [data-octobuddy-input-history-list] {
      flex: 1 1 auto !important;
      max-height: min(250px, 42vh) !important;
      min-height: 0 !important;
      overflow-y: auto !important;
      padding: 2px 6px 7px !important;
      scrollbar-width: thin !important;
    }
    [data-octobuddy-input-history-item] {
      background: transparent !important;
      border: 0 !important;
      border-radius: 7px !important;
      color: #343640 !important;
      cursor: pointer !important;
      display: block !important;
      font: inherit !important;
      margin: 2px 0 !important;
      min-width: 0 !important;
      padding: 9px 10px !important;
      text-align: left !important;
      transition: background-color 80ms ease !important;
      width: 100% !important;
    }
    [data-octobuddy-input-history-item]:hover { background: #f5f5f9 !important; }
    [data-octobuddy-input-history-item][data-selected="true"] {
      background: #f1eefb !important;
      box-shadow: inset 3px 0 #6c56c9 !important;
      color: #4936a0 !important;
    }
    [data-octobuddy-input-history-preview] {
      color: inherit !important;
      display: -webkit-box !important;
      font: inherit !important;
      overflow: hidden !important;
      overflow-wrap: anywhere !important;
      white-space: pre-line !important;
      -webkit-box-orient: vertical !important;
      -webkit-line-clamp: 2 !important;
    }
    [data-octobuddy-input-history-empty] {
      align-items: center !important;
      display: flex !important;
      gap: 11px !important;
      padding: 13px 8px 17px !important;
    }
    [data-octobuddy-input-history-empty-icon] {
      align-items: center !important;
      background: #f3f1fb !important;
      border-radius: 10px !important;
      color: #6c56c9 !important;
      display: flex !important;
      flex: 0 0 34px !important;
      font-size: 20px !important;
      height: 34px !important;
      justify-content: center !important;
    }
    [data-octobuddy-input-history-empty-title] {
      color: #4c4f5b !important;
      font-size: 13px !important;
      font-weight: 500 !important;
    }
    [data-octobuddy-input-history-empty-hint] {
      color: #8b8e99 !important;
      font-size: 12px !important;
      margin-top: 2px !important;
    }
    [data-octobuddy-input-history-loading],
    [data-octobuddy-input-history-error] {
      display: block !important;
      padding: 13px 10px 15px !important;
    }
    [data-octobuddy-input-history-loading] { color: #777a86 !important; }
    [data-octobuddy-input-history-error] { color: #b42318 !important; }
    [data-octobuddy-input-history-settings] {
      align-items: center !important;
      background: #f9f9fb !important;
      border-top: 1px solid #eeeef2 !important;
      color: #777a86 !important;
      display: flex !important;
      flex: none !important;
      font-size: 12px !important;
      justify-content: flex-end !important;
      padding: 7px 14px !important;
    }
    [data-octobuddy-input-history-settings] label {
      align-items: center !important;
      display: inline-flex !important;
      gap: 8px !important;
    }
    [data-octobuddy-input-history-limit] {
      background: #ffffff !important;
      border: 1px solid #dedfe6 !important;
      border-radius: 6px !important;
      color: #4c4f5b !important;
      font: inherit !important;
      height: 25px !important;
      padding: 2px 6px !important;
      width: 62px !important;
    }
    [data-octobuddy-input-history-limit]:focus-visible {
      border-color: #6c56c9 !important;
      outline: 2px solid #dcd5f5 !important;
      outline-offset: 1px !important;
    }
    @media (max-width: 480px), (max-height: 560px) {
      [data-octobuddy-input-history-root] { max-height: min(40vh, 280px) !important; }
      [data-octobuddy-input-history-item] { padding: 7px 9px !important; }
    }
    @media (prefers-reduced-motion: reduce) {
      [data-octobuddy-input-history-item] { transition-duration: 0s !important; }
    }
  `;

  const root = document.createElement("div");
  root.setAttribute("data-octobuddy-input-history-root", "");
  root.setAttribute("role", "group");
  root.setAttribute("aria-label", "输入历史");
  root.hidden = true;


  const header = document.createElement("div");
  header.setAttribute("data-octobuddy-input-history-header", "");
  const heading = document.createElement("span");
  heading.setAttribute("data-octobuddy-input-history-heading", "");
  heading.textContent = "输入历史";
  const shortcuts = document.createElement("span");
  shortcuts.setAttribute("data-octobuddy-input-history-shortcuts", "");
  shortcuts.textContent = "↑↓ 选择　↵ 使用";
  header.append(heading, shortcuts);
  const list = document.createElement("div");
  list.setAttribute("data-octobuddy-input-history-list", "");
  list.setAttribute("role", "listbox");
  list.setAttribute("aria-label", "输入历史记录");

  const settings = document.createElement("div");
  settings.setAttribute("data-octobuddy-input-history-settings", "");
  const settingLabel = document.createElement("label");
  settingLabel.textContent = "历史上限";
  const limitInput = document.createElement("input");
  limitInput.type = "number";
  limitInput.min = "1";
  limitInput.step = "1";
  limitInput.setAttribute("data-octobuddy-input-history-limit", "");
  limitInput.setAttribute("aria-label", "输入历史记录上限");
  settingLabel.append(limitInput);
  settings.append(settingLabel);
  root.append(header, list, settings);


  function chatIdentityKey(chat) {
    return chat ? JSON.stringify([chat.channelType, chat.channelId]) : undefined;
  }

  function validStoredEntry(value) {
    return value !== null && typeof value === "object"
      && typeof value.text === "string" && value.text.trim().length > 0
      && (value.mentions === undefined || Array.isArray(value.mentions));
  }

  function normalizeEntry(value) {
    if (!validStoredEntry(value)) throw new Error("Invalid input history entry");
    return {
      text: value.text,
      ...(value.mentions === undefined ? {} : { mentions: value.mentions }),
    };
  }

  function normalizeSnapshot(value) {
    if (!value || typeof value !== "object" || !Number.isInteger(value.limit) || value.limit < 1
      || !Array.isArray(value.entries)) throw new Error("Invalid input history snapshot");
    return { limit: value.limit, entries: value.entries.map(normalizeEntry) };
  }

  function bridgeMethod(name) {
    const method = capability[name];
    if (typeof method !== "function") throw new Error(`Missing input history bridge method: ${name}`);
    return method.bind(capability);
  }

  function enqueueBridge(operation) {
    const result = bridgeQueue.then(operation);
    bridgeQueue = result.catch(() => undefined);
    return result;
  }

  function readLegacyHistory(key) {
    const historyRaw = window.localStorage.getItem(LEGACY_HISTORY_KEY);
    const limitRaw = window.localStorage.getItem(LEGACY_LIMIT_KEY);
    let history = Object.create(null);
    if (historyRaw !== null) {
      const parsed = JSON.parse(historyRaw);
      if (!parsed || typeof parsed !== "object" || Array.isArray(parsed)) throw new Error("Invalid legacy history");
      history = parsed;
    }
    const hasChatHistory = Object.prototype.hasOwnProperty.call(history, key);
    const rawEntries = hasChatHistory ? history[key] : [];
    if (!Array.isArray(rawEntries)) throw new Error("Invalid legacy chat history");
    const parsedLimit = Number(limitRaw);
    return {
      entries: rawEntries.filter(validStoredEntry).map(normalizeEntry),
      limit: Number.isInteger(parsedLimit) && parsedLimit > 0 ? parsedLimit : DEFAULT_LIMIT,
      shouldImport: hasChatHistory || limitRaw !== null,
    };
  }

  function acknowledgeLegacyImport(key) {
    const historyRaw = window.localStorage.getItem(LEGACY_HISTORY_KEY);
    if (historyRaw === null) {
      window.localStorage.removeItem(LEGACY_LIMIT_KEY);
      return;
    }
    const history = JSON.parse(historyRaw);
    if (!history || typeof history !== "object" || Array.isArray(history)) return;
    delete history[key];
    if (Object.keys(history).length > 0) {
      window.localStorage.setItem(LEGACY_HISTORY_KEY, JSON.stringify(history));
      return;
    }
    window.localStorage.removeItem(LEGACY_HISTORY_KEY);
    window.localStorage.removeItem(LEGACY_LIMIT_KEY);
  }

  function currentHistoryState() {
    return chatKey ? historyStates.get(chatKey) : undefined;
  }


  function loadChatHistory(key, state) {
    if (state.loadPromise) return state.loadPromise;
    const generation = chatGeneration;
    state.phase = "loading";
    state.error = undefined;
    if (key === chatKey && menuVisible) renderMenu();
    const result = enqueueBridge(async () => {
      let migrationFailed = false;
      try {
        const legacy = readLegacyHistory(key);
        if (legacy.shouldImport) {
          normalizeSnapshot(await bridgeMethod("importInputHistory")(key, legacy.entries, legacy.limit));
          acknowledgeLegacyImport(key);
        }
      } catch {
        migrationFailed = true;
      }
      state.migrationFailed = migrationFailed;
      const snapshot = normalizeSnapshot(await bridgeMethod("loadInputHistory")(key));
      return { snapshot, migrationFailed };
    }).then(({ snapshot, migrationFailed }) => {
      state.entries = snapshot.entries;
      state.limit = snapshot.limit;
      state.phase = "loaded";
      state.error = undefined;
      state.migrationFailed = migrationFailed;
      if (key === chatKey && generation === chatGeneration) {
        limitIntent = snapshot.limit;
        limitInput.value = String(snapshot.limit);
        if (menuVisible) renderMenu();
      }
    }).catch(() => {
      state.phase = "error";
      state.error = "历史加载失败";
      if (key === chatKey && generation === chatGeneration && menuVisible) renderMenu();
    }).finally(() => {
      state.loadPromise = undefined;
    });
    state.loadPromise = result;
    return result;
  }

  function ensureHistoryState(key) {
    let state = historyStates.get(key);
    if (!state) {
      state = { phase: "loading", entries: [], limit: DEFAULT_LIMIT, error: undefined, migrationFailed: false, loadPromise: undefined };
      historyStates.set(key, state);
      loadChatHistory(key, state);
    } else if (state.phase === "error" && !state.loadPromise) {
      loadChatHistory(key, state);
    }
    return state;
  }

  function updateLimit() {
    const state = currentHistoryState();
    const value = Number(limitInput.value);
    if (!state || state.phase !== "loaded" || !Number.isInteger(value) || value < 1) {
      limitInput.value = String(state?.limit ?? DEFAULT_LIMIT);
      return;
    }
    if (value === limitIntent) return;
    limitIntent = value;
    const version = ++limitRequestVersion;
    const key = chatKey;
    enqueueBridge(async () => {
      const limit = await bridgeMethod("setInputHistoryLimit")(value);
      if (!Number.isInteger(limit) || limit < 1) throw new Error("Invalid input history limit");
      return limit;
    }).then((limit) => {
      for (const cached of historyStates.values()) {
        cached.limit = limit;
        cached.entries = cached.entries.slice(0, limit);
      }
      if (version === limitRequestVersion) limitIntent = limit;
      if (key === chatKey && version === limitRequestVersion) limitInput.value = String(limit);
      if (key === chatKey && menuVisible) renderMenu();
    }).catch(() => {
      const current = historyStates.get(key) ?? state;
      current.error = "历史上限保存失败";
      historyStates.set(key, current);
      if (version === limitRequestVersion) {
        limitIntent = current.limit;
        if (key === chatKey) limitInput.value = String(limitIntent);
      }
      if (key === chatKey && menuVisible) renderMenu();
    });
  }

  function isMentionElement(element) {
    return element.classList?.contains("mention") === true
      || element.getAttribute?.("data-type") === "mention";
  }

  function isAttachmentElement(element) {
    if (["IMG", "VIDEO", "AUDIO", "IFRAME", "OBJECT", "EMBED", "INPUT", "BUTTON"].includes(element.tagName)) return true;
    const type = element.getAttribute("data-type") ?? "";
    return /attachment|file|image|video|audio|upload/i.test(type)
      || /(?:attachment|file-upload)/i.test(typeof element.className === "string" ? element.className : "");
  }

  function captureEditorContent(source = editor) {
    if (!source || !source.isConnected) return { text: "" };
    const clone = source.cloneNode(true);
    clone.querySelectorAll("br.ProseMirror-trailingBreak").forEach((node) => node.remove());
    clone.querySelectorAll("img, video, audio, iframe, object, embed, input, button").forEach((node) => node.remove());
    clone.querySelectorAll("[data-type], [class*='attachment'], [class*='file-upload']").forEach((node) => {
      if (isAttachmentElement(node)) node.remove();
    });
    clone.querySelectorAll('[contenteditable="false"]').forEach((node) => {
      if (!isMentionElement(node)) node.remove();
    });

    let text = "";
    const mentions = [];
    function walk(node) {
      if (node.nodeType === Node.TEXT_NODE) {
        text += node.data;
        return;
      }
      if (node.nodeType !== Node.ELEMENT_NODE) return;
      if (isMentionElement(node) && node.hasAttribute("data-id")) {
        const start = text.length;
        text += node.textContent;
        mentions.push({
          start, end: text.length,
          id: node.getAttribute("data-id"),
          label: node.getAttribute("data-label"),
          char: node.getAttribute("data-mention-suggestion-char"),
        });
        return;
      }
      if (node.tagName === "BR") {
        text += "\n";
        return;
      }
      const block = node !== clone && /^(P|DIV|LI|BLOCKQUOTE|PRE|H[1-6])$/.test(node.tagName);
      if (block && text && !text.endsWith("\n")) text += "\n";
      for (const child of node.childNodes) walk(child);
      if (block && !text.endsWith("\n")) text += "\n";
    }
    walk(clone);
    if (text.endsWith("\n")) text = text.slice(0, -1);
    text = text.replace(/\r\n?/g, "\n");
    if (!text.trim()) text = "";
    return { text, mentions };
  }

  function recordSubmittedContent(content, targetKey = chatKey) {
    if (!targetKey || !content || !content.text.trim()) return;
    const entry = { text: content.text, mentions: content.mentions };
    const generation = chatGeneration;
    const state = ensureHistoryState(targetKey);
    enqueueBridge(async () => normalizeSnapshot(
      await bridgeMethod("appendInputHistory")(targetKey, entry),
    )).then((snapshot) => {
      state.entries = snapshot.entries;
      state.limit = snapshot.limit;
      state.phase = "loaded";
      state.error = undefined;
      if (targetKey === chatKey && generation === chatGeneration && menuVisible) renderMenu();
    }).catch(() => {
      const current = historyStates.get(targetKey) ?? state;
      current.error = "历史保存失败，刚才的输入未保存";
      historyStates.set(targetKey, current);
      if (targetKey === chatKey && generation === chatGeneration && menuVisible) renderMenu();
    });
  }

  function isVisible(element) {
    if (!(element instanceof Element) || !element.isConnected || element.hidden) return false;
    const style = window.getComputedStyle(element);
    return style.display !== "none" && style.visibility !== "hidden" && element.getClientRects().length > 0;
  }

  function higherPrioritySuggestionVisible() {
    const selectors = [
      ".wk-slash-command-menu",
      "[data-octobuddy-command-suggestions-root]",
      ".mention-list",
      ".emoji-suggestion-bar",
    ];
    for (const selector of selectors) {
      for (const element of document.querySelectorAll(selector)) {
        if (root.contains(element) || editor?.contains(element)) continue;
        if (isVisible(element)) return true;
      }
    }
    return false;
  }

  function ensureDomAttached() {
    if (disposed) return;
    if (!root.isConnected) {
      if (menuVisible) hideMenu();
      (document.body ?? document.documentElement).append(root);
    }
    if (!style.isConnected) (document.head ?? document.documentElement).append(style);
  }

  function positionMenu() {
    if (!menuVisible || !editor || !editor.isConnected) return;
    const rect = editor.getBoundingClientRect();
    const viewportWidth = document.documentElement.clientWidth || window.innerWidth;
    const viewportHeight = document.documentElement.clientHeight || window.innerHeight;
    if (rect.width <= 0 && rect.height <= 0) return;
    const width = Math.max(220, Math.min(rect.width, Math.max(220, viewportWidth - 16)));
    root.style.setProperty("width", `${Math.min(width, 520)}px`, "important");
    const height = root.getBoundingClientRect().height;
    const left = Math.max(8, Math.min(rect.left, viewportWidth - width - 8));
    let top = rect.top - height - 8;
    if (top < 8) top = Math.min(rect.bottom + 8, Math.max(8, viewportHeight - height - 8));
    root.style.setProperty("left", `${left}px`, "important");
    root.style.setProperty("top", `${top}px`, "important");
  }

  function setSelection(index) {
    selectedIndex = Math.max(0, Math.min(index, filteredEntries.length - 1));
    const items = list.querySelectorAll("[data-octobuddy-input-history-item]");
    items.forEach((item, itemIndex) => {
      const selected = itemIndex === selectedIndex;
      item.dataset.selected = String(selected);
      item.setAttribute("aria-selected", String(selected));
      if (selected) {
        root.setAttribute("aria-activedescendant", item.id);
        item.scrollIntoView?.({ block: "nearest" });
      }
    });
  }

  function createItem(entry, index) {
    const item = document.createElement("button");
    item.type = "button";
    item.setAttribute("data-octobuddy-input-history-item", "");
    item.setAttribute("role", "option");
    item.id = `octobuddy-input-history-option-${index}`;
    item.dataset.selected = String(index === selectedIndex);
    item.setAttribute("aria-selected", String(index === selectedIndex));

    const preview = document.createElement("span");
    preview.setAttribute("data-octobuddy-input-history-preview", "");
    preview.textContent = entry.text;
    item.append(preview);

    item.addEventListener("pointerdown", (event) => {
      if (event.button !== 0) return;
      event.preventDefault();
      event.stopPropagation();
    });
    item.addEventListener("click", (event) => {
      event.preventDefault();
      event.stopPropagation();
      restoreEntry(entry);
    });
    return item;
  }

  function renderMenu() {
    if (!currentChat || !editor || !editor.isConnected || composing || higherPrioritySuggestionVisible()) {
      hideMenu();
      return;
    }
    const query = captureEditorContent().text.toLocaleLowerCase();
    const state = currentHistoryState();
    const entries = state?.entries ?? [];
    filteredEntries = state?.phase === "loaded"
      ? entries.filter((entry) => !query || entry.text.toLocaleLowerCase().includes(query))
      : [];
    if (selectedIndex >= filteredEntries.length) selectedIndex = Math.max(0, filteredEntries.length - 1);
    list.replaceChildren();
    if (state?.phase === "loading") {
      const loading = document.createElement("div");
      loading.setAttribute("data-octobuddy-input-history-loading", "");
      loading.textContent = "正在加载输入历史…";
      list.append(loading);
    } else {
      for (const message of [
        state?.error,
        state?.migrationFailed ? "旧版历史迁移失败，原数据仍保留" : undefined,
      ]) {
        if (!message) continue;
        const error = document.createElement("div");
        error.setAttribute("data-octobuddy-input-history-error", "");
        error.textContent = message;
        list.append(error);
      }
      if (state?.phase !== "error" && filteredEntries.length === 0 && !state?.error && !state?.migrationFailed) {
        const empty = document.createElement("div");
        empty.setAttribute("data-octobuddy-input-history-empty", "");
        const icon = document.createElement("span");
        icon.setAttribute("data-octobuddy-input-history-empty-icon", "");
        icon.setAttribute("aria-hidden", "true");
        icon.textContent = "↶";
        const message = document.createElement("div");
        const title = document.createElement("div");
        title.setAttribute("data-octobuddy-input-history-empty-title", "");
        title.textContent = entries.length ? "没有找到匹配的记录" : "还没有输入历史";
        const hint = document.createElement("div");
        hint.setAttribute("data-octobuddy-input-history-empty-hint", "");
        hint.textContent = entries.length ? "换个关键词试试" : "发送消息后，记录会出现在这里";
        message.append(title, hint);
        empty.append(icon, message);
        list.append(empty);
      }
    }
    filteredEntries.forEach((entry, index) => list.append(createItem(entry, index)));
    limitInput.value = String(state?.phase === "loaded" ? limitIntent : state?.limit ?? DEFAULT_LIMIT);
    limitInput.disabled = state?.phase !== "loaded";
    root.hidden = false;
    menuVisible = true;
    setSelection(selectedIndex);
    positionMenu();
  }

  function openMenu() {
    if (disposed || composing || !currentChat || !editor || !editor.contains(document.activeElement)) return;
    if (higherPrioritySuggestionVisible()) return;
    selectedIndex = 0;
    renderMenu();
  }

  function hideMenu() {
    menuVisible = false;
    filteredEntries = [];
    selectedIndex = 0;
    root.hidden = true;
    root.removeAttribute("aria-activedescendant");
    list.replaceChildren();
  }

  function scheduleUpdate() {
    if (disposed || updateScheduled) return;
    updateScheduled = true;
    queueMicrotask(() => {
      updateScheduled = false;
      if (disposed || !menuVisible) return;
      if (higherPrioritySuggestionVisible()) {
        hideMenu();
        return;
      }
      renderMenu();
    });
  }

  function firstVisualLineCaret() {
    if (!editor || !editor.isConnected) return false;
    const selection = window.getSelection();
    if (!selection || selection.rangeCount === 0 || !selection.isCollapsed) return false;
    const anchor = selection.anchorNode;
    if (!anchor || !editor.contains(anchor)) return false;

    const caret = selection.getRangeAt(0).cloneRange();
    let caretRect = caret.getBoundingClientRect();
    if (caretRect.height === 0 && anchor.nodeType === Node.TEXT_NODE) {
      if (selection.anchorOffset < anchor.textContent.length) caret.setEnd(anchor, selection.anchorOffset + 1);
      else if (selection.anchorOffset > 0) caret.setStart(anchor, selection.anchorOffset - 1);
      caretRect = caret.getBoundingClientRect();
    }
    if (caretRect.height === 0) {
      const before = document.createRange();
      before.selectNodeContents(editor);
      before.setEnd(anchor, selection.anchorOffset);
      const beforeRects = [...before.getClientRects()].filter((rect) => rect.height > 0);
      if (beforeRects.length > 0) caretRect = beforeRects[beforeRects.length - 1];
    }

    const contents = document.createRange();
    contents.selectNodeContents(editor);
    const rects = [...contents.getClientRects()].filter((rect) => rect.height > 0);
    let firstTop = rects.length > 0 ? Math.min(...rects.map((rect) => rect.top)) : editor.getBoundingClientRect().top;
    const editorStyle = window.getComputedStyle(editor);
    const lineHeight = Number.parseFloat(editorStyle.lineHeight) || 18;
    if (caretRect.height === 0) caretRect = editor.getBoundingClientRect();
    return caretRect.top <= firstTop + Math.max(2, lineHeight * 0.55);
  }

  function restoreEntry(entry) {
    if (!editor || !editor.isConnected || !entry || typeof entry.text !== "string") return;
    const previous = captureEditorContent().text;
    try {
      editor.focus();
    } catch {
      return;
    }

    function selectAll() {
      try {
        document.execCommand("selectAll", false);
        return true;
      } catch {
        return false;
      }
    }

    function insertWholeText(value) {
      if (!selectAll()) return false;
      try {
        return document.execCommand("insertText", false, value);
      } catch {
        return false;
      }
    }

    let inserted;
    if (entry.mentions?.length) {
      const container = document.createElement("div");
      let offset = 0;
      for (const mention of entry.mentions) {
        container.append(document.createTextNode(entry.text.slice(offset, mention.start)));
        const span = document.createElement("span");
        span.setAttribute("data-type", "mention");
        span.setAttribute("data-id", mention.id);
        if (mention.label !== null) span.setAttribute("data-label", mention.label);
        if (mention.char !== null) span.setAttribute("data-mention-suggestion-char", mention.char);
        span.setAttribute("contenteditable", "false");
        span.textContent = entry.text.slice(mention.start, mention.end);
        container.append(span);
        offset = mention.end;
      }
      container.append(document.createTextNode(entry.text.slice(offset)));
      selectAll();
      inserted = document.execCommand("insertHTML", false, container.innerHTML);
    } else {
      inserted = insertWholeText(entry.text);
    }
    if (!inserted || captureEditorContent().text !== entry.text) {
      if (!selectAll()) {
        hideMenu();
        return;
      }
      inserted = true;
      const lines = entry.text.split("\n");
      for (let index = 0; index < lines.length; index += 1) {
        if (lines[index]) {
          try {
            if (!document.execCommand("insertText", false, lines[index])) inserted = false;
          } catch {
            inserted = false;
          }
        }
        if (index < lines.length - 1) {
          try {
            if (!document.execCommand("insertParagraph", false)) inserted = false;
          } catch {
            inserted = false;
          }
        }
      }
    }

    if (!inserted || captureEditorContent().text !== entry.text) {
      if (selectAll()) {
        try {
          document.execCommand("insertText", false, previous);
        } catch {
          // Keep the editor's native content rather than replacing it with generated markup.
        }
      }
      hideMenu();
      return;
    }
    hideMenu();
    scheduleUpdate();
  }

  function clearPendingSubmission() {
    pendingSubmission = undefined;
  }

  function checkPendingSubmission() {
    if (!pendingSubmission || disposed) return;
    if (Date.now() - pendingSubmission.startedAt > SUBMISSION_WINDOW_MS) {
      clearPendingSubmission();
      return;
    }
    if (pendingSubmission.chatKey !== chatKey) return;
    if (!editor || !editor.isConnected) {
      attachEditor(findEditor());
      if (!editor) return;
    }
    const current = captureEditorContent();
    if (current.text === "") {
      const completed = pendingSubmission;
      clearPendingSubmission();
      if (completed.generation === chatGeneration) recordSubmittedContent(completed.content, completed.chatKey);
      return;
    }
    if (current.text !== pendingSubmission.content.text) clearPendingSubmission();
  }

  function beginSubmission() {
    if (!currentChat || !chatKey || !editor || !editor.isConnected || composing
      || higherPrioritySuggestionVisible()) return;
    const content = captureEditorContent();
    if (!content.text.trim()) return;
    if (pendingSubmission && pendingSubmission.chatKey === chatKey
      && pendingSubmission.content.text === content.text
      && Date.now() - pendingSubmission.startedAt <= SUBMISSION_WINDOW_MS) return;
    pendingSubmission = { content, chatKey, generation: chatGeneration, startedAt: Date.now() };
  }

  function handleEditorInput() {
    checkPendingSubmission();
    if (disposed || composing) return;
    scheduleUpdate();
  }

  function handleCompositionStart() {
    composing = true;
    hideMenu();
  }

  function handleCompositionEnd() {
    composing = false;
    scheduleUpdate();
  }

  function handleEditorFocusOut(event) {
    const next = event.relatedTarget;
    if (next instanceof Node && (editor?.contains(next) || root.contains(next))) return;
    queueMicrotask(() => {
      if (disposed || (editor && editor.contains(document.activeElement)) || root.contains(document.activeElement)) return;
      hideMenu();
    });
  }

  function editorWrapper() {
    return editor?.closest(".wk-messageinput-editor") ?? editor;
  }

  function isSubmitButton(button) {
    if (!(button instanceof HTMLElement) || button.disabled || button.getAttribute("aria-disabled") === "true") return false;
    const icon = button.querySelector("svg")?.getAttribute("data-lucide") ?? "";
    const label = [
      button.getAttribute("aria-label"), button.getAttribute("title"),
      button.getAttribute("data-testid"), button.getAttribute("data-test"),
      typeof button.className === "string" ? button.className : "", icon, button.textContent,
    ].filter(Boolean).join(" ");
    if (!/(?:send|submit|发送)/i.test(label)) return false;
    if (editor?.contains(button)) return false;
    const wrapper = editorWrapper();
    let ancestor = wrapper;
    for (let depth = 0; ancestor && depth < 6; depth += 1, ancestor = ancestor.parentElement) {
      if (ancestor.contains(button)) return true;
      if (ancestor.matches?.("form, [role='dialog']")) break;
    }
    return false;
  }

  function handleDocumentKeydown(event) {
    if (disposed || composing || event.isComposing || event.keyCode === 229) return;
    if (event.key === "Escape" && menuVisible && root.contains(event.target)) {
      event.preventDefault();
      event.stopImmediatePropagation();
      hideMenu();
      editor?.focus();
      return;
    }
    if (!editor || !editor.contains(event.target) || !editor.contains(document.activeElement)) return;
    if (higherPrioritySuggestionVisible()) {
      if (menuVisible) hideMenu();
      return;
    }

    if (event.key === "Escape" && menuVisible) {
      event.preventDefault();
      event.stopImmediatePropagation();
      hideMenu();
      return;
    }
    if (menuVisible && (event.key === "ArrowUp" || event.key === "ArrowDown") && filteredEntries.length > 0) {
      event.preventDefault();
      event.stopImmediatePropagation();
      setSelection(selectedIndex + (event.key === "ArrowDown" ? 1 : -1));
      return;
    }
    if (menuVisible && (event.key === "Enter" || event.key === "Tab")
      && !event.shiftKey && !event.altKey && !event.ctrlKey && !event.metaKey
      && filteredEntries[selectedIndex]) {
      event.preventDefault();
      event.stopImmediatePropagation();
      restoreEntry(filteredEntries[selectedIndex]);
      return;
    }

    const plainEnter = event.key === "Enter"
      && !event.shiftKey && !event.altKey && !event.ctrlKey && !event.metaKey;
    if (plainEnter && !event.defaultPrevented) beginSubmission();

    if (event.key === "ArrowUp" && !event.shiftKey && !event.altKey && !event.ctrlKey && !event.metaKey
      && !menuVisible && firstVisualLineCaret()) {
      openMenu();
      if (menuVisible) {
        event.preventDefault();
        event.stopImmediatePropagation();
      }
    }
  }

  function handleDocumentClick(event) {
    if (disposed || !editor || !(event.target instanceof Element) || root.contains(event.target)) return;
    const button = event.target.closest("button, [role='button']");
    if (button && isSubmitButton(button)) beginSubmission();
  }

  function handleDocumentSubmit(event) {
    if (disposed || !editor || !(event.target instanceof Node) || !event.target.contains(editor)) return;
    beginSubmission();
  }

  function handleChatMessage(event) {
    if (disposed || event.source !== window) return;
    const message = event.data;
    if (!message || message.type !== CHAT_MESSAGE_TYPE) return;
    const channelType = message.channelType;
    const channelId = message.channelId;
    const nextChat = (channelType === 1 || channelType === 2 || channelType === 5)
      && typeof channelId === "string" && channelId.length > 0
      ? { channelType, channelId }
      : undefined;
    const nextKey = chatIdentityKey(nextChat);
    if (nextKey === chatKey) return;
    if (pendingSubmission && Date.now() - pendingSubmission.startedAt <= SUBMISSION_WINDOW_MS) {
      const completed = pendingSubmission;
      clearPendingSubmission();
      recordSubmittedContent(completed.content, completed.chatKey);
    } else {
      clearPendingSubmission();
    }
    const previousState = chatKey ? historyStates.get(chatKey) : undefined;
    if (previousState?.phase === "loaded" && !previousState.error && !previousState.migrationFailed) {
      historyStates.delete(chatKey);
    }
    currentChat = nextChat;
    chatKey = nextKey;
    chatGeneration += 1;
    composing = false;
    if (nextKey) ensureHistoryState(nextKey);
    hideMenu();
    scheduleUpdate();
  }

  function handleViewportChange() {
    if (menuVisible) positionMenu();
  }

  function handleDocumentFocusIn(event) {
    if (disposed) return;
    const target = event.target;
    if (!(target instanceof Node)) return;
    if (editor && (editor === target || editor.contains(target))) return;
    if (target instanceof Element) {
      const wrapper = target.closest(".wk-messageinput-editor");
      if (wrapper instanceof HTMLElement) {
        attachEditor(findEditor(wrapper));
        return;
      }
    }
    if (root.contains(target)) return;
    hideMenu();
  }

  function findEditor(wrapper = document.querySelector(".wk-messageinput-editor")) {
    if (!wrapper) return undefined;
    const contentEditable = wrapper.querySelector('[contenteditable="true"]');
    if (contentEditable instanceof HTMLElement) return contentEditable;
    if (wrapper instanceof HTMLElement && wrapper.isContentEditable) return wrapper;
    return undefined;
  }

  function detachEditor() {
    if (!editor) return;
    editor.removeEventListener("input", handleEditorInput);
    editor.removeEventListener("compositionstart", handleCompositionStart);
    editor.removeEventListener("compositionend", handleCompositionEnd);
    editor.removeEventListener("focusout", handleEditorFocusOut);
    editorResizeObserver?.disconnect();
    editorResizeObserver = undefined;
    editor = undefined;
  }

  function attachEditor(next) {
    if (editor === next) return;
    detachEditor();
    if (!next) return;
    editor = next;
    editor.addEventListener("input", handleEditorInput);
    editor.addEventListener("compositionstart", handleCompositionStart);
    editor.addEventListener("compositionend", handleCompositionEnd);
    editor.addEventListener("focusout", handleEditorFocusOut);
    if (typeof ResizeObserver === "function") {
      editorResizeObserver = new ResizeObserver(() => {
        if (menuVisible) positionMenu();
      });
      editorResizeObserver.observe(editor);
    }
  }

  function dispose() {
    if (disposedOnce) return;
    disposedOnce = true;
    disposed = true;
    clearPendingSubmission();
    detachEditor();
    observer?.disconnect();
    document.removeEventListener("keydown", handleDocumentKeydown, true);
    document.removeEventListener("click", handleDocumentClick, true);
    document.removeEventListener("submit", handleDocumentSubmit, true);
    document.removeEventListener("focusin", handleDocumentFocusIn, true);
    window.removeEventListener("message", handleChatMessage);
    window.removeEventListener("resize", handleViewportChange);
    window.removeEventListener("scroll", handleViewportChange, true);
    limitInput.removeEventListener("change", updateLimit);
    limitInput.removeEventListener("blur", updateLimit);
    root.remove();
    style.remove();
    currentChat = undefined;
    chatKey = undefined;
    if (window[INSTANCE_KEY] === instance) delete window[INSTANCE_KEY];
  }

  try {
    limitInput.value = String(DEFAULT_LIMIT);
    limitInput.disabled = true;
    limitInput.addEventListener("change", updateLimit);
    limitInput.addEventListener("blur", updateLimit);
    (document.head ?? document.documentElement).append(style);
    (document.body ?? document.documentElement).append(root);
    window.addEventListener("message", handleChatMessage);
    window.addEventListener("resize", handleViewportChange);
    window.addEventListener("scroll", handleViewportChange, true);
    document.addEventListener("keydown", handleDocumentKeydown, true);
    document.addEventListener("click", handleDocumentClick, true);
    document.addEventListener("submit", handleDocumentSubmit, true);
    document.addEventListener("focusin", handleDocumentFocusIn, true);

    observer = new MutationObserver(() => {
      if (disposed) return;
      ensureDomAttached();
      if (!editor || !editor.isConnected) {
        attachEditor(findEditor());
      }
      checkPendingSubmission();
    });
    observer.observe(document.documentElement, { childList: true, characterData: true, subtree: true });

    attachEditor(findEditor());
    instance = { active: true, dispose };
    Object.defineProperty(window, INSTANCE_KEY, { configurable: true, value: instance });
    capability.onDispose(dispose);
    window.postMessage({ type: CHAT_REQUEST_TYPE }, "*");
  } catch (error) {
    dispose();
    throw error;
  }
})();
