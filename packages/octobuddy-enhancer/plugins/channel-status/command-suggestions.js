(() => {
  const INSTANCE_KEY = "__octobuddyCommandSuggestionsPluginInstanceV1";
  const CHAT_MESSAGE_TYPE = "octobuddy-enhancer:chat";
  const CHAT_REQUEST_TYPE = "octobuddy-enhancer:chat-request";
  const REFRESH_INTERVAL_MS = 60_000;
  const existing = window[INSTANCE_KEY];
  if (existing && existing.active) return;

  if (window.top !== window || window.location.protocol !== "file:") return;
  if (!window.location.pathname.endsWith("/Contents/Resources/communication-renderer/index.html")) return;

  const capability = window.octoEnhancer;
  if (
    !capability ||
    typeof capability.getChatCommands !== "function" ||
    typeof capability.onDispose !== "function"
  ) {
    return;
  }

  const SERVICE_MESSAGES = {
    offline: "服务暂时无法连接，命令目录不可用。",
    unconfigured: "命令接口尚未配置。",
    unauthorized: "命令接口认证失败。",
    invalid: "命令接口响应无效。",
  };
  const SERVICE_LABELS = {
    offline: "离线",
    unconfigured: "未配置",
    unauthorized: "认证失败",
    invalid: "配置或协议错误",
  };

  let disposed = false;
  let editor;
  let editorResizeObserver;
  let currentChat;
  let chatKey;
  let chatGeneration = 0;
  let resolvedTarget;
  let targetState = "idle";
  let targetError;
  let targetAttemptedAt = 0;
  let catalog;
  let catalogState = "idle";
  let catalogError;
  let catalogFetchedAt = 0;
  let activeRequest;
  let requestSequence = 0;
  let refreshTimer = 0;
  let composing = false;
  let menuVisible = false;
  let context;
  let candidates = [];
  let selectedIndex = 0;
  let dismissedText;
  let updateScheduled = false;
  let instance;
  const suppressedNativeMenus = new Set();

  const style = document.createElement("style");
  style.textContent = `
    [data-octobuddy-command-suggestions-root] {
      all: initial !important;
      position: fixed !important;
      z-index: 2147483647 !important;
      display: block !important;
      background: #ffffff !important;
      border: 1px solid rgba(31, 35, 41, 0.14) !important;
      border-radius: 8px !important;
      box-shadow: 0 8px 24px rgba(31, 35, 41, 0.14) !important;
      color: #1f2329 !important;
      box-sizing: border-box !important;
      font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", "PingFang SC", sans-serif !important;
      font-size: 13px !important;
      line-height: 1.4 !important;
      max-height: min(320px, calc(100vh - 24px)) !important;
      overflow-y: auto !important;
      padding: 4px 0 !important;
      scrollbar-width: thin !important;
    }
    [data-octobuddy-command-suggestions-root][hidden] {
      display: none !important;
    }
    [data-octobuddy-command-suggestions-root] *,
    [data-octobuddy-command-suggestions-root] *::before,
    [data-octobuddy-command-suggestions-root] *::after {
      box-sizing: border-box !important;
    }
    [data-octobuddy-command-suggestions-item] {
      cursor: pointer !important;
      display: block !important;
      margin: 2px 4px !important;
      min-width: 0 !important;
      padding: 7px 10px !important;
      border-radius: 5px !important;
      color: #1f2329 !important;
      transition: background-color 80ms ease !important;
    }
    [data-octobuddy-command-suggestions-item]:hover {
      background: #f2f3f5 !important;
    }
    [data-octobuddy-command-suggestions-item][data-selected="true"] {
      background: #6c56c9 !important;
      color: #ffffff !important;
    }
    [data-octobuddy-command-suggestions-title-row] {
      align-items: baseline !important;
      display: flex !important;
      gap: 8px !important;
      min-width: 0 !important;
    }
    [data-octobuddy-command-suggestions-title] {
      color: inherit !important;
      flex: 0 1 auto !important;
      font-family: ui-monospace, SFMono-Regular, Menlo, Consolas, monospace !important;
      font-size: 13px !important;
      font-weight: 600 !important;
      min-width: 0 !important;
      overflow-wrap: anywhere !important;
      white-space: nowrap !important;
    }
    [data-octobuddy-command-suggestions-hint] {
      color: #8f959e !important;
      flex: 0 1 auto !important;
      font-size: 12px !important;
      min-width: 0 !important;
      overflow-wrap: anywhere !important;
    }
    [data-octobuddy-command-suggestions-item][data-selected="true"] [data-octobuddy-command-suggestions-hint] {
      color: rgba(255, 255, 255, 0.76) !important;
    }
    [data-octobuddy-command-suggestions-source] {
      color: #8f959e !important;
      flex: none !important;
      font-size: 11px !important;
      margin-left: auto !important;
      text-transform: uppercase !important;
    }
    [data-octobuddy-command-suggestions-item][data-selected="true"] [data-octobuddy-command-suggestions-source] {
      color: rgba(255, 255, 255, 0.7) !important;
    }
    [data-octobuddy-command-suggestions-description] {
      color: #646a73 !important;
      display: block !important;
      font-size: 12px !important;
      margin-top: 3px !important;
      overflow-wrap: anywhere !important;
    }
    [data-octobuddy-command-suggestions-item][data-selected="true"] [data-octobuddy-command-suggestions-description] {
      color: rgba(255, 255, 255, 0.82) !important;
    }
    [data-octobuddy-command-suggestions-aliases] {
      color: #8f959e !important;
      display: block !important;
      font-size: 11px !important;
      margin-top: 2px !important;
      overflow-wrap: anywhere !important;
    }
    [data-octobuddy-command-suggestions-item][data-selected="true"] [data-octobuddy-command-suggestions-aliases] {
      color: rgba(255, 255, 255, 0.7) !important;
    }
    [data-octobuddy-command-suggestions-note] {
      color: #646a73 !important;
      display: block !important;
      font-size: 12px !important;
      margin: 4px 0 !important;
      padding: 7px 14px !important;
      overflow-wrap: anywhere !important;
    }
    [data-octobuddy-command-suggestions-note][hidden] {
      display: none !important;
    }
    .wk-slash-command-menu[data-octobuddy-command-suggestions-native-hidden] {
      display: none !important;
    }
    @media (max-width: 480px), (max-height: 560px) {
      [data-octobuddy-command-suggestions-root] {
        max-height: min(40vh, 280px) !important;
      }
      [data-octobuddy-command-suggestions-item] {
        padding: 7px 9px !important;
      }
    }
    @media (prefers-reduced-motion: reduce) {
      [data-octobuddy-command-suggestions-item] {
        transition-duration: 0s !important;
      }
    }
  `;

  const root = document.createElement("div");
  root.setAttribute("data-octobuddy-command-suggestions-root", "");
  root.setAttribute("role", "listbox");
  root.setAttribute("aria-label", "命令建议");
  root.dataset.state = "idle";
  root.hidden = true;

  const list = document.createElement("div");
  list.setAttribute("data-octobuddy-command-suggestions-list", "");

  const note = document.createElement("div");
  note.setAttribute("data-octobuddy-command-suggestions-note", "");
  note.hidden = true;

  root.append(list, note);

  function sameTarget(left, right) {
    return Boolean(
      left && right &&
      left.channelType === right.channelType &&
      left.channelId === right.channelId
    );
  }

  function ensureDomAttached() {
    if (disposed) return;
    if (!root.isConnected) {
      // A rebuilt page discards the old composer and menu; discard its state before restoring the root.
      if (menuVisible) hideMenu({ cancel: true });
      (document.body ?? document.documentElement).append(root);
    }
    if (!style.isConnected) (document.head ?? document.documentElement).append(style);
  }

  function chatIdentityKey(chat) {
    return chat ? `${chat.channelType}:${chat.channelId}` : undefined;
  }

  function isMentionElement(element) {
    return element.classList?.contains("mention") === true
      || element.getAttribute?.("data-type") === "mention";
  }

  function isBlockElement(element) {
    return element.tagName === "P"
      || element.tagName === "DIV"
      || element.tagName === "LI"
      || element.tagName === "BLOCKQUOTE"
      || element.tagName === "PRE"
      || /^H[1-6]$/.test(element.tagName);
  }

  function collectEditorText() {
    const segments = [];
    let text = "";
    let offset = 0;

    function push(node, value, mention) {
      if (value.length === 0) return;
      segments.push({ node, start: offset, end: offset + value.length, mention });
      text += value;
      offset += value.length;
    }

    function newline() {
      if (text.length === 0 || text.endsWith("\n")) return;
      push(null, "\n", false);
    }

    function walk(node, insideMention) {
      if (node.nodeType === Node.TEXT_NODE) {
        push(node, node.data, insideMention);
        return;
      }
      if (node.nodeType !== Node.ELEMENT_NODE) return;
      const element = node;
      if (element.tagName === "BR") {
        newline();
        return;
      }
      const block = isBlockElement(element);
      const mention = insideMention || isMentionElement(element);
      if (block) newline();
      for (const child of element.childNodes) walk(child, mention);
      if (block) newline();
    }

    if (editor) walk(editor, false);
    return { segments, text };
  }

  function positionAt(segments, offset) {
    for (const segment of segments) {
      if (segment.node === null) continue;
      if (offset >= segment.start && offset <= segment.end) {
        return { node: segment.node, offset: offset - segment.start };
      }
    }
    for (let index = segments.length - 1; index >= 0; index -= 1) {
      const segment = segments[index];
      if (segment.node !== null) return { node: segment.node, offset: segment.end - segment.start };
    }
    return undefined;
  }

  function segmentAt(segments, offset) {
    for (const segment of segments) {
      if (offset >= segment.start && offset < segment.end) return segment;
      if (segment.node === null && offset === segment.start) return segment;
    }
    return undefined;
  }

  function currentSelection() {
    if (!editor || !editor.isConnected) return undefined;
    const selection = window.getSelection();
    if (!selection || selection.rangeCount === 0 || !selection.isCollapsed) return undefined;
    const node = selection.anchorNode;
    if (!node || !editor.contains(node)) return undefined;
    if (node.nodeType !== Node.TEXT_NODE) return undefined;

    const snapshot = collectEditorText();
    let offset;
    for (const segment of snapshot.segments) {
      if (segment.node === node) {
        offset = segment.start + selection.anchorOffset;
        break;
      }
    }
    if (offset === undefined) return undefined;
    return { ...snapshot, offset };
  }

  function normalizedAlias(alias) {
    return alias.replace(/^\/+/, "").toLowerCase();
  }

  function findExactCommand(name) {
    if (!catalog) return undefined;
    const wanted = name.toLowerCase();
    let found;
    for (const command of catalog.commands) {
      if (
        command.name.toLowerCase() !== wanted
        && !(command.aliases ?? []).some((alias) => normalizedAlias(alias) === wanted)
      ) continue;
      if (found === undefined || (found.source === "omp" && command.source === "channel")) found = command;
    }
    return found;
  }

  function commandMatches(command, query) {
    const wanted = query.toLowerCase();
    return command.name.toLowerCase().startsWith(wanted)
      || (command.aliases ?? []).some((alias) => normalizedAlias(alias).startsWith(wanted));
  }

  function isMentionBoundary(segments, offset) {
    const segment = segmentAt(segments, offset);
    return segment !== undefined && segment.mention && segment.start === offset;
  }

  function parseCommandContext(selection) {
    const { segments, text, offset } = selection;
    const caretSegment = segmentAt(segments, offset);
    if (caretSegment?.mention && offset > caretSegment.start) return undefined;
    const before = text.slice(0, offset);
    const lineStart = before.lastIndexOf("\n") + 1;

    let index = lineStart;
    while (index < before.length) {
      const segment = segmentAt(segments, index);
      if (segment && segment.mention && index === segment.start) {
        index = segment.end;
        continue;
      }
      if (/\s/.test(before[index])) {
        index += 1;
        continue;
      }
      break;
    }

    if (before[index] !== "/") return undefined;
    const commandStart = index;
    let nameEnd = commandStart + 1;
    while (nameEnd < before.length && !/\s/.test(before[nameEnd]) && !isMentionBoundary(segments, nameEnd)) {
      nameEnd += 1;
    }
    const name = before.slice(commandStart + 1, nameEnd);

    let fullNameEnd = nameEnd;
    while (fullNameEnd < text.length && !/\s/.test(text[fullNameEnd]) && !isMentionBoundary(segments, fullNameEnd)) {
      fullNameEnd += 1;
    }

    const exactCommand = findExactCommand(name);
    const afterNameBefore = before.slice(fullNameEnd);
    const subMode = exactCommand !== undefined
      && afterNameBefore.length > 0
      && /\s/.test(afterNameBefore[0]);

    const leadingWhitespace = afterNameBefore.length - afterNameBefore.replace(/^\s+/, "").length;
    const subStart = fullNameEnd + leadingWhitespace;
    if (subMode && isMentionBoundary(segments, subStart)) return undefined;
    const subTyped = afterNameBefore.replace(/^\s+/, "").trimEnd();
    const subQuery = subTyped.replace(/\s+/g, " ");
    const subTypedEnd = subStart + subTyped.length;

    return {
      commandStart,
      name,
      nameEnd: fullNameEnd,
      exactCommand,
      subMode,
      subStart,
      subTypedEnd,
      subQuery,
      hint: exactCommand?.hint,
    };
  }

  function filteredCommands(query) {
    if (!catalog) return [];
    const chosen = new Map();
    for (const command of catalog.commands) {
      const key = command.name.toLowerCase();
      const existing = chosen.get(key);
      if (existing === undefined) {
        chosen.set(key, command);
        continue;
      }
      // Channel routing remains authoritative even if the server lists the OMP duplicate first.
      if (existing.source === "omp" && command.source === "channel") chosen.set(key, command);
    }
    const matches = [];
    for (const command of chosen.values()) {
      if (commandMatches(command, query)) matches.push(command);
    }
    return matches;
  }

  function computeCandidates() {
    if (!context || !catalog) return [];
    if (context.subMode) {
      if (context.exactCommand === undefined) return [];
      const query = context.subQuery.toLowerCase();
      return (context.exactCommand.subcommands ?? [])
        .filter((subcommand) => subcommand.name.toLowerCase().startsWith(query))
        .map((subcommand) => ({
          kind: "subcommand",
          name: subcommand.name,
          description: subcommand.description,
          hint: subcommand.hint,
          command: context.exactCommand,
        }));
    }
    return filteredCommands(context.name).map((command) => ({
      kind: "command",
      name: `/${command.name}`,
      description: command.description,
      hint: command.hint,
      aliases: command.aliases,
      command,
    }));
  }

  function noteText() {
    const notes = [];
    if (targetState === "loading") notes.push("正在识别当前聊天…");
    if (targetState === "error") notes.push(targetError ?? "无法识别当前聊天。");
    if (targetState === "ready") {
      if (catalogState === "loading") notes.push("正在读取命令…");
      if (catalogState === "refreshing") notes.push("正在更新命令目录…");
      if (catalogState === "stale") notes.push("命令目录暂时无法更新，显示上次结果。");
      if (catalogState === "error") notes.push(SERVICE_MESSAGES[catalogError] ?? "命令目录暂不可用。");
      if (catalog && catalog.omp === "unavailable") notes.push("OMP 命令不可用，仅显示 channel 命令。");
      if (catalogState === "ready" && candidates.length === 0) {
        if (context?.subMode && context.hint) notes.push(`参数：${context.hint}`);
        else if (context?.subMode) notes.push("没有匹配的子命令。");
        else notes.push("没有匹配的命令。");
      }
      if (catalogState === "stale" && candidates.length === 0 && context?.hint) notes.push(`参数：${context.hint}`);
    }
    return notes.join(" ");
  }

  function applySelection() {
    const items = list.querySelectorAll("[data-octobuddy-command-suggestions-item]");
    items.forEach((item, index) => {
      const selected = index === selectedIndex;
      item.dataset.selected = String(selected);
      item.setAttribute("aria-selected", String(selected));
      if (selected) {
        root.setAttribute("aria-activedescendant", item.id);
        item.scrollIntoView?.({ block: "nearest" });
      }
    });
  }

  function createItem(candidate, index) {
    const item = document.createElement("div");
    item.setAttribute("data-octobuddy-command-suggestions-item", "");
    item.setAttribute("role", "option");
    item.id = `octobuddy-command-option-${index}`;
    item.dataset.selected = String(index === selectedIndex);
    item.setAttribute("aria-selected", String(index === selectedIndex));
    item.dataset.commandName = candidate.name;

    const titleRow = document.createElement("div");
    titleRow.setAttribute("data-octobuddy-command-suggestions-title-row", "");

    const title = document.createElement("span");
    title.setAttribute("data-octobuddy-command-suggestions-title", "");
    title.textContent = candidate.name;
    titleRow.append(title);

    if (candidate.hint) {
      const hint = document.createElement("span");
      hint.setAttribute("data-octobuddy-command-suggestions-hint", "");
      hint.textContent = candidate.hint;
      titleRow.append(hint);
    }

    if (candidate.kind === "command") {
      const source = document.createElement("span");
      source.setAttribute("data-octobuddy-command-suggestions-source", "");
      source.textContent = candidate.command.source;
      titleRow.append(source);
    }
    item.append(titleRow);

    if (candidate.description) {
      const description = document.createElement("span");
      description.setAttribute("data-octobuddy-command-suggestions-description", "");
      description.textContent = candidate.description;
      item.append(description);
    }

    if (candidate.aliases && candidate.aliases.length > 0) {
      const aliases = document.createElement("span");
      aliases.setAttribute("data-octobuddy-command-suggestions-aliases", "");
      aliases.textContent = `别名：${candidate.aliases.map((alias) => `/${alias.replace(/^\/+/, "")}`).join(" ")}`;
      item.append(aliases);
    }

    const activate = (event) => {
      if (event.button !== 0) return;
      event.preventDefault();
      event.stopPropagation();
      complete(candidate);
    };
    item.addEventListener("pointerdown", activate);
    item.addEventListener("mousedown", activate);
    item.addEventListener("mouseenter", () => {
      selectedIndex = index;
      applySelection();
    });
    item.addEventListener("click", (event) => {
      event.preventDefault();
      event.stopPropagation();
      complete(candidate);
    });

    return item;
  }

  function renderMenu() {
    candidates = computeCandidates();
    if (selectedIndex >= candidates.length) selectedIndex = Math.max(0, candidates.length - 1);

    list.replaceChildren();
    if (candidates.length > 0) {
      candidates.forEach((candidate, index) => list.append(createItem(candidate, index)));
    }

    const message = noteText();
    note.textContent = message;
    note.hidden = message.length === 0;

    root.dataset.state = catalogState;
    root.hidden = false;
    menuVisible = true;
    applySelection();
    positionMenu();
    if (candidates.length > 0) suppressNativeMenus();
    else restoreNativeMenus();
    scheduleRefresh();
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

  function suppressNativeMenus() {
    if (!editor) return;
    for (const menu of document.querySelectorAll(".wk-slash-command-menu")) {
      menu.setAttribute("data-octobuddy-command-suggestions-native-hidden", "");
      suppressedNativeMenus.add(menu);
    }
  }

  function restoreNativeMenus() {
    for (const menu of suppressedNativeMenus) menu.removeAttribute("data-octobuddy-command-suggestions-native-hidden");
    suppressedNativeMenus.clear();
    for (const menu of document.querySelectorAll("[data-octobuddy-command-suggestions-native-hidden]")) {
      menu.removeAttribute("data-octobuddy-command-suggestions-native-hidden");
    }
  }

  function clearRefreshTimer() {
    if (!refreshTimer) return;
    window.clearTimeout(refreshTimer);
    refreshTimer = 0;
  }

  function scheduleRefresh() {
    clearRefreshTimer();
    if (disposed || !menuVisible) return;
    refreshTimer = window.setTimeout(() => {
      refreshTimer = 0;
      if (disposed || !menuVisible) return;
      loadCommands(true);
    }, REFRESH_INTERVAL_MS);
  }

  function cancelActiveRequest() {
    if (!activeRequest) return;
    const requestId = activeRequest.id;
    activeRequest = undefined;
    requestSequence += 1;
    try {
      const canceled = capability.cancelChatCommands?.(requestId);
      if (canceled && typeof canceled.catch === "function") canceled.catch(() => undefined);
    } catch {
      // Cancellation is best-effort; the generation guard still discards the result.
    }
  }

  function hideMenu({ dismiss = false, cancel = true } = {}) {
    if (dismiss && editor) dismissedText = collectEditorText().text;
    clearRefreshTimer();
    if (cancel) cancelActiveRequest();
    menuVisible = false;
    context = undefined;
    candidates = [];
    selectedIndex = 0;
    root.hidden = true;
    list.replaceChildren();
    note.textContent = "";
    note.hidden = true;
    restoreNativeMenus();
  }

  function replaceTextRange(start, end, replacement) {
    if (!editor) return false;
    const snapshot = collectEditorText();
    const startPosition = positionAt(snapshot.segments, start);
    const endPosition = positionAt(snapshot.segments, end);
    if (!startPosition || !endPosition) return false;

    const range = document.createRange();
    try {
      range.setStart(startPosition.node, startPosition.offset);
      range.setEnd(endPosition.node, endPosition.offset);
    } catch {
      return false;
    }
    const selection = window.getSelection();
    if (!selection) return false;
    selection.removeAllRanges();
    selection.addRange(range);

    let inserted = false;
    try {
      inserted = document.execCommand("insertText", false, replacement);
    } catch {
      inserted = false;
    }
    if (!inserted) return false;

    const expected = snapshot.text.slice(0, start) + replacement + snapshot.text.slice(end);
    const after = collectEditorText();
    if (after.text !== expected) return false;

    const caret = positionAt(after.segments, start + replacement.length);
    if (caret) {
      const nextRange = document.createRange();
      nextRange.setStart(caret.node, caret.offset);
      nextRange.collapse(true);
      selection.removeAllRanges();
      selection.addRange(nextRange);
    }
    return true;
  }

  function startsWithName(value, name) {
    return value.length >= name.length
      && value.slice(0, name.length).toLowerCase() === name.toLowerCase();
  }

  function commonPrefixLength(value, name) {
    const limit = Math.min(value.length, name.length);
    let index = 0;
    while (index < limit && value[index].toLowerCase() === name[index].toLowerCase()) index += 1;
    // A trailing separator belongs to the following arguments, not to the completed name.
    while (index > 0 && /\s/.test(value[index - 1])) index -= 1;
    return index;
  }

  function subcommandReplacementEnd(candidate) {
    if (!context || !editor || candidate.kind !== "subcommand") return undefined;
    const snapshot = collectEditorText();
    const tail = snapshot.text.slice(context.subStart);

    // An already complete candidate names the current subcommand; preserve arguments after it.
    if (startsWithName(tail, candidate.name) && (tail.length === candidate.name.length || /\s/.test(tail[candidate.name.length]))) {
      return context.subStart + candidate.name.length;
    }

    // When switching between complete subcommands, replace the complete old name rather than only
    // the characters that happen to match the new candidate. Only a name that is at least as long
    // as the pending prefix can be the old name; a shorter name may merely prefix a longer one the
    // user is still typing (`memory` vs `memory mm list`).
    const typedLength = context.subTypedEnd - context.subStart;
    let existingLength = 0;
    for (const subcommand of context.exactCommand?.subcommands ?? []) {
      if (
        subcommand.name.length >= typedLength &&
        subcommand.name.length > existingLength &&
        startsWithName(tail, subcommand.name)
      ) {
        existingLength = subcommand.name.length;
      }
    }

    // Otherwise replace the pending fragment and any already-typed suffix that continues the
    // selected candidate. Text beyond that prefix stays in place as arguments.
    let replacementLength = Math.max(
      typedLength,
      existingLength,
      commonPrefixLength(tail, candidate.name),
    );
    while (replacementLength < tail.length && !/\s/.test(tail[replacementLength])
      && !isMentionBoundary(snapshot.segments, context.subStart + replacementLength)) replacementLength += 1;
    return context.subStart + replacementLength;
  }

  function complete(candidate) {
    if (!context || !editor) return;
    const start = candidate.kind === "command" ? context.commandStart : context.subStart;
    const end = candidate.kind === "command" ? context.nameEnd : subcommandReplacementEnd(candidate);
    if (end === undefined || !replaceTextRange(start, end, candidate.name)) return;

    dismissedText = collectEditorText().text;
    hideMenu({ cancel: true });
    try {
      editor.focus();
    } catch {
      // Focus restoration is best-effort after a valid edit.
    }
  }

  function updateFromEditor() {
    if (disposed) return;
    ensureDomAttached();
    if (!currentChat) {
      hideMenu();
      return;
    }
    if (!editor || !editor.isConnected) {
      attachEditor(findEditor());
      if (!editor) {
        hideMenu();
        return;
      }
    }
    if (!editor.contains(document.activeElement)) {
      hideMenu();
      return;
    }
    if (composing) return;

    const selection = currentSelection();
    if (!selection) {
      hideMenu();
      return;
    }
    if (dismissedText !== undefined && selection.text === dismissedText) return;
    dismissedText = undefined;

    context = parseCommandContext(selection);
    if (!context) {
      hideMenu();
      return;
    }

    targetState = targetState === "idle" || (targetState === "error" && Date.now() - targetAttemptedAt > 5_000)
      ? "idle"
      : targetState;
    if (!resolvedTarget) {
      resolveCommandsTarget();
      if (!resolvedTarget) {
        catalogState = "idle";
        candidates = [];
        renderMenu();
        return;
      }
    }

    loadCommands(false);
    if (catalogState === "not_found") {
      hideMenu({ cancel: false });
      return;
    }
    renderMenu();
  }

  function scheduleUpdate() {
    if (disposed || updateScheduled) return;
    updateScheduled = true;
    queueMicrotask(() => {
      updateScheduled = false;
      if (!disposed) updateFromEditor();
    });
  }

  async function loadPrivateTarget() {
    const chat = currentChat;
    if (!chat || chat.channelType !== 1) return;
    const generation = chatGeneration;
    targetState = "loading";
    targetAttemptedAt = Date.now();
    let result;
    try {
      if (typeof capability.getChannelStatus !== "function") {
        targetState = "error";
        targetError = "无法识别当前私聊。";
        scheduleUpdate();
        return;
      }
      result = await capability.getChannelStatus();
    } catch {
      result = { state: "offline" };
    }
    if (disposed || generation !== chatGeneration || !currentChat || currentChat.channelType !== 1) return;

    const snapshot = result?.state === "online" ? result.snapshot : undefined;
    if (snapshot && snapshot.botUid === currentChat.channelId && typeof snapshot.ownerUid === "string" && snapshot.ownerUid) {
      resolvedTarget = { channelType: 1, channelId: snapshot.ownerUid };
      targetState = "ready";
      targetError = undefined;
      scheduleUpdate();
      return;
    }

    targetState = "error";
    targetError = !snapshot
      ? `服务${SERVICE_LABELS[result?.state] ?? "不可用"}，无法识别当前私聊。`
      : snapshot.botUid !== currentChat.channelId
        ? "该私聊不是当前 Bot 会话。"
        : "无法识别当前私聊。";
    scheduleUpdate();
  }

  function resolveCommandsTarget() {
    if (!currentChat) return undefined;
    if (currentChat.channelType !== 1) {
      resolvedTarget = currentChat;
      targetState = "ready";
      targetError = undefined;
      return resolvedTarget;
    }
    if (resolvedTarget) return resolvedTarget;
    if (targetState === "loading") return undefined;
    if (targetState === "ready" && resolvedTarget) return resolvedTarget;
    if (targetState === "error" && Date.now() - targetAttemptedAt <= 5_000) return undefined;
    if (typeof capability.getChannelStatus !== "function") {
      targetState = "error";
      targetError = "无法识别当前私聊。";
      return undefined;
    }
    void loadPrivateTarget();
    return undefined;
  }

  async function loadCommands(force) {
    if (disposed || !currentChat || !context || !resolvedTarget) return;
    const target = resolvedTarget;
    const generation = chatGeneration;
    if (activeRequest && sameTarget(activeRequest.target, target)) return;
    // Throttle failed catalogs too; otherwise every keystroke would retry an unavailable service.
    if (!force && catalogFetchedAt !== 0 && Date.now() - catalogFetchedAt < REFRESH_INTERVAL_MS) return;
    if (activeRequest) cancelActiveRequest();

    const requestId = ++requestSequence;
    activeRequest = { id: requestId, target, generation };
    catalogState = catalog ? "refreshing" : "loading";
    catalogError = undefined;
    renderMenu();

    let result;
    try {
      result = await capability.getChatCommands(target, requestId);
    } catch {
      result = { state: "offline" };
    }
    if (disposed || generation !== chatGeneration || !currentChat
      || !sameTarget(target, resolvedTarget) || activeRequest?.id !== requestId) {
      return;
    }
    activeRequest = undefined;

    if (result?.state === "not_found") {
      catalog = undefined;
      catalogFetchedAt = Date.now();
      catalogState = "not_found";
      catalogError = undefined;
      scheduleUpdate();
      return;
    }

    if (result?.state === "online") {
      catalog = result.snapshot;
      catalogFetchedAt = Date.now();
      catalogState = "ready";
      catalogError = undefined;
    } else if (result?.lastSuccess) {
      catalog = result.lastSuccess;
      catalogFetchedAt = Date.now();
      catalogState = "stale";
      catalogError = result.state;
    } else {
      catalog = undefined;
      catalogFetchedAt = Date.now();
      catalogState = "error";
      catalogError = result?.state ?? "offline";
    }
    scheduleUpdate();
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
    editor.removeEventListener("input", handleInput);
    editor.removeEventListener("compositionstart", handleCompositionStart);
    editor.removeEventListener("compositionend", handleCompositionEnd);
    editor.removeEventListener("focusin", handleEditorFocusIn);
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
    editor.addEventListener("input", handleInput);
    editor.addEventListener("compositionstart", handleCompositionStart);
    editor.addEventListener("compositionend", handleCompositionEnd);
    editor.addEventListener("focusin", handleEditorFocusIn);
    editor.addEventListener("focusout", handleEditorFocusOut);
    if (typeof ResizeObserver === "function") {
      editorResizeObserver = new ResizeObserver(() => {
        if (menuVisible) positionMenu();
      });
      editorResizeObserver.observe(editor);
    }
  }

  function handleInput() {
    if (disposed || composing) return;
    scheduleUpdate();
  }

  function handleCompositionStart() {
    composing = true;
  }

  function handleCompositionEnd() {
    composing = false;
    scheduleUpdate();
  }

  function handleEditorFocusIn() {
    scheduleUpdate();
  }

  function handleEditorFocusOut(event) {
    const next = event.relatedTarget;
    if (next instanceof Node && (editor?.contains(next) || root.contains(next))) return;
    queueMicrotask(() => {
      if (disposed) return;
      if (editor && editor.contains(document.activeElement)) return;
      hideMenu();
    });
  }

  function handleDocumentKeydown(event) {
    if (disposed || composing || event.isComposing || event.keyCode === 229) return;
    if (!menuVisible || !editor || !editor.contains(event.target) || !editor.contains(document.activeElement)) return;

    if (event.key === "Escape") {
      event.preventDefault();
      event.stopImmediatePropagation();
      hideMenu({ dismiss: true });
      return;
    }

    if (candidates.length === 0) return;

    if (event.key === "ArrowDown" || event.key === "ArrowUp") {
      event.preventDefault();
      event.stopImmediatePropagation();
      const direction = event.key === "ArrowDown" ? 1 : -1;
      selectedIndex = (selectedIndex + direction + candidates.length) % candidates.length;
      applySelection();
      return;
    }

    if (
      (event.key === "Enter" || event.key === "Tab")
      && !event.shiftKey && !event.altKey && !event.ctrlKey && !event.metaKey
      && candidates[selectedIndex]
    ) {
      event.preventDefault();
      event.stopImmediatePropagation();
      complete(candidates[selectedIndex]);
    }
  }

  function handleSelectionChange() {
    if (disposed || composing) return;
    const selection = window.getSelection();
    if (!selection || !editor || !editor.contains(selection.anchorNode)) return;
    scheduleUpdate();
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

    currentChat = nextChat;
    chatKey = nextKey;
    chatGeneration += 1;
    resolvedTarget = undefined;
    targetState = "idle";
    targetError = undefined;
    targetAttemptedAt = 0;
    catalog = undefined;
    catalogState = "idle";
    catalogError = undefined;
    catalogFetchedAt = 0;
    dismissedText = undefined;
    composing = false;
    hideMenu({ cancel: true });
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
        scheduleUpdate();
        return;
      }
    }
    if (target instanceof Node && root.contains(target)) return;
    hideMenu();
  }

  let observer;
  let disposedOnce = false;

  function dispose() {
    if (disposedOnce) return;
    disposedOnce = true;
    disposed = true;
    const requestId = activeRequest?.id;
    activeRequest = undefined;
    requestSequence += 1;
    if (requestId !== undefined) {
      try {
        const canceled = capability.cancelChatCommands?.(requestId);
        if (canceled && typeof canceled.catch === "function") canceled.catch(() => undefined);
      } catch {
        // Best-effort cancellation during plugin disposal.
      }
    }
    clearRefreshTimer();
    detachEditor();
    observer?.disconnect();
    document.removeEventListener("keydown", handleDocumentKeydown, true);
    document.removeEventListener("selectionchange", handleSelectionChange);
    document.removeEventListener("focusin", handleDocumentFocusIn, true);
    window.removeEventListener("message", handleChatMessage);
    window.removeEventListener("resize", handleViewportChange);
    window.removeEventListener("scroll", handleViewportChange, true);
    restoreNativeMenus();
    list.replaceChildren();
    note.replaceChildren();
    root.remove();
    style.remove();
    currentChat = undefined;
    resolvedTarget = undefined;
    catalog = undefined;
    context = undefined;
    candidates = [];
    if (window[INSTANCE_KEY] === instance) delete window[INSTANCE_KEY];
  }

  try {
    (document.head ?? document.documentElement).append(style);
    (document.body ?? document.documentElement).append(root);
    window.addEventListener("message", handleChatMessage);
    window.addEventListener("resize", handleViewportChange);
    window.addEventListener("scroll", handleViewportChange, true);
    document.addEventListener("keydown", handleDocumentKeydown, true);
    document.addEventListener("selectionchange", handleSelectionChange);
    document.addEventListener("focusin", handleDocumentFocusIn, true);

    observer = new MutationObserver(() => {
      if (disposed) return;
      ensureDomAttached();
      if (!editor || !editor.isConnected) {
        attachEditor(findEditor());
        scheduleUpdate();
        return;
      }
      if (menuVisible && candidates.length > 0) suppressNativeMenus();
    });
    observer.observe(document.documentElement, { childList: true, subtree: true });

    attachEditor(findEditor());
    instance = { active: true, dispose };
    Object.defineProperty(window, INSTANCE_KEY, { configurable: true, value: instance });
    capability.onDispose(dispose);
    window.postMessage({ type: CHAT_REQUEST_TYPE }, "*");
    scheduleUpdate();
  } catch (error) {
    dispose();
    throw error;
  }
})();
