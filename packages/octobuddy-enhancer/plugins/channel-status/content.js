(() => {
  const INSTANCE_KEY = "__octobuddyChannelStatusPluginInstanceV1";
  const REFRESH_INTERVAL_MS = 5_000;
  const existing = window[INSTANCE_KEY];
  if (existing && existing.active) return;

  if (window.top !== window || window.location.protocol !== "file:") return;
  // Only the communication renderer owns the entry; the main renderer is intentionally excluded.
  if (!window.location.pathname.endsWith("/Contents/Resources/communication-renderer/index.html")) return;

  const capability = window.octoEnhancer;
  if (!capability || typeof capability.getChannelStatus !== "function" || typeof capability.onDispose !== "function") return;

  const STATUS_LABELS = {
    online: "在线",
    offline: "离线",
    unconfigured: "未配置",
    unauthorized: "认证失败",
    invalid: "配置或协议错误",
  };
  const STATUS_MESSAGES = {
    offline: "服务暂时无法连接。",
    unconfigured: "状态接口尚未配置。",
    unauthorized: "状态接口认证失败。",
    invalid: "状态接口配置或响应协议无效。",
  };
  const CHAT_STATUS_LABELS = {
    none: "无任务",
    idle: "空闲",
    queued: "排队中",
    running: "运行中",
    completed: "已完成",
    failed: "失败",
    cancelled: "已取消",
    interrupted: "已中断",
  };
  let disposed = false;
  let expanded = false;
  let detailReady = false;
  let requestInFlight = false;
  let refreshPending = false;
  let refreshTimeout = 0;
  let lastSuccess;
  let currentStatus;
  let currentChat;
  let chatLookupTarget;
  let chatStatus;
  let chatStatusLoading = false;
  let chatStatusError = false;
  let chatRequestSequence = 0;
  let activeChatRequest;
  let instance;

  const style = document.createElement("style");
  style.textContent = `
    [data-octobuddy-channel-status-root] {
      all: initial !important;
      position: fixed !important;
      z-index: 2147483646 !important;
      right: 16px !important;
      bottom: 16px !important;
      color: #eaf1f3 !important;
      color-scheme: dark !important;
      isolation: isolate !important;
      font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif !important;
      font-size: 12px !important;
      line-height: 1.45 !important;
    }
    [data-octobuddy-channel-status-root] *,
    [data-octobuddy-channel-status-root] *::before,
    [data-octobuddy-channel-status-root] *::after {
      box-sizing: border-box !important;
    }
    [data-octobuddy-channel-status-button] {
      align-items: center !important;
      background: #17272e !important;
      border: 1px solid #39515b !important;
      border-radius: 999px !important;
      box-shadow: 0 5px 18px rgba(0, 0, 0, 0.28) !important;
      color: #eaf1f3 !important;
      cursor: pointer !important;
      display: flex !important;
      font: 600 12px/1.2 -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif !important;
      gap: 8px !important;
      min-height: 38px !important;
      padding: 0 13px !important;
      white-space: nowrap !important;
    }
    [data-octobuddy-channel-status-button]:hover {
      background: #20343c !important;
    }
    [data-octobuddy-channel-status-button]:focus-visible {
      outline: 2px solid #71d8d0 !important;
      outline-offset: 3px !important;
    }
    [data-octobuddy-channel-status-dot] {
      background: #87949a !important;
      border-radius: 50% !important;
      flex: 0 0 8px !important;
      height: 8px !important;
      width: 8px !important;
    }
    [data-octobuddy-channel-status-root][data-state="online"] [data-octobuddy-channel-status-dot] {
      background: #62d5a2 !important;
    }
    [data-octobuddy-channel-status-root][data-state="offline"] [data-octobuddy-channel-status-dot] {
      background: #ff777d !important;
    }
    [data-octobuddy-channel-status-root][data-state="unauthorized"] [data-octobuddy-channel-status-dot],
    [data-octobuddy-channel-status-root][data-state="invalid"] [data-octobuddy-channel-status-dot] {
      background: #ffbd66 !important;
    }
    [data-octobuddy-channel-status-root][data-state="unconfigured"] [data-octobuddy-channel-status-dot] {
      background: #99a5aa !important;
    }
    [data-octobuddy-channel-status-panel] {
      background: #142229 !important;
      border: 1px solid #39515b !important;
      border-radius: 11px !important;
      box-shadow: 0 10px 32px rgba(0, 0, 0, 0.34) !important;
      color: #eaf1f3 !important;
      margin: 0 0 9px auto !important;
      max-width: calc(100vw - 32px) !important;
      padding: 13px 14px !important;
      width: 278px !important;
    }
    [data-octobuddy-channel-status-panel][hidden] {
      display: none !important;
    }
    [data-octobuddy-channel-status-heading] {
      color: #f4f8f9 !important;
      font-size: 13px !important;
      font-weight: 650 !important;
      margin: 0 0 9px !important;
    }
    [data-octobuddy-channel-status-row] {
      align-items: baseline !important;
      display: flex !important;
      gap: 8px !important;
      justify-content: space-between !important;
      margin: 5px 0 !important;
    }
    [data-octobuddy-channel-status-row] > span:first-child {
      color: #afc0c6 !important;
    }
    [data-octobuddy-channel-status-row] > span:last-child {
      color: #f2f6f7 !important;
      text-align: right !important;
      overflow-wrap: anywhere !important;
    }
    [data-octobuddy-channel-status-stale] {
      background: rgba(255, 189, 102, 0.11) !important;
      border-left: 2px solid #ffbd66 !important;
      color: #ffd497 !important;
      margin: 0 0 9px !important;
      padding: 5px 7px !important;
    }
    [data-octobuddy-channel-status-note] {
      color: #afc0c6 !important;
      margin: 8px 0 0 !important;
    }
    @media (prefers-reduced-motion: reduce) {
      [data-octobuddy-channel-status-root] *,
      [data-octobuddy-channel-status-root] *::before,
      [data-octobuddy-channel-status-root] *::after {
        scroll-behavior: auto !important;
        transition-duration: 0s !important;
      }
    }
    @media (max-width: 480px), (max-height: 560px) {
      [data-octobuddy-channel-status-root] {
        bottom: 8px !important;
        right: 8px !important;
      }
      [data-octobuddy-channel-status-button] {
        min-height: 34px !important;
        padding: 0 10px !important;
      }
      [data-octobuddy-channel-status-panel] {
        max-height: min(45vh, 300px) !important;
        overflow-y: auto !important;
        width: min(278px, calc(100vw - 16px)) !important;
      }
    }
  `;

  const root = document.createElement("div");
  root.setAttribute("data-octobuddy-channel-status-root", "");
  root.dataset.state = "loading";

  const button = document.createElement("button");
  button.type = "button";
  button.setAttribute("data-octobuddy-channel-status-button", "");
  button.setAttribute("aria-controls", "octobuddy-channel-status-panel");
  button.setAttribute("aria-expanded", "false");

  const dot = document.createElement("span");
  dot.setAttribute("data-octobuddy-channel-status-dot", "");
  dot.setAttribute("aria-hidden", "true");

  const buttonLabel = document.createElement("span");
  buttonLabel.textContent = "频道状态：读取中";
  button.append(dot, buttonLabel);

  const panel = document.createElement("section");
  panel.id = "octobuddy-channel-status-panel";
  panel.setAttribute("data-octobuddy-channel-status-panel", "");
  panel.setAttribute("role", "region");
  panel.setAttribute("aria-label", "频道状态详情");
  panel.hidden = true;

  function appendRow(label, value) {
    const row = document.createElement("div");
    row.setAttribute("data-octobuddy-channel-status-row", "");
    const labelNode = document.createElement("span");
    labelNode.textContent = label;
    const valueNode = document.createElement("span");
    valueNode.textContent = value;
    row.append(labelNode, valueNode);
    panel.append(row);
  }

  function appendSnapshot(snapshot, stale) {
    if (stale) {
      const notice = document.createElement("p");
      notice.setAttribute("data-octobuddy-channel-status-stale", "");
      notice.textContent = "过期快照：以下为最后成功采样的数据";
      panel.append(notice);
    }
    appendRow("PID", String(snapshot.pid));
    appendRow("运行中会话", String(snapshot.runningSessions));
    appendRow("排队任务", String(snapshot.queuedTasks));

    const sampleDate = new Date(snapshot.sampledAt);
    const sampledAt = Number.isNaN(sampleDate.getTime())
      ? snapshot.sampledAt
      : sampleDate.toLocaleString("zh-CN", { hour12: false });
    const time = document.createElement("time");
    time.dateTime = snapshot.sampledAt;
    time.textContent = sampledAt;
    const timeRow = document.createElement("div");
    timeRow.setAttribute("data-octobuddy-channel-status-row", "");
    const timeLabel = document.createElement("span");
    timeLabel.textContent = stale ? "最后成功采样" : "采样时间";
    const timeValue = document.createElement("span");
    timeValue.append(time);
    timeRow.append(timeLabel, timeValue);
    panel.append(timeRow);
  }

  function resolveChatTarget() {
    if (!currentChat) return undefined;
    if (currentChat.channelType !== 1) return currentChat;

    const snapshot = currentStatus?.state === "online" ? currentStatus.snapshot : undefined;
    if (!snapshot || currentChat.channelId !== snapshot.botUid || !snapshot.ownerUid) return undefined;
    return { channelType: 1, channelId: snapshot.ownerUid };
  }

  function sameTarget(left, right) {
    return Boolean(left && right
      && left.channelType === right.channelType
      && left.channelId === right.channelId);
  }

  function clearChatStatus() {
    if (activeChatRequest) void capability.cancelChatStatus(activeChatRequest.sequence);
    chatRequestSequence += 1;
    activeChatRequest = undefined;
    chatLookupTarget = undefined;
    chatStatus = undefined;
    chatStatusLoading = false;
    chatStatusError = false;
  }


  async function refreshChatStatus() {
    if (disposed) return;
    if (currentStatus?.state !== "online") {
      clearChatStatus();
      return;
    }

    const target = resolveChatTarget();
    if (!target) {
      clearChatStatus();
      return;
    }
    if (!sameTarget(chatLookupTarget, target)) {
      clearChatStatus();
      chatLookupTarget = target;
    }
    if (activeChatRequest && sameTarget(activeChatRequest.target, target)) return;

    const sequence = ++chatRequestSequence;
    activeChatRequest = { sequence, target };
    chatStatusLoading = true;
    chatStatusError = false;
    try {
      const response = await capability.getChatStatus(target, sequence);
      if (disposed || sequence !== chatRequestSequence) return;
      const currentTarget = resolveChatTarget();
      if (!sameTarget(target, currentTarget) || currentStatus?.state !== "online") return;
      chatStatus = response.state === "online" ? response : undefined;
      chatStatusError = !chatStatus;
    } catch {
      if (disposed || sequence !== chatRequestSequence) return;
      chatStatus = undefined;
      chatStatusError = true;
    } finally {
      if (disposed || sequence !== chatRequestSequence) return;
      activeChatRequest = undefined;
      chatStatusLoading = false;
      renderEntry();
    }
  }

  function handleChatMessage(event) {
    if (disposed || event.source !== window) return;
    const message = event.data;
    if (!message || message.type !== "octobuddy-enhancer:chat") return;
    const channelType = message.channelType;
    const channelId = message.channelId;
    const nextChat = (channelType === 1 || channelType === 2 || channelType === 5)
      && typeof channelId === "string"
      ? { channelType, channelId }
      : undefined;
    if (sameTarget(currentChat, nextChat) || (!currentChat && !nextChat)) return;

    currentChat = nextChat;
    clearChatStatus();
    renderEntry();
    if (currentStatus?.state === "online") void refreshChatStatus();
  }

  function appendChatDetails() {
    const heading = document.createElement("p");
    heading.setAttribute("data-octobuddy-channel-status-heading", "");
    heading.textContent = "当前聊天状态";
    panel.append(heading);

    if (!currentChat) {
      appendIdentityNotice();
      return;
    }
    if (!detailReady || !currentStatus) {
      const note = document.createElement("p");
      note.setAttribute("data-octobuddy-channel-status-note", "");
      note.textContent = "正在获取服务状态以识别当前聊天…";
      panel.append(note);
      return;
    }
    if (currentStatus.state !== "online") {
      const note = document.createElement("p");
      note.setAttribute("data-octobuddy-channel-status-note", "");
      note.textContent = "服务不可用，无法获取当前聊天状态";
      panel.append(note);
      return;
    }

    const target = resolveChatTarget();
    if (!target) {
      const note = document.createElement("p");
      note.setAttribute("data-octobuddy-channel-status-note", "");
      note.textContent = currentChat.channelType === 1
        ? "该私聊不是当前 Bot 会话"
        : "无法识别当前聊天";
      panel.append(note);
      return;
    }
    if (!chatStatus) {
      const note = document.createElement("p");
      note.setAttribute("data-octobuddy-channel-status-note", "");
      note.textContent = chatStatusLoading
        ? "正在获取当前聊天状态…"
        : chatStatusError ? "当前聊天状态暂不可用" : "等待聊天状态查询";
      panel.append(note);
      return;
    }

    const snapshot = chatStatus.snapshot;
    const channelLabel = snapshot.channelType === 1 ? "私聊" : snapshot.channelType === 2 ? "群聊" : "话题";
    appendRow("状态目标", `${channelLabel} ${snapshot.channelId}`);
    appendRow("执行状态", CHAT_STATUS_LABELS[snapshot.state]);
    appendRow("模型", snapshot.model ?? "由 OMP 自动选择");
    appendRow("工作目录", snapshot.cwd ?? "未绑定");
    appendRow("排队任务", String(snapshot.queuedTasks));
    appendRow("待回答问题", String(snapshot.pendingQuestions));
  }

  function appendIdentityNotice() {
    const note = document.createElement("p");
    note.setAttribute("data-octobuddy-channel-status-note", "");
    note.textContent = "无法识别当前聊天";
    panel.append(note);
  }

  function renderDetails() {
    panel.replaceChildren();
    if (!detailReady || !currentStatus) {
      const heading = document.createElement("p");
      heading.setAttribute("data-octobuddy-channel-status-heading", "");
      heading.textContent = "正在获取服务状态…";
      panel.append(heading);
      appendChatDetails();
      return;
    }

    const heading = document.createElement("p");
    heading.setAttribute("data-octobuddy-channel-status-heading", "");
    heading.textContent = `服务状态：${STATUS_LABELS[currentStatus.state]}`;
    panel.append(heading);

    if (currentStatus.state === "online") {
      appendSnapshot(currentStatus.snapshot, false);
    } else if (currentStatus.lastSuccess) {
      appendSnapshot(currentStatus.lastSuccess, true);
    } else {
      const message = document.createElement("p");
      message.setAttribute("data-octobuddy-channel-status-note", "");
      message.textContent = STATUS_MESSAGES[currentStatus.state];
      panel.append(message);
    }
    appendChatDetails();
  }

  function renderEntry() {
    const state = currentStatus?.state ?? "loading";
    const label = currentStatus ? STATUS_LABELS[state] : "读取中";
    root.dataset.state = state;
    buttonLabel.textContent = `频道状态：${label}`;
    button.setAttribute("aria-label", `频道状态：${label}`);
    if (expanded) renderDetails();
  }

  function togglePanel() {
    if (disposed) return;
    expanded = !expanded;
    button.setAttribute("aria-expanded", String(expanded));
    panel.hidden = !expanded;
    if (expanded) {
      detailReady = false;
      renderDetails();
      requestRefresh();
    } else {
      detailReady = false;
      panel.replaceChildren();
    }
  }


  async function refresh() {
    if (disposed || requestInFlight) return;
    requestInFlight = true;
    try {
      const response = await capability.getChannelStatus();
      if (disposed) return;
      currentStatus = response;
      if (currentStatus.state === "online") {
        lastSuccess = currentStatus.snapshot;
        void refreshChatStatus();
      } else if (currentStatus.state === "invalid" || currentStatus.state === "unconfigured") {
        lastSuccess = undefined;
        delete currentStatus.lastSuccess;
        clearChatStatus();
      } else if (currentStatus.lastSuccess) {
        lastSuccess = currentStatus.lastSuccess;
        clearChatStatus();
      } else {
        if (lastSuccess) currentStatus.lastSuccess = lastSuccess;
        clearChatStatus();
      }
    } catch {
      if (disposed) return;
      currentStatus = { state: "offline" };
      if (lastSuccess) currentStatus.lastSuccess = lastSuccess;
      clearChatStatus();
    } finally {
      requestInFlight = false;
      if (disposed) return;
      detailReady = true;
      renderEntry();
      if (refreshPending) {
        refreshPending = false;
        refreshTimeout = window.setTimeout(requestRefresh, 0);
      } else {
        refreshTimeout = window.setTimeout(requestRefresh, REFRESH_INTERVAL_MS);
      }
    }
  }

  function requestRefresh() {
    if (disposed) return;
    if (refreshTimeout) {
      window.clearTimeout(refreshTimeout);
      refreshTimeout = 0;
    }
    if (requestInFlight) {
      refreshPending = true;
      return;
    }
    void refresh();
  }

  function dispose() {
    if (disposed) return;
    disposed = true;
    chatRequestSequence += 1;
    activeChatRequest = undefined;
    if (refreshTimeout) window.clearTimeout(refreshTimeout);
    refreshTimeout = 0;
    refreshPending = false;
    button.removeEventListener("click", togglePanel);
    window.removeEventListener("message", handleChatMessage);
    panel.replaceChildren();
    root.remove();
    style.remove();
    currentStatus = undefined;
    currentChat = undefined;
    chatLookupTarget = undefined;
    chatStatus = undefined;
    lastSuccess = undefined;
    if (window[INSTANCE_KEY] === instance) delete window[INSTANCE_KEY];
  }

  button.addEventListener("click", togglePanel);
  window.addEventListener("message", handleChatMessage);
  root.append(panel, button);
  instance = { active: true, dispose };
  Object.defineProperty(window, INSTANCE_KEY, { configurable: true, value: instance });
  window.postMessage({ type: "octobuddy-enhancer:chat-request" }, "*");
  try {
    capability.onDispose(dispose);
    if (!disposed) {
      (document.head ?? document.documentElement).append(style);
      document.documentElement.append(root);
      requestRefresh();
    }
  } catch (error) {
    dispose();
    throw error;
  }
})();
