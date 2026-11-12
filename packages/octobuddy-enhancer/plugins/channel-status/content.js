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
  let activeTab = "status";
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
  let detailsLookupTarget;
  let chatDetailsResult;
  let chatDetailsLoading = false;
  let detailsRequestSequence = 0;
  let activeDetailsRequest;
  let drag;
  let suppressClick = false;
  let instance;

  const style = document.createElement("style");
  style.textContent = `
    [data-octobuddy-channel-status-root] {
      all: initial !important;
      position: fixed !important;
      z-index: 2147483646 !important;
      right: 0 !important;
      top: calc(50% - 19px) !important;
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
      position: relative !important;
      z-index: 1 !important;
      align-items: center !important;
      background: #17272e !important;
      border: 1px solid #39515b !important;
      border-radius: 999px !important;
      box-shadow: 0 5px 18px rgba(0, 0, 0, 0.28) !important;
      color: #eaf1f3 !important;
      opacity: 0.58 !important;
      touch-action: none !important;
      user-select: none !important;
      cursor: pointer !important;
      display: flex !important;
      font: 600 12px/1.2 -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif !important;
      gap: 8px !important;
      min-height: 38px !important;
      padding: 0 13px !important;
      white-space: nowrap !important;
    }
    [data-octobuddy-channel-status-button]:hover {
      opacity: 1 !important;
      background: #20343c !important;
    }
    [data-octobuddy-channel-status-button]:focus-visible {
      opacity: 1 !important;
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
      position: absolute !important;
      z-index: 0 !important;
      max-height: calc(100vh - 16px) !important;
      overflow-y: auto !important;
      background: #142229 !important;
      border: 1px solid #39515b !important;
      border-radius: 11px !important;
      box-shadow: 0 10px 32px rgba(0, 0, 0, 0.34) !important;
      color: #eaf1f3 !important;
      margin: 0 !important;
      max-width: calc(100vw - 16px) !important;
      padding: 13px 14px !important;
      width: min(420px, calc(100vw - 16px)) !important;
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
    [data-octobuddy-channel-status-tabs] {
      background: #101c21 !important;
      border: 1px solid #31464e !important;
      border-radius: 8px !important;
      display: grid !important;
      gap: 3px !important;
      grid-template-columns: repeat(3, minmax(0, 1fr)) !important;
      margin: 0 0 12px !important;
      padding: 3px !important;
    }
    [data-octobuddy-channel-status-tabs] [role="tab"] {
      background: transparent !important;
      border: 0 !important;
      border-radius: 5px !important;
      color: #afc0c6 !important;
      cursor: pointer !important;
      font: 600 12px/1.3 -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif !important;
      min-width: 0 !important;
      padding: 7px 5px !important;
      text-align: center !important;
      white-space: nowrap !important;
    }
    [data-octobuddy-channel-status-tabs] [role="tab"][aria-selected="true"] {
      background: #27434a !important;
      box-shadow: inset 0 -2px #71d8d0 !important;
      color: #eaf1f3 !important;
    }
    [data-octobuddy-channel-status-tabs] [role="tab"]:focus-visible {
      outline: 2px solid #71d8d0 !important;
      outline-offset: 1px !important;
    }
    [data-octobuddy-channel-status-tabpanel] {
      min-width: 0 !important;
      overflow-wrap: anywhere !important;
    }
    [data-octobuddy-channel-status-group-heading] {
      color: #f4f8f9 !important;
      font-size: 12px !important;
      font-weight: 650 !important;
      margin: 12px 0 6px !important;
    }
    [data-octobuddy-channel-status-list] {
      display: grid !important;
      gap: 7px !important;
      list-style: none !important;
      margin: 0 !important;
      padding: 0 !important;
    }
    [data-octobuddy-channel-status-task] {
      border-left: 2px solid #39515b !important;
      margin: 0 !important;
      min-width: 0 !important;
      padding: 2px 0 2px 9px !important;
    }
    [data-octobuddy-channel-status-link] {
      color: #8be0d6 !important;
      overflow-wrap: anywhere !important;
      text-decoration: underline !important;
      text-underline-offset: 2px !important;
    }
    [data-octobuddy-channel-status-link]:focus-visible {
      outline: 2px solid #71d8d0 !important;
      outline-offset: 2px !important;
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
        top: calc(50% - 17px) !important;
      }
      [data-octobuddy-channel-status-button] {
        min-height: 34px !important;
        padding: 0 10px !important;
      }
      [data-octobuddy-channel-status-panel] {
        max-height: min(45vh, 300px) !important;
        overflow-y: auto !important;
        width: min(420px, calc(100vw - 16px)) !important;
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

  const tabList = document.createElement("div");
  tabList.setAttribute("role", "tablist");
  tabList.setAttribute("aria-label", "频道状态视图");
  tabList.setAttribute("data-octobuddy-channel-status-tabs", "");

  const panelBody = document.createElement("div");
  panelBody.id = "octobuddy-channel-status-tabpanel";
  panelBody.setAttribute("role", "tabpanel");
  panelBody.setAttribute("tabindex", "0");
  panelBody.setAttribute("data-octobuddy-channel-status-tabpanel", "");

  const tabButtons = {};
  for (const [key, label] of Object.entries({
    status: "状态",
    context: "上下文",
    associations: "关联",
  })) {
    const tab = document.createElement("button");
    tab.type = "button";
    tab.id = `octobuddy-channel-status-tab-${key}`;
    tab.setAttribute("role", "tab");
    tab.setAttribute("aria-controls", panelBody.id);
    tab.setAttribute("aria-selected", String(key === activeTab));
    tab.tabIndex = key === activeTab ? 0 : -1;
    tab.textContent = label;
    tab.addEventListener("click", () => selectTab(key));
    tab.addEventListener("keydown", handleTabKeydown);
    tabButtons[key] = tab;
    tabList.append(tab);
  }
  panelBody.setAttribute("aria-labelledby", tabButtons.status.id);
  panel.append(tabList, panelBody);

  function appendRow(label, value) {
    const row = document.createElement("div");
    row.setAttribute("data-octobuddy-channel-status-row", "");
    const labelNode = document.createElement("span");
    labelNode.textContent = label;
    const valueNode = document.createElement("span");
    valueNode.textContent = value;
    row.append(labelNode, valueNode);
    panelBody.append(row);
  }

  function appendSnapshot(snapshot, stale) {
    if (stale) {
      const notice = document.createElement("p");
      notice.setAttribute("data-octobuddy-channel-status-stale", "");
      notice.textContent = "过期快照：以下为最后成功采样的数据";
      panelBody.append(notice);
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
    panelBody.append(timeRow);
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

  function clearChatDetails() {
    if (activeDetailsRequest && typeof capability.cancelChatDetails === "function") {
      void capability.cancelChatDetails(activeDetailsRequest.requestId);
    }
    detailsRequestSequence += 1;
    activeDetailsRequest = undefined;
    detailsLookupTarget = undefined;
    chatDetailsResult = undefined;
    chatDetailsLoading = false;
  }

  async function refreshChatDetails() {
    if (disposed || !expanded || activeTab === "status") return;
    if (currentStatus?.state !== "online") {
      clearChatDetails();
      return;
    }

    const target = resolveChatTarget();
    if (!target) {
      clearChatDetails();
      return;
    }
    if (!sameTarget(detailsLookupTarget, target)) {
      clearChatDetails();
      detailsLookupTarget = target;
    }
    if (activeDetailsRequest && sameTarget(activeDetailsRequest.target, target)) return;
    if (typeof capability.getChatDetails !== "function") {
      chatDetailsResult = { state: "invalid" };
      chatDetailsLoading = false;
      renderEntry();
      return;
    }

    const requestId = ++detailsRequestSequence;
    activeDetailsRequest = { requestId, target };
    chatDetailsLoading = true;
    renderEntry();
    try {
      const response = await capability.getChatDetails(target, requestId);
      if (disposed || requestId !== detailsRequestSequence) return;
      if (!sameTarget(target, resolveChatTarget()) || currentStatus?.state !== "online") return;
      chatDetailsResult = response;
    } catch {
      if (disposed || requestId !== detailsRequestSequence) return;
      chatDetailsResult = { state: "offline" };
    } finally {
      if (disposed || requestId !== detailsRequestSequence) return;
      activeDetailsRequest = undefined;
      chatDetailsLoading = false;
      renderEntry();
    }
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
    clearChatDetails();
    renderEntry();
    if (currentStatus?.state === "online") {
      void refreshChatStatus();
      if (expanded && activeTab !== "status") void refreshChatDetails();
    }
  }

  function appendHeading(text, group = false) {
    const heading = document.createElement("p");
    heading.setAttribute(group ? "data-octobuddy-channel-status-group-heading" : "data-octobuddy-channel-status-heading", "");
    heading.textContent = text;
    panelBody.append(heading);
  }

  function appendNote(text) {
    const note = document.createElement("p");
    note.setAttribute("data-octobuddy-channel-status-note", "");
    note.textContent = text;
    panelBody.append(note);
  }

  function appendStaleNotice(text = "过期数据：以下为当前聊天最后一次成功读取的快照") {
    const notice = document.createElement("p");
    notice.setAttribute("data-octobuddy-channel-status-stale", "");
    notice.textContent = text;
    panelBody.append(notice);
  }

  function formatTimestamp(value) {
    const date = new Date(value);
    return Number.isNaN(date.getTime())
      ? value
      : date.toLocaleString("zh-CN", { hour12: false });
  }

  function formatCount(value) {
    return value === null ? "不可用" : new Intl.NumberFormat("zh-CN").format(value);
  }

  function appendChatStatus() {
    appendHeading("当前聊天状态");
    if (!currentChat) {
      appendIdentityNotice();
      return;
    }
    if (!detailReady || !currentStatus) {
      appendNote("正在获取服务状态以识别当前聊天…");
      return;
    }
    if (currentStatus.state !== "online") {
      appendNote("服务不可用，无法获取当前聊天状态");
      return;
    }

    const target = resolveChatTarget();
    if (!target) {
      appendNote(currentChat.channelType === 1 ? "该私聊不是当前 Bot 会话" : "无法识别当前聊天");
      return;
    }
    if (!chatStatus) {
      appendNote(chatStatusLoading
        ? "正在获取当前聊天状态…"
        : chatStatusError ? "当前聊天状态暂不可用" : "等待聊天状态查询");
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
    appendNote("无法识别当前聊天");
  }

  function detailsSnapshot() {
    return chatDetailsResult?.state === "online" ? chatDetailsResult.snapshot : undefined;
  }

  function appendDetailsUnavailable(result) {
    const messages = {
      offline: "关联数据服务暂时无法连接。",
      unconfigured: "关联数据接口尚未配置。",
      unauthorized: "关联数据接口认证失败。",
      invalid: "关联数据响应无效。",
    };
    appendNote(messages[result.state] ?? "当前聊天关联数据暂不可用。");
  }

  function appendContextTab() {
    appendHeading("会话上下文");
    if (!currentChat) {
      appendIdentityNotice();
      return;
    }
    if (!detailReady || !currentStatus) {
      appendNote("正在获取服务状态以识别当前聊天…");
      return;
    }
    if (currentStatus.state !== "online") {
      appendNote(`服务${STATUS_LABELS[currentStatus.state]}，当前聊天用量不可用。`);
      return;
    }
    if (!resolveChatTarget()) {
      appendIdentityNotice();
      return;
    }
    if (!chatDetailsResult) {
      appendNote(chatDetailsLoading ? "正在读取当前会话用量…" : "等待当前会话用量查询");
      return;
    }

    const snapshot = detailsSnapshot();
    if (!snapshot) {
      appendDetailsUnavailable(chatDetailsResult);
      return;
    }

    const context = snapshot.context;
    if (context.stale || context.source === "persisted") appendStaleNotice();
    appendRow("模型", context.model ?? "不可用");
    appendRow("已用 token", formatCount(context.usedTokens));
    appendRow("上下文窗口", formatCount(context.contextWindow));
    appendRow("使用比例", context.percent === null
      ? "不可用"
      : `${new Intl.NumberFormat("zh-CN", { maximumFractionDigits: 2 }).format(context.percent)}%`);
    appendRow("数据来源", {
      live: "实时",
      persisted: "历史快照",
      unavailable: "不可用",
    }[context.source] ?? "不可用");
    appendRow("采样时间", formatTimestamp(snapshot.sampledAt));
    if (context.observedAt) appendRow("会话观测时间", formatTimestamp(context.observedAt));
    if (context.note) appendNote(context.note);
  }

  function httpUrl(value) {
    if (typeof value !== "string") return false;
    try {
      const protocol = new URL(value).protocol;
      return protocol === "http:" || protocol === "https:";
    } catch {
      return false;
    }
  }

  function createExternalLink(url, label) {
    const link = document.createElement("a");
    link.setAttribute("data-octobuddy-channel-status-link", "");
    link.href = url;
    link.textContent = label;
    link.target = "_blank";
    link.rel = "noopener noreferrer";
    return link;
  }

  function appendReferenceGroup(label, references) {
    appendHeading(label, true);
    if (references.length === 0) {
      appendNote(`暂无${label}关联`);
      return;
    }

    const list = document.createElement("ul");
    list.setAttribute("data-octobuddy-channel-status-list", "");
    for (const reference of references) {
      const item = document.createElement("li");
      if (httpUrl(reference)) item.append(createExternalLink(reference, reference));
      else item.textContent = reference;
      list.append(item);
    }
    panelBody.append(list);
  }

  function appendTaskList(tasks) {
    appendHeading("Loop Task", true);
    if (tasks.length === 0) {
      appendNote("暂无绑定的 Loop Task");
      return;
    }

    const list = document.createElement("ul");
    list.setAttribute("data-octobuddy-channel-status-list", "");
    for (const task of tasks) {
      const item = document.createElement("li");
      item.setAttribute("data-octobuddy-channel-status-task", "");
      const title = task.title ?? task.taskId;
      item.append(httpUrl(task.url) ? createExternalLink(task.url, title) : document.createTextNode(title));
      const status = document.createElement("p");
      status.setAttribute("data-octobuddy-channel-status-note", "");
      status.textContent = `状态：${task.status ?? "未知"}`;
      item.append(status);
      list.append(item);
    }
    panelBody.append(list);
  }

  function appendAssociationsTab() {
    appendHeading("当前线程关联");
    if (!currentChat) {
      appendIdentityNotice();
      return;
    }
    if (!detailReady || !currentStatus) {
      appendNote("正在获取服务状态以识别当前聊天…");
      return;
    }
    if (currentStatus.state !== "online") {
      appendNote(`服务${STATUS_LABELS[currentStatus.state]}，当前聊天关联数据不可用。`);
      return;
    }
    if (!resolveChatTarget()) {
      appendIdentityNotice();
      return;
    }
    if (!chatDetailsResult) {
      appendNote(chatDetailsLoading ? "正在读取当前线程关联…" : "等待当前线程关联查询");
      return;
    }

    const snapshot = detailsSnapshot();
    if (!snapshot) {
      appendDetailsUnavailable(chatDetailsResult);
      return;
    }
    if (snapshot.context.stale || snapshot.context.source === "persisted") appendStaleNotice();
    appendReferenceGroup("PR", snapshot.references.pr);
    appendReferenceGroup("MR", snapshot.references.mr);
    appendTaskList(snapshot.tasks);
  }

  function renderStatusTab() {
    if (!detailReady || !currentStatus) {
      appendHeading("服务状态");
      appendNote("正在获取服务状态…");
      appendChatStatus();
      return;
    }

    appendHeading(`服务状态：${STATUS_LABELS[currentStatus.state]}`);
    if (currentStatus.state === "online") {
      appendSnapshot(currentStatus.snapshot, false);
    } else if (currentStatus.lastSuccess) {
      appendSnapshot(currentStatus.lastSuccess, true);
    } else {
      appendNote(STATUS_MESSAGES[currentStatus.state]);
    }
    appendChatStatus();
  }

  function renderDetails() {
    panelBody.replaceChildren();
    panelBody.setAttribute("aria-labelledby", tabButtons[activeTab].id);
    if (activeTab === "status") renderStatusTab();
    else if (activeTab === "context") appendContextTab();
    else appendAssociationsTab();
  }

  function selectTab(key, focus = false) {
    if (!tabButtons[key]) return;
    const previousTab = activeTab;
    activeTab = key;
    for (const [tabKey, tab] of Object.entries(tabButtons)) {
      const selected = tabKey === key;
      tab.setAttribute("aria-selected", String(selected));
      tab.tabIndex = selected ? 0 : -1;
    }
    panelBody.setAttribute("aria-labelledby", tabButtons[key].id);
    if (focus) tabButtons[key].focus();
    if (expanded) {
      renderDetails();
      positionPanel();
    }
    if (key !== "status"
      && (previousTab === "status" || !chatDetailsResult || chatDetailsResult.state !== "online")) {
      void refreshChatDetails();
    }
  }

  function handleTabKeydown(event) {
    const keys = Object.keys(tabButtons);
    const currentIndex = keys.findIndex((key) => tabButtons[key] === event.currentTarget);
    let nextIndex;
    if (event.key === "ArrowRight") nextIndex = (currentIndex + 1) % keys.length;
    else if (event.key === "ArrowLeft") nextIndex = (currentIndex + keys.length - 1) % keys.length;
    else if (event.key === "Home") nextIndex = 0;
    else if (event.key === "End") nextIndex = keys.length - 1;
    else return;
    event.preventDefault();
    selectTab(keys[nextIndex], true);
  }

  function viewportSize() {
    const documentElement = document.documentElement;
    return {
      width: documentElement.clientWidth || window.innerWidth,
      height: documentElement.clientHeight || window.innerHeight,
    };
  }

  function clamp(value, minimum, maximum) {
    return Math.max(minimum, Math.min(value, maximum));
  }

  function positionPanel() {
    const viewport = viewportSize();
    const bounds = root.getBoundingClientRect();
    const width = Math.min(panel.offsetWidth, Math.max(0, viewport.width - 16));
    const height = Math.min(panel.offsetHeight, Math.max(0, viewport.height - 16));
    const x = bounds.left >= width + 8 ? bounds.left - width - 8 : bounds.right + 8;
    const y = bounds.top + bounds.height / 2 - height / 2;
    const maxLeft = Math.max(8, viewport.width - width - 8);
    const maxTop = Math.max(8, viewport.height - height - 8);
    panel.style.left = `${clamp(x, 8, maxLeft) - bounds.left}px`;
    panel.style.top = `${clamp(y, 8, maxTop) - bounds.top}px`;
  }

  function beginDrag(event) {
    if (event.button !== 0) return;
    const bounds = root.getBoundingClientRect();
    suppressClick = false;
    drag = { id: event.pointerId, x: event.clientX, y: event.clientY, left: bounds.left, top: bounds.top, moved: false };
    button.setPointerCapture(event.pointerId);
  }

  function moveDrag(event) {
    if (!drag || event.pointerId !== drag.id) return;
    const dx = event.clientX - drag.x;
    const dy = event.clientY - drag.y;
    if (!drag.moved && Math.hypot(dx, dy) < 4) return;
    drag.moved = true;
    const bounds = root.getBoundingClientRect();
    const viewport = viewportSize();
    root.style.setProperty("left", `${clamp(drag.left + dx, 0, Math.max(0, viewport.width - bounds.width))}px`, "important");
    root.style.setProperty("top", `${clamp(drag.top + dy, 0, Math.max(0, viewport.height - bounds.height))}px`, "important");
    root.style.setProperty("right", "auto", "important");
    if (expanded) positionPanel();
  }

  function endDrag(event) {
    if (!drag || event.pointerId !== drag.id) return;
    suppressClick = drag.moved;
    drag = undefined;
  }

  function handleViewportResize() {
    if (root.style.getPropertyValue("left")) {
      const bounds = root.getBoundingClientRect();
      const viewport = viewportSize();
      root.style.setProperty("left", `${clamp(bounds.left, 0, Math.max(0, viewport.width - bounds.width))}px`, "important");
      root.style.setProperty("top", `${clamp(bounds.top, 0, Math.max(0, viewport.height - bounds.height))}px`, "important");
    }
    if (expanded) positionPanel();
  }

  function renderEntry() {
    const state = currentStatus?.state ?? "loading";
    const label = currentStatus ? STATUS_LABELS[state] : "读取中";
    root.dataset.state = state;
    buttonLabel.textContent = `频道状态：${label}`;
    button.setAttribute("aria-label", `频道状态：${label}`);
    if (expanded) {
      renderDetails();
      positionPanel();
    }
  }

  function togglePanel() {
    if (suppressClick) {
      suppressClick = false;
      return;
    }
    if (disposed) return;
    expanded = !expanded;
    button.setAttribute("aria-expanded", String(expanded));
    panel.hidden = !expanded;
    if (expanded) {
      activeTab = "status";
      for (const [key, tab] of Object.entries(tabButtons)) {
        const selected = key === activeTab;
        tab.setAttribute("aria-selected", String(selected));
        tab.tabIndex = selected ? 0 : -1;
      }
      detailReady = false;
      renderDetails();
      positionPanel();
      requestRefresh();
    } else {
      detailReady = false;
      clearChatDetails();
      panelBody.replaceChildren();
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
        if (expanded && activeTab !== "status") void refreshChatDetails();
      } else {
        if (currentStatus.state === "invalid" || currentStatus.state === "unconfigured") {
          lastSuccess = undefined;
          delete currentStatus.lastSuccess;
        } else if (currentStatus.lastSuccess) {
          lastSuccess = currentStatus.lastSuccess;
        } else if (lastSuccess) {
          currentStatus.lastSuccess = lastSuccess;
        }
        clearChatStatus();
        clearChatDetails();
      }
    } catch {
      if (disposed) return;
      currentStatus = { state: "offline" };
      if (lastSuccess) currentStatus.lastSuccess = lastSuccess;
      clearChatStatus();
      clearChatDetails();
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
    clearChatDetails();
    chatRequestSequence += 1;
    activeChatRequest = undefined;
    if (refreshTimeout) window.clearTimeout(refreshTimeout);
    refreshTimeout = 0;
    refreshPending = false;
    button.removeEventListener("click", togglePanel);
    button.removeEventListener("pointerdown", beginDrag);
    button.removeEventListener("pointermove", moveDrag);
    button.removeEventListener("pointerup", endDrag);
    button.removeEventListener("pointercancel", endDrag);
    window.removeEventListener("message", handleChatMessage);
    window.removeEventListener("resize", handleViewportResize);
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
  button.addEventListener("pointerdown", beginDrag);
  button.addEventListener("pointermove", moveDrag);
  button.addEventListener("pointerup", endDrag);
  button.addEventListener("pointercancel", endDrag);
  window.addEventListener("message", handleChatMessage);
  window.addEventListener("resize", handleViewportResize);
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
