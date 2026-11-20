import { mkdtemp, mkdir, readFile, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { dirname, join } from "node:path";
import { pathToFileURL } from "node:url";
import { expect, test } from "bun:test";
import { chromium, type BrowserContext, type CDPSession, type Page } from "playwright";

type ChatIdentity = {
  channelType: 1 | 2 | 5;
  channelId: string;
};

type ChatSnapshot = ChatIdentity & {
  version: 1;
  sampledAt: string;
  state: "none" | "idle" | "queued" | "running" | "completed" | "failed" | "cancelled" | "interrupted";
  model: string | null;
  cwd: string | null;
  zedWorkspaceUrl?: string | null;
  queuedTasks: number;
  pendingQuestions: number;
};

type ChatStatus = { state: "online"; snapshot: ChatSnapshot };
type ChannelSnapshot = {
  version: 1;
  pid: number;
  botUid: string | null;
  ownerUid: string | null;
  runningSessions: number;
  queuedTasks: number;
  sampledAt: string;
};

type ChannelStatus =
  | { state: "online"; snapshot: ChannelSnapshot }
  | { state: "unconfigured" | "offline" | "unauthorized" | "invalid"; lastSuccess?: ChannelSnapshot };

type ChatDetails = {
  version: 1;
  sampledAt: string;
  channelType: 1 | 2 | 5;
  channelId: string;
  context: {
    source: "live" | "persisted" | "unavailable";
    stale: boolean;
    usedTokens: number | null;
    contextWindow: number | null;
    percent: number | null;
    model: string | null;
    observedAt: string | null;
    note: string;
  };
  references: { pr: string[]; mr: string[] };
  tasks: Array<{
    workspaceId: string;
    taskId: string;
    title: string | null;
    status: string | null;
    url: string | null;
  }>;
};

type ChatDetailsResult =
  | { state: "online"; snapshot: ChatDetails }
  | { state: "unconfigured" | "offline" | "unauthorized" | "invalid"; lastSuccess?: ChatDetails };

const onlineChannelStatus: ChannelStatus = {
  state: "online",
  snapshot: {
    version: 1,
    pid: 47291,
    botUid: "bot-17",
    ownerUid: "owner-17",
    runningSessions: 1,
    queuedTasks: 0,
    sampledAt: "2026-11-11T10:14:30.000Z",
  },
};

const emptyChatDetails: ChatDetails = {
  version: 1,
  sampledAt: "2026-11-12T10:00:00.000Z",
  channelType: 5,
  channelId: "group-17____thread-3",
  context: {
    source: "unavailable",
    stale: false,
    usedTokens: null,
    contextWindow: null,
    percent: null,
    model: null,
    observedAt: null,
    note: "当前会话用量不可用",
  },
  references: { pr: [], mr: [] },
  tasks: [],
};

const fullChatDetails: ChatDetails = {
  version: 1,
  sampledAt: "2026-11-12T10:30:00.000Z",
  channelType: 5,
  channelId: "group-17____thread-3",
  context: {
    source: "live",
    stale: false,
    usedTokens: 12345,
    contextWindow: 32000,
    percent: 42.5,
    model: "openai/gpt-6-luna",
    observedAt: "2026-11-12T10:29:45.000Z",
    note: "当前会话上下文用量",
  },
  references: {
    pr: ["https://github.com/acme/project/pull/42"],
    mr: ["https://gitlab.com/acme/project/-/merge_requests/19"],
  },
  tasks: [
    {
      workspaceId: "workspace-1",
      taskId: "loop-23",
      title: "Reduce chat panel state leakage",
      status: "In Progress",
      url: "https://im.deepminer.com.cn/fleet/test3-fxpv/issues/TES9-1",
    },
    {
      workspaceId: "workspace-2",
      taskId: "loop-24",
      title: "Unlinked task",
      status: "Queued",
      url: "javascript:alert(1)",
    },
  ],
};

const detailBodySentinel = "DO_NOT_RENDER_SESSION_BODY_9d2c";

type ChatDetailsWithUnexpectedBody = ChatDetails & { sessionBody: string };

const fullChatDetailsWithUnexpectedBody: { state: "online"; snapshot: ChatDetailsWithUnexpectedBody } = {
  state: "online",
  snapshot: {
    ...fullChatDetails,
    sessionBody: detailBodySentinel,
  },
};

type AgentHubSnapshot = {
  version: 1;
  channelType: 1 | 2 | 5;
  channelId: string;
  agents: {
    id: string;
    parentId: string | null;
    agent: string;
    status: string;
    description: string | null;
    activity: unknown[];
    messages: unknown[];
  }[];
};

type AgentHubResult =
  | { state: "online"; snapshot: AgentHubSnapshot }
  | { state: "unconfigured" | "offline" | "unauthorized" | "invalid"; lastSuccess?: AgentHubSnapshot };

type RecordedDetailsRequest = { target: ChatIdentity; requestId: number };

const emptyOfflineLastSuccess: ChannelSnapshot = {
  version: 1,
  pid: 68143,
  botUid: "bot-17",
  ownerUid: "owner-17",
  runningSessions: 2,
  queuedTasks: 5,
  sampledAt: "2026-11-10T18:20:00.000Z",
};

interface ChatPanelFixture {
  page: Page;
  chatTargets(): Promise<ChatIdentity[]>;
  cancelledTargets(): Promise<ChatIdentity[]>;
  detailsRequests(): Promise<RecordedDetailsRequest[]>;
  cancelledDetailRequestIds(): Promise<number[]>;
  waitForDetailsCount(count: number): Promise<number>;
  resolveChatStatus(index: number, status: ChatStatus): Promise<boolean>;
  resolveChatDetails(index: number, details: ChatDetailsResult): Promise<boolean>;
  setChannelStatus(status: ChannelStatus): Promise<boolean>;
  openedUrls(): string[];
  zedRequests(): Promise<ChatIdentity[]>;
  agentHubRequests(): Promise<ChatIdentity[]>;
  waitForAgentHubCount(count: number, timeoutMs?: number): Promise<number>;
  resolveAgentHub(index: number, result: AgentHubResult): Promise<boolean>;
}

const contentScriptPath = new URL("../plugins/channel-status/content.js", import.meta.url);
const isolatedWorldName = "octobuddy-chat-panel-test-world";
const chatPanelButton = '[data-octobuddy-channel-status-button]';
const chatPanelRegion = '[data-octobuddy-channel-status-panel]';

async function withChatPanelPage(
  exercise: (fixture: ChatPanelFixture) => Promise<void>,
  initialServiceStatus: ChannelStatus = onlineChannelStatus,
): Promise<void> {
  const pluginSource = await readFile(contentScriptPath, "utf8");
  const root = await mkdtemp(join(tmpdir(), "octobuddy-chat-panel-"));
  let context: BrowserContext | undefined;

  try {
    const profileDir = join(root, "profile");
    const targetPath = join(root, "OctoBuddy.app", "Contents", "Resources", "communication-renderer", "index.html");
    await Promise.all([
      mkdir(profileDir, { recursive: true }),
      mkdir(dirname(targetPath), { recursive: true }),
    ]);
    await writeFile(targetPath, "<!doctype html><html><head><meta charset=\"utf-8\"><title>OctoBuddy chat panel fixture</title></head><body><main>Conversation</main></body></html>", "utf8");

    context = await chromium.launchPersistentContext(profileDir, {
      headless: true,
      viewport: { width: 1280, height: 800 },
      args: [
        "--disable-background-networking",
        "--disable-component-update",
        "--disable-default-apps",
        "--disable-sync",
        "--no-first-run",
        "--no-default-browser-check",
      ],
    });
    const page = context.pages()[0] ?? await context.newPage();
    page.setDefaultTimeout(3_000);
    await context.route("**/*", async (route) => {
      const protocol = new URL(route.request().url()).protocol;
      if (protocol === "http:" || protocol === "https:") await route.abort();
      else await route.continue();
    });

    const cdp = await context.newCDPSession(page);
    await cdp.send("Page.enable");
    await cdp.send("Runtime.enable");
    const externalRequests: string[] = [];
    context.on("request", (request) => {
      if (/^https?:/.test(request.url())) externalRequests.push(request.url());
    });
    await page.goto(pathToFileURL(targetPath).href, { waitUntil: "load" });

    const frameTree = await cdp.send("Page.getFrameTree") as { frameTree: { frame: { id: string } } };
    const isolated = await cdp.send("Page.createIsolatedWorld", {
      frameId: frameTree.frameTree.frame.id,
      worldName: isolatedWorldName,
      grantUniveralAccess: false,
    }) as { executionContextId: number };
    const contextId = isolated.executionContextId;
    await evaluateInWorld(cdp, contextId, bridgeBootstrap(initialServiceStatus));
    await evaluateInWorld(cdp, contextId, pluginSource);

    const readBridge = async <T>(expression: string): Promise<T> => {
      const response = await cdp.send("Runtime.evaluate", {
        expression,
        contextId,
        awaitPromise: true,
        returnByValue: true,
      }) as { result?: { value?: T }; exceptionDetails?: { text?: string } };
      if (response.exceptionDetails) throw new Error(response.exceptionDetails.text ?? "chat panel fixture evaluation failed");
      return response.result?.value as T;
    };

    await exercise({
      page,
      chatTargets: () => readBridge("window.__chatPanelTestBridge.requests.map((request) => request.target)"),
      cancelledTargets: () => readBridge("window.__chatPanelTestBridge.cancelled.map((id) => window.__chatPanelTestBridge.requests.find((request) => request.id === id).target)"),
      detailsRequests: () => readBridge("window.__chatPanelTestBridge.details.map(({ target, requestId }) => ({ target, requestId }))"),
      agentHubRequests: () => readBridge("window.__chatPanelTestBridge.agentHub.map((request) => request.target)"),
      waitForAgentHubCount: (count, timeoutMs = 3_000) => readBridge(`(async () => {
        const deadline = Date.now() + ${timeoutMs};
        while (window.__chatPanelTestBridge.agentHub.length < ${count} && Date.now() < deadline) {
          const { promise, resolve } = Promise.withResolvers();
          window.setTimeout(resolve, 10);
          await promise;
        }
        return window.__chatPanelTestBridge.agentHub.length;
      })()`),
      cancelledDetailRequestIds: () => readBridge("window.__chatPanelTestBridge.cancelledDetails"),
      waitForDetailsCount: (count) => readBridge(`(async () => {
        const deadline = Date.now() + 3_000;
        while (window.__chatPanelTestBridge.details.length < ${count} && Date.now() < deadline) {
          const { promise, resolve } = Promise.withResolvers();
          window.setTimeout(resolve, 10);
          await promise;
        }
        return window.__chatPanelTestBridge.details.length;
      })()`),
      resolveChatStatus: (index, status) => readBridge(`window.__chatPanelTestBridge.resolve(${index}, ${JSON.stringify(status)})`),
      resolveChatDetails: (index, details) => readBridge(`window.__chatPanelTestBridge.resolveDetails(${index}, ${JSON.stringify(details)})`),
      resolveAgentHub: (index, result) => readBridge(`window.__chatPanelTestBridge.resolveAgentHub(${index}, ${JSON.stringify(result)})`),
      setChannelStatus: (status) => readBridge(`window.__chatPanelTestBridge.setServiceStatus(${JSON.stringify(status)})`),
      zedRequests: () => readBridge("window.__chatPanelTestBridge.zedRequests"),
      openedUrls: () => externalRequests.slice(),
    });
  } finally {
    try {
      await context?.close();
    } finally {
      await rm(root, { recursive: true, force: true });
    }
  }
}

async function evaluateInWorld(cdp: CDPSession, contextId: number, expression: string): Promise<void> {
  const result = await cdp.send("Runtime.evaluate", {
    expression,
    contextId,
    awaitPromise: true,
    returnByValue: true,
  }) as { exceptionDetails?: { text?: string } };
  if (result.exceptionDetails) throw new Error(result.exceptionDetails.text ?? "isolated-world plugin evaluation failed");
}

function bridgeBootstrap(initialServiceStatus: ChannelStatus): string {
  return `(() => {
  const requests = [];
  const cancelled = [];
  const details = [];
  const agentHub = [];
  const cancelledDetails = [];
  const zedRequests = [];
  const disposeCallbacks = [];
  let serviceStatus = ${JSON.stringify(initialServiceStatus)};
  Object.defineProperty(window, "__chatPanelTestBridge", {
    configurable: false,
    value: {
      requests,
      cancelled,
      details,
      agentHub,
      cancelledDetails,
      zedRequests,
      setServiceStatus(status) {
        serviceStatus = structuredClone(status);
        return true;
      },
      resolve(index, status) {
        const request = requests[index];
        if (!request || !request.resolve) return false;
        const resolve = request.resolve;
        request.resolve = undefined;
        resolve(structuredClone(status));
        return true;
      },
      resolveDetails(index, response) {
        const request = details[index];
        if (!request || !request.resolve) return false;
        const resolve = request.resolve;
        request.resolve = undefined;
        resolve(structuredClone(response));
        return true;
      },
      resolveAgentHub(index, response) {
        const request = agentHub[index];
        if (!request || !request.resolve) return false;
        const resolve = request.resolve;
        request.resolve = undefined;
        resolve(structuredClone(response));
        return true;
      },
    }
  });
  Object.defineProperty(window, "octoEnhancer", {
    configurable: false,
    value: Object.freeze({
      getChannelStatus: async () => structuredClone(serviceStatus),
      getChatStatus: (target, id) => {
        const { promise, resolve } = Promise.withResolvers();
        requests.push({ target: structuredClone(target), id, resolve });
        return promise;
      },
      cancelChatStatus: async (id) => { cancelled.push(id); },
      getChatDetails: (target, requestId) => {
        const { promise, resolve } = Promise.withResolvers();
        details.push({ target: structuredClone(target), requestId, resolve });
        return promise;
      },
      getAgentHub: (target) => {
        const { promise, resolve } = Promise.withResolvers();
        agentHub.push({ target: structuredClone(target), resolve });
        return promise;
      },
      cancelChatDetails: async (requestId) => { cancelledDetails.push(requestId); },
      openChatWorkspace: async (target) => { zedRequests.push(structuredClone(target)); },
      onDispose: (callback) => {
        if (typeof callback !== "function") throw new TypeError("onDispose requires a callback");
        disposeCallbacks.push(callback);
      }
    })
  });
})()`;
}

async function announceChat(page: Page, target: ChatIdentity): Promise<void> {
  await page.evaluate((identity) => {
    window.postMessage({ type: "octobuddy-enhancer:chat", ...identity }, "*");
  }, target);
  // Let the posted message and its listener run; assertions below report missing behavior directly.
  await page.evaluate(() => {
    const { promise, resolve } = Promise.withResolvers();
    window.setTimeout(resolve, 25);
    return promise;
  });
}

async function openPanel(page: Page): Promise<void> {
  const button = page.locator(chatPanelButton);
  const count = await button.count();
  expect(count).toBe(1);
  if (count !== 1) return;
  await button.evaluate((element) => (element as HTMLButtonElement).click());
}

async function panelText(page: Page): Promise<string> {
  const panel = page.locator(chatPanelRegion);
  const count = await panel.count();
  expect(count).toBe(1);
  if (count !== 1) return "";
  return panel.evaluate((element) => element.textContent ?? "");
}

async function settleStatusRendering(page: Page): Promise<void> {
  await page.evaluate(() => {
    const { promise, resolve } = Promise.withResolvers();
    window.setTimeout(resolve, 25);
    return promise;
  });
}

function expectChatDetails(text: string, snapshot: ChatSnapshot): void {
  expect(text).toContain(snapshot.model ?? "");
  expect(text).toContain(snapshot.cwd ?? "");
  expect(text).toMatch(/聊天状态|执行状态|chat status|status/i);
  expect(text).toMatch(/模型|model/i);
  expect(text).toMatch(/工作目录|cwd|working directory/i);
  expect(text).toMatch(/排队任务|queued tasks/i);
  expect(text).toMatch(/待回答|待答|pending questions/i);
  expect(text).toMatch(new RegExp(`(?:排队任务|queued tasks)[^\\d]*${snapshot.queuedTasks}`, "i"));
  expect(text).toMatch(new RegExp(`(?:待回答问题|待答问题|pending questions)[^\\d]*${snapshot.pendingQuestions}`, "i"));
}

function onlineChatStatus(snapshot: ChatSnapshot): ChatStatus {
  return { state: "online", snapshot };
}

const threadTarget: ChatIdentity = {
  channelType: 5,
  channelId: "group-17____thread-3",
};

const initialChat: ChatSnapshot = {
  version: 1,
  sampledAt: "2026-11-11T10:15:00.000Z",
  ...threadTarget,
  state: "running",
  model: "openai/gpt-6-luna",
  cwd: "/workspace/chat-alpha",
  queuedTasks: 4,
  pendingQuestions: 2,
};

test("the real content script displays the active chat snapshot from the page identity event", async () => {
  await withChatPanelPage(async ({ page, chatTargets, resolveChatStatus }) => {
    await openPanel(page);
    await announceChat(page, threadTarget);

    expect(await chatTargets()).toEqual([threadTarget]);
    expect(await resolveChatStatus(0, onlineChatStatus(initialChat))).toBe(true);
    await settleStatusRendering(page);

    expectChatDetails(await panelText(page), initialChat);
    expect(await panelText(page)).toMatch(/运行中|running/i);
  });
}, 15_000);

test("opens the current shared working directory in Zed from a trusted click", async () => {
  await withChatPanelPage(async ({ page, resolveChatStatus, zedRequests }) => {
    await openPanel(page);
    await announceChat(page, threadTarget);
    await resolveChatStatus(0, onlineChatStatus({ ...initialChat, cwd: "/workspaces/shared project", zedWorkspaceUrl: "zed://ssh/my-mini/workspaces/shared%20project" }));
    const button = page.getByRole("button", { name: /在 Zed 中打开工作目录/ });
    await button.click();
    expect(await zedRequests()).toEqual([threadTarget]);
  });
}, 15_000);

test("switching chats cancels only the preceding in-flight chat request", async () => {
  const first: ChatIdentity = { channelType: 2, channelId: "first" };
  const second: ChatIdentity = { channelType: 5, channelId: "group____second" };
  await withChatPanelPage(async ({ page, chatTargets, cancelledTargets }) => {
    await openPanel(page);
    await announceChat(page, first);
    await announceChat(page, second);
    expect(await chatTargets()).toEqual([first, second]);
    expect(await cancelledTargets()).toEqual([first]);
  });
}, 15_000);

test("switching the active chat clears the previous snapshot before the new response arrives", async () => {
  const oldTarget: ChatIdentity = { channelType: 2, channelId: "group-legacy" };
  const oldChat: ChatSnapshot = {
    version: 1,
    sampledAt: "2026-11-11T10:16:00.000Z",
    ...oldTarget,
    state: "queued",
    model: "openai/old-model",
    cwd: "/workspace/old-chat",
    queuedTasks: 9,
    pendingQuestions: 8,
  };
  const newTarget: ChatIdentity = { channelType: 5, channelId: "group-current____thread-4" };
  const newChat: ChatSnapshot = {
    version: 1,
    sampledAt: "2026-11-11T10:17:00.000Z",
    ...newTarget,
    state: "idle",
    model: "openai/new-model",
    cwd: "/workspace/new-chat",
    queuedTasks: 3,
    pendingQuestions: 1,
  };

  await withChatPanelPage(async ({ page, chatTargets, resolveChatStatus }) => {
    await openPanel(page);
    await announceChat(page, oldTarget);
    expect(await chatTargets()).toEqual([oldTarget]);
    expect(await resolveChatStatus(0, onlineChatStatus(oldChat))).toBe(true);
    await settleStatusRendering(page);
    expectChatDetails(await panelText(page), oldChat);

    await announceChat(page, newTarget);
    expect(await chatTargets()).toEqual([oldTarget, newTarget]);
    const clearedText = await panelText(page);
    expect(clearedText).not.toContain(oldChat.model ?? "");
    expect(clearedText).not.toContain(oldChat.cwd ?? "");
    expect(clearedText).not.toMatch(/(?:排队任务|queued tasks)[^\d]*9\b/i);
    expect(clearedText).not.toMatch(/(?:待回答问题|待答问题|pending questions)[^\d]*8\b/i);

    expect(await resolveChatStatus(1, onlineChatStatus(newChat))).toBe(true);
    await settleStatusRendering(page);
    expectChatDetails(await panelText(page), newChat);
  });
}, 15_000);

test("a late response for the previous chat cannot replace the current chat snapshot", async () => {
  const oldTarget: ChatIdentity = { channelType: 2, channelId: "group-before-switch" };
  const oldChat: ChatSnapshot = {
    version: 1,
    sampledAt: "2026-11-11T10:18:00.000Z",
    ...oldTarget,
    state: "running",
    model: "openai/stale-model",
    cwd: "/workspace/stale-chat",
    queuedTasks: 7,
    pendingQuestions: 6,
  };
  const currentTarget: ChatIdentity = { channelType: 5, channelId: "group-after-switch____thread-8" };
  const currentChat: ChatSnapshot = {
    version: 1,
    sampledAt: "2026-11-11T10:19:00.000Z",
    ...currentTarget,
    state: "completed",
    model: "openai/current-model",
    cwd: "/workspace/current-chat",
    queuedTasks: 2,
    pendingQuestions: 0,
  };

  await withChatPanelPage(async ({ page, chatTargets, resolveChatStatus }) => {
    await openPanel(page);
    await announceChat(page, oldTarget);
    expect(await chatTargets()).toEqual([oldTarget]);

    await announceChat(page, currentTarget);
    expect(await chatTargets()).toEqual([oldTarget, currentTarget]);
    expect(await resolveChatStatus(1, onlineChatStatus(currentChat))).toBe(true);
    await settleStatusRendering(page);
    expectChatDetails(await panelText(page), currentChat);

    expect(await resolveChatStatus(0, onlineChatStatus(oldChat))).toBe(true);
    await settleStatusRendering(page);
    const finalText = await panelText(page);
    expectChatDetails(finalText, currentChat);
    expect(finalText).not.toContain(oldChat.model ?? "");
    expect(finalText).not.toContain(oldChat.cwd ?? "");
  });
}, 15_000);

const botDmPageTarget: ChatIdentity = { channelType: 1, channelId: "bot-17" };
const ownerDmTarget: ChatIdentity = { channelType: 1, channelId: "owner-17" };
const ownerDmSnapshot: ChatSnapshot = {
  version: 1,
  sampledAt: "2026-11-11T10:20:00.000Z",
  ...ownerDmTarget,
  state: "idle",
  model: "openai/dm-model",
  cwd: "/workspace/private-chat",
  queuedTasks: 0,
  pendingQuestions: 0,
};

test("the bot identity in a private page maps to the owner chat for status lookup", async () => {
  await withChatPanelPage(async ({ page, chatTargets, resolveChatStatus }) => {
    await openPanel(page);
    await announceChat(page, botDmPageTarget);

    expect(await chatTargets()).toEqual([ownerDmTarget]);
    expect(await resolveChatStatus(0, onlineChatStatus(ownerDmSnapshot))).toBe(true);
    await settleStatusRendering(page);
    expectChatDetails(await panelText(page), ownerDmSnapshot);
    await announceChat(page, { channelType: 1, channelId: "another-user" });
    expect(await chatTargets()).toEqual([ownerDmTarget]);
  });
}, 15_000);


async function waitForOpenedUrl(url: string, openedUrls: () => string[]): Promise<void> {
  const deadline = Date.now() + 3_000;
  while (Date.now() < deadline) {
    if (openedUrls().includes(url)) return;
    const { promise, resolve } = Promise.withResolvers<void>();
    setTimeout(resolve, 10);
    await promise;
  }
  throw new Error(`expected a browser request for ${url}`);
}

test("the panel defaults to status, stays about 420px wide, and remains usable on a small screen", async () => {
  await withChatPanelPage(async ({ page }) => {
    await openPanel(page);
    const panel = page.locator(chatPanelRegion);
    await panel.waitFor({ state: "visible" });
    const statusTab = panel.getByRole("tab", { name: /状态|status/i });
    await statusTab.waitFor({ state: "visible" });
    expect(await panel.getByRole("tab").count()).toBe(4);
    expect(await statusTab.getAttribute("aria-selected")).toBe("true");
    expect(await panelText(page)).toContain("47291");

    const desktopBox = await panel.boundingBox();
    expect(desktopBox).not.toBeNull();
    if (desktopBox) {
      expect(desktopBox.width).toBeGreaterThanOrEqual(400);
      expect(desktopBox.width).toBeLessThanOrEqual(440);
    }

    await page.setViewportSize({ width: 360, height: 640 });
    await page.waitForFunction((selector) => {
      const element = document.querySelector(selector);
      if (!(element instanceof HTMLElement)) return false;
      const bounds = element.getBoundingClientRect();
      return bounds.left >= 0
        && bounds.top >= 0
        && bounds.right <= window.innerWidth
        && bounds.bottom <= window.innerHeight;
    }, chatPanelRegion, { timeout: 2_000 });
    const smallBox = await panel.boundingBox();
    expect(smallBox).not.toBeNull();
    if (smallBox) {
      expect(smallBox.x).toBeGreaterThanOrEqual(0);
      expect(smallBox.y).toBeGreaterThanOrEqual(0);
      expect(smallBox.x + smallBox.width).toBeLessThanOrEqual(360);
      expect(smallBox.y + smallBox.height).toBeLessThanOrEqual(640);
      expect(smallBox.width).toBeLessThanOrEqual(344);
    }
    expect(await page.evaluate(() => document.documentElement.scrollWidth)).toBeLessThanOrEqual(360);

    const button = page.locator(chatPanelButton);
    await button.click();
    expect(await button.getAttribute("aria-expanded")).toBe("false");
    const beforeDrag = (await button.boundingBox())!;
    const startX = beforeDrag.x + beforeDrag.width / 2;
    const startY = beforeDrag.y + beforeDrag.height / 2;
    await page.mouse.move(startX, startY);
    await page.mouse.down();
    await page.mouse.move(startX - 70, startY + 45, { steps: 4 });
    await page.mouse.up();
    const afterDrag = (await button.boundingBox())!;
    expect(afterDrag.x).toBeLessThan(beforeDrag.x - 30);
    expect(await button.getAttribute("aria-expanded")).toBe("false");
    await button.click();
    expect(await button.getAttribute("aria-expanded")).toBe("true");
  }, onlineChannelStatus);
}, 15_000);

test("context and associations tabs show usage and confirmed links without exposing session content", async () => {
  await withChatPanelPage(async ({ page, chatTargets, resolveChatStatus, detailsRequests, waitForDetailsCount, resolveChatDetails, openedUrls }) => {
    await openPanel(page);
    await announceChat(page, threadTarget);
    expect(await chatTargets()).toEqual([threadTarget]);
    expect(await resolveChatStatus(0, onlineChatStatus(initialChat))).toBe(true);
    await settleStatusRendering(page);

    const panel = page.locator(chatPanelRegion);
    const contextTab = panel.getByRole("tab", { name: /上下文|context/i });
    await contextTab.click();
    expect(await waitForDetailsCount(1)).toBeGreaterThanOrEqual(1);
    const contextRequest = (await detailsRequests())[0]!;
    expect(contextRequest.target).toEqual(threadTarget);
    expect(Number.isInteger(contextRequest.requestId)).toBe(true);
    expect(await resolveChatDetails(0, fullChatDetailsWithUnexpectedBody)).toBe(true);
    await panel.getByText("openai/gpt-6-luna", { exact: true }).waitFor({ state: "visible" });

    const contextText = await panel.getByRole("tabpanel").innerText();
    expect(contextText).toMatch(/模型|model/i);
    expect(contextText).toContain("openai/gpt-6-luna");
    expect(contextText).toMatch(/12[,，]?345/);
    expect(contextText).toMatch(/32[,，]?000/);
    expect(contextText).toContain("42.5");
    expect(contextText).toMatch(/来源|source/i);
    expect(contextText).toMatch(/实时|live/i);
    expect(contextText).toMatch(/采样|sampled/i);
    expect(contextText).toMatch(/2026[/-]11[/-]12/);
    expect(contextText).not.toContain(detailBodySentinel);

    await panel.getByRole("tab", { name: /关联|associations/i }).click();
    const associationText = await panel.getByRole("tabpanel").innerText();
    expect(associationText).toMatch(/PR|Pull Request/i);
    expect(associationText).toMatch(/MR|Merge Request/i);
    expect(associationText).toContain("Reduce chat panel state leakage");
    expect(associationText).toContain("In Progress");

    const prUrl = fullChatDetails.references.pr[0]!;
    const mrUrl = fullChatDetails.references.mr[0]!;
    const taskUrl = fullChatDetails.tasks[0]!.url!;
    const prLink = panel.locator(`a[href="${prUrl}"]`);
    const mrLink = panel.locator(`a[href="${mrUrl}"]`);
    const taskLink = panel.locator(`a[href="${taskUrl}"]`);
    expect(await prLink.isVisible()).toBe(true);
    expect(await mrLink.isVisible()).toBe(true);
    expect(await taskLink.isVisible()).toBe(true);
    expect(associationText).toContain("Unlinked task");
    expect(associationText).toContain("Queued");
    const unlinkedTask = panel.locator("[data-octobuddy-channel-status-task]").filter({ hasText: "Unlinked task" });
    await unlinkedTask.scrollIntoViewIfNeeded();
    expect(await unlinkedTask.isVisible()).toBe(true);
    expect(await panel.locator('a[href^="javascript:"]').count()).toBe(0);

    await page.route(taskUrl, (route) => route.fulfill({
      status: 200,
      contentType: "text/html",
      body: "fixture task destination",
    }));
    await taskLink.click({ noWaitAfter: true });
    await waitForOpenedUrl(taskUrl, openedUrls);
  }, onlineChannelStatus);
}, 15_000);

test("empty details are explicit and an offline service shows only its stale service snapshot", async () => {
  await withChatPanelPage(async ({ page, chatTargets, resolveChatStatus, waitForDetailsCount, resolveChatDetails }) => {
    await openPanel(page);
    await announceChat(page, threadTarget);
    expect(await chatTargets()).toEqual([threadTarget]);
    expect(await resolveChatStatus(0, onlineChatStatus(initialChat))).toBe(true);
    await page.locator(chatPanelRegion).getByRole("tab", { name: /上下文|context/i }).click();
    expect(await waitForDetailsCount(1)).toBeGreaterThanOrEqual(1);
    expect(await resolveChatDetails(0, { state: "online", snapshot: emptyChatDetails })).toBe(true);
    await page.getByText(emptyChatDetails.context.note, { exact: true }).waitFor({ state: "visible" });
    expect(await page.locator(chatPanelRegion).getByRole("tabpanel").innerText()).toMatch(/不可用|unavailable/i);

    await page.locator(chatPanelRegion).getByRole("tab", { name: /关联|associations/i }).click();
    const associationPanel = page.locator(chatPanelRegion);
    expect(await associationPanel.locator("a").count()).toBe(0);
    expect(await associationPanel.getByRole("tabpanel").innerText()).toMatch(/暂无|没有|无(?:关联|任务|PR|MR)|no (?:linked|confirmed|associated)/i);
  }, onlineChannelStatus);

  await withChatPanelPage(async ({ page, chatTargets, resolveChatStatus, waitForDetailsCount, resolveChatDetails, setChannelStatus }) => {
    await openPanel(page);
    await announceChat(page, threadTarget);
    expect(await chatTargets()).toEqual([threadTarget]);
    expect(await resolveChatStatus(0, onlineChatStatus(initialChat))).toBe(true);
    const panel = page.locator(chatPanelRegion);
    await panel.getByRole("tab", { name: /上下文|context/i }).click();
    expect(await waitForDetailsCount(1)).toBeGreaterThanOrEqual(1);
    expect(await resolveChatDetails(0, { state: "online", snapshot: fullChatDetails })).toBe(true);
    await panel.getByText("openai/gpt-6-luna", { exact: true }).waitFor({ state: "visible" });

    const nextTarget: ChatIdentity = { channelType: 2, channelId: "group-offline-current" };
    await announceChat(page, nextTarget);
    expect(await chatTargets()).toEqual([threadTarget, nextTarget]);
    const clearedText = await panel.getByRole("tabpanel").innerText();
    expect(clearedText).not.toContain("openai/gpt-6-luna");
    expect(clearedText).not.toContain("Reduce chat panel state leakage");

    expect(await setChannelStatus({ state: "offline", lastSuccess: emptyOfflineLastSuccess })).toBe(true);
    await page.getByRole("button", { name: /离线|offline/i }).waitFor({ state: "visible", timeout: 10_000 });
    const offlineContext = await panel.getByRole("tabpanel").innerText();
    expect(offlineContext).not.toContain("openai/gpt-6-luna");
    expect(offlineContext).not.toContain("Reduce chat panel state leakage");

    await panel.getByRole("tab", { name: /状态|status/i }).click();
    const staleText = await panel.getByRole("tabpanel").innerText();
    expect(staleText).toMatch(/离线|offline/i);
    expect(staleText).toMatch(/过期|stale|out of date|expired/i);
    expect(staleText).toContain(String(emptyOfflineLastSuccess.pid));
  }, onlineChannelStatus);
}, 30_000);

test("switching chats cancels pending details, clears prior associations and ignores a late reply", async () => {
  const firstTarget: ChatIdentity = { channelType: 2, channelId: "group-old" };
  const secondTarget: ChatIdentity = { channelType: 5, channelId: "group-new____thread-2" };
  const firstStatus: ChatSnapshot = {
    version: 1,
    sampledAt: "2026-11-12T10:40:00.000Z",
    ...firstTarget,
    state: "running",
    model: "openai/old-model",
    cwd: "/workspace/old",
    queuedTasks: 0,
    pendingQuestions: 0,
  };
  const secondStatus: ChatSnapshot = {
    version: 1,
    sampledAt: "2026-11-12T10:41:00.000Z",
    ...secondTarget,
    state: "idle",
    model: "openai/new-model",
    cwd: "/workspace/new",
    queuedTasks: 0,
    pendingQuestions: 0,
  };
  const previousDetails: ChatDetails = {
    ...fullChatDetails,
    channelType: firstTarget.channelType,
    channelId: firstTarget.channelId,
    context: { ...fullChatDetails.context, model: "openai/old-model" },
    references: { pr: ["https://github.com/acme/project/pull/7"], mr: [] },
    tasks: [{
      workspaceId: "workspace-old",
      taskId: "old-task",
      title: "Old chat confidential task",
      status: "Blocked",
      url: "https://example.com/workspaces/workspace-old/tasks/old-task",
    }],
  };
  const currentDetails: ChatDetails = {
    ...emptyChatDetails,
    channelType: secondTarget.channelType,
    channelId: secondTarget.channelId,
    context: {
      ...emptyChatDetails.context,
      source: "live",
      usedTokens: 900,
      contextWindow: 16000,
      percent: 5.6,
      model: "openai/new-model",
    },
    references: { pr: ["https://github.com/acme/project/pull/88"], mr: [] },
    tasks: [{
      workspaceId: "workspace-new",
      taskId: "new-task",
      title: "Current chat task",
      status: "In Progress",
      url: "https://example.com/workspaces/workspace-new/tasks/new-task",
    }],
  };

  await withChatPanelPage(async ({ page, chatTargets, resolveChatStatus, detailsRequests, cancelledDetailRequestIds, waitForDetailsCount, resolveChatDetails }) => {
    await openPanel(page);
    await announceChat(page, firstTarget);
    expect(await chatTargets()).toEqual([firstTarget]);
    expect(await resolveChatStatus(0, onlineChatStatus(firstStatus))).toBe(true);
    await page.locator(chatPanelRegion).getByRole("tab", { name: /上下文|context/i }).click();
    expect(await waitForDetailsCount(1)).toBeGreaterThanOrEqual(1);

    const firstRequest = (await detailsRequests())[0]!;
    expect(firstRequest.target).toEqual(firstTarget);
    await announceChat(page, secondTarget);
    expect(await chatTargets()).toEqual([firstTarget, secondTarget]);
    expect(await cancelledDetailRequestIds()).toContain(firstRequest.requestId);
    const clearedPanelText = await page.locator(chatPanelRegion).getByRole("tabpanel").innerText();
    expect(clearedPanelText).not.toContain("openai/old-model");
    expect(clearedPanelText).not.toContain("Old chat confidential task");
    expect(clearedPanelText).not.toContain("https://github.com/acme/project/pull/7");

    expect(await resolveChatStatus(1, onlineChatStatus(secondStatus))).toBe(true);
    expect(await waitForDetailsCount(2)).toBeGreaterThanOrEqual(2);
    expect(await resolveChatDetails(0, { state: "online", snapshot: previousDetails })).toBe(true);
    await settleStatusRendering(page);
    expect(await page.locator(chatPanelRegion).getByRole("tabpanel").innerText()).not.toContain("Old chat confidential task");

    expect(await resolveChatDetails(1, { state: "online", snapshot: currentDetails })).toBe(true);
    await page.getByText("openai/new-model", { exact: true }).waitFor({ state: "visible" });
    const panel = page.locator(chatPanelRegion);
    expect(await panel.getByRole("tabpanel").innerText()).toContain("openai/new-model");
    await panel.getByRole("tab", { name: /关联|associations/i }).click();
    const currentPanelText = await panel.getByRole("tabpanel").innerText();
    expect(currentPanelText).toContain("Current chat task");
    expect(currentPanelText).not.toContain("Old chat confidential task");
    expect(currentPanelText).not.toContain("openai/old-model");
  }, onlineChannelStatus);
}, 15_000);

test("the Agent Hub page renders its tree, activity, and complete conversation across wide and narrow layouts", async () => {
  const completeMessage = `${"Full Agent Hub conversation text. ".repeat(120)}FULL_AGENT_HUB_CONVERSATION_END`;
  const snapshot: AgentHubSnapshot = {
    version: 1,
    ...threadTarget,
    agents: [
      {
        id: "run-main",
        parentId: null,
        agent: "coordinator",
        status: "running",
        description: "Coordinates the current request",
        activity: [{ type: "tool", summary: "Started the chat task" }],
        messages: [{ role: "assistant", content: "I will delegate the research." }],
      },
      {
        id: "run-researcher",
        parentId: "run-main",
        agent: "researcher",
        status: "completed",
        description: "Read the linked implementation",
        activity: [{ type: "tool", summary: "AGENT_HUB_ACTIVITY_SENTINEL" }],
        messages: [
          { role: "user", content: "Inspect the current endpoint." },
          { role: "assistant", content: completeMessage },
        ],
      },
    ],
  };

  await withChatPanelPage(async ({ page, resolveChatStatus, agentHubRequests, waitForAgentHubCount, resolveAgentHub }) => {
    await openPanel(page);
    await announceChat(page, threadTarget);
    expect(await resolveChatStatus(0, onlineChatStatus(initialChat))).toBe(true);

    const panel = page.locator(chatPanelRegion);
    await panel.getByRole("tab", { name: /Agent Hub|智能体/i }).click();
    expect(await waitForAgentHubCount(1)).toBeGreaterThanOrEqual(1);
    expect(await agentHubRequests()).toEqual([threadTarget]);
    expect(await resolveAgentHub(0, { state: "online", snapshot })).toBe(true);

    const hub = page.locator("[data-octobuddy-agent-hub]");
    await hub.waitFor({ state: "visible" });
    const tree = hub.locator("[data-octobuddy-agent-hub-tree]");
    const items = tree.getByRole("treeitem");
    await items.nth(1).waitFor({ state: "visible" });
    expect(await items.count()).toBe(2);
    expect(await items.nth(0).getAttribute("aria-level")).toBe("1");
    expect(await items.nth(1).getAttribute("aria-level")).toBe("2");
    await items.nth(1).click();

    const details = hub.locator("[data-octobuddy-agent-hub-detail]");
    await details.waitFor({ state: "visible" });
    const detailsText = await details.innerText();
    expect(detailsText).toContain("researcher");
    expect(detailsText).toContain("completed");
    expect(detailsText).toContain("Read the linked implementation");
    expect(detailsText).toContain("AGENT_HUB_ACTIVITY_SENTINEL");
    expect(detailsText).toContain("Inspect the current endpoint.");
    expect(detailsText).toContain(completeMessage);

    const treeBox = await tree.boundingBox();
    const detailBox = await details.boundingBox();
    expect(treeBox).not.toBeNull();
    expect(detailBox).not.toBeNull();
    if (treeBox && detailBox) expect(treeBox.x + treeBox.width).toBeLessThanOrEqual(detailBox.x + 2);

    await page.setViewportSize({ width: 360, height: 640 });
    await hub.getByRole("button", { name: /Agent list|Back to list|智能体列表|返回列表/i }).click();
    expect(await tree.isVisible()).toBe(true);
    expect(await details.isVisible()).toBe(false);
    await items.nth(1).click();
    expect(await tree.isVisible()).toBe(false);
    expect(await details.isVisible()).toBe(true);
    expect(await details.innerText()).toContain(completeMessage);
  }, onlineChannelStatus);
}, 15_000);

test("Agent Hub renders OMP transcript text blocks naturally and retains complete tool calls and metadata", async () => {
  const firstText = "I will inspect the worker lease before answering.";
  const finalText = `The lease closes only its owned worker. ${"OMP transcript continuation. ".repeat(80)}OMP_TRANSCRIPT_TEXT_END`;
  const toolArgument = `${"inspect current implementation; ".repeat(80)}OMP_TOOL_ARGUMENT_END`;
  const messageMetadata = `${"persisted transcript metadata. ".repeat(80)}OMP_MESSAGE_METADATA_END`;
  const snapshot: AgentHubSnapshot = {
    version: 1,
    ...threadTarget,
    agents: [{
      id: "run-omp-transcript",
      parentId: null,
      agent: "worker",
      status: "completed",
      description: "Reads the worker lease implementation",
      activity: [],
      messages: [{
        role: "assistant",
        content: [
          { type: "text", text: firstText },
          {
            type: "toolCall",
            id: "call-omp-agent-hub-1",
            name: "read",
            arguments: { path: "src/runtime/worker-lease.ts", query: toolArgument },
          },
          { type: "text", text: finalText },
        ],
        api: "openai-responses",
        provider: "openai",
        model: "gpt-6-luna",
        timestamp: 1790000000000,
        stopReason: "toolUse",
        usage: { input: 412, output: 63, cacheRead: 81, cacheWrite: 0 },
        audit: messageMetadata,
      }],
    }],
  };

  await withChatPanelPage(async ({ page, resolveChatStatus, waitForAgentHubCount, resolveAgentHub }) => {
    await openPanel(page);
    await announceChat(page, threadTarget);
    expect(await resolveChatStatus(0, onlineChatStatus(initialChat))).toBe(true);
    const panel = page.locator(chatPanelRegion);
    await panel.getByRole("tab", { name: /Agent Hub|智能体/i }).click();
    expect(await waitForAgentHubCount(1)).toBeGreaterThanOrEqual(1);
    expect(await resolveAgentHub(0, { state: "online", snapshot })).toBe(true);

    const message = page.locator("[data-octobuddy-agent-hub-message]").first();
    const content = message.locator("[data-octobuddy-agent-hub-message-content]");
    await content.getByText("OMP_TRANSCRIPT_TEXT_END").waitFor({ state: "visible" });
    const bodyText = await content.innerText();
    expect(bodyText).toContain(firstText);
    expect(bodyText).toContain(finalText);
    expect(bodyText).not.toMatch(/"type"\s*:\s*"text"/);

    const renderedMessage = await message.innerText();
    expect(renderedMessage).toContain("call-omp-agent-hub-1");
    expect(renderedMessage).toContain("src/runtime/worker-lease.ts");
    expect(renderedMessage).toContain(toolArgument);
    expect(renderedMessage).toContain("openai-responses");
    expect(renderedMessage).toContain("gpt-6-luna");
    expect(renderedMessage).toContain("stopReason");
    expect(renderedMessage).toContain("toolUse");
    expect(renderedMessage).toContain("cacheWrite");
    expect(renderedMessage).toContain(messageMetadata);
  }, onlineChannelStatus);
}, 15_000);

test("Agent Hub distinguishes a chat with no agents from a failed data read", async () => {
  const emptyTarget: ChatIdentity = { channelType: 2, channelId: "group-without-agents" };
  const failedTarget: ChatIdentity = { channelType: 5, channelId: "group____agent-hub-offline" };
  const emptySnapshot: AgentHubSnapshot = { version: 1, ...emptyTarget, agents: [] };
  const noAgents = /no agents|暂无[ \t\n]*(?:agent|智能体)|没有(?:可用的)?[ \t\n]*(?:agent|智能体)/i;
  const readFailure = /读取失败|无法读取|读取错误|failed|failure|error|offline|unavailable|暂不可用/i;

  await withChatPanelPage(async ({ page, resolveChatStatus, agentHubRequests, waitForAgentHubCount, resolveAgentHub }) => {
    await openPanel(page);
    await announceChat(page, emptyTarget);
    expect(await resolveChatStatus(0, onlineChatStatus({ ...initialChat, ...emptyTarget }))).toBe(true);
    const panel = page.locator(chatPanelRegion);
    await panel.getByRole("tab", { name: /Agent Hub|智能体/i }).click();
    expect(await waitForAgentHubCount(1)).toBeGreaterThanOrEqual(1);
    expect(await resolveAgentHub(0, { state: "online", snapshot: emptySnapshot })).toBe(true);
    await page.getByText(noAgents).waitFor({ state: "visible" });
    expect(await panel.getByRole("tabpanel").innerText()).toMatch(noAgents);

    await announceChat(page, failedTarget);
    expect(await waitForAgentHubCount(2)).toBeGreaterThanOrEqual(2);
    expect(await agentHubRequests()).toEqual([emptyTarget, failedTarget]);
    const hub = page.locator("[data-octobuddy-agent-hub]");
    expect(await hub.innerText()).not.toMatch(noAgents);
    expect(await resolveAgentHub(1, { state: "offline" })).toBe(true);
    await page.getByText(readFailure).waitFor({ state: "visible" });
    const failedText = await hub.innerText();
    expect(failedText).toMatch(readFailure);
    expect(failedText).not.toMatch(noAgents);
  }, onlineChannelStatus);
}, 15_000);

test("Agent Hub refreshes the current chat until a running agent completes", async () => {
  const target: ChatIdentity = { channelType: 2, channelId: "group-agent-hub-refresh" };
  const runningSnapshot: AgentHubSnapshot = {
    version: 1,
    ...target,
    agents: [{
      id: "run-refresh",
      parentId: null,
      agent: "worker",
      status: "running",
      description: "Working on the current request",
      activity: [{ type: "tool", summary: "Started refresh fixture" }],
      messages: [{ role: "assistant", content: "RUNNING_AGENT_HUB_REFRESH_SENTINEL" }],
    }],
  };
  const completedSnapshot: AgentHubSnapshot = {
    version: 1,
    ...target,
    agents: [{
      id: "run-refresh",
      parentId: null,
      agent: "worker",
      status: "completed",
      description: "Finished the current request",
      activity: [{ type: "tool", summary: "Completed refresh fixture" }],
      messages: [{ role: "assistant", content: "COMPLETED_AGENT_HUB_REFRESH_SENTINEL" }],
    }],
  };

  await withChatPanelPage(async ({
    page,
    resolveChatStatus,
    agentHubRequests,
    waitForAgentHubCount,
    resolveAgentHub,
  }) => {
    await openPanel(page);
    await announceChat(page, target);
    expect(await resolveChatStatus(0, onlineChatStatus({ ...initialChat, ...target }))).toBe(true);

    const panel = page.locator(chatPanelRegion);
    await panel.getByRole("tab", { name: /Agent Hub|智能体/i }).click();
    expect(await waitForAgentHubCount(1)).toBeGreaterThanOrEqual(1);
    expect(await resolveAgentHub(0, { state: "online", snapshot: runningSnapshot })).toBe(true);

    const hub = page.locator("[data-octobuddy-agent-hub]");
    await page.getByText("RUNNING_AGENT_HUB_REFRESH_SENTINEL", { exact: true }).waitFor({ state: "visible" });
    expect(await waitForAgentHubCount(2, 8_000)).toBeGreaterThanOrEqual(2);
    expect(await agentHubRequests()).toEqual([target, target]);
    expect(await hub.innerText()).toContain("RUNNING_AGENT_HUB_REFRESH_SENTINEL");
    expect(await hub.innerText()).not.toContain("正在读取当前聊天的 Agent Hub");

    expect(await resolveAgentHub(1, { state: "online", snapshot: completedSnapshot })).toBe(true);
    await page.getByText("COMPLETED_AGENT_HUB_REFRESH_SENTINEL", { exact: true }).waitFor({ state: "visible" });
    const refreshedText = await hub.innerText();
    expect(refreshedText).toContain("completed");
    expect(refreshedText).toContain("COMPLETED_AGENT_HUB_REFRESH_SENTINEL");
    expect(refreshedText).not.toContain("RUNNING_AGENT_HUB_REFRESH_SENTINEL");
  }, onlineChannelStatus);
}, 15_000);

test("switching chats requests the new Agent Hub and ignores a late response from the previous chat", async () => {
  const previousTarget: ChatIdentity = { channelType: 2, channelId: "group-agent-hub-before-switch" };
  const currentTarget: ChatIdentity = { channelType: 5, channelId: "group____agent-hub-after-switch" };
  const previousSnapshot: AgentHubSnapshot = {
    version: 1,
    ...previousTarget,
    agents: [{
      id: "old-run",
      parentId: null,
      agent: "old-agent",
      status: "running",
      description: "Previous chat data",
      activity: [],
      messages: [{ role: "assistant", content: "OLD_AGENT_HUB_RESPONSE_SENTINEL" }],
    }],
  };
  const currentSnapshot: AgentHubSnapshot = {
    version: 1,
    ...currentTarget,
    agents: [{
      id: "new-run",
      parentId: null,
      agent: "current-agent",
      status: "running",
      description: "Current chat data",
      activity: [],
      messages: [{ role: "assistant", content: "CURRENT_AGENT_HUB_RESPONSE_SENTINEL" }],
    }],
  };

  await withChatPanelPage(async ({ page, resolveChatStatus, agentHubRequests, waitForAgentHubCount, resolveAgentHub }) => {
    await openPanel(page);
    await announceChat(page, previousTarget);
    expect(await resolveChatStatus(0, onlineChatStatus({ ...initialChat, ...previousTarget }))).toBe(true);
    const panel = page.locator(chatPanelRegion);
    await panel.getByRole("tab", { name: /Agent Hub|智能体/i }).click();
    expect(await waitForAgentHubCount(1)).toBeGreaterThanOrEqual(1);

    await announceChat(page, currentTarget);
    expect(await waitForAgentHubCount(2)).toBeGreaterThanOrEqual(2);
    expect(await agentHubRequests()).toEqual([previousTarget, currentTarget]);
    const hub = page.locator("[data-octobuddy-agent-hub]");
    expect(await hub.innerText()).not.toContain("OLD_AGENT_HUB_RESPONSE_SENTINEL");

    expect(await resolveAgentHub(1, { state: "online", snapshot: currentSnapshot })).toBe(true);
    await page.getByText("CURRENT_AGENT_HUB_RESPONSE_SENTINEL", { exact: true }).waitFor({ state: "visible" });
    expect(await resolveAgentHub(0, { state: "online", snapshot: previousSnapshot })).toBe(true);
    await settleStatusRendering(page);
    const currentText = await hub.innerText();
    expect(currentText).toContain("current-agent");
    expect(currentText).toContain("CURRENT_AGENT_HUB_RESPONSE_SENTINEL");
    expect(currentText).not.toContain("old-agent");
    expect(currentText).not.toContain("OLD_AGENT_HUB_RESPONSE_SENTINEL");
  }, onlineChannelStatus);
}, 15_000);
