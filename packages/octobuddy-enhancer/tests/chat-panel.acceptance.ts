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
  queuedTasks: number;
  pendingQuestions: number;
};

type ChatStatus = { state: "online"; snapshot: ChatSnapshot };

interface ChatPanelFixture {
  page: Page;
  chatTargets(): Promise<ChatIdentity[]>;
  cancelledTargets(): Promise<ChatIdentity[]>;
  resolveChatStatus(index: number, status: ChatStatus): Promise<boolean>;
}

const contentScriptPath = new URL("../plugins/channel-status/content.js", import.meta.url);
const isolatedWorldName = "octobuddy-chat-panel-test-world";
const chatPanelButton = '[data-octobuddy-channel-status-button]';
const chatPanelRegion = '[data-octobuddy-channel-status-panel]';

async function withChatPanelPage(exercise: (fixture: ChatPanelFixture) => Promise<void>): Promise<void> {
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
    await page.goto(pathToFileURL(targetPath).href, { waitUntil: "load" });

    const frameTree = await cdp.send("Page.getFrameTree") as { frameTree: { frame: { id: string } } };
    const isolated = await cdp.send("Page.createIsolatedWorld", {
      frameId: frameTree.frameTree.frame.id,
      worldName: isolatedWorldName,
      grantUniveralAccess: false,
    }) as { executionContextId: number };
    const contextId = isolated.executionContextId;
    await evaluateInWorld(cdp, contextId, bridgeBootstrap);
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
      resolveChatStatus: (index, status) => readBridge(`window.__chatPanelTestBridge.resolve(${index}, ${JSON.stringify(status)})`),
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

const bridgeBootstrap = `(() => {
  const requests = [];
  const cancelled = [];
  const disposeCallbacks = [];
  const serviceStatus = {
    state: "online",
    snapshot: {
      version: 1,
      pid: 47291,
      botUid: "bot-17",
      ownerUid: "owner-17",
      runningSessions: 1,
      queuedTasks: 0,
      sampledAt: "2026-11-11T10:14:30.000Z"
    }
  };
  Object.defineProperty(window, "__chatPanelTestBridge", {
    configurable: false,
    value: {
      requests,
      cancelled,
      resolve(index, status) {
        const request = requests[index];
        if (!request || !request.resolve) return false;
        const resolve = request.resolve;
        request.resolve = undefined;
        resolve(structuredClone(status));
        return true;
      }
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
      onDispose: (callback) => {
        if (typeof callback !== "function") throw new TypeError("onDispose requires a callback");
        disposeCallbacks.push(callback);
      }
    })
  });
})()`;

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

