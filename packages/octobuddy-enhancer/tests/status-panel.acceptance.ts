import { existsSync } from "node:fs";
import { mkdtemp, mkdir, readFile, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { dirname, join } from "node:path";
import { pathToFileURL } from "node:url";
import { expect, test } from "bun:test";
import { chromium, type BrowserContext, type CDPSession, type Page } from "playwright";

type StatusSnapshot = {
  version: 1;
  pid: number;
  botUid: string | null;
  ownerUid: string | null;
  runningSessions: number;
  queuedTasks: number;
  sampledAt: string;
};

type ChannelStatus =
  | { state: "online"; snapshot: StatusSnapshot }
  | { state: "unconfigured" | "offline" | "unauthorized" | "untrusted"; lastSuccess?: StatusSnapshot };

interface StatusPanelFixture {
  page: Page;
  statusCalls(): Promise<number>;
  dispose(): Promise<number>;
}

const contentScriptPath = new URL("../plugins/channel-status/content.js", import.meta.url);
const isolatedWorldName = "octobuddy-channel-status-test-world";

async function withStatusPanelPage(
  initialStatus: ChannelStatus,
  exercise: (fixture: StatusPanelFixture) => Promise<void>,
): Promise<void> {
  expect(existsSync(contentScriptPath)).toBe(true);
  const pluginSource = await readFile(contentScriptPath, "utf8");

  const root = await mkdtemp(join(tmpdir(), "octobuddy-channel-status-"));
  let context: BrowserContext | undefined;
  try {
    const profileDir = join(root, "profile");
    const targetPath = join(root, "OctoBuddy.app", "Contents", "Resources", "communication-renderer", "index.html");
    await Promise.all([
      mkdir(profileDir, { recursive: true }),
      mkdir(dirname(targetPath), { recursive: true }),
    ]);
    await writeFile(targetPath, "<!doctype html><html><head><meta charset=\"utf-8\"><title>OctoBuddy status fixture</title></head><body><main>Conversation</main></body></html>", "utf8");

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
    await evaluateInWorld(cdp, contextId, bridgeBootstrap(initialStatus));
    await evaluateInWorld(cdp, contextId, pluginSource);

    const readBridge = async <T>(expression: string): Promise<T> => {
      const response = await cdp.send("Runtime.evaluate", {
        expression,
        contextId,
        awaitPromise: true,
        returnByValue: true,
      }) as { result?: { value?: T }; exceptionDetails?: { text?: string } };
      if (response.exceptionDetails) throw new Error(response.exceptionDetails.text ?? "channel status fixture evaluation failed");
      return response.result?.value as T;
    };

    await exercise({
      page,
      statusCalls: () => readBridge("window.__channelStatusTestBridge.statusCalls"),
      dispose: () => readBridge(`(async () => {
        const callbacks = window.__channelStatusTestBridge.disposeCallbacks.splice(0);
        for (const callback of callbacks) await callback();
        return callbacks.length;
      })()`),
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

function bridgeBootstrap(initialStatus: ChannelStatus): string {
  return `(() => {
    let status = ${JSON.stringify(initialStatus)};
    let statusCalls = 0;
    const disposeCallbacks = [];
    Object.defineProperty(window, "__channelStatusTestBridge", {
      configurable: false,
      value: { disposeCallbacks, get statusCalls() { return statusCalls; } },
    });
    Object.defineProperty(window, "octoEnhancer", {
      configurable: false,
      value: Object.freeze({
        getChannelStatus: async () => {
          statusCalls += 1;
          return structuredClone(status);
        },
        onDispose: (callback) => {
          if (typeof callback !== "function") throw new TypeError("onDispose requires a callback");
          disposeCallbacks.push(callback);
        },
      }),
    });
  })()`;
}

const onlineSnapshot: StatusSnapshot = {
  version: 1,
  pid: 47291,
  botUid: "bot-17",
  ownerUid: "owner-17",
  runningSessions: 6,
  queuedTasks: 3,
  sampledAt: "2026-11-11T10:14:30.000Z",
};

test("channel status starts compact and expands and collapses with the live service snapshot", async () => {
  await withStatusPanelPage({ state: "online", snapshot: onlineSnapshot }, async ({ page, statusCalls }) => {
    const control = page.getByRole("button", { name: /状态|status/i });
    await control.waitFor({ state: "visible" });
    expect(await statusCalls()).toBeGreaterThan(0);
    expect(await control.getAttribute("aria-expanded")).toBe("false");

    const box = await control.boundingBox();
    expect(box).not.toBeNull();
    if (box) {
      expect(box.width).toBeLessThan(320);
      expect(box.height).toBeLessThan(80);
    }
    expect(await page.getByRole("dialog").count()).toBe(0);
    expect(await page.getByText(/运行中会话|running sessions/i).isVisible()).toBe(false);

    await control.click();
    expect(await control.getAttribute("aria-expanded")).toBe("true");
    await page.getByText(/运行中会话|running sessions/i).waitFor({ state: "visible" });
    const expandedText = await page.getByRole("region", { name: /频道状态详情/ }).innerText();
    expect(expandedText).toMatch(/在线|online/i);
    expect(expandedText).toMatch(/pid\s*:?\s*47291/i);
    expect(expandedText).toMatch(/(?:运行中会话|running sessions)\s*[：:]?\s*6/i);
    expect(expandedText).toMatch(/(?:排队任务|queued tasks)\s*[：:]?\s*3/i);

    await control.click();
    expect(await control.getAttribute("aria-expanded")).toBe("false");
    expect(await page.getByText(/运行中会话|running sessions/i).isVisible()).toBe(false);
  });
}, 15_000);

test("an offline response with a last successful snapshot is visibly marked stale", async () => {
  await withStatusPanelPage({
    state: "offline",
    lastSuccess: {
      version: 1,
      pid: 68143,
      botUid: "bot-17",
      ownerUid: "owner-17",
      runningSessions: 2,
      queuedTasks: 5,
      sampledAt: "2026-11-10T18:20:00.000Z",
    },
  }, async ({ page }) => {
    const control = page.getByRole("button", { name: /状态|status/i });
    await control.waitFor({ state: "visible" });
    await control.click();
    await page.getByRole("region", { name: /频道状态详情/ }).getByText(/离线|offline/i).waitFor({ state: "visible" });

    const panelText = await page.getByRole("region", { name: /频道状态详情/ }).innerText();
    expect(panelText).toMatch(/过期|stale|out of date|expired/i);
    expect(panelText).toContain("68143");
  });
}, 15_000);

test("plugin disposal removes the channel status control from the page", async () => {
  await withStatusPanelPage({ state: "online", snapshot: onlineSnapshot }, async ({ page, dispose }) => {
    const control = page.getByRole("button", { name: /状态|status/i });
    await control.waitFor({ state: "visible" });

    expect(await dispose()).toBeGreaterThan(0);
    await control.waitFor({ state: "detached" });
    expect(await page.getByRole("button", { name: /状态|status/i }).count()).toBe(0);
  });
}, 15_000);
