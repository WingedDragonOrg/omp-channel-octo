import { mkdtemp, mkdir, readFile, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { pathToFileURL } from "node:url";
import { expect, test } from "bun:test";
import { chromium, type Browser, type Page } from "playwright";

type OpenChannel = { channelID: string; channelType: 1 | 2 | 5 };
type ChatEvent = {
  type: "octobuddy-enhancer:chat";
  channelType: OpenChannel["channelType"] | null;
  channelId: string | null;
};
type ObserverPage = Window & {
  __octobuddyObserverMessages: ChatEvent[];
  __octobuddyObserverStore: { openChannel: OpenChannel | null };
  __octobuddyObserverTimerClears: number;
  __octobuddyStatusNavigation: { dispose(): void };
};

const navigationSourcePath = new URL("../plugins/channel-status/navigation.js", import.meta.url);
const chatEventType = "octobuddy-enhancer:chat";
const chatRequestType = "octobuddy-enhancer:chat-request";

async function readNavigationSource(): Promise<string> {
  try {
    return await readFile(navigationSourcePath, "utf8");
  } catch (error) {
    if (typeof error === "object" && error !== null && "code" in error && error.code === "ENOENT") {
      expect(false, `Expected production navigation observer at ${navigationSourcePath.pathname}`).toBe(true);
      return "";
    }
    throw error;
  }
}

async function waitForMessages(page: Page, count: number): Promise<void> {
  await page.waitForFunction((expectedCount) => {
    const messages = (window as unknown as ObserverPage).__octobuddyObserverMessages;
    return Array.isArray(messages) && messages.length >= expectedCount;
  }, count);
}

async function observedMessages(page: Page): Promise<ChatEvent[]> {
  return await page.evaluate(() => (window as unknown as ObserverPage).__octobuddyObserverMessages);
}

function chatEvent(channel: OpenChannel | null): ChatEvent {
  return {
    type: chatEventType,
    channelType: channel?.channelType ?? null,
    channelId: channel?.channelID ?? null,
  };
}

test("the injected navigation observer publishes active chat changes and stops after disposal", async () => {
  const source = await readNavigationSource();
  const root = await mkdtemp(join(tmpdir(), "octobuddy-chat-observer-"));
  const appDirectory = join(root, "OctoBuddy.app", "Contents", "Resources", "communication-renderer");
  const assetsDirectory = join(appDirectory, "assets");
  const pagePath = join(appDirectory, "index.html");
  let browser: Browser | undefined;

  try {
    await mkdir(assetsDirectory, { recursive: true });
    await writeFile(pagePath, `<!doctype html>
<html><head><meta charset="utf-8"><title>OctoBuddy chat observer fixture</title>
<link rel="modulepreload" href="./assets/App-fixture.js"></head><body><main>Conversation</main></body></html>`, "utf8");
    await writeFile(join(assetsDirectory, "App-fixture.js"), `
const shared = { openChannel: { channelID: "initial-group", channelType: 2 } };
window.__octobuddyObserverStore = shared;
export const i = { shared };
`, "utf8");

    browser = await chromium.launch({
      headless: true,
      args: ["--allow-file-access-from-files", "--no-first-run", "--no-default-browser-check"],
    });
    const page = await browser.newPage();
    page.setDefaultTimeout(5_000);
    await page.goto(pathToFileURL(pagePath).href, { waitUntil: "load" });
    await page.evaluate(() => {
      const observerPage = window as unknown as ObserverPage;
      observerPage.__octobuddyObserverMessages = [];
      observerPage.__octobuddyObserverTimerClears = 0;
      const originalClearInterval = window.clearInterval.bind(window);
      const originalClearTimeout = window.clearTimeout.bind(window);
      window.clearInterval = ((timer) => {
        observerPage.__octobuddyObserverTimerClears += 1;
        originalClearInterval(timer as number);
      }) as typeof window.clearInterval;
      window.clearTimeout = ((timer) => {
        observerPage.__octobuddyObserverTimerClears += 1;
        originalClearTimeout(timer as number);
      }) as typeof window.clearTimeout;
      window.addEventListener("message", (event) => {
        if (event.source === window && event.data?.type === "octobuddy-enhancer:chat") {
          observerPage.__octobuddyObserverMessages.push(event.data as ChatEvent);
        }
      });
    });

    await page.addScriptTag({ content: source });
    await waitForMessages(page, 1);
    expect(await observedMessages(page)).toEqual([chatEvent({ channelID: "initial-group", channelType: 2 })]);
    await page.addScriptTag({ content: source });
    const timerClearsBeforeDispose = await page.evaluate(() => (window as unknown as ObserverPage).__octobuddyObserverTimerClears);

    const changedChannel = { channelID: "changed-thread", channelType: 5 } as const;
    await page.evaluate((channel) => {
      (window as unknown as ObserverPage).__octobuddyObserverStore.openChannel = channel;
    }, changedChannel);
    await waitForMessages(page, 2);
    expect(await observedMessages(page)).toEqual([
      chatEvent({ channelID: "initial-group", channelType: 2 }),
      chatEvent(changedChannel),
    ]);

    await page.evaluate(() => {
      (window as unknown as ObserverPage).__octobuddyObserverStore.openChannel = null;
    });
    await waitForMessages(page, 3);
    expect(await observedMessages(page)).toEqual([
      chatEvent({ channelID: "initial-group", channelType: 2 }),
      chatEvent(changedChannel),
      chatEvent(null),
    ]);

    await page.evaluate((type) => window.postMessage({ type }, "*"), chatRequestType);
    await waitForMessages(page, 4);
    expect(await observedMessages(page)).toEqual([
      chatEvent({ channelID: "initial-group", channelType: 2 }),
      chatEvent(changedChannel),
      chatEvent(null),
      chatEvent(null),
    ]);

    await page.evaluate(() => (window as unknown as ObserverPage).__octobuddyStatusNavigation.dispose());
    expect(await page.evaluate(() => (window as unknown as ObserverPage).__octobuddyObserverTimerClears)).toBeGreaterThan(timerClearsBeforeDispose);
    await page.evaluate(() => {
      const observerPage = window as unknown as ObserverPage;
      observerPage.__octobuddyObserverStore.openChannel = { channelID: "after-dispose", channelType: 1 };
      window.postMessage({ type: "octobuddy-enhancer:chat-request" }, "*");
    });
    await page.waitForTimeout(800);
    expect(await observedMessages(page)).toEqual([
      chatEvent({ channelID: "initial-group", channelType: 2 }),
      chatEvent(changedChannel),
      chatEvent(null),
      chatEvent(null),
    ]);
  } finally {
    try {
      await browser?.close();
    } finally {
      await rm(root, { recursive: true, force: true });
    }
  }
}, 15_000);
