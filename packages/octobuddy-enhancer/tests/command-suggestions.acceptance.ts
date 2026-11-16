import { expect, test } from "bun:test";
import { existsSync } from "node:fs";
import { mkdtemp, mkdir, readFile, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { dirname, join } from "node:path";
import { pathToFileURL } from "node:url";
import { chromium, type Browser, type Page } from "playwright";

type ChannelType = 1 | 2 | 5;
type ChatTarget = { channelType: ChannelType; channelId: string };

type CommandSubcommand = { name: string; description?: string; hint?: string };
type Command = {
  name: string;
  source: "channel" | "omp";
  description?: string;
  hint?: string;
  aliases?: string[];
  subcommands?: CommandSubcommand[];
};

type CommandsSnapshot = {
  version: 1;
  channelType: ChannelType;
  channelId: string;
  commands: Command[];
  omp: "available" | "unavailable";
};

type CommandsResult =
  | { state: "online"; snapshot: CommandsSnapshot }
  | { state: "unconfigured" | "offline" | "unauthorized" | "invalid"; lastSuccess?: CommandsSnapshot }
  | { state: "not_found" };

type ChannelStatusResult =
  | { state: "online"; snapshot: { botUid: string | null; ownerUid: string | null } }
  | { state: "unconfigured" | "offline" | "unauthorized" | "invalid" };

type TestState = {
  commandsCalls: { target: ChatTarget; requestId: number }[];
  cancelCalls: number[];
  channelStatusCalls: number;
  sends: number;
  disposed: boolean;
  dispose?: () => void;
};

type TestWindow = Window & {
  __octoTestState: TestState;
  __octoTestGetCommands: (target: ChatTarget, requestId: number) => Promise<CommandsResult>;
  __octoTestGetChannelStatus: () => Promise<ChannelStatusResult>;
  octoEnhancer?: unknown;
};

interface HarnessOptions {
  commands?: Command[];
  omp?: "available" | "unavailable";
  getCommands?: (target: ChatTarget, requestId: number) => CommandsResult | Promise<CommandsResult>;
  getChannelStatus?: () => ChannelStatusResult | Promise<ChannelStatusResult>;
}

interface Harness {
  page: Page;
  state(): Promise<TestState>;
  cleanup(): Promise<void>;
}

const editorSelector = '.wk-messageinput-editor [contenteditable="true"]';
const rootSelector = "[data-octobuddy-command-suggestions-root]";
const itemSelector = "[data-octobuddy-command-suggestions-item]";
const noteSelector = "[data-octobuddy-command-suggestions-note]";
const scriptUrl = new URL("../plugins/channel-status/command-suggestions.js", import.meta.url);

const defaultCommands: Command[] = [
  { name: "status", source: "channel", description: "查看当前状态" },
  { name: "switch", source: "channel", description: "切换模型" },
];

function fixtureHtml(): string {
  return `<!doctype html>
<html><head><meta charset="utf-8"><title>Octo command suggestions fixture</title>
<style>
html, body { margin: 0; height: 100%; }
.wk-messageinput-editor { position: fixed; left: 180px; top: 420px; width: 520px; }
.wk-messageinput-editor .tiptap.ProseMirror { min-height: 22px; outline: none; font: 14px/22px -apple-system, BlinkMacSystemFont, sans-serif; white-space: pre-wrap; }
.wk-messageinput-editor .mention { color: #6c56c9; }
</style></head><body>
<div class="wk-chat-conversation">
  <div class="wk-messageinput-editor">
    <div class="tiptap ProseMirror" contenteditable="true" role="textbox" aria-label="消息输入"></div>
  </div>
</div>
</body></html>`;
}

function defaultResult(target: ChatTarget, options: HarnessOptions): CommandsResult {
  return {
    state: "online",
    snapshot: {
      version: 1,
      channelType: target.channelType,
      channelId: target.channelId,
      commands: options.commands ?? defaultCommands,
      omp: options.omp ?? "available",
    },
  };
}

async function waitFor(predicate: () => Promise<boolean>, description: string, timeoutMs = 3_000): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    if (await predicate()) return;
    await Bun.sleep(20);
  }
  if (await predicate()) return;
  throw new Error(`Timed out waiting for ${description}`);
}

async function stateOf(page: Page): Promise<TestState> {
  return await page.evaluate(() => {
    const state = (window as unknown as TestWindow).__octoTestState;
    return {
      commandsCalls: state.commandsCalls,
      cancelCalls: state.cancelCalls,
      channelStatusCalls: state.channelStatusCalls,
      sends: state.sends,
      disposed: state.disposed,
    };
  });
}

async function menuVisible(page: Page): Promise<boolean> {
  return await page.locator(`${rootSelector}:not([hidden])`).count() === 1;
}

async function menuTextCount(page: Page, text: string): Promise<number> {
  return await page.locator(rootSelector).getByText(text, { exact: true }).count();
}

async function waitForMenu(page: Page): Promise<void> {
  await waitFor(() => menuVisible(page), "the command suggestion menu");
}

async function itemCount(page: Page): Promise<number> {
  return await page.locator(itemSelector).count();
}

async function waitForCandidate(page: Page, name: string): Promise<void> {
  await waitFor(async () => await page.locator(`[data-command-name="${name}"]`).count() > 0, `candidate ${name}`);
}

async function waitForItemCount(page: Page, count: number): Promise<void> {
  await waitFor(async () => await itemCount(page) === count, `${count} command candidate(s)`);
}

async function waitForNote(page: Page, text: string): Promise<void> {
  await waitFor(async () => (await page.locator(noteSelector).innerText()).includes(text), `note ${text}`);
}

async function postChat(page: Page, channelType: ChannelType, channelId: string): Promise<void> {
  await page.evaluate(({ channelType: type, channelId: id }) => {
    window.postMessage({ type: "octobuddy-enhancer:chat", channelType: type, channelId: id }, "*");
  }, { channelType, channelId });
}

async function resetEditor(page: Page, html: string, caretOffset?: number): Promise<void> {
  await page.evaluate(({ html: value, caretOffset: offset }) => {
    const editor = document.querySelector('.wk-messageinput-editor [contenteditable="true"]');
    if (!(editor instanceof HTMLElement)) throw new Error("fixture composer is missing");
    editor.innerHTML = value;
    editor.focus();

    const range = document.createRange();
    if (offset === undefined) {
      range.selectNodeContents(editor);
      range.collapse(false);
    } else {
      let remaining = offset;
      let position: { node: Node; offset: number } | undefined;
      const walker = document.createTreeWalker(editor, NodeFilter.SHOW_TEXT);
      let node = walker.nextNode();
      while (node) {
        const length = node.nodeValue?.length ?? 0;
        if (remaining <= length) {
          position = { node, offset: remaining };
          break;
        }
        remaining -= length;
        node = walker.nextNode();
      }
      if (!position) {
        range.selectNodeContents(editor);
        range.collapse(false);
      } else {
        range.setStart(position.node, position.offset);
        range.collapse(true);
      }
    }
    const selection = window.getSelection();
    if (!selection) throw new Error("selection is unavailable");
    selection.removeAllRanges();
    selection.addRange(range);
    editor.dispatchEvent(new InputEvent("input", { bubbles: true, inputType: "insertText" }));
  }, { html, caretOffset });
  await Bun.sleep(30);
}

async function typeCommand(page: Page, text: string): Promise<void> {
  const editor = page.locator(editorSelector);
  await editor.click();
  await page.keyboard.type(text);
  await Bun.sleep(120);
}

async function createHarness(options: HarnessOptions = {}): Promise<Harness> {
  const root = await mkdtemp(join(tmpdir(), "octo-command-test-"));
  const file = join(root, "OctoBuddy.app/Contents/Resources/communication-renderer/index.html");
  await mkdir(dirname(file), { recursive: true });
  await writeFile(file, fixtureHtml(), "utf8");

  let browser: Browser | undefined;
  try {
    browser = await chromium.launch({ headless: true });
    const page = await browser.newPage();
    page.setDefaultTimeout(5_000);
    await page.goto(pathToFileURL(file).href, { waitUntil: "load" });

    await page.exposeFunction("__octoTestGetCommands", async (target: ChatTarget, requestId: number) => {
      return options.getCommands ? await options.getCommands(target, requestId) : defaultResult(target, options);
    });
    await page.exposeFunction("__octoTestGetChannelStatus", async () => {
      return options.getChannelStatus
        ? await options.getChannelStatus()
        : { state: "online", snapshot: { botUid: "bot", ownerUid: "owner" } };
    });

    await page.evaluate(() => {
      const testWindow = window as unknown as TestWindow;
      testWindow.__octoTestState = {
        commandsCalls: [],
        cancelCalls: [],
        channelStatusCalls: 0,
        sends: 0,
        disposed: false,
      };
      Object.defineProperty(testWindow, "octoEnhancer", {
        configurable: true,
        value: Object.freeze({
          onDispose(callback: () => void) {
            testWindow.__octoTestState.dispose = callback;
          },
          getChannelStatus() {
            testWindow.__octoTestState.channelStatusCalls += 1;
            return testWindow.__octoTestGetChannelStatus();
          },
          getChatCommands(target: ChatTarget, requestId: number) {
            testWindow.__octoTestState.commandsCalls.push({ target: { ...target }, requestId });
            return testWindow.__octoTestGetCommands(target, requestId);
          },
          cancelChatCommands(requestId: number) {
            testWindow.__octoTestState.cancelCalls.push(requestId);
            return Promise.resolve();
          },
        }),
      });

      const editor = document.querySelector('.wk-messageinput-editor [contenteditable="true"]');
      if (!(editor instanceof HTMLElement)) throw new Error("fixture composer is missing");
      editor.addEventListener("keydown", (event) => {
        const keyboard = event as KeyboardEvent;
        if (keyboard.key === "Enter" && !keyboard.shiftKey && !keyboard.isComposing && keyboard.keyCode !== 229) {
          testWindow.__octoTestState.sends += 1;
        }
      });
    });

    const source = await readFile(scriptUrl, "utf8");
    await page.addScriptTag({ content: source });
    await postChat(page, 5, "group____thread");
    await Bun.sleep(50);

    return {
      page,
      state: () => stateOf(page),
      async cleanup() {
        await browser?.close();
        await rm(root, { recursive: true, force: true });
      },
    };
  } catch (error) {
    await browser?.close();
    await rm(root, { recursive: true, force: true });
    throw error;
  }
}

test("selecting a slash command completes the draft without sending it", async () => {
  expect(existsSync(scriptUrl), `Expected production command suggestions at ${scriptUrl.pathname}`).toBe(true);
  const harness = await createHarness();
  try {
    const editor = harness.page.locator(editorSelector);
    await typeCommand(harness.page, "/stat");
    await waitForMenu(harness.page);
    await waitForCandidate(harness.page, "/status");
    expect(await menuTextCount(harness.page, "/status")).toBe(1);
    expect((await harness.state()).sends).toBe(0);

    await harness.page.keyboard.press("Enter");
    expect(await editor.innerText()).toMatch(/^\/status\s*$/);
    // The editor itself now contains the completed command; only the menu must be gone.
    expect(await menuTextCount(harness.page, "/status")).toBe(0);
    expect((await harness.state()).sends).toBe(0);

    // The editor remains usable; the next Enter reaches OctoBuddy's original send path.
    await harness.page.keyboard.press("Enter");
    expect((await harness.state()).sends).toBe(1);
    expect((await harness.state()).channelStatusCalls).toBe(0);
  } finally {
    await harness.cleanup();
  }
});

test("completes namespaced skill commands without sending", async () => {
  const harness = await createHarness({
    commands: [{ name: "skill:frontend/example", source: "omp", description: "Frontend skill" }],
  });
  try {
    await typeCommand(harness.page, "/skill:frontend/ex");
    await waitForCandidate(harness.page, "/skill:frontend/example");
    await harness.page.keyboard.press("Tab");
    expect(await harness.page.locator(editorSelector).innerText()).toMatch(/^\/skill:frontend\/example\s*$/);
    expect((await harness.state()).sends).toBe(0);
  } finally {
    await harness.cleanup();
  }
});

test("filters by name and alias and completes with arrow, tab, mouse, and subcommands", async () => {
  const commands: Command[] = [
    { name: "status", source: "channel", description: "查看当前状态", aliases: ["st"] },
    {
      name: "switch",
      source: "channel",
      description: "切换当前会话",
      hint: "<model|agent>",
      subcommands: [
        { name: "model", description: "切换模型", hint: "<model>" },
        { name: "agent", description: "切换代理", hint: "<agent>" },
        { name: "memory mm list", description: "列出 memory", hint: "<scope>" },
      ],
    },
    { name: "ask", source: "omp", description: "向 OMP 提问", hint: "<question>" },
  ];
  const harness = await createHarness({ commands });
  try {
    const editor = harness.page.locator(editorSelector);

    await typeCommand(harness.page, "/st");
    await waitForMenu(harness.page);
    await waitForCandidate(harness.page, "/status");
    expect(await harness.page.getByText("/status", { exact: true }).count()).toBe(1);
    expect(await harness.page.locator('[data-command-name="/switch"]').count()).toBe(0);

    await resetEditor(harness.page, "");
    await typeCommand(harness.page, "/");
    await waitForMenu(harness.page);
    await waitForItemCount(harness.page, 3);
    expect(await itemCount(harness.page)).toBe(3);

    await harness.page.keyboard.press("ArrowDown");
    expect(await harness.page.locator('[data-command-name="/switch"][data-selected="true"]').count()).toBe(1);
    await harness.page.keyboard.press("Tab");
    expect(await editor.innerText()).toMatch(/^\/switch\s*$/);
    expect((await harness.state()).sends).toBe(0);

    await resetEditor(harness.page, "");
    await typeCommand(harness.page, "/");
    await waitForMenu(harness.page);
    await harness.page.locator('[data-command-name="/switch"]').click();
    expect(await editor.innerText()).toMatch(/^\/switch\s*$/);
    expect((await harness.state()).sends).toBe(0);

    await resetEditor(harness.page, "");
    await typeCommand(harness.page, "/switch mo");
    await waitForMenu(harness.page);
    await waitForCandidate(harness.page, "model");
    expect(await harness.page.locator('[data-command-name="model"]').count()).toBe(1);
    await harness.page.keyboard.press("Enter");
    expect(await editor.innerText()).toMatch(/^\/switch model\s*$/);
    expect((await harness.state()).sends).toBe(0);

    // Completing the in-progress subcommand keeps a later argument that already exists in the draft.
    await resetEditor(harness.page, "/switch m --fast", 10);
    await waitForMenu(harness.page);
    await waitForCandidate(harness.page, "model");
    expect(await harness.page.locator('[data-command-name="model"]').count()).toBe(1);
    await harness.page.keyboard.press("Enter");
    expect(await editor.innerText()).toBe("/switch model --fast");
    expect((await harness.state()).sends).toBe(0);

    await resetEditor(harness.page, "/switch moX --fast", 10);
    await waitForCandidate(harness.page, "model");
    await harness.page.keyboard.press("Enter");
    expect(await editor.innerText()).toBe("/switch model --fast");
    expect((await harness.state()).sends).toBe(0);

    // A command with only a parameter hint shows the hint and keeps normal send behavior.
    await resetEditor(harness.page, "");
    await typeCommand(harness.page, "/ask ");
    await waitForMenu(harness.page);
    await waitForNote(harness.page, "参数：");
    expect(await itemCount(harness.page)).toBe(0);
    await harness.page.keyboard.press("Enter");
    expect((await harness.state()).sends).toBe(1);
  } finally {
    await harness.cleanup();
  }
});

test("multi-word subcommands filter and complete while preserving trailing arguments", async () => {
  const commands: Command[] = [
    {
      name: "switch",
      source: "channel",
      description: "切换当前会话",
      hint: "<scope>",
      subcommands: [
        { name: "model", description: "切换模型", hint: "<model>" },
        { name: "memory", description: "memory 总览", hint: "<action>" },
        { name: "memory mm list", description: "列出 memory", hint: "<scope>" },
      ],
    },
  ];
  const harness = await createHarness({ commands });
  try {
    const editor = harness.page.locator(editorSelector);

    // A multi-word prefix is matched as one name, and replacement stops before the existing args.
    await resetEditor(harness.page, "/switch memory mm --scope project", 17);
    await waitForMenu(harness.page);
    await waitForCandidate(harness.page, "memory mm list");
    expect(await harness.page.locator('[data-command-name="memory mm list"]').innerText()).toContain("<scope>");
    await harness.page.keyboard.press("Enter");
    expect(await editor.innerText()).toBe("/switch memory mm list --scope project");
    expect((await harness.state()).sends).toBe(0);

    // A fully typed single-word subcommand still must not consume the arguments following it.
    await resetEditor(harness.page, "/switch model --fast", 13);
    await waitForMenu(harness.page);
    await waitForCandidate(harness.page, "model");
    expect(await harness.page.locator('[data-command-name="model"]').innerText()).toContain("<model>");
    await harness.page.keyboard.press("Enter");
    expect(await editor.innerText()).toBe("/switch model --fast");
    expect((await harness.state()).sends).toBe(0);
  } finally {
    await harness.cleanup();
  }
});

test("a channel command keeps priority over an OMP command with the same name", async () => {
  const harness = await createHarness({
    commands: [
      { name: "dup", source: "channel", description: "channel wins" },
      { name: "dup", source: "omp", description: "omp duplicate" },
    ],
  });
  try {
    await typeCommand(harness.page, "/dup");
    await waitForMenu(harness.page);
    await waitForCandidate(harness.page, "/dup");
    expect(await harness.page.locator('[data-command-name="/dup"]').count()).toBe(1);
    expect(await harness.page.locator('[data-command-name="/dup"]').innerText()).toContain("channel wins");
  } finally {
    await harness.cleanup();
  }
});

test("structured mention prefixes and trailing mentions survive completion", async () => {
  const harness = await createHarness({ commands: [{ name: "status", source: "channel", description: "查看状态" }] });
  try {
    const editor = harness.page.locator(editorSelector);
    const mentionPrefix = '<span class="mention" data-type="mention" data-id="bot">@Bot</span>';
    const mentionArgument = '<span class="mention" data-type="mention" data-id="alice">@Alice</span>';
    await resetEditor(harness.page, `${mentionPrefix} /stat 请 ${mentionArgument} 查看`, 10);
    await waitForMenu(harness.page);
    await waitForCandidate(harness.page, "/status");
    expect(await harness.page.getByText("/status", { exact: true }).count()).toBe(1);

    await harness.page.keyboard.press("Enter");
    expect(await editor.innerText()).toBe("@Bot /status 请 @Alice 查看");
    expect(await harness.page.locator(".mention").count()).toBe(2);
    expect(await harness.page.locator(".mention").first().innerText()).toBe("@Bot");
    expect(await harness.page.locator(".mention").last().innerText()).toBe("@Alice");
    expect((await harness.state()).sends).toBe(0);
  } finally {
    await harness.cleanup();
  }
});

test("IME composition, escape, duplicate injection, and disposal preserve input and clean up listeners", async () => {
  const harness = await createHarness();
  try {
    await harness.page.evaluate(() => {
      (window as unknown as TestWindow).__octoTestState.disposed = false;
    });

    await typeCommand(harness.page, "/stat");
    await waitForMenu(harness.page);

    await harness.page.evaluate(() => {
      const editor = document.querySelector('.wk-messageinput-editor [contenteditable="true"]');
      if (!(editor instanceof HTMLElement)) throw new Error("fixture composer is missing");
      editor.dispatchEvent(new KeyboardEvent("keydown", {
        key: "Enter",
        code: "Enter",
        keyCode: 229,
        isComposing: true,
        bubbles: true,
        cancelable: true,
      }));
    });
    expect(await menuVisible(harness.page)).toBe(true);
    expect((await harness.state()).sends).toBe(0);

    await harness.page.keyboard.press("Escape");
    expect(await menuVisible(harness.page)).toBe(false);
    expect(await harness.page.getByText("/status", { exact: true }).count()).toBe(0);
    expect((await harness.page.locator(editorSelector).innerText())).toBe("/stat");

    await harness.page.keyboard.press("Enter");
    expect((await harness.state()).sends).toBe(1);

    const source = await readFile(scriptUrl, "utf8");
    await harness.page.addScriptTag({ content: source });
    expect(await harness.page.locator(rootSelector).count()).toBe(1);

    const dispose = await harness.page.evaluate(() => {
      const testWindow = window as unknown as TestWindow;
      testWindow.__octoTestState.disposed = true;
      testWindow.__octoTestState.dispose?.();
      return true;
    });
    expect(dispose).toBe(true);
    expect(await harness.page.locator(rootSelector).count()).toBe(0);
  } finally {
    await harness.cleanup();
  }
});

test("a private chat maps the bot UID to the owner UID before requesting commands", async () => {
  const harness = await createHarness({
    getChannelStatus: () => ({ state: "online", snapshot: { botUid: "bot", ownerUid: "owner/17" } }),
  });
  try {
    await postChat(harness.page, 1, "bot");
    await typeCommand(harness.page, "/");
    await waitFor(async () => (await stateOf(harness.page)).commandsCalls.length === 1, "the private command request");
    const state = await harness.state();
    expect(state.channelStatusCalls).toBe(1);
    expect(state.commandsCalls[0]!.target).toEqual({ channelType: 1, channelId: "owner/17" });
    expect(state.commandsCalls[0]!.target.channelId).not.toBe("bot");
  } finally {
    await harness.cleanup();
  }
});

test("an unknown private chat reports identity failure without issuing a command request", async () => {
  const harness = await createHarness({
    getChannelStatus: () => ({ state: "offline" }),
  });
  try {
    await postChat(harness.page, 1, "bot");
    await typeCommand(harness.page, "/");
    await waitForMenu(harness.page);
    await waitForNote(harness.page, "无法识别当前私聊");
    expect(await harness.page.locator(noteSelector).innerText()).toContain("无法识别当前私聊");
    expect((await harness.state()).commandsCalls).toHaveLength(0);

    await harness.page.keyboard.press("Enter");
    expect((await harness.state()).sends).toBe(1);
  } finally {
    await harness.cleanup();
  }
});

test("unavailable chats keep the native editor without command suggestions", async () => {
  const harness = await createHarness({ getCommands: () => ({ state: "not_found" }) });
  try {
    await typeCommand(harness.page, "/");
    await waitFor(async () => (await harness.state()).commandsCalls.length > 0, "the chat command request");
    await waitFor(async () => !await menuVisible(harness.page), "the unavailable chat menu to close");
    await harness.page.keyboard.type("stat");
    await Bun.sleep(50);
    expect(await menuVisible(harness.page)).toBe(false);
    expect(await harness.page.locator(editorSelector).innerText()).toBe("/stat");
    await harness.page.keyboard.press("Enter");
    expect((await harness.state()).sends).toBe(1);
  } finally {
    await harness.cleanup();
  }
});

test("the loading message is visible above the editor while commands are requested", async () => {
  const harness = await createHarness({ getCommands: () => new Promise<CommandsResult>(() => {}) });
  try {
    await typeCommand(harness.page, "/");
    await waitForNote(harness.page, "正在读取命令");
    const menu = await harness.page.locator(rootSelector).boundingBox();
    const editor = await harness.page.locator(editorSelector).boundingBox();
    expect(menu).not.toBeNull();
    expect(editor).not.toBeNull();
    expect(menu!.height).toBeGreaterThan(0);
    expect(Math.abs(menu!.x - editor!.x)).toBeLessThan(1);
    expect(editor!.y - (menu!.y + menu!.height)).toBeGreaterThanOrEqual(0);
    expect(editor!.y - (menu!.y + menu!.height)).toBeLessThan(10);
  } finally {
    await harness.cleanup();
  }
});

test("authentication failure still permits typing and sending", async () => {
  const harness = await createHarness({
    getCommands: () => ({ state: "unauthorized" }),
  });
  try {
    await typeCommand(harness.page, "/stat");
    await waitForMenu(harness.page);
    await waitForNote(harness.page, "认证失败");
    expect(await harness.page.locator(noteSelector).innerText()).toContain("认证失败");
    expect(await itemCount(harness.page)).toBe(0);
    await harness.page.keyboard.press("Enter");
    expect((await harness.state()).sends).toBe(1);
  } finally {
    await harness.cleanup();
  }
});

test("unavailable OMP catalogs show channel commands and stale results with explicit status", async () => {
  const channelCommand: Command = { name: "status", source: "channel", description: "查看状态" };
  const unavailable = await createHarness({ commands: [channelCommand], omp: "unavailable" });
  try {
    await typeCommand(unavailable.page, "/stat");
    await waitForMenu(unavailable.page);
    await waitForCandidate(unavailable.page, "/status");
    expect(await unavailable.page.getByText("/status", { exact: true }).count()).toBe(1);
    expect(await unavailable.page.locator(noteSelector).innerText()).toContain("OMP 命令不可用");
    await unavailable.page.keyboard.press("Enter");
    expect(await unavailable.page.locator(editorSelector).innerText()).toMatch(/^\/status\s*$/);
    expect((await unavailable.state()).sends).toBe(0);
  } finally {
    await unavailable.cleanup();
  }

  const stale = await createHarness({
    getCommands: (target) => ({
      state: "offline",
      lastSuccess: {
        version: 1,
        channelType: target.channelType,
        channelId: target.channelId,
        commands: [channelCommand],
        omp: "unavailable",
      },
    }),
  });
  try {
    await typeCommand(stale.page, "/");
    await waitForMenu(stale.page);
    await waitForCandidate(stale.page, "/status");
    expect(await stale.page.getByText("/status", { exact: true }).count()).toBe(1);
    expect(await stale.page.locator(noteSelector).innerText()).toContain("显示上次结果");
    await stale.page.keyboard.press("Enter");
    expect(await stale.page.locator(editorSelector).innerText()).toMatch(/^\/status\s*$/);
    expect((await stale.state()).sends).toBe(0);
  } finally {
    await stale.cleanup();
  }
});

test("a 125-command catalog is fully rendered and scrollable", async () => {
  const commands: Command[] = Array.from({ length: 125 }, (_, index) => ({
    name: `skill-${String(index + 1).padStart(3, "0")}`,
    source: "omp",
    description: `Skill ${index + 1}`,
  }));
  const harness = await createHarness({ commands, omp: "available" });
  try {
    await typeCommand(harness.page, "/");
    await waitForMenu(harness.page);
    await waitForItemCount(harness.page, 125);
    expect(await itemCount(harness.page)).toBe(125);
    const geometry = await harness.page.evaluate(() => {
      const root = document.querySelector("[data-octobuddy-command-suggestions-root]");
      if (!(root instanceof HTMLElement)) throw new Error("command menu is missing");
      return { clientHeight: root.clientHeight, scrollHeight: root.scrollHeight };
    });
    expect(geometry.scrollHeight).toBeGreaterThan(geometry.clientHeight);
    expect(await harness.page.locator('[data-command-name="/skill-001"][data-selected="true"]').count()).toBe(1);
    await harness.page.keyboard.press("ArrowDown");
    expect(await harness.page.locator('[data-command-name="/skill-002"][data-selected="true"]').count()).toBe(1);
  } finally {
    await harness.cleanup();
  }
});

test("the menu is a white system-font popover above the editor with a purple selection", async () => {
  const harness = await createHarness();
  try {
    await typeCommand(harness.page, "/stat");
    await waitForMenu(harness.page);
    await waitForCandidate(harness.page, "/status");
    const visual = await harness.page.evaluate(() => {
      const root = document.querySelector("[data-octobuddy-command-suggestions-root]");
      const editor = document.querySelector('.wk-messageinput-editor [contenteditable="true"]');
      const selected = document.querySelector('[data-octobuddy-command-suggestions-item][data-selected="true"]');
      if (!(root instanceof HTMLElement) || !(editor instanceof HTMLElement) || !(selected instanceof HTMLElement)) {
        throw new Error("command suggestion visual fixture is missing");
      }
      const rootRect = root.getBoundingClientRect();
      const editorRect = editor.getBoundingClientRect();
      const rootStyle = getComputedStyle(root);
      const selectedStyle = getComputedStyle(selected);
      return {
        background: rootStyle.backgroundColor,
        borderStyle: rootStyle.borderTopStyle,
        borderWidth: rootStyle.borderTopWidth,
        fontFamily: rootStyle.fontFamily,
        selectedBackground: selectedStyle.backgroundColor,
        rootBottom: rootRect.bottom,
        rootLeft: rootRect.left,
        editorLeft: editorRect.left,
        editorTop: editorRect.top,
        visible: !root.hidden && rootRect.width > 0 && rootRect.height > 0,
      };
    });
    expect(visual.visible).toBe(true);
    expect(visual.background).toBe("rgb(255, 255, 255)");
    expect(visual.borderStyle).toBe("solid");
    expect(visual.borderWidth).toBe("1px");
    expect(visual.fontFamily.toLowerCase()).toContain("-apple-system");
    expect(visual.selectedBackground).toBe("rgb(108, 86, 201)");
    expect(Math.abs(visual.rootLeft - visual.editorLeft)).toBeLessThan(1);
    expect(visual.editorTop - visual.rootBottom).toBeLessThan(10);
    expect(visual.rootBottom).toBeLessThanOrEqual(visual.editorTop);
  } finally {
    await harness.cleanup();
  }
});

test("switching chats clears the previous catalog immediately", async () => {
  const harness = await createHarness({
    getCommands: (target) => ({
      state: "online",
      snapshot: {
        version: 1,
        channelType: target.channelType,
        channelId: target.channelId,
        commands: [{
          name: target.channelId === "group____thread" ? "old" : "new",
          source: "channel",
          description: target.channelId === "group____thread" ? "旧聊天命令" : "新聊天命令",
        }],
        omp: "available",
      },
    }),
  });
  try {
    await typeCommand(harness.page, "/old");
    await waitForMenu(harness.page);
    await waitForCandidate(harness.page, "/old");
    expect(await menuTextCount(harness.page, "/old")).toBe(1);

    await postChat(harness.page, 2, "another-group");
    await waitFor(async () => await menuTextCount(harness.page, "/old") === 0, "the old catalog to clear");
    expect(await menuTextCount(harness.page, "/old")).toBe(0);

    await resetEditor(harness.page, "");
    await typeCommand(harness.page, "/new");
    await waitForMenu(harness.page);
    await waitForCandidate(harness.page, "/new");
    expect(await menuTextCount(harness.page, "/new")).toBe(1);
  } finally {
    await harness.cleanup();
  }
});

test("an older cross-chat response cannot replace the current chat catalog", async () => {
  const requests: {
    target: ChatTarget;
    requestId: number;
    resolve: (result: CommandsResult) => void;
  }[] = [];
  const harness = await createHarness({
    getCommands: (target, requestId) => {
      const pending = Promise.withResolvers<CommandsResult>();
      requests.push({ target, requestId, resolve: pending.resolve });
      return pending.promise;
    },
    commands: [],
    omp: "available",
  });
  try {
    await typeCommand(harness.page, "/");
    await waitFor(() => Promise.resolve(requests.length === 1), "the first chat request");
    const first = requests[0]!;

    await postChat(harness.page, 5, "group____new-thread");
    await Bun.sleep(30);
    await waitFor(() => Promise.resolve(requests.length === 2), "the second chat request");
    const second = requests[1]!;
    expect(second.target.channelId).toBe("group____new-thread");
    expect(second.requestId).not.toBe(first.requestId);

    second.resolve({
      state: "online",
      snapshot: {
        version: 1,
        channelType: second.target.channelType,
        channelId: second.target.channelId,
        commands: [{ name: "new", source: "channel", description: "新聊天命令" }],
        omp: "available",
      },
    });
    await waitFor(async () => await harness.page.getByText("/new", { exact: true }).count() === 1, "the current chat catalog");
    expect(await harness.page.getByText("/old", { exact: true }).count()).toBe(0);

    first.resolve({
      state: "online",
      snapshot: {
        version: 1,
        channelType: first.target.channelType,
        channelId: first.target.channelId,
        commands: [{ name: "old", source: "channel", description: "旧聊天命令" }],
        omp: "available",
      },
    });
    await Bun.sleep(80);
    expect(await harness.page.getByText("/old", { exact: true }).count()).toBe(0);
    expect(await harness.page.getByText("/new", { exact: true }).count()).toBe(1);
    expect((await harness.state()).cancelCalls.length).toBeGreaterThan(0);
  } finally {
    await harness.cleanup();
  }
});

test("disposing the plugin cancels an in-flight request and removes the active UI", async () => {
  const requests: { requestId: number; resolve: (result: CommandsResult) => void }[] = [];
  const harness = await createHarness({
    getCommands: (target, requestId) => {
      const pending = Promise.withResolvers<CommandsResult>();
      requests.push({ requestId, resolve: pending.resolve });
      return pending.promise;
    },
    commands: [],
  });
  try {
    await typeCommand(harness.page, "/");
    await waitForMenu(harness.page);
    await waitFor(() => Promise.resolve(requests.length === 1), "the pending command request");
    await waitForNote(harness.page, "正在读取命令");
    expect(await harness.page.locator(noteSelector).innerText()).toContain("正在读取命令");

    await harness.page.evaluate(() => {
      (window as unknown as TestWindow).__octoTestState.dispose?.();
    });
    const state = await harness.state();
    expect(state.cancelCalls).toContain(requests[0]!.requestId);
    expect(await harness.page.locator(rootSelector).count()).toBe(0);

    requests[0]!.resolve({ state: "unauthorized" });
    await Bun.sleep(50);
    expect(await harness.page.locator(rootSelector).count()).toBe(0);
  } finally {
    await harness.cleanup();
  }
});
