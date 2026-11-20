import { expect, test } from "bun:test";
import { Database } from "bun:sqlite";
import { existsSync } from "node:fs";
import { mkdtemp, mkdir, readFile, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { dirname, join } from "node:path";
import { pathToFileURL } from "node:url";
import { chromium, type Browser, type Page } from "playwright";

type ChannelType = 1 | 2 | 5;
type ChatTarget = { channelType: ChannelType; channelId: string };
type HistoryEntry = { text: string; mentions?: unknown[] };
type HistorySnapshot = { limit: number; entries: HistoryEntry[] };
type BridgeOperation = "load" | "append" | "setLimit" | "import";
type LegacySeed = { history?: Record<string, HistoryEntry[]>; limit?: number };
type TestState = { sentTexts: string[]; bridgePending: number; dispose?: () => void };

type TestWindow = Window & {
  __octoTestState: TestState;
  __octoLoadInputHistory: (chatKey: string) => Promise<HistorySnapshot>;
  __octoAppendInputHistory: (chatKey: string, entry: HistoryEntry) => Promise<HistorySnapshot>;
  __octoSetInputHistoryLimit: (limit: number) => Promise<number>;
  __octoImportInputHistory: (chatKey: string, entries: HistoryEntry[], limit: number) => Promise<HistorySnapshot>;
  octoEnhancer?: unknown;
};

interface Harness {
  page: Page;
  state(): Promise<TestState>;
  snapshot(target?: ChatTarget): HistorySnapshot;
  waitForStored(target: ChatTarget, text: string): Promise<void>;
  setDelay(operation: BridgeOperation, ms: number): void;
  failNext(operation: BridgeOperation, message: string): void;
  operations(): string[];
  legacyData(): Promise<{ history: Record<string, HistoryEntry[]> | null; limit: string | null }>;
  reload(target?: ChatTarget): Promise<void>;
  cleanup(): Promise<void>;
}

class SqliteHistoryFixture {
  private readonly db: Database;
  private readonly delays = new Map<BridgeOperation, number>();
  private readonly failures = new Map<BridgeOperation, string[]>();
  private readonly log: string[] = [];

  constructor(path: string) {
    this.db = new Database(path);
    this.db.exec(`
      CREATE TABLE settings (id INTEGER PRIMARY KEY CHECK (id = 1), history_limit INTEGER NOT NULL);
      INSERT INTO settings (id, history_limit) VALUES (1, 100);
      CREATE TABLE entries (
        chat_key TEXT NOT NULL,
        position INTEGER NOT NULL,
        text TEXT NOT NULL,
        mentions_json TEXT,
        PRIMARY KEY (chat_key, position)
      );
      CREATE TABLE imported_chats (chat_key TEXT PRIMARY KEY);
    `);
  }

  close(): void {
    this.db.close();
  }

  setDelay(operation: BridgeOperation, ms: number): void {
    this.delays.set(operation, ms);
  }

  failNext(operation: BridgeOperation, message: string): void {
    const failures = this.failures.get(operation) ?? [];
    failures.push(message);
    this.failures.set(operation, failures);
  }

  operations(): string[] {
    return [...this.log];
  }

  async load(chatKey: string): Promise<HistorySnapshot> {
    return await this.run("load", () => this.readSnapshot(chatKey));
  }

  async append(chatKey: string, entry: HistoryEntry): Promise<HistorySnapshot> {
    return await this.run("append", () => {
      const limit = this.readLimit();
      const entries = [entry, ...this.readEntries(chatKey)].slice(0, limit);
      this.writeEntries(chatKey, entries);
      return { limit, entries };
    });
  }

  async setLimit(limit: number): Promise<number> {
    return await this.run("setLimit", () => {
      const transaction = this.db.transaction((nextLimit: number) => {
        this.db.query("UPDATE settings SET history_limit = ? WHERE id = 1").run(nextLimit);
        const chats = this.db.query<{ chat_key: string }, []>("SELECT DISTINCT chat_key FROM entries").all();
        for (const { chat_key } of chats) {
          this.writeEntries(chat_key, this.readEntries(chat_key).slice(0, nextLimit));
        }
      });
      transaction(limit);
      return this.readLimit();
    });
  }

  async import(chatKey: string, entries: HistoryEntry[], limit: number): Promise<HistorySnapshot> {
    return await this.run("import", () => {
      const transaction = this.db.transaction(() => {
        const alreadyImported = this.db.query("SELECT 1 FROM imported_chats WHERE chat_key = ?").get(chatKey);
        if (alreadyImported) return;

        this.db.query("UPDATE settings SET history_limit = ? WHERE id = 1").run(limit);
        if (this.readEntries(chatKey).length === 0) this.writeEntries(chatKey, entries.slice(0, limit));
        this.db.query("INSERT INTO imported_chats (chat_key) VALUES (?)").run(chatKey);
        const chats = this.db.query<{ chat_key: string }, []>("SELECT DISTINCT chat_key FROM entries").all();
        for (const { chat_key: existingChat } of chats) {
          this.writeEntries(existingChat, this.readEntries(existingChat).slice(0, limit));
        }
      });
      transaction();
      return this.readSnapshot(chatKey);
    });
  }

  snapshot(target: ChatTarget): HistorySnapshot {
    return this.readSnapshot(chatKeyFor(target));
  }

  private async run<T>(operation: BridgeOperation, action: () => T): Promise<T> {
    this.log.push(`start:${operation}`);
    const delay = this.delays.get(operation) ?? 0;
    if (delay > 0) await Bun.sleep(delay);
    const failures = this.failures.get(operation);
    const failure = failures?.shift();
    if (failure) {
      this.log.push(`error:${operation}`);
      throw new Error(failure);
    }
    const result = action();
    this.log.push(`end:${operation}`);
    return result;
  }

  private readLimit(): number {
    return this.db.query<{ history_limit: number }, []>("SELECT history_limit FROM settings WHERE id = 1").get()!.history_limit;
  }

  private readEntries(chatKey: string): HistoryEntry[] {
    return this.db.query<{ text: string; mentions_json: string | null }, [string]>(
      "SELECT text, mentions_json FROM entries WHERE chat_key = ? ORDER BY position",
    ).all(chatKey).map(({ text, mentions_json }) => ({
      text,
      ...(mentions_json === null ? {} : { mentions: JSON.parse(mentions_json) as unknown[] }),
    }));
  }

  private readSnapshot(chatKey: string): HistorySnapshot {
    return { limit: this.readLimit(), entries: this.readEntries(chatKey) };
  }

  private writeEntries(chatKey: string, entries: HistoryEntry[]): void {
    this.db.query("DELETE FROM entries WHERE chat_key = ?").run(chatKey);
    const insert = this.db.query(
      "INSERT INTO entries (chat_key, position, text, mentions_json) VALUES (?, ?, ?, ?)",
    );
    entries.forEach((entry, position) => {
      insert.run(chatKey, position, entry.text, entry.mentions === undefined ? null : JSON.stringify(entry.mentions));
    });
  }
}


const editorSelector = '.wk-messageinput-editor [contenteditable="true"]';
const historyRootSelector = "[data-octobuddy-input-history-root]";
const historyItemSelector = "[data-octobuddy-input-history-item]";
const commandRootSelector = "[data-octobuddy-command-suggestions-root]";
const commandItemSelector = "[data-octobuddy-command-suggestions-item]";
const defaultChat: ChatTarget = { channelType: 5, channelId: "group____thread" };
const inputHistoryUrl = new URL("../plugins/channel-status/input-history.js", import.meta.url);
const commandSuggestionsUrl = new URL("../plugins/channel-status/command-suggestions.js", import.meta.url);

function chatKeyFor(target: ChatTarget): string {
  return JSON.stringify([target.channelType, target.channelId]);
}

function fixtureHtml(): string {
  return `<!doctype html>
<html><head><meta charset="utf-8"><title>Octo input history fixture</title>
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
    return { sentTexts: [...state.sentTexts], bridgePending: state.bridgePending };
  });
}

async function menuVisible(page: Page): Promise<boolean> {
  return await page.locator(`${historyRootSelector}:not([hidden])`).count() === 1;
}

async function waitForMenu(page: Page): Promise<void> {
  await waitFor(() => menuVisible(page), "the input history menu");
}

async function waitForBridgeIdle(page: Page): Promise<void> {
  let idleSamples = 0;
  await waitFor(async () => {
    idleSamples = (await stateOf(page)).bridgePending === 0 ? idleSamples + 1 : 0;
    return idleSamples >= 3;
  }, "input history storage operations");
}

async function itemCount(page: Page): Promise<number> {
  return await page.locator(historyItemSelector).count();
}

async function waitForItemCount(page: Page, count: number): Promise<void> {
  await waitFor(async () => await itemCount(page) === count, `${count} history item(s)`);
}

async function postChat(page: Page, target: ChatTarget): Promise<void> {
  await page.evaluate(({ channelType, channelId }) => {
    window.postMessage({ type: "octobuddy-enhancer:chat", channelType, channelId }, "*");
  }, target);
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

async function typeText(page: Page, text: string): Promise<void> {
  await page.locator(editorSelector).click();
  await page.keyboard.type(text);
}

async function sendText(page: Page, text: string, waitForPersistence = true): Promise<void> {
  await resetEditor(page, "");
  await typeText(page, text);
  await page.keyboard.press("Enter");
  await waitFor(async () => (await stateOf(page)).sentTexts.includes(text), `sent text ${text}`);
  await waitFor(async () => await page.locator(editorSelector).innerText() === "", "the consumed editor");
  if (waitForPersistence) await waitForBridgeIdle(page);
}

async function openHistory(page: Page, draft = ""): Promise<void> {
  await resetEditor(page, draft);
  await page.keyboard.press("ArrowUp");
  await waitForMenu(page);
}

async function selectedEntry(page: Page, entries: string[]): Promise<string> {
  const selected = page.locator(`${historyItemSelector}[data-selected="true"]`);
  const text = await selected.innerText();
  const entry = entries.find((candidate) => text.includes(candidate));
  if (!entry) throw new Error(`Selected history item did not contain an expected entry: ${text}`);
  return entry;
}

async function installScripts(
  page: Page,
  historySource: string,
  commandSuggestionsSource: string,
  legacySeed?: LegacySeed,
): Promise<void> {
  await page.evaluate((seed) => {
    const testWindow = window as unknown as TestWindow;
    testWindow.__octoTestState = { sentTexts: [], bridgePending: 0 };
    if (seed?.history) localStorage.setItem("octobuddy.inputHistory.v1", JSON.stringify(seed.history));
    if (seed?.limit !== undefined) localStorage.setItem("octobuddy.inputHistory.limit", String(seed.limit));

    const bridgeCall = <T>(operation: () => Promise<T>): Promise<T> => {
      testWindow.__octoTestState.bridgePending += 1;
      return operation().finally(() => {
        testWindow.__octoTestState.bridgePending -= 1;
      });
    };
    Object.defineProperty(testWindow, "octoEnhancer", {
      configurable: true,
      value: Object.freeze({
        onDispose(callback: () => void) {
          testWindow.__octoTestState.dispose = callback;
        },
        loadInputHistory(chatKey: string) {
          return bridgeCall(() => testWindow.__octoLoadInputHistory(chatKey));
        },
        appendInputHistory(chatKey: string, entry: HistoryEntry) {
          return bridgeCall(() => testWindow.__octoAppendInputHistory(chatKey, entry));
        },
        setInputHistoryLimit(limit: number) {
          return bridgeCall(() => testWindow.__octoSetInputHistoryLimit(limit));
        },
        importInputHistory(chatKey: string, entries: HistoryEntry[], limit: number) {
          return bridgeCall(() => testWindow.__octoImportInputHistory(chatKey, entries, limit));
        },
        getChatCommands(target: ChatTarget) {
          return Promise.resolve({
            state: "online",
            snapshot: {
              version: 1,
              channelType: target.channelType,
              channelId: target.channelId,
              commands: [{ name: "status", source: "channel", description: "查看当前状态" }],
              omp: "available",
            },
          });
        },
        cancelChatCommands() {
          return Promise.resolve();
        },
      }),
    });

    const editor = document.querySelector('.wk-messageinput-editor [contenteditable="true"]');
    if (!(editor instanceof HTMLElement)) throw new Error("fixture composer is missing");
    editor.addEventListener("keydown", (event) => {
      const keyboard = event as KeyboardEvent;
      if (keyboard.defaultPrevented || keyboard.key !== "Enter" || keyboard.shiftKey || keyboard.isComposing || keyboard.keyCode === 229) return;
      keyboard.preventDefault();
      testWindow.__octoTestState.sentTexts.push(editor.innerText);
      window.setTimeout(() => {
        editor.innerHTML = "";
        editor.dispatchEvent(new InputEvent("input", { bubbles: true, inputType: "deleteContentBackward" }));
      }, 0);
    });
  }, legacySeed);

  await page.addScriptTag({ content: commandSuggestionsSource });
  await page.addScriptTag({ content: historySource });
}

async function createHarness(legacySeed?: LegacySeed): Promise<Harness> {
  expect(existsSync(inputHistoryUrl), `Expected input history script at ${inputHistoryUrl.pathname}`).toBe(true);
  const historySource = await readFile(inputHistoryUrl, "utf8");
  const commandSuggestionsSource = await readFile(commandSuggestionsUrl, "utf8");
  const root = await mkdtemp(join(tmpdir(), "octo-input-history-test-"));
  const file = join(root, "OctoBuddy.app/Contents/Resources/communication-renderer/index.html");
  const historyDb = new SqliteHistoryFixture(join(root, "input-history.sqlite"));
  await mkdir(dirname(file), { recursive: true });
  await writeFile(file, fixtureHtml(), "utf8");

  let browser: Browser | undefined;
  try {
    browser = await chromium.launch({ headless: true });
    const page = await browser.newPage();
    page.setDefaultTimeout(5_000);
    await page.exposeFunction("__octoLoadInputHistory", (chatKey: string) => historyDb.load(chatKey));
    await page.exposeFunction("__octoAppendInputHistory", (chatKey: string, entry: HistoryEntry) => historyDb.append(chatKey, entry));
    await page.exposeFunction("__octoSetInputHistoryLimit", (limit: number) => historyDb.setLimit(limit));
    await page.exposeFunction("__octoImportInputHistory", (chatKey: string, entries: HistoryEntry[], limit: number) => historyDb.import(chatKey, entries, limit));
    await page.goto(pathToFileURL(file).href, { waitUntil: "load" });
    await installScripts(page, historySource, commandSuggestionsSource, legacySeed);
    await postChat(page, defaultChat);
    await waitForBridgeIdle(page);

    return {
      page,
      state: () => stateOf(page),
      snapshot: (target = defaultChat) => historyDb.snapshot(target),
      async waitForStored(target, text) {
        await waitFor(async () => historyDb.snapshot(target).entries.some((entry) => entry.text === text), `stored history entry ${text}`);
      },
      setDelay: (operation, ms) => historyDb.setDelay(operation, ms),
      failNext: (operation, message) => historyDb.failNext(operation, message),
      operations: () => historyDb.operations(),
      async legacyData() {
        return await page.evaluate(() => {
          const raw = localStorage.getItem("octobuddy.inputHistory.v1");
          return {
            history: raw === null ? null : JSON.parse(raw) as Record<string, HistoryEntry[]>,
            limit: localStorage.getItem("octobuddy.inputHistory.limit"),
          };
        });
      },
      async reload(target = defaultChat) {
        await page.reload({ waitUntil: "load" });
        await installScripts(page, historySource, commandSuggestionsSource);
        await postChat(page, target);
        await waitForBridgeIdle(page);
      },
      async cleanup() {
        await browser?.close();
        historyDb.close();
        await rm(root, { recursive: true, force: true });
      },
    };
  } catch (error) {
    await browser?.close();
    historyDb.close();
    await rm(root, { recursive: true, force: true });
    throw error;
  }
}

async function recordManyTexts(page: Page, count: number): Promise<string[]> {
  const entries = Array.from({ length: count }, (_, index) => `history-entry-${String(index + 1).padStart(3, "0")}`);
  for (const message of entries) await sendText(page, message);
  await waitFor(async () => (await stateOf(page)).sentTexts.length === count, `${count} submitted messages`);
  return entries;
}

test("submitted input history persists across a renderer reload", async () => {
  const harness = await createHarness();
  try {
    await sendText(harness.page, "survives reload");
    await harness.reload();
    await openHistory(harness.page);
    expect(await harness.page.locator(historyItemSelector).filter({ hasText: "survives reload" }).count()).toBe(1);
  } finally {
    await harness.cleanup();
  }
});
test("a submission during asynchronous initial load is retained without showing a false empty state", async () => {
  const harness = await createHarness();
  try {
    const slowChat = { channelType: 2 as const, channelId: "slow-load-chat" };
    harness.setDelay("load", 500);
    await postChat(harness.page, slowChat);
    await openHistory(harness.page);
    expect(await harness.page.locator(historyRootSelector).getByText(/正在加载/).isVisible()).toBe(true);
    expect(await harness.page.locator(historyRootSelector).getByText(/发送消息后/).count()).toBe(0);
    await harness.page.keyboard.press("Escape");

    await sendText(harness.page, "typed before initial load", false);
    expect((await harness.state()).sentTexts).toContain("typed before initial load");
    await harness.waitForStored(slowChat, "typed before initial load");
    await waitForBridgeIdle(harness.page);
    expect(harness.snapshot(slowChat).entries[0]?.text).toBe("typed before initial load");

    await openHistory(harness.page);
    expect(await harness.page.locator(historyItemSelector).filter({ hasText: "typed before initial load" }).count()).toBe(1);
  } finally {
    await harness.cleanup();
  }
});
test("rapid submissions are appended in order without overlapping asynchronous writes", async () => {
  const harness = await createHarness();
  try {
    harness.setDelay("append", 120);
    await sendText(harness.page, "first queued submission", false);
    await sendText(harness.page, "second queued submission", false);
    await waitForBridgeIdle(harness.page);

    expect(harness.snapshot(defaultChat).entries.map((entry) => entry.text).slice(0, 2)).toEqual([
      "second queued submission",
      "first queued submission",
    ]);
    const operations = harness.operations();
    const appends = operations.filter((operation) => operation.endsWith(":append"));
    expect(appends.slice(-4)).toEqual(["start:append", "end:append", "start:append", "end:append"]);
  } finally {
    await harness.cleanup();
  }
});

test("failed bridge loads and saves show an error instead of claiming the history is empty", async () => {
  const harness = await createHarness();
  try {
    const failedLoadChat = { channelType: 2 as const, channelId: "failed-load-chat" };
    harness.failNext("load", "temporary storage failure");
    await postChat(harness.page, failedLoadChat);
    await openHistory(harness.page);
    expect(await harness.page.locator(`${historyRootSelector} [data-octobuddy-input-history-error]`).isVisible()).toBe(true);
    expect(await harness.page.locator(historyRootSelector).getByText(/发送消息后/).count()).toBe(0);

    await postChat(harness.page, defaultChat);
    await waitForBridgeIdle(harness.page);
    harness.failNext("append", "temporary storage failure");
    await sendText(harness.page, "not saved", false);
    await waitForBridgeIdle(harness.page);
    await openHistory(harness.page);
    expect(await harness.page.locator(`${historyRootSelector} [data-octobuddy-input-history-error]`).isVisible()).toBe(true);
    expect(await harness.page.locator(historyRootSelector).getByText(/发送消息后/).count()).toBe(0);
    expect(harness.snapshot(defaultChat).entries.some((entry) => entry.text === "not saved")).toBe(false);
  } finally {
    await harness.cleanup();
  }
});

test("legacy history imports one chat at a time and clears only acknowledged entries", async () => {
  const otherChat = { channelType: 2 as const, channelId: "legacy-other-chat" };
  const legacyHistory = {
    [chatKeyFor(defaultChat)]: [{ text: "legacy current chat" }],
    [chatKeyFor(otherChat)]: [{ text: "legacy other chat" }],
  };
  const harness = await createHarness({ history: legacyHistory, limit: 7 });
  try {
    await openHistory(harness.page);
    expect(await harness.page.locator(historyItemSelector).filter({ hasText: "legacy current chat" }).count()).toBe(1);
    let remaining = await harness.legacyData();
    expect(remaining.history).toEqual({ [chatKeyFor(otherChat)]: [{ text: "legacy other chat" }] });
    expect(remaining.limit).toBe("7");

    await postChat(harness.page, otherChat);
    await waitForBridgeIdle(harness.page);
    await openHistory(harness.page);
    expect(await harness.page.locator(historyItemSelector).filter({ hasText: "legacy other chat" }).count()).toBe(1);
    remaining = await harness.legacyData();
    expect(remaining.history).toBeNull();
    expect(remaining.limit).toBeNull();
    expect(harness.snapshot(defaultChat).entries[0]?.text).toBe("legacy current chat");
    expect(harness.snapshot(otherChat).entries[0]?.text).toBe("legacy other chat");
  } finally {
    await harness.cleanup();
  }
});
test("failed legacy imports leave the legacy entries available for another attempt", async () => {
  const legacyChat = { channelType: 2 as const, channelId: "legacy-retry-chat" };
  const entry = { text: "must survive failed migration" };
  const harness = await createHarness({
    history: { [chatKeyFor(legacyChat)]: [entry] },
    limit: 6,
  });
  try {
    harness.failNext("import", "temporary storage failure");
    await postChat(harness.page, legacyChat);
    await waitForBridgeIdle(harness.page);
    const legacy = await harness.legacyData();
    expect(legacy.history).toEqual({ [chatKeyFor(legacyChat)]: [entry] });
    expect(legacy.limit).toBe("6");
    await openHistory(harness.page);
    expect(await harness.page.locator(`${historyRootSelector} [data-octobuddy-input-history-error]`).isVisible()).toBe(true);
    expect(await harness.page.locator(historyRootSelector).getByText(/发送消息后/).count()).toBe(0);
  } finally {
    await harness.cleanup();
  }
});

test("a late history load cannot replace the newly selected chat history", async () => {
  const oldChat = { channelType: 2 as const, channelId: "slow-old-chat" };
  const harness = await createHarness({
    history: { [chatKeyFor(oldChat)]: [{ text: "stale old chat entry" }] },
  });
  try {
    harness.setDelay("load", 300);
    await postChat(harness.page, oldChat);
    await openHistory(harness.page);
    expect(await harness.page.locator(historyRootSelector).getByText(/正在加载/).isVisible()).toBe(true);

    await postChat(harness.page, defaultChat);
    await openHistory(harness.page);
    expect(await harness.page.locator(historyItemSelector).filter({ hasText: "stale old chat entry" }).count()).toBe(0);
    await waitForBridgeIdle(harness.page);
    await openHistory(harness.page);
    expect(await harness.page.locator(historyItemSelector).filter({ hasText: "stale old chat entry" }).count()).toBe(0);
    expect(await harness.page.locator(historyRootSelector).getByText(/发送消息后/).isVisible()).toBe(true);
  } finally {
    await harness.cleanup();
  }
});

test("input history stays isolated by channel type and chat ID", async () => {
  const harness = await createHarness();
  try {
    await sendText(harness.page, "group-only entry");
    const otherChat = { channelType: 2 as const, channelId: "group____thread" };
    await postChat(harness.page, otherChat);
    await waitForBridgeIdle(harness.page);
    await openHistory(harness.page);
    expect(await harness.page.locator(historyItemSelector).filter({ hasText: "group-only entry" }).count()).toBe(0);

    await sendText(harness.page, "other chat entry");
    await postChat(harness.page, defaultChat);
    await waitForBridgeIdle(harness.page);
    await openHistory(harness.page);
    expect(await harness.page.locator(historyItemSelector).filter({ hasText: "group-only entry" }).count()).toBe(1);
    expect(await harness.page.locator(historyItemSelector).filter({ hasText: "other chat entry" }).count()).toBe(0);
  } finally {
    await harness.cleanup();
  }
});

test("empty history and a search with no matches offer different guidance", async () => {
  const harness = await createHarness();
  try {
    await openHistory(harness.page);
    expect(await harness.page.locator(historyRootSelector).getByText(/发送消息后/).isVisible()).toBe(true);
    await harness.page.keyboard.press("Escape");
    await sendText(harness.page, "earlier message");
    await openHistory(harness.page, "unmatched");
    expect(await harness.page.locator(historyRootSelector).getByText(/关键词/).isVisible()).toBe(true);
  } finally {
    await harness.cleanup();
  }
});

test("the default history limit retains the most recent 100 submissions", async () => {
  const harness = await createHarness();
  try {
    const entries = await recordManyTexts(harness.page, 101);
    await openHistory(harness.page);
    await waitForItemCount(harness.page, 100);
    expect(await harness.page.locator(historyItemSelector).filter({ hasText: entries[0]! }).count()).toBe(0);
    expect(await harness.page.locator(historyItemSelector).filter({ hasText: entries[100]! }).count()).toBe(1);
  } finally {
    await harness.cleanup();
  }
}, 30_000);

test("the popup limit control overrides and persists the number of retained submissions", async () => {
  const harness = await createHarness();
  try {
    await sendText(harness.page, "limit seed");
    await openHistory(harness.page);
    const limit = harness.page.locator(`${historyRootSelector} input[type="number"]`);
    await waitFor(async () => await limit.count() === 1, "the history limit control");
    expect(await limit.inputValue()).toBe("100");

    await limit.fill("2");
    await limit.blur();
    await waitForBridgeIdle(harness.page);
    expect(await limit.inputValue()).toBe("2");
    await harness.reload();
    await sendText(harness.page, "limit recent one");
    await sendText(harness.page, "limit recent two");
    await sendText(harness.page, "limit recent three");
    await openHistory(harness.page);

    const persistedLimit = harness.page.locator(`${historyRootSelector} input[type="number"]`);
    expect(await persistedLimit.inputValue()).toBe("2");
    await waitForItemCount(harness.page, 2);
    expect(await harness.page.locator(historyItemSelector).filter({ hasText: "limit recent one" }).count()).toBe(0);
    expect(await harness.page.locator(historyItemSelector).filter({ hasText: "limit recent three" }).count()).toBe(1);
  } finally {
    await harness.cleanup();
  }
});
test("limit changes are ordered before later submissions and trim durable history", async () => {
  const harness = await createHarness();
  try {
    await sendText(harness.page, "limit order one");
    await sendText(harness.page, "limit order two");
    await sendText(harness.page, "limit order three");
    await openHistory(harness.page);
    const limit = harness.page.locator(`${historyRootSelector} input[type="number"]`);
    harness.setDelay("setLimit", 150);
    await limit.fill("2");
    await limit.blur();
    await sendText(harness.page, "limit order after change");

    const snapshot = harness.snapshot(defaultChat);
    expect(snapshot.limit).toBe(2);
    expect(snapshot.entries.map((entry) => entry.text)).toEqual([
      "limit order after change",
      "limit order three",
    ]);
    const operations = harness.operations();
    const limitChange = operations.lastIndexOf("start:setLimit");
    const laterAppend = operations.lastIndexOf("start:append");
    expect(limitChange).toBeGreaterThan(-1);
    expect(limitChange).toBeLessThan(laterAppend);
  } finally {
    await harness.cleanup();
  }
});

test("the current draft filters history and the popup sits above the editor", async () => {
  const harness = await createHarness();
  try {
    await sendText(harness.page, "alpha saved draft");
    await sendText(harness.page, "beta saved draft");
    await openHistory(harness.page, "alpha");
    await waitFor(async () => await harness.page.locator(historyItemSelector).filter({ hasText: "alpha saved draft" }).count() === 1, "the matching history item");
    expect(await harness.page.locator(historyItemSelector).filter({ hasText: "beta saved draft" }).count()).toBe(0);

    const popup = await harness.page.locator(historyRootSelector).boundingBox();
    const editor = await harness.page.locator(editorSelector).boundingBox();
    expect(popup).not.toBeNull();
    expect(editor).not.toBeNull();
    expect(Math.abs(popup!.x - editor!.x)).toBeLessThan(1);
    expect(editor!.y - (popup!.y + popup!.height)).toBeGreaterThanOrEqual(0);
  } finally {
    await harness.cleanup();
  }
});

test("ArrowUp opens history only from the first visual line", async () => {
  const harness = await createHarness();
  try {
    await sendText(harness.page, "first-line history");
    const twoLines = "<div>first visual line</div><div>second visual line</div>";

    await resetEditor(harness.page, twoLines);
    await harness.page.keyboard.press("ArrowUp");
    expect(await menuVisible(harness.page)).toBe(false);

    await resetEditor(harness.page, twoLines, 1);
    await harness.page.keyboard.press("ArrowUp");
    await waitForMenu(harness.page);
  } finally {
    await harness.cleanup();
  }
});

test("ArrowUp and ArrowDown change the selected entry; Enter restores it without sending", async () => {
  const harness = await createHarness();
  try {
    const entries = ["keyboard entry one", "keyboard entry two", "keyboard entry three"];
    for (const entry of entries) await sendText(harness.page, entry);
    await openHistory(harness.page);

    await harness.page.keyboard.press("ArrowDown");
    const before = await selectedEntry(harness.page, entries);
    await harness.page.keyboard.press("ArrowDown");
    const afterDown = await selectedEntry(harness.page, entries);
    expect(afterDown).not.toBe(before);
    await harness.page.keyboard.press("ArrowUp");
    const afterUp = await selectedEntry(harness.page, entries);
    expect(afterUp).toBe(before);

    await harness.page.keyboard.press("Enter");
    expect(await harness.page.locator(editorSelector).innerText()).toBe(before);
    expect((await harness.state()).sentTexts).toHaveLength(entries.length);
    expect(await menuVisible(harness.page)).toBe(false);
  } finally {
    await harness.cleanup();
  }
});

test("Tab and clicking a history entry restore it without sending", async () => {
  const harness = await createHarness();
  try {
    const entries = ["restore by tab", "restore by click"];
    for (const entry of entries) await sendText(harness.page, entry);

    await openHistory(harness.page);
    await harness.page.keyboard.press("ArrowDown");
    const tabEntry = await selectedEntry(harness.page, entries);
    await harness.page.keyboard.press("Tab");
    expect(await harness.page.locator(editorSelector).innerText()).toBe(tabEntry);
    expect((await harness.state()).sentTexts).toHaveLength(entries.length);

    await openHistory(harness.page);
    await harness.page.locator(historyItemSelector).filter({ hasText: "restore by click" }).click();
    expect(await harness.page.locator(editorSelector).innerText()).toBe("restore by click");
    expect((await harness.state()).sentTexts).toHaveLength(entries.length);
  } finally {
    await harness.cleanup();
  }
});

test("Escape dismisses history and leaves the current draft untouched", async () => {
  const harness = await createHarness();
  try {
    await sendText(harness.page, "draft to keep");
    await openHistory(harness.page, "draft");
    await harness.page.keyboard.press("Escape");
    expect(await menuVisible(harness.page)).toBe(false);
    expect(await harness.page.locator(editorSelector).innerText()).toBe("draft");
    expect((await harness.state()).sentTexts).toHaveLength(1);
  } finally {
    await harness.cleanup();
  }
});

test("IME composition does not open history, select an entry, or send", async () => {
  const harness = await createHarness();
  try {
    await sendText(harness.page, "composed history");
    await resetEditor(harness.page, "composed");
    await harness.page.evaluate(() => {
      const editor = document.querySelector('.wk-messageinput-editor [contenteditable="true"]');
      if (!(editor instanceof HTMLElement)) throw new Error("fixture composer is missing");
      editor.dispatchEvent(new KeyboardEvent("keydown", {
        key: "ArrowUp",
        code: "ArrowUp",
        keyCode: 229,
        isComposing: true,
        bubbles: true,
        cancelable: true,
      }));
    });
    expect(await menuVisible(harness.page)).toBe(false);

    await openHistory(harness.page, "composed");
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
    expect(await harness.page.locator(editorSelector).innerText()).toBe("composed");
    expect((await harness.state()).sentTexts).toHaveLength(1);
  } finally {
    await harness.cleanup();
  }
});

test("normal text still reaches the editor send path", async () => {
  const harness = await createHarness();
  try {
    await typeText(harness.page, "send this message");
    await harness.page.keyboard.press("Enter");
    await waitFor(async () => (await stateOf(harness.page)).sentTexts.length === 1, "the normal editor send");
    expect((await harness.state()).sentTexts).toEqual(["send this message"]);
    expect(await menuVisible(harness.page)).toBe(false);
  } finally {
    await harness.cleanup();
  }
});

test("multiline input is restored with its line breaks", async () => {
  const harness = await createHarness();
  try {
    await resetEditor(harness.page, "<p>first line</p><p>second line</p>");
    await harness.page.keyboard.press("Enter");
    await waitFor(async () => (await harness.state()).sentTexts.length === 1, "the multiline submission");
    await waitFor(async () => await harness.page.locator(editorSelector).innerText() === "", "the consumed editor");
    await openHistory(harness.page);
    await harness.page.keyboard.press("Enter");
    expect(await harness.page.locator(editorSelector).innerText()).toBe("first line\nsecond line");
  } finally {
    await harness.cleanup();
  }
});

test("restored history retains structured mentions", async () => {
  const harness = await createHarness();
  try {
    await resetEditor(harness.page, '<p><span class="mention" data-type="mention" data-id="bot-1" data-label="Bot" contenteditable="false">@Bot</span> hello</p><p>second line</p>');
    await harness.page.keyboard.press("Enter");
    await waitFor(async () => (await harness.state()).sentTexts.length === 1, "the mention submission");
    await waitFor(async () => await harness.page.locator(editorSelector).innerText() === "", "the consumed editor");
    await openHistory(harness.page);
    await harness.page.keyboard.press("Enter");
    const mention = harness.page.locator(`${editorSelector} [data-type="mention"]`);
    expect(await mention.getAttribute("data-id")).toBe("bot-1");
    expect(await mention.getAttribute("data-label")).toBe("Bot");
    expect(await harness.page.locator(editorSelector).innerText()).toBe("@Bot hello\nsecond line");
  } finally {
    await harness.cleanup();
  }
});

test("slash command suggestions remain usable with input history installed", async () => {
  const harness = await createHarness();
  try {
    await typeText(harness.page, "/stat");
    await waitFor(async () => await harness.page.locator(`${commandRootSelector}:not([hidden])`).count() === 1, "the slash suggestion menu");
    await waitFor(async () => await harness.page.locator(`${commandItemSelector}[data-command-name="/status"]`).count() === 1, "the status command suggestion");
    expect(await harness.page.locator(`${historyRootSelector}:not([hidden])`).count()).toBe(0);

    await harness.page.keyboard.press("ArrowDown");
    await harness.page.keyboard.press("Enter");
    expect(await harness.page.locator(editorSelector).innerText()).toMatch(/^\/status\s*$/);
    expect((await harness.state()).sentTexts).toHaveLength(0);
  } finally {
    await harness.cleanup();
  }
});

test("native mention suggestions keep the arrow keys", async () => {
  const harness = await createHarness();
  try {
    await sendText(harness.page, "earlier message");
    await resetEditor(harness.page, "@Bot");
    await harness.page.evaluate(() => {
      const menu = document.createElement("div");
      menu.className = "mention-list";
      menu.textContent = "Bot";
      document.body.append(menu);
    });
    await harness.page.keyboard.press("ArrowUp");
    expect(await menuVisible(harness.page)).toBe(false);
  } finally {
    await harness.cleanup();
  }
});
