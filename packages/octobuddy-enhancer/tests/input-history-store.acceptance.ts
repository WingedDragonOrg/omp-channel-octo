import { expect, test } from "bun:test";
import { existsSync } from "node:fs";
import { mkdtemp, rm, stat } from "node:fs/promises";
import { tmpdir } from "node:os";
import { dirname, join } from "node:path";
import { InputHistoryStore } from "../src/host/input-history.ts";

const chatKey = JSON.stringify([2, "group-1"]);
const otherChatKey = JSON.stringify([5, "group-1____thread-1"]);

async function withStorePath(run: (databasePath: string) => Promise<void>): Promise<void> {
  const root = await mkdtemp(join(tmpdir(), "octobuddy-input-history-"));
  try {
    await run(join(root, "private", "input-history.sqlite"));
  } finally {
    await rm(root, { recursive: true, force: true });
  }
}

test("default history database is stored under the oco data directory", async () => {
  const home = await mkdtemp(join(tmpdir(), "octobuddy-history-home-"));
  try {
    const moduleUrl = new URL("../src/host/input-history.ts", import.meta.url).href;
    const process = Bun.spawn([Bun.which("bun")!, "-e", `
      import { InputHistoryStore } from ${JSON.stringify(moduleUrl)};
      const store = new InputHistoryStore();
      await store.appendInputHistory(${JSON.stringify(chatKey)}, { text: "saved" });
      await store.close();
    `], { env: { ...Bun.env, HOME: home }, stdout: "pipe", stderr: "pipe" });
    const error = await new Response(process.stderr).text();
    expect(await process.exited, error).toBe(0);
    expect(existsSync(join(home, ".oco", "data", "octo-buddy.sqlite"))).toBe(true);
  } finally {
    await rm(home, { recursive: true, force: true });
  }
});

test("opens lazily with private permissions and persists structured entries across store recreation", async () => {
  await withStorePath(async (databasePath) => {
    const first = new InputHistoryStore(databasePath);
    expect(existsSync(databasePath)).toBe(false);
    expect(await first.loadInputHistory(chatKey)).toEqual({ limit: 100, entries: [] });

    const entry = {
      text: "Review @[uid:42]",
      mentions: [{ start: 7, end: 17, id: "42", label: "Ada", char: "@" }],
    };
    expect(await first.appendInputHistory(chatKey, entry)).toEqual({ limit: 100, entries: [entry] });

    expect((await stat(dirname(databasePath))).mode & 0o777).toBe(0o700);
    expect((await stat(databasePath)).mode & 0o777).toBe(0o600);
    await first.close();

    const restarted = new InputHistoryStore(databasePath);
    expect(await restarted.loadInputHistory(chatKey)).toEqual({ limit: 100, entries: [entry] });
    await restarted.close();
  });
});

test("orders entries newest-first and trims every chat atomically when the global limit changes", async () => {
  await withStorePath(async (databasePath) => {
    const store = new InputHistoryStore(databasePath);
    await store.appendInputHistory(chatKey, { text: "oldest" });
    await store.appendInputHistory(chatKey, { text: "middle" });
    await store.appendInputHistory(chatKey, { text: "newest" });
    await store.appendInputHistory(otherChatKey, { text: "other oldest" });
    await store.appendInputHistory(otherChatKey, { text: "other newest" });

    expect(await store.setInputHistoryLimit(2)).toBe(2);
    expect(await store.loadInputHistory(chatKey)).toEqual({
      limit: 2,
      entries: [{ text: "newest" }, { text: "middle" }],
    });
    expect(await store.loadInputHistory(otherChatKey)).toEqual({
      limit: 2,
      entries: [{ text: "other newest" }, { text: "other oldest" }],
    });
    await store.close();
    const restarted = new InputHistoryStore(databasePath);
    expect(await restarted.loadInputHistory(chatKey)).toEqual({
      limit: 2,
      entries: [{ text: "newest" }, { text: "middle" }],
    });
    expect(await restarted.loadInputHistory(otherChatKey)).toEqual({
      limit: 2,
      entries: [{ text: "other newest" }, { text: "other oldest" }],
    });
    await restarted.close();
  });
});

test("imports legacy entries once without replacing existing entries or changing their order", async () => {
  await withStorePath(async (databasePath) => {
    const store = new InputHistoryStore(databasePath);
    await store.appendInputHistory(chatKey, { text: "already persisted" });
    const importedNewest = { text: "legacy newest", mentions: [{ start: 0, end: 5, id: "7" }] };
    const importedOldest = { text: "legacy oldest" };
    const imported = [importedNewest, importedOldest];

    expect(await store.importInputHistory(chatKey, imported, 10)).toEqual({
      limit: 10,
      entries: [{ text: "already persisted" }, ...imported],
    });
    await store.close();
    const restarted = new InputHistoryStore(databasePath);
    expect(await restarted.importInputHistory(chatKey, [{ text: "must not be imported twice" }], 1)).toEqual({
      limit: 10,
      entries: [{ text: "already persisted" }, ...imported],
    });

    await restarted.setInputHistoryLimit(2);
    expect(await restarted.loadInputHistory(chatKey)).toEqual({
      limit: 2,
      entries: [{ text: "already persisted" }, importedNewest],
    });
    await restarted.close();
  });
});
