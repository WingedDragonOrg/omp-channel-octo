import { expect, test } from "bun:test";
import { mkdtemp, readFile, rm, utimes, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import {
  followLog,
  installLogTee,
  logDateStamp,
  logFileName,
  newestLogFile,
  pruneLogFiles,
  readLogTail,
} from "../src/runtime/logs.ts";

async function tempDir(prefix: string): Promise<string> {
  return await mkdtemp(join(tmpdir(), prefix));
}

/**
 * `followLog` is a filesystem polling loop, so the test waits for the output it actually
 * observed rather than for a guessed duration.
 */
async function waitForText(seen: readonly string[], needle: string, timeoutMs = 3_000): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    if (seen.join("").includes(needle)) return;
    await Bun.sleep(10);
  }
  throw new Error(`followed log never produced ${needle}`);
}

test("log files are named after the local day", () => {
  const date = new Date(2026, 8, 16, 23, 30, 5);
  expect(logDateStamp(date)).toBe("2026-09-16");
  expect(logFileName(date)).toBe("channel-2026-09-16.log");
});

test("the tee mirrors console output into the day's file with a timestamp prefix", async () => {
  const root = await tempDir("oco-logs-tee-");
  const terminal: string[] = [];
  const originalLog = console.log;
  const originalError = console.error;
  let tee: ReturnType<typeof installLogTee> | undefined;
  try {
    console.log = (...args: unknown[]) => {
      terminal.push(args.map((value) => String(value)).join(" "));
    };
    console.error = (...args: unknown[]) => {
      terminal.push(args.map((value) => String(value)).join(" "));
    };
    tee = installLogTee({ directory: root, now: () => new Date(2026, 8, 16, 10, 11, 12) });
    console.log("first line");
    console.error("second\nthird");
    await tee.close();
    tee = undefined;

    expect(terminal).toEqual(["first line", "second\nthird"]);
    const written = await readFile(join(root, "channel-2026-09-16.log"), "utf8");
    expect(written).toBe(
      "[2026-09-16 10:11:12] first line\n"
      + "[2026-09-16 10:11:12] second\n"
      + "[2026-09-16 10:11:12] third\n",
    );
  } finally {
    if (tee) await tee.close();
    console.log = originalLog;
    console.error = originalError;
    await rm(root, { recursive: true, force: true });
  }
});

test("the tee rolls into the next day's file instead of growing one file forever", async () => {
  const root = await tempDir("oco-logs-rollover-");
  let day = new Date(2026, 8, 16, 23, 59, 0);
  const tee = installLogTee({ directory: root, now: () => day });
  try {
    console.log("before midnight");
    day = new Date(2026, 8, 17, 0, 1, 0);
    console.log("after midnight");
  } finally {
    await tee.close();
  }
  try {
    expect(await readFile(join(root, "channel-2026-09-16.log"), "utf8")).toContain("[2026-09-16 23:59:00] before midnight");
    expect(await readFile(join(root, "channel-2026-09-17.log"), "utf8")).toContain("[2026-09-17 00:01:00] after midnight");
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});

test("pruning keeps the retention window and ignores unrelated files", async () => {
  const root = await tempDir("oco-logs-prune-");
  try {
    const stale = join(root, "channel-2026-08-01.log");
    const fresh = join(root, "channel-2026-09-15.log");
    const other = join(root, "launchd.out.log");
    await writeFile(stale, "old\n", "utf8");
    await writeFile(fresh, "new\n", "utf8");
    await writeFile(other, "keep\n", "utf8");
    const old = new Date(2026, 7, 1).getTime() / 1000;
    await utimes(stale, old, old);

    const removed = await pruneLogFiles({ directory: root, now: new Date(2026, 8, 16) });
    expect(removed).toEqual([stale]);
    expect(await newestLogFile(root)).toBe(fresh);
    expect(await readFile(other, "utf8")).toBe("keep\n");
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});

test("the tail reader returns whole trailing lines only", async () => {
  const root = await tempDir("oco-logs-tail-");
  try {
    const file = join(root, "channel-2026-09-16.log");
    const lines = Array.from({ length: 50 }, (_, index) => `line ${index}`).join("\n");
    await writeFile(file, `${lines}\n`, "utf8");
    expect(await readLogTail(file, 3)).toBe("line 47\nline 48\nline 49");
    expect(await readLogTail(file, 1)).toBe("line 49");
    expect((await readLogTail(file, 100)).split("\n")).toHaveLength(50);
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});

test("following prints what is appended and picks up the next day's file", async () => {
  const root = await tempDir("oco-logs-follow-");
  const controller = new AbortController();
  try {
    const first = join(root, "channel-2026-09-16.log");
    await writeFile(first, "[2026-09-16 23:59:59] already there\n", "utf8");
    const seen: string[] = [];
    const ready = Promise.withResolvers<void>();
    const following = followLog({
      directory: root,
      signal: controller.signal,
      pollMs: 10,
      write: (text) => seen.push(text),
      onReady: () => ready.resolve(),
    });
    await ready.promise;

    await writeFile(first, "[2026-09-17 00:00:01] appended\n", { flag: "a" });
    await waitForText(seen, "appended");
    const rolled = join(root, "channel-2026-09-17.log");
    await writeFile(rolled, "[2026-09-17 00:00:02] new file\n", "utf8");
    await waitForText(seen, "new file");

    controller.abort();
    await following;
    const text = seen.join("");
    expect(text).toContain("appended");
    expect(text).toContain("new file");
    expect(text).not.toContain("already there");
  } finally {
    controller.abort();
    await rm(root, { recursive: true, force: true });
  }
});
