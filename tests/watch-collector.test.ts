import { expect, test } from "bun:test";
import { existsSync } from "node:fs";
import { access, mkdtemp, readFile, realpath, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { collectWatch } from "../src/watch/collector.ts";
import type { WatchSpec } from "../src/watch/types.ts";

function spec(argv: string[], cwd: string): WatchSpec {
  return { argv, cwd, condition: "the command reports completion", intervalMs: 60_000, timeoutMs: 600_000 };
}

async function withTempDirectory<T>(run: (directory: string) => Promise<T>): Promise<T> {
  const directory = await realpath(await mkdtemp(join(tmpdir(), "octo-watch-collector-")));
  try {
    return await run(directory);
  } finally {
    await rm(directory, { recursive: true, force: true });
  }
}

// These waits observe real child-process and filesystem state; fake timers cannot advance either.
async function waitForFile(path: string, timeoutMs = 5_000): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    try {
      await access(path);
      return;
    } catch {
      await Bun.sleep(10);
    }
  }
  throw new Error(`child did not create ${path} before timeout`);
}

function processGroupAlive(pgid: number): boolean {
  try {
    process.kill(-pgid, 0);
    return true;
  } catch (error) {
    return typeof error === "object" && error !== null && "code" in error && error.code === "EPERM";
  }
}

// Process-group disappearance is an OS observation; wait for that event rather than a guessed delay.
async function waitForGroupGone(pgid: number, timeoutMs = 5_000): Promise<boolean> {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline && processGroupAlive(pgid)) await Bun.sleep(20);
  return !processGroupAlive(pgid);
}

test("executes literal argv in the registered cwd with CLI allowlisting and persists identity before side effects", async () => {
  await withTempDirectory(async (directory) => {
    const marker = join(directory, "must-not-be-created");
    const literalArg = `; touch ${marker}`;
    const script = `const marker = process.argv.indexOf("watch-argv-marker"); process.stdout.write(JSON.stringify({ cwd: process.cwd(), args: process.argv.slice(marker), decisionKey: process.env.OMP_CHANNEL_OCTO_WATCH_DECISION_KEY ?? null, botId: process.env.OCTO_BOT_ID ?? null }));`;
    const events: string[] = [];
    let ownedGroup: number | undefined;
    const result = await collectWatch(
      spec([process.execPath, "-e", script, "watch-argv-marker", literalArg], directory),
      new AbortController().signal,
      (identity) => {
        events.push("spawn");
        ownedGroup = identity.pgid;
        expect(identity.pid).toBeGreaterThan(1);
        expect(identity.pgid).toBeGreaterThan(1);
        expect(identity.identity).toContain(`pid=${identity.pid};`);
        expect(existsSync(marker)).toBe(false);
      },
      () => events.push("exit"),
      {
        OCTO_CONFIG_DIR: "/private/bot-config",
        OCTO_API_BASE_URL: "https://example.invalid/",
        OCTO_BOT_ID: "bot-watch-test",
        OMP_CHANNEL_OCTO_WATCH_DECISION_KEY: "synthetic-provider-secret",
      },
    );

    expect(result.kind).toBe("success");
    if (result.kind !== "success") throw new Error("expected complete command output");
    expect(JSON.parse(result.stdout)).toEqual({
      cwd: directory,
      args: ["watch-argv-marker", literalArg],
      decisionKey: null,
      botId: "bot-watch-test",
    });
    expect(events).toEqual(["spawn", "exit"]);
    expect(ownedGroup).toBeGreaterThan(1);
    expect(await waitForGroupGone(ownedGroup!)).toBe(true);
    expect(existsSync(marker)).toBe(false);
  });
});

test("cancellation terminates the complete owned process group before onExit", async () => {
  await withTempDirectory(async (directory) => {
    const childPidPath = join(directory, "grandchild.pid");
    const controller = new AbortController();
    let ownedGroup: number | undefined;
    let exitCallbackSawGroupGone = false;
    const pending = collectWatch(
      spec(["/bin/sh", "-c", "sleep 60 & echo $! > grandchild.pid; wait"], directory),
      controller.signal,
      (identity) => { ownedGroup = identity.pgid; },
      () => {
        exitCallbackSawGroupGone = ownedGroup !== undefined && !processGroupAlive(ownedGroup);
      },
    );

    await waitForFile(childPidPath);
    const childPid = Number((await readFile(childPidPath, "utf8")).trim());
    expect(Number.isSafeInteger(childPid)).toBe(true);
    expect(ownedGroup).toBeGreaterThan(1);
    controller.abort(new DOMException("cancel the watch check", "AbortError"));

    const result = await pending;
    expect(result.kind).toBe("failure");
    expect(exitCallbackSawGroupGone).toBe(true);
    expect(await waitForGroupGone(ownedGroup!)).toBe(true);
    expect(() => process.kill(childPid, 0)).toThrow();
  });
}, 10_000);
test("reports confirmed shutdown after cancellation during persisted process startup", async () => {
  await withTempDirectory(async (directory) => {
    const marker = join(directory, "command-must-not-run");
    const controller = new AbortController();
    let ownedGroup: number | undefined;
    let exitCallbackSawGroupGone = false;

    const result = await collectWatch(
      spec([process.execPath, "-e", `require("node:fs").writeFileSync(${JSON.stringify(marker)}, "ran")`], directory),
      controller.signal,
      (identity) => {
        ownedGroup = identity.pgid;
        controller.abort(new DOMException("cancel parked watch startup", "AbortError"));
      },
      () => {
        exitCallbackSawGroupGone = ownedGroup !== undefined && !processGroupAlive(ownedGroup);
      },
    );

    expect(result.kind).toBe("failure");
    expect(ownedGroup).toBeGreaterThan(1);
    expect(exitCallbackSawGroupGone).toBe(true);
    expect(await waitForGroupGone(ownedGroup!)).toBe(true);
    expect(existsSync(marker)).toBe(false);
  });
}, 10_000);

test("propagates a process-identity persistence failure before the command runs and reaps the group", async () => {
  await withTempDirectory(async (directory) => {
    const marker = join(directory, "must-not-run-after-identity-failure");
    let ownedGroup: number | undefined;
    let exits = 0;

    const failure = await collectWatch(
      spec([process.execPath, "-e", `require("node:fs").writeFileSync(${JSON.stringify(marker)}, "ran")`], directory),
      new AbortController().signal,
      (identity) => {
        ownedGroup = identity.pgid;
        throw new Error("ledger write failed");
      },
      () => { exits += 1; },
    ).then(
      () => undefined,
      (error: unknown) => error,
    );

    expect(failure).toBeInstanceOf(Error);
    expect((failure as Error).message).toContain("ledger write failed");
    expect(ownedGroup).toBeGreaterThan(1);
    expect(await waitForGroupGone(ownedGroup!)).toBe(true);
    expect(existsSync(marker)).toBe(false);
    expect(exits).toBe(0);
  });
}, 10_000);

test("stdout exceeding the byte budget is a failure and never successful truncated evidence", async () => {
  await withTempDirectory(async (directory) => {
    const result = await collectWatch(
      spec([process.execPath, "-e", "process.stdout.write('💡'.repeat(4097))"], directory),
      new AbortController().signal,
      () => undefined,
      () => undefined,
    );

    expect(result.kind).toBe("failure");
    expect(result.kind === "success" ? Buffer.byteLength(result.stdout) : 0).toBe(0);
  });
}, 10_000);

// A real event-loop handle keeps the OS child alive until process-group termination can be observed.
test("stops the complete process group promptly when streaming stdout crosses its byte budget", async () => {
  await withTempDirectory(async (directory) => {
    const script = `process.stdout.write(Buffer.alloc(${16 * 1024 + 1}, 120)); setInterval(() => {}, 1000);`;
    const shellCommand = `${JSON.stringify(process.execPath)} -e '${script}' & wait`;
    const controller = new AbortController();
    const startedAt = Date.now();
    let ownedGroup: number | undefined;
    let exitCallbackSawGroupGone = false;

    const result = await collectWatch(
      spec(["/bin/sh", "-c", shellCommand], directory),
      controller.signal,
      (identity) => { ownedGroup = identity.pgid; },
      () => {
        exitCallbackSawGroupGone = ownedGroup !== undefined && !processGroupAlive(ownedGroup);
      },
    );

    expect(result.kind).toBe("failure");
    if (result.kind !== "failure") throw new Error("overflow must be an explicit failure");
    expect(result.code).toMatch(/output|byte/i);
    expect(Date.now() - startedAt).toBeLessThan(5_000);
    expect(exitCallbackSawGroupGone).toBe(true);
    expect(await waitForGroupGone(ownedGroup!)).toBe(true);
  });
}, 10_000);

test("stderr exceeding the byte budget is a failure rather than a successful partial collection", async () => {
  await withTempDirectory(async (directory) => {
    const result = await collectWatch(
      spec([process.execPath, "-e", "process.stderr.write('💡'.repeat(2049))"], directory),
      new AbortController().signal,
      () => undefined,
      () => undefined,
    );

    expect(result.kind).toBe("failure");
  });
}, 10_000);

test("accepts stdout and stderr exactly at their independent byte limits", async () => {
  await withTempDirectory(async (directory) => {
    const result = await collectWatch(
      spec([
        process.execPath,
        "-e",
        "process.stdout.write('💡'.repeat(4096)); process.stderr.write('💡'.repeat(2048));",
      ], directory),
      new AbortController().signal,
      () => undefined,
      () => undefined,
    );

    expect(result.kind).toBe("success");
    if (result.kind !== "success") throw new Error("output at both byte limits must be preserved");
    expect(Buffer.byteLength(result.stdout)).toBe(16 * 1024);
  });
}, 10_000);

test("preserves a non-zero executable exit as an explicit collection failure", async () => {
  await withTempDirectory(async (directory) => {
    const result = await collectWatch(
      spec([process.execPath, "-e", "process.exit(23)"], directory),
      new AbortController().signal,
      () => undefined,
      () => undefined,
    );

    expect(result.kind).toBe("failure");
    if (result.kind !== "failure") throw new Error("non-zero exit must not become evidence");
    expect(result.code).toContain("23");
  });
});

test("an unavailable fixed cwd fails instead of falling back to another directory", async () => {
  await withTempDirectory(async (directory) => {
    const result = await collectWatch(
      spec(["/bin/pwd"], join(directory, "deleted-workspace")),
      new AbortController().signal,
      () => undefined,
      () => undefined,
    );

    expect(result.kind).toBe("failure");
  });
});
