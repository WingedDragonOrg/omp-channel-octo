import * as assert from "node:assert/strict";
import { existsSync } from "node:fs";
import { mkdtemp, realpath, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
import { mock } from "bun:test";
import * as processHelpers from "../../src/omp/process.ts";
import type { CommitResult, WorkerProcess } from "../../src/contracts.ts";
import type { RpcChild } from "../../src/omp/process.ts";

const terminateOwnedProcessMock = mock(async () => false);
const watchdogs: RpcChild[] = [];
const realCreateWatchdog = processHelpers.createWatchdog;
const createWatchdogWithTracking = async (...args: Parameters<typeof realCreateWatchdog>) => {
  const result = await realCreateWatchdog(...args);
  if (result.ok) watchdogs.push(result.watchdog);
  return result;
};
mock.module(resolve(import.meta.dir, "../../src/omp/process.ts"), () => ({
  ...processHelpers,
  createWatchdog: createWatchdogWithTracking,
  terminateOwnedProcess: terminateOwnedProcessMock,
}));

// Import after mocking so this isolated subprocess can reproduce a failed stop without a test-only production API.
const { createNativeCommitWorker } = await import("../../src/omp/commit.ts");
const fixturePath = resolve(import.meta.dir, "native-commit-cli.ts");

function git(cwd: string, ...args: string[]): string {
  const result = Bun.spawnSync(["git", ...args], {
    cwd,
    stdin: "ignore",
    stdout: "pipe",
    stderr: "pipe",
  });
  const stdout = new TextDecoder().decode(result.stdout).trim();
  if (result.exitCode !== 0) {
    throw new Error(`git ${args.join(" ")} failed: ${new TextDecoder().decode(result.stderr)}`);
  }
  return stdout;
}

async function waitUntil(predicate: () => boolean, timeoutMs = 4_000): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    if (predicate()) return;
    await Bun.sleep(10);
  }
  throw new Error(`condition was not met within ${timeoutMs}ms`);
}

function watchdogPidFor(workerPid: number, workerPgid: number): number | undefined {
  const result = Bun.spawnSync(["/bin/ps", "-ww", "-axo", "pid=,pgid=,command="], {
    stdin: "ignore",
    stdout: "pipe",
    stderr: "ignore",
  });
  const lines = new TextDecoder().decode(result.stdout).split("\n");
  for (const line of lines) {
    if (!line.includes("__omp_watchdog_ready__") || !line.includes(String(workerPid))) continue;
    const match = line.trim().match(/^(\d+)\s+(\d+)\s+/u);
    if (!match) continue;
    const pid = Number(match[1]);
    const pgid = Number(match[2]);
    if (pid > 1 && pid !== workerPid && pgid !== workerPgid) return pid;
  }
  return undefined;
}

async function main(): Promise<void> {
  const root = await realpath(await mkdtemp(join(tmpdir(), "native-commit-stop-settlement-")));
  const config = {
    scenario: "hang",
    pidPath: join(root, "commit-child.pid"),
    readyPath: join(root, "commit-child.ready"),
    descendantPidPath: join(root, "commit-descendant.pid"),
  };
  const configPath = join(root, "fixture-config.json");
  let processInfo: WorkerProcess | undefined;
  let runPromise: Promise<CommitResult> | undefined;
  let worker: Awaited<ReturnType<typeof createNativeCommitWorker>> | undefined;
  let watchdogPid: number | undefined;
  let result: {
    stopResult: boolean;
    runStatus: "rejected";
    groupAliveAfterStop: boolean;
    watchdogAliveAfterStop: boolean;
    watchdogReaped: boolean;
  } | undefined;

  try {
    await writeFile(configPath, JSON.stringify(config));
    git(root, "init", "--quiet", "-b", "main");
    git(root, "config", "user.name", "Commit Stop Settlement Test");
    git(root, "config", "user.email", "commit-stop-settlement@example.invalid");
    await writeFile(join(root, "tracked.txt"), "initial\n");
    git(root, "add", "tracked.txt");
    git(root, "commit", "--quiet", "-m", "initial fixture commit");

    worker = await createNativeCommitWorker({
      ompCommand: process.execPath,
      extraArgs: [fixturePath, "--fixture-config", configPath],
      startupTimeoutMs: 2_000,
      cancelTimeoutMs: 350,
      commit: {
        cwd: root,
        onSpawn: info => {
          processInfo = info;
        },
      },
    });
    runPromise = worker.run();
    const settlement = runPromise.then(
      value => ({ kind: "resolved" as const, value }),
      error => ({ kind: "rejected" as const, error }),
    );
    await waitUntil(() => existsSync(config.readyPath));

    const stopResult = await worker.stop();
    assert.equal(stopResult, false);
    assert.ok(processInfo);
    const groupAliveAfterStop = processHelpers.groupAlive(processInfo.pgid);
    assert.equal(groupAliveAfterStop, true);
    watchdogPid = watchdogPidFor(processInfo.pid, processInfo.pgid);
    assert.ok(watchdogPid, "the real parent-death watchdog should remain running after stop=false");
    const watchdogAliveAfterStop = processHelpers.processAlive(watchdogPid);
    assert.equal(watchdogAliveAfterStop, true);

    const outcome = await Promise.race([
      settlement,
      Bun.sleep(250).then(() => ({ kind: "pending" as const })),
    ]);
    assert.equal(outcome.kind, "rejected", "run must settle promptly when process-group stop is unconfirmed");
    if (outcome.kind === "rejected") assert.match(String(outcome.error), /unknown/i);
    result = {
      stopResult,
      runStatus: "rejected",
      groupAliveAfterStop,
      watchdogAliveAfterStop,
      watchdogReaped: false,
    };
  } finally {
    if (processInfo && processHelpers.groupAlive(processInfo.pgid)) {
      assert.equal(await processHelpers.terminateProcessGroup(processInfo.pgid, 500), true);
      await waitUntil(() => !processHelpers.groupAlive(processInfo!.pgid));
    }
    if (runPromise) await runPromise.catch(() => undefined);
    if (watchdogs.length > 0) {
      const reaped = await Promise.all(
        watchdogs.map(async watchdog => {
          try {
            await waitUntil(() => !processHelpers.processAlive(watchdog.pid), 2_000);
            return true;
          } catch {
            processHelpers.stopWatchdog(watchdog);
            await processHelpers.waitForExit(watchdog, 1_000);
            return false;
          }
        }),
      );
      if (result) result.watchdogReaped = reaped.every(Boolean);
      assert.ok(reaped.every(Boolean), "the watchdog should self-reap after the worker group exits");
    }
    await rm(root, { recursive: true, force: true });
  }

  assert.ok(result);
  process.stdout.write(`${JSON.stringify(result)}\n`);
}

main().catch(error => {
  process.stderr.write(`${error instanceof Error ? error.stack ?? error.message : String(error)}\n`);
  process.exitCode = 1;
});
