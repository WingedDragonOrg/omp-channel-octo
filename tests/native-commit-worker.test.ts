import { expect, test } from "bun:test";
import { existsSync } from "node:fs";
import { chmod, mkdtemp, readFile, realpath, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
import { ChannelStore } from "../src/store/index.ts";
import type { CommitResult, CommitWorker, OmpWorker, WorkerFactory, WorkerProcess } from "../src/contracts.ts";
import { TaskStartupError } from "../src/contracts.ts";
import { createWorkerFactory } from "../src/omp/index.ts";
import { groupAlive, terminateProcessGroup } from "../src/omp/process.ts";
import { openTrackedCommit } from "../src/runtime/worker-lease.ts";
import type { WorkerLedgerRecord } from "../src/store/store-records.ts";

const fixturePath = resolve(import.meta.dir, "fixtures/native-commit-cli.ts");
const markerExtensionPath = resolve(import.meta.dir, "fixtures/omp-extension-marker.ts");
const watchdogParentFixturePath = resolve(import.meta.dir, "fixtures/omp-watchdog-parent.ts");
const RPC_EXTRA_ARGS = [
  "--profile",
  "native-commit-test",
  "--mode",
  "rpc",
  "--approval-mode",
  "yolo",
  "--cwd",
  "/wrong/cwd",
  "--session-dir",
  "/wrong/session",
  "--resume",
  "/wrong/session.jsonl",
  "--extension",
  "/wrong/context-extension.ts",
  "-e",
  "/wrong/short-extension.ts",
  "--hook",
  "/wrong/hook.ts",
  "--no-extensions",
];

type FixtureConfig = {
  scenario: "commit" | "commit-exit-1" | "fail" | "hang" | "commit-then-hang" | "interruptible-hang";
  pidPath: string;
  readyPath: string;
  descendantPidPath: string;
  argvPath: string;
  cwdPath: string;
  startedPath: string;
  outputBytes?: number;
};

type Harness = {
  root: string;
  initialHead: string;
  config: FixtureConfig;
  factory: WorkerFactory;
  createWorker(model?: string): Promise<{ worker: CommitWorker; processInfo: WorkerProcess }>;
  run(worker: CommitWorker): Promise<CommitResult>;
};


function git(cwd: string, ...args: string[]): string {
  const result = Bun.spawnSync(["git", ...args], {
    cwd,
    stdin: "ignore",
    stdout: "pipe",
    stderr: "pipe",
  });
  const stdout = new TextDecoder().decode(result.stdout).trim();
  if (result.exitCode !== 0) {
    const stderr = new TextDecoder().decode(result.stderr);
    throw new Error(`git ${args.join(" ")} failed (${result.exitCode}): ${stderr}`);
  }
  return stdout;
}

function isProcessAlive(pid: number): boolean {
  try {
    process.kill(pid, 0);
    return true;
  } catch (error) {
    if ((error as NodeJS.ErrnoException).code === "ESRCH") return false;
    if ((error as NodeJS.ErrnoException).code === "EPERM") return true;
    throw error;
  }
}

// These checks observe real child/process-group state; no in-process event signals OS process exit.
// The bounded poll is the only way to await those external state changes deterministically.
async function waitUntil(predicate: () => boolean | Promise<boolean>, timeoutMs = 4_000): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    if (await predicate()) return;
    await Bun.sleep(10);
  }
  throw new Error(`condition was not met within ${timeoutMs}ms`);
}

async function waitForProcessExit(pid: number, timeoutMs = 4_000): Promise<void> {
  await waitUntil(() => !isProcessAlive(pid), timeoutMs);
}

async function withHarness<T>(
  scenario: FixtureConfig["scenario"],
  run: (harness: Harness) => Promise<T>,
): Promise<T> {
  const root = await realpath(await mkdtemp(join(tmpdir(), "omp-native-commit-")));
  const config: FixtureConfig = {
    scenario,
    pidPath: join(root, "commit-child.pid"),
    readyPath: join(root, "commit-child.ready"),
    descendantPidPath: join(root, "commit-descendant.pid"),
    argvPath: join(root, "commit-argv.json"),
    cwdPath: join(root, "commit-cwd.txt"),
    startedPath: join(root, "commit-child.started"),
    ...(scenario === "fail" ? { outputBytes: 1_048_576 } : {}),
  };
  const configPath = join(root, "fixture-config.json");

  let initialHead = "";
  const workers = new Map<CommitWorker, WorkerProcess>();
  const runs: Promise<unknown>[] = [];
  try {
    await writeFile(configPath, JSON.stringify(config));
    git(root, "init", "--quiet", "-b", "main");
    await writeFile(
      join(root, ".git", "info", "exclude"),
      ["fixture-config.json", "commit-child.pid", "commit-child.ready", "commit-descendant.pid", "commit-argv.json", "commit-cwd.txt", "commit-child.started", "commit-parent.ready", "commit-parent-process.json", ""].join("\n"),
    );
    git(root, "config", "user.name", "Native Commit Fixture");
    git(root, "config", "user.email", "native-commit-fixture@example.invalid");
    await writeFile(join(root, "tracked.txt"), "initial\n");
    git(root, "add", "tracked.txt");
    git(root, "commit", "--quiet", "-m", "initial fixture commit");
    initialHead = git(root, "rev-parse", "HEAD");
    await writeFile(join(root, "tracked.txt"), "change for native commit\n");

    const factory = createWorkerFactory({
      ompCommand: process.execPath,
      extraArgs: [fixturePath, "--fixture-config", configPath, ...RPC_EXTRA_ARGS],
      startupTimeoutMs: 2_000,
      cancelTimeoutMs: 350,
    });
    const harness: Harness = {
      root,
      initialHead,
      config,
      factory,
      createWorker: async (model) => {
        let processInfo: WorkerProcess | undefined;
        const worker = await factory.createCommit({
          cwd: root,
          ...(model === undefined ? {} : { model }),
          onSpawn: (info) => {
            processInfo = info;
          },
        });
        if (!processInfo) throw new Error("commit worker did not report its managed process");
        workers.set(worker, processInfo);
        return { worker, processInfo };
      },
      run: (worker) => {
        const promise = worker.run();
        runs.push(promise);
        return promise;
      },
    };
    return await run(harness);
  } finally {
    for (const [worker, processInfo] of workers) {
      const stopped = await worker.stop().catch(() => false);
      if (!stopped && groupAlive(processInfo.pgid)) await terminateProcessGroup(processInfo.pgid, 350);
    }
    await Promise.all(runs.map(promise => promise.catch(() => undefined)));
    await rm(root, { recursive: true, force: true });
  }
}

async function readFixtureArgv(config: FixtureConfig): Promise<string[]> {
  return JSON.parse(await readFile(config.argvPath, "utf8")) as string[];
}

function argumentValue(argv: string[], flag: string): string | undefined {
  for (let index = argv.length - 1; index >= 0; index -= 1) {
    const value = argv[index]!;
    if (value === flag) return argv[index + 1];
    if (value.startsWith(`${flag}=`)) return value.slice(flag.length + 1);
  }
  return undefined;
}

function processesContaining(text: string): number[] {
  const result = Bun.spawnSync(["/bin/ps", "-ww", "-axo", "pid=,command="], {
    stdin: "ignore",
    stdout: "pipe",
    stderr: "ignore",
  });
  return new TextDecoder()
    .decode(result.stdout)
    .split("\n")
    .filter(line => line.includes(text))
    .map(line => Number(line.trim().match(/^\d+/u)?.[0]))
    .filter(pid => Number.isSafeInteger(pid) && pid > 1);
}

test("run 前无 Git 副作用，执行后返回真实 HEAD 变化", () =>
  withHarness("commit", async (harness) => {
    const { worker } = await harness.createWorker();
    expect(existsSync(harness.config.pidPath)).toBe(false);
    expect(git(harness.root, "rev-parse", "HEAD")).toBe(harness.initialHead);

    const result = await harness.run(worker);
    const headAfter = git(harness.root, "rev-parse", "HEAD");
    expect(result).toEqual({ exitCode: 0, headBefore: harness.initialHead, headAfter });
    expect(headAfter).not.toBe(harness.initialHead);
    expect(git(harness.root, "status", "--porcelain")).toBe("");
  }));

if (process.env.OMP_REAL_EXTENSION_SMOKE === "1") {
  test("real OMP RPC startup loads a configured extension and writes its marker", () =>
    withHarness("commit", async (harness) => {
      const markerPath = join(harness.root, "omp-extension.marker");
      const originalMarker = process.env.OMP_EXTENSION_MARKER;
      process.env.OMP_EXTENSION_MARKER = markerPath;

      let factory: ReturnType<typeof createWorkerFactory> | undefined;
      let worker: OmpWorker | undefined;
      let processInfo: WorkerProcess | undefined;
      try {
        factory = createWorkerFactory({
          ompCommand: "omp",
          extraArgs: ["--extension", markerExtensionPath],
          startupTimeoutMs: 30_000,
          cancelTimeoutMs: 1_000,
        });
        worker = await factory.create({
          cwd: harness.root,
          sessionDir: harness.root,
          onSpawn: info => {
            processInfo = info;
          },
        });
        await waitUntil(() => existsSync(markerPath), 30_000);
        expect(await readFile(markerPath, "utf8")).toBe("loaded\n");
      } finally {
        try {
          let stopError: unknown;
          if (worker) {
            try {
              if (!(await worker.stop())) stopError = new Error("real OMP extension smoke worker did not stop");
            } catch (error) {
              stopError = error;
            }
          }
          if (processInfo && factory) {
            try {
              if (!(await factory.recover(processInfo))) {
                throw new Error("real OMP extension smoke process group could not be recovered");
              }
            } catch (error) {
              if (stopError !== undefined) {
                throw new AggregateError([stopError, error], "real OMP extension smoke cleanup failed");
              }
              throw error;
            }
          }
          if (stopError !== undefined) throw stopError;
        } finally {
          if (originalMarker === undefined) delete process.env.OMP_EXTENSION_MARKER;
          else process.env.OMP_EXTENSION_MARKER = originalMarker;
        }
      }
    }), 60_000);
}

test("读取 Git HEAD 的非 unborn 错误必须拒绝且不得释放 commit gate", () =>
  withHarness("commit", async (harness) => {
    const shimDir = await mkdtemp(join(tmpdir(), "native-commit-git-shim-"));
    const shimPath = join(shimDir, "git");
    await writeFile(
      shimPath,
      [
        "#!/bin/sh",
        'if [ "$1" = "rev-parse" ]; then',
        '  printf "fixture git read failure\\\\n" >&2',
        "  exit 2",
        "fi",
        'exec /usr/bin/git "$@"',
        "",
      ].join("\n"),
    );
    await chmod(shimPath, 0o755);
    const originalPath = process.env.PATH;
    let processInfo: WorkerProcess | undefined;
    let worker: CommitWorker | undefined;
    try {
      process.env.PATH = `${shimDir}${originalPath ? `:${originalPath}` : ""}`;
      worker = await harness.factory.createCommit({
        cwd: harness.root,
        onSpawn: info => {
          processInfo = info;
        },
      });
      await expect(worker.run()).rejects.toThrow(/git|HEAD|rev-parse/i);
      expect(processInfo).toBeDefined();
      await waitUntil(() => !groupAlive(processInfo!.pgid));
      expect(existsSync(harness.config.pidPath)).toBe(false);
    } finally {
      if (worker) await worker.stop().catch(() => false);
      if (originalPath === undefined) delete process.env.PATH;
      else process.env.PATH = originalPath;
      await rm(shimDir, { recursive: true, force: true });
    }

    expect(git(harness.root, "rev-parse", "HEAD")).toBe(harness.initialHead);
  }));

test("原生 CLI 保留 profile 与指定 model，但不继承 RPC worker 参数", () =>
  withHarness("commit", async (harness) => {
    const { worker } = await harness.createWorker("fixture/model");
    await harness.run(worker);

    const argv = await readFixtureArgv(harness.config);
    expect(argv).toContain("commit");
    expect(argv).toContain("--profile");
    expect(argumentValue(argv, "--profile")).toBe("native-commit-test");
    expect(argumentValue(argv, "--model")).toBe("fixture/model");
    for (const flag of [
      "--mode",
      "--approval-mode",
      "--cwd",
      "--session-dir",
      "--resume",
      "--extension",
      "--no-extensions",
      "/wrong/context-extension.ts",
      "--push",
      "--dry-run",
      "--no-changelog",
      "--legacy",
      "--context",
      "-e",
      "--hook",
      "/wrong/short-extension.ts",
      "/wrong/hook.ts",
    ]) {
      expect(argv).not.toContain(flag);
    }
    expect(await readFile(harness.config.cwdPath, "utf8")).toBe(harness.root);
  }));

test("CLI exit 1 仍保留其已经创建的真实 Git commit", () =>
  withHarness("commit-exit-1", async (harness) => {
    const { worker } = await harness.createWorker();
    const result = await harness.run(worker);

    const headAfter = git(harness.root, "rev-parse", "HEAD");

    expect(result).toEqual({ exitCode: 1, headBefore: harness.initialHead, headAfter });
    expect(headAfter).not.toBe(harness.initialHead);
    expect(git(harness.root, "status", "--porcelain")).toBe("");
  }));

test("native CLI 非零退出时保留实际 HEAD，且 stdout/stderr 大量输出都被排空", () =>
  withHarness("fail", async (harness) => {
    const { worker } = await harness.createWorker();
    const result = await harness.run(worker);

    expect(result).toEqual({ exitCode: 7, headBefore: harness.initialHead, headAfter: harness.initialHead });
    expect(git(harness.root, "rev-parse", "HEAD")).toBe(harness.initialHead);
  }));
test("原生 CLI 命令无法启动时返回非零结果且不改变 HEAD", () =>
  withHarness("commit", async (harness) => {
    const factory = createWorkerFactory({
      ompCommand: join(harness.root, "missing-omp-command"),
      extraArgs: [],
      startupTimeoutMs: 2_000,
      cancelTimeoutMs: 350,
    });
    let worker: CommitWorker | undefined;
    let processInfo: WorkerProcess | undefined;
    try {
      worker = await factory.createCommit({
        cwd: harness.root,
        onSpawn: (info) => {
          processInfo = info;
        },
      });
      const result = await worker.run();
      expect(result.exitCode).not.toBe(0);
      expect(result.headBefore).toBe(harness.initialHead);
      expect(result.headAfter).toBe(harness.initialHead);
      expect(processInfo).toBeDefined();
      await waitUntil(() => !groupAlive(processInfo!.pgid));
    } finally {
      if (worker) {
        const stopped = await worker.stop().catch(() => false);
        if (!stopped && processInfo && groupAlive(processInfo.pgid)) {
          await terminateProcessGroup(processInfo.pgid, 350);
        }
      }
    }
  }));

test("stop 等待忽略 SIGTERM 的受管进程组及其后代全部退出", () =>
  withHarness("hang", async (harness) => {
    const { worker, processInfo } = await harness.createWorker();
    const runPromise = harness.run(worker);
    await waitUntil(() => existsSync(harness.config.readyPath));
    const descendantPid = Number(await readFile(harness.config.descendantPidPath, "utf8"));
    expect(isProcessAlive(descendantPid)).toBe(true);

    expect(await worker.stop()).toBe(true);
    await waitForProcessExit(processInfo.pid);
    await waitForProcessExit(descendantPid);
    await waitUntil(() => !groupAlive(processInfo.pgid));
    const result = await runPromise;
    expect(result.exitCode).not.toBe(0);
    expect(result.headBefore).toBe(harness.initialHead);
    expect(result.headAfter).toBe(harness.initialHead);
  }));

test("factory.recover 终止运行中的 native commit 组，不重跑且不影响其他进程组", () =>
  withHarness("hang", async (harness) => {
    const { worker, processInfo } = await harness.createWorker();
    const runPromise = harness.run(worker);
    await waitUntil(() => existsSync(harness.config.readyPath));
    const descendantPid = Number(await readFile(harness.config.descendantPidPath, "utf8"));
    const unrelatedGroup = Bun.spawn([process.execPath, "-e", "setInterval(() => {}, 1_000)"], {
      stdin: "ignore",
      stdout: "ignore",
      stderr: "ignore",
      detached: true,
    });
    try {
      expect(isProcessAlive(unrelatedGroup.pid)).toBe(true);
      expect(unrelatedGroup.pid).not.toBe(processInfo.pgid);
      expect(await harness.factory.recover(processInfo)).toBe(true);
      await waitForProcessExit(processInfo.pid);
      await waitForProcessExit(descendantPid);
      await waitUntil(() => !groupAlive(processInfo.pgid));

      const result = await runPromise;
      expect(result.exitCode).not.toBe(0);
      expect(result.headBefore).toBe(harness.initialHead);
      expect(result.headAfter).toBe(harness.initialHead);
      expect(await readFile(harness.config.startedPath, "utf8")).toBe(`${processInfo.pid}\n`);
      expect(isProcessAlive(unrelatedGroup.pid)).toBe(true);
      expect(await harness.factory.recover(processInfo)).toBe(true);
      expect(isProcessAlive(unrelatedGroup.pid)).toBe(true);
    } finally {
      if (isProcessAlive(unrelatedGroup.pid)) unrelatedGroup.kill("SIGKILL");
      await unrelatedGroup.exited;
    }
  }));

test("父进程消失时 watchdog 终止真实 commit CLI 子进程组及后代", () =>
  withHarness("interruptible-hang", async (harness) => {
    const parentReadyPath = join(harness.root, "commit-parent.ready");
    const processInfoPath = join(harness.root, "commit-parent-process.json");
    const parent = Bun.spawn(
      [
        process.execPath,
        watchdogParentFixturePath,
        "--temp-dir",
        harness.root,
        "--marker",
        harness.config.readyPath,
        "--ready",
        parentReadyPath,
        "--process-info",
        processInfoPath,
        "--native-commit-config",
        join(harness.root, "fixture-config.json"),
      ],
      {
        stdin: "ignore",
        stdout: "ignore",
        stderr: "ignore",
        detached: true,
      },
    );
    let parentKilled = false;
    let processInfo: WorkerProcess | undefined;
    try {
      await waitUntil(() => existsSync(parentReadyPath) || !isProcessAlive(parent.pid));
      expect(existsSync(parentReadyPath)).toBe(true);
      processInfo = JSON.parse(await readFile(processInfoPath, "utf8")) as WorkerProcess;
      const childPid = Number(await readFile(harness.config.pidPath, "utf8"));
      const descendantPid = Number(await readFile(harness.config.descendantPidPath, "utf8"));
      expect(childPid).toBe(processInfo.pid);
      expect(processInfo.pgid).not.toBe(parent.pid);
      expect(isProcessAlive(childPid)).toBe(true);
      expect(isProcessAlive(descendantPid)).toBe(true);

      parent.kill("SIGKILL");
      parentKilled = true;
      await parent.exited;
      await waitForProcessExit(childPid);
      await waitForProcessExit(descendantPid);
      await waitUntil(() => !groupAlive(processInfo!.pgid));
      expect(git(harness.root, "rev-parse", "HEAD")).toBe(harness.initialHead);
      expect(await readFile(harness.config.startedPath, "utf8")).toBe(`${childPid}\n`);
    } finally {
      if (!parentKilled) {
        if (isProcessAlive(parent.pid)) parent.kill("SIGKILL");
        await parent.exited;
      }
      if (processInfo && groupAlive(processInfo.pgid)) {
        await terminateProcessGroup(processInfo.pgid, 350);
      }
    }
  }));

test("重复 stop 调用都等待同一受管进程组退出", () =>
  withHarness("hang", async (harness) => {
    const { worker } = await harness.createWorker();
    const runPromise = harness.run(worker);
    await waitUntil(() => existsSync(harness.config.readyPath));

    const firstStop = worker.stop();
    const secondStop = worker.stop();
    expect(await Promise.all([firstStop, secondStop])).toEqual([true, true]);
    await runPromise;
  }));

test("commit 已写入 HEAD 后与 stop 竞争仍返回真实提交结果", () =>
  withHarness("commit-then-hang", async (harness) => {
    const { worker } = await harness.createWorker();
    const runPromise = harness.run(worker);
    await waitUntil(() => existsSync(harness.config.readyPath));
    const committedHead = git(harness.root, "rev-parse", "HEAD");
    expect(committedHead).not.toBe(harness.initialHead);

    expect(await worker.stop()).toBe(true);
    const result = await runPromise;
    expect(result.headBefore).toBe(harness.initialHead);
    expect(result.headAfter).toBe(committedHead);
    expect(git(harness.root, "rev-parse", "HEAD")).toBe(committedHead);
  }));

test("onSpawn 台账登记失败时终止已 spawn 的进程组且不执行 commit", () =>
  withHarness("commit", async (harness) => {
    let processInfo: WorkerProcess | undefined;
    const persistenceError = new Error("ledger write failed");
    await expect(
      harness.factory.createCommit({
        cwd: harness.root,
        onSpawn: (info) => {
          processInfo = info;
          throw persistenceError;
        },
      }),
    ).rejects.toBe(persistenceError);

    expect(processInfo?.pid).toBeGreaterThan(1);
    await waitForProcessExit(processInfo!.pid);
    await waitUntil(() => !groupAlive(processInfo!.pgid));
    expect(git(harness.root, "rev-parse", "HEAD")).toBe(harness.initialHead);
  }));

test("watchdog 启动失败时清理已验证身份的 commit gate 进程且不执行 commit", () =>
  withHarness("commit", async (harness) => {
    let watchdogAttempted = false;
    let spawnedGatePids: number[] = [];
    const factory = createWorkerFactory({
      ompCommand: process.execPath,
      extraArgs: [fixturePath, "--fixture-config", join(harness.root, "fixture-config.json"), ...RPC_EXTRA_ARGS],
      startupTimeoutMs: 500,
      cancelTimeoutMs: 350,
      spawnWatchdog: () => {
        watchdogAttempted = true;
        spawnedGatePids = processesContaining(join(harness.root, "fixture-config.json"));
        throw new Error("watchdog spawn rejected");
      },
    });
    const failure = await factory.createCommit({ cwd: harness.root, onSpawn: () => {} }).then(
      () => undefined,
      (error: unknown) => error,
    );

    expect(watchdogAttempted).toBe(true);
    expect(spawnedGatePids.length).toBeGreaterThan(0);
    expect(failure).toBeInstanceOf(TaskStartupError);
    expect((failure as TaskStartupError).code).toBe("watchdog-unavailable");
    for (const pid of spawnedGatePids) await waitForProcessExit(pid);
    await waitUntil(() => processesContaining(join(harness.root, "fixture-config.json")).length === 0);
    expect(git(harness.root, "rev-parse", "HEAD")).toBe(harness.initialHead);
  }));

test("tracked commit 在 run 前登记真实进程台账，close 后才释放资源", () =>
  withHarness("commit", async (harness) => {
    const databasePath = join(harness.root, ".git", "channel.sqlite");
    const store = await ChannelStore.open(databasePath);
    const botUid = "commit-fixture-bot";
    const sessionId = "bound-session";
    const ledger: Omit<WorkerLedgerRecord, keyof WorkerProcess | "state" | "moveTargetCwd"> = {
      id: "commit-worker-fixture",
      botUid,
      sessionId,
      sessionFile: join(harness.root, "sessions", "bound-session.jsonl"),
    };
    const blocked = new Set<string>();
    let closeLease: (() => Promise<boolean>) | undefined;
    try {
      const lease = await openTrackedCommit({
        workerFactory: harness.factory,
        store,
        worker: { cwd: harness.root },
        ledger,
        blockDirectory: (cwd) => blocked.add(cwd),
      });
      closeLease = () => lease.close();
      expect(store.listWorkerLedger(botUid)).toMatchObject([
        { id: ledger.id, cwd: harness.root, state: "running", moveTargetCwd: null },
      ]);
      expect(git(harness.root, "rev-parse", "HEAD")).toBe(harness.initialHead);

      const result = await lease.worker.run();
      expect(result.exitCode).toBe(0);
      expect(result.headBefore).toBe(harness.initialHead);
      expect(result.headAfter).not.toBe(harness.initialHead);
      expect(await lease.close()).toBe(true);
      expect(store.listWorkerLedger(botUid)).toEqual([]);
      expect(blocked).toEqual(new Set());
    } finally {
      if (closeLease) await closeLease().catch(() => false);
      store.close();
    }
  }));
