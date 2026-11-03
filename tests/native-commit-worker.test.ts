import { expect, test } from "bun:test";
import { existsSync } from "node:fs";
import { chmod, mkdir, mkdtemp, readFile, realpath, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
import { ChannelStore } from "../src/store/index.ts";
import type {
  CommitResult,
  CommitWorker,
  CommitWorkerOptions,
  OmpWorker,
  WorkerEvent,
  WorkerFactory,
  WorkerProcess,
} from "../src/contracts.ts";
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

type NativeCommitProgressEvent = Extract<WorkerEvent, { type: "commit_progress" }>;

type FixtureConfig = {
  scenario:
    | "commit"
    | "commit-exit-1"
    | "fail"
    | "hang"
    | "commit-then-hang"
    | "interruptible-hang"
    | "progress";
  pidPath: string;
  readyPath: string;
  descendantPidPath: string;
  argvPath: string;
  cwdPath: string;
  startedPath: string;
  outputBytes?: number;
  stdoutChunksBase64?: string[];
  stderrChunksBase64?: string[];
  chunkDelayMs?: number;
  trailingOutputBytes?: number;
  makeCommit?: boolean;
  commitMessage?: string;
  followupCommitMessage?: string;
  commitMessageFile?: string;
  commitPaths?: string[];
  commitOutput?: string;
  postStageAppend?: { path: string; content: string };
  corruptChangelogBlob?: boolean;
};

type HarnessOverrides = Omit<Partial<FixtureConfig>, "scenario"> & { unborn?: boolean };

type Harness = {
  root: string;
  initialHead: string;
  config: FixtureConfig;
  factory: WorkerFactory;
  createWorker(
    model?: string,
    onProgress?: (event: NativeCommitProgressEvent) => void,
  ): Promise<{ worker: CommitWorker; processInfo: WorkerProcess }>;
  createWorkerAt(
    cwd: string,
    model?: string,
    onProgress?: (event: NativeCommitProgressEvent) => void,
  ): Promise<{ worker: CommitWorker; processInfo: WorkerProcess }>;
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
  overrides: HarnessOverrides = {},
): Promise<T> {
  const root = await realpath(await mkdtemp(join(tmpdir(), "omp-native-commit-")));
  const { unborn = false, ...fixtureOverrides } = overrides;
  const config: FixtureConfig = {
    scenario,
    pidPath: join(root, "commit-child.pid"),
    readyPath: join(root, "commit-child.ready"),
    descendantPidPath: join(root, "commit-descendant.pid"),
    argvPath: join(root, "commit-argv.json"),
    cwdPath: join(root, "commit-cwd.txt"),
    startedPath: join(root, "commit-child.started"),
    ...(scenario === "fail" ? { outputBytes: 1_048_576 } : {}),
    ...fixtureOverrides,
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
    if (!unborn) {
      git(root, "add", "tracked.txt");
      git(root, "commit", "--quiet", "-m", "initial fixture commit");
      initialHead = git(root, "rev-parse", "HEAD");
    }
    await writeFile(join(root, "tracked.txt"), "change for native commit\n");

    const factory = createWorkerFactory({
      ompCommand: process.execPath,
      extraArgs: [fixturePath, "--fixture-config", configPath, ...RPC_EXTRA_ARGS],
      startupTimeoutMs: 2_000,
      cancelTimeoutMs: 350,
    });
    const createWorkerAt = async (
      cwd: string,
      model?: string,
      onProgress?: (event: NativeCommitProgressEvent) => void,
    ): Promise<{ worker: CommitWorker; processInfo: WorkerProcess }> => {
      let processInfo: WorkerProcess | undefined;
      const options: CommitWorkerOptions = {
        cwd,
        ...(model === undefined ? {} : { model }),
        onSpawn: (info) => {
          processInfo = info;
        },
        ...(onProgress === undefined ? {} : { onProgress }),
      };
      const worker = await factory.createCommit(options);
      if (!processInfo) throw new Error("commit worker did not report its managed process");
      workers.set(worker, processInfo);
      return { worker, processInfo };
    };
    const harness: Harness = {
      root,
      initialHead,
      config,
      factory,
      createWorker: (model, onProgress) => createWorkerAt(root, model, onProgress),
      createWorkerAt,
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

function encodeChunks(chunks: readonly (string | Uint8Array)[]): string[] {
  return chunks.map(chunk => Buffer.from(chunk).toString("base64"));
}

function progressSummaries(events: readonly NativeCommitProgressEvent[]): string[] {
  return events.map(event => event.summary);
}

function expectProgressEvents(events: readonly NativeCommitProgressEvent[]): void {
  let previousAtMs = 0;
  for (const event of events) {
    expect(event.type).toBe("commit_progress");
    expect(Number.isFinite(event.atMs)).toBe(true);
    expect(event.atMs).toBeGreaterThanOrEqual(previousAtMs);
    expect(event.summary.length).toBeGreaterThan(0);
    previousAtMs = event.atMs;
  }
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
    expect(result).toMatchObject({ exitCode: 0, headBefore: harness.initialHead, headAfter });
    expect(headAfter).not.toBe(harness.initialHead);
    expect(git(harness.root, "status", "--porcelain")).toBe("");
  }));

test("native CLI 成功退出但 HEAD 未变时不返回提交内容", () =>
  withHarness(
    "progress",
    async (harness) => {
      const { worker } = await harness.createWorker();
      const result = await harness.run(worker);

      expect(result.exitCode).toBe(0);
      expect(result.headBefore).toBe(harness.initialHead);
      expect(result.headAfter).toBe(harness.initialHead);
      expect(result.content).toBeUndefined();
    },
    { makeCommit: false },
  ));

test("提交内容来自真实 Git commit，并且 changelog 仅含提交的新条目", () =>
  withHarness(
    "commit",
    async (harness) => {
      const changelogPath = join(harness.root, "CHANGELOG.md");
      const sourcePath = join(harness.root, "src", "changed.ts");
      await mkdir(join(harness.root, "src"), { recursive: true });
      await writeFile(changelogPath, "# History\n\n## Previous\n- old committed entry\n");
      await writeFile(sourcePath, "export const state = \"before\";\n");
      git(harness.root, "add", "CHANGELOG.md", "src/changed.ts");
      git(harness.root, "commit", "--quiet", "-m", "seed changelog and source history");
      const headBefore = git(harness.root, "rev-parse", "HEAD");

      await writeFile(
        changelogPath,
        "# History\n\n## Previous\n- old committed entry\n\n## Current\n- committed changelog entry\n",
      );
      await writeFile(sourcePath, "export const state = \"source-only diff marker\";\n");

      const { worker } = await harness.createWorker();
      const result = await harness.run(worker);
      const message = result.content?.message ?? "";
      const changelog = result.content?.changelog ?? "";

      expect(result.exitCode).toBe(0);
      expect(result.headBefore).toBe(headBefore);
      expect(result.headAfter).not.toBe(headBefore);
      expect(message).toContain("真实提交标题\n\n真实提交正文");
      expect(message).not.toContain("stdout 伪造标题");
      expect(message).not.toContain("source-only diff marker");
      expect(message).not.toContain("diff --git");
      expect(changelog).toContain("committed changelog entry");
      expect(changelog).not.toContain("old committed entry");
      expect(changelog).not.toContain("source-only diff marker");
      expect(changelog).not.toContain("change for native commit");
      expect(changelog).not.toContain("uncommitted");
      expect(git(harness.root, "status", "--porcelain")).not.toBe("");
    },
    {
      commitMessage: "真实提交标题\n\n真实提交正文",
      commitOutput: "stdout 伪造标题\nstdout 伪造 changelog",
      commitPaths: ["CHANGELOG.md", "src/changed.ts"],
      postStageAppend: { path: "CHANGELOG.md", content: "- uncommitted changelog entry\n" },
    },
  ));

test("一次 native 命令聚合 HEAD 变化范围内所有真实 commit message", () =>
  withHarness(
    "commit",
    async (harness) => {
      const { worker } = await harness.createWorker();
      const result = await harness.run(worker);
      const message = result.content?.message ?? "";

      expect(result.headBefore).toBe(harness.initialHead);
      expect(result.headAfter).not.toBe(harness.initialHead);
      expect(message).toContain("first commit in this range");
      expect(message).toContain("second commit in this range");
      expect(message).not.toContain("initial fixture commit");
    },
    {
      commitMessage: "first commit in this range",
      followupCommitMessage: "second commit in this range",
    },
  ));

test("root worker 也能读取嵌套包 CHANGELOG.md 的已提交新增行", () =>
  withHarness(
    "commit",
    async (harness) => {
      const packagePath = join(harness.root, "packages", "nested");
      const rootChangelog = join(harness.root, "CHANGELOG.md");
      const nestedChangelog = join(packagePath, "CHANGELOG.md");
      await mkdir(packagePath, { recursive: true });
      await writeFile(rootChangelog, "# Root history\n- root historical marker\n");
      await writeFile(nestedChangelog, "# Package history\n- nested old marker\n");
      git(harness.root, "add", "CHANGELOG.md", "packages/nested/CHANGELOG.md");
      git(harness.root, "commit", "--quiet", "-m", "seed root and nested changelogs");
      const headBefore = git(harness.root, "rev-parse", "HEAD");
      await writeFile(nestedChangelog, "# Package history\n- nested old marker\n- nested committed marker\n");

      const { worker } = await harness.createWorker();
      const result = await harness.run(worker);
      const changelog = result.content?.changelog ?? "";

      expect(result.headBefore).toBe(headBefore);
      expect(result.headAfter).not.toBe(headBefore);
      expect(changelog).toContain("nested committed marker");
      expect(changelog).not.toContain("nested old marker");
      expect(changelog).not.toContain("root historical marker");
    },
    {
      commitMessage: "nested package update",
      commitPaths: ["packages/nested/CHANGELOG.md"],
    },
  ));

test("unborn repository returns first commit content with a null before HEAD", () =>
  withHarness(
    "commit",
    async (harness) => {
      await writeFile(join(harness.root, "CHANGELOG.md"), "# First release\n- first committed entry\n");
      const { worker } = await harness.createWorker();
      const result = await harness.run(worker);

      expect(result.exitCode).toBe(0);
      expect(result.headBefore).toBeNull();
      expect(result.headAfter).not.toBeNull();
      expect(result.content?.message).toContain("first repository commit");
      expect(result.content?.message).toContain("followup root commit");
      expect(result.content?.changelog).toContain("first committed entry");
    },
    {
      unborn: true,
      commitMessage: "first repository commit",
      followupCommitMessage: "followup root commit",
      commitPaths: ["CHANGELOG.md"],
    },
  ));

test("CLI 非零退出但已产生提交时仍返回该提交内容", () =>
  withHarness(
    "commit-exit-1",
    async (harness) => {
      const { worker } = await harness.createWorker();
      const result = await harness.run(worker);

      expect(result.exitCode).toBe(1);
      expect(result.headBefore).toBe(harness.initialHead);
      expect(result.headAfter).not.toBe(harness.initialHead);
      expect(result.content?.message).toContain("commit completed before nonzero exit");
      expect(result.content?.message).not.toContain("stdout fake message");
    },
    {
      commitMessage: "commit completed before nonzero exit",
      commitOutput: "stdout fake message",
    },
  ));

test("Git commit 输出超过 capture 上限时仍排空并返回截断内容", () =>
  withHarness(
    "commit",
    async (harness) => {
      await writeFile(
        join(harness.root, ".git", "native-large-commit-message.txt"),
        `large commit title\n\n${"x".repeat(128 * 1024)}`,
      );
      const { worker } = await harness.createWorker();
      const result = await harness.run(worker);
      const message = result.content?.message ?? "";

      expect(result.exitCode).toBe(0);
      expect(result.headAfter).not.toBe(harness.initialHead);
      expect(Array.from(message).length).toBeLessThanOrEqual(800);
      expect(message).toEndWith("…（已截断）");
    },
    {
      commitMessageFile: ".git/native-large-commit-message.txt",
    },
  ));

test("Git 返回内容按 Unicode code point 限长并过滤凭据与绝对路径", () =>
  withHarness(
    "commit",
    async (harness) => {
      const token = `ghp_${"a".repeat(36)}`;
      const privatePath = "/Users/fixture/private/secrets";
      const changelogPath = join(harness.root, "CHANGELOG.md");
      await writeFile(changelogPath, "# History\n- old entry\n");
      git(harness.root, "add", "CHANGELOG.md");
      git(harness.root, "commit", "--quiet", "-m", "seed changelog");
      await writeFile(
        changelogPath,
        [
          "# History",
          "- old entry",
          "- Authorization: Bearer very-secret-token",
          `- API_KEY=${token}`,
          `- private path ${privatePath}`,
          `- ${"更新🧭".repeat(1_300)}`,
          "",
        ].join("\n"),
      );
      const { worker } = await harness.createWorker();
      const result = await harness.run(worker);
      const message = result.content?.message ?? "";
      const changelog = result.content?.changelog ?? "";

      expect(Array.from(message).length).toBeLessThanOrEqual(800);
      expect(Array.from(changelog).length).toBeLessThanOrEqual(1_200);
      expect(message).toContain("\n\n");
      expect(message).toEndWith("…（已截断）");
      expect(changelog).toEndWith("…（已截断）");
      expect(message).not.toContain("very-secret-token");
      expect(message).not.toContain(token);
      expect(message).not.toContain(privatePath);
      expect(changelog).not.toContain("very-secret-token");
      expect(changelog).not.toContain(token);
      expect(changelog).not.toContain(privatePath);
    },
    {
      commitMessage: [
        "更新标题 🧭",
        "",
        "Authorization: Bearer very-secret-token",
        `API_KEY=ghp_${"a".repeat(36)}`,
        "private path /Users/fixture/private/secrets",
        "",
        "🧭".repeat(900),
      ].join("\n"),
      commitPaths: ["CHANGELOG.md"],
    },
  ));

test("Git commit 内容读取失败不会改变 native CLI exit code 或 HEAD 结果", () =>
  withHarness(
    "commit",
    async (harness) => {
      await writeFile(join(harness.root, "CHANGELOG.md"), "# Current\n- committed entry\n");
      const { worker } = await harness.createWorker();
      const result = await harness.run(worker);
      const headAfter = git(harness.root, "rev-parse", "HEAD");

      expect(result.exitCode).toBe(0);
      expect(result.headBefore).toBe(harness.initialHead);
      expect(result.headAfter).toBe(headAfter);
      expect(headAfter).not.toBe(harness.initialHead);
      expect(result.content?.changelog).toBeUndefined();
    },
    { corruptChangelogBlob: true },
  ));

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

test("run 释放 gate 后报告提交开始，run 前没有进度", () =>
  withHarness("hang", async (harness) => {
    const events: NativeCommitProgressEvent[] = [];
    const { worker } = await harness.createWorker(undefined, event => events.push(event));
    expect(events).toEqual([]);

    const runPromise = harness.run(worker);
    await waitUntil(() => existsSync(harness.config.readyPath));
    await waitUntil(() => events.length > 0, 2_000);
    expectProgressEvents(events);
    expect(progressSummaries(events)).toEqual(["正在提交"]);

    expect(await worker.stop()).toBe(true);
    await runPromise;
  }));

test("固定阶段按序映射，工具只认白名单名称与结束状态，动态输出不进入进度", () =>
  withHarness("progress", async (harness) => {
    const events: NativeCommitProgressEvent[] = [];
    const { worker } = await harness.createWorker(undefined, event => events.push(event));
    const result = await harness.run(worker);

    expect(result).toEqual({ exitCode: 0, headBefore: harness.initialHead, headAfter: harness.initialHead });
    expectProgressEvents(events);
    const summaries = progressSummaries(events);
    expect(summaries).toHaveLength(14);
    expect(summaries[0]).toBe("正在提交");
    expect(summaries[1]).toContain("解析模型");
    expect(summaries[2]).toContain("暂存");
    expect(summaries[3]).toContain("变更日志");
    expect(summaries[4]).toContain("上下文");
    expect(summaries[5]).toContain("启动提交代理");
    expect(summaries[6]).toContain("GitOverview");
    expect(summaries[6]).toContain("完成");
    expect(summaries[7]).toContain("GitHunk");
    expect(summaries[7]).toContain("失败");
    expect(summaries[8]).toContain("本轮已结束");
    expect(summaries[9]).toContain("回退");
    expect(summaries[10]).toContain("未提供方案");
    expect(summaries[11]).toContain("变更日志");
    expect(summaries[12]).toContain("创建提交");
    expect(summaries[13]).toContain("拆分提交");
    expect(summaries.join("\n")).not.toContain("SneakyTool");
  }, {
    makeCommit: false,
    chunkDelayMs: 0,
    stdoutChunksBase64: encodeChunks([
      "● Resolving model...\n",
      "No staged changes detected, staging all changes...\n",
      "● Detecting changelog targets...\n",
      "● Discovering context files...\n",
      "● Starting commit agent...\n",
      "\uF00C GitOverview\n",
      "\uF00D GitHunk\n",
      "\uF00C SneakyTool\n",
      "● agent finished (9 messages, 6 tools)\n",
      "● Using fallback commit generation...\n",
      "● Agent did not provide proposal, using fallback...\n",
      "● Applying changelog entries...\n",
      "● Creating commit...\n",
      "● Creating split commits...\n",
    ]),
  }));

test("UTF-8 终止标记、ANSI 与 CR 可跨 chunk，重复刷新不增长进度", () =>
  withHarness("progress", async (harness) => {
    const events: NativeCommitProgressEvent[] = [];
    const { worker } = await harness.createWorker(undefined, event => events.push(event));
    await harness.run(worker);

    expectProgressEvents(events);
    const summaries = progressSummaries(events);
    expect(summaries).toHaveLength(6);
    expect(summaries[0]).toBe("正在提交");
    expect(summaries[1]).toContain("解析模型");
    expect(summaries[2]).toContain("GitOverview");
    expect(summaries[3]).toContain("GitHunk");
    expect(summaries[4]).toContain("变更日志");
    expect(summaries[5]).toContain("创建提交");
  }, {
    makeCommit: false,
    chunkDelayMs: 10,
    stdoutChunksBase64: (() => {
      const bullet = Buffer.from("●");
      const success = Buffer.from("\uF00C GitOverview\n");
      const failure = Buffer.from("\uF00D GitHunk\n");
      return encodeChunks([
        Buffer.from("\u001b[2"),
        Buffer.from("K\r\u001b[3"),
        Buffer.from("2m"),
        bullet.subarray(0, 1),
        Buffer.concat([bullet.subarray(1), Buffer.from(" Resolving model...\u001b[0m\r\n")]),
        success.subarray(0, 1),
        success.subarray(1, 2),
        success.subarray(2),
        failure.subarray(0, 2),
        failure.subarray(2),
        Buffer.from("● Detecting changelog targets...\r"),
        Buffer.from("● Detecting changelog targets...\r\n"),
        Buffer.from("● Creating commit...\n"),
        Buffer.from("● Creating commit...\n"),
      ]);
    })(),
  }));

test("native agent 本轮结束后的非相邻重复工具步骤仍会报告", () =>
  withHarness("progress", async (harness) => {
    const events: NativeCommitProgressEvent[] = [];
    const { worker } = await harness.createWorker(undefined, event => events.push(event));
    await harness.run(worker);

    expectProgressEvents(events);
    const summaries = progressSummaries(events);
    expect(summaries).toHaveLength(4);
    expect(summaries[0]).toBe("正在提交");
    expect(summaries[1]).toContain("GitFileDiff");
    expect(summaries[1]).toContain("完成");
    expect(summaries[2]).toContain("本轮已结束");
    expect(summaries[3]).toContain("GitFileDiff");
    expect(summaries[3]).toContain("完成");
  }, {
    makeCommit: false,
    chunkDelayMs: 0,
    stdoutChunksBase64: encodeChunks([
      "\uF00C GitFileDiff\n",
      "● agent finished (2 messages, 1 tools)\n",
      "\uF00C GitFileDiff\n",
    ]),
  }));

test("超长行与敏感动态输出不进入进度，丢弃后仍能识别后续固定阶段", () =>
  withHarness("progress", async (harness) => {
    const events: NativeCommitProgressEvent[] = [];
    const { worker } = await harness.createWorker(undefined, event => events.push(event));
    const result = await harness.run(worker);

    expect(result).toEqual({ exitCode: 0, headBefore: harness.initialHead, headAfter: harness.initialHead });
    expectProgressEvents(events);
    const summaries = progressSummaries(events);
    expect(summaries[0]).toBe("正在提交");
    expect(summaries[summaries.length - 1]).toContain("拆分提交");
    const joined = summaries.join("\n");
    for (const sensitive of [
      "sk-live-SECRET",
      "/Users/dong/private",
      "token=",
      "Detected trivial",
      "diff --git",
      "Agent error",
    ]) {
      expect(joined).not.toContain(sensitive);
    }
    for (const summary of summaries) expect(summary.length).toBeLessThanOrEqual(64);
    expect(summaries.some(summary => summary.includes("创建提交"))).toBe(false);
  }, {
    makeCommit: false,
    chunkDelayMs: 5,
    stdoutChunksBase64: encodeChunks([
      "I will inspect /Users/dong/private/.env with token=sk-live-SECRET\n",
      "● Error: token=sk-live-SECRET\n",
      "● Detected trivial change: secret\n",
      "diff --git a/.env b/.env\n",
      Buffer.concat([
        Buffer.from("● Creating commit..."),
        Buffer.alloc(256 * 1024, 0x78),
        Buffer.from("\n"),
      ]),
      "● Creating split commits...\n",
    ]),
    stderrChunksBase64: encodeChunks([
      "Agent error: /Users/dong/private token=sk-live-SECRET\n",
      "{\"path\":\"/Users/dong/private/.env\"}\n",
    ]),
  }));

test("onProgress 回调抛错不影响管道排空、退出码与真实 HEAD 结果", () =>
  withHarness("progress", async (harness) => {
    let calls = 0;
    const { worker } = await harness.createWorker(undefined, () => {
      calls += 1;
      throw new Error("progress consumer failed");
    });
    const result = await harness.run(worker);
    const headAfter = git(harness.root, "rev-parse", "HEAD");

    expect(calls).toBeGreaterThan(0);
    expect(result).toMatchObject({ exitCode: 0, headBefore: harness.initialHead, headAfter });
    expect(headAfter).not.toBe(harness.initialHead);
    expect(git(harness.root, "status", "--porcelain")).toBe("");
  }, {
    makeCommit: true,
    chunkDelayMs: 5,
    stdoutChunksBase64: encodeChunks([
      "● Resolving model...\n",
      "● Starting commit agent...\n",
      "● Creating commit...\n",
    ]),
    stderrChunksBase64: encodeChunks([
      Buffer.alloc(512 * 1024, 0x79),
    ]),
    trailingOutputBytes: 1_048_576,
  }), 15_000);

test("读取 Git HEAD 的非 unborn 错误必须拒绝且不得释放 commit gate", () =>
  withHarness("commit", async (harness) => {
    const shimDir = await mkdtemp(join(tmpdir(), "native-commit-git-shim-"));
    const shimPath = join(shimDir, "git");
    await writeFile(
      shimPath,
      [
        "#!/bin/sh",
        'if [ "$1" = "--no-pager" ]; then shift; fi',
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

    expect(result).toMatchObject({ exitCode: 1, headBefore: harness.initialHead, headAfter });
    expect(headAfter).not.toBe(harness.initialHead);
    expect(git(harness.root, "status", "--porcelain")).toBe("");
  }));

test("native CLI 非零退出时保留实际 HEAD，且 stdout/stderr 大量输出都被排空", () =>
  withHarness("fail", async (harness) => {
    const { worker } = await harness.createWorker();
    const result = await harness.run(worker);

    expect(result).toEqual({ exitCode: 7, headBefore: harness.initialHead, headAfter: harness.initialHead });
    expect(result.content).toBeUndefined();
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
