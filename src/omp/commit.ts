import { existsSync, realpathSync } from "node:fs";
import { isAbsolute } from "node:path";
import type { CommitResult, CommitWorker, CommitWorkerOptions, WorkerProcess } from "../contracts.ts";
import { TaskStartupError, taskStartupError } from "../contracts.ts";
import {
  type RpcChild,
  createWatchdog,
  queryProcess,
  stopWatchdog,
  terminateOwnedProcess,
} from "./process.ts";
import { stripEnforcedArgs, workerEnv } from "./profile.ts";
import { readGitCommitContent, readGitHead } from "./git-commit-content.ts";
import { type CommitOutputParser, createCommitOutputParser } from "./commit-output.ts";

const NATIVE_COMMIT_VALUE_FLAGS = new Set(["--extension", "-e", "--hook"]);
const COMMIT_START_SUMMARY = "正在提交";

function nativeCommitArgs(extraArgs: readonly string[], model: string | undefined): string[] {
  const output = stripEnforcedArgs(extraArgs);
  const filtered: string[] = [];
  for (let index = 0; index < output.length; index += 1) {
    const arg = output[index]!;
    const equalsIndex = arg.indexOf("=");
    const flagName = equalsIndex >= 0 ? arg.slice(0, equalsIndex) : arg;
    if (NATIVE_COMMIT_VALUE_FLAGS.has(flagName)) {
      if (equalsIndex < 0) index += 1;
      continue;
    }
    filtered.push(arg);
  }
  filtered.push("commit");
  if (model) filtered.push("--model", model);
  return filtered;
}

async function drain(
  stream: ReadableStream<Uint8Array>,
  parser?: CommitOutputParser,
): Promise<void> {
  const reader = stream.getReader();
  try {
    for (;;) {
      const { done, value } = await reader.read();
      if (done) return;
      parser?.write(value);
    }
  } finally {
    parser?.end();
    reader.releaseLock();
  }
}

/**
 * Consecutive spinner refreshes are deduplicated while later real transitions remain visible.
 * A throwing consumer is ignored: card edits and transport I/O must never stall or fail the CLI.
 */
function createCommitProgressReporter(
  onProgress: CommitWorkerOptions["onProgress"],
): (summary: string) => void {
  if (onProgress === undefined) return () => {};
  let lastSummary: string | undefined;
  return (summary: string): void => {
    if (lastSummary === summary) return;
    lastSummary = summary;
    try {
      onProgress({ type: "commit_progress", atMs: Date.now(), summary });
    } catch {
      // Progress consumers are observability only; the native commit pipeline keeps draining.
    }
  };
}

class NativeCommitWorker implements CommitWorker {
  readonly #child: RpcChild;
  readonly #watchdog: RpcChild | undefined;
  readonly #pgid: number;
  readonly #cwd: string;
  readonly #cancelTimeoutMs: number;
  readonly #drains: readonly [Promise<void>, Promise<void>];
  readonly #reportProgress: (summary: string) => void;
  #runPromise: Promise<CommitResult> | undefined;
  #stopPromise: Promise<boolean> | undefined;
  #stopRequested = false;
  readonly #stopOutcome: Promise<boolean>;
  #resolveStopOutcome: (stopped: boolean) => void;

  constructor(
    child: RpcChild,
    watchdog: RpcChild | undefined,
    pgid: number,
    cwd: string,
    cancelTimeoutMs: number,
    drains: readonly [Promise<void>, Promise<void>],
    reportProgress: (summary: string) => void,
  ) {
    let resolveStopOutcome!: (stopped: boolean) => void;
    this.#stopOutcome = new Promise(resolve => {
      resolveStopOutcome = resolve;
    });
    this.#resolveStopOutcome = resolveStopOutcome;
    this.#child = child;
    this.#watchdog = watchdog;
    this.#pgid = pgid;
    this.#cwd = cwd;
    this.#cancelTimeoutMs = cancelTimeoutMs;
    this.#drains = drains;
    this.#reportProgress = reportProgress;
  }

  run(): Promise<CommitResult> {
    if (!this.#runPromise) this.#runPromise = this.#runInternal();
    return this.#runPromise;
  }

  stop(): Promise<boolean> {
    if (!this.#stopPromise) {
      this.#stopRequested = true;
      this.#stopPromise = this.#stopInternal();
      void this.#stopPromise.then(
        stopped => this.#resolveStopOutcome(stopped),
        () => this.#resolveStopOutcome(false),
      );
    }
    return this.#stopPromise;
  }

  async #runInternal(): Promise<CommitResult> {

    let headBefore: string | null;
    try {
      headBefore = await readGitHead(this.#cwd);
    } catch (error) {
      try {
        if (!(await this.stop())) throw new Error("native commit process group did not stop");
      } catch (stopError) {
        throw new AggregateError([error, stopError], "Could not read Git HEAD before native commit");
      }
      throw error;
    }
    if (!this.#stopRequested) {
      try {
        this.#child.stdin.write("commit\n");
        this.#child.stdin.end?.();
        this.#reportProgress(COMMIT_START_SUMMARY);
      } catch {
        void this.stop();
      }
    } else {
      this.#child.stdin.end?.();
    }

    const exitCode = await this.#waitForExitOrStop();
    if (!(await this.stop())) {
      throw new Error("native commit outcome is unknown: process group stop was not confirmed");
    }
    await Promise.all(this.#drains);
    const headAfter = await readGitHead(this.#cwd);
    const content = headAfter !== null && headAfter !== headBefore
      ? await readGitCommitContent(this.#cwd, headBefore, headAfter)
      : undefined;
    return {
      exitCode,
      headBefore,
      headAfter,
      ...(content === undefined ? {} : { content }),
    };
  }

  async #waitForExitOrStop(): Promise<number> {
    const exit = this.#child.exited.then(
      exitCode => ({ kind: "exit" as const, exitCode }),
      () => ({ kind: "exit" as const, exitCode: -1 }),
    );
    const stopped = this.#stopOutcome.then(value => ({ kind: "stop" as const, value }));
    const outcome = await Promise.race([exit, stopped]);
    if (outcome.kind === "stop" && !outcome.value) {
      throw new Error("native commit outcome is unknown: process group stop was not confirmed");
    }
    return outcome.kind === "exit" ? outcome.exitCode : this.#child.exited.catch(() => -1);
  }

  async #stopInternal(): Promise<boolean> {
    const stopped = await terminateOwnedProcess(this.#child, this.#pgid, this.#cancelTimeoutMs);
    if (stopped) {
      stopWatchdog(this.#watchdog);
      await Promise.all(this.#drains);
    }
    return stopped;
  }
}

export async function createNativeCommitWorker(options: {
  ompCommand: string;
  extraArgs: readonly string[];
  startupTimeoutMs: number;
  cancelTimeoutMs: number;
  cliEnv?: Record<string, string>;
  spawnWatchdog?: () => RpcChild;
  commit: CommitWorkerOptions;
}): Promise<CommitWorker> {
  const { commit } = options;
  if (!commit || !isAbsolute(commit.cwd)) throw new Error("OMP commit cwd must be absolute");
  if (!existsSync(commit.cwd)) throw new TaskStartupError("cwd-missing", `OMP commit cwd does not exist: ${commit.cwd}`);
  const cwd = realpathSync(commit.cwd);
  const command = [
    "/bin/sh",
    "-c",
    'IFS= read -r commit_gate || exit 125; [ "$commit_gate" = commit ] || exit 125; exec "$@"',
    "omp-channel-octo-commit-gate",
    options.ompCommand,
    ...nativeCommitArgs(options.extraArgs, commit.model),
  ];
  let child: RpcChild;
  try {
    child = Bun.spawn(command, {
      cwd,
      stdin: "pipe",
      stdout: "pipe",
      stderr: "pipe",
      env: workerEnv(undefined, options.cliEnv),
      detached: process.platform !== "win32",
    }) as unknown as RpcChild;
  } catch (error) {
    throw taskStartupError("spawn-failed", error);
  }

  const reportProgress = createCommitProgressReporter(commit.onProgress);
  const stdoutParser = commit.onProgress === undefined ? undefined : createCommitOutputParser(reportProgress);
  const stderrParser = commit.onProgress === undefined ? undefined : createCommitOutputParser(reportProgress);
  const drains: readonly [Promise<void>, Promise<void>] = [
    drain(child.stdout, stdoutParser),
    drain(child.stderr, stderrParser),
  ];
  const parentSnapshot = await queryProcess(process.pid);
  const snapshot = await queryProcess(child.pid);
  if (!snapshot) {
    const stopped = await terminateOwnedProcess(child, child.pid, options.cancelTimeoutMs);
    if (stopped) await Promise.all(drains);
    throw new TaskStartupError("identity-unverified", "Could not verify the native commit process identity");
  }

  const watchdogStart = await createWatchdog(
    child.pid,
    snapshot.pgid,
    parentSnapshot?.identity ?? "",
    options.startupTimeoutMs,
    options.spawnWatchdog,
  );
  if (process.platform !== "win32" && !watchdogStart.ok) {
    const stopped = await terminateOwnedProcess(child, snapshot.pgid, options.cancelTimeoutMs);
    if (stopped) await Promise.all(drains);
    throw new TaskStartupError(
      "watchdog-unavailable",
      watchdogStart.detail.length > 0
        ? `Could not start the OMP parent-death watchdog: ${watchdogStart.detail}`
        : "Could not start the OMP parent-death watchdog",
    );
  }

  const watchdog = watchdogStart.ok ? watchdogStart.watchdog : undefined;
  const processInfo: WorkerProcess = {
    pid: child.pid,
    pgid: snapshot.pgid,
    identity: snapshot.identity,
    cwd,
  };
  try {
    commit.onSpawn(processInfo);
  } catch (error) {
    const stopped = await terminateOwnedProcess(child, snapshot.pgid, options.cancelTimeoutMs);
    if (stopped) {
      stopWatchdog(watchdog);
      await Promise.all(drains);
    }
    throw error;
  }

  return new NativeCommitWorker(child, watchdog, snapshot.pgid, cwd, options.cancelTimeoutMs, drains, reportProgress);
}
