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

const NATIVE_COMMIT_VALUE_FLAGS = new Set(["--extension", "-e", "--hook"]);

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

async function drain(stream: ReadableStream<Uint8Array>): Promise<void> {
  const reader = stream.getReader();
  try {
    for (;;) {
      const { done } = await reader.read();
      if (done) return;
    }
  } finally {
    reader.releaseLock();
  }
}

async function gitHead(cwd: string): Promise<string | null> {
  const child = Bun.spawn(["git", "rev-parse", "--verify", "--quiet", "HEAD"], {
    cwd,
    stdin: "ignore",
    stdout: "pipe",
    stderr: "pipe",
    env: workerEnv(),
  });
  const [stdout, stderr, exitCode] = await Promise.all([
    new Response(child.stdout).text(),
    new Response(child.stderr).text(),
    child.exited,
  ]);
  if (exitCode === 1) return null;
  if (exitCode !== 0) {
    throw new Error(`git rev-parse --verify --quiet HEAD failed (${exitCode}): ${stderr.trim()}`);
  }
  const head = stdout.trim();
  if (head.length === 0) throw new Error("git rev-parse --verify --quiet HEAD returned no HEAD");
  return head;
}

class NativeCommitWorker implements CommitWorker {
  readonly #child: RpcChild;
  readonly #watchdog: RpcChild | undefined;
  readonly #pgid: number;
  readonly #cwd: string;
  readonly #cancelTimeoutMs: number;
  readonly #drains: readonly [Promise<void>, Promise<void>];
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
      headBefore = await gitHead(this.#cwd);
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
    const headAfter = await gitHead(this.#cwd);
    return { exitCode, headBefore, headAfter };
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

  const drains: readonly [Promise<void>, Promise<void>] = [drain(child.stdout), drain(child.stderr)];
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

  return new NativeCommitWorker(child, watchdog, snapshot.pgid, cwd, options.cancelTimeoutMs, drains);
}
