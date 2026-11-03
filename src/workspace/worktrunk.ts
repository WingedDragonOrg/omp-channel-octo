import { randomUUID } from "node:crypto";
import { homedir } from "node:os";
import { join } from "node:path";
import type {
  WorktreeMergeLifecycle,
  WorktreeMergeProcessIdentity,
  WorktreeMergeRequest,
  WorktreeMergeSummary,
  WorktreeRemovalOutcome,
  WorktrunkProbe,
} from "../contracts.ts";
import { safeDisplayText } from "../cards/execution-text.ts";
import type { RpcChild } from "../omp/process.ts";
import {
  createWatchdog,
  queryProcess,
  stopWatchdog,
  terminateProcessGroup,
  waitForExit,
  waitForGroupGone,
} from "../omp/process.ts";

/**
 * Worktrunk (wt) integration for isolated thread workspaces: availability probing, the
 * `wt switch --create` / `wt merge` / `wt remove` invocations, and the git fallback for removal.
 * The host owns these invocations; wt executes in a child process. The sub-thread agent triggers
 * them through host tools and never runs wt itself. Project hooks are governed by Worktrunk's own
 * approvals: the host never passes `-y` or `--no-hooks`, and an unapproved command fails the
 * invocation with a single bounded line instead of waiting for an interactive prompt.
 */

const VERSION_PATTERN = /v?(\d+\.\d+\.\d+)/u;
export const WORKTRUNK_TIMEOUT_DEFAULTS = {
  probeMs: 5_000,
  /** `bun install` runs as the blocking pre-start hook, so creation keeps a wide ceiling. */
  createMs: 120_000,
  /** The pre-merge hook runs `bun run typecheck` and the full test suite. */
  mergeMs: 600_000,
  removeMs: 120_000,
} as const;
const MAX_MERGE_OUTPUT_CHARS = 4_096;
const MAX_COMMAND_ERROR_CHARS = 200;
const COMMAND_GROUP_STOP_MS = 1_000;
const GUARDED_COMMAND_NOT_STARTED_PREFIX = "__omp_worktrunk_not_started__:";
const GUARDED_COMMAND_SCRIPT = String.raw`IFS= read -r __omp_start || exit 125
[ "$__omp_start" = "run" ] || exit 125
__omp_not_started="$1"
shift
command -v "$1" >/dev/null 2>&1 || { printf '%s\n' "$__omp_not_started" >&2; exit 127; }
exec "$@"
exec_status=$?
printf '%s\n' "$__omp_not_started" >&2
exit "$exec_status"`;

export class WorktreeMergeExecutionError extends Error {
  readonly exitCode: number;
  readonly diagnostic: string;

  constructor(exitCode: number, rawDiagnostic: string) {
    const prefix = `合并失败（退出码 ${exitCode}）：`;
    const diagnostic =
      safeDisplayText(rawDiagnostic, MAX_COMMAND_ERROR_CHARS - prefix.length - 1) ??
      "wt 未返回错误详情";
    super(`${prefix}${diagnostic}`);
    this.name = "WorktreeMergeExecutionError";
    this.exitCode = exitCode;
    this.diagnostic = diagnostic;
  }
}

export interface CommandResult {
  exitCode: number;
  stdout: string;
  stderr: string;
  timedOut: boolean;
  aborted: boolean;
  executionStarted: boolean;
}

export interface RunCommandOptions {
  timeoutMs: number;
  signal?: AbortSignal;
  cwd?: string;
  /** Worktree merge alone uses a parked process group and parent-death watchdog before wt starts. */
  parentDeathWatchdog?: boolean;
  /** Capture at most this much text per output stream while still draining it to EOF. */
  maxOutputChars?: number;
  lifecycle?: WorktreeMergeLifecycle;
  /** A successful wt cleanup must finish its real filesystem work before the process group settles. */
  waitForBackgroundCleanup?: {
    shouldWait(result: CommandResult): boolean;
    waitForResources(context: WorktrunkCleanupWaitContext): Promise<void>;
  };
}

type ProcessCompletion = {
  stdout: string;
  stderr: string;
  exitCode: number;
};

async function readToEnd(
  reader: ReadableStreamDefaultReader<Uint8Array>,
  maxChars = Number.POSITIVE_INFINITY,
): Promise<string> {
  const decoder = new TextDecoder();
  const chunks: string[] = [];
  let captured = 0;
  for (;;) {
    const { done, value } = await reader.read();
    if (done) {
      if (captured < maxChars) chunks.push(decoder.decode().slice(0, maxChars - captured));
      return chunks.join("");
    }
    if (!value || captured >= maxChars) continue;
    const visible = decoder.decode(value, { stream: true }).slice(0, maxChars - captured);
    chunks.push(visible);
    captured += visible.length;
  }
}

async function within<T>(
  promise: Promise<T>,
  timeoutMs: number,
): Promise<{ settled: true; value: T } | { settled: false }> {
  let timer: ReturnType<typeof setTimeout>;
  const timeout = new Promise<{ settled: false }>((resolve) => {
    timer = setTimeout(() => resolve({ settled: false }), Math.max(1, timeoutMs));
  });
  const result = await Promise.race([
    promise.then((value) => ({ settled: true as const, value })),
    timeout,
  ]);
  clearTimeout(timer!);
  return result;
}

async function terminateCommandGroup(pgid: number): Promise<boolean> {
  return terminateProcessGroup(pgid, COMMAND_GROUP_STOP_MS);
}

async function stopWatchdogProcess(watchdog: RpcChild): Promise<void> {
  stopWatchdog(watchdog);
  if (await waitForExit(watchdog, COMMAND_GROUP_STOP_MS)) return;
  try {
    watchdog.kill("SIGKILL");
  } catch {
    // Already stopped.
  }
  await waitForExit(watchdog, COMMAND_GROUP_STOP_MS);
}

function processIdentity(
  pid: number,
  pgid: number,
  identity: string,
  cwd: string,
): WorktreeMergeProcessIdentity {
  return { pid, pgid, identity, cwd };
}

function signalIsAborted(signal: AbortSignal | undefined): boolean {
  return signal?.aborted === true;
}

/**
 * Runs one command. A missing executable reports `null`; merge uses a parked shell whose process
 * identity and parent-death watchdog are established before it is allowed to exec wt.
 */
export async function runCommand(
  argv: string[],
  options: RunCommandOptions,
): Promise<CommandResult | null> {
  const guarded = options.parentDeathWatchdog === true;
  if (guarded && process.platform === "win32") {
    throw new Error("无法为 Worktrunk merge 建立受监护的进程组");
  }
  if (guarded && signalIsAborted(options.signal)) {
    return {
      exitCode: -1,
      stdout: "",
      stderr: "",
      timedOut: false,
      aborted: true,
      executionStarted: false,
    };
  }
  if (guarded) options.lifecycle?.beforeSpawn(options.cwd ?? "");

  const detached = guarded && process.platform !== "win32";
  const notStartedMarker = guarded
    ? `${GUARDED_COMMAND_NOT_STARTED_PREFIX}${randomUUID()}`
    : "";
  const spawnArgv = guarded
    ? ["/bin/sh", "-c", GUARDED_COMMAND_SCRIPT, "omp-worktrunk-guard", notStartedMarker, ...argv]
    : argv;
  let child: Bun.ReadableSubprocess;
  try {
    child = Bun.spawn(spawnArgv, {
      stdin: guarded ? "pipe" : "ignore",
      stdout: "pipe",
      stderr: "pipe",
      detached,
      ...(options.cwd === undefined ? {} : { cwd: options.cwd }),
    });
  } catch {
    if (guarded) options.lifecycle?.onNotSpawned();
    return null;
  }

  const stdoutReader = child.stdout.getReader();
  const stderrReader = child.stderr.getReader();
  const stdoutPromise = readToEnd(stdoutReader, options.maxOutputChars);
  const stderrPromise = readToEnd(stderrReader, options.maxOutputChars);
  const completion: Promise<ProcessCompletion> = Promise.all([
    stdoutPromise,
    stderrPromise,
    child.exited,
  ]).then(([stdout, stderr, exitCode]) => ({ stdout, stderr, exitCode }));
  void completion.catch(() => {});

  let watchdog: RpcChild | undefined;
  let processGroupConfirmed = false;
  let lifecycleResult: "none" | "not-spawned" | "confirmed" | "unknown" = "none";
  let executionStarted = !guarded;
  const reportNotSpawned = () => {
    if (lifecycleResult !== "none") return;
    lifecycleResult = "not-spawned";
    options.lifecycle?.onNotSpawned();
  };
  const reportConfirmedExit = () => {
    if (lifecycleResult !== "none") return;
    lifecycleResult = "confirmed";
    options.lifecycle?.onConfirmedExit();
  };
  const reportUnknown = () => {
    if (lifecycleResult !== "none") return;
    lifecycleResult = "unknown";
    options.lifecycle?.onUnknown();
  };
  const stopUnverifiedChild = async (): Promise<boolean> => {
    try {
      (child as unknown as Bun.Subprocess<"pipe", "pipe", "pipe">).stdin.end();
    } catch {
      // Closing the parked shell's handshake pipe is the safest stop without a verified group id.
    }
    if (await waitForExit(child as unknown as RpcChild, COMMAND_GROUP_STOP_MS)) return true;
    try {
      child.kill("SIGKILL");
    } catch {
      // SIGKILL is sent at most once; unresolved state remains durably blocked.
    }
    return waitForExit(child as unknown as RpcChild, COMMAND_GROUP_STOP_MS);
  };
  const stopBeforeStart = async (groupVerified: boolean): Promise<boolean> => {
    const stopped = groupVerified
      ? await terminateCommandGroup(child.pid)
      : await stopUnverifiedChild();
    if (stopped) {
      processGroupConfirmed = true;
      const exited = await waitForExit(child as unknown as RpcChild, COMMAND_GROUP_STOP_MS);
      const drained = exited ? await within(completion, COMMAND_GROUP_STOP_MS) : { settled: false as const };
      try {
        if (exited && drained.settled) {
          reportNotSpawned();
        } else {
          await Promise.all([stdoutReader.cancel().catch(() => {}), stderrReader.cancel().catch(() => {})]);
          reportUnknown();
        }
      } finally {
        if (watchdog) await stopWatchdogProcess(watchdog);
      }
    } else {
      await Promise.all([stdoutReader.cancel().catch(() => {}), stderrReader.cancel().catch(() => {})]);
      reportUnknown();
    }
    return processGroupConfirmed;
  };

  if (guarded) {
    const [snapshot, parent] = await Promise.all([
      queryProcess(child.pid),
      queryProcess(process.pid),
    ]);
    if (!snapshot || snapshot.pgid !== child.pid || !parent) {
      await stopBeforeStart(snapshot?.pgid === child.pid);
      throw new Error("Worktrunk merge 进程身份无法确认");
    }
    const watchdogStart = await createWatchdog(
      child.pid,
      snapshot.pgid,
      parent.identity,
      WORKTRUNK_TIMEOUT_DEFAULTS.probeMs,
      undefined,
      COMMAND_GROUP_STOP_MS,
      false, // wt may leave legitimate cleanup children after its leader exits.
    );
    if (!watchdogStart.ok) {
      await stopBeforeStart(true);
      throw new Error("Worktrunk merge 进程监护不可用");
    }
    watchdog = watchdogStart.watchdog;
    try {
      options.lifecycle?.onSpawn(processIdentity(child.pid, snapshot.pgid, snapshot.identity, options.cwd ?? ""));
    } catch (error) {
      await stopBeforeStart(true);
      throw error;
    }
    if (signalIsAborted(options.signal)) {
      await stopBeforeStart(true);
      throw new Error("Worktrunk merge 已取消");
    }
    try {
      (child as unknown as Bun.Subprocess<"pipe", "pipe", "pipe">).stdin.write("run\n");
      (child as unknown as Bun.Subprocess<"pipe", "pipe", "pipe">).stdin.end();
      executionStarted = true;
    } catch {
      const stopped = await terminateCommandGroup(child.pid).catch(() => false);
      if (stopped) {
        processGroupConfirmed = true;
        const drained = await within(completion, COMMAND_GROUP_STOP_MS);
        if (!drained.settled) {
          await Promise.all([stdoutReader.cancel().catch(() => {}), stderrReader.cancel().catch(() => {})]);
        }
        if (watchdog) await stopWatchdogProcess(watchdog);
      } else {
        await Promise.all([stdoutReader.cancel().catch(() => {}), stderrReader.cancel().catch(() => {})]);
      }
      reportUnknown();
      throw new Error("Worktrunk merge 启动状态无法确认");
    }
  }

  let timedOut = false;
  let stopPromise: Promise<boolean> | undefined;
  let resolveStop!: (stopped: boolean) => void;
  const stopObservation = new Promise<boolean>((resolve) => {
    resolveStop = resolve;
  });
  const terminate = (): Promise<boolean> => {
    if (!stopPromise) {
      if (!detached) {
        try {
          child.kill("SIGTERM");
        } catch {
          // The child may have exited before the signal.
        }
        stopPromise = child.exited.then(() => true, () => false);
      } else {
        stopPromise = terminateCommandGroup(child.pid);
      }
      void stopPromise.then(resolveStop, () => resolveStop(false));
    }
    return stopPromise;
  };
  const deadline = Date.now() + options.timeoutMs;
  const timer = setTimeout(() => {
    timedOut = true;
    void terminate();
  }, options.timeoutMs);
  const abort = () => {
    void terminate();
  };
  options.signal?.addEventListener("abort", abort, { once: true });

  let result: ProcessCompletion;
  try {
    if (signalIsAborted(options.signal)) abort();
    const outcome = guarded
      ? await Promise.race([
          completion.then((value) => ({ kind: "completed" as const, value })),
          stopObservation.then((stopped) => ({ kind: "stopped" as const, stopped })),
        ])
      : { kind: "completed" as const, value: await completion };
    if (outcome.kind === "stopped" && !outcome.stopped) {
      try {
        reportUnknown();
      } finally {
        await Promise.all([stdoutReader.cancel().catch(() => {}), stderrReader.cancel().catch(() => {})]);
      }
      throw new Error("Worktrunk merge 进程组退出状态无法确认");
    }
    if (outcome.kind === "stopped") {
      const drained = await within(completion, COMMAND_GROUP_STOP_MS);
      if (!drained.settled) {
        try {
          reportUnknown();
        } finally {
          await Promise.all([stdoutReader.cancel().catch(() => {}), stderrReader.cancel().catch(() => {})]);
        }
        throw new Error("Worktrunk merge 输出管道未排空，进程退出状态无法确认");
      }
      result = drained.value;
    } else {
      result = outcome.value;
    }

    const commandResult: CommandResult = {
      ...result,
      timedOut,
      aborted: signalIsAborted(options.signal),
      executionStarted,
    };
    const backgroundCleanup = guarded ? options.waitForBackgroundCleanup : undefined;
    if (backgroundCleanup?.shouldWait(commandResult)) {
      const interrupted = () => timedOut || signalIsAborted(options.signal);
      const interruptionError = () =>
        timedOut
          ? new Error(`合并超时（${options.timeoutMs} ms）：${options.cwd ?? ""}`)
          : new Error(`合并已取消：${options.cwd ?? ""}`);
      const failCleanup = async (error: unknown): Promise<never> => {
        if (await terminate().catch(() => false)) processGroupConfirmed = true;
        try {
          reportUnknown();
        } finally {
          if (processGroupConfirmed && watchdog) await stopWatchdogProcess(watchdog);
        }
        throw error;
      };
      if (interrupted()) return await failCleanup(interruptionError());
      const resourceWait = await Promise.race([
        backgroundCleanup
          .waitForResources({
            remainingMs: () => Math.max(0, deadline - Date.now()),
            interrupted,
          })
          .then(
            () => ({ kind: "ready" as const }),
            (error: unknown) => ({ kind: "error" as const, error }),
          ),
        stopObservation.then(() => ({ kind: "interrupted" as const })),
      ]);
      if (resourceWait.kind === "interrupted" || interrupted()) {
        return await failCleanup(interruptionError());
      }
      if (resourceWait.kind === "error") {
        if (Date.now() >= deadline && !signalIsAborted(options.signal)) timedOut = true;
        return await failCleanup(interrupted() ? interruptionError() : resourceWait.error);
      }

      const groupWait = await Promise.race([
        waitForGroupGone(child.pid, Math.max(0, deadline - Date.now())).then((gone) => ({
          kind: "group" as const,
          gone,
        })),
        stopObservation.then(() => ({ kind: "interrupted" as const })),
      ]);
      if (groupWait.kind === "interrupted" || interrupted()) {
        return await failCleanup(interruptionError());
      }
      if (!groupWait.gone) {
        timedOut = true;
        return await failCleanup(interruptionError());
      }
      processGroupConfirmed = true;
      reportConfirmedExit();
      return {
        ...commandResult,
        timedOut,
        aborted: signalIsAborted(options.signal),
      };
    }
    let groupGone = !detached || await waitForGroupGone(child.pid, 0);
    if (detached && !groupGone) groupGone = await terminateCommandGroup(child.pid);
    if (!groupGone) {
      reportUnknown();
      throw new Error("Worktrunk merge 进程组退出状态无法确认");
    }
    processGroupConfirmed = true;
    const executionDidNotStart = guarded && result.stderr.includes(notStartedMarker);
    if (executionDidNotStart) {
      result.stderr = result.stderr.replace(notStartedMarker, "").trim();
      executionStarted = false;
      reportNotSpawned();
    } else {
      reportConfirmedExit();
    }
    return {
      ...result,
      timedOut,
      aborted: signalIsAborted(options.signal),
      executionStarted,
    };
  } catch (error) {
    if (!processGroupConfirmed && lifecycleResult === "none") {
      if (detached) {
        const stopped = await terminateCommandGroup(child.pid);
        if (stopped) {
          processGroupConfirmed = true;
          reportConfirmedExit();
        } else {
          reportUnknown();
        }
      } else {
        try {
          child.kill("SIGTERM");
        } catch {
          // The child may have exited before the signal.
        }
        await child.exited.catch(() => -1);
        processGroupConfirmed = true;
      }
    }
    throw error;
  } finally {
    clearTimeout(timer);
    options.signal?.removeEventListener("abort", abort);
    if (processGroupConfirmed && watchdog) await stopWatchdogProcess(watchdog);
  }
}

/** Single-line, bounded failure detail from a command's captured output. */
export function commandFailure(result: CommandResult): string {
  const detail = `${result.stderr}\n${result.stdout}`.replace(/\s+/gu, " ").trim();
  const visible = detail.length > 0 ? detail : "命令未返回错误详情";
  return visible.slice(0, MAX_COMMAND_ERROR_CHARS);
}

function safeParse(text: string): unknown {
  try {
    return JSON.parse(text.trim());
  } catch {
    return null;
  }
}

/** Candidate commands, first working one wins: PATH name first, then the common installs. */
export function defaultWorktrunkCandidates(): string[] {
  const home = homedir();
  return [
    "wt",
    "/opt/homebrew/bin/wt",
    "/usr/local/bin/wt",
    join(home, ".cargo/bin/wt"),
    join(home, ".local/bin/wt"),
    join(home, ".bun/bin/wt"),
  ];
}

export interface ProbeWorktrunkOptions {
  candidates: readonly string[];
  timeoutMs?: number;
  signal?: AbortSignal;
}

/**
 * Probes candidates in order and uses the first executable one. Any `--version` output is optional
 * display metadata only; neither its format nor exit status gates availability. The probe runs per
 * operation (never cached), so installing or upgrading wt takes effect without a channel restart.
 */
export async function probeWorktrunk(options: ProbeWorktrunkOptions): Promise<WorktrunkProbe> {
  for (const command of options.candidates) {
    const result = await runCommand([command, "--version"], {
      timeoutMs: options.timeoutMs ?? WORKTRUNK_TIMEOUT_DEFAULTS.probeMs,
      ...(options.signal === undefined ? {} : { signal: options.signal }),
    });
    if (!result) continue;
    if (result.aborted) throw new Error("Worktrunk 探测已取消");
    const matched = VERSION_PATTERN.exec(result.stdout) ?? VERSION_PATTERN.exec(result.stderr);
    return {
      available: true,
      command,
      ...(matched === null ? {} : { version: `v${matched[1]}` }),
    };
  }
  return { available: false, reason: "missing" };
}

/** Describes the missing-executable case; executable wt failures are surfaced by their command. */
export function worktrunkUnavailableText(): string {
  return "本机没有可用的 Worktrunk";
}


export interface WorktrunkRunOptions {
  command: string;
  timeoutMs: number;
  signal?: AbortSignal;
}

export interface WorktrunkCreateOptions extends WorktrunkRunOptions {
  repoRoot: string;
  branch: string;
  /** Git ref used as the start point for the new branch. */
  base?: string;
  /** Exact worktree directory, pinned through `--config-set worktree-path`. */
  path: string;
}

/** Creates the worktree through `wt switch --create` and returns the path it reports. */
export async function worktrunkCreate(options: WorktrunkCreateOptions): Promise<string> {
  const pinnedPath = `worktree-path="${options.path.replace(/\\/gu, "\\\\").replace(/"/gu, '\\"')}"`;
  const result = await runCommand(
    [
      options.command,
      "switch",
      "--create",
      options.branch,
      ...(options.base === undefined ? [] : ["--base", options.base]),
      "--no-cd",
      "--format",
      "json",
      "--config-set",
      pinnedPath,
    ],
    {
      cwd: options.repoRoot,
      timeoutMs: options.timeoutMs,
      ...(options.signal === undefined ? {} : { signal: options.signal }),
    },
  );
  if (!result) throw new Error(`无法执行 Worktrunk（${options.command}）：${options.path}`);
  if (result.timedOut) throw new Error(`创建隔离工作区超时（${options.timeoutMs} ms）：${options.path}`);
  if (result.aborted) throw new Error(`创建隔离工作区已取消：${options.path}`);
  if (result.exitCode !== 0) {
    throw new Error(`创建隔离工作区失败（退出码 ${result.exitCode}）：${commandFailure(result)}`);
  }
  const parsed = safeParse(result.stdout);
  if (
    typeof parsed !== "object" ||
    parsed === null ||
    !("path" in parsed) ||
    typeof parsed.path !== "string" ||
    parsed.path.length === 0
  ) {
    throw new Error(`Worktrunk 未返回工作区路径：${options.path}`);
  }
  return parsed.path;
}

export interface WorktrunkCleanupWaitContext {
  remainingMs(): number;
  interrupted(): boolean;
}

export interface WorktrunkMergeOptions extends WorktrunkRunOptions {
  /** The worktree to merge; `wt merge` merges the branch checked out here. */
  cwd: string;
  request: WorktreeMergeRequest;
  waitForRemovedWorktree(context: WorktrunkCleanupWaitContext): Promise<void>;
}

function mergeArgv(request: WorktreeMergeRequest): string[] {
  const args = ["merge", "--format", "json"];
  if (request.target !== undefined) args.push(request.target);
  if (request.squash === false) args.push("--no-squash");
  if (request.commit === false) args.push("--no-commit");
  if (request.rebase === false) args.push("--no-rebase");
  if (request.remove === false) args.push("--no-remove");
  if (request.ff === false) args.push("--no-ff");
  if (request.stage !== undefined) args.push("--stage", request.stage);
  return args;
}

/** Runs the `wt merge` pipeline in the worktree and returns the wt result object. */
export async function worktrunkMerge(options: WorktrunkMergeOptions): Promise<WorktreeMergeSummary> {
  const result = await runCommand([options.command, ...mergeArgv(options.request)], {
    cwd: options.cwd,
    timeoutMs: options.timeoutMs,
    parentDeathWatchdog: true,
    maxOutputChars: MAX_MERGE_OUTPUT_CHARS,
    ...(options.request.lifecycle === undefined ? {} : { lifecycle: options.request.lifecycle }),
    ...(options.signal === undefined ? {} : { signal: options.signal }),
    waitForBackgroundCleanup: {
      shouldWait: (commandResult) => {
        const parsed = safeParse(commandResult.stdout);
        return (
          commandResult.exitCode === 0 &&
          typeof parsed === "object" &&
          parsed !== null &&
          "removed" in parsed &&
          parsed.removed === true
        );
      },
      waitForResources: options.waitForRemovedWorktree,
    },
  });
  if (!result) throw new Error(`无法执行 Worktrunk（${options.command}）：${options.cwd}`);
  if (result.timedOut) throw new Error(`合并超时（${options.timeoutMs} ms）：${options.cwd}`);
  if (result.aborted) throw new Error(`合并已取消：${options.cwd}`);
  if (!result.executionStarted) throw new Error(`Worktrunk merge 未能启动：${options.cwd}`);
  if (result.exitCode !== 0) {
    throw new WorktreeMergeExecutionError(result.exitCode, `${result.stderr}\n${result.stdout}`);
  }
  const parsed = safeParse(result.stdout);
  if (
    typeof parsed !== "object" ||
    parsed === null ||
    !("branch" in parsed) ||
    typeof parsed.branch !== "string" ||
    !("target" in parsed) ||
    typeof parsed.target !== "string"
  ) {
    throw new Error(`Worktrunk 未返回可解析的合并结果：${options.cwd}`);
  }
  return {
    branch: parsed.branch,
    target: parsed.target,
    squashed: "squashed" in parsed && parsed.squashed === true,
    rebased: "rebased" in parsed && parsed.rebased === true,
    committed: "committed" in parsed && parsed.committed === true,
    removed: "removed" in parsed && parsed.removed === true,
  };
}

export interface WorktrunkRemoveOptions extends WorktrunkRunOptions {
  path: string;
  force: boolean;
}

/** Removes the worktree through `wt remove --foreground`; wt deletes a merged branch with it. */
export async function worktrunkRemove(options: WorktrunkRemoveOptions): Promise<WorktreeRemovalOutcome> {
  const result = await runCommand(
    [
      options.command,
      "remove",
      "--foreground",
      "--format",
      "json",
      ...(options.force ? ["-f"] : []),
      options.path,
    ],
    {
      cwd: options.path,
      timeoutMs: options.timeoutMs,
      ...(options.signal === undefined ? {} : { signal: options.signal }),
    },
  );
  if (!result) throw new Error(`无法执行 Worktrunk（${options.command}）：${options.path}`);
  if (result.timedOut) throw new Error(`删除隔离工作区超时（${options.timeoutMs} ms）：${options.path}`);
  if (result.aborted) throw new Error(`删除隔离工作区已取消：${options.path}`);
  if (result.exitCode !== 0) {
    throw new Error(`删除隔离工作区失败（退出码 ${result.exitCode}）：${commandFailure(result)}`);
  }
  const parsed = safeParse(result.stdout);
  const entry = Array.isArray(parsed) ? parsed[0] : undefined;
  let branchOutcome: "deleted" | "kept" | null = null;
  if (typeof entry === "object" && entry !== null && "branch_outcome" in entry) {
    branchOutcome = entry.branch_outcome === "deleted" ? "deleted" : "kept";
  }
  return { kind: "removed", via: "wt", branchOutcome };
}

export interface GitWorktreeRemoveOptions {
  path: string;
  force: boolean;
  timeoutMs: number;
  signal?: AbortSignal;
}

/**
 * The git fallback for removal, used when no executable wt exists. git itself refuses the main
 * worktree and paths that are not worktrees of a repository, and refuses dirty worktrees without
 * `--force`; the branch is left untouched.
 */
export async function gitWorktreeRemove(options: GitWorktreeRemoveOptions): Promise<WorktreeRemovalOutcome> {
  const result = await runCommand(
    [
      "git",
      "-C",
      options.path,
      "worktree",
      "remove",
      ...(options.force ? ["--force"] : []),
      options.path,
    ],
    { timeoutMs: options.timeoutMs, ...(options.signal === undefined ? {} : { signal: options.signal }) },
  );
  if (!result) throw new Error("无法执行 git，未清理隔离工作区");
  if (result.timedOut) throw new Error(`删除隔离工作区超时（${options.timeoutMs} ms）：${options.path}`);
  if (result.aborted) throw new Error(`删除隔离工作区已取消：${options.path}`);
  if (result.exitCode !== 0) {
    throw new Error(`删除隔离工作区失败（退出码 ${result.exitCode}）：${commandFailure(result)}`);
  }
  return { kind: "removed", via: "git", branchOutcome: null };
}

/**
 * True when `path` is a linked git worktree (its private git dir differs from the common dir).
 * The main worktree and plain directories are both rejected, so a merge can never run against
 * the primary checkout.
 */
export async function isLinkedWorktree(
  path: string,
  options: { timeoutMs: number; signal?: AbortSignal },
): Promise<boolean> {
  const run = (args: string[]) =>
    runCommand(["git", "-C", path, ...args], {
      timeoutMs: options.timeoutMs,
      ...(options.signal === undefined ? {} : { signal: options.signal }),
    });
  const gitDir = await run(["rev-parse", "--git-dir"]);
  if (!gitDir || gitDir.exitCode !== 0) return false;
  const commonDir = await run(["rev-parse", "--git-common-dir"]);
  if (!commonDir || commonDir.exitCode !== 0) return false;
  const gitDirPath = gitDir.stdout.trim();
  const commonDirPath = commonDir.stdout.trim();
  return gitDirPath.length > 0 && commonDirPath.length > 0 && gitDirPath !== commonDirPath;
}
