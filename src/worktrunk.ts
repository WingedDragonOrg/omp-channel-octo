import { homedir } from "node:os";
import { join } from "node:path";
import type {
  WorktreeMergeRequest,
  WorktreeMergeSummary,
  WorktreeRemovalOutcome,
  WorktrunkProbe,
} from "./contracts.ts";

/**
 * Worktrunk (wt) integration for isolated thread workspaces: availability probing, the
 * `wt switch --create` / `wt merge` / `wt remove` invocations, and the git fallback for removal.
 * Everything runs in the host process with bounded timeouts; the sub-thread agent triggers these
 * through host tools and never runs wt itself. Project hooks are governed by Worktrunk's own
 * approvals: the host never passes `-y` or `--no-hooks`, and an unapproved command fails the
 * invocation with a single bounded line instead of waiting for an interactive prompt.
 */

/** Only the stable v0.76.x line is treated as compatible. */
const WORKTRUNK_VERSION_PREFIX = "0.76.";
const VERSION_PATTERN = /v?(\d+\.\d+\.\d+)/u;
export const WORKTRUNK_TIMEOUT_DEFAULTS = {
  probeMs: 5_000,
  /** `bun install` runs as the blocking pre-start hook, so creation keeps a wide ceiling. */
  createMs: 120_000,
  /** The pre-merge hook runs `bun run typecheck` and the full test suite. */
  mergeMs: 600_000,
  removeMs: 120_000,
} as const;
const MAX_COMMAND_ERROR_CHARS = 200;

export interface CommandResult {
  exitCode: number;
  stdout: string;
  stderr: string;
  timedOut: boolean;
  aborted: boolean;
}

export interface RunCommandOptions {
  timeoutMs: number;
  signal?: AbortSignal;
  cwd?: string;
}

/**
 * Runs one bounded command. A missing executable reports `null` (Bun.spawn throws ENOENT before
 * a child exists); every other failure is a result with its exit code and captured output.
 */
export async function runCommand(
  argv: string[],
  options: RunCommandOptions,
): Promise<CommandResult | null> {
  let child: Bun.ReadableSubprocess;
  try {
    child = Bun.spawn(argv, {
      stdin: "ignore",
      stdout: "pipe",
      stderr: "pipe",
      ...(options.cwd === undefined ? {} : { cwd: options.cwd }),
    });
  } catch {
    return null;
  }
  let timedOut = false;
  const timer = setTimeout(() => {
    timedOut = true;
    child.kill();
  }, options.timeoutMs);
  const abort = () => child.kill();
  options.signal?.addEventListener("abort", abort, { once: true });
  try {
    const [stdout, stderr, exitCode] = await Promise.all([
      new Response(child.stdout).text(),
      new Response(child.stderr).text(),
      child.exited,
    ]);
    return { exitCode, stdout, stderr, timedOut, aborted: options.signal?.aborted === true };
  } finally {
    clearTimeout(timer);
    options.signal?.removeEventListener("abort", abort);
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
 * Probes candidates in order; a candidate counts only when `--version` exits 0 and reports a
 * v0.76.x version. The probe runs per operation (never cached), so installing or upgrading wt
 * takes effect without a channel restart.
 */
export async function probeWorktrunk(options: ProbeWorktrunkOptions): Promise<WorktrunkProbe> {
  let incompatible: { command: string; version: string } | null = null;
  for (const command of options.candidates) {
    const result = await runCommand([command, "--version"], {
      timeoutMs: options.timeoutMs ?? WORKTRUNK_TIMEOUT_DEFAULTS.probeMs,
      ...(options.signal === undefined ? {} : { signal: options.signal }),
    });
    if (!result || result.exitCode !== 0 || result.timedOut || result.aborted) continue;
    const matched = VERSION_PATTERN.exec(result.stdout) ?? VERSION_PATTERN.exec(result.stderr);
    const patchLevel = matched?.[1];
    if (patchLevel === undefined) continue;
    if (patchLevel.startsWith(WORKTRUNK_VERSION_PREFIX)) {
      return { available: true, command, version: `v${patchLevel}` };
    }
    incompatible ??= { command, version: `v${patchLevel}` };
  }
  return incompatible === null
    ? { available: false, reason: "missing" }
    : { available: false, reason: "incompatible", command: incompatible.command, version: incompatible.version };
}

/** One bounded sentence for a probe result; used by the merge command prompt and tool errors. */
export function worktrunkUnavailableText(probe: WorktrunkProbe): string {
  if (!probe.available && probe.reason === "incompatible") {
    return `本机 Worktrunk 版本不受支持（${probe.version}，需要 v0.76.x）`;
  }
  return "本机没有可用的 Worktrunk（需要 v0.76.x）";
}

export interface WorktrunkRunOptions {
  command: string;
  timeoutMs: number;
  signal?: AbortSignal;
}

export interface WorktrunkCreateOptions extends WorktrunkRunOptions {
  repoRoot: string;
  branch: string;
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

export interface WorktrunkMergeOptions extends WorktrunkRunOptions {
  /** The worktree to merge; `wt merge` merges the branch checked out here. */
  cwd: string;
  request: WorktreeMergeRequest;
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
    ...(options.signal === undefined ? {} : { signal: options.signal }),
  });
  if (!result) throw new Error(`无法执行 Worktrunk（${options.command}）：${options.cwd}`);
  if (result.timedOut) throw new Error(`合并超时（${options.timeoutMs} ms）：${options.cwd}`);
  if (result.aborted) throw new Error(`合并已取消：${options.cwd}`);
  if (result.exitCode !== 0) {
    throw new Error(`合并失败（退出码 ${result.exitCode}）：${commandFailure(result)}`);
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
    { timeoutMs: options.timeoutMs, ...(options.signal === undefined ? {} : { signal: options.signal }) },
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
 * The git fallback for removal, used when no compatible wt exists. git itself refuses the main
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
