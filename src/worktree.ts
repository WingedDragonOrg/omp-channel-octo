import { realpath, stat } from "node:fs/promises";
import type { ThreadWorkspace, ThreadWorkspaceRequest, WorktreePort } from "./contracts.ts";

/**
 * Isolated thread workspaces: a new CommunityTopic thread can be created together with its own
 * git worktree next to the repository that backs the calling chat, and is then bound to it.
 *
 * Creation goes through the OMP executable the deployment already runs (`omp worktree add`), so
 * the workspace picks up the same clone-first behavior as OMP's own isolation: untracked and
 * ignored files such as `node_modules` come along instead of needing a fresh install.
 */

/** `omp worktree add` can clone a full working tree; keep a wide but bounded ceiling. */
const DEFAULT_WORKTREE_TIMEOUT_MS = 120_000;
const MAX_SLUG_CHARS = 32;
const MAX_COMMAND_ERROR_CHARS = 200;

export interface WorktreeCandidate {
  path: string;
  branch: string;
}

export interface OmpWorktreePortOptions {
  /** Resolved OMP executable; worktrees are created with its `worktree add` command. */
  command: string;
  timeoutMs?: number;
}

/**
 * ASCII slug used for the worktree directory and branch. Names without usable ASCII characters
 * (for example Chinese names) yield an empty slug; `octo_management` rejects that up front so a
 * workspace always carries the name the caller gave it.
 */
export function worktreeSlug(label: string): string {
  return label
    .toLowerCase()
    .replace(/[^a-z0-9]+/gu, "-")
    .slice(0, MAX_SLUG_CHARS)
    .replace(/^-+|-+$/gu, "");
}

/**
 * Workspace locations to try, most readable first; the first free candidate wins. `slug` must be
 * a non-empty `worktreeSlug` result: a name that carries no ASCII letters or digits cannot name a
 * workspace, and callers reject it before reaching this point.
 */
export function worktreeCandidates(
  repoRoot: string,
  slug: string,
  threadId: string,
): WorktreeCandidate[] {
  return [
    { path: `${repoRoot}.${slug}`, branch: slug },
    { path: `${repoRoot}.${slug}-${threadId}`, branch: `${slug}-${threadId}` },
  ];
}

interface CommandResult {
  exitCode: number;
  stdout: string;
  stderr: string;
  timedOut: boolean;
  aborted: boolean;
}

async function runCommand(
  argv: string[],
  timeoutMs: number,
  signal: AbortSignal | undefined,
): Promise<CommandResult> {
  const child = Bun.spawn(argv, { stdin: "ignore", stdout: "pipe", stderr: "pipe" });
  let timedOut = false;
  const timer = setTimeout(() => {
    timedOut = true;
    child.kill();
  }, timeoutMs);
  const abort = () => child.kill();
  signal?.addEventListener("abort", abort, { once: true });
  try {
    const [stdout, stderr, exitCode] = await Promise.all([
      new Response(child.stdout).text(),
      new Response(child.stderr).text(),
      child.exited,
    ]);
    return { exitCode, stdout, stderr, timedOut, aborted: signal?.aborted === true };
  } finally {
    clearTimeout(timer);
    signal?.removeEventListener("abort", abort);
  }
}

function commandFailure(result: CommandResult): string {
  const detail = `${result.stderr}\n${result.stdout}`.replace(/\s+/gu, " ").trim();
  const visible = detail.length > 0 ? detail : "命令未返回错误详情";
  return visible.slice(0, MAX_COMMAND_ERROR_CHARS);
}

export function createOmpWorktreePort(options: OmpWorktreePortOptions): WorktreePort {
  const timeoutMs = options.timeoutMs ?? DEFAULT_WORKTREE_TIMEOUT_MS;
  return {
    async create(
      request: ThreadWorkspaceRequest,
      signal?: AbortSignal,
    ): Promise<ThreadWorkspace> {
      if (signal?.aborted === true) {
        throw new Error(`创建隔离工作区已取消：${request.baseDir}`);
      }
      const slug = worktreeSlug(request.label);
      if (slug.length === 0) {
        throw new Error(`worktree 名称必须包含 ASCII 字母或数字：${request.label}`);
      }
      const root = await runCommand(
        ["git", "-C", request.baseDir, "rev-parse", "--show-toplevel"],
        timeoutMs,
        signal,
      );
      const repoRoot = root.stdout.trim();
      if (root.exitCode !== 0 || repoRoot.length === 0) {
        throw new Error(`目录不是 git 仓库，无法创建隔离工作区：${request.baseDir}`);
      }
      const branches = await runCommand(
        ["git", "-C", repoRoot, "for-each-ref", "--format=%(refname:short)", "refs/heads/"],
        timeoutMs,
        signal,
      );
      if (branches.exitCode !== 0) {
        throw new Error(`读取仓库分支失败：${commandFailure(branches)}`);
      }
      const taken = new Set(branches.stdout.split("\n").map((line) => line.trim()));
      let candidate: WorktreeCandidate | undefined;
      for (const entry of worktreeCandidates(repoRoot, slug, request.threadId)) {
        if (taken.has(entry.branch)) continue;
        const occupied = await stat(entry.path).then(
          () => true,
          () => false,
        );
        if (occupied) continue;
        candidate = entry;
        break;
      }
      if (!candidate) {
        throw new Error(`无法为线程分配隔离工作区目录：${repoRoot}.${slug} 已被占用`);
      }
      const created = await runCommand(
        [
          options.command,
          "worktree",
          "add",
          candidate.path,
          "-b",
          candidate.branch,
          "-C",
          request.baseDir,
          "-q",
        ],
        timeoutMs,
        signal,
      );
      if (created.timedOut) {
        throw new Error(`创建隔离工作区超时（${timeoutMs} ms）：${candidate.path}`);
      }
      if (created.aborted) {
        throw new Error(`创建隔离工作区已取消：${candidate.path}`);
      }
      if (created.exitCode !== 0) {
        throw new Error(
          `创建隔离工作区失败（退出码 ${created.exitCode}）：${commandFailure(created)}`,
        );
      }
      return { cwd: await realpath(candidate.path), branch: candidate.branch };
    },
  };
}
