import { realpath, stat } from "node:fs/promises";
import type {
  ThreadWorkspace,
  ThreadWorkspaceRequest,
  WorktreeMergeRequest,
  WorktreeMergeSummary,
  WorktreePort,
  WorktreeRemovalOutcome,
  WorktreeRemovalRequest,
  WorktrunkProbe,
} from "./contracts.ts";
import {
  commandFailure,
  defaultWorktrunkCandidates,
  gitWorktreeRemove,
  isLinkedWorktree,
  probeWorktrunk,
  runCommand,
  WORKTRUNK_TIMEOUT_DEFAULTS,
  worktrunkCreate,
  worktrunkMerge,
  worktrunkRemove,
  worktrunkUnavailableText,
} from "./worktrunk.ts";

/**
 * Isolated thread workspaces: a new CommunityTopic thread can be created together with its own
 * git worktree next to the repository that backs the calling chat, and is then bound to it.
 *
 * Creation prefers the local Worktrunk CLI (`wt switch --create`, stable v0.76.x) and falls back
 * to the OMP executable the deployment already runs (`omp worktree add`, whose clone-first
 * behavior carries untracked and ignored files such as `node_modules` along). Merging and removal
 * run through wt as well; merging has no fallback, while removal falls back to plain git so the
 * `thread-delete` linkage also works on deployments without Worktrunk.
 */

const MAX_SLUG_CHARS = 32;

export interface WorktreeCandidate {
  path: string;
  branch: string;
}

export interface WorktreePortOptions {
  /** Resolved OMP executable; the fallback creates worktrees with its `worktree add` command. */
  command: string;
  /** Probe list for the Worktrunk CLI; defaults to the PATH name plus the common installs. */
  worktrunkCandidates?: readonly string[];
  createTimeoutMs?: number;
  mergeTimeoutMs?: number;
  removeTimeoutMs?: number;
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

export function createWorktreePort(options: WorktreePortOptions): WorktreePort {
  const createTimeoutMs = options.createTimeoutMs ?? WORKTRUNK_TIMEOUT_DEFAULTS.createMs;
  const mergeTimeoutMs = options.mergeTimeoutMs ?? WORKTRUNK_TIMEOUT_DEFAULTS.mergeMs;
  const removeTimeoutMs = options.removeTimeoutMs ?? WORKTRUNK_TIMEOUT_DEFAULTS.removeMs;
  const candidates = options.worktrunkCandidates ?? defaultWorktrunkCandidates();
  const probe = (signal?: AbortSignal): Promise<WorktrunkProbe> =>
    probeWorktrunk({ candidates, ...(signal === undefined ? {} : { signal }) });

  return {
    worktrunk(signal) {
      return probe(signal);
    },

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
        { timeoutMs: createTimeoutMs, ...(signal === undefined ? {} : { signal }) },
      );
      if (!root) {
        throw new Error(`目录不是 git 仓库，无法创建隔离工作区：${request.baseDir}`);
      }
      const repoRoot = root.stdout.trim();
      if (root.exitCode !== 0 || repoRoot.length === 0) {
        throw new Error(`目录不是 git 仓库，无法创建隔离工作区：${request.baseDir}`);
      }
      const branches = await runCommand(
        ["git", "-C", repoRoot, "for-each-ref", "--format=%(refname:short)", "refs/heads/"],
        { timeoutMs: createTimeoutMs, ...(signal === undefined ? {} : { signal }) },
      );
      if (!branches || branches.exitCode !== 0) {
        const detail = branches
          ? commandFailure(branches)
          : `无法执行 git：${repoRoot}`;
        throw new Error(`读取仓库分支失败：${detail}`);
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
      const available = await probe(signal);
      if (available.available) {
        const createdPath = await worktrunkCreate({
          command: available.command,
          repoRoot,
          branch: candidate.branch,
          path: candidate.path,
          timeoutMs: createTimeoutMs,
          ...(signal === undefined ? {} : { signal }),
        });
        return { cwd: await realpath(createdPath), branch: candidate.branch };
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
        { timeoutMs: createTimeoutMs, ...(signal === undefined ? {} : { signal }) },
      );
      if (!created) {
        throw new Error(`无法执行 OMP（${options.command}）：${candidate.path}`);
      }
      if (created.timedOut) {
        throw new Error(`创建隔离工作区超时（${createTimeoutMs} ms）：${candidate.path}`);
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

    /**
     * `wt merge` runs the branch checked out in the workspace into the target branch (squash,
     * rebase, fast-forward, and removal of the worktree afterwards by default). The linked
     * worktree check keeps a merge from ever running against the primary checkout; conflicts and
     * pre-merge failures leave the workspace in place with a bounded reason.
     */
    async merge(
      request: WorktreeMergeRequest,
      signal?: AbortSignal,
    ): Promise<WorktreeMergeSummary> {
      const available = await probe(signal);
      if (!available.available) {
        throw new Error(worktrunkUnavailableText(available));
      }
      const present = await stat(request.cwd).then(
        () => true,
        () => false,
      );
      if (!present) {
        throw new Error(`隔离工作区目录已不存在：${request.cwd}`);
      }
      const linked = await isLinkedWorktree(request.cwd, {
        timeoutMs: removeTimeoutMs,
        ...(signal === undefined ? {} : { signal }),
      });
      if (!linked) {
        throw new Error(`不是独立的 git worktree，无法合并：${request.cwd}`);
      }
      return worktrunkMerge({
        command: available.command,
        cwd: request.cwd,
        request,
        timeoutMs: mergeTimeoutMs,
        ...(signal === undefined ? {} : { signal }),
      });
    },

    async remove(
      request: WorktreeRemovalRequest,
      signal?: AbortSignal,
    ): Promise<WorktreeRemovalOutcome> {
      const present = await stat(request.path).then(
        () => true,
        () => false,
      );
      if (!present) return { kind: "missing" };
      const available = await probe(signal);
      const run = { timeoutMs: removeTimeoutMs, ...(signal === undefined ? {} : { signal }) };
      if (available.available) {
        return worktrunkRemove({
          command: available.command,
          path: request.path,
          force: request.force === true,
          ...run,
        });
      }
      return gitWorktreeRemove({
        path: request.path,
        force: request.force === true,
        ...run,
      });
    },
  };
}
