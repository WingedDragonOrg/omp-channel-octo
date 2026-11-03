import type { BigIntStats, Stats } from "node:fs";
import { lstat, readdir, realpath } from "node:fs/promises";
import { basename, dirname, isAbsolute, resolve } from "node:path";
import type {
  ThreadWorkspace,
  ThreadWorkspaceRequest,
  WorktreeMergeProcessIdentity,
  WorktreeMergeRequest,
  WorktreeMergeSummary,
  WorktreePort,
  WorktreeRemovalOutcome,
  WorktreeRemovalRequest,
  WorktrunkProbe,
} from "../contracts.ts";
import { groupAlive, isSafeGroupId, queryProcess, terminateProcessGroup } from "../omp/process.ts";
import type { WorktrunkCleanupWaitContext } from "./worktrunk.ts";
import {
  commandFailure,
  defaultWorktrunkCandidates,
  gitWorktreeRemove,
  probeWorktrunk,
  runCommand,
  WORKTRUNK_TIMEOUT_DEFAULTS,
  worktrunkCreate,
  worktrunkMerge,
  worktrunkRemove,
  worktrunkUnavailableText,
} from "./worktrunk.ts";

/**
 * Isolated thread workspaces are linked Git worktrees from the calling chat's canonical repository.
 * They prefer the caller's project path when supplied and otherwise create a sibling worktree;
 * actual path collisions add readable numeric suffixes.
 * Candidate selection and creation are serialized by Git common directory.
 *
 * Creation prefers the local Worktrunk CLI (`wt switch --create`) and falls back to the OMP
 * executable the deployment already runs (`omp worktree add`, whose clone-first behavior carries
 * untracked and ignored files such as `node_modules`) only when wt cannot be executed. Merging
 * requires an executable wt; removal falls back to plain git only when wt cannot be executed, so
 * the `thread-delete` linkage also works on deployments without Worktrunk.
 */

const MAX_SLUG_CHARS = 100;

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
 * Full ASCII slug used for the worktree directory and branch. Names without usable ASCII
 * characters (for example Chinese names) yield an empty slug; callers reject that up front.
 */
export function worktreeSlug(label: string): string {
  return label
    .toLowerCase()
    .replace(/[^a-z0-9]+/gu, "-")
    .replace(/^-+|-+$/gu, "");
}

function isAborted(signal?: AbortSignal): boolean {
  return signal?.aborted === true;
}

const worktreeCreationTails = new Map<string, Promise<void>>();

function withWorktreeCreationLock<T>(
  commonDir: string,
  operation: () => Promise<T>,
): Promise<T> {
  const previous = worktreeCreationTails.get(commonDir) ?? Promise.resolve();
  const current = previous.then(operation);
  const tail: Promise<void> = current.then(
    () => {},
    () => {},
  );
  worktreeCreationTails.set(commonDir, tail);
  return current.finally(() => {
    if (worktreeCreationTails.get(commonDir) === tail) worktreeCreationTails.delete(commonDir);
  });
}

async function gitDirectory(
  cwd: string,
  flag: "--git-dir" | "--git-common-dir" | "--show-toplevel",
  timeoutMs: number,
  signal?: AbortSignal,
): Promise<string> {
  const result = await runCommand(["git", "-C", cwd, "rev-parse", flag], {
    timeoutMs,
    ...(signal === undefined ? {} : { signal }),
  });
  if (!result) throw new Error(`无法执行 git，无法读取 ${flag}：${cwd}`);
  if (result.timedOut) throw new Error(`读取 Git 目录超时：${cwd}`);
  if (result.aborted) throw new Error(`读取 Git 目录已取消：${cwd}`);
  if (result.exitCode !== 0) {
    throw new Error(`读取 Git 目录失败：${commandFailure(result)}`);
  }
  const value = result.stdout.trim();
  if (value.length === 0) throw new Error(`Git 未返回 ${flag}：${cwd}`);
  return realpath(isAbsolute(value) ? value : resolve(cwd, value));
}

function missingPath(error: unknown): boolean {
  return (
    typeof error === "object" &&
    error !== null &&
    "code" in error &&
    (error.code === "ENOENT" || error.code === "ENOTDIR")
  );
}

type PathStat = Stats | BigIntStats;

async function lstatIfPresent(path: string): Promise<PathStat | null> {
  try {
    return await lstat(path);
  } catch (error) {
    if (missingPath(error)) return null;
    throw error;
  }
}

interface WorktreeMergeIdentity {
  requestedPath: string;
  path: string;
  device: number | bigint;
  inode: number | bigint;
  commonDir: string;
  branch: string | null;
}

async function waitForWorktreeRemoval(
  identity: WorktreeMergeIdentity,
  context: WorktrunkCleanupWaitContext,
): Promise<void> {
  for (;;) {
    const current = await lstatIfPresent(identity.path);
    if (current === null) return;
    // wt moves the removed tree into .git/wt/trash and briefly leaves an empty path placeholder.
    if (current.isSymbolicLink() || !current.isDirectory()) {
      throw new Error(`Worktrunk 后台清理期间隔离工作区路径已更换，无法确认删除：${identity.requestedPath}`);
    }
    if (
      (current.dev !== identity.device || current.ino !== identity.inode) &&
      (await readdir(identity.path)).length > 0
    ) {
      throw new Error(`Worktrunk 后台清理期间隔离工作区路径已更换，无法确认删除：${identity.requestedPath}`);
    }
    if (context.interrupted()) {
      throw new Error(`等待 Worktrunk 后台清理已取消：${identity.requestedPath}`);
    }
    const remainingMs = context.remainingMs();
    if (remainingMs <= 0) {
      throw new Error(`等待 Worktrunk 后台清理超时：${identity.requestedPath}`);
    }
    await Bun.sleep(Math.min(20, remainingMs));
  }
}

async function inspectWorktreeForMerge(
  request: WorktreeMergeRequest,
  timeoutMs: number,
  signal?: AbortSignal,
): Promise<WorktreeMergeIdentity> {
  if (signal?.aborted === true) throw new Error(`合并隔离工作区已取消：${request.cwd}`);
  const requestedPath = resolve(request.cwd);
  const pathStat = await lstatIfPresent(requestedPath);
  if (pathStat === null) throw new Error(`隔离工作区目录已不存在：${request.cwd}`);
  if (pathStat.isSymbolicLink() || !pathStat.isDirectory()) {
    throw new Error(`不是独立的 git worktree，无法合并：${request.cwd}`);
  }
  const path = await realpath(requestedPath);
  if (path !== requestedPath) {
    throw new Error(`隔离工作区路径已更换，拒绝合并：${request.cwd}`);
  }
  const topLevel = await gitDirectory(path, "--show-toplevel", timeoutMs, signal);
  const gitDir = await gitDirectory(path, "--git-dir", timeoutMs, signal);
  const commonDir = await gitDirectory(path, "--git-common-dir", timeoutMs, signal);
  if (topLevel !== path || gitDir === commonDir) {
    throw new Error(`不是独立的 git worktree，无法合并：${request.cwd}`);
  }
  const branchResult = await runCommand(
    ["git", "-C", path, "branch", "--show-current"],
    { timeoutMs, ...(signal === undefined ? {} : { signal }) },
  );
  if (!branchResult) throw new Error(`无法执行 git，无法核对隔离工作区分支：${request.cwd}`);
  if (branchResult.timedOut) throw new Error(`核对隔离工作区分支超时：${request.cwd}`);
  if (branchResult.aborted) throw new Error(`核对隔离工作区分支已取消：${request.cwd}`);
  if (branchResult.exitCode !== 0) {
    throw new Error(`核对隔离工作区分支失败：${commandFailure(branchResult)}`);
  }
  const branch = branchResult.stdout.trim() || null;
  if (request.branch !== undefined && branch !== request.branch) {
    throw new Error(`隔离工作区分支与所有权记录不匹配：${request.cwd}`);
  }
  if (request.repo !== undefined) {
    const sourcePath = resolve(request.repo);
    const sourceRoot = await realpath(sourcePath);
    if (sourceRoot !== sourcePath) {
      throw new Error(`来源仓库路径已更换，拒绝合并：${request.cwd}`);
    }
    const sourceCommonDir = await gitDirectory(sourceRoot, "--git-common-dir", timeoutMs, signal);
    if (sourceCommonDir !== commonDir) {
      throw new Error(`隔离工作区所属仓库与所有权记录不匹配：${request.cwd}`);
    }
  }
  const identity: WorktreeMergeIdentity = {
    requestedPath,
    path,
    device: pathStat.dev,
    inode: pathStat.ino,
    commonDir,
    branch,
  };
  const current = await lstatIfPresent(requestedPath);
  if (
    current === null ||
    current.isSymbolicLink() ||
    !current.isDirectory() ||
    current.dev !== identity.device ||
    current.ino !== identity.inode ||
    (await realpath(requestedPath)) !== identity.path
  ) {
    throw new Error(`隔离工作区路径已更换，拒绝合并：${request.cwd}`);
  }
  return identity;
}

async function exactWorktreePath(path: string): Promise<string> {
  const absolutePath = resolve(path);
  const name = basename(absolutePath);
  if (name.length === 0) throw new Error(`隔离工作区路径不能是文件系统根目录：${path}`);
  return resolve(await realpath(dirname(absolutePath)), name);
}

interface WorktreeRemovalIdentity {
  requestedPath: string;
  path: string;
  device: number | bigint;
  inode: number | bigint;
  commonDir: string;
  branch: string | null;
}

async function assertWorktreePathUnchanged(identity: WorktreeRemovalIdentity): Promise<void> {
  const current = await lstatIfPresent(identity.requestedPath);
  if (
    current === null ||
    current.isSymbolicLink() ||
    !current.isDirectory() ||
    current.dev !== identity.device ||
    current.ino !== identity.inode ||
    (await realpath(identity.requestedPath)) !== identity.path
  ) {
    throw new Error(`隔离工作区路径已更换，拒绝删除：${identity.requestedPath}`);
  }
}

async function inspectWorktreeForRemoval(
  request: WorktreeRemovalRequest,
  timeoutMs: number,
  signal?: AbortSignal,
): Promise<WorktreeRemovalIdentity | null> {
  if (signal?.aborted === true) throw new Error(`删除隔离工作区已取消：${request.path}`);
  const requestedPath = resolve(request.path);
  const pathStat = await lstatIfPresent(requestedPath);
  if (pathStat === null) return null;
  if (pathStat.isSymbolicLink()) {
    throw new Error(`拒绝通过符号链接删除隔离工作区：${request.path}`);
  }
  if (!pathStat.isDirectory()) throw new Error(`隔离工作区路径不是目录：${request.path}`);
  const path = await realpath(requestedPath);
  if (path !== requestedPath) {
    throw new Error(`拒绝通过符号链接路径删除隔离工作区：${request.path}`);
  }
  const topLevel = await gitDirectory(path, "--show-toplevel", timeoutMs, signal);
  if (topLevel !== path) throw new Error(`隔离工作区路径不是 Git worktree 根目录：${request.path}`);
  const gitDir = await gitDirectory(path, "--git-dir", timeoutMs, signal);
  const commonDir = await gitDirectory(path, "--git-common-dir", timeoutMs, signal);
  if (gitDir === commonDir) {
    throw new Error(`不能删除 main working tree：${request.path}`);
  }
  const branchResult = await runCommand(
    ["git", "-C", path, "branch", "--show-current"],
    { timeoutMs, ...(signal === undefined ? {} : { signal }) },
  );
  if (!branchResult) throw new Error(`无法执行 git，无法核对隔离工作区分支：${request.path}`);
  if (branchResult.timedOut) throw new Error(`核对隔离工作区分支超时：${request.path}`);
  if (branchResult.aborted) throw new Error(`核对隔离工作区分支已取消：${request.path}`);
  if (branchResult.exitCode !== 0) {
    throw new Error(`核对隔离工作区分支失败：${commandFailure(branchResult)}`);
  }
  const branch = branchResult.stdout.trim() || null;
  const identity: WorktreeRemovalIdentity = {
    requestedPath,
    path,
    device: pathStat.dev,
    inode: pathStat.ino,
    commonDir,
    branch,
  };
  await assertWorktreePathUnchanged(identity);

  if (request.repo !== undefined) {
    const sourcePath = resolve(request.repo);
    const sourceRoot = await realpath(sourcePath);
    if (sourceRoot !== sourcePath) {
      throw new Error(`来源仓库路径已更换，拒绝删除隔离工作区：${request.path}`);
    }
    const sourceCommonDir = await gitDirectory(sourceRoot, "--git-common-dir", timeoutMs, signal);
    if (sourceCommonDir !== commonDir) {
      throw new Error(`隔离工作区所属仓库与所有权记录不匹配：${request.path}`);
    }
  }
  if (request.branch !== undefined && branch !== request.branch) {
    throw new Error(`隔离工作区分支与所有权记录不匹配：${request.path}`);
  }
  await assertWorktreePathUnchanged(identity);
  return identity;
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
      if (isAborted(signal)) {
        throw new Error(`创建隔离工作区已取消：${request.baseDir}`);
      }
      const slug = worktreeSlug(request.label);
      if (slug.length === 0) {
        throw new Error(`worktree 名称必须包含 ASCII 字母或数字：${request.label}`);
      }
      if (slug.length > MAX_SLUG_CHARS) {
        throw new Error(
          `worktree 名称超过 ${MAX_SLUG_CHARS} 个字符；请提供较短但保留完整语义的名称：${request.label}`,
        );
      }
      const root = await runCommand(
        ["git", "-C", request.baseDir, "rev-parse", "--show-toplevel"],
        { timeoutMs: createTimeoutMs, ...(signal === undefined ? {} : { signal }) },
      );
      if (!root) {
        throw new Error(`目录不是 git 仓库，无法创建隔离工作区：${request.baseDir}`);
      }
      if (root.timedOut) {
        throw new Error(`读取 Git 仓库超时：${request.baseDir}`);
      }
      if (root.aborted) {
        throw new Error(`创建隔离工作区已取消：${request.baseDir}`);
      }
      const repoRootOutput = root.stdout.trim();
      if (root.exitCode !== 0 || repoRootOutput.length === 0) {
        throw new Error(`目录不是 git 仓库，无法创建隔离工作区：${request.baseDir}`);
      }
      const repoRoot = await realpath(repoRootOutput);
      const commonDir = await gitDirectory(repoRoot, "--git-common-dir", createTimeoutMs, signal);

      return withWorktreeCreationLock(commonDir, async () => {
        if (signal?.aborted === true) {
          throw new Error(`创建隔离工作区已取消：${request.baseDir}`);
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
        const exactPath = request.path === undefined ? undefined : await exactWorktreePath(request.path);
        const requestedPath = exactPath ?? `${repoRoot}.${slug}`;
        let branch = slug;
        let branchSuffix = 2;
        while (taken.has(branch)) {
          branch = `${slug}-${branchSuffix}`;
          branchSuffix += 1;
        }
        let path = requestedPath;
        let pathSuffix = 2;
        while (await lstatIfPresent(path)) {
          path = `${requestedPath}-${pathSuffix}`;
          pathSuffix += 1;
        }
        const candidate = { path, branch };

        request.onPlanned?.({
          cwd: candidate.path,
          branch: candidate.branch,
          repo: repoRoot,
        });
        if (isAborted(signal)) {
          throw new Error(`创建隔离工作区已取消：${candidate.path}`);
        }
        const available = await probe(signal);
        if (isAborted(signal)) {
          throw new Error(`创建隔离工作区已取消：${candidate.path}`);
        }
        if (available.available) {
          const reportedPath = await worktrunkCreate({
            command: available.command,
            repoRoot,
            branch: candidate.branch,
            path: candidate.path,
            ...(request.base === undefined ? {} : { base: request.base }),
            timeoutMs: createTimeoutMs,
            ...(signal === undefined ? {} : { signal }),
          });
          const actualPath = await realpath(candidate.path);
          const created = await lstat(candidate.path);
          if (
            created.isSymbolicLink() ||
            !created.isDirectory() ||
            actualPath !== candidate.path ||
            (await realpath(isAbsolute(reportedPath) ? reportedPath : resolve(repoRoot, reportedPath))) !==
              candidate.path
          ) {
            throw new Error(`Worktrunk 创建到非预期工作区目录：${candidate.path}`);
          }
          return { cwd: actualPath, branch: candidate.branch };
        }
        const created = await runCommand(
          [
            options.command,
            "worktree",
            "add",
            candidate.path,
            ...(request.base === undefined ? [] : [request.base]),
            "-b",
            candidate.branch,
            "-C",
            repoRoot,
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
        const actualPath = await realpath(candidate.path);
        const createdPath = await lstat(candidate.path);
        if (createdPath.isSymbolicLink() || !createdPath.isDirectory() || actualPath !== candidate.path) {
          throw new Error(`OMP 创建到非预期工作区目录：${candidate.path}`);
        }
        return { cwd: actualPath, branch: candidate.branch };
      });
    },

    /**
     * `wt merge` only starts after the linked worktree path, common repository, and checked-out
     * branch have been verified against persisted ownership when available.
     */
    async merge(
      request: WorktreeMergeRequest,
      signal?: AbortSignal,
    ): Promise<WorktreeMergeSummary> {
      const available = await probe(signal);
      if (!available.available) {
        throw new Error(worktrunkUnavailableText());
      }
      const identity = await inspectWorktreeForMerge(request, removeTimeoutMs, signal);
      const summary = await worktrunkMerge({
        command: available.command,
        cwd: identity.path,
        request,
        timeoutMs: mergeTimeoutMs,
        waitForRemovedWorktree: (context) => waitForWorktreeRemoval(identity, context),
        ...(signal === undefined ? {} : { signal }),
      });
      const targetRef = summary.target.startsWith("refs/heads/")
        ? summary.target
        : `refs/heads/${summary.target}`;
      const targetHead = await runCommand(
        ["git", "--git-dir", identity.commonDir, "rev-parse", "--verify", "--end-of-options", targetRef],
        { timeoutMs: mergeTimeoutMs, ...(signal === undefined ? {} : { signal }) },
      );
      if (!targetHead || targetHead.timedOut || targetHead.aborted || targetHead.exitCode !== 0) {
        throw new Error(
          `合并后无法读取目标提交：${targetHead === null ? "git command unavailable" : commandFailure(targetHead)}`,
        );
      }
      const commit = targetHead.stdout.trim();
      if (commit.length === 0) throw new Error(`合并后 Git 未返回目标提交：${targetRef}`);
      return { ...summary, commit };
    },

    async recoverMergeProcess(identity: WorktreeMergeProcessIdentity): Promise<boolean> {
      if (
        process.platform === "win32" ||
        identity.pid !== identity.pgid ||
        !isSafeGroupId(identity.pgid) ||
        identity.identity.length === 0
      ) {
        return false;
      }
      if (!groupAlive(identity.pgid)) return true;
      const snapshot = await queryProcess(identity.pid);
      if (!snapshot || snapshot.pgid !== identity.pgid || snapshot.identity !== identity.identity) {
        return false;
      }
      return terminateProcessGroup(identity.pgid, 1_000);
    },

    async remove(
      request: WorktreeRemovalRequest,
      signal?: AbortSignal,
    ): Promise<WorktreeRemovalOutcome> {
      const initial = await inspectWorktreeForRemoval(request, removeTimeoutMs, signal);
      if (initial === null) return { kind: "missing" };
      const available = await probe(signal);
      if (signal?.aborted === true) {
        throw new Error(`删除隔离工作区已取消：${request.path}`);
      }
      const current = await inspectWorktreeForRemoval(request, removeTimeoutMs, signal);
      if (current === null) return { kind: "missing" };
      if (
        current.path !== initial.path ||
        current.device !== initial.device ||
        current.inode !== initial.inode ||
        current.commonDir !== initial.commonDir ||
        current.branch !== initial.branch
      ) {
        throw new Error(`隔离工作区在删除前已更换，拒绝删除：${request.path}`);
      }
      await assertWorktreePathUnchanged(current);
      if (isAborted(signal)) {
        throw new Error(`删除隔离工作区已取消：${request.path}`);
      }
      const run = { timeoutMs: removeTimeoutMs, ...(signal === undefined ? {} : { signal }) };
      if (available.available) {
        return worktrunkRemove({
          command: available.command,
          path: current.path,
          force: request.force === true,
          ...run,
        });
      }
      return gitWorktreeRemove({
        path: current.path,
        force: request.force === true,
        ...run,
      });
    },
  };
}
