import { relative, sep } from "node:path";
import { lstat, realpath, rmdir } from "node:fs/promises";
import { safeDisplayText } from "../cards/execution-text.ts";
import type {
  WorktreePort,
  WorktreeRemovalLifecycle,
  WorktreeRemovalOutcome,
  WorktreeRemovalStep,
} from "../contracts.ts";
import type { ChannelStore } from "../store/index.ts";
import type { ThreadWorkspaceRecord } from "../store/store-records.ts";
import type { DestructiveResourceLease } from "../tasks/task-scheduler.ts";

function isAborted(signal: AbortSignal | undefined): boolean {
  return signal?.aborted === true;
}

export interface ThreadWorkspaceCleanupOptions {
  readonly ownership: ThreadWorkspaceRecord;
  readonly store: Pick<ChannelStore, "saveThreadWorkspace">;
  readonly worktrees: WorktreePort | undefined;
  readonly force: boolean;
  readonly deleteBranch: boolean;
  readonly lease: DestructiveResourceLease;
  readonly signal?: AbortSignal;
  readonly lifecycle?: (
    project: ThreadWorkspaceRecord["projects"][number],
    step: WorktreeRemovalStep,
  ) => WorktreeRemovalLifecycle;
  readonly isUnknown?: () => boolean;
}

export interface ThreadWorkspaceCleanupResult {
  readonly complete: boolean;
  readonly unknown: boolean;
  readonly messages: readonly string[];
}

/** Paths whose persisted ownership must be exclusively leased before any cleanup begins. */
export function threadWorkspaceCleanupPaths(
  ownership: ThreadWorkspaceRecord,
  deleteBranch: boolean,
): string[] {
  const paths: string[] = [];
  for (const project of ownership.projects) {
    if (project.created && project.path !== null) paths.push(project.path);
    if (deleteBranch && (project.created || project.branch !== null)) paths.push(project.repo);
  }
  if (ownership.entryPath !== null) paths.push(ownership.entryPath);
  if (ownership.workspaceRoot != null) paths.push(ownership.workspaceRoot);
  return [...new Set(paths)];
}

/**
 * The sole persisted-owner cleanup algorithm shared by local `/thread remove` and remote thread-delete.
 * Each known project result is saved before another resource is touched; ambiguous process results
 * stop immediately, and shared entries are only removed with a checked, non-recursive rmdir.
 */
export async function cleanupOwnedThreadWorkspace(
  options: ThreadWorkspaceCleanupOptions,
): Promise<ThreadWorkspaceCleanupResult> {
  const { ownership, store, worktrees, force, deleteBranch, lease, signal } = options;
  const messages: string[] = [];
  let stop = false;
  const priorStatus = ownership.status;
  const priorError = ownership.error;
  ownership.status = "failed";
  ownership.error = "线程工作区清理尚未完成";
  try {
    store.saveThreadWorkspace(ownership);
  } catch (error) {
    ownership.status = priorStatus;
    ownership.error = priorError;
    lease.complete();
    return {
      complete: false,
      unknown: false,
      messages: [`清理前无法持久化工作区状态，资源已保留：${boundedError(error)}`],
    };
  }

  try {
    await lease.ready;
    if (ownership.layout === "shared") {
      const rootPath = ownership.workspaceRoot ?? null;
      const entryPath = ownership.entryPath;
      if (rootPath !== null && entryPath !== null && !isDirectChildPath(rootPath, entryPath)) {
        return { complete: false, unknown: false, messages: ["共享入口不属于已记录的任务目录，资源已保留"] };
      }
      const rootState = rootPath === null ? null : await inspectOwnedDirectory(rootPath);
      if (rootState?.kind === "unsafe") {
        return { complete: false, unknown: false, messages: [`任务目录已被替换，资源已保留：${rootState.reason}`] };
      }
      if (entryPath !== null) {
        const entryState = await inspectOwnedDirectory(entryPath);
        if (entryState.kind === "unsafe") {
          return { complete: false, unknown: false, messages: [`共享入口已被替换，资源已保留：${entryState.reason}`] };
        }
        if (rootState?.kind === "missing" && entryState.kind === "valid") {
          return { complete: false, unknown: false, messages: ["任务目录已不存在但入口仍存在，资源已保留"] };
        }
      }
    }
    for (const project of ownership.projects) {
      if (isAborted(signal)) {
        stop = true;
        messages.push("清理已中止；未归档");
        break;
      }
      if (!project.created) {
        if (project.path !== null || project.branch !== null) {
          messages.push(`${project.name} 的创建或资源状态未确认，保留记录`);
        }
        continue;
      }
      if (project.path === null || project.branch === null) {
        messages.push(`${project.name} 缺少路径或分支所有权，未自动清理`);
        continue;
      }
      if (!worktrees) {
        messages.push(`${project.name} 工作区能力不可用，资源已保留`);
        continue;
      }

      const prior = { created: project.created, path: project.path, branch: project.branch };
      try {
        const lifecycle = options.lifecycle === undefined || !deleteBranch
          ? undefined
          : (step: WorktreeRemovalStep) => options.lifecycle!(project, step);
        const request = {
          path: project.path,
          force,
          repo: project.repo,
          ...(project.branch === null ? {} : { branch: project.branch }),
          ...(deleteBranch ? { deleteBranch: true, ...(lifecycle === undefined ? {} : { lifecycle }) } : {}),
        };
        const outcome = await worktrees.remove(request, signal);
        applyRemovalOutcome(project, outcome, deleteBranch);
        try {
          store.saveThreadWorkspace(ownership);
        } catch (error) {
          Object.assign(project, prior);
          messages.push(`${project.name} 清理结果已知但所有权更新失败：${boundedError(error)}`);
          stop = true;
          break;
        }
        const label = safeDisplayText(`${project.name}（${prior.path}；分支 ${prior.branch}）`, 700) ?? "项目";
        const workspaceState = outcome.kind === "missing" ? "工作区已不存在" : "工作区已清理";
        const branchState = outcome.branchOutcome === "deleted" ? "分支已删除"
          : outcome.branchOutcome === "missing" ? "分支已不存在" : "分支保留";
        messages.push(`${label}：${workspaceState}，${branchState}`);
      } catch (error) {
        messages.push(`${safeDisplayText(`${project.name}（${project.path}；分支 ${project.branch}）`, 700) ?? "项目"} 清理失败：${boundedError(error)}`);
        if (options.isUnknown?.() === true || isAborted(signal)) {
          stop = true;
          break;
        }
      }
    }

    if (!stop && !isAborted(signal) && ownership.layout === "shared" && ownership.entryPath !== null && projectsReleased(ownership)) {
      const entryPath = ownership.entryPath;
      const entryResult = await removeSharedEntry(entryPath, signal);
      if (entryResult === "removed" || entryResult === "missing") {
        ownership.entryPath = null;
        try {
          store.saveThreadWorkspace(ownership);
          if (entryResult === "removed") messages.push("共享入口已清理");
        } catch (error) {
          ownership.entryPath = entryPath;
          messages.push(`共享入口状态更新失败：${boundedError(error)}`);
        }
      } else if (entryResult === "nonempty") {
        messages.push("共享入口仍含有文件，已保留");
      } else {
        messages.push(`共享入口未清理：${entryResult}`);
      }
    }
    const rootPath = ownership.workspaceRoot ?? null;
    if (
      !stop
      && !isAborted(signal)
      && ownership.layout === "shared"
      && rootPath !== null
      && ownership.entryPath === null
      && projectsReleased(ownership)
    ) {
      const rootResult = await removeSharedEntry(rootPath, signal);
      if (rootResult === "removed" || rootResult === "missing") {
        ownership.workspaceRoot = null;
        try {
          store.saveThreadWorkspace(ownership);
          if (rootResult === "removed") messages.push("任务目录已清理");
        } catch (error) {
          ownership.workspaceRoot = rootPath;
          messages.push(`任务目录状态更新失败：${boundedError(error)}`);
        }
      } else if (rootResult === "nonempty") {
        messages.push("任务目录仍含有未知资源，已保留");
      } else {
        messages.push(`任务目录未清理：${rootResult}`);
      }
    }
  } catch (error) {
    messages.push(`工作区清理失败：${boundedError(error)}`);
  } finally {
    lease.complete();
  }

  const complete = ownership.projects.every(project =>
    !project.created && project.path === null && project.branch === null,
  ) && ownership.entryPath === null && (ownership.workspaceRoot ?? null) === null;
  return {
    complete,
    unknown: options.isUnknown?.() === true,
    messages,
  };
}

function applyRemovalOutcome(
  project: ThreadWorkspaceRecord["projects"][number],
  outcome: WorktreeRemovalOutcome,
  deleteBranch: boolean,
): void {
  if (!deleteBranch) {
    project.created = false;
    project.path = null;
    project.branch = null;
    return;
  }
  if (outcome.branchOutcome === "kept") return;
  if (
    deleteBranch
    && outcome.branchOutcome !== "deleted"
    && outcome.branchOutcome !== "missing"
  ) {
    return;
  }
  project.created = false;
  project.path = null;
  if (
    outcome.branchOutcome === "deleted"
    || outcome.branchOutcome === "missing"
    || (!deleteBranch && outcome.kind === "missing")
  ) {
    project.branch = null;
  }
}

function projectsReleased(ownership: ThreadWorkspaceRecord): boolean {
  return ownership.projects.every(project =>
    !project.created && project.path === null && project.branch === null,
  );
}
type OwnedDirectoryInspection =
  | { readonly kind: "valid" }
  | { readonly kind: "missing" }
  | { readonly kind: "unsafe"; readonly reason: string };

function isDirectChildPath(parent: string, child: string): boolean {
  const relativePath = relative(parent, child);
  return relativePath !== ""
    && relativePath !== ".."
    && !relativePath.startsWith(`..${sep}`)
    && !relativePath.includes(sep);
}

async function inspectOwnedDirectory(path: string): Promise<OwnedDirectoryInspection> {
  try {
    const info = await lstat(path);
    if (info.isSymbolicLink() || !info.isDirectory()) {
      return { kind: "unsafe", reason: "不是原有的目录" };
    }
    if (await realpath(path) !== path) {
      return { kind: "unsafe", reason: "路径未规范化或父路径已变化" };
    }
    return { kind: "valid" };
  } catch (error) {
    if ((error as NodeJS.ErrnoException).code === "ENOENT") return { kind: "missing" };
    return { kind: "unsafe", reason: boundedError(error) };
  }
}


async function removeSharedEntry(path: string, signal?: AbortSignal): Promise<"removed" | "missing" | "nonempty" | string> {
  try {
    const info = await lstat(path);
    if (info.isSymbolicLink() || !info.isDirectory()) return "共享入口已被替换或不是目录，保留原路径";
    if (await realpath(path) !== path) return "共享入口路径未规范化或父路径已变化，保留原路径";
    if (isAborted(signal)) return "清理已中止，入口保留";
    await rmdir(path);
    return "removed";
  } catch (error) {
    const code = (error as NodeJS.ErrnoException).code;
    if (code === "ENOENT") return "missing";
    if (code === "ENOTEMPTY" || code === "EEXIST") return "nonempty";
    return boundedError(error);
  }
}

function boundedError(error: unknown): string {
  return safeDisplayText(error instanceof Error ? error.message : String(error), 180) ?? "清理失败";
}
