import { dirname, relative, resolve, sep } from "node:path";
import { lstat, readlink, realpath, rm, rmdir, unlink } from "node:fs/promises";
import { safeDisplayText } from "../cards/execution-text.ts";
import type {
  WorktreePort,
  WorktreeRemovalLifecycle,
  WorktreeRemovalOutcome,
  WorktreeRemovalPlan,
  WorktreeRemovalProgress,
  WorktreeRemovalStep,
} from "../contracts.ts";
import type { ChannelStore } from "../store/index.ts";
import type { ThreadWorkspaceRecord } from "../store/store-records.ts";

function isAborted(signal: AbortSignal | undefined): boolean {
  return signal?.aborted === true;
}

/** Minimal lease contract; each caller supplies the appropriate session or resource scope. */
export interface ThreadWorkspaceCleanupLease {
  readonly ready: Promise<void>;
  complete(): void;
}

export interface ThreadWorkspaceCleanupOptions {
  readonly ownership: ThreadWorkspaceRecord;
  readonly store: Pick<ChannelStore, "saveThreadWorkspace">;
  readonly worktrees: WorktreePort | undefined;
  readonly force: boolean;
  readonly deleteBranch: boolean;
  readonly lease: ThreadWorkspaceCleanupLease;
  readonly allowBranchMismatch?: boolean;
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

/** Persisted ownership paths that the cleanup operation may touch. */
export function threadWorkspaceCleanupPaths(
  ownership: ThreadWorkspaceRecord,
  deleteBranch: boolean,
): string[] {
  const paths: string[] = [];
  for (const project of ownership.projects) {
    const ownedPath = project.removal?.path ?? project.path;
    if ((project.created || (deleteBranch && project.removal !== undefined)) && ownedPath !== null) paths.push(ownedPath);
    if (project.accessPath != null) paths.push(project.accessPath);
    if (project.accessTarget != null) paths.push(project.accessTarget);
    if (deleteBranch && (project.created || project.branch !== null)) paths.push(project.repo);
  }
  if (ownership.entryPath !== null) paths.push(ownership.entryPath);
  if (ownership.workspaceRoot != null) paths.push(ownership.workspaceRoot);
  return [...new Set(paths)];
}

/**
 * The sole persisted-owner cleanup algorithm shared by local `/thread remove` and remote thread-delete.
 * Each known project result is saved before another resource is touched; ambiguous process results
 * stop immediately. Command cleanup removes the owned shared entry after all projects are released.
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
      const hasAccessOwnership = project.accessPath !== undefined || project.accessTarget !== undefined;
      if (
        hasAccessOwnership
        && (
          ownership.layout !== "shared"
          || project.accessPath == null
          || project.accessTarget == null
          || (project.path !== null && project.path !== project.accessTarget)
        )
      ) {
        messages.push(`${project.name} 的原项目链接所有权不完整或目标不一致，资源已保留`);
        continue;
      }
      const resumeRemoval = deleteBranch && project.removal !== undefined && project.branch !== null;
      if (!project.created && !resumeRemoval) {
        if (project.path !== null || project.branch !== null || project.removal !== undefined) {
          messages.push(`${project.name} 的创建或资源状态未确认，保留记录`);
        }
        continue;
      }
      const path = project.removal?.path ?? project.path;
      if (path === null || project.branch === null) {
        messages.push(`${project.name} 缺少路径或分支所有权，未自动清理`);
        continue;
      }
      const registeredBranch = project.branch;
      if (!worktrees) {
        messages.push(`${project.name} 工作区能力不可用，资源已保留`);
        continue;
      }

      let reportedPlan = project.removal;
      let checkpointFailed = false;
      const persist = (
        update: () => void,
        failureText: string,
      ): void => {
        const previous = {
          created: project.created,
          path: project.path,
          branch: project.branch,
          removal: project.removal === undefined
            ? undefined
            : { ...project.removal, refs: project.removal.refs.map((ref) => ({ ...ref })) },
        };
        update();
        try {
          store.saveThreadWorkspace(ownership);
          reportedPlan = project.removal;
        } catch (error) {
          project.created = previous.created;
          project.path = previous.path;
          project.branch = previous.branch;
          if (previous.removal === undefined) delete project.removal;
          else project.removal = previous.removal;
          checkpointFailed = true;
          throw new Error(`${failureText}：${boundedError(error)}`);
        }
      };
      try {
        const lifecycle = options.lifecycle === undefined || !deleteBranch
          ? undefined
          : (step: WorktreeRemovalStep) => options.lifecycle!(project, step);
        const onPlanned = deleteBranch
          ? (plan: WorktreeRemovalPlan): void => {
              persist(() => {
                project.removal = { ...plan, refs: plan.refs.map((ref) => ({ ...ref })) };
              }, "清理前无法持久化实际分支与引用身份");
            }
          : undefined;
        const onProgress = deleteBranch
          ? (progress: WorktreeRemovalProgress): void => {
              persist(() => {
                if (project.removal === undefined) throw new Error("清理进度缺少预读身份记录");
                if (progress.step === "worktree") {
                  // 分支确认清理前保留原工作区所有权；removal.worktree 记录实际删除状态。
                  project.removal.worktree = progress.state;
                } else {
                  const ref = project.removal.refs.find((candidate) => candidate.branch === progress.branch);
                  if (ref === undefined) throw new Error("清理进度不属于预读分支身份");
                  ref.state = progress.state;
                }
              }, "清理步骤结果已知但所有权更新失败");
            }
          : undefined;
        const request = {
          path,
          repo: project.repo,
          force,
          branch: registeredBranch,
          ...(deleteBranch
            ? {
                deleteBranch: true,
                allowBranchMismatch: force && options.allowBranchMismatch === true,
                ...(project.removal === undefined ? {} : { expectedPlan: project.removal }),
                ...(onPlanned === undefined ? {} : { onPlanned }),
                ...(onProgress === undefined ? {} : { onProgress }),
                ...(lifecycle === undefined ? {} : { lifecycle }),
              }
            : {}),
        };
        const outcome = await worktrees.remove(request, signal);
        const planBeforeFinalize = reportedPlan;
        const previous = {
          created: project.created,
          path: project.path,
          branch: project.branch,
          removal: project.removal === undefined
            ? undefined
            : { ...project.removal, refs: project.removal.refs.map((ref) => ({ ...ref })) },
        };
        applyRemovalOutcome(project, outcome, deleteBranch);
        try {
          store.saveThreadWorkspace(ownership);
        } catch (error) {
          project.created = previous.created;
          project.path = previous.path;
          project.branch = previous.branch;
          if (previous.removal === undefined) delete project.removal;
          else project.removal = previous.removal;
          messages.push(`${project.name} 清理结果已知但所有权更新失败：${boundedError(error)}`);
          stop = true;
          break;
        }
        const actualPath = planBeforeFinalize?.path ?? path;
        const label = safeDisplayText(`${project.name}（${actualPath}；登记分支 ${planBeforeFinalize?.registeredBranch ?? registeredBranch}）`, 700) ?? "项目";
        const workspaceState = outcome.kind === "missing" ? "工作区已不存在" : "工作区已清理";
        const branchState = outcome.branchOutcome === "deleted" ? "已删除"
          : outcome.branchOutcome === "missing" ? "已不存在" : "保留";
        const branchDetails = planBeforeFinalize === undefined
          ? `登记分支 ${registeredBranch} ${branchState}`
          : planBeforeFinalize.refs.map((ref) => {
              const state = ref.state === "deleted" ? "已删除"
                : ref.state === "missing" ? "已不存在"
                  : ref.state === "retained" ? "已保留（引用已变化）" : "尚未清理";
              return `${ref.branch === planBeforeFinalize.registeredBranch ? "登记分支" : "实际分支"} ${ref.branch} ${state}`;
            }).join("，");
        messages.push(`${label}：${workspaceState}，${branchDetails}`);
      } catch (error) {
        const planForFailure = reportedPlan;
        const pathForReport = planForFailure?.path ?? path;
        const worktreeDetail = planForFailure === undefined
          ? ""
          : `；工作区${planForFailure.worktree === "removed" ? "已清理"
              : planForFailure.worktree === "missing" ? "已不存在" : "状态未确认"}`;
        const branchDetail = planForFailure === undefined
          ? ""
          : `；${planForFailure.refs.map((ref) => {
              const state = ref.state === "deleted" ? "已删除"
                : ref.state === "missing" ? "已不存在"
                  : ref.state === "retained" ? "已保留（引用已变化）" : "尚未清理";
              return `${ref.branch === planForFailure.registeredBranch ? "登记分支" : "实际分支"} ${ref.branch} ${state}`;
            }).join("，")}`;
        messages.push(
          `${safeDisplayText(`${project.name}（${pathForReport}${worktreeDetail}${branchDetail}）`, 700) ?? "项目"} 清理失败：${boundedError(error)}`,
        );
        if (checkpointFailed || options.isUnknown?.() === true || isAborted(signal)) {
          stop = true;
          break;
        }
      }
    }
    if (!stop && !isAborted(signal) && ownership.layout === "shared" && projectsWorktreesReleased(ownership)) {
      for (const project of ownership.projects) {
        if (project.accessPath === undefined && project.accessTarget === undefined) continue;
        if (project.accessPath == null || project.accessTarget == null || ownership.entryPath === null) {
          messages.push(`${project.name} 的原项目链接记录不完整，入口及所有权已保留`);
          continue;
        }
        const linkResult = await removeOwnedProjectAccessLink(
          ownership.entryPath,
          project.accessPath,
          project.accessTarget,
          signal,
        );
        if (linkResult.kind === "unsafe") {
          messages.push(`${project.name} 的原项目链接未清理，入口及所有权已保留：${linkResult.reason}`);
          continue;
        }
        const priorAccessPath = project.accessPath;
        const priorAccessTarget = project.accessTarget;
        delete project.accessPath;
        delete project.accessTarget;
        try {
          store.saveThreadWorkspace(ownership);
          if (linkResult.kind === "removed") messages.push(`${project.name} 的原项目入口链接已清理`);
        } catch (error) {
          project.accessPath = priorAccessPath;
          project.accessTarget = priorAccessTarget;
          messages.push(`${project.name} 链接处置结果已知但所有权更新失败：${boundedError(error)}`);
          stop = true;
          break;
        }
      }
    }

    if (!stop && !isAborted(signal) && ownership.layout === "shared" && ownership.entryPath !== null && projectsReleased(ownership)) {
      const entryPath = ownership.entryPath;
      const entryResult = await removeSharedEntry(entryPath, signal, deleteBranch);
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
    !project.created
    && project.path === null
    && project.branch === null
    && project.removal === undefined
    && project.accessPath == null
    && project.accessTarget == null,
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
    delete project.removal;
    return;
  }
  if (outcome.branchOutcome !== "deleted" && outcome.branchOutcome !== "missing") return;
  project.created = false;
  project.path = null;
  project.branch = null;
  delete project.removal;
}

function projectsWorktreesReleased(ownership: ThreadWorkspaceRecord): boolean {
  return ownership.projects.every(project =>
    !project.created && project.path === null && project.branch === null && project.removal === undefined,
  );
}

function projectsReleased(ownership: ThreadWorkspaceRecord): boolean {
  return projectsWorktreesReleased(ownership) && ownership.projects.every(project =>
    project.accessPath == null && project.accessTarget == null,
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

type ProjectAccessLinkRemoval =
  | { readonly kind: "removed" | "missing" }
  | { readonly kind: "unsafe"; readonly reason: string };

async function removeOwnedProjectAccessLink(
  entryPath: string,
  accessPath: string,
  accessTarget: string,
  signal?: AbortSignal,
): Promise<ProjectAccessLinkRemoval> {
  if (resolve(accessPath) !== accessPath || !isDirectChildPath(entryPath, accessPath)) {
    return { kind: "unsafe", reason: "不是共享入口的直接子项" };
  }
  if (resolve(accessTarget) !== accessTarget) {
    return { kind: "unsafe", reason: "目标路径不是规范绝对路径" };
  }
  try {
    const entryState = await inspectOwnedDirectory(entryPath);
    if (entryState.kind === "missing") return { kind: "missing" };
    if (entryState.kind === "unsafe") return { kind: "unsafe", reason: `共享入口不可安全访问：${entryState.reason}` };
    const entryInfo = await lstat(entryPath);
    if (entryInfo.isSymbolicLink() || !entryInfo.isDirectory()) {
      return { kind: "unsafe", reason: "共享入口已被替换" };
    }
    const info = await lstat(accessPath);
    if (!info.isSymbolicLink()) return { kind: "unsafe", reason: "入口项已被替换或不是符号链接" };
    const target = await readlink(accessPath);
    if (resolve(dirname(accessPath), target) !== accessTarget) {
      return { kind: "unsafe", reason: "符号链接目标与记录的原项目路径不一致" };
    }
    const current = await lstat(accessPath);
    if (!current.isSymbolicLink() || current.dev !== info.dev || current.ino !== info.ino) {
      return { kind: "unsafe", reason: "核验期间符号链接已发生变化" };
    }
    const currentEntry = await lstat(entryPath);
    if (
      currentEntry.isSymbolicLink()
      || !currentEntry.isDirectory()
      || currentEntry.dev !== entryInfo.dev
      || currentEntry.ino !== entryInfo.ino
    ) {
      return { kind: "unsafe", reason: "核验期间共享入口已发生变化" };
    }
    if (isAborted(signal)) return { kind: "unsafe", reason: "清理已中止，入口保留" };
    await unlink(accessPath);
    return { kind: "removed" };
  } catch (error) {
    if ((error as NodeJS.ErrnoException).code === "ENOENT") return { kind: "missing" };
    return { kind: "unsafe", reason: boundedError(error) };
  }
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


async function removeSharedEntry(path: string, signal?: AbortSignal, recursive = false): Promise<"removed" | "missing" | "nonempty" | string> {
  try {
    const info = await lstat(path);
    if (info.isSymbolicLink() || !info.isDirectory()) return "共享入口已被替换或不是目录，保留原路径";
    if (await realpath(path) !== path) return "共享入口路径未规范化或父路径已变化，保留原路径";
    if (isAborted(signal)) return "清理已中止，入口保留";
    if (recursive) await rm(path, { recursive: true });
    else await rmdir(path);
    return "removed";
  } catch (error) {
    const code = (error as NodeJS.ErrnoException).code;
    if (code === "ENOENT") {
      const state = await inspectOwnedDirectory(path);
      return state.kind === "missing" ? "missing" : "删除时路径发生变化，保留原路径";
    }
    if (code === "ENOTEMPTY" || code === "EEXIST") return "nonempty";
    return boundedError(error);
  }
}

function boundedError(error: unknown): string {
  return safeDisplayText(error instanceof Error ? error.message : String(error), 180) ?? "清理失败";
}
