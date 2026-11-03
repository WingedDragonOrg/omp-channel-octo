import { throwIfAborted } from "../abort.ts";
import type {
  ChatTarget,
  WorktreeMergeLifecycle,
  WorktreeMergeOutcome,
  WorktreeMergeRequest,
  WorktreeMergeSummary,
  WorktreePort,
} from "../contracts.ts";
import { threadWorkspaceIsAvailable } from "../session/session-lifecycle.ts";
import type { ChannelStore } from "../store/index.ts";
import type { ThreadWorkspaceRecord } from "../store/store-records.ts";
import type { DirectoryLease } from "../tasks/task-scheduler.ts";
import { threadParentGroupId, threadShortId } from "./thread-target.ts";
import { WorktreeMergeExecutionError, worktrunkUnavailableText } from "../workspace/worktrunk.ts";

export { WorktreeMergeExecutionError };

export interface ThreadMergeHost {
  readonly botUid: string;
  readonly store: Pick<
    ChannelStore,
    | "getThreadWorkspace"
    | "getBinding"
    | "isThreadWorkspaceIntentBlocked"
    | "saveThreadWorkspace"
  >;
  readonly worktrees: WorktreePort | undefined;
  readonly sourceId?: string;
  /** Host-tool merges require OMP's stable toolCallId so a retry can replay a persisted outcome. */
  readonly requireStableSourceId?: boolean;
  readonly worktreeMergeLifecycle?: (
    target: ChatTarget,
    cwd: string,
    sourceId?: string,
  ) => WorktreeMergeLifecycle;
  readonly planThreadArchive: (
    target: ChatTarget,
    groupId: string,
    shortId: string,
    generation: number,
  ) => void;
  /** Generation captured by the command turn; an intervening inbound prevents stale archival. */
  readonly archiveGeneration: () => number;
  /**
   * Optional caller preflight after qualification, ownership checks and directory lease, before
   * the merge port runs: host tools persist listen provenance here. Merge suppression belongs to
   * WorktreeMergeLifecycle.beforeSpawn, after the port has verified the worktree identity.
   */
  readonly beforeMerge?: () => void;
  readonly tryAcquireDirectories: (cwds: readonly string[]) => DirectoryLease | undefined;
}

/**
 * The host owns one merge attempt from qualification through result persistence and archive
 * scheduling. Source identity is derived only from durable workspace ownership; legacy workspaces
 * without an ownership row retain the WorktreePort's linked-worktree validation.
 */
export async function mergeThreadWorkspace(
  host: ThreadMergeHost,
  target: ChatTarget,
  options: Omit<WorktreeMergeRequest, "cwd" | "repo" | "branch" | "lifecycle">,
  signal?: AbortSignal,
): Promise<WorktreeMergeSummary & { archive?: "scheduled" }> {
  const archiveGeneration = host.archiveGeneration();
  if (signal !== undefined) throwIfAborted(signal);
  if (target.channelType !== 5) throw new Error("thread-merge 只能在子区中调用");
  const groupId = threadParentGroupId(target.channelId);
  const shortId = threadShortId(target.channelId);
  if (groupId === null || shortId === null) {
    throw new Error("thread-merge 只能在子区中调用");
  }

  const binding = host.store.getBinding(host.botUid, target);
  const hasStableSourceId = typeof host.sourceId === "string" && host.sourceId.length > 0;
  const lifecycle = hasStableSourceId
    ? host.worktreeMergeLifecycle?.(target, binding?.cwd ?? "", host.sourceId)
    : undefined;
  const replayOutcome = lifecycle?.existingOutcome;
  if (replayOutcome !== undefined) {
    if (replayOutcome.kind === "execution-failed") {
      throw new WorktreeMergeExecutionError(replayOutcome.exitCode, replayOutcome.diagnostic);
    }
    const ownership = host.store.getThreadWorkspace(host.botUid, target);
    if (replayOutcome.summary.removed && binding && ownership !== null) {
      const projectIndex = ownership.projects.findIndex(
        (project) =>
          project.created &&
          project.path === binding.cwd &&
          project.branch === replayOutcome.summary.branch,
      );
      if (projectIndex >= 0) {
        host.store.saveThreadWorkspace({
          ...ownership,
          projects: ownership.projects.map((project, index) =>
            index === projectIndex ? { ...project, created: false, path: null, branch: null } : project,
          ),
        });
      }
    }
    if (replayOutcome.summary.removed) {
      host.planThreadArchive(target, groupId, shortId, archiveGeneration);
      return { ...replayOutcome.summary, archive: "scheduled" };
    }
    return replayOutcome.summary;
  }

  const ownership = host.store.getThreadWorkspace(host.botUid, target);
  if (ownership?.layout === "shared") {
    throw new Error("当前为多项目工作区，不支持 /thread merge。");
  }
  if (host.store.isThreadWorkspaceIntentBlocked(host.botUid, target)) {
    throw new Error("当前子区工作区未就绪，无法合并");
  }
  if (ownership !== null && !(await threadWorkspaceIsAvailable(ownership))) {
    throw new Error("当前子区工作区未就绪，无法合并");
  }
  if (!binding || binding.cwdPinned !== true) {
    throw new Error("当前子区未绑定隔离工作区，无法合并");
  }
  const worktrees = host.worktrees;
  if (!worktrees) throw new Error("本部署未配置隔离工作区能力");
  const worktrunk = await worktrees.worktrunk(signal);
  if (!worktrunk.available) throw new Error(worktrunkUnavailableText());

  let ownedProjectIndex = -1;
  if (ownership !== null) {
    ownedProjectIndex = ownership.projects.findIndex(
      (project) => project.created && project.path === binding.cwd && project.branch !== null,
    );
    if (ownedProjectIndex < 0) {
      throw new Error("当前子区隔离工作区与持久化所有权不匹配，无法合并");
    }
  }

  if (host.requireStableSourceId === true && !hasStableSourceId) {
    throw new Error("thread-merge 缺少可用于安全重试的 toolCallId，拒绝执行");
  }

  const lease = host.tryAcquireDirectories([binding.cwd]);
  if (!lease) throw new Error("隔离工作区正被其他任务使用，无法合并");
  try {
    await lease.ready;
    if (signal !== undefined) throwIfAborted(signal);
    const request: WorktreeMergeRequest = { ...options, cwd: binding.cwd };
    if (ownership !== null) {
      const project = ownership.projects[ownedProjectIndex];
      if (!project || project.path !== binding.cwd || project.branch === null) {
        throw new Error("当前子区隔离工作区与持久化所有权不匹配，无法合并");
      }
      request.repo = project.repo;
      request.branch = project.branch;
    }
    let confirmedExit = false;
    let outcomePersisted = false;
    if (lifecycle) {
      request.lifecycle = {
        ...lifecycle,
        onConfirmedExit: () => {
          lifecycle.onConfirmedExit();
          confirmedExit = true;
        },
      };
    }
    host.beforeMerge?.();
    try {
      const summary = await worktrees.merge(request, signal);
      const outcome: WorktreeMergeOutcome = { kind: "succeeded", summary };
      if (lifecycle) {
        lifecycle.complete(outcome);
        outcomePersisted = true;
      }
      if (summary.removed && ownership !== null) {
        const updated: ThreadWorkspaceRecord = {
          ...ownership,
          projects: ownership.projects.map((project, index) =>
            index === ownedProjectIndex
              ? { ...project, created: false, path: null, branch: null }
              : project,
          ),
        };
        host.store.saveThreadWorkspace(updated);
      }
      if (summary.removed) {
        host.planThreadArchive(target, groupId, shortId, archiveGeneration);
      }
      return summary.removed ? { ...summary, archive: "scheduled" } : summary;
    } catch (error) {
      if (error instanceof WorktreeMergeExecutionError && confirmedExit) {
        if (lifecycle) {
          try {
            lifecycle.complete({
              kind: "execution-failed",
              exitCode: error.exitCode,
              diagnostic: error.diagnostic,
            });
          } catch (persistError) {
            try {
              lifecycle.onUnknown();
            } catch {
              // The unknown transition fails closed in memory before its store write.
            }
            throw persistError;
          }
        }
      } else if (!outcomePersisted) {
        lifecycle?.onUnknown();
      }
      throw error;
    }
  } finally {
    lease.complete();
  }
}
