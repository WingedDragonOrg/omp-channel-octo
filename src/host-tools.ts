import { randomUUID } from "node:crypto";
import { copyFile, mkdir, realpath, rm, stat } from "node:fs/promises";
import { basename, isAbsolute, join } from "node:path";
import { throwIfAborted } from "./abort.ts";
import { normalizeGroupContext } from "./session/context-sidecar.ts";
import type {
  ChannelTransport,
  ChatTarget,
  GroupMember,
  HostTool,
  InboundMessage,
  ThreadListenLevel,
  ThreadSummary,
  WorktreeMergeLifecycle,
  WorktreeMergeRequest,
  WorktreeMergeSummary,
  WorktreePort,
} from "./contracts.ts";
import { isTransientServerFailure } from "./errors.ts";
import type { ChannelStore } from "./store/index.ts";
import type { TaskRecord } from "./store/store-records.ts";
import type { ThreadWorkspaceRecord } from "./store/store-records.ts";
import type { DestructiveResourceLease } from "./tasks/task-scheduler.ts";
import { threadWorkspaceIsAvailable } from "./session/session-lifecycle.ts";
import {
  createThreadWorkspaceBatch,
  validateThreadCreation,
  type ThreadWorkspaceBatchResult,
} from "./workspace/thread-workspaces.ts";
import { taskTarget } from "./target.ts";
import { threadChannelId, threadParentGroupId, threadShortId } from "./threads/thread-target.ts";
import { worktreeSlug } from "./workspace/worktree.ts";
import {
  mergeThreadWorkspace,
  WorktreeMergeExecutionError,
} from "./threads/thread-merge.ts";
import {
  cleanupOwnedThreadWorkspace as cleanupOwnedWorkspace,
  threadWorkspaceCleanupPaths,
} from "./threads/thread-workspace-cleanup.ts";
/**
 * Host tools need only the channel capabilities they actually use. The service passes this port
 * while keeping task scheduling, session migration and outbound delivery internals private.
 */
export interface HostToolHost {
  readonly stateDir: string;
  readonly transport: Pick<
    ChannelTransport,
    | "getGroupContext"
    | "updateGroupContext"
    | "getGroupInfo"
    | "getGroupMembers"
    | "updateGroupInfo"
    | "listThreads"
    | "createThread"
    | "getThread"
    | "getThreadMembers"
    | "joinThread"
    | "leaveThread"
    | "deleteThread"
  >;
  readonly worktrees: WorktreePort | undefined;
  readonly worktreeMergeLifecycle?: (
    target: ChatTarget,
    cwd: string,
    sourceId: string | undefined,
    resourceCwds: readonly string[],
  ) => WorktreeMergeLifecycle;
  readonly store: Pick<
    ChannelStore,
    | "enqueueOutbox"
    | "getBinding"
    | "ensureBinding"
    | "getOwnerId"
    | "getThreadWorkspace"
    | "saveThreadWorkspace"
    | "saveThreadWorkspaces"
    | "saveThreadWorkspaceIntent"
    | "addThreadWorkspaceIntentChannel"
    | "completeThreadWorkspaceIntent"
    | "failThreadWorkspaceIntent"
    | "isThreadWorkspaceIntentBlocked"
    | "workspaceDirectories"
    | "ensureThreadListen"
    | "getThreadListen"
    | "setThreadListen"
    | "suppressTaskRelay"
    | "listThreadWorkspaces"
  >;
  tryAcquireDestructiveResources(
    cwds: readonly string[],
    task: TaskRecord,
  ): Promise<DestructiveResourceLease | undefined>;
  /** 任务必然属于已注册的 Bot，构造工具时身份一定存在。 */
  readonly botUid: string;
  enqueueText(target: ChatTarget, text: string, taskId: string | null): void;
  flushTarget(target: ChatTarget): Promise<void>;
  /** Current generation; a new inbound invalidates an archive planned by an older merge. */
  threadArchiveGeneration(target: ChatTarget): number;
  /**
   * `thread-archive`（以及默认 pipeline 的 `thread-merge` 收尾）：把一次子区归档排定到该聊天
   * 本回合输出排空之后执行——任何发进子区的消息都会把归档子区解档，所以工具内不即时归档。
   */
  planThreadArchive(target: ChatTarget, groupId: string, shortId: string, generation?: number): void;
  publishThreadMergeSuccess(
    target: ChatTarget,
    summary: WorktreeMergeSummary,
    archive: "scheduled" | "not-scheduled",
    operationId: string,
  ): Promise<void>;
  replayThreadMergeSuccess(target: ChatTarget, operationId: string): Promise<string | null>;
  /** Delivers a synthetic task through the destination chat's ordinary steer-or-task path. */
  deliverToChat(message: InboundMessage): Promise<"steered" | "queued">;
  /**
   * `ask`：提问并等待答案。`toolCallId` 是 OMP 给这次工具调用的身份（retry 重执行同一个调用时
   * 复用），提问行据此在跨重启恢复时认回原问题。
   */
  askQuestions(
    task: TaskRecord,
    target: ChatTarget,
    args: Record<string, unknown>,
    signal: AbortSignal,
    toolCallId?: string,
  ): Promise<string>;
  channelStatus(): {
    pid: number;
    restart_pending: boolean;
    running_targets: string[];
    queued_tasks: number;
    blocked_cwds: number;
  };
  /** `channel_control` 的空闲重启；宿主重启端口不可用时返回说明。 */
  scheduleRestart(): string;
}

const MANAGEMENT_BASE_ACTIONS = [
  "group-info",
  "group-members",
  "group-update",
  "description-read",
  "description-write",
  "list-threads",
  "create-thread",
  "thread-info",
  "thread-members",
  "thread-listen",
  "thread-join",
  "thread-leave",
  "thread-delete",
  "thread-merge",
  "thread-archive",
] as const;
const MANAGEMENT_DISPATCH_ACTION = "thread-dispatch";
/** Synthetic inbound id prefix: a dispatched task must not dispatch further. */
const DISPATCH_MESSAGE_PREFIX = "dispatch:";
const MAX_MANAGEMENT_MEMBERS = 200;
const MAX_MANAGEMENT_THREADS = 50;
const MAX_MANAGEMENT_NAME_CHARS = 200;
const MAX_MANAGEMENT_NOTICE_CHARS = 2_000;
const MAX_MANAGEMENT_TASK_CHARS = 4_000;
const MAX_MANAGEMENT_ERROR_CHARS = 200;
const MAX_MANAGEMENT_WORKTREE_NAME_CHARS = 100;

function truncateForTool(value: string, maxChars: number): string {
  return value.length > maxChars ? `${value.slice(0, maxChars)}…` : value;
}

/** Single-line, bounded error text for management results. */
function boundedManagementError(error: unknown, fallback: string): string {
  const detail = (error instanceof Error ? error.message : String(error)).replace(/\s+/gu, " ").trim();
  return (detail.length > 0 ? detail : fallback).slice(0, MAX_MANAGEMENT_ERROR_CHARS);
}

function managementMembers(members: readonly GroupMember[]): {
  members: Array<{ uid: string; name: string; role: number; robot: boolean }>;
  count: number;
  truncated: boolean;
} {
  const visible = members.slice(0, MAX_MANAGEMENT_MEMBERS);
  return {
    members: visible.map((member) => ({
      uid: member.uid,
      name: truncateForTool(member.name, MAX_MANAGEMENT_NAME_CHARS),
      role: member.role,
      robot: member.robot,
    })),
    count: members.length,
    truncated: members.length > visible.length,
  };
}

function managementThread(thread: ThreadSummary): {
  short_id: string;
  channel_id: string;
  group_id: string;
  name: string;
  creator_uid: string;
  status: number;
  member_count: number;
  message_count: number;
  created_at: string;
  updated_at: string;
} {
  return {
    short_id: thread.shortId,
    channel_id: thread.channelId,
    group_id: thread.groupId,
    name: truncateForTool(thread.name, MAX_MANAGEMENT_NAME_CHARS),
    creator_uid: thread.creatorUid,
    status: thread.status,
    member_count: thread.memberCount,
    message_count: thread.messageCount,
    created_at: thread.createdAt,
    updated_at: thread.updatedAt,
  };
}
function isThreadListenLevel(value: string): value is ThreadListenLevel {
  return value === "off" || value === "merge" || value === "task";
}

function ensureThreadListen(
  host: HostToolHost,
  thread: ThreadSummary,
  creator: ChatTarget = { channelType: 2, channelId: thread.groupId },
  listen: ThreadListenLevel = "merge",
) {
  if (creator.channelType !== 2 && creator.channelType !== 5) {
    throw new Error("线程监听来源必须是群或子区");
  }
  return host.store.ensureThreadListen({
    botUid: host.botUid,
    channelId: thread.channelId,
    creatorChannelType: creator.channelType,
    creatorChannelId: creator.channelId,
    threadName: thread.name,
    listen,
  });
}

function ensureThreadListenForTarget(host: HostToolHost, target: ChatTarget) {
  if (target.channelType !== 5) throw new Error("thread listen target must be a thread");
  const existing = host.store.getThreadListen(host.botUid, target);
  if (existing !== null) return existing;
  const creatorChannelId = threadParentGroupId(target.channelId);
  if (creatorChannelId === null) throw new Error("无效的子区目标");
  return host.store.ensureThreadListen({
    botUid: host.botUid,
    channelId: target.channelId,
    creatorChannelType: 2,
    creatorChannelId,
    threadName: "",
    listen: "merge",
  });
}
function workspaceProjectResults(outcome: ThreadWorkspaceBatchResult["threads"][number]) {
  return outcome.record.projects.map((project, index) => ({
    name: project.name,
    repo: project.repo,
    ...(project.base === undefined ? {} : { base: project.base }),
    path: project.path,
    branch: project.branch,
    created: project.created,
    ...(outcome.projectErrors[index] === undefined ? {} : { error: outcome.projectErrors[index] }),
  }));
}

function threadWorkspaceCreationResult(
  batch: ThreadWorkspaceBatchResult,
  listen: ThreadListenLevel,
): Record<string, unknown> {
  if (batch.layout === "separate") {
    return {
      layout: batch.layout,
      ready: batch.ready,
      threads: batch.threads.map((outcome) => ({
        ...managementThread(outcome.thread),
        listen,
        ready: outcome.record.status === "ready",
        cwd: outcome.record.projects[0]?.created ? outcome.record.projects[0].path : null,
        projects: workspaceProjectResults(outcome),
        ...(outcome.error === undefined ? {} : { error: outcome.error }),
      })),
      ...(batch.error === undefined ? {} : { error: batch.error }),
      ...(batch.remoteFailures.length === 0 ? {} : { failed_projects: batch.remoteFailures }),
    };
  }

  const outcome = batch.threads[0];
  if (outcome === undefined) {
    return {
      layout: batch.layout,
      ready: false,
      listen,
      cwd: null,
      projects: [],
      ...(batch.error === undefined ? {} : { error: batch.error }),
      ...(batch.remoteFailures.length === 0 ? {} : { failed_projects: batch.remoteFailures }),
    };
  }
  const project = outcome.record.projects[0];
  const result: Record<string, unknown> = {
    ...managementThread(outcome.thread),
    layout: batch.layout,
    listen,
    ready: outcome.record.status === "ready",
    cwd: batch.layout === "shared"
      ? outcome.record.entryPath
      : project?.created ? project.path : null,
    projects: workspaceProjectResults(outcome),
    ...(outcome.error === undefined ? {} : { error: outcome.error }),
    ...(batch.error === undefined ? {} : { batch_error: batch.error }),
  };
  if (batch.legacyWorktreeName !== undefined) {
    if (project?.created && project.path !== null && project.branch !== null) {
      result.worktree = { path: project.path, branch: project.branch };
    } else {
      result.worktree_error = outcome.projectErrors[0] ?? outcome.error ?? batch.error ?? "隔离工作区创建失败";
    }
  }
  return result;
}

export function createHostTools(host: HostToolHost, task: TaskRecord): HostTool[] {
  const target = taskTarget(task);
  const fileTool: HostTool = {
    name: "octo_send_file",
    description: "发送文件：相对路径按当前 cwd 解析，绝对路径可引用本地其他文件；目标由当前聊天绑定。",
    parameters: {
      type: "object",
      properties: {
        path: { type: "string" },
        kind: { enum: ["image", "file"] },
      },
      required: ["path", "kind"],
      additionalProperties: false,
    },
    execute: async (args, signal) => {
      if (signal.aborted) throw new Error("host tool cancelled");
      if (typeof args.path !== "string" || (args.kind !== "image" && args.kind !== "file")) {
        throw new Error("path and kind are required");
      }
      const requested = isAbsolute(args.path) ? args.path : join(task.cwd, args.path);
      const canonical = await realpath(requested);
      const details = await stat(canonical);
      if (!details.isFile()) throw new Error("file path is not a regular file");
      const spoolDirectory = join(host.stateDir, "spool", randomUUID());
      await mkdir(spoolDirectory, { recursive: true });
      const spoolPath = join(spoolDirectory, basename(canonical));
      try {
        await copyFile(canonical, spoolPath);
        if (signal.aborted) throw new Error("host tool cancelled");
        const clientMsgNo = randomUUID();
        host.store.enqueueOutbox({
          id: randomUUID(),
          botUid: host.botUid,
          channelId: target.channelId,
          channelType: target.channelType,
          taskId: task.id,
          kind: "file",
          text: null,
          path: spoolPath,
          fileKind: args.kind,
          clientMsgNo,
        });
        host.flushTarget(target);
        return `文件已加入发送队列 ${clientMsgNo}`;
      } catch (error) {
        await rm(spoolDirectory, { recursive: true, force: true }).catch(() => {});
        throw error;
      }
    },
  };
  const askTool: HostTool = {
    name: "ask",
    description: "向当前聊天提问并等待用户回答；多个问题会在一张卡片上整体提交，也可直接回复文本一次作为整组答案。等待超时的提问会保持待答并停住当前回合：用户之后的答复会作为新消息送达，据此继续即可，不要自行换个方式重复提问。",
    parameters: {
      type: "object",
      properties: {
        questions: {
          type: "array",
          items: {
            type: "object",
            properties: {
              id: { type: "string", description: "调用方可选的稳定问题标识。" },
              question: { type: "string", description: "问题正文。" },
              options: {
                type: "array",
                items: {
                  type: "object",
                  properties: {
                    id: { type: "string" },
                    label: { type: "string" },
                    description: { type: "string" },
                  },
                  required: ["label"],
                  additionalProperties: false,
                },
              },
              multi: { type: "boolean", description: "是否允许选择多个选项。" },
              recommended: { type: ["string", "number", "boolean"] },
            },
            required: ["question"],
            additionalProperties: false,
          },
        },
      },
      required: ["questions"],
      additionalProperties: false,
    },
    execute: async (args, signal, toolCallId) => host.askQuestions(task, target, args, signal, toolCallId),
  };
  const channelTool = createChannelTool(host);
  const parentGroupId =
    target.channelType === 2
      ? target.channelId
      : target.channelType === 5
        ? threadParentGroupId(target.channelId)
        : null;
  if (parentGroupId === null) return [channelTool, fileTool, askTool];
  const currentShortId =
    target.channelType === 5 ? threadShortId(target.channelId) : null;
  // A task that was itself dispatched must not dispatch further: depth stays one.
  const dispatchable = !task.messageId.startsWith(DISPATCH_MESSAGE_PREFIX);
  const actions = dispatchable
    ? [...MANAGEMENT_BASE_ACTIONS, MANAGEMENT_DISPATCH_ACTION]
    : [...MANAGEMENT_BASE_ACTIONS];
  const pendingMergeLifecycles = new Map<string, WorktreeMergeLifecycle>();
  const managementTool: HostTool = {
    name: "octo_management",
    description:
      "管理当前群/线程：读取与管理群信息、共享说明和 thread。create-thread 的显式 projects（包括 single）必须提供完整任务 worktree_name；每项 projects[].name 独立生成稳定项目目录 slug，必须是互不冲突的 ASCII 名称。名称不翻译、不截断，无法规范化或发生冲突时请改用明确限定名（如 admin-web、customer-web）。单项目省略 layout 表示 single，多项目明确选择 shared 或 separate。background 只描述用途与实际路径；listen 与派发/合并策略按各字段说明。",
    parameters: {
      type: "object",
      properties: {
        action: { type: "string", enum: actions, description: actions.join(" | ") },
        short_id: {
          type: "string",
          description:
            "线程 short id；thread-* 动作使用，在 thread 聊天里省略即当前线程。",
        },
        name: { type: "string", description: "create-thread 的线程名；group-update 的新群名。" },
        notice: { type: "string", description: "group-update 的新群公告。" },
        content: { type: "string", description: "description-write 的共享说明正文。" },
        text: { type: "string", description: "thread-dispatch 要派发的任务正文。" },
        force: {
          type: "boolean",
          description:
            "thread-delete：为 true 时丢弃工作区里的未提交改动并强制清理（默认只清理干净工作区）。",
        },
        target: {
          type: "string",
          description: "thread-merge：成功摘要的目标分支；通知按子区策略发送到创建方。",
        },
        squash: { type: "boolean", description: "thread-merge：是否 squash（默认 true）。" },
        commit: { type: "boolean", description: "thread-merge：是否提交待合并内容（默认 true）。" },
        rebase: { type: "boolean", description: "thread-merge：是否 rebase 到目标分支（默认 true）。" },
        remove: {
          type: "boolean",
          description: "thread-merge：合并成功后是否删除工作区与已合并分支（默认 true）。",
        },
        ff: { type: "boolean", description: "thread-merge：是否 fast-forward 目标分支（默认 true）。" },
        stage: {
          type: "string",
          enum: ["all", "tracked", "none"],
          description: "thread-merge：提交前的暂存范围（默认 all）。",
        },
        worktree_name: {
          type: "string",
          description: "create-thread：完整任务名，规范化为 ASCII 小写单连字符 slug（1–100 字符）；显式 projects（包括 single）必填，必须保留完整语义，不翻译、不截断，超长时提供更短但完整的名称。无 projects 时作为旧式单仓库 worktree 名称。",
        },
        projects: {
          type: "array",
          description: "create-thread：每项指定项目显示名、绝对 Git 仓库路径和可选起始 commit/ref；name 同时生成稳定项目目录 slug，规范化后必须唯一且为 1–100 字符 ASCII slug。无可用 ASCII 字符、过长或归一化冲突均在创建前拒绝；请改用明确限定项目名（如 admin-web、customer-web），不要翻译或截断。显式 projects 必须同时提供完整 worktree_name。多项目必须显式提供 layout。",
          items: {
            type: "object",
            properties: {
              name: { type: "string", description: "明确的项目名，同时作为稳定目录 slug 来源；规范化后须为唯一的 1–100 字符 ASCII slug；冲突时改用明确限定名（如 admin-web、customer-web），不翻译或截断。" },
              repo: { type: "string", description: "绝对 Git 仓库路径。" },
              base: { type: "string", description: "该项目 worktree 的起始 commit/ref。" },
            },
            required: ["name", "repo"],
            additionalProperties: false,
          },
        },
        layout: {
          type: "string",
          enum: ["shared", "separate"],
          description: "create-thread：多个项目必须指定 shared（一个线程、一个非仓库入口）或 separate（每项目一个线程）；单项目省略时为 single。",
        },
        background: {
          type: "string",
          maxLength: 500,
          description: "create-thread：简短用途说明；只写用途、项目名、实际创建路径和 separate 相关 short_id，不包含当前任务正文。",
        },
        listen: {
          type: "string",
          enum: ["off", "merge", "task"],
          description: "create-thread：默认 merge；thread-listen 省略 listen 时查询当前级别。off 不自动回传；merge 仅宿主实际合并成功后回传摘要；task 还回传派发任务的四种终态。合并/修复失败留在子区，所有通知发往创建方。",
        },
      },
      required: ["action"],
      additionalProperties: false,
    },
    execute: async (args, signal, toolCallId) =>
      runManagementTool(
        host,
        task,
        target,
        parentGroupId,
        currentShortId,
        args,
        signal,
        toolCallId,
        toolCallId === undefined
          ? undefined
          : (lifecycle) => pendingMergeLifecycles.set(toolCallId, lifecycle),
      ),
    onResultAccepted: (toolCallId) => {
      const lifecycle = pendingMergeLifecycles.get(toolCallId);
      if (!lifecycle) return;
      lifecycle.settle();
      pendingMergeLifecycles.delete(toolCallId);
    },
  };
  return [channelTool, managementTool, fileTool, askTool];
}

/**
 * Channel-level control for the agent. Available in every chat, because the restart it can
 * request is a property of the channel, not of a group.
 */
function createChannelTool(host: HostToolHost): HostTool {
  return {
    name: "channel_control",
    description:
      "控制当前 channel（omp-channel-octo）自身：restart 排定一次重启，在本回合结束后、channel 没有运行中或排队任务时执行部署配置的重启命令；status 返回 channel 的进程 PID、运行中的会话、排队任务数、被阻塞目录数与是否已排定重启。本机装有 octo-cli 时，宿主已为本 bot 注入凭据（OCTO_CONFIG_DIR/OCTO_API_BASE_URL/OCTO_BOT_ID），可直接用 octo-cli 调用未被工具包装的 Octo API；先用 octo-cli auth status 确认身份是本 bot，不符或报错就不要使用。",
    parameters: {
      type: "object",
      properties: {
        action: { type: "string", enum: ["restart", "status"], description: "restart | status" },
      },
      required: ["action"],
      additionalProperties: false,
    },
    execute: async (args) => runChannelTool(host, args),
  };
}

async function runChannelTool(
  host: HostToolHost,
  args: Record<string, unknown>,
): Promise<string> {
  const action = args.action;
  if (action === "status") return JSON.stringify(host.channelStatus());
  if (action === "restart") return host.scheduleRestart();
  throw new Error(`unknown action: ${typeof action === "string" ? action : String(action)}`);
}

async function runManagementTool(
  host: HostToolHost,
  task: TaskRecord,
  target: ChatTarget,
  groupId: string,
  currentShortId: string | null,
  args: Record<string, unknown>,
  signal: AbortSignal,
  toolCallId?: string,
  onWorktreeMergeLifecycle?: (lifecycle: WorktreeMergeLifecycle) => void,
): Promise<string> {
  const action = args.action;
  const known: readonly string[] = [...MANAGEMENT_BASE_ACTIONS, MANAGEMENT_DISPATCH_ACTION];
  if (typeof action !== "string" || !known.includes(action)) {
    throw new Error(`unknown action: ${typeof action === "string" ? action : String(action)}`);
  }
  const optionalText = (field: string): string | undefined => {
    const value = args[field];
    if (value === undefined) return undefined;
    if (typeof value !== "string") throw new Error(`${field} must be a string`);
    return value;
  };
  const optionalBoolean = (field: string): boolean | undefined => {
    const value = args[field];
    if (value === undefined) return undefined;
    if (typeof value !== "boolean") throw new Error(`${field} must be a boolean`);
    return value;
  };
  /**
   * `thread-merge` parameters, one-to-one with wt's merge flags; every omitted field keeps wt's
   * default (squash, commit, rebase, fast-forward, remove the worktree, stage everything).
   */
  const mergeRequest = (): Omit<WorktreeMergeRequest, "cwd" | "repo" | "branch" | "lifecycle"> => {
    const request: Omit<WorktreeMergeRequest, "cwd" | "repo" | "branch" | "lifecycle"> = {};
    const target = optionalText("target")?.trim();
    if (target !== undefined && target.length > 0) {
      if (target.length > MAX_MANAGEMENT_NAME_CHARS) {
        throw new Error(`target must be at most ${MAX_MANAGEMENT_NAME_CHARS} characters`);
      }
      request.target = target;
    }
    const stage = optionalText("stage")?.trim();
    if (stage !== undefined && stage.length > 0) {
      if (stage !== "all" && stage !== "tracked" && stage !== "none") {
        throw new Error("stage must be all, tracked or none");
      }
      request.stage = stage;
    }
    for (const field of ["squash", "commit", "rebase", "remove", "ff"] as const) {
      const value = optionalBoolean(field);
      if (value !== undefined) request[field] = value;
    }
    return request;
  };
  const namedText = (field: string, maxChars: number): string => {
    const value = optionalText(field)?.trim();
    if (value === undefined || value.length === 0) throw new Error(`${field} is required`);
    if (value.length > maxChars) throw new Error(`${field} must be at most ${maxChars} characters`);
    return value;
  };
  const worktreeName = (): string | undefined => {
    const value = optionalText("worktree_name")?.trim();
    if (value === undefined || value.length === 0) return undefined;
    if (value.length > MAX_MANAGEMENT_WORKTREE_NAME_CHARS) {
      throw new Error(
        `worktree_name must be at most ${MAX_MANAGEMENT_WORKTREE_NAME_CHARS} characters; provide a shorter complete semantic name`,
      );
    }
    if (worktreeSlug(value).length === 0) {
      throw new Error("worktree_name must contain ASCII letters or digits");
    }
    return value;
  };
  const threadId = (): string => {
    const requested = optionalText("short_id")?.trim();
    const resolved = requested !== undefined && requested.length > 0 ? requested : currentShortId;
    if (resolved === null || resolved === undefined || resolved.length === 0) {
      throw new Error("short_id is required when the chat itself is not a thread");
    }
    return resolved;
  };
  const transport = host.transport;
  throwIfAborted(signal);
  switch (action) {
    case "description-read": {
      return JSON.stringify(normalizeGroupContext(await transport.getGroupContext(target)));
    }
    case "description-write": {
      const content = optionalText("content");
      if (content === undefined) throw new Error("content is required");
      const result = await transport.updateGroupContext(target, content, signal);
      return `群说明已更新到版本 ${result.version}`;
    }
    case "group-info": {
      const info = await transport.getGroupInfo(groupId, signal);
      return JSON.stringify({
        group_id: info.groupId,
        name: truncateForTool(info.name, MAX_MANAGEMENT_NAME_CHARS),
        notice: truncateForTool(info.notice, MAX_MANAGEMENT_NOTICE_CHARS),
        creator: info.creator,
        status: info.status,
        created_at: info.createdAt,
      });
    }
    case "group-members": {
      return JSON.stringify(
        managementMembers(await transport.getGroupMembers(groupId, signal)),
      );
    }
    case "group-update": {
      const name = optionalText("name");
      const notice = optionalText("notice");
      if (name === undefined && notice === undefined) {
        throw new Error("name or notice is required");
      }
      const patch: { name?: string; notice?: string } = {};
      if (name !== undefined) patch.name = namedText("name", MAX_MANAGEMENT_NAME_CHARS);
      if (notice !== undefined) {
        if (notice.length > MAX_MANAGEMENT_NOTICE_CHARS) {
          throw new Error(`notice must be at most ${MAX_MANAGEMENT_NOTICE_CHARS} characters`);
        }
        patch.notice = notice;
      }
      await transport.updateGroupInfo(groupId, patch, signal);
      return `群信息已更新：${Object.keys(patch).join(", ")}`;
    }
    case "list-threads": {
      const threads = await transport.listThreads(groupId, signal);
      const visible = threads.slice(0, MAX_MANAGEMENT_THREADS);
      return JSON.stringify({
        threads: visible.map(managementThread),
        count: threads.length,
        truncated: threads.length > visible.length,
      });
    }
    case "create-thread": {
      const name = namedText("name", MAX_MANAGEMENT_NAME_CHARS);
      const creation = await validateThreadCreation(args, task.cwd, worktreeName(), signal);
      if (creation.workspace === null) {
        const thread = await transport.createThread(groupId, name, signal);
        const policy = ensureThreadListen(host, thread, target, creation.listen);
        if (creation.background !== undefined) {
          try {
            await transport.updateGroupContext(
              { channelId: thread.channelId, channelType: 5 },
              `用途：${creation.background}`,
              signal,
            );
          } catch (error) {
            return JSON.stringify({
              ...managementThread(thread),
              listen: policy.listen,
              background_error: boundedManagementError(error, "线程说明写入失败"),
            });
          }
        }
        return JSON.stringify({ ...managementThread(thread), listen: policy.listen });
      }
      const result = await createThreadWorkspaceBatch(
        host,
        groupId,
        name,
        creation.workspace,
        signal,
        (thread) => ensureThreadListen(host, thread, target, creation.listen),
      );
      return JSON.stringify(threadWorkspaceCreationResult(result, creation.listen));
    }
    case "thread-info": {
      const thread = await transport.getThread(groupId, threadId(), signal);
      const policy = ensureThreadListen(host, thread);
      return JSON.stringify({ ...managementThread(thread), listen: policy.listen });
    }
    case "thread-listen": {
      const thread = await transport.getThread(groupId, threadId(), signal);
      const current = ensureThreadListen(host, thread);
      const listen = optionalText("listen");
      if (listen === undefined) {
        return JSON.stringify({ ...managementThread(thread), listen: current.listen });
      }
      if (!isThreadListenLevel(listen)) {
        throw new Error("listen must be off, merge or task");
      }
      const updated = host.store.setThreadListen(
        host.botUid,
        { channelId: thread.channelId, channelType: 5 },
        listen,
      );
      return JSON.stringify({ ...managementThread(thread), listen: updated.listen });
    }
    case "thread-members": {
      const shortId = threadId();
      return JSON.stringify(
        managementMembers(await transport.getThreadMembers(groupId, shortId, signal)),
      );
    }
    case "thread-join": {
      const shortId = threadId();
      await transport.joinThread(groupId, shortId, signal);
      return `已加入线程 ${shortId}`;
    }
    case "thread-leave": {
      const shortId = threadId();
      await transport.leaveThread(groupId, shortId, signal);
      return `已退出线程 ${shortId}`;
    }
    case "thread-delete": {
      const shortId = threadId();
      const force = optionalBoolean("force") ?? false;
      await deleteThread(host, groupId, shortId, signal);
      const cleaned = await cleanupThreadWorkspace(host, task, groupId, shortId, force, signal);
      return cleaned === null
        ? `已删除线程 ${shortId}`
        : `已删除线程 ${shortId}；${cleaned}`;
    }
    case "thread-merge": {
      if (currentShortId === null) {
        throw new Error("thread-merge 只能在子区中调用");
      }
      const workspaceTarget: ChatTarget = {
        channelId: threadChannelId(groupId, currentShortId),
        channelType: 5,
      };
      const operationId = toolCallId === undefined ? task.id : JSON.stringify([task.id, toolCallId]);
      const replay = await host.replayThreadMergeSuccess(workspaceTarget, operationId);
      if (replay !== null) return replay;
      host.store.suppressTaskRelay(task.id);

      try {
        const result = await mergeThreadWorkspace(
          {
            botUid: host.botUid,
            store: host.store,
            worktrees: host.worktrees,
            sourceId: operationId,
            requireStableSourceId: true,
            ...(host.worktreeMergeLifecycle === undefined
              ? {}
              : {
                  worktreeMergeLifecycle: (mergeTarget, cwd, sourceId, resourceCwds) => {
                    const lifecycle = host.worktreeMergeLifecycle!(
                      mergeTarget,
                      cwd,
                      sourceId,
                      resourceCwds,
                    );
                    onWorktreeMergeLifecycle?.(lifecycle);
                    return lifecycle;
                  },
                }),
            beforeMerge: () => {
              ensureThreadListenForTarget(host, workspaceTarget);
            },
            archiveGeneration: () => host.threadArchiveGeneration(workspaceTarget),
            planThreadArchive: (archiveTarget, archiveGroupId, shortId, generation) =>
              host.planThreadArchive(archiveTarget, archiveGroupId, shortId, generation),
            tryAcquireResources: (cwds) => host.tryAcquireDestructiveResources(cwds, task),
          },
          workspaceTarget,
          mergeRequest(),
          signal,
        );
        await host.publishThreadMergeSuccess(
          workspaceTarget,
          result,
          result.archive === "scheduled" ? "scheduled" : "not-scheduled",
          operationId,
        );
        return JSON.stringify({ merged: true, ...result });
      } catch (error) {
        if (error instanceof WorktreeMergeExecutionError) throw error;
        throw new Error(boundedManagementError(error, "合并隔离工作区失败"));
      }
    }
    case "thread-archive": {
      const shortId = threadId();
      host.planThreadArchive(target, groupId, shortId);
      return `已排定归档子区 ${shortId}：本回合最后一条回复送达后执行`;
    }
    case "thread-dispatch": {
      if (task.messageId.startsWith(DISPATCH_MESSAGE_PREFIX)) {
        throw new Error("dispatched tasks cannot dispatch further");
      }
      const shortId = threadId();
      const text = namedText("text", MAX_MANAGEMENT_TASK_CHARS);
      return dispatchThreadTask(host, task, target, groupId, shortId, text, signal);
    }
    default: {
      throw new Error(`unknown action: ${action}`);
    }
  }
}

/**
 * Deletes one thread, retrying once on a fresh connection when the server answers with a
 * transient storage failure (5xx, or the `store_failed` internal error the bot API wraps as
 * HTTP 400). The server's delete is idempotent, so the retry cannot delete anything twice;
 * a final failure surfaces as one bounded single-line error.
 */
async function deleteThread(
  host: HostToolHost,
  groupId: string,
  shortId: string,
  signal?: AbortSignal,
): Promise<void> {
  const transport = host.transport;
  try {
    await transport.deleteThread(groupId, shortId, signal);
    return;
  } catch (error) {
    if (signal?.aborted === true || !isTransientServerFailure(error)) {
      throw new Error(boundedManagementError(error, "删除线程失败"));
    }
  }
  try {
    await transport.deleteThread(groupId, shortId, signal, { freshConnection: true });
  } catch (error) {
    throw new Error(boundedManagementError(error, "删除线程失败"));
  }
}

/**
 * Thread deletion owns local resources independently of the current binding: `/new` and `/move`
 * may detach that binding while the worktree still belongs to the deleted thread.
 */
async function cleanupThreadWorkspace(
  host: HostToolHost,
  task: TaskRecord,
  groupId: string,
  shortId: string,
  force: boolean,
  signal?: AbortSignal,
): Promise<string | null> {
  const target: ChatTarget = { channelId: threadChannelId(groupId, shortId), channelType: 5 };
  const ownership = host.store.getThreadWorkspace(host.botUid, target);
  if (ownership !== null) {
    return cleanupOwnedThreadWorkspace(host, task, ownership, force, signal);
  }

  const binding = host.store.getBinding(host.botUid, target);
  if (!binding || binding.cwdPinned !== true || !host.worktrees) return null;
  const lease = await host.tryAcquireDestructiveResources([binding.cwd], task);
  if (!lease) return "工作区正被其他任务使用，资源已保留";
  try {
    await lease.ready;
    const outcome = await host.worktrees.remove({ path: binding.cwd, force }, signal);
    return outcome.kind === "missing"
      ? "隔离工作区已不存在"
      : outcome.branchOutcome === "deleted"
        ? "隔离工作区已清理（分支已删除）"
        : "隔离工作区已清理";
  } catch (error) {
    return cleanupFailureMessage(error, force);
  } finally {
    lease.complete();
  }
}

async function cleanupOwnedThreadWorkspace(
  host: HostToolHost,
  task: TaskRecord,
  ownership: ThreadWorkspaceRecord,
  force: boolean,
  signal?: AbortSignal,
): Promise<string | null> {
  ownership.status = "failed";
  ownership.error = "线程已删除；本地工作区清理尚未完成";
  try {
    host.store.saveThreadWorkspace(ownership);
  } catch (error) {
    return `清理前无法持久化工作区状态，资源已保留：${boundedManagementError(error, "存储失败")}`;
  }

  const ownedPaths = threadWorkspaceCleanupPaths(ownership, false);
  const lease = ownedPaths.length === 0
    ? { ready: Promise.resolve(), complete() {} }
    : await host.tryAcquireDestructiveResources(ownedPaths, task);
  if (!lease) return "工作区正被其他任务使用，资源已保留";
  const result = await cleanupOwnedWorkspace({
    ownership,
    store: host.store,
    worktrees: host.worktrees,
    force,
    deleteBranch: false,
    lease,
    ...(signal === undefined ? {} : { signal }),
  });
  return result.messages.length === 0
    ? null
    : truncateForTool(result.messages.join("；"), MAX_MANAGEMENT_ERROR_CHARS);
}

function cleanupFailureMessage(error: unknown, force: boolean): string {
  const prefix = force ? "隔离工作区清理失败：" : "隔离工作区未清理：";
  const suffix = force ? "" : "（可传 force=true 强制删除）";
  const room = MAX_MANAGEMENT_ERROR_CHARS - prefix.length - suffix.length;
  const reason = truncateForTool(
    boundedManagementError(error, "无法清理隔离工作区"),
    room,
  );
  return `${prefix}${reason}${suffix}`;
}
/**
 * Hands one task to a thread of the current group. The synthetic `dispatch:` id with the owner
 * as sender preserves dedup and the one-hop depth guard; dispatch completion policy comes from
 * the destination thread's persisted listen level.
 */
async function dispatchTaskToThread(
  host: HostToolHost,
  thread: ThreadSummary,
  text: string,
): Promise<{ messageId: string; delivery: "steered" | "queued" }> {
  const ownerId = host.store.getOwnerId(host.botUid)?.trim() ?? "";
  if (ownerId.length === 0) {
    throw new Error("owner is not enrolled; cannot dispatch a task");
  }
  const threadTarget: ChatTarget = { channelId: thread.channelId, channelType: 5 };
  host.enqueueText(threadTarget, `来自群会话的派发任务：\n\n${text}`, null);
  const messageId = `${DISPATCH_MESSAGE_PREFIX}${randomUUID()}`;
  const delivery = await host.deliverToChat({
    channelId: threadTarget.channelId,
    channelType: 5,
    messageId,
    senderId: ownerId,
    text,
    mentionedUserIds: [host.botUid],
    attachments: [],
    quoted: null,
    supported: true,
    messageSeq: 0,
    messageTimestamp: Date.now(),
  });
  return { messageId, delivery };
}
/**
 * Resolves the target thread, hands off the task, and reports delivery in the calling chat.
 * Depth is one: the dispatch tool is withheld from tasks that arrived this way.
 */
async function dispatchThreadTask(
  host: HostToolHost,
  task: TaskRecord,
  target: ChatTarget,
  groupId: string,
  shortId: string,
  text: string,
  signal: AbortSignal,
): Promise<string> {
  const threadTarget: ChatTarget = { channelId: threadChannelId(groupId, shortId), channelType: 5 };
  if (host.store.isThreadWorkspaceIntentBlocked(host.botUid, threadTarget)) {
    throw new Error("子区工作区未就绪，未派发任务。");
  }
  const ownership = host.store.getThreadWorkspace(host.botUid, threadTarget);
  if (ownership !== null) {
    const binding = host.store.getBinding(host.botUid, threadTarget);
    const workspaceAvailable = await threadWorkspaceIsAvailable(ownership);
    const legacyFallback =
      ownership.layout === "single" &&
      ownership.legacyWorktreeName !== undefined &&
      ownership.status === "ready" &&
      ownership.projects.length === 1 &&
      !workspaceAvailable;
    const bindingUsesWorkspace = binding !== null && (() => {
      const related = new Set(host.store.workspaceDirectories(binding.cwd));
      return (
        (ownership.entryPath !== null && related.has(ownership.entryPath)) ||
        ownership.projects.some(
          (project) => project.path !== null && related.has(project.path),
        )
      );
    })();
    if (
      (binding === null && !legacyFallback) ||
      (bindingUsesWorkspace && !workspaceAvailable && !legacyFallback)
    ) {
      throw new Error("子区工作区未就绪，未派发任务。");
    }
  }
  const thread = await host.transport.getThread(groupId, shortId, signal);
  if (thread.groupId.length > 0 && thread.groupId !== groupId) {
    throw new Error(`thread ${shortId} does not belong to the current group`);
  }
  ensureThreadListen(host, thread);
  const { messageId, delivery } = await dispatchTaskToThread(host, thread, text);
  host.enqueueText(
    target,
    delivery === "steered"
      ? `已把任务并入线程 ${shortId}（${thread.channelId}）的运行中回合。`
      : `已在子区 ${shortId}（${thread.channelId}）作为独立任务执行；后续回传遵循该子区的 listen 策略。`,
    null,
  );
  return JSON.stringify({
    dispatched: true,
    delivery,
    short_id: shortId,
    channel_id: thread.channelId,
    message_id: messageId,
    task_id: task.id,
  });
}
