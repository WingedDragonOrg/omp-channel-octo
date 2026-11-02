import { randomUUID } from "node:crypto";
import { copyFile, mkdir, realpath, rm, rmdir, stat } from "node:fs/promises";
import { basename, isAbsolute, join } from "node:path";
import { throwIfAborted } from "./abort.ts";
import { normalizeGroupContext } from "./session/context-sidecar.ts";
import type {
  ChannelTransport,
  ChatTarget,
  GroupMember,
  HostTool,
  InboundMessage,
  TaskRelayTarget,
  ThreadSummary,
  WorktreeMergeRequest,
  WorktreePort,
} from "./contracts.ts";
import { isTransientServerFailure } from "./errors.ts";
import type { ChannelStore } from "./store/index.ts";
import type { TaskRecord } from "./store/store-records.ts";
import type { ThreadWorkspaceRecord } from "./store/store-records.ts";
import type { DirectoryLease } from "./tasks/task-scheduler.ts";
import { threadWorkspaceIsAvailable } from "./session/session-lifecycle.ts";
import {
  createThreadWorkspaceBatch,
  validateThreadCreation,
  type ThreadWorkspaceBatchResult,
} from "./workspace/thread-workspaces.ts";
import { taskTarget } from "./target.ts";
import { threadChannelId, threadParentGroupId, threadShortId } from "./threads/thread-target.ts";
import { worktreeSlug } from "./workspace/worktree.ts";
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
  >;
  tryAcquireWorkspaceDirectories(cwds: readonly string[], task: TaskRecord): DirectoryLease | undefined;
  /** 任务必然属于已注册的 Bot，构造工具时身份一定存在。 */
  readonly botUid: string;
  enqueueText(target: ChatTarget, text: string, taskId: string | null): void;
  flushTarget(target: ChatTarget): Promise<void>;
  /**
   * `thread-archive`（以及默认 pipeline 的 `thread-merge` 收尾）：把一次子区归档排定到该聊天
   * 本回合输出排空之后执行——任何发进子区的消息都会把归档子区解档，所以工具内不即时归档。
   */
  planThreadArchive(target: ChatTarget, groupId: string, shortId: string): void;
  /** 把一条合成消息交付到它的聊天（thread-dispatch 用）：按该聊天的追加模式并入运行中回合，否则接受为该聊天自己的任务；relay 只在任务真正落盘时随任务持久化。 */
  deliverToChat(message: InboundMessage, relay?: TaskRelayTarget | null): Promise<"steered" | "queued">;
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

function threadWorkspaceCreationResult(batch: ThreadWorkspaceBatchResult): Record<string, unknown> {
  if (batch.layout === "separate") {
    return {
      layout: batch.layout,
      ready: batch.ready,
      threads: batch.threads.map((outcome) => ({
        ...managementThread(outcome.thread),
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
  const managementTool: HostTool = {
    name: "octo_management",
    description:
      "管理当前群/线程：读群信息与成员、改群名/群公告（需 bot 是群管理员）、读写共享说明、列出/创建/查询/加入/退出/删除 thread；create-thread 可用 projects 声明一个或多个真实 Git 仓库，单项目默认 single，多项目必须指定 shared 或 separate；shared 将多个项目放在一个非仓库入口下，separate 为每个项目创建独立线程；background 只写入用途、项目名、已创建路径和 separate 相关 short_id；worktree_name 保留旧式单项目 worktree 命名，也可与 projects 组合为命名 seed。thread-delete 按持久化所有权清理工作区，默认保留脏工作区，force=true 才强制清理；thread-merge 将当前单项目隔离工作区合并回目标分支（shared 多项目工作区不支持）；thread-archive 归档一个子区（幂等，short_id 省略即当前子区），与合并收尾一样排定在本回合最后一条回复送达后执行——任何发进子区的消息都会把归档子区解档，所以工具内不即时归档；thread-dispatch 把一条任务派发给本群的某个 thread，结果按该子区的追加模式处置——steer 且该子区有可接收 steering 的运行中回合时并入该回合（delivery=steered），否则在该子区作为独立任务执行（delivery=queued），两种情况本群都收到一条回执；可选 notify=true 让这条派发任务在结束（完成/失败/取消/中断）时向本聊天回传一条有界结果通知（默认 false，自行判断是否需要）；派发出去的任务不能再派发。目标固定为当前聊天，不能指向其他群。",
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
          description: "thread-merge：目标分支；省略使用仓库默认分支。",
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
          description: "create-thread：旧式单项目 worktree 路径名称；与 projects 组合时作为分支命名 seed，单项目直接使用，多项目按项目 slug 和序号区分。",
        },
        projects: {
          type: "array",
          description: "create-thread：每项指定一个项目名、绝对 Git 仓库路径和可选起始 commit/ref；多项目必须显式提供 layout。",
          items: {
            type: "object",
            properties: {
              name: { type: "string", description: "项目显示名。" },
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
        notify: {
          type: "boolean",
          description:
            "thread-dispatch：为 true 时这条派发的任务结束（完成/失败/取消/中断）时向本聊天回传一条有界结果通知，由本聊天的助手作为新输入处理（可并入运行中回合，否则成为该聊天的一个任务）；默认 false。派发后你不再盯这份活、又需要知道它结束时的结果（含失败）时才开启；派发被并入运行中回合时不生效（工具结果里 notify=skipped 且收据会说明）。",
        },
      },
      required: ["action"],
      additionalProperties: false,
    },
    execute: async (args, signal) =>
      runManagementTool(host, task, target, parentGroupId, currentShortId, args, signal),
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
  const mergeRequest = (cwd: string): WorktreeMergeRequest => {
    const request: WorktreeMergeRequest = { cwd };
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
        `worktree_name must be at most ${MAX_MANAGEMENT_WORKTREE_NAME_CHARS} characters`,
      );
    }
    if (worktreeSlug(value).length === 0) {
      throw new Error("worktree_name must contain ASCII letters or digits (e.g. fix-login)");
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
              background_error: boundedManagementError(error, "线程说明写入失败"),
            });
          }
        }
        return JSON.stringify(managementThread(thread));
      }
      const result = await createThreadWorkspaceBatch(
        host,
        groupId,
        name,
        creation.workspace,
        signal,
      );
      return JSON.stringify(threadWorkspaceCreationResult(result));
    }
    case "thread-info": {
      return JSON.stringify(managementThread(await transport.getThread(groupId, threadId(), signal)));
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
      const ownership = host.store.getThreadWorkspace(host.botUid, workspaceTarget);
      if (ownership?.layout === "shared") {
        throw new Error("当前为多项目工作区，不支持 /thread merge。");
      }
      if (host.store.isThreadWorkspaceIntentBlocked(host.botUid, workspaceTarget)) {
        throw new Error("当前子区工作区未就绪，无法合并");
      }
      if (ownership !== null && !(await threadWorkspaceIsAvailable(ownership))) {
        throw new Error("当前子区工作区未就绪，无法合并");
      }
      const workspace = host.store.getBinding(host.botUid, workspaceTarget);
      if (!workspace || workspace.cwdPinned !== true) {
        throw new Error("当前子区未绑定隔离工作区，无法合并");
      }
      const worktrees = host.worktrees;
      if (!worktrees) throw new Error("本部署未配置隔离工作区能力");
      const lease = host.tryAcquireWorkspaceDirectories([workspace.cwd], task);
      if (!lease) throw new Error("隔离工作区正被其他任务使用，无法合并");
      try {
        await lease.ready;
        const summary = await worktrees.merge(mergeRequest(workspace.cwd), signal);
        if (summary.removed === true && ownership !== null) {
          const project = ownership.projects.find(({ path }) => path === workspace.cwd);
          if (project !== undefined) {
            project.created = false;
            project.path = null;
            project.branch = null;
            host.store.saveThreadWorkspace(ownership);
          }
        }
        let archive: "scheduled" | null = null;
        if (summary.removed === true) {
          host.planThreadArchive(target, groupId, currentShortId);
          archive = "scheduled";
        }
        return JSON.stringify({ merged: true, ...summary, ...(archive === null ? {} : { archive }) });
      } catch (error) {
        throw new Error(boundedManagementError(error, "合并隔离工作区失败"));
      } finally {
        lease.complete();
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
      const notify = optionalBoolean("notify") ?? false;
      return dispatchThreadTask(host, task, target, groupId, shortId, text, notify, signal);
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
  const lease = host.tryAcquireWorkspaceDirectories([binding.cwd], task);
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

  const ownedPaths = ownership.projects
    .filter((project) => project.created && project.path !== null)
    .map((project) => project.path!);
  if (ownership.entryPath !== null) ownedPaths.push(ownership.entryPath);
  if (ownedPaths.length === 0) {
    const unresolved = ownership.projects.some(
      (project) => project.created || project.path !== null || project.branch !== null,
    );
    return unresolved ? "工作区所有权不完整，规划资源已保留" : null;
  }

  const lease = host.tryAcquireWorkspaceDirectories(ownedPaths, task);
  if (!lease) return "工作区正被其他任务使用，资源已保留";
  const notes: string[] = [];
  const worktrees = host.worktrees;
  try {
    await lease.ready;
    for (const project of ownership.projects) {
      if (!project.created) {
        if (project.path !== null) notes.push(`${project.name} 的创建状态未确认，路径已保留`);
        continue;
      }
      const projectPath = project.path;
      const projectBranch = project.branch;
      if (projectPath === null || projectBranch === null) {
        notes.push(`${project.name} 缺少路径或分支所有权，未自动清理`);
        continue;
      }
      if (!worktrees) {
        notes.push(`${project.name} 工作区能力不可用，资源已保留`);
        continue;
      }
      try {
        const outcome = await worktrees.remove(
          {
            path: projectPath,
            force,
            repo: project.repo,
            branch: projectBranch,
          },
          signal,
        );
        const prior = { created: project.created, path: project.path, branch: project.branch };
        project.created = false;
        project.path = null;
        project.branch = null;
        try {
          host.store.saveThreadWorkspace(ownership);
        } catch (error) {
          Object.assign(project, prior);
          notes.push(`${project.name} 已移除但所有权更新失败：${boundedManagementError(error, "存储失败")}`);
          continue;
        }
        if (outcome.kind === "missing") {
          notes.push(`${project.name} 工作区已不存在`);
        } else if (outcome.branchOutcome === "deleted") {
          notes.push(`${project.name} 已清理（分支已删除）`);
        } else {
          notes.push(`${project.name} 已清理`);
        }
      } catch (error) {
        notes.push(cleanupFailureMessage(error, force));
      }
    }

    const entryPath = ownership.entryPath;
    if (
      ownership.layout === "shared" &&
      entryPath !== null &&
      ownership.projects.every((project) => project.path === null && project.branch === null && !project.created)
    ) {
      try {
        await rmdir(entryPath);
        ownership.entryPath = null;
        try {
          host.store.saveThreadWorkspace(ownership);
          notes.push("共享入口已清理");
        } catch (error) {
          ownership.entryPath = entryPath;
          notes.push(`共享入口已移除但所有权更新失败：${boundedManagementError(error, "存储失败")}`);
        }
      } catch (error) {
        const code = (error as NodeJS.ErrnoException).code;
        if (code === "ENOENT") {
          ownership.entryPath = null;
          try {
            host.store.saveThreadWorkspace(ownership);
          } catch (saveError) {
            ownership.entryPath = entryPath;
            notes.push(`共享入口状态更新失败：${boundedManagementError(saveError, "存储失败")}`);
          }
        } else if (code === "ENOTEMPTY" || code === "EEXIST") {
          notes.push("共享入口仍含有文件，已保留");
        } else {
          notes.push(`共享入口未清理：${boundedManagementError(error, "清理失败")}`);
        }
      }
    }
  } catch (error) {
    notes.push(`工作区清理失败：${boundedManagementError(error, "清理失败")}`);
  } finally {
    lease.complete();
  }
  return notes.length === 0 ? null : truncateForTool(notes.join("；"), MAX_MANAGEMENT_ERROR_CHARS);
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
 * Hands one task to a thread of the current group: the thread shows the handoff, and the content
 * is delivered the way the target thread handles anything arriving while its run is in flight -
 * its own append mode (a steerable running turn takes it as steering, otherwise it becomes the
 * thread's next durable task). A dispatch is a hand-over rather than the owner's reply, so it is
 * never taken as the answer to an open question, and it keeps the synthetic `dispatch:` id with
 * the owner as sender, so dedup and the depth guard hold. `relay` rides only on the durable-task
 * path: a steered dispatch has no task row to carry it, so it reports no completion. The caller
 * reports which delivery happened. Depth is one: the dispatch tool is withheld from tasks that
 * arrived this way.
 */
async function dispatchTaskToThread(
  host: HostToolHost,
  thread: ThreadSummary,
  text: string,
  relay: TaskRelayTarget | null,
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
  }, relay);
  return { messageId, delivery };
}
/**
 * The `thread-dispatch` tool: resolves the target thread, hands the task over, and reports the
 * delivery in the calling chat. `notify` is the dispatching assistant's own choice: when set,
 * the task carries a completion relay back to the calling chat, which only takes effect if the
 * hand-off becomes a task of its own.
 */
async function dispatchThreadTask(
  host: HostToolHost,
  task: TaskRecord,
  target: ChatTarget,
  groupId: string,
  shortId: string,
  text: string,
  notify: boolean,
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
  const relay: TaskRelayTarget | null = notify && target.channelType !== 1
    ? { channelType: target.channelType, channelId: target.channelId, threadName: thread.name }
    : null;
  const { messageId, delivery } = await dispatchTaskToThread(host, thread, text, relay);
  host.enqueueText(
    target,
    delivery === "steered"
      ? relay === null
        ? `已把任务并入线程 ${shortId}（${thread.channelId}）的运行中回合。`
        : `已把任务并入线程 ${shortId}（${thread.channelId}）的运行中回合；并入的这部分不会单独回传结果。`
      : relay === null
        ? `已在子区 ${shortId}（${thread.channelId}）作为独立任务执行。`
        : `已在子区 ${shortId}（${thread.channelId}）作为独立任务执行；结束时将把结果回传到这里。`,
    null,
  );
  return JSON.stringify({
    dispatched: true,
    delivery,
    short_id: shortId,
    channel_id: thread.channelId,
    message_id: messageId,
    task_id: task.id,
    ...(relay === null
      ? {}
      : delivery === "queued"
        ? { notify: "registered" }
        : { notify: "skipped", notify_note: "内容并入运行中回合，不会单独回传结果" }),
  });
}
