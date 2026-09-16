import { randomUUID } from "node:crypto";
import { copyFile, mkdir, realpath, rm, stat } from "node:fs/promises";
import { basename, isAbsolute, join } from "node:path";
import { throwIfAborted } from "./abort.ts";
import { normalizeGroupContext } from "./context-sidecar.ts";
import type {
  ChannelServiceOptions,
  ChatTarget,
  GroupContext,
  GroupMember,
  HostTool,
  InboundMessage,
  ThreadSummary,
  WorktreeMergeRequest,
} from "./contracts.ts";
import type { ChannelStore } from "./store.ts";
import type { TaskRecord } from "./store-records.ts";
import { taskTarget } from "./target.ts";
import { threadChannelId, threadParentGroupId } from "./thread-target.ts";
import { worktreeSlug } from "./worktree.ts";

/**
 * Host tools 需要的那部分 channel 状态。工具由服务在任务内组装，依赖以接口传入：工具模块
 * 因此不持有服务实例，也不需要知道任务调度、会话迁移或出站投递是怎么实现的。
 */
export interface HostToolHost {
  readonly options: ChannelServiceOptions;
  readonly store: ChannelStore;
  /** 任务必然属于已注册的 Bot，构造工具时身份一定存在。 */
  readonly botUid: string;
  enqueueText(target: ChatTarget, text: string, taskId: string | null): void;
  flushTarget(target: ChatTarget): Promise<void>;
  /** 把一条合成消息交付到它的聊天（thread-dispatch 用）：按该聊天的追加模式并入运行中回合，否则接受为该聊天自己的任务。 */
  deliverToChat(message: InboundMessage): Promise<"steered" | "queued">;
  /** `ask`：提问并等待答案。 */
  askQuestions(
    task: TaskRecord,
    target: ChatTarget,
    args: Record<string, unknown>,
    signal: AbortSignal,
  ): Promise<string>;
  channelStatus(): {
    pid: number;
    restart_pending: boolean;
    running_targets: string[];
    queued_tasks: number;
    blocked_cwds: number;
  };
  /** `channel_control` 的 restart：没有配置重启命令时返回不可用文案。 */
  scheduleRestart(target: ChatTarget): string;
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

/**
 * Transient server failures a retry can clear: any 5xx, plus the internal `store_failed`
 * error the bot API reports as HTTP 400 while the body itself declares an internal 500.
 * The fields are read structurally, like `isPermanentOutboxError`, so the fake transport
 * can reproduce the shape without the real transport module.
 */
function isTransientManagementError(error: unknown): boolean {
  if (!error || typeof error !== "object") return false;
  const candidate = error as { code?: unknown; status?: unknown };
  if (typeof candidate.status === "number" && candidate.status >= 500) return true;
  return candidate.code === "err.server.bot_api.store_failed";
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

export function createHostTools(host: HostToolHost, task: TaskRecord, _snapshot: GroupContext | null): HostTool[] {
  const target = taskTarget(task);
  const fileTool: HostTool = {
    name: "octo_send_file",
    description: "发送当前 cwd 下的图片或文件；目标由当前聊天绑定。",
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
      const spoolDirectory = join(host.options.config.stateDir, "spool", randomUUID());
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
    execute: async (args, signal) => host.askQuestions(task, target, args, signal),
  };
  const channelTool = createChannelTool(host, target);
  const parentGroupId =
    target.channelType === 2
      ? target.channelId
      : target.channelType === 5
        ? threadParentGroupId(target.channelId)
        : null;
  if (parentGroupId === null) return [channelTool, fileTool, askTool];
  const currentShortId =
    target.channelType === 5 ? target.channelId.slice(parentGroupId.length + 4) : null;
  // A task that was itself dispatched must not dispatch further: depth stays one.
  const dispatchable = !task.messageId.startsWith(DISPATCH_MESSAGE_PREFIX);
  const actions = dispatchable
    ? [...MANAGEMENT_BASE_ACTIONS, MANAGEMENT_DISPATCH_ACTION]
    : [...MANAGEMENT_BASE_ACTIONS];
  const managementTool: HostTool = {
    name: "octo_management",
    description:
      "管理当前群/线程：读群信息与成员、改群名/群公告（需 bot 是群管理员）、读写共享说明、列出/创建/查询/加入/退出/删除 thread；create-thread 可带 worktree_name 为线程建立独立 git worktree 并把线程目录绑定到它（本机有兼容 Worktrunk 时经 wt 创建，否则经 OMP）；thread-delete 默认清理干净的工作区，force=true 时连同未提交改动一起强制清理（本机无兼容 wt 时回退 git）；thread-merge 把当前子区隔离工作区的分支经本机 Worktrunk（wt）的 merge pipeline 合并回目标分支，仅绑定隔离工作区的子区可用，省略参数即 wt 默认 pipeline；thread-dispatch 把一条任务派发给本群的某个 thread，结果按该子区的追加模式处置——steer 且该子区有可接收 steering 的运行中回合时并入该回合（delivery=steered），否则在该子区作为独立任务执行（delivery=queued），两种情况本群都收到一条回执；派发出去的任务不能再派发。目标固定为当前聊天，不能指向其他群。",
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
          description:
            "create-thread：给出名称即为新线程建立独立 git worktree（建在调用方当前目录所属仓库旁，目录 <仓库名>.<名称>，如 omp-channel-octo.fix-login），并把线程目录绑定过去；名称需包含 ASCII 字母或数字（如 fix-login）。成功返回 worktree 路径与分支，失败返回 worktree_error。省略则线程按默认目录继承。",
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
function createChannelTool(host: HostToolHost, target: ChatTarget): HostTool {
  return {
    name: "channel_control",
    description:
      "控制当前 channel（omp-channel-octo）自身：restart 排定一次重启，在本回合结束后、channel 没有运行中或排队任务时执行部署配置的重启命令，重启完成后会在本会话通报 PID；status 返回 channel 的进程 PID、运行中的会话、排队任务数、被阻塞目录数与是否已排定重启。本机装有 octo-cli 时，宿主已为本 bot 注入凭据（OCTO_CONFIG_DIR/OCTO_API_BASE_URL/OCTO_BOT_ID），可直接用 octo-cli 调用未被工具包装的 Octo API；先用 octo-cli auth status 确认身份是本 bot，不符或报错就不要使用。",
    parameters: {
      type: "object",
      properties: {
        action: { type: "string", enum: ["restart", "status"], description: "restart | status" },
      },
      required: ["action"],
      additionalProperties: false,
    },
    execute: async (args) => runChannelTool(host, target, args),
  };
}

async function runChannelTool(
  host: HostToolHost,
  target: ChatTarget,
  args: Record<string, unknown>,
): Promise<string> {
  const action = args.action;
  if (action === "status") return JSON.stringify(host.channelStatus());
  if (action === "restart") return host.scheduleRestart(target);
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
  const transport = host.options.transport;
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
      const workspaceName = worktreeName();
      const thread = await transport.createThread(groupId, name, signal);
      if (workspaceName === undefined) return JSON.stringify(managementThread(thread));
      return JSON.stringify({
        ...managementThread(thread),
        ...(await createThreadWorkspace(host, task.cwd, thread, workspaceName, signal)),
      });
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
      const note = await cleanupThreadWorkspace(host, groupId, shortId, force, signal);
      return note === null ? `已删除线程 ${shortId}` : `已删除线程 ${shortId}；${note}`;
    }
    case "thread-merge": {
      if (currentShortId === null) {
        throw new Error("thread-merge 只能在子区中调用");
      }
      const workspace = host.store.getBinding(host.botUid, {
        channelId: threadChannelId(groupId, currentShortId),
        channelType: 5,
      });
      if (!workspace || workspace.cwdPinned !== true) {
        throw new Error("当前子区未绑定隔离工作区，无法合并");
      }
      const worktrees = host.options.worktrees;
      if (!worktrees) throw new Error("本部署未配置隔离工作区能力");
      try {
        const summary = await worktrees.merge(mergeRequest(workspace.cwd), signal);
        return JSON.stringify({ merged: true, ...summary });
      } catch (error) {
        throw new Error(boundedManagementError(error, "合并隔离工作区失败"));
      }
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
  const transport = host.options.transport;
  try {
    await transport.deleteThread(groupId, shortId, signal);
    return;
  } catch (error) {
    if (signal?.aborted === true || !isTransientManagementError(error)) {
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
 * The `thread-delete` linkage: once the server has deleted the thread, its isolated workspace is
 * removed through wt, or through plain git when no compatible wt exists. Only a `cwd_pinned`
 * binding is touched; a workspace with uncommitted changes stays in place unless `force` discards
 * them. The result is one bounded single-line note appended to the delete receipt — a cleanup
 * failure never turns the (already effective) delete into a failure.
 */
async function cleanupThreadWorkspace(
  host: HostToolHost,
  groupId: string,
  shortId: string,
  force: boolean,
  signal?: AbortSignal,
): Promise<string | null> {
  const worktrees = host.options.worktrees;
  if (!worktrees) return null;
  const binding = host.store.getBinding(host.botUid, {
    channelId: threadChannelId(groupId, shortId),
    channelType: 5,
  });
  if (!binding || binding.cwdPinned !== true) return null;
  try {
    const outcome = await worktrees.remove({ path: binding.cwd, force }, signal);
    if (outcome.kind === "missing") return "隔离工作区已不存在";
    if (outcome.branchOutcome === "deleted") return "隔离工作区已清理（分支已删除）";
    return "隔离工作区已清理";
  } catch (error) {
    const prefix = force ? "隔离工作区清理失败：" : "隔离工作区未清理：";
    const suffix = force ? "" : "（可传 force=true 强制删除）";
    const room = MAX_MANAGEMENT_ERROR_CHARS - prefix.length - suffix.length;
    const reason = truncateForTool(
      boundedManagementError(error, "无法清理隔离工作区"),
      room,
    );
    return `${prefix}${reason}${suffix}`;
  }
}
/**
 * Gives a freshly created thread its own git worktree and pins the thread binding to it, so
 * tasks dispatched into that thread never share the parent group's directory. The workspace is
 * reported next to the created thread; a workspace that cannot be created becomes
 * `worktree_error` instead of a thrown rejection, because the thread itself already exists and
 * keeps the ordinary directory order.
 */
async function createThreadWorkspace(
  host: HostToolHost,
  baseDir: string,
  thread: ThreadSummary,
  workspaceName: string,
  signal?: AbortSignal,
): Promise<{ worktree: { path: string; branch: string } } | { worktree_error: string }> {
  const worktrees = host.options.worktrees;
  if (!worktrees) return { worktree_error: "本部署未配置隔离工作区能力" };
  try {
    const workspace = await worktrees.create(
      { baseDir, label: workspaceName, threadId: thread.shortId },
      signal,
    );
    host.store.ensureBinding({
      botUid: host.botUid,
      channelId: thread.channelId,
      channelType: 5,
      sessionId: randomUUID(),
      cwd: workspace.cwd,
      sessionFile: null,
      cwdPinned: true,
    });
    return { worktree: { path: workspace.cwd, branch: workspace.branch } };
  } catch (error) {
    return { worktree_error: boundedManagementError(error, "隔离工作区创建失败") };
  }
}
/**
 * Hands one task to a thread of the current group: the thread shows the handoff, and the content
 * is delivered the way the target thread handles anything arriving while its run is in flight -
 * its own append mode (a steerable running turn takes it as steering, otherwise it becomes the
 * thread's next durable task). A dispatch is a hand-over rather than the owner's reply, so it is
 * never taken as the answer to an open question, and it keeps the synthetic `dispatch:` id with
 * the owner as sender, so dedup and the depth guard hold. The caller reports which delivery
 * happened. Depth is one: the dispatch tool is withheld from tasks that arrived this way.
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
 * The `thread-dispatch` tool: resolves the target thread, hands the task over, and reports the
 * delivery in the calling chat.
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
  const thread = await host.options.transport.getThread(groupId, shortId, signal);
  if (thread.groupId.length > 0 && thread.groupId !== groupId) {
    throw new Error(`thread ${shortId} does not belong to the current group`);
  }
  const { messageId, delivery } = await dispatchTaskToThread(host, thread, text);
  host.enqueueText(
    target,
    delivery === "steered"
      ? `已把任务并入线程 ${shortId}（${thread.channelId}）的运行中回合。`
      : `已在子区 ${shortId}（${thread.channelId}）作为独立任务执行。`,
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
