import { constants } from "node:fs";
import { createHash, randomUUID } from "node:crypto";
import { access, copyFile, mkdir, open, readdir, realpath, rm, stat, unlink } from "node:fs/promises";
import { homedir, tmpdir } from "node:os";
import { basename, dirname, isAbsolute, join } from "node:path";
import {
  ATTACHMENT_DOWNLOAD_BUDGET_MS,
  ATTACHMENT_DOWNLOAD_TIMEOUT_MS,
  ATTACHMENT_MAX_AGE_MS,
  AttachmentError,
  MAX_MESSAGE_ATTACHMENTS,
  attachmentDirectory,
  composePromptText,
  safeAttachmentName,
  sweepAttachments,
  type AttachmentFailureReason,
  type SavedAttachment,
} from "./attachments.ts";
import type {
  BotCardAction,
  BotEvent,
  BotIdentity,
  ChannelCommandContext,
  ChannelCommandHandler,
  ChannelService,
  ChannelServiceOptions,
  ChannelStatus,
  ChatAppendMode,
  ChatExecutionState,
  ChatTarget,
  ContextInjection,
  CoreServiceConfig,
  GroupContext,
  GroupMember,
  HistoryMessage,
  HostTool,
  PromptContext,
  InboundMessage,
  OmpWorker,
  WorkerEvent,
  TextEvent,
  SteerConsumedEvent,
  ProgressCard,
  ThreadSummary,
  WorkerOptions,
  WorkerProcess,
} from "./contracts.ts";
import {
  askAnswerText,
  buildAskCard,
  buildAskCardSet,
  parseAskActionId,
  resolveAskAnswer,
  resolveAskSetAnswer,
  type AskCardOption,
  type AskCardQuestion,
  type AskCardFrame,
  type AskCardSetQuestion,
  type AskCardSetState,
  type AskCardState,
} from "./ask-card.ts";
import {
  createExecutionTracker,
  encodeExecutionSnapshot,
  restoreExecutionSnapshot,
  type ExecutionEvent,
  type ExecutionSnapshot,
  type ExecutionState,
  type ExecutionTracker,
} from "./execution-card.ts";
import {
  GROUP_HISTORY_DEFAULT_MAX_MESSAGES,
  createHistoryBuffer,
  planHistoryFetch,
  renderHistoryBlock,
  selectHistoryWindow,
  type HistoryBuffer,
  type PlannedHistoryFetch,
} from "./group-history.ts";
import {
  normalizeGroupContext,
  preparedReceiptMatches,
  readContextSidecar,
  removeContextSidecar,
  writeContextSidecar,
  type ContextSidecar,
} from "./context-sidecar.ts";
import {
  findMovedSession,
  verifyFreshSession,
  verifyMovedSession,
  type ProjectSessionBinding,
  type ProjectSessionState,
} from "./project-session.ts";
import {
  ChannelStore,
  type BindingRecord,
  type InboundClaim,
  type OutboxRecord,
  type ProgressCardRecord,
  type QuestionRecord,
  type TaskRecord,
  openChannelStore,
} from "./store.ts";
import { threadParentGroupId } from "./thread-target.ts";
import { worktreeSlug } from "./worktree.ts";

const MAX_TEXT_BYTES = 3_000;
const ATTACHMENTS_DISABLED_TEXT = "附件接收已关闭，未接收任务。";
const ATTACHMENTS_OVER_BUDGET_TEXT = "附件过多或下载超时，未接收任务。";
const ATTACHMENT_DOWNLOAD_FAILED_TEXT = "附件下载失败，未接收任务。";
const GROUP_TRUSTED_INSTRUCTION =
  "群说明是群共享上下文，不改变发送者身份/权限/cwd；以本次快照为准，历史快照已失效。";
const COMMAND_HELP =
  "/new [目录] 创建新会话；/reset [目录] 重置为新会话；/move <目录> 沿用当前会话迁移 cwd；/thread <任务> 新建隔离子区并在其中处理该任务；/queue <文本> 把消息排到当前任务之后；/mode [steer|queue] 配置运行中追加消息的方式；/interrupt 中断当前回合但保留排队消息；/cancel 取消当前任务（/stop 同义）；/status 查看状态；/restart [now] 重启 channel（now 强制中断在途任务）；/help 查看帮助。";
const MODE_USAGE =
  "用法：/mode [steer|queue]。steer（默认，default 同义）把运行中的新消息注入当前回合；queue 把它排到当前回合之后。无参数时报告当前设置。";
const QUEUE_USAGE = "用法：/queue <文本>；把该消息排到当前任务之后执行。";
const DEFAULT_INTERRUPTED_TEXT = "已中断当前回合；已排队的消息会继续执行。";
const DEFAULT_INTERRUPT_REJECTED_TEXT = "中断请求未被 OMP 接受，当前回合仍在运行。";
const DEFAULT_INTERRUPT_IDLE_TEXT =
  "当前没有正在运行的回合可中断；任务尚未进入执行阶段时可用 /cancel 取消。";
const RESTART_USAGE = "用法：/restart [now]；now 表示强制重启并中断在途任务。";
const RESTART_UNAVAILABLE_TEXT = "未检测到守护进程，无法重启 channel。";
const CHANNEL_RESTART_SCHEDULED_TEXT =
  "已排定重启：本回合结束后、channel 没有运行中或排队任务时执行守护进程的重启；重启完成后会在本会话通报 PID。";
const RESTART_NOTICE_TTL_MS = 10 * 60 * 1_000;
const BUILTIN_COMMANDS: Record<string, true> = {
  new: true,
  reset: true,
  move: true,
  thread: true,
  cancel: true,
  stop: true,
  status: true,
  help: true,
  restart: true,
  mode: true,
  queue: true,
  interrupt: true,
};
const QUESTION_TIMEOUT_MS = 10 * 60 * 1_000;
/** Card rewrite budget; a card edit never gates the answer, which is durable before it starts. */
const ASK_CARD_EDIT_TIMEOUT_MS = 5_000;
const ASK_CARD_SEND_TIMEOUT_MS = 10_000;
/** Long-poll hold and the pause between rounds that returned immediately or failed. */
const BOT_EVENT_POLL_WAIT_SECONDS = 20;
const BOT_EVENT_POLL_LIMIT = 50;
const BOT_EVENT_POLL_IDLE_MS = 1_000;
const BOT_EVENT_POLL_BACKOFF_MS = 5_000;
/** Attempts applied to one event before it is acknowledged and dropped, so the queue keeps moving. */
const BOT_EVENT_MAX_ATTEMPTS = 5;
const PROGRESS_MERGE_MS = 1_000;
const TEXT_MERGE_MS = 1_000;
const TYPING_RENEW_MS = 3_000;
const ACTIVITY_REQUEST_TIMEOUT_MS = 2_000;
/** Recovery attempts for a card left by a previous process before the row is dropped. */
const PROGRESS_CARD_RECOVERY_ATTEMPTS = 3;
/** Steered messages tracked for the consumption receipt; older entries fall out of observation. */
const MAX_PENDING_STEERS = 32;
/** Characters of the triggering message shown in a start/consumption receipt. */
const RECEIPT_EXCERPT_CHARS = 20;
const GROUP_HISTORY_FETCH_TIMEOUT_MS = 10_000;
/** How long one 免@ decision is reused before the group switch is consulted again. */
const MENTION_PREF_TTL_MS = 60_000;
/** A failed lookup keeps the stricter "must be @mentioned" answer only briefly. */
const MENTION_PREF_FAILURE_TTL_MS = 10_000;
/** Bounds the 免@ lookup on the inbound path: the handler, and RECVACK, wait for it. */
const MENTION_PREF_TIMEOUT_MS = 5_000;
/**
 * The one exception that lets an allowlist rejection speak: an owner's @Bot mention in an unlisted
 * group or thread gets a short notice carrying this exact reply text (compared after Bot-mention
 * stripping and outer whitespace trimming), and that reply enables the chat durably.
 */
const UNLISTED_CHAT_PASSPHRASE = "开通本群";
/** One notice per unlisted chat per window, so a burst of messages cannot become a burst of replies. */
const UNLISTED_CHAT_NOTICE_TTL_MS = 10 * 60 * 1_000;
const DEFAULT_CANCELLED_TEXT = "当前任务已取消；不回滚已发生的副作用。";
const DEFAULT_RESTARTING_TEXT = "channel 正在重启，本会话任务已中断；重启完成后会在本会话确认。";
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
const THREAD_USAGE =
  "用法：/thread <任务>；助手会先明确意图、确定子区名称与 worktree_name，然后新建隔离子区并把任务派发进去。";
const THREAD_GROUP_ONLY_TEXT = "/thread 需要在群聊中使用。";
const THREAD_NESTED_TEXT = "当前会话已是子区，不能再建子区。";
type RuntimeTask = {
  taskId: string;
  target: ChatTarget;
  cwd: string;
  worker?: OmpWorker;
  cancelRequested: boolean;
  cancelReplySent: boolean;
  /** True only while this task's OMP prompt is in flight, so appended messages have a target. */
  promptInFlight: boolean;
  interruptRequested: boolean;
  /** Set when the run produced events after `/interrupt`, i.e. the queued work really continued. */
  workAfterInterrupt: boolean;
  /** Steered messages not yet observed as consumed, in delivery order (OMP's queue is FIFO). */
  steerDeliveries: { text: string }[];
  stopActivity?: (abortProgress?: boolean) => void;
  stopText?: () => void;
  /** Closes the progress card of the round that just ended; a steered message starts a new round. */
  rollProgressCard?: () => void;
};

/**
 * One user round's progress card inside a run. A run serves every message of its chat, and each
 * appended message is answered in the card that appears right below it, so a round owns its own
 * card, idempotency key, and sequence.
 */
type ProgressRound = {
  /** Server idempotency key: stable across creation retries, distinct between rounds. */
  readonly clientMsgNo: string;
  readonly tracker: ExecutionTracker;
  /** True once creation of this round's card has been queued. */
  started: boolean;
  /** True once a terminal edit for this card has been queued. */
  finished: boolean;
  /** True once a later round took over; only this card's terminal edit may still run. */
  retired: boolean;
  disabled: boolean;
  /** Set after this card's first running-edit failure so a repeating error logs once. */
  failureLogged: boolean;
  messageId: string | undefined;
  seq: number;
  latest: ProgressCard | undefined;
  version: number;
  sentVersion: number;
  nextEditAt: number;
};

type QuestionWaiter = {
  taskId: string;
  resolve: (answer: string) => void;
  reject: (error: Error) => void;
};

/** One validated `ask` question before it owns a store row. */
type AskSpec = {
  /** Row id; the question identity for every later lookup. */
  id: string;
  /** Caller-provided question id, echoed back as `id`; the row id answers `questionId`. */
  sourceId: string | null;
  /** Stored payload: what the card builders, the text path and the click path read back. */
  payload: Record<string, unknown>;
};

/**
 * Group history injected into one task turn together with the ledger attempt it
 * claims; the attempt is committed once the prompt returns, cancellation included.
 */
type HistoryTurn = {
  text: string;
  lastSeq: number;
  attemptId: string;
};

type LockHandle = {
  path: string;
  release(): Promise<void>;
};
type DirectoryLease = {
  ready: Promise<void>;
  complete(): void;
};

type AppError = Error & { code?: string };

function targetKey(target: ChatTarget): string {
  return `${target.channelType}:${target.channelId}`;
}
function streamTargetKey(target: ChatTarget, streamMessageId: string): string {
  return `${targetKey(target)}:${streamMessageId}`;
}

function taskTarget(task: TaskRecord): ChatTarget {
  return { channelId: task.channelId, channelType: task.channelType };
}

function asAppError(error: unknown): AppError {
  return error instanceof Error ? error : new Error("operation failed");
}

function safeErrorMessage(_error: unknown, fallback: string): string {
  return fallback;
}

function isPermanentOutboxError(error: unknown, item: OutboxRecord): boolean {
  if (!error || typeof error !== "object") return false;
  const candidate = error as {
    code?: unknown;
    permanent?: unknown;
    status?: unknown;
  };
  if (candidate.permanent === true) return true;
  if (
    typeof candidate.status === "number" &&
    candidate.status >= 400 &&
    candidate.status < 500 &&
    ![408, 409, 425, 429].includes(candidate.status)
  ) {
    return true;
  }
  return item.kind === "file" && candidate.code === "ENOENT";
}

/**
 * A thread (channelType=5) inherits its parent group's allowlist entry, so a freshly
 * created thread needs no extra configuration; an explicit full channel ID still wins.
 */
function isAllowlistedChat(
  allowedGroupIds: readonly string[],
  message: Pick<InboundMessage, "channelId" | "channelType">,
): boolean {
  if (allowedGroupIds.includes(message.channelId)) return true;
  if (message.channelType !== 5) return false;
  const parentGroupId = threadParentGroupId(message.channelId);
  return parentGroupId !== null && allowedGroupIds.includes(parentGroupId);
}

function isGroupTarget(target: ChatTarget): boolean {
  return target.channelType !== 1;
}

function truncateForTool(value: string, maxChars: number): string {
  return value.length > maxChars ? `${value.slice(0, maxChars)}…` : value;
}

/** Single-line, bounded excerpt of the triggering message for a start/consumption receipt. */
function receiptText(prefix: string, text: string): string {
  const excerpt = text.replaceAll(/\s+/gu, " ").trim();
  if (excerpt.length === 0) return `${prefix}。`;
  const bounded = excerpt.length > RECEIPT_EXCERPT_CHARS
    ? `${excerpt.slice(0, RECEIPT_EXCERPT_CHARS)}…`
    : excerpt;
  return `${prefix} ▸ ${bounded}`;
}

/**
 * Instruction block for a `/thread` message: the command hands the intent over instead of
 * creating anything, and the assistant names the sub-thread and its workspace itself, then drives
 * `octo_management` - dispatching the work directly when the intent is clear, and a request for
 * the owner to clarify (inside the thread) when it is not.
 */
function threadIntentPrompt(intent: string): string {
  return [
    "用户希望把下面这件事放进一个新的隔离子区处理，而不是在当前会话直接执行。",
    "先理解意图，并据此确定子区名称（简洁、能说明这件事）与 worktree_name（ASCII 小写连字符，如 fix-login-flow）。",
    "然后调用 octo_management：create-thread（name=子区名称，worktree_name=你定的名字）创建隔离子区，",
    "再用 thread-dispatch（short_id=该子区）把任务派发进去；派发的任务正文按需求明确程度二选一：",
    "- 需求明确：直接写清要做什么（可整理原始意图，但不要丢信息），让子区立刻开工；",
    "- 需求不明确：写明已知的原始意图，并要求子区先用 ask 向 owner 提问澄清，明确后再执行。",
    "澄清发生在子区里：不要在当前会话执行这件事，也不要在当前会话提问。",
    "完成后简要汇报子区名称、short_id 与工作区路径。",
    "",
    "原始意图：",
    intent,
  ].join("\n");
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

function throwIfAborted(signal: AbortSignal): void {
  if (signal.aborted) throw new Error("question cancelled");
}
function normalizeCommandHandlers(
  handlers: readonly ChannelCommandHandler[] | undefined,
): ReadonlyMap<string, ChannelCommandHandler> {
  const normalized = new Map<string, ChannelCommandHandler>();
  for (const handler of handlers ?? []) {
    if (
      typeof handler !== "object" ||
      handler === null ||
      typeof handler.name !== "string" ||
      typeof handler.intercept !== "boolean" ||
      typeof handler.handle !== "function"
    ) {
      throw new Error("命令 handler 注册无效");
    }
    const name = handler.name.trim().toLowerCase();
    if (name.length === 0 || name.startsWith("/") || /\s/u.test(name)) {
      throw new Error("命令 handler 名称无效");
    }
    if (normalized.has(name)) {
      throw new Error(`命令 handler 重复注册：${name}`);
    }
    normalized.set(name, handler);
  }
  return normalized;
}

function isCommand(text: string): boolean {
  return /^\/\S+/.test(text.trim());
}

function parseCommand(text: string): { name: string; argument: string } | null {
  const match = /^\/([^\s]+)(?:\s+([\s\S]*))?$/.exec(text.trim());
  if (!match) return null;
  return { name: match[1]!.toLowerCase(), argument: match[2] ?? "" };
}

function splitUtf8(text: string, maxBytes = MAX_TEXT_BYTES): string[] {
  if (text.length === 0) return [""];
  const encoder = new TextEncoder();
  const chunks: string[] = [];
  let current = "";
  let currentBytes = 0;
  for (const character of text) {
    const bytes = encoder.encode(character).byteLength;
    if (current.length > 0 && currentBytes + bytes > maxBytes) {
      chunks.push(current);
      current = "";
      currentBytes = 0;
    }
    current += character;
    currentBytes += bytes;
  }
  if (current.length > 0) chunks.push(current);
  return chunks;
}

function statusState(task: TaskRecord | null, hasBinding: boolean): ChatExecutionState {
  if (!task) return hasBinding ? "idle" : "none";
  return task.state;
}

function lockFilePath(botUid: string): string {
  const digest = createHash("sha256").update(botUid, "utf8").digest("hex");
  return join(tmpdir(), "omp-channel-octo-locks", `${digest}.lock`);
}

async function processAlive(pid: number): Promise<boolean> {
  if (!Number.isSafeInteger(pid) || pid < 2) return false;
  try {
    process.kill(pid, 0);
    return true;
  } catch (error) {
    const appError = asAppError(error);
    return appError.code === "EPERM";
  }
}

/**
 * Live holder of this Bot's lock, or `undefined` when the lock is free. A lock file whose process
 * is gone counts as free, so a crashed instance never blocks the next one.
 */
async function botLockHolder(botUid: string): Promise<{ path: string; pid?: number } | undefined> {
  const path = lockFilePath(botUid);
  let text: string;
  try {
    text = await Bun.file(path).text();
  } catch {
    return undefined;
  }
  let parsed: { pid?: unknown; botUid?: unknown } = {};
  try {
    parsed = JSON.parse(text) as { pid?: unknown; botUid?: unknown };
  } catch {
    parsed = {};
  }
  const pid = typeof parsed.pid === "number" ? parsed.pid : undefined;
  if (parsed.botUid === botUid && pid !== undefined && !(await processAlive(pid))) return undefined;
  return { path, ...(pid === undefined ? {} : { pid }) };
}

/**
 * Refuses to continue while another live process serves this Bot, and does so before the Bot is
 * registered: a second instance would otherwise re-register the Bot and cut off the WebSocket of
 * the instance that is already serving messages.
 */
async function assertBotLockFree(botUid: string): Promise<void> {
  const holder = await botLockHolder(botUid);
  if (holder === undefined) return;
  const owner = holder.pid === undefined ? "" : `（PID ${holder.pid}）`;
  throw new Error(`同一 Bot 已有 channel 实例运行${owner}：一个 Bot 同时只服务一个实例，请先停止该实例再启动。`);
}

async function acquireBotLock(botUid: string): Promise<LockHandle> {
  const path = lockFilePath(botUid);
  await mkdir(dirname(path), { recursive: true });
  for (let attempt = 0; attempt < 2; attempt += 1) {
    try {
      const file = await open(path, "wx", 0o600);
      await file.writeFile(JSON.stringify({ pid: process.pid, botUid }), "utf8");
      await file.close();
      return {
        path,
        release: async () => {
          try {
            await unlink(path);
          } catch (error) {
            const appError = asAppError(error);
            if (appError.code !== "ENOENT") throw error;
          }
        },
      };
    } catch (error) {
      const appError = asAppError(error);
      if (appError.code !== "EEXIST" || attempt !== 0) {
        throw new Error("同一 Bot 已有 channel 实例运行");
      }
      if (await botLockHolder(botUid)) throw new Error("同一 Bot 已有 channel 实例运行");
      await unlink(path).catch(() => {});
    }
  }
  throw new Error("无法取得 Bot 独占锁");
}

async function canonicalExistingDirectory(path: string): Promise<string> {
  const canonical = await realpath(path);
  const details = await stat(canonical);
  if (!details.isDirectory()) throw new Error("路径不是目录");
  await access(canonical, constants.R_OK | constants.X_OK);
  return canonical;
}
function stripOuterQuotes(value: string): string {
  const trimmed = value.trim();
  if (trimmed.length >= 2) {
    const first = trimmed[0];
    const last = trimmed.at(-1);
    if ((first === "'" || first === '"') && last === first) {
      return trimmed.slice(1, -1);
    }
  }
  return trimmed;
}

async function resolveWorkDirectory(argument: string, config: CoreServiceConfig): Promise<string> {
  const home = config.homeDir ?? homedir();
  const raw = stripOuterQuotes(argument);
  if (raw.length === 0) {
    const defaultDirectory = join(home, "tmp");
    await mkdir(defaultDirectory, { recursive: true });
    return canonicalExistingDirectory(defaultDirectory);
  }
  if (raw === "~") {
    return canonicalExistingDirectory(home);
  }
  if (raw.startsWith("~/")) {
    return canonicalExistingDirectory(join(home, raw.slice(2)));
  }
  if (raw.startsWith("~")) {
    throw new Error("不支持其他用户的 ~name 路径");
  }
  if (!isAbsolute(raw)) {
    throw new Error("目录必须是绝对路径或 ~/ 路径");
  }
  return canonicalExistingDirectory(raw);
}
async function existingSessionFile(path: string): Promise<string | null> {
  try {
    const details = await stat(path);
    return details.isFile() ? path : null;
  } catch {
    return null;
  }
}

/** 已落盘且非空的附件可直接复用（跳过下载）；`null` 表示需要下载。 */
async function existingFileSize(path: string): Promise<number | null> {
  try {
    const details = await stat(path);
    return details.isFile() && details.size > 0 ? details.size : null;
  } catch {
    return null;
  }
}

/**
 * 失败原因到拒绝文案：单条超限给出配置的 MB 上限（整数不带小数，非整数保留一位），
 * 整条消息的预算耗尽给出超限文案，单条附件取不回来给出下载失败文案。
 */
function attachmentRefusalText(reason: AttachmentFailureReason, maxBytes: number): string {
  if (reason === "oversize") {
    const megabytes = maxBytes / (1024 * 1024);
    const limit = Number.isInteger(megabytes) ? `${megabytes}` : megabytes.toFixed(1);
    return `附件超过 ${limit} MB 上限，未接收任务。`;
  }
  if (reason === "budget") return ATTACHMENTS_OVER_BUDGET_TEXT;
  return ATTACHMENT_DOWNLOAD_FAILED_TEXT;
}

function createSessionId(): string {
  return randomUUID();
}

function contextSidecarRoot(
  config: CoreServiceConfig,
  botUid: string,
  target: ChatTarget,
): string {
  const scopeDigest = createHash("sha256")
    .update(`${botUid}\u0000${target.channelType}\u0000${target.channelId}`, "utf8")
    .digest("hex");
  return join(config.stateDir, "context-sidecars", scopeDigest);
}

function genericTaskFailure(contextFailure: boolean): string {
  return contextFailure ? "群说明读取失败，未启动任务。" : "任务执行失败；未能可靠完成。";
}

/** Cancellable pause that resolves early when the caller's signal aborts. */
function waitFor(ms: number, signal: AbortSignal): Promise<void> {
  const { promise, resolve } = Promise.withResolvers<void>();
  const onAbort = (): void => {
    clearTimeout(timer);
    resolve();
  };
  const timer = setTimeout(() => {
    signal.removeEventListener("abort", onAbort);
    resolve();
  }, ms);
  signal.addEventListener("abort", onAbort, { once: true });
  return promise;
}

/** Parsed question payload; `null` when the stored row cannot be read. */
function parseQuestionPayload(question: QuestionRecord): Record<string, unknown> | null {
  try {
    const value: unknown = JSON.parse(question.payload);
    if (value === null || typeof value !== "object" || Array.isArray(value)) return null;
    return value as Record<string, unknown>;
  } catch {
    return null;
  }
}

/** One question payload as the card understands it: text, options, multi select. */
function askQuestionFromPayload(payload: Record<string, unknown>): AskCardQuestion | null {
  const text = typeof payload.question === "string" ? payload.question.trim() : "";
  if (text.length === 0) return null;
  const raw = Array.isArray(payload.options) ? payload.options : [];
  const options: AskCardOption[] = [];
  for (const entry of raw) {
    if (!entry || typeof entry !== "object" || Array.isArray(entry)) continue;
    const option = entry as Record<string, unknown>;
    const label = typeof option.label === "string" ? option.label.trim() : "";
    if (label.length === 0) continue;
    const description = typeof option.description === "string" ? option.description.trim() : "";
    options.push(description.length > 0 ? { label, description } : { label });
  }
  return { question: text, options, multi: payload.multi === true };
}

/**
 * The 1-based position a question holds in its multi-question card, when it belongs to one. The
 * per-question input ids of a set frame are numbered by it.
 */
function questionPosition(payload: Record<string, unknown>): { index: number; size: number } | null {
  const index = payload.cardIndex;
  const size = payload.cardSize;
  if (typeof index !== "number" || !Number.isSafeInteger(index) || index < 1) return null;
  if (typeof size !== "number" || !Number.isSafeInteger(size) || size < index) return null;
  return { index, size };
}

/**
 * Result of applying one Bot event: `handled` and `ignored` are terminal (the event is
 * acknowledged), `retry` keeps the cursor so the same event is read again.
 */
type BotEventOutcome = "handled" | "ignored" | "retry";

class CoreService implements ChannelService {
  private readonly taskOperations = new Set<Promise<void>>();
  private readonly chatTails = new Map<string, Promise<void>>();
  private readonly scheduledTasks = new Map<string, Promise<void>>();
  /**
   * Tasks accepted while their chat or directory was already busy. They get one start receipt
   * when they finally begin, so a message that waited is distinguishable from one that started
   * right away. In-memory only: a restart interrupts such tasks instead of starting them.
   */
  private readonly queuedBehind = new Set<string>();
  private readonly directoryTails = new Map<string, Promise<void>>();
  private readonly runtimes = new Map<string, RuntimeTask>();
  private readonly questionWaiters = new Map<string, QuestionWaiter>();
  private readonly questionTimers = new Map<string, NodeJS.Timeout>();
  /** Card frame sequence per question card, strictly increasing as the server requires. */
  private readonly askCardSeq = new Map<string, number>();
  /** Card action-id generation per card message: a frame that must stay answerable gets fresh ids. */
  private readonly askCardGeneration = new Map<string, number>();
  private readonly botEventAttempts = new Map<number, number>();
  private eventAbort: AbortController | undefined;
  private eventLoop: Promise<void> | undefined;
  private readonly outboxFlushes = new Map<string, Promise<void>>();
  private readonly outboxActive = new Set<string>();
  private readonly retryTimers = new Map<string, NodeJS.Timeout>();
  private readonly streamTimers = new Map<string, NodeJS.Timeout>();
  private readonly streamNotBefore = new Map<string, number>();
  private readonly textAbortControllers = new Map<string, AbortController>();
  private readonly textAttempts = new Set<AbortController>();
  private readonly blockedDirectories = new Set<string>();
  private readonly groupHistoryBuffers = new Map<string, HistoryBuffer>();
  private readonly mentionPrefs = new Map<string, { effective: boolean; expiresAt: number }>();
  private readonly mentionPrefRequests = new Map<string, Promise<boolean>>();
  /** Last unlisted-chat notice per chat; process-local, so a restart may re-notify once. */
  private readonly unlistedChatNotices = new Map<string, number>();
  private readonly commandHandlers: ReadonlyMap<string, ChannelCommandHandler>;
  private bot: BotIdentity | undefined;
  private readonly activityOperations = new Set<Promise<void>>();
  private lock: LockHandle | undefined;
  private started = false;
  private stopping = false;
  private restartRequested = false;
  /**
   * A restart requested through `channel_control`, kept until every chat is idle so the turn that
   * asked for it still delivers its reply.
   */
  private pendingRestart: ChatTarget | null = null;
  private stopPromise: Promise<void> | undefined;
  constructor(
    private readonly options: ChannelServiceOptions,
    private readonly store: ChannelStore,
    commandHandlers: ReadonlyMap<string, ChannelCommandHandler>,
  ) {
    this.commandHandlers = commandHandlers;
  }

  async start(): Promise<void> {
    if (this.started) return;
    if (this.stopping) throw new Error("channel 正在停止");
    // Checked before `initialize`, because registering is what interrupts a running instance.
    const knownBotUid = this.store.getBotUid();
    if (knownBotUid !== null) await assertBotLockFree(knownBotUid);
    const identity = await this.options.transport.initialize();
    if (!identity.uid || !identity.name) throw new Error("Octo Bot 身份无效");
    const lock = await acquireBotLock(identity.uid);
    this.lock = lock;
    this.bot = identity;
    try {
      this.store.setBotUid(identity.uid);
      // The agent's own Octo identity: the per-bot octo-cli store every later worker inherits.
      // Best effort by contract - a missing CLI must not keep the channel from starting.
      await this.options.cliCredentials?.provision(identity.uid).catch((error: unknown) => {
        console.error(
          `[octo-cli] 凭据准备失败：${error instanceof Error ? error.message : String(error)}`,
        );
      });
      // 附件目录按 14 天保留期清理；best-effort，清理失败不影响启动。
      await sweepAttachments(this.options.config.stateDir, ATTACHMENT_MAX_AGE_MS).catch(() => 0);
      const configuredOwner =
        typeof this.options.config.ownerId === "string" ? this.options.config.ownerId.trim() : "";
      if (configuredOwner) {
        this.store.setOwnerId(identity.uid, configuredOwner);
      }
      const interrupted = this.store.markInterruptedOnStart(identity.uid);
      for (const task of interrupted) {
        await this.enqueueText(taskTarget(task), `服务已重启完成；任务 ${task.id} 因重启中断，请重新发送。`, null);
      }
      const restartedAt = Date.now();
      const notifiedTargets = new Set<string>();
      for (const notice of this.store.listRestartNotices(identity.uid)) {
        this.store.clearRestartNotice(notice.id);
        if (restartedAt - notice.requestedAt > RESTART_NOTICE_TTL_MS) continue;
        const key = targetKey(notice);
        if (notifiedTargets.has(key)) continue;
        notifiedTargets.add(key);
        await this.enqueueText(
          notice,
          `channel 已重启完成：PID ${process.pid}，启动于 ${new Date(restartedAt).toISOString()}。`,
          null,
        );
      }
      for (const ledger of this.store.listWorkerLedger(identity.uid)) {
        const stopped = await this.options.workerFactory.recover(ledger);
        if (stopped) {
          this.store.markWorkerStopped(ledger.id);
        } else {
          this.blockedDirectories.add(ledger.cwd);
          if (ledger.moveTargetCwd) this.blockedDirectories.add(ledger.moveTargetCwd);
        }
      }
      if (this.stopping) throw new Error("channel 正在停止");
      this.started = true;
      await this.options.transport.start(
        (message) => this.handleInbound(message),
        () => {
          this.resetHistoryCoverage();
          this.flushAllOutbox();
        },
      );
      if (this.stopping) throw new Error("channel 正在停止");
      this.flushAllOutbox();
      this.startBotEventLoop();
      await this.recoverProgressCards();
    } catch (error) {
      this.started = false;
      this.eventAbort?.abort();
      await this.eventLoop?.catch(() => {});
      this.eventAbort = undefined;
      this.eventLoop = undefined;
      await this.options.transport.stop().catch(() => {});
      await lock.release().catch(() => {});
      this.lock = undefined;
      this.bot = undefined;
      throw error;
    }
  }

  async stop(): Promise<void> {
    if (this.stopPromise) return this.stopPromise;
    this.stopPromise = this.stopInternal();
    return this.stopPromise;
  }

  private async stopInternal(): Promise<void> {
    if (this.stopping) return;
    this.stopping = true;
    this.started = false;
    const runtimes = [...this.runtimes.values()];
    for (const runtime of runtimes) {
      runtime.cancelRequested = true;
      runtime.stopText?.();
      runtime.stopActivity?.();
      if (this.bot) {
        this.store.cancelQuestions(this.bot.uid, runtime.target);
        if (this.restartRequested) {
          this.store.addRestartNotice({
            id: randomUUID(),
            botUid: this.bot.uid,
            channelType: runtime.target.channelType,
            channelId: runtime.target.channelId,
            requestedAt: Date.now(),
          });
        }
      }
      this.rejectQuestionsForTask(runtime.taskId, new Error("channel stopping"));
      if (runtime.worker) {
        await runtime.worker.stop().catch(() => false);
      }
    }
    for (const controller of this.textAbortControllers.values()) {
      controller.abort();
    }
    for (const controller of this.textAttempts) {
      controller.abort();
    }
    await Promise.allSettled([...this.taskOperations]);
    await Promise.allSettled([...this.activityOperations]);
    this.eventAbort?.abort();
    await this.eventLoop?.catch(() => {});
    this.eventAbort = undefined;
    this.eventLoop = undefined;
    for (const timer of this.questionTimers.values()) {
      clearTimeout(timer);
    }
    this.questionTimers.clear();
    for (const timer of this.retryTimers.values()) {
      clearTimeout(timer);
    }
    this.retryTimers.clear();
    for (const timer of this.streamTimers.values()) {
      clearTimeout(timer);
    }
    this.streamTimers.clear();
    this.streamNotBefore.clear();
    await Promise.allSettled([...this.outboxFlushes.values()]);
    await this.options.transport.stop().catch(() => {});
    if (this.lock) {
      await this.lock.release().catch(() => {});
      this.lock = undefined;
    }
    this.store.close();
  }

  async handleInbound(message: InboundMessage): Promise<void> {
    if (!this.started || !this.bot || this.stopping) return;
    if (message.messageId.trim().length === 0 || message.senderId.trim().length === 0) return;
    this.recordHistoryMessage(message);
    const text = message.text.trim();
    if (this.store.hasOwnerEnrollment(this.bot.uid, message, message.messageId)) return;
    let ownerId = this.store.getOwnerId(this.bot.uid)?.trim() || null;
    if (
      !ownerId &&
      message.channelType === 1 &&
      message.supported &&
      text.length > 0 &&
      message.senderId.trim() !== this.bot.uid
    ) {
      const acknowledgement = {
        id: randomUUID(),
        botUid: this.bot.uid,
        channelId: message.channelId,
        channelType: message.channelType,
        taskId: null,
        kind: "text" as const,
        text: `已绑定 Octo 用户 ${message.senderId.trim()}。接下来可直接发送任务，或使用 /new 创建会话。`,
        path: null,
        fileKind: null,
        clientMsgNo: randomUUID(),
      };
      let claimed: boolean;
      try {
        claimed = this.store.claimOwnerEnrollment(
          this.bot.uid,
          message,
          message.messageId,
          message.senderId.trim(),
          text,
          acknowledgement,
        );
      } catch {
        throw new Error("本地状态盘写入失败，未绑定 owner。");
      }
      if (claimed) {
        this.flushTarget(message);
        return;
      }
      ownerId = this.store.getOwnerId(this.bot.uid)?.trim() || null;
    }
    const allowed = await this.isAllowed(message, ownerId);
    if (!allowed) {
      await this.handleUnlistedOwnerContact(message, ownerId);
      return;
    }
    if (!message.supported) {
      const claim = this.commandClaim(message, "unsupported");
      if (!this.store.claimCommand(claim)) return;
      try {
        await this.enqueueText(message, "不支持的消息类型，不会启动任务。", null);
        this.store.completeCommand(claim);
      } catch {
        this.store.failCommand(claim);
        throw new Error("本地状态盘写入失败，未能记录不支持消息。");
      }
      return;
    }
    const command = isCommand(text) ? parseCommand(text) : null;
    if (command) {
      if (BUILTIN_COMMANDS[command.name] === true) {
        await this.handleCommand(message, command.name, command.argument);
        return;
      }
      const handler = this.commandHandlers.get(command.name);
      if (handler) {
        if (handler.intercept) {
          await this.handleInterceptingCommand(message, command, handler);
        } else {
          await this.handleObservingCommand(message, command, handler);
        }
        return;
      }
      // 未注册的 /xxx 不是命令，仍按普通任务走唯一组装点，附件照常取回。
      const slashPrompt = await this.materializeInbound(message);
      if (slashPrompt === null) return;
      await this.acceptTask(message, slashPrompt);
      return;
    }
    // A dispatched task is an explicit hand-over into this chat (see `dispatchTaskToThread`), so
    // it always becomes its own durable task: it never answers an open question and never joins a
    // running turn as steering — either diversion would leave the dispatch receipt ("已派发任务到
    // 线程 …，在那边排队执行") describing something that did not happen.
    if (message.messageId.startsWith(DISPATCH_MESSAGE_PREFIX)) {
      const dispatchPrompt = await this.materializeInbound(message);
      if (dispatchPrompt === null) return;
      await this.acceptTask(message, dispatchPrompt);
      return;
    }
    if (await this.answerPendingQuestion(message, text)) return;
    const promptText = await this.materializeInbound(message);
    if (promptText === null) return;
    if (await this.appendToRunningTask(message, promptText)) return;
    await this.acceptTask(message, promptText);
  }

  /**
   * 唯一组装点：在权限门、命令解析与待回答问题判定之后，把消息组装成任务正文
   * （用户文本 + 语音转写 + 附件清单）。附件在这里下载到本机、语音在这里转写。
   * 失败时经命令 claim + outbox 只回复一次并返回 `null`，不建任务、不进入 steering；
   * `null` 也表示该消息已被更早的处理路径（含重投去重）消费。
   *
   * `text` 默认是整条消息正文。命令路径传入自己的正文：`/thread <任务>` 与 `/queue <文本>` 传
   * 命令参数，注册 handler 传整条消息——凡是把正文交给 Agent 的命令路径都经这里组装，附件
   * （图片/文件/语音/视频）与引用块因此不会被命令吃掉。不消费正文的控制命令（`/status`、
   * `/cancel`、`/mode` 等）与待回答问题的答案不经过这里。
   */
  private async materializeInbound(message: InboundMessage, text = message.text): Promise<string | null> {
    const quotedText = message.quoted?.text.trim() ?? "";
    // 只带引用、不带附件时也要把引用块交给 Agent；普通文本任务的正文保持逐字不变。
    if (message.attachments.length === 0) {
      return quotedText.length === 0 ? text : composePromptText(text, [], message.quoted);
    }
    const attachmentConfig = this.options.config.attachments;
    if (attachmentConfig?.enabled !== true) {
      if (text.trim().length > 0 || quotedText.length > 0) {
        return composePromptText(text, [], message.quoted);
      }
      const claim = this.commandClaim(message, "attachment");
      if (!this.store.claimCommand(claim)) return null;
      await this.refuseAttachments(message, claim, ATTACHMENTS_DISABLED_TEXT);
      return null;
    }
    const claim = this.commandClaim(message, "attachment");
    if (!this.store.claimCommand(claim)) return null;
    if (message.attachments.length > MAX_MESSAGE_ATTACHMENTS) {
      await this.refuseAttachments(message, claim, ATTACHMENTS_OVER_BUDGET_TEXT);
      return null;
    }
    const directory = attachmentDirectory(this.options.config.stateDir, message, message.messageId);
    const created: string[] = [];
    const budget = new AbortController();
    // 整条消息的下载预算（含转写）：超时就中止在途下载，按超限失败处理。
    const budgetTimer = setTimeout(() => budget.abort(), ATTACHMENT_DOWNLOAD_BUDGET_MS);
    try {
      // 附件目录由服务创建（0700，与传输层的写入互为幂等），失败清理才有一个确定的所有者。
      await mkdir(directory, { recursive: true, mode: 0o700 });
      const files: SavedAttachment[] = [];
      for (const [index, attachment] of message.attachments.entries()) {
        // 客户端声明的大小先做超限预检，不发起无谓的请求。
        if (attachment.size > attachmentConfig.maxBytes) {
          throw new AttachmentError("oversize", `attachment ${index} exceeds ${attachmentConfig.maxBytes} bytes`);
        }
        const path = join(directory, safeAttachmentName(attachment.name, index, attachment.kind, attachment.url));
        const signal = AbortSignal.any([
          AbortSignal.timeout(ATTACHMENT_DOWNLOAD_TIMEOUT_MS),
          budget.signal,
        ]);
        let bytes = await existingFileSize(path);
        if (bytes === null) {
          try {
            bytes = await this.options.transport.downloadAttachment({
              url: attachment.url,
              path,
              maxBytes: attachmentConfig.maxBytes,
              signal,
            });
          } catch (error) {
            if (error instanceof AttachmentError && error.reason === "oversize") throw error;
            // 单条 60 秒超时与整条 120 秒预算都会中止同一条 signal；只有预算耗尽按「超限」回复。
            if (budget.signal.aborted) throw new AttachmentError("budget", "attachment budget exhausted");
            if (signal.aborted) throw new AttachmentError("timeout", "attachment download timed out");
            if (error instanceof AttachmentError) throw error;
            throw new AttachmentError("failed", "attachment download failed");
          }
          created.push(path);
        }
        files.push({
          path,
          kind: attachment.kind,
          name: attachment.name.trim().length > 0 ? attachment.name.trim() : basename(path),
          bytes,
          durationSeconds: attachment.durationSeconds,
          transcription:
            attachment.kind === "audio" ? await this.transcribeInboundAudio(path, budget.signal) : null,
        });
      }
      const promptText = composePromptText(text, files, message.quoted);
      this.store.releaseClaim(claim);
      return promptText;
    } catch (error) {
      // 失败清理：只删除本次新建的文件，已存在（可能被既有任务引用）的文件保留；目录空了才移除。
      for (const path of created) {
        await rm(path, { force: true }).catch(() => {});
      }
      try {
        if ((await readdir(directory)).length === 0) {
          await rm(directory, { recursive: true, force: true });
        }
      } catch {
        // 目录未落盘或不可读时无需清理。
      }
      const reason: AttachmentFailureReason = error instanceof AttachmentError ? error.reason : "failed";
      await this.refuseAttachments(message, claim, attachmentRefusalText(reason, attachmentConfig.maxBytes));
      return null;
    } finally {
      clearTimeout(budgetTimer);
    }
  }

  /**
   * 语音转写是 best-effort：普通失败、空文本甚至单条转写超时都降级为无转写，任务照常接受；
   * 只有整条消息的下载预算被耗尽才按超限失败处理。
   */
  private async transcribeInboundAudio(path: string, budgetSignal: AbortSignal): Promise<string | null> {
    const signal = AbortSignal.any([AbortSignal.timeout(ATTACHMENT_DOWNLOAD_TIMEOUT_MS), budgetSignal]);
    try {
      const transcription = await this.options.transport.transcribeAudio(path, signal);
      return transcription !== null && transcription.trim().length > 0 ? transcription : null;
    } catch {
      if (budgetSignal.aborted) {
        throw new AttachmentError("budget", "attachment budget exhausted during transcription");
      }
      return null;
    }
  }

  /** 附件拒绝：走既有命令 claim + outbox 通道，只回复一次、不写 task 行。 */
  private async refuseAttachments(message: InboundMessage, claim: InboundClaim, text: string): Promise<void> {
    try {
      await this.enqueueText(message, text, null);
      this.store.completeCommand(claim);
    } catch {
      this.store.failCommand(claim);
      throw new Error("本地状态盘写入失败，未能记录附件消息。");
    }
  }

  async status(target: ChatTarget): Promise<ChannelStatus> {
    if (!this.bot) {
      return {
        sessionId: null,
        cwd: null,
        state: "none",
        mode: "steer",
        queued: 0,
        outbox: 0,
        unknownOutbox: 0,
        pendingQuestionIds: [],
      };
    }
    const latest = this.store.getLatestTask(this.bot.uid, target);
    const active = this.store.getActiveTask(this.bot.uid, target);
    const binding = active
      ? this.store.getBinding(this.bot.uid, target)
      : await this.reconcileExternalBinding(target);
    const display = active ?? latest;
    return {
      sessionId: binding?.sessionId ?? null,
      cwd: binding?.cwd ?? null,
      state: statusState(display, Boolean(binding)),
      mode: this.store.getChatMode(this.bot.uid, target),
      queued: this.store.countQueuedTasks(this.bot.uid, target),
      outbox: this.store.countPendingOutbox(this.bot.uid, target),
      unknownOutbox: this.store.countUnknownOutbox(this.bot.uid, target),
      pendingQuestionIds: this.store.pendingQuestionIds(this.bot.uid, target),
    };
  }

  private commandClaim(message: InboundMessage, _name: string) {
    return {
      botUid: this.bot!.uid,
      target: message,
      messageId: message.messageId,
      senderId: message.senderId,
      text: message.text,
      supported: message.supported,
      kind: "command" as const,
    };
  }

  private taskClaim(message: InboundMessage) {
    return {
      botUid: this.bot!.uid,
      target: message,
      messageId: message.messageId,
      senderId: message.senderId,
      text: message.text,
      supported: message.supported,
      kind: "task" as const,
    };
  }

  /**
   * The allowlist is the config's `allowedGroupIds` plus the chats the owner enabled at runtime
   * (durable in the state store). Threads keep the same inheritance rule over both sets: a thread
   * is permitted by its own id or by its parent group's.
   */
  private isPermittedChat(message: Pick<InboundMessage, "channelId" | "channelType">): boolean {
    if (isAllowlistedChat(this.options.config.allowedGroupIds, message)) return true;
    if (!this.bot) return false;
    const granted = this.store.listAllowedChats(this.bot.uid);
    return granted.length > 0 && isAllowlistedChat(granted.map((chat) => chat.channelId), message);
  }

  private async isAllowed(message: InboundMessage, ownerId: string | null): Promise<boolean> {
    if (!this.bot || !ownerId || message.messageId.trim().length === 0 || message.senderId.trim().length === 0) {
      return false;
    }
    const senderId = message.senderId.trim();
    if (senderId === this.bot.uid) return false;
    if (message.channelType === 1) {
      return senderId === ownerId;
    }
    if (senderId !== ownerId || !this.isPermittedChat(message)) {
      return false;
    }
    if (message.mentionedUserIds.includes(this.bot.uid)) return true;
    return await this.groupAllowsNoMention(message);
  }

  /**
   * The single exception to "an allowlist rejection is silent": an owner's @Bot mention in an
   * unlisted group or thread gets one short, throttled notice with the passphrase, and the exact
   * passphrase enables that chat durably. Every other rejection path is unchanged - a non-owner,
   * an unmentioned message, a bot echo, and every DM stay reply-less, task-less and unlogged, and
   * a chat the owner already enabled never reaches here.
   */
  private async handleUnlistedOwnerContact(message: InboundMessage, ownerId: string | null): Promise<void> {
    const bot = this.bot;
    if (!bot || !ownerId) return;
    const senderId = message.senderId.trim();
    if (senderId !== ownerId || senderId === bot.uid) return;
    if (message.channelType !== 2 && message.channelType !== 5) return;
    if (!message.mentionedUserIds.includes(bot.uid)) return;
    if (this.isPermittedChat(message)) return;
    if (message.text.trim() === UNLISTED_CHAT_PASSPHRASE) {
      await this.grantUnlistedChat(message);
      return;
    }
    this.noticeUnlistedChat(message, bot.name.trim().length > 0 ? bot.name.trim() : bot.uid);
  }

  /** 本群 vs 本子区: the notice, the confirmation and the grant row all name the same scope. */
  private noticeUnlistedChat(message: InboundMessage, botName: string): void {
    const key = targetKey(message);
    const lastNoticedAt = this.unlistedChatNotices.get(key);
    if (lastNoticedAt !== undefined && Date.now() - lastNoticedAt < UNLISTED_CHAT_NOTICE_TTL_MS) return;
    const scope = message.channelType === 5 ? "本子区" : "本群";
    this.enqueueText(message, `${scope}未开通。回复「@${botName} ${UNLISTED_CHAT_PASSPHRASE}」即可开通${scope}。`, null);
    // Only a queued notice opens the window: a failed enqueue still leaves the next message free to try again.
    this.unlistedChatNotices.set(key, Date.now());
  }

  /**
   * Enables the chat the owner answered the passphrase in. The grant row is the durable permission
   * (the next message needs no restart and no config reload); the config file is then updated as
   * the operator-visible mirror, and a mirror failure is reported without taking the grant back.
   */
  private async grantUnlistedChat(message: InboundMessage): Promise<void> {
    const botUid = this.bot!.uid;
    // The passphrase is a control action, not a task: one durable claim per message id, so a
    // redelivery after the grant cannot fall through to a task that prompts the model with it.
    const claim = this.commandClaim(message, "allow-chat");
    if (!this.store.claimCommand(claim)) return;
    const scope = message.channelType === 5 ? "本子区" : "本群";
    let mirrorFailed = false;
    try {
      this.store.grantAllowedChat({
        botUid,
        channelType: message.channelType === 5 ? 5 : 2,
        channelId: message.channelId,
        grantedBy: message.senderId.trim(),
      });
      if (this.options.allowlistWriteback) {
        try {
          await this.options.allowlistWriteback.append(message.channelId);
        } catch {
          mirrorFailed = true;
        }
      }
      console.error(
        `[allow] 已开通${scope} ${targetKey(message)}（owner ${message.senderId.trim()}）${
          mirrorFailed ? "；配置文件写回失败，仅状态盘生效" : ""
        }`,
      );
      await this.enqueueText(
        message,
        mirrorFailed
          ? `已开通${scope}；配置文件写回失败，本次开通仅在状态盘生效。`
          : `已开通${scope}，后续 @${this.bot!.name} 即可正常使用。`,
        null,
      );
      this.store.completeCommand(claim);
    } catch {
      this.store.failCommand(claim);
      throw new Error("本地状态盘写入失败，未记录开通结果。");
    }
  }

  /**
   * 群级免@：服务器把 bot owner 的意图与群级开关 AND 后给出 `effective`，为真时群里不必 @Bot。
   * 结果按群缓存，查询失败一律保持「需要 @」，不因错误内容放宽权限。
   */
  private async groupAllowsNoMention(message: InboundMessage): Promise<boolean> {
    // 线程的免@开关属于父群；线程自身没有独立设置。
    const groupId =
      message.channelType === 5 ? threadParentGroupId(message.channelId) : message.channelId.trim();
    if (groupId === null || groupId.length === 0) return false;
    const cached = this.mentionPrefs.get(groupId);
    if (cached !== undefined && cached.expiresAt > Date.now()) return cached.effective;
    const inflight = this.mentionPrefRequests.get(groupId);
    if (inflight !== undefined) return await inflight;
    // 并发入站只发一次查询；同一个 promise 同时充当在途去重与结果缓存。
    const request = (async (): Promise<boolean> => {
      let effective = false;
      let ttl = MENTION_PREF_FAILURE_TTL_MS;
      try {
        const pref = await this.options.transport.getGroupMentionPref(
          groupId,
          AbortSignal.timeout(MENTION_PREF_TIMEOUT_MS),
        );
        effective = pref.effective;
        ttl = MENTION_PREF_TTL_MS;
      } catch {
        // 保留「需要 @」的既有行为，只做短暂负缓存，避免查询失败时长挂入站处理。
      }
      this.mentionPrefs.set(groupId, { effective, expiresAt: Date.now() + ttl });
      return effective;
    })();
    this.mentionPrefRequests.set(groupId, request);
    try {
      return await request;
    } finally {
      this.mentionPrefRequests.delete(groupId);
    }
  }

  private async sessionRootsFor(cwd: string): Promise<readonly string[]> {
    const stateSessions = join(this.options.config.stateDir, "sessions");
    if (!this.options.workerFactory.sessionRootsFor) return [stateSessions];
    try {
      return [stateSessions, ...(await this.options.workerFactory.sessionRootsFor(cwd))];
    } catch {
      return [stateSessions];
    }
  }

  private async reconcileExternalBinding(
    target: ChatTarget,
    binding: BindingRecord | null = this.bot ? this.store.getBinding(this.bot.uid, target) : null,
  ): Promise<BindingRecord | null> {
    if (!binding || !binding.sessionFile) return binding;
    if (this.blockedDirectories.has(binding.cwd)) return binding;
    const moved = await findMovedSession(binding, { roots: await this.sessionRootsFor(binding.cwd) });
    if (
      !moved ||
      (moved.sessionId === binding.sessionId &&
        moved.cwd === binding.cwd &&
        moved.sessionFile === binding.sessionFile)
    ) {
      return binding;
    }
    const rebound: BindingRecord = {
      ...binding,
      sessionId: moved.sessionId,
      cwd: moved.cwd,
      sessionFile: moved.sessionFile,
    };
    try {
      this.store.ensureBinding(rebound);
    } catch {
      return binding;
    }
    return rebound;
  }
  private hasRuntime(target: ChatTarget): boolean {
    const key = targetKey(target);
    for (const runtime of this.runtimes.values()) {
      if (targetKey(runtime.target) === key) return true;
    }
    return false;
  }

  /** The chat's task whose OMP prompt is currently in flight, if any. */
  private runningRuntime(target: ChatTarget): RuntimeTask | undefined {
    const key = targetKey(target);
    for (const runtime of this.runtimes.values()) {
      if (targetKey(runtime.target) === key && runtime.promptInFlight) return runtime;
    }
    return undefined;
  }

  /** Live per-chat coverage buffer; chats without one simply have no coverage history. */
  private historyBufferFor(target: ChatTarget): HistoryBuffer {
    const key = targetKey(target);
    const existing = this.groupHistoryBuffers.get(key);
    if (existing) return existing;
    const created = createHistoryBuffer(
      this.options.config.groupHistory?.maxMessages ?? GROUP_HISTORY_DEFAULT_MAX_MESSAGES,
    );
    this.groupHistoryBuffers.set(key, created);
    return created;
  }

  /**
   * Every group/thread message proves coverage before any permission gate; only text
   * from other senders becomes a renderable entry.
   */
  private recordHistoryMessage(message: InboundMessage): void {
    if (this.options.config.groupHistory?.enabled !== true) return;
    if (message.channelType !== 2 && message.channelType !== 5) return;
    const senderId = message.senderId.trim();
    this.historyBufferFor(message).record({
      seq: message.messageSeq,
      supported: message.supported,
      senderId,
      text: message.text,
      timestamp: message.messageTimestamp,
      fromBot: senderId.length > 0 && senderId === this.bot?.uid,
    });
  }

  /** A websocket connect/reconnect makes the covered seq range unknown again. */
  private resetHistoryCoverage(): void {
    for (const buffer of this.groupHistoryBuffers.values()) {
      buffer.resetCoverage();
    }
  }

  /** One bounded sync page; `null` marks a degraded turn that must not advance the watermark. */
  private async fetchHistoryPage(
    target: ChatTarget,
    plan: PlannedHistoryFetch,
  ): Promise<HistoryMessage[] | null> {
    const controller = new AbortController();
    const timer = setTimeout(() => controller.abort(), GROUP_HISTORY_FETCH_TIMEOUT_MS);
    try {
      return await this.options.transport.fetchGroupMessages(target, {
        sinceSeq: plan.sinceSeq,
        untilSeq: plan.untilSeq,
        limit: plan.limit,
        signal: controller.signal,
      });
    } catch (error) {
      const reason = error instanceof Error ? error.message : "fetch failed";
      console.error(
        `[group-history] sync failed target=${targetKey(target)} since=${plan.sinceSeq}`
        + ` until=${plan.untilSeq}: ${reason}`,
      );
      return null;
    } finally {
      clearTimeout(timer);
    }
  }

  /**
   * Builds the history block for one group/thread turn and claims the ledger attempt that
   * the post-prompt commit closes.  A range the live buffer cannot prove covered is taken
   * from sync; when that sync fails the turn keeps its plain text and the watermark stays
   * put, so the next turn replays the same range instead of silently losing it.
   */
  private async historyTurnFor(
    task: TaskRecord,
    target: ChatTarget,
    sessionId: string,
  ): Promise<HistoryTurn | null> {
    const config = this.options.config.groupHistory;
    if (config?.enabled !== true) return null;
    if (task.channelType !== 2 && task.channelType !== 5) return null;
    if (!task.historyEligible || !Number.isInteger(task.messageSeq) || task.messageSeq <= 0) return null;
    const botUid = this.bot!.uid;
    const currentSeq = task.messageSeq;
    // Session identity may have moved (`/new`, external rebinding); drop orphan rows first.
    this.store.pruneHistoryLedger(botUid, target, sessionId);
    const ledger = this.store.getHistoryLedger(botUid, target, sessionId);
    // A committed row's lastSeq is the accounted-through watermark; a pending row still
    // owes the range its previous attempt started from.
    const watermark = ledger === null
      ? null
      : ledger.phase === "pending"
        ? ledger.previousSeq
        : ledger.lastSeq;
    if (watermark !== null && watermark >= currentSeq) return null;
    const buffer = this.historyBufferFor(target);
    const candidates = buffer.entriesInRange(watermark ?? 0, currentSeq);
    let truncatedEarlier = false;
    if (!buffer.covers(watermark ?? 0, currentSeq)) {
      const plan = planHistoryFetch({
        previousSeq: watermark,
        currentSeq,
        maxMessages: config.maxMessages,
      });
      const page = await this.fetchHistoryPage(target, plan);
      if (page === null) return null;
      truncatedEarlier = plan.truncatedEarlier;
      for (const message of page) {
        if (!Number.isInteger(message.seq) || message.seq <= 0 || message.seq >= currentSeq) continue;
        candidates.push({
          seq: message.seq,
          senderId: message.senderId,
          text: message.text,
          timestamp: message.timestamp,
        });
      }
    }
    const { kept, skipped } = selectHistoryWindow({
      entries: candidates,
      maxMessages: config.maxMessages,
    });
    const block = renderHistoryBlock(kept, { skipped, truncatedEarlier });
    const prepared = this.store.prepareHistoryInjection(botUid, target, sessionId, currentSeq);
    console.error(
      `[group-history] target=${targetKey(target)} seq=${currentSeq}`
      + ` injected=${kept.length} skipped=${skipped}${truncatedEarlier ? " truncated" : ""}`,
    );
    return {
      text: block === null ? task.text : `${block}\n${task.text}`,
      lastSeq: prepared.lastSeq,
      attemptId: prepared.attemptId,
    };
  }

  /**
   * Closes the claimed attempt once the prompt returned.  A steer that landed mid-turn
   * raised the row's lastSeq under the same attempt, so the commit follows it instead of
   * reporting a CAS failure that would replay the steered message on the next turn.
   */
  private commitHistoryTurn(target: ChatTarget, sessionId: string, turn: HistoryTurn): void {
    const botUid = this.bot?.uid;
    if (botUid === undefined) return;
    try {
      const current = this.store.getHistoryLedger(botUid, target, sessionId);
      const lastSeq = current === null ? turn.lastSeq : Math.max(turn.lastSeq, current.lastSeq);
      if (this.store.commitHistoryInjection(botUid, target, sessionId, lastSeq, turn.attemptId)) return;
      console.error(`[group-history] commit rejected target=${targetKey(target)} seq=${lastSeq}`);
    } catch (error) {
      console.error(
        `[group-history] commit failed target=${targetKey(target)}:`
        + ` ${error instanceof Error ? error.message : "unknown error"}`,
      );
    }
  }

  /**
   * Persists the card frame the server accepted. A process that did not observe the run cannot
   * address the card, so this row is what lets a later process close it. Storage failures are
   * logged instead of being treated as card failures.
   */
  private rememberProgressCard(
    taskId: string,
    target: ChatTarget,
    messageId: string,
    lastSeq: number,
    snapshot: ExecutionSnapshot,
  ): void {
    const botUid = this.bot?.uid;
    if (botUid === undefined) return;
    try {
      this.store.saveProgressCard({
        botUid,
        taskId,
        channelType: target.channelType,
        channelId: target.channelId,
        messageId,
        lastSeq,
        snapshot: encodeExecutionSnapshot(snapshot),
      });
    } catch (error) {
      console.error(
        `[progress-card] 无法记录卡片状态 task=${taskId}：`
        + `${error instanceof Error ? error.message : "unknown error"}`,
      );
    }
  }

  /** Drops the persisted card row. Bookkeeping failures are logged, never fatal. */
  private forgetProgressCard(taskId: string, messageId: string): void {
    const botUid = this.bot?.uid;
    if (botUid === undefined) return;
    try {
      this.store.clearProgressCard(botUid, taskId, messageId);
    } catch (error) {
      console.error(
        `[progress-card] 无法清除卡片记录 task=${taskId}：`
        + `${error instanceof Error ? error.message : "unknown error"}`,
      );
    }
  }

  /**
   * Counts a recovery attempt, whose row also advances the stored sequence: an attempt whose
   * response was lost may already be accepted, so the next one has to stay strictly above it. A
   * failing state disk must not keep the channel from starting, so that failure is logged and
   * reported as the last attempt.
   */
  private recordProgressCardAttempt(taskId: string, messageId: string): number {
    const botUid = this.bot?.uid;
    if (botUid === undefined) return PROGRESS_CARD_RECOVERY_ATTEMPTS;
    try {
      return this.store.recordProgressCardAttempt(botUid, taskId, messageId);
    } catch (error) {
      console.error(
        `[progress-card] 无法记录补发尝试 task=${taskId}：`
        + `${error instanceof Error ? error.message : "unknown error"}`,
      );
      return PROGRESS_CARD_RECOVERY_ATTEMPTS;
    }
  }

  /**
   * Closes cards left open by an earlier process — a restart that interrupted the turn, or a
   * terminal edit that never landed. Without this the client keeps showing the last running state
   * forever, because the card's message id and sequence only existed in that process.
   */
  private async recoverProgressCards(): Promise<void> {
    const botUid = this.bot?.uid;
    if (botUid === undefined) return;
    for (const card of this.store.listProgressCards(botUid)) {
      const target: ChatTarget = { channelType: card.channelType, channelId: card.channelId };
      const task = this.store.getTask(card.taskId);
      const state: Exclude<ExecutionState, "running"> = task?.state === "completed"
        ? "completed"
        : task?.state === "failed"
          ? "failed"
          : "cancelled";
      const snapshot = restoreExecutionSnapshot(card.snapshot, { state, atMs: Date.now() });
      if (snapshot === null) {
        this.forgetProgressCard(card.taskId, card.messageId);
        console.error(`[progress-card] 丢弃无法解析的卡片记录 task=${card.taskId}`);
        continue;
      }
      try {
        await this.submitRecoveredCard(target, card, snapshot);
        this.forgetProgressCard(card.taskId, card.messageId);
        console.error(
          `[progress-card] 已补发终态卡片 task=${card.taskId} target=${targetKey(target)} state=${state}`,
        );
      } catch (error) {
        const attempts = this.recordProgressCardAttempt(card.taskId, card.messageId);
        console.error(
          `[progress-card] 补发终态失败 task=${card.taskId} attempt=${attempts}：`
          + `${error instanceof Error ? error.message : "unknown error"}`,
        );
        if (attempts >= PROGRESS_CARD_RECOVERY_ATTEMPTS) {
          this.forgetProgressCard(card.taskId, card.messageId);
        }
      }
    }
  }

  /** One recovery edit, under the same bounded deadline as a live card edit. */
  private async submitRecoveredCard(
    target: ChatTarget,
    card: ProgressCardRecord,
    snapshot: ExecutionSnapshot,
  ): Promise<void> {
    const controller = new AbortController();
    const timer = setTimeout(() => controller.abort(), ACTIVITY_REQUEST_TIMEOUT_MS);
    try {
      await this.options.transport.editProgressCard(
        target,
        card.messageId,
        snapshot,
        card.lastSeq + 1,
        controller.signal,
      );
    } finally {
      clearTimeout(timer);
    }
  }

  /** A steered message is already inside the running turn, so it is accounted for directly. */
  private recordSteeredHistory(message: InboundMessage, runtime: RuntimeTask): void {
    if (this.options.config.groupHistory?.enabled !== true) return;
    const sessionId = runtime.worker?.sessionId ?? "";
    if (sessionId.length === 0) return;
    this.store.advanceHistoryLedger(this.bot!.uid, message, sessionId, message.messageSeq);
  }

  private async deliverSteer(target: ChatTarget, text: string): Promise<boolean> {
    const runtime = this.runningRuntime(target);
    if (!runtime?.worker) return false;
    // Recorded before the request so the delivery order matches the RPC command order even when
    // two appends race; a refused frame removes its own entry.
    const delivery = { text };
    runtime.steerDeliveries.push(delivery);
    if (runtime.steerDeliveries.length > MAX_PENDING_STEERS) runtime.steerDeliveries.shift();
    try {
      await runtime.worker.steer(text);
      // The message starts a new round: its progress belongs below the message, not above it.
      runtime.rollProgressCard?.();
      return true;
    } catch {
      const index = runtime.steerDeliveries.indexOf(delivery);
      if (index >= 0) runtime.steerDeliveries.splice(index, 1);
      return false;
    }
  }

  /**
   * A steered message left OMP's queue and joined the running turn's context. The queue is FIFO
   * and this channel is its only user-attributed producer, so the consumption follows the
   * delivery order; a mismatch is worth a log line but never changes the answer already sent.
   */
  private noteSteerConsumed(runtime: RuntimeTask, event: SteerConsumedEvent): void {
    const delivered = runtime.steerDeliveries.shift();
    if (delivered === undefined) {
      console.error(`[steer] 消费事件没有对应投递 task=${runtime.taskId}`);
    } else if (delivered.text !== event.text) {
      console.error(`[steer] 消费顺序与投递不一致 task=${runtime.taskId}`);
    }
    console.error(`[steer] 已并入当前回合 task=${runtime.taskId} chars=${event.text.length}`);
    this.enqueueText(runtime.target, receiptText("已并入当前回合", event.text), null);
  }

  /**
   * In `steer` mode a message that arrives while its chat runs an OMP prompt is injected into that
   * turn instead of starting a second task.  Without a live streaming turn, or when OMP refuses
   * the frame, the text becomes an ordinary durable queued task so nothing is dropped.
   * Returns true when the message was consumed as a steering message.
   */
  private async appendToRunningTask(message: InboundMessage, text: string): Promise<boolean> {
    if (this.store.getChatMode(this.bot!.uid, message) !== "steer") return false;
    const runtime = this.runningRuntime(message);
    if (!runtime) return false;
    const claim = this.commandClaim(message, "steer");
    if (!this.store.claimCommand(claim)) return true;
    if (await this.deliverSteer(message, text)) {
      this.store.completeCommand(claim);
      this.recordSteeredHistory(message, runtime);
      return true;
    }
    this.store.releaseClaim(claim);
    return false;
  }

  private directoryUnavailable(cwd: string): boolean {
    if (this.blockedDirectories.has(cwd) || this.directoryTails.has(cwd)) return true;
    for (const runtime of this.runtimes.values()) {
      if (runtime.cwd === cwd) return true;
    }
    for (const task of this.store.listQueuedTasks()) {
      if (task.cwd === cwd) return true;
    }
    return false;
  }


  private async handleInterceptingCommand(
    message: InboundMessage,
    command: { name: string; argument: string },
    handler: ChannelCommandHandler,
  ): Promise<void> {
    // handler 拿到的是这条消息的正文，因此正文先经唯一组装点：注册命令同样不吃掉附件。
    const body = await this.materializeInbound(message);
    if (body === null) return;
    const claim = this.commandClaim(message, command.name);
    if (!this.store.claimCommand(claim)) return;
    try {
      await handler.handle({
        message: { ...message, text: body },
        name: command.name,
        argument: command.argument,
      });
      this.store.completeCommand(claim);
    } catch {
      this.store.failCommand(claim);
      throw new Error("命令未能持久完成，请重试。");
    }
  }

  private async handleObservingCommand(
    message: InboundMessage,
    command: { name: string; argument: string },
    handler: ChannelCommandHandler,
  ): Promise<void> {
    const body = await this.materializeInbound(message);
    if (body === null) return;
    const accepted = await this.acceptTask(message, body, false);
    if (!accepted) return;
    try {
      await handler.handle({
        message: { ...message, text: body },
        name: command.name,
        argument: command.argument,
      });
    } catch {
      // An observer must not affect durable task acceptance or OMP execution.
    }
  }

  private async handleCommand(message: InboundMessage, name: string, argument: string): Promise<void> {
    const claim = this.commandClaim(message, name);
    if (!this.store.claimCommand(claim)) return;
    const target: ChatTarget = message;
    try {
      let restartRequested = false;
      if (name === "new" || name === "reset") {
        await this.handleNew(target, argument);
      } else if (name === "thread") {
        await this.handleThreadCommand(message, argument);
      } else if (name === "move") {
        await this.handleMove(target, argument);
      } else if (name === "cancel" || name === "stop") {
        await this.handleCancel(target);
      } else if (name === "interrupt") {
        await this.handleInterrupt(target);
      } else if (name === "queue") {
        await this.handleQueue(message, argument);
      } else if (name === "mode") {
        await this.handleMode(target, argument);
      } else if (name === "status") {
        await this.handleStatus(target);
      } else if (name === "help") {
        await this.enqueueText(target, COMMAND_HELP, null);
      } else if (name === "restart") {
        restartRequested = await this.handleRestart(target, argument);
      }
      this.store.completeCommand(claim);
      if (restartRequested) {
        this.restartRequested = true;
        await this.options.restart?.request();
      }
    } catch {
      this.store.failCommand(claim);
      throw new Error("命令未能持久完成，请重试。");
    }
  }

  /**
   * `/thread <任务>`: hand the intent to the assistant instead of creating a sub-thread here. The
   * message carries an instruction block, so the agent reads the intent, names the sub-thread and
   * its workspace itself, and drives `octo_management`: a clear intent is dispatched as the work
   * itself, an underspecified one as a request for the owner to clarify in the thread. The body
   * follows the chat's append mode like any other message the assistant has to read: a running
   * turn that accepts steering gets the instruction inside that turn, otherwise it becomes a
   * durable task (`queue` mode included) that waits behind the run. Only groups grow threads, so
   * DMs and threads themselves are refused.
   */
  private async handleThreadCommand(message: InboundMessage, argument: string): Promise<void> {
    if (message.channelType === 5) {
      await this.enqueueText(message, THREAD_NESTED_TEXT, null);
      return;
    }
    if (message.channelType !== 2) {
      await this.enqueueText(message, THREAD_GROUP_ONLY_TEXT, null);
      return;
    }
    const intent = argument.trim();
    if (intent.length === 0) {
      await this.enqueueText(message, THREAD_USAGE, null);
      return;
    }
    // 这条命令的正文就是交给助手的任务正文：先释放命令 claim，再走与普通任务同一条组装点
    // （claim + 下载 + 转写 + 附件块），否则随消息发来的图片只剩 `[图片]` 占位。
    this.store.releaseClaim(this.commandClaim(message, "thread"));
    const request = await this.materializeInbound(message, intent);
    if (request === null) return;
    const body = threadIntentPrompt(request);
    if (await this.appendToRunningTask(message, body)) return;
    await this.acceptTask(message, body);
  }

  private async defaultWorkDirectory(target: ChatTarget): Promise<string> {
    const botUid = this.bot?.uid;
    const binding = botUid === undefined ? null : this.store.getBinding(botUid, target);
    if (binding?.sessionFile) return canonicalExistingDirectory(binding.cwd);
    if (binding?.cwdPinned === true) {
      try {
        return await canonicalExistingDirectory(binding.cwd);
      } catch {
        // An isolated workspace that no longer exists falls back to the ordinary order below.
      }
    }
    if (target.channelType === 5) {
      const parentId = threadParentGroupId(target.channelId);
      const parent = parentId === null || botUid === undefined
        ? null
        : this.store.getBinding(botUid, { channelId: parentId, channelType: 2 });
      if (parent) {
        try {
          return await canonicalExistingDirectory(parent.cwd);
        } catch {
          // The parent cwd is unusable: fall back to the default work directory.
        }
      }
    }
    return resolveWorkDirectory("", this.options.config);
  }

  private async handleNew(target: ChatTarget, argument: string): Promise<void> {
    if (this.bot && (this.store.getActiveTask(this.bot.uid, target) || this.hasRuntime(target))) {
      await this.enqueueText(target, "当前聊天有运行中或排队任务，请先 /cancel。", null);
      return;
    }
    const previous = await this.reconcileExternalBinding(target);
    let cwd: string;
    try {
      const trimmed = argument.trim();
      if (
        (trimmed.length > 0 && stripOuterQuotes(argument).length === 0) ||
        (trimmed.length === 0 && argument.length > 0)
      ) {
        throw new Error("空引号不是有效目录");
      }
      cwd = trimmed.length > 0
        ? await resolveWorkDirectory(argument, this.options.config)
        : await this.defaultWorkDirectory(target);
    } catch (error) {
      await this.enqueueText(target, `目录无效：${safeErrorMessage(error, "无法访问该目录")}`, null);
      return;
    }
    const commandLease = this.tryAcquireCommandDirectories([cwd]);
    if (!commandLease) {
      await this.enqueueText(target, "目标目录当前被任务或未确认会话占用，请稍后重试。", null);
      return;
    }
    try {
      await commandLease.ready;
      const sessionId = createSessionId();
    const sessionDir = join(this.options.config.stateDir, "sessions");
    await mkdir(sessionDir, { recursive: true });
    const ledgerId = `new-${sessionId}`;
    const ledgerSessionFile = join(sessionDir, `${sessionId}.jsonl`);
    let spawned: WorkerProcess | undefined;
    let candidate: OmpWorker | undefined;
    let stopped = false;
    let persistenceFailed = false;
    try {
      candidate = await this.options.workerFactory.create({
        cwd,
        sessionDir,
        onSpawn: (processInfo) => {
          spawned = processInfo;
          this.store.recordWorker({
            id: ledgerId,
            botUid: this.bot!.uid,
            sessionId,
            sessionFile: ledgerSessionFile,
            pid: processInfo.pid,
            pgid: processInfo.pgid,
            identity: processInfo.identity,
            cwd: processInfo.cwd,
            moveTargetCwd: null,
            state: "running",
          });
        },
      });
      const freshState: ProjectSessionState = await candidate.refreshState();
      const previousForFresh: ProjectSessionBinding = previous ?? {
        sessionId: "",
        cwd,
        sessionFile: null,
      };
      const fresh = await verifyFreshSession(previousForFresh, freshState, cwd);
      stopped = await candidate.stop();
      if (!stopped) {
        this.blockedDirectories.add(cwd);
        throw new Error("worker did not stop");
      }
      this.store.markWorkerStopped(ledgerId);
      const binding: BindingRecord = {
        botUid: this.bot!.uid,
        channelId: target.channelId,
        channelType: target.channelType,
        sessionId: fresh.sessionId,
        cwd: fresh.cwd,
        sessionFile: fresh.sessionFile,
      };
      try {
        this.store.ensureBinding(binding);
      } catch {
        persistenceFailed = true;
        throw new Error("本地状态盘写入失败，会话未切换。");
      }
      this.store.clearContextLedger(this.bot!.uid, target);
      await this.enqueueText(target, `已创建会话 ${binding.sessionId}，cwd=${binding.cwd}`, null);
    } catch {
      if (candidate && !stopped) {
        const verified = await candidate.stop().catch(() => false);
        if (verified) {
          this.store.markWorkerStopped(ledgerId);
        } else {
          this.blockedDirectories.add(cwd);
        }
      } else if (spawned && !stopped) {
        const verified = await this.options.workerFactory.recover(spawned).catch(() => false);
        if (verified) {
          this.store.markWorkerStopped(ledgerId);
        } else {
          this.blockedDirectories.add(cwd);
        }
      }
      if (persistenceFailed) throw new Error("本地状态盘写入失败，会话未切换。");
      await this.enqueueText(target, "无法创建可恢复的 OMP 会话，当前绑定未改变。", null);
    }
    } finally {
      commandLease.complete();
  }
  }

  private async handleMove(target: ChatTarget, argument: string): Promise<void> {
    if (this.bot && (this.store.getActiveTask(this.bot.uid, target) || this.hasRuntime(target))) {
      await this.enqueueText(target, "当前聊天有运行中或排队任务，请先 /cancel。", null);
      return;
    }
    const previous = await this.reconcileExternalBinding(target);
    if (!previous) {
      await this.enqueueText(target, "当前聊天没有可迁移的会话，请先 /new。", null);
      return;
    }
    let cwd: string;
    try {
      const trimmed = argument.trim();
      if (
        trimmed.length === 0 ||
        stripOuterQuotes(argument).length === 0 ||
        (trimmed.length > 0 && stripOuterQuotes(argument).length === 0)
      ) {
        throw new Error("必须指定目标目录");
      }
      cwd = await resolveWorkDirectory(argument, this.options.config);
    } catch (error) {
      await this.enqueueText(target, `目录无效：${safeErrorMessage(error, "无法访问该目录")}`, null);
      return;
    }
    if (!previous.sessionFile) {
      await this.enqueueText(target, "当前绑定没有可恢复的精确会话文件，未迁移。", null);
      return;
    }
    const sessionFile = await existingSessionFile(previous.sessionFile);
    if (!sessionFile) {
      await this.enqueueText(target, "绑定的 OMP 会话文件不存在，未迁移。", null);
      return;
    }
    const commandLease = this.tryAcquireCommandDirectories([previous.cwd, cwd]);
    if (!commandLease) {
      await this.enqueueText(target, "源目录或目标目录当前被任务或未确认会话占用，请稍后重试。", null);
      return;
    }
    try {
      await commandLease.ready;
    const sessionDir = join(this.options.config.stateDir, "sessions");
    await mkdir(sessionDir, { recursive: true });
    const ledgerId = `move-${randomUUID()}`;
    let spawned: WorkerProcess | undefined;
    let candidate: OmpWorker | undefined;
    let stopped = false;
    let persistenceFailed = false;
    try {
      candidate = await this.options.workerFactory.create({
        cwd: previous.cwd,
        sessionDir,
        sessionFile,
        onSpawn: (processInfo) => {
          spawned = processInfo;
          this.store.recordWorker({
            id: ledgerId,
            botUid: this.bot!.uid,
            sessionId: previous.sessionId,
            sessionFile,
            pid: processInfo.pid,
            pgid: processInfo.pgid,
            identity: processInfo.identity,
            cwd: processInfo.cwd,
            moveTargetCwd: null,
            state: "running",
          });
        },
      });
      this.store.markWorkerMoveTarget(ledgerId, cwd);
      await candidate.prompt(`/move ${cwd}`, () => {});
      const movedState: ProjectSessionState = await candidate.refreshState();
      const moved = await verifyMovedSession(previous, movedState, cwd);
      stopped = await candidate.stop();
      if (!stopped) {
        this.blockedDirectories.add(previous.cwd);
        this.blockedDirectories.add(cwd);
        throw new Error("worker did not stop");
      }
      this.store.markWorkerStopped(ledgerId);
      const binding: BindingRecord = {
        ...previous,
        sessionId: moved.sessionId,
        cwd: moved.cwd,
        sessionFile: moved.sessionFile,
        // An explicit /move leaves an isolated thread workspace behind.
        cwdPinned: false,
      };
      try {
        this.store.ensureBinding(binding);
      } catch {
        persistenceFailed = true;
        throw new Error("本地状态盘写入失败，会话未迁移。");
      }
      await this.enqueueText(target, `已迁移会话 ${binding.sessionId}，cwd=${binding.cwd}`, null);
    } catch {
      if (candidate && !stopped) {
        const verified = await candidate.stop().catch(() => false);
        if (verified) {
          this.store.markWorkerStopped(ledgerId);
        } else {
          this.blockedDirectories.add(previous.cwd);
          this.blockedDirectories.add(cwd);
        }
      } else if (spawned && !stopped) {
        const verified = await this.options.workerFactory.recover(spawned).catch(() => false);
        if (verified) {
          this.store.markWorkerStopped(ledgerId);
        } else {
          this.blockedDirectories.add(previous.cwd);
          this.blockedDirectories.add(cwd);
        }
      }
      if (persistenceFailed) throw new Error("本地状态盘写入失败，会话未迁移。");
      await this.enqueueText(target, "无法验证 OMP 原生 /move，当前绑定未改变。", null);
    }
    } finally {
      commandLease.complete();
    }
  }

  /**
   * Aborts the running OMP turn without clearing the messages already queued in that run and
   * without stopping the worker, so a steered or queued message still gets its turn.
   */
  private async handleInterrupt(target: ChatTarget): Promise<void> {
    const runtime = this.runningRuntime(target);
    if (!runtime?.worker) {
      await this.enqueueText(target, DEFAULT_INTERRUPT_IDLE_TEXT, null);
      return;
    }
    runtime.interruptRequested = true;
    runtime.workAfterInterrupt = false;
    try {
      await runtime.worker.abortTurn();
    } catch {
      runtime.interruptRequested = false;
      await this.enqueueText(target, DEFAULT_INTERRUPT_REJECTED_TEXT, null);
      return;
    }
    await this.enqueueText(target, DEFAULT_INTERRUPTED_TEXT, null);
  }

  /**
   * `/queue` is the explicit way to append behind the running task regardless of the chat's mode,
   * so it always takes the durable task path minus the command prefix.
   */
  private async handleQueue(message: InboundMessage, argument: string): Promise<void> {
    const text = argument.trim();
    if (text.length === 0) {
      await this.enqueueText(message, QUEUE_USAGE, null);
      return;
    }
    // 排队任务的正文就是这条命令的参数：和 `/thread` 一样先释放命令 claim，再走唯一组装点，
    // 附件（任意类型）与引用块随正文进入排队任务。
    this.store.releaseClaim(this.commandClaim(message, "queue"));
    const body = await this.materializeInbound(message, text);
    if (body === null) return;
    await this.acceptTask(message, body);
  }

  private async handleMode(target: ChatTarget, argument: string): Promise<void> {
    const requested = argument.trim().toLowerCase();
    if (requested.length === 0) {
      await this.enqueueText(
        target,
        `当前追加模式：${this.store.getChatMode(this.bot!.uid, target)}。${MODE_USAGE}`,
        null,
      );
      return;
    }
    const mode: ChatAppendMode | null =
      requested === "steer" || requested === "default"
        ? "steer"
        : requested === "queue"
          ? "queue"
          : null;
    if (!mode) {
      await this.enqueueText(target, MODE_USAGE, null);
      return;
    }
    this.store.setChatMode(this.bot!.uid, target, mode);
    await this.enqueueText(
      target,
      mode === "steer"
        ? "追加模式已设为 steer：运行中的新消息会注入当前回合。"
        : "追加模式已设为 queue：运行中的新消息会排到当前回合之后。",
      null,
    );
  }

  private async handleCancel(target: ChatTarget): Promise<void> {
    const queued = this.store.cancelQueued(this.bot!.uid, target);
    for (const taskId of queued) this.queuedBehind.delete(taskId);
    const openQuestions = this.store.listPendingQuestions(this.bot!.uid, target);
    this.store.cancelQuestions(this.bot!.uid, target);
    for (const [messageId, rows] of this.questionsByCard(openQuestions)) {
      this.forgetAskGeneration(messageId);
      const first = rows[0];
      if (first === undefined) continue;
      if (rows.length === 1) {
        this.rewriteAskCard(first, { kind: "inactive", note: "任务已取消：该问题不再接受回答。" });
      } else {
        this.rewriteAskCardSet(rows, { kind: "closed", note: "任务已取消：该问题不再接受回答。" }, null);
      }
    }
    let active = false;
    for (const runtime of this.runtimes.values()) {
      if (targetKey(runtime.target) !== targetKey(target)) continue;
      active = true;
      runtime.cancelRequested = true;
      runtime.stopText?.();
      this.textAbortControllers.delete(runtime.taskId);
      runtime.stopActivity?.();
      runtime.cancelReplySent = true;
      if (runtime.worker) {
        void runtime.worker.stop().catch(() => false);
      }
      this.rejectQuestionsForTask(runtime.taskId, new Error("task cancelled"));
      this.store.markCancelled(runtime.taskId);
    }
    await this.enqueueText(
      target,
      queued.length > 0 || active ? DEFAULT_CANCELLED_TEXT : "当前没有可取消的任务。",
      null,
    );
  }

  private busyTargets(): ChatTarget[] {
    const seen = new Set<string>();
    const targets: ChatTarget[] = [];
    const add = (target: ChatTarget): void => {
      const key = targetKey(target);
      if (seen.has(key)) return;
      seen.add(key);
      targets.push(target);
    };
    for (const runtime of this.runtimes.values()) add(runtime.target);
    const botUid = this.bot?.uid;
    for (const task of this.store.listQueuedTasks()) {
      if (botUid === undefined || task.botUid === botUid) add(task);
    }
    return targets;
  }

  /** Returns true when the host should ask the detected supervisor to restart this process. */
  private async handleRestart(target: ChatTarget, argument: string): Promise<boolean> {
    const restart = this.options.restart;
    if (!restart) {
      await this.enqueueText(target, RESTART_UNAVAILABLE_TEXT, null);
      return false;
    }
    const mode = argument.trim();
    if (mode.length > 0 && mode !== "now") {
      await this.enqueueText(target, RESTART_USAGE, null);
      return false;
    }
    const busy = this.busyTargets();
    if (busy.length > 0 && mode !== "now") {
      await this.enqueueText(
        target,
        `当前有 ${busy.length} 个会话在运行或排队任务，重启会中断它们：先 /cancel，或发送 /restart now 强制重启。`,
        null,
      );
      return false;
    }
    this.store.addRestartNotice({
      id: randomUUID(),
      botUid: this.bot!.uid,
      channelType: target.channelType,
      channelId: target.channelId,
      requestedAt: Date.now(),
    });
    await this.enqueueText(
      target,
      busy.length > 0
        ? `正在重启 channel；${busy.length} 个会话的在途任务会中断，重启完成后会在本会话确认。`
        : "正在重启 channel；重启完成后会在本会话确认。",
      null,
    );
    return true;
  }

  private async handleStatus(target: ChatTarget): Promise<void> {
    const current = await this.status(target);
    const taskState = current.state;
    const session = current.sessionId ?? "无";
    const cwd = current.cwd ?? "无";
    await this.enqueueText(
      target,
      `session=${session}\ncwd=${cwd}\nstate=${taskState}\nmode=${current.mode}\nqueued=${current.queued}\noutbox=${current.outbox}\nunknown=${current.unknownOutbox}`,
      null,
    );
  }

  /**
   * A plain reply in the chat holding the pending question is the answer itself: a valid
   * option index selects that option, any other text is returned verbatim as the free-form
   * answer. Returns false when nothing is pending, so the text stays ordinary task input.
   */
  private async answerPendingQuestion(message: InboundMessage, text: string): Promise<boolean> {
    if (text.length === 0) return false;
    const pending = this.store.listPendingQuestions(this.bot!.uid, message);
    const question = pending[0];
    if (!question) return false;
    const claim = this.commandClaim(message, "answer");
    if (!this.store.claimCommand(claim)) return true;
    try {
      if (pending.length > 1) {
        this.answerQuestionSetFromText(message, pending, text);
        this.store.completeCommand(claim);
        return true;
      }
      let normalized: string;
      try {
        normalized = this.normalizeAnswer(question, text);
      } catch {
        await this.enqueueText(message, "答案无效：请回复选项编号或直接输入答案。", null);
        this.store.completeCommand(claim);
        return true;
      }
      if (!this.applyQuestionAnswer(question, normalized)) {
        await this.enqueueText(message, "问题已失效，本次回复未作为答案记录。", null);
        this.store.completeCommand(claim);
        return true;
      }
      this.store.completeCommand(claim);
      return true;
    } catch {
      this.store.failCommand(claim);
      throw new Error("本地状态盘写入失败，未能记录答案。");
    }
  }

  /** Groups question rows by the card that carried them; a row without a card carries none. */
  private questionsByCard(rows: readonly QuestionRecord[]): Map<string, QuestionRecord[]> {
    const cards = new Map<string, QuestionRecord[]>();
    for (const question of rows) {
      const messageId = question.cardMessageId;
      if (messageId === null) continue;
      const existing = cards.get(messageId);
      if (existing === undefined) cards.set(messageId, [question]);
      else existing.push(question);
    }
    return cards;
  }

  /**
   * One text reply answers a whole multi-question card: it becomes the answer of every question
   * still open — each interpreting it under its own rule — and the ask returns at once instead of
   * waiting for the remaining questions one by one. The answers land as one compare-and-set per
   * row inside one transaction, and each card involved is rewritten once.
   */
  private answerQuestionSetFromText(
    message: InboundMessage,
    pending: readonly QuestionRecord[],
    text: string,
  ): void {
    const botUid = this.bot!.uid;
    const answers = pending.map((question) => {
      let answer = text;
      try {
        answer = this.normalizeAnswer(question, text);
      } catch {
        // A numeric reply that does not name this question's options is still an answer as typed.
      }
      return { id: question.id, answer };
    });
    const landed = this.store.answerQuestions(botUid, message, answers);
    const byId = new Map(answers.map(({ id, answer }) => [id, answer]));
    for (const id of landed) this.releaseQuestion(id, byId.get(id) ?? text);
    for (const [messageId, rows] of this.questionsByCard(pending)) {
      this.forgetAskGeneration(messageId);
      const first = rows[0];
      if (first === undefined) continue;
      if (rows.length === 1) {
        this.rewriteAskCard(first, { kind: "answered", answer: byId.get(first.id) ?? text });
      } else {
        const fresh = this.store.listQuestionsByCard(botUid, message, messageId);
        this.rewriteAskCardSet(fresh, { kind: "closed", note: null }, null);
      }
    }
  }

  private normalizeAnswer(question: { payload: string }, answer: string): string {
    const payload = JSON.parse(question.payload) as Record<string, unknown>;
    const options = payload.options;
    if (!Array.isArray(options) || options.length === 0) return answer;
    const numeric = /^\d+(?:\s*,\s*\d+)*$/u.test(answer);
    if (!numeric) return answer;
    const values = answer.split(",").map((value) => Number(value.trim()));
    if (payload.multi !== true && values.length !== 1) throw new Error("multiple options are not allowed");
    if (new Set(values).size !== values.length || values.some((value) => value < 1 || value > options.length)) {
      throw new Error("option is out of range");
    }
    return values.join(",");
  }

  private async acceptTask(
    message: InboundMessage,
    text: string,
    historyEligible = true,
  ): Promise<boolean> {
    let binding = await this.reconcileExternalBinding(
      message,
      this.store.getBinding(this.bot!.uid, message),
    );
    if (!binding) {
      let cwd: string;
      try {
        cwd = await this.defaultWorkDirectory(message);
      } catch {
        await this.enqueueText(message, "无法创建默认会话目录，未接收任务。", null);
        return false;
      }
      binding = {
        botUid: this.bot!.uid,
        channelId: message.channelId,
        channelType: message.channelType,
        sessionId: createSessionId(),
        cwd,
        sessionFile: null,
      };
      try {
        this.store.ensureBinding(binding);
      } catch {
        throw new Error("本地状态盘写入失败，任务未接收。");
      }
    } else if (!binding.sessionFile) {
      const current = binding;
      const cwd = await this.defaultWorkDirectory(message).catch(() => current.cwd);
      if (cwd !== current.cwd) {
        binding = { ...current, cwd };
        try {
          this.store.ensureBinding(binding);
        } catch {
          throw new Error("本地状态盘写入失败，任务未接收。");
        }
      }
    }
    const task: TaskRecord = {
      id: randomUUID(),
      botUid: this.bot!.uid,
      channelId: message.channelId,
      channelType: message.channelType,
      messageId: message.messageId,
      senderId: message.senderId,
      text,
      sessionId: binding.sessionId,
      cwd: binding.cwd,
      state: "queued",
      resultText: null,
      errorText: null,
      messageSeq: message.messageSeq,
      // 以 `/` 开头的任务正文是 OMP 自己的输入语言：内置命令（`/move`）与插件命令（`/wtm`）
      // 由 OMP 从 prompt 首行分派，历史块前缀会把这一行变回普通模型输入，因此不带历史块、
      // 不发起 sync，水位线也不因这轮推进。
      historyEligible: historyEligible && !isCommand(text),
    };
    let accepted: boolean;
    // Measured before acceptance so the new task's own row cannot count itself as the work it waits for.
    const startsBehind = this.hasBlockingWork(message, task.cwd);
    try {
      accepted = this.store.acceptTask(this.taskClaim(message), task);
    } catch {
      throw new Error("本地状态盘写入失败，任务未接收。");
    }
    if (!accepted) return false;
    if (startsBehind) this.queuedBehind.add(task.id);
    this.scheduleTask(task.id);
    this.flushTarget(message);
    return true;
  }

  /** True when a new task on this chat cannot start at once: the chat or its directory is busy. */
  private hasBlockingWork(target: ChatTarget, cwd: string): boolean {
    if (this.chatTails.has(targetKey(target))) return true;
    if (this.store.listQueuedTasks().some((queued) => queued.cwd === cwd)) return true;
    for (const runtime of this.runtimes.values()) {
      if (runtime.cwd === cwd) return true;
    }
    return false;
  }
  private scheduleTask(taskId: string): void {
    const task = this.store.getTask(taskId);
    if (!task) return;
    const target = taskTarget(task);
    const chatKey = targetKey(target);
    const previousChat = this.chatTails.get(chatKey) ?? Promise.resolve();
    const next = previousChat.catch(() => {}).then(() => this.runScheduledTask(taskId));
    this.chatTails.set(chatKey, next);
    this.scheduledTasks.set(taskId, next);
    this.taskOperations.add(next);
    const cleanup = () => {
      if (this.chatTails.get(chatKey) === next) this.chatTails.delete(chatKey);
      if (this.scheduledTasks.get(taskId) === next) this.scheduledTasks.delete(taskId);
      this.taskOperations.delete(next);
    };
    void next.then(cleanup, cleanup);
  }

  private createDirectoryLease(cwd: string): DirectoryLease {
    const previous = this.directoryTails.get(cwd);
    const completion = Promise.withResolvers<void>();
    const lease = completion.promise;
    this.directoryTails.set(cwd, lease);
    let complete = false;
    return {
      ready: previous?.catch(() => {}) ?? Promise.resolve(),
      complete: () => {
        if (complete) return;
        complete = true;
        if (this.directoryTails.get(cwd) === lease) this.directoryTails.delete(cwd);
        completion.resolve();
      },
    };
  }

  private tryAcquireCommandDirectories(cwds: readonly string[]): DirectoryLease | undefined {
    const unique = [...new Set(cwds)];
    if (unique.some(cwd => this.directoryUnavailable(cwd))) return undefined;
    const leases = unique.map(cwd => this.createDirectoryLease(cwd));
    return {
      ready: Promise.all(leases.map(lease => lease.ready)).then(() => {}),
      complete: () => {
        for (const lease of leases) lease.complete();
      },
    };
  }

  private async acquireDirectoryLease(cwd: string): Promise<DirectoryLease> {
    const lease = this.createDirectoryLease(cwd);
    await lease.ready;
    return lease;
  }

  private async earlierDirectoryOperations(task: TaskRecord, cwd: string): Promise<Promise<void>[]> {
    const queued = this.store.listQueuedTasks();
    const currentIndex = queued.findIndex(candidate => candidate.id === task.id);
    if (currentIndex < 0) return [];
    const operations: Promise<void>[] = [];
    for (let index = 0; index < currentIndex; index += 1) {
      const earlier = queued[index];
      if (!earlier) continue;
      const binding = await this.reconcileExternalBinding(
        taskTarget(earlier),
        this.store.getBinding(this.bot!.uid, taskTarget(earlier)),
      );
      if ((binding?.cwd ?? earlier.cwd) !== cwd) continue;
      const operation = this.scheduledTasks.get(earlier.id);
      if (operation) operations.push(operation);
    }
    return operations;
  }

  private async runScheduledTask(taskId: string): Promise<void> {
    let lease: DirectoryLease | undefined;
    try {
      for (let attempt = 0; attempt < 4; attempt += 1) {
        const current = this.store.getTask(taskId);
        if (this.stopping || !current || current.state !== "queued") return;
        const binding = await this.reconcileExternalBinding(
          taskTarget(current),
          this.store.getBinding(this.bot!.uid, taskTarget(current)),
        );
        const reconciledCurrent = this.store.getTask(taskId);
        if (this.stopping || !reconciledCurrent || reconciledCurrent.state !== "queued") return;
        const cwd = binding?.cwd ?? reconciledCurrent.cwd;
        const earlier = await this.earlierDirectoryOperations(reconciledCurrent, cwd);
        await Promise.all(earlier.map(operation => operation.catch(() => {})));

        const beforeLease = this.store.getTask(taskId);
        if (this.stopping || !beforeLease || beforeLease.state !== "queued") return;
        lease = await this.acquireDirectoryLease(cwd);

        const refreshed = await this.reconcileExternalBinding(
          taskTarget(beforeLease),
          this.store.getBinding(this.bot!.uid, taskTarget(beforeLease)),
        );
        const latest = this.store.getTask(taskId);
        if (this.stopping || !latest || latest.state !== "queued") return;
        const currentCwd = refreshed?.cwd ?? latest.cwd;
        if (currentCwd !== cwd) {
          lease.complete();
          lease = undefined;
          continue;
        }
        const newlyEarlier = await this.earlierDirectoryOperations(latest, currentCwd);
        if (newlyEarlier.length > 0) {
          lease.complete();
          lease = undefined;
          await Promise.all(newlyEarlier.map(operation => operation.catch(() => {})));
          continue;
        }
        await this.runTask(taskId, cwd);
        return;
      }
      const task = this.store.getTask(taskId);
      if (!task || task.state !== "queued") return;
      this.queuedBehind.delete(taskId);
      this.store.markFailed(taskId, "绑定目录在启动前持续变更，任务未启动。");
      await this.enqueueText(taskTarget(task), genericTaskFailure(false), taskId);
    } finally {
      lease?.complete();
    }
  }

  private async runTask(taskId: string, lockedCwd?: string): Promise<void> {
    const persistedTask = this.store.getTask(taskId);
    if (!persistedTask || persistedTask.state !== "queued") return;
    const bindingAtStart = await this.reconcileExternalBinding(
      taskTarget(persistedTask),
      this.store.getBinding(this.bot!.uid, taskTarget(persistedTask)),
    );
    const task = bindingAtStart
      ? { ...persistedTask, cwd: bindingAtStart.cwd, sessionId: bindingAtStart.sessionId }
      : persistedTask;
    if (lockedCwd !== undefined && task.cwd !== lockedCwd) {
      this.queuedBehind.delete(taskId);
      this.store.markFailed(taskId, "绑定目录在启动前再次变更，任务未启动。");
      await this.enqueueText(taskTarget(task), genericTaskFailure(false), taskId);
      return;
    }
    if (this.blockedDirectories.has(task.cwd)) {
      this.queuedBehind.delete(taskId);
      this.store.markFailed(taskId, "directory remains blocked by an unconfirmed worker");
      await this.enqueueText(taskTarget(task), genericTaskFailure(false), taskId);
      return;
    }
    if (!this.store.markStarted(taskId)) return;
    if (this.queuedBehind.delete(taskId)) {
      this.enqueueText(taskTarget(task), receiptText("开始执行排队消息", task.text), null);
    }
    const runtime: RuntimeTask = {
      taskId,
      target: taskTarget(task),
      cwd: task.cwd,
      cancelRequested: false,
      cancelReplySent: false,
      promptInFlight: false,
      interruptRequested: false,
      workAfterInterrupt: false,
      steerDeliveries: [],
    };
    this.runtimes.set(taskId, runtime);
    let worker: OmpWorker | undefined;
    let workerLedgerId: string | undefined;
    let spawned: WorkerProcess | undefined;
    let progressTimer: NodeJS.Timeout | undefined;
    let typingTimer: NodeJS.Timeout | undefined;
    let progressTail: Promise<void> = Promise.resolve();
    const progressAbort = new AbortController();
    const textAbort = new AbortController();
    this.textAbortControllers.set(taskId, textAbort);
    let typingAbort: AbortController | undefined;
    const target = taskTarget(task);
    let roundIndex = 0;
    const createRound = (): ProgressRound => ({
      clientMsgNo: `progress-${taskId}-${roundIndex}`,
      tracker: createExecutionTracker({ runId: taskId }),
      started: false,
      finished: false,
      retired: false,
      disabled: false,
      failureLogged: false,
      messageId: undefined,
      seq: 0,
      latest: undefined,
      version: 0,
      sentVersion: 0,
      nextEditAt: 0,
    });
    let progressRound = createRound();
    const assistantTexts = new Map<string, string>();
    let snapshot: GroupContext | null = null;
    let contextInjection: ContextInjection | undefined;
    let currentContextSidecarPath: string | undefined;
    let contextNeedsCommit = false;

    const trackActivity = (operation: Promise<void>): void => {
      const settled = operation.catch(() => {});
      this.activityOperations.add(settled);
      void settled.finally(() => {
        this.activityOperations.delete(settled);
      });
    };
    const runActivityRequest = async <T>(
      work: (signal: AbortSignal) => Promise<T>,
      parentSignal?: AbortSignal,
    ): Promise<T> => {
      const controller = new AbortController();
      const abort = () => controller.abort();
      if (parentSignal?.aborted) {
        controller.abort();
      } else {
        parentSignal?.addEventListener("abort", abort, { once: true });
      }
      const timer = setTimeout(() => controller.abort(), ACTIVITY_REQUEST_TIMEOUT_MS);
      try {
        return await work(controller.signal);
      } finally {
        clearTimeout(timer);
        parentSignal?.removeEventListener("abort", abort);
      }
    };
    const queueProgress = (work: () => Promise<void>): void => {
      progressTail = progressTail.then(work, work).catch(() => {});
      trackActivity(progressTail);
    };
    const scheduleProgressEdit = (): void => {
      const current = progressRound;
      if (this.stopping || current.retired || !current.started || current.disabled || progressTimer) return;
      if (current.version <= current.sentVersion) return;
      progressTimer = setTimeout(() => {
        progressTimer = undefined;
        const pending = progressRound;
        if (!pending.retired && pending.version > pending.sentVersion) {
          queueProgress(() => editProgress(pending));
        }
      }, Math.max(0, current.nextEditAt - Date.now()));
    };
    const editProgress = async (round: ProgressRound): Promise<void> => {
      const messageId = round.messageId;
      const progress = round.latest;
      if (round.disabled || !messageId || !progress) return;
      if (round.version <= round.sentVersion) {
        if (!round.retired) scheduleProgressEdit();
        return;
      }
      const running = progress.state === "running";
      // A superseded card only still receives the terminal edit that closes it.
      if (running && round.retired) return;
      if (running && Date.now() < round.nextEditAt) {
        scheduleProgressEdit();
        return;
      }
      const version = round.version;
      const cardSeq = ++round.seq;
      if (running) round.nextEditAt = Date.now() + PROGRESS_MERGE_MS;
      const parentSignal = running ? progressAbort.signal : undefined;
      try {
        await runActivityRequest(
          (signal) => this.options.transport.editProgressCard(target, messageId, progress, cardSeq, signal),
          parentSignal,
        );
        if (round.version === version) round.sentVersion = version;
        if (running) {
          this.rememberProgressCard(taskId, target, messageId, cardSeq, progress);
        } else {
          this.forgetProgressCard(taskId, messageId);
        }
      } catch (error) {
        const reason = error instanceof Error ? error.message : "unknown error";
        if (!running) {
          round.disabled = true;
          if (!round.retired) {
            clearTimeout(progressTimer);
            progressTimer = undefined;
          }
          console.error(
            `[progress-card] 终态编辑失败 target=${targetKey(target)} state=${progress.state}`
            + ` seq=${cardSeq}：${reason}`,
          );
        } else if (!round.failureLogged) {
          round.failureLogged = true;
          console.error(`[progress-card] 运行中编辑失败 target=${targetKey(target)} seq=${cardSeq}：${reason}`);
        }
      } finally {
        if (!round.retired) scheduleProgressEdit();
      }
    };
    const finishProgress = (state: Exclude<ProgressCard["state"], "running">): void => {
      const current = progressRound;
      if (!current.started || current.finished) return;
      current.finished = true;
      clearTimeout(progressTimer);
      progressTimer = undefined;
      current.latest = current.tracker.apply({
        type: "run_end",
        atMs: Date.now(),
        state,
      });
      current.version += 1;
      queueProgress(() => editProgress(current));
    };
    /**
     * A steered message opens the next user round. The card the reader is looking at must sit
     * below that message instead of staying where the first tool call of the run happened, so the
     * round in progress is closed and the next real tool progress opens a card of its own.
     */
    const rollProgressCard = (): void => {
      const previous = progressRound;
      if (previous.retired) return;
      previous.retired = true;
      clearTimeout(progressTimer);
      progressTimer = undefined;
      roundIndex += 1;
      progressRound = createRound();
      // The todo list belongs to the task rather than to one round, so the new card keeps it.
      const todos = previous.latest?.todos ?? [];
      if (todos.length > 0) {
        progressRound.tracker.apply({ type: "todo_update", atMs: Date.now(), phases: todos });
      }
      if (!previous.started || previous.finished || previous.disabled) return;
      previous.finished = true;
      previous.latest = previous.tracker.apply({ type: "run_end", atMs: Date.now(), state: "completed" });
      previous.version += 1;
      queueProgress(() => editProgress(previous));
    };
    const stopActivity = (abortProgress = true): void => {
      if (!progressRound.finished || abortProgress) {
        clearTimeout(progressTimer);
        progressTimer = undefined;
      }
      clearTimeout(typingTimer);
      typingTimer = undefined;
      if (abortProgress) progressAbort.abort();
      typingAbort?.abort();
    };
    const startTyping = (): void => {
      typingAbort = new AbortController();
      const renew = (): void => {
        const parentSignal = typingAbort?.signal;
        if (!parentSignal || parentSignal.aborted) return;
        trackActivity(
          runActivityRequest(
            (signal) => this.options.transport.sendTyping(target, signal),
            parentSignal,
          ),
        );
        typingTimer = setTimeout(renew, TYPING_RENEW_MS);
      };
      renew();
    };
    runtime.stopActivity = stopActivity;
    runtime.stopText = () => textAbort.abort();
    runtime.rollProgressCard = rollProgressCard;
    startTyping();

    try {
      const groupTask = isGroupTarget(target);
      snapshot = groupTask
        ? normalizeGroupContext(await this.options.transport.getGroupContext(target))
        : null;
      if (runtime.cancelRequested) {
        this.store.markCancelled(taskId);
        return;
      }
      const sessionDir = join(this.options.config.stateDir, "sessions");
      const sidecarRoot = contextSidecarRoot(this.options.config, this.bot!.uid, target);
      await mkdir(sessionDir, { recursive: true });
      const binding = bindingAtStart ?? this.store.getBinding(this.bot!.uid, target);
      const workerOptions: WorkerOptions = {
        ...(groupTask
          ? {
              contextScope: {
                sidecarRoot,
                botUid: this.bot!.uid,
                target: { ...target },
              },
            }
          : {}),
        cwd: task.cwd,
        sessionDir,
        onSpawn: (processInfo) => {
          spawned = processInfo;
          workerLedgerId = task.id;
          this.store.recordWorker({
            id: task.id,
            botUid: this.bot!.uid,
            sessionId: task.sessionId,
            sessionFile: binding?.sessionFile ?? join(sessionDir, `${task.sessionId}.jsonl`),
            pid: processInfo.pid,
            pgid: processInfo.pgid,
            identity: processInfo.identity,
            cwd: processInfo.cwd,
            moveTargetCwd: null,
            state: "running",
          });
        },
      };
      if (binding?.sessionFile) {
        const sessionFile = await existingSessionFile(binding.sessionFile);
        if (!sessionFile) throw new Error("绑定的 OMP 会话文件不存在");
        workerOptions.sessionFile = sessionFile;
      }
      worker = await this.options.workerFactory.create(workerOptions);
      runtime.worker = worker;
      if (runtime.cancelRequested || this.stopping) {
        const stopped = await worker.stop().catch(() => false);
        if (workerLedgerId) {
          if (stopped) this.store.markWorkerStopped(workerLedgerId);
          else this.blockedDirectories.add(task.cwd);
        }
        if (!stopped) this.blockedDirectories.add(task.cwd);
        if (runtime.cancelRequested) this.store.markCancelled(taskId);
        return;
      }
      const authoritativeState: ProjectSessionState = await worker.refreshState();
      const previousForWorker: ProjectSessionBinding = binding ?? {
        sessionId: "",
        cwd: task.cwd,
        sessionFile: null,
      };
      const verified = binding?.sessionFile
        ? await verifyMovedSession(previousForWorker, authoritativeState, task.cwd)
        : await verifyFreshSession(previousForWorker, authoritativeState, task.cwd);
      this.store.updateBindingSession(
        this.bot!.uid,
        target,
        verified.sessionId,
        verified.sessionFile,
      );
      if (groupTask) {
        const immutableSnapshot = snapshot;
        if (!immutableSnapshot) throw new Error("群说明快照缺失");
        const existingLedger = this.store.getContextLedger(this.bot!.uid, target, verified.sessionId);
        if (existingLedger && existingLedger.version > immutableSnapshot.version) {
          throw new Error("群说明版本回退，未启动任务");
        }
        let ledger = existingLedger;
        if (!ledger || ledger.version < immutableSnapshot.version) {
          ledger = this.store.prepareContextInjection(
            this.bot!.uid,
            target,
            verified.sessionId,
            immutableSnapshot.version,
          );
        }
        const previousVersion = ledger.previousVersion;
        const injectionDecision: ContextInjection["injectionDecision"] = ledger.phase === "committed"
          && ledger.version === immutableSnapshot.version
          ? "unchanged"
          : previousVersion === null
            ? "initial"
            : "version-change";
        const explanation = injectionDecision === "version-change" && previousVersion !== null
          ? `群说明版本从 ${previousVersion} 更新为 ${immutableSnapshot.version}，注入最新完整快照。`
          : injectionDecision === "initial"
            ? "首次注入当前群/线程共享说明完整快照。"
            : `群说明版本 ${immutableSnapshot.version} 未变化，本轮不重复注入正文。`;
        contextInjection = {
          snapshot: { ...immutableSnapshot },
          injectionDecision,
          attemptId: ledger.attemptId,
          explanation,
        };
        contextNeedsCommit = injectionDecision !== "unchanged";
        const sidecar: ContextSidecar = {
          schemaVersion: 1,
          botUid: this.bot!.uid,
          target: { ...target },
          sessionId: verified.sessionId,
          snapshot: { ...immutableSnapshot },
          injectionDecision,
          attemptId: ledger.attemptId,
          marker: {
            phase: ledger.phase,
            botUid: this.bot!.uid,
            target: { ...target },
            sessionId: verified.sessionId,
            version: immutableSnapshot.version,
            attemptId: ledger.attemptId,
          },
          ...(previousVersion === null ? {} : { previousVersion }),
        };
        currentContextSidecarPath = await writeContextSidecar(sidecarRoot, sidecar);
      }
      /**
       * Feeds one real execution event into the current round's card. The card is created lazily
       * on the round's first real tool/todo progress, so a round without tool work shows no card.
       */
      const trackProgress = (event: WorkerEvent, progress: ProgressCard): void => {
        const current = progressRound;
        current.latest = progress;
        if (current.retired) return;
        if (current.finished && progress.state !== "running") return;
        current.version += 1;
        const startsCard = event.type === "tool_start" ||
          event.type === "tool_end" ||
          event.type === "todo_update";
        if (!current.started && !startsCard) return;
        if (!current.started) {
          current.started = true;
          const initial = progress;
          const initialVersion = current.version;
          if (progress.state !== "running") {
            current.finished = true;
          }
          queueProgress(async () => {
            try {
              const receipt = await runActivityRequest(
                (signal) => this.options.transport.sendProgressCard(
                  target,
                  initial,
                  current.clientMsgNo,
                  signal,
                ),
                progressAbort.signal,
              );
              if (!receipt?.messageId) {
                current.disabled = true;
                console.error(
                  `[progress-card] 未创建进度卡 target=${targetKey(target)}：卡片能力不可用或内容超出限制`,
                );
                return;
              }
              current.messageId = receipt.messageId;
              current.seq = 1;
              current.sentVersion = initialVersion;
              this.rememberProgressCard(taskId, target, current.messageId, 1, initial);
              if (current.version > current.sentVersion) {
                if (current.finished) queueProgress(() => editProgress(current));
                else scheduleProgressEdit();
              } else if (!current.finished) {
                scheduleProgressEdit();
              }
            } catch (error) {
              current.started = false;
              console.error(
                `[progress-card] 创建失败 target=${targetKey(target)}：`
                + `${error instanceof Error ? error.message : "unknown error"}`,
              );
            }
          });
          return;
        }
        if (progress.state !== "running") {
          current.finished = true;
          clearTimeout(progressTimer);
          progressTimer = undefined;
          queueProgress(() => editProgress(current));
        } else {
          scheduleProgressEdit();
        }
      };
      const onEvent = (event: WorkerEvent): void => {
        if (runtime.cancelRequested || this.stopping) return;
        if (runtime.interruptRequested && event.type !== "turn_end" && event.type !== "run_end") {
          runtime.workAfterInterrupt = true;
        }
        if (event.type === "text") {
          if (event.messageId.trim().length === 0 || (event.text.length === 0 && !event.final)) return;
          assistantTexts.set(event.messageId, event.text);
          this.enqueueStreamText(target, task.id, event);
          return;
        }
        if (event.type === "steer_consumed") {
          this.noteSteerConsumed(runtime, event);
          return;
        }
        trackProgress(event, progressRound.tracker.apply(event));
      };
      const historyTurn = await this.historyTurnFor(task, target, verified.sessionId);
      const promptContext: PromptContext = {
        tools: this.createHostTools(task, snapshot),
        ...(contextInjection === undefined ? {} : { contextInjection }),
      };
      runtime.promptInFlight = true;
      try {
        await worker.prompt(historyTurn?.text ?? task.text, onEvent, promptContext);
      } finally {
        runtime.promptInFlight = false;
        if (historyTurn) this.commitHistoryTurn(target, verified.sessionId, historyTurn);
      }
      if (runtime.cancelRequested) {
        this.store.markCancelled(taskId);
        finishProgress("cancelled");
        await this.sendCancelledIfNeeded(runtime);
      } else if (runtime.interruptRequested && !runtime.workAfterInterrupt) {
        this.store.markInterrupted(taskId, "用户中断当前回合");
        finishProgress("cancelled");
      } else {
        if (contextNeedsCommit) {
          const sidecarPath = currentContextSidecarPath;
          const injection = contextInjection;
          if (!sidecarPath || !injection) {
            throw new Error("群说明注入状态缺失，未提交");
          }
          const sidecar = await readContextSidecar(sidecarPath);
          if (
            sidecar.marker.phase !== "pending"
            || sidecar.botUid !== this.bot!.uid
            || sidecar.sessionId !== worker.sessionId
            || sidecar.target.channelType !== target.channelType
            || sidecar.target.channelId !== target.channelId
            || sidecar.snapshot.version !== injection.snapshot.version
            || sidecar.attemptId !== injection.attemptId
            || !sidecar.prepared
            || !preparedReceiptMatches(sidecar, sidecar.prepared)
          ) {
            throw new Error("群说明注入未获得匹配的 prepared receipt，未提交");
          }
          if (!this.store.commitContextInjection(
            this.bot!.uid,
            target,
            worker.sessionId,
            injection.snapshot.version,
            injection.attemptId,
          )) {
            throw new Error("群说明 marker CAS 提交失败，保留待重放状态");
          }
        }
        if (currentContextSidecarPath) {
          await removeContextSidecar(currentContextSidecarPath).catch(() => {});
          currentContextSidecarPath = undefined;
        }
        const resultText = assistantTexts.size > 0 ? [...assistantTexts.values()].join("\n\n") : null;
        this.store.markCompleted(taskId, resultText);
        finishProgress("completed");
        this.flushTarget(target);
      }
    } catch (error) {
      if (runtime.cancelRequested) {
        this.store.markCancelled(taskId);
        finishProgress("cancelled");
        await this.sendCancelledIfNeeded(runtime);
      } else {
        const contextFailure = isGroupTarget(runtime.target) && !worker;
        this.store.markFailed(taskId, contextFailure ? "群说明读取失败" : safeErrorMessage(error, "worker failed"));
        finishProgress("failed");
        await this.enqueueText(runtime.target, genericTaskFailure(contextFailure), taskId);
      }
    } finally {
      runtime.stopActivity?.(!progressRound.finished);
      this.rejectQuestionsForTask(taskId, new Error("task finished"));
      if (worker) {
        const stopped = await worker.stop().catch(() => false);
        if (workerLedgerId) {
          if (stopped) this.store.markWorkerStopped(workerLedgerId);
          else this.blockedDirectories.add(task.cwd);
        }
        if (!stopped) this.blockedDirectories.add(task.cwd);
      } else if (workerLedgerId && spawned) {
        const stopped = await this.options.workerFactory.recover(spawned).catch(() => false);
        if (stopped) this.store.markWorkerStopped(workerLedgerId);
        else this.blockedDirectories.add(task.cwd);
      }
      this.releaseTextAbortIfIdle(taskId);
      this.runtimes.delete(taskId);
      this.maybeRunDeferredRestart();
    }
  }

  private async sendCancelledIfNeeded(runtime: RuntimeTask): Promise<void> {
    if (runtime.cancelReplySent) return;
    runtime.cancelReplySent = true;
    await this.enqueueText(
      runtime.target,
      this.restartRequested ? DEFAULT_RESTARTING_TEXT : DEFAULT_CANCELLED_TEXT,
      runtime.taskId,
    );
  }

  private createHostTools(task: TaskRecord, _snapshot: GroupContext | null): HostTool[] {
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
        const spoolDirectory = join(this.options.config.stateDir, "spool", randomUUID());
        await mkdir(spoolDirectory, { recursive: true });
        const spoolPath = join(spoolDirectory, basename(canonical));
        try {
          await copyFile(canonical, spoolPath);
          if (signal.aborted) throw new Error("host tool cancelled");
          const clientMsgNo = randomUUID();
          this.store.enqueueOutbox({
            id: randomUUID(),
            botUid: this.bot!.uid,
            channelId: target.channelId,
            channelType: target.channelType,
            taskId: task.id,
            kind: "file",
            text: null,
            path: spoolPath,
            fileKind: args.kind,
            clientMsgNo,
          });
          this.flushTarget(target);
          return `文件已加入发送队列 ${clientMsgNo}`;
        } catch (error) {
          await rm(spoolDirectory, { recursive: true, force: true }).catch(() => {});
          throw error;
        }
      },
    };
    const askTool: HostTool = {
      name: "ask",
      description: "向当前聊天提问并等待用户回答；多个问题会在一张卡片上整体提交，也可直接回复文本一次作为整组答案。",
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
      execute: async (args, signal) => this.askQuestions(task, target, args, signal),
    };
    const channelTool = this.createChannelTool(target);
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
        "管理当前群/线程：读群信息与成员、改群名/群公告（需 bot 是群管理员）、读写共享说明、列出/创建/查询/加入/退出/删除 thread；create-thread 可带 worktree_name 为线程建立独立 git worktree 并把线程目录绑定到它；thread-dispatch 把一条任务派发给本群的某个 thread（在那边排队执行，本群收到回执），派发出去的任务不能再派发。目标固定为当前聊天，不能指向其他群。",
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
        this.runManagementTool(task, target, parentGroupId, currentShortId, args, signal),
    };
    return [channelTool, managementTool, fileTool, askTool];
  }

  /**
   * Channel-level control for the agent. Available in every chat, because the restart it can
   * request is a property of the channel, not of a group.
   */
  private createChannelTool(target: ChatTarget): HostTool {
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
      execute: async (args) => this.runChannelTool(target, args),
    };
  }

  private async runChannelTool(target: ChatTarget, args: Record<string, unknown>): Promise<string> {
    const action = args.action;
    if (action === "status") {
      return JSON.stringify({
        pid: process.pid,
        restart_pending: this.pendingRestart !== null,
        running_targets: [...this.runtimes.values()].map((runtime) => targetKey(runtime.target)),
        queued_tasks: this.store.listQueuedTasks().length,
        blocked_cwds: this.blockedDirectories.size,
      });
    }
    if (action === "restart") {
      if (!this.options.restart) return RESTART_UNAVAILABLE_TEXT;
      this.pendingRestart = { channelType: target.channelType, channelId: target.channelId };
      return CHANNEL_RESTART_SCHEDULED_TEXT;
    }
    throw new Error(`unknown action: ${typeof action === "string" ? action : String(action)}`);
  }

  /**
   * Runs a restart the agent asked for once it cannot cost work: every chat idle and no queued
   * task. The request is fire-and-forget, because the supervisor stops this process.
   */
  private maybeRunDeferredRestart(): void {
    const target = this.pendingRestart;
    if (!target || this.stopping) return;
    if (this.runtimes.size > 0 || this.store.listQueuedTasks().length > 0) return;
    this.pendingRestart = null;
    this.restartRequested = true;
    this.store.addRestartNotice({
      id: randomUUID(),
      botUid: this.bot!.uid,
      channelType: target.channelType,
      channelId: target.channelId,
      requestedAt: Date.now(),
    });
    const restart = this.options.restart;
    if (!restart) return;
    void restart.request().catch((error: unknown) => {
      console.error(
        `[restart] 重启命令执行失败：${error instanceof Error ? error.message : "unknown error"}`,
      );
    });
  }

  private async runManagementTool(
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
    const transport = this.options.transport;
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
          ...(await this.createThreadWorkspace(task.cwd, thread, workspaceName, signal)),
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
        await this.deleteThread(groupId, shortId, signal);
        return `已删除线程 ${shortId}`;
      }
      case "thread-dispatch": {
        if (task.messageId.startsWith(DISPATCH_MESSAGE_PREFIX)) {
          throw new Error("dispatched tasks cannot dispatch further");
        }
        const shortId = threadId();
        const text = namedText("text", MAX_MANAGEMENT_TASK_CHARS);
        return this.dispatchThreadTask(task, target, groupId, shortId, text, signal);
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
  private async deleteThread(groupId: string, shortId: string, signal?: AbortSignal): Promise<void> {
    const transport = this.options.transport;
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
   * Gives a freshly created thread its own git worktree and pins the thread binding to it, so
   * tasks dispatched into that thread never share the parent group's directory. The workspace is
   * reported next to the created thread; a workspace that cannot be created becomes
   * `worktree_error` instead of a thrown rejection, because the thread itself already exists and
   * keeps the ordinary directory order.
   */
  private async createThreadWorkspace(
    baseDir: string,
    thread: ThreadSummary,
    workspaceName: string,
    signal?: AbortSignal,
  ): Promise<{ worktree: { path: string; branch: string } } | { worktree_error: string }> {
    const bot = this.bot;
    const worktrees = this.options.worktrees;
    if (!worktrees) return { worktree_error: "本部署未配置隔离工作区能力" };
    if (!bot) return { worktree_error: "Bot 身份未注册，无法绑定线程目录" };
    try {
      const workspace = await worktrees.create(
        { baseDir, label: workspaceName, threadId: thread.shortId },
        signal,
      );
      this.store.ensureBinding({
        botUid: bot.uid,
        channelId: thread.channelId,
        channelType: 5,
        sessionId: createSessionId(),
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
   * Hands one task to a thread of the current group through the durable inbound path: the thread
   * shows the handoff, the task is accepted with the owner as sender (so it keeps dedup,
   * queueing and question semantics), and the caller reports its own receipt. Depth is one - the
   * dispatch tool is withheld from tasks that arrived this way.
   */
  private async dispatchTaskToThread(thread: ThreadSummary, text: string): Promise<string> {
    const bot = this.bot;
    const ownerId = bot ? this.store.getOwnerId(bot.uid)?.trim() ?? "" : "";
    if (!bot || ownerId.length === 0) {
      throw new Error("owner is not enrolled; cannot dispatch a task");
    }
    const threadTarget: ChatTarget = { channelId: thread.channelId, channelType: 5 };
    this.enqueueText(threadTarget, `来自群会话的派发任务：\n\n${text}`, null);
    const messageId = `${DISPATCH_MESSAGE_PREFIX}${randomUUID()}`;
    await this.handleInbound({
      channelId: threadTarget.channelId,
      channelType: 5,
      messageId,
      senderId: ownerId,
      text,
      mentionedUserIds: [bot.uid],
      attachments: [],
      quoted: null,
      supported: true,
      messageSeq: 0,
      messageTimestamp: Date.now(),
    });
    return messageId;
  }
  /**
   * The `thread-dispatch` tool: resolves the target thread, hands the task over, and reports the
   * dispatch in the calling chat.
   */
  private async dispatchThreadTask(
    task: TaskRecord,
    target: ChatTarget,
    groupId: string,
    shortId: string,
    text: string,
    signal: AbortSignal,
  ): Promise<string> {
    const thread = await this.options.transport.getThread(groupId, shortId, signal);
    if (thread.groupId.length > 0 && thread.groupId !== groupId) {
      throw new Error(`thread ${shortId} does not belong to the current group`);
    }
    const messageId = await this.dispatchTaskToThread(thread, text);
    this.enqueueText(target, `已派发任务到线程 ${shortId}（${thread.channelId}）。`, null);
    return JSON.stringify({
      dispatched: true,
      short_id: shortId,
      channel_id: thread.channelId,
      message_id: messageId,
      task_id: task.id,
    });
  }

  private async askQuestions(
    task: TaskRecord,
    target: ChatTarget,
    args: Record<string, unknown>,
    signal: AbortSignal,
  ): Promise<string> {
    if (!Array.isArray(args.questions) || args.questions.length === 0) {
      throw new Error("questions must be a non-empty array");
    }
    const questions = args.questions as unknown[];
    const specs = questions.map((value, index) =>
      this.askSpec(task, value, questions.length > 1 ? index + 1 : null, questions.length));
    const answers = specs.length === 1
      ? [await this.askSingleQuestion(task, target, specs[0]!, signal)]
      : await this.askQuestionSet(task, target, specs, signal);
    return JSON.stringify({ answers });
  }

  /**
   * Validates one `ask` question and mints its row. A set question records its 1-based position
   * so every later frame keeps the same per-question input ids.
   */
  private askSpec(task: TaskRecord, value: unknown, cardIndex: number | null, cardSize: number): AskSpec {
    if (!value || typeof value !== "object" || Array.isArray(value)) {
      throw new Error("invalid question");
    }
    const question = value as Record<string, unknown>;
    if (typeof question.question !== "string" || question.question.trim().length === 0) {
      throw new Error("question text is required");
    }
    if (
      question.id !== undefined &&
      (typeof question.id !== "string" || question.id.trim().length === 0)
    ) {
      throw new Error("question id must be a non-empty string");
    }
    if (question.multi !== undefined && typeof question.multi !== "boolean") {
      throw new Error("question multi must be boolean");
    }
    if (question.options !== undefined) {
      if (!Array.isArray(question.options) || question.options.length === 0) {
        throw new Error("question options must be non-empty");
      }
      for (const option of question.options) {
        if (!option || typeof option !== "object" || Array.isArray(option)) {
          throw new Error("invalid question option");
        }
        const optionRecord = option as Record<string, unknown>;
        if (typeof optionRecord.label !== "string" || optionRecord.label.trim().length === 0) {
          throw new Error("question option label is required");
        }
      }
    }
    const id = `${task.id}:${randomUUID()}`;
    const sourceId = typeof question.id === "string" ? question.id : null;
    const payload: Record<string, unknown> = {
      id,
      sourceId,
      question: question.question,
      options: question.options,
      multi: question.multi === true,
      recommended: question.recommended,
    };
    if (cardIndex !== null) {
      payload.cardIndex = cardIndex;
      payload.cardSize = cardSize;
    }
    return { id, sourceId, payload };
  }

  /**
   * Asks one question on a card of its own — or in plain text when the deployment carries no
   * interactive frames — and resolves with the recorded answer.
   */
  private async askSingleQuestion(
    task: TaskRecord,
    target: ChatTarget,
    spec: AskSpec,
    signal: AbortSignal,
  ): Promise<{ id: string; questionId: string; answer: string }> {
    const { id, sourceId, payload } = spec;
    this.store.createQuestion({
      id,
      taskId: task.id,
      botUid: this.bot!.uid,
      ...target,
      payload: JSON.stringify(payload),
    });
    const completion = Promise.withResolvers<string>();
    void completion.promise.catch(() => {});
    this.questionWaiters.set(id, {
      taskId: task.id,
      resolve: completion.resolve,
      reject: completion.reject,
    });
    const abort = () => {
      this.store.cancelQuestion(id);
      this.questionWaiters.delete(id);
      completion.reject(new Error("question cancelled"));
    };
    signal.addEventListener("abort", abort, { once: true });
    const timer = setTimeout(() => {
      if (!this.store.expireQuestion(id)) return;
      this.questionTimers.delete(id);
      this.questionWaiters.delete(id);
      const expired = this.store.getQuestion(id);
      if (expired !== null) {
        this.rewriteAskCard(expired, { kind: "inactive", note: "问题已过期：未在 10 分钟内收到答案。" });
        this.forgetAskGeneration(expired.cardMessageId);
      }
      completion.reject(new Error("question timed out"));
    }, QUESTION_TIMEOUT_MS);
    this.questionTimers.set(id, timer);
    try {
      throwIfAborted(signal);
      await this.deliverQuestion(payload, target, id, task.id, signal);
      throwIfAborted(signal);
      const answer = await completion.promise;
      return { id: sourceId ?? id, questionId: id, answer };
    } finally {
      signal.removeEventListener("abort", abort);
      clearTimeout(timer);
      this.questionTimers.delete(id);
      this.questionWaiters.delete(id);
      this.forgetAskGeneration(this.store.getQuestion(id)?.cardMessageId ?? null);
      this.store.cancelQuestion(id);
    }
  }

  /**
   * Asks a whole set on one card: every question is created and waits at once, one submission
   * answers the questions it filled, and the result is produced once every question is settled.
   * A deployment that cannot carry the set frame — no interactive profile, a frame its limits
   * refuse, a failed send — falls back to asking the questions one at a time, so no answer is
   * ever lost to a card that could not be rendered.
   */
  private async askQuestionSet(
    task: TaskRecord,
    target: ChatTarget,
    specs: readonly AskSpec[],
    signal: AbortSignal,
  ): Promise<Array<{ id: string; questionId: string; answer: string }>> {
    const completions = specs.map(() => Promise.withResolvers<string>());
    const cleanups: Array<() => void> = [];
    specs.forEach((spec, index) => {
      this.store.createQuestion({
        id: spec.id,
        taskId: task.id,
        botUid: this.bot!.uid,
        ...target,
        payload: JSON.stringify(spec.payload),
      });
      const completion = completions[index]!;
      void completion.promise.catch(() => {});
      this.questionWaiters.set(spec.id, {
        taskId: task.id,
        resolve: completion.resolve,
        reject: completion.reject,
      });
      const abort = () => {
        this.store.cancelQuestion(spec.id);
        this.questionWaiters.delete(spec.id);
        completion.reject(new Error("question cancelled"));
      };
      signal.addEventListener("abort", abort, { once: true });
      cleanups.push(() => signal.removeEventListener("abort", abort));
      const timer = setTimeout(() => {
        if (!this.store.expireQuestion(spec.id)) return;
        this.questionTimers.delete(spec.id);
        this.questionWaiters.delete(spec.id);
        const expired = this.store.getQuestion(spec.id);
        if (expired !== null) {
          this.closeQuestionCard(expired, "问题已过期：未在 10 分钟内收到全部答案。");
        }
        completion.reject(new Error("question timed out"));
      }, QUESTION_TIMEOUT_MS);
      this.questionTimers.set(spec.id, timer);
    });
    try {
      throwIfAborted(signal);
      const messageId = await this.deliverQuestionSet(specs, target, signal);
      throwIfAborted(signal);
      if (messageId !== null) {
        this.store.setQuestionsCard(specs.map(({ id }) => id), messageId);
        const settled = await Promise.all(completions.map(({ promise }) => promise));
        return specs.map(({ id, sourceId }, index) => ({
          id: sourceId ?? id,
          questionId: id,
          answer: settled[index] ?? "",
        }));
      }
      // The set frame was refused: ask each question exactly like a single-question ask would. A
      // text reply can settle the remaining rows before their turn, so only still-open ones are
      // delivered and every resolved completion is read back in question order.
      const answers: Array<{ id: string; questionId: string; answer: string }> = [];
      for (const [index, spec] of specs.entries()) {
        if (this.store.getQuestion(spec.id)?.state === "pending") {
          await this.deliverQuestion(spec.payload, target, spec.id, task.id, signal);
        }
        const answer = await completions[index]!.promise;
        answers.push({ id: spec.sourceId ?? spec.id, questionId: spec.id, answer });
      }
      return answers;
    } finally {
      for (const spec of specs) {
        clearTimeout(this.questionTimers.get(spec.id));
        this.questionTimers.delete(spec.id);
        this.questionWaiters.delete(spec.id);
        this.forgetAskGeneration(this.store.getQuestion(spec.id)?.cardMessageId ?? null);
        this.store.cancelQuestion(spec.id);
      }
      for (const cleanup of cleanups) cleanup();
    }
  }

  /**
   * Sends the one card carrying a whole set. `null` means the deployment could not take it — no
   * interactive profile, a frame its limits refuse, or a failed send — and the caller asks the
   * questions one at a time instead.
   */
  private async deliverQuestionSet(
    specs: readonly AskSpec[],
    target: ChatTarget,
    signal: AbortSignal,
  ): Promise<string | null> {
    const entries: AskCardSetQuestion[] = [];
    const payloads: Record<string, unknown>[] = [];
    for (const spec of specs) {
      const ask = askQuestionFromPayload(spec.payload);
      if (ask === null) return null;
      entries.push({ question: ask, answer: null });
      payloads.push(spec.payload);
    }
    const frame = buildAskCardSet(entries, { kind: "open", generation: 1 }, this.formatQuestionSet(payloads));
    try {
      const receipt = await this.options.transport.sendCard(
        target,
        frame,
        randomUUID(),
        AbortSignal.any([signal, AbortSignal.timeout(ASK_CARD_SEND_TIMEOUT_MS)]),
      );
      return receipt?.messageId ?? null;
    } catch (error) {
      if (signal.aborted) throw error;
      console.error(
        `[ask-card] set send failed target=${targetKey(target)}：${safeErrorMessage(error, "send failed")}`,
      );
      return null;
    }
  }

  private formatQuestion(question: Record<string, unknown>): string {
    const entries = Array.isArray(question.options) ? question.options : [];
    // A question of a set is answered by the same rule as the whole set, even when its card was
    // refused and it ends up asked on its own: one text reply settles every open question.
    const rule = questionPosition(question) !== null
      ? "直接回复本消息即作为整组答案（一次回答全部问题）。"
      : entries.length === 0
        ? "直接回复即可，内容会作为答案返回。"
        : question.multi === true
          ? "直接回复编号（如 1,3）作为答案，或自填答案。"
          : "直接回复选项编号作为答案，或自填答案。";
    return `${this.questionPrompt(question)}${rule}`;
  }

  /**
   * The plain-text fallback of a set card: one block per question, then how a text reply counts
   * for the whole set. A reply is the answer of every open question at once.
   */
  private formatQuestionSet(payloads: readonly Record<string, unknown>[]): string {
    const blocks = payloads.map((payload) => this.questionPrompt(payload));
    return `${blocks.join("\n")}直接回复本消息即作为整组答案（一次回答全部问题）；也可以点击卡片一次提交。`;
  }

  /** One question block: its heading, numbered options and, with them, their descriptions. */
  private questionPrompt(question: Record<string, unknown>): string {
    const entries = Array.isArray(question.options) ? question.options : [];
    const list = entries
      .map((option, index) => {
        if (!option || typeof option !== "object" || Array.isArray(option)) return `${index + 1}.`;
        const value = option as Record<string, unknown>;
        const label = typeof value.label === "string" ? value.label : "";
        const description = typeof value.description === "string" ? ` — ${value.description}` : "";
        return `${index + 1}. ${label}${description}`;
      })
      .join("\n");
    const position = questionPosition(question);
    const heading = position === null ? "问题：" : `问题 ${position.index}/${position.size}：`;
    return `${heading}${question.question as string}\n${list}${list ? "\n" : ""}`;
  }

  private rejectQuestionsForTask(taskId: string, error: Error): void {
    for (const [id, waiter] of this.questionWaiters) {
      if (waiter.taskId !== taskId) continue;
      this.questionWaiters.delete(id);
      clearTimeout(this.questionTimers.get(id));
      this.questionTimers.delete(id);
      this.forgetAskGeneration(this.store.getQuestion(id)?.cardMessageId ?? null);
      waiter.reject(error);
    }
  }

  /**
   * Posts one question: the interactive card when the deployment carries interactive frames,
   * otherwise the plain text the channel has always sent. Both carry the same question text, so a
   * client that cannot render the card shows the question instead of nothing.
   */
  private async deliverQuestion(
    payload: Record<string, unknown>,
    target: ChatTarget,
    questionId: string,
    taskId: string,
    signal: AbortSignal,
  ): Promise<void> {
    const ask = askQuestionFromPayload(payload);
    if (ask !== null) {
      try {
        const receipt = await this.options.transport.sendCard(
          target,
          buildAskCard(ask, { kind: "pending", generation: 1 }, this.formatQuestion(payload)),
          randomUUID(),
          AbortSignal.any([signal, AbortSignal.timeout(ASK_CARD_SEND_TIMEOUT_MS)]),
        );
        if (receipt?.messageId !== undefined) {
          this.store.setQuestionCard(questionId, receipt.messageId);
          return;
        }
      } catch (error) {
        if (signal.aborted) throw error;
        console.error(`[ask-card] send failed target=${targetKey(target)}：${safeErrorMessage(error, "send failed")}`);
      }
    }
    this.enqueueText(target, this.formatQuestion(payload), taskId);
  }

  /**
   * Records one answer durably and releases the waiting task. The compare-and-set on the question
   * row is the single arbitration point between a text reply and a card click: whoever loses it
   * must not answer a second time.
   */
  private applyQuestionAnswer(question: QuestionRecord, answer: string): boolean {
    const botUid = this.bot?.uid;
    if (!botUid) return false;
    if (!this.store.answerQuestion(question.id, botUid, question, answer)) return false;
    this.releaseQuestion(question.id, answer);
    this.forgetAskGeneration(question.cardMessageId);
    this.rewriteAskCard(question, { kind: "answered", answer });
    return true;
  }

  /** Releases the waiting task of one recorded answer; the store CAS already decided it landed. */
  private releaseQuestion(questionId: string, answer: string): void {
    const waiter = this.questionWaiters.get(questionId);
    if (!waiter) return;
    this.questionWaiters.delete(questionId);
    clearTimeout(this.questionTimers.get(questionId));
    this.questionTimers.delete(questionId);
    waiter.resolve(answer);
  }

  /**
   * Rewrites one question card. Best-effort by contract: the state it renders is already durable
   * in the question row, so a failed edit changes nothing but the card's appearance.
   */
  private rewriteAskCard(question: QuestionRecord, state: AskCardState, hint?: string): void {
    const messageId = question.cardMessageId;
    if (messageId === null) return;
    const payload = parseQuestionPayload(question);
    const ask = payload === null ? null : askQuestionFromPayload(payload);
    if (ask === null) return;
    const note = state.kind === "answered"
      ? `已回复：${askAnswerText(ask, state.answer)}`
      : state.kind === "inactive"
        ? state.note
        : hint;
    const base = this.formatQuestion(payload ?? {});
    const frame = buildAskCard(ask, state, note === undefined ? base : `${base}\n${note}`);
    this.submitAskCardEdit(question, messageId, frame);
  }

  /**
   * Rewrites the shared card of a multi-question ask: answered questions show their answer, the
   * rest keep their inputs while the frame is open, and a closed frame carries none. Best-effort
   * like every card rewrite; `hint` is appended to the fallback text of this edit only.
   */
  private rewriteAskCardSet(
    rows: readonly QuestionRecord[],
    state: AskCardSetState,
    hint: string | null,
  ): void {
    const first = rows[0];
    const messageId = first?.cardMessageId ?? null;
    if (first === undefined || messageId === null) return;
    const entries: AskCardSetQuestion[] = [];
    const payloads: Record<string, unknown>[] = [];
    for (const row of rows) {
      const payload = parseQuestionPayload(row);
      const ask = payload === null ? null : askQuestionFromPayload(payload);
      if (payload === null || ask === null) return;
      entries.push({ question: ask, answer: row.state === "answered" ? row.answer ?? "" : null });
      payloads.push(payload);
    }
    const base = this.formatQuestionSet(payloads);
    const note = hint ?? (state.kind === "closed" ? state.note : null);
    const frame = buildAskCardSet(entries, state, note === null ? base : `${base}\n${note}`);
    this.submitAskCardEdit(first, messageId, frame);
  }

  /** Sends one card edit with the next sequence; failures are logged and never gate an answer. */
  private submitAskCardEdit(target: ChatTarget, messageId: string, frame: AskCardFrame): void {
    const seq = (this.askCardSeq.get(messageId) ?? 1) + 1;
    this.askCardSeq.set(messageId, seq);
    const operation = this.options.transport
      .editCard(target, messageId, frame, seq, AbortSignal.timeout(ASK_CARD_EDIT_TIMEOUT_MS))
      .catch((error: unknown) => {
        console.error(
          `[ask-card] edit failed target=${targetKey(target)}：${safeErrorMessage(error, "edit failed")}`,
        );
      });
    this.operationSet(operation);
  }

  /**
   * A click that must not answer leaves the question open. The server accepts one click per
   * `(message, action, operator)`, so answering it later needs a frame whose action ids differ —
   * this is where the next generation comes from.
   */
  private retryAskCard(question: QuestionRecord, generation: number, hint: string): void {
    const messageId = question.cardMessageId;
    if (messageId !== null) {
      this.rewriteAskCard(question, { kind: "pending", generation: this.nextAskGeneration(messageId, generation) }, hint);
    }
    this.enqueueText(question, hint, null);
  }

  /** The set-frame twin of `retryAskCard`: one fresh generation for the whole card, one hint. */
  private retryAskCardSet(rows: readonly QuestionRecord[], generation: number, hint: string): void {
    const first = rows[0];
    const messageId = first?.cardMessageId ?? null;
    if (first === undefined || messageId === null) return;
    this.rewriteAskCardSet(rows, { kind: "open", generation: this.nextAskGeneration(messageId, generation) }, hint);
    this.enqueueText(first, hint, null);
  }

  /**
   * The generation the next frame of this card must carry: strictly above the click that just
   * arrived and every id the card issued before, because a consumed id is never accepted again.
   */
  private nextAskGeneration(messageId: string, seen: number): number {
    const next = Math.max(seen, this.askCardGeneration.get(messageId) ?? 0) + 1;
    this.askCardGeneration.set(messageId, next);
    return next;
  }

  /** Drops the action-id generation of a card that reached its end. */
  private forgetAskGeneration(cardMessageId: string | null): void {
    if (cardMessageId !== null) this.askCardGeneration.delete(cardMessageId);
  }

  /**
   * Closes the card one question belongs to: a set card gets a single rewrite covering every row
   * it carries, a single card its own closed frame, and a question that was asked in plain text
   * has no card to rewrite at all.
   */
  private closeQuestionCard(question: QuestionRecord, note: string): void {
    const botUid = this.bot?.uid;
    const messageId = question.cardMessageId;
    if (botUid === undefined || messageId === null) return;
    this.forgetAskGeneration(messageId);
    const rows = this.store.listQuestionsByCard(botUid, question, messageId);
    if (rows.length <= 1) {
      this.rewriteAskCard(question, { kind: "inactive", note });
      return;
    }
    this.rewriteAskCardSet(rows, { kind: "closed", note }, null);
  }

  /**
   * Applies one card click. Only the owner's click can answer; a foreign operator, an unknown
   * action id or a card whose question already closed is acknowledged and dropped so the event
   * queue keeps moving.
   */
  private async answerQuestionFromCard(action: BotCardAction): Promise<BotEventOutcome> {
    const botUid = this.bot?.uid;
    if (!botUid || this.stopping) return "retry";
    const ownerId = this.store.getOwnerId(botUid)?.trim() ?? "";
    if (ownerId.length === 0 || action.operatorUid !== ownerId) return "ignored";
    const target: ChatTarget = { channelType: action.channelType, channelId: action.channelId };
    let rows: QuestionRecord[];
    try {
      rows = this.store.listQuestionsByCard(botUid, target, action.messageId);
    } catch {
      return "retry";
    }
    const first = rows[0];
    if (first === undefined) return "ignored";
    if (rows.length === 1) return this.answerSingleCardClick(first, action);
    return this.answerSetCardClick(target, rows, action);
  }

  /**
   * Applies one click on a single-question card. An answered question can only be reached by a
   * replay of the click that answered it (the rewrite removed its buttons), so it stays silent; a
   * question that never got an answer is worth telling the operator about.
   */
  private answerSingleCardClick(question: QuestionRecord, action: BotCardAction): BotEventOutcome {
    if (question.state === "answered") return "handled";
    if (question.state !== "pending") {
      this.enqueueText(question, "该问题已失效，本次点击未作为答案。", null);
      return "handled";
    }
    const ref = parseAskActionId(action.actionId);
    const payload = parseQuestionPayload(question);
    const ask = payload === null ? null : askQuestionFromPayload(payload);
    if (ref === null || ask === null) return "handled";
    const resolved = resolveAskAnswer(ask, ref, action.inputs);
    if (resolved.kind === "invalid") {
      this.retryAskCard(question, ref.generation, "未收到内容：请填写后重新提交，或直接回复本消息作答。");
      return "handled";
    }
    let answer: string;
    try {
      answer = this.normalizeAnswer(question, resolved.answer);
    } catch {
      this.retryAskCard(question, ref.generation, "答案无效：请重新选择，或直接回复本消息作答。");
      return "handled";
    }
    try {
      this.applyQuestionAnswer(question, answer);
    } catch {
      return "retry";
    }
    return "handled";
  }

  /**
   * Applies one submission of a multi-question card: every question the submission filled is
   * compare-and-set in a single transaction, the questions it left empty stay open, and the card
   * is rewritten once — closed when nothing waits anymore, otherwise re-rendered behind fresh
   * action ids. A replay of an already consumed submission changes nothing.
   */
  private answerSetCardClick(
    target: ChatTarget,
    rows: readonly QuestionRecord[],
    action: BotCardAction,
  ): BotEventOutcome {
    const pending = rows.filter(({ state }) => state === "pending");
    if (pending.length === 0) {
      if (!rows.every(({ state }) => state === "answered")) {
        this.enqueueText(target, "该问题已失效，本次点击未作为答案。", null);
      }
      return "handled";
    }
    const ref = parseAskActionId(action.actionId);
    if (ref === null || ref.kind !== "submit") return "handled";
    const filled: Array<{ id: string; answer: string }> = [];
    for (const question of pending) {
      const payload = parseQuestionPayload(question);
      const ask = payload === null ? null : askQuestionFromPayload(payload);
      const position = payload === null ? null : questionPosition(payload);
      if (ask === null || position === null) continue;
      const answer = resolveAskSetAnswer(ask, position.index, action.inputs);
      if (answer !== null) filled.push({ id: question.id, answer });
    }
    if (filled.length === 0) {
      this.retryAskCardSet(rows, ref.generation, "未收到内容：请至少填写一题后重新提交，或直接回复本消息作答。");
      return "handled";
    }
    let landed: string[];
    try {
      landed = this.store.answerQuestions(this.bot!.uid, target, filled);
    } catch {
      return "retry";
    }
    const byId = new Map(filled.map(({ id, answer }) => [id, answer]));
    for (const id of landed) this.releaseQuestion(id, byId.get(id) ?? "");
    const fresh = this.store.listQuestionsByCard(this.bot!.uid, target, action.messageId);
    if (fresh.some(({ state }) => state === "pending")) {
      this.rewriteAskCardSet(
        fresh,
        { kind: "open", generation: this.nextAskGeneration(action.messageId, ref.generation) },
        null,
      );
    } else {
      this.forgetAskGeneration(action.messageId);
      this.rewriteAskCardSet(fresh, { kind: "closed", note: null }, null);
    }
    return "handled";
  }

  /**
   * Consumes this Bot's event queue for the life of the process: long poll, apply, acknowledge,
   * advance the cursor. Delivery is at-least-once, so an event whose acknowledgement was lost is
   * replayed and the answer compare-and-set turns the replay into a no-op.
   */
  private startBotEventLoop(): void {
    if (this.eventLoop) return;
    const controller = new AbortController();
    this.eventAbort = controller;
    this.eventLoop = this.runBotEventLoop(controller.signal).catch((error: unknown) => {
      console.error(`[bot-events] poller stopped：${safeErrorMessage(error, "poll failed")}`);
    });
  }

  private async runBotEventLoop(signal: AbortSignal): Promise<void> {
    const botUid = this.bot?.uid;
    if (!botUid) return;
    let cursor = this.store.getBotEventCursor(botUid);
    while (!signal.aborted && !this.stopping) {
      let events: BotEvent[] | null;
      try {
        events = await this.options.transport.fetchBotEvents({
          afterEventId: cursor,
          limit: BOT_EVENT_POLL_LIMIT,
          waitSeconds: BOT_EVENT_POLL_WAIT_SECONDS,
          signal,
        });
      } catch (error) {
        if (signal.aborted || this.stopping) return;
        console.error(`[bot-events] poll failed：${safeErrorMessage(error, "poll failed")}`);
        await waitFor(BOT_EVENT_POLL_BACKOFF_MS, signal);
        continue;
      }
      // A deployment without an event endpoint answers once and is never asked again.
      if (events === null) return;
      for (const event of events) {
        if (signal.aborted || this.stopping) return;
        const outcome = event.kind === "card_action"
          ? await this.answerQuestionFromCard(event.action).catch((): BotEventOutcome => "retry")
          : "ignored";
        if (outcome === "retry") {
          const attempts = (this.botEventAttempts.get(event.eventId) ?? 0) + 1;
          this.botEventAttempts.set(event.eventId, attempts);
          // Keep the cursor: the next round re-reads this event from the same place.
          if (attempts < BOT_EVENT_MAX_ATTEMPTS) break;
          console.error(`[bot-events] dropped event ${event.eventId} after ${attempts} attempts`);
        }
        try {
          await this.options.transport.ackBotEvent(event.eventId, signal);
        } catch (error) {
          if (signal.aborted || this.stopping) return;
          console.error(`[bot-events] ack failed：${safeErrorMessage(error, "ack failed")}`);
          return;
        }
        this.botEventAttempts.delete(event.eventId);
        cursor = Math.max(cursor, event.eventId);
        try {
          this.store.setBotEventCursor(botUid, cursor);
        } catch {
          console.error("[bot-events] cursor write failed");
        }
      }
      if (events.length === 0) await waitFor(BOT_EVENT_POLL_IDLE_MS, signal);
    }
  }

  /** Tracks a background operation so shutdown waits for it without blocking a caller. */
  private operationSet(operation: Promise<void>): void {
    this.activityOperations.add(operation);
    void operation.finally(() => this.activityOperations.delete(operation));
  }
  private releaseTextAbortIfIdle(taskId: string): void {
    if (!this.bot) return;
    const task = this.store.getTask(taskId);
    if (
      !task ||
      task.state === "queued" ||
      task.state === "running" ||
      this.store.listPendingOutbox(this.bot.uid).some((item) => item.taskId === taskId)
    ) {
      return;
    }
    this.textAbortControllers.delete(taskId);
    const streamMarker = `:${taskId}:`;
    for (const [key, timer] of this.streamTimers) {
      if (!key.includes(streamMarker)) continue;
      clearTimeout(timer);
      this.streamTimers.delete(key);
    }
    for (const key of this.streamNotBefore.keys()) {
      if (key.includes(streamMarker)) this.streamNotBefore.delete(key);
    }
  }

  private enqueueStreamText(target: ChatTarget, taskId: string, event: TextEvent): void {
    if (!this.bot) return;
    const streamMessageId = `${taskId}:${event.messageId}`;
    const previous = this.store.listTextStream(this.bot.uid, taskId, streamMessageId);
    const version = previous.reduce(
      (highest, item) => Math.max(highest, item.desiredVersion),
      0,
    ) + 1;
    const chunks = event.text.length === 0 ? [] : splitUtf8(event.text);
    const segmentCount = Math.max(previous.length, chunks.length);
    let changed = false;
    for (let segmentIndex = 0; segmentIndex < segmentCount; segmentIndex += 1) {
      const chunk = chunks[segmentIndex] ?? "";
      const existing = previous[segmentIndex];
      if (existing) {
        if (existing.text === chunk) continue;
        if (this.store.updateTextOutbox(existing.id, chunk, version)) changed = true;
        continue;
      }
      if (chunk.length === 0) continue;
      this.store.enqueueOutbox({
        id: randomUUID(),
        botUid: this.bot.uid,
        channelId: target.channelId,
        channelType: target.channelType,
        taskId,
        kind: "text",
        text: chunk,
        path: null,
        fileKind: null,
        clientMsgNo: randomUUID(),
        streamMessageId,
        segmentIndex,
        desiredVersion: version,
        ackedVersion: 0,
      });
      changed = true;
    }
    if (!changed) return;
    if (previous.length > 0) {
      this.scheduleStreamFlush(target, streamMessageId);
    } else {
      void this.flushTarget(target).catch(() => {});
    }
  }

  private scheduleStreamFlush(target: ChatTarget, streamMessageId: string): void {
    const key = streamTargetKey(target, streamMessageId);
    if (this.streamTimers.has(key) || !this.started) return;
    const notBefore = this.streamNotBefore.get(key) ?? Date.now();
    const timer = setTimeout(() => {
      this.streamTimers.delete(key);
      void this.flushTarget(target).catch(() => {});
    }, Math.max(0, notBefore - Date.now()));
    this.streamTimers.set(key, timer);
  }

  private canSendStreamEdit(item: OutboxRecord, target: ChatTarget): boolean {
    if (!item.streamMessageId || !item.remoteMessageId) return true;
    const key = streamTargetKey(target, item.streamMessageId);
    const notBefore = this.streamNotBefore.get(key) ?? 0;
    if (Date.now() >= notBefore) return true;
    this.scheduleStreamFlush(target, item.streamMessageId);
    return false;
  }

  private markStreamEditStarted(item: OutboxRecord, target: ChatTarget): void {
    if (!item.streamMessageId) return;
    const key = streamTargetKey(target, item.streamMessageId);
    this.streamNotBefore.set(
      key,
      Math.max(this.streamNotBefore.get(key) ?? 0, Date.now() + TEXT_MERGE_MS),
    );
  }

  private enqueueText(target: ChatTarget, text: string, taskId: string | null): void {
    for (const chunk of splitUtf8(text)) {
      this.store.enqueueOutbox({
        id: randomUUID(),
        botUid: this.bot!.uid,
        channelId: target.channelId,
        channelType: target.channelType,
        taskId,
        kind: "text",
        text: chunk,
        path: null,
        fileKind: null,
        clientMsgNo: randomUUID(),
      });
    }
    this.flushTarget(target);
  }

  private flushAllOutbox(): void {
    if (!this.bot) return;
    for (const target of this.store.listPendingOutboxTargets(this.bot.uid)) {
      const key = targetKey(target);
      clearTimeout(this.retryTimers.get(key));
      this.retryTimers.delete(key);
      const flush = this.flushTarget(target, true);
      void flush.then(
        () => {
          void this.flushTarget(target).catch(() => {});
        },
        () => {
          void this.flushTarget(target).catch(() => {});
        },
      );
    }
  }
  private flushTarget(target: ChatTarget, announceUnknown = false): Promise<void> {
    if (!this.bot || !this.started) return Promise.resolve();
    const key = targetKey(target);
    if (this.outboxActive.has(key)) return this.outboxFlushes.get(key) ?? Promise.resolve();
    this.outboxActive.add(key);
    const drain = this.flushTargetInternal(target, announceUnknown, key);
    this.outboxFlushes.set(key, drain);
    void drain.catch(() => {}).then(() => {
      if (this.outboxFlushes.get(key) === drain) this.outboxFlushes.delete(key);
    });
    return drain;
  }

  private async flushTargetInternal(
    target: ChatTarget,
    announceUnknown: boolean,
    key: string,
  ): Promise<void> {
    let unknownNoticeSent = false;
    try {
      while (this.started && this.bot) {
        const item = this.store.listPendingOutbox(this.bot.uid, target)[0];
        if (!item) return;
        if (
          item.streamMessageId &&
          !item.remoteMessageId &&
          item.state === "pending" &&
          item.attempts === 0 &&
          (item.text ?? "") === ""
        ) {
          this.store.acknowledgeTextOutbox(item.id, item.desiredVersion, null);
          if (item.taskId) this.releaseTextAbortIfIdle(item.taskId);
          continue;
        }
        if (
          item.streamMessageId &&
          item.remoteMessageId &&
          !this.canSendStreamEdit(item, target)
        ) {
          return;
        }
        if (
          announceUnknown &&
          item.state === "unknown" &&
          !item.streamMessageId &&
          !unknownNoticeSent
        ) {
          unknownNoticeSent = true;
          await this.sendUnknownNotice(target, item);
        }
        try {
          const delivery = await this.sendOutbox(item, target);
          if (item.streamMessageId) {
            if (delivery.reconcile && delivery.remoteMessageId) {
              this.store.bindTextOutbox(item.id, delivery.remoteMessageId);
              const bound = this.store.getOutbox(item.id);
              if (bound?.remoteMessageId) {
                const correction = await this.sendOutbox(bound, target);
                this.store.acknowledgeTextOutbox(
                  bound.id,
                  correction.attemptedVersion,
                  correction.remoteMessageId,
                );
              }
              if (item.taskId) this.releaseTextAbortIfIdle(item.taskId);
              if (
                this.store.listPendingOutbox(this.bot.uid, target).some(
                  (pending) => pending.streamMessageId === item.streamMessageId,
                )
              ) {
                this.scheduleStreamFlush(target, item.streamMessageId);
              }
              continue;
            }
            this.store.acknowledgeTextOutbox(item.id, delivery.attemptedVersion, delivery.remoteMessageId);
            if (item.taskId) this.releaseTextAbortIfIdle(item.taskId);
          } else {
            this.store.markOutboxSent(item.id);
            if (item.taskId) this.releaseTextAbortIfIdle(item.taskId);
          }
        } catch (error) {
          if (item.streamMessageId) {
            this.store.failTextOutbox(item.id, item.desiredVersion, "transport unavailable; delivery is unknown");
          } else {
            this.store.markOutboxFailure(item.id, "transport unavailable; delivery is unknown");
          }
          if (item.kind === "file" && isPermanentOutboxError(error, item)) {
            this.store.markOutboxDead(item.id, "file delivery failed permanently");
            await this.enqueueText(target, "文件发送失败，未阻塞后续文字回复。", item.taskId);
            continue;
          }
          this.scheduleOutboxRetry(target);
          return;
        }
      }
    } finally {
      // Release the drain slot synchronously, before this promise settles: an item enqueued
      // after the last queue read must start a fresh drain instead of waiting for an unrelated
      // flush trigger.
      this.outboxActive.delete(key);
    }
  }

  private scheduleOutboxRetry(target: ChatTarget): void {
    const key = targetKey(target);
    if (this.retryTimers.has(key) || !this.started) return;
    const timer = setTimeout(() => {
      this.retryTimers.delete(key);
      this.flushTarget(target);
    }, 1_000);
    this.retryTimers.set(key, timer);
  }

  private async sendUnknownNotice(target: ChatTarget, item: OutboxRecord): Promise<void> {
    try {
      await this.options.transport.sendText(
        target,
        "上一条消息送达状态未知，正在重试。",
        `delivery-unknown-${item.id}`,
      );
    } catch {
      // The notice is intentionally best effort and never creates another outbox item.
    }
  }

  private async withTextAttempt<T>(
    taskId: string | null,
    work: (signal: AbortSignal) => Promise<T>,
  ): Promise<T> {
    const attempt = new AbortController();
    const parent = taskId ? this.textAbortControllers.get(taskId) : undefined;
    const abort = () => attempt.abort();
    if (parent?.signal.aborted) attempt.abort();
    else parent?.signal.addEventListener("abort", abort, { once: true });
    this.textAttempts.add(attempt);
    try {
      return await work(attempt.signal);
    } finally {
      this.textAttempts.delete(attempt);
      parent?.signal.removeEventListener("abort", abort);
    }
  }

  private async sendOutbox(
    item: OutboxRecord,
    target: ChatTarget,
  ): Promise<{ remoteMessageId: string | null; reconcile: boolean; attemptedVersion: number }> {
    if (item.kind === "file") {
      if (!item.path || !item.fileKind) throw new Error("invalid file outbox item");
      try {
        await stat(item.path);
      } catch (error) {
        if (asAppError(error).code === "ENOENT") {
          const missing = new Error("outbox file spool is missing") as AppError & { permanent?: boolean };
          missing.code = "ENOENT";
          missing.permanent = true;
          throw missing;
        }
        throw error;
      }
      await this.options.transport.sendFile(target, item.path, item.fileKind, item.clientMsgNo);
      return { remoteMessageId: null, reconcile: false, attemptedVersion: 0 };
    }
    if (item.streamMessageId) {
      if (item.remoteMessageId) {
        this.markStreamEditStarted(item, target);
        await this.withTextAttempt(item.taskId, (signal) =>
          this.options.transport.editText(target, item.remoteMessageId!, item.text ?? "", signal),
        );
        return {
          remoteMessageId: item.remoteMessageId,
          reconcile: false,
          attemptedVersion: item.desiredVersion,
        };
      }
      const reconcile = item.state === "unknown";
      if (!reconcile) this.store.beginTextCreate(item.id);
      this.markStreamEditStarted(item, target);
      const receipt = await this.withTextAttempt(item.taskId, (signal) =>
        this.options.transport.sendText(target, item.text ?? "", item.clientMsgNo, signal),
      );
      if (!receipt?.messageId) throw new Error("stream text send did not return a remote message id");
      return { remoteMessageId: receipt.messageId, reconcile, attemptedVersion: item.desiredVersion };
    }
    await this.options.transport.sendText(target, item.text ?? "", item.clientMsgNo);
    return { remoteMessageId: null, reconcile: false, attemptedVersion: 0 };
  }
}

export async function createChannelService(options: ChannelServiceOptions): Promise<ChannelService> {
  const commandHandlers = normalizeCommandHandlers(options.commandHandlers);
  const store = await openChannelStore(options.config.stateDir);
  return new CoreService(options, store, commandHandlers);
}

export { splitUtf8, resolveWorkDirectory, GROUP_TRUSTED_INSTRUCTION };
