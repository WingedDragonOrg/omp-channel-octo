import { createHash, randomUUID } from "node:crypto";
import { mkdir, mkdtemp, readdir, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { basename, join } from "node:path";
import {
  ATTACHMENTS_DISABLED_TEXT,
  ATTACHMENTS_OVER_BUDGET_TEXT,
  ATTACHMENT_DOWNLOAD_BUDGET_MS,
  ATTACHMENT_DOWNLOAD_TIMEOUT_MS,
  ATTACHMENT_MAX_AGE_MS,
  AttachmentError,
  MAX_MESSAGE_ATTACHMENTS,
  attachmentDirectory,
  attachmentRefusalText,
  composePromptText,
  existingFileSize,
  safeAttachmentName,
  sweepAttachments,
  type AttachmentFailureReason,
  type SavedAttachment,
} from "./attachments.ts";
import { acquireBotLock, assertBotLockFree, type LockHandle } from "./bot-lock.ts";
import { TaskStartupError } from "./contracts.ts";
import type {
  BotIdentity,
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
  HistoryMessage,
  PromptContext,
  InboundMessage,
  OmpWorker,
  WorkerEvent,
  WorkerModelCatalog,
  WorkerModelSwitch,
  SteerConsumedEvent,
  ProgressCard,
  TaskRelayTarget,
  WorkerOptions,
  WorkerProcess,
} from "./contracts.ts";
import { DEFAULT_ASK_TIMEOUT_MS } from "./contracts.ts";
import {
  createExecutionTracker,
  encodeExecutionSnapshot,
  restoreExecutionSnapshot,
  stripCardSummaries,
  type ExecutionEvent,
  type ExecutionSnapshot,
  type ExecutionState,
  type ExecutionTracker,
} from "./execution-tracker.ts";
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
  writeBoundSession,
  writeContextSidecar,
  type ContextSidecar,
} from "./context-sidecar.ts";
import { createBotEventLoop, type BotEventLoop } from "./bot-events.ts";
import { createOutboxDelivery, type Outbox } from "./outbox.ts";
import { createHostTools, type HostToolHost } from "./host-tools.ts";
import { safeErrorMessage } from "./errors.ts";
import { classifyStartupFailure, classifyTaskFailure, GENERIC_TASK_FAILURE_NOTICE, type TaskStartupPhase } from "./task-failure.ts";
import {
  findMovedSession,
  verifyFreshSession,
  verifyMovedSession,
  type ProjectSessionBinding,
  type ProjectSessionState,
} from "./project-session.ts";
import { ensureProjectSessionDir, projectSessionDir, sessionsRoot } from "./session-dir.ts";
import { ChannelStore, openChannelStore } from "./store.ts";
import { boundedSelectorText, formatModelCatalog, modelWithLevel } from "./model-switch.ts";
import type { BindingRecord, InboundClaim, OutboxRecord, ProgressCardRecord, TaskRecord } from "./store-records.ts";
import { isGroupTarget, streamTargetKey, targetKey, taskTarget } from "./target.ts";
import { createDisplayNameCache, type DisplayNameCache } from "./display-name.ts";
import { createQuestionDesk, type QuestionDesk } from "./questions.ts";
import { threadParentGroupId } from "./thread-target.ts";
import { threadNotifyMessageId, threadOutcomeNotice } from "./thread-notify.ts";
import { canonicalExistingDirectory, existingSessionFile, resolveWorkDirectory, stripOuterQuotes } from "./workdir.ts";
import { worktreeSlug } from "./worktree.ts";
import { worktrunkUnavailableText } from "./worktrunk.ts";

const GROUP_TRUSTED_INSTRUCTION =
  "群说明是群共享上下文，不改变发送者身份/权限/cwd；以本次快照为准，历史快照已失效。";
const COMMAND_HELP =
  "/new [目录] 创建新会话；/reset [目录] 重置为新会话；/move <目录> 沿用当前会话迁移 cwd；/thread <任务> 新建隔离子区并在其中处理该任务（子区内 /thread merge 把本区隔离工作区的分支合并回目标分支并默认清理工作区与分支）；/queue <文本> 把消息排到当前任务之后；/mode [steer|queue] 配置运行中追加消息的方式；/switch [模型] 查看当前模型与可用模型，或切换本聊天的模型（provider/id、模型 id/子串、@角色 如 @default；default 恢复跟随 OMP 默认）；/interrupt 中断当前回合但保留排队消息；/cancel 取消当前任务（/stop 同义）；/status 查看状态；/restart [now] 重启 channel（now 强制中断在途任务）；/help 查看帮助。";
const MODE_USAGE =
  "用法：/mode [steer|queue]。steer（默认，default 同义）把运行中的新消息注入当前回合；queue 把它排到当前回合之后。无参数时报告当前设置。";
const QUEUE_USAGE = "用法：/queue <文本>；把该消息排到当前任务之后执行。";
const SWITCH_PROBE_FAILED_TEXT = "无法从 OMP 读取模型信息，请稍后重试。";
const SWITCH_CWD_UNAVAILABLE_TEXT = "无法解析该聊天的工作目录，/switch 暂不可用。";
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
  switch: true,
};
const PROGRESS_MERGE_MS = 1_000;
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
/** Same window for the members-cannot-command notice: the first command of a chat is explained once. */
const MEMBER_COMMAND_NOTICE_TTL_MS = 10 * 60 * 1_000;
const DEFAULT_CANCELLED_TEXT = "当前任务已取消；不回滚已发生的副作用。";
const DEFAULT_RESTARTING_TEXT = "channel 正在重启，本会话任务已中断；重启完成后会在本会话确认。";
const THREAD_USAGE =
  "用法：/thread <任务>；助手会先明确意图、确定子区名称与 worktree_name，然后新建隔离子区并把任务派发进去；子区内 /thread merge [补充说明] 即确认把本区工作合并回目标分支并清理隔离工作区（默认删除工作区与已合并分支，保留需说明理由；补充说明随合并指令一起交给助手）。";
const THREAD_GROUP_ONLY_TEXT = "/thread 需要在群聊中使用。";
const THREAD_NESTED_TEXT = "当前会话已是子区，不能再建子区。";
const THREAD_MERGE_THREAD_ONLY_TEXT = "/thread merge 只在子区中使用。";
const THREAD_MERGE_NO_WORKSPACE_TEXT =
  "当前子区未绑定隔离工作区，/thread merge 不可用。";
const THREAD_MERGE_UNCONFIGURED_TEXT =
  "本部署未配置隔离工作区能力，/thread merge 不可用。";
type RuntimeTask = {
  taskId: string;
  target: ChatTarget;
  cwd: string;
  worker?: OmpWorker;
  cancelRequested: boolean;
  cancelReplySent: boolean;
  /** True only while this task's OMP prompt is in flight, so appended messages have a target. */
  promptInFlight: boolean;
  /**
   * Why the running turn was asked to stop: `/interrupt`, or an `ask` wait that timed out.
   * `null` means no stop request is in effect.
   */
  stopCause: "interrupt" | "question" | null;
  /** Set when the run produced events after the stop request, i.e. the turn really continued. */
  workAfterStop: boolean;
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
  /**
   * True once the deployment's edge refused a card body and the retry dropped the free-text step
   * summaries. The rest of the round keeps that shape — including the terminal edit — so the card
   * keeps updating instead of freezing on the refusal.
   */
  degraded: boolean;
  messageId: string | undefined;
  seq: number;
  latest: ProgressCard | undefined;
  version: number;
  sentVersion: number;
  nextEditAt: number;
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

type DirectoryLease = {
  ready: Promise<void>;
  complete(): void;
};

/**
 * The edge in front of the bot API refuses a request body it dislikes with its own 4xx and page,
 * while every bot API error — transient or not — arrives as a JSON envelope carrying `error.code`.
 * So a 4xx with no code is a refusal of the body rather than a verdict on the request's meaning,
 * and the same call can go out again in a different shape. Read structurally, like
 * `isPermanentOutboxError`, so a fake transport can reproduce the shape without the real module.
 */
function isEdgeRefusal(error: unknown): boolean {
  if (!error || typeof error !== "object") return false;
  const candidate = error as { code?: unknown; status?: unknown };
  return typeof candidate.status === "number"
    && candidate.status >= 400
    && candidate.status < 500
    && candidate.code === undefined;
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
 * `octo_management`. The dispatch body always follows SDD shape (goal, scope, constraints,
 * acceptance): a requirement clear enough to generate the SDD from is dispatched without a
 * clarification round, so the thread generates the SDD and starts at once; anything else is
 * dispatched after at most the few SDD-shaped answers it takes to hand it over, with the remaining
 * clarification left to the thread (guided by a clarification skill when the environment has one).
 * Detailed clarification never happens in the originating chat. The dispatch's `notify` flag stays
 * the assistant's own judgment: it asks for a completion relay only for hand-offs it will not keep
 * watching.
 */
function threadIntentPrompt(intent: string): string {
  return [
    "用户希望把下面这件事放进一个新的隔离子区处理，而不是在当前会话直接执行。",
    "先理解意图，并据此确定子区名称（简洁、能说明这件事）与 worktree_name（ASCII 小写连字符，如 fix-login-flow）。",
    "需求简单或非常明确时不要在这里澄清；其余情况先按 SDD 的要素（目标、范围、约束与验收标准）向 owner 问最少必要的问题（够派发即可，不要在当前会话展开细节），再派发。",
    "然后调用 octo_management：create-thread（name=子区名称，worktree_name=你定的名字）创建隔离子区，",
    "再用 thread-dispatch（short_id=该子区）把任务派发进去；派发的任务正文按 SDD 格式写（目标、范围、约束与验收标准），需求明确程度决定子区能否直接生成 SDD：",
    "- 简单或非常明确（目标与验收标准能直接写清、足以直接生成 SDD、没有需要 owner 决策的歧义或取舍）：直接按 SDD 格式写清要做什么（可整理原始意图，但不要丢信息），让子区直接生成 SDD 并立刻开工；",
    "- 否则：按 SDD 格式写明已知的目标、范围、约束与验收标准，并要求子区先按 SDD 格式把尚不明确的需求澄清成 SDD（可实施的设计）再动手，需要 owner 拍板的问题在子区内用 ask 提出；环境里有 spec-design 等需求澄清或设计技能时，引导子区用该技能完成澄清。",
    "派发时自行判断是否要回执：只有你派发后不会再盯着这份活、又需要在它结束（完成/失败/取消/中断）时知道结果，才给 thread-dispatch 传 notify=true；你会在子区里持续交互、或结果不需要回报时不要开（默认 false，不开就不回报）。",
    "详细的需求澄清在子区内进行：不要在当前会话执行这件事，也不要在当前会话做过于详细的需求澄清。",
    "完成后简要汇报子区名称、short_id 与工作区路径。",
    "",
    "原始意图：",
    intent,
  ].join("\n");
}

/**
 * `/thread` 参数按首词识别合并意图：首词是 `merge`（其后为行尾或非字母数字、下划线、连字符
 * 的分隔符——`merge-fix` 这类名字不算）时返回去掉分隔符并单行化后的补充说明（可为空串），
 * 其余情况返回 `undefined`。
 */
function readThreadMergeNote(argument: string): string | undefined {
  const trimmed = argument.trim();
  if (!/^merge(?:$|[^\p{L}\p{N}_-])/iu.test(trimmed)) return undefined;
  const note = trimmed
    .slice("merge".length)
    .replace(/^[\s,，、:：]+/u, "")
    .replace(/[\p{Cc}\p{Cf}]+/gu, " ")
    .replace(/\s+/gu, " ")
    .trim();
  return note.length > 200 ? `${note.slice(0, 199)}…` : note;
}

/**
 * Instruction block for a `/thread merge` message: like the creation prompt it hands the intent
 * to the assistant instead of acting itself. The command is the owner's confirmation to merge and
 * clean up, so the body names the default pipeline (stages and commits the workspace changes,
 * squashes, rebases onto the target branch, fast-forwards it, then removes the worktree and its
 * merged branch) as the expected behaviour and asks for a stated reason plus an explicit
 * `remove:false` whenever the workspace must be kept; it also states that the removal takes the
 * sub-thread's cwd with it as a known result that must not be repeated as a caveat or used as a
 * reason to keep the workspace. A trailing note after the `merge` keyword is the owner's own
 * supplement and is carried along verbatim. The merge itself is executed by the host through
 * `octo_management`'s `thread-merge`. Every outcome, including a conflict that must be resolved
 * before retrying, is reported to the owner.
 */
function threadMergePrompt(workspacePath: string, note: string): string {
  return [
    "用户要求把本子区（隔离工作区）已完成的工作合并回目标分支，走本机 Worktrunk（wt）的 merge pipeline。",
    "这条命令即代表 owner 确认合并并清理：除非有明确必要理由并向 owner 说明，直接按默认参数执行。",
    "默认 pipeline 会暂存并提交工作区改动、squash、rebase 到目标分支、fast-forward，合并成功后删除工作区与已合并分支；执行前先确认本子区的工作已收尾。",
    "要保留工作区时必须说明必要理由，并显式传 remove:false；目标分支等其他参数确有需要时同样显式传入并说明理由。",
    "工作区被删除后本子区 cwd 随之失效，这是该命令的既定结果、owner 已知：不要在汇报里反复说明这一点，也不要把 cwd 失效当作保留工作区的理由——保留工作区的必要理由应是尚未收尾的具体工作（例如部署、验收）。",
    "合并由宿主执行：调用 octo_management 的 thread-merge 动作，不要自己运行 wt。",
    "成功后向 owner 汇报目标分支与结果细节；失败时（例如 rebase 冲突或 pre-merge 检查失败）先在本子区处置再重试，并把最终结果告诉 owner。",
    ...(note.length === 0 ? [] : ["", "owner 补充说明：", note]),
    "",
    "隔离工作区：",
    workspacePath,
  ].join("\n");
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

/** One bounded line for a selector OMP could not resolve. */
function notFoundModelText(selector: string): string {
  return `未找到模型 "${boundedSelectorText(selector)}"；发送 /switch 查看可用模型列表。`;
}

/** A `/switch` probe whose chat directory cannot be resolved; reported apart from worker failures. */
class SwitchDirectoryError extends Error {
  constructor() {
    super("switch probe directory unavailable");
    this.name = "SwitchDirectoryError";
  }
}

function parseCommand(text: string): { name: string; argument: string } | null {
  const match = /^\/([^\s]+)(?:\s+([\s\S]*))?$/.exec(text.trim());
  if (!match) return null;
  return { name: match[1]!.toLowerCase(), argument: match[2] ?? "" };
}


function statusState(task: TaskRecord | null, hasBinding: boolean): ChatExecutionState {
  if (!task) return hasBinding ? "idle" : "none";
  return task.state;
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
  private readonly blockedDirectories = new Set<string>();
  private readonly groupHistoryBuffers = new Map<string, HistoryBuffer>();
  private readonly mentionPrefs = new Map<string, { effective: boolean; expiresAt: number }>();
  private readonly mentionPrefRequests = new Map<string, Promise<boolean>>();
  /** Last unlisted-chat notice per chat; process-local, so a restart may re-notify once. */
  private readonly unlistedChatNotices = new Map<string, number>();
  /** Last members-cannot-command notice per chat; same process-local window pattern. */
  private readonly memberCommandNotices = new Map<string, number>();
  /** 问题与问答卡：等待者、卡片代次与问题行的所有者。 */
  /** 出站投递：文字、流式编辑与文件共用的队列。 */
  private readonly outbox: Outbox;
  private readonly questions: QuestionDesk;
  /** 回答署名的显示名缓存：uid → 人名，有界、失败即回退（署名渲染用，不参与身份判断）。 */
  private readonly displayNames: DisplayNameCache;
  /** Bot 事件队列消费（卡片点击）。 */
  private readonly botEvents: BotEventLoop;
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
    this.outbox = createOutboxDelivery({
      store: this.store,
      transport: this.options.transport,
      botUid: () => this.bot?.uid,
      started: () => this.started,
    });
    this.displayNames = createDisplayNameCache(
      async (uid, signal) => (await this.options.transport.getUserInfo(uid, signal)).name,
    );
    this.questions = createQuestionDesk({
      store: this.store,
      transport: this.options.transport,
      botUid: () => this.bot?.uid,
      stopping: () => this.stopping,
      enqueueText: (target, text, taskId) => this.outbox.enqueueText(target, text, taskId),
      operationSet: (operation) => this.operationSet(operation),
      commandClaim: (message, name) => this.commandClaim(message, name),
      askTimeoutMs: () => this.options.config.ask?.timeoutMs ?? DEFAULT_ASK_TIMEOUT_MS,
      displayNames: (uids) => this.displayNames.names(uids),
      stopTurnForQuestion: (taskId) => this.stopTurnForQuestion(taskId),
      wakeTurn: (target, text) => {
        void this.wakeTurn(target, text);
      },
      memberAnswerAllowed: (target) => target.channelType !== 1 && this.isPermittedChat(target),
    });
    this.botEvents = createBotEventLoop({
      store: this.store,
      transport: this.options.transport,
      botUid: () => this.bot?.uid,
      stopping: () => this.stopping,
      onCardAction: (action) => this.questions.answerFromCard(action),
    });
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
        await this.outbox.enqueueText(taskTarget(task), `服务已重启完成；任务 ${task.id} 因重启中断，请重新发送。`, null);
      }
      const restartedAt = Date.now();
      const notifiedTargets = new Set<string>();
      for (const notice of this.store.listRestartNotices(identity.uid)) {
        this.store.clearRestartNotice(notice.id);
        if (restartedAt - notice.requestedAt > RESTART_NOTICE_TTL_MS) continue;
        const key = targetKey(notice);
        if (notifiedTargets.has(key)) continue;
        notifiedTargets.add(key);
        await this.outbox.enqueueText(
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
          this.outbox.flushAll();
        },
      );
      if (this.stopping) throw new Error("channel 正在停止");
      this.outbox.flushAll();
      this.botEvents.start();
      await this.recoverProgressCards();
    } catch (error) {
      this.started = false;
      await this.botEvents.stop();
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
      this.questions.rejectForTask(runtime.taskId, new Error("channel stopping"));
      if (runtime.worker) {
        await runtime.worker.stop().catch(() => false);
      }
    }
    this.outbox.abortAll();
    await Promise.allSettled([...this.taskOperations]);
    await Promise.allSettled([...this.activityOperations]);
    await this.botEvents.stop();
    this.questions.stop();
    this.outbox.stopTimers();
    await this.outbox.settle();
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
        this.outbox.flush(message);
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
        await this.outbox.enqueueText(message, "不支持的消息类型，不会启动任务。", null);
        this.store.completeCommand(claim);
      } catch {
        this.store.failCommand(claim);
        throw new Error("本地状态盘写入失败，未能记录不支持消息。");
      }
      return;
    }
    // Members never run commands: the slash text would otherwise be passed to OMP, whose own
    // builtin dispatch consumes it, so it only earns one throttled notice instead.
    if (isCommand(text) && message.senderId.trim() !== ownerId) {
      await this.noticeMemberCommand(message);
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
    if (await this.questions.answerPendingQuestion(message, text)) return;
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
      await this.outbox.enqueueText(message, text, null);
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
        model: null,
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
      model: this.store.getChatModel(this.bot.uid, target),
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
    if (!this.isPermittedChat(message)) return false;
    if (message.mentionedUserIds.includes(this.bot.uid)) return true;
    // The 免@ exemption belongs to the owner: a member's unmentioned message never runs — and
    // never triggers the upstream lookup either, so a rejected member costs no request.
    if (senderId !== ownerId) return false;
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
    this.outbox.enqueueText(message, `${scope}未开通。回复「@${botName} ${UNLISTED_CHAT_PASSPHRASE}」即可开通${scope}。`, null);
    // Only a queued notice opens the window: a failed enqueue still leaves the next message free to try again.
    this.unlistedChatNotices.set(key, Date.now());
  }

  /**
   * The only visible effect of a member's `/` message: the command is not executed and never
   * reaches the agent. One durable claim per message id keeps a redelivery silent, and one
   * notice per chat per window keeps a stream of attempts from becoming a stream of replies.
   */
  private async noticeMemberCommand(message: InboundMessage): Promise<void> {
    const bot = this.bot;
    if (!bot) return;
    const claim = this.commandClaim(message, "member-command");
    if (!this.store.claimCommand(claim)) return;
    const key = targetKey(message);
    const lastNoticedAt = this.memberCommandNotices.get(key);
    if (lastNoticedAt !== undefined && Date.now() - lastNoticedAt < MEMBER_COMMAND_NOTICE_TTL_MS) {
      this.store.completeCommand(claim);
      return;
    }
    const botName = bot.name.trim().length > 0 ? bot.name.trim() : bot.uid;
    try {
      await this.outbox.enqueueText(message, `命令仅 owner 可用；需要助手做事请直接 @${botName} 说明。`, null);
      this.memberCommandNotices.set(key, Date.now());
      this.store.completeCommand(claim);
    } catch {
      this.store.failCommand(claim);
      throw new Error("本地状态盘写入失败，未能记录成员命令提示。");
    }
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
      await this.outbox.enqueueText(
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
    // 项目目录在前，改动前的扁平根留在候选里，旧会话仍能被找回。
    const roots = [
      projectSessionDir(sessionsRoot(this.options.config), cwd),
      join(this.options.config.stateDir, "sessions"),
    ];
    if (!this.options.workerFactory.sessionRootsFor) return roots;
    try {
      return [...roots, ...(await this.options.workerFactory.sessionRootsFor(cwd))];
    } catch {
      return roots;
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

  /** The worker of the chat's live run, while its OMP session is still up. */
  private liveWorker(target: ChatTarget): OmpWorker | undefined {
    const key = targetKey(target);
    for (const runtime of this.runtimes.values()) {
      if (targetKey(runtime.target) === key && runtime.worker) return runtime.worker;
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
    this.outbox.enqueueText(runtime.target, receiptText("已并入当前回合", event.text), null);
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
      } else if (name === "switch") {
        await this.handleSwitch(message, argument);
      } else if (name === "status") {
        await this.handleStatus(target);
      } else if (name === "help") {
        await this.outbox.enqueueText(target, COMMAND_HELP, null);
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
   * its workspace itself, and drives `octo_management`: a simple or unambiguous intent is
   * dispatched as the work itself so the thread starts at once, an underspecified one after at
   * most the few answers it takes to hand it over, with the detailed, SDD-shaped clarification
   * left to the thread. The body follows the chat's append mode like any other message the
   * assistant has to read: a running turn that accepts steering gets the instruction inside that
   * turn, otherwise it becomes a durable task (`queue` mode included) that waits behind the run.
   * Only groups grow threads, so DMs and threads themselves are refused.
   */
  private async handleThreadCommand(message: InboundMessage, argument: string): Promise<void> {
    const mergeNote = readThreadMergeNote(argument);
    if (mergeNote !== undefined) {
      await this.handleThreadMergeCommand(message, mergeNote);
      return;
    }
    if (message.channelType === 5) {
      await this.outbox.enqueueText(message, THREAD_NESTED_TEXT, null);
      return;
    }
    if (message.channelType !== 2) {
      await this.outbox.enqueueText(message, THREAD_GROUP_ONLY_TEXT, null);
      return;
    }
    const intent = argument.trim();
    if (intent.length === 0) {
      await this.outbox.enqueueText(message, THREAD_USAGE, null);
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

  /**
   * `/thread merge`: a sub-thread that is bound to an isolated workspace can hand the merge
   * instruction to the assistant; a compatible local Worktrunk must exist, because merging has no
   * fallback. Every other caller — the main group, DMs, a thread without a bound workspace, a
   * deployment without the capability, a missing or incompatible wt — gets one prompt and no
   * task. The command runs nothing itself: the prompt is delivered like any other message the
   * assistant has to read (steering when the running turn accepts it, otherwise a durable task),
   * and the assistant drives `octo_management`'s `thread-merge`.
   */
  private async handleThreadMergeCommand(message: InboundMessage, mergeNote: string): Promise<void> {
    if (message.channelType !== 5) {
      await this.outbox.enqueueText(message, THREAD_MERGE_THREAD_ONLY_TEXT, null);
      return;
    }
    const worktrees = this.options.worktrees;
    if (!worktrees) {
      await this.outbox.enqueueText(message, THREAD_MERGE_UNCONFIGURED_TEXT, null);
      return;
    }
    const binding = this.store.getBinding(this.bot!.uid, message);
    if (!binding || binding.cwdPinned !== true) {
      await this.outbox.enqueueText(message, THREAD_MERGE_NO_WORKSPACE_TEXT, null);
      return;
    }
    const available = await worktrees.worktrunk();
    if (!available.available) {
      await this.outbox.enqueueText(
        message,
        `${worktrunkUnavailableText(available)}，/thread merge 不可用。`,
        null,
      );
      return;
    }
    this.store.releaseClaim(this.commandClaim(message, "thread"));
    const body = threadMergePrompt(binding.cwd, mergeNote);
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
      await this.outbox.enqueueText(target, "当前聊天有运行中或排队任务，请先 /cancel。", null);
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
      await this.outbox.enqueueText(target, `目录无效：${safeErrorMessage(error, "无法访问该目录")}`, null);
      return;
    }
    const commandLease = this.tryAcquireCommandDirectories([cwd]);
    if (!commandLease) {
      await this.outbox.enqueueText(target, "目标目录当前被任务或未确认会话占用，请稍后重试。", null);
      return;
    }
    try {
      await commandLease.ready;
      const sessionId = createSessionId();
      const sessionDir = await ensureProjectSessionDir(sessionsRoot(this.options.config), cwd);
      const ledgerId = `new-${sessionId}`;
      const ledgerSessionFile = join(sessionDir, `${sessionId}.jsonl`);
      let spawned: WorkerProcess | undefined;
      let candidate: OmpWorker | undefined;
      let stopped = false;
      let persistenceFailed = false;
      try {
        const chatModel = this.store.getChatModel(this.bot!.uid, target);
        candidate = await this.options.workerFactory.create({
          cwd,
          sessionDir,
          ...(chatModel === null ? {} : { model: chatModel }),
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
        const model = await this.createdSessionModel(candidate);
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
        await this.outbox.enqueueText(
          target,
          `已创建会话 ${binding.sessionId}，cwd=${binding.cwd}${model === null ? "" : `，模型=${model}`}`,
          null,
        );
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
        await this.outbox.enqueueText(target, "无法创建可恢复的 OMP 会话，当前绑定未改变。", null);
      }
    } finally {
      commandLease.complete();
    }
  }

  private async handleMove(target: ChatTarget, argument: string): Promise<void> {
    if (this.bot && (this.store.getActiveTask(this.bot.uid, target) || this.hasRuntime(target))) {
      await this.outbox.enqueueText(target, "当前聊天有运行中或排队任务，请先 /cancel。", null);
      return;
    }
    const previous = await this.reconcileExternalBinding(target);
    if (!previous) {
      await this.outbox.enqueueText(target, "当前聊天没有可迁移的会话，请先 /new。", null);
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
      await this.outbox.enqueueText(target, `目录无效：${safeErrorMessage(error, "无法访问该目录")}`, null);
      return;
    }
    if (!previous.sessionFile) {
      await this.outbox.enqueueText(target, "当前绑定没有可恢复的精确会话文件，未迁移。", null);
      return;
    }
    const sessionFile = await existingSessionFile(previous.sessionFile);
    if (!sessionFile) {
      await this.outbox.enqueueText(target, "绑定的 OMP 会话文件不存在，未迁移。", null);
      return;
    }
    const commandLease = this.tryAcquireCommandDirectories([previous.cwd, cwd]);
    if (!commandLease) {
      await this.outbox.enqueueText(target, "源目录或目标目录当前被任务或未确认会话占用，请稍后重试。", null);
      return;
    }
    try {
      await commandLease.ready;
      const sessionDir = await ensureProjectSessionDir(sessionsRoot(this.options.config), cwd);
      const ledgerId = `move-${randomUUID()}`;
      let spawned: WorkerProcess | undefined;
      let candidate: OmpWorker | undefined;
      let stopped = false;
      let persistenceFailed = false;
      try {
        const chatModel = this.store.getChatModel(this.bot!.uid, target);
        candidate = await this.options.workerFactory.create({
          cwd: previous.cwd,
          sessionDir,
          sessionFile,
          ...(chatModel === null ? {} : { model: chatModel }),
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
        await this.outbox.enqueueText(target, `已迁移会话 ${binding.sessionId}，cwd=${binding.cwd}`, null);
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
        await this.outbox.enqueueText(target, "无法验证 OMP 原生 /move，当前绑定未改变。", null);
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
      await this.outbox.enqueueText(target, DEFAULT_INTERRUPT_IDLE_TEXT, null);
      return;
    }
    runtime.stopCause = "interrupt";
    runtime.workAfterStop = false;
    try {
      await runtime.worker.abortTurn();
    } catch {
      runtime.stopCause = null;
      await this.outbox.enqueueText(target, DEFAULT_INTERRUPT_REJECTED_TEXT, null);
      return;
    }
    await this.outbox.enqueueText(target, DEFAULT_INTERRUPTED_TEXT, null);
  }

  /**
   * Stops the turn whose `ask` wait timed out, the way a cancelled tool call stops it: the abort
   * frame goes out first, so the model cannot use a tool error to keep going. The questions stay
   * pending and their cards stay submittable; the run settles as interrupted unless the turn
   * really produced work after the stop. Nothing is delivered to the chat (the owner is silent
   * by contract); their later answer wakes the agent through `wakeTurn`.
   */
  private stopTurnForQuestion(taskId: string): void {
    const runtime = this.runtimes.get(taskId);
    if (!runtime?.worker || !runtime.promptInFlight) return;
    runtime.stopCause = "question";
    runtime.workAfterStop = false;
    void runtime.worker.abortTurn().catch(() => {
      if (runtime.stopCause === "question") runtime.stopCause = null;
    });
  }

  /**
   * Delivers an answer recorded with no live `ask` waiting on it (the wait timed out and the turn
   * was stopped) as new input for that chat: a running turn takes it as a steering append, a
   * quieter chat runs it as a durable task in the same session. The answer itself is already
   * durable; a failed wake only costs the automatic continuation and is logged.
   */
  private async wakeTurn(target: ChatTarget, text: string): Promise<void> {
    const bot = this.bot;
    if (!bot) return;
    const message: InboundMessage = {
      ...target,
      messageId: `answer:${randomUUID()}`,
      senderId: this.store.getOwnerId(bot.uid)?.trim() || bot.uid,
      text,
      mentionedUserIds: [],
      attachments: [],
      quoted: null,
      supported: true,
      messageSeq: 0,
      messageTimestamp: Date.now(),
    };
    try {
      if (await this.appendToRunningTask(message, text)) return;
      await this.acceptTask(message, text);
    } catch (error) {
      console.error(
        `[ask] 答复唤醒交付失败 target=${targetKey(target)}：${safeErrorMessage(error, "wake failed")}`,
      );
    }
  }

  /**
   * The completion relay of `thread-dispatch --notify`: a dispatched task whose row carries a
   * relay target (`relayChannelType`/`relayChannelId`) reports one bounded notice to the chat
   * that dispatched it once it reaches a terminal state. The notice is a synthetic inbound
   * message with a deterministic id (`notify:<task>`), so a replay or a restart can never deliver
   * it twice; like `wakeTurn` it follows the target chat's append mode and never touches the
   * pending-question path or the permission gate. A failed delivery only costs the relay and is
   * logged.
   *
   * A graceful stop settles its in-flight tasks as `cancelled` (the stop sets
   * `runtime.cancelRequested`), and that lifecycle outcome is never relayed: the notice would be
   * accepted right before this process goes down, only to be interrupted by the next start and
   * read as a bogus "task interrupted by restart" in the dispatching chat. The `stopping` flag
   * separates that path from the owner's `/cancel`, which still relays.
   */
  private async notifyThreadOutcome(taskId: string): Promise<void> {
    try {
      if (this.stopping) return;
      const botUid = this.bot?.uid;
      if (botUid === undefined) return;
      const task = this.store.getTask(taskId);
      if (!task || task.channelType !== 5) return;
      if (task.relayChannelType === null || task.relayChannelId === null) return;
      const state = task.state;
      if (
        state !== "completed"
        && state !== "failed"
        && state !== "cancelled"
        && state !== "interrupted"
      ) {
        return;
      }
      const text = threadOutcomeNotice(task);
      const message: InboundMessage = {
        channelId: task.relayChannelId,
        channelType: task.relayChannelType,
        messageId: threadNotifyMessageId(task.id),
        senderId: this.store.getOwnerId(botUid)?.trim() || botUid,
        text,
        mentionedUserIds: [],
        attachments: [],
        quoted: null,
        supported: true,
        messageSeq: 0,
        messageTimestamp: Date.now(),
      };
      if (await this.appendToRunningTask(message, text)) return;
      await this.acceptTask(message, text);
    } catch (error) {
      console.error(
        `[thread-notify] 回传投递失败 task=${taskId}：${safeErrorMessage(error, "notify failed")}`,
      );
    }
  }

  /**
   * `/queue` is the explicit way to append behind the running task regardless of the chat's mode,
   * so it always takes the durable task path minus the command prefix.
   */
  private async handleQueue(message: InboundMessage, argument: string): Promise<void> {
    const text = argument.trim();
    if (text.length === 0) {
      await this.outbox.enqueueText(message, QUEUE_USAGE, null);
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
      await this.outbox.enqueueText(
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
      await this.outbox.enqueueText(target, MODE_USAGE, null);
      return;
    }
    this.store.setChatMode(this.bot!.uid, target, mode);
    await this.outbox.enqueueText(
      target,
      mode === "steer"
        ? "追加模式已设为 steer：运行中的新消息会注入当前回合。"
        : "追加模式已设为 queue：运行中的新消息会排到当前回合之后。",
      null,
    );
  }

  /**
   * The model the freshly created `/new`/`/reset` session actually came up with, asked of its own
   * candidate worker through the same `worker.models()` state query `/switch` and `/status` use: the stored
   * chat model when `--model` carried one, OMP's own default otherwise — it can never report
   * `default` as a model. A failed or empty answer degrades the creation receipt to its model-less
   * form instead of failing `/new` or inventing a value.
   */
  private async createdSessionModel(worker: OmpWorker): Promise<string | null> {
    const catalog = await worker.models().catch(() => null);
    if (catalog === null || catalog.current === null) return null;
    return modelWithLevel(catalog.current, catalog.level);
  }

  /**
   * `/switch [选择器]`: no argument reports the model this chat is on plus every model OMP offers;
   * `default` drops the chat's model; anything else is handed to OMP's own `/switch`, which
   * resolves `provider/id`, a model id or substring, `@role`, and an optional `:level` suffix
   * against its live catalog. The resolved `provider/id:level` is stored for the chat, so workers
   * started later (tasks, `/new`, `/reset`, `/move`) get it as `--model` — and when a turn is
   * already running, the same dispatch switches that session in place, because OMP consumes the
   * command locally instead of starting an agent turn.
   */
  private async handleSwitch(message: InboundMessage, argument: string): Promise<void> {
    const botUid = this.bot!.uid;
    const requested = argument.trim();
    const live = this.liveWorker(message);
    if (requested.length === 0) {
      let catalog: WorkerModelCatalog;
      try {
        catalog = live
          ? await live.models()
          : await this.withProbeWorker(message, worker => worker.models());
      } catch (error) {
        await this.replySwitchFailure(message, error);
        return;
      }
      await this.outbox.enqueueText(message, this.describeChatModel(message, live !== undefined, catalog), null);
      return;
    }
    if (requested.toLowerCase() === "default" && !requested.includes("/")) {
      await this.resetChatModel(message);
      return;
    }
    // A selector is a single token; catching a stray one here keeps a typo from starting a worker.
    if (/\s/u.test(requested)) {
      await this.outbox.enqueueText(message, notFoundModelText(requested), null);
      return;
    }
    let outcome: WorkerModelSwitch;
    try {
      outcome = live
        ? await live.switchModel(requested)
        : await this.withProbeWorker(message, worker => worker.switchModel(requested));
    } catch (error) {
      await this.replySwitchFailure(message, error);
      return;
    }
    if (!outcome.ok) {
      await this.outbox.enqueueText(message, notFoundModelText(requested), null);
      return;
    }
    const model = modelWithLevel(outcome.model, outcome.level);
    this.store.setChatModel(botUid, message, model);
    await this.outbox.enqueueText(
      message,
      live === undefined
        ? `已切换本聊天模型为 ${model}，从下一回合开始生效。`
        : `已切换本聊天模型为 ${model}（运行中会话已同步切换）。`,
      null,
    );
  }

  /**
   * The empty `/switch` answer: the live session's model while a turn runs (that is the model in
   * use), otherwise this chat's stored model, otherwise the model OMP would start a fresh session
   * with — followed by every model OMP currently offers.
   */
  private describeChatModel(target: ChatTarget, running: boolean, catalog: WorkerModelCatalog): string {
    const stored = this.store.getChatModel(this.bot!.uid, target);
    const live = catalog.current === null ? null : modelWithLevel(catalog.current, catalog.level);
    if (running && live !== null) {
      return formatModelCatalog({
        current: live,
        label:
          stored === null
            ? "运行中会话"
            : stored === live
              ? "本聊天指定"
              : `运行中会话；本聊天模型 ${stored}`,
        models: catalog.available,
      });
    }
    if (stored !== null) {
      return formatModelCatalog({ current: stored, label: "本聊天指定", models: catalog.available });
    }
    return formatModelCatalog({ current: live, label: "OMP 默认", models: catalog.available });
  }

  /**
   * `/switch default`: drop this chat's model so later workers start without `--model` and follow
   * OMP's own default selection. A running session is left alone — `/switch @default` is the
   * native way to move it.
   */
  private async resetChatModel(target: ChatTarget): Promise<void> {
    const previous = this.store.getChatModel(this.bot!.uid, target);
    if (previous === null) {
      await this.outbox.enqueueText(target, "本聊天未指定模型，继续跟随 OMP 默认。", null);
      return;
    }
    this.store.clearChatModel(this.bot!.uid, target);
    let current: string | null = null;
    if (this.liveWorker(target) === undefined) {
      const catalog = await this.withProbeWorker(target, worker => worker.models()).catch(() => null);
      if (catalog !== null && catalog.current !== null) current = modelWithLevel(catalog.current, catalog.level);
    }
    await this.outbox.enqueueText(
      target,
      `已恢复跟随 OMP 默认模型${current === null ? "" : `（当前为 ${current}）`}。`,
      null,
    );
  }

  /**
   * One throwaway worker serves a `/switch` probe: fresh (no `--resume`), no prompt of our own
   * beyond the switch dispatch, and a `mkdtemp` session directory it never shares with a chat
   * session, so it neither reads nor writes a running turn's transcript. It runs in the chat's own
   * directory — same OMP profile, config, and credentials as that chat's turns — is recorded in
   * the worker ledger like `/new`'s candidate worker, and is removed afterwards.
   */
  private async withProbeWorker<T>(target: ChatTarget, run: (worker: OmpWorker) => Promise<T>): Promise<T> {
    const cwd = await this.probeDirectory(target);
    if (cwd === null) throw new SwitchDirectoryError();
    const sessionDir = await mkdtemp(join(tmpdir(), "omp-channel-octo-switch-"));
    const sessionId = createSessionId();
    const ledgerId = `switch-${sessionId}`;
    let spawned: WorkerProcess | undefined;
    let stopped = false;
    try {
      const candidate = await this.options.workerFactory.create({
        cwd,
        sessionDir,
        onSpawn: (processInfo) => {
          spawned = processInfo;
          this.store.recordWorker({
            id: ledgerId,
            botUid: this.bot!.uid,
            sessionId,
            sessionFile: join(sessionDir, `${sessionId}.jsonl`),
            pid: processInfo.pid,
            pgid: processInfo.pgid,
            identity: processInfo.identity,
            cwd: processInfo.cwd,
            moveTargetCwd: null,
            state: "running",
          });
        },
      });
      try {
        return await run(candidate);
      } finally {
        stopped = await candidate.stop().catch(() => false);
        if (stopped) this.store.markWorkerStopped(ledgerId);
        else this.blockedDirectories.add(cwd);
      }
    } catch (error) {
      if (!stopped && spawned) {
        const verified = await this.options.workerFactory.recover(spawned).catch(() => false);
        if (verified) this.store.markWorkerStopped(ledgerId);
        else this.blockedDirectories.add(cwd);
      }
      throw error;
    } finally {
      await rm(sessionDir, { recursive: true, force: true }).catch(() => {});
    }
  }

  /**
   * The cwd a probe worker runs in: the chat's own directory — the same one its turns use — or
   * `null` when nothing usable resolves.
   */
  private async probeDirectory(target: ChatTarget): Promise<string | null> {
    try {
      return await this.defaultWorkDirectory(target).catch(() =>
        resolveWorkDirectory("", this.options.config),
      );
    } catch {
      return null;
    }
  }

  /** A probe failure as one bounded line: no working directory, or no usable OMP answer. */
  private async replySwitchFailure(target: ChatTarget, error: unknown): Promise<void> {
    await this.outbox.enqueueText(
      target,
      error instanceof SwitchDirectoryError ? SWITCH_CWD_UNAVAILABLE_TEXT : SWITCH_PROBE_FAILED_TEXT,
      null,
    );
  }

  private async handleCancel(target: ChatTarget): Promise<void> {
    const queued = this.store.cancelQueued(this.bot!.uid, target);
    for (const taskId of queued) this.queuedBehind.delete(taskId);
    for (const taskId of queued) await this.notifyThreadOutcome(taskId);
    this.questions.cancelForTarget(target);
    let active = false;
    for (const runtime of this.runtimes.values()) {
      if (targetKey(runtime.target) !== targetKey(target)) continue;
      active = true;
      runtime.cancelRequested = true;
      runtime.stopText?.();
      this.outbox.forgetTask(runtime.taskId);
      runtime.stopActivity?.();
      runtime.cancelReplySent = true;
      if (runtime.worker) {
        void runtime.worker.stop().catch(() => false);
      }
      this.questions.rejectForTask(runtime.taskId, new Error("task cancelled"));
      this.store.markCancelled(runtime.taskId);
    }
    await this.outbox.enqueueText(
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
      await this.outbox.enqueueText(target, RESTART_UNAVAILABLE_TEXT, null);
      return false;
    }
    const mode = argument.trim();
    if (mode.length > 0 && mode !== "now") {
      await this.outbox.enqueueText(target, RESTART_USAGE, null);
      return false;
    }
    const busy = this.busyTargets();
    if (busy.length > 0 && mode !== "now") {
      await this.outbox.enqueueText(
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
    await this.outbox.enqueueText(
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
    await this.outbox.enqueueText(
      target,
      `session=${session}\ncwd=${cwd}\nstate=${taskState}\nmode=${current.mode}\nmodel=${current.model ?? "未指定（OMP 默认）"}\nqueued=${current.queued}\noutbox=${current.outbox}\nunknown=${current.unknownOutbox}`,
      null,
    );
  }


  private async acceptTask(
    message: InboundMessage,
    text: string,
    historyEligible = true,
    relay: TaskRelayTarget | null = null,
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
        await this.outbox.enqueueText(message, "无法创建默认会话目录，未接收任务。", null);
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
      relayChannelType: relay?.channelType ?? null,
      relayChannelId: relay?.channelId ?? null,
      relayThreadName: relay?.threadName ?? null,
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
    this.outbox.flush(message);
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
      await this.outbox.enqueueText(taskTarget(task), GENERIC_TASK_FAILURE_NOTICE, taskId);
      await this.notifyThreadOutcome(taskId);
    } finally {
      lease?.complete();
    }
  }

  /**
   * Host tools 的宿主适配：工具模块只看到它真正需要的状态，重启排定与 channel 快照留在
   * 服务里，工具因此不能绕过运行时的调度与生命周期。
   */
  private hostToolHost(): HostToolHost {
    const botUid = this.bot?.uid;
    if (botUid === undefined) throw new Error("Bot 身份未注册，无法构造 host tools");
    return {
      options: this.options,
      store: this.store,
      botUid,
      enqueueText: (target, text, taskId) => this.outbox.enqueueText(target, text, taskId),
      flushTarget: (target) => this.outbox.flush(target),
      deliverToChat: async (message, relay) => {
        if (await this.appendToRunningTask(message, message.text)) return "steered";
        await this.acceptTask(message, message.text, true, relay ?? null);
        return "queued";
      },
      askQuestions: (task, target, args, signal) => this.questions.askQuestions(task, target, args, signal),
      channelStatus: () => ({
        pid: process.pid,
        restart_pending: this.pendingRestart !== null,
        running_targets: [...this.runtimes.values()].map((runtime) => targetKey(runtime.target)),
        queued_tasks: this.store.listQueuedTasks().length,
        blocked_cwds: this.blockedDirectories.size,
      }),
      scheduleRestart: (target) => {
        if (!this.options.restart) return RESTART_UNAVAILABLE_TEXT;
        this.pendingRestart = { channelType: target.channelType, channelId: target.channelId };
        return CHANNEL_RESTART_SCHEDULED_TEXT;
      },
    };
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
      await this.outbox.enqueueText(taskTarget(task), GENERIC_TASK_FAILURE_NOTICE, taskId);
      await this.notifyThreadOutcome(taskId);
      return;
    }
    if (this.blockedDirectories.has(task.cwd)) {
      this.queuedBehind.delete(taskId);
      this.store.markFailed(taskId, "directory remains blocked by an unconfirmed worker");
      await this.outbox.enqueueText(taskTarget(task), GENERIC_TASK_FAILURE_NOTICE, taskId);
      await this.notifyThreadOutcome(taskId);
      return;
    }
    if (!this.store.markStarted(taskId)) return;
    if (this.queuedBehind.delete(taskId)) {
      this.outbox.enqueueText(taskTarget(task), receiptText("开始执行排队消息", task.text), null);
    }
    const runtime: RuntimeTask = {
      taskId,
      target: taskTarget(task),
      cwd: task.cwd,
      cancelRequested: false,
      cancelReplySent: false,
      promptInFlight: false,
      stopCause: null,
      workAfterStop: false,
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
    const textDelivery = this.outbox.registerTask(taskId);
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
      degraded: false,
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
    let startupPhase: TaskStartupPhase = "context";

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
      const snapshot = round.degraded ? stripCardSummaries(progress) : progress;
      if (running) round.nextEditAt = Date.now() + PROGRESS_MERGE_MS;
      const parentSignal = running ? progressAbort.signal : undefined;
      try {
        await runActivityRequest(
          (signal) => this.options.transport.editProgressCard(target, messageId, snapshot, cardSeq, signal),
          parentSignal,
        );
        if (round.version === version) round.sentVersion = version;
        if (running) {
          this.rememberProgressCard(taskId, target, messageId, cardSeq, snapshot);
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
        } else {
          if (!round.failureLogged) {
            round.failureLogged = true;
            console.error(`[progress-card] 运行中编辑失败 target=${targetKey(target)} seq=${cardSeq}：${reason}`);
          }
          // The edge refused the body rather than the request: the same card goes out once without
          // the free-text step summaries, and the round keeps that shape from here on.
          if (!round.degraded && isEdgeRefusal(error)) {
            round.degraded = true;
            console.error(
              `[progress-card] 卡片正文被网关拒绝，改用不带步骤摘要的形态重试`
              + ` target=${targetKey(target)} seq=${cardSeq}`,
            );
          }
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
    runtime.stopText = () => textDelivery.stop();
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
      // Everything past the group-context read is "startup": a failure here is no longer a context
      // read problem, so the classification must not keep reporting one.
      startupPhase = "startup";
      const sessionDir = await ensureProjectSessionDir(sessionsRoot(this.options.config), task.cwd);
      const sidecarRoot = contextSidecarRoot(this.options.config, this.bot!.uid, target);
      const binding = bindingAtStart ?? this.store.getBinding(this.bot!.uid, target);
      const chatModel = this.store.getChatModel(this.bot!.uid, target);
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
        ...(chatModel === null ? {} : { model: chatModel }),
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
        if (!sessionFile) throw new TaskStartupError("session-invalid", "绑定的 OMP 会话文件不存在");
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
        // The scope publishes the session this turn belongs to; it stays as the
        // scope's current session so sub-sessions OMP opens in the same worker
        // are never mistaken for a channel turn.
        await writeBoundSession(sidecarRoot, {
          botUid: this.bot!.uid,
          target: { ...target },
          sessionId: verified.sessionId,
        });
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
        if (runtime.stopCause !== null && event.type !== "turn_end" && event.type !== "run_end") {
          runtime.workAfterStop = true;
        }
        if (event.type === "text") {
          if (event.messageId.trim().length === 0 || (event.text.length === 0 && !event.final)) return;
          assistantTexts.set(event.messageId, event.text);
          this.outbox.enqueueStreamText(target, task.id, event);
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
        tools: createHostTools(this.hostToolHost(), task, snapshot),
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
      } else if (runtime.stopCause !== null && !runtime.workAfterStop) {
        this.store.markInterrupted(
          taskId,
          runtime.stopCause === "question"
            ? "等待回答超时，回合已停止"
            : "用户中断当前回合",
        );
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
        this.outbox.flush(target);
      }
    } catch (error) {
      if (runtime.cancelRequested) {
        this.store.markCancelled(taskId);
        finishProgress("cancelled");
        await this.sendCancelledIfNeeded(runtime);
      } else {
        const failure = worker
          ? classifyTaskFailure(error)
          : classifyStartupFailure(error, startupPhase);
        console.error(
          `[task] ${worker ? "执行失败" : "启动失败"} task=${taskId} target=${targetKey(runtime.target)}`
          + ` kind=${failure.kind} code=${failure.code} phase=${startupPhase} detail=${failure.detail}`,
        );
        this.store.markFailed(taskId, failure.row);
        finishProgress("failed");
        await this.outbox.enqueueText(runtime.target, failure.notice, taskId);
      }
    } finally {
      runtime.stopActivity?.(!progressRound.finished);
      this.questions.rejectForTask(taskId, new Error("task finished"));
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
      this.outbox.releaseIfIdle(taskId);
      // Deregistered before the relay so a creator chat that shares this directory does not read
      // the relay as work that queued behind this run.
      this.runtimes.delete(taskId);
      await this.notifyThreadOutcome(taskId);
      this.maybeRunDeferredRestart();
    }
  }

  private async sendCancelledIfNeeded(runtime: RuntimeTask): Promise<void> {
    if (runtime.cancelReplySent) return;
    runtime.cancelReplySent = true;
    await this.outbox.enqueueText(
      runtime.target,
      this.restartRequested ? DEFAULT_RESTARTING_TEXT : DEFAULT_CANCELLED_TEXT,
      runtime.taskId,
    );
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



  /** Tracks a background operation so shutdown waits for it without blocking a caller. */
  private operationSet(operation: Promise<void>): void {
    this.activityOperations.add(operation);
    void operation.finally(() => this.activityOperations.delete(operation));
  }
}

export async function createChannelService(options: ChannelServiceOptions): Promise<ChannelService> {
  const commandHandlers = normalizeCommandHandlers(options.commandHandlers);
  const store = await openChannelStore(options.config.stateDir);
  return new CoreService(options, store, commandHandlers);
}

export { GROUP_TRUSTED_INSTRUCTION };