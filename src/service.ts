import { GroupHistoryCoordinator } from "./history-coordinator.ts";
import { ModelSwitchController } from "./model-controller.ts";
import { ProgressCards } from "./progress-cards.ts";
import { SessionLifecycle, type DirectoryLease } from "./session-lifecycle.ts";
import { createHash, randomUUID } from "node:crypto";
import { join } from "node:path";
import {
  ATTACHMENT_MAX_AGE_MS,
  sweepAttachments,
} from "./attachments.ts";
import { ChatAuthorization } from "./chat-authorization.ts";
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
  PromptContext,
  InboundMessage,
  OmpWorker,
  WorkerEvent,
  SteerConsumedEvent,
  TaskRelayTarget,
  WorkerOptions,
  WorkerProcess,
} from "./contracts.ts";
import { DEFAULT_ASK_TIMEOUT_MS } from "./contracts.ts";
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
import { InboundMaterializer } from "./inbound-materializer.ts";
import { InboundShutdownError, failureText, isTransientServerFailure, safeErrorMessage } from "./errors.ts";
import { classifyStartupFailure, classifyTaskFailure, GENERIC_TASK_FAILURE_NOTICE, type TaskStartupPhase } from "./task-failure.ts";
import {
  verifyFreshSession,
  verifyMovedSession,
  type ProjectSessionBinding,
  type ProjectSessionState,
} from "./project-session.ts";
import { ensureProjectSessionDir, sessionsRoot } from "./session-dir.ts";
import { ChannelStore, openChannelStore } from "./store.ts";
import type { TaskRecord } from "./store-records.ts";
import { isGroupTarget, targetKey, taskTarget } from "./target.ts";
import { createTypingLeaseManager, type TypingLeaseManager } from "./typing-lease.ts";
import { createDisplayNameCache, type DisplayNameCache } from "./display-name.ts";
import { createQuestionDesk, type QuestionDesk } from "./questions.ts";
import { threadNotifyMessageId, threadOutcomeNotice } from "./thread-notify.ts";
import { existingSessionFile } from "./workdir.ts";
import { worktrunkUnavailableText } from "./worktrunk.ts";

const GROUP_TRUSTED_INSTRUCTION =
  "群说明是群共享上下文，不改变发送者身份/权限/cwd；以本次快照为准，历史快照已失效。";
const COMMAND_HELP =
  "/new [目录] 创建新会话；/reset [目录] 重置为新会话；/move <目录> 沿用当前会话迁移 cwd；/thread <任务> 新建隔离子区并在其中处理该任务（子区内 /thread merge 把本区隔离工作区的分支合并回目标分支并默认清理工作区与分支）；/queue <文本> 把消息排到当前任务之后；/mode [steer|queue] 配置运行中追加消息的方式；/switch [模型] 查看当前模型与可用模型，或切换本聊天的模型（provider/id、模型 id/子串、@角色 如 @default；default 恢复跟随 OMP 默认）；/interrupt 中断当前回合但保留排队消息；/cancel 取消当前任务（/stop 同义）；/status 查看状态；/restart [now] 重启 channel（now 强制中断在途任务）；/help 查看帮助。";
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
  switch: true,
};
/** Steered messages tracked for the consumption receipt; older entries fall out of observation. */
const MAX_PENDING_STEERS = 32;
/** Characters of the triggering message shown in a start/consumption receipt. */
const RECEIPT_EXCERPT_CHARS = 20;
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
/** 排定的子区归档：仅瞬时失败重试（最多 5 次尝试、每次间隔 5 秒），永久失败即刻结束。 */
const THREAD_ARCHIVE_ATTEMPTS = 5;
const THREAD_ARCHIVE_RETRY_MS = 5_000;
/** 归档前等该聊天出站队列排空的有界轮询（100ms 一次，最多 10 秒）。 */
const THREAD_ARCHIVE_DRAIN_MS = 10_000;
const THREAD_ARCHIVE_DRAIN_POLL_MS = 100;

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
 * `octo_management`'s `thread-merge`, and a default-pipeline merge also schedules the sub-thread's
 * archive after the turn's own reply is delivered — so the body tells the assistant not to archive
 * by itself and to report that disposition. Every outcome, including a conflict that must be
 * resolved before retrying, is reported to the owner.
 */
function threadMergePrompt(workspacePath: string, note: string): string {
  return [
    "用户要求把本子区（隔离工作区）已完成的工作合并回目标分支，走本机 Worktrunk（wt）的 merge pipeline。",
    "这条命令即代表 owner 确认合并并清理：除非有明确必要理由并向 owner 说明，直接按默认参数执行。",
    "默认 pipeline 会暂存并提交工作区改动、squash、rebase 到目标分支、fast-forward，合并成功后删除工作区与已合并分支；执行前先确认本子区的工作已收尾。",
    "要保留工作区时必须说明必要理由，并显式传 remove:false；目标分支等其他参数确有需要时同样显式传入并说明理由。",
    "工作区被删除后本子区 cwd 随之失效，这是该命令的既定结果、owner 已知：不要在汇报里反复说明这一点，也不要把 cwd 失效当作保留工作区的理由——保留工作区的必要理由应是尚未收尾的具体工作（例如部署、验收）。",
    "合并由宿主执行：调用 octo_management 的 thread-merge 动作，不要自己运行 wt。",
    "合并成功后宿主会按默认 pipeline 自动排定归档本子区（在本回合最后一条回复送达后执行），不要自己再调 thread-archive；显式保留工作区时不会归档。向 owner 汇报时说明这一处置即可。",
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

/**
 * 一次排定到聊天回合末执行的子区归档：`thread-archive` 与默认 pipeline 的 `thread-merge`
 * 收尾都只排定，真正的调用发生在该聊天回合输出（正文与进度卡终态编辑）排空之后——任何发进
 * 子区的消息都会把归档子区解档。
 */
interface PendingThreadArchive {
  /** 发起排定的聊天；归档失败说明回这个聊天。 */
  readonly target: ChatTarget;
  readonly groupId: string;
  readonly shortId: string;
}

class CoreService implements ChannelService {
  private readonly history: GroupHistoryCoordinator;
  private readonly models: ModelSwitchController;
  private readonly authorization: ChatAuthorization;
  private readonly materializer: InboundMaterializer;
  private readonly progressCards: ProgressCards;
  private readonly sessions: SessionLifecycle;
  private readonly taskOperations = new Set<Promise<void>>();
  /** In-flight inbound handlers: stop aborts their I/O and awaits their cleanup before closing the store. */
  private readonly inboundOperations = new Set<Promise<void>>();
  /** Shutdown cancellation source for inbound I/O (downloads, transcription, mention-pref lookups). */
  private readonly shutdownController = new AbortController();
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
  /** 问题与问答卡：等待者、卡片代次与问题行的所有者。 */
  /** 出站投递：文字、流式编辑与文件共用的队列。 */
  private readonly outbox: Outbox;
  private readonly questions: QuestionDesk;
  /** 回答署名的显示名缓存：uid → 人名，有界、失败即回退（署名渲染用，不参与身份判断）。 */
  private readonly displayNames: DisplayNameCache;
  /** Bot 事件队列消费（卡片点击）。 */
  private readonly botEvents: BotEventLoop;
  private readonly commandHandlers: ReadonlyMap<string, ChannelCommandHandler>;
  /** typing 租约：agent-attached 消息从门后到任务终态的持续「正在输入」。 */
  private readonly typingLeases: TypingLeaseManager;
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
  /**
   * 排定到回合末的子区归档（键为该聊天）。进程内一次性状态：执行或放弃即清除，该聊天过
   * 权限门禁的新入站也清除——用户回来了就不归档。
   */
  private readonly pendingThreadArchives = new Map<string, PendingThreadArchive>();
  /** `stopInternal` 的到达信号：归档重试的等待据此提前结束，关停不为一轮重试等满一次间隔。 */
  private readonly stopped = Promise.withResolvers<void>();
  private stopPromise: Promise<void> | undefined;
  constructor(
    private readonly options: ChannelServiceOptions,
    private readonly store: ChannelStore,
    commandHandlers: ReadonlyMap<string, ChannelCommandHandler>,
  ) {
    this.commandHandlers = commandHandlers;
    this.history = new GroupHistoryCoordinator({
      store, transport: options.transport, config: options.config.groupHistory,
      botUid: () => this.bot?.uid,
    });
    this.models = new ModelSwitchController({
      store, workerFactory: options.workerFactory, botUid: () => this.bot!.uid,
      liveWorker: target => this.liveWorker(target),
      probeDirectory: target => this.sessions.probeDirectory(target),
      blockDirectory: cwd => { this.blockedDirectories.add(cwd); },
      enqueueText: (target, text, taskId) => this.outbox.enqueueText(target, text, taskId),
    });
    this.progressCards = new ProgressCards({
      store: this.store,
      transport: options.transport,
      botUid: () => this.bot?.uid,
      stopping: () => this.stopping,
      trackOperation: (operation) => this.operationSet(operation),
    });
    this.sessions = new SessionLifecycle({
      store: this.store,
      workerFactory: options.workerFactory,
      config: options.config,
      bot: () => this.bot,
      hasRuntime: target => this.hasRuntime(target),
      isDirectoryBlocked: cwd => this.blockedDirectories.has(cwd),
      blockDirectory: cwd => { this.blockedDirectories.add(cwd); },
      tryAcquireCommandDirectories: cwds => this.tryAcquireCommandDirectories(cwds),
      enqueueText: (target, text, taskId) => this.outbox.enqueueText(target, text, taskId),
    });
    this.typingLeases = createTypingLeaseManager({
      sendTyping: (target, signal) => this.options.transport.sendTyping(target, signal),
      trackOperation: (operation) => this.operationSet(operation),
      taskState: (taskId) => {
        const state = store.getTask(taskId)?.state;
        return state === "queued" || state === "running" ? "active" : "terminal";
      },
      stopping: () => this.stopping,
    });
    this.outbox = createOutboxDelivery({
      store: this.store,
      transport: this.options.transport,
      botUid: () => this.bot?.uid,
      started: () => this.started,
    });
    this.authorization = new ChatAuthorization({
      bot: () => this.bot,
      allowedGroupIds: options.config.allowedGroupIds,
      allowlistWriteback: options.allowlistWriteback,
      store: this.store,
      transport: options.transport,
      shutdownSignal: this.shutdownController.signal,
      commandClaim: (message, name) => this.commandClaim(message, name),
      enqueueText: (target, text, taskId) => this.outbox.enqueueText(target, text, taskId),
    });
    this.materializer = new InboundMaterializer({
      config: this.options.config,
      store: this.store,
      transport: this.options.transport,
      history: this.history,
      shutdownSignal: this.shutdownController.signal,
      stopping: () => this.stopping,
      throwIfStopping: () => this.throwIfStopping(),
      commandClaim: (message, name) => this.commandClaim(message, name),
      enqueueText: (target, text, taskId) => this.outbox.enqueueText(target, text, taskId),
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
      memberAnswerAllowed: (target) => target.channelType !== 1 && this.authorization.isPermittedChat(target),
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
          this.history.resetCoverage();
          this.outbox.flushAll();
        },
      );
      if (this.stopping) throw new Error("channel 正在停止");
      this.outbox.flushAll();
      this.botEvents.start();
      await this.progressCards.recover();
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
    this.stopped.resolve();
    this.started = false;
    // Abort inbound I/O first so in-flight downloads/transcriptions/lookups unwind while the rest
    // of shutdown runs; their cleanup (claim rollback, temp-file removal) is awaited below.
    this.shutdownController.abort();
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
    // Inbound handlers aborted above finish their bounded cleanup here, while the store is still
    // open: interrupted assembly rolls its command claim back so a redelivery is not swallowed.
    await Promise.allSettled([...this.inboundOperations]);
    this.typingLeases.releaseAll();
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

  /**
   * typing 的「门后即亮」段：agent-attached 消息在权限校验通过、进入附件组装/转写与接受之前
   * 亮起；其后的任何出口（组装失败、附件拒绝、重投去重、steer 命中、任务接受、抛错）都被
   * finally 覆盖，租赁不会因规格未枚举的出口泄漏。任务被持久接受时 `acceptTask` 把 task 租约
   * 并入同一条续期环，本段退出仅释放消息租约，环不重启。
   */
  private async withMessageTyping<T>(message: InboundMessage, work: () => Promise<T>): Promise<T> {
    const owner = `msg:${message.messageId}`;
    this.typingLeases.acquire(message, owner);
    try {
      return await work();
    } finally {
      this.typingLeases.release(message, owner);
    }
  }

  /**
   * Single shutdown gate for inbound continuations: any path that has not yet durably accepted the
   * message (task row, completed claim, delivered reply) rejects instead of silently resolving,
   * because the transport ACKs a resolved handler.
   */
  private throwIfStopping(): void {
    if (this.stopping) throw new InboundShutdownError();
  }

  async handleInbound(message: InboundMessage): Promise<void> {
    // Tracked so `stop` can await the interrupted handler's cleanup before closing the store.
    const operation = this.processInbound(message);
    this.inboundOperations.add(operation);
    const cleanup = () => {
      this.inboundOperations.delete(operation);
    };
    void operation.then(cleanup, cleanup);
    await operation;
  }

  private async processInbound(message: InboundMessage): Promise<void> {
    // A message that arrives during shutdown is never accepted: reject so the transport does not
    // ACK it; the same message is redelivered after restart.
    this.throwIfStopping();
    if (!this.started || !this.bot) return;
    if (message.messageId.trim().length === 0 || message.senderId.trim().length === 0) return;
    this.history.record(message);
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
    const allowed = await this.authorization.isAllowed(message, ownerId);
    this.throwIfStopping();
    if (!allowed) {
      await this.authorization.handleUnlistedOwnerContact(message, ownerId);
      return;
    }
    // 已授权的新入站说明这个聊天仍在继续：排定中的归档随之取消（用户回来了就不归档）。
    this.pendingThreadArchives.delete(targetKey(message));
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
      await this.authorization.noticeMemberCommand(message);
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
      await this.withMessageTyping(message, async () => {
        const slashPrompt = await this.materializer.materialize(message);
        if (slashPrompt === null) return;
        this.throwIfStopping();
        await this.acceptTask(message, slashPrompt);
      });
      return;
    }
    if (await this.questions.answerPendingQuestion(message, text)) return;
    await this.withMessageTyping(message, async() => {
      const promptText = await this.materializer.materialize(message);
      if (promptText === null) return;
      this.throwIfStopping();
      if (await this.appendToRunningTask(message, promptText)) return;
      await this.acceptTask(message, promptText);
    });
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
      : await this.sessions.reconcileExternalBinding(target);
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
    this.throwIfStopping();
    if (this.store.getChatMode(this.bot!.uid, message) !== "steer") return false;
    const runtime = this.runningRuntime(message);
    if (!runtime) return false;
    const claim = this.commandClaim(message, "steer");
    if (!this.store.claimCommand(claim)) return true;
    if (await this.deliverSteer(message, text)) {
      this.store.completeCommand(claim);
      this.history.recordSteered(message, runtime.worker?.sessionId ?? "");
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
    await this.withMessageTyping(message, async () => {
      const body = await this.materializer.materialize(message);
      if (body === null) return;
      this.throwIfStopping();
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
    });
  }

  private async handleObservingCommand(
    message: InboundMessage,
    command: { name: string; argument: string },
    handler: ChannelCommandHandler,
  ): Promise<void> {
    const body = await this.withMessageTyping(message, async () => {
      const materialized = await this.materializer.materialize(message);
      if (materialized === null) return null;
      return (await this.acceptTask(message, materialized, false)) ? materialized : null;
    });
    if (body === null) return;
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
        await this.sessions.handleNew(target, argument);
      } else if (name === "thread") {
        await this.handleThreadCommand(message, argument);
      } else if (name === "move") {
        await this.sessions.handleMove(target, argument);
      } else if (name === "cancel" || name === "stop") {
        await this.handleCancel(target);
      } else if (name === "interrupt") {
        await this.handleInterrupt(target);
      } else if (name === "queue") {
        await this.handleQueue(message, argument);
      } else if (name === "mode") {
        await this.handleMode(target, argument);
      } else if (name === "switch") {
        await this.models.handleSwitch(message, argument);
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
    await this.withMessageTyping(message, async () => {
      const request = await this.materializer.materialize(message, intent);
      if (request === null) return;
      const body = threadIntentPrompt(request);
      if (await this.appendToRunningTask(message, body)) return;
      await this.acceptTask(message, body);
    });
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
    await this.withMessageTyping(message, async () => {
      const body = threadMergePrompt(binding.cwd, mergeNote);
      if (await this.appendToRunningTask(message, body)) return;
      await this.acceptTask(message, body);
    });
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
    await this.withMessageTyping(message, async () => {
      const body = await this.materializer.materialize(message, text);
      if (body === null) return;
      await this.acceptTask(message, body);
    });
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
    this.throwIfStopping();
    let binding = await this.sessions.reconcileExternalBinding(
      message,
      this.store.getBinding(this.bot!.uid, message),
    );
    if (!binding) {
      let cwd: string;
      try {
        cwd = await this.sessions.defaultWorkDirectory(message);
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
      const cwd = await this.sessions.defaultWorkDirectory(message).catch(() => current.cwd);
      if (cwd !== current.cwd) {
        binding = { ...current, cwd };
        try {
          this.store.ensureBinding(binding);
        } catch {
          throw new Error("本地状态盘写入失败，任务未接收。");
        }
      }
    }
    // Shutdown can land during the binding probes above; never persist an acceptance from a
    // redeliverable message.
    this.throwIfStopping();
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
    // 持久接受即点亮 task 租约：排队等待（同聊天或跨聊天同目录）到终态都保持「正在输入」。
    this.typingLeases.acquire(message, `task:${task.id}`);
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
      const binding = await this.sessions.reconcileExternalBinding(
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
        const binding = await this.sessions.reconcileExternalBinding(
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

        const refreshed = await this.sessions.reconcileExternalBinding(
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
      planThreadArchive: (target, groupId, shortId) => {
        this.pendingThreadArchives.set(targetKey(target), { target: { ...target }, groupId, shortId });
      },
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
    const bindingAtStart = await this.sessions.reconcileExternalBinding(
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
    // 防御性幂等点亮：正常路径在接受时已持有 task 租约（此处 no-op）；任何未来触达路径自动点亮。
    this.typingLeases.acquire(taskTarget(task), `task:${taskId}`);
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
    const textDelivery = this.outbox.registerTask(taskId);
    const target = taskTarget(task);
    const progress = this.progressCards.startTask(taskId, target);
    const assistantTexts = new Map<string, string>();
    let snapshot: GroupContext | null = null;
    let contextInjection: ContextInjection | undefined;
    let currentContextSidecarPath: string | undefined;
    let contextNeedsCommit = false;
    /**
     * 本地命令回合（如 `/autoresearch` 的开关/清理）：无 agent 生成，OMP 不会写群说明
     * prepared receipt。置位后本回合跳过 context commit，但 sidecar 照常清理、ledger 保持
     * pending，下一个真实生成回合仍以 initial 注入并用真实 receipt 提交。
     */
    let localOnly = false;
    let startupPhase: TaskStartupPhase = "context";

    runtime.stopActivity = (abortProgress) => progress.stop(abortProgress);
    runtime.stopText = () => textDelivery.stop();
    runtime.rollProgressCard = () => progress.roll();

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
      const onEvent = (event: WorkerEvent): void => {
        if (runtime.cancelRequested || this.stopping) return;
        if (event.type === "notice") {
          // 扩展的通知帧（如 /autoresearch 的 enable/off/拒绝原因）是信息性 UI：原样投递到
          // 对应会话、带上级别标签，但它不是回合产出——不进 assistantTexts/resultText、不开
          // 进度卡，也绝不能改 workAfterStop，让 /interrupt 的归因保持诚实。
          if (event.messageId.trim().length === 0 || event.text.length === 0) return;
          const label = event.level === "error"
            ? "OMP 错误："
            : event.level === "warning"
              ? "OMP 警告："
              : "OMP：";
          this.outbox.enqueueStreamText(target, task.id, {
            type: "text",
            messageId: `notice:${event.messageId}`,
            text: `${label}${event.text}`,
            final: true,
          });
          return;
        }
        if (event.type === "local_command") {
          // no-agent 完成帧：标记本回合不产生 prepared receipt，commit 门限据此豁免；同样
          // 在 workAfterStop 之前返回，既不算工作产出也不触发进度。
          localOnly = true;
          return;
        }
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
        progress.apply(event);
      };
      const historyTurn = await this.history.prepareTurn(task, target, verified.sessionId);
      const promptContext: PromptContext = {
        tools: createHostTools(this.hostToolHost(), task, snapshot),
        ...(contextInjection === undefined ? {} : { contextInjection }),
      };
      runtime.promptInFlight = true;
      try {
        await worker.prompt(historyTurn?.text ?? task.text, onEvent, promptContext);
      } finally {
        runtime.promptInFlight = false;
        if (historyTurn) this.history.commitTurn(target, verified.sessionId, historyTurn);
      }
      if (runtime.cancelRequested) {
        this.store.markCancelled(taskId);
        progress.finish("cancelled");
        await this.sendCancelledIfNeeded(runtime);
      } else if (runtime.stopCause !== null && !runtime.workAfterStop) {
        this.store.markInterrupted(
          taskId,
          runtime.stopCause === "question"
            ? "等待回答超时，回合已停止"
            : "用户中断当前回合",
        );
        progress.finish("cancelled");
      } else {
        if (contextNeedsCommit && !localOnly) {
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
        progress.finish("completed");
        this.outbox.flush(target);
      }
    } catch (error) {
      if (runtime.cancelRequested) {
        this.store.markCancelled(taskId);
        progress.finish("cancelled");
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
        progress.finish("failed");
        await this.outbox.enqueueText(runtime.target, failure.notice, taskId);
      }
    } finally {
      progress.stop(!progress.finished);
      this.typingLeases.release(runtime.target, `task:${taskId}`);
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
      await this.runScheduledArchive(target, progress.tail);
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
   * 排定的子区归档收尾：先等本回合的进度卡终态编辑与出站投递排空——回复先送达、归档后执行，
   * 因为任何发进子区的消息都会把归档子区解档——再按重试策略调用 `archiveThread`。成功留一条
   * 日志；失败只给发起聊天一条 ≤200 字符的单行说明，不改变已经发生的合并；停止时放弃。
   */
  private async runScheduledArchive(target: ChatTarget, progressTail: Promise<void>): Promise<void> {
    const key = targetKey(target);
    const pending = this.pendingThreadArchives.get(key);
    if (!pending || this.stopping) return;
    await progressTail.catch(() => {});
    await this.drainChatOutbox(target);
    try {
      const outcome = await this.archivePendingThread(key, pending);
      if (outcome === "archived") {
        console.error(`[thread-archive] 已归档子区 ${pending.shortId} target=${key}`);
      }
    } catch (error) {
      const reason = failureText(error, "归档失败");
      console.error(`[thread-archive] 归档失败 target=${key} short=${pending.shortId}：${reason}`);
      this.outbox.enqueueText(target, `归档失败：${reason}`, null);
    } finally {
      if (this.pendingThreadArchives.get(key) === pending) this.pendingThreadArchives.delete(key);
    }
  }

  /**
   * 一次排定归档的调用：第 1 次立即执行，瞬时失败（5xx 或 bot API 的 `store_failed`）每间隔
   * 5 秒重试一次、总共至多 5 次尝试；永久失败即刻抛出。每次尝试前确认排定仍在（该聊天的新
   * 入站会取消它）且服务未在停止。
   */
  private async archivePendingThread(
    key: string,
    pending: PendingThreadArchive,
  ): Promise<"archived" | "cancelled"> {
    let attempt = 1;
    for (;;) {
      if (this.stopping || this.pendingThreadArchives.get(key) !== pending) return "cancelled";
      try {
        await this.options.transport.archiveThread(pending.groupId, pending.shortId);
        return "archived";
      } catch (error) {
        if (attempt >= THREAD_ARCHIVE_ATTEMPTS || !isTransientServerFailure(error)) throw error;
        attempt += 1;
        // 停止信号一到就提前结束，关停不为一轮重试等满一次间隔。
        await new Promise<void>((resolve) => {
          const timer = setTimeout(resolve, THREAD_ARCHIVE_RETRY_MS);
          void this.stopped.promise.then(() => {
            clearTimeout(timer);
            resolve();
          });
        });
      }
    }
  }

  /**
   * 等该聊天的出站队列排空（有界轮询）：回复与文件先送达，归档才排得上。超时照常归档——残余
   * 消息可能稍后把子区解档为 active，可重复调用补做。
   */
  private async drainChatOutbox(target: ChatTarget): Promise<void> {
    const botUid = this.bot?.uid;
    if (botUid === undefined) return;
    for (let waited = 0; waited < THREAD_ARCHIVE_DRAIN_MS; waited += THREAD_ARCHIVE_DRAIN_POLL_MS) {
      await this.outbox.flush(target).catch(() => {});
      if (this.stopping) return;
      if (this.store.countPendingOutbox(botUid, target) === 0) return;
      await new Promise<void>((resolve) => {
        setTimeout(resolve, THREAD_ARCHIVE_DRAIN_POLL_MS);
      });
    }
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