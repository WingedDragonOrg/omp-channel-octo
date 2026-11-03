import { GroupHistoryCoordinator } from "./inbound/history-coordinator.ts";
import { ModelSwitchController } from "./commands/model-controller.ts";
import { modelPolicyDescription, modelWithLevel } from "./commands/model-switch.ts";
import { formatGoalSession, goalErrorText, GOAL_USAGE, parseGoalCommand } from "./commands/goal-command.ts";
import { CONFIG_CARD_ACTION_PREFIX, createConfigMenuDesk, type ConfigMenuDesk } from "./commands/command-cards.ts";
import { ProgressCards, type TaskProgress } from "./cards/progress-cards.ts";
import type { CommitContent } from "./cards/commit-content.ts";
import { COMMIT_RESULT_UNKNOWN, type ExecutionEvent } from "./cards/execution-tracker.ts";
import { CHAT_GRANT_ACTION_PREFIX } from "./cards/chat-grant-card.ts";
import {
  isCommand,
  normalizeCommandHandlers,
  parseCommand,
  resolveBuiltinAction,
  type BuiltinAction,
  type BuiltinHandlers,
} from "./commands/command-routing.ts";
import { SessionLifecycle, threadWorkspaceIsAvailable } from "./session/session-lifecycle.ts";
import {
  TaskScheduler,
  resourcePathsRelated,
  type DestructiveCommandReservation,
  type CommitLease,
  type SessionLease,
} from "./tasks/task-scheduler.ts";
import { createHash, randomUUID } from "node:crypto";
import { lstat, rm } from "node:fs/promises";
import { dirname, join, resolve } from "node:path";
import {
  ATTACHMENT_MAX_AGE_MS,
  sweepAttachments,
} from "./inbound/attachments.ts";
import { ChatAuthorization } from "./inbound/chat-authorization.ts";
import { createChatGrantDesk, type ChatGrantDesk } from "./inbound/chat-grants.ts";
import { acquireBotLock, assertBotLockFree, type LockHandle } from "./runtime/bot-lock.ts";
import { openTrackedCommit, openTrackedWorker, type TrackedCommit, type TrackedWorker, type WorkerSession } from "./runtime/worker-lease.ts";
import { TaskStartupError, WorkerGoalError } from "./contracts.ts";
import type {
  BotIdentity,
  ChannelCommandHandler,
  ChannelService,
  ChannelServiceOptions,
  ChannelStatus,
  ChatAppendMode,
  ChatExecutionState,
  ChatTarget,
  CommitResult,
  ContextInjection,
  PromptContext,
  InboundMessage,
  SteerConsumedEvent,
  WorkerEvent,
  WorkerGoal,
  WorkerGoalResult,
  WorkerGoalRun,
  WorktreeMergeLifecycle,
  WorktreeMergeOutcome,
  WorktreeMergePlan,
  WorktreeMergeProcessIdentity,
  WorktreeRemovalLifecycle,
  WorktreeRemovalStep,
  WorktreeMergeRequest,
  WorkerOptions,
  WorktreeMergeSummary,
} from "./contracts.ts";
import type {
  BindingRecord,
  GoalTaskRecord,
  GoalTaskRequest,
  HostMergeLeaseRecord,
  HostRemovalLeaseRecord,
  HostThreadRemovalOutcome,
  InboundClaim,
  TaskRecord,
  ThreadListenRecord,
  ThreadMergeEventRecord,
  ThreadWorkspaceRecord,
} from "./store/store-records.ts";
import { DEFAULT_ASK_TIMEOUT_MS } from "./contracts.ts";
import { TurnContext } from "./session/turn-context.ts";
import { memberSnapshotFor, normalizeAgentMembers, type AgentContext, type AgentMemberSnapshot } from "./session/agent-context.ts";
import { threadShortId, threadParentGroupId } from "./threads/thread-target.ts";
import { createBotEventLoop, type BotEventLoop } from "./bot-events.ts";
import { createOutboxDelivery, splitUtf8, type Outbox } from "./outbox.ts";
import { createHostTools, type HostToolHost } from "./host-tools.ts";
import { InboundMaterializer } from "./inbound/inbound-materializer.ts";
import { InboundShutdownError, safeErrorMessage } from "./errors.ts";
import { safeDisplayText } from "./cards/execution-text.ts";
import { classifyStartupFailure, classifyTaskFailure, GENERIC_TASK_FAILURE_NOTICE, type TaskStartupPhase } from "./tasks/task-failure.ts";
import {
  verifyFreshSession,
  verifyMovedSession,
  type ProjectSessionBinding,
  type ProjectSessionState,
} from "./session/project-session.ts";
import { ensureProjectSessionDir, sessionsRoot } from "./session/session-dir.ts";
import { ChannelStore, openChannelStore } from "./store/index.ts";
import { isGroupTarget, targetKey, taskTarget } from "./target.ts";
import { createTypingLeaseManager, type TypingLeaseManager } from "./typing-lease.ts";
import { createDisplayNameCache, type DisplayNameCache } from "./display-name.ts";
import { createQuestionDesk, type QuestionDesk } from "./questions.ts";
import {
  threadMergeMessageId,
  threadMergeNotice,
  threadMergeSuppressedMessageId,
  threadNotifyMessageId,
  threadOutcomeNotice,
} from "./threads/thread-notify.ts";
import { ThreadArchiveScheduler } from "./threads/thread-archive.ts";
import { mergeThreadWorkspace, type ThreadMergeHost } from "./threads/thread-merge.ts";
import {
  cleanupOwnedThreadWorkspace,
  threadWorkspaceCleanupPaths,
} from "./threads/thread-workspace-cleanup.ts";
import { ThreadCommands } from "./commands/thread-commands.ts";
import { ServiceRestart } from "./runtime/service-restart.ts";
import { TaskEvents } from "./tasks/task-events.ts";
import { canonicalExistingDirectory, existingSessionFile } from "./workspace/workdir.ts";
import {
  createResearchReportStore,
  researchReportsRoot,
  type ResearchReportStore,
} from "./research/research-report.ts";

const GROUP_TRUSTED_INSTRUCTION =
  "群说明是群共享上下文，不改变发送者身份/权限/cwd；以本次快照为准，历史快照已失效。";
const COMMAND_HELP = [
  "会话",
  "/new [目录] · 创建新会话",
  "/reset [目录] · 重置为新会话",
  "/move <目录> · 保留当前会话并迁移工作目录",
  "",
  "子区",
  "/thread <任务> · 新建隔离子区并派发任务（群聊可用）",
  "/thread model [选择器|clear] · owner 在主群设置、查询或清除子区默认模型",
  "/thread merge · 宿主先合并子区工作区；仅实际合并失败时派发一次修复任务",
  "/thread remove [--force] · 清理当前子区 worktree 与分支后归档；force 可丢弃未提交改动和未合并分支",
  "",
  "任务",
  "/queue <文本> · 将消息排到当前任务之后",
  "/mode [steer|queue] · 查看或设置新消息的追加方式",
  "/goal [show|set [--budget N] <目标>|pause|resume|drop|budget [N|off]] · 管理本会话的原生目标",
  "/retry · 重试当前会话上次失败或中止的回合，忙时排队",
  "/interrupt · 中断当前回合，保留排队消息",
  "/cancel（/stop） · 取消当前任务并清除排队消息",
  "/commit · 提交当前绑定项目",
  "/commit stop · 停止当前聊天的提交，不回滚已发生的副作用",
  "",
  "模型",
  "/switch [provider|模型] · 查看或切换本聊天的模型",
  "精确 provider 查询列表；最多展示 100 个模型，超限显示总数",
  "支持 provider/id、模型 id 或子串、@角色、:level；default 清除本聊天覆盖（子区继承主群默认，否则由 OMP 决定）",
  "",
  "其他",
  "/status · 查看当前状态",
  "/restart [now] · 重启 channel，在途任务重启后续接；两种写法等价",
  "/autoresearch-history · 导出本聊天最近一次研究的完整记录",
  "/help · 查看帮助",
].join("\n");
const MODE_USAGE =
  "用法：/mode [steer|queue]。steer（默认，default 同义）把运行中的新消息注入当前回合；queue 把它排到当前回合之后。无参数时报告当前设置。";
/** `/mode` 卡片的两个取值：与文本命令接受的写法同义，取值原样进 store。 */
const MODE_CARD_CHOICES: readonly { title: string; value: string }[] = [
  { title: "steer：运行中的新消息注入当前回合", value: "steer" },
  { title: "queue：运行中的新消息排到当前回合之后", value: "queue" },
];
const QUEUE_USAGE = "用法：/queue <文本>；把该消息排到当前任务之后执行。";
const COMMIT_USAGE = "用法：/commit（提交当前项目）或 /commit stop（停止当前聊天的提交）。";
const DEFAULT_INTERRUPTED_TEXT = "已中断当前回合；已排队的消息会继续执行。";
const DEFAULT_INTERRUPT_REJECTED_TEXT = "中断请求未被 OMP 接受，当前回合仍在运行。";
const DEFAULT_INTERRUPT_IDLE_TEXT =
  "当前没有正在运行的回合可中断；任务尚未进入执行阶段时可用 /cancel 取消。";
const COMMIT_BUSY_TEXT = "当前项目有任务正在运行或排队，提交操作繁忙，未启动提交。";
/** Steered messages tracked for the consumption receipt; older entries fall out of observation. */
const MAX_PENDING_STEERS = 32;
const THREAD_WORKSPACE_UNAVAILABLE_TEXT = "当前子区工作区未就绪或资源已缺失，未接收任务。";
const LIFECYCLE_NOTICE_TIMEOUT_MS = 2_000;

type LifecycleNotice = "startup" | "stop";

/** Format wall-clock fields and their offset together, rather than pairing local time with UTC Z. */
function localTimestamp(date: Date): string {
  const pad = (value: number): string => String(value).padStart(2, "0");
  const offsetMinutes = -date.getTimezoneOffset();
  const offsetSign = offsetMinutes >= 0 ? "+" : "-";
  const absoluteOffset = Math.abs(offsetMinutes);
  return `${date.getFullYear()}-${pad(date.getMonth() + 1)}-${pad(date.getDate())}`
    + `T${pad(date.getHours())}:${pad(date.getMinutes())}:${pad(date.getSeconds())}`
    + `${offsetSign}${pad(Math.floor(absoluteOffset / 60))}:${pad(absoluteOffset % 60)}`;
}
/** Characters of the triggering message shown in a start/consumption receipt. */
const RECEIPT_EXCERPT_CHARS = 20;
const DEFAULT_CANCELLED_TEXT = "当前任务已取消；不回滚已发生的副作用。";
/**
 * 中断原因持久化供在线结算与重启恢复共用，用户任务回执使用固定终态说明，不传递此原始原因。
 */
const INTERRUPT_REASON_TEXT = "用户中断当前回合";
const QUESTION_TIMEOUT_REASON_TEXT = "等待回答超时，回合已停止";
/** 1/0 行的实际恢复失败说明:prompt 标记已落、OMP 是否接受未知，不放行也不重放。 */
const UNCERTAIN_RECOVERY_NOTICE =
  "任务未自动恢复：无法确认 OMP 是否已接受该输入，请核对会话后继续。";
const RESEARCH_HISTORY_EMPTY_TEXT =
  "本聊天还没有可导出的研究记录：先让任务运行 /autoresearch 并完成记录后再试。";
const MEMBER_LOOKUP_TIMEOUT_MS = 5_000;
const MEMBER_LOOKUP_FAILURE_STEER_NOTICE = "成员信息读取失败，本条消息未并入当前回合。";
type RuntimeTask = {
  taskId: string;
  isGoalTask: boolean;
  target: ChatTarget;
  cwd: string;
  lease?: TrackedWorker;
  cancelRequested: boolean;
  cancelController: AbortController;
  cancelReplySent: boolean;
  /**
   * 停机挂起：关停时置位，`runTask` 据此不把中断当成任务失败/取消——行保留 prompt 标记，
   * 由下一个进程续接。只由 `stopInternal` 写。
   */
  suspended: boolean;
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
  /** Serializes each member lookup, sidecar publication and worker delivery in input order. */
  steerSerial: Promise<void>;
  turnContext: TurnContext;
  agentContext?: AgentContext;
  stopActivity?: (abortProgress?: boolean) => void;
  stopText?: () => void;
  /** Closes the progress card of the round that just ended; a steered message starts a new round. */
  rollProgressCard?: () => void;
};

class GoalDirectoryBusyError extends Error {
  constructor() {
    super("原生目标控制命令遇到忙目录");
    this.name = "GoalDirectoryBusyError";
  }
}

type CommitOperation = {
  taskId: string;
  target: ChatTarget;
  root: string;
  binding: BindingRecord;
  reservation: CommitLease;
  done: Promise<void>;
  finish(): void;
  workerReady: Promise<void>;
  markWorkerReady(): void;
  tracked?: TrackedCommit;
  result?: CommitResult;
  stopPromise?: Promise<boolean>;
  stopRequested: boolean;
  stopConfirmed?: boolean;
  shutdownRequested: boolean;
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
function isNativeRetryPrompt(text: string): boolean {
  return /^\/retry(?:\s|$)/u.test(text.trimStart());
}

function statusState(task: TaskRecord | null, hasBinding: boolean): ChatExecutionState {
  if (!task) return hasBinding ? "idle" : "none";
  return task.state;
}

function createSessionId(): string {
  return randomUUID();
}
function threadMergeAbortError(signal: AbortSignal): Error {
  return signal.reason instanceof Error ? signal.reason : new Error("thread merge aborted");
}

async function waitWithAbort<T>(operation: Promise<T>, signal: AbortSignal): Promise<T> {
  if (signal.aborted) throw threadMergeAbortError(signal);
  const aborted = Promise.withResolvers<never>();
  const onAbort = (): void => aborted.reject(threadMergeAbortError(signal));
  signal.addEventListener("abort", onAbort, { once: true });
  try {
    return await Promise.race([operation, aborted.promise]);
  } finally {
    signal.removeEventListener("abort", onAbort);
  }
}

async function pauseThreadMergeWait(signal: AbortSignal): Promise<void> {
  const completion = Promise.withResolvers<void>();
  const finish = (): void => {
    clearTimeout(timer);
    signal.removeEventListener("abort", finish);
    completion.resolve();
  };
  const timer = setTimeout(finish, 50);
  if (signal.aborted) finish();
  else signal.addEventListener("abort", finish, { once: true });
  await completion.promise;
  if (signal.aborted) throw threadMergeAbortError(signal);
}

class CoreService implements ChannelService {
  private readonly history: GroupHistoryCoordinator;
  private readonly models: ModelSwitchController;
  /** `/switch`、`/mode` 配置卡片的进程内会话：绑定、owner 校验、应用串行与有界回收都在它内部。 */
  private readonly configMenus: ConfigMenuDesk;
  /** Owner-confirmed unlisted-chat cards and their durable SQLite grant operations. */
  private readonly chatGrants: ChatGrantDesk;
  private readonly authorization: ChatAuthorization;
  private readonly materializer: InboundMaterializer;
  private readonly progressCards: ProgressCards;
  private readonly sessions: SessionLifecycle;
  /** 任务排定与按 cwd 的租约：队列、等待回执与目录链都由它独占。 */
  private readonly scheduler: TaskScheduler;
  /** In-flight inbound handlers: stop aborts their I/O and awaits their cleanup before closing the store. */
  private readonly inboundOperations = new Set<Promise<void>>();
  /** Shutdown cancellation source for inbound I/O (downloads, transcription, mention-pref lookups). */
  private readonly shutdownController = new AbortController();
  private readonly runtimes = new Map<string, RuntimeTask>();
  /** In-flight native-state reads invalidated by a later explicit goal stop. */
  private readonly goalResumeChecks = new Map<string, { generation: number; active: number }>();
  /** Active host-first thread merges: /cancel, /interrupt and stop abort waits and wt execution. */
  private readonly threadMergeControllers = new Map<string, Set<AbortController>>();
  /** Active host merge command identities stay registered through durable receipt/repair acceptance. */
  private readonly threadMergeControllersByCommand = new Map<string, AbortController>();
  /** Active owner-triggered removals share the same cancel/stop discipline as host merges. */
  private readonly threadRemovalControllers = new Map<string, Set<AbortController>>();
  private readonly threadRemovalControllersByCommand = new Map<string, AbortController>();
  /** Per-chat progress edit tails remain visible after task leases release until terminal edits settle. */
  private readonly progressTails = new Map<string, Map<string, () => Promise<void>>>();
  /** A host command's archive is published only after its durable success receipt is inserted. */
  private readonly pendingHostMergeArchives = new Map<string, {
    readonly target: ChatTarget;
    readonly groupId: string;
    readonly shortId: string;
    readonly generation: number;
    readonly receiptOutboxIds: readonly string[];
  }>();
  /** Startup-recovered archives stay staged until progress-card recovery and transport readiness. */
  private readonly recoveredHostMergeArchiveTargets = new Set<string>();
  private readonly commitOperationsByTask = new Map<string, CommitOperation>();
  private readonly commitOperationsByTarget = new Map<string, CommitOperation>();
  private readonly blockedDirectories = new Set<string>();
  /** 问题与问答卡：等待者、卡片代次与问题行的所有者。 */
  /** 出站投递：文字、流式编辑与文件共用的队列。 */
  private readonly outbox: Outbox;
  private readonly questions: QuestionDesk;
  /** 回答署名的显示名缓存：uid → 人名，有界、失败即回退（署名渲染用，不参与身份判断）。 */
  private readonly displayNames: DisplayNameCache;
  /** Bot 事件队列消费（卡片点击）。 */
  private readonly botEvents: BotEventLoop;
  /** 研究完整记录的私有落盘：按 bot+聊天原子替换 latest，导出时复制不可变 spool。 */
  private readonly researchReports: ResearchReportStore;
  private readonly commandHandlers: ReadonlyMap<string, ChannelCommandHandler>;
  /** 内建命令的 canonical handler 表：识别（alias 表）与分派共用同一份 canonical action。 */
  private readonly builtinHandlers: BuiltinHandlers;
  /** typing 租约：agent-attached 消息从门后到任务终态的持续「正在输入」。 */
  private readonly typingLeases: TypingLeaseManager;
  private bot: BotIdentity | undefined;
  /** Captured after per-bot credential provisioning; never inferred from CLI installation. */
  private cliIdentityReady = false;
  private readonly activityOperations = new Set<Promise<void>>();
  /**
   * 恢复任务先占调度序、后执行所等的启动门：`start()` 接线全部成功才放行，失败即拒绝，
   * 已排定的恢复任务随之放弃（行留在 queued，下一次进程再试）。
   */
  private startupGate = Promise.withResolvers<void>();
  private lock: LockHandle | undefined;
  private started = false;
  private stopping = false;
  /** True after transport readiness and task recovery preparation, until shutdown completes. */
  private ready = false;
  /** Coalesces concurrent start calls and lets stop await an in-flight startup notice. */
  private startPromise: Promise<void> | undefined;
  /** channel 重启的所有者：/restart 校验、延期重启排定与跨进程重启通报都不再留在服务里。 */
  private readonly serviceRestart: ServiceRestart;
  /** `/thread` 家族命令的所有者：建区意图交接、宿主优先合并与资格文案都不再留在服务里。 */
  private readonly threadCommands: ThreadCommands;
  /** 子区归档排定的所有者：排定 map 与停止信号都在它内部，服务只负责接线与调用。 */
  private readonly threadArchives: ThreadArchiveScheduler;
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
      // 窄面在调用时解引用 `this.configMenus`：卡片菜单对象在紧随其后构造，箭头函数此时不解引用。
      menu: {
        begin: (target, kind) => this.configMenus.begin(target, kind),
        open: (target, view, ticket) => this.configMenus.open(target, view, ticket),
        invalidate: (target, kind) => this.configMenus.invalidate(target, kind),
        serialize: (kind, target, work) => this.configMenus.serialize(kind, target, work),
      },
    });
    this.configMenus = createConfigMenuDesk({
      store: this.store,
      transport: options.transport,
      botUid: () => this.bot?.uid,
      stopping: () => this.stopping,
      // 点击时刻重新校验聊天：私聊沿用消息门的 owner 判定（点击者非 owner 时更早被拒），群/子区
      // 仍须命中允许清单——被移出允许清单的聊天不能让旧卡片继续改配置。
      chatAllowed: target => target.channelType === 1 || this.authorization.isPermittedChat(target),
      shutdownSignal: this.shutdownController.signal,
      enqueueText: (target, text, taskId) => this.outbox.enqueueText(target, text, taskId),
      applyChoice: (kind, target, value) => kind === "switch"
        ? this.models.applySwitch(target, value)
        : Promise.resolve(this.applyModeChoice(target, value)),
    });
    this.progressCards = new ProgressCards({
      store: this.store,
      transport: options.transport,
      botUid: () => this.bot?.uid,
      stopping: () => this.stopping,
      trackOperation: (operation) => this.operationSet(operation),
    });
    // 排定与租约的所有者先于 SessionLifecycle 构造：会话侧的命令租约经端口落到这里。它的
    // reconcileBinding/runTask 回调只在任务真正执行时解引用 `this.sessions`（构造时不解引用，
    // 因此这里先构造不会碰到尚未赋值的字段）。
    this.scheduler = new TaskScheduler({
      store: this.store,
      botUid: () => this.bot?.uid,
      stopping: () => this.stopping,
      isDirectoryBlocked: cwd => this.isWorkspaceDirectoryBlocked(cwd),
      reconcileBinding: (taskId, target, binding) =>
        this.reconcileTaskBinding(taskId, target, binding),
      repositoryRoot: cwd => this.repositoryRoot(cwd),
      repositoryBlocked: root => this.isRepositoryBlocked(root),
      shouldRunTaskBeforeRepositoryProbe: (taskId, target, binding, cwd) =>
        this.shouldRunTaskBeforeRepositoryProbe(taskId, target, binding, cwd),
      runTask: (taskId, cwd) => this.runTask(taskId, cwd),
      awaitStartReady: () => this.startupGate.promise,
      enqueueText: (target, text, taskId) => this.outbox.enqueueText(target, text, taskId),
      notifyThreadOutcome: taskId => this.notifyThreadOutcome(taskId),
    });
    this.sessions = new SessionLifecycle({
      store: this.store,
      workerFactory: options.workerFactory,
      config: options.config,
      bot: () => this.bot,
      hasRuntime: target => this.hasRuntime(target),
      isDirectoryBlocked: cwd => this.isWorkspaceDirectoryBlocked(cwd),
      blockDirectory: cwd => { this.blockedDirectories.add(cwd); },
      tryAcquireCommandSession: (target, cwds) => this.scheduler.tryAcquireCommandSession(target, cwds),
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
    this.researchReports = createResearchReportStore({
      rootDir: researchReportsRoot(options.config.stateDir),
      spoolRootDir: join(options.config.stateDir, "spool"),
    });
    this.chatGrants = createChatGrantDesk({
      store: this.store,
      transport: options.transport,
      bot: () => this.bot,
      stopping: () => this.stopping,
      shutdownSignal: this.shutdownController.signal,
      allowlistWriteback: options.allowlistWriteback,
      isChatPermitted: (target) => this.authorization.isPermittedChat(target),
      enqueueText: (target, text, taskId) => this.outbox.enqueueText(target, text, taskId),
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
      offerUnlistedChat: (message, bot) => this.chatGrants.offer(message, bot),
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
      stopTurnForQuestion: (taskId, cancelQuestionIds) => this.stopTurnForQuestion(taskId, cancelQuestionIds),
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
      // Each card family has its own binding and effect boundary; unknown actions remain ask-card only.
      onCardAction: (action, eventId) => action.actionId.startsWith(CONFIG_CARD_ACTION_PREFIX)
        ? this.configMenus.handleAction(action)
        : action.actionId.startsWith(CHAT_GRANT_ACTION_PREFIX)
          ? this.chatGrants.handleAction(eventId, action)
          : this.questions.answerFromCard(action),
      onCardActionRetriesExhausted: (action, eventId) =>
        this.chatGrants.onRetriesExhausted(eventId, action),
    });
    this.threadArchives = new ThreadArchiveScheduler({
      store: this.store,
      transport: this.options.transport,
      outbox: this.outbox,
      botUid: () => this.bot?.uid,
      stopping: () => this.stopping,
    });
    this.threadCommands = new ThreadCommands({
      store: this.store,
      handleThreadModel: (message, argument) => this.models.handleThreadModel(message, argument),
      replayThreadMergeSuccess: (target, operationId) =>
        this.replayThreadMergeSuccess(target, operationId),
      ensureThreadListen: async message => {
        await this.ensureThreadListen(message);
      },
      refuseIncompleteThreadWorkspace: message => this.refuseIncompleteThreadWorkspace(message),
      botUid: () => this.bot?.uid,
      ...(options.worktrees === undefined ? {} : { worktrees: options.worktrees }),
      archiveGeneration: target => this.threadArchives.generation(target),
      stopping: () => this.stopping,
      executeHostMerge: (message, binding, receiptOutboxId, archiveGeneration, operatorNote) =>
        this.executeHostThreadMerge(message, binding, receiptOutboxId, archiveGeneration, operatorNote),
      executeHostRemoval: (message, ownership, force, receiptOutboxId) =>
        this.executeHostThreadRemoval(message, ownership, force, receiptOutboxId),
      enqueueMergeReceipt: (target, id, text) => this.enqueueThreadMergeReceipt(target, id, text),
      settleHostMerge: (target, sourceId) => this.settleHostMergeForSource(target, sourceId),
      enqueueRemovalReceipt: (target, id, text) => this.enqueueThreadRemovalReceipt(target, id, text),
      settleHostRemoval: (target, sourceId) => this.settleHostRemovalForSource(target, sourceId),
      recoverRemovalArchive: (target, receiptOutboxId, archiveGeneration) =>
        this.recoverHostRemovalArchive(target, receiptOutboxId, archiveGeneration),
      recoverMergeArchive: (target, receiptOutboxId) =>
        this.recoverHostMergeArchive(target, receiptOutboxId),
      runThreadArchive: target => this.runThreadArchive(target),
      materialize: (message, text) => this.materializer.materialize(message, text),
      commandClaim: (message, name) => this.commandClaim(message, name),
      withMessageTyping: (message, work) => this.withMessageTyping(message, work),
      appendToRunningTask: (message, text) => this.appendToRunningTask(message, text),
      acceptTask: (message, text, schedule) =>
        this.acceptTask(message, text, true, undefined, undefined, undefined, schedule),
      enqueueText: (target, text, taskId) => this.outbox.enqueueText(target, text, taskId),
    });
    this.serviceRestart = new ServiceRestart({
      store: this.store,
      stopping: () => this.stopping,
      // OMP runtime、原生 commit 与 host-first merge 共用同一在途视图，延期重启不会截断宿主合并回执。
      hasRunning: () =>
        this.runtimes.size > 0
        || this.commitOperationsByTask.size > 0
        || this.threadMergeControllers.size > 0
        || this.threadRemovalControllers.size > 0,
      restart: options.restart,
      enqueueText: (target, text, taskId) => this.outbox.enqueueText(target, text, taskId),
    });
    // 内建命令的 canonical handler 表：每个 action 一个函数，直接返回原 handler 的 Promise
    // （不再包一层 async），`handleCommand` 里的 await 因此仍是原来的微任务边界；只有
    // `restart` 的返回值参与重启判定。
    this.builtinHandlers = {
      commit: (message, argument) => this.handleCommit(message, argument),
      new: (message, argument) => this.sessions.handleNew(message, argument),
      move: (message, argument) => this.sessions.handleMove(message, argument),
      thread: (message, argument) => this.threadCommands.handle(message, argument),
      cancel: message => this.handleCancel(message),
      interrupt: message => this.handleInterrupt(message),
      queue: (message, argument) => this.handleQueue(message, argument),
      mode: (message, argument) => this.handleMode(message, argument),
      switch: (message, argument) => this.models.handleSwitch(message, argument),
      status: message => this.handleStatus(message),
      goal: (message, argument) => this.handleGoal(message, argument),
      help: message => this.outbox.enqueueText(message, COMMAND_HELP, null),
      "autoresearch-history": message => this.handleResearchHistory(message),
      restart: (message, argument) => this.serviceRestart.handleCommand(message, argument),
    };
  }

  start(): Promise<void> {
    if (this.startPromise) return this.startPromise;
    if (this.started) return Promise.resolve();
    if (this.stopping) return Promise.reject(new Error("channel 正在停止"));
    const startPromise = this.startInternal();
    this.startPromise = startPromise;
    void startPromise.then(
      () => {
        if (this.startPromise === startPromise) this.startPromise = undefined;
      },
      () => {
        if (this.startPromise === startPromise) this.startPromise = undefined;
      },
    );
    return startPromise;
  }

  private async startInternal(): Promise<void> {
    if (this.stopping) throw new Error("channel 正在停止");
    // 每次启动一条新门：这一批排定任务要么等到接线成功，要么随失败一起被放弃。
    this.startupGate = Promise.withResolvers<void>();
    // 没有恢复任务等待时也不产生未处理拒绝（门只在 start 失败时拒绝）。
    void this.startupGate.promise.catch(() => {});
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
      this.cliIdentityReady = false;
      const cliCredentials = this.options.cliCredentials;
      if (cliCredentials) {
        try {
          await cliCredentials.provision(identity.uid);
          const env = cliCredentials.env();
          this.cliIdentityReady = typeof env?.OCTO_CONFIG_DIR === "string"
            && env.OCTO_CONFIG_DIR.trim().length > 0
            && typeof env.OCTO_API_BASE_URL === "string"
            && env.OCTO_API_BASE_URL.trim().length > 0
            && env.OCTO_BOT_ID === identity.uid;
        } catch (error) {
          console.error(
            `[octo-cli] 凭据准备失败：${error instanceof Error ? error.message : String(error)}`,
          );
        }
      }
      // 附件目录按 14 天保留期清理；best-effort，清理失败不影响启动。
      await sweepAttachments(this.options.config.stateDir, ATTACHMENT_MAX_AGE_MS).catch(() => 0);
      const configuredOwner =
        typeof this.options.config.ownerId === "string" ? this.options.config.ownerId.trim() : "";
      if (configuredOwner) {
        this.store.setOwnerId(identity.uid, configuredOwner);
      }
      // Known host merge outcomes are never re-run. Failed command outcomes restore repair tasks
      // before the restart snapshot; successful notices are delivered after recovered work is staged.
      // Unknown or outcome-less rows remain fail-closed, with exact process-group recovery first.
      const successfulHostMergeRecoveries: HostMergeLeaseRecord[] = [];
      for (const lease of this.store.listHostMergeLeases(identity.uid)) {
        if (lease.outcome !== null) {
          if (lease.outcome.kind === "succeeded") {
            successfulHostMergeRecoveries.push(lease);
          } else if (lease.commandText !== null) {
            await this.threadCommands.recoverHostMergeOutcome(lease);
          }
          continue;
        }
        for (const resourceCwd of lease.resourceCwds) this.blockedDirectories.add(resourceCwd);
        const processIdentity = this.hostMergeProcessIdentity(lease);
        if (processIdentity && this.options.worktrees?.recoverMergeProcess) {
          let stopped = false;
          try {
            stopped = await this.options.worktrees.recoverMergeProcess(processIdentity);
          } catch (error) {
            console.error(`[thread merge recovery] 无法确认旧 worktrunk 进程组已停止 cwd=${lease.cwd}: ${safeErrorMessage(error, "process recovery failed")}`);
          }
          if (!stopped) {
            console.error(`[thread merge recovery] 旧 worktrunk 进程组仍未确认停止，目录保持封锁 cwd=${lease.cwd}`);
          }
        }
        this.store.markHostMergeLeaseUnknown(lease.id);
      }
      for (const lease of this.store.listHostRemovalLeases(identity.uid)) {
        if (lease.outcome !== null) {
          try {
            await this.threadCommands.recoverHostRemovalOutcome(lease);
          } catch (error) {
            console.error(`[thread remove recovery] 无法恢复清理回执 id=${lease.id}: ${safeErrorMessage(error, "receipt recovery failed")}`);
          }
          continue;
        }
        this.blockRemovalResources(lease);
        const processIdentity = this.hostRemovalProcessIdentity(lease);
        if (processIdentity && this.options.worktrees?.recoverMergeProcess) {
          let stopped = false;
          try {
            stopped = await this.options.worktrees.recoverMergeProcess(processIdentity);
          } catch (error) {
            console.error(`[thread remove recovery] 无法确认旧清理进程组已停止 cwd=${lease.cwd}: ${safeErrorMessage(error, "process recovery failed")}`);
          }
          if (!stopped) {
            console.error(`[thread remove recovery] 旧清理进程组仍未确认停止，资源保持封锁 cwd=${lease.cwd}`);
          }
        }
        if (lease.phase !== "unknown") this.store.markHostRemovalLeaseUnknown(lease.id);
      }
      // 重启不丢工作：先把每个遗留 worker 的进程组收干净，再动任何任务状态。无法确认死亡的
      // 目录保持 block——它的任务留在队列继续挡住同目录的新写者（runTask 原样放过）。
      for (const ledger of this.store.listWorkerLedger(identity.uid)) {
        const stopped = await this.options.workerFactory.recover(ledger);
        if (stopped) {
          this.store.markWorkerStopped(ledger.id);
        } else {
          this.blockedDirectories.add(ledger.cwd);
          if (ledger.moveTargetCwd) this.blockedDirectories.add(ledger.moveTargetCwd);
        }
      }
      // 旧版重启通报已随续接机制移除：遗留行静默清理，绝不再投递。
      this.serviceRestart.consumeStartupNotices(identity.uid);
      // started→queued（prompt 标记随行保留）由存储域一次性完成；先补发遗留卡片终态，恢复
      // 回合随后接上同一条卡片记录，不与卡片恢复竞争。
      const recovery = this.store.prepareTasksForRestart(identity.uid, {
        interrupt: INTERRUPT_REASON_TEXT,
        question_timeout: QUESTION_TIMEOUT_REASON_TEXT,
      });
      await this.progressCards.recover();
      // 续接任务重挂提问的快照必须在开放入站之前一次性建好：此后到达的答复由快照认领、交给
      // 续跑的原 ask 消费，不会因「此刻没有在跑的回合」被当成新输入再唤醒一个任务。恢复任务
      // 的实际执行在启动门之后，快照与 `worker.resume` 的先后因此由这里固定；续跑路径不再重复
      // 初始化——重复快照会把已经认领过的行重新交出去。
      for (const task of recovery.queued) {
        if (this.store.isTaskResuming(task.id) || this.store.isGoalTaskResuming(task.id)) this.questions.beginRecovery(task.id);
      }
      // 恢复任务先占住聊天链与目录序，再让 transport 开始接新入站：任何新消息（含下面的
      // 恢复失败回执与按持久化监听策略恢复的子区通知排在前面；恢复任务仍等启动门放行。
      for (const task of recovery.queued) this.scheduler.scheduleTask(task.id);
      for (const task of recovery.stopped) {
        // 用户中断的遗留提问记 `interrupted`（迟到提交不可接受）；提问超时停下的行保持可答。
        if (task.stopIntent !== "question_timeout") this.questions.cancelForTask(task.id, "interrupted");
        await this.notifyThreadOutcome(task.id);
      }
      // 1/0 行（prompt 已落标记、接受状态未知）既不重放也不续接：发送固定恢复失败回执并关闭遗留提问。
      // 通知不传递任务正文、Agent 输出或错误载荷。
      for (const task of recovery.uncertain) {
        // 接受状态未知不是用户叫停：关闭行记 `ended`，迟到提交仍作为该聊天的新输入接受。
        this.questions.cancelForTask(task.id, "ended");
        await this.outbox.enqueueText(taskTarget(task), UNCERTAIN_RECOVERY_NOTICE, null);
        await this.notifyThreadOutcome(task.id);
      }
      for (const task of recovery.goalStopped) {
        this.questions.cancelForTask(task.id, task.goalStopIntent === "interrupt" ? "interrupted" : "ended");
        await this.outbox.enqueueText(
          taskTarget(task),
          `原生目标当前状态为 ${task.goalStatus ?? "unknown"}；续跑授权已撤销，不会自动恢复。请使用 /goal show 核对。`,
          null,
        );
        await this.notifyThreadOutcome(task.id);
      }
      for (const lease of successfulHostMergeRecoveries) {
        if (this.stopping) throw new Error("channel 正在停止");
        const outcome = lease.outcome;
        if (outcome?.kind !== "succeeded") continue;
        await this.persistThreadMergeSuccess(
          { channelId: lease.channelId, channelType: 5 },
          outcome.summary,
          outcome.summary.removed ? "scheduled" : "not-scheduled",
          lease.sourceId,
        );
      }
      await this.recoverThreadMergeEvents(identity.uid);
      for (const lease of successfulHostMergeRecoveries) {
        if (this.stopping) throw new Error("channel 正在停止");
        if (lease.commandText !== null) {
          await this.threadCommands.recoverHostMergeOutcome(lease);
        } else {
          this.store.settleHostMergeLease(lease.id);
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
      await this.chatGrants.recoverPending(identity.uid);
      this.outbox.flushAll();
      this.botEvents.start();
      this.startupGate.resolve();
      this.ready = true;
      this.releaseRecoveredHostMergeArchives();
      await this.sendLifecycleNotice("startup");
    } catch (error) {
      this.ready = false;
      this.started = false;
      this.startupGate.reject(error instanceof Error ? error : new Error("channel 启动失败"));
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
    const wasReady = this.ready;
    this.stopping = true;
    this.abortAllThreadMerges(new Error("channel stopping"));
    this.abortAllThreadRemovals(new Error("channel stopping"));
    this.threadArchives.stop();
    this.pendingHostMergeArchives.clear();
    this.recoveredHostMergeArchiveTargets.clear();
    this.started = false;
    // Seal inbound processing and abort its I/O before any lifecycle send or worker shutdown.
    this.shutdownController.abort();
    // A ready start may still be sending its startup notice; leave the connection open until it ends.
    if (wasReady && this.startPromise) await this.startPromise.catch(() => {});
    if (wasReady) await this.sendLifecycleNotice("stop");
    const runtimes = [...this.runtimes.values()];
    for (const runtime of runtimes) {
      // 停机不是用户的 /cancel：只挂起。任务行保留 prompt 标记，由新进程按续接语义恢复；
      // 遗留提问保持 pending（beginRecovery 重挂），因此这里不再 durably 取消它们。
      runtime.suspended = true;
      runtime.stopText?.();
      runtime.stopActivity?.();
      this.questions.rejectForTask(runtime.taskId, new Error("channel stopping"));
      if (runtime.lease) {
        await runtime.lease.close().catch(() => false);
      }
    }
    for (const operation of [...this.commitOperationsByTask.values()]) {
      operation.shutdownRequested = true;
      await operation.workerReady;
      await this.stopCommitProcess(operation).catch(() => false);
    }
    this.outbox.abortAll();
    await this.scheduler.settle();
    await Promise.allSettled([...this.activityOperations]);
    // Inbound handlers aborted above finish their bounded cleanup here, while the store is still
    // open: interrupted assembly rolls its command claim back so a redelivery is not swallowed.
    await Promise.allSettled([...this.inboundOperations]);
    await this.researchReports.drain();
    this.typingLeases.releaseAll();
    await this.botEvents.stop();
    this.questions.stop();
    this.outbox.stopTimers();
    await this.outbox.settle();
    await this.options.transport.stop().catch(() => {});
    // A start still in preparation or awaiting CONNACK must unwind before its store is closed.
    if (this.startPromise) await this.startPromise.catch(() => {});
    if (this.lock) {
      await this.lock.release().catch(() => {});
      this.lock = undefined;
    }
    this.ready = false;
    this.store.close();
  }

  private async sendLifecycleNotice(notice: LifecycleNotice): Promise<void> {
    if (notice === "startup" && this.stopping) return;
    const botUid = this.bot!.uid;
    try {
      const ownerId = this.store.getOwnerId(botUid)?.trim();
      if (!ownerId) return;
      const controller = new AbortController();
      const body = notice === "startup"
        ? "omp-channel-octo 已启动"
        : "omp-channel-octo 正在停止；在途任务将挂起，启动后继续。";
      const send = this.options.transport.sendText(
        { channelType: 1, channelId: ownerId },
        `${body} 本地时间=${localTimestamp(new Date())} PID=${process.pid}`,
        `lifecycle-${notice}-${botUid}-${process.pid}`,
        controller.signal,
      );
      let timer: NodeJS.Timeout | undefined;
      try {
        await Promise.race([
          send,
          new Promise<never>((_, reject) => {
            timer = setTimeout(() => {
              controller.abort();
              reject(new Error("lifecycle notice timed out"));
            }, LIFECYCLE_NOTICE_TIMEOUT_MS);
          }),
        ]);
      } finally {
        clearTimeout(timer);
      }
    } catch {
      console.error("[lifecycle] 生命周期通知发送失败或超时，继续服务生命周期处理");
    }
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
    if (this.authorization.isOwnerMentionedGrantPassphrase(message, ownerId)) {
      const claimState = this.store.getCommandClaimState(this.bot.uid, message, message.messageId);
      if (claimState === "pending" || claimState === "completed") return;
      if (claimState === "failed") {
        await this.authorization.retryClaimedOwnerGrant(message, ownerId);
        return;
      }
    }
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
    if (!message.supported) {
      const claim = this.commandClaim(message, "unsupported");
      if (!this.store.claimCommand(claim)) return;
      this.threadArchives.cancel(message);
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
      const action = resolveBuiltinAction(command.name);
      if (action !== undefined) {
        await this.handleCommand(message, command.name, command.argument, action);
        return;
      }
      const handler = this.commandHandlers.get(command.name);
      if (handler) {
        if (await this.refuseIncompleteThreadWorkspace(message)) return;
        if (handler.intercept) {
          await this.handleInterceptingCommand(message, command, handler);
        } else {
          await this.handleObservingCommand(message, command, handler);
        }
        return;
      }
      if (await this.refuseIncompleteThreadWorkspace(message)) return;
      // 未注册的 /xxx 不是命令，仍按普通任务走唯一组装点，附件照常取回。
      await this.withMessageTyping(message, async () => {
        const slashPrompt = await this.materializer.materialize(message);
        if (slashPrompt === null) return;
        this.throwIfStopping();
        await this.acceptTask(message, slashPrompt);
      });
      return;
    }
    if (await this.refuseIncompleteThreadWorkspace(message)) return;
    if (await this.questions.answerPendingQuestion(message, text)) {
      this.threadArchives.cancel(message);
      return;
    }
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
      model: this.store.resolveChatModel(this.bot.uid, target).model,
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
   * 绑定核对的服务侧入口（调度与 `runTask` 共用）：恢复中的任务只读原绑定——核对会在会话
   * 搬移时改写绑定行，而恢复必须以崩溃前记录的会话身份与文件为准；缺失或身份不符由
   * `runTask` fail-closed，绝不静默换绑或新建会话。普通任务照常走可写核对与找回。
   */
  private async reconcileTaskBinding(
    taskId: string,
    target: ChatTarget,
    binding: BindingRecord | null,
  ): Promise<BindingRecord | null> {
    if (await this.threadWorkspaceIncomplete(target)) return binding;
    if (
      binding !== null
      && (this.store.isTaskResuming(taskId) || this.store.getGoalTask(taskId) !== null)
    ) return binding;
    return this.sessions.reconcileExternalBinding(target, binding);
  }

  private hostMergeProcessIdentity(
    lease: HostMergeLeaseRecord,
  ): WorktreeMergeProcessIdentity | undefined {
    if (
      lease.pid === null
      || lease.pgid === null
      || lease.processIdentity === null
      || lease.processIdentity.length === 0
    ) {
      return undefined;
    }
    return {
      pid: lease.pid,
      pgid: lease.pgid,
      identity: lease.processIdentity,
      cwd: lease.cwd,
    };
  }

  private hostRemovalProcessIdentity(
    lease: HostRemovalLeaseRecord,
  ): WorktreeMergeProcessIdentity | undefined {
    if (
      lease.pid === null
      || lease.pgid === null
      || lease.processIdentity === null
      || lease.processIdentity.length === 0
    ) {
      return undefined;
    }
    return {
      pid: lease.pid,
      pgid: lease.pgid,
      identity: lease.processIdentity,
      cwd: lease.cwd,
    };
  }

  private blockRemovalResources(lease: HostRemovalLeaseRecord): void {
    this.blockedDirectories.add(lease.cwd);
    for (const path of lease.resourcePaths) this.blockedDirectories.add(path);
  }

  private releaseOrBlockHostRemoval(id: string, resourcePaths: readonly string[]): void {
    try {
      this.store.releaseHostRemovalLease(id);
    } catch {
      for (const path of resourcePaths) this.blockedDirectories.add(path);
      try {
        this.store.markHostRemovalLeaseUnknown(id);
      } catch {
        // Resource blocks still fail closed if the durable row itself cannot be updated.
      }
    }
  }

  private isWorkspaceDirectoryBlocked(cwd: string): boolean {
    for (const blockedCwd of this.blockedDirectories) {
      if (resourcePathsRelated(cwd, blockedCwd)) return true;
    }
    return false;
  }

  private async isRepositoryBlocked(root: string): Promise<boolean> {
    if (this.isWorkspaceDirectoryBlocked(root)) return true;
    for (const blockedCwd of this.blockedDirectories) {
      if (blockedCwd === root) return true;
      try {
        if (await this.repositoryRoot(blockedCwd) === root) return true;
      } catch {
        // Unknown blocked roots must not be treated as safe for another writer.
        return true;
      }
    }
    return false;
  }

  /** Returns a worktree's own top-level checkout, not the shared Git common directory. */
  private async repositoryRoot(cwd: string): Promise<string | null> {
    const child = Bun.spawn(["git", "-C", cwd, "rev-parse", "--show-toplevel"], {
      stdin: "ignore",
      stdout: "pipe",
      stderr: "pipe",
    });
    const [stdout, stderr, exitCode] = await Promise.all([
      new Response(child.stdout).text(),
      new Response(child.stderr).text(),
      child.exited,
    ]);
    if (exitCode !== 0) {
      if (/not a git repository/i.test(stderr)) return null;
      throw new Error("Git worktree root lookup failed.");
    }
    const root = stdout.trim();
    if (root.length === 0) throw new Error("Git worktree root lookup returned no path.");
    return canonicalExistingDirectory(root);
  }
  private async shouldRunTaskBeforeRepositoryProbe(
    taskId: string,
    target: ChatTarget,
    binding: BindingRecord | null,
    cwd: string,
  ): Promise<boolean> {
    const task = this.store.getTask(taskId);
    if (task === null) return false;
    const goalTask = this.store.getGoalTask(taskId);
    if (task.text === "/commit" && this.store.isCommitTask(taskId)) return false;
    const goalResuming = goalTask !== null && this.store.isGoalTaskResuming(taskId);
    const resuming = this.store.isTaskResuming(taskId) || goalResuming;
    // A missing/mismatched goal binding or missing bound session file is rejected by runTask
    // before worker startup; let that path revoke continuation and classify the goal error.
    if (
      goalTask !== null
      && (
        task.sessionId !== goalTask.sessionId
        || binding === null
        || binding.sessionId !== goalTask.sessionId
      )
    ) {
      return true;
    }
    if (targetKey(taskTarget(task)) !== targetKey(target)) return true;
    if (resuming) {
      if (
        binding === null
        || binding.sessionFile === null
        || binding.sessionId !== task.sessionId
        || await existingSessionFile(binding.sessionFile) === null
      ) {
        return true;
      }
    } else if (
      goalTask !== null
      && binding !== null
      && binding.sessionFile !== null
      && await existingSessionFile(binding.sessionFile) === null
    ) {
      return true;
    }
    try {
      await canonicalExistingDirectory(cwd);
      return false;
    } catch {
      return true;
    }
  }

  private async threadWorkspaceIncomplete(
    target: ChatTarget,
    requireBinding = false,
  ): Promise<boolean> {
    if (target.channelType !== 5 || this.bot === undefined) return false;
    try {
      if (this.store.isThreadWorkspaceIntentBlocked(this.bot.uid, target)) return true;
      const workspace = this.store.getThreadWorkspace(this.bot.uid, target);
      if (workspace === null) return false;
      const missingLegacyWorktree =
        workspace.layout === "single"
        && workspace.legacyWorktreeName !== undefined
        && workspace.status === "ready"
        && !(await threadWorkspaceIsAvailable(workspace));
      if (missingLegacyWorktree) return false;
      const binding = this.store.getBinding(this.bot.uid, target);
      // Fresh inbound can create a binding inside a ready workspace; a persisted task cannot
      // trust its old cwd without the binding that established it.
      if (binding === null) {
        return requireBinding || !(await threadWorkspaceIsAvailable(workspace));
      }
      const related = new Set(this.store.workspaceDirectories(binding.cwd));
      const stillOwned =
        (workspace.entryPath !== null && related.has(workspace.entryPath))
        || workspace.projects.some((project) => project.path !== null && related.has(project.path));
      if (!stillOwned) return false;
      if (!(await threadWorkspaceIsAvailable(workspace))) return true;
      const bindingCwd = await canonicalExistingDirectory(binding.cwd).catch(() => null);
      return bindingCwd !== binding.cwd;
    } catch {
      return true;
    }
  }

  private async refuseIncompleteThreadWorkspace(target: ChatTarget): Promise<boolean> {
    if (!await this.threadWorkspaceIncomplete(target)) return false;
    await this.outbox.enqueueText(target, THREAD_WORKSPACE_UNAVAILABLE_TEXT, null);
    return true;
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
  private liveWorker(target: ChatTarget): WorkerSession | undefined {
    const key = targetKey(target);
    for (const runtime of this.runtimes.values()) {
      if (targetKey(runtime.target) === key && runtime.lease) return runtime.lease.worker;
    }
    return undefined;
  }

  private async fetchMemberSnapshot(
    target: ChatTarget,
    cancelSignal?: AbortSignal,
  ): Promise<AgentMemberSnapshot> {
    const controller = new AbortController();
    const signals = cancelSignal === undefined
      ? [controller.signal, this.shutdownController.signal]
      : [controller.signal, this.shutdownController.signal, cancelSignal];
    const signal = AbortSignal.any(signals);
    if (signal.aborted) throw signal.reason ?? new Error("成员 API 请求已取消");
    let timer: ReturnType<typeof setTimeout> | undefined;
    let removeAbortListener = (): void => {};
    const aborted = new Promise<never>((_, reject) => {
      const rejectAborted = (): void => {
        reject(signal.reason ?? new Error("成员 API 请求已取消"));
      };
      signal.addEventListener("abort", rejectAborted, { once: true });
      removeAbortListener = () => signal.removeEventListener("abort", rejectAborted);
    });
    timer = setTimeout(
      () => controller.abort(new Error("成员 API 查询超时")),
      MEMBER_LOOKUP_TIMEOUT_MS,
    );
    try {
      let request: Promise<unknown>;
      if (target.channelType === 2) {
        request = this.options.transport.getGroupMembers(target.channelId, signal);
      } else if (target.channelType === 5) {
        const groupId = threadParentGroupId(target.channelId);
        const shortId = threadShortId(target.channelId);
        if (groupId === null || shortId === null) throw new Error("线程成员查询身份无效");
        request = this.options.transport.getThreadMembers(groupId, shortId, signal);
      } else {
        throw new Error("私聊不读取成员");
      }
      const response = await Promise.race([request, aborted]);
      return memberSnapshotFor(target, normalizeAgentMembers(response));
    } finally {
      clearTimeout(timer);
      removeAbortListener();
    }
  }

  private canDeliverSteer(runtime: RuntimeTask): boolean {
    return this.runtimes.get(runtime.taskId) === runtime
      && runtime.promptInFlight
      && runtime.lease !== undefined
      && !runtime.cancelRequested
      && runtime.stopCause !== "question"
      && !runtime.suspended
      && !this.stopping;
  }

  private async deliverSteer(
    runtime: RuntimeTask,
    text: string,
    deliveryGuard?: () => boolean,
    beforeWorkerSteer?: () => boolean,
  ): Promise<boolean> {
    if (!this.canDeliverSteer(runtime) || !runtime.lease || (deliveryGuard && !deliveryGuard())) return false;
    if (beforeWorkerSteer && !beforeWorkerSteer()) return false;
    const delivery = { text };
    runtime.steerDeliveries.push(delivery);
    if (runtime.steerDeliveries.length > MAX_PENDING_STEERS) runtime.steerDeliveries.shift();
    try {
      await runtime.lease.worker.steer(text);
      runtime.rollProgressCard?.();
      return true;
    } catch {
      const index = runtime.steerDeliveries.indexOf(delivery);
      if (index >= 0) runtime.steerDeliveries.splice(index, 1);
      return false;
    }
  }
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

  private async deliverSteeringMessage(
    runtime: RuntimeTask,
    message: InboundMessage,
    text: string,
    claim: InboundClaim,
    deliveryGuard?: () => boolean,
    beforeWorkerSteer?: () => boolean,
  ): Promise<boolean> {
    if (!this.canDeliverSteer(runtime) || (deliveryGuard && !deliveryGuard())) {
      if (runtime.cancelRequested) {
        this.store.completeCommand(claim);
        return true;
      }
      this.store.releaseClaim(claim);
      return false;
    }

    let previousContext: AgentContext | undefined;
    let contextUpdated = false;
    let contextStaged = false;
    if (isGroupTarget(runtime.target)) {
      try {
        const guideInDeveloper = runtime.agentContext?.guideInDeveloper ?? false;
        const nextContext: AgentContext = isNativeRetryPrompt(text)
          ? { cliReady: this.cliIdentityReady, guideInDeveloper }
          : {
              cliReady: this.cliIdentityReady,
              guideInDeveloper,
              memberSnapshot: await this.fetchMemberSnapshot(
                runtime.target,
                runtime.cancelController.signal,
              ),
            };
        if (!this.canDeliverSteer(runtime) || (deliveryGuard && !deliveryGuard())) {
          if (runtime.cancelRequested) {
            this.store.completeCommand(claim);
            return true;
          }
          this.store.releaseClaim(claim);
          return false;
        }
        if (isNativeRetryPrompt(text)) {
          previousContext = await runtime.turnContext.updateAgentContext(nextContext);
          contextUpdated = true;
        } else {
          previousContext = await runtime.turnContext.stageAgentContext(message.messageId, text, nextContext);
          contextStaged = true;
        }
        runtime.agentContext = nextContext;
      } catch {
        if (deliveryGuard) {
          this.store.releaseClaim(claim);
          return false;
        }
        if (this.stopping) {
          this.store.releaseClaim(claim);
          return true;
        }
        if (runtime.cancelRequested) {
          this.store.completeCommand(claim);
          return true;
        }
        const noMembers: AgentContext = {
          cliReady: this.cliIdentityReady,
          guideInDeveloper: runtime.agentContext?.guideInDeveloper ?? false,
        };
        await runtime.turnContext.updateAgentContext(noMembers).catch(() => {});
        runtime.agentContext = noMembers;
        this.store.completeCommand(claim);
        await this.outbox.enqueueText(runtime.target, MEMBER_LOOKUP_FAILURE_STEER_NOTICE, null);
        return true;
      }
    }

    if (await this.deliverSteer(runtime, text, deliveryGuard, beforeWorkerSteer)) {
      this.store.completeCommand(claim);
      this.history.recordSteered(message, runtime.lease?.worker.sessionId ?? "");
      return true;
    }

    if (contextStaged || contextUpdated) {
      const restored: AgentContext = previousContext ?? {
        cliReady: this.cliIdentityReady,
        guideInDeveloper: runtime.agentContext?.guideInDeveloper ?? false,
      };
      if (contextStaged) {
        await runtime.turnContext.discardStagedAgentContext(message.messageId).catch(() => {});
      } else {
        await runtime.turnContext.updateAgentContext(restored).catch(() => {});
      }
      runtime.agentContext = restored;
    }
    if (this.stopping) {
      this.store.releaseClaim(claim);
      return true;
    }
    if (runtime.cancelRequested) {
      this.store.completeCommand(claim);
      return true;
    }
    this.store.releaseClaim(claim);
    return false;
  }

  /**
   * In `steer` mode a message that arrives while its chat runs an OMP prompt is injected into that turn
   * instead of starting a second task.
   * Member lookups and staged snapshots follow serialized input order; the extension exposes a
   * snapshot only after OMP includes its steering message in model context. A member-read failure
   * is consumed without changing the running prompt.
   */
  private async appendToRunningTask(
    message: InboundMessage,
    text: string,
    deliveryGuard?: () => boolean,
    beforeWorkerSteer?: () => boolean,
  ): Promise<boolean> {
    this.throwIfStopping();
    if (await this.refuseIncompleteThreadWorkspace(message)) return true;
    if (this.store.getChatMode(this.bot!.uid, message) !== "steer") return false;
    const runtime = this.runningRuntime(message);
    if (!runtime || runtime.stopCause === "question") return false;
    if (deliveryGuard && !deliveryGuard()) return false;
    if (await this.scheduler.hasPendingCommandReservation(message, runtime.cwd)) return false;
    const claim = this.commandClaim(message, "steer");
    if (!this.store.claimCommand(claim)) return true;
    this.threadArchives.cancel(message);
    const delivery = runtime.steerSerial.then(() =>
      this.deliverSteeringMessage(runtime, message, text, claim, deliveryGuard, beforeWorkerSteer));
    runtime.steerSerial = delivery.then(() => {}, () => {});
    return await delivery;
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
      this.threadArchives.cancel(message);
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

  private async handleCommand(
    message: InboundMessage,
    name: string,
    argument: string,
    action: BuiltinAction,
  ): Promise<void> {
    // 认领仍按键原始命令名（别名在 claim 之后只影响走哪个 handler），认领失败即丢弃。
    const claim = this.commandClaim(message, name);
    if (!this.store.claimCommand(claim)) return;
    this.threadArchives.cancel(message);
    try {
      const restartRequested = await this.builtinHandlers[action](message, argument);
      const commandKey = this.threadMergeCommandKey(message);
      if (
        this.threadMergeControllersByCommand.has(commandKey)
        || this.threadRemovalControllersByCommand.has(commandKey)
      ) {
        await this.outbox.flush(message);
      }
      this.store.completeCommand(claim);
      if (restartRequested === true) {
        await this.serviceRestart.request();
      }
    } catch (error) {
      if (this.store.getCommandClaimState(claim.botUid, message, message.messageId) === "pending") {
        this.store.failCommand(claim);
      }
      const detail = safeDisplayText(error instanceof Error ? error.message : String(error), 180);
      throw new Error(detail === undefined ? "命令未能持久完成。" : `命令未能持久完成：${detail}`);
    } finally {
      if (
        this.finishThreadMergeCommand(message)
        || this.finishThreadRemovalCommand(message)
      ) this.serviceRestart.maybeRunDeferred();
    }
  }

  /**
   * Aborts the running OMP turn without clearing the messages already queued in that run and
   * without stopping the worker, so a steered or queued message still gets its turn.
   */
  private async handleInterrupt(target: ChatTarget): Promise<void> {
    const mergeCancelled = this.abortThreadMerges(target, new Error("thread merge interrupted"));
    const removalCancelled = this.abortThreadRemovals(target, new Error("thread removal interrupted"));
    if (this.commitOperationsByTarget.has(targetKey(target))) {
      await this.outbox.enqueueText(
        target,
        mergeCancelled || removalCancelled
          ? "当前是 Git 提交任务；请使用 /commit stop。/interrupt 不会中断或伪确认提交。等待或执行中的子区宿主操作已请求中止。"
          : "当前是 Git 提交任务；请使用 /commit stop。/interrupt 不会中断或伪确认提交。",
        null,
      );
      return;
    }
    const runtime = this.runningRuntime(target);
    if (!runtime?.lease) {
      await this.outbox.enqueueText(
        target,
        mergeCancelled || removalCancelled
          ? "已请求中止正在等待或执行的子区宿主操作；进程结果若未确认，资源将保持封锁且不会自动重试。"
          : DEFAULT_INTERRUPT_IDLE_TEXT,
        null,
      );
      return;
    }
    const binding = this.store.getBinding(this.bot!.uid, target);
    if (binding) this.invalidateGoalResumeChecks(target, binding.sessionId);
    runtime.stopCause = "interrupt";
    runtime.workAfterStop = false;
    // 停止意图先落盘再回执：此后任何 abrupt 死亡都不能把用户已经中断的回合当成「可续接」重启
    // （回合随后又跑出真实工作时，由 task-events 撤回这条意图）。
    this.store.markTaskStopIntent(runtime.taskId, "interrupt");
    try {
      await runtime.lease.worker.abortTurn();
    } catch {
      runtime.stopCause = null;
      this.store.clearTaskStopIntent(runtime.taskId);
      await this.outbox.enqueueText(target, DEFAULT_INTERRUPT_REJECTED_TEXT, null);
      return;
    }
    await this.outbox.enqueueText(
      target,
      mergeCancelled || removalCancelled
        ? `${DEFAULT_INTERRUPTED_TEXT} 正在等待或执行的子区宿主操作也已请求中止。`
        : DEFAULT_INTERRUPTED_TEXT,
      null,
    );
  }

  /**
   * 提问超时：像取消 tool call 一样停住当前回合（abort 受 RPC 预算约束，随后照常收拢进程组），
   * 并把停止意图与「这次超时后再也不会被认领的旧问题」写进同一事务。
   *
   * `cancelQuestionIds` 是恢复快照里未认领且仍待答的精确行：进程若死在这次落库与回合收束之间，
   * 它们必须已经取消——否则重启后会被当成可答问题复活、唤醒一个早已结束的回合。当前这一轮超时的
   * 问题不在此列，保持可迟答（迟到答复照旧唤醒新回合）。
   */
  private stopTurnForQuestion(taskId: string, cancelQuestionIds: readonly string[] = []): void {
    const runtime = this.runtimes.get(taskId);
    if (!runtime?.lease || !runtime.promptInFlight) return;
    const binding = this.store.getBinding(this.bot!.uid, runtime.target);
    if (binding) this.invalidateGoalResumeChecks(runtime.target, binding.sessionId);
    runtime.stopCause = "question";
    runtime.workAfterStop = false;
    this.store.markTaskStopIntent(taskId, "question_timeout", cancelQuestionIds);
    const lease = runtime.lease;
    void lease.worker.abortTurn().catch(() => {}).finally(() => lease.close().catch(() => false));
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

  /** Dispatched tasks notify only at task level; the durable policy owns recipient provenance. */
  private async notifyThreadOutcome(taskId: string): Promise<void> {
    try {
      if (this.stopping) return;
      const botUid = this.bot?.uid;
      if (botUid === undefined) return;
      const task = this.store.getTask(taskId);
      if (
        !task
        || task.channelType !== 5
        || !task.messageId.startsWith("dispatch:")
        || task.relaySuppressed
      ) {
        return;
      }
      if (
        task.state !== "completed"
        && task.state !== "failed"
        && task.state !== "cancelled"
        && task.state !== "interrupted"
      ) {
        return;
      }
      const source: ChatTarget = { channelId: task.channelId, channelType: 5 };
      const policy = this.store.getThreadListen(botUid, source);
      if (policy === null || policy.listen !== "task" || policy.mergePending) return;
      const ownerId = this.store.getOwnerId(botUid)?.trim() ?? "";
      if (ownerId.length === 0) return;
      const recipient: ChatTarget = {
        channelId: policy.creatorChannelId,
        channelType: policy.creatorChannelType,
      };
      const text = threadOutcomeNotice(task, policy.threadName);
      const message: InboundMessage = {
        ...recipient,
        messageId: threadNotifyMessageId(task.id),
        senderId: ownerId,
        text,
        mentionedUserIds: [botUid],
        attachments: [],
        quoted: null,
        supported: true,
        messageSeq: 0,
        messageTimestamp: Date.now(),
      };
      if (!(await this.threadNoticeAuthorized(source, recipient, message, ownerId))) return;
      const deliveryGuard = (): boolean => {
        const currentTask = this.store.getTask(task.id);
        const currentPolicy = this.store.getThreadListen(botUid, source);
        return !this.stopping
          && this.bot?.uid === botUid
          && this.store.getOwnerId(botUid)?.trim() === ownerId
          && currentTask !== null
          && currentTask.messageId.startsWith("dispatch:")
          && !currentTask.relaySuppressed
          && currentTask.state === task.state
          && currentPolicy?.listen === "task"
          && !currentPolicy.mergePending
          && currentPolicy.creatorChannelType === recipient.channelType
          && currentPolicy.creatorChannelId === recipient.channelId
          && this.authorization.isPermittedChat(source)
          && this.authorization.isPermittedChat(recipient);
      };
      if (!deliveryGuard()) return;
      if (await this.appendToRunningTask(message, text, deliveryGuard)) return;
      await this.acceptTask(message, text, true, undefined, undefined, deliveryGuard);
    } catch (error) {
      console.error(
        `[thread-notify] 回传投递失败 task=${taskId}：${safeErrorMessage(error, "notify failed")}`,
      );
    }
  }

  private async ensureThreadListen(target: ChatTarget): Promise<ThreadListenRecord> {
    if (target.channelType !== 5) throw new Error("thread listen target must be a thread");
    const botUid = this.bot?.uid;
    if (botUid === undefined) throw new Error("Bot 身份未注册，无法读取子区监听策略");
    const existing = this.store.getThreadListen(botUid, target);
    if (existing !== null) return existing;
    const groupId = threadParentGroupId(target.channelId);
    const shortId = threadShortId(target.channelId);
    if (groupId === null || shortId === null) throw new Error("无效的子区目标");
    // Task acceptance has no remote metadata requirement; create-thread and management reads
    // persist the authoritative name when one is available.
    return this.store.ensureThreadListen({
      botUid,
      channelId: target.channelId,
      creatorChannelType: 2,
      creatorChannelId: groupId,
      threadName: "",
      listen: "merge",
    });
  }

  private async threadNoticeAuthorized(
    source: ChatTarget,
    recipient: ChatTarget,
    message: InboundMessage,
    ownerId: string,
  ): Promise<boolean> {
    return this.authorization.isPermittedChat(source)
      && this.authorization.isPermittedChat(recipient)
      && await this.authorization.isAllowed(message, ownerId);
  }

  private threadMergeEventId(botUid: string, target: ChatTarget, operationId: string): string {
    return `merge-${createHash("sha256")
      .update(JSON.stringify([botUid, target.channelType, target.channelId, operationId]))
      .digest("hex")}`;
  }

  /**
   * Persists and routes a confirmed merge once. Reuse `operationId` for retries of one operation:
   * host tools key it by task/tool-call identity, while a host-first command uses its message ID.
   */
  async publishThreadMergeSuccess(
    target: ChatTarget,
    summary: WorktreeMergeSummary,
    archive: "scheduled" | "not-scheduled",
    operationId: string,
  ): Promise<void> {
    const event = await this.persistThreadMergeSuccess(target, summary, archive, operationId);
    if (!this.store.isThreadMergeEventPending(event.id)) return;
    try {
      await this.deliverThreadMergeEvent(event);
    } catch (error) {
      console.error(
        `[thread-merge-notify] 回传投递失败 event=${event.id}：${safeErrorMessage(error, "notify failed")}`,
      );
    }
  }

  /**
   * Records a confirmed event without delivering it. Startup recovery stages known outcomes before
   * replaying pending events after restored tasks have already taken their queue order.
   */
  private async persistThreadMergeSuccess(
    target: ChatTarget,
    summary: WorktreeMergeSummary,
    archive: "scheduled" | "not-scheduled",
    operationId: string,
  ): Promise<ThreadMergeEventRecord> {
    const botUid = this.bot?.uid;
    if (botUid === undefined) throw new Error("Bot 身份未注册，无法记录合并事件");
    await this.ensureThreadListen(target);
    const commit = summary.commit?.trim() || null;
    const eventId = this.threadMergeEventId(botUid, target, operationId);
    const event: ThreadMergeEventRecord = {
      id: eventId,
      botUid,
      channelId: target.channelId,
      targetBranch: summary.target,
      commit,
      removed: summary.removed,
      archive,
    };
    this.store.saveThreadMergeEvent(event);
    return event;
  }

  /** Returns the durable success receipt for a retried host tool call, without repeating the merge. */
  async replayThreadMergeSuccess(target: ChatTarget, operationId: string): Promise<string | null> {
    const botUid = this.bot?.uid;
    if (botUid === undefined) throw new Error("Bot 身份未注册，无法读取合并事件");
    if (target.channelType !== 5) throw new Error("merge replay target must be a thread");
    const event = this.store.getThreadMergeEvent(
      this.threadMergeEventId(botUid, target, operationId),
    );
    if (event === null || event.botUid !== botUid || event.channelId !== target.channelId) return null;
    if (this.store.isThreadMergeEventPending(event.id)) {
      try {
        await this.deliverThreadMergeEvent(event);
      } catch (error) {
        console.error(
          `[thread-merge-notify] 回放投递失败 event=${event.id}：${safeErrorMessage(error, "notify failed")}`,
        );
      }
    }
    return JSON.stringify({
      merged: true,
      replayed: true,
      target: event.targetBranch,
      commit: event.commit,
      removed: event.removed,
      archive: event.archive,
    });
  }

  private async recoverThreadMergeEvents(botUid: string): Promise<void> {
    for (const event of this.store.listPendingThreadMergeEvents(botUid)) {
      if (this.stopping) return;
      try {
        await this.deliverThreadMergeEvent(event, true);
      } catch (error) {
        console.error(
          `[thread-merge-notify] 恢复投递失败 event=${event.id}：${safeErrorMessage(error, "notify failed")}`,
        );
      }
    }
  }

  private consumeThreadMergeEvent(event: ThreadMergeEventRecord, recipient: ChatTarget): void {
    const receipt: InboundMessage = {
      ...recipient,
      messageId: threadMergeSuppressedMessageId(event.id),
      senderId: event.botUid,
      text: "线程合并事件已按 listen=off 消费。",
      mentionedUserIds: [],
      attachments: [],
      quoted: null,
      supported: true,
      messageSeq: 0,
      messageTimestamp: Date.now(),
    };
    const claim = this.commandClaim(receipt, "thread-merge-suppressed");
    const state = this.store.getCommandClaimState(event.botUid, recipient, receipt.messageId);
    if (state === "completed") {
      this.store.markThreadMergeEventDelivered(event.id);
      return;
    }
    if (state === "pending" || this.store.claimCommand(claim)) {
      this.store.completeCommand(claim);
      this.store.markThreadMergeEventDelivered(event.id);
    }
  }

  private async deliverThreadMergeEvent(
    event: ThreadMergeEventRecord,
    recovering = false,
  ): Promise<void> {
    if (
      this.stopping
      || this.bot?.uid !== event.botUid
      || !this.store.isThreadMergeEventPending(event.id)
    ) return;
    const source: ChatTarget = { channelId: event.channelId, channelType: 5 };
    const policy = this.store.getThreadListen(event.botUid, source) ?? await this.ensureThreadListen(source);
    const recipient: ChatTarget = {
      channelId: policy.creatorChannelId,
      channelType: policy.creatorChannelType,
    };
    const messageId = threadMergeMessageId(event.id);
    const suppressionId = threadMergeSuppressedMessageId(event.id);
    const message: InboundMessage = {
      ...recipient,
      messageId,
      senderId: "",
      text: threadMergeNotice(event, policy.threadName),
      mentionedUserIds: [this.bot.uid],
      attachments: [],
      quoted: null,
      supported: true,
      messageSeq: 0,
      messageTimestamp: Date.now(),
    };
    const deliveryState = this.store.getCommandClaimState(event.botUid, recipient, messageId);
    if (deliveryState === "completed") {
      this.store.markThreadMergeEventDelivered(event.id);
      return;
    }
    if (deliveryState === "pending") {
      if (!recovering) return;
      this.store.releaseClaim(this.commandClaim(message, "steer"));
    }
    const suppressionState = this.store.getCommandClaimState(event.botUid, recipient, suppressionId);
    if (suppressionState === "completed" || suppressionState === "pending") {
      this.consumeThreadMergeEvent(event, recipient);
      return;
    }
    if (policy.listen === "off") {
      this.consumeThreadMergeEvent(event, recipient);
      return;
    }
    if (policy.mergePending) return;
    const ownerId = this.store.getOwnerId(event.botUid)?.trim() ?? "";
    if (ownerId.length === 0) return;
    message.senderId = ownerId;
    if (!(await this.threadNoticeAuthorized(source, recipient, message, ownerId))) return;
    const deliveryGuard = (): boolean => {
      const currentPolicy = this.store.getThreadListen(event.botUid, source);
      return !this.stopping
        && this.bot?.uid === event.botUid
        && this.store.getOwnerId(event.botUid)?.trim() === ownerId
        && currentPolicy !== null
        && currentPolicy.listen !== "off"
        && !currentPolicy.mergePending
        && currentPolicy.creatorChannelType === recipient.channelType
        && currentPolicy.creatorChannelId === recipient.channelId
        && this.authorization.isPermittedChat(source)
        && this.authorization.isPermittedChat(recipient);
    };
    if (!deliveryGuard()) {
      if (this.store.getThreadListen(event.botUid, source)?.listen === "off") {
        this.consumeThreadMergeEvent(event, recipient);
      }
      return;
    }
    let dispatchIntent = false;
    const beforeWorkerSteer = (): boolean => {
      if (!deliveryGuard()) return false;
      dispatchIntent = this.store.claimThreadMergeDispatchIntent(event.id);
      return dispatchIntent;
    };
    if (await this.appendToRunningTask(message, message.text, deliveryGuard, beforeWorkerSteer)) {
      if (dispatchIntent) this.store.markThreadMergeEventDelivered(event.id);
      return;
    }
    if (dispatchIntent || !this.store.isThreadMergeEventPending(event.id)) return;
    if (!deliveryGuard()) {
      if (this.store.getThreadListen(event.botUid, source)?.listen === "off") {
        this.consumeThreadMergeEvent(event, recipient);
      }
      return;
    }
    const accepted = await this.acceptTask(
      message,
      message.text,
      true,
      undefined,
      undefined,
      deliveryGuard,
    );
    if (accepted || deliveryGuard()) {
      this.store.markThreadMergeEventDelivered(event.id);
    } else if (this.store.getThreadListen(event.botUid, source)?.listen === "off") {
      this.consumeThreadMergeEvent(event, recipient);
    }
  }

  private activeGoalTask(target: ChatTarget, sessionId: string | null): GoalTaskRecord | null {
    for (const runtime of this.runtimes.values()) {
      if (!runtime.isGoalTask || targetKey(runtime.target) !== targetKey(target)) continue;
      const task = this.store.getTask(runtime.taskId);
      if (task?.state !== "running" && task?.state !== "queued") continue;
      const goalTask = this.store.getGoalTask(runtime.taskId);
      if (goalTask && (sessionId === null || goalTask.sessionId === sessionId)) return goalTask;
    }
    for (const task of this.store.listQueuedTasks()) {
      if (targetKey(taskTarget(task)) !== targetKey(target) || (sessionId !== null && task.sessionId !== sessionId)) continue;
      const goalTask = this.store.getGoalTask(task.id);
      if (goalTask) return goalTask;
    }
    return null;
  }
  private goalResumeCheckKey(target: ChatTarget, sessionId: string): string {
    return JSON.stringify([target.channelType, target.channelId, sessionId]);
  }

  private beginGoalResumeCheck(
    target: ChatTarget,
    sessionId: string,
  ): { isCurrent: () => boolean; release: () => void } {
    const key = this.goalResumeCheckKey(target, sessionId);
    const existing = this.goalResumeChecks.get(key);
    const state = existing ?? { generation: 0, active: 0 };
    if (!existing) this.goalResumeChecks.set(key, state);
    state.active += 1;
    const generation = state.generation;
    let released = false;
    return {
      isCurrent: () => !released && state.generation === generation,
      release: () => {
        if (released) return;
        released = true;
        state!.active -= 1;
        if (state!.active === 0 && this.goalResumeChecks.get(key) === state) {
          this.goalResumeChecks.delete(key);
        }
      },
    };
  }

  private invalidateGoalResumeChecks(target: ChatTarget, sessionId: string): void {
    const state = this.goalResumeChecks.get(this.goalResumeCheckKey(target, sessionId));
    if (state) state.generation += 1;
  }

  private async cleanupCancelledGoalTasks(target: ChatTarget, taskIds: readonly string[]): Promise<void> {
    for (const taskId of taskIds) {
      this.scheduler.takeWaitingReceipt(taskId);
      this.typingLeases.release(target, `task:${taskId}`);
      this.questions.cancelForTask(taskId, "cancelled");
      await this.notifyThreadOutcome(taskId);
    }
  }

  private async withGoalWorker<T>(
    target: ChatTarget,
    operation: (worker: WorkerSession, binding: BindingRecord) => Promise<T>,
  ): Promise<T> {
    const botUid = this.bot?.uid;
    if (!botUid) throw new Error("Bot identity unavailable");
    const binding = this.store.getBinding(botUid, target);
    if (!binding) throw new Error("当前聊天没有已绑定的 OMP 会话。");
    const live = this.liveWorker(target);
    if (live && !this.runningRuntime(target)) throw new GoalDirectoryBusyError();
    if (live) {
      if (live.sessionId !== binding.sessionId) {
        throw new WorkerGoalError("identity-mismatch", "当前绑定会话与运行中的会话不一致");
      }
      return operation(live, binding);
    }

    const sessionLease = await this.scheduler.tryAcquireCommandSession(target, [binding.cwd]);
    if (!sessionLease) throw new GoalDirectoryBusyError();
    let tracked: TrackedWorker | undefined;
    try {
      await sessionLease.ready;
      const current = this.store.getBinding(botUid, target);
      if (
        !current
        || current.sessionId !== binding.sessionId
        || current.sessionFile !== binding.sessionFile
      ) {
        throw new WorkerGoalError("identity-mismatch", "控制期间会话绑定已发生变化");
      }
      if (!binding.sessionFile) throw new Error("当前绑定尚无可控制的 OMP 会话文件。");
      const sessionFile = await existingSessionFile(binding.sessionFile);
      if (!sessionFile) throw new WorkerGoalError("identity-mismatch", "绑定的原生目标会话文件不存在");
      const sessionDir = await ensureProjectSessionDir(sessionsRoot(this.options.config), binding.cwd);
      const model = this.store.resolveChatModel(botUid, target).model;
      tracked = await openTrackedWorker({
        workerFactory: this.options.workerFactory,
        store: this.store,
        worker: { cwd: binding.cwd, sessionDir, sessionFile, ...(model === null ? {} : { model }) },
        ledger: {
          id: randomUUID(),
          botUid,
          sessionId: binding.sessionId,
          sessionFile,
        },
        blockDirectory: cwd => this.blockedDirectories.add(cwd),
      });
      const state = await tracked.worker.refreshState();
      const verified = await verifyMovedSession(binding, state, binding.cwd);
      if (verified.sessionId !== binding.sessionId || verified.sessionFile !== sessionFile) {
        throw new WorkerGoalError("identity-mismatch", "打开的 OMP 会话与绑定身份不一致");
      }
      return await operation(tracked.worker, binding);
    } finally {
      try {
        if (tracked && !(await tracked.close())) {
          throw new Error("原生目标控制 worker 未能确认退出");
        }
      } finally {
        sessionLease.complete();
      }
    }
  }

  private async handleGoal(message: InboundMessage, argument: string): Promise<void> {
    const parsed = parseGoalCommand(argument);
    if ("error" in parsed) {
      await this.outbox.enqueueText(message, parsed.error, null);
      return;
    }
    const command = parsed.command;
    switch (command.op) {
      case "show":
      case "budget-show":
        await this.handleGoalShow(message);
        return;
      case "set":
        await this.handleGoalSet(message, command.objective, command.tokenBudget);
        return;
      case "resume":
        await this.handleGoalResume(message);
        return;
      case "pause":
      case "drop":
        await this.handleGoalControl(message, command.op);
        return;
      case "budget-set":
        await this.outbox.enqueueText(
          message,
          `原生目标预算创建后不能修改；请在创建时使用 /goal set --budget N <目标>。${GOAL_USAGE}`,
          null,
        );
        return;
    }
  }

  private async handleGoalShow(target: ChatTarget): Promise<void> {
    const botUid = this.bot?.uid;
    if (!botUid) return;
    const binding = this.store.getBinding(botUid, target);
    if (!binding) {
      await this.outbox.enqueueText(target, formatGoalSession(null), null);
      return;
    }
    const stored = this.store.getGoalSession(botUid, target, binding.sessionId);
    if (!binding.sessionFile && !this.liveWorker(target)) {
      await this.outbox.enqueueText(target, formatGoalSession(stored), null);
      return;
    }
    try {
      const pending = this.activeGoalTask(target, binding.sessionId);
      const result = await this.withGoalWorker(target, worker => worker.goal("get"));
      if (result.goal) {
        if (result.goal.status === "complete" || result.goal.status === "dropped") {
          if (pending) this.store.saveGoalSessionProgress(botUid, target, binding.sessionId, result.goal);
          else this.store.clearGoalSession(botUid, target, binding.sessionId);
          await this.outbox.enqueueText(target, `原生目标 ${result.goal.id} 已结算，状态 ${result.goal.status}。`, null);
          return;
        }
        this.store.saveGoalSessionProgress(botUid, target, binding.sessionId, result.goal);
      } else if (pending) {
        await this.outbox.enqueueText(
          target,
          `原生 RPC 当前未返回目标；目标请求仍在执行或排队，${formatGoalSession(stored)}。`,
          null,
        );
        return;
      } else {
        this.store.clearGoalSession(botUid, target, binding.sessionId);
      }
      await this.outbox.enqueueText(
        target,
        result.goal ? formatGoalSession(this.store.getGoalSession(botUid, target, binding.sessionId)) : formatGoalSession(null),
        null,
      );
    } catch (error) {
      const text = error instanceof GoalDirectoryBusyError
        ? "原生目标控制目录正忙；任务结束后再试 /goal show。"
        : goalErrorText(error) ?? (stored ? formatGoalSession(stored) : "无法安全读取当前会话的原生目标状态。");
      await this.outbox.enqueueText(target, text, null);
    }
  }

  private async handleGoalSet(message: InboundMessage, objective: string, tokenBudget?: number): Promise<void> {
    const botUid = this.bot?.uid;
    if (!botUid) return;
    const binding = this.store.getBinding(botUid, message);
    if (binding) {
      if (this.store.getGoalSession(botUid, message, binding.sessionId)) {
        await this.outbox.enqueueText(
          message,
          "当前绑定会话已有原生目标或目标请求，不能替换；请使用 /goal show、/goal pause 或 /goal drop。",
          null,
        );
        return;
      }
      if (this.activeGoalTask(message, binding.sessionId)) {
        await this.outbox.enqueueText(message, "当前会话已有目标任务在排队或运行；未重复启动。", null);
        return;
      }
    }
    const request: GoalTaskRequest = {
      op: "create",
      objective,
      ...(tokenBudget === undefined ? {} : { tokenBudget }),
    };
    this.store.releaseClaim(this.commandClaim(message, "goal"));
    const accepted = await this.acceptTask(message, `目标：${objective}`, false, request);
    if (!accepted) {
      await this.outbox.enqueueText(message, "目标任务未能持久接收；没有发送普通模型 prompt。", null);
    }
  }

  private async handleGoalResume(message: InboundMessage): Promise<void> {
    const botUid = this.bot?.uid;
    if (!botUid) return;
    const binding = this.store.getBinding(botUid, message);
    if (!binding) {
      await this.outbox.enqueueText(message, "当前会话没有可恢复的原生目标。", null);
      return;
    }
    const reportExistingTask = async (): Promise<boolean> => {
      const active = this.activeGoalTask(message, binding.sessionId);
      if (!active) return false;
      const task = this.store.getTask(active.taskId);
      await this.outbox.enqueueText(
        message,
        task?.state === "queued"
          ? "原生目标恢复请求已排队；没有重复排队。"
          : "原生目标任务正在运行；没有重复启动。",
        null,
      );
      return true;
    };
    if (await reportExistingTask()) return;

    const resumeCheck = this.beginGoalResumeCheck(message, binding.sessionId);
    const bindingIsExact = (): boolean => {
      const current = this.store.getBinding(botUid, message);
      return current?.sessionId === binding.sessionId && current.sessionFile === binding.sessionFile;
    };
    const reportStopped = async (): Promise<void> => {
      await this.outbox.enqueueText(message, "目标恢复期间收到新的停止意图；未执行恢复。", null);
    };
    try {
      const enqueueResume = async (goalId: string): Promise<void> => {
        if (await reportExistingTask()) return;
        if (!resumeCheck.isCurrent()) {
          await reportStopped();
          return;
        }
        if (!bindingIsExact()) {
          await this.outbox.enqueueText(message, "绑定会话已变化；未在新会话恢复原生目标。", null);
          return;
        }
        if (!binding.sessionFile) {
          await this.outbox.enqueueText(message, "无法恢复原生目标：精确绑定会话文件不存在。", null);
          return;
        }
        const currentGoal = this.store.getGoalSession(botUid, message, binding.sessionId);
        if (!currentGoal || currentGoal.goalId !== goalId || currentGoal.status !== "paused") {
          await this.outbox.enqueueText(message, "原生目标状态已变化；未排队恢复，请重新检查目标状态。", null);
          return;
        }
        this.store.releaseClaim(this.commandClaim(message, "goal"));
        const accepted = await this.acceptTask(
          message,
          `恢复原生目标 ${goalId}`,
          false,
          { op: "resume" },
          binding.sessionId,
          resumeCheck.isCurrent,
        );
        if (!accepted) {
          await this.outbox.enqueueText(
            message,
            resumeCheck.isCurrent()
              ? "原生目标恢复请求未能在原绑定会话持久接收；没有发送普通模型 prompt。"
              : "目标恢复期间收到新的停止意图；未执行恢复。",
            null,
          );
        }
      };

      const stored = this.store.getGoalSession(botUid, message, binding.sessionId);
      if (!binding.sessionFile && !this.liveWorker(message)) {
        await this.outbox.enqueueText(
          message,
          stored?.goalId
            ? "无法安全读取原生目标状态：精确绑定会话文件不存在。"
            : "当前会话没有已接受且可恢复的原生目标。",
          null,
        );
        return;
      }

      let nativeGoal: WorkerGoal | null;
      try {
        nativeGoal = await this.withGoalWorker(message, async worker => {
          const result = await worker.goal("get");
          const current = this.store.getGoalSession(botUid, message, binding.sessionId);
          if (!result.goal) {
            this.store.clearGoalSession(botUid, message, binding.sessionId);
            return null;
          }
          if (current?.goalId && current.goalId !== result.goal.id) {
            throw new WorkerGoalError("identity-mismatch", "原生目标 ID 与精确绑定会话记录不一致");
          }
          if (result.goal.status === "complete" || result.goal.status === "dropped") {
            this.store.clearGoalSession(botUid, message, binding.sessionId);
          } else {
            this.store.saveGoalSessionProgress(botUid, message, binding.sessionId, result.goal);
          }
          return result.goal;
        });
      } catch (error) {
        if (!resumeCheck.isCurrent()) {
          await reportStopped();
          return;
        }
        if (error instanceof GoalDirectoryBusyError) {
          const cached = this.store.getGoalSession(botUid, message, binding.sessionId);
          if (cached?.status === "paused" && cached.goalId && binding.sessionFile) {
            await enqueueResume(cached.goalId);
            return;
          }
        }
        const text = error instanceof GoalDirectoryBusyError
          ? "原生目标恢复目录正忙；无法安全核实目标状态，未启动恢复。"
          : goalErrorText(error) ?? "读取精确绑定会话的原生目标状态失败；未启动恢复。";
        await this.outbox.enqueueText(message, text, null);
        return;
      }
      if (!resumeCheck.isCurrent()) {
        await reportStopped();
        return;
      }
      if (!bindingIsExact()) {
        await this.outbox.enqueueText(message, "绑定会话已变化；未在新会话恢复原生目标。", null);
        return;
      }
      if (!nativeGoal) {
        await this.outbox.enqueueText(message, formatGoalSession(null), null);
        return;
      }
      if (nativeGoal.status === "complete" || nativeGoal.status === "dropped") {
        await this.outbox.enqueueText(
          message,
          `原生目标 ${nativeGoal.id} 已结算，状态 ${nativeGoal.status}。`,
          null,
        );
        return;
      }
      if (nativeGoal.status === "budget-limited") {
        await this.outbox.enqueueText(message, "原生目标已达到预算限制；预算不能事后修改，也不会自动恢复。", null);
        return;
      }
      if (nativeGoal.status !== "paused") {
        await this.outbox.enqueueText(
          message,
          nativeGoal.status === "active"
            ? "原生目标仍处于 active；不会重复恢复，可使用 /goal pause。"
            : `原生目标状态为 ${nativeGoal.status}，不能恢复。`,
          null,
        );
        return;
      }
      await enqueueResume(nativeGoal.id);
    } finally {
      resumeCheck.release();
    }
  }

  private async handleGoalControl(target: ChatTarget, operation: "pause" | "drop"): Promise<void> {
    const botUid = this.bot?.uid;
    if (!botUid) return;
    const binding = this.store.getBinding(botUid, target);
    if (!binding) {
      const cancelled = this.store.stopGoalContinuation(botUid, target, null, operation);
      await this.cleanupCancelledGoalTasks(target, cancelled);
      await this.outbox.enqueueText(target, "当前聊天没有已绑定的原生目标会话。", null);
      return;
    }
    this.invalidateGoalResumeChecks(target, binding.sessionId);
    try {
      const cancelled = this.store.stopGoalContinuation(botUid, target, binding.sessionId, operation);
      await this.cleanupCancelledGoalTasks(target, cancelled);
      const result = await this.withGoalWorker(target, async worker => {
        const pending = this.activeGoalTask(target, binding.sessionId) !== null;
        const prior = this.store.getGoalSession(botUid, target, binding.sessionId);
        const before = await worker.goal("get");
        if (before.goal) {
          if (prior?.goalId && prior.goalId !== before.goal.id) {
            throw new WorkerGoalError("identity-mismatch", "控制会话的原生目标 ID 与持久记录不一致");
          }
          this.store.saveGoalSessionProgress(botUid, target, binding.sessionId, before.goal);
        } else if (!pending) {
          this.store.clearGoalSession(botUid, target, binding.sessionId);
        }
        const outcome = await worker.goal(operation);
        if (outcome.goal) {
          if (before.goal && outcome.goal.id !== before.goal.id) {
            throw new WorkerGoalError("identity-mismatch", "原生目标控制返回了不同的 goal ID");
          }
          if (operation === "pause" && outcome.goal.status === "active") {
            throw new WorkerGoalError("native-error", "原生目标仍处于 active，pause 未生效；自动续跑授权已撤销。");
          }
          if (operation === "drop" && outcome.goal.status !== "dropped" && outcome.goal.status !== "complete") {
            throw new WorkerGoalError("native-error", "原生目标 drop 未确认；自动续跑授权已撤销。");
          }
          if ((outcome.goal.status === "complete" || outcome.goal.status === "dropped") && !pending) {
            this.store.clearGoalSession(botUid, target, binding.sessionId);
          } else {
            this.store.saveGoalSessionProgress(botUid, target, binding.sessionId, outcome.goal);
          }
        } else if (!pending) {
          this.store.clearGoalSession(botUid, target, binding.sessionId);
        }
        return { before, outcome, pending };
      });
      if (!result.outcome.goal) {
        const text = result.pending
          ? "已撤销目标续跑授权；原生 RPC 尚无目标，目标任务仍在收尾。"
          : "已撤销目标续跑授权；原生会话当前没有目标。";
        await this.outbox.enqueueText(target, text, null);
      } else if (result.outcome.goal.status === "complete" || result.outcome.goal.status === "dropped") {
        await this.outbox.enqueueText(
          target,
          `原生目标 ${result.outcome.goal.id} 已结算，状态 ${result.outcome.goal.status}。`,
          null,
        );
      } else {
        const session = this.store.getGoalSession(botUid, target, binding.sessionId);
        const message = operation === "pause" && result.outcome.goal.status === "paused"
          ? `原生目标已暂停；${formatGoalSession(session)}`
          : `原生目标当前状态为 ${result.outcome.goal.status}；${formatGoalSession(session)}`;
        await this.outbox.enqueueText(target, message, null);
      }
    } catch (error) {
      const text = error instanceof GoalDirectoryBusyError
        ? "原生目标控制目录正忙；当前操作未能打开精确绑定会话。"
        : goalErrorText(error) ?? `原生目标 ${operation === "pause" ? "暂停" : "删除"}失败；自动续跑授权已撤销。`;
      await this.outbox.enqueueText(target, text, null);
    }
  }


  private async resolveCommitWorkspace(
    target: ChatTarget,
    binding: BindingRecord,
  ): Promise<{ root: string } | { error: string }> {
    const cwd = await canonicalExistingDirectory(binding.cwd).catch(() => null);
    if (cwd === null) return { error: "绑定目录不存在或不可访问，未创建提交任务。" };
    if (await this.threadWorkspaceIncomplete(target, true)) {
      return { error: THREAD_WORKSPACE_UNAVAILABLE_TEXT };
    }

    const botUid = this.bot?.uid;
    const relatedDirectories = new Set([
      ...this.store.workspaceDirectories(binding.cwd),
      ...this.store.workspaceDirectories(cwd),
    ]);
    const relatedWorkspaces = botUid === undefined
      ? []
      : this.store.listThreadWorkspaces(botUid).filter((workspace) =>
        (workspace.entryPath !== null && relatedDirectories.has(workspace.entryPath))
        || workspace.projects.some((project) =>
          project.path !== null && relatedDirectories.has(project.path),
        ),
      );
    if (relatedWorkspaces.length > 1) {
      return { error: "当前项目关联多个持久化工作区，无法安全确定提交范围。" };
    }
    const workspace = relatedWorkspaces[0];
    if (workspace?.layout === "shared") {
      return { error: "共享工作区包含多个项目，不支持 /commit。" };
    }
    if (workspace && workspace.projects.length !== 1) {
      return { error: "当前工作区包含多个项目或 worktree，无法安全确定提交范围。" };
    }
    if (workspace && workspace.status !== "ready") {
      return { error: THREAD_WORKSPACE_UNAVAILABLE_TEXT };
    }
    const project = workspace?.projects[0];
    if (workspace && (!project?.created || project.path === null)) {
      return { error: "当前工作区项目记录不完整，未创建提交任务。" };
    }

    let root: string | null;
    try {
      root = await this.repositoryRoot(cwd);
    } catch {
      return { error: "Git 工作树状态无法确认，未创建提交任务。" };
    }
    if (root === null) return { error: "当前绑定目录不属于可用的 Git 工作树，未创建提交任务。" };
    if (project) {
      const projectPath = await canonicalExistingDirectory(project.path!).catch(() => null);
      let projectRoot: string | null;
      try {
        projectRoot = projectPath === null ? null : await this.repositoryRoot(projectPath);
      } catch {
        return { error: "持久化项目的 Git 工作树状态无法确认，未创建提交任务。" };
      }
      if (projectRoot !== root) {
        return { error: "绑定目录与持久化项目记录不一致，未创建提交任务。" };
      }
    }
    return { root };
  }

  private newCommitOperation(
    taskId: string,
    target: ChatTarget,
    root: string,
    binding: BindingRecord,
    reservation: CommitLease,
  ): CommitOperation {
    const done = Promise.withResolvers<void>();
    const workerReady = Promise.withResolvers<void>();
    const operation: CommitOperation = {
      taskId,
      target,
      root,
      binding,
      reservation,
      done: done.promise,
      finish: done.resolve,
      workerReady: workerReady.promise,
      markWorkerReady: workerReady.resolve,
      stopRequested: false,
      shutdownRequested: false,
    };
    this.commitOperationsByTask.set(taskId, operation);
    this.commitOperationsByTarget.set(targetKey(target), operation);
    return operation;
  }

  private forgetCommitOperation(operation: CommitOperation): void {
    operation.reservation.complete();
    operation.markWorkerReady();
    if (this.commitOperationsByTask.get(operation.taskId) === operation) {
      this.commitOperationsByTask.delete(operation.taskId);
    }
    if (this.commitOperationsByTarget.get(targetKey(operation.target)) === operation) {
      this.commitOperationsByTarget.delete(targetKey(operation.target));
    }
    operation.finish();
  }

  private abandonCommitOperation(operation: CommitOperation): void {
    this.typingLeases.release(operation.target, `task:${operation.taskId}`);
    this.forgetCommitOperation(operation);
    this.serviceRestart.maybeRunDeferred();
  }

  private async handleCommit(message: InboundMessage, argument: string): Promise<void> {
    if (argument === "stop") {
      await this.handleCommitStop(message);
      return;
    }
    if (argument.length > 0) {
      await this.outbox.enqueueText(message, COMMIT_USAGE, null);
      return;
    }
    const botUid = this.bot?.uid;
    if (botUid === undefined) return;
    const initialBinding = this.store.getBinding(botUid, message);
    const binding = initialBinding;
    this.throwIfStopping();
    if (binding === null) {
      await this.outbox.enqueueText(
        message,
        "当前聊天没有绑定的项目，请先使用 /new [目录] 创建会话。",
        null,
      );
      return;
    }
    const commandReservation = this.scheduler.reserveDestructiveCommandSession(message, [binding.cwd]);
    const workspace = await this.resolveCommitWorkspace(message, binding).catch((error: unknown) => {
      commandReservation.release();
      throw error;
    });
    if ("error" in workspace) {
      commandReservation.release();
      await this.outbox.enqueueText(message, workspace.error, null);
      return;
    }

    const taskId = randomUUID();
    let reservation: CommitLease | undefined;
    try {
      reservation = await this.scheduler.tryAcquireCommit(
        message,
        binding.cwd,
        workspace.root,
        taskId,
        commandReservation.order,
      );
    } finally {
      commandReservation.release();
    }
    if (!reservation) {
      await this.outbox.enqueueText(message, COMMIT_BUSY_TEXT, null);
      return;
    }
    const currentBinding = this.store.getBinding(botUid, message);
    if (
      currentBinding === null
      || currentBinding.cwd !== binding.cwd
      || currentBinding.sessionId !== binding.sessionId
      || currentBinding.sessionFile !== binding.sessionFile
      || currentBinding.cwdPinned !== binding.cwdPinned
    ) {
      reservation.complete();
      await this.outbox.enqueueText(message, "提交绑定已变化，请重新确认当前项目后重试。", null);
      return;
    }
    const operation = this.newCommitOperation(taskId, message, workspace.root, binding, reservation);
    const task: TaskRecord = {
      id: taskId,
      botUid,
      channelId: message.channelId,
      channelType: message.channelType,
      messageId: message.messageId,
      senderId: message.senderId,
      text: "/commit",
      sessionId: binding.sessionId,
      cwd: binding.cwd,
      state: "queued",
      resultText: null,
      errorText: null,
      messageSeq: message.messageSeq,
      historyEligible: false,
      relaySuppressed: false,
    };
    let accepted = false;
    try {
      accepted = this.store.acceptCommitTask(this.commandClaim(message, "commit"), task);
    } catch {
      this.abandonCommitOperation(operation);
      throw new Error("本地状态盘写入失败，提交任务未接收。");
    }
    if (!accepted) {
      this.abandonCommitOperation(operation);
      return;
    }
    this.typingLeases.acquire(message, `task:${taskId}`);
    this.scheduler.scheduleCommitTask(
      taskId,
      reservation,
      (id) => this.runCommitTask(id, undefined, operation),
      () => this.abandonCommitOperation(operation),
    );
    this.outbox.enqueueText(message, "开始提交当前项目。", taskId);
  }

  private stopCommitOperation(operation: CommitOperation): Promise<boolean> {
    operation.stopRequested = true;
    operation.stopPromise ??= (async () => {
      await operation.workerReady;
      try {
        const stopped = await this.stopCommitProcess(operation);
        operation.stopConfirmed = stopped;
        return stopped;
      } catch {
        operation.stopConfirmed = false;
        this.blockedDirectories.add(operation.root);
        return false;
      }
    })();
    return operation.stopPromise;
  }

  /**
   * Feeds the same evidence the text receipt uses into the card, then closes it. Nothing here
   * awaits card I/O: the task state and text receipt must never depend on a slow client.
   */
  private settleCommitProgress(
    progress: TaskProgress | undefined,
    state: "completed" | "failed" | "cancelled",
    result?: string,
    content?: CommitContent,
  ): void {
    if (!progress) return;
    if (result !== undefined) {
      progress.apply({
        type: "commit_result",
        atMs: Date.now(),
        summary: result,
        ...(content === undefined ? {} : { content }),
      });
    }
    progress.finish(state);
  }

  private commitResultNotice(result: CommitResult, stopRequested: boolean): string {
    const heads = `HEAD=${this.shortHead(result.headBefore)}→${this.shortHead(result.headAfter)}`;
    if (result.exitCode === 0) {
      return result.headBefore === result.headAfter
        ? `提交已完成，没有新提交。${heads}。`
        : `提交已完成。${heads}。`;
    }
    if (stopRequested) {
      return result.headBefore !== result.headAfter
        ? `提交已停止，但 HEAD 已变化；已产生的提交保留且不会回滚。${heads}。`
        : `已停止当前提交；${heads}；不会回滚已发生的副作用。`;
    }
    return result.headBefore !== result.headAfter
      ? `提交失败，退出码 ${result.exitCode}；HEAD 已变化，已产生的提交保留且不会回滚。${heads}。`
      : `提交失败，退出码 ${result.exitCode}。${heads}。`;
  }

  private async handleCommitStop(target: ChatTarget): Promise<void> {
    const botUid = this.bot?.uid;
    if (botUid === undefined) return;
    const operation = this.commitOperationsByTarget.get(targetKey(target));
    const activeTask = operation
      ? this.store.getTask(operation.taskId)
      : this.store.getActiveTask(botUid, target);
    if (
      activeTask !== null
      && activeTask.state === "queued"
      && this.store.isCommitTask(activeTask.id)
    ) {
      this.scheduler.takeWaitingReceipt(activeTask.id);
      this.store.markCancelled(activeTask.id, "commit cancelled before start");
      this.questions.cancelForTask(activeTask.id, "cancelled");
      if (operation) this.abandonCommitOperation(operation);
      await this.outbox.enqueueText(target, "已停止尚未开始的提交；没有 Git 副作用。", null);
      return;
    }
    if (!operation) {
      await this.outbox.enqueueText(target, "当前没有正在进行的提交。", null);
      return;
    }
    const completedResult = operation.result;
    if (completedResult !== undefined) {
      await operation.done;
      await this.outbox.enqueueText(
        target,
        this.commitResultNotice(
          completedResult,
          operation.stopRequested && operation.stopConfirmed === true,
        ),
        null,
      );
      return;
    }

    const stopped = await this.stopCommitOperation(operation);
    if (!stopped) {
      await this.outbox.enqueueText(target, "无法确认提交进程已停止；不会重试或回滚，工作树仍受保护。", null);
      return;
    }
    await operation.done;
    const result = operation.result;
    await this.outbox.enqueueText(
      target,
      result === undefined
        ? "已停止当前提交；不会回滚已发生的副作用。"
        : this.commitResultNotice(
          result,
          operation.stopRequested && operation.stopConfirmed === true,
        ),
      null,
    );
  }

  private shortHead(head: string | null): string {
    return head === null ? "未知" : head.slice(0, 8);
  }
  /**
   * `/queue` is the explicit way to append behind the running task regardless of the chat's mode,
   * so it always takes the durable task path minus the command prefix.
   */
  private async handleQueue(message: InboundMessage, argument: string): Promise<void> {
    if (await this.refuseIncompleteThreadWorkspace(message)) return;
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
    const requested = argument.trim();
    if (requested.length === 0) {
      const ticket = this.configMenus.begin(target, "mode");
      try {
        const mode = this.store.getChatMode(this.bot!.uid, target);
        await this.configMenus.open(target, {
          kind: "mode",
          current: mode,
          choices: MODE_CARD_CHOICES,
          plain: `当前追加模式：${mode}。${MODE_USAGE}`,
        }, ticket);
      } finally {
        ticket.release();
      }
      return;
    }
    // 文本命令与卡片点击共用同一段应用逻辑；先让旧的模式卡片失效，再按到达顺序落地。
    this.configMenus.invalidate(target, "mode");
    const receipt = await this.configMenus.serialize(
      "mode",
      target,
      () => this.applyModeChoice(target, requested),
    );
    await this.outbox.enqueueText(target, receipt, null);
  }

  /**
   * Apply one append-mode request and return the receipt verbatim. The text command and a card
   * click share this: `default` stays a synonym of `steer`, anything else is the usage line, and
   * only a valid value reaches the store.
   */
  private applyModeChoice(target: ChatTarget, value: string): string {
    const requested = value.trim().toLowerCase();
    const mode: ChatAppendMode | null =
      requested === "steer" || requested === "default"
        ? "steer"
        : requested === "queue"
          ? "queue"
          : null;
    if (!mode) return MODE_USAGE;
    this.store.setChatMode(this.bot!.uid, target, mode);
    return mode === "steer"
      ? "追加模式已设为 steer：运行中的新消息会注入当前回合。"
      : "追加模式已设为 queue：运行中的新消息会排到当前回合之后。";
  }

  private async handleCancel(target: ChatTarget): Promise<void> {
    const binding = this.store.getBinding(this.bot!.uid, target);
    if (binding) this.invalidateGoalResumeChecks(target, binding.sessionId);
    const mergeCancelled = this.abortThreadMerges(target, new Error("thread merge cancelled"));
    const removalCancelled = this.abortThreadRemovals(target, new Error("thread removal cancelled"));
    const queued = this.store.cancelQueued(this.bot!.uid, target);
    const queuedCommit = this.commitOperationsByTarget.get(targetKey(target));
    if (queuedCommit && queued.includes(queuedCommit.taskId)) {
      this.abandonCommitOperation(queuedCommit);
    }
    for (const taskId of queued) this.scheduler.takeWaitingReceipt(taskId);
    // The store cancels queued questions with their tasks; settle cards before the chat-level cleanup.
    for (const taskId of queued) this.questions.cancelForTask(taskId, "cancelled");
    for (const taskId of queued) await this.notifyThreadOutcome(taskId);
    this.questions.cancelForTarget(target);
    let active = false;
    let commitStopUnconfirmed = false;
    let completedCommit: CommitOperation | undefined;
    const commitOperation = this.commitOperationsByTarget.get(targetKey(target));
    const commitTask = commitOperation ? this.store.getTask(commitOperation.taskId) : undefined;
    if (commitOperation && commitTask?.state === "running") {
      active = true;
      if (commitOperation.result === undefined) {
        const stopped = await this.stopCommitOperation(commitOperation);
        if (!stopped) {
          commitStopUnconfirmed = true;
        } else {
          await commitOperation.done;
          if (this.store.getTask(commitOperation.taskId)?.state === "completed") {
            completedCommit = commitOperation;
          }
        }
      } else {
        await commitOperation.done;
        completedCommit = commitOperation;
      }
    }
    for (const runtime of this.runtimes.values()) {
      if (targetKey(runtime.target) !== targetKey(target)) continue;
      active = true;
      runtime.cancelRequested = true;
      runtime.cancelController.abort(new Error("task cancelled"));
      runtime.stopText?.();
      runtime.stopActivity?.();
      runtime.cancelReplySent = true;
      if (runtime.lease) {
        void runtime.lease.close().catch(() => false);
      }
      this.questions.rejectForTask(runtime.taskId, new Error("task cancelled"));
      this.store.markCancelled(runtime.taskId);
    }
    const response = commitStopUnconfirmed
      ? "无法确认提交进程已停止；排队任务已清除，工作树仍受保护。"
      : completedCommit?.result !== undefined
        ? `当前提交已完成，无法取消或回滚；HEAD=${this.shortHead(completedCommit.result.headAfter)}。`
        : queued.length > 0 || active || mergeCancelled || removalCancelled
          ? DEFAULT_CANCELLED_TEXT
          : "当前没有可取消的任务。";
    await this.outbox.enqueueText(target, response, null);
  }
  private abortThreadMerges(target: ChatTarget, reason: Error): boolean {
    const controllers = this.threadMergeControllers.get(targetKey(target));
    if (!controllers) return false;
    for (const controller of controllers) controller.abort(reason);
    return controllers.size > 0;
  }

  private abortAllThreadMerges(reason: Error): void {
    for (const controllers of this.threadMergeControllers.values()) {
      for (const controller of controllers) controller.abort(reason);
    }
  }

  private abortThreadRemovals(target: ChatTarget, reason: Error): boolean {
    const controllers = this.threadRemovalControllers.get(targetKey(target));
    if (!controllers) return false;
    for (const controller of controllers) controller.abort(reason);
    return controllers.size > 0;
  }

  private abortAllThreadRemovals(reason: Error): void {
    for (const controllers of this.threadRemovalControllers.values()) {
      for (const controller of controllers) controller.abort(reason);
    }
  }
  private threadMergeCommandKey(message: InboundMessage): string {
    return `${targetKey(message)}\0${message.messageId}`;
  }

  private finishThreadMergeCommand(message: InboundMessage): boolean {
    const commandKey = this.threadMergeCommandKey(message);
    const controller = this.threadMergeControllersByCommand.get(commandKey);
    if (!controller) return false;
    this.threadMergeControllersByCommand.delete(commandKey);
    const targetKeyValue = targetKey(message);
    const controllers = this.threadMergeControllers.get(targetKeyValue);
    if (controllers) {
      controllers.delete(controller);
      if (controllers.size === 0 && this.threadMergeControllers.get(targetKeyValue) === controllers) {
        this.threadMergeControllers.delete(targetKeyValue);
      }
    }
    return true;
  }

  private finishThreadRemovalCommand(message: InboundMessage): boolean {
    const commandKey = this.threadMergeCommandKey(message);
    const controller = this.threadRemovalControllersByCommand.get(commandKey);
    if (!controller) return false;
    this.threadRemovalControllersByCommand.delete(commandKey);
    const targetKeyValue = targetKey(message);
    const controllers = this.threadRemovalControllers.get(targetKeyValue);
    if (controllers) {
      controllers.delete(controller);
      if (controllers.size === 0 && this.threadRemovalControllers.get(targetKeyValue) === controllers) {
        this.threadRemovalControllers.delete(targetKeyValue);
      }
    }
    return true;
  }


  private async handleStatus(target: ChatTarget): Promise<void> {
    const current = await this.status(target);
    const taskState = current.state;
    const session = current.sessionId ?? "无";
    const cwd = current.cwd ?? "无";
    const goal = current.sessionId
      ? this.store.getGoalSession(this.bot!.uid, target, current.sessionId)
      : null;
    const policy = this.store.resolveChatModel(this.bot!.uid, target);
    const live = this.liveWorker(target);
    let liveModel = "无运行中会话";
    if (live) {
      try {
        const catalog = await live.models();
        liveModel = catalog.current === null
          ? "未知（OMP 未报告）"
          : modelWithLevel(catalog.current, catalog.level);
      } catch {
        liveModel = "未知（OMP 查询失败）";
      }
    }
    await this.outbox.enqueueText(
      target,
      `session=${session}\ncwd=${cwd}\nstate=${taskState}\ngoal=${goal?.status ?? "none"}\nmode=${current.mode}\nmodel=${modelPolicyDescription(policy.model, policy.source)}（下次启动策略）\nliveModel=${liveModel}\nqueued=${current.queued}\noutbox=${current.outbox}\nunknown=${current.unknownOutbox}`,
      null,
    );
  }

  /**
   * `/autoresearch-history`：只读导出。owner-only 已由命令门保证；运行中任务不打断、不排队，
   * 只把本聊天最近一次研究完整记录放进文件 outbox。入队附件是不可变 spool——之后的报告
   * 改写碰不到这封待发件；没有研究记录的聊天只回一句短提示。
   */
  private async handleResearchHistory(target: ChatTarget): Promise<void> {
    const bot = this.bot;
    if (!bot || !this.started || this.stopping) return;
    // spool 目录由本方法唯一拥有：入队失败时只清理这一个目录（host-tools 同款），再交回
    // 现有命令失败路径——claim 保持未完成，用户重试得到的是全新 spool。
    let spoolPath: string | undefined;
    try {
      spoolPath = await this.researchReports.spool({ botUid: bot.uid, target });
      if (spoolPath === undefined) {
        this.outbox.enqueueText(target, RESEARCH_HISTORY_EMPTY_TEXT, null);
        return;
      }
      this.store.enqueueOutbox({
        id: randomUUID(),
        botUid: bot.uid,
        channelId: target.channelId,
        channelType: target.channelType,
        taskId: null,
        kind: "file",
        text: null,
        path: spoolPath,
        fileKind: "file",
        clientMsgNo: randomUUID(),
      });
    } catch (error) {
      if (spoolPath !== undefined) {
        await rm(dirname(spoolPath), { recursive: true, force: true }).catch(() => {});
      }
      throw error;
    }
    this.outbox.flush(target);
  }

  /** 研究事件的完整记录落盘：失败只记日志——报告 I/O 永远不能拖垮产生它的模型任务。 */
  private persistResearchReport(target: ChatTarget, report: string): void {
    const bot = this.bot;
    if (!bot) return;
    void this.researchReports.update({ botUid: bot.uid, target }, report).catch((error: unknown) => {
      console.error(
        `[research-report] 落盘失败 target=${targetKey(target)}：${safeErrorMessage(error, "report write failed")}`,
      );
    });
  }

  private async acceptTask(
    message: InboundMessage,
    text: string,
    historyEligible = true,
    goalRequest?: GoalTaskRequest,
    expectedSessionId?: string,
    acceptGuard?: () => boolean,
    schedule = true,
  ): Promise<boolean> {
    if (message.channelType === 5 && threadShortId(message.channelId) !== null) {
      await this.ensureThreadListen(message);
    }
    if (await this.refuseIncompleteThreadWorkspace(message)) return false;
    this.throwIfStopping();
    let binding = expectedSessionId === undefined
      ? await this.sessions.reconcileExternalBinding(
        message,
        this.store.getBinding(this.bot!.uid, message),
      )
      : this.store.getBinding(this.bot!.uid, message);
    if (acceptGuard && !acceptGuard()) return false;
    if (expectedSessionId !== undefined && binding?.sessionId !== expectedSessionId) return false;
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
    if (
      (expectedSessionId !== undefined
        && this.store.getBinding(this.bot!.uid, message)?.sessionId !== expectedSessionId)
      || (acceptGuard && !acceptGuard())
    ) {
      return false;
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
      // Slash prompts never enter ordinary history. Structured goals are a separate RPC payload and
      // stay in their persisted goal request instead of ever being parsed back out of task.text.
      historyEligible: goalRequest === undefined && historyEligible && !isCommand(text),
      relaySuppressed: false,
    };
    let accepted: boolean;
    // Measured before acceptance so the new task's own row cannot count itself as the work it waits for.
    const startsBehind = this.scheduler.hasBlockingWork(message, task.cwd);
    try {
      accepted = this.store.acceptTask(this.taskClaim(message), task, goalRequest);
    } catch {
      throw new Error("本地状态盘写入失败，任务未接收。");
    }
    if (!accepted) return false;
    this.threadArchives.cancel(message);
    // 持久接受即点亮 task 租约：同聊天或同会话排队等待到终态都保持「正在输入」。
    this.typingLeases.acquire(message, `task:${task.id}`);
    if (startsBehind) this.scheduler.markWaiting(task.id);
    if (schedule) this.scheduler.scheduleTask(task.id);
    this.outbox.flush(message);
    return true;
  }

  private async acquireThreadMergeLease(
    target: ChatTarget,
    expectedBinding: BindingRecord,
    reservation: DestructiveCommandReservation,
    signal: AbortSignal,
  ): Promise<SessionLease> {
    const botUid = this.bot?.uid;
    if (botUid === undefined) throw new Error("Bot 身份未注册，无法合并子区工作区");
    for (;;) {
      if (signal.aborted) throw threadMergeAbortError(signal);
      if (this.isWorkspaceDirectoryBlocked(expectedBinding.cwd)) {
        throw new Error("隔离工作区仍被未确认的 worker 或 host merge 占用，无法安全合并。");
      }
      const current = this.store.getBinding(botUid, target);
      if (
        !current
        || current.cwdPinned !== true
        || current.cwd !== expectedBinding.cwd
        || current.sessionId !== expectedBinding.sessionId
      ) {
        throw new Error("等待合并期间子区绑定已变更，未执行合并。");
      }
      const acquisition = reservation.tryAcquire();
      const lease = await waitWithAbort(acquisition, signal).catch((error: unknown) => {
        void acquisition.then((lateLease) => lateLease?.complete(), () => {});
        throw error;
      });
      if (lease) {
        try {
          await waitWithAbort(lease.ready, signal);
          if (signal.aborted) throw threadMergeAbortError(signal);
          const latest = this.store.getBinding(botUid, target);
          if (
            !latest
            || latest.cwdPinned !== true
            || latest.cwd !== expectedBinding.cwd
            || latest.sessionId !== expectedBinding.sessionId
          ) {
            throw new Error("等待合并期间子区绑定已变更，未执行合并。");
          }
          return lease;
        } catch (error) {
          lease.complete();
          throw error;
        }
      }
      await pauseThreadMergeWait(signal);
    }
  }

  private createHostMergeLifecycle(
    target: ChatTarget,
    cwd: string,
    sourceId: string | undefined,
    resourceCwds: readonly string[],
    commandContext?: {
      readonly commandText: string;
      readonly operatorNote: string;
      readonly senderId: string;
      readonly messageSeq: number;
      readonly receiptOutboxId: string;
    },
    commandBeforeSpawn?: () => void,
  ): WorktreeMergeLifecycle {
    const botUid = this.bot?.uid;
    if (botUid === undefined) throw new Error("Bot 身份未注册，无法合并子区工作区");
    const existing = sourceId === undefined
      ? null
      : this.store.getHostMergeLeaseBySourceId(botUid, target, sourceId);
    if (existing !== null) {
      if (existing.outcome !== null) {
        return {
          existingOutcome: existing.outcome,
          beforeSpawn: () => { throw new Error("known host merge outcomes must be replayed without spawning"); },
          onSpawn: () => { throw new Error("known host merge outcomes must not spawn"); },
          onConfirmedExit: () => { throw new Error("known host merge outcomes already exited"); },
          complete: () => { throw new Error("known host merge outcomes cannot be overwritten"); },
          settle: () => this.store.settleHostMergeLease(existing.id),
          onNotSpawned: () => {},
          onUnknown: () => {},
        };
      }
      return {
        beforeSpawn: () => { throw new Error("an unresolved host merge lease blocks this source id"); },
        onSpawn: () => { throw new Error("an unresolved host merge lease cannot spawn again"); },
        onConfirmedExit: () => { throw new Error("an unresolved host merge lease must be recovered first"); },
        complete: () => { throw new Error("an unresolved host merge lease cannot be overwritten"); },
        settle: () => this.store.settleHostMergeLease(existing.id),
        onNotSpawned: () => {},
        onUnknown: () => {
          for (const resourceCwd of existing.resourceCwds) this.blockedDirectories.add(resourceCwd);
          if (existing.phase !== "unknown") this.store.markHostMergeLeaseUnknown(existing.id);
        },
      };
    }
    const id = randomUUID();
    const plannedResources = [...resourceCwds];
    let reserved = false;
    let confirmedExit = false;
    let outcomePersisted = false;
    return {
      beforeSpawn: (spawnCwd) => {
        if (this.stopping) throw new Error("channel 正在停止");
        if (spawnCwd !== cwd || target.channelType !== 5) {
          throw new Error("host merge process identity changed before spawn");
        }
        if (sourceId === undefined || sourceId.length === 0) {
          throw new Error("host merge requires a stable source id");
        }
        // WorktreePort reaches this boundary after ownership/path validation, so a refusal before it
        // cannot suppress thread outcomes.
        commandBeforeSpawn?.();
        this.store.reserveHostMergeLease({
          id,
          botUid,
          target,
          sourceId,
          cwd,
          resourceCwds: plannedResources,
          ...commandContext,
        });
        reserved = true;
        this.store.beginThreadMerge(botUid, target);
      },
      onSpawn: (identity) => {
        if (!reserved || identity.cwd !== cwd) {
          throw new Error("host merge process spawned without its durable reservation");
        }
        this.store.recordHostMergeProcess(id, identity);
      },
      onConfirmedExit: () => {
        if (!reserved) throw new Error("host merge process exited without its durable reservation");
        this.store.markHostMergeProcessExited(id);
        confirmedExit = true;
      },
      complete: (outcome: WorktreeMergeOutcome) => {
        if (!reserved) throw new Error("host merge outcome has no durable reservation");
        if (!confirmedExit) throw new Error("host merge outcome cannot be persisted before confirmed process exit");
        this.store.persistHostMergeOutcome(id, outcome);
        outcomePersisted = true;
      },
      settle: () => {
        if (!reserved) return;
        this.store.settleHostMergeLease(id);
        reserved = false;
      },
      onNotSpawned: () => {
        if (!reserved) return;
        this.store.completeHostMergeLease(id, "not-spawned");
        reserved = false;
      },
      onUnknown: () => {
        if (!reserved || outcomePersisted) return;
        for (const resourceCwd of plannedResources) this.blockedDirectories.add(resourceCwd);
        this.store.markHostMergeLeaseUnknown(id);
      },
    };
  }

  private async executeHostThreadMerge(
    message: InboundMessage,
    expectedBinding: BindingRecord,
    receiptOutboxId: string,
    archiveGeneration: number,
    operatorNote: string,
  ): Promise<WorktreeMergeSummary & { archive?: "scheduled" }> {
    const botUid = this.bot?.uid;
    if (botUid === undefined) throw new Error("Bot 身份未注册，无法合并子区工作区");
    const targetKeyValue = targetKey(message);
    const controller = new AbortController();
    const controllers = this.threadMergeControllers.get(targetKeyValue) ?? new Set<AbortController>();
    controllers.add(controller);
    this.threadMergeControllersByCommand.set(this.threadMergeCommandKey(message), controller);
    this.threadMergeControllers.set(targetKeyValue, controllers);
    const shutdownSignal = this.shutdownController.signal;
    const abortOnShutdown = (): void => controller.abort(new Error("channel stopping"));
    if (shutdownSignal.aborted) abortOnShutdown();
    else shutdownSignal.addEventListener("abort", abortOnShutdown, { once: true });

    let reservation: DestructiveCommandReservation | undefined;
    let lease: SessionLease | undefined;
    try {
      const prior = this.store.getHostMergeLeaseBySourceId(botUid, message, message.messageId);
      let plan: WorktreeMergePlan | undefined;
      let resourceCwds: readonly string[];
      if (prior?.outcome !== undefined && prior.outcome !== null) {
        resourceCwds = prior.resourceCwds;
      } else {
        reservation = this.scheduler.reserveDestructiveCommandSession(message, [expectedBinding.cwd]);
        const worktrees = this.options.worktrees;
        if (!worktrees) throw new Error("本部署未配置隔离工作区能力");
        const request: WorktreeMergeRequest = { cwd: expectedBinding.cwd };
        const ownership = this.store.getThreadWorkspace(botUid, message);
        if (ownership !== null) {
          const project = ownership.projects.find(
            (candidate) =>
              candidate.created
              && candidate.path === expectedBinding.cwd
              && candidate.branch !== null,
          );
          if (!project || project.branch === null) {
            throw new Error("当前子区隔离工作区与持久化所有权不匹配，无法合并");
          }
          request.repo = project.repo;
          request.branch = project.branch;
        }
        plan = await worktrees.planMerge(request, controller.signal);
        resourceCwds = plan.cwds;
        reservation.extend(resourceCwds);
      }

      const lifecycle = this.createHostMergeLifecycle(
        message,
        expectedBinding.cwd,
        message.messageId,
        resourceCwds,
        {
          commandText: safeDisplayText(message.text, 800) ?? "/thread merge",
          operatorNote: safeDisplayText(operatorNote, 200) ?? "",
          senderId: message.senderId,
          messageSeq: message.messageSeq,
          receiptOutboxId,
        },
        () => {
          const current = this.store.getBinding(botUid, message);
          if (
            !current
            || current.cwdPinned !== true
            || current.cwd !== expectedBinding.cwd
            || current.sessionId !== expectedBinding.sessionId
          ) {
            throw new Error("合并执行前子区绑定已变更，未执行合并。");
          }
          this.store.completeCommand(this.commandClaim(message, "thread"));
        },
      );

      if (reservation !== undefined) {
        lease = await this.acquireThreadMergeLease(
          message,
          expectedBinding,
          reservation,
          controller.signal,
        );
      }
      const coveredResources = new Set(resourceCwds.map((cwd) => resolve(cwd)));
      const host: ThreadMergeHost = {
        botUid,
        store: this.store,
        worktrees: this.options.worktrees,
        sourceId: message.messageId,
        worktreeMergeLifecycle: () => lifecycle,
        archiveGeneration: () => archiveGeneration,
        planThreadArchive: (target, groupId, shortId, generation) => {
          this.pendingHostMergeArchives.set(targetKey(target), {
            target: { ...target },
            groupId,
            shortId,
            generation,
            receiptOutboxIds: [receiptOutboxId],
          });
        },
        tryAcquireResources: async (cwds) => {
          const requestedResources = cwds.map((cwd) => resolve(cwd));
          if (
            requestedResources.length === 0
            || requestedResources.some((cwd) => !coveredResources.has(cwd))
          ) {
            return undefined;
          }
          return { ready: Promise.resolve(), complete: () => {} };
        },
      };
      let summary: WorktreeMergeSummary & { archive?: "scheduled" };
      try {
        summary = await mergeThreadWorkspace(host, message, {}, controller.signal, plan);
      } catch (error) {
        lifecycle.onUnknown();
        this.pendingHostMergeArchives.delete(targetKeyValue);
        throw error;
      }
      // The command's message ID is the stable operation identity shared with recovery. The
      // publisher is the only owner of the success event; failures never reach it.
      await this.publishThreadMergeSuccess(
        message,
        summary,
        summary.archive === "scheduled" ? "scheduled" : "not-scheduled",
        message.messageId,
      );
      return summary;
    } finally {
      reservation?.release();
      lease?.complete();
      shutdownSignal.removeEventListener("abort", abortOnShutdown);
      // The controller stays active until handleCommand has durably accepted the receipt or repair.
    }
  }

  private async executeHostThreadRemoval(
    message: InboundMessage,
    ownership: ThreadWorkspaceRecord,
    force: boolean,
    receiptOutboxId: string,
  ): Promise<HostThreadRemovalOutcome | { kind: "refused"; message: string }> {
    const botUid = this.bot?.uid;
    const worktrees = this.options.worktrees;
    if (botUid === undefined) return { kind: "refused", message: "Bot 身份未注册，未执行本地清理。" };
    if (!worktrees) return { kind: "refused", message: "本部署未配置隔离工作区能力。" };

    const resourcePaths = threadWorkspaceCleanupPaths(ownership, true);
    const anchor = resourcePaths[0]
      ?? ownership.projects[0]?.repo
      ?? ownership.entryPath
      ?? this.store.getBinding(botUid, message)?.cwd;
    if (anchor === undefined) {
      return { kind: "refused", message: "当前子区没有可追踪的本地工作区资源，未执行清理。" };
    }
    if (resourcePaths.some(path => this.isWorkspaceDirectoryBlocked(path))) {
      return {
        kind: "refused",
        message: "相关工作区仍受未确认的清理进程封锁；本次未重试，也未归档。",
      };
    }

    const targetKeyValue = targetKey(message);
    const commandKey = this.threadMergeCommandKey(message);
    const controller = new AbortController();
    const controllers = this.threadRemovalControllers.get(targetKeyValue) ?? new Set<AbortController>();
    controllers.add(controller);
    this.threadRemovalControllers.set(targetKeyValue, controllers);
    this.threadRemovalControllersByCommand.set(commandKey, controller);
    const shutdownSignal = this.shutdownController.signal;
    const abortOnShutdown = (): void => controller.abort(new Error("channel stopping"));
    if (shutdownSignal.aborted) abortOnShutdown();
    else shutdownSignal.addEventListener("abort", abortOnShutdown, { once: true });

    let sessionLease: SessionLease | undefined;
    let reserved = false;
    let operationUnknown = false;
    const id = randomUUID();
    const context = {
      id,
      botUid,
      target: message,
      sourceId: message.messageId,
      commandText: safeDisplayText(message.text, 800) ?? "/thread remove",
      senderId: message.senderId,
      messageSeq: message.messageSeq,
      receiptOutboxId,
      resourcePaths,
    } as const;
    try {
      if (controller.signal.aborted || this.stopping) {
        return { kind: "refused", message: "channel 正在停止，未开始本地清理。" };
      }
      // Missing owned paths still need directory leases; only existing paths can be Git-probed.
      const repositoryCwds: string[] = [];
      for (const path of resourcePaths) {
        try {
          await lstat(path);
          repositoryCwds.push(path);
        } catch (error) {
          if ((error as NodeJS.ErrnoException).code !== "ENOENT") throw error;
        }
      }
      sessionLease = await this.scheduler.tryAcquireDestructiveCommandSession(
        message,
        repositoryCwds,
        resourcePaths,
      );
      if (!sessionLease) {
        return { kind: "refused", message: "当前子区或其工作区资源正被任务占用；清理命令已立即拒绝，未排队。" };
      }
      const cleanupLease: SessionLease = {
        ready: sessionLease.ready,
        complete: () => {},
      };
      const removalLifecycle = (
        project: ThreadWorkspaceRecord["projects"][number],
        step: WorktreeRemovalStep,
      ): WorktreeRemovalLifecycle => {
        let beforeSpawned = false;
        let processRunning = false;
        const expectedCwd = step === "worktree" ? project.path : project.repo;
        const failClosedProcess = (): void => {
          operationUnknown = true;
          for (const path of resourcePaths) this.blockedDirectories.add(path);
          if (reserved) {
            try {
              this.store.markHostRemovalLeaseUnknown(id);
            } catch {
              // In-memory directory blocks still protect the running process if persistence also fails.
            }
          }
        };
        return {
          beforeSpawn: cwd => {
            if (this.stopping || controller.signal.aborted) throw new Error("channel 正在停止");
            if (expectedCwd === null || cwd !== expectedCwd || message.channelType !== 5) {
              throw new Error("host removal process identity changed before spawn");
            }
            if (!reserved) {
              this.store.reserveHostRemovalLease({ ...context, cwd });
              reserved = true;
            }
            this.store.beginHostRemovalStep(id, step, cwd);
            beforeSpawned = true;
          },
          onSpawn: identity => {
            if (!reserved || !beforeSpawned || identity.cwd !== expectedCwd) {
              failClosedProcess();
              throw new Error("host removal process spawned without its durable reservation");
            }
            try {
              this.store.recordHostRemovalProcess(id, identity);
              processRunning = true;
            } catch (error) {
              failClosedProcess();
              throw error;
            }
          },
          onConfirmedExit: () => {
            if (!reserved || !processRunning) {
              failClosedProcess();
              throw new Error("host removal process exited without its durable reservation");
            }
            try {
              this.store.markHostRemovalProcessExited(id);
              processRunning = false;
              beforeSpawned = false;
            } catch (error) {
              failClosedProcess();
              throw error;
            }
          },
          onNotSpawned: () => {
            if (!reserved || !beforeSpawned) return;
            if (processRunning) this.store.markHostRemovalProcessExited(id);
            this.store.releaseHostRemovalLease(id);
            reserved = false;
            beforeSpawned = false;
            processRunning = false;
          },
          onUnknown: failClosedProcess,
        };
      };

      const result = await cleanupOwnedThreadWorkspace({
        ownership,
        store: this.store,
        worktrees,
        force,
        deleteBranch: true,
        lease: cleanupLease,
        signal: controller.signal,
        lifecycle: removalLifecycle,
        isUnknown: () => operationUnknown,
      });
      if (result.unknown || operationUnknown) {
        return {
          kind: "refused",
          message: "本地清理进程结果未确认；相关资源已封锁，本次不归档且不会自动重试。",
        };
      }
      if (this.stopping || controller.signal.aborted) {
        if (reserved) this.releaseOrBlockHostRemoval(id, resourcePaths);
        return { kind: "refused", message: "channel 已停止或清理已中止；本次不归档。" };
      }

      const detail = result.messages.join("；");
      const outcome: HostThreadRemovalOutcome = result.complete
        ? {
            kind: "complete",
            receiptText: `本地隔离工作区与记录分支清理完成；回执送达并完成当前输出后将归档子区。\n${detail}`,
          }
        : {
            kind: "partial",
            receiptText: `本地工作区清理未完整完成，本次不归档；已确认的资源结果已逐项保存。请核对后重新执行 /thread remove。${detail.length === 0 ? "" : `详情：${detail}`}`,
          };
      if (!reserved) {
        this.store.reserveHostRemovalLease({ ...context, cwd: anchor });
        reserved = true;
      }
      this.store.persistHostRemovalOutcome(id, outcome);
      return outcome;
    } catch (error) {
      if (reserved && !operationUnknown) this.releaseOrBlockHostRemoval(id, resourcePaths);
      const detail = safeDisplayText(safeErrorMessage(error, "清理失败"), 180) ?? "未知错误";
      return {
        kind: "refused",
        message: `本地清理未完成且不会自动重试：${detail}。请先核对资源状态。`,
      };
    } finally {
      sessionLease?.complete();
      shutdownSignal.removeEventListener("abort", abortOnShutdown);
    }
  }


  private enqueueThreadMergeReceipt(target: ChatTarget, id: string, text: string): void {
    this.enqueueHostCommandReceipt(target, id, text, "merge");
  }

  private enqueueThreadRemovalReceipt(target: ChatTarget, id: string, text: string): void {
    for (const part of this.removalReceiptParts(id, text)) {
      this.enqueueHostCommandReceipt(target, part.id, part.text, "remove");
    }
  }

  private removalReceiptParts(id: string, text: string): { id: string; text: string }[] {
    const chunks = splitUtf8(text);
    return chunks.map((chunk, index) => ({
      id: index === chunks.length - 1
        ? id
        : `${id.slice(0, 24)}${createHash("sha256").update(`${id}:${index}`).digest("hex").slice(0, 12)}`,
      text: chunk,
    }));
  }

  private enqueueHostCommandReceipt(
    target: ChatTarget,
    id: string,
    text: string,
    operation: "merge" | "remove",
  ): void {
    const botUid = this.bot?.uid;
    if (botUid === undefined) throw new Error("Bot 身份未注册，无法保存子区宿主操作回执。");
    try {
      const existing = this.store.getOutbox(id);
      if (existing) {
        if (
          existing.botUid !== botUid
          || existing.channelType !== target.channelType
          || existing.channelId !== target.channelId
          || existing.kind !== "text"
          || existing.text !== text
        ) {
          throw new Error(`stable thread ${operation} receipt id conflicts with a different outbox record`);
        }
      } else {
        this.store.enqueueOutbox({
          id,
          botUid,
          channelId: target.channelId,
          channelType: target.channelType,
          taskId: null,
          kind: "text",
          text,
          path: null,
          fileKind: null,
          clientMsgNo: id,
        });
      }
    } catch (error) {
      const detail = safeDisplayText(error instanceof Error ? error.message : String(error), 160);
      console.error(`[thread-${operation}] 回执持久化失败 id=${id}: ${detail ?? "outbox write failed"}`);
      this.pendingHostMergeArchives.delete(targetKey(target));
      throw new Error(`子区${operation === "merge" ? "合并" : "本地清理"}回执无法持久化；归档已阻止。`, { cause: error });
    }
    void this.outbox.flush(target).catch(() => {});
  }

  private settleHostMergeForSource(target: ChatTarget, sourceId: string): void {
    const botUid = this.bot?.uid;
    if (botUid === undefined) throw new Error("Bot 身份未注册，无法结算 host merge。");
    const lease = this.store.getHostMergeLeaseBySourceId(botUid, target, sourceId);
    if (!lease) throw new Error("host merge outcome lease disappeared before durable delivery");
    if (lease.outcome === null) throw new Error("host merge cannot settle before a known outcome is durable");
    this.store.settleHostMergeLease(lease.id);
  }

  private settleHostRemovalForSource(target: ChatTarget, sourceId: string): void {
    const botUid = this.bot?.uid;
    if (botUid === undefined) throw new Error("Bot 身份未注册，无法结算 host removal。");
    const lease = this.store.getHostRemovalLeaseBySourceId(botUid, target, sourceId);
    if (!lease) throw new Error("host removal outcome lease disappeared before durable delivery");
    if (lease.outcome === null) throw new Error("host removal cannot settle before a known outcome is durable");
    this.store.settleHostRemovalLease(lease.id);
  }

  private recoverHostRemovalArchive(
    target: InboundMessage,
    receiptOutboxId: string,
    archiveGeneration?: number,
  ): void {
    const groupId = threadParentGroupId(target.channelId);
    const shortId = threadShortId(target.channelId);
    if (groupId === null || shortId === null) throw new Error("cannot recover archive for an invalid thread target");
    const outcome = this.store.getHostRemovalLeaseBySourceId(this.bot!.uid, target, target.messageId)!.outcome!;
    this.pendingHostMergeArchives.set(targetKey(target), {
      target: { ...target },
      groupId,
      shortId,
      generation: archiveGeneration ?? this.threadArchives.generation(target),
      receiptOutboxIds: this.removalReceiptParts(receiptOutboxId, outcome.receiptText).map(part => part.id),
    });
    if (archiveGeneration === undefined) {
      this.recoveredHostMergeArchiveTargets.add(targetKey(target));
    }
  }

  private recoverHostMergeArchive(target: InboundMessage, receiptOutboxId: string): void {
    const groupId = threadParentGroupId(target.channelId);
    const shortId = threadShortId(target.channelId);
    if (groupId === null || shortId === null) throw new Error("cannot recover archive for an invalid thread target");
    this.pendingHostMergeArchives.set(targetKey(target), {
      target: { ...target },
      groupId,
      shortId,
      generation: this.threadArchives.generation(target),
      receiptOutboxIds: [receiptOutboxId],
    });
    this.recoveredHostMergeArchiveTargets.add(targetKey(target));
  }

  /** Release durable-outcome archive plans only after cards and the transport are ready. */
  private releaseRecoveredHostMergeArchives(): void {
    const targets = [...this.recoveredHostMergeArchiveTargets];
    this.recoveredHostMergeArchiveTargets.clear();
    for (const key of targets) {
      const pending = this.pendingHostMergeArchives.get(key);
      if (pending) this.runThreadArchive(pending.target);
    }
  }

  private trackProgressTail(target: ChatTarget, taskId: string, getTail: () => Promise<void>): void {
    const key = targetKey(target);
    const tails = this.progressTails.get(key) ?? new Map<string, () => Promise<void>>();
    tails.set(taskId, getTail);
    this.progressTails.set(key, tails);
  }

  private releaseProgressTail(target: ChatTarget, taskId: string, tail: Promise<void>): void {
    const key = targetKey(target);
    const tails = this.progressTails.get(key);
    if (!tails?.has(taskId)) return;
    const remove = (): void => {
      tails.delete(taskId);
      if (tails.size === 0 && this.progressTails.get(key) === tails) {
        this.progressTails.delete(key);
      }
    };
    void tail.then(remove, remove);
  }

  private runThreadArchive(target: ChatTarget): void {
    const key = targetKey(target);
    const pending = this.pendingHostMergeArchives.get(key);
    if (pending) {
      this.pendingHostMergeArchives.delete(key);
      this.threadArchives.schedule(
        pending.target,
        pending.groupId,
        pending.shortId,
        pending.generation,
        pending.receiptOutboxIds,
      );
    }
    const progressTail = Promise.allSettled(
      [...(this.progressTails.get(key)?.values() ?? [])].map((getTail) => getTail()),
    ).then(() => {});
    this.operationSet(this.threadArchives.run(target, progressTail));
  }

  /**
   * Host tools 的宿主适配：工具模块只看到它真正需要的状态。重启排定已委托给独立的
   * `ServiceRestart` 所有者（工具只拿到它的 `schedule` 与 `pending`），channel 快照仍留在
   * 服务里，工具因此不能绕过运行时的调度与生命周期。
   */
  private hostToolHost(): HostToolHost {
    const botUid = this.bot?.uid;
    if (botUid === undefined) throw new Error("Bot 身份未注册，无法构造 host tools");
    return {
      stateDir: this.options.config.stateDir,
      transport: this.options.transport,
      worktrees: this.options.worktrees,
      store: this.store,
      botUid,
      enqueueText: (target, text, taskId) => this.outbox.enqueueText(target, text, taskId),
      flushTarget: (target) => this.outbox.flush(target),
      planThreadArchive: (target, groupId, shortId, generation) =>
        this.threadArchives.schedule(target, groupId, shortId, generation),
      publishThreadMergeSuccess: (target, summary, archive, operationId) =>
        this.publishThreadMergeSuccess(target, summary, archive, operationId),
      replayThreadMergeSuccess: (target, operationId) =>
        this.replayThreadMergeSuccess(target, operationId),
      threadArchiveGeneration: (target) => this.threadArchives.generation(target),
      worktreeMergeLifecycle: (target, cwd, sourceId, resourceCwds) =>
        this.createHostMergeLifecycle(target, cwd, sourceId, resourceCwds),
      tryAcquireDestructiveResources: (cwds, task) =>
        this.scheduler.tryAcquireDestructiveResources(cwds, task),
      deliverToChat: async (message) => {
        if (await this.threadWorkspaceIncomplete(message)) {
          throw new Error("子区工作区未就绪，未派发任务。");
        }
        if (await this.appendToRunningTask(message, message.text)) return "steered";
        await this.acceptTask(message, message.text);
        return "queued";
      },
      askQuestions: (task, target, args, signal, toolCallId) =>
        this.questions.askQuestions(task, target, args, signal, toolCallId),
      channelStatus: () => ({
        pid: process.pid,
        restart_pending: this.serviceRestart.pending,
        running_targets: [...new Set([
          ...[...this.runtimes.values()].map((runtime) => targetKey(runtime.target)),
          ...this.commitOperationsByTarget.keys(),
          ...this.threadMergeControllers.keys(),
        ])],
        queued_tasks: this.store.listQueuedTasks().length,
        blocked_cwds: this.blockedDirectories.size,
      }),
      scheduleRestart: () => this.serviceRestart.schedule(),
    };
  }


  private async stopCommitProcess(operation: CommitOperation): Promise<boolean> {
    if (operation.tracked) {
      try {
        const stopped = await operation.tracked.close(true);
        if (!stopped) this.blockedDirectories.add(operation.root);
        return stopped;
      } catch {
        this.blockedDirectories.add(operation.root);
        return false;
      }
    }
    const botUid = this.bot?.uid;
    if (botUid === undefined) {
      this.blockedDirectories.add(operation.root);
      return false;
    }
    try {
      const ledger = this.store.listWorkerLedger(botUid).find((record) => record.id === operation.taskId);
      if (!ledger) return true;
      let stopped = false;
      try {
        stopped = await this.options.workerFactory.recover(ledger);
        if (stopped) this.store.markWorkerStopped(ledger.id);
      } catch {
        stopped = false;
      }
      if (!stopped) {
        this.blockedDirectories.add(ledger.cwd);
        this.blockedDirectories.add(operation.root);
      }
      return stopped;
    } catch {
      this.blockedDirectories.add(operation.root);
      return false;
    }
  }

  private async runCommitTask(
    taskId: string,
    expectedCwd?: string,
    scheduledOperation?: CommitOperation,
  ): Promise<void> {
    const persisted = this.store.getTask(taskId);
    if (!persisted || persisted.state !== "queued") return;
    const target = taskTarget(persisted);
    let operation = scheduledOperation;
    let acquiredReservation: CommitLease | undefined;
    let taskStarted = false;
    let progress: TaskProgress | undefined;
    try {
      if (this.stopping) return;
      const binding = this.store.getBinding(persisted.botUid, target);
      let current = this.store.getTask(taskId);
      if (this.stopping || !current || current.state !== "queued") return;
      if (
        binding === null
        || binding.cwd !== current.cwd
        || binding.sessionId !== current.sessionId
        || (expectedCwd !== undefined && binding.cwd !== expectedCwd)
        || (operation !== undefined && (
          binding.sessionFile !== operation.binding.sessionFile
          || binding.cwdPinned !== operation.binding.cwdPinned
        ))
      ) {
        this.store.markFailed(taskId, "提交绑定在执行前发生变化，未提交。");
        this.outbox.enqueueText(target, "提交绑定已变化，请重新确认当前项目后重试。", taskId);
        return;
      }
      const workspace = await this.resolveCommitWorkspace(target, binding);
      current = this.store.getTask(taskId);
      if (this.stopping || !current || current.state !== "queued") return;
      if ("error" in workspace) {
        this.store.markFailed(taskId, workspace.error);
        this.outbox.enqueueText(target, workspace.error, taskId);
        return;
      }
      if (operation && operation.root !== workspace.root) {
        this.store.markFailed(taskId, "提交工作树在调度前发生变化，未执行。");
        this.outbox.enqueueText(target, "提交工作树已变化，未执行提交。", taskId);
        return;
      }
      if (!operation) {
        acquiredReservation = await this.scheduler.tryAcquireCommitRepository(workspace.root, taskId);
        current = this.store.getTask(taskId);
        if (this.stopping || !current || current.state !== "queued") return;
        if (!acquiredReservation) {
          // A recovered 0/0 commit has not entered the native CLI yet. Keep it queued while an
          // unconfirmed worker still owns this Git worktree; restart will re-evaluate the lease.
          if (await this.isRepositoryBlocked(workspace.root)) return;
          this.store.markFailed(taskId, COMMIT_BUSY_TEXT);
          this.outbox.enqueueText(target, COMMIT_BUSY_TEXT, taskId);
          return;
        }
        operation = this.newCommitOperation(
          taskId,
          target,
          workspace.root,
          binding,
          acquiredReservation,
        );
      }
      if (operation.stopRequested) {
        this.store.markCancelled(taskId, "commit stopped before dispatch");
        return;
      }
      if (this.stopping || operation.shutdownRequested || this.isWorkspaceDirectoryBlocked(workspace.root)) {
        return;
      }
      if (!this.store.markStarted(taskId)) return;
      taskStarted = true;
      this.typingLeases.acquire(target, `task:${taskId}`);
      this.store.markTaskPromptStarted(taskId, persisted.sessionId);
      progress = this.progressCards.startTask(taskId, target);
      this.trackProgressTail(target, taskId, () => progress!.tail);

      const model = this.store.resolveChatModel(persisted.botUid, target).model;
      const commitWorker = {
        cwd: workspace.root,
        ...(model === null ? {} : { model }),
        /**
         * The native worker calls this only after its gate opens and its process identity is
         * durably recorded. The callback is synchronous and never awaits card I/O, so a slow or
         * broken card cannot stop stdout/stderr draining or the CLI result.
         */
        onProgress: (event: Extract<ExecutionEvent, { type: "commit_progress" }>) => {
          progress?.apply(event);
        },
      };
      try {
        try {
          operation.tracked = await openTrackedCommit({
            workerFactory: this.options.workerFactory,
            store: this.store,
            worker: commitWorker,
            ledger: {
              id: taskId,
              botUid: persisted.botUid,
              sessionId: persisted.sessionId,
              sessionFile: binding.sessionFile ?? "",
            },
            blockDirectory: (cwd) => this.blockedDirectories.add(cwd),
          });
        } finally {
          operation.markWorkerReady();
        }
      } catch {
        const stopped = operation.stopRequested
          ? await this.stopCommitOperation(operation)
          : await this.stopCommitProcess(operation);
        if (this.stopping || operation.shutdownRequested) return;
        if (!stopped) {
          this.outbox.enqueueText(target, "无法确认提交进程已停止；任务保持 fail-closed。", taskId);
          return;
        }
        if (operation.stopRequested) {
          this.store.markCancelled(taskId, "commit stopped before native execution");
          return;
        }
        this.store.markFailed(taskId, "native commit worker could not be created");
        this.outbox.enqueueText(target, "提交启动失败，Git 命令未执行。", taskId);
        return;
      }
      const tracked = operation.tracked;
      if (tracked === undefined) throw new Error("tracked commit worker missing after startup");

      if (this.stopping || operation.shutdownRequested || operation.stopRequested) {
        const stopped = operation.stopRequested
          ? await this.stopCommitOperation(operation)
          : await this.stopCommitProcess(operation);
        if (operation.stopRequested && !this.stopping) {
          if (stopped) {
            this.store.markCancelled(taskId, "commit stopped before native execution");
          } else {
            this.outbox.enqueueText(target, "无法确认提交进程已停止；任务保持 fail-closed。", taskId);
          }
        }
        return;
      }

      let result: CommitResult;
      try {
        const runPromise = tracked.worker.run();
        const shutdownSignal = this.shutdownController.signal;
        let shutdownListener: (() => void) | undefined;
        const shutdownPromise = new Promise<never>((_, reject) => {
          const onShutdown = () => reject(new Error("service is shutting down"));
          shutdownListener = onShutdown;
          if (shutdownSignal.aborted) onShutdown();
          else shutdownSignal.addEventListener("abort", onShutdown, { once: true });
        });
        try {
          result = await Promise.race([runPromise, shutdownPromise]);
          operation.result = result;
        } finally {
          if (shutdownListener) shutdownSignal.removeEventListener("abort", shutdownListener);
        }
      } catch {
        const stopped = operation.stopRequested
          ? await this.stopCommitOperation(operation)
          : await this.stopCommitProcess(operation);
        if (this.stopping || operation.shutdownRequested) return;
        if (!stopped) {
          this.settleCommitProgress(progress, "failed", COMMIT_RESULT_UNKNOWN);
          this.outbox.enqueueText(target, "无法确认提交进程已停止或结果；任务保持 fail-closed。", taskId);
          return;
        }
        if (operation.stopRequested) {
          this.store.markCancelled(taskId, "commit stopped without a reported result");
          this.settleCommitProgress(progress, "cancelled", COMMIT_RESULT_UNKNOWN);
          this.outbox.enqueueText(
            target,
            "已停止提交进程，但命令未返回 HEAD 结果；请检查当前 HEAD，且不会自动重试或回滚。",
            taskId,
          );
          return;
        }
        const failure = "native commit worker failed before reporting its result";
        this.store.markFailed(taskId, failure);
        this.settleCommitProgress(progress, "failed", COMMIT_RESULT_UNKNOWN);
        this.outbox.enqueueText(target, "提交未能返回结果；请检查仓库 HEAD 后再决定是否重试。", taskId);
        return;
      }

      const closed = operation.stopRequested
        ? await this.stopCommitOperation(operation)
        : await this.stopCommitProcess(operation);
      if (!closed) {
        // Process exit is not confirmed, so neither the task nor the card may claim a stop or a
        // successful result. The card gets only the fixed unknown line.
        this.settleCommitProgress(progress, "failed", COMMIT_RESULT_UNKNOWN, result.content);
        this.outbox.enqueueText(
          target,
          `无法确认提交进程已退出，任务保持 fail-closed；${this.commitResultNotice(result, false)}`,
          taskId,
        );
        return;
      }
      if (this.stopping || operation.shutdownRequested) return;

      const stopConfirmed = operation.stopRequested && operation.stopConfirmed === true;
      const summary = this.commitResultNotice(result, stopConfirmed);
      if (stopConfirmed && result.exitCode !== 0) {
        this.store.markCancelled(taskId, "commit stopped");
        this.settleCommitProgress(progress, "cancelled", summary, result.content);
        this.outbox.enqueueText(target, summary, taskId);
      } else if (result.exitCode === 0) {
        this.store.markCompleted(taskId, summary);
        this.settleCommitProgress(progress, "completed", summary, result.content);
        this.outbox.enqueueText(target, summary, taskId);
      } else {
        this.store.markFailed(taskId, summary);
        this.settleCommitProgress(progress, "failed", summary, result.content);
        this.outbox.enqueueText(target, summary, taskId);
      }
    } catch {
      if (this.stopping || operation?.shutdownRequested) return;
      const failure = "native commit task failed before completion";
      const task = this.store.getTask(taskId);
      if (task?.state === "queued" || task?.state === "running") {
        this.store.markFailed(taskId, failure);
        this.outbox.enqueueText(target, "提交启动失败，无法确认结果；请检查仓库 HEAD 后再决定是否重试。", taskId);
      }
    } finally {
      if (progress) {
        if (!progress.finished) {
          const state = this.store.getTask(taskId)?.state;
          // A shutdown return leaves the task running for recovery; do not invent a terminal
          // card there. Real completed/failed/cancelled rows close the card even when their
          // branch returned before applying a result (finish is a no-op for an unstarted card).
          if (state === "completed" || state === "failed" || state === "cancelled") {
            if (state !== "completed" && progress.commitStarted) {
              progress.apply({ type: "commit_result", atMs: Date.now(), summary: COMMIT_RESULT_UNKNOWN });
            }
            progress.finish(state);
          }
        }
        progress.stop(!progress.finished);
        this.releaseProgressTail(target, taskId, progress.tail);
      }
      operation?.markWorkerReady();
      if (taskStarted || operation !== undefined) {
        this.typingLeases.release(target, `task:${taskId}`);
      }
      if (operation) this.forgetCommitOperation(operation);
      else acquiredReservation?.complete();
      this.serviceRestart.maybeRunDeferred();
    }
  }

  private async runTask(taskId: string, expectedCwd?: string): Promise<void> {
    const persistedTask = this.store.getTask(taskId);
    if (!persistedTask || persistedTask.state !== "queued") return;
    const target = taskTarget(persistedTask);
    const goalTask = this.store.getGoalTask(taskId);
    if (persistedTask.text === "/commit" && this.store.isCommitTask(taskId)) {
      await this.runCommitTask(taskId, expectedCwd);
      return;
    }
    if (await this.threadWorkspaceIncomplete(target, true)) {
      if (goalTask) {
        const cancelled = this.store.stopGoalContinuation(this.bot!.uid, target, goalTask.sessionId, "pause");
        await this.cleanupCancelledGoalTasks(target, cancelled);
      }
      this.scheduler.takeWaitingReceipt(taskId);
      this.store.markFailed(taskId, THREAD_WORKSPACE_UNAVAILABLE_TEXT);
      this.questions.cancelForTask(taskId, "ended");
      this.questions.rejectForTask(taskId, new Error("task finished"));
      await this.outbox.enqueueText(target, THREAD_WORKSPACE_UNAVAILABLE_TEXT, taskId);
      await this.notifyThreadOutcome(taskId);
      return;
    }
    let goalSessionId = goalTask?.sessionId ?? persistedTask.sessionId;
    const bindingAtStart = goalTask
      ? this.store.getBinding(this.bot!.uid, target)
      : await this.reconcileTaskBinding(taskId, target, this.store.getBinding(this.bot!.uid, target));
    if (goalTask && (
      !bindingAtStart
      || bindingAtStart.sessionId !== goalTask.sessionId
      || persistedTask.sessionId !== goalTask.sessionId
    )) {
      const cancelled = this.store.stopGoalContinuation(this.bot!.uid, target, goalTask.sessionId, "pause");
      await this.cleanupCancelledGoalTasks(target, cancelled);
      this.scheduler.takeWaitingReceipt(taskId);
      this.store.markFailed(taskId, "原生目标绑定会话身份已变化，拒绝切换会话或重放请求。");
      this.typingLeases.release(target, `task:${taskId}`);
      await this.outbox.enqueueText(target, "原生目标绑定会话已变化；为避免在其他会话执行，本次操作已拒绝。", taskId);
      await this.notifyThreadOutcome(taskId);
      return;
    }
    const task = bindingAtStart
      ? { ...persistedTask, cwd: bindingAtStart.cwd, sessionId: bindingAtStart.sessionId }
      : persistedTask;
    if (expectedCwd !== undefined && task.cwd !== expectedCwd) {
      this.scheduler.takeWaitingReceipt(taskId);
      if (goalTask) {
        const cancelled = this.store.stopGoalContinuation(this.bot!.uid, target, goalTask.sessionId, "pause");
        await this.cleanupCancelledGoalTasks(target, cancelled);
      }
      this.store.markFailed(taskId, "绑定目录在启动前再次变更，任务未启动。");
      await this.outbox.enqueueText(target, goalTask ? "原生目标绑定目录已变化；为避免在其他会话执行，本次操作已拒绝。" : GENERIC_TASK_FAILURE_NOTICE, taskId);
      await this.notifyThreadOutcome(taskId);
      return;
    }
    if (this.isWorkspaceDirectoryBlocked(task.cwd)) {
      // 旧 worker 的死亡未确认：任务保持排队、继续占住该目录的队列位，绝不启动第二个写者，
      // 也不把它当作失败丢掉；下一个进程再试。失败收尾只留给确定无法恢复的情形。
      console.error(`[task] 目录仍被未确认的 worker 占用，任务保持排队 task=${taskId}`);
      return;
    }
    if (!this.store.markStarted(taskId)) return;
    // Goal recovery requires its own native goal receipt; ordinary prompts retain prompt receipts.
    const goalResuming = this.store.isGoalTaskResuming(taskId);
    const resuming = this.store.isTaskResuming(taskId) || goalResuming;
    this.typingLeases.acquire(taskTarget(task), `task:${taskId}`);
    if (this.scheduler.takeWaitingReceipt(taskId)) {
      this.outbox.enqueueText(taskTarget(task), receiptText("开始执行排队消息", task.text), null);
    }
    const turnContext = new TurnContext({
      store: this.store,
      transport: this.options.transport,
      stateDir: this.options.config.stateDir,
      botUid: this.bot!.uid,
      target,
    });
    const runtime: RuntimeTask = {
      taskId,
      isGoalTask: goalTask !== null,
      target,
      cwd: task.cwd,
      cancelRequested: false,
      cancelController: new AbortController(),
      cancelReplySent: false,
      promptInFlight: false,
      suspended: false,
      stopCause: null,
      workAfterStop: false,
      steerDeliveries: [],
      steerSerial: Promise.resolve(),
      turnContext,
    };
    this.runtimes.set(taskId, runtime);
    const textDelivery = this.outbox.registerTask(taskId);
    const progress = this.progressCards.startTask(taskId, target);
    this.trackProgressTail(target, taskId, () => progress.tail);
    /** 本回合的 worker 事件归集：通知帧、本地命令标记、assistant 文本与 steer 消费都归它。 */
    const events = new TaskEvents({
      target,
      taskId,
      control: runtime,
      stopping: () => this.stopping,
      parseAssistantMentions: true,
      outbox: this.outbox,
      progress,
      persistResearchReport: report => this.persistResearchReport(target, report),
      steerConsumed: (event) => this.noteSteerConsumed(runtime, event),
      discardStopIntent: () => this.store.clearTaskStopIntent(taskId),
    });
    let contextInjection: ContextInjection | undefined;
    let startupPhase: TaskStartupPhase = "context";

    runtime.stopActivity = (abortProgress) => progress.stop(abortProgress);
    runtime.stopText = () => textDelivery.stop();
    runtime.rollProgressCard = () => progress.roll();

    try {
      await turnContext.load();
      if (runtime.cancelRequested) {
        this.store.markCancelled(taskId);
        return;
      }
      startupPhase = "members";
      const guideInDeveloper = resuming || isNativeRetryPrompt(task.text);
      let agentContext: AgentContext = {
        cliReady: this.cliIdentityReady,
        guideInDeveloper,
      };
      if (!guideInDeveloper && isGroupTarget(target)) {
        agentContext = {
          cliReady: this.cliIdentityReady,
          guideInDeveloper,
          memberSnapshot: await this.fetchMemberSnapshot(target, runtime.cancelController.signal),
        };
      }
      if (runtime.cancelRequested) {
        this.store.markCancelled(taskId);
        return;
      }
      runtime.agentContext = agentContext;
      // All lookups required by the model turn are complete; later failures belong to worker startup.
      startupPhase = "startup";
      const sessionDir = await ensureProjectSessionDir(sessionsRoot(this.options.config), task.cwd);
      const binding = bindingAtStart ?? this.store.getBinding(this.bot!.uid, target);
      // 续接只认原会话：绑定缺失、文件丢失或身份不一致都 fail-closed，不新建也不重绑。
      if (
        resuming
        && (
          bindingAtStart === null
          || bindingAtStart.sessionFile === null
          || bindingAtStart.sessionId !== persistedTask.sessionId
        )
      ) {
        throw new TaskStartupError("session-invalid", "恢复所需的会话绑定缺失或身份不匹配");
      }
      const chatModel = this.store.resolveChatModel(this.bot!.uid, target).model;
      const workerOptions: Omit<WorkerOptions, "onSpawn"> = {
        contextScope: turnContext.workerScope,
        cwd: task.cwd,
        sessionDir,
        ...(chatModel === null ? {} : { model: chatModel }),
        ...(goalTask === null ? {} : { goalContinuation: true }),
      };
      if (binding?.sessionFile) {
        const sessionFile = await existingSessionFile(binding.sessionFile);
        if (!sessionFile) throw new TaskStartupError("session-invalid", "绑定的 OMP 会话文件不存在");
        workerOptions.sessionFile = sessionFile;
      }
      runtime.lease = await openTrackedWorker({
        workerFactory: this.options.workerFactory,
        store: this.store,
        worker: workerOptions,
        ledger: {
          id: task.id,
          botUid: this.bot!.uid,
          sessionId: task.sessionId,
          sessionFile: binding?.sessionFile ?? join(sessionDir, `${task.sessionId}.jsonl`),
        },
        blockDirectory: (cwd) => this.blockedDirectories.add(cwd),
      });
      const worker = runtime.lease.worker;
      if (runtime.cancelRequested || this.stopping) {
        await runtime.lease.close();
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
      if (goalTask && bindingAtStart?.sessionFile === null) {
        if (!verified.sessionFile) {
          throw new WorkerGoalError("invalid-response", "Fresh goal session did not expose its exact session file");
        }
        this.store.bindGoalTaskSession(taskId, goalSessionId, verified.sessionId, verified.sessionFile);
        goalSessionId = verified.sessionId;
      } else if (goalTask && verified.sessionId !== goalSessionId) {
        throw new WorkerGoalError("identity-mismatch", "Worker session changed before goal dispatch");
      } else if (!goalTask) {
        this.store.updateBindingSession(
          this.bot!.uid,
          target,
          verified.sessionId,
          verified.sessionFile,
        );
      }
      // 续接不重放原文、也不重灌历史快照：原回合的上下文与会话消息都在会话里；群/子区说明
      // 的注入事务照常发布——会话侧车正是为「原生续跑回合」准备的（`context` 钩子补灌），
      // 上一进程未结算的 pending 事务在这里被接手，成功的续跑回合照常提交。
      contextInjection = await turnContext.prepare(verified.sessionId, agentContext);
      const historyTurn = goalTask || resuming ? null : await this.history.prepareTurn(task, target, verified.sessionId);
      const promptContext: PromptContext = {
        tools: createHostTools(this.hostToolHost(), task),
        ...(contextInjection === undefined ? {} : { contextInjection }),
      };
      const resumeStreamPrefix = resuming ? `resume:${randomUUID()}:` : "";
      const pendingGoalUpdates: WorkerGoal[] = [];
      const latestGoalProgress: { value: WorkerGoal | null } = { value: null };
      const persistGoalProgress = (goal: WorkerGoal): void => {
        this.store.updateGoalTaskProgress(taskId, goal);
        latestGoalProgress.value = goal;
      };
      const consumeEvent = (event: WorkerEvent): void => {
        if (event.type === "prompt_accepted") {
          if (goalTask) this.store.markGoalTaskAgentAccepted(taskId);
          else this.store.markTaskPromptAccepted(taskId);
          return;
        }
        if (event.type === "goal_accepted") {
          if (!goalTask) throw new WorkerGoalError("invalid-response", "goal_accepted arrived for an ordinary task");
          const current = this.store.getGoalTask(taskId);
          if (current?.dispatchOp === "recover") {
            if (current.acceptedGoalId !== event.goalId) {
              throw new WorkerGoalError("identity-mismatch", "recovery acceptance did not match the persisted native goal id");
            }
          } else {
            this.store.markGoalTaskAccepted(taskId, event.goalId);
          }
          for (const goal of pendingGoalUpdates.splice(0)) persistGoalProgress(goal);
          return;
        }
        if (event.type === "goal_updated") {
          if (!goalTask) throw new WorkerGoalError("invalid-response", "goal_updated arrived for an ordinary task");
          if (event.goal === null) return;
          const current = this.store.getGoalTask(taskId);
          if (current?.accepted && current.acceptedGoalId !== null) {
            persistGoalProgress(event.goal);
          } else {
            pendingGoalUpdates.push(event.goal);
          }
          return;
        }
        events.onEvent(resumeStreamPrefix && (event.type === "text" || event.type === "notice")
          ? { ...event, messageId: `${resumeStreamPrefix}${event.messageId}` }
          : event);
      };
      let goalResult: WorkerGoalResult | null = null;
      let settledGoal: WorkerGoal | null = null;
      let goalAbsent = false;
      try {
        if (goalTask) {
          const latest = this.store.getGoalTask(taskId);
          if (!latest) throw new WorkerGoalError("invalid-response", "persisted goal request disappeared");
          let request: WorkerGoalRun;
          if (goalResuming) {
            if (!latest.accepted || latest.acceptedGoalId === null) {
              throw new WorkerGoalError("invalid-response", "no accepted goal receipt for recovery");
            }
            request = { op: "recover", goalId: latest.acceptedGoalId };
          } else {
            request = latest.request;
          }
          this.store.markGoalTaskDispatched(taskId, verified.sessionId, request.op);
          runtime.promptInFlight = true;
          goalResult = await worker.runGoal(request, consumeEvent, promptContext);
          const accepted = this.store.getGoalTask(taskId);
          if (!accepted?.accepted || !accepted.acceptedGoalId) {
            throw new WorkerGoalError("invalid-response", "native goal dispatch returned without a durable goal acceptance");
          }
          const expectedGoalId = request.op === "recover" ? request.goalId : accepted.acceptedGoalId;
          if (accepted.acceptedGoalId !== expectedGoalId) {
            throw new WorkerGoalError("identity-mismatch", "recovery goal id did not match its persisted acceptance");
          }
          if (goalResult.goal) {
            if (goalResult.goal.id !== expectedGoalId) {
              throw new WorkerGoalError("identity-mismatch", "native goal result id did not match accepted goal id");
            }
            this.store.finishGoalTaskProgress(taskId, goalResult.goal);
            settledGoal = goalResult.goal;
            if (settledGoal.status === "complete" || settledGoal.status === "dropped") {
              this.store.clearGoalSession(this.bot!.uid, target, goalSessionId);
              goalAbsent = true;
            }
          } else if (
            request.op === "recover"
            || latestGoalProgress.value?.status === "complete"
            || latestGoalProgress.value?.status === "dropped"
          ) {
            if (latestGoalProgress.value) this.store.finishGoalTaskProgress(taskId, latestGoalProgress.value);
            this.store.clearGoalSession(this.bot!.uid, target, goalSessionId);
            settledGoal = latestGoalProgress.value;
            goalAbsent = true;
          } else {
            throw new WorkerGoalError("invalid-response", "native goal start returned no goal and no terminal progress");
          }
        } else if (resuming) {
          runtime.promptInFlight = true;
          await worker.resume(consumeEvent, promptContext);
        } else {
          this.store.markTaskPromptStarted(taskId, verified.sessionId);
          runtime.promptInFlight = true;
          await worker.prompt(historyTurn?.text ?? task.text, consumeEvent, promptContext);
        }
      } finally {
        runtime.promptInFlight = false;
        if (historyTurn) this.history.commitTurn(target, verified.sessionId, historyTurn);
      }
      if (runtime.cancelRequested) {
        this.store.markCancelled(taskId);
        progress.finish("cancelled");
        await this.sendCancelledIfNeeded(runtime);
      } else if (runtime.stopCause !== null && !runtime.workAfterStop) {
        const questionTimeout = runtime.stopCause === "question";
        this.store.markInterrupted(
          taskId,
          questionTimeout ? QUESTION_TIMEOUT_REASON_TEXT : INTERRUPT_REASON_TEXT,
        );
        // 显式 /interrupt 到此确定不再续接：先丢掉该任务的恢复快照与遗留提问，否则快照里
        // 「已答复但还没被认领」的行会在随后的 rejectForTask 里被补投成一个新回合。
        // 提问超时不清快照：那一轮停下后迟到的答复要继续这次工作，唤醒正是它该走的路。
        if (!questionTimeout) this.questions.cancelForTask(taskId, "interrupted");
        progress.finish("cancelled");
      } else if (!runtime.suspended) {
        await runtime.steerSerial;
        if (goalTask) {
          let goal = settledGoal;
          let pauseFailure: unknown = null;
          const priorStop = this.store.getGoalSession(this.bot!.uid, target, goalSessionId)?.stopIntent;
          if (goal?.status === "active" && !goalAbsent) {
            const cancelled = this.store.stopGoalContinuation(this.bot!.uid, target, goalSessionId, "pause");
            await this.cleanupCancelledGoalTasks(target, cancelled);
            if (priorStop === "drop") {
              pauseFailure = new WorkerGoalError("native-error", "原生目标仍为 active，drop 未确认；自动续跑授权已撤销。");
            } else {
              let paused: WorkerGoalResult | null = null;
              try {
                paused = await worker.goal("pause");
              } catch (error) {
                pauseFailure = error;
              }
              if (paused?.goal) {
                if (paused.goal.id !== goal.id) {
                  throw new WorkerGoalError("identity-mismatch", "native goal pause returned a different goal id");
                }
                goal = paused.goal;
              } else if (paused && !paused.goal) {
                goal = null;
                goalAbsent = true;
                this.store.clearGoalSession(this.bot!.uid, target, goalSessionId);
              }
              if (goal?.status === "active" && pauseFailure === null) {
                pauseFailure = new WorkerGoalError("native-error", "原生目标仍为 active，pause 未确认；自动续跑授权已撤销。");
              }
            }
          }
          if (goal && !goalAbsent) this.store.saveGoalSessionProgress(this.bot!.uid, target, goalSessionId, goal);
          await turnContext.complete(worker.sessionId, events.localOnly);
          this.store.markCompleted(taskId, events.resultText);
          progress.finish("completed");
          const current = this.store.getGoalSession(this.bot!.uid, target, goalSessionId);
          const settled = goalAbsent
            ? goal
              ? `原生目标 ${goal.id} 已结算，状态 ${goal.status}。`
              : "原生目标已完成或已不存在。"
            : `原生目标执行已结算；${formatGoalSession(current)}`;
          const suffix = pauseFailure === null
            ? ""
            : ` 原生${priorStop === "drop" ? "drop" : "pause"}未确认：${goalErrorText(pauseFailure) ?? "请使用 /goal show 核对原生状态"}。`;
          await this.outbox.enqueueText(target, `${settled}${suffix}`, taskId);
          this.outbox.flush(target);
        } else {
          await turnContext.complete(worker.sessionId, events.localOnly);
          this.store.markCompleted(taskId, events.resultText);
          progress.finish("completed");
          this.outbox.flush(target);
        }
      }
    } catch (error) {
      if (runtime.cancelRequested) {
        this.store.markCancelled(taskId);
        progress.finish("cancelled");
        await this.sendCancelledIfNeeded(runtime);
      } else if (runtime.stopCause !== null && !runtime.workAfterStop) {
        // 显式 /interrupt 或 ask 超时已经把这个回合停在这里：即使紧接着进程停机，也按终态结算，
        // 不让一次重启把用户的中断续上（与成功分支同一判据）。停下后又有真实工作的回合照常
        // 落到下面的停机挂起，由新进程续跑。
        const questionTimeout = runtime.stopCause === "question";
        this.store.markInterrupted(
          taskId,
          questionTimeout ? QUESTION_TIMEOUT_REASON_TEXT : INTERRUPT_REASON_TEXT,
        );
        // 与成功分支同一判据：显式 /interrupt 的终态不保留恢复快照（提问超时相反）。
        if (!questionTimeout) this.questions.cancelForTask(taskId, "interrupted");
        progress.finish("cancelled");
      } else if (runtime.suspended || this.stopping) {
        // Stop aborts startup I/O before its per-runtime suspend loop; `stopping` closes that race.
        console.error(`[task] 停机挂起，任务保持未结算 task=${taskId}`);
      } else if (goalTask) {
        const cancelled = this.store.stopGoalContinuation(this.bot!.uid, target, goalSessionId, "pause");
        await this.cleanupCancelledGoalTasks(target, cancelled);
        const notice = goalErrorText(error)
          ?? "原生目标执行未能可靠完成；自动续跑授权已撤销，请使用 /goal show 核对状态。";
        this.store.markFailed(taskId, notice);
        progress.finish("failed");
        if (resuming) this.questions.cancelForTask(taskId, "ended");
        await this.outbox.enqueueText(target, notice, taskId);
      } else {
        const failure = runtime.lease
          ? classifyTaskFailure(error)
          : classifyStartupFailure(error, startupPhase);
        console.error(
          `[task] ${runtime.lease ? "执行失败" : "启动失败"} task=${taskId} target=${targetKey(runtime.target)}`
          + ` kind=${failure.kind} code=${failure.code} phase=${startupPhase} detail=${failure.detail}`,
        );
        this.store.markFailed(taskId, failure.row);
        progress.finish("failed");
        if (resuming) this.questions.cancelForTask(taskId, "ended");
        await this.outbox.enqueueText(runtime.target, failure.notice, taskId);
      }
    } finally {
      progress.stop(!progress.finished);
      this.releaseProgressTail(target, taskId, progress.tail);
      this.typingLeases.release(runtime.target, `task:${taskId}`);
      this.questions.rejectForTask(taskId, new Error("task finished"));
      if (runtime.lease) await runtime.lease.close();
      this.outbox.releaseIfIdle(taskId);
      // Deregister before a creator-chat notice so a shared directory does not queue behind this run.
      this.runtimes.delete(taskId);
      await this.notifyThreadOutcome(taskId);
      await this.threadArchives.run(target, progress.tail);
      this.serviceRestart.maybeRunDeferred();
    }
  }

  private async sendCancelledIfNeeded(runtime: RuntimeTask): Promise<void> {
    if (runtime.cancelReplySent) return;
    runtime.cancelReplySent = true;
    await this.outbox.enqueueText(runtime.target, DEFAULT_CANCELLED_TEXT, runtime.taskId);
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