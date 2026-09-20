import { GroupHistoryCoordinator } from "./history-coordinator.ts";
import { ModelSwitchController } from "./model-controller.ts";
import { ProgressCards } from "./progress-cards.ts";
import {
  isCommand,
  normalizeCommandHandlers,
  parseCommand,
  resolveBuiltinAction,
  type BuiltinAction,
  type BuiltinHandlers,
} from "./command-routing.ts";
import { SessionLifecycle } from "./session-lifecycle.ts";
import { TaskScheduler } from "./task-scheduler.ts";
import { randomUUID } from "node:crypto";
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
import { TurnContext } from "./turn-context.ts";
import { createBotEventLoop, type BotEventLoop } from "./bot-events.ts";
import { createOutboxDelivery, type Outbox } from "./outbox.ts";
import { createHostTools, type HostToolHost } from "./host-tools.ts";
import { InboundMaterializer } from "./inbound-materializer.ts";
import { InboundShutdownError, safeErrorMessage } from "./errors.ts";
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
import { ThreadArchiveScheduler } from "./thread-archive.ts";
import { ThreadCommands } from "./thread-commands.ts";
import { existingSessionFile } from "./workdir.ts";

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
/** Steered messages tracked for the consumption receipt; older entries fall out of observation. */
const MAX_PENDING_STEERS = 32;
/** Characters of the triggering message shown in a start/consumption receipt. */
const RECEIPT_EXCERPT_CHARS = 20;
const DEFAULT_CANCELLED_TEXT = "当前任务已取消；不回滚已发生的副作用。";
const DEFAULT_RESTARTING_TEXT = "channel 正在重启，本会话任务已中断；重启完成后会在本会话确认。";
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

function statusState(task: TaskRecord | null, hasBinding: boolean): ChatExecutionState {
  if (!task) return hasBinding ? "idle" : "none";
  return task.state;
}

function createSessionId(): string {
  return randomUUID();
}

class CoreService implements ChannelService {
  private readonly history: GroupHistoryCoordinator;
  private readonly models: ModelSwitchController;
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
  /** 内建命令的 canonical handler 表：识别（alias 表）与分派共用同一份 canonical action。 */
  private readonly builtinHandlers: BuiltinHandlers;
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
  /** `/thread` 家族命令的所有者：意图交接、合并交接与资格文案都不再留在服务里。 */
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
      hasRuntimeInDirectory: cwd => this.hasRuntimeInDirectory(cwd),
      isDirectoryBlocked: cwd => this.blockedDirectories.has(cwd),
      reconcileBinding: (target, binding) =>
        this.sessions.reconcileExternalBinding(target, binding),
      runTask: (taskId, cwd) => this.runTask(taskId, cwd),
      enqueueText: (target, text, taskId) => this.outbox.enqueueText(target, text, taskId),
      notifyThreadOutcome: taskId => this.notifyThreadOutcome(taskId),
    });
    this.sessions = new SessionLifecycle({
      store: this.store,
      workerFactory: options.workerFactory,
      config: options.config,
      bot: () => this.bot,
      hasRuntime: target => this.hasRuntime(target),
      isDirectoryBlocked: cwd => this.blockedDirectories.has(cwd),
      blockDirectory: cwd => { this.blockedDirectories.add(cwd); },
      tryAcquireCommandDirectories: cwds => this.scheduler.tryAcquireCommandDirectories(cwds),
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
    this.threadArchives = new ThreadArchiveScheduler({
      store: this.store,
      transport: this.options.transport,
      outbox: this.outbox,
      botUid: () => this.bot?.uid,
      stopping: () => this.stopping,
    });
    this.threadCommands = new ThreadCommands({
      store: this.store,
      botUid: () => this.bot?.uid,
      ...(options.worktrees === undefined ? {} : { worktrees: options.worktrees }),
      materialize: (message, text) => this.materializer.materialize(message, text),
      commandClaim: (message, name) => this.commandClaim(message, name),
      withMessageTyping: (message, work) => this.withMessageTyping(message, work),
      appendToRunningTask: (message, text) => this.appendToRunningTask(message, text),
      acceptTask: (message, text) => this.acceptTask(message, text),
      enqueueText: (target, text, taskId) => this.outbox.enqueueText(target, text, taskId),
    });
    // 内建命令的 canonical handler 表：每个 action 一个函数，直接返回原 handler 的 Promise
    // （不再包一层 async），`handleCommand` 里的 await 因此仍是原来的微任务边界；只有
    // `restart` 的返回值参与重启判定。
    this.builtinHandlers = {
      new: (message, argument) => this.sessions.handleNew(message, argument),
      move: (message, argument) => this.sessions.handleMove(message, argument),
      thread: (message, argument) => this.threadCommands.handle(message, argument),
      cancel: message => this.handleCancel(message),
      interrupt: message => this.handleInterrupt(message),
      queue: (message, argument) => this.handleQueue(message, argument),
      mode: (message, argument) => this.handleMode(message, argument),
      switch: (message, argument) => this.models.handleSwitch(message, argument),
      status: message => this.handleStatus(message),
      help: message => this.outbox.enqueueText(message, COMMAND_HELP, null),
      restart: (message, argument) => this.handleRestart(message, argument),
    };
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
    this.threadArchives.stop();
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
    await this.scheduler.settle();
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
    this.threadArchives.cancel(message);
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
      const action = resolveBuiltinAction(command.name);
      if (action !== undefined) {
        await this.handleCommand(message, command.name, command.argument, action);
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

  /** 该 cwd 是否有进程内运行任务：调度侧的目录判定遍历同一份 runtimes。 */
  private hasRuntimeInDirectory(cwd: string): boolean {
    for (const runtime of this.runtimes.values()) {
      if (runtime.cwd === cwd) return true;
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

  private async handleCommand(
    message: InboundMessage,
    name: string,
    argument: string,
    action: BuiltinAction,
  ): Promise<void> {
    // 认领仍按键原始命令名（别名在 claim 之后只影响走哪个 handler），认领失败即丢弃。
    const claim = this.commandClaim(message, name);
    if (!this.store.claimCommand(claim)) return;
    try {
      const restartRequested = await this.builtinHandlers[action](message, argument);
      this.store.completeCommand(claim);
      if (restartRequested === true) {
        this.restartRequested = true;
        await this.options.restart?.request();
      }
    } catch {
      this.store.failCommand(claim);
      throw new Error("命令未能持久完成，请重试。");
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
    for (const taskId of queued) this.scheduler.takeWaitingReceipt(taskId);
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
    const startsBehind = this.scheduler.hasBlockingWork(message, task.cwd);
    try {
      accepted = this.store.acceptTask(this.taskClaim(message), task);
    } catch {
      throw new Error("本地状态盘写入失败，任务未接收。");
    }
    if (!accepted) return false;
    // 持久接受即点亮 task 租约：排队等待（同聊天或跨聊天同目录）到终态都保持「正在输入」。
    this.typingLeases.acquire(message, `task:${task.id}`);
    if (startsBehind) this.scheduler.markWaiting(task.id);
    this.scheduler.scheduleTask(task.id);
    this.outbox.flush(message);
    return true;
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
      planThreadArchive: (target, groupId, shortId) =>
        this.threadArchives.schedule(target, groupId, shortId),
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
      this.scheduler.takeWaitingReceipt(taskId);
      this.store.markFailed(taskId, "绑定目录在启动前再次变更，任务未启动。");
      await this.outbox.enqueueText(taskTarget(task), GENERIC_TASK_FAILURE_NOTICE, taskId);
      await this.notifyThreadOutcome(taskId);
      return;
    }
    if (this.blockedDirectories.has(task.cwd)) {
      this.scheduler.takeWaitingReceipt(taskId);
      this.store.markFailed(taskId, "directory remains blocked by an unconfirmed worker");
      await this.outbox.enqueueText(taskTarget(task), GENERIC_TASK_FAILURE_NOTICE, taskId);
      await this.notifyThreadOutcome(taskId);
      return;
    }
    if (!this.store.markStarted(taskId)) return;
    // 防御性幂等点亮：正常路径在接受时已持有 task 租约（此处 no-op）；任何未来触达路径自动点亮。
    this.typingLeases.acquire(taskTarget(task), `task:${taskId}`);
    if (this.scheduler.takeWaitingReceipt(taskId)) {
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
    /** 本回合的群说明事务：快照、ledger 决策与本回合 sidecar 都归它，服务只按序调用。 */
    const turnContext = isGroupTarget(target) ? new TurnContext({
      store: this.store,
      transport: this.options.transport,
      stateDir: this.options.config.stateDir,
      botUid: this.bot!.uid,
      target,
    }) : null;
    const progress = this.progressCards.startTask(taskId, target);
    const assistantTexts = new Map<string, string>();
    let snapshot: GroupContext | null = null;
    let contextInjection: ContextInjection | undefined;
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
      snapshot = turnContext ? await turnContext.load() : null;
      if (runtime.cancelRequested) {
        this.store.markCancelled(taskId);
        return;
      }
      // Everything past the group-context read is "startup": a failure here is no longer a context
      // read problem, so the classification must not keep reporting one.
      startupPhase = "startup";
      const sessionDir = await ensureProjectSessionDir(sessionsRoot(this.options.config), task.cwd);
      const binding = bindingAtStart ?? this.store.getBinding(this.bot!.uid, target);
      const chatModel = this.store.getChatModel(this.bot!.uid, target);
      const contextScope = turnContext?.workerScope;
      const workerOptions: WorkerOptions = {
        ...(contextScope === undefined ? {} : { contextScope }),
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
      contextInjection = turnContext ? await turnContext.prepare(verified.sessionId) : undefined;
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
        if (turnContext) await turnContext.complete(worker.sessionId, localOnly);
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
      await this.threadArchives.run(target, progress.tail);
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