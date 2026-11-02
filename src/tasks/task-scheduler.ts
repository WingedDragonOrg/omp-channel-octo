/**
 * 任务调度与会话租约的唯一所有者：每聊天的任务串行链、已排定任务、关停要等的在途任务、
 * 排队等待任务的启动回执，以及按 session / chat key 持有的租约链都在这里。外部只能经
 * `scheduleTask`/`hasBlockingWork`/`tryAcquireCommandSession`/`markWaiting`/
 * `takeWaitingReceipt`/`settle` 观察，本对象的 map 不外露。
 *
 * 模块不依赖 `service.ts`：任务行与绑定行的读取、绑定核对、真正的执行入口（`runTask`）、
 * bot 身份与停止态、blocked 集合、回执与终态通知的投递路径都按原样注入。会话租约与 blocked
 * 集合仍然分别只有一个所有者，调度侧与绑定探测侧因此保持同一安全语义。
 */
import type { ChatTarget } from "../contracts.ts";
import type { Outbox } from "../outbox.ts";
import type { ChannelStore } from "../store/index.ts";
import type { BindingRecord, TaskRecord } from "../store/store-records.ts";
import { GENERIC_TASK_FAILURE_NOTICE } from "./task-failure.ts";
import { targetKey, taskTarget } from "../target.ts";

/**
 * 按 session 与聊天键的租约：任务和 `/new`、`/move` 共用对应的会话锁，聊天键还防止命令期间
 * 绑定切换后同聊天的新任务越过命令。`ready` 在它们都空闲后兑现，`complete` 恰好释放一次。
 */
export type SessionLease = {
  ready: Promise<void>;
  complete(): void;
};

/**
 * 调度访问持久层任务与绑定行、bot 身份、停止态、目录 block 状态、绑定核对、执行入口，以及
 * 启动回执与终态通知投递路径。
 */
export interface TaskSchedulerDeps {
  readonly store: Pick<
    ChannelStore,
    | "getTask"
    | "listQueuedTasks"
    | "getBinding"
    | "markFailed"
  >;
  /** 绑定行与任务行按键的 bot 身份。 */
  readonly botUid: () => string | undefined;
  /** channel 是否正在停止：停止后不再启动排定任务。 */
  readonly stopping: () => boolean;
  /** 该 cwd 是否因 worker 无法确认停止而被 block。 */
  readonly isDirectoryBlocked: (cwd: string) => boolean;
  /**
   * 绑定行与磁盘会话文件的核对：任务启动前确认它的 cwd。恢复中的任务由服务侧实现只读核对
   * （绑定行不能被改写），任务身份因此必须传进来。
   */
  readonly reconcileBinding: (
    taskId: string,
    target: ChatTarget,
    binding: BindingRecord | null,
  ) => Promise<BindingRecord | null>;
  /** 启动一个已排定任务；`cwd` 是启动前核对用的绑定目录，并非互斥键。 */
  readonly runTask: (taskId: string, cwd?: string) => Promise<void>;
  /**
   * 启动门：恢复任务先占住调度序，但要等服务接线成功才真正执行。`start()` 失败时该 Promise
   * 拒绝，排定任务直接放弃，不留一个已起 worker 的残局。
   */
  readonly awaitStartReady: () => Promise<void>;
  /** 为该聊天排一条回执文本，与其他路径一致。 */
  readonly enqueueText: Outbox["enqueueText"];
  /** 任务终态的 relay 通知（子区回传）。 */
  readonly notifyThreadOutcome: (taskId: string) => Promise<void>;
}

export class TaskScheduler {
  /** 每个聊天的任务串行链：该聊天的下一个任务接在上一个（或已排定的任务）之后。 */
  private readonly chatTails = new Map<string, Promise<void>>();
  /** 已排定任务操作：同 session 的较早任务据此等待。 */
  private readonly scheduledTasks = new Map<string, Promise<void>>();
  /** 关停要等的在途任务（已排定任务的操作）。 */
  private readonly taskOperations = new Set<Promise<void>>();
  /**
   * Tasks accepted while their chat or session was already busy. They get one start receipt
   * when they finally begin, so a message that waited is distinguishable from one that started
   * right away. In-memory only: a restart interrupts such tasks instead of starting them.
   */
  private readonly waiting = new Set<string>();
  /** Tasks and session commands share per-session and per-chat lease tails. */
  private readonly sessionTails = new Map<string, Promise<void>>();
  /** True when this chat or its bound session already has queued or running work. */
  hasBlockingWork(target: ChatTarget): boolean {
    const sessionKey = this.sessionKey(target, this.bindingFor(target));
    return this.hasLeaseFor(target, sessionKey) || this.hasQueuedWork(target, sessionKey);
  }

  constructor(private readonly deps: TaskSchedulerDeps) {}

  /** 排定一个任务：接在该聊天的任务链之后执行，并进入关停要等的在途集合。 */
  scheduleTask(taskId: string): void {
    const task = this.deps.store.getTask(taskId);
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

  /** 记下「这条任务在忙聊天或忙 session 后面等过」：启动时取回，据此补一条启动回执。 */
  markWaiting(taskId: string): void {
    this.waiting.add(taskId);
  }

  /**
   * 取走等待标记：true 表示该任务确实等过，调用方据此补一条启动回执。取消或失败的路径同样
   * 经这里清除，不额外保留一个只做删除的出口。
   */
  takeWaitingReceipt(taskId: string): boolean {
    return this.waiting.delete(taskId);
  }

  /**
   * 关停：等在途的排定任务收尾。按调用瞬间的快照结算——此刻不在集合里的任务不属于本次关停。
   */
  settle(): Promise<unknown> {
    return Promise.allSettled([...this.taskOperations]);
  }

  /**
   * A command holds the current session and chat keys, while cwd is consulted only for an
   * unverified worker left by recovery. Different sessions in the same cwd remain independent.
   */
  tryAcquireCommandSession(
    target: ChatTarget,
    cwds: readonly string[],
  ): SessionLease | undefined {
    const sessionKey = this.sessionKey(target, this.bindingFor(target));
    if (
      this.hasLeaseFor(target, sessionKey)
      || this.hasQueuedWork(target, sessionKey)
      || cwds.some((cwd) => this.deps.isDirectoryBlocked(cwd))
    ) {
      return undefined;
    }
    return this.createSessionLease(target, sessionKey);
  }

  private bindingFor(target: ChatTarget): BindingRecord | null {
    const botUid = this.deps.botUid();
    return botUid === undefined ? null : this.deps.store.getBinding(botUid, target);
  }

  private sessionKey(target: ChatTarget, binding: BindingRecord | null): string {
    return binding?.sessionId
      ? `session:${binding.sessionId}`
      : `chat:${targetKey(target)}`;
  }

  private leaseKeys(target: ChatTarget, sessionKey: string): string[] {
    return [...new Set([sessionKey, `chat:${targetKey(target)}`])];
  }

  private hasLeaseFor(target: ChatTarget, sessionKey: string): boolean {
    if (this.chatTails.has(targetKey(target))) return true;
    return this.leaseKeys(target, sessionKey).some((key) => this.sessionTails.has(key));
  }

  private hasQueuedWork(target: ChatTarget, sessionKey: string): boolean {
    const chatKey = targetKey(target);
    return this.deps.store.listQueuedTasks().some((task) => {
      const queuedTarget = taskTarget(task);
      return targetKey(queuedTarget) === chatKey
        || this.sessionKey(queuedTarget, this.bindingFor(queuedTarget)) === sessionKey;
    });
  }

  private createSessionLease(target: ChatTarget, sessionKey: string): SessionLease {
    const keys = this.leaseKeys(target, sessionKey);
    const previous = keys.flatMap((key) => {
      const tail = this.sessionTails.get(key);
      return tail === undefined ? [] : [tail];
    });
    const completion = Promise.withResolvers<void>();
    const lease = completion.promise;
    for (const key of keys) this.sessionTails.set(key, lease);
    let complete = false;
    return {
      ready: Promise.all(previous.map((tail) => tail.catch(() => {}))).then(() => {}),
      complete: () => {
        if (complete) return;
        complete = true;
        for (const key of keys) {
          if (this.sessionTails.get(key) === lease) this.sessionTails.delete(key);
        }
        completion.resolve();
      },
    };
  }

  private async acquireSessionLease(
    target: ChatTarget,
    sessionKey: string,
  ): Promise<SessionLease> {
    const lease = this.createSessionLease(target, sessionKey);
    await lease.ready;
    return lease;
  }

  /** Earlier queued tasks bound to this session must finish before the current task starts. */
  private async earlierSessionOperations(
    task: TaskRecord,
    sessionKey: string,
  ): Promise<Promise<void>[]> {
    const queued = this.deps.store.listQueuedTasks();
    const currentIndex = queued.findIndex((candidate) => candidate.id === task.id);
    if (currentIndex < 0) return [];
    const operations: Promise<void>[] = [];
    for (let index = 0; index < currentIndex; index += 1) {
      const earlier = queued[index];
      if (!earlier) continue;
      const target = taskTarget(earlier);
      const binding = await this.deps.reconcileBinding(
        earlier.id,
        target,
        this.bindingFor(target),
      );
      if (this.sessionKey(target, binding) !== sessionKey) continue;
      const operation = this.scheduledTasks.get(earlier.id);
      if (operation) operations.push(operation);
    }
    return operations;
  }

  private async runScheduledTask(taskId: string): Promise<void> {
    try {
      await this.deps.awaitStartReady();
    } catch {
      // 启动失败：这条排定任务不执行，行仍以 queued 留给下一次进程。
      return;
    }
    let lease: SessionLease | undefined;
    try {
      for (let attempt = 0; attempt < 4; attempt += 1) {
        const current = this.deps.store.getTask(taskId);
        if (this.deps.stopping() || !current || current.state !== "queued") return;
        const target = taskTarget(current);
        const binding = await this.deps.reconcileBinding(
          taskId,
          target,
          this.bindingFor(target),
        );
        const reconciledCurrent = this.deps.store.getTask(taskId);
        if (this.deps.stopping() || !reconciledCurrent || reconciledCurrent.state !== "queued") return;
        const cwd = binding?.cwd ?? reconciledCurrent.cwd;
        const sessionKey = this.sessionKey(target, binding);
        const earlier = await this.earlierSessionOperations(reconciledCurrent, sessionKey);
        await Promise.all(earlier.map((operation) => operation.catch(() => {})));

        const beforeLease = this.deps.store.getTask(taskId);
        if (this.deps.stopping() || !beforeLease || beforeLease.state !== "queued") return;
        const beforeTarget = taskTarget(beforeLease);
        lease = await this.acquireSessionLease(beforeTarget, sessionKey);

        const refreshed = await this.deps.reconcileBinding(
          taskId,
          beforeTarget,
          this.bindingFor(beforeTarget),
        );
        const latest = this.deps.store.getTask(taskId);
        if (this.deps.stopping() || !latest || latest.state !== "queued") return;
        const currentCwd = refreshed?.cwd ?? latest.cwd;
        const currentSessionKey = this.sessionKey(beforeTarget, refreshed);
        if (currentCwd !== cwd || currentSessionKey !== sessionKey) {
          lease.complete();
          lease = undefined;
          continue;
        }
        const newlyEarlier = await this.earlierSessionOperations(latest, currentSessionKey);
        if (newlyEarlier.length > 0) {
          lease.complete();
          lease = undefined;
          await Promise.all(newlyEarlier.map((operation) => operation.catch(() => {})));
          continue;
        }
        await this.deps.runTask(taskId, cwd);
        return;
      }
      const task = this.deps.store.getTask(taskId);
      if (!task || task.state !== "queued") return;
      this.takeWaitingReceipt(taskId);
      this.deps.store.markFailed(taskId, "绑定会话或目录在启动前持续变更，任务未启动。");
      await this.deps.enqueueText(taskTarget(task), GENERIC_TASK_FAILURE_NOTICE, taskId);
      await this.deps.notifyThreadOutcome(taskId);
    } finally {
      lease?.complete();
    }
  }
}
