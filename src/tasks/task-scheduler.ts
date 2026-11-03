/**
 * 任务调度与会话/目录租约的唯一所有者：每个聊天的任务串行链、已排定任务、关停要等的在途
 * 任务、排队等待任务的启动回执，以及按 session / chat key 与按 cwd 的租约链都在这里。外部只能经
 * `scheduleTask`/`hasBlockingWork`/`reserveCommandSession`/`tryAcquireCommandSession`/
 * `tryAcquireCommandDirectories`/`tryAcquireWorkspaceDirectories`/`markWaiting`/`takeWaitingReceipt`/
 * `settle` 观察，本对象的 map 不外露。
 *
 * 模块不依赖 `service.ts`：任务行与绑定行的读取、绑定核对、真正的执行入口（`runTask`）、
 * bot 身份与停止态、运行中任务与 blocked 集合、回执与终态通知的投递路径都按原样注入。会话
 * 租约、工作区目录租约与 blocked 集合仍然分别只有一个所有者，调度侧与绑定探测侧因此保持
 * 同一安全语义。
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
 * 按目录的租约：工作区命令在命令体内占据一个或多个 cwd，`ready` 在它们都空闲后兑现，
 * `complete` 恰好释放一次。租约本体（按 cwd 的 Promise 链）由 `TaskScheduler` 持有，租约不
 * 在两个模块里各存一份。
 */
export type DirectoryLease = {
  ready: Promise<void>;
  complete(): void;
};

/** A FIFO intent barrier that blocks later writes without acquiring any lease while earlier work drains. */
export interface CommandSessionReservation {
  tryAcquire(): SessionLease | undefined;
  release(): void;
}

interface CommandReservationRecord {
  readonly order: number;
  readonly target: ChatTarget;
  readonly sessionKey: string;
  readonly sessionKeys: readonly string[];
  readonly directories: readonly string[];
  readonly ready: Promise<void>;
  release(): void;
}

/**
 * 调度访问持久层任务与绑定行、bot 身份、停止态、进程内的运行中任务与 blocked 集合、绑定核对、
 * 执行入口，以及启动回执与终态通知投递路径。
 */
export interface TaskSchedulerDeps {
  readonly store: Pick<
    ChannelStore,
    | "getTask"
    | "listQueuedTasks"
    | "getBinding"
    | "markFailed"
    | "isWorkspaceDirectoryOwned"
    | "workspaceDirectories"
  >;
  /** 绑定行与任务行按键的 bot 身份。 */
  readonly botUid: () => string | undefined;
  /** channel 是否正在停止：停止后不再启动排定任务。 */
  readonly stopping: () => boolean;
  /** 该 cwd 是否有进程内运行任务——`CoreService` 遍历自己的 runtimes。 */
  readonly hasRuntimeInDirectory: (cwd: string) => boolean;
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
  /** 启动一个已排定任务；`cwd` 是启动前核对用的绑定目录；工作区资源已由调度租下。 */
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
  /** 已排定任务操作：同 session 或同工作区资源的较早任务据此等待。 */
  private readonly scheduledTasks = new Map<string, Promise<void>>();
  /** 关停要等的在途任务（已排定任务的操作）。 */
  private readonly taskOperations = new Set<Promise<void>>();
  /**
   * Tasks accepted while their chat, session, or workspace was already busy. They get one start
   * receipt when they finally begin, so a message that waited is distinguishable from one that
   * started right away. In-memory only: a restart interrupts such tasks instead of starting them.
   */
  private readonly waiting = new Set<string>();
  /** Tasks and session commands share per-session and per-chat lease tails. */
  private readonly sessionTails = new Map<string, Promise<void>>();
  /** 按 cwd 的租约链：`/new`、`/move` 与任务启动共用的工作区目录占用。 */
  private readonly directoryTails = new Map<string, Promise<void>>();
  /** The expanded resource keys owned by each currently executing task, for safe host-tool reuse. */
  private readonly taskDirectoryLeases = new Map<string, ReadonlySet<string>>();
  /** Scheduling order lets command barriers distinguish earlier tasks from later chatTails. */
  private readonly scheduledOrder = new Map<string, number>();
  private readonly commandReservations = new Set<CommandReservationRecord>();
  private nextOrder = 0;

  /**
   * True when a new task on this chat cannot start at once: the chat or its bound session is
   * busy, or an owned workspace resource for this cwd is busy. Ordinary sessions in the same
   * cwd remain independent, so a plain cwd does not make this true on its own.
   */
  hasBlockingWork(target: ChatTarget, cwd: string): boolean {
    const binding = this.bindingFor(target);
    const sessionKey = this.sessionKey(target, binding);
    const directories = this.ownedDirectoryKeys(
      [cwd],
      binding?.cwdPinned === true ? binding.cwd : undefined,
    );
    if (
      this.hasConflictingReservation(target, sessionKey, directories)
      || this.hasLeaseFor(target, sessionKey)
      || this.hasQueuedWork(target, sessionKey)
    ) {
      return true;
    }
    return directories.length > 0 && this.directoriesUnavailable(directories);
  }
  /** A command reservation is a FIFO barrier even when an active task could normally accept steering. */
  hasPendingCommandReservation(target: ChatTarget, cwd: string): boolean {
    const binding = this.bindingFor(target);
    const sessionKey = this.sessionKey(target, binding);
    const directories = this.ownedDirectoryKeys(
      [cwd],
      binding?.cwdPinned === true ? binding.cwd : undefined,
    );
    return this.hasConflictingReservation(target, sessionKey, directories);
  }

  constructor(private readonly deps: TaskSchedulerDeps) {}

  /** 排定一个任务：接在该聊天的任务链之后执行，并进入关停要等的在途集合。 */
  scheduleTask(taskId: string): void {
    const task = this.deps.store.getTask(taskId);
    if (!task) return;
    const order = ++this.nextOrder;
    this.scheduledOrder.set(taskId, order);
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
      if (this.scheduledOrder.get(taskId) === order) this.scheduledOrder.delete(taskId);
      this.taskOperations.delete(next);
    };
    void next.then(cleanup, cleanup);
  }

  /** 记下「这条任务在忙聊天、忙 session 或忙工作区后面等过」：启动时取回，据此补一条启动回执。 */
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
   * A command holds the current session and chat keys, while cwd is consulted for an unverified
   * worker left by recovery. When the command touches an owned workspace (or a pinned workspace
   * binding), its expanded resource keys are leased too so sibling projects cannot overlap.
   * Different sessions in the same ordinary cwd remain independent.
   */
  tryAcquireCommandSession(
    target: ChatTarget,
    cwds: readonly string[],
  ): SessionLease | undefined {
    const binding = this.bindingFor(target);
    const sessionKey = this.sessionKey(target, binding);
    const directories = this.ownedDirectoryKeys(
      cwds,
      binding?.cwdPinned === true ? binding.cwd : undefined,
    );
    if (
      this.hasConflictingReservation(target, sessionKey, directories)
      || this.hasLeaseFor(target, sessionKey)
      || this.hasQueuedWork(target, sessionKey)
      || cwds.some((cwd) => this.deps.isDirectoryBlocked(cwd))
    ) {
      return undefined;
    }
    if (directories.length > 0 && this.directoriesUnavailable(directories)) return undefined;
    return this.createLeases(target, sessionKey, directories);
  }

  /**
   * Install a FIFO barrier before waiting for accepted work. Later tasks wait on this intent;
   * the owner itself acquires its real session/directory leases only after earlier work drains.
   */
  reserveCommandSession(target: ChatTarget, cwds: readonly string[]): CommandSessionReservation {
    const binding = this.bindingFor(target);
    const sessionKey = this.sessionKey(target, binding);
    const directories = this.ownedDirectoryKeys(
      cwds,
      binding?.cwdPinned === true ? binding.cwd : undefined,
    );
    const completion = Promise.withResolvers<void>();
    let reservation!: CommandReservationRecord;
    reservation = {
      order: ++this.nextOrder,
      target: { ...target },
      sessionKey,
      sessionKeys: this.leaseKeys(target, sessionKey),
      directories,
      ready: completion.promise,
      release: () => {
        if (!this.commandReservations.delete(reservation)) return;
        completion.resolve();
      },
    };
    this.commandReservations.add(reservation);
    return {
      tryAcquire: () => this.tryAcquireReservedCommandSession(reservation),
      release: () => reservation.release(),
    };
  }

  /** Lease every owned resource related to these paths, not just the caller's cwd spelling. */
  tryAcquireCommandDirectories(cwds: readonly string[]): DirectoryLease | undefined {
    const directories = this.workspaceKeys(cwds);
    if (
      this.commandReservations.size > 0
      && [...this.commandReservations].some((reservation) =>
        reservation.directories.some((directory) => directories.includes(directory))
      )
    ) {
      return undefined;
    }
    if (this.directoriesUnavailable(directories)) return undefined;
    const leases = directories.map((cwd) => this.createDirectoryLease(cwd));
    return this.combineLeases(leases);
  }

  /**
   * A host operation may reuse the running task's exclusive resource lease only when every
   * requested key is already covered. A wider request goes through the ordinary busy check.
   */
  tryAcquireWorkspaceDirectories(cwds: readonly string[], task: TaskRecord): DirectoryLease | undefined {
    const requested = this.workspaceKeys(cwds);
    const held = this.taskDirectoryLeases.get(task.id);
    if (held && requested.every((directory) => held.has(directory))) {
      return { ready: Promise.resolve(), complete: () => {} };
    }
    return this.tryAcquireCommandDirectories(cwds);
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

  private reservationConflicts(
    reservation: CommandReservationRecord,
    target: ChatTarget,
    sessionKey: string,
    directories: readonly string[],
  ): boolean {
    const sessionKeys = new Set(this.leaseKeys(target, sessionKey));
    return reservation.sessionKeys.some((key) => sessionKeys.has(key))
      || reservation.directories.some((directory) => directories.includes(directory));
  }

  private hasConflictingReservation(
    target: ChatTarget,
    sessionKey: string,
    directories: readonly string[],
    beforeOrder = Number.POSITIVE_INFINITY,
    excluded?: CommandReservationRecord,
  ): boolean {
    return [...this.commandReservations].some((reservation) =>
      reservation !== excluded
      && reservation.order < beforeOrder
      && this.reservationConflicts(reservation, target, sessionKey, directories)
    );
  }

  private firstBlockingReservation(
    target: ChatTarget,
    sessionKey: string,
    directories: readonly string[],
    taskOrder: number,
  ): CommandReservationRecord | undefined {
    return [...this.commandReservations]
      .filter((reservation) =>
        reservation.order < taskOrder
        && this.reservationConflicts(reservation, target, sessionKey, directories)
      )
      .sort((left, right) => left.order - right.order)[0];
  }
  private async waitForPriorReservations(
    target: ChatTarget,
    sessionKey: string,
    directories: readonly string[],
    taskOrder: number,
  ): Promise<void> {
    while (!this.deps.stopping()) {
      const reservation = this.firstBlockingReservation(target, sessionKey, directories, taskOrder);
      if (!reservation) return;
      await reservation.ready;
    }
  }

  private tryAcquireReservedCommandSession(
    reservation: CommandReservationRecord,
  ): SessionLease | undefined {
    if (!this.commandReservations.has(reservation)) return undefined;
    const binding = this.bindingFor(reservation.target);
    const sessionKey = this.sessionKey(reservation.target, binding);
    const directories = reservation.directories;
    if (
      sessionKey !== reservation.sessionKey
      || this.hasConflictingReservation(
        reservation.target,
        sessionKey,
        directories,
        reservation.order,
        reservation,
      )
      || this.leaseKeys(reservation.target, sessionKey).some((key) => this.sessionTails.has(key))
      || this.hasQueuedWork(reservation.target, sessionKey, reservation.order)
      || directories.some((cwd) => this.deps.isDirectoryBlocked(cwd))
      || this.directoriesUnavailable(directories, reservation.order)
    ) {
      return undefined;
    }
    return this.createLeases(reservation.target, sessionKey, directories);
  }

  private hasLeaseFor(target: ChatTarget, sessionKey: string): boolean {
    if (this.chatTails.has(targetKey(target))) return true;
    return this.leaseKeys(target, sessionKey).some((key) => this.sessionTails.has(key));
  }

  private hasQueuedWork(target: ChatTarget, sessionKey: string, beforeOrder?: number): boolean {
    const chatKey = targetKey(target);
    return this.deps.store.listQueuedTasks().some((task) => {
      const order = this.scheduledOrder.get(task.id) ?? -1;
      if (beforeOrder !== undefined && order >= beforeOrder) return false;
      const queuedTarget = taskTarget(task);
      return targetKey(queuedTarget) === chatKey
        || this.sessionKey(queuedTarget, this.bindingFor(queuedTarget)) === sessionKey;
    });
  }

  /**
   * The resource keys a task or command must lease: only owned workspace resources (or the
   * caller's pinned workspace cwd), never an ordinary cwd. This keeps main's per-session lease
   * rule intact: different sessions in the same ordinary cwd still run concurrently.
   */
  private ownedDirectoryKeys(cwds: readonly string[], pinnedCwd?: string): string[] {
    const directories = new Set<string>();
    for (const cwd of cwds) {
      if (cwd !== pinnedCwd && !this.deps.store.isWorkspaceDirectoryOwned(cwd)) continue;
      for (const directory of this.workspaceKeys([cwd])) directories.add(directory);
    }
    return [...directories].sort();
  }

  private directoriesUnavailable(directories: readonly string[], beforeOrder?: number): boolean {
    const requested = new Set(directories);
    if (directories.some((cwd) =>
      this.deps.isDirectoryBlocked(cwd)
      || this.directoryTails.has(cwd)
      || this.deps.hasRuntimeInDirectory(cwd)
    )) {
      return true;
    }
    return this.deps.store.listQueuedTasks().some((task) => {
      const order = this.scheduledOrder.get(task.id) ?? -1;
      if (beforeOrder !== undefined && order >= beforeOrder) return false;
      const target = taskTarget(task);
      const cwd = this.bindingFor(target)?.cwd ?? task.cwd;
      return this.workspaceKeys([cwd]).some((directory) => requested.has(directory));
    });
  }

  private workspaceKeys(cwds: readonly string[]): string[] {
    const directories = new Set<string>();
    for (const cwd of cwds) {
      for (const directory of this.deps.store.workspaceDirectories(cwd)) directories.add(directory);
    }
    return [...directories].sort();
  }

  private combineLeases(leases: readonly DirectoryLease[]): DirectoryLease {
    return {
      ready: Promise.all(leases.map((lease) => lease.ready)).then(() => {}),
      complete: () => {
        for (const lease of leases) lease.complete();
      },
    };
  }

  /**
   * Create both leases synchronously, before anything awaits, so a command and a scheduled task
   * cannot interleave between registering their session and directory tails.
   */
  private createLeases(
    target: ChatTarget,
    sessionKey: string,
    directories: readonly string[],
  ): SessionLease {
    const sessionLease = this.createSessionLease(target, sessionKey);
    if (directories.length === 0) return sessionLease;
    return this.combineLeases([
      sessionLease,
      this.combineLeases(directories.map((cwd) => this.createDirectoryLease(cwd))),
    ]);
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

  /**
   * Earlier queued tasks must finish before the current task starts when they share its session
   * or any of its leased workspace resource keys.
   */
  private async earlierOperations(
    task: TaskRecord,
    sessionKey: string,
    directories: ReadonlySet<string>,
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
      const earlierCwd = binding?.cwd ?? earlier.cwd;
      const sharesSession = this.sessionKey(target, binding) === sessionKey;
      const sharesDirectory = directories.size > 0
        && this.workspaceKeys([earlierCwd]).some((directory) => directories.has(directory));
      if (!sharesSession && !sharesDirectory) continue;
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
        const directories = this.ownedDirectoryKeys(
          [cwd],
          binding?.cwdPinned === true ? binding.cwd : undefined,
        );
        const taskOrder = this.scheduledOrder.get(taskId) ?? Number.POSITIVE_INFINITY;
        await this.waitForPriorReservations(target, sessionKey, directories, taskOrder);
        if (this.deps.stopping()) return;
        const earlier = await this.earlierOperations(
          reconciledCurrent,
          sessionKey,
          new Set(directories),
        );
        await Promise.all(earlier.map((operation) => operation.catch(() => {})));
        await this.waitForPriorReservations(target, sessionKey, directories, taskOrder);
        if (this.deps.stopping()) return;

        const beforeLease = this.deps.store.getTask(taskId);
        if (this.deps.stopping() || !beforeLease || beforeLease.state !== "queued") return;
        const beforeTarget = taskTarget(beforeLease);
        lease = this.createLeases(beforeTarget, sessionKey, directories);
        await lease.ready;

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
        const newlyEarlier = await this.earlierOperations(
          latest,
          currentSessionKey,
          new Set(directories),
        );
        if (newlyEarlier.length > 0) {
          lease.complete();
          lease = undefined;
          await Promise.all(newlyEarlier.map((operation) => operation.catch(() => {})));
          continue;
        }
        const taskDirectories = new Set(directories);
        if (taskDirectories.size > 0) this.taskDirectoryLeases.set(taskId, taskDirectories);
        try {
          await this.deps.runTask(taskId, cwd);
        } finally {
          if (this.taskDirectoryLeases.get(taskId) === taskDirectories) {
            this.taskDirectoryLeases.delete(taskId);
          }
        }
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
