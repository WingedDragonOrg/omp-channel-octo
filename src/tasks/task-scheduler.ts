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
  readonly order: number;
  tryAcquire(): Promise<SessionLease | undefined>;
  release(): void;
}

interface CommandReservationRecord {
  readonly order: number;
  readonly target: ChatTarget;
  readonly sessionKey: string;
  readonly sessionKeys: readonly string[];
  readonly directories: readonly string[];
  readonly repositoryCwds: readonly string[];
  readonly ready: Promise<void>;
  release(): void;
}

/** Exclusive Git-worktree lease held for a native commit task. */
export type CommitLease = {
  readonly root: string;
  readonly taskId: string;
  readonly order: number;
  readonly ready: Promise<void>;
  complete(): void;
};

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
    | "isCommitTask"
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
  /** Canonical Git worktree root for a task cwd, or null when it is outside a repository. */
  readonly repositoryRoot: (cwd: string) => Promise<string | null>;
  /** A prior unconfirmed worker whose Git root matches this worktree blocks new writers. */
  readonly repositoryBlocked: (root: string) => Promise<boolean>;
  /**
   * A task that cannot pass its existing startup validation must reach `runTask` before Git-root
   * probing; this preserves its established failure classification without granting a writer lease.
   */
  readonly shouldRunTaskBeforeRepositoryProbe: (
    taskId: string,
    target: ChatTarget,
    binding: BindingRecord | null,
    cwd: string,
  ) => Promise<boolean>;
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
  /** Ordinary task roots remain shared; only a commit or host merge excludes all other writers. */
  private readonly activeRepositoryTasks = new Map<string, string>();
  private readonly exclusiveRepositoryRoots = new Map<string, { owner: string; done: Promise<void> }>();
  private repositoryLeaseSequence = 0;

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
  async hasPendingCommandReservation(target: ChatTarget, cwd: string): Promise<boolean> {
    const binding = this.bindingFor(target);
    const sessionKey = this.sessionKey(target, binding);
    const directories = this.ownedDirectoryKeys(
      [cwd],
      binding?.cwdPinned === true ? binding.cwd : undefined,
    );
    return await this.firstBlockingReservation(
      target,
      sessionKey,
      directories,
      Number.POSITIVE_INFINITY,
      cwd,
    ) !== undefined;
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
   * Register a FIFO intent before asynchronous root probes, then acquire the exclusive commit
   * lease only after earlier writers and reservations are clear. Later tasks cannot cross the
   * intent while those checks are in flight.
   */
  async tryAcquireCommit(
    target: ChatTarget,
    cwd: string,
    root: string,
    taskId: string,
    orderHint?: number,
  ): Promise<CommitLease | undefined> {
    const binding = this.bindingFor(target);
    const sessionKey = this.sessionKey(target, binding);
    const reservation = this.createCommandReservationRecord(
      target,
      [cwd, root],
      this.workspaceKeys([root]),
      orderHint,
    );
    const order = reservation.order;
    const directories = reservation.directories;
    const unavailable = (): boolean =>
      this.deps.stopping()
      || this.hasConflictingReservation(target, sessionKey, directories, order, reservation)
      || this.hasSessionLeaseFor(target, sessionKey)
      || this.hasQueuedWork(target, sessionKey, order)
      || this.activeRepositoryTasksHas(root)
      || this.exclusiveRepositoryRoots.has(root)
      || this.deps.isDirectoryBlocked(cwd)
      || this.deps.isDirectoryBlocked(root)
      || this.directoriesUnavailable(directories, order);
    let rootLease: { complete(): void } | undefined;
    let sessionLease: SessionLease | undefined;
    let transferred = false;
    let acquiredLease: CommitLease | undefined;
    try {
      if (
        unavailable()
        || await this.hasConflictingRepositoryReservation(root, order, reservation)
        || await this.repositoryIsBlocked(root)
        || await this.repositoryDirectoryLeaseConflicts(root)
        || unavailable()
        || await this.hasConflictingRepositoryReservation(root, order, reservation)
      ) {
        return undefined;
      }

      // This intent was registered before any Git probe; later tasks and host merges cannot
      // overtake it while the root is being checked and the exclusive lease is installed.
      rootLease = this.reserveExclusiveRepositoryRoot(root, taskId);
      if (!rootLease) return undefined;
      sessionLease = this.createLeases(target, sessionKey, directories);
      const lease: CommitLease = {
        root,
        taskId,
        order,
        ready: sessionLease.ready,
        complete: () => {
          sessionLease!.complete();
          rootLease!.complete();
          reservation.release();
        },
      };
      acquiredLease = lease;
      transferred = true;

      for (const candidate of this.deps.store.listQueuedTasks()) {
        const candidateOrder = this.scheduledOrder.get(candidate.id) ?? -1;
        if (candidateOrder >= order) continue;
        const candidateTarget = taskTarget(candidate);
        if (
          targetKey(candidateTarget) === targetKey(target)
          || this.sessionKey(candidateTarget, this.bindingFor(candidateTarget)) === sessionKey
        ) {
          lease.complete();
          return undefined;
        }
        if (await this.repositoryRootForTask(candidate) === root) {
          lease.complete();
          return undefined;
        }
      }
      if (
        this.deps.stopping()
        || this.activeRepositoryTasksHas(root)
        || await this.repositoryIsBlocked(root)
      ) {
        lease.complete();
        return undefined;
      }
      return lease;
    } catch {
      acquiredLease?.complete();
      return undefined;
    } finally {
      if (!transferred) {
        sessionLease?.complete();
        rootLease?.complete();
        reservation.release();
      }
    }
  }

  /** Repository-only reservation for a queued `/commit` recovered before native dispatch. */
  async tryAcquireCommitRepository(root: string, taskId: string): Promise<CommitLease | undefined> {
    const task = this.deps.store.getTask(taskId);
    const order = this.scheduledOrder.get(taskId);
    if (!task || task.state !== "queued" || order === undefined) return undefined;
    const target = taskTarget(task);
    const binding = this.bindingFor(target);
    const reservation = this.createCommandReservationRecord(
      target,
      [binding?.cwd ?? task.cwd, root],
      this.workspaceKeys([root]),
      order,
    );
    const ownDirectories = this.taskDirectoryLeases.get(taskId) ?? new Set<string>();
    const directories = this.workspaceKeys([root]).filter((directory) => !ownDirectories.has(directory));
    let rootLease: { complete(): void } | undefined;
    let directoryLeases: DirectoryLease[] = [];
    let transferred = false;
    try {
      if (
        this.deps.stopping()
        || this.activeRepositoryTasksHas(root)
        || this.exclusiveRepositoryRoots.has(root)
        || this.directoriesUnavailable(directories, order, taskId)
        || await this.hasConflictingRepositoryReservation(root, order, reservation)
        || await this.repositoryIsBlocked(root)
        || await this.repositoryDirectoryLeaseConflicts(root, ownDirectories)
        || this.deps.stopping()
        || this.activeRepositoryTasksHas(root)
        || this.exclusiveRepositoryRoots.has(root)
        || this.directoriesUnavailable(directories, order, taskId)
        || await this.hasConflictingRepositoryReservation(root, order, reservation)
      ) {
        return undefined;
      }
      rootLease = this.reserveExclusiveRepositoryRoot(root, taskId);
      if (!rootLease) return undefined;
      directoryLeases = directories.map((directory) => this.createDirectoryLease(directory));
      const lease: CommitLease = {
        root,
        taskId,
        order,
        ready: Promise.all(directoryLeases.map((directoryLease) => directoryLease.ready)).then(() => {}),
        complete: () => {
          for (const directoryLease of directoryLeases) directoryLease.complete();
          rootLease!.complete();
          reservation.release();
        },
      };
      transferred = true;
      for (const candidate of this.deps.store.listQueuedTasks()) {
        if (candidate.id === taskId) continue;
        const candidateOrder = this.scheduledOrder.get(candidate.id) ?? -1;
        if (candidateOrder >= order) continue;
        if (await this.repositoryRootForTask(candidate) === root) {
          lease.complete();
          return undefined;
        }
      }
      if (
        this.deps.stopping()
        || this.activeRepositoryTasksHas(root)
        || await this.repositoryIsBlocked(root)
      ) {
        lease.complete();
        return undefined;
      }
      return lease;
    } catch {
      if (transferred) {
        rootLease?.complete();
        for (const directoryLease of directoryLeases) directoryLease.complete();
        reservation.release();
      }
      return undefined;
    } finally {
      if (!transferred) {
        rootLease?.complete();
        for (const directoryLease of directoryLeases) directoryLease.complete();
        reservation.release();
      }
    }
  }


  /** Schedules an already-reserved commit; command/root leases enforce its FIFO position. */
  scheduleCommitTask(
    taskId: string,
    lease: CommitLease,
    runCommit: (taskId: string) => Promise<void>,
    abandon: () => void,
  ): void {
    const task = this.deps.store.getTask(taskId);
    if (!task) {
      lease.complete();
      abandon();
      return;
    }
    const order = lease.order;
    this.scheduledOrder.set(taskId, order);
    // Do not chain this after chatTails: a later task may already be waiting on this commit's
    // FIFO intent, so chaining behind it would deadlock. The root/session barriers serialize it.
    const next = Promise.resolve().then(async () => {
      try {
        await this.deps.awaitStartReady();
        const current = this.deps.store.getTask(taskId);
        if (this.deps.stopping() || !current || current.state !== "queued") {
          abandon();
          return;
        }
        await lease.ready;
        await runCommit(taskId);
      } catch {
        abandon();
      } finally {
        lease.complete();
      }
    });
    this.scheduledTasks.set(taskId, next);
    this.taskOperations.add(next);
    const cleanup = () => {
      if (this.scheduledTasks.get(taskId) === next) this.scheduledTasks.delete(taskId);
      if (this.scheduledOrder.get(taskId) === order) this.scheduledOrder.delete(taskId);
      this.taskOperations.delete(next);
    };
    void next.then(cleanup, cleanup);
  }

  /**
   * A command holds the current session and chat keys, while cwd is consulted for an unverified
   * worker left by recovery. When the command touches an owned workspace (or a pinned workspace
   * binding), its expanded resource keys are leased too so sibling projects cannot overlap.
   * Different sessions in the same ordinary cwd remain independent.
   */
  async tryAcquireCommandSession(
    target: ChatTarget,
    cwds: readonly string[],
  ): Promise<SessionLease | undefined> {
    const binding = this.bindingFor(target);
    const sessionKey = this.sessionKey(target, binding);
    const reservation = this.createCommandReservationRecord(target, cwds);
    const directories = reservation.directories;
    let repositoryLease: { complete(): void } | undefined;
    let sessionLease: SessionLease | undefined;
    let transferred = false;
    const unavailable = (): boolean =>
      this.deps.stopping()
      || this.hasConflictingReservation(target, sessionKey, directories, reservation.order, reservation)
      || this.hasSessionLeaseFor(target, sessionKey)
      || this.hasQueuedWork(target, sessionKey, reservation.order)
      || cwds.some((cwd) => this.deps.isDirectoryBlocked(cwd))
      || this.directoriesUnavailable(directories, reservation.order);
    try {
      if (unavailable()) return undefined;
      const roots = new Set<string>();
      for (const cwd of new Set(cwds)) {
        const root = await this.deps.repositoryRoot(cwd);
        if (root !== null) roots.add(root);
      }
      if (
        unavailable()
        || this.sessionKey(target, this.bindingFor(target)) !== sessionKey
        || await this.hasConflictingRepositoryReservations([...roots], reservation.order, reservation)
      ) {
        return undefined;
      }
      repositoryLease = this.reserveRepositoryTasks(
        [...roots],
        `command:${targetKey(target)}`,
      );
      if (!repositoryLease) return undefined;
      for (const root of roots) {
        if (await this.repositoryIsBlocked(root)) return undefined;
      }
      if (unavailable()) return undefined;
      sessionLease = this.createLeases(target, sessionKey, directories);
      transferred = true;
      return {
        ready: sessionLease.ready,
        complete: () => {
          sessionLease!.complete();
          repositoryLease!.complete();
          reservation.release();
        },
      };
    } catch {
      return undefined;
    } finally {
      if (!transferred) {
        sessionLease?.complete();
        repositoryLease?.complete();
        reservation.release();
      }
    }
  }

  /**
   * Install a FIFO barrier before waiting for accepted work. Later tasks wait on this intent;
   * the owner itself acquires its real session/directory leases only after earlier work drains.
   */
  reserveCommandSession(target: ChatTarget, cwds: readonly string[]): CommandSessionReservation {
    const reservation = this.createCommandReservationRecord(target, cwds);
    return {
      order: reservation.order,
      tryAcquire: () => this.tryAcquireReservedCommandSession(reservation),
      release: () => reservation.release(),
    };
  }

  private createCommandReservationRecord(
    target: ChatTarget,
    cwds: readonly string[],
    extraDirectories: readonly string[] = [],
    order?: number,
  ): CommandReservationRecord {
    const binding = this.bindingFor(target);
    const sessionKey = this.sessionKey(target, binding);
    const directories = [...new Set([
      ...this.ownedDirectoryKeys(
        cwds,
        binding?.cwdPinned === true ? binding.cwd : undefined,
      ),
      ...extraDirectories,
    ])].sort();
    const completion = Promise.withResolvers<void>();
    let reservation!: CommandReservationRecord;
    reservation = {
      order: order ?? ++this.nextOrder,
      target: { ...target },
      sessionKey,
      sessionKeys: this.leaseKeys(target, sessionKey),
      directories,
      repositoryCwds: [...new Set(cwds)],
      ready: completion.promise,
      release: () => {
        if (!this.commandReservations.delete(reservation)) return;
        completion.resolve();
      },
    };
    this.commandReservations.add(reservation);
    return reservation;
  }

  /** Lease every owned resource related to these paths, not just the caller's cwd spelling. */
  async tryAcquireCommandDirectories(cwds: readonly string[]): Promise<DirectoryLease | undefined> {
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
    const directoryLeases = directories.map((cwd) => this.createDirectoryLease(cwd));
    let repositoryLease: { complete(): void } | undefined;
    let transferred = false;
    try {
      const roots = new Set<string>();
      for (const cwd of new Set(cwds)) {
        const root = await this.deps.repositoryRoot(cwd);
        if (root !== null) roots.add(root);
      }
      repositoryLease = this.reserveRepositoryTasks([...roots], "directory-command");
      if (!repositoryLease) return undefined;
      for (const root of roots) {
        if (await this.repositoryIsBlocked(root)) return undefined;
      }
      transferred = true;
      return {
        ready: Promise.all(directoryLeases.map((lease) => lease.ready)).then(() => {}),
        complete: () => {
          for (const lease of directoryLeases) lease.complete();
          repositoryLease!.complete();
        },
      };
    } catch {
      return undefined;
    } finally {
      if (!transferred) {
        for (const lease of directoryLeases) lease.complete();
        repositoryLease?.complete();
      }
    }
  }

  /**
   * A host operation may reuse the running task's exclusive resource lease only when every
   * requested key is already covered. A wider request goes through the ordinary busy check.
   */
  async tryAcquireWorkspaceDirectories(
    cwds: readonly string[],
    task: TaskRecord,
  ): Promise<DirectoryLease | undefined> {
    const requested = this.workspaceKeys(cwds);
    const held = this.taskDirectoryLeases.get(task.id);
    if (requested.length > 0 && held && requested.every((directory) => held.has(directory))) {
      return { ready: Promise.resolve(), complete: () => {} };
    }
    return this.tryAcquireCommandDirectories(cwds);
  }

  private bindingFor(target: ChatTarget): BindingRecord | null {
    const botUid = this.deps.botUid();
    return botUid === undefined ? null : this.deps.store.getBinding(botUid, target);
  }

  private activeRepositoryTasksHas(root: string): boolean {
    for (const activeRoot of this.activeRepositoryTasks.values()) {
      if (activeRoot === root) return true;
    }
    return false;
  }

  private repositoryIsBlocked(root: string): Promise<boolean> {
    return this.deps.repositoryBlocked(root);
  }
  private reserveRepositoryTasks(
    roots: readonly string[],
    owner: string,
  ): { complete(): void } | undefined {
    const uniqueRoots = [...new Set(roots)];
    if (uniqueRoots.some((root) => this.exclusiveRepositoryRoots.has(root))) return undefined;
    const entries = uniqueRoots.map((root, index) => [
      `${owner}:${++this.repositoryLeaseSequence}:${index}`,
      root,
    ] as const);
    for (const [key, root] of entries) this.activeRepositoryTasks.set(key, root);
    let complete = false;
    return {
      complete: () => {
        if (complete) return;
        complete = true;
        for (const [key] of entries) this.activeRepositoryTasks.delete(key);
      },
    };
  }

  private async repositoryDirectoryLeaseConflicts(
    root: string,
    allowedDirectories: ReadonlySet<string> = new Set(),
  ): Promise<boolean> {
    const rootResources = new Set(this.workspaceKeys([root]));
    for (const directory of this.directoryTails.keys()) {
      if (allowedDirectories.has(directory)) continue;
      if (this.workspaceKeys([directory]).some((resource) => rootResources.has(resource))) return true;
      try {
        if (await this.deps.repositoryRoot(directory) === root) return true;
      } catch {
        // A directory whose repository cannot be identified must not be allowed to overlap a
        // commit; this is the fail-closed counterpart to treating an explicit non-repository as null.
        return true;
      }
    }
    return false;
  }

  private reserveExclusiveRepositoryRoot(
    root: string,
    owner: string,
  ): { complete(): void } | undefined {
    if (this.exclusiveRepositoryRoots.has(root)) return undefined;
    const completed = Promise.withResolvers<void>();
    const entry = { owner, done: completed.promise };
    this.exclusiveRepositoryRoots.set(root, entry);
    let complete = false;
    return {
      complete: () => {
        if (complete) return;
        complete = true;
        if (this.exclusiveRepositoryRoots.get(root) === entry) this.exclusiveRepositoryRoots.delete(root);
        completed.resolve();
      },
    };
  }

  /** Display text alone never authorizes a native commit. */
  private isCommitTask(task: TaskRecord): boolean {
    return task.text === "/commit" && this.deps.store.isCommitTask(task.id);
  }

  private async repositoryRootForTask(task: TaskRecord): Promise<string | null> {
    const target = taskTarget(task);
    const binding = await this.deps.reconcileBinding(task.id, target, this.bindingFor(target));
    const current = this.deps.store.getTask(task.id);
    if (!current || current.state !== "queued") return null;
    return this.deps.repositoryRoot(binding?.cwd ?? current.cwd);
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

  private async hasConflictingRepositoryReservation(
    root: string,
    beforeOrder: number,
    excluded?: CommandReservationRecord,
  ): Promise<boolean> {
    for (const reservation of this.commandReservations) {
      if (reservation === excluded || reservation.order >= beforeOrder) continue;
      for (const cwd of reservation.repositoryCwds) {
        try {
          if (await this.deps.repositoryRoot(cwd) === root) return true;
        } catch {
          // An earlier unresolved root is a barrier, not permission to let a writer pass.
          return true;
        }
      }
    }
    return false;
  }
  private async hasConflictingRepositoryReservations(
    roots: readonly string[],
    beforeOrder: number,
    excluded?: CommandReservationRecord,
  ): Promise<boolean> {
    for (const root of roots) {
      if (await this.hasConflictingRepositoryReservation(root, beforeOrder, excluded)) return true;
    }
    return false;
  }


  private async hasEarlierQueuedRepositoryWork(root: string, beforeOrder: number): Promise<boolean> {
    for (const task of this.deps.store.listQueuedTasks()) {
      const order = this.scheduledOrder.get(task.id) ?? -1;
      if (order >= beforeOrder) continue;
      if (await this.repositoryRootForTask(task) === root) return true;
    }
    return false;
  }

  private async firstBlockingReservation(
    target: ChatTarget,
    sessionKey: string,
    directories: readonly string[],
    taskOrder: number,
    cwd?: string,
  ): Promise<CommandReservationRecord | undefined> {
    const candidates = [...this.commandReservations]
      .filter((reservation) => reservation.order < taskOrder)
      .sort((left, right) => left.order - right.order);
    let taskRoot: string | null | undefined;
    for (const reservation of candidates) {
      if (this.reservationConflicts(reservation, target, sessionKey, directories)) {
        return reservation;
      }
      if (cwd === undefined) continue;
      if (taskRoot === undefined) taskRoot = await this.deps.repositoryRoot(cwd);
      if (taskRoot === null) continue;
      for (const reservedCwd of reservation.repositoryCwds) {
        if (await this.deps.repositoryRoot(reservedCwd) === taskRoot) return reservation;
      }
    }
    return undefined;
  }

  private async waitForPriorReservations(
    target: ChatTarget,
    sessionKey: string,
    directories: readonly string[],
    taskOrder: number,
    cwd?: string,
  ): Promise<void> {
    while (!this.deps.stopping()) {
      const reservation = await this.firstBlockingReservation(
        target,
        sessionKey,
        directories,
        taskOrder,
        cwd,
      );
      if (!reservation) return;
      await reservation.ready;
    }
  }

  private async tryAcquireReservedCommandSession(
    reservation: CommandReservationRecord,
  ): Promise<SessionLease | undefined> {
    if (!this.commandReservations.has(reservation)) return undefined;
    const binding = this.bindingFor(reservation.target);
    const sessionKey = this.sessionKey(reservation.target, binding);
    const directories = reservation.directories;
    const locallyUnavailable = (): boolean =>
      !this.commandReservations.has(reservation)
      || sessionKey !== this.sessionKey(reservation.target, this.bindingFor(reservation.target))
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
      || this.directoriesUnavailable(directories, reservation.order);
    if (locallyUnavailable()) return undefined;

    const roots = new Set<string>();
    for (const cwd of reservation.repositoryCwds) {
      const root = await this.deps.repositoryRoot(cwd);
      if (root !== null) roots.add(root);
    }
    if (locallyUnavailable()) return undefined;

    for (const root of roots) {
      if (await this.hasConflictingRepositoryReservation(root, reservation.order, reservation)) {
        return undefined;
      }
      if (await this.repositoryIsBlocked(root)) {
        throw new Error("该 Git 工作树仍有无法确认停止的 worker，无法安全合并。");
      }
      if (
        await this.hasEarlierQueuedRepositoryWork(root, reservation.order)
        || await this.repositoryDirectoryLeaseConflicts(root)
      ) {
        return undefined;
      }
    }
    if (locallyUnavailable()) return undefined;
    if ([...roots].some((root) =>
      this.activeRepositoryTasksHas(root) || this.exclusiveRepositoryRoots.has(root)
    )) {
      return undefined;
    }

    const rootLeases: { complete(): void }[] = [];
    for (const root of roots) {
      const rootLease = this.reserveExclusiveRepositoryRoot(
        root,
        `host-merge:${targetKey(reservation.target)}:${reservation.order}`,
      );
      if (!rootLease) {
        for (const held of rootLeases) held.complete();
        return undefined;
      }
      rootLeases.push(rootLease);
    }
    const sessionLease = this.createLeases(reservation.target, sessionKey, directories);
    let complete = false;
    return {
      ready: sessionLease.ready,
      complete: () => {
        if (complete) return;
        complete = true;
        sessionLease.complete();
        for (const rootLease of rootLeases) rootLease.complete();
      },
    };
  }

  private hasSessionLeaseFor(target: ChatTarget, sessionKey: string): boolean {
    return this.leaseKeys(target, sessionKey).some((key) => this.sessionTails.has(key));
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

  private directoriesUnavailable(
    directories: readonly string[],
    beforeOrder?: number,
    excludedTaskId?: string,
  ): boolean {
    const requested = new Set(directories);
    if (directories.some((cwd) =>
      this.deps.isDirectoryBlocked(cwd)
      || this.directoryTails.has(cwd)
      || this.deps.hasRuntimeInDirectory(cwd)
    )) {
      return true;
    }
    return this.deps.store.listQueuedTasks().some((task) => {
      if (task.id === excludedTaskId) return false;
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
    skipRepositoryOrdering = false,
  ): Promise<Promise<void>[]> {
    const queued = this.deps.store.listQueuedTasks();
    if (!queued.some((candidate) => candidate.id === task.id)) return [];
    const taskOrder = this.scheduledOrder.get(task.id) ?? Number.POSITIVE_INFINITY;
    const earlierTasks = queued.filter((candidate) =>
      candidate.id !== task.id
      && (this.scheduledOrder.get(candidate.id) ?? -1) < taskOrder
    );
    const commitOrderingRequired = !skipRepositoryOrdering && (
      this.isCommitTask(task)
      || earlierTasks.some((candidate) => this.isCommitTask(candidate))
    );
    const currentBinding = this.bindingFor(taskTarget(task));
    const currentRoot = commitOrderingRequired
      ? await this.deps.repositoryRoot(currentBinding?.cwd ?? task.cwd)
      : null;
    const operations: Promise<void>[] = [];
    for (const earlier of earlierTasks) {
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
      const sharesRepository = !skipRepositoryOrdering
        && currentRoot !== null
        && await this.deps.repositoryRoot(earlierCwd) === currentRoot;
      if (!sharesSession && !sharesDirectory && !sharesRepository) continue;
      const operation = this.scheduledTasks.get(earlier.id);
      if (operation) operations.push(operation);
    }
    return operations;
  }

  private async failRepositoryProbe(taskId: string, target: ChatTarget): Promise<void> {
    const current = this.deps.store.getTask(taskId);
    if (!current || current.state !== "queued") return;
    const failure = "Git 仓库状态无法确认，任务未启动。";
    this.takeWaitingReceipt(taskId);
    this.deps.store.markFailed(taskId, failure);
    await this.deps.enqueueText(target, failure, taskId);
    await this.deps.notifyThreadOutcome(taskId);
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
        const commitTask = this.isCommitTask(reconciledCurrent);
        if (!commitTask && this.deps.isDirectoryBlocked(cwd)) return;
        const sessionKey = this.sessionKey(target, binding);
        const directories = this.ownedDirectoryKeys(
          [cwd],
          binding?.cwdPinned === true ? binding.cwd : undefined,
        );
        const taskOrder = this.scheduledOrder.get(taskId) ?? Number.POSITIVE_INFINITY;
        let skipRepositoryOrdering = false;
        try {
          if (!commitTask) {
            skipRepositoryOrdering =
              await this.deps.shouldRunTaskBeforeRepositoryProbe(taskId, target, binding, cwd);
          }
          await this.waitForPriorReservations(
            target,
            sessionKey,
            directories,
            taskOrder,
            skipRepositoryOrdering ? undefined : cwd,
          );
          if (this.deps.stopping()) return;
          const earlier = await this.earlierOperations(
            reconciledCurrent,
            sessionKey,
            new Set(directories),
            skipRepositoryOrdering,
          );
          await Promise.all(earlier.map((operation) => operation.catch(() => {})));
          await this.waitForPriorReservations(
            target,
            sessionKey,
            directories,
            taskOrder,
            skipRepositoryOrdering ? undefined : cwd,
          );
          if (this.deps.stopping()) return;
        } catch {
          await this.failRepositoryProbe(taskId, target);
          return;
        }

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
        if (!commitTask && this.deps.isDirectoryBlocked(currentCwd)) {
          lease.complete();
          lease = undefined;
          return;
        }
        try {
          if (!commitTask) {
            skipRepositoryOrdering = await this.deps.shouldRunTaskBeforeRepositoryProbe(
              taskId,
              beforeTarget,
              refreshed,
              currentCwd,
            );
          }
          const newlyEarlier = await this.earlierOperations(
            latest,
            currentSessionKey,
            new Set(directories),
            skipRepositoryOrdering,
          );
          if (newlyEarlier.length > 0) {
            lease.complete();
            lease = undefined;
            await Promise.all(newlyEarlier.map((operation) => operation.catch(() => {})));
            continue;
          }
        } catch {
          await this.failRepositoryProbe(taskId, beforeTarget);
          return;
        }
        let activeRoot: string | undefined;
        if (!commitTask && !skipRepositoryOrdering) {
          let root: string | null;
          try {
            root = await this.deps.repositoryRoot(currentCwd);
          } catch {
            await this.failRepositoryProbe(taskId, beforeTarget);
            return;
          }
          if (root !== null) {
            let blocked: boolean;
            try {
              blocked = await this.repositoryIsBlocked(root);
            } catch {
              blocked = true;
            }
            if (blocked) {
              const failure = "该 Git 工作树仍有无法确认停止的 worker，任务未启动。";
              this.takeWaitingReceipt(taskId);
              this.deps.store.markFailed(taskId, failure);
              await this.deps.enqueueText(beforeTarget, failure, taskId);
              await this.deps.notifyThreadOutcome(taskId);
              return;
            }
            const reservation = this.exclusiveRepositoryRoots.get(root);
            if (reservation !== undefined) {
              lease.complete();
              lease = undefined;
              await reservation.done.catch(() => {});
              continue;
            }
            this.activeRepositoryTasks.set(taskId, root);
            activeRoot = root;
          }
        }
        const taskDirectories = new Set(directories);
        if (taskDirectories.size > 0) this.taskDirectoryLeases.set(taskId, taskDirectories);
        try {
          await this.deps.runTask(taskId, cwd);
        } finally {
          if (activeRoot !== undefined && this.activeRepositoryTasks.get(taskId) === activeRoot) {
            let blocked = true;
            try {
              blocked = await this.repositoryIsBlocked(activeRoot);
            } catch {
              // Keep the canonical root reservation if the blocked state cannot be established.
            }
            // Retain an unconfirmed root in this scheduler until restart, when worker recovery rechecks it.
            if (!blocked) this.activeRepositoryTasks.delete(taskId);
          }
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
