/**
 * 任务调度与会话租约的唯一所有者：每个聊天的任务串行链、已排定任务、关停要等的在途
 * 任务、启动回执，以及按 session / chat key 的租约链都在这里。显式声明路径资源保护的操作
 * 才另持 canonical worktree root 与精确路径的排他租约；`/thread merge` 与 `/thread remove`
 * 只保留精确聊天会话顺序，不预留工作区资源。普通任务不因 cwd 或 workspace 所有权互锁。
 *
 * 模块不依赖 `service.ts`：任务行与绑定行的读取、绑定核对、真正的执行入口（`runTask`）、
 * bot 身份与停止态、运行中任务与 blocked 集合、绑定探测、回执与终态通知的投递路径都按原样注入。
 * 会话租约、破坏性资源租约与 blocked 集合仍然分别只有一个所有者。
 */
import type { ChatTarget } from "../contracts.ts";
import type { Outbox } from "../outbox.ts";
import type { ChannelStore } from "../store/index.ts";
import type { BindingRecord, TaskRecord } from "../store/store-records.ts";
import { GENERIC_TASK_FAILURE_NOTICE } from "./task-failure.ts";
import { targetKey, taskTarget } from "../target.ts";
import { lstat } from "node:fs/promises";
import { isAbsolute, relative, resolve, sep } from "node:path";

async function waitForAbort<T>(operation: Promise<T>, signal: AbortSignal): Promise<T> {
  if (signal.aborted) throw signal.reason instanceof Error ? signal.reason : new Error("operation aborted");
  let onAbort: (() => void) | undefined;
  const aborted = new Promise<never>((_, reject) => {
    onAbort = () => reject(signal.reason instanceof Error ? signal.reason : new Error("operation aborted"));
    signal.addEventListener("abort", onAbort, { once: true });
  });
  try {
    return await Promise.race([operation, aborted]);
  } finally {
    if (onAbort) signal.removeEventListener("abort", onAbort);
  }
}

function abortableDelay(milliseconds: number, signal: AbortSignal): Promise<void> {
  return waitForAbort(
    new Promise<void>(resolve => setTimeout(resolve, milliseconds)),
    signal,
  );
}

/**
 * 按 session 与聊天键的租约：任务和 `/new`、`/move` 共用对应的会话锁，聊天键还防止命令期间
 * 绑定切换后同聊天的新任务越过命令。`ready` 在它们都空闲后兑现，`complete` 恰好释放一次。
 */
export type SessionLease = {
  ready: Promise<void>;
  complete(): void;
};

/** Destructive path/root lock; ordinary execution never acquires one. */
export type DestructiveResourceLease = {
  ready: Promise<void>;
  complete(): void;
};

/** A destructive FIFO intent that blocks later writes while earlier work drains. */
export interface DestructiveCommandReservation {
  readonly order: number;
  /** Add exact protected paths and, separately, the subset that can be probed as Git repositories. */
  extend(cwds: readonly string[], repositoryCwds?: readonly string[]): void;
  tryAcquire(): Promise<SessionLease | undefined>;
  release(): void;
}
/** Session-only FIFO intent with shared path-use registration for destructive operations. */
export interface SessionCommandReservation {
  readonly order: number;
  extend(cwds: readonly string[]): void;
  /** A recovery may bypass only the exact earlier unknown transition holds it verified stopped. */
  bypassUncertainPriorOrders(orders: readonly number[]): boolean;
  /** Wait for earlier known work; uncertain-outcome holds still block acquisition. */
  waitForPrior(signal: AbortSignal): Promise<void>;
  tryAcquire(): Promise<SessionLease | undefined>;
  release(): void;
}

export interface CommandSessionBarrier {
  /** Waits for work scheduled before this barrier on the exact chat/session, never cwd/Git peers. */
  waitForPrior(signal: AbortSignal): Promise<void>;
  release(): void;
}

interface CommandReservationRecord {
  readonly order: number;
  readonly target: ChatTarget;
  sessionKeys: readonly string[];
  /** An uncertain operation must remain a blocker, but a later safety probe must not await it. */
  readonly bypassedUncertainOrders: Set<number>;
  readonly uncertainOutcomeHold: boolean;
  resourcePaths: readonly string[];
  repositoryCwds: readonly string[];
  readonly ownerTaskId?: string;
  readonly ready: Promise<void>;
  release(): void;
}
interface ActiveResourceUse {
  readonly paths: readonly string[];
  readonly roots: ReadonlySet<string>;
}
interface SessionLeaseOwner {
  readonly taskId?: string;
}
interface ActiveResourceUse {
  readonly paths: readonly string[];
  readonly roots: ReadonlySet<string>;
}
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
    | "getTaskScheduleOrder"
    | "allocateScheduleOrder"
    | "listQueuedTasks"
    | "getBinding"
    | "markFailed"
    | "isCommitTask"
  >
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
  /** 启动一个已排定任务；`cwd` 是启动前核对用的绑定目录。 */
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
  /** 每个聊天的任务串行链：该聊天的下一个任务接在上一个（或已排定任务）之后。 */
  private readonly chatTails = new Map<string, Promise<void>>();
  /** 已排定任务操作：同 session 的较早任务据此等待。 */
  private readonly scheduledTasks = new Map<string, Promise<void>>();
  /** 关停要等的在途任务（已排定任务的操作）。 */
  private readonly taskOperations = new Set<Promise<void>>();
  /** In-memory marker used to add the startup receipt to tasks that waited for their chat/session. */
  private readonly waiting = new Set<string>();
  /** Tasks and session commands share per-session and per-chat lease tails. */
  private readonly sessionTails = new Map<string, Promise<void>>();
  private readonly sessionLeaseOwners = new Map<string, Set<SessionLeaseOwner>>();
  /** Scheduling order lets command barriers distinguish earlier work from later task tails. */
  private readonly scheduledOrder = new Map<string, number>();
  private readonly commandReservations = new Set<CommandReservationRecord>();
  /** Ordinary workers and session-control workers are shared resource users. */
  private readonly activeResourceUsers = new Map<string, ActiveResourceUse>();
  private readonly exclusiveRepositoryRoots = new Map<string, { owner: string; done: Promise<void> }>();
  private readonly exclusiveResourcePaths = new Map<string, { owner: string; done: Promise<void> }>();

  /**
   * True when a new task on this chat cannot start at once: the chat/session is busy, or an
   * explicit destructive operation owns an overlapping path. Ordinary cwd ownership is irrelevant.
   */
  hasBlockingWork(target: ChatTarget, cwd: string): boolean {
    const binding = this.bindingFor(target);
    const sessionKey = this.sessionKey(target, binding);
    return this.hasConflictingReservation(target, sessionKey)
      || this.hasLeaseFor(target, sessionKey)
      || this.hasQueuedWork(target, sessionKey)
      || this.hasExclusivePathConflict(cwd);
  }

  /** A command reservation is a FIFO barrier even when an active task could normally accept steering. */
  async hasPendingCommandReservation(target: ChatTarget, cwd: string): Promise<boolean> {
    const binding = this.bindingFor(target);
    const sessionKey = this.sessionKey(target, binding);
    return await this.firstBlockingReservation(
      target,
      sessionKey,
      Number.POSITIVE_INFINITY,
      cwd,
    ) !== undefined;
  }


  constructor(private readonly deps: TaskSchedulerDeps) {}

  /** Schedule a task using its durable acceptance-order token. */
  scheduleTask(taskId: string): void {
    if (this.scheduledTasks.has(taskId)) return;
    const task = this.deps.store.getTask(taskId);
    if (!task) return;
    const order = this.taskOrder(taskId);
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

  /**
   * Run one durable host operation at its persisted chat/session order. The reservation is
   * installed synchronously so later tasks cannot pass it; the operation itself runs only after
   * all earlier task/session owners release their leases.
   */
  scheduleSessionOperation(
    target: ChatTarget,
    order: number,
    signal: AbortSignal,
    run: () => Promise<void>,
    onFailure: (error: unknown) => void,
  ): void {
    const reservation = this.reserveSessionCommand(target, [], order);
    const next = Promise.resolve().then(async () => {
      let lease: SessionLease | undefined;
      try {
        await this.deps.awaitStartReady();
        await reservation.waitForPrior(signal);
        lease = await reservation.tryAcquire();
        if (lease === undefined) throw new Error("could not acquire ordered session operation lease");
        await lease.ready;
        if (this.deps.stopping() || signal.aborted) {
          throw signal.aborted
            ? (signal.reason instanceof Error ? signal.reason : new Error("session operation aborted"))
            : new Error("channel stopping");
        }
        await run();
      } catch (error) {
        onFailure(error);
      } finally {
        lease?.complete();
        reservation.release();
      }
    });
    this.taskOperations.add(next);
    const cleanup = () => this.taskOperations.delete(next);
    void next.then(cleanup, cleanup);
  }
  private taskOrder(taskId: string): number {
    const order = this.scheduledOrder.get(taskId) ?? this.deps.store.getTaskScheduleOrder(taskId);
    if (order === null) throw new Error(`queued task ${taskId} has no durable schedule order`);
    return order;
  }

  /** 记下「这条任务在忙聊天、忙 session 或排他资源后面等过」：启动时取回，据此补一条启动回执。 */
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

  /** Waits only for this scheduled task, including release of its scheduler session lease. */
  waitForTaskSettlement(taskId: string): Promise<void> {
    return this.scheduledTasks.get(taskId)?.catch(() => {}) ?? Promise.resolve();
  }

  /**
   * Install a FIFO intent before asynchronous root probes, then acquire exclusive ownership only
   * after earlier work and reservations clear. Later tasks cannot cross the intent meanwhile.
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
    const paths = this.resourcePaths([cwd, root]);
    const roots = new Set([root]);
    const reservation = this.createCommandReservationRecord(
      target,
      [cwd, root],
      true,
      orderHint,
    );
    const order = reservation.order;
    const unavailable = (): boolean =>
      this.deps.stopping()
      || this.hasConflictingReservation(target, sessionKey, order, reservation)
      || this.hasSessionLeaseFor(target, sessionKey)
      || this.hasQueuedWork(target, sessionKey, order)
      || paths.some((path) => this.deps.isDirectoryBlocked(path))
      || this.hasActiveResourceConflict(paths, roots, taskId)
      || this.hasExclusiveResourceConflict(paths, roots);
    let resourceLease: DestructiveResourceLease | undefined;
    let sessionLease: SessionLease | undefined;
    let transferred = false;
    let acquiredLease: CommitLease | undefined;
    try {
      if (
        unavailable()
        || await this.hasConflictingRepositoryReservation(root, order, reservation)
        || await this.hasEarlierQueuedResourceUse(paths, roots, order)
        || await this.repositoryIsBlocked(root)
        || unavailable()
        || await this.hasConflictingRepositoryReservation(root, order, reservation)
      ) {
        return undefined;
      }

      if (unavailable()) return undefined;
      resourceLease = this.reserveExclusiveResources(roots, paths, taskId);
      if (!resourceLease) return undefined;
      sessionLease = this.createSessionLease(target, sessionKey);

      const lease: CommitLease = {
        root,
        taskId,
        order,
        ready: sessionLease.ready,
        complete: () => {
          sessionLease!.complete();
          resourceLease!.complete();
          reservation.release();
        },
      };
      acquiredLease = lease;
      transferred = true;

      if (
        await this.hasEarlierQueuedResourceUse(paths, roots, order)
        || this.deps.stopping()
        || this.hasActiveResourceConflict(paths, roots, taskId)
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
        resourceLease?.complete();
        reservation.release();
      }
    }
  }


  /** Repository-root reservation for a queued `/commit` recovered before native dispatch. */
  async tryAcquireCommitRepository(root: string, taskId: string): Promise<CommitLease | undefined> {
    const task = this.deps.store.getTask(taskId);
    const order = this.scheduledOrder.get(taskId);
    if (!task || task.state !== "queued" || order === undefined) return undefined;
    const target = taskTarget(task);
    const binding = this.bindingFor(target);
    const cwd = binding?.cwd ?? task.cwd;
    const paths = this.resourcePaths([cwd, root]);
    const roots = new Set([root]);
    const reservation = this.createCommandReservationRecord(target, [cwd, root], true, order);
    let resourceLease: DestructiveResourceLease | undefined;
    let transferred = false;
    let acquiredLease: CommitLease | undefined;
    try {
      if (
        this.deps.stopping()
        || this.hasConflictingReservation(target, this.sessionKey(target, binding), order, reservation)
        || this.hasSessionLeaseFor(target, this.sessionKey(target, binding), taskId)
        || this.hasQueuedWork(target, this.sessionKey(target, binding), order, taskId)
        || paths.some((path) => this.deps.isDirectoryBlocked(path))
        || this.hasActiveResourceConflict(paths, roots, taskId)
        || this.hasExclusiveResourceConflict(paths, roots)
        || await this.hasConflictingRepositoryReservation(root, order, reservation)
        || await this.hasEarlierQueuedResourceUse(paths, roots, order, taskId)
        || await this.repositoryIsBlocked(root)
      ) {
        return undefined;
      }
      if (
        this.deps.stopping()
        || paths.some((path) => this.deps.isDirectoryBlocked(path))
        || this.hasActiveResourceConflict(paths, roots, taskId)
        || this.hasExclusiveResourceConflict(paths, roots)
      ) {
        return undefined;
      }
      resourceLease = this.reserveExclusiveResources(roots, paths, taskId);
      if (!resourceLease) return undefined;
      const lease: CommitLease = {
        root,
        taskId,
        order,
        ready: Promise.resolve(),
        complete: () => {
          resourceLease!.complete();
          reservation.release();
        },
      };
      acquiredLease = lease;
      transferred = true;
      if (
        await this.hasEarlierQueuedResourceUse(paths, roots, order, taskId)
        || this.deps.stopping()
        || this.hasActiveResourceConflict(paths, roots, taskId)
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
        resourceLease?.complete();
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
   * Session controls share actual path/root usage with ordinary workers so destructive operations
   * can drain them. They do not block independent ordinary tasks using the same repository.
   */
  tryAcquireCommandSession(
    target: ChatTarget,
    cwds: readonly string[],
  ): Promise<SessionLease | undefined> {
    return this.reserveSessionCommand(target, cwds).tryAcquire();
  }

  /**
   * Reserve a session command across asynchronous ingress preparation without turning its cwd
   * snapshots into exclusive locks. Once acquired, its paths are registered as shared use so later
   * destructive operations still wait for or refuse the candidate worker.
   * An uncertain-outcome hold remains a conflicting reservation but does not delay a later safety
   * probe that can only be refused.
   */
  reserveSessionCommand(
    target: ChatTarget,
    cwds: readonly string[],
    orderHint?: number,
    uncertainOutcomeHold = false,
  ): SessionCommandReservation {
    const binding = this.bindingFor(target);
    let sessionKey = this.sessionKey(target, binding);
    const reservation = this.createCommandReservationRecord(
      target,
      [],
      false,
      orderHint,
      undefined,
      undefined,
      uncertainOutcomeHold,
    );
    const currentSessionKey = (): string => {
      const current = this.sessionKey(target, this.bindingFor(target));
      if (current !== sessionKey) {
        sessionKey = current;
        reservation.sessionKeys = this.leaseKeys(target, current);
      }
      return sessionKey;
    };
    let resourceCwds = [...new Set(cwds)];
    let acquisitionStarted = false;
    return {
      order: reservation.order,
      extend: (additionalCwds) => {
        if (acquisitionStarted || !this.commandReservations.has(reservation)) {
          throw new Error("session reservation resources must be extended before acquisition");
        }
        resourceCwds = [...new Set([...resourceCwds, ...additionalCwds])];
      },
      bypassUncertainPriorOrders: orders => {
        if (acquisitionStarted || !this.commandReservations.has(reservation)) return false;
        const unique = new Set(orders);
        if (unique.size === 0) return false;
        const holds = [...this.commandReservations].filter(candidate =>
          unique.has(candidate.order)
          && candidate.order < reservation.order
          && candidate.uncertainOutcomeHold
          && this.reservationConflicts(candidate, reservation.target, currentSessionKey()),
        );
        if (holds.length !== unique.size) return false;
        for (const order of unique) reservation.bypassedUncertainOrders.add(order);
        return true;
      },
      waitForPrior: signal => {
        acquisitionStarted = true;
        return this.waitForPriorSessionWork(reservation, currentSessionKey, signal, true);
      },
      tryAcquire: () => {
        acquisitionStarted = true;
        return this.tryAcquireReservedSessionCommand(reservation, currentSessionKey(), resourceCwds);
      },
      release: () => reservation.release(),
    };
  }

  private async tryAcquireReservedSessionCommand(
    reservation: CommandReservationRecord,
    sessionKey: string,
    cwds: readonly string[],
  ): Promise<SessionLease | undefined> {
    if (!this.commandReservations.has(reservation)) return undefined;
    const target = reservation.target;
    const paths = this.resourcePaths(cwds);
    let sessionLease: SessionLease | undefined;
    let transferred = false;
    const unavailable = (roots: ReadonlySet<string>): boolean =>
      this.deps.stopping()
      || !this.commandReservations.has(reservation)
      || this.sessionKey(target, this.bindingFor(target)) !== sessionKey
      || this.hasConflictingReservation(target, sessionKey, reservation.order, reservation)
      || this.hasSessionLeaseFor(target, sessionKey)
      || this.hasQueuedWork(target, sessionKey, reservation.order)
      || paths.some((path) => this.deps.isDirectoryBlocked(path))
      || this.hasExclusiveResourceConflict(paths, roots);
    try {
      const noRoots = new Set<string>();
      if (
        unavailable(noRoots)
        || await this.hasConflictingResourceReservation(paths, noRoots, reservation.order, reservation)
        || unavailable(noRoots)
      ) {
        return undefined;
      }
      const roots = await this.repositoryRoots(cwds);
      if (
        unavailable(roots)
        || await this.hasConflictingResourceReservation(paths, roots, reservation.order, reservation)
      ) {
        return undefined;
      }
      for (const root of roots) {
        if (await this.repositoryIsBlocked(root)) return undefined;
      }
      if (unavailable(roots)) return undefined;

      sessionLease = this.createSessionLease(target, sessionKey);
      const owner = `session-command:${targetKey(target)}:${reservation.order}`;
      const usage: ActiveResourceUse = { paths, roots };
      this.activeResourceUsers.set(owner, usage);
      transferred = true;
      let completed = false;
      return {
        ready: sessionLease.ready,
        complete: () => {
          if (completed) return;
          completed = true;
          if (this.activeResourceUsers.get(owner) === usage) this.activeResourceUsers.delete(owner);
          sessionLease!.complete();
          reservation.release();
        },
      };
    } catch {
      return undefined;
    } finally {
      if (!transferred) {
        sessionLease?.complete();
        reservation.release();
      }
    }
  }

  /** A FIFO chat/session barrier with no cwd or repository ownership. */
  reserveSessionBarrier(target: ChatTarget): CommandSessionBarrier {
    const binding = this.bindingFor(target);
    const sessionKey = this.sessionKey(target, binding);
    const reservation = this.createCommandReservationRecord(target, []);
    return {
      waitForPrior: signal => this.waitForPriorSessionWork(reservation, sessionKey, signal),
      release: () => reservation.release(),
    };
  }

  hasChatWork(target: ChatTarget): boolean {
    const key = targetKey(target);
    const binding = this.bindingFor(target);
    const sessionKeys = new Set(this.leaseKeys(target, this.sessionKey(target, binding)));
    return this.chatTails.has(key)
      || this.deps.store.listQueuedTasks().some(task => targetKey(taskTarget(task)) === key)
      || [...this.commandReservations].some(reservation =>
        reservation.sessionKeys.some(sessionKey => sessionKeys.has(sessionKey)),
      );
  }

  /**
   * Keep a shared source cwd/root in use while an independent fork preparation runs. Unlike an
   * ordinary session acquisition this neither waits for nor rejects other shared users; it only
   * refuses blocked or destructive ownership and publishes its paths after all async probes.
   */
  async tryAcquireSharedResources(
    owner: string,
    cwds: readonly string[],
  ): Promise<DestructiveResourceLease | undefined> {
    const paths = this.resourcePaths(cwds);
    if (
      owner.length === 0
      || paths.length === 0
      || this.deps.stopping()
      || this.activeResourceUsers.has(owner)
      || paths.some(path => this.deps.isDirectoryBlocked(path))
      || this.hasExclusiveResourceConflict(paths, new Set<string>())
    ) {
      return undefined;
    }
    try {
      const roots = await this.repositoryRoots(cwds);
      const unavailable = (): boolean =>
        this.deps.stopping()
        || paths.some(path => this.deps.isDirectoryBlocked(path))
        || this.hasExclusiveResourceConflict(paths, roots);
      if (
        unavailable()
        || await this.hasConflictingResourceReservation(
          paths,
          roots,
          Number.POSITIVE_INFINITY,
        )
        || await this.repositoriesAreBlocked(roots)
        || await this.hasConflictingResourceReservation(
          paths,
          roots,
          Number.POSITIVE_INFINITY,
        )
        || unavailable()
      ) {
        return undefined;
      }
      const usage: ActiveResourceUse = { paths, roots };
      this.activeResourceUsers.set(owner, usage);
      let completed = false;
      return {
        ready: Promise.resolve(),
        complete: () => {
          if (completed) return;
          completed = true;
          if (this.activeResourceUsers.get(owner) === usage) this.activeResourceUsers.delete(owner);
        },
      };
    } catch {
      return undefined;
    }
  }

  /** Acquire a session lease and exclusive resource lease for local destructive chat commands. */
  async tryAcquireDestructiveCommandSession(
    target: ChatTarget,
    repositoryCwds: readonly string[],
    resourcePaths: readonly string[],
  ): Promise<SessionLease | undefined> {
    const reservation = this.createCommandReservationRecord(
      target,
      resourcePaths,
      true,
      undefined,
      undefined,
      repositoryCwds,
    );
    let transferred = false;
    try {
      const lease = await this.tryAcquireReservedCommandSession(reservation);
      if (!lease) return undefined;
      transferred = true;
      return {
        ready: lease.ready,
        complete: () => {
          lease.complete();
          reservation.release();
        },
      };
    } catch {
      return undefined;
    } finally {
      if (!transferred) reservation.release();
    }
  }

  /**
   * Install an ordered session barrier; non-empty cwd lists also protect their paths and repository roots.
   */
  reserveDestructiveCommandSession(
    target: ChatTarget,
    cwds: readonly string[],
    repositoryCwds: readonly string[] = cwds,
    orderHint?: number,
  ): DestructiveCommandReservation {
    const reservation = this.createCommandReservationRecord(
      target,
      cwds,
      true,
      orderHint,
      undefined,
      repositoryCwds,
    );
    let acquisitionStarted = false;
    return {
      order: reservation.order,
      extend: (additionalCwds, additionalRepositoryCwds = additionalCwds) => {
        if (acquisitionStarted || !this.commandReservations.has(reservation)) {
          throw new Error("destructive reservation resources must be extended before acquisition");
        }
        reservation.resourcePaths = [...new Set([...reservation.resourcePaths, ...additionalCwds])];
        reservation.repositoryCwds = [...new Set([...reservation.repositoryCwds, ...additionalRepositoryCwds])];
      },
      tryAcquire: () => {
        acquisitionStarted = true;
        return this.tryAcquireReservedCommandSession(reservation);
      },
      release: () => reservation.release(),
    };
  }

  private createCommandReservationRecord(
    target: ChatTarget,
    cwds: readonly string[],
    destructive = false,
    order?: number,
    ownerTaskId?: string,
    repositoryCwds?: readonly string[],
    uncertainOutcomeHold = false,
  ): CommandReservationRecord {
    const binding = this.bindingFor(target);
    const sessionKey = this.sessionKey(target, binding);
    const completion = Promise.withResolvers<void>();
    const resolvedOrder = order ?? this.deps.store.allocateScheduleOrder();
    let reservation!: CommandReservationRecord;
    reservation = {
      order: resolvedOrder,
      target: { ...target },
      sessionKeys: this.leaseKeys(target, sessionKey),
      resourcePaths: destructive ? [...new Set(cwds)] : [],
      repositoryCwds: destructive ? [...new Set(repositoryCwds ?? cwds)] : [],
      ...(ownerTaskId === undefined ? {} : { ownerTaskId }),
      uncertainOutcomeHold,
      bypassedUncertainOrders: new Set<number>(),
      ready: completion.promise,
      release: () => {
        if (!this.commandReservations.delete(reservation)) return;
        completion.resolve();
      },
    };
    this.commandReservations.add(reservation);
    return reservation;
  }

  /** Promote a host operation to exclusive ownership of its exact paths and canonical roots. */
  async tryAcquireDestructiveResources(
    cwds: readonly string[],
    task: TaskRecord,
  ): Promise<DestructiveResourceLease | undefined> {
    const target = taskTarget(task);
    const sessionKey = this.sessionKey(target, this.bindingFor(target));
    const reservation = this.createCommandReservationRecord(target, cwds, true, undefined, task.id, []);
    const order = reservation.order;
    const paths = this.resourcePaths(cwds);
    let resourceLease: DestructiveResourceLease | undefined;
    let transferred = false;
    try {
      const repositoryCwds: string[] = [];
      for (const cwd of cwds) {
        try {
          await lstat(cwd);
          repositoryCwds.push(cwd);
        } catch (error) {
          if ((error as NodeJS.ErrnoException).code !== "ENOENT") throw error;
        }
      }
      reservation.repositoryCwds = repositoryCwds;
      const roots = await this.repositoryRoots(repositoryCwds);
      const unavailable = (): boolean =>
        this.deps.stopping()
        || paths.some((path) => this.deps.isDirectoryBlocked(path))
        || this.hasActiveResourceConflict(paths, roots, task.id)
        || this.hasExclusiveResourceConflict(paths, roots)
        || this.hasQueuedWork(target, sessionKey, order, task.id);
      if (
        unavailable()
        || this.hasConflictingReservation(target, sessionKey, order, reservation, task.id)
        || await this.hasConflictingResourceReservation(paths, roots, order, reservation, task.id)
        || await this.hasEarlierQueuedResourceUse(paths, roots, order, task.id)
        || await this.repositoriesAreBlocked(roots)
        || unavailable()
      ) {
        return undefined;
      }
      resourceLease = this.reserveExclusiveResources(roots, paths, task.id);
      if (!resourceLease) return undefined;
      if (
        await this.hasEarlierQueuedResourceUse(paths, roots, order, task.id)
        || await this.repositoriesAreBlocked(roots)
      ) {
        return undefined;
      }
      if (
        this.deps.stopping()
        || paths.some((path) => this.deps.isDirectoryBlocked(path))
        || this.hasActiveResourceConflict(paths, roots, task.id)
        || this.hasQueuedWork(target, sessionKey, order, task.id)
      ) {
        return undefined;
      }
      transferred = true;
      return {
        ready: Promise.resolve(),
        complete: () => {
          resourceLease!.complete();
          reservation.release();
        },
      };
    } catch {
      return undefined;
    } finally {
      if (!transferred) {
        resourceLease?.complete();
        reservation.release();
      }
    }
  }
  private bindingFor(target: ChatTarget): BindingRecord | null {
    const botUid = this.deps.botUid();
    return botUid === undefined ? null : this.deps.store.getBinding(botUid, target);
  }

  private hasActiveResourceConflict(
    paths: readonly string[],
    roots: ReadonlySet<string>,
    excludedOwner?: string,
  ): boolean {
    for (const [owner, usage] of this.activeResourceUsers) {
      if (owner === excludedOwner) continue;
      if ([...roots].some((root) => usage.roots.has(root))) return true;
      if (
        paths.some((path) =>
          usage.paths.some((activePath) => resourcePathsRelated(path, activePath))
        )
      ) {
        return true;
      }
    }
    return false;
  }

  private hasExclusiveResourceConflict(
    paths: readonly string[],
    roots: ReadonlySet<string>,
  ): boolean {
    for (const root of roots) {
      if (this.exclusiveRepositoryRoots.has(root)) return true;
      for (const heldRoot of this.exclusiveRepositoryRoots.keys()) {
        if (resourcePathsRelated(root, heldRoot)) return true;
      }
      for (const heldPath of this.exclusiveResourcePaths.keys()) {
        if (resourcePathsRelated(root, heldPath)) return true;
      }
    }
    for (const path of paths) {
      for (const heldRoot of this.exclusiveRepositoryRoots.keys()) {
        if (resourcePathsRelated(path, heldRoot)) return true;
      }
      for (const heldPath of this.exclusiveResourcePaths.keys()) {
        if (resourcePathsRelated(path, heldPath)) return true;
      }
    }
    return false;
  }

  private hasExclusivePathConflict(cwd: string): boolean {
    return this.hasExclusiveResourceConflict(this.resourcePaths([cwd]), new Set<string>());
  }

  private exclusiveResourceBlocker(
    cwd: string,
    root: string | null,
    excludedTaskId: string,
  ): { owner: string; done: Promise<void> } | undefined {
    if (root !== null) {
      const reservation = this.exclusiveRepositoryRoots.get(root);
      if (reservation && reservation.owner !== excludedTaskId) return reservation;
    }
    const path = resolve(cwd);
    for (const [heldRoot, reservation] of this.exclusiveRepositoryRoots) {
      if (
        reservation.owner !== excludedTaskId
        && resourcePathsRelated(path, heldRoot)
      ) {
        return reservation;
      }
    }
    for (const [heldPath, reservation] of this.exclusiveResourcePaths) {
      if (
        reservation.owner !== excludedTaskId
        && resourcePathsRelated(path, heldPath)
      ) {
        return reservation;
      }
    }
    return undefined;
  }

  private resourcePaths(cwds: readonly string[]): string[] {
    return [...new Set(cwds.map((cwd) => resolve(cwd)))].sort();
  }

  private async repositoryRoots(cwds: readonly string[]): Promise<Set<string>> {
    const roots = new Set<string>();
    for (const cwd of new Set(cwds)) {
      const root = await this.deps.repositoryRoot(cwd);
      if (root !== null) roots.add(root);
    }
    return roots;
  }

  private async repositoriesAreBlocked(roots: ReadonlySet<string>): Promise<boolean> {
    for (const root of roots) {
      if (await this.repositoryIsBlocked(root)) return true;
    }
    return false;
  }

  private repositoryIsBlocked(root: string): Promise<boolean> {
    return this.deps.repositoryBlocked(root);
  }

  private reserveExclusiveResources(
    roots: ReadonlySet<string>,
    paths: readonly string[],
    owner: string,
  ): DestructiveResourceLease | undefined {
    if (this.hasExclusiveResourceConflict(paths, roots)) return undefined;
    const rootEntries = [...roots].map((root) => {
      const completion = Promise.withResolvers<void>();
      const entry = {
        owner,
        done: completion.promise,
        complete: completion.resolve,
      };
      this.exclusiveRepositoryRoots.set(root, entry);
      return [root, entry] as const;
    });
    const pathEntries = paths.map((path) => {
      const completion = Promise.withResolvers<void>();
      const entry = {
        owner,
        done: completion.promise,
        complete: completion.resolve,
      };
      this.exclusiveResourcePaths.set(path, entry);
      return [path, entry] as const;
    });
    let completed = false;
    return {
      ready: Promise.resolve(),
      complete: () => {
        if (completed) return;
        completed = true;
        for (const [root, entry] of rootEntries) {
          if (this.exclusiveRepositoryRoots.get(root) === entry) {
            this.exclusiveRepositoryRoots.delete(root);
          }
          entry.complete();
        }
        for (const [key, entry] of pathEntries) {
          if (this.exclusiveResourcePaths.get(key) === entry) {
            this.exclusiveResourcePaths.delete(key);
          }
          entry.complete();
        }
      },
    };
  }

  /** Display text alone never authorizes a native commit. */
  private isCommitTask(task: TaskRecord): boolean {
    return task.text === "/commit" && this.deps.store.isCommitTask(task.id);
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
  ): boolean {
    const sessionKeys = new Set(this.leaseKeys(target, sessionKey));
    return reservation.sessionKeys.some((key) => sessionKeys.has(key));
  }

  private hasConflictingReservation(
    target: ChatTarget,
    sessionKey: string,
    beforeOrder = Number.POSITIVE_INFINITY,
    excluded?: CommandReservationRecord,
    excludedOwnerTaskId?: string,
  ): boolean {
    return [...this.commandReservations].some((reservation) =>
      reservation !== excluded
      && (excludedOwnerTaskId === undefined || reservation.ownerTaskId !== excludedOwnerTaskId)
      && !(reservation.uncertainOutcomeHold && excluded?.bypassedUncertainOrders.has(reservation.order))
      && reservation.order < beforeOrder
      && this.reservationConflicts(reservation, target, sessionKey)
    );
  }

  private async hasConflictingRepositoryReservation(
    root: string,
    beforeOrder: number,
    excluded?: CommandReservationRecord,
  ): Promise<boolean> {
    for (const reservation of this.commandReservations) {
      if (
        reservation === excluded
        || reservation.order >= beforeOrder
        || reservation.resourcePaths.length === 0
      ) {
        continue;
      }
      for (const path of reservation.resourcePaths) {
        if (resourcePathsRelated(root, path)) return true;
      }
      for (const cwd of reservation.repositoryCwds) {
        try {
          if (await this.deps.repositoryRoot(cwd) === root) return true;
        } catch {
          // An earlier unresolved destructive root is a barrier, not permission to write.
          return true;
        }
      }
    }
    return false;
  }


  private async hasConflictingResourceReservation(
    paths: readonly string[],
    roots: ReadonlySet<string>,
    beforeOrder: number,
    excluded?: CommandReservationRecord,
    excludedOwnerTaskId?: string,
  ): Promise<boolean> {
    for (const reservation of this.commandReservations) {
      if (
        reservation === excluded
        || (excludedOwnerTaskId !== undefined && reservation.ownerTaskId === excludedOwnerTaskId)
        || reservation.order >= beforeOrder
        || reservation.resourcePaths.length === 0
      ) {
        continue;
      }
      if (
        paths.some((path) =>
          reservation.resourcePaths.some((cwd) => resourcePathsRelated(path, cwd))
        )
      ) {
        return true;
      }
      for (const cwd of reservation.repositoryCwds) {
        try {
          const root = await this.deps.repositoryRoot(cwd);
          if (root !== null && roots.has(root)) return true;
        } catch {
          // An earlier unresolved destructive root is a barrier, not permission to write.
          return true;
        }
      }
    }
    return false;
  }

  private async hasEarlierQueuedResourceUse(
    paths: readonly string[],
    roots: ReadonlySet<string>,
    beforeOrder: number,
    excludedTaskId?: string,
  ): Promise<boolean> {
    for (const task of this.deps.store.listQueuedTasks()) {
      if (task.id === excludedTaskId) continue;
      const order = this.taskOrder(task.id);
      if (order >= beforeOrder) continue;
      try {
        const target = taskTarget(task);
        const binding = await this.deps.reconcileBinding(
          task.id,
          target,
          this.bindingFor(target),
        );
        const current = this.deps.store.getTask(task.id);
        if (!current || current.state !== "queued") continue;
        const cwd = binding?.cwd ?? current.cwd;
        if (paths.some((path) => resourcePathsRelated(path, cwd))) return true;
        const taskRoot = await this.deps.repositoryRoot(cwd);
        if (taskRoot !== null && roots.has(taskRoot)) return true;
      } catch {
        return true;
      }
    }
    return false;
  }

  private async firstBlockingReservation(
    target: ChatTarget,
    sessionKey: string,
    taskOrder: number,
    cwd?: string,
    skipRepositoryProbe = false,
  ): Promise<CommandReservationRecord | undefined> {
    const candidates = [...this.commandReservations]
      .filter((reservation) => reservation.order < taskOrder)
      .sort((left, right) => left.order - right.order);
    let taskRoot: string | null | undefined;
    for (const reservation of candidates) {
      if (this.reservationConflicts(reservation, target, sessionKey)) return reservation;
      if (cwd === undefined || reservation.resourcePaths.length === 0) continue;
      if (reservation.resourcePaths.some((path) => resourcePathsRelated(path, cwd))) {
        return reservation;
      }
      if (skipRepositoryProbe) continue;
      if (taskRoot === undefined) {
        try {
          taskRoot = await this.deps.repositoryRoot(cwd);
        } catch {
          return reservation;
        }
      }
      if (taskRoot === null) continue;
      for (const reservedCwd of reservation.repositoryCwds) {
        try {
          if (await this.deps.repositoryRoot(reservedCwd) === taskRoot) return reservation;
        } catch {
          return reservation;
        }
      }
    }
    return undefined;
  }

  private async waitForPriorReservations(
    target: ChatTarget,
    sessionKey: string,
    taskOrder: number,
    cwd?: string,
    skipRepositoryProbe = false,
  ): Promise<void> {
    while (!this.deps.stopping()) {
      const reservation = await this.firstBlockingReservation(
        target,
        sessionKey,
        taskOrder,
        cwd,
        skipRepositoryProbe,
      );
      if (!reservation) return;
      await reservation.ready;
    }
  }


  private async tryAcquireReservedCommandSession(
    reservation: CommandReservationRecord,
  ): Promise<SessionLease | undefined> {
    if (!this.commandReservations.has(reservation)) return undefined;
    const sessionKey = this.sessionKey(reservation.target, this.bindingFor(reservation.target));
    const paths = this.resourcePaths(reservation.resourcePaths);
    const hasResources = paths.length > 0 || reservation.repositoryCwds.length > 0;
    const locallyUnavailable = (roots: ReadonlySet<string>): boolean =>
      !this.commandReservations.has(reservation)
      || sessionKey !== this.sessionKey(reservation.target, this.bindingFor(reservation.target))
      || this.hasConflictingReservation(
        reservation.target,
        sessionKey,
        reservation.order,
        reservation,
      )
      || this.leaseKeys(reservation.target, sessionKey).some((key) => this.sessionTails.has(key))
      || this.hasQueuedWork(reservation.target, sessionKey, reservation.order)
      || paths.some((path) => this.deps.isDirectoryBlocked(path))
      || this.hasActiveResourceConflict(paths, roots)
      || this.hasExclusiveResourceConflict(paths, roots);
    const unavailableAfterReserve = (roots: ReadonlySet<string>): boolean =>
      !this.commandReservations.has(reservation)
      || sessionKey !== this.sessionKey(reservation.target, this.bindingFor(reservation.target))
      || this.hasConflictingReservation(
        reservation.target,
        sessionKey,
        reservation.order,
        reservation,
      )
      || this.leaseKeys(reservation.target, sessionKey).some((key) => this.sessionTails.has(key))
      || this.hasQueuedWork(reservation.target, sessionKey, reservation.order)
      || paths.some((path) => this.deps.isDirectoryBlocked(path))
      || this.hasActiveResourceConflict(paths, roots);
    const roots = hasResources
      ? await this.repositoryRoots(reservation.repositoryCwds)
      : new Set<string>();
    if (locallyUnavailable(roots)) return undefined;
    if (
      hasResources
      && await this.hasConflictingResourceReservation(paths, roots, reservation.order, reservation)
    ) {
      return undefined;
    }
    if (hasResources && await this.hasEarlierQueuedResourceUse(paths, roots, reservation.order)) {
      return undefined;
    }
    if (hasResources && await this.repositoriesAreBlocked(roots)) {
      throw new Error("该 Git 工作树仍有无法确认停止的 worker，无法安全合并。");
    }
    if (locallyUnavailable(roots)) return undefined;

    const resourceLease = hasResources
      ? this.reserveExclusiveResources(
        roots,
        paths,
        `destructive:${targetKey(reservation.target)}:${reservation.order}`,
      )
      : undefined;
    if (hasResources && !resourceLease) return undefined;
    let transferred = false;
    try {
      if (
        (
          hasResources
          && (
            await this.hasEarlierQueuedResourceUse(paths, roots, reservation.order)
            || await this.repositoriesAreBlocked(roots)
          )
        )
        || unavailableAfterReserve(roots)
      ) {
        return undefined;
      }
      const sessionLease = this.createSessionLease(reservation.target, sessionKey);
      let completed = false;
      transferred = true;
      return {
        ready: sessionLease.ready,
        complete: () => {
          if (completed) return;
          completed = true;
          sessionLease.complete();
          resourceLease?.complete();
        },
      };
    } finally {
      if (!transferred) resourceLease?.complete();
    }
  }

  private hasSessionLeaseFor(
    target: ChatTarget,
    sessionKey: string,
    excludedTaskId?: string,
  ): boolean {
    const keys = this.leaseKeys(target, sessionKey);
    for (const key of keys) {
      const owners = this.sessionLeaseOwners.get(key);
      if (owners === undefined) {
        if (this.sessionTails.has(key)) return true;
        continue;
      }
      for (const owner of owners) {
        if (owner.taskId !== excludedTaskId) return true;
      }
    }
    return false;
  }

  private hasLeaseFor(target: ChatTarget, sessionKey: string): boolean {
    if (this.chatTails.has(targetKey(target))) return true;
    return this.leaseKeys(target, sessionKey).some((key) => this.sessionTails.has(key));
  }

  private hasQueuedWork(
    target: ChatTarget,
    sessionKey: string,
    beforeOrder?: number,
    excludedTaskId?: string,
  ): boolean {
    const chatKey = targetKey(target);
    return this.deps.store.listQueuedTasks().some((task) => {
      if (task.id === excludedTaskId) return false;
      const order = this.taskOrder(task.id);
      if (beforeOrder !== undefined && order >= beforeOrder) return false;
      const queuedTarget = taskTarget(task);
      return targetKey(queuedTarget) === chatKey
        || this.sessionKey(queuedTarget, this.bindingFor(queuedTarget)) === sessionKey;
    });
  }

  private async waitForPriorSessionWork(
    reservation: CommandReservationRecord,
    sessionKey: string | (() => string),
    signal: AbortSignal,
    refuseOnUncertainHold = false,
  ): Promise<void> {
    while (this.commandReservations.has(reservation)) {
      if (this.deps.stopping()) throw new Error("channel stopping");
      if (signal.aborted) throw signal.reason instanceof Error ? signal.reason : new Error("operation aborted");
      const currentSessionKey = typeof sessionKey === "function" ? sessionKey() : sessionKey;
      const earlierReservations = [...this.commandReservations]
        .filter(candidate =>
          candidate !== reservation
          && candidate.order < reservation.order
          && !(candidate.uncertainOutcomeHold && reservation.bypassedUncertainOrders.has(candidate.order))
          && this.reservationConflicts(candidate, reservation.target, currentSessionKey),
        );
      if (refuseOnUncertainHold && earlierReservations.some(candidate => candidate.uncertainOutcomeHold)) return;
      if (earlierReservations.length > 0) {
        await waitForAbort(Promise.all(earlierReservations.map(candidate => candidate.ready)).then(() => {}), signal);
        continue;
      }
      const earlier = [...this.scheduledTasks.entries()]
        .filter(([taskId]) => this.taskOrder(taskId) < reservation.order)
        .filter(([taskId]) => {
          const task = this.deps.store.getTask(taskId);
          if (!task) return false;
          const target = taskTarget(task);
          return this.reservationConflicts(
            reservation,
            target,
            this.sessionKey(target, this.bindingFor(target)),
          );
        })
        .map(([, operation]) => operation);
      if (earlier.length > 0) {
        await waitForAbort(Promise.allSettled(earlier).then(() => {}), signal);
        continue;
      }
      if (!this.hasQueuedWork(reservation.target, currentSessionKey, reservation.order)) return;
      await abortableDelay(50, signal);
    }
    throw new Error("session barrier was released");
  }


  private createSessionLease(
    target: ChatTarget,
    sessionKey: string,
    ownerTaskId?: string,
  ): SessionLease {
    const keys = this.leaseKeys(target, sessionKey);
    const previous = keys.flatMap((key) => {
      const tail = this.sessionTails.get(key);
      return tail === undefined ? [] : [tail];
    });
    const completion = Promise.withResolvers<void>();
    const lease = completion.promise;
    const owner: SessionLeaseOwner = ownerTaskId === undefined ? {} : { taskId: ownerTaskId };
    for (const key of keys) {
      this.sessionTails.set(key, lease);
      const owners = this.sessionLeaseOwners.get(key) ?? new Set<SessionLeaseOwner>();
      owners.add(owner);
      this.sessionLeaseOwners.set(key, owners);
    }
    let complete = false;
    return {
      ready: Promise.all(previous.map((tail) => tail.catch(() => {}))).then(() => {}),
      complete: () => {
        if (complete) return;
        complete = true;
        for (const key of keys) {
          if (this.sessionTails.get(key) === lease) this.sessionTails.delete(key);
          const owners = this.sessionLeaseOwners.get(key);
          owners?.delete(owner);
          if (owners?.size === 0) this.sessionLeaseOwners.delete(key);
        }
        completion.resolve();
      },
    };
  }


  /** Commit-related tasks preserve repository order; ordinary tasks only serialize by session. */
  private async earlierOperations(
    task: TaskRecord,
    sessionKey: string,
    skipRepositoryOrdering = false,
  ): Promise<Promise<void>[]> {
    const queued = this.deps.store.listQueuedTasks();
    if (!queued.some((candidate) => candidate.id === task.id)) return [];
    const orderByTaskId = new Map<string, number>();
    for (const candidate of queued) {
      const order = this.taskOrder(candidate.id);
      orderByTaskId.set(candidate.id, order);
    }
    const taskOrder = orderByTaskId.get(task.id);
    if (taskOrder === undefined) throw new Error(`queued task ${task.id} has no durable schedule order`);
    const earlierTasks = queued.filter(candidate =>
      candidate.id !== task.id
      && orderByTaskId.get(candidate.id)! < taskOrder,
    );
    const currentIsCommit = !skipRepositoryOrdering && this.isCommitTask(task);
    const commitOrderingRequired = currentIsCommit || (
      !skipRepositoryOrdering
      && earlierTasks.some((candidate) => this.isCommitTask(candidate))
    );
    const currentBinding = this.bindingFor(taskTarget(task));
    const currentRoot = commitOrderingRequired
      ? await this.deps.repositoryRoot(currentBinding?.cwd ?? task.cwd)
      : null;
    const operations: Promise<void>[] = [];
    for (const earlier of earlierTasks) {
      const target = taskTarget(earlier);
      const binding = await this.deps.reconcileBinding(
        earlier.id,
        target,
        this.bindingFor(target),
      );
      const earlierCwd = binding?.cwd ?? earlier.cwd;
      const sharesSession = this.sessionKey(target, binding) === sessionKey;
      const earlierIsCommit = !skipRepositoryOrdering && this.isCommitTask(earlier);
      const sharesRepository = !skipRepositoryOrdering
        && (currentIsCommit || earlierIsCommit)
        && currentRoot !== null
        && await this.deps.repositoryRoot(earlierCwd) === currentRoot;
      if (!sharesSession && !sharesRepository) continue;
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
        const taskOrder = this.taskOrder(taskId);
        let skipRepositoryOrdering = false;
        try {
          if (!commitTask) {
            skipRepositoryOrdering =
              await this.deps.shouldRunTaskBeforeRepositoryProbe(taskId, target, binding, cwd);
          }
          await this.waitForPriorReservations(
            target,
            sessionKey,
            taskOrder,
            cwd,
            skipRepositoryOrdering,
          );
          if (this.deps.stopping()) return;
          const earlier = await this.earlierOperations(
            reconciledCurrent,
            sessionKey,
            skipRepositoryOrdering,
          );
          await Promise.all(earlier.map((operation) => operation.catch(() => {})));
          await this.waitForPriorReservations(
            target,
            sessionKey,
            taskOrder,
            cwd,
            skipRepositoryOrdering,
          );
          if (this.deps.stopping()) return;
        } catch {
          await this.failRepositoryProbe(taskId, target);
          return;
        }

        const beforeLease = this.deps.store.getTask(taskId);
        if (this.deps.stopping() || !beforeLease || beforeLease.state !== "queued") return;
        const beforeTarget = taskTarget(beforeLease);
        lease = this.createSessionLease(beforeTarget, sessionKey, taskId);
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
            // Keep accepted work queued while an unconfirmed worker owns this Git worktree.
            if (blocked) return;
            activeRoot = root;
          }
        }
        const blocker = this.exclusiveResourceBlocker(currentCwd, activeRoot ?? null, taskId);
        if (blocker !== undefined) {
          lease.complete();
          lease = undefined;
          await blocker.done.catch(() => {});
          continue;
        }

        // Publish actual cwd and Git-root usage before yielding to another operation.
        const activeUse: ActiveResourceUse = {
          paths: this.resourcePaths([currentCwd]),
          roots: activeRoot === undefined ? new Set<string>() : new Set([activeRoot]),
        };
        this.activeResourceUsers.set(taskId, activeUse);
        try {
          await this.deps.runTask(taskId, currentCwd);
        } finally {
          if (activeRoot !== undefined && this.activeResourceUsers.get(taskId) === activeUse) {
            let blocked = true;
            try {
              blocked = await this.repositoryIsBlocked(activeRoot);
            } catch {
              // Keep unconfirmed activity until restart, when worker recovery rechecks it.
            }
            if (!blocked) this.activeResourceUsers.delete(taskId);
          } else if (!this.deps.isDirectoryBlocked(currentCwd)) {
            this.activeResourceUsers.delete(taskId);
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
export function resourcePathsRelated(left: string, right: string): boolean {
  const resolvedLeft = resolve(left);
  const resolvedRight = resolve(right);
  return pathContains(resolvedLeft, resolvedRight) || pathContains(resolvedRight, resolvedLeft);
}

function pathContains(parent: string, child: string): boolean {
  const relation = relative(parent, child);
  return relation === ""
    || (!isAbsolute(relation) && relation !== ".." && !relation.startsWith(`..${sep}`));
}
