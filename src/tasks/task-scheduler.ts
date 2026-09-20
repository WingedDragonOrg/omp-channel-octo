/**
 * 任务调度与目录租约的唯一所有者：每个聊天的任务串行链、已排定任务、关停要等的在途任务、
 * 排队等待任务的启动回执，以及按 cwd 的租约链都在这里。外部只能经 `scheduleTask`/
 * `hasBlockingWork`/`tryAcquireCommandDirectories`/`markWaiting`/`takeWaitingReceipt`/
 * `settle` 观察，本对象的 map 不外露。
 *
 * 模块不依赖 `service.ts`：任务行与绑定行的读取、绑定核对、真正的执行入口（`runTask`）、
 * bot 身份与停止态、运行中任务与 blocked 集合（`CoreService` 遍历自己的 runtimes），以及
 * 回执与终态通知的投递路径都按原样注入。运行中任务与 blocked 集合仍然只有一个所有者，调度
 * 侧的判定因此与绑定探测侧保持同一个语义。
 */

import type { ChatTarget } from "../contracts.ts";
import type { Outbox } from "../outbox.ts";
import type { ChannelStore } from "../store/index.ts";
import type { BindingRecord, TaskRecord } from "../store/store-records.ts";
import { GENERIC_TASK_FAILURE_NOTICE } from "./task-failure.ts";
import { targetKey, taskTarget } from "../target.ts";

/**
 * 按目录的租约：`/new`、`/move` 在命令体内占据一个或多个 cwd，`ready` 在它们都空闲后兑现，
 * `complete` 恰好释放一次。租约本体（按 cwd 的 Promise 链）由 `TaskScheduler` 持有，租约不
 * 在两个模块里各存一份。
 */
export type DirectoryLease = {
  ready: Promise<void>;
  complete(): void;
};

/**
 * 调度可以触及的外部状态：持久层里的任务行与绑定行、bot 身份（getter：start 之后才知道，
 * 任务只在接受后才会被排定）、停止态、进程内的运行中任务与 blocked 集合、绑定核对、真正的
 * 执行入口，以及启动回执与终态通知两条投递路径。
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
  /** 该 cwd 是否有进程内运行任务——`CoreService` 遍历自己的 runtimes。 */
  readonly hasRuntimeInDirectory: (cwd: string) => boolean;
  /** 该 cwd 是否因 worker 无法确认停止而被 block。 */
  readonly isDirectoryBlocked: (cwd: string) => boolean;
  /** 绑定行与磁盘会话文件的核对：任务启动前确认它的 cwd。 */
  readonly reconcileBinding: (
    target: ChatTarget,
    binding: BindingRecord | null,
  ) => Promise<BindingRecord | null>;
  /** 启动一个已排定任务；`cwd` 是调度已租下的目录。 */
  readonly runTask: (taskId: string, cwd?: string) => Promise<void>;
  /** 为该聊天排一条回执文本，与其他路径一致。 */
  readonly enqueueText: Outbox["enqueueText"];
  /** 任务终态的 relay 通知（子区回传）。 */
  readonly notifyThreadOutcome: (taskId: string) => Promise<void>;
}

export class TaskScheduler {
  /** 每个聊天的任务串行链：该聊天的下一个任务接在上一个（或已排定的任务）之后。 */
  private readonly chatTails = new Map<string, Promise<void>>();
  /** 已排定任务的操作：目录序靠前的任务据此等待。 */
  private readonly scheduledTasks = new Map<string, Promise<void>>();
  /** 关停要等的在途任务（已排定任务的操作）。 */
  private readonly taskOperations = new Set<Promise<void>>();
  /**
   * Tasks accepted while their chat or directory was already busy. They get one start receipt
   * when they finally begin, so a message that waited is distinguishable from one that started
   * right away. In-memory only: a restart interrupts such tasks instead of starting them.
   */
  private readonly waiting = new Set<string>();
  /** 按 cwd 的租约链：`/new`、`/move` 与任务启动共用的目录占用。 */
  private readonly directoryTails = new Map<string, Promise<void>>();

  constructor(private readonly deps: TaskSchedulerDeps) {}

  /** True when a new task on this chat cannot start at once: the chat or its directory is busy. */
  hasBlockingWork(target: ChatTarget, cwd: string): boolean {
    if (this.chatTails.has(targetKey(target))) return true;
    if (this.deps.store.listQueuedTasks().some((queued) => queued.cwd === cwd)) return true;
    return this.deps.hasRuntimeInDirectory(cwd);
  }

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

  /** 记下「这条任务在忙聊天或忙目录后面等过」：启动时取回，据此补一条启动回执。 */
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

  /** 为一次目录级命令租下全部给定 cwd，任一忙碌时返回 `undefined`；租约在命令的 finally 里释放。 */
  tryAcquireCommandDirectories(cwds: readonly string[]): DirectoryLease | undefined {
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

  /** 该 cwd 现在是否不可用于启动：被 block、已有租约、有运行中任务或有排队任务。 */
  private directoryUnavailable(cwd: string): boolean {
    if (this.deps.isDirectoryBlocked(cwd) || this.directoryTails.has(cwd)) return true;
    if (this.deps.hasRuntimeInDirectory(cwd)) return true;
    for (const task of this.deps.store.listQueuedTasks()) {
      if (task.cwd === cwd) return true;
    }
    return false;
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

  private async acquireDirectoryLease(cwd: string): Promise<DirectoryLease> {
    const lease = this.createDirectoryLease(cwd);
    await lease.ready;
    return lease;
  }

  /** 目录序在该任务之前、且核对后的 cwd 相同的已排定任务，供本次启动等待。 */
  private async earlierDirectoryOperations(task: TaskRecord, cwd: string): Promise<Promise<void>[]> {
    const queued = this.deps.store.listQueuedTasks();
    const currentIndex = queued.findIndex(candidate => candidate.id === task.id);
    if (currentIndex < 0) return [];
    const operations: Promise<void>[] = [];
    for (let index = 0; index < currentIndex; index += 1) {
      const earlier = queued[index];
      if (!earlier) continue;
      const binding = await this.deps.reconcileBinding(
        taskTarget(earlier),
        this.deps.store.getBinding(this.deps.botUid()!, taskTarget(earlier)),
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
        const current = this.deps.store.getTask(taskId);
        if (this.deps.stopping() || !current || current.state !== "queued") return;
        const binding = await this.deps.reconcileBinding(
          taskTarget(current),
          this.deps.store.getBinding(this.deps.botUid()!, taskTarget(current)),
        );
        const reconciledCurrent = this.deps.store.getTask(taskId);
        if (this.deps.stopping() || !reconciledCurrent || reconciledCurrent.state !== "queued") return;
        const cwd = binding?.cwd ?? reconciledCurrent.cwd;
        const earlier = await this.earlierDirectoryOperations(reconciledCurrent, cwd);
        await Promise.all(earlier.map(operation => operation.catch(() => {})));

        const beforeLease = this.deps.store.getTask(taskId);
        if (this.deps.stopping() || !beforeLease || beforeLease.state !== "queued") return;
        lease = await this.acquireDirectoryLease(cwd);

        const refreshed = await this.deps.reconcileBinding(
          taskTarget(beforeLease),
          this.deps.store.getBinding(this.deps.botUid()!, taskTarget(beforeLease)),
        );
        const latest = this.deps.store.getTask(taskId);
        if (this.deps.stopping() || !latest || latest.state !== "queued") return;
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
        await this.deps.runTask(taskId, cwd);
        return;
      }
      const task = this.deps.store.getTask(taskId);
      if (!task || task.state !== "queued") return;
      this.takeWaitingReceipt(taskId);
      this.deps.store.markFailed(taskId, "绑定目录在启动前持续变更，任务未启动。");
      await this.deps.enqueueText(taskTarget(task), GENERIC_TASK_FAILURE_NOTICE, taskId);
      await this.deps.notifyThreadOutcome(taskId);
    } finally {
      lease?.complete();
    }
  }
}
