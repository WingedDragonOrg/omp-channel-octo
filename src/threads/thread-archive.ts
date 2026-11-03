/**
 * 子区归档排定的所有者：`octo_management` 的 `thread-archive` 与默认 pipeline 的
 * `thread-merge` 收尾都只排定，真正的调用发生在该聊天回合输出（正文与进度卡终态编辑）排空
 * 之后——任何发进子区的消息都会把归档子区解档，所以归档必须等确认没有新消息再发。
 *
 * 模块不依赖 `service.ts`：出站队列的排空与失败说明入队（`outbox`）、待投递计数（`store`）、
 * 归档调用（`transport`）以及 bot 身份与停止态都由 `CoreService` 以端口注入；排定 map 与停止
 * 信号由本对象独占，外部只能经 `schedule`/`cancel`/`stop`/`run` 观察。
 */

import type { ChannelTransport, ChatTarget } from "../contracts.ts";
import { failureText, isTransientServerFailure } from "../errors.ts";
import type { Outbox } from "../outbox.ts";
import type { ChannelStore } from "../store/index.ts";
import { targetKey } from "../target.ts";

/** 排定的子区归档：仅瞬时失败重试（最多 5 次尝试、每次间隔 5 秒），永久失败即刻结束。 */
const THREAD_ARCHIVE_ATTEMPTS = 5;
const THREAD_ARCHIVE_RETRY_MS = 5_000;
/** 归档前按 100ms 轮询出站状态；重试退避不能被误当作队列排空。 */
const THREAD_ARCHIVE_DRAIN_POLL_MS = 100;

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
  readonly generation: number;
  /** Host-first merge requires this exact durable success receipt to reach `sent`. */
  readonly receiptOutboxId?: string;
}

/**
 * 本对象可以触达的外部状态：出站队列的排空与失败说明入队、该聊天的待投递计数、归档调用，
 * 以及 bot 身份与停止态——两个 getter 是因为 bot 在 start 之后才注册、停止态由服务持有。
 */
export interface ThreadArchiveDeps {
  readonly store: Pick<ChannelStore, "countPendingOutbox" | "getOutbox">;
  readonly transport: Pick<ChannelTransport, "archiveThread">;
  readonly outbox: Pick<Outbox, "flush" | "enqueueText">;
  readonly botUid: () => string | undefined;
  readonly stopping: () => boolean;
}

interface ActiveThreadArchive {
  readonly pending: PendingThreadArchive;
  readonly controller: AbortController;
  readonly operation: Promise<void>;
}

function pause(ms: number, signal: AbortSignal): Promise<void> {
  const completion = Promise.withResolvers<void>();
  const finish = (): void => {
    clearTimeout(timer);
    signal.removeEventListener("abort", finish);
    completion.resolve();
  };
  const timer = setTimeout(finish, ms);
  if (signal.aborted) finish();
  else signal.addEventListener("abort", finish, { once: true });
  return completion.promise;
}

export class ThreadArchiveScheduler {
  /**
   * 排定到回合末的子区归档（键为该聊天）。进程内一次性状态：执行或放弃即清除，该聊天过
   * 权限门禁的新入站也清除——用户回来了就不归档。
   */
  private readonly pendingThreadArchives = new Map<string, PendingThreadArchive>();
  private readonly generations = new Map<string, number>();
  /** One active runner per chat prevents the task-finally and host-first paths archiving twice. */
  private readonly activeRuns = new Map<string, ActiveThreadArchive>();

  constructor(private readonly deps: ThreadArchiveDeps) {}

  /** 当前聊天已授权入站代次；merge 等待期间发生的新输入会使旧计划失效。 */
  generation(target: ChatTarget): number {
    return this.generations.get(targetKey(target)) ?? 0;
  }

  /** 记下发起聊天快照；旧代的 merge 完成不得覆盖新入站对归档的取消。 */
  schedule(
    target: ChatTarget,
    groupId: string,
    shortId: string,
    generation = this.generation(target),
    receiptOutboxId?: string,
  ): void {
    const key = targetKey(target);
    if (generation !== this.generation(target)) return;
    this.pendingThreadArchives.set(key, {
      target: { ...target },
      groupId,
      shortId,
      generation,
      ...(receiptOutboxId === undefined ? {} : { receiptOutboxId }),
    });
    const active = this.activeRuns.get(key);
    if (active && active.pending !== this.pendingThreadArchives.get(key)) {
      active.controller.abort(new Error("thread archive plan replaced"));
    }
  }

  /** 已授权的新入站取消当前代次的计划与在途归档请求。 */
  cancel(target: ChatTarget): void {
    const key = targetKey(target);
    this.generations.set(key, this.generation(target) + 1);
    this.pendingThreadArchives.delete(key);
    this.activeRuns.get(key)?.controller.abort(new Error("thread archive cancelled by new inbound"));
  }

  /** 关停时中止在途归档与所有重试/投递等待。 */
  stop(): void {
    for (const active of this.activeRuns.values()) {
      active.controller.abort(new Error("channel stopping"));
    }
  }

  /**
   * 排定的子区归档收尾：先等本回合进度卡 tail 与出站排空；host-first merge 还必须确认其成功
   * 回执已进入 `sent`（`dead`、未知或缺行都 fail closed），再调用 archiveThread。
   */
  async run(target: ChatTarget, progressTail: Promise<void>): Promise<void> {
    const key = targetKey(target);
    const pending = this.pendingThreadArchives.get(key);
    if (!pending || this.deps.stopping()) return;
    const active = this.activeRuns.get(key);
    if (active) {
      if (active.pending === pending) {
        await active.operation;
        return;
      }
      active.controller.abort(new Error("thread archive plan replaced"));
      await active.operation.catch(() => {});
      return this.run(target, progressTail);
    }
    const controller = new AbortController();
    const operation = this.runPending(key, pending, progressTail, controller.signal);
    const current = { pending, controller, operation };
    this.activeRuns.set(key, current);
    try {
      await operation;
    } finally {
      if (this.activeRuns.get(key) === current) this.activeRuns.delete(key);
    }
  }

  private async runPending(
    key: string,
    pending: PendingThreadArchive,
    progressTail: Promise<void>,
    signal: AbortSignal,
  ): Promise<void> {
    try {
      await progressTail.catch(() => {});
      if (!this.isCurrent(key, pending, signal)) return;
      await this.drainChatOutbox(pending.target, signal);
      if (!this.isCurrent(key, pending, signal)) return;
      if (pending.receiptOutboxId !== undefined) {
        const delivered = await this.waitForReceipt(key, pending, signal);
        if (!delivered || !this.isCurrent(key, pending, signal)) return;
      }
      const outcome = await this.archivePendingThread(key, pending, signal);
      if (outcome === "archived") {
        console.error(`[thread-archive] 已归档子区 ${pending.shortId} target=${key}`);
      }
    } catch (error) {
      if (!this.isCurrent(key, pending, signal)) return;
      const reason = failureText(error, "归档失败");
      console.error(`[thread-archive] 归档失败 target=${key} short=${pending.shortId}：${reason}`);
      this.deps.outbox.enqueueText(pending.target, `归档失败：${reason}`, null);
    } finally {
      if (this.pendingThreadArchives.get(key) === pending) this.pendingThreadArchives.delete(key);
    }
  }

  private isCurrent(key: string, pending: PendingThreadArchive, signal: AbortSignal): boolean {
    return !signal.aborted
      && !this.deps.stopping()
      && this.pendingThreadArchives.get(key) === pending
      && this.generation(pending.target) === pending.generation;
  }

  /**
   * 一次排定归档的调用：第 1 次立即执行，瞬时失败（5xx 或 bot API 的 `store_failed`）每间隔
   * 5 秒重试一次、总共至多 5 次尝试；永久失败即刻结束。每次尝试前确认排定仍在（该聊天的新
   * 入站会取消它）且服务未在停止。
   */
  private async archivePendingThread(
    key: string,
    pending: PendingThreadArchive,
    signal: AbortSignal,
  ): Promise<"archived" | "cancelled"> {
    let attempt = 1;
    for (;;) {
      if (!this.isCurrent(key, pending, signal)) return "cancelled";
      try {
        await this.deps.transport.archiveThread(pending.groupId, pending.shortId, signal);
        return "archived";
      } catch (error) {
        if (!this.isCurrent(key, pending, signal)) return "cancelled";
        if (attempt >= THREAD_ARCHIVE_ATTEMPTS || !isTransientServerFailure(error)) throw error;
        attempt += 1;
        await pause(THREAD_ARCHIVE_RETRY_MS, signal);
      }
    }
  }

  /** 等所有待投递正文完成；停止或新授权入站会中止等待。 */
  private async drainChatOutbox(target: ChatTarget, signal: AbortSignal): Promise<void> {
    const botUid = this.deps.botUid();
    if (botUid === undefined) return;
    while (!signal.aborted && !this.deps.stopping()) {
      await this.deps.outbox.flush(target).catch(() => {});
      if (signal.aborted || this.deps.stopping()) return;
      if (this.deps.store.countPendingOutbox(botUid, target) === 0) return;
      await pause(THREAD_ARCHIVE_DRAIN_POLL_MS, signal);
    }
  }

  /** A permanent dead, missing, or ambiguous receipt is never evidence of successful delivery. */
  private async waitForReceipt(
    key: string,
    pending: PendingThreadArchive,
    signal: AbortSignal,
  ): Promise<boolean> {
    const botUid = this.deps.botUid();
    const receiptId = pending.receiptOutboxId;
    if (botUid === undefined || receiptId === undefined) return false;
    while (this.isCurrent(key, pending, signal)) {
      const receipt = this.deps.store.getOutbox(receiptId);
      if (!receipt) return false;
      if (receipt.state === "sent") return true;
      if (receipt.state === "dead") return false;
      await this.deps.outbox.flush(pending.target).catch(() => {});
      if (!this.isCurrent(key, pending, signal)) return false;
      await pause(THREAD_ARCHIVE_DRAIN_POLL_MS, signal);
    }
    return false;
  }
}
