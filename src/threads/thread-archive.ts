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
/** 归档前等该聊天出站队列排空的有界轮询（100ms 一次，最多 10 秒）。 */
const THREAD_ARCHIVE_DRAIN_MS = 10_000;
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
}

/**
 * 本对象可以触达的外部状态：出站队列的排空与失败说明入队、该聊天的待投递计数、子区归档调用，
 * 以及 bot 身份与停止态——两个 getter 是因为 bot 在 start 之后才注册、停止态由服务持有。
 */
export interface ThreadArchiveDeps {
  readonly store: Pick<ChannelStore, "countPendingOutbox">;
  readonly transport: Pick<ChannelTransport, "archiveThread">;
  readonly outbox: Pick<Outbox, "flush" | "enqueueText">;
  readonly botUid: () => string | undefined;
  readonly stopping: () => boolean;
}

export class ThreadArchiveScheduler {
  /**
   * 排定到回合末的子区归档（键为该聊天）。进程内一次性状态：执行或放弃即清除，该聊天过
   * 权限门禁的新入站也清除——用户回来了就不归档。
   */
  private readonly pendingThreadArchives = new Map<string, PendingThreadArchive>();
  /** 停止的到达信号：归档重试的等待据此提前结束，关停不为一轮重试等满一次间隔。 */
  private readonly stopped = Promise.withResolvers<void>();

  constructor(private readonly deps: ThreadArchiveDeps) {}

  /** 记下发起聊天的快照（不保留调用方对象），同一聊天重复排定按覆盖处理。 */
  schedule(target: ChatTarget, groupId: string, shortId: string): void {
    this.pendingThreadArchives.set(targetKey(target), { target: { ...target }, groupId, shortId });
  }

  /** 已授权的新入站说明这个聊天仍在继续：排定中的归档随之取消（用户回来了就不归档）。 */
  cancel(target: ChatTarget): void {
    this.pendingThreadArchives.delete(targetKey(target));
  }

  /**
   * 关停的到达信号：等待中的重试据此提前结束。排定本身不在这里清理——`run` 的停止检查与
   * 排定身份比较已经保证了停止后不会再执行归档。
   */
  stop(): void {
    this.stopped.resolve();
  }

  /**
   * 排定的子区归档收尾：先等本回合的进度卡终态编辑与出站投递排空——回复先送达、归档后执行，
   * 因为任何发进子区的消息都会把归档子区解档——再按重试策略调用 `archiveThread`。成功留一条
   * 日志；失败只给发起聊天一条 ≤200 字符的单行说明，不改变已经发生的合并；停止时放弃。
   */
  async run(target: ChatTarget, progressTail: Promise<void>): Promise<void> {
    const key = targetKey(target);
    const pending = this.pendingThreadArchives.get(key);
    if (!pending || this.deps.stopping()) return;
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
      this.deps.outbox.enqueueText(target, `归档失败：${reason}`, null);
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
      if (this.deps.stopping() || this.pendingThreadArchives.get(key) !== pending) return "cancelled";
      try {
        await this.deps.transport.archiveThread(pending.groupId, pending.shortId);
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
    const botUid = this.deps.botUid();
    if (botUid === undefined) return;
    for (let waited = 0; waited < THREAD_ARCHIVE_DRAIN_MS; waited += THREAD_ARCHIVE_DRAIN_POLL_MS) {
      await this.deps.outbox.flush(target).catch(() => {});
      if (this.deps.stopping()) return;
      if (this.deps.store.countPendingOutbox(botUid, target) === 0) return;
      await new Promise<void>((resolve) => {
        setTimeout(resolve, THREAD_ARCHIVE_DRAIN_POLL_MS);
      });
    }
  }
}
