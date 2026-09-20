/**
 * channel 重启的所有者：`/restart` 的校验、`channel_control` 的延期重启排定、延期重启在
 * channel idle 时的实际请求，以及旧版重启通报行的静默清理都在这里。
 *
 * 重启不再以中断任务为代价：在途回合由新进程按任务行的 prompt 标记续接，因此命令不再因忙碌
 * 被拒，也不再写「已重启完成 / 因重启中断」这类成功或中断通报——只留一条短回执。`pending`
 * 只读暴露给 host tools 的 channel 快照。
 *
 * 模块不依赖 `service.ts`：出站说明（`enqueueText`）、旧通知行的两个窄方法、在途任务视图
 * （`hasRunning`），以及宿主检测到的重启端口（`restart`，缺席即未检测到守护进程）都由
 * `CoreService` 以端口注入；端口缺席时命令只回一条说明，不猜重启机制。
 */

import type { ChatTarget, RestartPort } from "../contracts.ts";
import type { ChannelStore } from "../store/index.ts";

const RESTART_USAGE = "用法：/restart [now]。两种写法等价：重启后新进程会续接在途任务。";
const RESTART_UNAVAILABLE_TEXT = "未检测到守护进程，无法重启 channel。";
/** 唯一的一条重启回执：不做 PID/完成通报，任务续接由任务行自己保证。 */
const RESTART_ACK_TEXT = "正在重启 channel；在途任务会在重启后继续。";
const CHANNEL_RESTART_SCHEDULED_TEXT =
  "已排定重启：本回合结束后、channel 空闲时执行守护进程的重启。";

/**
 * 重启控制可以触达的外部状态：出站说明的入队、旧重启通知行的清理、在途任务视图（延期重启的
 * idle 判定），以及宿主注入的重启端口。
 */
export interface ServiceRestartDeps {
  readonly store: Pick<ChannelStore, "listQueuedTasks" | "listRestartNotices" | "clearRestartNotice">;
  readonly stopping: () => boolean;
  /** 在途任务视图：`CoreService` 的 `hasRunning` 判定。 */
  readonly hasRunning: () => boolean;
  /** 宿主检测到的进程守护端口；缺席表示这个部署无法重启自己。 */
  readonly restart: RestartPort | undefined;
  readonly enqueueText: (target: ChatTarget, text: string, taskId: string | null) => void;
}

export class ServiceRestart {
  /**
   * A restart requested through `channel_control`, kept until every chat is idle so the turn that
   * asked for it still delivers its reply.
   */
  private pendingRestart: ChatTarget | null = null;

  constructor(private readonly deps: ServiceRestartDeps) {}

  /** `channel_control` 是否排定了一次延期重启。 */
  get pending(): boolean {
    return this.pendingRestart !== null;
  }

  /**
   * `/restart` 的校验与执行：参数只接受空或 `now`；有在途任务时同样接受——重启会由新进程
   * 续接它们，不再有「先 /cancel 或 /restart now」的限制。通过校验回一条短回执，然后要求
   * 服务向守护进程请求重启。返回 true 表示服务应请求重启。端口缺席时报未检测到守护进程。
   */
  async handleCommand(target: ChatTarget, argument: string): Promise<boolean> {
    const restart = this.deps.restart;
    if (!restart) {
      await this.deps.enqueueText(target, RESTART_UNAVAILABLE_TEXT, null);
      return false;
    }
    const mode = argument.trim();
    if (mode.length > 0 && mode !== "now") {
      await this.deps.enqueueText(target, RESTART_USAGE, null);
      return false;
    }
    await this.deps.enqueueText(target, RESTART_ACK_TEXT, null);
    return true;
  }

  /** 重启请求：把宿主的请求原样交回调用方。 */
  request(): Promise<void> | undefined {
    return this.deps.restart?.request();
  }

  /** `channel_control` 的 restart：只排定，等 channel 空闲后由 `maybeRunDeferred` 真正请求。 */
  schedule(target: ChatTarget): string {
    if (!this.deps.restart) return RESTART_UNAVAILABLE_TEXT;
    this.pendingRestart = { channelType: target.channelType, channelId: target.channelId };
    return CHANNEL_RESTART_SCHEDULED_TEXT;
  }

  /**
   * Runs a restart the agent asked for once it cannot cost work: every chat idle and no queued
   * task. The request is fire-and-forget, because the supervisor stops this process.
   */
  maybeRunDeferred(): void {
    if (this.pendingRestart === null || this.deps.stopping()) return;
    if (this.deps.hasRunning() || this.deps.store.listQueuedTasks().length > 0) return;
    this.pendingRestart = null;
    const restart = this.deps.restart;
    if (!restart) return;
    void restart.request().catch((error: unknown) => {
      console.error(
        `[restart] 重启命令执行失败：${error instanceof Error ? error.message : "unknown error"}`,
      );
    });
  }

  /**
   * 旧版重启通报的遗留行：启动时静默清掉，绝不再投递。任务续接已由 `prompt_started` /
   * `prompt_accepted` 标记与 `prepareTasksForRestart` 保证，通报行没有剩余含义。
   */
  consumeStartupNotices(botUid: string): void {
    for (const notice of this.deps.store.listRestartNotices(botUid)) {
      this.deps.store.clearRestartNotice(notice.id);
    }
  }
}
