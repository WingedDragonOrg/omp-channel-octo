/**
 * `/restart` 的资格检查、`channel_control` 的空闲重启，以及旧版通报行的静默清理。
 * 在途任务由服务根据持久 prompt 标记续接；这里仅保存延期重启意图。
 * CoreService 注入出站说明、状态盘、活动视图与检测出的宿主重启端口。
 */

import type { ChatTarget, RestartPort } from "../contracts.ts";
import type { ChannelStore } from "../store/index.ts";

const RESTART_USAGE = "用法：/restart [now]。两种写法等价：重启后新进程会续接在途任务。";
const RESTART_UNAVAILABLE_TEXT = "未检测到守护进程，无法重启 channel。";
/** 命令仅发送请求回执；owner 私聊的启停通知由 CoreService 统一发送。 */
const RESTART_ACK_TEXT = "正在重启 channel；在途任务会在重启后继续。";
const CHANNEL_RESTART_SCHEDULED_TEXT =
  "已排定重启：本回合结束后、channel 空闲时执行守护进程的重启。";

/**
 * 重启控制可以触达的外部状态：出站说明的入队、旧重启通知行的清理、在途任务视图（延期重启的
 * idle 判定），以及宿主注入的重启端口。
 */
export interface ServiceRestartDeps {
  readonly store: Pick<ChannelStore, "listQueuedTasks" | "clearRestartNotices">;
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
  private pendingRestart = false;

  constructor(private readonly deps: ServiceRestartDeps) {}

  /** `channel_control` 是否排定了一次延期重启。 */
  get pending(): boolean {
    return this.pendingRestart;
  }

  /**
   * 参数接受空或 `now`；忙时也可请求重启。通过资格检查后回复一次，并返回 true 让服务
   * 在命令 claim 完成后调用宿主重启端口。端口缺席时明确拒绝。
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
  schedule(): string {
    if (!this.deps.restart) return RESTART_UNAVAILABLE_TEXT;
    this.pendingRestart = true;
    return CHANNEL_RESTART_SCHEDULED_TEXT;
  }

  /**
   * Runs a restart the agent asked for once it cannot cost work: every chat idle and no queued
   * task. The request is fire-and-forget, because the supervisor stops this process.
   */
  maybeRunDeferred(): void {
    if (!this.pendingRestart || this.deps.stopping()) return;
    if (this.deps.hasRunning() || this.deps.store.listQueuedTasks().length > 0) return;
    this.pendingRestart = false;
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
    this.deps.store.clearRestartNotices(botUid);
  }
}
