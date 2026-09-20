/**
 * channel 重启的所有者：`/restart` 的校验与忙碌目标判定、`channel_control` 的延期重启排定、
 * 延期重启在 channel idle 时的实际请求，以及跨进程的重启通报（10 分钟投递窗口、按聊天去重、
 * 启动时清理）都在这里。`requested`/`pending` 只读暴露：任务取消说明与关停通报据此区分
 * 「重启中断」与普通取消。
 *
 * 模块不依赖 `service.ts`：出站说明（`enqueueText`）、重启通知行（`store` 的四个窄方法）、
 * 在途任务视图（`runningRuntimes`/`hasRunning`，直接借用服务的 runtimes 集合、不复制）、
 * bot 身份与停止态，以及宿主检测到的重启端口（`restart`，缺席即未检测到守护进程）都由
 * `CoreService` 以端口注入；端口缺席时命令只回一条说明，不猜重启机制。
 */

import { randomUUID } from "node:crypto";
import type { ChatTarget, RestartPort } from "../contracts.ts";
import type { ChannelStore } from "../store/index.ts";
import { targetKey } from "../target.ts";

const RESTART_USAGE = "用法：/restart [now]；now 表示强制重启并中断在途任务。";
const RESTART_UNAVAILABLE_TEXT = "未检测到守护进程，无法重启 channel。";
const CHANNEL_RESTART_SCHEDULED_TEXT =
  "已排定重启：本回合结束后、channel 没有运行中或排队任务时执行守护进程的重启；重启完成后会在本会话通报 PID。";
/** 重启通报的投递窗口：更早的请求已经失去上下文，启动时静默丢弃（仍清理掉该行）。 */
const RESTART_NOTICE_TTL_MS = 10 * 60 * 1_000;

/**
 * 重启控制可以触达的外部状态：出站说明的入队、重启通知行、在途任务视图、bot 身份与停止态，
 * 以及宿主注入的重启端口。两个 getter 是因为 bot 在 start 之后才注册、停止态由服务持有；
 * 在途任务视图的两个入口共用同一份 runtimes：`hasRunning` 是 idle 判定，`runningRuntimes`
 * 供忙碌目标枚举。
 */
export interface ServiceRestartDeps {
  readonly store: Pick<
    ChannelStore,
    "listQueuedTasks" | "addRestartNotice" | "listRestartNotices" | "clearRestartNotice"
  >;
  readonly botUid: () => string | undefined;
  readonly stopping: () => boolean;
  /** 在途任务视图：`CoreService` 的 runtimes 本身（不复制），只借用 `target` 字段。 */
  readonly runningRuntimes: () => Iterable<{ readonly target: ChatTarget }>;
  readonly hasRunning: () => boolean;
  /** 宿主检测到的进程守护端口；缺席表示这个部署无法重启自己。 */
  readonly restart: RestartPort | undefined;
  readonly enqueueText: (target: ChatTarget, text: string, taskId: string | null) => void;
}

export class ServiceRestart {
  /**
   * 已请求重启：此后每个在途任务的关停都写一条重启通报（普通关停不写），任务取消说明也据此
   * 换成「正在重启」的文案。
   */
  private restartRequested = false;
  /**
   * A restart requested through `channel_control`, kept until every chat is idle so the turn that
   * asked for it still delivers its reply.
   */
  private pendingRestart: ChatTarget | null = null;

  constructor(private readonly deps: ServiceRestartDeps) {}

  /** 本进程是否已经请求重启。 */
  get requested(): boolean {
    return this.restartRequested;
  }

  /** `channel_control` 是否排定了一次延期重启。 */
  get pending(): boolean {
    return this.pendingRestart !== null;
  }

  /**
   * `/restart` 的校验与执行：参数只接受空或 `now`；有运行中或排队任务的会话时只接受 `now`；
   * 通过校验先记重启通报、再回执，然后要求服务向守护进程请求重启。返回 true 表示服务应请求
   * 重启。端口缺席时报未检测到守护进程，不猜重启机制。
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
    const busy = this.busyTargets();
    if (busy.length > 0 && mode !== "now") {
      await this.deps.enqueueText(
        target,
        `当前有 ${busy.length} 个会话在运行或排队任务，重启会中断它们：先 /cancel，或发送 /restart now 强制重启。`,
        null,
      );
      return false;
    }
    this.recordNotice(target);
    await this.deps.enqueueText(
      target,
      busy.length > 0
        ? `正在重启 channel；${busy.length} 个会话的在途任务会中断，重启完成后会在本会话确认。`
        : "正在重启 channel；重启完成后会在本会话确认。",
      null,
    );
    return true;
  }

  /**
   * 重启请求：先同步置位 `requested`（取消说明与关停通报都据此归因），再把宿主的请求原样交回
   * 调用方——本方法不包 async，`await` 的微任务边界与直接 `await restart?.request()` 一致。
   */
  request(): Promise<void> | undefined {
    this.restartRequested = true;
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
    const target = this.pendingRestart;
    if (!target || this.deps.stopping()) return;
    if (this.deps.hasRunning() || this.deps.store.listQueuedTasks().length > 0) return;
    this.pendingRestart = null;
    this.restartRequested = true;
    this.recordNotice(target);
    const restart = this.deps.restart;
    if (!restart) return;
    void restart.request().catch((error: unknown) => {
      console.error(
        `[restart] 重启命令执行失败：${error instanceof Error ? error.message : "unknown error"}`,
      );
    });
  }

  /** 带在途任务关停时的重启归因：只有真正请求过重启才写通报，普通关停不写。 */
  rememberInterrupted(target: ChatTarget): void {
    if (!this.restartRequested) return;
    this.recordNotice(target);
  }

  /**
   * 启动回执：上一次进程记录的重启请求在这里通报新 PID。每条通知行都先清理（无论是否在窗口
   * 内），超出投递窗口的静默丢弃；同一聊天的多条只通报一次。
   */
  async announceStartup(botUid: string): Promise<void> {
    const restartedAt = Date.now();
    const notifiedTargets = new Set<string>();
    for (const notice of this.deps.store.listRestartNotices(botUid)) {
      this.deps.store.clearRestartNotice(notice.id);
      if (restartedAt - notice.requestedAt > RESTART_NOTICE_TTL_MS) continue;
      const key = targetKey(notice);
      if (notifiedTargets.has(key)) continue;
      notifiedTargets.add(key);
      await this.deps.enqueueText(
        notice,
        `channel 已重启完成：PID ${process.pid}，启动于 ${new Date(restartedAt).toISOString()}。`,
        null,
      );
    }
  }

  /** `/restart`、延期重启与带在途任务的关停共用同一套通知行字段。 */
  private recordNotice(target: ChatTarget): void {
    const botUid = this.deps.botUid()!;
    this.deps.store.addRestartNotice({
      id: randomUUID(),
      botUid,
      channelType: target.channelType,
      channelId: target.channelId,
      requestedAt: Date.now(),
    });
  }

  /** 运行中或排队任务涉及的会话（按聊天去重，运行中的排在前面）。 */
  private busyTargets(): ChatTarget[] {
    const seen = new Set<string>();
    const targets: ChatTarget[] = [];
    const add = (target: ChatTarget): void => {
      const key = targetKey(target);
      if (seen.has(key)) return;
      seen.add(key);
      targets.push(target);
    };
    for (const runtime of this.deps.runningRuntimes()) add(runtime.target);
    const botUid = this.deps.botUid();
    for (const task of this.deps.store.listQueuedTasks()) {
      if (botUid === undefined || task.botUid === botUid) add(task);
    }
    return targets;
  }
}
