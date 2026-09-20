import type { ChatTarget } from "./contracts.ts";
import { targetKey } from "./target.ts";

/**
 * Typing 租约：按 (bot, chat) 单例的「正在输入」续期环。
 *
 * 一条续期环对应一个 target：首个 owner acquire 时立即发一次 `POST /v1/bot/typing`，之后每
 * `renewMs` 续期；最后一个 owner release 时清 timer 并 abort 在途请求。owner 是两个前缀族：
 * `msg:<messageId>`（门后到任务接受之间的消息租约，由调用方 try/finally 保证释放）与
 * `task:<taskId>`（任务租约，runTask 终态释放，另有每 tick 的任务行状态兜底——queued 任务被
 * `/cancel` 或启动失败等任何漏 release 路径都在下一 tick 内回收）。
 *
 * 没有「取消 typing」的服务端端点：熄灭 = 停止续期，客户端按自身契约在 ≤8s 内自取灭，任意新
 * 消息到达也会清除。服务端 90s/180s 节流窗口对本模块透明（一律 HTTP 200），本模块只承诺发送
 * 时机，不承诺客户端可见。
 */

/** 默认续期间隔：足以续住客户端每条 typing CMD 约 8s 的显示。 */
const DEFAULT_RENEW_MS = 3_000;
/** 单个 typing 请求的 deadline：失败只丢弃该次，下一 tick 重来。 */
const DEFAULT_REQUEST_TIMEOUT_MS = 2_000;

export type TypingLeaseTaskState = "active" | "terminal";

export interface TypingLeaseOptions {
  sendTyping: (target: ChatTarget, signal: AbortSignal) => Promise<void>;
  /** 把一次 typing 请求挂进服务的活动集合，让优雅停止能等到它落地。 */
  trackOperation: (operation: Promise<void>) => void;
  /** `task:` owner 的兜底校验：queued/started 为 active，其余（含行缺失）为 terminal。 */
  taskState: (taskId: string) => TypingLeaseTaskState;
  /** 服务停止中：acquire 立即失效，切断一切新入口。 */
  stopping: () => boolean;
  renewMs?: number;
  requestTimeoutMs?: number;
}

interface LeaseEntry {
  target: ChatTarget;
  owners: Set<string>;
  timer: NodeJS.Timeout | undefined;
  inFlight: AbortController | undefined;
}

const TASK_OWNER_PREFIX = "task:";

export interface TypingLeaseManager {
  acquire: (target: ChatTarget, owner: string) => void;
  release: (target: ChatTarget, owner: string) => void;
  releaseAll: () => void;
  /** 测试观测口：该 target 当前持有的 owner 数量。 */
  ownerCount: (target: ChatTarget) => number;
}

export function createTypingLeaseManager(options: TypingLeaseOptions): TypingLeaseManager {
  const renewMs = options.renewMs ?? DEFAULT_RENEW_MS;
  const requestTimeoutMs = options.requestTimeoutMs ?? DEFAULT_REQUEST_TIMEOUT_MS;
  const entries = new Map<string, LeaseEntry>();

  const stopEntry = (entry: LeaseEntry): void => {
    if (entry.timer !== undefined) {
      clearTimeout(entry.timer);
      entry.timer = undefined;
    }
    entry.inFlight?.abort();
  };

  /** 每个 typing 请求一个独立 deadline，父 signal 随租约熄灭中止在途请求。 */
  const runRequest = async (
    work: (signal: AbortSignal) => Promise<void>,
    parentSignal?: AbortSignal,
  ): Promise<void> => {
    const controller = new AbortController();
    const abort = () => controller.abort();
    if (parentSignal?.aborted) {
      controller.abort();
    } else {
      parentSignal?.addEventListener("abort", abort, { once: true });
    }
    const timer = setTimeout(() => controller.abort(), requestTimeoutMs);
    try {
      await work(controller.signal);
    } finally {
      clearTimeout(timer);
      parentSignal?.removeEventListener("abort", abort);
    }
  };

  const tick = (key: string, entry: LeaseEntry): void => {
    // 兜底：terminal 或行缺失的任务租约在下一 tick 内回收（覆盖排队中取消等漏 release 路径）。
    for (const owner of [...entry.owners]) {
      if (!owner.startsWith(TASK_OWNER_PREFIX)) continue;
      if (options.taskState(owner.slice(TASK_OWNER_PREFIX.length)) === "terminal") {
        entry.owners.delete(owner);
      }
    }
    if (entry.owners.size === 0) {
      entries.delete(key);
      stopEntry(entry);
      return;
    }
    const parent = new AbortController();
    entry.inFlight = parent;
    options.trackOperation(
      runRequest((signal) => options.sendTyping(entry.target, signal), parent.signal).catch(() => {}),
    );
    entry.timer = setTimeout(() => {
      entry.timer = undefined;
      tick(key, entry);
    }, renewMs);
  };

  return {
    acquire(target, owner) {
      if (options.stopping()) return;
      const key = targetKey(target);
      const existing = entries.get(key);
      if (existing) {
        existing.owners.add(owner);
        return;
      }
      const entry: LeaseEntry = {
        target: { channelType: target.channelType, channelId: target.channelId },
        owners: new Set([owner]),
        timer: undefined,
        inFlight: undefined,
      };
      entries.set(key, entry);
      tick(key, entry);
    },
    release(target, owner) {
      const key = targetKey(target);
      const entry = entries.get(key);
      if (!entry) return;
      entry.owners.delete(owner);
      if (entry.owners.size === 0) {
        entries.delete(key);
        stopEntry(entry);
      }
    },
    releaseAll() {
      for (const entry of entries.values()) stopEntry(entry);
      entries.clear();
    },
    ownerCount(target) {
      return entries.get(targetKey(target))?.owners.size ?? 0;
    },
  };
}
