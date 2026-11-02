import type { BotCardAction, BotEvent, ChannelTransport } from "./contracts.ts";
import { safeErrorMessage } from "./errors.ts";
import type { ChannelStore } from "./store/index.ts";

/** Long-poll hold and the pause between rounds that returned immediately or failed. */
const BOT_EVENT_POLL_WAIT_SECONDS = 20;
const BOT_EVENT_POLL_LIMIT = 50;
const BOT_EVENT_POLL_IDLE_MS = 1_000;
const BOT_EVENT_POLL_BACKOFF_MS = 5_000;
/** Attempts before a terminal callback; accepted durable work can keep the event unacknowledged. */
const BOT_EVENT_MAX_ATTEMPTS = 5;

/**
 * 一个事件的处理结果：`handled`/`ignored` 可 ACK，`retry` 保留有限重试计数，`pending` 等待恢复并保留游标。
 */
export type BotEventOutcome = "handled" | "ignored" | "retry" | "pending";
export type BotEventExhaustionOutcome = "handled" | "pending";

/** 一次 Bot 事件队列消费：轮询、应用、确认、推进游标。 */
export interface BotEventLoopDeps {
  readonly store: ChannelStore;
  readonly transport: ChannelTransport;
  botUid(): string | undefined;
  stopping(): boolean;
  /** 处理一次卡片点击；抛错按 `retry` 对待，event id 用于持久操作去重。 */
  onCardAction(action: BotCardAction, eventId: number): Promise<BotEventOutcome>;
  /** 处理重试耗尽时恢复已接受操作或投递失败提示；`pending` 继续保留事件且不 ACK。 */
  onCardActionRetriesExhausted?(
    action: BotCardAction,
    eventId: number,
  ): Promise<BotEventExhaustionOutcome | void>;
  /** Narrow retry controls for deterministic event-consumer tests; production uses the constants above. */
  readonly maxAttempts?: number;
  readonly retryDelayMs?: number;
}

export interface BotEventLoop {
  /** 幂等：已经在跑时什么都不做。 */
  start(): void;
  /** 中止并等待轮询结束，并清掉事件重试计数。 */
  stop(): Promise<void>;
}

/** Cancellable pause that resolves early when the caller's signal aborts. */
function waitFor(ms: number, signal: AbortSignal): Promise<void> {
  if (signal.aborted) return Promise.resolve();
  const { promise, resolve } = Promise.withResolvers<void>();
  const onAbort = (): void => {
    clearTimeout(timer);
    resolve();
  };
  const timer = setTimeout(() => {
    signal.removeEventListener("abort", onAbort);
    resolve();
  }, ms);
  signal.addEventListener("abort", onAbort, { once: true });
  return promise;
}

/**
 * Consumes this Bot's event queue for the life of the process: long poll, apply, acknowledge,
 * advance the cursor. Delivery is at-least-once, so an event whose acknowledgement was lost is
 * replayed and the answer compare-and-set turns the replay into a no-op.
 */
export function createBotEventLoop(deps: BotEventLoopDeps): BotEventLoop {
  const eventAttempts = new Map<number, number>();
  const maxAttempts = deps.maxAttempts ?? BOT_EVENT_MAX_ATTEMPTS;
  const retryDelayMs = deps.retryDelayMs ?? BOT_EVENT_POLL_BACKOFF_MS;
  let abort: AbortController | undefined;
  let loop: Promise<void> | undefined;

  async function run(signal: AbortSignal): Promise<void> {
    const botUid = deps.botUid();
    if (!botUid) return;
    let cursor = deps.store.getBotEventCursor(botUid);
    while (!signal.aborted && !deps.stopping()) {
      let events: BotEvent[] | null;
      try {
        events = await deps.transport.fetchBotEvents({
          afterEventId: cursor,
          limit: BOT_EVENT_POLL_LIMIT,
          waitSeconds: BOT_EVENT_POLL_WAIT_SECONDS,
          signal,
        });
      } catch (error) {
        if (signal.aborted || deps.stopping()) return;
        console.error(`[bot-events] poll failed：${safeErrorMessage(error, "poll failed")}`);
        await waitFor(BOT_EVENT_POLL_BACKOFF_MS, signal);
        continue;
      }
      // A deployment without an event endpoint answers once and is never asked again.
      if (events === null) return;
      for (const event of events) {
        if (signal.aborted || deps.stopping()) return;
        const outcome = event.kind === "card_action"
          ? await deps.onCardAction(event.action, event.eventId).catch((): BotEventOutcome => "retry")
          : "ignored";
        if (outcome === "pending") {
          // A durable grant exists; keep its event unacknowledged until mirror/result recovery finishes.
          eventAttempts.delete(event.eventId);
          await waitFor(retryDelayMs, signal);
          break;
        }
        if (outcome === "retry") {
          const attempts = (eventAttempts.get(event.eventId) ?? 0) + 1;
          eventAttempts.set(event.eventId, attempts);
          // Keep the cursor: the next round re-reads this event from the same place.
          if (attempts < maxAttempts) {
            await waitFor(retryDelayMs, signal);
            break;
          }
          if (attempts === maxAttempts) {
            console.error(`[bot-events] exhausted event ${event.eventId} after ${attempts} attempts`);
          }
          if (event.kind === "card_action") {
            let exhaustedOutcome: BotEventExhaustionOutcome | void;
            try {
              exhaustedOutcome = await deps.onCardActionRetriesExhausted?.(event.action, event.eventId);
            } catch (error) {
              console.error(`[bot-events] exhausted-event notice failed：${safeErrorMessage(error, "send failed")}`);
              exhaustedOutcome = "pending";
            }
            if (exhaustedOutcome === "pending") {
              await waitFor(retryDelayMs, signal);
              break;
            }
          }
        }
        try {
          await deps.transport.ackBotEvent(event.eventId, signal);
        } catch (error) {
          if (signal.aborted || deps.stopping()) return;
          console.error(`[bot-events] ack failed：${safeErrorMessage(error, "ack failed")}`);
          await waitFor(BOT_EVENT_POLL_BACKOFF_MS, signal);
          break;
        }
        eventAttempts.delete(event.eventId);
        cursor = Math.max(cursor, event.eventId);
        try {
          deps.store.setBotEventCursor(botUid, cursor);
        } catch {
          console.error("[bot-events] cursor write failed");
        }
      }
      if (events.length === 0) await waitFor(BOT_EVENT_POLL_IDLE_MS, signal);
    }
  }

  return {
    start(): void {
      if (loop) return;
      const controller = new AbortController();
      abort = controller;
      loop = run(controller.signal).catch((error: unknown) => {
        console.error(`[bot-events] poller stopped：${safeErrorMessage(error, "poll failed")}`);
      });
    },
    async stop(): Promise<void> {
      abort?.abort();
      await loop?.catch(() => {});
      abort = undefined;
      loop = undefined;
      eventAttempts.clear();
    },
  };
}
