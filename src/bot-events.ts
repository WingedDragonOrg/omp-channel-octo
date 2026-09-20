import type { BotCardAction, BotEvent, ChannelTransport } from "./contracts.ts";
import { safeErrorMessage } from "./errors.ts";
import type { ChannelStore } from "./store.ts";

/** Long-poll hold and the pause between rounds that returned immediately or failed. */
const BOT_EVENT_POLL_WAIT_SECONDS = 20;
const BOT_EVENT_POLL_LIMIT = 50;
const BOT_EVENT_POLL_IDLE_MS = 1_000;
const BOT_EVENT_POLL_BACKOFF_MS = 5_000;
/** Attempts applied to one event before it is acknowledged and dropped, so the queue keeps moving. */
const BOT_EVENT_MAX_ATTEMPTS = 5;

/**
 * 一个事件的处理结果：`handled`/`ignored` 终结该事件（可 ACK），`retry` 保留游标重读。
 */
export type BotEventOutcome = "handled" | "ignored" | "retry";

/** 一次 Bot 事件队列消费：轮询、应用、确认、推进游标。 */
export interface BotEventLoopDeps {
  readonly store: ChannelStore;
  readonly transport: ChannelTransport;
  botUid(): string | undefined;
  stopping(): boolean;
  /** 处理一次卡片点击；抛错按 `retry` 对待。 */
  onCardAction(action: BotCardAction): Promise<BotEventOutcome>;
}

export interface BotEventLoop {
  /** 幂等：已经在跑时什么都不做。 */
  start(): void;
  /** 中止并等待轮询结束，并清掉事件重试计数。 */
  stop(): Promise<void>;
}

/** Cancellable pause that resolves early when the caller's signal aborts. */
function waitFor(ms: number, signal: AbortSignal): Promise<void> {
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
          ? await deps.onCardAction(event.action).catch((): BotEventOutcome => "retry")
          : "ignored";
        if (outcome === "retry") {
          const attempts = (eventAttempts.get(event.eventId) ?? 0) + 1;
          eventAttempts.set(event.eventId, attempts);
          // Keep the cursor: the next round re-reads this event from the same place.
          if (attempts < BOT_EVENT_MAX_ATTEMPTS) break;
          console.error(`[bot-events] dropped event ${event.eventId} after ${attempts} attempts`);
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
