import { expect, test, vi } from "bun:test";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import type { BotEvent, BotEventFetch } from "../src/contracts.ts";
import { createBotEventLoop } from "../src/bot-events.ts";
import { openChannelStore } from "../src/store.ts";
import { FakeTransport } from "./fixtures/core-fakes.ts";

class RetryAckTransport extends FakeTransport {
  failures = 1;
  override async fetchBotEvents(request: BotEventFetch): Promise<BotEvent[] | null> {
    this.eventFetches.push(request.afterEventId);
    return request.afterEventId >= 2 ? null : [1, 2]
      .filter((id) => id > request.afterEventId)
      .map((eventId) => ({ eventId, kind: "other" as const }));
  }
  override async ackBotEvent(eventId: number, signal?: AbortSignal): Promise<void> {
    if (this.failures-- > 0) throw new Error("transient acknowledgement failure");
    await super.ackBotEvent(eventId, signal);
  }
}

async function flush(): Promise<void> {
  for (let i = 0; i < 20; i++) await Promise.resolve();
}

test("a failed acknowledgement retries from the held cursor and keeps later events moving", async () => {
  const root = await mkdtemp(join(tmpdir(), "oco-event-retry-"));
  const store = await openChannelStore(root);
  const transport = new RetryAckTransport({ bot: { uid: "event-bot", name: "Fixture" } });
  const loop = createBotEventLoop({
    store, transport, botUid: () => transport.bot.uid, stopping: () => false,
    onCardAction: async () => "handled",
  });
  vi.useFakeTimers();
  try {
    loop.start();
    await flush();
    expect(store.getBotEventCursor(transport.bot.uid)).toBe(0);
    expect(transport.ackedEvents).toEqual([]);
    vi.advanceTimersByTime(5_000);
    await flush();
    expect(transport.ackedEvents).toEqual([1, 2]);
    expect(transport.eventFetches.slice(0, 2)).toEqual([0, 0]);
    expect(store.getBotEventCursor(transport.bot.uid)).toBe(2);
  } finally {
    await loop.stop();
    vi.useRealTimers();
    store.close();
    await rm(root, { recursive: true, force: true });
  }
});

test("retryable event processing waits for recovery instead of exhausting its budget immediately", async () => {
  const root = await mkdtemp(join(tmpdir(), "oco-event-processing-"));
  const store = await openChannelStore(root);
  const transport = new RetryAckTransport({ bot: { uid: "processing-bot", name: "Fixture" } });
  transport.failures = 0;
  transport.fetchBotEvents = async ({ afterEventId }) => afterEventId >= 1 ? null : [{
    eventId: 1,
    kind: "card_action",
    action: { messageId: "card", channelType: 1, channelId: "owner", actionId: "ask_submit", operatorUid: "owner", inputs: {} },
  }];
  let available = false;
  let applied = 0;
  const loop = createBotEventLoop({
    store, transport, botUid: () => transport.bot.uid, stopping: () => false,
    onCardAction: async () => {
      if (!available) return "retry";
      applied++;
      return "handled";
    },
  });
  vi.useFakeTimers();
  try {
    loop.start();
    await flush();
    expect(transport.ackedEvents).toEqual([]);
    expect(store.getBotEventCursor(transport.bot.uid)).toBe(0);
    available = true;
    vi.advanceTimersByTime(5_000);
    await flush();
    expect(applied).toBe(1);
    expect(transport.ackedEvents).toEqual([1]);
    expect(store.getBotEventCursor(transport.bot.uid)).toBe(1);
  } finally {
    await loop.stop();
    vi.useRealTimers();
    store.close();
    await rm(root, { recursive: true, force: true });
  }
});
