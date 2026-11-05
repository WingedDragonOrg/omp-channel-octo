import { expect, test, vi } from "bun:test";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import type { BotEvent, BotEventFetch, CardFrame, ChatTarget, SendReceipt } from "../src/contracts.ts";
import { createBotEventLoop } from "../src/bot-events.ts";
import { openChannelStore } from "../src/store/index.ts";
import { createChatGrantDesk } from "../src/inbound/chat-grants.ts";
import { FakeTransport, inbound } from "./fixtures/core-fakes.ts";
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


class DelayedGrantTransport extends FakeTransport {
  readonly cardSendStarted = Promise.withResolvers<CardFrame>();
  readonly releaseCardSend = Promise.withResolvers<void>();
  override async sendCard(
    target: ChatTarget,
    frame: CardFrame,
    clientMsgNo: string,
    signal?: AbortSignal,
  ): Promise<SendReceipt | null> {
    this.cardSendStarted.resolve(frame);
    await this.releaseCardSend.promise;
    return await super.sendCard(target, frame, clientMsgNo, signal);
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
    onCardAction: () => ({ ackReady: Promise.resolve("handled") }),
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
    onCardAction: () => {
      if (!available) return { ackReady: Promise.resolve("retry" as const) };
      applied += 1;
      return { ackReady: Promise.resolve("handled" as const) };
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

test("stop during event processing does not wait on an already-aborted retry delay", async () => {
  const root = await mkdtemp(join(tmpdir(), "oco-event-stop-"));
  const store = await openChannelStore(root);
  const transport = new RetryAckTransport({ bot: { uid: "stopping-bot", name: "Fixture" } });
  const entered = Promise.withResolvers<void>();
  const completion = Promise.withResolvers<"retry">();
  transport.fetchBotEvents = async () => [{
    eventId: 1, kind: "card_action",
    action: { messageId: "card", channelType: 1, channelId: "owner", actionId: "ask_submit", operatorUid: "owner", inputs: {} },
  }];
  const loop = createBotEventLoop({
    store, transport, botUid: () => transport.bot.uid, stopping: () => false,
    onCardAction: () => {
      entered.resolve();
      return { ackReady: completion.promise };
    },
  });
  let stopped = false;
  vi.useFakeTimers();
  try {
    loop.start();
    await entered.promise;
    const stopping = loop.stop().then(() => { stopped = true; });
    completion.resolve("retry");
    await flush();
    expect(stopped).toBe(true);
    expect(transport.ackedEvents).toEqual([]);
    expect(store.getBotEventCursor(transport.bot.uid)).toBe(0);
    await stopping;
  } finally {
    completion.resolve("retry");
    vi.advanceTimersByTime(5_000);
    await loop.stop();
    vi.useRealTimers();
    store.close();
    await rm(root, { recursive: true, force: true });
  }
});

test("event retry exhaustion runs the terminal notice before acknowledgement and cursor advance", async () => {
  const root = await mkdtemp(join(tmpdir(), "oco-event-exhausted-"));
  const store = await openChannelStore(root);
  const event: BotEvent = {
    eventId: 7,
    kind: "card_action",
    action: {
      messageId: "grant-card",
      channelType: 2,
      channelId: "grant-group",
      actionId: "grant-chat:00000000-0000-4000-8000-000000000001",
      operatorUid: "owner",
      inputs: {},
    },
  };
  const transport = new RetryAckTransport({ bot: { uid: "exhausted-bot", name: "Fixture" } });
  transport.failures = 0;
  transport.fetchBotEvents = async ({ afterEventId }) => {
    transport.eventFetches.push(afterEventId);
    return afterEventId >= event.eventId ? null : [event];
  };
  let attempts = 0;
  let exhausted = 0;
  const ack = transport.ackBotEvent.bind(transport);
  transport.ackBotEvent = async (eventId, signal) => {
    expect(exhausted).toBe(1);
    await ack(eventId, signal);
  };
  const loop = createBotEventLoop({
    store,
    transport,
    botUid: () => transport.bot.uid,
    stopping: () => false,
    maxAttempts: 2,
    retryDelayMs: 0,
    onCardAction: () => {
      attempts += 1;
      return { ackReady: Promise.resolve("retry") };
    },
    onCardActionRetriesExhausted: async (action, eventId) => {
      expect(action.actionId).toBe(event.action.actionId);
      expect(eventId).toBe(event.eventId);
      exhausted += 1;
    },
  });
  vi.useFakeTimers();
  try {
    loop.start();
    await flush();
    expect(attempts).toBe(1);
    expect(transport.ackedEvents).toEqual([]);

    vi.advanceTimersByTime(0);
    await flush();
    expect(attempts).toBe(2);
    expect(exhausted).toBe(1);
    expect(transport.ackedEvents).toEqual([event.eventId]);
    expect(store.getBotEventCursor(transport.bot.uid)).toBe(event.eventId);
  } finally {
    await loop.stop();
    vi.useRealTimers();
    store.close();
    await rm(root, { recursive: true, force: true });
  }
});

test("retry exhaustion finishes an already accepted grant before acknowledging its event", async () => {
  const root = await mkdtemp(join(tmpdir(), "oco-event-accepted-exhausted-"));
  const store = await openChannelStore(root);
  const transport = new FakeTransport({ bot: { uid: "accepted-bot", name: "Fixture Bot" } });
  store.setBotUid(transport.bot.uid);
  store.setOwnerId(transport.bot.uid, "owner-1");
  transport.cardSupported = true;
  const desk = createChatGrantDesk({
    store,
    transport,
    bot: () => transport.bot,
    stopping: () => false,
    shutdownSignal: new AbortController().signal,
    allowlistWriteback: undefined,
    isChatPermitted: () => false,
    enqueueText: () => {},
  });
  const event: BotEvent = {
    eventId: 27,
    kind: "card_action",
    action: {
      messageId: "",
      channelType: 2,
      channelId: "accepted-group",
      actionId: "",
      operatorUid: "owner-1",
      inputs: {},
    },
  };
  let loop: ReturnType<typeof createBotEventLoop> | undefined;
  vi.useFakeTimers();
  try {
    await desk.offer(inbound({
      channelId: event.action.channelId,
      channelType: event.action.channelType,
      senderId: event.action.operatorUid,
      mentionedUserIds: [transport.bot.uid],
    }), transport.bot);
    const card = transport.sentCards[0]!;
    const actions = card.frame.card["actions"] as Array<{ id: string }>;
    event.action.messageId = card.messageId;
    event.action.actionId = actions[0]!.id;
    expect(store.acceptChatGrantOperation({
      actionId: event.action.actionId,
      eventId: event.eventId,
      botUid: transport.bot.uid,
      ownerId: "owner-1",
      actorId: "owner-1",
      channelType: 2,
      channelId: event.action.channelId,
      cardMessageId: card.messageId,
      resultMessageId: "stable-grant-result",
    })).toBe(true);
    transport.fetchBotEvents = async ({ afterEventId }) =>
      afterEventId >= event.eventId ? null : [event];
    const ack = transport.ackBotEvent.bind(transport);
    transport.ackBotEvent = async (eventId, signal) => {
      expect(transport.cardEdits).toHaveLength(1);
      await ack(eventId, signal);
    };
    loop = createBotEventLoop({
      store,
      transport,
      botUid: () => transport.bot.uid,
      stopping: () => false,
      maxAttempts: 2,
      retryDelayMs: 0,
      onCardAction: () => ({ ackReady: Promise.resolve("retry") }),
      onCardActionRetriesExhausted: (action, eventId) =>
        desk.onRetriesExhausted(eventId, action),
    });
    loop.start();
    await flush();
    expect(transport.ackedEvents).toEqual([]);
    vi.advanceTimersByTime(0);
    await flush();
    expect(transport.ackedEvents).toEqual([event.eventId]);
    expect(transport.sent).toHaveLength(0);
    expect(store.listAllowedChats(transport.bot.uid)).toHaveLength(1);
  } finally {
    if (loop !== undefined) await loop.stop();
    vi.useRealTimers();
    store.close();
    await rm(root, { recursive: true, force: true });
  }
});

test("owner action stays unacknowledged at exhaustion while its card send is still binding", async () => {
  const root = await mkdtemp(join(tmpdir(), "oco-event-send-inflight-"));
  const store = await openChannelStore(root);
  const transport = new DelayedGrantTransport({ bot: { uid: "inflight-bot", name: "Fixture Bot" } });
  store.setBotUid(transport.bot.uid);
  store.setOwnerId(transport.bot.uid, "owner-1");
  transport.cardSupported = true;
  const desk = createChatGrantDesk({
    store,
    transport,
    bot: () => transport.bot,
    stopping: () => false,
    shutdownSignal: new AbortController().signal,
    allowlistWriteback: undefined,
    isChatPermitted: () => false,
    enqueueText: () => {},
  });
  const event: BotEvent = {
    eventId: 31,
    kind: "card_action",
    action: {
      messageId: "card-1",
      channelType: 2,
      channelId: "inflight-group",
      actionId: "",
      operatorUid: "owner-1",
      inputs: {},
    },
  };
  let loop: ReturnType<typeof createBotEventLoop> | undefined;
  const offering = desk.offer(inbound({
    channelId: event.action.channelId,
    channelType: event.action.channelType,
    senderId: event.action.operatorUid,
    mentionedUserIds: [transport.bot.uid],
  }), transport.bot);
  vi.useFakeTimers();
  try {
    const frame = await transport.cardSendStarted.promise;
    event.action.actionId = (frame.card["actions"] as Array<{ id: string }>)[0]!.id;
    transport.fetchBotEvents = async ({ afterEventId, signal }) => {
      if (signal?.aborted) throw new Error("fixture bot events aborted");
      transport.eventFetches.push(afterEventId);
      return afterEventId >= event.eventId ? null : [event];
    };
    loop = createBotEventLoop({
      store,
      transport,
      botUid: () => transport.bot.uid,
      stopping: () => false,
      maxAttempts: 1,
      retryDelayMs: 0,
      onCardAction: (action, eventId) => ({ ackReady: desk.handleAction(eventId, action) }),
      onCardActionRetriesExhausted: (action, eventId) =>
        desk.onRetriesExhausted(eventId, action),
    });
    loop.start();
    await flush();
    expect(transport.ackedEvents).toEqual([]);
    expect(store.listAllowedChats(transport.bot.uid)).toHaveLength(0);

    transport.releaseCardSend.resolve();
    await offering;
    vi.advanceTimersByTime(0);
    await flush();
    expect(transport.ackedEvents).toEqual([event.eventId]);
    expect(transport.cardEdits).toHaveLength(1);
    expect(store.listAllowedChats(transport.bot.uid)).toHaveLength(1);
  } finally {
    transport.releaseCardSend.resolve();
    if (loop !== undefined) await loop.stop();
    await offering;
    vi.useRealTimers();
    store.close();
    await rm(root, { recursive: true, force: true });
  }
});
test("an unpersisted onboarding grant gets a failure prompt after bounded event retries", async () => {
  const root = await mkdtemp(join(tmpdir(), "oco-grant-exhausted-"));
  const store = await openChannelStore(root);
  const transport = new FakeTransport({ bot: { uid: "grant-bot", name: "Fixture Bot" } });
  const event: BotEvent = {
    eventId: 19,
    kind: "card_action",
    action: {
      messageId: "grant-card-1",
      channelType: 2,
      channelId: "grant-group",
      actionId: "grant-chat:00000000-0000-4000-8000-000000000019",
      operatorUid: "owner-1",
      inputs: {},
    },
  };
  store.setBotUid(transport.bot.uid);
  store.setOwnerId(transport.bot.uid, "owner-1");
  transport.cardSupported = true;
  const desk = createChatGrantDesk({
    store,
    transport,
    bot: () => transport.bot,
    stopping: () => false,
    shutdownSignal: new AbortController().signal,
    allowlistWriteback: undefined,
    isChatPermitted: () => false,
    enqueueText: () => {},
  });
  let loop: ReturnType<typeof createBotEventLoop> | undefined;
  vi.useFakeTimers();
  try {
    await desk.offer(inbound({
      channelId: "grant-group",
      channelType: 2,
      senderId: "owner-1",
      mentionedUserIds: [transport.bot.uid],
    }), transport.bot);
    const sentCard = transport.sentCards[0]!;
    event.action.messageId = sentCard.messageId;
    const submittedActions = sentCard.frame.card["actions"] as Array<{ id: string }>;
    event.action.actionId = submittedActions[0]!.id;
    transport.fetchBotEvents = async ({ afterEventId }) => {
      transport.eventFetches.push(afterEventId);
      return afterEventId >= event.eventId ? null : [event];
    };
    const ack = transport.ackBotEvent.bind(transport);
    transport.ackBotEvent = async (eventId, signal) => {
      expect(transport.sent.some(({ text }) => text.includes("开通失败"))).toBe(true);
      await ack(eventId, signal);
    };
    store.acceptChatGrantOperation = () => {
      throw new Error("fixture store write failure");
    };
    loop = createBotEventLoop({
      store,
      transport,
      botUid: () => transport.bot.uid,
      stopping: () => false,
      maxAttempts: 2,
      retryDelayMs: 0,
      onCardAction: (action, eventId) => ({ ackReady: desk.handleAction(eventId, action) }),
      onCardActionRetriesExhausted: (action, eventId) => desk.onRetriesExhausted(eventId, action),
    });
    loop.start();
    await flush();
    expect(transport.ackedEvents).toEqual([]);
    vi.advanceTimersByTime(0);
    await flush();
    expect(transport.ackedEvents).toEqual([event.eventId]);
    expect(transport.sent.map(({ text }) => text)).toContain(
      "本群开通失败，请重新 @Fixture Bot 获取开通方式。",
    );
    expect(store.listAllowedChats(transport.bot.uid)).toHaveLength(0);
  } finally {
    if (loop !== undefined) await loop.stop();
    vi.useRealTimers();
    store.close();
    await rm(root, { recursive: true, force: true });
  }
});
