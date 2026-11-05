import { expect, test, vi } from "bun:test";
import { Database } from "bun:sqlite";
import { join } from "node:path";
import { openChannelStore } from "../src/store/index.ts";
import type { AllowlistWriteback, BotEvent, BotEventFetch, CardFrame, ChatTarget, InboundMessage, SendReceipt } from "../src/contracts";
import {
  closeCoreHarness,
  createCoreHarness,
  FakeTransport,
  inbound,
  readIngressJob,
  submitAndWaitForIngressHandoff,
  waitForStatus,
  type CoreHarness,
} from "./fixtures/core-fakes";
import { cardActionEvent } from "./fixtures/core-fakes";

const ALLOWED = ["group-allowed"];
const DENIED_GROUP = "group-denied";
const DENIED_THREAD = "group-denied____topic-1";

/** Records the mirror calls; `failure` makes every append reject like an unwritable config file. */
class FakeWriteback implements AllowlistWriteback {
  readonly appended: string[] = [];
  failure: Error | undefined;

  async append(groupId: string): Promise<void> {
    if (this.failure) throw this.failure;
    this.appended.push(groupId);
  }
}

async function nextImmediate(): Promise<void> {
  const { promise, resolve } = Promise.withResolvers<void>();
  setImmediate(resolve);
  return promise;
}

async function settle(): Promise<void> {
  for (let tick = 0; tick < 50; tick += 1) await nextImmediate();
}

function sentToChat(
  harness: CoreHarness,
  target: { channelId: string; channelType: number },
) {
  return harness.transport.sent.filter(({ target: sentTarget }) =>
    sentTarget.channelId === target.channelId && sentTarget.channelType === target.channelType,
  );
}

async function waitForSent(
  harness: CoreHarness,
  target: { channelId: string; channelType: number },
  count: number,
): Promise<void> {
  for (let attempt = 0; attempt < 10_000; attempt += 1) {
    if (sentToChat(harness, target).length >= count) return;
    await nextImmediate();
  }
  throw new Error(`transport recorded ${sentToChat(harness, target).length} of ${count} messages`);
}

async function waitForAck(harness: CoreHarness, eventId: number): Promise<void> {
  for (let attempt = 0; attempt < 10_000; attempt += 1) {
    if (harness.transport.ackedEvents.includes(eventId)) return;
    await nextImmediate();
  }
  throw new Error(`Bot event ${eventId} was not acknowledged`);
}

function actionIdFromFrame(frame: CardFrame): string {
  const actions = frame.card["actions"];
  if (!Array.isArray(actions) || typeof actions[0]?.["id"] !== "string") {
    throw new Error("onboarding card has no submit action");
  }
  return actions[0]["id"];
}

function cardAction(
  harness: CoreHarness,
  overrides: {
    eventId: number;
    operatorUid?: string;
    channelId?: string;
    channelType?: 2 | 5;
    messageId?: string;
  },
) {
  const sent = harness.transport.sentCards[0];
  if (!sent) throw new Error("onboarding card was not sent");
  return cardActionEvent(overrides.eventId, {
    actionId: actionIdFromFrame(sent.frame),
    messageId: overrides.messageId ?? sent.messageId,
    channelType: overrides.channelType ?? 2,
    channelId: overrides.channelId ?? DENIED_GROUP,
    operatorUid: overrides.operatorUid ?? "owner-1",
    inputs: {},
  });
}

function frameText(frame: CardFrame): string {
  return JSON.stringify(frame.card);
}

class DelayedCardTransport extends FakeTransport {
  readonly cardSendStarted = Promise.withResolvers<CardFrame>();
  readonly releaseCardSend = Promise.withResolvers<void>();
  readonly eventDelivered = Promise.withResolvers<BotEvent>();
  private readonly unackedEvents = new Map<number, BotEvent>();

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

  override async fetchBotEvents(fetch: BotEventFetch): Promise<BotEvent[] | null> {
    if (fetch.signal?.aborted) throw new Error("fixture bot events aborted");
    const ready = [...this.unackedEvents.values()]
      .filter((event) => event.eventId > fetch.afterEventId)
      .sort((left, right) => left.eventId - right.eventId)
      .slice(0, fetch.limit);
    if (ready.length > 0) {
      this.eventFetches.push(fetch.afterEventId);
      return ready;
    }
    const events = await super.fetchBotEvents(fetch);
    for (const event of events ?? []) {
      this.unackedEvents.set(event.eventId, event);
      this.eventDelivered.resolve(event);
    }
    return events;
  }

  override async ackBotEvent(eventId: number, signal?: AbortSignal): Promise<void> {
    await super.ackBotEvent(eventId, signal);
    this.unackedEvents.delete(eventId);
  }
}

function queryRows(harness: CoreHarness, sql: string): Array<Record<string, unknown>> {
  const database = new Database(join(harness.stateDir, "channel.sqlite"), { readonly: true });
  try {
    return database.query(sql).all() as Array<Record<string, unknown>>;
  } finally {
    database.close();
  }
}

function ownerGroupMessage(overrides: Partial<InboundMessage>): InboundMessage {
  return inbound({
    channelId: DENIED_GROUP,
    channelType: 2,
    mentionedUserIds: [],
    ...overrides,
  });
}

function ingressObserver(harness: CoreHarness) {
  return {
    service: harness.service,
    stateDir: harness.stateDir,
    bot: harness.bot,
  };
}

async function submitAndWaitForHandoff(
  harness: CoreHarness,
  message: InboundMessage,
): Promise<void> {
  await submitAndWaitForIngressHandoff(ingressObserver(harness), message);
}

async function submitWithoutIngress(
  harness: CoreHarness,
  message: InboundMessage,
): Promise<void> {
  await harness.service.submitInbound(message);
  expect(await readIngressJob(ingressObserver(harness), message)).toBeNull();
}

async function waitForCardCount(harness: CoreHarness, count: number): Promise<void> {
  await waitForCondition(() => harness.transport.sentCards.length >= count);
}

test("an owner mention offers an interactive grant without starting a task", async () => {
  const harness = await createCoreHarness({ allowedGroupIds: ALLOWED });
  harness.transport.cardSupported = true;
  try {
    await submitWithoutIngress(harness, ownerGroupMessage({
      messageId: "unlisted-card",
      text: "请处理这项任务",
      mentionedUserIds: [harness.bot.uid],
    }));
    await waitForCardCount(harness, 1);
    expect(harness.transport.sentCards).toHaveLength(1);
    expect(harness.transport.sentCards[0]!.frame.profile).toBe("octo/v2");
    expect(queryRows(harness, "SELECT 1 FROM allowed_chats")).toHaveLength(0);
    expect(harness.workers.prompts).toHaveLength(0);
    await submitWithoutIngress(harness, ownerGroupMessage({
      messageId: "unlisted-card-throttled",
      text: "再次开通",
      mentionedUserIds: [harness.bot.uid],
    }));
    expect(harness.transport.sentCards).toHaveLength(1);
  } finally {
    await closeCoreHarness(harness);
  }
});

test("concurrent unlisted owner mentions share one in-flight onboarding offer", async () => {
  const transport = new DelayedCardTransport({
    bot: { uid: "bot-unlisted-concurrent", name: "Fixture Bot" },
  });
  transport.cardSupported = true;
  const harness = await createCoreHarness({ transport, allowedGroupIds: ALLOWED });
  try {
    const first = submitWithoutIngress(harness, ownerGroupMessage({
      messageId: "unlisted-concurrent-first",
      text: "请处理这项任务",
      mentionedUserIds: [harness.bot.uid],
    }));
    await transport.cardSendStarted.promise;
    await first;
    await submitWithoutIngress(harness, ownerGroupMessage({
      messageId: "unlisted-concurrent-second",
      text: "再试一次",
      mentionedUserIds: [harness.bot.uid],
    }));
    expect(transport.sentCards).toHaveLength(0);
    transport.releaseCardSend.resolve();
    await waitForCardCount(harness, 1);
    expect(transport.sentCards).toHaveLength(1);
    expect(harness.workers.prompts).toHaveLength(0);
  } finally {
    transport.releaseCardSend.resolve();
    await closeCoreHarness(harness);
  }
});

test("an owner mention in an unlisted group gets one throttled notice and never a task", async () => {
  const harness = await createCoreHarness({ allowedGroupIds: ALLOWED });
  try {
    const botUid = harness.bot.uid;
    await submitWithoutIngress(harness, ownerGroupMessage({
      messageId: "unlisted-first",
      text: "@Bot 看下这个群",
      mentionedUserIds: [botUid],
    }));
    await waitForSent(harness, { channelId: DENIED_GROUP, channelType: 2 }, 1);

    const groupReplies = sentToChat(harness, { channelId: DENIED_GROUP, channelType: 2 });
    expect(groupReplies).toHaveLength(1);
    const notice = groupReplies[0]!;
    expect(notice.target).toMatchObject({ channelId: DENIED_GROUP, channelType: 2 });
    expect(notice.text).toContain("本群未开通");
    expect(notice.text).toContain("开通本群");

    // Still inside the notice window: the same owner @ gets no second reply.
    await submitWithoutIngress(harness, ownerGroupMessage({
      messageId: "unlisted-second",
      text: "@Bot 再试一次",
      mentionedUserIds: [botUid],
    }));
    // Every other rejection path stays exactly as silent as before.
    await submitWithoutIngress(harness, ownerGroupMessage({
      messageId: "unlisted-other-member",
      senderId: "other-user",
      text: "other member tries",
      mentionedUserIds: [botUid],
    }));
    await submitWithoutIngress(harness, ownerGroupMessage({
      messageId: "unlisted-unmentioned",
      text: "owner talks without mentioning",
    }));
    await submitWithoutIngress(harness, ownerGroupMessage({
      messageId: "unlisted-bot-echo",
      senderId: botUid,
      text: "bot echo",
      mentionedUserIds: [botUid],
    }));

    expect(sentToChat(harness, { channelId: DENIED_GROUP, channelType: 2 })).toHaveLength(1);
    expect(harness.workers.prompts).toHaveLength(0);
    expect((await harness.service.status({ channelId: DENIED_GROUP, channelType: 2 })).state).toBe("none");
    // The rejection path persists nothing: no dedup row, no task, no queued reply, and the grant
    // set is untouched.
    expect(queryRows(harness, "SELECT 1 FROM inbound")).toHaveLength(0);
    expect(queryRows(harness, "SELECT 1 FROM tasks")).toHaveLength(0);
    expect(queryRows(harness, "SELECT 1 FROM allowed_chats")).toHaveLength(0);
    expect(queryRows(harness, "SELECT 1 FROM outbox WHERE state <> 'sent'")).toHaveLength(0);
  } finally {
    await closeCoreHarness(harness);
  }
});

test("the owner's passphrase enables the group durably and it survives a restart", async () => {
  const writeback = new FakeWriteback();
  const first = await createCoreHarness({ allowedGroupIds: ALLOWED, allowlistWriteback: writeback });
  const root = first.root;
  try {
    await submitWithoutIngress(first, ownerGroupMessage({
      messageId: "unlisted-mention-before-grant",
      text: "@Bot 看下这个群",
      mentionedUserIds: [first.bot.uid],
    }));
    await submitWithoutIngress(first, ownerGroupMessage({
      messageId: "unlisted-grant",
      text: " 开通本群 ",
      mentionedUserIds: [first.bot.uid],
    }));
    await waitForSent(first, { channelId: DENIED_GROUP, channelType: 2 }, 2);

    const groupReplies = sentToChat(first, { channelId: DENIED_GROUP, channelType: 2 });
    expect(groupReplies).toHaveLength(2);
    expect(groupReplies[0]!.text).toContain("本群未开通");
    expect(groupReplies[1]!.text).toContain("已开通本群");
    expect(writeback.appended).toEqual([DENIED_GROUP]);
    expect(queryRows(first, "SELECT channel_type, channel_id, granted_by FROM allowed_chats")).toEqual([
      { channel_type: 2, channel_id: DENIED_GROUP, granted_by: "owner-1" },
    ]);
    expect(first.workers.prompts).toHaveLength(0);

    // No restart, no config reload: the very next message in that group is a task, and the grant
    // did not open a second notice path for it.
    const work = ownerGroupMessage({
      messageId: "unlisted-work",
      text: "现在开始干活",
      mentionedUserIds: [first.bot.uid],
    });
    await submitAndWaitForHandoff(first, work);
    await waitForStatus(first.service, work, (status) => status.state === "completed");
    expect(first.workers.prompts.map(({ text }) => text)).toEqual(["现在开始干活"]);
    expect(sentToChat(first, { channelId: DENIED_GROUP, channelType: 2 })
      .filter(({ text }) => text.includes("未开通"))).toHaveLength(1);
    expect(sentToChat(first, { channelId: DENIED_GROUP, channelType: 2 })
      .filter(({ text }) => text.includes("已开通"))).toHaveLength(1);
  } finally {
    await closeCoreHarness(first, { remove: false });
  }

  const second = await createCoreHarness({ root, allowedGroupIds: ALLOWED });
  try {
    const resumed = ownerGroupMessage({
      messageId: "unlisted-after-restart",
      text: "重启后继续",
      mentionedUserIds: [second.bot.uid],
    });
    await submitAndWaitForHandoff(second, resumed);
    await waitForStatus(second.service, resumed, (status) => status.state === "completed");
    expect(second.workers.prompts.map(({ text }) => text)).toEqual(["重启后继续"]);
    expect(queryRows(second, "SELECT 1 FROM allowed_chats")).toHaveLength(1);
  } finally {
    await closeCoreHarness(second);
  }
});

test("the passphrase only works for the owner, only verbatim, and only with a mention", async () => {
  const harness = await createCoreHarness({ allowedGroupIds: ALLOWED });
  try {
    await submitWithoutIngress(harness, ownerGroupMessage({
      messageId: "unlisted-grant-other",
      senderId: "other-user",
      text: "开通本群",
      mentionedUserIds: [harness.bot.uid],
    }));
    await submitWithoutIngress(harness, ownerGroupMessage({
      messageId: "unlisted-grant-unmentioned",
      text: "开通本群",
    }));
    await submitWithoutIngress(harness, ownerGroupMessage({
      messageId: "unlisted-grant-not-exact",
      text: "请帮我开通本群",
      mentionedUserIds: [harness.bot.uid],
    }));
    await waitForSent(harness, { channelId: DENIED_GROUP, channelType: 2 }, 1);

    // The near-miss got the ordinary notice; nothing granted and no task started.
    const groupReplies = sentToChat(harness, { channelId: DENIED_GROUP, channelType: 2 });
    expect(groupReplies).toHaveLength(1);
    expect(groupReplies[0]!.text).toContain("本群未开通");
    expect(queryRows(harness, "SELECT 1 FROM allowed_chats")).toHaveLength(0);
    expect(harness.workers.prompts).toHaveLength(0);
    expect(queryRows(harness, "SELECT 1 FROM tasks")).toHaveLength(0);

    // The owner's exact reply still enables the group.
    await submitWithoutIngress(harness, ownerGroupMessage({
      messageId: "unlisted-grant-owner",
      text: "开通本群",
      mentionedUserIds: [harness.bot.uid],
    }));
    await waitForSent(harness, { channelId: DENIED_GROUP, channelType: 2 }, 2);
    expect(sentToChat(harness, { channelId: DENIED_GROUP, channelType: 2 })[1]!.text)
      .toContain("已开通本群");
    expect(queryRows(harness, "SELECT 1 FROM allowed_chats")).toHaveLength(1);
  } finally {
    await closeCoreHarness(harness);
  }
});

test("a thread carries its own notice and grant while its parent group stays unlisted", async () => {
  const harness = await createCoreHarness({ allowedGroupIds: ALLOWED });
  try {
    const botUid = harness.bot.uid;
    await submitWithoutIngress(harness, inbound({
      messageId: "unlisted-thread-mention",
      channelId: DENIED_THREAD,
      channelType: 5,
      text: "@Bot 子区里干活",
      mentionedUserIds: [botUid],
    }));
    await waitForSent(harness, { channelId: DENIED_THREAD, channelType: 5 }, 1);
    const threadReplies = sentToChat(harness, { channelId: DENIED_THREAD, channelType: 5 });
    expect(threadReplies[0]!.text).toContain("本子区未开通");
    expect(threadReplies[0]!.text).toContain("开通本子区");

    await submitWithoutIngress(harness, inbound({
      messageId: "unlisted-thread-grant",
      channelId: DENIED_THREAD,
      channelType: 5,
      text: "开通本群",
      mentionedUserIds: [botUid],
    }));
    await waitForSent(harness, { channelId: DENIED_THREAD, channelType: 5 }, 2);
    expect(sentToChat(harness, { channelId: DENIED_THREAD, channelType: 5 })[1]!.text)
      .toContain("已开通本子区");
    expect(queryRows(harness, "SELECT channel_type, channel_id FROM allowed_chats")).toEqual([
      { channel_type: 5, channel_id: DENIED_THREAD },
    ]);

    // The granted thread runs tasks; the parent group keeps its own unlisted state.
    const threadWork = inbound({
      messageId: "unlisted-thread-work",
      channelId: DENIED_THREAD,
      channelType: 5,
      text: "子区任务",
      mentionedUserIds: [botUid],
    });
    await submitAndWaitForHandoff(harness, threadWork);
    await waitForStatus(harness.service, threadWork, (status) => status.state === "completed");
    expect(harness.workers.prompts.map(({ text }) => text)).toEqual(["子区任务"]);
    expect(sentToChat(harness, { channelId: DENIED_THREAD, channelType: 5 })
      .map(({ text }) => text).filter((text) => text.includes("未开通"))).toHaveLength(1);

    // Granting the parent group covers its threads, including one that never had its own entry.
    await submitWithoutIngress(harness, ownerGroupMessage({
      messageId: "unlisted-parent-grant",
      text: "开通本群",
      mentionedUserIds: [botUid],
    }));
    await waitForSent(harness, { channelId: DENIED_GROUP, channelType: 2 }, 1);
    expect(sentToChat(harness, { channelId: DENIED_GROUP, channelType: 2 })[0]!.text)
      .toContain("已开通本群");
    const sibling = inbound({
      messageId: "unlisted-sibling-thread",
      channelId: "group-denied____topic-2",
      channelType: 5,
      text: "另一个子区任务",
      mentionedUserIds: [botUid],
    });
    await submitAndWaitForHandoff(harness, sibling);
    await waitForStatus(harness.service, sibling, (status) => status.state === "completed");
    expect(harness.workers.prompts.map(({ text }) => text)).toEqual(["子区任务", "另一个子区任务"]);
  } finally {
    await closeCoreHarness(harness);
  }
});

test("an allowlisted chat runs its message and never sees the notice", async () => {
  const harness = await createCoreHarness({ allowedGroupIds: ALLOWED });
  try {
    const allowed = inbound({
      messageId: "unlisted-control-allowed",
      channelId: "group-allowed",
      channelType: 2,
      text: "允许群任务",
      mentionedUserIds: [harness.bot.uid],
    });
    await submitAndWaitForHandoff(harness, allowed);
    await waitForStatus(harness.service, allowed, (status) => status.state === "completed");

    expect(harness.workers.prompts.map(({ text }) => text)).toEqual(["允许群任务"]);
    expect(sentToChat(harness, { channelId: "group-allowed", channelType: 2 })
      .filter(({ text }) => text.includes("未开通"))).toHaveLength(0);
    expect(queryRows(harness, "SELECT 1 FROM allowed_chats")).toHaveLength(0);
  } finally {
    await closeCoreHarness(harness);
  }
});

test("a failed config write-back keeps the durable grant and says so", async () => {
  const writeback = new FakeWriteback();
  writeback.failure = new Error("fixture config is read-only");
  const harness = await createCoreHarness({ allowedGroupIds: ALLOWED, allowlistWriteback: writeback });
  try {
    await submitWithoutIngress(harness, ownerGroupMessage({
      messageId: "unlisted-grant-mirror-failure",
      text: "开通本群",
      mentionedUserIds: [harness.bot.uid],
    }));
    await waitForSent(harness, { channelId: DENIED_GROUP, channelType: 2 }, 1);
    expect(sentToChat(harness, { channelId: DENIED_GROUP, channelType: 2 })[0]!.text)
      .toContain("写回失败");
    expect(queryRows(harness, "SELECT 1 FROM allowed_chats")).toHaveLength(1);

    const work = ownerGroupMessage({
      messageId: "unlisted-work-after-mirror-failure",
      text: "照常干活",
      mentionedUserIds: [harness.bot.uid],
    });
    await submitAndWaitForHandoff(harness, work);
    await waitForStatus(harness.service, work, (status) => status.state === "completed");
    expect(harness.workers.prompts.map(({ text }) => text)).toEqual(["照常干活"]);
  } finally {
    await closeCoreHarness(harness);
  }
});

test("a card grant reports mirror failure without rolling back the durable permission", async () => {
  const writeback = new FakeWriteback();
  writeback.failure = new Error("fixture config is read-only");
  const harness = await createCoreHarness({ allowedGroupIds: ALLOWED, allowlistWriteback: writeback });
  harness.transport.cardSupported = true;
  try {
    await submitWithoutIngress(harness, ownerGroupMessage({
      messageId: "unlisted-card-mirror-failure",
      text: "请开通这个群",
      mentionedUserIds: [harness.bot.uid],
    }));
    await waitForCardCount(harness, 1);
    const card = harness.transport.sentCards[0]!;
    harness.transport.pushBotEvent(cardAction(harness, { eventId: 55 }));
    await harness.transport.waitForCardEdit(() => true);
    await waitForAck(harness, 55);

    expect(queryRows(harness, "SELECT channel_type, channel_id FROM allowed_chats"))
      .toEqual([{ channel_type: 2, channel_id: DENIED_GROUP }]);
    expect(frameText(harness.transport.cardEdits[0]!.frame)).toContain("写回失败");
    expect(frameText(harness.transport.cardEdits[0]!.frame)).toContain("仅在状态盘生效");
    expect(writeback.appended).toEqual([]);
    expect(harness.workers.prompts).toHaveLength(0);
    expect(harness.transport.cardEdits[0]!.messageId).toBe(card.messageId);
  } finally {
    await closeCoreHarness(harness);
  }
});

test("a failed card edit falls back to one persisted result message before event acknowledgement", async () => {
  const harness = await createCoreHarness({ allowedGroupIds: ALLOWED });
  harness.transport.cardSupported = true;
  harness.transport.cardEditFailure = new Error("fixture card edit unavailable");
  try {
    await submitWithoutIngress(harness, ownerGroupMessage({
      messageId: "unlisted-card-edit-failure",
      text: "请开通这个群",
      mentionedUserIds: [harness.bot.uid],
    }));
    await waitForCardCount(harness, 1);
    harness.transport.pushBotEvent(cardAction(harness, { eventId: 56 }));
    await waitForSent(harness, { channelId: DENIED_GROUP, channelType: 2 }, 1);
    await waitForAck(harness, 56);
    expect(harness.transport.cardEdits).toHaveLength(0);
    expect(sentToChat(harness, { channelId: DENIED_GROUP, channelType: 2 })[0]!.text)
      .toContain("已开通本群");
    expect(queryRows(harness, "SELECT 1 FROM allowed_chats")).toHaveLength(1);

    harness.transport.pushBotEvent(cardAction(harness, { eventId: 57 }));
    await waitForAck(harness, 57);
    expect(sentToChat(harness, { channelId: DENIED_GROUP, channelType: 2 }))
      .toHaveLength(1);
  } finally {
    await closeCoreHarness(harness);
  }
});

test("only the current owner may approve the card bound to its exact chat and message", async () => {
  const harness = await createCoreHarness({ allowedGroupIds: ALLOWED });
  harness.transport.cardSupported = true;
  try {
    await submitWithoutIngress(harness, ownerGroupMessage({
      messageId: "grant-card-binding",
      text: "请开通这个群",
      mentionedUserIds: [harness.bot.uid],
    }));
    await waitForCardCount(harness, 1);
    const card = harness.transport.sentCards[0]!;
    expect(card.frame.profile).toBe("octo/v2");
    expect(frameText(card.frame)).toContain("只有 Bot owner 可以确认");
    expect(frameText(card.frame)).toContain("群成员");

    harness.transport.pushBotEvent(cardAction(harness, { eventId: 51, operatorUid: "member-1" }));
    await waitForAck(harness, 51);
    await waitForSent(harness, { channelId: DENIED_GROUP, channelType: 2 }, 1);
    expect(sentToChat(harness, { channelId: DENIED_GROUP, channelType: 2 })[0]!.text)
      .toContain("仅 Bot owner 可以开通");
    expect(queryRows(harness, "SELECT 1 FROM allowed_chats")).toHaveLength(0);

    harness.transport.pushBotEvent(cardAction(harness, {
      eventId: 52,
      channelId: "another-group",
    }));
    harness.transport.pushBotEvent(cardAction(harness, {
      eventId: 53,
      messageId: "another-card",
    }));
    await waitForAck(harness, 53);
    expect(queryRows(harness, "SELECT 1 FROM allowed_chats")).toHaveLength(0);

    harness.transport.pushBotEvent(cardAction(harness, { eventId: 54 }));
    await harness.transport.waitForCardEdit(() => true);
    await waitForAck(harness, 54);
    expect(queryRows(harness, "SELECT channel_type, channel_id, granted_by FROM allowed_chats"))
      .toEqual([{ channel_type: 2, channel_id: DENIED_GROUP, granted_by: "owner-1" }]);
    expect(harness.transport.cardEdits).toHaveLength(1);
    expect(harness.transport.cardEdits[0]!.messageId).toBe(card.messageId);
    expect(harness.workers.prompts).toHaveLength(0);
    expect(queryRows(harness, "SELECT 1 FROM tasks")).toHaveLength(0);
  } finally {
    await closeCoreHarness(harness);
  }
});
test("a persisted owner change invalidates every card sent to the previous owner", async () => {
  const harness = await createCoreHarness({ allowedGroupIds: ALLOWED });
  harness.transport.cardSupported = true;
  try {
    await submitWithoutIngress(harness, ownerGroupMessage({
      messageId: "grant-card-owner-change",
      text: "请开通",
      mentionedUserIds: [harness.bot.uid],
    }));
    await waitForCardCount(harness, 1);
    const store = await openChannelStore(harness.stateDir);
    try {
      store.setOwnerId(harness.bot.uid, "owner-2");
    } finally {
      store.close();
    }

    harness.transport.pushBotEvent(cardAction(harness, { eventId: 55, operatorUid: "owner-1" }));
    harness.transport.pushBotEvent(cardAction(harness, { eventId: 56, operatorUid: "owner-2" }));
    await waitForAck(harness, 56);
    expect(queryRows(harness, "SELECT 1 FROM allowed_chats")).toHaveLength(0);
    expect(harness.transport.cardEdits).toHaveLength(0);
    expect(harness.workers.prompts).toHaveLength(0);
  } finally {
    await closeCoreHarness(harness);
  }
});

test("replaying an accepted card click does not repeat its receipt or grant", async () => {
  const harness = await createCoreHarness({ allowedGroupIds: ALLOWED });
  harness.transport.cardSupported = true;
  try {
    await submitWithoutIngress(harness, ownerGroupMessage({
      messageId: "grant-card-replay",
      text: "开通这个群",
      mentionedUserIds: [harness.bot.uid],
    }));
    await waitForCardCount(harness, 1);
    const click = cardAction(harness, { eventId: 61 });
    harness.transport.pushBotEvent(click);
    await harness.transport.waitForCardEdit(() => true);
    await waitForAck(harness, 61);

    harness.transport.pushBotEvent(cardAction(harness, { eventId: 62 }));
    await waitForAck(harness, 62);
    expect(harness.transport.cardEdits).toHaveLength(1);
    expect(sentToChat(harness, { channelId: DENIED_GROUP, channelType: 2 })).toHaveLength(0);
    expect(queryRows(harness, "SELECT 1 FROM allowed_chats")).toHaveLength(1);
    expect(queryRows(harness, "SELECT 1 FROM tasks")).toHaveLength(0);
  } finally {
    await closeCoreHarness(harness);
  }
});

test("a durable card grant resumes its pending result after restart before replay is acknowledged", async () => {
  const writeback = new FakeWriteback();
  const first = await createCoreHarness({ allowedGroupIds: ALLOWED, allowlistWriteback: writeback });
  let click: BotEvent | null = null;
  first.transport.cardSupported = true;
  try {
    await submitWithoutIngress(first, ownerGroupMessage({
      messageId: "grant-card-recovery",
      text: "开通这个群",
      mentionedUserIds: [first.bot.uid],
    }));
    await waitForCardCount(first, 1);
    click = cardAction(first, { eventId: 71 });
    first.transport.cardEditFailure = new Error("temporary card edit failure");
    first.transport.setOffline();
    first.transport.pushBotEvent(click);
    await waitForCondition(() => queryRows(first, "SELECT 1 FROM allowed_chats").length === 1);
    await settle();
    expect(first.transport.ackedEvents).toHaveLength(0);
    expect(writeback.appended).toEqual([DENIED_GROUP]);
  } finally {
    await closeCoreHarness(first, { remove: false });
  }

  const recovered = await createCoreHarness({
    root: first.root,
    allowedGroupIds: ALLOWED,
    allowlistWriteback: writeback,
  });
  try {
    expect(recovered.transport.cardEdits).toHaveLength(1);
    expect(recovered.transport.cardEdits[0]!.messageId).toBe("card-1");
    expect(writeback.appended).toEqual([DENIED_GROUP]);

    if (click === null) throw new Error("accepted card click was not created");
    recovered.transport.pushBotEvent(click);
    await waitForAck(recovered, 71);
    expect(recovered.transport.cardEdits).toHaveLength(1);
    expect(recovered.transport.ackedEvents).toEqual([71]);
    expect(recovered.workers.prompts).toHaveLength(0);
    expect(queryRows(recovered, "SELECT 1 FROM allowed_chats")).toHaveLength(1);
  } finally {
    await closeCoreHarness(recovered);
  }
});

test("an unaccepted old card after restart only asks its owner to mention the bot again", async () => {
  const first = await createCoreHarness({ allowedGroupIds: ALLOWED });
  first.transport.cardSupported = true;
  let click: BotEvent | null = null;
  try {
    await submitWithoutIngress(first, ownerGroupMessage({
      messageId: "grant-card-old",
      text: "请开通",
      mentionedUserIds: [first.bot.uid],
    }));
    await waitForCardCount(first, 1);
    click = cardAction(first, { eventId: 81 });
  } finally {
    await closeCoreHarness(first, { remove: false });
  }

  const recovered = await createCoreHarness({ root: first.root, allowedGroupIds: ALLOWED });
  try {
    if (click === null) throw new Error("old card click was not created");
    recovered.transport.pushBotEvent(click);
    await waitForAck(recovered, 81);
    await waitForSent(recovered, { channelId: DENIED_GROUP, channelType: 2 }, 1);
    expect(sentToChat(recovered, { channelId: DENIED_GROUP, channelType: 2 })[0]!.text)
      .toContain("重新 @Fixture Bot");
    expect(queryRows(recovered, "SELECT 1 FROM allowed_chats")).toHaveLength(0);
    expect(queryRows(recovered, "SELECT 1 FROM tasks")).toHaveLength(0);
  } finally {
    await closeCoreHarness(recovered);
  }
});

test("a failed text-grant claim is reclaimed before the now-authorized phrase can route as work", async () => {
  const harness = await createCoreHarness({ allowedGroupIds: ALLOWED });
  try {
    const original = ownerGroupMessage({
      messageId: "grant-text-failed-claim",
      text: "开通本群",
      mentionedUserIds: [harness.bot.uid],
    });
    await submitWithoutIngress(harness, original);
    await waitForCondition(() =>
      queryRows(harness, "SELECT 1 FROM inbound WHERE message_id = 'grant-text-failed-claim' AND command_state = 'completed'")
        .length === 1);
    const database = new Database(join(harness.stateDir, "channel.sqlite"));
    try {
      database.query(
        "UPDATE inbound SET command_state = 'failed' WHERE bot_uid = ? AND channel_type = 2 AND channel_id = ? AND message_id = ?",
      ).run(harness.bot.uid, DENIED_GROUP, original.messageId);
    } finally {
      database.close();
    }

    await submitWithoutIngress(harness, original);
    await waitForCondition(() =>
      queryRows(harness, "SELECT 1 FROM inbound WHERE message_id = 'grant-text-failed-claim' AND command_state = 'completed'")
        .length === 1);
    expect(harness.workers.prompts).toHaveLength(0);
    expect(queryRows(harness, "SELECT kind, command_state FROM inbound WHERE message_id = 'grant-text-failed-claim'"))
      .toEqual([{ kind: "command", command_state: "completed" }]);

    const fresh = ownerGroupMessage({
      messageId: "grant-text-fresh-authorized",
      text: "开通本群",
      mentionedUserIds: [harness.bot.uid],
    });
    await submitAndWaitForHandoff(harness, fresh);
    await waitForStatus(harness.service, fresh, (status) => status.state === "completed");
    expect(harness.workers.prompts.map(({ text }) => text)).toEqual(["开通本群"]);
  } finally {
    await closeCoreHarness(harness);
  }
});

test("a redelivered text claim stays a command while a new matching message is ordinary work", async () => {
  const harness = await createCoreHarness({ allowedGroupIds: ALLOWED });
  try {
    const original = ownerGroupMessage({
      messageId: "grant-text-redelivery",
      text: "开通本群",
      mentionedUserIds: [harness.bot.uid],
    });
    await submitWithoutIngress(harness, original);
    await waitForSent(harness, { channelId: DENIED_GROUP, channelType: 2 }, 1);
    await submitWithoutIngress(harness, original);

    const fresh = ownerGroupMessage({
      messageId: "grant-text-new-message",
      text: "开通本群",
      mentionedUserIds: [harness.bot.uid],
    });
    await submitAndWaitForHandoff(harness, fresh);
    await waitForStatus(harness.service, fresh, (status) => status.state === "completed");
    expect(harness.workers.prompts.map(({ text }) => text)).toEqual(["开通本群"]);
    expect(queryRows(harness, "SELECT message_id, kind FROM inbound ORDER BY message_id")).toEqual([
      { message_id: "grant-text-new-message", kind: "task" },
      { message_id: "grant-text-redelivery", kind: "command" },
    ]);
  } finally {
    await closeCoreHarness(harness);
  }
});

test("a failed interactive send falls back to the exact text passphrase without granting", async () => {
  const harness = await createCoreHarness({ allowedGroupIds: ALLOWED });
  harness.transport.cardSupported = true;
  harness.transport.cardSendFailure = new Error("card deployment unavailable");
  try {
    await submitWithoutIngress(harness, ownerGroupMessage({
      messageId: "grant-card-fallback",
      text: "请处理这里",
      mentionedUserIds: [harness.bot.uid],
    }));
    await waitForSent(harness, { channelId: DENIED_GROUP, channelType: 2 }, 1);
    expect(harness.transport.sentCards).toHaveLength(0);
    expect(sentToChat(harness, { channelId: DENIED_GROUP, channelType: 2 })[0]!.text)
      .toContain("@Fixture Bot 开通本群");
    expect(queryRows(harness, "SELECT 1 FROM allowed_chats")).toHaveLength(0);
    expect(harness.workers.prompts).toHaveLength(0);
  } finally {
    await closeCoreHarness(harness);
  }
});

async function waitForCondition(condition: () => boolean): Promise<void> {
  for (let attempt = 0; attempt < 10_000; attempt += 1) {
    if (condition()) return;
    await nextImmediate();
  }
  throw new Error("condition did not become true");
}
test("events during card send reject non-owners promptly and bind owner clicks only after receipt", async () => {
  const transport = new DelayedCardTransport({
    bot: { uid: "bot-inflight-grant", name: "Fixture Bot" },
  });
  transport.cardSupported = true;
  const harness = await createCoreHarness({ transport, allowedGroupIds: ALLOWED });
  let fakeTimers = false;
  try {
    const message = ownerGroupMessage({
      messageId: "grant-card-inflight",
      text: "请开通",
      mentionedUserIds: [harness.bot.uid],
    });
    const inbound = submitWithoutIngress(harness, message);
    const frame = await transport.cardSendStarted.promise;
    expect(await readIngressJob(ingressObserver(harness), message)).toBeNull();
    const actionId = actionIdFromFrame(frame);
    const memberClick = cardActionEvent(90, {
      actionId,
      messageId: "card-1",
      channelType: 2,
      channelId: DENIED_GROUP,
      operatorUid: "member-1",
      inputs: {},
    });
    const ownerClick = cardActionEvent(91, {
      actionId,
      messageId: "card-1",
      channelType: 2,
      channelId: DENIED_GROUP,
      operatorUid: "owner-1",
      inputs: {},
    });
    vi.useFakeTimers();
    fakeTimers = true;
    transport.pushBotEvent(memberClick);
    transport.pushBotEvent(ownerClick);

    await transport.eventDelivered.promise;
    await transport.waitForAck(90);
    for (let tick = 0; tick < 30; tick++) await Promise.resolve();
    const promptSent = transport.sent.some(({ text }) => text.includes("仅 Bot owner 可以开通"));
    const promptQueued = queryRows(
      harness,
      "SELECT 1 FROM outbox WHERE text = '仅 Bot owner 可以开通。' AND state IN ('pending', 'unknown')",
    ).length > 0;
    expect(promptSent || promptQueued).toBe(true);
    expect(transport.ackedEvents).not.toContain(91);
    expect(queryRows(harness, "SELECT 1 FROM allowed_chats")).toHaveLength(0);

    transport.releaseCardSend.resolve();
    await inbound;
    for (let attempt = 0; attempt < 2 && !transport.ackedEvents.includes(91); attempt++) {
      vi.advanceTimersByTime(5_000);
      for (let tick = 0; tick < 30; tick++) await Promise.resolve();
    }
    expect(transport.ackedEvents).toContain(91);
    await transport.waitForCardEdit(() => true);
    expect(queryRows(harness, "SELECT channel_id FROM allowed_chats"))
      .toEqual([{ channel_id: DENIED_GROUP }]);
    expect(harness.workers.prompts).toHaveLength(0);
  } finally {
    transport.releaseCardSend.resolve();
    await closeCoreHarness(harness);
    if (fakeTimers) vi.useRealTimers();
  }
});

test("a thread card names and grants only its own scope", async () => {
  const harness = await createCoreHarness({ allowedGroupIds: ALLOWED });
  harness.transport.cardSupported = true;
  try {
    const message = inbound({
      messageId: "grant-thread-card",
      channelId: DENIED_THREAD,
      channelType: 5,
      text: "请开通子区",
      mentionedUserIds: [harness.bot.uid],
    });
    await submitWithoutIngress(harness, message);
    await waitForCardCount(harness, 1);
    expect(frameText(harness.transport.sentCards[0]!.frame)).toContain("本子区");
    expect(
      (harness.transport.sentCards[0]!.frame.card["actions"] as Array<{ title: string }>)[0]!.title,
    ).toBe("开通本子区");

    harness.transport.pushBotEvent(cardAction(harness, {
      eventId: 101,
      channelId: DENIED_THREAD,
      channelType: 5,
    }));
    await harness.transport.waitForCardEdit(() => true);
    await waitForAck(harness, 101);
    expect(queryRows(harness, "SELECT channel_type, channel_id FROM allowed_chats"))
      .toEqual([{ channel_type: 5, channel_id: DENIED_THREAD }]);
  } finally {
    await closeCoreHarness(harness);
  }
});
