import { expect, test } from "bun:test";
import { randomUUID } from "node:crypto";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import {
  createConfigMenuDesk,
  CONFIG_MENU_MAX_SESSIONS,
  CONFIG_MENU_TTL_MS,
  type ConfigApplyReservation,
  type ConfigCardActionSnapshot,
} from "../src/commands/command-cards.ts";
import type { CommandCardView } from "../src/cards/command-card.ts";
import type { BotCardAction, BotIdentity, CardFrame, ChatTarget, InboundMessage, SendReceipt } from "../src/contracts.ts";
import { openChannelStore } from "../src/store/index.ts";
import { cardActionEvent, closeCoreHarness, createCoreHarness, FakeTransport, inbound, waitForIngressJob, waitForStatus } from "./fixtures/core-fakes.ts";
import type { CoreHarness, SentCard } from "./fixtures/core-fakes.ts";

function nodes(value: unknown): Record<string, unknown>[] {
  if (Array.isArray(value)) return value.flatMap(nodes);
  if (!value || typeof value !== "object") return [];
  return [value as Record<string, unknown>, ...Object.values(value).flatMap(nodes)];
}

function select(card: SentCard, value: string): BotCardAction {
  const all = nodes(card.frame.card);
  const input = all.find(node => node.type === "Input.ChoiceSet");
  const submit = all.find(node => node.type === "Action.Submit");
  expect(input).toBeDefined();
  expect(submit).toBeDefined();
  expect(input!.choices).toContainEqual(expect.objectContaining({ value }));
  return {
    ...card.target,
    messageId: card.messageId,
    actionId: String(submit!.id),
    operatorUid: "owner-1",
    inputs: { [String(input!.id)]: value },
  };
}

/** The click as it arrives, with fields overridden: binding checks read exactly these values. */
function withOverrides(action: BotCardAction, overrides: Partial<BotCardAction>): BotCardAction {
  return { ...action, ...overrides };
}

/** Every text the frame paints; the terminal card's receipt is asserted through these. */
function cardTexts(frame: CardFrame): string[] {
  return nodes(frame.card).flatMap(node => (typeof node.text === "string" ? [node.text] : []));
}

/** Whether the frame still carries something clickable: a terminal card carries nothing. */
function hasControls(frame: CardFrame): boolean {
  return nodes(frame.card).some(node =>
    node.type === "Action.Submit" || String(node.type ?? "").startsWith("Input."));
}

/** The interactive configuration cards among everything this transport carried. */
function configCards(transport: FakeTransport): SentCard[] {
  return transport.sentCards.filter(card =>
    nodes(card.frame.card).some(node => String(node.id ?? "").startsWith("cfg:")));
}
const CARD_TEST_WAIT_TIMEOUT_MS = 3_000;

/** Bound card-test waits so failures reach `finally` and release the Bot harness lock. */
async function waitForCardTest<T>(promise: Promise<T>, operation: string): Promise<T> {
  const completed = Promise.withResolvers<{ kind: "success"; value: T } | { kind: "failure"; error: unknown }>();
  void promise.then(
    value => completed.resolve({ kind: "success", value }),
    error => completed.resolve({ kind: "failure", error }),
  );
  const deadline = performance.now() + CARD_TEST_WAIT_TIMEOUT_MS;
  while (performance.now() < deadline) {
    const turn = Promise.withResolvers<void>();
    setImmediate(turn.resolve);
    const outcome = await Promise.race([completed.promise, turn.promise.then(() => null)]);
    if (outcome === null) continue;
    if (outcome.kind === "failure") throw outcome.error;
    return outcome.value;
  }
  throw new Error(`${operation} timed out after ${CARD_TEST_WAIT_TIMEOUT_MS} ms`);
}

function waitForBotEventAck(transport: FakeTransport, eventId: number): Promise<number> {
  return waitForCardTest(transport.waitForAck(eventId), `Bot event ${eventId} ACK`);
}

/** Display completion is downstream of Bot-event ACK and proves the action has been applied. */
function waitForCardResult(transport: FakeTransport, card: SentCard) {
  return waitForCardTest(
    transport.waitForCardEdit(edit => edit.messageId === card.messageId),
    `configuration card ${card.messageId} result`,
  );
}

/** Text delivered to one chat, in delivery order. */
function chatTexts(transport: FakeTransport, target: ChatTarget): string[] {
  return transport.sent
    .filter(message =>
      message.target.channelType === target.channelType && message.target.channelId === target.channelId)
    .map(message => message.text);
}



/** Wait for an asynchronous card-event notice without mistaking an earlier chat reply for it. */
function waitForChatText(transport: FakeTransport, target: ChatTarget, text: string) {
  const previousIds = new Set(transport.listRemoteText(target).map(({ messageId }) => messageId));
  return waitForCardTest(
    transport.waitForRemoteText(entry =>
      !previousIds.has(entry.messageId)
      && entry.target.channelType === target.channelType
      && entry.target.channelId === target.channelId
      && entry.text.includes(text)),
    `chat notice ${text}`,
  );
}
async function submitForConfigCard(
  harness: CoreHarness,
  transport: FakeTransport,
  message: InboundMessage,
  target: ChatTarget,
  text: string,
): Promise<SentCard> {
  const existingIds = new Set(transport.sentCards.map(({ messageId }) => messageId));
  const card = waitForCardTest(transport.waitForCardSend((entry) =>
    !existingIds.has(entry.messageId)
    && entry.target.channelType === target.channelType
    && entry.target.channelId === target.channelId
    && (cardTexts(entry.frame).some((line) => line.includes(text)) || entry.frame.plain.includes(text))), "configuration card send");
  await waitForCardTest(harness.service.submitInbound(message), "inbound acceptance");
  const sentCard = await card;
  // `sendCard` records the fake frame before `open` binds its receipt messageId; wait for the
  // menu command handoff before allowing a click, just as the real event consumer must do.
  await waitForIngressJob(harness, message);
  return sentCard;
}

async function submitForChatText(
  harness: CoreHarness,
  transport: FakeTransport,
  message: InboundMessage,
  target: ChatTarget,
  text: string,
): Promise<void> {
  const existingIds = new Set(transport.listRemoteText(target).map(({ messageId }) => messageId));
  const reply = waitForCardTest(transport.waitForRemoteText((entry) =>
    !existingIds.has(entry.messageId)
    && entry.target.channelType === target.channelType
    && entry.target.channelId === target.channelId
    && entry.text.includes(text)), `chat reply ${text}`);
  await waitForCardTest(harness.service.submitInbound(message), "inbound acceptance");
  await reply;
}

function cardTransport(): FakeTransport {
  const transport = new FakeTransport();
  transport.cardSupported = true;
  return transport;
}

/** Assert the user-facing model policy and catalog without pinning incidental prose. */
function expectSwitchListing(texts: readonly string[]): void {
  const latest = texts.at(-1);
  expect(latest).toContain("不指定");
  expect(latest).toContain("fixture：default、fast");
  expect(latest).toContain("other：fast");
}

/** The first card send is held until released, so a click can precede its receipt. */
class GatedTransport extends FakeTransport {
  private readonly gate = Promise.withResolvers<void>();
  private readonly started = Promise.withResolvers<void>();
  private gateOnce = true;
  /** Resolves once a card frame has reached the transport and is being held. */
  readonly held: Promise<void> = this.started.promise;

  releaseSend(): void {
    this.gate.resolve();
  }

  override async sendCard(
    target: ChatTarget,
    frame: CardFrame,
    clientMsgNo: string,
    signal?: AbortSignal,
  ): Promise<SendReceipt | null> {
    if (this.gateOnce) {
      this.gateOnce = false;
      this.started.resolve();
      await this.gate.promise;
    }
    return await super.sendCard(target, frame, clientMsgNo, signal);
  }
}

// Catches cards that display controls but never apply the selected model to future turns.
test("model card selection persists and reaches the next worker without a generation turn", async () => {
  const transport = new FakeTransport();
  transport.cardSupported = true;
  const harness = await createCoreHarness({ transport });
  try {
    await submitForConfigCard(harness, transport, inbound({ messageId: "model-menu", text: "/switch" }), { channelId: "dm-owner", channelType: 1 }, "下次启动策略");
    expect(transport.sentCards).toHaveLength(1);
    const card = transport.sentCards[0]!;
    expect(cardTexts(card.frame)).toContain("下次启动策略：不指定（由 OMP 自行决定）");
    const applied = waitForCardResult(transport, card);
    transport.pushBotEvent(cardActionEvent(1, select(card, "fixture/fast")));
    await waitForBotEventAck(transport, 1);
    const edit = await applied;
    expect(cardTexts(edit.frame).join("\n")).toContain("fixture/fast");
    expect(hasControls(edit.frame)).toBe(false);
    const store = await openChannelStore(harness.stateDir);
    try {
      expect(store.getChatModel(harness.bot.uid, card.target)).toBe("fixture/fast");
    } finally { store.close(); }
    expect(harness.workers.prompts).toHaveLength(0);
    await waitForCardTest(harness.service.submitInbound(inbound({ messageId: "next-turn", text: "use selected model" })), "next-turn admission");
    await waitForCardTest(harness.workers.waitForPromptCount(1), "next-turn worker prompt");
    expect(harness.workers.creations.at(-1)?.model).toBe("fixture/fast");
  } finally { await closeCoreHarness(harness); }
});

// Catches member authorization leaks and reapplication of an already consumed card.
test("mode card is owner-only and a replay cannot overwrite a later text setting", async () => {
  const transport = new FakeTransport();
  transport.cardSupported = true;
  const harness = await createCoreHarness({ transport });
  const target = { channelType: 2 as const, channelId: "group-allowed" };
  const store = await openChannelStore(harness.stateDir);
  try {
    await submitForConfigCard(harness, transport, inbound({ ...target, messageId: "mode-menu", text: "/mode", mentionedUserIds: [harness.bot.uid] }), target, "当前追加模式");
    expect(transport.sentCards).toHaveLength(1);
    const action = select(transport.sentCards[0]!, "queue");
    transport.pushBotEvent(cardActionEvent(1, { ...action, operatorUid: "member-1" }));
    await waitForBotEventAck(transport, 1);
    expect(store.getChatMode(harness.bot.uid, target)).toBe("steer");
    const applied = waitForCardResult(transport, transport.sentCards[0]!);
    transport.pushBotEvent(cardActionEvent(2, action));
    await waitForBotEventAck(transport, 2);
    const edit = await applied;
    expect(cardTexts(edit.frame).join("\n")).toContain("已设为 queue");
    expect(hasControls(edit.frame)).toBe(false);
    await submitForChatText(harness, transport, inbound({ ...target, messageId: "mode-text", text: "/mode steer", mentionedUserIds: [harness.bot.uid] }), target, "追加模式已设为 steer");
    transport.pushBotEvent(cardActionEvent(3, action));
    await waitForBotEventAck(transport, 3);
    expect(store.getChatMode(harness.bot.uid, target)).toBe("steer");
    expect(harness.workers.prompts).toHaveLength(0);
  } finally { store.close(); await closeCoreHarness(harness); }
});

// Catches a "default" choice that only looks like the text command's semantics.
test("the switch card's default choice clears the chat model for the next worker", async () => {
  const transport = cardTransport();
  const harness = await createCoreHarness({ transport });
  const target = { channelId: "dm-default", channelType: 1 as const };
  try {
    await submitForChatText(harness, transport, inbound({ ...target, messageId: "default-set", text: "/switch fixture/fast" }), target, "fixture/fast");
    await submitForConfigCard(harness, transport, inbound({ ...target, messageId: "default-menu", text: "/switch" }), target, "下次启动策略");
    const card = configCards(transport)[0]!;
    const applied = waitForCardResult(transport, card);
    transport.pushBotEvent(cardActionEvent(1, select(card, "default")));
    await waitForBotEventAck(transport, 1);
    const edit = await applied;
    expect(cardTexts(edit.frame).join("\n")).toContain("下次启动策略：不指定（由 OMP 自行决定）");
    expect(hasControls(edit.frame)).toBe(false);
    const store = await openChannelStore(harness.stateDir);
    try {
      expect(store.getChatModel(harness.bot.uid, target)).toBeNull();
    } finally { store.close(); }
    await waitForCardTest(harness.service.submitInbound(inbound({ ...target, messageId: "default-task", text: "run the fixture" })), "default task admission");
    await waitForCardTest(harness.workers.waitForPromptCount(1), "default task worker prompt");
    expect(harness.workers.creations.at(-1)?.model).toBeUndefined();
  } finally { await closeCoreHarness(harness); }
});

// Catches a card that persists a model for later workers while leaving a running session behind.
test("a switch click moves the running session in place without starting a probe", async () => {
  const running = Promise.withResolvers<void>();
  const finish = Promise.withResolvers<void>();
  const transport = cardTransport();
  const harness = await createCoreHarness({
    transport,
    prompt: async ({ append, text }) => {
      if (text === "long-running") {
        running.resolve();
        await finish.promise;
      }
      await append("effects.log", `${text}\n`);
    },
  });
  const target = { channelId: "dm-live-card", channelType: 1 as const };
  try {
    await waitForCardTest(harness.service.submitInbound(inbound({ ...target, messageId: "live-start", text: "long-running" })), "running task admission");
    await waitForCardTest(running.promise, "running task start");
    await submitForConfigCard(harness, transport, inbound({ ...target, messageId: "live-menu", text: "/switch" }), target, "下次启动策略");
    const card = configCards(transport)[0]!;
    const applied = waitForCardResult(transport, card);
    transport.pushBotEvent(cardActionEvent(1, select(card, "other/fast")));
    await waitForBotEventAck(transport, 1);
    const edit = await applied;
    expect(harness.workers.workers[0]?.switchDispatches).toEqual(["other/fast"]);
    const store = await openChannelStore(harness.stateDir);
    try {
      expect(store.getChatModel(harness.bot.uid, target)).toBe("other/fast");
    } finally { store.close(); }
    expect(cardTexts(edit.frame)).toContain("已切换本聊天模型为 other/fast（运行中会话已同步切换）。");
    // 菜单由活会话自己回答，点击也走同一个会话：全程没有第二个 worker。
    expect(harness.workers.creations).toHaveLength(1);
    finish.resolve();
    await waitForCardTest(waitForStatus(harness.service, target, status => status.state === "completed"), "running task completion");
  } finally {
    finish.resolve();
    await closeCoreHarness(harness);
  }
});

// Catches a card path that answers a chat's pending question or starts a generation turn.
test("a configuration click leaves the chat's pending question and tasks untouched", async () => {
  const transport = cardTransport();
  const harness = await createCoreHarness({
    transport,
    prompt: async ({ append, promptContext }) => {
      const ask = promptContext?.tools?.find(({ name }) => name === "ask");
      if (!ask) throw new Error("ask host tool missing");
      await append("answer.log", await ask.execute(
        { questions: [{ id: "q1", question: "Which target?", options: [{ label: "a" }, { label: "b" }] }] },
        new AbortController().signal,
      ));
    },
  });
  const target = { channelId: "dm-ask-card", channelType: 1 as const };
  try {
    await waitForCardTest(harness.service.submitInbound(inbound({ ...target, messageId: "ask-start", text: "needs an answer" })), "ask task admission");
    await waitForCardTest(harness.workers.waitForPromptCount(1), "ask worker prompt");
    const pending = await waitForCardTest(waitForStatus(harness.service, target, status => status.pendingQuestionIds.length > 0), "pending question");
    await submitForConfigCard(harness, transport, inbound({ ...target, messageId: "ask-menu", text: "/switch fixture" }), target, "下次启动策略");
    const card = configCards(transport)[0]!;
    const applied = waitForCardResult(transport, card);
    transport.pushBotEvent(cardActionEvent(1, select(card, "fixture/fast")));
    await waitForBotEventAck(transport, 1);
    await applied;
    const store = await openChannelStore(harness.stateDir);
    try {
      expect(store.getChatModel(harness.bot.uid, target)).toBe("fixture/fast");
      expect(store.countQueuedTasks(harness.bot.uid, target)).toBe(0);
      expect(store.pendingQuestionIds(harness.bot.uid, target)).toEqual(pending.pendingQuestionIds);
    } finally { store.close(); }
    expect(harness.workers.prompts).toHaveLength(1);
    // 问答路径本身不受影响：同聊天的普通回复仍然作为答案，问题正常结束。
    await waitForCardTest(harness.service.submitInbound(inbound({ ...target, messageId: "ask-answer", text: "b" })), "ask answer admission");
    await waitForCardTest(waitForStatus(harness.service, target, status => status.pendingQuestionIds.length === 0), "ask completion");
  } finally { await closeCoreHarness(harness); }
});

// Catches clicks that escape their card: another operator, message, chat or a never-issued session.
test("clicks that do not match the card's exact binding change nothing and never consume it", async () => {
  const transport = cardTransport();
  const harness = await createCoreHarness({ transport });
  const target = { channelType: 2 as const, channelId: "group-allowed" };
  const other = { channelType: 2 as const, channelId: "group-a" };
  const store = await openChannelStore(harness.stateDir);
  try {
    await submitForConfigCard(harness, transport, inbound({
      ...target, messageId: "binding-menu", text: "/mode", mentionedUserIds: [harness.bot.uid],
    }), target, "当前追加模式");
    const action = select(configCards(transport)[0]!, "queue");
    const forged = [
      withOverrides(action, { operatorUid: "member-9" }),
      withOverrides(action, { messageId: "another-message" }),
      withOverrides(action, { channelId: "group-a" }),
      withOverrides(action, { actionId: `cfg:mode:${randomUUID()}` }),
    ];
    const staleNotice = waitForChatText(transport, target, "已失效");
    for (const [index, click] of forged.entries()) {
      transport.pushBotEvent(cardActionEvent(index + 1, click));
      await waitForBotEventAck(transport, index + 1);
      expect(store.getChatMode(harness.bot.uid, target)).toBe("steer");
      expect(store.getChatMode(harness.bot.uid, other)).toBe("steer");
    }
    // 只有从未发放的那一次给出指引；其余伪造点击静默，且都没有消耗 owner 的卡片。
    await staleNotice;
    expect(chatTexts(transport, target)).toHaveLength(1);
    expect(chatTexts(transport, target)[0]).toContain("已失效");
    expect(chatTexts(transport, other)).toEqual([]);
    const applied = waitForCardResult(transport, configCards(transport)[0]!);
    transport.pushBotEvent(cardActionEvent(9, action));
    await waitForBotEventAck(transport, 9);
    await applied;
    expect(store.getChatMode(harness.bot.uid, target)).toBe("queue");
    expect(harness.workers.prompts).toHaveLength(0);
  } finally { store.close(); await closeCoreHarness(harness); }
});

// Catches a card that applies a synonym (or anything else) it never offered.
test("a card applies only the values it issued", async () => {
  const transport = cardTransport();
  const harness = await createCoreHarness({ transport });
  const target = { channelId: "dm-issued", channelType: 1 as const };
  const store = await openChannelStore(harness.stateDir);
  try {
    await submitForConfigCard(harness, transport, inbound({ ...target, messageId: "issued-menu", text: "/mode" }), target, "当前追加模式");
    const action = select(configCards(transport)[0]!, "steer");
    // "default" 是文本命令的同义写法，但不是这张卡发放过的取值。
    const invalidReply = waitForChatText(transport, target, "未收到本卡片提供的选项");
    transport.pushBotEvent(cardActionEvent(1, withOverrides(action, { inputs: { config_value: "default" } })));
    await waitForBotEventAck(transport, 1);
    await invalidReply;
    expect(store.getChatMode(harness.bot.uid, target)).toBe("steer");
    expect(chatTexts(transport, target)).toEqual([
      "未收到本卡片提供的选项，本次没有修改；重新发送 /mode 可再次打开配置卡片。",
    ]);
  } finally { store.close(); await closeCoreHarness(harness); }
});

// Catches card sends that swallow the command's only answer when the frame cannot be carried.
test("a deployment that cannot carry cards keeps the command's original text reply", async () => {
  const unsupported = new FakeTransport();
  const plain = await createCoreHarness({ transport: unsupported });
  try {
    await submitForChatText(plain, unsupported, inbound({ messageId: "plain-switch", text: "/switch" }), { channelId: "dm-owner", channelType: 1 }, "fixture：default");
    expectSwitchListing(chatTexts(unsupported, { channelId: "dm-owner", channelType: 1 }));
    expect(unsupported.sentCards).toHaveLength(0);
  } finally { await closeCoreHarness(plain); }

  const transport = cardTransport();
  let refusedPlain = "";
  transport.cardSendRefusal = frame => { refusedPlain = frame.plain; return true; };
  const refused = await createCoreHarness({ transport });
  try {
    await submitForChatText(refused, transport, inbound({ messageId: "refused-switch", text: "/switch" }), { channelId: "dm-owner", channelType: 1 }, "fixture：default");
    expect(chatTexts(transport, { channelId: "dm-owner", channelType: 1 })).toEqual([refusedPlain]);
    const switchPlain = refusedPlain;
    await submitForChatText(refused, transport, inbound({ messageId: "refused-mode", text: "/mode" }), { channelId: "dm-owner", channelType: 1 }, "当前追加模式：steer");
    expect(chatTexts(transport, { channelId: "dm-owner", channelType: 1 }).at(-1)).toContain("当前追加模式：steer");
    // 抛错的发送同样回退，并且不留下任何可点击的会话。
    transport.cardSendRefusal = undefined;
    transport.cardSendFailure = new Error("card transport down");
    await submitForChatText(refused, transport, inbound({ messageId: "failed-switch", text: "/switch" }), { channelId: "dm-owner", channelType: 1 }, switchPlain);
    expect(chatTexts(transport, { channelId: "dm-owner", channelType: 1 }).at(-1)).toBe(switchPlain);
    expect(transport.sentCards).toHaveLength(0);
  } finally { await closeCoreHarness(refused); }
});

// Catches a stale card send that lands after a newer command and silently becomes authoritative.
test("a card whose send lands after a newer command never becomes live", async () => {
  const transport = new GatedTransport();
  transport.cardSupported = true;
  const harness = await createCoreHarness({ transport });
  const target = { channelType: 2 as const, channelId: "group-allowed" };
  const store = await openChannelStore(harness.stateDir);
  try {
    const first = harness.service.submitInbound(inbound({
      ...target, messageId: "stale-menu", text: "/mode", mentionedUserIds: [harness.bot.uid],
    }));
    await waitForCardTest(transport.held, "held configuration card send");
    await submitForChatText(harness, transport, inbound({
      ...target, messageId: "fresh-mode", text: "/mode queue", mentionedUserIds: [harness.bot.uid],
    }), target, "追加模式已设为 queue");
    const staleCard = waitForCardTest(transport.waitForCardSend((card) =>
      card.target.channelType === target.channelType
      && card.target.channelId === target.channelId
      && (cardTexts(card.frame).some((text) => text.includes("当前追加模式")) || card.frame.plain.includes("当前追加模式"))), "late stale card send");
    transport.releaseSend();
    await waitForCardTest(first, "stale menu admission");
    const sentCard = await staleCard;
    // 文本命令是权威：卡片确实发了出去（发送在途），但它属于被取代的会话。
    expect(transport.sentCards).toHaveLength(1);
    expect(chatTexts(transport, target)).toEqual(["追加模式已设为 queue：运行中的新消息会排到当前回合之后。"]);
    transport.pushBotEvent(cardActionEvent(1, select(sentCard, "steer")));
    await waitForBotEventAck(transport, 1);
    expect(store.getChatMode(harness.bot.uid, target)).toBe("queue");
  } finally {
    transport.releaseSend();
    store.close();
    await closeCoreHarness(harness);
  }
});

// Catches cross-process residue: a card from a previous service process must be inert.
test("a card from a previous process is inert after restart", async () => {
  const first = await createCoreHarness({ transport: cardTransport() });
  const root = first.root;
  const target = { channelId: "dm-restart-card", channelType: 1 as const };
  const stale: { action: BotCardAction | null } = { action: null };
  try {
    await submitForConfigCard(first, first.transport, inbound({ ...target, messageId: "pre-restart-menu", text: "/mode" }), target, "当前追加模式");
    stale.action = select(configCards(first.transport)[0]!, "queue");
  } finally { await closeCoreHarness(first, { remove: false }); }
  if (stale.action === null) throw new Error("fixture card missing");

  const restarted = await createCoreHarness({ root, transport: cardTransport() });
  try {
    const staleNotice = waitForChatText(restarted.transport, target, "已失效");
    restarted.transport.pushBotEvent(cardActionEvent(1, stale.action));
    await waitForBotEventAck(restarted.transport, 1);
    await staleNotice;
    const store = await openChannelStore(restarted.stateDir);
    try {
      expect(store.getChatMode(restarted.bot.uid, target)).toBe("steer");
    } finally { store.close(); }
    expect(chatTexts(restarted.transport, target).at(-1)).toContain("已失效");
    // 新进程的新卡片照常工作：跨进程残留只是被拒绝，不是把功能一起关掉。
    await submitForConfigCard(restarted, restarted.transport, inbound({ ...target, messageId: "post-restart-menu", text: "/mode" }), target, "当前追加模式");
    const card = configCards(restarted.transport)[0]!;
    const applied = waitForCardResult(restarted.transport, card);
    restarted.transport.pushBotEvent(cardActionEvent(2, select(card, "queue")));
    await waitForBotEventAck(restarted.transport, 2);
    await applied;
    const after = await openChannelStore(restarted.stateDir);
    try {
      expect(after.getChatMode(restarted.bot.uid, target)).toBe("queue");
    } finally { after.close(); }
  } finally { await closeCoreHarness(restarted); }
});

// Catches a newer menu that leaves the previous card clickable, or a click that revives it.
test("a newer menu supersedes the previous card of the same kind", async () => {
  const transport = cardTransport();
  const harness = await createCoreHarness({ transport });
  const target = { channelId: "dm-supersede", channelType: 1 as const };
  const store = await openChannelStore(harness.stateDir);
  try {
    await submitForConfigCard(harness, transport, inbound({ ...target, messageId: "menu-one", text: "/mode" }), target, "当前追加模式");
    await submitForConfigCard(harness, transport, inbound({ ...target, messageId: "menu-two", text: "/mode" }), target, "当前追加模式");
    const [older, newer] = configCards(transport);
    const staleNotice = waitForChatText(transport, target, "已失效");
    transport.pushBotEvent(cardActionEvent(1, select(older!, "queue")));
    await waitForBotEventAck(transport, 1);
    await staleNotice;
    expect(store.getChatMode(harness.bot.uid, target)).toBe("steer");
    expect(chatTexts(transport, target).at(-1)).toContain("已失效");
    const applied = waitForCardResult(transport, newer!);
    transport.pushBotEvent(cardActionEvent(2, select(newer!, "queue")));
    await waitForBotEventAck(transport, 2);
    await applied;
    expect(store.getChatMode(harness.bot.uid, target)).toBe("queue");
  } finally { store.close(); await closeCoreHarness(harness); }

});

const UNIT_BOT: BotIdentity = { uid: "bot-unit", name: "Unit Bot" };

/**
 * 直接驱动菜单会话的窄 harness：真实 store 与真实帧构造，transport 是替身，应用写成真正的
 * `chat_modes` 落库，用来覆盖核心用例到不了的生命周期（在途回执、TTL、上限）。
 */
async function unitDesk(options: { now?: () => number } = {}) {
  const stateDir = await mkdtemp(join(tmpdir(), "omp-channel-octo-config-"));
  const store = await openChannelStore(stateDir);
  store.setOwnerId(UNIT_BOT.uid, "owner-1");
  const transport = cardTransport();
  const texts: string[] = [];
  const operations: Promise<void>[] = [];
  const acceptedActions: Array<{
    snapshot: ConfigCardActionSnapshot;
    reservation: ConfigApplyReservation;
    checkpoint: { dispatchIntent: boolean; appliedResult: string | null };
  }> = [];
  let nextEventId = 1;
  const desk = createConfigMenuDesk({
    store,
    transport,
    botUid: () => UNIT_BOT.uid,
    stopping: () => false,
    chatAllowed: () => true,
    getAcceptedEvent: () => false,
    applyChoice: async (_kind, target, value) => {
      store.setChatMode(UNIT_BOT.uid, target, value === "queue" ? "queue" : "steer");
      return "已应用。";
    },
    acceptAction: (snapshot, _action, reservation) => {
      acceptedActions.push({
        snapshot,
        reservation,
        checkpoint: { dispatchIntent: false, appliedResult: null },
      });
      return "accepted";
    },
    enqueueText: (_target, text) => { texts.push(text); },
    ...(options.now === undefined ? {} : { now: options.now }),
  });
  return {
    desk, store, transport, texts,
    /** 按生产调用顺序打开一张卡片：先 begin 拿票据，open 之后归还。 */
    async open(target: ChatTarget, view: CommandCardView): Promise<void> {
      const ticket = desk.begin(target, view.kind);
      try {
        await desk.open(target, view, ticket);
      } finally {
        ticket.release();
      }
    },
    async handleAction(action: BotCardAction) {
      const acceptedBefore = acceptedActions.length;
      const outcome = await desk.handleAction(action, nextEventId++);
      for (const accepted of acceptedActions.slice(acceptedBefore)) {
        operations.push(desk.resumeAcceptedAction(accepted.snapshot, accepted.reservation, {
          get dispatchIntent() { return accepted.checkpoint.dispatchIntent; },
          get appliedResult() { return accepted.checkpoint.appliedResult; },
          markDispatchIntent: () => { accepted.checkpoint.dispatchIntent = true; return true; },
          markAppliedResult: (result) => { accepted.checkpoint.appliedResult = result; return true; },
          markUnknown: () => {},
        }));
      }
      await Promise.all(operations.splice(0));
      return outcome;
    },
    async close() {
      await Promise.all(operations.splice(0));
      store.close();
      await rm(stateDir, { recursive: true, force: true });
    },
  };
}

function modeView(current: string): CommandCardView {
  return {
    kind: "mode",
    current,
    choices: [{ title: "steer", value: "steer" }, { title: "queue", value: "queue" }],
    plain: `当前追加模式：${current}。`,
  };
}

test("a click that arrives while its send receipt is in flight is retried, not applied", async () => {
  const unit = await unitDesk();
  const gate = Promise.withResolvers<void>();
  try {
    const started = Promise.withResolvers<void>();
    const send = unit.transport.sendCard.bind(unit.transport);
    let held: CardFrame | undefined;
    unit.transport.sendCard = async (target, frame, clientMsgNo, signal) => {
      held = frame;
      started.resolve();
      await gate.promise;
      return await send(target, frame, clientMsgNo, signal);
    };
    const target: ChatTarget = { channelId: "dm-unit", channelType: 1 };
    const opening = unit.open(target, modeView("steer"));
    await waitForCardTest(started.promise, "card-send gate");
    expect(held).toBeDefined();
    const submit = nodes(held!.card).find(node => node.type === "Action.Submit");
    // 卡片已发布但回执未到：fake 给第一张卡的 messageId 是固定的 `card-1`。
    const action: BotCardAction = {
      channelType: 1, channelId: "dm-unit", messageId: "card-1",
      actionId: String(submit?.id ?? ""), operatorUid: "owner-1", inputs: { config_value: "queue" },
    };
    expect(await unit.handleAction(action)).toBe("retry");
    expect(unit.store.getChatMode(UNIT_BOT.uid, target)).toBe("steer");
    gate.resolve();
    await waitForCardTest(opening, "card open");
    expect(await unit.handleAction(action)).toBe("handled");
    expect(unit.store.getChatMode(UNIT_BOT.uid, target)).toBe("queue");
    expect(unit.transport.cardEdits).toHaveLength(1);
    expect(hasControls(unit.transport.cardEdits[0]!.frame)).toBe(false);
  } finally {
    gate.resolve();
    await unit.close();
  }
});

test("menus expire, keep only their owner and stay bounded", async () => {
  let clock = 1_000;
  const unit = await unitDesk({ now: () => clock });
  try {
    const target: ChatTarget = { channelId: "dm-unit-ttl", channelType: 1 };
    await unit.open(target, modeView("steer"));
    const live = select(unit.transport.sentCards.at(-1)!, "queue");
    clock += CONFIG_MENU_TTL_MS;
    // 过期（含跨进程语义）的卡片：owner 得到重开指引，状态零变化。
    expect(await unit.handleAction(live)).toBe("handled");
    expect(unit.store.getChatMode(UNIT_BOT.uid, target)).toBe("steer");
    expect(unit.texts.at(-1)).toContain("已失效");
    // 伪造的 action id 既不是本模块发放的形状，也不该得到回应。
    expect(await unit.handleAction({ ...live, actionId: "cfg:mode:not-a-session" })).toBe("ignored");
    expect(unit.texts).toHaveLength(1);

    // 未过期的卡片只归 owner：别人的点击静默且不消耗，owner 的点击照常落库。
    await unit.open(target, modeView("steer"));
    const owned = select(unit.transport.sentCards.at(-1)!, "queue");
    expect(await unit.handleAction({ ...owned, operatorUid: "member-1" })).toBe("ignored");
    expect(unit.store.getChatMode(UNIT_BOT.uid, target)).toBe("steer");
    expect(await unit.handleAction(owned)).toBe("handled");
    expect(unit.store.getChatMode(UNIT_BOT.uid, target)).toBe("queue");

    // 上限：最旧的会话先失效，最新的仍然可用。
    for (let index = 0; index <= CONFIG_MENU_MAX_SESSIONS; index += 1) {
      await unit.open({ channelId: `dm-cap-${index}`, channelType: 1 }, modeView("steer"));
    }
    const evicted = select(unit.transport.sentCards.at(-(CONFIG_MENU_MAX_SESSIONS + 1))!, "queue");
    const newest = select(unit.transport.sentCards.at(-1)!, "queue");
    expect(await unit.handleAction(evicted)).toBe("handled");
    expect(unit.store.getChatMode(UNIT_BOT.uid, evicted)).toBe("steer");
    expect(await unit.handleAction(newest)).toBe("handled");
    expect(unit.store.getChatMode(UNIT_BOT.uid, newest)).toBe("queue");
  } finally { await unit.close(); }
});

// A slow catalog response must not publish a fresh authority after a newer text switch.
test("a catalog probe superseded by a text switch cannot publish an active menu", async () => {
  const running = Promise.withResolvers<void>();
  const finish = Promise.withResolvers<void>();
  const queried = Promise.withResolvers<void>();
  const release = Promise.withResolvers<void>();
  const transport = cardTransport();
  const harness = await createCoreHarness({
    transport,
    prompt: async () => { running.resolve(); await finish.promise; },
  });
  try {
    await waitForCardTest(harness.service.submitInbound(inbound({ messageId: "probe-task", text: "running task" })), "probe task admission");
    await waitForCardTest(running.promise, "probe worker turn");
    const worker = harness.workers.workers[0]!;
    const models = worker.models.bind(worker);
    let held = false;
    worker.models = async () => {
      const catalog = await models();
      if (!held) { held = true; queried.resolve(); await release.promise; }
      return catalog;
    };
    const oldMenuMessage = inbound({ messageId: "slow-menu", text: "/switch" });
    const oldMenu = harness.service.submitInbound(oldMenuMessage);
    await waitForCardTest(queried.promise, "held model catalog query");
    const newerSelection = inbound({ messageId: "new-selection", text: "/switch other/fast" });
    const selectionTarget = { channelType: 1 as const, channelId: "dm-owner" };
    const priorReplies = new Set(transport.listRemoteText(selectionTarget).map(({ messageId }) => messageId));
    const selectionReply = waitForCardTest(transport.waitForRemoteText((entry) =>
      !priorReplies.has(entry.messageId)
      && entry.target.channelType === selectionTarget.channelType
      && entry.target.channelId === selectionTarget.channelId
      && entry.text.includes("other/fast")), "newer text switch reply");
    await waitForCardTest(harness.service.submitInbound(newerSelection), "newer text switch admission");
    release.resolve();
    await waitForCardTest(oldMenu, "superseded card request");
    await waitForIngressJob(harness, oldMenuMessage);
    await selectionReply;
    expect(configCards(transport)).toHaveLength(0);
    expect((await harness.service.status({ channelType: 1, channelId: "dm-owner" })).model).toBe("other/fast");
  } finally {
    release.resolve();
    finish.resolve();
    await closeCoreHarness(harness);
  }
});

// A provider query must not be interpreted as a native selector, even if it could resolve one.
test("an exact provider query is read-only and its card applies only on an owner click", async () => {
  const transport = cardTransport();
  const harness = await createCoreHarness({ transport });
  const target = { channelId: "dm-owner", channelType: 1 as const };
  const store = await openChannelStore(harness.stateDir);
  try {
    store.setChatModel(harness.bot.uid, target, "other/fast");
    await submitForConfigCard(harness, transport, inbound({ messageId: "provider-query", text: "/switch fixture" }), target, "provider fixture");
    const cards = configCards(transport);
    expect(cards).toHaveLength(1);
    const card = cards[0]!;
    const input = nodes(card.frame.card).find(node => node.type === "Input.ChoiceSet")!;
    const values = (input.choices as { value: string }[]).map(choice => choice.value);
    expect(values).toEqual(["default", "fixture/default", "fixture/fast"]);
    expect(cardTexts(card.frame).join("\n")).toContain("provider fixture");
    expect(cardTexts(card.frame).join("\n")).toContain("展示 2 个，共 2 个");
    expect(card.frame.plain).not.toContain("other：");
    expect(store.getChatModel(harness.bot.uid, target)).toBe("other/fast");
    expect(harness.workers.workers[0]?.switchDispatches).toEqual([]);
    expect(harness.workers.workers[0]?.stopped).toBe(true);
    expect(store.listWorkerLedger(harness.bot.uid)).toEqual([]);
    expect(harness.workers.prompts).toEqual([]);
    const action = select(card, "fixture/fast");
    transport.pushBotEvent(cardActionEvent(1, { ...action, operatorUid: "member-1" }));
    await waitForBotEventAck(transport, 1);
    expect(store.getChatModel(harness.bot.uid, target)).toBe("other/fast");
    const applied = waitForCardResult(transport, card);
    transport.pushBotEvent(cardActionEvent(2, action));
    await waitForBotEventAck(transport, 2);
    const edit = await applied;
    expect(cardTexts(edit.frame).join("\n")).toContain("fixture/fast");
    expect(store.getChatModel(harness.bot.uid, target)).toBe("fixture/fast");
    await submitForChatText(harness, transport, inbound({ messageId: "after-query-click", text: "/switch other/fast" }), target, "other/fast");
    transport.pushBotEvent(cardActionEvent(3, action));
    await waitForBotEventAck(transport, 3);
    expect(store.getChatModel(harness.bot.uid, target)).toBe("other/fast");
  } finally { store.close(); await closeCoreHarness(harness); }
});

for (const provider of [false, true]) {
  for (const count of [100, 101]) {
    test(`${provider ? "provider" : "global"} catalog shows 100 model choices and reports ${count} accurately`, async () => {
      const transport = cardTransport();
      const harness = await createCoreHarness({ transport });
      try {
        const models = Array.from({ length: count }, (_, index) => ({ provider: "fixture", id: `m${index}` }));
        harness.workers.catalog = {
          current: "other/current", level: null,
          available: provider ? [{ provider: "other", id: "outside" }, ...models] : models,
        };
        await submitForConfigCard(harness, transport, inbound({
          messageId: "bounded-menu", text: provider ? "/switch fixture" : "/switch",
        }), { channelId: "dm-owner", channelType: 1 }, provider ? "provider fixture" : "全部 provider");
        const card = configCards(transport)[0];
        expect(card).toBeDefined();
        const choices = nodes(card!.frame.card).find(node => node.type === "Input.ChoiceSet")!.choices as { value: string }[];
        expect(choices.map(choice => choice.value)).toEqual([
          "default", ...Array.from({ length: 100 }, (_, index) => `fixture/m${index}`),
        ]);
        const visible = cardTexts(card!.frame).join("\n");
        for (const text of [visible, card!.frame.plain]) {
          expect(text).toContain(provider ? "provider fixture" : "全部 provider");
          expect(text).toContain(`展示${count > 100 ? "前 100" : " 100"} 个，共 ${count} 个`);
          expect(text.includes("仅展示前 100 个")).toBe(count > 100);
          if (count > 100) {
            expect(text).toContain("/switch <provider>");
            expect(text).toContain("/switch provider/model");
          }
        }
        const applied = waitForCardResult(transport, card!);
        transport.pushBotEvent(cardActionEvent(1, select(card!, "fixture/m99")));
        await waitForBotEventAck(transport, 1);
        const edit = await applied;
        expect(cardTexts(edit.frame).join("\n")).toContain("fixture/m99");
        const store = await openChannelStore(harness.stateDir);
        try { expect(store.getChatModel(harness.bot.uid, card!.target)).toBe("fixture/m99"); }
        finally { store.close(); }
      } finally { await closeCoreHarness(harness); }
    });
  }
}

test("running provider queries use the live catalog and the newest filter invalidates older cards", async () => {
  const running = Promise.withResolvers<void>();
  const finish = Promise.withResolvers<void>();
  const queried = Promise.withResolvers<void>();
  const release = Promise.withResolvers<void>();
  const transport = cardTransport();
  const harness = await createCoreHarness({
    transport, prompt: async () => { running.resolve(); await finish.promise; },
  });
  try {
    await waitForCardTest(harness.service.submitInbound(inbound({ messageId: "filter-running", text: "hold" })), "provider task admission");
    await waitForCardTest(running.promise, "provider task start");
    const worker = harness.workers.workers[0]!;
    const before = await waitForCardTest(worker.models(), "initial provider catalog");
    await submitForConfigCard(harness, transport, inbound({ messageId: "first-filter", text: "/switch fixture" }), { channelId: "dm-owner", channelType: 1 }, "provider fixture");
    const old = configCards(transport)[0];
    const models = worker.models.bind(worker);
    let held = false;
    worker.models = async () => {
      const catalog = await models();
      if (!held) { held = true; queried.resolve(); await release.promise; }
      return catalog;
    };
    const slowMessage = inbound({ messageId: "slow-filter", text: "/switch fixture" });
    const slow = harness.service.submitInbound(slowMessage);
    await waitForCardTest(queried.promise, "held provider catalog query");
    const newFilterMessage = inbound({ messageId: "new-filter", text: "/switch other" });
    const filterTarget = { channelType: 1 as const, channelId: "dm-owner" };
    const existingCardIds = new Set(transport.sentCards.map(({ messageId }) => messageId));
    const newerCard = waitForCardTest(transport.waitForCardSend((card) =>
      !existingCardIds.has(card.messageId)
      && card.target.channelType === filterTarget.channelType
      && card.target.channelId === filterTarget.channelId
      && cardTexts(card.frame).some((text) => text.includes("provider other"))), "newer provider card");
    const newer = harness.service.submitInbound(newFilterMessage);
    release.resolve();
    await waitForCardTest(Promise.all([slow, newer]), "filtered catalog requests");
    await newerCard;
    await waitForIngressJob(harness, slowMessage);
    const cards = configCards(transport);
    expect(cards).toHaveLength(2);
    expect(cardTexts(cards[1]!.frame).join("\n")).toContain("provider other");
    transport.pushBotEvent(cardActionEvent(1, select(old!, "fixture/fast")));
    await waitForBotEventAck(transport, 1);
    expect(worker.switchDispatches).toEqual([]);
    expect(await waitForCardTest(worker.models(), "unchanged provider catalog")).toEqual(before);
    expect((await waitForCardTest(harness.service.status({ channelId: "dm-owner", channelType: 1 }), "provider chat status")).queued).toBe(0);
    expect(harness.workers.creations).toHaveLength(1);
  } finally {
    release.resolve(); finish.resolve();
    await closeCoreHarness(harness);
  }
});

test("a refused provider card falls back to the filtered counts and truncation guidance", async () => {
  const transport = cardTransport();
  transport.cardSendRefusal = () => true;
  const harness = await createCoreHarness({ transport });
  try {
    harness.workers.catalog.available = [
      { provider: "other", id: "outside" },
      ...Array.from({ length: 101 }, (_, index) => ({ provider: "fixture", id: `m${index}` })),
    ];
    await submitForChatText(harness, transport, inbound({ messageId: "filtered-fallback", text: "/switch fixture" }), { channelId: "dm-owner", channelType: 1 }, "provider fixture");
    const text = chatTexts(transport, { channelId: "dm-owner", channelType: 1 }).at(-1)!;
    expect(text).toContain("仅展示前 100 个，共 101 个");
    expect(text).not.toContain("other：");
    expect(text).toContain("m99");
    expect(text).not.toContain("m100");
    expect(text).toContain("/switch provider/model");
    expect(harness.workers.workers[0]?.switchDispatches).toEqual([]);
  } finally { await closeCoreHarness(harness); }
});
