import { expect, test } from "bun:test";
import { randomUUID } from "node:crypto";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { createConfigMenuDesk, CONFIG_MENU_MAX_SESSIONS, CONFIG_MENU_TTL_MS } from "../src/command-cards.ts";
import type { CommandCardView } from "../src/command-card.ts";
import type { BotCardAction, BotIdentity, CardFrame, ChatTarget, SendReceipt } from "../src/contracts.ts";
import { openChannelStore } from "../src/store.ts";
import { cardActionEvent, closeCoreHarness, createCoreHarness, FakeTransport, inbound, waitForStatus } from "./fixtures/core-fakes.ts";
import type { SentCard } from "./fixtures/core-fakes.ts";

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

/** Text delivered to one chat, in delivery order. */
function chatTexts(transport: FakeTransport, target: ChatTarget): string[] {
  return transport.sent
    .filter(message =>
      message.target.channelType === target.channelType && message.target.channelId === target.channelId)
    .map(message => message.text);
}

function cardTransport(): FakeTransport {
  const transport = new FakeTransport();
  transport.cardSupported = true;
  return transport;
}

/** The catalog listing the deployment renders when it cannot carry the card. */
const SWITCH_LISTING = [
  "当前模型：fixture/default（OMP 默认）",
  "可用模型（来自 OMP 已配置 provider，共 3 个）：",
  "fixture：default、fast",
  "other：fast",
].join("\n");

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
    await harness.service.handleInbound(inbound({ messageId: "model-menu", text: "/switch" }));
    expect(transport.sentCards).toHaveLength(1);
    const card = transport.sentCards[0]!;
    transport.pushBotEvent(cardActionEvent(1, select(card, "fixture/fast")));
    await transport.waitForAck(1);
    const store = await openChannelStore(harness.stateDir);
    try {
      expect(store.getChatModel(harness.bot.uid, card.target)).toBe("fixture/fast");
    } finally { store.close(); }
    expect(harness.workers.prompts).toHaveLength(0);
    await harness.service.handleInbound(inbound({ messageId: "next-turn", text: "use selected model" }));
    await harness.workers.waitForPromptCount(1);
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
    await harness.service.handleInbound(inbound({ ...target, messageId: "mode-menu", text: "/mode", mentionedUserIds: [harness.bot.uid] }));
    expect(transport.sentCards).toHaveLength(1);
    const action = select(transport.sentCards[0]!, "queue");
    transport.pushBotEvent(cardActionEvent(1, { ...action, operatorUid: "member-1" }));
    await transport.waitForAck(1);
    expect(store.getChatMode(harness.bot.uid, target)).toBe("steer");
    transport.pushBotEvent(cardActionEvent(2, action));
    await transport.waitForAck(2);
    expect(store.getChatMode(harness.bot.uid, target)).toBe("queue");
    await harness.service.handleInbound(inbound({ ...target, messageId: "mode-text", text: "/mode steer", mentionedUserIds: [harness.bot.uid] }));
    transport.pushBotEvent(cardActionEvent(3, action));
    await transport.waitForAck(3);
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
    await harness.service.handleInbound(inbound({ ...target, messageId: "default-set", text: "/switch fixture/fast" }));
    await harness.service.handleInbound(inbound({ ...target, messageId: "default-menu", text: "/switch" }));
    const card = configCards(transport)[0]!;
    transport.pushBotEvent(cardActionEvent(1, select(card, "default")));
    await transport.waitForAck(1);
    const store = await openChannelStore(harness.stateDir);
    try {
      expect(store.getChatModel(harness.bot.uid, target)).toBeNull();
    } finally { store.close(); }
    const edit = await transport.waitForCardEdit(entry => entry.messageId === card.messageId);
    expect(cardTexts(edit.frame)).toContain("已恢复跟随 OMP 默认模型（当前为 fixture/default）。");
    expect(hasControls(edit.frame)).toBe(false);
    await harness.service.handleInbound(inbound({ ...target, messageId: "default-task", text: "run the fixture" }));
    await harness.workers.waitForPromptCount(1);
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
    await harness.service.handleInbound(inbound({ ...target, messageId: "live-start", text: "long-running" }));
    await running.promise;
    await harness.service.handleInbound(inbound({ ...target, messageId: "live-menu", text: "/switch" }));
    const card = configCards(transport)[0]!;
    transport.pushBotEvent(cardActionEvent(1, select(card, "other/fast")));
    await transport.waitForAck(1);
    expect(harness.workers.workers[0]?.switchDispatches).toEqual(["other/fast"]);
    const store = await openChannelStore(harness.stateDir);
    try {
      expect(store.getChatModel(harness.bot.uid, target)).toBe("other/fast");
    } finally { store.close(); }
    const edit = await transport.waitForCardEdit(entry => entry.messageId === card.messageId);
    expect(cardTexts(edit.frame)).toContain("已切换本聊天模型为 other/fast（运行中会话已同步切换）。");
    // 菜单由活会话自己回答，点击也走同一个会话：全程没有第二个 worker。
    expect(harness.workers.creations).toHaveLength(1);
    finish.resolve();
    await waitForStatus(harness.service, target, status => status.state === "completed");
  } finally { await closeCoreHarness(harness); }
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
    await harness.service.handleInbound(inbound({ ...target, messageId: "ask-start", text: "needs an answer" }));
    await harness.workers.waitForPromptCount(1);
    const pending = await waitForStatus(harness.service, target, status => status.pendingQuestionIds.length > 0);
    await harness.service.handleInbound(inbound({ ...target, messageId: "ask-menu", text: "/mode" }));
    const card = configCards(transport)[0]!;
    transport.pushBotEvent(cardActionEvent(1, select(card, "queue")));
    await transport.waitForAck(1);
    const store = await openChannelStore(harness.stateDir);
    try {
      expect(store.getChatMode(harness.bot.uid, target)).toBe("queue");
      expect(store.countQueuedTasks(harness.bot.uid, target)).toBe(0);
      expect(store.pendingQuestionIds(harness.bot.uid, target)).toEqual(pending.pendingQuestionIds);
    } finally { store.close(); }
    expect(harness.workers.prompts).toHaveLength(1);
    // 问答路径本身不受影响：同聊天的普通回复仍然作为答案，问题正常结束。
    await harness.service.handleInbound(inbound({ ...target, messageId: "ask-answer", text: "b" }));
    await waitForStatus(harness.service, target, status => status.pendingQuestionIds.length === 0);
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
    await harness.service.handleInbound(inbound({
      ...target, messageId: "binding-menu", text: "/mode", mentionedUserIds: [harness.bot.uid],
    }));
    const action = select(configCards(transport)[0]!, "queue");
    const forged = [
      withOverrides(action, { operatorUid: "member-9" }),
      withOverrides(action, { messageId: "another-message" }),
      withOverrides(action, { channelId: "group-a" }),
      withOverrides(action, { actionId: `cfg:mode:${randomUUID()}` }),
    ];
    for (const [index, click] of forged.entries()) {
      transport.pushBotEvent(cardActionEvent(index + 1, click));
      await transport.waitForAck(index + 1);
      expect(store.getChatMode(harness.bot.uid, target)).toBe("steer");
      expect(store.getChatMode(harness.bot.uid, other)).toBe("steer");
    }
    // 只有从未发放的那一次给出指引；其余伪造点击静默，且都没有消耗 owner 的卡片。
    expect(chatTexts(transport, target)).toHaveLength(1);
    expect(chatTexts(transport, target)[0]).toContain("已失效");
    expect(chatTexts(transport, other)).toEqual([]);
    transport.pushBotEvent(cardActionEvent(9, action));
    await transport.waitForAck(9);
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
    await harness.service.handleInbound(inbound({ ...target, messageId: "issued-menu", text: "/mode" }));
    const action = select(configCards(transport)[0]!, "steer");
    // "default" 是文本命令的同义写法，但不是这张卡发放过的取值。
    transport.pushBotEvent(cardActionEvent(1, withOverrides(action, { inputs: { config_value: "default" } })));
    await transport.waitForAck(1);
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
    await plain.service.handleInbound(inbound({ messageId: "plain-switch", text: "/switch" }));
    expect(chatTexts(unsupported, { channelId: "dm-owner", channelType: 1 })).toEqual([SWITCH_LISTING]);
    expect(unsupported.sentCards).toHaveLength(0);
  } finally { await closeCoreHarness(plain); }

  const transport = cardTransport();
  transport.cardSendRefusal = () => true;
  const refused = await createCoreHarness({ transport });
  try {
    await refused.service.handleInbound(inbound({ messageId: "refused-switch", text: "/switch" }));
    expect(chatTexts(transport, { channelId: "dm-owner", channelType: 1 })).toEqual([SWITCH_LISTING]);
    await refused.service.handleInbound(inbound({ messageId: "refused-mode", text: "/mode" }));
    expect(chatTexts(transport, { channelId: "dm-owner", channelType: 1 }).at(-1)).toContain("当前追加模式：steer");
    // 抛错的发送同样回退，并且不留下任何可点击的会话。
    transport.cardSendRefusal = undefined;
    transport.cardSendFailure = new Error("card transport down");
    await refused.service.handleInbound(inbound({ messageId: "failed-switch", text: "/switch" }));
    expect(chatTexts(transport, { channelId: "dm-owner", channelType: 1 }).at(-1)).toBe(SWITCH_LISTING);
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
    const first = harness.service.handleInbound(inbound({
      ...target, messageId: "stale-menu", text: "/mode", mentionedUserIds: [harness.bot.uid],
    }));
    await transport.held;
    await harness.service.handleInbound(inbound({
      ...target, messageId: "fresh-mode", text: "/mode queue", mentionedUserIds: [harness.bot.uid],
    }));
    transport.releaseSend();
    await first;
    // 文本命令是权威：卡片确实发了出去（发送在途），但它属于被取代的会话。
    expect(transport.sentCards).toHaveLength(1);
    expect(chatTexts(transport, target)).toEqual(["追加模式已设为 queue：运行中的新消息会排到当前回合之后。"]);
    transport.pushBotEvent(cardActionEvent(1, select(transport.sentCards[0]!, "steer")));
    await transport.waitForAck(1);
    expect(store.getChatMode(harness.bot.uid, target)).toBe("queue");
    expect(chatTexts(transport, target).at(-1)).toContain("已失效");
  } finally { store.close(); await closeCoreHarness(harness); }
});

// Catches cross-process residue: a card from a previous service process must be inert.
test("a card from a previous process is inert after restart", async () => {
  const first = await createCoreHarness({ transport: cardTransport() });
  const root = first.root;
  const target = { channelId: "dm-restart-card", channelType: 1 as const };
  const stale: { action: BotCardAction | null } = { action: null };
  try {
    await first.service.handleInbound(inbound({ ...target, messageId: "pre-restart-menu", text: "/mode" }));
    stale.action = select(configCards(first.transport)[0]!, "queue");
  } finally { await closeCoreHarness(first, { remove: false }); }
  if (stale.action === null) throw new Error("fixture card missing");

  const restarted = await createCoreHarness({ root, transport: cardTransport() });
  try {
    restarted.transport.pushBotEvent(cardActionEvent(1, stale.action));
    await restarted.transport.waitForAck(1);
    const store = await openChannelStore(restarted.stateDir);
    try {
      expect(store.getChatMode(restarted.bot.uid, target)).toBe("steer");
    } finally { store.close(); }
    expect(chatTexts(restarted.transport, target).at(-1)).toContain("已失效");
    // 新进程的新卡片照常工作：跨进程残留只是被拒绝，不是把功能一起关掉。
    await restarted.service.handleInbound(inbound({ ...target, messageId: "post-restart-menu", text: "/mode" }));
    restarted.transport.pushBotEvent(cardActionEvent(2, select(configCards(restarted.transport)[0]!, "queue")));
    await restarted.transport.waitForAck(2);
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
    await harness.service.handleInbound(inbound({ ...target, messageId: "menu-one", text: "/mode" }));
    await harness.service.handleInbound(inbound({ ...target, messageId: "menu-two", text: "/mode" }));
    const [older, newer] = configCards(transport);
    transport.pushBotEvent(cardActionEvent(1, select(older!, "queue")));
    await transport.waitForAck(1);
    expect(store.getChatMode(harness.bot.uid, target)).toBe("steer");
    expect(chatTexts(transport, target).at(-1)).toContain("已失效");
    transport.pushBotEvent(cardActionEvent(2, select(newer!, "queue")));
    await transport.waitForAck(2);
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
  const desk = createConfigMenuDesk({
    store,
    transport,
    botUid: () => UNIT_BOT.uid,
    stopping: () => false,
    chatAllowed: () => true,
    applyChoice: async (_kind, target, value) => {
      store.setChatMode(UNIT_BOT.uid, target, value === "queue" ? "queue" : "steer");
      return "已应用。";
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
    async close() { store.close(); await rm(stateDir, { recursive: true, force: true }); },
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
  try {
    const started = Promise.withResolvers<void>();
    const gate = Promise.withResolvers<void>();
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
    await started.promise;
    expect(held).toBeDefined();
    const submit = nodes(held!.card).find(node => node.type === "Action.Submit");
    // 卡片已发布但回执未到：fake 给第一张卡的 messageId 是固定的 `card-1`。
    const action: BotCardAction = {
      channelType: 1, channelId: "dm-unit", messageId: "card-1",
      actionId: String(submit?.id ?? ""), operatorUid: "owner-1", inputs: { config_value: "queue" },
    };
    expect(await unit.desk.handleAction(action)).toBe("retry");
    expect(unit.store.getChatMode(UNIT_BOT.uid, target)).toBe("steer");
    gate.resolve();
    await opening;
    expect(await unit.desk.handleAction(action)).toBe("handled");
    expect(unit.store.getChatMode(UNIT_BOT.uid, target)).toBe("queue");
    expect(unit.transport.cardEdits).toHaveLength(1);
    expect(hasControls(unit.transport.cardEdits[0]!.frame)).toBe(false);
  } finally { await unit.close(); }
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
    expect(await unit.desk.handleAction(live)).toBe("handled");
    expect(unit.store.getChatMode(UNIT_BOT.uid, target)).toBe("steer");
    expect(unit.texts.at(-1)).toContain("已失效");
    // 伪造的 action id 既不是本模块发放的形状，也不该得到回应。
    expect(await unit.desk.handleAction({ ...live, actionId: "cfg:mode:not-a-session" })).toBe("ignored");
    expect(unit.texts).toHaveLength(1);

    // 未过期的卡片只归 owner：别人的点击静默且不消耗，owner 的点击照常落库。
    await unit.open(target, modeView("steer"));
    const owned = select(unit.transport.sentCards.at(-1)!, "queue");
    expect(await unit.desk.handleAction({ ...owned, operatorUid: "member-1" })).toBe("ignored");
    expect(unit.store.getChatMode(UNIT_BOT.uid, target)).toBe("steer");
    expect(await unit.desk.handleAction(owned)).toBe("handled");
    expect(unit.store.getChatMode(UNIT_BOT.uid, target)).toBe("queue");

    // 上限：最旧的会话先失效，最新的仍然可用。
    for (let index = 0; index <= CONFIG_MENU_MAX_SESSIONS; index += 1) {
      await unit.open({ channelId: `dm-cap-${index}`, channelType: 1 }, modeView("steer"));
    }
    const evicted = select(unit.transport.sentCards.at(-(CONFIG_MENU_MAX_SESSIONS + 1))!, "queue");
    const newest = select(unit.transport.sentCards.at(-1)!, "queue");
    expect(await unit.desk.handleAction(evicted)).toBe("handled");
    expect(unit.store.getChatMode(UNIT_BOT.uid, evicted)).toBe("steer");
    expect(await unit.desk.handleAction(newest)).toBe("handled");
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
    await harness.service.handleInbound(inbound({ messageId: "probe-task", text: "running task" }));
    await running.promise;
    const worker = harness.workers.workers[0]!;
    const models = worker.models.bind(worker);
    worker.models = async () => {
      const catalog = await models();
      queried.resolve();
      await release.promise;
      return catalog;
    };
    const oldMenu = harness.service.handleInbound(inbound({ messageId: "slow-menu", text: "/switch" }));
    await queried.promise;
    await harness.service.handleInbound(inbound({ messageId: "new-selection", text: "/switch other/fast" }));
    release.resolve();
    await oldMenu;
    expect(configCards(transport)).toHaveLength(0);
    expect((await harness.service.status({ channelType: 1, channelId: "dm-owner" })).model).toBe("other/fast");
  } finally {
    release.resolve();
    finish.resolve();
    await closeCoreHarness(harness);
  }
});
