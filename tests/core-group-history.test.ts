import { Database } from "bun:sqlite";
import { expect, test } from "bun:test";
import { join } from "node:path";
import type { ChannelCommandHandler, ChatTarget, InboundMessage } from "../src/contracts";
import { openChannelStore, type ChannelStore } from "../src/store";
import {
  closeCoreHarness,
  createCoreHarness,
  inbound,
  waitForStatus,
  type CoreHarness,
} from "./fixtures/core-fakes";

const GROUP: ChatTarget = { channelId: "group-a", channelType: 2 };
const THREAD: ChatTarget = { channelId: "thread-1", channelType: 5 };
const GROUP_IDS = ["group-a", "thread-1"];
const T0 = Date.UTC(2026, 8, 16, 5, 0);

interface LedgerRow {
  session_id: string;
  last_seq: number;
  previous_seq: number | null;
  phase: string;
}

interface ContextInjectionView {
  readonly snapshot?: { readonly content?: string };
  readonly injectionDecision?: string;
}

function stampFor(seq: number): string {
  return `${new Date(T0 + seq * 60_000).toISOString().slice(0, 16)}Z`;
}

function memberMessage(seq: number, text: string, target: ChatTarget = GROUP): InboundMessage {
  return inbound({
    ...target,
    senderId: "member-1",
    text,
    messageSeq: seq,
    messageTimestamp: T0 + seq * 60_000,
  });
}

function triggerMessage(
  harness: CoreHarness,
  seq: number,
  text: string,
  target: ChatTarget = GROUP,
): InboundMessage {
  return inbound({
    ...target,
    senderId: "owner-1",
    text,
    mentionedUserIds: [harness.bot.uid],
    messageSeq: seq,
    messageTimestamp: T0 + seq * 60_000,
  });
}

async function deliverGroupMessages(
  harness: CoreHarness,
  from: number,
  to: number,
  target: ChatTarget = GROUP,
): Promise<void> {
  for (let seq = from; seq <= to; seq += 1) {
    await harness.service.handleInbound(memberMessage(seq, `群消息 ${seq}`, target));
  }
}

async function waitForTask(harness: CoreHarness, count: number, target: ChatTarget = GROUP): Promise<void> {
  await harness.workers.waitForPromptCount(count);
  await waitForStatus(harness.service, target, (status) => status.state === "completed");
}

function historyBlockOf(prompt: string): string {
  const end = prompt.indexOf("\n\n---\n");
  return end < 0 ? "" : prompt.slice(0, end);
}

function historyHeaderOf(prompt: string): string {
  const block = historyBlockOf(prompt);
  return block.length === 0 ? "" : block.split("\n")[0] ?? "";
}

function historyLinesOf(prompt: string): string[] {
  return historyBlockOf(prompt)
    .split("\n")
    .filter((line) => line.startsWith("- "));
}

function expectedLine(seq: number, text: string, senderId = "member-1"): string {
  return `- ${stampFor(seq)} <${senderId}>: ${text}`;
}

function ledgerRows(harness: CoreHarness): LedgerRow[] {
  const database = new Database(join(harness.stateDir, "channel.sqlite"));
  try {
    return database
      .query("SELECT session_id, last_seq, previous_seq, phase FROM group_history_ledger ORDER BY updated_at, session_id")
      .all() as LedgerRow[];
  } finally {
    database.close();
  }
}

function contextInjectionOf(harness: CoreHarness, index: number): ContextInjectionView | undefined {
  return (harness.workers.prompts[index]?.promptContext as
    | { contextInjection?: ContextInjectionView }
    | undefined)?.contextInjection;
}

test("injects the newest maxMessages of the conversation on the first turn", async () => {
  const harness = await createCoreHarness({
    groupHistory: { enabled: true, maxMessages: 3 },
    allowedGroupIds: GROUP_IDS,
  });
  try {
    await deliverGroupMessages(harness, 1, 7);
    await harness.service.handleInbound(
      inbound({
        ...GROUP,
        senderId: harness.bot.uid,
        text: "机器人自己的发言",
        messageSeq: 8,
        messageTimestamp: T0 + 8 * 60_000,
      }),
    );
    await harness.service.handleInbound(
      inbound({
        ...GROUP,
        senderId: "member-2",
        text: "一张图片",
        supported: false,
        messageSeq: 9,
        messageTimestamp: T0 + 9 * 60_000,
      }),
    );

    await harness.service.handleInbound(triggerMessage(harness, 10, "@bot 请总结"));
    await waitForTask(harness, 1);

    const prompt = harness.workers.prompts[0]!.text;
    expect(historyHeaderOf(prompt)).toBe("[群聊历史，最近 3 条（另有 4 条更早的消息未注入）]");
    expect(historyLinesOf(prompt)).toEqual([
      expectedLine(5, "群消息 5"),
      expectedLine(6, "群消息 6"),
      expectedLine(7, "群消息 7"),
    ]);
    expect(prompt.endsWith("\n\n---\n@bot 请总结")).toBe(true);
    // The trigger, the bot's own message and the non-text message never render.
    expect(prompt).not.toContain("机器人自己的发言");
    expect(prompt).not.toContain("一张图片");
    // The live buffer proved the range, so no sync was needed.
    expect(harness.transport.historyCalls).toHaveLength(0);
    // History text stays out of the shared group-instruction snapshot.
    expect(contextInjectionOf(harness, 0)?.snapshot?.content ?? "").not.toContain("群消息");
    expect(ledgerRows(harness)).toEqual([
      {
        session_id: harness.workers.prompts[0]!.sessionId,
        last_seq: 10,
        previous_seq: null,
        phase: "committed",
      },
    ]);
  } finally {
    await closeCoreHarness(harness);
  }
});

test("a later turn on the same session only injects what is new", async () => {
  const harness = await createCoreHarness({
    groupHistory: { enabled: true, maxMessages: 5 },
    allowedGroupIds: GROUP_IDS,
  });
  try {
    await deliverGroupMessages(harness, 1, 3);
    await harness.service.handleInbound(triggerMessage(harness, 4, "@bot 第一轮"));
    await waitForTask(harness, 1);
    const first = harness.workers.prompts[0]!;
    expect(historyLinesOf(first.text)).toEqual([
      expectedLine(1, "群消息 1"),
      expectedLine(2, "群消息 2"),
      expectedLine(3, "群消息 3"),
    ]);

    await deliverGroupMessages(harness, 5, 6);
    await harness.service.handleInbound(triggerMessage(harness, 7, "@bot 第二轮"));
    await waitForTask(harness, 2);

    const second = harness.workers.prompts[1]!;
    expect(second.sessionId).toBe(first.sessionId);
    expect(historyHeaderOf(second.text)).toBe("[群聊历史，最近 2 条]");
    expect(historyLinesOf(second.text)).toEqual([
      expectedLine(5, "群消息 5"),
      expectedLine(6, "群消息 6"),
    ]);
    expect(historyBlockOf(second.text)).not.toContain("群消息 1");
    expect(ledgerRows(harness)).toEqual([
      { session_id: first.sessionId, last_seq: 7, previous_seq: 4, phase: "committed" },
    ]);
  } finally {
    await closeCoreHarness(harness);
  }
});

test("keeps the newest maxMessages of an oversized delta and never re-sends the dropped ones", async () => {
  const harness = await createCoreHarness({
    groupHistory: { enabled: true, maxMessages: 2 },
    allowedGroupIds: GROUP_IDS,
  });
  try {
    await deliverGroupMessages(harness, 1, 3);
    await harness.service.handleInbound(triggerMessage(harness, 4, "@bot 第一轮"));
    await waitForTask(harness, 1);

    await deliverGroupMessages(harness, 5, 8);
    await harness.service.handleInbound(triggerMessage(harness, 9, "@bot 第二轮"));
    await waitForTask(harness, 2);

    const second = harness.workers.prompts[1]!.text;
    expect(historyHeaderOf(second)).toBe("[群聊历史，最近 2 条（另有 2 条更早的消息未注入）]");
    expect(historyLinesOf(second)).toEqual([
      expectedLine(7, "群消息 7"),
      expectedLine(8, "群消息 8"),
    ]);

    // No new messages: no block at all, and the watermark still moves past this turn.
    await harness.service.handleInbound(triggerMessage(harness, 10, "@bot 无新增"));
    await waitForTask(harness, 3);
    expect(harness.workers.prompts[2]!.text).toBe("@bot 无新增");
    expect(historyLinesOf(harness.workers.prompts[2]!.text)).toEqual([]);

    await deliverGroupMessages(harness, 11, 11);
    await harness.service.handleInbound(triggerMessage(harness, 12, "@bot 第三轮"));
    await waitForTask(harness, 4);
    expect(historyLinesOf(harness.workers.prompts[3]!.text)).toEqual([
      expectedLine(11, "群消息 11"),
    ]);
    expect(harness.workers.prompts[3]!.text).not.toContain("群消息 6");
  } finally {
    await closeCoreHarness(harness);
  }
});

test("a /new session re-seeds the newest messages without rewriting the old watermark", async () => {
  const harness = await createCoreHarness({
    groupHistory: { enabled: true, maxMessages: 5 },
    allowedGroupIds: GROUP_IDS,
  });
  try {
    await deliverGroupMessages(harness, 1, 3);
    await harness.service.handleInbound(triggerMessage(harness, 4, "@bot 第一轮"));
    await waitForTask(harness, 1);
    const oldSession = harness.workers.prompts[0]!.sessionId;

    await harness.service.handleInbound(inbound({
      ...GROUP,
      senderId: "owner-1",
      text: "/new",
      mentionedUserIds: [harness.bot.uid],
      messageSeq: 5,
      messageTimestamp: T0 + 5 * 60_000,
    }));
    expect(ledgerRows(harness)).toEqual([
      { session_id: oldSession, last_seq: 4, previous_seq: null, phase: "committed" },
    ]);

    await harness.service.handleInbound(triggerMessage(harness, 6, "@bot 新会话"));
    await waitForTask(harness, 2);
    const seeded = harness.workers.prompts[1]!;
    expect(seeded.sessionId).not.toBe(oldSession);
    expect(historyHeaderOf(seeded.text)).toBe("[群聊历史，最近 5 条]");
    expect(historyLinesOf(seeded.text)).toEqual([
      expectedLine(1, "群消息 1"),
      expectedLine(2, "群消息 2"),
      expectedLine(3, "群消息 3"),
      `- ${stampFor(4)} <owner-1>: @bot 第一轮`,
      `- ${stampFor(5)} <owner-1>: /new`,
    ]);
    // The new session starts from scratch; the previous session's row is dropped as an orphan.
    expect(ledgerRows(harness)).toEqual([
      { session_id: seeded.sessionId, last_seq: 6, previous_seq: null, phase: "committed" },
    ]);
  } finally {
    await closeCoreHarness(harness);
  }
});

test("the first turn asks the server for the newest window", async () => {
  const harness = await createCoreHarness({
    groupHistory: { enabled: true, maxMessages: 4 },
    allowedGroupIds: GROUP_IDS,
  });
  try {
    harness.transport.setGroupHistory(GROUP, [
      { seq: 1, senderId: "member-1", text: "群消息 1", timestamp: T0 + 60_000 },
      { seq: 2, senderId: "member-1", text: "群消息 2", timestamp: T0 + 2 * 60_000 },
      { seq: 3, senderId: "member-1", text: "群消息 3", timestamp: T0 + 3 * 60_000 },
    ]);
    // Seq 3 never reached the socket, so only the server can prove what happened.
    await deliverGroupMessages(harness, 1, 2);
    await harness.service.handleInbound(triggerMessage(harness, 4, "@bot 请处理"));
    await waitForTask(harness, 1);

    expect(harness.transport.historyCalls).toHaveLength(1);
    expect(harness.transport.historyCalls[0]).toMatchObject({
      target: GROUP,
      sinceSeq: 0,
      untilSeq: 0,
      limit: 4,
    });
    expect(historyHeaderOf(harness.workers.prompts[0]!.text)).toBe("[群聊历史，最近 3 条]");
    expect(historyLinesOf(harness.workers.prompts[0]!.text)).toEqual([
      expectedLine(1, "群消息 1"),
      expectedLine(2, "群消息 2"),
      expectedLine(3, "群消息 3"),
    ]);
    expect(ledgerRows(harness)).toEqual([
      {
        session_id: harness.workers.prompts[0]!.sessionId,
        last_seq: 4,
        previous_seq: null,
        phase: "committed",
      },
    ]);
  } finally {
    await closeCoreHarness(harness);
  }
});

test("a trigger without a decoded seq runs without history accounting", async () => {
  const harness = await createCoreHarness({
    groupHistory: { enabled: true, maxMessages: 5 },
    allowedGroupIds: GROUP_IDS,
  });
  try {
    await deliverGroupMessages(harness, 1, 2);
    await harness.service.handleInbound(inbound({
      ...GROUP,
      senderId: "owner-1",
      text: "@bot 没有 seq",
      mentionedUserIds: [harness.bot.uid],
    }));
    await waitForTask(harness, 1);

    expect(harness.workers.prompts[0]!.text).toBe("@bot 没有 seq");
    expect(harness.transport.historyCalls).toHaveLength(0);
    expect(ledgerRows(harness)).toEqual([]);
  } finally {
    await closeCoreHarness(harness);
  }
});

test("a gap wider than one server page injects the newest segment only", async () => {
  const harness = await createCoreHarness({
    groupHistory: { enabled: true, maxMessages: 5 },
    allowedGroupIds: GROUP_IDS,
  });
  try {
    await harness.service.handleInbound(triggerMessage(harness, 1, "@bot 开场"));
    await waitForTask(harness, 1);
    // An empty candidate range needs no server page at all.
    expect(historyLinesOf(harness.workers.prompts[0]!.text)).toEqual([]);
    expect(harness.transport.historyCalls).toHaveLength(0);

    harness.transport.setGroupHistory(
      GROUP,
      Array.from({ length: 200 }, (_, index) => ({
        seq: 100 + index,
        senderId: "member-1",
        text: `历史消息 ${100 + index}`,
        timestamp: T0 + (100 + index) * 60_000,
      })),
    );
    await harness.service.handleInbound(triggerMessage(harness, 300, "@bot 总结"));
    await waitForTask(harness, 2);

    // The page is anchored on the newest end of the gap, not on the oldest one.
    expect(harness.transport.historyCalls[0]).toMatchObject({
      target: GROUP,
      sinceSeq: 100,
      untilSeq: 300,
      limit: 200,
    });
    expect(historyHeaderOf(harness.workers.prompts[1]!.text))
      .toBe("[群聊历史，最近 5 条（更早的消息未注入）]");
    expect(historyLinesOf(harness.workers.prompts[1]!.text)).toEqual([
      expectedLine(295, "历史消息 295"),
      expectedLine(296, "历史消息 296"),
      expectedLine(297, "历史消息 297"),
      expectedLine(298, "历史消息 298"),
      expectedLine(299, "历史消息 299"),
    ]);

    // Messages older than the page are accounted for and never come back.
    await harness.service.handleInbound(memberMessage(301, "群消息 301"));
    await harness.service.handleInbound(triggerMessage(harness, 302, "@bot 继续"));
    await waitForTask(harness, 3);
    expect(historyLinesOf(harness.workers.prompts[2]!.text)).toEqual([
      expectedLine(301, "群消息 301"),
    ]);
    expect(harness.transport.historyCalls).toHaveLength(1);
    expect(ledgerRows(harness)).toEqual([
      {
        session_id: harness.workers.prompts[0]!.sessionId,
        last_seq: 302,
        previous_seq: 300,
        phase: "committed",
      },
    ]);
  } finally {
    await closeCoreHarness(harness);
  }
});

test("a channel restart keeps the watermark and syncs the downtime gap", async () => {
  const groupHistory = { enabled: true, maxMessages: 5 };
  const first = await createCoreHarness({ groupHistory, allowedGroupIds: GROUP_IDS });
  const root = first.root;
  let sessionId = "";
  try {
    await deliverGroupMessages(first, 1, 4);
    first.transport.setGroupHistory(GROUP, [
      { seq: 1, senderId: "member-1", text: "群消息 1", timestamp: T0 + 60_000 },
      { seq: 2, senderId: "member-1", text: "群消息 2", timestamp: T0 + 2 * 60_000 },
      { seq: 3, senderId: "member-1", text: "群消息 3", timestamp: T0 + 3 * 60_000 },
      { seq: 4, senderId: "member-1", text: "群消息 4", timestamp: T0 + 4 * 60_000 },
    ]);
    await first.service.handleInbound(triggerMessage(first, 5, "@bot 第一轮"));
    await waitForTask(first, 1);
    sessionId = first.workers.prompts[0]!.sessionId;
    expect(first.transport.historyCalls).toHaveLength(0);
    expect(historyLinesOf(first.workers.prompts[0]!.text)).toEqual([
      expectedLine(1, "群消息 1"),
      expectedLine(2, "群消息 2"),
      expectedLine(3, "群消息 3"),
      expectedLine(4, "群消息 4"),
    ]);
  } finally {
    await closeCoreHarness(first, { remove: false });
  }

  const second = await createCoreHarness({ root, groupHistory, allowedGroupIds: GROUP_IDS });
  try {
    second.transport.setGroupHistory(GROUP, [
      { seq: 6, senderId: "member-1", text: "停机消息 6", timestamp: T0 + 6 * 60_000 },
      { seq: 7, senderId: "member-1", text: "停机消息 7", timestamp: T0 + 7 * 60_000 },
      { seq: 8, senderId: "member-1", text: "停机消息 8", timestamp: T0 + 8 * 60_000 },
      { seq: 9, senderId: "member-1", text: "停机消息 9", timestamp: T0 + 9 * 60_000 },
    ]);
    await second.service.handleInbound(triggerMessage(second, 10, "@bot 重启后"));
    await waitForTask(second, 1);

    expect(second.workers.prompts[0]!.sessionId).toBe(sessionId);
    expect(second.transport.historyCalls).toHaveLength(1);
    expect(second.transport.historyCalls[0]).toMatchObject({
      target: GROUP,
      sinceSeq: 6,
      untilSeq: 10,
      limit: 200,
    });
    const prompt = second.workers.prompts[0]!.text;
    expect(historyHeaderOf(prompt)).toBe("[群聊历史，最近 4 条]");
    expect(historyLinesOf(prompt)).toEqual([
      expectedLine(6, "停机消息 6"),
      expectedLine(7, "停机消息 7"),
      expectedLine(8, "停机消息 8"),
      expectedLine(9, "停机消息 9"),
    ]);
    expect(ledgerRows(second)).toEqual([
      { session_id: sessionId, last_seq: 10, previous_seq: 5, phase: "committed" },
    ]);
  } finally {
    await closeCoreHarness(second);
  }
});

test("a live coverage gap falls back to sync for exactly the missing range", async () => {
  const harness = await createCoreHarness({
    groupHistory: { enabled: true, maxMessages: 5 },
    allowedGroupIds: GROUP_IDS,
  });
  try {
    harness.transport.setGroupHistory(GROUP, [
      { seq: 6, senderId: "member-1", text: "缺口消息 6", timestamp: T0 + 6 * 60_000 },
    ]);
    await deliverGroupMessages(harness, 1, 4);
    await harness.transport.deliver(triggerMessage(harness, 5, "@bot 第一轮"));
    await waitForTask(harness, 1);
    expect(harness.transport.historyCalls).toHaveLength(0);

    // Seq 6 never arrived on the socket, 7..9 did.
    await deliverGroupMessages(harness, 7, 9);
    await harness.service.handleInbound(triggerMessage(harness, 10, "@bot 第二轮"));
    await waitForTask(harness, 2);

    expect(harness.transport.historyCalls).toHaveLength(1);
    expect(harness.transport.historyCalls[0]).toMatchObject({
      target: GROUP,
      sinceSeq: 6,
      untilSeq: 10,
      limit: 200,
    });
    expect(historyLinesOf(harness.workers.prompts[1]!.text)).toEqual([
      expectedLine(6, "缺口消息 6"),
      expectedLine(7, "群消息 7"),
      expectedLine(8, "群消息 8"),
      expectedLine(9, "群消息 9"),
    ]);
  } finally {
    await closeCoreHarness(harness);
  }
});

test("a failed sync keeps the prompt plain and retries the same range next turn", async () => {
  const harness = await createCoreHarness({
    groupHistory: { enabled: true, maxMessages: 5 },
    allowedGroupIds: GROUP_IDS,
  });
  try {
    await deliverGroupMessages(harness, 1, 4);
    await harness.service.handleInbound(triggerMessage(harness, 5, "@bot 第一轮"));
    await waitForTask(harness, 1);
    const sessionId = harness.workers.prompts[0]!.sessionId;
    const committed = ledgerRows(harness);

    harness.transport.setGroupHistory(
      GROUP,
      Array.from({ length: 6 }, (_, index) => ({
        seq: index + 5,
        senderId: "member-1",
        text: `重试消息 ${index + 5}`,
        timestamp: T0 + (index + 5) * 60_000,
      })),
    );
    harness.transport.failGroupHistoryWith(new Error("sync down"));
    await deliverGroupMessages(harness, 8, 9);
    await harness.service.handleInbound(triggerMessage(harness, 10, "@bot 重试"));
    await waitForTask(harness, 2);

    expect(harness.workers.prompts[1]!.text).toBe("@bot 重试");
    expect(harness.transport.historyCalls).toHaveLength(1);
    // Nothing was injected and nothing was claimed: the same range is retried.
    expect(ledgerRows(harness)).toEqual(committed);

    harness.transport.failGroupHistoryWith(undefined);
    await harness.service.handleInbound(triggerMessage(harness, 11, "@bot 补上"));
    await waitForTask(harness, 3);

    expect(harness.transport.historyCalls).toHaveLength(2);
    expect(harness.transport.historyCalls[1]).toMatchObject({
      target: GROUP,
      sinceSeq: 6,
      untilSeq: 11,
      limit: 200,
    });
    expect(historyLinesOf(harness.workers.prompts[2]!.text)).toEqual([
      expectedLine(6, "重试消息 6"),
      expectedLine(7, "重试消息 7"),
      expectedLine(8, "群消息 8"),
      expectedLine(9, "群消息 9"),
      `- ${stampFor(10)} <owner-1>: @bot 重试`,
    ]);
    expect(ledgerRows(harness)).toEqual([
      { session_id: sessionId, last_seq: 11, previous_seq: 5, phase: "committed" },
    ]);
  } finally {
    await closeCoreHarness(harness);
  }
});

test("leaves an explicitly disabled configuration exactly as before", async () => {
  const harness = await createCoreHarness({
    allowedGroupIds: GROUP_IDS,
    groupHistory: { enabled: false, maxMessages: 50 },
  });
  try {
    await deliverGroupMessages(harness, 1, 3);
    await harness.service.handleInbound(triggerMessage(harness, 4, "@bot 请处理"));
    await waitForTask(harness, 1);

    expect(harness.workers.prompts[0]!.text).toBe("@bot 请处理");
    expect(harness.transport.historyCalls).toHaveLength(0);
    expect(ledgerRows(harness)).toEqual([]);

    // Unauthorized group chatter still stops at the permission gate.
    await deliverGroupMessages(harness, 5, 6);
    expect(harness.workers.prompts).toHaveLength(1);
    expect(harness.transport.historyCalls).toHaveLength(0);
  } finally {
    await closeCoreHarness(harness);
  }
});

test("keeps private chats and threads out of each other's history", async () => {
  const harness = await createCoreHarness({
    groupHistory: { enabled: true, maxMessages: 5 },
    allowedGroupIds: GROUP_IDS,
  });
  try {
    await harness.service.handleInbound(inbound({
      channelId: "dm-owner",
      channelType: 1,
      text: "私聊任务",
      messageSeq: 50,
      messageTimestamp: T0 + 50 * 60_000,
    }));
    await waitForTask(harness, 1, { channelId: "dm-owner", channelType: 1 });
    expect(harness.workers.prompts[0]!.text).toBe("私聊任务");
    expect(ledgerRows(harness)).toEqual([]);

    // Neither a member message nor an unmentioned owner message starts a task here.
    await harness.service.handleInbound(memberMessage(1, "群消息 1"));
    await harness.service.handleInbound(inbound({
      ...GROUP,
      senderId: "owner-1",
      text: "没有 @ 的消息",
      messageSeq: 2,
      messageTimestamp: T0 + 2 * 60_000,
    }));
    expect(harness.workers.prompts).toHaveLength(1);
    expect(ledgerRows(harness)).toEqual([]);

    await harness.service.handleInbound(triggerMessage(harness, 3, "@bot 群任务"));
    await waitForTask(harness, 2);
    const groupPrompt = harness.workers.prompts[1]!.text;
    expect(historyLinesOf(groupPrompt)).toEqual([
      expectedLine(1, "群消息 1"),
      expectedLine(2, "没有 @ 的消息", "owner-1"),
    ]);
    expect(historyBlockOf(groupPrompt)).not.toContain("私聊任务");

    await harness.service.handleInbound(memberMessage(1, "线程消息 1", THREAD));
    await harness.service.handleInbound(triggerMessage(harness, 2, "@bot 线程任务", THREAD));
    await waitForTask(harness, 3, THREAD);
    const threadPrompt = harness.workers.prompts[2]!.text;
    expect(historyLinesOf(threadPrompt)).toEqual([expectedLine(1, "线程消息 1")]);
    expect(historyBlockOf(threadPrompt)).not.toContain("群消息 1");

    const rows = ledgerRows(harness);
    expect(rows).toHaveLength(2);
    expect(rows.map((row) => row.last_seq).sort()).toEqual([2, 3]);
    expect(rows.every((row) => row.phase === "committed")).toBe(true);
  } finally {
    await closeCoreHarness(harness);
  }
});

test("commits history even when the group instructions are unchanged", async () => {
  const harness = await createCoreHarness({
    groupHistory: { enabled: true, maxMessages: 5 },
    allowedGroupIds: GROUP_IDS,
  });
  try {
    harness.transport.setGroupContext(GROUP, {
      content: "群说明正文",
      version: 1,
      updatedAt: "2026-09-16T00:00:00.000Z",
      updatedBy: "admin",
    });
    await deliverGroupMessages(harness, 1, 4);
    await harness.service.handleInbound(triggerMessage(harness, 5, "@bot 第一轮"));
    await waitForTask(harness, 1);
    const sessionId = harness.workers.prompts[0]!.sessionId;
    expect(contextInjectionOf(harness, 0)?.injectionDecision).toBe("initial");
    expect(historyLinesOf(harness.workers.prompts[0]!.text)).toHaveLength(4);

    await harness.service.handleInbound(triggerMessage(harness, 6, "@bot 第二轮"));
    await waitForTask(harness, 2);
    // The unchanged-version turn still commits the history watermark.
    expect(contextInjectionOf(harness, 1)?.injectionDecision).toBe("unchanged");
    expect(harness.workers.prompts[1]!.text).toBe("@bot 第二轮");
    expect(ledgerRows(harness)).toEqual([
      { session_id: sessionId, last_seq: 6, previous_seq: 5, phase: "committed" },
    ]);

    await deliverGroupMessages(harness, 7, 8);
    await harness.service.handleInbound(triggerMessage(harness, 9, "@bot 第三轮"));
    await waitForTask(harness, 3);
    const third = harness.workers.prompts[2]!.text;
    expect(contextInjectionOf(harness, 2)?.injectionDecision).toBe("unchanged");
    expect(historyLinesOf(third)).toEqual([
      expectedLine(7, "群消息 7"),
      expectedLine(8, "群消息 8"),
    ]);
    expect(historyBlockOf(third)).not.toContain("群消息 1");
    expect(ledgerRows(harness)).toEqual([
      { session_id: sessionId, last_seq: 9, previous_seq: 6, phase: "committed" },
    ]);
  } finally {
    await closeCoreHarness(harness);
  }
});

test("a history commit failure is logged but never fails the task", async () => {
  let promptCount = 0;
  let dropLedger: (() => void) | undefined;
  const harness = await createCoreHarness({
    groupHistory: { enabled: true, maxMessages: 5 },
    allowedGroupIds: GROUP_IDS,
    prompt: async ({ text, append }) => {
      promptCount += 1;
      if (promptCount > 1) dropLedger?.();
      await append("effects.log", `${text}\n`);
    },
  });
  let probe: ChannelStore | undefined;
  try {
    await deliverGroupMessages(harness, 1, 2);
    await harness.service.handleInbound(triggerMessage(harness, 3, "@bot 请处理"));
    await harness.workers.waitForPromptCount(1);
    await waitForStatus(harness.service, GROUP, (status) => status.state === "completed");

    // The claimed row disappears between `prepare` and the post-prompt CAS.
    probe = await openChannelStore(harness.stateDir);
    dropLedger = () => {
      probe?.pruneHistoryLedger(harness.bot.uid, GROUP, "no-such-session");
    };
    await deliverGroupMessages(harness, 4, 5);
    await harness.service.handleInbound(triggerMessage(harness, 6, "@bot 第二轮"));
    await waitForTask(harness, 2);

    expect(historyLinesOf(harness.workers.prompts[1]!.text)).toEqual([
      expectedLine(4, "群消息 4"),
      expectedLine(5, "群消息 5"),
    ]);
    expect(ledgerRows(harness)).toEqual([]);
  } finally {
    probe?.close();
    await closeCoreHarness(harness);
  }
});

test("commands and observing handlers never inject history", async () => {
  const handlers: ChannelCommandHandler[] = [
    { name: "note", intercept: false, handle: () => {} },
  ];
  const harness = await createCoreHarness({
    groupHistory: { enabled: true, maxMessages: 10 },
    allowedGroupIds: GROUP_IDS,
    commandHandlers: handlers,
  });
  try {
    await deliverGroupMessages(harness, 1, 3);
    for (const [seq, text] of [[4, "/status"], [5, "/help"], [6, "/note"]] as const) {
      await harness.service.handleInbound(triggerMessage(harness, seq, text));
    }
    await harness.workers.waitForPromptCount(1);
    await waitForStatus(harness.service, GROUP, (status) => status.state === "completed");

    expect(harness.workers.prompts[0]!.text).toBe("/note");
    expect(historyBlockOf(harness.workers.prompts[0]!.text)).toBe("");
    expect(harness.transport.historyCalls).toHaveLength(0);
    expect(ledgerRows(harness)).toEqual([]);

    await harness.service.handleInbound(triggerMessage(harness, 7, "@bot 普通任务"));
    await waitForTask(harness, 2);
    // Commands did not advance anything: the earlier group text is still history.
    expect(historyLinesOf(harness.workers.prompts[1]!.text)).toEqual([
      expectedLine(1, "群消息 1"),
      expectedLine(2, "群消息 2"),
      expectedLine(3, "群消息 3"),
      `- ${stampFor(4)} <owner-1>: /status`,
      `- ${stampFor(5)} <owner-1>: /help`,
      `- ${stampFor(6)} <owner-1>: /note`,
    ]);
  } finally {
    await closeCoreHarness(harness);
  }
});

test("truncates an oversized history body inside the block budget", async () => {
  const harness = await createCoreHarness({
    groupHistory: { enabled: true, maxMessages: 5 },
    allowedGroupIds: GROUP_IDS,
  });
  try {
    await harness.service.handleInbound(memberMessage(1, "😀".repeat(1500)));
    await harness.service.handleInbound(triggerMessage(harness, 2, "@bot 请处理"));
    await waitForTask(harness, 1);

    const prompt = harness.workers.prompts[0]!.text;
    const line = historyLinesOf(prompt)[0] ?? "";
    const body = line.slice(line.indexOf(": ") + 2);
    expect(body).toBe("😀".repeat(1000));
    expect(Array.from(body)).toHaveLength(1000);
    expect(Buffer.byteLength(historyBlockOf(prompt), "utf8")).toBeLessThanOrEqual(32 * 1024);
  } finally {
    await closeCoreHarness(harness);
  }
});

test("accounts a steered message so the next turn never sends it back", async () => {
  const harness = await createCoreHarness({
    groupHistory: { enabled: true, maxMessages: 5 },
    allowedGroupIds: GROUP_IDS,
    prompt: async ({ text, append, waitForAppend }) => {
      if (text.includes("长任务")) await waitForAppend();
      await append("effects.log", `${text}\n`);
    },
  });
  try {
    await deliverGroupMessages(harness, 1, 3);
    await harness.service.handleInbound(triggerMessage(harness, 4, "@bot 长任务"));
    await harness.workers.waitForPromptCount(1);
    const sessionId = harness.workers.prompts[0]!.sessionId;

    // The chat is in `steer` mode: the message is injected into the running turn.
    await harness.service.handleInbound(inbound({
      ...GROUP,
      senderId: "owner-1",
      text: "补充要求",
      mentionedUserIds: [harness.bot.uid],
      messageSeq: 6,
      messageTimestamp: T0 + 6 * 60_000,
    }));
    await waitForStatus(harness.service, GROUP, (status) => status.state === "completed");

    // The steered seq is accounted, not merely injected.
    expect(ledgerRows(harness)).toEqual([
      { session_id: sessionId, last_seq: 6, previous_seq: null, phase: "committed" },
    ]);

    await harness.service.handleInbound(triggerMessage(harness, 7, "@bot 第二轮"));
    await waitForTask(harness, 2);
    const second = harness.workers.prompts[1]!.text;
    expect(second).toBe("@bot 第二轮");
    expect(second).not.toContain("补充要求");
    expect(ledgerRows(harness)).toEqual([
      { session_id: sessionId, last_seq: 7, previous_seq: 6, phase: "committed" },
    ]);
  } finally {
    await closeCoreHarness(harness);
  }
});

test("a live image message contributes only its caption to history and downloads nothing", async () => {
  const harness = await createCoreHarness({
    groupHistory: { enabled: true, maxMessages: 5 },
    allowedGroupIds: GROUP_IDS,
  });
  const image = (url: string, name: string) => ({
    url,
    kind: "image" as const,
    name,
    size: 12,
    durationSeconds: 0,
  });
  try {
    await deliverGroupMessages(harness, 1, 2);
    // 实时缓冲走同一张类型表：带 caption 的图片按 caption 渲染。
    await harness.service.handleInbound(inbound({
      ...GROUP,
      senderId: "member-2",
      text: "这是设计稿",
      attachments: [image("https://cdn.example.test/design.png", "design.png")],
      messageSeq: 3,
      messageTimestamp: T0 + 3 * 60_000,
    }));
    // 无 caption 的图片没有可渲染文本，只贡献覆盖。
    await harness.service.handleInbound(inbound({
      ...GROUP,
      senderId: "member-2",
      text: "",
      attachments: [image("https://cdn.example.test/bare.png", "bare.png")],
      messageSeq: 4,
      messageTimestamp: T0 + 4 * 60_000,
    }));

    await harness.service.handleInbound(triggerMessage(harness, 5, "@bot 看下这张图"));
    await waitForTask(harness, 1);

    const prompt = harness.workers.prompts[0]!.text;
    expect(historyLinesOf(prompt)).toEqual([
      expectedLine(1, "群消息 1"),
      expectedLine(2, "群消息 2"),
      expectedLine(3, "这是设计稿", "member-2"),
    ]);
    expect(prompt).not.toContain("bare.png");
    // 非 owner 的历史消息永不触发下载。
    expect(harness.transport.downloadRequests).toHaveLength(0);
  } finally {
    await closeCoreHarness(harness);
  }
});
