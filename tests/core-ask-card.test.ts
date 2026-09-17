import { expect, test } from "bun:test";
import { join } from "node:path";
import type { ChannelStatus, ChatTarget } from "../src/contracts";
import { cardStats } from "../src/octo/cards";
import {
  FakeTransport,
  cardActionEvent,
  closeCoreHarness,
  createCoreHarness,
  inbound,
  readText,
  waitForStatus,
} from "./fixtures/core-fakes";

function onlyPendingQuestionId(status: ChannelStatus): string {
  const questionId = status.pendingQuestionIds[0];
  if (questionId === undefined) {
    throw new Error("pending question ID missing");
  }
  return questionId;
}

async function readAnswers(harness: { root: string }): Promise<unknown> {
  return JSON.parse(await readText(join(harness.root, "tmp", "answer.log")));
}

function cardActions(edit: { frame: { card: Record<string, unknown> } }): string[] {
  const actions = edit.frame.card.actions;
  if (!Array.isArray(actions)) return [];
  return actions.map((action) => String((action as Record<string, unknown>).id));
}

function cardBody(card: Record<string, unknown>): Record<string, unknown>[] {
  const body = card.body;
  return Array.isArray(body) ? (body as Record<string, unknown>[]) : [];
}

/** Action ids of the card's body `ActionSet` rows in body order; one entry per row. */
function bodyActionRows(card: { frame: { card: Record<string, unknown> } }): string[][] {
  return cardBody(card.frame.card)
    .filter((element) => element.type === "ActionSet")
    .map((element) => (Array.isArray(element.actions) ? element.actions : [])
      .map((item) => String((item as Record<string, unknown>).id)));
}

/** Ids of the inputs one frame renders, in body order. */
function inputIds(card: { frame: { card: Record<string, unknown> } }): string[] {
  return cardBody(card.frame.card)
    .map((element) => element.id)
    .filter((id): id is string => typeof id === "string");
}

function startAskHarness(
  question: Record<string, unknown> | Array<Record<string, unknown>>,
  options: { cards?: boolean; setup?: (transport: FakeTransport) => void } = {},
) {
  const transport = new FakeTransport();
  transport.cardSupported = options.cards ?? true;
  options.setup?.(transport);
  const questions = Array.isArray(question) ? question : [question];
  return createCoreHarness({
    transport,
    prompt: async ({ append, promptContext }) => {
      const ask = promptContext?.tools?.find(({ name }) => name === "ask");
      if (!ask) {
        throw new Error("ask host tool missing");
      }
      const result = await ask.execute({ questions }, new AbortController().signal);
      await append("answer.log", result);
    },
  });
}

/**
 * One `ask` whose wait times out: the deployment timeout is 40 ms, the first prompt runs the
 * tool exactly like OMP does and ends when the stop aborts the run, and any later prompt is a
 * woke turn whose text is recorded.
 */
function startAskTimeoutHarness(question: Record<string, unknown> | Array<Record<string, unknown>>) {
  const transport = new FakeTransport();
  transport.cardSupported = true;
  const questions = Array.isArray(question) ? question : [question];
  return createCoreHarness({
    transport,
    ask: { timeoutMs: 40 },
    prompt: async ({ append, promptContext, text, waitForAbort }) => {
      if (text.startsWith("[ask 答复]")) {
        await append("wake.log", text);
        return;
      }
      const ask = promptContext?.tools?.find(({ name }) => name === "ask");
      if (!ask) {
        throw new Error("ask host tool missing");
      }
      try {
        await append("answer.log", await ask.execute({ questions }, new AbortController().signal));
      } catch {
        // The stop cancels the tool call; OMP ends the run without delivering it an answer.
        await waitForAbort();
      }
    },
  });
}

test("a card click answers the pending question and rewrites the card", async () => {
  const harness = await startAskHarness({
    id: "deployment-target",
    question: "Which deployment target?",
    options: [{ label: "staging", description: "shared" }, { label: "production" }],
  });
  try {
    const target: ChatTarget = { channelId: "dm-card-click", channelType: 1 };
    await harness.service.handleInbound(
      inbound({ messageId: "card-click-start", ...target, text: "needs an answer" }),
    );
    await harness.workers.waitForPromptCount(1);
    const pending = await waitForStatus(
      harness.service,
      target,
      (status) => status.pendingQuestionIds.length === 1,
    );
    const questionId = onlyPendingQuestionId(pending);

    const card = harness.transport.sentCards[0];
    if (!card) {
      throw new Error("ask card was not sent");
    }
    // The card carries the question; the text path stays silent unless the card cannot be sent.
    expect(harness.transport.sent).toHaveLength(0);
    expect(card.frame.profile).toBe("octo/v2");
    expect(JSON.stringify(card.frame.card)).toContain("2. production");
    expect(card.frame.plain).toContain("直接回复选项编号作为答案");

    harness.transport.pushBotEvent(cardActionEvent(7, {
      messageId: card.messageId,
      channelType: 1,
      channelId: target.channelId,
      actionId: "ask_opt_2",
      operatorUid: "owner-1",
      inputs: {},
    }));

    const completed = await waitForStatus(
      harness.service,
      target,
      (status) => status.state === "completed" && status.pendingQuestionIds.length === 0,
    );
    expect(completed.pendingQuestionIds).toHaveLength(0);
    expect(await readAnswers(harness)).toEqual({
      answers: [{ id: "deployment-target", questionId, answeredBy: "owner-1", answer: "2" }],
    });
    await harness.transport.waitForAck(7);

    const edit = await harness.transport.waitForCardEdit(({ messageId }) => messageId === card.messageId);
    expect(edit.cardSeq).toBe(2);
    expect(cardActions(edit)).toHaveLength(0);
    expect(JSON.stringify(edit.frame.card)).toContain("已由 owner-1 回复：2. production");
  } finally {
    await closeCoreHarness(harness);
  }
});

test("a single-choice card stacks every option as its own body row", async () => {
  const longLabel =
    "确认，直接按现有设计调整：先测量现有字号与布局，提出统一调整方案，再由赵奕在桌面端与窄屏各确认一次视觉方案后实施，实施前同步更新文档。";
  const longAscii = "deploy to the staging cluster with the canary flag and wait for owner approval";
  const harness = await startAskHarness({
    id: "deployment-target",
    question: "Which deployment target?",
    options: [
      { label: "staging", description: "shared" },
      { label: longLabel },
      { label: longAscii },
      { label: "production" },
    ],
  });
  try {
    const target: ChatTarget = { channelId: "dm-card-stack", channelType: 1 };
    await harness.service.handleInbound(
      inbound({ messageId: "card-stack-start", ...target, text: "needs an answer" }),
    );
    await harness.workers.waitForPromptCount(1);
    await waitForStatus(harness.service, target, (status) => status.pendingQuestionIds.length === 1);
    const card = harness.transport.sentCards[0];
    if (!card) {
      throw new Error("ask card was not sent");
    }

    // 每个选项各占一个 ActionSet 行：客户端把 body 的 actionset 纵向堆叠，没有按钮与别的
    // 按钮共用一行，因此不会被卡片边缘裁掉；action id 与选项顺序不变。
    expect(bodyActionRows(card)).toEqual([
      ["ask_opt_1"],
      ["ask_opt_2"],
      ["ask_opt_3"],
      ["ask_opt_4"],
    ]);
    // 动作区只剩自由输入的提交按钮，不再承载选项。
    expect(cardActions(card)).toEqual(["ask_submit"]);
    // 完整选项文案留在编号行里；按钮标题按显示宽度有界（40 列，即约 20 个全角字形），
    // 因为客户端按一行固有宽度摆放按钮，超宽只会溢出卡片边缘而不会换行。
    expect(longLabel.length).toBeGreaterThan(60);
    expect(JSON.stringify(card.frame.card)).toContain(longLabel);
    const rows = cardBody(card.frame.card).filter((element) => element.type === "ActionSet");
    const titles = rows.map((row) =>
      String((row.actions as Record<string, unknown>[])[0]?.title ?? ""));
    expect(titles[0]).toBe("1. staging");
    expect(titles[3]).toBe("4. production");
    // 全角：3（前缀）+18（正文）+1（省略号）= 22 个码点；半角文案在 40 列内。
    expect(titles[1]?.endsWith("…")).toBe(true);
    expect([...(titles[1] ?? "")].length).toBeLessThanOrEqual(22);
    expect(titles[2]?.endsWith("…")).toBe(true);
    expect([...(titles[2] ?? "")].length).toBeLessThanOrEqual(40);
    // 新增的每行一个 ActionSet 仍远在部署上限（max_nodes=200 / max_depth=16）之内。
    const stats = cardStats({
      type: 17,
      profile: card.frame.profile,
      card_version: "1.5",
      card: card.frame.card,
      plain: card.frame.plain,
    });
    expect(stats.nodes).toBeLessThanOrEqual(200);
    expect(stats.maxDepth).toBeLessThanOrEqual(16);

    // 布局不改变答案语义：点击第二个选项照旧记录为答案 "2" 并改写同一张卡。
    harness.transport.pushBotEvent(cardActionEvent(61, {
      messageId: card.messageId,
      channelType: 1,
      channelId: target.channelId,
      actionId: "ask_opt_2",
      operatorUid: "owner-1",
      inputs: {},
    }));
    await waitForStatus(
      harness.service,
      target,
      (status) => status.state === "completed" && status.pendingQuestionIds.length === 0,
    );
    expect(await readAnswers(harness)).toEqual({
      answers: [
        { id: "deployment-target", questionId: expect.any(String), answeredBy: "owner-1", answer: "2" },
      ],
    });
    const answered = await harness.transport.waitForCardEdit(
      ({ messageId }) => messageId === card.messageId,
    );
    expect(bodyActionRows(answered)).toEqual([]);
    expect(JSON.stringify(answered.frame.card)).toContain(`已由 owner-1 回复：2. ${longLabel}`);
  } finally {
    await closeCoreHarness(harness);
  }
});

test("a plain reply answers the card question and rewrites the same card", async () => {
  const harness = await startAskHarness({
    id: "deployment-target",
    question: "Which deployment target?",
    options: [{ label: "staging" }, { label: "production" }],
  });
  try {
    const target: ChatTarget = { channelId: "dm-card-text", channelType: 1 };
    await harness.service.handleInbound(
      inbound({ messageId: "card-text-start", ...target, text: "needs an answer" }),
    );
    await harness.workers.waitForPromptCount(1);
    await waitForStatus(harness.service, target, (status) => status.pendingQuestionIds.length === 1);
    const card = harness.transport.sentCards[0];
    if (!card) {
      throw new Error("ask card was not sent");
    }

    await harness.service.handleInbound(
      inbound({ messageId: "card-text-reply", ...target, text: "1" }),
    );

    const completed = await waitForStatus(
      harness.service,
      target,
      (status) => status.state === "completed" && status.pendingQuestionIds.length === 0,
    );
    expect(completed.pendingQuestionIds).toHaveLength(0);
    expect(await readAnswers(harness)).toMatchObject({
      answers: [{ id: "deployment-target", answer: "1" }],
    });

    const edit = await harness.transport.waitForCardEdit(({ messageId }) => messageId === card.messageId);
    expect(cardActions(edit)).toHaveLength(0);
    expect(JSON.stringify(edit.frame.card)).toContain("已由 owner-1 回复：1. staging");
  } finally {
    await closeCoreHarness(harness);
  }
});

test("a free-form submission answers a question that has no options", async () => {
  const harness = await startAskHarness({ id: "release-window", question: "When do we ship?" });
  try {
    const target: ChatTarget = { channelId: "dm-card-free", channelType: 1 };
    await harness.service.handleInbound(
      inbound({ messageId: "card-free-start", ...target, text: "needs an answer" }),
    );
    await harness.workers.waitForPromptCount(1);
    await waitForStatus(harness.service, target, (status) => status.pendingQuestionIds.length === 1);
    const card = harness.transport.sentCards[0];
    if (!card) {
      throw new Error("ask card was not sent");
    }
    expect(JSON.stringify(card.frame.card)).toContain("Input.Text");

    harness.transport.pushBotEvent(cardActionEvent(11, {
      messageId: card.messageId,
      channelType: 1,
      channelId: target.channelId,
      actionId: "ask_submit",
      operatorUid: "owner-1",
      inputs: { ask_text: "ship it tonight" },
    }));

    await waitForStatus(
      harness.service,
      target,
      (status) => status.state === "completed" && status.pendingQuestionIds.length === 0,
    );
    expect(await readAnswers(harness)).toMatchObject({
      answers: [{ id: "release-window", answer: "ship it tonight" }],
    });
  } finally {
    await closeCoreHarness(harness);
  }
});

test("an empty submission keeps the question open behind fresh action ids", async () => {
  const harness = await startAskHarness({ id: "release-window", question: "When do we ship?" });
  try {
    const target: ChatTarget = { channelId: "dm-card-empty", channelType: 1 };
    await harness.service.handleInbound(
      inbound({ messageId: "card-empty-start", ...target, text: "needs an answer" }),
    );
    await harness.workers.waitForPromptCount(1);
    await waitForStatus(harness.service, target, (status) => status.pendingQuestionIds.length === 1);
    const card = harness.transport.sentCards[0];
    if (!card) {
      throw new Error("ask card was not sent");
    }

    harness.transport.pushBotEvent(cardActionEvent(21, {
      messageId: card.messageId,
      channelType: 1,
      channelId: target.channelId,
      actionId: "ask_submit",
      operatorUid: "owner-1",
      inputs: { ask_text: "   " },
    }));

    const rerendered = await harness.transport.waitForCardEdit(({ messageId }) => messageId === card.messageId);
    expect(cardActions(rerendered)).toEqual(["ask_submit#2"]);
    expect(harness.transport.sent.map(({ text }) => text)).toContain(
      "未收到内容：请填写后重新提交，或直接回复本消息作答。",
    );
    await expect(harness.service.status(target)).resolves.toMatchObject({
      pendingQuestionIds: [expect.any(String)],
    });

    // The server accepts one click per (message, action, operator): the re-rendered id is the
    // only way this operator can answer the same question from the card.
    harness.transport.pushBotEvent(cardActionEvent(22, {
      messageId: card.messageId,
      channelType: 1,
      channelId: target.channelId,
      actionId: "ask_submit#2",
      operatorUid: "owner-1",
      inputs: { ask_text: "ship it tonight" },
    }));

    await waitForStatus(
      harness.service,
      target,
      (status) => status.state === "completed" && status.pendingQuestionIds.length === 0,
    );
    expect(await readAnswers(harness)).toMatchObject({
      answers: [{ id: "release-window", answer: "ship it tonight" }],
    });
  } finally {
    await closeCoreHarness(harness);
  }
});

test("a multi-select submission answers with the selected indexes", async () => {
  const harness = await startAskHarness({
    id: "capabilities",
    question: "Which capabilities?",
    multi: true,
    options: [{ label: "read" }, { label: "write" }, { label: "admin" }],
  });
  try {
    const target: ChatTarget = { channelId: "dm-card-multi", channelType: 1 };
    await harness.service.handleInbound(
      inbound({ messageId: "card-multi-start", ...target, text: "needs an answer" }),
    );
    await harness.workers.waitForPromptCount(1);
    await waitForStatus(harness.service, target, (status) => status.pendingQuestionIds.length === 1);
    const card = harness.transport.sentCards[0];
    if (!card) {
      throw new Error("ask card was not sent");
    }
    expect(JSON.stringify(card.frame.card)).toContain("isMultiSelect");

    harness.transport.pushBotEvent(cardActionEvent(31, {
      messageId: card.messageId,
      channelType: 1,
      channelId: target.channelId,
      actionId: "ask_submit",
      operatorUid: "owner-1",
      inputs: { ask_choices: "1,3" },
    }));

    await waitForStatus(
      harness.service,
      target,
      (status) => status.state === "completed" && status.pendingQuestionIds.length === 0,
    );
    expect(await readAnswers(harness)).toMatchObject({
      answers: [{ id: "capabilities", answer: "1,3" }],
    });
  } finally {
    await closeCoreHarness(harness);
  }
});

test("a click from anyone but the owner in a DM leaves the question pending", async () => {
  const harness = await startAskHarness({
    id: "deployment-target",
    question: "Which deployment target?",
    options: [{ label: "staging" }],
  });
  try {
    const target: ChatTarget = { channelId: "dm-card-foreign", channelType: 1 };
    await harness.service.handleInbound(
      inbound({ messageId: "card-foreign-start", ...target, text: "needs an answer" }),
    );
    await harness.workers.waitForPromptCount(1);
    await waitForStatus(harness.service, target, (status) => status.pendingQuestionIds.length === 1);
    const card = harness.transport.sentCards[0];
    if (!card) {
      throw new Error("ask card was not sent");
    }

    harness.transport.pushBotEvent(cardActionEvent(41, {
      messageId: card.messageId,
      channelType: 1,
      channelId: target.channelId,
      actionId: "ask_opt_1",
      operatorUid: "someone-else",
      inputs: {},
    }));

    await harness.transport.waitForAck(41);
    expect(harness.transport.cardEdits).toHaveLength(0);
    expect(harness.workers.prompts).toHaveLength(1);
    const status = await harness.service.status(target);
    expect(status.pendingQuestionIds).toHaveLength(1);
  } finally {
    await closeCoreHarness(harness);
  }
});

test("a member's click in an enabled group answers the card and records the submitter", async () => {
  const harness = await startAskHarness({
    id: "deploy-target",
    question: "Which deployment target?",
    options: [{ label: "staging" }, { label: "production" }],
  });
  try {
    const target: ChatTarget = { channelId: "group-allowed", channelType: 2 };
    await harness.service.handleInbound(
      inbound({
        messageId: "group-card-start",
        ...target,
        text: "needs an answer",
        mentionedUserIds: [harness.bot.uid],
      }),
    );
    await harness.workers.waitForPromptCount(1);
    await waitForStatus(harness.service, target, (status) => status.pendingQuestionIds.length === 1);
    const card = harness.transport.sentCards[0];
    if (!card) {
      throw new Error("ask card was not sent");
    }

    harness.transport.pushBotEvent(cardActionEvent(7, {
      messageId: card.messageId,
      channelType: 2,
      channelId: target.channelId,
      actionId: "ask_opt_2",
      operatorUid: "member-9",
      inputs: {},
    }));

    await harness.transport.waitForAck(7);
    const completed = await waitForStatus(
      harness.service,
      target,
      (status) => status.state === "completed" && status.pendingQuestionIds.length === 0,
    );
    expect(completed.pendingQuestionIds).toHaveLength(0);
    expect(await readAnswers(harness)).toEqual({
      answers: [{ id: "deploy-target", questionId: expect.any(String), answeredBy: "member-9", answer: "2" }],
    });
    const edit = await harness.transport.waitForCardEdit(({ messageId }) => messageId === card.messageId);
    expect(cardActions(edit)).toHaveLength(0);
    expect(JSON.stringify(edit.frame.card)).toContain("已由 member-9 回复：2. production");
  } finally {
    await closeCoreHarness(harness);
  }
});

test("a member's click works in a thread of an enabled group", async () => {
  const harness = await startAskHarness({
    id: "thread-answer",
    question: "Proceed in this thread?",
  });
  try {
    const target: ChatTarget = { channelId: "group-allowed____thread-1", channelType: 5 };
    await harness.service.handleInbound(
      inbound({
        messageId: "thread-card-start",
        ...target,
        text: "needs an answer",
        mentionedUserIds: [harness.bot.uid],
      }),
    );
    await harness.workers.waitForPromptCount(1);
    await waitForStatus(harness.service, target, (status) => status.pendingQuestionIds.length === 1);
    const card = harness.transport.sentCards[0];
    if (!card) {
      throw new Error("ask card was not sent");
    }

    harness.transport.pushBotEvent(cardActionEvent(8, {
      messageId: card.messageId,
      channelType: 5,
      channelId: target.channelId,
      actionId: "ask_submit",
      operatorUid: "member-9",
      inputs: { ask_text: "yes, go ahead" },
    }));

    await harness.transport.waitForAck(8);
    const completed = await waitForStatus(
      harness.service,
      target,
      (status) => status.state === "completed" && status.pendingQuestionIds.length === 0,
    );
    expect(completed.pendingQuestionIds).toHaveLength(0);
    expect(await readAnswers(harness)).toEqual({
      answers: [{ id: "thread-answer", questionId: expect.any(String), answeredBy: "member-9", answer: "yes, go ahead" }],
    });
  } finally {
    await closeCoreHarness(harness);
  }
});

test("a member's click addressed to another chat cannot answer a question that lives elsewhere", async () => {
  const harness = await startAskHarness({
    id: "deploy-target",
    question: "Which deployment target?",
    options: [{ label: "staging" }, { label: "production" }],
  });
  try {
    const target: ChatTarget = { channelId: "group-allowed", channelType: 2 };
    await harness.service.handleInbound(
      inbound({
        messageId: "elsewhere-start",
        ...target,
        text: "needs an answer",
        mentionedUserIds: [harness.bot.uid],
      }),
    );
    await harness.workers.waitForPromptCount(1);
    await waitForStatus(harness.service, target, (status) => status.pendingQuestionIds.length === 1);
    const card = harness.transport.sentCards[0];
    if (!card) {
      throw new Error("ask card was not sent");
    }

    // An unlisted chat is refused outright; an allowed but different chat finds no question row.
    for (const [eventId, channelId] of [[9, "group-denied"], [10, "group-a"]] as const) {
      harness.transport.pushBotEvent(cardActionEvent(eventId, {
        messageId: card.messageId,
        channelType: 2,
        channelId,
        actionId: "ask_opt_2",
        operatorUid: "member-9",
        inputs: {},
      }));
      await harness.transport.waitForAck(eventId);
    }

    expect(harness.transport.cardEdits).toHaveLength(0);
    expect(harness.workers.prompts).toHaveLength(1);
    expect((await harness.service.status(target)).pendingQuestionIds).toHaveLength(1);
  } finally {
    await closeCoreHarness(harness);
  }
});

test("a member's text reply in an enabled group answers the pending question with attribution", async () => {
  const harness = await startAskHarness({
    id: "release-window",
    question: "When do we ship?",
  });
  try {
    const target: ChatTarget = { channelId: "group-allowed", channelType: 2 };
    await harness.service.handleInbound(
      inbound({
        messageId: "group-text-start",
        ...target,
        text: "needs an answer",
        mentionedUserIds: [harness.bot.uid],
      }),
    );
    await harness.workers.waitForPromptCount(1);
    await waitForStatus(harness.service, target, (status) => status.pendingQuestionIds.length === 1);

    await harness.service.handleInbound(
      inbound({
        messageId: "group-text-reply",
        ...target,
        senderId: "member-9",
        text: "ship it tonight",
        mentionedUserIds: [harness.bot.uid],
      }),
    );

    const completed = await waitForStatus(
      harness.service,
      target,
      (status) => status.state === "completed" && status.pendingQuestionIds.length === 0,
    );
    expect(completed.pendingQuestionIds).toHaveLength(0);
    expect(await readAnswers(harness)).toEqual({
      answers: [{ id: "release-window", questionId: expect.any(String), answeredBy: "member-9", answer: "ship it tonight" }],
    });
    // The reply was the answer, not a task of its own.
    expect(harness.workers.prompts).toHaveLength(1);
  } finally {
    await closeCoreHarness(harness);
  }
});

test("a click replay after the reply was recorded stays silent and updates nothing", async () => {
  const harness = await startAskHarness({
    id: "deployment-target",
    question: "Which deployment target?",
    options: [{ label: "staging" }, { label: "production" }],
  });
  try {
    const target: ChatTarget = { channelId: "dm-card-replay", channelType: 1 };
    await harness.service.handleInbound(
      inbound({ messageId: "card-replay-start", ...target, text: "needs an answer" }),
    );
    await harness.workers.waitForPromptCount(1);
    await waitForStatus(harness.service, target, (status) => status.pendingQuestionIds.length === 1);
    const card = harness.transport.sentCards[0];
    if (!card) {
      throw new Error("ask card was not sent");
    }

    await harness.service.handleInbound(
      inbound({ messageId: "card-replay-reply", ...target, text: "2" }),
    );
    await waitForStatus(
      harness.service,
      target,
      (status) => status.state === "completed" && status.pendingQuestionIds.length === 0,
    );
    await harness.transport.waitForCardEdit(({ messageId }) => messageId === card.messageId);
    const textsAfterAnswer = harness.transport.sent.length;

    // At-least-once delivery: the click that answered the question is delivered again.
    harness.transport.pushBotEvent(cardActionEvent(51, {
      messageId: card.messageId,
      channelType: 1,
      channelId: target.channelId,
      actionId: "ask_opt_2",
      operatorUid: "owner-1",
      inputs: {},
    }));

    await harness.transport.waitForAck(51);
    expect(harness.transport.sent).toHaveLength(textsAfterAnswer);
    expect(harness.transport.cardEdits).toHaveLength(1);
    expect(await readAnswers(harness)).toMatchObject({
      answers: [{ id: "deployment-target", answer: "2" }],
    });
  } finally {
    await closeCoreHarness(harness);
  }
});

test("a failed card send falls back to the text question", async () => {
  const harness = await startAskHarness(
    {
      id: "deployment-target",
      question: "Which deployment target?",
      options: [{ label: "staging" }, { label: "production" }],
    },
    { cards: true },
  );
  try {
    const target: ChatTarget = { channelId: "dm-card-fallback", channelType: 1 };
    harness.transport.cardSendFailure = new Error("card send failed");

    await harness.service.handleInbound(
      inbound({ messageId: "card-fallback-start", ...target, text: "needs an answer" }),
    );
    await harness.workers.waitForPromptCount(1);
    await waitForStatus(harness.service, target, (status) => status.pendingQuestionIds.length === 1);

    expect(harness.transport.sentCards).toHaveLength(0);
    const question = harness.transport.sent.map(({ text }) => text).join("\n");
    expect(question).toContain("问题：Which deployment target?");
    expect(question).toContain("直接回复选项编号作为答案");

    await harness.service.handleInbound(
      inbound({ messageId: "card-fallback-reply", ...target, text: "1" }),
    );

    await waitForStatus(
      harness.service,
      target,
      (status) => status.state === "completed" && status.pendingQuestionIds.length === 0,
    );
    expect(await readAnswers(harness)).toMatchObject({
      answers: [{ id: "deployment-target", answer: "1" }],
    });
  } finally {
    await closeCoreHarness(harness);
  }
});

test("one submit answers every question of a multi-question card", async () => {
  const harness = await startAskHarness([
    {
      id: "target",
      question: "Which deployment target?",
      options: [{ label: "staging" }, { label: "production" }],
    },
    { id: "window", question: "When do we ship?" },
    {
      id: "caps",
      question: "Which capabilities?",
      multi: true,
      options: [{ label: "read" }, { label: "write" }, { label: "admin" }],
    },
    { id: "note", question: "Anything else?", options: [{ label: "none" }] },
  ]);
  try {
    const target: ChatTarget = { channelId: "dm-set-submit", channelType: 1 };
    await harness.service.handleInbound(
      inbound({ messageId: "set-submit-start", ...target, text: "needs answers" }),
    );
    await harness.workers.waitForPromptCount(1);
    const pending = await waitForStatus(
      harness.service,
      target,
      (status) => status.pendingQuestionIds.length === 4,
    );
    expect(pending.pendingQuestionIds).toHaveLength(4);

    // One card carries all four questions: per-question inputs and a single submit.
    expect(harness.transport.sent).toHaveLength(0);
    expect(harness.transport.sentCards).toHaveLength(1);
    const card = harness.transport.sentCards[0];
    if (!card) {
      throw new Error("ask card was not sent");
    }
    expect(cardActions(card)).toEqual(["ask_submit"]);
    expect(inputIds(card)).toEqual([
      "ask_choices_1",
      "ask_text_1",
      "ask_text_2",
      "ask_choices_3",
      "ask_text_3",
      "ask_choices_4",
      "ask_text_4",
    ]);
    expect(card.frame.plain).toContain("问题 1/4：Which deployment target?");
    expect(card.frame.plain).toContain("问题 4/4：Anything else?");
    expect(card.frame.plain).toContain("直接回复本消息即作为整组答案");

    harness.transport.pushBotEvent(cardActionEvent(101, {
      messageId: card.messageId,
      channelType: 1,
      channelId: target.channelId,
      actionId: "ask_submit",
      operatorUid: "owner-1",
      inputs: {
        ask_choices_1: "2",
        ask_text_2: "tonight",
        ask_choices_3: "1,3",
        ask_choices_4: "1",
        // A filled free-form field answers as typed, whatever the selection says.
        ask_text_4: "actually, use canary",
      },
    }));

    await waitForStatus(
      harness.service,
      target,
      (status) => status.state === "completed" && status.pendingQuestionIds.length === 0,
    );
    await harness.transport.waitForAck(101);
    expect(await readAnswers(harness)).toEqual({
      answers: [
        { id: "target", questionId: expect.any(String), answeredBy: "owner-1", answer: "2" },
        { id: "window", questionId: expect.any(String), answeredBy: "owner-1", answer: "tonight" },
        { id: "caps", questionId: expect.any(String), answeredBy: "owner-1", answer: "1,3" },
        { id: "note", questionId: expect.any(String), answeredBy: "owner-1", answer: "actually, use canary" },
      ],
    });

    const edit = await harness.transport.waitForCardEdit(({ messageId }) => messageId === card.messageId);
    expect(cardActions(edit)).toHaveLength(0);
    const rendered = JSON.stringify(edit.frame.card);
    expect(rendered).toContain("已由 owner-1 回复：2. production");
    expect(rendered).toContain("已由 owner-1 回复：tonight");
    expect(rendered).toContain("已由 owner-1 回复：1. read；3. admin");
    expect(rendered).toContain("已由 owner-1 回复：actually, use canary");
  } finally {
    await closeCoreHarness(harness);
  }
});

test("a partial set submission answers what it filled and leaves the rest open", async () => {
  const harness = await startAskHarness([
    { id: "target", question: "Which deployment target?", options: [{ label: "staging" }] },
    { id: "window", question: "When do we ship?" },
  ]);
  try {
    const target: ChatTarget = { channelId: "dm-set-partial", channelType: 1 };
    await harness.service.handleInbound(
      inbound({ messageId: "set-partial-start", ...target, text: "needs answers" }),
    );
    await harness.workers.waitForPromptCount(1);
    await waitForStatus(harness.service, target, (status) => status.pendingQuestionIds.length === 2);
    const card = harness.transport.sentCards[0];
    if (!card) {
      throw new Error("ask card was not sent");
    }

    harness.transport.pushBotEvent(cardActionEvent(111, {
      messageId: card.messageId,
      channelType: 1,
      channelId: target.channelId,
      actionId: "ask_submit",
      operatorUid: "owner-1",
      inputs: { ask_choices_1: "1" },
    }));

    // The answered question is kept, the open one stays behind a fresh submit id.
    const rerendered = await harness.transport.waitForCardEdit(({ messageId }) => messageId === card.messageId);
    expect(rerendered.cardSeq).toBe(2);
    expect(cardActions(rerendered)).toEqual(["ask_submit#2"]);
    expect(JSON.stringify(rerendered.frame.card)).toContain("已由 owner-1 回复：1. staging");
    await expect(harness.service.status(target)).resolves.toMatchObject({
      pendingQuestionIds: [expect.any(String)],
    });

    harness.transport.pushBotEvent(cardActionEvent(112, {
      messageId: card.messageId,
      channelType: 1,
      channelId: target.channelId,
      actionId: "ask_submit#2",
      operatorUid: "owner-1",
      inputs: { ask_text_2: "ship it tonight" },
    }));

    await waitForStatus(
      harness.service,
      target,
      (status) => status.state === "completed" && status.pendingQuestionIds.length === 0,
    );
    expect(await readAnswers(harness)).toEqual({
      answers: [
        { id: "target", questionId: expect.any(String), answeredBy: "owner-1", answer: "1" },
        { id: "window", questionId: expect.any(String), answeredBy: "owner-1", answer: "ship it tonight" },
      ],
    });
    const closed = await harness.transport.waitForCardEdit(
      ({ messageId, cardSeq }) => messageId === card.messageId && cardSeq === 3,
    );
    expect(cardActions(closed)).toHaveLength(0);
    expect(JSON.stringify(closed.frame.card)).not.toContain("Input.Text");
  } finally {
    await closeCoreHarness(harness);
  }
});

test("a plain reply answers the whole set at once", async () => {
  const harness = await startAskHarness([
    {
      id: "target",
      question: "Which deployment target?",
      options: [{ label: "staging" }, { label: "production" }],
    },
    { id: "window", question: "When do we ship?" },
  ]);
  try {
    const target: ChatTarget = { channelId: "dm-set-text", channelType: 1 };
    await harness.service.handleInbound(
      inbound({ messageId: "set-text-start", ...target, text: "needs answers" }),
    );
    await harness.workers.waitForPromptCount(1);
    await waitForStatus(harness.service, target, (status) => status.pendingQuestionIds.length === 2);
    const card = harness.transport.sentCards[0];
    if (!card) {
      throw new Error("ask card was not sent");
    }

    await harness.service.handleInbound(
      inbound({ messageId: "set-text-reply", ...target, text: "1" }),
    );

    const completed = await waitForStatus(
      harness.service,
      target,
      (status) => status.state === "completed" && status.pendingQuestionIds.length === 0,
    );
    expect(completed.pendingQuestionIds).toHaveLength(0);
    // The one reply answers every open question: an option index here, the raw text there.
    expect(await readAnswers(harness)).toEqual({
      answers: [
        { id: "target", questionId: expect.any(String), answeredBy: "owner-1", answer: "1" },
        { id: "window", questionId: expect.any(String), answeredBy: "owner-1", answer: "1" },
      ],
    });

    const closed = await harness.transport.waitForCardEdit(({ messageId }) => messageId === card.messageId);
    expect(cardActions(closed)).toHaveLength(0);
    expect(JSON.stringify(closed.frame.card)).toContain("已由 owner-1 回复：1. staging");
    expect(JSON.stringify(closed.frame.card)).not.toContain("Input.Text");
  } finally {
    await closeCoreHarness(harness);
  }
});

test("a replayed set submission answers nothing twice", async () => {
  const harness = await startAskHarness([
    { id: "target", question: "Which deployment target?", options: [{ label: "staging" }] },
    { id: "window", question: "When do we ship?" },
  ]);
  try {
    const target: ChatTarget = { channelId: "dm-set-replay", channelType: 1 };
    await harness.service.handleInbound(
      inbound({ messageId: "set-replay-start", ...target, text: "needs answers" }),
    );
    await harness.workers.waitForPromptCount(1);
    await waitForStatus(harness.service, target, (status) => status.pendingQuestionIds.length === 2);
    const card = harness.transport.sentCards[0];
    if (!card) {
      throw new Error("ask card was not sent");
    }

    const click = {
      messageId: card.messageId,
      channelType: 1 as const,
      channelId: target.channelId,
      actionId: "ask_submit",
      operatorUid: "owner-1",
      inputs: { ask_choices_1: "1", ask_text_2: "tonight" },
    };
    harness.transport.pushBotEvent(cardActionEvent(121, click));
    await waitForStatus(
      harness.service,
      target,
      (status) => status.state === "completed" && status.pendingQuestionIds.length === 0,
    );
    await harness.transport.waitForAck(121);
    expect(await readAnswers(harness)).toMatchObject({
      answers: [{ id: "target", answer: "1" }, { id: "window", answer: "tonight" }],
    });
    const editsAfterAnswer = harness.transport.cardEdits.length;
    const textsAfterAnswer = harness.transport.sent.length;

    // At-least-once delivery: the submission that answered the card is delivered again.
    harness.transport.pushBotEvent(cardActionEvent(122, click));
    await harness.transport.waitForAck(122);
    expect(harness.transport.cardEdits).toHaveLength(editsAfterAnswer);
    expect(harness.transport.sent).toHaveLength(textsAfterAnswer);
    expect(await readAnswers(harness)).toMatchObject({
      answers: [{ id: "target", answer: "1" }, { id: "window", answer: "tonight" }],
    });
  } finally {
    await closeCoreHarness(harness);
  }
});

test("a set frame the deployment refuses falls back to one card per question", async () => {
  const harness = await startAskHarness(
    [
      {
        id: "target",
        question: "Which deployment target?",
        options: [{ label: "staging" }, { label: "production" }],
      },
      { id: "window", question: "When do we ship?" },
    ],
    {
      setup: (transport) => {
        // The deployment can carry single-question cards but not the set frame.
        transport.cardSendRefusal = ({ card }) =>
          cardBody(card).filter(({ type }) => type === "Input.Text").length > 1;
      },
    },
  );
  try {
    const target: ChatTarget = { channelId: "dm-set-fallback", channelType: 1 };
    await harness.service.handleInbound(
      inbound({ messageId: "set-fallback-start", ...target, text: "needs answers" }),
    );
    await harness.workers.waitForPromptCount(1);
    await waitForStatus(harness.service, target, (status) => status.pendingQuestionIds.length === 2);

    const first = await harness.transport.waitForCardSend(({ messageId }) => messageId === "card-1");
    expect(inputIds(first)).toEqual(["ask_text"]);
    expect(bodyActionRows(first)).toEqual([["ask_opt_1"], ["ask_opt_2"]]);
    expect(cardActions(first)).toEqual(["ask_submit"]);
    expect(first.frame.plain).toContain("问题 1/2：Which deployment target?");

    harness.transport.pushBotEvent(cardActionEvent(131, {
      messageId: first.messageId,
      channelType: 1,
      channelId: target.channelId,
      actionId: "ask_opt_2",
      operatorUid: "owner-1",
      inputs: {},
    }));
    const second = await harness.transport.waitForCardSend(({ messageId }) => messageId !== first.messageId);
    expect(inputIds(second)).toEqual(["ask_text"]);
    expect(second.frame.plain).toContain("问题 2/2：When do we ship?");
    await expect(harness.service.status(target)).resolves.toMatchObject({
      pendingQuestionIds: [expect.any(String)],
    });

    harness.transport.pushBotEvent(cardActionEvent(132, {
      messageId: second.messageId,
      channelType: 1,
      channelId: target.channelId,
      actionId: "ask_submit",
      operatorUid: "owner-1",
      inputs: { ask_text: "tonight" },
    }));

    await waitForStatus(
      harness.service,
      target,
      (status) => status.state === "completed" && status.pendingQuestionIds.length === 0,
    );
    expect(await readAnswers(harness)).toEqual({
      answers: [
        { id: "target", questionId: expect.any(String), answeredBy: "owner-1", answer: "2" },
        { id: "window", questionId: expect.any(String), answeredBy: "owner-1", answer: "tonight" },
      ],
    });
  } finally {
    await closeCoreHarness(harness);
  }
});

test("a set without card support asks in text and one reply answers the rest", async () => {
  const harness = await startAskHarness(
    [
      { id: "target", question: "Which deployment target?", options: [{ label: "staging" }] },
      { id: "window", question: "When do we ship?" },
    ],
    { cards: false },
  );
  try {
    const target: ChatTarget = { channelId: "dm-set-plain", channelType: 1 };
    await harness.service.handleInbound(
      inbound({ messageId: "set-plain-start", ...target, text: "needs answers" }),
    );
    await harness.workers.waitForPromptCount(1);
    await waitForStatus(harness.service, target, (status) => status.pendingQuestionIds.length === 2);

    // Only the first question had to be shown: one reply settles the whole set.
    const question = await harness.transport.waitForRemoteText(({ text }) =>
      text.includes("问题 1/2：Which deployment target?"));
    expect(question.text).toContain("直接回复本消息即作为整组答案");
    expect(harness.transport.sent).toHaveLength(1);

    await harness.service.handleInbound(
      inbound({ messageId: "set-plain-reply", ...target, text: "production, tonight" }),
    );

    await waitForStatus(
      harness.service,
      target,
      (status) => status.state === "completed" && status.pendingQuestionIds.length === 0,
    );
    expect(harness.transport.sent).toHaveLength(1);
    expect(await readAnswers(harness)).toEqual({
      answers: [
        { id: "target", questionId: expect.any(String), answeredBy: "owner-1", answer: "production, tonight" },
        { id: "window", questionId: expect.any(String), answeredBy: "owner-1", answer: "production, tonight" },
      ],
    });
  } finally {
    await closeCoreHarness(harness);
  }
});

test("a timed-out question stops the turn and stays pending until the reply wakes the agent", async () => {
  const harness = await startAskTimeoutHarness({
    id: "deployment-target",
    question: "Which deployment target?",
    options: [{ label: "staging" }, { label: "production" }],
  });
  try {
    const target: ChatTarget = { channelId: "dm-ask-timeout", channelType: 1 };
    await harness.service.handleInbound(
      inbound({ messageId: "ask-timeout-start", ...target, text: "needs an answer" }),
    );
    await harness.workers.waitForPromptCount(1);

    const stopped = await waitForStatus(
      harness.service,
      target,
      (status) => status.state === "interrupted" && status.pendingQuestionIds.length === 1,
    );
    expect(stopped.pendingQuestionIds).toHaveLength(1);
    // 静默停住：一次 abort、没有提示消息、卡片没有被改写（按钮保持可提交）。
    expect(harness.workers.workers[0]?.abortCalls).toBe(1);
    expect(harness.transport.cardEdits).toHaveLength(0);
    expect(harness.transport.sent).toHaveLength(0);
    const card = harness.transport.sentCards[0];
    if (!card) {
      throw new Error("ask card was not sent");
    }

    await harness.service.handleInbound(
      inbound({ messageId: "ask-timeout-reply", ...target, text: "1" }),
    );
    await harness.workers.waitForPromptCount(2);

    // 唤醒：同一 session 的新回合，正文带题面、调用方 id 与还原后的答案。
    const wake = harness.workers.prompts[1];
    expect(wake?.sessionId).toBe(harness.workers.prompts[0]?.sessionId);
    expect(wake?.text).toContain("[ask 答复]");
    expect(wake?.text).toContain("（id=deployment-target）");
    expect(wake?.text).toContain("Which deployment target?");
    expect(wake?.text).toContain("答案（由 owner-1 回复）：1. staging");
    await waitForStatus(
      harness.service,
      target,
      (status) => status.state === "completed" && status.pendingQuestionIds.length === 0,
    );
    expect(await readText(join(harness.root, "tmp", "wake.log"))).toContain("答案（由 owner-1 回复）：1. staging");
    const edit = await harness.transport.waitForCardEdit(({ messageId }) => messageId === card.messageId);
    expect(cardActions(edit)).toHaveLength(0);
    expect(JSON.stringify(edit.frame.card)).toContain("已由 owner-1 回复：1. staging");
  } finally {
    await closeCoreHarness(harness);
  }
});

test("a card submission after the timeout answers once and wakes the agent", async () => {
  const harness = await startAskTimeoutHarness({
    id: "release-window",
    question: "When do we ship?",
  });
  try {
    const target: ChatTarget = { channelId: "dm-ask-timeout-card", channelType: 1 };
    await harness.service.handleInbound(
      inbound({ messageId: "ask-timeout-card-start", ...target, text: "needs an answer" }),
    );
    await harness.workers.waitForPromptCount(1);
    await waitForStatus(
      harness.service,
      target,
      (status) => status.state === "interrupted" && status.pendingQuestionIds.length === 1,
    );
    const card = harness.transport.sentCards[0];
    if (!card) {
      throw new Error("ask card was not sent");
    }
    // 卡片从未被改写，原始 action id 因此仍可提交。
    expect(harness.transport.cardEdits).toHaveLength(0);

    const click = {
      messageId: card.messageId,
      channelType: 1 as const,
      channelId: target.channelId,
      actionId: "ask_submit",
      operatorUid: "owner-1",
      inputs: { ask_text: "ship it tonight" },
    };
    harness.transport.pushBotEvent(cardActionEvent(41, click));
    await harness.workers.waitForPromptCount(2);
    await harness.transport.waitForAck(41);
    expect(harness.workers.prompts[1]?.text).toContain("答案（由 owner-1 回复）：ship it tonight");
    await waitForStatus(
      harness.service,
      target,
      (status) => status.state === "completed" && status.pendingQuestionIds.length === 0,
    );

    // 至少一次投递的重复点击不再作答、不再改写卡片、也不再唤醒。
    const editsAfterAnswer = harness.transport.cardEdits.length;
    const promptsAfterAnswer = harness.workers.prompts.length;
    harness.transport.pushBotEvent(cardActionEvent(42, click));
    await harness.transport.waitForAck(42);
    expect(harness.transport.cardEdits).toHaveLength(editsAfterAnswer);
    expect(harness.workers.prompts).toHaveLength(promptsAfterAnswer);
    expect(harness.transport.sent.map(({ text }) => text)).not.toContain(
      "该问题已失效，本次点击未作为答案。",
    );
  } finally {
    await closeCoreHarness(harness);
  }
});

test("a timed-out question set stops the turn once and keeps every row answerable", async () => {
  const harness = await startAskTimeoutHarness([
    {
      id: "target",
      question: "Which deployment target?",
      options: [{ label: "staging" }, { label: "production" }],
    },
    { id: "window", question: "When do we ship?" },
  ]);
  try {
    const target: ChatTarget = { channelId: "dm-ask-timeout-set", channelType: 1 };
    await harness.service.handleInbound(
      inbound({ messageId: "ask-timeout-set-start", ...target, text: "needs answers" }),
    );
    await harness.workers.waitForPromptCount(1);
    const stopped = await waitForStatus(
      harness.service,
      target,
      (status) => status.state === "interrupted" && status.pendingQuestionIds.length === 2,
    );
    expect(stopped.pendingQuestionIds).toHaveLength(2);
    // 整组共用一次停回合与一张卡片：卡片保持原样，两题都还可以作答。
    expect(harness.workers.workers[0]?.abortCalls).toBe(1);
    expect(harness.transport.cardEdits).toHaveLength(0);
    const card = harness.transport.sentCards[0];
    if (!card) {
      throw new Error("ask set card was not sent");
    }

    // 部分提交按既有规则作答已填问题，其余保持待答；答案唤醒一个新回合。
    harness.transport.pushBotEvent(cardActionEvent(51, {
      messageId: card.messageId,
      channelType: 1,
      channelId: target.channelId,
      actionId: "ask_submit",
      operatorUid: "owner-1",
      inputs: { ask_text_2: "ship it tonight" },
    }));
    await harness.workers.waitForPromptCount(2);
    expect(harness.workers.prompts[1]?.text).toContain("答案（由 owner-1 回复）：ship it tonight");
    expect(harness.workers.prompts[1]?.text).not.toContain("Which deployment target?");
    const reopened = await harness.transport.waitForCardEdit(({ messageId }) => messageId === card.messageId);
    expect(cardActions(reopened)).toEqual(["ask_submit#2"]);
    // 唤醒回合本身结束（其提示同一题不会重复作答）后，再交付第二题的回答。
    await waitForStatus(
      harness.service,
      target,
      (status) => status.state === "completed" && status.pendingQuestionIds.length === 1,
    );

    // 第二题之后用文本作答：同样记录并唤醒，两题都不重复作答。
    await harness.service.handleInbound(
      inbound({ messageId: "ask-timeout-set-reply", ...target, text: "2" }),
    );
    await harness.workers.waitForPromptCount(3);
    expect(harness.workers.prompts[2]?.text).toContain("Which deployment target?");
    expect(harness.workers.prompts[2]?.text).toContain("答案（由 owner-1 回复）：2. production");
    await waitForStatus(
      harness.service,
      target,
      (status) => status.state === "completed" && status.pendingQuestionIds.length === 0,
    );
    const closed = await harness.transport.waitForCardEdit(
      ({ messageId, cardSeq }) => messageId === card.messageId && cardSeq === 3,
    );
    expect(cardActions(closed)).toHaveLength(0);
  } finally {
    await closeCoreHarness(harness);
  }
});
