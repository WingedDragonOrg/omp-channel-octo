import { expect, test } from "bun:test";
import { join } from "node:path";
import type { ChannelStatus, ChatTarget } from "../src/contracts";
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

function startAskHarness(question: Record<string, unknown>, options: { cards?: boolean } = {}) {
  const transport = new FakeTransport();
  transport.cardSupported = options.cards ?? true;
  return createCoreHarness({
    transport,
    prompt: async ({ append, promptContext }) => {
      const ask = promptContext?.tools?.find(({ name }) => name === "ask");
      if (!ask) {
        throw new Error("ask host tool missing");
      }
      const result = await ask.execute({ questions: [question] }, new AbortController().signal);
      await append("answer.log", result);
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
      answers: [{ id: "deployment-target", questionId, answer: "2" }],
    });
    await harness.transport.waitForAck(7);

    const edit = await harness.transport.waitForCardEdit(({ messageId }) => messageId === card.messageId);
    expect(edit.cardSeq).toBe(2);
    expect(cardActions(edit)).toHaveLength(0);
    expect(JSON.stringify(edit.frame.card)).toContain("已回复：2. production");
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
    expect(JSON.stringify(edit.frame.card)).toContain("已回复：1. staging");
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

test("a click from anyone but the owner leaves the question pending", async () => {
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
