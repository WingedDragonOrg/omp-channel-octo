import { expect, test } from "bun:test";
import { join } from "node:path";
import {
  closeCoreHarness,
  createCoreHarness,
  inbound,
  readText,
  waitForStatus,
} from "./fixtures/core-fakes";

test("ask persists a question and /answer resumes only the original chat task", async () => {
  const harness = await createCoreHarness({
    prompt: async ({ append, promptContext, text }) => {
      if (!text.includes("needs an answer")) {
        throw new Error("unexpected prompt text");
      }
      const ask = promptContext?.tools?.find(({ name }) => name === "ask");
      if (!ask) {
        throw new Error("ask host tool missing");
      }
      const result = await ask.execute(
        {
          questions: [
            {
              id: "deployment-target",
              question: "Which deployment target?",
              options: [
                { label: "staging", description: "shared staging" },
                { label: "production", description: "customer traffic" },
              ],
            },
          ],
        },
        new AbortController().signal,
      );
      await append("answer.log", result);
    },
  });
  try {
    const target = { channelId: "dm-ask", channelType: 1 as const };
    await harness.service.handleInbound(
      inbound({ messageId: "ask-start", ...target, text: "needs an answer" }),
    );
    await harness.workers.waitForPromptCount(1);
    const pending = await waitForStatus(
      harness.service,
      target,
      (status) => status.pendingQuestionIds.length === 1,
    );
    const questionId = pending.pendingQuestionIds[0];
    if (questionId === undefined) {
      throw new Error("pending question ID missing");
    }
    expect(questionId).toEqual(expect.any(String));
    expect(
      harness.transport.sent.some(
        ({ target: sentTarget, text }) =>
          sentTarget.channelId === target.channelId && text.includes(questionId),
      ),
    ).toBe(true);

    const wrongTarget = { channelId: "dm-other", channelType: 1 as const };
    await harness.service.handleInbound(
      inbound({
        messageId: "answer-wrong-chat",
        ...wrongTarget,
        text: `/answer ${questionId} 1`,
      }),
    );
    expect((await harness.service.status(target)).pendingQuestionIds).toEqual([questionId]);
    expect(harness.workers.prompts).toHaveLength(1);

    await harness.service.handleInbound(
      inbound({
        messageId: "answer-correct-chat",
        ...target,
        text: `/answer ${questionId} 2`,
      }),
    );
    const completed = await waitForStatus(
      harness.service,
      target,
      (status) => status.state === "completed" && status.pendingQuestionIds.length === 0,
    );
    expect(completed.pendingQuestionIds).toHaveLength(0);
    expect(await readText(join(harness.root, "tmp", "answer.log"))).toContain("2");
    expect(harness.workers.prompts).toHaveLength(1);
  } finally {
    await closeCoreHarness(harness);
  }
});

test("/cancel invalidates a pending ask without choosing a default", async () => {
  const harness = await createCoreHarness({
    prompt: async ({ promptContext, text }) => {
      const ask = promptContext?.tools?.find(({ name }) => name === "ask");
      if (!ask) {
        throw new Error("ask host tool missing");
      }
      await ask.execute(
        { questions: [{ id: "approval", question: text }] },
        new AbortController().signal,
      );
    },
  });
  try {
    const target = { channelId: "dm-ask-cancel", channelType: 1 as const };
    await harness.service.handleInbound(
      inbound({ messageId: "ask-cancel-start", ...target, text: "pending approval" }),
    );
    await harness.workers.waitForPromptCount(1);
    const pending = await waitForStatus(
      harness.service,
      target,
      (status) => status.pendingQuestionIds.length === 1,
    );
    const questionId = pending.pendingQuestionIds[0];
    if (questionId === undefined) {
      throw new Error("pending question ID missing");
    }
    await harness.service.handleInbound(
      inbound({ messageId: "ask-cancel-command", ...target, text: "/cancel" }),
    );
    const cancelled = await waitForStatus(
      harness.service,
      target,
      (status) => status.state === "cancelled" && status.pendingQuestionIds.length === 0,
    );
    expect(cancelled.pendingQuestionIds).toHaveLength(0);
    await harness.service.handleInbound(
      inbound({
        messageId: "ask-late-answer",
        ...target,
        text: `/answer ${questionId} 1`,
      }),
    );
    expect(harness.workers.prompts).toHaveLength(1);
    expect(
      harness.transport.sent.some(({ target: sentTarget, text }) =>
        sentTarget.channelId === target.channelId && /取消|cancel/i.test(text),
      ),
    ).toBe(true);
  } finally {
    await closeCoreHarness(harness);
  }
});
