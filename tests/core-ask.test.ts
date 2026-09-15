import { expect, test } from "bun:test";
import { join } from "node:path";
import type { ChannelStatus } from "../src/contracts";
import {
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

test("a plain reply answers the pending question with its option index", async () => {
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
    const questionId = onlyPendingQuestionId(pending);

    const question = harness.transport.sent
      .filter(({ target: sentTarget }) => sentTarget.channelId === target.channelId)
      .map(({ text }) => text)
      .join("\n");
    expect(question).toContain("问题：Which deployment target?");
    expect(question).toContain("2. production — customer traffic");
    expect(question).toContain("直接回复选项编号作为答案");

    await harness.service.handleInbound(
      inbound({ messageId: "answer-plain-reply", ...target, text: "2" }),
    );
    const completed = await waitForStatus(
      harness.service,
      target,
      (status) => status.state === "completed" && status.pendingQuestionIds.length === 0,
    );
    expect(completed.pendingQuestionIds).toHaveLength(0);
    expect(JSON.parse(await readText(join(harness.root, "tmp", "answer.log")))).toEqual({
      answers: [{ id: "deployment-target", questionId, answer: "2" }],
    });
    expect(harness.workers.prompts).toHaveLength(1);
  } finally {
    await closeCoreHarness(harness);
  }
});

test("a free-form reply is returned as the free-form answer instead of starting a task", async () => {
  const harness = await createCoreHarness({
    prompt: async ({ append, promptContext, text }) => {
      const ask = promptContext?.tools?.find(({ name }) => name === "ask");
      if (!ask) {
        throw new Error("ask host tool missing");
      }
      const result = await ask.execute(
        {
          questions: [
            {
              id: "release-window",
              question: text,
              options: [{ label: "now" }, { label: "later" }],
            },
          ],
        },
        new AbortController().signal,
      );
      await append("answer.log", result);
    },
  });
  try {
    const target = { channelId: "dm-ask-free", channelType: 1 as const };
    await harness.service.handleInbound(
      inbound({ messageId: "ask-free-start", ...target, text: "when do we ship?" }),
    );
    await harness.workers.waitForPromptCount(1);
    const pending = await waitForStatus(
      harness.service,
      target,
      (status) => status.pendingQuestionIds.length === 1,
    );
    const questionId = onlyPendingQuestionId(pending);

    await harness.service.handleInbound(
      inbound({ messageId: "ask-free-reply", ...target, text: "先不发布，等周四" }),
    );
    const completed = await waitForStatus(
      harness.service,
      target,
      (status) => status.state === "completed" && status.pendingQuestionIds.length === 0,
    );
    expect(completed.pendingQuestionIds).toHaveLength(0);
    expect(JSON.parse(await readText(join(harness.root, "tmp", "answer.log")))).toEqual({
      answers: [{ id: "release-window", questionId, answer: "先不发布，等周四" }],
    });
    expect(harness.workers.prompts.map(({ text }) => text)).toEqual(["when do we ship?"]);
  } finally {
    await closeCoreHarness(harness);
  }
});

test("an out-of-range option reply leaves the question pending and starts no task", async () => {
  const harness = await createCoreHarness({
    prompt: async ({ promptContext, text }) => {
      const ask = promptContext?.tools?.find(({ name }) => name === "ask");
      if (!ask) {
        throw new Error("ask host tool missing");
      }
      await ask.execute(
        { questions: [{ id: "approval", question: text, options: [{ label: "yes" }, { label: "no" }] }] },
        new AbortController().signal,
      );
    },
  });
  try {
    const target = { channelId: "dm-ask-invalid", channelType: 1 as const };
    await harness.service.handleInbound(
      inbound({ messageId: "ask-invalid-start", ...target, text: "approve?" }),
    );
    await harness.workers.waitForPromptCount(1);
    const pending = await waitForStatus(
      harness.service,
      target,
      (status) => status.pendingQuestionIds.length === 1,
    );
    const questionId = onlyPendingQuestionId(pending);

    await harness.service.handleInbound(
      inbound({ messageId: "ask-invalid-reply", ...target, text: "9" }),
    );
    const rejected = await harness.transport.waitForAttempt(({ text }) => text.includes("答案无效"));
    expect(rejected.text).toContain("直接输入答案");
    const after = await harness.service.status(target);
    expect(after.pendingQuestionIds).toEqual([questionId]);
    expect(after.queued).toBe(0);
    expect(after.state).toBe("running");
    expect(harness.workers.prompts).toHaveLength(1);
  } finally {
    await closeCoreHarness(harness);
  }
});

test("a reply in another chat does not answer the pending question", async () => {
  const harness = await createCoreHarness({
    prompt: async ({ promptContext, text }) => {
      const ask = promptContext?.tools?.find(({ name }) => name === "ask");
      if (!ask) {
        throw new Error("ask host tool missing");
      }
      await ask.execute(
        { questions: [{ id: "scope", question: text }] },
        new AbortController().signal,
      );
    },
  });
  try {
    const target = { channelId: "dm-ask-scope", channelType: 1 as const };
    await harness.service.handleInbound(
      inbound({ messageId: "ask-scope-start", ...target, text: "which scope?" }),
    );
    await harness.workers.waitForPromptCount(1);
    const pending = await waitForStatus(
      harness.service,
      target,
      (status) => status.pendingQuestionIds.length === 1,
    );
    const questionId = onlyPendingQuestionId(pending);

    const other = { channelId: "dm-ask-other", channelType: 1 as const };
    await harness.service.handleInbound(
      inbound({ messageId: "ask-scope-other-reply", ...other, text: "backend only" }),
    );

    expect((await harness.service.status(target)).pendingQuestionIds).toEqual([questionId]);
    const otherStatus = await harness.service.status(other);
    expect(otherStatus.state).toBe("queued");
    expect(otherStatus.queued).toBe(1);
    expect(harness.workers.prompts).toHaveLength(1);
  } finally {
    await closeCoreHarness(harness);
  }
});

test("/cancel invalidates a pending ask and a later reply is ordinary task input", async () => {
  const harness = await createCoreHarness({
    prompt: async ({ append, promptContext, text }) => {
      const ask = promptContext?.tools?.find(({ name }) => name === "ask");
      if (!ask) {
        throw new Error("ask host tool missing");
      }
      await ask.execute(
        { questions: [{ id: "approval", question: text }] },
        new AbortController().signal,
      );
      await append("answer.log", "answered\n");
    },
  });
  try {
    const target = { channelId: "dm-ask-cancel", channelType: 1 as const };
    await harness.service.handleInbound(
      inbound({ messageId: "ask-cancel-start", ...target, text: "pending approval" }),
    );
    await harness.workers.waitForPromptCount(1);
    await waitForStatus(
      harness.service,
      target,
      (status) => status.pendingQuestionIds.length === 1,
    );

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
      inbound({ messageId: "ask-late-reply", ...target, text: "1" }),
    );
    await harness.workers.waitForPromptCount(2);
    expect(harness.workers.prompts.map(({ text }) => text)).toEqual(["pending approval", "1"]);
    expect(await readText(join(harness.root, "tmp", "answer.log"))).toBe("");
    expect(
      harness.transport.sent.some(({ target: sentTarget, text }) =>
        sentTarget.channelId === target.channelId && /取消|cancel/i.test(text),
      ),
    ).toBe(true);
  } finally {
    await closeCoreHarness(harness);
  }
});
