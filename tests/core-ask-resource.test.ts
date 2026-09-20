import { expect, test, vi } from "bun:test";
import { closeCoreHarness, createCoreHarness, inbound, waitForStatus } from "./fixtures/core-fakes.ts";

// An abort acknowledgement does not guarantee agent_end. Without authoritative
// worker shutdown, this task holds its directory and the late answer never runs.
test("ask timeout releases a worker that acknowledges abort without ending its turn", async () => {
  const harness = await createCoreHarness({
    ask: { timeoutMs: 40 },
    prompt: async ({ text, promptContext, onStop, append }) => {
      if (text.startsWith("[ask 答复]")) {
        await append("answer.txt", text);
        return;
      }
      const stopped = Promise.withResolvers<void>();
      onStop(stopped.resolve);
      const ask = promptContext!.tools!.find(tool => tool.name === "ask")!;
      try { await ask.execute({ questions: [{ question: "Which target?" }] }, new AbortController().signal); }
      catch { await stopped.promise; }
    },
  });
  const target = { channelType: 1 as const, channelId: "timeout-resource" };
  try {
    vi.useFakeTimers();
    await harness.service.handleInbound(inbound({ ...target, messageId: "start", text: "ask" }));
    await harness.workers.waitForPromptCount(1);
    await waitForStatus(harness.service, target, s => s.pendingQuestionIds.length === 1);
    vi.advanceTimersByTime(40);
    const status = await waitForStatus(harness.service, target, s => s.state === "interrupted");
    expect(status.pendingQuestionIds).toHaveLength(1);
    expect(harness.transport.sent.some(message => message.text.includes("失败"))).toBe(false);
    await harness.service.handleInbound(inbound({ ...target, messageId: "answer", text: "staging" }));
    await waitForStatus(harness.service, target, s => s.state === "completed");
    expect(harness.workers.prompts[1]?.sessionId).toBe(harness.workers.prompts[0]?.sessionId);
    expect(await Bun.file(`${harness.root}/tmp/answer.txt`).text()).toContain("staging");
  } finally {
    vi.useRealTimers();
    await closeCoreHarness(harness);
  }
});
