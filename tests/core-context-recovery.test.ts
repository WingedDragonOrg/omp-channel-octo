import { expect, test } from "bun:test";
import {
  closeCoreHarness,
  createCoreHarness,
  FakeTransport,
  FakeWorkerFactory,
  inbound,
  waitForStatus,
} from "./fixtures/core-fakes";
import type { CoreHarness } from "./fixtures/core-fakes";

type ContextInjection = {
  readonly snapshot?: {
    readonly content?: string;
    readonly version?: number;
  };
  readonly injectionDecision?: string;
  readonly attemptId?: string;
};

function contextInjection(
  prompt: { promptContext: unknown } | undefined,
): ContextInjection | undefined {
  return (prompt?.promptContext as { contextInjection?: ContextInjection } | undefined)
    ?.contextInjection;
}

test("does not repeat unchanged group context after restoring the same session", async () => {
  const bot = { uid: "bot-context-recovery-success", name: "Context Recovery Bot" };
  const target = { channelId: "group-a", channelType: 2 as const };
  const sharedContent = "persistent shared context for the restored session";
  const transport = new FakeTransport({ bot });
  transport.setGroupContext(target, {
    content: sharedContent,
    version: 11,
    updatedAt: "2026-09-15T00:00:00.000Z",
    updatedBy: "group-admin",
  });

  const first = await createCoreHarness({ bot, transport });
  let restarted: CoreHarness | undefined;
  try {
    const firstTask = inbound({
      messageId: "context-recovery-success-first",
      ...target,
      text: "first group recovery task",
      mentionedUserIds: [bot.uid],
    });
    await first.service.handleInbound(firstTask);
    await first.workers.waitForPromptCount(1);
    await waitForStatus(first.service, target, (status) => status.state === "completed");

    const firstPrompt = first.workers.prompts[0];
    const firstInjection = contextInjection(firstPrompt);
    expect(firstPrompt?.text).toBe(firstTask.text);
    expect(firstInjection?.snapshot).toMatchObject({ content: sharedContent, version: 11 });
    expect(firstInjection?.injectionDecision).toEqual(expect.any(String));
    expect(firstInjection?.attemptId).toEqual(expect.any(String));

    const originalBinding = await first.service.status(target);
    expect(originalBinding.sessionId).toEqual(expect.any(String));
    await closeCoreHarness(first, { remove: false });

    restarted = await createCoreHarness({
      root: first.root,
      bot,
      transport,
    });
    const restoredBinding = await restarted.service.status(target);
    expect(restoredBinding.sessionId).toBe(originalBinding.sessionId);
    expect(restoredBinding.cwd).toBe(originalBinding.cwd);

    const secondTask = inbound({
      messageId: "context-recovery-success-second",
      ...target,
      text: "same version recovery follow-up",
      mentionedUserIds: [bot.uid],
    });
    await restarted.service.handleInbound(secondTask);
    await restarted.workers.waitForPromptCount(1);
    await waitForStatus(restarted.service, target, (status) => status.state === "completed");

    const resumedPrompt = restarted.workers.prompts[0];
    const resumedInjection = contextInjection(resumedPrompt);
    expect(resumedPrompt?.text).toBe(secondTask.text);
    expect(resumedPrompt?.text).not.toContain(sharedContent);
    expect(resumedInjection?.snapshot).toMatchObject({ content: sharedContent, version: 11 });
    expect(resumedInjection?.injectionDecision).toBe("unchanged");
  } finally {
    if (restarted) {
      await closeCoreHarness(restarted);
    } else {
      await closeCoreHarness(first);
    }
  }
});

test("replays pending group context injection after a prompt failure and restart", async () => {
  const bot = { uid: "bot-context-recovery-failure", name: "Context Recovery Failure Bot" };
  const target = { channelId: "group-a", channelType: 2 as const };
  const sharedContent = "pending shared context must be replayed after failure";
  const transport = new FakeTransport({ bot });
  transport.setGroupContext(target, {
    content: sharedContent,
    version: 17,
    updatedAt: "2026-09-15T00:01:00.000Z",
    updatedBy: "group-admin",
  });
  const failingWorkers = new FakeWorkerFactory(async () => {
    throw new Error("fixture prompt failed after entry");
  });

  const first = await createCoreHarness({ bot, transport, workers: failingWorkers });
  let restarted: CoreHarness | undefined;
  try {
    const failedTask = inbound({
      messageId: "context-recovery-failure-first",
      ...target,
      text: "group task whose prompt fails",
      mentionedUserIds: [bot.uid],
    });
    await first.service.handleInbound(failedTask);
    await first.workers.waitForPromptCount(1);
    await waitForStatus(first.service, target, (status) => status.state === "failed");

    const failedPrompt = first.workers.prompts[0];
    const pendingInjection = contextInjection(failedPrompt);
    expect(failedPrompt?.text).toBe(failedTask.text);
    expect(pendingInjection?.snapshot).toMatchObject({ content: sharedContent, version: 17 });
    expect(pendingInjection?.injectionDecision).toEqual(expect.any(String));
    expect(pendingInjection?.attemptId).toEqual(expect.any(String));

    const originalBinding = await first.service.status(target);
    expect(originalBinding.sessionId).toEqual(expect.any(String));
    await closeCoreHarness(first, { remove: false });

    restarted = await createCoreHarness({
      root: first.root,
      bot,
      transport,
    });
    const restoredBinding = await restarted.service.status(target);
    expect(restoredBinding.sessionId).toBe(originalBinding.sessionId);
    expect(restoredBinding.cwd).toBe(originalBinding.cwd);

    const replayTask = inbound({
      messageId: "context-recovery-failure-replay",
      ...target,
      text: "replay group task after prompt failure",
      mentionedUserIds: [bot.uid],
    });
    await restarted.service.handleInbound(replayTask);
    await restarted.workers.waitForPromptCount(1);
    await waitForStatus(restarted.service, target, (status) => status.state === "completed");

    const replayPrompt = restarted.workers.prompts[0];
    const replayInjection = contextInjection(replayPrompt);
    expect(replayPrompt?.text).toBe(replayTask.text);
    expect(replayInjection?.snapshot).toMatchObject({ content: sharedContent, version: 17 });
    expect(replayInjection?.attemptId).toBe(pendingInjection?.attemptId);
  } finally {
    if (restarted) {
      await closeCoreHarness(restarted);
    } else {
      await closeCoreHarness(first);
    }
  }
});
