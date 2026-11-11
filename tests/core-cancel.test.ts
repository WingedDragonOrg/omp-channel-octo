import { expect, test } from "bun:test";
import { join } from "node:path";
import {
  closeCoreHarness,
  createCoreHarness,
  inbound,
  readText,
  waitForStatus,
} from "./fixtures/core-fakes";

test("/cancel interrupts the active task and clears queued work immediately", async () => {
  let stopReady!: () => void;
  let releaseOnStop!: () => void;
  const stopReadySignal = new Promise<void>((resolve) => {
    stopReady = resolve;
  });
  const onStopSignal = new Promise<void>((resolve) => {
    releaseOnStop = resolve;
  });
  const harness = await createCoreHarness({
    prompt: async ({ append, onStop, text, isStopped }) => {
      const label = text.includes("first") ? "first" : "queued";
      await append("cancel.log", `start:${label}\n`);
      onStop(() => releaseOnStop());
      stopReady();
      await onStopSignal;
      if (isStopped()) {
        throw new Error("fixture worker aborted");
      }
      await append("cancel.log", `end:${label}\n`);
    },
  });
  try {
    const target = { channelId: "dm-cancel", channelType: 1 as const };
    await harness.service.submitInbound(
      inbound({ messageId: "cancel-bind", ...target, text: "/new" }),
    );
    const firstAccepted = harness.service.submitInbound(
      inbound({ messageId: "cancel-first", ...target, text: "first" }),
    );
    await harness.workers.waitForPromptCount(1);
    await stopReadySignal;

    const secondAccepted = harness.service.submitInbound(
      inbound({ messageId: "cancel-second", ...target, text: "second" }),
    );
    const thirdAccepted = harness.service.submitInbound(
      inbound({ messageId: "cancel-third", ...target, text: "third" }),
    );
    await Promise.all([secondAccepted, thirdAccepted]);
    await harness.service.submitInbound(
      inbound({ messageId: "cancel-command", ...target, text: "/cancel" }),
    );
    await Promise.all([firstAccepted, onStopSignal]);

    const status = await waitForStatus(
      harness.service,
      target,
      (next) => next.state === "cancelled" && next.queued === 0,
    );
    expect(status.state).toBe("cancelled");
    expect(status.queued).toBe(0);
    expect(harness.workers.prompts).toHaveLength(1);
    expect((await readText(join(harness.root, "tmp", "cancel.log"))).trim()).toBe(
      "start:first",
    );
    expect(
      harness.transport.sent.some(
        ({ target: sentTarget, text }) =>
          sentTarget.channelId === target.channelId && /取消|cancel/i.test(text),
      ),
    ).toBe(true);
  } finally {
    releaseOnStop();
    await closeCoreHarness(harness);
  }
});

test("/cancel delivers a safe compaction terminal notice but suppresses other late notices", async () => {
  let promptReady!: () => void;
  const promptReadySignal = new Promise<void>((resolve) => {
    promptReady = resolve;
  });
  const harness = await createCoreHarness({
    prompt: async ({ emit, waitForAbort }) => {
      promptReady();
      await waitForAbort();
      emit({
        type: "notice",
        messageId: "compaction:fixture:late-start",
        text: "会话压缩已开始。",
        level: "info",
        compactionStatus: "started",
      });
      emit({
        type: "notice",
        messageId: "compaction:fixture:cancelled",
        text: "会话压缩已取消。",
        level: "warning",
        compactionStatus: "cancelled",
      });
      emit({
        type: "notice",
        messageId: "provider-error",
        text: "fixture-private-provider-error",
        level: "error",
      });
    },
  });
  try {
    const target = { channelId: "dm-cancel-compaction", channelType: 1 as const };
    await harness.service.submitInbound(
      inbound({ messageId: "cancel-compaction-bind", ...target, text: "/new" }),
    );
    const compactionNotice = harness.transport.waitForRemoteText(({ target: sentTarget, text }) =>
      sentTarget.channelId === target.channelId && text.includes("会话压缩已取消。"),
    );
    await harness.service.submitInbound(
      inbound({ messageId: "cancel-compaction-task", ...target, text: "compact something" }),
    );
    await harness.workers.waitForPromptCount(1);
    await promptReadySignal;
    await harness.service.submitInbound(
      inbound({ messageId: "cancel-compaction-command", ...target, text: "/cancel" }),
    );

    const notice = await compactionNotice;
    expect(notice.text).toBe("OMP 警告：会话压缩已取消。");
    expect(harness.transport.visibleText(target)).not.toContain("fixture-private-provider-error");
    expect(harness.transport.visibleText(target)).not.toContain("会话压缩已开始。");
    const status = await waitForStatus(
      harness.service,
      target,
      (next) => next.state === "cancelled",
    );
    expect(status.state).toBe("cancelled");
  } finally {
    await closeCoreHarness(harness);
  }
});
