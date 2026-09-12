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
    await harness.service.handleInbound(
      inbound({ messageId: "cancel-bind", ...target, text: "/new" }),
    );
    const firstAccepted = harness.service.handleInbound(
      inbound({ messageId: "cancel-first", ...target, text: "first" }),
    );
    await harness.workers.waitForPromptCount(1);
    await stopReadySignal;

    const secondAccepted = harness.service.handleInbound(
      inbound({ messageId: "cancel-second", ...target, text: "second" }),
    );
    const thirdAccepted = harness.service.handleInbound(
      inbound({ messageId: "cancel-third", ...target, text: "third" }),
    );
    await Promise.all([secondAccepted, thirdAccepted]);
    await harness.service.handleInbound(
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
